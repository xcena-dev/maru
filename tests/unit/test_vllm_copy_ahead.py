# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Copy-ahead for loads without a placement kernel (vLLM 0.29+ layouts).

Such a load copies each chunk slab to the GPU and injects it layer by layer.
``_CopyAhead`` runs the copies on a side stream into a small ring of device
slabs, so that the next chunk's copy overlaps this chunk's injects. These tests
check which slabs take it, that a slab in the ring is never overwritten before
its reader has run, and that loads through it restore every layer, inline and
deferred.
"""

import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import torch

from maru_vllm.connector import _chunk_keys, _CopyAhead
from tests.unit.test_vllm_logical_page import logical_pages
from tests.unit.vllm_connector_helpers import (
    attach_capturing_handler,
    deferred_metadata,
    make_flash_attn_metadata,
    make_worker,
    store_metadata,
)

requires_cuda = pytest.mark.skipif(
    not torch.cuda.is_available(), reason="requires CUDA"
)

# GPU cycles the reader spins before each read, to fall well behind the copies
LAG = 20_000_000


class TestWhichSlabs:
    """No GPU needed."""

    def test_negative_depth_is_rejected(self):
        with pytest.raises(ValueError, match="maru_load_copy_ahead"):
            make_worker(4, 8, {"maru_load_copy_ahead": -1})

    def test_slabs_whose_copy_cannot_overlap_are_copied_in_line(self):
        worker = make_worker(4, 8)
        slab = torch.zeros(2, 3, 8, 6)
        cuda = torch.device("cuda", 0)
        assert worker._copy_ahead_for(torch.device("cpu"), slab, None) is None
        assert worker._copy_ahead_for(cuda, slab, None) is None  # pageable
        # Part of a chunk is a strided slice
        assert worker._copy_ahead_for(cuda, slab[:, :, :4], None) is None
        off = make_worker(4, 8, {"maru_load_copy_ahead": 0})
        assert off._copy_ahead_for(cuda, slab, None) is None

    def test_a_failed_inject_still_waits_for_its_copy(self, monkeypatch):
        """Without a load stream nothing else waits for the side stream, and
        the caller releases the host slabs once the load returns, those of
        a request whose inject raised (a layout refusal) too."""
        from maru_handler.memory.types import MemoryInfo
        from maru_vllm.connector import MaruReqMeta

        events = []

        class Ring:
            def stage(self, host):
                events.append("stage")
                return host, None

            def done(self, slot):
                events.append("done")

            def synchronize(self):
                events.append("synchronize")

        def refuse(*args, **kwargs):
            events.append("inject")
            raise ValueError("unrecognized layout")

        worker = make_worker(4, 8)
        monkeypatch.setattr(worker, "_packed_load_kernel_ctx", lambda *a: None)
        monkeypatch.setattr(worker, "_copy_ahead_for", lambda *a: Ring())
        monkeypatch.setattr(worker, "_inject_kv_into_layer", refuse)
        layers = [
            (f"model.layers.{i}.self_attn", torch.zeros(2, 2, 4, 1, 6), i)
            for i in range(2)
        ]
        slab = torch.zeros(2, 2, 8, 6)  # [2, layers, chunk tokens, hidden]
        request = MaruReqMeta(
            req_id="r1",
            token_ids=list(range(8)),
            block_ids=[0, 1],
            is_store=False,
            num_matched_chunks=1,
        )
        info = MemoryInfo(
            view=memoryview(slab.numpy()).cast("B"), region_id=0, page_index=0
        )
        failed = worker._load_packed(
            layers,
            [(request, 1, torch.arange(8), [info])],
            make_flash_attn_metadata(),
        )
        events.append("return")
        assert failed == {"r1"}
        assert events == ["stage", "inject", "done", "synchronize", "return"]


@requires_cuda
class TestRing:
    @staticmethod
    def _device():
        return torch.device("cuda", torch.cuda.current_device())

    @staticmethod
    def _hosts(count):
        return [torch.full((1 << 20,), float(i)).pin_memory() for i in range(count)]

    def _read_through(self, ahead, hosts):
        """Stage each host slab and read it back late, on the consumer."""
        seen = []
        with torch.cuda.stream(ahead.consumer):
            for host in hosts:
                slab, slot = ahead.stage(host)
                torch.cuda._sleep(LAG)
                seen.append(slab.clone())
                ahead.done(slot)
        return seen

    def test_a_ring_slab_is_not_overwritten_before_its_reader(self):
        device = self._device()
        ahead = _CopyAhead(device, torch.cuda.Stream(device=device), depth=2)
        seen = self._read_through(ahead, self._hosts(6))
        ahead.consumer.synchronize()
        for i, slab in enumerate(seen):
            assert torch.all(slab == i), f"slab {i} holds a later copy"

    def test_synchronize_lets_the_caller_reuse_its_host_slabs(self):
        device = self._device()
        ahead = _CopyAhead(device, torch.cuda.Stream(device=device), depth=2)
        hosts = self._hosts(4)
        seen = self._read_through(ahead, hosts)
        ahead.synchronize()
        for host in hosts:
            host.fill_(-1.0)
        ahead.consumer.synchronize()
        for i, slab in enumerate(seen):
            assert torch.all(slab == i)

    def test_each_stream_that_loads_has_its_own_ring(self):
        worker = make_worker(4, 8)
        device = self._device()
        slab = torch.zeros(2, 3, 8, 6).pin_memory()
        current = worker._copy_ahead_for(device, slab, None)
        assert current is not None
        assert current.consumer == torch.cuda.current_stream(device)
        assert worker._copy_ahead_for(device, slab, None) is current
        other = worker._copy_ahead_for(device, slab, torch.cuda.Stream(device=device))
        assert other is not None and other is not current

    def test_each_thread_that_loads_has_its_own_ring(self):
        """Without a load stream the engine and loader threads may both inject
        on the default stream, and a ring is not thread-safe."""
        worker = make_worker(4, 8)
        device = self._device()
        slab = torch.zeros(2, 3, 8, 6).pin_memory()
        mine = worker._copy_ahead_for(device, slab, None)
        theirs = []
        thread = threading.Thread(
            target=lambda: theirs.append(worker._copy_ahead_for(device, slab, None))
        )
        thread.start()
        thread.join()
        assert theirs[0].consumer == mine.consumer
        assert theirs[0] is not mine

    def test_interleaved_shapes_each_take_their_whole_ring(self):
        device = self._device()
        ahead = _CopyAhead(device, torch.cuda.Stream(device=device), depth=2)
        small = torch.zeros(4).pin_memory()
        large = torch.zeros(8).pin_memory()
        slots = []
        with torch.cuda.stream(ahead.consumer):
            for host in (small, large, small, large):
                _, slot = ahead.stage(host)
                ahead.done(slot)
                slots.append(slot)
        ahead.consumer.synchronize()
        assert slots[0] is not slots[2]
        assert slots[1] is not slots[3]


@requires_cuda
class TestLoadRoundtrip:
    """Loads of vLLM 0.29+ logical pages through the per-layer fallback."""

    BLOCK, CHUNK, PROMPT = 4, 8, 64  # 8 chunks: every ring wraps
    LAYERS, HEADS, HEAD_SIZE = 3, 2, 3
    NUM_BLOCKS = PROMPT // BLOCK
    LAYOUT = "LBNHC"

    def _kv(self):
        shape = (self.NUM_BLOCKS, self.BLOCK, self.HEADS, self.HEAD_SIZE)
        numel = self.NUM_BLOCKS * self.BLOCK * self.HEADS * self.HEAD_SIZE
        k = [
            (i * 10_000 + torch.arange(numel, dtype=torch.float32)).reshape(shape)
            for i in range(self.LAYERS)
        ]
        return k, [t + 5_000 for t in k]

    def _names(self):
        return [f"model.layers.{i}.self_attn" for i in range(self.LAYERS)]

    def _worker(self, caches, extra_config=None):
        worker = make_worker(
            block_size=self.BLOCK,
            kv_chunk_tokens=self.CHUNK,
            extra_config=extra_config,
            num_kv_heads=self.HEADS,
            head_size=self.HEAD_SIZE,
            kv_cache_layout=self.LAYOUT,
        )
        worker._kv_caches = caches
        worker._num_layers = len(caches)
        worker._kv_layout = worker._resolve_kv_layout(caches)
        return worker

    def _stored(self):
        """The reference pages, and the slabs a store of them writes."""
        k, v = self._kv()
        src = dict(zip(self._names(), logical_pages(self.LAYOUT, k, v), strict=True))
        writer = self._worker(src)
        stored = attach_capturing_handler(writer, capture=bytes)
        token_ids = list(range(self.PROMPT))
        metadata = store_metadata(
            token_ids=token_ids,
            block_ids=list(range(self.NUM_BLOCKS)),
            num_scheduled_tokens=self.PROMPT,
        )
        attn_metadata = make_flash_attn_metadata()
        for name, cache in src.items():
            writer.save_kv_layer(name, cache, attn_metadata, metadata)
        return src, stored, token_ids

    @pytest.mark.parametrize("deferred", [False, True], ids=["inline", "deferred"])
    @pytest.mark.parametrize("pinned", [True, False], ids=["pinned", "pageable"])
    @pytest.mark.parametrize("depth", [0, 1, 2])
    def test_load_restores_every_layer(self, depth, pinned, deferred):
        src, stored, token_ids = self._stored()
        # Host slabs as a pool hands them out: page-locked or not
        host = {
            key: torch.frombuffer(bytearray(data), dtype=torch.uint8)
            for key, data in stored.items()
        }
        if pinned:
            host = {key: tensor.pin_memory() for key, tensor in host.items()}
        zeros = [torch.zeros_like(t) for t in self._kv()[0]]
        dst = dict(
            zip(
                self._names(),
                logical_pages(self.LAYOUT, zeros, zeros, device="cuda"),
                strict=True,
            )
        )
        reader = self._worker(dst, {"maru_load_copy_ahead": depth})
        reader._handler = MagicMock()
        reader._handler.batch_retrieve.side_effect = lambda keys: [
            SimpleNamespace(
                view=memoryview(host[key].numpy()), region_id=0, page_index=i
            )
            for i, key in enumerate(keys)
        ]
        forward_context = SimpleNamespace(
            no_compile_layers={
                layer: SimpleNamespace(kv_cache=tensor) for layer, tensor in dst.items()
            },
            attn_metadata=make_flash_attn_metadata(),
        )
        reader.start_load_kv(
            forward_context,
            deferred_metadata(
                token_ids=token_ids,
                block_ids=list(range(self.NUM_BLOCKS)),
                num_matched_chunks=len(_chunk_keys(token_ids, self.CHUNK)),
                deferred_load=deferred,
            ),
        )
        if deferred:
            # On CUDA a deferred load runs on the background loader thread.
            deadline = time.monotonic() + 30.0
            finished = None
            while finished is None and time.monotonic() < deadline:
                finished = reader.get_finished_loading()
                if finished is None:
                    time.sleep(0.01)
            assert finished == {"r1"}
        torch.cuda.synchronize()
        assert reader.take_failed_load_blocks() == set()
        assert bool(reader._copy_ahead) == (pinned and depth > 0)
        for layer in self._names():
            torch.testing.assert_close(dst[layer].cpu(), src[layer])
        reader.shutdown()
