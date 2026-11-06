# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""vLLM 0.29+ KV cache layouts and the connector interface around them.

vLLM 0.29 and later resolve one ``KVCacheLayout`` per model and give every
layer the logical page ``[B, H, N, K|V]``: there is no K/V axis of its own, K
and V are the two halves of each head's row, and the layout name changes only
the strides. These tests build such pages in each physical order and check that
the connector stores the same K/V-major slab a rank-5 flash cache did, that
it reads the resolved layout name rather than assuming NHD, and that it keeps
the step ordering its layerwise loads rely on.

The packed tests run on the CPU, where every transfer takes the per-layer
path, and on a device with a built ``maru_kv_ops``, where the logical-page
kernel places the pages and must store and load the same bytes.
"""

import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import torch

import maru_kv_ops
from maru_vllm.connector import _chunk_keys, _logical_page_format
from maru_vllm.kv_layout import _detect_kv_layout, _vllm_kv_cache_layout
from tests.unit.vllm_connector_helpers import (
    attach_capturing_handler,
    deferred_metadata,
    fake_new_request_data,
    fake_scheduler_output,
    make_bare_worker,
    make_flash_attn_metadata,
    make_scheduler,
    make_vllm_scheduler,
    make_worker,
    resolved_cache_config,
    store_metadata,
)

LAYOUTS = ["LBNHC", "LBHNC", "BLNHC", "BHLNC"]


def logical_pages(layout: str, k: list, v: list, device: str = "cpu") -> list:
    """Layer views as vLLM 0.29+ registers them: ``[B, H, N, K|V]``.

    ``k[l]`` and ``v[l]`` are layer ``l``'s keys and values as
    ``[blocks, tokens, heads, head_size]``. The backing memory follows the
    layout's physical order — per layer for the L-outermost layouts, blocks
    outermost (layers interleaved inside each block) for the others — while
    every returned view has the same logical shape, as in vLLM's
    ``create_kv_cache_views``. The pages are allocated on ``device``.
    """
    nb, bs, nh, hs = k[0].shape
    layers, c = len(k), 2 * hs

    def empty(*shape):
        return torch.empty(*shape, device=device)

    if layout == "LBHNC":
        views = [empty(nb, nh, bs, c) for _ in range(layers)]
    elif layout == "LBNHC":
        views = [empty(nb, bs, nh, c).permute(0, 2, 1, 3) for _ in range(layers)]
    elif layout == "BLNHC":
        raw = empty(nb, layers, bs, nh, c)
        views = [raw[:, i].permute(0, 2, 1, 3) for i in range(layers)]
    elif layout == "BHLNC":
        raw = empty(nb, nh, layers, bs, c)
        views = [raw[:, :, i] for i in range(layers)]
    else:
        raise ValueError(layout)
    for view, key, value in zip(views, k, v, strict=True):
        view[..., :hs] = key.permute(0, 2, 1, 3).to(device)
        view[..., hs:] = value.permute(0, 2, 1, 3).to(device)
    return views


requires_kernel = pytest.mark.skipif(
    not torch.cuda.is_available() or not maru_kv_ops.is_available(),
    reason="needs a CUDA device and a built maru_kv_ops extension",
)
# cpu: per-layer copies. cuda: the logical-page kernel, unless refused.
DEVICES = ["cpu", pytest.param("cuda", marks=requires_kernel)]


def triton_metadata() -> MagicMock:
    from vllm.v1.attention.backends.triton_attn import TritonAttentionMetadata

    meta = MagicMock()
    meta.__class__ = type("TritonMeta", (TritonAttentionMetadata,), {})
    return meta


class TestDetection:
    NB, BS, NH, HS = 7, 4, 2, 3

    @pytest.mark.parametrize("name", LAYOUTS)
    def test_page_splits_into_a_kv_axis(self, name):
        shape = (self.NB, self.NH, self.BS, 2 * self.HS)
        got = _detect_kv_layout(shape, self.BS, name, self.NH, self.HS)
        assert got is not None
        assert (got.kv_axis, got.block_axis, got.token_axis) == (3, 0, 2)
        assert got.kv_packed
        assert (got.num_blocks, got.block_size, got.num_heads, got.head_size) == (
            self.NB,
            self.BS,
            self.NH,
            self.HS,
        )
        assert got.format_name is None

    def test_uneven_or_unknown_content_keeps_the_fused_reading(self):
        """Only a row exactly two heads wide is K then V. diffkv's K and V
        differ in width and MLA passes no head size; both keep the fused
        fold, which round-trips but is not K/V-major."""
        diffkv = _detect_kv_layout(
            (self.NB, self.NH, self.BS, self.HS + 5), self.BS, "LBNHC", self.NH, self.HS
        )
        mla = _detect_kv_layout((self.NB, 1, self.BS, 2 * self.HS), self.BS, "LBNHC")
        for got in (diffkv, mla):
            assert got is not None
            assert got.kv_axis is None and not got.kv_packed
            assert (got.block_axis, got.token_axis) == (0, 2)

    def test_legacy_names_keep_legacy_readings(self):
        """Under the NHD/HND names of vLLM < 0.29 the shape keeps its fused reading."""
        shape = (self.NB, self.NH, self.BS, 2 * self.HS)
        got = _detect_kv_layout(shape, self.BS, "HND", self.NH, self.HS)
        assert got is not None and got.kv_axis is None

    def test_unknown_layout_keeps_rank5_off_the_kernel(self):
        """Assuming NHD would hand an HND cache to the NHD kernel format."""
        shape = (self.NB, 2, self.BS, self.NH, self.HS)
        unknown = _detect_kv_layout(shape, self.BS, None, self.NH, self.HS)
        assert unknown is not None and unknown.kv_axis == 1
        assert unknown.format_name is None
        hnd = _detect_kv_layout(shape, self.BS, "HND", self.NH, self.HS)
        assert hnd.format_name == "NL_X_NB_TWO_NH_BS_HS"


class TestKernelFormat:
    """The logical-page kernel is handed the pages' own strides, or nothing."""

    NB, BS, NH, HS, LAYERS = 5, 4, 3, 2, 2
    # Physical order of [L, B, H, N, C], as vllm.v1.kv_cache_layout lists it.
    ORDERS = {
        "LBHNC": (0, 1, 2, 3, 4),
        "LBNHC": (0, 1, 3, 2, 4),
        "LHBNC": (0, 2, 1, 3, 4),
        "BLHNC": (1, 0, 2, 3, 4),
        "BLNHC": (1, 0, 3, 2, 4),
        "BHLNC": (1, 2, 0, 3, 4),
    }

    def _pages(self, name):
        order = self.ORDERS[name]
        logical = (self.LAYERS, self.NB, self.NH, self.BS, 2 * self.HS)
        raw = torch.zeros(tuple(logical[d] for d in order), dtype=torch.bfloat16)
        view = raw.permute(tuple(order.index(d) for d in range(5)))
        return [view[i] for i in range(self.LAYERS)]

    def _layout(self, name):
        shape = (self.NB, self.NH, self.BS, 2 * self.HS)
        return _detect_kv_layout(shape, self.BS, name, self.NH, self.HS)

    @staticmethod
    def _record(**strides):
        return strides

    @pytest.mark.parametrize("name", list(ORDERS))
    def test_strides_come_from_the_pages(self, name):
        pages = self._pages(name)
        got = _logical_page_format(self._record, pages, self._layout(name))
        block, head, token, content = pages[0].stride()
        assert content == 1
        assert got == {
            "block_stride": block,
            "head_stride": head,
            "token_stride": token,
            "num_heads": self.NH,
        }

    def test_lbnhc_is_token_major_inside_a_block(self):
        """The default layout, spelled out: [B, N, H, C] in memory."""
        got = _logical_page_format(
            self._record, self._pages("LBNHC"), self._layout("LBNHC")
        )
        c = 2 * self.HS
        assert got == {
            "block_stride": self.BS * self.NH * c,
            "head_stride": c,
            "token_stride": self.NH * c,
            "num_heads": self.NH,
        }

    def test_layers_with_different_strides_get_none(self):
        """One stride set serves the whole pointer table."""
        mixed = [self._pages("LBHNC")[0], self._pages("LBNHC")[1]]
        assert _logical_page_format(self._record, mixed, self._layout("LBNHC")) is None

    def test_content_that_is_not_innermost_gets_none(self):
        layout = self._layout("LBHNC")
        raw = torch.zeros(self.NB, self.NH, 2 * self.HS, self.BS, dtype=torch.bfloat16)
        page = raw.transpose(2, 3)
        assert page.shape == layout.shape and page.stride()[3] != 1
        assert _logical_page_format(self._record, [page], layout) is None

    def test_a_misaligned_page_gets_none(self):
        layout = self._layout("LBHNC")
        flat = torch.zeros(
            self.NB * self.NH * self.BS * 2 * self.HS + 1, dtype=torch.bfloat16
        )
        page = flat[1:].view(layout.shape)
        assert page.data_ptr() % 16
        assert _logical_page_format(self._record, [page], layout) is None

    def test_an_extension_without_the_format_gets_none(self):
        """A build predating the logical-page kernel keeps the fallback."""
        name = "LBNHC"
        assert _logical_page_format(None, self._pages(name), self._layout(name)) is None


class TestReadLayout:
    def test_reads_the_resolved_name(self):
        assert _vllm_kv_cache_layout(resolved_cache_config("LBHNC")) == "LBHNC"

    def test_real_cache_config(self):
        from vllm.config import CacheConfig

        utils = pytest.importorskip("vllm.v1.attention.backends.utils")
        record = getattr(utils, "record_kv_cache_layout", None)
        if record is None:
            pytest.skip("vLLM < 0.29 reports the layout through get_kv_cache_layout")
        config = CacheConfig(block_size=16)
        # Unresolved: refuse to guess rather than claim NHD.
        assert _vllm_kv_cache_layout(config) is None
        record(config, "HND")  # the legacy alias resolves to its member
        assert _vllm_kv_cache_layout(config) == "LBHNC"

    def test_worker_reads_its_engine_layout_at_registration(self):
        worker = make_worker(4, 8, num_kv_heads=2, head_size=3, kv_cache_layout="BLHNC")
        worker._ensure_handler = MagicMock()
        worker.register_kv_caches({"model.layers.0.self_attn": torch.zeros(7, 2, 4, 6)})
        assert worker._cache_layout == "BLHNC"
        assert worker._kv_layout is not None and worker._kv_layout.kv_packed

    def test_worker_built_without_init_knows_no_layout(self):
        # Test helpers and subclasses allocate workers with __new__; the class
        # defaults stand in for what __init__ and register_kv_caches set.
        from maru_vllm.connector import MaruWorkerConnector

        worker = MaruWorkerConnector.__new__(MaruWorkerConnector)
        worker._block_size, worker._num_kv_heads, worker._head_size = 4, None, None
        worker._kv_layout = None
        assert worker._cache_config is None and worker._cache_layout is None
        rank5 = torch.zeros(2, 7, 4, 2, 3)  # a pre-0.29 flash cache
        expected = _detect_kv_layout(tuple(rank5.shape), 4, None, None, None)
        assert expected is not None and worker._layout_for(rank5) == expected


class TestPackedSlab:
    """The default chunkwise format over logical pages, CPU fallback path."""

    BLOCK, CHUNK, PROMPT = 4, 8, 32
    LAYERS, HEADS, HEAD_SIZE = 3, 2, 3
    NUM_BLOCKS = PROMPT // BLOCK
    HIDDEN = HEADS * HEAD_SIZE

    def _kv(self):
        shape = (self.NUM_BLOCKS, self.BLOCK, self.HEADS, self.HEAD_SIZE)
        numel = self.NUM_BLOCKS * self.BLOCK * self.HIDDEN
        k = [
            (i * 10_000 + torch.arange(numel, dtype=torch.float32)).reshape(shape)
            for i in range(self.LAYERS)
        ]
        return k, [t + 5_000 for t in k]

    def _names(self):
        return [f"model.layers.{i}.self_attn" for i in range(self.LAYERS)]

    def _worker(self, caches, kv_cache_layout):
        worker = make_worker(
            block_size=self.BLOCK,
            kv_chunk_tokens=self.CHUNK,
            num_kv_heads=self.HEADS,
            head_size=self.HEAD_SIZE,
            kv_cache_layout=kv_cache_layout,
        )
        worker._kv_caches = caches
        worker._num_layers = len(caches)
        worker._kv_layout = worker._resolve_kv_layout(caches)
        return worker

    def _store(self, caches, kv_cache_layout, attn_metadata=None):
        writer = self._worker(caches, kv_cache_layout)
        stored = attach_capturing_handler(writer, capture=bytearray)
        token_ids = list(range(self.PROMPT))
        metadata = store_metadata(
            token_ids=token_ids,
            block_ids=list(range(self.NUM_BLOCKS)),
            num_scheduled_tokens=self.PROMPT,
        )
        meta = attn_metadata or make_flash_attn_metadata()
        for name, cache in caches.items():
            writer.save_kv_layer(name, cache, meta, metadata)
        return stored, token_ids, writer

    @pytest.mark.parametrize("device", DEVICES)
    @pytest.mark.parametrize("name", LAYOUTS)
    def test_slab_bytes_match_a_rank5_cache(self, name, device):
        """The pool holds the KV_2LTD bytes a vLLM 0.23-0.25 flash cache of
        the same K and V stored, so engines on either side can share it."""
        k, v = self._kv()
        logical = dict(
            zip(self._names(), logical_pages(name, k, v, device), strict=True)
        )
        rank5 = {
            layer: torch.stack([key, value], dim=1)
            for layer, key, value in zip(self._names(), k, v, strict=True)
        }

        stored, token_ids, writer = self._store(logical, name)
        expected, _, _ = self._store(rank5, "NHD")
        # The kernel stored it on a device, the per-layer copies on the CPU.
        assert (writer._store_kernel_ctx is not None) == (device == "cuda")

        chunk_keys = _chunk_keys(token_ids, self.CHUNK)
        assert set(stored) == set(chunk_keys)
        assert stored == expected
        for ci, base_key in enumerate(chunk_keys):
            slab = torch.frombuffer(stored[base_key], dtype=torch.float32).view(
                2, self.LAYERS, self.CHUNK, self.HIDDEN
            )
            for li in range(self.LAYERS):
                for ti in range(self.CHUNK):
                    block, offset = divmod(ci * self.CHUNK + ti, self.BLOCK)
                    for plane, src in enumerate((k, v)):
                        torch.testing.assert_close(
                            slab[plane, li, ti],
                            src[li][block, offset].reshape(self.HIDDEN),
                        )

    @pytest.mark.parametrize("device", DEVICES)
    @pytest.mark.parametrize("triton", [False, True], ids=["flash", "triton"])
    @pytest.mark.parametrize("name", LAYOUTS)
    def test_roundtrip_restores_every_layer(self, name, triton, device):
        """Triton metadata must not reach the legacy rank-4 Triton branch,
        whose token-major rows do not match the chunk slab. On a device its
        logical page takes the kernel, as Flash's does."""
        meta = triton_metadata() if triton else make_flash_attn_metadata()
        k, v = self._kv()
        src = dict(zip(self._names(), logical_pages(name, k, v, device), strict=True))
        stored, token_ids, writer = self._store(src, name, meta)
        kernel = device == "cuda"
        assert (writer._store_kernel_ctx is not None) == kernel

        zeros = [torch.zeros_like(t) for t in k]
        dst = dict(
            zip(self._names(), logical_pages(name, zeros, zeros, device), strict=True)
        )
        reader = self._worker(dst, name)
        contexts = []
        build = reader._packed_load_kernel_ctx

        def recording(*args):
            contexts.append(build(*args))
            return contexts[-1]

        reader._packed_load_kernel_ctx = recording
        reader._handler = MagicMock()
        reader._handler.batch_retrieve.side_effect = lambda keys: [
            SimpleNamespace(view=memoryview(stored[key]), region_id=0, page_index=i)
            for i, key in enumerate(keys)
        ]
        forward_context = SimpleNamespace(
            no_compile_layers={
                layer: SimpleNamespace(kv_cache=tensor) for layer, tensor in dst.items()
            },
            attn_metadata=meta,
        )
        reader.start_load_kv(
            forward_context,
            deferred_metadata(
                token_ids=token_ids,
                block_ids=list(range(self.NUM_BLOCKS)),
                num_matched_chunks=len(_chunk_keys(token_ids, self.CHUNK)),
            ),
        )

        # A device load completes on its stream; poll as vLLM does each step.
        deadline = time.monotonic() + 30
        finished = reader.get_finished_loading()
        while finished is None and device == "cuda" and time.monotonic() < deadline:
            time.sleep(0.01)
            finished = reader.get_finished_loading()
        assert finished == {"r1"}
        assert reader.take_failed_load_blocks() == set()
        assert contexts and all((c is not None) == kernel for c in contexts)
        for layer in self._names():
            torch.testing.assert_close(dst[layer], src[layer])

    @requires_kernel
    @pytest.mark.parametrize("name", LAYOUTS)
    def test_kernel_writes_and_reads_pinned_slabs_in_place(self, name):
        """Pinned host slabs, as a CUDA-registered CXL mapping hands them out:
        the store kernel writes them and an inline load's kernel reads them,
        with no device copy in between."""
        k, v = self._kv()
        src = dict(zip(self._names(), logical_pages(name, k, v, "cuda"), strict=True))
        writer = self._worker(src, name)
        stored: dict[str, torch.Tensor] = {}

        def alloc(nbytes, **_):
            slab = torch.empty(nbytes, dtype=torch.uint8).pin_memory()
            return SimpleNamespace(buf=memoryview(slab.numpy()), slab=slab)

        def batch_store(keys, handles):
            stored.update({key: h.slab for key, h in zip(keys, handles, strict=True)})
            return [True] * len(keys)

        writer._handler = MagicMock()
        writer._handler.alloc.side_effect = alloc
        writer._handler.batch_store.side_effect = batch_store
        meta = make_flash_attn_metadata()
        handed: list[tuple[str, bool]] = []

        def spying(ctx):
            ops, *rest = ctx

            def transfer(slab, *args, **kwargs):
                handed.append((slab.device.type, slab.is_pinned()))
                return ops.multi_layer_kv_transfer(slab, *args, **kwargs)

            spy = SimpleNamespace(
                multi_layer_kv_transfer=transfer,
                TransferDirection=ops.TransferDirection,
            )
            return (spy, *rest)

        writer._store_kernel_ctx = spying(writer._packed_store_kernel_ctx(meta))
        token_ids = list(range(self.PROMPT))
        metadata = store_metadata(
            token_ids=token_ids,
            block_ids=list(range(self.NUM_BLOCKS)),
            num_scheduled_tokens=self.PROMPT,
        )
        for layer, cache in src.items():
            writer.save_kv_layer(layer, cache, meta, metadata)
        chunks = len(_chunk_keys(token_ids, self.CHUNK))
        assert len(stored) == chunks
        assert handed == [("cpu", True)] * chunks

        zeros = [torch.zeros_like(t) for t in k]
        dst = dict(
            zip(self._names(), logical_pages(name, zeros, zeros, "cuda"), strict=True)
        )
        reader = self._worker(dst, name)
        reader._handler = MagicMock()
        reader._handler.batch_retrieve.side_effect = lambda keys: [
            SimpleNamespace(
                view=memoryview(stored[key].numpy()), region_id=0, page_index=i
            )
            for i, key in enumerate(keys)
        ]
        build = reader._packed_load_kernel_ctx
        reader._packed_load_kernel_ctx = lambda *a: spying(build(*a))
        handed.clear()
        reader.start_load_kv(
            SimpleNamespace(
                no_compile_layers={
                    layer: SimpleNamespace(kv_cache=t) for layer, t in dst.items()
                },
                attn_metadata=meta,
            ),
            deferred_metadata(
                token_ids=token_ids,
                block_ids=list(range(self.NUM_BLOCKS)),
                num_matched_chunks=chunks,
                deferred_load=False,
            ),
        )
        torch.cuda.synchronize()
        assert reader.take_failed_load_blocks() == set()
        assert handed == [("cpu", True)] * chunks
        for layer in self._names():
            torch.testing.assert_close(dst[layer], src[layer])


@requires_kernel
@pytest.mark.parametrize(
    ("layout", "kernel"), [("LBNHC", True), ("NHD", False)], ids=["logical", "rank5"]
)
def test_triton_keeps_the_kernel_only_on_a_logical_page(layout, kernel):
    """vLLM < 0.29's Triton page may pad each head with an inline scale, so
    its rank-5 page stays off the kernel; the 0.29+ page is Flash's."""
    nb, bs, nh, hs = 4, 4, 2, 8
    if layout == "NHD":
        page = torch.zeros(nb, 2, bs, nh, hs, device="cuda")
    else:
        page = torch.zeros(nb, bs, nh, 2 * hs, device="cuda").permute(0, 2, 1, 3)
    worker = make_worker(
        block_size=bs,
        kv_chunk_tokens=bs,
        num_kv_heads=nh,
        head_size=hs,
        kv_cache_layout=layout,
    )
    worker._kv_caches = {"model.layers.0.self_attn": page}
    worker._num_layers = 1
    worker._kv_layout = worker._resolve_kv_layout(worker._kv_caches)
    layers = [("model.layers.0.self_attn", page, 0)]
    triton = worker._packed_load_kernel_ctx(layers, triton_metadata())
    flash = worker._packed_load_kernel_ctx(layers, make_flash_attn_metadata())
    assert flash is not None
    assert (triton is not None) == kernel


def test_mla_registered_and_bound_pages_agree():
    """MLA registers ``[B, 1, N, C]`` and binds its layers to the squeeze.

    Both must address the same slots, also when the page is strided across
    layers, where a reshape would have copied it and lost the index-put.
    """
    from vllm.model_executor.layers.attention.mla_attention import MLACommonMetadata

    meta = MagicMock()
    meta.__class__ = type("MLAMeta", (MLACommonMetadata,), {})
    nb, bs, latent = 8, 4, 6
    raw = torch.zeros(nb, 2, bs, 1, latent)  # blocks outermost, two layers
    registered = raw[:, 0].permute(0, 2, 1, 3)  # [B, 1, N, C], strided
    bound = registered.squeeze(1)
    worker = make_bare_worker(block_size=bs, kv_cache_layout="BLNHC")
    slots = torch.cat([torch.arange(bs), torch.arange(3 * bs, 4 * bs)])
    src = torch.randn(slots.shape[0], latent)

    worker._inject_kv_into_layer(registered, src, slots, meta, "L0")

    assert torch.count_nonzero(raw[:, 1]) == 0, "wrote into another layer"
    for page in (registered, bound):
        got = worker._extract_kv_from_layer(page, slots, meta, "L0")
        torch.testing.assert_close(got, src)


class TestStepOrdering:
    """vLLM 0.29+ starts loads after the forward unless one is synchronous."""

    def _activate(self, output):
        sched = make_scheduler(4, 8, {"maru_async_load": True})
        sched._layerwise_overlap = True
        request = SimpleNamespace(
            request_id="r1", prompt_token_ids=list(range(64)), num_tokens=64
        )
        sched._last_match_result["r1"] = 8
        blocks = MagicMock()
        blocks.get_block_ids.return_value = ([3, 4, 5],)
        sched.update_state_after_alloc(request, blocks, 64)
        sched.build_connector_meta(fake_scheduler_output())
        sched.update_state_after_alloc(request, MagicMock(), 0)
        output.scheduled_new_reqs = [
            fake_new_request_data(request.prompt_token_ids, [3, 4, 5])
        ]
        output.num_scheduled_tokens = {"r1": 1}
        return sched.build_connector_meta(output)

    def test_layerwise_activation_starts_loads_before_the_forward(self):
        """The resumed request's attention waits on per-layer events that
        start_load_kv registers, so they must exist before it runs."""
        output = fake_scheduler_output()
        output.has_sync_kv_loads = False
        meta = self._activate(output)
        assert meta.layerwise_load_req_ids == {"r1"}
        assert output.has_sync_kv_loads is True

    def test_older_scheduler_output_is_left_alone(self):
        output = fake_scheduler_output()
        meta = self._activate(output)
        assert meta.layerwise_load_req_ids == {"r1"}
        assert not hasattr(output, "has_sync_kv_loads")

    def test_steps_without_layerwise_loads_keep_vllms_choice(self):
        sched = make_scheduler(4, 8)
        output = fake_scheduler_output()
        output.has_sync_kv_loads = False
        sched.build_connector_meta(output)
        assert output.has_sync_kv_loads is False


def test_preemption_does_not_drop_output(tmp_path):
    """A best-effort cache opts out of vLLM's producer-role default."""
    engine = make_vllm_scheduler(tmp_path, 4, 8)
    assert engine.connector.requires_kv_delivery is False
    assert getattr(engine, "requires_kv_delivery", False) is False
