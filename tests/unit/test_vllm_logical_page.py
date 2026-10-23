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

CPU only: the packed kernel has no format for these pages, so every transfer
takes the per-layer path.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import torch

from maru_vllm.connector import _chunk_keys
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


def logical_pages(layout: str, k: list, v: list) -> list:
    """Layer views as vLLM 0.29+ registers them: ``[B, H, N, K|V]``.

    ``k[l]`` and ``v[l]`` are layer ``l``'s keys and values as
    ``[blocks, tokens, heads, head_size]``. The backing memory follows the
    layout's physical order — per layer for the L-outermost layouts, blocks
    outermost (layers interleaved inside each block) for the others — while
    every returned view has the same logical shape, as in vLLM's
    ``create_kv_cache_views``.
    """
    nb, bs, nh, hs = k[0].shape
    layers, c = len(k), 2 * hs
    if layout == "LBHNC":
        views = [torch.empty(nb, nh, bs, c) for _ in range(layers)]
    elif layout == "LBNHC":
        views = [torch.empty(nb, bs, nh, c).permute(0, 2, 1, 3) for _ in range(layers)]
    elif layout == "BLNHC":
        raw = torch.empty(nb, layers, bs, nh, c)
        views = [raw[:, i].permute(0, 2, 1, 3) for i in range(layers)]
    elif layout == "BHLNC":
        raw = torch.empty(nb, nh, layers, bs, c)
        views = [raw[:, :, i] for i in range(layers)]
    else:
        raise ValueError(layout)
    for view, key, value in zip(views, k, v, strict=True):
        view[..., :hs] = key.permute(0, 2, 1, 3)
        view[..., hs:] = value.permute(0, 2, 1, 3)
    return views


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
        return stored, token_ids

    @pytest.mark.parametrize("name", LAYOUTS)
    def test_slab_bytes_match_a_rank5_cache(self, name):
        """The pool holds the KV_2LTD bytes a vLLM 0.23-0.25 flash cache of
        the same K and V stored, so engines on either side can share it."""
        k, v = self._kv()
        logical = dict(zip(self._names(), logical_pages(name, k, v), strict=True))
        rank5 = {
            layer: torch.stack([key, value], dim=1)
            for layer, key, value in zip(self._names(), k, v, strict=True)
        }

        stored, token_ids = self._store(logical, name)
        expected, _ = self._store(rank5, "NHD")

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

    @pytest.mark.parametrize("triton", [False, True], ids=["flash", "triton"])
    @pytest.mark.parametrize("name", LAYOUTS)
    def test_roundtrip_restores_every_layer(self, name, triton):
        """Triton metadata must not reach the legacy rank-4 Triton branch,
        whose token-major rows do not match the chunk slab."""
        meta = triton_metadata() if triton else make_flash_attn_metadata()
        k, v = self._kv()
        src = dict(zip(self._names(), logical_pages(name, k, v), strict=True))
        stored, token_ids = self._store(src, name, meta)

        zeros = [torch.zeros_like(t) for t in k]
        dst = dict(zip(self._names(), logical_pages(name, zeros, zeros), strict=True))
        reader = self._worker(dst, name)
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

        assert reader.get_finished_loading() == {"r1"}
        assert reader.take_failed_load_blocks() == set()
        for layer in self._names():
            torch.testing.assert_close(dst[layer], src[layer])


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
