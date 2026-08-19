# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for maru_vllm.connector utilities.

Tests pure functions and layout helpers without requiring CXL hardware,
a running MaruServer, or GPU. All tensors are CPU-only.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("torch", reason="torch not installed")

import torch

from maru_vllm.connector import (
    _align_down,
    _chunk_keys,
    _parse_size,
)
from tests.unit.vllm_connector_helpers import (
    attach_capturing_handler,
    capture_float32,
    deferred_metadata,
    deferred_req_meta,
    fake_cached_reqs,
    fake_new_request_data,
    fake_scheduler_output,
    make_bare_worker,
    make_flash_attn_metadata,
    make_scheduler,
    make_worker,
    store_metadata,
)

# =============================================================================
# _parse_size
# =============================================================================


class TestParseSize:
    def test_int_passthrough(self):
        assert _parse_size(1024) == 1024

    def test_plain_number_string(self):
        assert _parse_size("4096") == 4096

    def test_kilobytes(self):
        assert _parse_size("4K") == 4 * 1024

    def test_megabytes(self):
        assert _parse_size("500M") == 500 * 1024**2

    def test_gigabytes(self):
        assert _parse_size("2G") == 2 * 1024**3

    def test_terabytes(self):
        assert _parse_size("1T") == 1024**4

    def test_with_b_suffix(self):
        assert _parse_size("4GB") == 4 * 1024**3

    def test_lowercase(self):
        assert _parse_size("500m") == 500 * 1024**2

    def test_float_value(self):
        assert _parse_size("1.5G") == int(1.5 * 1024**3)

    def test_invalid_string_raises(self):
        with pytest.raises(ValueError, match="Invalid size string"):
            _parse_size("invalid")

    def test_zero(self):
        assert _parse_size(0) == 0


# =============================================================================
# _align_down
# =============================================================================


class TestAlignDown:
    def test_exact_multiple(self):
        assert _align_down(256, 16) == 256

    def test_not_aligned(self):
        assert _align_down(260, 16) == 256

    def test_less_than_block(self):
        assert _align_down(10, 16) == 0

    def test_zero(self):
        assert _align_down(0, 16) == 0


# =============================================================================
# _chunk_keys
# =============================================================================


class TestChunkKeys:
    def test_basic_chunking(self):
        token_ids = list(range(512))
        keys = _chunk_keys(token_ids, chunk_tokens=256)
        assert len(keys) == 2
        assert all(k.startswith("kv_") for k in keys)

    def test_partial_last_chunk_ignored(self):
        token_ids = list(range(300))  # 1 full chunk + 44 leftover
        keys = _chunk_keys(token_ids, chunk_tokens=256)
        assert len(keys) == 1

    def test_empty_input(self):
        assert _chunk_keys([], chunk_tokens=256) == []

    def test_shorter_than_chunk(self):
        token_ids = list(range(100))
        assert _chunk_keys(token_ids, chunk_tokens=256) == []

    def test_deterministic(self):
        token_ids = list(range(512))
        keys1 = _chunk_keys(token_ids, chunk_tokens=256)
        keys2 = _chunk_keys(token_ids, chunk_tokens=256)
        assert keys1 == keys2

    def test_prefix_sensitivity(self):
        """Different prefixes produce different keys even for same chunk index."""
        tokens_a = list(range(256))
        tokens_b = list(range(1, 257))
        keys_a = _chunk_keys(tokens_a, chunk_tokens=256)
        keys_b = _chunk_keys(tokens_b, chunk_tokens=256)
        assert keys_a != keys_b

    def test_rolling_prefix_hash(self):
        """Chunk N's key encodes the full prefix, not just chunk N's tokens."""
        tokens = list(range(512))
        keys = _chunk_keys(tokens, chunk_tokens=256)
        # Chunk 1's key should differ from chunking just tokens[256:512]
        keys_second_only = _chunk_keys(tokens[256:], chunk_tokens=256)
        assert keys[1] != keys_second_only[0]


# =============================================================================
# MaruWorkerConnector._build_slot_mapping
# =============================================================================


class TestBuildSlotMapping:
    def _make_worker(self, block_size=16):
        """Create a minimal MaruWorkerConnector for testing."""
        return make_bare_worker(block_size=block_size)

    def test_single_block(self):
        worker = self._make_worker(block_size=16)
        slots = worker._build_slot_mapping([0], num_tokens=16)
        assert slots.tolist() == list(range(16))

    def test_multiple_blocks(self):
        worker = self._make_worker(block_size=4)
        slots = worker._build_slot_mapping([0, 2], num_tokens=8)
        # block 0: slots 0,1,2,3  block 2: slots 8,9,10,11
        assert slots.tolist() == [0, 1, 2, 3, 8, 9, 10, 11]

    def test_truncation(self):
        worker = self._make_worker(block_size=4)
        slots = worker._build_slot_mapping([0, 1], num_tokens=6)
        assert len(slots) == 6
        assert slots.tolist() == [0, 1, 2, 3, 4, 5]

    @pytest.mark.skipif(not torch.cuda.is_available(), reason="requires CUDA")
    def test_pin_for_async_h2d_preserves_values(self):
        worker = self._make_worker(block_size=4)
        slots = worker._build_slot_mapping([0, 2], num_tokens=8)

        pinned = worker._pin_slot_mapping_for_async_h2d(slots)

        assert pinned.is_pinned()
        assert pinned.tolist() == slots.tolist()
        assert worker._pin_slot_mapping_for_async_h2d(pinned) is pinned


# =============================================================================
# MaruWorkerConnector P1/P2 helpers
# =============================================================================


class TestBatchRetrieveAll:
    def test_splits_payload_and_preserves_order(self):
        worker = make_worker(block_size=4, kv_chunk_tokens=4)
        worker._handler = MagicMock()
        worker._handler.batch_retrieve.side_effect = lambda keys: [
            f"info:{k}" for k in keys
        ]

        keys = [f"key-{i}" for i in range(5)]
        result = worker._batch_retrieve_all(keys, batch_size=2)

        assert result == [f"info:{key}" for key in keys]
        assert [
            call.args[0] for call in worker._handler.batch_retrieve.call_args_list
        ] == [
            keys[0:2],
            keys[2:4],
            keys[4:5],
        ]


class TestBatchStoreLayer:
    def test_registers_all_chunks_with_one_batch_store(self):
        worker = make_worker(
            block_size=4, kv_chunk_tokens=4, extra_config={"maru_use_layerwise": True}
        )
        worker._handler = MagicMock()
        worker._handler.alloc.side_effect = lambda nbytes: SimpleNamespace(
            buf=bytearray(nbytes)
        )
        worker._handler.batch_store.return_value = [True, True]
        worker._handler.store.return_value = True
        worker._num_layers = 1

        kv_layer = torch.arange(2 * 2 * 4 * 2, dtype=torch.float32).reshape(2, 2, 4, 2)
        attn_metadata = make_flash_attn_metadata()
        metadata = store_metadata(
            req_id="request-1",
            token_ids=list(range(8)),
            block_ids=[0, 1],
            num_scheduled_tokens=8,
        )

        worker.save_kv_layer(
            "model.layers.0.self_attn", kv_layer, attn_metadata, metadata
        )

        worker._handler.batch_store.assert_called_once()
        stored_keys, handles = worker._handler.batch_store.call_args.args
        assert len(stored_keys) == 2
        assert len(handles) == 2
        assert all(key.endswith("_L0") for key in stored_keys)
        assert worker._handler.store.call_count == 2
        assert worker._chunk_layer_progress == {}


class TestChunkObjectBytes:
    def _make_worker(self, block_size=16, chunk_tokens=256):
        return make_worker(block_size=block_size, kv_chunk_tokens=chunk_tokens)

    def _with_caches(self, worker, tensor):
        """Register caches the way the worker does, resolving the layout."""
        worker._kv_caches = {"layer": tensor}
        worker._kv_layout = worker._resolve_kv_layout(worker._kv_caches)
        return worker

    def test_flash_layout_kv_axis_first(self):
        """vLLM <= 0.22.1 on CUDA, and ROCm throughout: [2, NB, BS, NH, HS]."""
        worker = self._with_caches(
            self._make_worker(), torch.empty(2, 8, 16, 4, 2, dtype=torch.float16)
        )

        assert worker._chunk_object_bytes() == 2 * 4 * 2 * 256 * 2

    def test_flash_layout_kv_axis_second(self):
        """vLLM 0.23.0+ moved the K/V axis to position 1: [NB, 2, BS, NH, HS].

        Same tensor, same bytes per token. Reading the axes positionally made
        the connector skip every store on this layout, which is how a real run
        ended up with a 0% cache hit rate.
        """
        worker = self._with_caches(
            self._make_worker(), torch.empty(8, 2, 16, 4, 2, dtype=torch.float16)
        )

        assert worker._chunk_object_bytes() == 2 * 4 * 2 * 256 * 2

    def test_mla_layout(self):
        worker = self._with_caches(
            self._make_worker(), torch.empty(8, 16, 12, dtype=torch.float16)
        )

        assert worker._chunk_object_bytes() == 12 * 256 * 2

    def test_unrecognized_layout_keeps_default(self):
        worker = self._with_caches(
            self._make_worker(), torch.empty(3, 5, dtype=torch.float16)
        )

        assert worker._chunk_object_bytes() is None

    def test_block_size_mismatch_keeps_default(self):
        """A layout whose block axis disagrees with vLLM's configured block
        size means the axes were misread, so refuse rather than size a page
        from a plausible-looking wrong number."""
        worker = self._with_caches(
            self._make_worker(block_size=32),
            torch.empty(8, 2, 16, 4, 2, dtype=torch.float16),
        )

        assert worker._chunk_object_bytes() is None


class TestDetectKVLayout:
    """``_detect_kv_layout`` is pure, so every axis order is checkable here.

    The shapes below are each some shipped vLLM backend's
    ``get_kv_cache_shape()`` return value, not hypotheticals.
    """

    NB, BS, NH, HS = 7, 16, 4, 32

    def _expect(self, shape, kv_layout, axes, fmt, **kw):
        """Assert the resolved dims and (kv, block, token) axis indices."""
        from maru_vllm.kv_layout import _detect_kv_layout

        got = _detect_kv_layout(shape, self.BS, kv_layout, **kw)
        assert got is not None, f"{shape} {kv_layout} went unrecognized"
        assert (got.num_blocks, got.block_size, got.num_heads, got.head_size) == (
            self.NB,
            self.BS,
            self.NH,
            self.HS,
        )
        assert got.page_buffer_size == self.NB * self.BS
        assert (got.kv_axis, got.block_axis, got.token_axis) == axes
        assert got.format_name == fmt
        return got

    def test_kv_axis_first(self):
        """``(2, NB, BS, NH, HS)`` — rocm_attn."""
        nb, bs, nh, hs = self.NB, self.BS, self.NH, self.HS
        self._expect((2, nb, bs, nh, hs), "NHD", (0, 1, 2), "NL_X_TWO_NB_BS_NH_HS")

    def test_kv_axis_second(self):
        """``(NB, 2, BS, NH, HS)`` — flash_attn, flashinfer, flex, triton."""
        nb, bs, nh, hs = self.NB, self.BS, self.NH, self.HS
        self._expect((nb, 2, bs, nh, hs), "NHD", (1, 0, 2), "NL_X_NB_TWO_BS_NH_HS")

    def test_hnd_moves_no_axis_only_the_kernel_format(self):
        """HND permutes strides, not axes.

        vLLM allocates with the NHD/HND permutation applied and then permutes
        straight back (``gpu_model_runner``: ``kv_cache.permute(*inv_order)``),
        so the shape handed to the connector is identical under both. Reading
        HND as a different axis order made every HND deployment fail the
        block_size cross-check and go unrecognized — the same empty-pool
        outcome this detection exists to prevent. Only the kernel, which walks
        raw memory, sees the difference.
        """
        nb, bs, nh, hs = self.NB, self.BS, self.NH, self.HS
        nhd = self._expect(
            (nb, 2, bs, nh, hs), "NHD", (1, 0, 2), "NL_X_NB_TWO_BS_NH_HS"
        )
        hnd = self._expect(
            (nb, 2, bs, nh, hs), "HND", (1, 0, 2), "NL_X_NB_TWO_NH_BS_HS"
        )
        assert (nhd.kv_axis, nhd.block_axis, nhd.token_axis) == (
            hnd.kv_axis,
            hnd.block_axis,
            hnd.token_axis,
        )

    def test_fused_last_dimension(self):
        """K and V sharing the last dimension, both orders.

        ``(NB, BS, NH, 2*HS)`` is flash_attn_diffkv and turboquant_attn;
        ``(NB, NH, BS, 2*HS)`` is cpu_attn. Neither gets a kernel format: the
        LMCache ``*_TWO_HS`` constants describe an even K/V split, which
        diffkv (head_size + head_size_v) and turboquant (packed scales) do not
        provide.
        """
        nb, bs, nh, hs = self.NB, self.BS, self.NH, self.HS
        self._expect((nb, bs, nh, 2 * hs), "NHD", (None, 0, 1), None)
        self._expect((nb, nh, bs, 2 * hs), "HND", (None, 0, 2), None)

    def test_fused_orders_separated_by_num_kv_heads(self):
        """When num_kv_heads == block_size the two fused orders are identical
        shapes, and only the engine's own head count can tell them apart."""
        from maru_vllm.kv_layout import _detect_kv_layout

        bs = self.BS
        shape = (self.NB, bs, bs, 2 * self.HS)  # NH == BS: ambiguous alone
        got = _detect_kv_layout(shape, bs, "NHD", num_kv_heads=bs, head_size=self.HS)
        assert got is not None
        assert got.num_heads == bs

    def test_head_count_rejects_a_wrong_reading(self):
        """A candidate that parses but disagrees with the engine is refused."""
        from maru_vllm.kv_layout import _detect_kv_layout

        shape = (self.NB, 2, self.BS, self.NH, self.HS)
        assert (
            _detect_kv_layout(shape, self.BS, "NHD", num_kv_heads=self.NH) is not None
        )
        assert (
            _detect_kv_layout(shape, self.BS, "NHD", num_kv_heads=self.NH + 1) is None
        )

    def test_two_kv_heads_does_not_hijack_a_fused_shape(self):
        """``(NB, 2, BS, F)`` from cpu_attn with 2 KV heads must not read as a
        K/V axis at position 1, which would treat the head axis as K/V."""
        from maru_vllm.kv_layout import _detect_kv_layout

        shape = (self.NB, 2, self.BS, 2 * self.HS)
        got = _detect_kv_layout(
            shape, self.BS, "HND", num_kv_heads=2, head_size=self.HS
        )
        assert got is not None
        assert got.kv_axis is None, "fused shape misread as having a K/V axis"
        assert (got.block_axis, got.token_axis, got.num_heads) == (0, 2, 2)

    def test_mla(self):
        from maru_vllm.kv_layout import _detect_kv_layout

        got = _detect_kv_layout((self.NB, self.BS, 656), self.BS, "NHD")
        assert got is not None
        assert got.page_buffer_size == self.NB * self.BS
        assert (got.kv_axis, got.block_axis, got.token_axis) == (None, 0, 1)

    def test_mla_gets_no_kernel_format(self):
        """rank-3 must stay off the raw-pointer kernel (pre-detection gating
        was dim() != 5). The fused transfer path stores K/V-half-major bytes,
        not the token-major rows NL_X_NB_BS_HS describes, and sparse-MLA
        metadata is not MLACommonMetadata, so a format here would reach the
        kernel and silently mis-order memory."""
        from maru_vllm.kv_layout import _detect_kv_layout

        got = _detect_kv_layout((self.NB, self.BS, 656), self.BS, "NHD")
        assert got is not None and got.format_name is None

    def test_two_blocks_resolves_to_the_common_backend(self):
        """(2, 2, BS, NH, HS): a flash_attn cache with num_blocks == 2 also
        parses as rocm_attn and no cross-check can separate the two, so the
        tie must go to the far more common kv_axis=1 family."""
        from maru_vllm.kv_layout import _detect_kv_layout

        got = _detect_kv_layout(
            (2, 2, self.BS, self.NH, self.HS),
            self.BS,
            "NHD",
            num_kv_heads=self.NH,
            head_size=self.HS,
        )
        assert got is not None
        assert got.kv_axis == 1

    def test_rejects_when_block_size_disagrees(self):
        from maru_vllm.kv_layout import _detect_kv_layout

        shape = (self.NB, 2, self.BS, self.NH, self.HS)
        # Block size the engine never configured.
        assert _detect_kv_layout(shape, 32, "NHD") is None

    def test_rejects_unknown_rank(self):
        from maru_vllm.kv_layout import _detect_kv_layout

        assert _detect_kv_layout((3, 5), self.BS, "NHD") is None


class TestRegisterKVCaches:
    def test_eagerly_connects_after_deriving_packed_page_size(self):
        """Keep expensive CXL mapping off the first populate request path."""
        worker = make_worker(block_size=16, kv_chunk_tokens=256)
        observed_page_sizes = []
        worker._ensure_handler = lambda: observed_page_sizes.append(
            worker._page_size_bytes
        )
        kv_caches = {
            "layer0": torch.empty(2, 8, 16, 8, dtype=torch.float16),
            "layer1": torch.empty(2, 8, 16, 8, dtype=torch.float16),
        }

        worker.register_kv_caches(kv_caches)

        per_layer_bytes = 2 * 8 * 256 * 2
        assert observed_page_sizes == [per_layer_bytes * len(kv_caches)]

    def test_empty_registration_does_not_connect_with_default_page_size(self):
        worker = make_worker(block_size=16, kv_chunk_tokens=256)
        worker._ensure_handler = MagicMock()

        worker.register_kv_caches({})

        worker._ensure_handler.assert_not_called()

    def test_eager_failure_does_not_back_off_first_request_retry(self):
        worker = make_worker(block_size=16, kv_chunk_tokens=256)
        kv_caches = {
            "layer0": torch.empty(2, 8, 16, 8, dtype=torch.float16),
        }

        with patch(
            "maru_vllm.connector._create_maru_handler",
            side_effect=RuntimeError("server not ready"),
        ):
            worker.register_kv_caches(kv_caches)

        assert worker._handler is None
        assert worker._handler_retry_after == 0.0


class TestAsyncLayerLoad:
    def test_cpu_layout_falls_back_to_sync(self):
        worker = make_worker(
            block_size=4,
            kv_chunk_tokens=4,
            extra_config={"maru_enable_async_loading": True},
        )
        layers = [("layer", torch.empty(2, 1, 4, 2), 0)]

        assert not worker._schedule_async_loads(layers, [], MagicMock())

    def test_wait_for_layer_joins_event_on_current_stream(self, monkeypatch):
        worker = make_worker(block_size=4, kv_chunk_tokens=4)
        event = MagicMock()
        current_stream = MagicMock()
        worker._layer_load_events = {"layer": event}
        monkeypatch.setattr(torch.cuda, "current_stream", lambda: current_stream)

        worker.wait_for_layer_load("layer")

        current_stream.wait_event.assert_called_once_with(event)

    def test_chunk_runs_coalesce_consecutive_full_pages(self):
        worker = make_worker(block_size=4, kv_chunk_tokens=4)
        worker._effective_page_size_bytes = 4
        region = bytearray(b"aaaabbbbcccc")
        infos = [
            SimpleNamespace(
                view=memoryview(region)[i * 4 : (i + 1) * 4],
                region_id=7,
                page_index=i,
            )
            for i in range(3)
        ]

        runs = worker._chunk_runs(infos)

        assert len(runs) == 1
        assert runs[0][0:2] == (0, 3)
        assert bytes(runs[0][2]) == bytes(region)

    def test_chunk_runs_keep_gapped_pages_separate(self):
        worker = make_worker(block_size=4, kv_chunk_tokens=4)
        worker._effective_page_size_bytes = 4
        region = bytearray(b"aaaabbbbcccc")
        infos = [
            SimpleNamespace(view=memoryview(region)[0:4], region_id=7, page_index=0),
            SimpleNamespace(view=memoryview(region)[8:12], region_id=7, page_index=2),
        ]

        runs = worker._chunk_runs(infos)

        assert [(start, count) for start, count, _ in runs] == [(0, 1), (1, 1)]

    def test_coalesced_flash_chunks_preserve_kv_layout(self):
        worker = make_worker(block_size=4, kv_chunk_tokens=4)
        source = torch.arange(2 * 2 * 4 * 3, dtype=torch.float32).reshape(2, 2, 4, 3)
        destination = torch.zeros_like(source)
        chunk_0_slots = torch.tensor([0, 1, 2, 3])
        chunk_1_slots = torch.tensor([4, 5, 6, 7])
        attn_metadata = make_flash_attn_metadata()
        chunks = [
            worker._extract_kv_from_layer(
                source, slots, attn_metadata, "model.layers.0.self_attn"
            )
            for slots in (chunk_0_slots, chunk_1_slots)
        ]
        stored_bytes = torch.cat([chunk.reshape(-1) for chunk in chunks])

        worker._inject_kv_into_layer(
            destination,
            stored_bytes,
            torch.arange(8),
            attn_metadata,
            "model.layers.0.self_attn",
            num_chunks=2,
        )

        torch.testing.assert_close(destination, source)


# =============================================================================
# MaruWorkerConnector._get_layer_index
# =============================================================================


class TestGetLayerIndex:
    def _make_worker(self):
        return make_bare_worker(
            block_size=16, kv_caches={"layer_a": None, "layer_b": None}
        )

    def test_standard_layer_name(self):
        worker = self._make_worker()
        assert worker._get_layer_index("model.layers.5.self_attn") == 5

    def test_fallback_to_kv_caches_key(self):
        worker = self._make_worker()
        assert worker._get_layer_index("layer_b") == 1

    def test_unknown_returns_zero_with_warning(self):
        worker = self._make_worker()
        idx = worker._get_layer_index("unknown_layer")
        assert idx == 0


# =============================================================================
# _inject_kv_into_layer / _extract_kv_from_layer roundtrip
# =============================================================================


class TestKVLayerRoundtrip:
    """Test inject/extract roundtrip for Flash attention layout (default)."""

    def _make_worker(self, block_size=4):
        return make_bare_worker(block_size=block_size)

    def test_flash_roundtrip(self):
        """Flash attention: [2, num_pages, page_size, head_dim]"""
        worker = self._make_worker(block_size=4)
        num_pages, page_size, head_dim = 4, 4, 8
        kv_cache = torch.zeros(2, num_pages, page_size, head_dim)

        # Create source data for 4 tokens
        src_data = torch.randn(2, 4, head_dim)
        slot_mapping = torch.tensor([0, 1, 2, 3])

        # Use a plain object as attn_metadata (not MLA or Triton → Flash branch)
        attn_metadata = make_flash_attn_metadata()

        worker._inject_kv_into_layer(
            kv_cache, src_data, slot_mapping, attn_metadata, "layer0"
        )

        extracted = worker._extract_kv_from_layer(
            kv_cache, slot_mapping, attn_metadata, "layer0"
        )

        torch.testing.assert_close(extracted, src_data)

    def test_flash_roundtrip_noncontiguous_slots(self):
        """Slots from non-contiguous blocks."""
        worker = self._make_worker(block_size=4)
        num_pages, page_size, head_dim = 8, 4, 8
        kv_cache = torch.zeros(2, num_pages, page_size, head_dim)

        src_data = torch.randn(2, 4, head_dim)
        # Slots from block 0 (0-3) and block 3 (12-15), take first 4
        slot_mapping = torch.tensor([0, 1, 12, 13])

        attn_metadata = make_flash_attn_metadata()

        worker._inject_kv_into_layer(
            kv_cache, src_data, slot_mapping, attn_metadata, "layer0"
        )
        extracted = worker._extract_kv_from_layer(
            kv_cache, slot_mapping, attn_metadata, "layer0"
        )

        torch.testing.assert_close(extracted, src_data)

    def test_dict_attn_metadata(self):
        """attn_metadata as dict keyed by layer_name."""
        worker = self._make_worker(block_size=4)
        num_pages, page_size, head_dim = 4, 4, 8
        kv_cache = torch.zeros(2, num_pages, page_size, head_dim)

        src_data = torch.randn(2, 4, head_dim)
        slot_mapping = torch.tensor([0, 1, 2, 3])

        inner_meta = make_flash_attn_metadata()
        attn_metadata = {"layer0": inner_meta, "layer1": inner_meta}

        worker._inject_kv_into_layer(
            kv_cache, src_data, slot_mapping, attn_metadata, "layer0"
        )
        extracted = worker._extract_kv_from_layer(
            kv_cache, slot_mapping, attn_metadata, "layer0"
        )

        torch.testing.assert_close(extracted, src_data)


# Every shape below is some shipped backend's get_kv_cache_shape() output.
# (label, shape_fn(nb, bs, nh, hs), num_kv_heads)
_PAGED_LAYOUTS = [
    ("rocm_attn", lambda nb, bs, nh, hs: (2, nb, bs, nh, hs), None),
    ("flash_attn", lambda nb, bs, nh, hs: (nb, 2, bs, nh, hs), None),
    ("diffkv_fused", lambda nb, bs, nh, hs: (nb, bs, nh, 2 * hs), None),
    ("cpu_attn_fused", lambda nb, bs, nh, hs: (nb, nh, bs, 2 * hs), None),
    ("flash_attn_2_kv_heads", lambda nb, bs, nh, hs: (nb, 2, bs, 2, hs), 2),
    ("cpu_attn_2_kv_heads", lambda nb, bs, nh, hs: (nb, 2, bs, 2 * hs), 2),
]


class TestKVLayerRoundtripAllLayouts:
    """inject -> extract must be an exact identity for every layout accepted.

    The pre-existing round-trip tests only ever built ``[2, NB, BS, HD]``, so
    a ``_inject_kv_into_layer`` that hardcoded that order stayed green while
    it was broken for every other backend. These parametrize the shape.
    """

    NB, BS, NH, HS = 5, 4, 3, 8

    def _cache(self, shape_fn, hnd_strides=False):
        shape = shape_fn(self.NB, self.BS, self.NH, self.HS)
        t = torch.zeros(shape)
        if hnd_strides and len(shape) == 5:
            # Reproduce what HND allocation leaves behind: same shape, head and
            # block axes swapped in memory, so the tensor is not contiguous.
            t = t.permute(0, 1, 3, 2, 4).contiguous().permute(0, 1, 3, 2, 4)
            assert not t.is_contiguous()
        return t

    @pytest.mark.parametrize(
        "label,shape_fn,num_kv_heads",
        _PAGED_LAYOUTS,
        ids=[layout[0] for layout in _PAGED_LAYOUTS],
    )
    @pytest.mark.parametrize("hnd_strides", [False, True], ids=["nhd", "hnd"])
    def test_roundtrip(self, label, shape_fn, num_kv_heads, hnd_strides):
        kv_cache = self._cache(shape_fn, hnd_strides)
        worker = make_bare_worker(
            block_size=self.BS,
            kv_caches={"layer0": kv_cache},
            num_kv_heads=num_kv_heads,
            head_size=self.HS,
        )
        assert worker._kv_layout is not None, f"{label} {tuple(kv_cache.shape)}"

        ntok = 2 * self.BS
        hidden = kv_cache.numel() // (2 * self.NB * self.BS)
        src = torch.randn(2, ntok, hidden)
        # Slots from two different pages, so a wrong block/offset split shows.
        slots = torch.cat(
            [torch.arange(self.BS), torch.arange(3 * self.BS, 4 * self.BS)]
        )
        meta = make_flash_attn_metadata()

        worker._inject_kv_into_layer(kv_cache, src, slots, meta, "layer0")
        got = worker._extract_kv_from_layer(kv_cache, slots, meta, "layer0")

        torch.testing.assert_close(got, src)

    def test_untouched_slots_stay_zero(self):
        """A wrong axis split can round-trip and still scribble elsewhere."""
        kv_cache = self._cache(lambda nb, bs, nh, hs: (nb, 2, bs, nh, hs))
        worker = make_bare_worker(
            block_size=self.BS, kv_caches={"layer0": kv_cache}, head_size=self.HS
        )
        slots = torch.arange(self.BS)
        meta = make_flash_attn_metadata()
        worker._inject_kv_into_layer(
            kv_cache, torch.randn(2, self.BS, self.NH * self.HS), slots, meta, "layer0"
        )

        # Page 0 was written; every other page must be untouched.
        assert torch.count_nonzero(kv_cache[1:]) == 0

    def test_extract_is_identical_across_axis_orders(self):
        """The same logical cache in two axis orders must extract to the same
        bytes, or two engines sharing a CXL pool would disagree."""
        meta = make_flash_attn_metadata()
        slots = torch.arange(2 * self.BS)
        kv_axis_second = torch.randn(self.NB, 2, self.BS, self.NH, self.HS)
        kv_axis_first = kv_axis_second.permute(1, 0, 2, 3, 4).contiguous()

        out = []
        for cache in (kv_axis_first, kv_axis_second):
            worker = make_bare_worker(
                block_size=self.BS, kv_caches={"layer0": cache}, head_size=self.HS
            )
            out.append(worker._extract_kv_from_layer(cache, slots, meta, "layer0"))

        assert torch.equal(out[0], out[1])

    def test_refuses_unrecognized_layout(self):
        """Refusing degrades the request to recompute; guessing would register
        scrambled KV."""
        worker = make_bare_worker(block_size=self.BS)
        cache = torch.zeros(3, 5)
        meta = make_flash_attn_metadata()

        with pytest.raises(RuntimeError, match="unrecognized paged KV tensor"):
            worker._extract_kv_from_layer(cache, torch.arange(2), meta, "layer0")
        with pytest.raises(RuntimeError, match="unrecognized paged KV tensor"):
            worker._inject_kv_into_layer(
                cache, torch.zeros(2, 2, 1), torch.arange(2), meta, "layer0"
            )

    def test_hybrid_layer_is_redetected(self):
        """A registered rank-5 layout must not be applied to another layer of
        a different shape: movedim succeeds on the wrong rank and returns
        wrong bytes silently. A hybrid model's odd layer re-detects from its
        own shape (sparse-MLA metadata is not MLACommonMetadata, so it lands
        in the layout branch)."""
        flash = torch.zeros(self.NB, 2, self.BS, self.NH, self.HS)
        worker = make_bare_worker(
            block_size=self.BS, kv_caches={"L0": flash}, head_size=self.HS
        )
        assert worker._kv_layout is not None and worker._kv_layout.kv_axis == 1

        mla = torch.zeros(self.NB, self.BS, 2 * self.HS)
        meta = make_flash_attn_metadata()
        slots = torch.arange(self.BS)
        src = torch.randn(2, self.BS, self.HS)

        worker._inject_kv_into_layer(mla, src, slots, meta, "L1")
        got = worker._extract_kv_from_layer(mla, slots, meta, "L1")

        torch.testing.assert_close(got, src)

    def test_rank4_fused_layer_not_read_through_registered_rank5_layout(self):
        """Exact-shape matching in _layout_fits.

        A rank-5 layout's block, token, and K/V axis positions can all land
        on matching dims of a rank-4 fused tensor with two KV heads, which
        would read the head axis as K/V. The layer must re-detect as fused.
        """
        flash = torch.zeros(self.NB, 2, self.BS, 2, self.HS)
        worker = make_bare_worker(
            block_size=self.BS,
            kv_caches={"L0": flash},
            num_kv_heads=2,
            head_size=self.HS,
        )
        assert worker._kv_layout is not None and worker._kv_layout.kv_axis == 1

        fused = torch.zeros(self.NB, 2, self.BS, 2 * self.HS)  # cpu_attn-style
        layout = worker._layout_for(fused)
        assert layout is not None
        assert layout.kv_axis is None, "head axis misread as K/V"

    def test_odd_fused_width_is_refused_cleanly(self):
        """An odd fused row cannot be halved. No shipped backend emits one
        (turboquant explicitly rounds up to even), but the refusal must name
        the cause instead of failing inside a reshape."""
        worker = make_bare_worker(block_size=self.BS)
        cache = torch.zeros(self.NB, self.BS, 577)
        meta = make_flash_attn_metadata()

        with pytest.raises(RuntimeError, match="odd fused"):
            worker._extract_kv_from_layer(cache, torch.arange(2), meta, "L0")
        with pytest.raises(RuntimeError, match="odd fused"):
            worker._inject_kv_into_layer(
                cache, torch.zeros(2, 2, 288), torch.arange(2), meta, "L0"
            )

    def _make_triton_metadata(self):
        from vllm.v1.attention.backends.triton_attn import TritonAttentionMetadata

        meta = MagicMock()
        meta.__class__ = type("TritonMeta", (TritonAttentionMetadata,), {})
        return meta

    def test_triton_rank5_takes_the_layout_branch(self):
        """triton_attn is rank-5 (NB, 2, BS, NH, HS) on current vLLM. The
        legacy Triton branch assumed rank-4 with heads at dim 1, so its inject
        raised a shape mismatch on the K/V axis — and its extract was
        token-major, unlike every other backend's slab bytes."""
        kv = torch.zeros(self.NB, 2, self.BS, self.NH, self.HS)
        worker = make_bare_worker(
            block_size=self.BS, kv_caches={"L0": kv}, head_size=self.HS
        )
        meta = self._make_triton_metadata()
        slots = torch.cat(
            [torch.arange(self.BS), torch.arange(3 * self.BS, 4 * self.BS)]
        )
        src = torch.randn(2, slots.shape[0], self.NH * self.HS)

        worker._inject_kv_into_layer(kv, src, slots, meta, "L0")
        got = worker._extract_kv_from_layer(kv, slots, meta, "L0")

        torch.testing.assert_close(got, src)

    def test_triton_rank4_legacy_branch_kept(self):
        """Old rank-4 Triton (NB, NH, BS, HD) keeps its dedicated branch."""
        kv = torch.zeros(self.NB, self.NH, self.BS, self.HS)
        worker = make_bare_worker(block_size=self.BS)
        meta = self._make_triton_metadata()
        slots = torch.arange(self.BS)
        src = torch.randn(self.BS, self.NH, self.HS)

        worker._inject_kv_into_layer(kv, src, slots, meta, "L0")
        got = worker._extract_kv_from_layer(kv, slots, meta, "L0")

        torch.testing.assert_close(got, src)


# =============================================================================
# MaruSchedulerConnector._count_matched_chunks (mocked handler)
# =============================================================================


class TestCountMatchedChunks:
    def _make_scheduler(self, chunk_tokens=256):
        from maru_vllm.connector import MaruSchedulerConnector

        sched = MaruSchedulerConnector.__new__(MaruSchedulerConnector)
        sched._block_size = 16
        sched._kv_chunk_tokens = chunk_tokens
        sched._extra_config = {}
        sched._handler = MagicMock()
        sched._known_keys = set()
        sched._timing = False
        sched._use_layerwise = False
        return sched

    def test_all_cached(self):
        sched = self._make_scheduler(chunk_tokens=256)
        token_ids = list(range(512))  # 2 chunks
        sched._handler.batch_exists.return_value = [True, True]

        result = sched._count_matched_chunks(token_ids)
        assert result == 2

    def test_partial_cache(self):
        sched = self._make_scheduler(chunk_tokens=256)
        token_ids = list(range(768))  # 3 chunks
        sched._handler.batch_exists.return_value = [True, False, False]

        result = sched._count_matched_chunks(token_ids)
        assert result == 1

    def test_no_cache(self):
        sched = self._make_scheduler(chunk_tokens=256)
        token_ids = list(range(512))
        sched._handler.batch_exists.return_value = [False, False]

        result = sched._count_matched_chunks(token_ids)
        assert result == 0

    def test_local_cache_avoids_rpc(self):
        sched = self._make_scheduler(chunk_tokens=256)
        token_ids = list(range(256))  # 1 chunk
        keys = _chunk_keys(token_ids, 256)
        # Packed default: the chunk's own key is the completion marker.
        sched._known_keys.add(sched._chunk_exists_key(keys[0]))

        result = sched._count_matched_chunks(token_ids)
        assert result == 1
        sched._handler.batch_exists.assert_not_called()

    def test_empty_tokens(self):
        sched = self._make_scheduler(chunk_tokens=256)
        assert sched._count_matched_chunks([]) == 0


# =============================================================================
# Chunked-prefill store: fragmented (non-chunk-aligned) step boundaries
# =============================================================================


class TestChunkedPrefillFragmentedStore:
    """Regression tests for the inflight-8 external-hit loss.

    When concurrent prefills share the token budget, per-request step
    boundaries are not multiples of kv_chunk_tokens. The store path must
    still (a) store every full chunk exactly once (no chunk silently
    skipped at a step boundary) and (b) extract each chunk's KV from the
    slots of the tokens the chunk key represents (no shifted-slot
    corruption). See design note
    20260713_direct-connector-perf-parity-redesign.
    """

    BLOCK = 4
    CHUNK = 8
    PROMPT = 64  # 8 chunks, 16 blocks

    def _make_worker(self):
        worker = make_worker(
            block_size=self.BLOCK,
            kv_chunk_tokens=self.CHUNK,
            extra_config={"maru_use_layerwise": True},
        )
        stored = attach_capturing_handler(
            worker, capture=capture_float32, min_alloc_bytes=4
        )

        def _store(key, handle=None):
            stored[key] = "DONE"
            return True

        worker._handler.store.side_effect = _store
        worker._num_layers = 1
        return worker, stored

    def _kv_layer(self, layout="legacy"):
        # K value at slot s is s, V value is 1000 + s, so stored bytes
        # reveal exactly which token positions were extracted. Same logical
        # data in both paged axis orders — the pipeline must not care.
        k = torch.arange(64, dtype=torch.float32)
        if layout == "legacy":
            # (2, NB, BS, H) — the pre-0.23 order these tests always used.
            kv = torch.empty(2, 16, self.BLOCK, 1)
            kv[0] = k.reshape(16, self.BLOCK, 1)
            kv[1] = 1000 + k.reshape(16, self.BLOCK, 1)
        else:
            # (NB, 2, BS, NH, HS) — vLLM 0.23+ flash_attn.
            kv = torch.empty(16, 2, self.BLOCK, 1, 1)
            kv[:, 0] = k.reshape(16, self.BLOCK, 1, 1)
            kv[:, 1] = 1000 + k.reshape(16, self.BLOCK, 1, 1)
        return kv

    def _run_steps(self, worker, steps, layout="legacy"):
        kv = self._kv_layer(layout)
        attn = make_flash_attn_metadata()
        token_ids = list(range(self.PROMPT))
        for block_ids, sched, computed in steps:
            metadata = store_metadata(
                token_ids=token_ids,
                block_ids=block_ids,
                num_scheduled_tokens=sched,
                num_computed_tokens=computed,
            )
            worker.save_kv_layer("model.layers.0.self_attn", kv, attn, metadata)
        return token_ids

    @pytest.mark.parametrize("layout", ["legacy", "vllm023"])
    def test_fragmented_steps_store_all_chunks_with_correct_data(self, layout):
        """Boundaries 20/40 are not chunk-aligned; no chunk may be lost."""
        worker, stored = self._make_worker()
        # block_ids accumulate from token 0, as the fixed scheduler builds
        # them for continuation steps.
        token_ids = self._run_steps(
            worker,
            [
                (list(range(0, 5)), 20, 0),
                (list(range(0, 10)), 20, 20),
                (list(range(0, 16)), 24, 40),
            ],
            layout,
        )

        chunk_keys = _chunk_keys(token_ids, self.CHUNK)
        assert len(chunk_keys) == 8
        for ci, base_key in enumerate(chunk_keys):
            data = stored.get(f"{base_key}_L0")
            assert data is not None, f"chunk {ci} was never stored"
            assert stored.get(f"{base_key}_DONE") == "DONE", (
                f"chunk {ci} has no _DONE marker"
            )
            expected_k = [float(ci * self.CHUNK + i) for i in range(self.CHUNK)]
            assert data[: self.CHUNK].tolist() == expected_k, (
                f"chunk {ci} stored data from wrong slots"
            )

        sched = make_scheduler(
            block_size=self.BLOCK,
            kv_chunk_tokens=self.CHUNK,
            extra_config={"maru_use_layerwise": True},
        )
        sched._handler = MagicMock()
        sched._handler.batch_exists.side_effect = lambda keys: [
            k in stored for k in keys
        ]
        assert sched._count_matched_chunks(token_ids) == 8

    def test_aligned_steps_unchanged(self):
        """Chunk-aligned boundaries (the inflight-1 shape) keep working."""
        worker, stored = self._make_worker()
        token_ids = self._run_steps(
            worker,
            [
                (list(range(0, 8)), 32, 0),
                (list(range(0, 16)), 32, 32),
            ],
        )
        chunk_keys = _chunk_keys(token_ids, self.CHUNK)
        for ci, base_key in enumerate(chunk_keys):
            data = stored.get(f"{base_key}_L0")
            assert data is not None
            expected_k = [float(ci * self.CHUNK + i) for i in range(self.CHUNK)]
            assert data[: self.CHUNK].tolist() == expected_k

    def test_insufficient_blocks_skips_step_without_corruption(self):
        """If block_ids cannot cover the chunk range, skip loudly, not shift."""
        worker, stored = self._make_worker()
        # Continuation step with only per-step new blocks (the pre-fix
        # scheduler shape): 5 blocks cover 20 tokens but chunks up to
        # index 5 need 40.
        self._run_steps(worker, [(list(range(5, 10)), 20, 20)])
        assert not stored, "step with insufficient block coverage must store nothing"


class TestStoreLoadRoundtripModernLayout:
    """Full public-API loop at the vLLM 0.23 shape (NB, 2, BS, NH, HS).

    The extract/inject units are covered per layout, but the pipeline
    between them — chunk keys, slab slicing, the per-layer load fallback —
    only ever ran under the legacy (2, NB, BS, H) shape in tests, which is
    how a broken inject shipped. save_kv_layer writes into a dict store and
    start_load_kv reads it back into a zeroed cache of the same shape; the
    caches must match bit for bit.
    """

    BLOCK, CHUNK, PROMPT = 4, 8, 64

    def _modern_cache(self):
        kv = torch.empty(16, 2, self.BLOCK, 1, 1)
        k = torch.arange(64, dtype=torch.float32)
        kv[:, 0] = k.reshape(16, self.BLOCK, 1, 1)
        kv[:, 1] = 1000 + k.reshape(16, self.BLOCK, 1, 1)
        return kv

    def _make_worker(self):
        return make_worker(
            block_size=self.BLOCK,
            kv_chunk_tokens=self.CHUNK,
            extra_config={"maru_use_layerwise": True},
        )

    def test_roundtrip(self):
        from types import SimpleNamespace

        # Store round: save every chunk of the prompt.
        src = self._modern_cache()
        writer = self._make_worker()
        stored = attach_capturing_handler(writer, min_alloc_bytes=4)
        writer._handler.store.side_effect = lambda key, handle=None: (
            stored.__setitem__(key, "DONE") or True
        )
        writer._num_layers = 1
        token_ids = list(range(self.PROMPT))
        metadata = store_metadata(
            token_ids=token_ids,
            block_ids=list(range(16)),
            num_scheduled_tokens=self.PROMPT,
            num_computed_tokens=0,
        )
        writer.save_kv_layer(
            "model.layers.0.self_attn", src, make_flash_attn_metadata(), metadata
        )
        chunk_keys = _chunk_keys(token_ids, self.CHUNK)
        assert all(f"{k}_L0" in stored for k in chunk_keys)

        # Load round: a second engine with a zeroed cache of the same shape
        # reads the shared pool back.
        dst = torch.zeros_like(src)
        reader = self._make_worker()
        reader._handler = MagicMock()
        reader._handler.batch_retrieve.side_effect = lambda keys: [
            SimpleNamespace(view=memoryview(stored[k]), region_id=0, page_index=i)
            for i, k in enumerate(keys)
        ]
        reader._num_layers = 1
        fwd = SimpleNamespace(
            no_compile_layers={
                "model.layers.0.self_attn": SimpleNamespace(kv_cache=dst)
            },
            attn_metadata=None,
        )
        reader.start_load_kv(
            fwd,
            deferred_metadata(
                token_ids=token_ids,
                block_ids=list(range(16)),
                num_matched_chunks=len(chunk_keys),
            ),
        )
        assert reader.get_finished_loading() == {"r1"}
        assert reader.take_failed_load_blocks() == set()

        torch.testing.assert_close(dst, src)


# =============================================================================
# build_connector_meta: store-side block accumulation across steps
# =============================================================================


class TestBuildConnectorMetaStoreAccumulation:
    CHUNK = 8

    def _make_scheduler(self):
        return make_scheduler(block_size=4, kv_chunk_tokens=self.CHUNK)

    @staticmethod
    def _output(new_reqs, cached, num_scheduled):
        return fake_scheduler_output(new_reqs, cached, num_scheduled)

    @staticmethod
    def _cached(req_ids, new_block_ids, num_computed, resumed=()):
        return fake_cached_reqs(req_ids, new_block_ids, num_computed, resumed)

    def test_continuation_meta_carries_accumulated_blocks(self):
        sched = self._make_scheduler()
        token_ids = list(range(64))

        new_req = fake_new_request_data(
            prompt_token_ids=token_ids, block_ids=[0, 1, 2, 3, 4]
        )
        meta1 = sched.build_connector_meta(
            self._output([new_req], self._cached([], [], []), {"r1": 20})
        )
        assert meta1.requests[0].block_ids == [0, 1, 2, 3, 4]

        meta2 = sched.build_connector_meta(
            self._output(
                [], self._cached(["r1"], [([5, 6, 7, 8, 9],)], [20]), {"r1": 20}
            )
        )
        req = meta2.requests[0]
        assert req.is_store
        assert req.block_ids == list(range(10))
        assert req.num_computed_tokens == 20
        assert req.num_scheduled_tokens == 20

        meta3 = sched.build_connector_meta(
            self._output(
                [],
                self._cached(["r1"], [(list(range(10, 16)),)], [40]),
                {"r1": 24},
            )
        )
        assert meta3.requests[0].block_ids == list(range(16))
        # 64 tokens complete -> tracking dropped
        assert "r1" not in sched._requests_need_store

    def test_continuation_step_without_new_blocks_still_stores(self):
        sched = self._make_scheduler()
        token_ids = list(range(64))
        new_req = fake_new_request_data(
            prompt_token_ids=token_ids, block_ids=list(range(15))
        )
        sched.build_connector_meta(
            self._output([new_req], self._cached([], [], []), {"r1": 58})
        )

        meta = sched.build_connector_meta(
            self._output([], self._cached(["r1"], [None], [58]), {"r1": 2})
        )
        assert len(meta.requests) == 1
        req = meta.requests[0]
        assert req.is_store
        assert req.block_ids == list(range(15))
        assert req.num_computed_tokens == 58

    def test_preempted_ids_are_forwarded_for_store_stream_safety(self):
        sched = self._make_scheduler()
        output = self._output([], self._cached([], [], []), {})
        output.preempted_req_ids = {"preempted-1", "preempted-2"}

        meta = sched.build_connector_meta(output)

        assert meta.preempted_req_ids == {"preempted-1", "preempted-2"}

    @pytest.mark.parametrize("enabled", [False, True])
    def test_request_finished_transfers_block_ownership_only_for_write_behind(
        self, enabled
    ):
        sched = make_scheduler(
            block_size=4,
            kv_chunk_tokens=self.CHUNK,
            extra_config={"maru_enable_write_behind": enabled},
        )

        owns_blocks, params = sched.request_finished(MagicMock(), [1, 2])

        assert owns_blocks is enabled
        assert params is None


# =============================================================================
# Deferred (between-step) loading
# =============================================================================


class TestDeferredLoading:
    CHUNK = 8

    def _make_scheduler(self, deferred=True, layerwise_overlap=False):
        sched = make_scheduler(
            block_size=4,
            kv_chunk_tokens=self.CHUNK,
            extra_config={
                "maru_enable_deferred_loading": deferred,
                "maru_enable_layerwise_overlap": layerwise_overlap,
            },
        )
        sched._handler = MagicMock()
        return sched

    def _request(self, num_tokens=64):
        return SimpleNamespace(
            request_id="r1", prompt_token_ids=list(range(num_tokens))
        )

    def test_matched_tokens_reported_async(self):
        sched = self._make_scheduler()
        sched._handler.batch_exists.return_value = [True] * 8
        matched, load_async = sched.get_num_new_matched_tokens(self._request(), 0)
        assert matched == 64 and load_async is True

    def test_sync_mode_reports_sync(self):
        sched = self._make_scheduler(deferred=False)
        sched._handler.batch_exists.return_value = [True] * 8
        _, load_async = sched.get_num_new_matched_tokens(self._request(), 0)
        assert load_async is False

    def test_deferred_meta_emitted_once_with_alloc_blocks(self):
        sched = self._make_scheduler()
        sched._handler.batch_exists.return_value = [True] * 8
        request = self._request()
        sched.get_num_new_matched_tokens(request, 0)

        blocks = MagicMock()
        blocks.get_block_ids.return_value = ([3, 4, 5],)
        sched.update_state_after_alloc(request, blocks, 64)

        out = fake_scheduler_output()
        meta = sched.build_connector_meta(out)
        assert len(meta.requests) == 1
        req = meta.requests[0]
        assert req.deferred_load and not req.is_store
        assert req.block_ids == [3, 4, 5]
        assert req.num_matched_chunks == 8
        # Emitted exactly once
        assert sched.build_connector_meta(out).requests == []

    def test_second_alloc_call_after_load_is_ignored(self):
        sched = self._make_scheduler()
        sched.update_state_after_alloc(self._request(), MagicMock(), 0)
        assert sched._pending_deferred_loads == {}

    def test_second_alloc_activates_packed_layerwise_load_once(self):
        sched = self._make_scheduler(layerwise_overlap=True)
        request = self._request()
        sched._last_match_result[request.request_id] = 8
        blocks = MagicMock()
        blocks.get_block_ids.return_value = ([3, 4, 5],)
        sched.update_state_after_alloc(request, blocks, 64)

        empty = fake_scheduler_output()
        first = sched.build_connector_meta(empty)
        assert first.requests[0].deferred_load
        assert first.requests[0].layerwise_load
        assert first.layerwise_load_req_ids == set()

        sched.update_state_after_alloc(request, MagicMock(), 0)
        resumed = fake_new_request_data(
            prompt_token_ids=request.prompt_token_ids, block_ids=[3, 4, 5]
        )
        output = fake_scheduler_output(
            [resumed], empty.scheduled_cached_reqs, {"r1": 1}
        )

        second = sched.build_connector_meta(output)
        assert second.layerwise_load_req_ids == {"r1"}
        assert sched.build_connector_meta(empty).layerwise_load_req_ids == set()

    def test_concurrent_deferred_batch_keeps_whole_request_dma(self):
        sched = self._make_scheduler(layerwise_overlap=True)
        for req_id in ("r1", "r2"):
            request = SimpleNamespace(
                request_id=req_id,
                prompt_token_ids=list(range(64)),
            )
            sched._last_match_result[req_id] = 8
            blocks = MagicMock()
            blocks.get_block_ids.return_value = ([3, 4, 5],)
            sched.update_state_after_alloc(request, blocks, 64)

        output = fake_scheduler_output()
        metadata = sched.build_connector_meta(output)

        assert len(metadata.requests) == 2
        assert not any(request.layerwise_load for request in metadata.requests)
        assert sched._deferred_layerwise_waiting == set()

    def test_staggered_admission_sees_live_deferred_concurrency(self):
        sched = self._make_scheduler(layerwise_overlap=True)
        output = fake_scheduler_output()

        first_request = self._request()
        sched._last_match_result[first_request.request_id] = 8
        first_blocks = MagicMock()
        first_blocks.get_block_ids.return_value = ([3, 4, 5],)
        sched.update_state_after_alloc(first_request, first_blocks, 64)
        first = sched.build_connector_meta(output)
        assert first.requests[0].layerwise_load

        second_request = SimpleNamespace(
            request_id="r2",
            prompt_token_ids=list(range(64)),
        )
        sched._last_match_result[second_request.request_id] = 8
        second_blocks = MagicMock()
        second_blocks.get_block_ids.return_value = ([6, 7, 8],)
        sched.update_state_after_alloc(second_request, second_blocks, 64)
        second = sched.build_connector_meta(output)

        assert not second.requests[0].layerwise_load
        assert sched._active_deferred_req_ids == {"r1", "r2"}

        finished = SimpleNamespace(**vars(output))
        finished.finished_req_ids = {"r1"}
        sched.build_connector_meta(finished)
        assert sched._active_deferred_req_ids == {"r2"}

    def test_failed_deferred_load_reports_blocks_and_completion(self):
        worker = make_worker(block_size=4, kv_chunk_tokens=self.CHUNK)
        meta = deferred_req_meta(
            token_ids=list(range(64)),
            block_ids=[7, 8, 9],
            num_matched_chunks=8,
        )
        worker._fail_deferred_load(meta)
        assert worker.get_finished_loading() == {"r1"}
        assert worker.get_finished_loading() is None
        assert worker.take_failed_load_blocks() == {7, 8, 9}
        assert worker.take_failed_load_blocks() == set()

    def test_cpu_deferred_load_completes_synchronously(self):
        worker = make_worker(block_size=4, kv_chunk_tokens=self.CHUNK)
        kv = torch.zeros(2, 16, 4, 1)
        chunk_bytes = 2 * self.CHUNK * 4  # K/V x tokens x fp32
        infos = [
            SimpleNamespace(
                view=memoryview(bytearray([layer + 1] * chunk_bytes)),
                region_id=0,
                page_index=i,
            )
            for layer in range(1)
            for i in range(8)
        ]
        worker._handler = MagicMock()
        worker._handler.batch_retrieve.return_value = infos
        worker._num_layers = 1

        layer = SimpleNamespace(kv_cache=kv)
        fwd = SimpleNamespace(no_compile_layers={"l0": layer}, attn_metadata=None)
        metadata = deferred_metadata(
            token_ids=list(range(64)),
            block_ids=list(range(16)),
            num_matched_chunks=8,
        )
        worker.start_load_kv(fwd, metadata)
        assert worker.get_finished_loading() == {"r1"}
        assert worker.take_failed_load_blocks() == set()
        assert kv.abs().sum() > 0  # KV was actually injected

    def test_load_failure_recomputes_instead_of_raising(self):
        """An inject refusal during load must never escape start_load_kv.

        vLLM calls start_load_kv outside its own try, so a raise here kills
        the engine. The request's blocks must go through the failed-blocks
        path (get_block_ids_with_load_errors) so vLLM recomputes them, and
        the request must still unpark via get_finished_loading.
        """
        worker = make_worker(block_size=4, kv_chunk_tokens=self.CHUNK)
        chunk_bytes = 2 * self.CHUNK * 4
        infos = [
            SimpleNamespace(
                view=memoryview(bytearray(chunk_bytes)), region_id=0, page_index=i
            )
            for i in range(8)
        ]
        worker._handler = MagicMock()
        worker._handler.batch_retrieve.return_value = infos
        worker._num_layers = 1

        bad_cache = torch.zeros(3, 5)  # unrecognizable layout: inject refuses
        fwd = SimpleNamespace(
            no_compile_layers={"l0": SimpleNamespace(kv_cache=bad_cache)},
            attn_metadata=None,
        )
        metadata = deferred_metadata(
            token_ids=list(range(64)),
            block_ids=list(range(16)),
            num_matched_chunks=8,
        )

        worker.start_load_kv(fwd, metadata)  # must not raise
        assert worker.get_finished_loading() == {"r1"}
        assert worker.take_failed_load_blocks() == set(range(16))


class TestAsyncDeferredPackedLoad:
    """True-async deferred loads: retrieve + H2D run on the loader thread."""

    CHUNK = 8

    def _make_worker(self):
        return make_worker(block_size=4, kv_chunk_tokens=self.CHUNK)

    def _deferred_meta(self):
        return deferred_req_meta(
            token_ids=list(range(64)),
            block_ids=list(range(16)),
            num_matched_chunks=8,
        )

    def _poll_finished(self, worker, timeout_s=5.0):
        import time as _time

        deadline = _time.monotonic() + timeout_s
        done: set = set()
        while _time.monotonic() < deadline:
            finished = worker.get_finished_loading()
            if finished:
                done |= finished
                return done
            _time.sleep(0.01)
        return done

    def test_submit_refused_without_registered_caches(self):
        worker = self._make_worker()
        assert worker._try_submit_deferred_packed_load(self._deferred_meta()) is False

    def test_submit_refused_on_cpu_caches(self):
        worker = self._make_worker()
        worker._kv_caches = {"l0": torch.zeros(2, 16, 4, 1)}
        worker._num_layers = 1
        assert worker._try_submit_deferred_packed_load(self._deferred_meta()) is False

    def test_layerwise_mode_background_job_stops_after_retrieve(self):
        worker = make_worker(
            block_size=4,
            kv_chunk_tokens=self.CHUNK,
            extra_config={
                "maru_enable_deferred_loading": True,
                "maru_enable_layerwise_overlap": True,
            },
        )
        worker._num_layers = 1
        slab_bytes = 2 * self.CHUNK * 4
        infos = [
            SimpleNamespace(
                view=memoryview(bytearray([i + 1] * slab_bytes)),
                region_id=0,
                page_index=i,
            )
            for i in range(8)
        ]
        worker._handler = MagicMock()
        worker._handler.batch_retrieve.return_value = infos
        meta = self._deferred_meta()
        meta.layerwise_load = True

        worker._deferred_packed_load_job(
            meta,
            [("l0", torch.zeros(2, 16, 4, 1), 0)],
            torch.device("cpu"),
        )

        assert worker.get_finished_loading() == {"r1"}
        retained = worker._deferred_layerwise_loads["r1"]
        assert retained[0] is meta
        assert retained[3] == infos
        assert worker._deferred_events == {}

    def test_layerwise_activation_cpu_fallback_preserves_packed_layout(self):
        from maru_vllm.connector import MaruConnectorMetadata

        worker = make_worker(
            block_size=4,
            kv_chunk_tokens=4,
            extra_config={
                "maru_enable_deferred_loading": True,
                "maru_enable_layerwise_overlap": True,
            },
        )
        worker._handler = MagicMock()
        worker._num_layers = 2
        source = torch.arange(2 * 2 * 4, dtype=torch.float32).view(2, 2, 4, 1)
        info = SimpleNamespace(
            view=memoryview(bytearray(source.numpy().tobytes())),
            region_id=0,
            page_index=0,
        )
        meta = self._deferred_meta()
        meta.num_matched_chunks = 1
        meta.block_ids = [0]
        worker._deferred_layerwise_loads["r1"] = (
            meta,
            1,
            torch.arange(4),
            [info],
        )
        kv0 = torch.zeros(2, 1, 4, 1)
        kv1 = torch.zeros_like(kv0)
        forward = SimpleNamespace(
            no_compile_layers={
                "model.layers.0.self_attn": SimpleNamespace(kv_cache=kv0),
                "model.layers.1.self_attn": SimpleNamespace(kv_cache=kv1),
            },
            attn_metadata=None,
        )

        worker.start_load_kv(
            forward,
            MaruConnectorMetadata(layerwise_load_req_ids={"r1"}),
        )

        torch.testing.assert_close(kv0, source[:, 0].view_as(kv0))
        torch.testing.assert_close(kv1, source[:, 1].view_as(kv1))
        assert worker._deferred_layerwise_loads == {}
        assert worker._layer_load_events == {}

    @pytest.mark.skipif(not torch.cuda.is_available(), reason="requires CUDA")
    def test_layerwise_activation_records_one_event_per_layer(self):
        from maru_vllm.connector import MaruConnectorMetadata

        worker = make_worker(
            block_size=4,
            kv_chunk_tokens=4,
            extra_config={
                "maru_enable_deferred_loading": True,
                "maru_enable_layerwise_overlap": True,
            },
        )
        worker._handler = MagicMock()
        worker._num_layers = 2
        source = torch.arange(2 * 2 * 2 * 4, dtype=torch.float32).view(2, 2, 2, 4, 1)
        region = bytearray(source.numpy().tobytes())
        page_bytes = source[0].numel() * source.element_size()
        infos = [
            SimpleNamespace(
                view=memoryview(region)[i * page_bytes : (i + 1) * page_bytes],
                region_id=0,
                page_index=i,
            )
            for i in range(2)
        ]
        worker._effective_page_size_bytes = page_bytes
        meta = self._deferred_meta()
        meta.num_matched_chunks = 2
        meta.block_ids = [0, 1]
        worker._deferred_layerwise_loads["r1"] = (
            meta,
            2,
            torch.arange(8),
            infos,
        )
        kv0 = torch.zeros(2, 2, 4, 1, device="cuda")
        kv1 = torch.zeros_like(kv0)
        forward = SimpleNamespace(
            no_compile_layers={
                "model.layers.0.self_attn": SimpleNamespace(kv_cache=kv0),
                "model.layers.1.self_attn": SimpleNamespace(kv_cache=kv1),
            },
            attn_metadata=None,
        )

        worker.start_load_kv(
            forward,
            MaruConnectorMetadata(layerwise_load_req_ids={"r1"}),
        )

        assert set(worker._layer_load_events) == {
            "model.layers.0.self_attn",
            "model.layers.1.self_attn",
        }
        worker.wait_for_layer_load("model.layers.0.self_attn")
        worker.wait_for_layer_load("model.layers.1.self_attn")
        torch.cuda.synchronize()
        torch.testing.assert_close(kv0.cpu(), source[:, :, 0].permute(1, 0, 2, 3))
        torch.testing.assert_close(kv1.cpu(), source[:, :, 1].permute(1, 0, 2, 3))

    def test_preemption_discards_retained_layerwise_load(self):
        from maru_vllm.connector import MaruConnectorMetadata

        worker = make_worker(
            block_size=4,
            kv_chunk_tokens=self.CHUNK,
            extra_config={
                "maru_enable_deferred_loading": True,
                "maru_enable_layerwise_overlap": True,
            },
        )
        worker._deferred_layerwise_loads["r1"] = MagicMock()

        worker.handle_preemptions(MaruConnectorMetadata(preempted_req_ids={"r1"}))

        assert worker._deferred_layerwise_loads == {}

    @pytest.mark.skipif(not torch.cuda.is_available(), reason="requires CUDA")
    def test_offthread_load_completes_via_event(self):
        worker = self._make_worker()
        # 4-dim layer keeps _packed_load_kernel_ctx unusable (dim != 5), so
        # the job takes the per-layer inject fallback that plain host
        # bytearrays support.
        kv = torch.zeros(2, 16, 4, 1, device="cuda")
        worker._kv_caches = {"l0": kv}
        worker._num_layers = 1
        slab_bytes = 2 * 1 * self.CHUNK * 1 * 4  # K/V x layers x tokens x fp32
        infos = [
            SimpleNamespace(
                view=memoryview(bytearray([i + 1] * slab_bytes)),
                region_id=0,
                page_index=i,
            )
            for i in range(8)
        ]
        worker._handler = MagicMock()
        worker._handler.batch_retrieve.return_value = infos

        assert worker._try_submit_deferred_packed_load(self._deferred_meta()) is True
        assert self._poll_finished(worker) == {"r1"}
        assert worker.take_failed_load_blocks() == set()
        torch.cuda.synchronize()
        assert kv.abs().sum() > 0  # KV was actually injected

    @pytest.mark.skipif(not torch.cuda.is_available(), reason="requires CUDA")
    def test_offthread_load_retains_pinned_slot_source_until_event(self):
        worker = self._make_worker()
        kv = torch.zeros(2, 16, 4, 1, device="cuda")
        worker._kv_caches = {"l0": kv}
        worker._num_layers = 1
        slab_bytes = 2 * self.CHUNK * 4
        infos = [
            SimpleNamespace(
                view=memoryview(bytearray([i + 1] * slab_bytes)),
                region_id=0,
                page_index=i,
            )
            for i in range(8)
        ]
        worker._handler = MagicMock()
        worker._handler.batch_retrieve.return_value = infos
        meta = self._deferred_meta()

        worker._deferred_packed_load_job(
            meta,
            [("l0", kv, 0)],
            kv.device,
        )

        refs = worker._deferred_refs["r1"]
        assert refs[0] == infos
        assert refs[1].device.type == "cpu"
        assert refs[1].is_pinned()
        assert refs[2].device.type == "cuda"
        torch.cuda.synchronize()
        assert worker.get_finished_loading() == {"r1"}

    @pytest.mark.skipif(not torch.cuda.is_available(), reason="requires CUDA")
    def test_offthread_retrieve_failure_reports_blocks(self):
        worker = self._make_worker()
        worker._kv_caches = {"l0": torch.zeros(2, 16, 4, 1, device="cuda")}
        worker._num_layers = 1
        worker._handler = MagicMock()
        worker._handler.batch_retrieve.side_effect = RuntimeError("rpc down")

        assert worker._try_submit_deferred_packed_load(self._deferred_meta()) is True
        assert self._poll_finished(worker) == {"r1"}
        assert worker.take_failed_load_blocks() == set(range(16))

    @pytest.mark.skipif(not torch.cuda.is_available(), reason="requires CUDA")
    def test_offthread_miss_reports_blocks(self):
        worker = self._make_worker()
        worker._kv_caches = {"l0": torch.zeros(2, 16, 4, 1, device="cuda")}
        worker._num_layers = 1
        worker._handler = MagicMock()
        worker._handler.batch_retrieve.return_value = [None] * 8

        assert worker._try_submit_deferred_packed_load(self._deferred_meta()) is True
        assert self._poll_finished(worker) == {"r1"}
        assert worker.take_failed_load_blocks() == set(range(16))


class _FakeAdmissionEvent:
    """Stand-in CUDA event: query() flips true after synchronize()."""

    def __init__(self, complete: bool = False):
        self.complete = complete
        self.sync_calls = 0

    def query(self) -> bool:
        return self.complete

    def synchronize(self) -> None:
        self.sync_calls += 1
        self.complete = True


class TestLoadAdmissionWindow:
    """Enqueue-time admission: bound in-flight deferred loads."""

    def _make_worker(self, window):
        return make_worker(
            block_size=4,
            kv_chunk_tokens=8,
            extra_config={"maru_load_admission_window": window},
        )

    def test_window_disabled_by_default(self):
        worker = make_worker(block_size=4, kv_chunk_tokens=8)
        assert worker._load_admission_window == 0

    def test_window_parsed_from_extra_config(self):
        assert self._make_worker(2)._load_admission_window == 2
        assert self._make_worker("1")._load_admission_window == 1

    def test_gate_waits_on_oldest_incomplete_load(self):
        worker = self._make_worker(1)
        oldest = _FakeAdmissionEvent()
        newer = _FakeAdmissionEvent()
        worker._deferred_events = {"r1": oldest, "r2": newer}

        worker._wait_for_load_admission("r3")

        assert oldest.sync_calls == 1
        # After the oldest completes, one incomplete load (newer) remains —
        # still at the window, so the gate waits on it too before admitting.
        assert newer.sync_calls == 1

    def test_gate_passes_below_window_without_waiting(self):
        worker = self._make_worker(2)
        outstanding = _FakeAdmissionEvent()
        worker._deferred_events = {"r1": outstanding}

        worker._wait_for_load_admission("r2")

        assert outstanding.sync_calls == 0

    def test_gate_ignores_completed_events(self):
        worker = self._make_worker(1)
        done = _FakeAdmissionEvent(complete=True)
        worker._deferred_events = {"r1": done}

        worker._wait_for_load_admission("r2")

        assert done.sync_calls == 0

    def test_gate_noop_with_empty_stream(self):
        worker = self._make_worker(1)

        worker._wait_for_load_admission("r1")

        assert worker._deferred_events == {}


# =============================================================================
# Packed-store write-behind lifecycle
# =============================================================================


class TestWriteBehindStoreLifecycle:
    def _make_worker(self):
        worker = make_worker(
            block_size=4,
            kv_chunk_tokens=8,
            extra_config={"maru_enable_write_behind": True},
        )
        worker._handler = MagicMock()
        return worker

    @staticmethod
    def _reserve(worker, key="k1", req_ids=("r1",)):
        worker._pending_store_keys.add(key)
        worker._store_key_waiters[key] = set(req_ids)
        for req_id in req_ids:
            worker._request_pending_store_keys.setdefault(req_id, set()).add(key)

    def test_finished_request_waits_until_all_keys_complete(self):
        worker = self._make_worker()
        self._reserve(worker, "k1")
        self._reserve(worker, "k2")

        assert worker.get_finished_saving({"r1"}) is None
        worker._complete_write_behind_keys(["k1"], [True])
        assert worker.get_finished_saving(set()) is None
        worker._complete_write_behind_keys(["k2"], [True])

        assert worker.get_finished_saving(set()) == {"r1"}
        assert worker.get_finished_saving(set()) is None
        assert worker._stored_keys == {"k1", "k2"}

    def test_failure_releases_request_without_publishing_key(self):
        worker = self._make_worker()
        self._reserve(worker)

        assert worker.get_finished_saving({"r1"}) is None
        worker._complete_write_behind_keys(["k1"], [False])

        assert worker.get_finished_saving(set()) == {"r1"}
        assert "k1" not in worker._stored_keys
        assert worker._pending_store_keys == set()

    def test_shared_key_releases_all_waiters(self):
        worker = self._make_worker()
        self._reserve(worker, req_ids=("r1", "r2"))

        assert worker.get_finished_saving({"r1", "r2"}) is None
        worker._complete_write_behind_keys(["k1"], [True])

        assert worker.get_finished_saving(set()) == {"r1", "r2"}

    def test_background_registration_publishes_only_after_event(self):
        worker = self._make_worker()
        self._reserve(worker)
        worker._handler.batch_store.return_value = [True]
        event = MagicMock()
        refs = [object()]
        handle = object()

        worker._finish_write_behind_store(event, ["k1"], [handle], refs)

        event.synchronize.assert_called_once_with()
        worker._handler.batch_store.assert_called_once_with(["k1"], [handle])
        assert refs == []
        assert worker._stored_keys == {"k1"}

    def test_background_registration_exception_frees_handles(self):
        worker = self._make_worker()
        self._reserve(worker)
        worker._handler.batch_store.side_effect = RuntimeError("rpc down")
        event = MagicMock()
        handle = object()

        worker._finish_write_behind_store(event, ["k1"], [handle], [])

        worker._handler.free.assert_called_once_with(handle)
        assert worker._stored_keys == set()
        assert worker._pending_store_keys == set()

    def test_preemption_drains_store_stream_before_block_reuse(self):
        from maru_vllm.connector import MaruConnectorMetadata

        worker = self._make_worker()
        worker._store_stream = MagicMock()

        worker.handle_preemptions(
            MaruConnectorMetadata(preempted_req_ids={"preempted"})
        )

        worker._store_stream.synchronize.assert_called_once_with()

    def test_shutdown_launches_batches_skipped_by_final_get_finished(self):
        from maru_vllm.connector import MaruConnectorMetadata

        worker = self._make_worker()
        kernel = ("ops",) * 6
        metadata = MaruConnectorMetadata()
        worker._queued_store_batches = [(kernel, metadata)]
        worker._store_packed_slabs_write_behind = MagicMock()

        worker.shutdown()

        worker._store_packed_slabs_write_behind.assert_called_once_with(
            kernel, metadata
        )
        assert worker._queued_store_batches == []


# =============================================================================
# Fused UVA gather-scatter load (P5)
# =============================================================================


class TestResolveLmcOps:
    """_resolve_lmc_ops must bundle kernels+enums across LMCache generations:
    old builds carry everything on ``lmcache.c_ops``; newer upstream routes
    c_ops through a device-ops shim (kernels only) and relocates the enums
    into ``lmcache.lmcache_native``."""

    def _fake_c_ops(self, with_enums):
        import types as _types

        mod = _types.ModuleType("lmcache.c_ops")
        mod.multi_layer_kv_transfer = lambda *a, **k: None
        mod.single_layer_kv_transfer = lambda *a, **k: None
        if with_enums:
            mod.EngineKVFormat = SimpleNamespace(NL_X_TWO_NB_BS_NH_HS="F")
            mod.TransferDirection = SimpleNamespace(H2D="H2D", D2H="D2H")
        return mod

    def _fake_native(self):
        import types as _types

        mod = _types.ModuleType("lmcache.lmcache_native")
        mod.EngineKVFormat = SimpleNamespace(NL_X_TWO_NB_BS_NH_HS="F")
        mod.TransferDirection = SimpleNamespace(H2D="H2D", D2H="D2H")
        return mod

    def test_old_style_everything_on_c_ops(self, monkeypatch):
        import sys

        from maru_vllm.connector import _resolve_lmc_ops

        monkeypatch.setitem(sys.modules, "lmcache.c_ops", self._fake_c_ops(True))
        monkeypatch.setitem(sys.modules, "lmcache.lmcache_native", None)
        ops = _resolve_lmc_ops()
        assert ops is not None
        assert ops.TransferDirection.H2D == "H2D"

    def test_new_style_enums_from_lmcache_native(self, monkeypatch):
        import sys

        from maru_vllm.connector import _resolve_lmc_ops

        monkeypatch.setitem(sys.modules, "lmcache.c_ops", self._fake_c_ops(False))
        monkeypatch.setitem(sys.modules, "lmcache.lmcache_native", self._fake_native())
        ops = _resolve_lmc_ops()
        assert ops is not None
        assert ops.EngineKVFormat.NL_X_TWO_NB_BS_NH_HS == "F"
        assert callable(ops.multi_layer_kv_transfer)

    def test_enums_nowhere_returns_none(self, monkeypatch):
        import sys

        from maru_vllm.connector import _resolve_lmc_ops

        monkeypatch.setitem(sys.modules, "lmcache.c_ops", self._fake_c_ops(False))
        monkeypatch.setitem(sys.modules, "lmcache.lmcache_native", None)
        assert _resolve_lmc_ops() is None

    def test_kernels_missing_returns_none(self, monkeypatch):
        import sys
        import types as _types

        from maru_vllm.connector import _resolve_lmc_ops

        empty = _types.ModuleType("lmcache.c_ops")
        monkeypatch.setitem(sys.modules, "lmcache.c_ops", empty)
        monkeypatch.setitem(sys.modules, "lmcache.lmcache_native", self._fake_native())
        assert _resolve_lmc_ops() is None


class TestUseLmcacheKernelsFlag:
    """maru_use_lmcache_kernels=false must force the per-layer paths without
    ever attempting kernel resolution, regardless of what lmcache provides."""

    def _make_worker(self, **extra):
        from maru_vllm.connector import MaruWorkerConnector

        return MaruWorkerConnector(block_size=4, kv_chunk_tokens=8, extra_config=extra)

    def test_packed_kernel_ctx_skips_resolution_when_disabled(self):
        worker = self._make_worker(maru_use_lmcache_kernels=False)
        worker._kv_layout = SimpleNamespace(
            format_name="NL_X_NB_TWO_BS_NH_HS",
            block_size=4,
            head_size=2,
            page_buffer_size=64,
        )
        fake_layer = SimpleNamespace(
            device=SimpleNamespace(type="cuda"), data_ptr=lambda: 0x1000
        )
        layers = [("model.layers.0.self_attn", fake_layer, 0)]

        with patch("torch.cuda.is_available", return_value=True):
            ctx = worker._packed_load_kernel_ctx(layers, MagicMock())

        assert ctx is None
        assert worker._lmc_ops is None  # resolution never attempted

    def test_fused_load_disabled_when_kernels_off(self):
        worker = self._make_worker(
            maru_use_lmcache_kernels=False, maru_enable_fused_load=True
        )
        assert worker._ensure_fused_ops() is False
        assert worker._fused_load is False
        assert worker._lmc_ops is None

    def test_default_keeps_kernels_enabled(self):
        worker = self._make_worker()
        assert worker._use_lmc_kernels is True


class TestFusedLoad:
    CHUNK = 8

    def _make_worker(self, fused=True):
        return make_worker(
            block_size=4,
            kv_chunk_tokens=self.CHUNK,
            extra_config={"maru_enable_fused_load": fused},
        )

    def _fake_ops(self):
        ops = MagicMock()
        ops.TransferDirection.H2D = "H2D"
        # A real namespace, not a MagicMock: the connector probes formats with
        # getattr(..., None), which a MagicMock would satisfy for any name.
        ops.EngineKVFormat = SimpleNamespace(
            NL_X_TWO_NB_BS_NH_HS="FLASH",
            NL_X_NB_TWO_BS_NH_HS="FLASH023",
            NL_X_TWO_NB_NH_BS_HS="ROCM_HND",
            NL_X_NB_TWO_NH_BS_HS="FLASH_HND",
        )
        return ops

    def _hnd_cache(self, nb, bs, nh, hs):
        """An HND cache exactly as vLLM allocates one: physical (NB, 2, NH,
        BS, HS) storage permuted back to the logical (NB, 2, BS, NH, HS)
        shape (gpu_model_runner), so only the strides say HND."""
        order = (0, 1, 3, 2, 4)  # flash_attn get_kv_cache_stride_order() HND
        logical = (nb, 2, bs, nh, hs)
        phys = tuple(logical[i] for i in order)
        inv = [order.index(i) for i in range(5)]
        return torch.zeros(phys).permute(inv)

    def test_disabled_flag_skips_fused(self):
        worker = self._make_worker(fused=False)
        assert worker._ensure_fused_ops() is False

    def test_flash_meta_selects_fused_and_dict_meta_dispatches(self):
        worker = self._make_worker()
        worker._lmc_ops = self._fake_ops()
        flash_meta = MagicMock()
        flash_meta.__class__ = type("FlashMetadata", (), {})
        assert worker._use_fused_load(flash_meta, "l0") is True
        assert worker._use_fused_load({"l0": flash_meta}, "l0") is True

    @pytest.mark.parametrize(
        "shape,expected_format",
        [
            ((2, 4, 4, 1, 2), "FLASH"),  # legacy [2, NB, BS, NH, HS]
            ((4, 2, 4, 1, 2), "FLASH023"),  # vLLM 0.23+ [NB, 2, BS, NH, HS]
        ],
        ids=["legacy", "vllm023"],
    )
    def test_fused_run_transfer_calls_kernel_per_chunk(self, shape, expected_format):
        """The kernel format must follow the cache's actual axis order.

        The pre-fix code hardcoded NL_X_TWO_NB_BS_NH_HS, so on vLLM 0.23+ the
        kernel walked raw memory with the K/V and block axes swapped.
        """
        worker = self._make_worker()
        ops = self._fake_ops()
        worker._lmc_ops = ops
        # (chunk x layer) object: [2, 8 tokens, hidden=2] fp32 = 128 B
        obj_bytes = 2 * self.CHUNK * 2 * 4
        worker._chunk_object_bytes = lambda: obj_bytes
        kv_layer = torch.zeros(*shape)
        run_view = memoryview(bytearray(range(0, 256)))[: 2 * obj_bytes]
        slots = torch.arange(2 * self.CHUNK)

        ok = worker._fused_run_transfer(kv_layer, run_view, 2, slots)
        assert ok is True
        assert ops.single_layer_kv_transfer.call_count == 2
        args, kwargs = ops.single_layer_kv_transfer.call_args_list[1]
        src, dst, slot_arg = args[0], args[1], args[2]
        assert src.shape == (2, self.CHUNK, 2) and src.dtype == kv_layer.dtype
        assert dst is kv_layer
        assert slot_arg.tolist() == list(range(self.CHUNK, 2 * self.CHUNK))
        assert args[3] == "H2D" and args[4] == expected_format
        assert kwargs == {"token_major": False}

    def test_fused_refuses_layout_without_kernel_format(self):
        """rank-3 (sparse MLA) and fused caches carry no kernel format and
        must fall back to memcpy+inject, mirroring _packed_load_kernel_ctx."""
        worker = self._make_worker()
        worker._lmc_ops = self._fake_ops()
        worker._chunk_object_bytes = lambda: 128
        kv_layer = torch.zeros(4, 4, 4)  # rank-3, block_size=4

        ok = worker._fused_run_transfer(
            kv_layer, memoryview(bytearray(256)), 1, torch.arange(self.CHUNK)
        )
        assert ok is False
        assert worker._lmc_ops.single_layer_kv_transfer.call_count == 0

    def test_fused_refuses_missing_format_constant(self):
        """An older c_ops build without the needed constant falls back."""
        worker = self._make_worker()
        ops = self._fake_ops()
        del ops.EngineKVFormat.NL_X_NB_TWO_BS_NH_HS
        worker._lmc_ops = ops
        worker._chunk_object_bytes = lambda: 128
        kv_layer = torch.zeros(4, 2, 4, 1, 2)

        ok = worker._fused_run_transfer(
            kv_layer, memoryview(bytearray(256)), 1, torch.arange(self.CHUNK)
        )
        assert ok is False
        assert ops.single_layer_kv_transfer.call_count == 0

    def test_fused_survives_shim_ops_without_format_enum(self):
        """LMCache 0.4.8rc5+ exposes c_ops through a device-ops shim that has
        no EngineKVFormat attribute at all — the enum access itself raises
        AttributeError. The lookup must absorb that and fall back instead of
        letting the exception escape into the forward pass."""
        worker = self._make_worker()
        ops = self._fake_ops()
        del ops.EngineKVFormat  # MagicMock: access now raises AttributeError
        worker._lmc_ops = ops
        worker._chunk_object_bytes = lambda: 128
        kv_layer = torch.zeros(4, 2, 4, 1, 2)

        ok = worker._fused_run_transfer(
            kv_layer, memoryview(bytearray(256)), 1, torch.arange(self.CHUNK)
        )
        assert ok is False
        assert ops.single_layer_kv_transfer.call_count == 0

    def test_fused_refuses_non_contiguous_cache(self):
        """A cache with HND strides whose layout resolved to an NHD format
        (the case _vllm_kv_cache_layout warns about: physical layout
        misreported as NHD) gets no permute, and the kernel would read
        swapped head/token axes. Fall back instead."""
        worker = self._make_worker()
        worker._lmc_ops = self._fake_ops()
        worker._chunk_object_bytes = lambda: 128
        kv_layer = torch.zeros(4, 2, 2, 4, 2).permute(0, 1, 3, 2, 4)
        assert tuple(kv_layer.shape) == (4, 2, 4, 2, 2)
        assert not kv_layer.is_contiguous()

        ok = worker._fused_run_transfer(
            kv_layer, memoryview(bytearray(256)), 1, torch.arange(self.CHUNK)
        )
        assert ok is False
        assert worker._lmc_ops.single_layer_kv_transfer.call_count == 0

    @pytest.mark.parametrize("nh", [1, 2], ids=["nh1", "nh2"])
    def test_fused_hnd_cache_hands_kernel_the_physical_view(self, nh):
        """On HND the kernel reads NH from size(2) and BS from size(3) of the
        tensor it is handed (mem_kernels.cu positional reads), so the cache
        must arrive in physical (NB, 2, NH, BS, HS) order.

        nh=1 is the trap: torch.is_contiguous() ignores strides of size-1
        dims, so a contiguity guard passes the logical view through and the
        kernel swaps NH and BS. nh=1 is common — a model with as many KV
        heads as the TP degree runs one head per rank.
        """
        from maru_vllm.kv_layout import _detect_kv_layout

        hs = 2
        worker = self._make_worker()
        ops = self._fake_ops()
        worker._lmc_ops = ops
        obj_bytes = 2 * self.CHUNK * nh * hs * 4
        worker._chunk_object_bytes = lambda: obj_bytes
        kv_layer = self._hnd_cache(nb=4, bs=4, nh=nh, hs=hs)
        worker._kv_layout = _detect_kv_layout(
            tuple(kv_layer.shape), 4, "HND", num_kv_heads=nh, head_size=hs
        )
        assert worker._kv_layout.format_name == "NL_X_NB_TWO_NH_BS_HS"

        ok = worker._fused_run_transfer(
            kv_layer, memoryview(bytearray(obj_bytes)), 1, torch.arange(self.CHUNK)
        )
        assert ok is True
        args, _ = ops.single_layer_kv_transfer.call_args
        dst = args[1]
        assert (dst.size(2), dst.size(3)) == (nh, 4)  # physical NH, BS
        assert dst.is_contiguous()
        assert dst.data_ptr() == kv_layer.data_ptr()  # a view, not a copy
        assert args[4] == "FLASH_HND"

    def test_fused_hnd_cache_with_foreign_strides_falls_back(self):
        """An HND-format layout whose tensor is not a pure HND allocation
        (here: a strided slice) cannot be fixed by the permute; refuse it."""
        from maru_vllm.kv_layout import _detect_kv_layout

        worker = self._make_worker()
        worker._lmc_ops = self._fake_ops()
        worker._chunk_object_bytes = lambda: 128
        kv_layer = torch.zeros(4, 2, 4, 2, 4)[..., ::2]  # (4, 2, 4, 2, 2)
        worker._kv_layout = _detect_kv_layout(
            tuple(kv_layer.shape), 4, "HND", num_kv_heads=2, head_size=2
        )
        assert worker._kv_layout.format_name == "NL_X_NB_TWO_NH_BS_HS"

        ok = worker._fused_run_transfer(
            kv_layer, memoryview(bytearray(256)), 1, torch.arange(self.CHUNK)
        )
        assert ok is False
        assert worker._lmc_ops.single_layer_kv_transfer.call_count == 0

    def test_fused_run_transfer_kernel_error_disables_and_falls_back(self):
        worker = self._make_worker()
        ops = self._fake_ops()
        ops.single_layer_kv_transfer.side_effect = RuntimeError("boom")
        worker._lmc_ops = ops
        obj_bytes = 2 * self.CHUNK * 2 * 4
        worker._chunk_object_bytes = lambda: obj_bytes
        kv_layer = torch.zeros(2, 4, 4, 1, 2)
        run_view = memoryview(bytearray(obj_bytes))
        ok = worker._fused_run_transfer(kv_layer, run_view, 1, torch.arange(self.CHUNK))
        assert ok is False and worker._fused_load is False

    def test_insufficient_run_bytes_falls_back(self):
        worker = self._make_worker()
        worker._lmc_ops = self._fake_ops()
        worker._chunk_object_bytes = lambda: 128
        kv_layer = torch.zeros(2, 4, 4, 1, 2)
        ok = worker._fused_run_transfer(
            kv_layer, memoryview(bytearray(100)), 1, torch.arange(self.CHUNK)
        )
        assert ok is False


# =============================================================================
# P6: chunk-packed (non-layerwise) storage
# =============================================================================


class TestPackedStorage:
    """Default (non-layerwise) path: one CXL object per chunk holding all
    layers, one key = base_key. Verifies store->retrieve->inject reconstructs
    the same KV the layerwise path would, and that key granularity drops to
    one per chunk.
    """

    BLOCK = 4
    CHUNK = 8
    NUM_LAYERS = 3
    PROMPT = 16  # 2 chunks

    def _make_worker(self):
        from maru_vllm.connector import MaruWorkerConnector

        worker = MaruWorkerConnector(
            block_size=self.BLOCK, kv_chunk_tokens=self.CHUNK, extra_config={}
        )
        assert worker._use_layerwise is False  # default
        worker._num_layers = self.NUM_LAYERS
        store: dict[str, bytes] = {}

        def _alloc(nbytes):
            return SimpleNamespace(buf=memoryview(bytearray(nbytes)))

        def _batch_store(keys, handles):
            for k, h in zip(keys, handles, strict=True):
                store[k] = bytes(h.buf)
            return [True] * len(keys)

        worker._handler = MagicMock()
        worker._handler.alloc.side_effect = _alloc
        worker._handler.batch_store.side_effect = _batch_store
        return worker, store

    def _kv_layers(self):
        # Distinct per-(layer, slot) values so a mis-sliced load is caught.
        # Flash layout: [2, num_blocks, block_size, head=1]
        layers = []
        for li in range(self.NUM_LAYERS):
            kv = torch.empty(2, 4, self.BLOCK, 1)
            base = li * 1000
            kv[0] = base + torch.arange(16, dtype=torch.float32).reshape(
                4, self.BLOCK, 1
            )
            kv[1] = (
                base
                + 500
                + torch.arange(16, dtype=torch.float32).reshape(4, self.BLOCK, 1)
            )
            layers.append(kv)
        return layers

    def _flash_attn(self):
        attn = MagicMock()
        attn.__class__ = type("FlashMetadata", (), {})
        return attn

    def test_mid_chunk_store_error_slab_reclaimed_at_next_step(self):
        """A mid-chunk store error must not leak the re-created slab.

        Layer 0 fills the slab; layer 1 raises inside the slab write (its
        float64 extract inflates slab_bytes past the allocated buffer — a
        stand-in for any transient copy error), discarding the original
        slab; layer 2 re-creates the entry with ``written={2}``, which can
        never reach ``_num_layers``. The next step boundary must reclaim it
        instead of pinning the CXL page forever.
        """
        from maru_vllm.connector import MaruConnectorMetadata

        worker, store = self._make_worker()
        kv_layers = self._kv_layers()
        attn = self._flash_attn()
        token_ids = list(range(self.CHUNK))  # 1 chunk
        names = [f"model.layers.{i}.self_attn" for i in range(self.NUM_LAYERS)]

        def _meta():
            return store_metadata(
                token_ids=token_ids,
                block_ids=[0, 1],
                num_scheduled_tokens=self.CHUNK,
            )

        worker.save_kv_layer(names[0], kv_layers[0], attn, _meta())
        worker.save_kv_layer(names[1], kv_layers[1].double(), attn, _meta())
        worker.save_kv_layer(names[2], kv_layers[2], attn, _meta())

        assert store == {}
        assert len(worker._pending_slabs) == 1  # re-created, unfinishable
        stale_handle = next(iter(worker._pending_slabs.values()))[0]

        forward = SimpleNamespace(
            no_compile_layers={
                names[0]: SimpleNamespace(kv_cache=kv_layers[0]),
            },
            attn_metadata=None,
        )
        worker.start_load_kv(forward, MaruConnectorMetadata())

        assert worker._pending_slabs == {}
        worker._handler.free.assert_called_with(stale_handle)

    def test_store_one_key_per_chunk_then_load_roundtrip(self):
        from maru_vllm.connector import (
            MaruConnectorMetadata,
            MaruReqMeta,
            _chunk_keys,
        )

        worker, store = self._make_worker()
        kv_layers = self._kv_layers()
        attn = self._flash_attn()
        token_ids = list(range(self.PROMPT))
        # names map to indices 0..N-1 via _get_layer_index fallback
        names = [f"model.layers.{i}.self_attn" for i in range(self.NUM_LAYERS)]

        # Store: vLLM calls save_kv_layer once per layer.
        for li, name in enumerate(names):
            meta = MaruConnectorMetadata(
                requests=[
                    MaruReqMeta(
                        req_id="r1",
                        token_ids=token_ids,
                        block_ids=[0, 1, 2, 3],
                        is_store=True,
                        num_scheduled_tokens=self.PROMPT,
                        num_computed_tokens=0,
                    )
                ]
            )
            worker.save_kv_layer(name, kv_layers[li], attn, meta)

        # One key per chunk (2), not per (chunk, layer) (6).
        chunk_keys = _chunk_keys(token_ids, self.CHUNK)
        assert set(store.keys()) == set(chunk_keys)
        assert worker._pending_slabs == {}

        # Each slab holds all layers.
        slab_bytes = len(store[chunk_keys[0]])
        assert slab_bytes == self.NUM_LAYERS * (2 * self.CHUNK * 1 * 4)

        # Load through the real _load_packed (CPU tensors; current stream).
        from maru_handler.memory.types import MemoryInfo
        from maru_vllm.connector import MaruReqMeta

        slab_infos = [
            MemoryInfo(view=memoryview(bytearray(store[ck])), region_id=i, page_index=0)
            for i, ck in enumerate(chunk_keys)
        ]
        dst_layers = [torch.zeros_like(kv) for kv in kv_layers]
        packed_layers = [
            (names[li], dst_layers[li], li) for li in range(self.NUM_LAYERS)
        ]
        slot_mapping = worker._build_slot_mapping([0, 1, 2, 3], self.PROMPT)
        req = MaruReqMeta(
            req_id="r1",
            token_ids=token_ids,
            block_ids=[0, 1, 2, 3],
            is_store=False,
            num_matched_chunks=len(chunk_keys),
        )
        worker._load_packed(
            packed_layers,
            [(req, len(chunk_keys), slot_mapping, slab_infos)],
            attn,
        )
        for li in range(self.NUM_LAYERS):
            assert torch.equal(dst_layers[li], kv_layers[li]), f"layer {li} mismatch"

    def test_scheduler_checks_base_key_not_done(self):
        from maru_vllm.connector import MaruSchedulerConnector

        sched = MaruSchedulerConnector(
            block_size=self.BLOCK, kv_chunk_tokens=self.CHUNK, extra_config={}
        )
        assert sched._use_layerwise is False
        assert sched._chunk_exists_key("kv_abc") == "kv_abc"
        sched_lw = MaruSchedulerConnector(
            block_size=self.BLOCK,
            kv_chunk_tokens=self.CHUNK,
            extra_config={"maru_use_layerwise": True},
        )
        assert sched_lw._chunk_exists_key("kv_abc") == "kv_abc_DONE"

    def _fake_store_kernel(self, kv_layers):
        """Fake multi_layer_kv_transfer ctx mirroring the c_ops D2H branch.

        For engine_kv_format NL_X_TWO_NB_BS_NH_HS the D2H direction gathers
        ``paged[:, slots]`` of every layer into the KV_2LTD slab
        ``[2, num_layers, tokens, hidden]`` (the exact inverse of the H2D
        scatter pinned by test_kv2ltd_slab_scatter_contract).
        """
        direction = SimpleNamespace(H2D="H2D", D2H="D2H")

        def _transfer(
            slab, ptrs, slots, dev, pbs, dir_, fmt, block_size=None, head_size=None
        ):
            assert dir_ == "D2H"
            assert ptrs.tolist() == [kv.data_ptr() for kv in kv_layers]
            for li, kv in enumerate(kv_layers):
                flat = kv.reshape(2, -1, kv.shape[-1])  # [2, page_buffer, hidden]
                slab[:, li] = flat[:, slots]

        ops = SimpleNamespace(
            multi_layer_kv_transfer=MagicMock(side_effect=_transfer),
            TransferDirection=direction,
        )
        ptrs = torch.tensor([kv.data_ptr() for kv in kv_layers], dtype=torch.int64)
        return (ops, ptrs, 16, self.BLOCK, 1, "FMT")

    @pytest.mark.parametrize("order", ["forward", "reversed"])
    def test_kernel_store_defers_all_work_to_final_call(self, order):
        """With a usable kernel ctx, per-layer calls are no-ops; only the
        physically last call of the step — whatever the layer order — stores
        the whole step once (no per-layer alloc/copy)."""
        from maru_vllm.connector import MaruConnectorMetadata, MaruReqMeta

        worker, _ = self._make_worker()
        worker._packed_store_kernel_ctx = lambda attn: ("ops",) * 6
        worker._store_packed_slabs = MagicMock()
        attn = self._flash_attn()
        names = [f"model.layers.{i}.self_attn" for i in range(self.NUM_LAYERS)]
        kv_layers = self._kv_layers()

        indices = list(range(self.NUM_LAYERS))
        if order == "reversed":
            indices.reverse()
        for pos, li in enumerate(indices):
            meta = MaruConnectorMetadata(
                requests=[
                    MaruReqMeta(
                        req_id="r1",
                        token_ids=list(range(self.PROMPT)),
                        block_ids=[0, 1, 2, 3],
                        is_store=True,
                        num_scheduled_tokens=self.PROMPT,
                        num_computed_tokens=0,
                    )
                ]
            )
            worker.save_kv_layer(names[li], kv_layers[li], attn, meta)
            expected_calls = 1 if pos == self.NUM_LAYERS - 1 else 0
            assert worker._store_packed_slabs.call_count == expected_calls

        # No per-layer fallback work happened, and the per-step layer set was
        # cleared so the next step starts fresh.
        assert worker._handler.alloc.call_count == 0
        assert worker._pending_slabs == {}
        assert worker._store_layers_seen == set()

    def test_write_behind_launches_only_after_forward_completion(self):
        from maru_vllm.connector import MaruConnectorMetadata, MaruReqMeta

        worker, _ = self._make_worker()
        worker._write_behind = True
        kernel = ("ops",) * 6
        worker._packed_store_kernel_ctx = lambda attn: kernel
        worker._store_packed_slabs_write_behind = MagicMock()
        attn = self._flash_attn()
        names = [f"model.layers.{i}.self_attn" for i in range(self.NUM_LAYERS)]
        metadata = MaruConnectorMetadata(
            requests=[
                MaruReqMeta(
                    req_id="r1",
                    token_ids=list(range(self.PROMPT)),
                    block_ids=[0, 1, 2, 3],
                    is_store=True,
                    num_scheduled_tokens=self.PROMPT,
                    num_computed_tokens=0,
                )
            ]
        )

        for layer_name, kv_layer in zip(names, self._kv_layers(), strict=True):
            worker.save_kv_layer(layer_name, kv_layer, attn, metadata)

        worker._store_packed_slabs_write_behind.assert_not_called()
        assert worker._queued_store_batches == [(kernel, metadata)]

        worker.get_finished_saving(set())

        worker._store_packed_slabs_write_behind.assert_called_once_with(
            kernel, metadata
        )
        assert worker._queued_store_batches == []

    def test_packed_kernel_ctx_survives_shim_ops_without_format_enum(self):
        """Same shim guard for the packed kernel resolution: a device-ops
        shim lacking EngineKVFormat must resolve the ctx to None (per-layer
        fallback) instead of raising out of save_kv_layer — the pre-guard
        code killed the EngineCore here on the first request."""
        worker, _ = self._make_worker()
        ops = MagicMock()
        del ops.EngineKVFormat  # access now raises AttributeError, like the shim
        worker._lmc_ops = ops
        worker._kv_layout = SimpleNamespace(
            format_name="NL_X_NB_TWO_BS_NH_HS",
            block_size=self.BLOCK,
            head_size=2,
            page_buffer_size=64,
        )
        fake_layer = SimpleNamespace(
            device=SimpleNamespace(type="cuda"), data_ptr=lambda: 0x1000
        )
        layers = [("model.layers.0.self_attn", fake_layer, 0)]

        with patch("torch.cuda.is_available", return_value=True):
            ctx = worker._packed_load_kernel_ctx(layers, MagicMock())

        assert ctx is None
        assert ops.multi_layer_kv_transfer.call_count == 0

    def test_kernel_ctx_caching_contract(self):
        """_packed_store_kernel_ctx resolves once and caches the outcome:
        empty caches -> unresolved (re-probed later), unusable kernel ->
        unusable flag (never re-probed), usable -> tuple reused without rebuild."""
        worker, _ = self._make_worker()
        attn = self._flash_attn()

        # Not registered yet: unresolved, not cached.
        assert worker._packed_store_kernel_ctx(attn) is None
        assert worker._store_kernel_ctx is None

        kv_layers = self._kv_layers()
        names = [f"model.layers.{i}.self_attn" for i in range(self.NUM_LAYERS)]
        worker._kv_caches = dict(zip(names, kv_layers, strict=True))

        # Unusable (CPU tensors fail the device gate): cached separately,
        # the probe is never repeated.
        probe = MagicMock(wraps=worker._packed_load_kernel_ctx)
        worker._packed_load_kernel_ctx = probe
        assert worker._packed_store_kernel_ctx(attn) is None
        assert worker._store_kernel_ctx is None
        assert worker._store_kernel_unusable is True
        assert worker._packed_store_kernel_ctx(attn) is None
        assert probe.call_count == 1

        # Usable: the resolved tuple is cached and reused without rebuild.
        worker2, _ = self._make_worker()
        worker2._kv_caches = dict(zip(names, kv_layers, strict=True))
        sentinel = ("ops",) * 6
        worker2._packed_load_kernel_ctx = MagicMock(return_value=sentinel)
        assert worker2._packed_store_kernel_ctx(attn) is sentinel
        assert worker2._packed_store_kernel_ctx(attn) is sentinel
        assert worker2._packed_load_kernel_ctx.call_count == 1

    def test_kernel_store_dedupes_shared_prefix_across_requests(self):
        """Two same-step requests sharing a prefix store each chunk once."""
        from maru_vllm.connector import (
            MaruConnectorMetadata,
            MaruReqMeta,
            _chunk_keys,
        )

        worker, store = self._make_worker()
        kv_layers = self._kv_layers()
        names = [f"model.layers.{i}.self_attn" for i in range(self.NUM_LAYERS)]
        worker._kv_caches = dict(zip(names, kv_layers, strict=True))
        kernel = self._fake_store_kernel(kv_layers)
        token_ids = list(range(self.PROMPT))

        def _req(rid):
            return MaruReqMeta(
                req_id=rid,
                token_ids=token_ids,
                block_ids=[0, 1, 2, 3],
                is_store=True,
                num_scheduled_tokens=self.PROMPT,
                num_computed_tokens=0,
            )

        meta = MaruConnectorMetadata(requests=[_req("r1"), _req("r2")])
        worker._store_packed_slabs(kernel, meta)

        chunk_keys = _chunk_keys(token_ids, self.CHUNK)
        assert set(store.keys()) == set(chunk_keys)
        assert kernel[0].multi_layer_kv_transfer.call_count == len(chunk_keys)
        assert worker._handler.batch_store.call_count == 1

    def test_kernel_store_frees_handles_when_batch_store_raises(self):
        """A batch_store RPC exception must not leak the slab handles."""
        from maru_vllm.connector import MaruConnectorMetadata, MaruReqMeta

        worker, _ = self._make_worker()
        kv_layers = self._kv_layers()
        names = [f"model.layers.{i}.self_attn" for i in range(self.NUM_LAYERS)]
        worker._kv_caches = dict(zip(names, kv_layers, strict=True))
        kernel = self._fake_store_kernel(kv_layers)
        allocated = []
        worker._handler.alloc.side_effect = lambda n: (
            allocated.append(SimpleNamespace(buf=memoryview(bytearray(n))))
            or allocated[-1]
        )
        worker._handler.batch_store.side_effect = RuntimeError("connection lost")
        freed = []
        worker._handler.free.side_effect = freed.append

        meta = MaruConnectorMetadata(
            requests=[
                MaruReqMeta(
                    req_id="r1",
                    token_ids=list(range(self.PROMPT)),
                    block_ids=[0, 1, 2, 3],
                    is_store=True,
                    num_scheduled_tokens=self.PROMPT,
                    num_computed_tokens=0,
                )
            ]
        )
        worker._store_packed_slabs(kernel, meta)
        assert len(allocated) == 2  # one slab per chunk
        assert freed == allocated  # every handle returned on the except path
        assert worker._stored_keys == set()

    def test_kernel_store_writes_kv2ltd_slab_roundtrip(self):
        """_store_packed_slabs: one kernel D2H per chunk, one key per chunk,
        and the slab roundtrips through _load_packed bit-exactly."""
        from maru_handler.memory.types import MemoryInfo
        from maru_vllm.connector import (
            MaruConnectorMetadata,
            MaruReqMeta,
            _chunk_keys,
        )

        worker, store = self._make_worker()
        kv_layers = self._kv_layers()
        names = [f"model.layers.{i}.self_attn" for i in range(self.NUM_LAYERS)]
        worker._kv_caches = dict(zip(names, kv_layers, strict=True))
        kernel = self._fake_store_kernel(kv_layers)
        token_ids = list(range(self.PROMPT))

        meta = MaruConnectorMetadata(
            requests=[
                MaruReqMeta(
                    req_id="r1",
                    token_ids=token_ids,
                    block_ids=[0, 1, 2, 3],
                    is_store=True,
                    num_scheduled_tokens=self.PROMPT,
                    num_computed_tokens=0,
                )
            ]
        )
        worker._store_packed_slabs(kernel, meta)

        chunk_keys = _chunk_keys(token_ids, self.CHUNK)
        assert set(store.keys()) == set(chunk_keys)
        assert kernel[0].multi_layer_kv_transfer.call_count == len(chunk_keys)
        slab_bytes = self.NUM_LAYERS * (2 * self.CHUNK * 1 * 4)
        assert all(len(store[ck]) == slab_bytes for ck in chunk_keys)

        # Roundtrip through the real _load_packed (CPU fallback inject).
        attn = self._flash_attn()
        slab_infos = [
            MemoryInfo(view=memoryview(bytearray(store[ck])), region_id=i, page_index=0)
            for i, ck in enumerate(chunk_keys)
        ]
        dst_layers = [torch.zeros_like(kv) for kv in kv_layers]
        packed_layers = [
            (names[li], dst_layers[li], li) for li in range(self.NUM_LAYERS)
        ]
        slot_mapping = worker._build_slot_mapping([0, 1, 2, 3], self.PROMPT)
        req = MaruReqMeta(
            req_id="r1",
            token_ids=token_ids,
            block_ids=[0, 1, 2, 3],
            is_store=False,
            num_matched_chunks=len(chunk_keys),
        )
        worker._load_packed(
            packed_layers,
            [(req, len(chunk_keys), slot_mapping, slab_infos)],
            attn,
        )
        for li in range(self.NUM_LAYERS):
            assert torch.equal(dst_layers[li], kv_layers[li]), f"layer {li} mismatch"

    def test_kernel_store_chunked_prefill_stores_only_completed_chunks(self):
        """Chunked prefill: only chunks completed this step are transferred,
        and absolute slot positions are used (mirrors 023ece2 semantics)."""
        from maru_vllm.connector import (
            MaruConnectorMetadata,
            MaruReqMeta,
            _chunk_keys,
        )

        worker, store = self._make_worker()
        kv_layers = self._kv_layers()
        names = [f"model.layers.{i}.self_attn" for i in range(self.NUM_LAYERS)]
        worker._kv_caches = dict(zip(names, kv_layers, strict=True))
        kernel = self._fake_store_kernel(kv_layers)
        token_ids = list(range(self.PROMPT))
        chunk_keys = _chunk_keys(token_ids, self.CHUNK)

        def _meta(computed, scheduled):
            return MaruConnectorMetadata(
                requests=[
                    MaruReqMeta(
                        req_id="r1",
                        token_ids=token_ids,
                        block_ids=[0, 1, 2, 3],
                        is_store=True,
                        num_scheduled_tokens=scheduled,
                        num_computed_tokens=computed,
                    )
                ]
            )

        # Step 1 covers tokens [0, 12): only chunk 0 completes (boundary
        # straddler is left for the finishing step).
        worker._store_packed_slabs(kernel, _meta(0, 12))
        assert set(store.keys()) == {chunk_keys[0]}
        # Step 2 covers tokens [12, 16): chunk 1 completes; chunk 0 skipped
        # via _stored_keys.
        worker._store_packed_slabs(kernel, _meta(12, 4))
        assert set(store.keys()) == set(chunk_keys)
        assert kernel[0].multi_layer_kv_transfer.call_count == 2

    @pytest.mark.skipif(not torch.cuda.is_available(), reason="requires CUDA")
    def test_write_behind_gpu_store_reports_completion(self):
        """Exercise staged D2H and background registration."""
        import time as _time

        from maru_vllm.connector import (
            MaruConnectorMetadata,
            MaruReqMeta,
            _chunk_keys,
        )

        worker, store = self._make_worker()
        worker._write_behind = True
        kv_layers = [kv.cuda() for kv in self._kv_layers()]
        names = [f"model.layers.{i}.self_attn" for i in range(self.NUM_LAYERS)]
        worker._kv_caches = dict(zip(names, kv_layers, strict=True))
        kernel = self._fake_store_kernel(kv_layers)
        token_ids = list(range(self.PROMPT))
        meta = MaruConnectorMetadata(
            requests=[
                MaruReqMeta(
                    req_id="r1",
                    token_ids=token_ids,
                    block_ids=[0, 1, 2, 3],
                    is_store=True,
                    num_scheduled_tokens=self.PROMPT,
                    num_computed_tokens=0,
                )
            ]
        )

        worker._store_packed_slabs_write_behind(kernel, meta)
        done = worker.get_finished_saving({"r1"})
        deadline = _time.monotonic() + 5.0
        while done is None and _time.monotonic() < deadline:
            _time.sleep(0.01)
            done = worker.get_finished_saving(set())

        assert done == {"r1"}
        chunk_keys = _chunk_keys(token_ids, self.CHUNK)
        assert set(store) == set(chunk_keys)
        assert kernel[0].multi_layer_kv_transfer.call_count == len(chunk_keys)
        assert worker._pending_store_keys == set()
        assert worker._request_pending_store_keys == {}
        worker.shutdown()

    def test_kv2ltd_slab_scatter_contract(self):
        """Pin the KV_2LTD slab ↔ paged-buffer scatter contract the no-staging
        kernel relies on (design note P6 v2 시도 3).

        Mirrors LMCache python_ops_fallback.multi_layer_kv_transfer's
        non-mla/non-flashinfer H2D branch:
            paged_tensor.index_copy_(1, slots, key_value[:, layer, valid, :])
        i.e. for engine_kv_format NL_X_TWO_NB_BS_NH_HS the slab must be
        [2, num_layers, tokens, hidden] and each layer's paged buffer
        [2, page_buffer_size, hidden]. If our _save_kv_layer_packed layout ever
        diverges from this, the (bit-exact) assertion breaks. The real c_ops
        kernel is validated end-to-end by the device benchmark.
        """
        num_layers, ct, hidden = 3, self.CHUNK, 2
        page_buffer_size = 32
        slab = torch.arange(2 * num_layers * ct * hidden, dtype=torch.float32).reshape(
            2, num_layers, ct, hidden
        )  # KV_2LTD, as _save_kv_layer_packed writes
        paged = [torch.zeros(2, page_buffer_size, hidden) for _ in range(num_layers)]
        slots = torch.arange(ct, dtype=torch.long) + 5

        # The exact operation the kernel performs, per layer.
        for li in range(num_layers):
            paged[li].index_copy_(1, slots, slab[:, li, :, :])

        for li in range(num_layers):
            for ti in range(ct):
                assert torch.equal(paged[li][:, int(slots[ti]), :], slab[:, li, ti, :])


# =============================================================================
# start_load_kv early-return safety and step-boundary state reset
# =============================================================================


class TestStartLoadKvEarlyReturnSafety:
    """Early returns must strand no parked request and leak no per-step state.

    ``start_load_kv`` can return before iterating ``metadata.requests``
    (worker handler in connect backoff, no attention layers). Deferred
    requests are already parked in WAITING_FOR_REMOTE_KVS and must degrade
    to recompute via ``get_finished_loading``/``take_failed_load_blocks``
    instead of hanging. Independently, the previous step's packed-store
    dispatch state (``_store_layers_seen``, incomplete ``_pending_slabs``)
    must be reset at the step boundary even when the method bails out early.
    """

    def _make_worker(self):
        return make_worker(block_size=4, kv_chunk_tokens=8)

    def _worker_in_backoff(self):
        import time as _time

        worker = self._make_worker()
        worker._handler = None
        worker._handler_retry_after = _time.monotonic() + 60.0
        return worker

    def _metadata(self, deferred=True):
        return deferred_metadata(
            token_ids=list(range(16)),
            block_ids=[0, 1, 2, 3],
            num_matched_chunks=2,
            deferred_load=deferred,
        )

    def _forward(self, with_layer=True):
        layers = {}
        if with_layer:
            layers["model.layers.0.self_attn"] = SimpleNamespace(
                kv_cache=torch.zeros(2, 4, 4, 1)
            )
        return SimpleNamespace(no_compile_layers=layers, attn_metadata=None)

    def test_handler_outage_degrades_deferred_load_to_recompute(self):
        worker = self._worker_in_backoff()

        worker.start_load_kv(self._forward(), self._metadata())

        assert worker.get_finished_loading() == {"r1"}
        assert worker.take_failed_load_blocks() == {0, 1, 2, 3}

    def test_no_attention_layers_degrades_deferred_load_to_recompute(self):
        worker = self._make_worker()
        worker._handler = MagicMock()

        worker.start_load_kv(self._forward(with_layer=False), self._metadata())

        assert worker.get_finished_loading() == {"r1"}
        assert worker.take_failed_load_blocks() == {0, 1, 2, 3}

    def test_inline_load_unaffected_by_handler_outage(self):
        worker = self._worker_in_backoff()

        worker.start_load_kv(self._forward(), self._metadata(deferred=False))

        assert worker.get_finished_loading() is None
        assert worker.take_failed_load_blocks() == set()

    def test_zero_matched_chunks_degrades_deferred_load_to_recompute(self):
        """A parked request with nothing to load must still be reported.

        `update_state_after_alloc` defaults its match result to 0 when the
        entry is missing, so the scheduler can emit a deferred request with
        `num_matched_chunks=0`. The worker reaches its request loop normally
        in that case — the early returns never fire — so the loop itself has
        to report the request or it stays parked forever.
        """
        worker = self._make_worker()
        worker._handler = MagicMock()
        metadata = self._metadata()
        metadata.requests[0].num_matched_chunks = 0

        worker.start_load_kv(self._forward(), metadata)

        assert worker.get_finished_loading() == {"r1"}
        assert worker.take_failed_load_blocks() == {0, 1, 2, 3}

    def test_zero_matched_chunks_inline_load_unaffected(self):
        worker = self._make_worker()
        worker._handler = MagicMock()
        metadata = self._metadata(deferred=False)
        metadata.requests[0].num_matched_chunks = 0

        worker.start_load_kv(self._forward(), metadata)

        assert worker.get_finished_loading() is None
        assert worker.take_failed_load_blocks() == set()

    def test_carried_store_layer_state_cleared_at_step_boundary(self):
        from maru_vllm.connector import MaruConnectorMetadata

        worker = self._make_worker()
        worker._handler = MagicMock()
        worker._num_layers = 4
        worker._store_layers_seen.update({2, 3})

        worker.start_load_kv(self._forward(), MaruConnectorMetadata())

        assert worker._store_layers_seen == set()

    def test_store_layer_state_cleared_even_in_handler_outage(self):
        from maru_vllm.connector import MaruConnectorMetadata

        worker = self._worker_in_backoff()
        worker._store_layers_seen.update({2, 3})

        worker.start_load_kv(self._forward(), MaruConnectorMetadata())

        assert worker._store_layers_seen == set()

    def test_stale_pending_slab_reclaimed_at_step_boundary(self):
        from maru_vllm.connector import MaruConnectorMetadata

        worker = self._make_worker()
        handler = MagicMock()
        worker._handler = handler
        handle = SimpleNamespace(buf=memoryview(bytearray(4)))
        worker._pending_slabs["chunk-a"] = (handle, {1})

        worker.start_load_kv(self._forward(), MaruConnectorMetadata())

        assert worker._pending_slabs == {}
        handler.free.assert_called_once_with(handle)

    def test_slab_orphaned_through_outage_is_freed_on_recovery(self):
        """An outage must neither leak the handle nor leave it reusable.

        The sweep cannot free a handle while the handler is down, so it
        parks it. Leaving the entry in ``_pending_slabs`` instead would let
        a later layer of the same chunk — once the handler recovers
        mid-step — resume writing into a slab that was already condemned,
        mixing planes written in different steps.
        """
        from maru_vllm.connector import MaruConnectorMetadata

        worker = self._worker_in_backoff()
        handle = SimpleNamespace(buf=memoryview(bytearray(4)))
        worker._pending_slabs["chunk-a"] = (handle, {1})

        worker.start_load_kv(self._forward(), MaruConnectorMetadata())
        assert worker._pending_slabs == {}  # not reusable by a later layer
        assert worker._orphan_slab_handles == [handle]  # but not leaked

        handler = MagicMock()
        worker._handler = handler
        worker.start_load_kv(self._forward(), MaruConnectorMetadata())
        assert worker._orphan_slab_handles == []
        handler.free.assert_called_once_with(handle)


# =============================================================================
# Event-gated release of in-flight load references
# =============================================================================


class TestActiveLoadRefsRelease:
    """Load-batch refs are dropped only after their queued copies complete.

    Under vLLM async scheduling the next step's ``start_load_kv`` can run
    while the previous step's H2D copies are still queued on the load
    stream; releasing the pinned slot mappings and CXL mmap views on that
    time basis (instead of event completion) would let the copies read
    freed memory.
    """

    def _make_worker(self):
        return make_worker(block_size=4, kv_chunk_tokens=8)

    def test_pending_batch_survives_release(self):
        worker = self._make_worker()
        done = MagicMock()
        done.query.return_value = True
        pending = MagicMock()
        pending.query.return_value = False
        kept_refs = ["cxl-view"]
        worker._active_load_refs = [(done, ["drained-view"]), (pending, kept_refs)]

        worker._release_completed_load_refs()

        assert worker._active_load_refs == [(pending, kept_refs)]

    def test_completed_batches_all_drop(self):
        worker = self._make_worker()
        done = MagicMock()
        done.query.return_value = True
        worker._active_load_refs = [(done, ["a"]), (done, ["b"])]

        worker._release_completed_load_refs()

        assert worker._active_load_refs == []
