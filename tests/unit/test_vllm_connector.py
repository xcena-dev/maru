# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for maru_vllm.connector utilities.

Tests pure functions and layout helpers without requiring CXL hardware,
a running MaruServer, or GPU. All tensors are CPU-only.
"""

from unittest.mock import MagicMock, patch

import pytest
import torch

from maru_vllm.connector import (
    _align_down,
    _chunk_keys,
    _parse_size,
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
        from maru_vllm.connector import MaruWorkerConnector

        worker = MaruWorkerConnector.__new__(MaruWorkerConnector)
        worker._block_size = block_size
        return worker

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


# =============================================================================
# MaruWorkerConnector._get_layer_index
# =============================================================================


class TestGetLayerIndex:
    def _make_worker(self):
        from maru_vllm.connector import MaruWorkerConnector

        worker = MaruWorkerConnector.__new__(MaruWorkerConnector)
        worker._block_size = 16
        worker._kv_caches = {"layer_a": None, "layer_b": None}
        return worker

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
        from maru_vllm.connector import MaruWorkerConnector

        worker = MaruWorkerConnector.__new__(MaruWorkerConnector)
        worker._block_size = block_size
        return worker

    def test_flash_roundtrip(self):
        """Flash attention: [2, num_pages, page_size, head_dim]"""
        worker = self._make_worker(block_size=4)
        num_pages, page_size, head_dim = 4, 4, 8
        kv_cache = torch.zeros(2, num_pages, page_size, head_dim)

        # Create source data for 4 tokens
        src_data = torch.randn(2, 4, head_dim)
        slot_mapping = torch.tensor([0, 1, 2, 3])

        # Use a plain object as attn_metadata (not MLA or Triton → Flash branch)
        attn_metadata = MagicMock()
        attn_metadata.__class__ = type("FlashMetadata", (), {})

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

        attn_metadata = MagicMock()
        attn_metadata.__class__ = type("FlashMetadata", (), {})

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

        inner_meta = MagicMock()
        inner_meta.__class__ = type("FlashMetadata", (), {})
        attn_metadata = {"layer0": inner_meta, "layer1": inner_meta}

        worker._inject_kv_into_layer(
            kv_cache, src_data, slot_mapping, attn_metadata, "layer0"
        )
        extracted = worker._extract_kv_from_layer(
            kv_cache, slot_mapping, attn_metadata, "layer0"
        )

        torch.testing.assert_close(extracted, src_data)


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
        sched._known_keys.add(f"{keys[0]}_DONE")

        result = sched._count_matched_chunks(token_ids)
        assert result == 1
        sched._handler.batch_exists.assert_not_called()

    def test_empty_tokens(self):
        sched = self._make_scheduler(chunk_tokens=256)
        assert sched._count_matched_chunks([]) == 0
