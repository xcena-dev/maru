# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tests for OwnedRegionManager.

Pure unit tests — no RPC server needed.
Uses DaxMapper + add_region() directly.
"""

import pytest
from conftest import _make_handle

from maru_handler.memory import DaxMapper, OwnedRegionManager

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mapper():
    return DaxMapper()


@pytest.fixture
def mgr(mapper):
    return OwnedRegionManager(mapper=mapper, chunk_size=1024)


def _add_region(mgr, region_id=10, length=4096):
    """Helper: add a region via add_region()."""
    handle = _make_handle(region_id, length)
    return mgr.add_region(handle)


# =============================================================================
# add_region
# =============================================================================


class TestOwnedRegionManagerAddRegion:
    """Test add_region lifecycle."""

    def test_add_region(self, mgr):
        region = _add_region(mgr)
        assert mgr.is_owned(10)
        assert region.region_id == 10
        assert region.allocator.page_count == 4  # 4096 / 1024

    def test_add_multiple_regions(self, mgr):
        _add_region(mgr, region_id=10)
        _add_region(mgr, region_id=20)

        assert mgr.is_owned(10)
        assert mgr.is_owned(20)
        assert mgr.get_stats()["num_regions"] == 2

    def test_first_region_becomes_active(self, mgr):
        _add_region(mgr, region_id=10)
        assert mgr._active_region_id == 10

    def test_second_region_does_not_override_active(self, mgr):
        _add_region(mgr, region_id=10)
        _add_region(mgr, region_id=20)
        assert mgr._active_region_id == 10


# =============================================================================
# allocate
# =============================================================================


class TestOwnedRegionManagerAllocate:
    """Test allocation across regions."""

    def test_allocate_from_single_region(self, mgr):
        _add_region(mgr)
        result = mgr.allocate()
        assert result is not None
        region_id, page_index = result
        assert region_id == 10
        assert page_index == 0

    def test_allocate_all_pages(self, mgr):
        _add_region(mgr)
        results = []
        for _ in range(4):
            r = mgr.allocate()
            assert r is not None
            results.append(r)
        assert len(results) == 4

    def test_allocate_returns_none_when_full(self, mgr):
        _add_region(mgr)
        for _ in range(4):
            mgr.allocate()

        # No auto-expansion — returns None
        assert mgr.allocate() is None

    def test_allocate_scans_other_regions(self, mgr):
        _add_region(mgr, region_id=10)
        _add_region(mgr, region_id=20)

        # Fill first region (4 pages)
        for _ in range(4):
            mgr.allocate()

        # Should allocate from second region
        result = mgr.allocate()
        assert result is not None
        assert result[0] == 20  # region_id

    def test_allocate_switches_active_region(self, mgr):
        _add_region(mgr, region_id=10)
        _add_region(mgr, region_id=20)

        # Fill first region
        for _ in range(4):
            mgr.allocate()

        # Allocate from second region switches active
        mgr.allocate()
        assert mgr._active_region_id == 20

    def test_allocate_empty_manager(self, mgr):
        assert mgr.allocate() is None


# =============================================================================
# free
# =============================================================================


class TestOwnedRegionManagerFree:
    """Test free operations."""

    def test_free_and_reallocate(self, mgr):
        _add_region(mgr)
        rid, pid = mgr.allocate()
        mgr.free(rid, pid)

        result = mgr.allocate()
        assert result is not None

    def test_free_invalid_region(self, mgr):
        _add_region(mgr)
        with pytest.raises(KeyError):
            mgr.free(999, 0)


# =============================================================================
# Query
# =============================================================================


class TestOwnedRegionManagerQuery:
    """Test query operations."""

    def test_is_owned(self, mgr):
        assert mgr.is_owned(10) is False
        _add_region(mgr)
        assert mgr.is_owned(10) is True
        assert mgr.is_owned(999) is False

    def test_is_full_empty(self, mgr):
        # No regions → all() on empty is True
        assert mgr.is_full is True

    def test_is_full_with_free_pages(self, mgr):
        _add_region(mgr)
        assert mgr.is_full is False

    def test_is_full_all_allocated(self, mgr):
        _add_region(mgr)
        for _ in range(4):
            mgr.allocate()
        assert mgr.is_full is True

    def test_get_chunk_size(self, mgr):
        assert mgr.get_chunk_size() == 1024

    def test_get_owned_region(self, mgr):
        assert mgr.get_owned_region(10) is None
        _add_region(mgr)
        region = mgr.get_owned_region(10)
        assert region is not None
        assert region.region_id == 10

    def test_get_first_allocator_empty(self, mgr):
        assert mgr.get_first_allocator() is None

    def test_get_first_allocator(self, mgr):
        _add_region(mgr)
        alloc = mgr.get_first_allocator()
        assert alloc is not None
        assert alloc.page_count == 4


# =============================================================================
# Stats
# =============================================================================


class TestOwnedRegionManagerStats:
    """Test stats operations."""

    def test_stats_single_region(self, mgr):
        _add_region(mgr)
        mgr.allocate()

        stats = mgr.get_stats()
        assert stats["num_regions"] == 1
        assert stats["total_page_count"] == 4
        assert stats["total_free_pages"] == 3
        assert stats["total_allocated_pages"] == 1
        assert stats["utilization"] == pytest.approx(0.25)

    def test_stats_multiple_regions(self, mgr):
        _add_region(mgr, region_id=10)
        _add_region(mgr, region_id=20)

        for _ in range(5):
            mgr.allocate()

        stats = mgr.get_stats()
        assert stats["num_regions"] == 2
        assert stats["total_page_count"] == 8
        assert stats["total_allocated_pages"] == 5

    def test_stats_empty(self, mgr):
        stats = mgr.get_stats()
        assert stats["num_regions"] == 0
        assert stats["utilization"] == 0.0


# =============================================================================
# close
# =============================================================================


class TestOwnedRegionManagerClose:
    """Test close operations."""

    def test_close_returns_region_ids(self, mgr):
        _add_region(mgr, region_id=10)
        _add_region(mgr, region_id=20)
        mgr.allocate()

        ids = mgr.close()
        assert ids == [10, 20]
        assert mgr.get_stats()["num_regions"] == 0

    def test_close_empty(self, mgr):
        ids = mgr.close()
        assert ids == []

    def test_close_clears_active_region(self, mgr):
        _add_region(mgr)
        mgr.close()
        assert mgr._active_region_id is None


class TestOwnedRegionManagerCloseEdgeCases:
    """Test close edge cases for 100% coverage."""

    def test_close_region_is_none_after_get(self, mgr):
        """Test close() when region is None after get (edge case)."""
        _add_region(mgr, region_id=10)

        # Manually corrupt the dict to simulate edge case
        mgr._regions[10] = None

        # Should not raise
        ids = mgr.close()
        assert 10 in ids

    def test_close_allocator_close_exception(self, mgr):
        """Test close() allocator.close() raises exception → logged."""
        import logging
        from unittest.mock import patch

        region = _add_region(mgr, region_id=10)

        # Mock allocator.close() to raise exception
        with patch.object(
            region.allocator, "close", side_effect=Exception("close error")
        ):
            with patch.object(
                logging.getLogger("maru_handler.memory.owned_region_manager"), "error"
            ) as mock_error:
                ids = mgr.close()

        assert 10 in ids
        assert mock_error.called
        # Check the format string pattern
        assert "Error closing owned region %d allocator" in mock_error.call_args[0][0]
