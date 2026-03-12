"""Unit tests for OwnedRegionManager (fs mode).

VFS operations exercised against a real tmpfs directory.
"""

import tempfile
from unittest.mock import patch

import pytest

from maru_handler.memory.marufs_mapper import MarufsMapper
from maru_handler.memory.owned_region_manager import OwnedRegionManager
from maru_handler.memory.types import OwnedRegion
from marufs.client import MarufsClient

# 64 KiB pool, 4 KiB chunk → 16 pages per region
CHUNK_SIZE = 4096
POOL_SIZE = CHUNK_SIZE * 16


@pytest.fixture()
def tmpdir():
    """Provide a temporary directory that simulates an marufs mount point."""
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture()
def marufs_client(tmpdir):
    """MarufsClient backed by a tmpfs directory (perm ioctls mocked)."""
    c = MarufsClient(tmpdir)
    with patch.object(c, "perm_set_default"):
        yield c
    c.close()


@pytest.fixture()
def mapper(marufs_client):
    """MarufsMapper backed by the tmpfs MarufsClient."""
    m = MarufsMapper(marufs_client)
    yield m
    m.close()


@pytest.fixture()
def mgr():
    """OwnedRegionManager with default chunk size."""
    return OwnedRegionManager(chunk_size=CHUNK_SIZE)


def _add_region(mgr, mapper, name):
    """Helper: map region via MarufsMapper, then add to manager."""
    mapper.map_owned_region(name, POOL_SIZE)
    return mgr.add_region(name, POOL_SIZE)


# ---------------------------------------------------------------------------
# add_region
# ---------------------------------------------------------------------------


class TestAddRegion:
    def test_add_region_returns_owned_region(self, mgr, mapper):
        """add_region returns an OwnedRegion instance."""
        region = _add_region(mgr, mapper, "region1")
        assert isinstance(region, OwnedRegion)

    def test_add_region_key_set(self, mgr, mapper):
        """OwnedRegion has the correct key."""
        region = _add_region(mgr, mapper, "region1")
        assert region.key == "region1"

    def test_add_region_allocator_initialized(self, mgr, mapper):
        """Region allocator has the expected page count."""
        region = _add_region(mgr, mapper, "region1")
        assert region.allocator.page_count == POOL_SIZE // CHUNK_SIZE

    def test_add_region_is_owned(self, mgr, mapper):
        """is_owned returns True after add_region."""
        _add_region(mgr, mapper, "region1")
        assert mgr.is_owned("region1") is True

    def test_add_region_not_owned_before_add(self, mgr):
        """is_owned returns False for regions not yet added."""
        assert mgr.is_owned("ghost") is False

    def test_add_region_mapper_has_mapping(self, mgr, mapper):
        """MarufsMapper has the region mapped after add_region."""
        _add_region(mgr, mapper, "region1")
        assert mapper.is_mapped("region1") is True


# ---------------------------------------------------------------------------
# allocate
# ---------------------------------------------------------------------------


class TestAllocate:
    def test_allocate_returns_tuple(self, mgr, mapper):
        """allocate returns (region_name, page_index) tuple."""
        _add_region(mgr, mapper, "r1")
        result = mgr.allocate()
        assert result is not None
        name, page_index = result
        assert isinstance(name, str)
        assert isinstance(page_index, int)

    def test_allocate_page_index_in_range(self, mgr, mapper):
        """Allocated page_index is within valid range."""
        _add_region(mgr, mapper, "r1")
        _, page_index = mgr.allocate()
        assert 0 <= page_index < POOL_SIZE // CHUNK_SIZE

    def test_allocate_multiple_unique(self, mgr, mapper):
        """Consecutive allocations return different page indices."""
        _add_region(mgr, mapper, "r1")
        results = [mgr.allocate() for _ in range(5)]
        page_indices = [r[1] for r in results]
        assert len(set(page_indices)) == 5

    def test_allocate_region_name_matches(self, mgr, mapper):
        """Allocated region name matches the added region."""
        _add_region(mgr, mapper, "r1")
        name, _ = mgr.allocate()
        assert name == "r1"


# ---------------------------------------------------------------------------
# allocate exhausted
# ---------------------------------------------------------------------------


class TestAllocateExhausted:
    def test_allocate_returns_none_when_full(self, mgr, mapper):
        """allocate returns None after all pages are consumed."""
        _add_region(mgr, mapper, "r1")
        pages_per_region = POOL_SIZE // CHUNK_SIZE
        for _ in range(pages_per_region):
            assert mgr.allocate() is not None
        assert mgr.allocate() is None

    def test_is_full_after_exhaustion(self, mgr, mapper):
        """is_full is True after all pages are consumed."""
        _add_region(mgr, mapper, "r1")
        pages_per_region = POOL_SIZE // CHUNK_SIZE
        for _ in range(pages_per_region):
            mgr.allocate()
        assert mgr.is_full is True

    def test_is_full_false_when_pages_available(self, mgr, mapper):
        """is_full is False when pages are available."""
        _add_region(mgr, mapper, "r1")
        assert mgr.is_full is False


# ---------------------------------------------------------------------------
# allocate multi-region
# ---------------------------------------------------------------------------


class TestAllocateMultiRegion:
    def test_allocate_spans_both_regions(self, mgr, mapper):
        """Allocation uses second region after first is exhausted."""
        _add_region(mgr, mapper, "r1")
        _add_region(mgr, mapper, "r2")
        pages_per_region = POOL_SIZE // CHUNK_SIZE

        # Exhaust r1
        for _ in range(pages_per_region):
            mgr.allocate()

        # Next allocation should come from r2
        result = mgr.allocate()
        assert result is not None
        name, _ = result
        assert name == "r2"

    def test_allocate_total_pages(self, mgr, mapper):
        """Total allocatable pages equals sum across both regions."""
        _add_region(mgr, mapper, "r1")
        _add_region(mgr, mapper, "r2")
        pages_per_region = POOL_SIZE // CHUNK_SIZE
        total = pages_per_region * 2

        results = [mgr.allocate() for _ in range(total)]
        assert all(r is not None for r in results)
        assert mgr.allocate() is None


# ---------------------------------------------------------------------------
# free
# ---------------------------------------------------------------------------


class TestFree:
    def test_free_allows_reallocation(self, mgr, mapper):
        """Freed page can be re-allocated."""
        _add_region(mgr, mapper, "r1")
        name, page_index = mgr.allocate()
        mgr.free(name, page_index)
        # After free, a new allocation should succeed
        result = mgr.allocate()
        assert result is not None

    def test_free_invalid_region_raises(self, mgr, mapper):
        """free raises KeyError for unknown region name."""
        _add_region(mgr, mapper, "r1")
        with pytest.raises(KeyError):
            mgr.free("nonexistent", 0)

    def test_free_restores_page_count(self, mgr, mapper):
        """Freeing a page restores free_pages count."""
        _add_region(mgr, mapper, "r1")
        pages_per_region = POOL_SIZE // CHUNK_SIZE

        # Exhaust all pages
        allocated = []
        for _ in range(pages_per_region):
            allocated.append(mgr.allocate())

        assert mgr.is_full is True

        # Free one page — is_full should become False
        name, page_index = allocated[0]
        mgr.free(name, page_index)
        assert mgr.is_full is False


# ---------------------------------------------------------------------------
# is_owned / is_full
# ---------------------------------------------------------------------------


class TestIsOwnedIsFull:
    def test_is_owned_true(self, mgr, mapper):
        _add_region(mgr, mapper, "r1")
        assert mgr.is_owned("r1") is True

    def test_is_owned_false(self, mgr):
        assert mgr.is_owned("r99") is False

    def test_is_full_empty_manager(self, mgr):
        """is_full is True (vacuously) when no regions are added."""
        assert mgr.is_full is True


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------


class TestClose:
    def test_close_returns_region_names(self, mgr, mapper):
        """close returns the list of region names."""
        _add_region(mgr, mapper, "r1")
        _add_region(mgr, mapper, "r2")
        names = mgr.close()
        assert sorted(names) == ["r1", "r2"]

    def test_close_clears_regions(self, mgr, mapper):
        """After close, is_owned returns False for all regions."""
        _add_region(mgr, mapper, "r1")
        _add_region(mgr, mapper, "r2")
        mgr.close()
        assert mgr.is_owned("r1") is False
        assert mgr.is_owned("r2") is False

    def test_close_empty_manager(self, mgr):
        """Closing an empty manager returns empty list."""
        names = mgr.close()
        assert names == []


# ---------------------------------------------------------------------------
# get_stats
# ---------------------------------------------------------------------------


class TestGetStats:
    def test_get_stats_structure(self, mgr, mapper):
        """get_stats returns a dict with expected keys."""
        _add_region(mgr, mapper, "r1")
        stats = mgr.get_stats()
        assert "num_regions" in stats
        assert "total_pool_size" in stats
        assert "total_page_count" in stats
        assert "total_free_pages" in stats
        assert "total_allocated_pages" in stats
        assert "utilization" in stats
        assert "regions" in stats

    def test_get_stats_num_regions(self, mgr, mapper):
        """num_regions matches the number of added regions."""
        _add_region(mgr, mapper, "r1")
        _add_region(mgr, mapper, "r2")
        stats = mgr.get_stats()
        assert stats["num_regions"] == 2

    def test_get_stats_region_key(self, mgr, mapper):
        """Each region entry in stats has region_key."""
        _add_region(mgr, mapper, "r1")
        stats = mgr.get_stats()
        assert stats["regions"][0]["region_key"] == "r1"

    def test_get_stats_utilization_zero_when_empty(self, mgr, mapper):
        """utilization is 0.0 when no pages are allocated."""
        _add_region(mgr, mapper, "r1")
        stats = mgr.get_stats()
        assert stats["utilization"] == 0.0

    def test_get_stats_utilization_after_alloc(self, mgr, mapper):
        """utilization increases after allocation."""
        _add_region(mgr, mapper, "r1")
        pages_per_region = POOL_SIZE // CHUNK_SIZE
        for _ in range(pages_per_region // 2):
            mgr.allocate()
        stats = mgr.get_stats()
        assert stats["utilization"] == pytest.approx(0.5)

    def test_get_stats_total_page_count(self, mgr, mapper):
        """total_page_count equals sum of pages across all regions."""
        _add_region(mgr, mapper, "r1")
        _add_region(mgr, mapper, "r2")
        stats = mgr.get_stats()
        pages_per_region = POOL_SIZE // CHUNK_SIZE
        assert stats["total_page_count"] == pages_per_region * 2
