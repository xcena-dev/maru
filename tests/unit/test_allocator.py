# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tests for PagedMemoryAllocator."""

import pytest

from maru_handler.memory import PagedMemoryAllocator


def _make_allocator(length=4096, chunk_size=1024):
    """Create an allocator for testing."""
    allocator = PagedMemoryAllocator(
        region_id=1,
        pool_size=length,
        chunk_size=chunk_size,
    )
    return allocator


class TestPagedMemoryAllocatorInit:
    """Test allocator initialization."""

    def test_init_basic(self):
        allocator = _make_allocator(length=4096, chunk_size=1024)

        assert allocator.page_count == 4
        assert allocator.num_free_pages == 4
        assert allocator.num_allocated == 0
        assert allocator.chunk_size == 1024

        allocator.close()

    def test_init_truncates_remainder(self):
        allocator = _make_allocator(length=5000, chunk_size=1024)

        # 5000 // 1024 = 4 pages (remainder 904 bytes unused)
        assert allocator.page_count == 4

        allocator.close()

    def test_init_invalid_chunk_size(self):
        with pytest.raises(ValueError, match="chunk_size must be positive"):
            PagedMemoryAllocator(
                region_id=1,
                pool_size=4096,
                chunk_size=0,
            )

    def test_init_pool_too_small(self):
        with pytest.raises(ValueError, match="smaller than chunk_size"):
            PagedMemoryAllocator(
                region_id=1,
                pool_size=512,
                chunk_size=1024,
            )


class TestPagedMemoryAllocatorAllocation:
    """Test allocate/free operations."""

    @pytest.fixture
    def allocator(self):
        alloc = _make_allocator()
        yield alloc
        alloc.close()

    def test_allocate_single(self, allocator):
        page = allocator.allocate()
        assert page is not None
        assert page == 0
        assert allocator.num_allocated == 1
        assert allocator.num_free_pages == 3

    def test_allocate_all_pages(self, allocator):
        pages = []
        for _ in range(4):
            p = allocator.allocate()
            assert p is not None
            pages.append(p)

        assert allocator.num_free_pages == 0
        assert allocator.num_allocated == 4
        assert set(pages) == {0, 1, 2, 3}

    def test_allocate_exhaustion(self, allocator):
        for _ in range(4):
            allocator.allocate()

        assert allocator.allocate() is None

    def test_free_and_reuse(self, allocator):
        page = allocator.allocate()
        allocator.free(page)

        assert allocator.num_free_pages == 4
        assert allocator.num_allocated == 0

        page2 = allocator.allocate()
        assert page2 is not None

    def test_free_invalid_index(self, allocator):
        with pytest.raises(ValueError, match="Invalid page_index"):
            allocator.free(-1)

        with pytest.raises(ValueError, match="Invalid page_index"):
            allocator.free(4)

    def test_allocate_free_allocate_cycle(self, allocator):
        # Fill pool
        pages = [allocator.allocate() for _ in range(4)]
        assert allocator.allocate() is None

        # Free one
        allocator.free(pages[2])
        assert allocator.num_free_pages == 1

        # Allocate again
        new_page = allocator.allocate()
        assert new_page == pages[2]  # reused page


class TestPagedMemoryAllocatorStats:
    """Test stats and lifecycle."""

    def test_get_stats(self):
        allocator = _make_allocator()

        allocator.allocate()
        allocator.allocate()

        stats = allocator.get_stats()
        assert stats["pool_size"] == 4096
        assert stats["chunk_size"] == 1024
        assert stats["page_count"] == 4
        assert stats["free_pages"] == 2
        assert stats["allocated_pages"] == 2
        assert stats["utilization"] == 0.5

        allocator.close()

    def test_close(self):
        allocator = _make_allocator()

        # close should not raise
        allocator.close()


class TestDoubleFree:
    """Test double-free detection."""

    def test_double_free_raises(self):
        """Freeing the same page twice raises ValueError."""
        alloc = PagedMemoryAllocator(
            region_id=1,
            pool_size=4096,
            chunk_size=1024,
        )
        page_idx = alloc.allocate()
        assert page_idx is not None
        alloc.free(page_idx)
        # Second free should raise
        with pytest.raises(ValueError, match="[Dd]ouble free"):
            alloc.free(page_idx)
