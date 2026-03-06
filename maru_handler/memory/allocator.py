# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Paged memory allocator for shared memory regions.

Fixed-size page allocator over a pre-mapped memory region.
Follows LMCache's PagedTensorMemoryAllocator pattern:
deque-based free list for O(1) allocate/free.

Does NOT perform read/write — Handler handles data access directly.
The allocator does NOT own the mmap lifecycle — it receives a
pre-mapped base address from OwnedRegionManager (via DaxMapper).
"""

import logging
from collections import deque

logger = logging.getLogger(__name__)


class PagedMemoryAllocator:
    """Fixed-size paged memory allocator over pre-mapped shared memory.

    On init:
    - Receives a pre-mapped base address (mmap owned by DaxMapper)
    - Splits into chunk_size pages, each represented by a page index
    - Free list is a deque of page indices:
      O(1) allocate (popleft), O(1) free (append)

    Thread safety:
    - deque.popleft() and deque.append() are atomic in CPython
      (implemented in C). No explicit lock needed.
    - Same approach as LMCache's PagedTensorMemoryAllocator.

    Args:
        region_id: ID of the memory region
        pool_size: Total size of the memory pool in bytes
        chunk_size: Size of each page/chunk in bytes
    """

    def __init__(self, region_id: int, pool_size: int, chunk_size: int):
        if chunk_size <= 0:
            raise ValueError(f"chunk_size must be positive, got {chunk_size}")

        self._region_id = region_id
        self._chunk_size = chunk_size

        # Compute page count (truncate any remainder)
        self._pool_size = pool_size
        self._page_count = self._pool_size // self._chunk_size

        if self._page_count == 0:
            raise ValueError(
                f"Pool size {self._pool_size} is smaller than "
                f"chunk_size {self._chunk_size}"
            )

        # Build free list: deque of page indices
        # NOTE: deque.popleft() and deque.append() are atomic in CPython,
        # so no explicit lock is needed (same approach as LMCache).
        self._free_pages: deque[int] = deque(range(self._page_count))

        # Track allocated pages to prevent double-free
        self._allocated_pages: set[int] = set()

        # Debug counters
        self._num_allocated = 0

        logger.debug(
            "PagedMemoryAllocator initialized: region=%d, pool_size=%d, "
            "chunk_size=%d, page_count=%d",
            self._region_id,
            self._pool_size,
            self._chunk_size,
            self._page_count,
        )

    @property
    def chunk_size(self) -> int:
        """Page/chunk size in bytes."""
        return self._chunk_size

    @property
    def page_count(self) -> int:
        """Total number of pages."""
        return self._page_count

    @property
    def num_free_pages(self) -> int:
        """Number of free pages."""
        return len(self._free_pages)

    @property
    def num_allocated(self) -> int:
        """Number of allocated pages."""
        return self._num_allocated

    # =========================================================================
    # Allocation
    # =========================================================================

    def allocate(self) -> int | None:
        """Allocate a single page.

        Returns:
            Page index, or None if pool is exhausted.
        """
        try:
            page_index = self._free_pages.popleft()
        except IndexError:
            logger.warning("Pool exhausted: no free pages available")
            return None

        self._allocated_pages.add(page_index)
        self._num_allocated += 1
        logger.debug(
            "Allocated page %d (%d free remaining)",
            page_index,
            len(self._free_pages),
        )
        return page_index

    def free(self, page_index: int) -> None:
        """Free a page back to the pool.

        Args:
            page_index: Page index to free
        """
        if page_index < 0 or page_index >= self._page_count:
            raise ValueError(
                f"Invalid page_index: {page_index} (range: 0-{self._page_count - 1})"
            )
        if page_index not in self._allocated_pages:
            raise ValueError(
                f"Double free detected: page {page_index} is not allocated"
            )

        self._allocated_pages.discard(page_index)
        self._free_pages.append(page_index)
        self._num_allocated -= 1
        logger.debug("Freed page %d (%d free)", page_index, len(self._free_pages))

    # =========================================================================
    # Stats & Lifecycle
    # =========================================================================

    def get_stats(self) -> dict:
        """Get allocator statistics."""
        return {
            "pool_size": self._pool_size,
            "chunk_size": self._chunk_size,
            "page_count": self._page_count,
            "free_pages": len(self._free_pages),
            "allocated_pages": self._num_allocated,
            "utilization": self._num_allocated / self._page_count
            if self._page_count > 0
            else 0.0,
        }

    def close(self) -> None:
        """Release page tracking state.

        Does NOT unmap memory — mmap lifecycle is owned by DaxMapper via OwnedRegionManager.
        """
        self._free_pages.clear()
        self._allocated_pages.clear()
        self._num_allocated = 0
        logger.debug(
            "PagedMemoryAllocator closed: region %d page tracking cleared",
            self._region_id,
        )
