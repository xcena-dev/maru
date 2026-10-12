# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""OwnedRegionManager - Manages multiple owned regions with PagedMemoryAllocator.

Handles allocation, write, and region lifecycle via DaxMapper.
Does NOT own RPC — expansion and return_alloc are Handler's responsibility.
"""

import logging
import threading

from maru_shm.types import MaruHandle

from .allocator import PagedMemoryAllocator
from .mapper import DaxMapper
from .types import OwnedRegion

logger = logging.getLogger(__name__)


class OwnedRegionManager:
    """Manages multiple owned memory regions for store operations.

    Each owned region is backed by an mmap'd CXL region and managed by
    a PagedMemoryAllocator for page-level allocation.

    Does NOT handle expansion — when all regions are exhausted,
    allocate() returns None and the caller (Handler) is responsible
    for requesting a new region via RPC and calling add_region().

    Architecture::

        OwnedRegionManager
            ├── DaxMapper (shared, for mmap/munmap)
            └── OwnedRegion[]
                └── PagedMemoryAllocator (deque-based, per-region)

    Allocation strategy:
        1. Try active region (fast path, O(1))
        2. Scan other regions for free pages
        3. Return None if all exhausted (caller handles expansion)
    """

    def __init__(self, mapper: DaxMapper, chunk_size: int):
        """Initialize the OwnedRegionManager.

        Args:
            mapper: DaxMapper for mmap/munmap operations
            chunk_size: Page/chunk size for PagedMemoryAllocator
        """
        self._mapper = mapper
        self._chunk_size = chunk_size
        self._lock = threading.Lock()

        self._regions: dict[int, OwnedRegion] = {}
        self._region_order: list[int] = []
        self._active_region_id: int | None = None

    # =========================================================================
    # Lifecycle
    # =========================================================================

    def add_region(self, handle: MaruHandle) -> OwnedRegion:
        """Map a region via DaxMapper and create a PagedMemoryAllocator.

        Called by Handler during connect() and _expand_region().
        Thread-safe: protected by internal lock.

        Args:
            handle: MaruHandle from server allocation

        Returns:
            The created OwnedRegion

        Raises:
            RuntimeError: If mmap fails
            ValueError: If allocator initialization fails
        """
        self._mapper.map_region(handle)

        allocator = PagedMemoryAllocator(
            region_id=handle.region_id,
            pool_size=handle.length,
            chunk_size=self._chunk_size,
        )

        region = OwnedRegion(
            region_id=handle.region_id,
            allocator=allocator,
        )
        with self._lock:
            self._regions[handle.region_id] = region
            self._region_order.append(handle.region_id)

            if self._active_region_id is None:
                self._active_region_id = handle.region_id

        logger.info(
            "Added owned region %d: pages=%d, chunk_size=%d",
            handle.region_id,
            allocator.page_count,
            self._chunk_size,
        )
        return region

    def close(self) -> list[int]:
        """Close all owned regions: allocator cleanup only.

        Does NOT unmap — DaxMapper.close() handles all unmaps.
        Returns list of region_ids for the caller to return_alloc via RPC.
        Thread-safe: protected by internal lock.

        Returns:
            List of region_ids that were closed.
        """
        with self._lock:
            region_ids = list(self._region_order)

            for rid in region_ids:
                region = self._regions.get(rid)
                if region is None:
                    continue

                try:
                    region.allocator.close()
                except Exception:
                    logger.error(
                        "Error closing owned region %d allocator", rid, exc_info=True
                    )

            self._regions.clear()
            self._region_order.clear()
            self._active_region_id = None

            return region_ids

    # =========================================================================
    # Allocation
    # =========================================================================

    def allocate(self) -> tuple[int, int] | None:
        """Allocate a page from any available owned region.

        Thread-safe: protected by internal lock.

        Strategy:
            1. Try active region first (fast path)
            2. If exhausted, scan other regions for free pages
            3. Return None if all exhausted (caller handles expansion)

        Returns:
            (region_id, page_index) on success, None on failure.
        """
        with self._lock:
            # 1. Fast path: try active region
            if self._active_region_id is not None:
                active = self._regions.get(self._active_region_id)
                if active is not None:
                    page_index = active.allocator.allocate()
                    if page_index is not None:
                        return (self._active_region_id, page_index)

            # 2. Scan other regions
            for rid in self._region_order:
                if rid == self._active_region_id:
                    continue
                region = self._regions[rid]
                if region.allocator.num_free_pages > 0:
                    page_index = region.allocator.allocate()
                    if page_index is not None:
                        self._active_region_id = rid
                        logger.debug("Switched active region to %d", rid)
                        return (rid, page_index)

            return None

    def free(self, region_id: int, page_index: int) -> None:
        """Free a page in the specified owned region.

        Thread-safe: protected by internal lock.

        Raises:
            KeyError: If region_id not found
        """
        with self._lock:
            region = self._regions.get(region_id)
            if region is None:
                raise KeyError(f"Owned region {region_id} not found")
            region.allocator.free(page_index)

    # =========================================================================
    # Query
    # =========================================================================

    def is_owned(self, region_id: int) -> bool:
        """Check if a region is owned by this manager."""
        return region_id in self._regions

    @property
    def is_full(self) -> bool:
        """Check if all owned regions are exhausted."""
        return all(
            region.allocator.num_free_pages == 0 for region in self._regions.values()
        )

    def get_chunk_size(self) -> int:
        """Return the chunk size."""
        return self._chunk_size

    def get_region_ids(self) -> list[int]:
        """Get list of owned region IDs in insertion order."""
        return list(self._region_order)

    def get_owned_region(self, region_id: int) -> OwnedRegion | None:
        """Get an owned region by ID."""
        return self._regions.get(region_id)

    def get_first_region_id(self) -> int | None:
        """Get the first region's ID."""
        return self._region_order[0] if self._region_order else None

    def get_first_allocator(self) -> PagedMemoryAllocator | None:
        """Get the first region's allocator (backward compat)."""
        first_rid = self.get_first_region_id()
        if first_rid is None:
            return None
        first_region = self._regions.get(first_rid)
        return first_region.allocator if first_region else None

    # =========================================================================
    # Stats
    # =========================================================================

    def get_stats(self) -> dict:
        """Return aggregated stats across all owned regions."""
        regions_stats = []
        total_pool_size = 0
        total_page_count = 0
        total_free = 0
        total_allocated = 0

        for rid in self._region_order:
            region = self._regions[rid]
            s = region.allocator.get_stats()
            s["region_id"] = rid
            regions_stats.append(s)
            total_pool_size += s["pool_size"]
            total_page_count += s["page_count"]
            total_free += s["free_pages"]
            total_allocated += s["allocated_pages"]

        return {
            "num_regions": len(self._regions),
            "total_pool_size": total_pool_size,
            "total_page_count": total_page_count,
            "total_free_pages": total_free,
            "total_allocated_pages": total_allocated,
            "utilization": total_allocated / total_page_count
            if total_page_count > 0
            else 0.0,
            "regions": regions_stats,
        }
