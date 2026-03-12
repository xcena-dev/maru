# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""OwnedRegionManager - Manages multiple owned regions with PagedMemoryAllocator.

Unified manager for both remote (DaxMapper) and fs (MarufsMapper) modes.
Does NOT own mapper — the caller (Handler) is responsible for mapping
regions before calling add_region(), and for unmapping on close.
"""

import logging
import threading

from .allocator import PagedMemoryAllocator
from .types import OwnedRegion

logger = logging.getLogger(__name__)

# Type alias for region key: int (region_id) in remote mode, str (name) in fs mode
RegionKey = str | int


class OwnedRegionManager:
    """Manages multiple owned memory regions for store operations.

    Each owned region is backed by an mmap'd CXL region and managed by
    a PagedMemoryAllocator for page-level allocation.

    Does NOT handle expansion — when all regions are exhausted,
    allocate() returns None and the caller (Handler) is responsible
    for creating a new region and calling add_region().

    Does NOT own mapper — the caller maps/unmaps regions directly.
    This class is a pure allocator manager.

    Architecture::

        OwnedRegionManager
            └── OwnedRegion[]
                └── PagedMemoryAllocator (deque-based, per-region)

    Allocation strategy:
        1. Try active region (fast path, O(1))
        2. Scan other regions for free pages
        3. Return None if all exhausted (caller handles expansion)
    """

    def __init__(self, chunk_size: int):
        """Initialize the OwnedRegionManager.

        Args:
            chunk_size: Page/chunk size for PagedMemoryAllocator
        """
        self._chunk_size = chunk_size
        self._lock = threading.Lock()

        self._regions: dict[RegionKey, OwnedRegion] = {}
        self._region_order: list[RegionKey] = []
        self._active_region: RegionKey | None = None
        self._region_counter: int = 0

    # =========================================================================
    # Lifecycle
    # =========================================================================

    def add_region(self, key: RegionKey, pool_size: int) -> OwnedRegion:
        """Register an already-mapped region and create a PagedMemoryAllocator.

        The caller is responsible for mapping the region (via DaxMapper or
        MarufsMapper) before calling this method.

        Thread-safe: protected by internal lock.

        Args:
            key: Region identifier — region_id (int) for remote mode,
                 region_name (str) for fs mode.
            pool_size: Size in bytes of the region.

        Returns:
            The created OwnedRegion
        """
        allocator = PagedMemoryAllocator(
            region_id=self._region_counter,
            pool_size=pool_size,
            chunk_size=self._chunk_size,
        )

        region = OwnedRegion(
            key=key,
            allocator=allocator,
        )
        with self._lock:
            self._regions[key] = region
            self._region_order.append(key)
            self._region_counter += 1

            if self._active_region is None:
                self._active_region = key

        logger.info(
            "Added owned region %s: pages=%d, chunk_size=%d",
            key,
            allocator.page_count,
            self._chunk_size,
        )
        return region

    def close(self) -> list[RegionKey]:
        """Close all owned regions: allocator cleanup only.

        Does NOT unmap — the caller's mapper handles all unmaps.
        Returns list of region keys for the caller to clean up.
        Thread-safe: protected by internal lock.

        Returns:
            List of region keys that were closed.
        """
        with self._lock:
            region_keys = list(self._region_order)

            for key in region_keys:
                region = self._regions.get(key)
                if region is None:
                    continue

                try:
                    region.allocator.close()
                except Exception:
                    logger.error(
                        "Error closing owned region %s allocator", key, exc_info=True
                    )

            self._regions.clear()
            self._region_order.clear()
            self._active_region = None

            return region_keys

    # =========================================================================
    # Allocation
    # =========================================================================

    def allocate(self) -> tuple[RegionKey, int] | None:
        """Allocate a page from any available owned region.

        Thread-safe: protected by internal lock.

        Strategy:
            1. Try active region first (fast path)
            2. If exhausted, scan other regions for free pages
            3. Return None if all exhausted (caller handles expansion)

        Returns:
            (region_key, page_index) on success, None on failure.
        """
        with self._lock:
            # 1. Fast path: try active region
            if self._active_region is not None:
                active = self._regions.get(self._active_region)
                if active is not None:
                    page_index = active.allocator.allocate()
                    if page_index is not None:
                        return (self._active_region, page_index)

            # 2. Scan other regions
            for key in self._region_order:
                if key == self._active_region:
                    continue
                region = self._regions[key]
                if region.allocator.num_free_pages > 0:
                    page_index = region.allocator.allocate()
                    if page_index is not None:
                        self._active_region = key
                        logger.debug("Switched active region to %s", key)
                        return (key, page_index)

            return None

    def free(self, key: RegionKey, page_index: int) -> None:
        """Free a page in the specified owned region.

        Thread-safe: protected by internal lock.

        Raises:
            KeyError: If region key not found
        """
        with self._lock:
            region = self._regions.get(key)
            if region is None:
                raise KeyError(f"Owned region {key} not found")
            region.allocator.free(page_index)

    # =========================================================================
    # Query
    # =========================================================================

    def is_owned(self, key: RegionKey) -> bool:
        """Check if a region is owned by this manager."""
        return key in self._regions

    @property
    def is_full(self) -> bool:
        """Check if all owned regions are exhausted."""
        return all(
            region.allocator.num_free_pages == 0 for region in self._regions.values()
        )

    def get_chunk_size(self) -> int:
        """Return the chunk size."""
        return self._chunk_size

    def get_owned_region(self, key: RegionKey) -> OwnedRegion | None:
        """Get an owned region by key."""
        return self._regions.get(key)

    def get_first_key(self) -> RegionKey | None:
        """Get the first region's key."""
        return self._region_order[0] if self._region_order else None

    def get_first_allocator(self) -> PagedMemoryAllocator | None:
        """Get the first region's allocator (backward compat)."""
        first_key = self.get_first_key()
        if first_key is None:
            return None
        first_region = self._regions.get(first_key)
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

        for key in self._region_order:
            region = self._regions[key]
            s = region.allocator.get_stats()
            s["region_key"] = key
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
