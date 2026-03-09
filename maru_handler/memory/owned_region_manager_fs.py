"""OwnedRegionManagerFs - File-based owned region manager for marufs.

Manages multiple owned regions with PagedMemoryAllocator via MarufsMapper.
Replaces OwnedRegionManager (DaxMapper-based, RPC-dependent) for marufs/fs mode.
"""

import logging
import threading

from .allocator import PagedMemoryAllocator
from .marufs_mapper import MarufsMappedRegion, MarufsMapper  # noqa: F401

logger = logging.getLogger(__name__)


class OwnedRegionFs:
    """An owned region in marufs/fs architecture (file-based)."""

    __slots__ = ("name", "allocator")

    def __init__(self, name: str, allocator: PagedMemoryAllocator):
        self.name = name
        self.allocator = allocator


class OwnedRegionManagerFs:
    """Manages multiple owned marufs regions for store operations.

    Each owned region is backed by an mmap'd marufs file and managed by
    a PagedMemoryAllocator for page-level allocation.

    Key differences from RPC OwnedRegionManager:
    - Regions are identified by name (str) instead of id (int)
    - Uses MarufsMapper instead of DaxMapper
    - No RPC dependency — expansion creates files directly via marufs
    - add_region creates the marufs file directly (was mmap via MaruHandle)

    Thread-safe: protected by internal lock.
    """

    def __init__(self, mapper: MarufsMapper, chunk_size: int, pool_size: int):
        """Initialize OwnedRegionManagerFs.

        Args:
            mapper: MarufsMapper for mmap/munmap operations
            chunk_size: Page/chunk size for PagedMemoryAllocator
            pool_size: Size in bytes for each region
        """
        self._mapper = mapper
        self._chunk_size = chunk_size
        self._pool_size = pool_size
        self._lock = threading.Lock()

        self._regions: dict[str, OwnedRegionFs] = {}
        self._region_order: list[str] = []
        self._active_region_name: str | None = None
        self._region_counter: int = 0

    # =========================================================================
    # Lifecycle
    # =========================================================================

    def add_region(self, name: str) -> OwnedRegionFs:
        """Create and map a new owned region via MarufsMapper.

        Note: This method must be called under the caller's write lock
        (e.g. MaruHandlerFs._write_lock) to prevent concurrent add_region
        calls with the same name.

        Args:
            name: Region filename (unique)

        Returns:
            The created OwnedRegionFs
        """
        self._mapper.map_owned_region(name, self._pool_size)

        allocator = PagedMemoryAllocator(
            region_id=self._region_counter,  # use counter as id for logging
            pool_size=self._pool_size,
            chunk_size=self._chunk_size,
        )

        region = OwnedRegionFs(name=name, allocator=allocator)
        with self._lock:
            self._regions[name] = region
            self._region_order.append(name)
            self._region_counter += 1

            if self._active_region_name is None:
                self._active_region_name = name

        logger.info(
            "Added owned region %s: pages=%d, chunk_size=%d",
            name,
            allocator.page_count,
            self._chunk_size,
        )
        return region

    def close(self) -> list[str]:
        """Close all owned regions: allocator cleanup only.

        Does NOT unmap — MarufsMapper.close() handles all unmaps.
        Returns list of region names.
        """
        with self._lock:
            region_names = list(self._region_order)

            for name in region_names:
                region = self._regions.get(name)
                if region is None:
                    continue
                try:
                    region.allocator.close()
                except Exception as e:
                    logger.error("Error closing region %s allocator: %s", name, e)

            self._regions.clear()
            self._region_order.clear()
            self._active_region_name = None

            return region_names

    # =========================================================================
    # Allocation
    # =========================================================================

    def allocate(self) -> tuple[str, int] | None:
        """Allocate a page from any available owned region.

        Returns:
            (region_name, page_index) on success, None if all exhausted.
        """
        with self._lock:
            # Fast path: try active region
            if self._active_region_name is not None:
                active = self._regions.get(self._active_region_name)
                if active is not None:
                    page_index = active.allocator.allocate()
                    if page_index is not None:
                        return (self._active_region_name, page_index)

            # Scan other regions
            for name in self._region_order:
                if name == self._active_region_name:
                    continue
                region = self._regions[name]
                if region.allocator.num_free_pages > 0:
                    page_index = region.allocator.allocate()
                    if page_index is not None:
                        self._active_region_name = name
                        return (name, page_index)

            return None

    def free(self, region_name: str, page_index: int) -> None:
        """Free a page in the specified owned region.

        Raises:
            KeyError: If region_name not found
        """
        with self._lock:
            region = self._regions.get(region_name)
            if region is None:
                raise KeyError(f"Owned region {region_name} not found")
            region.allocator.free(page_index)

    # =========================================================================
    # Query
    # =========================================================================

    def is_owned(self, region_name: str) -> bool:
        """Check if a region is owned by this manager."""
        return region_name in self._regions

    @property
    def is_full(self) -> bool:
        """Check if all owned regions are exhausted."""
        return all(r.allocator.num_free_pages == 0 for r in self._regions.values())

    def get_chunk_size(self) -> int:
        return self._chunk_size

    def get_first_region_name(self) -> str | None:
        return self._region_order[0] if self._region_order else None

    def get_first_allocator(self) -> PagedMemoryAllocator | None:
        first = self.get_first_region_name()
        if first is None:
            return None
        region = self._regions.get(first)
        return region.allocator if region else None

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

        for name in self._region_order:
            region = self._regions[name]
            s = region.allocator.get_stats()
            s["region_name"] = name
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
