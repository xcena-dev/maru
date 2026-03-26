"""MarufsClient — Python interface to marufs kernel filesystem.

Wraps marufs VFS syscalls for region management and delegates
allocation, permissions, and ownership to the Resource Manager.
"""

import logging
import mmap as mmap_module
import os
import threading

from maru_common.constants import ANY_POOL_ID
from maru_common.resource_manager_client import ResourceManagerClient
from maru_common.types import DaxType, MaruHandle

logger = logging.getLogger(__name__)


class MarufsClient:
    """Python interface to marufs kernel filesystem.

    Provides methods for:
    - Region allocation/free (delegated to Resource Manager)
    - Memory mapping (mmap via standard mmap module)
    - Permission management (delegated to Resource Manager)

    Uses ResourceManagerClient for all RM communication.
    """

    def __init__(self) -> None:
        """Initialize MarufsClient."""
        self._rm = ResourceManagerClient()
        self._fd_cache: dict[int, int] = {}  # region_id → fd
        self._mmap_cache: dict[
            int, tuple[mmap_module.mmap, int]
        ] = {}  # region_id → (mmap, prot)
        self._lock = threading.Lock()

        logger.debug("MarufsClient initialised")

    def __enter__(self) -> "MarufsClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Allocation (delegated to Resource Manager)
    # ------------------------------------------------------------------

    def alloc(self, size: int, pool_id: int = ANY_POOL_ID) -> MaruHandle:
        """Allocate a region via the Resource Manager.

        RM creates the region file on marufs, sets permissions,
        and returns a handle + fd.

        Args:
            size: Region size in bytes.
            pool_id: Pool ID (passed to RM).

        Returns:
            MaruHandle for the new region.
        """
        handle, fd = self._rm.alloc(size, pool_id=pool_id, pool_type=DaxType.MARUFS)
        with self._lock:
            self._fd_cache[handle.region_id] = fd
        logger.info("alloc: region_id=%d size=%d", handle.region_id, size)
        return handle

    def free(self, handle: MaruHandle) -> None:
        """Free a region via the Resource Manager.

        Closes cached fd and mmap, then tells RM to free.

        Args:
            handle: MaruHandle from a previous alloc() call.
        """
        with self._lock:
            entry = self._mmap_cache.pop(handle.region_id, None)
            fd = self._fd_cache.pop(handle.region_id, None)

        if entry is not None:
            entry[0].close()
        if fd is not None:
            os.close(fd)

        self._rm.free(handle)
        logger.debug("free: region_id=%d", handle.region_id)

    # ------------------------------------------------------------------
    # Memory mapping (direct)
    # ------------------------------------------------------------------

    def mmap(self, handle: MaruHandle, prot: int, flags: int = 0) -> mmap_module.mmap:
        """Memory-map a region by handle.

        Uses fd from alloc() cache or requests one from RM via get_fd().

        Args:
            handle: MaruHandle from alloc() or server lookup.
            prot: Protection flags (mmap_module.PROT_READ, mmap_module.PROT_WRITE).
            flags: Ignored (present for interface compatibility).

        Returns:
            mmap object for the region.
        """
        if handle.region_id < 0:
            raise ValueError(f"Invalid region_id: {handle.region_id}")

        with self._lock:
            # Return cached mmap if compatible with requested prot
            entry = self._mmap_cache.get(handle.region_id)
            if entry is not None:
                cached_mm, cached_prot = entry
                if prot != cached_prot:
                    raise ValueError(
                        f"Region {handle.region_id} already mapped with prot=0x{cached_prot:x}, "
                        f"cannot remap with prot=0x{prot:x} (existing users would crash)"
                    )
                return cached_mm

            # Get fd: from alloc cache or request from RM
            fd = self._fd_cache.get(handle.region_id)
            if fd is None:
                fd = self._rm.get_fd(handle)
                self._fd_cache[handle.region_id] = fd

            # Determine access mode
            if prot & mmap_module.PROT_WRITE:
                access = mmap_module.ACCESS_WRITE
            else:
                access = mmap_module.ACCESS_READ

            mm = mmap_module.mmap(fd, handle.length, access=access)
            self._mmap_cache[handle.region_id] = (mm, prot)

            return mm

    def munmap(self, handle: MaruHandle) -> None:
        """Unmap a previously mapped region.

        Args:
            handle: MaruHandle to unmap.
        """
        with self._lock:
            entry = self._mmap_cache.pop(handle.region_id, None)
        if entry is not None:
            entry[0].close()
        logger.debug("munmap: region_id=%d", handle.region_id)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close all cached mmap objects and file descriptors."""
        with self._lock:
            # Close mmaps first (before closing their backing fds)
            for region_id, (mm, _prot) in self._mmap_cache.items():
                try:
                    mm.close()
                except Exception:
                    logger.warning(
                        "close: failed to close mmap for region %d", region_id
                    )
            self._mmap_cache.clear()

            # Close fds
            for region_id, fd in self._fd_cache.items():
                try:
                    os.close(fd)
                except OSError:
                    logger.warning("close: failed to close fd for region %d", region_id)
            self._fd_cache.clear()
