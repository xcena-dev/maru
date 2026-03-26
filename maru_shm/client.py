# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruShmClient — shared memory client for the Maru Resource Manager.

Communicates with the resource manager over UDS using the binary IPC protocol
defined in maru_shm.ipc.
"""

import logging
import mmap as mmap_module
import os
import threading

from maru_common.resource_manager_client import ResourceManagerClient

from .constants import ANY_POOL_ID, MAP_SHARED
from .types import MaruHandle, MaruPoolInfo

logger = logging.getLogger(__name__)


class MaruShmClient:
    """Client for the Maru Resource Manager.

    FDs received from alloc/get_fd are cached by region_id.
    mmap() returns Python mmap objects (buffer protocol).

    Uses ResourceManagerClient for all RM communication.
    """

    def __init__(self, socket_path: str | None = None):
        self._rm = ResourceManagerClient(socket_path)
        self._fd_cache: dict[int, int] = {}  # region_id -> fd
        self._mmap_cache: dict[int, mmap_module.mmap] = {}  # region_id -> mmap
        self._lock = threading.Lock()

    def stats(self) -> list[MaruPoolInfo]:
        """Query pool statistics from the resource manager."""
        return self._rm.stats()

    def alloc(self, size: int, pool_id: int = ANY_POOL_ID) -> MaruHandle:
        """Allocate shared memory from the resource manager."""
        handle, fd = self._rm.alloc(size, pool_id=pool_id)
        with self._lock:
            self._fd_cache[handle.region_id] = fd
        logger.debug(
            "alloc(size=%d, pool_id=%d) -> region_id=%d",
            size, pool_id, handle.region_id,
        )
        return handle

    def free(self, handle: MaruHandle) -> None:
        """Free a previously allocated handle."""
        with self._lock:
            self._close_region_locked(handle.region_id)
        self._rm.free(handle)
        logger.debug("free(region_id=%d)", handle.region_id)

    def _request_fd(self, handle: MaruHandle) -> int:
        """Request an FD from the resource manager."""
        return self._rm.get_fd(handle)

    def mmap(self, handle: MaruHandle, prot: int, flags: int = 0) -> mmap_module.mmap:
        """Memory-map a handle into the calling process.

        Args:
            handle: Handle from alloc() or lookup.
            prot: Protection flags (PROT_READ | PROT_WRITE).
            flags: Mapping flags (defaults to MAP_SHARED).

        Returns:
            Python mmap object with buffer protocol support.
        """
        with self._lock:
            # Return cached mmap if available
            if handle.region_id in self._mmap_cache:
                return self._mmap_cache[handle.region_id]

            # Get FD from cache or request from resource manager
            fd = self._fd_cache.get(handle.region_id)
            if fd is None:
                fd = self._request_fd(handle)
                self._fd_cache[handle.region_id] = fd

            if flags == 0:
                flags = MAP_SHARED

            # Convert our prot/flags to Python mmap access mode
            access = mmap_module.ACCESS_READ
            if prot & 0x2:  # PROT_WRITE
                access = mmap_module.ACCESS_WRITE

            mm = mmap_module.mmap(
                fd,
                handle.length,
                access=access,
                offset=handle.offset,
            )

            self._mmap_cache[handle.region_id] = mm

        logger.debug(
            "mmap(region_id=%d, length=%d, offset=%d)",
            handle.region_id,
            handle.length,
            handle.offset,
        )
        return mm

    def munmap(self, handle: MaruHandle) -> None:
        """Unmap a previously mapped handle.

        Args:
            handle: Handle to unmap.
        """
        with self._lock:
            mm = self._mmap_cache.pop(handle.region_id, None)
        if mm is not None:
            mm.close()
        logger.debug("munmap(region_id=%d)", handle.region_id)

    # perm_grant, perm_revoke, perm_set_default, chown inherited from ResourceManagerClient

    def _close_region_locked(self, region_id: int) -> None:
        """Close mmap and FD for a region (must hold self._lock)."""
        mm = self._mmap_cache.pop(region_id, None)
        if mm is not None:
            mm.close()
        fd = self._fd_cache.pop(region_id, None)
        if fd is not None:
            os.close(fd)

    def close(self) -> None:
        """Close all cached FDs and mmaps."""
        with self._lock:
            num_mmaps = len(self._mmap_cache)
            num_fds = len(self._fd_cache)
            for region_id in list(self._mmap_cache.keys()):
                self._close_region_locked(region_id)
            # Close any remaining FDs without mmaps
            for _region_id, fd in list(self._fd_cache.items()):
                os.close(fd)
            self._fd_cache.clear()
            self._mmap_cache.clear()
        logger.debug("close(): released %d mmaps, %d fds", num_mmaps, num_fds)

    def __del__(self) -> None:
        self.close()
