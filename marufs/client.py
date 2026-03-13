"""MarufsClient — Python interface to marufs kernel filesystem.

Wraps marufs VFS syscalls (open/ftruncate/unlink/listdir) and ioctl commands
(global name index, permissions) in a convenient Python API.

marufs uses a partitioned global index for name-ref entries. Calling
name_offset(fd, name, offset) on a region's fd inserts a name-ref into
the global index with that region's RAT entry ID. find_name() searches
the global index and returns (region_name, offset).
"""

import ctypes
import errno
import fcntl
import logging
import mmap as mmap_module
import os
import re
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from maru_shm.types import MaruHandle

from .ioctl import (
    MARUFS_BATCH_FIND_MAX,
    MARUFS_BATCH_STORE_MAX,
    MARUFS_IOC_BATCH_FIND_NAME,
    MARUFS_IOC_BATCH_NAME_OFFSET,
    MARUFS_IOC_CLEAR_NAME,
    MARUFS_IOC_FIND_NAME,
    MARUFS_IOC_NAME_OFFSET,
    MARUFS_IOC_PERM_GRANT,
    MARUFS_IOC_PERM_REVOKE,
    MARUFS_IOC_PERM_SET_DEFAULT,
    MARUFS_NAME_MAX,
    PERM_ALL,
    MarufsBatchFindEntry,
    MarufsBatchFindReq,
    MarufsBatchNameOffsetEntry,
    MarufsBatchNameOffsetReq,
    MarufsFindNameReq,
    MarufsNameOffsetReq,
    MarufsPermReq,
)

logger = logging.getLogger(__name__)

# Pre-defined ctypes array types to avoid per-call type creation overhead.
_BatchFindArray = MarufsBatchFindEntry * MARUFS_BATCH_FIND_MAX
_BatchStoreArray = MarufsBatchNameOffsetEntry * MARUFS_BATCH_STORE_MAX

_REGION_NAME_RE = re.compile(r"^[A-Za-z0-9_\-]+$")


class MarufsClient:
    """Python interface to marufs kernel filesystem.

    Provides methods for:
    - Region management (create/open/delete/list via VFS syscalls)
    - Global name index operations (name-ref registration/lookup via ioctl)
    - Permission management (grant/revoke via ioctl)
    - Memory mapping (mmap via standard mmap module)

    File descriptors are cached internally; call :meth:`close` when done.
    """

    def __init__(self, mount_path: str) -> None:
        """Initialize MarufsClient.

        Args:
            mount_path: Path where marufs is mounted (e.g., ``"/mnt/marufs"``).
        """
        self._mount_path = mount_path
        self._fds: dict[str, int] = {}  # name → fd cache
        self._fd_modes: dict[str, bool] = {}  # name → readonly flag
        self._dir_fd: int | None = None  # directory fd for find_name
        self._node_id: int | None = None  # cached node_id from /proc/mounts
        self._real_mount_path = os.path.realpath(self._mount_path)
        self._lock = threading.Lock()

        # MaruShmClient-compatible state (alloc/free/mmap/munmap)
        self._next_region_id = 1
        self._region_names: dict[int, str] = {}  # region_id → filename
        self._mmap_cache: dict[int, mmap_module.mmap] = {}  # region_id → mmap
        self._perm_all = PERM_ALL

        logger.debug("MarufsClient initialised with mount_path=%s", mount_path)

    def _validate_region_name(self, name: str) -> None:
        """Validate region name to prevent path traversal attacks.

        Args:
            name: Region filename to validate.

        Raises:
            ValueError: If name contains invalid characters or path traversal.
        """
        if not _REGION_NAME_RE.match(name):
            raise ValueError(f"Invalid region name {name!r}: must match [A-Za-z0-9_-]+")
        real_mount = self._real_mount_path
        real_path = os.path.realpath(os.path.join(self._mount_path, name))
        if not (real_path == real_mount or real_path.startswith(real_mount + os.sep)):
            raise ValueError(f"path traversal detected for region name {name!r}")

    # ------------------------------------------------------------------
    # Region management
    # ------------------------------------------------------------------

    def _create_region(self, name: str, size: int) -> int:
        """Create a new region file and set its size.

        Opens the file with ``O_CREAT | O_RDWR``, calls ``ftruncate`` to
        allocate *size* bytes, then caches the fd.

        Args:
            name: Region filename (must not already exist).
            size: Desired size in bytes.

        Returns:
            Open file descriptor for the new region.
        """
        self._validate_region_name(name)
        path = os.path.join(self._mount_path, name)
        logger.debug("create_region: path=%s size=%d", path, size)
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        os.ftruncate(fd, size)
        self._fds[name] = fd
        self._fd_modes[name] = False  # created as RDWR
        return fd

    def _open_region(self, name: str, readonly: bool = True) -> int:
        """Open an existing region file.

        Returns a cached fd if the region was already opened in this session.

        Args:
            name:     Region filename.
            readonly: If ``True`` (default), open with ``O_RDONLY``; otherwise
                      ``O_RDWR``.

        Returns:
            Open file descriptor for the region.
        """
        self._validate_region_name(name)
        if name in self._fds:
            # Re-open if cached fd was opened with a different mode
            if self._fd_modes.get(name) != readonly:
                logger.debug(
                    "open_region: mode mismatch for %s (cached readonly=%s, "
                    "requested readonly=%s), re-opening",
                    name,
                    self._fd_modes.get(name),
                    readonly,
                )
                try:
                    os.close(self._fds.pop(name))
                except OSError:
                    logger.warning("open_region: failed to close old fd for %s", name)
                self._fd_modes.pop(name, None)
            else:
                logger.debug("open_region: returning cached fd for %s", name)
                return self._fds[name]
        path = os.path.join(self._mount_path, name)
        flags = os.O_RDONLY if readonly else os.O_RDWR
        logger.debug("open_region: path=%s readonly=%s", path, readonly)
        fd = os.open(path, flags)
        self._fds[name] = fd
        self._fd_modes[name] = readonly
        return fd

    def _delete_region(self, name: str) -> None:
        """Delete a region file (unlink).

        Closes and removes the cached fd before unlinking.

        Args:
            name: Region filename to delete.
        """
        self._validate_region_name(name)
        if name in self._fds:
            try:
                os.close(self._fds.pop(name))
            except OSError:
                logger.warning("delete_region: failed to close fd for %s", name)
            self._fd_modes.pop(name, None)
        path = os.path.join(self._mount_path, name)
        logger.debug("delete_region: unlinking %s", path)
        os.unlink(path)

    def _list_regions(self, prefix: str | None = None) -> list[str]:
        """List region files in the marufs mount point.

        Args:
            prefix: If provided, only return entries whose names start with
                    this string.

        Returns:
            Sorted list of region filenames.
        """
        try:
            entries = os.listdir(self._mount_path)
        except OSError:
            logger.warning("list_regions: could not listdir %s", self._mount_path)
            return []
        if prefix:
            entries = [e for e in entries if e.startswith(prefix)]
        return sorted(entries)

    def _exists(self, name: str) -> bool:
        """Check whether a region file exists in the marufs mount.

        Args:
            name: Region filename.

        Returns:
            ``True`` if the file exists.
        """
        self._validate_region_name(name)
        path = os.path.join(self._mount_path, name)
        return os.path.exists(path)

    def _get_fd(self, name: str) -> int | None:
        """Return the cached file descriptor for *name*, or ``None``.

        Args:
            name: Region filename.

        Returns:
            Cached fd, or ``None`` if not cached.
        """
        return self._fds.get(name)

    # ------------------------------------------------------------------
    # Global name index (ioctl)
    # ------------------------------------------------------------------

    def name_offset(
        self, fd: int, name: str | bytes, offset: int, name_hash: int = 0
    ) -> None:
        """Register a name-ref in the global index: name → (region, offset).

        Issues ``ioctl(MARUFS_IOC_NAME_OFFSET)`` on the region's fd.

        Args:
            fd:        Open file descriptor for the **data region**.
            name:      Key string or bytes (max MARUFS_NAME_MAX bytes UTF-8).
            offset:    Byte offset within the region's data area.
            name_hash: Pre-computed hash (0 = kernel uses djb2 fallback).
        """
        req = MarufsNameOffsetReq()
        req.name = (name if isinstance(name, bytes) else name.encode("utf-8"))[
            :MARUFS_NAME_MAX
        ]
        req.offset = offset
        req.name_hash = name_hash
        logger.debug("name_offset: fd=%d name=%s offset=%d", fd, name, offset)
        fcntl.ioctl(fd, MARUFS_IOC_NAME_OFFSET, req)

    def batch_name_offset(
        self,
        fd: int,
        names: list[str | bytes],
        offsets: list[int],
        hashes: list[int] | None = None,
    ) -> list[bool]:
        """Register multiple name-refs in the global index with a single ioctl.

        Issues ``ioctl(MARUFS_IOC_BATCH_NAME_OFFSET)``. Up to 512 entries
        per call; automatically splits if len(names) > 512.

        Args:
            fd:      Open file descriptor for the **data region**.
            names:   List of key strings or bytes.
            offsets: List of byte offsets within the region.
            hashes:  Optional pre-computed hashes (0 = djb2 fallback).

        Returns:
            List of booleans indicating success for each entry.
        """
        if not names:
            return []

        results: list[bool] = []

        for chunk_start in range(0, len(names), MARUFS_BATCH_STORE_MAX):
            chunk_names = names[chunk_start : chunk_start + MARUFS_BATCH_STORE_MAX]
            n = len(chunk_names)

            entries = _BatchStoreArray()

            for i, name in enumerate(chunk_names):
                raw = name if isinstance(name, bytes) else name.encode("utf-8")
                entries[i].name = raw[:MARUFS_NAME_MAX]
                entries[i].offset = offsets[chunk_start + i]
                if hashes is not None:
                    entries[i].name_hash = hashes[chunk_start + i]

            req = MarufsBatchNameOffsetReq()
            req.count = n
            req.stored = 0
            req.entries = ctypes.addressof(entries)

            fcntl.ioctl(fd, MARUFS_IOC_BATCH_NAME_OFFSET, req)

            for i in range(n):
                results.append(entries[i].status == 0)

            logger.debug(
                "batch_name_offset: chunk=%d names, stored=%d",
                n,
                req.stored,
            )

        return results

    def find_name(
        self, fd: int, name: str | bytes, name_hash: int = 0
    ) -> tuple[str, int] | None:
        """Look up a name-ref in the global index.

        Issues ``ioctl(MARUFS_IOC_FIND_NAME)``. Callable on any fd (file or
        directory). Returns the region filename and byte offset.

        Args:
            fd:        Any open file descriptor on the marufs mount.
            name:      Key string or bytes to look up.
            name_hash: Pre-computed hash (0 = kernel uses djb2 fallback).

        Returns:
            ``(region_name, offset)`` if found, ``None`` if not present.
        """
        req = MarufsFindNameReq()
        req.name = (name if isinstance(name, bytes) else name.encode("utf-8"))[
            :MARUFS_NAME_MAX
        ]
        req.name_hash = name_hash
        try:
            fcntl.ioctl(fd, MARUFS_IOC_FIND_NAME, req)
            region_name = req.region_name.decode("utf-8").rstrip("\x00")
            return (region_name, req.offset)
        except OSError as e:
            if e.errno == errno.ENOENT:
                return None
            raise

    def batch_find_name(
        self,
        fd: int,
        names: list[str | bytes],
        hashes: list[int] | None = None,
    ) -> list[tuple[str, int] | None]:
        """Look up multiple name-refs in the global index with a single ioctl.

        Issues ``ioctl(MARUFS_IOC_BATCH_FIND_NAME)``. Up to 32 names per ioctl
        call; automatically splits into multiple ioctl calls if needed.

        Args:
            fd:     Any open file descriptor on the marufs mount.
            names:  List of key strings or bytes to look up.
            hashes: Optional pre-computed hashes (u64) per name.
                    If provided, kernel skips djb2 hash computation.
                    Pass 0 per entry for kernel-side fallback.

        Returns:
            List of ``(region_name, offset)`` for found entries, ``None`` for
            entries not found.
        """
        if not names:
            return []

        results: list[tuple[str, int] | None] = []

        for chunk_start in range(0, len(names), MARUFS_BATCH_FIND_MAX):
            chunk = names[chunk_start : chunk_start + MARUFS_BATCH_FIND_MAX]
            n = len(chunk)

            entries = _BatchFindArray()

            for i, name in enumerate(chunk):
                raw = name if isinstance(name, bytes) else name.encode("utf-8")
                entries[i].name = raw[:MARUFS_NAME_MAX]
                if hashes is not None:
                    entries[i].name_hash = hashes[chunk_start + i]

            req = MarufsBatchFindReq()
            req.count = n
            req.found = 0
            req.entries = ctypes.addressof(entries)

            fcntl.ioctl(fd, MARUFS_IOC_BATCH_FIND_NAME, req)

            for i in range(n):
                if entries[i].status == 0:
                    region_name = entries[i].region_name.decode("utf-8").rstrip("\x00")
                    results.append((region_name, entries[i].offset))
                else:
                    results.append(None)

            logger.debug(
                "batch_find_name: chunk=%d names, found=%d",
                n,
                req.found,
            )

        return results

    def clear_name(self, fd: int, name: str | bytes, name_hash: int = 0) -> None:
        """Remove a name-ref from the global index.

        Issues ``ioctl(MARUFS_IOC_CLEAR_NAME)``.

        Args:
            fd:        Open file descriptor for the region (permission check).
            name:      Key string or bytes to remove.
            name_hash: Pre-computed hash (0 = kernel uses djb2 fallback).
        """
        req = MarufsNameOffsetReq()
        req.name = (name if isinstance(name, bytes) else name.encode("utf-8"))[
            :MARUFS_NAME_MAX
        ]
        req.offset = 0
        req.name_hash = name_hash
        logger.debug("clear_name: fd=%d name=%s", fd, name)
        fcntl.ioctl(fd, MARUFS_IOC_CLEAR_NAME, req)

    def get_dir_fd(self) -> int:
        """Get a directory fd for the marufs mount (for find_name calls).

        The fd is cached and reused across calls.

        Returns:
            Open file descriptor for the mount directory.
        """
        if self._dir_fd is not None:
            return self._dir_fd
        with self._lock:
            if self._dir_fd is None:
                self._dir_fd = os.open(self._mount_path, os.O_RDONLY | os.O_DIRECTORY)
                logger.debug(
                    "get_dir_fd: opened dir fd=%d for %s",
                    self._dir_fd,
                    self._mount_path,
                )
            return self._dir_fd

    # ------------------------------------------------------------------
    # Node ID (from /proc/mounts)
    # ------------------------------------------------------------------

    def get_node_id(self) -> int:
        """Read node_id for this mount point from ``/proc/mounts``.

        Parses the marufs mount options (e.g., ``rw,relatime,node_id=0``)
        to extract the ``node_id`` value. The result is cached.

        Returns:
            Node ID integer.

        Raises:
            RuntimeError: If mount point not found or node_id option missing.
        """
        if self._node_id is not None:
            return self._node_id

        mount_path = os.path.realpath(self._mount_path)
        try:
            with open("/proc/mounts") as f:
                for line in f:
                    parts = line.split()
                    if len(parts) < 4:
                        continue
                    # parts: device, mountpoint, fstype, options, ...
                    if os.path.realpath(parts[1]) == mount_path:
                        for opt in parts[3].split(","):
                            if opt.startswith("node_id="):
                                self._node_id = int(opt.split("=", 1)[1])
                                logger.debug(
                                    "get_node_id: mount=%s node_id=%d",
                                    self._mount_path,
                                    self._node_id,
                                )
                                return self._node_id
                        raise RuntimeError(
                            f"marufs mount {self._mount_path} has no node_id option. "
                            f"Update marufs kernel module to include show_options."
                        )
        except OSError as e:
            raise RuntimeError(f"Cannot read /proc/mounts: {e}") from e

        raise RuntimeError(f"Mount point {self._mount_path} not found in /proc/mounts")

    # ------------------------------------------------------------------
    # Permission management (ioctl)
    # ------------------------------------------------------------------

    def perm_grant(self, fd: int, node_id: int, pid: int, perms: int) -> None:
        """Grant permissions to a process.

        Issues ``ioctl(MARUFS_IOC_PERM_GRANT)``.

        Args:
            fd:      Open file descriptor for the region.
            node_id: NUMA/CXL node identifier.
            pid:     Target process ID.
            perms:   Permission flags (combine :data:`PERM_READ` / :data:`PERM_WRITE`).
        """
        req = MarufsPermReq(node_id=node_id, pid=pid, perms=perms, reserved=0)
        logger.debug(
            "perm_grant: fd=%d node_id=%d pid=%d perms=0x%x",
            fd,
            node_id,
            pid,
            perms,
        )
        fcntl.ioctl(fd, MARUFS_IOC_PERM_GRANT, req)

    def perm_revoke(self, fd: int, node_id: int, pid: int) -> None:
        """Revoke permissions from a process.

        Issues ``ioctl(MARUFS_IOC_PERM_REVOKE)``.

        Args:
            fd:      Open file descriptor for the region.
            node_id: NUMA/CXL node identifier.
            pid:     Target process ID.
        """
        req = MarufsPermReq(node_id=node_id, pid=pid, perms=0, reserved=0)
        logger.debug("perm_revoke: fd=%d node_id=%d pid=%d", fd, node_id, pid)
        fcntl.ioctl(fd, MARUFS_IOC_PERM_REVOKE, req)

    def perm_set_default(self, fd: int, perms: int) -> None:
        """Set the default permissions for a region.

        Issues ``ioctl(MARUFS_IOC_PERM_SET_DEFAULT)``.
        Kernel expects marufs_perm_req struct (node_id/pid ignored for defaults).

        Args:
            fd:    Open file descriptor for the region.
            perms: Default permission flags.
        """
        req = MarufsPermReq(node_id=0, pid=0, perms=perms, reserved=0)
        logger.debug("perm_set_default: fd=%d perms=0x%x", fd, perms)
        fcntl.ioctl(fd, MARUFS_IOC_PERM_SET_DEFAULT, req)

    # ------------------------------------------------------------------
    # Memory mapping
    # ------------------------------------------------------------------

    def _mmap_region(
        self, fd: int, size: int, prot: int = mmap_module.PROT_READ
    ) -> mmap_module.mmap:
        """Memory-map a region file.

        Args:
            fd:   Open file descriptor for the region.
            size: Number of bytes to map.
            prot: Protection flags — use :data:`mmap_module.PROT_READ` for read-only
                  or ``mmap_module.PROT_READ | mmap_module.PROT_WRITE`` for read-write.
                  Defaults to :data:`mmap_module.PROT_READ`.

        Returns:
            :class:`mmap_module.mmap` object covering the region.
        """
        if prot & mmap_module.PROT_WRITE:
            access = mmap_module.ACCESS_WRITE
        else:
            access = mmap_module.ACCESS_READ
        logger.debug("mmap_region: fd=%d size=%d access=%s", fd, size, access)
        return mmap_module.mmap(fd, size, access=access)

    # ------------------------------------------------------------------
    # MaruShmClient-compatible interface (alloc / free / mmap / munmap)
    # ------------------------------------------------------------------

    def alloc(self, size: int, pool_id: int = 0) -> "MaruHandle":
        """Allocate a region, returning a MaruHandle (MaruShmClient compat).

        Creates a region file on the marufs mount, sets default permissions,
        and returns a MaruHandle with a synthetic region_id.

        Args:
            size: Region size in bytes.
            pool_id: Ignored (present for interface compatibility).

        Returns:
            MaruHandle for the new region.
        """
        from maru_shm.types import MaruHandle

        with self._lock:
            region_id = self._next_region_id
            self._next_region_id += 1

        region_name = f"region_{region_id}"
        fd = self._create_region(region_name, size)
        self.perm_set_default(fd, self._perm_all)

        handle = MaruHandle(
            region_id=region_id,
            offset=0,
            length=size,
            auth_token=0,
        )

        with self._lock:
            self._region_names[region_id] = region_name

        logger.info(
            "alloc: created region %s (id=%d, size=%d)",
            region_name,
            region_id,
            size,
        )
        return handle

    def free(self, handle: "MaruHandle") -> None:
        """Free a region (MaruShmClient compat).

        Closes the fd, unmaps if mapped, and deletes the region file.

        Args:
            handle: MaruHandle from a previous alloc() call.
        """
        with self._lock:
            region_name = self._region_names.pop(handle.region_id, None)
            mm = self._mmap_cache.pop(handle.region_id, None)

        if mm is not None:
            mm.close()

        if region_name is not None:
            self._close_fd(region_name)
            try:
                self._delete_region(region_name)
            except OSError as e:
                logger.warning("free: delete_region failed for %s: %s", region_name, e)
        else:
            logger.warning("free: unknown region_id=%d", handle.region_id)

    def mmap(self, handle: "MaruHandle", prot: int, flags: int = 0) -> mmap_module.mmap:
        """Memory-map a region by handle (MaruShmClient compat).

        Opens the region file if not already open, then mmaps it.

        Args:
            handle: MaruHandle from alloc() or server lookup.
            prot: Protection flags (mmap_module.PROT_READ, mmap_module.PROT_WRITE).
            flags: Ignored (present for interface compatibility).

        Returns:
            mmap object for the region.
        """
        with self._lock:
            # Return cached mmap if available
            cached = self._mmap_cache.get(handle.region_id)
            if cached is not None:
                return cached

            region_name = self._region_names.get(handle.region_id)

        if region_name is None:
            # Derive name from region_id (for shared regions from other instances)
            region_name = f"region_{handle.region_id}"
            with self._lock:
                self._region_names[handle.region_id] = region_name

        # Open if not already open
        if self._get_fd(region_name) is None:
            readonly = not (prot & mmap_module.PROT_WRITE)
            self._open_region(region_name, readonly=readonly)

        fd = self._get_fd(region_name)
        mm = self._mmap_region(fd, handle.length, prot)

        with self._lock:
            self._mmap_cache[handle.region_id] = mm

        return mm

    def munmap(self, handle: "MaruHandle") -> None:
        """Unmap a previously mapped region (MaruShmClient compat).

        Args:
            handle: MaruHandle to unmap.
        """
        with self._lock:
            mm = self._mmap_cache.pop(handle.region_id, None)
        if mm is not None:
            mm.close()
        logger.debug("munmap: region_id=%d", handle.region_id)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _close_fd(self, name: str) -> None:
        """Close and remove a cached file descriptor for *name*.

        Args:
            name: Region filename whose fd should be closed.
        """
        fd = self._fds.pop(name, None)
        self._fd_modes.pop(name, None)
        if fd is not None:
            logger.debug("close_fd: closing fd=%d for %s", fd, name)
            os.close(fd)

    def close(self) -> None:
        """Close all cached file descriptors."""
        logger.debug("close: closing %d fds", len(self._fds))
        for name in list(self._fds):
            try:
                os.close(self._fds[name])
            except OSError:
                logger.warning("close: failed to close fd for %s", name)
        self._fds.clear()
        self._fd_modes.clear()

        if self._dir_fd is not None:
            try:
                os.close(self._dir_fd)
            except OSError:
                logger.warning("close: failed to close dir fd")
            self._dir_fd = None
