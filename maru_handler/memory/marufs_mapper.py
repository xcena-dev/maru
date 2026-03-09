"""MarufsMapper - File-based memory mapping via MarufsClient.

Replaces DaxMapper for marufs/fs mode. Maps marufs regions via standard
open()/mmap() instead of UDS+SCM_RIGHTS.
"""

import ctypes
import logging
import mmap as mmap_module
import os
import threading
import time

from marufs import MarufsClient
from marufs.ioctl import PERM_ALL

logger = logging.getLogger(__name__)

# CUDA host register flags
_CUDA_HOST_REGISTER_DEFAULT = 0
_CUDA_HOST_REGISTER_READ_ONLY = 8  # cudaHostRegisterReadOnly (CUDA 11.1+)

# Prefault constants (same pattern as DaxMapper)
_MADV_POPULATE_READ = getattr(mmap_module, "MADV_POPULATE_READ", 22)
_MADV_POPULATE_WRITE = getattr(mmap_module, "MADV_POPULATE_WRITE", 23)
_PREFAULT_ENABLED = os.environ.get("MARU_PREFAULT", "1") != "0"
_PAGE_SIZE = os.sysconf("SC_PAGESIZE") if hasattr(os, "sysconf") else 4096


# Buffer address extraction — works for both read-only and writable buffers.
# ctypes.c_char.from_buffer() requires PyBUF_WRITABLE which read-only mmaps
# (PROT_READ) cannot provide. We use PyObject_GetBuffer with PyBUF_SIMPLE (0)
# which accepts read-only buffers.


class _PyBuffer(ctypes.Structure):  # noqa: N801
    """CPython Py_buffer struct for buffer protocol access."""

    _fields_ = [
        ("buf", ctypes.c_void_p),
        ("obj", ctypes.py_object),
        ("len", ctypes.c_ssize_t),
        ("itemsize", ctypes.c_ssize_t),
        ("readonly", ctypes.c_int),
        ("ndim", ctypes.c_int),
        ("format", ctypes.c_char_p),
        ("shape", ctypes.POINTER(ctypes.c_ssize_t)),
        ("strides", ctypes.POINTER(ctypes.c_ssize_t)),
        ("suboffsets", ctypes.POINTER(ctypes.c_ssize_t)),
        ("internal", ctypes.c_void_p),
    ]


def _get_buffer_addr(buf: object) -> int:
    """Get the base virtual address of a buffer object.

    Works for both writable and read-only buffers (e.g. PROT_READ mmap).
    Uses PyObject_GetBuffer with PyBUF_SIMPLE which doesn't require write access.
    """
    try:
        # Fast path: writable buffer
        return ctypes.addressof(ctypes.c_char.from_buffer(buf))
    except TypeError:
        # Read-only buffer: use buffer protocol directly
        pybuf = _PyBuffer()
        ret = ctypes.pythonapi.PyObject_GetBuffer(
            ctypes.py_object(buf),
            ctypes.byref(pybuf),
            0,  # PyBUF_SIMPLE
        )
        if ret != 0:
            raise BufferError(
                "PyObject_GetBuffer failed for read-only buffer"
            ) from None
        try:
            return pybuf.buf
        finally:
            ctypes.pythonapi.PyBuffer_Release(ctypes.byref(pybuf))


class MarufsMappedRegion:
    """A memory-mapped marufs region.

    Tracks fd, mmap, memoryview, and metadata for one region file.
    Eagerly creates memoryview(mmap_obj) at construction time.
    """

    def __init__(
        self,
        name: str,
        fd: int,
        mmap_obj: mmap_module.mmap,
        size: int,
        owned: bool,
    ):
        self.name = name
        self.fd = fd
        self.size = size
        self.owned = owned
        self._mmap_obj: mmap_module.mmap | None = mmap_obj
        self._buffer_view: memoryview | None = memoryview(mmap_obj)
        self._cuda_pinned = False

    @property
    def is_mapped(self) -> bool:
        return self._mmap_obj is not None

    def get_buffer_view(self, offset: int, size: int) -> memoryview | None:
        """Return a zero-copy memoryview slice."""
        if self._buffer_view is None:
            return None
        if offset < 0 or size < 0 or offset + size > len(self._buffer_view):
            return None
        return self._buffer_view[offset : offset + size]

    def release(self) -> None:
        """Release memoryview and mmap references."""
        self._buffer_view = None
        if self._mmap_obj is not None:
            self._mmap_obj.close()
            self._mmap_obj = None


class MarufsMapper:
    """Maps marufs regions via standard open()/mmap().

    Replaces DaxMapper for marufs/fs mode. All memory mapping
    goes through MarufsClient and standard POSIX mmap.

    Thread-safe: protected by internal lock.
    """

    def __init__(self, marufs_client: MarufsClient):
        self._marufs = marufs_client
        self._regions: dict[str, MarufsMappedRegion] = {}  # name → region
        self._lock = threading.Lock()

    def map_owned_region(self, name: str, size: int) -> MarufsMappedRegion:
        """Create and map a new owned region (O_RDWR + PROT_READ|PROT_WRITE).

        Args:
            name: Region filename
            size: Size in bytes

        Returns:
            MarufsMappedRegion with writable mmap
        """
        with self._lock:
            if name in self._regions:
                return self._regions[name]

            fd = self._marufs.create_region(name, size)
            self._marufs.perm_set_default(fd, PERM_ALL)
            mm = self._marufs.mmap_region(
                fd, size, mmap_module.PROT_READ | mmap_module.PROT_WRITE
            )

            region = MarufsMappedRegion(
                name=name, fd=fd, mmap_obj=mm, size=size, owned=True
            )
            self._regions[name] = region

        # Outside lock: prefault and CUDA pin are idempotent
        self._prefault_owned_region(mm, name, size)
        self._cuda_pin(region)

        logger.debug("Mapped owned region %s: size=%d", name, size)

        return region

    def map_shared_region(self, name: str) -> MarufsMappedRegion:
        """Open and map an existing shared region.

        Opens with O_RDWR and maps with PROT_READ|PROT_WRITE for CUDA
        cudaHostRegister compatibility. The region is marked owned=False
        to distinguish from locally-created regions.

        Args:
            name: Region filename

        Returns:
            MarufsMappedRegion with owned=False
        """
        with self._lock:
            if name in self._regions:
                return self._regions[name]

            # O_RDWR + PROT_WRITE required for cudaHostRegister compatibility.
            # Without PROT_WRITE, CUDA pinning fails on shared CXL memory.
            fd = self._marufs.open_region(name, readonly=False)
            size = os.fstat(fd).st_size
            mm = self._marufs.mmap_region(
                fd, size, mmap_module.PROT_READ | mmap_module.PROT_WRITE
            )

            region = MarufsMappedRegion(
                name=name, fd=fd, mmap_obj=mm, size=size, owned=False
            )
            self._regions[name] = region

        # Outside lock: prefault and CUDA pin are idempotent
        self._prefault_shared_region(mm, name, size)
        self._cuda_pin(region)

        logger.debug("Mapped shared region %s: size=%d", name, size)

        return region

    # Read-only accessors: dict.get() is GIL-atomic in CPython.
    # No lock needed; worst case is a stale read during close().

    def get_region(self, name: str) -> MarufsMappedRegion | None:
        """Get a mapped region by name."""
        return self._regions.get(name)

    def is_mapped(self, name: str) -> bool:
        """Check if a region is mapped."""
        return name in self._regions

    def get_fd(self, name: str) -> int | None:
        """Get the fd for a mapped region."""
        region = self._regions.get(name)
        return region.fd if region else None

    def get_buffer_view(self, name: str, offset: int, size: int) -> memoryview | None:
        """Get a memoryview slice from a mapped region."""
        region = self._regions.get(name)
        if region is None:
            return None
        return region.get_buffer_view(offset, size)

    def unmap_region(self, name: str) -> bool:
        """Unmap a region and close its fd.

        Returns:
            True if successfully unmapped
        """
        with self._lock:
            region = self._regions.pop(name, None)
            if region is None:
                return False
            try:
                self._cuda_unpin(region)
                region.release()
                self._marufs.close_fd(
                    name
                )  # closes fd and removes from MarufsClient cache
                logger.debug("Unmapped region %s", name)
                return True
            except Exception as e:
                logger.error("Error unmapping region %s: %s", name, e)
                return False

    def close(self) -> None:
        """Unmap all regions."""
        with self._lock:
            for name in list(self._regions):
                region = self._regions.pop(name, None)
                if region is None:
                    continue
                try:
                    self._cuda_unpin(region)
                    region.release()
                    self._marufs.close_fd(
                        name
                    )  # closes fd and removes from MarufsClient cache
                except Exception as e:
                    logger.error("Failed to unmap region %s during close: %s", name, e)
            self._regions.clear()

    def _cuda_pin(self, region: MarufsMappedRegion) -> None:
        """Register mmap memory with CUDA for GPU DMA access.

        Uses region._mmap_obj (not memoryview) for address extraction —
        Python mmap objects export writable buffer protocol regardless of
        PROT_READ, avoiding BufferError on read-only memoryviews.

        For shared (read-only) regions, uses cudaHostRegisterReadOnly flag
        (CUDA 11.1+) to enable GPU DMA without write permission.
        """
        if region._mmap_obj is None:
            return
        try:
            import torch

            if torch.cuda.is_available():
                addr = _get_buffer_addr(region._mmap_obj)
                cudart = torch.cuda.cudart()

                # Try cudaHostRegisterReadOnly for shared regions,
                # fall back to default flags if it fails (driver/HW compat)
                flags_to_try = (
                    [_CUDA_HOST_REGISTER_DEFAULT]
                    if region.owned
                    else [_CUDA_HOST_REGISTER_READ_ONLY, _CUDA_HOST_REGISTER_DEFAULT]
                )
                for flags in flags_to_try:
                    t0 = time.monotonic()
                    ret = cudart.cudaHostRegister(addr, region.size, flags)
                    elapsed_ms = (time.monotonic() - t0) * 1000
                    err = ret[0] if isinstance(ret, tuple) else ret
                    if err == 0:
                        region._cuda_pinned = True
                        logger.info(
                            "CUDA pinned region %s (%d bytes, flags=%d) in %.1fms",
                            region.name,
                            region.size,
                            flags,
                            elapsed_ms,
                        )
                        return
                    # Clear CUDA error state before retry/exit
                    if hasattr(cudart, "cudaGetLastError"):
                        cudart.cudaGetLastError()
                    else:
                        torch.cuda.init()
                    logger.warning(
                        "cudaHostRegister failed for %s (flags=%d, err=%s)",
                        region.name,
                        flags,
                        err,
                    )
                # All attempts failed
                logger.warning(
                    "cudaHostRegister: all flag variants failed for %s "
                    "(DAX/CXL memory may not support pinning)",
                    region.name,
                )
        except (ImportError, RuntimeError, OSError) as e:
            if not isinstance(e, ImportError):
                logger.warning("cudaHostRegister failed for %s: %s", region.name, e)

    def _cuda_unpin(self, region: MarufsMappedRegion) -> None:
        """Unregister mmap memory from CUDA."""
        if not region._cuda_pinned or region._mmap_obj is None:
            return
        try:
            import torch

            if torch.cuda.is_available():
                addr = _get_buffer_addr(region._mmap_obj)
                torch.cuda.cudart().cudaHostUnregister(addr)
                region._cuda_pinned = False
        except (ImportError, RuntimeError, OSError) as e:
            if not isinstance(e, ImportError):
                logger.warning("cudaHostUnregister failed for %s: %s", region.name, e)

    @staticmethod
    def _prefault_shared_region(
        mmap_obj: mmap_module.mmap, name: str, size: int
    ) -> str:
        """Pre-fault all pages in a shared (read-only) region.

        Uses MADV_POPULATE_READ to populate page table entries without
        write permission. Falls back to per-page read touch.

        Returns:
            The method used: "madvise" or "touch", or "" if skipped.
        """
        if not _PREFAULT_ENABLED:
            return ""
        t0 = time.monotonic()
        method = "madvise"
        try:
            mmap_obj.madvise(_MADV_POPULATE_READ)
        except OSError:
            method = "touch"
            for off in range(0, size, _PAGE_SIZE):
                _ = mmap_obj[off]
        elapsed_ms = (time.monotonic() - t0) * 1000
        logger.info(
            "Prefaulted shared region %s: %d MB in %.1fms (%s)",
            name,
            size >> 20,
            elapsed_ms,
            method,
        )
        return method

    @staticmethod
    def _prefault_owned_region(mmap_obj: mmap_module.mmap, name: str, size: int) -> str:
        """Pre-fault all pages in an owned (writable) region.

        Uses MADV_POPULATE_WRITE to populate page table entries with
        write permission. Falls back to per-page read touch.

        Returns:
            The method used: "madvise" or "touch", or "" if skipped.
        """
        if not _PREFAULT_ENABLED:
            return ""
        t0 = time.monotonic()
        method = "madvise"
        try:
            mmap_obj.madvise(_MADV_POPULATE_WRITE)
        except OSError:
            method = "touch"
            for off in range(0, size, _PAGE_SIZE):
                _ = mmap_obj[off]
        elapsed_ms = (time.monotonic() - t0) * 1000
        logger.info(
            "Prefaulted owned region %s: %d MB in %.1fms (%s)",
            name,
            size >> 20,
            elapsed_ms,
            method,
        )
        return method
