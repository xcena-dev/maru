# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""DaxMapper - Memory mapping via MaruShmClient.

Single owner of MaruShmClient for mmap/munmap operations.
Does NOT perform read/write — Handler handles data access directly.
"""

import ctypes
import logging
import mmap as mmap_module
import os
import threading
import time

from maru_shm import PROT_READ, PROT_WRITE, MaruHandle, MaruShmClient

from .types import MappedRegion

logger = logging.getLogger(__name__)

# Linux MADV_POPULATE_WRITE (kernel 5.14+): pre-fault pages with write permission
_MADV_POPULATE_WRITE = getattr(mmap_module, "MADV_POPULATE_WRITE", 23)
_PREFAULT_ENABLED = os.environ.get("MARU_PREFAULT", "1") != "0"
_PAGE_SIZE = os.sysconf("SC_PAGESIZE") if hasattr(os, "sysconf") else 4096


def _cuda_rc(ret) -> int:
    """Normalize torch cudart's return value (enum, int, or 1-tuple) to int."""
    return int(ret[0]) if isinstance(ret, tuple) else int(ret)


def _find_libcudart_paths() -> list[str]:
    """Return every distinct libcudart mapped into this process.

    Matches on the basename so torch wheels that bundle a hashed copy
    (``libcudart-<hash>.so.X``) are found too, and strips the
    ``" (deleted)"`` suffix /proc/self/maps appends to unlinked files.
    """
    paths: list[str] = []
    with open("/proc/self/maps") as f:
        for line in f:
            fields = line.rstrip("\n").split(maxsplit=5)
            if len(fields) < 6:
                continue
            path = fields[5]
            if path.endswith(" (deleted)"):
                path = path[: -len(" (deleted)")]
            if "libcudart" in os.path.basename(path) and path not in paths:
                paths.append(path)
    return paths


def _clear_cuda_sticky_error() -> None:
    """Consume CUDA's per-thread sticky error after a failed cudaHostRegister.

    torch's cudart binding does not expose cudaGetLastError, so locate the
    libcudart instance(s) loaded in this process (dlopen of the same path
    returns the already-loaded instance) and consume the error there.  The
    error state is per instance, so every mapped copy is cleared.  Without
    this, the next error-checked CUDA call on this thread — typically an
    unrelated kernel launch — raises a misattributed
    "CUDA error: out of memory".
    """
    cleared = False
    try:
        for path in _find_libcudart_paths():
            try:
                lib = ctypes.CDLL(path)
                lib.cudaGetLastError.restype = ctypes.c_int
                lib.cudaGetLastError()
                cleared = True
            except (OSError, AttributeError):
                logger.debug(
                    "could not clear CUDA sticky error via %s", path, exc_info=True
                )
    except OSError:
        pass
    if not cleared:
        logger.warning(
            "could not clear CUDA sticky error (no loadable libcudart in "
            "/proc/self/maps) — the next CUDA call on this thread may raise "
            "a misattributed error"
        )


class DaxMapper:
    """Maps shared memory regions via MaruShmClient.

    Single owner of MaruShmClient — all mmap/munmap goes through here.
    Does NOT perform read/write — Handler accesses mapped memory directly.
    All regions are mapped with PROT_READ | PROT_WRITE.

    Example:
        mapper = DaxMapper()
        region = mapper.map_region(handle)
        mapper.unmap_region(handle.region_id)
    """

    def __init__(
        self,
        rm_address: str | None = None,
        device_table: dict[str, str] | None = None,
    ):
        self._client = MaruShmClient(address=rm_address, device_table=device_table)
        self._lock = threading.Lock()
        self._regions: dict[int, MappedRegion] = {}

    # =========================================================================
    # Map / Unmap
    # =========================================================================

    def map_region(
        self,
        handle: MaruHandle,
        prefault: bool = True,
    ) -> MappedRegion:
        """Map a region into memory via MaruShmClient.

        If already mapped, returns the existing MappedRegion.
        Thread-safe: protected by internal lock (idempotent).
        Always maps with PROT_READ | PROT_WRITE and registers with
        cudaHostRegister for GPU DMA.

        Args:
            handle: MaruHandle from server allocation or lookup
            prefault: If True, pre-fault pages after mapping to avoid
                page faults on first access. Set to False for shared
                regions to avoid NUMA placement pollution.

        Returns:
            MappedRegion with mmap object

        Raises:
            RuntimeError: If mmap fails
        """
        t_total = time.monotonic()
        mmap_ms = 0.0
        prefault_ms = 0.0
        cuda_pin_ms = 0.0

        with self._lock:
            region_id = handle.region_id

            existing = self._regions.get(region_id)
            if existing is not None and existing.is_mapped:
                return existing

            prot = PROT_READ | PROT_WRITE
            try:
                t0 = time.monotonic()
                result = self._client.mmap(handle, prot)
                mmap_ms = (time.monotonic() - t0) * 1000
            except Exception as e:
                raise RuntimeError(f"Failed to map region {region_id}: {e}") from e

            region = MappedRegion(
                region_id=region_id,
                handle=handle,
                size=handle.length,
                _mmap_obj=result,
            )
            self._regions[region_id] = region

            logger.debug(
                "Mapped region %d: length=%d",
                region_id,
                handle.length,
            )

        # Outside lock: prefault is idempotent (mmap already completed)
        if prefault and _PREFAULT_ENABLED and result is not None:
            t0 = time.monotonic()
            self._prefault_region(result, region_id, handle.length)
            prefault_ms = (time.monotonic() - t0) * 1000

        # Outside lock: CUDA pin is idempotent
        if region._buffer_view is not None:
            try:
                import torch

                if torch.cuda.is_available():
                    addr = ctypes.addressof(
                        ctypes.c_char.from_buffer(region._buffer_view)
                    )
                    t0 = time.monotonic()
                    rc = _cuda_rc(
                        torch.cuda.cudart().cudaHostRegister(addr, handle.length, 0)
                    )
                    cuda_pin_ms = (time.monotonic() - t0) * 1000
                    if rc == 0:
                        region._cuda_pinned = True
                        logger.info(
                            "CUDA pinned region %d (%d bytes)",
                            region_id,
                            handle.length,
                        )
                    else:
                        # NVIDIA r580+ drivers reject a single registration
                        # of >= 512 GiB (per-registration page table hits
                        # the kernel's kvmalloc INT_MAX cap: "NVRM: failed
                        # to allocate page table"). Clear the per-thread
                        # sticky error so an unrelated later CUDA call does
                        # not crash with a misattributed OOM.
                        _clear_cuda_sticky_error()
                        logger.warning(
                            "cudaHostRegister failed for region %d "
                            "(%d bytes): rc=%d — region stays unpinned, "
                            "GPU transfers fall back to pageable copies",
                            region_id,
                            handle.length,
                            rc,
                        )
            except (ImportError, RuntimeError, OSError) as e:
                if not isinstance(e, ImportError):
                    logger.warning(
                        "cudaHostRegister failed for region %d: %s",
                        region_id,
                        e,
                    )

        total_ms = (time.monotonic() - t_total) * 1000
        logger.info(
            "map_region %d: %d MB total=%.1fms "
            "(mmap=%.1fms, prefault=%.1fms, cuda_pin=%.1fms)",
            region_id,
            handle.length >> 20,
            total_ms,
            mmap_ms,
            prefault_ms,
            cuda_pin_ms,
        )

        return region

    @staticmethod
    def _prefault_region(mmap_obj: mmap_module.mmap, region_id: int, size: int) -> None:
        """Pre-fault all pages in a mapped region.

        Eliminates page fault latency on first data access by populating
        page table entries upfront. Especially important for DAX/CXL
        device memory where first-touch determines NUMA placement.

        Strategy:
            1. madvise(MADV_POPULATE_WRITE) — kernel-space, O(1) syscall (Linux 5.14+)
            2. Fallback: per-page read touch from Python
        """
        t0 = time.monotonic()
        method = "madvise"
        try:
            mmap_obj.madvise(_MADV_POPULATE_WRITE)
        except OSError:
            # Kernel too old or not supported for this mapping type;
            # fall back to manual per-page read touch.
            method = "touch"
            for off in range(0, size, _PAGE_SIZE):
                _ = mmap_obj[off]  # read to fault page in
        elapsed_ms = (time.monotonic() - t0) * 1000
        logger.info(
            "Prefaulted region %d: %d MB in %.1f ms (%s)",
            region_id,
            size >> 20,
            elapsed_ms,
            method,
        )

    def unmap_region(self, region_id: int) -> bool:
        """Unmap a region.

        Thread-safe: protected by internal lock.

        Args:
            region_id: Region to unmap

        Returns:
            True if successfully unmapped
        """
        with self._lock:
            region = self._regions.pop(region_id, None)
            if region is None:
                logger.warning("Region %d not found for unmapping", region_id)
                return False

            try:
                # CUDA unpin before munmap (order matters — needs buffer_view for addr)
                self._cuda_unpin_region(region)

                if region.is_mapped:
                    region.release()
                    try:
                        self._client.munmap(region.handle)
                    except BufferError:
                        logger.debug(
                            "Region %d munmap deferred to GC "
                            "(exported pointers still held)",
                            region_id,
                        )
                    else:
                        logger.debug("Unmapped region %d", region_id)
                return True
            except Exception:
                logger.error("Error unmapping region %d", region_id, exc_info=True)
                return False

    @staticmethod
    def _cuda_unpin_region(region: MappedRegion) -> None:
        """cudaHostUnregister a region that actually pinned (rc-checked)."""
        if not (region._cuda_pinned and region._buffer_view is not None):
            return
        try:
            import torch

            if torch.cuda.is_available():
                addr = ctypes.addressof(ctypes.c_char.from_buffer(region._buffer_view))
                rc = _cuda_rc(torch.cuda.cudart().cudaHostUnregister(addr))
                if rc != 0:
                    _clear_cuda_sticky_error()
                    logger.warning(
                        "cudaHostUnregister failed for region %d: rc=%d",
                        region.region_id,
                        rc,
                    )
                region._cuda_pinned = False
        except (ImportError, RuntimeError, OSError) as e:
            if not isinstance(e, ImportError):
                logger.warning(
                    "cudaHostUnregister failed for region %d: %s",
                    region.region_id,
                    e,
                )

    # =========================================================================
    # Query
    # =========================================================================

    def get_region(self, region_id: int) -> MappedRegion | None:
        """Get a mapped region by ID."""
        return self._regions.get(region_id)

    def get_dax_path(self, region_id: int) -> str | None:
        """Return the DAX device path for a mapped region, or None."""
        return self._client.get_dax_path(region_id)

    def get_buffer_view(
        self, region_id: int, offset: int, size: int
    ) -> memoryview | None:
        """Get a memoryview slice from a mapped region.

        Convenience method combining get_region() + get_buffer_view().
        Returns None if the region is not mapped or buffer unavailable.
        """
        region = self._regions.get(region_id)
        if region is None:
            return None
        return region.get_buffer_view(offset, size)

    def close(self) -> None:
        """Unmap all regions (owned + shared).

        Thread-safe: protected by internal lock.
        """
        with self._lock:
            for rid in list(self._regions):
                region = self._regions.pop(rid, None)
                if region is None:
                    continue
                try:
                    # CUDA unpin before munmap (order matters!)
                    self._cuda_unpin_region(region)

                    if region.is_mapped:
                        region.release()
                        try:
                            self._client.munmap(region.handle)
                        except BufferError:
                            logger.debug(
                                "Region %d munmap deferred to GC "
                                "(exported pointers still held)",
                                rid,
                            )
                        else:
                            logger.debug("Unmapped region %d", rid)
                except Exception:
                    logger.error(
                        "Failed to unmap region %d during close", rid, exc_info=True
                    )
            self._regions.clear()
