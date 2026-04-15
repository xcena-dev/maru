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
                    torch.cuda.cudart().cudaHostRegister(addr, handle.length, 0)
                    cuda_pin_ms = (time.monotonic() - t0) * 1000
                    logger.info(
                        "CUDA pinned region %d (%d bytes)",
                        region_id,
                        handle.length,
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
                if region._buffer_view is not None:
                    try:
                        import torch

                        if torch.cuda.is_available():
                            addr = ctypes.addressof(
                                ctypes.c_char.from_buffer(region._buffer_view)
                            )
                            torch.cuda.cudart().cudaHostUnregister(addr)
                    except (ImportError, RuntimeError, OSError) as e:
                        if not isinstance(e, ImportError):
                            logger.warning(
                                "cudaHostUnregister failed for region %d: %s",
                                region_id,
                                e,
                            )

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

    # =========================================================================
    # Query
    # =========================================================================

    def get_region(self, region_id: int) -> MappedRegion | None:
        """Get a mapped region by ID."""
        return self._regions.get(region_id)

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
                    if region._buffer_view is not None:
                        try:
                            import torch

                            if torch.cuda.is_available():
                                addr = ctypes.addressof(
                                    ctypes.c_char.from_buffer(region._buffer_view)
                                )
                                torch.cuda.cudart().cudaHostUnregister(addr)
                        except (ImportError, RuntimeError, OSError) as e:
                            if not isinstance(e, ImportError):
                                logger.warning(
                                    "cudaHostUnregister failed for region %d: %s",
                                    rid,
                                    e,
                                )

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
