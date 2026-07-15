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

# CUDA pinning strategy.
#
# A single cudaHostRegister call fails with cudaErrorMemoryAllocation for
# ranges >= 2^39 bytes (512 GiB) — the NVIDIA kernel driver cannot build a
# page table for one mapping that large ("NVRM: failed to allocate page
# table").  The limit is per call, not cumulative, so large regions are
# registered in chunks.
#
#   MARU_PIN_MODE:     "eager" (default) — register all chunks synchronously
#                      inside map_region(); the region is fully pinned when
#                      map_region() returns.
#                      "lazy" — return immediately and register chunks in a
#                      background thread (LMCache LazyMemoryAllocator style).
#                      Chunks not yet pinned degrade GPU DMA to pageable
#                      copies but remain fully usable.
#   MARU_PIN_CHUNK_GB: chunk size in GiB (default 128, clamped to 256 so a
#                      chunk can never reach the 512 GiB wall; 0 = single
#                      call, i.e. the pre-chunking behavior).
_PIN_MODE = os.environ.get("MARU_PIN_MODE", "eager")
if _PIN_MODE not in ("eager", "lazy"):
    _PIN_MODE = "eager"
try:
    _PIN_CHUNK_BYTES = int(os.environ.get("MARU_PIN_CHUNK_GB", "128")) << 30
except ValueError:
    _PIN_CHUNK_BYTES = 128 << 30
if _PIN_CHUNK_BYTES > 256 << 30:
    _PIN_CHUNK_BYTES = 256 << 30


def _clear_cuda_sticky_error() -> None:
    """Consume CUDA's per-thread sticky error after a failed cudaHostRegister.

    torch's cudart binding does not expose cudaGetLastError, so locate the
    libcudart torch itself loaded (dlopen of the same path returns the
    already-loaded instance) and consume the error there.  Without this, the
    next error-checked CUDA call on this thread — typically an unrelated
    kernel launch — raises a misattributed "CUDA error: out of memory".
    """
    try:
        path = None
        with open("/proc/self/maps") as f:
            for line in f:
                if "libcudart.so" in line:
                    path = line.rstrip("\n").split(maxsplit=5)[-1]
                    break
        if path:
            lib = ctypes.CDLL(path)
            lib.cudaGetLastError.restype = ctypes.c_int
            lib.cudaGetLastError()
    except (OSError, IndexError, AttributeError):
        logger.debug("could not clear CUDA sticky error", exc_info=True)


def _cuda_pin_chunks(
    cudart,
    base_addr: int,
    length: int,
    region_id: int,
    records: list[tuple[int, int]],
    stop_event: threading.Event | None = None,
) -> None:
    """Register [base_addr, base_addr+length) with CUDA in chunks.

    Appends an (addr, size) record to ``records`` for every successfully
    registered chunk (records are what cudaHostUnregister must be called
    with later).  A failed chunk is logged and its sticky CUDA error
    cleared; pinning continues so the remaining chunks still get DMA.
    """
    chunk = _PIN_CHUNK_BYTES if _PIN_CHUNK_BYTES > 0 else length
    off = 0
    while off < length:
        if stop_event is not None and stop_event.is_set():
            return
        n = min(chunk, length - off)
        rc = cudart.cudaHostRegister(base_addr + off, n, 0)
        rc = int(rc[0]) if isinstance(rc, tuple) else int(rc)
        if rc == 0:
            records.append((base_addr + off, n))
        else:
            _clear_cuda_sticky_error()
            logger.warning(
                "cudaHostRegister failed for region %d chunk [+%d, +%d): "
                "rc=%d; chunk stays unpinned (GPU DMA degraded)",
                region_id,
                off,
                off + n,
                rc,
            )
        off += n


def _cuda_unpin_records(
    cudart, records: list[tuple[int, int]], region_id: int
) -> None:
    """Unregister every previously pinned chunk of a region."""
    for addr, _size in records:
        try:
            rc = cudart.cudaHostUnregister(addr)
            rc = int(rc[0]) if isinstance(rc, tuple) else int(rc)
            if rc != 0:
                _clear_cuda_sticky_error()
                logger.warning(
                    "cudaHostUnregister failed for region %d addr=%#x: rc=%d",
                    region_id,
                    addr,
                    rc,
                )
        except (RuntimeError, OSError) as e:
            logger.warning(
                "cudaHostUnregister failed for region %d: %s", region_id, e
            )
    records.clear()


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

            # Base address for CUDA pinning — resolved while the lock still
            # guarantees the buffer view is alive (a concurrent unmap after
            # publication would otherwise race the from_buffer export).
            pin_addr = (
                ctypes.addressof(ctypes.c_char.from_buffer(region._buffer_view))
                if region._buffer_view is not None
                else None
            )

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
        if pin_addr is not None:
            try:
                import torch

                if torch.cuda.is_available():
                    cudart = torch.cuda.cudart()
                    if _PIN_MODE == "lazy":
                        thread = threading.Thread(
                            target=self._pin_worker,
                            args=(cudart, region, pin_addr, handle.length),
                            daemon=True,
                            name=f"maru-pin-{region_id}",
                        )
                        thread.start()
                        # Published only after a successful start() so unmap
                        # never joins a never-started thread.
                        region._pin_thread = thread
                        logger.info(
                            "CUDA pinning region %d in background (%d bytes)",
                            region_id,
                            handle.length,
                        )
                    else:
                        t0 = time.monotonic()
                        with region._pin_lock:
                            _cuda_pin_chunks(
                                cudart, pin_addr, handle.length, region_id,
                                region._pin_records,
                                stop_event=region._pin_stop,
                            )
                            pinned = sum(n for _, n in region._pin_records)
                            n_chunks = len(region._pin_records)
                        cuda_pin_ms = (time.monotonic() - t0) * 1000
                        if pinned == handle.length:
                            logger.info(
                                "CUDA pinned region %d (%d bytes, %d chunks)",
                                region_id,
                                pinned,
                                n_chunks,
                            )
                        else:
                            logger.warning(
                                "CUDA pinned region %d PARTIALLY: %d/%d bytes"
                                " — unpinned ranges fall back to pageable"
                                " copies",
                                region_id,
                                pinned,
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
    def _cuda_unpin_region(region) -> None:
        """Stop/join any in-flight pinning, then unregister pinned chunks.

        Setting _pin_stop makes a concurrent pin loop (eager in another
        thread, or the lazy worker) exit at its next chunk boundary, so
        acquiring _pin_lock below waits for at most one chunk.
        """
        region._pin_stop.set()
        if region._pin_thread is not None:
            region._pin_thread.join()
            region._pin_thread = None
        with region._pin_lock:
            if not region._pin_records:
                return
            try:
                import torch

                if torch.cuda.is_available():
                    _cuda_unpin_records(
                        torch.cuda.cudart(),
                        region._pin_records,
                        region.region_id,
                    )
            except (ImportError, RuntimeError, OSError) as e:
                if not isinstance(e, ImportError):
                    logger.warning(
                        "cudaHostUnregister failed for region %d: %s",
                        region.region_id,
                        e,
                    )

    def _pin_worker(
        self, cudart, region, base_addr: int, length: int
    ) -> None:
        """Background chunk-pinning worker (MARU_PIN_MODE=lazy).

        Appends to region._pin_records as chunks are registered so that
        unmap_region()/close() — which join this thread first — always
        unregister exactly what was pinned.
        """
        t0 = time.monotonic()
        try:
            with region._pin_lock:
                _cuda_pin_chunks(
                    cudart,
                    base_addr,
                    length,
                    region.region_id,
                    region._pin_records,
                    stop_event=region._pin_stop,
                )
                pinned = sum(n for _, n in region._pin_records)
        except (RuntimeError, OSError) as e:
            logger.warning(
                "cudaHostRegister failed for region %d: %s",
                region.region_id,
                e,
            )
            return
        if region._pin_stop.is_set() and pinned < length:
            logger.info(
                "CUDA background pinning region %d cancelled at %d/%d bytes",
                region.region_id,
                pinned,
                length,
            )
        elif pinned < length:
            logger.warning(
                "CUDA background pinning region %d PARTIAL: %d/%d bytes — "
                "unpinned ranges fall back to pageable copies",
                region.region_id,
                pinned,
                length,
            )
        else:
            logger.info(
                "CUDA background pinning region %d finished: %d/%d bytes "
                "in %.1f ms",
                region.region_id,
                pinned,
                length,
                (time.monotonic() - t0) * 1000,
            )

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
                # CUDA unpin before munmap (join lazy pin thread first so
                # _pin_records is final, then unregister each pinned chunk)
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
