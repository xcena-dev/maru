# SPDX-License-Identifier: Apache-2.0
"""A bounded, process-local CPU pool and explicit read-buffer ownership.

With ``pin`` the pool is faulted in and registered with CUDA when it opens,
as DaxMapper does for CXL regions: GPU copies then run at the DMA rate
instead of being staged through the driver's own pinned buffers, and a store
no longer faults in fresh pages. Registration is best effort. A pool that
fails it stays pageable, and copies check each slab's pin status. The pool
never hands a host pointer to the direct-access CUDA kernels. Eviction is a
later capability, not an assumption of this allocator.
"""

import ctypes
import logging
import mmap
import time
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from threading import RLock

from maru_common.storage_types import CpuLocation

logger = logging.getLogger(__name__)


@dataclass(eq=False)
class CpuAllocation:
    buf: memoryview
    location: CpuLocation
    state: str = "writing"

    @property
    def size(self) -> int:
        return self.location.length


@dataclass(eq=False)
class CpuReadLease:
    """Keep the pool alive until the caller's final read/copy completes.

    Call release() (or use a context manager). GC is not the synchronization
    mechanism. The connector releases these after its synchronous H2D copies.
    """

    view: memoryview
    location: CpuLocation
    _release: Callable[[], None]
    _released: bool = False

    def release(self) -> None:
        if not self._released:
            self._released = True
            self.view.release()
            self._release()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.release()


class CpuPool:
    def __init__(
        self,
        pool_id: str,
        capacity: int,
        page_size: int,
        *,
        medium: str = "cpu",
        mapping: mmap.mmap | None = None,
        pin: bool = False,
    ):
        if capacity < page_size or page_size <= 0 or capacity % page_size:
            raise ValueError(
                "CPU pool capacity must be a positive multiple of page size"
            )
        self.pool_id = pool_id
        self.capacity = capacity
        self.page_size = page_size
        self.medium = medium
        if mapping is not None and len(mapping) < capacity:
            raise ValueError("Mapped region is smaller than the pool capacity")
        self._mapping = mapping if mapping is not None else mmap.mmap(-1, capacity)
        self._free = deque(range(capacity // page_size))
        self._generation = 0
        self._allocations: dict[str, CpuAllocation] = {}
        self._lock = RLock()
        self._closed = False
        # Whether the pool is registered with CUDA (see page_lock)
        self.cuda_registered = False
        self._cuda_address = 0
        if pin:
            self.page_lock()

    def page_lock(self) -> None:
        """Fault the pool in and register it with CUDA, best effort.

        Prefaulting writes every page once, so that neither the registration
        nor a later store faults them in one at a time; ``MARU_PREFAULT=0``
        skips it, as for CXL regions. On failure the pool stays pageable.
        A pool that is already registered is left as it is.
        """
        if self.cuda_registered:
            return
        if self._closed:
            raise RuntimeError("CPU pool is closed")
        from maru_handler.memory.mapper import (
            _MADV_POPULATE_WRITE,
            _PAGE_SIZE,
            _PREFAULT_ENABLED,
            _clear_cuda_sticky_error,
            _cuda_rc,
        )

        name = f"{self.medium.upper()} pool {self.pool_id}"
        t0 = time.monotonic()
        if _PREFAULT_ENABLED:
            try:
                self._mapping.madvise(_MADV_POPULATE_WRITE)
            except OSError:
                # Kernels before 5.14. Writing each page's first byte back
                # faults it in writable without changing what it holds; a
                # read would map the shared zero page of an anonymous pool.
                for offset in range(0, self.capacity, _PAGE_SIZE):
                    self._mapping[offset] = self._mapping[offset]
        t1 = time.monotonic()
        try:
            import torch

            if not torch.cuda.is_available():
                return
            address = ctypes.addressof(ctypes.c_char.from_buffer(self._mapping))
            rc = _cuda_rc(
                torch.cuda.cudart().cudaHostRegister(address, self.capacity, 0)
            )
        except (ImportError, RuntimeError, OSError) as exc:
            if not isinstance(exc, ImportError):
                logger.warning(
                    "cudaHostRegister failed for the %s: %s; it stays pageable",
                    name,
                    exc,
                )
            return
        if rc != 0:
            # As for CXL regions (see DaxMapper.map_region): clear the
            # per-thread sticky error so that a later, unrelated CUDA call
            # does not fail with it.
            _clear_cuda_sticky_error()
            logger.warning(
                "cudaHostRegister failed for the %s (%d bytes): rc=%d; it "
                "stays pageable, and GPU copies take the pageable path",
                name,
                self.capacity,
                rc,
            )
            return
        self._cuda_address = address
        self.cuda_registered = True
        logger.info(
            "Page-locked the %s: %d MiB (prefault %.1f s, CUDA registration %.1f s)",
            name,
            self.capacity >> 20,
            t1 - t0,
            time.monotonic() - t1,
        )

    def _page_unlock(self) -> None:
        """Unregister the pool from CUDA, before its mapping is closed."""
        if not self.cuda_registered:
            return
        self.cuda_registered = False
        try:
            import torch

            from maru_handler.memory.mapper import _clear_cuda_sticky_error, _cuda_rc

            rc = _cuda_rc(torch.cuda.cudart().cudaHostUnregister(self._cuda_address))
            if rc != 0:
                _clear_cuda_sticky_error()
                logger.warning(
                    "cudaHostUnregister failed for the %s pool %s: rc=%d",
                    self.medium.upper(),
                    self.pool_id,
                    rc,
                )
        except (ImportError, RuntimeError, OSError) as exc:
            logger.warning(
                "cudaHostUnregister failed for the %s pool %s: %s",
                self.medium.upper(),
                self.pool_id,
                exc,
            )

    def alloc(self, size: int) -> CpuAllocation:
        with self._lock:
            if self._closed:
                raise RuntimeError("CPU pool is closed")
            if type(size) is not int or not 0 < size <= self.page_size:
                raise ValueError("CPU allocation must fit in one page")
            if not self._free:
                raise MemoryError(
                    f"{self.medium.upper()} cache is full; skipping new cache admission"
                )
            page = self._free.popleft()
            self._generation += 1
            offset = page * self.page_size
            location = CpuLocation(
                self.pool_id,
                uuid.uuid4().hex,
                self._generation,
                offset,
                size,
                self.medium,
            )
            handle = CpuAllocation(
                memoryview(self._mapping)[offset : offset + size], location
            )
            self._allocations[location.allocation_id] = handle
            return handle

    def validate(self, handle: CpuAllocation) -> None:
        if (
            not isinstance(handle, CpuAllocation)
            or self._allocations.get(handle.location.allocation_id) is not handle
        ):
            raise ValueError("Allocation does not belong to this CPU pool")

    def free(self, handle: CpuAllocation) -> None:
        with self._lock:
            self.validate(handle)
            if handle.state != "writing":
                # Ownership was transferred to commit. In particular a lost
                # reply must never cause the connector's cleanup to reuse it.
                return
            handle.buf.release()
            handle.state = "freed"
            del self._allocations[handle.location.allocation_id]
            self._free.append(handle.location.offset // self.page_size)

    def resolve(self, location: CpuLocation) -> memoryview:
        with self._lock:
            handle = self._allocations.get(location.allocation_id)
            if self._closed or handle is None or handle.location != location:
                raise ValueError("Stale or foreign CPU location")
            if handle.state != "ready":
                raise ValueError("CPU allocation is not ready")
            return handle.buf[:]

    def usage(self) -> dict:
        with self._lock:
            return {
                "allocated_bytes": len(self._allocations) * self.page_size,
                "quarantined_bytes": sum(
                    h.state == "quarantined" for h in self._allocations.values()
                )
                * self.page_size,
            }

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._closed = True
                for handle in self._allocations.values():
                    handle.buf.release()
                    handle.state = "closed"
                self._allocations.clear()
                self._free.clear()
            self._page_unlock()
            try:
                self._mapping.close()
            except BufferError:
                # Caller-held views/tensors keep the retired mapping alive.
                # Never forcibly unmap or return those bytes to another pool.
                raise RuntimeError(
                    f"Release exported {self.medium.upper()} buffers before closing the pool"
                ) from None
