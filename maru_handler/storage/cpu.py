# SPDX-License-Identifier: Apache-2.0
"""A bounded, process-local CPU pool and explicit read-buffer ownership.

M1 uses pageable memory and synchronous GPU copies. It never hands a pageable
host pointer to the direct-access CUDA kernels. Pinning/async transfer and
eviction are later capabilities, not assumptions of this allocator.
"""

import mmap
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from threading import RLock

from maru_common.storage_types import CpuLocation


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
            try:
                self._mapping.close()
            except BufferError:
                # Caller-held views/tensors keep the retired mapping alive.
                # Never forcibly unmap or return those bytes to another pool.
                raise RuntimeError(
                    f"Release exported {self.medium.upper()} buffers before closing the pool"
                ) from None
