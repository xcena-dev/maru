# SPDX-License-Identifier: Apache-2.0
"""Regions of a remote CXL storage session and the pages a worker allocates in them."""

from __future__ import annotations

import hashlib
import threading
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from maru_common.storage_types import StorageLocation, StorageUnavailableError

REMOTE_CXL = "remote_cxl"


def pick_mapper(mappers: list[str], region_id: int) -> str:
    """The region mapper that serves a region (rendezvous hash).

    Every Handler with the same list picks the same node for a region, so
    the owner and the readers of a region use one memory node's NIC.

    Args:
        mappers: Region mapper addresses of the pool.
        region_id: Region number.

    Returns:
        One address of ``mappers``.
    """
    return max(
        mappers,
        key=lambda address: hashlib.sha256(f"{address}|{region_id}".encode()).digest(),
    )


@dataclass(eq=False)
class RemoteCxlAllocation:
    """A remote page being written through one staging slot."""

    buf: memoryview
    location: StorageLocation
    slot: int
    region: RemoteCxlRegion
    state: str = "writing"

    @property
    def size(self) -> int:
        """Bytes the caller may write into ``buf``."""
        return self.location.length


class RemoteCxlRegion:
    """Pages of one region; the worker is their only allocator."""

    def __init__(self, pool_id: str, capacity: int, page_size: int, handle: dict):
        """Track a region the session got.

        Args:
            pool_id: The region's pool id at MaruServer.
            capacity: Region size in bytes, a multiple of ``page_size``.
            page_size: Bytes per page (one KV chunk).
            handle: ``MaruHandle.to_dict()`` of the region.

        Raises:
            ValueError: if the capacity is not a positive multiple of the page.
        """
        if capacity < page_size or page_size <= 0 or capacity % page_size:
            raise ValueError(
                "Remote CXL region capacity must be a positive multiple of page size"
            )
        self.pool_id = pool_id
        self.capacity = capacity
        self.page_size = page_size
        self.handle = dict(handle)
        self.region_id = int(handle["region_id"])
        self._free = deque(range(capacity // page_size))
        self._generation = 0
        self._writing: dict[str, RemoteCxlAllocation] = {}
        self._quarantined: set[int] = set()
        self._lock = threading.RLock()
        self.retired = False
        # False while its region mapper cannot map it (node down)
        self.reachable = True
        # Whether a region mapper ever answered a map of it: until then no
        # RDMA can have been sent to it, so it goes back without a release.
        self.mapped = False
        # Boot of the memory node at the last map
        self.boot = ""

    def has_free(self) -> bool:
        """Whether a page can be taken now."""
        with self._lock:
            return not self.retired and self.reachable and bool(self._free)

    def take_page(self, size: int) -> StorageLocation:
        """Take a free page for ``size`` bytes.

        Raises:
            StorageUnavailableError: if the region left with a closed session.
            ValueError: if ``size`` does not fit in a page.
            MemoryError: if no page is free.
        """
        with self._lock:
            if self.retired:
                raise StorageUnavailableError(
                    "Remote CXL region belongs to a closed session"
                )
            if type(size) is not int or not 0 < size <= self.page_size:
                raise ValueError("Remote CXL allocation must fit in one page")
            if not self._free:
                raise MemoryError("Remote CXL region is full")
            page = self._free.popleft()
            self._generation += 1
            return StorageLocation(
                self.pool_id,
                uuid.uuid4().hex,
                self._generation,
                page * self.page_size,
                size,
                REMOTE_CXL,
            )

    def track(self, handle: RemoteCxlAllocation) -> None:
        """Remember an allocation being written."""
        with self._lock:
            self._writing[handle.location.allocation_id] = handle

    def validate(self, handle: RemoteCxlAllocation) -> None:
        """Check that an allocation is one of this region's writes.

        Raises:
            ValueError: if it is not.
        """
        if (
            not isinstance(handle, RemoteCxlAllocation)
            or self._writing.get(handle.location.allocation_id) is not handle
        ):
            raise ValueError("Allocation does not belong to this remote CXL region")

    def untrack(self, handle: RemoteCxlAllocation) -> None:
        """Forget an allocation that was committed, freed or isolated."""
        with self._lock:
            self._writing.pop(handle.location.allocation_id, None)

    def give_page(self, location: StorageLocation) -> None:
        """Return a page whose key was never committed or was removed."""
        with self._lock:
            if location.pool_id != self.pool_id or self.retired:
                return
            self._free.append(location.offset // self.page_size)

    def quarantine(self, location: StorageLocation) -> None:
        """Count a page a late transfer may still touch."""
        with self._lock:
            self._quarantined.add(location.offset)

    def unquarantine(self, location: StorageLocation) -> None:
        """Stop counting a page whose transfer ended."""
        with self._lock:
            self._quarantined.discard(location.offset)

    def usage(self) -> dict:
        """Allocated and quarantined bytes, for the heartbeat report."""
        with self._lock:
            used = self.capacity // self.page_size - len(self._free)
            return {
                "allocated_bytes": used * self.page_size,
                "quarantined_bytes": len(self._quarantined) * self.page_size,
            }

    def retire(self) -> None:
        """Take no more pages from the region; it is going back."""
        with self._lock:
            self.retired = True
            self._free.clear()


@dataclass(frozen=True)
class RegionAccess:
    """Where a region is reachable: the NIXL peer of its region mapper, the
    region mapper's address of the region (VA_R), which connection mapped it
    and the boot of the memory node then."""

    peer: str
    base_addr: int
    length: int
    mapper: str
    generation: int
    boot: str = ""


@dataclass
class IsolatedTransfer:
    """A transfer whose end is not confirmed and what it may still touch."""

    pending: Any  # maru_remote.transport.PendingTransfer
    op: str
    peer: str
    region: RemoteCxlRegion | None
    locations: list[StorageLocation] = field(default_factory=list)
    slots: list[int] = field(default_factory=list)
