# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""PoolMaps: node pool mappings and their CUDA registration.

Every location is checked against the mapped pools before a byte is touched
(#59). A mapping taken out of use is closed (CUDA unregister, unmap, then its
RM hold dropped) on a closer thread, never on the thread that let it go.
"""

import collections
import logging
import mmap
import os
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from maru_common.node_protocol import process_identity
from maru_common.storage_types import (
    DaxMapping,
    Location,
    PoolDescriptor,
    PoolInfo,
    PoolKind,
    run_holder,
)
from maru_handler.storage.cpu import CpuPool
from maru_shm.client import MaruShmClient
from maru_shm.unholds import Unholds

logger = logging.getLogger(__name__)

# Pools a GPU copy may still write: mapped and registered until the process ends
_KEPT_UNTIL_EXIT: list["MappedPool"] = []


class LocationError(ValueError):
    """A location that does not fall inside a mapped pool (#59)."""


class StaleRunError(LocationError):
    """The pool is mapped for another node run than the one asked for."""


class PoolMapping(Protocol):
    """A pool mapped into this process: views of its bytes, CUDA registration,
    and the unmap once no view is held."""

    @property
    def cuda_registered(self) -> bool: ...

    def view(self, offset: int, length: int) -> memoryview: ...

    def page_lock(self) -> None:
        """Register with CUDA, best effort."""
        ...

    def close(self) -> None:
        """Unregister and unmap; RuntimeError while views are held."""
        ...


def map_dram_memfd(info: PoolInfo, fd: int) -> PoolMapping:
    """A DRAM pool's memfd mapped shared; the fd stays the caller's. Page-locked
    with the cpu backend's code (#128)."""
    size = os.fstat(fd).st_size
    if size < info.capacity:
        raise LocationError(
            f"pool {info.pool_id}: the memfd has {size} bytes, the pool {info.capacity}"
        )
    mapping = mmap.mmap(
        fd,
        info.capacity,
        flags=mmap.MAP_SHARED,
        prot=mmap.PROT_READ | mmap.PROT_WRITE,
    )
    return CpuPool(
        info.pool_id,
        info.capacity,
        info.page_size,
        medium="dram",
        mapping=mapping,
        shared=True,
    )


# One RM client per address for the process: it only checks handles
_rm_clients: dict[str, MaruShmClient] = {}
_rm_clients_lock = threading.Lock()


def _rm_client(address: str) -> MaruShmClient:
    with _rm_clients_lock:
        client = _rm_clients.get(address)
        if client is None:
            client = _rm_clients[address] = MaruShmClient(address=address or None)
        return client


@dataclass(frozen=True)
class RegionHolder:
    """Who holds a CXL region in the RM: a worker incarnation and the process
    the RM checks to reap the hold once it ends."""

    holder: str
    boot_id: str
    pid: int
    pid_start: int
    pid_ns: int

    @classmethod
    def this_process(cls, holder: str) -> "RegionHolder":
        return cls(holder, *process_identity())


class RegionHolds:
    """One RM hold per region and node run for the process while any mapping
    of it lives: a region is mapped only after its hold is durable, and the
    hold is dropped only once every mapping of it in that run is unregistered
    and unmapped. The holder name carries the run, so an old run's mapping
    and a new run's are separate holds. Drops go out on a thread of their
    own (never on the thread that released the last view), each sent again
    until the RM confirms it."""

    def __init__(self, unholds: Unholds | None = None) -> None:
        self._lock = threading.Lock()
        # (RM id, region, holder name) as the RM keys holds: an RM written as
        # another address is the same RM, and two clients of one process hold
        # apart
        self._count: dict[tuple[str, int, str], int] = {}
        self.unholds = unholds or Unholds()

    def acquire(self, dax: DaxMapping, who: RegionHolder):
        """The region's device access, holding it; RuntimeError (stale run,
        bad token) if the RM refuses."""
        name = run_holder(who.holder, dax.node_epoch)
        key = (dax.rm_domain, dax.handle.region_id, name)
        client = _rm_client(dax.rm_address)
        with self._lock:
            if self._count.get(key):
                access = client.get_access(dax.handle)
                self._count[key] += 1
                return access
            # A drop still queued is not needed any more (NODE_HOLD of the
            # same holder again is harmless); queued again if the hold fails
            taken = self.unholds.take_back(key)
            try:
                access = client.node_hold(
                    dax.node_id,
                    dax.node_epoch,
                    dax.handle,
                    name,
                    boot_id=who.boot_id,
                    pid=who.pid,
                    pid_start=who.pid_start,
                    pid_ns=who.pid_ns,
                )
            except BaseException:
                if taken is not None:
                    self.unholds.put(key, *taken)
                raise
            self._count[key] = 1
            return access

    def release(self, dax: DaxMapping, who: RegionHolder) -> None:
        """Queue the drop once no mapping of the hold is left."""
        name = run_holder(who.holder, dax.node_epoch)
        key = (dax.rm_domain, dax.handle.region_id, name)
        with self._lock:
            count = self._count.get(key, 0) - 1
            if count > 0:
                self._count[key] = count
                return
            self._count.pop(key, None)
            handle, address = dax.handle, dax.rm_address
            self.unholds.put(
                key,
                lambda: _rm_client(address).unhold(handle, name, name),
                f"the hold {name} on region {handle.region_id}",
            )


_HOLDS = RegionHolds()


class CxlRegionMapping:
    """A CXL region mapped while this process holds it in the RM (NODE_HOLD,
    which also checks the handle's token). Page-locked like a DRAM pool."""

    def __init__(
        self,
        info: PoolInfo,
        dax: DaxMapping,
        who: RegionHolder | None,
        holds: RegionHolds | None = None,
    ):
        handle = dax.handle
        if who is None:
            raise LocationError(f"pool {info.pool_id}: no holder to hold it in the RM")
        if handle.length < info.capacity:
            raise LocationError(
                f"pool {info.pool_id}: the region has {handle.length} bytes, "
                f"the pool {info.capacity}"
            )
        holds = holds or _HOLDS
        self._dax, self._who, self._holds = dax, who, holds
        access = holds.acquire(dax, who)
        try:
            fd = os.open(access.dax_path, os.O_RDWR)
            try:
                mapping = mmap.mmap(
                    fd,
                    handle.length,
                    flags=mmap.MAP_SHARED,
                    prot=mmap.PROT_READ | mmap.PROT_WRITE,
                    offset=handle.offset,
                )
            finally:
                os.close(fd)
            try:
                self._pool = CpuPool(
                    info.pool_id,
                    info.capacity,
                    info.page_size,
                    medium="cxl",
                    mapping=mapping,
                    shared=True,
                )
            except BaseException:
                mapping.close()
                raise
        except BaseException:
            holds.release(dax, who)  # nothing maps it
            raise

    @property
    def cuda_registered(self) -> bool:
        return self._pool.cuda_registered

    def view(self, offset: int, length: int) -> memoryview:
        return self._pool.view(offset, length)

    def page_lock(self) -> None:
        self._pool.page_lock()

    def close(self) -> None:
        self._pool.close()  # raises while views are held: the hold stays
        if self._pool.unlock_failed:
            logger.error(
                "pool %s: CUDA may still reach it; its RM hold stays until the "
                "process ends",
                self._pool.pool_id,
            )
            return
        self._holds.release(self._dax, self._who)


@dataclass(eq=False)
class MappedPool:
    info: PoolInfo
    node_epoch: str
    mapping: PoolMapping
    # Views handed out and not yet released (PEP 688 export count)
    views: int = 0
    removing: bool = False
    # Taken out of use while views were held: the last released view closes it
    retired: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)


class _PoolSlice:
    """Part of a mapped pool; its views are counted until released or freed."""

    def __init__(self, maps: "PoolMaps", mapped: MappedPool, offset: int, length: int):
        self._maps = maps
        self._mapped = mapped
        self._offset = offset
        self._length = length

    def __buffer__(self, flags: int) -> memoryview:
        mapped = self._mapped
        with mapped.lock:
            mapped.views += 1
        return mapped.mapping.view(self._offset, self._length)

    def __release_buffer__(self, view: memoryview) -> None:
        view.release()
        mapped = self._mapped
        with mapped.lock:
            mapped.views -= 1
            close = mapped.retired and mapped.views == 0
            if close:
                mapped.retired = False
        if close:
            self._maps._close_later(mapped)


class PoolMaps:
    def __init__(
        self,
        pin_pools: bool = True,
        map_memfd: Callable[[PoolInfo, int], PoolMapping] = map_dram_memfd,
        map_dax: Callable[[PoolInfo, DaxMapping], PoolMapping] | None = None,
        holder: RegionHolder | None = None,
    ):
        """``holder``: who holds the CXL regions this process maps."""
        self.pin_pools = pin_pools
        self._map_memfd = map_memfd
        self._map_dax = map_dax or (
            lambda info, dax: CxlRegionMapping(info, dax, holder)
        )
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._pools: dict[str, MappedPool] = {}
        # Out of use and not closed yet: waiting for their last view, queued
        # for the closer, or being closed
        self._detached: set[MappedPool] = set()
        self._closes: collections.deque[MappedPool] = collections.deque()
        self._closing: MappedPool | None = None
        # Closes that raised: the mapping and its RM hold stay
        self._failed: set[MappedPool] = set()
        self._closer: threading.Thread | None = None

    def add(self, descriptor: PoolDescriptor, fd: int, node_epoch: str) -> MappedPool:
        """Map a DRAM pool from its memfd; the fd is closed here."""
        info = descriptor.pool
        try:
            if info.kind != PoolKind.DRAM:
                raise LocationError(f"pool {info.pool_id}: a memfd maps DRAM only")
            _check_shape(info)
            mapping = self._map_memfd(info, fd)
        finally:
            os.close(fd)
        return self._install(info, mapping, node_epoch)

    def add_dax(self, descriptor: PoolDescriptor, node_epoch: str) -> MappedPool:
        """Map a CXL region through its RM."""
        info, dax = descriptor.pool, descriptor.dax
        if info.kind != PoolKind.CXL or dax is None:
            raise LocationError(f"pool {info.pool_id}: not a CXL region")
        _check_shape(info)
        return self._install(info, self._map_dax(info, dax), node_epoch)

    def _install(
        self, info: PoolInfo, mapping: PoolMapping, node_epoch: str
    ) -> MappedPool:
        if self.pin_pools:
            mapping.page_lock()
        mapped = MappedPool(info=info, node_epoch=node_epoch, mapping=mapping)
        with self._lock:
            old = self._pools.get(info.pool_id)
            self._pools[info.pool_id] = mapped
        if old is not None:
            self._detach(old)
        logger.info(
            "mapped %s (%d MiB, page-locked: %s)",
            info.pool_id,
            info.capacity >> 20,
            mapping.cuda_registered,
        )
        return mapped

    def check(self, location: Location) -> MappedPool:
        """The mapped pool ``location`` lies in; LocationError otherwise."""
        mapped = self._pools.get(location.pool_id)
        if mapped is None:
            raise LocationError(f"pool {location.pool_id!r} is not mapped")
        info = mapped.info
        if location.kind != info.kind:
            raise LocationError(f"kind {location.kind} is not the pool's {info.kind}")
        if (
            location.offset < 0
            or location.length <= 0
            or location.offset % info.page_size
            or location.length > info.page_size
            or location.offset + location.length > info.capacity
        ):
            raise LocationError(
                f"offset {location.offset} length {location.length} is not inside "
                f"a page of {location.pool_id} ({info.capacity} bytes, pages of "
                f"{info.page_size})"
            )
        return mapped

    def view(
        self,
        location: Location,
        length: int | None = None,
        node_epoch: str | None = None,
    ) -> memoryview:
        """Checked bytes of ``location`` (the first ``length``); with
        ``node_epoch``, only through that run's mapping of the pool."""
        n = location.length if length is None else length
        if not 0 < n <= location.length:
            raise LocationError(f"length {n} is not within the location")
        with self._lock:
            mapped = self.check(location)
            if mapped.removing:
                raise LocationError(f"pool {location.pool_id!r} is being unmapped")
            if node_epoch is not None and mapped.node_epoch != node_epoch:
                raise StaleRunError(
                    f"pool {location.pool_id!r} is mapped for node run "
                    f"{mapped.node_epoch}, not {node_epoch}"
                )
            # Counted before the caller sees it
            return memoryview(_PoolSlice(self, mapped, location.offset, n))

    def get(self, pool_id: str) -> MappedPool | None:
        with self._lock:
            return self._pools.get(pool_id)

    def pool_ids(self) -> list[str]:
        with self._lock:
            return list(self._pools)

    def page_locked(self) -> bool:
        """Some pool is mapped and every one is registered with CUDA."""
        with self._lock:
            pools = list(self._pools.values())
        return bool(pools) and all(m.mapping.cuda_registered for m in pools)

    def remove(self, pool_id: str, expected: MappedPool | None = None) -> bool:
        """Unregister and unmap on the closer thread (wait_closed tells when);
        False (nothing touched) while views are held. With ``expected``, only
        that mapping: another one installed since is left."""
        with self._lock:
            mapped = self._pools.get(pool_id)
            if mapped is None or (expected is not None and mapped is not expected):
                return True
            with mapped.lock:
                if mapped.views:
                    return False
                mapped.removing = True
            del self._pools[pool_id]
            self._detached.add(mapped)
            self._queue_close(mapped)
        return True

    def remove_all(self) -> list[str]:
        """Take every pool out of use: closed now if no view is held, else
        once its last view is released. Returns their pool_ids."""
        with self._lock:
            pools, self._pools = list(self._pools.values()), {}
        for mapped in pools:
            self._detach(mapped)
        return [m.info.pool_id for m in pools]

    def wait_closed(self, pool_id: str, timeout: float) -> bool:
        """Whether every mapping of ``pool_id`` taken out of use is closed
        within ``timeout``; False at once if a close failed."""

        def open_ones() -> list[MappedPool]:
            return [m for m in self._detached if m.info.pool_id == pool_id]

        with self._cv:
            self._cv.wait_for(
                lambda: not open_ones() or any(m in self._failed for m in open_ones()),
                timeout,
            )
            return not open_ones()

    def close(
        self, keep: set[str] | frozenset[str] = frozenset(), timeout: float = 10.0
    ) -> None:
        """Unmap every pool but ``keep``, which stay mapped and registered
        until the process ends, and wait up to ``timeout`` for the closes
        queued; RuntimeError if views are still held."""
        errors = []
        for pool_id in self.pool_ids():
            if pool_id in keep:
                with self._lock:
                    _KEPT_UNTIL_EXIT.append(self._pools.pop(pool_id))
            elif not self.remove(pool_id):
                errors.append(pool_id)
        with self._lock:
            # Detached by a lost connection while a view is held, or queued
            # and not started: kept too, and no later view release closes it
            for mapped in list(self._detached):
                if (
                    mapped.info.pool_id in keep
                    and mapped is not self._closing
                    and mapped not in self._failed
                ):
                    self._detached.discard(mapped)
                    if mapped in self._closes:
                        self._closes.remove(mapped)
                    with mapped.lock:
                        mapped.retired = False
                    _KEPT_UNTIL_EXIT.append(mapped)
            self._cv.notify_all()
        self.flush(timeout)
        if errors:
            raise RuntimeError(
                f"Release the buffers of {errors} before closing the handler"
            )

    def flush(self, timeout: float) -> bool:
        """Whether every close queued so far ended within ``timeout``."""
        with self._cv:
            return self._cv.wait_for(
                lambda: not self._closes and self._closing is None, timeout
            )

    def _detach(self, mapped: MappedPool) -> None:
        """Out of ``_pools`` already: closed now, or by its last view."""
        with self._lock:
            self._detached.add(mapped)
        with mapped.lock:
            mapped.removing = True
            if mapped.views:
                mapped.retired = True
                return
        self._close_later(mapped)

    def _close_later(self, mapped: MappedPool) -> None:
        with self._lock:
            if mapped in self._detached:  # not kept meanwhile
                self._queue_close(mapped)

    def _queue_close(self, mapped: MappedPool) -> None:
        """Caller holds the lock."""
        self._closes.append(mapped)
        if self._closer is None:
            self._closer = threading.Thread(
                target=self._close_loop, name="maru-pool-close", daemon=True
            )
            self._closer.start()
        self._cv.notify_all()

    def _close_loop(self) -> None:
        while True:
            with self._cv:
                if not self._closes:
                    self._closer = None  # a later close starts a new thread
                    return
                mapped = self._closing = self._closes.popleft()
            closed = True
            try:
                # No view is left, so the unmap after the CUDA unregister works
                mapped.mapping.close()
            except Exception:
                closed = False
                logger.exception(
                    "closing the mapping of %s failed", mapped.info.pool_id
                )
            with self._cv:
                self._closing = None
                if closed:
                    self._detached.discard(mapped)
                else:
                    self._failed.add(mapped)  # not retried; wait_closed is False
                self._cv.notify_all()

    def views(self, pool_id: str) -> int:
        mapped = self.get(pool_id)
        return mapped.views if mapped else 0


def _check_shape(info: PoolInfo) -> None:
    if info.capacity <= 0 or info.page_size <= 0 or info.capacity % info.page_size:
        raise LocationError(f"pool {info.pool_id}: bad capacity or page size")
