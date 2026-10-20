# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Region map service: map regions for remote Handlers and keep them registered.

This is the remote counterpart of ``DaxMapper`` in a Handler on a
CXL-attached node. For each region a Handler asks for:

1. The first request checks the handle with the resource manager
   (``GET_ACCESS``, as ``DaxMapper`` does), maps the region and registers the
   mapping with this node's NIC through NIXL.
2. A later request for a mapped region only compares its handle with the
   checked one (region id, offset, length, auth token) and shares the
   registration.
3. The registration lives while some connected Handler holds the region.
   When the last holder unmaps it, or its connection closes, the region is
   deregistered and unmapped.
4. Whoever gives the region back (its owner, or MaruServer once the owner
   left) releases it first: the region is deregistered and unmapped whoever
   else holds it, and later requests to map that handle are refused. After
   the reply, an RDMA that names the old registration fails at the NIC.
5. The resource manager also frees regions on its own: when MaruServer, the
   client that owns them, exits or loses its connection, they are reaped
   after a grace period without a release here, and their device range may
   go to a new region. So before a first map registers a region, every held
   mapping of an overlapping device range is checked with the resource
   manager, and the ones it freed are released; the first map waits while
   the resource manager cannot be asked. Every remote writer of the new
   region maps it here first, so no RDMA reaches it through an old
   registration. A sweep (:meth:`RegionMapService.start_sweep`) also checks
   every held mapping every :data:`SWEEP_INTERVAL_S` and releases the freed
   ones, which bounds how long an old registration overlaps a region that a
   Handler on this node writes through its own mapping.

Two kinds of handle name a region. The handle itself carries the resource
manager's auth token; MaruServer gives it to the region's owner only. A
reader gets a read handle, whose read token is derived from the auth token
(``maru_remote.region_map.read_handle``). A read handle maps a region that is
mapped here already and never releases one, so a reader cannot take a region
away from its owner.

A map reply carries the NIXL metadata of that region's registration only,
so a Handler learns the remote key of the regions it asked for and no other.

Requests for different regions run concurrently; requests for one region
wait for the first one to finish mapping it, and a first map waits for
earlier first maps of an overlapping range.
"""

from __future__ import annotations

import hmac
import itertools
import logging
import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

from maru_remote.region_map import read_token
from maru_shm.types import MaruHandle

logger = logging.getLogger(__name__)

_MAP_WAIT_S = 600.0
# Released handles remembered so that a late map of a returned region is
# refused. Region ids grow, so old entries only matter for a short while.
_RELEASED_KEPT = 65536
# How often the sweep checks every held mapping with the resource manager.
# The resource manager reaps a region of a client that left after about a
# second (same node) to 30 s (another node); this bounds how much longer an
# old registration lives.
SWEEP_INTERVAL_S = 1.0
# How long close() waits for a sweep in progress (it may wait on the RM).
_SWEEP_JOIN_S = 5.0


class MapDenied(PermissionError):  # noqa: N818 (a PermissionError)
    """A handle does not match the region it names, or was released."""


class NotMapped(LookupError):  # noqa: N818 (a LookupError)
    """A read handle names a region that is not mapped here now.

    Only a handle with the auth token can map a region the first time; the
    region's owner or MaruServer maps it again.
    """


@dataclass(frozen=True)
class ReadHandle:
    """A reader's name for a region: where it is and its read token."""

    region_id: int
    offset: int
    length: int
    read_token: str

    def matches(self, handle: MaruHandle) -> bool:
        """Whether this read handle was derived from ``handle``."""
        return (
            (self.region_id, self.offset, self.length)
            == (handle.region_id, handle.offset, handle.length)
        ) and hmac.compare_digest(self.read_token, read_token(handle.to_dict()))


@dataclass
class _Mapping:
    """One region mapped here, from its first map until its release.

    ``ready`` is set once the first map finished. A mapping with ``error``
    set is out of use: requests are refused, and if ``registration`` is set
    the NIC still holds it, so release, the sweep and close deregister it.
    """

    handle: MaruHandle
    seq: int = 0  # order of first maps; an earlier one of a range goes first
    ready: threading.Event = field(default_factory=threading.Event)
    error: BaseException | None = None
    device: str = ""  # the device the resource manager placed the region on
    addr: int = 0
    registration: Any = None
    metadata: bytes = b""
    holders: set[int] = field(default_factory=set)


def parse_handle(value: Any) -> MaruHandle | ReadHandle:
    """Validate a handle or read handle received over the wire.

    Args:
        value: ``MaruHandle.to_dict()`` or ``read_handle()``-shaped map.

    Returns:
        The handle, or the read handle if the map has ``read_token``.

    Raises:
        ValueError: if a field is missing or malformed.
    """
    if not isinstance(value, dict):
        raise ValueError("handle must be a map")
    fields = {}
    names = ("region_id", "offset", "length")
    if "read_token" not in value:
        names += ("auth_token",)
    for name in names:
        item = value.get(name)
        if type(item) is not int or item < 0:
            raise ValueError(f"handle.{name} must be a nonnegative integer")
        fields[name] = item
    if fields["length"] == 0:
        raise ValueError("handle.length must be positive")
    if "auth_token" in fields:
        return MaruHandle(**fields)
    token = value["read_token"]
    if not isinstance(token, str) or not token:
        raise ValueError("handle.read_token must be a string")
    return ReadHandle(read_token=token, **fields)


def _device_of(access: Any) -> str:
    """The device a ``GET_ACCESS`` reply places a region on.

    A DEV_DAX region names its device (by UUID when it has one); an FS_DAX
    region names a file of its own, so it overlaps no other region.
    """
    return access.device_uuid or access.dax_path


def _overlap(a: MaruHandle, b: MaruHandle) -> bool:
    """Whether two regions' offset ranges share a byte."""
    return a.offset < b.offset + b.length and b.offset < a.offset + a.length


class RegionMapService:
    """Regions mapped for remote Handlers on one memory node.

    The resource manager client, the mapper and the NIXL transport are passed
    in so tests can use fakes. The daemon calls :meth:`start_sweep` once.
    """

    def __init__(
        self,
        rm: Any,
        mapper: Any,
        transport: Any,
        *,
        node_id: str,
        epoch: str,
        boot_id: str = "",
    ) -> None:
        """Create the service.

        Args:
            rm: Resource manager client with ``get_access(handle)``
                (``MaruShmClient``). ``get_access`` raises RuntimeError when
                the resource manager refuses a handle (its region was freed,
                or the id names another region now) and another exception
                (ConnectionError, OSError) when it cannot be asked.
            mapper: Maps a region's device (``DaxRegionMapper``).
            transport: NIXL transport that registers mappings with the NIC
                (``NixlTransport``).
            node_id: Name of this memory node.
            epoch: Identity of this process, new on every start.
            boot_id: Identity of this boot of the node. A new boot lost what
                the node's CPU caches held, including bytes its NIC wrote
                there that had not reached the CXL memory yet.
        """
        self._rm = rm
        self._mapper = mapper
        self._transport = transport
        self.node_id = node_id
        self.epoch = epoch
        self.boot_id = boot_id
        self._maps: dict[int, _Mapping] = {}
        self._released: OrderedDict[int, MaruHandle] = OrderedDict()
        self._seq = itertools.count(1)
        self._drain_failed = False
        self._sweep_failed = False
        self._stop = threading.Event()
        self._sweeper: threading.Thread | None = None
        self._lock = threading.Lock()

    def hello(self) -> dict:
        """Identity of this remote CXL agent process and of the node's boot."""
        self._discard_notifs()
        return {"node": self.node_id, "epoch": self.epoch, "boot": self.boot_id}

    def holders(self) -> dict[int, set[int]]:
        """Mapped regions and the connections holding each (for tests, status)."""
        with self._lock:
            return {
                rid: set(m.holders)
                for rid, m in self._maps.items()
                if m.ready.is_set() and m.error is None
            }

    def map(self, conn: int, handle: Any) -> dict:
        """Map a region for a connection, or share an existing mapping.

        Args:
            conn: Connection id of the requesting Handler.
            handle: ``MaruHandle.to_dict()`` of the region, or its read
                handle (``maru_remote.region_map.read_handle``).

        Returns:
            ``{"region_id", "addr", "length", "metadata"}``; ``metadata``
            names this region's registration only.

        Raises:
            ValueError: if the handle is malformed.
            MapDenied: if the handle does not match a mapped region or the
                region was released.
            NotMapped: if a read handle names a region not mapped here.
            Exception: if the resource manager, the device or NIXL fails,
                or the resource manager cannot be asked whether an old
                mapping of the region's range is gone.
        """
        self._discard_notifs()
        h = parse_handle(handle)
        if isinstance(h, ReadHandle):
            return self._map_for_reader(conn, h)
        while True:
            with self._lock:
                if self._released.get(h.region_id) == h:
                    raise MapDenied(f"region {h.region_id} was released")
                m = self._maps.get(h.region_id)
                if m is None:
                    # A new region may reuse the id of a released one only
                    # with another handle (a restarted resource manager).
                    self._released.pop(h.region_id, None)
                    m = _Mapping(h, seq=next(self._seq))
                    self._maps[h.region_id] = m
                    step = "establish"
                elif not m.ready.is_set():
                    step = "wait"
                elif m.error is None and m.handle == h:
                    m.holders.add(conn)
                    return self._reply(m)
                else:
                    step = "replace"
            if step == "establish":
                return self._establish(m, conn)
            if step == "wait":
                self._wait(m)
            else:
                self._replace(m)

    def _map_for_reader(self, conn: int, h: ReadHandle) -> dict:
        """Share the mapping of a region with a reader."""
        while True:
            with self._lock:
                released = self._released.get(h.region_id)
                if released is not None and h.matches(released):
                    raise MapDenied(f"region {h.region_id} was released")
                m = self._maps.get(h.region_id)
                if m is None or (m.ready.is_set() and m.error is not None):
                    raise NotMapped(f"region {h.region_id} is not mapped here")
                if m.ready.is_set():
                    if not h.matches(m.handle):
                        raise MapDenied(
                            f"read token does not match region {h.region_id}"
                        )
                    m.holders.add(conn)
                    return self._reply(m)
            self._wait(m)

    def unmap(self, conn: int, region_id: int) -> None:
        """Drop a connection's hold on a region; release it if none is left.

        Args:
            conn: Connection id.
            region_id: Region number.
        """
        with self._lock:
            m = self._maps.get(region_id)
            if m is None or not m.ready.is_set():
                return
            m.holders.discard(conn)
            if not m.holders:
                self._release_locked(region_id, m)

    def release(self, handle: Any) -> None:
        """Deregister and unmap a region, whoever holds it.

        Whoever gives the region back calls this first: its owner, or
        MaruServer for a region whose owner left. Once it returns, the NIC
        refuses RDMA to the region's old registration and the handle cannot
        be mapped again. Releasing a region that is not mapped (never was,
        this process restarted, or the sweep released it already) succeeds.

        Args:
            handle: ``MaruHandle.to_dict()`` of the region.

        Raises:
            ValueError: if the handle is malformed.
            MapDenied: if the handle is a read handle, or a mapped region
                with this id has another handle.
            RuntimeError: if NIXL refused to deregister; the region stays
                registered and must not be given back.
        """
        h = parse_handle(handle)
        if isinstance(h, ReadHandle):
            raise MapDenied(
                f"region {h.region_id}: a read handle cannot release a region"
            )
        while True:
            with self._lock:
                m = self._maps.get(h.region_id)
                if m is None:
                    self._remember_released(h)
                    return
                if m.ready.is_set():
                    if m.handle != h:
                        raise MapDenied(f"handle does not match region {h.region_id}")
                    if not self._release_locked(h.region_id, m):
                        raise RuntimeError(f"deregistering region {h.region_id} failed")
                    self._remember_released(h)
                    return
            try:
                self._wait(m)
            except RuntimeError:
                pass  # the mapping failed: the next pass finds what is left

    def drop(self, conn: int) -> None:
        """A connection closed: drop its hold on every region.

        Args:
            conn: Connection id.
        """
        with self._lock:
            for rid, m in list(self._maps.items()):
                if not m.ready.is_set() or conn not in m.holders:
                    continue
                m.holders.discard(conn)
                if not m.holders:
                    self._release_locked(rid, m)

    def sweep(self) -> None:
        """Check every held mapping with the resource manager once.

        A mapping whose handle the resource manager refuses (the region was
        freed, or its id names another region now) is released as a release
        does: deregistered, unmapped, refused to later maps. A mapping that
        is out of use (refused, or with no holder left) and still registered
        gets its deregistration again. While the resource manager cannot be
        asked, every mapping stays.
        """
        with self._lock:
            held = [
                m
                for m in self._maps.values()
                if m.ready.is_set() and m.registration is not None
            ]
        for m in held:
            if m.error is None:
                try:
                    allocated = self._allocated(m.handle)
                except Exception as exc:
                    # Say so once while the resource manager stays away; the
                    # other mappings wait for the next sweep.
                    if not self._sweep_failed:
                        logger.warning(
                            "cannot check mapped regions with the resource "
                            "manager; keeping them: %s",
                            exc,
                        )
                    self._sweep_failed = True
                    return
                self._sweep_failed = False
                if allocated:
                    with self._lock:
                        if self._maps.get(m.handle.region_id) is m and not m.holders:
                            # Its last holder left, but deregistering failed.
                            self._release_locked(m.handle.region_id, m)
                    continue
            rid = m.handle.region_id
            with self._lock:
                if self._maps.get(rid) is not m:
                    continue  # released meanwhile
                if m.error is None:
                    logger.warning(
                        "region %d was freed by the resource manager; releasing it",
                        rid,
                    )
                    self._retire_locked(m, "the resource manager freed it")
                else:
                    self._release_locked(rid, m)

    def start_sweep(self, interval_s: float = SWEEP_INTERVAL_S) -> None:
        """Run :meth:`sweep` every ``interval_s`` seconds until :meth:`close`.

        Args:
            interval_s: Seconds between sweeps.
        """
        if self._sweeper is not None:
            return
        self._sweeper = threading.Thread(
            target=self._sweep_loop,
            args=(interval_s,),
            name="region-map-sweep",
            daemon=True,
        )
        self._sweeper.start()

    def _discard_notifs(self) -> None:
        """Drop the notifications workers send to check their connection here."""
        try:
            self._transport.discard_notifs()
        except Exception:
            # They pile up while this fails; say so once.
            if not self._drain_failed:
                logger.warning("dropping NIXL notifications failed", exc_info=True)
            self._drain_failed = True
        else:
            self._drain_failed = False

    def close(self) -> None:
        """Stop the sweep and release every region (the process is ending)."""
        self._stop.set()
        if self._sweeper is not None:
            self._sweeper.join(timeout=_SWEEP_JOIN_S)
        with self._lock:
            for rid, m in list(self._maps.items()):
                if m.ready.is_set() and m.registration is not None:
                    self._release_locked(rid, m)

    def _sweep_loop(self, interval_s: float) -> None:
        while not self._stop.wait(interval_s):
            try:
                self.sweep()
            except Exception:
                logger.error("sweeping mapped regions failed", exc_info=True)

    def _wait(self, m: _Mapping) -> None:
        """Wait for another request to finish mapping a region."""
        if not m.ready.wait(_MAP_WAIT_S):
            raise TimeoutError(f"region {m.handle.region_id} is still being mapped")
        if m.error is not None:
            raise RuntimeError(
                f"mapping region {m.handle.region_id} failed: {m.error}"
            ) from m.error

    def _allocated(self, handle: MaruHandle) -> bool:
        """Ask the resource manager whether ``handle`` names a live region.

        Returns:
            True if it accepts the handle, False if it refuses it.

        Raises:
            Exception: if the resource manager cannot be asked.
        """
        try:
            self._rm.get_access(handle)
        except RuntimeError:
            return False
        return True

    def _replace(self, m: _Mapping) -> None:
        """Clear a ready mapping that a map request cannot share.

        ``m`` holds the requested region id under another handle (a
        restarted resource manager gave the id out again), or is out of use
        and still registered. The first is released if the resource manager
        refuses its handle; the second gets its deregistration again.

        Raises:
            MapDenied: if the resource manager accepts ``m``'s handle.
            RuntimeError: if NIXL still refuses to deregister ``m``.
            Exception: if the resource manager cannot be asked.
        """
        rid = m.handle.region_id
        if m.error is None and self._allocated(m.handle):
            raise MapDenied(f"handle does not match region {rid}")
        with self._lock:
            if self._maps.get(rid) is not m:
                return  # released meanwhile
            if m.error is None:
                done = self._retire_locked(m, "the resource manager freed it")
            else:
                done = self._release_locked(rid, m)
        if not done:
            raise RuntimeError(
                f"region {rid} is still registered: deregistering failed"
            )

    def _wait_for_earlier(self, m: _Mapping) -> None:
        """Wait for earlier first maps of an overlapping range to finish.

        :meth:`_fence` sees a mapping once it is registered; an earlier first
        map that is still registering finishes first so that the fence sees
        it. A first map waits for earlier ones only, so none wait for each
        other. The device is known only once the resource manager answers,
        so overlapping offsets on any device count.
        """
        while True:
            with self._lock:
                earlier = next(
                    (
                        o
                        for o in self._maps.values()
                        if o.seq < m.seq
                        and not o.ready.is_set()
                        and _overlap(o.handle, m.handle)
                    ),
                    None,
                )
            if earlier is None:
                return
            if not earlier.ready.wait(_MAP_WAIT_S):
                raise TimeoutError(
                    f"region {earlier.handle.region_id} is still being mapped"
                )

    def _fence(self, m: _Mapping) -> None:
        """Release the held mappings of ``m``'s device range before it maps.

        The resource manager accepted ``m``'s handle, so the range is
        ``m``'s now, and any other mapping of it is of a region the resource
        manager freed without a release here. Each is checked and released,
        so ``m`` is registered only once no old registration names the range.

        Raises:
            RuntimeError: if an old mapping stays registered, or the
                resource manager still accepts its handle.
            ConnectionError: if the resource manager cannot be asked.
        """
        h = m.handle
        with self._lock:
            held = [
                o
                for o in self._maps.values()
                if o is not m
                and o.ready.is_set()
                and o.registration is not None
                and o.device == m.device
                and _overlap(o.handle, h)
            ]
        for o in held:
            rid = o.handle.region_id
            if o.error is None:
                try:
                    allocated = self._allocated(o.handle)
                except Exception as exc:
                    raise ConnectionError(
                        f"cannot check mapped region {rid} with the resource "
                        f"manager before mapping region {h.region_id} over its "
                        f"range: {exc}"
                    ) from exc
                if allocated:
                    raise RuntimeError(
                        f"region {h.region_id} overlaps mapped region {rid}, "
                        "which the resource manager still accepts"
                    )
            with self._lock:
                if self._maps.get(rid) is not o:
                    continue  # released meanwhile
                if o.error is None:
                    logger.warning(
                        "region %d takes the range of region %d, which the "
                        "resource manager freed; releasing region %d",
                        h.region_id,
                        rid,
                        rid,
                    )
                    done = self._retire_locked(o, "the resource manager freed it")
                else:
                    done = self._release_locked(rid, o)
            if not done:
                raise RuntimeError(
                    f"region {rid} over the range of region {h.region_id} is "
                    "still registered: deregistering it failed"
                )

    def _establish(self, m: _Mapping, conn: int) -> dict:
        """Check, map and register a region; the caller created ``m``."""
        h = m.handle
        try:
            self._wait_for_earlier(m)
            access = self._rm.get_access(h)
            m.device = _device_of(access)
            self._fence(m)
            addr = self._mapper.map(
                access.dax_path, access.device_uuid, h.offset, h.length
            )
            try:
                registration = self._transport.register(
                    addr, h.length, tag=f"region-{h.region_id}"
                )
            except BaseException:
                # No key was handed out for these addresses: reuse is safe.
                self._mapper.discard(addr, h.length)
                raise
            try:
                metadata = self._transport.partial_metadata(registration)
            except BaseException:
                # The NIC holds a key for these addresses: they are
                # deregistered and kept out of use as on a release.
                m.addr, m.registration = addr, registration
                raise
        except BaseException as exc:
            with self._lock:
                m.error = exc
                m.ready.set()
                if m.registration is None:
                    self._maps.pop(h.region_id, None)
                elif not self._release_locked(h.region_id, m):
                    # Tracked until deregistered: release, the sweep, close
                    # or a later map of the region try again.
                    logger.error(
                        "region %d stays registered until it is deregistered",
                        h.region_id,
                    )
            logger.warning("mapping region %d failed: %s", h.region_id, exc)
            raise
        with self._lock:
            m.addr = addr
            m.registration = registration
            m.metadata = metadata
            m.holders.add(conn)
            m.ready.set()
        logger.info("mapped region %d (%d bytes) at %#x", h.region_id, h.length, addr)
        return self._reply(m)

    @staticmethod
    def _reply(m: _Mapping) -> dict:
        return {
            "region_id": m.handle.region_id,
            "addr": m.addr,
            "length": m.handle.length,
            "metadata": m.metadata,
        }

    def _remember_released(self, h: MaruHandle) -> None:
        self._released[h.region_id] = h
        self._released.move_to_end(h.region_id)
        while len(self._released) > _RELEASED_KEPT:
            self._released.popitem(last=False)

    def _retire_locked(self, m: _Mapping, reason: str) -> bool:
        """Release a mapping whose region the resource manager freed.

        The lock is held. Its holders lose it and its handle is refused from
        now on, as after a release. If NIXL refuses to deregister, the
        mapping stays out of use and tracked, and the sweep, a release, a
        later map of the range or close try again.

        Returns:
            True if the region is deregistered (and no longer mapped here).
        """
        rid = m.handle.region_id
        m.error = MapDenied(f"region {rid} was released: {reason}")
        m.holders.clear()
        self._remember_released(m.handle)
        return self._release_locked(rid, m)

    def _release_locked(self, region_id: int, m: _Mapping) -> bool:
        """Deregister and unmap a region; the lock is held.

        Releasing under the lock keeps a new request for the same region
        from mapping it again before the old mapping is gone.

        Returns:
            True if the region is deregistered (and no longer mapped here).
        """
        try:
            self._transport.deregister(m.registration)
        except Exception:
            # Keep the mapping: unmapping a range the NIC still holds would
            # leave a registration without its mapping. A mapping in use is
            # shared by later requests; one out of use is deregistered again
            # by the next release, sweep, map of its range or close.
            logger.error("deregistering region %d failed", region_id, exc_info=True)
            return False
        del self._maps[region_id]
        try:
            self._mapper.unmap(m.addr, m.handle.length)
        except OSError:
            logger.error("unmapping region %d failed", region_id, exc_info=True)
        logger.info("released region %d", region_id)
        return True
