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
4. The region's owner releases it before giving it back to MaruServer: the
   region is deregistered and unmapped whoever else holds it, and later
   requests to map that handle are refused. After the reply, an RDMA that
   names the old registration fails at the NIC.

A map reply carries the NIXL metadata of that region's registration only,
so a Handler learns the remote key of the regions it asked for and no other.

Requests for different regions run concurrently; requests for one region
wait for the first one to finish mapping it.
"""

from __future__ import annotations

import logging
import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

from maru_shm.types import MaruHandle

logger = logging.getLogger(__name__)

_MAP_WAIT_S = 600.0
# Released handles remembered so that a late map of a returned region is
# refused. Region ids grow, so old entries only matter for a short while.
_RELEASED_KEPT = 65536


class MapDenied(PermissionError):  # noqa: N818 (a PermissionError)
    """A handle does not match the region it names, or was released."""


@dataclass
class _Mapping:
    handle: MaruHandle
    ready: threading.Event = field(default_factory=threading.Event)
    error: BaseException | None = None
    addr: int = 0
    registration: Any = None
    metadata: bytes = b""
    holders: set[int] = field(default_factory=set)


def parse_handle(value: Any) -> MaruHandle:
    """Validate a handle received over the wire.

    Args:
        value: ``MaruHandle.to_dict()``-shaped map.

    Returns:
        The handle.

    Raises:
        ValueError: if a field is missing or not a nonnegative integer.
    """
    if not isinstance(value, dict):
        raise ValueError("handle must be a map")
    fields = {}
    for name in ("region_id", "offset", "length", "auth_token"):
        item = value.get(name)
        if type(item) is not int or item < 0:
            raise ValueError(f"handle.{name} must be a nonnegative integer")
        fields[name] = item
    if fields["length"] == 0:
        raise ValueError("handle.length must be positive")
    return MaruHandle(**fields)


class RegionMapService:
    """Regions mapped for remote Handlers on one memory node.

    The resource manager client, the mapper and the NIXL transport are passed
    in so tests can use fakes.
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
                (``MaruShmClient``).
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
        self._lock = threading.Lock()

    def hello(self) -> dict:
        """Identity of this region mapper process and of the node's boot."""
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
            handle: ``MaruHandle.to_dict()`` of the region.

        Returns:
            ``{"region_id", "addr", "length", "metadata"}``; ``metadata``
            names this region's registration only.

        Raises:
            ValueError: if the handle is malformed.
            MapDenied: if the handle does not match a mapped region or its
                owner released it.
            Exception: if the resource manager, the device or NIXL fails.
        """
        h = parse_handle(handle)
        while True:
            with self._lock:
                if self._released.get(h.region_id) == h:
                    raise MapDenied(f"region {h.region_id} was released by its owner")
                m = self._maps.get(h.region_id)
                if m is None:
                    # A new region may reuse the id of a released one only
                    # with another handle (a restarted resource manager).
                    self._released.pop(h.region_id, None)
                    m = self._maps[h.region_id] = _Mapping(h)
                    creator = True
                elif m.ready.is_set():
                    if m.handle != h:
                        raise MapDenied(f"handle does not match region {h.region_id}")
                    m.holders.add(conn)
                    return self._reply(m)
                else:
                    creator = False
            if creator:
                return self._establish(m, conn)
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
        """Deregister and unmap a region for its owner, whoever holds it.

        The owner calls this before it gives the region back. Once it
        returns, the NIC refuses RDMA to the region's old registration and
        the handle cannot be mapped again. Releasing a region that is not
        mapped (never was, or this process restarted) succeeds.

        Args:
            handle: ``MaruHandle.to_dict()`` of the region.

        Raises:
            ValueError: if the handle is malformed.
            MapDenied: if a mapped region with this id has another handle.
            RuntimeError: if NIXL refused to deregister; the region stays
                registered and must not be given back.
        """
        h = parse_handle(handle)
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
                pass  # the mapping failed: the next pass finds nothing mapped

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

    def close(self) -> None:
        """Release every region (the process is ending)."""
        with self._lock:
            for rid, m in list(self._maps.items()):
                if m.ready.is_set() and m.error is None:
                    self._release_locked(rid, m)

    def _wait(self, m: _Mapping) -> None:
        """Wait for another request to finish mapping a region."""
        if not m.ready.wait(_MAP_WAIT_S):
            raise TimeoutError(f"region {m.handle.region_id} is still being mapped")
        if m.error is not None:
            raise RuntimeError(
                f"mapping region {m.handle.region_id} failed: {m.error}"
            ) from m.error

    def _establish(self, m: _Mapping, conn: int) -> dict:
        """Check, map and register a region; the caller created ``m``."""
        h = m.handle
        try:
            access = self._rm.get_access(h)
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
                self._transport.deregister(registration)
                self._mapper.unmap(addr, h.length)
                raise
        except BaseException as exc:
            with self._lock:
                self._maps.pop(h.region_id, None)
                m.error = exc
                m.ready.set()
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
            # leave a registration without its mapping. A later request for
            # the region reuses it; the end of the process ends it.
            logger.error("deregistering region %d failed", region_id, exc_info=True)
            return False
        del self._maps[region_id]
        try:
            self._mapper.unmap(m.addr, m.handle.length)
        except OSError:
            logger.error("unmapping region %d failed", region_id, exc_info=True)
        logger.info("released region %d", region_id)
        return True
