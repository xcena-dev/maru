# SPDX-License-Identifier: Apache-2.0
"""A worker's staging buffer, NIXL agent and region mapper connections."""

from __future__ import annotations

import enum
import logging
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

from maru_common.storage_types import StorageError, StorageUnavailableError
from maru_remote.region_map import pick_mapper

from ..staging import StagingBuffer
from .region import RegionAccess

logger = logging.getLogger(__name__)

DEFAULT_STAGING = 1024**3
# A region mapper whose map failed is not asked again for this long, so loads
# and stores on its regions miss at once instead of waiting for a connect.
DOWN_BACKOFF_S = 5.0
# Connecting to a region mapper; replies may take longer (a first map
# registers a whole region with the NIC).
CONNECT_TIMEOUT_S = 3.0


class _StaleReplyError(StorageUnavailableError):
    """A map reply came over a connection to the region mapper that ended."""


class Release(enum.Enum):
    """Outcome of asking a region mapper to deregister a region."""

    DONE = "done"  # the region is not registered at its mapper
    FAILED = "failed"  # the mapper answered; the region stays registered
    NO_REPLY = "no_reply"  # the mapper did not answer


class RdmaDataPath:
    """Staging buffer, NIXL agent and region mapper connections of a worker.

    One worker keeps one staging buffer and one NIXL agent however many
    pools it uses. Region access is cached per region; a region mapper
    connection that breaks loses every mapping it held, so its cached
    accesses are dropped and the regions are mapped again on next use.
    """

    def __init__(
        self,
        config: Any,
        transport_factory: Callable[[str], Any] | None = None,
        mapper_factory: Callable[[str], Any] | None = None,
    ) -> None:
        """Create an idle data path; :meth:`start` sets it up.

        Args:
            config: ``MaruConfig`` of the worker.
            transport_factory: ``agent name -> NixlTransport`` (tests inject
                fakes).
            mapper_factory: ``address -> RegionMapClient``.
        """
        self.config = config
        self._transport_factory = transport_factory or _default_transport(config)
        self._mapper_factory = mapper_factory or self._default_mapper
        # Region mapper key, read by start() for the default mapper factory
        self._key: bytes | None = None
        self._needs_key = mapper_factory is None
        self.staging: StagingBuffer | None = None
        self.transport: Any = None
        self._registration: Any = None
        self._mappers: dict[str, Any] = {}
        self._access: dict[str, RegionAccess] = {}
        # Region mapper -> time until which it counts as down
        self._down: dict[str, float] = {}
        # Region -> time until which a map the mapper failed is not retried
        self._map_failed: dict[str, float] = {}
        # Own region -> (mapper, connection generation) of a map reply that
        # was not turned into an access; the mapper holds the region for
        # that connection until it is released or the connection ends.
        self._attempted: dict[str, tuple[str, int]] = {}
        # Region mapper -> (NIXL peer, connection generation) when the peer's
        # metadata was last loaded through that mapper's connection. A peer
        # dropped after a transfer error stays listed; has_peer() tells.
        self._loaded: dict[str, tuple[str, int]] = {}
        self._lock = threading.Lock()
        # Held from the check of a peer to the load of its metadata, and
        # while a peer is dropped
        self._load_lock = threading.RLock()

    def start(self) -> None:
        """Map the staging buffer and register it with this node's NIC.

        Raises:
            StorageError: if the region mapper key cannot be read.
            Exception: if the buffer, the NIXL agent or the registration
                fails; nothing is left set up.
        """
        if self.transport is not None:
            return
        if self._needs_key and self._key is None:
            from maru_remote.region_map import KeyFileError, load_key

            try:
                self._key = load_key(self.config.region_mapper_key_file)
            except KeyFileError as exc:
                raise StorageError(str(exc)) from exc
        page = self.config.chunk_size_bytes
        size = self.config.rdma_staging_size or max(64 * page, DEFAULT_STAGING)
        staging = transport = None
        try:
            staging = StagingBuffer(size, page)
            staging.load_reserve = int(staging.count * self.config.rdma_load_reserve)
            staging.cuda_register()
            name = f"maru-worker-{self.config.engine_id}-{uuid.uuid4().hex[:8]}"
            transport = self._transport_factory(name)
            registration = transport.register(
                staging.address, staging.nbytes, "staging"
            )
        except BaseException:
            if transport is not None:
                try:
                    transport.close()
                except Exception:
                    logger.warning("RDMA transport cleanup failed", exc_info=True)
            if staging is not None:
                staging.close()
            raise
        self.staging, self.transport, self._registration = (
            staging,
            transport,
            registration,
        )

    def region(
        self, pool_id: str, handle: dict, mappers: list[str], own: bool = False
    ) -> RegionAccess:
        """Access to a region, asking its region mapper to map it on first use.

        Args:
            pool_id: The region's pool id at MaruServer.
            handle: ``MaruHandle.to_dict()`` of the region.
            mappers: Region mapper addresses of the region's pool.
            own: Whether the region belongs to this worker's session; a map
                reply is then remembered even if no access results from it
                (see :meth:`map_attempted`).

        Returns:
            Where the region is reachable.

        Raises:
            StorageUnavailableError: if the region cannot be mapped now (its
                region mapper is down or refused, or NIXL refused its
                metadata).
        """
        from maru_remote.region_map import RegionMapError, RegionMapUnavailable

        with self._lock:
            access = self._access.get(pool_id)
            client = self._mappers.get(access.mapper) if access else None
            retry_at = self._map_failed.get(pool_id)
        if access is not None and client is not None:
            if client.generation == access.generation:
                return access
        if not mappers:
            raise StorageUnavailableError("MaruServer named no region mapper")
        region_id = handle.get("region_id")
        if retry_at is not None and time.monotonic() < retry_at:
            raise StorageUnavailableError(
                f"region {region_id} failed to map a moment ago"
            )
        address = pick_mapper(mappers, int(handle["region_id"]))
        if self.is_down(address):
            raise StorageUnavailableError(f"region mapper {address} is down")
        client = self._client(address)
        for attempt in range(2):
            try:
                reply = client.map(handle)
            except RegionMapUnavailable as exc:
                # The client closed the connection: whatever the request
                # mapped is released by the mapper when it sees the
                # connection end.
                self._mark_down(address)
                raise StorageUnavailableError(
                    f"region {region_id} could not be mapped: {exc}"
                ) from exc
            except RegionMapError as exc:
                # The mapper refused and holds no mapping of this region.
                self._not_mapped(pool_id)
                if exc.code == "unavailable":
                    with self._lock:
                        self._map_failed[pool_id] = time.monotonic() + DOWN_BACKOFF_S
                raise StorageUnavailableError(
                    f"region {region_id} could not be mapped: {exc}"
                ) from exc
            if own:
                with self._lock:
                    self._attempted[pool_id] = (address, reply.generation)
            if reply.generation != client.generation:
                # The connection the reply came over ended since; the mapper
                # released what it held.
                if attempt:
                    raise _StaleReplyError(
                        f"region {region_id} could not be mapped: its "
                        "connection to the region mapper keeps breaking"
                    )
                continue
            # The reply names this region's registration only; loading it
            # into a peer this agent already knows adds the region to it.
            try:
                peer = self._load_peer(
                    address, reply.generation, reply.metadata, pool_id, region_id
                )
            except _StaleReplyError:
                if attempt:
                    raise
                continue  # map again over the current connection
            break
        access = RegionAccess(
            peer, reply.addr, reply.length, address, reply.generation, reply.boot
        )
        with self._lock:
            self._access[pool_id] = access
            self._down.pop(address, None)
            self._map_failed.pop(pool_id, None)
        return access

    def _load_peer(
        self,
        address: str,
        generation: int,
        metadata: bytes,
        pool_id: str,
        region_id: object,
    ) -> str:
        """Load a map reply's metadata into the region mapper's NIXL peer.

        UCX fails a connection on its own when the peer stops answering (a
        stopped process, a network outage), and NIXL closes it but keeps it
        in the peer: loading metadata into that peer again crashes the
        process. So the loaded peer is dropped and connected anew when
        :meth:`NixlTransport.peer_usable` says no (a transfer to it failed,
        or NIXL refuses to send to it), and also when the connection to the
        mapper broke since it was loaded (the mapper stopped answering or
        restarted; a restarted mapper's old peer is then dropped too). The
        check holds for its moment: UCX can still fail the connection in the
        moment between the check and the load.

        Raises:
            _StaleReplyError: if the reply came over a connection that has ended
                since another map loaded the peer; the mapper released what
                that connection held.
            StorageUnavailableError: if NIXL refused to drop the old peer or
                to load the metadata.
        """
        with self._load_lock:
            with self._lock:
                loaded = self._loaded.get(address)
            if loaded is not None and generation < loaded[1]:
                raise _StaleReplyError(
                    f"region {region_id} could not be mapped: the reply came "
                    "over an earlier connection"
                )
            if (
                loaded is not None
                and self.transport.has_peer(loaded[0])
                and (
                    generation > loaded[1] or not self.transport.peer_usable(loaded[0])
                )
                and not self.disconnect({loaded[0]})
                and self.transport.has_peer(loaded[0])
            ):
                with self._lock:
                    self._map_failed[pool_id] = time.monotonic() + DOWN_BACKOFF_S
                raise StorageUnavailableError(
                    f"region {region_id} could not be mapped: NIXL kept the "
                    f"connection to {loaded[0]}"
                )
            try:
                peer = self.transport.add_peer(metadata)
            except Exception as exc:
                raise StorageUnavailableError(
                    f"NIXL refused the metadata of region {region_id}: {exc}"
                ) from exc
            with self._lock:
                self._loaded[address] = (peer, generation)
        return peer

    def is_down(self, address: str) -> bool:
        """Whether a map through this region mapper failed a moment ago."""
        with self._lock:
            until = self._down.get(address)
        return until is not None and time.monotonic() < until

    def all_down(self, mappers: list[str]) -> bool:
        """Whether every one of these region mappers is down right now."""
        return bool(mappers) and all(self.is_down(m) for m in mappers)

    def has_access(self, pool_id: str) -> bool:
        """Whether a region is mapped for this worker (an access is cached)."""
        with self._lock:
            return pool_id in self._access

    def map_attempted(self, pool_id: str) -> bool:
        """Whether a region is mapped at its mapper for this worker.

        True while an access is cached, and also after a map reply for an
        own region that gave no access (NIXL refused its metadata) as long
        as that reply's connection is open: the mapper then still holds the
        region, so it needs a release before it goes back. A map that got
        no reply needs none; its connection is closed, and the mapper
        releases what a closed connection held.
        """
        with self._lock:
            if pool_id in self._access:
                return True
            held = self._attempted.get(pool_id)
            client = self._mappers.get(held[0]) if held else None
        return client is not None and client.generation == held[1]

    def forget(self, pool_id: str) -> None:
        """Drop a region's cached access; the next use maps it again."""
        with self._lock:
            self._access.pop(pool_id, None)

    def boots(self, addresses: list[str]) -> dict[str, str]:
        """Ask region mappers this worker talks to for their node's boot id.

        A connection found broken is opened again, which also tells the
        caller's cached accesses through that mapper to map again.

        Args:
            addresses: Region mapper addresses.

        Returns:
            ``address -> boot id`` for the region mappers that answered.
        """
        from maru_remote.region_map import RegionMapError, RegionMapUnavailable

        out = {}
        for address in addresses:
            with self._lock:
                client = self._mappers.get(address)
            if client is None or self.is_down(address):
                continue
            try:
                out[address] = str(client.hello().get("boot", ""))
            except RegionMapUnavailable:
                self._mark_down(address)
            except RegionMapError:
                continue
        return out

    def release(self, pool_id: str, handle: dict, mappers: list[str]) -> Release:
        """Have the region's mapper deregister a region this worker owns.

        Args:
            pool_id: The region's pool id at MaruServer.
            handle: ``MaruHandle.to_dict()`` of the region.
            mappers: Region mapper addresses of the region's pool.

        Returns:
            ``Release.DONE`` once the region is not registered at its mapper:
            released now, never mapped there, or that mapper maps another
            region under this id. ``Release.FAILED`` if the mapper answered
            that it could not deregister the region. ``Release.NO_REPLY`` if
            the mapper did not answer; it then counts as down.
        """
        from maru_remote.region_map import RegionMapError, RegionMapUnavailable

        self.forget(pool_id)
        if not mappers:
            return Release.FAILED
        address = pick_mapper(mappers, int(handle["region_id"]))
        try:
            self._client(address).release(handle)
        except RegionMapError as exc:
            if exc.code != "denied":
                logger.error(
                    "release of region %s failed: %s", handle["region_id"], exc
                )
                return Release.FAILED
            # denied: the mapper's region under this id is not ours
        except RegionMapUnavailable as exc:
            self._mark_down(address)
            logger.error(
                "release of region %s not confirmed: %s", handle["region_id"], exc
            )
            return Release.NO_REPLY
        self._not_mapped(pool_id)
        return Release.DONE

    def unmap(self, pool_id: str, handle: dict, mappers: list[str]) -> None:
        """Drop this worker's hold on a region at its region mapper.

        The region stays registered while someone else holds it. A broken
        connection already dropped the hold.

        Args:
            pool_id: The region's pool id at MaruServer.
            handle: The region's handle or read handle.
            mappers: Region mapper addresses of the region's pool.
        """
        from maru_remote.region_map import RegionMapError

        self.forget(pool_id)
        self._not_mapped(pool_id)
        if not mappers:
            return
        address = pick_mapper(mappers, int(handle["region_id"]))
        with self._lock:
            client = self._mappers.get(address)
        if client is None:
            return
        try:
            client.unmap(int(handle["region_id"]))
        except RegionMapError as exc:
            logger.warning("unmap of region %s failed: %s", handle["region_id"], exc)

    def disconnect(self, peers: set[str]) -> bool:
        """Close the NIXL connections to these peers; True if all are closed.

        A load of metadata into a peer in progress finishes first.

        A transfer that ends in an error leaves a reliable connection in the
        error state, so later transfers to the same memory node fail too.
        Removing the peer drops that connection; the next use of a region on
        the node maps it again and loads the peer's metadata anew.
        """
        ok = True
        with self._load_lock:
            for peer in peers:
                ok = self.transport.disconnect(peer) and ok
            with self._lock:
                for pool_id, access in list(self._access.items()):
                    if access.peer in peers:
                        del self._access[pool_id]
        return ok

    def close(self) -> None:
        """Close region mapper connections, the NIXL agent and the staging."""
        with self._lock:
            clients = list(self._mappers.values())
            self._mappers.clear()
            self._access.clear()
            self._loaded.clear()
        for client in clients:
            client.close()  # releases every mapping the connection held
        if self.transport is not None:
            try:
                self.transport.close()
            except Exception:
                logger.warning("RDMA transport cleanup failed", exc_info=True)
            self.transport = None
        if self.staging is not None:
            self.staging.close()
            self.staging = None

    def _default_mapper(self, address: str) -> Any:
        from maru_remote.region_map import RegionMapClient

        return RegionMapClient(
            address,
            timeout_s=self.config.rdma_timeout_s,
            connect_timeout_s=CONNECT_TIMEOUT_S,
            key=self._key,
        )

    def _client(self, address: str) -> Any:
        with self._lock:
            client = self._mappers.get(address)
            if client is None:
                client = self._mappers[address] = self._mapper_factory(address)
            return client

    def _mark_down(self, address: str) -> None:
        with self._lock:
            self._down[address] = time.monotonic() + DOWN_BACKOFF_S

    def _not_mapped(self, pool_id: str) -> None:
        with self._lock:
            self._attempted.pop(pool_id, None)
            now = time.monotonic()
            for stale in [p for p, t in self._map_failed.items() if t <= now]:
                del self._map_failed[stale]


def _default_transport(config: Any) -> Callable[[str], Any]:
    def make(name: str) -> Any:
        from maru_remote.transport import NixlTransport

        return NixlTransport(name, ucx_device=config.rdma_device)

    return make
