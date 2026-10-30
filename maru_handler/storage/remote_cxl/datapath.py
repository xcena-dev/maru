# SPDX-License-Identifier: Apache-2.0
"""A worker's staging buffer, NIXL agent and remote CXL agent connections."""

from __future__ import annotations

import enum
import logging
import mmap
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

from maru_common.storage_types import StorageError, StorageUnavailableError
from maru_remote.region_map import pick_agent
from maru_remote.transport import buffer_address

from ..staging import StagingBuffer
from .region import RegionAccess

logger = logging.getLogger(__name__)

DEFAULT_STAGING = 1024**3
# A remote CXL agent whose map failed is not asked again for this long, so loads
# and stores on its regions miss at once instead of waiting for a connect.
DOWN_BACKOFF_S = 5.0
# Connecting to a remote CXL agent; replies may take longer (a first map
# registers a whole region with the NIC).
CONNECT_TIMEOUT_S = 3.0
# Shortest wait for a remote CXL agent's reply. Replies otherwise wait as long
# as an RDMA transfer (``rdma_timeout_s``); a short transfer deadline must not
# cut off a map that registers a whole region.
REPLY_TIMEOUT_MIN_S = 3.0
# Bytes a new NIXL connection reads from the region it was loaded for, which
# completes the connection's setup (see RdmaDataPath._complete_connection)
CONNECT_PROBE_BYTES = 64


class _StaleReplyError(StorageUnavailableError):
    """A map reply came over a connection to the remote CXL agent that ended."""


class Release(enum.Enum):
    """Outcome of asking a remote CXL agent to deregister a region."""

    DONE = "done"  # the region is not registered at its agent
    FAILED = "failed"  # the agent answered; the region stays registered
    NO_REPLY = "no_reply"  # the agent did not answer


class RdmaDataPath:
    """Staging buffer, NIXL agent and remote CXL agent connections of a worker.

    One worker keeps one staging buffer and one NIXL agent however many
    pools it uses. Region access is cached per region; a remote CXL agent
    connection that breaks loses every mapping it held, so its cached
    accesses are dropped and the regions are mapped again on next use.
    """

    def __init__(
        self,
        config: Any,
        transport_factory: Callable[[str], Any] | None = None,
        agent_factory: Callable[[str], Any] | None = None,
    ) -> None:
        """Create an idle data path; :meth:`start` sets it up.

        Args:
            config: ``MaruConfig`` of the worker.
            transport_factory: ``agent name -> NixlTransport`` (tests inject
                fakes).
            agent_factory: ``address -> RegionMapClient``.
        """
        self.config = config
        self._transport_factory = transport_factory or _default_transport(config)
        self._agent_factory = agent_factory or self._default_agent
        # Remote CXL agent key, read by start() for the default agent factory
        self._key: bytes | None = None
        self._needs_key = agent_factory is None
        self.staging: StagingBuffer | None = None
        self.transport: Any = None
        # A transport retained after failed setup/cleanup is not ready to use.
        self._started = False
        # Name of the NIXL agent in self.transport; map requests carry it
        self.transport_name = ""
        self._registration: Any = None
        # Registered page that connection probes read into, and the probes
        # that did not finish (their handles are released before close)
        self._probe: mmap.mmap | None = None
        self._probe_addr = 0
        self._probes: list[Any] = []
        self._agents: dict[str, Any] = {}
        self._access: dict[str, RegionAccess] = {}
        # Remote CXL agent -> time until which it counts as down
        self._down: dict[str, float] = {}
        # Remote CXL agents whose last request got no reply; one stays here
        # until a later request is answered (the back-off above ends sooner)
        self._unanswered: set[str] = set()
        # Region -> time until which a map the agent failed is not retried
        self._map_failed: dict[str, float] = {}
        # Own region -> (agent, connection generation) of a map reply that
        # was not turned into an access; the agent holds the region for
        # that connection until it is released or the connection ends.
        self._attempted: dict[str, tuple[str, int]] = {}
        # Own regions that had a map reply: until then a region (allocated
        # in this session) holds no KV, and its map says so (``fresh``).
        self._replied: set[str] = set()
        # Remote CXL agent -> (NIXL peer, connection generation) when the peer's
        # metadata was last loaded through that agent's connection. A peer
        # dropped after a transfer error stays listed; has_peer() tells.
        self._loaded: dict[str, tuple[str, int]] = {}
        # A peer introduced by a map whose connection probe has not yet
        # succeeded. Retain its identity to check/drop a failed endpoint
        # before a later map tries to load metadata into it again.
        self._unready: dict[str, tuple[str, int]] = {}
        self._lock = threading.Lock()
        # Held from the check of a peer to the load of its metadata, and
        # while a peer is dropped
        self._load_lock = threading.RLock()

    def start(self) -> None:
        """Map the staging buffer and register it with this node's NIC.

        Raises:
            StorageError: if the remote CXL agent key cannot be read.
            StorageUnavailableError: if earlier setup or cleanup retained
                incomplete resources; close them before restarting.
            Exception: if the buffer, the NIXL agent or the registration
                fails. Buffers remain owned if deregistration also fails.
        """
        if self.transport is not None:
            if not self._started:
                raise StorageUnavailableError(
                    "RDMA data path cleanup is pending; close it before restarting"
                )
            return
        if self._needs_key and self._key is None:
            from maru_remote.region_map import KeyFileError, load_key

            try:
                self._key = load_key(self.config.remote_cxl_agent_key_file)
            except KeyFileError as exc:
                raise StorageError(str(exc)) from exc
        page = self.config.chunk_size_bytes
        size = self.config.rdma_staging_size or max(64 * page, DEFAULT_STAGING)
        staging = transport = registration = None
        probe = mmap.mmap(-1, mmap.PAGESIZE)
        probe_addr = buffer_address(probe)
        try:
            staging = StagingBuffer(size, page)
            staging.load_reserve = int(staging.count * self.config.rdma_load_reserve)
            staging.cuda_register()
            name = f"maru-worker-{self.config.engine_id}-{uuid.uuid4().hex[:8]}"
            transport = self._transport_factory(name)
            registration = transport.register(
                staging.address, staging.nbytes, "staging"
            )
            transport.register(probe_addr, len(probe), "connect-probe")
        except BaseException:
            cleaned = True
            if transport is not None:
                try:
                    transport.close()
                except Exception:
                    logger.warning("RDMA transport cleanup failed", exc_info=True)
                    cleaned = False
            if cleaned:
                if staging is not None:
                    staging.close()
                probe.close()
            else:
                # The first registration may have succeeded before the
                # probe's registration failed. Keep both buffers until a
                # later close confirms deregistration.
                self.staging, self.transport, self._registration = (
                    staging,
                    transport,
                    registration,
                )
                self.transport_name = name
                self._probe, self._probe_addr = probe, probe_addr
            raise
        self.staging, self.transport, self._registration = (
            staging,
            transport,
            registration,
        )
        self.transport_name = name
        self._probe, self._probe_addr = probe, probe_addr
        self._started = True

    def region(
        self, pool_id: str, handle: dict, agents: list[str], own: bool = False
    ) -> RegionAccess:
        """Access to a region, asking its remote CXL agent to map it on first use.

        Args:
            pool_id: The region's pool id at MaruServer.
            handle: ``MaruHandle.to_dict()`` of the region.
            agents: Remote CXL agent addresses of the region's pool.
            own: Whether the region belongs to this worker's session; a map
                reply is then remembered even if no access results from it
                (see :meth:`map_attempted`), and the maps before the first
                reply tell the agent the region holds no KV yet.

        Returns:
            Where the region is reachable.

        Raises:
            StorageUnavailableError: if the region cannot be mapped now (its
                remote CXL agent is down or refused, or NIXL refused its
                metadata).
        """
        from maru_remote.region_map import RegionMapError, RegionMapUnavailable

        with self._lock:
            access = self._access.get(pool_id)
            client = self._agents.get(access.agent) if access else None
            retry_at = self._map_failed.get(pool_id)
        if access is not None and client is not None:
            if client.generation == access.generation:
                return access
        if not agents:
            raise StorageUnavailableError("MaruServer named no remote CXL agent")
        region_id = handle.get("region_id")
        if retry_at is not None and time.monotonic() < retry_at:
            raise StorageUnavailableError(
                f"region {region_id} failed to map a moment ago"
            )
        address = pick_agent(agents, int(handle["region_id"]))
        if self.is_down(address):
            raise StorageUnavailableError(f"remote CXL agent {address} is down")
        client = self._client(address)
        for attempt in range(2):
            with self._lock:
                fresh = own and pool_id not in self._replied
            try:
                reply = client.map(handle, nixl_agent=self.transport_name, fresh=fresh)
            except RegionMapUnavailable as exc:
                # The client closed the connection: whatever the request
                # mapped is released by the agent when it sees the
                # connection end.
                self._mark_down(address)
                raise StorageUnavailableError(
                    f"region {region_id} could not be mapped: {exc}"
                ) from exc
            except RegionMapError as exc:
                # The agent refused and holds no mapping of this region.
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
                    self._replied.add(pool_id)
            if reply.generation != client.generation:
                # The connection the reply came over ended since; the agent
                # released what it held.
                if attempt:
                    raise _StaleReplyError(
                        f"region {region_id} could not be mapped: its "
                        "connection to the remote CXL agent keeps breaking"
                    )
                continue
            # The reply names this region's registration only; loading it
            # into a peer this agent already knows adds the region to it.
            try:
                peer = self._load_peer(
                    address,
                    reply.generation,
                    reply.metadata,
                    pool_id,
                    region_id,
                    reply.addr,
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
            self._unanswered.discard(address)
            self._map_failed.pop(pool_id, None)
        return access

    def _load_peer(
        self,
        address: str,
        generation: int,
        metadata: bytes,
        pool_id: str,
        region_id: object,
        remote_addr: int | None = None,
    ) -> str:
        """Load a map reply's metadata into the remote CXL agent's NIXL peer.

        Then the peer is sent an empty notification (``announce``), which
        ends the agent's wait for this worker's connection, and a peer loaded
        anew has its connection completed by a small READ at ``remote_addr``
        (:meth:`_complete_connection`).

        UCX fails a connection on its own when the peer stops answering (a
        stopped process, a network outage), and NIXL closes it but keeps it
        in the peer: loading metadata into that peer again crashes the
        process. So the loaded peer is dropped and connected anew when
        :meth:`NixlTransport.peer_usable` says no (a transfer to it failed,
        or NIXL refuses to send to it), and also when the connection to the
        agent broke since it was loaded (the agent stopped answering or
        restarted; a restarted agent's old peer is then dropped too). The
        check holds for its moment: UCX can still fail the connection in the
        moment between the check and the load.

        Raises:
            _StaleReplyError: if the reply came over a connection that has ended
                since another map loaded the peer; the agent released what
                that connection held.
            StorageUnavailableError: if NIXL refused to drop the old peer or
                to load the metadata, or a new connection failed.
        """
        with self._load_lock:
            with self._lock:
                ready = self._loaded.get(address)
                loaded = ready or self._unready.get(address)
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
            connected = loaded is not None and self.transport.has_peer(loaded[0])
            if not connected:
                with self._lock:
                    self._loaded.pop(address, None)
                    self._unready.pop(address, None)
            try:
                peer = self.transport.add_peer(
                    metadata,
                    peer=loaded[0] if connected and loaded is not None else None,
                    timeout_s=self.config.rdma_timeout_s,
                )
            except Exception as exc:
                raise StorageUnavailableError(
                    f"NIXL refused the metadata of region {region_id}: {exc}"
                ) from exc
            with self._lock:
                self._unready[address] = (peer, generation)
            # Keep disconnect and another metadata load out until the new
            # connection is ready; a failed probe must not mark it ready.
            if not self.transport.announce(peer):
                logger.debug("NIXL refused to announce this worker to %s", peer)
            if remote_addr is not None and not (
                connected and ready is not None and ready[0] == peer
            ):
                self._complete_connection(peer, remote_addr, region_id)
            with self._lock:
                self._loaded[address] = (peer, generation)
                self._unready.pop(address, None)
        return peer

    def _complete_connection(
        self, peer: str, remote_addr: int, region_id: object
    ) -> None:
        """Complete a new NIXL connection with a small READ from its region.

        The worker progresses UCX through status queries and its native event
        thread. Complete setup here while the remote CXL agent drives its
        passive side for this map, so later KV transfers need not wait for
        connection setup.

        Args:
            peer: Peer loaded anew.
            remote_addr: Address of the region the map reply named.
            region_id: The region, for messages.

        Raises:
            StorageUnavailableError: if the READ fails or does not finish in
                time. An uncertain request and its probe buffer stay owned
                until completion is confirmed.
        """
        self.reap_probes()
        try:
            self.transport.read(
                peer,
                [(self._probe_addr, remote_addr, CONNECT_PROBE_BYTES)],
                timeout_s=self.config.rdma_timeout_s,
            )
        except BaseException as exc:
            pending = getattr(exc, "pending", None) or getattr(
                exc, "pending_transfer", None
            )
            if pending is not None:
                with self._lock:
                    self._probes.append(pending)
            if not isinstance(exc, Exception):
                raise
            raise StorageUnavailableError(
                f"region {region_id} could not be mapped: the new RDMA "
                f"connection to {peer} failed: {exc}"
            ) from exc

    def has_unconfirmed_probes(self) -> bool:
        """Whether a connection probe may still touch registered memory."""
        with self._lock:
            return bool(self._probes)

    def reap_probes(self) -> bool:
        """Release completed connection probes; True when none remains.

        The client and shared-pool router include these probes in their
        late-transfer cleanup, even if no KV transfer was ever posted.
        """
        with self._lock:
            self._probes = [p for p in self._probes if not p.poll()]
            return not self._probes

    def is_down(self, address: str) -> bool:
        """Whether a map through this remote CXL agent failed a moment ago."""
        with self._lock:
            until = self._down.get(address)
        return until is not None and time.monotonic() < until

    def answering(self, address: str) -> bool:
        """Whether a remote CXL agent answered since its last unanswered request.

        A remote CXL agent that stopped answering stays out of this until a
        later request to it (a map, or the heartbeat's boot check) gets a
        reply, however long that takes.
        """
        with self._lock:
            return address not in self._unanswered

    def all_down(self, agents: list[str]) -> bool:
        """Whether every one of these remote CXL agents is down right now."""
        return bool(agents) and all(self.is_down(m) for m in agents)

    def has_access(self, pool_id: str) -> bool:
        """Whether a region is mapped for this worker (an access is cached)."""
        with self._lock:
            return pool_id in self._access

    def map_attempted(self, pool_id: str) -> bool:
        """Whether a region is mapped at its agent for this worker.

        True while an access is cached, and also after a map reply for an
        own region that gave no access (NIXL refused its metadata) as long
        as that reply's connection is open: the agent then still holds the
        region, so it needs a release before it goes back. A map that got
        no reply needs none; its connection is closed, and the agent
        releases what a closed connection held.
        """
        with self._lock:
            if pool_id in self._access:
                return True
            held = self._attempted.get(pool_id)
            client = self._agents.get(held[0]) if held else None
        return client is not None and client.generation == held[1]

    def forget(self, pool_id: str) -> None:
        """Drop a region's cached access; the next use maps it again."""
        with self._lock:
            self._access.pop(pool_id, None)

    def boots(self, addresses: list[str]) -> dict[str, str]:
        """Ask remote CXL agents this worker talks to for their node's boot id.

        A connection found broken is opened again, which also tells the
        caller's cached accesses through that agent to map again.

        Args:
            addresses: Remote CXL agent addresses.

        Returns:
            ``address -> boot id`` for the remote CXL agents that answered.
        """
        from maru_remote.region_map import RegionMapError, RegionMapUnavailable

        out = {}
        for address in addresses:
            with self._lock:
                client = self._agents.get(address)
            if client is None or self.is_down(address):
                continue
            try:
                out[address] = str(client.hello().get("boot", ""))
            except RegionMapUnavailable:
                self._mark_down(address)
                continue
            except RegionMapError:
                continue
            with self._lock:
                self._unanswered.discard(address)
        return out

    def release(self, pool_id: str, handle: dict, agents: list[str]) -> Release:
        """Have the region's agent deregister a region this worker owns.

        Args:
            pool_id: The region's pool id at MaruServer.
            handle: ``MaruHandle.to_dict()`` of the region.
            agents: Remote CXL agent addresses of the region's pool.

        Returns:
            ``Release.DONE`` once the region is not registered at its agent:
            released now, never mapped there, or that agent maps another
            region under this id. ``Release.FAILED`` if the agent answered
            that it could not deregister the region. ``Release.NO_REPLY`` if
            the agent did not answer; it then counts as down.
        """
        from maru_remote.region_map import RegionMapError, RegionMapUnavailable

        self.forget(pool_id)
        if not agents:
            return Release.FAILED
        address = pick_agent(agents, int(handle["region_id"]))
        try:
            self._client(address).release(handle)
        except RegionMapError as exc:
            if exc.code != "denied":
                logger.error(
                    "release of region %s failed: %s", handle["region_id"], exc
                )
                return Release.FAILED
            # denied: the agent's region under this id is not ours
        except RegionMapUnavailable as exc:
            self._mark_down(address)
            logger.error(
                "release of region %s not confirmed: %s", handle["region_id"], exc
            )
            return Release.NO_REPLY
        with self._lock:
            self._unanswered.discard(address)
            self._replied.discard(pool_id)
        self._not_mapped(pool_id)
        return Release.DONE

    def unmap(self, pool_id: str, handle: dict, agents: list[str]) -> None:
        """Drop this worker's hold on a region at its remote CXL agent.

        The region stays registered while someone else holds it. A broken
        connection already dropped the hold.

        Args:
            pool_id: The region's pool id at MaruServer.
            handle: The region's handle or read handle.
            agents: Remote CXL agent addresses of the region's pool.
        """
        from maru_remote.region_map import RegionMapError

        self.forget(pool_id)
        self._not_mapped(pool_id)
        if not agents:
            return
        address = pick_agent(agents, int(handle["region_id"]))
        with self._lock:
            client = self._agents.get(address)
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
        """Close remote CXL agent connections, the NIXL agent and the staging.

        If the NIXL agent cannot be cleaned up, it and the staging buffer stay,
        and a later call tries again.
        """
        self._started = False
        with self._lock:
            clients = list(self._agents.values())
            self._agents.clear()
            self._access.clear()
            self._loaded.clear()
            self._unready.clear()
        for client in clients:
            client.close()  # releases every mapping the connection held
        if self.transport is not None:
            self.reap_probes()
            try:
                self.transport.close()
            except Exception:
                # NIXL may still have the staging buffer registered with the
                # NIC: keep both, so a later close retries (NixlTransport.close).
                logger.warning(
                    "RDMA transport cleanup failed; keeping the staging buffer",
                    exc_info=True,
                )
                return
            self.transport = None
        if self._probe is not None:
            self._probe.close()
            self._probe = None
        if self.staging is not None:
            self.staging.close()
            self.staging = None

    def _default_agent(self, address: str) -> Any:
        from maru_remote.region_map import RegionMapClient

        return RegionMapClient(
            address,
            timeout_s=max(self.config.rdma_timeout_s, REPLY_TIMEOUT_MIN_S),
            connect_timeout_s=CONNECT_TIMEOUT_S,
            key=self._key,
        )

    def _client(self, address: str) -> Any:
        with self._lock:
            client = self._agents.get(address)
            if client is None:
                client = self._agents[address] = self._agent_factory(address)
            return client

    def _mark_down(self, address: str) -> None:
        with self._lock:
            self._down[address] = time.monotonic() + DOWN_BACKOFF_S
            self._unanswered.add(address)

    def _not_mapped(self, pool_id: str) -> None:
        with self._lock:
            self._attempted.pop(pool_id, None)
            now = time.monotonic()
            for stale in [p for p, t in self._map_failed.items() if t <= now]:
                del self._map_failed[stale]


def _default_transport(config: Any) -> Callable[[str], Any]:
    def make(name: str) -> Any:
        from maru_remote.native_agent import NativeNixlAgent
        from maru_remote.transport import NixlTransport

        agent = NativeNixlAgent(name, progress_thread=True, event_timeout_us=1000)
        params = {"ucx_devices": config.rdma_device} if config.rdma_device else {}
        agent.create_backend("UCX", params)
        return NixlTransport(name, agent=agent)

    return make
