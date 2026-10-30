# SPDX-License-Identifier: Apache-2.0
"""The remote CXL storage client: a storage session whose regions live in a memory
node's CXL, written and read with one-sided RDMA through a staging buffer."""

from __future__ import annotations

import logging
import socket
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict
from typing import Any

from maru_common.storage_types import (
    MAX_STORAGE_BATCH,
    REMOTE_CXL_STORAGE_CAPABILITY,
    STORAGE_CAPABILITY,
    StorageError,
    StorageLocation,
    StorageUnavailableError,
)

from ..cpu import CpuReadLease
from ..staging import StagingBuffer
from .datapath import REPLY_TIMEOUT_MIN_S, RdmaDataPath
from .hints import ReadHints
from .lifecycle import RegionLifecycle
from .region import (
    REMOTE_CXL,
    IsolatedTransfer,
    RegionAccess,
    RemoteCxlAllocation,
    RemoteCxlRegion,
)

logger = logging.getLogger(__name__)

# The refill thread gets room for stores ahead of them once the free pages of
# usable regions fall below this many (at most an eighth of a region).
ROOM_LOW_PAGES = 64
# How often a store that found no remote page says so
UNPLACED_WARN_INTERVAL_S = 60.0
# How often a closed client looks for its late transfers' end, and after how
# long it says that they have not ended
LATE_TRANSFER_POLL_S = 1.0
LATE_TRANSFER_WARN_S = 60.0
# How often the refill thread looks at the room without being woken
REFILL_CHECK_S = 1.0

# MaruServer refusals of ``open`` that time heals
TRANSIENT_OPEN_REFUSALS = (
    "reservation failed",
    "one active worker per engine_id",
    "session limit reached",
)


def wait_for_late_transfers(clients: list[RemoteCxlStorageClient]) -> None:
    """Block until no late RDMA can touch the closed ``clients``' memory.

    Runs on a thread of its own after close: the heartbeat thread that
    reaped released transfers is gone. A transfer NIXL never releases keeps
    it waiting, as its memory must stay registered.
    """
    started, warned = time.monotonic(), False
    while True:
        try:
            ended = [c.reap_unconfirmed() for c in clients]  # each one reaps
            if all(ended):
                return
        except Exception:
            logger.warning("Could not reap late RDMA transfers", exc_info=True)
        if not warned and time.monotonic() - started >= LATE_TRANSFER_WARN_S:
            warned = True
            logger.warning(
                "RDMA transfers are still unconfirmed %d s after close; the "
                "staging buffer stays registered until they end",
                LATE_TRANSFER_WARN_S,
            )
        time.sleep(LATE_TRANSFER_POLL_S)


class RemoteCxlStorageClient(RegionLifecycle):
    """Storage-session client whose regions are reached over RDMA."""

    def __init__(
        self,
        config: Any,
        rpc: Any,
        *,
        data: RdmaDataPath | None = None,
        transport_factory: Callable[[str], Any] | None = None,
        agent_factory: Callable[[str], Any] | None = None,
    ) -> None:
        """Create a client; :meth:`connect` opens the session.

        Args:
            config: ``MaruConfig`` of the worker (``storage_backend="remote_cxl"``).
            rpc: MaruServer RPC client.
            data: Data path shared with other pools (``PoolRouter``); a new
                one is made and owned when omitted.
            transport_factory: NIXL transport factory for a new data path.
            agent_factory: Remote CXL agent client factory for a new data path.
        """
        self.config = config
        self.rpc = rpc
        self.connected = False
        # True once connected: a client that lost its session afterwards keeps
        # the cache off until the worker restarts, as storage sessions do.
        self.ever_connected = False
        self.regions: dict[str, RemoteCxlRegion] = {}
        self._owns_data = data is None
        self._data = data or RdmaDataPath(config, transport_factory, agent_factory)
        self._credentials: dict = {}
        self._agents: list[str] = []
        self._session_ttl = 30.0
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._reads = 0
        self._sequence = 0
        self._sequence_lock = threading.Lock()
        self._keys: dict[str, StorageLocation] = {}
        self._pending: dict[str, tuple[list[str], list[RemoteCxlAllocation]]] = {}
        # Victims whose remove reply was lost: their pages wait for a retry.
        self._removing: list[dict] = []
        # op_id of an expand whose reply was lost, resent by the next attempt
        self._expand_op: str | None = None
        # Keys that must leave MaruServer (region unreachable or its memory
        # node rebooted), retried while a reader's lease keeps them
        self._dropping: list[dict] = []
        # Regions whose memory node rebooted and that could not go back yet
        self._unreturned: list[RemoteCxlRegion] = []
        # Read leases whose release was not confirmed: MaruServer keeps a
        # lease while the session lives, and a leased key cannot be evicted
        self._unreleased: list[str] = []
        self._isolated: list[IsolatedTransfer] = []
        self._staging_warned = False
        # RDMA transfers run without the lock; close waits for them to end.
        self._transfers = 0
        self._transfers_idle = threading.Condition(self._lock)
        # A nonblocking alloc takes only a staging slot; the store places its
        # page, and the refill thread expands or evicts ahead when this is set.
        self._nonblocking = bool(getattr(config, "rdma_nonblocking_alloc", False))
        self._refill = threading.Event()
        # Orders free and store of an allocation that has no page yet
        self._unplaced_lock = threading.Lock()
        self._unplaced_warned_at = float("-inf")
        # Read hints (hints.py): the memory side hears which bytes a load
        # will read before it starts. Off unless the config asks for them.
        self._hints = (
            ReadHints(self._hint_locate, self._hint_send)
            if getattr(config, "read_hints", False)
            else None
        )
        self._hint_agents: dict[str, Any] = {}
        self._hint_key: bytes | None = None

    # ------------------------------------------------------------ read hints

    @property
    def read_hints_enabled(self) -> bool:
        """Whether this backend sends read hints."""
        return self._hints is not None

    def hint_read(
        self, request_id: str, keys: list[str], route_key: str | None = None
    ) -> None:
        """Tell the memory side that ``request_id`` will read ``keys`` soon.

        Returns at once: the lookup of the leading hit keys and the messages
        to their remote CXL agents run on the hint thread. Without read
        hints this does nothing. A hint takes no lease; the load still
        leases and reads every key.
        """
        if self._hints is not None:
            self._hints.hint(request_id, keys)

    def end_read_hint(self, request_id: str) -> None:
        """``request_id`` ended: the memory side may drop what it prepared."""
        if self._hints is not None:
            self._hints.end(request_id)

    def _hint_locate(self, keys: list[str]) -> list[tuple[str, list[dict]]]:
        """The leading hit keys as ranges, per remote CXL agent that maps
        their regions, in key order (on the hint thread)."""
        from maru_remote.region_map import pick_agent

        # Safe beside the data path: the MaruServer RPC client serializes its
        # calls, and the session fields read here are replaced, never
        # changed in place. A probe refreshes the hit keys' last use, as a
        # lookup (batch_exists) does.
        self._ensure(data=False)
        by_agent: dict[str, list[dict]] = {}
        for i in range(0, len(keys), MAX_STORAGE_BATCH):
            batch = keys[i : i + MAX_STORAGE_BATCH]
            result = self._call(
                "probe",
                engine_id=self.config.engine_id,
                namespace=self.config.cache_namespace,
                keys=batch,
                locations=True,
            )
            entries = result.get("entries") or []
            regions = result.get("regions") or {}
            for entry in entries[: len(batch)]:
                if entry is None:
                    # The load reads the leading hits only.
                    return list(by_agent.items())
                handle = regions.get(entry["pool_id"])
                if handle is None or not self._agents:
                    return list(by_agent.items())
                agent = pick_agent(self._agents, int(handle["region_id"]))
                by_agent.setdefault(agent, []).append(
                    {
                        "handle": handle,
                        "offset": entry["offset"],
                        "length": entry["length"],
                    }
                )
            if len(entries) < len(batch):
                break
        return list(by_agent.items())

    def _hint_load(
        self, hits: list[tuple[int, StorageLocation]], regions: dict[str, dict]
    ) -> str:
        """Hint the bytes a load is about to READ; the hint's name, or "".

        With read hints on, a process that reads (a worker) hints each load
        as it starts, under a name of its own, and ends that hint once the
        load is over. Queued only: the READs do not wait for it.
        """
        if self._hints is None or not hits or not self._agents:
            return ""
        from maru_remote.region_map import pick_agent

        by_agent: dict[str, list[dict]] = {}
        for _, location in hits:
            own = self.regions.get(location.pool_id)
            handle = own.handle if own is not None else regions.get(location.pool_id)
            if handle is None:
                continue
            agent = pick_agent(self._agents, int(handle["region_id"]))
            by_agent.setdefault(agent, []).append(
                {"handle": handle, "offset": location.offset, "length": location.length}
            )
        if not by_agent:
            return ""
        name = f"maru-load-{uuid.uuid4().hex}"
        self._hints.hint_ranges(name, list(by_agent.items()))
        return name

    def _hint_send(self, agent: str, message: dict) -> bool:
        """Send a hint message on this process's own connection to ``agent``;
        False if the agent has no region extension that takes its action."""
        from maru_remote.region_map import RegionMapClient, load_key

        client = self._hint_agents.get(agent)
        if client is None:
            if self._hint_key is None:
                self._hint_key = load_key(self.config.remote_cxl_agent_key_file)
            client = self._hint_agents[agent] = RegionMapClient(
                agent,
                timeout_s=max(self.config.rdma_timeout_s, REPLY_TIMEOUT_MIN_S),
                key=self._hint_key,
            )
        if not client.epoch:
            client.hello()  # connects; a reconnect later renews the epoch
        if message["action"] not in client.hint_actions:
            return False
        client.extension({**message, "epoch": client.epoch})
        return True

    # ------------------------------------------------------------------ setup

    @property
    def remote_cxl_agents(self) -> list[str]:
        """Remote CXL agent addresses MaruServer gave in the handshake."""
        return list(self._agents)

    def connect(self) -> bool:
        """Handshake, open the session, map its first region, start renewing.

        Returns:
            True when connected.

        Raises:
            StorageError: if MaruServer refused the configuration.
            StorageUnavailableError: if MaruServer is unreachable or refused
                for a reason time heals; the connector tries again later.
        """
        with self._lock:
            if self._stop.is_set():
                raise RuntimeError("Create a fresh remote CXL handler after close")
            if self.connected:
                return True
            self.rpc.connect()
            self._credentials = {}  # a failed earlier attempt left its token
            refused = False
            try:
                self._handshake()
                if not self.config.metadata_only:
                    try:
                        grant = self._open_session()
                    except StorageError as exc:
                        refused = True  # MaruServer granted nothing
                        if any(r in str(exc) for r in TRANSIENT_OPEN_REFUSALS):
                            # Time heals it; the connector tries again later.
                            raise StorageUnavailableError(str(exc)) from exc
                        raise
                    self._data.start()
                    for info in grant["pools"]:
                        if info["medium"] == REMOTE_CXL:
                            self._add_region(info)
                    interval = self._session_ttl / 3
                    self._threads = [
                        threading.Thread(
                            target=self._heartbeat_loop,
                            args=(interval,),
                            daemon=True,
                            name="maru-remote-cxl-heartbeat",
                        ),
                        threading.Thread(
                            target=self._remap_loop,
                            args=(interval,),
                            daemon=True,
                            name="maru-remote-cxl-remap",
                        ),
                    ]
                if self._nonblocking and self._threads:
                    self._threads.append(
                        threading.Thread(
                            target=self._refill_loop,
                            daemon=True,
                            name="maru-remote-cxl-refill",
                        )
                    )
                self.connected = True
                self.ever_connected = True
                for thread in self._threads:
                    thread.start()
                return True
            except Exception:
                if self.regions:
                    self._return_regions()
                elif not refused:
                    # The open may have been granted: close it. After a
                    # refusal a close would only leave a tombstone behind.
                    self._close_session_quietly()
                self._stop_data_path()
                self.rpc.close()
                raise

    def _handshake(self) -> dict:
        handshake = self.rpc.handshake()
        if not handshake.get("success", True):
            raise StorageUnavailableError(handshake.get("error", "Handshake failed"))
        capabilities = handshake.get("capabilities", [])
        if STORAGE_CAPABILITY not in capabilities:
            raise StorageError("MaruServer does not support cpu_storage_v1")
        agents = [str(m) for m in handshake.get("remote_cxl_agents") or []]
        if not self.config.metadata_only:
            if REMOTE_CXL_STORAGE_CAPABILITY not in capabilities or not agents:
                raise StorageError(
                    "Remote CXL storage requires remote_cxl_storage_v1 and --remote-cxl-agent "
                    "on MaruServer"
                )
        self._agents = agents
        self._credentials = {
            "server_epoch": handshake["server_epoch"],
            "session_token": uuid.uuid4().hex,
        }
        return handshake

    @property
    def _staging(self) -> StagingBuffer | None:
        return self._data.staging

    @property
    def _transport(self) -> Any:
        return self._data.transport

    def _open_session(self) -> dict:
        """Open the session and get its first region (not mapped yet).

        Raises:
            StorageError: if MaruServer refused; :meth:`connect` turns a
                refusal that time heals (the resource manager is full, the
                session of this engine_id before a restart has not expired
                yet) into :class:`StorageUnavailableError`, which the
                connector retries as it retries a cxl Handler that cannot
                connect.
            StorageUnavailableError: if the outcome is unknown.
        """
        page = self.config.chunk_size_bytes
        size = self.config.pool_size // page * page
        grant = self._call(
            "open",
            engine_id=self.config.engine_id,
            namespace=self.config.cache_namespace,
            schema=self.config.storage_schema,
            node_id=self.config.node_id or socket.gethostname(),
            capacity=0,
            remote_cxl_region_size=size,
            page_size=page,
        )
        self._session_ttl = grant["session_ttl"]
        return grant

    def _call(self, action: str, **payload: Any) -> dict:
        return self.rpc.storage(action, {**self._credentials, **payload})

    def _ensure(self, data: bool = True) -> None:
        if not self.connected or self._stop.is_set():
            raise StorageUnavailableError("Remote CXL cache session is unavailable")
        if data and self.config.metadata_only:
            # A data client may hold no region for a while (its only region
            # went back after a reboot); its next store asks for one.
            raise RuntimeError("A metadata-only client cannot access RDMA buffers")

    def _stop_data_path(self) -> None:
        if self._owns_data:
            self._data.close()

    # -------------------------------------------------------------- lifetime

    def _heartbeat_loop(self, interval: float) -> None:
        """Renew the session and do housekeeping that takes no long waits."""
        while not self._stop.wait(interval):
            try:
                self._heartbeat()
            except StorageError as exc:
                # A new epoch or an expired session cannot be resumed with the
                # old regions: fail closed until the worker restarts.
                restarted = "epoch changed" in str(exc)
                self._disable(
                    "MaruServer restarted"
                    if restarted
                    else "Remote CXL cache session expired",
                    server_restarted=restarted,
                )
                return
            except Exception:
                # A timeout is neither proof of a lost session nor permission
                # to reuse memory; acquire revalidates everything.
                logger.warning("Remote CXL cache heartbeat failed", exc_info=True)
            # Housekeeping waits for a free moment; a transfer in progress
            # only delays it to the next beat.
            if self._lock.acquire(blocking=False):
                try:
                    self._resolve_pending()
                    self._retry_removes()
                    self._retry_drops()
                    self._retry_returns()
                    self._retry_lease_releases()
                    self._reap_isolated()
                except Exception:
                    logger.warning(
                        "Remote CXL cache housekeeping failed", exc_info=True
                    )
                finally:
                    self._lock.release()

    def _remap_loop(self, interval: float) -> None:
        """Map regions set aside and notice rebooted memory nodes.

        Kept off the heartbeat thread: talking to a memory node that is down
        waits for a connect timeout, which must not delay the renewal.
        """
        while not self._stop.wait(interval):
            if not self.connected:
                return
            try:
                self._retry_unreachable()
                self._check_boots()
            except Exception:
                logger.warning("Remote CXL region remap failed", exc_info=True)

    def _refill_loop(self) -> None:
        """Get room for stores before they need it.

        A store places its pages when it starts (:meth:`batch_store`), and
        asking MaruServer for a region, or evicting, waits on the network.
        This thread does it ahead, when the free pages run low, so a store
        rarely waits for it.
        """
        while not self._stop.is_set():
            self._refill.wait(REFILL_CHECK_S)
            self._refill.clear()
            if self._stop.is_set() or not self.connected:
                return
            try:
                with self._lock:
                    if self.connected:
                        self._make_room()
            except Exception:
                logger.warning("Remote CXL refill failed", exc_info=True)

    def _make_room(self) -> None:
        """Expand or evict until the usable regions have enough free pages.

        Called with the lock held.
        """
        for _ in range(8):
            if self._room() >= self._low_room():
                return
            if self._may_expand() and self._expand():
                continue
            if not self._evict():
                return

    def _room(self) -> int:
        return sum(r.free_count() for r in self.regions.values() if self._writable(r))

    def _low_room(self) -> int:
        """Free pages to keep: :data:`ROOM_LOW_PAGES`, at most an eighth of a
        region, and at least one."""
        page = self.config.chunk_size_bytes
        region_pages = max(1, self.config.pool_size // page)
        return max(1, min(ROOM_LOW_PAGES, region_pages // 8))

    def _heartbeat(self) -> None:
        """Renew the session without the client lock.

        Stores and loads hold the lock across a transfer that may take up to
        ``rdma_timeout_s``; the renewal must not wait for them, or one slow
        transfer would let the session expire.
        """
        with self._sequence_lock:
            self._sequence += 1
            sequence = self._sequence
        # The boot of each region's memory node goes along, so MaruServer
        # can tell a reboot after it takes a region over.
        reports = [
            {"pool_id": region.pool_id, **region.usage(), "boot": region.boot}
            for region in list(self.regions.values())
        ]
        self._call("heartbeat", sequence=sequence, pools=reports)

    def _disable(self, reason: str, server_restarted: bool = False) -> None:
        """Stop using the cache and leave the regions.

        After an expiry MaruServer has taken the regions over, so the worker
        only leaves them. A restarted MaruServer knows nothing of them; the
        worker then releases them itself before the resource manager
        reclaims them.
        """
        with self._lock:
            if not self.connected:
                return
            logger.warning("%s; restart the worker to re-enable the cache", reason)
            self.connected = False
            if server_restarted:
                self._return_regions()
            else:
                self._leave()

    # ------------------------------------------------------------ allocation

    def alloc(self, size: int, route_key: str | None = None) -> RemoteCxlAllocation:
        """A staging slot for ``size`` bytes and, with a blocking alloc, the
        remote page they go to.

        With ``rdma_nonblocking_alloc`` (the connector's engine thread) it
        takes only the slot and never waits for the client lock or the
        network: :meth:`batch_store` places the page, on the thread that
        stores.

        Args:
            size: Bytes to store, at most one page.
            route_key: Ignored (one pool); see ``PoolRouter``.

        Returns:
            The allocation; write into ``buf`` and pass it to
            :meth:`batch_store` or :meth:`free`.

        Raises:
            MemoryError: if no staging slot is free, or (blocking alloc) no
                page after trying to get a region and to evict.
            ValueError: if ``size`` does not fit in a page.
            StorageUnavailableError: if the session is unavailable.
        """
        if type(size) is not int or not 0 < size <= self.config.chunk_size_bytes:
            raise ValueError("Remote CXL allocation must fit in one page")
        if self._nonblocking:
            return self._take_slot(size)
        with self._lock:
            handle = self._take_slot(size)
            try:
                handle.region, handle.location = self._take_page(size)
            except BaseException:
                self._give_slot(handle)
                raise
            handle.region.track(handle)
            return handle

    def _take_slot(self, size: int) -> RemoteCxlAllocation:
        self._ensure()
        staging = self._staging
        if staging is None:
            raise StorageUnavailableError("RDMA staging buffer is not set up")
        slots = staging.take(1, keep=staging.load_reserve)
        if slots is None:
            raise MemoryError("RDMA staging buffer has no slot for stores")
        return RemoteCxlAllocation(
            staging.view(slots[0])[:size], None, slots[0], None, size, owner=self
        )

    def _take_page(self, size: int) -> tuple[RemoteCxlRegion, StorageLocation]:
        """A free page: from a region with room, a new region, or eviction.

        Only regions a WRITE can reach now are used: a WRITE to a region
        whose remote CXL agent has not answered since it last failed would
        fail after the transport's timeout and leave the page quarantined.
        Eviction likewise frees pages only in those regions (``_evict``).
        Called with the lock held.
        """
        for attempt in ("free", "expand", "evict"):
            if attempt == "expand" and not (self._may_expand() and self._expand()):
                continue
            if attempt == "evict" and not self._evict():
                continue
            for region in self.regions.values():
                if region.has_free() and self._writable(region):
                    return region, region.take_page(size)
        if self.regions and not any(self._writable(r) for r in self.regions.values()):
            raise MemoryError(
                "No remote CXL region is reachable now; skipping new cache admission"
            )
        raise MemoryError("Remote CXL regions are full; skipping new cache admission")

    def _place(self, handles: list[RemoteCxlAllocation]) -> list[bool]:
        """Give each allocation without a page one; per allocation, whether
        it has a page now. One that gets none returns its slot.

        Called with the lock held, by the thread that stores.
        """
        placed = []
        error: Exception | None = None
        for handle in handles:
            if not handle.placed and error is None:
                try:
                    region, location = self._take_page(handle.length)
                    handle.region, handle.location = region, location
                    region.track(handle)
                except (MemoryError, StorageUnavailableError) as exc:
                    error = exc  # the later ones would not find a page either
                except Exception as exc:
                    # A malformed MaruServer reply while expanding or evicting
                    logger.error("Remote CXL page placement failed", exc_info=True)
                    error = exc
            if not handle.placed:
                self._give_slot(handle)
                handle.state = "freed"
            placed.append(handle.placed)
        if error is not None:
            now = time.monotonic()
            if now - self._unplaced_warned_at >= UNPLACED_WARN_INTERVAL_S:
                self._unplaced_warned_at = now
                logger.warning(
                    "%s (%d of %d chunks not stored); further ones are not "
                    "logged for %d s",
                    error,
                    placed.count(False),
                    len(handles),
                    UNPLACED_WARN_INTERVAL_S,
                )
        if self._nonblocking and self._room() < self._low_room():
            self._refill.set()
        return placed

    def free(self, handle: RemoteCxlAllocation) -> None:
        """Give back an allocation that was not stored.

        A committed allocation or one a transfer may still touch is left
        alone; the connector's cleanup must not reuse it. An allocation
        without a page gives back its slot without the client lock.
        """
        if not isinstance(handle, RemoteCxlAllocation):
            raise ValueError("Allocation does not belong to this remote CXL client")
        with self._unplaced_lock:
            if handle.state != "writing":
                return  # submitted: the store gives it back
            if not handle.placed:
                self._give_slot(handle)
                handle.state = "freed"
                return
        with self._lock:
            self._free_locked(handle)

    def _free_locked(self, handle: RemoteCxlAllocation) -> None:
        if handle.state != "writing":
            return
        region = handle.region
        if region.retired or self.regions.get(region.pool_id) is not region:
            # The region left with a closed session: only the slot returns.
            self._give_slot(handle)
            handle.state = "freed"
            return
        region.validate(handle)
        region.untrack(handle)
        region.give_page(handle.location)
        self._give_slot(handle)
        handle.state = "freed"

    def _give_slot(self, handle: RemoteCxlAllocation) -> None:
        try:
            handle.buf.release()
        except BufferError:
            pass
        if self._staging is not None:
            self._staging.give(handle.slot)

    def retrieve_capacity(self) -> int | None:
        """How many keys the next :meth:`batch_retrieve` should ask for at most.

        Stores leave the load reserve free, so a batch of this size finds its
        slots while loads run one at a time. None before the staging buffer
        exists.
        """
        staging = self._staging
        return None if staging is None else staging.load_capacity()

    def storage_gpu_accessible(self) -> bool:
        """Whether the staging buffer is page-locked for GPU copy kernels."""
        staging = self._staging
        return staging is not None and staging.cuda_registered

    def has_local(self, key: str) -> bool:
        """Whether this session committed ``key`` and still holds it."""
        # Read without the lock: the engine thread asks, and a dict lookup
        # sees either the old or the new state.
        return self.connected and not self._stop.is_set() and key in self._keys

    # ----------------------------------------------------------------- store

    def batch_store(
        self, keys: list[str], handles: list[RemoteCxlAllocation]
    ) -> list[bool]:
        """Write the staged bytes to their remote pages, then commit the keys.

        Args:
            keys: Chunk keys.
            handles: Allocations from :meth:`alloc`, one per key.

        Returns:
            Per key, whether it is stored (by this call or already present).
        """
        if len(keys) != len(handles):
            raise ValueError("keys and handles must have the same length")
        if len(keys) > MAX_STORAGE_BATCH:
            out = []
            for i in range(0, len(keys), MAX_STORAGE_BATCH):
                out.extend(
                    self.batch_store(
                        keys[i : i + MAX_STORAGE_BATCH],
                        handles[i : i + MAX_STORAGE_BATCH],
                    )
                )
            return out
        if not keys:
            return []
        with self._lock:
            self._ensure()
            if len({id(h) for h in handles}) != len(handles):
                raise ValueError("A remote CXL allocation can only be committed once")
            for key, handle in zip(keys, handles, strict=True):
                if not isinstance(key, str) or not key or len(key) > 4096:
                    raise ValueError("Invalid remote CXL key")
                if not isinstance(handle, RemoteCxlAllocation):
                    raise ValueError("Allocation does not belong to this client")
                if handle.placed:
                    handle.region.validate(handle)
            with self._unplaced_lock:
                if any(h.state != "writing" for h in handles):
                    raise ValueError("Remote CXL allocation was already submitted")
                for handle in handles:
                    handle.state = "sending"  # ``free`` leaves it alone now
            try:
                placed = self._place(handles)
            except BaseException:
                # The caller can no longer free them: pages and slots go back.
                self._give_back(
                    [h for h in handles if h.placed and h.state == "sending"]
                )
                for h in handles:
                    if h.state == "sending":
                        self._give_slot(h)
                        h.state = "freed"
                raise
            if not any(placed):
                return [False] * len(keys)
            self._transfers += 1
        sent = [h for h, ok in zip(handles, placed, strict=True) if ok]
        try:
            written = self._write(sent)
        except BaseException:
            with self._lock:
                # The caller can no longer free them: give back what is
                # still being sent (isolated pages stay out of use).
                self._give_back([h for h in sent if h.state == "sending"])
                self._end_transfer_locked()
            raise
        with self._lock:
            try:
                if not written:
                    stored = [False] * len(sent)
                else:
                    sent_keys = [k for k, ok in zip(keys, placed, strict=True) if ok]
                    stored = self._commit(sent_keys, sent)
            finally:
                # close waits for this: the commit must not land after it
                self._end_transfer_locked()
        results = iter(stored)
        return [next(results) if ok else False for ok in placed]

    def _end_transfer_locked(self) -> None:
        self._transfers -= 1
        self._transfers_idle.notify_all()

    def _commit(
        self, keys: list[str], handles: list[RemoteCxlAllocation]
    ) -> list[bool]:
        op_id = uuid.uuid4().hex
        self._pending[op_id] = (keys[:], handles[:])
        for handle in handles:
            handle.state = "committing"
        entries = [
            {"key": key, "location": asdict(h.location)}
            for key, h in zip(keys, handles, strict=True)
        ]
        try:
            result = self._call("commit", op_id=op_id, entries=entries)
        except StorageError:
            self._finish_commit(op_id, ["REJECTED"] * len(keys))
            return [False] * len(keys)
        except Exception:
            logger.warning("RDMA commit outcome unknown; retaining pages")
            return [False] * len(keys)
        return self._finish_commit(op_id, result.get("statuses", []))

    def _write(self, handles: list[RemoteCxlAllocation]) -> bool:
        """Move staged bytes to their remote pages; False if not confirmed.

        Pages are written region by region. If any region fails, nothing is
        committed: written pages and their slots go back, unconfirmed ones
        are isolated. Runs without the client lock, which only the
        bookkeeping takes: a transfer can wait up to ``rdma_timeout_s``.
        Another thread may give the region back meanwhile; it releases the
        region at its agent first, so a WRITE still on its way fails at the
        NIC, and the commit drops a key whose region went back.
        """
        groups: dict[str, list[RemoteCxlAllocation]] = {}
        for h in handles:
            groups.setdefault(h.region.pool_id, []).append(h)
        written: list[RemoteCxlAllocation] = []
        ok = True
        for group in groups.values():
            if ok and self._write_group(group):
                written.extend(group)
                continue
            if ok:
                ok = False  # _write_group gave back or isolated the group
            else:
                with self._lock:
                    self._give_back(group)
        with self._lock:
            if not ok:
                self._give_back(written)
                return False
            for h in handles:
                self._give_slot(h)
            return True

    def _map_for_write(
        self, region: RemoteCxlRegion, group: list[RemoteCxlAllocation]
    ) -> RegionAccess | None:
        """Map an own region for a WRITE and check its memory node's boot.

        Returns None, with the group's pages given back, if the region
        cannot be mapped (it is set aside) or went back.
        """
        try:
            # Mapping may ask the region's agent: outside the lock.
            access = self._region(region.pool_id, region.handle, own=True)
        except StorageUnavailableError as exc:
            logger.warning(
                "Remote CXL region %d cannot be mapped: %s", region.region_id, exc
            )
            with self._lock:
                self._give_back(group)
                self._set_aside(region)
            return None
        with self._lock:
            if region.retired:
                # It went back while this map ran: undo the registration, as
                # the retry of set-aside regions does.
                region.mapped = True
                self._data.release(region.pool_id, region.handle, self._agents)
                self._give_back(group)
                return None
            self._adopt(region, access)
            if region.retired:
                # Its memory node rebooted and the region went back.
                self._give_back(group)
                return None
        return access

    def _write_group(self, group: list[RemoteCxlAllocation]) -> bool:
        """WRITE the pages of one region; on failure they are given back or
        isolated, and False is returned."""
        from maru_remote.transport import TransferTimeout, TransferUncertainError

        region = group[0].region
        with self._lock:
            if region.retired:
                # It went back after the pages were taken (memory node rebooted).
                self._give_back(group)
                return False
        access = self._map_for_write(region, group)
        if access is None:
            return False
        with self._lock:
            stale = not self._data.has_access(region.pool_id)
        if stale:
            # Another transfer's failure dropped the connection meanwhile. A
            # restarted agent drops it too, so the new map's boot is checked.
            access = self._map_for_write(region, group)
            if access is None:
                return False
        with self._lock:
            if region.retired:
                self._give_back(group)
                return False
            pairs = [
                (
                    self._staging.addr(h.slot),
                    access.base_addr + h.location.offset,
                    h.location.length,
                )
                for h in group
            ]
        try:
            self._transport.write(
                access.peer, pairs, timeout_s=self.config.rdma_timeout_s
            )
        except (TransferTimeout, TransferUncertainError) as exc:
            # The NIC may still write: keep pages and slots until it ends.
            self._isolate_write(region, group, exc.pending)
            logger.warning("RDMA WRITE not confirmed: %s", exc)
            return False
        except Exception as exc:
            logger.warning("RDMA WRITE failed: %s", exc)
            with self._lock:
                # The memory node's NIC still accepts the region's key, and a
                # WRITE that ended in an error at this end may still be in the
                # network. Its pages stay out of use while this worker owns the
                # region; the release before the region goes back ends any late
                # WRITE. The slots are safe once this end's connection is torn
                # down, so the connection goes before the slots.
                for h in group:
                    region.untrack(h)
                    region.quarantine(h.location)
                    h.state = "freed"
                self._data.disconnect({access.peer})
                for h in group:
                    self._give_slot(h)
            # A node that cannot be reached any more has the region set aside
            # now; its keys stop being reported as hits if it stays so.
            try:
                access = self._region(region.pool_id, region.handle, own=True)
            except StorageUnavailableError as map_exc:
                logger.warning(
                    "Remote CXL region %d cannot be mapped: %s",
                    region.region_id,
                    map_exc,
                )
                with self._lock:
                    self._set_aside(region)
            else:
                with self._lock:
                    if not region.retired:
                        self._adopt(region, access)
            return False
        except BaseException as exc:
            pending = getattr(exc, "pending_transfer", None)
            if pending is None:
                # Interrupted before NIXL had a transfer: nothing was sent.
                with self._lock:
                    self._give_back(group)
            else:
                # Interrupted while the WRITE may run: keep pages and slots
                # out of use until it ends (the caller gives back only what
                # is still being sent).
                self._isolate_write(region, group, pending)
            raise
        return True

    def _isolate_write(
        self, region: RemoteCxlRegion, group: list[RemoteCxlAllocation], pending: Any
    ) -> None:
        """Keep the pages and slots of a WRITE that may still run out of use;
        they come back once its NIXL transfer (``pending``) ends."""
        with self._lock:
            for h in group:
                region.untrack(h)
                region.quarantine(h.location)
                h.state = "isolated"
            self._isolated.append(
                IsolatedTransfer(
                    pending,
                    "WRITE",
                    pending.peer,
                    region,
                    [h.location for h in group],
                    [h.slot for h in group],
                )
            )

    def _give_back(self, handles: list[RemoteCxlAllocation]) -> None:
        """Pages written but not committed (or never written) return."""
        for h in handles:
            h.region.untrack(h)
            h.region.give_page(h.location)
            self._give_slot(h)
            h.state = "freed"

    def _finish_commit(self, op_id: str, statuses: list[str]) -> list[bool]:
        keys, handles = self._pending[op_id]
        if len(statuses) != len(keys) or any(
            s not in {"CREATED", "ALREADY_PRESENT", "REJECTED"} for s in statuses
        ):
            return [False] * len(keys)  # retain uncertain ownership
        for key, handle, status in zip(keys, handles, statuses, strict=True):
            handle.region.untrack(handle)
            if status == "CREATED" and handle.region.retired:
                # Its region went back (memory node rebooted): MaruServer
                # dropped the key with it.
                handle.state = "freed"
            elif status == "CREATED":
                handle.state = "ready"
                self._keys[key] = handle.location
            else:
                handle.state = "freed"
                handle.region.give_page(handle.location)
        del self._pending[op_id]
        return [s != "REJECTED" for s in statuses]

    def _resolve_pending(self) -> None:
        for op_id in list(self._pending)[:32]:
            result = self._call("resolve", op_id=op_id)
            if result.get("state") == "COMMITTED":
                self._finish_commit(op_id, result["statuses"])
            elif result.get("state") == "UNKNOWN":
                keys, handles = self._pending[op_id]
                entries = [
                    {"key": key, "location": asdict(handle.location)}
                    for key, handle in zip(keys, handles, strict=True)
                ]
                result = self._call("commit", op_id=op_id, entries=entries)
                self._finish_commit(op_id, result.get("statuses", []))

    def has_unconfirmed_transfers(self) -> bool:
        """Whether a late RDMA may still touch this client's memory."""
        with self._lock:
            return (
                bool(self._isolated)
                or self._transfers > 0
                or self._data.has_unconfirmed_probes()
            )

    def reap_unconfirmed(self) -> bool:
        """Give back what the transfers NIXL has released held.

        Returns:
            True when no late RDMA can touch this client's memory any more.
        """
        with self._lock:
            self._reap_isolated()
            probes_ended = self._data.reap_probes()
            return not self._isolated and not self._transfers and probes_ended

    def _stop_data_path_later(self) -> None:
        """After close: stop the data path once the late transfers ended."""
        wait_for_late_transfers([self])
        with self._lock:
            try:
                self._stop_data_path()
            except Exception:
                logger.warning("Could not stop the RDMA data path", exc_info=True)

    # ------------------------------------------------------------------ load

    def batch_exists(self, keys: list[str], route_key: str | None = None) -> list[bool]:
        """Whether each key is stored in this pool (any worker's region).

        Args:
            keys: Chunk keys.
            route_key: Ignored (one pool).

        Returns:
            Per key, True on a hit; False on a miss or if MaruServer did not
            answer.
        """
        self._ensure(data=False)
        out = []
        for i in range(0, len(keys), MAX_STORAGE_BATCH):
            batch = keys[i : i + MAX_STORAGE_BATCH]
            try:
                result = self._call(
                    "probe",
                    engine_id=self.config.engine_id,
                    namespace=self.config.cache_namespace,
                    keys=batch,
                )
                values = result.get("results", [])
                out.extend(
                    values if len(values) == len(batch) else [False] * len(batch)
                )
            except Exception:
                out.extend([False] * len(batch))
        return out

    def batch_retrieve(
        self, keys: list[str], route_key: str | None = None
    ) -> list[CpuReadLease | None]:
        """READ stored keys into staging slots under a server lease.

        Args:
            keys: Chunk keys.
            route_key: Ignored (one pool).

        Returns:
            Per key, a read lease on the bytes, or None on a miss. Bytes are
            given only if the server lease held for the whole read.
        """
        if len(keys) > MAX_STORAGE_BATCH:
            out = []
            try:
                for i in range(0, len(keys), MAX_STORAGE_BATCH):
                    out.extend(self.batch_retrieve(keys[i : i + MAX_STORAGE_BATCH]))
            except Exception:
                for info in out:
                    if info is not None:
                        info.release()
                raise
            return out
        with self._lock:
            self._ensure()
            requested = uuid.uuid4().hex
            try:
                result = self._call("acquire", keys=keys, lease_id=requested)
            except StorageUnavailableError:
                # The server may have granted it: release so it pins nothing.
                self._release_lease(requested)
                raise
            lease_id = result.get("lease_id")
            entries = result.get("entries", [])
            if len(entries) != len(keys):
                if lease_id:
                    self._release_lease(lease_id)
                raise StorageUnavailableError("Malformed remote CXL acquire response")
            hits = [
                (i, StorageLocation(**entry))
                for i, entry in enumerate(entries)
                if entry is not None
            ]
            if not hits:
                return [None] * len(keys)
            slots = self._take_read_slots(len(hits))
            hits = hits[: len(slots)]
            self._transfers += 1
        load_hint = self._hint_load(hits, result.get("regions", {}))
        try:
            got = self._read(
                hits,
                slots,
                result.get("regions", {}),
                result.get("region_boots", {}),
            )
        except BaseException:
            # _read hands every slot back or isolates it before it
            # returns; what escapes it (an interrupt) may leave a READ
            # running, so its slots stay taken.
            with self._lock:
                try:
                    if lease_id:
                        self._release_lease(lease_id)
                finally:
                    self._end_transfer_locked()
            raise
        finally:
            if load_hint:
                self._hints.end(load_hint)
        with self._lock:
            try:
                return self._finish_read(keys, hits, slots, got, lease_id)
            finally:
                # close waits for this: the staging must outlive the leases
                self._end_transfer_locked()

    def _finish_read(
        self,
        keys: list[str],
        hits: list[tuple[int, StorageLocation]],
        slots: list[int],
        got: bool,
        lease_id: str | None,
    ) -> list[CpuReadLease | None]:
        """Hand out the bytes read if the server lease held; lock held."""
        held = bool(lease_id) and self._release_lease(lease_id)
        out: list[CpuReadLease | None] = [None] * len(keys)
        unused = set(slots)
        for (i, location), slot in zip(hits, slots, strict=False):
            # close() may have begun while the READ ran without the lock.
            # It waits for this transfer and then tears the staging down, so
            # do not hand out a new lease after its initial live-lease check.
            if not (got and held and not self._stop.is_set()):
                continue
            unused.discard(slot)
            view = self._staging.view(slot)[: location.length]
            out[i] = CpuReadLease(view, location, self._end_read(slot))
            self._reads += 1
        if got:
            for slot in unused:
                self._staging.give(slot)
        return out

    def _take_read_slots(self, hits: int) -> list[int]:
        """Staging slots for up to ``hits`` reads; the rest load as misses."""
        want = min(hits, self._staging.free_count())
        slots = (self._staging.take(want) or []) if want else []
        if len(slots) < hits and not self._staging_warned:
            self._staging_warned = True
            logger.warning(
                "RDMA staging buffer has %d free chunk slots for %d hits; "
                "the rest load as misses. Raise maru_rdma_staging_size.",
                len(slots),
                hits,
            )
        return slots

    def _read(
        self,
        hits: list[tuple[int, StorageLocation]],
        slots: list[int],
        regions: dict[str, dict],
        region_boots: dict[str, str],
    ) -> bool:
        """READ each hit into its slot; False if not confirmed for all.

        On False the slots are back in the staging buffer, or isolated if a
        READ may still land in them. Errors end in False; only an interrupt
        raises, and then the slots stay taken if NIXL had created the
        transfer, and go back otherwise. Runs without the client lock;
        the server lease keeps the keys (and so their pages) from being
        evicted until the read is over.
        """
        from maru_remote.transport import TransferTimeout, TransferUncertainError

        by_peer: dict[str, list[tuple[int, int, int]]] = {}
        slots_by_peer: dict[str, list[int]] = {}
        try:
            for (_, location), slot in zip(hits, slots, strict=False):
                # An own region is mapped with its handle; another session's
                # with the read handle MaruServer gave with the lease.
                own = self.regions.get(location.pool_id)
                handle = (
                    own.handle if own is not None else regions.get(location.pool_id)
                )
                if handle is None:
                    raise StorageUnavailableError("No region for a leased location")
                if not 0 < location.length <= self._staging.slot_bytes:
                    # A READ longer than its slot would land in the next one.
                    raise StorageUnavailableError(
                        f"A leased location of {location.length} bytes does not "
                        f"fit a {self._staging.slot_bytes}-byte staging slot"
                    )
                access = self._region(location.pool_id, handle)
                expected_boot = region_boots.get(location.pool_id)
                if not expected_boot or access.boot != expected_boot:
                    raise StorageUnavailableError(
                        "A leased region's memory-node boot is unknown or changed"
                    )
                if location.offset + location.length > access.length:
                    raise StorageUnavailableError(
                        "A leased location ends past its region"
                    )
                by_peer.setdefault(access.peer, []).append(
                    (
                        self._staging.addr(slot),
                        access.base_addr + location.offset,
                        location.length,
                    )
                )
                slots_by_peer.setdefault(access.peer, []).append(slot)
        except Exception as exc:
            # A region that cannot be mapped (node down, or its owner
            # released it) loads as a miss.
            logger.warning("RDMA READ skipped: %s", exc)
            for slot in slots:
                self._staging.give(slot)
            return False
        for peer, pairs in by_peer.items():
            try:
                self._transport.read(peer, pairs, timeout_s=self.config.rdma_timeout_s)
            except (TransferTimeout, TransferUncertainError) as exc:
                # A late READ only lands in these slots: isolate them.
                peer_slots = slots_by_peer[peer]
                with self._lock:
                    self._isolated.append(
                        IsolatedTransfer(
                            exc.pending, "READ", exc.pending.peer, None, [], peer_slots
                        )
                    )
                # Earlier peers completed; later peers have not been posted.
                held = set(peer_slots)
                for slot in slots:
                    if slot not in held:
                        self._staging.give(slot)
                logger.warning("RDMA READ not confirmed: %s", exc)
                return False
            except Exception as exc:
                # NIXL released the failed transfer: nothing lands any more.
                logger.warning("RDMA READ failed: %s", exc)
                for slot in slots:
                    self._staging.give(slot)
                try:
                    self._data.disconnect({peer})
                except Exception:
                    logger.warning(
                        "closing the connection to %s failed", peer, exc_info=True
                    )
                return False
            except BaseException as exc:
                pending = getattr(exc, "pending_transfer", None)
                if pending is None:
                    # Interrupted before NIXL had a transfer: nothing lands.
                    for slot in slots:
                        self._staging.give(slot)
                else:
                    # Interrupted while the READ may run: its slots stay taken
                    # until NIXL releases the transfer.
                    peer_slots = slots_by_peer[peer]
                    with self._lock:
                        self._isolated.append(
                            IsolatedTransfer(
                                pending, "READ", pending.peer, None, [], peer_slots
                            )
                        )
                    held = set(peer_slots)
                    for slot in slots:
                        if slot not in held:
                            self._staging.give(slot)
                raise
        return True

    def _release_lease(self, lease_id: str) -> bool:
        """Release a server lease; True only if it held for the whole read.

        A release that cannot be confirmed is sent again by the housekeeping
        (:meth:`_retry_lease_releases`); MaruServer answers a release of a
        lease it no longer has.
        """
        try:
            return bool(self._call("release", lease_id=lease_id).get("held"))
        except Exception:
            logger.warning("RDMA read lease release could not be confirmed")
            self._unreleased.append(lease_id)
            # MaruServer refuses leases past 4096 per session anyway.
            del self._unreleased[:-4096]
            return False

    def _retry_lease_releases(self) -> None:
        """Send the unconfirmed lease releases again until one is answered."""
        pending, self._unreleased = self._unreleased, []
        for i, lease_id in enumerate(pending):
            try:
                self._call("release", lease_id=lease_id)
            except StorageError:
                # Refused: the session is gone (MaruServer restarted), and
                # with it the lease.
                continue
            except Exception:
                self._unreleased.extend(pending[i:])
                return

    def _end_read(self, slot: int) -> Callable[[], None]:
        def release() -> None:
            with self._lock:
                self._reads -= 1
                if self._staging is not None:
                    self._staging.give(slot)

        return release

    # ----------------------------------------------------------------- close

    def close(self) -> None:
        """Hand the regions over to MaruServer and close the session.

        Raises:
            RuntimeError: if a read lease given to the connector is still
                held.
        """
        with self._lock:
            if self._reads:
                raise RuntimeError(
                    "Release RDMA read leases before closing the handler"
                )
            self._stop.set()
            self._refill.set()  # the refill thread sees the stop at once
        for thread in self._threads:
            thread.join()
        self._threads = []
        # The hint thread owns its agent connections until it ended.
        if self._hints is None or self._hints.close():
            for agent in self._hint_agents.values():
                agent.close()
            self._hint_agents.clear()
        with self._lock:
            # Transfers run without the lock: let those under way end (each
            # within its timeout) before the regions and the data path go.
            self._transfers_idle.wait_for(
                lambda: not self._transfers, self.config.rdma_timeout_s + 5
            )
            if self.connected or self.regions:
                # A disabled session already left its regions.
                self._leave()
            if not self.has_unconfirmed_transfers():
                self._stop_data_path()
            else:
                logger.warning(
                    "RDMA transfers still own staging memory; it stays "
                    "registered until they end"
                )
                if self._owns_data:
                    # The heartbeat thread that reaped them is gone.
                    threading.Thread(
                        target=self._stop_data_path_later,
                        daemon=True,
                        name="maru-remote-cxl-late-transfers",
                    ).start()
            self._keys.clear()
            self._pending.clear()
            self.connected = False
            self.rpc.close()
