# SPDX-License-Identifier: Apache-2.0
"""The remote CXL storage client: a storage session whose regions live in a memory
node's CXL, written and read with one-sided RDMA through a staging buffer."""

from __future__ import annotations

import logging
import socket
import threading
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
from .datapath import RdmaDataPath
from .lifecycle import RegionLifecycle
from .region import REMOTE_CXL, IsolatedTransfer, RemoteCxlAllocation, RemoteCxlRegion

logger = logging.getLogger(__name__)

# MaruServer refusals of ``open`` that time heals
TRANSIENT_OPEN_REFUSALS = (
    "reservation failed",
    "one active worker per engine_id",
    "session limit reached",
)


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
        """A staging slot and a remote page for ``size`` bytes.

        Args:
            size: Bytes to store, at most one page.
            route_key: Ignored (one pool); see ``PoolRouter``.

        Returns:
            The allocation; write into ``buf`` and pass it to
            :meth:`batch_store` or :meth:`free`.

        Raises:
            MemoryError: if no staging slot or page is free after trying to
                get a region and to evict.
            StorageUnavailableError: if the session is unavailable.
        """
        with self._lock:
            self._ensure()
            staging = self._staging
            if staging is None:
                raise StorageUnavailableError("RDMA staging buffer is not set up")
            slots = staging.take(1, keep=staging.load_reserve)
            if slots is None:
                raise MemoryError("RDMA staging buffer has no slot for stores")
            try:
                region, location = self._take_page(size)
            except BaseException:
                self._staging.give(slots[0])
                raise
            handle = RemoteCxlAllocation(
                self._staging.view(slots[0])[:size], location, slots[0], region
            )
            region.track(handle)
            return handle

    def _take_page(self, size: int) -> tuple[RemoteCxlRegion, StorageLocation]:
        """A free page: from a region with room, a new region, or eviction.

        Only regions a WRITE can reach now are used: a WRITE to a region
        whose remote CXL agent has not answered since it last failed would fail
        after the transport's timeout and leave the page quarantined. Eviction likewise
        frees pages only in those regions (``_evict``).
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

    def free(self, handle: RemoteCxlAllocation) -> None:
        """Give back an allocation that was not stored.

        A committed allocation or one a transfer may still touch is left
        alone; the connector's cleanup must not reuse it.
        """
        with self._lock:
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
        with self._lock:
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
                handle.region.validate(handle)
                if handle.state != "writing":
                    raise ValueError("Remote CXL allocation was already submitted")
            if not self._write(handles):
                return [False] * len(keys)
            return self._commit(keys, handles)

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
        are isolated.
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
                self._give_back(group)
        if not ok:
            self._give_back(written)
            return False
        for h in handles:
            self._give_slot(h)
        return True

    def _write_group(self, group: list[RemoteCxlAllocation]) -> bool:
        """WRITE the pages of one region; on failure they are given back or
        isolated, and False is returned."""
        from maru_remote.transport import TransferTimeout, TransferUncertainError

        region = group[0].region
        if region.retired:
            # It went back after the pages were taken (memory node rebooted).
            self._give_back(group)
            return False
        try:
            access = self._map_own(region)
        except StorageUnavailableError as exc:
            logger.warning(
                "Remote CXL region %d cannot be mapped: %s", region.region_id, exc
            )
            self._give_back(group)
            self._set_aside(region)
            return False
        if region.retired:
            # Its memory node rebooted and the region went back.
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
            for h in group:
                region.untrack(h)
                region.quarantine(h.location)
                h.state = "isolated"
            self._isolated.append(
                IsolatedTransfer(
                    exc.pending,
                    "WRITE",
                    exc.pending.peer,
                    region,
                    [h.location for h in group],
                    [h.slot for h in group],
                )
            )
            logger.warning("RDMA WRITE not confirmed: %s", exc)
            return False
        except Exception as exc:
            logger.warning("RDMA WRITE failed: %s", exc)
            self._data.disconnect({access.peer})
            # The memory node's NIC still accepts the region's key, and a
            # WRITE that ended in an error at this end may still be in the
            # network. Its pages stay out of use while this worker owns the
            # region; the release before the region goes back ends any late
            # WRITE. The slots are safe: this end's connection is torn down.
            for h in group:
                region.untrack(h)
                region.quarantine(h.location)
                self._give_slot(h)
                h.state = "freed"
            # A node that cannot be reached any more has the region set aside
            # now; its keys stop being reported as hits if it stays so.
            try:
                self._map_own(region)
            except StorageUnavailableError as map_exc:
                logger.warning(
                    "Remote CXL region %d cannot be mapped: %s",
                    region.region_id,
                    map_exc,
                )
                self._set_aside(region)
            return False
        return True

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
            return bool(self._isolated)

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
            try:
                got = self._read(hits, slots, result.get("regions", {}))
            except BaseException:
                # _read hands every slot back or isolates it before it
                # returns; what escapes it (an interrupt) may leave a READ
                # running, so its slots stay taken.
                if lease_id:
                    self._release_lease(lease_id)
                raise
            held = bool(lease_id) and self._release_lease(lease_id)
            out: list[CpuReadLease | None] = [None] * len(keys)
            unused = set(slots)
            for (i, location), slot in zip(hits, slots, strict=False):
                if not (got and held):
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
    ) -> bool:
        """READ each hit into its slot; False if not confirmed for all.

        On False the slots are back in the staging buffer, or isolated if a
        READ may still land in them. Errors end in False; only an interrupt
        raises, and then the slots stay taken.
        """
        from maru_remote.transport import TransferTimeout, TransferUncertainError

        by_peer: dict[str, list[tuple[int, int, int]]] = {}
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
                self._isolated.append(
                    IsolatedTransfer(
                        exc.pending, "READ", exc.pending.peer, None, [], list(slots)
                    )
                )
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
        for thread in self._threads:
            thread.join()
        self._threads = []
        with self._lock:
            self._leave()
            if not self._isolated:
                self._stop_data_path()
            else:
                logger.warning(
                    "RDMA transfers still own staging memory; leaving it registered"
                )
            self._keys.clear()
            self._pending.clear()
            self.connected = False
            self.rpc.close()
