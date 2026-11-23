# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruClient: the storage client behind MaruHandler on maru_node.

ATTACH to the node, map its pools, open the worker session with the node-view
pid, keep credit at hand. On a UDS break it unmaps the pools once unused (a
new run may need their memory) and attaches again: to the same run it maps
them again, keeps credit and resends unresolved ops; to a new run it drops
them. A read goes only through the mapping of the
run its lookup was answered under. If the node refuses the attach after the
start, the client turns off: no loads, no stores, no retries. After a
MaruServer restart it drops the pins of the ended epoch. A metadata-only
client (a vLLM scheduler) attaches as a probe: no pool, credit or pin, PROBE
only.
"""

import collections
import dataclasses
import heapq
import itertools
import logging
import os
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass, field

from maru_common.config import MaruConfig
from maru_common.ms_rpc import PLACEHOLDER_OP
from maru_common.node_protocol import (
    AbortRequest,
    AttachRequest,
    AttachResponse,
    CreditRequest,
    CreditReturnRequest,
    DetachRequest,
    GroupLayout,
    HintRequest,
    NodeMessageType,
    Notify,
    Peers,
    ReadJoinRequest,
    UnmapAckRequest,
    WriteDoneItem,
    WriteDoneRequest,
    process_start_time,
)
from maru_common.remote_protocol import TransferEnd
from maru_common.storage_types import (
    HINT_CAPABILITY,
    MAX_BATCH,
    MS_PROTOCOL,
    WORKER_RDMA_CAPABILITY,
    HintAction,
    ItemOutcome,
    ItemResult,
    Key,
    Location,
    MissReason,
    NotifyKind,
    SessionRole,
    Status,
    StorageLocation,
    StorageUnavailableError,
    WaitLevel,
)

from ..storage.cpu import CpuReadLease
from .credit_cache import CreditCache
from .pin_table import PinTable
from .pool_maps import (
    LocationError,
    MappedPool,
    PoolMaps,
    RegionHolder,
    StaleRunError,
)
from .server_conn import ServerConn, WorkerIdentity
from .storage_conn import (
    AttachRefusedError,
    NodeNotListeningError,
    OpTracker,
    StorageConn,
    close_fds,
)
from .worker_rdma import WorkerRdma

logger = logging.getLogger(__name__)

RECONNECT_S = 0.5
REFILL_IDLE_S = 0.2
REFILL_EMPTY_S = 0.05
# A store waiting for its step's CREDIT looks again this often; a step's ask
# not met is asked again until this long after the step began
DEMAND_POLL_S = 0.005
DEMAND_TTL_S = 1.0
# Longest pause between attempts of a background connect
CONNECT_RETRY_MAX_S = 1.0
# Wait for DETACH and the last requests of close()
CLOSE_TIMEOUT_S = 5.0
BUSY_RESEND_S = 0.2
ATTACH_WARN_S = 30.0


class _DeadlineQueue:
    """One process-wide thread for deadlines and delayed retries."""

    def __init__(self):
        self._cv = threading.Condition()
        self._heap = []
        self._seq = itertools.count()
        self.thread = threading.Thread(
            target=self._run, name="maru-client-deadlines", daemon=True
        )
        self.thread.start()

    def schedule(self, deadline, callback):
        task = [deadline, next(self._seq), callback]
        with self._cv:
            heapq.heappush(self._heap, task)
            self._cv.notify()

        def cancel():
            with self._cv:
                task[2] = None
                self._cv.notify()

        return cancel

    def _run(self):
        while True:
            with self._cv:
                while self._heap and self._heap[0][2] is None:
                    heapq.heappop(self._heap)
                if not self._heap:
                    self._cv.wait()
                    continue
                left = self._heap[0][0] - time.monotonic()
                if left > 0:
                    self._cv.wait(left)
                    continue
                _, _, callback = heapq.heappop(self._heap)
            if callback is not None:
                try:
                    callback()
                except Exception:
                    logger.exception("deadline callback failed")


_deadline_lock = threading.Lock()
_deadline_pid = None
_deadline_queue = None


def _reset_deadlines():
    global _deadline_lock, _deadline_pid, _deadline_queue
    _deadline_lock = threading.Lock()
    _deadline_pid = None
    _deadline_queue = None


os.register_at_fork(after_in_child=_reset_deadlines)


def _deadlines():
    global _deadline_pid, _deadline_queue
    with _deadline_lock:
        if _deadline_pid != os.getpid():
            _deadline_pid = os.getpid()
            _deadline_queue = _DeadlineQueue()
        return _deadline_queue


def _miss_label(source: str, outcome: ItemOutcome) -> str:
    """Label one lookup miss."""
    if outcome.result == ItemResult.MISS:
        return f"{source}:{outcome.reason.value if outcome.reason else 'miss'}"
    return f"{source}:{outcome.result.value.lower()}"


def _release_view(view: memoryview) -> None:
    """Release our view; if the caller still exports it, GC releases it later
    (the pool stays mapped until then: the view count guards the unmap)."""
    try:
        view.release()
    except BufferError:
        logger.warning("a released buffer is still exported by the caller")


class CreditUnavailableError(ValueError):
    """No credit at hand: this store is skipped."""


@dataclass(eq=False)
class CreditHandle:
    """A credit page lent to the caller; ``buf`` is ``size`` bytes."""

    buf: memoryview
    location: Location
    node_epoch: str
    size: int
    # writing -> submitted -> done; or writing -> freed / dropped
    state: str = "writing"
    remote: object = None


@dataclass(eq=False)
class PinnedRead(CpuReadLease):
    """A pinned read; release() after the last read or copy."""

    key: str = ""
    pin_id: str = ""
    server_epoch: str = ""
    node_epoch: str | None = None
    owner: object = None

    def release(self):
        if self.owner is None:
            super().release()
        else:
            self.owner.release_retrieved([self])


@dataclass(eq=False)
class _ChunkRead:
    """One lookup batch and its pending direct reads."""

    out: list[PinnedRead | None]
    server: ServerConn | None = None
    keys: list[str] = field(default_factory=list)
    outcomes: list[ItemOutcome] = field(default_factory=list)
    epoch: str = ""
    node_epoch: str | None = None
    remote: dict = field(default_factory=dict)


class PendingRetrieve:
    """Direct reads started by one retrieve; result transfers ownership once."""

    def __init__(self, client):
        self._client = client
        self.misses = collections.Counter()
        self._chunks = []
        self.hint_id = ""

    def done(self):
        return all(r.done() for c in self._chunks for r in c.remote.values())

    def result(self):
        try:
            return self._client._finish_retrieve(self)
        finally:
            if self.hint_id:
                self._client.end_read_hint(self.hint_id)

    def cancel(self):
        chunks, self._chunks = self._chunks, []
        self._client._drop_chunks(chunks)
        if self.hint_id:
            self._client.end_read_hint(self.hint_id)


class _Throttle:
    """Log a failure that repeats at most once per ATTACH_WARN_S."""

    def __init__(self) -> None:
        self._since: float | None = None
        self._last = 0.0

    def __call__(self, what: str, error: Exception) -> None:
        now = time.monotonic()
        self._since = self._since or now
        if now - self._last >= ATTACH_WARN_S:
            self._last = now
            logger.warning("%s for %.0f s: %s", what, now - self._since, error)


def probe_node(config: MaruConfig) -> AttachResponse:
    """One probe ATTACH, closed at once: what the node tells its workers
    (node, MaruServer, page size, transfer defaults). Waits up to
    ``node_connect_timeout_s`` for a node that is not listening yet."""
    client = MaruClient(dataclasses.replace(config, metadata_only=True))
    conn, response, fds = client._first_attach()
    close_fds(fds)
    try:
        conn.request(NodeMessageType.DETACH, DetachRequest(), client.rpc_timeout_s)
    except Exception as e:
        logger.debug("DETACH of a node probe: %s", e)
    conn.close()
    return response


class MaruClient:
    def __init__(
        self,
        config: MaruConfig,
        *,
        rpc_timeout_s: float | None = None,
        reconnect_s: float = RECONNECT_S,
    ):
        if not config.maru_node:
            raise ValueError("MaruClient needs maru_node (the StorageService socket)")
        if not config.engine_id or not config.cache_namespace:
            raise ValueError("maru_node needs engine_id and cache_namespace")
        self.config = config
        self._socket_path: str = config.maru_node
        self.role = SessionRole.PROBE if config.metadata_only else SessionRole.WORKER
        # Probes and load-only workers never write
        self._writes = not config.metadata_only and not config.load_only
        # Per client: one process may hold two (TP=1 scheduler and worker)
        self.identity = WorkerIdentity(
            engine_id=config.engine_id,
            dp_index=config.dp_index,
            tp_rank=config.tp_rank,
            incarnation=uuid.uuid4().hex,
            namespace=config.cache_namespace,
        )
        self.rpc_timeout_s = (
            rpc_timeout_s if rpc_timeout_s is not None else config.timeout_ms / 1000
        )
        self.reconnect_s = reconnect_s
        self.pools = PoolMaps(
            pin_pools=config.pin_pools,
            holder=RegionHolder.this_process(self.identity.incarnation),
        )
        self.credits = CreditCache()
        self.pins = PinTable()

        self._lock = threading.RLock()
        self._cv = threading.Condition(self._lock)
        self._storage: StorageConn | None = None
        self._connecting: StorageConn | None = None  # an ATTACH under way
        self._server: ServerConn | None = None
        self._ops: OpTracker | None = None
        self.attach_info: AttachResponse | None = None
        self.node_epoch: str | None = None
        # The run whose pools an ATTACH is installing, not current yet
        self._attaching_epoch: str | None = None
        self._handles: set[CreditHandle] = set()
        self._reads: set[PinnedRead] = set()
        self._rdma: WorkerRdma | None = None
        self._stopped = threading.Event()
        self._retrieve_local = threading.local()
        self._returns: list[Location] = []
        self._connected = False
        self._closing = threading.Event()
        self._refill_wake = threading.Event()
        # Set when a WRITE_DONE or ABORT frees credit the node holds for us
        self._credit_freed = threading.Event()
        self._threads: list[threading.Thread] = []
        # Background connect: pools mapped (or refused), and the refusal
        self._connect_thread: threading.Thread | None = None
        self._attached = threading.Event()
        self._ready = threading.Event()  # connected, or refused
        self.refused: AttachRefusedError | None = None
        # The node refused a re-attach after the start: Maru is off here
        self.off: AttachRefusedError | None = None
        # Server epochs that ended; reads of their pins finish, none start
        self._ended_epochs: set[str] = set()
        self._lost = threading.Event()
        self._credit_op: tuple[OpTracker, int] | None = None
        # The current step's ask (bytes, 0 once answered), its number, and how
        # long the step's stores may still wait for the answer
        self._demand = 0
        self._demand_seq = 0
        self._demand_wait = 0.0
        self._demand_until = 0.0
        # Credit the step took since its ask: the ask shrinks by it
        self._demand_taken = 0
        self._sends: collections.deque = collections.deque()
        self._sends_cv = threading.Condition()
        # Outcomes by class: unavailable, not_found and fetch apart
        self.stats: collections.Counter[str] = collections.Counter()
        self._stats_lock = threading.Lock()
        from maru_handler.hints import MemoryHints

        self._hints = MemoryHints(self._hint_locate, self._hint_send)

    def stop(self):
        """Freeze ownership after a CUDA error."""
        self._stopped.set()
        self._closing.set()
        self._refill_wake.set()
        self._lost.set()
        with self._sends_cv:
            self._sends_cv.notify_all()
        if self._rdma is not None:
            self._rdma.stop()
        if self._server is not None:
            self._server.freeze()
        self.credits.close()
        if self._storage is not None:
            self._storage.close()

    # ------------------------------------------------------------- lifecycle

    def connect(self) -> bool:
        with self._lock:
            if self._closing.is_set():
                raise RuntimeError("Create a fresh handler after close")
            if self._connected:
                return True
        server = None
        try:
            conn, response, fds = self._first_attach()
            self._take_attach(conn, response, fds, first=True)
            server = ServerConn(
                response.server,
                self.identity,
                node_id=response.node_id,
                node_epoch=response.node_epoch,
                pid=response.pid,
                pid_start=response.pid_start,
                timeout_s=self.rpc_timeout_s,
                lookup_timeout_s=self.config.lookup_timeout_ms / 1000,
                role=self.role,
            )
            server.on_server_epoch.append(self._server_epoch_changed)
            server.on_stale_node.append(self._stale_node)
            server.open()
            with self._lock:
                if self._closing.is_set():
                    raise StorageUnavailableError("closed while connecting")
                self._server = server
                self._connected = True
            conn.start_notifications()
            server.start()
            if self._writes:
                self._refill_once()
        except BaseException:
            if server is not None and self._server is not server:
                server.close()  # no allocation yet: WORKER_CLOSE is safe
            try:
                self._teardown()
            except Exception:
                logger.exception("cleaning up a failed connect")
            raise
        self._attached.set()
        self._ready.set()
        self._start_thread(self._keep_attached, "maru-storage-keeper")
        self._start_session_threads()
        return True

    def connect_background(self) -> None:
        """Connect on a thread of its own, retrying until close(): attach and
        map the pools once, then open the worker session. ``connected`` turns
        True once both are done; a MaruServer outage never unmaps the pools.
        A refusal by the node stops it (``refused``, ``wait_attached``)."""
        with self._lock:
            if self._closing.is_set():
                raise RuntimeError("Create a fresh handler after close")
            if self._connected or self._connect_thread is not None:
                return
            self._connect_thread = threading.Thread(
                target=self._connect_loop, name="maru-connect", daemon=True
            )
            thread = self._connect_thread
        thread.start()

    def stop_credit_waits(self) -> None:
        """End every wait for credit, now and later; allocations go on."""
        self.credits.close()

    def wait_attached(self, timeout_s: float | None = None) -> bool:
        """Wait until the pools are mapped (by default up to
        ``node_connect_timeout_s``); whether they are.

        Raises:
            AttachRefusedError: If the node refused this client.
        """
        self._attached.wait(
            self.config.node_connect_timeout_s if timeout_s is None else timeout_s
        )
        if self.refused is not None:
            raise self.refused
        return self._attached.is_set()

    def wait_connected(self, timeout_s: float) -> bool:
        """Wait until connected; whether it is.

        Raises:
            AttachRefusedError: If the node refused this client.
        """
        self._ready.wait(timeout_s)
        if self.refused is not None:
            raise self.refused
        return self.connected

    def _start_thread(self, target: Callable[[], None], name: str) -> None:
        with self._lock:
            if self._closing.is_set():
                return
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self._threads.append(thread)

    def _start_session_threads(self) -> None:
        _deadlines()
        if self.role == SessionRole.WORKER:
            self._start_thread(self._send_loop, "maru-write-done")
        if self._writes:
            self._start_thread(self._refill_loop, "maru-credit-refill")

    def _connect_loop(self) -> None:
        """connect_background's thread."""
        warned = _Throttle()
        pause = 0.05
        while True:
            try:
                conn, response, fds = self._attach(
                    max(self.config.node_connect_timeout_s, self.rpc_timeout_s)
                )
                self._take_attach(conn, response, fds, first=True)
                break
            except AttachRefusedError as e:
                self.refused = e
                logger.error("the StorageService refuses this client: %s", e)
                self._attached.set()
                self._ready.set()
                return
            except Exception as e:
                if self._closing.is_set():
                    return
                warned("cannot attach to the StorageService", e)
            if self._closing.wait(pause):
                return
            pause = min(pause * 2, CONNECT_RETRY_MAX_S)
        self._attached.set()
        with self._lock:
            storage = self._storage
        if storage is not None:
            try:
                storage.start_notifications()
            except RuntimeError:
                pass  # closed meanwhile; the keeper attaches again
        self._start_thread(self._keep_attached, "maru-storage-keeper")
        server = None
        pause = 0.05
        while not self._closing.is_set() and self.off is None:
            with self._lock:
                info = self.attach_info
            if info is not None:
                if server is None:
                    server = ServerConn(
                        info.server,
                        self.identity,
                        node_id=info.node_id,
                        node_epoch=info.node_epoch,
                        pid=info.pid,
                        pid_start=info.pid_start,
                        timeout_s=self.rpc_timeout_s,
                        lookup_timeout_s=self.config.lookup_timeout_ms / 1000,
                        role=self.role,
                    )
                    server.on_server_epoch.append(self._server_epoch_changed)
                else:
                    # The node may have restarted meanwhile
                    server.node_epoch = info.node_epoch
                    server.pid, server.pid_start = info.pid, info.pid_start
                try:
                    server.open()
                    break
                except StorageUnavailableError as e:
                    warned("cannot open the MaruServer session", e)
            if self._closing.wait(pause):
                break
            pause = min(pause * 2, CONNECT_RETRY_MAX_S)
        with self._lock:
            installed = (
                server is not None and not self._closing.is_set() and self.off is None
            )
            if installed:
                self._server = server
                self._connected = True
        if not installed:
            if server is not None:
                server.close()  # no allocation yet: WORKER_CLOSE is safe
            return
        assert server is not None
        server.on_stale_node.append(self._stale_node)
        server.start()
        self._start_session_threads()
        self._refill_wake.set()
        self._ready.set()
        logger.info("connected to the StorageService and MaruServer")

    def _attach_request(self) -> AttachRequest:
        pid = os.getpid()
        return AttachRequest(
            engine_id=self.identity.engine_id,
            dp_index=self.identity.dp_index,
            tp_rank=self.identity.tp_rank,
            pid=pid,
            pid_start=process_start_time(pid),
            incarnation=self.identity.incarnation,
            namespace=self.identity.namespace,
            groups=[]
            if self.role == SessionRole.PROBE
            else [
                GroupLayout(
                    object_bytes=self.config.chunk_size_bytes,
                    layout=self.config.storage_schema,
                )
            ],
            protocol=MS_PROTOCOL,
            capabilities=[WORKER_RDMA_CAPABILITY, HINT_CAPABILITY],
            instance=self.config.instance_id,
            role=self.role,
            step_objects=self.config.step_objects if self._writes else 0,
        )

    def _first_attach(self) -> tuple[StorageConn, AttachResponse, list[int]]:
        """ATTACH, waiting up to node_connect_timeout_s for a node that is not
        listening yet (starting or restarting)."""
        deadline = time.monotonic() + self.config.node_connect_timeout_s
        pause = 0.05
        while True:
            try:
                return self._attach(
                    max(deadline - time.monotonic(), self.rpc_timeout_s)
                )
            except NodeNotListeningError as e:
                if time.monotonic() + pause > deadline:
                    raise
                if self._closing.wait(pause):
                    raise StorageUnavailableError(
                        "closed while waiting for the node"
                    ) from e
                pause = min(pause * 2, 0.5)

    def _attach(
        self, timeout_s: float
    ) -> tuple[StorageConn, AttachResponse, list[int]]:
        """One ATTACH within ``timeout_s``; close() cancels it."""
        conn = StorageConn(
            self._socket_path, on_notify=self._on_notify, on_lost=self._on_lost
        )
        with self._lock:
            if self._closing.is_set():
                raise StorageUnavailableError("the client is closed")
            self._connecting = conn
        try:
            response, fds = conn.connect(self._attach_request(), timeout_s)
        finally:
            with self._lock:
                self._connecting = None
        return conn, response, fds

    def _take_attach(
        self, conn: StorageConn, response: AttachResponse, fds: list[int], first: bool
    ) -> None:
        """Take in an ATTACH reply (pools, credit and ops of its run); after
        close() nothing is installed (a re-attach close() gave up waiting on)."""
        added: list[str] = []
        try:
            if self._closing.is_set():
                raise StorageUnavailableError("the client is closed")
            if self.role == SessionRole.WORKER:
                if response.page_size < self.config.chunk_size_bytes:
                    raise AttachRefusedError(
                        f"the node page ({response.page_size} bytes) is smaller "
                        f"than an object ({self.config.chunk_size_bytes} bytes)"
                    )
                if response.budgets is None:
                    raise StorageUnavailableError("the ATTACH reply has no budgets")
                if first and self._writes:
                    b = response.budgets
                    logger.info(
                        "credit: %d pages at hand, up to %d pages a step asks for",
                        b.credit_refill_high // response.page_size,
                        b.credit_max_bytes // response.page_size,
                    )
                if first and 2 * self.config.chunk_size_bytes <= response.page_size:
                    logger.warning(
                        "objects of %d bytes fill %d%% of the node's %d-byte pages; "
                        "a node page_size near the object size wastes less",
                        self.config.chunk_size_bytes,
                        100 * self.config.chunk_size_bytes // response.page_size,
                        response.page_size,
                    )
            if (
                self.role == SessionRole.WORKER
                and WORKER_RDMA_CAPABILITY in response.capabilities
            ):
                if response.staging is not None:
                    settings = response.staging
                    if self._rdma is not None and (
                        self._rdma.staging.nbytes != settings.size
                        or self.config.rdma_device != settings.rdma_device
                    ):
                        raise AttachRefusedError("worker staging settings changed")
                    self.config.rdma_staging_size = settings.size
                    self.config.rdma_device = settings.rdma_device
                    self.config.rdma_timeout_s = settings.timeout_s
                if self._rdma is None:
                    self._rdma = WorkerRdma(
                        self.config,
                        response.page_size,
                        quarantine_max=response.staging.quarantine_max
                        if response.staging
                        else None,
                    )
                elif self._rdma.staging.slot_bytes != response.page_size:
                    raise AttachRefusedError("RDMA page size changed")
                self._rdma.notify_peers(response.peers)
                joined = conn.request(
                    NodeMessageType.READ_JOIN,
                    ReadJoinRequest(self._rdma.name, self._rdma.transport.metadata()),
                    self.rpc_timeout_s,
                )
                if joined.status != Status.OK:
                    raise AttachRefusedError("READ_JOIN refused")
            same_run = response.node_epoch == self.node_epoch
            with self._lock:
                # Its pools are not old ones while they are installed
                self._attaching_epoch = response.node_epoch
            for descriptor in response.pools:
                mapped = self.pools.get(descriptor.pool.pool_id)
                if same_run and mapped and mapped.node_epoch == response.node_epoch:
                    continue  # still mapped
                if descriptor.dax is not None:
                    try:
                        self.pools.add_dax(descriptor, response.node_epoch)
                    except (OSError, RuntimeError, ValueError) as e:
                        logger.error(
                            "cannot map CXL pool %s: %s", descriptor.pool.pool_id, e
                        )
                        continue
                elif descriptor.memfd is not None:
                    # pools.add closes the fd, also on failure
                    fd, fds[descriptor.memfd] = fds[descriptor.memfd], -1
                    self.pools.add(descriptor, fd, response.node_epoch)
                else:
                    continue
                added.append(descriptor.pool.pool_id)
            close_fds(fds)  # those of pools still mapped
        except BaseException:
            with self._lock:
                self._attaching_epoch = None
            close_fds(fds)
            conn.close()
            raise
        with self._lock:
            self._attaching_epoch = None
            # close() sets _closing under this lock: it sees what is installed
            closed = self._closing.is_set()
            if not closed:
                if not same_run:
                    if self._rdma is not None:
                        self._rdma.advance()
                    if self.node_epoch is not None:
                        logger.warning(
                            "the StorageService restarted (node epoch %s -> %s); "
                            "old credit and unresolved writes are dropped",
                            self.node_epoch,
                            response.node_epoch,
                        )
                    self.node_epoch = response.node_epoch
                    self._ops = OpTracker(
                        self.identity.incarnation, response.node_epoch
                    )
                    self._returns.clear()
                    self.credits.reset(response.node_epoch)
                self.attach_info = response
                self._storage = conn
                self._count("attach")
                self._cv.notify_all()
        if closed:
            conn.close()
            for pool_id in added:
                self.pools.remove(pool_id)
            raise StorageUnavailableError("the client is closed")
        if not first:
            if same_run:
                self._resend_unresolved()
            elif self._server is not None:
                self._server.move_to(
                    response.node_epoch, response.pid, response.pid_start
                )
            conn.start_notifications()
            self._refill_wake.set()
            self._unmap_unused_old_pools()

    def close(self) -> None:
        """Return unused credit, WORKER_CLOSE, DETACH and unmap the pools.
        Pools of allocations a copy may still write, and of reads not yet
        released, stay mapped and registered until the process ends, so each
        such close() keeps them."""
        if self._stopped.is_set():
            from .worker_rdma import _KEPT_UNTIL_EXIT

            _KEPT_UNTIL_EXIT.append(self)
            return
        self._hints.close(timeout_s=1, end_open=True)
        with self._lock:
            if self._closing.is_set():
                return
            self._closing.set()
            if self._rdma is not None:
                self._rdma.advance()
            handles = [h for h in self._handles if h.state == "writing"]
            reads = list(self._reads)
            connecting = self._connecting
            connect_thread = self._connect_thread
        if connecting is not None:
            connecting.close()
        if handles:
            # Copies into them may still run: no DETACH, no WORKER_CLOSE, and
            # their pools stay mapped and registered; the node cleans up when
            # this process ends
            logger.warning(
                "closing with %d allocations not stored or freed; their credit "
                "stays with the StorageService until this process ends",
                len(handles),
            )
        if reads:
            # A copy may still read them: their pins stay until the node
            # reports this process's exit (no WORKER_CLOSE, no DETACH)
            logger.warning(
                "closing with %d pinned reads not released; their pools stay "
                "mapped and their pins held until this process ends",
                len(reads),
            )
        self._refill_wake.set()
        self._credit_freed.set()
        self.credits.close()  # no credit wait outlasts close()
        self._lost.set()
        with self._sends_cv:
            self._sends_cv.notify_all()
        if connect_thread is not None:
            connect_thread.join(timeout=CLOSE_TIMEOUT_S)
        with self._lock:
            threads = list(self._threads)
        for thread in threads:
            thread.join(timeout=CLOSE_TIMEOUT_S)
        storage = self._storage
        if storage is not None and not storage.closed:
            unused = self.credits.take_all()
            try:
                for i in range(0, len(unused), MAX_BATCH):
                    storage.request(
                        NodeMessageType.CREDIT_RETURN,
                        CreditReturnRequest(credits=unused[i : i + MAX_BATCH]),
                        CLOSE_TIMEOUT_S,
                    )
            except Exception as e:
                logger.warning("returning credit before closing failed: %s", e)
        # MaruServer drops our pins on WORKER_CLOSE, or on the WORKER_EXIT the
        # node reports once this process ends. A DETACH makes the node forget
        # us, so it goes only once MaruServer holds nothing of ours.
        with self._lock:
            server, self._server = self._server, None
        ended = server.close(goodbye=not (handles or reads)) if server else True
        if ended and not (handles or reads):
            try:
                if storage is not None and not storage.closed:
                    storage.request(
                        NodeMessageType.DETACH, DetachRequest(), CLOSE_TIMEOUT_S
                    )
            except Exception as e:
                logger.warning("DETACH before closing failed: %s", e)
        self._teardown(writing=handles, reading=reads)
        if self._rdma is not None:
            self._rdma.close()
        reads_seen = {
            name: n
            for name, n in sorted(self._counters().items())
            if name.startswith("retrieve_")
        }
        if reads_seen:
            logger.info(
                "maru_node worker %s reads: %s", self.identity.incarnation, reads_seen
            )

    def _teardown(
        self,
        goodbye: bool = True,
        writing: list[CreditHandle] | None = None,
        reading: list[PinnedRead] | None = None,
    ) -> None:
        """``writing``: handles a copy may still write, ``reading``: reads it
        may still read; their views and pools are left as they are."""
        kept = set(writing or ())
        with self._lock:
            storage, self._storage = self._storage, None
            server, self._server = self._server, None
            self._connected = False
            # A later connect() starts as a new run: its pools get mapped
            self.node_epoch = None
            if self._rdma is not None:
                self._rdma.advance()
            self._ops = None
            self.attach_info = None
            for handle in self._handles:
                if handle not in kept:
                    _release_view(handle.buf)
            self._handles.clear()
            self._cv.notify_all()
        if storage is not None:
            storage.close()
        if server is not None:
            server.close(goodbye=goodbye)
        self.pools.close(
            keep={h.location.pool_id for h in kept}
            | {r.location.pool_id for r in reading or ()}
        )

    @property
    def connected(self) -> bool:
        with self._lock:
            return self._connected and not self._closing.is_set()

    @property
    def storage_connected(self) -> bool:
        with self._lock:
            return self._storage is not None and not self._storage.closed

    def _ensure(self, data: bool = True) -> None:
        if self._stopped.is_set() or not self.connected:
            raise StorageUnavailableError("the maru_node client is not connected")

    def healthcheck(self) -> bool:
        """Both connections are up and MaruServer answers a heartbeat."""
        server = self._server
        return bool(
            self.connected
            and self.storage_connected
            and server is not None
            and server.heartbeat_once()
        )

    def _count(self, name: str, n: int = 1) -> None:
        with self._stats_lock:
            self.stats[name] += n

    def local_stats(self) -> dict:
        """This client's counters and holdings (no MaruServer call)."""
        with self._lock:
            return {
                "node_epoch": self.node_epoch,
                "storage_connected": self.storage_connected,
                "unused_credit_bytes": self.credits.unused_bytes,
                "allocations": len(self._handles),
                "pinned_reads": len(self._reads),
                "pools": self.pools.pool_ids(),
                "rdma": self._rdma.snapshot() if self._rdma else None,
                "counters": self._counters(),
            }

    def _counters(self) -> dict[str, int]:
        with self._stats_lock:
            return dict(self.stats)

    @property
    def server_conn(self) -> ServerConn | None:
        return self._server

    @property
    def page_size(self) -> int:
        return self.attach_info.page_size if self.attach_info else 0

    def storage_gpu_accessible(self) -> bool:
        """Whether every mapped pool is page-locked for GPU copy kernels."""
        mapped = (
            self.pools.page_locked()
            if self.pools.pool_ids()
            else self._rdma is not None
        )
        return mapped and (self._rdma is None or self._rdma.staging.cuda_registered)

    # ----------------------------------------------------------------- writes

    def alloc(
        self,
        size: int,
        route_key: str | None = None,
        wait_s: float = 0.0,
        retry_later: bool = False,
    ) -> CreditHandle:
        """A credit page for ``size`` bytes, without an RPC; with ``wait_s``
        it waits that long for the refill when no credit is at hand.
        ``retry_later``: a miss is counted as deferred, not as a skip."""
        if self.off is not None:
            self._count("store_skipped_off")
            raise CreditUnavailableError(f"Maru is off on this worker: {self.off}")
        self._ensure()
        if self.role == SessionRole.PROBE:
            raise RuntimeError("A metadata-only handler cannot allocate memory")
        if not self._writes:
            raise CreditUnavailableError("this worker only loads; it holds no credit")
        if type(size) is not int or not 0 < size <= self.config.chunk_size_bytes:
            raise ValueError(
                f"an allocation holds 1..{self.config.chunk_size_bytes} bytes"
            )
        if not self.storage_connected:
            self._count("store_skipped_unavailable")
            raise CreditUnavailableError(
                "the StorageService is unreachable; skipping this store"
            )
        taken = self.credits.take()
        if taken is None and wait_s > 0:
            self._refill_wake.set()
            taken = self.credits.take(wait_s)
        elif taken is None:
            taken = self._wait_step_credit()
        if taken is None:
            self._count(
                "store_deferred_no_credit" if retry_later else "store_skipped_no_credit"
            )
            self._refill_wake.set()
            raise CreditUnavailableError("no credit at hand; skipping this store")
        credit, node_epoch = taken
        with self._lock:
            if self._demand:
                self._demand_taken += credit.length
        remote = None
        try:
            if self._rdma is not None and self._rdma.owns_credit(credit):
                if node_epoch != self.node_epoch:
                    raise StaleRunError("credit run ended")
                remote = self._rdma.alloc_write(credit, size)
                if remote is None:
                    if self._rdma.credit_retired(credit):
                        self.credits.discard(self._rdma.credit_retired)
                        self._refill_wake.set()
                        raise CreditUnavailableError("write domain ended")
                    self.credits.put([credit], node_epoch)
                    raise CreditUnavailableError("no remote staging slot")
                view = remote.view
            else:
                view = self.pools.view(credit, size, node_epoch=node_epoch)
        except LocationError as e:
            with self._lock:
                ended = node_epoch != self.node_epoch
            if ended or isinstance(e, StaleRunError):
                # Of an ended run: not given back to the new one
                self._count("store_skipped_stale_run")
                raise CreditUnavailableError(f"credit of an ended run: {e}") from e
            # Given back: the node may give it to a worker that maps it
            logger.error("a credit outside the mapped pools: %s", e)
            self._return_credits([credit])
            self._count("store_skipped_unmapped")
            raise CreditUnavailableError(f"credit of an unmapped pool: {e}") from e
        handle = CreditHandle(
            buf=view, location=credit, node_epoch=node_epoch, size=size, remote=remote
        )
        with self._lock:
            # Against Maru turning off (under this lock), which ends the
            # session once no handle is writing
            off = self.off is not None
            if not off:
                self._handles.add(handle)
            info = self.attach_info
        if off:
            _release_view(view)
            self._return_credits([credit])
            self._count("store_skipped_off")
            raise CreditUnavailableError("Maru is off")
        if (
            info is not None
            and info.budgets is not None
            and self.credits.unused_bytes < info.budgets.credit_refill_low
        ):
            self._refill_wake.set()
        return handle

    def _own(self, handle) -> CreditHandle:
        with self._lock:
            if not isinstance(handle, CreditHandle) or handle not in self._handles:
                raise ValueError("Allocation does not belong to this maru_node client")
        return handle

    def free(self, handle: CreditHandle) -> None:
        """Give back an unstored allocation; a stored one is left alone."""
        if self._stopped.is_set():
            return
        with self._lock:
            if not isinstance(handle, CreditHandle) or handle not in self._handles:
                return
            if handle.state != "writing":
                return
            self._finish_handle(handle, "freed")
            reuse = handle.node_epoch == self.node_epoch
        if not reuse:
            self._unmap_unused_old_pools()
        elif not self.credits.put([handle.location], handle.node_epoch):
            # the run ended meanwhile
            self._return_credits([handle.location])
        if self.off is not None:
            self._end_session_when_off()

    def _finish_handle(self, handle: CreditHandle, state: str) -> None:
        """Caller holds the lock."""
        if self._stopped.is_set():
            return
        handle.state = state
        if handle.remote is not None:
            self._rdma.release_write(handle.remote)
        _release_view(handle.buf)
        self._handles.discard(handle)
        self._cv.notify_all()

    def has_local(self, key: str) -> bool:
        """KV has no owner here."""
        return False

    def batch_store(self, keys: list[str], handles: list[CreditHandle]) -> list[bool]:
        """Publish written allocations; one bool per key. Waits at most
        ``store_timeout_s``: unfinished items are False, their op stays
        tracked and their pages stay with the node."""
        chunks = self._submit_store(keys, handles)
        out = [False] * len(keys)
        deadline = time.monotonic() + self.config.store_timeout_s
        for start, future in chunks:
            try:
                results = future.result(max(0.0, deadline - time.monotonic()))
            except FutureTimeout:
                self._count("store_timeout")
                continue
            out[start : start + len(results)] = results
        return out

    def store_async(self, keys: list[str], handles: list[CreditHandle]) -> Future:
        """batch_store as a future (no timeout); WRITE_DONE goes from the
        sender thread."""
        chunks = self._submit_store(keys, handles)
        future: Future = Future()
        if not chunks:
            future.set_result([])
            return future

        lock = threading.Lock()
        remaining = [len(chunks)]

        def gather(_=None) -> None:
            # Chunks may finish on different threads: the last one answers
            with lock:
                remaining[0] -= 1
                if remaining[0]:
                    return
            out = []
            for _, c in chunks:
                out.extend(c.result())
            future.set_result(out)

        for _, c in chunks:
            c.add_done_callback(gather)
        return future

    def _submit_store(self, keys, handles) -> list[tuple[int, Future]]:
        """Check and mark every handle at once (free() cannot slip between),
        then queue WRITE_DONE in batches."""
        if len(keys) != len(handles):
            raise ValueError("keys and handles must have the same length")
        self._ensure()
        if len({id(h) for h in handles}) != len(handles):
            raise ValueError("An allocation can only be stored once per batch")
        for key in keys:
            if not isinstance(key, str) or not key or len(key) > 4096:
                raise ValueError("Invalid key")
        with self._lock:
            for handle in handles:
                self._own(handle)
                if handle.state != "writing":
                    raise ValueError("The allocation was already submitted")
            for handle in handles:
                handle.state = "submitted"
        chunks = []
        for i in range(0, len(keys), MAX_BATCH):
            future: Future = Future()
            chunks.append((i, future))
            self._queue_store(
                keys[i : i + MAX_BATCH], handles[i : i + MAX_BATCH], future
            )
        return chunks

    def _queue_store(self, keys, handles, future: Future) -> None:
        def send() -> None:
            try:
                self._send_write_done(keys, handles, future)
            except Exception:
                logger.exception("sending WRITE_DONE failed")
                if not future.done():
                    future.set_result([False] * len(keys))

        self._post(send)

    def _post(self, task: Callable[[], object]) -> None:
        """Run ``task`` on the sender thread (never on the receive thread)."""
        with self._sends_cv:
            self._sends.append(task)
            self._sends_cv.notify()

    def _send_loop(self) -> None:
        while True:
            with self._sends_cv:
                while not self._sends and not self._closing.is_set():
                    self._sends_cv.wait()
                if not self._sends:
                    return
                task = self._sends.popleft()
            if self._stopped.is_set():
                return
            try:
                task()
            except Exception:
                logger.exception("a send task failed")

    def _send_write_done(self, keys, handles, future: Future) -> None:
        results = [False] * len(keys)
        if self._stopped.is_set():
            future.set_result(results)
            return
        live = []
        returns: list[Location] = []
        with self._lock:
            storage, ops, epoch = self._storage, self._ops, self.node_epoch
            for i, handle in enumerate(handles):
                if handle.node_epoch != epoch:
                    self._finish_handle(handle, "dropped")
                elif storage is None or storage.closed:
                    self._finish_handle(handle, "freed")
                    self._count("store_skipped_unavailable")
                    if not self.credits.put([handle.location], handle.node_epoch):
                        returns.append(handle.location)  # the run ended meanwhile
                else:
                    live.append(i)
        if returns:
            self._return_credits(returns)
        if self.off is not None:
            self._end_session_when_off()
        if len(live) < len(handles):
            self._unmap_unused_old_pools()
        if not live:
            future.set_result(results)
            return
        ends = {}
        for i in list(live):
            if self._stopped.is_set():
                future.set_result(results)
                return
            handle = handles[i]
            if handle.remote is not None:
                ends[i] = self._rdma.write(handle.remote)
                if ends[i] == TransferEnd.NOT_POSTED:
                    live.remove(i)
                    self._abort([handle.location])
                    with self._lock:
                        self._finish_handle(handle, "freed")
        if self._stopped.is_set() or not live:
            future.set_result(results)
            return
        assert ops is not None
        request = ops.origin.stamp(
            WriteDoneRequest(
                op=PLACEHOLDER_OP,
                items=[
                    WriteDoneItem(
                        key=Key(self.identity.namespace, keys[i]),
                        credit=handles[i].location,
                        logical_bytes=handles[i].size,
                        domain=handles[i].remote.agent.domain
                        if handles[i].remote
                        else None,
                        end=ends.get(i, TransferEnd.DONE),
                    )
                    for i in live
                ],
                wait=WaitLevel.PUBLISHED,
            )
        )
        credits = [handles[i].location for i in live]
        abortable = [handles[i].location for i in live if handles[i].remote is None]
        with self._lock:
            for i in live:
                self._finish_handle(handles[i], "done")

        def on_result(response) -> None:
            if response.status != Status.OK:
                logger.error(
                    "WRITE_DONE refused: %s %s", response.status, response.error
                )
                self._abort(abortable)
                return
            rejected = [
                credit
                for credit, outcome in zip(credits, response.items, strict=True)
                if credit in abortable
                and outcome.result not in (ItemResult.ADDED, ItemResult.DUPLICATE)
            ]
            if rejected:
                self._abort(rejected)

        reply = self._send_op(NodeMessageType.WRITE_DONE, request, on_result, ops)
        # The node no longer counts these pages against our credit
        self._credit_freed.set()
        self._refill_wake.set()

        def done(f: Future) -> None:
            try:
                response = f.result()
            except Exception:
                self._count("write_done_unknown")
                future.set_result(results)
                return
            if response.status == Status.OK and len(response.items) == len(live):
                for i, outcome in zip(live, response.items, strict=True):
                    results[i] = ends.get(
                        i, TransferEnd.DONE
                    ) == TransferEnd.DONE and outcome.result in (
                        ItemResult.ADDED,
                        ItemResult.DUPLICATE,
                    )
            future.set_result(results)

        reply.add_done_callback(done)

    def _abort(self, credits: list[Location]) -> None:
        """ABORT refused credits (from the sender thread); never written again."""
        self._post(lambda: self._abort_now(credits))

    def _abort_now(self, credits: list[Location]) -> None:
        with self._lock:
            ops = self._ops
        if ops is None or not credits:
            return
        for i in range(0, len(credits), MAX_BATCH):
            request = ops.origin.stamp(
                AbortRequest(op=PLACEHOLDER_OP, credits=credits[i : i + MAX_BATCH])
            )
            self._send_op(NodeMessageType.ABORT, request, lambda _r: None, ops)
        self._credit_freed.set()
        self._refill_wake.set()

    def _send_op(
        self,
        code: NodeMessageType,
        request,
        on_result: Callable,
        ops: OpTracker,
        resend: bool = False,
        *,
        deadline: float | None = None,
    ) -> Future:
        """Send an op, tracked until its result is known; on_result runs once.
        Unknown and BUSY stay tracked: resent after a reconnect or a pause."""
        reply: Future = Future()
        if self._stopped.is_set():
            reply.set_exception(StorageUnavailableError("worker stopped"))
            return reply
        with self._lock:
            if ops is not self._ops:
                reply.set_exception(StorageUnavailableError("the run ended"))
                return reply
            if resend:
                if not ops.update(request):
                    reply.set_exception(StorageUnavailableError("already resolved"))
                    return reply
            else:
                ops.track(code, request, on_result)
            storage = self._storage
        if storage is None or storage.closed:
            reply.set_exception(StorageUnavailableError("not connected"))
            return reply
        reply = storage.submit(code, request)
        seq = request.op.seq

        def done(f: Future) -> None:
            try:
                response = f.result()
            except Exception:
                return  # unresolved; resent after reconnecting
            if response.status == Status.BUSY:
                _deadlines().schedule(
                    time.monotonic() + BUSY_RESEND_S,
                    lambda: self._post(
                        lambda: self._send_op(
                            code, ops.origin.refresh(request), on_result, ops, True
                        )
                    ),
                )
                return
            if not ops.resolved(seq):
                return  # another reply of this op was handled already
            try:
                on_result(response)
            except Exception:
                logger.exception("handling the result of 0x%02X failed", code)

        reply.add_done_callback(done)
        return reply

    def _resend_unresolved(self) -> None:
        with self._lock:
            ops = self._ops
        if ops is None:
            return
        for code, request, on_result in ops.unresolved():
            self._send_op(code, ops.origin.refresh(request), on_result, ops, True)
        with self._lock:
            returns, self._returns = self._returns, []
        if returns:
            self._return_credits(returns)

    # ----------------------------------------------------------------- credit

    def _refill_once(self) -> bool:
        """One CREDIT if below the low mark; whether credit came."""
        if not self._writes:
            return False
        with self._lock:
            storage, ops, server, info = (
                self._storage,
                self._ops,
                self._server,
                self.attach_info,
            )
        if (
            storage is None
            or storage.closed
            or ops is None
            or server is None
            or not server.session
            or info is None
            or info.budgets is None
        ):
            return False
        with self._lock:
            pending = self._credit_op
        if (
            pending is not None
            and pending[0] is ops
            and pending[1] in ops.origin.outstanding
        ):
            return False  # one CREDIT at a time; that one is resent until known
        budgets = info.budgets
        if self._rdma is not None:
            self.credits.discard(self._rdma.credit_retired)
        unused = self.credits.unused_bytes
        with self._lock:
            demand = max(0, self._demand - self._demand_taken)
            demand_seq = self._demand_seq
        if demand <= unused:
            self._demand_answered(demand_seq)
            demand = 0
        if not demand and unused >= budgets.credit_refill_low and unused > 0:
            return False
        want = max(budgets.credit_refill_high, demand) - unused
        if want <= 0:
            return False
        request = ops.origin.stamp(
            CreditRequest(
                op=PLACEHOLDER_OP,
                worker_session=server.session,
                want_bytes=want,
            )
        )
        epoch = ops.node_epoch
        got = []
        with self._lock:
            self._credit_op = (ops, request.op.seq)

        def on_result(response) -> None:
            if response.status != Status.OK:
                logger.warning("CREDIT: %s %s", response.status, response.error)
                return
            if not response.credits:
                return
            remote = getattr(response, "write_agent", None)
            remote_credits = []
            if remote is not None and self._rdma is not None:
                remote_credits = [
                    c
                    for c in response.credits
                    if any(p.pool_id == c.pool_id for p in remote.pools)
                ]
                if not self._rdma.bind_credits(
                    remote_credits, remote, info.node_id, epoch
                ):
                    self._count("credit_bad_domain")
                    return
            mapped = [
                c
                for c in response.credits
                if self.pools.get(c.pool_id) or c in remote_credits
            ]
            if len(mapped) < len(response.credits):
                # A pool this worker could not map: given back at once
                self._return_credits([c for c in response.credits if c not in mapped])
                self._count("credit_unmapped")
            if not mapped:
                return
            if self.credits.put(mapped, epoch):
                got.extend(mapped)

        reply = self._send_op(NodeMessageType.CREDIT, request, on_result, ops)
        try:
            reply.result(self.rpc_timeout_s * 5)
        except Exception:
            return False
        if demand:
            if self.credits.unused_bytes >= demand:
                self._demand_answered(demand_seq)
            else:
                # Answered short: the stores stop waiting; the ask goes on
                # in the background while credit frees
                with self._lock:
                    if self._demand_seq == demand_seq:
                        self._demand_wait = 0.0
        return bool(got)

    def want_credit(self, pages: int) -> None:
        """This step stores ``pages`` pages: ask the node now, not at the low
        mark, so the credit is at hand when the stores come. A store may wait
        up to ``credit_wait_s`` in the step for that answer."""
        if pages <= 0 or not self._writes or self.off is not None:
            return
        info = self.attach_info
        if info is None or info.budgets is None:
            return
        nbytes = min(pages * info.page_size, info.budgets.credit_max_bytes)
        enough = self.credits.unused_bytes >= nbytes
        with self._lock:
            self._demand_seq += 1
            self._demand = 0 if enough else nbytes
            self._demand_wait = 0.0 if enough else self.config.credit_wait_s
            self._demand_until = time.monotonic() + DEMAND_TTL_S
            self._demand_taken = 0
        if not enough:
            self._refill_wake.set()

    def _demand_answered(self, seq: int) -> None:
        with self._lock:
            if self._demand_seq == seq:
                self._demand = 0

    def _demand_unmet(self) -> bool:
        """What the step still needs is not at hand, and its ask is current."""
        with self._lock:
            if self._demand and time.monotonic() >= self._demand_until:
                self._demand = 0
            need = self._demand - self._demand_taken
        return need > self.credits.unused_bytes

    def _wait_step_credit(self) -> tuple[Location, str] | None:
        """Credit while this step's ask is unmet, within the step's
        ``credit_wait_s``; None once met or out of time."""
        while not self._closing.is_set() and not self.credits.closed:
            with self._lock:
                if self._demand <= 0 or self._demand_wait <= 0:
                    return None
                budget = min(self._demand_wait, DEMAND_POLL_S)
            started = time.monotonic()
            taken = self.credits.take(budget)
            with self._lock:
                self._demand_wait -= time.monotonic() - started
            if taken is not None:
                return taken
        return None

    def _refill_loop(self) -> None:
        while not self._closing.is_set():
            self._refill_wake.wait(REFILL_IDLE_S)
            self._refill_wake.clear()
            if self._closing.is_set() or self.off is not None:
                return
            try:
                # A CREDIT sent after a WRITE_DONE sees the credit it freed
                self._credit_freed.clear()
                got = self._refill_once()
                if (not got and self.credits.unused_bytes == 0) or self._demand_unmet():
                    # Ask again as soon as a WRITE_DONE frees credit (the
                    # node's commit-pending budget, or credit of its own)
                    self._credit_freed.wait(REFILL_EMPTY_S)
                    self._refill_wake.set()
            except Exception:
                logger.exception("credit refill failed")

    def _return_credits(self, credits: list[Location]) -> None:
        if self._stopped.is_set():
            return
        with self._lock:
            storage = self._storage
        if storage is None or storage.closed:
            with self._lock:
                self._returns.extend(credits)
            return
        for i in range(0, len(credits), MAX_BATCH):
            batch = credits[i : i + MAX_BATCH]
            reply = storage.submit(
                NodeMessageType.CREDIT_RETURN, CreditReturnRequest(credits=batch)
            )

            def done(f: Future, batch=batch) -> None:
                try:
                    f.result()
                except Exception:
                    with self._lock:
                        self._returns.extend(batch)  # sent again after reconnect

            reply.add_done_callback(done)

    # ---------------------------------------------------------- parked fetch

    # ------------------------------------------------------------------ reads

    @property
    def read_hints_enabled(self):
        info = self.attach_info
        return bool(
            self.config.read_hints and info and HINT_CAPABILITY in info.capabilities
        )

    def hint_read(self, request_id, keys, route_key=None):
        if self.read_hints_enabled and not self._stopped.is_set():
            self._hints.hint(request_id, keys)

    def end_read_hint(self, request_id):
        if self.config.read_hints and not self._stopped.is_set():
            self._hints.end(request_id)

    def promote_read(self, request_id, route_key=None):
        if self.config.read_promotes and not self._stopped.is_set():
            self._hints.promote(request_id, ["node"])

    def _hint_locate(self, keys):
        server = self._server
        if server is None or self._stopped.is_set() or not self.storage_connected:
            return []
        ranges = []
        for i in range(0, len(keys), MAX_BATCH):
            outcomes = server.probe(
                [Key(self.identity.namespace, k) for k in keys[i : i + MAX_BATCH]],
                with_locations=True,
            )
            for outcome in outcomes or []:
                if outcome.result != ItemResult.HIT or outcome.location is None:
                    return [("node", ranges)] if ranges else []
                ranges.append(outcome.location)
            if outcomes is None:
                break
        return [("node", ranges)] if ranges else []

    def _hint_send(self, target, message):
        storage, info = self._storage, self.attach_info
        if (
            self._stopped.is_set()
            or storage is None
            or info is None
            or HINT_CAPABILITY not in info.capabilities
        ):
            return False
        reply = storage.submit(
            NodeMessageType.HINT,
            HintRequest(
                HintAction(message["action"]), message["request"], message["ranges"]
            ),
            deadline=time.monotonic() + self.rpc_timeout_s,
        ).result(timeout=self.rpc_timeout_s)
        return reply.status == Status.OK

    def prepare_read_thread(self) -> None:
        server = self._server
        if server is not None:
            server.prepare_read_thread()

    def retrieve_capacity(self, wanted=None):
        return self._rdma.capacity() if self._rdma is not None else None

    def batch_retrieve(self, keys, route_key=None):
        return self.start_retrieve(keys, route_key).result()

    def start_retrieve(self, keys, route_key=None):
        pending = PendingRetrieve(self)
        self._retrieve_local.misses = pending.misses
        rdma = self._rdma
        generation = rdma.capture() if rdma is not None else None
        if (
            self.off is not None
            or self._stopped.is_set()
            or (rdma is not None and generation is None)
        ):
            self._count(
                "retrieve_off" if self.off is not None else "retrieve_unavailable",
                len(keys),
            )
            pending._chunks.append(_ChunkRead([None] * len(keys)))
            pending.misses["unavailable"] += len(keys)
            return pending
        self._ensure()
        if self.read_hints_enabled and self.role == SessionRole.WORKER:
            pending.hint_id = f"maru-load-{uuid.uuid4().hex}"
            self.hint_read(pending.hint_id, keys)
        try:
            for i in range(0, len(keys), MAX_BATCH):
                pending._chunks.append(
                    self._start_chunk(keys[i : i + MAX_BATCH], generation)
                )
        except BaseException:
            self._drop_chunks(pending._chunks)
            if pending.hint_id:
                self.end_read_hint(pending.hint_id)
            raise
        return pending

    def _start_chunk(self, keys, generation):
        server = self._server
        if not keys or server is None or not self.storage_connected:
            self._count("retrieve_unavailable", len(keys))
            return _ChunkRead([None] * len(keys))
        answered = server.lookup_and_pin(
            [Key(self.identity.namespace, k) for k in keys]
        )
        if answered is None:
            self._count("retrieve_unavailable", len(keys))
            self._note_miss("lookup:unavailable", len(keys))
            return _ChunkRead([None] * len(keys))
        outcomes, epoch, node_epoch = answered
        chunk = _ChunkRead([], server, keys, outcomes, epoch, node_epoch)
        try:
            for key, outcome in zip(keys, outcomes, strict=True):
                if (
                    self._rdma is not None
                    and outcome.result == ItemResult.HIT
                    and outcome.location is not None
                    and self.pools.get(outcome.location.pool_id) is None
                ):
                    # The captured lifetime precedes the LOOKUP.
                    if node_epoch != self.node_epoch or epoch in self._ended_epochs:
                        outcome_generation = -1
                    else:
                        outcome_generation = generation
                    chunk.remote[len(chunk.out)] = self._rdma.start(
                        outcome,
                        outcome_generation,
                        server.unpin,
                        current=lambda: (
                            server.server_epoch == epoch
                            and self.node_epoch == node_epoch
                            and not self._closing.is_set()
                            and self.off is None
                        ),
                    )
                    chunk.out.append(None)
                else:
                    chunk.out.append(
                        self._take_outcome(server, key, outcome, epoch, node_epoch)
                    )
        except BaseException:
            self._drop_chunks([chunk])
            if not self._stopped.is_set():
                server.unpin([o.pin_id for o in outcomes[len(chunk.out) :] if o.pin_id])
            raise
        return chunk

    def _finish_retrieve(self, pending):
        self._retrieve_local.misses = misses = pending.misses
        chunks, pending._chunks = pending._chunks, []
        out = []
        for n, chunk in enumerate(chunks):
            try:
                for index, request in chunk.remote.items():
                    chunk.out[index] = request.result()
                    if chunk.out[index] is None:
                        self._note_miss(f"reader:{request.reason or 'stopped'}")
                out.extend(chunk.out)
            except BaseException:
                self.release_retrieved(out + chunk.out)
                self._drop_chunks(chunks[n + 1 :])
                raise
        unknown = sum(read is None for read in out) - sum(misses.values())
        if unknown > 0:
            misses["unknown"] += unknown
        return [None] * len(out) if self._stopped.is_set() else out

    def _drop_chunks(self, chunks):
        for chunk in chunks:
            for request in chunk.remote.values():
                request.cancel()
                request.result()
            self.release_retrieved(chunk.out)

    def last_retrieve_misses(self):
        return dict(getattr(self._retrieve_local, "misses", {}))

    def _note_miss(self, what, n=1):
        misses = getattr(self._retrieve_local, "misses", None)
        if misses is not None:
            misses[what] += n

    def _take_outcome(
        self,
        server: ServerConn,
        key: str,
        outcome: ItemOutcome,
        server_epoch: str,
        node_epoch: str | None = None,
        prefix: str = "retrieve",
    ) -> PinnedRead | None:
        """Take a directly mapped HIT."""
        source = "lookup"
        if outcome.result != ItemResult.HIT:
            self._count_miss(prefix, outcome)
            self._note_miss(_miss_label(source, outcome))
            pins = []
            if outcome.pin_id is not None:
                pins.append(outcome.pin_id)
            if pins:
                server.unpin(pins)
            return None
        if outcome.location is None or outcome.pin_id is None:
            logger.error("a HIT without location or pin for %s", key)
            self._note_miss(f"{source}:bad_hit")
            if outcome.pin_id is not None:
                server.unpin([outcome.pin_id])
            return None
        if self.pins.holds(outcome.pin_id):
            # Never unpin it: it belongs to a read already handed out
            logger.error("MaruServer gave pin %s twice", outcome.pin_id)
            self._note_miss(f"{source}:bad_hit")
            return None
        try:
            # Only through the mapping of the run the lookup was answered
            # under: a new run released that run's pins
            view = self.pools.view(outcome.location, node_epoch=node_epoch)
        except StaleRunError as e:
            logger.debug("a lookup of %s from another node run: %s", key, e)
            self._count(f"{prefix}_unavailable")
            self._note_miss("unavailable")
            server.unpin([outcome.pin_id])
            return None
        except LocationError as e:
            if not self.storage_connected:  # unmapped with the connection
                self._count(f"{prefix}_unavailable")
                self._note_miss("unavailable")
                server.unpin([outcome.pin_id])
                return None
            # #59: never touch bytes outside a mapped pool
            logger.error("MaruServer gave %s for %s: %s", outcome.location, key, e)
            self._count(f"{prefix}_bad_location")
            self._note_miss(f"{source}:bad_location")
            server.unpin([outcome.pin_id])
            return None
        self.pins.add(
            outcome.pin_id,
            Key(self.identity.namespace, key),
            outcome.location,
            server_epoch,
        )
        loc = outcome.location
        read = PinnedRead(
            view=view,
            location=StorageLocation(
                loc.pool_id,
                loc.allocation_id,
                loc.generation,
                loc.offset,
                loc.length,
                medium=loc.kind.value,
            ),
            _release=None,
            key=key,
            pin_id=outcome.pin_id,
            server_epoch=server_epoch,
            owner=self,
        )
        with self._lock:
            # Against a MaruServer restart, whose pins are gone, and against
            # Maru turning off, whose WORKER_CLOSE may have released the pin
            # already (both under this lock)
            off = self.off is not None
            ended = off or server_epoch in self._ended_epochs
            if not ended:
                self._reads.add(read)
        if ended:
            # never read, and never unpinned (an ended epoch or the session's
            # end releases the pin)
            self.pins.release([outcome.pin_id], None)
            _release_view(view)
            self._count(f"{prefix}_off" if off else f"{prefix}_unavailable")
            self._note_miss("off" if off else "unavailable")
            return None
        self._count(f"{prefix}_hit")
        return read

    def release_retrieved(self, infos: list) -> None:
        """Return reads after the caller finished every CPU read or GPU copy."""
        if self._stopped.is_set():
            return
        reads = []
        for info in infos:
            if isinstance(info, PinnedRead) and not info._released:
                info._released = True
                _release_view(info.view)
                reads.append(info)
            elif info is not None and not isinstance(info, PinnedRead):
                info.release()
        self._release_reads(reads)

    def _release_reads(self, reads: list[PinnedRead]) -> None:
        if not reads or self._stopped.is_set():
            return
        with self._lock:
            server = self._server
        epoch = server.server_epoch if server is not None else None
        unpins = self.pins.release([r.pin_id for r in reads], epoch)
        if unpins and server is not None:
            server.unpin(unpins)
        with self._lock:
            for read in reads:
                self._reads.discard(read)
            self._cv.notify_all()
        self._unmap_unused_old_pools()
        if self.off is not None:
            self._end_session_when_off()

    def batch_exists(self, keys: list[str], route_key: str | None = None) -> list[bool]:
        """PROBE each key (a hint, no pin)."""
        if self.off is not None or self._stopped.is_set():
            return [False] * len(keys)
        self._ensure(data=False)
        out: list[bool] = []
        for i in range(0, len(keys), MAX_BATCH):
            batch = keys[i : i + MAX_BATCH]
            server = self._server
            outcomes = (
                None
                if server is None
                else server.probe([Key(self.identity.namespace, k) for k in batch])
            )
            if outcomes is None:
                self._count("probe_unavailable", len(batch))
                out.extend([False] * len(batch))
                continue
            # Plans are readable only when lease fetches were negotiated.
            for outcome in outcomes:
                if outcome.result == ItemResult.HIT:
                    self._count("probe_hit")
                    out.append(True)
                else:
                    self._count_miss("probe", outcome)
                    out.append(False)
        return out

    def _count_miss(self, prefix: str, outcome: ItemOutcome) -> None:
        """Count fetch failures separately from absent and offline keys."""
        result = outcome.result
        if result == ItemResult.MISS and outcome.reason in (
            MissReason.BUSY,
            MissReason.TIMEOUT,
            MissReason.IO_ERROR,
        ):
            self._count(f"{prefix}_{outcome.reason.value}")
        elif result == ItemResult.MISS and outcome.reason == MissReason.ABSENT:
            self._count(f"{prefix}_not_found")
        elif result == ItemResult.MISS and outcome.reason == MissReason.OFFLINE:
            self._count(f"{prefix}_unavailable")
            self._count(f"{prefix}_unavailable_offline")
        else:
            self._count(f"{prefix}_{result.value.lower()}")

    def _server_epoch_changed(self, old: str, new: str) -> None:
        with self._lock:
            self._ended_epochs.add(old)
            if self._rdma is not None:
                self._rdma.advance()
        dead = self.pins.drop_epoch(old)
        if dead:
            logger.warning(
                "%d pins of server epoch %s are gone with it; their reads finish "
                "but are not unpinned",
                len(dead),
                old,
            )

    # ------------------------------------------------------- connection loss

    def _on_lost(self, conn: StorageConn) -> None:
        with self._lock:
            if conn is self._storage:
                self._storage = None
        self._lost.set()

    def _keep_attached(self) -> None:
        """Attach again whenever the StorageService connection is gone."""
        failing_since = None
        last_warning = 0.0
        while not self._closing.is_set():
            self._lost.wait(self.reconnect_s)
            self._lost.clear()
            with self._lock:
                storage = self._storage
            if self._closing.is_set() or (storage is not None and not storage.closed):
                continue
            self._unmap_on_loss()
            try:
                conn, response, fds = self._attach(
                    max(self.config.node_connect_timeout_s, self.rpc_timeout_s)
                )
                self._take_attach(conn, response, fds, first=False)
            except AttachRefusedError as e:
                self._turn_off(e)
                return
            except Exception as e:
                now = time.monotonic()
                failing_since = failing_since or now
                if now - last_warning >= ATTACH_WARN_S:
                    last_warning = now
                    logger.warning(
                        "cannot attach to the StorageService for %.0f s: %s",
                        now - failing_since,
                        e,
                    )
                continue
            failing_since = None
            logger.info("attached again to node epoch %s", response.node_epoch)

    def _turn_off(self, error: AttachRefusedError) -> None:
        """The node refused a re-attach (a config that changed, such as a
        smaller page): no loads or stores until the process restarts; no
        retries. Reads already handed out finish."""
        with self._lock:
            if self.off is not None:
                return
            self.off = error
            self.refused = error
            if self._rdma is not None:
                self._rdma.advance()
        self.credits.close()
        self._refill_wake.set()
        logger.error(
            "the StorageService refused to attach this worker again (%s); Maru "
            "is off here until vLLM restarts",
            error,
        )
        server = self._server
        if server is not None:
            server.stop_reopening()
        self._end_session_when_off()

    def _end_session_when_off(self) -> None:
        """With Maru off, end the MaruServer session once nothing is read
        or written (only then may WORKER_CLOSE go)."""
        with self._lock:
            if (
                self.off is None
                or self._reads
                or any(h.state == "writing" for h in self._handles)
            ):
                return
            server, self._server = self._server, None
        if server is not None:
            threading.Thread(
                target=server.close, name="maru-off-close", daemon=True
            ).start()

    def _stale_node(self, node_epoch: str) -> None:
        """WORKER_OPEN was refused for ``node_epoch``. Reopen under the run the
        UDS is attached to if that is newer; never close a healthy UDS (a new
        run breaks it anyway)."""
        server = self._server
        with self._lock:
            info = self.attach_info if self._storage is not None else None
        if server is None or info is None or info.node_epoch == node_epoch:
            return  # MaruServer does not know this run yet: nothing to gain
        logger.warning(
            "worker session on node epoch %s; reopening on %s",
            node_epoch,
            info.node_epoch,
        )
        if self._rdma is not None:
            self._rdma.advance()
        server.move_to(info.node_epoch, info.pid, info.pid_start)

    def _unmap_on_loss(self) -> None:
        """The node connection is gone: every pool is unmapped once unused (a
        view still held keeps its pool until released), so a new run of the
        node may take its memory; a same-run ATTACH maps them again."""
        removed = self.pools.remove_all()
        if removed:
            logger.info(
                "the StorageService connection is gone: unmapping %s once unused",
                removed,
            )

    def _unmap_unused_old_pools(self) -> None:
        """Unmap pools of ended runs once unused."""
        with self._lock:
            current = (self.node_epoch, self._attaching_epoch)
            old = [
                m
                for pool_id in self.pools.pool_ids()
                if (m := self.pools.get(pool_id)) is not None
                and m.node_epoch not in current
            ]
            busy = {h.location.pool_id for h in self._handles} | {
                r.location.pool_id for r in self._reads
            }
        for m in old:
            pool_id = m.info.pool_id
            if pool_id not in busy and self.pools.remove(pool_id, expected=m):
                logger.info("unmapped %s of an ended StorageService run", pool_id)

    # ----------------------------------------------------------------- notify

    def _on_notify(self, notify: Notify, conn: StorageConn) -> None:
        """A notice of ``conn``; one that acts on pools or credits counts only
        while ``conn`` is the live connection (a late callback of an ended
        run's connection must not touch the new run's)."""
        if self._stopped.is_set():
            return
        if isinstance(notify, Peers):
            if conn is self._storage and not conn.closed and self._rdma is not None:
                self._rdma.notify_peers(notify.peers)
            return
        if notify.kind == NotifyKind.CREDIT_RETURN:
            with self._lock:
                if conn is not self._storage or conn.closed:
                    return
                credits = self.credits.take_bytes(notify.bytes or 0)
            if credits:
                self._return_credits(credits)
        elif notify.kind == NotifyKind.POOL_REMOVE:
            with self._lock:
                if conn is not self._storage or conn.closed:
                    return
                # What the notice is about, as it arrives: this run, this
                # connection, this mapping (an old run's is left to the
                # old-pool cleanup)
                epoch, storage = self.node_epoch, conn
                mapped = (
                    self.pools.get(notify.pool_id)
                    if notify.pool_id is not None
                    else None
                )
                if mapped is not None and mapped.node_epoch != epoch:
                    mapped = None
            threading.Thread(
                target=self._pool_remove,
                args=(notify, epoch, storage, mapped),
                name="maru-pool-remove",
                daemon=True,
            ).start()
        elif notify.kind == NotifyKind.POOL_ADD:
            logger.warning(
                "pool_add for %s: mapping added pools is not supported yet",
                notify.pool.pool.pool_id if notify.pool else None,
            )

    def _pool_remove(
        self,
        notify: Notify,
        epoch: str | None,
        storage: StorageConn | None,
        mapped: MappedPool | None,
    ) -> None:
        """``epoch``, ``storage`` and ``mapped``: the run, connection and
        mapping the notice arrived for."""
        pool_id, generation = notify.pool_id, notify.pool_generation
        if pool_id is None or generation is None:
            logger.warning("NOTIFY pool_remove without pool_id or generation; ignored")
            return
        with self._lock:
            if self.node_epoch != epoch or self._storage is not storage:
                return  # a new run or connection owns the pool_id now
            # Its credit at hand goes back first: no new write starts in it
            credits = self.credits.remove_pool(pool_id)
        if credits and not self._stopped.is_set():
            self._return_credits(credits)
        with self._lock:
            self._cv.wait_for(
                lambda: (
                    self._closing.is_set()
                    or not any(h.location.pool_id == pool_id for h in self._handles)
                    and not any(r.location.pool_id == pool_id for r in self._reads)
                )
            )
        with self._lock:
            # A new run or connection owns the pool_id now: not this notice's
            if (
                self._closing.is_set()
                or self.node_epoch != epoch
                or self._storage is not storage
            ):
                return
            credits = self.credits.remove_pool(pool_id)
        if credits and not self._stopped.is_set():
            self._return_credits(credits)
        if (
            mapped is not None and not self.pools.remove(pool_id, expected=mapped)
        ) or not self.pools.wait_closed(pool_id, CLOSE_TIMEOUT_S):
            logger.warning(
                "pool %s is still in use by the caller; not unmapped", pool_id
            )
            return
        if storage is not None and not storage.closed:
            storage.submit(
                NodeMessageType.UNMAP_ACK,
                UnmapAckRequest(pool_id=pool_id, pool_generation=generation),
            )
