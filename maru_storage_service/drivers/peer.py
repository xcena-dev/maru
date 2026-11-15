# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Remote tier: this GPU node's pages on memory nodes, moved by one-sided RDMA.

A memory node reserves, commits and frees its own pages; this node only moves
bytes and says how each transfer ended. Each run has its own NIXL agent
(UCX limited first) and registers its L1 pools whole as they attach. A memory
node is ready once its TCP connection proved the key, HELLO was answered, its
pools' metadata is loaded and a probe READ ended DONE; a lost connection is
tried again every RECONNECT_S, and a new memory run drops what the old one
gave. Requests are ops (seq, ack, digest): one whose answer is lost is sent
again with its seq after a reconnect to the same memory run.

Only DONE ends a transfer. One that ended otherwise, or ran past timeout_s,
leaves its local page with the tier (``kept``) until a late DONE (``settled``)
or the run's end. Kept pages count whole; no transfer is posted while the
kept and the running ones would pass quarantine_max, so the kept never do.
"""

from __future__ import annotations

import functools
import logging
import mmap
import os
import threading
import time
from collections.abc import Callable
from typing import Any

from maru_common.ms_rpc import PLACEHOLDER_OP, OpOrigin
from maru_common.remote_protocol import (
    REMOTE_WIRE_VERSION,
    CancelRequest,
    DropRequest,
    GoodbyeRequest,
    HelloRequest,
    HelloResponse,
    PingRequest,
    RemoteMessageType,
    RemotePool,
    RemoteWriteDoneRequest,
    ReserveRequest,
    StageRequest,
    TransferEnd,
    UnstageRequest,
    Written,
    peer_origin,
)
from maru_common.storage_types import (
    MAX_BATCH,
    REMOTE_CAPABILITY,
    STAGE_CAPABILITY,
    ItemOutcome,
    Key,
    Location,
    Status,
)
from maru_remote.checked import require_checked_transport
from maru_remote.exporter import NixlExporter
from maru_remote.region_map import DEFAULT_COPY_FILE, load_key
from maru_remote.transport import (
    NixlTransport,
    PendingTransfer,
    TransferState,
    buffer_address,
)

from ..config_fields import (
    NodeConfigError,
    TierConfig,
    TierContext,
    TierSpec,
    _number,
    parse_size,
)
from ..page_table import PageRef
from ..remote_client import RemoteConnection
from ..tiers import IOResult, Level, RemoteTier, Span, Staged, TierEnv

logger = logging.getLogger(__name__)

DEFAULT_RDMA_TIMEOUT_S = 30.0  # the same as maru_rdma_timeout_s today
DEFAULT_QUARANTINE_MAX = 1 << 30
# Requests to a memory node, and HELLO with the key proof
RPC_TIMEOUT_S = 5.0
RECONNECT_S = 5.0
PING_S = 5.0
# Demotion writes in flight per memory node
IO_DEPTH_PER_NODE = 8
PROBE_BYTES = 4096
PROBE_TIMEOUT_S = 2.0
_POLL_S = 0.0005


def _parse(raw: dict, ctx: TierContext) -> dict:
    what = ctx.what
    urls = raw.get("url")
    if isinstance(urls, str):
        urls = [u.strip() for u in urls.split(",")]
    if (
        not isinstance(urls, list)
        or not urls
        or not all(isinstance(u, str) and u for u in urls)
    ):
        raise NodeConfigError(f"{what}.url: required, one address or several")
    timeout = _number(raw, "timeout_s", what, DEFAULT_RDMA_TIMEOUT_S)
    if timeout <= 0:
        raise NodeConfigError(
            f"{what}.timeout_s: expected a positive number, got {timeout!r}"
        )
    device = raw.get("rdma_device")
    if device is not None and not isinstance(device, str):
        raise NodeConfigError(f"{what}.rdma_device: expected a string, got {device!r}")
    key_file = raw.get("key_file", DEFAULT_COPY_FILE)
    if not isinstance(key_file, str) or not key_file.strip():
        raise NodeConfigError(f"{what}.key_file: expected a path, got {key_file!r}")
    cap = parse_size(
        raw.get("quarantine_max", DEFAULT_QUARANTINE_MAX), f"{what}.quarantine_max"
    )
    if cap == 0:
        raise NodeConfigError(f"{what}.quarantine_max: must be positive")
    return {
        "url": tuple(urls),
        "rdma_device": device or None,
        "timeout_s": float(timeout),
        "options": {
            "key_file": os.path.expanduser(key_file),
            "quarantine_max": cap,
        },
    }


def _credit_key(location: Location) -> tuple[str, int, int]:
    return (location.pool_id, location.offset, location.generation)


def agent_name(node_id: str, node_epoch: str) -> str:
    return f"maru-gpu-{node_id}-{node_epoch[:8]}"


def _boot_id() -> str:
    try:
        with open("/proc/sys/kernel/random/boot_id") as f:
            return f.read().strip()
    except OSError:
        return ""


class RemoteTransfer:
    """One posted READ or WRITE (an IOResult)."""

    def __init__(
        self,
        *,
        run: str,
        op: str,
        node_id: str,
        src: PageRef | None,
        dst: PageRef | None,
        nbytes: int,
        pending: PendingTransfer,
        deadline: float | None,
        timeout_at: float,
        callback: Callable[[IOResult], None] | None,
        settled: Callable[[IOResult], None] | None,
    ):
        self.run, self.op, self.node_id = run, op, node_id
        self.src, self.dst, self.nbytes = src, dst, nbytes
        self.pending = pending
        self.deadline, self.timeout_at = deadline, timeout_at
        self.callback, self.settled = callback, settled
        self.ok = False
        self.late = False
        self.cancelled = False
        self.error: Exception | None = None
        self.kept = False
        self.done = threading.Event()
        # NIXL's last word: PENDING until the handle is released
        self.state = TransferState.PENDING
        # running -> ended (DONE first) | kept -> settled (a late DONE)
        self.phase = "running"


class _MemoryNode:
    """One memory node of the url list, and what its current run gave."""

    def __init__(self, url: str):
        self.url = url
        self.node_id = ""
        self.node_epoch = ""
        self.conn: RemoteConnection | None = None
        # Its latest connection's number (unique in this tier), and the
        # number of the one it was made ready on
        self.conn_gen = 0
        self.ready_gen = 0
        self.peer = ""  # NIXL agent of its run
        self.pools: dict[str, RemotePool] = {}
        self.capabilities: list[str] = []
        self.ready = False
        self.room = 0
        self.reserved = 0  # bytes reserved since room was told
        self.ops: OpOrigin | None = None
        # Requests whose answer is not known, by seq: sent again after a
        # reconnect to the same run
        self.unanswered: dict[int, tuple[int, Any, Callable[[Any], None]]] = {}
        # RESERVEs whose credits never came back: canceled after a reconnect
        self.lost_reserves: set[int] = set()
        # Credits given and not posted yet: page -> the connection (conn_gen)
        # they came on; only those of the current one are written
        self.credits: dict[tuple[str, int, int], int] = {}
        # WRITEs running there
        self.writes = 0
        # Set when its connection ends (or the tier stops): ping it now
        self.wake = threading.Event()


class NixlRemoteTier(RemoteTier):
    """Thread safe."""

    def __init__(
        self,
        tier: TierConfig,
        env: TierEnv,
        *,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.env = env
        self.page_size = env.page_size
        self.timeout_s = tier.timeout_s or DEFAULT_RDMA_TIMEOUT_S
        self.key_file = tier.options.get("key_file", DEFAULT_COPY_FILE)
        self.quarantine_max = int(
            tier.options.get("quarantine_max", DEFAULT_QUARANTINE_MAX)
        )
        self.rdma_device = tier.rdma_device or ""
        self.origin = peer_origin(env.node_id, env.node_epoch)
        self.agent = agent_name(env.node_id, env.node_epoch)
        self._clock = clock
        self._lock = threading.RLock()
        self._wake = threading.Condition(self._lock)
        self._nodes = [_MemoryNode(u) for u in tier.url]
        self.io_depth = IO_DEPTH_PER_NODE * len(self._nodes)
        self.transport: NixlTransport | None = None
        self.exporter: NixlExporter | None = None
        self._key = b""
        self._probe: mmap.mmap | None = None
        self._probe_reg: Any = None
        # L1 pools attached, and those the NIC refused: then no remote I/O
        self._attached: set[str] = set()
        self._refused: set[str] = set()
        self._server_epoch = ""
        self._capabilities: list[str] = []
        # Transfers to poll: running, or kept with the handle not released
        self._transfers: list[RemoteTransfer] = []
        # Kept, released in ERR: bytes may still land until the run ends
        self._held: list[RemoteTransfer] = []
        # Transfers whose first end was not reported yet (its callback done)
        self._unreported: set[RemoteTransfer] = set()
        # Local pages kept, and posted transfers running (each holds a page)
        self._kept_bytes = 0
        self._running = 0
        # NIXL peers of earlier memory runs, removed once no handle holds them
        self._stale_peers: set[str] = set()
        self._next_stage = 1  # stage_id: unique in this run
        self._conn_seq = 0  # connection numbers, over all memory nodes
        self._lost_callbacks: list[Callable[[str, int], None]] = []
        self._drained = False
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    # ------------------------------------------------------------ lifetime

    def open(self) -> None:
        """The run's NIXL agent, with UCX limited to key-checked transports."""
        self._key = load_key(self.key_file)
        if self.env.make_transport is not None:
            self.transport = self.env.make_transport(self.agent, self.rdma_device)
        else:
            require_checked_transport(os.environ)
            self.transport = NixlTransport(self.agent, ucx_device=self.rdma_device)
        self.exporter = NixlExporter(self.transport)
        self._probe = mmap.mmap(-1, PROBE_BYTES)
        self._probe_reg = self.transport.register(
            buffer_address(self._probe), PROBE_BYTES, tag="probe"
        )

    def capabilities(self) -> list[str]:
        """remote_v1 and stage_v1 once every L1 pool is registered with the
        NIC."""
        with self._lock:
            usable = self.transport is not None and self._attached and not self._refused
        return [REMOTE_CAPABILITY, STAGE_CAPABILITY] if usable else []

    def opened(self, server_epoch: str, accepted: list[str]) -> None:
        """The run is open: connect to the memory nodes if MaruServer
        accepted remote_v1."""
        if REMOTE_CAPABILITY not in accepted or not self.capabilities():
            logger.warning(
                "remote tier off: %s",
                "MaruServer does not accept remote_v1"
                if self.capabilities()
                else "this node's L1 pools are not all registered with the NIC",
            )
            return
        with self._lock:
            self._server_epoch, self._capabilities = server_epoch, list(accepted)
        for node in self._nodes:
            self._spawn(self._connect_loop, node, f"maru-remote-{node.url}")
        self._spawn(self._poll_loop, None, "maru-remote-poll")

    def _spawn(self, target, arg, name: str) -> None:
        args = () if arg is None else (arg,)
        thread = threading.Thread(target=target, args=args, name=name, daemon=True)
        self._threads.append(thread)
        thread.start()

    def describe(self, adopted: set[str]) -> str:
        return f"{len(self._nodes)} memory nodes {[n.url for n in self._nodes]}"

    def stop(self) -> None:
        """Connections end; each memory node is told the run is over."""
        self._stop.set()
        with self._wake:
            self._wake.notify_all()
        for node in self._nodes:
            node.wake.set()
        for node in self._nodes:
            conn = node.conn
            if conn is not None and node.ready:
                self._goodbye(node, conn)
            if conn is not None:
                conn.close()

    @staticmethod
    def _goodbye(node: _MemoryNode, conn: RemoteConnection) -> None:
        try:
            conn.call(RemoteMessageType.GOODBYE, GoodbyeRequest(), 1.0)
        except Exception:
            logger.debug("GOODBYE to %s failed", node.url, exc_info=True)

    def close(self) -> None:
        """The NIXL agent ends, unless a transfer may still use its memory."""
        for thread in self._threads:
            if thread is not threading.current_thread():
                thread.join(timeout=5.0)
        transport = self.transport
        if transport is None:
            return
        if transport.pending_peers():
            logger.error("remote transfers still hold this run's memory: kept")
            return
        transport.close()

    # --------------------------------------------------------- registration

    def attach(self, pool_id: str, memory: mmap.mmap) -> None:
        """Register the L1 mapping whole; one the NIC refuses turns the
        remote tier off for the run."""
        assert self.exporter is not None
        try:
            self.exporter.attach(pool_id, memory)
        except Exception as e:
            logger.error("the NIC refused %s; no remote I/O this run: %s", pool_id, e)
            with self._lock:
                self._refused.add(pool_id)
            return
        with self._lock:
            self._attached.add(pool_id)

    def detach(self, pool_id: str) -> None:
        """Deregister, unless a transfer handle still names this memory."""
        assert self.exporter is not None
        with self._lock:
            if any(t.state is TransferState.PENDING for t in self._transfers):
                raise RuntimeError(
                    f"remote transfers may still use {pool_id}: it stays registered"
                )
            self._attached.discard(pool_id)
        self.exporter.detach(pool_id)

    # --------------------------------------------------------------- nodes

    def ready(self, node_id: str) -> bool:
        with self._lock:
            if self._full() or self._drained:
                return False
            return any(n.ready and n.node_id == node_id for n in self._nodes)

    def connection(self, node_id: str) -> int:
        with self._lock:
            for node in self._nodes:
                if node.ready and node.node_id == node_id:
                    return node.ready_gen
        return 0

    def owner(self, pool_id: str) -> str | None:
        with self._lock:
            for node in self._nodes:
                if pool_id in node.pools:
                    return node.node_id
        return None

    def _node_of(self, pool_id: str) -> _MemoryNode | None:
        """Caller holds the lock."""
        for node in self._nodes:
            if pool_id in node.pools:
                return node
        return None

    def _connect_loop(self, node: _MemoryNode) -> None:
        while not self._stop.is_set():
            if node.conn is None or node.conn.closed:
                try:
                    self._connect(node)
                except Exception as e:
                    logger.warning("memory node %s: not connected: %s", node.url, e)
                    self._disconnect(node)
                    self._stop.wait(RECONNECT_S)
                    continue
            elif not node.ready:
                try:  # a transfer failed: its NIXL connection is made anew
                    self._load(node)
                except Exception as e:
                    logger.warning("memory node %s: not ready: %s", node.url, e)
                    self._stop.wait(RECONNECT_S)
                    continue
            node.wake.wait(PING_S)
            node.wake.clear()
            if self._stop.is_set():
                return
            self._drop_stale_peers()
            try:
                self._ping(node)
            except Exception as e:
                logger.warning("memory node %s: lost: %s", node.url, e)
                self._disconnect(node)

    def _hello(self, node: _MemoryNode) -> HelloRequest:
        assert self.transport is not None
        with self._lock:
            self._conn_seq += 1
            node.conn_gen = self._conn_seq
            return HelloRequest(
                version=REMOTE_WIRE_VERSION,
                node_id=self.env.node_id,
                node_epoch=self.env.node_epoch,
                host_boot_id=_boot_id(),
                pid=os.getpid(),
                server_epoch=self._server_epoch,
                agent_name=self.agent,
                agent_metadata=self.transport.metadata(),
                page_size=self.page_size,
                conn_gen=node.conn_gen,
                capabilities=list(self._capabilities),
                # Which memory run answers is not known yet, and seqs count
                # per memory run: every op carries its own ack
                ack=0,
            )

    def _connect(self, node: _MemoryNode) -> None:
        """Key proof, HELLO, the run's pools, announce, then a probe READ."""
        assert self.transport is not None
        conn, hello = RemoteConnection.open(
            node.url, self._key, self._hello(node), RPC_TIMEOUT_S
        )
        if hello.status != Status.OK:
            conn.close()
            raise ConnectionError(f"HELLO refused: {hello.refusal} {hello.error}")
        replay = self._take_run(node, conn, hello)
        with self._lock:
            gen = node.conn_gen
        conn.on_close(
            functools.partial(self._conn_lost, node, conn, gen, hello.node_id)
        )
        if self._stop.is_set():  # stop() may have passed this connection
            self._goodbye(node, conn)
            self._disconnect(node)
            raise ConnectionError("stopping")
        try:
            self._load(node)
        except BaseException:
            with self._lock:  # still to send again on the next connection
                for entry in replay:
                    node.unanswered.setdefault(entry[1].op.seq, entry)
            raise
        logger.info(
            "memory node %s ready: %s@%s, %d pools, room %d MiB",
            node.url,
            hello.node_id,
            hello.node_epoch,
            len(hello.pools),
            hello.room >> 20,
        )
        for code, request, callback in replay:
            self._send(node, code, request, callback)
        self._cancel_lost(node)

    def _load(self, node: _MemoryNode) -> None:
        """Load the memory run's pools into NIXL, announce, and probe; then
        the node is ready. A peer a transfer failed on is removed first,
        once no handle of it is held (else ConnectionError: tried later)."""
        assert self.transport is not None
        self._drop_stale_peers()
        with self._lock:
            peer, node_id, pools = node.peer, node.node_id, list(node.pools.values())
        if self.transport.has_peer(peer) and not self.transport.peer_usable(peer):
            if not self.transport.disconnect(peer):
                raise ConnectionError(f"{peer} still has transfers held")
        reached = [p for p in pools if p.metadata]  # the others are staged
        for pool in reached:
            self.transport.add_peer(pool.metadata, peer=peer, timeout_s=RPC_TIMEOUT_S)
        if not self.transport.announce(peer):
            raise ConnectionError(f"cannot reach {peer}")
        if reached and not self._probe_read(node_id, peer, reached[0]):
            raise ConnectionError(f"the probe READ of {peer} did not end DONE")
        with self._lock:
            if node.conn is None or node.conn.closed:
                raise ConnectionError(f"{node.url}: connection lost")
            node.ready, node.ready_gen = True, node.conn_gen

    def _take_run(
        self, node: _MemoryNode, conn: RemoteConnection, hello: HelloResponse
    ) -> list[tuple[int, Any, Callable[[Any], None]]]:
        """Take HELLO's memory run; what an earlier run gave is dropped.
        Returns the requests to send again (the same run)."""
        with self._lock:
            same = node.node_epoch == hello.node_epoch and node.ops is not None
            if not same:
                if node.node_epoch:
                    logger.warning(
                        "memory node %s: a new run %s; dropping run %s",
                        node.url,
                        hello.node_epoch,
                        node.node_epoch,
                    )
                node.ops = OpOrigin(self.origin)
                node.unanswered.clear()
                node.lost_reserves.clear()
                node.credits.clear()
            if node.peer and node.peer != hello.agent_name:
                self._stale_peers.add(node.peer)
            node.node_id, node.node_epoch = hello.node_id, hello.node_epoch
            node.conn, node.peer = conn, hello.agent_name
            node.pools = {p.pool_id: p for p in hello.pools}
            node.capabilities = list(hello.capabilities)
            node.room, node.reserved = hello.room, 0
            replay = sorted(node.unanswered.items())
            node.unanswered.clear()
            return [entry for _, entry in replay]

    def _probe_read(self, node_id: str, peer: str, pool: RemotePool) -> bool:
        assert self.transport is not None and self._probe is not None
        pending = self.transport.post(
            "READ", peer, [(buffer_address(self._probe), pool.base, PROBE_BYTES)]
        )
        deadline = self._clock() + PROBE_TIMEOUT_S
        while self._clock() < deadline:
            state = pending.poll_state(release_disconnected=True)
            if state is TransferState.DONE:
                return True
            if state is TransferState.ERR:
                return False
            time.sleep(_POLL_S)
        # Still running: a late READ only lands in the probe buffer, which
        # stays registered until the run ends
        with self._lock:
            self._transfers.append(
                RemoteTransfer(
                    run=self.env.node_epoch,
                    op="READ",
                    node_id=node_id,
                    src=None,
                    dst=None,
                    nbytes=0,
                    pending=pending,
                    deadline=None,
                    timeout_at=deadline,
                    callback=None,
                    settled=None,
                )
            )
        return False

    def _drop_stale_peers(self) -> None:
        """Remove the NIXL peers of earlier memory runs no handle holds."""
        assert self.transport is not None
        with self._lock:
            stale = list(self._stale_peers)
        for peer in stale:
            if not self.transport.has_peer(peer) or self.transport.disconnect(peer):
                with self._lock:
                    self._stale_peers.discard(peer)

    def on_lost(self, callback: Callable[[str, int], None]) -> None:
        with self._lock:
            self._lost_callbacks.append(callback)

    def _conn_lost(
        self, node: _MemoryNode, conn: RemoteConnection, gen: int, node_id: str
    ) -> None:
        """Connection ``gen`` to memory node ``node_id`` ended (on its
        reader, or ours closing it): no transfer goes there until one is
        made anew, and what began on it or before is told now."""
        with self._lock:
            was_ready = node.conn is conn and node.ready
            if node.conn is conn:
                node.ready = False
            callbacks = list(self._lost_callbacks)
        node.wake.set()
        if self._stop.is_set() or not node_id:
            return
        if was_ready:
            logger.warning("memory node %s (%s): connection lost", node.url, node_id)
        for callback in callbacks:
            try:
                callback(node_id, gen)
            except Exception:
                logger.exception("a connection-lost callback failed")

    def _disconnect(self, node: _MemoryNode) -> None:
        with self._lock:
            node.ready = False
            conn, node.conn = node.conn, None
        if conn is not None:
            conn.close()

    def _ping(self, node: _MemoryNode) -> None:
        conn = node.conn
        if conn is None or conn.closed:
            raise ConnectionError("not connected")
        response = conn.call(RemoteMessageType.PING, PingRequest(), RPC_TIMEOUT_S)
        if response.status != Status.OK:
            raise ConnectionError(f"PING answered {response.status}")
        with self._lock:
            node.room, node.reserved = response.room, 0

    # ----------------------------------------------------------------- ops

    def _stamp(self, node: _MemoryNode, request: Any) -> Any:
        assert node.ops is not None
        return node.ops.stamp(request)

    def _send(
        self,
        node: _MemoryNode,
        code: int,
        request: Any,
        callback: Callable[[Any], None],
        *,
        resolve: bool = True,
        abandoned: Callable[[], bool] = lambda: False,
        lost: Callable[[], None] | None = None,
        run: OpOrigin | None = None,
    ) -> None:
        """Send an op; its answer goes to ``callback``. One whose answer is
        lost is kept to be sent again (same seq) on the next connection,
        unless its sender abandoned it; ``lost`` is told at once. With
        ``resolve`` False the sender resolves the seq itself. With ``run``,
        nothing is sent once that memory run has ended."""
        seq = request.op.seq
        with self._lock:  # a memory run and its connection, together
            ops, conn = node.ops, node.conn
        if run is not None and ops is not run:
            return

        def answered(response: Any) -> None:
            if isinstance(response, ConnectionError):
                again = False
                with self._lock:
                    if node.ops is ops and not abandoned():  # same memory run
                        current = node.conn
                        # A newer connection took the replays already
                        again = current is not None and current is not conn
                        if not again:
                            node.unanswered[seq] = (code, request, callback)
                if lost is not None:
                    lost()
                if again:
                    self._send(
                        node,
                        code,
                        request,
                        callback,
                        resolve=resolve,
                        abandoned=abandoned,
                        run=ops,
                    )
                return
            if ops is not None and resolve:
                ops.resolved(seq)
            callback(response)

        if conn is None or conn.closed:
            answered(ConnectionError("not connected"))
            return
        conn.submit(
            code, ops.refresh(request) if ops is not None else request, answered
        )

    def _call(
        self, node: _MemoryNode, code: int, request: Any, *, resolve: bool = True
    ) -> Any:
        """Send an op and wait for its answer; raises TimeoutError when it is
        not known by then: then the op is abandoned with ``resolve`` False
        (a late answer is dropped, the seq stays outstanding), else sent
        again later. An answer either arrives in time or is abandoned."""
        cv = threading.Condition()
        state: dict[str, Any] = {}

        def callback(response: Any) -> None:
            with cv:
                if not state.get("abandoned"):
                    state["answer"] = response
                    cv.notify_all()

        def abandoned() -> bool:
            with cv:
                return bool(state.get("abandoned")) and not resolve

        self._send(node, code, request, callback, resolve=resolve, abandoned=abandoned)
        with cv:
            if not cv.wait_for(lambda: "answer" in state, RPC_TIMEOUT_S):
                state["abandoned"] = True
                raise TimeoutError(f"no answer from {node.url}")
            return state["answer"]

    def _pick(self, nbytes: int, skip: set[str]) -> _MemoryNode | None:
        """Caller holds the lock: the ready memory node with the most room."""
        best = None
        for node in self._nodes:
            if not node.ready or node.node_id in skip:
                continue
            room = node.room - node.reserved
            if room >= self.page_size and (best is None or room > best[0]):
                best = (room, node)
        return None if best is None else best[1]

    def reserve(self, keys: list[Key]) -> list[Location | None]:
        """A credited page per key, from the ready memory nodes with the most
        room, each asked once, no more than fit under quarantine_max; None
        where none was given."""
        out: list[Location | None] = [None] * len(keys)
        if not keys or not self._usable():
            return out
        given, tried = 0, set[str]()
        while given < len(keys):
            with self._lock:
                node = self._pick(self.page_size, tried)
                if node is None:
                    break
                tried.add(node.node_id)
                fits = self._fits()
                if fits <= 0:
                    break
                slots = IO_DEPTH_PER_NODE - node.writes - len(node.credits)
                want = min(len(keys) - given, MAX_BATCH, max(0, slots), fits)
                if want <= 0:
                    continue
                node.reserved += want * self.page_size
                request = self._stamp(node, ReserveRequest(PLACEHOLDER_OP, want))
            credits = self._reserve_from(node, request)
            for credit in credits[: len(keys) - given]:
                out[given] = credit
                given += 1
        return out

    def _reserve_from(
        self, node: _MemoryNode, request: ReserveRequest
    ) -> list[Location]:
        seq = request.op.seq
        ops = node.ops
        try:
            response = self._call(
                node, RemoteMessageType.RESERVE, request, resolve=False
            )
        except (TimeoutError, ConnectionError):
            # Its seq stays outstanding until the CANCEL of it is answered,
            # so no ack passes it before
            with self._lock:
                node.unanswered.pop(seq, None)  # canceled instead
                node.lost_reserves.add(seq)
            self._cancel_lost(node)
            return []
        if ops is not None:
            ops.resolved(seq)
        with self._lock:
            node.room, node.reserved = response.room, 0
            if response.status == Status.OK and node.ops is ops:
                for credit in response.credits:
                    node.credits[_credit_key(credit)] = node.conn_gen
        if response.status != Status.OK:
            logger.debug("RESERVE refused by %s: %s", node.url, response.error)
            return []
        return list(response.credits)

    def _cancel_lost(self, node: _MemoryNode) -> None:
        """CANCEL the RESERVEs whose credits never came back."""
        with self._lock:
            if node.conn is None or node.conn.closed or not node.lost_reserves:
                return
            lost = sorted(node.lost_reserves)
            node.lost_reserves.clear()
            requests = [
                self._stamp(node, CancelRequest(PLACEHOLDER_OP, reserve_seq=seq))
                for seq in lost
            ]
        ops = node.ops
        for seq, request in zip(lost, requests, strict=True):

            def canceled(_response: Any, seq: int = seq) -> None:
                if ops is not None:
                    ops.resolved(seq)  # its result is never needed now

            self._send(node, RemoteMessageType.CANCEL, request, canceled)

    def write_done(self, items: list[Written]) -> list[ItemOutcome]:
        """Per item: ADDED, DUPLICATE or REJECTED; raises when the answer of
        a memory node is not known (that op is sent again later)."""
        out: list[ItemOutcome | None] = [None] * len(items)
        groups: dict[str, tuple[_MemoryNode, list[int]]] = {}
        with self._lock:
            for i, item in enumerate(items):
                node = self._node_of(item.location.pool_id)
                if node is None:
                    raise ConnectionError(f"no memory node has {item.location.pool_id}")
                groups.setdefault(node.url, (node, []))[1].append(i)
        for node, idx in groups.values():
            for start in range(0, len(idx), MAX_BATCH):
                part = idx[start : start + MAX_BATCH]
                with self._lock:
                    request = self._stamp(
                        node,
                        RemoteWriteDoneRequest(
                            PLACEHOLDER_OP, [items[i] for i in part]
                        ),
                    )
                response = self._call(node, RemoteMessageType.WRITE_DONE, request)
                if response.status != Status.OK or len(response.items) != len(part):
                    raise ConnectionError(
                        f"WRITE_DONE answered {response.status}: {response.error}"
                    )
                for i, outcome in zip(part, response.items, strict=True):
                    out[i] = outcome
                # Those credits are this run's no longer: room again there
                self._more_room(node, len(part))
        return [o for o in out if o is not None]

    def _more_room(self, node: _MemoryNode, pages: int) -> None:
        """Credits this run held there went back (written or canceled): the
        room it was last told grows by them until the next answer tells."""
        with self._lock:
            node.room += pages * self.page_size

    def cancel(self, locations: list[Location]) -> None:
        with self._lock:
            for location in locations:
                node = self._node_of(location.pool_id)
                if node is not None:
                    node.credits.pop(_credit_key(location), None)
        self._fire(
            locations, lambda part: CancelRequest(PLACEHOLDER_OP, locations=part)
        )

    def drop(self, key: Key, location: Location) -> None:
        self._fire([location], lambda _part: DropRequest(PLACEHOLDER_OP, key, location))

    def _fire(self, locations: list[Location], make: Callable[[list], Any]) -> None:
        """Send without waiting, grouped by memory node; sent again after a
        reconnect if the answer is lost."""
        with self._lock:
            groups: dict[int, tuple[_MemoryNode, list[Location]]] = {}
            for location in locations:
                node = self._node_of(location.pool_id)
                if node is None or node.ops is None:
                    continue
                groups.setdefault(id(node), (node, []))[1].append(location)
            batches = []
            for node, locs in groups.values():
                for start in range(0, len(locs), MAX_BATCH):
                    request = self._stamp(node, make(locs[start : start + MAX_BATCH]))
                    batches.append((node, request))
        for node, request in batches:
            code = (
                RemoteMessageType.DROP
                if isinstance(request, DropRequest)
                else RemoteMessageType.CANCEL
            )
            self._send(node, code, request, functools.partial(self._fired, node))

    def _fired(self, node: _MemoryNode, response: Any) -> None:
        if getattr(response, "status", None) == Status.OK:
            self._more_room(node, getattr(response, "freed", 0))

    def stages(self, node_id: str) -> bool:
        with self._lock:
            return any(
                n.node_id == node_id and STAGE_CAPABILITY in n.capabilities
                for n in self._nodes
            )

    def stage(
        self,
        key: Key,
        source: Location,
        deadline: float,
        callback: Callable[[Staged | None], None],
    ) -> None:
        """STAGE on the owner; ``callback`` once, with the staging page or
        None (a miss, or no answer by ``deadline``: that STAGE is then
        UNSTAGEd as never posted, in case it ran)."""
        request = None
        with self._lock:
            node = self._node_of(source.pool_id)
            remaining_ms = int(max(0.0, deadline - self._clock()) * 1000)
            if (
                node is not None
                and node.ready
                and node.ops is not None
                and not self._full()
                and remaining_ms > 0
            ):
                stage_id = self._next_stage
                self._next_stage += 1
                node_id, epoch = node.node_id, node.node_epoch
                request = self._stamp(
                    node,
                    StageRequest(PLACEHOLDER_OP, stage_id, key, source, remaining_ms),
                )
        if node is None or request is None:
            callback(None)
            return
        once = threading.Lock()

        def finish(staged: Staged | None) -> bool:
            if not once.acquire(blocking=False):
                return False
            callback(staged)
            return True

        def answered(response: Any) -> None:
            staging = getattr(response, "staging", None)
            if getattr(response, "status", None) == Status.OK and staging is not None:
                staged = Staged(node_id, epoch, stage_id, staging)
                if not finish(staged):  # too late: never read
                    self.unstage(staged, TransferEnd.NOT_POSTED)
                return
            if getattr(response, "limit", None) is not None:
                with self._lock:
                    node.room, node.reserved = 0, 0
            finish(None)

        def expired() -> None:
            if finish(None):
                timer.cancel()
                self.unstage(
                    Staged(node_id, epoch, stage_id, source), TransferEnd.NOT_POSTED
                )

        timer = threading.Timer(remaining_ms / 1000.0, expired)
        timer.daemon = True
        timer.start()
        # Its answer lost with the connection: a miss now, as at the deadline
        self._send(node, RemoteMessageType.STAGE, request, answered, lost=expired)

    def unstage(self, staged: Staged, end: TransferEnd) -> None:
        """How the READ of a staging page ended; nothing if its memory run
        is gone (its pages went with it)."""
        with self._lock:
            node = next(
                (
                    n
                    for n in self._nodes
                    if n.node_id == staged.node_id
                    and n.node_epoch == staged.node_epoch
                    and n.ops is not None
                ),
                None,
            )
            if node is None:
                return
            request = self._stamp(
                node, UnstageRequest(PLACEHOLDER_OP, staged.stage_id, end)
            )
        self._send(node, RemoteMessageType.UNSTAGE, request, lambda _r: None)

    # ----------------------------------------------------------------- I/O

    def _usable(self) -> bool:
        with self._lock:
            return not self._drained and not self._stop.is_set() and not self._full()

    def _full(self) -> bool:
        """Caller holds the lock: one more transfer could pass the cap."""
        held = self._kept_bytes + self._running * self.page_size
        return held + self.page_size > self.quarantine_max

    def _fits(self) -> int:
        """Caller holds the lock: the transfers that still fit under the
        cap, less the credits given and not written yet."""
        held = self._kept_bytes + self._running * self.page_size
        given = sum(len(node.credits) for node in self._nodes)
        return (self.quarantine_max - held) // self.page_size - given

    def submit_store(
        self,
        src: Span,
        dst: Location,
        *,
        refs: tuple[PageRef | None, PageRef | None],
        callback: Callable[[IOResult], None] | None = None,
        deadline: float | None = None,
        settled: Callable[[IOResult], None] | None = None,
    ) -> IOResult:
        """WRITE ``src`` to the memory node page ``dst``."""
        return self._post("WRITE", src, dst, refs, callback, deadline, settled)

    def submit_load(
        self,
        src: Location,
        dst: Span,
        *,
        refs: tuple[PageRef | None, PageRef | None],
        callback: Callable[[IOResult], None] | None = None,
        deadline: float | None = None,
        settled: Callable[[IOResult], None] | None = None,
    ) -> IOResult:
        """READ the memory node page ``src`` into ``dst``."""
        return self._post("READ", dst, src, refs, callback, deadline, settled)

    def _post(
        self,
        op: str,
        span: Span,
        remote: Location,
        refs: tuple[PageRef | None, PageRef | None],
        callback: Callable[[IOResult], None] | None,
        deadline: float | None,
        settled: Callable[[IOResult], None] | None,
    ) -> RemoteTransfer:
        assert self.transport is not None and self.exporter is not None
        n = span.nbytes
        with self._lock:
            if self._drained or self._stop.is_set():
                raise RuntimeError("the remote tier is drained")
            if self._full():
                raise RuntimeError("remote quarantine at its cap: no new transfer")
            node = self._node_of(remote.pool_id)
            if node is None or not node.ready:
                raise RuntimeError(f"no ready memory node has {remote.pool_id}")
            if op == "WRITE":
                # Only a credit of this memory run and connection is written
                if node.credits.get(_credit_key(remote)) != node.conn_gen:
                    raise RuntimeError(f"{remote} is no credit of this connection")
                if node.writes >= IO_DEPTH_PER_NODE:
                    raise RuntimeError(f"{IO_DEPTH_PER_NODE} writes run there")
            pool = node.pools[remote.pool_id]
            local = self.exporter.export(span.pool_id)
            if (
                local is None
                or not pool.metadata  # staged, never reached by RDMA
                or not 0 < n <= remote.length
                or span.offset < 0
                or span.offset + n > local.nbytes
                or remote.offset < 0
                or remote.offset + n > pool.capacity
            ):
                raise ValueError(f"no remote I/O for {span} and {remote}")
            pending = self.transport.post(
                op,
                node.peer,
                [(local.base + span.offset, pool.base + remote.offset, n)],
            )
            now = self._clock()
            transfer = RemoteTransfer(
                run=self.env.node_epoch,
                op=op,
                node_id=node.node_id,
                src=refs[0],
                dst=refs[1],
                nbytes=n,
                pending=pending,
                deadline=deadline,
                timeout_at=now + self.timeout_s,
                callback=callback,
                settled=settled,
            )
            self._transfers.append(transfer)
            self._unreported.add(transfer)
            self._running += 1
            if op == "WRITE":
                node.credits.pop(_credit_key(remote), None)
                node.writes += 1
            self._wake.notify_all()
        return transfer

    def _poll_loop(self) -> None:
        while True:
            with self._wake:
                while not self._transfers and not self._stop.is_set():
                    self._wake.wait()
                if not self._transfers:
                    return
                transfers = list(self._transfers)
            moved = False
            for transfer in transfers:
                action = self._advance(transfer)
                if action is not None:
                    moved = True
                    self._report(transfer, action)
            if not moved:
                time.sleep(_POLL_S)

    def _advance(self, transfer: RemoteTransfer) -> str | None:
        """Poll ``transfer`` and move its phase: "end" for its first end
        (DONE, or kept on ERR or past timeout_s), "settle" for a kept one
        that ended DONE; None if nothing is to be reported."""
        with self._lock:
            if transfer.phase not in ("running", "kept"):
                return None
            state = transfer.pending.poll_state(release_disconnected=True)
            transfer.state = state
            if transfer.phase == "kept":
                if state is TransferState.DONE:
                    if not transfer.done.is_set():
                        return None  # its end callback runs first
                    # ``kept`` stays: it tells how the first end went
                    transfer.phase = "settled"
                    self._kept_bytes -= self.page_size
                    self._transfers.remove(transfer)
                    return "settle"
                if state is TransferState.ERR:  # released: nothing more to poll
                    self._transfers.remove(transfer)
                    self._held.append(transfer)
                return None
            now = self._clock()
            if state is TransferState.DONE:
                transfer.phase, transfer.ok = "ended", True
                transfer.late = (
                    transfer.deadline is not None and now > transfer.deadline
                )
                self._transfers.remove(transfer)
                self._running -= transfer.nbytes > 0  # the probe is not counted
                self._write_ended(transfer)
                return "end"
            if state is TransferState.PENDING and now < transfer.timeout_at:
                return None
            transfer.phase, transfer.kept = "kept", True
            self._running -= transfer.nbytes > 0
            self._write_ended(transfer)
            self._kept_bytes += self.page_size
            if state is TransferState.ERR:
                transfer.error = RuntimeError(f"RDMA {transfer.op} ended in an error")
                self._transfers.remove(transfer)
                self._held.append(transfer)
                # Its connection failed: no transfer goes there until made anew
                for node in self._nodes:
                    if node.node_id == transfer.node_id:
                        node.ready = False
            else:
                transfer.error = TimeoutError(
                    f"RDMA {transfer.op} not done in {self.timeout_s} s"
                )
            if self._kept_bytes >= self.quarantine_max:
                logger.error(
                    "remote quarantine reached %d MiB: no new remote transfer",
                    self._kept_bytes >> 20,
                )
            return "end"

    def _write_ended(self, transfer: RemoteTransfer) -> None:
        """Caller holds the lock: a WRITE's slot there is free."""
        if transfer.op == "WRITE" and transfer.nbytes > 0:
            for node in self._nodes:
                if node.node_id == transfer.node_id and node.writes > 0:
                    node.writes -= 1

    def _report(self, transfer: RemoteTransfer, action: str) -> None:
        callback = transfer.settled if action == "settle" else transfer.callback
        if callback is not None:
            try:
                callback(transfer)
            except Exception:
                logger.exception("a remote transfer's callback failed")
        if action == "end":
            transfer.done.set()
            with self._lock:
                self._unreported.discard(transfer)

    def drain(self) -> None:
        """No new transfer; returns once every one posted had its first end
        (DONE, ERR or timeout_s) and its callback ran, also one whose
        callback another thread is running now."""
        with self._lock:
            self._drained = True
            waiting = list(self._unreported)
        for transfer in waiting:
            while not transfer.done.is_set():
                action = self._advance(transfer)
                if action is not None:
                    self._report(transfer, action)
                elif not transfer.done.is_set():
                    transfer.done.wait(_POLL_S)

    @property
    def kept_bytes(self) -> int:
        with self._lock:
            return self._kept_bytes


def _make(tier: TierConfig, env: TierEnv) -> NixlRemoteTier:
    return NixlRemoteTier(tier, env)


# The memory nodes' pools: not this node's, and of their own kinds. The first
# L2 tier: an ssd tier below it takes what it cannot
SPEC = TierSpec(
    store="remote",
    level=Level.L2,
    local=False,
    kind=None,
    keys=frozenset(
        {"store", "url", "rdma_device", "timeout_s", "key_file", "quarantine_max"}
    ),
    common=False,
    gpu_only=True,
    parse=_parse,
    make=_make,
)
