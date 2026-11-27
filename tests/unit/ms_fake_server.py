# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""In-process MaruServer for multi-storage tests.

Keeps a Catalog, pins, sessions and op ledgers by MsService's rules.
Failure injection: pause/resume, drop_responses, drop_requests, delay, restart
(a new server epoch with no state, as after a crash; nodes start new runs).
"""

import collections
import heapq
import itertools
import threading
import time
import uuid
from dataclasses import dataclass, field

import zmq

from maru_common.protocol import (
    MESSAGE_CLASSES,
    MS_MESSAGE_CLASSES,
    AdminDeleteResponse,
    CommitResponse,
    HandshakeResponse,
    LookupAndPinResponse,
    MessageType,
    NodeCloseResponse,
    NodeHeartbeatResponse,
    NodeOpenResponse,
    NodeRecoverListResponse,
    ProbeResponse,
    RecoverItem,
    RetireResponse,
    UnpinResponse,
    WorkerCloseResponse,
    WorkerExitResponse,
    WorkerHeartbeatResponse,
    WorkerOpenResponse,
)
from maru_common.serializer import MessageDecodeError, Serializer, op_digest
from maru_common.storage_types import (
    MAX_BATCH,
    MEMORY_CAPABILITY,
    MS_PROTOCOL,
    POOL_KINDS,
    REMOTE_CAPABILITY,
    WORKER_RDMA_CAPABILITY,
    AccessHints,
    BlockRequest,
    CloseMode,
    ItemOutcome,
    ItemResult,
    KeptCause,
    Key,
    KeyLocation,
    Location,
    MissReason,
    PoolInfo,
    PoolKind,
    SessionRole,
    Status,
    WorkerRef,
    generation_epoch_seq,
)
from maru_server.ms_service import result_bytes  # MsService's op_bytes_max estimate

OP_CODES = {
    MessageType.LOOKUP_AND_PIN,
    MessageType.COMMIT,
    MessageType.RETIRE,
    MessageType.WORKER_EXIT,
}
NODE_CODES = {
    MessageType.NODE_HEARTBEAT,
    MessageType.NODE_CLOSE,
    MessageType.NODE_RECOVER_LIST,
    MessageType.WORKER_EXIT,
    MessageType.COMMIT,
    MessageType.RETIRE,
}
WORKER_CODES = {
    MessageType.WORKER_HEARTBEAT,
    MessageType.WORKER_CLOSE,
    MessageType.PROBE,
    MessageType.LOOKUP_AND_PIN,
    MessageType.UNPIN,
}


def loc_id(location: Location) -> tuple[str, str, int]:
    return (location.pool_id, location.allocation_id, location.generation)


# The node capabilities MaruServer knows
NODE_CAPABILITIES = frozenset(
    {REMOTE_CAPABILITY, MEMORY_CAPABILITY, WORKER_RDMA_CAPABILITY}
)


@dataclass
class NodeRec:
    node_id: str
    node_epoch: str
    session: str
    epoch_seq: int
    pools: dict[str, PoolInfo]
    last_seen: float
    online: bool = True
    usage: list = field(default_factory=list)
    heartbeats: int = 0
    capabilities: list[str] = field(default_factory=list)  # accepted
    memory_peer: tuple[str, str] | None = None


@dataclass
class WorkerRec:
    token: str
    incarnation: str
    node_id: str
    node_epoch: str
    engine_id: str
    dp_index: int
    tp_rank: int
    pid: int
    pid_start: int
    namespace: str
    role: SessionRole
    last_seen: float
    open: bool = True

    def ref(self) -> WorkerRef:
        return WorkerRef(
            engine_id=self.engine_id,
            dp_index=self.dp_index,
            tp_rank=self.tp_rank,
            pid=self.pid,
            pid_start=self.pid_start,
            incarnation=self.incarnation,
            worker_session=self.token,
        )


@dataclass
class Entry:
    key: Key
    location: Location
    logical_bytes: int
    node_id: str


@dataclass
class Pin:
    pin_id: str
    # "worker:<incarnation>" or "node:<node_id>"
    owner: str
    key: Key
    location: Location


@dataclass
class _OriginLog:
    ack: int = 0
    entries: dict[int, tuple[bytes, object, int]] = field(default_factory=dict)
    bytes: int = 0


class FakeMaruServer:
    def __init__(
        self,
        *,
        heartbeat_s: float = 0.2,
        session_ttl_s: float = 60.0,
        op_window: int = 65536,
        op_bytes_max: int = 64 << 20,
        recover_page: int = 2,
        capabilities: frozenset[str] = NODE_CAPABILITIES,
    ):
        """``capabilities``: the node capabilities NODE_OPEN accepts."""
        self.capabilities = capabilities
        self.heartbeat_s = heartbeat_s
        self.session_ttl_s = session_ttl_s
        self.op_window = op_window
        self.op_bytes_max = op_bytes_max
        self.recover_page = recover_page
        # The legacy HANDSHAKE too (node session renewal asks it)
        self.serializer = Serializer(MESSAGE_CLASSES, MS_MESSAGE_CLASSES)
        self._lock = threading.RLock()
        self._ctx = zmq.Context()
        self._sock = self._ctx.socket(zmq.ROUTER)
        self._sock.setsockopt(zmq.LINGER, 0)
        port = self._sock.bind_to_random_port("tcp://127.0.0.1")
        self.url = f"tcp://127.0.0.1:{port}"
        self._running = True
        self._paused: str | None = None  # None, "hold" or "drop"
        self._held: list[list[bytes]] = []
        self._drop_responses: collections.Counter = collections.Counter()
        self._drop_requests: collections.Counter = collections.Counter()
        self._delays: dict[int, float] = {}
        self._fail: dict[int, list] = collections.defaultdict(list)
        self._rewrite: dict[int, list] = collections.defaultdict(list)
        self._later: list[tuple[float, int, list[bytes]]] = []
        self._later_seq = itertools.count()
        self._reset_state()
        # Persistent across restarts (inspection)
        self.log: list[tuple[int, object]] = []
        self._thread = threading.Thread(
            target=self._loop, name="fake-maru-server", daemon=True
        )
        self._thread.start()

    # ---------------------------------------------------------------- state

    def _reset_state(self) -> None:
        self.server_epoch = uuid.uuid4().hex
        self.nodes: dict[str, NodeRec] = {}
        self.node_sessions: dict[str, str] = {}  # token -> node_id
        self.epoch_seqs: dict[str, int] = {}
        self.workers: dict[str, WorkerRec] = {}  # token -> rec
        self.by_incarnation: dict[str, str] = {}  # incarnation -> token
        self.catalog: dict[Key, list[Entry]] = {}
        self.by_loc: dict[tuple, Entry] = {}
        self.pins: dict[str, Pin] = {}
        self.loc_pins: dict[tuple, set[str]] = collections.defaultdict(set)
        self.preserved: set[Key] = set()
        self.hidden_pools: set[str] = set()
        self.ledgers: dict[str, _OriginLog] = {}
        # (pool_id, offset, generation) read per node, least recent first
        self.access: dict[str, dict[tuple, None]] = collections.defaultdict(dict)
        self.blocks: dict[str, list[BlockRequest]] = collections.defaultdict(list)
        self.ended_epochs: set[str] = set()
        self.adopted_by: dict[str, list[PoolInfo]] = {}
        self.adopt: dict[str, list[tuple[PoolInfo, list[RecoverItem]]]] = {}
        self.recover_items: dict[str, list[RecoverItem]] = {}
        self.worker_exits: list[tuple[WorkerRef, str]] = []
        self.executed: collections.Counter = collections.Counter()
        self.replayed: collections.Counter = collections.Counter()

    # ------------------------------------------------------ failure injection

    def pause(self, hold: bool = True) -> None:
        """Stop answering; held requests run on resume (like SIGSTOP)."""
        with self._lock:
            self._paused = "hold" if hold else "drop"

    def resume(self) -> None:
        with self._lock:
            self._paused = None
            held, self._held = self._held, []
        for frames in held:
            self._handle(frames)

    def drop_responses(self, code: MessageType, count: int = 1) -> None:
        """Run the next ``count`` requests of ``code``, losing the replies."""
        with self._lock:
            self._drop_responses[int(code)] += count

    def drop_requests(self, code: MessageType, count: int = 1) -> None:
        with self._lock:
            self._drop_requests[int(code)] += count

    def fail_requests(self, code: MessageType, status: Status, count: int = 1) -> None:
        """Answer the next ``count`` requests of ``code`` with ``status``, unrun."""
        with self._lock:
            self._fail[int(code)].extend([status] * count)

    def rewrite_responses(self, code: MessageType, fn, count: int = 1) -> None:
        """Run the next ``count`` requests of ``code``, then reply ``fn(response)``."""
        with self._lock:
            self._rewrite[int(code)].extend([fn] * count)

    def delay(self, code: MessageType, seconds: float) -> None:
        with self._lock:
            self._delays[int(code)] = seconds

    def restart(self) -> None:
        """New server_epoch, no state."""
        with self._lock:
            self._reset_state()

    def close(self) -> None:
        self._running = False
        self._thread.join(timeout=5.0)
        self._sock.close()
        self._ctx.term()

    # -------------------------------------------------------------- admin

    def preserve(self, key: Key) -> None:
        with self._lock:
            self.preserved.add(key)

    def set_adopted(
        self, node_id: str, pools: list[tuple[PoolInfo, list[RecoverItem]]]
    ) -> None:
        """Pools the next NODE_OPEN of ``node_id`` adopts."""
        with self._lock:
            self.adopt[node_id] = pools

    def queue_block(self, node_id: str, block: BlockRequest) -> None:
        with self._lock:
            self.blocks[node_id].append(block)

    def keys(self) -> set[Key]:
        with self._lock:
            return {k for k, v in self.catalog.items() if v}

    def locations(self, key: Key) -> list[Location]:
        with self._lock:
            return [e.location for e in self.catalog.get(key, [])]

    def pin_count(self, owner_incarnation: str | None = None) -> int:
        with self._lock:
            if owner_incarnation is None:
                return len(self.pins)
            return sum(
                1
                for p in self.pins.values()
                if p.owner == f"worker:{owner_incarnation}"
            )

    def worker_session_of(self, incarnation: str) -> WorkerRec | None:
        with self._lock:
            token = self.by_incarnation.get(incarnation)
            return self.workers.get(token) if token else None

    def node(self, node_id: str) -> NodeRec | None:
        with self._lock:
            return self.nodes.get(node_id)

    def requests(self, code: MessageType) -> list:
        with self._lock:
            return [r for c, r in self.log if c == int(code)]

    # --------------------------------------------------------------- loop

    def _loop(self) -> None:
        poller = zmq.Poller()
        poller.register(self._sock, zmq.POLLIN)
        while self._running:
            events = dict(poller.poll(10))
            if self._sock in events:
                while True:
                    try:
                        frames = self._sock.recv_multipart(zmq.NOBLOCK)
                    except zmq.Again:
                        break
                    with self._lock:
                        paused = self._paused
                        if paused == "hold":
                            self._held.append(frames)
                            continue
                    if paused == "drop":
                        continue
                    self._handle(frames)
            now = time.monotonic()
            while self._later and self._later[0][0] <= now:
                _, _, reply = heapq.heappop(self._later)
                self._sock.send_multipart(reply)

    def _handle(self, frames: list[bytes]) -> None:
        identity, frame = frames[0], frames[-1]
        try:
            decoded = self.serializer.decode_request_payload(frame)
        except MessageDecodeError as e:
            if e.header is None:
                return
            reply = self.serializer.encode_response(
                e.header, {"status": Status.REJECTED.value, "error": str(e)}
            )
            self._sock.send_multipart([identity, b"", reply])
            return
        code = decoded.header.msg_type
        with self._lock:
            if self._drop_requests[code] > 0:
                self._drop_requests[code] -= 1
                return
            self.log.append((code, decoded.request))
            if self._fail[code]:
                _, response_cls = MESSAGE_CLASSES[code]
                response = response_cls(
                    status=self._fail[code].pop(0), error="injected"
                )
            else:
                response = self._dispatch(code, decoded)
                if self._rewrite[code]:
                    response = self._rewrite[code].pop(0)(response)
            drop = self._drop_responses[code] > 0
            if drop:
                self._drop_responses[code] -= 1
            delay = self._delays.get(code, 0.0)
        if drop:
            return
        reply = [
            identity,
            b"",
            self.serializer.encode_response(decoded.header, response),
        ]
        if delay > 0:
            heapq.heappush(
                self._later, (time.monotonic() + delay, next(self._later_seq), reply)
            )
        else:
            self._sock.send_multipart(reply)

    # ----------------------------------------------------------- dispatch

    def _dispatch(self, code: int, decoded):
        request = decoded.request
        if code == MessageType.HANDSHAKE:
            return HandshakeResponse(success=True, server_epoch=self.server_epoch)
        if code == MessageType.ADMIN_DELETE:
            return self._admin_delete(request)  # no session
        _, response_cls = MS_MESSAGE_CLASSES[code]
        now = time.monotonic()
        self._expire(now)
        if code == MessageType.NODE_OPEN:
            return self._node_open(request, now)
        if code == MessageType.WORKER_OPEN:
            return self._worker_open(request, now)
        session = getattr(request, "session", None)
        if request.server_epoch != self.server_epoch:
            return response_cls(status=Status.EPOCH, error="server_epoch")
        if code in NODE_CODES:
            node_id = self.node_sessions.get(session)
            if node_id is None:
                return response_cls(status=Status.EPOCH, error="no such node session")
            node = self.nodes[node_id]
            node.last_seen = now
            actor = node
            origin = node.node_epoch
        elif code in WORKER_CODES:
            worker = self.workers.get(session)
            if worker is None or not worker.open:
                return response_cls(status=Status.EPOCH, error="no such worker session")
            worker.last_seen = now
            actor = worker
            origin = worker.incarnation
            if worker.role == SessionRole.PROBE and code in (
                MessageType.LOOKUP_AND_PIN,
                MessageType.UNPIN,
            ):
                return response_cls(status=Status.REJECTED, error="probe session")
            node = self.nodes.get(worker.node_id)
            if (
                code in (MessageType.PROBE, MessageType.LOOKUP_AND_PIN)
                and worker.role == SessionRole.WORKER
                and (node is None or node.node_epoch != worker.node_epoch)
            ):
                # Opened on an older node run: attach again and reopen
                return response_cls(status=Status.EPOCH, error="stale node epoch")
        else:
            return response_cls(status=Status.DISABLED, error="not in the fake")
        if len(getattr(request, "items", ())) > MAX_BATCH:
            return response_cls(status=Status.REJECTED, error="batch too large")
        handler = getattr(self, f"_{MessageType(code).name.lower()}")
        if code not in OP_CODES:
            return handler(actor, request)
        # Op rules
        op = request.op
        digest = op_digest(decoded.payload)
        if op.origin != origin or op.digest != digest or op.seq < 1 or op.ack >= op.seq:
            return response_cls(status=Status.REJECTED, error="bad op")
        log = self.ledgers.setdefault(origin, _OriginLog())
        if op.ack > log.ack:
            log.ack = op.ack
            for seq in [s for s in log.entries if s <= log.ack]:
                log.bytes -= log.entries.pop(seq)[2]
        if op.seq <= log.ack:
            items = getattr(request, "items", None)
            count = len(items if items is not None else request.workers)
            return response_cls(
                status=Status.OK, items=[ItemOutcome(ItemResult.STALE)] * count
            )
        recorded = log.entries.get(op.seq)
        if recorded is not None:
            if recorded[0] != digest:
                return response_cls(status=Status.REJECTED, error="digest conflict")
            self.replayed[code] += 1
            return recorded[1]
        if len(log.entries) >= self.op_window or log.bytes >= self.op_bytes_max:
            return response_cls(status=Status.BUSY, error="op window full")
        response = handler(actor, request)
        self.executed[code] += 1
        # Every run is recorded, so a resend never applies part of it again
        size = result_bytes(response)
        log.entries[op.seq] = (digest, response, size)
        log.bytes += size
        return response

    def _expire(self, now: float) -> None:
        for token, node_id in list(self.node_sessions.items()):
            node = self.nodes[node_id]
            if now - node.last_seen > self.session_ttl_s:
                del self.node_sessions[token]
                node.online = False
        held = {p.owner for p in self.pins.values()}
        for worker in list(self.workers.values()):
            if now - worker.last_seen <= self.session_ttl_s:
                continue
            worker.open = False  # pins stay
            if f"worker:{worker.incarnation}" not in held:
                # Expired and pinless, it goes with its ops
                del self.workers[worker.token]
                if self.by_incarnation.get(worker.incarnation) == worker.token:
                    del self.by_incarnation[worker.incarnation]
                    if worker.role == SessionRole.WORKER:
                        self.ledgers.pop(worker.incarnation, None)

    # --------------------------------------------------------- node session

    def _node_open(self, req, now):
        if req.protocol != MS_PROTOCOL:
            return NodeOpenResponse(status=Status.REJECTED, error="protocol")
        if req.node_epoch in self.ended_epochs:
            return NodeOpenResponse(status=Status.REJECTED, error="node epoch ended")
        domains = {d.rm_domain for d in req.rm_domains}
        for pool in req.pools:
            ok_id = pool.pool_id == f"dram:{req.node_id}:{req.node_epoch}" or (
                pool.kind == PoolKind.CXL
                and pool.rm_domain in domains
                and pool.region_id is not None
                and pool.pool_id == f"cxl:{pool.rm_domain}:{pool.region_id}"
            )
            if (
                not ok_id
                or pool.pool_generation < 1
                or not 0 < pool.page_size <= pool.capacity
            ):
                return NodeOpenResponse(
                    status=Status.REJECTED, error=f"pool {pool.pool_id}"
                )
        old = self.nodes.get(req.node_id)
        if old is not None and old.node_epoch == req.node_epoch:
            # Renewal after expiry: same epoch_seq, new token
            self.node_sessions.pop(old.session, None)
            old.session = uuid.uuid4().hex
            old.last_seen = now
            old.online = True
            old.memory_peer = None
            self.node_sessions[old.session] = req.node_id
            return self._open_response(old, list(self._adopted_of(old)))
        if old is not None:
            self.ended_epochs.add(old.node_epoch)
            self.node_sessions.pop(old.session, None)
            if old.node_epoch != req.node_epoch:
                # The old run's node pins go; its DRAM
                # locations are hidden
                for pin in [
                    p for p in self.pins.values() if p.owner == f"node:{req.node_id}"
                ]:
                    self._drop_pin(pin.pin_id)
                for pool in old.pools.values():
                    if pool.kind == PoolKind.DRAM:
                        self.hidden_pools.add(pool.pool_id)
                self._drop_hidden_unpinned()
        seq = self.epoch_seqs.get(req.node_id, 0) + 1
        self.epoch_seqs[req.node_id] = seq
        token = uuid.uuid4().hex
        adopted = self.adopt.pop(req.node_id, [])
        pools = {p.pool_id: p for p in req.pools}
        for info, items in adopted:
            pools[info.pool_id] = info
            self.recover_items[info.pool_id] = list(items)
        self.nodes[req.node_id] = NodeRec(
            node_id=req.node_id,
            node_epoch=req.node_epoch,
            session=token,
            epoch_seq=seq,
            pools=pools,
            last_seen=now,
            capabilities=[c for c in req.capabilities if c in self.capabilities],
        )
        self.node_sessions[token] = req.node_id
        self.adopted_by[req.node_id] = [info for info, _ in adopted]
        return self._open_response(
            self.nodes[req.node_id], self.adopted_by[req.node_id]
        )

    def _adopted_of(self, node: NodeRec):
        return self.adopted_by.get(node.node_id, [])

    def _open_response(self, node: NodeRec, adopted) -> NodeOpenResponse:
        return NodeOpenResponse(
            status=Status.OK,
            server_epoch=self.server_epoch,
            node_session=node.session,
            epoch_seq=node.epoch_seq,
            heartbeat_s=self.heartbeat_s,
            session_ttl_s=self.session_ttl_s,
            adopted=adopted,
            capabilities=list(node.capabilities),
            pool_registrations={
                p: node.epoch_seq * 1000 + i for i, p in enumerate(node.pools, 1)
            },
        )

    def _node_heartbeat(self, node: NodeRec, req):
        node.memory_peer = req.memory_peer
        node.usage = list(req.usage)
        node.heartbeats += 1
        # A block is sent again every heartbeat until its items are gone
        self.blocks[node.node_id] = [
            b
            for b in self.blocks[node.node_id]
            if any(
                (e := self.by_loc.get(loc_id(i.location))) is not None
                and e.key == i.key
                for i in b.items
            )
        ]
        return NodeHeartbeatResponse(
            status=Status.OK,
            access=AccessHints.pack(list(self.access.pop(node.node_id, {}))),
            blocks=list(self.blocks[node.node_id]),
            policy_version=0,
        )

    def _admin_delete(self, req):
        items = []
        for key in req.items:
            if key in self.preserved:
                items.append(ItemOutcome(ItemResult.KEPT, cause=KeptCause.PINNED))
                continue
            # Only ACTIVE pools of nodes whose run goes on can block
            entries = self.catalog.get(key, [])
            queued = False
            for entry in entries:
                node = self.nodes.get(entry.node_id)
                if (
                    entry.location.pool_id in self.hidden_pools
                    or node is None
                    or node.node_epoch in self.ended_epochs
                ):
                    continue
                queued = True
                self.queue_block(
                    entry.node_id,
                    BlockRequest(
                        block_id=uuid.uuid4().hex,
                        items=(KeyLocation(entry.key, entry.location),),
                        reason="delete",
                    ),
                )
            if queued:
                items.append(ItemOutcome(ItemResult.OK))
            else:
                reason = MissReason.OFFLINE if entries else MissReason.ABSENT
                items.append(ItemOutcome(ItemResult.MISS, reason=reason))
        return AdminDeleteResponse(status=Status.OK, items=items)

    def _node_close(self, node: NodeRec, req):
        self.ended_epochs.add(node.node_epoch)
        self.node_sessions.pop(node.session, None)
        node.online = False
        for pool in node.pools.values():
            if req.mode == CloseMode.LEAVE:
                for entry in [
                    e
                    for e in self.by_loc.values()
                    if e.location.pool_id == pool.pool_id
                ]:
                    self._remove(entry)
            elif pool.kind == PoolKind.DRAM:
                self.hidden_pools.add(pool.pool_id)
        return NodeCloseResponse(status=Status.OK)

    def _node_recover_list(self, node: NodeRec, req):
        items = self.recover_items.get(req.pool_id)
        if items is None:
            return NodeRecoverListResponse(status=Status.REJECTED, error="not adopted")
        start = int(req.cursor or 0)
        page = items[start : start + self.recover_page]
        end = start + len(page)
        return NodeRecoverListResponse(
            status=Status.OK,
            items=page,
            next_cursor=str(end) if end < len(items) else None,
        )

    def _worker_exit(self, node: NodeRec, req):
        items = []
        for ref in req.workers:
            token = self.by_incarnation.get(ref.incarnation)
            if token is not None:
                w = self.workers[token]
                if w.node_id != node.node_id or (w.pid, w.pid_start) != (
                    ref.pid,
                    ref.pid_start,
                ):
                    items.append(ItemOutcome(ItemResult.REJECTED))
                    continue
                w.open = False
                del self.by_incarnation[ref.incarnation]
            self.worker_exits.append((ref, req.cause.value))
            self._release_worker(ref.incarnation)
            items.append(ItemOutcome(ItemResult.OK))
        return WorkerExitResponse(status=Status.OK, items=items)

    # ------------------------------------------------------- worker session

    def _worker_open(self, req, now):
        if req.protocol != MS_PROTOCOL:
            return WorkerOpenResponse(status=Status.REJECTED, error="protocol")
        node = self.nodes.get(req.node_id)
        # A probe answers for its node, whichever run it attached to
        if req.role == SessionRole.WORKER and (
            node is None or node.node_epoch != req.node_epoch
        ):
            return WorkerOpenResponse(
                status=Status.EPOCH, error="node epoch", server_epoch=self.server_epoch
            )
        old = self.by_incarnation.get(req.incarnation)
        if old is not None:
            w = self.workers[old]
            # Identity: node, engine, ranks, pid and start time, namespace
            if (
                w.node_id,
                w.engine_id,
                w.dp_index,
                w.tp_rank,
                w.pid,
                w.pid_start,
                w.namespace,
            ) != (
                req.node_id,
                req.engine_id,
                req.dp_index,
                req.tp_rank,
                req.pid,
                req.pid_start,
                req.namespace,
            ):
                return WorkerOpenResponse(
                    status=Status.REJECTED, error="incarnation of another identity"
                )
            w.open = False  # pins move with the incarnation
        token = uuid.uuid4().hex
        self.workers[token] = WorkerRec(
            token=token,
            incarnation=req.incarnation,
            node_id=req.node_id,
            node_epoch=req.node_epoch,
            engine_id=req.engine_id,
            dp_index=req.dp_index,
            tp_rank=req.tp_rank,
            pid=req.pid,
            pid_start=req.pid_start,
            namespace=req.namespace,
            role=req.role,
            last_seen=now,
        )
        self.by_incarnation[req.incarnation] = token
        return WorkerOpenResponse(
            status=Status.OK,
            server_epoch=self.server_epoch,
            worker_session=token,
            heartbeat_s=self.heartbeat_s,
            session_ttl_s=self.session_ttl_s,
        )

    def _worker_heartbeat(self, worker: WorkerRec, req):
        return WorkerHeartbeatResponse(status=Status.OK)

    def _worker_close(self, worker: WorkerRec, req):
        self._release_worker(worker.incarnation)
        worker.open = False
        if self.by_incarnation.get(worker.incarnation) == worker.token:
            del self.by_incarnation[worker.incarnation]
        return WorkerCloseResponse(status=Status.OK)

    # ------------------------------------------------------------ lookups

    def _visible(self, entry: Entry, worker: WorkerRec) -> tuple[str, Entry | None]:
        if entry.location.pool_id in self.hidden_pools:
            return "hidden", None
        node = self.nodes.get(entry.node_id)
        mappable = (
            entry.node_id == worker.node_id
            and node is not None
            and (
                worker.role == SessionRole.PROBE or worker.node_epoch == node.node_epoch
            )
            and entry.location.kind in (PoolKind.DRAM, PoolKind.CXL)
        )
        if mappable:
            return "hit", entry
        requester = self.nodes.get(worker.node_id)
        fetchable = (
            requester is not None
            and WORKER_RDMA_CAPABILITY in requester.capabilities
            and node is not None
            and requester.memory_peer == (node.node_id, node.node_epoch)
            and MEMORY_CAPABILITY in node.capabilities
            and POOL_KINDS[entry.location.kind].mappable
        )
        if not fetchable or node is None or not node.online or not requester.online:
            return "offline", None
        return "hit", entry

    def _find(self, key: Key, worker: WorkerRec):
        offline = False
        for entry in self.catalog.get(key, []):
            how, found = self._visible(entry, worker)
            if how == "hit":
                return ItemResult.HIT, found
            if how in ("offline", "hidden"):
                offline = True
        if offline:
            return ItemResult.MISS, MissReason.OFFLINE
        return ItemResult.MISS, MissReason.ABSENT

    def _probe(self, worker: WorkerRec, req):
        items = []
        for key in req.items:
            result, found = self._find(key, worker)
            if result == ItemResult.MISS:
                items.append(ItemOutcome(ItemResult.MISS, reason=found))
            else:
                items.append(
                    ItemOutcome(
                        result, location=found.location if req.with_locations else None
                    )
                )
        return ProbeResponse(status=Status.OK, items=items)

    def _lookup_and_pin(self, worker: WorkerRec, req):
        items = []
        for key in req.items:
            result, found = self._find(key, worker)
            if result == ItemResult.HIT:
                pin_id = uuid.uuid4().hex
                self.pins[pin_id] = Pin(
                    pin_id, f"worker:{worker.incarnation}", key, found.location
                )
                self.loc_pins[loc_id(found.location)].add(pin_id)
                # As MaruServer: the owning node's most recent read
                loc = found.location
                target = (loc.pool_id, loc.offset, loc.generation)
                self.access[found.node_id].pop(target, None)
                self.access[found.node_id][target] = None
                items.append(
                    ItemOutcome(ItemResult.HIT, location=found.location, pin_id=pin_id)
                )
            else:
                items.append(ItemOutcome(ItemResult.MISS, reason=found))
        return LookupAndPinResponse(status=Status.OK, items=items)

    def _drop_pin(self, pin_id: str) -> None:
        pin = self.pins.pop(pin_id, None)
        if pin is not None:
            self.loc_pins[loc_id(pin.location)].discard(pin_id)
            self._drop_hidden_unpinned()

    def _unpin(self, worker: WorkerRec, req):
        items = []
        for pin_id in req.pin_ids:
            pin = self.pins.get(pin_id)
            if pin is not None and pin.owner != f"worker:{worker.incarnation}":
                items.append(ItemOutcome(ItemResult.REJECTED))  # another session's
                continue
            self._drop_pin(pin_id)
            items.append(ItemOutcome(ItemResult.OK))  # unknown pins are OK too
        return UnpinResponse(status=Status.OK, items=items)

    def _release_worker(self, incarnation: str) -> None:
        for pin in [
            p for p in self.pins.values() if p.owner == f"worker:{incarnation}"
        ]:
            self._drop_pin(pin.pin_id)

    def _drop_hidden_unpinned(self) -> None:
        for entry in [
            e for e in self.by_loc.values() if e.location.pool_id in self.hidden_pools
        ]:
            if not self.loc_pins.get(loc_id(entry.location)):
                self._remove(entry)

    # ---------------------------------------------------- publish, remove

    def _commit(self, node: NodeRec, req):
        items = []
        for item in req.items:
            loc = item.location
            pool = node.pools.get(loc.pool_id)
            given_by = generation_epoch_seq(loc.generation)
            if (
                pool is None
                or loc.pool_id in self.hidden_pools
                or loc.kind != pool.kind
                or not loc.allocation_id
                or not 0 < item.logical_bytes <= loc.length
                or loc.offset < 0
                or loc.offset + loc.length > pool.capacity
                or given_by != node.epoch_seq
            ):
                items.append(ItemOutcome(ItemResult.REJECTED))
                continue
            # As Catalog.commit: the key's location in this pool first (the very
            # same one is ADDED again), then any page holder REJECTS
            same_pool = [
                e
                for e in self.catalog.get(item.key, [])
                if e.location.pool_id == loc.pool_id
            ]
            if same_pool and same_pool[0].location != loc:
                items.append(
                    ItemOutcome(ItemResult.DUPLICATE, location=same_pool[0].location)
                )
                continue
            if same_pool:  # the same location again
                items.append(ItemOutcome(ItemResult.ADDED))
                continue
            if self._page_holder(loc) is not None:
                items.append(ItemOutcome(ItemResult.REJECTED))
                continue
            entry = Entry(item.key, loc, item.logical_bytes, node.node_id)
            self.catalog.setdefault(item.key, []).append(entry)
            self.by_loc[loc_id(loc)] = entry
            items.append(ItemOutcome(ItemResult.ADDED))
        return CommitResponse(status=Status.OK, items=items)

    def _retire(self, node: NodeRec, req):
        items = []
        for kl in req.items:
            loc = kl.location
            if loc.pool_id not in node.pools or loc.pool_id in self.hidden_pools:
                items.append(ItemOutcome(ItemResult.KEPT, cause=KeptCause.INVALID))
                continue
            entry = self.by_loc.get(loc_id(loc))
            if entry is None or entry.key != kl.key or entry.location != loc:
                # MOVED only when another published location (another key,
                # or this key at another generation) occupies the page
                occupant = self._page_holder(loc)
                items.append(
                    ItemOutcome(
                        ItemResult.KEPT,
                        cause=KeptCause.MOVED,
                        location=occupant.location,
                        key=occupant.key,
                    )
                    if occupant is not None
                    else ItemOutcome(ItemResult.RETIRED)
                )
                continue
            if self.loc_pins.get(loc_id(loc)) or kl.key in self.preserved:
                items.append(ItemOutcome(ItemResult.KEPT, cause=KeptCause.PINNED))
                continue
            self._remove(entry)
            items.append(ItemOutcome(ItemResult.RETIRED))
        return RetireResponse(status=Status.OK, items=items)

    def _page_holder(self, loc: Location) -> Entry | None:
        """A live location with this allocation_id, else one overlapping these
        bytes."""
        in_pool = [e for e in self.by_loc.values() if e.location.pool_id == loc.pool_id]
        for e in in_pool:
            if e.location.allocation_id == loc.allocation_id:
                return e
        for e in in_pool:
            if (
                e.location.offset < loc.offset + loc.length
                and loc.offset < e.location.offset + e.location.length
            ):
                return e
        return None

    def _remove(self, entry: Entry) -> None:
        self.by_loc.pop(loc_id(entry.location), None)
        entries = self.catalog.get(entry.key, [])
        if entry in entries:
            entries.remove(entry)
        if not entries:
            self.catalog.pop(entry.key, None)
        for pin_id in list(self.loc_pins.pop(loc_id(entry.location), ())):
            self.pins.pop(pin_id, None)
