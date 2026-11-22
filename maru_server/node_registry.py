# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Node and worker sessions, node restarts, and what NODE_HEARTBEAT replies
carry (access info, block requests).

Not thread safe: callers hold the service lock.
"""

import heapq
import itertools
import logging
import re
import time
import uuid
from collections import OrderedDict, deque
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from operator import itemgetter

from maru_common.protocol import (
    NodeHeartbeatRequest,
    NodeHeartbeatResponse,
    NodeOpenRequest,
    NodeOpenResponse,
    NodeOrphansResponse,
    NodeRecoverListResponse,
    RecoverItem,
    WorkerOpenRequest,
    WorkerOpenResponse,
)
from maru_common.storage_types import (
    EPOCH_SEQ_MAX,
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
    OrphanWorker,
    PoolInfo,
    PoolKind,
    PoolUsage,
    SessionRole,
    Status,
    WorkerRef,
)

from .catalog import Catalog, Entry, PoolRecord, PoolState, Requester
from .op_ledger import OpLedger, OriginKey
from .pin_manager import PinManager

logger = logging.getLogger(__name__)

NODE_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")
NODE_CAPABILITIES = frozenset(
    {REMOTE_CAPABILITY, MEMORY_CAPABILITY, WORKER_RDMA_CAPABILITY}
)
# ended node_epochs kept per node, so a late old process cannot come back
RETIRED_EPOCHS_KEPT = 4096
# pages read kept per node (the first queued go first), and sent per heartbeat
ACCESS_MAX_PER_NODE = 1 << 18
ACCESS_PER_HEARTBEAT = 1 << 14
# block requests: items per block, blocks per heartbeat reply
BLOCK_ITEMS = 256
BLOCKS_PER_HEARTBEAT = 4
# pins released inline by a request; the rest by the maintenance thread
PINS_INLINE = MAX_BATCH
# Entries of an ended run's maps, sets and listings freed per unit of
# maintenance work, measured so a unit costs about what the other units do
# (~1.5 us): one maintain() hold at the default budget stays under ~1 ms
ACCESS_FREES_PER_UNIT = 4
LISTING_FREES_PER_UNIT = 8
BLOCKED_FREES_PER_UNIT = 3  # each frees its key and location too
BLOCK_ITEM_FREES_PER_UNIT = 16  # their keys and locations go with blocked
BLOCK_FREES_PER_UNIT = 1  # its items are queued (copied) and freed next


def node_origin(node_epoch: str) -> OriginKey:
    return ("node", node_epoch)


def worker_origin(incarnation: str) -> OriginKey:
    return ("worker", incarnation)


class OpenError(Exception):
    """NODE_OPEN or WORKER_OPEN refused with ``status``."""

    def __init__(self, status: Status, message: str):
        super().__init__(message)
        self.status = status


@dataclass(eq=False)
class NodeSession:
    token: str
    node_id: str
    node_epoch: str
    epoch_seq: int
    host_boot_id: str
    rm_domains: dict[str, frozenset[str]]  # device_ids by rm_domain
    deadline: float
    pid_ns: int = 0
    orphans: tuple[OrphanWorker, ...] = ()
    capabilities: tuple[str, ...] = ()
    closed_at: float | None = None  # NODE_CLOSE restart
    ended: bool = False  # renewed, replaced, left or given up

    def valid(self, now: float) -> bool:
        return not self.ended and self.closed_at is None and now < self.deadline

    @property
    def creator(self) -> tuple[str, str]:
        return (self.node_id, self.node_epoch)


@dataclass(eq=False)
class _Listing:
    pool_id: str
    entries: list[Entry]
    # the last page, kept after the snapshot is released so a resend matches
    last: tuple[str, NodeRecoverListResponse] | None = None


@dataclass(eq=False)
class _Block:
    block_id: str
    items: tuple[KeyLocation, ...]
    reason: str


@dataclass(eq=False)
class NodeRecord:
    node_id: str
    host_boot_id: str = ""
    pid_ns: int = 0
    worker_rdma: bool = False
    epoch_seq: int = 0  # the last one given
    node_epoch: str | None = None  # of the latest run
    session: NodeSession | None = None  # of the latest run, while it lasts
    retired_epochs: deque[str] = field(
        default_factory=lambda: deque(maxlen=RETIRED_EPOCHS_KEPT)
    )
    gone: bool = False  # left or given up: no locations, no session
    adopted: list[str] = field(default_factory=list)
    listings: dict[str, _Listing] = field(default_factory=dict)  # by listing id
    listing_ids: Iterator[int] = field(default_factory=lambda: itertools.count(1))
    usage: dict[str, PoolUsage] = field(default_factory=dict)
    blocks: OrderedDict[str, _Block] = field(default_factory=OrderedDict)
    blocked: set[tuple[Key, Location]] = field(default_factory=set)
    # worker sessions of a left or given up run, still to drop (maintain)
    dropping: set[str] = field(default_factory=set)


@dataclass(eq=False)
class WorkerSession:
    token: str
    incarnation: str
    role: SessionRole
    node_id: str
    node_epoch: str
    node_epoch_seq: int  # which pools the worker maps (worker role)
    engine_id: str
    dp_index: int
    tp_rank: int
    pid: int
    pid_start: int
    namespace: str
    deadline: float
    host_boot_id: str = ""
    pid_ns: int = 0
    closed: bool = False
    # Its node run ended while it held departure pins.
    orphan: bool = False
    orphan_since: float | None = None

    def valid(self, now: float) -> bool:
        return not self.closed and now < self.deadline

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


class NodeRegistry:
    def __init__(
        self,
        catalog: Catalog,
        pins: PinManager,
        ledger: OpLedger,
        server_epoch: str,
        session_ttl_s: float,
        heartbeat_s: float,
        node_give_up_s: float,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.catalog = catalog
        self.pins = pins
        self.ledger = ledger
        self.server_epoch = server_epoch
        self.session_ttl_s = session_ttl_s
        self.heartbeat_s = heartbeat_s
        self.node_give_up_s = node_give_up_s
        self._clock = clock
        self.nodes: dict[str, NodeRecord] = {}
        self._node_sessions: dict[str, NodeSession] = {}
        self._workers: dict[str, WorkerSession] = {}
        self._incarnations: dict[tuple[str, SessionRole], str] = {}
        self._workers_by_node: dict[str, set[str]] = {}
        # (deadline, token) of every worker session, checked when due
        self._expiry: list[tuple[float, str]] = []
        # (pool_id, offset, generation) read per node -> its last read, in the
        # order first queued
        self._access: dict[str, OrderedDict[tuple[str, int, int], int]] = {}
        self._read_seq = itertools.count()
        # Holders whose pins are still to release (keeping departure pins if True).
        self._pin_releases: deque[tuple[str, bool]] = deque()
        # ids of pins on a given-up node's locations, still to release
        self._pin_drops: deque[set[str]] = deque()
        # nodes with worker sessions still to drop
        self._dropping: deque[NodeRecord] = deque()
        # maps, sets and listings of ended runs, emptied in maintain() steps
        self._freeing: deque[tuple[dict | set | list, int]] = deque()
        self.policy_version = 0

    # ================================================================ lookup

    def node_session(self, token: str, now: float) -> NodeSession | None:
        session = self._node_sessions.get(token)
        return session if session is not None and session.valid(now) else None

    def worker_session(self, token: str, now: float) -> WorkerSession | None:
        session = self._current(token)
        return session if session is not None and session.valid(now) else None

    def known_worker(self, token: str) -> WorkerSession | None:
        """Not closed; may be expired."""
        session = self._current(token)
        return session if session is not None and not session.closed else None

    def _current(self, token: str) -> WorkerSession | None:
        """The session; one of a dropped run is dropped first (as maintain
        would), so a request never sees it half done."""
        session = self._workers.get(token)
        if session is not None:
            node = self.nodes.get(session.node_id)
            if node is not None and token in node.dropping:
                node.dropping.discard(token)
                self._drop_worker(session)
                session = self._workers.get(token)
        return session

    def node_online(self, node_id: str, now: float) -> bool:
        node = self.nodes.get(node_id)
        return bool(node and node.session and node.session.valid(now))

    def capabilities_of(self, node_id: str) -> tuple[str, ...]:
        """Accepted capabilities of the node's live session."""
        node = self.nodes.get(node_id)
        if (
            node is None
            or node.session is None
            or not node.session.valid(self._clock())
        ):
            return ()
        return node.session.capabilities

    def stale_run(self, worker: WorkerSession) -> bool:
        """A worker attached to a node run that has been replaced."""
        if worker.role != SessionRole.WORKER:
            return False
        node = self.nodes.get(worker.node_id)
        return node is None or node.gone or node.node_epoch != worker.node_epoch

    def requester(self, worker: WorkerSession, now: float) -> Requester:
        return Requester(
            node_id=worker.node_id,
            can_map=lambda pool: (
                self.can_map(worker, pool) or self.can_rdma(worker, pool, now)
            ),
            online=lambda node_id: self.node_online(node_id, now),
        )

    def can_map(self, worker: WorkerSession, pool: PoolRecord) -> bool:
        """The worker maps its node's DRAM of the run it attached
        to and that run's CXL regions; a probe answers for its node."""
        if pool.node_id != worker.node_id:
            return self._maps_shared_cxl(worker, pool)
        if not POOL_KINDS[pool.kind].mappable:
            return False
        if worker.role == SessionRole.PROBE:
            return True
        if POOL_KINDS[pool.kind].run_scoped:
            return pool.since_epoch_seq == worker.node_epoch_seq
        return pool.since_epoch_seq <= worker.node_epoch_seq

    def can_rdma(self, worker: WorkerSession, pool: PoolRecord, now: float) -> bool:
        return (
            WORKER_RDMA_CAPABILITY in self.capabilities_of(worker.node_id)
            and MEMORY_CAPABILITY in self.capabilities_of(pool.node_id)
            and pool.node_id != worker.node_id
            and pool.state == PoolState.ACTIVE
            and self.node_online(worker.node_id, now)
            and self.node_online(pool.node_id, now)
        )

    @staticmethod
    def keep_pin_on_departure(worker: WorkerSession, pool: PoolRecord) -> bool:
        return not (
            pool.node_id == worker.node_id
            and POOL_KINDS[pool.kind].run_scoped
            and pool.since_epoch_seq == worker.node_epoch_seq
        )

    @staticmethod
    def _maps_shared_cxl(worker: WorkerSession, pool: PoolRecord) -> bool:
        """Another host's CXL region mapped directly: not supported yet."""
        return False

    # ============================================================ node open

    def node_open(self, req: NodeOpenRequest, now: float) -> NodeOpenResponse:
        """Raises OpenError when refused."""
        self._check_node_open(req)
        self._check_other_nodes(req)
        node = self.nodes.get(req.node_id)
        if WORKER_RDMA_CAPABILITY in req.capabilities and req.pid_ns <= 0:
            raise OpenError(Status.REJECTED, "worker_rdma_v1 needs pid_ns")
        if (
            node is not None
            and node.host_boot_id == req.host_boot_id
            and node.pid_ns != req.pid_ns
            and (node.worker_rdma or WORKER_RDMA_CAPABILITY in req.capabilities)
        ):
            logger.error("node %s changed pid_ns on the same boot", req.node_id)
            raise OpenError(Status.REJECTED, "pid_ns changed on the same boot")
        if node is not None and req.node_epoch in node.retired_epochs:
            raise OpenError(
                Status.REJECTED,
                f"node_epoch {req.node_epoch} of {req.node_id} has ended; "
                "a new StorageService process opens with a new node_epoch",
            )
        if (
            node is not None
            and node.node_epoch == req.node_epoch
            and node.session is not None
        ):
            return self._reopen_same_epoch(node, req, now)
        return self._open_new_epoch(node, req, now)

    def _check_node_open(self, req: NodeOpenRequest) -> None:
        if req.protocol != MS_PROTOCOL:
            raise OpenError(Status.REJECTED, f"unknown protocol {req.protocol!r}")
        if not NODE_ID_RE.match(req.node_id):
            raise OpenError(Status.REJECTED, f"bad node_id {req.node_id!r}")
        if not req.node_epoch or not req.host_boot_id:
            raise OpenError(Status.REJECTED, "node_epoch and host_boot_id are needed")
        if req.pid_ns < 0:
            raise OpenError(Status.REJECTED, "pid_ns must be nonnegative")
        if len(req.pools) > MAX_BATCH or len(req.rm_domains) > MAX_BATCH:
            raise OpenError(Status.REJECTED, f"more than {MAX_BATCH} pools or domains")
        domains = {d.rm_domain for d in req.rm_domains}
        if len(domains) < len(req.rm_domains):
            raise OpenError(Status.REJECTED, "an rm_domain is listed twice")
        seen = set()
        for info in req.pools:
            error = self._pool_error(req, info, domains)
            if error is None and info.pool_id in seen:
                error = "listed twice"
            if error is not None:
                raise OpenError(Status.REJECTED, f"pool {info.pool_id}: {error}")
            seen.add(info.pool_id)

    @staticmethod
    def _pool_error(
        req: NodeOpenRequest, info: PoolInfo, domains: set[str]
    ) -> str | None:
        """This run's DRAM pool and the CXL regions it holds in its
        rm_domains."""
        if info.node_id != req.node_id:
            return f"belongs to node {info.node_id}"
        if info.pool_generation < 1:
            return "pool_generation starts at 1"
        if info.page_size <= 0 or info.capacity < info.page_size:
            return "needs page_size > 0 and capacity >= page_size"
        if info.kind == PoolKind.DRAM:
            expected = f"dram:{req.node_id}:{req.node_epoch}"
        else:
            if not info.rm_domain or info.region_id is None:
                return "a CXL pool needs its rm_domain and region_id"
            if info.rm_domain not in domains:
                return f"rm_domain {info.rm_domain!r} is not in rm_domains"
            expected = f"cxl:{info.rm_domain}:{info.region_id}"
        if info.pool_id != expected:
            return f"pool_id must be {expected!r}"
        return None

    def _check_other_nodes(self, req: NodeOpenRequest) -> None:
        """A region another node holds (not on its way out) is a
        misconfiguration. The rm_domain is the RM's own id, so nodes sharing an
        RM list it with whatever devices their regions are on."""
        for info in req.pools:
            pool = self.catalog.pool(info.pool_id)
            if (
                pool is not None
                and pool.node_id != req.node_id
                and pool.state != PoolState.REMOVING
            ):
                raise OpenError(
                    Status.REJECTED,
                    f"pool {info.pool_id} is registered by node {pool.node_id}",
                )

    def _reopen_same_epoch(
        self, node: NodeRecord, req: NodeOpenRequest, now: float
    ) -> NodeOpenResponse:
        """Same process again (expired session, lost reply): the run keeps
        its epoch_seq, pools, pins, op records and blocks."""
        old = node.session
        assert old is not None
        if old.closed_at is not None or old.ended:
            raise OpenError(
                Status.REJECTED,
                f"node_epoch {req.node_epoch} closed its session; a new process "
                "opens with a new node_epoch",
            )
        if old.host_boot_id != req.host_boot_id:
            raise OpenError(Status.REJECTED, "host_boot_id changed within a run")
        if old.pid_ns != req.pid_ns:
            raise OpenError(Status.REJECTED, "pid_ns changed within a run")
        to_register = []
        for info in req.pools:
            pool = self.catalog.pool(info.pool_id)
            if pool is None:
                to_register.append(info)
            elif pool.state != PoolState.ACTIVE or pool.info != info:
                raise OpenError(
                    Status.REJECTED,
                    f"pool {info.pool_id} differs from the one this run registered",
                )
        for info in to_register:
            self.catalog.register_pool(info, node.epoch_seq)
        session = self._new_session(node, req, now)
        self.pins.move_holder(old.token, session.token)
        old.ended = True
        self._node_sessions.pop(old.token, None)
        logger.info(
            "node %s reopened its session (node_epoch %s, epoch_seq %d)",
            node.node_id,
            req.node_epoch,
            node.epoch_seq,
        )
        return self._open_reply(node, session)

    def _open_new_epoch(
        self, node: NodeRecord | None, req: NodeOpenRequest, now: float
    ) -> NodeOpenResponse:
        if node is None:
            node = NodeRecord(node_id=req.node_id)
        if node.epoch_seq >= EPOCH_SEQ_MAX:
            raise OpenError(Status.REJECTED, f"epoch_seq of {req.node_id} exhausted")
        listed = {info.pool_id: info for info in req.pools}
        old = node.session
        adopt, remove = [], []
        for pool in self.catalog.pools_of(req.node_id):
            if pool.state == PoolState.REMOVING:
                continue  # already going; a listed one is registered anew
            if self._adoptable(pool, old, req, listed):
                adopt.append(pool)
            else:
                remove.append(pool)
        self.nodes[node.node_id] = node
        if old is not None and not old.ended:
            self._end_run(node, old)
        released = self._release_earlier_workers(node, req.node_epoch)
        for pool in remove:
            self.catalog.remove_pool(pool)
        for pool in adopt:
            pool.info = listed[pool.pool_id]  # as this run describes it
        node.epoch_seq += 1
        for pool in adopt:
            self.catalog.renew_registration(pool)
        node.node_epoch = req.node_epoch
        node.gone = False
        node.adopted = sorted(pool.pool_id for pool in adopt)
        adopted_ids = set(node.adopted)
        self._keep_blocks(node, adopted_ids)
        for info in req.pools:
            if info.pool_id not in adopted_ids:
                self.catalog.register_pool(info, node.epoch_seq)
        session = self._new_session(node, req, now)
        logger.info(
            "node %s opened node_epoch %s (epoch_seq %d): %d pools adopted, %d "
            "removed, %d earlier worker sessions processed",
            node.node_id,
            req.node_epoch,
            node.epoch_seq,
            len(adopt),
            len(remove),
            released,
        )
        return self._open_reply(node, session)

    def _release_earlier_workers(self, node: NodeRecord, node_epoch: str) -> int:
        """Release pins on earlier workers' own run-scoped pools."""
        released = 0
        for worker in self._node_workers(node.node_id):
            if (
                worker.role == SessionRole.WORKER
                and worker.node_epoch != node_epoch
                and self.pins.holds(worker.token)
            ):
                self._mark_orphan(worker)
                self._release_pins(worker.token, 0, keep_retained=True)
                released += 1
        return released

    @staticmethod
    def _adoptable(
        pool: PoolRecord,
        old: NodeSession | None,
        req: NodeOpenRequest,
        listed: dict[str, PoolInfo],
    ) -> bool:
        """Only a listed CXL pool, with a run before on the same boot, of the
        same region. A changed one starts empty; DRAM never stays."""
        info, was = listed.get(pool.pool_id), pool.info
        if info is None or not POOL_KINDS[pool.kind].rm_region:
            return False
        if old is None or old.host_boot_id != req.host_boot_id:
            return False
        return (
            info.rm_domain == was.rm_domain
            and info.region_id == was.region_id
            and info.capacity == was.capacity
            and info.page_size == was.page_size
        )

    def _new_session(
        self, node: NodeRecord, req: NodeOpenRequest, now: float
    ) -> NodeSession:
        session = NodeSession(
            token=uuid.uuid4().hex,
            node_id=req.node_id,
            node_epoch=req.node_epoch,
            epoch_seq=node.epoch_seq,
            host_boot_id=req.host_boot_id,
            pid_ns=req.pid_ns,
            rm_domains={d.rm_domain: frozenset(d.device_ids) for d in req.rm_domains},
            capabilities=tuple(
                cap
                for cap in dict.fromkeys(req.capabilities)
                if cap in NODE_CAPABILITIES
            ),
            deadline=now + self.session_ttl_s,
        )
        node.worker_rdma = WORKER_RDMA_CAPABILITY in session.capabilities or (
            node.host_boot_id == req.host_boot_id and node.worker_rdma
        )
        node.host_boot_id = req.host_boot_id
        node.pid_ns = req.pid_ns
        node.session = session
        self._node_sessions[session.token] = session
        return session

    def _open_reply(self, node: NodeRecord, session: NodeSession) -> NodeOpenResponse:
        session.orphans = tuple(
            OrphanWorker(w.incarnation, w.pid, w.pid_start, w.host_boot_id, w.pid_ns)
            for w in self._node_workers(node.node_id)
            if w.orphan and self.pins.holds_retained(w.token)
        )
        unwatchable = sum(
            self._unwatchable(w.host_boot_id, w.pid_ns, node) for w in session.orphans
        )
        if unwatchable:
            logger.error(
                "node %s has %d unwatchable orphans", node.node_id, unwatchable
            )
        page = self.node_orphans(session, None)
        adopted = []
        for pool_id in node.adopted:
            pool = self.catalog.pool(pool_id)
            if pool is not None and pool.state == PoolState.ACTIVE:
                adopted.append(pool.info)
        return NodeOpenResponse(
            status=Status.OK,
            server_epoch=self.server_epoch,
            node_session=session.token,
            epoch_seq=session.epoch_seq,
            heartbeat_s=self.heartbeat_s,
            session_ttl_s=self.session_ttl_s,
            adopted=adopted,
            capabilities=list(session.capabilities),
            orphans=page.orphans,
            next_cursor=page.next_cursor,
            pool_registrations={
                p.pool_id: p.registration
                for p in self.catalog.pools_of(node.node_id)
                if p.state == PoolState.ACTIVE
            },
        )

    def node_orphans(
        self, session: NodeSession, cursor: str | None
    ) -> NodeOrphansResponse:
        position = 0
        if cursor is not None:
            token, _, offset = cursor.partition(":")
            if (
                token != session.token
                or not offset.isascii()
                or not offset.isdigit()
                or len(offset) > 20
                or not 0 <= int(offset) < len(session.orphans)
            ):
                return NodeOrphansResponse(status=Status.REJECTED, error="bad cursor")
            position = int(offset)
        end = position + MAX_BATCH
        return NodeOrphansResponse(
            status=Status.OK,
            orphans=list(session.orphans[position:end]),
            next_cursor=f"{session.token}:{end}"
            if end < len(session.orphans)
            else None,
        )

    def _end_run(
        self, node: NodeRecord, session: NodeSession, inline: int = PINS_INLINE
    ):
        """The run behind a session ends: its pins, op records, blocks and
        recovery listings go, and its epoch may not open again."""
        session.ended = True
        self._node_sessions.pop(session.token, None)
        if node.session is session:
            node.session = None
        self._release_pins(session.token, inline)
        self.ledger.drop_origin(node_origin(session.node_epoch))
        node.retired_epochs.append(session.node_epoch)
        for listing in node.listings.values():  # snapshots of whole pools
            self._free(listing.entries, LISTING_FREES_PER_UNIT)
        node.listings = {}
        node.usage.clear()

    @staticmethod
    def _keep_blocks(node: NodeRecord, pool_ids: set[str]) -> None:
        """Deletes queued for pools the new run adopted carry over."""
        for block_id in list(node.blocks):
            block = node.blocks[block_id]
            items = tuple(i for i in block.items if i.location.pool_id in pool_ids)
            if items:
                block.items = items
            else:
                del node.blocks[block_id]
        node.blocked = {
            (i.key, i.location) for b in node.blocks.values() for i in b.items
        }

    def _release_pins(
        self, holder: str, inline: int, keep_retained: bool = False
    ) -> None:
        self.pins.release_holder(holder, inline, keep_retained)
        if self.pins.holds(holder, retained=not keep_retained):
            self._pin_releases.append((holder, keep_retained))

    def _node_workers(self, node_id: str) -> list[WorkerSession]:
        tokens = set(self._workers_by_node.get(node_id, ()))
        node = self.nodes.get(node_id)
        if node is not None:
            tokens |= node.dropping
        return [w for t in sorted(tokens) if (w := self._workers.get(t)) is not None]

    # ===================================================== node session ops

    def node_heartbeat(
        self, session: NodeSession, req: NodeHeartbeatRequest, now: float
    ) -> NodeHeartbeatResponse:
        session.deadline = now + self.session_ttl_s
        node = self.nodes[session.node_id]
        for usage in req.usage:
            pool = self.catalog.pool(usage.pool_id)
            if pool is not None and pool.node_id == node.node_id:
                node.usage[usage.pool_id] = usage
        return NodeHeartbeatResponse(
            status=Status.OK,
            access=self._take_access(node.node_id),
            blocks=self._pending_blocks(node),
            policy_version=self.policy_version,
        )

    def node_close(self, session: NodeSession, mode: CloseMode, now: float) -> None:
        """restart hides the DRAM locations; leave removes all of them."""
        if mode == CloseMode.RESTART:
            session.closed_at = now
            self._node_sessions.pop(session.token, None)
            for pool in self.catalog.pools_of(session.node_id):
                if POOL_KINDS[pool.kind].run_scoped:
                    self.catalog.hide_pool(pool)
            logger.info("node %s closed for a restart", session.node_id)
            return
        self._drop_node(self.nodes[session.node_id], "left", inline=PINS_INLINE)

    def _drop_node(
        self, node: NodeRecord, why: str, inline: int, force_cxl: bool = False
    ) -> None:
        """Left or given up: the run ends and its locations go. Its worker
        sessions (with their pins and op records) go in maintain() steps, or
        when a request names one."""
        if node.session is not None and not node.session.ended:
            self._end_run(node, node.session, inline)
        elif node.node_epoch is not None and node.node_epoch not in node.retired_epochs:
            node.retired_epochs.append(node.node_epoch)
        node.session = None
        for pool in self.catalog.pools_of(node.node_id):
            self.catalog.remove_pool(pool, force=True, force_cxl=force_cxl)
        tokens = self._workers_by_node.pop(node.node_id, set())
        if len(tokens) < len(node.dropping):
            node.dropping.update(tokens)
        else:
            tokens.update(node.dropping)
            node.dropping = tokens
        if node.dropping:
            self._dropping.append(node)
        node.gone = True
        node.adopted = []
        self._free(node.blocks, BLOCK_FREES_PER_UNIT)
        node.blocks = OrderedDict()
        self._free(node.blocked, BLOCKED_FREES_PER_UNIT)
        node.blocked = set()
        self._free(self._access.pop(node.node_id, None), ACCESS_FREES_PER_UNIT)
        logger.info("node %s %s: its locations are removed", node.node_id, why)

    def _free(self, container: dict | set | list | None, per_unit: int) -> None:
        """Empty ``container`` in maintain() steps, not in one go."""
        if container:
            self._freeing.append((container, per_unit))

    def recover_list(
        self, session: NodeSession, pool_id: str, cursor: str | None
    ) -> NodeRecoverListResponse:
        """Cursors index one snapshot per pool, so a resent page is the same;
        the snapshot goes after its last page."""
        node = self.nodes[session.node_id]
        if pool_id not in node.adopted:
            return NodeRecoverListResponse(
                status=Status.REJECTED,
                error=f"{pool_id} is not a pool this run adopted",
            )
        listing_id = next(
            (k for k, v in node.listings.items() if v.pool_id == pool_id), ""
        )
        listing = node.listings.get(listing_id)
        if listing is not None and listing.last is not None:
            if cursor == listing.last[0]:
                return listing.last[1]
            if cursor is not None:
                return self._bad_cursor(cursor)
            del node.listings[listing_id]  # a fresh listing
            listing = None
        if cursor is None:
            if listing is None:
                listing_id = str(next(node.listing_ids))
                entries = list(self.catalog.entries_in(pool_id).values())
                listing = node.listings[listing_id] = _Listing(pool_id, entries)
            position = 0
        else:
            given_id, _, pos = cursor.partition(":")
            if (
                listing is None
                or given_id != listing_id
                or not pos.isdigit()
                or int(pos) > len(listing.entries)
            ):
                return self._bad_cursor(cursor)
            position = int(pos)
        current = self.catalog.entries_in(pool_id)
        end = min(position + MAX_BATCH, len(listing.entries))
        items = [
            RecoverItem(
                key=entry.key,
                location=entry.location,
                logical_bytes=entry.logical_bytes,
                pinned=bool(self.pins.count(entry.key, entry.location)),
            )
            for entry in listing.entries[position:end]
            if current.get(entry.location.allocation_id) is entry
        ]
        done = end >= len(listing.entries)
        response = NodeRecoverListResponse(
            status=Status.OK,
            items=items,
            next_cursor=None if done else f"{listing_id}:{end}",
        )
        if done:
            listing.entries = []
            listing.last = (cursor or "", response)
        return response

    @staticmethod
    def _bad_cursor(cursor: str) -> NodeRecoverListResponse:
        return NodeRecoverListResponse(
            status=Status.REJECTED, error=f"unknown cursor {cursor!r}"
        )

    # =========================================================== worker ops

    def worker_open(self, req: WorkerOpenRequest, now: float) -> WorkerOpenResponse:
        if req.protocol != MS_PROTOCOL:
            raise OpenError(Status.REJECTED, f"unknown protocol {req.protocol!r}")
        if not req.incarnation:
            raise OpenError(Status.REJECTED, "empty incarnation")
        node_epoch_seq = 0
        node = self.nodes.get(req.node_id)
        if req.role == SessionRole.WORKER:
            if node is None:
                raise OpenError(
                    Status.EPOCH,
                    f"node {req.node_id} has not opened its session here yet",
                )
            if node.gone or node.node_epoch != req.node_epoch:
                raise OpenError(
                    Status.EPOCH,
                    f"node_epoch {req.node_epoch} is not the current run of "
                    f"node {req.node_id}; attach to its StorageService again",
                )
            node_epoch_seq = node.epoch_seq
        previous = self._current(
            self._incarnations.get((req.incarnation, req.role), "")
        )
        identity = (
            req.node_id,
            req.engine_id,
            req.dp_index,
            req.tp_rank,
            req.pid,
            req.pid_start,
            req.namespace,
        )
        if previous is not None and identity != (
            previous.node_id,
            previous.engine_id,
            previous.dp_index,
            previous.tp_rank,
            previous.pid,
            previous.pid_start,
            previous.namespace,
        ):
            raise OpenError(
                Status.REJECTED,
                f"incarnation {req.incarnation} is open with another identity",
            )
        session = WorkerSession(
            token=uuid.uuid4().hex,
            incarnation=req.incarnation,
            role=req.role,
            node_id=req.node_id,
            node_epoch=req.node_epoch,
            node_epoch_seq=node_epoch_seq,
            engine_id=req.engine_id,
            dp_index=req.dp_index,
            tp_rank=req.tp_rank,
            pid=req.pid,
            pid_start=req.pid_start,
            namespace=req.namespace,
            deadline=now + self.session_ttl_s,
            host_boot_id=node.host_boot_id if node is not None else "",
            pid_ns=node.pid_ns if node is not None else 0,
        )
        if previous is not None:
            if previous.node_epoch == session.node_epoch:
                self.pins.move_holder(previous.token, session.token)
            else:
                self._release_pins(previous.token, 0)
            self._forget_worker(previous)
        self._workers[session.token] = session
        self._incarnations[(session.incarnation, session.role)] = session.token
        self._workers_by_node.setdefault(session.node_id, set()).add(session.token)
        heapq.heappush(self._expiry, (session.deadline, session.token))
        return WorkerOpenResponse(
            status=Status.OK,
            server_epoch=self.server_epoch,
            worker_session=session.token,
            heartbeat_s=self.heartbeat_s,
            session_ttl_s=self.session_ttl_s,
        )

    def worker_heartbeat(self, session: WorkerSession, now: float) -> None:
        session.deadline = now + self.session_ttl_s

    def worker_close(self, session: WorkerSession) -> None:
        self._release_pins(session.token, PINS_INLINE)
        self._forget_worker(session)
        if session.role == SessionRole.WORKER:  # a probe has no op records
            self.ledger.drop_origin(worker_origin(session.incarnation), session.node_id)

    def _drop_worker(self, worker: WorkerSession) -> None:
        """A worker session of a run that left or was given up."""
        self.ledger.drop_origin(worker_origin(worker.incarnation), worker.node_id)
        if self.pins.holds_retained(worker.token):
            self._mark_orphan(worker)
            self._workers_by_node.setdefault(worker.node_id, set()).add(worker.token)
            self._release_pins(worker.token, 0, keep_retained=True)
        else:
            self._release_pins(worker.token, 0)
            self._forget_worker(worker)

    def _mark_orphan(self, worker: WorkerSession) -> None:
        if self.pins.holds_retained(worker.token):
            worker.orphan = True
            if worker.orphan_since is None:
                worker.orphan_since = self._clock()

    def _forget_worker(self, session: WorkerSession) -> None:
        session.closed = True
        self._workers.pop(session.token, None)
        key = (session.incarnation, session.role)
        if self._incarnations.get(key) == session.token:
            del self._incarnations[key]
        tokens = self._workers_by_node.get(session.node_id)
        if tokens is not None:
            tokens.discard(session.token)
            if not tokens:
                del self._workers_by_node[session.node_id]

    def worker_exit(
        self, session: NodeSession, workers: Iterable[WorkerRef]
    ) -> list[ItemOutcome]:
        """A worker of another node or another pid is REJECTED; an unknown
        one is OK. Op records go only for workers of this node."""
        outcomes = []
        for ref in workers:
            token = self._incarnations.get((ref.incarnation, SessionRole.WORKER))
            worker = self._current(token) if token else None
            if worker is not None and (
                worker.node_id != session.node_id
                or (worker.pid, worker.pid_start) != (ref.pid, ref.pid_start)
            ):
                outcomes.append(ItemOutcome(ItemResult.REJECTED))
                continue
            if worker is not None:
                self.worker_close(worker)
            self.ledger.drop_origin(worker_origin(ref.incarnation), session.node_id)
            outcomes.append(ItemOutcome(ItemResult.OK))
        return outcomes

    # ========================================================== access info

    def record_access(self, pool: PoolRecord, location: Location) -> None:
        """A read of ``location`` (a HIT, or a fetched source), for the owning
        node's LRU. A re-read updates its last read but keeps its place in
        the send order; past ACCESS_MAX_PER_NODE the first queued goes."""
        access = self._access.get(pool.node_id)
        if access is None:
            access = self._access[pool.node_id] = OrderedDict()
        target = (location.pool_id, location.offset, location.generation)
        if target not in access and len(access) >= ACCESS_MAX_PER_NODE:
            access.popitem(last=False)
        access[target] = next(self._read_seq)

    def _take_access(self, node_id: str) -> AccessHints:
        """Up to ACCESS_PER_HEARTBEAT of the node's reads, those queued
        first, least recently read first."""
        access = self._access.get(node_id)
        if not access:
            return AccessHints()
        count = min(len(access), ACCESS_PER_HEARTBEAT)
        taken = [access.popitem(last=False) for _ in range(count)]
        taken.sort(key=itemgetter(1))
        return AccessHints.pack([target for target, _ in taken])

    # ======================================================= block requests

    def delete(self, keys: Iterable[Key], reason: str = "delete") -> list[ItemOutcome]:
        """Block requests to the owning nodes, which retire the
        locations themselves. OK when queued (or already queued); MISS when
        the key has no location (offline: none a running node can block)."""
        outcomes = []
        fresh: dict[str, list[KeyLocation]] = {}
        for key in keys:
            if self.catalog.is_preserved(key):
                outcomes.append(ItemOutcome(ItemResult.KEPT, cause=KeptCause.PINNED))
                continue
            queued = present = False
            for entry in self.catalog.locations(key):
                if entry.pool.state == PoolState.REMOVING:
                    continue
                present = True
                node = self.nodes.get(entry.pool.node_id)
                if (
                    entry.pool.state != PoolState.ACTIVE
                    or node is None
                    or not self._may_block(node)
                ):
                    continue
                queued = True
                target = (key, entry.location)
                if target not in node.blocked:
                    node.blocked.add(target)
                    fresh.setdefault(node.node_id, []).append(KeyLocation(*target))
            if queued:
                outcomes.append(ItemOutcome(ItemResult.OK))
            else:
                reason_ = MissReason.OFFLINE if present else MissReason.ABSENT
                outcomes.append(ItemOutcome(ItemResult.MISS, reason=reason_))
        for node_id, items in fresh.items():
            node = self.nodes[node_id]
            for start in range(0, len(items), BLOCK_ITEMS):
                block = _Block(
                    uuid.uuid4().hex, tuple(items[start : start + BLOCK_ITEMS]), reason
                )
                node.blocks[block.block_id] = block
        return outcomes

    @staticmethod
    def _may_block(node: NodeRecord) -> bool:
        """Its run goes on (maybe expired): a block would reach it."""
        session = node.session
        return session is not None and not session.ended and session.closed_at is None

    def _pending_blocks(self, node: NodeRecord) -> list[BlockRequest]:
        """Sent in turn (each goes to the back) until their items are gone."""
        out: list[BlockRequest] = []
        for block_id in list(node.blocks):
            if len(out) >= BLOCKS_PER_HEARTBEAT:
                break
            block = node.blocks[block_id]
            live = []
            for item in block.items:
                if self._live(item):
                    live.append(item)
                else:
                    node.blocked.discard((item.key, item.location))
            if not live:
                del node.blocks[block_id]
                continue
            block.items = tuple(live)
            node.blocks.move_to_end(block_id)
            out.append(BlockRequest(block_id, block.items, block.reason))
        return out

    def _live(self, item: KeyLocation) -> bool:
        entry = self.catalog.entry_exact(item.key, item.location)
        return entry is not None and entry.pool.state == PoolState.ACTIVE

    def blocks_waiting(self) -> int:
        return sum(len(n.blocks) for n in self.nodes.values())

    # ========================================================== maintenance

    def give_up_due(self, now: float) -> list[NodeRecord]:
        due = []
        for node in self.nodes.values():
            session = node.session
            if node.gone or session is None or session.ended or session.valid(now):
                continue
            offline_since = (
                session.closed_at if session.closed_at is not None else session.deadline
            )
            if now >= offline_since + self.node_give_up_s:
                due.append(node)
        return due

    def give_up(self, now: float, limit: int) -> int:
        """Drop due nodes; only a memory node's departure drops all pool pins."""
        due = self.give_up_due(now)[:limit]
        for node in due:
            memory = (
                node.session is not None
                and MEMORY_CAPABILITY in node.session.capabilities
            )
            pins = [
                self.pins.take_pool(p.pool_id)
                for p in self.catalog.pools_of(node.node_id)
                if memory or POOL_KINDS[p.kind].run_scoped
            ]
            self._pin_drops.extend(s for s in pins if s)
            self._drop_node(
                node, f"stayed away over {self.node_give_up_s}s", 0, force_cxl=memory
            )
            logger.info(
                "node %s given up: %d pins on its locations released",
                node.node_id,
                sum(len(s) for s in pins),
            )
        return len(due)

    def maintain(self, now: float, budget: int) -> int:
        """At most ``budget`` work: due worker sessions (expired ones without
        pins go), worker sessions of dropped runs, pin releases and op
        records, one unit each, and entries of an ended run's maps, sets and
        listings, *_FREES_PER_UNIT a unit; returns the work done."""
        work = 0
        while self._expiry and work < budget:
            deadline, token = self._expiry[0]
            if deadline > now:
                break
            heapq.heappop(self._expiry)
            work += 1
            session = self._workers.get(token)
            if session is None or session.closed:
                continue
            if session.valid(now):
                heapq.heappush(self._expiry, (session.deadline, token))
            elif not self.pins.holds(token):
                # expired holding no pins: it goes, with its op records; the
                # incarnation may open afresh. Pins never go by time (an
                # orphan's queued releases finish first)
                self._release_pins(token, 0)
                self._forget_worker(session)
                if session.role == SessionRole.WORKER:
                    self.ledger.drop_origin(
                        worker_origin(session.incarnation), session.node_id
                    )
            else:
                # WORKER_EXIT, WORKER_CLOSE, UNPIN or a reopen frees them
                heapq.heappush(self._expiry, (now + self.session_ttl_s, token))
        # Before the worker sessions: those of a given-up node keep no pin on
        # its locations (they are not kept as orphans for them)
        while self._pin_drops and work < budget:
            pin_ids = self._pin_drops[0]
            while pin_ids and work < budget:
                self.pins.unpin(pin_ids.pop())
                work += 1
            if not pin_ids:
                self._pin_drops.popleft()
        while self._dropping and work < budget:
            node = self._dropping[0]
            while node.dropping and work < budget:
                session = self._workers.get(node.dropping.pop())
                work += 1
                if session is not None and not session.closed:
                    self._drop_worker(session)
            if not node.dropping:
                self._dropping.popleft()
        while self._pin_releases and work < budget:
            holder, keep_retained = self._pin_releases[0]
            released = self.pins.release_holder(holder, budget - work, keep_retained)
            work += max(released, 1)
            if not self.pins.holds(holder, retained=not keep_retained):
                self._pin_releases.popleft()
        while self._freeing and work < budget:
            container, per_unit = self._freeing[0]
            if not container:
                self._freeing.popleft()
                continue
            for _ in range(min(per_unit, len(container))):
                if not isinstance(container, dict):
                    container.pop()
                    continue
                value = container.popitem()[1]
                if isinstance(value, _Block) and value.items:
                    # up to BLOCK_ITEMS locations: freed next, in steps too
                    self._freeing.appendleft(
                        (list(value.items), BLOCK_ITEM_FREES_PER_UNIT)
                    )
                    break
            work += 1
        work += self.ledger.sweep(budget - work)
        return work

    def worker_sessions(self) -> list[WorkerSession]:
        return list(self._workers.values())

    def orphan_sessions(self) -> int:
        return sum(1 for w in self._workers.values() if w.orphan)

    @staticmethod
    def _unwatchable(boot: str, pid_ns: int, node: NodeRecord) -> bool:
        return (
            not boot
            or not node.host_boot_id
            or (boot == node.host_boot_id and (not pid_ns or pid_ns != node.pid_ns))
        )

    def orphan_stats(self, now: float) -> dict:
        workers = [w for w in self._workers.values() if w.orphan]
        pins = [
            p
            for w in workers
            for p in self.pins.held_by(w.token)
            if p.keep_on_departure
        ]
        unwatchable = sum(
            self._unwatchable(w.host_boot_id, w.pid_ns, self.nodes[w.node_id])
            for w in workers
        )
        return {
            "orphan_sessions": len(workers),
            "orphan_pins": len(pins),
            "orphan_bytes": sum(p.location.length for p in pins),
            "orphan_oldest_s": max(
                (now - w.orphan_since for w in workers if w.orphan_since is not None),
                default=0.0,
            ),
            "orphan_unwatchable": unwatchable,
        }
