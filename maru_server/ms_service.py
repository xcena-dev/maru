# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruServer side of the multi-storage structure.

One lock guards Catalog, pins, registry and op ledger. Expiry scans,
deferred removals and policy reloads run on a maintenance thread in bounded
steps.
"""

import json
import logging
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

from maru_common.protocol import (
    AdminDeleteRequest,
    AdminDeleteResponse,
    CommitRequest,
    CommitResponse,
    FetchCompleteRequest,
    FetchCompleteResponse,
    LookupAndPinRequest,
    LookupAndPinResponse,
    NodeCloseRequest,
    NodeCloseResponse,
    NodeHeartbeatRequest,
    NodeHeartbeatResponse,
    NodeOpenRequest,
    NodeOpenResponse,
    NodeOrphansRequest,
    NodeOrphansResponse,
    NodeRecoverListRequest,
    NodeRecoverListResponse,
    ProbeRequest,
    ProbeResponse,
    RetireRequest,
    RetireResponse,
    SourcePinRequest,
    SourcePinResponse,
    UnpinRequest,
    UnpinResponse,
    WorkerCloseRequest,
    WorkerCloseResponse,
    WorkerExitRequest,
    WorkerExitResponse,
    WorkerHeartbeatRequest,
    WorkerHeartbeatResponse,
    WorkerOpenRequest,
    WorkerOpenResponse,
)
from maru_common.serializer import op_digest
from maru_common.storage_types import (
    MAX_BATCH,
    ItemOutcome,
    ItemResult,
    Key,
    OpRef,
    SessionRole,
    Status,
)

from .catalog import Catalog, PoolRecord
from .node_registry import (
    NodeRegistry,
    NodeSession,
    OpenError,
    WorkerSession,
    node_origin,
    worker_origin,
)
from .op_ledger import (
    DEFAULT_OP_BYTES_MAX,
    DEFAULT_OP_WINDOW,
    OpLedger,
    OriginKey,
    Verdict,
)
from .pin_manager import HolderKind, Pin, PinManager

logger = logging.getLogger(__name__)

DEFAULT_SESSION_TTL_S = 30.0
DEFAULT_NODE_GIVE_UP_S = 600.0
# maintenance steps per hold of the lock (about 1 ms), and the pause between
DEFAULT_MAINTENANCE_BUDGET = 512
DEFAULT_MAINTENANCE_INTERVAL_S = 1.0
MAINTENANCE_YIELD_S = 0.0005

R = TypeVar("R")


class PolicyError(ValueError):
    pass


def load_policy(path: str) -> frozenset[Key]:
    """Preserved keys from a policy file: {"preserve": [{"namespace", "key"}]}."""

    def no_duplicates(pairs):
        keys = [k for k, _ in pairs]
        if len(keys) != len(set(keys)):
            raise PolicyError(f"{path}: a key appears twice in one object")
        return dict(pairs)

    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f, object_pairs_hook=no_duplicates)
    except (OSError, ValueError) as e:
        raise PolicyError(f"{path}: {e}") from e
    if not isinstance(data, dict) or set(data) - {"preserve"}:
        raise PolicyError(f"{path}: expected an object with only 'preserve'")
    entries = data.get("preserve", [])
    if not isinstance(entries, list):
        raise PolicyError(f"{path}: 'preserve' must be a list")
    keys = []
    for entry in entries:
        if (
            not isinstance(entry, dict)
            or set(entry) != {"namespace", "key"}
            or not all(isinstance(v, str) for v in entry.values())
        ):
            raise PolicyError(f"{path}: bad preserve entry {entry!r}")
        keys.append(Key(namespace=entry["namespace"], key=entry["key"]))
    return frozenset(keys)


def result_bytes(result: Any) -> int:
    """Rough size of a stored op result, for op_bytes_max."""
    size = 64
    for item in getattr(result, "items", ()):
        size += 48 + len(item.pin_id or "")
        size += len(item.plan.pin_id or "") if item.plan else 0
        for loc in (item.location, item.plan.source if item.plan else None):
            if loc is not None:
                size += 64 + len(loc.pool_id) + len(loc.allocation_id)
    return size


@dataclass
class _Batch:
    """Items applied so far; ``applying`` is set before the first change."""

    applying: bool = False
    outcomes: list[ItemOutcome] = field(default_factory=list)


class MsService:
    def __init__(
        self,
        server_epoch: str,
        session_ttl_s: float = DEFAULT_SESSION_TTL_S,
        heartbeat_s: float | None = None,
        node_give_up_s: float = DEFAULT_NODE_GIVE_UP_S,
        op_window: int = DEFAULT_OP_WINDOW,
        op_bytes_max: int = DEFAULT_OP_BYTES_MAX,
        policy_file: str | None = None,
        clock: Callable[[], float] = time.monotonic,
        maintenance_interval_s: float = DEFAULT_MAINTENANCE_INTERVAL_S,
        maintenance_budget: int = DEFAULT_MAINTENANCE_BUDGET,
    ):
        if heartbeat_s is None:
            heartbeat_s = session_ttl_s / 3
        if not session_ttl_s > 0:
            raise ValueError("session_ttl_s must be positive")
        if not 0 < heartbeat_s < session_ttl_s:
            raise ValueError("heartbeat_s must be positive and below session_ttl_s")
        if not node_give_up_s > 0:
            raise ValueError("node_give_up_s must be positive")
        if not maintenance_interval_s > 0 or maintenance_budget < 1:
            raise ValueError("maintenance interval and budget must be positive")
        self.server_epoch = server_epoch
        self.session_ttl_s = session_ttl_s
        self.heartbeat_s = heartbeat_s
        self.node_give_up_s = node_give_up_s
        self._clock = clock
        self.maintenance_interval_s = maintenance_interval_s
        self.maintenance_budget = maintenance_budget
        self.lock = threading.Lock()
        self.pins = PinManager()
        self.catalog = Catalog(self.pins)
        self.ledger = OpLedger(op_window, op_bytes_max)
        self.registry = NodeRegistry(
            self.catalog,
            self.pins,
            self.ledger,
            server_epoch=server_epoch,
            session_ttl_s=session_ttl_s,
            heartbeat_s=heartbeat_s,
            node_give_up_s=node_give_up_s,
            clock=clock,
        )
        self._policy_file = policy_file
        self._policy_mtime: int | None = None  # of the policy in force
        self._policy_failed: int | None = None  # mtime last logged as bad
        if policy_file is not None:
            self._policy_mtime = os.stat(policy_file).st_mtime_ns
            self.catalog.set_preserved(load_policy(policy_file))
        self._thread: threading.Thread | None = None
        self._thread_lock = threading.Lock()
        self._stop = threading.Event()

    # ============================================================ helpers

    def _node_session(
        self, req: Any, response_cls: Callable[..., R]
    ) -> NodeSession | R:
        """The session, or the EPOCH/REJECTED reply."""
        error = self._epoch_error(req)
        if error is None:
            session = self.registry.node_session(req.session, self._clock())
            if session is not None:
                return session
            if self.registry.known_worker(req.session) is not None:
                return response_cls(
                    status=Status.REJECTED, error="a worker session sent a node request"
                )
            error = "node session is unknown, closed or expired"
        return response_cls(status=Status.EPOCH, error=error)

    def _worker_session(
        self, req: Any, response_cls: Callable[..., R], probe_ok: bool = False
    ) -> WorkerSession | R:
        """The session, or the EPOCH/REJECTED reply; probes may only PROBE."""
        error = self._epoch_error(req)
        if error is None:
            session = self.registry.worker_session(req.session, self._clock())
            if session is not None:
                if session.role == SessionRole.PROBE and not probe_ok:
                    return response_cls(
                        status=Status.REJECTED, error="a probe session may only PROBE"
                    )
                return session
            if self.registry.node_session(req.session, self._clock()) is not None:
                return response_cls(
                    status=Status.REJECTED, error="a node session sent a worker request"
                )
            error = "worker session is unknown, closed or expired"
        return response_cls(status=Status.EPOCH, error=error)

    def _lookup_session(
        self, req: Any, response_cls: Callable[..., R], probe_ok: bool
    ) -> WorkerSession | R:
        """A worker attached to a replaced node run reopens before looking up."""
        session = self._worker_session(req, response_cls, probe_ok)
        if isinstance(session, WorkerSession) and self.registry.stale_run(session):
            return response_cls(
                status=Status.EPOCH,
                error="the node run this session attached to was replaced; "
                "attach again and reopen the session",
            )
        return session

    def _epoch_error(self, req: Any) -> str | None:
        if req.server_epoch != self.server_epoch:
            return "server_epoch is not this MaruServer run's; open the session again"
        return None

    @staticmethod
    def _too_many(items: list, response_cls: Callable[..., R], what: str) -> R | None:
        if len(items) > MAX_BATCH:
            return response_cls(
                status=Status.REJECTED,
                error=f"{len(items)} {what}; at most {MAX_BATCH} per request",
            )
        return None

    @staticmethod
    def _op_error(op: OpRef, origin: str, digest: bytes) -> str | None:
        if op.seq < 1 or op.ack < 0:
            return "op needs seq >= 1 and ack >= 0"
        if op.ack >= op.seq:
            return "op ack must be below its seq"
        if op.origin != origin:
            return f"op origin {op.origin!r} is not this session's run"
        if op.digest != digest:
            return "op digest does not match the request"
        return None

    def _run_op(
        self,
        op: OpRef,
        origin: OriginKey,
        owner: str,
        digest: bytes,
        response_cls: Callable[..., R],
        n_items: int,
        run: Callable[[_Batch], R],
    ) -> R:
        """Op rules (see OpLedger). A run that fails after changing the Catalog
        is recorded with its applied items (the rest REJECTED), never dropped."""
        error = self._op_error(op, origin[1], digest)
        if error is not None:
            return response_cls(status=Status.REJECTED, error=error)
        verdict, record = self.ledger.begin(origin, op, owner)
        if verdict == Verdict.NEW:
            assert record is not None
            batch = _Batch()
            try:
                result = run(batch)
            except BaseException as exc:
                if not batch.applying:
                    self.ledger.abort(origin, record)
                    raise
                rest = n_items - len(batch.outcomes)
                result = response_cls(
                    status=Status.OK,
                    error="internal error; items not applied are REJECTED",
                    items=batch.outcomes + [ItemOutcome(ItemResult.REJECTED)] * rest,
                )
                self.ledger.finish(origin, record, result, result_bytes(result))
                if not isinstance(exc, Exception):
                    raise
                logger.error(
                    "op %s/%d failed part way", op.origin, op.seq, exc_info=True
                )
                return result
            self.ledger.finish(origin, record, result, result_bytes(result))
            return result
        if verdict == Verdict.REPLAY:
            assert record is not None
            replay: R = record.result
            return replay
        if verdict == Verdict.STALE:
            return response_cls(
                status=Status.OK,
                items=[ItemOutcome(ItemResult.STALE) for _ in range(n_items)],
            )
        if verdict == Verdict.REJECTED:
            return response_cls(
                status=Status.REJECTED,
                error=f"op seq {op.seq} was used for another request",
            )
        # BUSY; WAIT cannot happen: begin, run and finish share one lock hold
        return response_cls(
            status=Status.BUSY,
            error=f"too many unacknowledged ops of {op.origin}; resend later",
        )

    # ======================================================== node session

    def node_open(self, req: NodeOpenRequest, payload: dict) -> NodeOpenResponse:
        with self.lock:
            now = self._clock()
            try:
                return self.registry.node_open(req, now)
            except OpenError as e:
                return NodeOpenResponse(
                    status=e.status, error=str(e), server_epoch=self.server_epoch
                )

    def node_heartbeat(
        self, req: NodeHeartbeatRequest, payload: dict
    ) -> NodeHeartbeatResponse:
        with self.lock:
            session = self._node_session(req, NodeHeartbeatResponse)
            if not isinstance(session, NodeSession):
                return session
            rejected = self._too_many(req.usage, NodeHeartbeatResponse, "usage entries")
            if rejected:
                return rejected
            return self.registry.node_heartbeat(session, req, self._clock())

    def node_orphans(
        self, req: NodeOrphansRequest, payload: dict
    ) -> NodeOrphansResponse:
        with self.lock:
            session = self._node_session(req, NodeOrphansResponse)
            if not isinstance(session, NodeSession):
                return session
            return self.registry.node_orphans(session, req.cursor)

    def node_close(self, req: NodeCloseRequest, payload: dict) -> NodeCloseResponse:
        with self.lock:
            session = self._node_session(req, NodeCloseResponse)
            if not isinstance(session, NodeSession):
                return session
            self.registry.node_close(session, req.mode, self._clock())
            return NodeCloseResponse(status=Status.OK)

    def node_recover_list(
        self, req: NodeRecoverListRequest, payload: dict
    ) -> NodeRecoverListResponse:
        with self.lock:
            session = self._node_session(req, NodeRecoverListResponse)
            if not isinstance(session, NodeSession):
                return session
            return self.registry.recover_list(session, req.pool_id, req.cursor)

    def worker_exit(self, req: WorkerExitRequest, payload: dict) -> WorkerExitResponse:
        digest = op_digest(payload)
        with self.lock:
            session = self._node_session(req, WorkerExitResponse)
            if not isinstance(session, NodeSession):
                return session
            rejected = self._too_many(req.workers, WorkerExitResponse, "workers")
            if rejected:
                return rejected

            def run(batch: _Batch) -> WorkerExitResponse:
                logger.info(
                    "node %s reports %d workers ended (%s)",
                    session.node_id,
                    len(req.workers),
                    req.cause,
                )
                batch.applying = True
                batch.outcomes.extend(self.registry.worker_exit(session, req.workers))
                return WorkerExitResponse(status=Status.OK, items=batch.outcomes)

            return self._run_op(
                req.op,
                node_origin(session.node_epoch),
                session.node_id,
                digest,
                WorkerExitResponse,
                len(req.workers),
                run,
            )

    def admin_delete(
        self, req: AdminDeleteRequest, payload: dict
    ) -> AdminDeleteResponse:
        """Block requests to the owning nodes, which retire."""
        with self.lock:
            rejected = self._too_many(req.items, AdminDeleteResponse, "items")
            if rejected:
                return rejected
            items = self.registry.delete(req.items)
            return AdminDeleteResponse(status=Status.OK, items=items)

    # ====================================================== worker session

    def worker_open(self, req: WorkerOpenRequest, payload: dict) -> WorkerOpenResponse:
        with self.lock:
            now = self._clock()
            try:
                return self.registry.worker_open(req, now)
            except OpenError as e:
                # with the epoch: a worker tells a restarted MaruServer
                return WorkerOpenResponse(
                    status=e.status, error=str(e), server_epoch=self.server_epoch
                )

    def worker_heartbeat(
        self, req: WorkerHeartbeatRequest, payload: dict
    ) -> WorkerHeartbeatResponse:
        with self.lock:
            session = self._worker_session(req, WorkerHeartbeatResponse, probe_ok=True)
            if not isinstance(session, WorkerSession):
                return session
            self.registry.worker_heartbeat(session, self._clock())
            return WorkerHeartbeatResponse(status=Status.OK)

    def worker_close(
        self, req: WorkerCloseRequest, payload: dict
    ) -> WorkerCloseResponse:
        with self.lock:
            session = self._worker_session(req, WorkerCloseResponse, probe_ok=True)
            if not isinstance(session, WorkerSession):
                return session
            self.registry.worker_close(session)
            return WorkerCloseResponse(status=Status.OK)

    # ===================================================== lookup and pin

    def probe(self, req: ProbeRequest, payload: dict) -> ProbeResponse:
        with self.lock:
            session = self._lookup_session(req, ProbeResponse, probe_ok=True)
            if not isinstance(session, WorkerSession):
                return session
            rejected = self._too_many(req.items, ProbeResponse, "items")
            if rejected:
                return rejected
            requester = self.registry.requester(session, self._clock())
            items = []
            for key in req.items:
                selection = self.catalog.select(key, requester)
                location = None
                if req.with_locations and selection.entry is not None:
                    location = selection.entry.location
                items.append(
                    ItemOutcome(
                        selection.result,
                        location=location,
                        reason=selection.reason,
                        pool_registration=selection.entry.pool.registration
                        if location is not None
                        else 0,
                    )
                )
            return ProbeResponse(status=Status.OK, items=items)

    def lookup_and_pin(
        self, req: LookupAndPinRequest, payload: dict
    ) -> LookupAndPinResponse:
        digest = op_digest(payload)
        with self.lock:
            session = self._lookup_session(req, LookupAndPinResponse, probe_ok=False)
            if not isinstance(session, WorkerSession):
                return session
            rejected = self._too_many(req.items, LookupAndPinResponse, "items")
            if rejected:
                return rejected

            def run(batch: _Batch) -> LookupAndPinResponse:
                requester = self.registry.requester(session, self._clock())
                batch.applying = True
                for key in req.items:
                    batch.outcomes.append(
                        self._lookup_one(key, requester, session, req.pin_sources)
                    )
                return LookupAndPinResponse(status=Status.OK, items=batch.outcomes)

            return self._run_op(
                req.op,
                worker_origin(session.incarnation),
                session.node_id,
                digest,
                LookupAndPinResponse,
                len(req.items),
                run,
            )

    def _lookup_one(
        self, key: Key, requester, session: WorkerSession, pin_sources: bool
    ) -> ItemOutcome:
        selection = self.catalog.select(key, requester)
        entry = selection.entry
        if selection.result == ItemResult.HIT:
            assert entry is not None
            pin_id = self.pins.pin(
                key,
                entry.location,
                session.token,
                HolderKind.WORKER,
                keep_on_departure=self.registry.keep_pin_on_departure(
                    session, entry.pool
                ),
            )
            try:
                self.registry.record_access(entry.pool, entry.location)
            except BaseException:
                self.pins.unpin(pin_id)  # a failed item leaves no pin
                raise
            return ItemOutcome(
                ItemResult.HIT,
                location=entry.location,
                pin_id=pin_id,
                pool_registration=entry.pool.registration,
            )
        if selection.result == ItemResult.FETCH:
            assert entry is not None
            source_pin = None
            if pin_sources:
                # For the worker's node run to read under (FETCH_COMPLETE)
                source_pin = self.pins.pin(
                    key,
                    entry.location,
                    session.token,
                    HolderKind.SOURCE,
                    creator=(session.node_id, session.node_epoch),
                )
            plan = self.catalog.fetch_plan(entry, source_pin)
            return ItemOutcome(ItemResult.FETCH, plan=plan)
        return ItemOutcome(ItemResult.MISS, reason=selection.reason)

    def unpin(self, req: UnpinRequest, payload: dict) -> UnpinResponse:
        """Own pins; with ``worker_incarnation``, only that worker's source
        pins for the node's run. Unknown ids are OK, others' pins
        REJECTED."""
        with self.lock:
            now = self._clock()
            error = self._epoch_error(req)
            worker = node = None
            if error is None:
                worker = self.registry.worker_session(req.session, now)
                node = None if worker else self.registry.node_session(req.session, now)
                if worker is None and node is None:
                    error = "session is unknown, closed or expired"
            if error is not None:
                return UnpinResponse(status=Status.EPOCH, error=error)
            if worker is not None and worker.role == SessionRole.PROBE:
                return UnpinResponse(
                    status=Status.REJECTED, error="a probe session may only PROBE"
                )
            rejected = self._too_many(req.pin_ids, UnpinResponse, "pin_ids")
            if rejected:
                return rejected
            items = []
            for pin_id in req.pin_ids:
                pin = self.pins.get(pin_id)
                if pin is None:
                    items.append(ItemOutcome(ItemResult.OK))
                    continue
                if req.worker_incarnation:
                    mine = node is not None and self._own_source(
                        node, pin, req.worker_incarnation
                    )
                else:
                    # A node releases a worker's source pin only by naming
                    # the worker
                    mine = pin.holder == req.session
                if mine:
                    self.pins.unpin(pin_id)
                    items.append(ItemOutcome(ItemResult.OK))
                else:
                    items.append(ItemOutcome(ItemResult.REJECTED))
            return UnpinResponse(status=Status.OK, items=items)

    def source_pin(self, req: SourcePinRequest, payload: dict) -> SourcePinResponse:
        digest = op_digest(payload)
        with self.lock:
            session = self._node_session(req, SourcePinResponse)
            if not isinstance(session, NodeSession):
                return session
            rejected = self._too_many(req.items, SourcePinResponse, "items")
            if rejected:
                return rejected

            def run(batch: _Batch) -> SourcePinResponse:
                # Only a new op is checked: a resend gets its first answer,
                # with the pins it made, also once the worker went
                if req.worker_incarnation:
                    worker = self.registry.known_worker(req.worker_session)
                    error = self._not_bound(session, worker, req.worker_incarnation)
                    if error is None and any(
                        i.key.namespace != worker.namespace for i in req.items
                    ):
                        error = "a key is not in the worker's namespace"
                    if error is not None:
                        return SourcePinResponse(status=Status.REJECTED, error=error)
                now = self._clock()

                def online(node_id: str) -> bool:
                    return self.registry.node_online(node_id, now)

                def can_fetch(pool: PoolRecord) -> bool:
                    return self.registry.can_fetch(session.node_id, pool)

                batch.applying = True
                for item in req.items:
                    batch.outcomes.append(
                        self.catalog.source_pin(
                            session.node_id,
                            session.token,
                            item.key,
                            item.location,
                            online,
                            can_fetch if req.worker_incarnation else None,
                        )
                    )
                return SourcePinResponse(status=Status.OK, items=batch.outcomes)

            return self._run_op(
                req.op,
                node_origin(session.node_epoch),
                session.node_id,
                digest,
                SourcePinResponse,
                len(req.items),
                run,
            )

    # =================================================== publish and remove

    def commit(self, req: CommitRequest, payload: dict) -> CommitResponse:
        digest = op_digest(payload)
        with self.lock:
            session = self._node_session(req, CommitResponse)
            if not isinstance(session, NodeSession):
                return session
            rejected = self._too_many(req.items, CommitResponse, "items")
            if rejected:
                return rejected

            def run(batch: _Batch) -> CommitResponse:
                errors = [
                    self.catalog.check_commit(session.node_id, session.epoch_seq, item)
                    for item in req.items
                ]
                # No finalizer is kept here: such an item is no plain commit
                for i, raw in enumerate(payload["items"]):
                    if raw.get("finalizer"):
                        errors[i] = "a finalizer is not served"
                batch.applying = True
                for item, error in zip(req.items, errors, strict=True):
                    if error is not None:
                        logger.debug("COMMIT %s rejected: %s", item.key, error)
                        batch.outcomes.append(ItemOutcome(ItemResult.REJECTED))
                    else:
                        batch.outcomes.append(self.catalog.commit(item))
                return CommitResponse(status=Status.OK, items=batch.outcomes)

            return self._run_op(
                req.op,
                node_origin(session.node_epoch),
                session.node_id,
                digest,
                CommitResponse,
                len(req.items),
                run,
            )

    def retire(self, req: RetireRequest, payload: dict) -> RetireResponse:
        digest = op_digest(payload)
        with self.lock:
            session = self._node_session(req, RetireResponse)
            if not isinstance(session, NodeSession):
                return session
            rejected = self._too_many(req.items, RetireResponse, "items")
            if rejected:
                return rejected

            def run(batch: _Batch) -> RetireResponse:
                batch.applying = True
                for item in req.items:
                    batch.outcomes.append(
                        self.catalog.retire(session.node_id, item.key, item.location)
                    )
                return RetireResponse(status=Status.OK, items=batch.outcomes)

            return self._run_op(
                req.op,
                node_origin(session.node_epoch),
                session.node_id,
                digest,
                RetireResponse,
                len(req.items),
                run,
            )

    def fetch_complete(
        self, req: FetchCompleteRequest, payload: dict
    ) -> FetchCompleteResponse:
        digest = op_digest(payload)
        with self.lock:
            session = self._node_session(req, FetchCompleteResponse)
            if not isinstance(session, NodeSession):
                return session
            rejected = self._too_many(req.items, FetchCompleteResponse, "items")
            if rejected:
                return rejected

            def run(batch: _Batch) -> FetchCompleteResponse:
                batch.applying = True
                for item in req.items:
                    source = self.pins.get(item.source_pin_id)
                    owned = self._own_source(session, source, item.source_worker)
                    outcome = self.catalog.fetch_complete(item, owned)
                    batch.outcomes.append(outcome)
                    # A read that succeeded, of the source the node pinned
                    if (
                        outcome.result == ItemResult.OK
                        and item.logical_bytes > 0
                        and source is not None
                        and (pool := self.catalog.pool(source.location.pool_id))
                        is not None
                    ):
                        self.registry.record_access(pool, source.location)
                return FetchCompleteResponse(status=Status.OK, items=batch.outcomes)

            return self._run_op(
                req.op,
                node_origin(session.node_epoch),
                session.node_id,
                digest,
                FetchCompleteResponse,
                len(req.items),
                run,
            )

    @staticmethod
    def _not_bound(
        session: NodeSession, worker: WorkerSession | None, incarnation: str
    ) -> str | None:
        """Why ``worker`` is not the open session of the worker
        ``incarnation`` on the node run of ``session``, or None."""
        if worker is None:
            return "the worker session is unknown or closed"
        if (
            worker.role != SessionRole.WORKER
            or worker.node_id != session.node_id
            or worker.node_epoch != session.node_epoch
            or worker.incarnation != incarnation
        ):
            return "the worker session is not that worker's on this node run"
        return None

    def _own_source(
        self, session: NodeSession, pin: Pin | None, source_worker: str
    ) -> bool:
        """``pin`` is one ``session`` reads under and releases: with
        ``source_worker``, the source pin of that worker's FETCH plan for this
        node run; else the session's own node pin."""
        if pin is None:
            return False
        if not source_worker:
            return pin.holder_kind == HolderKind.NODE and pin.holder == session.token
        if pin.holder_kind != HolderKind.SOURCE or pin.creator != session.creator:
            return False
        holder = self.registry.known_worker(pin.holder)
        return holder is not None and holder.incarnation == source_worker

    # =========================================================== admin

    def check_policy(self) -> None:
        """Reload the policy file when its mtime changes; a bad file keeps
        the policy in force."""
        if self._policy_file is None:
            return
        try:
            mtime = os.stat(self._policy_file).st_mtime_ns
        except OSError as e:
            # Keep the loaded policy, but retry a restored file even if its
            # mtime equals the one seen before it disappeared.
            self._policy_mtime = None
            if self._policy_failed != -1:
                logger.error("policy file unreadable, keeping the policy: %s", e)
                self._policy_failed = -1
            return
        if mtime == self._policy_mtime:
            return
        try:
            preserved = load_policy(self._policy_file)
        except PolicyError as e:
            # retried on the next pass: it may have been read mid-write
            if self._policy_failed != mtime:
                logger.error("policy file invalid, keeping the policy: %s", e)
                self._policy_failed = mtime
            return
        self._policy_mtime = mtime
        self._policy_failed = None
        with self.lock:
            if self.catalog.set_preserved(preserved):
                self.registry.policy_version += 1
                logger.info("policy reloaded: %d preserved keys", len(preserved))

    def stats(self) -> dict:
        with self.lock:
            catalog = self.catalog.stats()
            return {
                "keys": catalog.keys,
                "locations": catalog.locations,
                "pools": catalog.pools,
                "pins": len(self.pins),
                "nodes": len(self.registry.nodes),
                "worker_sessions": len(self.registry.worker_sessions()),
                **self.registry.orphan_stats(self._clock()),
                "blocks_waiting": self.registry.blocks_waiting(),
            }

    # ========================================================= maintenance

    def maintain(self, give_up: bool = True) -> int:
        """One step of at most maintenance_budget work (plus, when
        ``give_up``, the scan for nodes to give up, O(nodes)); returns the
        work done, the budget when more may be left."""
        with self.lock:
            now = self._clock()
            budget = self.maintenance_budget
            work = self.registry.give_up(now, budget) if give_up else 0
            work += self.registry.maintain(now, budget - work)
            work += self.catalog.sweep(budget - work)
            return work

    def ensure_started(self) -> None:
        """Start the maintenance thread (on the first multi-storage request)."""
        if self._thread is not None:
            return
        with self._thread_lock:
            if self._thread is None and not self._stop.is_set():
                self._thread = threading.Thread(
                    target=self._maintenance_loop, name="ms-maintenance", daemon=True
                )
                self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        with self._thread_lock:
            thread = self._thread
        if thread is not None:
            thread.join(timeout=5.0)

    def _maintenance_loop(self) -> None:
        while not self._stop.wait(self.maintenance_interval_s):
            try:
                self.check_policy()
                work = self.maintain()
                # Python locks are unfair: pause between steps so waiting
                # requests get the lock
                while work >= self.maintenance_budget:
                    if self._stop.wait(MAINTENANCE_YIELD_S):
                        return
                    work = self.maintain(give_up=False)
            except Exception:
                logger.error("multi-storage maintenance failed", exc_info=True)
