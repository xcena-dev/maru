# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Worker requests, op records and process lifetimes."""

import collections
import logging
import threading
import time
from collections.abc import Callable
from typing import TypeVar

from maru_common.node_protocol import (
    AbortRequest,
    AbortResponse,
    CreditRequest,
    CreditResponse,
    CreditReturnRequest,
    CreditReturnResponse,
    Notify,
    UnmapAckRequest,
    UnmapAckResponse,
    WriteDoneRequest,
    WriteDoneResponse,
)
from maru_common.protocol import CommitItem
from maru_common.serializer import DecodedRequest, op_digest
from maru_common.storage_types import (
    MAX_BATCH,
    ExitCause,
    ItemOutcome,
    ItemResult,
    Location,
    NotifyKind,
    OpRef,
    PoolKind,
    Status,
    WorkerRef,
)

from ..config import TierTraits
from ..node_store import DEFAULT_EVICT_INTERVAL_S, NodeStore
from ..op_ledger import OP_WINDOW, LedgerEntry, OpLedger, Verdict
from ..placement import PlacementPolicy
from ..pool_manager import PageRef, PoolManager
from ..ports import (
    EpochChanged,
    MetadataPort,
    ProcessWatcher,
    ServerStopped,
    WorkerEndpoint,
)
from .local_pin import LocalPins
from .worker_registry import WorkerRecord, WorkerRegistry

logger = logging.getLogger("maru_storage_service.storage_manager")
R = TypeVar("R")
DEFAULT_GPU_DRAIN_GRACE_S = 5.0
EXIT_RETRY_S = 0.5
EXIT_RETRY_MAX_S = 30.0


class WorkerOps(NodeStore):
    def __init__(
        self,
        *,
        node_epoch: str,
        pool_manager: PoolManager,
        placement: PlacementPolicy,
        local_pins: LocalPins,
        metadata: MetadataPort,
        watcher: ProcessWatcher,
        gpu_drain_grace_s: float = DEFAULT_GPU_DRAIN_GRACE_S,
        evict_interval_s: float = DEFAULT_EVICT_INTERVAL_S,
        op_window: int = OP_WINDOW,
        traits: dict[str, TierTraits] | None = None,
        on_worker_settled: Callable[[str], None] = lambda incarnation: None,
        on_end: Callable[[str], None] = lambda reason: None,
        registry_factory=WorkerRegistry,
    ):
        """``on_end`` ends a run whose metadata result stays unknown."""
        super().__init__(
            node_epoch=node_epoch,
            pool_manager=pool_manager,
            placement=placement,
            local_pins=local_pins,
            metadata=metadata,
            evict_interval_s=evict_interval_s,
            traits=traits,
            on_end=on_end,
        )
        self.ledger = OpLedger(op_window)
        self.watcher = watcher
        self.workers = registry_factory(
            node_epoch=node_epoch,
            pools=pool_manager,
            ledger=self.ledger,
            watcher=self.watcher,
            on_exit=self._queue_exit,
            on_settled=on_worker_settled,
            gpu_drain_grace_s=gpu_drain_grace_s,
        )

        self._exits: collections.deque[WorkerRef] = collections.deque()
        self.orphans = None
        self._unmapped: set[tuple[str, str, int]] = set()

    def on_heartbeat(self, response):
        super().on_heartbeat(response)
        if self.orphans is not None:
            self.orphans.retry()

    def start(self) -> None:
        """After the session opened."""
        self.watcher.start(self.workers.on_process_exit)
        for target, name in (
            (self._commit_loop, "maru-commit"),
            (self._evict_loop, "maru-evict"),
            (self._exit_loop, "maru-worker-exit"),
        ):
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self._threads.append(thread)

    def stop(self) -> None:
        """Stop the threads; unresolved requests are given up."""
        with self._cv:
            self._stopping = True
            self._cv.notify_all()
        self.workers.stop()
        self._evict_wake.set()
        self.metadata.stop()
        for thread in self._threads:
            thread.join(timeout=10.0)
        self._threads.clear()
        self.watcher.stop()

    def local_pins_open(self) -> bool:
        """Whether a new local pin may be given (none is while local pins are
        off): never once the run ended, nor once MaruServer has not confirmed
        the session for node_stale_s (half the session TTL)."""
        if self._ended is not None or self.metadata.epoch_lost:
            return False
        last = self.metadata.last_heartbeat_ok
        stale_s = self.metadata.session_ttl_s / 2
        return last > 0 and time.monotonic() - last < stale_s

    def _check_op(self, rec: WorkerRecord, op: OpRef, decoded: DecodedRequest):
        """The digest of a worker op, or None and the reason it is malformed."""
        if op.origin != rec.incarnation:
            return None, "the op origin is not this worker's incarnation"
        if op.seq < 1 or op.ack < 0 or op.ack >= op.seq:
            return None, f"bad op seq {op.seq} / ack {op.ack}"
        digest = op_digest(decoded.payload)
        if digest != op.digest:
            return None, "the op digest does not match the request"
        return digest, ""

    def _begin_sync(
        self,
        rec: WorkerRecord,
        op: OpRef,
        decoded: DecodedRequest,
        response_cls: Callable[..., R],
        stale: Callable[[], R],
    ) -> LedgerEntry | R:
        """Op rules for a synchronous request: a new LedgerEntry to run it,
        or the response to answer now."""
        digest, reason = self._check_op(rec, op, decoded)
        if digest is None:
            return response_cls(status=Status.REJECTED, error=reason)
        begun = self.workers.begin_op(rec, op, digest)
        if begun is None:
            return response_cls(status=Status.REJECTED, error="worker has ended")
        verdict, entry = begun
        if verdict == Verdict.NEW and entry is not None:
            return entry
        if verdict == Verdict.REPLAY and entry is not None:
            entry.done.wait()
            replay: R = entry.response
            return replay
        if verdict == Verdict.STALE:
            return stale()
        if verdict == Verdict.CONFLICT:
            return response_cls(
                status=Status.REJECTED,
                error=f"op {op.seq} was sent before with another digest",
            )
        return response_cls(status=Status.BUSY, error="too many unacknowledged ops")

    @property
    def credit_kinds(self) -> list[PoolKind]:
        """The kinds of the credit tiers' pools, in credit order (the wire
        names tiers by kind), whether or not a tier has pools now."""
        kinds = (self.traits[t].kind for t in self.credit_tiers)
        return [k for k in kinds if k is not None]

    def credit(self, rec: WorkerRecord, decoded: DecodedRequest) -> CreditResponse:
        req: CreditRequest = decoded.request
        if not req.worker_session:
            return CreditResponse(
                status=Status.REJECTED,
                error="credit needs the worker_session of MaruServer",
            )
        if "kind" in decoded.payload:
            return CreditResponse(
                status=Status.REJECTED, error="credit kind is not supported"
            )
        if req.want_bytes <= 0:
            return CreditResponse(
                status=Status.REJECTED, error="want_bytes must be positive"
            )
        entry = self._begin_sync(
            rec,
            req.op,
            decoded,
            CreditResponse,
            lambda: CreditResponse(
                status=Status.REJECTED, error="stale op: seq is at or below its ack"
            ),
        )
        if not isinstance(entry, LedgerEntry):
            return entry

        def grant() -> tuple[list[Location], list[tuple[WorkerEndpoint, int]]]:
            rec.worker_session = req.worker_session
            credits = self.pools.credit(
                rec.incarnation,
                req.want_bytes,
                self.credit_tiers,
            )
            asks = [
                (conn, nbytes)
                for incarnation, nbytes in self.pools.credit_asks(rec.incarnation)
                if (conn := self.workers.endpoint(incarnation)) is not None
            ]
            return credits, asks

        asks: list[tuple[WorkerEndpoint, int]] = []
        try:
            granted = self.workers.while_attached(rec, grant)
            if granted is None:
                response = CreditResponse(status=Status.REJECTED, error="ended")
            else:
                credits, asks = granted
                response = CreditResponse(status=Status.OK, credits=credits)
        except Exception as e:
            logger.exception("CREDIT failed")
            response = CreditResponse(status=Status.REJECTED, error=f"internal: {e}")
        self.ledger.finish(rec.incarnation, entry, response)
        for conn, nbytes in asks:  # idle credit of others, for this waiting worker
            conn.post_notify(Notify(kind=NotifyKind.CREDIT_RETURN, bytes=nbytes))
        return response

    def credit_return(
        self, rec: WorkerRecord, req: CreditReturnRequest
    ) -> CreditReturnResponse:
        if len(req.credits) > MAX_BATCH:
            return CreditReturnResponse(
                status=Status.REJECTED, error=f"more than {MAX_BATCH} items"
            )
        items = []
        for credit in req.credits:
            ok, reason = self.pools.credit_return(rec.incarnation, credit)
            if not ok:
                logger.debug("CREDIT_RETURN item refused: %s", reason)
            items.append(ItemOutcome(ItemResult.OK if ok else ItemResult.REJECTED))
        return CreditReturnResponse(status=Status.OK, items=items)

    def abort(self, rec: WorkerRecord, decoded: DecodedRequest) -> AbortResponse:
        req: AbortRequest = decoded.request
        if not req.credits or len(req.credits) > MAX_BATCH:
            return AbortResponse(
                status=Status.REJECTED, error=f"an ABORT carries 1..{MAX_BATCH} items"
            )
        entry = self._begin_sync(
            rec,
            req.op,
            decoded,
            AbortResponse,
            lambda: AbortResponse(
                status=Status.OK,
                items=[ItemOutcome(ItemResult.STALE)] * len(req.credits),
            ),
        )
        if not isinstance(entry, LedgerEntry):
            return entry
        items = []
        for credit in req.credits:
            try:
                ok, reason = self.pools.abort(rec.incarnation, credit)
            except Exception as e:
                logger.exception("ABORT item failed")
                ok, reason = False, str(e)
            if not ok:
                logger.debug("ABORT item refused: %s", reason)
            items.append(ItemOutcome(ItemResult.OK if ok else ItemResult.REJECTED))
        response = AbortResponse(status=Status.OK, items=items)
        self.ledger.finish(rec.incarnation, entry, response)
        return response

    def write_done(
        self,
        rec: WorkerRecord,
        decoded: DecodedRequest,
        reply: Callable[[WriteDoneResponse], None],
        waiter: WorkerEndpoint | None = None,
    ) -> None:
        """Answer via ``reply`` once every item is published."""
        req: WriteDoneRequest = decoded.request
        if not req.items or len(req.items) > MAX_BATCH:
            reply(
                WriteDoneResponse(
                    status=Status.REJECTED,
                    error=f"a WRITE_DONE carries 1..{MAX_BATCH} items",
                )
            )
            return
        digest, reason = self._check_op(rec, req.op, decoded)
        if digest is None:
            reply(WriteDoneResponse(status=Status.REJECTED, error=reason))
            return
        verdict, entry = self.ledger.begin(rec.incarnation, req.op, digest)
        if verdict == Verdict.STALE:
            reply(
                WriteDoneResponse(
                    status=Status.OK,
                    items=[ItemOutcome(ItemResult.STALE)] * len(req.items),
                )
            )
            return
        if verdict == Verdict.CONFLICT:
            reply(
                WriteDoneResponse(
                    status=Status.REJECTED,
                    error=f"op {req.op.seq} was sent before with another digest",
                )
            )
            return
        if verdict == Verdict.BUSY:
            reply(
                WriteDoneResponse(
                    status=Status.BUSY, error="too many unacknowledged ops"
                )
            )
            return
        assert entry is not None  # NEW and REPLAY carry one
        key = reply if waiter is None else waiter
        if verdict == Verdict.REPLAY:
            waiting, displaced = self.ledger.attach(entry, reply, key)
            if not waiting:
                reply(entry.response)
            elif displaced is not None:
                # Only past the bound: that request asks again later
                displaced(
                    WriteDoneResponse(
                        status=Status.BUSY, error="too many resends of this op"
                    )
                )
            return
        self.ledger.attach(entry, reply, key)
        origin = rec.incarnation

        def finish(items: list[ItemOutcome]) -> None:
            response = WriteDoneResponse(status=Status.OK, items=items)
            for answer in self.ledger.finish(origin, entry, response):
                try:
                    answer(response)
                except Exception:
                    logger.debug("could not answer a WRITE_DONE", exc_info=True)

        outcomes: list[ItemOutcome | None] = [None] * len(req.items)
        pages: dict[int, tuple[PageRef, CommitItem]] = {}
        for i, item in enumerate(req.items):
            try:
                ref, location, why = self.pools.write_done(
                    rec.incarnation, item.credit, item.key, item.logical_bytes
                )
            except Exception as e:
                logger.exception("WRITE_DONE item failed")
                ref, location, why = None, None, str(e)
            if ref is None or location is None:
                logger.debug("WRITE_DONE item %d refused: %s", i, why)
                outcomes[i] = ItemOutcome(ItemResult.REJECTED)
                continue
            pages[i] = (
                ref,
                CommitItem(
                    key=item.key, location=location, logical_bytes=item.logical_bytes
                ),
            )
        self.commit_written(outcomes, pages, finish)

    def unmap_ack(self, rec: WorkerRecord, req: UnmapAckRequest) -> UnmapAckResponse:
        with self._cv:
            self._unmapped.add((rec.incarnation, req.pool_id, req.pool_generation))
            self._cv.notify_all()
        return UnmapAckResponse(status=Status.OK)

    def unmap_pools(self, pools: list[tuple[str, int]], timeout_s: float) -> set[str]:
        """Ask every worker of this run that may map ``pools`` (pool_id,
        generation) to unmap them; the pool_ids each one unmapped (UNMAP_ACK)
        or settled (detached, or ended with its GPU drain grace over) within
        ``timeout_s``. A worker that reconnects is asked again; one whose
        connection dropped counts only once it settles. A timeout is no
        confirmation. The caller stops new ATTACHes from mapping them first.
        Whatever this answers, the RM keeps a region out of reuse while any
        worker holds it."""
        asked: dict[str, WorkerEndpoint | None] = {}
        deadline = time.monotonic() + timeout_s
        with self._cv:
            while True:
                workers = self.workers.mapping()
                for inc in workers:
                    asked.setdefault(inc, None)
                for inc, conn in workers.items():
                    if conn is not None and asked.get(inc) is not conn:
                        asked[inc] = conn
                        for pool_id, generation in pools:
                            conn.post_notify(
                                Notify(
                                    kind=NotifyKind.POOL_REMOVE,
                                    pool_id=pool_id,
                                    pool_generation=generation,
                                )
                            )
                done = {
                    pool_id
                    for pool_id, generation in pools
                    if all(
                        (inc, pool_id, generation) in self._unmapped
                        or self.workers.settled(inc)
                        for inc in asked
                    )
                }
                left = deadline - time.monotonic()
                if len(done) == len(pools) or left <= 0:
                    return done
                # An exit or a reconnect does not notify: look again shortly
                self._cv.wait(min(left, 0.05))

    def _queue_exit(self, ref: WorkerRef) -> None:
        with self._cv:
            self._exits.append(ref)
            self._cv.notify_all()

    def _exit_loop(self) -> None:
        pause = EXIT_RETRY_S
        while True:
            with self._cv:
                while not self._stopping and (not self._exits or self._paused()):
                    self._cv.wait()
                if self._stopping:
                    return
                batch = [
                    self._exits.popleft()
                    for _ in range(min(MAX_BATCH, len(self._exits)))
                ]
            try:
                response = self.metadata.worker_exit(batch, ExitCause.EXIT)
                ok = response.status == Status.OK
                if not ok:
                    logger.error(
                        "WORKER_EXIT refused as a whole: %s %s; trying again",
                        response.status,
                        response.error,
                    )
            except EpochChanged as e:
                # The run is over: MaruServer drops the session's workers
                self.end_run(f"MaruServer no longer knows the session ({e})")
                continue
            except ServerStopped:
                return
            except Exception:
                # An exit is idempotent: a new op confirms it
                logger.exception("WORKER_EXIT failed; trying again")
                ok = False
            if not ok:
                with self._cv:
                    self._exits.extendleft(reversed(batch))
                    self._cv.wait(pause)
                pause = min(pause * 2, EXIT_RETRY_MAX_S)
                continue
            pause = EXIT_RETRY_S
            for ref, outcome in zip(batch, response.items, strict=True):
                if outcome.result != ItemResult.OK:
                    logger.warning(
                        "WORKER_EXIT of %s answered %s", ref.incarnation, outcome.result
                    )
            logger.info("reported the exit of %s", [r.incarnation for r in batch])

    def pending_exits(self) -> int:
        with self._lock:
            return len(self._exits)
