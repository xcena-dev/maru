# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageManager: credit, WRITE_DONE -> COMMIT, removal (eviction, write-back
demotion, RETIRE and central deletes) and WORKER_EXIT reports, each in the
order it needs.

Worker records live in a WorkerRegistry, op results in an OpLedger; MaruServer
is reached through a MetadataPort. A request whose result stays unknown, and a
node session MaruServer no longer knows, end the run (``on_end``): the
StorageService starts a new one.
"""

import collections
import dataclasses
import functools
import logging
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
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
from maru_common.protocol import CommitItem, MoveItem, NodeHeartbeatResponse
from maru_common.remote_protocol import TransferEnd, Written
from maru_common.serializer import DecodedRequest, op_digest
from maru_common.storage_types import (
    MAX_BATCH,
    AccessInfo,
    BlockRequest,
    ExitCause,
    ItemOutcome,
    ItemResult,
    KeptCause,
    Key,
    KeyLocation,
    Location,
    NotifyKind,
    OpRef,
    PoolKind,
    Status,
    WorkerRef,
)

from .config import TierTraits
from .local_pin import LocalPins
from .op_ledger import OP_WINDOW, LedgerEntry, OpLedger, Verdict
from .placement import PlacementPolicy
from .pool_manager import InvalidTransitionError, PageRef, PageState, PoolManager
from .ports import (
    EpochChanged,
    MetadataPort,
    ProcessWatcher,
    ReplyError,
    ServerStopped,
    WorkerEndpoint,
)
from .tiers import IOResult, Level, RemoteTier
from .transfer_engine import Copy, TransferEngine
from .worker_registry import WorkerRecord, WorkerRegistry

logger = logging.getLogger(__name__)

R = TypeVar("R")

DEFAULT_GPU_DRAIN_GRACE_S = 5.0
DEFAULT_EVICT_INTERVAL_S = 0.05
# Wait for more WRITE_DONE items to join a COMMIT batch
COMMIT_LINGER_S = 0.002
EXIT_RETRY_S = 0.5
EXIT_RETRY_MAX_S = 30.0
# Demotion into an I/O tier (SSD): at most this much a pass, so room comes in
# steps, and this many writes in flight at a time: reads keep the rest of
# the executor's queue
IO_BATCH_BYTES = 1 << 30
IO_DEPTH = 2
# This many failed writes in a row to a pool below (an error, a full queue,
# never started, or ended late) take it out of demotion for IO_PAUSE_S: the
# tier above then drops past its high watermark, as when the tier below is
# full. One failure only keeps the source for a retry
IO_FAILURES_MAX = 3
IO_PAUSE_S = 10.0


@dataclass(eq=False)
class _WriteDone:
    """Written pages committing: ``finish`` gets every item's outcome."""

    finish: Callable[[list[ItemOutcome]], None]
    outcomes: list[ItemOutcome | None]
    remaining: int


@dataclass(eq=False)
class _Ticket:
    ref: PageRef
    item: CommitItem
    write_done: _WriteDone
    index: int


@dataclass(eq=False)
class _Move:
    """A READY page moving to the tier below (write-back demotion)."""

    src: PageRef
    key: Key
    source: Location
    dest: PageRef
    dest_location: Location  # what is committed: the logical bytes
    finalizer_id: str = ""  # once ADDED: FINALIZE_MOVE releases it
    # The copy is READY below already (left by a KEPT move): the COMMIT of the
    # same location only places a finalizer on it again
    existing: bool = False
    # A write-through copy of a page of this tier: committed with no
    # finalizer, its source never moves
    through: str | None = None


@dataclass(eq=False)
class _RemoteMove:
    """A READY page moving to a memory node (write-back demotion)."""

    src: PageRef
    key: Key
    source: Location
    credit: Location  # the credited page, whole
    dest: Location  # what is committed there: the logical bytes
    # The copy is READY there already: only FINALIZE_MOVE runs
    existing: bool = False
    # As _Move.through
    through: str | None = None


class StorageManager:
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
        transfers: TransferEngine | None = None,
        on_end: Callable[[str], None] = lambda reason: None,
        remote: dict[str, RemoteTier] | None = None,
    ):
        """``on_end(reason)``: this run cannot go on (once, from any thread).
        ``remote``: the remote tiers by store name: demotions to them go to
        memory nodes."""
        self.node_epoch = node_epoch
        # By tier name, in config order
        self.traits = traits or {
            "dram": TierTraits(
                "dram", Level.L1, PoolKind.DRAM, credit=True, evicts=True
            )
        }
        # Credit comes from the top tier, then from the fallback tiers in order
        self.credit_tiers = [n for n, t in self.traits.items() if t.credit] + [
            n for n, t in self.traits.items() if t.credit_fallback
        ]
        self.pools = pool_manager
        self.placement = placement
        # Copies for demotion; without it a tier that demotes drops instead
        self.transfers = transfers
        self.remote = dict(remote or {})
        self.local_pins = local_pins
        self.metadata = metadata
        self.evict_interval_s = evict_interval_s
        self._on_end = on_end
        self.ledger = OpLedger(op_window)
        self.watcher = watcher
        self.workers = WorkerRegistry(
            node_epoch=node_epoch,
            pools=pool_manager,
            ledger=self.ledger,
            watcher=self.watcher,
            on_exit=self._queue_exit,
            on_settled=on_worker_settled,
            on_ended=self._worker_ended,
            gpu_drain_grace_s=gpu_drain_grace_s,
        )

        self._lock = threading.RLock()
        self._cv = threading.Condition(self._lock)
        self._tickets: collections.deque[_Ticket] = collections.deque()
        self._exits: collections.deque[WorkerRef] = collections.deque()
        self._retiring: set[PageRef] = set()
        # Pages of block requests to RETIRE at a time (they stay blocked)
        self._retire_queue: dict[PageRef, float] = {}
        self._central: set[PageRef] = set()
        self._blocks: dict[str, set[PageRef]] = {}
        self._evict_wake = threading.Event()
        # Why this run is over: nothing more goes to MaruServer
        self._ended: str | None = None
        self._stopping = False
        self._threads: list[threading.Thread] = []
        self._unmapped: set[tuple[str, str, int]] = set()
        # RETIRE results by outcome ("RETIRED", "KEPT:PINNED", ...)
        self.retire_results: collections.Counter[str] = collections.Counter()
        # Demotions: FINALIZE_MOVE results as above, plus "COMMIT:<result>",
        # "COPY_FAILED" and "DROPPED" (no room below past the high watermark)
        self.move_results: collections.Counter[str] = collections.Counter()
        # Bytes each tier copied into each tier below and committed there
        # (write-back and write-through), as "move_<tier>_to_<below>"
        self.moved_bytes: collections.Counter[str] = collections.Counter()
        # Source -> its READY copy below, left by a KEPT or DUPLICATE result:
        # the next demotion finalizes onto that copy instead of copying
        self._copies: dict[PageRef, PageRef] = {}
        # Pools MaruServer called INVALID: no demotion goes there
        self._invalid_pools: set[str] = set()
        # FREE bytes a fetch wants now, by tier (see want_room)
        self._demand: dict[str, int] = {}
        # Pools below whose writes keep failing: failures in a row, and until
        # when the pool takes no demotion
        self._io_failures: collections.Counter[str] = collections.Counter()
        self._io_paused: dict[str, float] = {}
        # Demotion writes submitted and not yet ended; ended ones wait for
        # the eviction thread to COMMIT or drop them
        self._writing = 0
        self._written: collections.deque[tuple[_Move, IOResult]] = collections.deque()
        # The same for writes to memory nodes, and kept ones that ended DONE
        # after all (settled)
        self._remote_writing = 0
        self._remote_written: collections.deque[tuple[_RemoteMove, IOResult]] = (
            collections.deque()
        )
        self._remote_settled: collections.deque[_RemoteMove] = collections.deque()
        # Source -> its READY copy on a memory node: the next demotion only
        # finalizes onto it
        self._remote_copies: dict[PageRef, Location] = {}
        # Source -> (key, writes): its writes there whose end is not known
        # (kept, or WRITE_DONE unanswered) may still commit, so it goes to no
        # other tier below meanwhile
        self._remote_unsettled: dict[PageRef, tuple[Key, int]] = {}
        # Write-through: pages to copy below now, the bytes of copies in
        # flight per tier, and how the copies went (COPIED bytes, SKIPPED,
        # NO_ROOM, COPY_FAILED, COMMIT:<result>)
        self._through: collections.deque[PageRef] = collections.deque()
        self._through_inflight: collections.Counter[str] = collections.Counter()
        # Sources whose write-through copy is not settled yet (in flight, or
        # ended with its result not applied): no demotion of them
        self._through_active: set[PageRef] = set()
        self.through_results: collections.Counter[str] = collections.Counter()
        # FETCH from workers: a FetchManager on a node with an SSD tier
        self.fetches = None

    # ------------------------------------------------------------- lifecycle

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

    def start_heartbeat(self) -> None:
        """Report pool usage and act on each reply; a lost session ends the
        run."""
        self.metadata.start_heartbeat(self.pools.usage, self.on_heartbeat, self.end_run)

    def stop(self) -> None:
        """Stop the threads; unresolved requests are given up."""
        with self._cv:
            self._stopping = True
            self._cv.notify_all()
        self.workers.stop()
        self._evict_wake.set()
        self.metadata.stop()
        if self.fetches is not None:
            self.fetches.stop()
        for thread in self._threads:
            thread.join(timeout=10.0)
        self._threads.clear()
        self.watcher.stop()

    @property
    def ended(self) -> str | None:
        """Why this run is over, or None."""
        return self._ended

    def end_run(self, reason: str) -> None:
        """This run cannot go on (MaruServer restarted or ended it, or a
        result stays unknown): no new credit, nothing more to MaruServer, and
        ``on_end`` starts a new run. Pages whose state is unknown are never
        reused: they go with the run."""
        with self._cv:
            if self._ended is not None or self._stopping:
                return
            self._ended = reason
            self._cv.notify_all()
        self.pools.stop_credit(reason)
        logger.error("this run ends: %s", reason)
        self._on_end(reason)

    def local_pins_open(self) -> bool:
        """Whether a new local pin may be given (none is while local pins are
        off): never once the run ended, nor once MaruServer has not confirmed
        the session for node_stale_s (half the session TTL)."""
        if self._ended is not None or self.metadata.epoch_lost:
            return False
        last = self.metadata.last_heartbeat_ok
        stale_s = self.metadata.session_ttl_s / 2
        return last > 0 and time.monotonic() - last < stale_s

    def stats(self) -> dict:
        with self._lock:
            pending = len(self._tickets)
            retire = dict(self.retire_results)
            demotions = dict(self.move_results)
            moved = dict(self.moved_bytes)
            through = dict(self.through_results)
        return {
            "ended": self._ended,
            "pending_commits": pending,
            "retire_results": retire,
            "demotions": demotions,
            "moved_bytes": moved,
            "through": through,
            "credit_short": dict(self.pools.credit_short),
            "credit_short_bytes": dict(self.pools.credit_short_bytes),
        }

    # -------------------------------------------------------------- requests

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
        verdict, entry = self.ledger.begin(rec.incarnation, op, digest)
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
        credit_kinds = self.credit_kinds
        if req.kind is not None and req.kind not in credit_kinds:
            return CreditResponse(
                status=Status.REJECTED,
                error=f"credit comes from the {credit_kinds} tiers, not {req.kind}",
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
                self.credit_tiers
                if req.kind is None
                else [t for t in self.credit_tiers if self.traits[t].kind == req.kind],
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

    def commit_written(
        self,
        outcomes: list[ItemOutcome | None],
        pages: dict[int, tuple[PageRef, CommitItem]],
        finish: Callable[[list[ItemOutcome]], None],
    ) -> None:
        """COMMIT written PENDING pages, by item index, in the next batches;
        ``finish`` gets every item's outcome once all are known (``outcomes``
        holds those of the other items)."""
        wd = _WriteDone(finish=finish, outcomes=list(outcomes), remaining=len(pages))
        tickets = [
            _Ticket(ref=ref, item=item, write_done=wd, index=i)
            for i, (ref, item) in pages.items()
        ]
        if not tickets:
            self._finish_write_done(wd)
            return
        with self._cv:
            self._tickets.extend(tickets)
            self._cv.notify_all()

    def _finish_write_done(self, wd: _WriteDone) -> None:
        items = [o for o in wd.outcomes if o is not None]
        assert len(items) == len(wd.outcomes)  # every item has its outcome
        try:
            wd.finish(items)
        except Exception:
            logger.exception("finishing a WRITE_DONE failed")

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

    # ---------------------------------------------------------------- commit

    def _paused(self) -> bool:
        """Caller holds the lock: nothing goes to MaruServer now."""
        return self._ended is not None

    def _commit_loop(self) -> None:
        while True:
            with self._cv:
                while not self._stopping and (not self._tickets or self._paused()):
                    self._cv.wait()
                if self._stopping:
                    return
                if len(self._tickets) < MAX_BATCH:
                    self._cv.wait(COMMIT_LINGER_S)
                    if self._stopping:
                        return
                    if self._paused():
                        continue
                batch = [
                    self._tickets.popleft()
                    for _ in range(min(MAX_BATCH, len(self._tickets)))
                ]
            if batch:
                self._commit_batch(batch)
            if self._stopping:
                return

    def _commit_batch(self, batch: list[_Ticket]) -> None:
        try:
            response = self.metadata.commit([t.item for t in batch])
        except (EpochChanged, ServerStopped) as e:
            # Not sent again: the run is over
            with self._cv:
                self._tickets.extendleft(reversed(batch))
            if isinstance(e, EpochChanged):
                self.end_run(f"MaruServer no longer knows the session ({e})")
            return
        except ReplyError:
            # It ran, but the results are unusable
            self._apply_commit(batch, None)
            return
        except Exception:
            logger.exception("COMMIT failed; the pages stay PENDING")
            with self._cv:
                self._tickets.extendleft(reversed(batch))
            time.sleep(0.1)
            return
        self._apply_commit(batch, response)

    def _apply_commit(self, batch: list[_Ticket], response) -> None:
        outcomes: list[ItemOutcome | None]
        if response is None:
            outcomes = [None] * len(batch)
        elif response.status == Status.OK:
            outcomes = response.items
        else:
            # Not run: none of these locations is in the Catalog
            logger.error(
                "COMMIT refused as a whole (%s %s); %d pages are freed",
                response.status,
                response.error,
                len(batch),
            )
            outcomes = [ItemOutcome(ItemResult.REJECTED)] * len(batch)
        added = False
        finished = []
        for ticket, outcome in zip(batch, outcomes, strict=True):
            try:
                answer, freshly_added = self._apply_outcome(ticket, outcome)
                added = added or freshly_added
            except Exception:
                logger.exception("applying the COMMIT result of %s failed", ticket.ref)
                answer = ItemOutcome(ItemResult.REJECTED)
            wd = ticket.write_done
            with self._lock:
                wd.outcomes[ticket.index] = answer
                wd.remaining -= 1
                if wd.remaining == 0:
                    finished.append(wd)
        for wd in finished:
            self._finish_write_done(wd)
        if added:
            self._evict_wake.set()

    def _apply_outcome(self, ticket: _Ticket, outcome: ItemOutcome | None):
        """Move the page by its COMMIT result; returns (answer, added)."""
        result = outcome.result if outcome is not None else None
        if result == ItemResult.ADDED:
            self.pools.commit_added(ticket.ref)
            self.placement.on_ready(ticket.ref)
            self._note_ready(ticket.ref)
            return outcome, True
        if result in (ItemResult.DUPLICATE, ItemResult.REJECTED):
            self.pools.commit_freed(ticket.ref)
            return outcome, False
        # Unknown whether the location is in the Catalog: the page stays
        # PENDING and goes with the run
        self.end_run(f"COMMIT of page {ticket.ref} answered {result}")
        return ItemOutcome(ItemResult.REJECTED), False

    # --------------------------------------------------------------- removal

    def _evict_loop(self) -> None:
        while True:
            self._evict_wake.wait(self.evict_interval_s)
            self._evict_wake.clear()
            with self._cv:
                if self._stopping:
                    return
                run = not self._paused()
            try:
                self._reclaim_retiring()
                self._writes_ended()
                self._remote_writes_ended()
                if run:
                    self._copy_through()
                    self._retire_queued()
                    self._evict_once()
            except ServerStopped:
                return
            except Exception:
                logger.exception("eviction pass failed")

    def want_room(self, tier: str, nbytes: int) -> None:
        """A fetch needs ``nbytes`` FREE in ``tier`` now: the next
        pass evicts up to that much whatever the watermarks, pages with a
        copy below first. Asked again, the larger demand stands."""
        with self._lock:
            self._demand[tier] = max(self._demand.get(tier, 0), nbytes)
        self._evict_wake.set()

    def _evict_once(self) -> None:
        # Lower tiers first: the room they make takes the pages from above
        for name in reversed(list(self.traits)):
            if self._halted():  # the session went during this pass
                return
            traits = self.traits[name]
            with self._lock:
                demand = self._demand.pop(name, 0)
                prefer = (
                    set(self._copies) | set(self._remote_copies) if demand else set()
                )
            if not traits.evicts:
                continue
            pools = self._evictable_pools(name)
            if not pools:
                continue
            capacity, free, retiring, page_size = self._tier_usage(pools)
            targets = traits.demotes_to
            limit = MAX_BATCH
            if any(self.traits[t].io_only for t in targets):
                limit = max(1, min(limit, IO_BATCH_BYTES // page_size))
            candidates = self.placement.candidates(
                name,
                pools,
                capacity=capacity,
                free_bytes=free,
                retiring_bytes=retiring,
                page_size=page_size,
                limit=limit,
                demand_bytes=demand,
                prefer=prefer,
            )
            refs, items = self._ready_items(candidates)
            if not refs:
                continue
            if targets and self.transfers is not None:
                self._demote(name, pools, targets, refs, items)
            else:
                self.retire(refs, items)

    def _halted(self) -> bool:
        """Nothing may go to MaruServer now."""
        with self._lock:
            paused = self._paused() or self._stopping
        return paused or self.metadata.epoch_lost

    def _evictable_pools(self, name: str) -> list[str]:
        """The tier's pools pressure is measured over and removed from: not
        invalid, not quarantined for an earlier run and (in the credit tier)
        creditable."""
        traits = self.traits[name]
        with self._lock:
            invalid = set(self._invalid_pools)
        pools = []
        for pool_id in self.pools.pool_ids():
            if (
                self.pools.tier_of(pool_id) != name
                or pool_id in invalid
                or self.pools.pool_quarantined(pool_id)
                or (
                    (traits.credit or traits.credit_fallback)
                    and not self.pools.creditable(pool_id)
                )
            ):
                continue
            pools.append(pool_id)
        return pools

    def _tier_usage(self, pools: list[str]) -> tuple[int, int, int, int]:
        """Capacity, FREE bytes, RETIRING bytes and page size over ``pools``."""
        capacity = free = retiring = page_size = 0
        for pool_id in pools:
            info = self.pools.pool_info(pool_id)
            if info is None:
                continue
            capacity += info.capacity
            free += self.pools.bytes_in(PageState.FREE, pool_id)
            retiring += self.pools.bytes_in(PageState.RETIRING, pool_id)
            page_size = info.page_size
        return capacity, free, retiring, page_size

    def _ready_items(self, candidates) -> tuple[list[PageRef], list[KeyLocation]]:
        refs, items = [], []
        for ref in candidates:
            page = self.pools.page(ref)
            if (
                page is None
                or page.state != PageState.READY
                or page.key is None
                or page.location is None
            ):
                self.placement.on_gone(ref)
                self._forget_queued(ref)
                continue
            refs.append(ref)
            items.append(KeyLocation(key=page.key, location=page.location))
        return refs, items

    # ------------------------------------------------------------- demotion

    def _demote(
        self,
        name: str,
        pools: list[str],
        targets: tuple[str, ...],
        refs: list[PageRef],
        items: list[KeyLocation],
    ) -> None:
        """Write-back: copy READY pages to the tier below, COMMIT the copies
        with a finalizer, then FINALIZE_MOVE the sources. With two tiers
        below, the first (remote) takes what it can and the last the rest. A
        source with a copy below already gets a finalizer on that copy
        instead. Pages that find no room wait for the tier below to evict (it
        ran first in this pass); while the tier (``pools``) is past its high
        watermark they are dropped instead: writes must not stall."""
        with self._lock:
            busy = {r for r in refs if r in self._through_active}
        if busy:  # their copy below is on its way: finalized onto it later
            self.placement.release(list(busy))
            kept = [(r, i) for r, i in zip(refs, items, strict=True) if r not in busy]
            refs, items = [r for r, _ in kept], [i for _, i in kept]
            if not refs:
                return
        *first, below = targets
        for target in first:
            refs, items = self._demote_remote(
                name, pools, self.remote[target], refs, items, spill=True
            )
            if not refs:
                return
        if below in self.remote:
            self._demote_remote(name, pools, self.remote[below], refs, items)
            return
        moves: list[_Move] = []
        todo, drop = [], []
        io = self.traits[below].io_only
        assert self.transfers is not None
        for ref, item in zip(refs, items, strict=True):
            copy = self._copy_of(ref, item.key)
            if copy is not None:
                moves.append(
                    _Move(ref, item.key, item.location, copy[0], copy[1], existing=True)
                )
            elif not self.transfers.has_pool(ref.pool_id):
                # Never copied out (a region the engine could not map): dropped
                drop.append((ref, item))
            else:
                todo.append((ref, item))
        if io:
            # Bounded writes in flight; the rest are offered again
            with self._lock:
                slots = max(0, IO_DEPTH - self._writing)
            self.placement.release([ref for ref, _ in todo[slots:]], backoff=False)
            todo = todo[:slots]
        dests = self._reserve_below(below, len(todo))
        for (ref, item), dest in zip(todo, dests, strict=False):
            location = self.pools.transfer_location(dest)
            moves.append(
                _Move(
                    ref,
                    item.key,
                    item.location,
                    dest,
                    dataclasses.replace(location, length=item.location.length),
                )
            )
        rest = todo[len(dests) :]
        if rest:
            capacity, free, _, _ = self._tier_usage(pools)
            if self.placement.above_high(name, capacity=capacity, free_bytes=free):
                logger.debug("no room below %s: dropping %d pages", name, len(rest))
                with self._lock:
                    self.move_results["DROPPED"] += len(rest)
                drop += rest
            else:
                self.placement.release([ref for ref, _ in rest], backoff=False)
        # Every reserved page and every candidate is settled, also on an error
        try:
            if moves:
                self._run_moves(moves, io=io)
        finally:
            if drop:
                self.retire([r for r, _ in drop], [i for _, i in drop])

    # ---------------------------------------------------------- write-through

    def _note_ready(self, ref: PageRef) -> None:
        """A page became READY in its tier (written, demoted or copied in):
        copied below at once if the tier writes through."""
        name = self.pools.tier_of(ref.pool_id)
        traits = self.traits.get(name) if name is not None else None
        if traits is not None and traits.copies_to:
            with self._lock:
                self._through.append(ref)
            self._evict_wake.set()

    def _copy_through(self) -> None:
        """Copy the pages noted READY in write-through tiers to the tier
        below, each tier within its in-flight bytes (past it a page is
        skipped: pressure demotes it later). The copy is committed with no
        finalizer and kept as the source's copy below; nothing waits."""
        with self._lock:
            queued = list(dict.fromkeys(self._through))
            self._through.clear()
        if not queued or self.transfers is None or self._halted():
            return
        by_tier: dict[str, list[PageRef]] = {}
        for ref in queued:
            name = self.pools.tier_of(ref.pool_id)
            if name is not None and self.traits.get(name) is not None:
                by_tier.setdefault(name, []).append(ref)
        for name, refs in by_tier.items():
            try:
                self._copy_tier(name, refs)
            except ServerStopped:
                raise
            except Exception:
                logger.exception("write-through copies of %s failed", name)

    def _copy_tier(self, name: str, refs: list[PageRef]) -> None:
        traits = self.traits[name]
        if not traits.copies_to:
            return
        todo: list[tuple[PageRef, KeyLocation]] = []
        with self._lock:
            room = traits.through_max_inflight - self._through_inflight[name]
        for ref in refs:
            page = self.pools.page(ref)
            if (
                page is None
                or page.state != PageState.READY
                or page.key is None
                or page.location is None
            ):
                continue
            with self._lock:
                copied = ref in self._copies or ref in self._remote_copies
            if copied:
                continue
            nbytes = page.location.length
            if nbytes > room:
                with self._lock:
                    self.through_results["SKIPPED"] += 1
                continue
            room -= nbytes
            todo.append((ref, KeyLocation(key=page.key, location=page.location)))
        if not todo:
            return
        # One copy per page: a remote tier makes those it gets credit for and
        # posts, the next tier the rest
        for below in traits.copies_to:
            last = below == traits.copies_to[-1]
            if below in self.remote:
                tier = self.remote[below]
                credits = tier.reserve([item.key for _, item in todo])
                left = [p for p, c in zip(todo, credits, strict=True) if c is None]
                if last and left:
                    with self._lock:
                        self.through_results["NO_ROOM"] += len(left)
                for (ref, item), credit in zip(todo, credits, strict=True):
                    if credit is None:
                        continue
                    dest = dataclasses.replace(credit, length=item.location.length)
                    move = _RemoteMove(
                        ref, item.key, item.location, credit, dest, through=name
                    )
                    if self._write_remote(tier, move):
                        continue
                    if last:
                        self._count(move, "COPY_FAILED")
                    else:
                        left.append((ref, item))
                todo = left
            else:
                dests = self._reserve_below(below, len(todo))
                if last:
                    with self._lock:
                        self.through_results["NO_ROOM"] += len(todo) - len(dests)
                moves = [
                    _Move(
                        ref,
                        item.key,
                        item.location,
                        dest,
                        dataclasses.replace(
                            self.pools.transfer_location(dest),
                            length=item.location.length,
                        ),
                        through=name,
                    )
                    for (ref, item), dest in zip(todo, dests, strict=False)
                ]
                todo = todo[len(dests) :]
                if moves:
                    self._run_moves(moves, io=self.traits[below].io_only)
            if not todo:
                return

    def _release(
        self, moves: list[_Move] | list[_RemoteMove], backoff: bool = True
    ) -> None:
        """Offer the moves' sources again; a write-through copy never took
        its source."""
        refs = [m.src for m in moves if m.through is None]
        if refs:
            self.placement.release(refs, backoff=backoff)

    def _count(self, move: _Move | _RemoteMove, result: str) -> None:
        moved = None
        if result == "COMMIT:ADDED" and not move.existing:
            below = (
                self.pools.tier_of(move.dest.pool_id)
                if isinstance(move, _Move)
                else RemoteTier.name
            )
            moved = f"move_{self.pools.tier_of(move.src.pool_id)}_to_{below}"
        with self._lock:
            if moved is not None:
                self.moved_bytes[moved] += move.source.length
            if move.through is None:
                self.move_results[result] += 1
                return
            self.through_results[result] += 1
            if result == "COMMIT:ADDED":
                self.through_results["COPIED"] += move.source.length

    def _through_ended(self, move: _Move | _RemoteMove) -> None:
        """Caller holds the lock: a write-through copy left the wire."""
        if move.through is not None:
            self._through_inflight[move.through] -= move.source.length

    def _through_settled(self, moves: Iterable[_Move | _RemoteMove]) -> None:
        """Caller holds the lock: these copies' results are being applied
        on the eviction thread, before its demotions run."""
        for move in moves:
            if move.through is not None:
                self._through_active.discard(move.src)

    # ------------------------------------------------- demotion to memory nodes

    def _demote_remote(
        self,
        name: str,
        pools: list[str],
        tier: RemoteTier,
        refs: list[PageRef],
        items: list[KeyLocation],
        spill: bool = False,
    ) -> tuple[list[PageRef], list[KeyLocation]]:
        """Write-back to memory nodes: RESERVE pages there, WRITE, WRITE_DONE
        (the memory node commits them), then FINALIZE_MOVE the sources with
        no finalizer. A source with a copy there already is only finalized.
        With no room there the pages wait, or past the high watermark are
        dropped, as for a local tier. With ``spill`` the pages it cannot take
        now (no WRITE slot, no credit, a WRITE not posted) and those with a
        copy in a local tier below are returned for the next tier instead,
        except a source whose earlier WRITE may still commit there."""
        assert self.transfers is not None
        moves: list[_RemoteMove] = []
        todo: list[tuple[PageRef, KeyLocation]] = []
        passed: set[PageRef] = set()
        for ref, item in zip(refs, items, strict=True):
            with self._lock:
                copy = self._remote_copies.get(ref)
            if copy is not None:
                moves.append(
                    _RemoteMove(ref, item.key, item.location, copy, copy, existing=True)
                )
            elif spill and self._copy_of(ref, item.key) is not None:
                passed.add(ref)
            else:
                todo.append((ref, item))
        with self._lock:
            slots = max(0, tier.io_depth - self._remote_writing)
            # Not passed on: a late commit there would leave the key in both
            stay = {r for r, i in todo if spill and self._may_commit(r, i.key)}

        def spills(ref: PageRef) -> bool:
            return spill and ref not in stay

        passed.update(ref for ref, _ in todo[slots:] if spills(ref))
        self.placement.release(
            [ref for ref, _ in todo[slots:] if not spills(ref)], backoff=False
        )
        todo = todo[:slots]
        credits = tier.reserve([item.key for _, item in todo]) if todo else []
        rest, drop = [], []
        for (ref, item), credit in zip(todo, credits, strict=True):
            if credit is None:
                rest.append((ref, item))
                continue
            dest = dataclasses.replace(credit, length=item.location.length)
            moves.append(_RemoteMove(ref, item.key, item.location, credit, dest))
        passed.update(ref for ref, _ in rest if spills(ref))
        rest = [(ref, item) for ref, item in rest if not spills(ref)]
        if rest:
            capacity, free, _, _ = self._tier_usage(pools)
            if self.placement.above_high(name, capacity=capacity, free_bytes=free):
                logger.debug("no room below %s: dropping %d pages", name, len(rest))
                with self._lock:
                    self.move_results["DROPPED"] += len(rest)
                drop += rest
            else:
                self.placement.release([ref for ref, _ in rest], backoff=False)
        try:
            fresh = [m for m in moves if not m.existing]
            existing = [m for m in moves if m.existing]
            if self._halted():  # nothing posted: the credits go back
                tier.cancel([m.credit for m in fresh])
                self._release(moves, backoff=False)
            else:
                for move in fresh:
                    if self._write_remote(tier, move):
                        continue
                    if spills(move.src):
                        passed.add(move.src)
                    else:
                        self._count(move, "COPY_FAILED")
                        self._release([move])
                if existing:
                    self._finalize_remote(tier, existing)
        finally:
            if drop:
                self.retire([r for r, _ in drop], [i for _, i in drop])
        left = [(r, i) for r, i in zip(refs, items, strict=True) if r in passed]
        return [r for r, _ in left], [i for _, i in left]

    def _write_remote(self, tier: RemoteTier, move: _RemoteMove) -> bool:
        """Post the WRITE; the source keeps an I/O reference until it ends,
        or, if the tier kept it, until it settles. False if not posted: the
        credit went back and the caller passes the source on or releases
        it."""
        assert self.transfers is not None
        if not self.pools.io_acquire_one(move.src):  # the source went
            tier.cancel([move.credit])
            self._release([move])
            return True
        with self._lock:
            self._remote_writing += 1
            if move.through is not None:
                self._through_inflight[move.through] += move.source.length
                self._through_active.add(move.src)
        try:
            self.transfers.submit_io(
                Copy(move.source, move.dest, move.source.length),
                move.src,
                None,
                callback=functools.partial(self._remote_write_ended, move),
                settled=functools.partial(self._remote_write_settled, move),
            )
        except Exception as e:  # not posted: the credit goes back
            with self._lock:
                self._remote_writing -= 1
                self._through_ended(move)
                self._through_settled([move])
            self.pools.io_release([move.src])
            logger.debug("cannot write %s to a memory node: %s", move.src, e)
            tier.cancel([move.credit])
            return False
        return True

    def _remote_write_ended(self, move: _RemoteMove, result: IOResult) -> None:
        """On the tier's thread: no RPC here."""
        if not result.kept:
            self.pools.io_release([move.src])
        with self._lock:
            self._remote_writing -= 1
            self._through_ended(move)
            self._remote_written.append((move, result))
        self._evict_wake.set()

    def _remote_write_settled(self, move: _RemoteMove, result: IOResult) -> None:
        """A kept write ended DONE after all: its source is free to go."""
        self.pools.io_release([move.src])
        with self._lock:
            self._remote_settled.append(move)
        self._evict_wake.set()

    def flush_remote(self) -> None:
        """Tell the memory nodes how the drained writes ended (the teardown,
        before the remote tier says GOODBYE)."""
        self._remote_writes_ended()

    def _remote_writes_ended(self) -> None:
        """Tell the memory nodes how the writes ended; FINALIZE_MOVE the
        sources of those they committed."""
        with self._lock:
            done = list(self._remote_written)
            self._remote_written.clear()
            self._through_settled(m for m, _ in done)
            settled = list(self._remote_settled)
            self._remote_settled.clear()
        if not (done or settled) or not self.remote:
            return
        tier = next(iter(self.remote.values()))
        ok = [m for m, r in done if r.ok]
        failed = [m for m, r in done if not r.ok]
        self._unsettle([m for m, r in done if r.kept])  # a late DONE may come
        for move in failed:  # the source stays; the memory node quarantines
            self._count(move, "COPY_FAILED")
            self._release([move])
        self._remote_write_done(tier, failed, TransferEnd.UNKNOWN)
        # A late DONE commits there: the next demotion finalizes onto it
        late = self._remote_write_done(tier, settled, TransferEnd.DONE)
        if late is not None:
            self._settle(settled)
            for move, outcome in zip(settled, late, strict=True):
                self._count(move, f"COMMIT:{outcome.result.value}")
                self._note_remote_copy(move, outcome)
        if not ok:
            return
        outcomes = self._remote_write_done(tier, ok, TransferEnd.DONE)
        if outcomes is None:  # unknown: the sources stay, maybe copied there
            self._unsettle(ok)
            self._release(ok)
            return
        finals: list[_RemoteMove] = []
        for move, outcome in zip(ok, outcomes, strict=True):
            self._count(move, f"COMMIT:{outcome.result.value}")
            if move.through is not None:  # a copy: noted, the source stays
                self._note_remote_copy(move, outcome)
            elif outcome.result == ItemResult.ADDED:
                finals.append(move)
            elif (
                outcome.result == ItemResult.DUPLICATE and outcome.location is not None
            ):
                finals.append(
                    dataclasses.replace(move, dest=outcome.location, existing=True)
                )
            else:
                self.placement.release([move.src])
        if finals and not self._halted():
            self._finalize_remote(tier, finals)
        elif finals:
            self.placement.release([m.src for m in finals])

    def _remote_write_done(
        self, tier: RemoteTier, moves: list[_RemoteMove], end: TransferEnd
    ) -> list[ItemOutcome] | None:
        """WRITE_DONE of ``moves`` ending ``end``; None if the answer is not
        known (the tier sends it again)."""
        if not moves:
            return []
        try:
            return tier.write_done(
                [Written(m.key, m.credit, m.source.length, end) for m in moves]
            )
        except Exception as e:
            with self._lock:
                self.move_results["COMMIT:UNKNOWN"] += len(moves)
            logger.warning("WRITE_DONE of %d pages: answer unknown (%s)", len(moves), e)
            return None

    def note_remote_copy(self, ref: PageRef, key: Key, location: Location) -> None:
        """``ref`` holds ``key`` as ``location`` on a memory node does: its
        demotion only finalizes onto that copy."""
        if self._is_ready(ref, key):
            with self._lock:
                self._remote_copies[ref] = location

    def _unsettle(self, moves: list[_RemoteMove]) -> None:
        with self._lock:
            for move in moves:
                key, n = self._remote_unsettled.get(move.src, (move.key, 0))
                n = n + 1 if key == move.key else 1
                self._remote_unsettled[move.src] = (move.key, n)

    def _settle(self, moves: list[_RemoteMove]) -> None:
        with self._lock:
            for move in moves:
                key, n = self._remote_unsettled.get(move.src, (None, 0))
                if key != move.key:
                    continue
                if n > 1:
                    self._remote_unsettled[move.src] = (key, n - 1)
                else:
                    del self._remote_unsettled[move.src]

    def _may_commit(self, ref: PageRef, key: Key) -> bool:
        """Caller holds the lock: a write of ``ref`` there may still commit."""
        entry = self._remote_unsettled.get(ref)
        return entry is not None and entry[0] == key

    def _note_remote_copy(self, move: _RemoteMove, outcome: ItemOutcome) -> None:
        if outcome.result == ItemResult.ADDED:
            location = move.dest
        elif outcome.result == ItemResult.DUPLICATE and outcome.location is not None:
            location = outcome.location
        else:
            return
        if self._is_ready(move.src, move.key):
            with self._lock:
                self._remote_copies[move.src] = location

    def _finalize_remote(self, tier: RemoteTier, moves: list[_RemoteMove]) -> None:
        """Remove the sources onto their copies on memory nodes (no
        finalizer: MaruServer checks the exact copy under its lock)."""
        live = [m for m in moves if self._is_ready(m.src, m.key)]
        if not live:
            return
        self.local_pins.block([m.src for m in live])
        try:
            response = self.metadata.finalize_move(
                [
                    MoveItem(key=m.key, source=m.source, dest=m.dest, finalizer_id="")
                    for m in live
                ]
            )
        except ServerStopped:
            self.placement.release([m.src for m in live])
            raise
        except EpochChanged as e:
            self._move_unknown(live, f"MaruServer no longer knows the session ({e})")
            return
        except Exception as e:
            if not isinstance(e, ReplyError):
                logger.exception("FINALIZE_MOVE to memory nodes failed")
            self._move_unknown(live, f"FINALIZE_MOVE: result unknown ({e})")
            return
        if response.status != Status.OK:
            # Not run, and no finalizer is left: the sources stay
            logger.error(
                "FINALIZE_MOVE to memory nodes refused as a whole: %s %s",
                response.status,
                response.error,
            )
            self._retire_failed([m.src for m in live])
            return
        for move, outcome in zip(live, response.items, strict=True):
            try:
                self._apply_remote_move(tier, move, outcome)
            except Exception:
                logger.exception(
                    "applying the FINALIZE_MOVE result of %s failed", move.src
                )
                self._hold([move.src])

    def _apply_remote_move(
        self, tier: RemoteTier, move: _RemoteMove, outcome: ItemOutcome
    ) -> None:
        result, cause = outcome.result, outcome.cause
        if result == ItemResult.REJECTED:
            # The copy is gone (or not of this key): the source stays, and a
            # later demotion finds the key's copy again
            with self._lock:
                self.move_results["REJECTED"] += 1
                self._remote_copies.pop(move.src, None)
            if self._is_ready(move.src, move.key):
                self._retire_failed([move.src])
            return
        if result not in (ItemResult.RETIRED, ItemResult.KEPT):
            self._move_unknown([move], f"FINALIZE_MOVE of {move.src} answered {result}")
            return
        if (
            result == ItemResult.KEPT
            and cause == KeptCause.INVALID
            and outcome.location is not None
            and outcome.location.pool_id == move.dest.pool_id
        ):
            # The memory node's pool is no longer valid: the source stays
            with self._lock:
                self.move_results["KEPT:INVALID"] += 1
                self._remote_copies.pop(move.src, None)
            self._retire_failed([move.src])
            return
        if not self._is_ready(move.src, move.key):
            return  # the source went meanwhile
        foreign = (
            result == ItemResult.KEPT
            and cause == KeptCause.MOVED
            and outcome.key != move.key
        )
        if foreign:
            # The source held another key: its copy there is not this key's
            tier.drop(move.key, move.dest)
        elif result == ItemResult.KEPT:
            with self._lock:
                self._remote_copies[move.src] = move.dest
        self._apply_retire(move.src, outcome, self.move_results)

    def _copy_of(self, ref: PageRef, key: Key) -> tuple[PageRef, Location] | None:
        """The source's READY copy below and its location, if it has one."""
        with self._lock:
            dest = self._copies.get(ref)
            invalid = dest is not None and dest.pool_id in self._invalid_pools
        if dest is None:
            return None
        page = self.pools.page(dest)
        if (
            not invalid
            and page is not None
            and page.state == PageState.READY
            and page.key == key
            and page.location is not None
        ):
            return dest, page.location
        with self._lock:
            self._copies.pop(ref, None)
        return None

    def _is_ready(self, ref: PageRef, key: Key) -> bool:
        page = self.pools.page(ref)
        return page is not None and page.state == PageState.READY and page.key == key

    def _reserve_below(self, tier: str, count: int) -> list[PageRef]:
        """Destination pages, each from the pool of ``tier`` with the most
        room: one region filled past its high watermark would drop pages
        while the others have room."""
        assert self.transfers is not None
        now = time.monotonic()
        with self._lock:
            invalid = set(self._invalid_pools)
            paused = {p for p, until in self._io_paused.items() if until > now}
        room: dict[str, int] = {}
        for pool_id in self.pools.pool_ids():
            info = self.pools.pool_info(pool_id)
            if (
                info is not None
                and self.pools.tier_of(pool_id) == tier
                and pool_id not in invalid
                and pool_id not in paused
                and self.transfers.has_pool(pool_id)
                and not self.pools.pool_quarantined(pool_id)
            ):
                free = self.pools.bytes_in(PageState.FREE, pool_id)
                room[pool_id] = free // info.page_size
        take = dict.fromkeys(room, 0)
        for _ in range(count):
            best = max(room, key=lambda p: room[p] - take[p], default=None)
            if best is None or room[best] - take[best] <= 0:
                break
            take[best] += 1
        out: list[PageRef] = []
        for pool_id, n in take.items():
            if n:
                out += self.pools.reserve_transfer(pool_id, n)
        return out

    def _run_moves(self, moves: list[_Move], io: bool = False) -> None:
        """Copy, COMMIT and FINALIZE_MOVE ``moves``. With ``io`` (the pages
        below are in a file pool) the writes only start here: each ends on
        the I/O executor, and the next pass carries it on (_writes_ended)."""
        assert self.transfers is not None
        fresh = [m for m in moves if not m.existing]
        existing = [m for m in moves if m.existing]
        if self._halted():  # no copy started: all go back
            for move in fresh:
                self.pools.transfer_failed(move.dest)
            self._release(moves, backoff=False)
            return
        if io:
            self._write_io(fresh)
            self._moved(existing)
            return
        copies = [Copy(m.source, m.dest_location, m.source.length) for m in fresh]
        results = self.transfers.run(copies) if copies else []
        self._moved(existing + self._copied(fresh, results))

    def _copied(self, moves: list[_Move], results: list[bool]) -> list[_Move]:
        """Settle ended copies: those that moved every byte, PENDING."""
        out = []
        for move, ok in zip(moves, results, strict=True):
            try:
                if ok:
                    self.pools.transfer_done(move.dest, move.key, move.source.length)
                    out.append(move)
                    continue
                self.pools.transfer_failed(move.dest)
                self._count(move, "COPY_FAILED")
            except Exception:
                logger.exception("ending the copy to %s failed", move.dest)
            self._release([move])
        return out

    def _moved(self, copied: list[_Move]) -> None:
        if self._halted():
            # The run is over: the copies go with it
            self._release(copied)
        elif copied:
            finals = self._commit_moves(copied)
            if finals:
                self._finalize_moves(finals)
        self._reclaim_retiring()

    def _write_io(self, moves: list[_Move]) -> None:
        """Start the writes to the file pool pages. Both pages of a write
        keep an I/O reference until it ends; the destination stays TRANSFER
        until the eviction thread takes the result."""
        assert self.transfers is not None
        for move in moves:
            if not self.pools.io_acquire(move.src, move.dest):
                self.pools.transfer_failed(move.dest)  # the source went
                self._release([move])
                continue
            with self._lock:
                self._writing += 1
                if move.through is not None:
                    self._through_inflight[move.through] += move.source.length
                    self._through_active.add(move.src)
            try:
                self.transfers.submit_io(
                    Copy(move.source, move.dest_location, move.source.length),
                    move.src,
                    move.dest,
                    callback=lambda ticket, m=move: self._write_ended(m, ticket),
                )
            except Exception as e:  # a full queue, or the engine stopped
                with self._lock:
                    self._writing -= 1
                    self._through_ended(move)
                    self._through_settled([move])
                self.pools.io_release([move.src, move.dest])
                logger.debug("cannot write %s below: %s", move.src, e)
                self._io_result(move.dest.pool_id, e)
                self._copied([move], [False])

    def _write_ended(self, move: _Move, ticket: IOResult) -> None:
        """On the I/O thread: no RPC here."""
        self.pools.io_release([move.src, move.dest])
        with self._lock:
            self._writing -= 1
            self._through_ended(move)
            self._written.append((move, ticket))
        self._evict_wake.set()

    def _writes_ended(self) -> None:
        """COMMIT the writes that moved every byte, free the others' pages.
        One that never ran or ended late counts as a failure too."""
        with self._lock:
            done = list(self._written)
            self._written.clear()
            self._through_settled(m for m, _ in done)
        if not done:
            return
        for move, ticket in done:
            error = ticket.error
            if error is None and ticket.cancelled:
                error = RuntimeError("the write never started")
            elif error is None and ticket.late:
                error = TimeoutError("the write ended past its deadline")
            self._io_result(move.dest.pool_id, error)
        moves = [m for m, _ in done]
        self._moved(self._copied(moves, [t.ok for _, t in done]))

    def _io_result(self, pool_id: str, error: Exception | None) -> None:
        """Count a write to ``pool_id``; IO_FAILURES_MAX failures in a row
        pause the pool for IO_PAUSE_S."""
        with self._lock:
            if error is None:
                self._io_failures.pop(pool_id, None)
                return
            self._io_failures[pool_id] += 1
            failures = self._io_failures[pool_id]
            if failures >= IO_FAILURES_MAX:
                del self._io_failures[pool_id]
                self._io_paused[pool_id] = time.monotonic() + IO_PAUSE_S
        if failures >= IO_FAILURES_MAX:
            logger.error(
                "writes to %s failed %d times in a row (%s): no demotion goes "
                "there for %.0f s; the tier above drops past its high watermark",
                pool_id,
                failures,
                error,
                IO_PAUSE_S,
            )
        else:
            logger.warning("writing a page to %s failed: %s", pool_id, error)

    def _commit_moves(self, moves: list[_Move]) -> list[_Move]:
        """COMMIT the copies with a finalizer; returns those to FINALIZE_MOVE.
        A result left unknown ends the run."""
        live = []
        for move in moves:
            if move.existing and not self._is_ready(move.dest, move.key):
                self._release([move])  # the copy went meanwhile
            elif self._is_ready(move.src, move.key):
                live.append(move)
            elif not move.existing:
                self.pools.commit_freed(move.dest)  # the source went meanwhile
        if not live:
            return []
        try:
            response = self.metadata.commit(
                [
                    CommitItem(
                        key=m.key,
                        location=m.dest_location,
                        logical_bytes=m.source.length,
                        finalizer=m.through is None,
                    )
                    for m in live
                ]
            )
        except (EpochChanged, ServerStopped) as e:
            # The run is over: the copies go with it
            self._release(live)
            if isinstance(e, ServerStopped):
                raise
            self.end_run(f"MaruServer no longer knows the session ({e})")
            return []
        except Exception as e:
            # Sent, maybe applied with a finalizer whose id is unknown
            if not isinstance(e, ReplyError):
                logger.exception("COMMIT of %d demoted pages failed", len(live))
            self._release(live)
            self.end_run(f"COMMIT of {len(live)} demoted pages: result unknown ({e})")
            return []
        if response.status == Status.OK:
            outcomes = response.items
        else:
            logger.error(
                "COMMIT of %d demoted pages refused as a whole: %s %s",
                len(live),
                response.status,
                response.error,
            )
            outcomes = [ItemOutcome(ItemResult.REJECTED)] * len(live)
        finals: list[_Move] = []
        for move, outcome in zip(live, outcomes, strict=True):
            try:
                if self._apply_move_commit(move, outcome):
                    finals.append(move)
            except Exception:
                logger.exception("applying the COMMIT result of %s failed", move.dest)
                self._release([move])
        return finals

    def _apply_move_commit(self, move: _Move, outcome: ItemOutcome) -> bool:
        """True if the copy has its finalizer for FINALIZE_MOVE. An unknown
        result ends the run."""
        result = outcome.result
        self._count(move, f"COMMIT:{result.value}")
        if result == ItemResult.ADDED:
            if not move.existing:
                self.pools.commit_added(move.dest)
            if outcome.finalizer_id:
                move.finalizer_id = outcome.finalizer_id
                return True
            self._keep_copy(move)  # no finalizer to move under
        elif result in (ItemResult.DUPLICATE, ItemResult.REJECTED):
            if move.existing:
                with self._lock:
                    self._copies.pop(move.src, None)
            else:
                self.pools.commit_freed(move.dest)
            if result == ItemResult.DUPLICATE and outcome.location is not None:
                # The key is in that pool already: the next demotion
                # finalizes onto that copy
                self._note_copy(move.src, move.key, outcome.location)
        else:
            self.end_run(f"COMMIT of demoted page {move.dest} answered {result}")
        self._release([move])
        return False

    def _note_copy(self, src: PageRef, key: Key, location: Location) -> None:
        ref = self.pools.find(location)
        if ref is not None and self._is_ready(ref, key):
            with self._lock:
                self._copies[src] = ref

    def _finalize_moves(self, moves: list[_Move]) -> None:
        """Remove the sources now that the copies have their finalizers."""
        self.local_pins.block([m.src for m in moves])
        try:
            response = self.metadata.finalize_move(
                [
                    MoveItem(
                        key=m.key,
                        source=m.source,
                        dest=m.dest_location,
                        finalizer_id=m.finalizer_id,
                    )
                    for m in moves
                ]
            )
        except ServerStopped:
            self.placement.release([m.src for m in moves])
            raise
        except EpochChanged as e:
            self._move_unknown(moves, f"MaruServer no longer knows the session ({e})")
            return
        except Exception as e:
            if not isinstance(e, ReplyError):
                logger.exception("FINALIZE_MOVE failed")
            self._move_unknown(moves, f"FINALIZE_MOVE: result unknown ({e})")
            return
        if response.status != Status.OK:
            # Not run, but the copies' finalizers stay until the session ends
            self._move_unknown(
                moves,
                f"FINALIZE_MOVE refused as a whole: {response.status} {response.error}",
            )
            return
        # Copies not kept as locations of their key: retired, freed once RETIRED
        retire: list[_Move] = []
        for move, outcome in zip(moves, response.items, strict=True):
            try:
                if self._apply_move(move, outcome):
                    retire.append(move)
            except Exception:
                logger.exception(
                    "applying the FINALIZE_MOVE result of %s failed", move.src
                )
                self._hold([move.src])
        if retire:
            for move in retire:
                self.placement.on_ready(move.dest)
            self.retire(
                [m.dest for m in retire],
                [KeyLocation(key=m.key, location=m.dest_location) for m in retire],
            )

    def _apply_move(self, move: _Move, outcome: ItemOutcome) -> bool:
        """Act on one FINALIZE_MOVE result; whether to retire the copy."""
        result, cause = outcome.result, outcome.cause
        if result == ItemResult.REJECTED:
            # The copy had no finalizer of this session: the source stays
            with self._lock:
                self.move_results["REJECTED"] += 1
            if self._is_ready(move.src, move.key):
                self._retire_failed([move.src])
            return True
        if result not in (ItemResult.RETIRED, ItemResult.KEPT):
            self._move_unknown([move], f"FINALIZE_MOVE of {move.src} answered {result}")
            return False
        if (
            result == ItemResult.KEPT
            and cause == KeptCause.INVALID
            and outcome.location is not None
            and outcome.location.pool_id == move.dest_location.pool_id
        ):
            # The pool below is the invalid one: the source stays, the copy
            # is not used
            with self._lock:
                self.move_results["KEPT:INVALID"] += 1
            self._retire_failed([move.src])
            self._pool_invalid(move.dest)
            return False
        if not self._is_ready(move.src, move.key):
            self.placement.on_ready(move.dest)  # the source went meanwhile
            return False
        # The page holds another key per the Catalog: its bytes were not
        # this key's
        foreign = (
            result == ItemResult.KEPT
            and cause == KeptCause.MOVED
            and outcome.key != move.key
        )
        if not foreign:
            self.placement.on_ready(move.dest)  # a location of the key either way
            self._note_ready(move.dest)
            if result == ItemResult.KEPT:
                with self._lock:
                    self._copies[move.src] = move.dest
        # The source as after a RETIRE with this result
        self._apply_retire(move.src, outcome, self.move_results)
        return foreign

    def _move_unknown(
        self, moves: list[_Move] | list[_RemoteMove], reason: str
    ) -> None:
        """No usable FINALIZE_MOVE result: both pages stay as they are, the
        sources' local pins stay blocked, and the run ends."""
        with self._lock:
            self.move_results["UNKNOWN"] += len(moves)
        self.placement.release([m.src for m in moves])
        self.end_run(reason)

    def _keep_copy(self, move: _Move) -> None:
        self.placement.on_ready(move.dest)
        self._note_ready(move.dest)
        with self._lock:
            self._copies[move.src] = move.dest

    # --------------------------------------------------------------- removal

    def _retire_queued(self) -> None:
        """RETIRE pages of block requests."""
        now = time.monotonic()
        with self._lock:
            due = [r for r, at in self._retire_queue.items() if at <= now]
            for ref in due:
                self._retire_queue[ref] = float("inf")  # in flight
        for i in range(0, len(due), MAX_BATCH):
            refs, items = self._ready_items(due[i : i + MAX_BATCH])
            if refs:
                self.retire(refs, items)

    def retire(
        self, refs: list[PageRef], items: list[KeyLocation]
    ) -> list[ItemOutcome] | None:
        """Block local pins, RETIRE, act on each result; returns the results,
        None if there are none. A result left unknown holds the pages (never
        reused) and ends the run."""
        with self._lock:
            paused = self._paused()
        if paused or self.metadata.epoch_lost:
            self._retire_failed(refs)  # not sent
            return None
        self.local_pins.block(refs)
        try:
            response = self.metadata.retire(items)
        except ServerStopped:
            self._hold(refs)
            raise
        except EpochChanged as e:
            self._hold(refs)
            self.end_run(f"MaruServer no longer knows the session ({e})")
            return None
        except Exception as e:
            if not isinstance(e, ReplyError):
                logger.exception("RETIRE failed")
            self._hold(refs)
            self.end_run(f"RETIRE of {len(refs)} pages: result unknown ({e})")
            return None
        if response.status != Status.OK:
            logger.error(
                "RETIRE refused as a whole: %s %s", response.status, response.error
            )
            self._retire_failed(refs)
            return None
        for ref, outcome in zip(refs, response.items, strict=True):
            try:
                self._apply_retire(ref, outcome)
            except Exception:
                logger.exception("applying the RETIRE result of %s failed", ref)
                self._hold([ref])
        self._reclaim_retiring()
        return list(response.items)

    def _apply_retire(
        self,
        ref: PageRef,
        outcome: ItemOutcome,
        results: collections.Counter[str] | None = None,
    ) -> None:
        with self._lock:
            central = ref in self._central
            cause = f":{outcome.cause.value}" if outcome.cause else ""
            counts = self.retire_results if results is None else results
            counts[f"{outcome.result.value}{cause}"] += 1
        if outcome.result == ItemResult.RETIRED:
            self._retired(ref)
        elif outcome.result == ItemResult.KEPT and outcome.cause == KeptCause.PINNED:
            if central:
                self._retry_queued([ref])  # stays blocked
            else:
                self.local_pins.after_kept(ref, KeptCause.PINNED)
                self.placement.kept(ref, KeptCause.PINNED)
        elif outcome.result == ItemResult.KEPT and outcome.cause == KeptCause.MOVED:
            self._reconcile_moved(ref, outcome.key, outcome.location)
        elif outcome.result == ItemResult.KEPT and outcome.cause == KeptCause.INVALID:
            self._pool_invalid(ref)
        else:
            self._hold([ref])
            self.end_run(f"RETIRE of {ref} answered {outcome.result} {outcome.cause}")

    def _retired(self, ref: PageRef) -> None:
        self.pools.retired(ref)
        self.placement.retired(ref)
        self._forget_queued(ref)
        with self._lock:
            self._retiring.add(ref)

    def _reconcile_moved(self, ref: PageRef, key, occupant) -> None:
        """KEPT(MOVED): another published location occupies the page. Never
        free it; the ledger takes the occupant as its READY entry."""
        self._forget_queued(ref)
        if key is None or occupant is None:
            logger.error("RETIRE of %s: MOVED without an occupant; page held", ref)
            self.placement.hold(ref)
            return
        try:
            new = self.pools.reconcile(ref, key, occupant)
        except InvalidTransitionError as e:
            logger.error(
                "RETIRE of %s: MOVED to %s does not fit (%s); page held",
                ref,
                occupant,
                e,
            )
            self.placement.hold(ref)
            return
        logger.warning("page %s holds %s %s per the Catalog", ref, key, occupant)
        self.placement.on_gone(ref)
        self.local_pins.forget(ref)
        self.placement.on_ready(new)

    def is_invalid(self, pool_id: str) -> bool:
        """MaruServer said the pool is no longer valid for this node."""
        with self._lock:
            return pool_id in self._invalid_pools

    def _pool_invalid(self, ref: PageRef) -> None:
        with self._lock:
            self._invalid_pools.add(ref.pool_id)
        if self.pools.disable_pool(ref.pool_id):
            logger.error(
                "MaruServer says pool %s is no longer valid for this node; it is "
                "not used any more",
                ref.pool_id,
            )
        self._hold([ref])

    def _retire_failed(self, refs: list[PageRef]) -> None:
        """No usable result: queued pages are tried again, others reopened."""
        with self._lock:
            queued = [r for r in refs if r in self._central]
        self._retry_queued(queued)
        others = [r for r in refs if r not in queued]
        self.local_pins.unblock(others)
        self.placement.release(others)

    def _hold(self, refs: list[PageRef]) -> None:
        for ref in refs:
            self._forget_queued(ref)
            self.placement.hold(ref)

    def _retry_queued(self, refs: list[PageRef]) -> None:
        at = time.monotonic() + self.placement.kept_backoff_s
        with self._lock:
            for ref in refs:
                self._retire_queue[ref] = at

    def _forget_queued(self, ref: PageRef) -> None:
        with self._lock:
            self._retire_queue.pop(ref, None)
            self._central.discard(ref)
            self._copies.pop(ref, None)
            self._remote_copies.pop(ref, None)
            self._remote_unsettled.pop(ref, None)

    def _reclaim_retiring(self) -> None:
        with self._lock:
            refs = list(self._retiring)
        for ref in refs:
            # Not reused while a reader or an I/O may still touch it
            if self.local_pins.readers(ref) or not self.pools.reclaim(ref):
                continue
            self.local_pins.forget(ref)
            with self._lock:
                self._retiring.discard(ref)

    # ------------------------------------------------------------- heartbeat

    def on_heartbeat(self, response: NodeHeartbeatResponse) -> None:
        if response.access:
            self.placement.on_access(response.access, self._resolve_access)
        for block in response.blocks:
            self._block(block)
        sent = {b.block_id for b in response.blocks}
        with self._lock:
            for block_id in list(self._blocks):
                if block_id not in sent and not self._blocks[block_id] & self._central:
                    del self._blocks[block_id]

    def _resolve_access(self, info: AccessInfo) -> PageRef | None:
        ref = self.pools.find_allocation(
            info.pool_id, info.allocation_id, info.generation
        )
        if ref is None:
            return None
        page = self.pools.page(ref)
        return ref if page is not None and page.state == PageState.READY else None

    def _block(self, block: BlockRequest) -> None:
        """A central delete: block new local pins, then RETIRE the items. It is
        sent again until done, so pages already taken are skipped."""
        refs: list[PageRef] = []
        for item in block.items:
            ref = self.pools.find(item.location)
            page = self.pools.page(ref) if ref is not None else None
            if (
                ref is not None
                and page is not None
                and page.state == PageState.READY
                and page.key == item.key
            ):
                refs.append(ref)
        with self._lock:
            done = self._blocks.setdefault(block.block_id, set())
            refs = [r for r in refs if r not in done]
            done.update(refs)
            for ref in refs:
                self._central.add(ref)
                self._retire_queue[ref] = 0.0
        if not refs:
            return
        self.local_pins.block(refs)
        for ref in refs:
            self.placement.hold(ref)  # never an eviction candidate
        logger.info(
            "block %s (%s): %d pages blocked", block.block_id, block.reason, len(refs)
        )
        self._evict_wake.set()

    # ----------------------------------------------------------- worker exit

    def _worker_ended(self, incarnation: str) -> None:
        if self.fetches is not None:
            self.fetches.worker_ended(incarnation)

    def _queue_exit(self, ref: WorkerRef) -> None:
        if self.fetches is not None:
            self.fetches.forget(ref.incarnation)
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

    # ----------------------------------------------------------------- state

    def pending_exits(self) -> int:
        with self._lock:
            return len(self._exits)

    def pending_commits(self) -> int:
        with self._lock:
            return len(self._tickets)
