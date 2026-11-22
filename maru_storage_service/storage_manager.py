# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageManager: credit, COMMIT, RETIRE, central deletes and WORKER_EXIT.

Worker records live in a WorkerRegistry, op results in an OpLedger; MaruServer
is reached through a MetadataPort. A request whose result stays unknown, and a
node session MaruServer no longer knows, end the run (``on_end``): the
StorageService starts a new one.
"""

import collections
import logging
import threading
import time
from collections.abc import Callable
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
from maru_common.protocol import CommitItem, NodeHeartbeatResponse
from maru_common.serializer import DecodedRequest, op_digest
from maru_common.storage_types import (
    MAX_BATCH,
    BlockRequest,
    ExitCause,
    ItemOutcome,
    ItemResult,
    KeptCause,
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
from .tiers import Level
from .worker_registry import WorkerRecord, WorkerRegistry

logger = logging.getLogger(__name__)

R = TypeVar("R")

DEFAULT_GPU_DRAIN_GRACE_S = 5.0
DEFAULT_EVICT_INTERVAL_S = 0.05
# Wait for more WRITE_DONE items to join a COMMIT batch
COMMIT_LINGER_S = 0.002
EXIT_RETRY_S = 0.5
EXIT_RETRY_MAX_S = 30.0


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
        on_end: Callable[[str], None] = lambda reason: None,
    ):
        """``on_end`` ends a run whose metadata result stays unknown."""
        self.node_epoch = node_epoch
        # By tier name, in config order
        self.traits = traits or {
            "dram": TierTraits(
                "dram", Level.L1, PoolKind.DRAM, credit=True, evicts=False
            )
        }
        self.credit_tiers = [n for n, t in self.traits.items() if t.credit]
        self.pools = pool_manager
        self.placement = placement
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
        # Pools MaruServer called INVALID: no credit or eviction
        self._invalid_pools: set[str] = set()

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
            return {
                "ended": self._ended,
                "pending_commits": len(self._tickets),
                "retire_results": dict(self.retire_results),
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
                if run:
                    self._retire_queued()
                    self._evict_once()
            except ServerStopped:
                return
            except Exception:
                logger.exception("eviction pass failed")

    def _evict_once(self) -> None:
        for name, traits in self.traits.items():
            if self._halted():
                return
            if not traits.evicts:
                continue
            pools = self._evictable_pools(name)
            if not pools:
                continue
            capacity, free, retiring, page_size = self._tier_usage(pools)
            candidates = self.placement.candidates(
                name,
                pools,
                capacity=capacity,
                free_bytes=free,
                retiring_bytes=retiring,
                page_size=page_size,
                limit=MAX_BATCH,
            )
            refs, items = self._ready_items(candidates)
            if refs:
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
                or (traits.credit and not self.pools.creditable(pool_id))
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
        for block in response.blocks:
            self._block(block)
        sent = {b.block_id for b in response.blocks}
        with self._lock:
            for block_id in list(self._blocks):
                if block_id not in sent and not self._blocks[block_id] & self._central:
                    del self._blocks[block_id]
        if response.access:  # last: malformed hints raise ValueError
            self.placement.on_access(self.pools.find_ready(response.access.reads()))

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

    # ----------------------------------------------------------------- state

    def pending_exits(self) -> int:
        with self._lock:
            return len(self._exits)

    def pending_commits(self) -> int:
        with self._lock:
            return len(self._tickets)
