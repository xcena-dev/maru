# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Page publication, retirement, central deletion and eviction."""

import collections
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from maru_common.protocol import CommitItem, NodeHeartbeatResponse
from maru_common.storage_types import (
    MAX_BATCH,
    BlockRequest,
    ItemOutcome,
    ItemResult,
    KeptCause,
    KeyLocation,
    PoolKind,
    Status,
)

from .config import TierTraits
from .placement import PlacementPolicy
from .pool_manager import InvalidTransitionError, PageRef, PageState, PoolManager
from .ports import EpochChanged, LocalPinPort, MetadataPort, ReplyError, ServerStopped
from .tiers import Level

logger = logging.getLogger("maru_storage_service.storage_manager")

DEFAULT_EVICT_INTERVAL_S = 0.05
# Wait for more WRITE_DONE items to join a COMMIT batch
COMMIT_LINGER_S = 0.002


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


class NodeStore:
    def __init__(
        self,
        *,
        node_epoch: str,
        pool_manager: PoolManager,
        placement: PlacementPolicy,
        local_pins: LocalPinPort,
        metadata: MetadataPort,
        evict_interval_s: float = DEFAULT_EVICT_INTERVAL_S,
        traits: dict[str, TierTraits] | None = None,
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
        self._lock = threading.RLock()
        self._cv = threading.Condition(self._lock)
        self._tickets: collections.deque[_Ticket] = collections.deque()
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
        # RETIRE results by outcome ("RETIRED", "KEPT:PINNED", ...)
        self.retire_results: collections.Counter[str] = collections.Counter()
        # Pools MaruServer called INVALID: no credit or eviction
        self._invalid_pools: set[str] = set()

    def start(self) -> None:
        """After the session opened."""
        for target, name in (
            (self._commit_loop, "maru-commit"),
            (self._evict_loop, "maru-evict"),
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
        self._evict_wake.set()
        self.metadata.stop()
        for thread in self._threads:
            thread.join(timeout=10.0)
        self._threads.clear()

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

    def stats(self) -> dict:
        with self._lock:
            return {
                "ended": self._ended,
                "pending_commits": len(self._tickets),
                "retire_results": dict(self.retire_results),
                "credit_short": dict(self.pools.credit_short),
                "credit_short_bytes": dict(self.pools.credit_short_bytes),
            }

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
                demand_bytes=(
                    self.pools.budgets.transfer_reserve_bytes.get(name, 0) + page_size
                    if traits.credit
                    else 0
                ),
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

    def pending_commits(self) -> int:
        with self._lock:
            return len(self._tickets)
