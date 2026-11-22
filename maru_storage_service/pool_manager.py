# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""PoolManager: the node's pools, credit budgets, generations and quarantine.

The pages of each pool are in its PageTable. A page gets a new generation each
time it leaves FREE. Worker requests that do not match the ledger are refused;
a wrong transition asked by the service is a bug (InvalidTransitionError).
"""

import collections
import heapq
import logging
import math
import threading
import time
from collections.abc import Iterable, Sequence
from dataclasses import replace

from maru_common.node_protocol import Budgets
from maru_common.storage_types import (
    GENERATION_COUNTER_MAX,
    POOL_KINDS,
    Key,
    Location,
    PoolInfo,
    PoolUsage,
    generation_epoch_seq,
    make_generation,
)

from .page_table import (
    RECOVERY_TAG,
    InvalidTransitionError,
    Page,
    PageRef,
    PageState,
    PageTable,
    Transitions,
)

__all__ = [
    "RECOVERY_TAG",
    "InvalidTransitionError",
    "Page",
    "PageRef",
    "PageState",
    "PoolManager",
    "holder_tag",
]

logger = logging.getLogger(__name__)

# A holder short of credit by the node-total budget keeps its claim this long
# (it asks again meanwhile); a holder asked to return credit is asked again
# only after CREDIT_ASK_S
CREDIT_WAIT_S = 1.0
CREDIT_ASK_S = 0.5


def holder_tag(holder: str) -> str:
    """Quarantine tag of a worker's pages; never equal to RECOVERY_TAG."""
    return f"worker:{holder}"


class PoolManager:
    """The pools of this node and their PageTables. Thread safe."""

    def __init__(self, budgets: Budgets, page_size: int):
        self.budgets = budgets
        self.page_size = page_size
        self._lock = threading.RLock()
        self._tables: dict[str, PageTable] = {}
        # The tier (store name) of each pool
        self._tier: dict[str, str] = {}
        # Pools that give credit; a quarantined or a disabled one does not
        self._creditable: set[str] = set()
        # Pools an earlier run may still write (see quarantine_pool)
        self._quarantined: set[str] = set()
        self._epoch_seq: int | None = None
        self._counter = 0
        self._credit_stopped: str | None = None
        # Credited bytes per holder (worker incarnation)
        self._held: collections.Counter[str] = collections.Counter()
        # Holders below their fair share, short by the node-total budget:
        # bytes still owed and when they last asked
        self._waiting: dict[str, tuple[int, float]] = {}
        # When each holder was last asked to return credit
        self._asked: dict[str, float] = {}
        # I/O references prevent page reuse.
        self._io: collections.Counter[tuple[str, int]] = collections.Counter()
        # How many times each transition ran, for tests and metrics
        self.transitions: Transitions = collections.Counter()
        # CREDITs answered short, and the bytes they lacked, by the limit
        # that bound them: space, worker_cap, node_cap, pending, stopped
        self.credit_short: collections.Counter[str] = collections.Counter()
        self.credit_short_bytes: collections.Counter[str] = collections.Counter()

    # ------------------------------------------------------------------ pools

    def add_pool(
        self,
        info: PoolInfo,
        *,
        creditable: bool = True,
        state: PageState = PageState.FREE,
        tier: str,
    ) -> None:
        """Add a pool under its store name, FREE or QUARANTINED."""
        if state not in (PageState.FREE, PageState.QUARANTINED):
            raise ValueError(f"a pool starts FREE or QUARANTINED, not {state}")
        if creditable and not POOL_KINDS[info.kind].mappable:
            raise ValueError(f"pool {info.pool_id}: an unmapped pool gives no credit")
        if info.page_size != self.page_size:
            raise ValueError(
                f"pool {info.pool_id}: page_size {info.page_size} is not the "
                f"node's {self.page_size}"
            )
        with self._lock:
            if info.pool_id in self._tables:
                raise ValueError(f"pool {info.pool_id} added twice")
            self._tables[info.pool_id] = PageTable(info, state, self.transitions)
            self._tier[info.pool_id] = tier
            if creditable:
                self._creditable.add(info.pool_id)

    def pool_info(self, pool_id: str) -> PoolInfo | None:
        with self._lock:
            table = self._tables.get(pool_id)
            return table.info if table else None

    def tier_of(self, pool_id: str) -> str | None:
        """The pool's tier; None for a pool this node does not have."""
        with self._lock:
            return self._tier.get(pool_id)

    def creditable(self, pool_id: str) -> bool:
        with self._lock:
            return pool_id in self._creditable

    def disable_pool(self, pool_id: str) -> bool:
        """No more credit from a pool MaruServer no longer accepts (KEPT INVALID);
        whether it was enabled."""
        with self._lock:
            if pool_id not in self._creditable:
                return False
            self._creditable.discard(pool_id)
            return True

    def enable_pool(self, pool_id: str) -> None:
        """Enable credit from a mapped pool."""
        with self._lock:
            table = self._tables.get(pool_id)
            if table is not None and POOL_KINDS[table.info.kind].mappable:
                self._creditable.add(pool_id)

    def quarantine_pool(self, pool_id: str) -> None:
        """An earlier run may still write the pool: no credit from it, and a
        page of it that would become FREE is QUARANTINED instead, until
        end_pool_quarantine. Its READY pages are read and retired as usual."""
        with self._lock:
            if pool_id not in self._tables:
                raise InvalidTransitionError(f"no pool {pool_id}")
            self._quarantined.add(pool_id)
            self._creditable.discard(pool_id)

    def pool_quarantined(self, pool_id: str) -> bool:
        with self._lock:
            return pool_id in self._quarantined

    def end_pool_quarantine(self, pool_id: str, creditable: bool) -> int:
        """No earlier run writes the pool any more: its quarantined pages are
        FREE (returns how many) and it gives credit if ``creditable``."""
        with self._lock:
            self._quarantined.discard(pool_id)
            freed = self.release_quarantine(RECOVERY_TAG, pool_id)
            if creditable:
                self.enable_pool(pool_id)
            return freed

    def pool_ids(self) -> list[str]:
        with self._lock:
            return list(self._tables)

    # ------------------------------------------------------------------ epoch

    def set_epoch_seq(self, epoch_seq: int) -> None:
        """The epoch_seq MaruServer gave in NODE_OPEN; credit starts with it."""
        make_generation(epoch_seq, 0)  # range check
        with self._lock:
            if self._epoch_seq is not None and self._epoch_seq != epoch_seq:
                raise InvalidTransitionError(
                    f"epoch_seq already {self._epoch_seq}; changing it is not supported"
                )
            self._epoch_seq = epoch_seq

    @property
    def epoch_seq(self) -> int | None:
        return self._epoch_seq

    def stop_credit(self, reason: str) -> None:
        """No new credit from now on (safe state, exhausted counter)."""
        with self._lock:
            if self._credit_stopped is None:
                self._credit_stopped = reason
                logger.warning("New credit stopped: %s", reason)

    @property
    def credit_stopped(self) -> str | None:
        return self._credit_stopped

    def _next_generation(self) -> int:
        """Caller holds the lock and checked the counter is not exhausted."""
        if self._epoch_seq is None:
            raise InvalidTransitionError("no page leaves FREE before the epoch_seq")
        generation = make_generation(self._epoch_seq, self._counter)
        self._counter += 1
        return generation

    # ----------------------------------------------------------------- credit

    def credit(
        self,
        holder: str,
        want_bytes: int,
        tiers: str | Sequence[str] | None = None,
    ) -> list[Location]:
        """FREE pages for ``holder`` within every budget, from ``tiers`` in
        order (all creditable pools when None): the first tier's
        pages come first, and within a tier the roomiest pool gives each page.
        Each tier keeps its transfer reserve. Empty when credit is stopped or
        the workers' commit-pending bytes reached their cap. A short grant
        counts its cause in ``credit_short``."""
        if want_bytes <= 0:
            return []
        ps = self.page_size
        want = math.ceil(want_bytes / ps)
        with self._lock:
            if self._credit_stopped is not None or self._epoch_seq is None:
                self._short("stopped", want)
                return []
            if self._write_pending() >= self.budgets.commit_pending_max_bytes:
                self._short("pending", want)
                return []
            rooms, space, worker_room, node_room = self._rooms(holder, tiers)
            held = self._held[holder]
            uncapped = min(want, worker_room, space)
            granted = max(min(uncapped, node_room), 0)
            self._note_waiting(
                holder, (uncapped - granted) * ps, held + granted * ps, ps
            )
            if granted < want:
                limits = {
                    "space": space,
                    "worker_cap": worker_room,
                    "node_cap": node_room,
                }
                self._short(min(limits, key=limits.__getitem__), want - granted)
            if granted <= 0:
                return []
            left = GENERATION_COUNTER_MAX - self._counter + 1
            if granted > left:
                self._short("stopped", granted - max(left, 0))
                granted = left
                if granted <= 0:
                    self._credit_stopped = (
                        "the generation counter is exhausted; restart the "
                        "StorageService"
                    )
                    logger.error("New credit stopped: %s", self._credit_stopped)
                    return []
            out: list[Location] = []
            for tables, room in rooms:
                # Roomiest pool first, the earlier one on a tie
                heap = [(-t.free_count(), i) for i, t in enumerate(tables)]
                heapq.heapify(heap)
                for _ in range(min(room, granted - len(out))):
                    free, i = heapq.heappop(heap)
                    table = tables[i]
                    page = table.take(PageState.CREDITED, self._next_generation())
                    page.holder = holder
                    self._held[holder] += ps
                    out.append(table.location(page))
                    heapq.heappush(heap, (free + 1, i))
                if len(out) >= granted:
                    break
            return out

    def credit_room(self, holder: str, tiers: str | Sequence[str] | None = None) -> int:
        """The bytes credit() would give ``holder`` now, at most."""
        with self._lock:
            if self._credit_stopped is not None or self._epoch_seq is None:
                return 0
            if self._write_pending() >= self.budgets.commit_pending_max_bytes:
                return 0
            _, space, worker_room, node_room = self._rooms(holder, tiers)
            return max(0, min(space, worker_room, node_room)) * self.page_size

    def _rooms(
        self, holder: str, tiers: str | Sequence[str] | None
    ) -> tuple[list[tuple[list[PageTable], int]], int, int, int]:
        """Caller holds the lock. The credit tiers' rooms, and the pages
        ``holder`` may get by space, by its own cap and by the node cap."""
        ps = self.page_size
        rooms = self._credit_tiers(tiers)
        now = time.monotonic()
        self._waiting = {
            h: w for h, w in self._waiting.items() if now - w[1] < CREDIT_WAIT_S
        }
        # Room owed to other waiting holders is not given away
        owed = sum(w[0] for h, w in self._waiting.items() if h != holder)
        worker_room = (self.budgets.credit_max_bytes - self._held[holder]) // ps
        node_room = (
            self.budgets.credit_total_max_bytes - self._bytes(PageState.CREDITED) - owed
        ) // ps
        return rooms, sum(room for _, room in rooms), worker_room, node_room

    def _credit_tiers(
        self, tiers: str | Sequence[str] | None
    ) -> list[tuple[list[PageTable], int]]:
        """Caller holds the lock. Per tier in credit order: its creditable
        pools and the pages it may give (FREE less its transfer reserve)."""
        if tiers is None:
            order = list(
                dict.fromkeys(
                    self._tier[p] for p in self._tables if p in self._creditable
                )
            )
        elif isinstance(tiers, str):
            order = [tiers]
        else:
            order = list(tiers)
        out = []
        for tier in order:
            tables = [
                t
                for p, t in self._tables.items()
                if p in self._creditable and self._tier[p] == tier
            ]
            if not tables:
                continue
            reserve = self.budgets.transfer_reserve_bytes.get(tier, 0)
            free = sum(t.bytes[PageState.FREE] for t in tables)
            out.append((tables, max(0, (free - reserve) // self.page_size)))
        return out

    def write_pending_bytes(self) -> int:
        """Bytes waiting for a COMMIT result."""
        with self._lock:
            return self._write_pending()

    def _write_pending(self) -> int:
        """Caller holds the lock."""
        return sum(t.bytes[PageState.PENDING] for t in self._tables.values())

    def _short(self, cause: str, pages: int) -> None:
        """Caller holds the lock."""
        self.credit_short[cause] += 1
        self.credit_short_bytes[cause] += pages * self.page_size

    def _fair_share(self, holder: str, ps: int) -> int:
        """The node-total budget split among holders and waiting holders."""
        holders = {h for h, b in self._held.items() if b > 0}
        holders.update(self._waiting, (holder,))
        return max(ps, self.budgets.credit_total_max_bytes // len(holders) // ps * ps)

    def _note_waiting(self, holder: str, short: int, held: int, ps: int) -> None:
        """Caller holds the lock. ``holder``, holding ``held`` bytes, got
        ``short`` bytes less than it asked for (beyond its own limits)."""
        owed = min(short, self._fair_share(holder, ps) - held)
        if owed > 0:
            self._waiting[holder] = (owed, time.monotonic())
        else:
            self._waiting.pop(holder, None)

    def credit_asks(self, holder: str) -> list[tuple[str, int]]:
        """Holders above their fair share to ask for credit back while
        ``holder`` waits, largest first; each is asked once per CREDIT_ASK_S.
        Credit only comes back by their CREDIT_RETURN."""
        with self._lock:
            waiting = self._waiting.get(holder)
            if waiting is None:
                return []
            need = waiting[0]
            ps = self.page_size
            fair = self._fair_share(holder, ps)
            now = time.monotonic()
            asks = []
            for other, held in self._held.most_common():
                if need <= 0 or held <= fair:
                    break
                if other == holder:
                    continue
                ask = min(held - fair, math.ceil(need / ps) * ps)
                need -= ask
                if now - self._asked.get(other, -CREDIT_ASK_S) >= CREDIT_ASK_S:
                    self._asked[other] = now
                    asks.append((other, ask))
            return asks

    def held_bytes(self, holder: str) -> int:
        with self._lock:
            return self._held[holder]

    def forget_holder(self, holder: str) -> bool:
        """Drop a holder's records; False (nothing dropped) while it holds credit."""
        with self._lock:
            if self._held[holder]:
                return False
            del self._held[holder]
            self._waiting.pop(holder, None)
            self._asked.pop(holder, None)
            return True

    # ------------------------------------------------- worker-named transitions

    def check_credit(
        self, holder: str, credit: Location, *, returning: bool = False
    ) -> tuple[Page | None, str]:
        """The page ``credit`` names if ``holder`` holds it, else the reason.
        Credit of a disabled pool can still come back (``returning``), not be
        written."""
        table = self._tables.get(credit.pool_id)
        if table is None or (credit.pool_id not in self._creditable and not returning):
            return None, f"pool {credit.pool_id!r} is not a pool of this node"
        if generation_epoch_seq(credit.generation) != self._epoch_seq:
            return None, "the credit comes from another StorageService run"
        if credit.kind != table.info.kind:
            return None, f"kind {credit.kind} is not the pool's {table.info.kind}"
        page = table.at(credit.offset)
        if page is None:
            return None, f"offset {credit.offset} is not a page of the pool"
        if credit.length != table.page_size:
            return None, f"length {credit.length} is not the page size"
        if page.generation != credit.generation:
            return None, "the page is at another generation"
        if page.allocation_id != credit.allocation_id:
            return None, "the allocation id does not match"
        if page.state != PageState.CREDITED:
            return None, f"the page is {page.state}, not credited"
        if page.holder != holder:
            return None, "the page is credited to another worker"
        return page, ""

    def write_done(
        self, holder: str, credit: Location, key: Key, logical_bytes: int
    ) -> tuple[PageRef | None, Location | None, str]:
        """CREDITED -> PENDING; the location to commit has length=logical_bytes."""
        with self._lock:
            page, reason = self.check_credit(holder, credit)
            if page is None:
                return None, None, reason
            if not 0 < logical_bytes <= credit.length:
                return (
                    None,
                    None,
                    (f"logical_bytes {logical_bytes} is not within 1..{credit.length}"),
                )
            table = self._tables[credit.pool_id]
            table.move(page, PageState.CREDITED, PageState.PENDING)
            self._held[holder] -= table.page_size
            page.holder = None
            page.key = key
            page.location = replace(credit, length=logical_bytes)
            return table.ref(page), page.location, ""

    def abort(self, holder: str, credit: Location) -> tuple[bool, str]:
        """CREDITED -> FREE: the worker drained its GPU work on the page."""
        with self._lock:
            page, reason = self.check_credit(holder, credit, returning=True)
            if page is None:
                return False, reason
            self._free_credited(page, credit.pool_id, holder)
            return True, ""

    def credit_return(self, holder: str, credit: Location) -> tuple[bool, str]:
        """CREDITED -> FREE. CREDIT_RETURN has no op, so a credit already back is
        OK again (no change)."""
        with self._lock:
            page, reason = self.check_credit(holder, credit, returning=True)
            if page is not None:
                self._free_credited(page, credit.pool_id, holder)
                return True, ""
            table = self._tables.get(credit.pool_id)
            if (
                table is not None
                and generation_epoch_seq(credit.generation) == self._epoch_seq
            ):
                current = table.at(credit.offset)
                if current is not None and (
                    (
                        current.generation == credit.generation
                        and current.allocation_id == credit.allocation_id
                        and current.state == PageState.FREE
                    )
                    or current.generation > credit.generation
                ):
                    return True, "already returned"
            return False, reason

    def _free_credited(self, page: Page, pool_id: str, holder: str) -> None:
        table = self._tables[pool_id]
        table.free(page, PageState.CREDITED)
        self._held[holder] -= table.page_size

    # ------------------------------------------------ service-side transitions

    def commit_added(self, ref: PageRef) -> Location:
        """PENDING -> READY: MaruServer added the location."""
        with self._lock:
            table, page = self._page(ref, PageState.PENDING)
            if page.location is None:
                raise InvalidTransitionError(f"PENDING page {ref} has no location")
            table.move(page, PageState.PENDING, PageState.READY)
            return page.location

    def commit_freed(self, ref: PageRef) -> None:
        """PENDING -> FREE: DUPLICATE or REJECTED."""
        with self._lock:
            table, page = self._page(ref, PageState.PENDING)
            self._to_free(table, page, PageState.PENDING)

    def retired(self, ref: PageRef) -> None:
        """READY -> RETIRING: MaruServer removed the location (RETIRED)."""
        with self._lock:
            table, page = self._page(ref, PageState.READY)
            table.move(page, PageState.READY, PageState.RETIRING)

    def reclaim(self, ref: PageRef) -> bool:
        """RETIRING -> FREE once local accesses ended; False while an I/O
        still runs on it."""
        with self._lock:
            table, page = self._page(ref, PageState.RETIRING)
            if self._io.get((table.pool_id, page.index)):
                return False
            self._to_free(table, page, PageState.RETIRING)
            return True

    def reconcile(self, ref: PageRef, key: Key, location: Location) -> PageRef:
        """READY page takes the Catalog's occupant (KEPT MOVED): its key,
        allocation and generation. Returns the page at its new generation."""
        with self._lock:
            table, page = self._page(ref, PageState.READY)
            if (
                location.pool_id != ref.pool_id
                or location.offset != page.index * table.page_size
                or not 0 < location.length <= table.page_size
                or location.kind != table.info.kind
            ):
                raise InvalidTransitionError(f"{location} is not page {page.index}")
            other = table.index_of(location.allocation_id)
            if other is not None and other != page.index:
                raise InvalidTransitionError(
                    f"allocation {location.allocation_id} is page {other} already"
                )
            table.bind(page, location.allocation_id, location.generation)
            page.key = key
            page.location = location
            return table.ref(page)

    def quarantine_holder(self, holder: str, tag: str | None = None) -> list[PageRef]:
        """CREDITED -> QUARANTINED for an ended holder, under ``tag``
        (holder_tag by default)."""
        tag = holder_tag(holder) if tag is None else tag
        with self._lock:
            out = []
            for table in self._tables.values():
                for page in table.pages:
                    if page.state == PageState.CREDITED and page.holder == holder:
                        table.quarantine(page, PageState.CREDITED, tag)
                        self._held[holder] -= table.page_size
                        out.append(table.ref(page))
            if self._held[holder] == 0:
                del self._held[holder]
            self._waiting.pop(holder, None)
            self._asked.pop(holder, None)
            return out

    def quarantine_credit(
        self, holder: str, credit: Location, tag: str
    ) -> PageRef | None:
        """CREDITED -> QUARANTINED under ``tag`` for the one page ``holder``
        holds at ``credit``; None if it holds none there."""
        with self._lock:
            page, _ = self.check_credit(holder, credit, returning=True)
            if page is None:
                return None
            table = self._tables[credit.pool_id]
            table.quarantine(page, PageState.CREDITED, tag)
            self._held[holder] -= table.page_size
            return table.ref(page)

    def resolve_quarantined(
        self,
        location: Location,
        tag: str,
        key: Key | None = None,
        logical_bytes: int = 0,
    ) -> tuple[PageRef | None, Location | None, str]:
        """Release evidence for the page quarantined under ``tag`` at exactly
        ``location`` (a whole page): a transfer to it that ended DONE
        (``key``) makes it PENDING and gives the location to commit (length
        ``logical_bytes``); one never posted (no key) frees it. Else the
        reason it is not such a page."""
        with self._lock:
            table = self._tables.get(location.pool_id)
            ref = table.find(location) if table is not None else None
            if table is None or ref is None:
                return None, None, f"no page at {location}"
            page = table.pages[ref.index]
            if page.state != PageState.QUARANTINED or page.tag != tag:
                return None, None, f"the page is {page.state}, not quarantined by {tag}"
            if location.kind != table.info.kind or location.length != table.page_size:
                return None, None, "the location is not the whole page"
            if key is None:
                self._to_free(table, page, PageState.QUARANTINED)
                return ref, None, ""
            if not 0 < logical_bytes <= table.page_size:
                return None, None, f"logical_bytes {logical_bytes} out of range"
            table.move(page, PageState.QUARANTINED, PageState.PENDING)
            page.tag = None
            page.key = key
            page.location = replace(location, length=logical_bytes)
            return ref, page.location, ""

    def release_quarantine(self, tag: str, pool_id: str | None = None) -> int:
        """QUARANTINED -> FREE for pages waiting on ``tag``; returns the count."""
        with self._lock:
            freed = 0
            for table in self._tables.values():
                if pool_id is not None and table.pool_id != pool_id:
                    continue
                for page in table.pages:
                    if page.state == PageState.QUARANTINED and page.tag == tag:
                        self._to_free(table, page, PageState.QUARANTINED)
                        freed += 1
            return freed

    def recovered(
        self, pool_id: str, items: Iterable[tuple[Key, Location]]
    ) -> list[PageRef]:
        """QUARANTINED -> READY for pages NODE_RECOVER_LIST lists."""
        with self._lock:
            table = self._tables.get(pool_id)
            if table is None:
                raise InvalidTransitionError(f"no pool {pool_id}")
            out = []
            for key, location in items:
                page = table.at(location.offset)
                if location.pool_id != pool_id or page is None:
                    raise InvalidTransitionError(
                        f"{location} is not a page of {pool_id}"
                    )
                if page.state != PageState.QUARANTINED or page.tag != RECOVERY_TAG:
                    raise InvalidTransitionError(f"page {page.index} listed twice")
                table.move(page, PageState.QUARANTINED, PageState.READY)
                page.tag = None
                page.key = key
                page.location = location
                table.bind(page, location.allocation_id, location.generation)
                out.append(table.ref(page))
            return out

    def ready_pages(self) -> list[tuple[PageRef, Key, Location]]:
        """Every READY page with its key and location."""
        with self._lock:
            out = []
            for table in self._tables.values():
                for page in table.pages:
                    if (
                        page.state == PageState.READY
                        and page.key is not None
                        and page.location is not None
                    ):
                        out.append((table.ref(page), page.key, page.location))
            return out

    def reserve_transfer(self, pool_id: str, count: int) -> list[PageRef]:
        """FREE -> TRANSFER for the service's own transfers; may use the reserve."""
        with self._lock:
            if self._epoch_seq is None:
                return []
            table = self._tables[pool_id]
            count = min(
                count, table.free_count(), GENERATION_COUNTER_MAX - self._counter + 1
            )
            out = []
            for _ in range(max(0, count)):
                page = table.take(PageState.TRANSFER, self._next_generation())
                out.append(table.ref(page))
            return out

    def transfer_location(self, ref: PageRef) -> Location:
        with self._lock:
            table, page = self._page(ref, PageState.TRANSFER)
            return table.location(page)

    def transfer_failed(self, ref: PageRef) -> None:
        """TRANSFER -> FREE, once the failed transfer has ended."""
        with self._lock:
            table, page = self._page(ref, PageState.TRANSFER)
            self._to_free(table, page, PageState.TRANSFER)

    def quarantine_transfer(self, ref: PageRef, tag: str) -> None:
        """TRANSFER -> QUARANTINED after the tier takes over its I/O."""
        with self._lock:
            table, page = self._page(ref, PageState.TRANSFER)
            if self._io.get((ref.pool_id, ref.index)):
                raise InvalidTransitionError(f"page {page.index} has I/O running")
            table.quarantine(page, PageState.TRANSFER, tag)

    def io_acquire_transfer(self, ref: PageRef) -> bool:
        """An I/O reference on a TRANSFER destination at its generation."""
        with self._lock:
            table = self._tables.get(ref.pool_id)
            if table is None or not 0 <= ref.index < len(table.pages):
                return False
            page = table.pages[ref.index]
            if page.generation != ref.generation or page.state != PageState.TRANSFER:
                return False
            self._io[(ref.pool_id, ref.index)] += 1
            return True

    def io_acquire(self, src: PageRef, dst: PageRef) -> bool:
        """An I/O reference on both pages of a transfer, if ``src`` is READY
        and ``dst`` TRANSFER, each at its generation; else none is taken."""
        with self._lock:
            for ref, state in ((src, PageState.READY), (dst, PageState.TRANSFER)):
                table = self._tables.get(ref.pool_id)
                if table is None or not 0 <= ref.index < len(table.pages):
                    return False
                page = table.pages[ref.index]
                if page.generation != ref.generation or page.state != state:
                    return False
            for ref in (src, dst):
                self._io[(ref.pool_id, ref.index)] += 1
            return True

    def io_acquire_one(self, ref: PageRef) -> bool:
        """An I/O reference on a READY page at its generation (a transfer
        whose other end is another node's page); False if it is not."""
        with self._lock:
            table = self._tables.get(ref.pool_id)
            if table is None or not 0 <= ref.index < len(table.pages):
                return False
            page = table.pages[ref.index]
            if page.generation != ref.generation or page.state != PageState.READY:
                return False
            self._io[(ref.pool_id, ref.index)] += 1
            return True

    def io_release(self, refs: Sequence[PageRef]) -> None:
        """The I/O on these pages has ended."""
        with self._lock:
            for ref in refs:
                key = (ref.pool_id, ref.index)
                if self._io[key] <= 0:
                    raise InvalidTransitionError(f"no I/O runs on {ref}")
                self._io[key] -= 1
                if not self._io[key]:
                    del self._io[key]

    def io_refs(self, ref: PageRef) -> int:
        """I/O that may still run on the page, at any generation."""
        with self._lock:
            return self._io.get((ref.pool_id, ref.index), 0)

    # ---------------------------------------------------------------- queries

    def page(self, ref: PageRef) -> Page | None:
        """A copy of the page if it is still at ``ref``'s generation."""
        with self._lock:
            table = self._tables.get(ref.pool_id)
            if table is None or not 0 <= ref.index < len(table.pages):
                return None
            page = table.pages[ref.index]
            if page.generation != ref.generation:
                return None
            return replace(page)

    def find(self, location: Location) -> PageRef | None:
        """The non-FREE page at exactly this location."""
        with self._lock:
            table = self._tables.get(location.pool_id)
            return table.find(location) if table else None

    def find_allocation(
        self, pool_id: str, allocation_id: str, generation: int
    ) -> PageRef | None:
        """The page holding this allocation at this generation, if any."""
        with self._lock:
            table = self._tables.get(pool_id)
            return table.find_allocation(allocation_id, generation) if table else None

    def find_ready(self, reads: Iterable[tuple[str, int, int]]) -> list[PageRef]:
        """The READY pages at these (pool_id, offset, generation), in order;
        others are skipped. The lock is taken per page, never for long."""
        out = []
        for pool_id, offset, generation in reads:
            with self._lock:
                table = self._tables.get(pool_id)
                page = table.at(offset) if table is not None else None
                if (
                    page is None
                    or page.generation != generation
                    or page.state != PageState.READY
                ):
                    continue
            out.append(PageRef(pool_id, page.index, generation))
        return out

    def pages_in(self, pool_id: str, state: PageState) -> list[PageRef]:
        with self._lock:
            return self._tables[pool_id].pages_in(state)

    def bytes_in(self, state: PageState, pool_id: str | None = None) -> int:
        with self._lock:
            if pool_id is not None:
                return self._tables[pool_id].bytes[state]
            return self._bytes(state)

    def usage(self) -> list[PoolUsage]:
        with self._lock:
            return [
                PoolUsage(
                    pool_id=table.pool_id,
                    capacity=table.info.capacity,
                    free=table.bytes[PageState.FREE],
                    credited=table.bytes[PageState.CREDITED],
                    transfer=table.bytes[PageState.TRANSFER],
                    pending=table.bytes[PageState.PENDING],
                    ready=table.bytes[PageState.READY],
                    retiring=table.bytes[PageState.RETIRING],
                    quarantined=table.bytes[PageState.QUARANTINED],
                )
                for table in self._tables.values()
            ]

    def check(self) -> None:
        """Assert the invariants of every pool and the credit per holder."""
        with self._lock:
            held: collections.Counter[str] = collections.Counter()
            for table in self._tables.values():
                held += table.check()
            assert +held == +self._held, (held, self._held)
            for pool_id, index in self._io:
                page = self._tables[pool_id].pages[index]
                assert page.state != PageState.FREE, (pool_id, index)

    # ---------------------------------------------------------------- helpers

    def _bytes(self, state: PageState) -> int:
        return sum(table.bytes[state] for table in self._tables.values())

    def _page(self, ref: PageRef, state: PageState) -> tuple[PageTable, Page]:
        table = self._tables.get(ref.pool_id)
        if table is None:
            raise InvalidTransitionError(f"no page {ref}")
        return table, table.get(ref, state)

    def _to_free(self, table: PageTable, page: Page, src: PageState) -> None:
        """``src`` -> FREE, or QUARANTINED while its pool is quarantined."""
        if self._io.get((table.pool_id, page.index)):
            raise InvalidTransitionError(f"page {page.index} has I/O running")
        if table.pool_id in self._quarantined:
            if src == PageState.QUARANTINED:
                page.tag = RECOVERY_TAG
            else:
                table.quarantine(page, src, RECOVERY_TAG)
            return
        table.free(page, src)
