# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Catalog: key -> locations, and the pool registry.

Not thread safe: callers hold the service lock.
"""

import itertools
import logging
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from enum import StrEnum, unique

from maru_common.protocol import CommitItem
from maru_common.storage_types import (
    POOL_KINDS,
    ItemOutcome,
    ItemResult,
    KeptCause,
    Key,
    Location,
    MissReason,
    PoolInfo,
    PoolKind,
    generation_epoch_seq,
)

from .pin_manager import PinManager

logger = logging.getLogger(__name__)

_GENERATION_MAX = (1 << 64) - 1


@unique
class PoolState(StrEnum):
    ACTIVE = "active"
    HIDDEN = "hidden"  # kept but not found: DRAM of a node closed for restart
    REMOVING = "removing"  # not found; locations go once unpinned (or forced)


@dataclass(eq=False)
class PoolRecord:
    """One registration of a pool; its locations belong to it, so a pool_id
    registered again starts empty while the old one is swept."""

    info: PoolInfo
    # epoch_seq of the node run that registered it: which workers map it
    since_epoch_seq: int
    registration: int = 0
    state: PoolState = PoolState.ACTIVE
    force: bool = False  # remove even pinned locations (never for CXL)
    entries: dict[str, "Entry"] = field(default_factory=dict)  # by allocation_id
    kept: dict[str, "Entry"] = field(default_factory=dict)  # swept but pinned
    grid: dict[int, list["Entry"]] = field(default_factory=dict)  # by page

    @property
    def pool_id(self) -> str:
        return self.info.pool_id

    @property
    def node_id(self) -> str:
        return self.info.node_id

    @property
    def kind(self) -> PoolKind:
        return self.info.kind

    def pages(self, location: Location) -> range:
        """Page indexes a byte range touches, clipped to the pool."""
        start = max(location.offset, 0)
        end = min(location.offset + location.length, self.info.capacity)
        if end <= start or self.info.page_size <= 0:
            return range(0)
        size = self.info.page_size
        return range(start // size, (end - 1) // size + 1)


@dataclass(eq=False)
class Entry:
    key: Key
    location: Location
    logical_bytes: int
    pool: PoolRecord


@dataclass
class Requester:
    """Who looks up: which pools it reads directly, which its node can fetch, and
    which StorageServices are up."""

    node_id: str
    can_map: Callable[[PoolRecord], bool]
    online: Callable[[str], bool]


@dataclass
class Selection:
    result: ItemResult  # HIT or MISS
    entry: Entry | None = None
    reason: MissReason | None = None


def location_cost(requester_node: str, pool: PoolRecord) -> int:
    """Cost order: node DRAM < node CXL < memory node DRAM/CXL; any other
    node counts as a memory node."""
    traits = POOL_KINDS[pool.kind]
    return traits.local_rank if pool.node_id == requester_node else traits.remote_rank


def _overlap(a: Location, b: Location) -> bool:
    return a.offset < b.offset + b.length and b.offset < a.offset + a.length


@dataclass
class CatalogStats:
    keys: int = 0
    locations: int = 0
    pools: dict[str, int] = field(default_factory=dict)


class Catalog:
    def __init__(self, pins: PinManager):
        self.pins = pins
        pins.on_last_unpin = self._location_unpinned
        self._pools: dict[str, PoolRecord] = {}  # current registration
        self._node_pools: dict[str, set[str]] = {}
        # may still hold entries of an older registration until swept
        self._by_key: dict[Key, dict[str, Entry]] = {}
        self._preserved: frozenset[Key] = frozenset()
        self._removing: set[PoolRecord] = set()
        self._sweeps: dict[PoolRecord, None] = {}  # REMOVING pools to sweep
        self._registrations = itertools.count(1)

    # ------------------------------------------------------------- registry

    def register_pool(self, info: PoolInfo, since_epoch_seq: int) -> PoolRecord:
        current = self._pools.get(info.pool_id)
        if current is not None and current.state != PoolState.REMOVING:
            raise ValueError(f"pool {info.pool_id} is registered already")
        if info.page_size <= 0:
            raise ValueError("page_size must be positive")
        if current is not None and current.node_id != info.node_id:
            # a CXL region now with another node: the old record is swept
            # on its own and no longer one of its node's pools
            self._unindex(current)
        record = PoolRecord(
            info=info,
            since_epoch_seq=since_epoch_seq,
            registration=next(self._registrations),
        )
        self._pools[info.pool_id] = record
        self._node_pools.setdefault(info.node_id, set()).add(info.pool_id)
        return record

    def renew_registration(self, pool: PoolRecord) -> None:
        pool.registration = next(self._registrations)

    def pool(self, pool_id: str) -> PoolRecord | None:
        return self._pools.get(pool_id)

    def pools_of(self, node_id: str) -> list[PoolRecord]:
        return [self._pools[p] for p in sorted(self._node_pools.get(node_id, ()))]

    def entries_in(self, pool_id: str) -> dict[str, Entry]:
        pool = self._pools.get(pool_id)
        return pool.entries if pool is not None else {}

    def hide_pool(self, pool: PoolRecord) -> None:
        if pool.state == PoolState.ACTIVE:
            pool.state = PoolState.HIDDEN

    def remove_pool(
        self, pool: PoolRecord, force: bool = False, force_cxl: bool = False
    ) -> None:
        """Hide now; ``sweep`` removes the locations. CXL is forced only with
        ``force_cxl`` (its node given up, its pins released): otherwise a
        region goes back only once nothing pins it."""
        pool.force = pool.force or (
            force and (not POOL_KINDS[pool.kind].rm_region or force_cxl)
        )
        pool.state = PoolState.REMOVING
        self._removing.add(pool)
        self._sweeps[pool] = None
        self._drop_if_empty(pool)

    def sweep(self, budget: int) -> int:
        """Remove locations of REMOVING pools, at most ``budget`` steps;
        unforced pinned ones go with their last pin."""
        done = 0
        for pool in list(self._sweeps):
            while done < budget:
                if pool.entries:
                    allocation_id, entry = pool.entries.popitem()
                    done += 1
                    if pool.force or not self.pins.count(entry.key, entry.location):
                        self._unlink(entry)
                    else:
                        pool.kept[allocation_id] = entry
                elif pool.force and pool.kept:
                    self._unlink(pool.kept.popitem()[1])
                    done += 1
                else:
                    break
            if not pool.entries and not (pool.force and pool.kept):
                self._sweeps.pop(pool, None)
                self._drop_if_empty(pool)
            if done >= budget:
                break
        return done

    def sweeping(self) -> bool:
        return bool(self._sweeps)

    def _drop_if_empty(self, pool: PoolRecord) -> None:
        if pool.state != PoolState.REMOVING or pool.entries or pool.kept:
            return
        self._removing.discard(pool)
        self._sweeps.pop(pool, None)
        if self._pools.get(pool.pool_id) is pool:
            del self._pools[pool.pool_id]
            self._unindex(pool)

    def _unindex(self, pool: PoolRecord) -> None:
        node_pools = self._node_pools.get(pool.node_id)
        if node_pools is not None:
            node_pools.discard(pool.pool_id)
            if not node_pools:
                del self._node_pools[pool.node_id]

    def _location_unpinned(self, key: Key, location: Location) -> None:
        candidates = [self._by_key.get(key, {}).get(location.pool_id)]
        for pool in self._removing:
            if pool.pool_id == location.pool_id:
                candidates.append(pool.entries.get(location.allocation_id))
                candidates.append(pool.kept.get(location.allocation_id))
        for entry in candidates:
            if (
                entry is not None
                and entry.key == key
                and entry.location == location
                and entry.pool.state == PoolState.REMOVING
            ):
                self._remove(entry)
                self._drop_if_empty(entry.pool)
                return

    # -------------------------------------------------------------- entries

    def locations(self, key: Key) -> list[Entry]:
        """All entries of the key, in any pool state."""
        return list(self._by_key.get(key, {}).values())

    def _current(self, key: Key, pool_id: str) -> Entry | None:
        entry = self._by_key.get(key, {}).get(pool_id)
        if entry is None or entry.pool is not self._pools.get(pool_id):
            return None
        return entry

    def entry_exact(self, key: Key, location: Location) -> Entry | None:
        entry = self._current(key, location.pool_id)
        return entry if entry is not None and entry.location == location else None

    def _page_holder(self, pool: PoolRecord, location: Location) -> Entry | None:
        """A live location on the same allocation or overlapping bytes."""
        same = pool.entries.get(location.allocation_id)
        if same is not None:
            return same
        for page in pool.pages(location):
            for entry in pool.grid.get(page, ()):
                if _overlap(entry.location, location):
                    return entry
        return None

    def _add(self, entry: Entry) -> None:
        """All or nothing: a failure leaves no trace of the entry."""
        pool = entry.pool
        try:
            pool.entries[entry.location.allocation_id] = entry
            for page in pool.pages(entry.location):
                pool.grid.setdefault(page, []).append(entry)
            self._by_key.setdefault(entry.key, {})[entry.location.pool_id] = entry
        except BaseException:
            self._remove(entry)
            raise

    def _unlink(self, entry: Entry) -> None:
        """Drop an entry from the key index and the page grid."""
        pool = entry.pool
        for page in pool.pages(entry.location):
            on_page = pool.grid.get(page)
            if on_page is not None:
                on_page[:] = [e for e in on_page if e is not entry]
                if not on_page:
                    del pool.grid[page]
        by_key = self._by_key.get(entry.key)
        if by_key is not None and by_key.get(entry.location.pool_id) is entry:
            del by_key[entry.location.pool_id]
            if not by_key:
                del self._by_key[entry.key]

    def _remove(self, entry: Entry) -> None:
        allocation_id = entry.location.allocation_id
        for table in (entry.pool.entries, entry.pool.kept):
            if table.get(allocation_id) is entry:
                del table[allocation_id]
        self._unlink(entry)

    def remove_location(self, entry: Entry) -> None:
        self._remove(entry)
        self._drop_if_empty(entry.pool)

    def stats(self) -> CatalogStats:
        states: dict[str, int] = {}
        for pool in self._pools.values():
            states[pool.state.value] = states.get(pool.state.value, 0) + 1
        return CatalogStats(
            keys=len(self._by_key),
            locations=sum(len(e) for e in self._by_key.values()),
            pools=states,
        )

    # ---------------------------------------------------------- preservation

    def set_preserved(self, keys: Iterable[Key]) -> bool:
        """Replace the preserved keys; returns whether they changed."""
        preserved = frozenset(keys)
        changed = preserved != self._preserved
        self._preserved = preserved
        return changed

    def is_preserved(self, key: Key) -> bool:
        return key in self._preserved

    def held(self, entry: Entry) -> bool:
        """A pin or preservation keeps it from a retire."""
        return entry.key in self._preserved or bool(
            self.pins.count(entry.key, entry.location)
        )

    # ------------------------------------------------------------- commit

    def own_pool(self, node_id: str, pool_id: str) -> PoolRecord | None:
        """The pool if it is ACTIVE and belongs to ``node_id``."""
        pool = self._pools.get(pool_id)
        if pool is None or pool.node_id != node_id or pool.state != PoolState.ACTIVE:
            return None
        return pool

    def check_new_location(
        self,
        node_id: str,
        epoch_seq: int,
        location: Location,
        logical_bytes: int,
    ) -> str | None:
        """Why the node may not commit this location, or None."""
        pool = self.own_pool(node_id, location.pool_id)
        if pool is None:
            return f"pool {location.pool_id} is not an active pool of {node_id}"
        if location.kind != pool.kind:
            return f"location kind {location.kind} differs from pool {pool.kind}"
        if not location.allocation_id:
            return "empty allocation_id"
        if location.offset < 0 or location.length <= 0:
            return "location needs offset >= 0 and length > 0"
        if location.offset + location.length > pool.info.capacity:
            return "location ends beyond the pool capacity"
        if not 0 < logical_bytes <= location.length:
            return "logical_bytes must be in (0, length]"
        if not 0 <= location.generation <= _GENERATION_MAX:
            return "generation out of range"
        # The high bits name the run that gave the page
        given_by = generation_epoch_seq(location.generation)
        if given_by != epoch_seq:
            return f"generation {location.generation} is not of epoch_seq {epoch_seq}"
        return None

    def check_commit(
        self, node_id: str, epoch_seq: int, item: CommitItem
    ) -> str | None:
        # A location already in (an adopted page keeps its run's generation)
        # is ADDED again
        if self.entry_exact(item.key, item.location) is not None and self.own_pool(
            node_id, item.location.pool_id
        ):
            return None
        return self.check_new_location(
            node_id, epoch_seq, item.location, item.logical_bytes
        )

    def commit(self, item: CommitItem) -> ItemOutcome:
        """Apply one item that passed check_commit."""
        loc = item.location
        pool = self._pools[loc.pool_id]
        existing = self._current(item.key, loc.pool_id)
        if existing is not None:
            if existing.location != loc:
                return ItemOutcome(ItemResult.DUPLICATE, location=existing.location)
            # this very location is in already: not a duplicate
            return ItemOutcome(ItemResult.ADDED)
        if self._page_holder(pool, loc) is not None:
            logger.debug("COMMIT %s rejected: overlaps a live location", item.key)
            return ItemOutcome(ItemResult.REJECTED)
        self._add(Entry(item.key, loc, item.logical_bytes, pool))
        return ItemOutcome(ItemResult.ADDED)

    # ------------------------------------------------------------- retire

    def _moved(self, location: Location) -> ItemOutcome | None:
        """KEPT(MOVED) with the occupant when another published location (any
        key, or this key with another generation) holds the page; None when
        the page is free (reported as RETIRED)."""
        pool = self._pools.get(location.pool_id)
        occupant = self._page_holder(pool, location) if pool is not None else None
        if occupant is None:
            return None
        return ItemOutcome(
            ItemResult.KEPT,
            cause=KeptCause.MOVED,
            location=occupant.location,
            key=occupant.key,
        )

    def retire(self, node_id: str, key: Key, location: Location) -> ItemOutcome:
        if self.own_pool(node_id, location.pool_id) is None:
            return ItemOutcome(ItemResult.KEPT, cause=KeptCause.INVALID)
        entry = self.entry_exact(key, location)
        if entry is None:
            # absent from the Catalog means it does not exist
            return self._moved(location) or ItemOutcome(ItemResult.RETIRED)
        if self.held(entry):
            return ItemOutcome(ItemResult.KEPT, cause=KeptCause.PINNED)
        self.remove_location(entry)
        return ItemOutcome(ItemResult.RETIRED)

    # --------------------------------------------------------- source pin

    # ------------------------------------------------------------- lookup

    def select(self, key: Key, requester: Requester) -> Selection:
        """Choose the cheapest directly readable location."""
        entries = self._by_key.get(key)
        if not entries:
            return Selection(ItemResult.MISS, reason=MissReason.ABSENT)
        best_hit = None
        hit_cost = 0
        present = False
        for entry in entries.values():
            pool = entry.pool
            if pool.state == PoolState.REMOVING:
                continue
            present = True
            if pool.state == PoolState.HIDDEN:
                continue
            cost = location_cost(requester.node_id, pool)
            if requester.can_map(pool):
                if best_hit is None or cost < hit_cost:
                    best_hit, hit_cost = entry, cost
                continue
        if best_hit is not None:
            return Selection(ItemResult.HIT, entry=best_hit)
        reason = MissReason.OFFLINE if present else MissReason.ABSENT
        return Selection(ItemResult.MISS, reason=reason)

    def live_locations(self, key: Key) -> Iterator[Entry]:
        """Entries in ACTIVE pools."""
        return (
            e
            for e in self._by_key.get(key, {}).values()
            if e.pool.state == PoolState.ACTIVE
        )
