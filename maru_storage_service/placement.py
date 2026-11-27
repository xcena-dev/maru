# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""PlacementPolicy: LRU removal candidates per tier.

Candidates require a successful RETIRE before their pages are reused.
"""

import collections
import heapq
import itertools
import threading
import time
from collections.abc import Callable, Collection, Iterable

from maru_common.storage_types import KeptCause

from .pool_manager import PageRef

DEFAULT_KEPT_BACKOFF_S = 1.0


class PlacementPolicy:
    """LRU per pool and the watermark rule per tier: a tier's pools are one
    space (a full CXL region beside empty ones is no pressure). Thread safe."""

    def __init__(
        self,
        high_watermark: float,
        low_watermark: float,
        *,
        kept_backoff_s: float = DEFAULT_KEPT_BACKOFF_S,
        clock: Callable[[], float] = time.monotonic,
    ):
        if not 0 < low_watermark < high_watermark <= 1:
            raise ValueError("need 0 < low_watermark < high_watermark <= 1")
        self.high = high_watermark
        self.low = low_watermark
        self.kept_backoff_s = kept_backoff_s
        self._clock = clock
        self._lock = threading.Lock()
        # Least recently used first; the value orders pages across pools
        self._lru: dict[str, collections.OrderedDict[PageRef, int]] = {}
        self._seq = itertools.count()
        self._inflight: set[PageRef] = set()
        self._backoff: dict[PageRef, float] = {}
        # Never offered again (KEPT MOVED/INVALID, unknown outcome)
        self._held: set[PageRef] = set()
        self._evicting: dict[str, bool] = {}
        # (high, low) of tiers other than the top one
        self._marks: dict[str, tuple[float, float]] = {}

    def set_watermarks(self, tier: str, high: float, low: float) -> None:
        """The watermarks of ``tier`` instead of the top tier's."""
        if not 0 < low < high <= 1:
            raise ValueError("need 0 < low_watermark < high_watermark <= 1")
        with self._lock:
            self._marks[tier] = (high, low)

    def above_high(self, tier: str, *, capacity: int, free_bytes: int) -> bool:
        """The tier (its pools' capacity and FREE bytes) uses more than its
        high watermark."""
        with self._lock:
            high, _ = self._marks.get(tier, (self.high, self.low))
        return capacity - free_bytes > high * capacity

    def on_ready(self, ref: PageRef) -> None:
        """A page became READY: most recently used."""
        with self._lock:
            lru = self._lru.setdefault(ref.pool_id, collections.OrderedDict())
            lru[ref] = next(self._seq)
            lru.move_to_end(ref)

    def on_gone(self, ref: PageRef) -> None:
        """The page left READY for good."""
        with self._lock:
            self._forget(ref)

    def on_access(self, refs: Iterable[PageRef]) -> int:
        """Move pages MaruServer saw read, oldest read first, to the recent
        end; returns the count."""
        moved = 0
        for ref in refs:
            with self._lock:
                lru = self._lru.get(ref.pool_id)
                if lru is not None and ref in lru:
                    lru[ref] = next(self._seq)
                    lru.move_to_end(ref)
                    moved += 1
        return moved

    def candidates(
        self,
        tier: str,
        pool_ids: Iterable[str],
        *,
        capacity: int,
        free_bytes: int,
        retiring_bytes: int,
        page_size: int,
        limit: int,
        demand_bytes: int = 0,
        prefer: Collection[PageRef] = (),
    ) -> list[PageRef]:
        """READY pages of ``pool_ids`` to retire, LRU first across them: from
        above the tier's ``high`` down to its ``low``, and until FREE bytes
        reach ``demand_bytes`` (a fetch needs them now, whatever the
        watermarks). ``capacity`` and the bytes are those pools' sums.
        In-flight and RETIRING pages count as leaving. Pages in ``prefer``
        come first."""
        pools = set(pool_ids)
        with self._lock:
            high, low = self._marks.get(tier, (self.high, self.low))
            used = capacity - free_bytes
            evicting = self._evicting.get(tier, False)
            if not evicting and used > high * capacity:
                evicting = True
            if used <= low * capacity:
                evicting = False
            self._evicting[tier] = evicting
            inflight = sum(1 for r in self._inflight if r.pool_id in pools)
            leaving = retiring_bytes + inflight * page_size
            need = used - leaving - low * capacity if evicting else 0
            need = max(need, demand_bytes - free_bytes - leaving)
            if need <= 0:
                return []
            now = self._clock()
            out: list[PageRef] = []
            oldest_first: Iterable[tuple[int, PageRef]] = heapq.merge(
                *(
                    ((seq, ref) for ref, seq in self._lru[p].items())
                    for p in pools
                    if p in self._lru
                ),
                key=lambda item: item[0],
            )
            if prefer:
                ordered = list(oldest_first)
                oldest_first = [i for i in ordered if i[1] in prefer] + [
                    i for i in ordered if i[1] not in prefer
                ]
            for _, ref in oldest_first:
                if need <= 0 or len(out) >= limit:
                    break
                if ref in self._inflight or ref in self._held:
                    continue
                until = self._backoff.get(ref)
                if until is not None:
                    if until > now:
                        continue
                    del self._backoff[ref]
                out.append(ref)
                need -= page_size
            self._inflight.update(out)
            return out

    def retired(self, ref: PageRef) -> None:
        with self._lock:
            self._forget(ref)

    def kept(self, ref: PageRef, cause: KeptCause) -> None:
        """PINNED: retried after a backoff; MOVED, INVALID: never again."""
        with self._lock:
            self._inflight.discard(ref)
            if cause == KeptCause.PINNED:
                self._backoff[ref] = self._clock() + self.kept_backoff_s
            else:
                self._held.add(ref)

    def hold(self, ref: PageRef) -> None:
        """Never offer the page again."""
        with self._lock:
            self._inflight.discard(ref)
            self._held.add(ref)

    def release(self, refs: Iterable[PageRef], backoff: bool = True) -> None:
        """Not retired; offered again, after a backoff unless ``backoff`` is
        False."""
        with self._lock:
            until = self._clock() + self.kept_backoff_s
            for ref in refs:
                self._inflight.discard(ref)
                if backoff:
                    self._backoff[ref] = until

    def is_held(self, ref: PageRef) -> bool:
        with self._lock:
            return ref in self._held

    def _forget(self, ref: PageRef) -> None:
        lru = self._lru.get(ref.pool_id)
        if lru is not None:
            lru.pop(ref, None)
        self._inflight.discard(ref)
        self._backoff.pop(ref, None)
        self._held.discard(ref)
