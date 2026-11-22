# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Owns mapped pools and delegates asynchronous I/O to L2 tiers."""

import logging
import mmap
import threading
from collections.abc import Callable
from dataclasses import dataclass

from maru_common.storage_types import Location

from .page_table import PageRef
from .tiers import IOResult, L2Tier, Span

DEFAULT_TRANSFER_BYTES_PER_S = 4 << 30

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Copy:
    """``nbytes`` from the start of ``src`` to the start of ``dst``."""

    src: Location
    dst: Location
    nbytes: int


class TransferEngine:
    """Thread safe. The engine owns the mappings it is given and closes them."""

    def __init__(self, bytes_per_s: float = DEFAULT_TRANSFER_BYTES_PER_S):
        if bytes_per_s <= 0:
            raise ValueError(f"bytes_per_s must be positive, got {bytes_per_s}")
        self.bytes_per_s = bytes_per_s
        # Guards mappings and L2 registrations
        self._lock = threading.Lock()
        self._maps: dict[str, mmap.mmap] = {}
        # L2 pools: only their tier moves their pages
        self._l2: dict[str, L2Tier] = {}
        # Every L2 tier, in the order added, pools or not
        self._l2_tiers: list[L2Tier] = []
        # Mappings a tier could not detach: never closed
        self._kept: dict[str, mmap.mmap] = {}

    def add_pool(self, pool_id: str, mapping: mmap.mmap) -> None:
        with self._lock:
            self._maps[pool_id] = mapping
            for tier in self._tiers():
                tier.attach(pool_id, mapping)

    def add_l2(self, tier: L2Tier) -> None:
        """An L2 tier (and its pools): it gets every mapping; stop() drains
        it, its owner closes it after."""
        with self._lock:
            self._l2_tiers.append(tier)
            for info in tier.pool_infos():
                self._l2[info.pool_id] = tier
            for pool_id, mapping in self._maps.items():
                tier.attach(pool_id, mapping)

    def has_pool(self, pool_id: str) -> bool:
        """Whether transfers reach the pool: mapped here, or an L2 pool."""
        with self._lock:
            return pool_id in self._maps or pool_id in self._l2

    def l2_pools(self) -> dict[str, L2Tier]:
        """The L2 tier of each L2 pool."""
        with self._lock:
            return dict(self._l2)

    def _tiers(self) -> list[L2Tier]:
        """Caller holds the lock: each L2 tier once, in the order added."""
        return list(self._l2_tiers)

    def _io_tier(self, copy: Copy) -> tuple[L2Tier | None, bool]:
        """The tier that moves ``copy`` and whether it writes to the tier.
        Caller holds the lock."""
        if copy.dst.pool_id in self._l2:
            return self._l2[copy.dst.pool_id], True
        if copy.src.pool_id in self._l2:
            return self._l2[copy.src.pool_id], False
        return None, False

    def submit_io(
        self,
        copy: Copy,
        src: PageRef | None,
        dst: PageRef | None,
        *,
        callback: Callable[[IOResult], None] | None = None,
        deadline: float | None = None,
        settled: Callable[[IOResult], None] | None = None,
    ) -> IOResult:
        """Submit L2 I/O; hold page references until callback, or settled if kept.
        The deadline is monotonic; rejected submissions raise."""
        n = copy.nbytes
        with self._lock:
            tier, write = self._io_tier(copy)
            mapping = self._maps.get(copy.src.pool_id if write else copy.dst.pool_id)
        offset = (copy.src if write else copy.dst).offset
        if (
            tier is None
            or mapping is None
            or not 0 < n <= min(copy.src.length, copy.dst.length)
            or offset < 0
            or offset + n > len(mapping)
        ):
            raise ValueError(f"no file I/O for {copy}")
        refs = (src, dst)
        # A tier that never keeps pages need not take ``settled``
        extra = {} if settled is None else {"settled": settled}
        if write:
            span = Span(copy.src.pool_id, offset, n)
            return tier.submit_store(
                span, copy.dst, refs=refs, callback=callback, deadline=deadline, **extra
            )
        span = Span(copy.dst.pool_id, offset, n)
        return tier.submit_load(
            copy.src, span, refs=refs, callback=callback, deadline=deadline, **extra
        )

    def stop(self) -> None:
        """Drain every L2 tier."""
        with self._lock:
            tiers = self._tiers()
        for tier in tiers:
            tier.drain()

    def close(self) -> set[str]:
        """Drain and detach before unmapping; keep mappings a tier cannot detach."""
        self.stop()
        with self._lock:
            maps, self._maps = self._maps, {}
            tiers, self._l2, self._l2_tiers = self._tiers(), {}, []
        for pool_id, mapping in maps.items():
            try:
                for tier in tiers:
                    tier.detach(pool_id)
            except Exception:
                logger.exception("cannot detach %s, so it stays mapped", pool_id)
                self._kept[pool_id] = mapping
                continue
            mapping.close()
        return set(self._kept)
