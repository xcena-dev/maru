# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Memory node capabilities and store policy."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING

from maru_common.storage_types import MEMORY_CAPABILITY, CloseMode, KeptCause

from ..config import TierTraits
from ..node_store import NodeStore
from ..placement import PlacementPolicy
from ..pool_manager import PageRef
from ..tiers import L1Pool, Leased

if TYPE_CHECKING:
    from ..node_run import NodeRun


class MemoryNode:
    """A store without local workers or local readers."""

    name = "memory"
    local = None

    def capabilities(self) -> list[str]:
        return [MEMORY_CAPABILITY]

    def bind(self, run: NodeRun) -> None:
        pass

    def make_store(
        self, run: NodeRun, placement: PlacementPolicy, traits: dict[str, TierTraits]
    ) -> NodeStore:
        return NodeStore(
            node_epoch=run.node_epoch,
            pool_manager=run.pools,
            placement=placement,
            local_pins=self,
            metadata=run.server,
            evict_interval_s=run.evict_interval_s,
            traits=traits,
            on_end=lambda reason: run._end_run(reason, CloseMode.RESTART, new_run=True),
        )

    def serve(self, run: NodeRun) -> None:
        pass

    def unmap_leased(
        self, run: NodeRun, tiers: Sequence[Leased], pools: Sequence[L1Pool]
    ) -> set[str]:
        earlier: set[str] = set()
        for tier in tiers:
            earlier |= set(tier.earlier_holds())
        return {p.pool_id for p in pools} - earlier

    def stop_workers(self) -> None:
        pass

    def worker_settled(self, run: NodeRun, incarnation: str) -> None:
        pass

    def block(self, refs: Iterable[PageRef]) -> None:
        pass

    def unblock(self, refs: Iterable[PageRef]) -> None:
        pass

    def after_kept(self, ref: PageRef, cause: KeptCause) -> None:
        pass

    def forget(self, ref: PageRef) -> None:
        pass

    def readers(self, ref: PageRef) -> int:
        return 0
