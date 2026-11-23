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

    def __init__(self):
        self.parts = None
        self.ops = None
        from ..memory_node import RunOps

        self.remote_ops = RunOps()

    def capabilities(self) -> list[str]:
        return [MEMORY_CAPABILITY]

    def bind(self, run: NodeRun) -> None:
        if run.config.remote_server is not None:
            from ..memory_node import make_remote_parts

            self.parts = make_remote_parts(run)
            self.parts.server.start()

    def make_store(
        self, run: NodeRun, placement: PlacementPolicy, traits: dict[str, TierTraits]
    ) -> NodeStore:
        if self.parts is not None:
            from ..tiers import L1Tier

            for tier in run._tiers.values():
                if isinstance(tier, L1Tier):
                    for pool in tier.pools:
                        mapping = tier.map_for_transfers(pool)
                        if mapping is None:
                            raise RuntimeError("pool mapping failed")
                        self.parts.attach(pool.info(), mapping)
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
        if self.parts is None:
            return
        from ..peer_ops import PeerOps

        rs = run.config.remote_server
        capacity = sum(
            info.capacity
            for info in (run.pools.pool_info(p) for p in run.pools.pool_ids())
        )
        self.parts.pool_registrations = run.pool_registrations
        self.ops = PeerOps(
            run.store,
            self.parts,
            reserve_ttl_s=rs.reserve_ttl_s,
            peer_quarantine_max=rs.peer_quarantine_max,
            quarantine_max=rs.quarantine_max
            or max(run.config.page_size, capacity // 8),
        )
        self.remote_ops.target = self.ops
        self.ops.start()
        self.parts.server.boot = run.server.host_boot_id
        self.parts.server.ready(run.server.server_epoch, self.parts.remote_pools())

    def unmap_leased(
        self, run: NodeRun, tiers: Sequence[Leased], pools: Sequence[L1Pool]
    ) -> set[str]:
        earlier: set[str] = set()
        for tier in tiers:
            earlier |= set(tier.earlier_holds())
        return {p.pool_id for p in pools} - earlier

    def stop_workers(self) -> None:
        if self.parts is not None:
            self.parts.server.stop()
        if self.ops is not None:
            self.ops.stop()

    def close_exports(self) -> None:
        if self.parts is not None:
            self.parts.close()

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
