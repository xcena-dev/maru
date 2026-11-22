# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""GPU node worker endpoint and leased-pool holds."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from maru_common.storage_types import CloseMode, PoolDescriptor

from ..config import TierTraits
from ..placement import PlacementPolicy
from ..tiers import L1Pool, L1Tier, Leased
from .local_pin import LocalPins
from .local_server import LocalServer
from .peer_watcher import PeerWatcher
from .workers import WorkerOps

if TYPE_CHECKING:
    from ..node_run import NodeRun


class GpuNode:
    """Worker services for one node run."""

    name = "gpu"

    def __init__(self):
        self.local: LocalServer | None = None

    def capabilities(self) -> list[str]:
        return []

    def make_store(
        self, run: NodeRun, placement: PlacementPolicy, traits: dict[str, TierTraits]
    ) -> WorkerOps:
        return WorkerOps(
            node_epoch=run.node_epoch,
            pool_manager=run.pools,
            placement=placement,
            local_pins=LocalPins(),
            metadata=run.server,
            watcher=PeerWatcher(),
            gpu_drain_grace_s=run.gpu_drain_grace_s,
            evict_interval_s=run.evict_interval_s,
            op_window=run.op_window,
            traits=traits,
            on_worker_settled=lambda incarnation: self.worker_settled(run, incarnation),
            on_end=lambda reason: run._end_run(reason, CloseMode.RESTART, new_run=True),
        )

    def bind(self, run: NodeRun) -> None:
        """Bind the worker socket with both ownership locks held."""
        config = run.config
        self.local = LocalServer(
            config.socket,
            None,
            node_id=config.node_id,
            node_epoch=run.node_epoch,
            server_address=config.server,
            page_size=config.page_size,
            budgets=config.budgets,  # the workers' are set before it serves
            pools=[],
            transfer_defaults=config.transfer_defaults(),
            socket_group=config.socket_group,
            capabilities=frozenset(),
            ownership=run._ownership,
        )
        self.local.claim()

    def serve(self, run: NodeRun) -> None:
        """Workers attach from now: they map the L1 pools."""
        assert self.local is not None and run.store is not None
        self.local.manager = run.store
        # memfd indexes the fds sent with ATTACH
        attach: list[tuple[PoolDescriptor, int | None]] = []
        for tier in run._tiers.values():
            if isinstance(tier, L1Tier):
                for pool in tier.pools:
                    fd = pool.worker_fd()
                    memfd = (
                        None if fd is None else sum(f is not None for _, f in attach)
                    )
                    attach.append((pool.descriptor(memfd=memfd), fd))
        self.local.pools = attach
        self.local.start()

    def worker_settled(self, run: NodeRun, incarnation: str) -> None:
        """Its GPU drain grace is over: its holds on leased pools can go."""
        for lease in run._leases():
            lease.worker_settled(incarnation)

    def unmap_leased(
        self, run: NodeRun, tiers: Sequence[Leased], pools: Sequence[L1Pool]
    ) -> set[str]:
        """Of the leased tiers' ``pools``, those no worker can map any more,
        before they go back."""
        # A pool an earlier run still holds stays held after this
        earlier: set[str] = set()
        for tier in tiers:
            earlier |= set(tier.earlier_holds())
        if run.store is None or self.local is None or not self.local.started:
            # No worker of this run mapped them
            return {p.pool_id for p in pools} - earlier
        # A new ATTACH maps none of them: its record exists before its reply;
        # and no credit comes from them any more
        ids = {p.pool_id for p in pools}
        self.local.pools = [
            (d, fd) for d, fd in self.local.pools if d.pool.pool_id not in ids
        ]
        for pool in pools:
            run.pools.disable_pool(pool.pool_id)
        timeout = run.unmap_timeout_s
        if run.ended is not None:  # the lessor may free them once the lease ends
            timeout = min([timeout, *(t.time_left() / 2 for t in tiers)])
        done = run.store.unmap_pools(
            [(p.pool_id, p.pool_generation) for p in pools], max(0.0, timeout)
        )
        return done - earlier

    def stop_workers(self) -> None:
        if self.local is not None:
            self.local.stop()
