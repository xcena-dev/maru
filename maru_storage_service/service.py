# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Assemble node runs and their role services."""

import mmap
from collections.abc import Callable
from typing import Any

from .config import NodeConfig
from .config_fields import NotServableError as NotServableError
from .drivers.cxl import DEFAULT_RENEW_S, CxlRegionPool, RegionSource
from .node import NEW_RUN_PAUSE_MAX_S, NEW_RUN_PAUSE_S
from .node import Node as CoreNode
from .node_run import DEFAULT_UNMAP_TIMEOUT_S, NodeRun
from .node_run import CapabilityRefusedError as CapabilityRefusedError
from .node_run import check_servable as check_servable
from .node_store import DEFAULT_EVICT_INTERVAL_S
from .op_ledger import OP_WINDOW
from .placement import DEFAULT_KEPT_BACKOFF_S
from .transfer_engine import DEFAULT_TRANSFER_BYTES_PER_S


class StorageService(NodeRun):
    """A node run with the configured role."""

    def __init__(
        self,
        config: NodeConfig,
        *,
        rpc_timeout_s: float = 2.0,
        gpu_drain_grace_s: float = 5.0,
        evict_interval_s: float = DEFAULT_EVICT_INTERVAL_S,
        kept_backoff_s: float = DEFAULT_KEPT_BACKOFF_S,
        host_boot_id: str | None = None,
        node_epoch: str | None = None,
        op_window: int = OP_WINDOW,
        region_source: RegionSource | None = None,
        cxl_renew_s: float = DEFAULT_RENEW_S,
        unmap_timeout_s: float = DEFAULT_UNMAP_TIMEOUT_S,
        transfer_bytes_per_s: float = DEFAULT_TRANSFER_BYTES_PER_S,
        map_region: Callable[[CxlRegionPool], mmap.mmap] | None = None,
        make_transport: Callable[[str, str], Any] | None = None,
    ):
        self.make_transport = make_transport
        if config.role == "memory":
            from .memory.service import MemoryNode

            role = MemoryNode()
        else:
            from .gpu.service import GpuNode

            role = GpuNode()
        super().__init__(
            config,
            role=role,
            rpc_timeout_s=rpc_timeout_s,
            gpu_drain_grace_s=gpu_drain_grace_s,
            evict_interval_s=evict_interval_s,
            kept_backoff_s=kept_backoff_s,
            host_boot_id=host_boot_id,
            node_epoch=node_epoch,
            op_window=op_window,
            region_source=region_source,
            cxl_renew_s=cxl_renew_s,
            unmap_timeout_s=unmap_timeout_s,
            transfer_bytes_per_s=transfer_bytes_per_s,
            map_region=map_region,
        )

    @property
    def manager(self):
        return self.store

    @property
    def local(self):
        return self.role.local

    def _worker_settled(self, incarnation: str) -> None:
        self.role.worker_settled(self, incarnation)


class Node(CoreNode):
    """Runs with the configured role and one shared ownership lock."""

    def __init__(
        self,
        config: NodeConfig,
        make_service: Callable[[NodeConfig], NodeRun] = StorageService,
        pause_s: float = NEW_RUN_PAUSE_S,
        max_pause_s: float = NEW_RUN_PAUSE_MAX_S,
    ):
        super().__init__(config, make_service, pause_s, max_pause_s)
