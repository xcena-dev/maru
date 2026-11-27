# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Memory node capabilities and store policy."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING

from maru_common.remote_protocol import RemoteHintResponse
from maru_common.storage_types import MEMORY_CAPABILITY, CloseMode, KeptCause

from .. import region_extension
from ..config import TierTraits
from ..drivers.cxl import CxlTier
from ..node_store import NodeStore
from ..placement import PlacementPolicy
from ..pool_manager import PageRef
from ..ports import ServerStopped
from ..region_generation import MemoryGeneration, MemoryGenerationError
from ..server_client import read_host_boot_id
from ..tiers import L1Pool, Leased

if TYPE_CHECKING:
    from ..node_run import NodeRun

logger = logging.getLogger(__name__)
EXTENSION_DRAIN_S = 30.0
MAX_HINT_RANGES = 4096
MAX_WRITE_WAIT_MS = 10_000


class MemoryNode:
    """A store without local workers or local readers."""

    name = "memory"
    local = None

    def __init__(self):
        self.parts = None
        self.ops = None
        self.run = None
        self.extension = None
        self.generation = None
        self.hint_actions = []
        self._regions = {}
        self._hint_requests = {}
        self._extension_lock = threading.Lock()
        self._fence_failed = None
        self._extension_closed = False
        from ..memory_node import RunOps

        self.remote_ops = RunOps()

    def capabilities(self) -> list[str]:
        return [MEMORY_CAPABILITY]

    def bind(self, run: NodeRun) -> None:
        self.run = run
        config = run.config.region_extension
        if config is not None:
            try:
                self.extension = region_extension.load_region_extension(
                    config.name, run.node_epoch, config.config
                )
                while True:
                    try:
                        self.generation = MemoryGeneration(
                            self.extension,
                            run.host_boot_id or read_host_boot_id(),
                            grace_s=config.generation_grace_s,
                            state_path=Path(run.config.state_dir)
                            / f"{run.config.node_id}.generation",
                            on_change=self._generation_changed,
                        )
                        break
                    except MemoryGenerationError:
                        if not run.later_run:
                            raise
                        if run._interrupted.wait(config.generation_poll_s):
                            raise ServerStopped() from None
                run.host_boot_id = self.generation.boot
                self.hint_actions = [
                    action
                    for action in region_extension.HINT_ACTIONS
                    if action
                    in getattr(
                        self.extension,
                        "hint_actions",
                        region_extension.DEFAULT_HINT_ACTIONS,
                    )
                ]
            except ServerStopped:
                raise
            except Exception as exc:
                raise MemoryGenerationError(
                    f"cannot start region extension: {exc}"
                ) from exc
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
                    for pool in list(tier.pools):
                        if not self.check_generation():
                            raise ServerStopped(
                                "memory generation changed before mapping"
                            )
                        handle = None
                        if self.extension is not None and isinstance(tier, CxlTier):
                            handle = pool.region.handle.to_dict()
                            try:
                                self.extension.validate_access(handle, pool.region)
                            except Exception:
                                logger.warning(
                                    "region extension refused %s", pool.pool_id
                                )
                                tier.pools.remove(pool)
                                continue
                        if not self.check_generation():
                            raise ServerStopped(
                                "memory generation became unknown before mapping"
                            )
                        if handle is not None:
                            self._regions[pool.pool_id] = pool
                        mapping = tier.map_for_transfers(pool)
                        if mapping is None:
                            raise RuntimeError("pool mapping failed")
                        self.parts.attach(pool.info(), mapping)
                        if handle is not None:
                            try:
                                self.extension.mapped(
                                    handle, pool.region, fresh=not pool.claimed
                                )
                            except Exception as exc:
                                raise region_extension.RegionExtensionError(
                                    f"region extension mapping failed: {exc}"
                                ) from exc
                        if not self.check_generation():
                            raise ServerStopped("memory reset while region was mapped")
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
            if self.generation is not None:
                self.generation.start(run.config.region_extension.generation_poll_s)
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
            check_memory=self.check_generation,
        )
        self.remote_ops.target = self.ops
        self.ops.start()
        self.parts.server.boot = run.server.host_boot_id
        self.parts.server.ready(run.server.server_epoch, self.parts.remote_pools())
        if self.generation is not None:
            self.generation.start(run.config.region_extension.generation_poll_s)

    def check_generation(self) -> bool:
        if self.generation is None:
            return True
        usable = self.generation.check()
        if self.parts is not None:
            self.parts.server.boot = self.generation.boot
        return usable

    def _generation_changed(self) -> None:
        if self.parts is not None:
            self.parts.server.boot = self.generation.boot
            try:
                self.parts.revoke()
            except Exception as exc:
                self._fence_failed = exc
        self.run._end_run(
            "memory generation changed or unknown", CloseMode.LEAVE, new_run=True
        )

    def unmap_leased(
        self, run: NodeRun, tiers: Sequence[Leased], pools: Sequence[L1Pool]
    ) -> set[str]:
        earlier: set[str] = set()
        for tier in tiers:
            earlier |= set(tier.earlier_holds())
        return {p.pool_id for p in pools} - earlier

    def stop_workers(self) -> None:
        if self.generation is not None:
            self.generation.stop()
        if self.parts is not None:
            self.parts.server.stop()
        if self.ops is not None:
            self.ops.stop()

    def close_exports(self) -> None:
        if self._fence_failed is not None:
            raise RuntimeError(
                "memory registration revocation failed"
            ) from self._fence_failed
        if self.parts is not None:
            self.parts.revoke()
        if self.extension is not None:
            deadline = time.monotonic() + EXTENSION_DRAIN_S
            try:
                while True:
                    if self.sweep_regions():
                        break
                    if time.monotonic() >= deadline:
                        raise RuntimeError("region extension still holds memory")
                    time.sleep(0.05)
            finally:
                if not self._extension_closed:
                    self.extension.close()
                    self._extension_closed = True
            self.extension = None
        if self.parts is not None:
            self.parts.close()

    def sweep_regions(self) -> bool:
        """Ask about fenced regions; False keeps mappings and RM holds alive."""
        with self._extension_lock:
            for pool_id, pool in list(self._regions.items()):
                if self.extension.retire(pool.region.handle.to_dict()):
                    del self._regions[pool_id]
            return not self._regions

    def connection_closed(self, conn: int) -> None:
        with self._extension_lock:
            self._hint_requests.pop(conn, None)
        callback = getattr(self.extension, "connection_closed", None)
        if callback is not None:
            try:
                callback(conn)
            except Exception:
                logger.debug("extension connection cleanup failed", exc_info=True)

    def _ranges(self, locations) -> list[dict]:
        ranges = []
        for location in locations:
            pool = self._regions.get(location.pool_id)
            page = self.run.pools.find(location)
            if pool is None or page is None or location.kind != pool.info().kind:
                raise ValueError("hint range is not current")
            if not (
                0 < location.length
                and page.index * pool.page_size <= location.offset
                and location.offset + location.length
                <= (page.index + 1) * pool.page_size
            ):
                raise ValueError("hint range is outside its page")
            ranges.append(
                {
                    "handle": pool.region.handle.to_dict(),
                    "offset": location.offset,
                    "length": location.length,
                }
            )
        return ranges

    def hint(self, conn, request, done, is_closed=lambda: False) -> None:
        from maru_common.storage_types import Status

        action = request.action
        if (
            action not in self.hint_actions
            or action not in region_extension.HINT_ACTIONS[:3]
        ):
            done(RemoteHintResponse(Status.REJECTED, error="hint action unsupported"))
            return
        added = False
        try:
            if not self.check_generation():
                raise ValueError("memory generation unavailable")
            if not request.request:
                raise ValueError("hint request is empty")
            if len(request.ranges) > MAX_HINT_RANGES:
                raise ValueError("too many hint ranges")
            with self._extension_lock:
                if is_closed():
                    return
                ranges = self._ranges(request.ranges)
                opened = self._hint_requests.setdefault(conn, set())
                if action == "read_hint":
                    if (
                        request.request not in opened
                        and len(opened)
                        >= self.run.config.region_extension.hint_requests_max
                    ):
                        raise ValueError("too many hint requests")
                    added = request.request not in opened
                    opened.add(request.request)
                elif action == "read_hint_end":
                    opened.discard(request.request)
            self.extension.hint(
                conn,
                {
                    "action": str(action),
                    "request": request.request,
                    "epoch": self.run.node_epoch,
                },
                ranges,
            )
        except Exception as exc:
            if added:
                with self._extension_lock:
                    self._hint_requests.get(conn, set()).discard(request.request)
            done(RemoteHintResponse(Status.REJECTED, error=str(exc)))
        else:
            done(RemoteHintResponse(Status.OK))

    def reserve_hint(self, conn, request, credits, is_closed=lambda: False) -> None:
        if is_closed():
            return
        if request.hint not in self.hint_actions:
            return
        if request.hint not in ("write_hint", "write_wait"):
            return
        if (
            request.hint == "write_wait"
            and not 0 < request.wait_ms <= MAX_WRITE_WAIT_MS
        ):
            return
        try:
            self.extension.hint(
                conn,
                {
                    "action": str(request.hint),
                    "request": f"{request.op.origin}:{request.op.seq}",
                    "epoch": self.run.node_epoch,
                    "timeout_ms": request.wait_ms,
                },
                self._ranges(credits),
            )
        except Exception:
            logger.debug("extension write hint failed", exc_info=True)

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
