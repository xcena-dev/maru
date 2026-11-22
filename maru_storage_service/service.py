# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Builds one node's store and runs its metadata and worker services."""

import logging
import mmap
import threading
import time
import uuid
from collections.abc import Callable, Sequence

from maru_common import gc_freeze
from maru_common.storage_types import (
    MEMORY_CAPABILITY,
    POOL_KINDS,
    CloseMode,
    PoolDescriptor,
    PoolInfo,
    run_holder,
)

from .config import (
    DEFAULT_HIGH_WATERMARK,
    DEFAULT_LOW_WATERMARK,
    NodeConfig,
    NodeConfigError,
    TierConfig,
)
from .config_fields import NotServableError as NotServableError
from .drivers.cxl import (
    DEFAULT_RENEW_S,
    OWN_HOLDER,
    CxlRegionPool,
    RegionSource,
)
from .drivers.dram import DramPoolError
from .drivers.registry import TIERS
from .local_pin import LocalPins
from .local_server import LocalServer
from .op_ledger import OP_WINDOW
from .peer_watcher import PeerWatcher
from .placement import DEFAULT_KEPT_BACKOFF_S, PlacementPolicy
from .pool_manager import PageState, PoolManager
from .ports import EpochChanged, ServerStopped
from .server_client import ServerClient
from .storage_manager import (
    DEFAULT_EVICT_INTERVAL_S,
    DEFAULT_GPU_DRAIN_GRACE_S,
    StorageManager,
)
from .tiers import L1Pool, L1Tier, Leased, Tier, TierEnv
from .transfer_engine import DEFAULT_TRANSFER_BYTES_PER_S, TransferEngine

logger = logging.getLogger(__name__)


# How long a leave waits for the workers to unmap the leased pools (CXL)
DEFAULT_UNMAP_TIMEOUT_S = 30.0
# Pause between a run that ended and the next one, doubled up to the max for
# runs that end soon after they start
NEW_RUN_PAUSE_S = 1.0
NEW_RUN_PAUSE_MAX_S = 60.0
# A run at least this long resets the pause
NEW_RUN_RESET_S = 300.0
# How long a stopped node waits for a run's teardown
STOP_WAIT_S = 30.0
# A DRAM pool that does not fit is tried again, this often (doubled up to the
# max), and logged this often
DRAM_RETRY_S = 1.0
DRAM_RETRY_MAX_S = 30.0
DRAM_LOG_S = 30.0


SERVED_TIERS = {"gpu": (["dram"], ["local_cxl"]), "memory": (["dram"], ["local_cxl"])}


class CapabilityRefusedError(RuntimeError):
    """MaruServer did not accept what the node needs to serve its role."""


def check_servable(config: NodeConfig) -> None:
    """Refuse configs that cannot be served in full."""
    stores = [t.store for t in config.tiers]
    if stores not in SERVED_TIERS.get(config.role, ()):
        served = ", ".join(
            f"[{', '.join(s)}]" for s in SERVED_TIERS.get(config.role, ())
        )
        raise NotServableError(
            f"serving the tiers {stores} is not implemented: only {served} are served"
        )
    if config.remote_server is not None:
        raise NotServableError("remote_server is not served")
    for t in config.tiers:
        TIERS[t.store].check_servable(t)


class StorageService:
    def __init__(
        self,
        config: NodeConfig,
        *,
        rpc_timeout_s: float = 2.0,
        gpu_drain_grace_s: float = DEFAULT_GPU_DRAIN_GRACE_S,
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
    ):
        """``map_region`` supplies the node's mapping of a CXL region."""
        check_servable(config)
        self.config = config
        self.node_epoch = node_epoch or uuid.uuid4().hex
        # This process's RM hold on its mapped regions
        self.own_holder = run_holder(OWN_HOLDER, self.node_epoch)
        self.rpc_timeout_s = rpc_timeout_s
        self.gpu_drain_grace_s = gpu_drain_grace_s
        self.evict_interval_s = evict_interval_s
        self.kept_backoff_s = kept_backoff_s
        self.host_boot_id = host_boot_id
        self.op_window = op_window
        self.region_source = region_source
        self.cxl_renew_s = cxl_renew_s
        self.unmap_timeout_s = unmap_timeout_s
        self.transfer_bytes_per_s = transfer_bytes_per_s
        self.map_region = map_region
        self.transfers: TransferEngine | None = None
        # The run's tiers by store name, in the order they started
        self._tiers: dict[str, Tier] = {}
        # Why the run ended on its own, and whether a new run follows
        self.ended: str | None = None
        self.new_run = False
        # An earlier run of this process may still have its pool mapped
        self.later_run = False
        # Why tearing the run down failed: nothing may run after it
        self.teardown_failed: str | None = None
        self._interrupted = threading.Event()
        self._finished = threading.Event()
        # One stop at a time, and never while a start is half done
        self._stop_lock = threading.Lock()
        self._lifecycle = threading.Lock()
        self.pools: PoolManager | None = None
        self.server: ServerClient | None = None
        self.manager: StorageManager | None = None
        self.local: LocalServer | None = None
        self.memory = config.role == "memory"
        # Pools of the run's tiers left out of it (never announced)
        self._unlisted: set[str] = set()
        # Pools taken over from the run before: adopted or claimed from the RM
        self.adopted: list[str] = []
        self._credit_tiers: set[str] = set()
        self._stopped = threading.Event()

    def tier(self, store: str) -> Tier | None:
        """The run's tier ``store``, once made."""
        return self._tiers.get(store)

    def capabilities(self) -> list[str]:
        """The capability required for this node's role."""
        return [MEMORY_CAPABILITY] if self.memory else []

    def _leases(self) -> list[Leased]:
        """The run's leased tiers; a tier's thread may ask during the start."""
        return [t for t in list(self._tiers.values()) if isinstance(t, Leased)]

    def _closing_order(self) -> list[Tier]:
        """How a teardown stops and closes the tiers: the leased ones first,
        then the others in the order they started."""
        tiers = list(self._tiers.values())
        return [t for t in tiers if isinstance(t, Leased)] + [
            t for t in tiers if not isinstance(t, Leased)
        ]

    @property
    def claimed_elsewhere(self) -> bool:
        """A lessor gave the node to another run: never claimed back."""
        return any(t.claimed_elsewhere for t in self._leases())

    def _env(self) -> TierEnv:
        return TierEnv(
            node_id=self.config.node_id,
            node_epoch=self.node_epoch,
            page_size=self.config.page_size,
            own_holder=self.own_holder,
            transfer_bytes_per_s=self.transfer_bytes_per_s,
            interrupted=self._interrupted,
            later_run=self.later_run,
            on_lost=self._lose_regions,
            renew_s=self.cxl_renew_s,
            region_source=self.region_source,
            map_region=self.map_region,
        )

    def _make(self, tier: TierConfig) -> Tier:
        """The tier object of ``tier``, made by its store's TierSpec; the run
        keeps it."""
        spec = TIERS[tier.store]
        assert spec.make is not None  # check_servable lets served stores only
        made = spec.make(tier, self._env())
        if spec.leased != isinstance(made, Leased):
            raise TypeError(
                f"store {tier.store}: its spec says leased={spec.leased}, "
                f"but it made a {type(made).__name__}"
            )
        self._tiers[tier.store] = made
        return made

    def start(self) -> None:
        """Bring the node up; on failure everything started is undone.
        ServerStopped if stopped meanwhile, or if the run ended during the
        start (``ended``)."""
        try:
            with self._lifecycle:
                self._start()
        except EpochChanged as e:
            self._end_run(
                f"MaruServer no longer knows the session ({e})",
                CloseMode.RESTART,
                new_run=True,
            )
            raise ServerStopped(str(e)) from e
        except CapabilityRefusedError:
            self.stop(CloseMode.LEAVE)  # it never serves: gone, not back soon
            raise
        except BaseException:
            self.stop(CloseMode.RESTART)
            raise

    def _check_stopped(self) -> None:
        """After a blocking step: a stop asked meanwhile ends the start."""
        if self._stopped.is_set():
            raise ServerStopped()

    def _claim_socket(self) -> None:
        """The workers' socket, claimed first: a second start must fail
        before NODE_OPEN."""
        config = self.config
        self.local = LocalServer(
            config.socket,
            None,
            node_id=config.node_id,
            node_epoch=self.node_epoch,
            server_address=config.server,
            page_size=config.page_size,
            budgets=config.budgets,  # the workers' are set before it serves
            pools=[],
            transfer_defaults=config.transfer_defaults(),
            socket_group=config.socket_group,
            capabilities=frozenset(),
        )
        self.local.claim()

    def _start(self) -> None:
        config = self.config
        top = config.top
        size = top.size
        high = (
            top.high_watermark
            if top.high_watermark is not None
            else DEFAULT_HIGH_WATERMARK
        )
        low = (
            top.low_watermark
            if top.low_watermark is not None
            else DEFAULT_LOW_WATERMARK
        )
        if size is None:
            raise NodeConfigError(f"the {top.store} tier needs a size")
        if self._interrupted.is_set():
            raise ServerStopped()
        if not self.memory:
            self._claim_socket()
        for t in config.tiers:
            if not TIERS[t.store].leased:
                self._open(self._make(t))
        # Then the leased ones: only regions this run owns are announced
        for t in config.tiers:
            if TIERS[t.store].leased:
                leased = self._make(t)
                assert isinstance(leased, Leased)
                leased.open()
                leased.claim()
                if self._interrupted.is_set():
                    raise ServerStopped()
                leased.start_renewing()
                self._check_stopped()
        # In the order they started: the order pools are announced and logged
        tiers = self._tiers
        traits = config.traits()
        self.pools = PoolManager(config.budgets, config.page_size)
        self._credit_tiers = {name for name, t in traits.items() if t.credit}
        # A pool that ends with its run (DRAM) is never adopted
        for t in config.tiers:
            for info in self._listed(tiers[t.store]):
                if POOL_KINDS[info.kind].run_scoped:
                    self.pools.add_pool(
                        info, creditable=t.store in self._credit_tiers, tier=t.store
                    )
        self.server = ServerClient(
            config.server,
            config.node_id,
            self.node_epoch,
            rpc_timeout_s=self.rpc_timeout_s,
            host_boot_id=self.host_boot_id,
        )
        if self._interrupted.is_set():  # before interrupt() could stop it
            raise ServerStopped()
        placement = PlacementPolicy(high, low, kept_backoff_s=self.kept_backoff_s)
        for t in config.tiers[1:]:
            if t.high_watermark is not None and t.low_watermark is not None:
                placement.set_watermarks(t.store, t.high_watermark, t.low_watermark)
        self.manager = StorageManager(
            node_epoch=self.node_epoch,
            pool_manager=self.pools,
            placement=placement,
            local_pins=LocalPins(),
            metadata=self.server,
            watcher=PeerWatcher(),
            gpu_drain_grace_s=self.gpu_drain_grace_s,
            evict_interval_s=self.evict_interval_s,
            op_window=self.op_window,
            traits=traits,
            on_worker_settled=self._worker_settled,
            on_end=lambda reason: self._end_run(
                reason, CloseMode.RESTART, new_run=True
            ),
        )
        leases = [t for t in tiers.values() if isinstance(t, Leased)]
        # The tier of each pool this run opens: adopted ones are among them
        announced = [(n, i) for n, t in tiers.items() for i in self._listed(t)]
        pool_tiers = {info.pool_id: name for name, info in announced}
        rm_domains = [d for t in leases for d in t.rm_domains()]
        opened = self.server.open(
            [info for _, info in announced],
            rm_domains=rm_domains,
            capabilities=self.capabilities(),
        )
        self._check_stopped()
        refused = set(self.capabilities()) - set(opened.capabilities)
        if refused and self.memory:
            raise CapabilityRefusedError(
                f"MaruServer {config.server} does not accept {sorted(refused)}: "
                f"it cannot serve a {config.role} node"
            )
        self.pools.set_epoch_seq(opened.epoch_seq)
        adopted_ids = {info.pool_id for info in opened.adopted}
        for info in opened.adopted:
            # Committed pages are READY; the rest wait for earlier holds.
            name = pool_tiers.get(info.pool_id)
            if name is None:
                raise RuntimeError(f"MaruServer adopted {info.pool_id}, not opened")
            self.pools.add_pool(
                info, creditable=False, state=PageState.QUARANTINED, tier=name
            )
            self.pools.quarantine_pool(info.pool_id)
            items = self.server.recover_list(info.pool_id)
            refs = self.pools.recovered(
                info.pool_id, [(i.key, i.location) for i in items]
            )
            for ref in refs:
                placement.on_ready(ref)
            self.adopted.append(info.pool_id)
            logger.info("adopted pool %s: %d committed pages", info.pool_id, len(refs))
        # Claimed pools without Catalog entries start quarantined.
        for t in config.tiers:
            tier = tiers[t.store]
            for info in self._listed(tier):
                if info.pool_id in adopted_ids or POOL_KINDS[info.kind].run_scoped:
                    continue
                if isinstance(tier, Leased) and tier.taken_over(info.pool_id):
                    # The run before may have written it: quarantined as an
                    # adopted pool, with no committed page
                    self.pools.add_pool(
                        info,
                        creditable=False,
                        state=PageState.QUARANTINED,
                        tier=t.store,
                    )
                    self.pools.quarantine_pool(info.pool_id)
                    self.adopted.append(info.pool_id)
                else:
                    self.pools.add_pool(
                        info, creditable=t.store in self._credit_tiers, tier=t.store
                    )
        self.manager.start()
        # Quarantined while an earlier run holds the region in the RM
        earlier: dict[str, set[str]] = {}
        for lease in leases:
            earlier.update(lease.earlier_holds())
        for pool_id in self.adopted:
            if pool_id in earlier:
                logger.info(
                    "pool %s stays quarantined while earlier runs hold it: %s",
                    pool_id,
                    sorted(earlier[pool_id]),
                )
            else:
                self._end_quarantine(pool_id)
        for lease in leases:
            lease.watch_holds(self._end_quarantine)
        self.manager.start_heartbeat()
        if not self.memory:
            self._serve_workers()
        logger.info(
            "StorageService %s serving %s node %s, pages of %d KiB",
            self.node_epoch,
            config.role,
            config.node_id,
            config.page_size >> 10,
        )
        for name, tier in tiers.items():
            if tier.pool_infos():
                logger.info("%s tier: %s", name, tier.describe(adopted_ids))

    def _serve_workers(self) -> None:
        """Workers attach from now: they map the L1 pools."""
        assert self.local is not None and self.manager is not None
        self.local.manager = self.manager
        # memfd indexes the fds sent with ATTACH
        attach: list[tuple[PoolDescriptor, int | None]] = []
        for tier in self._tiers.values():
            if isinstance(tier, L1Tier):
                for pool in tier.pools:
                    fd = pool.worker_fd()
                    memfd = (
                        None if fd is None else sum(f is not None for _, f in attach)
                    )
                    attach.append((pool.descriptor(memfd=memfd), fd))
        self.local.pools = attach
        self.local.start()

    def _listed(self, tier: Tier) -> list[PoolInfo]:
        """The pools opened by the store."""
        return tier.pool_infos()

    def _open(self, tier: Tier) -> None:
        """Open ``tier``. A DRAM pool that ran short of memory may fit once
        workers unmap an earlier run's pool: tried again with a backoff until
        it fits or the run is stopped, as is any DRAM failure of a later run
        in this process."""
        pause, logged = DRAM_RETRY_S, None
        while True:
            try:
                tier.open()
                return
            except DramPoolError as e:
                if not (e.short or self.later_run):
                    raise
                now = time.monotonic()
                if logged is None or now - logged >= DRAM_LOG_S:
                    logged = now
                    logger.warning(
                        "cannot allocate the dram pool yet; workers may still map "
                        "the pool of an earlier run. Trying again: %s",
                        e,
                    )
            if self._interrupted.wait(pause):
                raise ServerStopped("stopped while waiting for the dram pool")
            pause = min(pause * 2, DRAM_RETRY_MAX_S)

    def _map_for_transfers(self, engine: TransferEngine) -> None:
        """Keep node mappings until the leased pools can be released."""
        tiers = self._tiers.values()
        for tier in tiers:
            if isinstance(tier, L1Tier):
                for pool in tier.pools:
                    mapping = tier.map_for_transfers(pool)
                    if mapping is not None:
                        engine.add_pool(pool.pool_id, mapping)

    def _end_quarantine(self, pool_id: str) -> None:
        """No earlier run holds the pool: its quarantined pages are free, and
        it gives credit if its tier does and MaruServer did not call it
        invalid. Never after a stop began."""
        assert self.pools is not None and self.manager is not None
        with self._stop_lock:
            if self._stopped.is_set():
                return
            tier = self.pools.tier_of(pool_id)
            if tier is None:  # left out of the run
                return
            creditable = tier in self._credit_tiers and not self.manager.is_invalid(
                pool_id
            )
            freed = self.pools.end_pool_quarantine(pool_id, creditable)
        logger.info(
            "pool %s: no earlier run holds it; %d quarantined pages are free",
            pool_id,
            freed,
        )

    def _worker_settled(self, incarnation: str) -> None:
        """Its GPU drain grace is over: its holds on leased pools can go."""
        for lease in self._leases():
            lease.worker_settled(incarnation)

    def interrupt(self) -> None:
        """Stop a start still waiting for the RM or MaruServer."""
        self._interrupted.set()
        if self.server is not None:
            self.server.stop()

    def _lose_regions(self, reason: str) -> None:
        """A lease may end: stop with leave so no location keeps pointing at
        the leased pools and no worker copies into them. A new run follows
        unless another run claimed the node."""
        self._end_run(reason, CloseMode.LEAVE, new_run=not self.claimed_elsewhere)

    def _end_run(self, reason: str, mode: CloseMode, new_run: bool) -> None:
        """The run ends on its own: stop with ``mode`` on a thread of its own
        (the caller may be a thread the stop joins). The first reason wins;
        none after a stop began."""
        with self._stop_lock:
            if self.ended is not None or self._stopped.is_set():
                return
            self.ended, self.new_run = reason, new_run
        logger.warning("run %s ends (%s): %s", self.node_epoch, mode.value, reason)
        threading.Thread(
            target=self.stop, args=(mode,), name="maru-run-end", daemon=True
        ).start()

    def stop(self, mode: CloseMode = CloseMode.RESTART) -> None:
        """Stop serving, then NODE_CLOSE (restart: back soon; leave: gone)."""
        with self._stop_lock:
            if self._stopped.is_set():
                return
            self._stopped.set()
        self.interrupt()
        try:
            with self._lifecycle:  # a start still running gives up first
                self._teardown(mode)
        except Exception as e:
            self.teardown_failed = f"{type(e).__name__}: {e}"
            logger.exception("tearing run %s down failed", self.node_epoch)
        finally:
            self._finished.set()

    def _teardown(self, mode: CloseMode) -> None:
        tiers = self._closing_order()
        leases = self._leases()
        for tier in tiers:
            tier.stop()
        # On leave workers unmap these before they go back
        unmapped = [
            t
            for t in tiers
            if isinstance(t, L1Tier) and isinstance(t, Leased) and t.unmap_on_leave
        ]
        if unmapped and mode == CloseMode.LEAVE:
            gone = self._unmap_leased(unmapped, [p for t in unmapped for p in t.pools])
            for tier in unmapped:
                still = {p.pool_id for p in tier.pools} - gone
                if still:
                    # Their holds keep them out of reuse in the RM until unmapped
                    logger.warning(
                        "%s regions still mapped by a worker: %s",
                        tier.name,
                        sorted(still),
                    )
        if self.local is not None:
            self.local.stop()
        if self.server is not None:
            self.server.stop()
        if self.manager is not None:
            self.manager.stop()
            stats = self.manager.stats()
            logger.info(
                "this run's retire results: %s; credit short bytes: %s",
                stats["retire_results"],
                stats["credit_short_bytes"],
            )
        kept: set[str] = set()
        if self.transfers is not None:
            kept = self.transfers.close()  # unmapped before the regions may go back
            if kept:
                logger.error(
                    "pools %s stay mapped: their hold and regions stay", sorted(kept)
                )
            else:
                for lease in leases:
                    lease.after_transfers_closed()
        if self.server is not None:
            if self.server.close_session(mode):
                logger.info("node session closed (%s)", mode.value)
            self.server.close()
        if mode == CloseMode.LEAVE and not kept:
            for lease in leases:
                lease.release()
        for tier in tiers:
            tier.close()

    def _unmap_leased(
        self, tiers: Sequence[Leased], pools: Sequence[L1Pool]
    ) -> set[str]:
        """Of the leased tiers' ``pools``, those no worker can map any more,
        before they go back."""
        # A pool an earlier run still holds stays held after this
        earlier: set[str] = set()
        for tier in tiers:
            earlier |= set(tier.earlier_holds())
        if self.manager is None or self.local is None or not self.local.started:
            # No worker of this run mapped them
            return {p.pool_id for p in pools} - earlier
        # A new ATTACH maps none of them: its record exists before its reply;
        # and no credit comes from them any more
        ids = {p.pool_id for p in pools}
        self.local.pools = [
            (d, fd) for d, fd in self.local.pools if d.pool.pool_id not in ids
        ]
        for pool in pools:
            self.pools.disable_pool(pool.pool_id)
        timeout = self.unmap_timeout_s
        if self.ended is not None:  # the lessor may free them once the lease ends
            timeout = min([timeout, *(t.time_left() / 2 for t in tiers)])
        done = self.manager.unmap_pools(
            [(p.pool_id, p.pool_generation) for p in pools], max(0.0, timeout)
        )
        return done - earlier

    def wait(self, timeout: float | None = None) -> bool:
        """Whether stopping has begun."""
        return self._stopped.wait(timeout)

    def wait_finished(self, timeout: float | None = None) -> bool:
        """Whether a stop has run to its end."""
        return self._finished.wait(timeout)


class Node:
    """Runs of the node's StorageService, one after another in this process:
    a run that ends on its own and asks for a new one is followed by a new
    run with a new node_epoch, after a pause that grows while runs end soon
    after they start. Workers re-attach to it as after a process restart."""

    def __init__(
        self,
        config: NodeConfig,
        make_service: Callable[[NodeConfig], StorageService] = StorageService,
        pause_s: float = NEW_RUN_PAUSE_S,
        max_pause_s: float = NEW_RUN_PAUSE_MAX_S,
    ):
        check_servable(config)
        self.config = config
        self._make_service = make_service
        self.pause_s = pause_s
        self.max_pause_s = max_pause_s
        self._stop = threading.Event()
        self.service: StorageService | None = None
        self.runs = 0
        # Why the last run ended on its own
        self.ended: str | None = None
        # A run's teardown failed: no run follows, the process must exit
        self.failed: str | None = None

    def stop(self) -> None:
        """End the current run with restart and start no other; any thread,
        a signal handler too (no lock)."""
        self._stop.set()
        service = self.service
        if service is not None:
            service.interrupt()

    def run(self) -> bool:
        """Serve until stop() (True), or until no run may follow (False):
        another run claimed the node, or a teardown failed (``failed``). A
        start that fails otherwise raises."""
        pause = self.pause_s
        while True:
            if self._stop.is_set():
                return True
            service = self._make_service(self.config)
            service.later_run = self.runs > 0
            self.service = service
            self.runs += 1
            if self._stop.is_set():  # stop() may have missed it
                service.interrupt()
            started = time.monotonic()
            try:
                service.start()
            except ServerStopped:
                pass  # stopped, or the run ended during its start
            except Exception as e:
                if self._claimed_elsewhere(service, str(e)):
                    return False
                if service.teardown_failed is not None:
                    self.failed = service.teardown_failed
                raise
            while not self._stop.wait(0.1) and not service.wait(0):
                pass
            if service.ended is None:
                # Stopped: torn down on a thread, waited for up to STOP_WAIT_S
                threading.Thread(
                    target=service.stop,
                    args=(CloseMode.RESTART,),
                    name="maru-run-stop",
                    daemon=True,
                ).start()
            if not self._finished(service):
                return True
            if service.teardown_failed is not None:
                self.failed = service.teardown_failed
                logger.error("no new run: the teardown failed (%s)", self.failed)
                return False
            if service.ended is None:
                return True
            self.ended = service.ended
            if not service.new_run or self._claimed_elsewhere(service, service.ended):
                return False
            if self._stop.is_set():
                return True
            if time.monotonic() - started >= NEW_RUN_RESET_S:
                pause = self.pause_s
            logger.warning(
                "starting a new run in %.1f s after: %s", pause, service.ended
            )
            # The ended run's objects: its cycles were frozen while it ran
            self.service = None
            del service
            gc_freeze.reclaim()
            if self._stop.wait(pause):
                return True
            pause = min(pause * 2, self.max_pause_s)

    def _finished(self, service: StorageService) -> bool:
        """Wait for the run's teardown; once stopped, for STOP_WAIT_S more."""
        deadline = None
        while not service.wait_finished(0.1):
            if self._stop.is_set():
                deadline = deadline or time.monotonic() + STOP_WAIT_S
                if time.monotonic() >= deadline:
                    logger.error("the run's teardown did not end; stopping anyway")
                    return False
        return True

    def _claimed_elsewhere(self, service: StorageService, reason: str) -> bool:
        """Another run claimed the node in the RM: never claim it back."""
        if not service.claimed_elsewhere:
            return False
        self.ended = f"another run claimed the node ({reason})"
        return True
