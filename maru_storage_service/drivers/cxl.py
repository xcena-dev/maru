# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""CXL driver: the regions this node owns in the RM, as pools of the local_cxl tier.

The node, not a process, owns its regions (spec 4.4). A new run claims them
back with its node_epoch before NODE_OPEN, renews the lease while it runs and
gives them back on leave. A run that cannot renew for half the lease, or whose
lease the RM says ended, must let go before the RM may give the regions to
another node, and so must one whose node another run claimed. A claimed region
that an earlier run still holds in the RM (a worker's or the node's own mapping)
is watched with NODE_HOLDS until those holds are gone.
"""

import logging
import mmap
import os
import platform
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from maru_common.node_protocol import process_identity, process_start_time
from maru_common.storage_types import (
    MAX_BATCH,
    DaxMapping,
    PoolDescriptor,
    PoolInfo,
    PoolKind,
    RmDomain,
    Visibility,
    run_holder,
)
from maru_shm.client import MaruShmClient
from maru_shm.ipc import GetAccessResp
from maru_shm.types import (
    MaruHandle,
    NodeClaim,
    NodeLeaseExpiredError,
    NodeOwnershipUnsupportedError,
    NodeRegion,
    RetryableNodeError,
    StaleNodeEpochError,
)
from maru_shm.unholds import Unholds

from ..config_fields import (
    NodeConfigError,
    NotServableError,
    TierConfig,
    TierContext,
    TierSpec,
    _str,
    parse_size,
)
from ..tiers import L1Pool, L1Tier, Leased, Level, TierEnv
from . import PoolAccess

logger = logging.getLogger(__name__)

DEFAULT_RENEW_S = 10.0
# How long stop() waits for each of its threads
STOP_JOIN_S = 1.0
# How often NODE_HOLDS is asked while a region waits for an earlier run, and
# how often the holds it waits for are logged
HOLDS_POLL_S = 1.0
HOLDS_LOG_S = 30.0
# Holder name of the StorageService's own holds (run_holder)
OWN_HOLDER = "ss"
DEFAULT_RM = "127.0.0.1:9850"
DEFAULT_REGION_SIZE = 1 << 30  # Lower bound for the default.

# Holds the node failed to drop, sent again until the RM confirms them; for
# the process, so they outlive the run that queued them
UNHOLDS = Unholds()


class CxlTierError(RuntimeError):
    """The local_cxl tier could not be set up."""


class RegionSource(Protocol):
    """The RM's node requests (MaruShmClient)."""

    def node_claim(self, node_id: str, node_epoch: str) -> NodeClaim: ...

    def node_alloc(
        self, node_id: str, node_epoch: str, size: int, dax_path: str = ""
    ) -> NodeRegion: ...

    def node_free(self, node_id: str, node_epoch: str, handle: MaruHandle) -> None: ...

    def node_renew(self, node_id: str, node_epoch: str) -> float: ...

    def node_release(self, node_id: str, node_epoch: str) -> int: ...

    def node_hold(
        self,
        node_id: str,
        node_epoch: str,
        handle: MaruHandle,
        holder: str,
        *,
        boot_id: str,
        pid: int,
        pid_start: int,
        pid_ns: int,
    ) -> GetAccessResp: ...

    def node_holds(self, node_id: str, node_epoch: str) -> list[NodeRegion]: ...

    def unhold(self, handle: MaruHandle, holder: str, instance: str) -> None: ...


def _alive(pid: int, pid_start: int) -> bool:
    """Whether the process (pid, start time) still runs here."""
    try:
        return process_start_time(pid) == pid_start
    except (OSError, ValueError, IndexError):
        return False


def cxl_pool_id(rm_domain: str, region_id: int) -> str:
    return f"cxl:{rm_domain}:{region_id}"


def _ceil_div(value: int, divisor: int) -> int:
    return (value + divisor - 1) // divisor


@dataclass
class CxlRegionPool:
    """One usable RM region is one pool in the run's NODE_OPEN list."""

    node_id: str
    rm_domain: str
    region: NodeRegion
    page_size: int
    visibility: Visibility
    rm_address: str = ""
    # This run: a worker's hold names it, so the RM refuses an older run's
    node_epoch: str = ""
    # Claimed from the run before (it may hold its pages); else allocated now
    claimed: bool = False
    pool_generation: int = 1

    @property
    def access(self) -> PoolAccess:
        return PoolAccess.MAPPED

    @property
    def pool_id(self) -> str:
        return cxl_pool_id(self.rm_domain, self.region.handle.region_id)

    @property
    def capacity(self) -> int:
        return self.region.handle.length // self.page_size * self.page_size

    def info(self) -> PoolInfo:
        return PoolInfo(
            pool_id=self.pool_id,
            kind=PoolKind.CXL,
            node_id=self.node_id,
            pool_generation=self.pool_generation,
            capacity=self.capacity,
            page_size=self.page_size,
            rm_domain=self.rm_domain,
            region_id=self.region.handle.region_id,
            device_id=self.region.device_uuid or None,
            visibility=self.visibility,
        )

    def descriptor(self, memfd: int | None = None) -> PoolDescriptor:
        return PoolDescriptor(
            pool=self.info(),
            dax=DaxMapping(
                self.rm_domain,
                self.region.handle,
                self.rm_address,
                node_id=self.node_id,
                node_epoch=self.node_epoch,
            ),
        )

    def worker_fd(self) -> int | None:
        return None

    def close(self) -> None:
        """Nothing to undo: the region stays the node's in the RM."""


def map_region(source: RegionSource, pool: CxlRegionPool, holder: str) -> mmap.mmap:
    """The region's bytes for the node's own copies, held in the RM by
    ``holder`` (this process in this run) with NODE_HOLD, which also checks
    the handle's token. The caller drops the hold after unmapping."""
    handle = pool.region.handle
    boot_id, pid, pid_start, pid_ns = process_identity()
    access = source.node_hold(
        pool.node_id,
        pool.node_epoch,
        handle,
        holder,
        boot_id=boot_id,
        pid=pid,
        pid_start=pid_start,
        pid_ns=pid_ns,
    )
    try:
        fd = os.open(access.dax_path, os.O_RDWR)
        try:
            return mmap.mmap(
                fd,
                handle.length,
                flags=mmap.MAP_SHARED,
                prot=mmap.PROT_READ | mmap.PROT_WRITE,
                offset=handle.offset,
            )
        finally:
            os.close(fd)
    except BaseException:
        source.unhold(handle, holder, holder)  # nothing maps it
        raise


class CxlTier(L1Tier, Leased):
    """The node's regions in one RM: claimed at start, renewed while the run
    lasts, given back on leave. ``on_lost`` is called once, from the renew
    thread, when the run must let go of them. ``source`` None: a client of
    the RM at ``tier.rm``, closed by close(). The node maps a region for its
    own copies held by ``own_holder``, or with ``mapper``."""

    name = "local_cxl"

    def __init__(
        self,
        source: RegionSource | None,
        tier: TierConfig,
        *,
        node_id: str,
        node_epoch: str,
        page_size: int,
        on_lost: Callable[[str], None],
        renew_s: float = DEFAULT_RENEW_S,
        clock: Callable[[], float] = time.monotonic,
        interrupted: threading.Event | None = None,
        own_holder: str | None = None,
        mapper: Callable[[CxlRegionPool], mmap.mmap] | None = None,
    ):
        if tier.size is None or tier.region_size is None:
            raise CxlTierError("the local_cxl tier needs size and region_size")
        self._client: MaruShmClient | None = None
        if source is None:
            source = self._client = MaruShmClient(address=tier.rm)
        self.source = source
        self.tier = tier
        self.node_id = node_id
        self.node_epoch = node_epoch
        self.page_size = page_size
        self.renew_s = renew_s
        self._on_lost = on_lost
        self._clock = clock
        self._interrupted = interrupted or threading.Event()
        self.own_holder = own_holder or run_holder(OWN_HOLDER, node_epoch)
        self._mapper = mapper
        self.rm_domain: str | None = None
        self.lease_s = 0.0
        self.pools: list[CxlRegionPool] = []
        self._renewed_at = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._watcher: threading.Thread | None = None
        self.lost: str | None = None
        # The RM says another run of the node claimed it: this process must
        # not claim it back
        self.claimed_elsewhere = False
        # Claimed pool -> names of the earlier runs' holds on it, as last
        # listed (by NODE_CLAIM, then NODE_HOLDS); a pool without any is gone
        self._lock = threading.Lock()
        self._earlier: dict[str, set[str]] = {}

    def claim(
        self, stopped: threading.Event | None = None, retry_s: float = 1.0
    ) -> None:
        """Take the node's regions back, then allocate up to the tier size.
        Retries until the RM answers: no pool is announced that this run has
        not claimed. Returns early, with no pools, once ``stopped`` (by
        default ``interrupted``) is set."""
        if stopped is None:
            stopped = self._interrupted
        claim = None
        while claim is None and not stopped.is_set():
            sent = self._clock()  # the RM's lease restarts no earlier than this
            try:
                claim = self.source.node_claim(self.node_id, self.node_epoch)
            except (OSError, ConnectionError, TimeoutError, RetryableNodeError) as e:
                logger.warning(
                    "RM %s not answering NODE_CLAIM (%s); retrying", self.tier.rm, e
                )
                stopped.wait(retry_s)
            except RuntimeError as e:  # another host's node, an old RM
                raise CxlTierError(f"RM {self.tier.rm}: {e}") from e
        if claim is None:
            return
        self._renewed_at = sent
        self.rm_domain = claim.rm_domain
        self.lease_s = claim.lease_s
        # A region on a device the RM does not serve now has no path, and one
        # smaller than a (new) page holds none: still the node's, not used
        claimed_pools = [
            self._pool(r, claimed=True)
            for r in claim.regions
            if r.dax_path and r.handle.length >= self.page_size
        ]
        claimed = len(claim.regions)
        unusable = claimed - len(claimed_pools)
        if unusable:
            logger.warning(
                "local_cxl tier: %d regions are on devices the RM does not serve now or "
                "smaller than a page; kept, not used",
                unusable,
            )
        size, region_size = self.tier.size, self.tier.region_size
        assert size is not None and region_size is not None
        total = sum(p.capacity for p in claimed_pools)
        additional = _ceil_div(max(0, size - total), region_size)
        if claimed + additional > MAX_BATCH:
            raise CxlTierError(
                f"local_cxl: {claimed} claimed regions and {additional} more needed "
                f"exceed the node limit of {MAX_BATCH}"
            )
        self.pools = claimed_pools
        with self._lock:
            for pool in self.pools:
                held = self._earlier_holds(pool.region) - self._drop_ended_own(pool)
                if held:
                    self._earlier[pool.pool_id] = held
        if self._earlier:
            self._check_node_holds()
        while total < size and not stopped.is_set():
            sent = self._clock()
            try:
                region = self.source.node_alloc(
                    self.node_id, self.node_epoch, region_size
                )
            except StaleNodeEpochError as e:
                if not isinstance(e, NodeLeaseExpiredError):
                    self.claimed_elsewhere = True
                raise CxlTierError(
                    f"RM {self.tier.rm}: this run no longer owns the node: {e}"
                ) from e
            except (OSError, RuntimeError) as e:
                logger.warning(
                    "local_cxl tier: the RM gave %d of %d bytes; going on with them (%s)",
                    total,
                    size,
                    e,
                )
                break
            self._renewed_at = sent
            pool = self._pool(region, claimed=False)
            self.pools.append(pool)
            total += pool.capacity
        if total > size:
            logger.info(
                "local_cxl tier: %d bytes claimed, more than its size %d", total, size
            )

    def _pool(self, region: NodeRegion, claimed: bool) -> CxlRegionPool:
        assert self.rm_domain is not None and self.tier.visibility is not None
        return CxlRegionPool(
            node_id=self.node_id,
            rm_domain=self.rm_domain,
            region=region,
            page_size=self.page_size,
            visibility=self.tier.visibility,
            rm_address=self.tier.rm or "",
            node_epoch=self.node_epoch,
            claimed=claimed,
        )

    # ------------------------------------------------------------ the lease

    def start_renewing(self) -> None:
        self._thread = threading.Thread(
            target=self._renew_loop, name="maru-cxl-lease", daemon=True
        )
        self._thread.start()

    def renew_interval(self) -> float:
        """Several renewals per half lease, however short the lease."""
        return min(self.renew_s, self.lease_s / 4) if self.lease_s else self.renew_s

    def time_left(self) -> float:
        """Seconds until the RM may free the regions, by this run's clock."""
        return self.lease_s - (self._clock() - self._renewed_at)

    def _renew_loop(self) -> None:
        while not self._stop.wait(self.renew_interval()):
            reason = self.renew_once()
            if reason is not None:
                self.lost = reason
                logger.error("local_cxl tier: letting go of the regions: %s", reason)
                self._on_lost(reason)
                return

    def renew_once(self) -> str | None:
        """Renew the lease; why the run must let go, or None."""
        sent = self._clock()
        try:
            self.lease_s = self.source.node_renew(self.node_id, self.node_epoch)
            self._renewed_at = sent
            return None
        except NodeLeaseExpiredError:
            return "the RM no longer counts this run as the owner (its lease ended)"
        except StaleNodeEpochError:
            self.claimed_elsewhere = True
            return (
                "the RM no longer counts this run as the owner (another run "
                "claimed the node)"
            )
        except (OSError, ConnectionError, TimeoutError, RuntimeError) as e:
            silent = self._clock() - self._renewed_at
            if silent >= self.lease_s / 2:
                return f"no lease renewal for {silent:.0f} s ({e})"
            logger.warning("local_cxl tier: lease renewal failed (%s); retrying", e)
            return None

    def stop(self, timeout: float | None = STOP_JOIN_S) -> None:
        """Stop renewing the lease and watching holds. A request under way may
        outlast ``timeout``; it changes nothing."""
        self._stop.set()
        for thread in (self._thread, self._watcher):
            if thread is not None and thread is not threading.current_thread():
                thread.join(timeout)

    # ------------------------------------------------------------- the holds

    def _earlier_holds(self, region: NodeRegion) -> set[str]:
        """Holds of another run of this node (a remote agent's names none)."""
        return {
            h.holder
            for h in region.holders
            if h.node_epoch and h.node_epoch != self.node_epoch
        }

    def _drop_ended_own(self, pool: CxlRegionPool) -> set[str]:
        """Drop the holds of earlier StorageService runs whose process is
        this one (that run's teardown unmapped) or is gone, judged as the RM
        judges a holder, from this host, boot and pid namespace: the RM cannot
        judge one in another pid namespace. Returns their names."""
        boot, pid, start, pid_ns = process_identity()
        dropped = set()
        for h in pool.region.holders:
            if (
                h.holder.partition("@")[0] != OWN_HOLDER
                or not h.node_epoch
                or h.node_epoch == self.node_epoch
                or not (boot and pid_ns and h.pid and h.pid_start)
                or (h.host, h.boot_id, h.pid_ns) != (platform.node(), boot, pid_ns)
            ):
                continue
            if (h.pid, h.pid_start) != (pid, start) and _alive(h.pid, h.pid_start):
                continue
            logger.info(
                "local_cxl tier: dropping %s's hold on %s: its run is over",
                h.holder,
                pool.pool_id,
            )
            self._drop(pool, h.holder)
            dropped.add(h.holder)
        return dropped

    def _check_node_holds(self) -> None:
        """An RM without NODE_HOLDS would keep the held regions quarantined
        for good: refuse to start."""
        try:
            self.source.node_holds(self.node_id, self.node_epoch)
        except NodeOwnershipUnsupportedError as e:
            raise CxlTierError(
                f"RM {self.tier.rm} does not answer NODE_HOLDS, needed while an "
                f"earlier run holds a region; upgrade the RM ({e})"
            ) from e
        except Exception as e:  # the watch asks again
            logger.warning("local_cxl tier: NODE_HOLDS failed (%s)", e)

    def earlier_holds(self) -> dict[str, set[str]]:
        """Claimed pools an earlier run still holds, with those holders."""
        with self._lock:
            return {pool_id: set(held) for pool_id, held in self._earlier.items()}

    def watch_holds(
        self, on_clear: Callable[[str], None], interval_s: float = HOLDS_POLL_S
    ) -> None:
        """Ask NODE_HOLDS every ``interval_s`` while a pool waits for an
        earlier run's holds; ``on_clear(pool_id)``, from the watch thread,
        once a pool has none left."""
        if not self.earlier_holds():
            return
        self._watcher = threading.Thread(
            target=self._watch_loop,
            args=(on_clear, interval_s),
            name="maru-cxl-holds",
            daemon=True,
        )
        self._watcher.start()

    def _watch_loop(self, on_clear: Callable[[str], None], interval_s: float) -> None:
        failing = False
        logged = self._clock()
        while not self._stop.wait(interval_s):
            if self._clock() - logged >= HOLDS_LOG_S:
                logged = self._clock()
                self._log_waiting()
            try:
                regions = self.source.node_holds(self.node_id, self.node_epoch)
            except StaleNodeEpochError:
                return  # the run is over; the lease thread lets go
            except Exception as e:
                if not failing:
                    logger.warning(
                        "local_cxl tier: NODE_HOLDS failed (%s); retrying", e
                    )
                failing = True
                continue
            failing = False
            assert self.rm_domain is not None
            listed = {
                cxl_pool_id(self.rm_domain, r.handle.region_id): r for r in regions
            }
            cleared, gone = [], []
            with self._lock:
                for pool_id in list(self._earlier):
                    region = listed.get(pool_id)
                    if region is None:
                        # Not the node's any more: never reused, not watched
                        del self._earlier[pool_id]
                        gone.append(pool_id)
                        continue
                    held = self._earlier_holds(region)
                    if held:
                        self._earlier[pool_id] = held
                    else:
                        del self._earlier[pool_id]
                        cleared.append(pool_id)
                done = not self._earlier
            for pool_id in gone:
                logger.warning(
                    "local_cxl tier: %s is no longer the node's in the RM; it stays "
                    "quarantined",
                    pool_id,
                )
            for pool_id in cleared:
                if self._stop.is_set():
                    return
                on_clear(pool_id)
            if done:
                return

    def _log_waiting(self) -> None:
        held = self.earlier_holds()
        if not held:
            return
        logger.warning(
            "local_cxl tier: still quarantined while earlier runs hold them: %s. The "
            "RM drops a hold once it sees its process gone, but not one of "
            "another host or pid namespace (a worker or StorageService in a "
            "container): once that process is surely gone, run "
            "'maru-rm-ctl drop-holds <holder> --confirm'",
            {pool_id: sorted(names) for pool_id, names in sorted(held.items())},
        )

    def unhold(self, holder: str) -> None:
        """Drop ``holder``'s hold on every region; one that fails is sent
        again in the background. Dropping one that is gone does nothing."""
        for pool in self.pools:
            self._drop(pool, holder)

    def _drop(self, pool: CxlRegionPool, holder: str) -> None:
        source, handle = self.source, pool.region.handle
        try:
            source.unhold(handle, holder, holder)
        except Exception:
            UNHOLDS.put(
                (self.rm_domain, handle.region_id, holder),
                lambda: source.unhold(handle, holder, holder),
                f"the hold {holder} on {pool.pool_id}",
            )

    def drop_holds(self, incarnation: str) -> None:
        """A worker settled (ended, its GPU drain grace over): drop its holds
        of this run and of the earlier runs still listed."""
        names = {run_holder(incarnation, self.node_epoch)}
        with self._lock:
            for held in self._earlier.values():
                names |= {h for h in held if h.partition("@")[0] == incarnation}
        for name in sorted(names):
            self.unhold(name)

    def release(self) -> None:
        """Give every region back (leave). One still mapped stays out of reuse
        in the RM until its last hold goes; one the RM does not take back now
        is freed when the lease ends."""
        try:
            freed = self.source.node_release(self.node_id, self.node_epoch)
            logger.info("local_cxl tier: %d regions given back to the RM", freed)
        except Exception as e:
            logger.error(
                "local_cxl tier: could not give the regions back (%s); the RM frees "
                "them when the lease ends",
                e,
            )

    # ------------------------------------------------------- the tier hooks

    def taken_over(self, pool_id: str) -> bool:
        return any(p.claimed for p in self.pools if p.pool_id == pool_id)

    def rm_domains(self) -> list[RmDomain]:
        if self.rm_domain is None:
            return []
        devices = sorted({p.region.device_uuid for p in self.pools} - {""})
        return [RmDomain(self.rm_domain, tuple(devices))]

    def worker_settled(self, incarnation: str) -> None:
        self.drop_holds(incarnation)

    def map_for_transfers(self, pool: L1Pool) -> mmap.mmap | None:
        assert isinstance(pool, CxlRegionPool)
        try:
            if self._mapper is not None:
                return self._mapper(pool)
            return map_region(self.source, pool, self.own_holder)
        except (OSError, RuntimeError, ValueError) as e:
            logger.error(
                "local_cxl tier: cannot map %s for transfers: %s",
                pool.pool_id,
                e,
            )
            return None

    def after_transfers_closed(self) -> None:
        """Drop the hold of the node's own mappings (none with ``mapper``)."""
        if self._mapper is None:
            self.unhold(self.own_holder)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

    def describe(self, adopted: set[str]) -> str:
        ids = {p.pool_id for p in self.pools}
        return (
            f"{len(self.pools)} regions ({sum(p.capacity for p in self.pools) >> 20}"
            f" MiB) from RM {self.rm_domain}, {len(adopted & ids)} adopted"
        )


def _make(tier: TierConfig, env: TierEnv) -> CxlTier:
    return CxlTier(
        env.region_source,
        tier,
        node_id=env.node_id,
        node_epoch=env.node_epoch,
        page_size=env.page_size,
        on_lost=env.on_lost,
        renew_s=env.renew_s,
        interrupted=env.interrupted,
        own_holder=env.own_holder,
        mapper=env.map_region,
    )


def _parse(raw: dict, ctx: TierContext) -> dict:
    what, size, page_size = ctx.what, ctx.size, ctx.page_size
    tier: dict = {}
    if "region_size" in raw:
        region = parse_size(raw["region_size"], f"{what}.region_size")
        if region < page_size or region % page_size:
            raise NodeConfigError(
                f"{what}.region_size: {region} must be a multiple of page_size {page_size}"
            )
        count = _ceil_div(size or 0, region)
        if count > MAX_BATCH:
            raise NodeConfigError(
                f"{what}: size {raw['size']} with region_size {raw['region_size']} "
                f"needs {count} regions (node limit {MAX_BATCH}). "
                "Raise region_size or leave it unset."
            )
    else:
        minimum = max(DEFAULT_REGION_SIZE, _ceil_div(size or 0, MAX_BATCH))
        region = _ceil_div(minimum, page_size) * page_size
    if size is not None and size < region:
        raise NodeConfigError(
            f"{what}.size: {size} is smaller than region_size {region}"
        )
    tier["region_size"] = region
    tier["rm"] = _str(raw, "rm", what, DEFAULT_RM)
    visibility = raw.get("visibility", Visibility.COHERENT.value)
    try:
        tier["visibility"] = Visibility(visibility)
    except ValueError:
        raise NodeConfigError(
            f"{what}.visibility: one of {[v.value for v in Visibility]}, got {visibility!r}"
        ) from None
    return tier


def _check_servable(tier: TierConfig) -> None:
    if tier.size is None:
        raise NotServableError("the local_cxl tier needs a size")


SPEC = TierSpec(
    store="local_cxl",
    level=Level.L1,
    local=True,
    kind=PoolKind.CXL,
    keys=frozenset(
        {
            "store",
            "size",
            "high_watermark",
            "low_watermark",
            "region_size",
            "rm",
            "visibility",
        }
    ),
    size_optional=True,
    parse=_parse,
    check_servable=_check_servable,
    make=_make,
    leased=True,
)
