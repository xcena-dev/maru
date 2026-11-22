# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""The local_cxl tier: claiming the node's regions from the RM, the lease, letting
go, and the StorageService registering them."""

import dataclasses
import threading
import time

import pytest

from maru_common.storage_types import CloseMode, PoolKind, Visibility
from maru_shm.types import (
    MaruHandle,
    NodeHostConflictError,
    NodeLeaseExpiredError,
    NodeRegion,
    RegionHolder,
    StaleNodeEpochError,
)
from maru_storage_service.config import TierConfig
from maru_storage_service.drivers import PoolAccess
from maru_storage_service.drivers.cxl import CxlTier, CxlTierError, cxl_pool_id
from maru_storage_service.pool_manager import PageState
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import (
    PAGE,
    FakeRm,
    anon_region,
    node_config,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)

DOMAIN = "rm-test-id"
TIER = TierConfig(
    store="local_cxl",
    size=4 * PAGE,
    region_size=2 * PAGE,
    rm="127.0.0.1:1",
    visibility=Visibility.COHERENT,
)


def tier(rm: FakeRm, epoch: str = "e1", clock=None, on_lost=None) -> CxlTier:
    return CxlTier(
        rm,
        TIER,
        node_id="n",
        node_epoch=epoch,
        page_size=PAGE,
        on_lost=on_lost or (lambda reason: None),
        renew_s=0.01,
        **({"clock": clock} if clock else {}),
    )


# =============================================================================
# Claim
# =============================================================================


def test_a_first_claim_allocates_up_to_the_tier_size():
    rm = FakeRm()
    t = tier(rm)
    t.claim(threading.Event())
    assert t.rm_domain == DOMAIN
    assert [p.claimed for p in t.pools] == [False, False]
    assert sum(p.capacity for p in t.pools) == 4 * PAGE


def test_a_new_run_takes_the_regions_back_and_tops_up():
    rm = FakeRm()
    rm.regions["n"] = [rm.region(2 * PAGE)]
    t = tier(rm, epoch="e2")
    t.claim(threading.Event())
    assert [p.claimed for p in t.pools] == [True, False]
    assert t.pools[0].pool_id == cxl_pool_id(DOMAIN, 1)
    assert rm.epochs["n"] == "e2"


def test_a_region_on_an_unserved_device_is_kept_but_not_used():
    rm = FakeRm()
    region = rm.region(2 * PAGE)
    rm.regions["n"] = [
        NodeRegion(region.handle, region.requested_size, dax_path="", device_uuid="")
    ]
    t = tier(rm, epoch="e2")
    t.claim(threading.Event())
    assert all(not p.claimed for p in t.pools)  # two new ones fill the size
    assert len(rm.regions["n"]) == 3


def test_the_claim_waits_for_the_rm():
    rm = FakeRm()
    rm.unreachable = 2
    t = tier(rm)
    t.claim(threading.Event(), retry_s=0.0)
    assert rm.calls[:3] == ["claim"] * 3 and t.pools


def test_a_stop_ends_the_wait_without_pools():
    rm = FakeRm()
    rm.unreachable = 10**6
    stopped = threading.Event()
    stopped.set()
    t = tier(rm)
    t.claim(stopped)
    assert t.pools == [] and t.rm_domain is None


def test_a_node_of_another_host_is_refused():
    rm = FakeRm()
    rm.node_claim = lambda *a: (_ for _ in ()).throw(NodeHostConflictError("host"))
    with pytest.raises(CxlTierError, match="host"):
        tier(rm).claim(threading.Event())


def test_a_short_rm_goes_on_with_what_it_gave():
    rm = FakeRm()
    rm.capacity = 2 * PAGE
    t = tier(rm)
    t.claim(threading.Event())
    assert sum(p.capacity for p in t.pools) == 2 * PAGE


def test_the_pool_names_its_region_and_maps_by_handle():
    rm = FakeRm()
    t = tier(rm)
    t.claim(threading.Event())
    pool = t.pools[0]
    info = pool.info()
    assert (info.kind, info.rm_domain, info.region_id) == (PoolKind.CXL, DOMAIN, 1)
    assert (info.capacity, info.page_size, info.device_id) == (
        2 * PAGE,
        PAGE,
        "dev-uuid",
    )
    assert pool.access == PoolAccess.MAPPED and pool.worker_fd() is None
    dax = pool.descriptor().dax
    assert dax is not None and dax.rm_domain == DOMAIN and dax.handle.region_id == 1


# =============================================================================
# Lease
# =============================================================================


def test_renewal_failures_let_go_only_after_half_the_lease():
    now = [0.0]
    rm = FakeRm(lease_s=100.0)
    t = tier(rm, clock=lambda: now[0])
    t.claim(threading.Event())
    rm.renew_error = ConnectionRefusedError("RM down")
    now[0] = 49.0
    assert t.renew_once() is None
    now[0] = 50.0
    assert "no lease renewal" in (t.renew_once() or "")
    rm.renew_error = None
    now[0] = 51.0
    assert t.renew_once() is None  # it got through: the clock restarts
    rm.renew_error = ConnectionRefusedError("RM down")
    now[0] = 100.0
    assert t.renew_once() is None


def test_a_stale_epoch_lets_go_at_once():
    rm = FakeRm()
    t = tier(rm)
    t.claim(threading.Event())
    rm.epochs["n"] = "e2"  # another run claimed the node
    assert "another run claimed the node" in (t.renew_once() or "")


def test_a_fenced_run_is_told_from_another_runs_claim():
    rm = FakeRm()
    t = tier(rm)
    t.claim(threading.Event())
    rm.renew_error = NodeLeaseExpiredError("lease ran out")
    reason = t.renew_once() or ""
    assert "lease ended" in reason and "another run" not in reason


def test_the_renew_thread_reports_once():
    rm = FakeRm()
    lost = []
    t = tier(rm, on_lost=lost.append)
    t.claim(threading.Event())
    t.start_renewing()
    rm.epochs["n"] = "e2"
    wait_until(lambda: lost, what="lost")
    t.stop()
    assert len(lost) == 1 and t.lost == lost[0]


def test_the_hold_watch_retries_until_the_rm_answers():
    rm = FakeRm()
    region = rm.region(2 * PAGE)
    rm.regions["n"] = [region]
    rm.hold(region, "w@e0", "e0")
    t = tier(rm, epoch="e1")
    t.claim(threading.Event())
    real, fails = rm.node_holds, [2]

    def flaky(*args):
        if fails[0]:
            fails[0] -= 1
            raise ConnectionRefusedError("RM down")
        return real(*args)

    rm.node_holds = flaky
    cleared = []
    t.watch_holds(cleared.append, interval_s=0.01)
    rm.unhold(region.handle, "w@e0", "w@e0")
    wait_until(lambda: cleared, what="cleared")
    assert cleared == [t.pools[0].pool_id] and t.earlier_holds() == {}
    assert fails == [0]
    t.stop()


def test_a_region_the_node_cannot_map_is_not_left_held():
    from maru_storage_service.drivers.cxl import map_region

    rm = FakeRm()
    t = tier(rm)
    t.claim(threading.Event())
    rm.dax_path = "/nonexistent/dax"
    with pytest.raises(OSError):
        map_region(rm, t.pools[0], "ss@e1")
    assert rm.unholds == [(t.pools[0].region.handle.region_id, "ss@e1")]
    assert "ss@e1" not in rm.holds[t.pools[0].region.handle.region_id]


# =============================================================================
# StorageService
# =============================================================================


@pytest.fixture
def fake():
    server = FakeMaruServer(heartbeat_s=0.1)
    yield server
    server.close()


@pytest.fixture
def sockdir():
    path = short_tmpdir()
    yield path
    remove_tree(path)


def cxl_node(fake, sockdir, rm, **kwargs):
    config = node_config(fake.url, f"{sockdir}/node.sock", cxl_pages=4, region_pages=2)
    kwargs.setdefault("map_region", anon_region)
    return start_node(config, region_source=rm, cxl_renew_s=0.02, **kwargs)


def test_leave_gives_the_regions_back(fake, sockdir):
    rm = FakeRm()
    node = cxl_node(fake, sockdir, rm)
    epoch = node.node_epoch
    node.stop(CloseMode.LEAVE)
    assert rm.released == [("gpu-test", epoch)]


def test_a_lost_lease_stops_the_run_with_leave(fake, sockdir):
    rm = FakeRm()
    node = cxl_node(fake, sockdir, rm)
    rm.epochs["gpu-test"] = "someone-else"
    assert node.wait_finished(5.0)
    assert node.ended and "no longer counts this run" in node.ended
    assert not node.new_run  # another run claimed the node
    assert not fake.nodes["gpu-test"].online  # closed with leave


# =============================================================================
# Unmapping before the regions go back (leave)
# =============================================================================


def test_renewals_come_several_times_per_half_lease():
    rm = FakeRm(lease_s=8.0)
    t = tier(rm)
    t.renew_s = 10.0
    t.claim(threading.Event())
    assert t.renew_interval() == 2.0
    rm.lease_s = 900.0
    t.lease_s = 900.0
    assert t.renew_interval() == 10.0


def test_a_lease_lost_during_the_start_stops_it(fake, sockdir):
    rm = FakeRm()
    fake.pause()  # NODE_OPEN waits
    config = node_config(fake.url, f"{sockdir}/node.sock", cxl_pages=4, region_pages=2)
    from maru_storage_service.ports import ServerStopped
    from maru_storage_service.service import StorageService

    service = StorageService(
        config,
        rpc_timeout_s=0.3,
        region_source=rm,
        cxl_renew_s=0.02,
        map_region=anon_region,
    )
    starter = threading.Thread(target=lambda: _start_quietly(service, ServerStopped))
    starter.start()
    wait_until(lambda: "renew" in rm.calls, what="renewing")
    rm.epochs["gpu-test"] = "someone-else"
    starter.join(10)
    fake.resume()
    assert not starter.is_alive()
    assert service.wait_finished(5.0) and service.ended
    assert service.manager is None or service.local is None or not service.local.started


def _start_quietly(service, stopped_error) -> None:
    try:
        service.start()
    except stopped_error:
        pass


# =============================================================================
# Credit from the cxl tier
# =============================================================================


def _two_pool_manager(free_a: int, free_b: int, reserve: int = 0):
    from maru_common.node_protocol import Budgets
    from maru_common.storage_types import PoolInfo
    from maru_storage_service.pool_manager import PoolManager

    pm = PoolManager(
        Budgets(
            credit_max_bytes=64 * PAGE,
            credit_total_max_bytes=64 * PAGE,
            credit_refill_low=PAGE,
            credit_refill_high=4 * PAGE,
            commit_pending_max_bytes=64 * PAGE,
            transfer_reserve_bytes={"local_cxl": reserve},
        ),
        PAGE,
    )
    for name, pages in (("a", free_a), ("b", free_b)):
        pm.add_pool(
            PoolInfo(
                pool_id=cxl_pool_id(DOMAIN, ord(name)),
                kind=PoolKind.CXL,
                node_id="n",
                pool_generation=1,
                capacity=pages * PAGE,
                page_size=PAGE,
            ),
            tier="local_cxl",
        )
    pm.set_epoch_seq(1)
    return pm


def test_each_page_comes_from_the_pool_with_the_most_room():
    pm = _two_pool_manager(2, 6)
    a, b = cxl_pool_id(DOMAIN, ord("a")), cxl_pool_id(DOMAIN, ord("b"))
    got = pm.credit("w", 3 * PAGE, "local_cxl")
    assert [c.pool_id for c in got] == [b, b, b]
    # b has 3 left and a 2: b, then a on the tie, then the rest
    got = pm.credit("w", 5 * PAGE, "local_cxl")
    assert [c.pool_id for c in got] == [b, a, b, a, b]
    pm.check()


def test_the_transfer_reserve_is_the_tiers_over_all_its_pools():
    pm = _two_pool_manager(4, 4, reserve=6 * PAGE)
    got = pm.credit("w", 8 * PAGE, "local_cxl")
    assert len(got) == 2  # 8 free in the tier, 6 kept for transfers


def cxl_only_config(
    fake, sockdir, pages: int = 16, region_pages: int = 8, high=0.9, low=0.8, role="gpu"
):
    from maru_storage_service.config import parse_node_config

    return parse_node_config(
        {
            "node_id": "gpu-test",
            "role": role,
            "socket": f"{sockdir}/node.sock",
            "server": fake.url,
            "page_size": PAGE,
            "tiers": [
                {
                    "store": "local_cxl",
                    "size": pages * PAGE,
                    "region_size": region_pages * PAGE,
                    **(
                        {"high_watermark": high, "low_watermark": low}
                        if role == "memory"
                        else {}
                    ),
                }
            ],
            "budgets": {
                "credit_max_bytes": 4 * PAGE,
                "credit_total_max_bytes": 8 * PAGE,
                "credit_refill_low": PAGE,
                "credit_refill_high": 4 * PAGE,
                "commit_pending_max_bytes": 4 * PAGE,
                "transfer_reserve_bytes": PAGE,
            },
        }
    )


def test_a_cxl_only_node_gives_credit_from_its_regions(fake, sockdir):
    from tests.unit.ms_node_helpers import RawWorker

    rm = FakeRm()
    node = start_node(
        cxl_only_config(fake, sockdir), region_source=rm, cxl_renew_s=0.02
    )
    try:
        assert node.tier("dram") is None
        worker = RawWorker(f"{sockdir}/node.sock")
        response = worker.attach()
        assert {d.pool.pool_id for d in response.pools} == {
            p.pool_id for p in node.tier("local_cxl").pools
        }
        credits = worker.credit(2 * PAGE).credits
        assert credits and all(c.kind == PoolKind.CXL for c in credits)
        assert worker.write_done(credits[:1], ["k1"]).status.value == "OK"
        worker.close()
    finally:
        node.stop()


def test_pressure_on_a_cxl_only_node_retires_its_pages(fake, sockdir):
    from tests.unit.ms_node_helpers import publish

    rm = FakeRm()
    node = start_node(cxl_only_config(fake, sockdir, role="memory"), region_source=rm)
    try:
        for i in range(15):
            publish(node, f"k{i}")
        wait_until(lambda: node.manager.retire_results, what="a retire")
        assert node.pools.bytes_in(PageState.READY) <= 14 * PAGE
    finally:
        node.stop()


def test_a_fuller_region_is_no_pressure_while_the_tier_has_room(fake, sockdir):
    from tests.unit.ms_node_helpers import publish

    rm = FakeRm()
    config = cxl_only_config(
        fake, sockdir, pages=32, region_pages=4, high=0.4, low=0.3, role="memory"
    )
    node = start_node(config, region_source=rm)
    try:
        for i in range(12):
            publish(node, f"k{i}")
        used = [
            node.pools.bytes_in(PageState.READY, p.pool_id) // PAGE
            for p in node.tier("local_cxl").pools
        ]
        assert max(used) == 2 and sum(used) == 12
        node.manager._evict_once()
        assert not node.manager.retire_results
    finally:
        node.stop()


def test_a_cxl_tier_the_rm_gave_no_region_answers_cxl_credit_with_none(fake, sockdir):
    from tests.unit.ms_node_helpers import RawWorker

    rm = FakeRm()
    rm.capacity = 0
    node = start_node(cxl_only_config(fake, sockdir), region_source=rm)
    worker = RawWorker(node.config.socket)
    try:
        assert node.tier("local_cxl").pools == []
        assert node.manager.credit_kinds == [PoolKind.CXL]
        worker.attach()
        assert worker.credit(PAGE).credits == []
    finally:
        worker.close()
        node.stop()


def test_leave_gives_no_more_cxl_credit(fake, sockdir):
    from maru_common.storage_types import CloseMode

    node = cxl_node(fake, sockdir, FakeRm())
    ids = [p.pool_id for p in node.tier("local_cxl").pools]
    assert all(node.pools.creditable(p) for p in ids)
    node.stop(CloseMode.LEAVE)
    assert not any(node.pools.creditable(p) for p in ids)


class GoneWatcher:
    """Every watched process has already ended."""

    def watch(self, key, pid, pid_start) -> bool:
        return False

    def start(self, callback) -> None:
        pass

    def stop(self) -> None:
        pass


# =============================================================================
# Holds: who maps a region, across restarts (Codex P1 on CXL reuse)
# =============================================================================


def test_leave_gives_back_every_region_and_holds_keep_mapped_ones(fake, sockdir):
    """A worker that does not unmap in time keeps its hold, so the RM keeps
    the region out of reuse; the node still gives every region back."""
    from maru_common.node_protocol import NodeMessageType, UnmapAckRequest
    from maru_common.storage_types import NotifyKind
    from tests.unit.ms_node_helpers import RawWorker

    rm = FakeRm()
    node = cxl_node(fake, sockdir, rm, unmap_timeout_s=0.3)
    worker = RawWorker(f"{sockdir}/node.sock")
    worker.attach()
    first, second = node.tier("local_cxl").pools
    rm.hold(second.region, worker.incarnation, node.node_epoch)
    stopper = threading.Thread(target=node.stop, args=(CloseMode.LEAVE,))
    stopper.start()
    wait_until(
        lambda: (
            len([n for n in worker.notices if n.kind == NotifyKind.POOL_REMOVE]) >= 2
        ),
        what="pool_remove notices",
    )
    worker.request(
        NodeMessageType.UNMAP_ACK,
        UnmapAckRequest(pool_id=first.pool_id, pool_generation=1),
    )
    stopper.join(10)
    worker.close()
    assert rm.released == [("gpu-test", node.node_epoch)]
    assert worker.incarnation in rm.holds[second.region.handle.region_id]


def test_an_ended_worker_counts_as_unmapped_only_after_the_gpu_grace():
    import time

    from maru_storage_service.op_ledger import OpLedger
    from maru_storage_service.worker_registry import WorkerRegistry

    registry = WorkerRegistry(
        node_epoch="e1",
        pools=_two_pool_manager(1, 1),
        ledger=OpLedger(16),
        watcher=GoneWatcher(),
        on_exit=lambda ref: None,
        gpu_drain_grace_s=0.4,
    )
    with registry._lock:
        registry._settles_at["w"] = time.monotonic() + 0.4  # ended just now
    assert not registry.settled("w")
    wait_until(lambda: registry.settled("w"), what="grace over")
    registry.stop()


def test_claimed_regions_without_holders_are_free_at_once(fake, sockdir):
    """No worker maps a region without holding it: none held, none to wait for."""
    rm = FakeRm()
    rm.regions["gpu-test"] = [rm.region(2 * PAGE)]
    node = cxl_node(fake, sockdir, rm, gpu_drain_grace_s=30.0)
    try:
        claimed = node.tier("local_cxl").pools[0].pool_id
        assert claimed in node.adopted
        assert node.pools.bytes_in(PageState.FREE, claimed) == 2 * PAGE
        assert not node.pools.pool_quarantined(claimed)
        assert "holds" not in rm.calls  # nothing to watch
    finally:
        node.stop()


def test_an_earlier_runs_hold_quarantines_the_region_until_it_goes(fake, sockdir):
    """Holds of this run and a remote agent's do not count; an earlier run's
    does, whatever its process, until NODE_HOLDS no longer lists it."""
    rm = FakeRm()
    held, free = rm.region(2 * PAGE), rm.region(2 * PAGE)
    rm.regions["gpu-test"] = [held, free]
    rm.hold(held, "old-worker@e0", "e0")
    rm.hold(free, "agent", "")
    node = cxl_node(fake, sockdir, rm, node_epoch="e1")
    try:
        held_id, free_id = (p.pool_id for p in node.tier("local_cxl").pools)
        assert node.tier("local_cxl").earlier_holds() == {held_id: {"old-worker@e0"}}
        assert node.pools.pool_quarantined(held_id)
        assert node.pools.bytes_in(PageState.QUARANTINED, held_id) == 2 * PAGE
        assert node.pools.bytes_in(PageState.FREE, free_id) == 2 * PAGE
        rm.hold(held, "old-worker@e1", "e1")  # it maps the region again
        wait_until(lambda: rm.calls.count("holds") >= 2, what="watched")
        assert node.pools.pool_quarantined(held_id)
        rm.unhold(held.handle, "old-worker@e0", "old-worker@e0")  # its old map
        wait_until(
            lambda: node.pools.bytes_in(PageState.FREE, held_id) == 2 * PAGE,
            what="quarantine ended",
        )
        assert not node.pools.pool_quarantined(held_id)
        assert node.pools.creditable(held_id) == node.pools.creditable(free_id)
        assert node.tier("local_cxl").earlier_holds() == {}
        node.pools.check()
    finally:
        node.stop()


def test_a_quarantined_region_keeps_its_retired_pages(fake, sockdir):
    """Its READY pages are read and retired as usual, but none is FREE until
    the earlier run's holds are gone."""
    from maru_common.protocol import RecoverItem
    from maru_common.storage_types import Key, Location, make_generation
    from maru_storage_service.drivers.cxl import CxlRegionPool

    rm = FakeRm()
    region = rm.region(2 * PAGE)
    rm.regions["gpu-test"] = [region]
    rm.hold(region, "old-worker@e0", "e0")
    info = CxlRegionPool("gpu-test", DOMAIN, region, PAGE, Visibility.COHERENT).info()
    pool_id = info.pool_id
    item = RecoverItem(
        key=Key("ns", "k"),
        location=Location(pool_id, "a0", make_generation(1, 0), 0, 9, PoolKind.CXL),
        logical_bytes=9,
        pinned=False,
    )
    fake.set_adopted("gpu-test", [(info, [item])])
    node = cxl_node(fake, sockdir, rm, node_epoch="e1")
    try:
        assert node.adopted == [pool_id]
        ((ref, _, _),) = node.pools.ready_pages()
        node.pools.retired(ref)
        node.pools.reclaim(ref)
        assert node.pools.bytes_in(PageState.QUARANTINED, pool_id) == 2 * PAGE
        assert pool_id not in node.manager._evictable_pools("local_cxl")
        rm.unhold(region.handle, "old-worker@e0", "old-worker@e0")
        wait_until(
            lambda: node.pools.bytes_in(PageState.FREE, pool_id) == 2 * PAGE,
            what="quarantine ended",
        )
    finally:
        node.stop()


def test_a_settled_worker_drops_its_holds_of_every_run(fake, sockdir):
    """Its process ended and its GPU drain grace is over: its hold of this
    run and those of earlier runs still listed go."""
    from tests.unit.ms_node_helpers import RawWorker

    rm = FakeRm()
    region = rm.region(2 * PAGE)
    rm.regions["gpu-test"] = [region]
    worker = RawWorker(f"{sockdir}/node.sock")
    rm.hold(region, f"{worker.incarnation}@e0", "e0")
    rm.hold(region, "other@e0", "e0")
    node = cxl_node(fake, sockdir, rm, node_epoch="e1", gpu_drain_grace_s=0.1)
    try:
        worker.attach()
        rm.hold(region, f"{worker.incarnation}@e1", "e1")
        node.manager.workers._worker_exited(worker.incarnation)
        wait_until(
            lambda: (region.handle.region_id, f"{worker.incarnation}@e1") in rm.unholds,
            what="holds dropped",
        )
        assert (region.handle.region_id, f"{worker.incarnation}@e0") in rm.unholds
        assert "other@e0" in rm.holds[region.handle.region_id]
        worker.close()
    finally:
        node.stop()


def test_a_cxl_only_restart_gives_credit_from_claimed_regions_once_released(
    fake, sockdir
):
    from tests.unit.ms_node_helpers import RawWorker

    rm = FakeRm()
    rm.regions["gpu-test"] = [rm.region(8 * PAGE), rm.region(8 * PAGE)]
    first = rm.regions["gpu-test"][0]
    for region in rm.regions["gpu-test"]:
        rm.hold(region, "old-worker@e0", "e0")
    node = start_node(
        cxl_only_config(fake, sockdir),
        region_source=rm,
        cxl_renew_s=0.02,
        gpu_drain_grace_s=1.0,
    )
    try:
        assert all(
            p.claimed for p in node.tier("local_cxl").pools
        )  # nothing new to allocate
        worker = RawWorker(f"{sockdir}/node.sock")
        worker.attach()
        assert worker.credit(PAGE).credits == []  # all quarantined still
        rm.unhold(first.handle, "old-worker@e0", "old-worker@e0")
        wait_until(lambda: worker.credit(PAGE).credits, what="credit after release")
        worker.close()
    finally:
        node.stop()


# =============================================================================
# Hold naming and worker settlement
# =============================================================================


def test_a_hold_name_fits_the_rm():
    """1..128 printable bytes, no spaces, as the RM checks holder names."""
    import uuid

    from maru_common.storage_types import run_holder

    for name in (run_holder(uuid.uuid4().hex, uuid.uuid4().hex), "ss@" + "e" * 32):
        assert 0 < len(name) <= 128
        assert all(0x21 <= ord(c) <= 0x7E for c in name)


def test_a_forgotten_detached_worker_counts_as_settled():
    from maru_storage_service.op_ledger import OpLedger
    from maru_storage_service.worker_registry import WorkerRegistry

    registry = WorkerRegistry(
        node_epoch="e1",
        pools=_two_pool_manager(1, 1),
        ledger=OpLedger(16),
        watcher=GoneWatcher(),
        on_exit=lambda ref: None,
        gpu_drain_grace_s=30.0,
    )
    with registry._lock:
        registry._settles_at["w"] = 0.0  # what forgetting a detached one records
    assert registry.settled("w")
    registry.stop()


# =============================================================================
# Unholds that fail are sent again; the node drops its own ended runs' holds
# =============================================================================


def test_the_node_sends_a_failed_unhold_again():
    from maru_storage_service.drivers.cxl import UNHOLDS

    rm = FakeRm()
    t = tier(rm)
    t.claim(threading.Event())
    region = t.pools[0].region
    rm.hold(region, "ss@e1", "e1")
    rm.unhold_errors = 2
    t.unhold("ss@e1")
    assert UNHOLDS.flush(10)
    assert "ss@e1" not in rm.holds[region.handle.region_id]
    assert rm.unhold_errors == 0


def _dead_pid() -> int:
    import os

    pid = 4_000_000
    while os.path.exists(f"/proc/{pid}"):
        pid -= 1
    return pid


def test_an_ended_runs_own_hold_is_dropped_at_the_start():
    """This process's earlier run (its teardown unmapped) and one of a process
    that is gone, judged from this host, boot and pid namespace; not another
    namespace's, nor a worker's (the RM's own reaper judges those)."""
    import platform

    from maru_common.node_protocol import process_identity

    rm = FakeRm()
    region = rm.region(2 * PAGE)
    rm.regions["n"] = [region]
    boot, pid, start, ns = process_identity()
    host, rid = platform.node(), region.handle.region_id
    dead = _dead_pid()
    rm.holds[rid] = {
        "ss@e0": RegionHolder("ss@e0", "e0", host, boot, pid, start, ns),
        "ss@e8": RegionHolder("ss@e8", "e8", host, boot, dead, 7, ns),
        "ss@e9": RegionHolder("ss@e9", "e9", host, boot, dead, 7, ns + 1),
        "w@e0": RegionHolder("w@e0", "e0", host, boot, dead, 7, ns),
    }
    t = tier(rm, epoch="e1")
    t.claim(threading.Event())
    assert t.earlier_holds() == {t.pools[0].pool_id: {"ss@e9", "w@e0"}}
    assert sorted(h for _, h in rm.unholds) == ["ss@e0", "ss@e8"]


def test_an_rm_without_node_holds_refuses_the_start_while_regions_are_held():
    from maru_shm.types import NodeOwnershipUnsupportedError

    rm = FakeRm()
    rm.holds_error = NodeOwnershipUnsupportedError("old RM")
    plain = tier(rm, epoch="e1")
    plain.claim(threading.Event())  # nothing held: NODE_HOLDS is not needed
    region = rm.region(2 * PAGE)
    rm.regions["n"].append(region)
    rm.hold(region, "w@e1", "e1")
    with pytest.raises(CxlTierError, match="upgrade the RM"):
        tier(rm, epoch="e2").claim(threading.Event())


def test_a_region_no_longer_the_nodes_is_watched_no_more(caplog):
    import logging

    rm = FakeRm()
    gone, kept = rm.region(2 * PAGE), rm.region(2 * PAGE)
    rm.regions["n"] = [gone, kept]
    rm.hold(gone, "w@e0", "e0")
    rm.hold(kept, "w@e0", "e0")
    t = tier(rm, epoch="e1")
    t.claim(threading.Event())
    log = logging.getLogger("maru_storage_service.drivers.cxl")
    log.addHandler(caplog.handler)
    cleared = []
    try:
        caplog.set_level(logging.INFO, logger=log.name)
        rm.regions["n"] = [kept]  # an operator freed it
        t.watch_holds(cleared.append, interval_s=0.01)
        wait_until(
            lambda: set(t.earlier_holds()) == {t.pools[1].pool_id}, what="dropped"
        )
        time.sleep(0.1)
    finally:
        log.removeHandler(caplog.handler)
        t.stop()
    assert cleared == []
    notes = {id(r) for r in caplog.records if "no longer the node's" in r.getMessage()}
    assert len(notes) == 1


def test_regions_still_held_are_logged_with_a_hint(caplog, monkeypatch):
    import logging

    from maru_storage_service.drivers import cxl

    monkeypatch.setattr(cxl, "HOLDS_LOG_S", 0.0)
    rm = FakeRm()
    region = rm.region(2 * PAGE)
    rm.regions["n"] = [region]
    rm.hold(region, "w@e0", "e0")
    t = tier(rm, epoch="e1")
    t.claim(threading.Event())
    log = logging.getLogger("maru_storage_service.drivers.cxl")
    log.addHandler(caplog.handler)
    try:
        caplog.set_level(logging.INFO, logger=log.name)
        t.watch_holds(lambda pool_id: None, interval_s=0.01)
        wait_until(
            lambda: any("drop-holds" in r.getMessage() for r in caplog.records),
            what="the hint",
        )
    finally:
        log.removeHandler(caplog.handler)
        t.stop()
    (hint, *_) = [
        r.getMessage() for r in caplog.records if "drop-holds" in r.getMessage()
    ]
    assert "w@e0" in hint and "maru-rm-ctl drop-holds <holder> --confirm" in hint


def test_an_invalid_pool_gives_no_credit_when_its_quarantine_ends(fake, sockdir):
    from maru_storage_service.pool_manager import PageRef

    rm = FakeRm()
    region = rm.region(2 * PAGE)
    rm.regions["gpu-test"] = [region]
    rm.hold(region, "w@e0", "e0")
    node = cxl_node(fake, sockdir, rm, node_epoch="e1")
    try:
        pool_id = node.tier("local_cxl").pools[0].pool_id
        node.manager._pool_invalid(PageRef(pool_id, 0, 0))
        rm.unhold(region.handle, "w@e0", "w@e0")
        wait_until(lambda: not node.pools.pool_quarantined(pool_id), what="ended")
        assert not node.pools.creditable(pool_id)
    finally:
        node.stop()


# =============================================================================
# Asynchronous unhold retries
# =============================================================================


class TestUnholds:
    def test_each_drop_is_retried_on_its_own_schedule(self):
        from maru_shm.unholds import Unholds

        unholds = Unholds(retry_s=0.05, max_retry_s=0.2)
        sent: list[str] = []
        fails = {"a": 100, "b": 1}

        def send(key):
            def go():
                sent.append(key)
                if fails[key]:
                    fails[key] -= 1
                    raise ConnectionRefusedError("RM down")

            return go

        unholds.put("a", send("a"), "a")
        unholds.put("b", send("b"), "b")
        wait_until(lambda: sent.count("b") == 2, what="b retried once")
        time.sleep(0.5)
        assert sent.count("b") == 2  # confirmed: not sent again
        assert 3 <= sent.count("a") <= 8  # backing off to 0.2 s, not every pass
        assert unholds.pending() == 1

    def test_a_refused_drop_is_not_sent_again(self, caplog):
        import logging

        from maru_shm.client import HoldRefusedError
        from maru_shm.unholds import Unholds

        unholds = Unholds(retry_s=0.01)
        sent: list[int] = []

        def refused():
            sent.append(1)
            raise HoldRefusedError("Unhold failed with status -22")

        log = logging.getLogger("maru_shm.unholds")
        log.addHandler(caplog.handler)
        try:
            unholds.put("k", refused, "the hold w@e1 on region 5")
            assert unholds.flush(5)
            time.sleep(0.1)
        finally:
            log.removeHandler(caplog.handler)
        assert sent == [1] and unholds.pending() == 0
        notes = {id(r) for r in caplog.records if "not sent again" in r.getMessage()}
        assert len(notes) == 1

    def test_a_drop_taken_back_is_warned_about_afresh(self, caplog):
        import logging

        from maru_shm.unholds import Unholds

        # No retry on its own within the test: each send is stepped by hand
        unholds = Unholds(retry_s=60, max_retry_s=60)
        entered, gate = threading.Semaphore(0), threading.Semaphore(0)
        fails = [2]

        def flaky():
            entered.release()
            assert gate.acquire(timeout=5)
            if fails[0]:
                fails[0] -= 1
                raise ConnectionRefusedError("RM down")

        log = logging.getLogger("maru_shm.unholds")
        log.addHandler(caplog.handler)
        try:
            unholds.put("k", flaky, "the drop")
            assert entered.acquire(timeout=5)
            gate.release()  # the first send fails and is queued again
            assert unholds.take_back("k") is not None  # after the send ended
            unholds.put("k", flaky, "the drop")
            assert entered.acquire(timeout=5)
            gate.release()  # the second send fails: warned about afresh
            wait_until(
                lambda: fails[0] == 0 and unholds._sending is None,
                what="the second send to end",
            )
            gate.release()  # the one flush sends again succeeds
            assert unholds.flush(5)
        finally:
            log.removeHandler(caplog.handler)
        warnings = {id(r) for r in caplog.records if "could not drop" in r.getMessage()}
        assert len(warnings) == 2  # once before the take-back, once after


def test_a_worker_maps_a_cxl_descriptor_through_its_rm():
    from maru_handler.client.pool_maps import PoolMaps
    from maru_storage_service.drivers.cxl import CxlRegionPool

    rm = FakeRm()
    pool = CxlRegionPool(
        node_id="n",
        rm_domain=DOMAIN,
        region=rm.region(2 * PAGE),
        page_size=PAGE,
        visibility=Visibility.COHERENT,
        rm_address="127.0.0.1:9851",
    )
    seen = []

    class Mapping:
        cuda_registered = False

        def view(self, offset, length):
            return memoryview(bytearray(length))

        def page_lock(self):
            pass

        def close(self):
            seen.append("closed")

    def map_dax(info, dax):
        seen.append((info.pool_id, dax.rm_address, dax.handle.region_id))
        return Mapping()

    maps = PoolMaps(pin_pools=False, map_dax=map_dax)
    maps.add_dax(pool.descriptor(), "e1")
    assert seen == [(pool.pool_id, "127.0.0.1:9851", 1)]
    assert maps.get(pool.pool_id) is not None
    assert maps.remove(pool.pool_id) and maps.flush(5) and seen[-1] == "closed"


@pytest.fixture
def holding_rm(monkeypatch):
    import os

    from maru_handler.client import pool_maps

    fd = os.memfd_create("fake-cxl")
    os.ftruncate(fd, 8 * PAGE)
    rm = HoldingRm(f"/proc/self/fd/{fd}")
    monkeypatch.setattr(pool_maps, "_rm_client", lambda address: rm)
    monkeypatch.setattr(pool_maps, "_HOLDS", pool_maps.RegionHolds())
    yield rm
    os.close(fd)


def unheld(rm, maps):
    """The last RM call once every queued close and unhold went out."""
    from maru_handler.client import pool_maps

    assert maps.flush(5) and pool_maps._HOLDS.unholds.flush(5)
    return rm.calls[-1]


def _cxl_descriptor(region_pages: int = 2, epoch: str = "e1"):
    from maru_common.storage_types import DaxMapping, PoolDescriptor, PoolInfo

    handle = MaruHandle(region_id=5, offset=0, length=region_pages * PAGE, auth_token=7)
    info = PoolInfo(
        pool_id=cxl_pool_id(DOMAIN, 5),
        kind=PoolKind.CXL,
        node_id="n",
        pool_generation=1,
        capacity=region_pages * PAGE,
        page_size=PAGE,
    )
    return PoolDescriptor(
        pool=info, dax=DaxMapping(DOMAIN, handle, "rm:1", node_id="n", node_epoch=epoch)
    )


def test_a_worker_holds_a_region_before_mapping_and_unholds_after(holding_rm):
    from maru_handler.client.pool_maps import PoolMaps, RegionHolder

    maps = PoolMaps(pin_pools=False, holder=RegionHolder.this_process("w1"))
    descriptor = _cxl_descriptor()
    maps.add_dax(descriptor, "e1")
    assert holding_rm.calls == [("hold", 5, "w1@e1", "e1")]
    maps.add_dax(descriptor, "e1")  # the same run maps it again: no new hold
    assert holding_rm.calls[-1] == ("access", 5)
    assert maps.remove(descriptor.pool.pool_id)
    assert unheld(holding_rm, maps) == ("unhold", 5, "w1@e1")


def test_each_run_holds_apart_and_the_old_runs_hold_goes_with_its_mapping(
    holding_rm,
):
    """The next run maps the region again under its own hold; closing the old
    run's mapping (its last view released) drops only the old run's hold."""
    from maru_common.storage_types import Location
    from maru_handler.client.pool_maps import PoolMaps, RegionHolder

    maps = PoolMaps(pin_pools=False, holder=RegionHolder.this_process("w1"))
    old = _cxl_descriptor(epoch="e1")
    maps.add_dax(old, "e1")
    view = maps.view(Location(old.pool.pool_id, "a", 1, 0, PAGE, PoolKind.CXL))
    maps.add_dax(_cxl_descriptor(epoch="e2"), "e2")  # retires the old mapping
    assert holding_rm.calls == [("hold", 5, "w1@e1", "e1"), ("hold", 5, "w1@e2", "e2")]
    view.release()  # the old mapping closes
    assert unheld(holding_rm, maps) == ("unhold", 5, "w1@e1")
    assert maps.remove(old.pool.pool_id)
    assert unheld(holding_rm, maps) == ("unhold", 5, "w1@e2")


def test_a_refused_hold_maps_nothing(holding_rm):
    from maru_handler.client.pool_maps import PoolMaps, RegionHolder

    holding_rm.refuse = StaleNodeEpochError("an older run")
    maps = PoolMaps(pin_pools=False, holder=RegionHolder.this_process("w1"))
    with pytest.raises(StaleNodeEpochError):
        maps.add_dax(_cxl_descriptor(), "e1")
    assert maps.pool_ids() == []
    assert [c[0] for c in holding_rm.calls] == ["hold"]


def test_a_mapping_still_in_use_keeps_its_hold(holding_rm):
    from maru_common.storage_types import Location
    from maru_handler.client.pool_maps import PoolMaps, RegionHolder

    maps = PoolMaps(pin_pools=False, holder=RegionHolder.this_process("w1"))
    descriptor = _cxl_descriptor()
    maps.add_dax(descriptor, "e1")
    view = maps.view(Location(descriptor.pool.pool_id, "a", 1, 0, PAGE, PoolKind.CXL))
    assert not maps.remove(descriptor.pool.pool_id)  # a view is held
    assert holding_rm.calls == [("hold", 5, "w1@e1", "e1")]
    view.release()
    assert maps.remove(descriptor.pool.pool_id)
    assert unheld(holding_rm, maps) == ("unhold", 5, "w1@e1")


def test_one_rm_under_two_addresses_is_one_hold(holding_rm):
    """The RM keys holds by region and holder; so does the worker, by the RM's
    id, not by the address it was reached at."""
    import dataclasses

    from maru_handler.client.pool_maps import PoolMaps, RegionHolder

    maps = PoolMaps(pin_pools=False, holder=RegionHolder.this_process("w1"))
    first = _cxl_descriptor(epoch="e1")
    maps.add_dax(first, "e1")
    second = dataclasses.replace(
        first, dax=dataclasses.replace(first.dax, rm_address="other:1")
    )
    maps.add_dax(second, "e1")  # retires the first mapping
    assert [c[0] for c in holding_rm.calls] == ["hold", "access"]
    assert maps.remove(first.pool.pool_id)
    assert unheld(holding_rm, maps) == ("unhold", 5, "w1@e1")


def test_a_failed_second_mapping_does_not_keep_a_count(holding_rm):
    from maru_handler.client.pool_maps import PoolMaps, RegionHolder

    maps = PoolMaps(pin_pools=False, holder=RegionHolder.this_process("w1"))
    descriptor = _cxl_descriptor()
    maps.add_dax(descriptor, "e1")
    holding_rm.get_access = lambda handle: (_ for _ in ()).throw(OSError("RM down"))
    with pytest.raises(OSError):
        maps.add_dax(descriptor, "e1")
    assert maps.remove(descriptor.pool.pool_id)
    assert unheld(holding_rm, maps) == ("unhold", 5, "w1@e1")


def test_a_worker_drops_a_hold_off_the_releasing_thread_and_again_on_failure(
    holding_rm,
):
    from maru_handler.client.pool_maps import PoolMaps, RegionHolder

    maps = PoolMaps(pin_pools=False, holder=RegionHolder.this_process("w1"))
    descriptor = _cxl_descriptor()
    maps.add_dax(descriptor, "e1")
    gate = threading.Event()
    unhold = holding_rm.unhold

    def slow_unhold(handle, holder, instance):
        assert gate.wait(5)
        unhold(handle, holder, instance)

    holding_rm.unhold = slow_unhold
    holding_rm.unhold_error = ConnectionRefusedError("RM down")
    started = time.monotonic()
    assert maps.remove(descriptor.pool.pool_id)
    assert time.monotonic() - started < 1.0  # the RM is not waited on
    gate.set()
    assert unheld(holding_rm, maps) == ("unhold", 5, "w1@e1")
    assert [c for c in holding_rm.calls if c[0] == "unhold"] == [
        ("unhold", 5, "w1@e1")
    ] * 2  # failed once, then confirmed


def test_a_hold_wanted_again_is_not_dropped(holding_rm):
    """A drop still queued when the run maps the region again is taken back."""
    from maru_handler.client import pool_maps
    from maru_handler.client.pool_maps import PoolMaps, RegionHolder

    gate = threading.Event()
    unhold = holding_rm.unhold

    def slow_unhold(handle, holder, instance):
        assert gate.wait(5)
        unhold(handle, holder, instance)

    holding_rm.unhold = slow_unhold
    maps = PoolMaps(pin_pools=False, holder=RegionHolder.this_process("w1"))
    descriptor = _cxl_descriptor()
    other = dataclasses.replace(
        descriptor,
        dax=dataclasses.replace(
            descriptor.dax,
            handle=dataclasses.replace(descriptor.dax.handle, region_id=6),
        ),
        pool=dataclasses.replace(descriptor.pool, pool_id=cxl_pool_id(DOMAIN, 6)),
    )
    maps.add_dax(other, "e1")
    assert maps.remove(other.pool.pool_id)  # its drop holds up the queue
    assert maps.flush(5)
    maps.add_dax(descriptor, "e1")
    assert maps.remove(descriptor.pool.pool_id)  # queued behind it
    assert maps.flush(5)
    maps.add_dax(descriptor, "e1")  # mapped again before it went out
    gate.set()
    assert pool_maps._HOLDS.unholds.flush(5)
    dropped = [c for c in holding_rm.calls if c[0] == "unhold"]
    assert dropped == [("unhold", 6, "w1@e1")]
    assert holding_rm.calls.count(("hold", 5, "w1@e1", "e1")) == 2


def _lookup_client():
    from maru_common.config import MaruConfig
    from maru_handler.client.maru_client import MaruClient
    from maru_handler.client.pool_maps import PoolMaps, RegionHolder

    client = MaruClient(
        MaruConfig(
            maru_node="/nonexistent/node.sock",
            engine_id="e",
            cache_namespace="ns",
            auto_connect=False,
            pin_pools=False,
        )
    )
    client.pools = PoolMaps(pin_pools=False, holder=RegionHolder.this_process("w1"))
    return client


def _hit(pin_id: str):
    from maru_common.storage_types import ItemOutcome, ItemResult, Location

    location = Location(cxl_pool_id(DOMAIN, 5), "a", 1, 0, PAGE, PoolKind.CXL)
    return ItemOutcome(ItemResult.HIT, location=location, pin_id=pin_id)


def test_a_lookup_of_an_earlier_run_is_not_read_through_the_new_mapping(holding_rm):
    """The new run released that run's pins: the page may be reused under a
    read through the new run's mapping of the same region."""
    client = _lookup_client()
    client.pools.add_dax(_cxl_descriptor(epoch="e1"), "e1")
    client.pools.add_dax(_cxl_descriptor(epoch="e2"), "e2")  # re-attached
    client.node_epoch = "e2"
    server = _Unpins()
    assert client._take_outcome(server, "k", _hit("p1"), "srv", "e1") is None
    assert server.pins == ["p1"] and not client._reads
    assert client.stats["retrieve_unavailable"] == 1
    read = client._take_outcome(server, "k", _hit("p2"), "srv", "e2")
    assert read is not None
    client.release_retrieved([read])


def test_an_earlier_runs_mapping_keeps_its_hold_while_it_serves_a_read(holding_rm):
    client = _lookup_client()
    client.pools.add_dax(_cxl_descriptor(epoch="e1"), "e1")
    client.node_epoch = "e1"
    read = client._take_outcome(_Unpins(), "k", _hit("p1"), "srv", "e1")
    assert read is not None
    client.pools.add_dax(_cxl_descriptor(epoch="e2"), "e2")  # re-attached
    client.node_epoch = "e2"
    assert unheld(holding_rm, client.pools) == ("hold", 5, "w1@e2", "e2")  # e1's stays
    client.release_retrieved([read])  # the old mapping closes
    assert unheld(holding_rm, client.pools) == ("unhold", 5, "w1@e1")


def test_a_drop_taken_back_goes_out_again_if_the_hold_fails(holding_rm):
    from maru_handler.client import pool_maps
    from maru_handler.client.pool_maps import PoolMaps, RegionHolder

    gate = threading.Event()
    unhold = holding_rm.unhold

    def slow_unhold(handle, holder, instance):
        assert gate.wait(5)
        unhold(handle, holder, instance)

    holding_rm.unhold = slow_unhold
    maps = PoolMaps(pin_pools=False, holder=RegionHolder.this_process("w1"))
    descriptor = _cxl_descriptor()
    other = dataclasses.replace(
        descriptor,
        dax=dataclasses.replace(
            descriptor.dax,
            handle=dataclasses.replace(descriptor.dax.handle, region_id=6),
        ),
        pool=dataclasses.replace(descriptor.pool, pool_id=cxl_pool_id(DOMAIN, 6)),
    )
    maps.add_dax(other, "e1")
    assert maps.remove(other.pool.pool_id)  # its drop holds up the queue
    assert maps.flush(5)
    maps.add_dax(descriptor, "e1")
    assert maps.remove(descriptor.pool.pool_id)  # queued behind it
    assert maps.flush(5)
    holding_rm.refuse = OSError("RM down")
    with pytest.raises(OSError):
        maps.add_dax(descriptor, "e1")  # takes the drop back, then fails
    gate.set()
    assert pool_maps._HOLDS.unholds.flush(5)
    dropped = [c for c in holding_rm.calls if c[0] == "unhold"]
    assert ("unhold", 5, "w1@e1") in dropped  # sent after all


class HoldingRm:
    """The RM client a worker's CXL mapping uses."""

    def __init__(self, path: str):
        self.path = path
        self.calls: list[tuple] = []
        self.refuse: Exception | None = None
        self.unhold_error: Exception | None = None  # the next unhold raises it

    def node_hold(self, node_id, node_epoch, handle, holder, **who):
        self.calls.append(("hold", handle.region_id, holder, node_epoch))
        if self.refuse is not None:
            raise self.refuse
        from types import SimpleNamespace

        return SimpleNamespace(dax_path=self.path)

    def get_access(self, handle):
        from types import SimpleNamespace

        self.calls.append(("access", handle.region_id))
        return SimpleNamespace(dax_path=self.path)

    def unhold(self, handle, holder, instance):
        self.calls.append(("unhold", handle.region_id, holder))
        if self.unhold_error is not None:
            error, self.unhold_error = self.unhold_error, None
            raise error


class _Unpins:
    def __init__(self):
        self.pins: list[str] = []

    def unpin(self, pin_ids):
        self.pins.extend(pin_ids)
