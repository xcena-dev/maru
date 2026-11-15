# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Write-back demotion in a [dram, cxl] node: DRAM makes room by moving its
pages to CXL (COMMIT with a finalizer, FINALIZE_MOVE), CXL by dropping. memfds stand
in for the CXL regions, both in the node and in the workers."""

import dataclasses
import mmap
import os
import threading
import time

import pytest

from maru_common.config import MaruConfig
from maru_common.node_protocol import Budgets
from maru_common.protocol import CommitResponse, MessageType
from maru_common.storage_types import (
    ItemOutcome,
    ItemResult,
    Key,
    Location,
    PoolInfo,
    PoolKind,
    Status,
)
from maru_handler import MaruHandler
from maru_handler.client import pool_maps
from maru_handler.storage.cpu import CpuPool
from maru_storage_service.config import parse_node_config
from maru_storage_service.pool_manager import PageState, PoolManager
from maru_storage_service.ports import EpochChanged
from maru_storage_service.service import (
    Node,
    StorageService,
    check_servable,
)
from maru_storage_service.transfer_engine import Copy, TransferEngine
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import (
    PAGE,
    node_config,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)
from tests.unit.test_ms_cxl_tier import FakeRm

NS = "ns"
SMALL_CREDIT = {
    "credit_max_bytes": 2 * PAGE,
    "credit_total_max_bytes": 2 * PAGE,
    "credit_refill_low": 1 * PAGE,
    "credit_refill_high": 2 * PAGE,
    "commit_pending_max_bytes": 2 * PAGE,
}


def value_of(key: str, size: int = 3000) -> bytes:
    return (key.encode() * size)[:size]


class Regions:
    """memfds standing in for the CXL regions: the node maps them for its
    copies, workers for their reads."""

    def __init__(self):
        self.fds: dict[int, int] = {}
        self.maps: list[mmap.mmap] = []
        self.refuse = False

    def node_map(self, pool) -> mmap.mmap:
        if self.refuse:
            raise OSError("no access")
        handle = pool.region.handle
        fd = os.memfd_create(f"cxl-{handle.region_id}")
        os.ftruncate(fd, handle.length)
        self.fds[handle.region_id] = fd
        return mmap.mmap(
            fd,
            handle.length,
            flags=mmap.MAP_SHARED,
            prot=mmap.PROT_READ | mmap.PROT_WRITE,
        )

    def worker_map(self, info: PoolInfo, dax, who):
        fd = self.fds.get(dax.handle.region_id)
        if fd is None:
            raise OSError("no such region")
        mapping = mmap.mmap(
            fd,
            info.capacity,
            flags=mmap.MAP_SHARED,
            prot=mmap.PROT_READ | mmap.PROT_WRITE,
        )
        return CpuPool(
            info.pool_id,
            info.capacity,
            info.page_size,
            medium="cxl",
            mapping=mapping,
            shared=True,
        )

    def close(self) -> None:
        for fd in self.fds.values():
            os.close(fd)


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


@pytest.fixture
def regions(monkeypatch):
    stand_in = Regions()
    # workers map the memfds instead of holding the regions in an RM
    monkeypatch.setattr(pool_maps, "CxlRegionMapping", stand_in.worker_map)
    yield stand_in
    stand_in.close()


@pytest.fixture
def nodes(fake, sockdir, regions):
    started = []

    def make(pages=16, high=0.5, low=0.25, cxl_pages=16, region_pages=8, **kwargs):
        config = node_config(
            fake.url,
            f"{sockdir}/node.sock",
            pages=pages,
            high=high,
            low=low,
            cxl_pages=cxl_pages,
            region_pages=region_pages,
            budgets=SMALL_CREDIT,
        )
        # Never an RM of this host: the regions come from a FakeRm
        cxl = dataclasses.replace(config.tiers[1], rm="127.0.0.1:1")
        config = dataclasses.replace(config, tiers=(config.tiers[0], cxl))
        options = {
            "region_source": FakeRm(),
            "cxl_renew_s": 0.05,
            "map_region": regions.node_map,
        }
        options.update(kwargs)
        node = start_node(config, **options)
        started.append(node)
        return node

    yield make
    for node in started:
        node.stop()


@pytest.fixture
def handlers(nodes):
    made = []

    def make(node):
        handler = MaruHandler(
            MaruConfig(
                maru_node=node.config.socket,
                engine_id="eng",
                cache_namespace=NS,
                chunk_size_bytes=PAGE,
                pool_size=PAGE,
                pin_pools=False,
                timeout_ms=300,
                auto_connect=False,
                store_timeout_s=10.0,
            )
        )
        handler.connect()
        made.append(handler)
        return handler

    yield make
    for handler in made:
        try:
            handler.close()
        except Exception:
            pass


def put(handler, key: str) -> bool:
    wait_until(lambda: handler._cpu.credits.unused_bytes >= PAGE, what="credit")
    value = value_of(key)
    handle = handler.alloc(len(value))
    handle.buf[:] = value
    return handler.store(key, handle)


def put_until_the_run_ends(node, handler, count: int = 10) -> None:
    for i in range(count):
        wait_until(
            lambda: node.wait(0) or handler._cpu.credits.unused_bytes >= PAGE,
            what="credit or the end of the run",
        )
        if node.wait(0):
            return
        try:
            put(handler, f"k{i}")
        except Exception:
            return  # the run ended meanwhile


def kinds(fake, key: str) -> set[PoolKind]:
    return {loc.kind for loc in fake.locations(Key(NS, key))}


def cxl_ids(node) -> list[str]:
    return [p.pool_id for p in node.tier("cxl").pools]


def cxl_bytes(node, state: PageState) -> int:
    return sum(node.pools.bytes_in(state, p) for p in cxl_ids(node))


def finalized(fake) -> list:
    return [i for r in fake.requests(MessageType.FINALIZE_MOVE) for i in r.items]


# =============================================================================
# Traits and config
# =============================================================================


def _config(stores, **tier):
    tiers = []
    for store in stores:
        t = {"store": store, "size": 16 * PAGE}
        if store == "cxl":
            t["region_size"] = 8 * PAGE
        tiers.append(t)
    tiers[0].update(tier)
    return parse_node_config(
        {
            "node_id": "n",
            "server": "tcp://127.0.0.1:1",
            "page_size": PAGE,
            "tiers": tiers,
            "budgets": SMALL_CREDIT | {"transfer_reserve_bytes": PAGE},
        }
    )


def test_dram_over_cxl_demotes_and_cxl_drops():
    traits = _config(["dram", "cxl"]).traits()
    dram, cxl = traits[PoolKind.DRAM], traits[PoolKind.CXL]
    assert (dram.credit, dram.evicts, dram.demotes_to) == (True, True, ("cxl",))
    assert (cxl.credit, cxl.evicts, cxl.demotes_to) == (False, True, ())


def test_one_tier_nodes_drop_as_before():
    for store in ("dram", "cxl"):
        (only,) = _config([store]).traits().values()
        assert (only.credit, only.evicts, only.demotes_to) == (True, True, ())


def test_write_through_is_served():
    check_servable(_config(["dram", "cxl"]))
    config = _config(["dram", "cxl"], demotion="through")
    check_servable(config)
    dram = config.traits()["dram"]
    assert dram.demotes_to == dram.copies_to == ("cxl",)
    assert dram.through_max_inflight == 256 << 20


# =============================================================================
# TransferEngine
# =============================================================================


def _loc(pool_id: str, offset: int, length: int = PAGE) -> Location:
    return Location(
        pool_id=pool_id,
        allocation_id="a",
        generation=1,
        offset=offset,
        length=length,
        kind=PoolKind.DRAM,
    )


def _engine(rate: float = 1 << 40) -> tuple[TransferEngine, mmap.mmap, mmap.mmap]:
    engine = TransferEngine(rate)
    src, dst = mmap.mmap(-1, 8 * PAGE), mmap.mmap(-1, 8 * PAGE)
    engine.add_pool("src", src)
    engine.add_pool("dst", dst)
    engine.start()
    return engine, src, dst


def test_the_engine_copies_and_answers_each_copy():
    engine, src, dst = _engine()
    src[PAGE : PAGE + 5] = b"hello"
    results = engine.run(
        [
            Copy(_loc("src", PAGE, 5), _loc("dst", 3 * PAGE), 5),
            Copy(_loc("src", 0), _loc("nowhere", 0), 5),  # not mapped
            Copy(_loc("src", 7 * PAGE), _loc("dst", 8 * PAGE), 5),  # outside
        ]
    )
    assert results == [True, False, False]
    assert dst[3 * PAGE : 3 * PAGE + 5] == b"hello"
    assert engine.copied_bytes == 5
    engine.close()


def test_the_engine_keeps_to_its_bandwidth_budget():
    rate = 10 * PAGE  # bytes a second: one page every 0.1 s
    engine, _, _ = _engine(rate)
    copies = [
        Copy(_loc("src", i * PAGE), _loc("dst", i * PAGE), PAGE) for i in range(6)
    ]
    started = time.monotonic()
    assert engine.run(copies) == [True] * 6
    elapsed = time.monotonic() - started
    assert elapsed >= 5 * PAGE / rate * 0.95  # the first one starts at once
    assert engine.copied_bytes == 6 * PAGE
    engine.close()


def test_a_stopped_engine_fails_what_it_has_not_started():
    engine, _, _ = _engine(rate=2 * PAGE)  # half a second a page
    copies = [
        Copy(_loc("src", i * PAGE), _loc("dst", i * PAGE), PAGE) for i in range(4)
    ]
    got = []
    runner = threading.Thread(target=lambda: got.append(engine.run(copies)))
    runner.start()
    time.sleep(0.2)
    engine.stop()
    runner.join(5)
    assert got and got[0][0] and not any(got[0][1:])
    assert engine.run(copies[:1]) == [False]  # nothing starts after stop
    engine.close()


# =============================================================================
# Demotion in a [dram, cxl] node
# =============================================================================


def test_a_full_dram_moves_its_pages_to_cxl_and_reads_follow(fake, nodes, handlers):
    node = nodes()
    h = handlers(node)
    assert node.transfers is not None
    for i in range(10):
        assert put(h, f"k{i}")
    wait_until(lambda: node.manager.move_results["RETIRED"] >= 3, what="pages demoted")
    moved = [f"k{i}" for i in range(10) if kinds(fake, f"k{i}") == {PoolKind.CXL}]
    assert moved
    for key in moved:
        (location,) = fake.locations(Key(NS, key))
        ref = node.pools.find(location)
        assert ref is not None and node.pools.page(ref).state == PageState.READY
        read = h.retrieve(key)
        assert read is not None and bytes(read.view) == value_of(key)
        h.release_retrieved([read])
    # every key is somewhere, and only one place once moved
    assert all(kinds(fake, f"k{i}") for i in range(10))
    for item in finalized(fake):
        assert item.dest.kind == PoolKind.CXL and item.source.kind == PoolKind.DRAM
    wait_until(lambda: fake.pin_count() == 0, what="every finalizer released")
    node.pools.check()


def test_a_pinned_source_is_kept_and_its_copy_moves_it_later(fake, nodes, handlers):
    # demote nearly everything, into regions that never fill
    node = nodes(low=0.0625, cxl_pages=32, region_pages=16)
    fake.preserve(Key(NS, "k0"))  # a RETIRE would keep it; a move does not
    h = handlers(node)
    assert put(h, "k0")
    pinned = h.retrieve("k0")  # a reader holds k0 in DRAM
    for i in range(1, 10):
        assert put(h, f"k{i}")
    wait_until(
        lambda: node.manager.move_results["KEPT:PINNED"] >= 1, what="a KEPT move"
    )
    assert kinds(fake, "k0") == {PoolKind.DRAM, PoolKind.CXL}
    assert bytes(pinned.view) == value_of("k0")
    with fake._lock:  # the finalizer went with FINALIZE_MOVE
        assert not [
            p
            for p in fake.pins.values()
            if p.owner.startswith("finalizer:") and p.key == Key(NS, "k0")
        ]
    h.release_retrieved([pinned])
    time.sleep(0.3)  # past the KEPT backoff
    # the next demotion pins the copy again and moves the source onto it
    for i in range(10, 30):  # pressure again, if it went meanwhile
        if kinds(fake, "k0") == {PoolKind.CXL}:
            break
        assert put(h, f"k{i}")
    wait_until(lambda: kinds(fake, "k0") == {PoolKind.CXL}, what="source moved")
    first, again = [i.dest for i in finalized(fake) if i.key.key == "k0"]
    assert first == again  # no second copy
    assert not [
        i
        for r in fake.requests(MessageType.RETIRE)
        for i in r.items
        if i.key.key == "k0" and i.location.kind == PoolKind.DRAM
    ]
    read = h.retrieve("k0")
    assert bytes(read.view) == value_of("k0")
    h.release_retrieved([read])
    node.pools.check()


def test_a_rejected_move_retires_and_frees_the_copy(fake, nodes, handlers):
    node = nodes(low=0.0625)  # keeps demoting: the sources are tried again
    h = handlers(node)
    finalizers: set[str] = set()

    def drop_finalizers(response):
        # The finalizers of one batch vanish before FINALIZE_MOVE: REJECTED
        if not finalizers and any(o.finalizer_id for o in response.items):
            for outcome in response.items:
                finalizers.add(outcome.finalizer_id)
                fake._drop_pin(outcome.finalizer_id)
        return response

    fake.rewrite_responses(MessageType.COMMIT, drop_finalizers, count=1000)
    for i in range(10):
        assert put(h, f"k{i}")
    wait_until(lambda: node.manager.move_results["REJECTED"] >= 1, what="rejected")
    rejected = [i for i in finalized(fake) if i.finalizer_id in finalizers]
    assert len(rejected) == node.manager.move_results["REJECTED"]
    for item in rejected:
        # retired, then freed: the page left that generation
        wait_until(
            lambda item=item: node.pools.find(item.dest) is None, what="copy freed"
        )
        assert item.dest not in fake.locations(item.key)
    assert node.manager.retire_results["RETIRED"] >= len(rejected)
    # the sources stayed, and moved on a later try
    wait_until(
        lambda: all(kinds(fake, i.key.key) == {PoolKind.CXL} for i in rejected),
        what="moved later",
    )
    node.pools.check()


def test_an_invalid_pool_below_keeps_the_source_and_dram_credit(fake, nodes, handlers):
    """KEPT(INVALID) names the invalid pool: here the CXL one, so DRAM keeps
    giving credit and the sources stay."""
    node = nodes(cxl_pages=8, region_pages=8)
    h = handlers(node)
    (cxl,) = cxl_ids(node)

    def hide_cxl(response):
        if any(o.finalizer_id for o in response.items):
            with fake._lock:
                fake.hidden_pools.add(cxl)
        return response

    fake.rewrite_responses(MessageType.COMMIT, hide_cxl, count=1000)
    for i in range(10):
        assert put(h, f"k{i}")
    wait_until(
        lambda: node.manager.move_results["KEPT:INVALID"] >= 1, what="invalid below"
    )
    assert cxl in node.manager._invalid_pools
    assert not node.pools.creditable(cxl)
    dram = next(p for p in node.pools.pool_ids() if p.startswith("dram:"))
    assert node.pools.creditable(dram)
    assert any(PoolKind.DRAM in kinds(fake, f"k{i}") for i in range(10))
    node.pools.check()


def test_a_failed_copy_frees_its_destination(fake, nodes, handlers, regions):
    node = nodes(cxl_pages=8, region_pages=8)
    (pool_id,) = cxl_ids(node)
    with node.transfers._lock:
        node.transfers._maps[pool_id].close()  # every copy into it fails
    h = handlers(node)
    for i in range(10):
        put(h, f"k{i}")
    wait_until(
        lambda: node.manager.move_results["COPY_FAILED"] >= 3, what="failed copies"
    )
    node.stop()
    assert node.pools.bytes_in(PageState.FREE, pool_id) == 8 * PAGE
    assert node.pools.transitions[(PageState.TRANSFER, PageState.FREE)] >= 3
    assert not finalized(fake)
    assert all(kinds(fake, f"k{i}") <= {PoolKind.DRAM} for i in range(10))
    node.pools.check()


def test_no_demotion_once_the_run_ended(fake, nodes, handlers):
    node = nodes(high=0.9, low=0.8)
    h = handlers(node)
    for i in range(8):
        assert put(h, f"k{i}")
    assert not node.manager.move_results
    node.manager._on_end = lambda reason: None  # the run is not stopped here
    node.manager.end_run("test")
    node.manager.placement.high, node.manager.placement.low = 0.3, 0.1  # pressure
    time.sleep(0.5)
    assert not node.manager.move_results
    assert cxl_bytes(node, PageState.FREE) == 16 * PAGE


def test_a_full_cxl_drops_its_own_pages(fake, nodes, handlers):
    node = nodes(cxl_pages=4, region_pages=4)
    h = handlers(node)
    for i in range(16):
        assert put(h, f"k{i}")
    wait_until(
        lambda: any(
            i.location.kind == PoolKind.CXL
            for r in fake.requests(MessageType.RETIRE)
            for i in r.items
        ),
        what="a CXL page retired",
    )
    assert node.manager.move_results["RETIRED"] >= 4
    assert node.pools.transitions[(PageState.RETIRING, PageState.FREE)] >= 1
    node.pools.check()


def test_with_no_room_below_dram_drops_past_its_high_watermark(fake, nodes, handlers):
    node = nodes(cxl_pages=4, region_pages=4)
    for i in range(4):
        fake.preserve(Key(NS, f"k{i}"))  # CXL cannot drop these
    h = handlers(node)
    for i in range(16):
        assert put(h, f"k{i}")
    wait_until(lambda: node.manager.move_results["DROPPED"], what="DRAM dropped")
    dropped = [
        i.key.key
        for r in fake.requests(MessageType.RETIRE)
        for i in r.items
        if i.location.kind == PoolKind.DRAM
    ]
    assert dropped
    assert cxl_bytes(node, PageState.FREE) == 0
    assert any(not kinds(fake, k) for k in dropped)  # gone, not stalled
    assert put(h, "after")  # writes go on
    node.pools.check()


def test_a_region_it_cannot_map_takes_no_demotions(fake, nodes, handlers, regions):
    regions.refuse = True
    node = nodes(high=0.5, low=0.25)
    assert node.transfers is not None
    assert not any(node.transfers.has_pool(p) for p in cxl_ids(node))
    h = handlers(node)
    for i in range(12):
        assert put(h, f"k{i}")
    wait_until(lambda: node.manager.move_results["DROPPED"], what="dropped instead")
    assert cxl_bytes(node, PageState.FREE) == 16 * PAGE


def test_the_budget_reaches_the_engine(nodes):
    node = nodes(transfer_bytes_per_s=123 * PAGE)
    assert node.transfers.bytes_per_s == 123 * PAGE


def test_a_demotion_reserves_pages_of_the_tier_below_only(nodes):
    node = nodes()
    free = node.pools.bytes_in(PageState.FREE)
    refs = node.manager._reserve_below("cxl", 64)  # more than CXL has
    try:
        assert {r.pool_id for r in refs} == set(cxl_ids(node))
        assert len(refs) * PAGE == cxl_bytes(node, PageState.TRANSFER)
        assert (
            node.pools.bytes_in(PageState.TRANSFER, node.tier("dram").pool.pool_id) == 0
        )
    finally:
        for ref in refs:
            node.pools.transfer_failed(ref)
    assert node.pools.bytes_in(PageState.FREE) == free


def test_cxl_keeps_its_own_watermarks(nodes):
    node = nodes(high=0.5, low=0.25)  # the cxl tier's are 0.9 and 0.8
    placement = node.manager.placement
    assert not placement.above_high("cxl", capacity=100, free_bytes=15)
    assert placement.above_high("cxl", capacity=100, free_bytes=5)
    assert placement.above_high("dram", capacity=100, free_bytes=45)


def test_one_tier_nodes_have_no_engine(fake, sockdir):
    node = start_node(node_config(fake.url, f"{sockdir}/node.sock"))
    try:
        assert node.transfers is None and node.manager.transfers is None
    finally:
        node.stop()


def test_a_lost_finalize_move_reply_is_sent_again_as_the_same_op(fake, nodes, handlers):
    node = nodes()
    h = handlers(node)
    fake.drop_responses(MessageType.FINALIZE_MOVE)
    for i in range(10):
        assert put(h, f"k{i}")
    wait_until(lambda: fake.replayed[MessageType.FINALIZE_MOVE], what="a resend")
    first, again = fake.requests(MessageType.FINALIZE_MOVE)[:2]
    assert (first.op.seq, first.op.digest) == (again.op.seq, again.op.digest)
    wait_until(lambda: node.manager.move_results["RETIRED"] >= 3, what="demoted")
    assert not node.manager.move_results["UNKNOWN"]
    node.pools.check()


def test_a_new_run_demotes_again(fake, sockdir, regions, handlers):
    """After a MaruServer restart the next run in the process maps the
    regions for its copies again and demotes."""
    config = node_config(
        fake.url,
        f"{sockdir}/node.sock",
        pages=16,
        high=0.5,
        low=0.0625,
        cxl_pages=16,
        region_pages=8,
        budgets=SMALL_CREDIT,
    )
    cxl = dataclasses.replace(config.tiers[1], rm="127.0.0.1:1")
    config = dataclasses.replace(config, tiers=(config.tiers[0], cxl))
    rm = FakeRm()
    node = Node(
        config,
        lambda c: StorageService(
            c,
            rpc_timeout_s=0.3,
            evict_interval_s=0.02,
            kept_backoff_s=0.2,
            region_source=rm,
            cxl_renew_s=0.05,
            map_region=regions.node_map,
        ),
        pause_s=0.05,
    )
    thread = threading.Thread(target=node.run, daemon=True)
    thread.start()
    try:
        first = wait_until(lambda: serving(node, None), what="the first run")
        h = handlers(first)
        for i in range(10):
            assert put(h, f"k{i}")
        wait_until(lambda: first.manager.move_results["RETIRED"] >= 3, what="demoted")
        fake.restart()
        second = wait_until(lambda: serving(node, first), what="the next run")
        wait_until(lambda: h._cpu.node_epoch == second.node_epoch, what="re-attached")
        assert all(
            second.transfers.has_pool(p.pool_id) for p in second.tier("cxl").pools
        )
        for i in range(10, 20):
            assert put(h, f"k{i}")
        wait_until(
            lambda: second.manager.move_results["RETIRED"] >= 3, what="demoting again"
        )
        second.pools.check()
    finally:
        node.stop()
        thread.join(15)


def serving(node, old):
    service = node.service
    if (
        service is None
        or service is old
        or service.local is None
        or not service.local.started
        or service.wait(0)
    ):
        return None
    return service


# =============================================================================
# Review fixes: credit, preservation, MOVED, invalid pools, partial room
# =============================================================================


def test_copies_pending_below_do_not_hold_back_credit():
    pm = PoolManager(
        Budgets(
            credit_max_bytes=4 * PAGE,
            credit_total_max_bytes=4 * PAGE,
            credit_refill_low=PAGE,
            credit_refill_high=4 * PAGE,
            commit_pending_max_bytes=PAGE,
        ),
        PAGE,
    )
    for kind, pool_id in ((PoolKind.DRAM, "dram:n:e"), (PoolKind.CXL, "cxl:d:1")):
        pm.add_pool(
            PoolInfo(
                pool_id=pool_id,
                kind=kind,
                node_id="n",
                pool_generation=1,
                capacity=4 * PAGE,
                page_size=PAGE,
            ),
            creditable=kind == PoolKind.DRAM,
        )
    pm.set_epoch_seq(1)
    for ref in pm.reserve_transfer("cxl:d:1", 2):
        pm.transfer_done(ref, Key(NS, f"c{ref.index}"), 8)
    (credit,) = pm.credit("w", PAGE, PoolKind.DRAM)  # CXL PENDING is not counted
    pm.write_done("w", credit, Key(NS, "k"), 8)
    assert pm.credit("w", PAGE, PoolKind.DRAM) == []  # DRAM PENDING is
    pm.check()


def test_a_session_lost_on_a_demotion_commit_ends_the_run(
    fake, nodes, handlers, monkeypatch
):
    node = nodes(high=0.5, low=0.25)
    m = node.manager
    real_commit = m.metadata.commit

    def commit(items):
        if items[0].finalizer:
            raise EpochChanged("test: the session went")
        return real_commit(items)

    monkeypatch.setattr(m.metadata, "commit", commit)
    h = handlers(node)
    put_until_the_run_ends(node, h)
    assert node.wait_finished(10)
    assert "no longer knows the session" in node.ended and node.new_run
    assert not finalized(fake)


def test_a_copy_of_a_page_now_holding_another_key_is_retired(fake, nodes, handlers):
    node = nodes()
    h = handlers(node)
    moved: list = []

    def take_page(response):
        # Before FINALIZE_MOVE, another key takes the source page of the
        # first demoted key: KEPT(MOVED) names it
        if moved or not any(o.finalizer_id for o in response.items):
            return response
        item = fake.requests(MessageType.COMMIT)[-1].items[0]
        (source,) = [
            e for e in fake.catalog[item.key] if e.location.kind == PoolKind.DRAM
        ]
        fake._remove(source)
        occupant = dataclasses.replace(source.location, allocation_id="intruder")
        entry = type(source)(Key(NS, "intruder"), occupant, 8, source.node_id)
        fake.catalog.setdefault(entry.key, []).append(entry)
        fake.by_loc[(occupant.pool_id, occupant.allocation_id, occupant.generation)] = (
            entry
        )
        moved.append((item, occupant))
        return response

    fake.rewrite_responses(MessageType.COMMIT, take_page, count=1000)
    for i in range(10):
        assert put(h, f"k{i}")
    wait_until(lambda: node.manager.move_results["KEPT:MOVED"], what="MOVED")
    ((item, occupant),) = moved
    wait_until(lambda: not fake.locations(item.key), what="its copy retired")
    wait_until(lambda: node.pools.find(item.location) is None, what="copy freed")
    # the ledger took the occupant: it lives on (moved down itself, maybe)
    assert fake.locations(Key(NS, "intruder"))
    assert node.manager.move_results["KEPT:MOVED"] == 1
    node.pools.check()


def test_a_copy_in_an_invalid_pool_is_not_used(fake, nodes, handlers):
    node = nodes(low=0.0625, cxl_pages=32, region_pages=16)
    m = node.manager
    h = handlers(node)
    assert put(h, "k0")
    pinned = h.retrieve("k0")
    for i in range(1, 10):
        assert put(h, f"k{i}")
    wait_until(lambda: m.move_results["KEPT:PINNED"] >= 1, what="a KEPT move")
    ((src, copy),) = [(s, d) for s, d in m._copies.items()]
    assert m._copy_of(src, Key(NS, "k0")) is not None
    with m._lock:
        m._invalid_pools.add(copy.pool_id)
    assert m._copy_of(src, Key(NS, "k0")) is None
    h.release_retrieved([pinned])
    time.sleep(0.3)
    for i in range(10, 30):
        if PoolKind.DRAM not in kinds(fake, "k0"):
            break
        assert put(h, f"k{i}")
    wait_until(lambda: PoolKind.DRAM not in kinds(fake, "k0"), what="moved")
    first, again = [i.dest for i in finalized(fake) if i.key.key == "k0"]
    assert first.pool_id == copy.pool_id != again.pool_id  # a new copy elsewhere


def test_an_invalid_pool_is_not_evicted(fake, nodes, handlers):
    node = nodes(cxl_pages=8, region_pages=8)
    m = node.manager
    (cxl,) = cxl_ids(node)
    h = handlers(node)
    for i in range(10):
        assert put(h, f"k{i}")
    wait_until(lambda: cxl_bytes(node, PageState.READY) >= 3 * PAGE, what="moved")

    def cxl_retires() -> int:
        return sum(
            i.location.pool_id == cxl
            for r in fake.requests(MessageType.RETIRE)
            for i in r.items
        )

    with m._lock:
        m._invalid_pools.add(cxl)
    # A pass that began before the mark may still retire: let it end first
    wait_until(lambda: not m._retire_queue, what="nothing queued")
    time.sleep(0.2)  # several passes at evict_interval_s
    m.placement.set_watermarks("cxl", 0.2, 0.1)  # it would evict now
    before = cxl_retires()
    time.sleep(0.5)
    assert cxl_retires() == before
    with m._lock:
        m._invalid_pools.discard(cxl)
    wait_until(lambda: cxl_retires() > before, what="evicted once valid")


def test_past_high_the_pages_without_room_are_dropped_while_others_move(
    fake, nodes, handlers, monkeypatch
):
    node = nodes(low=0.0625, cxl_pages=4, region_pages=4)
    m = node.manager
    passes: list[tuple[int, int]] = []
    real = m._demote

    def demote(kind, pools, below, refs, items):
        added, dropped = m.move_results["COMMIT:ADDED"], m.move_results["DROPPED"]
        real(kind, pools, below, refs, items)
        passes.append(
            (
                m.move_results["COMMIT:ADDED"] - added,
                m.move_results["DROPPED"] - dropped,
            )
        )

    monkeypatch.setattr(m, "_demote", demote)
    h = handlers(node)
    for i in range(12):
        assert put(h, f"k{i}")
    wait_until(
        lambda: any(a and d for a, d in passes), what="a pass that moved and dropped"
    )
    node.pools.check()


# =============================================================================
# A demotion whose result stays unknown ends the run
# =============================================================================


def test_an_unusable_commit_reply_ends_the_run(fake, nodes, handlers):
    node = nodes(low=0.0625)
    h = handlers(node)
    lost: list = []

    def unusable(response):
        # One result short: ReplyError, while the copies were ADDED with
        # finalizers whose ids are unknown
        if not lost and any(o.finalizer_id for o in response.items):
            lost.extend(fake.requests(MessageType.COMMIT)[-1].items)
            return CommitResponse(status=Status.OK, items=response.items[:-1])
        return response

    fake.rewrite_responses(MessageType.COMMIT, unusable, count=1000)
    put_until_the_run_ends(node, h)
    assert node.wait_finished(10)
    assert "result unknown" in node.ended and node.new_run
    assert not finalized(fake)
    assert [c.mode.value for c in fake.requests(MessageType.NODE_CLOSE)] == ["restart"]


def test_a_finalize_move_refused_as_a_whole_ends_the_run(fake, nodes, handlers):
    """Not run, but the copies' finalizers stay until the session ends."""
    node = nodes(low=0.0625)
    h = handlers(node)
    fake.fail_requests(MessageType.FINALIZE_MOVE, Status.REJECTED)
    put_until_the_run_ends(node, h)
    assert node.wait_finished(10)
    assert "refused as a whole" in node.ended
    refused = fake.requests(MessageType.FINALIZE_MOVE)[0].items
    assert node.manager.move_results["UNKNOWN"] == len(refused)
    for item in refused:  # both stay published
        assert item.source in fake.locations(item.key)


# =============================================================================
# Credit from CXL when DRAM lacks room
# =============================================================================


def test_a_worker_that_cannot_map_cxl_takes_dram_credit_only(
    nodes, handlers, monkeypatch
):
    node = nodes(pages=8, cxl_pages=16, region_pages=8)

    def no_dax(*args, **kwargs):
        raise OSError("no dax in this container")

    monkeypatch.setattr(pool_maps, "CxlRegionMapping", no_dax)
    h = handlers(node)
    client = h._cpu
    assert client._credit_kind == PoolKind.DRAM
    client.want_credit(8)  # past the 2-page cap: DRAM only, whatever is short
    wait_until(lambda: client.credits.unused_bytes == 2 * PAGE, what="credit")
    assert {c.kind for c in client.credits.take_all()} == {PoolKind.DRAM}


def test_credit_of_a_pool_the_worker_did_not_map_goes_back(
    nodes, handlers, monkeypatch
):
    # DRAM gives 1 page past its reserve of 1: the rest is CXL
    node = nodes(pages=2, cxl_pages=16, region_pages=8)

    def no_dax(*args, **kwargs):
        raise OSError("no dax in this container")

    monkeypatch.setattr(pool_maps, "CxlRegionMapping", no_dax)
    h = handlers(node)
    client = h._cpu
    client._credit_kind = None  # as a node of only fallback tiers would ask
    client.want_credit(2)
    wait_until(lambda: client.stats["credit_unmapped"] >= 1, what="a return")
    assert {c.kind for c in client.credits.take_all()} <= {PoolKind.DRAM}
    incarnation = client.identity.incarnation
    cxl = [p.pool_id for p in node.tier("cxl").pools]
    wait_until(
        lambda: sum(node.pools.bytes_in(PageState.CREDITED, p) for p in cxl) == 0,
        what="the CXL credit back",
    )
    assert node.pools.held_bytes(incarnation) <= PAGE


def _stale(response):
    response.items = [ItemOutcome(ItemResult.STALE)] * len(response.items)
    return response


def test_an_unknown_finalize_move_result_ends_the_run(fake, nodes, handlers):
    node = nodes(low=0.0625)
    h = handlers(node)
    fake.rewrite_responses(MessageType.FINALIZE_MOVE, _stale)
    put_until_the_run_ends(node, h)
    assert node.wait_finished(10)
    assert "FINALIZE_MOVE" in node.ended and node.new_run
    assert node.manager.move_results["UNKNOWN"] >= 1


def test_an_unknown_commit_result_of_a_demoted_copy_ends_the_run(fake, nodes, handlers):
    node = nodes(low=0.0625)
    h = handlers(node)
    first: list = []

    def stale_copies(response):
        # Only the COMMIT of demoted copies: those carry a finalizer
        request = fake.requests(MessageType.COMMIT)[-1]
        if request.items[0].finalizer:
            first.append(request)
            return _stale(response)
        return response

    fake.rewrite_responses(MessageType.COMMIT, stale_copies, count=1000)
    put_until_the_run_ends(node, h)
    assert node.wait_finished(10) and first
    assert "COMMIT of demoted page" in node.ended and node.new_run
    assert not finalized(fake)
