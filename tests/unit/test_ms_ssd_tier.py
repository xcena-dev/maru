# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""The SSD tier: [dram, ssd], [cxl, ssd] and [dram, cxl, ssd] nodes demote
into one locked file through the I/O executor (write, then COMMIT with a
finalizer, then FINALIZE_MOVE). A restart on the same boot adopts the file;
a reboot starts it empty. Workers do not fetch yet. The files live under
/var/tmp, a local filesystem with direct I/O."""

import dataclasses
import errno
import fcntl
import mmap
import os
import shutil
import socket
import tempfile
import threading
import time

import pytest

from maru_common.protocol import MessageType
from maru_common.storage_types import (
    BlockRequest,
    CloseMode,
    Key,
    KeyLocation,
    Location,
    PoolKind,
)
from maru_handler.client import pool_maps
from maru_server.catalog import PoolState
from maru_server.rpc_async_server import RpcAsyncServer
from maru_server.server import MaruServer
from maru_storage_service.config import parse_node_config
from maru_storage_service.drivers.io_executor import IOQueueFullError
from maru_storage_service.drivers.ssd import (
    SsdPool,
    SsdPoolError,
    SsdTier,
    create_ssd_pool,
)
from maru_storage_service.drivers.ssd_alignment import aligned_buffer
from maru_storage_service.page_table import PageRef
from maru_storage_service.pool_manager import PageState
from maru_storage_service.service import (
    Node,
    NotServableError,
    StorageService,
    check_servable,
)
from maru_storage_service.transfer_engine import Copy, TransferEngine
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import (
    PAGE,
    RawWorker,
    node_config,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)
from tests.unit.test_ms_cxl_tier import FakeRm
from tests.unit.test_ms_demotion import (
    SMALL_CREDIT,
    Regions,
    finalized,
    kinds,
    put,
    value_of,
)
from tests.unit.test_ms_new_run import SERVICE_OPTIONS, new_run

NS = "ns"
SSD_ROOT = "/var/tmp"


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
def ssd_dir():
    path = tempfile.mkdtemp(prefix="maru-ssd-tier-", dir=SSD_ROOT)
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def regions(monkeypatch):
    stand_in = Regions()
    monkeypatch.setattr(pool_maps, "CxlRegionMapping", stand_in.worker_map)
    yield stand_in
    stand_in.close()


def dram_ssd(url, sockdir, ssd_dir, *, high=0.5, low=0.25, cxl_pages=None):
    config = node_config(
        url,
        f"{sockdir}/node.sock",
        pages=16,
        high=high,
        low=low,
        cxl_pages=cxl_pages,
        region_pages=8,
        budgets=SMALL_CREDIT,
        ssd_path=ssd_dir,
        ssd_pages=32,
    )
    if cxl_pages is None:
        return config
    dram, cxl, ssd = config.tiers
    # Never an RM of this host; CXL keeps its own watermarks
    cxl = dataclasses.replace(
        cxl, rm="127.0.0.1:1", high_watermark=0.5, low_watermark=0.25
    )
    return dataclasses.replace(config, tiers=(dram, cxl, ssd))


def cxl_ssd(url, sockdir, ssd_dir):
    return parse_node_config(
        {
            "node_id": "gpu-test",
            "socket": f"{sockdir}/node.sock",
            "server": url,
            "page_size": PAGE,
            "tiers": [
                {
                    "store": "cxl",
                    "size": 16 * PAGE,
                    "region_size": 8 * PAGE,
                    "rm": "127.0.0.1:1",
                    "high_watermark": 0.5,
                    "low_watermark": 0.25,
                },
                {"store": "ssd", "size": 32 * PAGE, "path": ssd_dir},
            ],
            "budgets": SMALL_CREDIT | {"transfer_reserve_bytes": PAGE},
        }
    )


@pytest.fixture
def nodes(fake, sockdir, ssd_dir, regions):
    started = []

    def make(config=None, **kwargs):
        options = {
            "region_source": FakeRm(),
            "cxl_renew_s": 0.05,
            "map_region": regions.node_map,
        }
        options.update(kwargs)
        node = start_node(config or dram_ssd(fake.url, sockdir, ssd_dir), **options)
        started.append(node)
        return node

    yield make
    for node in started:
        node.stop()


@pytest.fixture
def handlers():
    from maru_common.config import MaruConfig
    from maru_handler import MaruHandler

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


def ssd_bytes(pool: SsdPool, location: Location) -> bytes:
    with aligned_buffer(pool.io_size(location.length), pool.alignment.memory) as buf:
        pool.read(location.offset, location.length, buf, deadline=time.monotonic() + 5)
        return bytes(buf[: location.length])


def ssd_commits(fake) -> list:
    return [
        i
        for r in fake.requests(MessageType.COMMIT)
        for i in r.items
        if i.location.kind == PoolKind.SSD
    ]


def no_io(node) -> bool:
    return all(
        node.pools.io_refs(PageRef(pool_id, i, 0)) == 0
        for pool_id in node.pools.pool_ids()
        for i in range(node.pools.pool_info(pool_id).capacity // PAGE)
    )


# =============================================================================
# Config and traits
# =============================================================================


def _config(stores, **top):
    tiers = []
    for store in stores:
        tier = {"store": store, "size": 16 * PAGE}
        if store == "cxl":
            tier["region_size"] = 8 * PAGE
        if store == "ssd":
            tier["path"] = "/var/tmp/unused"
        if store == "remote":
            tier = {"store": "remote", "url": "tcp://mem:1"}
        tiers.append(tier)
    tiers[0].update(top)
    return parse_node_config(
        {
            "node_id": "n",
            "server": "tcp://127.0.0.1:1",
            "page_size": PAGE,
            "tiers": tiers,
            "budgets": SMALL_CREDIT | {"transfer_reserve_bytes": PAGE},
        }
    )


@pytest.mark.parametrize(
    "stores, demotes",
    [
        (["dram", "ssd"], {PoolKind.DRAM: (PoolKind.SSD,)}),
        (["cxl", "ssd"], {PoolKind.CXL: (PoolKind.SSD,)}),
        (
            ["dram", "cxl", "ssd"],
            {PoolKind.DRAM: (PoolKind.CXL,), PoolKind.CXL: (PoolKind.SSD,)},
        ),
    ],
)
def test_the_tiers_over_an_ssd_demote_into_it(stores, demotes):
    config = _config(stores)
    check_servable(config)
    traits = config.traits()
    assert {k: t.demotes_to for k, t in traits.items() if t.demotes_to} == demotes
    ssd = traits.pop(PoolKind.SSD)
    assert (ssd.credit, ssd.credit_fallback, ssd.evicts, ssd.demotes_to) == (
        False,
        False,
        True,
        (),
    )
    assert ssd.io_only and not any(t.io_only for t in traits.values())


def test_other_ssd_configs_are_refused():
    check_servable(_config(["dram", "ssd"], demotion="through"))
    with pytest.raises(NotServableError, match="not implemented"):
        check_servable(_config(["dram", "ssd", "remote"]))
    # A memory node serves the same tiers
    memory = dataclasses.replace(_config(["dram", "ssd"]), role="memory")
    check_servable(memory)
    check_servable(
        dataclasses.replace(_config(["dram", "ssd"], demotion="through"), role="memory")
    )


# =============================================================================
# TransferEngine with a file pool
# =============================================================================


def test_the_engine_writes_and_reads_a_file_pool(ssd_dir):
    pool = create_ssd_pool("n", ssd_dir, 4 * PAGE, PAGE)
    engine = TransferEngine(1 << 40)
    memory = mmap.mmap(-1, 4 * PAGE)
    try:
        engine.add_pool("mem", memory)
        engine.add_l2(SsdTier(pool, run="run-1", bytes_per_s=1 << 40))
        engine.start()
        assert engine.has_pool(pool.pool_id) and engine.has_pool("mem")
        memory[PAGE : PAGE + 5000] = b"w" * 5000

        def loc(pool_id, offset, length=PAGE):
            return Location(pool_id, "a", 1, offset, length, PoolKind.SSD)

        mem_ref, file_ref = PageRef("mem", 1, 1), PageRef(pool.pool_id, 2, 1)
        done = []
        write = engine.submit_io(
            Copy(loc("mem", PAGE, 5000), loc(pool.pool_id, 2 * PAGE), 5000),
            mem_ref,
            file_ref,
            callback=done.append,
        )
        assert write.done.wait(5) and write.ok and write.write
        assert write.run == "run-1"
        assert done == [write]
        assert ssd_bytes(pool, loc(pool.pool_id, 2 * PAGE, 5000)) == b"w" * 5000
        read = engine.submit_io(
            Copy(loc(pool.pool_id, 2 * PAGE), loc("mem", 3 * PAGE), 5000),
            file_ref,
            PageRef("mem", 3, 1),
        )
        assert read.done.wait(5) and read.ok and not read.write
        assert memory[3 * PAGE : 3 * PAGE + 5000] == b"w" * 5000
        # a deadline already past moves nothing
        late = engine.submit_io(
            Copy(loc(pool.pool_id, 2 * PAGE), loc("mem", 0), 5000),
            file_ref,
            PageRef("mem", 0, 1),
            deadline=time.monotonic() - 1,
        )
        assert late.done.wait(5) and not late.ok
        assert isinstance(late.error, TimeoutError)
        assert memory[:5000] == b"\0" * 5000
        # not a file pool and a mapped pool
        with pytest.raises(ValueError):
            engine.submit_io(Copy(loc("mem", 0), loc("mem", PAGE), 5), mem_ref, mem_ref)
        engine.close()
        # the file pool stays open and locked for its owner until it closes it
        assert pool.fd >= 0 and os.fstat(pool.fd).st_size > 0
        assert ssd_bytes(pool, loc(pool.pool_id, 2 * PAGE, 5000)) == b"w" * 5000
        other = os.open(pool.path, os.O_RDONLY | os.O_CLOEXEC)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(other)
    finally:
        engine.close()
        pool.close()
    assert pool.fd == -1


# =============================================================================
# Demotion into the SSD
# =============================================================================


def test_a_full_dram_moves_its_pages_to_the_ssd(fake, nodes, handlers):
    node = nodes()
    assert node.tier("ssd").pool is not None and node.transfers is not None
    ssd = node.tier("ssd").pool.pool_id
    assert not node.pools.creditable(ssd)
    assert ssd not in {d.pool.pool_id for d, _ in node.local.pools}  # not to workers
    h = handlers(node)
    for i in range(10):
        assert put(h, f"k{i}")
    wait_until(lambda: node.manager.move_results["RETIRED"] >= 3, what="pages demoted")
    moved = [f"k{i}" for i in range(10) if kinds(fake, f"k{i}") == {PoolKind.SSD}]
    assert moved
    for key in moved:
        (location,) = fake.locations(Key(NS, key))
        ref = node.pools.find(location)
        assert ref is not None and node.pools.page(ref).state == PageState.READY
        assert ssd_bytes(node.tier("ssd").pool, location) == value_of(key)
        assert h.retrieve(key) is None  # workers do not fetch yet
    assert all(kinds(fake, f"k{i}") for i in range(10))
    for item in finalized(fake):
        assert (item.source.kind, item.dest.kind) == (PoolKind.DRAM, PoolKind.SSD)
    assert all(i.finalizer for i in ssd_commits(fake))
    wait_until(lambda: fake.pin_count() == 0, what="every finalizer released")
    # A later demotion's write may still run
    wait_until(lambda: no_io(node), what="every write ended")
    node.pools.check()


def test_a_copy_is_published_only_once_its_write_returned(
    fake, nodes, handlers, monkeypatch
):
    node = nodes()
    gate, entered = threading.Event(), threading.Event()
    write = node.tier("ssd").pool.write

    def held(*args, **kwargs):
        entered.set()
        assert gate.wait(10)
        return write(*args, **kwargs)

    monkeypatch.setattr(node.tier("ssd").pool, "write", held)
    h = handlers(node)
    for i in range(10):
        assert put(h, f"k{i}")
        if entered.is_set():
            break
    wait_until(entered.is_set, what="a write under way")
    time.sleep(0.1)
    # under way: the page below is reserved, referenced and not committed
    (dest,) = node.pools.pages_in(node.tier("ssd").pool.pool_id, PageState.TRANSFER)[:1]
    assert node.pools.io_refs(dest) == 1
    assert not ssd_commits(fake)
    # the eviction thread goes on meanwhile: a central delete gets done
    idle = [
        (name, loc)
        for name in [f"k{j}" for j in range(10)]
        for loc in fake.locations(Key(NS, name))
        if loc.kind == PoolKind.DRAM
        and (ref := node.pools.find(loc)) is not None
        and not node.pools.io_refs(ref)
    ]
    name, loc = idle[0]
    fake.queue_block(
        "gpu-test",
        BlockRequest("blk-held", (KeyLocation(Key(NS, name), loc),), "admin"),
    )
    wait_until(lambda: Key(NS, name) not in fake.keys(), what="deleted meanwhile")
    assert not ssd_commits(fake)
    gate.set()
    wait_until(lambda: node.manager.move_results["RETIRED"] >= 1, what="moved")
    assert ssd_commits(fake)
    wait_until(lambda: no_io(node), what="every write ended")
    node.pools.check()


def torn_writes(node, code: int, count: float = float("inf")) -> list[int]:
    """The next ``count`` writes land half their bytes, then fail with
    ``code``; returns the page offsets they tore."""
    write = node.tier("ssd").pool.write
    torn: list[int] = []

    def tear(page_offset, src, n, **kwargs):
        if len(torn) >= count:
            return write(page_offset, src, n, **kwargs)
        torn.append(page_offset)
        write(page_offset, src, max(1, n // 2), **kwargs)
        raise OSError(code, os.strerror(code))

    node.tier("ssd").pool.write = tear
    return torn


def served_right(fake, node, h, names) -> None:
    """Every key reads its own bytes, from DRAM or the SSD, or misses."""
    for name in names:
        for location in fake.locations(Key(NS, name)):
            if location.kind == PoolKind.SSD:
                pool = node.tier("ssd").pool
                assert ssd_bytes(pool, location) == value_of(name), name
        read = h.retrieve(name)
        if read is not None:
            assert bytes(read.view) == value_of(name), name
            h.release_retrieved([read])


@pytest.mark.parametrize("code", [errno.ENOSPC, errno.EDQUOT, errno.EIO])
def test_a_failed_write_publishes_nothing_and_its_source_waits(
    fake, nodes, handlers, code
):
    node = nodes()
    torn = torn_writes(node, code, count=1)
    h = handlers(node)
    names = [f"k{i}" for i in range(10)]
    for name in names:
        assert put(h, name)
    wait_until(lambda: torn, what="a torn write")
    # its source stays and moves on a later try; the torn bytes never count
    wait_until(lambda: node.manager.move_results["RETIRED"] >= 3, what="moved")
    assert node.manager.move_results["COPY_FAILED"] == 1
    assert not node.manager.move_results["DROPPED"]
    assert all(kinds(fake, n) for n in names)
    served_right(fake, node, h, names)
    node.pools.check()


def test_a_persistent_write_failure_lets_stores_go_on(
    fake, nodes, handlers, monkeypatch
):
    """Writes that keep failing pause the SSD: DRAM drops past its high
    watermark (RETIRE), so stores keep coming. Nothing torn is published;
    once the pause is over, pages move to the SSD again."""
    monkeypatch.setattr("maru_storage_service.storage_manager.IO_PAUSE_S", 0.5)
    node = nodes()
    torn = torn_writes(node, errno.EIO)
    h = handlers(node)
    names = [f"k{i}" for i in range(40)]  # DRAM holds 16 pages
    for name in names:
        assert put(h, name)
    assert len(torn) >= 3 and node.manager.move_results["DROPPED"] > 0
    assert not ssd_commits(fake) and not finalized(fake)
    served_right(fake, node, h, names)
    node.pools.check()
    del node.tier("ssd").pool.write  # the disk is back
    for name in [f"n{i}" for i in range(10)]:
        assert put(h, name)
    wait_until(lambda: finalized(fake), what="moves to the SSD again")
    served_right(fake, node, h, names + [f"n{i}" for i in range(10)])
    node.pools.check()


@pytest.mark.parametrize("how", ["queue_full", "late"])
def test_full_queues_and_late_writes_count_toward_the_pause(
    fake, nodes, handlers, monkeypatch, how
):
    """A write the executor did not take, or one that moved every byte past
    its deadline, counts as a failure: three in a row pause the SSD. A late
    one is still published."""
    monkeypatch.setattr("maru_storage_service.storage_manager.IO_PAUSE_S", 60.0)
    node = nodes()
    if how == "queue_full":

        def full(*args, **kwargs):
            raise IOQueueFullError("injected")

        monkeypatch.setattr(node.transfers, "submit_io", full)
    else:
        monkeypatch.setattr("maru_storage_service.drivers.ssd.IO_TIMEOUT_S", 0.05)
        write = node.tier("ssd").pool.write

        def slow(*args, **kwargs):
            done = write(*args, **kwargs)
            time.sleep(0.1)
            return done

        monkeypatch.setattr(node.tier("ssd").pool, "write", slow)
    h = handlers(node)
    names = [f"k{i}" for i in range(40)]
    for name in names:
        assert put(h, name)
    wait_until(
        lambda: node.tier("ssd").pool.pool_id in node.manager._io_paused,
        what="the SSD paused",
    )
    assert bool(ssd_commits(fake)) == (how == "late")
    served_right(fake, node, h, names)
    node.pools.check()


def test_a_retired_ssd_page_is_not_reused_while_an_io_runs(fake, nodes, handlers):
    node = nodes()
    h = handlers(node)
    for i in range(10):
        assert put(h, f"k{i}")
    wait_until(lambda: node.manager.move_results["RETIRED"] >= 1, what="moved")
    key = next(f"k{i}" for i in range(10) if kinds(fake, f"k{i}") == {PoolKind.SSD})
    (location,) = fake.locations(Key(NS, key))
    ref = node.pools.find(location)
    # as a fetch reading it into DRAM would
    (dest,) = node.pools.reserve_transfer(node.tier("dram").pool.pool_id, 1)
    assert node.pools.io_acquire(ref, dest)
    fake.queue_block(
        "gpu-test",
        BlockRequest(
            block_id="blk-1",
            items=(KeyLocation(Key(NS, key), location),),
            reason="admin delete",
        ),
    )
    wait_until(lambda: Key(NS, key) not in fake.keys(), what="retired centrally")
    wait_until(
        lambda: node.pools.page(ref).state == PageState.RETIRING, what="retiring"
    )
    time.sleep(0.2)  # eviction passes go on
    assert node.pools.page(ref).state == PageState.RETIRING
    node.pools.io_release([ref, dest])
    node.pools.transfer_failed(dest)
    wait_until(
        lambda: (page := node.pools.page(ref)) is None or page.state == PageState.FREE,
        what="reclaimed",
    )
    node.pools.check()


def test_a_demotion_into_the_ssd_reserves_ssd_pages_only(nodes):
    node = nodes()
    ssd = node.tier("ssd").pool.pool_id
    refs = node.manager._reserve_below("ssd", 64)  # more than the SSD has
    try:
        assert refs and {r.pool_id for r in refs} == {ssd}
        assert node.pools.bytes_in(PageState.TRANSFER) == len(refs) * PAGE
        assert node.pools.bytes_in(PageState.FREE, ssd) == 0
    finally:
        for ref in refs:
            node.pools.transfer_failed(ref)


def test_a_fetch_demand_makes_room_below_the_watermarks(
    fake, nodes, handlers, sockdir, ssd_dir
):
    node = nodes(dram_ssd(fake.url, sockdir, ssd_dir, high=0.9, low=0.8))
    h = handlers(node)
    for i in range(4):
        assert put(h, f"k{i}")
    dram = node.tier("dram").pool.pool_id
    time.sleep(0.2)
    assert not node.manager.move_results  # far below the high watermark
    free = node.pools.bytes_in(PageState.FREE, dram)
    node.manager.want_room(PoolKind.DRAM, free + 2 * PAGE)
    wait_until(lambda: node.manager.move_results["RETIRED"] >= 2, what="demand moves")
    time.sleep(0.2)
    # exactly the demand, oldest first
    assert node.manager.move_results["RETIRED"] == 2
    assert [kinds(fake, f"k{i}") for i in range(4)] == [{PoolKind.SSD}] * 2 + [
        {PoolKind.DRAM}
    ] * 2
    node.pools.check()


def test_dram_moves_to_cxl_and_cxl_to_the_ssd(fake, nodes, handlers, sockdir, ssd_dir):
    node = nodes(dram_ssd(fake.url, sockdir, ssd_dir, cxl_pages=8))
    h = handlers(node)
    for i in range(20):
        assert put(h, f"k{i}")
        if any(i.dest.kind == PoolKind.SSD for i in finalized(fake)):
            break
    wait_until(
        lambda: any(i.dest.kind == PoolKind.SSD for i in finalized(fake)),
        what="a page moved from CXL to the SSD",
    )
    pairs = {(i.source.kind, i.dest.kind) for i in finalized(fake)}
    assert pairs <= {(PoolKind.DRAM, PoolKind.CXL), (PoolKind.CXL, PoolKind.SSD)}
    assert (PoolKind.CXL, PoolKind.SSD) in pairs
    for item in finalized(fake):
        if item.dest.kind == PoolKind.SSD and fake.locations(item.key) == [item.dest]:
            assert ssd_bytes(node.tier("ssd").pool, item.dest) == value_of(item.key.key)
    node.pools.check()


def test_a_cxl_top_tier_moves_to_the_ssd(fake, nodes, regions, sockdir, ssd_dir):
    node = nodes(cxl_ssd(fake.url, sockdir, ssd_dir))
    assert node.tier("dram") is None
    by_pool = {p.pool_id: p.region.handle.region_id for p in node.tier("cxl").pools}
    worker = RawWorker(node.config.socket)
    worker.attach()
    written = 0
    while written < 12:
        credits = worker.credit(2 * PAGE).credits
        assert all(c.kind == PoolKind.CXL for c in credits)
        for credit in credits:
            # what a worker writes into its mapping of the region
            fd = regions.fds[by_pool[credit.pool_id]]
            os.pwrite(fd, value_of(f"k{written}"), credit.offset)
            keys = [f"k{written}"]
            size = len(value_of(keys[0]))
            assert worker.write_done([credit], keys, size).status.value == "OK"
            written += 1
    wait_until(lambda: finalized(fake), what="a page moved to the SSD")
    for item in finalized(fake):
        assert (item.source.kind, item.dest.kind) == (PoolKind.CXL, PoolKind.SSD)
        assert ssd_bytes(node.tier("ssd").pool, item.dest) == value_of(item.key.key)
    worker.close()
    node.pools.check()


def test_a_region_the_node_cannot_map_drops_its_pages(
    fake, nodes, regions, sockdir, ssd_dir
):
    """Its pages are never copied out: eviction retires them instead of
    trying again forever."""
    mapped = []

    def map_some(pool):
        mapped.append(pool.pool_id)
        mapping = regions.node_map(pool)  # the region's bytes, for the worker
        if len(mapped) == 1:
            mapping.close()
            raise OSError(errno.EIO, "injected")
        return mapping

    node = nodes(cxl_ssd(fake.url, sockdir, ssd_dir), map_region=map_some)
    unmapped = mapped[0]
    by_pool = {p.pool_id: p.region.handle.region_id for p in node.tier("cxl").pools}
    worker = RawWorker(node.config.socket)
    worker.attach()
    names = []
    while len(names) < 12:
        for credit in worker.credit(2 * PAGE).credits:
            name = f"k{len(names)}"
            fd = regions.fds[by_pool[credit.pool_id]]
            os.pwrite(fd, value_of(name), credit.offset)
            assert worker.write_done([credit], [name], len(value_of(name)))
            names.append(name)
    wait_until(lambda: node.manager.retire_results["RETIRED"] >= 1, what="dropped")
    assert not node.manager.move_results["COPY_FAILED"]
    assert all(i.source.pool_id != unmapped for i in finalized(fake))
    worker.close()
    node.pools.check()


# =============================================================================
# Runs: a new run in the process, a restart on the same boot, a reboot
# =============================================================================


def test_a_new_run_opens_the_file_once_the_old_runs_writes_ended(
    fake, sockdir, ssd_dir, handlers, monkeypatch
):
    config = dram_ssd(fake.url, sockdir, ssd_dir)
    node = Node(config, lambda c: StorageService(c, **SERVICE_OPTIONS), pause_s=0.05)
    thread = threading.Thread(target=node.run, daemon=True)
    thread.start()
    gate, entered = threading.Event(), threading.Event()
    write = SsdPool.write

    def held(self, *args, **kwargs):
        entered.set()
        assert gate.wait(10)
        return write(self, *args, **kwargs)

    try:
        first = new_run(node, None)
        monkeypatch.setattr(SsdPool, "write", held)
        h = handlers(first)
        for i in range(10):
            assert put(h, f"k{i}")
            if entered.is_set():
                break
        wait_until(entered.is_set, what="a write under way")
        fake.restart()  # MaruServer lost the run: it ends
        wait_until(lambda: first.wait(0), what="the run ends")
        time.sleep(0.5)
        # its teardown waits for the write; the file stays open and locked
        assert node.service is first and first.tier("ssd").pool.fd >= 0
        gate.set()
        second = new_run(node, first)
        old, new = first.tier("ssd").pool, second.tier("ssd").pool
        assert old.fd == -1
        assert new.fd >= 0 and new.path == old.path
        assert new.device_id == old.device_id
        # another boot of MaruServer: nothing adopted, all of it FREE
        assert second.adopted == []
        ssd = new.pool_id
        assert second.pools.bytes_in(PageState.FREE, ssd) == new.capacity
    finally:
        gate.set()
        node.stop()
        thread.join(15)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def real_server():
    server = MaruServer(enable_cxl=False, session_ttl_s=6.0, heartbeat_s=0.3)
    port = _free_port()
    rpc = RpcAsyncServer(server, host="127.0.0.1", port=port, num_workers=2)
    thread = threading.Thread(target=rpc.start, daemon=True)
    thread.start()
    time.sleep(0.2)
    yield server, f"tcp://127.0.0.1:{port}"
    rpc.stop()
    thread.join(5)
    server.close()


def ssd_locations(server, key: str) -> list[Location]:
    service = server.ms_service
    with service.lock:
        return [
            e.location
            for e in service.catalog.locations(Key(NS, key))
            if e.location.kind == PoolKind.SSD and e.pool.state == PoolState.ACTIVE
        ]


def test_the_same_boot_adopts_the_file_and_a_reboot_empties_it(
    real_server, sockdir, ssd_dir, handlers
):
    server, url = real_server
    config = dram_ssd(url, sockdir, ssd_dir)
    keys = [f"k{i}" for i in range(10)]
    first = start_node(config, host_boot_id="boot-1")
    try:
        h = handlers(first)
        for key in keys:
            assert put(h, key)
        wait_until(
            lambda: first.manager.move_results["RETIRED"] >= 3, what="pages demoted"
        )
        h.close()
    finally:
        first.stop(CloseMode.RESTART)
    moved = {k: loc for k in keys if (loc := ssd_locations(server, k))}
    assert moved

    second = start_node(config, host_boot_id="boot-1")
    try:
        pool = second.tier("ssd").pool
        ssd = pool.pool_id
        assert ssd == first.tier("ssd").pool.pool_id and second.adopted == [ssd]
        assert not second.pools.pool_quarantined(ssd)
        for key, (location,) in moved.items():
            ref = second.pools.find(location)
            assert ref is not None and second.pools.page(ref).state == PageState.READY
            assert ssd_bytes(pool, location) == value_of(key)
        ready = second.pools.bytes_in(PageState.READY, ssd)
        assert ready == len(moved) * PAGE
        assert second.pools.bytes_in(PageState.FREE, ssd) == pool.capacity - ready
        second.pools.check()
    finally:
        second.stop(CloseMode.RESTART)

    third = start_node(config, host_boot_id="boot-2")
    try:
        assert third.adopted == []
        pool = third.tier("ssd").pool
        ssd = pool.pool_id
        assert third.pools.bytes_in(PageState.FREE, ssd) == pool.capacity
        wait_until(
            lambda: not any(ssd_locations(server, k) for k in moved),
            what="the SSD keys of the old boot gone",
        )
    finally:
        third.stop()


def _maru_server():
    server = MaruServer(enable_cxl=False, session_ttl_s=6.0, heartbeat_s=0.3)
    port = _free_port()
    rpc = RpcAsyncServer(server, host="127.0.0.1", port=port, num_workers=2)
    thread = threading.Thread(target=rpc.start, daemon=True)
    thread.start()
    time.sleep(0.2)
    return server, rpc, thread, f"tcp://127.0.0.1:{port}"


def test_a_run_on_another_server_in_between_is_never_adopted(
    sockdir, ssd_dir, handlers
):
    """The node leaves SSD keys at S1, runs on S2 (which reuses the extents
    from scratch), then comes back to S1 on the same boot: S1 must not adopt
    the file, whose extents now hold S2's keys."""
    s1, r1, t1, u1 = _maru_server()
    s2, r2, t2, u2 = _maru_server()
    try:
        a = start_node(dram_ssd(u1, sockdir, ssd_dir), host_boot_id="boot-1")
        try:
            h = handlers(a)
            for i in range(10):
                assert put(h, f"a{i}")
            wait_until(lambda: a.manager.move_results["RETIRED"] >= 3, what="moved")
            h.close()
        finally:
            a.stop(CloseMode.RESTART)
        saved = {k: loc for i in range(10) if (loc := ssd_locations(s1, k := f"a{i}"))}
        assert saved
        b = start_node(dram_ssd(u2, sockdir, ssd_dir), host_boot_id="boot-1")
        try:
            assert b.adopted == []
            h = handlers(b)
            for i in range(10):
                assert put(h, f"zz{i}")
            wait_until(lambda: b.manager.move_results["RETIRED"] >= 3, what="moved")
            h.close()
        finally:
            b.stop(CloseMode.RESTART)
        c = start_node(dram_ssd(u1, sockdir, ssd_dir), host_boot_id="boot-1")
        try:
            # S2's run overwrote extents S1 still lists: never adopted
            assert any(
                ssd_bytes(c.tier("ssd").pool, loc) != value_of(k)
                for k, (loc,) in saved.items()
            )
            assert c.adopted == []
            assert (
                c.tier("ssd").pool.pool_generation
                == a.tier("ssd").pool.pool_generation + 2
            )
            wait_until(
                lambda: not any(ssd_locations(s1, k) for k in saved),
                what="S1 dropped the old extents",
            )
            h = handlers(c)
            for key in saved:
                read = h.retrieve(key)
                assert read is None or bytes(read.view) == value_of(key), key
            h.close()
        finally:
            c.stop(CloseMode.RESTART)
    finally:
        for rpc, thread, server in ((r1, t1, s1), (r2, t2, s2)):
            rpc.stop()
            thread.join(5)
            server.close()


def test_another_node_never_reuses_the_file(real_server, sockdir, ssd_dir, handlers):
    """A stops with its keys on the SSD; B, configured with the same path,
    cannot open the file, so A adopts its own bytes again. Handing the file
    over (removing it) gives a new UUID: A's old locations are not adopted."""
    server, url = real_server
    config = dram_ssd(url, sockdir, ssd_dir)
    first = start_node(config, host_boot_id="boot-1")
    try:
        h = handlers(first)
        for i in range(10):
            assert put(h, f"a{i}")
        wait_until(lambda: first.manager.move_results["RETIRED"] >= 3, what="moved")
        h.close()
    finally:
        first.stop(CloseMode.RESTART)
    saved = {k: loc for i in range(10) if (loc := ssd_locations(server, k := f"a{i}"))}
    assert saved
    other = dataclasses.replace(config, node_id="node-b")
    with pytest.raises(SsdPoolError, match="belongs to node 'gpu-test'"):
        start_node(other, host_boot_id="boot-1")
    again = start_node(config, host_boot_id="boot-1")
    try:
        assert again.adopted == [first.tier("ssd").pool.pool_id]
        for key, (location,) in saved.items():
            assert ssd_bytes(again.tier("ssd").pool, location) == value_of(key)
    finally:
        again.stop(CloseMode.RESTART)
    os.unlink(first.tier("ssd").pool.path)
    taken = start_node(other, host_boot_id="boot-1")
    taken.stop(CloseMode.RESTART)
    os.unlink(first.tier("ssd").pool.path)
    last = start_node(config, host_boot_id="boot-1")
    try:
        assert last.tier("ssd").pool.device_id != first.tier("ssd").pool.device_id
        assert last.adopted == []
        wait_until(
            lambda: not any(ssd_locations(server, k) for k in saved),
            what="A's old SSD keys gone",
        )
    finally:
        last.stop()
