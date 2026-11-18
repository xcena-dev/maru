# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""FETCH of SSD pages into DRAM by the node, for workers that agreed
fetch_v1, against the real MaruServer: one read per key for every waiter,
a pin each, cancel and deadline before and after the handoff, the node's
quota, failed reads, and a load larger than DRAM. A worker here is a raw
UDS connection plus its MaruServer session; Events hold the reads."""

import dataclasses
import errno
import mmap
import os
import threading
import time
from types import SimpleNamespace

import pytest

from maru_common.config import MaruConfig
from maru_common.ms_rpc import PLACEHOLDER_OP
from maru_common.node_protocol import (
    DetachRequest,
    FetchCancelRequest,
    FetchItem,
    FetchRequest,
    NodeMessageType,
    process_start_time,
)
from maru_common.protocol import CommitItem, UnpinResponse
from maru_common.storage_types import (
    FETCH_CAPABILITY,
    CloseMode,
    ItemOutcome,
    ItemResult,
    Key,
    Location,
    MissReason,
    PoolKind,
    Status,
)
from maru_handler import MaruHandler
from maru_handler.client import pool_maps
from maru_server.catalog import PoolState
from maru_server.node_rpc import NodeRpcServer
from maru_server.node_server import NodeServer
from maru_storage_service.drivers.ssd_alignment import aligned_buffer
from maru_storage_service.fetch_manager import CANCEL_MARKS, FLIGHT_WAITERS
from maru_storage_service.pool_manager import PageState
from maru_storage_service.ports import ReplyError
from tests.unit.ms_fakes import FakeWorker, Wire
from tests.unit.ms_node_helpers import (
    PAGE,
    RawWorker,
    node_config,
    remove_tree,
    short_tmpdir,
    start_node,
)
from tests.unit.ms_node_helpers import wait_until as _wait_until
from tests.unit.test_ms_cxl_tier import FakeRm
from tests.unit.test_ms_demotion import SMALL_CREDIT, Regions, put, value_of
from tests.unit.test_ms_ssd_tier import _free_port, cxl_ssd, dram_ssd

NS = "ns"


@pytest.fixture
def sockdir():
    path = short_tmpdir()
    yield path
    remove_tree(path)


@pytest.fixture
def real_server():
    server = NodeServer(session_ttl_s=6.0, heartbeat_s=0.3)
    port = _free_port()
    rpc = NodeRpcServer(server, host="127.0.0.1", port=port)
    thread = threading.Thread(target=rpc.start, daemon=True)
    thread.start()
    time.sleep(0.2)
    yield server, f"tcp://127.0.0.1:{port}"
    rpc.stop()
    thread.join(5)
    server.close()


@pytest.fixture
def handlers():
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


def wait_until(pred, what: str = "", timeout: float = 15.0):
    return _wait_until(pred, timeout=timeout, what=what)


def make_node(url, sockdir, ssd_dir, **budgets):
    config = node_config(
        url,
        f"{sockdir}/node.sock",
        pages=16,
        high=0.5,
        low=0.0625,
        budgets=SMALL_CREDIT | budgets,
        ssd_path=ssd_dir,
        ssd_pages=64,
    )
    return start_node(config, host_boot_id="boot-1")


@pytest.fixture
def env(real_server, sockdir, ssd_dir, handlers):
    """A [dram, ssd] node whose first keys were moved to the SSD."""
    server, url = real_server
    made = []

    def make(keys: int = 12, **budgets):
        node = make_node(url, sockdir, ssd_dir, **budgets)
        made.append(node)
        h = handlers(node)
        names = [f"k{i}" for i in range(keys)]
        for name in names:
            assert put(h, name)
        wait_until(lambda: len(on_ssd(server, names)) >= keys // 2, "moved to SSD")
        h.close()
        return node, on_ssd(server, names)

    yield server, make
    for node in made:
        node.stop()


def miss(reason: MissReason | None = None) -> ItemOutcome:
    return ItemOutcome(ItemResult.MISS, reason=reason)


def locations(server, key: str) -> list[Location]:
    service = server.ms_service
    with service.lock:
        return [
            e.location
            for e in service.catalog.locations(Key(NS, key))
            if e.pool.state == PoolState.ACTIVE
        ]


def on_ssd(server, names: list[str]) -> list[str]:
    """Keys only on the SSD."""
    return [
        n
        for n in names
        if (locs := locations(server, n))
        and all(loc.kind == PoolKind.SSD for loc in locs)
    ]


def pins(server, key: str, location: Location) -> int:
    service = server.ms_service
    with service.lock:
        return service.pins.count(Key(NS, key), location)


class Loader:
    """A worker's loader: its UDS connection (fetch_v1) and its MaruServer
    session."""

    def __init__(self, node, server, tp_rank: int = 0):
        self.node = node
        self.service = server.ms_service
        self.raw = RawWorker(node.config.socket)
        reply = self.raw.attach(capabilities=[FETCH_CAPABILITY], tp_rank=tp_rank)
        assert reply.capabilities == [FETCH_CAPABILITY]
        self.budgets = reply.budgets
        self.session = FakeWorker(
            Wire(self.service),
            SimpleNamespace(node_id=node.config.node_id, epoch=node.node_epoch),
            tp_rank=tp_rank,
        )
        self.session.incarnation = self.session.origin = self.raw.incarnation
        self.session.pid, self.session.pid_start = (
            os.getpid(),
            process_start_time(os.getpid()),
        )
        assert self.session.open().status == Status.OK
        self.dram = None
        if node.tier("dram") is not None:  # its memfd comes first
            self.dram = mmap.mmap(
                self.raw.fds[0], node.tier("dram").pool.capacity, prot=mmap.PROT_READ
            )

    @property
    def token(self) -> str:
        return self.session.token

    def items(self, *names) -> list[FetchItem]:
        response = self.session.lookup(*[Key(NS, n) for n in names])
        out = []
        for name, outcome in zip(names, response.items, strict=True):
            assert outcome.result == ItemResult.FETCH, (name, outcome)
            out.append(FetchItem(key=Key(NS, name), plan=outcome.plan))
        return out

    def request(self, items, *, deadline_ms=5000, max_bytes=1 << 30):
        return self.raw.stamp(
            FetchRequest(
                op=PLACEHOLDER_OP,
                worker_session=self.token,
                items=items,
                max_bytes=max_bytes,
                deadline_ms=deadline_ms,
            )
        )

    def send(self, request):
        """The FETCH's future."""
        return self.raw.conn.submit(NodeMessageType.FETCH, request)

    def fetch(self, *names, **kwargs):
        request = self.request(self.items(*names), **kwargs)
        return self.send(request).result(10)

    def cancel(self, request):
        return self.raw.request(
            NodeMessageType.FETCH_CANCEL, FetchCancelRequest(target=request.op)
        )

    def read(self, location: Location) -> bytes:
        return self.dram[location.offset : location.offset + location.length]

    def unpin(self, *pin_ids):
        return self.session.unpin(*pin_ids)

    def close(self):
        if self.dram is not None:
            self.dram.close()
        self.raw.close()


class Reads:
    """Counts the node's SSD reads; ``hold()`` keeps them until ``release``."""

    def __init__(self, node, monkeypatch, error: Exception | None = None):
        self.calls = 0
        self.entered = threading.Event()
        self.gate = threading.Event()
        self.gate.set()
        read = node.tier("ssd").pool.read

        def counted(*args, **kwargs):
            self.calls += 1
            self.entered.set()
            assert self.gate.wait(10)
            if error is not None:
                raise error
            return read(*args, **kwargs)

        monkeypatch.setattr(node.tier("ssd").pool, "read", counted)

    def hold(self):
        self.gate.clear()
        self.entered.clear()
        return self

    def release(self):
        self.gate.set()


def settled(node, server) -> bool:
    """No flight, no quota, and no source or node pin left."""
    fetches = node.manager.fetches
    return (
        fetches.flights() == 0
        and fetches.inflight_bytes == 0
        and node_pins(server, node) == 0
    )


def node_pins(server, node) -> int:
    """Pins the node session holds (source and copy pins)."""
    service = server.ms_service
    with service.lock:
        return len(service.pins.held_by(node.server.session))


def no_transfer_pages(node) -> bool:
    return node.pools.bytes_in(PageState.TRANSFER, node.tier("dram").pool.pool_id) == 0


# =============================================================================
# One read, a pin each
# =============================================================================


def test_two_workers_share_one_read_and_get_their_own_pins(env, monkeypatch):
    server, make = env
    node, keys = make()
    name = keys[0]
    (source,) = locations(server, name)
    a, b = Loader(node, server), Loader(node, server, tp_rank=1)
    reads = Reads(node, monkeypatch).hold()
    first = a.send(a.request(a.items(name)))
    assert reads.entered.wait(5)
    second = b.send(b.request(b.items(name)))
    wait_until(lambda: node.manager.fetches.results["joined"] == 1, "joined")
    reads.release()
    one, two = first.result(10).items[0], second.result(10).items[0]
    assert (one.result, two.result) == (ItemResult.HIT, ItemResult.HIT)
    assert one.location == two.location and one.pin_id != two.pin_id
    assert reads.calls == 1
    copy = one.location
    assert copy.kind == PoolKind.DRAM and a.read(copy) == value_of(name)
    assert b.read(copy) == value_of(name)
    # the node pin went after the fan-out; the source pin with FETCH_COMPLETE
    wait_until(lambda: pins(server, name, copy) == 2, "the node pin released")
    assert pins(server, name, source) == 0
    # each UNPIN is its own
    a.unpin(one.pin_id)
    assert pins(server, name, copy) == 1
    b.unpin(two.pin_id)
    assert pins(server, name, copy) == 0
    wait_until(lambda: settled(node, server), "quota back")
    node.pools.check()
    a.close()
    b.close()


@pytest.mark.parametrize("how", ["cancel", "exit"])
def test_the_leader_leaving_does_not_hold_up_the_others(env, monkeypatch, how):
    server, make = env
    node, keys = make()
    name = keys[0]
    a, b = Loader(node, server), Loader(node, server, tp_rank=1)
    reads = Reads(node, monkeypatch).hold()
    request = a.request(a.items(name))
    first = a.send(request)
    assert reads.entered.wait(5)
    second = b.send(b.request(b.items(name)))
    wait_until(lambda: node.manager.fetches.results["joined"] == 1, "joined")
    if how == "cancel":
        assert a.cancel(request).status == Status.OK
        answer = first.result(10)
        assert answer.items[0].result == ItemResult.MISS
        assert answer.items[0].reason is None
    else:
        # its MaruServer session stays open: the node reports the exit
        node.manager.workers.on_process_exit(f"worker:{a.raw.incarnation}")
        # its FETCH is answered at the exit, not at its deadline
        wait_until(
            lambda: node.manager.fetches.results["MISS"] == 1,
            "answered at the exit",
            timeout=2.0,
        )
    reads.release()
    hit = second.result(10).items[0]
    assert hit.result == ItemResult.HIT and b.read(hit.location) == value_of(name)
    assert reads.calls == 1
    wait_until(lambda: pins(server, name, hit.location) == 1, "only b's pin")
    if how == "exit":
        wait_until(
            lambda: (
                node.manager.workers.settled(a.raw.incarnation)
                and node.manager.pending_exits() == 0
            ),
            "WORKER_EXIT reported",
        )
    with server.ms_service.lock:
        assert server.ms_service.pins.held_by(a.token) == []
    b.unpin(hit.pin_id)
    wait_until(lambda: settled(node, server), "quota back")
    node.pools.check()
    a.close()
    b.close()


# =============================================================================
# Cancel and deadline, before and after the handoff
# =============================================================================


def test_a_cancel_before_the_handoff_cleans_up_and_keeps_the_copy(env, monkeypatch):
    server, make = env
    node, keys = make()
    name = keys[0]
    (source,) = locations(server, name)
    loader = Loader(node, server)
    reads = Reads(node, monkeypatch).hold()
    request = loader.request(loader.items(name))
    reply = loader.send(request)
    assert reads.entered.wait(5)
    assert loader.cancel(request).status == Status.OK
    assert reply.result(10).items[0] == miss()
    # the read under way ends; the copy stays as cache, with no pin
    reads.release()
    wait_until(lambda: settled(node, server), "the flight ended")
    wait_until(lambda: len(locations(server, name)) == 2, "the copy published")
    copy = next(c for c in locations(server, name) if c.kind == PoolKind.DRAM)
    wait_until(lambda: pins(server, name, copy) == 0, "no pin left")
    assert pins(server, name, source) == 0
    assert node.pools.page(node.pools.find(copy)).state == PageState.READY
    assert no_transfer_pages(node)
    # a resend is answered the same
    assert loader.send(request).result(10).items[0] == miss()
    node.pools.check()
    loader.close()


def test_a_cancel_after_the_handoff_leaves_the_pin_to_the_worker(env):
    server, make = env
    node, keys = make()
    name = keys[0]
    loader = Loader(node, server)
    request = loader.request(loader.items(name))
    hit = loader.send(request).result(10).items[0]
    assert hit.result == ItemResult.HIT
    assert loader.cancel(request).status == Status.OK
    wait_until(lambda: pins(server, name, hit.location) == 1, "the node pin gone")
    time.sleep(0.1)
    assert pins(server, name, hit.location) == 1  # the worker's stays
    assert loader.read(hit.location) == value_of(name)
    loader.unpin(hit.pin_id)
    assert pins(server, name, hit.location) == 0
    loader.close()


def test_a_cancel_that_comes_first_stops_its_fetch(env, monkeypatch):
    server, make = env
    node, keys = make()
    loader = Loader(node, server)
    reads = Reads(node, monkeypatch)
    request = loader.request(loader.items(keys[0]))
    assert loader.cancel(request).status == Status.OK
    assert loader.send(request).result(10).items[0] == miss()
    assert reads.calls == 0 and settled(node, server)
    # another worker's op cannot be cancelled
    other = Loader(node, server, tp_rank=1)
    assert other.cancel(request).status == Status.REJECTED
    loader.close()
    other.close()


def test_a_deadline_answers_timeout_and_the_read_still_lands(env, monkeypatch):
    server, make = env
    node, keys = make()
    name = keys[0]
    loader = Loader(node, server)
    reads = Reads(node, monkeypatch).hold()
    started = time.monotonic()
    reply = loader.send(loader.request(loader.items(name), deadline_ms=150))
    assert reply.result(10).items[0] == miss(MissReason.TIMEOUT)
    assert time.monotonic() - started < 2.0
    reads.release()
    wait_until(lambda: settled(node, server), "the flight ended")
    wait_until(lambda: len(locations(server, name)) == 2, "the copy published")
    with server.ms_service.lock:
        assert server.ms_service.pins.held_by(loader.token) == []
    assert node.manager.fetches.results["MISS:timeout"] == 1
    node.pools.check()
    loader.close()


# =============================================================================
# Quota and failures
# =============================================================================


def test_the_quota_holds_page_and_bounce_until_the_handoff(env, monkeypatch):
    server, make = env
    # room for one page and its bounce only
    node, keys = make(fetch_request_max_bytes=PAGE, fetch_max_inflight_bytes=2 * PAGE)
    loader = Loader(node, server)
    reads = Reads(node, monkeypatch).hold()
    first = loader.send(loader.request(loader.items(keys[0])))
    assert reads.entered.wait(5)
    bounce = node.tier("ssd").pool.io_size(len(value_of(keys[0])))
    fetches = node.manager.fetches
    assert fetches.inflight_bytes == PAGE + bounce
    # another key does not fit
    busy = loader.fetch(keys[1])
    assert busy.items[0] == miss(MissReason.BUSY)
    # one over the request cap is refused as a whole
    over = loader.fetch(keys[1], keys[2])
    assert over.status == Status.REJECTED and "cap" in over.error
    reads.release()
    hit = first.result(10).items[0]
    assert hit.result == ItemResult.HIT
    wait_until(lambda: settled(node, server), "quota back")
    assert loader.fetch(keys[1]).items[0].result == ItemResult.HIT
    loader.close()


@pytest.mark.parametrize("code", [errno.EIO, errno.EINVAL])
def test_a_failed_read_is_an_io_error_and_frees_all_it_held(env, monkeypatch, code):
    server, make = env
    node, keys = make()
    name = keys[0]
    (source,) = locations(server, name)
    loader = Loader(node, server)
    Reads(node, monkeypatch, error=OSError(code, os.strerror(code)))
    answer = loader.fetch(name)
    assert answer.items[0] == miss(MissReason.IO_ERROR)
    wait_until(lambda: settled(node, server), "quota back")
    wait_until(lambda: pins(server, name, source) == 0, "the source unpinned")
    assert locations(server, name) == [source]  # nothing published
    assert no_transfer_pages(node)
    node.pools.check()
    loader.close()


def test_a_resend_gets_the_same_answer_without_a_second_read(env, monkeypatch):
    server, make = env
    node, keys = make()
    loader = Loader(node, server)
    reads = Reads(node, monkeypatch)
    request = loader.request(loader.items(keys[0]))
    first = loader.send(request).result(10)
    assert first.items[0].result == ItemResult.HIT
    assert loader.send(request).result(10) == first
    assert reads.calls == 1
    loader.unpin(first.items[0].pin_id)
    loader.close()


def test_what_is_refused(env):
    server, make = env
    node, keys = make()
    loader = Loader(node, server)
    (item,) = loader.items(keys[0])
    for request, why in [
        (loader.request([item], deadline_ms=0), "deadline_ms"),
        (loader.request([item], max_bytes=PAGE - 1), "cap"),
        (loader.request([]), "items"),
    ]:
        answer = loader.send(request).result(10)
        assert answer.status == Status.REJECTED and why in answer.error
    # a plan of another node, or not on the SSD
    other = FetchItem(
        item.key,
        type(item.plan)(item.plan.source, "another-node", item.plan.bytes),
    )
    dram = FetchItem(
        item.key,
        type(item.plan)(
            Location(node.tier("dram").pool.pool_id, "a", 1, 0, PAGE, PoolKind.DRAM),
            item.plan.source_node,
            item.plan.bytes,
        ),
    )
    answer = loader.send(loader.request([other, dram])).result(10)
    assert [o.result for o in answer.items] == [ItemResult.REJECTED] * 2
    # a worker that did not agree fetch_v1
    plain = RawWorker(node.config.socket)
    assert plain.attach().capabilities == []
    stamped = plain.stamp(
        FetchRequest(PLACEHOLDER_OP, loader.token, [item], 1 << 30, 1000)
    )
    assert plain.request(NodeMessageType.FETCH, stamped).status == Status.DISABLED
    plain.close()
    loader.close()


def test_a_worker_exit_keeps_its_fetched_pins_through_the_gpu_grace(env):
    server, make = env
    node, keys = make()
    name = keys[0]
    loader = Loader(node, server)
    hit = loader.fetch(name).items[0]
    assert hit.result == ItemResult.HIT
    wait_until(lambda: pins(server, name, hit.location) == 1, "the node pin gone")
    node.manager.workers.on_process_exit(f"worker:{loader.raw.incarnation}")
    # its GPU may still read the copy: the pin stays for the grace
    time.sleep(0.1)
    assert pins(server, name, hit.location) == 1
    wait_until(lambda: pins(server, name, hit.location) == 0, "WORKER_EXIT")
    loader.close()


# =============================================================================
# Progress past the DRAM size
# =============================================================================


def test_a_load_larger_than_dram_goes_through_in_windows(env, monkeypatch):
    """40 keys on the SSD, 16 DRAM pages: windows of 4, each unpinned after
    its reads; eviction makes room by dropping fetched copies onto their SSD
    pages."""
    server, make = env
    node, keys = make(keys=40)
    all_names = [f"k{i}" for i in range(40)]
    wait_until(lambda: len(on_ssd(server, all_names)) >= 24, "on the SSD")
    names = on_ssd(server, all_names)
    assert len(names) * PAGE > node.tier("dram").pool.capacity
    writes = []
    write = node.tier("ssd").pool.write
    monkeypatch.setattr(
        node.tier("ssd").pool,
        "write",
        lambda *a, **k: writes.append(1) or write(*a, **k),
    )
    loader = Loader(node, server)
    for i in range(0, len(names), 4):
        window = names[i : i + 4]
        answer = loader.fetch(*window)
        assert [o.result for o in answer.items] == [ItemResult.HIT] * len(window), [
            (o.result, o.reason) for o in answer.items
        ]
        for name, outcome in zip(window, answer.items, strict=True):
            assert loader.read(outcome.location) == value_of(name)
        loader.unpin(*[o.pin_id for o in answer.items])
    fetches = node.manager.fetches
    assert fetches.results["reads"] == len(names)
    # fetched copies left by dropping them onto their SSD pages: no rewrite
    assert node.manager.move_results["RETIRED"] > 40
    assert len(writes) <= 40 - len(names)
    assert not fetches.results["MISS:busy"] and not fetches.results["MISS:timeout"]
    wait_until(lambda: settled(node, server), "quota back")
    node.pools.check()
    loader.close()


def test_after_a_restart_fetched_copies_drop_onto_the_adopted_ssd_pages(
    real_server, sockdir, ssd_dir, handlers, monkeypatch
):
    """The adopted SSD pages keep the old run's generations: dropping a
    fetched copy onto one must still work, or DRAM fills and fetches stop."""
    server, url = real_server
    first = make_node(url, sockdir, ssd_dir)
    all_names = [f"k{i}" for i in range(40)]
    try:
        h = handlers(first)
        for name in all_names:
            assert put(h, name)
        wait_until(lambda: len(on_ssd(server, all_names)) >= 24, "on the SSD")
        h.close()
    finally:
        first.stop(CloseMode.RESTART)
    node = make_node(url, sockdir, ssd_dir)
    try:
        assert node.adopted == [node.tier("ssd").pool.pool_id]
        names = on_ssd(server, all_names)
        assert len(names) * PAGE > node.tier("dram").pool.capacity
        writes = []
        write = node.tier("ssd").pool.write
        monkeypatch.setattr(
            node.tier("ssd").pool,
            "write",
            lambda *a, **k: writes.append(1) or write(*a, **k),
        )
        loader = Loader(node, server)
        for i in range(0, len(names), 4):
            window = names[i : i + 4]
            answer = loader.fetch(*window)
            assert [o.result for o in answer.items] == [ItemResult.HIT] * len(window), (
                i,
                [o.reason for o in answer.items],
                dict(node.manager.move_results),
            )
            for name, outcome in zip(window, answer.items, strict=True):
                assert loader.read(outcome.location) == value_of(name)
            loader.unpin(*[o.pin_id for o in answer.items])
        moves = node.manager.move_results
        assert not moves["COMMIT:REJECTED"] and not moves["COMMIT:DUPLICATE"]
        assert moves["RETIRED"] >= len(names) - node.tier("dram").pool.capacity // PAGE
        assert not writes  # no rewrite of a page the SSD has
        wait_until(lambda: settled(node, server), "quota back")
        node.pools.check()
        loader.close()
    finally:
        node.stop()


# =============================================================================
# Into CXL: [cxl, ssd] fetches into a region, [dram, cxl, ssd] into DRAM
# =============================================================================


@pytest.fixture
def regions(monkeypatch):
    stand_in = Regions()
    monkeypatch.setattr(pool_maps, "CxlRegionMapping", stand_in.worker_map)
    yield stand_in
    stand_in.close()


def start_cxl_node(config, regions):
    return start_node(
        config,
        host_boot_id="boot-1",
        region_source=FakeRm(),
        cxl_renew_s=0.05,
        map_region=regions.node_map,
    )


def region_bytes(node, regions, location: Location) -> bytes:
    """What a worker's mapping of the region holds there."""
    (pool,) = [p for p in node.tier("cxl").pools if p.pool_id == location.pool_id]
    fd = regions.fds[pool.region.handle.region_id]
    return os.pread(fd, location.length, location.offset)


def test_a_cxl_top_tier_fetches_into_a_region(real_server, sockdir, ssd_dir, regions):
    server, url = real_server
    node = start_cxl_node(cxl_ssd(url, sockdir, ssd_dir), regions)
    try:
        by_pool = {p.pool_id: p.region.handle.region_id for p in node.tier("cxl").pools}
        writer = RawWorker(node.config.socket)
        writer.attach()
        names = []
        while len(names) < 12:
            for credit in writer.credit(2 * PAGE).credits:
                name = f"k{len(names)}"
                fd = regions.fds[by_pool[credit.pool_id]]
                os.pwrite(fd, value_of(name), credit.offset)
                done = writer.write_done([credit], [name], len(value_of(name)))
                assert done.status == Status.OK
                names.append(name)
        writer.close()
        wait_until(lambda: len(on_ssd(server, names)) >= 3, "moved to the SSD")
        moved = on_ssd(server, names)[:3]
        loader = Loader(node, server)
        answer = loader.fetch(*moved)
        assert [o.result for o in answer.items] == [ItemResult.HIT] * 3
        for name, outcome in zip(moved, answer.items, strict=True):
            copy = outcome.location
            assert copy.kind == PoolKind.CXL and copy.pool_id in by_pool
            assert region_bytes(node, regions, copy) == value_of(name)
        loader.unpin(*[o.pin_id for o in answer.items])
        wait_until(lambda: settled(node, server), "quota back")
        node.pools.check()
        loader.close()
    finally:
        node.stop()


def test_dram_over_cxl_over_ssd_fetches_into_dram(
    real_server, sockdir, ssd_dir, regions, handlers
):
    server, url = real_server
    node = start_cxl_node(dram_ssd(url, sockdir, ssd_dir, cxl_pages=8), regions)
    try:
        h = handlers(node)
        names = [f"k{i}" for i in range(24)]
        for name in names:
            assert put(h, name)
        wait_until(lambda: len(on_ssd(server, names)) >= 3, "moved down to the SSD")
        h.close()
        moved = on_ssd(server, names)[:3]
        loader = Loader(node, server)
        answer = loader.fetch(*moved)
        assert [o.result for o in answer.items] == [ItemResult.HIT] * 3
        for name, outcome in zip(moved, answer.items, strict=True):
            assert outcome.location.kind == PoolKind.DRAM
            assert loader.read(outcome.location) == value_of(name)
        loader.unpin(*[o.pin_id for o in answer.items])
        wait_until(lambda: settled(node, server), "quota back")
        node.pools.check()
        loader.close()
    finally:
        node.stop()


# =============================================================================
# Review fixes: whose session, early cancels, waiters, deadlines, late plans
# =============================================================================


def test_a_worker_cannot_fetch_with_another_workers_session(env, monkeypatch):
    server, make = env
    node, keys = make()
    a, b = Loader(node, server), Loader(node, server, tp_rank=1)
    reads = Reads(node, monkeypatch)
    forged = a.raw.stamp(
        FetchRequest(PLACEHOLDER_OP, b.token, a.items(keys[0]), 1 << 30, 5000)
    )
    answer = a.send(forged).result(10)
    assert answer.items[0].result == ItemResult.REJECTED
    assert reads.calls == 0  # refused before any read
    with server.ms_service.lock:
        assert server.ms_service.pins.held_by(b.token) == []
    # nor by joining another worker's read with a forged session
    reads.hold()
    first = a.send(a.request(a.items(keys[1])))
    assert reads.entered.wait(5)
    joined = b.raw.stamp(
        FetchRequest(PLACEHOLDER_OP, a.token, b.items(keys[1]), 1 << 30, 5000)
    )
    second = b.send(joined)
    wait_until(lambda: node.manager.fetches.results["joined"] == 1, "joined")
    reads.release()
    hit = first.result(10).items[0]
    assert hit.result == ItemResult.HIT
    assert second.result(10).items[0].result == ItemResult.REJECTED
    wait_until(lambda: pins(server, keys[1], hit.location) == 1, "only a's pin")
    a.unpin(hit.pin_id)
    wait_until(lambda: node_pins(server, node) == 0, "no node pin left")
    a.close()
    b.close()


def test_an_unknown_unpin_result_is_retried_then_ends_the_run(env, monkeypatch):
    server, make = env
    node, keys = make()
    loader = Loader(node, server)
    unpin = node.manager.metadata.unpin
    calls = []

    def flaky(ids):
        calls.append(list(ids))
        if len(calls) == 1:
            raise ReplyError("UNPIN: 0 results")
        return unpin(ids)

    monkeypatch.setattr(node.manager.metadata, "unpin", flaky)
    hit = loader.fetch(keys[0]).items[0]
    assert hit.result == ItemResult.HIT
    # the node pin is sent again, not dropped
    wait_until(lambda: pins(server, keys[0], hit.location) == 1, "node pin gone")
    assert len(calls) == 2 and calls[0] == calls[1]
    assert node.manager.ended is None
    loader.unpin(hit.pin_id)

    def broken(ids):
        raise ReplyError("UNPIN: 0 results")

    monkeypatch.setattr(node.manager.metadata, "unpin", broken)
    loader.fetch(keys[1])
    wait_until(lambda: node.manager.ended is not None, "the run ends")
    assert "UNPIN" in node.manager.ended
    loader.close()


def test_early_cancels_are_kept_until_acked(env, monkeypatch):
    server, make = env
    node, keys = make()
    loader = Loader(node, server)
    reads = Reads(node, monkeypatch)
    items = loader.items(keys[0])
    cancelled = [loader.request(items) for _ in range(CANCEL_MARKS)]
    for request in cancelled:
        assert loader.cancel(request).status == Status.OK
    # one more cancel, and a FETCH not cancelled, wait for acks
    assert loader.cancel(loader.request(items)).status == Status.BUSY
    assert loader.send(loader.request(items)).result(10).status == Status.BUSY
    # a cancelled FETCH that comes late never reads
    assert loader.send(cancelled[0]).result(10).items[0] == miss()
    assert reads.calls == 0
    for request in cancelled:
        loader.raw.origin.resolved(request.op.seq)
    hit = loader.fetch(keys[0]).items[0]  # its ack drops the marks
    assert hit.result == ItemResult.HIT and reads.calls == 1
    loader.unpin(hit.pin_id)
    loader.close()


def test_a_flight_takes_a_bounded_number_of_waiters(env, monkeypatch):
    server, make = env
    node, keys = make()
    a, b = Loader(node, server), Loader(node, server, tp_rank=1)
    reads = Reads(node, monkeypatch)
    entered, release = threading.Event(), threading.Event()
    tier = node.tier("ssd")
    submit = tier.submit_load

    def hold_before_io(*args, **kwargs):
        entered.set()
        assert release.wait(10)
        return submit(*args, **kwargs)

    # Filling the waiter list deliberately stalls this flight. Hold it
    # before submission so that wait is outside the SSD's 1 s I/O budget.
    monkeypatch.setattr(tier, "submit_load", hold_before_io)
    first = a.send(a.request(a.items(keys[0])))
    try:
        assert entered.wait(5)
        items = b.items(keys[0])
        fetches = node.manager.fetches
        # cancelled waiters leave the flight
        for i in range(FLIGHT_WAITERS + 6):
            request = b.request(items)
            reply = b.send(request)
            wait_until(lambda i=i: fetches.results["joined"] == i + 1, "joined")
            assert b.cancel(request).status == Status.OK
            assert reply.result(10).items[0] == miss()
        with fetches._lock:
            (flight,) = fetches._flights.values()
            assert flight.waiters == []
        waiting = [b.send(b.request(items)) for _ in range(FLIGHT_WAITERS)]
        over = b.send(b.request(items)).result(10).items[0]
        assert over == miss(MissReason.BUSY)
        assert reads.calls == 0
    finally:
        release.set()
    hits = [f.result(10).items[0] for f in [first, *waiting]]
    assert all(h.result == ItemResult.HIT for h in hits), hits
    assert reads.calls == 1
    a.unpin(hits[0].pin_id)
    b.unpin(*[h.pin_id for h in hits[1:]])
    wait_until(lambda: pins(server, keys[0], hits[0].location) == 0, "pins gone")
    wait_until(lambda: node_pins(server, node) == 0, "no node pin left")
    a.close()
    b.close()


def test_each_waiter_waits_for_room_until_its_own_deadline(env, monkeypatch):
    server, make = env
    node, keys = make()
    a, b = Loader(node, server), Loader(node, server, tp_rank=1)
    fetches = node.manager.fetches
    reserve = fetches._reserve
    short = threading.Event()
    monkeypatch.setattr(fetches, "_reserve", lambda count: short.set() or [])
    first = a.send(a.request(a.items(keys[0]), deadline_ms=150))
    assert short.wait(5)
    second = b.send(b.request(b.items(keys[0]), deadline_ms=5000))
    wait_until(lambda: fetches.results["joined"] == 1, "joined")
    assert first.result(5).items[0] == miss(MissReason.TIMEOUT)
    time.sleep(0.2)  # the flight goes on for b
    assert not second.done()
    monkeypatch.setattr(fetches, "_reserve", reserve)  # room now
    hit = second.result(5).items[0]
    assert hit.result == ItemResult.HIT
    b.unpin(hit.pin_id)
    wait_until(lambda: settled(node, server), "quota back")
    a.close()
    b.close()


def test_a_late_plan_uses_the_copy_published_before(env, monkeypatch):
    server, make = env
    node, keys = make()
    a, b = Loader(node, server), Loader(node, server, tp_rank=1)
    reads = Reads(node, monkeypatch)
    late = b.request(b.items(keys[0]))  # planned on the SSD, sent later
    first = a.fetch(keys[0]).items[0]
    assert first.result == ItemResult.HIT
    wait_until(lambda: settled(node, server), "the first flight ended")
    second = b.send(late).result(10).items[0]
    assert second.result == ItemResult.HIT and second.location == first.location
    assert reads.calls == 1
    assert node.manager.fetches.results["published_used"] == 1
    wait_until(lambda: node_pins(server, node) == 0, "the copy's node pin gone")
    assert pins(server, keys[0], first.location) == 2
    a.unpin(first.pin_id)
    b.unpin(second.pin_id)
    assert pins(server, keys[0], first.location) == 0
    node.pools.check()
    a.close()
    b.close()


# =============================================================================
# A worker's fetch window (the node client in a parked load's fetch scope)
# =============================================================================


def loader_client(node) -> MaruHandler:
    handler = MaruHandler(
        MaruConfig(
            maru_node=node.config.socket,
            engine_id="loader",
            cache_namespace=NS,
            chunk_size_bytes=PAGE,
            pool_size=PAGE,
            pin_pools=False,
            timeout_ms=300,
            auto_connect=False,
            load_only=True,
            fetch_loads=True,
        )
    )
    handler.connect()
    return handler


def test_the_window_fits_the_top_tier_and_a_load_goes_through_it(env):
    server, make = env
    node, _ = make(keys=40)
    names = on_ssd(server, [f"k{i}" for i in range(40)])
    # 16 DRAM pages, less 2 of credit, 2 of commit pending, 1 of reserve
    window = 11
    assert len(names) > window
    h = loader_client(node)
    try:
        assert h._cpu.attach_info.budgets.fetch_request_max_bytes == window * PAGE
        assert h.retrieve_capacity() is None  # outside a fetch scope
        with h.fetch_scope("r1"):
            assert h.retrieve_capacity() == window
            done = 0
            while done < len(names):  # as the connector's leased batches
                wait_until(lambda: h.retrieve_capacity() == window, "slots back")
                batch = names[done : done + window]
                reads = h.batch_retrieve(batch)
                assert all(r is not None for r in reads), h._cpu.stats
                for name, read in zip(batch, reads, strict=True):
                    assert bytes(read.view) == value_of(name)
                assert h.retrieve_capacity() == window - len(batch)
                h.release_retrieved(reads)
                done += len(batch)
        assert h._cpu.stats["fetch_hit"] == len(names)
    finally:
        h.close()


def test_a_slot_stays_until_its_unpin_is_confirmed(env, monkeypatch):
    server, make = env
    node, keys = make(fetch_request_max_bytes=PAGE, fetch_max_inflight_bytes=2 * PAGE)
    h = loader_client(node)
    conn = h._cpu._server
    flush = conn._flush_unpins
    entered, gate = threading.Event(), threading.Event()

    def held():
        if conn.pending_unpins():
            entered.set()
            assert gate.wait(10)
        return flush()

    try:
        with h.fetch_scope("r1"):
            assert h.retrieve_capacity() == 1
            (read,) = h.batch_retrieve([keys[0]])
            assert read is not None and h.retrieve_capacity() == 0
            monkeypatch.setattr(conn, "_flush_unpins", held)
            h.release_retrieved([read])
            assert entered.wait(5)
            # Released here, still pinned at MaruServer: nothing more fits
            assert conn.pending_unpins() == 1 and h.retrieve_capacity() == 0
            assert h.batch_retrieve([keys[1]]) == [None]
            assert h._cpu.stats["fetch_over_window"] == 1
            gate.set()
            wait_until(lambda: conn.pending_unpins() == 0, "the UNPIN confirmed")
            assert h.retrieve_capacity() == 1
    finally:
        gate.set()
        h.close()


def test_two_loaders_never_take_the_same_slot(env, monkeypatch):
    server, make = env
    node, keys = make(
        fetch_request_max_bytes=2 * PAGE, fetch_max_inflight_bytes=4 * PAGE
    )
    h = loader_client(node)
    client = h._cpu
    fetch = type(client)._fetch
    entered, gate = threading.Event(), threading.Event()
    sent = []

    def held(self, *args):
        sent.append(args)
        if len(sent) == 1:  # r1: its slot is taken, its FETCH not sent yet
            entered.set()
            assert gate.wait(10)
        return fetch(self, *args)

    monkeypatch.setattr(type(client), "_fetch", held)
    got = {}

    def load(name: str, wanted: list[str]) -> None:
        with h.fetch_scope(name):
            got[name] = h.batch_retrieve(wanted)

    first = threading.Thread(target=load, args=("r1", keys[:1]))
    try:
        first.start()
        assert entered.wait(5)
        load("r2", keys[1:3])  # two more do not fit beside r1's slot
        assert got["r2"] == [None, None] and len(sent) == 1
        assert client.stats["fetch_over_window"] == 2
        load("r2", keys[1:2])  # one does
        (second,) = got["r2"]
        assert second is not None and bytes(second.view) == value_of(keys[1])
        with h.fetch_scope("r2"):
            assert h.retrieve_capacity() == 0
        gate.set()
        first.join(10)
        (read,) = got["r1"]
        assert read is not None and bytes(read.view) == value_of(keys[0])
        with h.fetch_scope("r2"):
            assert h.retrieve_capacity() == 0  # each slot counted once
        h.release_retrieved([read, second])
    finally:
        gate.set()
        first.join(10)
        h.close()


# =============================================================================
# Review fixes: the window fits the quota and the real pools; a live waiter pins
# =============================================================================


def test_a_full_window_fits_the_inflight_cap(env):
    server, make = env
    node, keys = make(
        fetch_request_max_bytes=4 * PAGE, fetch_max_inflight_bytes=4 * PAGE
    )
    loader = Loader(node, server)
    # each flight holds a page and its bounce: two fit, not three
    assert node.tier("ssd").pool.io_size(PAGE) == PAGE
    assert loader.budgets.fetch_request_max_bytes == 2 * PAGE
    answer = loader.fetch(*keys[:2])
    assert [o.result for o in answer.items] == [ItemResult.HIT] * 2
    assert node.manager.fetches.results["MISS:busy"] == 0
    loader.unpin(*[o.pin_id for o in answer.items])
    wait_until(lambda: settled(node, server), "quota back")
    loader.close()


def seed_ssd(node, names: list[str]) -> None:
    """Commit each name's page straight to the node's SSD."""
    refs = node.pools.reserve_transfer(node.tier("ssd").pool.pool_id, len(names))
    assert len(refs) == len(names)
    for name, ref in zip(names, refs, strict=True):
        with aligned_buffer(PAGE, node.tier("ssd").pool.alignment.memory) as buffer:
            buffer[:] = value_of(name, PAGE)
            node.tier("ssd").pool.write(
                node.pools.transfer_location(ref).offset, buffer, PAGE
            )
        node.pools.transfer_done(ref, Key(NS, name), PAGE)
        location = node.pools.page(ref).location
        commit = node.server.commit([CommitItem(Key(NS, name), location, PAGE)])
        assert commit.items[0].result == ItemResult.ADDED
        node.pools.commit_added(ref)
        node.manager.placement.on_ready(ref)


@pytest.mark.parametrize("short", ["rm", "mapping"])
def test_the_window_fits_the_regions_fetches_reach(
    real_server, sockdir, ssd_dir, regions, short
):
    """Of the regions the tier asks for, the RM gives one, or the node
    cannot map one for transfer: the window fits the one copies can go to."""
    server, url = real_server
    config = cxl_ssd(url, sockdir, ssd_dir)
    rm = FakeRm()
    map_region = regions.node_map
    if short == "rm":
        rm.capacity = 8 * PAGE
    else:

        def map_region(pool):
            mapping = regions.node_map(pool)  # workers still map it
            if len(regions.fds) == 1:
                mapping.close()
                raise OSError("injected")
            return mapping

    node = start_node(
        config,
        host_boot_id="boot-1",
        region_source=rm,
        cxl_renew_s=0.05,
        map_region=map_region,
    )
    loader = None
    try:
        (reached,) = [
            p for p in node.tier("cxl").pools if node.transfers.has_pool(p.pool_id)
        ]
        assert reached.info().capacity == 8 * PAGE
        budgets = config.budgets
        room = (
            8 * PAGE
            - budgets.credit_total_max_bytes
            - budgets.commit_pending_max_bytes
            - PAGE
        )
        assert PAGE <= room < budgets.fetch_request_max_bytes
        loader = Loader(node, server)
        assert loader.budgets.fetch_request_max_bytes == room
        if short == "mapping":  # a full window goes through
            names = [f"s{i}" for i in range(room // PAGE)]
            seed_ssd(node, names)
            answer = loader.fetch(*names)
            assert [o.result for o in answer.items] == [ItemResult.HIT] * len(names)
            for name, outcome in zip(names, answer.items, strict=True):
                assert outcome.location.pool_id == reached.pool_id
                assert region_bytes(node, regions, outcome.location) == value_of(
                    name, PAGE
                )
            loader.unpin(*[o.pin_id for o in answer.items])
            wait_until(lambda: settled(node, server), "quota back")
    finally:
        if loader is not None:
            loader.close()
        node.stop()


@pytest.mark.parametrize("how", ["timeout", "closed"])
def test_a_waiter_pins_the_source_once_the_leader_left(env, monkeypatch, how):
    server, make = env
    node, keys = make()
    name = keys[0]
    fetches = node.manager.fetches
    reserve, room = fetches._reserve, threading.Event()
    monkeypatch.setattr(
        fetches, "_reserve", lambda count: reserve(count) if room.is_set() else []
    )
    a, b = Loader(node, server), Loader(node, server, tp_rank=1)
    first = a.send(
        a.request(a.items(name), deadline_ms=150 if how == "timeout" else 5000)
    )
    wait_until(lambda: fetches.flights() == 1, "the flight waits for room")
    second = b.send(b.request(b.items(name)))
    wait_until(lambda: fetches.results["joined"] == 1, "joined")
    if how == "timeout":
        assert first.result(10).items[0].result == ItemResult.MISS
    a.session.close()
    room.set()
    hit = second.result(10).items[0]
    assert hit.result == ItemResult.HIT and b.read(hit.location) == value_of(name)
    if how == "closed":  # asked with its session first: refused, b's used next
        assert first.result(10).items[0].result == ItemResult.REJECTED
    wait_until(lambda: pins(server, name, hit.location) == 1, "only b's pin")
    b.unpin(hit.pin_id)
    wait_until(lambda: node_pins(server, node) == 0, "no node pin left")
    wait_until(lambda: settled(node, server), "quota back")
    node.pools.check()
    a.close()
    b.close()


# =============================================================================
# Review fixes: per-flight progress, shared room, resends, exits, ends
# =============================================================================


def test_a_flight_reads_while_another_of_its_op_waits_for_room(env, monkeypatch):
    server, make = env
    node, keys = make()
    fetches = node.manager.fetches
    reserve, room = fetches._reserve, threading.Event()
    given = []

    def one_page(count):
        if given and not room.is_set():
            return []
        refs = reserve(min(count, 1))
        given.extend(refs)
        return refs

    monkeypatch.setattr(fetches, "_reserve", one_page)
    reads = Reads(node, monkeypatch)
    loader = Loader(node, server)
    reply = loader.send(loader.request(loader.items(*keys[:2])))
    # the first read runs while the second flight still has no page
    wait_until(lambda: reads.calls == 1 and fetches.results["reads"] == 1, "read")
    assert len(given) == 1 and not reply.done()
    room.set()
    answer = reply.result(10)
    assert [o.result for o in answer.items] == [ItemResult.HIT] * 2
    loader.unpin(*[o.pin_id for o in answer.items])
    wait_until(lambda: settled(node, server), "quota back")
    loader.close()


def test_two_loaders_share_the_room_and_both_go_on(env, monkeypatch):
    """Each worker's window is its share of the room at its ATTACH. FETCHes
    short of room take it in turn: even windows that add up to more than the
    room (an earlier worker's) never hold pages the other waits for, also
    when room comes a page at a time."""
    server, make = env
    node, _ = make(keys=40)
    fetches = node.manager.fetches
    reserve = fetches._reserve
    monkeypatch.setattr(fetches, "_reserve", lambda count: reserve(min(count, 1)))
    names = on_ssd(server, [f"k{i}" for i in range(40)])
    a, b = Loader(node, server), Loader(node, server, tp_rank=1)
    room = 11  # 16 DRAM pages less credit, commit pending and reserve
    assert a.budgets.fetch_request_max_bytes == room * PAGE
    assert b.budgets.fetch_request_max_bytes == room // 2 * PAGE
    halves = {a: names[::2], b: names[1::2]}
    done, errors = {a: 0, b: 0}, []
    together = threading.Barrier(2)

    def load(loader):
        try:
            mine = halves[loader]
            for i in range(0, len(mine), room):  # both as large as a's window
                batch = mine[i : i + room]
                request = loader.request(loader.items(*batch), deadline_ms=2000)
                together.wait(10)
                answer = loader.send(request).result(10)
                assert [o.result for o in answer.items] == [ItemResult.HIT] * len(
                    batch
                ), [(o.result, o.reason) for o in answer.items]
                for name, outcome in zip(batch, answer.items, strict=True):
                    assert loader.read(outcome.location) == value_of(name)
                loader.unpin(*[o.pin_id for o in answer.items])
                done[loader] += len(batch)
        except BaseException as e:
            together.abort()
            errors.append(e)

    threads = [threading.Thread(target=load, args=(w,)) for w in (a, b)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(60)
    assert not errors, errors
    assert done == {a: len(halves[a]), b: len(halves[b])}
    wait_until(lambda: settled(node, server), "quota back")
    node.pools.check()
    a.close()
    b.close()


def test_room_goes_by_arrival_not_by_which_task_runs_first(env, monkeypatch):
    """b comes after a and joins a flight of a's. b's task runs first, but
    it waits for a's turn: it holds no page a needs while it waits for a's
    flight."""
    server, make = env
    node, _ = make(keys=40)
    names = on_ssd(server, [f"k{i}" for i in range(40)])
    a, b = Loader(node, server), Loader(node, server, tp_rank=1)
    assert a.budgets.fetch_request_max_bytes == 11 * PAGE
    assert b.budgets.fetch_request_max_bytes == 5 * PAGE
    writer = RawWorker(node.config.socket)
    writer.attach()
    assert len(writer.credit(2 * PAGE).credits) == 2  # 14 pages for a's 11, b's 4
    fetches = node.manager.fetches
    run, first = fetches._run, fetches._first
    paused, go, refused = threading.Event(), threading.Event(), threading.Event()
    a_names = names[:11]
    b_names = [*names[11:15], a_names[-1]]

    def a_runs_late(flights):
        if any(f.leader.op.origin == a.raw.incarnation for f in flights):
            paused.set()
            assert go.wait(10)
        return run(flights)

    def first_or_refused(turn):
        ok = first(turn)
        if not ok:
            refused.set()
        return ok

    monkeypatch.setattr(fetches, "_run", a_runs_late)
    monkeypatch.setattr(fetches, "_first", first_or_refused)

    def unpin_on_answer(loader, future):
        loader.unpin(*[o.pin_id for o in future.result().items if o.pin_id])

    try:
        replies = []
        for loader, batch in ((a, a_names), (b, b_names)):
            reply = loader.send(loader.request(loader.items(*batch), deadline_ms=3000))
            reply.add_done_callback(lambda f, w=loader: unpin_on_answer(w, f))
            replies.append(reply)
            assert paused.wait(10)
        wait_until(refused.is_set, "b waits for a's turn")
        assert fetches.results["joined"] == 1 and no_transfer_pages(node)
        go.set()
        for reply, batch in zip(replies, (a_names, b_names), strict=True):
            outcomes = [(o.result, o.reason) for o in reply.result(10).items]
            assert outcomes == [(ItemResult.HIT, None)] * len(batch)
        wait_until(lambda: settled(node, server), "quota back")
        node.pools.check()
    finally:
        go.set()
        for worker in (a, b, writer):
            worker.close()


def test_published_copies_are_pinned_in_turn_too(env, monkeypatch):
    """b comes after a with four keys published after its LOOKUP, plus a's
    last. b pins none of those copies before a's turn: the pins would keep
    pages a waits for, while b waits for a's flight."""
    server, make = env
    node, _ = make(keys=40)
    names = on_ssd(server, [f"k{i}" for i in range(40)])
    a, b = Loader(node, server), Loader(node, server, tp_rank=1)
    assert a.budgets.fetch_request_max_bytes == 11 * PAGE
    assert b.budgets.fetch_request_max_bytes == 5 * PAGE
    a_names, cached = names[:11], names[11:15]
    b_items = b.items(*cached, a_names[-1])  # planned before the copies
    warm = a.fetch(*cached)
    assert [o.result for o in warm.items] == [ItemResult.HIT] * 4
    # Keep these copies alive until b has joined the room queue. Otherwise
    # background eviction may remove them before the cached-flight assertion.
    wait_until(lambda: settled(node, server), "the copies published")
    writer = RawWorker(node.config.socket)
    writer.attach()
    assert len(writer.credit(2 * PAGE).credits) == 2
    fetches = node.manager.fetches
    run, first = fetches._run, fetches._first
    paused, go, waits = threading.Event(), threading.Event(), threading.Event()

    def a_runs_late(flights):
        if any(f.leader.op.origin == a.raw.incarnation for f in flights):
            paused.set()
            assert go.wait(10)
        return run(flights)

    def b_waits(turn):
        ok = first(turn)
        if not ok and turn.origin == b.raw.incarnation:
            waits.set()
        return ok

    def b_pins() -> int:
        with server.ms_service.lock:
            return len(server.ms_service.pins.held_by(b.token))

    monkeypatch.setattr(fetches, "_run", a_runs_late)
    monkeypatch.setattr(fetches, "_first", b_waits)

    def unpin_on_answer(loader, future):
        loader.unpin(*[o.pin_id for o in future.result().items if o.pin_id])

    try:
        first_reply = a.send(a.request(a.items(*a_names), deadline_ms=3000))
        first_reply.add_done_callback(lambda f: unpin_on_answer(a, f))
        assert paused.wait(10)
        second = b.send(b.request(b_items, deadline_ms=3000))
        second.add_done_callback(lambda f: unpin_on_answer(b, f))
        wait_until(lambda: waits.is_set() or b_pins() == 4, "b waits or pins")
        assert b_pins() == 0 and fetches.results["joined"] == 1
        with fetches._lock:
            flights = list(fetches._flights.values())
        led = [f for f in flights if f.leader.op.origin == b.raw.incarnation]
        assert len(led) == 4 and all(f.cached for f in led)
        a.unpin(*[o.pin_id for o in warm.items])
        go.set()
        for reply, count in ((first_reply, 11), (second, 5)):
            outcomes = [(o.result, o.reason) for o in reply.result(10).items]
            assert outcomes == [(ItemResult.HIT, None)] * count
        wait_until(lambda: settled(node, server), "quota back")
        node.pools.check()
    finally:
        go.set()
        for worker in (a, b, writer):
            worker.close()


def test_a_resend_may_carry_less_of_its_deadline(env, monkeypatch):
    server, make = env
    node, keys = make()
    loader = Loader(node, server)
    reads = Reads(node, monkeypatch).hold()
    request = loader.request(loader.items(keys[0]), deadline_ms=5000)
    first = loader.send(request)
    assert reads.entered.wait(5)
    # the same op with the time left: joins it, its first deadline stands
    resent = loader.send(dataclasses.replace(request, deadline_ms=10))
    time.sleep(0.1)
    assert not resent.done()
    reads.release()
    answer = first.result(10)
    assert answer.items[0].result == ItemResult.HIT
    assert resent.result(10) == answer and reads.calls == 1
    loader.unpin(answer.items[0].pin_id)
    loader.close()


def test_cancels_after_the_answer_or_of_a_detached_worker_are_not_kept(env):
    server, make = env
    node, keys = make()
    fetches = node.manager.fetches
    loader = Loader(node, server)
    request = loader.request(loader.items(keys[0]))
    hit = loader.send(request).result(10).items[0]
    assert hit.result == ItemResult.HIT
    assert loader.cancel(request).status == Status.OK
    with fetches._lock:
        assert not fetches._cancelled.get(loader.raw.incarnation)
    # one for a FETCH not seen yet is kept, until the worker detaches
    assert loader.cancel(loader.request(loader.items(keys[1]))).status == Status.OK
    with fetches._lock:
        assert len(fetches._cancelled[loader.raw.incarnation]) == 1
    loader.unpin(hit.pin_id)
    detached = loader.raw.request(NodeMessageType.DETACH, DetachRequest())
    assert detached.status == Status.OK
    with fetches._lock:
        assert loader.raw.incarnation not in fetches._cancelled
    loader.close()


def test_a_refused_unpin_ends_the_run(env, monkeypatch):
    server, make = env
    node, keys = make()
    loader = Loader(node, server)

    def refused(ids):
        return UnpinResponse(
            status=Status.OK, items=[ItemOutcome(ItemResult.REJECTED)] * len(ids)
        )

    monkeypatch.setattr(node.manager.metadata, "unpin", refused)
    loader.fetch(keys[0])  # its node pin goes after the fan-out
    wait_until(lambda: node.manager.ended is not None, "the run ends")
    assert "UNPIN" in node.manager.ended
    loader.close()


def test_a_failure_inside_a_fetch_ends_the_run(env, monkeypatch):
    server, make = env
    node, keys = make()
    loader = Loader(node, server)

    def broken(flights):
        raise RuntimeError("injected")

    monkeypatch.setattr(node.manager.fetches, "_run", broken)
    answer = loader.fetch(keys[0])
    assert answer.items[0] == miss(MissReason.BUSY)
    wait_until(lambda: node.manager.ended is not None, "the run ends")
    assert "injected" in node.manager.ended
    loader.close()


def test_a_copy_is_published_for_a_waiter_once_the_leader_left(env, monkeypatch):
    server, make = env
    node, keys = make()
    name = keys[0]
    sessions = []
    complete = node.manager.metadata.fetch_complete

    def recorded(session, items, **kwargs):
        sessions.append((session, [i.hold_copy for i in items]))
        return complete(session, items, **kwargs)

    monkeypatch.setattr(node.manager.metadata, "fetch_complete", recorded)
    a, b = Loader(node, server), Loader(node, server, tp_rank=1)
    reads = Reads(node, monkeypatch).hold()
    first = a.send(a.request(a.items(name), deadline_ms=150))
    assert reads.entered.wait(5)
    second = b.send(b.request(b.items(name)))
    wait_until(lambda: node.manager.fetches.results["joined"] == 1, "joined")
    assert first.result(10).items[0] == miss(MissReason.TIMEOUT)
    reads.release()
    hit = second.result(10).items[0]
    assert hit.result == ItemResult.HIT and b.read(hit.location) == value_of(name)
    # published with b's session, b's pin in the same request
    assert sessions == [(b.token, [True])]
    b.unpin(hit.pin_id)
    wait_until(lambda: settled(node, server), "quota back")
    a.close()
    b.close()
