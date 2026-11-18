# SPDX-License-Identifier: Apache-2.0
"""Worker FETCH from a memory node, with a real catalog and a fake NIC."""

import dataclasses
import threading
import time

import pytest

from maru_common.storage_types import ItemResult, MissReason, PoolKind, Status
from maru_storage_service.page_table import InvalidTransitionError, PageRef, PageState
from tests.unit.ms_node_helpers import PAGE, wait_demotions_applied, wait_until
from tests.unit.remote_fakes import reset_fake_agents
from tests.unit.test_ms_demotion import put, value_of
from tests.unit.test_ms_fetch import Loader, loader_client, pins, settled
from tests.unit.test_ms_remote_demotion import Pair
from tests.unit.test_ms_remote_demotion import ms as ms
from tests.unit.test_ms_remote_demotion import workers as workers

NS = "ns"


@pytest.fixture(autouse=True)
def _fresh_agents():
    reset_fake_agents()
    yield
    reset_fake_agents()


@pytest.fixture
def env(ms, workers):
    server, url = ms
    pair = Pair(server, url, timeout_s=5.0)
    loaders = []
    try:
        writer = workers(pair.gpu)
        names = [f"k{i}" for i in range(12)]
        for name in names:
            assert put(writer, name)
        wait_until(
            lambda: sum(pair.on_memory(n) for n in names) >= 2,
            what="keys demoted to memory",
        )
        writer.close()
        pair.gpu.manager.placement.set_watermarks("dram", 0.95, 0.9)
        # An empty write queue alone can precede applying WRITE_DONE and
        # another eviction pass. Settle both before a test stalls the NIC.
        wait_demotions_applied(pair.gpu.manager)
        moved = [n for n in names if pair.on_memory(n)]

        def loader():
            worker = Loader(pair.gpu, server, tp_rank=len(loaders))
            loaders.append(worker)
            return worker

        yield pair, moved, loader
    finally:
        pair.remote.transport.stall(False)
        pair.remote.transport.finish_stalled()
        for worker in loaders:
            worker.close()
        pair.close()


class Reads:
    """Observe posted reads after the tier has accepted them."""

    def __init__(self, pair, monkeypatch):
        self.entered = threading.Event()
        self.calls = 0
        self.tickets = []
        submit = pair.remote.submit_load

        def recorded(*args, **kwargs):
            ticket = submit(*args, **kwargs)
            self.tickets.append(ticket)
            self.calls += 1
            self.entered.set()
            return ticket

        monkeypatch.setattr(pair.remote, "submit_load", recorded)


def local_copies(pair, name):
    pool_id = pair.gpu.tier("dram").pool.pool_id
    return [loc for loc in pair.locations(name) if loc.pool_id == pool_id]


def test_a_worker_fetches_a_demoted_key_and_publishes_a_dram_copy(env):
    pair, names, _ = env
    name = names[0]
    (source,) = pair.locations(name)
    worker = loader_client(pair.gpu)
    assert pair.gpu.manager.fetches is not None
    assert pair.remote.io_size(PAGE) == 0
    try:
        with worker.fetch_scope("read-back"):
            lease = worker.retrieve(name)
        assert lease is not None, worker._cpu.stats
        try:
            assert bytes(lease.view) == value_of(name)
            (copy,) = local_copies(pair, name)
            assert copy.kind == PoolKind.DRAM
            assert pins(pair.server, name, source) == 0
        finally:
            lease.release()
    finally:
        worker.close()
    wait_until(lambda: settled(pair.gpu, pair.server), what="flight ended")
    pair.gpu.pools.check()


@pytest.mark.parametrize(
    "bad", ["offline", "owner", "self", "unknown", "ssd", "length"]
)
def test_an_unusable_remote_plan_is_an_immediate_offline_miss(env, monkeypatch, bad):
    pair, names, make_loader = env
    loader = make_loader()
    item = loader.items(names[0])[0]
    if bad == "offline":
        monkeypatch.setattr(pair.remote, "ready", lambda _node: False)
    elif bad in ("owner", "self"):
        item = dataclasses.replace(
            item,
            plan=dataclasses.replace(
                item.plan,
                source_node="other" if bad == "owner" else pair.gpu.config.node_id,
            ),
        )
    else:
        changes = {
            "unknown": {"pool_id": "dram:unknown:1"},
            "ssd": {"kind": PoolKind.SSD},
            "length": {"length": item.plan.bytes + 1},
        }[bad]
        item = dataclasses.replace(
            item,
            plan=dataclasses.replace(
                item.plan, source=dataclasses.replace(item.plan.source, **changes)
            ),
        )
    reads = Reads(pair, monkeypatch)
    reply = loader.send(loader.request([item], deadline_ms=5000)).result(2)
    assert reply.status == Status.OK
    assert reply.items[0].result == ItemResult.MISS
    assert reply.items[0].reason == MissReason.OFFLINE
    assert reads.calls == 0 and settled(pair.gpu, pair.server)


def test_a_read_past_the_request_deadline_is_published_as_cache(env, monkeypatch):
    pair, names, make_loader = env
    name = names[0]
    (source,) = pair.locations(name)
    loader = make_loader()
    reads = Reads(pair, monkeypatch)
    pair.remote.transport.stall()
    reply = loader.send(loader.request(loader.items(name), deadline_ms=300))
    assert reads.entered.wait(2)
    ticket = reads.tickets[0]
    assert ticket.src is None and ticket.dst is not None
    assert pair.gpu.pools.io_refs(ticket.dst) == 1
    assert ticket.deadline is not None
    assert reply.result(2).items[0].reason == MissReason.TIMEOUT
    assert pins(pair.server, name, source) == 1
    assert pair.gpu.manager.fetches.flights() == 1
    assert not local_copies(pair, name)
    pair.remote.transport.stall(False)
    assert pair.remote.transport.finish_stalled() == 1
    wait_until(lambda: settled(pair.gpu, pair.server), what="late read published")
    (copy,) = local_copies(pair, name)
    assert loader.read(copy) == value_of(name)
    assert pins(pair.server, name, source) == pins(pair.server, name, copy) == 0
    assert ticket.ok and ticket.late
    assert pair.gpu.pools.io_refs(ticket.dst) == 0
    pair.gpu.pools.check()


@pytest.mark.parametrize("failure", ["err", "timeout"])
def test_a_read_without_done_quarantines_its_destination(env, monkeypatch, failure):
    pair, names, make_loader = env
    name = names[0]
    (source,) = pair.locations(name)
    loader = make_loader()
    reads = Reads(pair, monkeypatch)
    completions = []
    complete = pair.gpu.server.fetch_complete

    def recorded(session, items, **kwargs):
        completions.extend(items)
        return complete(session, items, **kwargs)

    monkeypatch.setattr(pair.gpu.server, "fetch_complete", recorded)
    pair.remote.timeout_s = 0.3
    if failure == "err":
        pair.remote.transport.fail_next()
    else:
        pair.remote.transport.stall()
    answer = loader.fetch(name, deadline_ms=1000).items[0]
    assert answer.result == ItemResult.MISS
    assert answer.reason == (
        MissReason.IO_ERROR if failure == "err" else MissReason.TIMEOUT
    )
    assert reads.entered.wait(2)
    ticket = reads.tickets[0]
    dest = ticket.dst
    pools = pair.gpu.pools
    assert dest is not None
    wait_until(lambda: settled(pair.gpu, pair.server), what="failed read ended")
    assert pools.page(dest).state == PageState.QUARANTINED
    assert pools.page(dest).tag == "rdma:mem1"
    assert pools.io_refs(dest) == 0
    assert pair.remote.kept_bytes == pair.gpu.config.page_size  # the whole page
    assert pins(pair.server, name, source) == 0
    assert not local_copies(pair, name)
    assert len(completions) == 1 and completions[0].logical_bytes == 0
    reserved = pools.reserve_transfer(dest.pool_id, 100)
    assert dest not in reserved
    for ref in reserved:
        pools.transfer_failed(ref)
    pools.check()
    if failure == "timeout":
        pair.remote.transport.stall(False)
        assert pair.remote.transport.finish_stalled() == 1
        wait_until(
            lambda: pools.page(dest).state == PageState.FREE,
            what="late DONE freed quarantine",
        )
        assert pair.remote.kept_bytes == 0
        assert not local_copies(pair, name)
    else:
        assert ticket.state.released
        assert pools.page(dest).state == PageState.QUARANTINED
    pools.check()


def test_the_quarantine_cap_prevents_another_remote_read(env, monkeypatch):
    pair, names, make_loader = env
    loader = make_loader()
    reads = Reads(pair, monkeypatch)
    items = loader.items(*names[:2])
    pair.remote.quarantine_max = pair.gpu.config.page_size  # one kept page
    pair.remote.transport.fail_next()
    assert loader.send(loader.request(items[:1])).result(2).items[0].result == (
        ItemResult.MISS
    )
    wait_until(lambda: settled(pair.gpu, pair.server), what="quarantined read ended")
    assert not pair.remote.ready("mem1")
    answer = loader.send(loader.request(items[1:])).result(2).items[0]
    assert answer.result == ItemResult.MISS and answer.reason == MissReason.OFFLINE
    assert reads.calls == 1 and settled(pair.gpu, pair.server)


def test_a_late_done_can_settle_before_the_flight_handles_its_failure(env, monkeypatch):
    pair, names, make_loader = env
    name = names[0]
    loader = make_loader()
    reads = Reads(pair, monkeypatch)
    fetches = pair.gpu.manager.fetches
    result = fetches._read_result
    entered, release = threading.Event(), threading.Event()

    def held(flight):
        entered.set()
        assert release.wait(5)
        return result(flight)

    monkeypatch.setattr(fetches, "_read_result", held)
    pair.remote.timeout_s = 0.2
    pair.remote.transport.stall()
    reply = loader.send(loader.request(loader.items(name), deadline_ms=300))
    try:
        assert reads.entered.wait(2) and entered.wait(2)
        dest = reads.tickets[0].dst
        assert pair.gpu.pools.page(dest).state == PageState.QUARANTINED
        pair.remote.transport.stall(False)
        assert pair.remote.transport.finish_stalled() == 1
        wait_until(
            lambda: pair.gpu.pools.page(dest).state == PageState.FREE,
            what="settled before failure handling",
        )
    finally:
        release.set()
    assert reply.result(2).items[0].result == ItemResult.MISS
    wait_until(lambda: settled(pair.gpu, pair.server), what="flight ended")
    assert not local_copies(pair, name)
    pair.gpu.pools.check()


def test_a_late_copy_is_not_published_after_server_give_up(env, monkeypatch):
    pair, names, make_loader = env
    name = names[0]
    (source,) = pair.locations(name)
    loader = make_loader()
    reads = Reads(pair, monkeypatch)
    pair.remote.transport.stall()
    reply = loader.send(loader.request(loader.items(name), deadline_ms=300))
    assert reads.entered.wait(2)
    assert reply.result(2).items[0].reason == MissReason.TIMEOUT
    assert pins(pair.server, name, source) == 1
    pair.memory.server.stop()
    service = pair.server.ms_service
    with service.lock:
        registry = service.registry
        session = registry.nodes["mem1"].session
        session.deadline = time.monotonic() - registry.node_give_up_s - 1
        assert registry.give_up(time.monotonic(), 1) == 1
        registry.maintain(time.monotonic(), 1000)
    assert pins(pair.server, name, source) == 0
    pair.remote.transport.stall(False)
    assert pair.remote.transport.finish_stalled() == 1
    wait_until(lambda: settled(pair.gpu, pair.server), what="rejected late copy")
    assert reads.tickets[0].ok
    assert not local_copies(pair, name)
    assert pair.gpu.pools.page(reads.tickets[0].dst).state == PageState.FREE
    pair.gpu.pools.check()


def test_only_an_unreferenced_transfer_page_can_be_quarantined(env):
    pair, _, make_loader = env
    loader = make_loader()
    pools = pair.gpu.pools
    pool_id = pair.gpu.tier("dram").pool.pool_id
    (ref,) = pools.reserve_transfer(pool_id, 1)
    location = pools.transfer_location(ref)
    assert pools.io_acquire_transfer(ref)
    with pytest.raises(InvalidTransitionError, match="I/O running"):
        pools.quarantine_transfer(ref, "rdma:mem1")
    pools.io_release([ref])
    stale = PageRef(ref.pool_id, ref.index, ref.generation + 1)
    assert not pools.io_acquire_transfer(stale)
    pools.quarantine_transfer(ref, "rdma:mem1")
    assert not pools.io_acquire_transfer(ref)
    assert pools.resolve_quarantined(location, "rdma:other")[0] is None
    assert pools.page(ref).state == PageState.QUARANTINED
    assert pools.resolve_quarantined(location, "rdma:mem1")[0] == ref
    assert pools.page(ref).state == PageState.FREE
    assert loader.budgets.fetch_request_max_bytes == 11 * PAGE
    pools.check()


def test_a_read_from_a_lost_memory_node_misses_at_once(env, monkeypatch):
    pair, names, make_loader = env
    name = names[0]
    loader = make_loader()
    reads = Reads(pair, monkeypatch)
    pair.remote.transport.stall()
    start = time.monotonic()
    reply = loader.send(loader.request(loader.items(name), deadline_ms=5000))
    assert reads.entered.wait(2)
    pair.memory.stop()  # its stream ends; the READ stays posted
    answer = reply.result(2).items[0]
    assert time.monotonic() - start < 2.5  # not the 5 s deadline
    assert answer.result == ItemResult.MISS
    assert answer.reason == MissReason.OFFLINE
    assert pair.gpu.manager.fetches.results["node_lost"] == 1
    assert pair.gpu.manager.fetches.flights() == 0  # no one joins it
    # The posted READ still ends its own way (the peer went: ERR, kept)
    dest = reads.tickets[0].dst
    pair.remote.transport.stall(False)
    pair.remote.transport.finish_stalled()
    wait_until(lambda: pair.gpu.pools.io_refs(dest) == 0, what="the read ended")
    assert pair.gpu.pools.page(dest).state == PageState.QUARANTINED
    pair.gpu.pools.check()


def test_only_reads_begun_on_the_lost_connection_miss(env, monkeypatch):
    pair, names, make_loader = env
    name = names[0]
    loader = make_loader()
    reads = Reads(pair, monkeypatch)
    fetches = pair.gpu.manager.fetches
    conn = pair.remote.connection("mem1")
    assert conn > 0
    pair.remote.transport.stall()
    reply = loader.send(loader.request(loader.items(name), deadline_ms=5000))
    assert reads.entered.wait(2)
    fetches.node_lost("mem1", conn - 1)  # an earlier connection's late notice
    with pytest.raises(TimeoutError):
        reply.result(0.3)
    fetches.node_lost("mem1", conn)
    assert reply.result(2).items[0].reason == MissReason.OFFLINE
    # A FETCH planned on the ended connection misses at once
    again = loader.send(loader.request(loader.items(names[1]), deadline_ms=5000))
    assert again.result(2).items[0].reason == MissReason.OFFLINE
    pair.remote.transport.stall(False)
    assert pair.remote.transport.finish_stalled() == 1
    wait_until(lambda: settled(pair.gpu, pair.server), what="the read published")
    pair.gpu.pools.check()


def test_a_read_that_ended_is_not_missed_on_a_lost_connection(env, monkeypatch):
    pair, names, make_loader = env
    name = names[0]
    loader = make_loader()
    fetches = pair.gpu.manager.fetches
    conn = pair.remote.connection("mem1")
    publishing, release = threading.Event(), threading.Event()
    complete = pair.gpu.server.fetch_complete

    def held(session, items, **kwargs):  # the read is done; it is publishing
        publishing.set()
        assert release.wait(10)
        return complete(session, items, **kwargs)

    monkeypatch.setattr(pair.gpu.server, "fetch_complete", held)
    reply = loader.send(loader.request(loader.items(name), deadline_ms=5000))
    assert publishing.wait(5)
    fetches.node_lost("mem1", conn)
    release.set()
    answer = reply.result(5).items[0]
    assert answer.result == ItemResult.HIT
    loader.unpin(answer.pin_id)
    wait_until(lambda: settled(pair.gpu, pair.server), what="published")
    pair.gpu.pools.check()
