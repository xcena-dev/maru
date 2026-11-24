# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Catalog and pins through MsService with fake nodes and workers: lookup
order, commit, retire, source pins, fetch_complete, preservation, unpin.
A memory node's pool is the FETCH source."""

import dataclasses
from dataclasses import replace

import pytest

from maru_common.storage_types import (
    MEMORY_CAPABILITY,
    WORKER_RDMA_CAPABILITY,
    KeptCause,
    MissReason,
    PoolKind,
    SessionRole,
    Status,
)
from tests.unit.ms_fakes import (
    PAGE,
    FakeClock,
    FakeNode,
    FakeWorker,
    Wire,
    key,
    make_service,
    policy_service,
)


@pytest.fixture
def env():
    clock = FakeClock()
    service = make_service(clock)
    wire = Wire(service)
    return clock, service, wire


def region(node_id: str) -> int:
    return ord(node_id[0])


def open_node(wire, node_id, cxl=False, caps=()):
    node = FakeNode(wire, node_id)
    node.capabilities = list(caps)
    pools = [node.dram()]
    if cxl:
        pools.append(node.cxl(region_id=region(node_id)))
    assert node.open(pools=pools, pid_ns=101).status == Status.OK
    return node


def gpu_and_memory(wire, cxl=False):
    """GPU node a, which reads memory nodes over RDMA, and memory node m."""
    a = open_node(wire, "a", cxl=cxl, caps=[WORKER_RDMA_CAPABILITY])
    m = open_node(wire, "m", caps=[MEMORY_CAPABILITY])
    assert a.heartbeat(memory_peer=(m.node_id, m.epoch)).status == Status.OK
    return a, m


def results(response):
    return [item.result for item in response.items]


class TestLookupOrder:
    def test_several_locations_are_chosen_in_cost_order(self, env):
        clock, service, wire = env
        a, m = gpu_and_memory(wire, cxl=True)
        b = open_node(wire, "b", cxl=True)
        k = key("k")
        a_dram = a.loc(a.dram(), 1)
        a_cxl = a.loc(a.cxl(region_id=region("a")), 1)
        m_dram = m.loc(m.dram(), 1)
        b_dram = b.loc(b.dram(), 1)
        b_cxl = b.loc(b.cxl(region_id=region("b")), 1)
        # committed in reverse order of cost
        assert results(b.commit((k, b_cxl))) == ["ADDED"]
        assert results(b.commit((k, b_dram))) == ["ADDED"]
        assert results(m.commit((k, m_dram))) == ["ADDED"]
        assert results(a.commit((k, a_cxl))) == ["ADDED"]
        assert results(a.commit((k, a_dram))) == ["ADDED"]
        worker = FakeWorker(wire, a)
        assert worker.open().status == Status.OK
        probe = FakeWorker(wire, a, role=SessionRole.PROBE)
        assert probe.open().status == Status.OK

        # b's copies are another GPU node's: never a HIT or a FETCH for a
        expected = [
            ("HIT", a_dram, a),  # node DRAM
            ("HIT", a_cxl, a),  # node CXL
            ("HIT", m_dram, m),  # a memory node's DRAM
        ]
        for result, location, owner in expected:
            response = worker.lookup(k)
            item = response.items[0]
            assert item.result == result
            assert item.location == location and item.pin_id
            worker.unpin(item.pin_id)
            # PROBE gives the same answer without pin or location
            hint = probe.probe(k).items[0]
            assert hint.result == result
            assert hint.location is None and hint.pin_id is None
            # with_locations names the location chosen, still with no pin
            named = probe.probe(k, with_locations=True).items[0]
            assert (named.result, named.location) == (result, location)
            assert named.pin_id is None
            # retire the chosen one; the next one is chosen after it
            assert results(owner.retire((k, location))) == ["RETIRED"]
        miss = worker.lookup(k).items[0]
        assert miss.result == "MISS" and miss.reason == MissReason.OFFLINE
        assert probe.probe(k, with_locations=True).items[0].location is None
        assert results(b.retire((k, b_dram), (k, b_cxl))) == ["RETIRED"] * 2
        miss = worker.lookup(k).items[0]
        assert miss.result == "MISS" and miss.reason == MissReason.ABSENT

    def test_probe_session_gives_no_pins(self, env):
        _, service, wire = env
        a = open_node(wire, "a")
        k = key("k")
        a.commit((k, a.loc(a.dram(), 1)))
        probe = FakeWorker(wire, a, role=SessionRole.PROBE)
        probe.open()
        assert probe.probe(k).items[0].result == "HIT"
        assert len(service.pins) == 0
        response = probe.lookup(k)
        assert response.status == Status.REJECTED
        assert probe.unpin("x").status == Status.REJECTED
        assert probe.heartbeat().status == Status.OK
        assert probe.close().status == Status.OK


class TestMissReasons:
    def test_absent_vs_offline(self, env):
        clock, _, wire = env
        a, m = gpu_and_memory(wire)
        k = key("k")
        m.commit((k, m.loc(m.dram(), 1)))
        worker = FakeWorker(wire, a)
        worker.open()
        assert worker.lookup(key("other")).items[0].reason == MissReason.ABSENT
        assert worker.lookup(k).items[0].result == "HIT"
        # the sessions expire: no StorageService can send the bytes
        clock.advance(20)
        worker.heartbeat()
        clock.advance(15)
        item = worker.lookup(k).items[0]
        assert item.result == "MISS" and item.reason == MissReason.OFFLINE
        assert worker.probe(k).items[0].reason == MissReason.OFFLINE

    def test_fetch_needs_the_requesters_storage_service_too(self, env):
        clock, _, wire = env
        a, m = gpu_and_memory(wire)
        b = open_node(wire, "b")
        k_remote, k_memory, k_dram = key("remote"), key("memory"), key("dram")
        b.commit((k_remote, b.loc(b.dram(), 1)))
        m.commit((k_memory, m.loc(m.dram(), 1)))
        a.commit((k_dram, a.loc(a.dram(), 2)))
        worker = FakeWorker(wire, a)
        worker.open()
        # another GPU node's DRAM is never fetched
        assert results(worker.lookup(k_remote, k_memory, k_dram)) == [
            "MISS",
            "HIT",
            "HIT",
        ]
        # a's session expires while b and m stay up
        clock.advance(20)
        b.heartbeat()
        m.heartbeat()
        worker.heartbeat()
        clock.advance(15)
        response = worker.lookup(k_remote, k_memory, k_dram)
        assert results(response) == ["MISS", "MISS", "HIT"]
        assert [i.reason for i in response.items[:2]] == [MissReason.OFFLINE] * 2

    def test_restart_close_hides_dram_but_not_cxl(self, env):
        _, service, wire = env
        a = open_node(wire, "a", cxl=True)
        k_dram, k_cxl = key("d"), key("c")
        a.commit((k_dram, a.loc(a.dram(), 1)))
        a.commit((k_cxl, a.loc(a.cxl(region_id=region("a")), 1)))
        worker = FakeWorker(wire, a)
        worker.open()
        assert results(worker.lookup(k_dram, k_cxl)) == ["HIT", "HIT"]
        assert a.close("restart").status == Status.OK
        response = worker.lookup(k_dram, k_cxl)
        assert results(response) == ["MISS", "HIT"]
        assert response.items[0].reason == MissReason.OFFLINE
        # the closed session's requests get EPOCH
        assert a.heartbeat().status == Status.EPOCH


class TestDirectSources:
    """A node fetches only memory nodes' pools, with remote I/O agreed:
    another GPU node serves no FETCH, so its copies are never a plan."""

    def test_only_another_gpu_nodes_copy_is_an_offline_miss(self, env):
        _, _, wire = env
        a, _ = gpu_and_memory(wire)
        b = open_node(wire, "b", cxl=True)
        k = key("k")
        b_dram, b_cxl = b.loc(b.dram(), 1), b.loc(b.cxl(region_id=region("b")), 1)
        b.commit((k, b_dram), (k, b_cxl))
        worker = FakeWorker(wire, a)
        worker.open()
        probe = FakeWorker(wire, a, role=SessionRole.PROBE)
        probe.open()
        # a location, but none a maps or fetches: OFFLINE, as for any
        # copy the requester cannot reach
        item = worker.lookup(k).items[0]
        assert item.result == "MISS" and item.reason == MissReason.OFFLINE
        assert probe.probe(k).items[0].reason == MissReason.OFFLINE
        # b's own worker still maps it
        b_probe = FakeWorker(wire, b, role=SessionRole.PROBE)
        b_probe.open()
        assert b_probe.probe(k).items[0].result == "HIT"
        assert results(b.retire((k, b_dram), (k, b_cxl))) == ["RETIRED"] * 2
        assert worker.lookup(k).items[0].reason == MissReason.ABSENT


class TestCommit:
    def test_resend_same_op_gives_same_result(self, env):
        _, service, wire = env
        a = open_node(wire, "a")
        from maru_common.protocol import CommitItem, CommitRequest, MessageType

        loc = a.loc(a.dram(), 1)
        request = a.build(
            CommitRequest, **a.envelope(), items=[CommitItem(key("k"), loc, PAGE)]
        )
        first = wire.call(MessageType.COMMIT, request)
        assert results(first) == ["ADDED"]
        again = wire.call(MessageType.COMMIT, request)
        assert again == first
        assert len(service.catalog.locations(key("k"))) == 1

    def test_duplicate_and_rejections(self, env):
        _, service, wire = env
        a = open_node(wire, "a", cxl=True)
        b = open_node(wire, "b")
        k = key("k")
        first = a.loc(a.dram(), 1)
        assert results(a.commit((k, first))) == ["ADDED"]
        # same key, same pool: DUPLICATE with the location already there
        response = a.commit((k, a.loc(a.dram(), 2)))
        assert results(response) == ["DUPLICATE"]
        assert response.items[0].location == first
        # same key, another pool: one more location
        assert results(a.commit((k, a.loc(a.cxl(region_id=region("a")), 1)))) == [
            "ADDED"
        ]
        assert len(service.catalog.locations(k)) == 2
        # a pool of another node
        assert results(a.commit((key("x"), b.loc(b.dram(), 5)))) == ["REJECTED"]
        # a page that already holds another key's location
        taken = a.loc(a.dram(), 1)
        assert results(a.commit((key("y"), taken))) == ["REJECTED"]
        # a generation of another run (not this epoch_seq)
        bad = a.loc(a.dram(), 9)
        from dataclasses import replace

        old_run = replace(bad, generation=bad.generation - (1 << 40))
        assert results(a.commit((key("z"), old_run))) == ["REJECTED"]
        # beyond the pool, wrong kind, bad logical bytes
        beyond = replace(a.loc(a.dram(), 10), offset=a.dram().capacity)
        wrong_kind = replace(a.loc(a.dram(), 11), kind=PoolKind.CXL)
        assert results(a.commit((key("p"), beyond), (key("q"), wrong_kind))) == [
            "REJECTED",
            "REJECTED",
        ]
        assert results(a.commit((key("r"), a.loc(a.dram(), 12), 0))) == ["REJECTED"]
        # an unknown pool
        unknown = replace(a.loc(a.dram(), 13), pool_id="dram:a:other")
        assert results(a.commit((key("s"), unknown))) == ["REJECTED"]

    def test_same_location_again_is_added_not_duplicate(self, env):
        _, service, wire = env
        a = open_node(wire, "a")
        k = key("k")
        loc = a.loc(a.dram(), 1)
        a.commit((k, loc))
        # a new op naming the very same location: it is in, not a duplicate
        again = a.commit((k, loc))
        assert results(again) == ["ADDED"]
        assert [e.location for e in service.catalog.locations(k)] == [loc]
        assert len(service.pins) == 0

    def test_an_adopted_location_is_added_again_in_the_new_run(self, env):
        _, service, wire = env
        a = open_node(wire, "a", cxl=True)
        k = key("k")
        cxl = a.cxl(region_id=region("a"))
        old = a.loc(cxl, 1)
        assert results(a.commit((k, old))) == ["ADDED"]
        a.restart()
        response = a.open(pools=[a.dram(), cxl])
        assert [p.pool_id for p in response.adopted] == [cxl.pool_id]
        # the page keeps the old run's generation: in already, so ADDED
        assert results(a.commit((k, old))) == ["ADDED"]
        assert [e.location for e in service.catalog.locations(k)] == [old]
        # a new location still needs this run's generation
        assert results(a.commit((key("x"), old))) == ["REJECTED"]
        other = replace(old, allocation_id="a2", offset=2 * PAGE)
        assert results(a.commit((key("y"), other))) == ["REJECTED"]

    def test_overlapping_bytes_are_rejected(self, env):
        _, _, wire = env
        a = open_node(wire, "a")
        first = a.loc(a.dram(), 1, length=2 * PAGE)  # pages 1 and 2
        assert results(a.commit((key("a"), first))) == ["ADDED"]
        from dataclasses import replace

        inside = replace(a.loc(a.dram(), 9), offset=first.offset + PAGE + 10, length=8)
        assert results(a.commit((key("b"), inside))) == ["REJECTED"]
        before = replace(a.loc(a.dram(), 8), offset=first.offset - 8, length=16)
        assert results(a.commit((key("c"), before))) == ["REJECTED"]
        after = replace(a.loc(a.dram(), 7), offset=first.offset + 2 * PAGE, length=8)
        assert results(a.commit((key("d"), after))) == ["ADDED"]

    def test_an_item_asking_for_a_finalizer_is_rejected(self, env):
        """No finalizer is kept here: such an item is refused, not taken as
        a plain commit; the others of the request go on."""
        from maru_common.protocol import CommitItem, CommitRequest, MessageType
        from maru_common.serializer import op_digest

        _, service, wire = env
        a = open_node(wire, "a")
        plain, asking = a.loc(a.dram(), 1), a.loc(a.dram(), 2)
        request = a.build(
            CommitRequest,
            **a.envelope(),
            items=[
                CommitItem(key("p"), plain, PAGE),
                CommitItem(key("f"), asking, PAGE),
            ],
        )
        payload = dataclasses.asdict(request)
        payload["items"][1]["finalizer"] = True
        payload["op"]["digest"] = op_digest(payload)
        response = wire.call(MessageType.COMMIT, payload)
        assert response.status == Status.OK
        assert results(response) == ["ADDED", "REJECTED"]
        assert not service.catalog.locations(key("f"))
        assert len(service.pins) == 0

    def test_batch_over_limit_is_rejected(self, env):
        _, _, wire = env
        a = open_node(wire, "a")
        items = [(key(f"k{i}"), a.loc(a.dram(), i)) for i in range(1025)]
        assert a.commit(*items).status == Status.REJECTED


class TestRetire:
    def test_kept_pinned_while_pinned_then_retired(self, env):
        _, _, wire = env
        a = open_node(wire, "a")
        k = key("k")
        loc = a.loc(a.dram(), 1)
        a.commit((k, loc))
        worker = FakeWorker(wire, a)
        worker.open()
        pin_id = worker.lookup(k).items[0].pin_id
        response = a.retire((k, loc))
        assert results(response) == ["KEPT"]
        assert response.items[0].cause == KeptCause.PINNED
        assert worker.unpin(pin_id).items[0].result == "OK"
        assert results(a.retire((k, loc))) == ["RETIRED"]
        assert worker.lookup(k).items[0].reason == MissReason.ABSENT

    def test_kept_moved_names_the_occupant_of_the_page(self, env):
        _, _, wire = env
        a = open_node(wire, "a")
        k = key("k")
        loc = a.loc(a.dram(), 1)
        a.commit((k, loc))
        # the same key with another generation on the page
        stale = replace(loc, generation=loc.generation + 1)
        response = a.retire((k, stale))
        assert results(response) == ["KEPT"]
        item = response.items[0]
        assert item.cause == KeptCause.MOVED
        assert (item.key, item.location) == (k, loc)
        # another key on the page, by allocation or by overlapping bytes
        for other in (
            replace(loc, generation=loc.generation + 2),
            replace(loc, allocation_id="x", offset=loc.offset + 1, length=8),
        ):
            item = a.retire((key("j"), other)).items[0]
            assert item.cause == KeptCause.MOVED
            assert (item.key, item.location) == (k, loc)

    def test_retired_when_the_page_is_free_even_if_the_key_is_elsewhere(self, env):
        _, service, wire = env
        a = open_node(wire, "a")
        k = key("k")
        loc = a.loc(a.dram(), 1)
        a.commit((k, loc))
        elsewhere = a.loc(a.dram(), 5)  # a page nothing published holds
        assert results(a.retire((k, elsewhere))) == ["RETIRED"]
        assert [e.location for e in service.catalog.locations(k)] == [loc]

    def test_kept_invalid_for_a_pool_that_is_not_the_nodes(self, env):
        _, _, wire = env
        a = open_node(wire, "a")
        b = open_node(wire, "b")
        k = key("k")
        loc = b.loc(b.dram(), 1)
        b.commit((k, loc))
        response = a.retire((k, loc))
        assert response.items[0].cause == KeptCause.INVALID

    def test_preserved_location_is_kept_pinned(self, tmp_path):
        service, wire, preserve = policy_service(tmp_path)
        a = open_node(wire, "a")
        k = key("k")
        loc = a.loc(a.dram(), 1)
        a.commit((k, loc))
        version = a.heartbeat().policy_version
        preserve(k)
        assert a.heartbeat().policy_version == version + 1
        response = a.retire((k, loc))
        assert response.items[0].cause == KeptCause.PINNED
        preserve()
        assert results(a.retire((k, loc))) == ["RETIRED"]

    def test_a_location_already_gone_is_retired(self, env):
        _, _, wire = env
        a = open_node(wire, "a")
        k = key("k")
        loc = a.loc(a.dram(), 1)
        assert results(a.retire((k, loc))) == ["RETIRED"]


class TestUnpin:
    def test_unknown_is_ok_foreign_is_rejected(self, env):
        _, service, wire = env
        a = open_node(wire, "a")
        k = key("k")
        a.commit((k, a.loc(a.dram(), 1)))
        w1, w2 = FakeWorker(wire, a), FakeWorker(wire, a, tp_rank=1)
        w1.open()
        w2.open()
        pin = w1.lookup(k).items[0].pin_id
        response = w2.unpin(pin, "unknown")
        assert results(response) == ["REJECTED", "OK"]
        assert service.pins.get(pin) is not None
        assert results(w1.unpin(pin, pin)) == ["OK", "OK"]
        assert service.pins.get(pin) is None

    def test_several_pins_on_one_location(self, env):
        _, service, wire = env
        a = open_node(wire, "a")
        k = key("k")
        loc = a.loc(a.dram(), 1)
        a.commit((k, loc))
        w = FakeWorker(wire, a)
        w.open()
        pins = [item.pin_id for item in w.lookup(k, k).items]
        assert len(set(pins)) == 2
        assert service.pins.count(k, loc) == 2
        w.unpin(pins[0])
        assert a.retire((k, loc)).items[0].cause == KeptCause.PINNED
        w.unpin(pins[1])
        assert results(a.retire((k, loc))) == ["RETIRED"]
