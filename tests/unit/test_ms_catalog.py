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
    REMOTE_CAPABILITY,
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
    assert node.open(pools=pools).status == Status.OK
    return node


def gpu_and_memory(wire, cxl=False):
    """GPU node a, which reads memory nodes over RDMA, and memory node m."""
    a = open_node(wire, "a", cxl=cxl, caps=[REMOTE_CAPABILITY])
    m = open_node(wire, "m", caps=[MEMORY_CAPABILITY])
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
            ("FETCH", m_dram, m),  # a memory node's DRAM
        ]
        for result, location, owner in expected:
            response = worker.lookup(k)
            item = response.items[0]
            assert item.result == result
            if result == "HIT":
                assert item.location == location and item.pin_id
                worker.unpin(item.pin_id)
            else:
                assert item.plan.source == location
                assert item.plan.source_node == owner.node_id
                assert item.plan.bytes == location.length
                assert item.pin_id is None
            # PROBE gives the same answer without pin or location
            hint = probe.probe(k).items[0]
            assert hint.result == result
            assert hint.location is None and hint.pin_id is None and hint.plan is None
            # with_locations names the location chosen, still with no pin
            named = probe.probe(k, with_locations=True).items[0]
            assert (named.result, named.location) == (result, location)
            assert named.pin_id is None and named.plan is None
            # retire the chosen one; the next one is chosen after it
            assert results(owner.retire((k, location))) == ["RETIRED"]
        miss = worker.lookup(k).items[0]
        assert miss.result == "MISS" and miss.reason == MissReason.OFFLINE
        assert probe.probe(k, with_locations=True).items[0].location is None
        assert results(b.retire((k, b_dram), (k, b_cxl))) == ["RETIRED"] * 2
        miss = worker.lookup(k).items[0]
        assert miss.result == "MISS" and miss.reason == MissReason.ABSENT

    def test_fetch_plan_names_source_node(self, env):
        _, _, wire = env
        a, m = gpu_and_memory(wire)
        k = key("k")
        m.commit((k, m.loc(m.dram(), 3), 100))
        worker = FakeWorker(wire, a)
        worker.open()
        item = worker.lookup(k).items[0]
        assert item.result == "FETCH"
        assert item.plan.source_node == "m" and item.plan.bytes == 100

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
        assert worker.lookup(k).items[0].result == "FETCH"
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
            "FETCH",
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


class TestFetchSources:
    """A node fetches only memory nodes' pools, with remote I/O agreed:
    another GPU node serves no FETCH, so its copies are never a plan."""

    def test_a_memory_nodes_copy_is_fetched_not_another_gpu_nodes(self, env):
        _, _, wire = env
        a, m = gpu_and_memory(wire)
        b = open_node(wire, "b", cxl=True)
        k = key("k")
        # as cheap by cost as m's DRAM, but not fetchable by a
        b.commit((k, b.loc(b.dram(), 1)))
        b.commit((k, b.loc(b.cxl(region_id=region("b")), 1)))
        source = m.loc(m.dram(), 1)
        m.commit((k, source))
        worker = FakeWorker(wire, a)
        worker.open()
        probe = FakeWorker(wire, a, role=SessionRole.PROBE)
        probe.open()
        item = worker.lookup(k).items[0]
        assert item.result == "FETCH"
        assert item.plan.source == source and item.plan.source_node == "m"
        assert probe.probe(k).items[0].result == "FETCH"

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
        assert item.plan is None
        assert probe.probe(k).items[0].reason == MissReason.OFFLINE
        # b's own worker still maps it
        b_probe = FakeWorker(wire, b, role=SessionRole.PROBE)
        b_probe.open()
        assert b_probe.probe(k).items[0].result == "HIT"
        assert results(b.retire((k, b_dram), (k, b_cxl))) == ["RETIRED"] * 2
        assert worker.lookup(k).items[0].reason == MissReason.ABSENT

    def test_can_fetch_is_only_memory_nodes_pools_with_remote_agreed(self, env):
        _, service, wire = env
        open_node(wire, "a", cxl=True, caps=[REMOTE_CAPABILITY])
        open_node(wire, "b", cxl=True)
        open_node(wire, "m", cxl=True, caps=[MEMORY_CAPABILITY])
        pools = [p for n in "abm" for p in service.catalog.pools_of(n)]
        assert len(pools) == 6

        def fetchable(node_id):
            return {
                (p.node_id, p.kind)
                for p in pools
                if service.registry.can_fetch(node_id, p)
            }

        assert fetchable("a") == {("m", PoolKind.DRAM), ("m", PoolKind.CXL)}
        # no remote_v1 (b), or only its own pools, which it maps (m)
        assert fetchable("b") == fetchable("m") == set()


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


class TestFetch:
    def _setup(self, wire):
        a, m = gpu_and_memory(wire)
        worker = FakeWorker(wire, a)
        worker.open()
        k = key("k")
        source = m.loc(m.dram(), 1)
        m.commit((k, source))
        return a, m, worker, k, source

    def test_source_pin_and_fetch_complete(self, env):
        _, service, wire = env
        a, m, worker, k, source = self._setup(wire)
        plan = worker.lookup(k).items[0]
        assert plan.result == "FETCH" and plan.plan.source == source
        pinned = a.source_pin((k, plan.plan.source))
        assert results(pinned) == ["HIT"]
        source_pin = pinned.items[0].pin_id
        # the source pin keeps the source from its node's retire
        assert m.retire((k, source)).items[0].cause == KeptCause.PINNED
        done = a.fetch_complete((k, source_pin))
        assert results(done) == ["OK"]
        assert done.items[0].location is None and done.items[0].pin_id is None
        # the source pin went with it, and no copy came in
        assert service.pins.get(source_pin) is None
        assert [e.location for e in service.catalog.locations(k)] == [source]
        assert results(m.retire((k, source))) == ["RETIRED"]

    def test_a_fetched_copy_is_never_published(self, env):
        _, service, wire = env
        a, m, worker, k, source = self._setup(wire)
        pin = a.source_pin((k, source)).items[0].pin_id
        copy = a.loc(a.dram(), 1)
        done = a.fetch_complete((k, pin), copy=copy)
        assert results(done) == ["REJECTED"]
        assert done.items[0].location is None and done.items[0].pin_id is None
        # the source pin goes all the same; the copy is no location
        assert service.pins.get(pin) is None and len(service.pins) == 0
        assert [e.location for e in service.catalog.locations(k)] == [source]
        assert worker.lookup(k).items[0].result == "FETCH"

    def test_fetch_complete_rejections(self, env):
        _, service, wire = env
        a, m, worker, k, source = self._setup(wire)
        # a source pin that is not there
        done = a.fetch_complete((k, "nope"))
        assert results(done) == ["REJECTED"]
        assert service.catalog.entry_exact(k, source) is not None
        # another node's source pin: neither read under nor released
        b = open_node(wire, "b", caps=[REMOTE_CAPABILITY])
        foreign = b.source_pin((k, source)).items[0].pin_id
        assert results(a.fetch_complete((k, foreign))) == ["REJECTED"]
        assert service.pins.get(foreign) is not None
        # a pin of another key: refused, and released (it is the node's)
        other, where = key("other"), m.loc(m.dram(), 2)
        m.commit((other, where))
        pin = a.source_pin((other, where)).items[0].pin_id
        assert results(a.fetch_complete((k, pin))) == ["REJECTED"]
        assert service.pins.get(pin) is None

    def test_a_read_pin_is_not_a_source_pin(self, env):
        _, service, wire = env
        a, m, worker, k, source = self._setup(wire)
        hit = key("hit")
        a.commit((hit, a.loc(a.dram(), 1)))
        read = worker.lookup(hit).items[0].pin_id
        done = a.fetch_complete((hit, read))
        assert results(done) == ["REJECTED"]
        assert service.pins.get(read) is not None

    def test_source_pin_misses(self, env):
        clock, _, wire = env
        a, m, worker, k, source = self._setup(wire)
        gone = replace(source, generation=source.generation + 5)
        response = a.source_pin((k, gone))
        assert results(response) == ["MISS"]
        assert response.items[0].reason == MissReason.ABSENT
        clock.advance(20)
        a.heartbeat()
        clock.advance(15)
        response = a.source_pin((k, source))
        assert response.items[0].reason == MissReason.OFFLINE

    def test_fetch_complete_resend_replays_its_first_answer(self, env):
        _, service, wire = env
        a, m, worker, k, source = self._setup(wire)
        from maru_common.protocol import (
            FetchCompleteItem,
            FetchCompleteRequest,
            MessageType,
        )

        pin = a.source_pin((k, source)).items[0].pin_id
        request = a.build(
            FetchCompleteRequest,
            **a.envelope(),
            items=[FetchCompleteItem(k, pin, PAGE)],
        )
        first = wire.call(MessageType.FETCH_COMPLETE, request)
        assert results(first) == ["OK"]
        # the reply was lost; the resend gets the same result, not REJECTED
        assert wire.call(MessageType.FETCH_COMPLETE, request) == first


class TestWorkerBinding:
    """SOURCE_PIN for a named worker: only its own open session on this
    node's run, with its namespace, gets a pin."""

    def _setup(self, wire):
        """A key on memory node m, as a's worker's fetch reads it; b is
        another GPU node."""
        a, m = gpu_and_memory(wire)
        b = open_node(wire, "b")
        worker = FakeWorker(wire, a)
        worker.open()
        k = key("k")
        source = m.loc(m.dram(), 1)
        m.commit((k, source))
        return a, b, worker, k, source

    def _bound_pin(self, a, worker, k, location):
        from maru_common.protocol import MessageType, SourcePinRequest
        from maru_common.storage_types import KeyLocation

        return a.send(
            MessageType.SOURCE_PIN,
            a.build(
                SourcePinRequest,
                **a.envelope(),
                items=[KeyLocation(k, location)],
                worker_session=worker.token,
                worker_incarnation=worker.incarnation,
            ),
        )

    def test_a_workers_source_pin_is_only_on_pools_its_node_fetches(self, env):
        _, service, wire = env
        a, b, worker, _, _ = self._setup(wire)
        k = key("on-b")
        elsewhere = b.loc(b.dram(), 2)
        b.commit((k, elsewhere))
        assert results(self._bound_pin(a, worker, k, elsewhere)) == ["REJECTED"]
        assert len(service.pins) == 0
        # the node's own SOURCE_PIN, for no worker, is not limited
        assert results(a.source_pin((k, elsewhere))) == ["HIT"]

    def test_a_workers_source_pin_agrees_with_the_lookup(self, env):
        _, _, wire = env
        a, b, worker, k, source = self._setup(wire)
        b_dram = b.loc(b.dram(), 2)
        b.commit((k, b_dram))
        # the lookup's source is pinned; another GPU node's DRAM is not
        assert worker.lookup(k).items[0].plan.source == source
        assert results(self._bound_pin(a, worker, k, source)) == ["HIT"]
        assert results(self._bound_pin(a, worker, k, b_dram)) == ["REJECTED"]
        # a copy in the node's own DRAM is pinned for its worker
        copy = a.loc(a.dram(), 2)
        a.commit((k, copy))
        assert results(self._bound_pin(a, worker, k, copy)) == ["HIT"]

    def test_source_pin_refuses_another_workers_session(self, env):
        from maru_common.protocol import MessageType, SourcePinRequest
        from maru_common.storage_types import KeyLocation

        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        other = FakeWorker(wire, a, tp_rank=1)
        other.open()

        def pin(session: str, incarnation: str, keyed=k):
            return a.send(
                MessageType.SOURCE_PIN,
                a.build(
                    SourcePinRequest,
                    **a.envelope(),
                    items=[KeyLocation(keyed, source)],
                    worker_session=session,
                    worker_incarnation=incarnation,
                ),
            )

        refused = pin(other.token, worker.incarnation)
        assert refused.status == Status.REJECTED and len(service.pins) == 0
        assert pin("nope", worker.incarnation).status == Status.REJECTED
        foreign = replace(k, namespace="other")
        assert pin(worker.token, worker.incarnation, foreign).status == Status.REJECTED
        assert results(pin(worker.token, worker.incarnation)) == ["HIT"]

    def test_a_resend_after_the_worker_left_replays(self, env):
        from maru_common.protocol import (
            FetchCompleteItem,
            FetchCompleteRequest,
            MessageType,
            SourcePinRequest,
        )
        from maru_common.storage_types import KeyLocation

        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        bound = {
            "worker_session": worker.token,
            "worker_incarnation": worker.incarnation,
        }
        pin_request = a.build(
            SourcePinRequest,
            **a.envelope(),
            items=[KeyLocation(k, source)],
            **bound,
        )
        pinned = wire.call(MessageType.SOURCE_PIN, pin_request)
        assert results(pinned) == ["HIT"]
        done_request = a.build(
            FetchCompleteRequest,
            **a.envelope(),
            items=[FetchCompleteItem(k, pinned.items[0].pin_id, PAGE)],
        )
        worker.close()
        # the replies were lost: the same pin comes back, none is added
        assert wire.call(MessageType.SOURCE_PIN, pin_request) == pinned
        assert len(service.pins) == 1
        done = wire.call(MessageType.FETCH_COMPLETE, done_request)
        assert done.status == Status.OK and len(service.pins) == 0
        assert wire.call(MessageType.FETCH_COMPLETE, done_request) == done
        # a new op for the worker that went is refused, and replays so
        late = a.build(
            SourcePinRequest,
            **a.envelope(),
            items=[KeyLocation(k, source)],
            **bound,
        )
        refused = wire.call(MessageType.SOURCE_PIN, late)
        assert refused.status == Status.REJECTED and len(service.pins) == 0
        assert wire.call(MessageType.SOURCE_PIN, late) == refused


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


class TestPlanSourcePins:
    """LOOKUP_AND_PIN with pin_sources: a FETCH plan carries a source pin,
    held by the worker's session for its node run, which reads under it and
    releases it in FETCH_COMPLETE or UNPIN."""

    def _setup(self, wire):
        """A key on memory node m, planned for a's worker; b is another GPU
        node that reads m too."""
        a, self.m = gpu_and_memory(wire)
        b = open_node(wire, "b", caps=[REMOTE_CAPABILITY])
        worker = FakeWorker(wire, a)
        worker.open()
        k = key("k")
        source = self.m.loc(self.m.dram(), 1)
        self.m.commit((k, source))
        return a, b, worker, k, source

    def _plan_pin(self, worker, k) -> str:
        item = worker.lookup(k, pin_sources=True).items[0]
        assert item.result == "FETCH" and item.plan.pin_id and item.pin_id is None
        return item.plan.pin_id

    def test_a_plan_has_a_source_pin_only_when_asked(self, env):
        _, service, wire = env
        a, _, worker, k, source = self._setup(wire)
        assert worker.lookup(k).items[0].plan.pin_id is None
        assert len(service.pins) == 0
        pin_id = self._plan_pin(worker, k)
        pin = service.pins.get(pin_id)
        assert (pin.holder, pin.holder_kind) == (worker.token, "source")
        assert pin.location == source and pin.creator == (a.node_id, a.epoch)
        # it keeps the source from its node's retire, as a SOURCE_PIN does
        assert self.m.retire((k, source)).items[0].cause == KeptCause.PINNED
        # a HIT takes its read pin only
        hit = key("hit")
        a.commit((hit, a.loc(a.dram(), 2)))
        assert worker.lookup(hit, pin_sources=True).items[0].plan is None
        assert len(service.pins) == 2

    def test_its_node_run_reads_under_it_and_fetch_complete_releases_it(self, env):
        _, service, wire = env
        a, _, worker, k, source = self._setup(wire)
        pin_id = self._plan_pin(worker, k)
        done = a.fetch_complete((k, pin_id, PAGE, source, worker.incarnation))
        assert results(done) == ["OK"]
        assert service.pins.get(pin_id) is None
        assert results(self.m.retire((k, source))) == ["RETIRED"]
        # the source read is reported to its node, as for a SOURCE_PIN
        assert service.registry._access["m"]

    def test_a_read_of_another_location_is_rejected_and_the_pin_released(self, env):
        _, service, wire = env
        a, _, worker, k, source = self._setup(wire)
        pin_id = self._plan_pin(worker, k)
        elsewhere = replace(source, allocation_id="a2", offset=source.offset + PAGE)
        done = a.fetch_complete((k, pin_id, PAGE, elsewhere, worker.incarnation))
        assert results(done) == ["REJECTED"]
        assert service.pins.get(pin_id) is None
        assert "m" not in service.registry._access

    def test_only_the_workers_node_run_uses_or_releases_it(self, env):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        pin_id = self._plan_pin(worker, k)
        named = worker.incarnation
        done = b.fetch_complete((k, pin_id, PAGE, source, named))
        assert results(done) == ["REJECTED"]
        assert results(b.unpin(pin_id, worker_incarnation=named)) == ["REJECTED"]
        # a's node releases it only naming the worker
        assert results(a.unpin(pin_id)) == ["REJECTED"]
        assert service.pins.get(pin_id) is not None
        assert results(a.unpin(pin_id, worker_incarnation=named)) == ["OK"]
        assert service.pins.get(pin_id) is None

    def test_the_worker_unpins_a_plan_it_does_not_fetch(self, env):
        _, service, wire = env
        _, _, worker, k, _ = self._setup(wire)
        pin_id = self._plan_pin(worker, k)
        assert results(worker.unpin(pin_id)) == ["OK"]
        assert len(service.pins) == 0

    @pytest.mark.parametrize("end", ["close", "exit", "new_run"])
    def test_it_goes_with_the_worker_session(self, env, end):
        _, service, wire = env
        a, _, worker, k, _ = self._setup(wire)
        pin_id = self._plan_pin(worker, k)
        if end == "close":
            assert worker.close().status == Status.OK
        elif end == "exit":
            assert results(a.worker_exit(worker.ref())) == ["OK"]
        else:
            a.restart()
            assert a.open(pools=[a.dram()]).status == Status.OK
            service.maintain()  # earlier runs' worker pins go in steps
        assert service.pins.get(pin_id) is None

    def test_a_reopened_session_keeps_it_for_the_same_run(self, env):
        _, service, wire = env
        a, _, worker, k, source = self._setup(wire)
        pin_id = self._plan_pin(worker, k)
        old = worker.token
        assert worker.open().status == Status.OK and worker.token != old
        assert service.pins.get(pin_id).holder == worker.token
        done = a.fetch_complete((k, pin_id, PAGE, source, worker.incarnation))
        assert results(done) == ["OK"] and service.pins.get(pin_id) is None

    def test_a_replayed_lookup_gives_the_same_pin(self, env):
        from maru_common.protocol import LookupAndPinRequest, MessageType

        _, service, wire = env
        _, _, worker, k, _ = self._setup(wire)
        request = worker.build(
            LookupAndPinRequest, **worker.envelope(), items=[k], pin_sources=True
        )
        first = wire.call(MessageType.LOOKUP_AND_PIN, request)
        again = wire.call(MessageType.LOOKUP_AND_PIN, request)
        assert first.items[0].plan.pin_id == again.items[0].plan.pin_id
        assert len(service.pins) == 1

    def _others(self, wire, a, worker, k, source):
        """Pins a node run must not release or read under for ``worker``:
        another worker's plan pin and read pin, and the node's own pin."""
        other = FakeWorker(wire, a, tp_rank=1)
        other.open()
        plan = self._plan_pin(other, k)
        own = a.source_pin((k, source)).items[0].pin_id
        k2 = key("k2")
        a.commit((k2, a.loc(a.dram(), 2)))
        read = other.lookup(k2).items[0].pin_id
        assert own and read
        return other, {"plan": plan, "read": read, "node": own}

    def test_the_node_releases_only_the_named_workers_source_pins(self, env):
        _, service, wire = env
        a, _, worker, k, source = self._setup(wire)
        mine = self._plan_pin(worker, k)
        _, others = self._others(wire, a, worker, k, source)
        named = worker.incarnation
        response = a.unpin(*others.values(), mine, "gone", worker_incarnation=named)
        assert results(response) == ["REJECTED"] * 3 + ["OK", "OK"]
        assert all(service.pins.get(p) is not None for p in others.values())
        assert service.pins.get(mine) is None
        # without the worker: never a source pin, nor a worker's read pin
        assert results(a.unpin(others["plan"])) == ["REJECTED"]
        assert results(a.unpin(others["read"])) == ["REJECTED"]

    @pytest.mark.parametrize("which", ["plan", "read", "node"])
    def test_fetch_complete_reads_only_under_the_named_workers_pin(self, env, which):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        _, others = self._others(wire, a, worker, k, source)
        pin_id = others[which]
        named = worker.incarnation
        done = a.fetch_complete((k, pin_id, PAGE, source, named))
        assert results(done) == ["REJECTED"]
        assert service.pins.get(pin_id) is not None
        # nor does an item that names no worker read under a source pin
        if which == "plan":
            done = a.fetch_complete((k, pin_id, PAGE, source))
            assert results(done) == ["REJECTED"]
            assert service.pins.get(pin_id) is not None

    def test_one_on_cxl_keeps_no_orphan_session(self):
        from maru_common.storage_types import Key, Location
        from maru_server.pin_manager import HolderKind, PinManager

        pins = PinManager()
        cxl = Location("cxl:m:rm1:7", "a1", 1, 0, PAGE, PoolKind.CXL)
        pin_id = pins.pin(Key("ns", "k"), cxl, "w", HolderKind.SOURCE, ("n", "e"))
        assert pins.holds("w") and not pins.holds_cxl("w")
        assert pins.release_holder("w", keep_cxl=True) == 1
        assert pins.get(pin_id) is None
