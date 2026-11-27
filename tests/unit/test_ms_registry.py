# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Node and worker sessions, CXL pools, node restarts, give-up, access info
and block requests, through MsService."""

import time
from dataclasses import replace

import pytest

from maru_common.protocol import MessageType
from maru_common.storage_types import (
    EPOCH_SEQ_MAX,
    MEMORY_CAPABILITY,
    REMOTE_CAPABILITY,
    WORKER_RDMA_CAPABILITY,
    CloseMode,
    KeptCause,
    MissReason,
    OpRef,
    PoolUsage,
    RmDomain,
    SessionRole,
    Status,
)
from maru_server.catalog import Entry, PoolState
from tests.unit.ms_fakes import (
    PAGE,
    POOL_PAGES,
    FakeClock,
    FakeNode,
    FakeWorker,
    Wire,
    admin_delete,
    key,
    make_service,
    policy_service,
)


@pytest.fixture
def env():
    clock = FakeClock()
    service = make_service(clock)
    return clock, service, Wire(service)


def results(response):
    return [item.result for item in response.items]


def drain(service):
    while service.maintain() >= service.maintenance_budget:
        pass


class TestNodeOpen:
    def test_open_gives_epoch_seq_and_times(self, env):
        _, service, wire = env
        node = FakeNode(wire, "n1")
        response = node.open()
        assert response.status == Status.OK
        assert response.server_epoch == service.server_epoch
        assert response.epoch_seq == 1
        assert response.session_ttl_s == 30.0 and response.heartbeat_s == 10.0
        assert response.adopted == []

    def test_open_accepts_only_the_capabilities_served(self, env):
        """Those of another release (stage_v1, finalizer_v1) are not
        accepted: the node sees them missing and does not use them."""
        _, service, wire = env
        node = FakeNode(wire, "n1")
        node.capabilities = [
            REMOTE_CAPABILITY,
            "stage_v1",
            WORKER_RDMA_CAPABILITY,
            "finalizer_v1",
            MEMORY_CAPABILITY,
            REMOTE_CAPABILITY,
        ]
        response = node.open(pid_ns=101)
        accepted = [REMOTE_CAPABILITY, WORKER_RDMA_CAPABILITY, MEMORY_CAPABILITY]
        assert response.status == Status.OK and response.capabilities == accepted
        assert service.registry.capabilities_of("n1") == tuple(accepted)

    @pytest.mark.parametrize(
        "change, needle",
        [
            ({"protocol": "ms0"}, "protocol"),
            ({"node_id": "bad id"}, "node_id"),
            ({"host_boot_id": ""}, "host_boot_id"),
        ],
    )
    def test_bad_open_is_rejected(self, env, change, needle):
        _, _, wire = env
        node = FakeNode(wire, "n1")
        request = replace(node.open_request(), **change)
        response = wire.call(MessageType.NODE_OPEN, request)
        assert response.status == Status.REJECTED and needle in response.error

    def test_bad_pools_are_rejected(self, env):
        _, _, wire = env
        node = FakeNode(wire, "n1")
        for pools in (
            [replace(node.dram(), pool_id="dram:n1:other")],
            [replace(node.dram(), node_id="n2")],
            [replace(node.dram(), pool_generation=0)],
            [node.dram(), node.dram()],
        ):
            assert node.open(pools=pools).status == Status.REJECTED

    def test_same_epoch_reopen_keeps_the_run(self, env):
        clock, service, wire = env
        node = FakeNode(wire, "n1")
        node.open(pools=[node.dram(), node.cxl()])
        k = key("k")
        loc = node.loc(node.cxl(), 1)
        node.commit((k, loc))
        old_token = node.token
        clock.advance(31)  # the session expired; the process is still there
        assert node.heartbeat().status == Status.EPOCH
        response = node.open(pools=[node.dram(), node.cxl()])
        assert response.status == Status.OK and response.epoch_seq == 1
        assert node.token != old_token
        # its pin moved to the new session and its op records stayed
        assert ("node", node.epoch) in service.ledger
        # the old token stays dead
        node.token, new = old_token, node.token
        assert node.heartbeat().status == Status.EPOCH
        node.token = new

    def test_same_epoch_reopen_repeats_adoption(self, env):
        _, service, wire = env
        node = FakeNode(wire, "n1")
        node.open(pools=[node.dram(), node.cxl()])
        node.restart()
        first = node.open(pools=[node.dram(), node.cxl()])
        assert [p.pool_id for p in first.adopted] == [node.cxl().pool_id]
        # the reply was lost and the same process opens again
        again = node.open(pools=[node.dram(), node.cxl()])
        assert again.adopted == first.adopted and again.epoch_seq == first.epoch_seq

    def test_ended_epoch_cannot_come_back(self, env):
        _, _, wire = env
        node = FakeNode(wire, "n1")
        node.open()
        zombie = node.epoch
        node.restart()
        assert node.open().status == Status.OK
        node.epoch = node.origin = zombie
        response = node.open(pools=[node.dram()])
        assert response.status == Status.REJECTED

    def test_epoch_seq_is_bounded(self, env):
        _, service, wire = env
        node = FakeNode(wire, "n1")
        node.open()
        service.registry.nodes["n1"].epoch_seq = EPOCH_SEQ_MAX
        node.restart()
        response = node.open()
        assert response.status == Status.REJECTED and "exhausted" in response.error

    def test_closed_epoch_cannot_reopen(self, env):
        _, _, wire = env
        node = FakeNode(wire, "n1")
        node.open()
        node.close(CloseMode.RESTART)
        assert node.open().status == Status.REJECTED


class TestCxlPools:
    """CXL regions the StorageService holds in its RMs, listed in NODE_OPEN."""

    def test_open_registers_a_cxl_pool(self, env):
        _, service, wire = env
        node = FakeNode(wire, "n1")
        assert node.open(pools=[node.dram(), node.cxl()]).status == Status.OK
        pool = service.catalog.pool(node.cxl().pool_id)
        assert pool.info == node.cxl() and pool.state == PoolState.ACTIVE
        k, loc = key("c"), node.loc(node.cxl(), 1)
        assert results(node.commit((k, loc))) == ["ADDED"]
        worker = FakeWorker(wire, node)
        worker.open()
        hit = worker.lookup(k).items[0]
        assert hit.result == "HIT" and hit.location == loc

    @pytest.mark.parametrize(
        "change, needle",
        [
            ({"pool_id": "cxl:rm1:8"}, "pool_id must be 'cxl:rm1:7'"),
            ({"pool_id": "cxl:rm9:7", "rm_domain": "rm9"}, "not in rm_domains"),
            ({"region_id": None}, "needs its rm_domain and region_id"),
            ({"rm_domain": None}, "needs its rm_domain and region_id"),
        ],
        ids=["pool_id", "unknown-rm_domain", "no-region_id", "no-rm_domain"],
    )
    def test_bad_cxl_pool_is_rejected(self, env, change, needle):
        _, service, wire = env
        node = FakeNode(wire, "n1")
        response = node.open(pools=[node.dram(), replace(node.cxl(), **change)])
        assert response.status == Status.REJECTED and needle in response.error
        assert "n1" not in service.registry.nodes

    def test_rm_domain_listed_twice_is_rejected(self, env):
        _, _, wire = env
        node = FakeNode(wire, "n1")
        twice = [RmDomain("rm1", ("dev0",)), RmDomain("rm1", ("dev0",))]
        response = node.open(rm_domains=twice)
        assert response.status == Status.REJECTED and "twice" in response.error

    def test_nodes_sharing_an_rm_list_their_own_devices(self, env):
        _, _, wire = env
        a, b = FakeNode(wire, "a"), FakeNode(wire, "b")
        assert a.open().status == Status.OK  # rm1 with dev0
        assert b.open(rm_domains=[RmDomain("rm1", ("dev1",))]).status == Status.OK
        assert FakeNode(wire, "c").open(rm_domains=[RmDomain("rm1", ())]).status == (
            Status.OK
        )

    def test_region_of_another_node_is_rejected(self, env):
        _, service, wire = env
        a, b = FakeNode(wire, "a"), FakeNode(wire, "b")
        a.open(pools=[a.dram(), a.cxl()])
        k = key("c")
        a.commit((k, a.loc(a.cxl(), 1)))
        response = b.open(pools=[b.dram(), b.cxl()])
        assert response.status == Status.REJECTED
        assert "registered by node a" in response.error
        # a left while a reader still pins its location: b may take the region
        reader = FakeWorker(wire, a)
        reader.open()
        pin = reader.lookup(k).items[0].pin_id
        a.close(CloseMode.LEAVE)
        assert b.open(pools=[b.dram(), b.cxl()]).status == Status.OK
        assert service.catalog.pool(b.cxl().pool_id).node_id == "b"
        assert service.catalog.pools_of("a") == []
        assert results(b.commit((key("b"), b.loc(b.cxl(), 1)))) == ["ADDED"]
        # a's next run and the old location's last pin leave b's pool alone
        a.restart()
        assert a.open().status == Status.OK
        reader.unpin(pin)
        assert not service.catalog.locations(k)
        pool = service.catalog.pool(b.cxl().pool_id)
        assert pool.node_id == "b" and pool.state == PoolState.ACTIVE
        assert len(pool.entries) == 1


class TestEpochReplacement:
    """A StorageService restarts: NODE_OPEN with a new node_epoch."""

    @staticmethod
    def _pools(node):
        return [node.dram(), node.cxl()]

    def _node(self, wire, service):
        node = FakeNode(wire, "n1")
        assert node.open(pools=self._pools(node)).status == Status.OK
        worker = FakeWorker(wire, node)
        assert worker.open().status == Status.OK
        keys = {name: key(name) for name in ("d", "c")}
        locs = {"d": node.loc(node.dram(), 1), "c": node.loc(node.cxl(), 1)}
        node.commit(*[(keys[n], locs[n]) for n in "dc"])
        return node, worker, keys, locs

    def test_restart_adopts_cxl(self, env):
        _, service, wire = env
        node, worker, keys, locs = self._node(wire, service)
        # the worker reads DRAM and CXL; the node pins the CXL page too
        worker_pins = [i.pin_id for i in worker.lookup(keys["d"], keys["c"]).items]
        old_epoch, old_token, old_dram = node.epoch, node.token, node.dram_id
        node.restart()
        response = node.open(pools=self._pools(node))
        assert response.status == Status.OK
        assert response.epoch_seq == 2
        assert [p.pool_id for p in response.adopted] == [node.cxl().pool_id]
        # the old session gets EPOCH, its pins and op records are gone
        node.token, new_token = old_token, node.token
        assert node.heartbeat().status == Status.EPOCH
        node.token = new_token
        assert old_epoch not in service.ledger
        # Only the worker's own run-scoped DRAM pin goes on departure.
        drain(service)
        assert service.pins.get(worker_pins[0]) is None
        assert service.pins.get(worker_pins[1]) is not None
        assert worker.lookup(keys["d"]).status == Status.EPOCH
        assert worker.open().status == Status.OK
        response = worker.lookup(keys["d"], keys["c"])
        assert results(response) == ["MISS", "HIT"]
        assert response.items[0].reason == MissReason.ABSENT
        # with no pin left, the old DRAM pool goes
        drain(service)
        assert not service.catalog.locations(keys["d"])
        assert service.catalog.pool(old_dram) is None

    def test_recover_list_pages_through_an_adopted_pool(self, env):
        _, service, wire = env
        node = FakeNode(wire, "n1")
        node.open(pools=[node.dram(), node.cxl()])
        locs = [(key(f"k{i}"), node.loc(node.cxl(), i)) for i in range(1500)]
        node.commit(*locs[:1024])
        node.commit(*locs[1024:])
        worker = FakeWorker(wire, node)
        worker.open()
        node.restart()
        response = node.open(pools=[node.dram(), node.cxl()])
        cxl = node.cxl().pool_id
        assert [p.pool_id for p in response.adopted] == [cxl]
        first = node.recover_list(cxl)
        assert first.status == Status.OK
        assert len(first.items) == 1024 and first.next_cursor
        # a resent page is the same page
        assert node.recover_list(cxl).items[0] == first.items[0]
        again = node.recover_list(cxl, first.next_cursor)
        assert again == node.recover_list(cxl, first.next_cursor)
        assert len(again.items) == 476 and again.next_cursor is None
        listed = {(i.key, i.location) for i in first.items + again.items}
        assert listed == set(locs)
        assert all(i.logical_bytes == 4096 and not i.pinned for i in again.items)
        # only adopted pools, only known cursors
        assert node.recover_list(node.dram_id).status == Status.REJECTED
        assert node.recover_list(cxl, "9:0").status == Status.REJECTED
        assert node.recover_list(cxl, "garbage").status == Status.REJECTED

    def test_recover_list_releases_its_snapshot_after_the_last_page(self, env):
        _, service, wire = env
        node = FakeNode(wire, "n1")
        node.open(pools=[node.dram(), node.cxl()])
        items = [(key(f"k{i}"), node.loc(node.cxl(), i)) for i in range(1030)]
        node.commit(*items[:1024])
        node.commit(*items[1024:])
        node.restart()
        node.open(pools=[node.dram(), node.cxl()])
        cxl = node.cxl().pool_id
        first = node.recover_list(cxl)
        last = node.recover_list(cxl, first.next_cursor)
        assert last.next_cursor is None and len(last.items) == 6
        listing = next(iter(service.registry.nodes["n1"].listings.values()))
        assert listing.entries == []
        # a resent last page is still answered the same
        assert node.recover_list(cxl, first.next_cursor) == last
        assert node.recover_list(cxl, "1:0").status == Status.REJECTED

    def test_recover_list_reports_pins(self, env):
        _, service, wire = env
        node, worker, keys, locs = self._node(wire, service)
        worker.lookup(keys["c"])
        node.restart()
        node.open(pools=self._pools(node))
        listed = node.recover_list(node.cxl().pool_id)
        assert [(i.key, i.location, i.pinned) for i in listed.items] == [
            (keys["c"], locs["c"], True)
        ]

    def test_reboot_drops_cxl(self, env):
        _, service, wire = env
        node, worker, keys, locs = self._node(wire, service)
        worker.lookup(keys["c"])  # its pin goes with the old run
        node.restart(boot="boot-2")
        response = node.open(pools=self._pools(node))
        assert response.adopted == []
        # hidden at once, removed with no pin left
        worker.open()
        response = worker.lookup(keys["c"])
        assert results(response) == ["MISS"]
        drain(service)
        assert not service.catalog.locations(keys["c"])
        # the region, listed again, starts empty in this run
        pool = service.catalog.pool(node.cxl().pool_id)
        assert pool.state == PoolState.ACTIVE and not pool.entries
        assert pool.since_epoch_seq == 2
        assert results(node.commit((keys["c"], node.loc(node.cxl(), 1)))) == ["ADDED"]

    def test_cxl_needs_the_same_rm_domain(self, env):
        _, service, wire = env
        node, _, keys, _ = self._node(wire, service)
        node.restart()
        response = node.open(
            pools=[node.dram(), node.cxl(rm_domain="rm2")],
            rm_domains=[RmDomain("rm2", ())],
        )
        assert response.adopted == []
        drain(service)
        assert not service.catalog.locations(keys["c"])

    def test_cxl_not_listed_is_removed(self, env):
        _, service, wire = env
        node, worker, keys, _ = self._node(wire, service)
        worker.lookup(keys["c"])  # its pin goes with the old run
        node.restart()
        # the RM no longer holds the region for the node
        response = node.open(pools=[node.dram()])
        assert response.adopted == []
        worker.open()
        assert worker.lookup(keys["c"]).items[0].result == "MISS"
        drain(service)
        assert not service.catalog.locations(keys["c"])
        assert service.catalog.pool(node.cxl().pool_id) is None

    @pytest.mark.parametrize(
        "change",
        [{"capacity": 2 * POOL_PAGES * PAGE}, {"page_size": 2 * PAGE}],
        ids=["capacity", "page_size"],
    )
    def test_cxl_listed_with_another_size_starts_empty(self, env, change):
        _, service, wire = env
        node, _, keys, _ = self._node(wire, service)
        node.restart()
        changed = replace(node.cxl(), **change)
        response = node.open(pools=[node.dram(), changed])
        assert response.adopted == []
        drain(service)
        assert not service.catalog.locations(keys["c"])
        pool = service.catalog.pool(changed.pool_id)
        assert pool.info == changed and not pool.entries

    def test_cxl_adopted_as_the_new_run_lists_it(self, env):
        _, service, wire = env
        node, _, keys, locs = self._node(wire, service)
        node.restart()
        listed = replace(node.cxl(), pool_generation=2)
        pools = [node.dram(), listed]
        response = node.open(pools=pools)
        assert listed in response.adopted
        assert [e.location for e in service.catalog.locations(keys["c"])] == [locs["c"]]
        # the same process opening again finds the pools it listed
        assert node.open(pools=pools).adopted == response.adopted

    def test_cxl_after_a_leave_starts_empty(self, env):
        _, service, wire = env
        node, worker, keys, _ = self._node(wire, service)
        pin = worker.lookup(keys["c"]).items[0].pin_id
        node.close(CloseMode.LEAVE)
        drain(service)
        assert service.pins.get(pin) is not None  # kept while the node is away
        node.restart()
        response = node.open(pools=self._pools(node))
        assert response.status == Status.OK and response.adopted == []
        pool = service.catalog.pool(node.cxl().pool_id)
        assert pool.state == PoolState.ACTIVE and not pool.entries
        # A new run still needs evidence that the old reader ended.
        drain(service)
        assert service.pins.get(pin) is not None
        node.worker_exit(worker.ref())
        drain(service)
        assert service.pins.get(pin) is None
        assert not service.catalog.locations(keys["c"])

    def test_a_new_run_releases_only_its_own_dram_pins(self, env):
        clock, service, wire = env
        node, other = FakeNode(wire, "n1"), FakeNode(wire, "n2")
        node.open(pools=[node.dram(), node.cxl()])
        other.open(pools=[other.dram(), other.cxl(region_id=8)])
        c, d, o = key("c"), key("d"), key("o")
        node.commit((c, node.loc(node.cxl(), 1)), (d, node.loc(node.dram(), 1)))
        other.commit((o, other.loc(other.cxl(region_id=8), 1)))
        reopens, stays, expired = (FakeWorker(wire, node, tp_rank=i) for i in range(3))
        reader = FakeWorker(wire, other)
        for w in (reopens, stays, expired, reader):
            w.open()
        pins = {w: [i.pin_id for i in w.lookup(c, d).items] for w in (reopens, stays)}
        expired_pin = expired.lookup(c).items[0].pin_id
        other_pin = reader.lookup(o).items[0].pin_id
        clock.advance(20)
        for alive in (node, other, reopens, stays, reader):
            alive.heartbeat()
        clock.advance(15)  # "expired" expired: its pin stays (never by time)
        drain(service)
        assert service.pins.get(expired_pin) is not None
        # a renewal of the same run keeps them
        assert node.open(pools=[node.dram(), node.cxl()]).status == Status.OK
        drain(service)
        assert all(service.pins.get(p) for p in pins[stays] + [expired_pin])
        node.restart()
        response = node.open(pools=[node.dram(), node.cxl()])
        assert [p.pool_id for p in response.adopted] == [node.cxl().pool_id]
        assert reopens.open().status == Status.OK  # before maintain runs
        drain(service)
        for pin in pins[reopens] + [pins[stays][1]]:
            assert service.pins.get(pin) is None
        assert service.pins.get(pins[stays][0]) is not None
        assert service.pins.get(expired_pin) is not None
        assert service.pins.held_by(reopens.token) == []
        assert service.pins.get(other_pin) is not None  # another node's
        assert results(reopens.lookup(c)) == ["HIT"]

    def test_worker_must_reopen_to_map_the_new_dram(self, env):
        _, service, wire = env
        node = FakeNode(wire, "n1")
        node.open()
        worker = FakeWorker(wire, node)
        worker.open()
        node.restart()
        node.open()
        k = key("k")
        node.commit((k, node.loc(node.dram(), 1)))
        # still on the replaced run: lookups get EPOCH; other requests work
        assert worker.lookup(k).status == Status.EPOCH
        assert worker.probe(k).status == Status.EPOCH
        assert worker.heartbeat().status == Status.OK
        assert worker.unpin("x").status == Status.OK
        # a WORKER_OPEN with the old node_epoch is refused
        stale = FakeWorker(wire, node)
        assert stale.open(node_epoch="old").status == Status.EPOCH
        # after attaching again it reopens with the new node_epoch
        assert worker.open().status == Status.OK
        assert worker.lookup(k).items[0].result == "HIT"


class TestNodeClose:
    def test_leave_removes_every_location(self, env):
        _, service, wire = env
        node = FakeNode(wire, "n1")
        node.open(pools=[node.dram(), node.cxl()])
        k = key("k")
        cxl = node.loc(node.cxl(), 1)
        node.commit((k, node.loc(node.dram(), 1)), (key("c"), cxl))
        reader, idle = FakeWorker(wire, node), FakeWorker(wire, node, tp_rank=1)
        reader.open()
        idle.open()
        reader.lookup(key("c"), k)  # pins CXL and DRAM
        assert node.close(CloseMode.LEAVE).status == Status.OK
        assert node.heartbeat().status == Status.EPOCH
        # a worker without CXL pins goes with the node
        assert idle.heartbeat().status == Status.EPOCH
        # one still reading mapped CXL is kept for those pins only
        assert reader.heartbeat().status == Status.OK
        assert reader.lookup(k).status == Status.EPOCH
        assert ("worker", reader.incarnation) not in service.ledger
        drain(service)
        assert service.pins.held_by(reader.token)[0].location == cxl
        assert not service.catalog.locations(k)
        assert service.catalog.pool(node.cxl().pool_id) is not None
        # the region's pool goes once the reader is done
        reader.close()
        assert not service.catalog.locations(key("c"))
        assert service.catalog.pool(node.cxl().pool_id) is None

    def test_give_up_releases_every_pin_on_the_nodes_locations(self, env, caplog):
        import logging

        from maru_server.pin_manager import HolderKind

        log = logging.getLogger("maru_server.node_registry")  # does not propagate
        log.addHandler(caplog.handler)
        caplog.set_level(logging.INFO, logger=log.name)
        clock, service, wire = env
        node, other = FakeNode(wire, "n1"), FakeNode(wire, "n2")
        other_pool = other.cxl(region_id=8)
        node.capabilities = [MEMORY_CAPABILITY]
        node.open(pools=[node.dram(), node.cxl()])
        other.open(pools=[other.dram(), other_pool])
        c, d, f = key("c"), key("d"), key("f")
        cxl = node.loc(node.cxl(), 1)
        node.commit((c, cxl), (d, node.loc(node.dram(), 1)))
        node_cxl = node.loc(node.cxl(), 2)
        node.commit((f, node_cxl))
        o = key("o")
        other_cxl = other.loc(other_pool, 1)
        other.commit((o, other_cxl))
        worker = FakeWorker(wire, node)
        worker.open()
        cxl_pin, dram_pin = (i.pin_id for i in worker.lookup(c, d).items)
        reader = FakeWorker(wire, other)
        reader.open()
        other_pin = reader.lookup(o).items[0].pin_id
        with service.lock:  # n1's worker also reads n2's CXL: not n1's location
            foreign = service.pins.pin(o, other_cxl, worker.token, HolderKind.WORKER)
        try:
            for _ in range(32):  # n1 stays away; n2 and both workers stay alive
                clock.advance(20)
                for alive in (worker, reader, other):
                    alive.heartbeat()
                drain(service)
        finally:
            log.removeHandler(caplog.handler)
        assert service.registry.nodes["n1"].gone
        assert all(service.pins.get(p) is None for p in (cxl_pin, dram_pin))
        assert not service.catalog.locations(c) and not service.catalog.locations(f)
        assert service.catalog.pool(node.cxl().pool_id) is None
        assert "n1 given up: 2 pins on its locations released" in caplog.text
        # n2's location and every pin on it stay
        assert service.pins.get(other_pin) and service.pins.get(foreign)
        assert [e.location for e in service.catalog.locations(o)] == [other_cxl]
        # kept for its pin on n2's CXL only; UNPIN of a released pin is OK
        assert service.registry.known_worker(worker.token).orphan
        assert results(worker.unpin(cxl_pin)) == ["OK"]
        clock.advance(600)  # pins never go by time
        drain(service)
        assert service.pins.get(foreign) is not None
        # The next run still needs evidence that the foreign reader ended.
        node.restart()
        node.open()
        drain(service)
        assert service.pins.get(foreign) is not None
        node.worker_exit(worker.ref())
        drain(service)
        assert service.pins.get(foreign) is None
        other.open(pools=[other.dram(), other_pool])
        clock.advance(31)
        drain(service)
        assert service.stats()["orphan_sessions"] == 0

    def test_leave_still_keeps_cxl_pins_until_unpinned(self, env):
        clock, service, wire = env
        node = FakeNode(wire, "n1")
        node.open(pools=[node.dram(), node.cxl()])
        c, cxl = key("c"), node.loc(node.cxl(), 1)
        node.commit((c, cxl))
        worker = FakeWorker(wire, node)
        worker.open()
        pin = worker.lookup(c).items[0].pin_id
        node.close(CloseMode.LEAVE)
        drain(service)
        assert service.pins.get(pin) is not None
        assert service.catalog.pool(node.cxl().pool_id) is not None
        assert results(worker.unpin(pin)) == ["OK"]
        assert service.catalog.pool(node.cxl().pool_id) is None

    def test_an_orphan_keeps_pins_it_took_after_its_node_left(self, env):
        from maru_server.pin_manager import HolderKind

        clock, service, wire = env
        node, other = FakeNode(wire, "n1"), FakeNode(wire, "n2")
        node.open(pools=[node.dram(), node.cxl()])
        other.open()
        node.commit((key("c"), node.loc(node.cxl(), 1)))
        k, loc = key("k"), other.loc(other.dram(), 1)
        other.commit((k, loc))
        worker = FakeWorker(wire, node)
        worker.open()
        cxl_pin = worker.lookup(key("c")).items[0].pin_id
        node.close(CloseMode.LEAVE)
        drain(service)
        assert service.registry.known_worker(worker.token).orphan
        with service.lock:  # a pin taken after the node left, then no CXL pin
            late = service.pins.pin(k, loc, worker.token, HolderKind.WORKER)
            service.pins.unpin(cxl_pin)
        clock.advance(31)  # expired: pins never go by time
        other.heartbeat()
        drain(service)
        assert service.registry.known_worker(worker.token) is not None
        assert service.pins.get(late) is not None
        with service.lock:
            service.pins.unpin(late)
        clock.advance(31)  # pinless now: a later check purges it
        other.heartbeat()
        drain(service)
        assert service.registry.known_worker(worker.token) is None

    def test_an_orphan_without_cxl_pins_goes_on_expiry(self, env):
        clock, service, wire = env
        node = FakeNode(wire, "n1")
        node.open(pools=[node.dram(), node.cxl()])
        node.commit((key("c"), node.loc(node.cxl(), 1)))
        worker = FakeWorker(wire, node)
        worker.open()
        pin = worker.lookup(key("c")).items[0].pin_id
        node.close(CloseMode.LEAVE)
        worker.unpin(pin)  # done reading: nothing holds it any more
        clock.advance(31)
        drain(service)
        assert service.registry.known_worker(worker.token) is None
        assert service.stats()["orphan_sessions"] == 0

    def test_give_up_after_node_give_up_s(self, env):
        clock, service, wire = env
        node = FakeNode(wire, "n1")
        node.open(pools=[node.dram(), node.cxl()])
        k = key("k")
        node.commit((k, node.loc(node.cxl(), 1)))
        clock.advance(31)  # expired at +30: offline, locations kept
        drain(service)
        assert service.catalog.locations(k)
        clock.advance(598)  # 1 s short of node_give_up_s after expiry
        drain(service)
        assert service.catalog.locations(k)
        clock.advance(2)
        drain(service)
        assert not service.catalog.locations(k)
        # the process lost everything: it opens as a new run
        assert node.open().status == Status.REJECTED
        node.restart()
        assert node.open().epoch_seq == 2

    def test_give_up_drops_the_nodes_workers(self, env):
        clock, service, wire = env
        node = FakeNode(wire, "n1")
        node.open()
        k = key("k")
        node.commit((k, node.loc(node.dram(), 1)))
        worker = FakeWorker(wire, node)
        worker.open()
        worker.lookup(k)
        clock.advance(631)
        drain(service)
        assert service.registry.known_worker(worker.token) is None
        assert ("worker", worker.incarnation) not in service.ledger
        assert len(service.pins) == 0
        assert not service.catalog.locations(k)

    def test_cxl_goes_back_only_once_unpinned_even_when_forced(self, env):
        _, service, wire = env
        a, b = FakeNode(wire, "a"), FakeNode(wire, "b")
        a.open(pools=[a.dram(), a.cxl()])
        b.open()
        k = key("k")
        loc = a.loc(a.cxl(), 1)
        a.commit((k, loc))
        worker = FakeWorker(wire, a)
        worker.open()
        pin = worker.lookup(k).items[0].pin_id
        a.close(CloseMode.LEAVE)
        drain(service)
        assert service.catalog.pool(a.cxl().pool_id) is not None
        assert worker.unpin(pin).items[0].result == "OK"
        assert service.catalog.pool(a.cxl().pool_id) is None
        assert not service.catalog.locations(k)

    def test_give_up_also_after_a_restart_close(self, env):
        clock, service, wire = env
        node = FakeNode(wire, "n1")
        node.open(pools=[node.dram(), node.cxl()])
        k = key("k")
        node.commit((k, node.loc(node.cxl(), 1)))
        node.close(CloseMode.RESTART)
        clock.advance(601)
        drain(service)
        assert not service.catalog.locations(k)

    def test_reopen_before_give_up_keeps_locations(self, env):
        clock, service, wire = env
        node = FakeNode(wire, "n1")
        node.open(pools=[node.dram(), node.cxl()])
        k = key("k")
        node.commit((k, node.loc(node.cxl(), 1)))
        clock.advance(500)
        node.restart()
        assert node.open(pools=[node.dram(), node.cxl()]).status == Status.OK
        clock.advance(500)
        drain(service)
        assert service.catalog.locations(k)


class TestWorkerSessions:
    def test_expired_session_keeps_pins_worker_exit_releases(self, env):
        clock, service, wire = env
        node = FakeNode(wire, "n1")
        node.open()
        k = key("k")
        loc = node.loc(node.dram(), 1)
        node.commit((k, loc))
        worker = FakeWorker(wire, node)
        worker.open()
        pin = worker.lookup(k).items[0].pin_id
        clock.advance(20)
        node.heartbeat()
        clock.advance(15)
        node.heartbeat()
        drain(service)
        # expired: requests get EPOCH, the pin stays
        assert worker.heartbeat().status == Status.EPOCH
        assert worker.lookup(k).status == Status.EPOCH
        assert service.pins.get(pin) is not None
        assert node.retire((k, loc)).items[0].cause == KeptCause.PINNED
        # the node saw the worker end
        assert results(node.worker_exit(worker.ref())) == ["OK"]
        assert service.pins.get(pin) is None
        assert results(node.retire((k, loc))) == ["RETIRED"]
        # a report for a worker MaruServer no longer knows is OK
        assert results(node.worker_exit(worker.ref())) == ["OK"]

    def test_reopen_with_same_incarnation_moves_pins(self, env):
        _, service, wire = env
        node = FakeNode(wire, "n1")
        node.open()
        k = key("k")
        loc = node.loc(node.dram(), 1)
        node.commit((k, loc))
        worker = FakeWorker(wire, node)
        worker.open()
        pin = worker.lookup(k).items[0].pin_id
        old = worker.token
        assert worker.open().status == Status.OK
        assert service.pins.get(pin).holder == worker.token
        worker.token, new = old, worker.token
        assert worker.heartbeat().status == Status.EPOCH
        worker.token = new
        assert results(worker.unpin(pin)) == ["OK"]
        assert results(node.retire((k, loc))) == ["RETIRED"]

    def test_reopen_with_another_identity_is_rejected(self, env):
        _, _, wire = env
        node = FakeNode(wire, "n1")
        node.open()
        worker = FakeWorker(wire, node)
        worker.open()
        assert worker.open(tp_rank=5).status == Status.REJECTED

    def test_close_releases_pins(self, env):
        _, service, wire = env
        node = FakeNode(wire, "n1")
        node.open()
        k = key("k")
        node.commit((k, node.loc(node.dram(), 1)))
        worker = FakeWorker(wire, node)
        worker.open()
        worker.lookup(k)
        assert len(service.pins) == 1
        assert worker.close().status == Status.OK
        assert len(service.pins) == 0
        assert worker.heartbeat().status == Status.EPOCH

    def test_many_pins_are_released_in_bounded_steps(self, env):
        _, service, wire = env
        node = FakeNode(wire, "n1")
        node.open(pools=[node.dram(pages=3000)])
        keys = [key(f"k{i}") for i in range(2500)]
        for start in range(0, 2500, 1000):
            node.commit(
                *[(k, node.loc(node.dram(), i)) for i, k in enumerate(keys)][
                    start : start + 1000
                ]
            )
        worker = FakeWorker(wire, node)
        worker.open()
        for start in range(0, 2500, 1000):
            worker.lookup(*keys[start : start + 1000])
        assert len(service.pins) == 2500
        worker.close()
        assert len(service.pins) == 2500 - 1024  # the rest is queued
        drain(service)
        assert len(service.pins) == 0

    def test_worker_exit_checks_the_node_and_pid(self, env):
        _, service, wire = env
        n1, n2 = FakeNode(wire, "n1"), FakeNode(wire, "n2")
        n1.open()
        n2.open()
        worker = FakeWorker(wire, n1)
        worker.open()
        assert results(n2.worker_exit(worker.ref())) == ["REJECTED"]
        other_pid = replace(worker.ref(), pid_start=1)
        assert results(n1.worker_exit(other_pid)) == ["REJECTED"]
        assert worker.heartbeat().status == Status.OK
        assert results(n1.worker_exit(worker.ref())) == ["OK"]
        assert worker.heartbeat().status == Status.EPOCH

    def test_worker_open_rules(self, env):
        _, _, wire = env
        node = FakeNode(wire, "n1")
        worker = FakeWorker(wire, node)
        # its node has no session yet
        assert worker.open().status == Status.EPOCH
        node.open()
        assert worker.open(protocol="ms0").status == Status.REJECTED
        assert worker.open().status == Status.OK
        # a scheduler that does not attach: no node check
        probe = FakeWorker(wire, node, role=SessionRole.PROBE)
        assert probe.open(node_epoch="unknown").status == Status.OK

    def test_probe_and_worker_of_one_process_coexist(self, env):
        _, _, wire = env
        node = FakeNode(wire, "n1")
        node.open()
        worker = FakeWorker(wire, node)
        worker.open()
        probe = FakeWorker(wire, node, role=SessionRole.PROBE)
        probe.incarnation = worker.incarnation
        probe.open()
        assert worker.heartbeat().status == Status.OK
        assert probe.heartbeat().status == Status.OK

    def test_expired_probe_sessions_are_dropped(self, env):
        clock, service, wire = env
        node = FakeNode(wire, "n1")
        node.open()
        probe = FakeWorker(wire, node, role=SessionRole.PROBE)
        probe.open()
        clock.advance(31)
        drain(service)
        assert service.registry.known_worker(probe.token) is None

    def test_expired_worker_without_pins_is_purged(self, env):
        clock, service, wire = env
        node = FakeNode(wire, "n1")
        node.open()
        worker = FakeWorker(wire, node)
        worker.open()
        assert results(worker.lookup(key("absent"))) == ["MISS"]  # an op record
        origin = ("worker", worker.incarnation)
        assert origin in service.ledger
        clock.advance(31)  # past the session TTL, no pins held
        drain(service)
        registry = service.registry
        assert registry.known_worker(worker.token) is None
        assert origin not in service.ledger
        assert (worker.incarnation, SessionRole.WORKER) not in registry._incarnations
        assert service.stats()["worker_sessions"] == 0

    def test_expired_worker_with_pins_stays(self, env):
        clock, service, wire = env
        node = FakeNode(wire, "n1")
        node.open()
        k = key("k")
        node.commit((k, node.loc(node.dram(), 1)))
        node.heartbeat()
        worker = FakeWorker(wire, node)
        worker.open()
        assert results(worker.lookup(k)) == ["HIT"]
        for _ in range(3):  # pins never go by time
            clock.advance(31)
            node.heartbeat()
            drain(service)
            assert service.registry.known_worker(worker.token) is not None
            assert service.pins.held_by(worker.token)
        assert ("worker", worker.incarnation) in service.ledger

    def test_reopen_after_the_purge_is_a_fresh_open(self, env):
        clock, service, wire = env
        node = FakeNode(wire, "n1")
        node.open()
        k = key("k")
        node.commit((k, node.loc(node.dram(), 1)))
        worker = FakeWorker(wire, node)
        worker.open()
        old = worker.token
        assert results(worker.lookup(key("absent"))) == ["MISS"]
        clock.advance(31)
        node.heartbeat()
        drain(service)
        assert service.registry.known_worker(old) is None
        # the same incarnation opens again: a new session, a new op ledger
        assert worker.open().status == Status.OK and worker.token != old
        hit = worker.lookup(k)
        assert results(hit) == ["HIT"]
        assert service.pins.held_by(worker.token)
        assert results(worker.unpin(hit.items[0].pin_id)) == ["OK"]


class TestEpochChecks:
    def test_stale_server_epoch_and_sessions_get_epoch(self, env):
        _, service, wire = env
        node = FakeNode(wire, "n1")
        node.open()
        worker = FakeWorker(wire, node)
        worker.open()
        node.server_epoch = worker.server_epoch = "another-run"
        assert node.heartbeat().status == Status.EPOCH
        assert worker.heartbeat().status == Status.EPOCH
        assert worker.lookup(key("k")).status == Status.EPOCH
        assert node.commit((key("k"), node.loc(node.dram(), 1))).status == (
            Status.EPOCH
        )
        assert worker.unpin("x").status == Status.EPOCH
        node.server_epoch = worker.server_epoch = None
        assert node.heartbeat().status == Status.OK
        node.token = "unknown"
        assert node.heartbeat().status == Status.EPOCH

    def test_session_of_the_wrong_kind_is_rejected(self, env):
        _, _, wire = env
        node = FakeNode(wire, "n1")
        node.open()
        worker = FakeWorker(wire, node)
        worker.open()
        node_token = node.token
        node.token = worker.token
        assert node.heartbeat().status == Status.REJECTED
        worker.token = node_token
        assert worker.heartbeat().status == Status.REJECTED

    def test_batch_limits(self, env):
        _, _, wire = env
        node = FakeNode(wire, "n1")
        node.open()
        worker = FakeWorker(wire, node)
        worker.open()
        keys = [key(f"k{i}") for i in range(1025)]
        assert worker.lookup(*keys).status == Status.REJECTED
        assert worker.probe(*keys).status == Status.REJECTED
        assert worker.unpin(*[str(i) for i in range(1025)]).status == Status.REJECTED
        usage = [PoolUsage(node.dram_id, 1, 1, 0, 0, 0, 0, 0, 0)] * 1025
        assert node.heartbeat(usage=usage).status == Status.REJECTED
        assert admin_delete(wire, *keys).status == Status.REJECTED
        assert worker.lookup(*keys[:1024]).status == Status.OK


def heartbeat_reads(node) -> list[tuple[str, int, int]]:
    return list(node.heartbeat().access.reads())


def read_of(location) -> tuple[str, int, int]:
    return (location.pool_id, location.offset, location.generation)


class TestHeartbeatQueues:
    @staticmethod
    def _reader(wire, count: int):
        """Node a with keys k0.. in DRAM, and a worker of it."""
        a = FakeNode(wire, "a")
        a.open()
        items = [(key(f"k{i}"), a.loc(a.dram(), i)) for i in range(count)]
        a.commit(*items)
        worker = FakeWorker(wire, a)
        worker.open()
        return a, worker, items

    def test_access_info_goes_to_the_owning_node_once(self, env):
        _, _, wire = env
        a, worker, [(k, loc)] = self._reader(wire, 1)
        b = FakeNode(wire, "b")
        b.open()
        for _ in range(3):
            worker.lookup(k)
        worker.probe(k)  # a hint, not a read
        assert heartbeat_reads(b) == []
        assert heartbeat_reads(a) == [read_of(loc)]
        assert heartbeat_reads(a) == []

    def test_a_batch_goes_least_recently_read_first(self, env):
        _, _, wire = env
        a, worker, items = self._reader(wire, 3)
        for i in (0, 1, 2, 0):
            worker.lookup(items[i][0])
        assert heartbeat_reads(a) == [read_of(items[i][1]) for i in (1, 2, 0)]

    def test_a_full_map_drops_the_first_queued(self, env, monkeypatch):
        monkeypatch.setattr("maru_server.node_registry.ACCESS_MAX_PER_NODE", 3)
        _, _, wire = env
        a, worker, items = self._reader(wire, 4)
        for i in (0, 1, 2, 0, 3):
            worker.lookup(items[i][0])
        # 0 was queued first when 3 came: a re-read keeps its place
        assert heartbeat_reads(a) == [read_of(items[i][1]) for i in (1, 2, 3)]

    def test_a_heartbeat_takes_up_to_its_limit_first_queued_first(
        self, env, monkeypatch
    ):
        monkeypatch.setattr("maru_server.node_registry.ACCESS_PER_HEARTBEAT", 2)
        _, _, wire = env
        a, worker, items = self._reader(wire, 5)
        worker.lookup(*[k for k, _ in items])
        want = [read_of(loc) for _, loc in items]
        got = [heartbeat_reads(a) for _ in range(4)]
        assert got == [want[:2], want[2:4], want[4:], []]

    def test_rereads_never_hold_back_pages_queued_after_them(self, env, monkeypatch):
        monkeypatch.setattr("maru_server.node_registry.ACCESS_PER_HEARTBEAT", 4)
        _, _, wire = env
        a, worker, items = self._reader(wire, 5)
        reads = [read_of(loc) for _, loc in items]
        sent = []
        for _ in range(4):
            for k, _ in items:  # the same order between any two heartbeats
                worker.lookup(k)
            sent.append(heartbeat_reads(a))
        # every page within two heartbeats, each batch least recently read first
        assert sent == [
            reads[:4],
            [reads[i] for i in (0, 1, 2, 4)],
            reads[:4],
            [reads[i] for i in (0, 1, 2, 4)],
        ]

    def test_block_requests_until_the_node_retired_them(self, env):
        _, service, wire = env
        a, b = FakeNode(wire, "a"), FakeNode(wire, "b")
        a.open()
        b.open()
        k, j = key("k"), key("j")
        ka, kb, ja = a.loc(a.dram(), 1), b.loc(b.dram(), 1), a.loc(a.dram(), 2)
        a.commit((k, ka), (j, ja))
        b.commit((k, kb))
        worker = FakeWorker(wire, a)
        worker.open()
        pin = worker.lookup(j).items[0].pin_id
        response = admin_delete(wire, k, j, key("none"))
        assert results(response) == ["OK", "OK", "MISS"]
        assert response.items[2].reason == MissReason.ABSENT
        # deleting again queues nothing new
        assert results(admin_delete(wire, k)) == ["OK"]
        (block,) = a.heartbeat().blocks
        assert {(i.key, i.location) for i in block.items} == {(k, ka), (j, ja)}
        # MaruServer removes nothing itself; the block comes until it is done
        assert a.heartbeat().blocks == [block]
        assert len(service.catalog.locations(k)) == 2
        # the node retires them with ordinary RETIREs; pins still hold
        response = a.retire((k, ka), (j, ja))
        assert results(response) == ["RETIRED", "KEPT"]
        assert response.items[1].cause == KeptCause.PINNED
        (block,) = a.heartbeat().blocks
        assert [(i.key, i.location) for i in block.items] == [(j, ja)]
        worker.unpin(pin)
        assert results(a.retire((j, ja))) == ["RETIRED"]
        assert a.heartbeat().blocks == []
        (block_b,) = b.heartbeat().blocks
        assert results(b.retire((k, kb))) == ["RETIRED"]
        assert b.heartbeat().blocks == []
        assert service.registry.blocks_waiting() == 0

    def test_a_preserved_key_is_not_queued(self, tmp_path):
        service, wire, preserve = policy_service(tmp_path)
        a = FakeNode(wire, "a")
        a.open()
        k, j = key("k"), key("j")
        a.commit((k, a.loc(a.dram(), 1)), (j, a.loc(a.dram(), 2)))
        preserve(k)
        response = admin_delete(wire, k, j)
        assert results(response) == ["KEPT", "OK"]
        assert response.items[0].cause == KeptCause.PINNED
        (block,) = a.heartbeat().blocks
        assert [i.key for i in block.items] == [j]
        preserve()
        assert results(admin_delete(wire, k)) == ["OK"]

    def test_every_block_is_delivered_past_stuck_ones(self, env):
        _, service, wire = env
        a = FakeNode(wire, "a")
        a.open(pools=[a.dram(pages=3000)])
        keys = [key(f"k{i}") for i in range(2000)]
        items = [(k, a.loc(a.dram(pages=3000), i)) for i, k in enumerate(keys)]
        a.commit(*items[:1024])
        a.commit(*items[1024:])
        # a reader pins one key in each of the first blocks: they never empty
        reader = FakeWorker(wire, a)
        reader.open()
        reader.lookup(*[keys[i] for i in (0, 256, 512, 768)])
        admin_delete(wire, *keys[:1024])
        admin_delete(wire, *keys[1024:])
        delivered = set()
        for _ in range(10):
            blocks = a.heartbeat().blocks
            sent = [(i.key, i.location) for b in blocks for i in b.items]
            delivered.update(k for k, _ in sent)
            if sent:
                a.retire(*sent)  # the node retires what it was asked to
        assert delivered == set(keys)
        assert service.registry.blocks_waiting() == 4  # the stuck ones

    def test_blocks_follow_adopted_pools_into_a_new_run(self, env):
        clock, service, wire = env
        a = FakeNode(wire, "a")
        a.open(pools=[a.dram(), a.cxl()])
        c, d = key("c"), key("d")
        cxl_loc = a.loc(a.cxl(), 1)
        a.commit((c, cxl_loc), (d, a.loc(a.dram(), 1)))
        admin_delete(wire, c, d)
        clock.advance(31)  # a renewal continues the run: its blocks stay
        assert a.open(pools=[a.dram(), a.cxl()]).status == Status.OK
        assert a.heartbeat().blocks
        a.restart()  # a new run keeps only the deletes of adopted pools
        a.open(pools=[a.dram(), a.cxl()])
        (block,) = a.heartbeat().blocks
        assert [(i.key, i.location) for i in block.items] == [(c, cxl_loc)]

    def test_delete_on_a_node_closed_for_restart_is_offline(self, env):
        _, service, wire = env
        a = FakeNode(wire, "a")
        a.open(pools=[a.dram(), a.cxl()])
        k, c = key("k"), key("c")
        a.commit((k, a.loc(a.dram(), 1)), (c, a.loc(a.cxl(), 1)))
        a.close(CloseMode.RESTART)
        # hidden DRAM, and a CXL location no running node could block
        response = admin_delete(wire, k, c)
        assert [(i.result, i.reason) for i in response.items] == [
            ("MISS", MissReason.OFFLINE)
        ] * 2
        assert service.registry.blocks_waiting() == 0

    def test_usage_is_kept_for_own_pools(self, env):
        _, service, wire = env
        a = FakeNode(wire, "a")
        a.open()
        usage = PoolUsage(a.dram_id, 100, 50, 10, 0, 10, 30, 0, 0)
        foreign = replace(usage, pool_id="dram:b:x")
        assert a.heartbeat(usage=[usage, foreign]).status == Status.OK
        assert service.registry.nodes["a"].usage == {a.dram_id: usage}


class TestMaintenanceThread:
    def test_thread_gives_up_a_node_and_sweeps(self):
        service = make_service(
            clock=time.monotonic,
            session_ttl_s=0.2,
            node_give_up_s=0.3,
            maintenance_interval_s=0.02,
        )
        wire = Wire(service)
        node = FakeNode(wire, "n1")
        node.open(pools=[node.dram(), node.cxl()])
        k = key("k")
        node.commit((k, node.loc(node.cxl(), 1)))
        service.ensure_started()
        service.ensure_started()  # once
        deadline = time.monotonic() + 5
        while service.catalog.locations(k) and time.monotonic() < deadline:
            time.sleep(0.02)
        service.stop()
        assert not service.catalog.locations(k)

    def test_budget_bounds_each_step(self):
        clock = FakeClock()
        service = make_service(clock, maintenance_budget=10)
        wire = Wire(service)
        node = FakeNode(wire, "n1")
        node.open(pools=[node.dram(), node.cxl()])
        node.commit(*[(key(f"k{i}"), node.loc(node.cxl(), i)) for i in range(35)])
        node.close(CloseMode.LEAVE)
        steps = 0
        while service.maintain() >= 10:
            steps += 1
            assert len(service.catalog.locations(key("k0"))) <= 1
        assert steps == 3
        assert all(not service.catalog.locations(key(f"k{i}")) for i in range(35))

    def test_a_large_dropped_node_is_cleaned_in_bounded_steps(self):
        import weakref
        from collections import OrderedDict
        from fractions import Fraction

        from maru_common.protocol import WorkerOpenRequest
        from maru_common.storage_types import MS_PROTOCOL, KeyLocation
        from maru_server import node_registry as nr
        from maru_server.node_registry import (
            ACCESS_MAX_PER_NODE,
            BLOCK_ITEMS,
            _Block,
            _Listing,
            worker_origin,
        )
        from maru_server.pin_manager import HolderKind

        clock = FakeClock()
        budget = 256
        service = make_service(clock, maintenance_budget=budget)
        registry = service.registry
        node = FakeNode(Wire(service), "big")
        node.capabilities = [MEMORY_CAPABILITY]
        node.open(pools=[node.dram(), node.cxl()])
        n = 25_000
        target = (key("k"), node.loc(node.dram(), 1))
        records = weakref.WeakSet()  # op records not freed yet
        with service.lock:
            for i in range(n):
                token = registry.worker_open(
                    WorkerOpenRequest(
                        node_id="big",
                        node_epoch=node.epoch,
                        engine_id="e",
                        dp_index=0,
                        tp_rank=0,
                        pid=1000 + i,
                        pid_start=1,
                        incarnation=f"w{i}",
                        namespace="ns",
                        role=SessionRole.WORKER,
                        protocol=MS_PROTOCOL,
                        capabilities=[],
                    ),
                    clock(),
                ).worker_session
                service.pins.pin(
                    *target, token, HolderKind.WORKER, keep_on_departure=False
                )
                origin = worker_origin(f"w{i}")
                _, record = service.ledger.begin(
                    origin, OpRef(f"w{i}", 1, b"d", 0), "big"
                )
                service.ledger.finish(origin, record, None, 1)
                records.add(record)
                if i == 0:  # it still reads mapped CXL: an orphan
                    orphan = token
                    cxl = node.loc(node.cxl(), 1)
                    service.pins.pin(key("c"), cxl, token, HolderKind.WORKER)
            # a full access map, queued block requests and a pool listing
            listed = 300_000
            big = registry.nodes["big"]
            big.listings["1"] = _Listing(
                node.dram_id, [object() for _ in range(listed)]
            )
            registry._access["big"] = OrderedDict.fromkeys(
                ("p", i, i) for i in range(ACCESS_MAX_PER_NODE)
            )
            blocks = 200
            for b in range(blocks):  # full ones
                items = tuple(
                    KeyLocation(key(f"b{b}-{j}"), node.loc(node.dram(), j))
                    for j in range(BLOCK_ITEMS)
                )
                big.blocks[f"b{b}"] = _Block(f"b{b}", items, "delete")
                big.blocked.update((i.key, i.location) for i in items)
        del record  # the ledger holds the records alone
        # held here, so one dropped whole instead of freed in steps stays full
        listing = big.listings["1"].entries
        access = registry._access["big"]
        blocks_left, blocked_left = big.blocks, big.blocked

        def left():
            """(count, freed per unit of work) of what the node leaves."""
            return (
                (len(registry._workers), 1),
                (len(service.pins), 1),
                (len(records), 1),
                (len(listing), nr.LISTING_FREES_PER_UNIT),
                (len(access), nr.ACCESS_FREES_PER_UNIT),
                (len(blocked_left), nr.BLOCKED_FREES_PER_UNIT),
                (len(blocks_left), nr.BLOCK_FREES_PER_UNIT),
            )

        clock.advance(631)  # past node_give_up_s
        works = []
        while not works or works[-1] >= budget:
            before = left()
            works.append(service.maintain())
            after = left()
            # what a step frees, in units, is within the work it reports
            freed = sum(
                Fraction(b - a, per)
                for (b, per), (a, _) in zip(before, after, strict=True)
            )
            assert freed <= works[-1], (len(works), works[-1], before, after)
        assert max(works) <= budget

        # one unit per node given up, due session check, worker session
        # dropped, pin and op record, and per *_FREES_PER_UNIT entries freed
        def units(size: int, per_unit: int) -> int:
            return -(-size // per_unit)

        frees = (
            units(ACCESS_MAX_PER_NODE, nr.ACCESS_FREES_PER_UNIT)
            + units(listed, nr.LISTING_FREES_PER_UNIT)
            + units(blocks * BLOCK_ITEMS, nr.BLOCKED_FREES_PER_UNIT)
            + units(blocks, nr.BLOCK_FREES_PER_UNIT)
            + blocks * units(BLOCK_ITEMS, nr.BLOCK_ITEM_FREES_PER_UNIT)
        )
        # and the CXL pin: every pin on a given-up node's locations goes
        assert sum(works) == 1 + 4 * n + 1 + frees
        assert registry.nodes["big"].gone and "big" not in registry._access
        assert not registry._freeing and not big.blocks and not big.blocked
        assert not big.listings
        # the CXL reader's pin went with the node: no orphan is left
        assert registry.known_worker(orphan) is None and not service.pins.held_by(
            orphan
        )
        assert registry.worker_sessions() == [] and cxl.pool_id == node.cxl().pool_id
        assert len(service.pins) == 0 and ("worker", "w0") not in service.ledger
        assert not any(count for count, _ in left())

    def test_large_forced_sweep_allows_lookup_between_batches(self, monkeypatch):
        from concurrent.futures import ThreadPoolExecutor
        from queue import Queue

        from maru_server.ms_service import MAINTENANCE_YIELD_S

        assert MAINTENANCE_YIELD_S > 0
        service = make_service(clock=time.monotonic, maintenance_interval_s=0.01)
        wire = Wire(service)
        reader = FakeNode(wire, "reader")
        reader.open()
        keys = [key(f"r{i}") for i in range(32)]
        reader.commit(*[(k, reader.loc(reader.dram(), i)) for i, k in enumerate(keys)])
        worker = FakeWorker(wire, reader)
        worker.open()
        big = FakeNode(wire, "big")
        size = 300_000
        big.open(pools=[big.dram(pages=size)])
        pool = service.catalog.pool(big.dram_id)
        for i in range(size):  # straight into the Catalog: fast to set up
            loc = big.loc(big.dram(pages=size), i)
            service.catalog._add(Entry(key(f"b{i}"), loc, PAGE, pool))

        def lookup():
            response = worker.lookup(*keys)
            assert all(item.result.value == "HIT" for item in response.items)
            worker.unpin(*[item.pin_id for item in response.items])

        # Pause at real maintenance yields so scheduler load and coverage
        # cannot decide whether a request gets to run between the batches.
        # The wall-clock p99 check lives in benchmarks/test_ms_maintenance.py.
        batches = Queue()
        resume = Queue()
        controlled = True
        wait = service._stop.wait

        def checkpoint(timeout):
            if controlled and timeout == MAINTENANCE_YIELD_S:
                batches.put(len(pool.entries))
                resume.get(timeout=10)
            return wait(timeout)

        monkeypatch.setattr(service._stop, "wait", checkpoint)
        big.close(CloseMode.LEAVE)
        with ThreadPoolExecutor(max_workers=1) as requests:
            service.ensure_started()
            try:
                previous = size
                for step in range(3):
                    remaining = batches.get(timeout=10)
                    assert 0 < previous - remaining <= service.maintenance_budget
                    # A lock held across batches would block this real lookup
                    # until resume, and fail instead of passing on a fast CPU.
                    requests.submit(lookup).result(timeout=10)
                    assert len(pool.entries) == remaining
                    previous = remaining
                    if step == 2:
                        controlled = False
                    resume.put(None)
                deadline = time.monotonic() + 30
                while pool.entries and time.monotonic() < deadline:
                    time.sleep(0.01)
            finally:
                controlled = False
                resume.put(None)
                service.stop()
        assert not pool.entries


def test_pin_manager_keeps_departure_pins_apart():
    from maru_common.storage_types import Location, PoolKind
    from maru_server.pin_manager import HolderKind, PinManager

    pins = PinManager()

    def loc(kind: PoolKind, page: int) -> Location:
        return Location(f"{kind.value}:p", f"a{page}", 1, page * PAGE, PAGE, kind)

    for page in range(3):
        pins.pin(
            key(f"d{page}"),
            loc(PoolKind.DRAM, page),
            "w",
            HolderKind.WORKER,
            keep_on_departure=False,
        )
    cxl = pins.pin(key("c"), loc(PoolKind.CXL, 0), "w", HolderKind.WORKER)
    assert pins.holds_retained("w") and pins.holds("w", retained=False)
    assert pins.move_holder("w", "v") == 4 and not pins.holds("w")
    # keeping CXL pins releases only the others, at most ``limit`` a call
    assert pins.release_holder("v", 2, keep_retained=True) == 2
    assert pins.release_holder("v", 5, keep_retained=True) == 1
    assert not pins.holds("v", retained=False) and pins.holds_retained("v")
    assert [p.pin_id for p in pins.held_by("v")] == [cxl]
    assert pins.unpin(cxl) is not None and not pins.holds("v")
    assert len(pins) == 0
