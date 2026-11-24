# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Catalog contracts for worker RDMA reads and orphan lifetimes."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from maru_common.protocol import (
    LookupAndPinRequest,
    MessageType,
    NodeOrphansRequest,
    WorkerExitRequest,
)
from maru_common.storage_types import (
    MEMORY_CAPABILITY,
    REMOTE_CAPABILITY,
    WORKER_RDMA_CAPABILITY,
    CloseMode,
    ExitCause,
    ItemResult,
    SessionRole,
    Status,
)
from maru_server.pin_manager import HolderKind
from tests.unit.ms_fakes import (
    PAGE,
    FakeClock,
    FakeNode,
    FakeWorker,
    Wire,
    key,
    make_service,
)


def drain(service):
    while service.maintain() >= service.maintenance_budget:
        pass


@pytest.fixture
def env():
    clock = FakeClock()
    service = make_service(clock)
    wire = Wire(service)
    gpu, memory = FakeNode(wire, "gpu"), FakeNode(wire, "memory")
    gpu.capabilities = [WORKER_RDMA_CAPABILITY, REMOTE_CAPABILITY]
    memory.capabilities = [MEMORY_CAPABILITY]
    assert gpu.open(pools=[gpu.dram(), gpu.cxl()], pid_ns=101).status == Status.OK
    assert (
        memory.open(pools=[memory.dram(), memory.cxl(region_id=8)]).status == Status.OK
    )
    assert gpu.heartbeat(memory_peer=(memory.node_id, memory.epoch)).status == Status.OK
    worker = FakeWorker(wire, gpu)
    assert worker.open().status == Status.OK
    targets = {}
    for name, node, pool in (
        ("local_dram", gpu, gpu.dram()),
        ("local_cxl", gpu, gpu.cxl()),
        ("remote_dram", memory, memory.dram()),
        ("remote_cxl", memory, memory.cxl(region_id=8)),
    ):
        location = node.loc(pool, 1)
        assert node.commit((key(name), location)).items[0].result == ItemResult.ADDED
        targets[name] = location
    return SimpleNamespace(
        clock=clock,
        service=service,
        wire=wire,
        gpu=gpu,
        memory=memory,
        worker=worker,
        targets=targets,
    )


def lookup_all(env):
    reply = env.worker.lookup(*(key(name) for name in env.targets))
    assert all(i.result == ItemResult.HIT for i in reply.items)
    return dict(zip(env.targets, reply.items, strict=True))


def reopen(env, **kwargs):
    env.gpu.restart()
    return env.gpu.open(pools=[env.gpu.dram(), env.gpu.cxl()], pid_ns=101, **kwargs)


def orphans(env, cursor=None, **kwargs):
    return env.wire.call(
        MessageType.NODE_ORPHANS,
        NodeOrphansRequest(**(env.gpu.envelope() | kwargs), cursor=cursor),
    )


def assert_departure_pins(env, hits):
    drain(env.service)
    assert env.service.pins.get(hits["local_dram"].pin_id) is None
    for name in ("local_cxl", "remote_dram", "remote_cxl"):
        pin = env.service.pins.get(hits[name].pin_id)
        assert pin is not None and pin.holder_kind == HolderKind.WORKER
    for name in ("remote_dram", "remote_cxl"):
        assert (
            env.memory.retire((key(name), env.targets[name])).items[0].result
            == ItemResult.KEPT
        )
    assert env.service.registry.known_worker(env.worker.token).orphan


def test_new_run_keeps_orphan_pins_while_a_read_is_in_flight(env):
    hits = lookup_all(env)
    response = reopen(env)
    assert response.capabilities == env.gpu.capabilities
    assert [o.incarnation for o in response.orphans] == [env.worker.incarnation]
    assert_departure_pins(env, hits)


def test_give_up_keeps_memory_node_pins(env):
    hits = lookup_all(env)
    for _ in range(32):
        env.clock.advance(20)
        env.memory.heartbeat()
        env.worker.heartbeat()
        drain(env.service)
    assert env.service.registry.nodes["gpu"].gone
    assert_departure_pins(env, hits)
    response = reopen(env)
    assert [o.incarnation for o in response.orphans] == [env.worker.incarnation]
    assert_departure_pins(env, hits)


def test_leave_keeps_memory_node_pins(env):
    hits = lookup_all(env)
    env.gpu.close(CloseMode.LEAVE)
    assert_departure_pins(env, hits)
    assert reopen(env).orphans[0].incarnation == env.worker.incarnation
    assert_departure_pins(env, hits)


def test_pins_never_expire(env):
    hit = env.worker.lookup(key("remote_dram")).items[0]
    reopen(env)
    for _ in range(100):
        env.clock.advance(25)
        env.memory.heartbeat()
        env.gpu.heartbeat()
        drain(env.service)
    assert env.service.pins.get(hit.pin_id) is not None
    assert env.service.stats()["orphan_oldest_s"] == 2500


def test_same_run_refresh_moves_pins(env):
    hits = lookup_all(env)
    old = env.worker.token
    env.clock.advance(31)
    env.gpu.open(pools=[env.gpu.dram(), env.gpu.cxl()], pid_ns=101)
    env.memory.open(pools=[env.memory.dram(), env.memory.cxl(region_id=8)])
    assert env.worker.open().status == Status.OK
    assert not env.service.pins.holds(old)
    assert all(
        env.service.pins.get(h.pin_id).holder == env.worker.token for h in hits.values()
    )
    assert env.service.stats()["orphan_sessions"] == 0


def test_orphan_of_a_killed_worker_is_released_once_its_exit_is_seen(env):
    hits = lookup_all(env)
    old_exit = env.gpu.build(
        WorkerExitRequest,
        **env.gpu.envelope(),
        workers=[env.worker.ref()],
        cause=ExitCause.EXIT,
    )
    reopen(env)
    assert env.wire.call(MessageType.WORKER_EXIT, old_exit).status == Status.EPOCH
    wrong = replace(env.worker.ref(), pid_start=env.worker.pid_start + 1)
    assert env.gpu.worker_exit(wrong).items[0].result == ItemResult.REJECTED
    other_inc = replace(env.worker.ref(), incarnation="other-worker")
    assert env.gpu.worker_exit(other_inc).items[0].result == ItemResult.OK
    assert_departure_pins(env, hits)
    assert env.gpu.worker_exit(env.worker.ref()).items[0].result == ItemResult.OK
    drain(env.service)
    assert not env.service.pins.holds(env.worker.token)
    assert env.gpu.worker_exit(env.worker.ref()).items[0].result == ItemResult.OK


def test_orphan_watch_survives_a_new_run_dying_and_a_lost_exit(env, monkeypatch):
    from maru_server import node_registry

    monkeypatch.setattr(node_registry, "MAX_BATCH", 2)
    workers = [env.worker] + [
        FakeWorker(env.wire, env.gpu, tp_rank=i) for i in range(1, 5)
    ]
    for worker in workers:
        if worker is not env.worker:
            worker.open()
        worker.lookup(key("remote_dram"))
    first = reopen(env)
    assert len(first.orphans) == 2 and first.next_cursor
    page = orphans(env, first.next_cursor)
    assert len(page.orphans) == 2 and orphans(env, first.next_cursor) == page
    last = orphans(env, page.next_cursor)
    assert len(last.orphans) == 1 and last.next_cursor is None
    listed = first.orphans + page.orphans + last.orphans
    assert {o.incarnation for o in listed} == {w.incarnation for w in workers}
    old_envelope = env.gpu.envelope()
    old_cursor = first.next_cursor
    assert len(reopen(env).orphans) == 2
    assert orphans(env, old_cursor).status == Status.REJECTED
    assert orphans(env, **old_envelope).status == Status.EPOCH
    # Applying EXIT and losing its answer leaves the next listing correct.
    env.gpu.worker_exit(workers[0].ref())
    assert env.gpu.worker_exit(workers[0].ref()).items[0].result == ItemResult.OK
    assert env.worker.incarnation not in {o.incarnation for o in reopen(env).orphans}
    assert len(env.service.registry.nodes["gpu"].session.orphans) == 4


@pytest.mark.parametrize(
    "cursor", ["bad", "foreign:0", "x:-1", "x:999999999999999999999999999", "x:١"]
)
def test_bad_orphan_cursor_is_rejected(env, cursor):
    env.worker.lookup(key("remote_dram"))
    reopen(env)
    cursor = cursor.replace("x:", env.gpu.token + ":")
    assert orphans(env, cursor).status == Status.REJECTED


def test_sessions_copy_boot_and_pid_namespace(env):
    old_session = env.service.registry.known_worker(env.worker.token)
    assert (old_session.host_boot_id, old_session.pid_ns) == (env.gpu.boot, 101)
    env.worker.lookup(key("remote_dram"))
    response = reopen(env)
    orphan = response.orphans[0]
    assert (orphan.host_boot_id, orphan.pid_ns, orphan.pid, orphan.pid_start) == (
        env.gpu.boot,
        101,
        env.worker.pid,
        env.worker.pid_start,
    )
    assert env.worker.open().status == Status.OK
    current = env.service.registry.known_worker(env.worker.token)
    assert (current.host_boot_id, current.pid_ns) == (env.gpu.boot, 101)
    assert not current.orphan


@pytest.mark.parametrize("leave", [False, True])
def test_worker_rdma_rejects_a_new_pid_namespace_on_the_same_boot(env, leave):
    env.worker.lookup(key("remote_dram"))
    if leave:
        env.gpu.close(CloseMode.LEAVE)
    env.gpu.restart()
    response = env.gpu.open(pid_ns=202)
    assert response.status == Status.REJECTED and "pid_ns" in response.error
    assert env.service.registry.nodes["gpu"].epoch_seq == 1
    assert env.service.pins.holds(env.worker.token)


def test_worker_rdma_needs_a_known_pid_namespace(env):
    other = FakeNode(env.wire, "other")
    other.capabilities = [WORKER_RDMA_CAPABILITY]
    assert other.open().status == Status.REJECTED
    assert "other" not in env.service.registry.nodes


def test_reboot_releases_orphans_of_the_old_boot(env):
    hit = env.worker.lookup(key("remote_dram")).items[0]
    env.gpu.restart(boot="boot-2")
    response = env.gpu.open(pid_ns=202)
    assert response.status == Status.OK
    assert response.orphans[0].host_boot_id == "boot-1"
    assert env.service.pins.get(hit.pin_id) is not None
    assert env.service.stats()["orphan_unwatchable"] == 0
    env.gpu.worker_exit(env.worker.ref())
    drain(env.service)
    assert env.service.pins.get(hit.pin_id) is None


def test_an_orphan_without_evidence_of_its_end_is_kept(env):
    env.gpu.capabilities = [REMOTE_CAPABILITY]
    # A separate legacy node has no fixed pid namespace contract.
    legacy = FakeNode(env.wire, "legacy")
    legacy.open(pools=[legacy.cxl(region_id=9)], pid_ns=303)
    loc = legacy.loc(legacy.cxl(region_id=9), 1)
    legacy.commit((key("legacy"), loc))
    worker = FakeWorker(env.wire, legacy)
    worker.open()
    pin = worker.lookup(key("legacy")).items[0].pin_id
    legacy.restart()
    legacy.open(pools=[legacy.cxl(region_id=9)], pid_ns=404)
    drain(env.service)
    assert env.service.stats()["orphan_unwatchable"] == 1
    replacement = FakeWorker(env.wire, legacy)
    replacement.open()
    env.clock.advance(31)
    drain(env.service)
    assert env.service.pins.get(pin) is not None
    assert replacement.incarnation != worker.incarnation


@pytest.mark.parametrize("name", ["remote_dram", "remote_cxl"])
def test_worker_rdma_hit_owns_a_worker_pin_and_pool_registration(env, name):
    hit = env.worker.lookup(key(name)).items[0]
    assert hit.result == ItemResult.HIT
    assert hit.location == env.targets[name]
    assert (
        hit.pool_registration
        == env.memory.last_open.pool_registrations[hit.location.pool_id]
    )
    pin = env.service.pins.get(hit.pin_id)
    assert pin.holder_kind == HolderKind.WORKER and pin.keep_on_departure


def test_remote_lookups_select_only_the_connected_memory_node(env):
    other = FakeNode(env.wire, "other")
    other.capabilities = [MEMORY_CAPABILITY]
    assert other.open().status == Status.OK
    only, both = key("other-only"), key("both")
    first, second = (other.loc(other.dram(), i) for i in (1, 2))
    reachable = env.memory.loc(env.memory.dram(), 2)
    assert all(
        i.result == ItemResult.ADDED
        for i in other.commit((only, first), (both, second)).items
    )
    assert env.memory.commit((both, reachable)).items[0].result == ItemResult.ADDED
    probe = FakeWorker(env.wire, env.gpu, role=SessionRole.PROBE)
    assert probe.open().status == Status.OK

    def check(expected):
        hints = probe.probe(only, both, with_locations=True).items
        reads = env.worker.lookup(only, both).items
        for hint, read, location in zip(hints, reads, expected, strict=True):
            assert hint.location == read.location == location
            assert (
                hint.result
                == read.result
                == (ItemResult.HIT if location else ItemResult.MISS)
            )
            assert hint.pin_id is None
            assert bool(read.pin_id) == bool(location)
            if read.pin_id:
                env.worker.unpin(read.pin_id)

    check([None, reachable])
    env.gpu.heartbeat(memory_peer=(other.node_id, other.epoch))
    check([first, second])
    env.gpu.heartbeat()
    check([None, None])


def test_memory_role_is_required_for_direct_hit(env):
    env.memory.capabilities = []
    env.memory.open(pools=[env.memory.dram(), env.memory.cxl(region_id=8)])
    assert env.worker.lookup(key("remote_dram")).items[0].result == ItemResult.MISS


def test_same_pool_new_registration_has_a_new_number(env):
    old = env.worker.lookup(key("remote_cxl")).items[0]
    env.memory.restart()
    response = env.memory.open(pools=[env.memory.dram(), env.memory.cxl(region_id=8)])
    assert response.adopted
    k = key("remote_cxl")
    assert env.worker.lookup(k).items[0].result == ItemResult.MISS
    assert env.worker.probe(k).items[0].result == ItemResult.MISS
    env.gpu.heartbeat(memory_peer=(env.memory.node_id, env.memory.epoch))
    new = env.worker.lookup(key("remote_cxl")).items[0]
    assert new.location == old.location
    assert new.pool_registration > old.pool_registration
    pool = env.service.catalog.pool(new.location.pool_id)
    assert pool.since_epoch_seq == 1 and pool.registration == new.pool_registration
    env.memory.open(pools=[env.memory.dram(), env.memory.cxl(region_id=8)])
    assert (
        env.worker.lookup(key("remote_cxl")).items[0].pool_registration
        == new.pool_registration
    )


def test_orphan_stats_count_retained_pins_bytes_and_age(env):
    lookup_all(env)
    reopen(env)
    assert env.service.stats()["orphan_pins"] == 3
    assert env.service.stats()["orphan_bytes"] == 3 * PAGE
    drain(env.service)
    env.clock.advance(7)
    stats = env.service.stats()
    assert {
        name: stats[name]
        for name in (
            "orphan_sessions",
            "orphan_pins",
            "orphan_bytes",
            "orphan_oldest_s",
            "orphan_unwatchable",
        )
    } == {
        "orphan_sessions": 1,
        "orphan_pins": 3,
        "orphan_bytes": 3 * PAGE,
        "orphan_oldest_s": 7,
        "orphan_unwatchable": 0,
    }
    env.gpu.worker_exit(env.worker.ref())
    drain(env.service)
    stats = env.service.stats()
    assert all(
        stats[name] == 0
        for name in (
            "orphan_sessions",
            "orphan_pins",
            "orphan_bytes",
            "orphan_oldest_s",
            "orphan_unwatchable",
        )
    )


def test_memory_node_give_up_drops_its_pool_pins(env):
    env.worker.lookup(key("remote_dram"), key("remote_cxl"))
    reopen(env)
    for _ in range(32):
        env.clock.advance(20)
        env.gpu.heartbeat()
        drain(env.service)
    assert env.service.registry.nodes["memory"].gone
    assert not env.service.pins.holds(env.worker.token)


def test_replayed_lookup_keeps_the_original_pool_registration(env):
    request = env.worker.build(
        LookupAndPinRequest, **env.worker.envelope(), items=[key("remote_cxl")]
    )
    first = env.wire.call(MessageType.LOOKUP_AND_PIN, request)
    env.memory.restart()
    env.memory.open(pools=[env.memory.dram(), env.memory.cxl(region_id=8)])
    assert env.wire.call(MessageType.LOOKUP_AND_PIN, request) == first
    env.gpu.heartbeat(memory_peer=(env.memory.node_id, env.memory.epoch))
    current = env.worker.lookup(key("remote_cxl")).items[0]
    assert current.pool_registration != first.items[0].pool_registration


def test_probe_with_locations_carries_registration_without_a_pin(env):
    before = len(env.service.pins)
    hit = env.worker.probe(key("remote_dram"), with_locations=True).items[0]
    assert hit.result == ItemResult.HIT
    assert (
        hit.pool_registration
        == env.memory.last_open.pool_registrations[hit.location.pool_id]
    )
    assert hit.location == env.targets["remote_dram"] and hit.pin_id is None
    assert len(env.service.pins) == before


@pytest.mark.parametrize("departure", ["new_run", "leave", "give_up"])
def test_departure_keeps_worker_pins_in_bounded_maintenance(env, departure):
    from maru_server.node_registry import PINS_INLINE

    location = env.targets["remote_dram"]
    local = env.targets["local_dram"]
    for _ in range(PINS_INLINE + 2):
        env.service.pins.pin(
            key("remote_dram"), location, env.worker.token, HolderKind.WORKER
        )
        env.service.pins.pin(
            key("local_dram"),
            local,
            env.worker.token,
            HolderKind.WORKER,
            keep_on_departure=False,
        )
    if departure == "new_run":
        reopen(env)
    elif departure == "leave":
        env.gpu.close(CloseMode.LEAVE)
    else:
        env.clock.advance(631)
        env.memory.open(pools=[env.memory.dram(), env.memory.cxl(region_id=8)])
    drain(env.service)
    pins = env.service.pins.held_by(env.worker.token)
    assert len(pins) == PINS_INLINE + 2
    assert all(p.location == location for p in pins)
    if departure != "new_run":
        reopen(env)
    env.gpu.worker_exit(env.worker.ref())
    assert env.service.pins.holds(env.worker.token)
    drain(env.service)
    assert not env.service.pins.holds(env.worker.token)


def test_same_pool_registration_never_repeats_when_its_owner_changes(env):
    old = env.worker.lookup(key("remote_cxl")).items[0]
    env.memory.close(CloseMode.LEAVE)
    drain(env.service)
    successor = FakeNode(env.wire, "successor")
    successor.capabilities = [MEMORY_CAPABILITY]
    pool = successor.cxl(region_id=8)
    reply = successor.open(pools=[pool])
    assert reply.status == Status.OK
    assert successor.epoch_seq == env.memory.epoch_seq == 1
    loc = successor.loc(pool, 1)
    successor.commit((key("successor"), loc))
    assert env.worker.lookup(key("successor")).items[0].result == ItemResult.MISS
    env.gpu.heartbeat(memory_peer=(successor.node_id, successor.epoch))
    hit = env.worker.lookup(key("successor")).items[0]
    assert hit.location.pool_id == old.location.pool_id
    assert hit.pool_registration != old.pool_registration
    assert hit.pool_registration == reply.pool_registrations[pool.pool_id]
    assert env.service.pins.get(old.pin_id) is not None


def test_capability_absence_is_a_remote_miss(env):
    env.gpu.capabilities = [REMOTE_CAPABILITY]
    env.gpu.open(pools=[env.gpu.dram(), env.gpu.cxl()], pid_ns=101)
    env.gpu.heartbeat(memory_peer=(env.memory.node_id, env.memory.epoch))
    assert env.worker.lookup(key("remote_dram")).items[0].result == ItemResult.MISS
