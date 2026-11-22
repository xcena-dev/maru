# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Memory-node negotiation and FETCH eligibility."""

from dataclasses import replace

import pytest

from maru_common.protocol import MessageType, SourcePinRequest
from maru_common.storage_types import (
    FETCH_CAPABILITY,
    MEMORY_CAPABILITY,
    REMOTE_CAPABILITY,
    CloseMode,
    KeyLocation,
    PoolKind,
    SessionRole,
    Status,
)
from tests.unit.ms_fakes import (
    FakeClock,
    FakeNode,
    FakeWorker,
    Wire,
    key,
    make_service,
)


@pytest.fixture
def env():
    clock = FakeClock()
    service = make_service(clock)
    return clock, service, Wire(service)


def open_node(node, capabilities=(), *, cxl=False):
    pools = [node.dram()] + ([node.cxl()] if cxl else [])
    request = replace(node.open_request(pools=pools), capabilities=list(capabilities))
    response = node.wire.call(MessageType.NODE_OPEN, request)
    assert response.status == Status.OK
    node.token, node.epoch_seq = response.node_session, response.epoch_seq
    return response


def bound_pin(node, worker, k, source):
    return node.send(
        MessageType.SOURCE_PIN,
        node.build(
            SourcePinRequest,
            **node.envelope(),
            worker_session=worker.token,
            worker_incarnation=worker.incarnation,
            items=[KeyLocation(k, source)],
        ),
    ).items[0]


def setup_fetch(
    wire,
    kind=PoolKind.DRAM,
    requester_caps=(REMOTE_CAPABILITY,),
    owner_caps=(MEMORY_CAPABILITY,),
):
    gpu, memory = FakeNode(wire, "gpu"), FakeNode(wire, "memory")
    open_node(gpu, requester_caps)
    open_node(memory, owner_caps, cxl=True)
    k = key("fetch")
    source = gpu.loc(gpu.dram(), 1)
    dest = memory.loc(getattr(memory, kind.value)(), 1)
    assert gpu.commit((k, source)).items[0].result == "ADDED"
    assert memory.commit((k, dest)).items[0].result == "ADDED"
    return gpu, memory, k, source, dest


@pytest.mark.parametrize(
    "requested, accepted",
    [
        ([], []),
        (["unknown_v1"], []),
        ([REMOTE_CAPABILITY], [REMOTE_CAPABILITY]),
        ([MEMORY_CAPABILITY, FETCH_CAPABILITY], [MEMORY_CAPABILITY, FETCH_CAPABILITY]),
        (
            [
                "unknown_v1",
                REMOTE_CAPABILITY,
                FETCH_CAPABILITY,
                REMOTE_CAPABILITY,
                MEMORY_CAPABILITY,
            ],
            [REMOTE_CAPABILITY, FETCH_CAPABILITY, MEMORY_CAPABILITY],
        ),
    ],
)
def test_node_open_stores_and_returns_only_accepted_capabilities(
    env, requested, accepted
):
    _, service, wire = env
    node = FakeNode(wire, "gpu")
    response = open_node(node, requested)
    assert response.capabilities == accepted
    assert service.registry.nodes[node.node_id].session.capabilities == tuple(accepted)
    assert service.registry.capabilities_of(node.node_id) == tuple(accepted)
    assert service.registry.capabilities_of("unknown-node") == ()


def test_same_epoch_reopen_renegotiates_capabilities(env):
    clock, service, wire = env
    node = FakeNode(wire, "gpu")
    first = open_node(node, [REMOTE_CAPABILITY, "unknown_v1"])
    clock.advance(30)
    assert service.registry.capabilities_of(node.node_id) == ()
    again = open_node(node, [REMOTE_CAPABILITY, FETCH_CAPABILITY, "unknown_v2"])
    assert again.epoch_seq == first.epoch_seq
    assert again.capabilities == [REMOTE_CAPABILITY, FETCH_CAPABILITY]
    assert service.registry.node_session(first.node_session, clock()) is None
    assert service.registry.capabilities_of(node.node_id) == tuple(again.capabilities)
    node.restart()
    assert open_node(node).capabilities == []
    assert service.registry.capabilities_of(node.node_id) == ()


@pytest.mark.parametrize("mode", list(CloseMode))
def test_closed_node_has_no_live_capabilities(env, mode):
    _, service, wire = env
    node = FakeNode(wire, "memory")
    open_node(node, [MEMORY_CAPABILITY])
    assert node.close(mode).status == Status.OK
    assert service.registry.capabilities_of(node.node_id) == ()


@pytest.mark.parametrize("kind", [PoolKind.DRAM, PoolKind.CXL])
@pytest.mark.parametrize(
    "requester_caps", [(), (REMOTE_CAPABILITY,), (MEMORY_CAPABILITY,)]
)
@pytest.mark.parametrize("owner_caps", [(), (MEMORY_CAPABILITY,), (REMOTE_CAPABILITY,)])
def test_memory_fetch_capability_table(env, kind, requester_caps, owner_caps):
    _, service, wire = env
    gpu, memory, k, own, source = setup_fetch(wire, kind, requester_caps, owner_caps)
    # Leave just the remote copy for lookup and worker-bound SOURCE_PIN.
    assert gpu.retire((k, own)).items[0].result == "RETIRED"
    pool = service.catalog.pool(source.pool_id)
    expected = REMOTE_CAPABILITY in requester_caps and MEMORY_CAPABILITY in owner_caps
    assert service.registry.can_fetch(gpu.node_id, pool) == expected
    worker = FakeWorker(wire, gpu)
    assert worker.open().status == Status.OK
    probe = FakeWorker(wire, gpu, role=SessionRole.PROBE)
    assert probe.open().status == Status.OK
    item = worker.lookup(k).items[0]
    assert item.result == ("FETCH" if expected else "MISS")
    assert probe.probe(k).items[0].result == item.result
    if expected:
        assert item.plan.source == source
        assert item.plan.source_node == memory.node_id
    else:
        assert item.plan is None
    pinned = bound_pin(gpu, worker, k, source)
    assert pinned.result == ("HIT" if expected else "REJECTED")
    if expected:
        assert service.pins.get(pinned.pin_id).location == source
    # Node pins do not require remote I/O capabilities.
    unbound = gpu.source_pin((k, source)).items[0]
    assert unbound.result == "HIT"
    assert service.pins.get(unbound.pin_id).location == source


@pytest.mark.parametrize("kind", [PoolKind.DRAM, PoolKind.CXL])
def test_a_memory_fetch_reports_the_read_to_the_memory_node(env, kind):
    _, _, wire = env
    gpu, memory, k, own, source = setup_fetch(wire, kind)
    assert gpu.retire((k, own)).items[0].result == "RETIRED"
    worker = FakeWorker(wire, gpu)
    assert worker.open().status == Status.OK
    assert worker.lookup(k).items[0].result == "FETCH"
    assert list(memory.heartbeat().access.reads()) == []
    pin = bound_pin(gpu, worker, k, source).pin_id
    done = gpu.fetch_complete((k, pin))
    assert done.items[0].result == "OK"
    assert list(gpu.heartbeat().access.reads()) == []
    assert list(memory.heartbeat().access.reads()) == [
        (source.pool_id, source.offset, source.generation)
    ]


@pytest.mark.parametrize("kind", [PoolKind.DRAM, PoolKind.CXL])
@pytest.mark.parametrize("state", ["expired", "restart", "leave", "hidden", "removing"])
def test_memory_fetch_requires_online_owner_and_active_pool(env, kind, state):
    clock, service, wire = env
    gpu, memory, k, own, source = setup_fetch(wire, kind)
    assert gpu.retire((k, own)).items[0].result == "RETIRED"
    worker = FakeWorker(wire, gpu)
    assert worker.open().status == Status.OK
    pool = service.catalog.pool(source.pool_id)
    if state == "expired":
        clock.advance(20)
        gpu.heartbeat()
        worker.heartbeat()
        clock.advance(10)
    elif state in ("restart", "leave"):
        memory.close(CloseMode(state))
    else:
        with service.lock:
            if state == "hidden":
                service.catalog.hide_pool(pool)
            else:
                service.catalog.remove_pool(pool)
    assert not service.registry.can_fetch(gpu.node_id, pool)
    assert worker.lookup(k).items[0].result == "MISS"
    assert bound_pin(gpu, worker, k, source).result != "HIT"
    assert gpu.source_pin((k, source)).items[0].result != "HIT"
    assert not service.pins.count(k, source)
