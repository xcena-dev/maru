# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Memory-node negotiation and FETCH eligibility."""

from dataclasses import replace

import pytest

from maru_common.protocol import MessageType
from maru_common.storage_types import (
    MEMORY_CAPABILITY,
    REMOTE_CAPABILITY,
    WORKER_RDMA_CAPABILITY,
    CloseMode,
    Status,
)
from tests.unit.ms_fakes import (
    FakeClock,
    FakeNode,
    Wire,
    make_service,
)


@pytest.fixture
def env():
    clock = FakeClock()
    service = make_service(clock)
    return clock, service, Wire(service)


def open_node(node, capabilities=(), *, cxl=False):
    pools = [node.dram()] + ([node.cxl()] if cxl else [])
    request = replace(
        node.open_request(pools=pools, pid_ns=101), capabilities=list(capabilities)
    )
    response = node.wire.call(MessageType.NODE_OPEN, request)
    assert response.status == Status.OK
    node.token, node.epoch_seq = response.node_session, response.epoch_seq
    return response


@pytest.mark.parametrize(
    "requested, accepted",
    [
        ([], []),
        (["unknown_v1"], []),
        ([REMOTE_CAPABILITY], [REMOTE_CAPABILITY]),
        (
            [MEMORY_CAPABILITY, WORKER_RDMA_CAPABILITY],
            [MEMORY_CAPABILITY, WORKER_RDMA_CAPABILITY],
        ),
        (
            [
                "unknown_v1",
                REMOTE_CAPABILITY,
                WORKER_RDMA_CAPABILITY,
                REMOTE_CAPABILITY,
                MEMORY_CAPABILITY,
            ],
            [REMOTE_CAPABILITY, WORKER_RDMA_CAPABILITY, MEMORY_CAPABILITY],
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
    again = open_node(node, [REMOTE_CAPABILITY, WORKER_RDMA_CAPABILITY, "unknown_v2"])
    assert again.epoch_seq == first.epoch_seq
    assert again.capabilities == [REMOTE_CAPABILITY, WORKER_RDMA_CAPABILITY]
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
