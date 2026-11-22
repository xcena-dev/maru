# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""The fake MaruServer chooses lookup results as MaruServer does."""

import time

import pytest

from maru_common.storage_types import (
    ItemResult,
    Key,
    Location,
    MissReason,
    PoolKind,
    SessionRole,
)
from tests.unit.ms_fake_server import Entry, FakeMaruServer, NodeRec, WorkerRec

KEY = Key("ns", "k")


def location(pool_id: str, kind: PoolKind) -> Location:
    return Location(pool_id, "a", 1, 0, 4096, kind)


@pytest.fixture
def server():
    server = FakeMaruServer()
    for node_id in ("a", "b"):
        server.nodes[node_id] = NodeRec(node_id, "run", "s", 1, {}, time.monotonic())
    yield server
    server.close()


def worker() -> WorkerRec:
    return WorkerRec(
        "t", "inc", "a", "run", "e", 0, 0, 1, 1, "ns", SessionRole.WORKER, 0.0
    )


def add(server: FakeMaruServer, node_id: str, pool_id: str, kind: PoolKind) -> None:
    entry = Entry(KEY, location(pool_id, kind), 4096, node_id)
    server.catalog.setdefault(KEY, []).append(entry)


def test_another_nodes_memory_copy_is_not_a_fetch_source(server):
    add(server, "b", "dram:b:1", PoolKind.DRAM)
    assert server._find(KEY, worker()) == (ItemResult.MISS, MissReason.OFFLINE)


def test_remote_fetch_requires_the_workers_node_capability(server):
    from maru_common.storage_types import (
        MEMORY_CAPABILITY,
        WORKER_RDMA_CAPABILITY,
    )

    add(server, "b", "cxl:memory:1", PoolKind.CXL)
    server.nodes["a"].capabilities = [WORKER_RDMA_CAPABILITY]
    server.nodes["b"].capabilities = [MEMORY_CAPABILITY]
    result, found = server._find(KEY, worker())
    assert result == ItemResult.HIT and found.node_id == "b"
