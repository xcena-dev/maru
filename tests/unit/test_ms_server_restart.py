# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruServer after a restart, through MsService: it knows no KV and answers
MISS at once; a node comes back only as a new run."""

import json

import pytest

from maru_common.protocol import MS_MESSAGE_CLASSES, MessageType, NodeOpenResponse
from maru_common.storage_types import ItemResult, KeptCause, MissReason, Status
from tests.unit.ms_fakes import (
    FakeClock,
    FakeNode,
    FakeWorker,
    Wire,
    admin_delete,
    key,
    make_service,
)


@pytest.fixture
def env():
    clock = FakeClock()
    service = make_service(clock)
    return clock, service, Wire(service)


def results(response):
    return [item.result for item in response.items]


def test_an_absent_key_is_a_miss_at_once(env):
    _, _, wire = env
    node = FakeNode(wire, "n1")
    assert node.open().status == Status.OK
    worker = FakeWorker(wire, node)
    assert worker.open().status == Status.OK
    for response in (worker.probe(key("k")), worker.lookup(key("k"))):
        assert results(response) == [ItemResult.MISS]
        assert response.items[0].reason == MissReason.ABSENT
    deleted = admin_delete(wire, key("k"))
    assert deleted.items[0].reason == MissReason.ABSENT


def test_the_recovery_messages_are_gone():
    assert not {0x43, 0x5C} & {int(code) for code in MessageType}
    assert not {0x43, 0x5C} & {int(code) for code in MS_MESSAGE_CLASSES}
    assert "recovering" not in NodeOpenResponse.__dataclass_fields__


def test_refused_opens_carry_the_server_epoch(env):
    _, service, wire = env
    node = FakeNode(wire, "bad id")
    refused = node.open()
    assert refused.status == Status.REJECTED
    assert refused.server_epoch == service.server_epoch
    worker = FakeWorker(wire, node)
    early = worker.open(node_epoch=node.epoch)
    assert early.status == Status.EPOCH
    assert early.server_epoch == service.server_epoch


def test_a_worker_ahead_of_its_node_is_told_so(env):
    _, _, wire = env
    node = FakeNode(wire, "n1")  # opens later
    worker = FakeWorker(wire, node)
    early = worker.open(node_epoch=node.epoch)
    assert early.status == Status.EPOCH
    assert "not opened its session here yet" in early.error
    assert "attach" not in early.error
    node.open()
    assert worker.open().status == Status.OK
    node.restart()  # a new run: the old run is named as replaced
    node.open()
    stale = worker.open(node_epoch=worker.node_epoch)
    assert stale.status == Status.EPOCH and "attach" in stale.error


def test_policy_is_read_again_at_start(tmp_path):
    path = tmp_path / "policy.json"
    k = key("keep")
    path.write_text(json.dumps({"preserve": [{"namespace": "ns", "key": "keep"}]}))
    service = make_service(FakeClock(), policy_file=str(path))
    node = FakeNode(Wire(service), "n1")
    node.open()
    loc = node.loc(node.dram(), 0)
    assert results(node.commit((k, loc))) == [ItemResult.ADDED]
    kept = node.retire((k, loc))
    assert results(kept) == [ItemResult.KEPT]
    assert kept.items[0].cause == KeptCause.PINNED
