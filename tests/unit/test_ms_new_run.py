# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""A new run instead of recovery: Node starts a new StorageService run in the
same process when MaruServer restarted or ended the run, when a result stays
unknown, or when the RM lease is lost; workers re-attach without a restart.
Also the worker's side of a MaruServer restart."""

import dataclasses
import threading
import time

import pytest

from maru_common.protocol import MessageType
from maru_common.storage_types import (
    CloseMode,
    ExitCause,
    ItemOutcome,
    ItemResult,
    Key,
    KeyLocation,
    PoolKind,
)
from maru_shm.types import NodeLeaseExpiredError
from maru_storage_service.ports import EpochChanged
from maru_storage_service.service import Node, StorageService
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import (
    PAGE,
    FakeRm,
    RawWorker,
    anon_region,
    node_config,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)

NS = "ns"
SERVICE_OPTIONS = {
    "rpc_timeout_s": 0.3,
    "gpu_drain_grace_s": 0.3,
    "evict_interval_s": 0.02,
    "kept_backoff_s": 0.2,
}


@pytest.fixture
def fake():
    server = FakeMaruServer(heartbeat_s=0.1)
    yield server
    server.close()


@pytest.fixture
def sockdir():
    path = short_tmpdir()
    yield path
    remove_tree(path)


@pytest.fixture
def nodes(fake, sockdir):
    """A StorageService run alone: nothing starts the next one."""
    started = []

    def make(**kwargs):
        service = start_node(node_config(fake.url, f"{sockdir}/node.sock", **kwargs))
        started.append(service)
        return service

    yield make
    for service in started:
        service.stop()


@pytest.fixture
def runs(fake, sockdir):
    """A Node serving its runs on a thread of its own; ``node.kept`` holds
    what run() returned."""
    started = []

    def make(pause_s: float = 0.05, cxl: dict | None = None, **kwargs):
        config = node_config(fake.url, f"{sockdir}/node.sock", **kwargs)
        options = dict(SERVICE_OPTIONS, **(cxl or {}))
        node = Node(config, lambda c: StorageService(c, **options), pause_s=pause_s)
        node.kept = []
        thread = threading.Thread(
            target=lambda: node.kept.append(node.run()), daemon=True
        )
        thread.start()
        started.append((node, thread))
        new_run(node, None)
        return node

    yield make
    for node, thread in started:
        node.stop()
        thread.join(15)


def new_run(node: Node, old: StorageService | None) -> StorageService:
    """The run after ``old``, once it serves."""

    def serving():
        service = node.service
        if (
            service is None
            or service is old
            or service.local is None
            or not service.local.started
            or service.wait(0)
        ):
            return None
        return service

    return wait_until(serving, timeout=15, what="a run that serves")


# =============================================================================
# What ends a run, and what follows
# =============================================================================


def test_an_expired_session_goes_on_in_the_same_run(fake, runs):
    node = runs()
    service = node.service
    with fake._lock:
        fake.node_sessions.pop(fake.nodes["gpu-test"].session)
    wait_until(lambda: service.server.renewals == 1, what="reopened")
    time.sleep(0.3)
    assert node.service is service and service.ended is None
    assert not service.wait(0)


def test_a_run_maruserver_ended_is_followed_by_a_new_run(fake, runs):
    node = runs()
    first = node.service
    with fake._lock:  # MaruServer gave the node up
        rec = fake.nodes["gpu-test"]
        fake.node_sessions.pop(rec.session)
        fake.ended_epochs.add(rec.node_epoch)
    new_run(node, first)
    assert "ended this run" in first.ended


def test_an_unknown_commit_result_starts_a_new_run(fake, runs):
    """Retried with the same op while the result is unknown; a result that
    cannot be used ends the run (closed with restart)."""
    node = runs()
    first = node.service
    raw = RawWorker(node.config.socket)
    raw.attach()
    try:
        (credit,) = raw.credit(PAGE).credits
        fake.drop_responses(MessageType.COMMIT)  # resent with the same op
        assert raw.write_done([credit], ["k1"]).items[0].result == ItemResult.ADDED
        assert fake.replayed[MessageType.COMMIT] == 1
        assert node.service is first and first.ended is None

        def stale(response):
            response.items = [ItemOutcome(ItemResult.STALE)] * len(response.items)
            return response

        fake.rewrite_responses(MessageType.COMMIT, stale)
        (credit,) = raw.credit(PAGE).credits
        raw.write_done([credit], ["k2"])
    finally:
        raw.close()
    new_run(node, first)
    assert "COMMIT" in first.ended
    closes = fake.requests(MessageType.NODE_CLOSE)
    assert [c.mode for c in closes] == [CloseMode.RESTART]


def test_a_lost_lease_leaves_then_starts_a_new_run(fake, runs):
    rm = FakeRm()
    cxl = {"region_source": rm, "cxl_renew_s": 0.02, "map_region": anon_region}
    node = runs(pause_s=0.5, cxl=cxl, cxl_pages=4, region_pages=2)
    first = node.service
    rm.renew_error = NodeLeaseExpiredError("the lease ran out")
    wait_until(lambda: first.ended, what="the run ended")
    rm.renew_error = None
    second = new_run(node, first)
    assert "lease ended" in first.ended and first.new_run
    assert rm.released == [("gpu-test", first.node_epoch)]  # left
    assert rm.epochs["gpu-test"] == second.node_epoch  # claimed again
    assert second.tier("local_cxl").pools and not node.kept


def test_another_runs_claim_ends_the_node(fake, runs):
    rm = FakeRm()
    cxl = {"region_source": rm, "cxl_renew_s": 0.02, "map_region": anon_region}
    node = runs(cxl=cxl, cxl_pages=4, region_pages=2)
    first = node.service
    rm.epochs["gpu-test"] = "someone-else"
    wait_until(lambda: node.kept, timeout=15, what="run() returned")
    assert node.kept == [False] and node.runs == 1
    assert "another run claimed" in node.ended
    assert not fake.nodes["gpu-test"].online  # closed with leave
    assert first.wait_finished(0)


def test_a_stop_during_a_start_ends_the_node(fake, sockdir):
    fake.pause()  # NODE_OPEN waits
    node = Node(node_config(fake.url, f"{sockdir}/node.sock"), pause_s=0.05)
    kept = []
    thread = threading.Thread(target=lambda: kept.append(node.run()))
    thread.start()
    wait_until(lambda: fake.requests(MessageType.NODE_OPEN) or fake._held, what="open")
    node.stop()
    thread.join(10)
    fake.resume()
    assert kept == [True] and node.runs == 1


def test_a_session_lost_during_the_start_starts_a_new_run(fake, runs, monkeypatch):
    calls = []
    real = StorageService._start

    def lose_once(self):
        real(self)
        if not calls:
            calls.append(self)
            raise EpochChanged("lost during the start")

    monkeypatch.setattr(StorageService, "_start", lose_once)
    node = runs()
    # The first run can already listen before _start raises. Wait for its
    # replacement, rather than treating that brief listening state as run 2.
    first = wait_until(lambda: calls and calls[0], what="the failed start")
    new_run(node, first)
    assert node.runs == 2 and calls[0].ended and calls[0].new_run


# =============================================================================
# The node's parts
# =============================================================================


def test_local_pins_close_once_the_run_ended(fake, nodes):
    node = nodes()
    manager = node.manager
    wait_until(lambda: node.server.last_heartbeat_ok > 0, what="a heartbeat")
    assert manager.local_pins_open()
    node.server.last_heartbeat_ok = time.monotonic() - node.server.session_ttl_s
    assert not manager.local_pins_open()  # node_stale_s passed
    node.server.last_heartbeat_ok = time.monotonic()
    manager.end_run("test")
    assert not manager.local_pins_open()
    assert node.wait_finished(10) and node.ended == "test" and node.new_run


def test_an_op_is_never_resent_to_a_new_maruserver(fake):
    from maru_common.storage_types import PoolInfo, WorkerRef
    from maru_storage_service.server_client import ServerClient

    client = ServerClient(
        fake.url, "solo", "run-1", rpc_timeout_s=0.2, host_boot_id="boot"
    )
    dram = PoolInfo(
        pool_id="dram:solo:run-1",
        kind=PoolKind.DRAM,
        node_id="solo",
        pool_generation=1,
        capacity=4 * PAGE,
        page_size=PAGE,
    )
    try:
        client.open([dram])
        old = client.server_epoch
        ref = WorkerRef("e", 0, 0, 1, 1, "gone-worker", None)
        errors = []

        def report():
            try:
                client.worker_exit([ref], ExitCause.EXIT)
            except EpochChanged as e:
                errors.append(e)

        fake.pause(hold=False)  # unanswered: resent with the same op
        thread = threading.Thread(target=report)
        thread.start()
        wait_until(lambda: client.resends > 0, what="resent")
        fake.restart()
        fake.resume()
        thread.join(15)
        assert errors, "the op did not end"
        assert client.epoch_lost and "restarted" in client.lost_reason
        assert client.server_epoch == old
        assert all(
            r.server_epoch == old for r in fake.requests(MessageType.WORKER_EXIT)
        )
        assert fake.executed[MessageType.WORKER_EXIT] == 0
    finally:
        client.close()


def test_a_restart_between_handshake_and_reopen_is_a_restart(fake, nodes):
    from maru_common.protocol import HandshakeResponse

    node = nodes()
    client = node.server
    old = client.server_epoch
    call = client._rpc.call
    state = {"once": False}

    def old_server_answers_first(code, request, *a, **kw):
        if code == MessageType.HANDSHAKE and not state["once"]:
            state["once"] = True  # the old one answers, then is gone
            return HandshakeResponse(success=True, server_epoch=old)
        return call(code, request, *a, **kw)

    client._rpc.call = old_server_answers_first
    fake.restart()
    assert node.wait_finished(15)
    assert "restarted" in node.ended and node.new_run
    assert client.server_epoch == old


# =============================================================================
# The worker's side of a MaruServer restart
# =============================================================================


# =============================================================================
# The worker unmaps its pools when the node connection is lost
# =============================================================================


def test_a_maruserver_restart_starts_a_new_run(fake, runs):
    node = runs()
    first = node.service
    worker = RawWorker(first.config.socket)
    worker.attach()
    worker.write_done(worker.credit(PAGE).credits, ["k"])
    old_pool = first.tier("dram").pool.pool_id
    fake.restart()
    second = new_run(node, first)
    worker.close()
    assert first.new_run and "restarted" in first.ended
    assert second.tier("dram").pool.pool_id != old_pool
    assert Key("ns", "k") not in fake.keys()
    again = RawWorker(second.config.socket)
    try:
        assert again.attach().node_epoch == second.node_epoch
        assert (
            again.write_done(again.credit(PAGE).credits, ["new"]).items[0].result
            is ItemResult.ADDED
        )
    finally:
        again.close()
    assert node.runs == 2


def test_an_unknown_retire_result_starts_a_new_run(fake, runs):
    from tests.unit.ms_node_helpers import publish

    node = runs()
    first = node.service
    publish(first, "k")
    ((ref, key, location),) = first.pools.ready_pages()
    fake.rewrite_responses(
        MessageType.RETIRE,
        lambda r: dataclasses.replace(r, items=[ItemOutcome(ItemResult.STALE)]),
    )
    first.manager.retire([ref], [KeyLocation(key, location)])
    new_run(node, first)
    assert "RETIRE" in first.ended
