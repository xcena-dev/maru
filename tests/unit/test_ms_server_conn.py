# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""ServerConn: reopens after EPOCH, a heartbeat that survives a failing
callback, and lookups that never wait for a reopen."""

import logging
import os
import threading
import time

import pytest

from maru_common.ms_rpc import OpOrigin, RpcUnknownError
from maru_common.protocol import (
    LookupAndPinResponse,
    MessageType,
    ProbeResponse,
    UnpinResponse,
    WorkerCloseResponse,
    WorkerHeartbeatResponse,
    WorkerOpenResponse,
)
from maru_common.storage_types import ItemOutcome, ItemResult, Key, SessionRole, Status
from maru_handler.client import server_conn
from maru_handler.client.server_conn import ServerConn, WorkerIdentity
from tests.unit.ms_node_helpers import wait_until

KEYS = [Key("ns", "k")]


def make_conn(call, *, heartbeat_s: float = 30.0) -> ServerConn:
    """A conn with an open session s1 at server epoch "old"; ``call`` stands
    in for the RPC."""
    identity = WorkerIdentity("e", 0, 0, "inc", "ns")
    conn = ServerConn(
        "tcp://127.0.0.1:1",
        identity,
        node_id="n",
        node_epoch="node",
        pid=os.getpid(),
        pid_start=1,
        timeout_s=2.0,
        role=SessionRole.WORKER,
    )
    conn.session, conn.server_epoch, conn.session_node_epoch = "s1", "old", "node"
    conn.heartbeat_s = heartbeat_s
    conn._origin = OpOrigin("inc")
    conn._rpc.call = call
    return conn


def opened(session: str, epoch: str, heartbeat_s: float = 30) -> WorkerOpenResponse:
    return WorkerOpenResponse(
        status=Status.OK,
        worker_session=session,
        server_epoch=epoch,
        heartbeat_s=heartbeat_s,
        session_ttl_s=30,
    )


def test_a_reopen_after_epoch_is_skipped_once_the_session_moved_on():
    calls = []

    def call(code, request):
        calls.append(code)
        return opened("s3", "new")

    conn = make_conn(call)
    try:
        conn.session, conn.server_epoch = "s2", "new"  # the heartbeat reopened
        assert conn._reopen_after_epoch("old", "s1") is False  # moved on: no open
        assert not calls
        assert conn._reopen_after_epoch("new", "s1") is True
        assert not calls
        conn.session = "s1"  # still the request's session: reopened
        conn._reopen_after_epoch("old", "s1")
        assert calls == [MessageType.WORKER_OPEN] and conn.session == "s3"
    finally:
        conn._rpc.close()


def test_a_reopen_waiting_for_another_open_does_not_open_again():
    gate, entered, calls = threading.Event(), threading.Event(), []

    def call(code, request):
        calls.append(code)
        entered.set()
        assert gate.wait(10)
        return opened("s2", "old")

    conn = make_conn(call)
    first = threading.Thread(target=conn.open)
    second = threading.Thread(target=lambda: conn._reopen_after_epoch("old", "s1"))
    waiting = threading.Event()
    real_open = conn.open

    def open_marked(**kwargs):
        waiting.set()  # right before the open lock is asked for
        real_open(**kwargs)

    conn.open = open_marked
    try:
        first.start()
        assert entered.wait(5)
        second.start()
        assert waiting.wait(5)
        gate.set()
        first.join(5)
        second.join(5)
        assert conn.session == "s2" and calls == [MessageType.WORKER_OPEN]
    finally:
        gate.set()
        conn._rpc.close()


def test_the_heartbeat_goes_on_when_a_callback_raises(monkeypatch, caplog):
    monkeypatch.setattr(server_conn, "DOWN_POLL_S", 0.01)
    opens, beats = [], []

    def call(code, request):
        if code == MessageType.WORKER_OPEN:
            opens.append(1)
            return opened("s2", f"epoch{len(opens)}", 0.01)
        beats.append(1)
        return WorkerHeartbeatResponse(status=Status.OK)

    conn = make_conn(call, heartbeat_s=0.01)

    def broken(old, new):
        raise RuntimeError("callback bug")

    conn.on_server_epoch.append(broken)
    log = logging.getLogger("maru_handler.client.server_conn")
    log.addHandler(caplog.handler)
    try:
        conn.start()
        for want in (1, 2, 3):
            with conn._lock:
                conn._reopen = True
            conn._beat.set()
            wait_until(lambda want=want: len(opens) >= want, what=f"open {want}")
        seen = len(beats)
        wait_until(lambda: len(beats) > seen + 1, what="heartbeats")
        assert conn._thread.is_alive()
    finally:
        log.removeHandler(caplog.handler)
        conn.close(goodbye=False)
    failures = [r for r in caplog.records if "heartbeat failed" in r.getMessage()]
    assert len({id(r) for r in failures}) == 1  # once per error type and 30 s


@pytest.mark.parametrize("op", ["lookup", "probe"])
def test_lookups_and_probes_miss_without_an_rpc_while_a_reopen_is_due(op):
    calls = []
    conn = make_conn(lambda code, request: calls.append(code))
    try:
        conn._reopen = True
        result = conn.lookup_and_pin(KEYS) if op == "lookup" else conn.probe(KEYS)
        assert result is None and not calls
    finally:
        conn._rpc.close()


@pytest.mark.parametrize("op", ["lookup", "probe"])
def test_an_epoch_to_a_foreground_op_never_waits_for_the_reopen(op):
    gate, entered, open_threads = threading.Event(), threading.Event(), []

    def call(code, request, timeout_s=None):
        if code == MessageType.WORKER_OPEN:
            open_threads.append(threading.current_thread().name)
            entered.set()
            assert gate.wait(10)
            return opened("s2", "new")
        if code == MessageType.LOOKUP_AND_PIN:
            return LookupAndPinResponse(status=Status.EPOCH)
        if code == MessageType.PROBE:
            return ProbeResponse(status=Status.EPOCH)
        return WorkerHeartbeatResponse(status=Status.OK)

    conn = make_conn(call)
    try:
        conn.start()
        start = time.monotonic()
        result = conn.lookup_and_pin(KEYS) if op == "lookup" else conn.probe(KEYS)
        assert result is None and time.monotonic() - start < 2
        assert conn._reopen
        assert entered.wait(5)  # the heartbeat thread is in WORKER_OPEN, blocked
        # misses at once, while the open is blocked, without another open
        start = time.monotonic()
        assert conn.lookup_and_pin(KEYS) is None and conn.probe(KEYS) is None
        assert time.monotonic() - start < 2
        assert open_threads == ["maru-worker-heartbeat"]
    finally:
        gate.set()
        conn.close(goodbye=False)
    assert open_threads == ["maru-worker-heartbeat"]


def test_only_foreground_calls_use_the_lookup_timeout():
    calls = []

    def call(code, request, timeout_s=None):
        calls.append((code, conn._rpc.timeout_s if timeout_s is None else timeout_s))
        if code == MessageType.WORKER_OPEN:
            return opened("s1", "old")
        if code == MessageType.PROBE:
            return ProbeResponse(status=Status.OK, items=[ItemOutcome(ItemResult.MISS)])
        if code == MessageType.LOOKUP_AND_PIN:
            if timeout_s is not None:
                raise RpcUnknownError("slow lookup")
            return LookupAndPinResponse(
                status=Status.OK, items=[ItemOutcome(ItemResult.HIT, pin_id="pin")]
            )
        if code == MessageType.WORKER_HEARTBEAT:
            return WorkerHeartbeatResponse(status=Status.OK)
        if code == MessageType.UNPIN:
            return UnpinResponse(status=Status.OK, items=[ItemOutcome(ItemResult.OK)])
        assert code == MessageType.WORKER_CLOSE
        return WorkerCloseResponse(status=Status.OK)

    conn = make_conn(call)
    try:
        conn.open()
        assert conn.probe(KEYS) is not None
        assert conn.lookup_and_pin(KEYS) is None
        assert conn.down and conn.unresolved_lookups() == 1
        assert conn.heartbeat_once()
        conn._resolve_lookups()
        assert conn.late_hits == 1 and conn.unresolved_lookups() == 0
        assert conn._flush_unpins()
        assert conn.close()
    finally:
        conn._rpc.close()
    assert calls == [
        (MessageType.WORKER_OPEN, 2.0),
        (MessageType.PROBE, 0.1),
        (MessageType.LOOKUP_AND_PIN, 0.1),
        (MessageType.WORKER_HEARTBEAT, 2.0),
        (MessageType.LOOKUP_AND_PIN, 2.0),
        (MessageType.UNPIN, 2.0),
        (MessageType.WORKER_CLOSE, 2.0),
    ]


def test_a_late_or_malformed_lookup_gives_back_its_plans_source_pins():
    from maru_common.storage_types import FetchPlan, Location, PoolKind

    plan = FetchPlan(
        Location("cxl:memory:1", "a", 1, 0, 8, PoolKind.CXL), "n", 8, "src"
    )
    answers = []
    sent = []

    def call(code, request, timeout_s=None):
        sent.append(request)
        if code == MessageType.LOOKUP_AND_PIN:
            if timeout_s is not None and not answers:
                raise RpcUnknownError("slow lookup")
            return answers.pop(0)
        assert code == MessageType.WORKER_HEARTBEAT
        return WorkerHeartbeatResponse(status=Status.OK)

    conn = make_conn(call)
    try:
        # late: the resolver unpins its HIT and its plan's source pin
        assert conn.lookup_and_pin(KEYS * 2, pin_sources=True) is None
        assert sent[-1].pin_sources is True
        answers.append(
            LookupAndPinResponse(
                status=Status.OK,
                items=[
                    ItemOutcome(ItemResult.HIT, pin_id="hit"),
                    ItemOutcome(ItemResult.FETCH, plan=plan),
                ],
            )
        )
        assert conn.heartbeat_once()
        conn._resolve_lookups()
        assert conn.late_hits == 1 and conn._unpins == ["hit", "src"]
        # malformed: one result for two keys
        conn._unpins.clear()
        answers.append(
            LookupAndPinResponse(
                status=Status.OK, items=[ItemOutcome(ItemResult.FETCH, plan=plan)]
            )
        )
        assert conn.lookup_and_pin(KEYS * 2, pin_sources=True) is None
        assert conn._unpins == ["src"]
    finally:
        conn._rpc.close()
