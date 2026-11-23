# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""TCP authentication, run lifetimes and operation replay."""

from __future__ import annotations

import dataclasses
import gc
import secrets
import socket
import threading
import time
import types
from collections.abc import Callable
from typing import Any
from unittest.mock import patch

import pytest

from maru_common.node_protocol import recv_frame, send_frame
from maru_common.remote_protocol import (
    LEDGER_CODES,
    REMOTE_MESSAGE_CLASSES,
    REMOTE_WIRE_VERSION,
    AgentsResponse,
    CancelRequest,
    CancelResponse,
    FenceDomainRequest,
    FenceDomainResponse,
    FenceResult,
    GoodbyeRequest,
    HelloRefusal,
    HelloRequest,
    PingRequest,
    RemoteHintRequest,
    RemoteHintResponse,
    RemotePool,
    RemoteWriteDoneRequest,
    RemoteWriteDoneResponse,
    ReserveRequest,
    ReserveResponse,
    RoomLimit,
    TransferEnd,
    Written,
    peer_origin,
)
from maru_common.remote_protocol import (
    RemoteMessageType as Code,
)
from maru_common.serializer import Serializer, op_digest
from maru_common.storage_types import (
    MEMORY_CAPABILITY,
    REMOTE_CAPABILITY,
    WORKER_RDMA_CAPABILITY,
    HintAction,
    ItemOutcome,
    ItemResult,
    Key,
    Location,
    OpRef,
    PoolKind,
    Status,
    WriteDomain,
)
from maru_remote.auth import NONCE_BYTES, prove
from maru_remote.framing import recv_frame as recv_proof
from maru_remote.framing import send_frame as send_proof
from maru_remote.progress import ConnectProgress
from maru_storage_service import remote_server
from maru_storage_service.op_ledger import WAITERS_PER_CONNECTION
from maru_storage_service.remote_client import RemoteConnection
from maru_storage_service.remote_server import TcpRemoteServer
from tests.unit.remote_transport_fakes import FakeTransport

KEY = b"test key with at least sixteen bytes"
ORIGIN = peer_origin("gpu", "e1")
POOL = RemotePool("dram:memory:e1", PoolKind.DRAM, 1, 65536, 0x123000, b"secret-md")
LOCATION = Location("dram:memory:e1", "slot", 7, 0, 4096, PoolKind.DRAM)
KEY_RECORD = Key("namespace", "key")
WAIT = 3.0


def hello(**changes) -> HelloRequest:
    req = HelloRequest(
        REMOTE_WIRE_VERSION,
        "gpu",
        "e1",
        "boot",
        123,
        "catalog",
        "gpu-agent",
        b"gpu-md",
        4096,
        1,
        [REMOTE_CAPABILITY, WORKER_RDMA_CAPABILITY],
    )
    return dataclasses.replace(req, **changes)


def request(code: Code, seq: int = 1, *, origin: str = ORIGIN, ack: int = 0):
    op = OpRef(origin, seq, b"", ack)
    req: Any
    if code == Code.RESERVE:
        req = ReserveRequest(op, 2)
    elif code == Code.WRITE_DONE:
        req = RemoteWriteDoneRequest(
            op, [Written(KEY_RECORD, LOCATION, 100, TransferEnd.DONE)]
        )
    elif code == Code.CANCEL:
        req = CancelRequest(op, reserve_seq=1)
    else:
        req = FenceDomainRequest(op, WriteDomain("gpu", "e1", "instance", 1))
    return dataclasses.replace(req, op=dataclasses.replace(op, digest=op_digest(req)))


class Answer:
    def __init__(self) -> None:
        self.event = threading.Event()
        self.values: list[Any] = []

    def __call__(self, value):
        self.values.append(value)
        self.event.set()

    def get(self):
        assert self.event.wait(WAIT), "response did not arrive"
        assert len(self.values) == 1
        return self.values[0]


class FakeOps:
    def __init__(self) -> None:
        self.cv = threading.Condition()
        self.calls: list[tuple[Code, str, Any, float | None]] = []
        self.pending: dict[tuple[str, int], tuple[Callable, Any]] = {}
        self.hold: set[Code] = set()
        self.lost: list[str] = []
        self.retired: list[str] = []
        self.received: list[str] = []
        self.available: tuple[int, RoomLimit | None] = (32768, None)

    def _run(self, code, origin, req, done, response, deadline=None):
        with self.cv:
            self.calls.append((code, origin, req, deadline))
            self.pending[origin, req.op.seq] = (done, response)
            self.cv.notify_all()
        if code not in self.hold:
            self.finish(origin, req.op.seq)

    def reserve(self, origin, req, done):
        self._run(
            Code.RESERVE, origin, req, done, ReserveResponse(Status.OK, room=req.pages)
        )

    def write_done(self, origin, req, done):
        self._run(
            Code.WRITE_DONE,
            origin,
            req,
            done,
            RemoteWriteDoneResponse(Status.OK, items=[ItemOutcome(ItemResult.ADDED)]),
        )

    def cancel(self, origin, req, done):
        self._run(Code.CANCEL, origin, req, done, CancelResponse(Status.OK, freed=1))

    def fence_domain(self, origin, req, done):
        self._run(
            Code.FENCE_DOMAIN,
            origin,
            req,
            done,
            FenceDomainResponse(Status.OK, result=FenceResult.SUCCESS),
        )

    def agents(self, origin, req, done):
        done(AgentsResponse(Status.OK))

    def hint(self, origin, req, done):
        done(RemoteHintResponse(Status.REJECTED))

    def room(self, origin) -> tuple[int, RoomLimit | None]:
        return self.available

    def peer_received(self, origin):
        self.received.append(origin)

    def peer_lost(self, origin):
        with self.cv:
            self.lost.append(origin)
            self.cv.notify_all()

    def peer_retired(self, origin):
        with self.cv:
            self.retired.append(origin)
            self.cv.notify_all()

    def finish(self, origin, seq):
        with self.cv:
            done, response = self.pending.pop((origin, seq))
        done(response)

    def wait_calls(self, code, count=1):
        with self.cv:
            assert self.cv.wait_for(
                lambda: sum(c[0] == code for c in self.calls) >= count, WAIT
            ), "operation was not dispatched"
            return [c for c in self.calls if c[0] == code]

    def wait_lost(self, count=1):
        with self.cv:
            assert self.cv.wait_for(lambda: len(self.lost) >= count, WAIT)


class Harness:
    def __init__(self, *, ready=True, **options) -> None:
        self.ops = FakeOps()
        self.transport = FakeTransport(f"remote-test-{secrets.token_hex(4)}")
        self.progress = ConnectProgress(self.transport)
        self._drain_patch = patch.object(
            self.progress, "drain", wraps=self.progress.drain
        )
        self._drain_patch.start()
        self.server = TcpRemoteServer(
            "127.0.0.1:0",
            KEY,
            node_id="memory",
            node_epoch="m1",
            capabilities=[MEMORY_CAPABILITY],
            page_size=4096,
            agent_name=self.transport.agent.name,
            progress=self.progress,
            ops=self.ops,
            **options,
        )
        self.server.start()
        if ready:
            self.server.ready("catalog", [POOL])
        self.clients: list[RemoteConnection] = []
        self.sockets: list[socket.socket] = []

    def open(self, req=None, *, key=KEY):
        assert self.server.address is not None
        conn, response = RemoteConnection.open(
            self.server.address, key, hello() if req is None else req, WAIT
        )
        self.clients.append(conn)
        return conn, response

    def raw(self, *, authenticate=True):
        assert self.server.address is not None
        sock = socket.create_connection(self.server.address, WAIT)
        self.sockets.append(sock)
        challenge = recv_proof(sock)
        assert challenge is not None and set(challenge) == {"nonce"}
        assert len(challenge["nonce"]) == NONCE_BYTES
        if authenticate:
            nonce = b"n" * NONCE_BYTES
            send_proof(
                sock,
                {
                    "op": "auth",
                    "nonce": nonce,
                    "proof": prove(KEY, "handler", challenge["nonce"]),
                },
            )
            assert recv_proof(sock) == {"ok": True, "proof": prove(KEY, "agent", nonce)}
        return sock

    def close(self):
        for client in self.clients:
            client.close()
        for sock in self.sockets:
            sock.close()
        self.server.stop()
        self.transport.close()
        self._drain_patch.stop()


@pytest.fixture
def make_server():
    servers = []

    def make(**options):
        server = Harness(**options)
        servers.append(server)
        return server

    yield make
    for server in servers:
        server.close()


def submit(conn, code, req) -> Answer:
    answer = Answer()
    conn.submit(code, req, answer)
    return answer


def ping(conn):
    return conn.call(Code.PING, PingRequest(), WAIT)


def test_ready_and_registered_metadata_and_progress(make_server):
    h = make_server(ready=False)
    _, refused = h.open()
    assert refused.status == Status.BUSY and refused.refusal == HelloRefusal.NOT_READY
    assert refused.pools == [] and refused.agent_name == "" and refused.node_id == ""
    h.server.ready("catalog", [POOL])
    h.ops.available = (0, RoomLimit.QUARANTINE)
    conn, accepted = h.open()
    assert accepted.status == Status.OK
    assert h.ops.received == [ORIGIN]
    assert (accepted.node_id, accepted.node_epoch, accepted.server_epoch) == (
        "memory",
        "m1",
        "catalog",
    )
    assert accepted.pools == [POOL]
    assert accepted.capabilities == [MEMORY_CAPABILITY]
    assert accepted.agent_name == h.transport.agent.name
    assert (accepted.room, accepted.limit) == h.ops.available
    h.progress.drain.assert_called()
    assert h.progress.expecting() == set()
    h.progress.drain.reset_mock()
    assert (ping(conn).room, ping(conn).limit) == h.ops.available
    assert h.ops.received == [ORIGIN] * 3
    h.progress.drain.assert_called()


@pytest.mark.parametrize("fails", [False, True])
def test_slow_hints_keep_a_bounded_ordered_queue(make_server, monkeypatch, fails):
    monkeypatch.setattr(remote_server, "_MAX_PENDING_HINTS", 1)
    entered, release = threading.Event(), threading.Event()
    seen = []

    def hint(conn_id, req, done, is_closed):
        seen.append(req.request)
        if req.request == "active":
            entered.set()
            assert release.wait(WAIT)
            if fails:
                raise RuntimeError("hint failed")
        done(RemoteHintResponse(Status.OK))

    h = make_server(extension_hint=hint)
    conn, _ = h.open()
    try:
        active = submit(
            conn, Code.HINT, RemoteHintRequest(HintAction.READ_HINT, "active")
        )
        assert entered.wait(WAIT)
        queued = submit(
            conn, Code.HINT, RemoteHintRequest(HintAction.READ_HINT_END, "queued")
        )
        overflow = submit(
            conn, Code.HINT, RemoteHintRequest(HintAction.READ_PROMOTE, "overflow")
        )
        assert overflow.get().status == Status.BUSY
        assert ping(conn).status == Status.OK
        assert h.ops.received == [ORIGIN] * 5
        assert not active.event.is_set() and not queued.event.is_set()
    finally:
        release.set()
    assert active.get().status == (Status.REJECTED if fails else Status.OK)
    assert queued.get().status == Status.OK
    assert seen == ["active", "queued"]


def test_connection_close_discards_queued_hints(make_server):
    entered, release = threading.Event(), threading.Event()
    seen = []

    def hint(conn_id, req, done, is_closed):
        seen.append(req.request)
        entered.set()
        assert release.wait(WAIT)
        done(RemoteHintResponse(Status.OK))

    h = make_server(extension_hint=hint)
    conn, _ = h.open()
    with h.server._cv:
        server_conn = next(iter(h.server._connections))
    try:
        active = submit(
            conn, Code.HINT, RemoteHintRequest(HintAction.READ_HINT, "active")
        )
        assert entered.wait(WAIT)
        queued = submit(
            conn, Code.HINT, RemoteHintRequest(HintAction.READ_HINT_END, "queued")
        )
        assert ping(conn).status == Status.OK
        conn.close()
        assert server_conn.closed.wait(WAIT)
        assert isinstance(active.get(), ConnectionError)
        assert isinstance(queued.get(), ConnectionError)
    finally:
        release.set()
        server_conn.join(time.monotonic() + WAIT)
    assert not server_conn._hint_thread.is_alive()
    assert seen == ["active"]


@pytest.mark.parametrize(
    "changes,ready,refusal,status",
    [
        (
            {"version": 99, "server_epoch": "bad", "page_size": 1},
            False,
            HelloRefusal.VERSION,
            Status.REJECTED,
        ),
        (
            {"server_epoch": "bad", "page_size": 1},
            False,
            HelloRefusal.NOT_READY,
            Status.BUSY,
        ),
        (
            {"server_epoch": "bad", "page_size": 1, "capabilities": []},
            True,
            HelloRefusal.SERVER_EPOCH,
            Status.BUSY,
        ),
        (
            {"page_size": 1, "capabilities": []},
            True,
            HelloRefusal.PAGE_SIZE,
            Status.REJECTED,
        ),
        ({"capabilities": []}, True, None, Status.REJECTED),
    ],
)
def test_hello_refusals_in_order(make_server, changes, ready, refusal, status):
    h = make_server(ready=ready)
    conn, response = h.open(hello(**changes))
    assert response.status == status and response.refusal == refusal
    assert response.pools == [] and response.agent_name == ""
    assert response.node_id == response.node_epoch == response.server_epoch == ""
    assert conn._closed.wait(WAIT)
    assert not h.ops.calls and not h.ops.lost and not h.ops.retired


def test_wrong_key_and_no_metadata_before_proof(make_server):
    h = make_server()
    with pytest.raises(ConnectionError, match="proof"):
        h.open(key=b"wrong key")
    sock = h.raw(authenticate=False)
    sock.settimeout(0.03)
    with pytest.raises(TimeoutError):
        sock.recv(1)
    sock.settimeout(WAIT)
    send_proof(sock, {"op": "hello"})
    assert sock.recv(1) == b""
    assert h.server.ledger.origins() == 0


def test_serializer_request_cannot_skip_proof(make_server):
    h = make_server()
    sock = h.raw(authenticate=False)
    send_frame(sock, Serializer(REMOTE_MESSAGE_CLASSES).encode(Code.HELLO, hello()))
    assert sock.recv(1) == b""
    assert h.server.ledger.origins() == 0


def test_proof_timeout_cuts_partial_frames(make_server, monkeypatch):
    monkeypatch.setattr(remote_server, "AUTH_TIMEOUT_S", 0.05)
    h = make_server()
    sock = h.raw(authenticate=False)
    sock.sendall(b"\0")
    assert sock.recv(1) == b""
    assert h.server.ledger.origins() == 0


@pytest.mark.parametrize("nonce", [b"", b"short", "not bytes"])
def test_invalid_proof_nonce(make_server, nonce):
    h = make_server()
    sock = h.raw(authenticate=False)
    send_proof(sock, {"op": "auth", "nonce": nonce, "proof": b"x"})
    assert sock.recv(1) == b""


def test_connection_limits_count_unproven_per_source(make_server, monkeypatch):
    monkeypatch.setattr(remote_server, "MAX_PER_SOURCE", 1)
    h = make_server()
    first = h.raw(authenticate=False)
    assert h.server.address is not None
    with socket.create_connection(h.server.address, WAIT) as rejected:
        assert rejected.recv(1) == b""
    first.close()
    # Wait for the closed socket to leave the accept table.
    with h.server._cv:
        remaining = list(h.server._connections)
    for conn in remaining:
        conn.join(time.monotonic() + WAIT)
    proven, response = h.open()
    assert response.status == Status.OK
    second = h.raw(authenticate=False)
    assert ping(proven).status == Status.OK
    second.close()


def test_total_connection_limit(make_server, monkeypatch):
    monkeypatch.setattr(remote_server, "MAX_CONNECTIONS", 1)
    h = make_server()
    conn, _ = h.open()
    assert h.server.address is not None
    with socket.create_connection(h.server.address, WAIT) as rejected:
        assert rejected.recv(1) == b""
    assert ping(conn).status == Status.OK


def maru_cycles(run) -> list[str]:
    """Maru objects and functions ``run`` left in reference cycles: a
    process that freezes what survives a collection would never free them."""
    gc.collect()
    before = len(gc.garbage)
    gc.set_debug(gc.DEBUG_SAVEALL)
    try:
        run()
        gc.collect()
        names = []
        for obj in gc.garbage[before:]:
            if isinstance(obj, types.MethodType):
                obj = obj.__func__
            if isinstance(obj, types.FunctionType):
                name = f"{obj.__module__}.{obj.__qualname__}"
            else:
                name = f"{type(obj).__module__}.{type(obj).__qualname__}"
            if name.startswith("maru"):
                names.append(name)
        return names
    finally:
        gc.set_debug(0)
        del gc.garbage[before:]


def test_a_connection_that_never_proves_itself_leaves_no_cycle(make_server):
    h = make_server()

    def unproven():
        sock = h.raw(authenticate=False)
        with h.server._cv:
            conn = next(iter(h.server._connections))
        sock.close()
        conn.join(time.monotonic() + WAIT)
        assert conn.closed.is_set()

    def run():
        unproven()
        # The accept loop holds the last connection it made: one more
        unproven()

    assert maru_cycles(run) == []


def test_keepalive_settings(make_server):
    h = make_server()
    h.open()
    with h.server._cv:
        conn = next(iter(h.server._connections))
        assert conn.sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 1
        for name, value in (
            ("TCP_KEEPIDLE", 10),
            ("TCP_KEEPINTVL", 5),
            ("TCP_KEEPCNT", 3),
        ):
            if hasattr(socket, name):
                assert (
                    conn.sock.getsockopt(socket.IPPROTO_TCP, getattr(socket, name))
                    == value
                )


@pytest.mark.parametrize("code", [Code.PING, *sorted(LEDGER_CODES)])
def test_hello_must_be_first(make_server, code):
    h = make_server()
    sock = h.raw()
    ser = Serializer(REMOTE_MESSAGE_CLASSES)
    req = PingRequest() if code == Code.PING else request(code)
    send_frame(sock, ser.encode(code, req, seq=17))
    frame = recv_frame(sock)
    assert frame is not None
    header, response = ser.decode_response(frame[0])
    assert header.sequence == 17 and response.status == Status.REJECTED
    assert sock.recv(1) == b""
    assert not h.ops.calls
    assert not h.ops.received


def test_same_epoch_reconnect_keeps_ledger_and_drops_old_waiters(make_server):
    h = make_server()
    h.ops.hold.add(Code.RESERVE)
    old, _ = h.open()
    req = request(Code.RESERVE)
    waiting = submit(old, Code.RESERVE, req)
    h.ops.wait_calls(Code.RESERVE)
    new, _ = h.open(hello(conn_gen=2))
    assert isinstance(waiting.get(), ConnectionError)
    assert old._closed.wait(WAIT)
    assert not h.ops.lost and not h.ops.retired
    replay = submit(new, Code.RESERVE, req)
    ping(new)
    assert h.server.ledger.waiters(ORIGIN) == 1
    h.ops.finish(ORIGIN, 1)
    assert replay.get().status == Status.OK
    assert new.call(Code.RESERVE, req, WAIT) == replay.get()
    assert len(h.ops.calls) == 1
    new.close()
    h.ops.wait_lost()
    assert h.ops.lost == [ORIGIN]


def test_new_epoch_retires_once_and_never_returns(make_server):
    h = make_server()
    h.ops.hold.add(Code.RESERVE)
    old, _ = h.open()
    pending = submit(old, Code.RESERVE, request(Code.RESERVE))
    h.ops.wait_calls(Code.RESERVE)
    new, accepted = h.open(hello(node_epoch="e2"))
    assert accepted.status == Status.OK
    assert isinstance(pending.get(), ConnectionError)
    assert old._closed.wait(WAIT)
    assert h.ops.retired == [ORIGIN] and not h.ops.lost
    h.ops.finish(ORIGIN, 1)
    assert h.server.ledger.origins() == 1
    for changes, refusal in (
        ({"page_size": 1, "capabilities": []}, HelloRefusal.PAGE_SIZE),
        ({"capabilities": []}, HelloRefusal.RETIRED),
        ({}, HelloRefusal.RETIRED),
    ):
        _, response = h.open(hello(**changes))
        assert response.status == Status.REJECTED and response.refusal == refusal
    assert ping(new).status == Status.OK
    assert h.ops.retired == [ORIGIN]


def test_disconnected_old_epoch_is_retired_on_new_epoch(make_server):
    h = make_server()
    old, _ = h.open()
    old.close()
    h.ops.wait_lost()
    h.open(hello(node_epoch="e2"))
    assert h.ops.retired == [ORIGIN]
    _, refused = h.open()
    assert refused.refusal == HelloRefusal.RETIRED


def test_hello_ack_applies_without_creating_an_op(make_server):
    h = make_server()
    old, _ = h.open()
    req = request(Code.RESERVE)
    assert old.call(Code.RESERVE, req, WAIT).status == Status.OK
    assert h.server.ledger.size(ORIGIN) == 1
    new, _ = h.open(hello(conn_gen=2, ack=1))
    assert h.server.ledger.size(ORIGIN) == 0
    response = new.call(Code.RESERVE, req, WAIT)
    assert response.status == Status.REJECTED and response.error == "stale op"
    assert len(h.ops.calls) == 1


def test_goodbye_retires_and_closes(make_server):
    h = make_server()
    conn, _ = h.open()
    response = conn.call(Code.GOODBYE, GoodbyeRequest(), WAIT)
    assert response.status == Status.OK
    assert conn._closed.wait(WAIT)
    assert h.ops.retired == [ORIGIN] and not h.ops.lost
    assert h.server.ledger.origins() == 0
    _, response = h.open()
    assert response.refusal == HelloRefusal.RETIRED
    assert h.ops.retired == [ORIGIN]


@pytest.mark.parametrize("code", sorted(LEDGER_CODES))
@pytest.mark.parametrize("bad", ["origin", "digest", "unknown-field"])
def test_invalid_op_is_rejected_without_a_ledger_entry(make_server, code, bad):
    h = make_server()
    conn, _ = h.open()
    req = request(code)
    if bad == "origin":
        req = dataclasses.replace(req, op=dataclasses.replace(req.op, origin="other"))
    elif bad == "digest":
        req = dataclasses.replace(req, op=dataclasses.replace(req.op, digest=b"bad"))
    else:
        req = dataclasses.asdict(req)
        req["unknown"] = {"count": 1}
    assert conn.call(code, req, WAIT).status == Status.REJECTED
    assert h.server.ledger.size(ORIGIN) == 0 and not h.ops.calls


@pytest.mark.parametrize("code", sorted(LEDGER_CODES))
def test_stale_per_op(make_server, code):
    h = make_server()
    conn, _ = h.open()
    response = conn.call(code, request(code, ack=1), WAIT)
    if code == Code.WRITE_DONE:
        assert response.status == Status.OK
        assert response.items == [ItemOutcome(ItemResult.STALE)]
    else:
        assert response.status == Status.REJECTED and response.error == "stale op"
    assert not h.ops.calls and h.server.ledger.size(ORIGIN) == 0


@pytest.mark.parametrize("code", sorted(LEDGER_CODES))
def test_conflict_and_both_replay_states_per_op(make_server, code):
    h = make_server()
    h.ops.hold.add(code)
    conn, _ = h.open()
    req = request(code)
    first = submit(conn, code, req)
    h.ops.wait_calls(code)
    conflict = dataclasses.asdict(req)
    conflict["future"] = 1
    conflict["op"]["digest"] = op_digest(conflict)
    assert conn.call(code, conflict, WAIT).status == Status.REJECTED
    second = submit(conn, code, req)
    ping(conn)
    assert h.server.ledger.waiters(ORIGIN) == 2
    h.ops.finish(ORIGIN, 1)
    assert first.get() == second.get()
    assert conn.call(code, req, WAIT) == first.get()
    assert len(h.ops.calls) == 1


@pytest.mark.parametrize("code", sorted(LEDGER_CODES))
def test_busy_window_per_op_and_ack_releases_room(make_server, code):
    h = make_server(op_window=1)
    conn, _ = h.open()
    assert conn.call(code, request(code), WAIT).status == Status.OK
    assert conn.call(code, request(code, 2), WAIT).status == Status.BUSY
    assert conn.call(code, request(code, 2, ack=1), WAIT).status == Status.OK
    assert len(h.ops.calls) == 2
    assert h.server.ledger.size(ORIGIN) == 1


def test_raw_unknown_fields_with_valid_digest_are_accepted(make_server):
    h = make_server()
    conn, _ = h.open()
    payload = dataclasses.asdict(request(Code.RESERVE))
    payload["future"] = {"hello": b"bytes"}
    payload["op"]["digest"] = op_digest(payload)
    assert conn.call(Code.RESERVE, payload, WAIT).status == Status.OK
    assert conn.call(Code.RESERVE, payload, WAIT).status == Status.OK
    assert len(h.ops.calls) == 1


def test_waiter_bound_displaces_oldest_only(make_server):
    h = make_server()
    h.ops.hold.add(Code.RESERVE)
    conn, _ = h.open()
    req = request(Code.RESERVE)
    answers = [
        submit(conn, Code.RESERVE, req) for _ in range(WAITERS_PER_CONNECTION + 1)
    ]
    assert answers[0].get().status == Status.BUSY
    ping(conn)
    assert h.server.ledger.waiters(ORIGIN) == WAITERS_PER_CONNECTION
    h.ops.finish(ORIGIN, 1)
    assert all(a.get().status == Status.OK for a in answers[1:])
    assert len(h.ops.calls) == 1


def test_responses_can_finish_out_of_order(make_server):
    h = make_server()
    h.ops.hold.add(Code.RESERVE)
    conn, _ = h.open()
    first = submit(conn, Code.RESERVE, request(Code.RESERVE, 1))
    second = submit(conn, Code.RESERVE, request(Code.RESERVE, 2))
    h.ops.wait_calls(Code.RESERVE, 2)
    assert ping(conn).status == Status.OK
    assert (
        conn.call(Code.WRITE_DONE, request(Code.WRITE_DONE, 3), WAIT).status
        == Status.OK
    )
    h.ops.finish(ORIGIN, 2)
    assert second.get().status == Status.OK
    assert not first.event.is_set()
    h.ops.finish(ORIGIN, 1)
    assert first.get().status == Status.OK


def test_close_drops_waiters_but_records_completion_for_reconnect(make_server):
    h = make_server()
    h.ops.hold.add(Code.RESERVE)
    conn, _ = h.open()
    req = request(Code.RESERVE)
    pending = submit(conn, Code.RESERVE, req)
    h.ops.wait_calls(Code.RESERVE)
    conn.close()
    h.ops.wait_lost()
    assert isinstance(pending.get(), ConnectionError)
    assert h.server.ledger.waiters(ORIGIN) == 0
    h.ops.finish(ORIGIN, 1)
    new, _ = h.open(hello(conn_gen=2))
    assert new.call(Code.RESERVE, req, WAIT).status == Status.OK
    assert len(h.ops.calls) == 1


def test_address_in_use_raises_on_start(make_server):
    h = make_server()
    assert h.server.address is not None
    other = TcpRemoteServer(
        f"{h.server.address[0]}:{h.server.address[1]}",
        KEY,
        node_id="other",
        node_epoch="m2",
        capabilities=[],
        page_size=4096,
        agent_name="other-agent",
        progress=ConnectProgress(h.transport),
        ops=h.ops,
    )
    try:
        with pytest.raises(OSError):
            other.start()
    finally:
        other.stop()


def test_repeated_hello_is_refused_and_logged_without_replacing_peer(
    make_server, caplog
):
    caplog.set_level("INFO", logger=remote_server.__name__)
    h = make_server()
    conn, _ = h.open()
    response = conn.call(Code.HELLO, hello(node_epoch="e2"), WAIT)
    assert response.status == Status.REJECTED
    assert ping(conn).status == Status.OK
    assert not h.ops.lost and not h.ops.retired
    lines = [r.message for r in caplog.records if "HELLO" in r.message]
    assert len(lines) == 2 and "accepted" in lines[0] and "refused" in lines[1]


def test_bad_payload_is_rejected_with_its_sequence_and_connection_survives(make_server):
    h = make_server()
    conn, _ = h.open()
    payload = dataclasses.asdict(request(Code.RESERVE))
    payload["pages"] = "not an integer"
    assert conn.call(Code.RESERVE, payload, WAIT).status == Status.REJECTED
    assert ping(conn).status == Status.OK
    assert not h.ops.calls


def test_operation_exception_is_recorded_and_replayed(make_server, monkeypatch):
    h = make_server()
    conn, _ = h.open()
    calls = []

    def fail(origin, req, done):
        calls.append(req.op.seq)
        raise RuntimeError("test failure")

    monkeypatch.setattr(h.ops, "reserve", fail)
    req = request(Code.RESERVE)
    response = conn.call(Code.RESERVE, req, WAIT)
    assert response.status == Status.REJECTED
    assert conn.call(Code.RESERVE, req, WAIT) == response
    assert calls == [1]


def test_done_from_another_thread_can_finish_before_the_op_returns(
    make_server, monkeypatch
):
    h = make_server()
    conn, _ = h.open()
    completed = threading.Event()
    threads = []

    def reserve(origin, req, done):
        def finish():
            done(ReserveResponse(Status.OK, room=123))
            completed.set()

        thread = threading.Thread(target=finish, daemon=True)
        threads.append(thread)
        thread.start()
        assert completed.wait(WAIT)

    monkeypatch.setattr(h.ops, "reserve", reserve)
    response = conn.call(Code.RESERVE, request(Code.RESERVE), WAIT)
    assert response.status == Status.OK and response.room == 123
    assert completed.wait(WAIT)
    assert ping(conn).status == Status.OK
    for thread in threads:
        thread.join(WAIT)
        assert not thread.is_alive()


def test_a_legacy_remote_only_hello_is_refused(make_server):
    _, answer = make_server().open(hello(capabilities=[REMOTE_CAPABILITY]))
    assert answer.status == Status.REJECTED and not answer.pools


def test_stop_is_bounded_idempotent_and_drops_late_control_completion(
    make_server, monkeypatch
):
    monkeypatch.setattr(remote_server, "_JOIN_TIMEOUT_S", 0.1)
    h = make_server()
    h.ops.hold.add(Code.FENCE_DOMAIN)
    conn, _ = h.open()
    answer = submit(conn, Code.FENCE_DOMAIN, request(Code.FENCE_DOMAIN))
    h.ops.wait_calls(Code.FENCE_DOMAIN)
    started = time.monotonic()
    h.server.stop()
    h.server.stop()
    assert time.monotonic() - started < 1
    assert isinstance(answer.get(), ConnectionError)
    h.ops.finish(ORIGIN, 1)
    assert not h.server.ledger.finished(ORIGIN, 1)
