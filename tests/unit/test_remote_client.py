# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""The thin client's mutual proof, seq matching and connection failure callbacks."""

from __future__ import annotations

import dataclasses
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest

from maru_common.node_protocol import recv_frame, send_frame
from maru_common.remote_protocol import (
    REMOTE_MESSAGE_CLASSES,
    AgentMetadata,
    AgentsRequest,
    PingRequest,
)
from maru_common.remote_protocol import RemoteMessageType as Code
from maru_common.serializer import Serializer, op_digest
from maru_common.storage_types import Status
from maru_remote.auth import NONCE_BYTES, prove
from maru_remote.framing import recv_frame as recv_proof
from maru_remote.framing import send_frame as send_proof
from maru_storage_service import remote_client
from maru_storage_service.remote_client import RemoteConnection
from tests.unit.test_remote_server import (
    KEY,
    ORIGIN,
    WAIT,
    Answer,
    hello,
    maru_cycles,
    ping,
    request,
    submit,
)
from tests.unit.test_remote_server import make_server as make_server


@contextmanager
def scripted_server(script):
    listener = socket.create_server(("127.0.0.1", 0))
    listener.settimeout(WAIT)
    failures = []

    def run():
        try:
            with listener.accept()[0] as sock:
                sock.settimeout(WAIT)
                script(sock)
        except BaseException as exc:
            failures.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        yield listener.getsockname()[:2]
    finally:
        listener.close()
        thread.join(WAIT)
        assert not thread.is_alive()
        if failures:
            raise failures[0]


def authenticate(sock):
    nonce = b"s" * NONCE_BYTES
    send_proof(sock, {"nonce": nonce})
    auth = recv_proof(sock)
    assert auth is not None
    assert auth["proof"] == prove(KEY, "handler", nonce)
    send_proof(sock, {"ok": True, "proof": prove(KEY, "agent", auth["nonce"])})
    ser = Serializer(REMOTE_MESSAGE_CLASSES)
    frame = recv_frame(sock)
    assert frame is not None
    header, _ = ser.decode_request(frame[0])
    return ser, header


def test_client_verifies_the_agent_before_sending_hello():
    def script(sock):
        send_proof(sock, {"nonce": b"s" * NONCE_BYTES})
        auth = recv_proof(sock)
        assert auth is not None
        send_proof(
            sock, {"ok": True, "proof": prove(b"wrong key", "agent", auth["nonce"])}
        )
        assert sock.recv(1) == b""

    with scripted_server(script) as address:
        with pytest.raises(ConnectionError, match="proof"):
            RemoteConnection.open(address, KEY, hello(), WAIT)


@pytest.mark.parametrize("challenge", [{}, {"nonce": "string"}, {"nonce": b"short"}])
def test_client_rejects_invalid_challenge(challenge):
    def script(sock):
        send_proof(sock, challenge)
        assert sock.recv(1) == b""

    with scripted_server(script) as address:
        with pytest.raises(ConnectionError, match="challenge"):
            RemoteConnection.open(address, KEY, hello(), WAIT)


@pytest.mark.parametrize("wrong", ["sequence", "code"])
def test_open_checks_the_hello_header(wrong):
    def script(sock):
        ser, header = authenticate(sock)
        if wrong == "sequence":
            header.sequence += 1
        else:
            header.msg_type = Code.PING
        send_frame(sock, ser.encode_response(header, {"status": Status.OK}))
        assert sock.recv(1) == b""

    with scripted_server(script) as address:
        with pytest.raises(ConnectionError, match="HELLO"):
            RemoteConnection.open(address, KEY, hello(), WAIT)


def test_closed_connection_answers_submit_once_and_call_raises(make_server):
    h = make_server()
    conn, _ = h.open()
    conn.close()
    conn.close()
    answer = submit(conn, Code.PING, PingRequest())
    assert isinstance(answer.get(), ConnectionError)
    with pytest.raises(ConnectionError):
        ping(conn)
    assert not conn._reader.is_alive()


def test_a_call_the_connection_fails_leaves_no_cycle(make_server):
    """The error call() raises holds its frame: no cycle through it."""
    h = make_server()
    h.ops.hold.add(Code.RESERVE)
    conn, _ = h.open()

    def run():
        failed = []

        def call():
            try:
                conn.call(Code.RESERVE, request(Code.RESERVE, 1), WAIT)
            except ConnectionError:
                failed.append(True)

        caller = threading.Thread(target=call)
        caller.start()
        h.ops.wait_calls(Code.RESERVE, 1)
        with h.server._cv:
            next(iter(h.server._connections)).close()
        caller.join(WAIT)
        assert failed == [True]

    assert maru_cycles(run) == []
    h.ops.finish(ORIGIN, 1)


def test_broken_connection_fails_all_pending_callbacks_once(make_server):
    h = make_server()
    h.ops.hold.add(Code.RESERVE)
    conn, _ = h.open()
    answers = [submit(conn, Code.RESERVE, request(Code.RESERVE, i)) for i in (1, 2, 3)]
    h.ops.wait_calls(Code.RESERVE, 3)
    with h.server._cv:
        server_conn = next(iter(h.server._connections))
    server_conn.close()
    assert all(isinstance(a.get(), ConnectionError) for a in answers)
    for i in (1, 2, 3):
        h.ops.finish(ORIGIN, i)
    assert all(len(a.values) == 1 for a in answers)


def test_concurrent_calls_match_their_own_sequences(make_server):
    h = make_server()
    conn, _ = h.open()

    def call(i):
        req = request(Code.RESERVE, i)
        req.pages = i
        req.op = dataclasses.replace(req.op, digest=op_digest(req))
        return conn.call(Code.RESERVE, req, WAIT).room

    with ThreadPoolExecutor(max_workers=8) as pool:
        assert list(pool.map(call, range(1, 25))) == list(range(1, 25))
    assert len(h.ops.calls) == 24


def test_bad_callback_does_not_break_the_reader(make_server):
    h = make_server()
    conn, _ = h.open()
    called = threading.Event()

    def bad_callback(response):
        called.set()
        raise ValueError("callback failure")

    conn.submit(Code.PING, PingRequest(), bad_callback)
    assert called.wait(WAIT)
    assert ping(conn).status == Status.OK


def test_call_timeout_discards_only_its_pending_response(make_server):
    h = make_server()
    h.ops.hold.add(Code.RESERVE)
    conn, _ = h.open()
    with pytest.raises(TimeoutError):
        conn.call(Code.RESERVE, request(Code.RESERVE), 0.5)
    h.ops.wait_calls(Code.RESERVE)
    assert not conn._pending
    h.ops.finish(ORIGIN, 1)
    assert ping(conn).status == Status.OK
    assert conn.call(Code.RESERVE, request(Code.RESERVE), WAIT).status == Status.OK
    assert len(h.ops.calls) == 1


@pytest.mark.parametrize("block", ["send_buffer", "send_lock"])
def test_send_deadline_closes_the_stream(block):
    sock, peer = socket.socketpair()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
    conn = RemoteConnection(sock, Serializer(REMOTE_MESSAGE_CLASSES))
    closed = threading.Event()
    conn.on_close(closed.set)
    payload = b"x" * (1 << 20)
    request = AgentsRequest(add=[AgentMetadata("worker", payload)])
    errors = []

    def call():
        try:
            conn.call(Code.AGENTS, request, 0.5)
        except OSError as exc:
            errors.append(exc)

    if block == "send_lock":
        conn._send_lock.acquire()
    caller = threading.Thread(target=call)
    try:
        caller.start()
        caller.join(2)
        assert not caller.is_alive()
        assert len(errors) == 1 and isinstance(errors[0], TimeoutError)
        assert closed.wait(WAIT) and not conn._pending
        peer.settimeout(WAIT)
        received = b"".join(iter(lambda: peer.recv(65536), b""))
        if block == "send_lock":
            assert not received
        else:
            assert 0 < len(received) < len(payload)
    finally:
        if block == "send_lock":
            conn._send_lock.release()
        peer.close()
        conn.close()
        caller.join(WAIT)


@pytest.mark.parametrize("bad", ["code", "payload", "truncated"])
def test_bad_response_fails_pending_callback(bad):
    def script(sock):
        ser, header = authenticate(sock)
        send_frame(sock, ser.encode_response(header, {"status": Status.OK}))
        frame = recv_frame(sock)
        assert frame is not None
        header, _ = ser.decode_request(frame[0])
        if bad == "code":
            header.msg_type = Code.RESERVE
        payload = {"status": "invalid" if bad == "payload" else Status.OK}
        encoded = ser.encode_response(header, payload)
        if bad == "truncated":
            sock.sendall(encoded[:-1])
            return
        send_frame(sock, encoded)
        assert sock.recv(1) == b""

    with scripted_server(script) as address:
        conn, response = RemoteConnection.open(address, KEY, hello(), WAIT)
        try:
            assert response.status == Status.OK
            assert isinstance(
                submit(conn, Code.PING, PingRequest()).get(), ConnectionError
            )
        finally:
            conn.close()


def test_send_failure_closes_and_fails_the_registered_callback(
    make_server, monkeypatch
):
    h = make_server()
    conn, _ = h.open()

    def fail(*args):
        raise OSError("injected send failure")

    monkeypatch.setattr(remote_client, "send_frame", fail)
    answer = submit(conn, Code.PING, PingRequest())
    assert isinstance(answer.get(), ConnectionError)
    assert conn.closed


def test_close_racing_a_failed_send_calls_callback_once(make_server, monkeypatch):
    h = make_server()
    conn, _ = h.open()
    sending = threading.Event()
    release = threading.Event()

    def fail(*args):
        sending.set()
        assert release.wait(WAIT)
        raise OSError("closed during send")

    monkeypatch.setattr(remote_client, "send_frame", fail)
    answer = Answer()
    thread = threading.Thread(
        target=conn.submit, args=(Code.PING, PingRequest(), answer), daemon=True
    )
    thread.start()
    try:
        assert sending.wait(WAIT)
        conn.close()
        assert isinstance(answer.get(), ConnectionError)
    finally:
        release.set()
        thread.join(WAIT)
        assert not thread.is_alive()
    assert len(answer.values) == 1


def test_a_failed_send_is_answered_on_the_reader_after_the_close_hooks(
    make_server, monkeypatch
):
    h = make_server()
    conn, _ = h.open()
    seen = []
    conn.on_close(lambda: seen.append(("hook", threading.current_thread())))

    def fail(*args):
        raise OSError("injected send failure")

    monkeypatch.setattr(remote_client, "send_frame", fail)
    answer = Answer()

    def callback(response):
        seen.append(("callback", threading.current_thread()))
        answer(response)

    conn.submit(Code.PING, PingRequest(), callback)
    assert isinstance(answer.get(), ConnectionError)
    # Not on the submitting thread, which may hold locks the callbacks take
    assert [what for what, _ in seen] == ["hook", "callback"]
    assert all(thread is conn._reader for _, thread in seen)


def test_on_close_after_the_end_runs_at_once(make_server):
    h = make_server()
    conn, _ = h.open()
    conn.close()
    ran = []
    conn.on_close(lambda: ran.append(threading.current_thread()))
    assert ran == [threading.current_thread()]
