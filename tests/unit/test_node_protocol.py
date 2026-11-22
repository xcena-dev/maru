# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageService local protocol: messages, framing and descriptor passing."""

import dataclasses
import os
import shutil
import socket
import struct
import subprocess
import threading
import time

import pytest

from maru_common.node_protocol import (
    MAX_NODE_FDS,
    MAX_NODE_PAYLOAD,
    NODE_MESSAGE_CLASSES,
    NOTIFY_SEQUENCE,
    AttachResponse,
    FrameError,
    NodeMessageType,
    Notify,
    peer_credentials,
    process_start_time,
    recv_frame,
    same_pid_namespace,
    send_frame,
)
from maru_common.protocol import (
    HEADER_FORMAT,
    PROTOCOL_MAGIC,
    PROTOCOL_VERSION,
    MessageType,
    NodeOpenRequest,
)
from maru_common.serializer import RESPONSE_BIT, Serializer
from maru_common.storage_types import NotifyKind
from tests.unit.ms_samples import sample

CODES = sorted(NODE_MESSAGE_CLASSES)


@pytest.fixture
def pair():
    a, b = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    yield a, b
    a.close()
    b.close()


def test_codes_are_unique_and_below_the_response_bit():
    assert len(set(CODES)) == len(CODES) == 16
    assert all(code & RESPONSE_BIT == 0 for code in CODES)
    assert 0x28 not in CODES  # DRAIN_ACK, retired: never reused


@pytest.mark.parametrize("code", CODES, ids=lambda c: NodeMessageType(c).name)
def test_round_trip(code):
    request_cls, response_cls = NODE_MESSAGE_CLASSES[code]
    ser = Serializer(NODE_MESSAGE_CLASSES)
    request = sample(request_cls)
    header, decoded = ser.decode_request(ser.encode(code, request))
    assert decoded == request
    response = sample(response_cls)
    _, decoded = ser.decode_response(ser.encode_response(header, response))
    assert decoded == response


def test_fetch_contract_on_the_wire():
    from maru_common.node_protocol import Budgets, FetchResponse
    from maru_common.storage_types import (
        FETCH_CAPABILITY,
        ItemOutcome,
        ItemResult,
        MissReason,
        Status,
    )

    ser = Serializer(NODE_MESSAGE_CLASSES)
    assert FETCH_CAPABILITY == "fetch_v1"
    request = sample(NODE_MESSAGE_CLASSES[NodeMessageType.ATTACH][0])
    header, _ = ser.decode_request(ser.encode(NodeMessageType.ATTACH, request))
    # an older node's reply has no capabilities, and an older batch cap
    reply = dataclasses.asdict(sample(AttachResponse))
    reply["capabilities"] = [FETCH_CAPABILITY]
    _, back = ser.decode_response(ser.encode_response(header, reply))
    assert back.capabilities == [FETCH_CAPABILITY]
    del reply["capabilities"]
    reply["budgets"]["fetch_max_batch_bytes"] = 1
    _, back = ser.decode_response(ser.encode_response(header, reply))
    assert back.capabilities == []
    assert isinstance(back.budgets, Budgets)
    # FETCH items: MISS once taken (busy, timeout, io_error), MISS with no
    # reason for a cancel before the reply
    header, _ = ser.decode_request(
        ser.encode(
            NodeMessageType.FETCH,
            sample(NODE_MESSAGE_CLASSES[NodeMessageType.FETCH][0]),
        )
    )
    items = [ItemOutcome(ItemResult.MISS, reason=r) for r in MissReason]
    items.append(ItemOutcome(ItemResult.MISS))
    response = FetchResponse(status=Status.OK, items=items)
    _, back = ser.decode_response(ser.encode_response(header, response))
    assert back == response
    assert [i.reason for i in back.items] == [
        "absent",
        "offline",
        "busy",
        "timeout",
        "io_error",
        None,
    ]
    # The worker's window travels with each answer; an older node's answer
    # has none (0: the worker keeps its window)
    windowed = FetchResponse(status=Status.OK, items=items, window_bytes=3 << 20)
    _, back = ser.decode_response(ser.encode_response(header, windowed))
    assert back == windowed
    older = dataclasses.asdict(windowed)
    del older["window_bytes"]
    _, back = ser.decode_response(ser.encode_response(header, older))
    assert back.window_bytes == 0


def test_a_fetch_hit_is_a_lease_given_back_by_fetch_release():
    from maru_common.node_protocol import (
        FetchReleaseRequest,
        FetchReleaseResponse,
        FetchResponse,
    )
    from maru_common.storage_types import (
        LEASE_CAPABILITY,
        ItemOutcome,
        ItemResult,
        Location,
        PoolKind,
        Status,
    )

    assert LEASE_CAPABILITY == "lease_v1"
    assert NodeMessageType.FETCH_RELEASE == 0x1A
    ser = Serializer(NODE_MESSAGE_CLASSES)
    header, _ = ser.decode_request(
        ser.encode(
            NodeMessageType.FETCH,
            sample(NODE_MESSAGE_CLASSES[NodeMessageType.FETCH][0]),
        )
    )
    page = Location("dram:gpu:e", "a1", (3 << 40) | 7, 0, 8, PoolKind.DRAM)
    lease_id = (3 << 40) | 9
    hit = FetchResponse(
        status=Status.OK,
        items=[ItemOutcome(ItemResult.HIT, location=page, lease_id=lease_id)],
    )
    _, back = ser.decode_response(ser.encode_response(header, hit))
    assert back == hit and back.items[0].pin_id is None
    # a uint64 id, every item answered OK or STALE
    release = FetchReleaseRequest(lease_ids=[lease_id, (1 << 64) - 1])
    frame = ser.encode(NodeMessageType.FETCH_RELEASE, release)
    header, decoded = ser.decode_request(frame)
    assert decoded == release
    reply = FetchReleaseResponse(
        status=Status.OK,
        items=[ItemOutcome(ItemResult.OK), ItemOutcome(ItemResult.STALE)],
    )
    _, back = ser.decode_response(ser.encode_response(header, reply))
    assert back == reply


def test_hints_on_the_wire():
    from maru_common.node_protocol import HintRequest
    from maru_common.serializer import MessageDecodeError
    from maru_common.storage_types import (
        HINT_CAPABILITY,
        HintAction,
        Location,
        PoolKind,
    )

    assert HINT_CAPABILITY == "hint_v1"
    assert NodeMessageType.HINT == 0x1B
    assert [a.value for a in HintAction] == [
        "read_hint",
        "read_hint_end",
        "read_promote",
        "write_hint",
        "write_wait",
    ]
    ser = Serializer(NODE_MESSAGE_CLASSES)
    where = Location("cxl:rm1:7", "a1", (2 << 40) | 1, 4096, 8192, PoolKind.CXL)
    for hint in (
        HintRequest(HintAction.READ_HINT, "req-1", [where, where]),
        HintRequest(HintAction.READ_HINT_END, "req-1"),
        HintRequest(HintAction.READ_PROMOTE, "req-1"),
    ):
        frame = ser.encode(NodeMessageType.HINT, hint)
        assert ser.decode_request(frame)[1] == hint
    # an action this side does not know is refused, not taken as another
    payload = dataclasses.asdict(HintRequest(HintAction.READ_HINT, "req-1"))
    payload["action"] = "read_soon"
    with pytest.raises(MessageDecodeError):
        ser.decode_request_payload(ser.encode(NodeMessageType.HINT, payload))


def test_a_wait_this_side_does_not_serve_is_refused():
    from maru_common.node_protocol import WriteDoneRequest
    from maru_common.serializer import MessageDecodeError
    from maru_common.storage_types import WaitLevel

    assert list(WaitLevel) == [WaitLevel.PUBLISHED]
    ser = Serializer(NODE_MESSAGE_CLASSES)
    payload = dataclasses.asdict(sample(WriteDoneRequest))
    payload["wait"] = "lower"
    with pytest.raises(MessageDecodeError):
        ser.decode_request_payload(ser.encode(NodeMessageType.WRITE_DONE, payload))


def test_codes_belong_to_their_endpoint():
    # Each endpoint has its own table, so a code from the other one is unknown
    # here (codes may overlap: 0x10 is REGISTER_KV on MaruServer).
    ser = Serializer(NODE_MESSAGE_CLASSES)
    frame = Serializer().encode(MessageType.NODE_OPEN, sample(NodeOpenRequest))
    with pytest.raises(ValueError, match="Unknown message type"):
        ser.decode_request(frame)


class TestFraming:
    def test_messages_back_to_back(self, pair):
        a, b = pair
        ser = Serializer(NODE_MESSAGE_CLASSES)
        frames = [
            ser.encode(code, sample(NODE_MESSAGE_CLASSES[code][0])) for code in CODES
        ]
        for f in frames:
            send_frame(a, f)
        for f in frames:
            got, fds = recv_frame(b)
            assert got == f and fds == []

    def test_a_message_split_across_writes(self, pair):
        a, b = pair
        ser = Serializer(NODE_MESSAGE_CLASSES)
        frame = ser.encode(
            NodeMessageType.ATTACH,
            sample(NODE_MESSAGE_CLASSES[NodeMessageType.ATTACH][0]),
        )
        for i in range(0, len(frame), 5):
            a.sendall(frame[i : i + 5])
        got, _ = recv_frame(b)
        assert got == frame

    def test_peer_closed_between_messages(self, pair):
        a, b = pair
        a.close()
        assert recv_frame(b) is None

    def test_peer_closed_inside_a_message(self, pair):
        a, b = pair
        frame = Serializer(NODE_MESSAGE_CLASSES).encode(NodeMessageType.DETACH, {})
        a.sendall(frame[:10])
        a.close()
        with pytest.raises(FrameError):
            recv_frame(b)

    def test_not_a_maru_message(self, pair):
        a, b = pair
        a.sendall(
            struct.pack(HEADER_FORMAT, 0xBEEF, PROTOCOL_VERSION, 1, 0, 0, 0, 0, 1, 0)
        )
        with pytest.raises(FrameError):
            recv_frame(b)

    def test_too_large(self, pair):
        a, b = pair
        a.sendall(
            struct.pack(
                HEADER_FORMAT,
                PROTOCOL_MAGIC,
                PROTOCOL_VERSION,
                1,
                0,
                0,
                0,
                0,
                1,
                MAX_NODE_PAYLOAD + 1,
            )
        )
        with pytest.raises(FrameError):
            recv_frame(b)

    def test_short_frame_is_refused(self, pair):
        a, _ = pair
        with pytest.raises(ValueError):
            send_frame(a, b"\x00" * 3)


class TestDescriptors:
    def test_memfd_travels_with_attach(self, pair):
        a, b = pair
        ser = Serializer(NODE_MESSAGE_CLASSES)
        fds = []
        for content in (b"pool-0", b"pool-1"):
            fd = os.memfd_create("maru-test")
            os.write(fd, content)
            fds.append(fd)
        frame = ser.encode(
            NodeMessageType.ATTACH | RESPONSE_BIT, sample(AttachResponse)
        )
        try:
            send_frame(a, frame, fds)
            got, received = recv_frame(b, max_fds=MAX_NODE_FDS)
            assert got == frame
            assert [os.pread(fd, 6, 0) for fd in received] == [b"pool-0", b"pool-1"]
            # the receiver gets its own descriptors, closed on exec
            assert all(os.get_inheritable(fd) is False for fd in received)
            assert set(received).isdisjoint(fds)
        finally:
            for fd in fds + locals().get("received", []):
                os.close(fd)

    def test_descriptors_then_next_message(self, pair):
        a, b = pair
        ser = Serializer(NODE_MESSAGE_CLASSES)
        fd = os.memfd_create("maru-test")
        first = ser.encode(
            NodeMessageType.ATTACH | RESPONSE_BIT, sample(AttachResponse)
        )
        second = ser.encode(
            NodeMessageType.NOTIFY, Notify(kind=NotifyKind.CREDIT_RETURN, bytes=4096)
        )
        try:
            send_frame(a, first, [fd])
            send_frame(a, second)
            got1, fds1 = recv_frame(b, max_fds=1)
            got2, fds2 = recv_frame(b)
            assert (got1, len(fds1)) == (first, 1)
            assert (got2, fds2) == (second, [])
            os.close(fds1[0])
        finally:
            os.close(fd)

    def test_too_many_descriptors(self, pair):
        a, _ = pair
        frame = Serializer(NODE_MESSAGE_CLASSES).encode(NodeMessageType.DETACH, {})
        with pytest.raises(ValueError):
            send_frame(a, frame, list(range(100)))


def test_peer_credentials(pair):
    _, b = pair
    pid, uid, gid = peer_credentials(b)
    assert (pid, uid, gid) == (os.getpid(), os.getuid(), os.getgid())


def test_notify_uses_sequence_zero():
    ser = Serializer(NODE_MESSAGE_CLASSES)
    frame = ser.encode(
        NodeMessageType.NOTIFY,
        Notify(kind=NotifyKind.POOL_REMOVE),
        seq=NOTIFY_SEQUENCE,
    )
    header, decoded = ser.decode_request(frame)
    assert header.sequence == 0 and decoded.kind is NotifyKind.POOL_REMOVE


# ---------------------------------------------------------------------------
# Descriptor limits, leaks, large frames, checks on send
# ---------------------------------------------------------------------------


def _open_fds() -> int:
    return len(os.listdir("/proc/self/fd"))


def _memfds(n):
    return [os.memfd_create(f"maru-test-{i}") for i in range(n)]


class TestDescriptorLimits:
    def test_unexpected_descriptors_are_closed_and_refused(self, pair):
        a, b = pair
        frame = Serializer(NODE_MESSAGE_CLASSES).encode(NodeMessageType.DETACH, {})
        fds = _memfds(3)
        before = _open_fds()
        try:
            send_frame(a, frame, fds)
            with pytest.raises(FrameError, match="at most 0"):
                recv_frame(b)  # a request brings no descriptors
            assert _open_fds() == before
        finally:
            for fd in fds:
                os.close(fd)

    def test_more_than_the_maximum_is_truncated_and_refused(self, pair):
        a, b = pair
        frame = Serializer(NODE_MESSAGE_CLASSES).encode(NodeMessageType.DETACH, {})
        fds = _memfds(MAX_NODE_FDS + 4)
        before = _open_fds()
        try:
            socket.send_fds(a, [frame], fds)  # a raw peer, past send_frame's check
            with pytest.raises(FrameError):
                recv_frame(b, max_fds=MAX_NODE_FDS)
            assert _open_fds() == before
        finally:
            for fd in fds:
                os.close(fd)

    def test_max_fds_range(self, pair):
        _, b = pair
        with pytest.raises(ValueError):
            recv_frame(b, max_fds=MAX_NODE_FDS + 1)

    def test_a_frame_larger_than_the_socket_buffer(self, pair):
        a, b = pair
        ser = Serializer(NODE_MESSAGE_CLASSES)
        reply = sample(AttachResponse)
        reply.error = "x" * (8 << 20)  # far beyond SO_SNDBUF, so sendall continues
        frame = ser.encode(NodeMessageType.ATTACH | RESPONSE_BIT, reply)
        (fd,) = _memfds(1)
        got = {}

        def reader():
            got["frame"], got["fds"] = recv_frame(b, max_fds=1)

        t = threading.Thread(target=reader)
        t.start()
        try:
            send_frame(a, frame, [fd])
            t.join(timeout=30)
            assert got["frame"] == frame and len(got["fds"]) == 1
            os.close(got["fds"][0])
        finally:
            os.close(fd)


class TestSendChecks:
    def test_length_must_match_the_header(self, pair):
        a, _ = pair
        frame = Serializer(NODE_MESSAGE_CLASSES).encode(NodeMessageType.DETACH, {})
        with pytest.raises(ValueError, match="payload bytes"):
            send_frame(a, frame + b"\x00")

    def test_too_large_is_refused_before_sending(self, pair):
        a, _ = pair
        header = struct.pack(
            HEADER_FORMAT,
            PROTOCOL_MAGIC,
            PROTOCOL_VERSION,
            1,
            0,
            0,
            0,
            0,
            1,
            MAX_NODE_PAYLOAD + 1,
        )
        with pytest.raises(ValueError, match="too large"):
            send_frame(a, header + b"\x00" * (MAX_NODE_PAYLOAD + 1))

    def test_version_mismatch_is_named(self, pair):
        a, b = pair
        a.sendall(
            struct.pack(
                HEADER_FORMAT, PROTOCOL_MAGIC, PROTOCOL_VERSION + 1, 1, 0, 0, 0, 0, 1, 0
            )
        )
        with pytest.raises(FrameError, match="protocol version"):
            recv_frame(b)


class TestProcessIdentity:
    def test_start_time_matches_proc(self):
        stat = open("/proc/self/stat").read()
        expected = int(stat[stat.rindex(")") + 2 :].split()[19])
        assert process_start_time(os.getpid()) == expected

    def test_start_time_with_spaces_in_the_name(self, tmp_path):
        # The command name may hold spaces and parentheses; only the last ')'
        # ends it.
        odd = tmp_path / "a) b (c"
        odd.symlink_to(shutil.which("sleep"))
        child = subprocess.Popen([str(odd), "30"])
        try:
            deadline = time.monotonic() + 10
            while b") b (c)" not in open(f"/proc/{child.pid}/stat", "rb").read():
                assert time.monotonic() < deadline, "child never exec'd"
                time.sleep(0.01)
            assert process_start_time(child.pid) >= process_start_time(os.getpid())
        finally:
            child.kill()
            child.wait()

    def test_same_namespace(self):
        assert same_pid_namespace(os.getpid())


@pytest.mark.parametrize(
    "code,path,bad",
    [
        (NodeMessageType.READ_JOIN, ["agent_name"], 3),
        (NodeMessageType.READ_JOIN, ["agent_metadata"], "not-bytes"),
        (NodeMessageType.PEERS, ["peers", 0, "generation_known"], 1),
        (NodeMessageType.PEERS, ["peers", 0, "conn_gen"], True),
        (NodeMessageType.PEERS, ["peers", 0, "pools", 0, "pool_registration"], "1"),
        (NodeMessageType.PEERS, ["peers", 0, "pools", 0, "metadata"], "not-bytes"),
    ],
)
def test_direct_read_messages_decode_strictly(code, path, bad):
    from maru_common.serializer import MessageDecodeError

    payload = dataclasses.asdict(sample(NODE_MESSAGE_CLASSES[code][0]))
    value = payload
    for name in path[:-1]:
        value = value[name]
    value[path[-1]] = bad
    ser = Serializer(NODE_MESSAGE_CLASSES)
    with pytest.raises(MessageDecodeError):
        ser.decode_request_payload(ser.encode(code, payload))


def test_attach_defaults_to_no_peers_and_peers_notification_uses_sequence_zero():
    ser = Serializer(NODE_MESSAGE_CLASSES)
    header, _ = ser.decode_request(
        ser.encode(
            NodeMessageType.ATTACH,
            sample(NODE_MESSAGE_CLASSES[NodeMessageType.ATTACH][0]),
        )
    )
    payload = dataclasses.asdict(sample(AttachResponse))
    del payload["peers"]
    assert ser.decode_response(ser.encode_response(header, payload))[1].peers == []
    peers = sample(NODE_MESSAGE_CLASSES[NodeMessageType.PEERS][0])
    header, back = ser.decode_request(
        ser.encode(NodeMessageType.PEERS, peers, seq=NOTIFY_SEQUENCE)
    )
    assert header.sequence == 0 and back == peers
