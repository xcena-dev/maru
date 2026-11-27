# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""The node server's RPC: multi-storage requests over real ZMQ round trips,
HANDSHAKE, refused storage-backend requests, frames it cannot read, the order
requests are answered in, and start and stop."""

import dataclasses
import socket
import struct
import threading
import time
from types import SimpleNamespace

import msgpack
import pytest
import zmq

from maru_common.ms_rpc import PLACEHOLDER_OP, MsRpcClient, RpcUnknownError
from maru_common.protocol import (
    HEADER_FORMAT,
    HEADER_SIZE,
    MS_MESSAGE_CLASSES,
    PROTOCOL_MAGIC,
    PROTOCOL_VERSION,
    CommitItem,
    CommitRequest,
    HandshakeRequest,
    LookupAndPinRequest,
    MessageType,
    ProbeRequest,
)
from maru_common.serializer import Serializer
from maru_common.storage_types import Status
from maru_server.node_rpc import NodeRpcServer
from maru_server.node_server import NodeServer
from tests.unit.ms_fakes import FakeNode, FakeWorker, RpcWire, admin_delete, key
from tests.unit.ms_node_helpers import wait_until


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(params=[MessageType.PROBE, MessageType.LOOKUP_AND_PIN])
def direct_request(request):
    code = request.param
    if code == MessageType.PROBE:
        return code, ProbeRequest("session", "epoch", [])
    return code, LookupAndPinRequest("session", "epoch", PLACEHOLDER_OP, [])


def test_disconnected_direct_send_fails_without_waiting(direct_request):
    rpc = MsRpcClient(f"tcp://127.0.0.1:{free_port()}", timeout_s=1.0)
    try:
        started = time.monotonic()
        with pytest.raises(RpcUnknownError):
            rpc.call(*direct_request)
        assert time.monotonic() - started < 0.5
    finally:
        rpc.close()


def test_busy_direct_send_is_attempted_once_without_polling(direct_request):
    rpc = MsRpcClient(f"tcp://127.0.0.1:{free_port()}", timeout_s=0.5)
    sends = []

    def send(parts, flags):
        sends.append(flags)
        raise zmq.Again()

    rpc._direct_local.entry = (
        SimpleNamespace(
            send_multipart=send,
            poll=lambda *args: pytest.fail("unsent RPC polled"),
        ),
        threading.Lock(),
    )
    try:
        with pytest.raises(RpcUnknownError):
            rpc.call(*direct_request)
        assert sends == [zmq.NOBLOCK]
    finally:
        rpc.close()


def test_sent_direct_request_keeps_its_response_timeout(direct_request):
    context = zmq.Context()
    router = context.socket(zmq.ROUTER)
    port = router.bind_to_random_port("tcp://127.0.0.1")
    rpc = MsRpcClient(f"tcp://127.0.0.1:{port}", timeout_s=1.0)
    try:
        sock, _ = rpc._direct_local.entry
        assert sock.poll(1000, zmq.POLLOUT)
        started = time.monotonic()
        with pytest.raises(RpcUnknownError):
            rpc.call(*direct_request)
        assert time.monotonic() - started >= 0.5
        assert router.recv_multipart(flags=zmq.NOBLOCK)
    finally:
        rpc.close()
        router.close(linger=0)
        context.term()


class _Served:
    """A NodeRpcServer serving on a thread, and REQ wires to it."""

    def __init__(self, server=None, **kwargs):
        self.server = server or NodeServer()
        port = free_port()
        self.address = f"tcp://127.0.0.1:{port}"
        self.rpc = NodeRpcServer(self.server, host="127.0.0.1", port=port, **kwargs)
        self.thread = threading.Thread(target=self.rpc.start, daemon=True)
        self.thread.start()
        self.wires = []

    def wire(self, timeout_ms=5000):
        wire = RpcWire(self.address, self.server.ms_service, timeout_ms=timeout_ms)
        self.wires.append(wire)
        return wire

    def close(self):
        for wire in self.wires:
            wire.close()
        self.rpc.stop()
        self.thread.join(5)
        self.server.close()
        assert not self.thread.is_alive()


@pytest.fixture
def served():
    s = _Served()
    yield s.server, s.wire()
    s.close()


def raw_frame(code: int, payload, seq: int = 77) -> bytes:
    body = msgpack.packb(payload, use_bin_type=True)
    header = struct.pack(
        HEADER_FORMAT,
        PROTOCOL_MAGIC,
        PROTOCOL_VERSION,
        code,
        0,
        0,
        0,
        0,
        seq,
        len(body),
    )
    return header + body


def test_store_lookup_unpin_round_trip(served):
    server, wire = served
    node = FakeNode(wire, "n1")
    assert node.open().status == Status.OK
    k = key("k")
    loc = node.loc(node.dram(), 1)
    assert node.commit((k, loc)).items[0].result == "ADDED"
    worker = FakeWorker(wire, node)
    assert worker.open().status == Status.OK
    hit = worker.lookup(k).items[0]
    assert hit.result == "HIT" and hit.location == loc
    assert worker.unpin(hit.pin_id).items[0].result == "OK"
    assert node.retire((k, loc)).items[0].result == "RETIRED"
    # the first multi-storage request started the maintenance thread
    assert server.ms_service._thread is not None


@pytest.mark.parametrize(
    "code, payload",
    [
        # wrong field types
        (
            MessageType.COMMIT,
            {"session": 1, "server_epoch": "e", "op": {}, "items": []},
        ),
        # bool where an int belongs
        (
            MessageType.UNPIN,
            {"session": "s", "server_epoch": "e", "pin_ids": [True]},
        ),
        # unknown enum value
        (MessageType.NODE_CLOSE, {"session": "s", "server_epoch": "e", "mode": "nap"}),
        # missing required field
        (MessageType.LOOKUP_AND_PIN, {"session": "s", "server_epoch": "e"}),
        # not a map
        (MessageType.WORKER_OPEN, ["not", "a", "map"]),
    ],
    ids=["types", "bool", "enum", "missing", "not-a-map"],
)
def test_malformed_request_is_rejected_with_its_header(served, code, payload):
    server, wire = served
    header, response = wire.exchange(raw_frame(code, payload, seq=4321))
    assert header.msg_type == code | 0x80
    assert header.sequence == 4321
    assert response.status == Status.REJECTED and response.error
    # the server keeps serving
    assert FakeNode(wire, "n1").open().status == Status.OK


@pytest.mark.parametrize("body", [b"\xc1", None], ids=["not-msgpack", "truncated"])
def test_a_payload_that_cannot_be_read_is_rejected_with_its_header(served, body):
    _, wire = served
    if body is None:  # the header says one byte more than follows
        frame = raw_frame(MessageType.NODE_OPEN, {"session": "s"}, seq=42)[:-1]
    else:
        header = struct.pack(
            HEADER_FORMAT,
            PROTOCOL_MAGIC,
            PROTOCOL_VERSION,
            MessageType.NODE_OPEN,
            0,
            0,
            0,
            0,
            42,
            len(body),
        )
        frame = header + body
    header, response = wire.exchange(frame)
    assert header.msg_type == MessageType.NODE_OPEN | 0x80
    assert header.sequence == 42
    assert response.status == Status.REJECTED and response.error
    assert FakeNode(wire, "n1").open().status == Status.OK


def test_a_started_server_answers_miss_at_once(served):
    _, wire = served
    node = FakeNode(wire, "n1")
    assert node.open().status == Status.OK
    k = key("k")
    response = node.commit((k, node.loc(node.dram(), 0)))
    assert [i.result.value for i in response.items] == ["ADDED"]
    worker = FakeWorker(wire, node)
    assert worker.open().status == Status.OK
    lookup = worker.lookup(k, key("absent"))
    assert [i.result.value for i in lookup.items] == ["HIT", "MISS"]


def test_handshake_tells_the_runs_epoch(served):
    server, wire = served
    header, response = wire.exchange(
        wire.serializer.encode(MessageType.HANDSHAKE, HandshakeRequest(), seq=5)
    )
    assert header.msg_type == MessageType.HANDSHAKE | 0x80 and header.sequence == 5
    assert response.success is True
    assert response.server_epoch == server.server_epoch


def test_storage_backend_requests_are_refused(served):
    _, wire = served
    wire.socket.send(wire.serializer.encode(0x30, {"action": "usage"}))
    header, reply = wire.serializer.decode(wire.socket.recv())
    assert header.msg_type == 0x30 | 0x80
    assert reply["error"] == (
        "This server uses --backend node. CXL clients use maru-server --backend cxl."
    )
    # the server keeps serving
    assert FakeNode(wire, "n1").open().status == Status.OK


def test_meanings_of_another_release_are_refused(served):
    """An SSD pool, FINALIZE_MOVE (0x5A, reserved) and a pool kind this
    server does not know are refused, never taken as something else."""
    server, wire = served
    node = FakeNode(wire, "n1")
    request = dataclasses.asdict(node.open_request())
    ssd = dict(request["pools"][0], pool_id="ssd:n1:/mnt/nvme0/maru", kind="ssd")
    request["pools"].append(ssd)
    frame = wire.serializer.encode(MessageType.NODE_OPEN, request, seq=9)
    header, response = wire.exchange(frame)
    assert header.msg_type == MessageType.NODE_OPEN | 0x80 and header.sequence == 9
    assert response.status == Status.REJECTED and "ssd" in response.error
    assert "n1" not in server.ms_service.registry.nodes
    for code in range(0x40, 0x60):
        if code in MS_MESSAGE_CLASSES:
            continue
        wire.socket.send(raw_frame(code, {"session": "s", "items": []}, seq=11))
        header, reply = wire.serializer.decode(wire.socket.recv())
        assert header.msg_type == code | 0x80 and header.sequence == 11
        assert reply["error"] == f"message 0x{code:02X} is not served by this release"
        assert "maru-server" not in reply["error"]
    # the server keeps serving
    assert node.open().status == Status.OK
    k, loc = key("k"), node.loc(node.dram(), 1)
    commit = node.build(
        CommitRequest, **node.envelope(), items=[CommitItem(k, loc, loc.length)]
    )
    payload = dataclasses.asdict(commit)
    payload["items"][0]["location"]["kind"] = "ssd"
    header, response = wire.exchange(
        wire.serializer.encode(MessageType.COMMIT, payload, seq=12)
    )
    assert response.status == Status.REJECTED
    assert not server.ms_service.catalog.locations(k)


def test_a_failing_handler_answers_rejected():
    server = NodeServer()
    rpc = NodeRpcServer(server, port=free_port())

    def boom(request, payload):
        raise RuntimeError("boom")

    rpc._get_ms_handlers()[MessageType.PROBE] = boom
    serializer = Serializer()
    frame = raw_frame(
        MessageType.PROBE, {"session": "s", "server_epoch": "e", "items": []}, seq=9
    )
    header, response = serializer.decode_response(
        rpc._process_ms_frame(serializer, frame)
    )
    assert header.msg_type == MessageType.PROBE | 0x80 and header.sequence == 9
    assert response.status == Status.REJECTED and "boom" in response.error
    server.close()


def test_admin_delete_and_its_cli_over_zmq(served, capsys):
    from maru_server import ms_admin

    server, wire = served
    node = FakeNode(wire, "n1")
    node.open()
    loc = node.loc(node.dram(), 1)
    node.commit((key("k"), loc))
    address = wire.socket.getsockopt_string(zmq.LAST_ENDPOINT)
    assert (
        ms_admin.main(["delete", "--server", address, "--namespace", "ns", "k", "gone"])
        == 0
    )
    out = capsys.readouterr().out.splitlines()
    assert out == ["k\tOK", "gone\tMISS (absent)"]
    (block,) = node.heartbeat().blocks
    assert [(i.key, i.location) for i in block.items] == [(key("k"), loc)]
    # idempotent: the same request again changes nothing
    response = admin_delete(wire, key("k"))
    assert [i.result for i in response.items] == ["OK"]
    assert len(node.heartbeat().blocks) == 1


def test_admin_cli_reports_an_unreachable_server(capsys):
    from maru_server import ms_admin

    port = free_port()
    code = ms_admin.main(
        [
            "delete",
            "--server",
            f"tcp://127.0.0.1:{port}",
            "--namespace",
            "ns",
            "--timeout-ms",
            "200",
            "k",
        ]
    )
    assert code == 1 and "delete failed" in capsys.readouterr().err


def test_stop_while_multi_storage_requests_are_in_flight():
    served = _Served()
    setup = served.wire()
    node = FakeNode(setup, "n1")
    node.open()
    keys = [key(f"k{i}") for i in range(64)]
    node.commit(*[(k, node.loc(node.dram(), i)) for i, k in enumerate(keys)])
    stop = threading.Event()
    answered = []

    def client():
        wire = RpcWire(served.address, served.server.ms_service, timeout_ms=1000)
        worker = FakeWorker(wire, node)
        try:
            worker.open()
            while not stop.is_set():
                worker.lookup(*keys)
                answered.append(1)
        except zmq.ZMQError:
            pass  # the server went away mid-request
        finally:
            wire.close()

    clients = [threading.Thread(target=client, daemon=True) for _ in range(4)]
    for t in clients:
        t.start()
    try:
        wait_until(lambda: len(answered) >= 20, what="20 lookups answered")
        started = time.monotonic()
    finally:
        try:
            served.close()
        finally:
            stop.set()
            for t in clients:
                t.join(5)
    assert time.monotonic() - started < 5
    assert not any(t.is_alive() for t in clients)


def test_a_stop_before_the_start_is_kept():
    server = NodeServer()
    port = free_port()
    rpc = NodeRpcServer(server, host="127.0.0.1", port=port)
    rpc.stop()  # e.g. a signal while the process starts
    serving = threading.Thread(target=rpc.start, daemon=True)
    serving.start()
    serving.join(5)
    assert not serving.is_alive()
    with socket.socket() as s:
        s.bind(("127.0.0.1", port))  # never bound
    server.close()


def test_a_port_in_use_fails_the_start():
    server = NodeServer()
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen()
        port = taken.getsockname()[1]
        rpc = NodeRpcServer(server, host="127.0.0.1", port=port)
        with pytest.raises(zmq.ZMQError):
            rpc.start()
    assert not rpc._running
    server.close()


def _block_slow_probes(server):
    """A PROBE of session "slow" is held until ``release`` is set."""
    entered, release = threading.Event(), threading.Event()
    probe = server.ms_service.probe

    def slow_probe(request, payload):
        if request.session == "slow":
            entered.set()
            release.wait(10)
        return probe(request, payload)

    server.ms_service.probe = slow_probe
    return entered, release


def _probe_frame(session="s"):
    request = ProbeRequest(session=session, server_epoch="e", items=[])
    return Serializer().encode(MessageType.PROBE, request)


def test_requests_are_answered_one_at_a_time_in_order():
    server = NodeServer()
    entered, release = _block_slow_probes(server)
    served = _Served(server)
    try:
        slow, later = served.wire(), served.wire()
        slow.socket.send(_probe_frame("slow"))
        assert entered.wait(5)
        later.socket.send(_probe_frame())
        # waits behind the slow one; never refused BUSY
        assert not later.socket.poll(300)
        release.set()
        slow.socket.recv()
        reply = later.serializer.decode_response(later.socket.recv())[1]
        assert reply.status != Status.BUSY
    finally:
        release.set()
        served.close()


@pytest.mark.parametrize(
    "frame",
    [
        b"short",
        # a multi-storage code with a bad magic and no readable payload
        bytes([0, 0, 0, MessageType.PROBE]) + bytes(HEADER_SIZE - 4) + b"\xc1",
    ],
    ids=["shorter-than-a-header", "bad-magic"],
)
def test_a_frame_without_a_valid_header_gets_no_answer(served, frame):
    server, wire = served
    dealer = zmq.Context.instance().socket(zmq.DEALER)
    dealer.setsockopt(zmq.LINGER, 0)
    dealer.connect(wire.socket.getsockopt_string(zmq.LAST_ENDPOINT))
    try:
        dealer.send_multipart([b"", frame])
        assert not dealer.poll(300)
    finally:
        dealer.close()
    # the server keeps serving
    assert FakeNode(wire, "n1").open().status == Status.OK
