# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""The node server's RPC: multi-storage requests over real ZMQ round trips,
HANDSHAKE, refused storage-backend requests, the request queue and its
workers."""

import socket
import struct
import threading
import time

import msgpack
import pytest
import zmq

from maru_common.protocol import (
    HEADER_FORMAT,
    PROTOCOL_MAGIC,
    PROTOCOL_VERSION,
    HandshakeRequest,
    MessageType,
    ProbeRequest,
    StorageRequest,
)
from maru_common.serializer import Serializer
from maru_common.storage_types import Status
from maru_server.node_rpc import NodeRpcServer
from maru_server.node_server import NodeServer
from tests.unit.ms_fakes import FakeNode, FakeWorker, RpcWire, admin_delete, key


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _Served:
    """A NodeRpcServer serving on a thread, and REQ wires to it."""

    def __init__(self, server=None, **kwargs):
        self.server = server or NodeServer()
        port = free_port()
        self.address = f"tcp://127.0.0.1:{port}"
        kwargs.setdefault("num_workers", 2)
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
    wire.socket.send(
        wire.serializer.encode(MessageType.STORAGE, StorageRequest(action="usage"))
    )
    header, reply = wire.serializer.decode(wire.socket.recv())
    assert header.msg_type == MessageType.STORAGE | 0x80
    assert "maru-server" in reply["error"]
    # the server keeps serving
    assert FakeNode(wire, "n1").open().status == Status.OK


def test_a_failing_handler_answers_rejected():
    server = NodeServer()
    rpc = NodeRpcServer(server, port=free_port(), num_workers=1)

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
    while len(answered) < 20:
        time.sleep(0.01)
    started = time.monotonic()
    served.close()
    stop.set()
    for t in clients:
        t.join(5)
    assert time.monotonic() - started < 5
    assert not any(t.is_alive() for t in clients)


class _ProxyHarness:
    """Drives _proxy_routine with mock sockets and a scripted poller."""

    def __init__(self, queue_max=2):
        from unittest.mock import MagicMock

        self.server = NodeServer()
        self.rpc = NodeRpcServer(
            self.server, port=5556, num_workers=1, queue_max=queue_max
        )
        self.rpc._frontend = MagicMock()
        self.rpc._replies = MagicMock()
        for sock in (self.rpc._frontend, self.rpc._replies):
            sock.getsockopt.return_value = 0  # one frame per poll
        self.serializer = Serializer()

    def frame(self, code, request, seq):
        return [b"id", b"", self.serializer.encode(code, request, seq=seq)]

    def run(self, rounds, unpolled=()):
        """rounds: (socket, message) pairs, one readable socket per poll;
        ``unpolled``: frames the frontend has after those, with no poll."""
        from unittest.mock import MagicMock, patch

        self.rpc._replies.recv_multipart.side_effect = [
            m for s, m in rounds if s is self.rpc._replies
        ]
        self.rpc._frontend.recv_multipart.side_effect = [
            *(m for s, m in rounds if s is self.rpc._frontend),
            *unpolled,
        ]
        script = iter(rounds)

        def poll(_timeout):
            try:
                sock, _ = next(script)
            except StopIteration:
                self.rpc._running = False
                return []
            return [(sock, zmq.POLLIN)]

        poller = MagicMock()
        poller.poll.side_effect = poll
        self.rpc._running = True
        with patch.object(zmq, "Poller", return_value=poller):
            self.rpc._proxy_routine()

    def queued(self):
        out = []
        while not self.rpc._requests.empty():
            out.append(self.rpc._requests.get())
        return out

    def replies(self):
        return [
            self.serializer.decode_response(call.args[0][2])
            for call in self.rpc._frontend.send_multipart.call_args_list
        ]

    def close(self):
        self.server.close()


def _probe(seq):
    return MessageType.PROBE, ProbeRequest(session="s", server_epoch="e", items=[]), seq


def test_a_full_queue_answers_busy():
    h = _ProxyHarness(queue_max=2)
    front = h.rpc._frontend
    probes = [h.frame(*_probe(seq)) for seq in (1, 2, 3)]
    h.run([(front, p) for p in probes])
    # two frames wait in the queue; the third overflows and gets BUSY
    ((header, reply),) = h.replies()
    assert header.sequence == 3 and reply.status == Status.BUSY
    assert h.queued() == probes[:2]
    h.close()


def test_requests_wait_in_order():
    h = _ProxyHarness(queue_max=10)
    front = h.rpc._frontend
    frames = [h.frame(*_probe(seq)) for seq in (1, 2, 3)]
    h.run([(front, f) for f in frames])
    assert h.queued() == frames and h.replies() == []
    h.close()


def test_replies_go_back_with_their_envelope():
    h = _ProxyHarness()
    answer = [b"id", b"", b"answer"]
    h.run([(h.rpc._replies, answer)])
    h.rpc._frontend.send_multipart.assert_called_once_with(answer)
    h.close()


def test_the_proxy_moves_every_ready_frame_before_it_polls_again():
    h = _ProxyHarness(queue_max=10)
    front = h.rpc._frontend
    frames = [h.frame(*_probe(seq)) for seq in (1, 2, 3)]
    front.getsockopt.side_effect = [zmq.POLLIN, zmq.POLLIN, 0]
    h.run([(front, frames[0])], unpolled=frames[1:])
    assert h.queued() == frames
    h.close()


def test_a_request_sent_before_workers_connect_is_served():
    class SlowWorkers(NodeRpcServer):
        def _worker_routine(self, worker_id):
            time.sleep(0.5)
            super()._worker_routine(worker_id)

    server = NodeServer()
    port = free_port()
    rpc = SlowWorkers(server, host="127.0.0.1", port=port, num_workers=2)
    serving = threading.Thread(target=rpc.start, daemon=True)
    serving.start()
    wire = RpcWire(f"tcp://127.0.0.1:{port}", server.ms_service)
    try:
        assert FakeNode(wire, "n1").open().status == Status.OK
    finally:
        wire.close()
        rpc.stop()
        serving.join(5)
        server.close()


def test_stop_leaves_a_running_proxys_sockets_to_it():
    from unittest.mock import MagicMock

    server = NodeServer()
    rpc = NodeRpcServer(server, port=5556, num_workers=1)
    rpc._owns_sockets = True  # as after start()
    frontend, context = MagicMock(), MagicMock()
    rpc._frontend, rpc._context = frontend, context
    rpc._proxy_thread = MagicMock()
    rpc._proxy_thread.is_alive.return_value = True  # join timed out
    rpc.stop()
    frontend.close.assert_not_called()
    context.term.assert_not_called()
    server.close()


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_start_gives_up_when_workers_never_connect():
    class DeadWorkers(NodeRpcServer):
        def _worker_routine(self, worker_id):
            raise RuntimeError("worker died before connecting")

    server = NodeServer()
    rpc = DeadWorkers(server, port=free_port(), num_workers=1, connect_timeout_s=0.5)
    started = time.monotonic()
    with pytest.raises(RuntimeError, match="did not connect"):
        rpc.start()
    assert time.monotonic() - started < 5
    assert rpc._context is None and rpc._frontend is None and not rpc._running
    server.close()


def _block_slow_probes(server):
    """A PROBE of session "slow" holds its worker until ``release`` is set."""
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


def test_a_slow_request_does_not_delay_one_an_idle_worker_can_take():
    server = NodeServer()
    entered, release = _block_slow_probes(server)
    served = _Served(server, num_workers=2)
    try:
        slow = served.wire()
        slow.socket.send(_probe_frame("slow"))
        assert entered.wait(5)
        fast = served.wire(timeout_ms=2000)
        for _ in range(8):
            started = time.monotonic()
            fast.exchange(_probe_frame())
            assert time.monotonic() - started < 1.0
        release.set()
        slow.socket.recv()
    finally:
        release.set()
        served.close()


def test_with_every_worker_busy_a_full_queue_answers_busy():
    server = NodeServer()
    entered, release = _block_slow_probes(server)
    served = _Served(server, num_workers=1, queue_max=1)
    try:
        slow, queued, refused = served.wire(), served.wire(), served.wire(2000)
        slow.socket.send(_probe_frame("slow"))
        assert entered.wait(5)
        queued.socket.send(_probe_frame())
        time.sleep(0.2)  # queued before the next one arrives
        refused.socket.send(_probe_frame())
        data = refused.socket.recv()
        assert refused.serializer.decode_response(data)[1].status == Status.BUSY
        release.set()
        reply = queued.serializer.decode_response(queued.socket.recv())[1]
        assert reply.status != Status.BUSY
        slow.socket.recv()
    finally:
        release.set()
        served.close()
