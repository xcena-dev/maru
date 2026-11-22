# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageService UDS rules, request by request."""

import dataclasses
import mmap
import os
import socket
import threading

import pytest

from maru_common.ms_rpc import PLACEHOLDER_OP
from maru_common.node_protocol import (
    NODE_MESSAGE_CLASSES,
    AbortRequest,
    CreditRequest,
    CreditReturnRequest,
    DetachRequest,
    FetchCancelRequest,
    FetchRequest,
    GroupLayout,
    LocalPinRequest,
    LocalUnpinRequest,
    NodeMessageType,
    Notify,
    UnmapAckRequest,
    recv_frame,
    send_frame,
)
from maru_common.protocol import MessageType
from maru_common.serializer import Serializer, op_digest
from maru_common.storage_types import (
    FETCH_CAPABILITY,
    LEASE_CAPABILITY,
    MAX_BATCH,
    ItemResult,
    Key,
    NotifyKind,
    OpRef,
    PoolKind,
    SessionRole,
    Status,
    StorageUnavailableError,
    make_generation,
)
from maru_storage_service import local_server
from maru_storage_service.pool_manager import PageState
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import (
    PAGE,
    RawWorker,
    node_config,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)


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
def node(fake, sockdir):
    service = start_node(node_config(fake.url, f"{sockdir}/node.sock"))
    yield service
    service.stop()


@pytest.fixture
def worker(node):
    w = RawWorker(node.config.socket)
    yield w
    w.close()


class TestAttach:
    def test_reply_passes_the_memfd_and_the_node_view(self, node, worker, fake):
        r = worker.attach()
        assert r.status == Status.OK
        assert (r.node_id, r.node_epoch, r.server) == (
            "gpu-test",
            node.node_epoch,
            fake.url,
        )
        assert r.page_size == PAGE and r.local_pin is False
        assert r.budgets == node.config.budgets
        assert (r.pid, r.pid_start) == (os.getpid(), worker.attach_request().pid_start)
        (descriptor,) = r.pools
        assert descriptor.memfd == 0 and descriptor.dax is None
        assert descriptor.pool.pool_id == f"dram:gpu-test:{node.node_epoch}"
        assert descriptor.pool.kind == PoolKind.DRAM
        assert len(worker.fds) == 1
        # the worker maps the very pool the node made
        mine = mmap.mmap(worker.fds[0], descriptor.pool.capacity)
        theirs = mmap.mmap(node.tier("dram").pool.fd, node.tier("dram").pool.capacity)
        mine[PAGE : PAGE + 4] = b"same"
        assert theirs[PAGE : PAGE + 4] == b"same"
        mine.close()
        theirs.close()

    @pytest.mark.parametrize(
        "overrides, error",
        [
            ({"pid": 1}, "is not the peer"),
            ({"pid_start": 1}, "is not the peer"),
            ({"protocol": "ms0"}, "protocol"),
            (
                {"groups": [GroupLayout(object_bytes=PAGE + 1, layout="x")]},
                "larger than the page",
            ),
            ({"groups": []}, "groups is empty"),
            ({"groups": [GroupLayout(object_bytes=0, layout="x")]}, "no object bytes"),
        ],
    )
    def test_refused(self, worker, overrides, error):
        with pytest.raises(StorageUnavailableError, match=error):
            worker.attach(**overrides)
        assert worker.fds == []

    def test_other_pid_namespace_uses_the_peer_pid(self, worker, monkeypatch):
        monkeypatch.setattr(local_server, "same_pid_namespace", lambda pid: False)
        r = worker.attach(pid=4242, pid_start=7)
        assert r.status == Status.OK
        assert r.pid == os.getpid()  # SO_PEERCRED, as the node sees it

    def test_largest_group_decides(self, worker):
        groups = [GroupLayout(PAGE // 4, "a"), GroupLayout(PAGE, "b")]
        assert worker.attach(groups=groups).status == Status.OK

    def test_one_attach_per_connection(self, worker):
        worker.attach()
        r = worker.request(NodeMessageType.ATTACH, worker.attach_request())
        assert r.status == Status.REJECTED and "already attached" in r.error

    def test_incarnation_of_another_identity(self, node, worker):
        worker.attach()
        other = RawWorker(node.config.socket, incarnation=worker.incarnation)
        try:
            with pytest.raises(StorageUnavailableError, match="another identity"):
                other.attach(engine_id="someone-else")
        finally:
            other.close()

    def test_capabilities_are_those_asked_and_served(self, node):
        def attach(**overrides):
            w = RawWorker(node.config.socket)
            try:
                return w.attach(**overrides).capabilities
            finally:
                w.close()

        asked = [FETCH_CAPABILITY, LEASE_CAPABILITY, "later_v9"]
        assert attach(capabilities=asked) == []  # this node serves no FETCH
        node.local.capabilities = frozenset({FETCH_CAPABILITY, LEASE_CAPABILITY})
        assert attach(capabilities=asked) == [FETCH_CAPABILITY, LEASE_CAPABILITY]
        assert attach() == []
        probe = attach(capabilities=asked, role=SessionRole.PROBE, groups=[])
        assert probe == [FETCH_CAPABILITY, LEASE_CAPABILITY]

    def test_requests_need_an_attach(self, worker):
        ser = Serializer(NODE_MESSAGE_CLASSES)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(worker.path)
        try:
            request = worker.stamp(
                CreditRequest(op=PLACEHOLDER_OP, worker_session="s", want_bytes=PAGE)
            )
            send_frame(sock, ser.encode(NodeMessageType.CREDIT, request))
            frame, fds = recv_frame(sock)
            _, response = ser.decode_response(frame)
            assert (
                response.status == Status.REJECTED and response.error == "not attached"
            )
            assert fds == []
        finally:
            sock.close()


class TestCredit:
    def test_needs_a_worker_session_and_positive_credit(self, worker):
        worker.attach()
        assert worker.credit(PAGE, session="").status == Status.REJECTED
        r = worker.request(
            NodeMessageType.CREDIT,
            worker.stamp(
                CreditRequest(
                    op=PLACEHOLDER_OP,
                    worker_session="s",
                    want_bytes=PAGE,
                )
            ),
        )
        assert r.status == Status.OK and r.credits
        assert worker.credit(0).status == Status.REJECTED
        assert len(worker.credit(PAGE).credits) == 1

    def test_budgets(self, node):
        workers = [RawWorker(node.config.socket) for _ in range(3)]
        try:
            for w in workers:
                w.attach()
            a = workers[0].credit(100 * PAGE).credits
            assert len(a) == 4  # credit_max_bytes per worker
            assert workers[0].credit(PAGE).credits == []
            assert len(workers[1].credit(100 * PAGE).credits) == 4
            assert workers[2].credit(PAGE).credits == []  # node total (8 pages)
            workers[0].request(
                NodeMessageType.CREDIT_RETURN, CreditReturnRequest(credits=a)
            )
            assert len(workers[2].credit(PAGE).credits) == 1
        finally:
            for w in workers:
                w.close()

    def test_transfer_reserve_is_kept(self, fake, sockdir):
        config = node_config(
            fake.url,
            f"{sockdir}/r.sock",
            budgets={
                "credit_max_bytes": 8 * PAGE,
                "credit_total_max_bytes": 8 * PAGE,
                "commit_pending_max_bytes": 1 * PAGE,
                "transfer_reserve_bytes": 3 * PAGE,
            },
        )
        service = start_node(config)
        w = RawWorker(config.socket)
        try:
            w.attach()
            credits = w.credit(8 * PAGE).credits
            assert len(credits) == 8
            w.write_done(credits, [f"k{i}" for i in range(8)])
            # 8 pages free, 3 of them the reserve that credit leaves alone
            assert len(w.credit(8 * PAGE).credits) == 5
            assert service.pools.bytes_in(PageState.FREE) == 3 * PAGE
        finally:
            w.close()
            service.stop()

    def test_op_rules(self, node, worker):
        worker.attach()
        request = worker.stamp(
            CreditRequest(op=PLACEHOLDER_OP, worker_session="s", want_bytes=2 * PAGE)
        )
        first = worker.request(NodeMessageType.CREDIT, request)
        again = worker.request(NodeMessageType.CREDIT, request)
        assert first == again and len(first.credits) == 2
        assert node.pools.held_bytes(worker.incarnation) == 2 * PAGE  # ran once
        conflict = dataclasses.replace(request, want_bytes=PAGE)
        conflict = dataclasses.replace(
            conflict, op=dataclasses.replace(request.op, digest=_digest(conflict))
        )
        r = worker.request(NodeMessageType.CREDIT, conflict)
        assert r.status == Status.REJECTED and "another digest" in r.error
        worker.origin.resolved(request.op.seq)
        later = worker.credit(PAGE)  # acks seq 1
        assert later.status == Status.OK
        stale = worker.request(NodeMessageType.CREDIT, request)
        assert stale.status == Status.REJECTED and "stale" in stale.error

    def test_malformed_ops(self, worker):
        worker.attach()
        request = worker.stamp(
            CreditRequest(op=PLACEHOLDER_OP, worker_session="s", want_bytes=PAGE)
        )
        bad_digest = dataclasses.replace(
            request, op=dataclasses.replace(request.op, digest=b"\0" * 16)
        )
        r = worker.request(NodeMessageType.CREDIT, bad_digest)
        assert r.status == Status.REJECTED and "digest" in r.error
        other_origin = dataclasses.replace(
            request, op=dataclasses.replace(request.op, origin="someone")
        )
        r = worker.request(NodeMessageType.CREDIT, other_origin)
        assert r.status == Status.REJECTED and "origin" in r.error
        ack_ahead = dataclasses.replace(
            request, op=dataclasses.replace(request.op, ack=request.op.seq)
        )
        r = worker.request(NodeMessageType.CREDIT, ack_ahead)
        assert r.status == Status.REJECTED and "ack" in r.error

    def test_op_window(self, fake, sockdir):
        config = node_config(fake.url, f"{sockdir}/w.sock")
        service = start_node(config, op_window=2)
        w = RawWorker(config.socket)
        try:
            w.attach()
            for _ in range(2):
                # results are kept until acked; do not ack
                w.request(
                    NodeMessageType.CREDIT,
                    w.stamp(
                        CreditRequest(
                            op=PLACEHOLDER_OP, worker_session="s", want_bytes=1
                        )
                    ),
                )
            r = w.request(
                NodeMessageType.CREDIT,
                w.stamp(
                    CreditRequest(op=PLACEHOLDER_OP, worker_session="s", want_bytes=1)
                ),
            )
            assert r.status == Status.BUSY
        finally:
            w.close()
            service.stop()


def _digest(request):
    return op_digest(request)


class TestWriteDone:
    def test_published_items(self, node, worker, fake):
        worker.attach()
        credits = worker.credit(3 * PAGE).credits
        r = worker.write_done(credits[:2], ["a", "b"], size=100)
        assert [o.result for o in r.items] == [ItemResult.ADDED, ItemResult.ADDED]
        (loc,) = fake.locations(Key("ns", "a"))
        assert loc.length == 100 and loc.offset == credits[0].offset
        assert node.pools.bytes_in(PageState.READY) == 2 * PAGE
        # the same credit again (a new op) is refused: the page is READY
        r = worker.write_done(credits[:1], ["a2"])
        assert r.items[0].result == ItemResult.REJECTED

    def test_duplicate_frees_the_new_page(self, node, worker, fake):
        worker.attach()
        a, b = worker.credit(2 * PAGE).credits
        worker.write_done([a], ["k"])
        r = worker.write_done([b], ["k"])
        assert r.items[0].result == ItemResult.DUPLICATE
        assert r.items[0].location.offset == a.offset
        assert node.pools.bytes_in(PageState.READY) == PAGE
        assert node.pools.bytes_in(PageState.CREDITED) == 0

    def test_item_checks(self, node, worker):
        worker.attach()
        good, other = worker.credit(2 * PAGE).credits
        old_run = dataclasses.replace(other, generation=make_generation(99, 1))
        r = worker.write_done([good, old_run], ["g", "o"])
        assert [o.result for o in r.items] == [ItemResult.ADDED, ItemResult.REJECTED]
        # another worker's credit
        thief = RawWorker(node.config.socket)
        try:
            thief.attach()
            r = thief.write_done([other], ["x"])
            assert r.items[0].result == ItemResult.REJECTED
        finally:
            thief.close()
        too_long = worker.write_done([other], ["x"], size=PAGE + 1)
        assert too_long.items[0].result == ItemResult.REJECTED
        node.pools.check()

    def test_batch_limit(self, worker):
        worker.attach()
        (c,) = worker.credit(PAGE).credits
        request = worker.write_done_request(
            [c] * (MAX_BATCH + 1), [f"k{i}" for i in range(MAX_BATCH + 1)]
        )
        r = worker.request(NodeMessageType.WRITE_DONE, request)
        assert r.status == Status.REJECTED

    def test_resend_while_pending_gets_the_same_result(self, node, worker, fake):
        worker.attach()
        (c,) = worker.credit(PAGE).credits
        fake.pause()
        request = worker.write_done_request([c], ["slow"])
        first = worker.conn.submit(NodeMessageType.WRITE_DONE, request)
        second = worker.conn.submit(NodeMessageType.WRITE_DONE, request)
        wait_until(
            lambda: node.pools.bytes_in(PageState.PENDING) == PAGE, what="pending"
        )
        assert not first.done() and not second.done()
        fake.resume()
        # both requests of the one connection get the final result
        assert first.result(10) == second.result(10)
        assert first.result().items[0].result == ItemResult.ADDED
        assert fake.executed[MessageType.COMMIT] == 1
        # a third resend after the result is a replay too
        assert worker.request(NodeMessageType.WRITE_DONE, request) == second.result()

    def test_stale_items(self, worker):
        worker.attach()
        (c,) = worker.credit(PAGE).credits
        request = worker.write_done_request([c], ["s"])
        worker.request(NodeMessageType.WRITE_DONE, request)
        worker.origin.resolved(request.op.seq)
        worker.credit(PAGE)  # carries ack >= the WRITE_DONE's seq
        r = worker.request(NodeMessageType.WRITE_DONE, request)
        assert r.status == Status.OK and r.items[0].result == ItemResult.STALE


class TestAbortAndReturn:
    def test_abort_frees(self, node, worker):
        worker.attach()
        a, b = worker.credit(2 * PAGE).credits
        request = worker.stamp(AbortRequest(op=PLACEHOLDER_OP, credits=[a, b]))
        r = worker.request(NodeMessageType.ABORT, request)
        assert [o.result for o in r.items] == [ItemResult.OK, ItemResult.OK]
        assert node.pools.bytes_in(PageState.CREDITED) == 0
        # the same op replays its result; a new op finds nothing to free
        assert worker.request(NodeMessageType.ABORT, request) == r
        r2 = worker.request(
            NodeMessageType.ABORT,
            worker.stamp(AbortRequest(op=PLACEHOLDER_OP, credits=[a])),
        )
        assert r2.items[0].result == ItemResult.REJECTED
        node.pools.check()

    def test_credit_return_is_idempotent(self, node, worker):
        worker.attach()
        credits = worker.credit(2 * PAGE).credits
        for _ in range(2):
            r = worker.request(
                NodeMessageType.CREDIT_RETURN, CreditReturnRequest(credits=credits)
            )
            assert [o.result for o in r.items] == [ItemResult.OK, ItemResult.OK]
        assert node.pools.held_bytes(worker.incarnation) == 0


class TestOtherRequests:
    @pytest.mark.parametrize(
        "code, request_",
        [
            (NodeMessageType.LOCAL_PIN, LocalPinRequest()),
            (NodeMessageType.LOCAL_UNPIN, LocalUnpinRequest()),
            (
                NodeMessageType.FETCH,
                FetchRequest(
                    op=OpRef("o", 1, b"d", 0),
                    worker_session="s",
                    items=[],
                    max_bytes=1,
                    deadline_ms=1,
                ),
            ),
            (
                NodeMessageType.FETCH_CANCEL,
                FetchCancelRequest(target=OpRef("o", 1, b"d", 0)),
            ),
        ],
    )
    def test_disabled(self, worker, code, request_):
        worker.attach()
        assert worker.request(code, request_).status == Status.DISABLED

    def test_undecodable_request_is_rejected_with_its_header(self, worker):
        worker.attach()
        r = worker.request(
            NodeMessageType.CREDIT,
            {"op": "x", "worker_session": 3, "want_bytes": "big"},
        )
        assert r.status == Status.REJECTED
        # the connection stays usable
        assert worker.credit(PAGE).status == Status.OK

    def test_notify_is_not_a_request(self, worker):
        worker.attach()
        r = worker.request(
            NodeMessageType.NOTIFY, Notify(kind=NotifyKind.CREDIT_RETURN)
        )
        assert r.status == Status.REJECTED

    def test_unmap_and_detach(self, worker):
        worker.attach()
        r = worker.request(
            NodeMessageType.UNMAP_ACK, UnmapAckRequest(pool_id="p", pool_generation=1)
        )
        assert r.status == Status.OK
        assert (
            worker.request(NodeMessageType.DETACH, DetachRequest()).status == Status.OK
        )
        assert worker.credit(PAGE).status == Status.REJECTED

    def test_closed_connection_keeps_the_credit(self, node, worker):
        worker.attach()
        credits = worker.credit(2 * PAGE).credits
        worker.close()
        wait_until(lambda: not node.local.connections(), what="connection gone")
        assert node.pools.held_bytes(worker.incarnation) == 2 * PAGE
        again = RawWorker(node.config.socket, incarnation=worker.incarnation)
        again.origin = worker.origin  # the same process goes on with its seqs
        try:
            again.attach()
            r = again.write_done(credits, ["x", "y"])
            assert [o.result for o in r.items] == [ItemResult.ADDED] * 2
        finally:
            again.close()

    def test_many_workers_at_once(self, node):
        workers = [RawWorker(node.config.socket) for _ in range(4)]
        errors = []

        def run(w, i):
            try:
                w.attach()
                (c,) = w.credit(PAGE).credits
                r = w.write_done([c], [f"w{i}"])
                assert r.items[0].result == ItemResult.ADDED
            except Exception as e:  # pragma: no cover - reported below
                errors.append(e)

        threads = [
            threading.Thread(target=run, args=(w, i)) for i, w in enumerate(workers)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        for w in workers:
            w.close()
        assert errors == []
        assert node.pools.bytes_in(PageState.READY) == 4 * PAGE


class _Endpoint:
    """What a Connection needs of its LocalServer to post frames."""

    serializer = Serializer(NODE_MESSAGE_CLASSES)


class _HalfFirst:
    """A socket whose first non-blocking send takes half of the frame."""

    def __init__(self, sock):
        self._sock = sock
        self.halved = False

    def send(self, data, flags=0):
        if not self.halved:
            self.halved = True
            return self._sock.send(data[: len(data) // 2], flags)
        return self._sock.send(data, flags)

    def __getattr__(self, name):
        return getattr(self._sock, name)


@pytest.fixture
def pair():
    """A Connection over a socketpair, its threads not started, and the
    worker's end."""
    ours, theirs = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    theirs.settimeout(10)
    conn = local_server.Connection(_Endpoint(), ours)
    yield conn, theirs
    conn.close()
    conn.join(5)
    ours.close()
    theirs.close()


def _notify(n: int) -> Notify:
    return Notify(kind=NotifyKind.CREDIT_RETURN, bytes=n)


def _received(sock, count: int) -> list[int]:
    out = []
    for _ in range(count):
        frame, _ = recv_frame(sock)
        _, notify = _Endpoint.serializer.decode_as(frame, Notify)
        out.append(notify.bytes)
    return out


class TestPostedFrames:
    def test_the_posting_thread_sends_when_nothing_is_ahead(self, pair):
        conn, theirs = pair
        conn.post_notify(_notify(1))  # no writer thread runs
        assert _received(theirs, 1) == [1]

    def test_a_full_socket_never_holds_up_a_post_and_order_holds(self, pair):
        conn, theirs = pair
        conn.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        conn._writer.start()
        count = 2000
        for n in range(count):  # the worker reads nothing meanwhile
            conn.post_notify(_notify(n))
        with conn._outbox_cv:
            assert conn._outbox or conn._sending  # the socket filled
        assert _received(theirs, count) == list(range(count))

    def test_the_end_of_a_frame_begun_at_once_goes_first(self, pair):
        conn, theirs = pair
        conn.sock = _HalfFirst(conn.sock)
        conn.post_notify(_notify(1))  # half sent: the send lock stays held
        conn.post_notify(_notify(2))  # queued behind its end
        assert conn._rest is not None and len(conn._outbox) == 1
        frame = conn.server.serializer.encode(NodeMessageType.NOTIFY, _notify(3), seq=7)
        answer = threading.Thread(target=conn.send, args=(frame,))
        answer.start()  # the reader's answer waits for the frame's end
        answer.join(0.2)
        assert answer.is_alive()
        conn._writer.start()
        answer.join(5)
        assert not answer.is_alive()
        got = _received(theirs, 3)
        assert got[0] == 1 and sorted(got[1:]) == [2, 3]

    def test_a_post_while_the_reader_sends_goes_to_the_writer(self, pair):
        conn, theirs = pair
        with conn.send_lock:  # the reader is answering
            conn.post_notify(_notify(1))
            assert list(conn._outbox) and not conn._sending
        conn._writer.start()
        assert _received(theirs, 1) == [1]

    def test_closing_lets_go_of_the_send_lock_of_an_unsent_end(self, pair):
        conn, _ = pair
        conn.sock = _HalfFirst(conn.sock)
        conn.post_notify(_notify(1))
        assert conn._rest is not None and conn.send_lock.locked()
        conn.close()
        assert conn._rest is None and not conn.send_lock.locked()
