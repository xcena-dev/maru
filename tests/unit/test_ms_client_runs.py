# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruClient across node runs: a credit of an ended run, and pools being
installed by an attach."""

import threading
from concurrent.futures import Future
from types import SimpleNamespace

import pytest

from maru_common.config import MaruConfig
from maru_common.node_protocol import AttachResponse, Budgets, NodeMessageType, Notify
from maru_common.storage_types import (
    DaxMapping,
    Location,
    NotifyKind,
    PoolDescriptor,
    PoolInfo,
    PoolKind,
    Status,
)
from maru_handler.client import CreditUnavailableError, maru_client
from maru_handler.client.maru_client import MaruClient
from maru_handler.client.pool_maps import PoolMaps
from maru_shm.types import MaruHandle
from maru_storage_service.drivers import dram

M = 1 << 20


class FakeMapping:
    cuda_registered = False

    def __init__(self):
        self.views = 0
        self.closed = False

    def view(self, offset, length):
        self.views += 1
        return memoryview(bytearray(length))

    def page_lock(self):
        pass

    def close(self):
        self.closed = True


@pytest.fixture
def pool():
    p = dram.create_dram_pool("n", "e", 2 * M, M, hugepage=False)
    yield p
    p.close()


@pytest.fixture
def client():
    c = MaruClient(
        MaruConfig(
            maru_node="/nonexistent/node.sock",
            engine_id="e",
            cache_namespace="ns",
            auto_connect=False,
            pin_pools=False,
        )
    )
    c.pools = PoolMaps(pin_pools=False)
    c._ensure = lambda data=True: None
    c._storage = type("Conn", (), {"closed": False})()
    c._writes = True
    return c


def test_a_credit_of_an_ended_run_gets_no_view_of_the_new_runs_pool(client, pool):
    old, new = FakeMapping(), FakeMapping()
    info = pool.info()
    client.pools._install(info, old, "e1")
    client.node_epoch = "e1"
    credit = Location(info.pool_id, "a1", 1, 0, M, PoolKind.DRAM)
    client.credits.reset("e1")
    assert client.credits.put([credit], "e1")
    # the run ends, its mapping is closed and the next run maps the same pool
    assert client.pools.remove(info.pool_id)
    assert client.pools.flush(5)
    client.pools._install(info, new, "e2")
    client.node_epoch = "e2"
    with pytest.raises(CreditUnavailableError):
        client.alloc(10)
    assert new.views == 0 and not client._handles
    assert client._counters()["store_skipped_stale_run"] == 1


def test_an_attach_in_progress_keeps_its_new_mappings(client, pool):
    info = pool.info()
    client.node_epoch = "e1"
    client.pools._install(info, FakeMapping(), "e2")  # installed, not switched yet
    client._attaching_epoch = "e2"
    client._unmap_unused_old_pools()  # a read released meanwhile
    assert client.pools.get(info.pool_id) is not None
    client._attaching_epoch = None
    client.node_epoch = "e2"
    client._unmap_unused_old_pools()
    assert client.pools.get(info.pool_id) is not None


def test_removing_leaves_a_mapping_installed_since(client, pool):
    info = pool.info()
    maps = client.pools
    first = maps._install(info, FakeMapping(), "e1")
    second = maps._install(info, FakeMapping(), "e2")
    assert maps.remove(info.pool_id, expected=first)  # nothing to do
    assert maps.get(info.pool_id) is second
    assert maps.remove(info.pool_id, expected=second)
    assert maps.get(info.pool_id) is None


class Busy(SimpleNamespace):
    __hash__ = object.__hash__


def test_a_pool_remove_of_an_ended_run_leaves_the_new_runs_mapping(client, pool):
    class Conn:
        closed = False

        def __init__(self):
            self.sent = []

        def submit(self, code, request):
            self.sent.append(code)

    old_conn, new_conn = Conn(), Conn()
    info = pool.info()
    client.pools._install(info, FakeMapping(), "e1")
    client.node_epoch, client._storage = "e1", old_conn
    busy = Busy(location=Location(info.pool_id, "a", 1, 0, M, PoolKind.DRAM))
    client._handles.add(busy)
    started = threading.Event()
    real = client.credits.remove_pool
    client.credits.remove_pool = lambda pool_id: (started.set(), real(pool_id))[1]
    notify = SimpleNamespace(pool_id=info.pool_id, pool_generation=1)
    thread = threading.Thread(
        target=client._pool_remove,
        args=(notify, "e1", old_conn, client.pools.get(info.pool_id)),
    )
    thread.start()
    assert started.wait(5)
    # a new run maps the same pool_id while the notice waits on the handle
    new = FakeMapping()
    client.pools._install(info, new, "e2")
    with client._lock:
        client.node_epoch, client._storage = "e2", new_conn
        client._handles.discard(busy)
        client._cv.notify_all()
    thread.join(5)
    assert not thread.is_alive()
    assert client.pools.get(info.pool_id) is not None and not new.closed
    assert not new_conn.sent and not old_conn.sent


CXL = PoolInfo("cxl-domain-1", PoolKind.CXL, "node", 1, 2 * M, M)


class AttachConn:
    def __init__(self):
        self.closed = False
        self.sent = []

    def start_notifications(self):
        pass

    def submit(self, code, request):
        self.sent.append(code)
        future = Future()
        future.set_result(None)
        return future


def attach_response(epoch, *, pools=True):
    dax = DaxMapping(
        "domain", MaruHandle(1, 0, 2 * M, 42), node_id="node", node_epoch=epoch
    )
    return AttachResponse(
        status=Status.OK,
        node_id="node",
        node_epoch=epoch,
        page_size=M,
        budgets=Budgets(4 * M, 8 * M, M, 4 * M, 4 * M),
        pools=[PoolDescriptor(CXL, dax=dax)] if pools else [],
    )


@pytest.fixture
def attached(client):
    client.pools = PoolMaps(pin_pools=False, map_dax=lambda info, dax: FakeMapping())
    client._take_attach(AttachConn(), attach_response("e1"), [], first=True)
    yield client
    client.pools.close()


def notice():
    return Notify(kind=NotifyKind.POOL_REMOVE, pool_id=CXL.pool_id, pool_generation=1)


def test_a_delayed_notice_is_not_taken_for_the_new_runs(attached, monkeypatch):
    client = attached
    started = []

    class Deferred:
        def __init__(self, target, args, **kwargs):
            started.append((target, args))

        def start(self):
            pass

    monkeypatch.setattr(maru_client.threading, "Thread", Deferred)
    client._on_notify(notice(), client._storage)  # on the e1 connection
    monkeypatch.undo()
    old_conn = client._storage
    old_conn.closed = True
    client._on_lost(old_conn)
    client._unmap_on_loss()
    assert client.pools.flush(5)
    new_conn = AttachConn()
    client._take_attach(new_conn, attach_response("e2"), [], first=False)
    new = client.pools.get(CXL.pool_id)
    assert new is not None and new.node_epoch == "e2"
    target, args = started[0]
    target(*args)  # the thread runs late
    assert client.pools.get(CXL.pool_id) is new and not new.mapping.closed
    assert not new_conn.sent


def test_no_ack_while_a_detached_mapping_is_still_closing(attached, monkeypatch):
    client = attached
    monkeypatch.setattr(maru_client, "CLOSE_TIMEOUT_S", 0.2)
    entered, resume = threading.Event(), threading.Event()
    old = client.pools.get(CXL.pool_id)
    real_close = old.mapping.close

    def blocked_close():
        entered.set()
        assert resume.wait(5)
        real_close()

    old.mapping.close = blocked_close
    old_conn = client._storage
    old_conn.closed = True
    client._on_lost(old_conn)
    client._unmap_on_loss()
    try:
        assert entered.wait(5)
        conn = AttachConn()
        client._take_attach(conn, attach_response("e1", pools=False), [], first=False)
        args = (notice(), client.node_epoch, client._storage, None)
        client._pool_remove(*args)
        assert not conn.sent  # the detached mapping is not closed yet
        resume.set()
        assert client.pools.flush(5)
        client._pool_remove(*args)
        assert conn.sent == [NodeMessageType.UNMAP_ACK]
    finally:
        resume.set()


def test_a_late_callback_of_an_ended_connection_is_ignored(client, monkeypatch):
    from maru_handler.client.storage_conn import StorageConn

    client.pools = PoolMaps(pin_pools=False, map_dax=lambda info, dax: FakeMapping())
    entered, resume = threading.Event(), threading.Event()

    def delayed(notify, conn):
        entered.set()  # the notifier took the notice, nothing captured yet
        assert resume.wait(5)
        client._on_notify(notify, conn)

    old_conn = StorageConn(
        "/nonexistent/node.sock", on_notify=delayed, on_lost=client._on_lost
    )
    client._take_attach(old_conn, attach_response("e1"), [], first=True)
    old_conn._notices.append(notice())
    notifier = threading.Thread(target=old_conn._notify_loop)
    try:
        notifier.start()
        assert entered.wait(5)
        old_conn._fail("simulated receiver EOF")
        assert old_conn.closed and client._storage is None
        client._unmap_on_loss()
        assert client.pools.flush(5)
        new_conn = AttachConn()
        client._take_attach(new_conn, attach_response("e2"), [], first=False)
        new = client.pools.get(CXL.pool_id)
        assert new is not None and new.node_epoch == "e2"
        resume.set()
        notifier.join(5)
        assert not notifier.is_alive()
        assert client.pools.flush(5)
        assert client.pools.get(CXL.pool_id) is new and not new.mapping.closed
        assert not new_conn.sent
    finally:
        resume.set()
        notifier.join(5)
        client.pools.close()


def test_catalog_close_waits_for_a_live_read_socket():
    import zmq

    from maru_common.ms_rpc import MsRpcClient, RpcUnknownError
    from maru_common.protocol import MessageType, ProbeRequest
    from tests.unit.ms_node_helpers import wait_until

    context = zmq.Context()
    router = context.socket(zmq.ROUTER)
    port = router.bind_to_random_port("tcp://127.0.0.1")
    rpc = MsRpcClient(f"tcp://127.0.0.1:{port}", timeout_s=5)
    entries, errors = [], []

    def read():
        try:
            rpc.prepare_thread()
            entries.append(rpc._direct_local.entry)
            assert entries[-1][0].poll(1000, zmq.POLLOUT)
            rpc.call(MessageType.PROBE, ProbeRequest("s", "e", []))
        except BaseException as e:
            errors.append(e)

    thread = threading.Thread(target=read)
    thread.start()
    try:
        wait_until(lambda: entries and entries[0][1].locked())
        rpc._fill_direct_spares()
        assert thread.is_alive() and not entries[0][0].closed
        rpc.close()
        thread.join(1)
        assert not thread.is_alive()
        assert len(errors) == 1 and isinstance(errors[0], RpcUnknownError)
        assert entries[0][0].closed
        assert not rpc._direct_sockets and not rpc._direct_owners
    finally:
        rpc.close()
        thread.join(1)
        router.close(linger=0)
        context.term()


@pytest.mark.parametrize(
    "inherited", [False, True], ids=["new-client", "inherited-client"]
)
def test_fork_resets_deadlines_and_rejects_inherited_rpc(inherited):
    import subprocess
    import sys
    import textwrap

    script = textwrap.dedent("""
        import os
        import signal
        import sys
        import threading
        import time

        from maru_common.ms_rpc import MsRpcClient, RpcUnknownError
        from maru_common.protocol import MessageType, ProbeRequest
        from maru_handler.client import maru_client

        rpc = MsRpcClient("tcp://127.0.0.1:1", timeout_s=0.05)
        parent_queue = maru_client._deadlines()
        parent_queue.schedule(time.monotonic() + 60, lambda: None)
        locked, release = threading.Event(), threading.Event()

        def hold():
            with maru_client._deadline_lock, rpc._lock, rpc._serializer._seq_lock:
                locked.set()
                release.wait(5)

        holder = threading.Thread(target=hold)
        holder.start()
        assert locked.wait(2)
        pid = os.fork()
        if pid == 0:
            signal.alarm(3)
            try:
                assert maru_client._deadline_queue is None
                queue = maru_client._deadlines()
                assert queue is not parent_queue and not queue._heap
                fired = threading.Event()
                queue.schedule(time.monotonic(), fired.set)
                assert fired.wait(1)
                request = ProbeRequest(session="s", server_epoch="e", items=[])
                if sys.argv[1] == "True":
                    for action in (
                        lambda: rpc.call(MessageType.PROBE, request),
                        rpc.prepare_thread,
                        rpc.close,
                    ):
                        try:
                            action()
                        except RpcUnknownError as e:
                            assert "after fork" in str(e)
                        else:
                            raise AssertionError("inherited RPC accepted")
                else:
                    fresh = MsRpcClient(rpc.url, timeout_s=0.05)
                    try:
                        fresh.call(MessageType.PROBE, request)
                    except RpcUnknownError as e:
                        assert "after fork" not in str(e)
                    else:
                        raise AssertionError("offline RPC succeeded")
                    fresh.close()
            except BaseException:
                import traceback
                traceback.print_exc()
                os._exit(1)
            os._exit(0)
        release.set()
        holder.join(2)
        _, status = os.waitpid(pid, 0)
        rpc.close()
        assert os.waitstatus_to_exitcode(status) == 0, status
    """)
    result = subprocess.run(
        [sys.executable, "-c", script, str(inherited)],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.fixture
def local_client():
    from maru_handler import MaruHandler
    from tests.unit.ms_fake_server import FakeMaruServer
    from tests.unit.ms_node_helpers import (
        node_config,
        remove_tree,
        short_tmpdir,
        start_node,
    )

    fake = FakeMaruServer(heartbeat_s=0.1)
    directory = short_tmpdir()
    node = start_node(node_config(fake.url, f"{directory}/node.sock"))
    handler = MaruHandler(
        MaruConfig(
            maru_node=node.config.socket,
            engine_id="reader",
            cache_namespace="ns",
            chunk_size_bytes=node.config.page_size,
            auto_connect=False,
            load_only=True,
            pin_pools=False,
        )
    )
    try:
        handler.connect()
        yield handler
    finally:
        handler.close()
        node.stop()
        fake.close()
        remove_tree(directory)


def test_catalog_sockets_of_finished_read_threads_are_reaped(local_client):
    import os

    from maru_common.storage_types import Key
    from tests.unit.ms_node_helpers import wait_until

    handler = local_client
    server = handler._cpu.server_conn
    rpc = server._rpc
    wait_until(lambda: len(rpc._direct_spares) == 2)
    baseline = len(rpc._direct_sockets)
    baseline_fds = len(os.listdir("/proc/self/fd"))
    live = rpc._direct_local.entry[0]
    errors, finished = [], []

    def read():
        try:
            handler.prepare_read_thread()
            finished.append(rpc._direct_local.entry[0])
            assert server.probe([Key("ns", "key")]) is not None
        except BaseException as e:
            errors.append(e)

    for _ in range(24):
        thread = threading.Thread(target=read)
        thread.start()
        thread.join(2)
        assert not thread.is_alive() and not errors
        wait_until(lambda: finished[-1].closed)
        wait_until(lambda: len(rpc._direct_spares) == 2)
        assert len(rpc._direct_sockets) == baseline
        assert len(rpc._direct_owners) == baseline - 2
        assert not live.closed
    assert all(sock.closed for sock in finished)
    wait_until(lambda: len(os.listdir("/proc/self/fd")) <= baseline_fds)


def test_catalog_reads_use_the_callers_preconnected_socket(local_client, monkeypatch):
    import zmq

    from maru_common.storage_types import Key

    handler = local_client
    server = handler._cpu.server_conn
    rpc = server._rpc
    calls = []
    send, receive = zmq.Socket.send_multipart, zmq.Socket.recv_multipart

    def record(method, sock, *args, **kwargs):
        if any(sock is direct for direct, _ in rpc._direct_sockets):
            calls.append((method.__name__, threading.get_ident()))
        return method(sock, *args, **kwargs)

    monkeypatch.setattr(
        zmq.Socket,
        "send_multipart",
        lambda sock, *a, **kw: record(send, sock, *a, **kw),
    )
    monkeypatch.setattr(
        zmq.Socket,
        "recv_multipart",
        lambda sock, *a, **kw: record(receive, sock, *a, **kw),
    )
    errors = []
    preconnected = {id(sock) for sock, _ in rpc._direct_spares}

    def reader():
        try:
            handler.prepare_read_thread()
            sock = rpc._direct_local.entry[0]
            assert id(sock) in preconnected
            assert sock not in [entry[0] for entry in rpc._direct_spares]
            assert server.probe([Key("ns", "key")]) is not None
            assert server.lookup_and_pin([Key("ns", "key")]) is not None
            assert rpc._direct_local.entry[0] is sock
        except BaseException as e:
            errors.append(e)

    thread = threading.Thread(target=reader)
    thread.start()
    thread.join(3)
    assert not thread.is_alive() and not errors
    assert (
        calls
        == [("send_multipart", thread.ident), ("recv_multipart", thread.ident)] * 2
    )
