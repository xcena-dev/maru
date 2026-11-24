# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageService through its UDS and metadata boundaries."""

import dataclasses
import grp
import os
import socket
import stat
import threading
import time
import weakref

import pytest

from maru_common.config import MaruConfig
from maru_common.ms_rpc import PLACEHOLDER_OP, MsRpcClient
from maru_common.node_protocol import (
    CreditRequest,
    CreditReturnRequest,
    DetachRequest,
    NodeMessageType,
    Notify,
)
from maru_common.protocol import LookupAndPinRequest, MessageType
from maru_common.storage_types import (
    BlockRequest,
    CloseMode,
    ItemOutcome,
    ItemResult,
    KeptCause,
    Key,
    KeyLocation,
    Location,
    MissReason,
    NotifyKind,
    PoolInfo,
    PoolKind,
    Status,
    StorageUnavailableError,
    WorkerRef,
    make_generation,
)
from maru_handler import MaruHandler
from maru_handler.client import CreditUnavailableError
from maru_storage_service.gpu.local_server import LocalServerError
from maru_storage_service.gpu.worker_registry import process_key
from maru_storage_service.pool_manager import PageState
from maru_storage_service.service import StorageService
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

NS = "ns"


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
    started = []

    def make(**kwargs):
        options = {k: kwargs.pop(k) for k in list(kwargs) if k in ("op_window",)}
        service = start_node(
            node_config(fake.url, f"{sockdir}/node.sock", **kwargs), **options
        )
        started.append(service)
        return service

    yield make
    for service in started:
        service.stop()


def page_of(node, key: str):
    for pool_id in node.pools.pool_ids():
        for ref in node.pools.pages_in(pool_id, PageState.READY):
            page = node.pools.page(ref)
            if page is not None and page.key == Key(NS, key):
                return ref, page
    return None, None


class TestClientRobustness:
    def test_no_credit_for_an_ended_worker(self, nodes):
        node = nodes()
        raw = RawWorker(node.config.socket)
        try:
            raw.attach()
            node.manager.workers.get(raw.incarnation).exited = True
            r = raw.credit(PAGE)
            assert r.status.value == "REJECTED"
            assert node.pools.held_bytes(raw.incarnation) == 0
        finally:
            raw.close()

    def test_keeps_attaching_after_quick_breaks_and_errors(
        self, nodes, handlers, monkeypatch
    ):
        import logging

        records = []
        handler = logging.Handler(logging.WARNING)
        handler.emit = records.append
        log = logging.getLogger("maru_handler.client.maru_client")
        log.addHandler(handler)
        node = nodes()
        h = handlers(node)
        client = h._cpu
        real = client._take_attach
        calls = []

        def flaky(*args, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                raise OSError("mmap failed")  # not a StorageUnavailableError
            return real(*args, **kwargs)

        def break_and_wait():
            old = client._storage
            for conn in node.local.connections():
                conn.close()
            wait_until(
                lambda: (
                    (c := client._storage) is not None and c is not old and not c.closed
                ),
                what="attached again",
            )

        monkeypatch.setattr(client, "_take_attach", flaky)
        break_and_wait()
        assert len(calls) >= 2  # the error did not stop the keeper
        log.removeHandler(handler)
        assert any("cannot attach" in r.getMessage() for r in records)
        for _ in range(3):
            break_and_wait()
        wait_credit(h)
        assert put(h, "after-breaks")

    def test_connect_can_be_retried(self, fake, nodes, sockdir):
        node = nodes()
        handler = MaruHandler(
            MaruConfig(
                maru_node=node.config.socket,
                engine_id="retry",
                cache_namespace=NS,
                chunk_size_bytes=PAGE,
                pool_size=PAGE,
                pin_pools=False,
                timeout_ms=300,
                auto_connect=False,
            )
        )
        fake.drop_requests(MessageType.WORKER_OPEN)
        with pytest.raises(StorageUnavailableError):
            handler.connect()
        assert handler.connect()
        wait_credit(handler)
        assert put(handler, "retried")
        handler.close()

    def test_busy_ops_are_sent_again(self, fake, nodes, handlers):
        node = nodes(op_window=1)
        h = handlers(node)
        client = h._cpu
        wait_credit(h, 4)
        fake.pause()
        handle = h.alloc(4)
        handle.buf[:] = b"busy"
        out = []
        t = threading.Thread(target=lambda: out.append(h.store("busy", handle)))
        t.start()
        wait_until(
            lambda: node.pools.bytes_in(PageState.PENDING) == PAGE, what="pending"
        )
        client.credits.take_all()  # force a CREDIT while the WRITE_DONE runs
        client._refill_wake.set()
        time.sleep(0.5)  # the CREDIT gets BUSY (one op at a time) and waits
        assert len(client._ops.unresolved()) == 2
        fake.resume()
        t.join(10)
        assert out == [True]
        wait_until(lambda: not client._ops.unresolved(), what="the BUSY op resolved")
        wait_credit(h)

    def test_failed_pool_map_closes_the_memfd_once(self, nodes, handlers, monkeypatch):
        import os

        from maru_common.node_protocol import AttachResponse
        from maru_common.storage_types import PoolDescriptor
        from maru_handler.client import pool_maps, storage_conn

        node = nodes()
        h = handlers(node)
        client = h._cpu
        r, w = os.pipe()
        os.close(w)
        closed = []
        real_close = os.close

        def counting_close(fd):
            if fd == r:
                closed.append(fd)
            real_close(fd)

        monkeypatch.setattr(pool_maps.os, "close", counting_close)
        monkeypatch.setattr(storage_conn.os, "close", counting_close)
        info = dataclasses.replace(
            node.tier("dram").pool.info(), pool_id="dram:gpu-test:other"
        )
        reply = AttachResponse(
            status=client.attach_info.status,
            node_id="gpu-test",
            node_epoch="another-run",
            server="x",
            page_size=PAGE,
            pools=[PoolDescriptor(pool=info, memfd=0)],
            budgets=client.attach_info.budgets,
        )

        class Conn:
            def close(self):
                pass

        with pytest.raises(pool_maps.LocationError):
            client._take_attach(Conn(), reply, [r], first=False)
        assert closed == [r]


class TestConfig:
    def test_maru_node_needs_identity(self):
        with pytest.raises(ValueError, match="engine_id"):
            MaruConfig(maru_node="/tmp/x.sock")
        with pytest.raises(ValueError, match="tp_rank"):
            MaruConfig(
                maru_node="/tmp/x.sock", engine_id="e", cache_namespace="n", tp_rank=-1
            )
        probe = MaruConfig(
            maru_node="/tmp/x.sock",
            engine_id="e",
            cache_namespace="n",
            metadata_only=True,
        )
        assert probe.metadata_only
        config = MaruConfig(maru_node="/tmp/x.sock", engine_id="e", cache_namespace="n")
        assert config.maru_node == "/tmp/x.sock"
        # The StorageService sizes the pool
        assert MaruConfig(
            maru_node="/tmp/x.sock",
            engine_id="e",
            cache_namespace="n",
            chunk_size_bytes=1 << 30,
            pool_size=1,
        )


def test_kept_cause_reopen_table():
    from maru_storage_service.gpu.local_pin import REOPEN_ON_KEPT

    assert REOPEN_ON_KEPT == {
        KeptCause.PINNED: True,
        KeptCause.MOVED: False,
        KeptCause.INVALID: False,
    }
    assert ItemResult.KEPT


class TestNodeStart:
    def test_socket_mode_and_group(self, fake, sockdir):
        group = grp.getgrgid(os.getgid()).gr_name
        config = dataclasses.replace(
            node_config(fake.url, f"{sockdir}/g.sock"), socket_group=group
        )
        service = start_node(config)
        try:
            st = os.stat(config.socket)
            assert stat.S_IMODE(st.st_mode) == 0o660  # never world-writable
            assert st.st_gid == grp.getgrnam(group).gr_gid
        finally:
            service.stop()
        assert not os.path.exists(config.socket)

    def test_socket_group_must_exist(self, fake, sockdir):
        from maru_storage_service.config import NodeConfigError, parse_node_config

        raw = {
            "node_id": "n",
            "server": fake.url,
            "page_size": PAGE,
            "socket": f"{sockdir}/x.sock",
            "socket_group": "no-such-group-maru",
            "tiers": [{"store": "dram", "size": 16 * PAGE, "hugepage": False}],
        }
        with pytest.raises(NodeConfigError, match="socket_group"):
            parse_node_config(raw)


class TestRepliesAndResults:
    def test_a_refused_worker_exit_is_sent_again(self, fake, nodes):
        node = nodes()
        fake.fail_requests(MessageType.WORKER_EXIT, Status.REJECTED)
        node.manager._queue_exit(
            WorkerRef("e", 0, 0, 1, 1, "gone-worker", worker_session=None)
        )
        wait_until(
            lambda: any(r.incarnation == "gone-worker" for r, _ in fake.worker_exits),
            what="WORKER_EXIT retried",
        )

    def test_two_replies_to_one_op_run_its_result_once(
        self, nodes, handlers, monkeypatch
    ):
        node = nodes()
        h = handlers(node)
        client = h._cpu
        wait_credit(h)
        real_credit = node.manager.credit

        def slow_credit(rec, decoded):
            time.sleep(0.3)  # both sends are out before the first reply
            return real_credit(rec, decoded)

        monkeypatch.setattr(node.manager, "credit", slow_credit)
        monkeypatch.setattr(client, "_refill_once", lambda: False)  # no other CREDIT
        client._refill_wake.clear()
        client.credits.take_all()
        ops = client._ops
        request = ops.origin.stamp(
            CreditRequest(
                op=PLACEHOLDER_OP,
                worker_session=client.server_conn.session,
                want_bytes=2 * PAGE,
            )
        )
        got = []

        def on_result(response, storage):
            assert storage is client._storage
            got.append(response)
            client.credits.put(response.credits, ops.node_epoch)

        first = client._send_op(NodeMessageType.CREDIT, request, on_result, ops)
        second = client._send_op(NodeMessageType.CREDIT, request, on_result, ops, True)
        first.result(5)
        second.result(5)  # both replies arrive
        time.sleep(0.2)
        assert len(got) == 1  # the replay's reply is dropped
        keys = [(c.pool_id, c.offset, c.generation) for c in client.credits.take_all()]
        assert len(keys) == len(set(keys)) == len(got[0].credits)

    def test_credit_cache_never_holds_a_page_twice(self):
        from maru_handler.client.credit_cache import CreditCache

        cache = CreditCache()
        cache.reset("e")
        loc = Location("p", "a", 1, 0, PAGE, PoolKind.DRAM)
        cache.put([loc, loc], "e")
        cache.put([loc], "e")
        assert len(cache) == 1

    def test_duplicate_pin_from_maruserver(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        assert put(h, "a")
        wait_credit(h)
        assert put(h, "b")

        def same_pin(response):
            first = response.items[0].pin_id
            return dataclasses.replace(
                response,
                items=[
                    response.items[0],
                    dataclasses.replace(response.items[1], pin_id=first),
                ],
            )

        fake.rewrite_responses(MessageType.LOOKUP_AND_PIN, same_pin)
        reads = h.batch_retrieve(["a", "b"])
        assert reads[0] is not None and reads[1] is None
        assert h._cpu.pins.holds(reads[0].pin_id)  # not unpinned by the duplicate
        assert bytes(reads[0].view) == b"a"
        h.release_retrieved(reads)

    def test_unpin_of_another_sessions_pin_is_not_retried(self, fake, nodes, handlers):
        node = nodes()
        a = handlers(node)
        b = handlers(node, engine="b")
        assert put(a, "k")
        read = a.retrieve("k")
        b._cpu.server_conn.unpin([read.pin_id])  # not b's pin
        wait_until(lambda: b._cpu.server_conn.pending_unpins() == 0, what="answered")
        assert fake.pin_count(a._cpu.identity.incarnation) == 1
        a.release_retrieved([read])

    def test_unresolved_lookups_are_capped(self, fake, nodes, handlers, monkeypatch):
        from maru_handler.client.server_conn import MAX_UNRESOLVED_LOOKUPS

        node = nodes()
        h = handlers(node, timeout_ms=60)
        assert put(h, "k")
        conn = h._cpu.server_conn
        fake.pause(hold=False)
        for _ in range(5):
            assert h.retrieve("k") is None
        # Once one is unanswered the rest are not sent while it is down
        assert conn.down and conn.unresolved_lookups() == 1
        # Without that, the cap still bounds them
        monkeypatch.setattr(conn, "_set_down", lambda *args, **kwargs: None)
        conn._down = False
        for _ in range(MAX_UNRESOLVED_LOOKUPS + 5):
            assert h.retrieve("k") is None
        assert conn.unresolved_lookups() == MAX_UNRESOLVED_LOOKUPS
        fake.resume()
        wait_until(lambda: conn.unresolved_lookups() == 0, timeout=20, what="resolved")
        wait_until(lambda: fake.pin_count() == 0, what="late pins released")


class TestClientLifecycle:
    def test_detach_holding_credit_waits_for_the_exit(self, nodes):
        node = nodes()
        raw = RawWorker(node.config.socket)
        try:
            raw.attach()
            assert raw.credit(PAGE).credits
            assert raw.request(NodeMessageType.DETACH, DetachRequest()).status == (
                Status.OK
            )
            rec = node.manager.workers.get(raw.incarnation)
            assert rec is not None and rec.detached
            assert node.manager.watcher.watching(
                process_key(rec.ident.pid, rec.ident.pid_start)
            )
            # a detached record gets no credit
            assert raw.credit(PAGE).status == Status.REJECTED
        finally:
            raw.close()

    def test_reattach_while_a_detach_is_forgotten_keeps_its_watch(
        self, nodes, monkeypatch
    ):
        node = nodes()
        manager = node.manager
        first = RawWorker(node.config.socket)
        response = first.attach()
        key = process_key(response.pid, response.pid_start)
        second = RawWorker(node.config.socket, incarnation=first.incarnation)
        attached = threading.Event()
        unwatch = manager.watcher.unwatch

        def reattach_meanwhile(watched: str) -> None:
            if watched == key and not attached.is_set():
                # the same incarnation attaches again while it is forgotten
                threading.Thread(
                    target=lambda: (second.attach(), attached.set()), daemon=True
                ).start()
                attached.wait(0.5)
            unwatch(watched)

        monkeypatch.setattr(manager.watcher, "unwatch", reattach_meanwhile)
        try:
            assert first.request(NodeMessageType.DETACH, DetachRequest()).status == (
                Status.OK
            )
            wait_until(attached.is_set, what="attached again")
            rec = manager.workers.get(first.incarnation)
            assert rec is not None and not rec.detached
            assert manager.watcher.watching(key)
        finally:
            first.close()
            second.close()

    def test_op_sent_again_after_a_clean_detach(self, nodes):
        node = nodes()
        raw = RawWorker(node.config.socket)
        again = RawWorker(node.config.socket, incarnation=raw.incarnation)
        try:
            raw.attach()
            request = raw.stamp(
                CreditRequest(op=PLACEHOLDER_OP, worker_session="s", want_bytes=PAGE)
            )
            got = raw.request(NodeMessageType.CREDIT, request)
            assert got.status == Status.OK and got.credits
            returned = CreditReturnRequest(credits=got.credits)
            assert raw.request(NodeMessageType.CREDIT_RETURN, returned).status == (
                Status.OK
            )
            assert raw.request(NodeMessageType.DETACH, DetachRequest()).status == (
                Status.OK
            )
            wait_until(lambda: node.manager.workers.get(raw.incarnation) is None)
            # on the detached connection: not attached
            late = raw.request(NodeMessageType.CREDIT, request)
            assert late.status == Status.REJECTED and late.error == "not attached"
            # attached again: a fresh record and op ledger run it as new
            again.attach()
            fresh = again.request(NodeMessageType.CREDIT, request)
            assert fresh.status == Status.OK and fresh.credits
        finally:
            raw.close()
            again.close()

    def test_failed_connect_leaks_no_connection(self, fake, nodes):
        node = nodes()

        def rpc_threads():
            return [t for t in threading.enumerate() if t.name == "maru-worker-rpc"]

        before = len(rpc_threads())
        handler = MaruHandler(
            MaruConfig(
                maru_node=node.config.socket,
                engine_id="leak",
                cache_namespace=NS,
                chunk_size_bytes=PAGE,
                pool_size=PAGE,
                pin_pools=False,
                timeout_ms=300,
                auto_connect=False,
            )
        )
        fake.fail_requests(MessageType.WORKER_OPEN, Status.REJECTED)
        with pytest.raises(StorageUnavailableError):
            handler.connect()
        assert len(rpc_threads()) == before
        assert handler.connect()
        handler.close()

    def test_close_with_open_allocations_says_no_goodbye(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        wait_credit(h)
        held = h.alloc(4)  # a copy may still be running into it
        incarnation = h._cpu.identity.incarnation
        h.close()
        assert fake.worker_session_of(incarnation) is not None  # no WORKER_CLOSE
        assert not fake.requests(MessageType.WORKER_CLOSE)
        assert not node.manager.workers.get(incarnation).detached  # no DETACH
        del held

    def test_close_keeps_the_pools_of_writing_handles(self, nodes, handlers):
        from maru_handler.client import pool_maps

        torch = pytest.importorskip("torch")
        node = nodes()
        writer = handlers(node, engine="w", pin_pools=True)
        idle = handlers(node, engine="i", pin_pools=True)
        wait_credit(writer)
        held = writer.alloc(4)  # a GPU copy may still write into it
        kept = writer._cpu.pools.get(held.location.pool_id)
        released = idle._cpu.pools.get(held.location.pool_id)
        assert kept is not None and released is not None
        registered = torch.cuda.is_available()
        assert (
            kept.mapping.cuda_registered
            == released.mapping.cuda_registered
            == registered
        )
        writer.close()
        idle.close()
        try:
            # mapped and registered until the process ends; the view works
            assert kept in pool_maps._KEPT_UNTIL_EXIT
            assert (
                not kept.mapping._closed and kept.mapping.cuda_registered == registered
            )
            held.buf[:2] = b"ok"
            # a pool without a writing handle is released
            assert released.mapping._closed and not released.mapping.cuda_registered
            assert writer._cpu.pools.pool_ids() == idle._cpu.pools.pool_ids() == []
        finally:
            pool_maps._KEPT_UNTIL_EXIT.remove(kept)
            held.buf.release()
            kept.mapping.close()

    def test_clean_closes_leave_no_records_or_fds(self, nodes, handlers):
        node = nodes(pages=64)
        manager = node.manager

        def left() -> tuple:
            return (
                len(manager.workers._workers),
                len(manager.watcher._fds),
                manager.ledger.origins(),
                len(node.pools._held),
            )

        def cycle(engine: str) -> None:
            h = handlers(node, engine=engine)
            wait_credit(h)
            assert put(h, engine)
            h.close()

        cycle("warm-up")
        wait_until(lambda: left() == (0, 0, 0, 0), what="first forgotten")
        fds = len(os.listdir("/proc/self/fd"))
        for i in range(20):
            cycle(f"e{i}")
        wait_until(lambda: left() == (0, 0, 0, 0), what="all forgotten")
        assert len(os.listdir("/proc/self/fd")) <= fds + 2

    def test_late_reattach_after_close_installs_nothing(self, nodes, handlers):
        node = nodes()
        h = handlers(node)
        client = h._cpu
        # a re-attach that close() gave up waiting for
        conn, response, fds = client._attach(5.0)
        h.close()
        with pytest.raises(StorageUnavailableError, match="closed"):
            client._take_attach(conn, response, fds, first=False)
        assert conn.closed and client._storage is None
        assert client.pools.pool_ids() == []

    def test_close_while_a_reattach_maps_pools_unmaps_them(
        self, nodes, handlers, monkeypatch
    ):
        node = nodes()
        h = handlers(node)
        client = h._cpu
        conn, response, fds = client._attach(5.0)
        h.close()
        client._closing.clear()  # close() lands while the pools are mapped
        add = client.pools.add

        def add_then_close(*args):
            mapped = add(*args)
            client._closing.set()
            return mapped

        monkeypatch.setattr(client.pools, "add", add_then_close)
        with pytest.raises(StorageUnavailableError, match="closed"):
            client._take_attach(conn, response, fds, first=False)
        assert conn.closed and client._storage is None
        assert client.pools.pool_ids() == []

    def test_healthcheck_and_stats(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        assert h.healthcheck()
        stats = h.get_stats()["maru_node"]
        assert stats["node_epoch"] == node.node_epoch and stats["storage_connected"]
        with pytest.raises(NotImplementedError, match="maru_node"):
            h.pin("k")

    def test_expired_worker_session_is_reopened_with_its_pins(
        self, fake, nodes, handlers
    ):
        node = nodes()
        h = handlers(node)
        assert put(h, "k")
        read = h.retrieve("k")
        conn = h._cpu.server_conn
        old = conn.session
        with fake._lock:
            fake.workers[old].open = False  # expired at MaruServer
        sent = len(fake.requests(MessageType.LOOKUP_AND_PIN))
        # EPOCH: a miss at once, not resent; the heartbeat thread reopens
        assert h.retrieve("k") is None
        assert h._cpu.stats["retrieve_unavailable"] == 1
        assert len(fake.requests(MessageType.LOOKUP_AND_PIN)) == sent + 1
        assert not conn.unresolved_lookups()
        wait_until(lambda: conn.session not in (None, old), what="reopened")
        again = h.retrieve("k")  # same incarnation: the first pin moved
        assert again is not None
        assert fake.pin_count(h._cpu.identity.incarnation) == 2
        h.release_retrieved([read, again])
        wait_until(lambda: fake.pin_count() == 0, what="both unpinned")

    def test_a_background_resend_reopens_and_sends_the_same_op(
        self, fake, nodes, handlers
    ):
        node = nodes()
        h = handlers(node)
        assert put(h, "k")
        conn = h._cpu.server_conn
        origin = conn._origin
        request = origin.stamp(
            LookupAndPinRequest(
                session=conn.session,
                server_epoch=conn.server_epoch,
                op=PLACEHOLDER_OP,
                items=[Key(NS, "k")],
            )
        )
        old = conn.session
        with fake._lock:
            fake.workers[old].open = False
        answered = conn._send_lookup(
            origin, request, conn.session_node_epoch, background=True
        )
        assert answered is not None and answered[0][0].result == ItemResult.HIT
        assert conn.session != old
        sent = [
            r
            for r in fake.requests(MessageType.LOOKUP_AND_PIN)
            if r.op.seq == request.op.seq
        ]
        assert len(sent) == 2 and sent[1].op.digest == request.op.digest
        conn.unpin([answered[0][0].pin_id])
        wait_until(lambda: fake.pin_count() == 0, what="unpinned")

    def test_a_probe_on_an_expired_session_misses_and_is_not_resent(
        self, fake, nodes, handlers
    ):
        node = nodes()
        h = handlers(node)
        assert put(h, "k")
        conn = h._cpu.server_conn
        old = conn.session
        with fake._lock:
            fake.workers[old].open = False
        sent = len(fake.requests(MessageType.PROBE))
        assert h.batch_exists(["k"]) == [False]
        assert h._cpu.stats["probe_unavailable"] == 1
        assert len(fake.requests(MessageType.PROBE)) == sent + 1
        wait_until(lambda: conn.session not in (None, old), what="reopened")
        assert h.batch_exists(["k"]) == [True]

    def test_stored_handle_is_not_freed_again(self, nodes, handlers):
        node = nodes()
        h = handlers(node)
        wait_credit(h)
        handle = h.alloc(4)
        handle.buf[:] = b"kept"
        assert h.store("s", handle)
        h.free(handle)  # stored: free() does nothing
        assert all(
            (c.offset, c.generation)
            != (handle.location.offset, handle.location.generation)
            for c in h._cpu.credits.take_all()
        )
        read = h.retrieve("s")
        assert bytes(read.view) == b"kept"
        h.release_retrieved([read])


class TestStaleRuns:
    def test_exit_between_watch_and_record_is_not_lost(self, fake, nodes, monkeypatch):
        node = nodes()

        def exit_during_watch(key, pid, pid_start):
            threading.Thread(target=node.manager._on_process_exit, args=(key,)).start()
            return True

        monkeypatch.setattr(node.manager.watcher, "watch", exit_during_watch)
        raw = RawWorker(node.config.socket)
        try:
            with pytest.raises(StorageUnavailableError):
                raw.attach()
            wait_until(
                lambda: any(
                    r.incarnation == raw.incarnation for r, _ in fake.worker_exits
                ),
                what="WORKER_EXIT",
            )
            assert node.manager.workers.get(raw.incarnation) is None
        finally:
            raw.close()

    def test_waiters_stay_bounded_across_reattaches(
        self, fake, nodes, handlers, monkeypatch
    ):
        node = nodes()
        h = handlers(node, store_timeout_s=0.2)
        client = h._cpu
        wait_credit(h)
        fake.pause()
        handle = h.alloc(1)
        handle.buf[:] = b"w"
        assert h.store("w", handle) is False
        sends = []
        real = node.manager.write_done
        monkeypatch.setattr(
            node.manager,
            "write_done",
            lambda *a, **k: (sends.append(1), real(*a, **k))[1],
        )
        for _ in range(5):  # each same-run re-attach resends the WRITE_DONE
            old = client._storage
            for conn in node.local.connections():
                conn.close()
            wait_until(
                lambda old=old: (
                    (c := client._storage) is not None and c is not old and not c.closed
                ),
                what="attached again",
            )
            time.sleep(0.05)
        assert len(sends) >= 5  # the WRITE_DONE was resent each time
        assert node.manager.ledger.waiters(client.identity.incarnation) <= 1
        fake.resume()
        wait_until(lambda: not client._ops.unresolved(), what="resolved")

    def test_nested_reopen_is_a_successful_open(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        assert put(h, "k")
        conn = h._cpu.server_conn
        current = conn.node_epoch
        with fake._lock:  # the session stands on an older run of the node
            fake.workers[conn.session].node_epoch = "older-run"
        conn.node_epoch = "older-run"
        assert h.retrieve("k") is None  # EPOCH: a miss; reopened in the background
        # WORKER_OPEN refused for the older run; reopened on the attached one
        wait_until(lambda: conn.node_epoch == current and not conn._reopen, what="open")
        read = h.retrieve("k")
        assert read is not None and bytes(read.view) == b"k"
        assert h._cpu.stats["retrieve_unavailable"] == 1
        h.release_retrieved([read])

    def test_resends_on_one_connection_all_get_the_result(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node, store_timeout_s=0.2)
        client = h._cpu
        wait_credit(h)
        fake.pause()
        handle = h.alloc(1)
        handle.buf[:] = b"r"
        assert h.store("r", handle) is False  # its WRITE_DONE is pending
        ((code, request, on_result),) = client._ops.unresolved()
        replies = [
            client._send_op(code, request, on_result, client._ops, resend=True)
            for _ in range(2)
        ]
        wait_until(
            lambda: node.manager.ledger.waiters(client.identity.incarnation) == 3,
            what="three waiters on one connection",
        )
        fake.resume()
        for reply in replies:
            response = reply.result(10)
            assert response.status == Status.OK  # no BUSY, so no resend loop
            assert response.items[0].result == ItemResult.ADDED
        wait_until(lambda: not client._ops.unresolved(), what="resolved")


class TestFakeFidelity:
    def test_op_bytes_budget_answers_busy(self, sockdir):
        from maru_common.protocol import CommitItem, CommitRequest
        from maru_common.serializer import op_digest
        from maru_common.storage_types import OpRef

        fake = FakeMaruServer(heartbeat_s=0.1, op_bytes_max=1)
        node = start_node(node_config(fake.url, f"{sockdir}/b.sock"))
        rpc = MsRpcClient(fake.url, timeout_s=2)
        try:
            loc = Location(node.tier("dram").pool.pool_id, "x", 1, 0, 8, PoolKind.DRAM)
            request = CommitRequest(
                session=node.server.session,
                server_epoch=node.server.server_epoch,
                op=PLACEHOLDER_OP,
                items=[CommitItem(key=Key(NS, "x"), location=loc, logical_bytes=8)],
            )
            digest = op_digest(request)
            statuses = []
            for seq in (1000, 1001):  # neither acknowledged
                op = OpRef(origin=node.node_epoch, seq=seq, digest=digest, ack=0)
                reply = rpc.call(
                    MessageType.COMMIT, dataclasses.replace(request, op=op)
                )
                statuses.append(reply.status)
            assert statuses == [Status.OK, Status.BUSY]
        finally:
            rpc.close()
            node.stop()
            fake.close()


@pytest.fixture
def raw(nodes):
    node = nodes()
    worker = RawWorker(node.config.socket)
    worker.attach()
    yield node, worker
    worker.close()


def test_lost_response_is_resolved_with_the_same_op(fake, raw):
    node, worker = raw
    credits = worker.credit(PAGE).credits
    fake.drop_responses(MessageType.COMMIT)
    assert worker.write_done(credits, ["k"]).items[0].result is ItemResult.ADDED
    requests = fake.requests(MessageType.COMMIT)
    assert len(requests) >= 2
    assert (
        len({r.op.seq for r in requests}) == len({r.op.digest for r in requests}) == 1
    )
    assert fake.executed[MessageType.COMMIT] == 1
    assert fake.replayed[MessageType.COMMIT] >= 1
    assert node.pools.transitions[(PageState.PENDING, PageState.READY)] == 1
    node.pools.check()


def test_lost_duplicate_frees_the_page_once(fake, raw):
    node, worker = raw
    for _ in range(2):
        credits = worker.credit(PAGE).credits
        fake.drop_responses(MessageType.COMMIT)
        answer = worker.write_done(credits, ["k"])
    assert answer.items[0].result is ItemResult.DUPLICATE
    assert node.pools.transitions[(PageState.PENDING, PageState.FREE)] == 1
    assert node.pools.bytes_in(PageState.READY) == PAGE
    node.pools.check()


@pytest.mark.parametrize("store", ["dram", "local_cxl"])
def test_a_full_gpu_store_skips_and_never_retires(fake, sockdir, store):
    from tests.unit.ms_node_helpers import FakeRm

    config = node_config(
        fake.url,
        f"{sockdir}/node.sock",
        pages=16,
        cxl_pages=16 if store == "local_cxl" else None,
    )
    node = start_node(config, region_source=FakeRm())
    worker = RawWorker(config.socket)
    try:
        response = worker.attach(capabilities=["fetch_v1", "lease_v1"])
        assert "fetch_v1" not in response.capabilities
        for i in range(15):
            credit = worker.credit(PAGE).credits
            assert len(credit) == 1
            assert (
                worker.write_done(credit, [f"k{i}"]).items[0].result is ItemResult.ADDED
            )
        before = fake.keys()
        assert worker.credit(PAGE).credits == []
        fake.pause()
        try:
            started = time.monotonic()
            answer = worker.credit(PAGE)
            assert answer.status is Status.OK and answer.credits == []
            assert time.monotonic() - started < node.rpc_timeout_s
        finally:
            fake.resume()
        for _ in range(3):
            node.manager._evict_once()
        assert fake.keys() == before and len(before) == 15
        assert not fake.requests(MessageType.RETIRE)
        assert node.pools.bytes_in(PageState.READY) == 15 * PAGE
        node.pools.check()
    finally:
        worker.close()
        node.stop()


def test_lru_retire_keeps_pinned_and_takes_new_stores(fake, nodes):
    from tests.unit.ms_node_helpers import publish

    node = nodes(role="memory", high=0.75, low=0.5)
    fake.preserved.add(Key("ns", "k0"))
    for i in range(13):
        assert publish(node, f"k{i}").result is ItemResult.ADDED
    wait_until(lambda: node.manager.retire_results.get("RETIRED", 0) >= 5)
    assert Key("ns", "k0") in fake.keys()
    assert node.manager.retire_results["KEPT:PINNED"] > 0
    assert publish(node, "new").result is ItemResult.ADDED
    node.pools.check()


def test_heartbeat_reports_usage(fake, raw):
    node, worker = raw
    credits = worker.credit(2 * PAGE).credits
    worker.write_done(credits[:1], ["k"])
    wait_until(
        lambda: (
            fake.requests(MessageType.NODE_HEARTBEAT)
            and any(
                u.ready == PAGE
                for u in fake.requests(MessageType.NODE_HEARTBEAT)[-1].usage
            )
        )
    )
    node.pools.check()


def test_a_maruserver_restart_ends_the_run(fake, raw):
    node, worker = raw
    worker.write_done(worker.credit(PAGE).credits, ["k"])
    fake.restart()
    assert node.wait_finished(10)
    assert node.new_run and "restarted" in node.ended
    assert node.pools.credit("w", PAGE) == []


def test_whole_request_rejected_commit_frees_the_pages(fake, raw):
    node, worker = raw
    fake.fail_requests(MessageType.COMMIT, Status.REJECTED)
    answer = worker.write_done(worker.credit(PAGE).credits, ["r"])
    assert answer.items[0].result is ItemResult.REJECTED
    assert node.pools.transitions[(PageState.PENDING, PageState.FREE)] == 1
    node.pools.check()


@pytest.mark.parametrize("wrong_count", [False, True])
def test_unknown_commit_results_end_the_run(fake, raw, wrong_count):
    node, worker = raw
    fake.rewrite_responses(
        MessageType.COMMIT,
        lambda r: dataclasses.replace(
            r, items=[] if wrong_count else [ItemOutcome(ItemResult.STALE)]
        ),
    )
    try:
        worker.write_done(worker.credit(PAGE).credits, ["u"])
    except StorageUnavailableError:
        pass
    assert node.wait_finished(10)
    assert node.new_run and "COMMIT" in node.ended
    assert node.pools.bytes_in(PageState.PENDING) == PAGE


def test_a_bad_retire_reply_ends_the_run(fake, raw):
    node, worker = raw
    worker.write_done(worker.credit(PAGE).credits, ["k"])
    ref, page = page_of(node, "k")
    fake.rewrite_responses(
        MessageType.RETIRE, lambda r: dataclasses.replace(r, items=[])
    )
    node.manager.retire([ref], [KeyLocation(page.key, page.location)])
    assert node.wait_finished(10) and node.new_run
    assert node.pools.page(ref).state is PageState.READY
    assert node.manager.placement.is_held(ref)


def test_credit_kind_from_another_release_is_rejected(raw):
    node, worker = raw
    request = worker.stamp(
        CreditRequest(op=PLACEHOLDER_OP, worker_session="s", want_bytes=PAGE)
    )
    payload = dataclasses.asdict(request)
    payload["kind"] = "dram"
    # The field is in the digest even though the typed request ignores it.
    from maru_common.serializer import op_digest

    payload["op"]["digest"] = op_digest(payload)
    response = worker.request(NodeMessageType.CREDIT, payload)
    assert response.status is Status.REJECTED
    assert node.pools.held_bytes(worker.incarnation) == 0


@pytest.mark.parametrize("role", ["gpu", "memory"])
@pytest.mark.parametrize("store", ["dram", "local_cxl"])
def test_storage_restart_adopts_only_local_cxl_on_the_same_boot(sockdir, role, store):
    from maru_common.storage_types import Key
    from tests.unit.ms_node_helpers import FakeRm, MetadataServer, publish

    metadata = MetadataServer()
    rm = FakeRm()
    config = node_config(
        metadata.address,
        f"{sockdir}/node.sock",
        role=role,
        cxl_pages=16 if store == "local_cxl" else None,
    )
    first = second = third = None
    try:
        first = start_node(config, region_source=rm, host_boot_id="boot1")
        assert publish(first, "k").result is ItemResult.ADDED
        first.stop()
        second = start_node(config, region_source=rm, host_boot_id="boot1")
        if store == "local_cxl":
            assert second.adopted
            assert second.pools.bytes_in(PageState.READY) == PAGE
            assert metadata.server.ms_service.catalog.locations(Key("ns", "k"))
        else:
            assert not second.adopted
            assert second.pools.bytes_in(PageState.READY) == 0
            wait_until(
                lambda: not metadata.server.ms_service.catalog.locations(Key("ns", "k"))
            )
        assert publish(second, "new").result is ItemResult.ADDED
        second.stop()
        third = start_node(config, region_source=rm, host_boot_id="boot2")
        assert third.pools.bytes_in(PageState.READY) == 0
        wait_until(
            lambda: not metadata.server.ms_service.catalog.locations(Key("ns", "new"))
        )
        third.pools.check()
    finally:
        for node in (third, second, first):
            if node is not None:
                node.stop()
        metadata.close()


def test_workers_share_one_dram_mapping(raw):
    import mmap

    node, writer = raw
    reader = RawWorker(node.config.socket)
    try:
        response = reader.attach(engine_id="other")
        assert [d.pool.pool_id for d in response.pools] == [
            d.pool.pool_id for d in writer.response.pools
        ]
        credit = writer.credit(PAGE).credits[0]
        with mmap.mmap(writer.fds[0], node.config.top.size) as src:
            src[credit.offset : credit.offset + 5] = b"bytes"
            assert (
                writer.write_done([credit], ["shared"], size=5).items[0].result
                is ItemResult.ADDED
            )
            with mmap.mmap(reader.fds[0], node.config.top.size) as dst:
                assert dst[credit.offset : credit.offset + 5] == b"bytes"
    finally:
        reader.close()


def test_idle_credit_is_asked_back_for_a_waiting_worker(raw):
    node, first = raw
    second = RawWorker(node.config.socket)
    try:
        assert len(first.credit(4 * PAGE).credits) == 4
        second.attach(engine_id="other")
        assert len(second.credit(4 * PAGE).credits) == 4
        # A third worker gets a fair share by asking idle credit back.
        third = RawWorker(node.config.socket)
        try:
            third.attach(engine_id="third")
            assert third.credit(PAGE).credits == []
            wait_until(
                lambda: any(n.kind is NotifyKind.CREDIT_RETURN for n in first.notices)
            )
        finally:
            third.close()
    finally:
        second.close()


def test_write_done_with_an_invalid_allocation_is_rejected(raw):
    node, worker = raw
    fake_credit = Location(
        node.tier("dram").pool.pool_id, "not-credited", 1, 0, PAGE, PoolKind.DRAM
    )
    answer = worker.write_done([fake_credit], ["invalid"])
    assert answer.items[0].result is ItemResult.REJECTED
    assert node.pools.bytes_in(PageState.READY) == 0


def test_memory_lru_uses_read_access_and_skips_quarantined_pools(fake, nodes):
    from maru_common.protocol import NodeHeartbeatResponse
    from maru_common.storage_types import AccessHints
    from tests.unit.ms_node_helpers import publish

    node = nodes(role="memory", high=1.0, low=0.5)
    for i in range(3):
        publish(node, f"k{i}")
    entries = node.pools.ready_pages()
    first = entries[0][2]
    node.manager.on_heartbeat(
        NodeHeartbeatResponse(
            status=Status.OK,
            access=AccessHints.pack([(first.pool_id, first.offset, first.generation)]),
        )
    )
    pool_id = first.pool_id
    candidates = node.manager.placement.candidates(
        "dram",
        [pool_id],
        capacity=3 * PAGE,
        free_bytes=0,
        retiring_bytes=0,
        page_size=PAGE,
        limit=1,
        demand_bytes=PAGE,
    )
    assert candidates == [entries[1][0]]
    node.pools.quarantine_pool(pool_id)
    assert node.manager._evictable_pools("dram") == []


def test_retire_results_by_cause(fake, nodes):
    node = nodes()
    from tests.unit.ms_node_helpers import publish

    names = ("gone", "away", "here", "taken", "lost", "invalid")
    for key in names:
        assert publish(node, key).result is ItemResult.ADDED
    refs = {key: page_of(node, key) for key in names}

    def lid(loc):
        return (loc.pool_id, loc.allocation_id, loc.generation)

    def retire(key):
        ref, page = refs[key]
        node.manager.retire([ref], [KeyLocation(page.key, page.location)])

    with fake._lock:
        # removed elsewhere: nothing published on the page
        fake._remove(fake.by_loc[lid(refs["gone"][1].location)])
        # the key lives on another page now; this page is empty
        away = refs["away"][1].location
        entry = fake.by_loc.pop(lid(away))
        entry.location = dataclasses.replace(
            away,
            allocation_id="elsewhere",
            offset=node.tier("dram").pool.capacity - PAGE,
        )
        fake.by_loc[lid(entry.location)] = entry
        # the same key at another generation occupies the page
        here = refs["here"][1].location
        entry = fake.by_loc.pop(lid(here))
        entry.location = dataclasses.replace(here, generation=here.generation + 1)
        fake.by_loc[lid(entry.location)] = entry
        # another key occupies the page
        taken = fake.by_loc[lid(refs["taken"][1].location)]
        fake.catalog[taken.key].remove(taken)
        taken.key = Key(NS, "intruder")
        fake.catalog.setdefault(taken.key, []).append(taken)
    for key in ("gone", "away", "here", "taken"):
        retire(key)
    # RETIRED (nothing occupies the page, wherever the key lives): freed
    assert node.pools.page(refs["gone"][0]).state == PageState.FREE
    assert node.pools.page(refs["away"][0]).state == PageState.FREE
    # MOVED: never freed; the page takes the occupant as READY
    page = node.pools.page(
        dataclasses.replace(refs["here"][0], generation=here.generation + 1)
    )
    assert page.state == PageState.READY and page.key == Key(NS, "here")
    taken_ref = refs["taken"][0]
    page = node.pools.page(taken_ref)
    assert page.state == PageState.READY and page.key == Key(NS, "intruder")
    assert not node.manager.placement.is_held(taken_ref)  # an ordinary page
    # MOVED without an occupant (should not happen): held, never freed
    fake.rewrite_responses(
        MessageType.RETIRE,
        lambda r: dataclasses.replace(
            r, items=[ItemOutcome(ItemResult.KEPT, cause=KeptCause.MOVED)]
        ),
    )
    retire("lost")
    ref = refs["lost"][0]
    assert node.pools.page(ref).state == PageState.READY
    assert node.manager.local_pins.is_blocked(ref)
    assert node.manager.placement.is_held(ref)
    # INVALID: the pool is not used any more
    with fake._lock:
        fake.hidden_pools.add(refs["invalid"][1].location.pool_id)
    retire("invalid")
    assert node.manager.placement.is_held(refs["invalid"][0])
    assert not node.pools.creditable(node.tier("dram").pool.pool_id)
    raw = RawWorker(node.config.socket)
    raw.attach()
    assert raw.credit(PAGE).credits == []
    raw.close()
    node.pools.check()


def test_admin_delete_through_the_fake(fake, nodes):
    from maru_common.protocol import AdminDeleteRequest

    node = nodes()
    from tests.unit.ms_node_helpers import publish

    assert publish(node, "d").result is ItemResult.ADDED
    fake.preserve(Key(NS, "keep"))
    rpc = MsRpcClient(fake.url, timeout_s=2)
    response = rpc.call(
        MessageType.ADMIN_DELETE,
        AdminDeleteRequest(items=[Key(NS, "d"), Key(NS, "keep"), Key(NS, "none")]),
    )
    rpc.close()
    assert [o.result for o in response.items] == [
        ItemResult.OK,
        ItemResult.KEPT,
        ItemResult.MISS,
    ]
    wait_until(lambda: node.pools.bytes_in(PageState.READY) == 0, what="retired")
    assert Key(NS, "d") not in fake.keys()


@pytest.mark.parametrize("store", ["dram", "local_cxl"])
def test_memory_lru_preserves_a_read_pin_then_reclaims_it(sockdir, store):
    from tests.unit.ms_fakes import FakeNode, FakeWorker, Wire
    from tests.unit.ms_node_helpers import FakeRm, MetadataServer, publish

    metadata = MetadataServer()
    node = None
    try:
        config = node_config(
            metadata.address,
            f"{sockdir}/node.sock",
            role="memory",
            high=0.75,
            low=0.5,
            cxl_pages=16 if store == "local_cxl" else None,
        )
        node = start_node(config, region_source=FakeRm())
        assert publish(node, "k0").result is ItemResult.ADDED
        reader = FakeNode(Wire(metadata.server.ms_service), "reader")
        from maru_common.storage_types import WORKER_RDMA_CAPABILITY

        reader.capabilities = [WORKER_RDMA_CAPABILITY]
        reader.open(pools=[], pid_ns=101)
        reader.heartbeat(memory_peer=(config.node_id, node.node_epoch))
        worker = FakeWorker(reader.wire, reader)
        worker.open()
        ref, page = page_of(node, "k0")
        pinned = worker.lookup(page.key).items[0]
        assert pinned.pin_id
        for i in range(1, 13):
            publish(node, f"k{i}")
        wait_until(lambda: node.manager.retire_results.get("KEPT:PINNED", 0) > 0)
        assert node.pools.page(ref).state is PageState.READY
        worker.unpin(pinned.pin_id)
        node.manager.retire([ref], [KeyLocation(page.key, page.location)])
        assert node.pools.page(ref).state is PageState.FREE
        node.pools.check()
    finally:
        if node is not None:
            node.stop()
        metadata.close()


def test_memory_capability_refusal_fails_start(sockdir):
    from maru_storage_service.service import CapabilityRefusedError, StorageService

    fake = FakeMaruServer(capabilities=frozenset())
    try:
        config = node_config(fake.url, f"{sockdir}/node.sock", role="memory")
        service = StorageService(config)
        with pytest.raises(CapabilityRefusedError):
            service.start()
        assert service.wait_finished(1) and service.teardown_failed is None
    finally:
        fake.close()


def test_exception_in_retire_leaves_nothing_in_flight(nodes, monkeypatch):
    node = nodes()
    from tests.unit.ms_node_helpers import publish

    assert publish(node, "x").result is ItemResult.ADDED
    ref, page = page_of(node, "x")
    node.manager.placement._inflight.add(ref)

    def boom(items):
        raise RuntimeError("bug")

    monkeypatch.setattr(node.server, "retire", boom)
    node.manager.retire([ref], [KeyLocation(page.key, page.location)])
    assert ref not in node.manager.placement._inflight
    assert node.manager.placement.is_held(ref)
    assert node.pools.page(ref).state == PageState.READY


def test_our_allocation_elsewhere_in_the_catalog_holds_the_page(fake, nodes):
    node = nodes()
    from tests.unit.ms_node_helpers import publish

    assert publish(node, "k").result is ItemResult.ADDED
    ref, page = page_of(node, "k")
    loc = page.location
    with fake._lock:  # the Catalog has our allocation at another offset
        entry = fake.by_loc.pop((loc.pool_id, loc.allocation_id, loc.generation))
        entry.location = dataclasses.replace(
            loc, offset=node.tier("dram").pool.capacity - PAGE
        )
        fake.by_loc[(loc.pool_id, loc.allocation_id, loc.generation)] = entry
    node.manager.retire([ref], [KeyLocation(page.key, page.location)])
    retire = fake.requests(MessageType.RETIRE)[-1]
    assert retire.items[0].location == loc
    assert node.pools.page(ref).state == PageState.READY  # never freed
    assert node.manager.placement.is_held(ref)
    assert node.manager.local_pins.is_blocked(ref)


def test_admin_delete_skips_closed_nodes(fake, nodes):
    from maru_common.protocol import AdminDeleteRequest

    node = nodes()
    from tests.unit.ms_node_helpers import publish

    assert publish(node, "gone").result is ItemResult.ADDED
    node.stop(CloseMode.RESTART)  # NODE_CLOSE(restart): its DRAM is hidden
    rpc = MsRpcClient(fake.url, timeout_s=2)
    response = rpc.call(
        MessageType.ADMIN_DELETE, AdminDeleteRequest(items=[Key(NS, "gone")])
    )
    rpc.close()
    (item,) = response.items
    assert item.result == ItemResult.MISS and item.reason == MissReason.OFFLINE


@pytest.mark.parametrize("role", ["gpu", "memory"])
@pytest.mark.parametrize("store", ["dram", "local_cxl"])
def test_second_start_leaves_the_running_node_alone(sockdir, monkeypatch, role, store):
    from maru_server.node_server import NodeServer
    from tests.unit.ms_node_helpers import FakeRm, MetadataServer, publish

    metadata = MetadataServer(NodeServer(heartbeat_s=0.05))
    rm = FakeRm()
    config = node_config(
        metadata.address,
        f"{sockdir}/node.sock",
        role=role,
        cxl_pages=16 if store == "local_cxl" else None,
    )
    node = None
    try:
        node = start_node(config, region_source=rm, host_boot_id="boot1")
        assert publish(node, "existing").result is ItemResult.ADDED
        catalog = metadata.server.ms_service.catalog
        registry = metadata.server.ms_service.registry
        locations = catalog.locations(Key(NS, "existing"))
        epoch_seq = registry.nodes[config.node_id].epoch_seq
        session = node.server.session
        rm_calls = list(rm.calls)

        def unexpected(*args, **kwargs):
            pytest.fail("second start reached pool creation or NODE_OPEN")

        with monkeypatch.context() as patch:
            patch.setattr(StorageService, "_make", unexpected)
            patch.setattr(type(node.server), "open", unexpected)
            second = StorageService(config, region_source=rm, host_boot_id="boot1")
            with pytest.raises(LocalServerError, match="another StorageService holds"):
                second.start()
        assert second.pools is None and second.server is None
        assert not second._tiers
        assert [c for c in rm.calls if c != "renew"] == [
            c for c in rm_calls if c != "renew"
        ]
        assert os.path.exists(config.socket) is (role == "gpu")
        assert (node.local is None) is (role == "memory")
        assert publish(node, "still-serving").result is ItemResult.ADDED
        node.manager.on_heartbeat(node.server.heartbeat(node.pools.usage()))
        assert not node.wait(0) and node.manager.ended is None
        assert node.server.session == session
        assert catalog.locations(Key(NS, "existing")) == locations
        assert registry.nodes[config.node_id].epoch_seq == epoch_seq == 1
        assert node.pools.bytes_in(PageState.READY) == 2 * PAGE
    finally:
        if node is not None:
            node.stop()
        metadata.close()


def test_memory_role_creates_no_worker_objects(nodes, monkeypatch):
    from maru_storage_service.gpu.local_pin import LocalPins
    from maru_storage_service.gpu.local_server import LocalServer
    from maru_storage_service.gpu.peer_watcher import PeerWatcher
    from maru_storage_service.gpu.worker_registry import WorkerRegistry
    from maru_storage_service.node_store import NodeStore
    from maru_storage_service.op_ledger import OpLedger
    from tests.unit.ms_node_helpers import publish

    def unexpected(*args, **kwargs):
        pytest.fail("memory role constructed a worker object")

    for cls in (LocalServer, WorkerRegistry, PeerWatcher, OpLedger, LocalPins):
        monkeypatch.setattr(cls, "__init__", unexpected)
    node = nodes(role="memory")
    assert type(node.store) is NodeStore
    assert node.local is None and not os.path.exists(node.config.socket)
    assert not any(
        hasattr(node.store, name) for name in ("workers", "watcher", "ledger")
    )
    assert [thread.name for thread in node.store._threads] == [
        "maru-commit",
        "maru-evict",
    ]
    assert publish(node, "memory-page").result is ItemResult.ADDED


def test_maruserver_stopped_reaches_the_cap_and_comes_back(fake, nodes):
    node = nodes(budgets={"commit_pending_max_bytes": 2 * PAGE})
    worker = RawWorker(node.config.socket)
    worker.attach()
    credits = worker.credit(2 * PAGE).credits
    answers = []
    thread = threading.Thread(
        target=lambda: answers.append(worker.write_done(credits, ["a", "b"]))
    )
    try:
        fake.pause(hold=False)
        thread.start()
        wait_until(lambda: node.pools.write_pending_bytes() == 2 * PAGE)
        assert node.pools.credit("other", PAGE) == []
        assert node.pools.credit_short["pending"] > 0
        fake.resume()
        thread.join(10)
        assert not thread.is_alive()
        assert [i.result for i in answers[0].items] == [ItemResult.ADDED] * 2
        assert worker.credit(PAGE).credits
        node.pools.check()
    finally:
        fake.resume()
        thread.join(10)
        worker.close()


def test_block_request_is_retired_by_the_node(fake, nodes):
    from tests.unit.ms_node_helpers import publish

    node = nodes()
    for k in ("b", "p"):
        publish(node, k)
    ref_b, page_b = page_of(node, "b")
    ref_p, page_p = page_of(node, "p")
    fake.preserve(Key(NS, "p"))
    fake.queue_block(
        "gpu-test",
        BlockRequest(
            block_id="blk",
            items=(
                KeyLocation(page_b.key, page_b.location),
                KeyLocation(page_p.key, page_p.location),
            ),
            reason="delete",
        ),
    )
    wait_until(lambda: node.pools.page(ref_b).state is PageState.FREE)
    wait_until(lambda: ref_p in node.manager._retire_queue)
    assert node.pools.page(ref_p).state is PageState.READY
    assert node.manager.local_pins.is_blocked(ref_p)
    assert node.manager.placement.is_held(ref_p)
    assert (
        len(
            [
                i
                for r in fake.requests(MessageType.RETIRE)
                for i in r.items
                if i.key.key == "b"
            ]
        )
        == 1
    )
    with fake._lock:
        fake.preserved.discard(Key(NS, "p"))
    wait_until(lambda: node.pools.page(ref_p).state is PageState.FREE)
    wait_until(lambda: not node.manager._blocks)
    node.pools.check()


def test_eviction_kept_does_not_reopen_a_blocked_page(fake, nodes):
    from tests.unit.ms_node_helpers import publish

    node = nodes()
    publish(node, "k")
    ref, page = page_of(node, "k")
    fake.preserve(page.key)
    with node.manager._lock:
        node.manager._central.add(ref)
    node.manager.retire([ref], [KeyLocation(page.key, page.location)])
    assert node.manager.local_pins.is_blocked(ref)
    assert ref in node.manager._retire_queue


def test_a_pool_adopted_but_not_opened_fails_the_start(fake, nodes):
    nodes().stop()
    info = PoolInfo(
        pool_id="cxl:rm:999",
        kind=PoolKind.CXL,
        node_id="gpu-test",
        pool_generation=1,
        capacity=4 * PAGE,
        page_size=PAGE,
        rm_domain="rm",
        region_id=999,
    )
    fake.set_adopted("gpu-test", [(info, [])])
    with pytest.raises(RuntimeError, match="adopted cxl:rm:999, not opened"):
        nodes()


@pytest.fixture
def handlers(nodes):
    made = []

    def make(node, engine="eng", store_timeout_s=10.0, timeout_ms=300, pin_pools=False):
        handler = MaruHandler(
            MaruConfig(
                maru_node=node.config.socket,
                engine_id=engine,
                cache_namespace=NS,
                chunk_size_bytes=PAGE,
                pool_size=PAGE,
                pin_pools=pin_pools,
                timeout_ms=timeout_ms,
                auto_connect=False,
                store_timeout_s=store_timeout_s,
            )
        )
        handler.connect()
        made.append(handler)
        return handler

    yield make
    for handler in made:
        try:
            handler.close()
        except Exception:
            pass


def put(handler, key: str, value: bytes | None = None) -> bool:
    value = value if value is not None else key.encode()
    handle = handler.alloc(len(value))
    handle.buf[:] = value
    return handler.store(key, handle)


def wait_credit(handler, pages: int = 1):
    wait_until(lambda: handler._cpu.credits.unused_bytes >= pages * PAGE, what="credit")


class TestRestart:
    def test_a_worker_attaches_to_the_new_run_at_once(self, fake, nodes, handlers):
        """Nothing waits for its copy into the old run's DRAM; the old pool
        is unmapped once that copy is released."""
        first = nodes()
        h = handlers(first)
        client = h._cpu
        assert put(h, "old")
        wait_credit(h)
        held = h.alloc(16)  # a copy into an old credit page still going on
        held.buf[:] = b"x" * 16
        first.stop()
        second = nodes()
        assert second.node_epoch != first.node_epoch
        wait_until(
            lambda: client.node_epoch == second.node_epoch, what="attached again"
        )
        # the old pool is gone from lookups; the new one takes writes
        assert h.retrieve("old") is None
        wait_credit(h)
        assert put(h, "new")
        h.free(held)
        wait_until(
            lambda: client.server_conn.session_node_epoch == second.node_epoch,
            what="worker session moved",
        )
        assert client.pools.pool_ids() == [second.tier("dram").pool.pool_id]
        read = h.retrieve("new")
        assert bytes(read.view) == b"new"
        h.release_retrieved([read])


class TestReconnect:
    def test_same_run_keeps_credit_and_resends_unresolved_writes(
        self, fake, nodes, handlers
    ):
        node = nodes()
        h = handlers(node)
        client = h._cpu
        wait_credit(h, 2)
        epoch = client.node_epoch
        fake.pause()
        handle = h.alloc(5)
        handle.buf[:] = b"hello"
        out = []
        t = threading.Thread(target=lambda: out.append(h.store("r", handle)))
        t.start()
        wait_until(
            lambda: node.pools.bytes_in(PageState.PENDING) == PAGE, what="pending"
        )
        for conn in node.local.connections():
            conn.close()  # the UDS breaks; both processes live on
        t.join(10)
        assert out == [False]  # unknown to the caller
        assert len(client._ops.unresolved()) == 1
        wait_until(lambda: client.storage_connected, what="attached again")
        assert client.node_epoch == epoch
        assert client.credits.unused_bytes > 0  # kept: the same run holds it
        fake.resume()
        wait_until(lambda: not client._ops.unresolved(), what="the same op resolved")
        assert fake.executed[MessageType.COMMIT] == 1
        read = h.retrieve("r")
        assert bytes(read.view) == b"hello"
        h.release_retrieved([read])
        node.pools.check()


class TestClientNotify:
    def test_credit_return_and_pool_remove(self, nodes, handlers):
        node = nodes()
        h = handlers(node)
        client = h._cpu
        wait_credit(h, 2)
        held = node.pools.held_bytes(client.identity.incarnation)
        client._on_notify(
            Notify(kind=NotifyKind.CREDIT_RETURN, bytes=PAGE), client._storage
        )
        wait_until(
            lambda: node.pools.held_bytes(client.identity.incarnation) < held,
            what="credit returned",
        )
        pool_id = node.tier("dram").pool.pool_id
        client._on_notify(
            Notify(kind=NotifyKind.POOL_REMOVE, pool_id=pool_id, pool_generation=1),
            client._storage,
        )
        wait_until(
            lambda: (client.identity.incarnation, pool_id, 1) in node.manager._unmapped,
            what="UNMAP_ACK",
        )
        assert client.pools.pool_ids() == []


class TestHandlerFacade:
    def test_store_without_credit_is_skipped(self, nodes, handlers, monkeypatch):
        from maru_handler.client.storage_conn import StorageConn

        h = handlers(nodes())
        wait_credit(h)
        client = h._cpu
        monkeypatch.setattr(client.credits, "put", lambda *_: False)
        client.credits.take_all()
        client._refill_wake.clear()
        caller = threading.get_ident()
        calls = []

        def record(method, *args, **kwargs):
            if threading.get_ident() == caller:
                calls.append(method.__name__)
            return method(*args, **kwargs)

        rpc_call, submit = MsRpcClient.call, StorageConn.submit
        monkeypatch.setattr(
            MsRpcClient, "call", lambda *a, **kw: record(rpc_call, *a, **kw)
        )
        monkeypatch.setattr(
            StorageConn, "submit", lambda *a, **kw: record(submit, *a, **kw)
        )

        def no_wait(*args, **kwargs):
            raise AssertionError("empty credit allocation waited")

        monkeypatch.setattr(client.credits._added, "wait", no_wait)
        assert client.config.credit_wait_s == 0
        before = client.stats["store_skipped_no_credit"]
        with pytest.raises(CreditUnavailableError) as err:
            h.alloc(4)
        assert isinstance(err.value, ValueError)
        assert client.credits.unused_bytes == 0
        assert client.stats["store_skipped_no_credit"] == before + 1
        assert not calls and not client._handles

    def test_methods(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        wait_credit(h, 2)
        with pytest.raises(ValueError):
            h.alloc(PAGE + 1)
        unused = h._cpu.credits.unused_bytes
        handle = h.alloc(3)
        h.free(handle)
        assert h._cpu.credits.unused_bytes == unused  # the credit is reused
        h.free(handle)  # freeing twice does nothing
        assert put(h, "single", b"one")
        a, b = h.alloc(3), h.alloc(3)
        a.buf[:] = b"aaa"
        b.buf[:] = b"bbb"
        assert h.batch_store(["a", "single"], [a, b]) == [
            True,
            True,
        ]  # DUPLICATE is True
        assert h.exists("a") and not h.exists("nope")
        assert h.batch_exists(["a", "nope", "single"]) == [True, False, True]
        reads = h.batch_retrieve(["a", "nope", "single"])
        assert reads[1] is None
        assert bytes(reads[0].view) == b"aaa" and bytes(reads[2].view) == b"one"
        assert fake.pin_count(h._cpu.identity.incarnation) == 2
        unpins = len(fake.requests(MessageType.UNPIN))
        h.release_retrieved(reads)
        wait_until(lambda: fake.pin_count() == 0, what="unpinned")
        assert len(fake.requests(MessageType.UNPIN)) == unpins + 1  # one UNPIN
        h.release_retrieved(reads)  # each pin is released once
        assert len(fake.requests(MessageType.UNPIN)) == unpins + 1
        with h.retrieve("a") as read:  # the lease is a context manager
            assert bytes(read.view) == b"aaa"
        wait_until(lambda: fake.pin_count() == 0, what="unpinned")
        assert h.retrieve("nope") is None
        node.pools.check()

    @pytest.mark.parametrize("bulk", [False, True])
    def test_released_reads_drop_ownership_and_each_read_pins_again(
        self, fake, nodes, handlers, bulk
    ):
        h = handlers(nodes())
        assert put(h, "k")
        sent = len(fake.requests(MessageType.LOOKUP_AND_PIN))
        reads = h.batch_retrieve(["k", "k"])
        assert reads[0] is not reads[1]
        assert reads[0].pin_id != reads[1].pin_id
        assert len(fake.requests(MessageType.LOOKUP_AND_PIN)) == sent + 1
        assert fake.pin_count() == 2 and len(h._cpu._reads) == 2
        ref = weakref.ref(reads[0])
        old_pin = reads[0].pin_id
        if bulk:
            h.release_retrieved(reads)
        else:
            reads[0].release()
            reads[1].release()
        h.release_retrieved(reads)
        wait_until(lambda: fake.pin_count() == 0, what="unpinned")
        assert not h._cpu._reads and not h._cpu.pins
        del reads
        assert ref() is None
        with h.retrieve("k") as fresh:
            assert fresh.pin_id != old_pin and bytes(fresh.view) == b"k"
        assert len(fake.requests(MessageType.LOOKUP_AND_PIN)) == sent + 2
        wait_until(lambda: fake.pin_count() == 0, what="unpinned again")

    def test_argument_checks(self, nodes, handlers):
        node = nodes()
        h = handlers(node)
        other = handlers(node, engine="other")
        wait_credit(h)
        wait_credit(other)
        mine = h.alloc(1)
        theirs = other.alloc(1)
        with pytest.raises(ValueError):
            h.batch_store(["a", "b"], [mine])
        with pytest.raises(ValueError):
            h.store("x", theirs)
        with pytest.raises(ValueError):
            h.batch_store(["a", "b"], [mine, mine])
        with pytest.raises(ValueError):
            h.store("", mine)
        assert h.store("ok", mine)
        with pytest.raises(ValueError):
            h.store("again", mine)  # already submitted
        other.free(theirs)

    def test_rejected_item_is_aborted(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        wait_credit(h, 2)
        good, bad = h.alloc(2), h.alloc(2)
        bad.size = PAGE + 1  # the node refuses the item; the page stays ours
        bad_ref = node.pools.find(bad.location)
        frees = node.pools.transitions[(PageState.CREDITED, PageState.FREE)]
        assert h.batch_store(["g", "b"], [good, bad]) == [True, False]
        # the refused credit is aborted: FREE at the node, never written again
        wait_until(
            lambda: (
                (p := node.pools.page(bad_ref)) is None or p.state == PageState.FREE
            ),
            what="aborted",
        )
        assert node.pools.transitions[(PageState.CREDITED, PageState.FREE)] >= frees + 1
        assert all(
            (c.offset, c.generation) != (bad.location.offset, bad.location.generation)
            for c in h._cpu.credits.take_all()
        )
        node.pools.check()

    def test_big_batches_are_split(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        assert put(h, "k5")
        keys = [f"k{i}" for i in range(1100)]
        exists = h.batch_exists(keys)
        assert exists.count(True) == 1 and exists[5]
        reads = h.batch_retrieve(keys)
        assert len(reads) == 1100 and sum(r is not None for r in reads) == 1
        assert len(fake.requests(MessageType.LOOKUP_AND_PIN)) == 2
        h.release_retrieved(reads)

    def test_location_outside_the_pools_is_never_read(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        assert put(h, "k")
        with fake._lock:
            (entry,) = fake.catalog[Key(NS, "k")]
            entry.location = dataclasses.replace(
                entry.location, offset=node.tier("dram").pool.capacity
            )
        assert h.retrieve("k") is None
        assert h._cpu.stats["retrieve_bad_location"] == 1
        wait_until(lambda: fake.pin_count() == 0, what="pin released")

    def test_unknown_lookup_is_resolved_in_the_background(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        assert put(h, "k")
        fake.drop_responses(MessageType.LOOKUP_AND_PIN)
        assert h.retrieve("k") is None  # unavailable now
        conn = h._cpu.server_conn
        wait_until(lambda: conn.unresolved_lookups() == 0, what="resolved")
        assert fake.replayed[MessageType.LOOKUP_AND_PIN] == 1
        wait_until(lambda: fake.pin_count() == 0, what="its pin released")
        read = h.retrieve("k")
        assert bytes(read.view) == b"k"
        h.release_retrieved([read])

    def test_close_returns_credit_and_detaches(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        wait_credit(h, 2)
        assert put(h, "c")
        read = h.retrieve("c")
        h.release_retrieved([read])
        h.close()
        incarnation = h._cpu.identity.incarnation
        assert node.pools.held_bytes(incarnation) == 0
        assert fake.worker_session_of(incarnation) is None  # WORKER_CLOSE
        wait_until(lambda: node.manager.workers.get(incarnation) is None)  # DETACH

    def test_close_with_a_held_read_keeps_its_pool_and_pin(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        wait_credit(h, 2)
        assert put(h, "c")
        read = h.retrieve("c")
        incarnation = h._cpu.identity.incarnation
        h.close()
        assert node.pools.held_bytes(incarnation) == 0  # credit back
        # No WORKER_CLOSE and no DETACH: the node keeps the worker and reports
        # its exit, which releases the pin; a copy may still read the pool
        assert fake.worker_session_of(incarnation) is not None
        assert fake.pin_count(incarnation) == 1
        time.sleep(0.3)
        assert node.manager.workers.get(incarnation) is not None
        assert bytes(read.view) == b"c"
        h.release_retrieved([read])


class TestCreditRefill:
    def test_a_write_done_asks_for_credit_at_once(
        self, fake, nodes, handlers, monkeypatch
    ):
        """An empty CREDIT is retried once a WRITE_DONE frees credit, not
        after a pause."""
        import maru_handler.client.maru_client as maru_client

        monkeypatch.setattr(maru_client, "REFILL_EMPTY_S", 30.0)
        node = nodes(
            budgets={
                "credit_max_bytes": 2 * PAGE,
                "credit_refill_low": 2 * PAGE,
                "credit_refill_high": 2 * PAGE,
            }
        )
        h = handlers(node)
        wait_credit(h, 2)
        handles = [h.alloc(PAGE), h.alloc(PAGE)]
        # The node holds 2 pages for us: a CREDIT now comes back empty
        h._cpu._refill_wake.set()
        wait_until(lambda: h._cpu.stats["store_skipped_no_credit"] == 0)
        time.sleep(0.2)
        assert h._cpu.credits.unused_bytes == 0
        for i, handle in enumerate(handles):
            handle.buf[:1] = b"x"
            assert h.store(f"w{i}", handle)
        wait_credit(h, 2)  # long before REFILL_EMPTY_S


def test_store_async_answers_once_when_its_batches_end_together(monkeypatch):
    """Batches answered on different threads complete the future once."""
    from concurrent.futures import Future

    import maru_handler.client.maru_client as maru_client

    answers = []

    class Counting(Future):
        def set_result(self, result):
            answers.append(result)
            time.sleep(0.01)  # another answer would come meanwhile
            super().set_result(result)

    class Batch(Future):
        """Runs its callbacks once both batches are answered."""

        def __init__(self, both: threading.Barrier):
            super().__init__()
            self.both = both

        def _invoke_callbacks(self):
            self.both.wait()
            super()._invoke_callbacks()

    monkeypatch.setattr(maru_client, "Future", Counting)
    client = maru_client.MaruClient(
        MaruConfig(
            maru_node="/tmp/unused.sock",
            engine_id="e",
            cache_namespace=NS,
            auto_connect=False,
        )
    )
    for _ in range(20):
        answers.clear()
        both = threading.Barrier(2)
        batches = [(0, Batch(both)), (1, Batch(both))]
        monkeypatch.setattr(client, "_submit_store", lambda *_a, b=batches: b)
        future = client.store_async(["a", "b"], [None, None])
        threads = [
            threading.Thread(target=batch.set_result, args=([i],))
            for i, (_, batch) in enumerate(batches)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert future.result(1) == [0, 1]
        assert len(answers) == 1


def test_the_credit_cache_keeps_its_byte_total_exact():
    import random

    from maru_handler.client.credit_cache import CreditCache

    rng = random.Random(0)
    cache = CreditCache()
    cache.reset("e1")
    serial = 0
    expected = []
    byte_returns = partial_returns = 0
    for _ in range(3000):
        op = rng.choice(["put", "put", "take", "bytes", "pool", "all", "reset"])
        if op == "put":
            credits = []
            for _ in range(rng.randint(1, 4)):
                serial += 1
                credits.append(
                    Location(
                        pool_id=f"p{serial % 3}",
                        allocation_id=f"a{serial}",
                        generation=make_generation(1, serial),
                        offset=serial * PAGE,
                        length=rng.choice([PAGE, PAGE // 2]),
                        kind=PoolKind.DRAM,
                    )
                )
            cache.put(credits, cache.node_epoch or "e1")
            expected.extend(credits)
        elif op == "take":
            taken = cache.take()
            assert taken == ((expected.pop(0), cache.node_epoch) if expected else None)
        elif op == "bytes":
            budget = rng.randint(0, 3 * PAGE)
            total = 0
            count = 0
            for credit in expected:
                if total + credit.length > budget:
                    break
                total += credit.length
                count += 1
            before = sum(c.length for c in expected)
            returned = cache.take_bytes(budget)
            assert returned == expected[:count]
            assert sum(c.length for c in returned) == total <= budget
            expected = expected[count:]
            assert cache.unused_bytes == before - total
            byte_returns += bool(returned)
            partial_returns += bool(returned and expected)
        elif op == "pool":
            pool_id = f"p{rng.randint(0, 2)}"
            assert cache.remove_pool(pool_id) == [
                c for c in expected if c.pool_id == pool_id
            ]
            expected = [c for c in expected if c.pool_id != pool_id]
        elif op == "all" and rng.random() < 0.1:
            assert cache.take_all() == expected
            expected = []
        elif op == "reset" and rng.random() < 0.05:
            epoch = f"e{serial}"
            if epoch != cache.node_epoch:
                expected = []
            cache.reset(epoch)
        assert cache.unused_bytes == sum(c.length for c in expected)
    assert byte_returns > 0 and partial_returns > 0


class TestCreditCounters:
    def test_a_miss_retried_later_is_deferred_not_skipped(self, nodes, handlers):
        node = nodes(
            budgets={
                "credit_max_bytes": 2 * PAGE,
                "credit_refill_low": PAGE,
                "credit_refill_high": 2 * PAGE,
            }
        )
        h = handlers(node)
        wait_credit(h, 2)
        held = [h.alloc(PAGE), h.alloc(PAGE)]
        with pytest.raises(CreditUnavailableError):
            h.alloc(PAGE, retry_later=True)
        with pytest.raises(CreditUnavailableError):
            h.alloc(PAGE)
        counters = h.get_stats()["maru_node"]["counters"]
        assert counters["store_deferred_no_credit"] == 1
        assert counters["store_skipped_no_credit"] == 1
        for handle in held:
            h.free(handle)

    def test_close_ends_a_wait_for_credit(self, nodes, handlers):
        node = nodes(
            budgets={
                "credit_max_bytes": 2 * PAGE,
                "credit_refill_low": PAGE,
                "credit_refill_high": 2 * PAGE,
            }
        )
        h = handlers(node)
        wait_credit(h, 2)
        held = [h.alloc(PAGE), h.alloc(PAGE)]
        for handle in held:
            h.free(handle)
        h._cpu.credits.take_all()  # none at hand; the node still counts them
        outcome = []

        def wait():
            t0 = time.monotonic()
            try:
                h.alloc(PAGE, wait_s=10.0)
            except Exception as e:
                outcome.append((type(e).__name__, time.monotonic() - t0))

        waiter = threading.Thread(target=wait)
        waiter.start()
        time.sleep(0.2)
        t0 = time.monotonic()
        h.close()
        waiter.join(5)
        assert outcome and outcome[0][1] < 2.0
        assert time.monotonic() - t0 < 2.0


def test_stopping_credit_waits_ends_one_under_way_and_keeps_the_rest(nodes, handlers):
    node = nodes(
        budgets={
            "credit_max_bytes": 2 * PAGE,
            "credit_refill_low": PAGE,
            "credit_refill_high": 2 * PAGE,
        }
    )
    h = handlers(node)
    wait_credit(h, 2)
    held = [h.alloc(PAGE), h.alloc(PAGE)]  # none left at hand
    ended = []
    waiter = threading.Thread(
        target=lambda: ended.append(
            pytest.raises(CreditUnavailableError, h.alloc, PAGE, wait_s=10.0)
        )
    )
    waiter.start()
    time.sleep(0.2)
    t0 = time.monotonic()
    h.stop_credit_waits()
    waiter.join(5)
    assert ended and time.monotonic() - t0 < 1.0
    for handle in held:
        h.free(handle)  # credit back in the cache, a store still finds it
    assert put(h, "after")
    incarnation = h._cpu.identity.incarnation
    h.close()
    assert node.pools.held_bytes(incarnation) == 0  # CREDIT_RETURN went


class TestBackgroundConnect:
    def test_connects_once_the_node_is_up(self, fake, nodes, sockdir):
        handler = MaruHandler(self.config(f"{sockdir}/node.sock"))
        try:
            t0 = time.monotonic()
            assert handler.connect(wait=False) is False
            assert time.monotonic() - t0 < 0.1
            assert not handler.connected
            with pytest.raises(StorageUnavailableError):
                handler.batch_retrieve(["k"])
            nodes()
            assert handler.wait_connected(10.0)
            wait_credit(handler)
            assert put(handler, "late")
        finally:
            handler.close()

    def test_maps_the_pools_once_across_a_maruserver_outage(
        self, fake, nodes, monkeypatch
    ):
        from maru_handler.client.pool_maps import PoolMaps

        added = []
        add = PoolMaps.add

        def counting(self, *args, **kwargs):
            added.append(args[0].pool.pool_id)
            return add(self, *args, **kwargs)

        monkeypatch.setattr(PoolMaps, "add", counting)
        node = nodes()
        fake.pause(hold=False)
        handler = MaruHandler(self.config(node.config.socket))
        try:
            handler.connect(wait=False)
            assert handler.wait_attached()
            assert not handler.wait_connected(1.5)  # WORKER_OPEN unanswered
            fake.resume()
            assert handler.wait_connected(10.0)
            assert len(added) == 1
            wait_credit(handler)
            assert put(handler, "after-outage")
        finally:
            handler.close()

    def test_a_refusal_stops_it_and_is_raised(self, fake, nodes):
        from maru_handler.client import AttachRefusedError

        node = nodes()
        handler = MaruHandler(
            self.config(node.config.socket, chunk_size_bytes=2 * PAGE)
        )
        try:
            handler.connect(wait=False)
            with pytest.raises(AttachRefusedError, match="larger than the page"):
                handler.wait_attached(10.0)
            with pytest.raises(AttachRefusedError):
                handler.wait_connected(0)
            assert not handler.connected
        finally:
            handler.close()

    def test_close_while_waiting_for_the_node_returns_promptly(self, sockdir):
        handler = MaruHandler(self.config(f"{sockdir}/node.sock"))
        handler.connect(wait=False)
        time.sleep(0.2)
        t0 = time.monotonic()
        handler.close()
        assert time.monotonic() - t0 < 2.0
        assert not handler.connected

    def config(self, socket_path, **kwargs):
        return MaruConfig(
            maru_node=socket_path,
            engine_id="bg",
            cache_namespace=NS,
            chunk_size_bytes=kwargs.pop("chunk_size_bytes", PAGE),
            pool_size=PAGE,
            pin_pools=False,
            timeout_ms=300,
            auto_connect=False,
            node_connect_timeout_s=kwargs.pop("node_connect_timeout_s", 5.0),
            **kwargs,
        )


class TestMaruServerDown:
    def test_lookups_miss_at_once_until_it_answers(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node, timeout_ms=500)
        wait_credit(h)
        assert put(h, "k")
        assert h.batch_exists(["k"]) == [True]
        fake.pause(hold=False)
        try:
            t0 = time.monotonic()
            assert h.batch_exists(["k"]) == [False]
            assert time.monotonic() - t0 < 2.0  # one RPC timeout at most
            for _ in range(20):
                t0 = time.monotonic()
                assert h.batch_exists(["k"]) == [False]
                assert h.batch_retrieve(["k"]) == [None]
                assert time.monotonic() - t0 < 0.05
        finally:
            fake.resume()
        wait_until(lambda: h.batch_exists(["k"]) == [True], timeout=5.0)
        counters = h.get_stats()["maru_node"]["counters"]
        assert counters["probe_unavailable"] >= 21
        assert counters["retrieve_unavailable"] >= 20

    def test_close_does_not_wait_for_it(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node, timeout_ms=500)
        wait_credit(h)
        fake.pause(hold=False)
        try:
            h.batch_exists(["k"])  # finds it down
            t0 = time.monotonic()
            h.close()
            assert time.monotonic() - t0 < 3.0
            # WORKER_CLOSE did not go: no DETACH, the node reports the exit
            time.sleep(0.3)
            assert node.manager.workers.get(h._cpu.identity.incarnation) is not None
        finally:
            fake.resume()


class TestFirstConnect:
    def test_waits_for_a_node_that_is_starting(self, fake, sockdir):
        from maru_storage_service.gpu.local_server import LocalServer

        path = f"{sockdir}/late.sock"
        handler = self._handler(path, 20.0)
        out = []
        t = threading.Thread(target=lambda: out.append(handler.connect()))
        t.start()
        time.sleep(0.3)  # no socket yet: ENOENT
        # bound but not listening yet (as claim() leaves it): ECONNREFUSED
        held = LocalServer(
            path,
            None,
            node_id="x",
            node_epoch="e",
            server_address="",
            page_size=PAGE,
            budgets=None,
            pools=[],
        )
        held.claim()
        time.sleep(0.3)
        held.stop()
        node = start_node(node_config(fake.url, path))
        try:
            t.join(20)
            assert out == [True]
            handler.close()
        finally:
            node.stop()

    def test_gives_up_after_its_timeout(self, sockdir):
        handler = self._handler(f"{sockdir}/never.sock", 0.3)
        t0 = time.monotonic()
        with pytest.raises(StorageUnavailableError, match="cannot reach"):
            handler.connect()
        assert time.monotonic() - t0 < 3.0

    def test_close_stops_the_wait(self, sockdir):
        handler = self._handler(f"{sockdir}/never.sock", 30.0)
        errors = []

        def connect():
            try:
                handler.connect()
            except Exception as e:
                errors.append(e)

        t = threading.Thread(target=connect)
        t.start()
        time.sleep(0.2)  # in the wait
        t0 = time.monotonic()
        handler.close()
        t.join(5)
        assert not t.is_alive() and time.monotonic() - t0 < 3.0
        (error,) = errors
        assert isinstance(error, StorageUnavailableError)
        assert "closed while waiting" in str(error)
        assert not handler._cpu.connected

    def test_close_while_connecting_leaves_no_connected_client(
        self, fake, sockdir, monkeypatch
    ):
        from maru_handler.client import maru_client

        path = f"{sockdir}/n.sock"
        node = start_node(node_config(fake.url, path))
        handler = self._handler(path, 30.0)
        server_open = maru_client.ServerConn.open

        def open_then_close(conn):
            server_open(conn)
            handler._cpu.close()  # close() lands after WORKER_OPEN

        monkeypatch.setattr(maru_client.ServerConn, "open", open_then_close)
        try:
            with pytest.raises(
                StorageUnavailableError, match="closed while connecting"
            ):
                handler.connect()
            assert not handler._cpu.connected
            assert not handler._cpu.storage_connected
            # no allocation was made: the session is closed, not left to expire
            assert fake.worker_session_of(handler._cpu.identity.incarnation) is None
            assert fake.requests(MessageType.WORKER_CLOSE)
        finally:
            node.stop()

    @pytest.mark.skipif(os.geteuid() == 0, reason="root ignores the socket mode")
    def test_no_permission_fails_at_once(self, sockdir):
        path = f"{sockdir}/closed.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(path)
        listener.listen()
        os.chmod(path, 0)  # EACCES
        handler = self._handler(path, 30.0)
        t0 = time.monotonic()
        try:
            with pytest.raises(StorageUnavailableError, match="Permission denied"):
                handler.connect()
        finally:
            listener.close()
        assert time.monotonic() - t0 < 3.0

    def test_refused_attach_fails_at_once(self, fake, sockdir, monkeypatch):
        from maru_handler.client import maru_client

        path = f"{sockdir}/n.sock"
        node = start_node(node_config(fake.url, path))
        monkeypatch.setattr(maru_client, "MS_PROTOCOL", "maru-ms/0")
        handler = self._handler(path, 30.0)
        t0 = time.monotonic()
        try:
            with pytest.raises(StorageUnavailableError, match="ATTACH refused"):
                handler.connect()
            assert time.monotonic() - t0 < 3.0
        finally:
            node.stop()

    def test_unanswered_attach_fails_at_the_deadline(self, sockdir):
        path = f"{sockdir}/silent.sock"
        listener = self._silent_node(path)
        handler = self._handler(path, 0.6)
        t0 = time.monotonic()
        try:
            with pytest.raises(StorageUnavailableError, match="timed out"):
                handler.connect()
        finally:
            listener.close()
        assert 0.4 < time.monotonic() - t0 < 3.0

    def test_close_cancels_an_attach_in_progress(self, sockdir):
        path = f"{sockdir}/silent.sock"
        listener = self._silent_node(path)
        handler = self._handler(path, 30.0)
        before = set(threading.enumerate())
        errors = []

        def connect():
            try:
                handler.connect()
            except Exception as e:
                errors.append(e)

        t = threading.Thread(target=connect)
        try:
            t.start()
            wait_until(lambda: handler._cpu._connecting is not None, what="attaching")
            time.sleep(0.1)  # waiting for the reply
            t0 = time.monotonic()
            handler.close()
            t.join(5)
            assert not t.is_alive() and time.monotonic() - t0 < 3.0
        finally:
            listener.close()
        (error,) = errors
        assert isinstance(error, StorageUnavailableError)
        assert "closed while attaching" in str(error)
        assert not handler._cpu.connected
        assert set(threading.enumerate()) <= before  # no thread is left

    def _handler(self, socket_path, timeout_s):
        return MaruHandler(
            MaruConfig(
                maru_node=socket_path,
                engine_id="early",
                cache_namespace=NS,
                chunk_size_bytes=PAGE,
                pool_size=PAGE,
                pin_pools=False,
                timeout_ms=300,
                auto_connect=False,
                node_connect_timeout_s=timeout_s,
            )
        )

    def _silent_node(self, path):
        """Accepts (the backlog does) and never answers."""
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(path)
        listener.listen()
        return listener
