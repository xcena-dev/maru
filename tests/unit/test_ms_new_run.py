# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""A new run instead of recovery: Node starts a new StorageService run in the
same process when MaruServer restarted or ended the run, when a result stays
unknown, or when the RM lease is lost; workers re-attach without a restart.
Also the worker's side of a MaruServer restart."""

import dataclasses
import logging
import os
import threading
import time

import pytest

from maru_common.config import MaruConfig
from maru_common.protocol import MessageType, WorkerOpenResponse
from maru_common.storage_types import (
    CloseMode,
    ExitCause,
    ItemOutcome,
    ItemResult,
    Key,
    KeyLocation,
    PoolKind,
    SessionRole,
    Status,
)
from maru_handler import MaruHandler
from maru_handler.client import CreditUnavailableError
from maru_handler.client.maru_client import MaruClient
from maru_handler.client.server_conn import ServerConn
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


@pytest.fixture
def handlers():
    made = []

    def make(node, engine="eng"):
        handler = MaruHandler(
            MaruConfig(
                maru_node=node.config.socket,
                engine_id=engine,
                cache_namespace=NS,
                chunk_size_bytes=PAGE,
                pool_size=PAGE,
                pin_pools=False,
                timeout_ms=300,
                auto_connect=False,
                store_timeout_s=10.0,
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


def put(handler, key: str) -> bool:
    value = key.encode()
    handle = handler.alloc(len(value), wait_s=5.0)
    handle.buf[:] = value
    return handler.store(key, handle)


def read(handler, key: str, timeout: float = 10.0) -> bytes:
    def attempt():
        lease = handler.retrieve(key)
        if lease is None:
            return None
        try:
            return bytes(lease.view)
        finally:
            handler.release_retrieved([lease])

    return wait_until(attempt, timeout=timeout, what=f"a hit on {key}")


def attached_to(handler, service: StorageService, fake) -> None:
    """The worker re-attached to ``service`` and opened its session there."""
    client = handler._cpu

    def done():
        server = client.server_conn
        return (
            client.node_epoch == service.node_epoch
            and server is not None
            and server.session_node_epoch == service.node_epoch
            and server.server_epoch == fake.server_epoch
        )

    wait_until(done, timeout=15, what="the worker on the new run")


def test_a_refused_reattach_turns_maru_off(fake, nodes, handlers):
    first = nodes()
    h = handlers(first)
    client = h._cpu
    assert put(h, "k")
    attempts = []
    attach = client._attach

    def counted(timeout_s):
        attempts.append(timeout_s)
        return attach(timeout_s)

    client._attach = counted
    first.stop()
    nodes(page_size=PAGE // 2)  # objects no longer fit its pages
    wait_until(lambda: client.off is not None, what="off")
    assert "larger than the page size" in str(client.off)
    with pytest.raises(CreditUnavailableError):
        h.alloc(4)
    assert h.retrieve("k") is None
    assert h.batch_exists(["k"]) == [False]
    tried = len(attempts)
    time.sleep(1.0)
    assert len(attempts) == tried  # no retries
    stats = h.get_stats()["maru_node"]["counters"]
    assert stats["store_skipped_off"] == 1 and stats["retrieve_off"] == 1


def test_maru_off_ends_its_maruserver_session(fake, nodes, handlers):
    first = nodes()
    h = handlers(first)
    client = h._cpu
    assert put(h, "k")
    lease = h.retrieve("k")  # a read still held when Maru turns off
    server = client.server_conn
    first.stop()
    nodes(page_size=PAGE // 2)
    wait_until(lambda: client.off is not None, what="off")
    time.sleep(0.3)
    assert client.server_conn is server  # kept while the read goes on
    assert not fake.requests(MessageType.WORKER_CLOSE)
    h.release_retrieved([lease])
    wait_until(lambda: client.server_conn is None, what="session ended")
    wait_until(lambda: fake.requests(MessageType.WORKER_CLOSE), what="goodbye")
    wait_until(
        lambda: (
            not any(
                t.is_alive()
                and t.name in ("maru-credit-refill", "maru-worker-heartbeat")
                for t in client._threads + [server._thread]
            )
        ),
        what="threads stopped",
    )
    # a MaruServer restart brings no new session or RPC from it
    sent = len([r for _, r in fake.log if getattr(r, "incarnation", None)])
    fake.restart()
    time.sleep(1.5)
    opens = [
        r
        for r in fake.requests(MessageType.WORKER_OPEN)
        if r.incarnation == client.identity.incarnation
    ]
    assert len(opens) == 1  # only the first one
    assert sent == len([r for _, r in fake.log if getattr(r, "incarnation", None)])


def test_a_refused_open_on_a_lookup_polls_the_session_back(fake, runs, handlers):
    node = runs()
    first = node.service
    h = handlers(node)
    server = h._cpu.server_conn
    server.heartbeat_s = 60.0
    assert put(h, "k")
    time.sleep(0.2)  # the loop waits out its long beat now
    fake.fail_requests(MessageType.NODE_OPEN, Status.BUSY, count=1000)
    fake.restart()
    rpcs = len(fake.log)
    wait_until(lambda: h.retrieve("k") is None and server._refused, what="refused")
    for _ in range(10):
        assert h.retrieve("k") is None  # no RPC each
    assert len(fake.log) - rpcs < 10
    with fake._lock:
        fake._fail[int(MessageType.NODE_OPEN)].clear()
    second = new_run(node, first)
    attached_to(h, second, fake)
    assert put(h, "k2")
    assert read(h, "k2") == b"k2"


def test_a_lookup_of_the_ended_server_is_not_sent_to_the_new_one(fake, runs, handlers):
    from maru_common.ms_rpc import PLACEHOLDER_OP
    from maru_common.protocol import LookupAndPinRequest

    node = runs()
    first = node.service
    h = handlers(node)
    assert put(h, "k")
    server = h._cpu.server_conn
    origin = server._origin
    request = origin.stamp(
        LookupAndPinRequest(
            session=server.session,
            server_epoch=server.server_epoch,
            op=PLACEHOLDER_OP,
            items=[Key(NS, "k")],
        )
    )
    fake.restart()
    attached_to(h, new_run(node, first), fake)
    assert (
        server._send_lookup(origin, request, first.node_epoch, background=True) is None
    )
    assert not [
        r
        for r in fake.requests(MessageType.LOOKUP_AND_PIN)
        if r.server_epoch == fake.server_epoch and r.op.seq == request.op.seq
    ]


def test_a_worker_ahead_of_its_node_logs_no_warning(fake, runs, handlers, caplog):
    node = runs()
    first = node.service
    h = handlers(node)
    server = h._cpu.server_conn
    assert put(h, "k")
    fake.fail_requests(MessageType.NODE_OPEN, Status.BUSY, count=1000)
    log = logging.getLogger("maru_handler.client.server_conn")
    log.addHandler(caplog.handler)
    try:
        caplog.set_level("DEBUG", logger=log.name)
        fake.restart()
        wait_until(lambda: server._refused, what="refused before the node opened")
        time.sleep(0.6)
        with fake._lock:
            fake._fail[int(MessageType.NODE_OPEN)].clear()
        attached_to(h, new_run(node, first), fake)
    finally:
        log.removeHandler(caplog.handler)
    records = [r for r in caplog.records if r.name == "maru_handler.client.server_conn"]
    assert not [
        r for r in records if r.levelname == "WARNING" and "refused" in r.getMessage()
    ]


def test_a_server_seen_down_is_polled_at_once(fake, runs, handlers):
    node = runs()
    first = node.service
    h = handlers(node)
    server = h._cpu.server_conn
    server.heartbeat_s = 60.0  # a slow beat must not delay the poll
    assert put(h, "k")
    time.sleep(0.2)  # the loop waits out its long beat now
    fake.pause(hold=False)
    assert h.retrieve("k") is None  # unanswered: down
    assert server.down
    fake.restart()
    fake.resume()
    wait_until(lambda: not server.down, timeout=5, what="polled")
    attached_to(h, new_run(node, first), fake)


def test_a_node_that_does_not_open_is_warned_about_once(fake, nodes, handlers, caplog):
    node = nodes()
    h = handlers(node)
    server = h._cpu.server_conn
    assert put(h, "k")
    server.session_ttl_s = 0.5  # warn after this long
    log = logging.getLogger("maru_handler.client.server_conn")
    log.addHandler(caplog.handler)
    try:
        caplog.set_level("INFO", logger=log.name)
        fake.restart()  # the run ends, and no next run opens
        wait_until(lambda: server._refused, what="waiting for the node")
        time.sleep(2.0)  # polls go on
    finally:
        log.removeHandler(caplog.handler)
    # pytest >= 9.1 also attaches caplog.handler to the non-propagating
    # maru_handler logger, so one record can be listed twice
    warnings = {
        id(r)
        for r in caplog.records
        if r.levelname == "WARNING" and "has not opened" in r.getMessage()
    }
    assert len(warnings) == 1
    opens = [
        r
        for r in fake.requests(MessageType.WORKER_OPEN)
        if r.incarnation == h._cpu.identity.incarnation
    ]
    assert len(opens) >= 3  # still polled


def test_a_late_open_reply_never_takes_the_epoch_back():
    """Opens run one at a time: the later one's server epoch stays and the
    live epoch is never counted as ended."""
    client = MaruClient(
        MaruConfig(
            maru_node="/nonexistent/node.sock",
            engine_id="e",
            cache_namespace=NS,
            auto_connect=False,
            pin_pools=False,
        )
    )
    conn = ServerConn(
        "tcp://127.0.0.1:1",
        client.identity,
        node_id="n",
        node_epoch="node",
        pid=os.getpid(),
        pid_start=1,
        timeout_s=0.1,
        role=SessionRole.PROBE,
    )
    conn.server_epoch, conn.session = "old", "old-session"
    conn.on_server_epoch.append(client._server_epoch_changed)
    entered, release = threading.Event(), threading.Event()
    answers = iter(["old", "new"])  # in the order the requests reach a server

    def call(code, request):
        epoch = next(answers)
        if epoch == "old":
            entered.set()
            assert release.wait(5)
        return WorkerOpenResponse(
            status=Status.OK,
            worker_session=f"{epoch}-session",
            server_epoch=epoch,
            heartbeat_s=1,
            session_ttl_s=30,
        )

    conn._rpc.call = call
    first = threading.Thread(target=conn.open)
    second = threading.Thread(target=conn.open)
    try:
        first.start()
        assert entered.wait(2)
        second.start()
        time.sleep(0.05)
        release.set()
        first.join(2)
        second.join(2)
        assert conn.server_epoch == "new"
        assert client._ended_epochs == {"old"}
    finally:
        release.set()
        conn._rpc.close()


class TestMaruOffRaces:
    def test_a_lookup_in_flight_when_maru_turns_off_is_not_handed_out(
        self, fake, nodes, handlers
    ):
        from maru_handler.client.storage_conn import AttachRefusedError

        node = nodes()
        h = handlers(node)
        client = h._cpu
        assert put(h, "k")
        server = client.server_conn
        lookup = server.lookup_and_pin

        def lookup_then_off(keys, pin_sources=False):
            answered = lookup(keys, pin_sources)  # pinned at MaruServer
            client._turn_off(AttachRefusedError("refused"))
            wait_until(lambda: fake.requests(MessageType.WORKER_CLOSE), what="close")
            return answered

        server.lookup_and_pin = lookup_then_off
        assert h.retrieve("k") is None  # its pin went with WORKER_CLOSE
        assert not client._reads and len(client.pins) == 0
        assert h.get_stats()["maru_node"]["counters"]["retrieve_off"] == 1

    def test_an_alloc_racing_maru_off_writes_nothing(self, fake, nodes, handlers):
        from maru_handler.client.storage_conn import AttachRefusedError

        node = nodes()
        h = handlers(node)
        client = h._cpu
        wait_until(lambda: client.credits.unused_bytes >= PAGE, what="credit")
        take = client.credits.take
        returned = []
        client._return_credits = returned.extend

        def take_then_off(wait_s=0.0):
            taken = take(wait_s)
            client._turn_off(AttachRefusedError("refused"))
            return taken

        client.credits.take = take_then_off
        with pytest.raises(CreditUnavailableError):
            h.alloc(4)
        assert not client._handles and len(returned) == 1
        wait_until(lambda: client.server_conn is None, what="session ended")


def drop_connection(node) -> None:
    """The node side closes every worker connection; the node goes on."""
    for conn in node.local.connections():
        conn.close()


def test_a_lost_connection_unmaps_an_idle_pool_and_a_same_run_attach_maps_it(
    fake, nodes, handlers
):
    node = nodes()
    h = handlers(node)
    client = h._cpu
    assert put(h, "k")
    pool_id = node.tier("dram").pool.pool_id
    before = client.pools.get(pool_id)
    drop_connection(node)
    wait_until(lambda: before.mapping._closed, what="unmapped")
    wait_until(
        lambda: (m := client.pools.get(pool_id)) is not None and m is not before,
        what="mapped again",
    )
    assert client.node_epoch == node.node_epoch  # the same run
    assert read(h, "k") == b"k"
    assert put(h, "k2") and read(h, "k2") == b"k2"


def test_a_pool_busy_when_the_connection_is_lost_goes_with_its_last_read(
    fake, nodes, handlers
):
    node = nodes()
    h = handlers(node)
    client = h._cpu
    assert put(h, "k")
    lease = h.retrieve("k")
    before = client.pools.get(node.tier("dram").pool.pool_id)
    drop_connection(node)
    wait_until(
        lambda: client.pools.get(node.tier("dram").pool.pool_id) not in (None, before),
        what="mapped again",
    )
    assert not before.mapping._closed  # the read holds it
    assert bytes(lease.view) == b"k"
    h.release_retrieved([lease])
    wait_until(lambda: before.mapping._closed, what="unmapped after the read")
