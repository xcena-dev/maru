# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Probe attaches (a vLLM scheduler), rank identity, transfer defaults and
outcome counters of MaruClient, on a StorageService and the fake MaruServer."""

import dataclasses
import os
import threading
import time

import pytest

from maru_common.config import MaruConfig
from maru_common.ms_rpc import PLACEHOLDER_OP, OpOrigin
from maru_common.node_protocol import (
    NODE_MESSAGE_CLASSES,
    AttachRequest,
    CreditRequest,
    GroupLayout,
    NodeMessageType,
    TransferDefaults,
    process_start_time,
)
from maru_common.protocol import MessageType
from maru_common.serializer import MessageDecodeError, Serializer
from maru_common.storage_types import (
    MS_PROTOCOL,
    ItemOutcome,
    ItemResult,
    MissReason,
    SessionRole,
    Status,
)
from maru_handler import MaruHandler
from maru_handler.client import (
    AttachRefusedError,
    CreditUnavailableError,
    probe_node,
)
from maru_handler.client.storage_conn import StorageConn, close_fds
from maru_storage_service.config import parse_node_config
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
def node(fake):
    sockdir = short_tmpdir()
    service = start_node(node_config(fake.url, f"{sockdir}/node.sock"))
    yield service
    service.stop()
    remove_tree(sockdir)


@pytest.fixture
def handlers(node):
    made = []

    def make(**kwargs):
        options = {
            "maru_node": node.config.socket,
            "engine_id": "eng",
            "cache_namespace": NS,
            "chunk_size_bytes": PAGE,
            "pin_pools": False,
            "timeout_ms": 300,
            "auto_connect": False,
        }
        options.update(kwargs)
        handler = MaruHandler(MaruConfig(**options))
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
    handle = handler.alloc(len(key))
    handle.buf[:] = key.encode()
    return bool(handler.store(key, handle))


def attach(path: str, role: SessionRole, incarnation: str | None = None):
    """One ATTACH by hand; returns (conn, reply, fds)."""
    conn = StorageConn(path, on_notify=lambda n, c: None, on_lost=lambda c: None)
    pid = os.getpid()
    request = AttachRequest(
        engine_id="eng",
        dp_index=0,
        tp_rank=0,
        pid=pid,
        pid_start=process_start_time(pid),
        incarnation=incarnation or os.urandom(8).hex(),
        namespace=NS,
        groups=[] if role == SessionRole.PROBE else [GroupLayout(PAGE, "opaque")],
        protocol=MS_PROTOCOL,
        role=role,
    )
    reply, fds = conn.connect(request, 5.0)
    return conn, reply, fds


# =============================================================================
# Wire
# =============================================================================


def test_step_objects_is_optional_and_checked():
    ser = Serializer(NODE_MESSAGE_CLASSES)
    request = AttachRequest(
        engine_id="e",
        dp_index=0,
        tp_rank=0,
        pid=1,
        pid_start=2,
        incarnation="i",
        namespace=NS,
        groups=[],
        protocol=MS_PROTOCOL,
        step_objects=33,
    )
    payload = dataclasses.asdict(request)
    _, decoded = ser.decode_request(ser.encode(NodeMessageType.ATTACH, payload))
    assert decoded.step_objects == 33
    del payload["step_objects"]  # a sender that predates the field
    _, decoded = ser.decode_request(ser.encode(NodeMessageType.ATTACH, payload))
    assert decoded.step_objects == 0
    for bad in (True, "33", 1.5):
        payload["step_objects"] = bad
        with pytest.raises(MessageDecodeError):
            ser.decode_request(ser.encode(NodeMessageType.ATTACH, payload))


def test_unknown_role_is_refused_and_a_missing_one_is_a_worker():
    ser = Serializer(NODE_MESSAGE_CLASSES)
    request = AttachRequest(
        engine_id="e",
        dp_index=0,
        tp_rank=0,
        pid=1,
        pid_start=2,
        incarnation="i",
        namespace=NS,
        groups=[],
        protocol=MS_PROTOCOL,
    )
    payload = dataclasses.asdict(request)
    del payload["role"]  # a sender that predates the field
    _, decoded = ser.decode_request(ser.encode(NodeMessageType.ATTACH, payload))
    assert decoded.role == SessionRole.WORKER
    payload["role"] = "admin"
    with pytest.raises(MessageDecodeError):
        ser.decode_request(ser.encode(NodeMessageType.ATTACH, payload))


# =============================================================================
# StorageService
# =============================================================================


class TestProbeAttach:
    def test_a_probe_gets_no_pool_credit_or_pin(self, node):
        conn, reply, fds = attach(node.config.socket, SessionRole.PROBE)
        try:
            assert fds == [] and reply.pools == [] and reply.budgets is None
            assert reply.server == node.config.server
            assert reply.page_size == PAGE and reply.node_epoch == node.node_epoch
            assert reply.transfer_defaults == TransferDefaults(False, False)
            assert reply.pid == os.getpid()
            workers = node.manager.workers
            assert len(workers.probes()) == 1
            origin = OpOrigin(os.urandom(8).hex())
            credit = conn.request(
                NodeMessageType.CREDIT,
                origin.stamp(
                    CreditRequest(
                        op=PLACEHOLDER_OP, worker_session="s", want_bytes=PAGE
                    )
                ),
                5.0,
            )
            assert credit.status == Status.REJECTED and not credit.credits
            assert node.pools.held_bytes(workers.probes()[0]) == 0
        finally:
            conn.close()
        wait_until(lambda: node.manager.workers.probes() == [], what="probe gone")

    def test_a_probe_incarnation_cannot_attach_as_a_worker(self, node):
        conn, _, _ = attach(node.config.socket, SessionRole.PROBE, "same")
        try:
            with pytest.raises(AttachRefusedError, match="probe"):
                attach(node.config.socket, SessionRole.WORKER, "same")
        finally:
            conn.close()

    def test_a_detached_probe_is_forgotten(self, node):
        from maru_common.node_protocol import DetachRequest

        conn, _, _ = attach(node.config.socket, SessionRole.PROBE)
        reply = conn.request(NodeMessageType.DETACH, DetachRequest(), 5.0)
        assert reply.status == Status.OK
        assert node.manager.workers.probes() == []
        conn.close()

    def test_transfer_defaults_follow_the_tiers(self):
        base = {
            "node_id": "n",
            "server": "tcp://127.0.0.1:1",
            "page_size": PAGE,
            "budgets": {"transfer_reserve_bytes": PAGE},
        }
        for store in ("dram", "local_cxl"):
            tier = {"store": store, "size": 64 * PAGE}
            if store == "local_cxl":
                tier["region_size"] = 4 * PAGE
            config = dict(base, tiers=[tier])
            assert parse_node_config(config).transfer_defaults() == TransferDefaults(
                async_load=False, async_store=False
            )


# =============================================================================
# MaruClient
# =============================================================================


class TestProbeHandler:
    def test_scheduler_probe_and_worker_in_one_process(self, fake, node, handlers):
        worker = handlers()
        probe = handlers(metadata_only=True)
        workers = node.manager.workers
        assert len(workers.probes()) == 1
        assert workers.get(worker._cpu.identity.incarnation) is not None
        session = fake.worker_session_of(probe._cpu.identity.incarnation)
        assert session.role == SessionRole.PROBE

        assert put(worker, "k1")
        assert probe.batch_exists(["k1", "nothing"]) == [True, False]
        counters = probe.get_stats()["maru_node"]["counters"]
        assert counters["probe_hit"] == 1 and counters["probe_not_found"] == 1
        with pytest.raises(RuntimeError, match="metadata-only"):
            probe.alloc(8)
        with pytest.raises(RuntimeError, match="metadata-only"):
            probe.batch_retrieve(["k1"])
        assert (
            probe._cpu.pools.pool_ids() == [] and probe._cpu.credits.unused_bytes == 0
        )
        # It holds no pin: PROBE gives none
        assert fake.pin_count(probe._cpu.identity.incarnation) == 0

        probe.close()
        assert workers.probes() == []
        assert fake.worker_session_of(probe._cpu.identity.incarnation) is None

    def test_probe_node_learns_the_node_and_leaves(self, node):
        reply = probe_node(
            MaruConfig(maru_node=node.config.socket, engine_id="e", cache_namespace=NS)
        )
        assert reply.node_id == node.config.node_id and reply.page_size == PAGE
        assert reply.transfer_defaults == TransferDefaults(False, False)
        wait_until(lambda: node.manager.workers.probes() == [], what="probe gone")

    def test_an_object_larger_than_the_page_is_refused(self, node):
        handler = MaruHandler(
            MaruConfig(
                maru_node=node.config.socket,
                engine_id="e",
                cache_namespace=NS,
                chunk_size_bytes=2 * PAGE,
                auto_connect=False,
            )
        )
        with pytest.raises(AttachRefusedError, match="larger than the page"):
            handler.connect()

    def test_a_small_object_is_said_to_waste_the_page(
        self, node, handlers, monkeypatch
    ):
        from maru_handler.client import maru_client

        said = []
        monkeypatch.setattr(
            maru_client.logger, "warning", lambda msg, *a: said.append(msg % a)
        )
        handlers(chunk_size_bytes=PAGE // 8)
        assert any("fill 12% of the node's" in line for line in said)


class TestLookupTimeout:
    @pytest.mark.parametrize("probe", [False, True])
    def test_a_500ms_reply_misses_at_100ms_and_recovers(self, fake, handlers, probe):
        worker = handlers(timeout_ms=2000)
        assert put(worker, "here")
        handler = handlers(timeout_ms=2000, metadata_only=True) if probe else worker
        conn = handler._cpu.server_conn
        code = MessageType.PROBE if probe else MessageType.LOOKUP_AND_PIN
        counter = "probe_unavailable" if probe else "retrieve_unavailable"
        call = handler.batch_exists if probe else handler.batch_retrieve
        miss = [False] if probe else [None]
        fake.delay(code, 0.5)
        fake.delay(MessageType.WORKER_HEARTBEAT, 0.5)
        fake.delay(MessageType.UNPIN, 0.5)

        started = time.monotonic()
        assert call(["here"]) == miss
        elapsed = time.monotonic() - started
        assert 0.08 <= elapsed < 0.4  # at the 100 ms timeout, not the reply
        assert conn.down
        assert handler.get_stats()["maru_node"]["counters"][counter] == 1
        sent = len(fake.requests(code))
        started = time.monotonic()
        assert call(["here"]) == miss
        assert time.monotonic() - started < 0.05
        assert len(fake.requests(code)) == sent == 1
        assert handler.get_stats()["maru_node"]["counters"][counter] == 2

        wait_until(lambda: not conn.down, what="heartbeat recovery")
        if probe:
            assert conn.unresolved_lookups() == 0
            assert fake.pin_count() == 0
        else:
            wait_until(lambda: conn.unresolved_lookups() == 0, what="lookup resolved")
            wait_until(lambda: fake.pin_count() == 0, what="late pin released")
            assert conn.late_hits == 1
            first, resent = fake.requests(code)
            assert first.op == resent.op
            assert fake.executed[code] == fake.replayed[code] == 1
        fake.delay(code, 0)
        if probe:
            assert call(["here"]) == [True]
        else:
            reads = call(["here"])
            assert bytes(reads[0].view) == b"here"
            handler.release_retrieved(reads)

    @pytest.mark.parametrize("wait", [False, True])
    @pytest.mark.parametrize("lookup_ms", [None, 250])
    def test_config_reaches_both_connection_paths(self, node, wait, lookup_ms):
        options = {} if lookup_ms is None else {"lookup_timeout_ms": lookup_ms}
        config = MaruConfig(
            maru_node=node.config.socket,
            engine_id="eng",
            cache_namespace=NS,
            chunk_size_bytes=PAGE,
            pin_pools=False,
            auto_connect=False,
            **options,
        )
        assert config.credit_wait_s == 0
        handler = MaruHandler(config)
        try:
            handler.connect(wait=wait)
            client = handler._cpu
            assert client.wait_connected(5)
            assert client.rpc_timeout_s == client.server_conn._rpc.timeout_s == 2
            assert client.server_conn._lookup_timeout_s == (lookup_ms or 100) / 1000
        finally:
            handler.close()

    @pytest.mark.parametrize("value", [0, -1, True, 0.5])
    def test_invalid_lookup_timeout_is_rejected(self, value):
        with pytest.raises(ValueError, match="lookup_timeout_ms"):
            MaruConfig(lookup_timeout_ms=value)


class TestRankIdentity:
    def test_ranks_share_the_engine_id_and_name_their_ranks(self, fake, node, handlers):
        ranks = [handlers(dp_index=1, tp_rank=rank) for rank in range(2)]
        for rank, handler in enumerate(ranks):
            incarnation = handler._cpu.identity.incarnation
            ident = node.manager.workers.get(incarnation).ident
            assert (ident.engine_id, ident.dp_index, ident.tp_rank) == ("eng", 1, rank)
            session = fake.worker_session_of(incarnation)
            assert (session.engine_id, session.dp_index, session.tp_rank) == (
                "eng",
                1,
                rank,
            )

    def test_a_load_only_worker_asks_no_credit(self, fake, node, handlers):
        writer = handlers()
        reader = handlers(load_only=True)
        assert put(writer, "k1")
        with pytest.raises(CreditUnavailableError, match="only loads"):
            reader.alloc(8)
        assert node.pools.held_bytes(reader._cpu.identity.incarnation) == 0
        assert "maru-credit-refill" not in [t.name for t in reader._cpu._threads]
        assert reader._cpu._refill_once() is False
        (read,) = reader.batch_retrieve(["k1"])
        assert bytes(read.view) == b"k1"
        reader.release_retrieved([read])
        wait_until(lambda: fake.pin_count() == 0, what="unpinned")


class TestOutcomeCounters:
    def test_unavailable_not_found_and_read_errors_are_counted_apart(
        self, fake, node, handlers
    ):
        handler = handlers()
        assert put(handler, "here")

        def offline(response):
            response.items[2] = ItemOutcome(ItemResult.MISS, reason=MissReason.IO_ERROR)
            response.items[-1] = ItemOutcome(ItemResult.MISS, reason=MissReason.OFFLINE)
            return response

        fake.rewrite_responses(MessageType.LOOKUP_AND_PIN, offline)
        reads = handler.batch_retrieve(["here", "gone", "planned", "late"])
        assert reads[0] is not None and reads[1:] == [None, None, None]
        handler.release_retrieved(reads)
        stats = handler.get_stats()["maru_node"]["counters"]
        assert stats["retrieve_hit"] == 1
        assert stats["retrieve_not_found"] == 1
        assert stats["retrieve_io_error"] == 1
        assert stats["retrieve_unavailable"] == 1
        assert stats["retrieve_unavailable_offline"] == 1

        fake.pause(hold=False)
        assert handler.batch_retrieve(["here"]) == [None]
        assert handler.batch_exists(["here"]) == [False]
        fake.resume()
        stats = handler.get_stats()["maru_node"]["counters"]
        assert stats["retrieve_unavailable"] == 2
        assert stats["probe_unavailable"] == 1

    def test_an_offline_miss_is_unavailable(self, handlers):
        handler = handlers()
        client = handler._cpu
        client._count_miss(
            "retrieve", ItemOutcome(ItemResult.MISS, reason=MissReason.OFFLINE)
        )
        client._count_miss(
            "retrieve", ItemOutcome(ItemResult.MISS, reason=MissReason.ABSENT)
        )
        assert client.stats["retrieve_unavailable_offline"] == 1
        assert client.stats["retrieve_unavailable"] == 1
        assert client.stats["retrieve_not_found"] == 1


def test_probe_attach_from_another_thread_while_a_worker_writes(node, handlers):
    """A TP=1 engine runs its scheduler's probe beside its worker."""
    worker = handlers()
    errors: list[BaseException] = []

    def probe_loop():
        try:
            for _ in range(5):
                conn, reply, fds = attach(node.config.socket, SessionRole.PROBE)
                close_fds(fds)
                conn.close()
        except BaseException as e:
            errors.append(e)

    thread = threading.Thread(target=probe_loop)
    thread.start()
    for i in range(4):
        assert put(worker, f"k{i}")
    thread.join()
    assert errors == []


# =============================================================================
# Credit for one engine step
# =============================================================================


class TestStepCredit:
    @pytest.fixture
    def step_node(self, fake, monkeypatch):
        """A node whose credit at hand is 16 pages (as with 32 MiB pages)
        and whose total, each worker's cap, is 40."""
        import maru_storage_service.config as node_config_module

        monkeypatch.setattr(node_config_module, "CREDIT_MAX_MIN_BYTES", 0)
        sockdir = short_tmpdir()
        config = parse_node_config(
            {
                "node_id": "gpu-step",
                "state_dir": sockdir,
                "socket": f"{sockdir}/node.sock",
                "server": fake.url,
                "page_size": PAGE,
                "tiers": [{"store": "dram", "size": 128 * PAGE, "hugepage": False}],
                "budgets": {
                    "credit_total_max_bytes": 40 * PAGE,
                    "commit_pending_max_bytes": 16 * PAGE,
                    "transfer_reserve_bytes": PAGE,
                },
            }
        )
        assert config.budgets.credit_max_bytes == 40 * PAGE
        assert config.budgets.credit_refill_high == 16 * PAGE
        service = start_node(config)
        yield service
        service.stop()
        remove_tree(sockdir)

    @pytest.mark.parametrize("step_objects", [0, 24, 100])
    def test_a_worker_holds_the_credit_at_hand_whatever_its_step(
        self, step_node, handlers_on, step_objects, caplog
    ):
        with caplog.at_level("INFO"):
            h = handlers_on(step_node, step_objects=step_objects)
            wait_until(lambda: h._cpu.credits.unused_bytes == 16 * PAGE)
        assert h._cpu.attach_info.budgets == step_node.config.budgets
        incarnation = h._cpu.identity.incarnation
        assert step_node.pools.held_bytes(incarnation) == 16 * PAGE
        assert "credit 16 pages at hand, up to 40 pages" in caplog.text

    def test_a_step_asks_for_its_stores_at_once(self, step_node, handlers_on):
        h = handlers_on(step_node)
        wait_until(lambda: h._cpu.credits.unused_bytes == 16 * PAGE)
        h.want_credit(30)  # above the low mark, still asked for now
        wait_until(lambda: h._cpu.credits.unused_bytes == 30 * PAGE)
        handles = [h.alloc(8) for _ in range(30)]  # no wait, no skip
        for handle in handles:
            h.free(handle)  # back at hand
        h.want_credit(100)  # capped at the node total
        wait_until(lambda: h._cpu.credits.unused_bytes == 40 * PAGE)

    def test_a_step_ask_refused_for_now_is_asked_again(
        self, step_node, handlers_on, monkeypatch
    ):
        """The node answers the step's ask empty (its commits pending): the
        worker asks again once credit frees, without a new step."""
        from maru_common.node_protocol import CreditResponse
        from maru_common.storage_types import Status

        h = handlers_on(step_node)
        wait_until(lambda: h._cpu.credits.unused_bytes == 16 * PAGE)
        real = step_node.manager.credit
        answered = []

        def first_empty(rec, decoded):
            if not answered:
                answered.append(True)
                return CreditResponse(status=Status.OK, credits=[])
            return real(rec, decoded)

        monkeypatch.setattr(step_node.manager, "credit", first_empty)
        h.want_credit(30)
        wait_until(lambda: h._cpu.credits.unused_bytes == 30 * PAGE, timeout=2)
        assert answered

    def test_credit_request_has_no_kind(self, step_node, handlers_on):
        from dataclasses import fields

        h = handlers_on(step_node)
        wait_until(lambda: h._cpu.credits.unused_bytes == 16 * PAGE)
        assert "kind" not in {f.name for f in fields(CreditRequest)}
        h.want_credit(30)
        wait_until(lambda: h._cpu.credits.unused_bytes == 30 * PAGE, timeout=2)

    @pytest.mark.parametrize(
        "delay, credit_wait_s, stored",
        [(0.005, 0.02, 30), (0.3, 0.02, 16), (0.3, None, 16)],
    )
    def test_a_store_waits_a_little_for_its_steps_credit(
        self, step_node, handlers_on, monkeypatch, delay, credit_wait_s, stored
    ):
        """The step's CREDIT answers late: a store past the credit at hand
        waits for it, at most credit_wait_s in the whole step."""
        from maru_handler.client import CreditUnavailableError

        options = {} if credit_wait_s is None else {"credit_wait_s": credit_wait_s}
        h = handlers_on(step_node, **options)
        wait_until(lambda: h._cpu.credits.unused_bytes == 16 * PAGE)
        real = step_node.manager.credit

        def slow(rec, decoded):
            time.sleep(delay)
            return real(rec, decoded)

        monkeypatch.setattr(step_node.manager, "credit", slow)
        h.want_credit(30)
        handles, started = [], time.monotonic()
        for _ in range(30):
            try:
                handles.append(h.alloc(8))  # kept: a freed credit is reused
            except CreditUnavailableError:
                pass
        took = time.monotonic() - started
        for handle in handles:
            h.free(handle)
        got = len(handles)
        assert got == stored
        assert took < h._cpu.config.credit_wait_s + 0.1 + (delay if got == 30 else 0)

    def test_workers_capped_at_the_node_total_share_it(self, step_node, handlers_on):
        """Both ask for the whole node total; the second gets its share back
        from the first."""
        a = handlers_on(step_node)
        a.want_credit(40)
        wait_until(lambda: a._cpu.credits.unused_bytes == 40 * PAGE)
        b = handlers_on(step_node, engine_id="b")
        b.want_credit(40)
        wait_until(lambda: b._cpu.credits.unused_bytes >= 16 * PAGE, timeout=10)
        held = {
            h: step_node.pools.held_bytes(h._cpu.identity.incarnation) for h in (a, b)
        }
        assert sum(held.values()) <= 40 * PAGE
        handle = b.alloc(8)
        handle.buf[:] = b"b" * 8
        assert b.store("b", handle)

    def test_a_negative_step_is_refused(self, step_node):
        from maru_handler.client import AttachRefusedError

        raw = RawWorker(step_node.config.socket)
        try:
            with pytest.raises(AttachRefusedError, match="step_objects -1 is negative"):
                raw.attach(step_objects=-1)
        finally:
            raw.close()


@pytest.fixture
def handlers_on():
    made = []

    def make(node, **kwargs):
        options = {
            "maru_node": node.config.socket,
            "engine_id": "eng",
            "cache_namespace": NS,
            "chunk_size_bytes": PAGE,
            "pin_pools": False,
            "timeout_ms": 300,
            "auto_connect": False,
        }
        handler = MaruHandler(MaruConfig(**{**options, **kwargs}))
        handler.connect()
        made.append(handler)
        return handler

    yield make
    for handler in made:
        handler.close()
