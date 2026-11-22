# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageService through its UDS and metadata boundaries."""

import dataclasses
import grp
import os
import stat
import threading
import time

import pytest

from maru_common.config import MaruConfig
from maru_common.ms_rpc import PLACEHOLDER_OP, MsRpcClient
from maru_common.node_protocol import (
    CreditRequest,
    CreditReturnRequest,
    DetachRequest,
    NodeMessageType,
)
from maru_common.protocol import MessageType
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
)
from maru_storage_service.local_server import LocalServerError
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
    from maru_storage_service.local_pin import REOPEN_ON_KEPT

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
            assert node.manager.watcher.watching(f"worker:{raw.incarnation}")
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
        first.attach()
        key = f"worker:{first.incarnation}"
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


class TestStaleRuns:
    def test_exit_between_watch_and_record_is_not_lost(self, fake, nodes, monkeypatch):
        node = nodes()
        # watch() succeeds, then the process ends before the record exists
        monkeypatch.setattr(node.manager.watcher, "watch", lambda *a: True)
        raw = RawWorker(node.config.socket)
        try:
            with pytest.raises(StorageUnavailableError):
                raw.attach()
            assert node.manager.workers.get(raw.incarnation) is None
            wait_until(
                lambda: any(
                    r.incarnation == raw.incarnation for r, _ in fake.worker_exits
                ),
                what="WORKER_EXIT",
            )
        finally:
            raw.close()


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
    from maru_common.storage_types import REMOTE_CAPABILITY
    from tests.unit.ms_fakes import FakeNode, Wire
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
        reader.capabilities = [REMOTE_CAPABILITY]
        reader.open(pools=[])
        ref, page = page_of(node, "k0")
        pinned = reader.source_pin((page.key, page.location)).items[0]
        assert pinned.pin_id
        for i in range(1, 13):
            publish(node, f"k{i}")
        wait_until(lambda: node.manager.retire_results.get("KEPT:PINNED", 0) > 0)
        assert node.pools.page(ref).state is PageState.READY
        reader.unpin(pinned.pin_id)
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
