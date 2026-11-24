# SPDX-License-Identifier: Apache-2.0
"""Worker staging through GPU control, memory COMMIT and Catalog pins."""

import dataclasses
import errno
import json
import mmap
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from maru_common.config import MaruConfig
from maru_common.ms_rpc import PLACEHOLDER_OP, RpcUnknownError
from maru_common.node_protocol import (
    CreditRequest,
    CreditReturnRequest,
    NodeMessageType,
    Notify,
    ReadJoinRequest,
    WriteDoneItem,
    WriteDoneRequest,
)
from maru_common.protocol import NodeHeartbeatRequest
from maru_common.remote_protocol import TransferEnd
from maru_common.storage_types import (
    WORKER_RDMA_CAPABILITY,
    ItemResult,
    Key,
    NotifyKind,
    Status,
)
from maru_handler.client import AttachRefusedError
from maru_handler.client.maru_client import MaruClient
from maru_remote.auth import write_key
from maru_storage_service.config import RemoteServerConfig, parse_node_config
from maru_storage_service.pool_manager import PageState
from maru_storage_service.service import StorageService
from tests.unit.ms_node_helpers import (
    PAGE,
    FakeRm,
    MetadataServer,
    RawWorker,
    free_port,
    node_config,
    remove_tree,
    short_tmpdir,
    wait_until,
)
from tests.unit.remote_fakes import reset_fake_agents
from tests.unit.remote_transport_fakes import FakeTransport


@pytest.fixture
def pair(monkeypatch, request):
    reset_fake_agents()
    directory = short_tmpdir()
    catalog = MetadataServer()
    key = f"{directory}/maru.key"
    write_key(key)
    profile = getattr(request, "param", "dram")
    store_batches = profile == "store_batches"
    if store_batches:
        # Scale the 32 MiB page workload to the test pool.
        monkeypatch.setattr(
            "maru_storage_service.config.CREDIT_MAX_MIN_BYTES", 16 * PAGE
        )
    memory_config = dataclasses.replace(
        node_config(
            catalog.address,
            f"{directory}/unused",
            node_id="memory",
            role="memory",
            pages=128 if store_batches else 16,
            budgets={"credit_total_max_bytes": 32 * PAGE} if store_batches else None,
        ),
        remote_server=RemoteServerConfig(f"127.0.0.1:{free_port()}", key, 8 * PAGE),
    )
    options = {}
    if profile == "local_cxl":
        memory_config = dataclasses.replace(
            memory_config,
            tiers=node_config(
                catalog.address,
                f"{directory}/unused",
                node_id="memory",
                role="memory",
                cxl_pages=16,
                region_pages=4,
            ).tiers,
        )
        options = {
            "region_source": FakeRm(),
            "map_region": lambda pool: mmap.mmap(-1, pool.capacity),
        }
    memory = StorageService(
        memory_config,
        make_transport=lambda name, device: FakeTransport(name),
        rpc_timeout_s=0.3,
        evict_interval_s=100,
        **options,
    )
    memory.start()
    gpu_config = parse_node_config(
        {
            "node_id": "gpu",
            "socket": f"{directory}/node.sock",
            "state_dir": directory,
            "server": catalog.address,
            "page_size": PAGE,
            "tiers": [
                {
                    "store": "remote",
                    "url": memory_config.remote_server.listen,
                    "key_file": key,
                    "staging_size": (32 if store_batches else 8) * PAGE,
                    "staging_max_bytes": 32 * PAGE,
                    "quarantine_max": 2 * PAGE,
                    "timeout_s": 0.5,
                }
            ],
            "budgets": {}
            if store_batches
            else {
                "credit_max_bytes": 4 * PAGE,
                "credit_total_max_bytes": 8 * PAGE,
            },
        }
    )
    nodes, clients = [memory], []

    def start_gpu():
        node = StorageService(
            gpu_config, rpc_timeout_s=0.3, gpu_drain_grace_s=0.01, evict_interval_s=100
        )
        nodes.append(node)
        node.start()
        return node

    gpu = start_gpu()
    monkeypatch.setattr(
        "maru_handler.client.worker_rdma._transport",
        lambda config, name: FakeTransport(name),
    )

    def client(instance="instance", **kwargs):
        config = MaruConfig(
            maru_node=gpu_config.socket,
            engine_id=f"engine-{len(clients)}",
            cache_namespace="ns",
            instance_id=instance,
            chunk_size_bytes=PAGE,
            pin_pools=False,
            auto_connect=False,
            **kwargs,
        )
        worker = MaruClient(config, rpc_timeout_s=1, reconnect_s=0.02)
        clients.append(worker)
        assert worker.connect()
        return worker

    def raw():
        worker = RawWorker(gpu_config.socket)
        clients.append(worker)
        assert (
            worker.attach(
                capabilities=[WORKER_RDMA_CAPABILITY], instance="instance"
            ).status
            == Status.OK
        )
        agent = FakeTransport(worker.incarnation)
        clients.append(agent)
        assert (
            worker.request(
                NodeMessageType.READ_JOIN,
                ReadJoinRequest(worker.incarnation, agent.metadata()),
            ).status
            == Status.OK
        )
        return worker, agent

    yield SimpleNamespace(
        gpu=gpu,
        memory=memory,
        catalog=catalog,
        client=client,
        raw=raw,
        nodes=nodes,
        start_gpu=start_gpu,
        config=gpu_config,
    )
    for c in reversed(clients):
        c.close()
    for node in reversed(nodes):
        node.stop()
        assert node.teardown_failed is None
    catalog.close()
    remove_tree(directory)
    reset_fake_agents()


def store(pair, value=b"a" * PAGE):
    writer = pair.client()
    wait_until(lambda: writer.credits.unused_bytes >= PAGE)
    handle = writer.alloc(PAGE)
    handle.buf[:] = value
    assert writer.batch_store(["kv"], [handle]) == [True]
    return writer


def test_worker_writer_commits_and_another_worker_reads_without_a_copy_cache(pair):
    writer = store(pair)
    reader = pair.client("reader", load_only=True)
    assert writer.attach_info.pools == []
    assert writer._rdma.staging.nbytes == pair.config.top.options["staging_size"]
    assert writer._rdma.kept_limit == 2
    first = reader.batch_retrieve(["kv"])[0]
    assert first is not None and bytes(first.view) == b"a" * PAGE
    posts = len(reader._rdma.transport.agent.released)
    reader.release_retrieved([first])
    second = reader.batch_retrieve(["kv"])[0]
    assert second is not None and bytes(second.view) == b"a" * PAGE
    assert len(reader._rdma.transport.agent.released) > posts
    reader.release_retrieved([second])


@pytest.mark.parametrize("pair", ["store_batches"], indirect=True)
def test_remote_credit_keeps_up_with_consecutive_store_steps(pair, monkeypatch):
    grants = []
    reserve = pair.memory.role.ops.reserve

    def record_reserve(origin, request, done):
        def granted(response):
            grants.append(len(response.credits))
            done(response)

        reserve(origin, request, granted)

    monkeypatch.setattr(pair.memory.role.ops, "reserve", record_reserve)
    writer = pair.client()
    assert grants == [16]
    assert writer.config.credit_wait_s == 0
    for step in range(12):
        keys = [f"{step}-{i}" for i in range(4)]
        writer.want_credit(len(keys))
        handles = [writer.alloc(PAGE) for _ in keys]
        assert writer.batch_store(keys, handles) == [True] * len(keys)
    assert writer.stats["store_skipped_no_credit"] == 0
    assert sum(grants) >= 48
    assert max(grants[1:]) >= 4
    assert pair.memory.pools.bytes_in(PageState.READY) == 48 * PAGE


@pytest.mark.parametrize("pair", ["store_batches"], indirect=True)
def test_worker_can_use_its_last_step_of_credit_while_reserve_is_in_flight(
    pair, monkeypatch
):
    writer = pair.client()
    handles = [writer.alloc(PAGE) for _ in range(8)]
    assert writer.batch_store([f"first-{i}" for i in range(8)], handles) == [True] * 8

    started, release = threading.Event(), threading.Event()
    reserve = pair.memory.role.ops.reserve

    def hold_reserve(origin, request, done):
        started.set()
        assert release.wait(5)
        reserve(origin, request, done)

    monkeypatch.setattr(pair.memory.role.ops, "reserve", hold_reserve)
    try:
        handles = [writer.alloc(PAGE) for _ in range(4)]
        assert started.wait(5)
        handles.extend(writer.alloc(PAGE) for _ in range(4))
    finally:
        release.set()
    assert writer.batch_store([f"next-{i}" for i in range(8)], handles) == [True] * 8


def test_catalog_publication_waits_for_memory_commit(pair, monkeypatch):
    writer = pair.client()
    wait_until(lambda: writer.credits.unused_bytes >= PAGE)
    held, release = threading.Event(), threading.Event()
    real = pair.memory.store.metadata.commit

    def commit(items):
        held.set()
        assert release.wait(5)
        return real(items)

    monkeypatch.setattr(pair.memory.store.metadata, "commit", commit)
    handle = writer.alloc(PAGE)
    handle.buf[:] = b"b" * PAGE
    pending = writer.store_async(["kv"], [handle])
    assert held.wait(5)
    try:
        assert not pending.done()
        assert writer.batch_exists(["kv"]) == [False]
    finally:
        release.set()
    assert pending.result(5) == [True]
    assert writer.batch_exists(["kv"]) == [True]


def test_credit_replay_and_return_use_the_same_reservation(pair):
    worker, _ = pair.raw()
    request = worker.stamp(CreditRequest(PLACEHOLDER_OP, "session", PAGE))
    first = worker.credit(PAGE, stamped=request)
    again = worker.credit(PAGE, stamped=request)
    assert again.credits == first.credits
    assert len(pair.gpu.store._reserves) == 1
    assert pair.memory.pools.bytes_in(PageState.CREDITED) == PAGE
    assert first.status == Status.OK and len(first.credits) == 1
    response = worker.request(
        NodeMessageType.CREDIT_RETURN, CreditReturnRequest(first.credits)
    )
    assert response.items[0].result == ItemResult.OK
    assert not pair.gpu.store._credits
    assert pair.memory.pools.bytes_in(PageState.CREDITED) == 0


def test_failed_write_agent_load_returns_the_full_grant(pair, monkeypatch):
    monkeypatch.setattr(MaruClient, "_refill_loop", lambda self: None)
    load = FakeTransport.add_peer
    failed = []

    def fail_once(transport, metadata, *, peer=None):
        if peer and "-write-" in peer and not failed:
            failed.append(peer)
            raise RuntimeError("write agent unavailable")
        return load(transport, metadata, peer=peer)

    monkeypatch.setattr(FakeTransport, "add_peer", fail_once)
    worker = pair.client()
    wait_until(lambda: worker.stats["credit_bad_domain"] == 1)
    assert len(failed) == 1 and worker.credits.unused_bytes == 0
    wait_until(lambda: not pair.gpu.store._credits, what="unused grant returned")
    worker._refill_once()
    wait_until(lambda: worker.credits.unused_bytes == 4 * PAGE)
    handle = worker.alloc(PAGE)
    assert worker.batch_store(["after-bind-failure"], [handle]) == [True]


def test_returning_remote_credit_releases_128_worker_bindings(pair, monkeypatch):
    monkeypatch.setattr(MaruClient, "_refill_loop", lambda self: None)
    worker = pair.client()
    returned = set()
    for batch in range(32):
        if batch:
            worker._refill_once()
        wait_until(lambda: worker.credits.unused_bytes == 4 * PAGE)
        assert len(worker._rdma._credit_agents) == 4
        returned.update(worker._rdma._credit_agents)
        worker._on_notify(
            Notify(NotifyKind.CREDIT_RETURN, bytes=4 * PAGE), worker._storage
        )
        wait_until(lambda: not worker._rdma._credit_agents)
    assert len(returned) == 128
    assert worker.credits.unused_bytes == 0
    assert not pair.gpu.store._credits
    assert pair.memory.pools.bytes_in(PageState.CREDITED) == 0


def test_unknown_write_fences_the_shared_instance_before_reclaim(pair):
    first, _ = pair.raw()
    second, _ = pair.raw()
    a, b = first.credit(PAGE), second.credit(PAGE)
    assert a.write_agent.domain == b.write_agent.domain
    domain = a.write_agent.domain
    request = first.stamp(
        WriteDoneRequest(
            PLACEHOLDER_OP,
            [
                WriteDoneItem(
                    Key("ns", "unknown"),
                    a.credits[0],
                    PAGE,
                    domain,
                    TransferEnd.UNKNOWN,
                )
            ],
        )
    )
    assert (
        first.request(NodeMessageType.WRITE_DONE, request).items[0].result
        == ItemResult.OK
    )
    assert domain in pair.memory.role.parts._closed_domains
    assert pair.memory.pools.bytes_in(PageState.QUARANTINED) == 0
    assert pair.memory.pools.bytes_in(PageState.CREDITED) == 0
    assert not pair.gpu.store._credits
    newer = second.credit(PAGE)
    assert newer.write_agent.domain.number > domain.number


@pytest.mark.parametrize("failure", ["idle_ttl", "write_error"])
def test_worker_resumes_storing_after_its_cached_domain_is_fenced(pair, failure):
    writer = pair.client()
    wait_until(lambda: writer.credits.unused_bytes == 4 * PAGE)
    original = pair.gpu.store._domains["instance"]
    if failure == "idle_ttl":
        ops = pair.memory.role.ops
        now = time.monotonic()
        ops._clock = lambda: now + 120
        assert ops.sweep() == 4
        assert original in pair.memory.role.parts._closed_domains
    else:
        writer._rdma.transport.fail_next()
    handle = writer.alloc(PAGE)
    handle.buf[:] = b"a" * PAGE
    assert writer.batch_store(["failed"], [handle]) == [False]
    assert writer._rdma.capacity() == writer._rdma.slot_count
    wait_until(
        lambda: (
            pair.gpu.store._domains.get("instance", original) != original
            and writer.credits.unused_bytes >= PAGE
        )
    )
    assert original in pair.memory.role.parts._closed_domains
    handle = writer.alloc(PAGE)
    assert handle.remote.agent.domain.number > original.number
    handle.buf[:] = b"b" * PAGE
    assert writer.batch_store(["resumed"], [handle]) == [True]
    assert writer.batch_exists(["failed", "resumed"]) == [False, True]


def test_worker_exit_fences_only_its_instance_domain(pair):
    first = pair.client("first")
    second = pair.client("second")
    wait_until(lambda: len(pair.gpu.store._credits) >= 2)
    domain = pair.gpu.store._domains["first"]
    other = pair.gpu.store._domains["second"]
    pair.gpu.store.workers._worker_exited(first.identity.incarnation)
    wait_until(lambda: domain in pair.memory.role.parts._closed_domains)
    assert other not in pair.memory.role.parts._closed_domains
    assert second.connected


def test_cuda_stop_fences_writes_and_keeps_worker_ownership(pair):
    writer = pair.client()
    wait_until(lambda: writer.credits.unused_bytes >= PAGE)
    handle = writer.alloc(PAGE)
    domain = pair.gpu.store._domains["instance"]
    writer.stop()
    wait_until(lambda: domain in pair.memory.role.parts._closed_domains)
    assert handle in writer._handles and handle.state == "writing"
    assert writer.identity.incarnation in pair.gpu.store._admitted


def test_inflight_read_keeps_its_orphan_pin_while_the_gpu_ss_restarts(pair):
    from maru_handler.client.server_conn import ServerConn, WorkerIdentity
    from maru_handler.client.worker_rdma import WorkerRdma

    store(pair)
    raw, agent = pair.raw()
    info = raw.response
    server = ServerConn(
        info.server,
        WorkerIdentity("raw", 0, 0, raw.incarnation, "ns"),
        node_id=info.node_id,
        node_epoch=info.node_epoch,
        pid=info.pid,
        pid_start=info.pid_start,
        timeout_s=1,
    )
    config = MaruConfig(
        maru_node=pair.config.socket,
        engine_id="raw",
        cache_namespace="ns",
        chunk_size_bytes=PAGE,
        rdma_staging_size=4 * PAGE,
        rdma_timeout_s=5,
    )
    rdma = WorkerRdma(
        config, PAGE, transport_factory=lambda name: agent, progress=False
    )
    try:
        server.open()
        hit = server.lookup_and_pin([Key("ns", "kv")])[0][0]
        rdma.notify_peers(info.peers)
        agent.stall()
        pending = rdma.start(hit, rdma.capture(), server.unpin)
        assert pending.flight is not None
        pair.gpu.stop()
        pair.gpu = pair.start_gpu()
        assert raw.incarnation in pair.gpu.role.orphans.pending
        assert pair.catalog.server.ms_service.pins.get(hit.pin_id) is not None
        agent.finish_stalled()
        rdma.progress()
        read = pending.result()
        assert read is not None and bytes(read.view) == b"a" * PAGE
        read.release()
        assert server._flush_unpins()
        assert pair.catalog.server.ms_service.pins.get(hit.pin_id) is None
    finally:
        server.close()
        rdma.close()


def test_control_disconnect_invalidates_peers_and_ends_the_gpu_run(pair, monkeypatch):
    store(pair)
    reader = pair.client("reader", load_only=True)
    scheduler = pair.client("scheduler", metadata_only=True)
    keys = [Key("ns", "kv")]
    assert scheduler._server.probe(keys)[0].result == ItemResult.HIT
    stopping, resume = threading.Event(), threading.Event()
    stop_workers = pair.gpu.role.stop_workers

    def hold_workers():
        stopping.set()
        assert resume.wait(5)
        stop_workers()

    monkeypatch.setattr(pair.gpu.role, "stop_workers", hold_workers)
    try:
        pair.gpu.store.remote.connection.close()
        assert stopping.wait(5)
        wait_until(lambda: not reader._rdma._peers)
        assert scheduler._server.probe(keys)[0].result == ItemResult.MISS
        miss = reader._server.lookup_and_pin(keys)[0][0]
        assert miss.result == ItemResult.MISS and miss.pin_id is None
    finally:
        resume.set()
    assert pair.gpu.wait_finished(5)
    assert pair.gpu.ended


def test_control_disconnect_still_closes_run_if_withdrawal_is_unanswered(
    pair, monkeypatch
):
    peers = []

    def unanswered(usage):
        peers.append(pair.gpu.server.memory_peer)
        raise RpcUnknownError("heartbeat unanswered")

    monkeypatch.setattr(pair.gpu.server, "heartbeat", unanswered)
    pair.gpu.store.remote.connection.close()
    assert pair.gpu.wait_finished(5)
    assert peers == [None]
    assert (
        pair.catalog.server.ms_service.registry.nodes["gpu"].session.closed_at
        is not None
    )


def test_lost_registration_reply_is_retried_before_workers_connect(pair, monkeypatch):
    pair.gpu.stop()
    rpc = pair.catalog.rpc
    answer = rpc._answer
    attempts = []

    def drop_first_reply(frame):
        reply = answer(frame)
        _, request = rpc._serializer.decode_request(frame)
        if (
            isinstance(request, NodeHeartbeatRequest)
            and request.memory_peer is not None
        ):
            _, response = rpc._serializer.decode_response(reply)
            with socket.socket(socket.AF_UNIX) as worker:
                attempts.append(
                    (request, response.status, worker.connect_ex(pair.config.socket))
                )
            if len(attempts) == 1:
                return None
        return reply

    monkeypatch.setattr(rpc, "_answer", drop_first_reply)
    gpu = pair.start_gpu()
    request = attempts[0][0]
    assert request.session == gpu.server.session
    assert request.memory_peer == ("memory", pair.memory.node_epoch)
    assert attempts == [(request, Status.OK, errno.ECONNREFUSED)] * 2
    worker = pair.client(load_only=True)
    assert worker.attach_info.node_epoch == gpu.node_epoch


def test_memory_restart_uses_new_pool_registrations(pair):
    writer = store(pair)
    before = writer.attach_info.peers[0]
    pair.memory.stop()
    wait_until(lambda: pair.gpu.wait_finished(0))
    replacement = StorageService(
        pair.memory.config,
        make_transport=lambda name, device: FakeTransport(name),
        rpc_timeout_s=0.3,
        evict_interval_s=100,
    )
    pair.nodes.append(replacement)
    replacement.start()
    pair.gpu = pair.start_gpu()
    after = pair.gpu.store.remote.peers[0]
    assert after.node_epoch != before.node_epoch
    assert {p.pool_registration for p in after.pools}.isdisjoint(
        p.pool_registration for p in before.pools
    )
    reader = pair.client("reader", load_only=True)
    assert reader.batch_retrieve(["kv"]) == [None]


def test_staging_budget_rejects_a_fifth_worker(pair):
    for i in range(4):
        pair.client(str(i), load_only=True)
    with pytest.raises(AttachRefusedError, match="staging budget"):
        pair.client("extra", load_only=True)


@pytest.mark.parametrize("pair", ["local_cxl"], indirect=True)
def test_read_agent_metadata_covers_every_memory_pool(pair):
    writer = pair.client()
    for index in range(5):
        wait_until(lambda: writer.credits.unused_bytes >= PAGE)
        handle = writer.alloc(PAGE)
        handle.buf[:] = bytes([index]) * PAGE
        assert writer.batch_store([f"k{index}"], [handle]) == [True]
    reader = pair.client("reader", load_only=True)
    reads = reader.batch_retrieve([f"k{i}" for i in range(5)])
    assert all(r is not None for r in reads)
    assert len({r.location.pool_id for r in reads}) > 1
    for i, read in enumerate(reads):
        assert bytes(read.view) == bytes([i]) * PAGE
    reader.release_retrieved(reads)


@pytest.mark.parametrize("restart", [False, True])
def test_pidfd_exit_releases_credit_and_pins_across_gpu_runs(pair, restart):
    store(pair)
    # The fake NIC lives in this process; the UDS and pidfd identify the child.
    agent = FakeTransport("child-reader")
    script = """
import json, sys
sys.path.insert(0, sys.argv[3])
from ms_node_helpers import RawWorker, PAGE
from maru_common.node_protocol import NodeMessageType, ReadJoinRequest
from maru_common.storage_types import WORKER_RDMA_CAPABILITY, Key
from maru_handler.client.server_conn import ServerConn, WorkerIdentity
w = RawWorker(sys.argv[1])
a = w.attach(capabilities=[WORKER_RDMA_CAPABILITY], instance="child-instance")
assert a.status.value == "OK"
assert w.request(NodeMessageType.READ_JOIN, ReadJoinRequest("child-reader", bytes.fromhex(sys.argv[2]))).status.value == "OK"
s = ServerConn(a.server, WorkerIdentity("raw", 0, 0, w.incarnation, "ns"), node_id=a.node_id, node_epoch=a.node_epoch, pid=a.pid, pid_start=a.pid_start, timeout_s=1)
s.open()
credit = w.credit(PAGE, s.session)
assert credit.status.value == "OK" and credit.credits
hit = s.lookup_and_pin([Key("ns", "kv")])[0][0]
assert hit.result.value == "HIT"
print(json.dumps({"pin": hit.pin_id, "incarnation": w.incarnation}), flush=True)
sys.stdin.read()
"""
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            script,
            pair.config.socket,
            agent.metadata().hex(),
            str(Path(__file__).resolve().parent),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        info = json.loads(child.stdout.readline())
        service = pair.catalog.server.ms_service
        assert service.pins.get(info["pin"]) is not None
        domain = pair.gpu.store._domains["child-instance"]
        if restart:
            pair.gpu.stop()
            pair.gpu = pair.start_gpu()
            assert info["incarnation"] in pair.gpu.role.orphans.pending
            assert service.pins.get(info["pin"]) is not None
        child.kill()
        child.wait(5)
        wait_until(lambda: service.pins.get(info["pin"]) is None)
        wait_until(lambda: domain in pair.memory.role.parts._closed_domains)
        assert pair.memory.pools.bytes_in(PageState.QUARANTINED) == 0
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(5)
        child.stdin.close()
        child.stdout.close()
        agent.close()


def test_reserve_reply_loss_cancels_by_sequence_and_keeps_the_gpu_run_closed(
    pair, monkeypatch
):
    worker, _ = pair.raw()
    connection = pair.gpu.store.remote.connection
    real = connection.call
    submit = connection.submit
    seen, sent = [], []

    def record(code, request, callback):
        sent.append((code, request))
        return submit(code, request, callback)

    def lose(code, request, timeout):
        from maru_common.remote_protocol import RemoteMessageType

        response = real(code, request, timeout)
        if code == RemoteMessageType.RESERVE:
            seen.append(request.op.seq)
            raise TimeoutError("lost reply")
        return response

    monkeypatch.setattr(connection, "call", lose)
    monkeypatch.setattr(connection, "submit", record)
    assert worker.credit(PAGE).credits == []
    assert seen
    from maru_common.remote_protocol import RemoteMessageType

    assert any(
        code == RemoteMessageType.CANCEL and request.reserve_seq == seen[0]
        for code, request in sent
    )
    wait_until(lambda: pair.gpu.wait_finished(0))
    assert not pair.gpu.store._credits
    wait_until(lambda: pair.memory.pools.bytes_in(PageState.CREDITED) == 0)


def test_failed_fence_keeps_quarantine_until_memory_process_restart(pair, monkeypatch):
    worker, _ = pair.raw()
    credit = worker.credit(PAGE)
    domain = credit.write_agent.domain

    def failed(domain):
        raise RuntimeError("revocation unknown")

    monkeypatch.setattr(pair.memory.role.parts, "fence", failed)
    pair.gpu.store.workers._worker_exited(worker.incarnation)
    wait_until(lambda: pair.gpu.wait_finished(0))
    assert pair.memory.pools.bytes_in(PageState.QUARANTINED) == PAGE
    assert domain in pair.memory.role.parts._writers
