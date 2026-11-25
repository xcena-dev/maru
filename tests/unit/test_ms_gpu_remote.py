# SPDX-License-Identifier: Apache-2.0
"""Worker staging through GPU control, memory COMMIT and Catalog pins."""

import dataclasses
import errno
import json
import mmap
import os
import select
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from maru_common.config import MaruConfig
from maru_common.ms_rpc import PLACEHOLDER_OP, RpcUnknownError
from maru_common.node_protocol import (
    AbortRequest,
    CreditRequest,
    CreditReturnRequest,
    DetachRequest,
    NodeMessageType,
    Notify,
    Peers,
    ReadJoinRequest,
    WriteDoneItem,
    WriteDoneRequest,
)
from maru_common.protocol import NodeHeartbeatRequest
from maru_common.remote_protocol import RemoteMessageType, TransferEnd
from maru_common.storage_types import (
    WORKER_RDMA_CAPABILITY,
    ItemResult,
    Key,
    KeyLocation,
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
    exit_before_orphan_drain,
    free_port,
    node_config,
    remove_tree,
    short_tmpdir,
    wait_until,
)
from tests.unit.remote_fakes import reset_fake_agents
from tests.unit.remote_transport_fakes import FakeTransport, mapped


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
                    **(
                        {}
                        if profile == "default_staging_budget"
                        else {"staging_max_bytes": 32 * PAGE}
                    ),
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

    def client(instance="instance", *, reconnect_s=0.02, **kwargs):
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
        worker = MaruClient(config, rpc_timeout_s=1, reconnect_s=reconnect_s)
        clients.append(worker)
        assert worker.connect()
        return worker

    def raw(**overrides):
        worker = RawWorker(gpu_config.socket)
        clients.append(worker)
        assert (
            worker.attach(
                capabilities=[WORKER_RDMA_CAPABILITY], instance="instance", **overrides
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


def test_worker_reconnects_its_read_peer_while_control_stays_connected(pair):
    store(pair)
    reader = pair.client("reader", load_only=True)
    storage = reader._storage
    transport = reader._rdma.transport
    peer = reader.attach_info.peers[0].agent_name
    transport.fail_next()
    assert reader.batch_retrieve(["kv"]) == [None]
    wait_until(lambda: transport.peer_ready_hint(peer))
    [read] = reader.batch_retrieve(["kv"])
    assert bytes(read.view) == b"a" * PAGE
    reader.release_retrieved([read])
    assert reader._storage is storage and not storage.closed
    wait_until(lambda: reader._rdma.snapshot().get("kept_slots") == 1)


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
    original = pair.gpu.store.workers.domains["instance"]
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
            pair.gpu.store.workers.domains.get("instance", original) != original
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
    domain = pair.gpu.store.workers.domains["first"]
    other = pair.gpu.store.workers.domains["second"]
    pair.gpu.store.workers._worker_exited(first.identity.incarnation)
    wait_until(lambda: domain in pair.memory.role.parts._closed_domains)
    assert other not in pair.memory.role.parts._closed_domains
    assert second.connected


def test_cuda_stop_fences_writes_and_keeps_worker_ownership(pair):
    writer = pair.client()
    wait_until(lambda: writer.credits.unused_bytes >= PAGE)
    handle = writer.alloc(PAGE)
    domain = pair.gpu.store.workers.domains["instance"]
    writer.stop()
    wait_until(lambda: domain in pair.memory.role.parts._closed_domains)
    assert handle in writer._handles and handle.state == "writing"
    assert writer.identity.incarnation in next(
        iter(pair.gpu.store.workers.admissions.values())
    )


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


@pytest.mark.parametrize("after", ["connected", "restart", "reattach", "disconnect"])
def test_pidfd_exit_releases_credit_and_pins_across_gpu_runs(pair, monkeypatch, after):
    store(pair)
    # The fake NIC lives in this process; the UDS and pidfd identify the child.
    agent = FakeTransport("child-reader")
    staging = read = None
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
command = sys.stdin.readline().strip()
if command == "disconnect":
    w.close()
    sys.stdin.read()
elif command == "reattach":
    again = RawWorker(sys.argv[1], w.incarnation)
    assert again.attach(capabilities=[WORKER_RDMA_CAPABILITY], instance="child-instance").status.value == "OK"
    print("attached", flush=True)
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
        domain = pair.gpu.store.workers.domains["child-instance"]
        if after == "restart":
            pin = service.pins.get(info["pin"])
            item = KeyLocation(pin.key, pin.location)
            (peer,) = pair.gpu.store.remote.peers
            pool = next(p for p in peer.pools if p.pool_id == pin.location.pool_id)
            staging, address = mapped(PAGE)
            agent.register(address, PAGE, "staging")
            agent.add_peer(peer.agent_metadata, peer=peer.agent_name)
            agent.stall()
            read = agent.post_nowait(
                "READ",
                peer.agent_name,
                [(address, pool.base + pin.location.offset, PAGE)],
            )
            assert read is not None and not read.completed
        if after in ("restart", "reattach"):
            pair.gpu.stop()
            pair.gpu = pair.start_gpu()
            assert info["incarnation"] in pair.gpu.role.orphans.pending
            assert service.pins.get(info["pin"]) is not None
        if after == "disconnect":
            child.stdin.write("disconnect\n")
            child.stdin.flush()
            wait_until(lambda: domain in pair.memory.role.parts._closed_domains)
            registry = pair.gpu.store.workers
            with pause_call(monkeypatch, registry, "_worker_exited") as (
                entered,
                resume,
                finished,
            ):
                child.kill()
                child.wait(5)
                assert entered.wait(3)
                sibling = pair.client("child-instance")
                handle = sibling.alloc(PAGE)
                successor = handle.remote.agent.domain
                assert successor != domain
                resume.set()
                assert finished.wait(3)
            pair.gpu.store._control_tasks.submit(lambda: None).result(3)
            assert successor not in pair.memory.role.parts._closed_domains
            assert sibling.batch_store(["survives-exit"], [handle]) == [True]
        elif after == "reattach":
            child.stdin.write("reattach\n")
            child.stdin.flush()
            assert child.stdout.readline().strip() == "attached"
            worker_ops = pair.gpu.store
            orphan_reports = []
            monkeypatch.setattr(worker_ops, "_queue_exit", orphan_reports.append)
            with pause_call(monkeypatch, worker_ops.workers, "_grace_over") as (
                draining,
                resume,
                _,
            ):
                child.kill()
                child.wait(5)
                assert draining.wait(3)
                with worker_ops.orphans.lock:
                    assert info["incarnation"] not in worker_ops.orphans.pending
                    assert not orphan_reports
                assert service.pins.get(info["pin"]) is not None
                resume.set()
        elif after == "restart":
            resume = exit_before_orphan_drain(
                pair.gpu, child, info["incarnation"], monkeypatch
            )
            assert service.pins.get(info["pin"]) is not None
            assert pair.memory.server.retire([item]).items[0].result == ItemResult.KEPT
            assert agent.finish_stalled() == 1
            assert agent.poll_nowait(read) and read.completed
            resume()
        else:
            child.kill()
            child.wait(5)
        wait_until(lambda: service.pins.get(info["pin"]) is None)
        if after == "restart":
            assert (
                pair.memory.server.retire([item]).items[0].result == ItemResult.RETIRED
            )
        wait_until(lambda: domain in pair.memory.role.parts._closed_domains)
        assert pair.memory.pools.bytes_in(PageState.QUARANTINED) == 0
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(5)
        child.stdin.close()
        child.stdout.close()
        if read is not None and not read.finished:
            agent.fail_stalled()
            assert agent.poll_nowait(read)
        agent.close()
        if staging is not None:
            staging.close()


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
        response = real(code, request, timeout)
        if code == RemoteMessageType.RESERVE:
            seen.append(request.op.seq)
            raise TimeoutError("lost reply")
        return response

    monkeypatch.setattr(connection, "call", lose)
    monkeypatch.setattr(connection, "submit", record)
    assert worker.credit(PAGE).credits == []
    assert seen
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


@contextmanager
def pause_call(monkeypatch, obj, name, *, when=lambda *args, **kwargs: True):
    """Hold the first matching call before it enters the method."""
    entered, resume, finished = threading.Event(), threading.Event(), threading.Event()
    method = getattr(obj, name)

    def paused(*args, **kwargs):
        if entered.is_set() or not when(*args, **kwargs):
            return method(*args, **kwargs)
        entered.set()
        try:
            assert resume.wait(10)
            return method(*args, **kwargs)
        finally:
            finished.set()

    with monkeypatch.context() as patch:
        patch.setattr(obj, name, paused)
        try:
            yield entered, resume, finished
        finally:
            resume.set()


def test_late_peer_withdrawal_keeps_reattached_workers_reads(pair, monkeypatch):
    store(pair)
    reader = pair.client("reader", load_only=True)
    old = reader._storage
    rdma = reader._rdma

    def read():
        [value] = reader.batch_retrieve(["kv"])
        assert value is not None and bytes(value.view) == b"a" * PAGE
        reader.release_retrieved([value])

    with pause_call(
        monkeypatch, rdma, "notify_peers", when=lambda peers, *a, **kw: not peers
    ) as (entered, resume, finished):
        pair.gpu.store.remote.connection.close()
        assert entered.wait(3)
        assert pair.gpu.wait_finished(5)
        pair.gpu = pair.start_gpu()
        wait_until(lambda: reader.storage_connected and reader._storage is not old)
        wait_until(lambda: reader._server.session_node_epoch == pair.gpu.node_epoch)
        read()
        resume.set()
        assert finished.wait(3)
    rdma.refresh_peers()
    read()


@pytest.mark.parametrize("pair", ["default_staging_budget"], indirect=True)
@pytest.mark.parametrize("failure", ["nixl", "cuda", "exported_view"])
def test_uncertain_staging_teardown_keeps_admission(pair, monkeypatch, failure):
    torch = pytest.importorskip("torch") if failure == "cuda" else None
    worker = pair.client(load_only=True)
    staging = worker._rdma.staging
    if failure == "nixl":

        def refuse(registration):
            raise RuntimeError("registration still active")

        monkeypatch.setattr(worker._rdma.transport, "deregister", refuse)
    elif failure == "cuda":
        staging.cuda_registered = True
        monkeypatch.setattr(
            torch.cuda,
            "cudart",
            lambda: SimpleNamespace(cudaHostUnregister=lambda _: 1),
        )
        monkeypatch.setattr(
            "maru_handler.memory.mapper._clear_cuda_sticky_error", lambda: None
        )
    else:
        view = staging.view(0)
    worker.close()
    with pytest.raises(AttachRefusedError, match="staging budget"):
        pair.client(load_only=True)
    incarnation = worker.identity.incarnation
    service = pair.gpu.store
    assert {incarnation} in service.workers.admissions.values()
    assert service.workers.get(incarnation).detached
    assert len(service.watcher._fds) == 1
    if failure == "cuda":
        assert staging.cuda_registered
    elif failure == "exported_view":
        view.release()


@pytest.mark.parametrize("pair", ["dram", "default_staging_budget"], indirect=True)
@pytest.mark.parametrize("load_only", [False, True])
def test_clean_close_reuses_budget_and_keeps_one_process_watch(pair, load_only):
    service = pair.gpu.store
    workers = service.workers
    for _ in range(5):
        client = pair.client(load_only=load_only)
        rec = workers.get(client.identity.incarnation)
        assert rec.staging_key == (
            rec.ident.pid,
            rec.ident.pid_start,
            client._staging_id,
        )
        assert len(service.watcher._fds) == 1
        client.close()
        assert not workers.admissions
        wait_until(lambda: not workers._workers)
        assert not service._agents and not service._reserves and not service._credits
        assert not workers._settles_at and service.ledger.origins() == 0
        again = RawWorker(pair.config.socket, rec.incarnation)
        try:
            with pytest.raises(AttachRefusedError, match="incarnation"):
                again.attach(capabilities=[WORKER_RDMA_CAPABILITY], instance="instance")
        finally:
            again.close()
    assert len(service.watcher._fds) == 1


def test_same_staging_and_legacy_workers_have_distinct_admissions(pair):
    registry = pair.gpu.store.workers
    a, _ = pair.raw(staging_id="shared")
    b, _ = pair.raw(staging_id="shared")
    legacy, _ = pair.raw()
    other, _ = pair.raw()
    assert len(registry.admissions) == 3
    assert {a.incarnation, b.incarnation} in registry.admissions.values()
    assert {legacy.incarnation} in registry.admissions.values()
    assert {other.incarnation} in registry.admissions.values()
    rec = registry.get(a.incarnation)
    assert (
        registry.detach(rec, registry.endpoint(b.incarnation), True).status
        == Status.REJECTED
    )
    assert a.request(NodeMessageType.DETACH, DetachRequest(True)).status == Status.OK
    assert {b.incarnation} in registry.admissions.values()
    assert len(pair.gpu.store.watcher._fds) == 1


def test_lost_credit_reply_disables_same_run_without_replay(pair, monkeypatch):
    from maru_handler.client import CreditUnavailableError
    from maru_storage_service.gpu.local_server import Connection

    grants = []
    respond = Connection.respond

    def lose_credit(conn, header, response, *args):
        if header.msg_type == NodeMessageType.CREDIT and response.credits:
            grants.append(response)
            if len(grants) == 1:
                conn.close()
                return False
        return respond(conn, header, response, *args)

    monkeypatch.setattr(Connection, "respond", lose_credit)
    worker = pair.client()
    wait_until(lambda: worker.off or worker.credits.unused_bytes)
    assert "incarnation" in str(worker.off)
    assert len(grants) == 1 and not worker.credits.unused_bytes
    wait_until(
        lambda: grants[0].write_agent.domain in pair.memory.role.parts._closed_domains
    )
    assert not worker._rdma._peers and not worker._stopped.is_set()
    assert worker.batch_retrieve(["missing"]) == [None]
    with pytest.raises(CreditUnavailableError):
        worker.alloc(PAGE)


@pytest.mark.parametrize("restart", [False, True])
def test_disconnect_retains_source_pin_and_gpu_consumer(pair, restart):
    store(pair)
    reader = pair.client("reader", load_only=True)
    rdma = reader._rdma
    old = reader._storage
    old_server = reader._server
    consumer = reader.batch_retrieve(["kv"])[0]
    rdma.transport.stall()
    pending = reader.start_retrieve(["kv"])
    request = pending._chunks[0].remote[0]
    pin = request.outcome.pin_id
    pins = pair.catalog.server.ms_service.pins
    assert request.flight.pending is not None
    if restart:
        pair.gpu.stop()
        pair.gpu = pair.start_gpu()
        wait_until(lambda: reader.storage_connected and reader._storage is not old)
        assert reader.off is None and old_server.session_node_epoch != reader.node_epoch
    else:
        pair.gpu.store.workers.endpoint(reader.identity.incarnation).close()
        wait_until(lambda: reader.off)
    assert pending.result() == [None]
    assert reader._server is old_server and pins.get(pin) is not None
    assert rdma.snapshot()["free_slots"] == rdma.slot_count - 2
    assert bytes(consumer.view) == b"a" * PAGE
    assert rdma.transport.finish_stalled() == 1
    rdma.transport.stall(False)
    wait_until(lambda: pins.get(pin) is None)
    wait_until(lambda: rdma.snapshot().get("free_slots") == rdma.slot_count - 1)
    reader.release_retrieved([consumer])
    wait_until(lambda: rdma.snapshot().get("free_slots") == rdma.slot_count)
    if restart:
        wait_until(lambda: old_server.session_node_epoch == pair.gpu.node_epoch)
        [hit] = reader.batch_retrieve(["kv"])
        assert bytes(hit.view) == b"a" * PAGE
        reader.release_retrieved([hit])
    else:
        assert reader.batch_retrieve(["kv"]) == [None]
        wait_until(lambda: reader._server is None)


@pytest.mark.parametrize("old_first", [False, True])
@pytest.mark.parametrize("fence_first", [False, True])
def test_sibling_writes_mixed_domains(pair, monkeypatch, old_first, fence_first):
    service = pair.gpu.store
    monkeypatch.setattr(MaruClient, "_refill_loop", lambda self: None)
    first, _ = pair.raw()
    d1 = first.credit(PAGE).write_agent.domain
    sibling = pair.client()
    old = sibling.alloc(PAGE)
    assert old.remote.agent.domain == d1
    sibling._on_notify(
        Notify(NotifyKind.CREDIT_RETURN, bytes=3 * PAGE), sibling._storage
    )
    wait_until(lambda: len(service._credits) == 2)
    with pause_call(monkeypatch, service, "_end_worker") as (entered, resume, finished):
        first.close()
        assert entered.wait(3)
        assert d1 not in service.workers.domains.values()
        if fence_first:
            resume.set()
            assert finished.wait(3)
        assert sibling._refill_once()
        new = sibling.alloc(PAGE)
        d2 = new.remote.agent.domain
        assert d2 != d1 and new.location in service._credits
        assert (old.location in service._credits) == (not fence_first)
        resume.set()
        assert finished.wait(3)
    service.workers._worker_exited(first.incarnation)
    assert d2 in service.workers.domains.values()
    assert d2 not in pair.memory.role.parts._closed_domains
    handles = [old, new] if old_first else [new, old]
    keys = ["old", "new"] if old_first else ["new", "old"]
    results = [key == "new" for key in keys]
    assert sibling.store_async(keys, handles).result(5) == results
    assert sibling.batch_exists(keys) == results
    assert old.location not in service._credits and new.location not in service._credits


@pytest.mark.parametrize("failure", ["response", "timeout"])
def test_unknown_credit_waits_for_fence_success(pair, monkeypatch, failure):
    first, _ = pair.raw()
    service = pair.gpu.store
    grant = first.credit(4 * PAGE)
    credit = grant.credits[0]
    domain = grant.write_agent.domain
    called = []
    call = service.remote.call

    def unconfirmed(code, request, **kwargs):
        if code == RemoteMessageType.FENCE_DOMAIN:
            called.append((request.domain, set(service._credits)))
            if failure == "timeout":
                raise TimeoutError
            from maru_common.remote_protocol import FenceDomainResponse, FenceResult

            return FenceDomainResponse(Status.OK, result=FenceResult.FAILURE)
        return call(code, request, **kwargs)

    monkeypatch.setattr(service.remote, "call", unconfirmed)
    req = WriteDoneRequest(
        PLACEHOLDER_OP,
        [
            WriteDoneItem(
                Key("ns", "unknown"), credit, PAGE, domain, TransferEnd.UNKNOWN
            )
        ],
    )
    result = first.request(NodeMessageType.WRITE_DONE, first.stamp(req))
    assert result.items[0].result == ItemResult.OK
    assert called == [(domain, set(grant.credits))]
    assert set(service._credits) == set(grant.credits)


@pytest.mark.parametrize(
    "code",
    [
        NodeMessageType.CREDIT,
        NodeMessageType.READ_JOIN,
        NodeMessageType.WRITE_DONE,
        NodeMessageType.ABORT,
    ],
)
def test_dispatched_request_cannot_recreate_terminal_worker(pair, monkeypatch, code):
    worker, agent = pair.raw()
    service = pair.gpu.store
    rec = service.workers.get(worker.incarnation)
    request = {
        NodeMessageType.CREDIT: CreditRequest(PLACEHOLDER_OP, "session", PAGE),
        NodeMessageType.READ_JOIN: ReadJoinRequest(
            worker.incarnation, agent.metadata()
        ),
        NodeMessageType.WRITE_DONE: WriteDoneRequest(PLACEHOLDER_OP, []),
        NodeMessageType.ABORT: AbortRequest(PLACEHOLDER_OP, []),
    }[code]
    if code != NodeMessageType.READ_JOIN:
        request = worker.stamp(request)
    method = "_write_done" if code == NodeMessageType.WRITE_DONE else code.name.lower()
    replies = []
    send_name = "post_response" if code == NodeMessageType.WRITE_DONE else "respond"
    endpoint = rec.conn
    send = getattr(endpoint, send_name)

    def record(header, response):
        replies.append(response)
        return send(header, response)

    monkeypatch.setattr(endpoint, send_name, record)
    with pause_call(monkeypatch, service, method) as (entered, resume, finished):
        worker.conn.submit(code, request)
        assert entered.wait(3)
        service.workers._worker_exited(worker.incarnation)
        assert service.workers.get(worker.incarnation) is None
        assert service.ledger.origins() == 0
        resume.set()
        assert finished.wait(3)
        wait_until(lambda: replies)
    assert replies[0].status == Status.REJECTED
    assert not service.ledger.origins()
    service._control_tasks.submit(lambda: None).result(3)
    assert worker.incarnation not in service._agents
    assert worker.incarnation not in pair.memory.role.parts._agents


@pytest.mark.parametrize("pause_at", ["notify_peers", "_on_notify"])
def test_closed_snapshot_cannot_post_before_loss_callback(pair, monkeypatch, pause_at):
    store(pair)
    reader = pair.client("reader", load_only=True, reconnect_s=30)
    old = reader._storage
    rdma = reader._rdma
    endpoint = pair.gpu.store.workers.endpoint(reader.identity.incarnation)
    posts = []
    post_nowait = rdma.transport.post_nowait

    def record(op, *args):
        posts.append(op)
        return post_nowait(op, *args)

    monkeypatch.setattr(rdma.transport, "post_nowait", record)
    with (
        ThreadPoolExecutor(max_workers=1) as pool,
        pause_call(monkeypatch, rdma, "post_reads") as (reserved, post, _),
        pause_call(
            monkeypatch, rdma if pause_at == "notify_peers" else old, pause_at
        ) as (notice, deliver, delivered),
        pause_call(monkeypatch, old, "_on_lost") as (closed, _, _),
    ):
        result = pool.submit(reader.batch_retrieve, ["kv"])
        assert reserved.wait(3)
        endpoint.post_notify(Peers([]))
        assert notice.wait(3)
        endpoint.close()
        assert closed.wait(3)
        assert old.closed and reader._storage is old
        deliver.set()
        assert delivered.wait(3)
        post.set()
        assert result.result(3) == [None]
        assert not posts


@pytest.mark.parametrize(
    ("pair", "released"),
    [("default_staging_budget", True), ("dram", False)],
    indirect=["pair"],
)
def test_terminal_detach_cannot_rebind_or_release_successor(
    pair, monkeypatch, released
):
    worker, _ = pair.raw(staging_id="first")
    service = pair.gpu.store
    rec = service.workers.get(worker.incarnation)
    conn = rec.conn
    old = worker.credit(PAGE)
    with pause_call(monkeypatch, service, "_end_worker") as (entered, resume, finished):
        assert (
            worker.request(NodeMessageType.DETACH, DetachRequest(released)).status
            == Status.OK
        )
        assert entered.wait(3)
        again = RawWorker(pair.config.socket, worker.incarnation)
        try:
            with pytest.raises(AttachRefusedError, match="incarnation"):
                again.attach(
                    capabilities=[WORKER_RDMA_CAPABILITY],
                    instance="instance",
                    staging_id="first",
                )
        finally:
            again.close()
        successor, _ = pair.raw(staging_id="next")
        new = successor.credit(PAGE)
        assert old.write_agent.domain != new.write_agent.domain
        assert old.credits[0] in service._credits
        assert service.workers.detach(rec, conn, True).status == Status.REJECTED
        service.workers.connection_lost(rec, conn)
        resume.set()
        assert finished.wait(3)
    assert (service.workers.get(worker.incarnation) is None) == released
    assert old.credits[0] not in service._credits
    assert new.credits[0] in service._credits
    assert {successor.incarnation} in service.workers.admissions.values()
    assert successor.credit(PAGE).credits


def test_close_with_posted_read_keeps_catalog_pin_and_staging(pair):
    store(pair)
    reader = pair.client(load_only=True)
    rdma = reader._rdma
    rdma.transport.stall()
    pending = reader.start_retrieve(["kv"])
    request = pending._chunks[0].remote[0]
    rec = pair.gpu.store.workers.get(reader.identity.incarnation)
    reader.close()
    wait_until(lambda: rec.detached)
    assert pending.result() == [None]
    assert pair.catalog.server.ms_service.pins.get(request.outcome.pin_id) is not None
    assert request.flight in rdma._flights
    assert rec.staging_key in pair.gpu.store.workers.admissions


def test_writer_recovers_on_new_run_with_same_staging(pair):
    writer = pair.client()
    old = writer.alloc(PAGE)
    first = pair.gpu.store.workers.get(writer.identity.incarnation)
    staging = writer._rdma.staging
    pair.gpu.stop()
    pair.gpu = pair.start_gpu()
    wait_until(lambda: writer.node_epoch == pair.gpu.node_epoch)
    second = pair.gpu.store.workers.get(writer.identity.incarnation)
    assert second.staging_key == first.staging_key and writer._rdma.staging is staging
    assert not writer.off and not writer._rdma.owns_credit(old.location)
    assert writer.batch_store(["old"], [old]) == [False]
    wait_until(lambda: writer.credits.unused_bytes >= PAGE)
    new = writer.alloc(PAGE)
    assert new.remote.agent.domain.node_epoch == pair.gpu.node_epoch
    assert writer.batch_store(["new"], [new]) == [True]
    assert writer.batch_exists(["old", "new"]) == [False, True]


@pytest.mark.parametrize("return_credit", [False, True])
def test_reserve_finishing_after_exit_never_grants_or_fences_siblings_domain(
    pair, monkeypatch, return_credit
):
    service = pair.gpu.store
    sibling, _ = pair.raw()
    grant = sibling.credit(PAGE)
    if return_credit:
        returned = sibling.request(
            NodeMessageType.CREDIT_RETURN, CreditReturnRequest(grant.credits)
        )
        assert returned.items[0].result == ItemResult.OK
    worker, _ = pair.raw()
    with pause_call(
        monkeypatch,
        service.remote,
        "call",
        when=lambda code, *args, **kwargs: code == RemoteMessageType.RESERVE,
    ) as (entered, resume, _):
        req = worker.stamp(CreditRequest(PLACEHOLDER_OP, "session", PAGE))
        worker.conn.submit(NodeMessageType.CREDIT, req)
        assert entered.wait(3)
        rec = service.workers.get(worker.incarnation)
        assert not rec.issued
        service.workers._worker_exited(worker.incarnation)
        resume.set()
    wait_until(lambda: worker.incarnation not in service._agents)
    assert set(service._credits) == (set() if return_credit else set(grant.credits))
    assert service.workers.domains["instance"] == grant.write_agent.domain
    assert grant.write_agent.domain not in pair.memory.role.parts._closed_domains
    assert sibling.credit(PAGE).credits


def test_disconnect_waits_for_the_batch_post(pair, monkeypatch):
    writer = store(pair)
    assert writer.batch_store(["second"], [writer.alloc(PAGE)]) == [True]
    reader = pair.client(load_only=True, reconnect_s=30)
    old, rdma = reader._storage, reader._rdma
    endpoint = pair.gpu.store.workers.endpoint(reader.identity.incarnation)
    second = pair.catalog.server.ms_service.catalog.locations(Key("ns", "second"))[
        0
    ].location
    posts = []
    post = rdma.transport.post_nowait

    def record(op, peer, pairs):
        posts.append((op, len(pairs)))
        return post(op, peer, pairs)

    monkeypatch.setattr(rdma.transport, "post_nowait", record)
    with (
        ThreadPoolExecutor(max_workers=1) as pool,
        pause_call(
            monkeypatch, rdma, "_valid", when=lambda f: f.location == second
        ) as (checking, resume, _),
        pause_call(monkeypatch, rdma, "connection_lost") as (loss, revoke, revoked),
    ):
        pending = pool.submit(reader.start_retrieve, ["kv", "second"])
        assert checking.wait(3)
        endpoint.close()
        assert loss.wait(3)
        revoke.set()
        assert old.closed and reader._storage is old
        assert not revoked.is_set()
        resume.set()
        requests = pending.result(3)
        assert revoked.wait(3)
    assert posts == [("READ", 2)]
    assert requests.result() == [None, None]


def test_disconnect_waits_for_the_hit_handoff(pair, monkeypatch):
    store(pair)
    reader = pair.client(load_only=True, reconnect_s=30)
    old, rdma = reader._storage, reader._rdma
    endpoint = pair.gpu.store.workers.endpoint(reader.identity.incarnation)
    pending = reader.start_retrieve(["kv", "kv"])
    first, second = pending._chunks[0].remote.values()
    assert first.ready.wait(3) and second.ready.wait(3)
    with (
        ThreadPoolExecutor(max_workers=1) as pool,
        pause_call(monkeypatch, rdma.staging, "view") as (handoff, resume, _),
        pause_call(monkeypatch, rdma, "connection_lost") as (loss, revoke, revoked),
    ):
        result = pool.submit(first.result)
        assert handoff.wait(3)
        endpoint.close()
        assert loss.wait(3)
        revoke.set()
        assert old.closed and reader._storage is old
        assert not revoked.is_set()
        resume.set()
        hit = result.result(3)
        assert revoked.wait(3)
    assert second.result() is None
    assert hit in first.flight.consumers and bytes(hit.view) == b"a" * PAGE
    reader.release_retrieved([hit])
    wait_until(lambda: rdma.capacity() == rdma.slot_count)


def test_disconnect_before_batch_post_discards_the_snapshot(pair, monkeypatch):
    store(pair)
    reader = pair.client(load_only=True, reconnect_s=30)
    rdma = reader._rdma
    endpoint = pair.gpu.store.workers.endpoint(reader.identity.incarnation)
    posts = []
    monkeypatch.setattr(rdma.transport, "post_nowait", lambda *args: posts.append(args))
    with (
        ThreadPoolExecutor(max_workers=1) as pool,
        pause_call(monkeypatch, rdma, "post_reads") as (reserved, resume, _),
    ):
        result = pool.submit(reader.batch_retrieve, ["kv"])
        assert reserved.wait(3)
        endpoint.close()
        wait_until(lambda: reader._storage is None)
        resume.set()
        assert result.result(3) == [None]
    assert not posts


def test_empty_reserve_leaves_writer_capacity_for_the_next_instance(pair, monkeypatch):
    pools = pair.memory.pools
    held = pools.credit("other-gpu", 8 * PAGE)
    assert held and pools.credit_room("empty") == 0
    parts = pair.memory.role.parts
    monkeypatch.setattr(parts, "domains_max", 1)
    monkeypatch.setattr(MaruClient, "_refill_loop", lambda self: None)
    first = pair.client("empty")
    assert not first.credits.unused_bytes
    assert not parts._writers
    first.close()
    pair.gpu.store._control_tasks.submit(lambda: None).result(3)
    for credit in held:
        assert pools.abort("other-gpu", credit)[0]
    later = pair.client("next")
    assert later.credits.unused_bytes == 4 * PAGE
    assert later.batch_store(["next"], [later.alloc(PAGE)]) == [True]


def test_refill_after_the_only_writer_slot_is_released(pair, monkeypatch):
    monkeypatch.setattr(MaruClient, "_refill_loop", lambda self: None)
    parts = pair.memory.role.parts
    parts.domains_max = 1
    first = pair.client("a")
    assert first.credits.unused_bytes == 4 * PAGE
    second = pair.client("b")
    assert second.credits.unused_bytes == 0
    domain = pair.gpu.store.workers.domains["b"]
    assert domain not in parts._closed_domains
    first.close()
    wait_until(lambda: not parts._writers)
    assert second._refill_once()
    assert second.credits.unused_bytes == 4 * PAGE
    assert domain in parts._writers
    assert second.batch_store(["b"], [second.alloc(PAGE)]) == [True]


def test_refill_after_the_first_writer_registration_fails(pair, monkeypatch):
    monkeypatch.setattr(MaruClient, "_refill_loop", lambda self: None)
    parts = pair.memory.role.parts
    parts.domains_max = 1
    register = parts.writer

    def fail(domain):
        register(domain)
        raise RuntimeError("writer metadata unavailable")

    with monkeypatch.context() as patch:
        patch.setattr(parts, "writer", fail)
        worker = pair.client()
    assert worker.credits.unused_bytes == 0
    (closed,) = parts._closed_domains
    assert not parts._writers
    assert worker._refill_once()
    handle = worker.alloc(PAGE)
    assert handle.remote.agent.domain != closed
    assert worker.batch_store(["after"], [handle]) == [True]


@pytest.mark.parametrize("fence_fails", [False, True])
def test_registration_failure_on_an_issued_domain(pair, monkeypatch, fence_fails):
    monkeypatch.setattr(MaruClient, "_refill_loop", lambda self: None)
    worker = pair.client()
    old = worker.alloc(PAGE)
    domain = old.remote.agent.domain
    service = pair.gpu.store
    worker._on_notify(Notify(NotifyKind.CREDIT_RETURN, bytes=3 * PAGE), worker._storage)
    wait_until(lambda: set(service._credits) == {old.location})
    sibling, _ = pair.raw()
    parts = pair.memory.role.parts
    reserves = dict(service._reserves)

    def fail(requested):
        assert requested == domain
        raise RuntimeError("registration error")

    with monkeypatch.context() as patch:
        patch.setattr(parts, "writer", fail)
        if fence_fails:
            patch.setattr(parts, "fence", fail)
        assert sibling.credit(PAGE).status == Status.REJECTED
    if fence_fails:
        assert service.workers.domains[domain.instance] == domain
        assert old.location in service._credits and service._reserves == reserves
        assert pair.memory.pools.bytes_in(PageState.QUARANTINED) == 2 * PAGE
        worker.free(old)
    else:
        assert not service._credits and not service._reserves
        assert worker._refill_once()
        new = worker.alloc(PAGE)
        assert new.remote.agent.domain != domain
        assert worker.batch_store(["old", "new"], [old, new]) == [False, True]
        assert worker.batch_exists(["old", "new"]) == [False, True]


def test_restarted_readers_share_one_pidfd_with_their_orphan_pins(pair, monkeypatch):
    store(pair)
    readers = [pair.client(load_only=True) for _ in range(2)]
    requests = []
    for reader in readers:
        reader._rdma.transport.stall()
        pending = reader.start_retrieve(["kv"])
        request = pending._chunks[0].remote[0]
        assert request.flight.pending is not None
        requests.append(request)
    pins = pair.catalog.server.ms_service.pins
    pair.gpu.stop()
    opened = []
    pidfd_open = os.pidfd_open

    def record(pid):
        fd = pidfd_open(pid)
        if pid == os.getpid():
            opened.append(fd)
        return fd

    monkeypatch.setattr(os, "pidfd_open", record)
    pair.gpu = pair.start_gpu()
    wait_until(lambda: all(r.node_epoch == pair.gpu.node_epoch for r in readers))
    orphans = pair.gpu.role.orphans
    for reader, request in zip(readers, requests, strict=True):
        assert reader.identity.incarnation in orphans.pending
        assert pins.get(request.outcome.pin_id) is not None
        assert request.result() is None
        assert pins.get(request.outcome.pin_id) is not None
        assert reader._rdma.transport.finish_stalled() == 1
    wait_until(lambda: all(pins.get(r.outcome.pin_id) is None for r in requests))
    assert len(opened) == 1
    assert list(pair.gpu.store.watcher._fds.values()) == opened


def test_rejected_second_read_join_preserves_the_registered_agent(pair):
    worker, agent = pair.raw()
    service = pair.gpu.store
    name = worker.incarnation
    parts = pair.memory.role.parts
    answer = worker.request(
        NodeMessageType.READ_JOIN, ReadJoinRequest(name, b"changed")
    )
    assert answer.status == Status.REJECTED
    assert parts._agents[name][1] == agent.metadata()
    assert service._agents[name] == name
    worker.close()
    wait_until(lambda: service.workers.endpoint(name) is None)
    service._control_tasks.submit(lambda: None).result(3)
    assert name not in service._agents and name not in parts._agents


@pytest.mark.parametrize("fence_fails", [True, False])
def test_mixed_write_done_keeps_rejected_credit_until_fenced(
    pair, monkeypatch, fence_fails
):
    service = pair.gpu.store
    worker, _ = pair.raw()
    grant = worker.credit(4 * PAGE)
    domain = grant.write_agent.domain
    parts = pair.memory.role.parts
    fence = parts.fence
    calls = []

    def revoke(target):
        calls.append(target)
        if len(calls) == 1 or fence_fails:
            raise RuntimeError("registration still active")
        fence(target)

    monkeypatch.setattr(parts, "fence", revoke)
    before = []
    call = service.remote.call

    def observe(code, req, **kwargs):
        if code == RemoteMessageType.FENCE_DOMAIN:
            before.append(
                (
                    set(service._credits),
                    pair.memory.pools.bytes_in(PageState.QUARANTINED),
                )
            )
        return call(code, req, **kwargs)

    monkeypatch.setattr(service.remote, "call", observe)
    items = [
        WriteDoneItem(
            Key("ns", "unknown"), grant.credits[0], PAGE, domain, TransferEnd.UNKNOWN
        ),
        WriteDoneItem(
            Key("ns", "invalid"), grant.credits[1], 0, domain, TransferEnd.DONE
        ),
    ]
    expected = [ItemResult.OK, ItemResult.REJECTED]
    retained = set(grant.credits)
    if not fence_fails:
        items.append(
            WriteDoneItem(
                Key("ns", "known"), grant.credits[2], PAGE, domain, TransferEnd.DONE
            )
        )
        expected.append(ItemResult.ADDED)
        retained.remove(grant.credits[2])
    result = worker.request(
        NodeMessageType.WRITE_DONE,
        worker.stamp(WriteDoneRequest(PLACEHOLDER_OP, items)),
    )
    assert [outcome.result for outcome in result.items] == expected
    assert before == [(retained, len(retained) * PAGE)]
    assert set(service._credits) == (retained if fence_fails else set())
    assert pair.memory.pools.bytes_in(PageState.QUARANTINED) == (
        len(retained) * PAGE if fence_fails else 0
    )


def test_exit_before_first_grant_releases_writer_capacity(pair, monkeypatch):
    service = pair.gpu.store
    parts = pair.memory.role.parts
    monkeypatch.setattr(parts, "domains_max", 1)
    reserved, resume = threading.Event(), threading.Event()
    call = service.remote.call
    cancelled = []

    def hold_reply(code, request, **kwargs):
        answer = call(code, request, **kwargs)
        if code == RemoteMessageType.RESERVE and not reserved.is_set():
            assert len(answer.credits) == 4
            reserved.set()
            assert resume.wait(10)
        if code == RemoteMessageType.CANCEL:
            assert answer.freed == len(request.locations)
            cancelled.extend(request.locations)
        return answer

    monkeypatch.setattr(service.remote, "call", hold_reply)
    script = """
import json, sys
from types import SimpleNamespace
from maru_common.config import MaruConfig
from maru_handler.client import worker_rdma
from maru_handler.client.maru_client import MaruClient

def transport(config, name):
    print(json.dumps([name, client.identity.incarnation]), flush=True)
    metadata = bytes.fromhex(input())
    return SimpleNamespace(
        register=lambda *args: None, metadata=lambda: metadata,
        add_peer=lambda metadata, *, peer: peer,
    )

worker_rdma._transport = transport
config = MaruConfig(
    maru_node=sys.argv[1], instance_id="child", engine_id="child",
    cache_namespace="ns", chunk_size_bytes=int(sys.argv[2]), pin_pools=False,
    auto_connect=False,
)
client = MaruClient(config, rpc_timeout_s=10)
client.connect()
"""
    with subprocess.Popen(
        [sys.executable, "-c", script, pair.config.socket, str(PAGE)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    ) as child:
        try:
            assert select.select([child.stdout], [], [], 10)[0]
            name, incarnation = json.loads(child.stdout.readline())
            # The fake NIC resolves metadata within one process.
            agent = FakeTransport(name)
            try:
                child.stdin.write(agent.metadata().hex() + "\n")
                child.stdin.flush()
                assert reserved.wait(5)
                (domain,) = parts._writers
                rec = service.workers.get(incarnation)
                child.kill()
                child.wait(5)
                wait_until(lambda: rec.exited)
                resume.set()
                wait_until(lambda: incarnation not in service._agents)
            finally:
                agent.close()
        finally:
            resume.set()
            if child.poll() is None:
                child.kill()
    assert len(set(cancelled)) == 4
    assert not service._credits
    assert pair.memory.pools.bytes_in(PageState.CREDITED) == 0
    assert domain in parts._closed_domains and not parts._writers
    later = pair.client("next")
    assert later.credits.unused_bytes == 4 * PAGE
    assert later.batch_store(["next"], [later.alloc(PAGE)]) == [True]
