# SPDX-License-Identifier: Apache-2.0
"""Direct reader ownership and instance-domain writer contracts."""

import dataclasses
import threading
import time
import uuid

import pytest

from maru_common.config import MaruConfig
from maru_common.remote_protocol import TransferEnd
from maru_common.storage_types import (
    ItemOutcome,
    ItemResult,
    Location,
    PoolKind,
    ReadPeer,
    RemotePool,
    WriteAgent,
    WriteDomain,
)
from maru_handler.client.worker_rdma import _KEPT_UNTIL_EXIT, WorkerRdma
from maru_remote.transport import NixlTransport, buffer_address
from tests.unit.remote_fakes import FakeNixlAgent

PAGE = 4096


@pytest.fixture
def env():
    name = uuid.uuid4().hex
    memory = bytearray(b"a" * PAGE + b"b" * PAGE)
    remote = NixlTransport(f"read-{name}", agent=FakeNixlAgent(f"read-{name}"))
    remote.register(buffer_address(memory), len(memory))
    agent = FakeNixlAgent(f"worker-{name}")
    config = MaruConfig(
        maru_node="/unused",
        engine_id="engine",
        cache_namespace="ns",
        instance_id="instance",
        auto_connect=False,
        pin_pools=False,
        chunk_size_bytes=PAGE,
        rdma_staging_size=4 * PAGE,
        rdma_timeout_s=0.03,
    )
    reader = WorkerRdma(
        config,
        PAGE,
        transport_factory=lambda name: NixlTransport(name, agent=agent),
        progress=False,
    )
    pool = RemotePool(
        "memory-pool",
        PoolKind.DRAM,
        1,
        len(memory),
        buffer_address(memory),
        remote.metadata(),
        7,
    )
    peer = ReadPeer(
        "memory",
        "memory-run",
        remote._agent.name,
        remote.metadata(),
        [pool],
        "boot:1",
        1,
        True,
    )
    reader.notify_peers([peer])
    unpins = []

    def outcome(offset=0, pin=None, **kwargs):
        return ItemOutcome(
            ItemResult.HIT,
            location=Location(
                pool.pool_id, f"alloc-{offset}", 1, offset, PAGE, PoolKind.DRAM
            ),
            pin_id=pin or uuid.uuid4().hex,
            pool_registration=7,
            **kwargs,
        )

    def start(hit=None, **kwargs):
        return reader.start(hit or outcome(), reader.capture(), unpins.extend, **kwargs)

    class Env:
        pass

    e = Env()
    e.reader, e.agent, e.remote, e.memory, e.pool, e.peer = (
        reader,
        agent,
        remote,
        memory,
        pool,
        peer,
    )
    e.unpins, e.outcome, e.start = unpins, outcome, start
    yield e
    reader.close()
    remote.close()


def take(env, request=None):
    request = request or env.start()
    env.reader.progress()
    return request.result()


def test_slot_stays_until_the_gpu_copy_ends(env):
    read = take(env)
    assert bytes(read.view) == b"a" * PAGE
    assert env.reader.capacity() == 3
    assert len(env.unpins) == 1
    env.reader.progress()
    assert env.reader.capacity() == 3
    read.release()
    assert env.reader.capacity() == 4


def test_joined_copies_free_the_slot_after_the_last(env):
    env.agent.stall = True
    first, second = env.start(), env.start()
    assert first.flight is second.flight
    env.agent.finish_stalled()
    env.reader.progress()
    a, b = first.result(), second.result()
    assert a is not None and b is not None
    a.release()
    assert env.reader.capacity() == 3
    b.release()
    assert env.reader.capacity() == 4


def test_finished_copy_keeps_the_slot_for_undecided_joiners(env):
    env.agent.stall = True
    first, second = env.start(), env.start()
    env.agent.finish_stalled()
    env.reader.progress()
    a = first.result()
    a.release()
    assert env.reader.capacity() == 3
    b = second.result()
    assert bytes(b.view) == b"a" * PAGE
    b.release()
    assert env.reader.capacity() == 4


def test_a_finished_copy_is_not_cached(env):
    first = take(env)
    env.memory[:PAGE] = b"c" * PAGE
    second = take(env)
    assert bytes(first.view) == b"a" * PAGE
    assert bytes(second.view) == b"c" * PAGE
    assert first.flight is not second.flight
    first.release()
    second.release()


@pytest.mark.parametrize("which", ["gate", "transport", "poll"])
def test_busy_post_is_a_terminal_miss(env, which):
    lock = env.reader.gate if which == "gate" else env.reader.transport._lock
    if which == "poll":
        request = env.start()
        lock = request.flight.pending._poll_lock
    lock.acquire()
    try:
        if which != "poll":
            request = env.reader.start(env.outcome(), 0, env.unpins.extend)
            assert request.done()
            assert request.result() is None
        else:
            env.reader.progress()
            assert not request.done()
    finally:
        lock.release()
    if which == "poll":
        take(env, request).release()
    else:
        env.reader.progress()
        assert request.result() is None
        assert env.reader.capacity() == 4


def test_hit_decision_obeys_the_same_gate(env):
    request = env.start()
    env.reader.progress()
    with env.reader.gate:
        assert request.result() is None
    assert request.reason == "gate_busy"
    env.reader.progress()
    assert env.reader.capacity() == 4


def test_post_holds_the_gate_lock_from_check_to_post(env, monkeypatch):
    entered, leave = threading.Event(), threading.Event()
    original = env.agent.transfer

    def pause(handle):
        entered.set()
        assert leave.wait(2)
        return original(handle)

    monkeypatch.setattr(env.agent, "transfer", pause)
    requests = []
    posting = threading.Thread(target=lambda: requests.append(env.start()))
    posting.start()
    assert entered.wait(1)
    assert not env.reader.gate.acquire(False)
    busy = env.reader.start(env.outcome(), 0, env.unpins.extend)
    assert busy.done() and busy.result() is None
    advancing = threading.Thread(target=env.reader.advance)
    advancing.start()
    assert advancing.is_alive()
    leave.set()
    posting.join(2)
    advancing.join(2)
    assert not posting.is_alive() and not advancing.is_alive()
    assert take(env, requests[0]) is None


def test_cuda_error_during_post_must_not_disappear(env, monkeypatch):
    original = env.agent.transfer

    def stop(handle):
        env.reader.stop()
        return original(handle)

    monkeypatch.setattr(env.agent, "transfer", stop)
    request = env.start()
    assert request.result() is None
    assert env.unpins == []
    assert not env.reader._free or len(env.reader._free) == 3
    assert env.reader._flights
    assert not env.reader.close()
    assert env.reader in _KEPT_UNTIL_EXIT


@pytest.mark.parametrize(
    "notice", ["boot", "unknown", "registration", "disconnect", "removed"]
)
def test_peer_notice_misses_a_done_but_undecided_read(env, notice):
    request = env.start()
    env.reader.progress()
    changed = dataclasses.replace(env.peer)
    if notice == "boot":
        changed.boot = "boot:2"
    elif notice == "unknown":
        changed.generation_known = False
    elif notice == "registration":
        changed.pools = [dataclasses.replace(env.pool, pool_registration=8)]
    else:
        changed.conn_gen = 2
    env.reader.notify_peers([] if notice == "removed" else [changed])
    assert request.result() is None
    assert env.reader.capacity() == 4
    assert env.unpins == [request.outcome.pin_id]


def test_notice_between_check_and_post_cannot_be_lost(env, monkeypatch):
    original = env.agent.transfer

    def notice(handle):
        # The invalidation snapshot is visible even while the gate is owned.
        env.reader._peers = {}
        result = original(handle)
        return result

    monkeypatch.setattr(env.agent, "transfer", notice)
    request = env.start()
    assert request.reason == "generation"
    assert take(env, request) is None


def test_same_pool_new_registration_is_a_miss(env):
    hit = dataclasses.replace(env.outcome(), pool_registration=6)
    request = env.start(hit)
    assert request.result() is None
    assert request.reason == "stale_reg"
    assert env.reader.capacity() == 4


def test_generation_unknown_blocks_reads(env):
    env.reader.notify_peers([dataclasses.replace(env.peer, generation_known=False)])
    assert env.start().result() is None
    assert env.reader.capacity() == 4


def test_conn_lost_misses_now_and_a_stale_notice_spares_new_reads(env):
    newer = dataclasses.replace(env.peer, conn_gen=2)
    env.reader.notify_peers([newer])
    request = env.start()
    env.reader.progress()
    env.reader.notify_peers([env.peer])
    read = request.result()
    assert read is not None
    read.release()


def test_read_posted_before_the_reopen_never_hits_after_it(env):
    request = env.start()
    env.reader.progress()
    env.reader.advance()
    assert request.result() is None
    assert env.reader.capacity() == 4


def test_post_stalled_after_the_answer(env):
    generation = env.reader.capture()
    env.reader.advance()
    request = env.reader.start(env.outcome(), generation, env.unpins.extend)
    assert request.result() is None and request.reason == "generation"
    assert env.reader.capacity() == 4


def test_err_keeps_its_slot_until_the_staging_registration_ends(env):
    env.agent.fail_endpoint(env.remote._agent.name)
    request = env.start()
    env.reader.progress()
    assert request.result() is None
    assert request.flight.pending.finished and not request.flight.pending.completed
    assert env.reader.capacity() == 3
    env.reader.progress()
    assert env.reader.capacity() == 3
    assert not env.reader.close()
    assert env.reader.transport._registrations


def test_deadline_keeps_the_slot_until_the_late_done(env):
    env.agent.stall = True
    request = env.start(deadline=time.monotonic())
    assert request.result() is None
    assert env.reader.capacity() == 3
    env.agent.finish_stalled()
    env.reader.progress()
    assert env.reader.capacity() == 4


def test_kept_limit_misses_then_recovers_on_late_done(env):
    env.agent.stall = True
    for offset in (0, PAGE):
        request = env.start(env.outcome(offset), deadline=time.monotonic())
        assert request.result() is None
    request = env.start()
    assert request.result() is None and request.reason == "kept_limit"
    env.agent.finish_stalled()
    env.reader.progress()
    assert env.reader.capacity() == 4
    env.agent.stall = False
    take(env).release()


def test_cancel_releases_only_an_unissued_gpu_copy(env):
    env.agent.stall = True
    first, second = env.start(), env.start()
    first.cancel()
    assert first.result() is None
    assert env.reader.capacity() == 3
    env.agent.finish_stalled()
    read = take(env, second)
    second.cancel()
    assert env.reader.capacity() == 3
    read.release()
    assert env.reader.capacity() == 4


def test_close_keeps_bytes_until_gpu_copy_ends(env):
    read = take(env)
    assert not env.reader.close()
    assert bytes(read.view) == b"a" * PAGE
    assert env.reader.transport._registrations
    read.release()
    assert env.reader.transport._registrations


def test_pr0_stop_gives_nothing_back(env):
    read = take(env)
    unpins = list(env.unpins)
    env.reader.stop()
    read.release()
    assert not read._released
    assert env.unpins == unpins
    assert bytes(read.view) == b"a" * PAGE
    assert not env.reader.close()


@pytest.fixture
def writer(env):
    name = f"write-{uuid.uuid4().hex}"
    transport = NixlTransport(name, agent=FakeNixlAgent(name))
    reg = transport.register(buffer_address(env.memory), len(env.memory))
    pool = dataclasses.replace(
        env.pool, metadata=transport.metadata(), pool_registration=17
    )
    domain = WriteDomain("gpu", "gpu-run", "instance", 1)
    agent = WriteAgent(domain, name, transport.metadata(), [pool])
    credit = env.outcome().location
    assert env.reader.bind_credits([credit], agent, "gpu", "gpu-run")
    yield transport, reg, agent, credit
    transport.close()


def test_a_write_is_published_after_done(env, writer):
    _, _, agent, credit = writer
    buffer = env.reader.alloc_write(credit, PAGE)
    buffer.view[:] = b"w" * PAGE
    assert env.reader.write(buffer) == TransferEnd.DONE
    assert env.memory[:PAGE] == b"w" * PAGE
    assert buffer.agent.domain == agent.domain
    assert env.reader.capacity() == 4


def test_a_unknown_write_fences_its_domain_and_the_worker_gets_a_new_one(env, writer):
    transport, registration, agent, credit = writer
    env.agent.stall = True
    buffer = env.reader.alloc_write(credit, PAGE)
    buffer.view[:] = b"w" * PAGE
    assert env.reader.write(buffer) == TransferEnd.UNKNOWN
    # A timeout sends UNKNOWN; it does not reclaim the memory-node page.
    assert env.reader.alloc_write(credit, PAGE) is None
    transport.deregister(registration)
    env.agent.finish_stalled()
    assert env.memory[:PAGE] == b"a" * PAGE
    env.reader.progress()
    assert env.reader.capacity() == 4
    assert agent.domain in env.reader._retired_domains


def test_a_fence_revokes_the_domains_own_registration(env, writer):
    transport, registration, _, credit = writer
    transport.deregister(registration)
    buffer = env.reader.alloc_write(credit, PAGE)
    assert env.reader.write(buffer) == TransferEnd.UNKNOWN
    # The independent read registration still works after the write fence.
    read = take(env)
    assert bytes(read.view) == b"a" * PAGE
    read.release()


def test_a_stopped_writer_keeps_its_credit_until_exit(env, writer):
    _, _, _, credit = writer
    buffer = env.reader.alloc_write(credit, PAGE)
    env.reader.stop()
    assert env.reader.write(buffer) == TransferEnd.NOT_POSTED
    env.reader.release_write(buffer)
    assert not buffer.released
    assert env.reader._writing == {buffer}
    assert not env.reader.close()


@pytest.mark.parametrize(
    "field,value",
    [("instance", "other"), ("node_epoch", "old"), ("node_id", "other"), ("number", 0)],
)
def test_credit_is_bound_to_the_instance_domain(env, writer, field, value):
    _, _, agent, credit = writer
    bad = dataclasses.replace(
        agent, domain=dataclasses.replace(agent.domain, **{field: value})
    )
    assert not env.reader.bind_credits([credit], bad, "gpu", "gpu-run")


def test_peer_notice_does_not_revoke_a_decided_copy(env):
    read = take(env)
    env.reader.notify_peers([dataclasses.replace(env.peer, generation_known=False)])
    assert bytes(read.view) == b"a" * PAGE
    assert env.reader.capacity() == 3
    read.release()
    assert env.reader.capacity() == 4


def test_ref_is_owned_before_hit_handoff_is_visible(env, monkeypatch):
    request = env.start()
    env.reader.progress()
    observed = []

    def unpin(pins):
        observed.append((len(request.flight.consumers), env.reader.gate.locked()))
        env.unpins.extend(pins)

    request.unpin = unpin
    read = request.result()
    assert observed == [(1, False)]
    read.release()


def test_unknown_post_keeps_the_slot_until_late_done(env, monkeypatch):
    env.agent.stall = True
    post = env.agent.transfer

    def uncertain(handle):
        post(handle)
        raise RuntimeError("post outcome unknown")

    monkeypatch.setattr(env.agent, "transfer", uncertain)
    request = env.start()
    assert request.result() is None
    assert env.reader.capacity() == 3
    env.agent.finish_stalled()
    env.reader.progress()
    assert env.reader.capacity() == 4


def test_busy_finish_and_late_done_keep_existing_consumer(env):
    env.agent.stall = True
    a, b = env.start(), env.start()
    env.agent.finish_stalled()
    read = take(env, a)
    with env.reader.gate:
        assert b.result() is None
    env.reader.progress()
    assert env.reader.capacity() == 3
    assert bytes(read.view) == b"a" * PAGE
    read.release()
    assert env.reader.capacity() == 4


def test_two_domains_cannot_share_a_write_agent(env, writer):
    _, _, agent, credit = writer
    new = dataclasses.replace(agent, domain=dataclasses.replace(agent.domain, number=2))
    assert not env.reader.bind_credits([credit], new, "gpu", "gpu-run")


def test_old_credit_keeps_its_own_write_registration(env, writer):
    _, _, agent, credit = writer
    buffer = env.reader.alloc_write(credit, PAGE)
    agent.agent_name = "new-agent"
    agent.pools[0].base = 1
    assert buffer.agent.agent_name != agent.agent_name
    assert env.reader.write(buffer) == TransferEnd.DONE


@pytest.fixture
def client(env, writer):
    from types import SimpleNamespace

    from maru_common.node_protocol import AttachResponse, Budgets, CreditResponse
    from maru_common.storage_types import Status
    from maru_handler.client.maru_client import MaruClient
    from maru_handler.client.storage_conn import OpTracker
    from tests.unit.remote_fakes import FakeStorageControl

    _, _, agent, credit = writer
    c = MaruClient(env.reader.config)
    c._rdma = env.reader
    c._connected = True
    c.node_epoch = "gpu-run"
    c._ops = OpTracker(c.identity.incarnation, c.node_epoch)
    c.credits.reset(c.node_epoch)
    c.attach_info = AttachResponse(
        Status.OK,
        node_id="gpu",
        node_epoch=c.node_epoch,
        page_size=PAGE,
        budgets=Budgets(PAGE, PAGE, PAGE, PAGE, PAGE),
    )
    c._storage = FakeStorageControl(
        CreditResponse(Status.OK, credits=[credit], write_agent=agent)
    )
    c._server = SimpleNamespace(
        session="session",
        server_epoch="server",
        lookup_and_pin=lambda keys: (
            [env.outcome() for k in keys],
            "server",
            c.node_epoch,
        ),
        unpin=env.unpins.extend,
        freeze=lambda: None,
    )
    yield c
    c._server = None
    c._storage = None


def test_client_start_retrieve_and_release_preserve_gpu_ownership(env, client):
    pending = client.start_retrieve(["key"])
    assert not pending.done()
    env.reader.progress()
    assert pending.done()
    reads = pending.result()
    assert bytes(reads[0].view) == b"a" * PAGE
    assert env.reader.capacity() == 3
    client.release_retrieved(reads)
    assert env.reader.capacity() == 4


def test_client_generation_is_captured_before_the_lookup(env, client):
    original = client._server.lookup_and_pin

    def lookup(keys):
        result = original(keys)
        env.reader.advance()
        return result

    client._server.lookup_and_pin = lookup
    assert client.batch_retrieve(["key"]) == [None]
    assert env.reader.capacity() == 4
    assert client.last_retrieve_misses() == {"reader:generation": 1}


def test_client_cancel_keeps_the_slot_until_the_late_done(env, client):
    env.agent.stall = True
    pending = client.start_retrieve(["key"])
    pending.cancel()
    assert env.reader.capacity() == 3
    env.agent.finish_stalled()
    env.reader.progress()
    assert env.reader.capacity() == 4


def run_sender(client):
    task = client._sends.popleft()
    task()


def test_client_write_done_waits_for_memory_node_commit(env, client, writer):
    from maru_common.storage_types import Key

    assert client._refill_once()
    handle = client.alloc(PAGE)
    handle.buf[:] = b"w" * PAGE
    stored = client.store_async(["written"], [handle])
    run_sender(client)
    assert not stored.done()
    assert env.memory[:PAGE] == b"w" * PAGE
    assert not client._storage.published
    report = client._storage.writes[0][0].items[0]
    assert report.end == TransferEnd.DONE and report.domain == writer[2].domain
    client._storage.commit()
    assert stored.result() == [True]
    assert client._storage.published == {Key("ns", "written")}
    assert env.reader.capacity() == 4


def test_client_unknown_write_reports_without_returning_the_credit(env, client):
    from maru_common.node_protocol import NodeMessageType

    assert client._refill_once()
    handle = client.alloc(PAGE)
    handle.buf[:] = b"w" * PAGE
    env.agent.stall = True
    stored = client.store_async(["unknown"], [handle])
    run_sender(client)
    report = client._storage.writes[0][0].items[0]
    assert report.end == TransferEnd.UNKNOWN
    client._storage.commit()
    assert stored.result() == [False]
    assert not client._storage.published
    assert client.credits.unused_bytes == 0
    assert not client._sends
    assert [code for code, _ in client._storage.requests] == [
        NodeMessageType.CREDIT,
        NodeMessageType.WRITE_DONE,
    ]
    env.agent.finish_stalled()
    env.reader.progress()


def test_client_stop_returns_no_reads_slots_or_credit(env, client):
    assert client._refill_once()
    handle = client.alloc(PAGE)
    reads = client.start_retrieve(["key"])
    env.reader.progress()
    read = reads.result()[0]
    before = list(client._storage.requests)
    client.stop()
    client.release_retrieved([read])
    client.free(handle)
    client.close()
    assert not read._released and handle.state == "writing"
    assert list(client._storage.requests) == before
    assert env.reader.stopped.is_set()
    assert env.reader._writing and env.reader._flights


def test_peers_notification_uses_the_uds_sequence_zero(env):
    import socket

    from maru_common.node_protocol import (
        NODE_MESSAGE_CLASSES,
        NodeMessageType,
        Peers,
        send_frame,
    )
    from maru_common.serializer import Serializer
    from maru_handler.client.storage_conn import StorageConn

    local, remote = socket.socketpair()
    notices, received = [], threading.Event()

    def notify(message, conn):
        notices.append(message)
        received.set()

    conn = StorageConn("unused", on_notify=notify, on_lost=lambda _: None)
    conn._sock = local
    conn._receiver = threading.Thread(target=conn._receive_loop)
    conn._notifier = threading.Thread(target=conn._notify_loop)
    conn._receiver.start()
    conn.start_notifications()
    try:
        send_frame(
            remote,
            Serializer(NODE_MESSAGE_CLASSES).encode(
                NodeMessageType.PEERS, Peers([env.peer]), seq=0
            ),
        )
        assert received.wait(1)
        assert notices[0].peers == [env.peer]
        assert not conn.closed
    finally:
        conn.close()
        remote.close()
        conn._receiver.join(1)
        conn._notifier.join(1)


@pytest.mark.parametrize("page", [PAGE, 32 << 20])
def test_default_staging_holds_one_gib_or_sixty_four_pages(page):
    config = MaruConfig(
        maru_node="/unused",
        engine_id="engine",
        cache_namespace="ns",
        auto_connect=False,
        chunk_size_bytes=page,
    )
    name = uuid.uuid4().hex
    reader = WorkerRdma(
        config,
        page,
        progress=False,
        transport_factory=lambda _: NixlTransport(name, agent=FakeNixlAgent(name)),
    )
    try:
        assert reader.staging.nbytes == max(1 << 30, 64 * page)
        assert reader.slot_count >= 64
    finally:
        assert reader.close()


def test_post_and_notifications_under_real_thread_contention(env):
    errors = []
    start = threading.Barrier(3)

    def reads():
        try:
            start.wait()
            for _ in range(100):
                request = env.start(deadline=time.monotonic() + 0.03)
                env.reader.progress()
                read = request.result()
                if read is not None:
                    assert bytes(read.view) == b"a" * PAGE
                    read.release()
        except BaseException as error:
            errors.append(error)

    def notices():
        try:
            start.wait()
            for i in range(100):
                env.reader.notify_peers(
                    [dataclasses.replace(env.peer, boot=f"boot:{i}")]
                )
                env.reader.advance()
        except BaseException as error:
            errors.append(error)

    threads = [threading.Thread(target=reads), threading.Thread(target=notices)]
    for thread in threads:
        thread.start()
    start.wait()
    for thread in threads:
        thread.join(3)
    assert not any(t.is_alive() for t in threads)
    assert not errors
    env.reader.progress()
    assert env.reader.capacity() == 4


@pytest.mark.parametrize(
    "settings",
    [
        {"UCX_TLS": "tcp"},
        {"UCX_TLS": "all"},
        {"UCX_TLS": "rc,tcp"},
        {"UCX_TLS": "cuda_copy"},
        {"UCX_TLS": "rc", "UCX_RCACHE_ENABLE": "y"},
    ],
)
def test_worker_registration_requires_checked_transport(settings):
    from maru_remote.checked import UncheckedTransportError, require_checked_transport

    with pytest.raises(UncheckedTransportError):
        require_checked_transport(settings)


def test_checked_settings_precede_agent_creation(env, monkeypatch):
    import maru_remote.native_agent
    from maru_handler.client.worker_rdma import _transport

    observed = []

    def native(name, **kwargs):
        import os

        observed.append((os.environ["UCX_TLS"], os.environ["UCX_RCACHE_ENABLE"]))
        return FakeNixlAgent(name)

    monkeypatch.setenv("UCX_TLS", "")
    monkeypatch.setenv("UCX_RCACHE_ENABLE", "")
    monkeypatch.setattr(maru_remote.native_agent, "NativeNixlAgent", native)
    transport = _transport(env.reader.config, uuid.uuid4().hex)
    try:
        assert observed == [("rc,dc,cuda_copy", "n")]
    finally:
        transport.close()


def test_a_dead_writer_is_fenced_before_its_pages_are_reclaimed(env, writer):
    old_transport, old_registration, agent, credit = writer
    env.agent.stall = True
    old = env.reader.alloc_write(credit, PAGE)
    old.view[:] = b"o" * PAGE
    assert env.reader.write(old) == TransferEnd.UNKNOWN
    assert env.reader.alloc_write(credit, PAGE) is None
    # The receiver revokes the old domain before reusing this address.
    old_transport.deregister(old_registration)
    name = f"new-domain-{uuid.uuid4().hex}"
    new_transport = NixlTransport(name, agent=FakeNixlAgent(name))
    new_transport.register(buffer_address(env.memory), len(env.memory))
    successor = dataclasses.replace(
        agent,
        domain=dataclasses.replace(agent.domain, number=2),
        agent_name=name,
        agent_metadata=new_transport.metadata(),
        pools=[dataclasses.replace(env.pool, pool_registration=18)],
    )
    new_credit = dataclasses.replace(credit, generation=2, allocation_id="next-life")
    try:
        assert env.reader.bind_credits([new_credit], successor, "gpu", "gpu-run")
        env.agent.stall = False
        current = env.reader.alloc_write(new_credit, PAGE)
        current.view[:] = b"n" * PAGE
        assert env.reader.write(current) == TransferEnd.DONE
        env.agent.finish_stalled()
        env.reader.progress()
        assert env.memory[:PAGE] == b"n" * PAGE
        read = take(env)
        assert bytes(read.view) == b"n" * PAGE
        read.release()
    finally:
        new_transport.close()


def test_remote_only_staging_reports_cuda_registration(env, client, monkeypatch):
    assert client.pools.pool_ids() == []
    monkeypatch.setattr(env.reader.staging, "cuda_registered", True)
    assert client.storage_gpu_accessible()


def test_read_post_runs_on_the_calling_thread(env, monkeypatch):
    posted = []
    original = env.agent.transfer

    def post(handle):
        posted.append(threading.get_ident())
        return original(handle)

    monkeypatch.setattr(env.agent, "transfer", post)
    request = env.start()
    assert posted == [threading.get_ident()]
    take(env, request).release()


@pytest.mark.parametrize("boundary", ["post", "hit"])
def test_catalog_restart_done_before_notice_is_valid_after_notice_a_miss(
    env, client, boundary
):
    if boundary == "post":
        lookup = client._server.lookup_and_pin

        def restart(keys):
            result = lookup(keys)
            client._server.server_epoch = "new-server"
            return result

        client._server.lookup_and_pin = restart
        assert client.batch_retrieve(["key"]) == [None]
    else:
        request = client.start_retrieve(["key"])
        env.reader.progress()
        client._server.server_epoch = "new-server"
        assert request.result() == [None]
    assert env.reader.capacity() == 4


def test_same_run_refresh_moves_pins_without_invalidating_the_read(env, client):
    pending = client.start_retrieve(["key"])
    env.reader.progress()
    client._server.session = "refreshed-session"
    read = pending.result()[0]
    assert bytes(read.view) == b"a" * PAGE
    client.release_retrieved([read])
    assert env.reader.capacity() == 4


def test_close_waits_for_a_post_in_progress(env, monkeypatch):
    entered, leave, closed = threading.Event(), threading.Event(), threading.Event()
    original = env.agent.transfer
    requests = []

    def post(handle):
        entered.set()
        assert leave.wait(2)
        return original(handle)

    monkeypatch.setattr(env.agent, "transfer", post)
    posting = threading.Thread(target=lambda: requests.append(env.start()))
    posting.start()
    assert entered.wait(1)

    def close():
        env.reader.close()
        closed.set()

    closing = threading.Thread(target=close)
    closing.start()
    assert not closed.is_set()
    leave.set()
    posting.join(2)
    closing.join(2)
    assert closed.is_set()
    assert requests[0].result() is None
    assert env.reader.transport._registrations
