# SPDX-License-Identifier: Apache-2.0
"""SSD STAGE against real nodes, Catalog and workers, with a fake NIC."""

import concurrent.futures
import dataclasses
import threading
import time
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from maru_common.ms_rpc import PLACEHOLDER_OP
from maru_common.remote_protocol import RemoteMessageType as R
from maru_common.remote_protocol import (
    RoomLimit,
    StageRequest,
    TransferEnd,
    UnstageRequest,
)
from maru_common.storage_types import ItemResult, Key, MissReason, PoolKind, Status
from maru_storage_service.page_table import PageState
from maru_storage_service.remote_client import RemoteConnection
from tests.unit.ms_node_helpers import PAGE, wait_until
from tests.unit.ms_remote_helpers import NS, sentinel, state
from tests.unit.test_ms_fetch import loader_client, pins, settled
from tests.unit.test_ms_remote_demotion import workers as workers
from tests.unit.test_ms_remote_gate import gate as gate
from tests.unit.test_ms_remote_gate import ms as ms
from tests.unit.test_ms_remote_gate import observe_posts, wait_state


@pytest.fixture
def staged_env(gate):
    memory = gate.memory(("dram", "ssd"))
    peer = gate.peer(memory)
    peer.write("staged")
    gate.pressure(memory, "dram")
    wait_until(
        lambda: [p.kind for p in gate.locations("staged", memory)] == [PoolKind.SSD],
        what="memory pressure moved source to SSD",
    )
    source = gate.locations("staged", memory)[0]
    answered = set()
    stage_answers = []
    ops = memory.peer_ops
    original_stage, original_unstage = ops.stage, ops.unstage

    def stage(origin, request, deadline, done):
        def answer(response):
            stage_answers.append(response)
            if response.staging is not None:
                loc = response.staging
                answered.add((loc.pool_id, loc.offset // PAGE, loc.generation))
            done(response)

        return original_stage(origin, request, deadline, answer)

    def unstage(origin, request, done):
        if request.end in (TransferEnd.DONE, TransferEnd.NOT_POSTED):
            key = (origin, request.stage_id)
            with ops._lock:
                entry = ops._staged.get(key)
                loc = entry[1] if entry else ops._staged_quarantine.get(key)
            if loc is not None:
                gate.permit(memory, loc, request.end.value)
        return original_unstage(origin, request, done)

    gate.monkeypatch.setattr(ops, "stage", stage)
    gate.monkeypatch.setattr(ops, "unstage", unstage)
    for table in memory.pools._tables.values():
        move = table.move

        def checked(page, src, dst, table=table, move=move):
            token = (table.pool_id, page.index, page.generation)
            if dst == PageState.FREE and token in answered:
                assert (id(table), page.index, page.generation) in gate.evidence
            return move(page, src, dst)

        gate.monkeypatch.setattr(table, "move", checked)
    gpu = gate.gpu(memory)
    reader = gate.loader(gpu)
    env = SimpleNamespace(
        gate=gate,
        memory=memory,
        peer=peer,
        source=source,
        gpu=gpu,
        reader=reader,
        remote=gpu.tier("remote"),
        stage_answers=stage_answers,
    )
    yield env
    gate.check()


@pytest.fixture
def routed(staged_env):
    """Isolate FetchManager from the separately tested SSD ownership bug."""
    env = staged_env
    remote = env.remote
    owner, node_of = remote.owner, remote._node_of
    node = remote._nodes[0]
    env.gate.monkeypatch.setattr(
        remote,
        "owner",
        lambda pool: node.node_id if pool == env.source.pool_id else owner(pool),
    )
    env.gate.monkeypatch.setattr(
        remote,
        "_node_of",
        lambda pool: node if pool == env.source.pool_id else node_of(pool),
    )
    return env


def stage_async(env, *, peer=None, stage_id=1, deadline_ms=5000):
    peer = peer or env.peer
    request = peer.ops.stamp(
        StageRequest(
            PLACEHOLDER_OP, stage_id, Key(NS, "staged"), env.source, deadline_ms
        )
    )
    future = concurrent.futures.Future()

    def answered(response):
        if isinstance(response, Exception):
            future.set_exception(response)
        else:
            peer.ops.resolved(request.op.seq)
            future.set_result(response)

    peer.conn.submit(R.STAGE, request, answered)
    return future


def unstage(env, stage_id=1, end=TransferEnd.NOT_POSTED, *, peer=None):
    peer = peer or env.peer
    return peer.call(R.UNSTAGE, UnstageRequest(PLACEHOLDER_OP, stage_id, end))


def observe_unstages(env):
    calls = []
    original = env.remote.unstage

    def record(staged, end):
        calls.append((staged, end))
        return original(staged, end)

    env.gate.monkeypatch.setattr(env.remote, "unstage", record)
    return calls


@contextmanager
def hold_loads(env):
    release = threading.Event()
    entered = []
    original = env.memory.peer_ops._load

    def load(request, deadline):
        entered.append(request.stage_id)
        assert release.wait(10), "test did not release SSD load"
        return original(request, deadline)

    with env.gate.monkeypatch.context() as patch:
        patch.setattr(env.memory.peer_ops, "_load", load)
        try:
            yield entered, release
        finally:
            release.set()


def fill_workers(env, entered):
    futures = []
    for i in range(env.memory.remote.server._worker_count):
        futures.append(stage_async(env, stage_id=100 + i))
        wait_until(lambda i=i: len(entered) == i + 1, what="stage worker occupied")
    return futures


def free_stages(env, futures):
    for i, future in enumerate(futures):
        answer = future.result(3)
        if answer.staging is not None:
            ref = env.memory.pools.find(answer.staging)
            assert unstage(env, 100 + i).status == Status.OK
            assert env.memory.pools.page(ref).state == PageState.FREE


def test_ssd_fetch_is_a_hit_without_routing_shim(staged_env):
    env = staged_env
    answer = env.reader.fetch("staged").items[0]
    assert answer.result == ItemResult.HIT
    assert env.reader.read(answer.location) == sentinel("staged")
    env.reader.unpin(answer.pin_id)


def test_fetch_reads_staging_bytes_and_eviction_finalizes_onto_ssd(routed):
    env = routed
    calls = observe_unstages(env)
    _, tickets = observe_posts(env.gate, env.remote, "submit_load")
    answer = env.reader.fetch("staged").items[0]
    assert answer.result == ItemResult.HIT
    assert env.reader.read(answer.location) == sentinel("staged")
    env.reader.unpin(answer.pin_id)
    wait_until(lambda: calls, what="UNSTAGE done")
    staged, end = calls[0]
    assert end == TransferEnd.DONE and tickets[0].ok
    assert staged.location != env.source
    wait_until(lambda: state(env.memory, staged.location) is None, what="staging FREE")
    ref = env.gpu.pools.find(answer.location)
    assert env.gpu.manager._remote_copies[ref] == env.source
    wait_until(lambda: settled(env.gpu, env.gate.server), what="fetch pins released")
    _, writes = observe_posts(env.gate, env.remote, "submit_store")
    env.gate.pressure(env.gpu, "dram")
    env.gate.moved(env.gpu, "staged")
    assert not writes and env.gate.locations("staged", env.memory) == [env.source]


def test_maru_handler_fetch_scope_reads_ssd_bytes_through_stage(routed):
    env = routed
    calls = observe_unstages(env)
    handler = loader_client(env.gpu)
    env.gate.clients.append(handler)
    with handler.fetch_scope("ssd-stage"):
        read = handler.retrieve("staged")
        assert read is not None
        assert bytes(read.view) == sentinel("staged")
        handler.release_retrieved([read])
    wait_until(lambda: calls, what="handler UNSTAGE")
    assert calls[0][1] == TransferEnd.DONE
    wait_until(lambda: state(env.memory, calls[0][0].location) is None, what="FREE")
    assert handler._cpu.stats["fetch_hit"] == 1


@pytest.mark.parametrize("bad", ["not-ready", "no-stage", "length", "owner"])
def test_unavailable_or_malformed_ssd_plan_is_immediate_offline(routed, bad):
    env = routed
    items = env.reader.items("staged")
    if bad == "not-ready":
        env.gate.monkeypatch.setattr(env.remote, "ready", lambda _n: False)
    elif bad == "no-stage":
        env.gate.monkeypatch.setattr(env.remote, "stages", lambda _n: False)
    elif bad == "owner":
        items[0] = dataclasses.replace(
            items[0], plan=dataclasses.replace(items[0].plan, source_node="other")
        )
    else:
        items[0] = dataclasses.replace(
            items[0],
            plan=dataclasses.replace(items[0].plan, bytes=env.source.length - 1),
        )
    _, tickets = observe_posts(env.gate, env.remote, "submit_load")
    answer = env.reader.send(env.reader.request(items)).result(1).items[0]
    assert (answer.result, answer.reason) == (ItemResult.MISS, MissReason.OFFLINE)
    assert not tickets and pins(env.gate.server, "staged", env.source) == 0


def test_full_stage_queue_is_busy_and_fetch_misses_without_blocking_control(routed):
    env = routed
    env.memory.remote.server._queue_limit = 1
    with hold_loads(env) as (entered, release):
        running = fill_workers(env, entered)
        queued = stage_async(env, stage_id=200)
        wait_until(lambda: len(env.memory.remote.server._queue) == 1, what="queue full")
        busy = stage_async(env, stage_id=201).result(1)
        assert busy.status == Status.BUSY and busy.reason == MissReason.BUSY
        assert env.reader.fetch("staged").items[0].result == ItemResult.MISS
        wait_until(lambda: settled(env.gpu, env.gate.server), what="busy FETCH cleanup")
        assert pins(env.gate.server, "staged", env.source) == 0
        healthy = env.gate.peer(env.memory, "healthy")
        started = time.monotonic()
        assert healthy.ping().status == Status.OK
        location = healthy.write("healthy")
        assert location and time.monotonic() - started < 0.8
        release.set()
        free_stages(env, running)
        answer = queued.result(3)
        if answer.staging:
            unstage(env, 200)


def test_deadline_passes_in_stage_queue_and_fetch_misses(routed):
    env = routed
    with hold_loads(env) as (entered, release):
        running = fill_workers(env, entered)
        queued = stage_async(env, stage_id=200, deadline_ms=40)
        wait_until(lambda: len(env.memory.remote.server._queue) == 1, what="queued")
        answer = env.reader.fetch("staged", deadline_ms=60).items[0]
        assert answer.result == ItemResult.MISS
        release.set()
        response = queued.result(3)
        assert response.staging is None and response.reason == MissReason.TIMEOUT
        free_stages(env, running)
    wait_until(lambda: env.gpu.manager.fetches.flights() == 0, what="expired flight")
    assert pins(env.gate.server, "staged", env.source) == 0


def test_lost_stage_answer_sends_not_posted_and_frees_answered_page(routed):
    env = routed
    lost, staging = threading.Event(), []
    submit = RemoteConnection.submit
    calls = observe_unstages(env)

    def send(conn, code, request, callback):
        if code != R.STAGE or request.op.origin != env.remote.origin:
            return submit(conn, code, request, callback)

        def discard(response):
            assert response.staging is not None
            staging.append(response.staging)
            lost.set()
            # Lose the answer after the memory node allocated its page.
            # A 150 ms fetch deadline could instead expire before STAGE
            # ran, testing no lost answer and no allocated-page cleanup.
            callback(ConnectionError("the STAGE answer was lost"))

        return submit(conn, code, request, discard)

    env.gate.monkeypatch.setattr(RemoteConnection, "submit", send)
    _, tickets = observe_posts(env.gate, env.remote, "submit_load")
    answer = env.reader.fetch("staged").items[0]
    assert lost.is_set() and answer.result == ItemResult.MISS
    wait_until(lambda: calls, what="UNSTAGE not_posted")
    assert calls[0][1] == TransferEnd.NOT_POSTED
    wait_until(lambda: state(env.memory, staging[0]) is None, what="lost answer FREE")
    wait_until(lambda: settled(env.gpu, env.gate.server), what="MISS cleanup")
    assert not tickets and pins(env.gate.server, "staged", env.source) == 0


def test_unstage_before_stage_tombstones_and_duplicate_late_unstage_is_noop(staged_env):
    env = staged_env
    assert unstage(env).status == Status.OK
    answer = stage_async(env).result(2)
    assert answer.status == Status.OK and answer.staging is None
    assert not env.memory.peer_ops._staged
    answer = stage_async(env, stage_id=2).result(2)
    assert answer.staging is not None
    ref = env.memory.pools.find(answer.staging)
    assert unstage(env, 2).status == Status.OK
    generation = env.memory.pools.page(ref).generation
    for end in TransferEnd:
        assert unstage(env, 2, end).status == Status.OK
    page = env.memory.pools.page(ref)
    assert page.state == PageState.FREE and page.generation == generation


@pytest.mark.parametrize("wrong", ["key", "generation", "length"])
def test_stage_requires_the_exact_ready_ssd_source(staged_env, wrong):
    env = staged_env
    key, source = Key(NS, "staged"), env.source
    if wrong == "key":
        key = Key(NS, "another")
    elif wrong == "generation":
        source = dataclasses.replace(source, generation=source.generation + 1)
    else:
        source = dataclasses.replace(source, length=source.length - 1)
    answer = env.peer.call(R.STAGE, StageRequest(PLACEHOLDER_OP, 1, key, source, 5000))
    assert answer.staging is None and answer.reason == MissReason.ABSENT
    assert not env.memory.peer_ops._staged
    assert env.memory.peer_ops._staging_bytes[env.peer.ops.origin] == 0


@pytest.mark.parametrize(
    "event",
    [
        "unstage",
        "deadline",
        "peer-lost",
    ],
)
def test_inflight_ssd_load_holds_io_refs_and_frees_unanswered_stage(staged_env, event):
    env = staged_env
    entered, release = threading.Event(), threading.Event()
    pool = env.memory.tier("ssd").pool
    original = pool.read
    reads = []

    def read(*args, **kwargs):
        entered.set()
        assert release.wait(5), "SSD read not released"
        reads.append(True)
        return original(*args, **kwargs)

    with env.gate.monkeypatch.context() as patch:
        patch.setattr(pool, "read", read)
        future = stage_async(env)
        try:
            assert entered.wait(2)
            source_ref = env.memory.pools.find(env.source)
            assert env.memory.pools.io_refs(source_ref) == 1
            assert env.memory.peer_ops._staging_bytes[env.peer.ops.origin] == PAGE
            if event == "unstage":
                assert unstage(env).status == Status.OK
            elif event == "deadline":
                patch.setattr(
                    env.memory.peer_ops, "_clock", lambda: time.monotonic() + 10
                )
            else:
                env.peer.conn.close()
                wait_until(
                    lambda: (
                        env.memory.remote.server._peers[env.peer.node_id].connection
                        is None
                    ),
                    what="peer loss during SSD read",
                )
        finally:
            release.set()
        if event != "peer-lost":
            answer = future.result(2)
            assert answer.staging is None
            if event == "deadline":
                assert answer.reason == MissReason.TIMEOUT
        wait_until(lambda: reads, what="SSD read ended")
        wait_until(
            lambda: env.memory.pools.io_refs(source_ref) == 0, what="I/O refs released"
        )
        wait_until(lambda: env.stage_answers, what="STAGE completed after SSD read")
    assert env.stage_answers[-1].staging is None
    assert env.memory.peer_ops._staging_bytes[env.peer.ops.origin] == 0
    assert not env.memory.peer_ops._staged


@pytest.mark.parametrize(
    "fault",
    [
        "timeout",
        "err",
    ],
)
def test_kept_read_quarantines_both_pages_until_late_done(routed, fault):
    env = routed
    env.remote.timeout_s = 0.1
    calls = observe_unstages(env)
    entered, tickets = observe_posts(env.gate, env.remote, "submit_load")
    env.remote.transport.stall()
    reply = env.reader.send(env.reader.request(env.reader.items("staged")))
    assert entered.wait(2)
    if fault == "err":
        assert env.remote.transport.fail_stalled(late=True) == 1
    assert reply.result(2).items[0].result == ItemResult.MISS
    wait_until(lambda: calls, what="UNSTAGE unknown")
    staged, end = calls[0]
    assert end == TransferEnd.UNKNOWN
    dest = tickets[0].dst
    dest_location = env.gpu.pools._tables[dest.pool_id].location(
        env.gpu.pools.page(dest)
    )
    wait_state(env.memory, staged.location, PageState.QUARANTINED)
    assert env.gpu.pools.page(dest).state == PageState.QUARANTINED
    assert pins(env.gate.server, "staged", env.source) == 0
    env.gate.check()
    env.gate.permit(env.gpu, dest_location, "late READ DONE")
    env.remote.transport.stall(False)
    if fault == "err":
        assert env.remote.transport.deliver_late() == 1
        assert not tickets[0].pending.completed
        assert env.gpu.pools.page(dest).state == PageState.QUARANTINED
        assert state(env.memory, staged.location) == PageState.QUARANTINED
        assert not any(end == TransferEnd.DONE for _, end in calls)
        # A packet is no evidence; inject a separate confirmed late DONE.
        tickets[0].settled(tickets[0])
    else:
        assert env.remote.transport.finish_stalled() == 1
    wait_until(
        lambda: any(end == TransferEnd.DONE for _, end in calls),
        timeout=1,
        what="late DONE UNSTAGE",
    )
    wait_until(
        lambda: env.gpu.pools.page(dest).state == PageState.FREE, what="dest FREE"
    )
    assert not env.gate.locations("staged", env.gpu)
    wait_until(lambda: state(env.memory, staged.location) is None, what="staging FREE")


def test_submit_raised_unstages_not_posted_and_frees_destination(routed):
    env = routed
    calls = observe_unstages(env)

    def refuse(*args, **kwargs):
        raise RuntimeError("READ never posted")

    env.gate.monkeypatch.setattr(env.gpu.transfers, "submit_io", refuse)
    assert env.reader.fetch("staged").items[0].result == ItemResult.MISS
    wait_until(lambda: calls, what="UNSTAGE not_posted")
    assert calls[0][1] == TransferEnd.NOT_POSTED
    wait_until(lambda: state(env.memory, calls[0][0].location) is None, what="FREE")
    wait_until(lambda: settled(env.gpu, env.gate.server), what="failed submit cleanup")
    assert pins(env.gate.server, "staged", env.source) == 0
    assert env.gpu.manager.fetches.inflight_bytes == 0


def test_stage_wait_releases_fetch_executor_and_cancel_unstages_before_read(routed):
    env = routed
    calls = observe_unstages(env)
    _, tickets = observe_posts(env.gate, env.remote, "submit_load")
    fetches = env.gpu.manager.fetches
    fetches._tasks.shutdown()
    fetches._tasks = concurrent.futures.ThreadPoolExecutor(1)
    with hold_loads(env) as (entered, release):
        request = env.reader.request(env.reader.items("staged"))
        reply = env.reader.send(request)
        wait_until(lambda: entered, what="STAGE waiting on SSD")
        assert fetches._tasks.submit(lambda: "available").result(0.5) == "available"
        assert pins(env.gate.server, "staged", env.source) == 1
        env.reader.cancel(request)
        assert reply.result(1).items[0].result == ItemResult.MISS
        release.set()
        wait_until(lambda: calls, what="cancel UNSTAGE")
    assert calls[0][1] == TransferEnd.NOT_POSTED and not tickets
    wait_until(lambda: state(env.memory, calls[0][0].location) is None, what="FREE")
    wait_until(
        lambda: settled(env.gpu, env.gate.server), what="canceled flight settled"
    )


@pytest.mark.parametrize(
    "order",
    [
        "credit-first",
        "stage-first",
    ],
)
def test_staging_and_reserves_share_the_peer_credit_cap(staged_env, order):
    env = staged_env
    cap = env.memory.pools.budgets.credit_max_bytes // PAGE
    if order == "credit-first":
        credits = env.peer.reserve(cap).credits
        answer = stage_async(env).result(2)
        assert answer.staging is None and answer.reason == MissReason.BUSY
        env.peer.cancel(credits)
    else:
        answer = stage_async(env).result(2)
        assert answer.staging is not None
        credits = env.peer.reserve(cap).credits
        held = (
            len(credits)
            + env.memory.peer_ops._staging_bytes[env.peer.ops.origin] // PAGE
        )
        env.peer.cancel(credits)
        unstage(env)
        assert held == cap


@pytest.mark.parametrize(
    "cause",
    [
        "disconnect",
        "ttl",
        "retired",
    ],
)
def test_answered_stage_quarantines_until_its_own_runs_evidence(staged_env, cause):
    env = staged_env
    answer = stage_async(env).result(2)
    location = answer.staging
    assert location is not None
    origin = env.peer.ops.origin
    if cause == "disconnect":
        env.peer.conn.close()
        wait_state(env.memory, location, PageState.QUARANTINED)
        env.peer.connect()
    elif cause == "ttl":
        env.memory.peer_ops._clock = lambda: time.monotonic() + 1000
        assert env.memory.peer_ops.sweep() == 1
    else:
        env.gate.peer(env.memory, env.peer.node_id, "new-run")
        wait_state(env.memory, location, PageState.QUARANTINED)
    assert state(env.memory, location) == PageState.QUARANTINED
    other = env.gate.peer(env.memory, "other")
    assert unstage(env, peer=other).status == Status.OK
    assert state(env.memory, location) == PageState.QUARANTINED
    assert env.memory.peer_ops.quarantined[env.peer.node_id] == PAGE
    if cause != "retired":
        assert unstage(env).status == Status.OK
        assert state(env.memory, location) is None
        assert env.memory.peer_ops.quarantined[env.peer.node_id] == 0
        assert (origin, 1) not in env.memory.peer_ops._staged_quarantine


@pytest.mark.parametrize("cap", ["peer", "node"])
def test_memory_quarantine_cap_answers_stage_with_quarantine(staged_env, cap):
    env = staged_env
    if cap == "peer":
        env.memory.peer_ops.peer_quarantine_max = PAGE
    else:
        env.memory.peer_ops.quarantine_max = PAGE
    answer = stage_async(env).result(2)
    assert answer.staging is not None
    assert unstage(env, end=TransferEnd.UNKNOWN).status == Status.OK
    target = env.peer if cap == "peer" else env.gate.peer(env.memory, "other")
    refused = stage_async(env, peer=target, stage_id=2).result(2)
    assert refused.staging is None
    assert (refused.reason, refused.limit) == (MissReason.BUSY, RoomLimit.QUARANTINE)
    assert state(env.memory, answer.staging) == PageState.QUARANTINED


def test_a_stage_whose_connection_ends_misses_at_once(routed):
    env = routed
    calls = observe_unstages(env)
    node = env.remote._nodes[0]
    with hold_loads(env) as (entered, release):
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            start = time.monotonic()
            fetch = pool.submit(env.reader.fetch, "staged", deadline_ms=5000)
            wait_until(lambda: entered, what="STAGE loading")
            node.conn.close()  # the stream ends with the STAGE unanswered
            answer = fetch.result(3).items[0]
        assert time.monotonic() - start < 2.5  # not the 5 s deadline
        assert answer.result == ItemResult.MISS
        assert calls and calls[0][1] == TransferEnd.NOT_POSTED
        release.set()
        # Reconnected to the same run: the replayed STAGE's page is freed
        wait_until(
            lambda: env.remote.ready(node.node_id) and node.conn is not None,
            what="reconnected",
        )
        wait_until(
            lambda: (
                not env.memory.peer_ops._staged
                and not env.memory.peer_ops._staging_bytes[env.remote.origin]
            ),
            what="staging page freed",
        )
    wait_until(lambda: settled(env.gpu, env.gate.server), what="MISS cleanup")
    assert pins(env.gate.server, "staged", env.source) == 0


def test_an_unstage_whose_send_fails_neither_blocks_fetches_nor_leaks(routed):
    from maru_common.protocol import MessageHeader
    from maru_storage_service import remote_client

    env = routed
    real = remote_client.send_frame
    failed = []

    def send(sock, frame):  # the first UNSTAGE meets a reset stream
        if MessageHeader.unpack(frame).msg_type == R.UNSTAGE and not failed:
            failed.append(frame)
            raise BrokenPipeError(32, "Broken pipe")
        return real(sock, frame)

    env.gate.monkeypatch.setattr(remote_client, "send_frame", send)
    node = env.remote._nodes[0]
    answer = env.reader.fetch("staged").items[0]
    assert answer.result == ItemResult.HIT
    env.reader.unpin(answer.pin_id)
    wait_until(lambda: failed, what="the UNSTAGE send failed")
    fetches = env.gpu.manager.fetches
    wait_until(lambda: not fetches._lock.locked(), timeout=2, what="no thread stuck")
    # Reconnected to the same run: the UNSTAGE is sent again, the page freed
    wait_until(
        lambda: env.remote.ready(node.node_id) and not node.conn.closed,
        what="reconnected",
    )
    wait_until(
        lambda: (
            not env.memory.peer_ops._staged
            and not env.memory.peer_ops._staging_bytes[env.remote.origin]
        ),
        what="staging page freed",
    )
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        assert pool.submit(fetches.flights).result(2) == 0  # its lock is free
    wait_until(lambda: settled(env.gpu, env.gate.server), what="pins released")


def test_an_unstage_lost_while_a_reconnect_ran_is_sent_on_the_new_connection(
    routed, monkeypatch
):
    from maru_common.protocol import MessageHeader
    from maru_storage_service import remote_client

    env = routed
    tier = env.remote
    node = tier._nodes[0]
    got = []
    tier.stage(Key(NS, "staged"), env.source, time.monotonic() + 5, got.append)
    wait_until(lambda: got, what="staged")
    page = got[0]
    assert page is not None
    key = (tier.origin, page.stage_id)
    old = node.conn
    send = remote_client.send_frame

    def fail_unstage(sock, frame):
        if sock is old._sock and MessageHeader.unpack(frame).msg_type == R.UNSTAGE:
            raise BrokenPipeError(32, "Broken pipe")
        return send(sock, frame)

    monkeypatch.setattr(remote_client, "send_frame", fail_unstage)
    fetches = env.gpu.manager.fetches
    # The close hook waits on this lock while the connect loop reconnects:
    # the UNSTAGE's error arrives after the new connection took the replays
    with fetches._lock:
        tier.unstage(page, TransferEnd.NOT_POSTED)
        wait_until(
            lambda: node.conn is not old and node.ready, timeout=5, what="reconnected"
        )
    old._reader.join(3)
    assert not old._reader.is_alive()
    # Sent on the new connection at once: no second reconnect needed
    wait_until(
        lambda: (
            not node.unanswered
            and key not in env.memory.peer_ops._staged
            and key not in env.memory.peer_ops._staged_quarantine
        ),
        what="the staging page freed",
    )
    assert node.conn is not old and not node.conn.closed
