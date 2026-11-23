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
from maru_handler.client.worker_rdma import _GATE_WAIT_S, _KEPT_UNTIL_EXIT, WorkerRdma
from maru_remote.transport import NixlTransport, buffer_address
from tests.unit.remote_fakes import FakeNixlAgent

PAGE = 4096


@pytest.fixture
def env(request):
    settings = getattr(request, "param", {})
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
        rdma_staging_size=settings.get("slots", 4) * PAGE,
        rdma_timeout_s=0.03,
    )
    reader = WorkerRdma(
        config,
        PAGE,
        transport_factory=lambda name: NixlTransport(name, agent=agent),
        progress=settings.get("progress", False),
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


def test_progress_polls_reads_quickly_and_idles_while_the_gpu_owns_a_slot(env):
    env.agent.stall = True
    request = env.start(deadline=time.monotonic() + 1)
    waits, reads = [], []

    class Wake:
        def wait(self, timeout=None):
            waits.append(timeout)
            if len(waits) == 1:
                assert not request.ready.is_set()
                env.agent.finish_stalled()
            else:
                assert request.ready.is_set()
                reads.append(request.result())
                assert reads[0] is not None
                assert env.reader.capacity() == 3
                env.reader.closed = True

        def clear(self):
            pass

    wake = env.reader._wake
    env.reader._wake = Wake()
    try:
        env.reader._run()
    finally:
        env.reader._wake = wake
        env.reader.closed = False
        for read in reads:
            read.release()
    assert waits[0] > 0 and waits[1] is None
    assert env.reader.capacity() == 4


def test_batched_reads_share_a_transfer_and_keep_each_gpu_slot(env, monkeypatch):
    batch, posts, polls = [], [], []
    env.agent.stall = True
    requests = [env.start(env.outcome(offset), batch=batch) for offset in (0, PAGE)]
    assert all(r.flight.pending is None for r in requests)
    post = env.reader.transport.post_nowait
    poll = env.reader.transport.poll_nowait

    def posted(op, peer, pairs):
        assert env.reader.gate.locked()
        assert all(r.flight in env.reader._flights for r in requests)
        assert len(env.reader._free) == 2
        posts.append(pairs)
        return post(op, peer, pairs)

    def polled(pending):
        polls.append(pending)
        return poll(pending)

    monkeypatch.setattr(env.reader.transport, "post_nowait", posted)
    monkeypatch.setattr(env.reader.transport, "poll_nowait", polled)
    env.reader.post_reads(batch)
    assert len(posts) == 1 and len(posts[0]) == 2
    assert requests[0].flight.pending is requests[1].flight.pending
    env.reader.progress()
    assert len(polls) == 1
    env.agent.finish_stalled()
    env.reader.progress()
    first, second = [r.result() for r in requests]
    assert bytes(first.view) == b"a" * PAGE
    assert bytes(second.view) == b"b" * PAGE
    assert len(env.unpins) == 2
    first.release()
    assert env.reader.capacity() == 3
    second.release()
    assert env.reader.capacity() == 4


def test_a_consumer_does_not_hide_an_undecided_joiners_invalidation(env):
    env.agent.stall = True
    first, second = env.start(), env.start()
    env.agent.finish_stalled()
    env.reader.progress()
    read = first.result()
    assert read is not None
    env.reader.advance()
    env.reader.progress()
    assert second.result() is None and second.reason == "generation"
    assert env.reader.capacity() == 3
    read.release()
    assert env.reader.capacity() == 4


@pytest.mark.parametrize("change", ["generation", "peer", "current"])
def test_batched_reads_recheck_lifetimes_before_post(env, monkeypatch, change):
    batch, posts = [], []
    current = [True]
    first = env.start(batch=batch, current=lambda: current[0])
    second = env.start(env.outcome(PAGE), batch=batch)
    if change == "generation":
        env.reader.advance()
    elif change == "peer":
        env.reader.notify_peers([dataclasses.replace(env.peer, conn_gen=2)])
    else:
        current[0] = False
    post = env.reader.transport.post_nowait

    def posted(op, peer, pairs):
        posts.append(pairs)
        return post(op, peer, pairs)

    monkeypatch.setattr(env.reader.transport, "post_nowait", posted)
    env.reader.post_reads(batch)
    env.reader.progress()
    assert first.result() is None and first.reason == "generation"
    read = second.result()
    if change == "current":
        assert len(posts) == 1 and len(posts[0]) == 1
        assert bytes(read.view) == b"b" * PAGE
        read.release()
    else:
        assert not posts and read is None
    assert len(env.unpins) == 2
    assert env.reader.capacity() == 4


@pytest.mark.parametrize("complete", [True, False])
def test_uncertain_batch_keeps_every_slot_until_completion(env, monkeypatch, complete):
    batch = []
    env.agent.stall = True
    requests = [env.start(env.outcome(offset), batch=batch) for offset in (0, PAGE)]
    post = env.reader.transport.post_nowait

    def uncertain(*args):
        pending = post(*args)
        error = RuntimeError("post failed")
        error.pending_transfer = pending
        raise error

    monkeypatch.setattr(env.reader.transport, "post_nowait", uncertain)
    env.reader.post_reads(batch)
    assert all(r.result() is None and r.reason == "io_error" for r in requests)
    assert env.reader.snapshot()["free_slots"] == 2
    assert env.reader.capacity() == 0
    assert len(env.unpins) == 2
    if complete:
        env.agent.finish_stalled()
    else:
        env.agent.fail_stalled(late=True)
    env.reader.progress()
    assert env.reader.snapshot()["free_slots"] == (4 if complete else 2)
    if not complete:
        env.agent.deliver_late()
        env.reader.progress()
        assert env.reader.snapshot()["free_slots"] == 2


def test_busy_batch_post_misses_without_posting_or_leaking_slots(env):
    batch = []
    requests = [env.start(env.outcome(offset), batch=batch) for offset in (0, PAGE)]
    with env.reader.gate:
        env.reader.post_reads(batch)
    assert all(r.flight.pending is None for r in requests)
    assert all(r.result() is None and r.reason == "gate_busy" for r in requests)
    assert len(env.unpins) == 2
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


@pytest.mark.parametrize("env", [{"slots": 64, "progress": True}], indirect=True)
def test_repeated_starts_with_progress_have_no_gate_busy_misses(env, monkeypatch):
    polling_threads = []
    poll = env.reader.transport.poll_nowait

    def observe(pending):
        polling_threads.append(threading.current_thread())
        return poll(pending)

    monkeypatch.setattr(env.reader.transport, "poll_nowait", observe)
    requests = []
    for i in range(64):
        assert env.reader.capacity() > 0
        generation = env.reader.capture()
        assert generation == 0
        outcome = env.outcome()
        outcome = dataclasses.replace(
            outcome,
            location=dataclasses.replace(outcome.location, allocation_id=str(i)),
        )
        requests.append(
            env.reader.start(
                outcome, generation, env.unpins.extend, deadline=time.monotonic() + 2
            )
        )
    reads = []
    try:
        for request in requests:
            read = request.result()
            assert request.reason != "gate_busy"
            assert read is not None
            reads.append(read)
            assert bytes(read.view) == b"a" * PAGE
        assert len(env.unpins) == 64
        assert polling_threads and all(t is env.reader._thread for t in polling_threads)
    finally:
        for read in reads:
            read.release()


@pytest.mark.parametrize("operation", ["start", "finish", "capacity", "capture"])
def test_gate_wait_expires_while_the_owner_keeps_the_gate(env, operation):
    request = env.start(deadline=time.monotonic() + 2)
    env.reader.progress()
    results, errors = [], []
    entered = threading.Event()

    def call():
        try:
            entered.set()
            if operation == "start":
                result = env.reader.start(env.outcome(), 0, env.unpins.extend)
            elif operation == "finish":
                result = request.result()
            else:
                result = getattr(env.reader, operation)()
            results.append(result)
        except BaseException as exc:
            errors.append(exc)

    caller = threading.Thread(target=call)
    with env.reader.gate:
        caller.start()
        assert entered.wait(1)
        caller.join(0.1)
        returned_while_held = not caller.is_alive()
    caller.join(1)
    assert not caller.is_alive() and not errors
    assert returned_while_held
    if operation == "start":
        busy = results[0]
        assert busy.done() and busy.flight is None
        assert busy.reason == "gate_busy" and busy.result() is None
    elif operation == "finish":
        assert results == [None] and request.reason == "gate_busy"
        assert not request.flight.consumers
    else:
        assert results == [0 if operation == "capacity" else None]
    if not request.decided:
        request.result().release()
    env.reader.progress()
    assert env.reader.capacity() == 4


@pytest.mark.parametrize("operation", ["start", "finish", "capacity", "capture"])
def test_ttft_gate_uses_one_bounded_acquire(env, monkeypatch, operation):
    request = env.start()
    env.reader.progress()
    timeouts = []

    class BusyGate:
        def acquire(self, *, timeout):
            timeouts.append(timeout)
            return False

    with monkeypatch.context() as patch:
        patch.setattr(env.reader, "gate", BusyGate())
        if operation == "start":
            busy = env.reader.start(env.outcome(), 0, env.unpins.extend)
            assert busy.reason == "gate_busy"
        elif operation == "finish":
            assert request.result() is None
        else:
            getattr(env.reader, operation)()
    assert timeouts == [_GATE_WAIT_S]
    if operation == "start":
        assert busy.result() is None
    if not request.decided:
        request.result().release()
    env.reader.progress()


@pytest.mark.parametrize("operation", ["start", "finish"])
@pytest.mark.parametrize("remaining", [0.0005, 0, -1])
def test_gate_wait_respects_the_request_deadline(
    env, monkeypatch, operation, remaining
):
    request = env.start()
    env.reader.progress()
    timeouts = []

    class BusyGate:
        def acquire(self, *, timeout):
            timeouts.append(timeout)
            return False

    with monkeypatch.context() as patch:
        patch.setattr(time, "monotonic", lambda: 100)
        patch.setattr(env.reader, "gate", BusyGate())
        if operation == "start":
            busy = env.reader.start(
                env.outcome(), 0, env.unpins.extend, deadline=100 + remaining
            )
            assert busy.reason == "gate_busy"
        else:
            request.deadline = 100 + remaining
            assert env.reader.finish(request) is None
            assert request.reason == "gate_busy"
    assert timeouts == [pytest.approx(max(0, remaining))]
    if operation == "start":
        assert busy.result() is None
        request.result().release()
    env.reader.progress()


@pytest.mark.parametrize("remaining", [0.0005, 0, -1])
def test_batch_post_gate_wait_respects_the_earliest_deadline(
    env, monkeypatch, remaining
):
    batch = []
    requests = [env.start(env.outcome(offset), batch=batch) for offset in (0, PAGE)]
    requests[0].deadline = 100 + remaining
    requests[1].deadline = 100 + remaining + 1
    timeouts = []

    class BusyGate:
        def acquire(self, *, timeout):
            timeouts.append(timeout)
            return False

    with monkeypatch.context() as patch:
        patch.setattr(time, "monotonic", lambda: 100)
        patch.setattr(env.reader, "gate", BusyGate())
        env.reader.post_reads(batch)
    assert timeouts == [pytest.approx(max(0, remaining))]
    assert [r.reason for r in requests] == ["gate_busy", "gate_busy"]
    assert [r.result() for r in requests] == [None, None]
    env.reader.progress()


def test_idle_progress_does_not_acquire_the_gate(env, monkeypatch):
    class IdleGate:
        def acquire(self, *args, **kwargs):
            pytest.fail("idle progress acquired the gate")

    with monkeypatch.context() as patch:
        patch.setattr(env.reader, "gate", IdleGate())
        env.reader.progress()


def test_engine_release_does_not_wait_for_read_post_gate(env, client):
    pending = client.start_retrieve(["key"])
    env.reader.progress()
    reads = pending.result()
    assert reads[0] is not None
    entered, finished = threading.Event(), threading.Event()

    def release():
        entered.set()
        client.release_retrieved(reads)
        finished.set()

    env.reader.gate.acquire()
    releasing = threading.Thread(target=release)
    try:
        releasing.start()
        assert entered.wait(1)
        returned_without_gate = finished.wait(0.1)
    finally:
        env.reader.gate.release()
        releasing.join(1)
    assert not releasing.is_alive()
    assert returned_without_gate
    assert not reads[0]._released
    assert reads[0] in reads[0].flight.consumers
    assert bytes(reads[0].view) == b"a" * PAGE
    assert env.reader.capacity() == 3
    env.reader.progress()
    assert reads[0]._released
    assert env.reader.capacity() == 4


@pytest.mark.parametrize("ending", ["progress", "stop", "close"])
def test_deferred_release_keeps_joined_consumers_safe(env, ending):
    env.agent.stall = True
    first, second = env.start(), env.start()
    env.agent.finish_stalled()
    env.reader.progress()
    a, b = first.result(), second.result()
    assert a.flight is b.flight
    with env.reader.gate:
        a.release()
        a.release()
    assert not a._released and not b._released
    assert env.reader.capacity() == 3
    if ending == "stop":
        unpins = list(env.unpins)
        env.reader.stop()
        env.reader.progress()
        b.release()
        assert not a._released and not b._released
        assert a.flight.consumers == {a, b}
        assert bytes(a.view) == bytes(b.view) == b"a" * PAGE
        assert env.unpins == unpins
        assert not env.reader.close()
    elif ending == "close":
        assert not env.reader.close()
        assert env.reader in _KEPT_UNTIL_EXIT
        assert env.reader.transport._registrations
        assert not a._released and not b._released
        assert bytes(a.view) == bytes(b.view) == b"a" * PAGE
    else:
        env.reader.progress()
        assert a._released and not b._released
        assert bytes(b.view) == b"a" * PAGE
        assert env.reader.capacity() == 3
        b.release()
        env.reader.progress()
        assert env.reader.capacity() == 4
        assert len(set(env.reader._free)) == 4


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


@pytest.mark.parametrize("boundary", ["check", "view"])
@pytest.mark.parametrize("recover", [False, True])
def test_notice_between_hit_check_and_handoff_is_a_miss(
    env, monkeypatch, boundary, recover
):
    request = env.start(deadline=time.monotonic() + 2)
    env.reader.progress()
    owner, method = (
        (env.reader, "_valid") if boundary == "check" else (env.reader.staging, "view")
    )
    original = getattr(owner, method)
    checked, resume = threading.Event(), threading.Event()
    results = []

    def pause_after_check(*args):
        result = original(*args)
        checked.set()
        assert resume.wait(1)
        return result

    monkeypatch.setattr(owner, method, pause_after_check)
    finishing = threading.Thread(target=lambda: results.append(request.result()))
    finishing.start()
    try:
        assert checked.wait(1)
        assert not request.decided and not request.flight.consumers
        env.reader.notify_peers([dataclasses.replace(env.peer, generation_known=False)])
        if recover:
            env.reader.notify_peers([env.peer])
    finally:
        resume.set()
        finishing.join(1)
    assert not finishing.is_alive()
    read = results[0]
    try:
        assert read is None
        assert request.reason == "generation"
        assert not request.flight.consumers
        assert env.reader.capacity() == 4
        assert env.unpins == [request.outcome.pin_id]
    finally:
        if read is not None:
            read.release()


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


def test_client_keeps_credit_when_staging_is_temporarily_full(env, client):
    from maru_handler.client.maru_client import CreditUnavailableError

    assert client._refill_once()
    with env.reader.gate:
        slots = list(env.reader._free)
        env.reader._free.clear()
    try:
        with pytest.raises(CreditUnavailableError, match="no remote staging slot"):
            client.alloc(PAGE)
        assert client.credits.unused_bytes == PAGE
    finally:
        with env.reader.gate:
            env.reader._free.extend(slots)
    handle = client.alloc(PAGE)
    client.free(handle)
    assert env.reader.capacity() == env.reader.slot_count


def test_client_discards_remaining_credit_of_a_retired_domain(env, client, writer):
    from maru_handler.client.maru_client import CreditUnavailableError

    assert client._refill_once()
    _, _, agent, _ = writer
    other = env.outcome(PAGE).location
    assert env.reader.bind_credits([other], agent, "gpu", "gpu-run")
    assert client.credits.put([other], client.node_epoch)
    env.reader._retired_domains.add(agent.domain)
    with pytest.raises(CreditUnavailableError, match="write domain ended"):
        client.alloc(PAGE)
    assert client.credits.unused_bytes == 0
    assert client._refill_wake.is_set()
    assert env.reader.capacity() == env.reader.slot_count
    assert len(client._storage.requests) == 1


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
