# SPDX-License-Identifier: Apache-2.0
"""Credit ownership, COMMIT boundaries and registration revocation."""

import dataclasses
from concurrent.futures import ThreadPoolExecutor

import pytest

from maru_common.remote_protocol import (
    CancelRequest,
    FenceDomainRequest,
    FenceResult,
    PingRequest,
    RemoteMessageType,
    RemoteWriteDoneRequest,
    ReserveRequest,
    RoomLimit,
    TransferEnd,
    Written,
    peer_origin,
)
from maru_common.storage_types import MAX_BATCH, ItemResult, Key, Status
from maru_remote.transport import TransferState
from maru_storage_service.pool_manager import PageRef, PageState
from tests.unit.ms_node_helpers import PAGE, wait_until
from tests.unit.remote_transport_fakes import FakeTransport, mapped
from tests.unit.test_ms_memory_role import DOMAIN, A, call
from tests.unit.test_ms_memory_role import memory as memory


def state(memory, location):
    return memory.node.pools.page(
        PageRef(location.pool_id, location.offset // PAGE, location.generation)
    ).state


def fence(memory, domain=DOMAIN):
    return call(
        memory.ops.fence_domain,
        peer_origin(domain.node_id, domain.node_epoch),
        FenceDomainRequest(memory.op(), domain),
    )


def fail_fence(memory, monkeypatch, domain=DOMAIN):
    transport = memory.parts._writers[domain].transport
    monkeypatch.setattr(
        transport,
        "deregister",
        lambda _: (_ for _ in ()).throw(RuntimeError("NIC busy")),
    )


def test_reserve_tracks_credit_per_origin_across_instance_domains(memory):
    a = memory.reserve(3)
    other = dataclasses.replace(DOMAIN, instance="instance-b")
    b = memory.reserve(8, other)
    assert len(a.credits) == 3 and len(b.credits) == 5
    assert memory.node.pools.held_bytes(A) == 8 * PAGE
    assert memory.reserve(1, dataclasses.replace(DOMAIN, number=2)).credits == []
    assert a.write_agent.agent_name != b.write_agent.agent_name
    assert memory.reserve(1, dataclasses.replace(DOMAIN, node_id="gpu-b")).credits == []
    memory.node.pools.check()


@pytest.mark.parametrize("pages", [0, -1, MAX_BATCH + 1])
def test_reserve_bounds(memory, pages):
    assert memory.reserve(pages).status == Status.REJECTED


@pytest.mark.parametrize(
    "domain",
    [
        None,
        dataclasses.replace(DOMAIN, node_epoch="wrong"),
        dataclasses.replace(DOMAIN, instance=""),
    ],
)
def test_domain_is_required_and_bound_to_the_origin(memory, domain):
    response = call(
        memory.ops.reserve, A, ReserveRequest(memory.op(), 1, domain=domain)
    )
    assert response.status == Status.REJECTED


def test_done_commits_and_duplicate_frees_only_the_offer(memory):
    first = memory.reserve().credits
    assert memory.write(first).items[0].result == ItemResult.ADDED
    second = memory.reserve().credits
    outcome = memory.write(second).items[0]
    assert outcome.result == ItemResult.DUPLICATE
    assert outcome.location.allocation_id == first[0].allocation_id
    assert state(memory, first[0]) == PageState.READY
    assert state(memory, second[0]) == PageState.FREE


def test_wrong_domain_and_wrong_allocation_do_not_release_credit(memory):
    (credit,) = memory.reserve().credits
    other = dataclasses.replace(DOMAIN, number=2)
    memory.reserve(domain=other)
    assert memory.write([credit], other).items[0].result == ItemResult.REJECTED
    invalid = dataclasses.replace(credit, allocation_id="wrong")
    assert memory.write([invalid]).items[0].result == ItemResult.REJECTED
    assert state(memory, credit) == PageState.CREDITED


def test_write_done_answers_only_after_catalog_commit(memory):
    (credit,) = memory.reserve().credits
    memory.fake.pause()
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(memory.write, [credit])
        try:
            wait_until(lambda: state(memory, credit) == PageState.PENDING)
            assert not future.done()
            assert not memory.fake.by_loc
        finally:
            memory.fake.resume()
        assert future.result(5).items[0].result == ItemResult.ADDED
    assert state(memory, credit) == PageState.READY


def test_cancel_locations_and_reserve_sequence_return_only_never_posted(memory):
    op = memory.op()
    credits = call(memory.ops.reserve, A, ReserveRequest(op, 3, domain=DOMAIN)).credits
    assert memory.write(credits[:1]).items[0].result == ItemResult.ADDED
    req = CancelRequest(
        memory.op(), reserve_seq=op.seq, locations=[credits[1]], domain=DOMAIN
    )
    assert call(memory.ops.cancel, A, req).freed == 2
    assert (
        call(memory.ops.cancel, A, dataclasses.replace(req, op=memory.op())).freed == 0
    )
    assert state(memory, credits[0]) == PageState.READY


def test_cancel_before_reserve_tombstones_and_ack_forgets(memory):
    req = CancelRequest(memory.op(), reserve_seq=30, domain=DOMAIN)
    assert call(memory.ops.cancel, A, req).freed == 0
    op = dataclasses.replace(memory.op(), seq=30)
    assert (
        call(memory.ops.reserve, A, ReserveRequest(op, 1, domain=DOMAIN)).credits == []
    )
    op = dataclasses.replace(memory.op(), seq=31, ack=30)
    assert call(memory.ops.reserve, A, ReserveRequest(op, 1, domain=DOMAIN)).credits
    assert not memory.ops._tombstones


def test_fence_precedes_reuse_and_rejects_old_domain(memory, monkeypatch):
    (credit,) = memory.reserve().credits
    original = memory.parts.fence
    observed = []

    def checked(domain):
        observed.append(state(memory, credit))
        assert memory.node.pools.bytes_in(PageState.FREE) == 15 * PAGE
        original(domain)

    monkeypatch.setattr(memory.parts, "fence", checked)
    assert fence(memory).result == FenceResult.SUCCESS
    assert observed == [PageState.QUARANTINED]
    assert state(memory, credit) == PageState.FREE
    assert memory.write([credit]).status == Status.REJECTED
    assert memory.reserve().status == Status.REJECTED
    assert fence(memory).result == FenceResult.SUCCESS


def test_a_fence_of_one_instance_preserves_other_writers_and_shared_reads(memory):
    a = memory.reserve()
    other = dataclasses.replace(DOMAIN, instance="other")
    b = memory.reserve(domain=other)
    shared = memory.parts.exporter.exports()
    assert fence(memory).result == FenceResult.SUCCESS
    assert memory.parts.exporter.exports() == shared
    assert memory.write(b.credits, other).items[0].result == ItemResult.ADDED
    assert state(memory, a.credits[0]) == PageState.FREE


def test_fence_holds_commit_pending_until_the_result(memory):
    credits = memory.reserve(2).credits
    memory.fake.pause()
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(memory.write, credits[:1])
        try:
            wait_until(lambda: state(memory, credits[0]) == PageState.PENDING)
            assert fence(memory).result == FenceResult.SUCCESS
            assert state(memory, credits[0]) == PageState.PENDING
            assert state(memory, credits[1]) == PageState.FREE
            assert not future.done()
        finally:
            memory.fake.resume()
        assert future.result(5).items[0].result == ItemResult.ADDED
    assert state(memory, credits[0]) == PageState.READY


def test_commit_response_loss_during_fence_never_frees_published_page(memory):
    from maru_common.protocol import MessageType

    (credit,) = memory.reserve().credits
    memory.fake.drop_responses(MessageType.COMMIT, 10)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(memory.write, [credit])
        try:
            wait_until(lambda: bool(memory.fake.by_loc))
            assert state(memory, credit) == PageState.PENDING
            assert fence(memory).result == FenceResult.SUCCESS
            assert state(memory, credit) == PageState.PENDING and not future.done()
        finally:
            memory.fake._drop_responses[MessageType.COMMIT] = 0
        assert future.result(5).items[0].result == ItemResult.ADDED


@pytest.mark.parametrize("trigger", ["explicit", "ttl"])
def test_fence_failure_is_q2_and_time_or_late_done_never_releases_it(
    memory, monkeypatch, trigger
):
    (credit,) = memory.reserve().credits
    with monkeypatch.context() as patch:
        fail_fence(memory, patch)
        if trigger == "ttl":
            memory.ops._clock = lambda: 10**20
            assert memory.ops.sweep() == 1
        else:
            assert fence(memory).result == FenceResult.FAILURE
        assert state(memory, credit) == PageState.QUARANTINED
        memory.ops._clock = lambda: 10**20
        assert memory.ops.sweep() == 0
        assert memory.write([credit]).status == Status.REJECTED
        assert (
            call(
                memory.ops.cancel,
                A,
                CancelRequest(memory.op(), locations=[credit], domain=DOMAIN),
            ).freed
            == 0
        )
        assert memory.ops.quarantined[DOMAIN.node_id] == PAGE
    assert fence(memory).result == FenceResult.SUCCESS
    assert state(memory, credit) == PageState.FREE
    assert memory.ops.quarantined[DOMAIN.node_id] == 0


def test_quarantine_cap_is_by_gpu_node_across_runs(memory, monkeypatch):
    credits = memory.reserve(2).credits
    with monkeypatch.context() as patch:
        fail_fence(memory, patch)
        assert (
            memory.write(credits, end=TransferEnd.UNKNOWN).items[0].result
            == ItemResult.OK
        )
        replacement = dataclasses.replace(DOMAIN, node_epoch="run-b")
        denied = memory.reserve(domain=replacement)
        assert denied.credits == [] and denied.limit == RoomLimit.QUARANTINE
        assert not memory.reserve(
            domain=dataclasses.replace(DOMAIN, node_id="gpu-b")
        ).limit
        assert memory.ops.quarantined[DOMAIN.node_id] == 2 * PAGE
    fence(memory)


def test_unknown_fences_the_entire_domain_but_preserves_accepted_done(memory):
    credits = memory.reserve(2).credits
    request = RemoteWriteDoneRequest(
        memory.op(),
        [
            Written(Key("ns", "known"), credits[0], PAGE, TransferEnd.DONE),
            Written(Key("ns", "unknown"), credits[1], PAGE, TransferEnd.UNKNOWN),
        ],
        DOMAIN,
    )
    response = call(memory.ops.write_done, A, request)
    assert [i.result for i in response.items] == [ItemResult.ADDED, ItemResult.OK]
    assert state(memory, credits[0]) == PageState.READY
    assert state(memory, credits[1]) == PageState.FREE
    assert memory.ops.quarantined[DOMAIN.node_id] == 0


@pytest.mark.parametrize("trigger", ["lost", "retired", "ttl"])
def test_connection_loss_retirement_and_silence_fence_before_reuse(
    memory, monkeypatch, trigger
):
    now = 0.0
    monkeypatch.setattr(memory.ops, "_clock", lambda: now)
    (credit,) = memory.reserve().credits
    original = memory.parts.fence
    observed = []

    def checked(domain):
        observed.append(state(memory, credit))
        original(domain)

    monkeypatch.setattr(memory.parts, "fence", checked)
    if trigger == "ttl":
        now = memory.ops.reserve_ttl_s - 1
        assert memory.ops.sweep() == 0
        now += 1
        assert memory.ops.sweep() == 1
    else:
        getattr(memory.ops, f"peer_{trigger}")(A)
    assert observed == [PageState.QUARANTINED]
    assert state(memory, credit) == PageState.FREE
    assert memory.reserve().status == Status.REJECTED


def test_pings_preserve_idle_domains_and_later_write_succeeds(memory, monkeypatch):
    now = 0.0
    monkeypatch.setattr(memory.ops, "_clock", lambda: now)
    conn, hello = memory.connect()
    assert hello.status == Status.OK
    first = memory.send(
        conn,
        RemoteMessageType.RESERVE,
        ReserveRequest(memory.op(), 1, domain=DOMAIN),
    )
    other_domain = dataclasses.replace(DOMAIN, instance="other")
    other = memory.send(
        conn,
        RemoteMessageType.RESERVE,
        ReserveRequest(memory.op(), 1, domain=other_domain),
    )
    (credit,) = first.credits
    (idle,) = other.credits
    for _ in range(8):
        now += memory.ops.reserve_ttl_s / 2
        assert conn.call(RemoteMessageType.PING, PingRequest(), 3).status == Status.OK
        assert memory.ops.sweep() == 0
        assert state(memory, credit) == state(memory, idle) == PageState.CREDITED

    worker = FakeTransport("idle-writer")
    data, addr = mapped(PAGE, 7)
    try:
        worker.register(addr, PAGE)
        peer = worker.add_peer(first.write_agent.agent_metadata)
        remote = first.write_agent.pools[0].base + credit.offset
        assert (
            worker.post("WRITE", peer, [(addr, remote, PAGE)]).poll_state()
            == TransferState.DONE
        )
        answer = memory.send(
            conn,
            RemoteMessageType.WRITE_DONE,
            RemoteWriteDoneRequest(
                memory.op(),
                [Written(Key("ns", "after-idle"), credit, PAGE, TransferEnd.DONE)],
                DOMAIN,
            ),
        )
        assert answer.status == Status.OK
        assert answer.items[0].result == ItemResult.ADDED
        assert state(memory, credit) == PageState.READY
        pool = memory.parts._maps[credit.pool_id]
        assert pool[credit.offset : credit.offset + PAGE] == bytes([7]) * PAGE
    finally:
        worker.close()
        data.close()

    now += memory.ops.reserve_ttl_s - 1
    assert memory.ops.sweep() == 0
    now += 1
    assert memory.ops.sweep() == 1
    assert state(memory, idle) == PageState.FREE
    assert state(memory, credit) == PageState.READY


def test_requests_from_another_peer_do_not_extend_a_silent_peers_ttl(
    memory, monkeypatch
):
    now = 0.0
    monkeypatch.setattr(memory.ops, "_clock", lambda: now)
    (silent,) = memory.reserve().credits
    domain = dataclasses.replace(DOMAIN, node_id="gpu-b")
    conn, hello = memory.connect(node_id=domain.node_id)
    assert hello.status == Status.OK
    reserved = memory.send(
        conn,
        RemoteMessageType.RESERVE,
        ReserveRequest(
            memory.op(peer_origin(domain.node_id, domain.node_epoch)), 1, domain=domain
        ),
    )
    assert reserved.status == Status.OK
    (active,) = reserved.credits
    now = memory.ops.reserve_ttl_s
    assert conn.call(RemoteMessageType.PING, PingRequest(), 3).status == Status.OK
    assert memory.ops.sweep() == 1
    assert state(memory, silent) == PageState.FREE
    assert state(memory, active) == PageState.CREDITED


def test_late_packets_cannot_write_a_reused_page_after_fence(memory):
    first = memory.reserve()
    worker = FakeTransport("late-writer")
    data, addr = mapped(PAGE, 9)
    worker.register(addr, PAGE)
    peer = worker.add_peer(first.write_agent.agent_metadata)
    remote = first.write_agent.pools[0].base + first.credits[0].offset
    worker.stall()
    pending = worker.post("WRITE", peer, [(addr, remote, PAGE)])
    worker.fail_stalled(late=True)
    assert pending.poll_state() == TransferState.ERR
    assert fence(memory).result == FenceResult.SUCCESS
    pool = memory.parts._maps[first.credits[0].pool_id]
    offset = first.credits[0].offset
    pool[offset : offset + PAGE] = bytes([3]) * PAGE
    assert worker.deliver_late() == 0
    assert pool[offset : offset + PAGE] == bytes([3]) * PAGE
    worker.close()
    data.close()


@pytest.mark.parametrize("unknown_first", [False, True])
def test_conflicting_done_and_unknown_for_the_same_credit_never_publish(
    memory, unknown_first
):
    (credit,) = memory.reserve().credits
    done = Written(Key("ns", "conflict"), credit, PAGE, TransferEnd.DONE)
    unknown = dataclasses.replace(done, end=TransferEnd.UNKNOWN)
    items = [unknown, done] if unknown_first else [done, unknown]
    answer = call(
        memory.ops.write_done, A, RemoteWriteDoneRequest(memory.op(), items, DOMAIN)
    )
    assert sorted(item.result for item in answer.items) == [
        ItemResult.OK,
        ItemResult.REJECTED,
    ]
    assert not memory.fake.keys()
    assert state(memory, credit) == PageState.FREE


def test_write_done_cannot_cross_a_fence_in_progress(memory, monkeypatch):
    import threading

    (credit,) = memory.reserve().credits
    entered, release = threading.Event(), threading.Event()
    original = memory.parts.fence

    def blocked(domain):
        entered.set()
        assert release.wait(5)
        original(domain)

    monkeypatch.setattr(memory.parts, "fence", blocked)
    with ThreadPoolExecutor(max_workers=2) as executor:
        fencing = executor.submit(fence, memory)
        try:
            assert entered.wait(5)
            assert state(memory, credit) == PageState.QUARANTINED
            writing = executor.submit(memory.write, [credit])
            assert not writing.done()
        finally:
            release.set()
        assert fencing.result(5).result == FenceResult.SUCCESS
        assert writing.result(5).status == Status.REJECTED
    assert not memory.fake.keys()
