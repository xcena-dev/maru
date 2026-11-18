# SPDX-License-Identifier: Apache-2.0
"""PeerOps on a running memory node: credit to a peer run, commit of its
DONE writes, quarantine of every page a transfer may still touch, release by
the same run's evidence only, the quarantine caps and the credit TTL."""

import threading
import time

import pytest

from maru_common.remote_protocol import (
    CancelRequest,
    DropRequest,
    RemoteWriteDoneRequest,
    ReserveRequest,
    RoomLimit,
    StageRequest,
    TransferEnd,
    UnstageRequest,
    Written,
    peer_origin,
)
from maru_common.storage_types import (
    MAX_BATCH,
    ItemResult,
    Key,
    OpRef,
    Status,
)
from maru_storage_service.pool_manager import PageState
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import remove_tree, short_tmpdir, wait_until
from tests.unit.remote_fakes import reset_fake_agents
from tests.unit.test_ms_memory_role import PAGE, Parts, memory_node

A = peer_origin("gpu-a", "ea")
A2 = peer_origin("gpu-a", "ea2")  # a later run of the same node
B = peer_origin("gpu-b", "eb")


@pytest.fixture(autouse=True)
def _fresh_agents():
    reset_fake_agents()
    yield
    reset_fake_agents()


@pytest.fixture
def fake():
    server = FakeMaruServer()
    yield server
    server.close()


@pytest.fixture
def node(fake):
    sockdir = short_tmpdir()
    service = memory_node(fake, sockdir, Parts([]))
    yield service
    service.stop()
    remove_tree(sockdir)


class Seqs:
    def __init__(self):
        self.n = 0

    def op(self, origin: str, ack: int = 0) -> OpRef:
        self.n += 1
        return OpRef(origin, self.n, b"", ack)


def call(fn, *args, timeout: float = 10.0):
    """Run an op and wait for its one answer."""
    got, event = [], threading.Event()

    def done(response):
        got.append(response)
        event.set()

    fn(*args, done)
    assert event.wait(timeout), "no answer"
    assert len(got) == 1
    return got[0]


def reserve(ops, origin, pages, seqs, ack=0):
    return call(ops.reserve, origin, ReserveRequest(seqs.op(origin, ack), pages))


def written(credit, key, end=TransferEnd.DONE, logical=PAGE):
    return Written(key, credit, logical, end)


def write_done(ops, origin, items, seqs):
    return call(ops.write_done, origin, RemoteWriteDoneRequest(seqs.op(origin), items))


def state(node, location):
    ref = node.pools.find(location)
    return None if ref is None else node.pools.page(ref).state


def check(node):
    node.pools.check()


def test_reserve_credits_the_peer_run(node):
    ops, seqs = node.peer_ops, Seqs()
    room0, limit = ops.room(A)
    assert room0 > 0 and limit is None
    r = reserve(ops, A, 2, seqs)
    assert r.status == Status.OK and len(r.credits) == 2 and r.limit is None
    assert r.room == room0 - 2 * PAGE
    assert all(state(node, c) == PageState.CREDITED for c in r.credits)
    assert node.pools.held_bytes(A) == 2 * PAGE
    check(node)


@pytest.mark.parametrize("pages", [0, MAX_BATCH + 1])
def test_reserve_bounds(node, pages):
    r = reserve(node.peer_ops, A, pages, Seqs())
    assert r.status == Status.REJECTED


def test_a_done_write_is_committed_without_a_finalizer(node, fake):
    ops, seqs = node.peer_ops, Seqs()
    [credit] = reserve(ops, A, 1, seqs).credits
    key = Key("ns", "k1")
    r = write_done(ops, A, [written(credit, key, logical=PAGE - 7)], seqs)
    assert r.status == Status.OK and [o.result for o in r.items] == [ItemResult.ADDED]
    assert state(node, credit) == PageState.READY
    [loc] = fake.locations(key)
    assert (loc.pool_id, loc.offset, loc.length) == (
        credit.pool_id,
        credit.offset,
        PAGE - 7,
    )
    assert node.pools.held_bytes(A) == 0
    check(node)


def test_a_write_of_another_run_or_page_is_rejected(node):
    ops, seqs = node.peer_ops, Seqs()
    [credit] = reserve(ops, A, 1, seqs).credits
    r = write_done(ops, B, [written(credit, Key("ns", "k"))], seqs)
    assert [o.result for o in r.items] == [ItemResult.REJECTED]
    assert state(node, credit) == PageState.CREDITED
    check(node)


def test_an_unknown_end_quarantines_until_the_same_run_says_done(node, fake):
    ops, seqs = node.peer_ops, Seqs()
    [credit] = reserve(ops, A, 1, seqs).credits
    key = Key("ns", "late")
    r = write_done(ops, A, [written(credit, key, TransferEnd.UNKNOWN)], seqs)
    assert [o.result for o in r.items] == [ItemResult.OK]
    assert state(node, credit) == PageState.QUARANTINED
    assert ops.quarantined["gpu-a"] == PAGE
    # Another run of the node is no evidence
    r = write_done(ops, A2, [written(credit, key)], seqs)
    assert [o.result for o in r.items] == [ItemResult.REJECTED]
    assert state(node, credit) == PageState.QUARANTINED
    # The late DONE of the same run, as a new op
    r = write_done(ops, A, [written(credit, key)], seqs)
    assert [o.result for o in r.items] == [ItemResult.ADDED]
    assert state(node, credit) == PageState.READY and fake.locations(key)
    assert ops.quarantined["gpu-a"] == 0
    check(node)


def test_cancel_by_locations_frees_credits_and_quarantined_pages(node):
    ops, seqs = node.peer_ops, Seqs()
    c1, c2 = reserve(ops, A, 2, seqs).credits
    write_done(ops, A, [written(c2, Key("ns", "x"), TransferEnd.UNKNOWN)], seqs)
    r = call(ops.cancel, A, CancelRequest(seqs.op(A), locations=[c1, c2]))
    assert r.status == Status.OK and r.freed == 2
    assert state(node, c1) is None and state(node, c2) is None  # FREE
    assert ops.quarantined["gpu-a"] == 0 and node.pools.held_bytes(A) == 0
    check(node)


def test_cancel_by_reserve_seq_frees_what_is_left_of_it(node):
    ops, seqs = node.peer_ops, Seqs()
    r = reserve(ops, A, 2, seqs)  # the peer's credit cap
    reserve_seq = seqs.n
    c1, c2 = r.credits
    write_done(ops, A, [written(c1, Key("ns", "w"))], seqs)
    r = call(ops.cancel, A, CancelRequest(seqs.op(A), reserve_seq=reserve_seq))
    assert r.freed == 1
    assert state(node, c1) == PageState.READY and state(node, c2) is None
    check(node)


def test_a_cancel_before_its_reserve_leaves_a_tombstone(node):
    ops = node.peer_ops
    r = call(ops.cancel, A, CancelRequest(OpRef(A, 2, b"", 0), reserve_seq=5))
    assert r.freed == 0
    late = call(ops.reserve, A, ReserveRequest(OpRef(A, 5, b"", 0), 2))
    assert late.status == Status.OK and late.credits == []
    assert node.pools.held_bytes(A) == 0
    # Once the peer acked past it, the seq is forgotten
    call(ops.reserve, A, ReserveRequest(OpRef(A, 6, b"", 5), 1))
    assert ops._tombstones[A] == set()
    check(node)


def test_reserves_are_forgotten_once_acked(node):
    ops, seqs = node.peer_ops, Seqs()
    reserve(ops, A, 1, seqs)
    first = seqs.n
    reserve(ops, A, 1, seqs, ack=first)
    assert first not in ops._reserves[A]


def test_a_lost_peer_run_is_quarantined_and_its_evidence_releases(node):
    ops, seqs = node.peer_ops, Seqs()
    c1, c2 = reserve(ops, A, 2, seqs).credits
    ops.peer_lost(A)
    assert {state(node, c) for c in (c1, c2)} == {PageState.QUARANTINED}
    assert ops.quarantined["gpu-a"] == 2 * PAGE
    # It reconnects: DONE for one, never posted for the other
    r = write_done(ops, A, [written(c1, Key("ns", "back"))], seqs)
    assert [o.result for o in r.items] == [ItemResult.ADDED]
    r = call(ops.cancel, A, CancelRequest(seqs.op(A), locations=[c2]))
    assert r.freed == 1
    assert ops.quarantined["gpu-a"] == 0
    check(node)


def test_a_retired_peer_run_stays_quarantined(node):
    ops, seqs = node.peer_ops, Seqs()
    [credit] = reserve(ops, A, 1, seqs).credits
    ops.peer_retired(A)
    assert state(node, credit) == PageState.QUARANTINED
    r = write_done(ops, A, [written(credit, Key("ns", "k"))], seqs)
    assert [o.result for o in r.items] == [ItemResult.REJECTED]
    assert call(
        ops.cancel, A, CancelRequest(seqs.op(A), locations=[credit])
    ).status == (Status.REJECTED)
    assert reserve(ops, A, 1, seqs).status == Status.REJECTED
    assert state(node, credit) == PageState.QUARANTINED
    assert ops.quarantined["gpu-a"] == PAGE
    check(node)


def test_a_peer_at_its_quarantine_cap_gets_no_room(node):
    ops, seqs = node.peer_ops, Seqs()
    ops.peer_quarantine_max, ops.quarantine_max = PAGE, 4 * PAGE
    [credit] = reserve(ops, A, 1, seqs).credits
    ops.peer_lost(A)
    assert ops.room(A) == (0, RoomLimit.QUARANTINE)
    r = reserve(ops, A2, 1, seqs)  # every run of the node
    assert r.credits == [] and (r.room, r.limit) == (0, RoomLimit.QUARANTINE)
    assert reserve(ops, B, 1, seqs).credits  # another node goes on
    # The node cap holds every peer
    ops.quarantine_max = PAGE
    assert ops.room(B) == (0, RoomLimit.QUARANTINE)
    check(node)


def test_credits_past_their_ttl_are_quarantined(node):
    ops, seqs = node.peer_ops, Seqs()
    now = [1000.0]
    ops._clock = lambda: now[0]
    [credit] = reserve(ops, A, 1, seqs).credits
    assert ops.sweep() == 0
    now[0] += ops.reserve_ttl_s
    assert ops.sweep() == 1
    assert state(node, credit) == PageState.QUARANTINED
    assert ops.sweep() == 0
    # A late WRITE_DONE of the same run still commits it
    r = write_done(ops, A, [written(credit, Key("ns", "slow"))], seqs)
    assert [o.result for o in r.items] == [ItemResult.ADDED]
    check(node)


def test_drop_retires_the_exact_ready_location(node, fake):
    ops, seqs = node.peer_ops, Seqs()
    [credit] = reserve(ops, A, 1, seqs).credits
    key = Key("ns", "gone")
    write_done(ops, A, [written(credit, key)], seqs)
    [loc] = fake.locations(key)
    other = Key("ns", "other")
    r = call(ops.drop, A, DropRequest(seqs.op(A), other, loc))
    assert r.outcome.result == ItemResult.REJECTED
    r = call(ops.drop, A, DropRequest(seqs.op(A), key, loc))
    assert r.status == Status.OK and r.outcome.result == ItemResult.RETIRED
    assert fake.locations(key) == []
    wait_until(lambda: state(node, credit) is None, what="reclaimed")
    check(node)


def test_stage_misses_a_credit_and_unstage_is_served(node):
    ops, seqs = node.peer_ops, Seqs()
    [credit] = reserve(ops, A, 1, seqs).credits
    r = call(
        ops.stage,
        A,
        StageRequest(seqs.op(A), 1, Key("ns", "k"), credit, 1000),
        1e12,
    )
    assert r.status == Status.OK and r.staging is None
    r = call(ops.unstage, A, UnstageRequest(seqs.op(A), 1, TransferEnd.DONE))
    assert r.status == Status.OK
    assert state(node, credit) == PageState.CREDITED
    check(node)


@pytest.mark.parametrize("end", [TransferEnd.DONE, TransferEnd.NOT_POSTED])
def test_stage_loads_a_ready_ssd_page_and_unstage_frees_it(fake, end, ssd_dir):
    root = short_tmpdir()
    service = memory_node(fake, root, Parts([]), ssd_path=ssd_dir)
    try:
        key = Key("ns", "stage")
        ops, seqs = service.peer_ops, Seqs()
        [credit] = reserve(ops, A, 1, seqs).credits
        assert (
            write_done(ops, A, [written(credit, key)], seqs).items[0].result
            == ItemResult.ADDED
        )
        free = service.pools.bytes_in(PageState.FREE, credit.pool_id)
        service.manager.want_room("dram", free + PAGE)
        wait_until(
            lambda: (
                len(fake.locations(key)) == 1
                and fake.locations(key)[0].pool_id == service.tier("ssd").pool.pool_id
            ),
            what="SSD source",
        )
        source = fake.locations(key)[0]
        answer = call(
            ops.stage,
            A,
            StageRequest(seqs.op(A), 1, key, source, 5000),
            time.monotonic() + 5,
        )
        assert answer.status == Status.OK and answer.staging is not None
        ref = service.pools.find(answer.staging)
        assert service.pools.page(ref).state == PageState.TRANSFER
        assert ops._staging_bytes[A] == PAGE
        assert (
            call(ops.unstage, A, UnstageRequest(seqs.op(A), 1, end)).status == Status.OK
        )
        assert service.pools.page(ref).state == PageState.FREE
        assert ops._staging_bytes[A] == 0
        check(service)
    finally:
        service.stop()
        remove_tree(root)


def test_a_refused_op_keeps_the_credits_ttl(node):
    ops, seqs = node.peer_ops, Seqs()
    now = [1000.0]
    ops._clock = lambda: now[0]
    [credit] = reserve(ops, A, 1, seqs).credits
    key = Key("ns", "k")
    # Another run's DONE, CANCEL and UNKNOWN, and its own bad DONE, all refused
    write_done(ops, B, [written(credit, key)], seqs)
    call(ops.cancel, B, CancelRequest(seqs.op(B), locations=[credit]))
    write_done(ops, B, [written(credit, key, TransferEnd.UNKNOWN)], seqs)
    r = write_done(ops, A, [written(credit, key, logical=0)], seqs)
    assert [o.result for o in r.items] == [ItemResult.REJECTED]
    assert state(node, credit) == PageState.CREDITED
    now[0] += ops.reserve_ttl_s
    assert ops.sweep() == 1
    assert state(node, credit) == PageState.QUARANTINED
    check(node)


def test_staging_and_credits_together_stay_within_the_credit_cap(fake, ssd_dir):
    root = short_tmpdir()
    service = memory_node(fake, root, Parts([]), ssd_path=ssd_dir)
    try:
        key = Key("ns", "cap")
        ops, seqs = service.peer_ops, Seqs()
        cap = service.pools.budgets.credit_max_bytes
        [credit] = reserve(ops, A, 1, seqs).credits
        write_done(ops, A, [written(credit, key)], seqs)
        free = service.pools.bytes_in(PageState.FREE, credit.pool_id)
        service.manager.want_room("dram", free + PAGE)
        ssd = service.tier("ssd").pool.pool_id
        wait_until(
            lambda: [loc.pool_id for loc in fake.locations(key)] == [ssd],
            what="SSD source",
        )
        source = fake.locations(key)[0]
        answer = call(
            ops.stage,
            A,
            StageRequest(seqs.op(A), 1, key, source, 5000),
            time.monotonic() + 5,
        )
        assert answer.staging is not None
        # Staging, then credits up to the cap, then none
        got = reserve(ops, A, 2, seqs).credits
        assert len(got) == cap // PAGE - 1
        assert reserve(ops, A, 1, seqs).credits == []
        assert service.pools.held_bytes(A) + ops._staging_bytes[A] == cap
        call(ops.unstage, A, UnstageRequest(seqs.op(A), 1, TransferEnd.DONE))
        assert len(reserve(ops, A, 1, seqs).credits) == 1
        check(service)
    finally:
        service.stop()
        remove_tree(root)


def test_a_cap_that_is_not_whole_pages_is_kept_in_whole_pages():
    from types import SimpleNamespace

    from maru_common.ms_rpc import PLACEHOLDER_OP
    from maru_common.storage_types import PoolInfo, PoolKind
    from maru_storage_service.config import parse_node_config
    from maru_storage_service.peer_ops import PeerOps
    from maru_storage_service.pool_manager import PoolManager

    page = 4096
    config = parse_node_config(
        {
            "node_id": "mem",
            "server": "tcp://127.0.0.1:1",
            "page_size": page,
            "tiers": [{"store": "dram", "size": 16 * page, "hugepage": False}],
            "budgets": {
                "credit_max_bytes": 5 * page // 2,
                "credit_total_max_bytes": 8 * page,
                "credit_refill_low": page,
                "credit_refill_high": 2 * page,
                "commit_pending_max_bytes": 3 * page,
                "transfer_reserve_bytes": page,
            },
        }
    )
    pools = PoolManager(config.budgets, page)
    pools.add_pool(PoolInfo("dram:mem:e", PoolKind.DRAM, "mem", 1, 16 * page, page))
    pools.set_epoch_seq(1)
    manager = SimpleNamespace(
        pools=pools, credit_tiers=["dram"], want_room=lambda *args: None
    )
    ops = PeerOps(
        manager,
        reserve_ttl_s=60,
        peer_quarantine_max=8 * page,
        quarantine_max=16 * page,
    )
    # A staging page charged to the peer: 1.5 pages of the cap are left
    assert len(pools.reserve_transfer("dram:mem:e", 1)) == 1
    ops._staging_bytes[A] = page
    assert len(ops._reserve(A, ReserveRequest(PLACEHOLDER_OP, 2))) == 1
    assert (
        pools.held_bytes(A) + ops._staging_bytes[A] <= config.budgets.credit_max_bytes
    )
