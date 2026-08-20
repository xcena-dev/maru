# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""DeadlineStagePolicy: EDF admission, expiry, replace, budget contracts."""

from maru_vllm.staging_prefetch import DeadlineStagePolicy

KEY_BYTES = 16 * 1024**2


class FakeClock:
    """Deterministic monotonic clock for deadline arithmetic."""

    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def make_policy(
    clock: FakeClock,
    *,
    max_requests: int = 1,
    max_bytes: int = 0,
    deadline_s: float = 0.5,
    grace_s: float = 0.2,
) -> DeadlineStagePolicy:
    return DeadlineStagePolicy(
        max_requests=max_requests,
        max_bytes=max_bytes,
        estimated_bytes_per_key=KEY_BYTES,
        deadline_s=deadline_s,
        grace_s=grace_s,
        clock=clock,
    )


def test_earliest_deadline_admitted_first() -> None:
    """Enqueue order loses to deadline order."""
    clock = FakeClock()
    policy = make_policy(clock)
    assert policy.enqueue("late", ["k1"], deadline_at=clock.now + 2.0)
    assert policy.enqueue("soon", ["k2"], deadline_at=clock.now + 0.3)
    admitted = policy.advance(consumed=set(), canceled=set())
    assert [plan.req_id for plan in admitted] == ["soon"]
    assert policy.queued_requests == 1


def test_default_deadline_is_enqueue_time_plus_lead() -> None:
    clock = FakeClock()
    policy = make_policy(clock, deadline_s=0.5)
    policy.enqueue("a", ["k"])
    (plan,) = policy.advance(consumed=set(), canceled=set())
    assert plan.deadline_at == clock.now + 0.5


def test_expired_plan_dropped_and_counted() -> None:
    """A plan whose deadline plus grace passed is never staged."""
    clock = FakeClock()
    policy = make_policy(clock, deadline_s=0.5, grace_s=0.2)
    policy.enqueue("stale", ["k"])
    clock.now += 0.71  # past deadline (0.5) + grace (0.2)
    assert policy.advance(consumed=set(), canceled=set()) == []
    assert policy.queued_requests == 0
    assert policy.expired_total == 1


def test_grace_keeps_recent_past_deadline_admittable() -> None:
    """Just past the deadline, the target may still be waiting — stage it."""
    clock = FakeClock()
    policy = make_policy(clock, deadline_s=0.5, grace_s=0.2)
    policy.enqueue("close", ["k"])
    clock.now += 0.65  # past deadline, inside grace
    admitted = policy.advance(consumed=set(), canceled=set())
    assert [plan.req_id for plan in admitted] == ["close"]
    assert policy.expired_total == 0


def test_reenqueue_replaces_queued_keys_and_deadline() -> None:
    """The newest confirmed prefix and arrival estimate win."""
    clock = FakeClock()
    policy = make_policy(clock)
    policy.enqueue("s1", ["old1", "old2"], deadline_at=clock.now + 0.4)
    assert policy.enqueue("s1", ["new1"], deadline_at=clock.now + 0.3)
    assert policy.queued_requests == 1
    assert policy.replaced_total == 1
    (plan,) = policy.advance(consumed=set(), canceled=set())
    assert plan.keys == ("new1",)
    assert plan.deadline_at == clock.now + 0.3


def test_requeue_allowed_while_same_id_inflight() -> None:
    """Session-keyed ids: the next turn's plan queues behind the inflight one."""
    clock = FakeClock()
    policy = make_policy(clock)
    policy.enqueue("s1", ["turn1"])
    assert policy.advance(consumed=set(), canceled=set())
    assert policy.enqueue("s1", ["turn2"])
    assert policy.queued_requests == 1
    assert policy.inflight_requests == 1


def test_released_frees_slot_without_touching_queue() -> None:
    """Parity with FIFO's released semantics (turn_end regression shape)."""
    clock = FakeClock()
    policy = make_policy(clock)
    policy.enqueue("s1", ["turn1"])
    assert policy.advance(consumed=set(), canceled=set())
    policy.enqueue("s1", ["turn2"])
    admitted = policy.advance(consumed=set(), canceled=set(), released={"s1"})
    assert [plan.req_id for plan in admitted] == ["s1"]
    assert admitted[0].keys == ("turn2",)


def test_canceled_removes_queued_and_inflight() -> None:
    clock = FakeClock()
    policy = make_policy(clock, max_requests=2)
    policy.enqueue("a", ["k1"])
    policy.enqueue("b", ["k2"])
    policy.advance(consumed=set(), canceled=set())
    assert policy.inflight_requests == 2
    policy.enqueue("c", ["k3"])
    policy.advance(consumed=set(), canceled={"a", "c"})
    assert policy.inflight_requests == 1
    assert policy.queued_requests == 0


def test_consumed_strands_are_dropped_from_queue() -> None:
    """A request whose load already left cannot use a later-relayed plan."""
    clock = FakeClock()
    policy = make_policy(clock)
    policy.enqueue("a", ["k1"])
    policy.advance(consumed=set(), canceled=set())
    policy.enqueue("b", ["k2"])
    policy.advance(consumed={"b"}, canceled=set())
    assert policy.queued_requests == 0


def test_byte_budget_holds_and_oversized_runs_alone() -> None:
    clock = FakeClock()
    policy = make_policy(clock, max_requests=4, max_bytes=3 * KEY_BYTES)
    policy.enqueue("big", ["k"] * 5, deadline_at=clock.now + 0.1)
    policy.enqueue("small", ["k"], deadline_at=clock.now + 0.2)
    admitted = policy.advance(consumed=set(), canceled=set())
    # The oversized earliest plan runs alone; the small one waits.
    assert [plan.req_id for plan in admitted] == ["big"]
    assert policy.queued_requests == 1
    admitted = policy.advance(consumed=set(), canceled=set(), released={"big"})
    assert [plan.req_id for plan in admitted] == ["small"]


def test_k2_admits_two_by_deadline() -> None:
    clock = FakeClock()
    policy = make_policy(clock, max_requests=2)
    policy.enqueue("c", ["k"], deadline_at=clock.now + 0.3)
    policy.enqueue("a", ["k"], deadline_at=clock.now + 0.1)
    policy.enqueue("b", ["k"], deadline_at=clock.now + 0.2)
    admitted = policy.advance(consumed=set(), canceled=set())
    assert [plan.req_id for plan in admitted] == ["a", "b"]
    assert policy.queued_requests == 1
