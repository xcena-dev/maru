# SPDX-License-Identifier: Apache-2.0
"""Short responses cannot return store-held credit before the store notice."""

import pytest

from maru_vllm.connector import _hint_plan_id
from tests.unit.test_session_hint_stage import _make_scheduler, _output, _request


@pytest.mark.parametrize("policy", ["fifo", "deadline"])
@pytest.mark.parametrize("ack_before_finish", [False, True])
def test_response_finished_obeys_store_credit(monkeypatch, policy, ack_before_finish):
    monkeypatch.setenv("MARU_STAGE_POLICY", policy)
    monkeypatch.setenv("MARU_STAGE_MAX_REQUESTS", "1")
    scheduler = _make_scheduler(
        monkeypatch, "turn_end" if policy == "fifo" else "imminent", release="store"
    )
    alias = _hint_plan_id("s1")
    assert scheduler._stage_policy.enqueue(alias, ["a", "b"])
    assert scheduler.build_connector_meta(_output()).stage_plans
    request = _request("r1", session="s1")
    scheduler._pending_stage_aliases["r1"] = alias
    scheduler._requests_need_load["r1"] = (request, 2)
    reading = scheduler.build_connector_meta(_output(new_reqs=[request]))
    assert reading.stage_aliases == {"r1": alias}
    assert not reading.stage_release_ids

    # Decode has ended and its next plan is queued, but D2H/registration may
    # still be pending. The older admitted plan owns the sole credit.
    scheduler.request_finished(request, [])
    if policy == "deadline":
        # Deadline policy uses imminent hints, while turn_end supplies FIFO's
        # not_before argument. Queue the next imminent hint explicitly.
        assert scheduler._stage_policy.enqueue(alias, ["next_a", "next_b"])
    if ack_before_finish:
        scheduler.note_store_finished({"r1"})
    finished = scheduler.build_connector_meta(_output(finished=["r1"]))
    if not ack_before_finish:
        assert not finished.stage_release_ids
        assert not finished.stage_plans
        assert scheduler._stage_policy.inflight_requests == 1
        assert scheduler._stage_policy.queued_requests == 1
        assert not scheduler.build_connector_meta(_output()).stage_release_ids
        scheduler.note_store_finished({"r1"})
        finished = scheduler.build_connector_meta(_output())
    assert finished.stage_release_ids == [alias]
    assert [p.req_id for p in finished.stage_plans] == [alias]
    assert scheduler._stage_policy.inflight_requests == 1
    assert scheduler._stage_policy.queued_requests == 0
    assert not scheduler.build_connector_meta(_output()).stage_release_ids


def test_preemption_can_cancel_the_old_ticket(monkeypatch):
    monkeypatch.setenv("MARU_STAGE_MAX_REQUESTS", "1")
    scheduler = _make_scheduler(monkeypatch, "turn_end", release="store")
    alias = _hint_plan_id("s1")
    scheduler._stage_policy.enqueue(alias, ["a", "b"])
    scheduler.build_connector_meta(_output())
    request = _request("r1", session="s1")
    scheduler._pending_stage_aliases["r1"] = alias
    scheduler._requests_need_load["r1"] = (request, 2)
    scheduler.build_connector_meta(_output(new_reqs=[request]))
    output = _output()
    output.preempted_req_ids = {"r1"}
    canceled = scheduler.build_connector_meta(output)
    assert canceled.stage_release_ids == [alias]
    assert scheduler._stage_policy.inflight_requests == 0
