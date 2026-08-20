# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for session-hint staging (MARU_STAGE_TRIGGER=turn_end|imminent).

Covers the scheduler-side session registry / hint triggers / req-to-ticket
alias plumbing and the worker-side alias join and pin-release dispatch. The
frozen match-trigger behavior is asserted unchanged.
"""

from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from maru_handler import StageResult
from maru_vllm.connector import (
    MaruConnectorMetadata,
    MaruSchedulerConnector,
    MaruWorkerConnector,
    _hint_plan_id,
    _request_session_params,
)
from maru_vllm.staging_prefetch import (
    DeadlineStagePolicy,
    FifoStagePolicy,
    StagePlan,
    StageTicket,
)

CHUNK = 8


def _request(
    req_id: str = "r1",
    tokens: int = 16,
    session: str | None = None,
    imminent: str | None = None,
) -> SimpleNamespace:
    params: dict = {}
    if session:
        params["maru_session_id"] = session
    if imminent:
        params["maru_imminent_session"] = imminent
    return SimpleNamespace(
        request_id=req_id,
        req_id=req_id,
        prompt_token_ids=list(range(tokens)),
        block_ids=([0, 1, 2, 3],),
        kv_transfer_params=params or None,
    )


def _output(new_reqs=(), finished=(), num_scheduled=None):
    return SimpleNamespace(
        scheduled_new_reqs=list(new_reqs),
        scheduled_cached_reqs=SimpleNamespace(
            req_ids=[], new_block_ids=[], num_computed_tokens=[], resumed_req_ids=set()
        ),
        num_scheduled_tokens=num_scheduled or {},
        finished_req_ids=set(finished),
        preempted_req_ids=set(),
    )


def _make_scheduler(monkeypatch, trigger: str) -> MaruSchedulerConnector:
    monkeypatch.setenv("MARU_STAGE_PIPELINE", "1")
    monkeypatch.setenv("MARU_STAGE_TRIGGER", trigger)
    sched = MaruSchedulerConnector(block_size=4, kv_chunk_tokens=CHUNK, extra_config={})
    sched._handler = MagicMock()
    return sched


class TestPolicySelection:
    def test_default_policy_is_fifo(self, monkeypatch):
        monkeypatch.delenv("MARU_STAGE_POLICY", raising=False)
        sched = _make_scheduler(monkeypatch, "imminent")
        assert isinstance(sched._stage_policy, FifoStagePolicy)

    def test_deadline_policy_selected_by_env(self, monkeypatch):
        monkeypatch.setenv("MARU_STAGE_POLICY", "deadline")
        sched = _make_scheduler(monkeypatch, "imminent")
        assert isinstance(sched._stage_policy, DeadlineStagePolicy)

    def test_unknown_policy_falls_back_to_fifo(self, monkeypatch):
        monkeypatch.setenv("MARU_STAGE_POLICY", "bogus")
        sched = _make_scheduler(monkeypatch, "imminent")
        assert isinstance(sched._stage_policy, FifoStagePolicy)

    def test_deadline_policy_serves_imminent_hint(self, monkeypatch):
        monkeypatch.setenv("MARU_STAGE_POLICY", "deadline")
        sched = _make_scheduler(monkeypatch, "imminent")
        sched._session_keys["s2"] = ("kv_a", "kv_b")
        sched._handler.batch_exists.return_value = [False, False]
        sched.get_num_new_matched_tokens(_request(req_id="r1", imminent="s2"), 0)
        assert sched._stage_policy.queued_requests == 1


class TestSessionParams:
    def test_extracts_both(self):
        req = _request(session="s1", imminent="s2")
        assert _request_session_params(req) == ("s1", "s2")

    def test_absent_params(self):
        req = SimpleNamespace(kv_transfer_params=None)
        assert _request_session_params(req) == (None, None)

    def test_malformed_params(self):
        req = SimpleNamespace(kv_transfer_params="not-a-dict")
        assert _request_session_params(req) == (None, None)

    def test_missing_attribute(self):
        assert _request_session_params(SimpleNamespace()) == (None, None)


class TestSchedulerTriggers:
    def test_match_mode_keeps_p0_behavior(self, monkeypatch):
        sched = _make_scheduler(monkeypatch, "match")
        req = _request(session="s1")
        sched.request_finished(req, [])
        assert sched._session_keys == {}
        assert sched._stage_policy.queued_requests == 0

    def test_turn_end_records_prefix_and_enqueues(self, monkeypatch):
        sched = _make_scheduler(monkeypatch, "turn_end")
        req = _request(session="s1")
        sched.request_finished(req, [])
        assert "s1" in sched._session_keys
        assert len(sched._session_keys["s1"]) == 2  # 16 tokens / chunk 8
        assert sched._stage_policy.queued_requests == 1

    def test_turn_end_without_session_is_noop(self, monkeypatch):
        sched = _make_scheduler(monkeypatch, "turn_end")
        sched.request_finished(_request(), [])
        assert sched._session_keys == {}
        assert sched._stage_policy.queued_requests == 0

    def test_imminent_enqueues_from_registry(self, monkeypatch):
        sched = _make_scheduler(monkeypatch, "imminent")
        sched._session_keys["s2"] = ("kv_a", "kv_b")
        sched._handler.batch_exists.return_value = [False, False]
        sched.get_num_new_matched_tokens(_request(req_id="r1", imminent="s2"), 0)
        assert sched._stage_policy.queued_requests == 1

    def test_imminent_without_registry_is_noop(self, monkeypatch):
        sched = _make_scheduler(monkeypatch, "imminent")
        sched._handler.batch_exists.return_value = [False, False]
        sched.get_num_new_matched_tokens(_request(req_id="r1", imminent="s2"), 0)
        assert sched._stage_policy.queued_requests == 0

    def test_arrival_registers_alias(self, monkeypatch):
        sched = _make_scheduler(monkeypatch, "imminent")
        sched._handler.batch_exists.return_value = [False, False]
        sched.get_num_new_matched_tokens(_request(req_id="r1", session="s1"), 0)
        assert sched._pending_stage_aliases == {"r1": _hint_plan_id("s1")}

    def test_imminent_registry_survives_turn_updates(self, monkeypatch):
        sched = _make_scheduler(monkeypatch, "imminent")
        sched.request_finished(_request(req_id="r1", session="s1", tokens=16), [])
        assert "s1" in sched._session_keys
        # imminent mode records the prefix but does not stage at turn end
        assert sched._stage_policy.queued_requests == 0


class TestBuildMetaAliasPlumbing:
    def _prime_hint(self, sched, session="s1"):
        sched._session_keys[session] = ("kv_a", "kv_b")
        assert sched._stage_policy.enqueue(_hint_plan_id(session), ["kv_a", "kv_b"])

    def test_load_request_consumes_hint_and_relays_alias(self, monkeypatch):
        sched = _make_scheduler(monkeypatch, "imminent")
        self._prime_hint(sched)
        req = _request(req_id="r1", session="s1")
        sched._pending_stage_aliases["r1"] = _hint_plan_id("s1")
        sched._requests_need_load["r1"] = (req, 2)

        meta = sched.build_connector_meta(_output(new_reqs=[req]))

        assert meta.stage_aliases == {"r1": _hint_plan_id("s1")}
        assert [p.req_id for p in meta.stage_plans] == [_hint_plan_id("s1")]
        assert "r1" not in sched._pending_stage_aliases
        # Same-step admit + consume: the slot re-entered inflight after the
        # consumed set was applied; it is reclaimed when the request finishes.
        assert sched._stage_policy.inflight_requests == 1

        meta2 = sched.build_connector_meta(_output(finished=["r1"]))

        assert sched._stage_policy.inflight_requests == 0
        # The worker drops the (already consumed) ticket idempotently.
        assert meta2.stage_release_ids == [_hint_plan_id("s1")]

    def test_turn_end_plan_survives_its_requests_finish_sweep(self, monkeypatch):
        """The finish sweep must not cancel the plan queued at that finish.

        Live regression (2026-08-20): every arriving session request registers
        a pending alias to its session's hint plan id; when the request
        finished, the stale sweep canceled that id — killing the turn-end
        plan the same finish had just queued, so no stage was ever admitted.
        """
        sched = _make_scheduler(monkeypatch, "turn_end")
        req = _request(req_id="r1", session="s1")
        # Arrival registers the alias (get_num_new_matched path).
        sched._process_session_hints(req)
        assert sched._pending_stage_aliases == {"r1": _hint_plan_id("s1")}
        # Completion queues the next turn's plan.
        sched.request_finished(req, [])
        # The next step sweeps r1 as finished — the fresh plan must survive
        # and be admitted.
        meta = sched.build_connector_meta(_output(finished=["r1"]))
        assert [p.req_id for p in meta.stage_plans] == [_hint_plan_id("s1")]
        assert "r1" not in sched._pending_stage_aliases

    def test_turn_end_relayed_release_spares_fresh_plan(self, monkeypatch):
        """Retiring a consumed stage must not kill the same-id queued plan."""
        sched = _make_scheduler(monkeypatch, "turn_end")
        # Steady state: r2 consumed its session's stage earlier (alias
        # relayed, plan inflight), then finishes — queueing the next plan.
        sched._relayed_stage_aliases["r2"] = _hint_plan_id("s1")
        sched._stage_policy.enqueue(_hint_plan_id("s1"), ["k0"])
        assert sched._stage_policy.advance(consumed=set(), canceled=set())
        req = _request(req_id="r2", session="s1")
        sched.request_finished(req, [])
        meta = sched.build_connector_meta(_output(finished=["r2"]))
        # The consumed stage's ticket drops, its slot frees, and the fresh
        # plan is admitted into it.
        assert meta.stage_release_ids == [_hint_plan_id("s1")]
        assert [p.req_id for p in meta.stage_plans] == [_hint_plan_id("s1")]

    def test_stale_request_emits_ticket_release(self, monkeypatch):
        sched = _make_scheduler(monkeypatch, "imminent")
        self._prime_hint(sched)
        sched._pending_stage_aliases["r1"] = _hint_plan_id("s1")

        meta = sched.build_connector_meta(_output(finished=["r1"]))

        assert meta.stage_release_ids == [_hint_plan_id("s1")]
        assert meta.stage_aliases == {}
        assert sched._stage_policy.inflight_requests == 0
        assert sched._stage_policy.queued_requests == 0

    def test_store_meta_leaves_alias_pending(self, monkeypatch):
        sched = _make_scheduler(monkeypatch, "imminent")
        req = _request(req_id="r1", session="s1")
        sched._pending_stage_aliases["r1"] = _hint_plan_id("s1")

        meta = sched.build_connector_meta(
            _output(new_reqs=[req], num_scheduled={"r1": 16})
        )

        assert meta.requests and meta.requests[0].is_store
        assert meta.stage_aliases == {}
        assert sched._pending_stage_aliases == {"r1": _hint_plan_id("s1")}


def _ready_result(keys: int = 2) -> StageResult:
    return StageResult(
        requested_keys=keys,
        found_keys=keys,
        eligible_keys=keys,
        prepared_bytes=keys * 32,
        issued_ranges=1,
    )


def _ready_ticket(plan_id: str, keys=("kv_a", "kv_b")) -> StageTicket:
    ticket = StageTicket(
        StagePlan(req_id=plan_id, keys=tuple(keys), estimated_bytes=64)
    )
    future: Future = Future()
    ticket.bind(future)
    future.set_result(_ready_result(len(keys)))
    return ticket


class _ImmediateExecutor:
    """Runs submitted jobs inline; records that a submit happened."""

    def __init__(self):
        self.submitted = 0

    def submit(self, fn, *args):
        self.submitted += 1
        fn(*args)
        return None


def _make_worker(pin: bool = False) -> MaruWorkerConnector:
    worker = MaruWorkerConnector.__new__(MaruWorkerConnector)
    worker._stage_enabled = True
    worker._stage_pin_enabled = pin
    worker._stage_aliases = {}
    worker._stage_tickets = {}
    import threading

    worker._stage_lock = threading.Lock()
    worker._stage_executor = _ImmediateExecutor()
    worker._handler = MagicMock()
    worker._timing = False
    return worker


class TestWorkerAliasJoin:
    def test_await_resolves_alias_to_hint_ticket(self):
        worker = _make_worker()
        hint_id = _hint_plan_id("s1")
        worker._stage_tickets[hint_id] = _ready_ticket(hint_id)
        worker._stage_aliases["r1"] = hint_id

        result = worker._await_stage("r1")

        assert result is not None and result.ready

    def test_await_without_alias_uses_req_id(self):
        worker = _make_worker()
        worker._stage_tickets["r1"] = _ready_ticket("r1")
        assert worker._await_stage("r1") is not None

    def test_release_pops_alias_and_ticket(self):
        worker = _make_worker()
        hint_id = _hint_plan_id("s1")
        worker._stage_tickets[hint_id] = _ready_ticket(hint_id)
        worker._stage_aliases["r1"] = hint_id

        worker._release_stage_ticket("r1")

        assert worker._stage_tickets == {}
        assert worker._stage_aliases == {}

    def test_release_by_hint_id_directly(self):
        worker = _make_worker()
        hint_id = _hint_plan_id("s1")
        worker._stage_tickets[hint_id] = _ready_ticket(hint_id)

        worker._release_stage_ticket(hint_id)

        assert worker._stage_tickets == {}


class TestWorkerPinReleaseDispatch:
    def test_release_dispatches_stage_release_when_pin_enabled(self):
        worker = _make_worker(pin=True)
        hint_id = _hint_plan_id("s1")
        worker._stage_tickets[hint_id] = _ready_ticket(hint_id)

        worker._release_stage_ticket(hint_id)

        worker._handler.stage_release.assert_called_once_with(["kv_a", "kv_b"])
        assert worker._stage_executor.submitted == 1

    def test_release_skips_dispatch_when_pin_disabled(self):
        worker = _make_worker(pin=False)
        worker._stage_tickets["r1"] = _ready_ticket("r1")

        worker._release_stage_ticket("r1")

        worker._handler.stage_release.assert_not_called()

    def test_cancel_dispatches_stage_release(self):
        worker = _make_worker(pin=True)
        hint_id = _hint_plan_id("s2")
        worker._stage_tickets[hint_id] = _ready_ticket(hint_id)
        worker._stage_aliases["r2"] = hint_id

        worker._cancel_stage_requests({"r2"})

        worker._handler.stage_release.assert_called_once_with(["kv_a", "kv_b"])
        assert worker._stage_tickets == {}


class TestMetadataDefaults:
    def test_new_fields_default_empty(self):
        meta = MaruConnectorMetadata()
        assert meta.stage_aliases == {}
        assert meta.stage_release_ids == []


class TestTriggerValidation:
    def test_unknown_trigger_falls_back_to_match(self, monkeypatch):
        sched = _make_scheduler(monkeypatch, "sometimes")
        assert sched._stage_trigger == "match"

    @pytest.mark.parametrize("trigger", ["match", "turn_end", "imminent"])
    def test_known_triggers_accepted(self, monkeypatch, trigger):
        sched = _make_scheduler(monkeypatch, trigger)
        assert sched._stage_trigger == trigger


class TestDemandReadsActive:
    """Worker-side ground truth for the plugin's stage-yield probe."""

    def _worker_with_events(self, *done_flags: bool) -> MaruWorkerConnector:
        import threading

        worker = MaruWorkerConnector.__new__(MaruWorkerConnector)
        worker._deferred_lock = threading.Lock()
        worker._active_load_refs = [
            (SimpleNamespace(query=lambda done=done: done), []) for done in done_flags
        ]
        return worker

    def test_no_refs_is_inactive(self):
        assert self._worker_with_events()._demand_reads_active() is False

    def test_incomplete_copy_is_active(self):
        assert self._worker_with_events(True, False)._demand_reads_active() is True

    def test_all_complete_is_inactive(self):
        assert self._worker_with_events(True, True)._demand_reads_active() is False
