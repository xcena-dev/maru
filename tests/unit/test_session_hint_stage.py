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


def _make_scheduler(
    monkeypatch, trigger: str, release: str | None = None
) -> MaruSchedulerConnector:
    monkeypatch.setenv("MARU_STAGE_PIPELINE", "1")
    monkeypatch.setenv("MARU_STAGE_TRIGGER", trigger)
    if release is None:
        monkeypatch.delenv("MARU_STAGE_RELEASE", raising=False)
    else:
        monkeypatch.setenv("MARU_STAGE_RELEASE", release)
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
    def test_terminal_sessions_release_full_window_for_remaining_session(
        self, monkeypatch
    ):
        """Four finished conversations must not strand the live tail of a replay."""
        monkeypatch.setenv("MARU_STAGE_MAX_REQUESTS", "4")
        sched = _make_scheduler(monkeypatch, "turn_end", release="read")
        for i in range(4):
            sid = f"done{i}"
            sched._session_keys[sid] = ("k0",)
            sched._stage_policy.enqueue(_hint_plan_id(sid), ["k0"])
        assert len(sched._stage_policy.advance(consumed=set(), canceled=set())) == 4
        sched._stage_policy.enqueue(_hint_plan_id("live"), ["next"])
        for i in range(4):
            req = _request(req_id=f"final{i}", session=f"done{i}")
            req.kv_transfer_params["maru_session_end"] = True
            sched.request_finished(req, [])
        meta = sched.build_connector_meta(
            _output(finished=[f"final{i}" for i in range(4)])
        )
        assert [p.req_id for p in meta.stage_plans] == [_hint_plan_id("live")]
        assert sched._stage_policy.inflight_requests == 1
        assert sched._stage_policy.queued_requests == 0
        assert set(meta.stage_release_ids) == {
            _hint_plan_id(f"done{i}") for i in range(4)
        }
        assert sched._session_keys == {}

    def test_terminal_request_without_load_cancels_queued_successor(self, monkeypatch):
        sched = _make_scheduler(monkeypatch, "turn_end", release="read")
        sched._stage_policy.enqueue(_hint_plan_id("done"), ["k0"])
        req = _request(session="done", tokens=0)
        req.kv_transfer_params["maru_session_end"] = True
        sched.request_finished(req, [])
        meta = sched.build_connector_meta(_output(finished=[req.request_id]))
        assert meta.stage_plans == []
        assert meta.stage_release_ids == [_hint_plan_id("done")]
        assert sched._stage_policy.queued_requests == 0
        assert sched._stage_policy.inflight_requests == 0

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

    def test_read_release_frees_the_slot_one_step_after_the_relay(self, monkeypatch):
        """release=read 는 읽기가 끝난 다음 스텝에 자리를 돌려준다.

        접두사가 장치 DRAM 에 남아 있어야 하는 것은 GPU 로 복사를 마칠 때까지다.
        읽기는 relay 한 그 스텝의 워커 통과에서 막고 도므로, 다음 스텝이
        시작될 때는 끝나 있다. 기본값 consume 은 그 요청이 끝날 때까지 자리를
        잡고 있어, 디코딩 동안 칸을 헛되게 물고 있었다.
        """
        sched = _make_scheduler(monkeypatch, "imminent", release="read")
        self._prime_hint(sched)
        req = _request(req_id="r1", session="s1")
        sched._pending_stage_aliases["r1"] = _hint_plan_id("s1")
        sched._requests_need_load["r1"] = (req, 2)

        # relay 한 스텝에서는 아직 돌려주지 않는다 — 읽기가 이 스텝에서 돈다.
        meta = sched.build_connector_meta(_output(new_reqs=[req]))
        assert meta.stage_aliases == {"r1": _hint_plan_id("s1")}
        assert meta.stage_release_ids == []
        assert sched._stage_policy.inflight_requests == 1

        # 다음 스텝. 요청은 아직 디코딩 중인데 자리가 돌아온다.
        meta2 = sched.build_connector_meta(_output())
        assert meta2.stage_release_ids == [_hint_plan_id("s1")]
        assert sched._stage_policy.inflight_requests == 0
        assert sched._relayed_stage_aliases == {}

    @pytest.mark.parametrize("policy", ["fifo", "deadline"])
    @pytest.mark.parametrize("release", ["read", "store", "consume"])
    @pytest.mark.parametrize("limit", ["requests", "bytes"])
    def test_preadmitted_hint_keeps_credit_until_release(
        self, monkeypatch, policy, release, limit
    ):
        """A resident prefix and its replacement cannot spend the same credit."""
        monkeypatch.setenv("MARU_STAGE_POLICY", policy)
        monkeypatch.setenv(
            "MARU_STAGE_MAX_REQUESTS", "1" if limit == "requests" else "8"
        )
        monkeypatch.setenv("MARU_STAGE_EST_BYTES_PER_KEY", "16")
        monkeypatch.setenv("MARU_STAGE_MAX_BYTES", "0" if limit == "requests" else "32")
        sched = _make_scheduler(monkeypatch, "turn_end", release=release)
        self._prime_hint(sched)
        first = sched.build_connector_meta(_output())
        assert [p.req_id for p in first.stage_plans] == [_hint_plan_id("s1")]
        assert sched._stage_policy.enqueue(_hint_plan_id("s2"), ["other_a", "other_b"])
        req = _request(req_id="r1", session="s1")
        sched._pending_stage_aliases["r1"] = _hint_plan_id("s1")
        sched._requests_need_load["r1"] = (req, 2)

        reading = sched.build_connector_meta(_output(new_reqs=[req]))

        assert reading.stage_aliases == {"r1": _hint_plan_id("s1")}
        assert reading.stage_release_ids == []
        assert reading.stage_plans == []
        assert sched._stage_policy.inflight_requests == 1
        assert sched._stage_policy.queued_requests == 1
        if release == "consume":
            assert sched.build_connector_meta(_output()).stage_plans == []
            following = sched.build_connector_meta(_output(finished=["r1"]))
        elif release == "store":
            sched._store_scheduled.add("r1")
            assert sched.build_connector_meta(_output()).stage_plans == []
            sched._store_done.add("r1")
            following = sched.build_connector_meta(_output())
        else:
            following = sched.build_connector_meta(_output())
        assert following.stage_release_ids == [_hint_plan_id("s1")]
        assert [p.req_id for p in following.stage_plans] == [_hint_plan_id("s2")]
        assert sched._stage_policy.inflight_requests == 1
        assert sched._stage_policy.queued_requests == 0

    @pytest.mark.parametrize("policy", ["fifo", "deadline"])
    def test_arrived_unadmitted_hint_is_not_staged_after_its_read(
        self, monkeypatch, policy
    ):
        """Retaining a live lease must not retain an obsolete queued plan."""
        monkeypatch.setenv("MARU_STAGE_POLICY", policy)
        monkeypatch.setenv("MARU_STAGE_MAX_REQUESTS", "1")
        sched = _make_scheduler(monkeypatch, "turn_end", release="read")
        self._prime_hint(sched)
        assert sched.build_connector_meta(_output()).stage_plans
        sched._stage_policy.enqueue(_hint_plan_id("s2"), ["other_a", "other_b"])
        req = _request(req_id="r2", session="s2")
        sched._pending_stage_aliases["r2"] = _hint_plan_id("s2")
        sched._requests_need_load["r2"] = (req, 2)
        assert sched.build_connector_meta(_output(new_reqs=[req])).stage_plans == []
        assert sched._stage_policy.inflight_requests == 1
        assert sched._stage_policy.queued_requests == 0
        # The occupied window later opens, but the already-read s2 stays gone.
        assert (
            sched._stage_policy.advance(
                consumed=set(), canceled=set(), released={_hint_plan_id("s1")}
            )
            == []
        )

    def test_consume_release_holds_the_slot_until_the_request_finishes(
        self, monkeypatch
    ):
        """기본값 consume 의 종전 동작 — 요청이 끝날 때까지 자리를 잡고 있다."""
        sched = _make_scheduler(monkeypatch, "imminent")
        self._prime_hint(sched)
        req = _request(req_id="r1", session="s1")
        sched._pending_stage_aliases["r1"] = _hint_plan_id("s1")
        sched._requests_need_load["r1"] = (req, 2)

        sched.build_connector_meta(_output(new_reqs=[req]))
        meta2 = sched.build_connector_meta(_output())
        assert meta2.stage_release_ids == []
        assert sched._stage_policy.inflight_requests == 1

        meta3 = sched.build_connector_meta(_output(finished=["r1"]))
        assert meta3.stage_release_ids == [_hint_plan_id("s1")]
        assert sched._stage_policy.inflight_requests == 0

    def test_read_release_does_not_double_release_a_finished_request(self, monkeypatch):
        """relay 한 스텝에 그 요청이 끝나면 해제는 한 번만 나간다."""
        sched = _make_scheduler(monkeypatch, "imminent", release="read")
        self._prime_hint(sched)
        req = _request(req_id="r1", session="s1")
        sched._pending_stage_aliases["r1"] = _hint_plan_id("s1")
        sched._requests_need_load["r1"] = (req, 2)

        sched.build_connector_meta(_output(new_reqs=[req]))
        # 같은 스텝에 끝난 경우: 끝남 쪽이 먼저 해제한다.
        meta2 = sched.build_connector_meta(_output(finished=["r1"]))
        assert meta2.stage_release_ids == [_hint_plan_id("s1")]
        assert sched._stage_policy.inflight_requests == 0
        # 그 다음 스텝에 또 나가지 않는다.
        meta3 = sched.build_connector_meta(_output())
        assert meta3.stage_release_ids == []

    def test_unknown_release_mode_falls_back_to_consume(self, monkeypatch):
        sched = _make_scheduler(monkeypatch, "imminent", release="nonsense")
        assert sched._stage_release_on_read is False
        assert sched._stage_release_on_complete is False

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
        worker._demand_load_depth = 0
        worker._demand_load_windows = 0
        return worker

    def test_no_refs_is_inactive(self):
        assert self._worker_with_events()._demand_reads_active() is False

    def test_incomplete_copy_is_active(self):
        assert self._worker_with_events(True, False)._demand_reads_active() is True

    def test_all_complete_is_inactive(self):
        assert self._worker_with_events(True, True)._demand_reads_active() is False

    def test_open_demand_window_is_active_without_events(self):
        """The sync packed path records no CUDA events — the depth counter is
        the only signal that sees it (v2 blind spot, experiment note §5.7.3)."""
        worker = self._worker_with_events()
        worker._enter_demand_load()
        assert worker._demand_reads_active() is True
        worker._exit_demand_load()
        assert worker._demand_reads_active() is False

    def test_windows_nest_and_never_go_negative(self):
        worker = self._worker_with_events()
        worker._enter_demand_load()
        worker._enter_demand_load()
        worker._exit_demand_load()
        assert worker._demand_reads_active() is True
        worker._exit_demand_load()
        worker._exit_demand_load()  # extra exit clamps at zero
        assert worker._demand_reads_active() is False
        assert worker._demand_load_depth == 0


class TestDemandWindowRealPath:
    """The v2/v3 blind-spot class is a probe whose signal source is not on the
    executed path — so these tests walk the REAL ``start_load_kv`` sync packed
    path and assert the probe reads True inside it, not just that the counter
    arithmetic works."""

    def _worker(self) -> MaruWorkerConnector:
        import threading

        worker = MaruWorkerConnector.__new__(MaruWorkerConnector)
        worker._deferred_lock = threading.Lock()
        worker._active_load_refs = []
        worker._demand_load_depth = 0
        worker._demand_load_windows = 0
        worker._store_layers_seen = set()
        worker._reclaim_stale_pending_slabs = lambda: None
        worker._timing = False
        worker._ensure_handler = lambda: None
        worker._handler = MagicMock()
        worker._arrival_hint_enabled = False
        worker._stage_enabled = False
        worker._layer_load_events = {}
        worker._kv_chunk_tokens = CHUNK
        worker._use_layerwise = False
        worker._last_attn_metadata = object()
        worker._await_stage = lambda req_id: None
        worker._release_stage_ticket = lambda req_id: None
        worker._build_slot_mapping = lambda block_ids, total: MagicMock()
        worker._get_layer_index = lambda name: 0
        return worker

    def test_probe_is_true_inside_retrieve_and_packed_load(self, monkeypatch):
        worker = self._worker()
        seen: dict = {}

        def fake_retrieve(keys, **kwargs):
            seen["retrieve_active"] = worker._demand_reads_active()
            return [object() for _ in keys]

        def fake_load_packed(layers, prepared, attn):
            seen["load_active"] = worker._demand_reads_active()

        worker._batch_retrieve_all = fake_retrieve
        worker._load_packed = fake_load_packed
        monkeypatch.setattr(
            "maru_vllm.connector._req_chunk_keys", lambda meta, ct: ["k0"]
        )

        layer = SimpleNamespace(kv_cache=MagicMock())
        fw = SimpleNamespace(no_compile_layers={"l0": layer}, attn_metadata=object())
        req_meta = SimpleNamespace(
            req_id="r1",
            is_store=False,
            num_matched_chunks=1,
            deferred_load=False,
            block_ids=([0],),
        )
        meta = SimpleNamespace(
            requests=[req_meta],
            layerwise_load_req_ids=[],
            arrival_hint_keys=[],
            stage_aliases={},
            stage_release_ids=[],
            stage_plans=[],
            preempted_req_ids=set(),
        )

        worker.start_load_kv(fw, meta)

        assert seen == {"retrieve_active": True, "load_active": True}
        assert worker._demand_reads_active() is False
        assert worker._demand_load_windows == 2  # retrieve + packed load

    def test_ensure_handler_wires_the_probe(self, monkeypatch):
        worker = MaruWorkerConnector.__new__(MaruWorkerConnector)
        import threading

        worker._deferred_lock = threading.Lock()
        worker._active_load_refs = []
        worker._demand_load_depth = 0
        worker._demand_load_windows = 0
        worker._handler = None
        worker._handler_retry_after = 0.0
        worker._extra_config = {}
        worker._page_size_bytes = None
        created = MagicMock()
        monkeypatch.setattr(
            "maru_vllm.connector._create_maru_handler", lambda cfg: created
        )

        worker._ensure_handler()

        created.set_demand_probe.assert_called_once_with(worker._demand_reads_active)
