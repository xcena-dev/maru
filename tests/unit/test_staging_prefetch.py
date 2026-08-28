# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for completion-returning SSD-to-DRAM request staging."""

from __future__ import annotations

import threading
from concurrent.futures import Future, ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from maru import MaruConfig, MaruHandler
from maru_handler import StageResult
from maru_vllm.connector import (
    MaruConnectorMetadata,
    MaruSchedulerConnector,
    MaruWorkerConnector,
)
from maru_vllm.staging_prefetch import (
    FifoStagePolicy,
    FillDurationMailbox,
    HymCacheObject,
    HymCacheRollingPipeline,
    ObservedTurnEstimator,
    StageCompletionMailbox,
    StagePlan,
    StageState,
    StageTicket,
    build_hymcache_objects,
)


def _ready_result(keys: int = 1, prepared_bytes: int = 4096) -> StageResult:
    return StageResult(
        requested_keys=keys,
        found_keys=keys,
        eligible_keys=keys,
        prepared_bytes=prepared_bytes,
        issued_ranges=1,
    )


def _empty_scheduler_output() -> SimpleNamespace:
    return SimpleNamespace(
        scheduled_new_reqs=[],
        scheduled_cached_reqs=SimpleNamespace(
            req_ids=[],
            new_block_ids=[],
            num_computed_tokens=[],
            resumed_req_ids=set(),
        ),
        num_scheduled_tokens={},
        finished_req_ids=set(),
        preempted_req_ids=set(),
    )


class TestHymCacheRollingWindow:
    def test_builds_one_ordered_object_per_llama_block(self):
        mib = 1024**2
        keys = [f"k{i}" for i in range(9)]

        objects = build_hymcache_objects(
            "r0",
            keys,
            [16 * mib] * len(keys),
        )

        assert [obj.key for obj in objects] == keys
        assert [obj.index for obj in objects] == list(range(9))
        assert [obj.nbytes for obj in objects] == [16 * mib] * 9

    def test_128_mib_rolling_window_holds_eight_llama_blocks(self):
        mib = 1024**2
        objects = build_hymcache_objects(
            "r0",
            [f"k{i}" for i in range(9)],
            [16 * mib] * 9,
        )
        events: list[str] = []
        active = 0
        peak = 0

        def stage(obj):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            events.append(f"stage-{obj.index}")
            return obj.key

        def release(obj):
            nonlocal active
            active -= 1
            events.append(f"release-{obj.index}")

        with ThreadPoolExecutor(max_workers=1) as executor:
            HymCacheRollingPipeline(executor, window_bytes=128 * mib).run(
                [objects],
                stage=stage,
                consume=lambda obj, result: None,
                release=release,
            )

        assert peak == 8
        assert events.index("release-0") < events.index("stage-8")
        assert active == 0

    def test_oversized_object_runs_alone_without_reordering(self):
        objects = build_hymcache_objects(
            "r0",
            ["a", "big", "c"],
            [4, 20, 4],
        )
        events: list[str] = []
        active_bytes = 0
        max_active_bytes = 0

        def stage(obj):
            nonlocal active_bytes, max_active_bytes
            active_bytes += obj.nbytes
            max_active_bytes = max(max_active_bytes, active_bytes)
            events.append(f"stage-{obj.key}")
            return obj.key

        def consume(obj, result):
            assert result == obj.key
            events.append(f"consume-{obj.key}")

        def release(obj):
            nonlocal active_bytes
            active_bytes -= obj.nbytes
            events.append(f"release-{obj.key}")

        with ThreadPoolExecutor(max_workers=1) as executor:
            HymCacheRollingPipeline(executor, window_bytes=8).run(
                [objects],
                stage=stage,
                consume=consume,
                release=release,
            )

        assert [event for event in events if event.startswith("consume-")] == [
            "consume-a",
            "consume-big",
            "consume-c",
        ]
        assert events.index("release-a") < events.index("stage-big")
        assert events.index("release-big") < events.index("stage-c")
        assert max_active_bytes == 20
        assert active_bytes == 0

    def test_issue_next_precedes_consume_and_leases_stay_bounded(self):
        objects = build_hymcache_objects(
            "r0",
            ["a", "b", "c"],
            [4, 4, 4],
        )
        events: list[str] = []
        next_staged = threading.Event()
        active_leases = 0
        max_active_leases = 0
        lock = threading.Lock()

        def stage(obj):
            nonlocal active_leases, max_active_leases
            with lock:
                active_leases += 1
                max_active_leases = max(max_active_leases, active_leases)
                events.append(f"stage-{obj.index}")
            if obj.index == 1:
                next_staged.set()
            return obj.key

        def consume(obj, result):
            events.append(f"consume-start-{obj.index}")
            assert result == obj.key
            if obj.index == 0:
                assert next_staged.wait(timeout=1.0)
            events.append(f"consume-end-{obj.index}")

        def release(obj):
            nonlocal active_leases
            with lock:
                events.append(f"release-{obj.index}")
                active_leases -= 1

        with ThreadPoolExecutor(max_workers=1) as executor:
            timings = HymCacheRollingPipeline(executor, window_bytes=8).run(
                [objects],
                stage=stage,
                consume=consume,
                release=release,
            )

        assert events.index("stage-1") < events.index("consume-end-0")
        assert events.index("release-0") < events.index("stage-2")
        assert max_active_leases == 2
        assert active_leases == 0
        assert [timing.object.index for timing in timings] == [0, 1, 2]

    def test_concurrent_requests_fill_initial_depth_round_robin(self):
        requests = [
            build_hymcache_objects("r0", ["a0", "a1"], [4, 4]),
            build_hymcache_objects("r1", ["b0", "b1"], [4, 4]),
        ]
        staged: list[str] = []

        def stage(obj):
            staged.append(obj.key)
            return obj.key

        with ThreadPoolExecutor(max_workers=1) as executor:
            HymCacheRollingPipeline(executor, window_bytes=8).run(
                requests,
                stage=stage,
                consume=lambda obj, result: None,
                release=lambda obj: None,
            )

        assert staged == ["a0", "b0", "a1", "b1"]

    def test_connector_mode_excludes_other_hint_pipelines(self, monkeypatch):
        monkeypatch.setenv("MARU_HYMCACHE_WINDOW_BYTES", str(128 * 1024**2))
        monkeypatch.setenv("MARU_STAGE_PIPELINE", "1")
        monkeypatch.setenv("MARU_ARRIVAL_HINT", "1")

        scheduler = MaruSchedulerConnector(
            block_size=4,
            kv_chunk_tokens=4,
            extra_config={},
        )
        worker = MaruWorkerConnector(
            block_size=4,
            kv_chunk_tokens=4,
            extra_config={"maru_enable_deferred_loading": True},
        )

        assert scheduler._hymcache_window_bytes == 128 * 1024**2
        assert not scheduler._stage_enabled
        assert not scheduler._arrival_hint_enabled
        assert worker._hymcache_window_bytes == 128 * 1024**2
        assert not worker._stage_enabled
        assert not worker._arrival_hint_enabled
        assert not worker._try_submit_deferred_packed_load(SimpleNamespace())
        worker.shutdown()

    def test_packed_load_dispatches_to_hymcache_path(self, monkeypatch):
        monkeypatch.setenv("MARU_HYMCACHE_WINDOW_BYTES", "4096")
        worker = MaruWorkerConnector(
            block_size=4,
            kv_chunk_tokens=4,
            extra_config={},
        )
        worker._load_packed_hymcache = MagicMock()

        worker._load_packed([], [], None)

        worker._load_packed_hymcache.assert_called_once_with([], [], None)
        worker.shutdown()

    def test_no_window_keeps_demand_path_and_stages_nothing(self, monkeypatch):
        """W=0 is the demand-load baseline: no Stage 1 call may be issued.

        The GPU-read timing added to the demand path must not turn it into a
        prefetching setting, so this pins that nothing reaches the handler's
        staging entry point.
        """
        import torch

        monkeypatch.delenv("MARU_HYMCACHE_WINDOW_BYTES", raising=False)
        worker = MaruWorkerConnector(
            block_size=4,
            kv_chunk_tokens=4,
            extra_config={},
        )
        worker._load_packed_hymcache = MagicMock()
        handler = MagicMock()
        worker._handler = handler
        layers = [("model.layers.0.self_attn", torch.zeros(2, 4, 1, 1), 0)]

        worker._load_packed(layers, [], None)

        assert worker._hymcache_window_bytes == 0
        worker._load_packed_hymcache.assert_not_called()
        handler.stage_batch.assert_not_called()
        worker.shutdown()


class TestStageCompletionMailbox:
    def test_drain_reports_each_completion_once(self):
        box = StageCompletionMailbox()
        box.publish("r0")
        box.publish("r1")

        assert box.drain() == {"r0", "r1"}
        assert box.drain() == set()

    def test_duplicate_publish_of_one_plan_counts_once_in_the_drain(self):
        box = StageCompletionMailbox()
        box.publish("r0")
        box.publish("r0")

        assert box.drain() == {"r0"}

    def test_published_counts_every_publish_so_a_split_process_is_visible(self):
        box = StageCompletionMailbox()
        assert box.published == 0

        box.publish("r0")
        box.drain()

        # 비운 뒤에도 누계는 남는다 — 스케줄러가 「완료를 한 건도 못 받았다」와
        # 「받았지만 지금은 없다」를 구별하는 근거다.
        assert box.published == 1

    def test_publish_from_many_threads_loses_nothing(self):
        box = StageCompletionMailbox()
        # 스레드 수는 잠금이 실제로 걸리는지 보일 만큼만 둔다. 많이 띄우면 같은
        # 프로세스의 시각 민감한 테스트를 흔든다.
        ids = [f"r{i}" for i in range(24)]

        threads = [threading.Thread(target=box.publish, args=(rid,)) for rid in ids]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert box.drain() == set(ids)
        assert box.published == len(ids)


class TestObservedTurnEstimator:
    def test_it_refuses_to_answer_without_history(self):
        est = ObservedTurnEstimator(margin_s=1.0)

        assert est.launch_at("u1") is None

    def test_one_turn_start_is_not_enough(self):
        est = ObservedTurnEstimator(margin_s=1.0)
        est.note_turn_start("u1", 100.0)
        est.note_fill(0.3)

        # 간격을 알려면 시작이 둘 있어야 한다.
        assert est.launch_at("u1") is None

    def test_it_needs_a_fill_measurement_too(self):
        est = ObservedTurnEstimator(margin_s=1.0)
        est.note_turn_start("u1", 100.0)
        est.note_turn_start("u1", 107.0)

        # 적재가 얼마나 걸리는지 모르면 언제 시작해야 하는지도 모른다.
        assert est.launch_at("u1") is None

    def test_launch_is_the_estimate_minus_the_fill_and_the_margin(self):
        est = ObservedTurnEstimator(margin_s=1.0)
        est.note_turn_start("u1", 100.0)
        est.note_turn_start("u1", 107.0)
        est.note_fill(0.3)

        # 다음 시작 추정 107 + 7 = 114, 거기서 적재 0.3 과 여유 1.0 을 뺀다.
        assert est.launch_at("u1") == pytest.approx(112.7)

    def test_each_session_keeps_its_own_interval(self):
        est = ObservedTurnEstimator(margin_s=0.0)
        est.note_fill(0.5)
        for session, step in (("u1", 4.0), ("u2", 9.0)):
            est.note_turn_start(session, 10.0)
            est.note_turn_start(session, 10.0 + step)

        # 라운드 안 위치가 다르면 간격도 다르다. 전역 평균은 둘 다 틀린다.
        assert est.launch_at("u1") == pytest.approx(14.0 + 4.0 - 0.5)
        assert est.launch_at("u2") == pytest.approx(19.0 + 9.0 - 0.5)

    def test_a_backwards_or_equal_timestamp_is_ignored(self):
        est = ObservedTurnEstimator(margin_s=0.0)
        est.note_fill(0.5)
        est.note_turn_start("u1", 100.0)
        est.note_turn_start("u1", 100.0)

        assert est.launch_at("u1") is None

    def test_the_estimate_follows_a_changed_interval(self):
        est = ObservedTurnEstimator(margin_s=0.0, weight=1.0)
        est.note_fill(0.0 + 0.5)
        est.note_turn_start("u1", 0.0)
        est.note_turn_start("u1", 10.0)
        first = est.launch_at("u1")
        est.note_turn_start("u1", 14.0)
        second = est.launch_at("u1")

        # 가중치 1 이면 최신 간격만 본다: 10 → 4 로 줄면 추정도 따라 준다.
        assert first == pytest.approx(20.0 - 0.5)
        assert second == pytest.approx(18.0 - 0.5)


class TestPerPlanLaunchInstant:
    def test_a_plan_waits_for_its_own_launch_instant(self):
        now = [0.0]
        policy = FifoStagePolicy(
            max_requests=1,
            max_bytes=0,
            estimated_bytes_per_key=100,
            clock=lambda: now[0],
        )
        policy.enqueue("u1", ["a"], not_before=5.0)

        assert policy.advance(consumed=set(), canceled=set()) == []

        now[0] = 5.0

        assert [p.req_id for p in policy.advance(consumed=set(), canceled=set())] == [
            "u1"
        ]

    def test_a_plan_without_a_launch_instant_goes_at_once(self):
        policy = FifoStagePolicy(
            max_requests=1,
            max_bytes=0,
            estimated_bytes_per_key=100,
        )
        policy.enqueue("u1", ["a"])

        assert [p.req_id for p in policy.advance(consumed=set(), canceled=set())] == [
            "u1"
        ]

    def test_plans_do_not_all_wait_the_same_amount(self):
        now = [0.0]
        policy = FifoStagePolicy(
            max_requests=2,
            max_bytes=0,
            estimated_bytes_per_key=100,
            clock=lambda: now[0],
        )
        policy.enqueue("early", ["a"], not_before=1.0)
        policy.enqueue("late", ["b"], not_before=9.0)

        now[0] = 1.0
        first = policy.advance(consumed=set(), canceled=set())

        # 고정 지연과 다른 점이 이것이다 — 계획마다 자기 시각을 갖는다.
        assert [p.req_id for p in first] == ["early"]

    def test_a_plan_that_is_due_goes_past_one_that_is_not(self):
        """줄 머리가 아직 때가 아니어도 뒤에 선 계획은 나간다.

        줄은 등록 순서인데 발사 시각은 세션마다 다른 턴 간격이 정하므로 두 순서는
        어긋난다. 머리에서 멈추면 여유가 계획별 발사 시각이 아니라 줄 전체의 방출
        속도를 조절하는 손잡이가 되어 버린다.
        """
        now = [0.0]
        policy = FifoStagePolicy(
            max_requests=1,
            max_bytes=0,
            estimated_bytes_per_key=100,
            clock=lambda: now[0],
        )
        policy.enqueue("head", ["a"], not_before=9.0)
        policy.enqueue("tail", ["b"], not_before=1.0)

        now[0] = 1.0

        assert [p.req_id for p in policy.advance(consumed=set(), canceled=set())] == [
            "tail"
        ]

    def test_a_skipped_plan_stays_queued_and_goes_when_it_is_due(self):
        now = [0.0]
        policy = FifoStagePolicy(
            max_requests=1,
            max_bytes=0,
            estimated_bytes_per_key=100,
            clock=lambda: now[0],
        )
        policy.enqueue("head", ["a"], not_before=9.0)
        policy.enqueue("tail", ["b"], not_before=1.0)

        now[0] = 1.0
        policy.advance(consumed=set(), canceled=set())
        assert policy.queued_requests == 1

        now[0] = 9.0

        assert [
            p.req_id for p in policy.advance(consumed={"tail"}, canceled=set())
        ] == ["head"]

    def test_skipped_plans_keep_their_order_among_themselves(self):
        now = [0.0]
        policy = FifoStagePolicy(
            max_requests=2,
            max_bytes=0,
            estimated_bytes_per_key=100,
            clock=lambda: now[0],
        )
        policy.enqueue("second", ["a"], not_before=8.0)
        policy.enqueue("third", ["b"], not_before=9.0)
        policy.enqueue("first", ["c"], not_before=1.0)

        now[0] = 1.0
        policy.advance(consumed=set(), canceled=set())

        now[0] = 9.0
        rest = policy.advance(consumed={"first"}, canceled=set())

        assert [p.req_id for p in rest] == ["second", "third"]


class TestFillDurationMailbox:
    def test_drain_reports_each_sample_once(self):
        box = FillDurationMailbox()
        box.publish(0.3)
        box.publish(0.4)

        assert box.drain() == [0.3, 0.4]
        assert box.drain() == []

    def test_a_non_positive_sample_is_dropped(self):
        box = FillDurationMailbox()
        box.publish(0.0)
        box.publish(-1.0)

        assert box.drain() == []


class TestFifoStagePolicy:
    def test_request_window_preserves_fifo(self):
        policy = FifoStagePolicy(
            max_requests=1,
            max_bytes=1024,
            estimated_bytes_per_key=100,
        )
        assert policy.enqueue("r0", ["a", "b"])
        assert policy.enqueue("r1", ["c"])

        first = policy.advance(consumed=set(), canceled=set())
        assert [plan.req_id for plan in first] == ["r0"]
        assert policy.queued_requests == 1

        second = policy.advance(consumed={"r0"}, canceled=set())
        assert [plan.req_id for plan in second] == ["r1"]

    def test_completed_frees_the_slot_without_waiting_for_consumption(self):
        policy = FifoStagePolicy(
            max_requests=1,
            max_bytes=0,
            estimated_bytes_per_key=100,
        )
        assert policy.enqueue("r0", ["a"])
        assert policy.enqueue("r1", ["b"])

        first = policy.advance(consumed=set(), canceled=set())
        assert [plan.req_id for plan in first] == ["r0"]

        # r0 의 적재가 끝났다. 그 접두사는 아직 소비되지 않았지만, 창 자리가
        # 지키는 것은 「장치에 동시에 걸린 적재 수」이므로 지금 돌아와야 한다.
        second = policy.advance(consumed=set(), canceled=set(), completed={"r0"})

        assert [plan.req_id for plan in second] == ["r1"]
        assert policy.inflight_requests == 1

    def test_completed_leaves_queued_plans_alone(self):
        policy = FifoStagePolicy(
            max_requests=1,
            max_bytes=0,
            estimated_bytes_per_key=100,
        )
        policy.enqueue("s0", ["a"])
        policy.advance(consumed=set(), canceled=set())
        # turn_end 모드에서는 같은 세션 id 가 다음 턴 계획을 이미 줄에 갖는다.
        policy.enqueue("s0", ["b"])

        admitted = policy.advance(consumed=set(), canceled=set(), completed={"s0"})

        assert [plan.req_id for plan in admitted] == ["s0"]
        assert policy.queued_requests == 0

    def test_completed_retire_is_logged_with_its_own_reason(self):
        policy = FifoStagePolicy(
            max_requests=1,
            max_bytes=0,
            estimated_bytes_per_key=100,
        )
        policy.enqueue("r0", ["a"])
        policy.advance(consumed=set(), canceled=set())
        policy.drain_events()

        policy.advance(consumed=set(), canceled=set(), completed={"r0"})

        retires = [line for line in policy.drain_events() if "slot retire" in line]
        assert len(retires) == 1
        assert "reason=complete" in retires[0]

    def test_completed_and_hold_do_not_retire_the_same_slot_twice(self):
        now = [0.0]
        policy = FifoStagePolicy(
            max_requests=1,
            max_bytes=0,
            estimated_bytes_per_key=100,
            hold_s=0.010,
            clock=lambda: now[0],
        )
        policy.enqueue("r0", ["a"])
        policy.advance(consumed=set(), canceled=set())
        policy.drain_events()
        now[0] += 0.020

        policy.advance(consumed=set(), canceled=set(), completed={"r0"})

        retires = [line for line in policy.drain_events() if "slot retire" in line]
        assert len(retires) == 1
        assert "reason=complete" in retires[0]

    def test_byte_window_blocks_younger_request(self):
        policy = FifoStagePolicy(
            max_requests=2,
            max_bytes=250,
            estimated_bytes_per_key=100,
        )
        policy.enqueue("r0", ["a", "b"])
        policy.enqueue("r1", ["c"])

        admitted = policy.advance(consumed=set(), canceled=set())

        assert [plan.req_id for plan in admitted] == ["r0"]
        assert policy.queued_requests == 1

    def test_oversized_oldest_runs_alone(self):
        policy = FifoStagePolicy(
            max_requests=2,
            max_bytes=100,
            estimated_bytes_per_key=100,
        )
        policy.enqueue("large", ["a", "b"])

        admitted = policy.advance(consumed=set(), canceled=set())

        assert [plan.req_id for plan in admitted] == ["large"]

    def test_consumed_matched_request_can_be_admitted_with_its_load(self):
        policy = FifoStagePolicy(
            max_requests=1,
            max_bytes=0,
            estimated_bytes_per_key=100,
        )
        policy.enqueue("r0", ["a"])

        admitted = policy.advance(consumed={"r0"}, canceled=set())

        assert [plan.req_id for plan in admitted] == ["r0"]
        assert policy.queued_requests == 0

    def test_consumed_request_blocked_by_budget_is_dropped(self):
        policy = FifoStagePolicy(
            max_requests=1,
            max_bytes=0,
            estimated_bytes_per_key=100,
        )
        policy.enqueue("r0", ["a"])
        policy.enqueue("r1", ["b"])

        admitted = policy.advance(consumed={"r0", "r1"}, canceled=set())

        assert [plan.req_id for plan in admitted] == ["r0"]
        assert policy.queued_requests == 0


class TestFifoStageHoldAndDelay:
    """The two knobs that separate draining the queue from timing the fill."""

    def test_default_holds_the_slot_until_the_turn_consumes_it(self):
        """Without hold_s the slot survives any amount of elapsed time."""
        now = [100.0]
        policy = FifoStagePolicy(
            max_requests=1,
            max_bytes=0,
            estimated_bytes_per_key=100,
            clock=lambda: now[0],
        )
        policy.enqueue("r0", ["a"])
        policy.enqueue("r1", ["b"])
        assert [p.req_id for p in policy.advance(consumed=set(), canceled=set())] == [
            "r0"
        ]

        now[0] += 60.0
        assert policy.advance(consumed=set(), canceled=set()) == []
        assert policy.queued_requests == 1

        assert [p.req_id for p in policy.advance(consumed={"r0"}, canceled=set())] == [
            "r1"
        ]

    def test_hold_returns_the_slot_without_waiting_for_consumption(self):
        """A held-out slot frees the next plan and is logged as reason=hold."""
        now = [100.0]
        policy = FifoStagePolicy(
            max_requests=1,
            max_bytes=0,
            estimated_bytes_per_key=100,
            hold_s=0.4,
            clock=lambda: now[0],
        )
        policy.enqueue("r0", ["a"])
        policy.enqueue("r1", ["b"])
        policy.advance(consumed=set(), canceled=set())
        policy.drain_events()

        now[0] += 0.3
        assert policy.advance(consumed=set(), canceled=set()) == []

        now[0] += 0.2
        admitted = policy.advance(consumed=set(), canceled=set())

        assert [p.req_id for p in admitted] == ["r1"]
        retires = [e for e in policy.drain_events() if "slot retire" in e]
        assert len(retires) == 1 and "reason=hold" in retires[0]

    def test_hold_leaves_a_consumed_slot_to_the_consumed_path(self):
        """Consumption in the same step wins, so the reason stays truthful."""
        now = [100.0]
        policy = FifoStagePolicy(
            max_requests=1,
            max_bytes=0,
            estimated_bytes_per_key=100,
            hold_s=0.4,
            clock=lambda: now[0],
        )
        policy.enqueue("r0", ["a"])
        policy.advance(consumed=set(), canceled=set())
        policy.drain_events()

        now[0] += 0.5
        policy.advance(consumed={"r0"}, canceled=set())

        retires = [e for e in policy.drain_events() if "slot retire" in e]
        assert len(retires) == 1 and "reason=consumed" in retires[0]

    def test_queue_delay_withholds_a_plan_until_its_delay_elapses(self):
        """The delay sets when the fill starts, and order is preserved."""
        now = [100.0]
        policy = FifoStagePolicy(
            max_requests=2,
            max_bytes=0,
            estimated_bytes_per_key=100,
            queue_delay_s=1.0,
            clock=lambda: now[0],
        )
        policy.enqueue("r0", ["a"])
        now[0] += 0.5
        policy.enqueue("r1", ["b"])

        assert policy.advance(consumed=set(), canceled=set()) == []

        now[0] += 0.5  # r0 는 1.0 초 지났고 r1 은 0.5 초다
        assert [p.req_id for p in policy.advance(consumed=set(), canceled=set())] == [
            "r0"
        ]

        now[0] += 0.5
        assert [p.req_id for p in policy.advance(consumed=set(), canceled=set())] == [
            "r1"
        ]

    def test_both_knobs_drain_the_queue_and_still_pace_the_fill(self):
        """Hold empties the window; delay decides how early each fill runs."""
        now = [100.0]
        policy = FifoStagePolicy(
            max_requests=1,
            max_bytes=0,
            estimated_bytes_per_key=100,
            hold_s=0.3,
            queue_delay_s=2.0,
            clock=lambda: now[0],
        )
        for i in range(3):
            policy.enqueue(f"r{i}", ["a"])

        assert policy.advance(consumed=set(), canceled=set()) == []
        assert policy.queued_requests == 3

        now[0] += 2.0
        admitted = []
        for _ in range(6):
            admitted += [
                p.req_id for p in policy.advance(consumed=set(), canceled=set())
            ]
            now[0] += 0.3

        assert admitted == ["r0", "r1", "r2"]
        assert policy.queued_requests == 0


class TestStageTicket:
    def test_ready_future_is_consumed_then_released(self):
        plan = StagePlan("r0", ("k0",), 4096)
        ticket = StageTicket(plan)
        future: Future[StageResult] = Future()
        ticket.bind(future)
        ticket.mark_running()
        future.set_result(_ready_result())

        assert ticket.state is StageState.READY
        assert ticket.wait() is not None
        assert ticket.state is StageState.CONSUMED
        ticket.release()
        assert ticket.state is StageState.RELEASED

    def test_partial_result_fails_open(self):
        plan = StagePlan("r0", ("k0",), 4096)
        ticket = StageTicket(plan)
        future: Future[StageResult] = Future()
        ticket.bind(future)
        ticket.mark_running()
        future.set_result(StageResult(requested_keys=1, found_keys=0))

        assert ticket.wait() is None
        assert ticket.state is StageState.FAILED

    def test_queued_future_can_be_canceled(self):
        ticket = StageTicket(StagePlan("r0", ("k0",), 4096))
        future: Future[StageResult] = Future()
        ticket.bind(future)

        assert ticket.cancel()
        assert ticket.state is StageState.CANCELED


class TestSchedulerStageRelay:
    def test_arrival_is_relayed_as_request_plan(self, monkeypatch):
        monkeypatch.setenv("MARU_STAGE_PIPELINE", "1")
        monkeypatch.delenv("MARU_ARRIVAL_HINT", raising=False)
        scheduler = MaruSchedulerConnector(
            block_size=4,
            kv_chunk_tokens=4,
            extra_config={},
        )
        request = SimpleNamespace(
            request_id="r0",
            prompt_token_ids=list(range(8)),
        )

        scheduler._count_matched_chunk_keys = lambda keys: 2
        matched, deferred = scheduler.get_num_new_matched_tokens(request, 0)
        metadata = scheduler.build_connector_meta(_empty_scheduler_output())

        assert matched == 8
        assert not deferred
        assert len(metadata.stage_plans) == 1
        assert metadata.stage_plans[0].req_id == "r0"
        assert len(metadata.stage_plans[0].keys) == 2
        assert metadata.arrival_hint_keys == []

    def test_stage_pipeline_supersedes_async_arrival_hint(self, monkeypatch):
        monkeypatch.setenv("MARU_STAGE_PIPELINE", "1")
        monkeypatch.setenv("MARU_ARRIVAL_HINT", "1")

        scheduler = MaruSchedulerConnector(
            block_size=4,
            kv_chunk_tokens=4,
            extra_config={},
        )

        assert scheduler._stage_enabled
        assert not scheduler._arrival_hint_enabled


class TestWorkerStageThread:
    def test_start_load_submits_without_blocking_engine_thread(self, monkeypatch):
        monkeypatch.setenv("MARU_STAGE_PIPELINE", "1")
        started = threading.Event()
        unblock = threading.Event()
        stage_thread_names: list[str] = []

        def stage_batch(keys):
            stage_thread_names.append(threading.current_thread().name)
            started.set()
            assert unblock.wait(timeout=2.0)
            return _ready_result(len(keys))

        worker = MaruWorkerConnector(block_size=4, kv_chunk_tokens=4, extra_config={})
        worker._handler = SimpleNamespace(stage_batch=stage_batch, close=lambda: None)
        context = SimpleNamespace(no_compile_layers={}, attn_metadata=None)
        metadata = MaruConnectorMetadata(stage_plans=[StagePlan("r0", ("k0",), 4096)])

        worker.start_load_kv(context, metadata)

        assert started.wait(timeout=1.0)
        assert stage_thread_names == ["maru-im-stage_0"]
        assert worker._stage_tickets["r0"].state is StageState.RUNNING
        unblock.set()
        assert worker._await_stage("r0") is not None
        worker._release_stage_ticket("r0")
        worker.shutdown()

    def test_failed_stage_falls_back_and_releases_ticket(self, monkeypatch):
        monkeypatch.setenv("MARU_STAGE_PIPELINE", "1")
        worker = MaruWorkerConnector(block_size=4, kv_chunk_tokens=4, extra_config={})
        worker._handler = SimpleNamespace(
            stage_batch=lambda keys: StageResult(
                requested_keys=len(keys),
                found_keys=0,
                error="miss",
            ),
            close=lambda: None,
        )
        context = SimpleNamespace(no_compile_layers={}, attn_metadata=None)
        metadata = MaruConnectorMetadata(stage_plans=[StagePlan("r0", ("k0",), 4096)])
        worker.start_load_kv(context, metadata)

        assert worker._await_stage("r0") is None
        assert "r0" not in worker._stage_tickets
        worker.shutdown()


class _StagePlugin:
    def __init__(self, result: StageResult):
        self.result = result
        self.calls: list[tuple[list[str], object]] = []

    def on_stage(self, handler, keys, batch_resp):
        self.calls.append((keys, batch_resp))
        return self.result


class TestHandlerStageBatch:
    def _handler(self) -> MaruHandler:
        handler = MaruHandler(MaruConfig(auto_connect=False))
        handler._connected = True
        handler._owned = MagicMock()
        handler._owned.is_owned.return_value = False
        handler._mapper = MagicMock()
        handler._mapper.get_region.return_value = object()
        handler._rpc = MagicMock()
        return handler

    def test_maps_lookup_and_returns_plugin_completion(self):
        handler = self._handler()
        response = SimpleNamespace(
            entries=[
                SimpleNamespace(
                    found=True,
                    handle=SimpleNamespace(region_id=7),
                )
            ]
        )
        handler._rpc.batch_lookup_kv.return_value = response
        result = _ready_result()
        plugin = _StagePlugin(result)
        handler._plugins = [plugin]

        assert handler.stage_batch(["k0"]) is result
        assert plugin.calls == [(["k0"], response)]

    def test_missing_stage_plugin_returns_failed_result(self):
        handler = self._handler()
        handler._rpc.batch_lookup_kv.return_value = SimpleNamespace(entries=[])
        handler._plugins = []

        result = handler.stage_batch(["k0"])

        assert not result.ready
        assert result.error == "no stage-capable handler plugin"


class TestObjectSpans:
    """The window's value is that Stage 1 overlaps Stage 2.

    Durations alone cannot show that overlap, so the pipeline stamps when each
    phase happened. These tests pin the stamps against a stage that sleeps a
    known amount, so a regression that drops or mis-orders them is caught here
    rather than in a campaign whose plots then cannot be drawn.
    """

    def _objects(self, count: int, nbytes: int = 16):
        from maru_vllm.staging_prefetch import HymCacheObject

        return [
            HymCacheObject(req_id="r1", index=i, key=f"k{i}", nbytes=nbytes)
            for i in range(count)
        ]

    def _run(self, count: int, window_bytes: int, stage_delay: float = 0.0):
        import time

        from maru_vllm.staging_prefetch import HymCacheRollingPipeline

        with ThreadPoolExecutor(max_workers=1) as executor:
            pipeline = HymCacheRollingPipeline(executor, window_bytes=window_bytes)

            def stage(obj):
                if stage_delay:
                    time.sleep(stage_delay)
                return _ready_result()

            return pipeline.run(
                [self._objects(count)],
                stage=stage,
                consume=lambda obj, result: None,
                release=lambda obj: None,
            )

    def test_stamps_are_ordered_within_each_object(self):
        for timing in self._run(4, window_bytes=32):
            assert timing.submitted_at <= timing.stage_started_at
            assert timing.stage_started_at <= timing.stage_completed_at
            assert timing.stage_completed_at <= timing.consume_started_at
            assert timing.consume_started_at <= timing.consume_completed_at

    def test_stage_duration_reflects_the_worker_not_the_queue(self):
        """queue_ms and stage_ms must not be the same number.

        A later object waits in the queue behind earlier ones; only its own
        stage call should land in stage_ms.
        """
        timings = self._run(4, window_bytes=32, stage_delay=0.02)
        for timing in timings:
            assert timing.stage_ms >= 18.0, timing.stage_ms
        # The last object queues behind the earlier ones, the first does not.
        assert timings[0].queue_ms < timings[-1].queue_ms

    def test_ready_age_is_the_lead_the_window_bought(self):
        """With a window of two, object 0 is consumed while object 1 stages."""
        timings = self._run(4, window_bytes=32, stage_delay=0.02)
        assert all(t.ready_age_ms >= 0.0 for t in timings)
        # Object 0 is consumed as soon as it is ready, so its lead is small;
        # later objects were staged well before their turn came.
        assert timings[0].ready_age_ms < timings[-1].ready_age_ms

    def test_a_failed_stage_releases_exactly_what_it_admitted(self):
        """A raising stage aborts the run without leaking or over-releasing.

        The window admits two 16-byte objects into a 32-byte budget; the third
        never gets a lease because the failure happens before any release frees
        room. So two releases is correct and three would mean releasing a lease
        that was never taken.
        """
        import pytest as _pytest

        from maru_vllm.staging_prefetch import HymCacheRollingPipeline

        released = []
        with ThreadPoolExecutor(max_workers=1) as executor:
            pipeline = HymCacheRollingPipeline(executor, window_bytes=32)

            def stage(obj):
                raise RuntimeError("boom")

            with _pytest.raises(RuntimeError):
                pipeline.run(
                    [self._objects(3)],
                    stage=stage,
                    consume=lambda obj, result: None,
                    release=released.append,
                )
        assert len(released) == 2


class TestIssueThenWait:
    """Admission splits into a non-blocking hint and a blocking readiness wait.

    Without ``issue`` the pipeline can only put one object into the device at a
    time, because admission *is* the blocking stage. These lock the ordering
    that lets window depth reach the device.
    """

    def _objects(self, count: int, nbytes: int = 16) -> list[HymCacheObject]:
        return [
            HymCacheObject(req_id="r", index=i, key=f"k{i}", nbytes=nbytes)
            for i in range(count)
        ]

    def test_issue_fires_before_the_object_is_staged(self) -> None:
        order: list[str] = []
        objects = self._objects(3)
        with ThreadPoolExecutor(max_workers=1) as pool:
            HymCacheRollingPipeline(pool, window_bytes=32).run(
                [objects],
                issue=lambda o: order.append(f"issue{o.index}"),
                stage=lambda o: order.append(f"stage{o.index}") or o.index,
                consume=lambda o, _v: order.append(f"consume{o.index}"),
                release=lambda o: None,
            )
        for i in range(3):
            assert order.index(f"issue{i}") < order.index(f"stage{i}")

    def test_whole_window_is_hinted_before_the_first_consume(self) -> None:
        """The point of the hint: object 1 is already moving while 0 is read."""
        order: list[str] = []
        objects = self._objects(4)
        with ThreadPoolExecutor(max_workers=1) as pool:
            HymCacheRollingPipeline(pool, window_bytes=32).run(
                [objects],
                issue=lambda o: order.append(f"issue{o.index}"),
                stage=lambda o: o.index,
                consume=lambda o, _v: order.append(f"consume{o.index}"),
                release=lambda o: None,
            )
        assert order.index("issue1") < order.index("consume0")

    def test_issue_count_matches_object_count(self) -> None:
        issued: list[int] = []
        objects = self._objects(6)
        with ThreadPoolExecutor(max_workers=1) as pool:
            HymCacheRollingPipeline(pool, window_bytes=32).run(
                [objects],
                issue=lambda o: issued.append(o.index),
                stage=lambda o: o.index,
                consume=lambda o, _v: None,
                release=lambda o: None,
            )
        assert issued == list(range(6))

    def test_window_still_bounds_how_far_the_hint_runs_ahead(self) -> None:
        """A hint outside the window would defeat the byte bound."""
        live: list[int] = []
        peak = 0
        objects = self._objects(6)

        def _issue(obj: HymCacheObject) -> None:
            nonlocal peak
            live.append(obj.index)
            peak = max(peak, len(live))

        def _consume(obj: HymCacheObject, _v: int) -> None:
            live.remove(obj.index)

        with ThreadPoolExecutor(max_workers=1) as pool:
            HymCacheRollingPipeline(pool, window_bytes=32).run(
                [objects],
                issue=_issue,
                stage=lambda o: o.index,
                consume=_consume,
                release=lambda o: None,
            )
        assert peak == 2  # window_bytes 32 / nbytes 16

    def test_absent_issue_keeps_the_previous_behaviour(self) -> None:
        """No hint: an object's only device contact is its blocking stage.

        Where a later object's stage lands relative to an earlier consume is a
        scheduling race, so only the per-object order and the consume order are
        asserted.
        """
        order: list[str] = []
        objects = self._objects(3)
        with ThreadPoolExecutor(max_workers=1) as pool:
            HymCacheRollingPipeline(pool, window_bytes=32).run(
                [objects],
                stage=lambda o: order.append(f"stage{o.index}") or o.index,
                consume=lambda o, _v: order.append(f"consume{o.index}"),
                release=lambda o: None,
            )
        assert [e for e in order if e.startswith("consume")] == [
            "consume0",
            "consume1",
            "consume2",
        ]
        for i in range(3):
            assert order.index(f"stage{i}") < order.index(f"consume{i}")
        assert not any(e.startswith("issue") for e in order)

    def test_a_failing_hint_does_not_stop_the_stream(self) -> None:
        """The blocking stage still brings the object in without the hint."""
        consumed: list[int] = []
        objects = self._objects(3)

        def _issue(obj: HymCacheObject) -> None:
            if obj.index == 1:
                raise RuntimeError("device busy")

        with ThreadPoolExecutor(max_workers=1) as pool:
            with pytest.raises(RuntimeError):
                HymCacheRollingPipeline(pool, window_bytes=32).run(
                    [objects],
                    issue=_issue,
                    stage=lambda o: o.index,
                    consume=lambda o, _v: consumed.append(o.index),
                    release=lambda o: None,
                )
