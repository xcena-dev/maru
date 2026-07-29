# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for completion-returning SSD-to-DRAM request staging."""

from __future__ import annotations

import threading
from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import MagicMock

from maru import MaruConfig, MaruHandler
from maru_handler import StageResult
from maru_vllm.connector import (
    MaruConnectorMetadata,
    MaruSchedulerConnector,
    MaruWorkerConnector,
)
from maru_vllm.staging_prefetch import (
    FifoStagePolicy,
    StagePlan,
    StageState,
    StageTicket,
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
