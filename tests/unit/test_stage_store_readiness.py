# SPDX-License-Identifier: Apache-2.0
"""Stage admission must join write-behind registration for its own keys."""

import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

from maru_handler import StageResult
from maru_vllm.connector import MaruWorkerConnector
from maru_vllm.staging_prefetch import StagePlan, StageTicket


def make_worker(monkeypatch, wait_ms=1000):
    monkeypatch.setenv("MARU_STAGE_STORE_WAIT_MS", str(wait_ms))
    worker = MaruWorkerConnector(
        block_size=4, kv_chunk_tokens=8, extra_config={"maru_async_store": True}
    )
    worker._handler = MagicMock()
    worker._handler.stage_batch.return_value = StageResult(
        requested_keys=2, found_keys=2, eligible_keys=2, issued_ranges=1
    )
    return worker


def ticket(*keys):
    return StageTicket(StagePlan(req_id="session", keys=keys, estimated_bytes=16))


def test_stage_joins_all_own_writes_before_lookup(monkeypatch):
    worker = make_worker(monkeypatch)
    worker._pending_store_keys.update({"a", "b", "unrelated"})
    sleeping = threading.Event()
    original_wait = worker._store_ready.wait

    def observe_wait(timeout=None):
        sleeping.set()
        return original_wait(timeout)

    monkeypatch.setattr(worker._store_ready, "wait", observe_wait)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(worker._run_stage, ticket("a", "b"))
        assert sleeping.wait(1)
        worker._handler.stage_batch.assert_not_called()
        sleeping.clear()
        worker._complete_write_behind_keys(["a"], [True])
        assert sleeping.wait(1)
        worker._handler.stage_batch.assert_not_called()
        worker._complete_write_behind_keys(["b"], [True])
        assert future.result(timeout=1).ready
    worker._handler.stage_batch.assert_called_once_with(["a", "b"])
    assert worker._pending_store_keys == {"unrelated"}


def test_unrelated_write_does_not_delay_stage(monkeypatch):
    worker = make_worker(monkeypatch, wait_ms=0)
    worker._pending_store_keys.add("other")
    assert worker._run_stage(ticket("a", "b")).ready
    worker._handler.stage_batch.assert_called_once_with(["a", "b"])


def test_pending_write_timeout_falls_back_without_partial_stage(monkeypatch):
    worker = make_worker(monkeypatch, wait_ms=0)
    worker._pending_store_keys.add("a")
    result = worker._run_stage(ticket("a", "b"))
    assert not result.ready
    assert result.error == "write-behind registration wait timed out"
    worker._handler.stage_batch.assert_not_called()
    assert worker._pending_store_keys == {"a"}


def test_failed_store_wakes_stage_and_preserves_lookup_failure(monkeypatch):
    worker = make_worker(monkeypatch)
    worker._pending_store_keys.add("a")
    sleeping = threading.Event()
    original_wait = worker._store_ready.wait

    def observe_wait(timeout=None):
        sleeping.set()
        return original_wait(timeout)

    monkeypatch.setattr(worker._store_ready, "wait", observe_wait)
    worker._handler.stage_batch.return_value = StageResult(
        requested_keys=1, found_keys=0
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(worker._run_stage, ticket("a"))
        assert sleeping.wait(1)
        worker._complete_write_behind_keys(["a"], [False])
        assert not future.result(timeout=1).ready
    assert "a" not in worker._stored_keys
