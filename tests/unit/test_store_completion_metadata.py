# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Prompt-store completion must not release a still-generating request's blocks."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from maru_vllm import connector as mod
from tests.unit.vllm_connector_helpers import (
    make_scheduler,
    make_worker,
    store_metadata,
)


def adapter(worker=None, scheduler=None, workers=1):
    result = object.__new__(mod.MaruKVConnector)
    result._worker = worker
    result._scheduler = scheduler
    result._store_worker_count = workers
    return result


def reserve(worker, key, requests):
    worker._pending_store_keys.add(key)
    worker._store_key_waiters[key] = set(requests)
    for request in requests:
        worker._request_pending_store_keys.setdefault(request, set()).add(key)


def queue(worker, *, request="r1", tokens=16, computed=0, scheduled=16):
    meta = store_metadata(
        token_ids=list(range(tokens)),
        block_ids=[0, 1, 2, 3],
        num_scheduled_tokens=scheduled,
        num_computed_tokens=computed,
        req_id=request,
    )
    worker._queued_store_batches.append((MagicMock(), meta))


@pytest.fixture
def worker():
    result = make_worker(4, 8, extra_config={"maru_async_store": True})
    result._handler = MagicMock()
    result._store_packed_slabs_write_behind = MagicMock()
    return result


def test_prompt_store_releases_stage_while_decode_keeps_gpu_blocks(worker):
    """Exercise worker output and scheduler input while finished_req_ids is empty."""
    scheduler = make_scheduler(4, 8)
    scheduler._store_scheduled.add("r1")
    reserve(worker, "k1", ["r1"])
    queue(worker)
    source, target = adapter(worker=worker), adapter(scheduler=scheduler)
    assert source.get_finished(set()) == (None, None)
    assert source.build_connector_worker_meta() is None
    assert scheduler._store_pending("r1")

    worker._complete_write_behind_keys(["k1"], [True])
    metadata = source.build_connector_worker_meta()
    assert metadata is not None
    target.update_connector_output(
        SimpleNamespace(kv_connector_worker_meta=metadata, finished_sending=None)
    )
    assert not scheduler._store_pending("r1")
    assert source.get_finished(set()) == (None, None)
    assert source.build_connector_worker_meta() is None
    assert source.get_finished({"r1"}) == ({"r1"}, None)
    assert source.get_finished(set()) == (None, None)


@pytest.mark.parametrize("success", [True, False])
def test_store_drain_waits_for_every_key_and_does_not_claim_success(worker, success):
    queue(worker)
    reserve(worker, "k1", ["r1"])
    reserve(worker, "k2", ["r1"])
    source = adapter(worker=worker)
    source.get_finished(set())
    worker._complete_write_behind_keys(["k1"], [True])
    assert source.build_connector_worker_meta() is None
    worker._complete_write_behind_keys(["k2"], [success])
    assert source.build_connector_worker_meta().completed_workers == {"r1": {0}}
    assert ("k2" in worker._stored_keys) is success
    assert source.get_finished(set()) == (None, None)


def test_shared_pending_key_notifies_all_joining_requests(worker):
    queue(worker, request="r1")
    queue(worker, request="r2")
    reserve(worker, "shared", ["r1", "r2"])
    source = adapter(worker=worker)
    source.get_finished(set())
    assert source.build_connector_worker_meta() is None
    worker._complete_write_behind_keys(["shared"], [True])
    assert source.build_connector_worker_meta().completed_workers == {
        "r1": {0},
        "r2": {0},
    }


@pytest.mark.parametrize("tokens", [4, 16])
def test_no_new_store_keys_finishes_without_waiting_for_decode(worker, tokens):
    queue(worker, tokens=tokens, scheduled=tokens)
    source = adapter(worker=worker)
    source.get_finished(set())
    assert source.build_connector_worker_meta().completed_workers == {"r1": {0}}
    assert source.get_finished(set()) == (None, None)


def test_partial_prefill_has_no_completion_before_final_prompt_batch(worker):
    queue(worker, tokens=24, scheduled=8)
    source = adapter(worker=worker)
    source.get_finished(set())
    assert source.build_connector_worker_meta() is None
    queue(worker, tokens=24, computed=8, scheduled=16)
    source.get_finished(set())
    assert source.build_connector_worker_meta().completed_workers == {"r1": {0}}


def test_store_is_launched_before_completion_can_be_observed(worker):
    worker._store_packed_slabs_write_behind.side_effect = lambda *_: reserve(
        worker, "k1", ["r1"]
    )
    queue(worker)
    source = adapter(worker=worker)
    assert source.build_connector_worker_meta() is None
    source.get_finished(set())
    assert source.build_connector_worker_meta() is None
    worker._complete_write_behind_keys(["k1"], [True])
    assert source.build_connector_worker_meta().completed_workers == {"r1": {0}}


def test_completion_metadata_combines_distinct_ranks_without_mutating_inputs():
    left = mod.MaruStoreCompletionMetadata({"r1": {0}})
    right = mod.MaruStoreCompletionMetadata({"r1": {1}, "r2": {1}})
    merged = left.aggregate(right)
    assert merged.completed_workers == {"r1": {0, 1}, "r2": {1}}
    assert left.completed_workers == {"r1": {0}}
    assert right.completed_workers == {"r1": {1}, "r2": {1}}


def test_scheduler_waits_for_all_ranks_across_steps_and_ignores_duplicates():
    scheduler = make_scheduler(4, 8)
    scheduler._store_scheduled.add("r1")
    target = adapter(scheduler=scheduler, workers=2)
    for rank in [3, 3, 5]:
        target.update_connector_output(
            SimpleNamespace(
                kv_connector_worker_meta=mod.MaruStoreCompletionMetadata(
                    {"r1": {rank}}
                ),
                finished_sending=None,
            )
        )
        assert scheduler._store_pending("r1") is (rank != 5)


def test_retired_or_unknown_request_notice_is_ignored():
    scheduler = make_scheduler(4, 8)
    target = adapter(scheduler=scheduler, workers=2)
    target.update_connector_output(
        SimpleNamespace(
            kv_connector_worker_meta=mod.MaruStoreCompletionMetadata({"old": {0}}),
            finished_sending={"old"},
        )
    )
    assert scheduler._store_done == set()
    assert scheduler._store_completed_workers == {}


def test_legacy_finished_sending_still_releases_a_scheduled_request():
    scheduler = make_scheduler(4, 8)
    scheduler._store_scheduled.add("r1")
    adapter(scheduler=scheduler).update_connector_output(
        SimpleNamespace(finished_sending={"r1"})
    )
    assert not scheduler._store_pending("r1")
