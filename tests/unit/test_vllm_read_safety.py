# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""CPU regressions for CXL load failure reporting and read-pin lifetimes."""

from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("torch")
import torch

from maru_handler.memory.pinned import PinnedViews
from maru_vllm.connector import MaruConnectorMetadata, MaruReqMeta, _chunk_keys
from tests.unit.vllm_connector_helpers import (
    attach_capturing_handler,
    fake_new_request_data,
    fake_scheduler_output,
    make_flash_attn_metadata,
    make_scheduler,
    make_worker,
    store_metadata,
)


def read_request(**kwargs):
    values = {
        "req_id": "read",
        "token_ids": list(range(13)),
        "block_ids": [1, 2, 3, 4],
        "is_store": False,
        "num_matched_chunks": 3,
        "load_start_token": 4,
        "load_end_token": 12,
    }
    values.update(kwargs)
    return MaruReqMeta(**values)


def pinned_worker(layerwise=False):
    worker = make_worker(4, 4, {"maru_use_layerwise": layerwise})
    worker._num_layers = 1
    worker._handler = MagicMock()
    worker._handler.supports_pinned_retrieve = True
    released = []

    def release():
        # RPC release must never execute under the non-reentrant load lock.
        assert worker._deferred_lock.acquire(blocking=False)
        worker._deferred_lock.release()
        released.append(True)

    return worker, released, release


def info(value):
    data = torch.full((2, 1, 4, 1), value, dtype=torch.float32)
    return SimpleNamespace(
        view=memoryview(bytearray(data.numpy().tobytes())), region_id=1, page_index=0
    )


def test_inline_partial_miss_preserves_gpu_prefix_and_compute_tail():
    worker, released, release = pinned_worker()
    worker._handler.batch_retrieve_pinned.return_value = PinnedViews(
        [info(10), info(20), None], release
    )
    cache = torch.full((2, 6, 4, 1), -99.0)
    context = SimpleNamespace(
        no_compile_layers={"model.layers.0.attn": SimpleNamespace(kv_cache=cache)},
        attn_metadata=make_flash_attn_metadata(),
    )
    worker.start_load_kv(context, MaruConnectorMetadata(requests=[read_request()]))
    assert torch.all(cache[:, 1] == -99)  # shared GPU prefix
    assert torch.all(cache[:, 2] == 20)  # copied contiguous external prefix
    assert torch.all(cache[:, 3] == -99)  # missing chunk
    assert torch.all(cache[:, 4] == -99)  # compute tail
    assert worker.take_failed_load_blocks() == {3}
    assert worker.get_finished_loading() is None
    assert released == [True]


@pytest.mark.parametrize("reply", [[], [None, None, None]])
def test_inline_truncation_and_miss_report_only_external_blocks(reply):
    worker, released, release = pinned_worker()
    worker._handler.batch_retrieve_pinned.return_value = PinnedViews(reply, release)
    context = SimpleNamespace(
        no_compile_layers={
            "model.layers.0.attn": SimpleNamespace(kv_cache=torch.zeros(2, 6, 4, 1))
        },
        attn_metadata=make_flash_attn_metadata(),
    )
    worker.start_load_kv(context, MaruConnectorMetadata(requests=[read_request()]))
    assert worker.take_failed_load_blocks() == {2, 3}
    assert released == [True]


def test_layerwise_common_prefix_retains_parent_lease():
    worker, released, release = pinned_worker(layerwise=True)
    values = [info(i) for i in range(6)]
    values[5] = None  # layer 1 loses chunk 2
    req = read_request()
    width, views = worker._available_load_prefix(
        req, 3, PinnedViews(values, release), 2
    )
    assert width == 2 and len(views) == 4
    assert [view.view for view in views] == [values[i].view for i in [0, 1, 3, 4]]
    assert req._failed_from_token == 8
    assert released == []
    views.release()
    assert released == [True]


@pytest.mark.parametrize("active", [False, True])
def test_async_views_remain_pinned_until_copy_event_finishes(active):
    worker, released, release = pinned_worker()
    event = MagicMock()
    event.query.return_value = False
    views = PinnedViews([info(1)], release)
    if active:
        worker._active_load_refs.append((event, [views]))
    else:
        worker._deferred_events["read"] = event
        worker._deferred_refs["read"] = [views]
    assert worker.get_finished_loading() is None
    assert released == []
    event.query.return_value = True
    assert worker.get_finished_loading() == (None if active else {"read"})
    assert released == [True]
    worker.get_finished_loading()
    assert released == [True]


def test_partial_deferred_failure_waits_for_prefix_copy():
    worker, released, release = pinned_worker()
    req = read_request(deferred_load=True)
    req._failed_from_token = 8
    event = MagicMock()
    event.query.return_value = False
    worker._deferred_events[req.req_id] = event
    worker._deferred_refs[req.req_id] = [PinnedViews([info(1)], release)]
    worker._deferred_failure_meta[req.req_id] = req
    assert worker.get_finished_loading() is None
    assert worker.take_failed_load_blocks() == set()
    event.query.return_value = True
    assert worker.get_finished_loading() == {req.req_id}
    assert worker.take_failed_load_blocks() == {3}
    assert released == [True]
    assert worker.get_finished_loading() is None


def test_abort_and_shutdown_drain_before_unpinning():
    worker, released, release = pinned_worker()
    stream = MagicMock()
    event = MagicMock()
    event.query.side_effect = lambda: stream.synchronize.called
    worker._layerwise_streams = [stream]
    worker._active_load_refs = [(event, [PinnedViews([info(1)], release)])]
    worker._handler.close.side_effect = lambda: released.append("closed")
    worker.shutdown()
    stream.synchronize.assert_called_once_with()
    assert released == [True, "closed"]

    worker, released, release = pinned_worker()
    worker._deferred_layerwise_loads["read"] = (
        read_request(),
        1,
        None,
        PinnedViews([info(1)], release),
    )
    worker._drain_layerwise_copies({"read"}, report=False)
    assert released == [True]


def test_failed_later_rpc_releases_already_pinned_batches():
    worker, released, release = pinned_worker()
    worker._handler.batch_retrieve_pinned.side_effect = [
        PinnedViews([info(1)], release),
        ConnectionError("timeout"),
    ]
    with pytest.raises(ConnectionError):
        worker._batch_retrieve_all(["a", "b"], batch_size=1)
    assert released == [True]


def test_deferred_submission_records_dependency_on_calling_stream(monkeypatch):
    worker, _, _ = pinned_worker()
    worker._kv_caches = {
        "model.layers.0.attn": SimpleNamespace(device=torch.device("cuda:0"))
    }
    worker._deferred_executor = MagicMock()
    caller_stream = object()
    event = MagicMock()
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)
    monkeypatch.setattr(torch.cuda, "current_stream", lambda device: caller_stream)
    monkeypatch.setattr(torch.cuda, "Event", lambda: event)
    req = read_request(deferred_load=True)
    assert worker._try_submit_deferred_load(req)
    event.record.assert_called_once_with(caller_stream)
    assert req._compute_ready is event
    worker._deferred_executor.submit.assert_called_once()


def test_layerwise_loader_waits_for_compute_before_copies(monkeypatch):
    worker, _, _ = pinned_worker()
    stream = MagicMock()
    ready = object()
    order = []
    req = read_request(deferred_load=True, _compute_ready=ready)
    stream.wait_event.side_effect = lambda event: order.append(("wait", event))
    worker._layerwise_stream_for = lambda device: stream
    worker._pin_slot_mapping_for_async_h2d = lambda slots: slots
    worker._copy_chunkwise_layer_to_device = lambda *args: order.append(("copy", None))
    worker._inject_kv_into_layer = MagicMock()
    monkeypatch.setattr(torch.cuda, "set_device", lambda device: None)
    monkeypatch.setattr(torch.cuda, "stream", lambda stream: nullcontext())
    monkeypatch.setattr(torch.cuda, "Event", MagicMock)
    events = worker._issue_layerwise_copies_offthread(
        req,
        1,
        torch.arange(4),
        [info(1)],
        [("model.layers.0.attn", torch.zeros(2, 6, 4, 1), 0)],
        torch.device("cpu"),
    )
    assert events is not None
    assert order == [("wait", ready), ("copy", None)]


@pytest.mark.parametrize("layerwise", [False, True])
def test_recomputed_key_is_stored_again_after_peer_deletion(layerwise):
    worker = make_worker(4, 4, {"maru_use_layerwise": layerwise})
    worker._num_layers = 1
    stored = attach_capturing_handler(worker)
    cache = torch.arange(2 * 6 * 4, dtype=torch.float32).reshape(2, 6, 4, 1)
    tokens = list(range(8))
    for attempt in range(2):
        meta = store_metadata(
            token_ids=tokens, block_ids=[1, 2], num_scheduled_tokens=8
        )
        worker.save_kv_layer(
            "model.layers.0.attn", cache, make_flash_attn_metadata(), meta
        )
        keys = _chunk_keys(tokens, 4)
        expected = {f"{key}_L0" for key in keys} if layerwise else set(keys)
        assert expected <= stored.keys()
        if attempt == 0:
            stored.clear()  # a peer removes directory entries
    assert worker._handler.batch_store.call_count == 2
    assert worker._handler.batch_exists.call_count == 2


def test_scheduler_propagates_external_interval_and_stops_repeat_failures():
    scheduler = make_scheduler(4, 4)
    scheduler._handler = MagicMock()
    scheduler._handler.batch_exists.return_value = [True] * 3
    request = SimpleNamespace(
        request_id="read", prompt_token_ids=list(range(13)), num_preemptions=0
    )
    assert scheduler.get_num_new_matched_tokens(request, 4) == (8, False)
    blocks = SimpleNamespace(get_block_ids=lambda: ([1, 2, 3, 4],))
    scheduler.update_state_after_alloc(request, blocks, 8)
    output = fake_scheduler_output(
        new_reqs=[fake_new_request_data(list(range(13)), [1, 2, 3, 4], req_id="read")]
    )
    metadata = scheduler.build_connector_meta(output)
    req = metadata.requests[0]
    assert (req.load_start_token, req.load_end_token) == (4, 12)
    assert scheduler.get_num_new_matched_tokens(request, 4) == (8, False)
    scheduler.update_state_after_alloc(request, blocks, 8)
    assert scheduler.get_num_new_matched_tokens(request, 4) == (0, False)
