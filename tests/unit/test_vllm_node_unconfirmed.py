# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""The stop rules on the maru_node paths: a CUDA error from a copy or a wait
stops the worker and keeps what the copies use; once stopped nothing is given
back, published or reported; any other error is handled as before.

The node load paths (inline and loader-thread batches, parked loads, FETCHes
in flight) and store paths (WRITE_DONE, write-behind, chunks waiting for
credit) on fake handlers and streams; CPU only."""

import contextlib
import dataclasses
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")
import torch

import maru_vllm.connector as connector_module
from maru_vllm.connector import (
    MaruConnectorMetadata,
    _QueuedCopyError,
    _UnconfirmedCopyError,
)
from tests.unit.test_vllm_node import (
    UNCONFIRMED,
    BrokenWait,
    Done,
    FailedWait,
    Pending,
    external_meta,
    fetching_loader,
    forward_of,
    group_loader,
    is_kept,
    lease,
    page,
    park,
    park_group,
    parked_loader,
    pinning_worker,
    released,
    run,
    sentinel_kv,
    stopped,
    submit,
    three,
    wait_on,
    write_behind_worker,
)
from tests.unit.vllm_connector_helpers import cuda_error, holds


def inline_reader(reads: list):
    """A node worker whose inline loads read ``reads``."""
    worker = pinning_worker()
    worker._handler.retrieve_capacity.return_value = None
    worker._handler.batch_retrieve.side_effect = lambda keys, **_: reads[: len(keys)]
    worker._last_attn_metadata = MagicMock()
    return worker


def stop(worker) -> None:
    worker._stop_if_unconfirmed(cuda_error(), req_id="r0")


# =============================================================================
# Loads
# =============================================================================


def test_an_inline_node_copy_cuda_error_stops_and_keeps_its_reads():
    """The step's later loads are not read, and the reads are not given back."""
    reads = [lease("a"), lease("b")]
    worker = inline_reader(reads)
    worker._load_packed = MagicMock(side_effect=cuda_error())
    with pytest.raises(_UnconfirmedCopyError, match="req r1"):
        worker.start_load_kv(forward_of(sentinel_kv()), three(external_meta(False)))
    assert worker._handler.batch_retrieve.call_count == 1
    worker._handler.release_retrieved.assert_not_called()
    assert holds(worker, reads[0]) and not worker._failed_load_blocks
    assert stopped(worker, "req r1")


def test_an_inline_node_copy_failing_otherwise_recomputes_as_before():
    """Its stream drained: the reads go back and its external blocks are
    recomputed."""
    reads = [lease("a"), lease("b")]
    worker = inline_reader(reads)
    worker._load_packed = MagicMock(side_effect=RuntimeError("bad layout"))
    meta = MaruConnectorMetadata(requests=[external_meta(False)])
    worker.start_load_kv(forward_of(sentinel_kv()), meta)
    assert released(worker) == reads
    assert worker.take_failed_load_blocks() == {1, 2}
    assert worker._stopped is None


def test_a_loader_batch_copy_cuda_error_keeps_every_batch_still_copying(monkeypatch):
    """The first batch's copies may still run with the second's: both keep
    their reads, the load neither parks nor is reported."""
    worker, stream, log, reads = parked_loader(monkeypatch)
    copies = []

    def copy(layers, entries, attn, stream=None, wait=True):
        copies.append(entries[0][0].req_id)
        if len(copies) == 2:
            raise cuda_error()
        return set()

    worker._load_packed = copy
    park(worker, "r1")
    worker._handler.release_retrieved.assert_not_called()
    assert holds(worker, reads[0]) and holds(worker, reads[1])
    assert not worker._parked_loads and not worker._deferred_done
    assert stopped(worker, "req r1")


def test_a_loader_wait_for_room_that_raises_a_cuda_error_stops(monkeypatch):
    """One slot: the second batch waits for the first one's copies."""
    worker, stream, log, reads = parked_loader(monkeypatch, slots=1)

    def first_waits(event):
        event.wait_error = cuda_error()
        return event

    record = stream.record_event
    stream.record_event = lambda: first_waits(record())
    park(worker, "r1")
    assert log == ["read 0", "copy r1", "wait e0"]
    worker._handler.release_retrieved.assert_not_called()
    assert holds(worker, reads[0])
    assert not worker._deferred_done and not worker._failed_load_blocks
    assert stopped(worker, "req r1")


def test_an_answer_taken_once_stopped_is_kept_uncopied(monkeypatch):
    """A stop while the loader takes a FETCH answer: nothing of it is copied
    or given back, nor are the load's earlier batches."""
    worker, stream, log, reads, fetches = fetching_loader(monkeypatch)
    r1, r2 = submit(worker, "r1", "r2")
    run(worker, r1)
    answer = fetches[0].result

    def stopped_meanwhile():
        stop(worker)
        return answer()

    fetches[0].result = stopped_meanwhile
    run(worker, r2)
    assert "copy r1" not in log
    worker._handler.release_retrieved.assert_not_called()
    assert holds(worker, reads[0]) and holds(worker, reads[1])
    assert not worker._deferred_done and not worker._inflight_deferred_req_ids
    assert stopped(worker, "req r0")


def test_vllms_poll_once_stopped_gives_back_no_parked_batch(monkeypatch):
    """Even batches whose copies ended stay with the parked load."""
    worker, stream, log, reads = parked_loader(monkeypatch)
    park(worker, "r1")
    for event in stream.events:
        event.ended = True
    stop(worker)
    assert worker._settle_parked_loads() is False
    with pytest.raises(_UnconfirmedCopyError):
        worker.get_finished_loading()
    worker._handler.release_retrieved.assert_not_called()
    assert len(worker._parked_loads["r1"][1]) == 2


def test_a_parked_group_query_failing_otherwise_recomputes_it(monkeypatch):
    """Not a CUDA error: the load is recomputed by its id, its reads keep
    their slots, and the worker goes on."""
    worker, stream, log, reads = group_loader(monkeypatch)
    park_group(worker, "r1")
    stream.events[0].error = RuntimeError("query failed")
    assert worker.get_finished_loading() == {"r1"}
    assert worker.take_failed_recv_req_ids({"r1"}) == {"r1"}
    worker._handler.release_retrieved.assert_not_called()
    assert not worker._parked_loads and worker._stopped is None


def test_a_layerwise_drain_cuda_error_keeps_retained_and_queued_reads():
    """Blocks reclaimed while their layer copies run: the worker stops before
    vLLM reuses them, and keeps the reads never issued and those issued."""
    worker = pinning_worker()
    retained, queued = [lease("retained")], [lease("queued")]
    worker._deferred_layerwise_loads["r1"] = ("meta", 1, torch.zeros(1), retained)
    worker._deferred_layerwise_events["r2"] = {"layer": FailedWait()}
    worker._deferred_events["r2"] = Pending()
    worker._deferred_refs["r2"] = [queued]
    with pytest.raises(_UnconfirmedCopyError):
        worker._drain_layerwise_copies({"r1", "r2"}, report=True)
    worker._handler.release_retrieved.assert_not_called()
    assert holds(worker, retained[0]) and holds(worker, queued[0])
    assert not worker._deferred_done


@pytest.mark.parametrize("node", [True, False], ids=["maru_node", "cxl"])
def test_overlap_copies_that_cannot_be_queued_nor_drained(monkeypatch, node):
    """Not a CUDA error, and the stream cannot be waited for: on maru_node the
    load fails without giving the node's pages back; elsewhere the views are
    left for the resumed forward, as before."""
    worker = pinning_worker()
    worker._node_mode = worker._leased_mode = node
    monkeypatch.setattr(torch.cuda, "set_device", lambda device: None)
    monkeypatch.setattr(torch.cuda, "stream", lambda s: contextlib.nullcontext())
    worker._layerwise_stream_for = lambda device: BrokenWait()
    worker._pin_slot_mapping_for_async_h2d = lambda slots: slots
    worker._copy_chunkwise_layer_to_device = MagicMock(
        side_effect=RuntimeError("copy failed")
    )
    issue = lambda: worker._issue_layerwise_copies_offthread(  # noqa: E731
        external_meta(True, layerwise_load=True),
        2,
        torch.arange(8),
        [lease("a"), lease("b")],
        [("layer", torch.zeros(1), 0)],
        torch.device("cpu"),
    )
    if node:
        with pytest.raises(_QueuedCopyError):
            issue()
    else:
        assert issue() is None
    assert worker._stopped is None


def test_a_loader_job_whose_streams_cannot_be_drained_keeps_its_reads():
    """The overlap issue failed and a stream cannot be waited for: the load
    is recomputed and its reads are not given back."""
    reads = [lease("a"), lease("b")]
    worker = inline_reader(reads)
    worker._issue_layerwise_copies_offthread = MagicMock(side_effect=_QueuedCopyError)
    worker._layerwise_streams = [BrokenWait()]
    worker._inflight_deferred_req_ids.add("r1")
    meta = external_meta(True, layerwise_load=True)
    layers = [(f"model.layers.{i}.self_attn", None, i) for i in range(2)]
    worker._deferred_load_job(meta, layers, torch.device("cpu"))
    worker._handler.release_retrieved.assert_not_called()
    assert worker._deferred_done == {"r1"}
    assert worker.take_failed_load_blocks() == {1, 2}
    assert worker._stopped is None


def test_shutdown_once_stopped_keeps_parked_reads_and_the_node_handler(monkeypatch):
    worker, stream, log, reads = parked_loader(monkeypatch)
    park(worker, "r1")
    handler = worker._handler
    worker._node_handler = handler
    stop(worker)
    worker.shutdown()
    handler.release_retrieved.assert_not_called()
    handler.close.assert_not_called()
    assert worker._handler is handler and worker._node_handler is handler
    assert len(worker._parked_loads["r1"][1]) == 2 and is_kept(worker)


def test_read_leases_are_kept_once_stopped():
    worker = pinning_worker()
    read = lease("a")
    stop(worker)
    worker._release_reads([[read]])
    worker._handler.release_retrieved.assert_not_called()
    assert holds(worker, read)


# =============================================================================
# Stores
# =============================================================================


def test_a_publish_once_stopped_keeps_its_pages():
    """No WRITE_DONE: the pages are kept and no waiting request finishes."""
    worker = write_behind_worker()
    handle = page()
    wait_on(worker, "r", "k")
    stop(worker)
    worker._publish_on_node(["k"], [handle])
    worker._handler.store_async.assert_not_called()
    worker._handler.free.assert_not_called()
    assert holds(worker, handle) and "k" not in worker._pending_store_keys
    assert "r" in worker._request_pending_store_keys


def test_an_answer_after_a_stop_finishes_no_request_and_sends_no_marker():
    """MaruServer answers a store sent before the stop: its keys complete,
    but no request finishes and a layerwise chunk's marker is kept unsent."""
    worker = write_behind_worker()
    answer = connector_module.Future()
    worker._handler.store_async.return_value = answer
    marker, layer = page(), page()
    wait_on(worker, "r", "k_L0")
    worker._publish_chunk_layers("k", marker, {0: ("k_L0", layer)})
    stop(worker)
    answer.set_result([True])
    assert worker._handler.store_async.call_count == 1  # no marker
    assert holds(worker, marker)
    assert "r" in worker._request_pending_store_keys
    assert not worker._pending_store_keys


def test_a_write_behind_job_after_a_stop_publishes_nothing():
    """Its wait would succeed: still, nothing is published, freed or waited
    for credit, and the chunks waiting for it are kept."""
    worker = write_behind_worker()
    handle, slab = page(), torch.zeros(4)
    wait_on(worker, "r", "k", "d")
    stop(worker)
    deferred = [("d", slab, {})]
    worker._finish_write_behind_store(Done(), ["k"], [handle], [], deferred)
    worker._handler.store_async.assert_not_called()
    worker._handler.free.assert_not_called()
    assert worker._credit_executor is None
    assert holds(worker, handle) and holds(worker, slab)
    assert "r" in worker._request_pending_store_keys


def test_a_credit_wait_after_a_stop_allocates_nothing():
    worker = write_behind_worker()
    slab = torch.zeros(4)
    worker._deferred_bytes = 16
    stop(worker)
    deferred = [("d", slab, {})]
    worker._store_after_credit(worker._handler, deferred)
    worker._handler.alloc.assert_not_called()
    worker._handler.store_async.assert_not_called()
    assert deferred == [("d", slab, {})] and holds(worker, slab)
    assert worker._deferred_bytes == 16


def test_a_node_store_copy_cuda_error_stops_the_step(monkeypatch):
    """A packed store's copy to its page raises a CUDA error: the worker
    stops at once and keeps the page."""
    worker = pinning_worker()
    worker._num_layers = 2
    worker._handler.has_local.return_value = False
    handle = SimpleNamespace(buf=memoryview(bytearray(4096)))
    worker._handler.alloc.return_value = handle
    worker._packed_store_kernel_ctx = lambda attn: None
    monkeypatch.setattr(
        worker, "_extract_kv_from_layer", lambda *a: torch.zeros(2, 8, 1)
    )
    monkeypatch.setattr(torch.Tensor, "copy_", MagicMock(side_effect=cuda_error()))
    meta = dataclasses.replace(
        external_meta(False),
        block_ids=list(range(len(external_meta(False).block_ids) + 1)),
        is_store=True,
        num_scheduled_tokens=16,
        num_computed_tokens=0,
    )
    with pytest.raises(_UnconfirmedCopyError):
        worker.save_kv_layer(
            "model.layers.0.self_attn",
            torch.zeros(1),
            None,
            MaruConnectorMetadata(requests=[meta]),
        )
    worker._handler.free.assert_not_called()
    worker._handler.store_async.assert_not_called()
    monkeypatch.undo()
    assert stopped(worker, UNCONFIRMED)
