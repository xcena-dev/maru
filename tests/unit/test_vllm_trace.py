# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Trace failures must not affect KV ownership, and GPU clocks must join steps."""

import errno
import time
from types import SimpleNamespace
from unittest.mock import MagicMock
from urllib.parse import unquote

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import torch

from maru_vllm import trace
from maru_vllm.connector import MaruKVConnector, _CopyAhead
from tests.unit.vllm_connector_helpers import deferred_req_meta, make_worker


def request():
    return deferred_req_meta(
        token_ids=list(range(8)), block_ids=[1, 2], num_matched_chunks=1
    )


@pytest.mark.parametrize("error", [errno.EAGAIN, errno.EPIPE, errno.ENOSPC])
def test_broken_stderr_does_not_strand_a_deferred_request(monkeypatch, error):
    worker = make_worker(4, 8, {"maru_log_timing": True, "maru_async_load": True})
    worker._inflight_deferred_req_ids.add("r1")
    broken = MagicMock()
    broken.write.side_effect = OSError(error, "stderr unavailable")
    monkeypatch.setattr(trace.sys, "stderr", broken)

    worker._deferred_load_job(request(), [], torch.device("cpu"))

    assert worker.get_finished_loading() == {"r1"}
    assert worker.take_failed_load_blocks() == {1, 2}
    assert not worker._inflight_deferred_req_ids


def test_trace_backend_failure_preserves_pending_layerwise_copies(monkeypatch):
    worker = make_worker(
        4,
        8,
        {
            "maru_log_timing": True,
            "maru_async_load": True,
            "maru_overlap_load_with_compute": True,
        },
    )
    worker._handler = MagicMock()
    worker._num_layers = 2
    worker._handler.batch_retrieve.return_value = [
        SimpleNamespace(view=memoryview(bytearray(128)))
    ]
    events = trace._LayerLoadEvents({"l0": MagicMock(), "l1": MagicMock()})
    for event in events.values():
        event.query.return_value = False
    events.start, events.nbytes = MagicMock(), 128
    monkeypatch.setattr(worker, "_issue_layerwise_copies_offthread", lambda *a: events)
    monkeypatch.setattr(
        worker._trace_poller,
        "add_load",
        MagicMock(side_effect=RuntimeError("trace failed")),
    )
    req = request()
    req.layerwise_load = True

    worker._deferred_load_job(
        req,
        [("l0", torch.zeros(2, 2, 4, 1), 0), ("l1", torch.zeros(2, 2, 4, 1), 1)],
        torch.device("cpu"),
    )

    assert worker.get_finished_loading() is None
    assert not worker.take_failed_load_blocks()
    assert worker._deferred_layerwise_events["r1"] is events
    for event in events.values():
        event.query.return_value = True
    assert worker.get_finished_loading() == {"r1"}


def test_poller_failure_cannot_skip_handler_close(monkeypatch):
    worker = make_worker(4, 8, {"maru_log_timing": True})
    handler = worker._handler = MagicMock()
    monkeypatch.setattr(
        worker._trace_poller,
        "shutdown",
        MagicMock(side_effect=RuntimeError("trace failed")),
    )

    worker.shutdown()

    handler.close.assert_called_once()


def test_failed_loads_are_explicit_and_preserve_attempt(monkeypatch, capsys):
    worker = make_worker(4, 8, {"maru_log_timing": True})
    req = request()
    req.trace_attempt = 3
    worker._trace_attempts["r1"] = 3
    worker._fail_deferred_load(req)
    worker._fail_deferred_load(req)
    assert worker.get_finished_loading() == {"r1"}
    lines = capsys.readouterr().err.splitlines()
    failed = [line for line in lines if line.startswith("REQ-TRACE load_failed")]
    assert len(failed) == 1
    assert "attempt=3" in failed[0]
    recv = next(line for line in lines if line.startswith("REQ-TRACE recv_reported"))
    assert "success=0" in recv and "attempt=3" in recv


@pytest.mark.parametrize(
    "req_id",
    ["query-4 a", "query-4 b", "query\nGPU-LOAD-QOS forged=1", "query%20 literal"],
)
def test_request_ids_are_reversible_single_line_fields(capsys, req_id):
    trace._trace_req("lookup_hit", req_id)
    lines = capsys.readouterr().err.splitlines()
    assert len(lines) == 1
    fields = dict(part.split("=", 1) for part in lines[0].split() if "=" in part)
    assert fields["req_encoding"] == "percent"
    assert unquote(fields["req"]) == req_id


def test_v2_finish_forward_closes_once_and_keeps_scheduler_step(monkeypatch):
    worker = make_worker(4, 8, {"maru_log_timing": True})
    worker._trace_step = 77
    monkeypatch.setattr(worker._trace_poller, "event", lambda *a, **kw: MagicMock())
    monkeypatch.setattr(torch.cuda, "current_stream", lambda: MagicMock())
    connector = MaruKVConnector.__new__(MaruKVConnector)
    connector._worker = worker
    worker._trace_forward_start()

    connector.finish_forward()
    connector.get_finished(set())

    assert worker._trace_fwd is None
    assert len(worker._trace_poller._steps) == 1
    assert worker._trace_poller._steps[0].step == 77
    assert worker._trace_poller._steps[0].wstep == 1


def test_data_parallel_replica_is_identified(capsys):
    worker = make_worker(4, 8, {"maru_log_timing": True}, dp_rank=2)
    worker._trace_worker_req("job_start", "r1")
    assert "dp_rank=2" in capsys.readouterr().err


def test_bare_worker_can_wait_without_optional_trace_state(monkeypatch):
    from maru_vllm.connector import MaruWorkerConnector

    worker = MaruWorkerConnector.__new__(MaruWorkerConnector)
    event = MagicMock()
    worker._layer_load_events = {"layer": [event]}
    stream = MagicMock()
    monkeypatch.setattr(torch.cuda, "current_stream", lambda: stream)
    worker.wait_for_layer_load("layer")
    stream.wait_event.assert_called_once_with(event)


def test_finished_trace_state_is_bounded_but_pending_receives_keep_their_attempt():
    worker = make_worker(4, 8, {"maru_log_timing": True})
    worker._trace_attempts.update(done=2, pending=3)
    event = MagicMock()
    event.query.return_value = False
    worker._deferred_events["pending"] = event

    worker.get_finished_saving({"done", "pending"})
    assert worker._trace_attempts == {"pending": 3}
    event.query.return_value = True
    worker.get_finished_loading()
    assert not worker._trace_attempts


def test_completed_layer_copies_do_not_record_wait_timers(monkeypatch):
    worker = make_worker(4, 8, {"maru_log_timing": True})
    worker._trace_fwd = SimpleNamespace(waits=[])
    event = MagicMock()
    event.query.return_value = True
    worker._layer_load_events["layer"] = [event]
    stream = MagicMock()
    monkeypatch.setattr(torch.cuda, "current_stream", lambda: stream)
    timer = MagicMock()
    monkeypatch.setattr(worker._trace_poller, "event", timer)
    worker.wait_for_layer_load("layer")
    stream.wait_event.assert_called_once_with(event)
    timer.assert_not_called()


def test_completed_trace_does_not_recycle_kv_synchronization_events():
    clock = SimpleNamespace(to_mono=lambda event: 1.0, error_ms=0.0)
    poller = trace._TracePoller(clock=clock)
    start, gate, end = MagicMock(), MagicMock(), MagicMock()
    start.elapsed_time.return_value = 1.0
    poller.add_load("r1", start, gate, end, 1024, 1)
    poller.poll()
    assert poller._free_events == [start]
    assert gate not in poller._free_events and end not in poller._free_events


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires CUDA")
def test_copy_ahead_start_is_recorded_on_the_actual_copy_stream(monkeypatch):
    worker = make_worker(4, 8, {"maru_log_timing": True})
    consumer = torch.cuda.Stream()
    ahead = _CopyAhead(torch.device("cuda:0"), consumer, 2)
    # Warm up calibration before the deliberately busy consumer stream.
    worker._trace_poller.event(ahead.stream)
    slab = torch.zeros(2, 1, 8, 1, pin_memory=True)
    monkeypatch.setattr(worker, "_copy_ahead_for", lambda *args: ahead)
    monkeypatch.setattr(worker, "_inject_kv_into_layer", lambda *args, **kw: None)
    timing = trace._CopyTiming()
    old_start = torch.cuda.Event(enable_timing=True)
    slots = torch.arange(8, device="cuda")
    with torch.cuda.stream(consumer):
        torch.cuda._sleep(30_000_000)
        old_start.record(consumer)
    worker._inject_slab_layers(
        slab,
        torch.device("cuda:0"),
        consumer,
        [("l0", None, 0)],
        slots,
        None,
        True,
        trace=timing,
    )
    consumer.synchronize()
    assert timing.start is not None
    assert timing.start.elapsed_time(old_start) > 0
    worker._trace_poller.shutdown()


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires CUDA")
def test_gpu_clock_bracket_and_event_reuse():
    poller = trace._TracePoller()
    stream = torch.cuda.Stream()
    a = poller.event(stream)
    b = poller.event(stream)
    b.synchronize()
    before = trace._mono_ms()
    c = poller.event(stream)
    c.synchronize()
    after = trace._mono_ms()
    anchored = poller._clock.to_mono(c)
    assert (
        before - poller._clock.error_ms - 0.1
        <= anchored
        <= after + poller._clock.error_ms + 0.1
    )
    poller.add_step(1, before, a, b, [], step=12)
    time.sleep(0.01)
    poller.poll()
    recycled = poller.event(stream)
    assert recycled is a or recycled is b
    stream.synchronize()
    poller.shutdown()


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires CUDA")
def test_long_lived_worker_refreshes_the_gpu_clock():
    poller = trace._TracePoller()
    stream = torch.cuda.Stream()
    start, end = poller.event(stream), poller.event(stream)
    end.synchronize()
    old = poller._clock
    old.mono -= 31_000
    poller.add_step(1, trace._mono_ms(), start, end, [])
    poller.poll()
    assert poller._clock is not old
    assert not poller._steps
    poller.shutdown()
