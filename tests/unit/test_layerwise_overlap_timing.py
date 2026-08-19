# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for the layerwise-overlap timing seam and its mode exclusions.

Two things are pinned here. First, that a HyMCache byte window and layerwise
overlap are never both in effect: they consume the same bytes in opposite
orders (whole objects versus whole layers), so a config asking for both must
resolve to one. Second, that the overlap path emits both of its timelines —
per-layer transfer spans and per-layer compute stalls — because "the overlap
was enabled" and "the transfer was hidden" are different claims and only the
second one is a measurement.
"""

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

from maru_vllm.connector import MaruSchedulerConnector, MaruWorkerConnector

WINDOW = str(128 * 1024**2)


class _StubEvent:
    """Duck-typed torch.cuda.Event with a fixed timeline position."""

    def __init__(self, at_ms: float = 0.0, complete: bool = True) -> None:
        self._at_ms = at_ms
        self._complete = complete
        self.recorded_on: object = None

    def query(self) -> bool:
        return self._complete

    def record(self, stream=None) -> None:
        self.recorded_on = stream

    def elapsed_time(self, end: "_StubEvent") -> float:
        return end._at_ms - self._at_ms


class TestWindowSupersedesOverlap:
    """A byte window and layerwise overlap cannot both be in effect."""

    OVERLAP_CONFIG = {
        "maru_async_load": True,
        "maru_overlap_load_with_compute": True,
    }

    def test_scheduler_drops_overlap_when_window_is_set(self, monkeypatch):
        monkeypatch.setenv("MARU_HYMCACHE_WINDOW_BYTES", WINDOW)
        sched = MaruSchedulerConnector(
            block_size=16,
            kv_chunk_tokens=128,
            extra_config=dict(self.OVERLAP_CONFIG),
        )
        assert sched._hymcache_window_bytes == int(WINDOW)
        assert sched._layerwise_overlap is False

    def test_worker_drops_overlap_when_window_is_set(self, monkeypatch):
        monkeypatch.setenv("MARU_HYMCACHE_WINDOW_BYTES", WINDOW)
        worker = MaruWorkerConnector(
            block_size=16,
            kv_chunk_tokens=128,
            extra_config=dict(self.OVERLAP_CONFIG),
        )
        assert worker._hymcache_window_bytes == int(WINDOW)
        assert worker._layerwise_overlap is False
        worker.shutdown()

    def test_overlap_survives_without_a_window(self, monkeypatch):
        monkeypatch.delenv("MARU_HYMCACHE_WINDOW_BYTES", raising=False)
        sched = MaruSchedulerConnector(
            block_size=16,
            kv_chunk_tokens=128,
            extra_config=dict(self.OVERLAP_CONFIG),
        )
        worker = MaruWorkerConnector(
            block_size=16,
            kv_chunk_tokens=128,
            extra_config=dict(self.OVERLAP_CONFIG),
        )
        assert sched._layerwise_overlap is True
        assert worker._layerwise_overlap is True
        worker.shutdown()

    def test_window_declines_the_whole_request_loader(self, monkeypatch):
        """With a window set the request must take the window's own loop."""
        monkeypatch.setenv("MARU_HYMCACHE_WINDOW_BYTES", WINDOW)
        worker = MaruWorkerConnector(
            block_size=16,
            kv_chunk_tokens=128,
            extra_config=dict(self.OVERLAP_CONFIG),
        )
        worker._kv_caches = {"model.layers.0.self_attn": MagicMock()}
        worker._num_layers = 32
        assert worker._try_submit_deferred_packed_load(SimpleNamespace()) is False
        worker.shutdown()


def _timing_worker() -> MaruWorkerConnector:
    worker = MaruWorkerConnector(
        block_size=16,
        kv_chunk_tokens=128,
        extra_config={"maru_log_timing": True},
    )
    return worker


class TestLayerTransferSpans:
    """Per-layer transfer spans on the load stream, one axis per request."""

    def test_completed_request_emits_one_line_per_layer(self, capsys):
        worker = _timing_worker()
        epoch = _StubEvent(0.0)
        worker._layerwise_spans["r0"] = (
            _StubEvent(complete=True),
            epoch,
            [
                (0, _StubEvent(1.0), _StubEvent(3.5), 4096),
                (1, _StubEvent(3.5), _StubEvent(5.0), 4096),
            ],
        )

        worker._emit_layerwise_timing()

        lines = capsys.readouterr().err
        assert "kv-layer-transfer layer=0 start_ms=1.000 end_ms=3.500" in lines
        assert "kv-layer-transfer layer=1 start_ms=3.500 end_ms=5.000" in lines
        assert "(req r0)" in lines
        assert worker._layerwise_spans == {}
        worker.shutdown()

    def test_incomplete_request_is_kept_for_a_later_call(self, capsys):
        worker = _timing_worker()
        worker._layerwise_spans["r0"] = (
            _StubEvent(complete=False),
            _StubEvent(0.0),
            [(0, _StubEvent(1.0), _StubEvent(2.0), 4096)],
        )

        worker._emit_layerwise_timing()

        assert "kv-layer-transfer" not in capsys.readouterr().err
        assert "r0" in worker._layerwise_spans
        worker.shutdown()

    def test_unreadable_events_do_not_propagate(self, capsys):
        class _Broken(_StubEvent):
            def elapsed_time(self, end):
                raise RuntimeError("events not recorded")

        worker = _timing_worker()
        worker._layerwise_spans["r0"] = (
            _StubEvent(complete=True),
            _Broken(),
            [(0, _StubEvent(1.0), _StubEvent(2.0), 4096)],
        )

        worker._emit_layerwise_timing()

        assert "kv-layer-transfer" not in capsys.readouterr().err
        assert worker._layerwise_spans == {}
        worker.shutdown()


class TestLayerStallSpans:
    """Per-layer compute stalls, bracketed on the compute stream."""

    def _stub_stream(self, monkeypatch, worker):
        """Make wait_for_layer_load run without CUDA."""
        stream = SimpleNamespace(
            waited=[], wait_event=lambda e: stream.waited.append(e)
        )
        monkeypatch.setattr(
            "maru_vllm.connector.torch.cuda.current_stream", lambda *a, **k: stream
        )
        monkeypatch.setattr(
            "maru_vllm.connector.torch.cuda.Event",
            lambda *a, **k: _StubEvent(float(len(worker._layer_wait_spans) * 2)),
        )
        return stream

    def test_wait_brackets_the_layer_and_still_waits(self, monkeypatch):
        worker = _timing_worker()
        stream = self._stub_stream(monkeypatch, worker)
        load_event = object()
        worker._layer_load_events["model.layers.0.self_attn"] = [load_event]

        worker.wait_for_layer_load("model.layers.0.self_attn")

        assert stream.waited == [load_event]
        assert len(worker._layer_wait_spans) == 1
        assert worker._layer_wait_spans[0][0] == "model.layers.0.self_attn"
        worker.shutdown()

    def test_layer_without_load_events_records_nothing(self, monkeypatch):
        worker = _timing_worker()
        self._stub_stream(monkeypatch, worker)
        worker.wait_for_layer_load("model.layers.0.self_attn")
        assert worker._layer_wait_spans == []
        worker.shutdown()

    def test_emitted_stall_reports_the_gap_and_drains(self, capsys):
        worker = _timing_worker()
        worker._layer_wait_spans = [
            ("model.layers.3.self_attn", _StubEvent(10.0), _StubEvent(17.25)),
        ]

        worker._emit_layerwise_timing()

        assert "kv-layer-stall layer=3 stall_ms=7.250" in capsys.readouterr().err
        assert worker._layer_wait_spans == []
        worker.shutdown()

    def test_unfired_stall_is_kept(self, capsys):
        worker = _timing_worker()
        worker._layer_wait_spans = [
            ("model.layers.3.self_attn", _StubEvent(10.0), _StubEvent(17.0, False)),
        ]

        worker._emit_layerwise_timing()

        assert "kv-layer-stall" not in capsys.readouterr().err
        assert len(worker._layer_wait_spans) == 1
        worker.shutdown()

    def test_timing_off_records_no_spans(self, monkeypatch):
        worker = MaruWorkerConnector(
            block_size=16, kv_chunk_tokens=128, extra_config={}
        )
        stream = SimpleNamespace(
            waited=[], wait_event=lambda e: stream.waited.append(e)
        )
        monkeypatch.setattr(
            "maru_vllm.connector.torch.cuda.current_stream", lambda *a, **k: stream
        )
        load_event = object()
        worker._layer_load_events["model.layers.0.self_attn"] = [load_event]

        worker.wait_for_layer_load("model.layers.0.self_attn")

        assert stream.waited == [load_event]
        assert worker._layer_wait_spans == []
        worker.shutdown()


class TestSpanStateIsThreadGuarded:
    """The loader thread writes spans; the engine thread drains them."""

    def test_drain_takes_the_deferred_lock(self):
        worker = _timing_worker()
        assert isinstance(worker._deferred_lock, type(threading.Lock()))
        with worker._deferred_lock:
            # A drain attempt while the lock is held must block, proving the
            # drain is guarded rather than racing the loader thread.
            done = threading.Event()
            t = threading.Thread(
                target=lambda: (worker._emit_layerwise_timing(), done.set())
            )
            t.start()
            assert not done.wait(timeout=0.2)
        t.join(timeout=2.0)
        assert done.is_set()
        worker.shutdown()


class TestInlineHitAlsoStores:
    """An inline cache hit must still publish the suffix it computes.

    A conversation's turn N loads the prefix its earlier turns stored and
    computes the rest. If the load metadata suppressed the store metadata, that
    newly computed rest would never reach Maru and turn N+1 would match no
    deeper than turn 1 — the matched prefix could not grow. The asynchronous
    load path parks the request and emits its store on resume, so only the
    inline path can reach one step holding both.
    """

    CHUNK = 8

    def _sched(self) -> MaruSchedulerConnector:
        # Third Party
        from tests.unit.vllm_connector_helpers import make_scheduler

        return make_scheduler(block_size=4, kv_chunk_tokens=self.CHUNK)

    def _output(self, new_reqs, num_scheduled):
        # Third Party
        from tests.unit.vllm_connector_helpers import (
            fake_cached_reqs,
            fake_scheduler_output,
        )

        return fake_scheduler_output(
            new_reqs, fake_cached_reqs([], [], []), num_scheduled
        )

    def _new_req(self, token_ids, block_ids):
        # Third Party
        from tests.unit.vllm_connector_helpers import fake_new_request_data

        return fake_new_request_data(prompt_token_ids=token_ids, block_ids=block_ids)

    def test_hit_request_emits_both_load_and_store(self):
        sched = self._sched()
        token_ids = list(range(32))
        request = SimpleNamespace(prompt_token_ids=token_ids, request_id="r1")
        sched._requests_need_load["r1"] = (request, 2)

        meta = sched.build_connector_meta(
            self._output(
                [self._new_req(token_ids, [0, 1, 2, 3, 4, 5, 6, 7])], {"r1": 32}
            )
        )

        loads = [r for r in meta.requests if not r.is_store]
        stores = [r for r in meta.requests if r.is_store]
        assert [r.num_matched_chunks for r in loads] == [2]
        assert len(stores) == 1
        assert stores[0].num_scheduled_tokens == 32
        assert stores[0].token_ids == token_ids

    def test_partially_scheduled_hit_keeps_store_continuation(self):
        sched = self._sched()
        token_ids = list(range(64))
        request = SimpleNamespace(prompt_token_ids=token_ids, request_id="r1")
        sched._requests_need_load["r1"] = (request, 2)

        sched.build_connector_meta(
            self._output([self._new_req(token_ids, list(range(15)))], {"r1": 20})
        )

        assert "r1" in sched._requests_need_store
        assert sched._requests_need_store["r1"][0] == token_ids


class TestWhyTheOrdersCannotCompose:
    """The two halves that made a combined config a half-applied state.

    Kept as a test rather than a comment because the exclusion above is only
    justified by what happened without it: the window's loader declines the
    request, so the overlap's activation step finds nothing retained and warns
    once per request while the window quietly does the whole load. Both halves
    are asserted here so a later change that removes the exclusion has to
    confront them.
    """

    def test_the_window_loader_declines_the_request(self, monkeypatch):
        monkeypatch.setenv("MARU_HYMCACHE_WINDOW_BYTES", WINDOW)
        worker = MaruWorkerConnector(
            block_size=16, kv_chunk_tokens=128, extra_config={"maru_async_load": True}
        )
        worker._kv_caches = {"model.layers.0.self_attn": MagicMock()}
        worker._num_layers = 32
        assert worker._try_submit_deferred_packed_load(SimpleNamespace()) is False
        worker.shutdown()

    def test_activation_without_a_retained_load_warns_and_does_nothing(
        self, monkeypatch, caplog
    ):
        # Third Party
        import logging

        monkeypatch.delenv("MARU_HYMCACHE_WINDOW_BYTES", raising=False)
        worker = MaruWorkerConnector(
            block_size=16, kv_chunk_tokens=128, extra_config={}
        )
        with caplog.at_level(logging.WARNING):
            worker._schedule_deferred_packed_layerwise_loads([], {"r0"}, None)
        assert "missing retained load" in caplog.text
        assert "r0" in caplog.text
        worker.shutdown()
