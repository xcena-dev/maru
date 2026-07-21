# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for smart-prefetch arrival-hint (MARU_ARRIVAL_HINT).

Covers the three vendor-neutral pieces: the scheduler queues a request's chunk
keys on arrival, ``build_connector_meta`` relays them once, the worker fires
them through ``MaruHandler.prefetch_batch``, and ``prefetch_batch`` dispatches
the ``on_prefetch`` plugin seam. The device hint itself lives in an out-of-tree
plugin and is tested separately.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

from maru import MaruConfig, MaruHandler
from maru_vllm.connector import (
    MaruConnectorMetadata,
    MaruSchedulerConnector,
    MaruWorkerConnector,
)


def _request(token_ids: list[int], req_id: str = "r0") -> SimpleNamespace:
    """Duck-typed vLLM Request with the fields on_new_request reads."""
    return SimpleNamespace(prompt_token_ids=token_ids, request_id=req_id)


def _empty_scheduler_output() -> SimpleNamespace:
    """A scheduler_output with no scheduled requests (isolates the relay)."""
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


class TestSchedulerArrivalHint:
    """Scheduler-side queuing and relay of arrival-hint keys."""

    def _scheduler(self, monkeypatch, enabled: bool) -> MaruSchedulerConnector:
        if enabled:
            monkeypatch.setenv("MARU_ARRIVAL_HINT", "1")
        else:
            monkeypatch.delenv("MARU_ARRIVAL_HINT", raising=False)
        return MaruSchedulerConnector(block_size=4, kv_chunk_tokens=4, extra_config={})

    def test_enabled_queues_chunk_keys(self, monkeypatch):
        """A prompt spanning two chunks queues two chunk base keys."""
        sched = self._scheduler(monkeypatch, enabled=True)
        sched.on_new_request(_request(list(range(8))))  # 8 tokens / 4 = 2 chunks
        assert len(sched._pending_arrival_hint_keys) == 2

    def test_short_prompt_is_skipped(self, monkeypatch):
        """A prompt shorter than one chunk queues nothing."""
        sched = self._scheduler(monkeypatch, enabled=True)
        sched.on_new_request(_request([1, 2]))  # 2 < chunk_tokens (4)
        assert sched._pending_arrival_hint_keys == []

    def test_disabled_is_noop(self, monkeypatch):
        """With the hint disabled, arrival queues nothing."""
        sched = self._scheduler(monkeypatch, enabled=False)
        assert sched._arrival_hint_enabled is False
        sched.on_new_request(_request(list(range(8))))
        assert sched._pending_arrival_hint_keys == []

    def test_build_connector_meta_relays_and_clears(self, monkeypatch):
        """Queued keys land in the metadata once, then the queue is cleared."""
        sched = self._scheduler(monkeypatch, enabled=True)
        sched.on_new_request(_request(list(range(12))))  # 3 chunks
        queued = list(sched._pending_arrival_hint_keys)
        assert len(queued) == 3

        meta = sched.build_connector_meta(_empty_scheduler_output())
        assert isinstance(meta, MaruConnectorMetadata)
        assert meta.arrival_hint_keys == queued
        # Drained: a second build carries no stale keys.
        assert sched._pending_arrival_hint_keys == []
        meta2 = sched.build_connector_meta(_empty_scheduler_output())
        assert meta2.arrival_hint_keys == []


class TestWorkerArrivalHint:
    """Worker-side firing of relayed arrival-hint keys."""

    def _worker(self, enabled: bool) -> MaruWorkerConnector:
        worker = MaruWorkerConnector(block_size=4, kv_chunk_tokens=4, extra_config={})
        worker._arrival_hint_enabled = enabled
        worker._handler = MagicMock()
        return worker

    def test_fire_calls_prefetch_batch_with_chunk_keys(self):
        """Chunk keys are fired as-is (no per-layer expansion)."""
        worker = self._worker(enabled=True)
        worker._fire_arrival_hints(["c0", "c1", "c2"])
        worker._handler.prefetch_batch.assert_called_once_with(["c0", "c1", "c2"])

    def test_fire_swallows_handler_error(self):
        """A prefetch failure never propagates out of the fire path."""
        worker = self._worker(enabled=True)
        worker._handler.prefetch_batch.side_effect = RuntimeError("boom")
        worker._fire_arrival_hints(["c0"])  # must not raise

    def test_start_load_kv_fires_when_enabled(self):
        """start_load_kv fires the relayed keys before its load work."""
        worker = self._worker(enabled=True)
        fc = MagicMock()
        fc.no_compile_layers = {}  # -> load path returns after firing
        fc.attn_metadata = None
        meta = MaruConnectorMetadata(arrival_hint_keys=["k0", "k1"])
        worker.start_load_kv(fc, meta)
        worker._handler.prefetch_batch.assert_called_once_with(["k0", "k1"])

    def test_start_load_kv_noop_when_disabled(self):
        """With the hint disabled, no prefetch is fired even if keys arrive."""
        worker = self._worker(enabled=False)
        fc = MagicMock()
        fc.no_compile_layers = {}
        fc.attn_metadata = None
        meta = MaruConnectorMetadata(arrival_hint_keys=["k0", "k1"])
        worker.start_load_kv(fc, meta)
        worker._handler.prefetch_batch.assert_not_called()


class _RecordingPrefetchPlugin:
    """Records on_prefetch dispatches."""

    def __init__(self):
        self.batches: list[tuple[list[str], object]] = []

    def on_prefetch(self, handler, keys, batch_resp):
        self.batches.append((keys, batch_resp))


class TestHandlerPrefetchBatch:
    """MaruHandler.prefetch_batch: lookup-only + on_prefetch dispatch."""

    def _handler(self) -> MaruHandler:
        handler = MaruHandler(MaruConfig(auto_connect=False))
        handler._connected = True
        handler._owned = MagicMock()  # satisfy _ensure_connected
        handler._rpc = MagicMock()
        return handler

    def test_dispatches_on_prefetch_and_counts_found(self):
        """Looks up once, dispatches on_prefetch, returns the found count."""
        handler = self._handler()
        resp = SimpleNamespace(
            entries=[SimpleNamespace(found=True), SimpleNamespace(found=False)]
        )
        handler._rpc.batch_lookup_kv.return_value = resp
        recorder = _RecordingPrefetchPlugin()
        handler._plugins = [recorder]

        found = handler.prefetch_batch(["k0", "k1"])

        assert found == 1
        handler._rpc.batch_lookup_kv.assert_called_once_with(["k0", "k1"])
        assert recorder.batches == [(["k0", "k1"], resp)]

    def test_no_plugin_is_harmless(self):
        """With no plugin loaded, prefetch is a cheap lookup returning the count."""
        handler = self._handler()
        handler._rpc.batch_lookup_kv.return_value = SimpleNamespace(
            entries=[SimpleNamespace(found=True), SimpleNamespace(found=True)]
        )
        handler._plugins = []
        assert handler.prefetch_batch(["k0", "k1"]) == 2

    def test_rpc_failure_returns_zero_without_dispatch(self):
        """An RPC failure yields 0 and never dispatches to a plugin."""
        handler = self._handler()
        handler._rpc.batch_lookup_kv.side_effect = RuntimeError("rpc down")
        recorder = _RecordingPrefetchPlugin()
        handler._plugins = [recorder]
        assert handler.prefetch_batch(["k0"]) == 0
        assert recorder.batches == []
