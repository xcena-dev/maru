# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Best-effort request diagnostics; no diagnostic may fail a KV operation."""

from __future__ import annotations

import functools
import logging
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import torch


def _best_effort(fn):
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception:
            # stderr, CUDA diagnostics and allocation of trace records are
            # optional. In particular, never turn queued copies into a failure.
            return None

    return wrapped


class _StderrHandler(logging.StreamHandler):
    def emit(self, record):
        # Resolve at emit time, including under pytest's stderr capture.
        self.stream = sys.stderr
        super().emit(record)

    def handleError(self, record):  # noqa: N802 - logging.Handler override
        pass


_LOG = logging.getLogger("maru_vllm.trace")
_LOG.propagate = False
_LOG.setLevel(logging.INFO)
if not _LOG.handlers:
    _LOG.addHandler(_StderrHandler())


@_best_effort
def _write_stderr_line(line: str) -> None:
    # StreamHandler performs one locked write of the text and newline.
    _LOG.info("%s", line.replace("\r", "\\r").replace("\n", "\\n"))


def _emit_timing(msg: str) -> None:
    _write_stderr_line(f"Maru timing: {msg}")


def _emit_trace(line: str) -> None:
    _write_stderr_line(line)


def _mono_ms() -> float:
    return time.monotonic() * 1000.0


def _clock_fields() -> str:
    return f"mono={_mono_ms():.3f} wall={time.time():.6f}"


@_best_effort
def _trace_req(event: str, req_id: str, **fields: Any) -> None:
    extra = "".join(f" {key}={value}" for key, value in fields.items())
    _emit_trace(
        f"REQ-TRACE {event} req={quote(req_id, safe='-_.~')} {_clock_fields()} "
        f"req_encoding=percent{extra}"
    )


class _GpuClock:
    """Relate CUDA events to host time with an explicitly bracketed anchor.

    The bracket is calibration uncertainty, rather than a polling delay.
    A separate stream avoids waiting for previously queued model work.
    """

    def __init__(self, device):
        self.device = device
        stream = torch.cuda.Stream(device=device)
        samples = []
        for _ in range(3):
            ref = torch.cuda.Event(enable_timing=True)
            before = _mono_ms()
            ref.record(stream)
            ref.synchronize()
            after = _mono_ms()
            samples.append((after - before, (before + after) / 2, ref))
        width, self.mono, self.ref = min(samples, key=lambda sample: sample[0])
        self._bracket_error_ms = width / 2
        self.error_ms = self._bracket_error_ms

    def to_mono(self, event) -> float:
        elapsed = self.ref.elapsed_time(event)
        # CUDA returns a float millisecond interval. Include its rounding
        # scale, and periodically refresh the anchor in long-running workers.
        self.error_ms = max(
            self.error_ms, self._bracket_error_ms + abs(elapsed) * 2**-23 + 0.001
        )
        return float(self.mono + elapsed)


@dataclass
class _LoadTiming:
    req: str
    start: Any
    gate: Any
    end: Any
    nbytes: int
    gate_layer: int
    attempt: int


@dataclass
class _StepTiming:
    wstep: int
    step: int
    mono: float
    start: Any
    end: Any
    waits: list


@dataclass
class _CopyTiming:
    start: Any = None

    @_best_effort
    def begin(self, poller, stream):
        if self.start is None:
            self.start = poller.event(stream)


class _LayerLoadEvents(dict):
    """Completion events in enqueue order, with optional diagnostic state."""

    start: Any = None
    nbytes: int = 0


class _TracePoller:
    """Drain completed timing records from worker callbacks, without a thread.

    CUDA durations cover the entire KV-placement interval, including device
    idle gaps while the host enqueues work. GBps is effective throughput.
    The event clock, rather than the time poll() runs, supplies timestamps.
    """

    def __init__(self, rank: int = 0, tp_size: int = 1, dp_rank: int = 0, clock=None):
        self._rank = rank
        self._tp_size = tp_size
        self._dp_rank = dp_rank
        self._clock = clock
        self._lock = threading.Lock()
        self._loads: list[_LoadTiming] = []
        self._steps: list[_StepTiming] = []
        self._free_events: list[Any] = []
        self._closed = False

    @_best_effort
    def event(self, stream, *, reusable: bool = True):
        with self._lock:
            if self._closed:
                return None
            if self._clock is None:
                self._clock = _GpuClock(stream.device)
            event = (
                self._free_events.pop()
                if reusable and self._free_events
                else torch.cuda.Event(enable_timing=True)
            )
        event.record(stream)
        return event

    @_best_effort
    def add_load(self, req_id, start, gate, end, nbytes, gate_layer, *, attempt=0):
        if start is None or gate is None or end is None:
            return
        with self._lock:
            if not self._closed:
                self._loads.append(
                    _LoadTiming(req_id, start, gate, end, nbytes, gate_layer, attempt)
                )

    @_best_effort
    def add_step(self, wstep, mono, start, end, waits, *, step=0):
        self.add_forward(_StepTiming(wstep, step, mono, start, end, waits))

    @_best_effort
    def add_forward(self, record):
        if record.start is None or record.end is None:
            return
        with self._lock:
            if not self._closed:
                self._steps.append(record)

    def _recycle(self, events):
        self._free_events.extend(set(events))

    @_best_effort
    def poll(self):
        lines = []
        with self._lock:
            if self._clock is None:
                return
            if (
                isinstance(self._clock, _GpuClock)
                and _mono_ms() - self._clock.mono > 30_000
            ):
                try:
                    self._clock = _GpuClock(self._clock.device)
                except Exception:
                    pass  # The previous anchor remains usable.
            loads = []
            for load in self._loads:
                try:
                    if not load.end.query() or not load.gate.query():
                        loads.append(load)
                        continue
                    start = self._clock.to_mono(load.start)
                    end = self._clock.to_mono(load.end)
                    gate = self._clock.to_mono(load.gate)
                    dur = load.start.elapsed_time(load.end)
                    hold = load.start.elapsed_time(load.gate)
                    gbps = load.nbytes / (dur * 1e6) if dur > 0 else 0.0
                    lines.append(
                        f"GPU-LOAD-QOS bytes={load.nbytes} dur_ms={dur:.3f} "
                        f"GBps={gbps:.3f} req={quote(load.req, safe='-_.~')} "
                        f"gate_layer={load.gate_layer} hold_ms={hold:.3f} "
                        f"gate_mono={gate:.3f} end_mono={end:.3f} start_mono={start:.3f} "
                        f"attempt={load.attempt} tp_size={self._tp_size} "
                        f"req_encoding=percent clock_error_ms={self._clock.error_ms:.3f} "
                        f"scope=elapsed dp_rank={self._dp_rank} rank={self._rank}"
                    )
                except Exception:
                    pass
                # Gate/end also protect KV ownership and can still be used
                # by the engine after this diagnostic record is drained.
                self._recycle((load.start,))
            self._loads = loads
            steps = []
            for step in self._steps:
                try:
                    if not step.end.query():
                        steps.append(step)
                        continue
                    fwd = step.start.elapsed_time(step.end)
                    wait = sum(a.elapsed_time(b) for a, b in step.waits)
                    lines.append(
                        f"STEP-GPU wstep={step.wstep} mono={step.mono:.3f} "
                        f"fwd_ms={fwd:.3f} layer_wait_ms={wait:.3f} "
                        f"waits={len(step.waits)} end_mono={self._clock.to_mono(step.end):.3f} "
                        f"step={step.step} start_mono={self._clock.to_mono(step.start):.3f} "
                        f"clock_error_ms={self._clock.error_ms:.3f} "
                        f"dp_rank={self._dp_rank} rank={self._rank}"
                    )
                except Exception:
                    pass
                self._recycle(
                    (step.start, step.end, *(e for pair in step.waits for e in pair))
                )
            self._steps = steps
        for line in lines:
            _emit_trace(line)

    @_best_effort
    def shutdown(self):
        # Worker shutdown has drained its streams before reaching this hook.
        self.poll()
        with self._lock:
            self._closed = True
            self._loads.clear()
            self._steps.clear()
            self._free_events.clear()
