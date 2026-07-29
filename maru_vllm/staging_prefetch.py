# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Bounded request staging for InfiniteMemory-backed packed KV objects.

The scheduler-side policy is intentionally small: FIFO ordering plus explicit
request and byte budgets. The worker-side ticket turns the blocking
SSD-to-device-DRAM operation into an asynchronous preparation contract without
putting that latency on the vLLM engine thread.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass, field
from enum import StrEnum

from maru_handler import StageResult


@dataclass(frozen=True)
class StagePlan:
    """Scheduler-to-worker request staging command."""

    req_id: str
    keys: tuple[str, ...]
    estimated_bytes: int
    queued_at: float = field(default_factory=time.monotonic, compare=False)


class FifoStagePolicy:
    """Admit oldest requests under bounded request and byte windows."""

    def __init__(
        self,
        *,
        max_requests: int,
        max_bytes: int,
        estimated_bytes_per_key: int,
    ) -> None:
        if max_requests <= 0:
            raise ValueError("max_requests must be positive")
        if max_bytes < 0:
            raise ValueError("max_bytes must be non-negative")
        if estimated_bytes_per_key <= 0:
            raise ValueError("estimated_bytes_per_key must be positive")
        self._max_requests = max_requests
        self._max_bytes = max_bytes
        self._estimated_bytes_per_key = estimated_bytes_per_key
        self._queued: deque[StagePlan] = deque()
        self._queued_ids: set[str] = set()
        self._inflight: dict[str, StagePlan] = {}

    def enqueue(self, req_id: str, keys: list[str]) -> bool:
        """Queue one request once, returning whether it was accepted."""
        if not keys or req_id in self._queued_ids or req_id in self._inflight:
            return False
        plan = StagePlan(
            req_id=req_id,
            keys=tuple(keys),
            estimated_bytes=len(keys) * self._estimated_bytes_per_key,
        )
        self._queued.append(plan)
        self._queued_ids.add(req_id)
        return True

    def advance(
        self,
        *,
        consumed: set[str],
        canceled: set[str],
    ) -> list[StagePlan]:
        """Retire stale work and admit the oldest plans that fit.

        ``consumed`` retires a previously admitted window slot. A newly queued
        matched request may still be admitted in the same scheduler step as
        its deferred load; the load then joins its worker-side ticket. An
        oversized oldest request may run alone so FIFO does not deadlock
        permanently on a conservative byte estimate.
        """
        for req_id in consumed | canceled:
            self._inflight.pop(req_id, None)
        if canceled and self._queued:
            self._queued = deque(
                plan for plan in self._queued if plan.req_id not in canceled
            )
            self._queued_ids = {plan.req_id for plan in self._queued}

        admitted: list[StagePlan] = []
        inflight_bytes = sum(plan.estimated_bytes for plan in self._inflight.values())
        while self._queued and len(self._inflight) < self._max_requests:
            plan = self._queued[0]
            fits_bytes = (
                self._max_bytes == 0
                or inflight_bytes + plan.estimated_bytes <= self._max_bytes
            )
            if not fits_bytes and self._inflight:
                break
            self._queued.popleft()
            self._queued_ids.remove(plan.req_id)
            self._inflight[plan.req_id] = plan
            inflight_bytes += plan.estimated_bytes
            admitted.append(plan)
        # A matched request whose load metadata left in this scheduler step
        # cannot benefit from a plan relayed later. Keep the admitted subset
        # and discard only the consumed requests still stranded in the queue.
        if consumed and self._queued:
            self._queued = deque(
                plan for plan in self._queued if plan.req_id not in consumed
            )
            self._queued_ids = {plan.req_id for plan in self._queued}
        return admitted

    @property
    def queued_requests(self) -> int:
        """Number of requests waiting for a stage slot."""
        return len(self._queued)

    @property
    def inflight_requests(self) -> int:
        """Number of admitted requests not yet consumed or canceled."""
        return len(self._inflight)


class StageState(StrEnum):
    """Lifecycle of one worker-side stage command."""

    QUEUED = "queued"
    RUNNING = "running"
    READY = "ready"
    FAILED = "failed"
    CANCELED = "canceled"
    CONSUMED = "consumed"
    RELEASED = "released"


class StageTicket:
    """Thread-safe completion ticket for one blocking stage operation."""

    def __init__(self, plan: StagePlan) -> None:
        self.plan = plan
        self._lock = threading.Lock()
        self._done = threading.Event()
        self._future: Future[StageResult] | None = None
        self._state = StageState.QUEUED
        self._discard = False
        self._result: StageResult | None = None
        self._error: str | None = None
        self._started_at: float | None = None
        self._completed_at: float | None = None
        self._consumed_at: float | None = None

    def bind(self, future: Future[StageResult]) -> None:
        """Bind the executor future and arrange terminal-state publication."""
        with self._lock:
            if self._future is not None:
                raise RuntimeError("stage ticket already bound")
            self._future = future
        future.add_done_callback(self._finish)

    def mark_running(self) -> None:
        """Mark entry into the dedicated stage worker."""
        with self._lock:
            if self._state is StageState.QUEUED:
                self._state = StageState.RUNNING
                self._started_at = time.monotonic()

    def cancel(self) -> bool:
        """Cancel queued work or discard the result of already-running work."""
        with self._lock:
            if self._state in {
                StageState.READY,
                StageState.FAILED,
                StageState.CANCELED,
                StageState.CONSUMED,
                StageState.RELEASED,
            }:
                return False
            self._discard = True
            future = self._future
        if future is not None and future.cancel():
            return True
        return False

    def wait(self, timeout: float | None = None) -> StageResult | None:
        """Wait for readiness and consume a successful result."""
        if not self._done.wait(timeout):
            return None
        with self._lock:
            if self._state is not StageState.READY:
                return None
            self._state = StageState.CONSUMED
            self._consumed_at = time.monotonic()
            return self._result

    def release(self) -> None:
        """Drop the ticket after the dependent H2D transfer is safe."""
        with self._lock:
            self._state = StageState.RELEASED
        self._done.set()

    def _finish(self, future: Future[StageResult]) -> None:
        completed_at = time.monotonic()
        try:
            result = future.result()
            error = result.error
        except BaseException as exc:
            result = None
            error = f"{type(exc).__name__}: {exc}"
        with self._lock:
            self._completed_at = completed_at
            self._result = result
            self._error = error
            if future.cancelled() or self._discard:
                self._state = StageState.CANCELED
            elif result is not None and result.ready:
                self._state = StageState.READY
            else:
                self._state = StageState.FAILED
        self._done.set()

    @property
    def state(self) -> StageState:
        """Current lifecycle state."""
        with self._lock:
            return self._state

    @property
    def result(self) -> StageResult | None:
        """Preparation result, if the stage worker has returned."""
        with self._lock:
            return self._result

    @property
    def error(self) -> str | None:
        """Failure summary, if available."""
        with self._lock:
            return self._error

    @property
    def stage_ms(self) -> float | None:
        """Wall time spent inside the stage worker."""
        with self._lock:
            if self._started_at is None or self._completed_at is None:
                return None
            return (self._completed_at - self._started_at) * 1000.0

    @property
    def ready_age_ms(self) -> float | None:
        """Time readiness led demand consumption."""
        with self._lock:
            if self._completed_at is None or self._consumed_at is None:
                return None
            return (self._consumed_at - self._completed_at) * 1000.0
