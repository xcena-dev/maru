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
from collections.abc import Callable, Sequence
from concurrent.futures import Executor, Future
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Generic, TypeVar

from maru_handler import StageResult

_StageValue = TypeVar("_StageValue")


@dataclass(frozen=True)
class HymCacheObject:
    """One ordered request-local KV object in a rolling prefetch window."""

    req_id: str
    index: int
    key: str
    nbytes: int


def build_hymcache_objects(
    req_id: str,
    keys: Sequence[str],
    key_sizes: Sequence[int],
) -> list[HymCacheObject]:
    """Build the ordered KV-object stream consumed by a rolling window.

    Args:
        req_id: Request identifier used only for tracing.
        keys: Ordered request-local KV object keys.
        key_sizes: Payload bytes corresponding one-to-one with ``keys``.

    Returns:
        One descriptor per input object, in the original request order.

    Raises:
        ValueError: If keys and sizes cannot define valid objects.
    """
    if len(keys) != len(key_sizes):
        raise ValueError("keys and key_sizes must have the same length")
    if any(size <= 0 for size in key_sizes):
        raise ValueError("key sizes must be positive")

    return [
        HymCacheObject(
            req_id=req_id,
            index=index,
            key=key,
            nbytes=size,
        )
        for index, (key, size) in enumerate(zip(keys, key_sizes, strict=True))
    ]


@dataclass(frozen=True)
class HymCacheObjectTiming:
    """When each stage of one pipeline object happened, and for how long.

    The four ``*_at`` values are ``time.monotonic()`` stamps, so a caller can
    lay Stage 1 and Stage 2 on one time axis and see where they overlap. That
    overlap is the whole point of the window, and durations alone cannot show
    it: two objects with identical stage and copy durations can be perfectly
    pipelined or fully serialized.

    ``ready_age_ms`` is how far Stage 1 finished ahead of the consumer -- the
    lead the window actually bought.
    """

    object: HymCacheObject
    demand_wait_ms: float
    consume_ms: float
    submitted_at: float = 0.0
    stage_started_at: float = 0.0
    stage_completed_at: float = 0.0
    consume_started_at: float = 0.0
    consume_completed_at: float = 0.0

    @property
    def stage_ms(self) -> float:
        """Wall time the stage worker spent on this object."""
        return (self.stage_completed_at - self.stage_started_at) * 1000.0

    @property
    def queue_ms(self) -> float:
        """Time the object waited in the worker queue before staging began."""
        return (self.stage_started_at - self.submitted_at) * 1000.0

    @property
    def ready_age_ms(self) -> float:
        """How long the object sat ready before its CXL->GPU copy started."""
        return (self.consume_started_at - self.stage_completed_at) * 1000.0


class HymCacheRollingPipeline(Generic[_StageValue]):
    """Run HyMCache's per-request byte-bounded rolling object window.

    Each request may have at most ``window_bytes`` scheduled for staging at a
    time (except that one oversized object is admitted alone). Objects are
    initially issued round-robin across requests. Once an object's CXL-to-GPU
    consumption completes, its release is queued before replacement objects
    from the same request, preserving the byte bound while keeping Stage 1
    ahead of Stage 2.
    """

    def __init__(self, executor: Executor, *, window_bytes: int) -> None:
        if window_bytes <= 0:
            raise ValueError("window_bytes must be positive")
        self._executor = executor
        self._window_bytes = window_bytes

    def run(
        self,
        requests: Sequence[Sequence[HymCacheObject]],
        *,
        stage: Callable[[HymCacheObject], _StageValue],
        consume: Callable[[HymCacheObject, _StageValue], None],
        release: Callable[[HymCacheObject], None],
        issue: Callable[[HymCacheObject], None] | None = None,
    ) -> list[HymCacheObjectTiming]:
        """Run all request streams and drain every release before returning.

        ``issue`` splits admission into HyMCache's two steps. Without it an
        object is admitted by submitting its blocking ``stage``, so only one
        object is ever in the device at a time no matter how deep the window
        is. With it, admission first fires a non-blocking hint for the object
        and ``stage`` becomes the readiness check on a fetch already under way,
        so window depth reaches the device. It is called on the caller's
        thread, before the object's stage is submitted, and must not block.
        """
        object_streams = [tuple(objects) for objects in requests if objects]
        if not object_streams:
            return []

        next_indices = [0] * len(object_streams)
        admitted_bytes = [0] * len(object_streams)
        pending: deque[tuple[int, HymCacheObject, Future[_StageValue], float]] = deque()
        release_futures: list[Future[None]] = []
        timings: list[HymCacheObjectTiming] = []
        # Stamped inside the worker so the stage lane can be drawn against the
        # consume lane; the future alone says nothing about when it ran.
        stage_spans: dict[tuple[str, int], tuple[float, float]] = {}

        def timed_stage(obj: HymCacheObject) -> _StageValue:
            started = time.monotonic()
            try:
                return stage(obj)
            finally:
                stage_spans[(obj.req_id, obj.index)] = (started, time.monotonic())

        def admit_one(request_index: int) -> bool:
            next_index = next_indices[request_index]
            objects = object_streams[request_index]
            if next_index >= len(objects):
                return False
            obj = objects[next_index]
            live_bytes = admitted_bytes[request_index]
            if live_bytes and live_bytes + obj.nbytes > self._window_bytes:
                return False
            if issue is not None:
                issue(obj)
            submitted_at = time.monotonic()
            pending.append(
                (
                    request_index,
                    obj,
                    self._executor.submit(timed_stage, obj),
                    submitted_at,
                )
            )
            next_indices[request_index] += 1
            admitted_bytes[request_index] += obj.nbytes
            return True

        # Fill every request's initial window in object-depth order rather
        # than letting the first request monopolize the device queue.
        admitted = True
        while admitted:
            admitted = False
            for request_index in range(len(object_streams)):
                admitted = admit_one(request_index) or admitted

        try:
            while pending:
                request_index, obj, future, submitted_at = pending.popleft()
                wait_t0 = time.monotonic()
                try:
                    result = future.result()
                except BaseException:
                    release_futures.append(self._executor.submit(release, obj))
                    raise
                demand_wait_ms = (time.monotonic() - wait_t0) * 1000.0

                consume_t0 = time.monotonic()
                try:
                    consume(obj, result)
                finally:
                    # Replacement stages are submitted only after this release.
                    # Previously admitted objects remain ahead in the queue and
                    # overlap the current CXL->GPU transfer.
                    release_futures.append(self._executor.submit(release, obj))
                    admitted_bytes[request_index] -= obj.nbytes
                    while admit_one(request_index):
                        pass
                consume_done = time.monotonic()
                stage_started, stage_completed = stage_spans.pop(
                    (obj.req_id, obj.index), (submitted_at, wait_t0)
                )
                timings.append(
                    HymCacheObjectTiming(
                        object=obj,
                        demand_wait_ms=demand_wait_ms,
                        consume_ms=(consume_done - consume_t0) * 1000.0,
                        submitted_at=submitted_at,
                        stage_started_at=stage_started,
                        stage_completed_at=stage_completed,
                        consume_started_at=consume_t0,
                        consume_completed_at=consume_done,
                    )
                )
        finally:
            # If a consumer failed, every already-issued stage still gets a
            # matching release. Objects not yet admitted acquired no lease.
            while pending:
                _, obj, future, _submitted = pending.popleft()
                try:
                    future.result()
                except BaseException:
                    pass
                release_futures.append(self._executor.submit(release, obj))
            for future in release_futures:
                future.result()
        return timings


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
        """Queue one request once, returning whether it was accepted.

        An id that is only INFLIGHT may re-queue: session-hint plan ids are
        keyed by session, and in turn_end mode the next turn's plan is queued
        at the moment the previous turn — whose stage may still hold the
        inflight slot — finishes. The old instance leaves via its release.
        """
        if not keys or req_id in self._queued_ids:
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
        released: set[str] = frozenset(),  # type: ignore[assignment]
    ) -> list[StagePlan]:
        """Retire stale work and admit the oldest plans that fit.

        ``consumed`` retires a previously admitted window slot. A newly queued
        matched request may still be admitted in the same scheduler step as
        its deferred load; the load then joins its worker-side ticket. An
        oversized oldest request may run alone so FIFO does not deadlock
        permanently on a conservative byte estimate.

        ``released`` frees an inflight slot WITHOUT touching queued plans.
        Session-hint plan ids are keyed by session, so when a request retires
        the stage it consumed, that same id may already hold the session's
        NEXT plan in the queue (queued at the request's completion in
        turn_end mode) — a full cancel would silently kill it.
        """
        for req_id in consumed | canceled | set(released):
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
