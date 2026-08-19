# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for per-object GPU-read timing on the demand-load packed path.

The demand-load setting (W=0) had no per-object CUDA timing, so its GPU read
bandwidth could not be compared against the smart-prefetch setting. These
tests pin the two properties that make the added instrumentation usable:
it must not synchronize between objects, and it must produce exactly one
record per object with no gaps or repeats.
"""

from __future__ import annotations

from maru_vllm.connector import (
    PackedObjectTimer,
    _format_kv_object_timing,
    _slab_nbytes,
)


class FakeEvent:
    """CUDA event stand-in that counts synchronize calls.

    ``elapsed_time`` returns a value derived from the record order so a test
    can tell records apart without a GPU.
    """

    instances: list[FakeEvent] = []

    def __init__(self) -> None:
        self.recorded = False
        self.synchronize_calls = 0
        self.order = len(FakeEvent.instances)
        FakeEvent.instances.append(self)

    def record(self) -> None:
        self.recorded = True

    def synchronize(self) -> None:
        self.synchronize_calls += 1

    def elapsed_time(self, other: FakeEvent) -> float:
        return float(other.order - self.order)


def _fresh_factory():
    FakeEvent.instances = []
    return FakeEvent


def _run_objects(timer: PackedObjectTimer, req_id: str, count: int) -> None:
    """Drive the timer the way ``_load_packed`` does for one request."""
    timer.expect(req_id, count)
    for index in range(count):
        handle = timer.begin(req_id, index, count, 16 << 20)
        timer.end(handle)


def test_no_synchronize_between_objects():
    """Timing many objects must not drain the stream between them.

    A per-object synchronize would serialize the enqueue pipeline, so the
    instrumented run would stop measuring the uninstrumented one.
    """
    timer = PackedObjectTimer(event_factory=_fresh_factory())
    _run_objects(timer, "r1", 6)

    assert len(FakeEvent.instances) == 12
    assert all(event.recorded for event in FakeEvent.instances)
    assert all(event.synchronize_calls == 0 for event in FakeEvent.instances)

    timer.collect()
    assert all(event.synchronize_calls == 0 for event in FakeEvent.instances)


def test_every_object_recorded_once_after_final_sync():
    timer = PackedObjectTimer(event_factory=_fresh_factory())
    _run_objects(timer, "r1", 4)

    records, problems = timer.collect()

    assert problems == []
    assert [r.index for r in records] == [0, 1, 2, 3]
    assert {r.req_id for r in records} == {"r1"}
    assert all(r.total == 4 for r in records)
    assert all(r.nbytes == 16 << 20 for r in records)
    # Consecutive begin/end pairs are one event apart in record order.
    assert all(r.cxl_gpu_ms == 1.0 for r in records)


def test_multiple_requests_keep_their_own_object_indices():
    timer = PackedObjectTimer(event_factory=_fresh_factory())
    _run_objects(timer, "r1", 2)
    _run_objects(timer, "r2", 3)

    records, problems = timer.collect()

    assert problems == []
    assert sorted((r.req_id, r.index) for r in records) == [
        ("r1", 0),
        ("r1", 1),
        ("r2", 0),
        ("r2", 1),
        ("r2", 2),
    ]


def test_duplicate_object_record_is_reported():
    timer = PackedObjectTimer(event_factory=_fresh_factory())
    timer.expect("r1", 2)
    for index in (0, 1, 1):
        timer.end(timer.begin("r1", index, 2, 4096))

    records, problems = timer.collect()

    assert len(records) == 2
    assert any("duplicate" in p and "idx=1" in p for p in problems)


def test_missing_object_record_is_reported():
    timer = PackedObjectTimer(event_factory=_fresh_factory())
    timer.expect("r1", 3)
    timer.end(timer.begin("r1", 0, 3, 4096))

    records, problems = timer.collect()

    assert len(records) == 1
    assert any("incomplete" in p and "expected=3" in p for p in problems)


def test_record_format_is_shared_by_both_settings():
    """One parser must read the demand-load and the smart-prefetch record."""
    baseline = _format_kv_object_timing(
        req_id="r1",
        index=2,
        total=9,
        nbytes=16777216,
        cxl_gpu_ms=2.4567,
        prefetched=False,
    )
    smart = _format_kv_object_timing(
        req_id="r1",
        index=2,
        total=9,
        nbytes=16777216,
        cxl_gpu_ms=2.4567,
        prefetched=True,
    )

    assert baseline == (
        "kv-object idx=2/9 bytes=16777216 cxl_gpu_ms=2.46 prefetch=0 (req r1)"
    )
    assert smart.endswith("prefetch=1 (req r1)")
    assert baseline.split("prefetch=")[0] == smart.split("prefetch=")[0]


def test_slab_nbytes_handles_views_without_nbytes():
    assert _slab_nbytes(memoryview(bytearray(64))) == 64
    assert _slab_nbytes(bytearray(32)) == 32
