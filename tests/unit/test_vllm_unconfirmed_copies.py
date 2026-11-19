# SPDX-License-Identifier: Apache-2.0
"""A CUDA error from a GPU copy or a wait stops the worker on every backend.

Such a copy's end is unknown: vLLM would compute into blocks a load copy may
still write, or reuse blocks a store copy may still read. The worker stops,
the next connector call raises, and from then on nothing is given back. Any
other error (out of memory, a rejected layout, a miss) is handled as before,
and a load that fails after some copies were queued is reported only once
they ended.
"""

import contextlib
import dataclasses
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")
import torch

import maru_vllm.connector as connector_module
from maru_vllm.connector import (
    _KEPT_UNTIL_EXIT,
    MaruConnectorMetadata,
    MaruKVConnector,
    MaruReqMeta,
    _SlabTarget,
    _unconfirmed,
    _UnconfirmedCopyError,
)
from maru_vllm.trace import _LoadTiming
from tests.unit.test_vllm_cpu_config import extra as cpu_extra
from tests.unit.test_vllm_cpu_config import mixed_extra
from tests.unit.test_vllm_remote_cxl_config import extra as remote_cxl_extra
from tests.unit.vllm_connector_helpers import (
    cuda_error,
    holds,
    kept_by,
    make_flash_attn_metadata,
    make_worker,
)

BLOCK = 4
CHUNK = 8
PROMPT = 16
LAYER = "model.layers.0.self_attn"
UNCONFIRMED = "could not be confirmed finished"

BACKENDS = {
    "cxl": dict,
    "cpu": cpu_extra,
    "mixed": mixed_extra,
    "remote_cxl": remote_cxl_extra,
}


class Done:
    """An event or stream whose copies have ended."""

    def query(self):
        return True

    def synchronize(self):
        pass


class Pending(Done):
    def query(self):
        return False


class FailedWait(Done):
    def synchronize(self):
        raise cuda_error()


class FailedQuery(Done):
    def query(self):
        raise cuda_error()


class Drains(Done):
    """A stream whose waits are noted in ``log``."""

    def __init__(self, log: list):
        self.log = log

    def synchronize(self):
        self.log.append("drain")


class FailsOnce(Done):
    """Its first wait (or query) raises a CUDA error; later ones succeed."""

    def __init__(self, query=False):
        self.waits = 0
        self.fail_query = query

    def query(self):
        if self.fail_query:
            self.fail_query = False
            raise cuda_error()
        return False

    def synchronize(self):
        self.waits += 1
        if self.waits == 1:
            raise cuda_error()


def worker_of(backend: str, **extra):
    worker = make_worker(BLOCK, CHUNK, {**BACKENDS[backend](), **extra})
    worker._handler = MagicMock()
    worker._handler.retrieve_capacity.return_value = None
    worker._handler.storage_gpu_accessible.return_value = False
    return worker


def views(keys) -> list:
    return [SimpleNamespace(view=memoryview(bytearray(64))) for _ in keys]


def load_meta(**kwargs) -> MaruReqMeta:
    fields = {
        "req_id": "r1",
        "token_ids": list(range(PROMPT)),
        "block_ids": list(range(PROMPT // BLOCK)),
        "is_store": False,
        "num_matched_chunks": PROMPT // CHUNK,
        "load_start_token": 0,
        "load_end_token": PROMPT,
    }
    return MaruReqMeta(**{**fields, **kwargs})


def group_meta(**kwargs) -> MaruReqMeta:
    return load_meta(block_ids=[1, 2], group_block_ids=[[1, 2]], **kwargs)


def three(meta: MaruReqMeta) -> MaruConnectorMetadata:
    return MaruConnectorMetadata(
        requests=[dataclasses.replace(meta, req_id=r) for r in ("r1", "r2", "r3")]
    )


def forward() -> SimpleNamespace:
    return SimpleNamespace(
        no_compile_layers={LAYER: SimpleNamespace(kv_cache=torch.zeros(4, 2, 4, 1, 1))},
        attn_metadata=make_flash_attn_metadata(),
    )


def stop_at_r1(worker, calls: list):
    """A load that stops the worker at r1 and notes every request it gets."""

    def load(*args, **kwargs):
        req_meta = next(a for a in args if isinstance(a, MaruReqMeta))
        calls.append(req_meta.req_id)
        if req_meta.req_id == "r1":
            worker._stop_if_unconfirmed(cuda_error(), req_id="r1")

    return load


def failing_slots(error=None) -> MagicMock:
    """A slot mapping whose copy to the device fails."""
    slots = MagicMock()
    slots.to.side_effect = error or RuntimeError("copy failed")
    return slots


def fake_cuda(monkeypatch, worker, stream) -> torch.device:
    """Have ``worker`` load on ``stream`` as if on a CUDA device."""
    device = torch.device("cuda")
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)
    monkeypatch.setattr(torch.cuda, "current_stream", lambda device=None: Done())
    monkeypatch.setattr(torch.cuda, "stream", lambda s: contextlib.nullcontext())
    monkeypatch.setattr(torch.cuda, "set_device", lambda device: None)
    monkeypatch.setattr(worker, "_pin_slot_mapping_for_async_h2d", lambda m: m)
    stream.wait_stream = lambda other: None
    worker._load_stream, worker._load_stream_device = stream, device
    return device


def is_kept(worker) -> bool:
    """Whether ``worker`` (and so all of its state) stays until exit."""
    return any(owner is worker for owner, _ in _KEPT_UNTIL_EXIT)


def stopped(worker, match: str = UNCONFIRMED) -> bool:
    """Every connector call vLLM makes next raises, so vLLM never computes
    into blocks the copy may still write, nor issues new copies; and the
    worker with all its state is kept."""
    empty = MaruConnectorMetadata()
    calls = (
        lambda: worker.start_load_kv(None, empty),
        lambda: worker.handle_preemptions(empty),
        worker.get_finished_loading,
        worker.take_failed_load_blocks,
        lambda: worker.take_failed_recv_req_ids({"r1"}),
        lambda: worker.get_finished_saving({"r1"}),
        lambda: worker.wait_for_layer_load(LAYER),
        lambda: worker.save_kv_layer(LAYER, torch.zeros(1), None, empty),
        lambda: worker.wait_for_save(empty),
    )
    for call in calls:
        with pytest.raises(_UnconfirmedCopyError, match=match):
            call()
    return is_kept(worker)


@pytest.mark.parametrize(
    ("error", "unconfirmed"),
    [
        (cuda_error(), True),
        (RuntimeError("CUDA error: an illegal memory access"), True),
        (_UnconfirmedCopyError("GPU copies could not be confirmed finished"), True),
        (torch.OutOfMemoryError("CUDA error: out of memory"), False),
        (RuntimeError("Unsupported EngineKVFormat: 9"), False),
        (MemoryError("no page"), False),
    ],
    ids=["accelerator", "message", "stopped", "oom", "check", "alloc"],
)
def test_only_a_cuda_error_leaves_copies_unconfirmed(error, unconfirmed):
    assert _unconfirmed(error) is unconfirmed


# =============================================================================
# A CUDA error from a wait or a query stops the worker
# =============================================================================


@pytest.mark.parametrize("parked", [True, False], ids=["parked", "layerwise"])
def test_copies_that_cannot_be_asked_about_stop_the_worker(parked):
    worker = worker_of("cxl")
    held = views("a")
    if parked:
        worker._deferred_events["r1"] = FailedQuery()
        worker._deferred_refs["r1"] = [held]
        with pytest.raises(_UnconfirmedCopyError):
            worker.get_finished_loading()
        assert worker._deferred_refs["r1"] == [held]
    else:
        worker._active_load_refs = [(FailedQuery(), [held])]
        with pytest.raises(_UnconfirmedCopyError):
            worker.start_load_kv(forward(), MaruConnectorMetadata())
        assert worker._active_load_refs[0][1] == [held]
    worker._write_behind = True
    worker._queued_store_batches = [("kernel", MaruConnectorMetadata())]
    worker._store_packed_slabs_write_behind = MagicMock()
    assert stopped(worker, "req r1" if parked else UNCONFIRMED)
    # Nor does a stopped worker issue the stores the step queued
    worker._store_packed_slabs_write_behind.assert_not_called()


def test_an_undrained_copy_into_reclaimed_blocks_stops_the_worker():
    """A request aborted while its layers load: its blocks go back to vLLM
    only once its copies are known to have ended."""
    worker = worker_of("cxl")
    worker._layerwise_overlap = True
    held = views("a")
    worker._deferred_layerwise_events["r1"] = {LAYER: FailedWait()}
    worker._deferred_events["r1"] = Pending()
    worker._deferred_refs["r1"] = [held]
    with pytest.raises(_UnconfirmedCopyError, match="req r1"):
        worker.handle_preemptions(MaruConnectorMetadata(finished_req_ids={"r1"}))
    assert "r1" not in worker._deferred_done
    assert holds(worker, held[0])
    assert stopped(worker, "req r1")


def test_a_group_batch_wait_that_fails_once_stops_the_worker(monkeypatch):
    """A later wait that succeeds does not make the reads go back."""
    worker = worker_of("cpu")
    stream = FailsOnce()
    reads = views("a")
    monkeypatch.setattr(
        worker, "_group_load_plan", lambda m: (1, [(None, 0, None)], ["a"])
    )
    monkeypatch.setattr(worker, "_batch_retrieve_all", lambda keys, **r: reads)
    monkeypatch.setattr(worker, "_place_group_objects", lambda *a, **k: None)
    loaded = worker._load_group_leased(
        group_meta(), torch.device("cpu"), stream, offthread=True
    )
    assert loaded is None and stream.waits == 1
    worker._handler.release_retrieved.assert_not_called()
    assert not worker._failed_load_blocks and not worker._failed_recv_req_ids
    assert holds(worker, reads[0])
    assert stopped(worker, "req r1")


@pytest.mark.parametrize("fails", ["query", "wait"])
def test_an_admission_wait_that_fails_once_stops_the_worker(monkeypatch, fails):
    """The admitted load queues nothing and is not reported; the parked load
    waited for keeps its reads."""
    worker = worker_of("cxl")
    worker._load_admission_window = 1
    worker._deferred_stream = Done()
    worker._deferred_stream_device = torch.device("cpu")
    worker._inject_slab_layers = MagicMock()
    worker._inflight_deferred_req_ids.add("r1")
    held = views("o")
    worker._deferred_events["other"] = FailsOnce(query=fails == "query")
    worker._deferred_refs["other"] = held
    monkeypatch.setattr(torch.cuda, "set_device", lambda device: None)
    monkeypatch.setattr(worker, "_pin_slot_mapping_for_async_h2d", lambda slots: slots)
    worker._batch_retrieve_all = lambda keys, **route: views(keys)
    worker._deferred_load_job(
        load_meta(deferred_load=True), [(LAYER, torch.zeros(1), 0)], torch.device("cpu")
    )
    worker._inject_slab_layers.assert_not_called()
    assert "r1" not in worker._deferred_events and not worker._deferred_done
    assert not worker._failed_load_blocks
    assert worker._deferred_refs["other"] is held
    assert stopped(worker)


def test_vllms_poll_then_admission_on_parked_loads():
    """In turn: a query that succeeds may give back its load's reads; one
    that fails stops the worker, and nothing is given back after."""
    worker = worker_of("cxl")
    worker._load_admission_window = 1
    ended, unknown = views("e"), views("u")
    worker._deferred_events.update(ended=Done(), unknown=FailedQuery())
    worker._deferred_refs.update(ended=[ended], unknown=[unknown])
    # vLLM's poll asks first: the ended load goes, the other one stops it
    with pytest.raises(_UnconfirmedCopyError):
        worker.get_finished_loading()
    assert "ended" not in worker._deferred_refs
    assert worker._deferred_refs["unknown"] == [unknown]
    worker._wait_for_load_admission("r1")  # the loader thread then finds it
    assert worker._deferred_refs["unknown"] == [unknown]
    assert stopped(worker)


@pytest.mark.parametrize("fails", ["query", "wait"])
def test_admission_stops_before_vllms_poll_can_report_its_load(fails):
    """The loader's query or wait fails while vLLM's poll waits for the lock:
    the poll raises instead of reporting the load, though its own query would
    succeed."""
    worker = worker_of("cxl")
    worker._load_admission_window = 1
    held = views("a")
    stopping, polled = threading.Event(), threading.Event()
    loader = threading.Thread(target=worker._wait_for_load_admission, args=("r2",))

    class Racing(Done):
        def query(self):
            if threading.current_thread() is not loader:
                return True
            if fails == "query":
                raise cuda_error()
            return False

        def synchronize(self):
            raise cuda_error()

    worker._deferred_events["r1"] = Racing()
    worker._deferred_refs["r1"] = [held]
    stop = worker._stop_if_unconfirmed

    def stop_after_the_poll(*args, **kwargs):
        stopping.set()
        polled.wait(0.5)  # the poll goes first if the lock lets it
        return stop(*args, **kwargs)

    worker._stop_if_unconfirmed = stop_after_the_poll
    outcome = []

    def poll():
        try:
            outcome.append(worker.get_finished_loading())
        except _UnconfirmedCopyError:
            outcome.append("raised")
        polled.set()

    loader.start()
    assert stopping.wait(5)
    poller = threading.Thread(target=poll)
    poller.start()
    loader.join(5)
    poller.join(5)
    assert outcome == ["raised"]
    assert worker._deferred_refs["r1"] == [held] and holds(worker, held[0])
    assert stopped(worker, "req r1")


# =============================================================================
# A CUDA error from a copy call stops the worker, whatever a later drain says
# =============================================================================


@pytest.mark.parametrize("error", ["cuda", "other"])
@pytest.mark.parametrize("drained", [True, False])
def test_an_overlap_issue_failing_stops_on_a_cuda_error(monkeypatch, error, drained):
    """A CUDA error from the first copy stops the worker even if the stream
    then drains; another error waits for the copies queued before it and
    fails the load (under TP), unless that wait raises a CUDA error."""
    worker = worker_of("cxl")
    worker._state_shard = dataclasses.replace(worker._state_shard, parts=2)
    stream = Done() if drained else FailedWait()
    monkeypatch.setattr(torch.cuda, "set_device", lambda device: None)
    monkeypatch.setattr(torch.cuda, "stream", lambda s: contextlib.nullcontext())
    monkeypatch.setattr(worker, "_layerwise_stream_for", lambda device: stream)
    worker._pin_slot_mapping_for_async_h2d = lambda m: failing_slots(
        cuda_error() if error == "cuda" else None
    )
    worker._batch_retrieve_all = lambda keys, **route: views(keys)
    worker._inflight_deferred_req_ids.add("r1")
    meta = load_meta(deferred_load=True, layerwise_load=True)
    worker._deferred_load_job(meta, [(LAYER, None, 0)], torch.device("cpu"))
    if error == "other" and drained:
        assert worker._deferred_done == {"r1"} and worker._failed_load_blocks
        assert worker._stopped is None
    else:
        assert not worker._deferred_done and not worker._failed_load_blocks
        assert stopped(worker, "req r1")


def test_a_slot_mapping_that_cannot_be_pinned_fails_as_on_main(monkeypatch):
    """Pinning is a host allocation before the load's first copy: even a CUDA
    error from it fails the load (under TP) without stopping the worker."""
    worker = worker_of("cxl")
    worker._state_shard = dataclasses.replace(worker._state_shard, parts=2)
    monkeypatch.setattr(torch.cuda, "set_device", lambda device: None)
    monkeypatch.setattr(worker, "_layerwise_stream_for", lambda device: Done())
    monkeypatch.setattr(torch.Tensor, "is_pinned", lambda self: False)

    def pin_memory(self, *args, **kwargs):
        raise cuda_error("out of memory")

    monkeypatch.setattr(torch.Tensor, "pin_memory", pin_memory)
    worker._batch_retrieve_all = lambda keys, **route: views(keys)
    worker._inflight_deferred_req_ids.add("r1")
    meta = load_meta(deferred_load=True, layerwise_load=True)
    worker._deferred_load_job(meta, [(LAYER, None, 0)], torch.device("cpu"))
    assert worker._deferred_done == {"r1"}
    assert worker.take_failed_load_blocks() == set(meta.block_ids)
    assert worker._stopped is None


@pytest.mark.parametrize("error", ["cuda", "other"])
@pytest.mark.parametrize("drained", [True, False])
def test_an_overlap_activation_failing_stops_on_a_cuda_error(
    monkeypatch, error, drained
):
    worker = worker_of("cxl")
    device = fake_cuda(monkeypatch, worker, Done() if drained else FailedWait())
    monkeypatch.setattr(torch.cuda, "Stream", MagicMock())  # not created again
    meta = load_meta(deferred_load=True, layerwise_load=True)
    slots = failing_slots(cuda_error() if error == "cuda" else None)
    worker._deferred_layerwise_loads["r1"] = (meta, 2, slots, views("ab"))
    layers = [(LAYER, SimpleNamespace(device=device), 0)]
    worker._schedule_deferred_layerwise_loads(layers, {"r1"}, None)
    if error == "other" and drained:
        assert worker.take_failed_load_blocks() == set(meta.block_ids)
    else:
        assert not worker._failed_load_blocks
        assert stopped(worker, "req r1")


@pytest.mark.parametrize("error", ["cuda", "other"])
@pytest.mark.parametrize("drained", [True, False])
def test_a_failed_parked_load_is_reported_only_once_drained(
    monkeypatch, error, drained
):
    """The layerwise-format parked loads of the step's load stream."""
    worker = worker_of("cxl")
    log: list = []
    device = fake_cuda(monkeypatch, worker, Drains(log) if drained else FailedWait())
    report = worker._fail_load
    worker._fail_load = lambda meta, e: log.append("report") or report(meta, e)
    meta = load_meta(deferred_load=True)
    slots = failing_slots(cuda_error() if error == "cuda" else None)
    layers = [(LAYER, SimpleNamespace(device=device), 0)]
    worker._schedule_deferred_loads(layers, [(meta, 2, slots, [])])
    if error == "other" and drained:
        assert log == ["drain", "report"]
        assert worker.take_failed_load_blocks() == set(meta.block_ids)
        assert worker._deferred_done == {"r1"}
    else:
        assert not worker._failed_load_blocks and not worker._deferred_done
        assert stopped(worker, "req r1")


@pytest.mark.parametrize("drained", [True, False])
def test_an_overlap_load_failing_after_its_copies_drains_them_first(drained):
    """On cxl too: a parked load that fails once its layer copies are queued
    is reported only after them."""
    worker = worker_of("cxl")
    worker._batch_retrieve_all = lambda keys, **route: views(keys)
    worker._issue_layerwise_copies_offthread = lambda *a: {LAYER: Done()}
    worker._unpark_gate_event = MagicMock(side_effect=RuntimeError("no gate"))
    worker._layerwise_streams = [Done() if drained else FailedWait()]
    worker._inflight_deferred_req_ids.add("r1")
    meta = load_meta(deferred_load=True, layerwise_load=True)
    worker._deferred_load_job(meta, [(LAYER, None, 0)], torch.device("cpu"))
    if drained:
        assert worker._deferred_done == {"r1"} and worker._failed_load_blocks
    else:
        assert not worker._deferred_done and not worker._failed_load_blocks
        assert stopped(worker, "req r1")


@pytest.mark.parametrize("error", ["cuda", "other"])
@pytest.mark.parametrize("drained", [True, False])
def test_a_failed_group_placement_is_reported_only_once_drained(
    monkeypatch, error, drained
):
    """Its first objects may be copying: vLLM recomputes into the blocks only
    once they ended, never after a CUDA error from the placement."""
    worker = worker_of("cxl")
    log: list = []
    device = fake_cuda(monkeypatch, worker, Drains(log) if drained else FailedWait())
    report = worker._fail_load
    worker._fail_load = lambda meta, e: log.append("report") or report(meta, e)
    worker._group_caches = [SimpleNamespace(tensors=[SimpleNamespace(device=device)])]
    read = views("a")[0]
    worker._retrieve_group_objects = lambda meta: (1, [(None, 0, None, read)])
    worker._place_group_objects = MagicMock(
        side_effect=cuda_error() if error == "cuda" else RuntimeError("place failed")
    )
    metadata = MaruConnectorMetadata(requests=[group_meta()])
    if drained:
        worker._start_group_loads(metadata)
    else:
        with pytest.raises(_UnconfirmedCopyError, match="req r1"):
            worker._start_group_loads(metadata)
    if error == "other" and drained:
        assert log == ["drain", "report"]
        assert worker.take_failed_load_blocks() == {1, 2}
    else:
        assert not worker._failed_load_blocks
        assert holds(worker, read)
        assert stopped(worker, "req r1")


@pytest.mark.parametrize("backend", ["cpu", "remote_cxl"])
def test_a_group_batch_whose_copies_cannot_be_waited_for_keeps_its_reads(
    monkeypatch, backend
):
    """One slot: the first of two batches waits for its copies."""
    worker = worker_of(backend)
    worker._handler.retrieve_capacity.return_value = 1
    plan = [(None, 0, None), (None, 1, None)]
    monkeypatch.setattr(worker, "_group_load_plan", lambda m: (1, plan, ["a", "b"]))
    monkeypatch.setattr(worker, "_batch_retrieve_all", lambda keys, **r: views(keys))
    monkeypatch.setattr(worker, "_place_group_objects", lambda *a, **k: None)
    loaded = worker._load_group_leased(
        group_meta(), torch.device("cpu"), FailedWait(), offthread=True
    )
    assert loaded is None
    worker._handler.release_retrieved.assert_not_called()
    assert not worker._failed_load_blocks and not worker._failed_recv_req_ids
    assert kept_by(worker)
    assert stopped(worker, "req r1")


@pytest.mark.parametrize("drained", [True, False])
def test_a_failed_parked_group_load_is_recomputed_only_once_drained(
    monkeypatch, drained
):
    """A load that failed before its copies, or whose copies ended, is a plain
    recompute; one whose copies cannot be waited for stops the worker."""
    worker = worker_of("cpu")
    monkeypatch.setattr(torch.cuda, "set_device", lambda device: None)
    monkeypatch.setattr(torch.cuda, "stream", lambda s: contextlib.nullcontext())
    worker._deferred_stream = Done() if drained else FailedWait()
    worker._deferred_stream.wait_event = lambda event: None
    worker._deferred_stream_device = torch.device("cpu")
    worker._load_group_leased = MagicMock(side_effect=RuntimeError("copy failed"))
    meta = group_meta(deferred_load=True)
    worker._group_load_job(meta, torch.device("cpu"), Done())
    if drained:
        assert worker._failed_recv_req_ids == {"r1"}
        assert worker._deferred_done == {"r1"}
        assert worker._stopped is None
    else:
        assert not worker._failed_recv_req_ids and not worker._deferred_done
        assert stopped(worker, "req r1")


# =============================================================================
# Load copies on the current stream: cpu and mixed without a kernel
# =============================================================================


def session_worker(backend: str):
    """A cpu or mixed worker whose one-layer cache takes real injects."""
    worker = make_worker(BLOCK, CHUNK, BACKENDS[backend](), num_kv_heads=1, head_size=1)
    cache = torch.full((8, 2, 4, 1, 1), -1.0)
    worker._kv_layout = worker._resolve_kv_layout({"layer": cache})
    worker._handler = MagicMock()
    worker._handler.storage_gpu_accessible.return_value = False
    slab = torch.arange(16, dtype=torch.float32).reshape(2, 1, 8, 1)
    worker._batch_retrieve_all = lambda keys, **route: [
        SimpleNamespace(view=bytearray(slab.numpy().tobytes())) for _ in keys
    ]
    context = SimpleNamespace(
        no_compile_layers={LAYER: SimpleNamespace(kv_cache=cache)},
        attn_metadata=make_flash_attn_metadata(),
    )
    return worker, context


def failing_inject(worker, error, at: int):
    """Make the ``at``-th inject raise ``error``; returns the load's refs."""
    inject = worker._inject_kv_into_layer
    calls, seen = [], {}

    def maybe_fail(*args, **kwargs):
        calls.append(1)
        if len(calls) == at:
            raise error
        return inject(*args, **kwargs)

    worker._inject_kv_into_layer = maybe_fail
    copy = worker._inject_slab_layers

    def note_refs(*args, **kwargs):
        seen["refs"] = kwargs.get("refs")
        return copy(*args, **kwargs)

    worker._inject_slab_layers = note_refs
    return calls, seen


@pytest.mark.parametrize("backend", ["cpu", "mixed"])
def test_a_current_stream_inject_cuda_error_stops_the_worker(backend):
    """Nothing waits for the injects queued on the current stream: the leases,
    the slot mapping and the device slab in use are kept, and the load is not
    reported."""
    worker, context = session_worker(backend)
    calls, seen = failing_inject(worker, cuda_error(), at=2)
    meta = load_meta(block_ids=[3, 1, 5, 7])
    with pytest.raises(_UnconfirmedCopyError, match="req r1"):
        worker.start_load_kv(context, MaruConnectorMetadata(requests=[meta]))
    assert len(calls) == 2
    worker._handler.release_retrieved.assert_not_called()
    assert not worker._failed_load_blocks
    refs = seen["refs"]
    assert holds(worker, refs[-1]) and holds(worker, refs[-2])  # slab, slots
    assert stopped(worker, "req r1")


@pytest.mark.parametrize("pinned", [True, False], ids=["pinned", "staged"])
def test_a_placement_kernel_cuda_error_keeps_its_source_slab(monkeypatch, pinned):
    """The kernel raises a CUDA error: the slab it reads (the CXL view, or its
    copy staged on the device), the reads, the slot mapping and the pointer
    table are kept."""
    worker = worker_of("cxl")
    stream = Done()
    stream.wait_stream = lambda other: None
    worker._load_stream, worker._load_stream_device = stream, torch.device("cpu")
    monkeypatch.setattr(torch.cuda, "current_stream", lambda device=None: Done())
    monkeypatch.setattr(torch.cuda, "stream", lambda s: contextlib.nullcontext())
    monkeypatch.setattr(torch.Tensor, "is_pinned", lambda self: pinned)
    monkeypatch.setattr(worker, "_pin_slot_mapping_for_async_h2d", lambda m: m)
    sources = []

    def kernel_call(src, *args, **kwargs):
        sources.append(src)
        raise cuda_error()

    ops = SimpleNamespace(
        multi_layer_kv_transfer=kernel_call,
        TransferDirection=SimpleNamespace(H2D="H2D"),
    )
    ptrs = torch.zeros(1, dtype=torch.int64)  # built outside the load stream
    kernel = (ops, ptrs, None, BLOCK, 1, None)
    worker._packed_load_kernel_ctx = lambda layers, attn: kernel
    slots, infos = torch.arange(PROMPT), views(range(2))
    layers = [(LAYER, torch.zeros(4, 2, 4, 1, 1), 0)]
    with pytest.raises(_UnconfirmedCopyError, match="req r1"):
        worker._load_packed(
            layers, [(load_meta(), 2, slots, infos)], make_flash_attn_metadata()
        )
    assert len(sources) == 1 and holds(worker, sources[0])
    assert holds(worker, infos[0]) and holds(worker, slots)
    assert holds(worker, ptrs)
    assert not worker._failed_load_blocks
    assert stopped(worker, "req r1")


@pytest.mark.parametrize("path", ["packed", "sync"])
def test_a_load_stopped_meanwhile_takes_and_logs_no_later_request(
    monkeypatch, caplog, path
):
    """Another thread stops the worker during r1's copies: r2 and r3 are not
    copied, nor logged as loaded."""
    worker = worker_of("cxl")
    copies = []

    def stop_meanwhile(*args, **kwargs):
        copies.append(1)
        worker._stop_if_unconfirmed(cuda_error(), req_id="r0")

    requests = [
        (
            dataclasses.replace(load_meta(), req_id=r),
            2,
            torch.arange(PROMPT),
            views("ab"),
        )
        for r in ("r1", "r2", "r3")
    ]
    layers = [(LAYER, torch.zeros(4, 2, 4, 1, 1), 0)]
    if path == "sync":
        worker._inject_kv_into_layer = stop_meanwhile
        assert worker._load_sync(layers, requests, None) == 1
        assert len(copies) == 2  # r1's two chunks
        return
    stream = Done()
    stream.wait_stream = lambda other: None
    worker._load_stream, worker._load_stream_device = stream, torch.device("cpu")
    monkeypatch.setattr(torch.cuda, "current_stream", lambda device=None: Done())
    monkeypatch.setattr(torch.cuda, "stream", lambda s: contextlib.nullcontext())
    monkeypatch.setattr(torch.Tensor, "is_pinned", lambda self: True)
    monkeypatch.setattr(worker, "_pin_slot_mapping_for_async_h2d", lambda m: m)
    ops = SimpleNamespace(
        multi_layer_kv_transfer=stop_meanwhile,
        TransferDirection=SimpleNamespace(H2D="H2D"),
    )
    kernel = (ops, torch.zeros(1, dtype=torch.int64), None, BLOCK, 1, None)
    worker._packed_load_kernel_ctx = lambda layers, attn: kernel
    caplog.set_level(logging.INFO, logger="maru_vllm.connector")
    worker._load_packed(layers, requests, make_flash_attn_metadata())
    assert len(copies) == 2  # r1's two chunks
    loaded = [r.getMessage() for r in caplog.records if "packed-loaded" in r.message]
    assert len(loaded) == 1 and loaded[0].endswith("for req r1 (kernel)")


@pytest.mark.parametrize("backend", ["cpu", "mixed"])
@pytest.mark.parametrize("failure", ["short slab", "oom", "layout", "shape"])
def test_a_load_failing_without_a_cuda_error_is_recomputed(backend, failure):
    """As on main: a slab too short to view, out of memory or a rejected
    layout in the first copy call, or a copy whose shapes differ, recompute
    the request and give its leases back."""
    worker, context = session_worker(backend)
    if failure == "short slab":
        worker._batch_retrieve_all = lambda keys, **route: [
            SimpleNamespace(view=bytearray(12)) for _ in keys
        ]
    else:
        error = {
            "oom": torch.OutOfMemoryError("CUDA out of memory"),
            "layout": RuntimeError("Maru: cannot inject KV for layout (8, 2)"),
            "shape": RuntimeError("The size of tensor a (2) must match (3)"),
        }[failure]
        failing_inject(worker, error, at=1)
    meta = load_meta(block_ids=[3, 1, 5, 7])
    worker.start_load_kv(context, MaruConnectorMetadata(requests=[meta]))
    assert worker.take_failed_load_blocks() == {3, 1, 5, 7}
    assert worker._handler.release_retrieved.call_count == 1
    assert worker._stopped is None


@pytest.mark.parametrize("error", ["cuda", "other"])
def test_an_inline_layerwise_inject_error_stops_only_on_a_cuda_error(error):
    worker = worker_of("cxl")
    layers = [(LAYER, torch.zeros(4), 0)]
    worker._inject_kv_into_layer = MagicMock(
        side_effect=cuda_error() if error == "cuda" else RuntimeError("bad layout")
    )
    reads = views("ab")
    load = [(load_meta(), 2, torch.arange(PROMPT), reads)]
    if error == "other":
        worker._load_sync(layers, load, None)
        assert worker.take_failed_load_blocks()
        return
    with pytest.raises(_UnconfirmedCopyError, match="req r1"):
        worker._load_sync(layers, load, None)
    assert not worker._failed_load_blocks
    assert holds(worker, reads[0])
    assert stopped(worker, "req r1")


@pytest.mark.parametrize("backend", ["cpu", "mixed"])
def test_session_reads_of_an_unconfirmed_load_are_kept(backend):
    worker = worker_of(backend)
    worker._batch_retrieve_all = lambda keys, **route: views(keys)

    def unconfirmed(layers, prepared, attn):
        worker._stop_if_unconfirmed(cuda_error(), prepared, req_id="r1")
        raise _UnconfirmedCopyError(worker._stopped)

    worker._load_packed = unconfirmed
    with pytest.raises(_UnconfirmedCopyError, match="req r1"):
        worker.start_load_kv(forward(), MaruConnectorMetadata(requests=[load_meta()]))
    worker._handler.release_retrieved.assert_not_called()
    assert stopped(worker, "req r1")


def test_a_layerwise_batch_without_its_event_stops_the_worker(monkeypatch):
    """The reads and the host and device slot mappings stay; the layers'
    device tensors went back to the caching allocator, which reuses them only
    after the queued copies in the load stream's order."""
    worker = worker_of("cxl")
    device = fake_cuda(monkeypatch, worker, Done())
    layer_devs = []

    def copy_layer(infos, num_chunks, layer_idx, layer, stream, refs):
        # As the helper does: the tensor is in refs before its copy
        layer_devs.append(torch.zeros(1))
        refs.append(layer_devs[-1])
        return layer_devs[-1]

    monkeypatch.setattr(worker, "_copy_chunkwise_layer_to_device", copy_layer)
    worker._inject_kv_into_layer = MagicMock()
    records = []

    class Event:
        def record(self, stream=None):
            records.append(1)
            if len(records) == 2:  # the batch event, after the layer's
                raise cuda_error()

    monkeypatch.setattr(torch.cuda, "Event", Event)
    slots, slots_dev = MagicMock(), MagicMock()
    slots.to.return_value = slots_dev
    meta = load_meta(deferred_load=True, layerwise_load=True)
    reads = views("ab")
    worker._deferred_layerwise_loads["r1"] = (meta, 2, slots, reads)
    worker._schedule_deferred_layerwise_loads(
        [(LAYER, SimpleNamespace(device=device), 0)], {"r1"}, None
    )
    assert not worker._active_load_refs
    assert holds(worker, reads[0])
    assert holds(worker, slots) and holds(worker, slots_dev)
    assert len(layer_devs) == 1 and not holds(worker, layer_devs[0])
    assert stopped(worker, "req r1")


@pytest.mark.parametrize("layout", ["chunkwise", "layerwise"])
def test_a_layer_copy_that_raises_keeps_its_device_tensor(monkeypatch, layout):
    """cudaMemcpy2DAsync (chunkwise) or copy_ (layerwise) raises a CUDA error:
    the layer's device tensor it was copying into is kept."""
    worker = worker_of("cxl", maru_use_layerwise=layout == "layerwise")
    worker._num_layers = 1
    stream = Done()
    stream.cuda_stream = 0
    monkeypatch.setattr(torch.cuda, "set_device", lambda device: None)
    monkeypatch.setattr(torch.cuda, "stream", lambda s: contextlib.nullcontext())
    monkeypatch.setattr(worker, "_layerwise_stream_for", lambda device: stream)
    monkeypatch.setattr(worker, "_pin_slot_mapping_for_async_h2d", lambda m: m)
    made = []
    empty = torch.empty

    def note_empty(*args, **kwargs):
        made.append(empty(*args, **kwargs))
        return made[-1]

    monkeypatch.setattr(torch, "empty", note_empty)
    if layout == "chunkwise":
        failed = MagicMock(return_value=1)  # cudaMemcpy2DAsync's error code
        monkeypatch.setattr(
            connector_module, "_get_cuda_memcpy2d_async", lambda: failed
        )
        infos = views("ab")
    else:

        def copy_(self, *args, **kwargs):
            raise cuda_error()

        monkeypatch.setattr(torch.Tensor, "copy_", copy_)
        infos = {0: views("ab")}
    layer = torch.zeros(4, 2, 4, 1, 1)
    events = worker._issue_layerwise_copies_offthread(
        load_meta(), 2, torch.arange(PROMPT), infos, [(LAYER, layer, 0)], "cpu"
    )
    assert events is None and made
    assert holds(worker, made[-1])
    assert stopped(worker, "req r1")


# =============================================================================
# A stop ends every load, and nothing read since is given back
# =============================================================================


@pytest.mark.parametrize("backend", ["cxl", "cpu", "mixed"])
def test_a_stop_leaves_the_later_loads_of_the_step_unread(backend):
    """Another load stops the worker while this step reads r1's chunks."""
    worker = worker_of(backend)
    reads = []

    def retrieve(keys, **route):
        reads.append(views(keys))
        worker._stop_if_unconfirmed(cuda_error(), req_id="other")
        return reads[-1]

    worker._batch_retrieve_all = retrieve
    worker._load_packed = MagicMock()
    with pytest.raises(_UnconfirmedCopyError, match="req other"):
        worker.start_load_kv(forward(), three(load_meta()))
    assert len(reads) == 1
    worker._load_packed.assert_not_called()
    worker._handler.release_retrieved.assert_not_called()  # kept, never queued
    assert holds(worker, reads[0][0])


def test_a_stop_leaves_the_later_leased_loads_of_the_step_unread():
    worker = worker_of("remote_cxl")
    calls = []
    worker._load_leased_request = stop_at_r1(worker, calls)
    with pytest.raises(_UnconfirmedCopyError, match="req r1"):
        worker.start_load_kv(forward(), three(load_meta()))
    assert calls == ["r1"]


@pytest.mark.parametrize("backend", ["cxl", "cpu", "remote_cxl"])
def test_a_stop_leaves_the_later_group_loads_of_the_step_unread(backend):
    worker = worker_of(backend)
    worker._group_caches = [SimpleNamespace(tensors=[torch.zeros(1)])]
    calls = []
    if backend == "cxl":
        worker._retrieve_group_objects = stop_at_r1(worker, calls)
    else:
        worker._load_group_leased = stop_at_r1(worker, calls)
    worker._start_group_loads(three(group_meta()))
    assert calls == ["r1"]
    assert stopped(worker, "req r1")


@pytest.mark.parametrize("backend", ["cxl", "cpu", "remote_cxl"])
def test_a_queued_load_job_reads_nothing_once_the_worker_stopped(backend):
    worker = worker_of(backend)
    worker._stop_if_unconfirmed(cuda_error(), req_id="r0")
    worker._batch_retrieve_all = MagicMock()
    worker._retrieve_group_objects = MagicMock()
    worker._load_group_leased = MagicMock()
    worker._load_leased_request = MagicMock()
    worker._inflight_deferred_req_ids.add("r1")
    worker._deferred_load_job(
        load_meta(deferred_load=True), [(LAYER, None, 0)], torch.device("cpu")
    )
    worker._group_load_job(group_meta(deferred_load=True), torch.device("cpu"), Done())
    worker._batch_retrieve_all.assert_not_called()
    worker._retrieve_group_objects.assert_not_called()
    worker._load_group_leased.assert_not_called()
    worker._load_leased_request.assert_not_called()
    # Nor does the memory side hear of a load (a read promote)
    worker._handler.promote_read.assert_not_called()
    assert not worker._inflight_deferred_req_ids and not worker._deferred_done


def test_a_stopped_leased_load_sends_no_read_promote():
    worker = worker_of("remote_cxl")
    worker._stop_if_unconfirmed(cuda_error(), req_id="r0")
    worker._batch_retrieve_all = MagicMock()
    worker._load_leased_request([(LAYER, None, 0)], load_meta(), None)
    worker._handler.promote_read.assert_not_called()
    worker._batch_retrieve_all.assert_not_called()


@pytest.mark.parametrize("backend", ["cpu", "remote_cxl"])
def test_a_group_load_stopped_during_retrieve_keeps_without_placing(
    monkeypatch, backend
):
    worker = worker_of(backend)
    reads = views("k")
    monkeypatch.setattr(
        worker, "_group_load_plan", lambda meta: (1, [(None, 0, None)], ["key"])
    )
    entered, resume = threading.Event(), threading.Event()

    def retrieve(keys, **route):
        entered.set()
        assert resume.wait(5)
        return reads

    monkeypatch.setattr(worker, "_batch_retrieve_all", retrieve)
    worker._place_group_objects = MagicMock()
    stream = MagicMock()
    with ThreadPoolExecutor(max_workers=1) as executor:
        load = executor.submit(
            worker._load_group_leased,
            group_meta(),
            torch.device("cpu"),
            stream,
            offthread=True,
        )
        try:
            assert entered.wait(5)
            worker._stop_if_unconfirmed(cuda_error(), req_id="other")
        finally:
            resume.set()
        loaded = load.result(5)

    assert worker._stopped is not None and loaded is None
    worker._handler.release_retrieved.assert_not_called()
    assert holds(worker, reads[0])
    worker._place_group_objects.assert_not_called()
    stream.synchronize.assert_not_called()
    assert not worker._failed_load_blocks and not worker._failed_recv_req_ids


def test_a_cxl_load_stopped_during_admission_releases_without_submitting(monkeypatch):
    worker = worker_of("cxl")
    worker._load_admission_window = 1
    worker._packed_store_kernel_ctx = lambda attn: None
    worker._deferred_stream = Done()
    worker._deferred_stream_device = torch.device("cpu")
    worker._inject_slab_layers = MagicMock()
    worker._inflight_deferred_req_ids.add("r1")
    entered, resume = threading.Event(), threading.Event()

    class Admission:
        ended = False

        def query(self):
            return self.ended

        def synchronize(self):
            entered.set()
            assert resume.wait(5)
            self.ended = True

    worker._deferred_events["other"] = Admission()
    monkeypatch.setattr(torch.cuda, "set_device", lambda device: None)
    monkeypatch.setattr(torch.cuda, "stream", lambda stream: contextlib.nullcontext())
    event = MagicMock()
    monkeypatch.setattr(torch.cuda, "Event", event)
    # Page-locked memory needs a GPU; the slot mapping stays as it is
    monkeypatch.setattr(worker, "_pin_slot_mapping_for_async_h2d", lambda slots: slots)
    reads = []

    def retrieve(keys, **route):
        reads.extend(views(keys))
        return reads

    monkeypatch.setattr(worker, "_batch_retrieve_all", retrieve)
    with ThreadPoolExecutor(max_workers=1) as executor:
        load = executor.submit(
            worker._deferred_load_job,
            load_meta(deferred_load=True),
            [(LAYER, torch.zeros(1), 0)],
            torch.device("cpu"),
        )
        try:
            assert entered.wait(5)
            worker._stop_if_unconfirmed(cuda_error(), req_id="other")
        finally:
            resume.set()
        load.result(5)

    assert worker._stopped is not None and holds(worker, reads[0])
    worker._inject_slab_layers.assert_not_called()
    event.assert_not_called()
    assert "r1" not in worker._deferred_events and "r1" not in worker._deferred_refs
    assert not worker._inflight_deferred_req_ids and not worker._deferred_done
    assert not worker._failed_load_blocks and not worker._failed_recv_req_ids


@pytest.mark.parametrize("failure", ["miss", "truncated"])
def test_a_parked_load_failing_before_its_copies_is_recomputed(failure):
    """Whatever the streams: no copy was queued."""
    worker = worker_of("cxl")
    worker._deferred_stream = FailedWait()
    worker._layerwise_streams = [FailedWait()]
    worker._inflight_deferred_req_ids.add("r1")

    def retrieve(keys, **route):
        infos = views(keys)
        return infos[:-1] if failure == "truncated" else [None, *infos[1:]]

    worker._batch_retrieve_all = retrieve
    meta = load_meta(deferred_load=True)
    worker._deferred_load_job(meta, [(LAYER, torch.zeros(1), 0)], torch.device("cpu"))
    assert worker.take_failed_load_blocks() == set(meta.block_ids)
    assert worker._deferred_done == {"r1"}
    assert worker._stopped is None


# =============================================================================
# Store copies
# =============================================================================


def store_meta(req_id="r1") -> MaruReqMeta:
    return MaruReqMeta(
        req_id,
        list(range(PROMPT)),
        list(range(PROMPT // BLOCK)),
        True,
        num_scheduled_tokens=PROMPT,
        num_computed_tokens=0,
    )


def store_worker(**extra):
    worker = make_worker(BLOCK, CHUNK, extra)
    handles = []

    def alloc(nbytes, **route):
        handles.append(SimpleNamespace(buf=bytearray(nbytes)))
        return handles[-1]

    worker._handler = MagicMock()
    worker._handler.alloc.side_effect = alloc
    return worker, handles


@pytest.mark.parametrize("layerwise", [True, False], ids=["layerwise", "packed"])
@pytest.mark.parametrize("fails", ["oom", "short page", "shape", "cuda"])
def test_a_blocking_store_copy_keeps_its_page_only_on_a_cuda_error(
    monkeypatch, layerwise, fails
):
    """As on main, a failure without a CUDA error skips the chunk and frees
    any page it took. A CUDA error from the copy keeps the page and stops the
    worker."""
    worker, handles = store_worker(maru_use_layerwise=layerwise)
    worker._num_layers = 1
    monkeypatch.setattr(
        worker, "_extract_kv_from_layer", lambda *a: torch.zeros(2, CHUNK, 4)
    )
    metadata = MaruConnectorMetadata(requests=[store_meta()])
    alloc = worker._handler.alloc.side_effect
    if fails == "oom":
        worker._handler.alloc.side_effect = torch.OutOfMemoryError("out of memory")
    elif fails == "short page":
        worker._handler.alloc.side_effect = lambda n, **r: alloc(n // 2)
    else:

        def copy_(self, *args, **kwargs):
            raise cuda_error() if fails == "cuda" else RuntimeError("size mismatch")

        monkeypatch.setattr(torch.Tensor, "copy_", copy_)
    if fails != "cuda":
        worker.save_kv_layer(LAYER, torch.zeros(1), None, metadata)
        assert worker._stopped is None
        freed = [c.args[0] for c in worker._handler.free.call_args_list]
        assert freed == handles  # every page taken went back, chunk by chunk
        return
    with pytest.raises(_UnconfirmedCopyError, match=UNCONFIRMED):
        worker.save_kv_layer(LAYER, torch.zeros(1), None, metadata)
    worker._handler.free.assert_not_called()
    worker._handler.batch_store.assert_not_called()
    worker._handler.store.assert_not_called()
    assert len(handles) == 1
    # The packed slab stays a pending slab, which a stopped worker never frees
    assert holds(worker, handles[0]) or any(
        handle is handles[0] for handle, _ in worker._pending_slabs.values()
    )
    assert stopped(worker)


@pytest.mark.parametrize("layerwise", [True, False], ids=["layerwise", "packed"])
def test_a_blocking_store_stopped_meanwhile_registers_nothing(layerwise):
    """Another thread stops the worker while this store takes its first
    page: that chunk's copy ends, the next chunk is not copied, and nothing
    is registered or freed."""
    worker, handles = store_worker(maru_use_layerwise=layerwise)
    worker._num_layers = 1
    worker._extract_kv_from_layer = lambda *a: torch.zeros(2, CHUNK, 4)
    alloc = worker._handler.alloc.side_effect

    def stop_meanwhile(nbytes, **route):
        worker._stop_if_unconfirmed(cuda_error(), req_id="r0")
        return alloc(nbytes, **route)

    worker._handler.alloc.side_effect = stop_meanwhile
    metadata = MaruConnectorMetadata(requests=[store_meta()])
    worker.save_kv_layer(LAYER, torch.zeros(1), None, metadata)
    assert len(handles) == 1  # of two chunks
    worker._handler.batch_store.assert_not_called()
    worker._handler.store.assert_not_called()
    worker._handler.free.assert_not_called()
    assert holds(worker, handles[0])
    assert stopped(worker, "req r0")


def sync_store(monkeypatch, pinned=True, stream_fails=False, kernel_error=None):
    """A synchronous kernel store of two chunks on a fake CUDA stream; the
    kernel raises ``kernel_error`` on the second chunk."""
    worker, handles = store_worker()
    dev = SimpleNamespace(type="cuda")
    stream = FailsOnce() if stream_fails else Done()
    stream.wait_stream = lambda other: None
    worker._sync_store_stream, worker._sync_store_stream_device = stream, dev
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)
    monkeypatch.setattr(torch.cuda, "current_stream", lambda device=None: stream)
    monkeypatch.setattr(torch.cuda, "stream", lambda s: contextlib.nullcontext())
    monkeypatch.setattr(torch.Tensor, "is_pinned", lambda self: pinned)
    monkeypatch.setattr(torch, "empty_like", lambda t, device=None: torch.zeros_like(t))
    slots = MagicMock()
    slots.shape = (PROMPT,)
    slots.to.return_value = slots
    monkeypatch.setattr(worker, "_build_slot_mapping", lambda ids, n: slots)
    launches = []

    def kernel_call(*args, **kwargs):
        launches.append(1)
        if len(launches) == 2 and kernel_error is not None:
            raise kernel_error

    ops = MagicMock()
    ops.multi_layer_kv_transfer.side_effect = kernel_call
    kernel = (ops, None, None, BLOCK, 1, None)
    target = _SlabTarget(1, 2 * CHUNK * 4, torch.float32, dev)
    worker._slab_target = lambda: target
    worker._handler.batch_store.side_effect = lambda keys, h: [True] * len(keys)
    return worker, handles, kernel, slots


@pytest.mark.parametrize("pinned", [True, False], ids=["pinned", "staged"])
def test_a_sync_store_that_cannot_be_waited_for_keeps_its_pages(monkeypatch, pinned):
    worker, handles, kernel, slots = sync_store(
        monkeypatch, pinned=pinned, stream_fails=True
    )
    metadata = MaruConnectorMetadata(requests=[store_meta()])
    with pytest.raises(_UnconfirmedCopyError, match=UNCONFIRMED):
        worker._store_packed_slabs(kernel, metadata)
    assert len(handles) == 2
    worker._handler.free.assert_not_called()
    worker._handler.batch_store.assert_not_called()
    assert all(holds(worker, h) for h in handles) and holds(worker, slots)
    staging = [x for x in kept_by(worker) if isinstance(x, torch.Tensor)]
    assert bool(staging) != pinned  # the staging the copies read
    assert stopped(worker)


@pytest.mark.parametrize("error", ["cuda", "check", "alloc"])
def test_a_sync_store_chunk_failing_keeps_its_page_only_on_a_cuda_error(
    monkeypatch, error
):
    """A CUDA error from the second chunk's kernel keeps its page and stores
    nothing; a rejected kernel argument (TORCH_CHECK) or a failed allocation
    skips that chunk and stores the first, as on main."""
    worker, handles, kernel, slots = sync_store(
        monkeypatch,
        kernel_error={
            "cuda": cuda_error(),
            "check": RuntimeError("Unsupported EngineKVFormat: 9"),
            "alloc": None,
        }[error],
    )
    if error == "alloc":
        alloc = worker._handler.alloc.side_effect
        allocs = []

        def second_fails(nbytes, **route):
            allocs.append(1)
            if len(allocs) == 2:
                raise MemoryError("no page")
            return alloc(nbytes, **route)

        worker._handler.alloc.side_effect = second_fails
    metadata = MaruConnectorMetadata(requests=[store_meta()])
    if error != "cuda":
        worker._store_packed_slabs(kernel, metadata)
        assert worker._stopped is None
        assert worker._handler.batch_store.call_args.args[1] == handles[:1]
        assert [c.args[0] for c in worker._handler.free.call_args_list] == handles[1:]
        return
    with pytest.raises(_UnconfirmedCopyError, match=UNCONFIRMED):
        worker._store_packed_slabs(kernel, metadata)
    worker._handler.free.assert_not_called()
    worker._handler.batch_store.assert_not_called()  # stopped: none stored
    assert all(holds(worker, h) for h in handles) and holds(worker, slots)
    assert stopped(worker)


@pytest.mark.parametrize("wait", ["cuda", "other"])
def test_a_sync_store_wait_failing_frees_its_pages_only_without_a_cuda_error(
    monkeypatch, wait
):
    """As on main, a final wait failing otherwise frees the pages it copied
    into, stores none and raises."""
    worker, handles, kernel, slots = sync_store(monkeypatch)

    def synchronize():
        raise cuda_error() if wait == "cuda" else RuntimeError("stream destroyed")

    worker._sync_store_stream.synchronize = synchronize
    metadata = MaruConnectorMetadata(requests=[store_meta()])
    with pytest.raises(RuntimeError) as raised:
        worker._store_packed_slabs(kernel, metadata)
    worker._handler.batch_store.assert_not_called()
    freed = [c.args[0] for c in worker._handler.free.call_args_list]
    if wait == "other":
        assert "stream destroyed" in str(raised.value)
        assert len(handles) == 2 and freed == handles
        assert worker._stopped is None
        return
    assert raised.type is _UnconfirmedCopyError
    assert not freed and all(holds(worker, h) for h in handles)
    assert stopped(worker)


def test_a_sync_store_whose_slot_mapping_copy_raises_stops(monkeypatch):
    """The copy fails before any chunk's and the stream then drains: the
    worker still stops and stores nothing."""
    worker, handles, kernel, slots = sync_store(monkeypatch)
    slots.to.side_effect = cuda_error()
    metadata = MaruConnectorMetadata(requests=[store_meta()])
    with pytest.raises(_UnconfirmedCopyError, match=UNCONFIRMED):
        worker._store_packed_slabs(kernel, metadata)
    assert not handles
    worker._handler.batch_store.assert_not_called()
    assert holds(worker, slots)
    assert stopped(worker)


def test_a_sync_store_stopped_meanwhile_copies_no_later_request(monkeypatch):
    """Another thread stops the worker during r1's first copy: r1's next
    chunk and r2 are not copied, and nothing is stored."""
    worker, handles, kernel, slots = sync_store(monkeypatch)
    launches = []

    def launch(*args, **kwargs):
        launches.append(1)
        worker._stop_if_unconfirmed(cuda_error(), req_id="r0")

    kernel[0].multi_layer_kv_transfer.side_effect = launch
    metadata = MaruConnectorMetadata(requests=[store_meta("r1"), store_meta("r2")])
    worker._store_packed_slabs(kernel, metadata)
    assert len(launches) == 1 and slots.to.call_count == 1
    assert len(handles) == 1 and holds(worker, handles[0])
    worker._handler.batch_store.assert_not_called()
    worker._handler.free.assert_not_called()
    assert stopped(worker, "req r0")


# =============================================================================
# CUDA calls outside a copy path's except
# =============================================================================


def test_a_layer_wait_that_raises_stops_the_worker(monkeypatch):
    worker = worker_of("cxl")
    stream = MagicMock()
    stream.wait_event.side_effect = cuda_error()
    monkeypatch.setattr(torch.cuda, "current_stream", lambda device=None: stream)
    worker._layer_load_events[LAYER] = [Done()]
    with pytest.raises(_UnconfirmedCopyError, match=UNCONFIRMED):
        worker.wait_for_layer_load(LAYER)
    assert stopped(worker)


def test_a_group_load_stream_that_cannot_wait_stops_the_worker(monkeypatch):
    worker = worker_of("cxl")
    stream = Done()
    device = fake_cuda(monkeypatch, worker, stream)
    stream.wait_stream = MagicMock(side_effect=cuda_error())
    worker._group_caches = [SimpleNamespace(tensors=[SimpleNamespace(device=device)])]
    worker._retrieve_group_objects = MagicMock()
    with pytest.raises(_UnconfirmedCopyError, match=UNCONFIRMED):
        worker._start_group_loads(MaruConnectorMetadata(requests=[group_meta()]))
    worker._retrieve_group_objects.assert_not_called()
    assert stopped(worker)


@pytest.mark.parametrize("error", ["cuda", "other", "stopped meanwhile"])
def test_an_activation_without_a_common_stream_stops_only_on_a_cuda_error(error):
    """Layers on the CPU or on several devices inject on the current stream:
    a CUDA error keeps the reads and slot mappings and stops the worker;
    another error recomputes the request as before. A stop from another
    thread ends the activation at the next request."""
    worker = worker_of("cxl")
    worker._num_layers = 1
    worker._inject_kv_into_layer = MagicMock(
        side_effect=cuda_error() if error == "cuda" else RuntimeError("bad layout")
    )
    meta = load_meta(deferred_load=True, layerwise_load=True)
    reads, slots = views("ab"), torch.arange(PROMPT)
    worker._deferred_layerwise_loads["r1"] = (meta, 2, slots, reads)
    layers = [(LAYER, torch.zeros(4, 2, 4, 1, 1), 0)]
    if error == "stopped meanwhile":
        other = dataclasses.replace(meta, req_id="r2")
        worker._deferred_layerwise_loads["r2"] = (other, 2, slots, views("cd"))
        worker._inject_kv_into_layer.side_effect = lambda *a: (
            worker._stop_if_unconfirmed(cuda_error(), req_id="r0")
        )
        worker._schedule_deferred_layerwise_loads(layers, {"r1", "r2"}, None)
        assert worker._inject_kv_into_layer.call_count == 2  # one request's chunks
        assert not worker._failed_load_blocks
        assert stopped(worker, "req r0")
        return
    if error == "other":
        worker._schedule_deferred_layerwise_loads(layers, {"r1"}, None)
        assert worker.take_failed_load_blocks() == set(meta.block_ids)
        assert worker._stopped is None
        return
    with pytest.raises(_UnconfirmedCopyError, match="req r1"):
        worker._schedule_deferred_layerwise_loads(layers, {"r1"}, None)
    assert holds(worker, reads[0]) and holds(worker, slots)
    assert not worker._failed_load_blocks
    assert stopped(worker, "req r1")


@pytest.mark.parametrize("call", ["record", "poll", "layer wait"])
def test_a_timing_event_that_raises_stops_the_worker(monkeypatch, call):
    """maru_log_timing: a CUDA error from recording or asking about a timing
    event is not dropped with the timing record."""
    worker = worker_of("cxl", maru_log_timing=True)
    poller = worker._trace_poller
    poller._clock = object()  # no GPU clock to build
    if call == "record":

        class FailedRecord:
            def __init__(self, **kwargs):
                pass

            def record(self, stream=None):
                raise cuda_error()

        monkeypatch.setattr(torch.cuda, "Event", FailedRecord)
        assert poller.event(Done()) is None
    elif call == "poll":
        poller._loads.append(
            _LoadTiming("r1", Done(), FailedQuery(), FailedQuery(), 0, 0, 0)
        )
        poller.poll()
    else:
        assert worker._trace_wait_start(object(), [FailedQuery()], None) is None
    assert stopped(worker)


# =============================================================================
# Gates
# =============================================================================


def test_the_helpers_give_nothing_back_once_stopped():
    """Refs of copies that ended stay, and no _DONE marker page is taken,
    once the worker stopped."""
    worker = worker_of("cxl")
    refs = [object()]
    worker._active_load_refs.append((Done(), refs))
    worker._stop_if_unconfirmed(cuda_error(), req_id="r0")
    worker._release_completed_load_refs()
    assert [entry[1] for entry in worker._active_load_refs] == [refs]
    worker._is_stored = lambda key: True
    worker._register_done_markers({"base": ["base@0"]})
    worker._handler.alloc.assert_not_called()
    worker._handler.batch_store.assert_not_called()
    entry = (load_meta(), 2, torch.arange(PROMPT), views(range(2)))
    worker._deferred_layerwise_loads["r1"] = entry
    worker._inject_kv_into_layer = MagicMock()
    layers = [(LAYER, torch.zeros(4, 2, 4, 1, 1), 0)]
    worker._schedule_deferred_layerwise_loads(layers, {"r1"}, None)
    worker._inject_kv_into_layer.assert_not_called()
    assert worker._deferred_layerwise_loads["r1"] is entry


def test_a_done_marker_taken_as_the_worker_stops_is_kept():
    worker = worker_of("cxl")
    marker = object()

    def alloc(nbytes):
        worker._stop_if_unconfirmed(cuda_error(), req_id="r0")
        return marker

    worker._handler.alloc.side_effect = alloc
    worker._write_done_marker("base")
    worker._handler.store.assert_not_called()
    worker._handler.free.assert_not_called()
    assert holds(worker, marker)


def test_the_single_group_save_hook_raises_once_stopped():
    worker = worker_of("cxl")
    connector = MaruKVConnector.__new__(MaruKVConnector)
    connector._kv_groups, connector._worker = [], worker
    connector.wait_for_save()  # one group: nothing to store here
    worker._stop_if_unconfirmed(cuda_error(), req_id="r0")
    with pytest.raises(_UnconfirmedCopyError, match="req r0"):
        connector.wait_for_save()


# =============================================================================
# Shutdown
# =============================================================================


def test_a_stopped_worker_issues_no_queued_store_at_shutdown():
    worker = worker_of("cxl")
    handler = worker._handler
    worker._stop_if_unconfirmed(cuda_error(), req_id="r0")
    worker._queued_store_batches = [("kernel", MaruConnectorMetadata())]
    worker._store_packed_slabs_write_behind = MagicMock()
    staging = worker._store_staging = torch.zeros(1)
    worker.shutdown()
    worker._store_packed_slabs_write_behind.assert_not_called()
    assert worker._queued_store_batches == []
    # Nothing is given back or closed, though nothing was held by name
    handler.close.assert_not_called()
    assert worker._handler is handler and worker._store_staging is staging
    assert is_kept(worker)


@pytest.mark.parametrize("error", ["cuda", "other"])
def test_a_queued_store_failing_at_shutdown_lets_shutdown_go_on(error):
    """A CUDA error keeps everything; another error is only logged."""
    worker = worker_of("cxl")
    handler = worker._handler
    worker._queued_store_batches = [("kernel", MaruConnectorMetadata())]
    worker._store_packed_slabs_write_behind = MagicMock(
        side_effect=cuda_error() if error == "cuda" else RuntimeError("bad batch")
    )
    loader = worker._deferred_executor = MagicMock()
    worker._load_stream = load = MagicMock()
    worker.shutdown()
    loader.shutdown.assert_called_once_with(wait=True)
    load.synchronize.assert_called_once_with()
    if error == "cuda":
        handler.close.assert_not_called()
        assert is_kept(worker)
    else:
        handler.close.assert_called_once_with()
