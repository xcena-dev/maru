# SPDX-License-Identifier: Apache-2.0
"""remote_cxl loads in staging-sized batches, asynchronous loads and stores, and the
copy kernel over the staging buffer (design note 3.11)."""

import contextlib
import itertools
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")
import torch

from maru_vllm.connector import MaruConnectorMetadata, MaruReqMeta, _req_chunk_keys
from tests.unit.test_vllm_remote_cxl_config import extra as remote_cxl_extra
from tests.unit.vllm_connector_helpers import (
    make_flash_attn_metadata,
    make_scheduler,
    make_worker,
)


def _slab(i=0):
    return torch.arange(16, dtype=torch.float32).reshape(2, 1, 8, 1) + i * 100


def _view(slab):
    return SimpleNamespace(view=bytearray(slab.numpy().tobytes()))


def _worker(capacity):
    worker = make_worker(4, 8, remote_cxl_extra(), num_kv_heads=1, head_size=1)
    cache = torch.full((12, 2, 4, 1, 1), -1.0)
    worker._kv_layout = worker._resolve_kv_layout({"layer": cache})
    worker._ensure_handler = lambda: None
    worker._handler = Mock()
    worker._handler.retrieve_capacity.return_value = capacity
    worker._handler.storage_gpu_accessible.return_value = False
    return worker, cache


def _load_meta(**kwargs):
    return MaruReqMeta(
        "load",
        list(range(24)),
        list(range(6)),
        False,
        num_matched_chunks=3,
        load_start_token=0,
        load_end_token=24,
        **kwargs,
    )


def _context(cache):
    return SimpleNamespace(
        attn_metadata=make_flash_attn_metadata(),
        no_compile_layers={"layer": SimpleNamespace(kv_cache=cache)},
    )


@pytest.mark.parametrize("capacity", [1, 2, 3])
def test_loads_go_in_batches_the_staging_buffer_can_hold(capacity):
    """Each batch is released before the next one is retrieved."""
    worker, cache = _worker(capacity)
    meta = _load_meta()
    keys = _req_chunk_keys(meta, 8)
    slabs = {key: _slab(i) for i, key in enumerate(keys)}
    held = []

    def retrieve(requested, route_key=None):
        assert len(requested) <= capacity and not held
        # Every batch routes by the request's first chunk key.
        assert route_key == keys[0]
        held.extend(requested)
        return [_view(slabs[k]) for k in requested]

    worker._handler.release_retrieved.side_effect = lambda infos: held.clear()
    worker._batch_retrieve_all = retrieve
    worker.start_load_kv(_context(cache), MaruConnectorMetadata(requests=[meta]))
    assert not worker.take_failed_load_blocks()
    assert worker._handler.release_retrieved.call_count == -(-3 // capacity)
    for token in range(24):
        value = cache[token // 4, :, token % 4, 0, 0]
        torch.testing.assert_close(value, slabs[keys[token // 8]][:, 0, token % 8, 0])


def test_a_missing_later_batch_fails_only_from_that_batch():
    worker, cache = _worker(1)
    meta = _load_meta()
    keys = _req_chunk_keys(meta, 8)
    worker._batch_retrieve_all = lambda req, route_key=None: (
        [None] if req[0] == keys[1] else [_view(_slab())]
    )
    worker.start_load_kv(_context(cache), MaruConnectorMetadata(requests=[meta]))
    assert worker.take_failed_load_blocks() == {2, 3, 4, 5}  # chunk 1 onward


def test_a_failed_batch_copy_stops_the_later_batches():
    worker, cache = _worker(1)
    requested = []

    def retrieve(keys, route_key=None):
        requested.append(keys)
        return [SimpleNamespace(view=bytearray(3)) for _ in keys]  # too short

    worker._batch_retrieve_all = retrieve
    meta = _load_meta()
    worker.start_load_kv(_context(cache), MaruConnectorMetadata(requests=[meta]))
    assert worker.take_failed_load_blocks() == {0, 1, 2, 3, 4, 5}
    # One batch of one chunk was read; the later batches are not.
    assert requested == [_req_chunk_keys(meta, 8)[:1]]
    assert worker._handler.release_retrieved.call_count == 1


def test_no_free_staging_slot_recomputes_the_request():
    worker, cache = _worker(0)
    worker._batch_retrieve_all = Mock()
    worker.start_load_kv(
        _context(cache), MaruConnectorMetadata(requests=[_load_meta()])
    )
    assert worker.take_failed_load_blocks() == {0, 1, 2, 3, 4, 5}
    worker._batch_retrieve_all.assert_not_called()


def test_a_deferred_load_unparks_only_after_its_last_batch():
    worker, cache = _worker(1)
    worker._last_attn_metadata = make_flash_attn_metadata()
    meta = _load_meta(deferred_load=True)
    seen_done = []

    def retrieve(requested, route_key=None):
        seen_done.append("load" in worker._deferred_done)
        return [_view(_slab())]

    worker._batch_retrieve_all = retrieve
    worker._load_leased_request([("layer", cache, 0)], meta, worker._last_attn_metadata)
    assert seen_done == [False, False, False]  # never unparked between batches
    assert worker._deferred_done == {"load"}
    assert not worker.take_failed_load_blocks()
    assert worker._handler.release_retrieved.call_count == 3
    torch.testing.assert_close(cache[0, :, 0, 0, 0], _slab()[:, 0, 0, 0])


def test_a_failed_deferred_batch_unparks_for_recompute():
    worker, cache = _worker(1)
    worker._last_attn_metadata = make_flash_attn_metadata()
    meta = _load_meta(deferred_load=True)
    keys = _req_chunk_keys(meta, 8)
    worker._batch_retrieve_all = lambda req, route_key=None: (
        [None] if req[0] == keys[1] else [_view(_slab())]
    )
    worker._load_leased_request([("layer", cache, 0)], meta, worker._last_attn_metadata)
    assert worker._deferred_done == {"load"}
    assert worker.take_failed_load_blocks() == {2, 3, 4, 5}  # chunk 1 onward


def test_the_loader_thread_takes_the_batched_path_on_its_own_stream(monkeypatch):
    worker, cache = _worker(2)
    streams = []
    monkeypatch.setattr(torch.cuda, "set_device", lambda device: None)
    monkeypatch.setattr(
        torch.cuda,
        "Stream",
        lambda device=None, priority=0: streams.append(_FakeStream()) or streams[-1],
    )
    worker._load_leased_request = MagicMock()
    meta = _load_meta(deferred_load=True)
    layers = [("layer", cache, 0)]
    worker._inflight_deferred_req_ids.add("load")
    worker._deferred_load_job(meta, layers, torch.device("cpu"))
    worker._load_leased_request.assert_called_once_with(
        layers, meta, worker._last_attn_metadata, stream=streams[0]
    )
    assert "load" not in worker._inflight_deferred_req_ids


class _FakeStream:
    def __init__(self, log=None, name="load", fail_sync=False, done=False):
        self.log = [] if log is None else log
        self.name = name
        self.fail_sync = fail_sync
        self.done = done

    def wait_stream(self, other):
        self.log.append(f"{self.name} waits")

    def synchronize(self):
        self.log.append(f"{self.name} sync")
        if self.fail_sync:
            raise RuntimeError("CUDA error")

    def record_event(self):
        return _FakeEvent(self.log, self.name, self.fail_sync, self.done)


class _FakeEvent:
    """A CUDA event of a fake stream: its copies have ended once waited for,
    or already when ``done``; ``fail_sync`` makes asking about them fail."""

    def __init__(self, log, name, fail_sync=False, done=False):
        self.log, self.name, self.fail_sync, self.done = log, name, fail_sync, done

    def query(self):
        if self.fail_sync:
            raise RuntimeError("CUDA error")
        return self.done

    def synchronize(self):
        self.log.append(f"{self.name} event sync")
        if self.fail_sync:
            raise RuntimeError("CUDA error")


def _kernel_worker(monkeypatch, log, start=0, stream_fails=False):
    """A remote_cxl worker whose copy kernel and CUDA streams run on the CPU."""
    worker, cache = _worker(3)
    worker._handler.storage_gpu_accessible.return_value = True
    worker._handler.release_retrieved.side_effect = lambda infos: log.append(
        f"release {len(infos)}"
    )
    load = _FakeStream(log, "load", fail_sync=stream_fails)
    monkeypatch.setattr(torch.cuda, "Stream", lambda device=None, priority=0: load)
    monkeypatch.setattr(
        torch.cuda, "current_stream", lambda device=None: _FakeStream(log, "compute")
    )
    monkeypatch.setattr(torch.cuda, "stream", lambda s: contextlib.nullcontext())
    monkeypatch.setattr(worker, "_pin_slot_mapping_for_async_h2d", lambda m: m)
    ops = SimpleNamespace(
        multi_layer_kv_transfer=MagicMock(
            side_effect=lambda *a, **k: log.append("kernel")
        ),
        TransferDirection=SimpleNamespace(H2D="H2D"),
    )
    monkeypatch.setattr(
        worker,
        "_packed_load_kernel_ctx",
        lambda layers, attn: (ops, torch.zeros(1), 16, 4, 1, "FMT"),
    )
    meta = _load_meta()
    meta.load_start_token = start
    keys = _req_chunk_keys(meta, 8)
    worker._batch_retrieve_all = lambda req, route_key=None: [
        _view(_slab(keys.index(k))) for k in req
    ]
    return worker, cache, meta, ops


def test_a_batch_is_released_only_after_its_copies_finish(monkeypatch):
    log = []
    worker, cache, meta, _ = _kernel_worker(monkeypatch, log)
    worker.start_load_kv(_context(cache), MaruConnectorMetadata(requests=[meta]))
    assert log == ["load waits", "kernel", "kernel", "kernel", "load sync", "release 3"]
    assert not worker.take_failed_load_blocks()


def test_part_of_a_chunk_takes_the_per_layer_copy(monkeypatch):
    log = []
    worker, cache, meta, ops = _kernel_worker(monkeypatch, log, start=4)
    worker.start_load_kv(_context(cache), MaruConnectorMetadata(requests=[meta]))
    # Chunk 0 is loaded from token 4 on: a strided slice, so no kernel.
    assert ops.multi_layer_kv_transfer.call_count == 2
    assert log[-2:] == ["load sync", "release 3"]
    torch.testing.assert_close(cache[1, :, 0, 0, 0], _slab(0)[:, 0, 4, 0])
    torch.testing.assert_close(cache[0, :, 0, 0, 0], torch.full((2,), -1.0))


def test_an_offthread_batch_does_not_wait_for_the_compute_stream(monkeypatch):
    log = []
    worker, cache, meta, _ = _kernel_worker(monkeypatch, log)
    own = _FakeStream(log, "loader")
    worker._last_attn_metadata = make_flash_attn_metadata()
    worker._load_leased_request(
        [("layer", cache, 0)], meta, worker._last_attn_metadata, stream=own
    )
    assert "load waits" not in log and "loader waits" not in log
    assert log[-2:] == ["loader event sync", "release 3"]


def _pipelined(
    monkeypatch,
    log,
    stream_fails=False,
    missing=None,
    chunks=1,
    slots=None,
    done=False,
    deferred=False,
    setup=None,
):
    """A loader-thread load of 3 chunks, read ``chunks`` chunks per batch.

    ``missing`` is the index of a chunk the pool no longer has, ``slots`` the
    staging slots of the handler (the load's own batches take them), ``done``
    whether a batch's copies have ended as soon as they are queued, and
    ``setup(worker, ops, stream)`` may change the worker before the load.
    """
    import maru_vllm.connector as connector

    monkeypatch.setattr(connector, "_LOAD_PIPELINE_CHUNKS", chunks)
    worker, cache, meta, ops = _kernel_worker(monkeypatch, log)
    meta.deferred_load = deferred
    keys = _req_chunk_keys(meta, 8)
    held = [0]

    def retrieve(requested, route_key=None):
        log.append(f"read {keys.index(requested[0])}")
        held[0] += len(requested)
        return [
            None if keys.index(k) == missing else _view(_slab(keys.index(k)))
            for k in requested
        ]

    def release(infos):
        held[0] -= len(infos)
        log.append(f"release {len(infos)}")
        if deferred:
            log.append(f"done {'load' in worker._deferred_done}")

    worker._handler.release_retrieved.side_effect = release
    if slots is not None:
        worker._handler.retrieve_capacity.side_effect = lambda: slots - held[0]
    worker._batch_retrieve_all = retrieve
    worker._last_attn_metadata = make_flash_attn_metadata()
    own = _FakeStream(log, "loader", fail_sync=stream_fails, done=done)
    if setup is not None:
        setup(worker, ops, own)
    worker._load_leased_request(
        [("layer", cache, 0)], meta, worker._last_attn_metadata, stream=own
    )
    return worker, keys


def _fail_copy(at, then=None):
    """Make the copy of chunk ``at`` raise, after calling ``then(stream)``."""

    def setup(worker, ops, stream):
        calls = []

        def kernel(*a, **k):
            calls.append(1)
            if len(calls) == at + 1:
                if then is not None:
                    then(stream)
                raise RuntimeError("copy failed")
            stream.log.append("kernel")

        ops.multi_layer_kv_transfer.side_effect = kernel

    return setup


def test_a_loader_thread_reads_the_next_batch_while_copies_run(monkeypatch):
    # The copy of a request no longer waits for its whole READ.
    log = []
    worker, _ = _pipelined(monkeypatch, log)
    assert log.index("read 1") < log.index("loader event sync")
    assert log.index("read 2") < log.index("loader event sync")
    assert log.count("kernel") == 3 and log.count("release 1") == 3
    assert "loader sync" not in log  # each batch waits on its own event
    assert not worker.take_failed_load_blocks()


def test_a_pipelined_miss_fails_from_its_batch_after_the_earlier_copies(monkeypatch):
    log = []
    worker, _ = _pipelined(monkeypatch, log, missing=1)
    assert "read 2" not in log
    # The miss batch goes back at once, chunk 0 once its copies ended.
    assert log.count("release 1") == 2 and "loader event sync" in log
    assert worker.take_failed_load_blocks() == {2, 3, 4, 5}


def test_unconfirmed_pipelined_copies_keep_their_slots(monkeypatch):
    # Waiting on a batch's event failed: its copies may still read the slots,
    # and the request is recomputed from that batch.
    log = []
    worker, _ = _pipelined(monkeypatch, log, stream_fails=True)
    assert "release 1" not in log
    assert worker.take_failed_load_blocks() == {0, 1, 2, 3, 4, 5}
    assert not worker._loaded_request_keys["load"][1]


def test_a_short_staging_waits_for_the_oldest_batch(monkeypatch):
    # Two slots and batches of two: the third chunk waits for the first
    # batch's copies instead of finding no slot and recomputing.
    log = []
    worker, _ = _pipelined(monkeypatch, log, chunks=2, slots=2)
    assert log.index("loader event sync") < log.index("release 2") < log.index("read 2")
    assert log.count("kernel") == 3
    assert not worker.take_failed_load_blocks()


def test_batches_whose_copies_ended_go_back_before_the_next_read(monkeypatch):
    log = []
    worker, _ = _pipelined(monkeypatch, log, done=True)
    assert log.index("release 1") < log.index("read 1")
    assert log.count("release 1") == 3
    assert not worker.take_failed_load_blocks()


def test_a_deferred_load_is_done_only_after_its_last_batch(monkeypatch):
    log = []
    worker, keys = _pipelined(monkeypatch, log, deferred=True)
    assert log.count("release 1") == 3 and "done True" not in log
    assert worker._deferred_done == {"load"}
    assert not worker.take_failed_load_blocks()
    assert worker._loaded_request_keys["load"][1] == set(keys)


def test_a_failed_copy_fails_from_its_batch_after_the_earlier_ones(monkeypatch):
    # The stream drained: every batch goes back, and chunk 2 is recomputed.
    log = []
    worker, _ = _pipelined(monkeypatch, log, setup=_fail_copy(2))
    assert log.count("release 1") == 3
    assert worker.take_failed_load_blocks() == {4, 5}
    assert not worker._loaded_request_keys["load"][1]


def test_an_undrained_failed_copy_keeps_every_unconfirmed_slot(monkeypatch):
    # The stream could not be drained: no batch is known to be done, so all
    # keep their slots and the request is recomputed from its first batch.
    def stuck(stream):
        stream.fail_sync = True

    log = []
    worker, _ = _pipelined(monkeypatch, log, setup=_fail_copy(2, then=stuck))
    assert "release 1" not in log
    assert worker.take_failed_load_blocks() == {0, 1, 2, 3, 4, 5}


def test_an_event_that_cannot_be_queried_keeps_its_slots(monkeypatch):
    def stuck(worker, ops, stream):
        stream.fail_sync = True

    log = []
    worker, _ = _pipelined(monkeypatch, log, setup=stuck)
    assert log == ["read 0", "kernel"]
    assert worker.take_failed_load_blocks() == {0, 1, 2, 3, 4, 5}


def test_an_event_that_cannot_be_recorded_drains_and_releases(monkeypatch):
    def no_event(worker, ops, stream):
        events = []

        def record():
            events.append(1)
            if len(events) == 2:
                raise RuntimeError("no event")
            return _FakeEvent(stream.log, stream.name)

        stream.record_event = record

    log = []
    worker, _ = _pipelined(monkeypatch, log, setup=no_event)
    assert log.count("release 1") == 2 and "read 2" not in log
    assert worker.take_failed_load_blocks() == {2, 3, 4, 5}


def test_a_loader_thread_uploads_the_pointer_table_once(monkeypatch):
    # Uploading it on the loader thread's default stream made every load wait
    # for the forward running on the compute stream.
    log = []
    worker, cache, meta, ops = _kernel_worker(monkeypatch, log)
    built = []
    monkeypatch.setattr(
        worker,
        "_packed_load_kernel_ctx",
        lambda layers, attn: built.append(1) or (ops, torch.zeros(1), 16, 4, 1, "F"),
    )
    own = _FakeStream(log, "loader")
    worker._last_attn_metadata = make_flash_attn_metadata()
    for _ in range(3):
        worker._load_leased_request(
            [("layer", cache, 0)], meta, worker._last_attn_metadata, stream=own
        )
    assert len(built) == 1
    # New KV caches (another address) get a new table.
    other = cache.clone()
    worker._load_leased_request(
        [("layer", other, 0)], meta, worker._last_attn_metadata, stream=own
    )
    assert len(built) == 2


def test_a_copy_error_waits_for_queued_copies_before_releasing(monkeypatch):
    log = []
    worker, cache, meta, ops = _kernel_worker(monkeypatch, log)
    worker._packed_load_kernel_ctx = Mock(side_effect=RuntimeError("bad context"))
    worker._load_stream = _FakeStream(log, "load")
    worker.start_load_kv(_context(cache), MaruConnectorMetadata(requests=[meta]))
    assert log[-2:] == ["load sync", "release 3"]
    assert worker.take_failed_load_blocks() == {0, 1, 2, 3, 4, 5}


def test_slots_stay_taken_if_queued_copies_cannot_be_waited_for(monkeypatch):
    log = []
    worker, cache, meta, ops = _kernel_worker(monkeypatch, log, stream_fails=True)
    worker.start_load_kv(_context(cache), MaruConnectorMetadata(requests=[meta]))
    assert log[-1] == "release 0"  # nothing given back
    assert worker.take_failed_load_blocks() == {0, 1, 2, 3, 4, 5}


def test_the_loader_thread_waits_briefly_for_a_slot(monkeypatch):
    worker, cache = _worker(0)
    monkeypatch.setattr(torch.cuda, "stream", lambda s: contextlib.nullcontext())
    monkeypatch.setattr(worker, "_pin_slot_mapping_for_async_h2d", lambda m: m)
    capacities = itertools.chain([0, 0], itertools.repeat(3))
    worker._handler.retrieve_capacity.side_effect = lambda: next(capacities)
    meta = _load_meta(deferred_load=True)
    worker._batch_retrieve_all = lambda req, route_key=None: [
        _view(_slab()) for _ in req
    ]
    worker._last_attn_metadata = make_flash_attn_metadata()
    worker._load_leased_request(
        [("layer", cache, 0)], meta, worker._last_attn_metadata, stream=_FakeStream()
    )
    assert not worker.take_failed_load_blocks()
    assert worker._deferred_done == {"load"}


def test_a_failed_async_load_is_recomputed_instead_of_retried():
    scheduler = make_scheduler(4, 8, remote_cxl_extra(maru_async_load=True))
    scheduler._count_matched_chunks = lambda tokens: 2
    request = SimpleNamespace(request_id="parked", prompt_token_ids=list(range(16)))
    assert scheduler.get_num_new_matched_tokens(request, 0) == (12, True)
    blocks = SimpleNamespace(get_block_ids=lambda: [[0, 1, 2]])
    scheduler.update_state_after_alloc(request, blocks, 12)
    # vLLM asks again only after the load failed.
    assert scheduler.get_num_new_matched_tokens(request, 0) == (0, False)
    scheduler.request_finished(request, [0, 1, 2])
    assert scheduler.get_num_new_matched_tokens(request, 0) == (12, True)


def _empty_output(preempted=()):
    return SimpleNamespace(
        preempted_req_ids=set(preempted),
        finished_req_ids=set(),
        scheduled_new_reqs=[],
        scheduled_cached_reqs=SimpleNamespace(
            req_ids=[], new_block_ids=[], num_computed_tokens=[], resumed_req_ids=set()
        ),
        num_scheduled_tokens={},
    )


def test_a_preempted_request_gets_its_hit_again():
    scheduler = make_scheduler(4, 8, remote_cxl_extra(maru_async_load=True))
    scheduler._count_matched_chunks = lambda tokens: 2
    request = SimpleNamespace(request_id="r", prompt_token_ids=list(range(16)))
    scheduler.get_num_new_matched_tokens(request, 0)
    blocks = SimpleNamespace(get_block_ids=lambda: [[0, 1, 2]])
    scheduler.update_state_after_alloc(request, blocks, 12)
    scheduler.build_connector_meta(_empty_output())
    scheduler.build_connector_meta(_empty_output(preempted=["r"]))
    assert scheduler.get_num_new_matched_tokens(request, 0) == (12, True)


def test_an_async_load_carries_its_external_token_range():
    """Blocks before the range are vLLM's own prefix cache: a deferred load
    must leave them alone, as an inline load does."""
    scheduler = make_scheduler(4, 8, remote_cxl_extra(maru_async_load=True))
    scheduler._count_matched_chunks = lambda tokens: 2
    request = SimpleNamespace(request_id="parked", prompt_token_ids=list(range(16)))
    assert scheduler.get_num_new_matched_tokens(request, 4) == (8, True)
    blocks = SimpleNamespace(get_block_ids=lambda: [[0, 1, 2]])
    scheduler.update_state_after_alloc(request, blocks, 8)
    output = SimpleNamespace(
        preempted_req_ids=set(),
        finished_req_ids=set(),
        scheduled_new_reqs=[],
        scheduled_cached_reqs=SimpleNamespace(
            req_ids=[], new_block_ids=[], num_computed_tokens=[], resumed_req_ids=set()
        ),
        num_scheduled_tokens={},
    )
    meta = scheduler.build_connector_meta(output)
    (load,) = meta.requests
    assert load.deferred_load
    assert (load.load_start_token, load.load_end_token) == (4, 12)


def test_the_copy_kernel_runs_only_over_page_locked_staging():
    worker, _cache = _worker(1)
    worker._handler.storage_gpu_accessible.return_value = False
    assert worker._lease_buffers_gpu_accessible() is False
    worker._handler.storage_gpu_accessible.return_value = True
    assert worker._lease_buffers_gpu_accessible() is True
    worker._handler = None
    assert worker._lease_buffers_gpu_accessible() is False
    # The store kernel waits for the handler instead of caching "unusable".
    worker._kv_caches = {"layer": torch.zeros(12, 2, 4, 1, 1)}
    worker._num_layers = 1
    assert worker._packed_store_kernel_ctx(None) is None
    assert worker._store_kernel_unusable is False


def test_cpu_storage_keeps_the_kernel_off():
    from tests.unit.test_vllm_cpu_config import extra as cpu_extra

    worker = make_worker(4, 8, cpu_extra())
    worker._handler = Mock()
    worker._handler.storage_gpu_accessible.return_value = True
    assert worker._lease_buffers_gpu_accessible() is False


class _FakeCudaEvent:
    def record(self, stream=None):
        pass

    def synchronize(self):
        pass


def _cpu_cuda(monkeypatch):
    """Let the write-behind path run its CUDA stream calls on the CPU."""
    stream = SimpleNamespace(wait_stream=lambda other: None)
    monkeypatch.setattr(torch.cuda, "Stream", lambda device=None, **kw: stream)
    monkeypatch.setattr(torch.cuda, "current_stream", lambda device=None: stream)
    monkeypatch.setattr(torch.cuda, "stream", lambda s: contextlib.nullcontext())
    monkeypatch.setattr(torch.cuda, "Event", _FakeCudaEvent)


def _store_worker(monkeypatch, url_count):
    _cpu_cuda(monkeypatch)
    urls = ",".join(f"tcp://pool{i}:1" for i in range(url_count))
    worker = make_worker(
        4,
        8,
        remote_cxl_extra(maru_async_store=True, maru_server_url=urls),
        num_kv_heads=1,
        head_size=1,
    )
    cache = torch.arange(6 * 2 * 4, dtype=torch.float32).reshape(6, 2, 4, 1, 1)
    worker._kv_caches = {"model.layers.0.self_attn": cache}
    worker._kv_layout = worker._resolve_kv_layout(worker._kv_caches)
    worker._num_layers = 1
    allocs = []

    def alloc(nbytes, route_key=None):
        allocs.append(route_key)
        return SimpleNamespace(buf=memoryview(bytearray(nbytes)))

    worker._handler = MagicMock()
    worker._handler.has_local.return_value = False
    worker._handler.alloc.side_effect = alloc
    worker._handler.batch_store.side_effect = lambda keys, handles: [True] * len(keys)
    ops = SimpleNamespace(
        multi_layer_kv_transfer=MagicMock(),
        TransferDirection=SimpleNamespace(D2H="D2H"),
    )
    kernel = (ops, torch.zeros(1, dtype=torch.int64), 16, 4, 1, "FMT")
    return worker, kernel, allocs


def test_an_async_store_allocates_in_the_pool_of_its_request(monkeypatch):
    worker, kernel, allocs = _store_worker(monkeypatch, url_count=2)
    token_ids = list(range(16))
    meta = MaruConnectorMetadata(
        requests=[
            MaruReqMeta(
                "r1",
                token_ids,
                [0, 1, 2, 3],
                True,
                num_scheduled_tokens=16,
                num_computed_tokens=0,
            )
        ]
    )
    worker._store_packed_slabs_write_behind(kernel, meta)
    first_key = _req_chunk_keys(meta.requests[0], 8)[0]
    assert allocs == [first_key, first_key]  # both chunks, one route key
    worker.shutdown()


def test_a_full_staging_buffer_skips_the_async_store_quietly(monkeypatch, caplog):
    worker, kernel, _ = _store_worker(monkeypatch, url_count=1)
    worker._handler.alloc.side_effect = MemoryError("no slot for stores")
    meta = MaruConnectorMetadata(
        requests=[
            MaruReqMeta(
                "r1",
                list(range(16)),
                [0, 1, 2, 3],
                True,
                num_scheduled_tokens=16,
                num_computed_tokens=0,
            )
        ]
    )
    with caplog.at_level("WARNING"):
        worker._store_packed_slabs_write_behind(kernel, meta)
    skips = [r for r in caplog.records if "store skipped" in r.getMessage()]
    assert len(skips) == 1  # two chunks skipped, one line per minute
    assert not [r for r in caplog.records if r.levelname == "ERROR"]
    # and no further line per chunk when the skipped keys complete
    assert not [r for r in caplog.records if "store failed" in r.getMessage()]
    worker.shutdown()


def _loaded_store_worker(monkeypatch, *, tokens=24, start=0, end=16, missing=None):
    """Actually load CPU-backed remote slabs before checking store copies."""
    worker, kernel, allocs = _store_worker(monkeypatch, url_count=1)
    cache = next(iter(worker._kv_caches.values()))
    worker._handler.retrieve_capacity.return_value = 1
    worker._handler.storage_gpu_accessible.return_value = False
    load = MaruReqMeta(
        "loaded",
        list(range(tokens)),
        list(range(6)),
        False,
        num_matched_chunks=tokens // 8,
        load_start_token=start,
        load_end_token=end,
    )
    keys = _req_chunk_keys(load, 8)
    worker._batch_retrieve_all = lambda requested, route_key=None: [
        None if keys.index(key) == missing else _view(_slab(keys.index(key)))
        for key in requested
    ]
    worker._load_leased_request([("layer", cache, 0)], load, make_flash_attn_metadata())
    return worker, kernel, allocs, load, keys


def _resumed_store(load, *, request_id=None, computed=None):
    tokens = len(load.token_ids)
    computed = load.load_end_token if computed is None else computed
    return MaruConnectorMetadata(
        requests=[
            MaruReqMeta(
                request_id or load.req_id,
                load.token_ids,
                load.block_ids,
                True,
                num_computed_tokens=computed,
                num_scheduled_tokens=max(1, tokens - computed),
            )
        ]
    )


@pytest.mark.parametrize("tokens, expected", [(16, []), (24, [2])])
def test_a_remote_hit_skips_prefix_gather_and_write_behind(
    monkeypatch, tokens, expected
):
    worker, kernel, allocs, load, keys = _loaded_store_worker(
        monkeypatch, tokens=tokens
    )
    handler = worker._handler
    assert not worker.take_failed_load_blocks()
    worker._store_packed_slabs_write_behind(kernel, _resumed_store(load))
    worker.shutdown()  # wait for any background batch_store to finish
    assert len(allocs) == kernel[0].multi_layer_kv_transfer.call_count == len(expected)
    if expected:
        assert handler.batch_store.call_args.args[0] == [keys[i] for i in expected]
    else:
        handler.batch_store.assert_not_called()
    assert not worker._stored_keys


def test_a_partial_remote_range_keeps_boundary_chunks_eligible(monkeypatch):
    worker, kernel, allocs, load, keys = _loaded_store_worker(
        monkeypatch, start=4, end=20
    )
    handler = worker._handler
    assert not worker.take_failed_load_blocks()
    worker._store_packed_slabs_write_behind(kernel, _resumed_store(load))
    worker.shutdown()
    assert len(allocs) == kernel[0].multi_layer_kv_transfer.call_count == 2
    assert handler.batch_store.call_args.args[0] == [keys[0], keys[2]]


@pytest.mark.parametrize("missing", [0, 1])
def test_a_failed_remote_load_keeps_recomputed_prefix_storable(monkeypatch, missing):
    worker, kernel, allocs, load, keys = _loaded_store_worker(
        monkeypatch, end=24, missing=missing
    )
    handler = worker._handler
    assert worker.take_failed_load_blocks()
    assert not worker._loaded_request_keys[load.req_id][1]
    # The failed request itself stores nothing: its forward ran over blocks
    # the load never filled (#111).
    worker._store_packed_slabs_write_behind(kernel, _resumed_store(load, computed=0))
    assert not allocs
    # A later request with the prefix recomputes and stores all of it.
    worker.handle_preemptions(MaruConnectorMetadata(finished_req_ids={load.req_id}))
    worker._store_packed_slabs_write_behind(
        kernel, _resumed_store(load, request_id="later", computed=0)
    )
    worker.shutdown()
    assert len(allocs) == kernel[0].multi_layer_kv_transfer.call_count == 3
    assert handler.batch_store.call_args.args[0] == keys


def test_write_behind_still_coalesces_prefill_without_a_remote_load(monkeypatch):
    worker, kernel, allocs = _store_worker(monkeypatch, url_count=1)
    load = MaruReqMeta("compute", list(range(24)), list(range(6)), False)
    handler = worker._handler
    first = _resumed_store(load, computed=0)
    first.requests[0].num_scheduled_tokens = 8
    worker._store_packed_slabs_write_behind(kernel, first)
    assert not allocs
    worker._store_packed_slabs_write_behind(kernel, _resumed_store(load, computed=8))
    worker.shutdown()
    assert len(allocs) == 3
    assert handler.batch_store.call_args.args[0] == _req_chunk_keys(load, 8)


def test_write_behind_starts_at_the_first_chunk_the_request_computed(monkeypatch):
    """The chunks before it were loaded or are vLLM's prefix cache (#111)."""
    worker, kernel, allocs = _store_worker(monkeypatch, url_count=1)
    load = MaruReqMeta("hit", list(range(24)), list(range(6)), False)
    metadata = _resumed_store(load, computed=12)
    metadata.requests[0].store_start_token = 12
    handler = worker._handler
    worker._store_packed_slabs_write_behind(kernel, metadata)
    worker.shutdown()
    # Chunk 1 holds token 12: the request computed part of it.
    assert len(allocs) == 2
    assert handler.batch_store.call_args.args[0] == _req_chunk_keys(load, 8)[1:]


def test_write_behind_skips_the_chunk_a_full_hit_holds_back(monkeypatch):
    """vLLM computes the held-back block again, but Maru has its chunk."""
    worker, kernel, allocs = _store_worker(monkeypatch, url_count=1)
    load = MaruReqMeta("hit", list(range(24)), list(range(6)), False)
    metadata = _resumed_store(load, computed=20)
    metadata.requests[0].store_start_token = 24
    handler = worker._handler
    worker._store_packed_slabs_write_behind(kernel, metadata)
    worker.shutdown()
    assert not allocs
    handler.batch_store.assert_not_called()


@pytest.mark.parametrize("request_id", ["another", "loaded"])
def test_loaded_keys_are_not_permanently_skipped_by_later_requests(
    monkeypatch, request_id
):
    worker, kernel, allocs, load, keys = _loaded_store_worker(monkeypatch, tokens=16)
    handler = worker._handler
    worker._store_packed_slabs_write_behind(kernel, _resumed_store(load))
    assert not allocs
    worker.get_finished_saving({load.req_id})
    assert not worker._loaded_request_keys
    # A later request rechecks the pool. If its load misses, its recomputed
    # KV must be eligible even with the same request id or the same keys.
    worker._store_packed_slabs_write_behind(
        kernel, _resumed_store(load, request_id=request_id, computed=0)
    )
    worker.shutdown()
    assert len(allocs) == 2
    assert handler.batch_store.call_args.args[0] == keys


@pytest.mark.parametrize(
    "field", ["preempted_req_ids", "finished_req_ids", "recv_only_finished_req_ids"]
)
def test_request_cleanup_discards_its_loaded_store_keys(monkeypatch, field):
    worker, _, _, load, _ = _loaded_store_worker(monkeypatch, tokens=16)
    assert worker._loaded_request_keys[load.req_id][1]
    worker.handle_preemptions(MaruConnectorMetadata(**{field: {load.req_id}}))
    assert not worker._loaded_request_keys
    worker.shutdown()


def test_aborting_a_queued_loader_prevents_late_store_skip_state(monkeypatch):
    worker, _, _, load, _ = _loaded_store_worker(monkeypatch, tokens=16)
    cache = next(iter(worker._kv_caches.values()))
    # Capture the real submit gate before its queued job can start.
    worker._deferred_executor = SimpleNamespace(submit=Mock())
    worker._kv_caches = {"layer": SimpleNamespace(device=torch.device("cuda"))}
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)
    assert worker._try_submit_deferred_load(load)
    assert load.req_id in worker._inflight_deferred_req_ids
    worker.handle_preemptions(
        MaruConnectorMetadata(recv_only_finished_req_ids={load.req_id})
    )
    # It starts after the abort, and every copy succeeds. The identity
    # registered at submit was removed, so completion must not recreate it.
    worker._load_leased_request([("layer", cache, 0)], load, make_flash_attn_metadata())
    assert not worker.take_failed_load_blocks()
    assert not worker._loaded_request_keys
    worker._deferred_executor = None
    worker.shutdown()
