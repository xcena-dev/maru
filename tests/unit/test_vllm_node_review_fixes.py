# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Group copy lifetimes, node publication and loader failure accounting on CPU."""

import contextlib
import dataclasses
import gc
import weakref
from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")
import torch

import maru_vllm.connector as connector_module
from maru_common.storage_types import StorageUnavailableError
from maru_handler.client import CreditUnavailableError
from maru_handler.storage.cpu import CpuReadLease
from maru_vllm.connector import MaruConnectorMetadata, MaruReqMeta
from maru_vllm.kv_groups import (
    GROUP_FULL,
    GROUP_MAMBA,
    GROUP_MLA,
    GroupCache,
    KVGroup,
    describe_kv_groups,
)
from tests.unit.test_vllm_kv_groups import (
    DSV41_BLOCKS_A,
    _dsv41_blocks,
    _dsv41_shared,
    _dsv41_worker,
    _store_meta,
    _unbounded_config,
    _unbounded_shared,
    _unbounded_worker,
    dsv41_config,
    dsv41_kv_caches,
    unbounded_kv_caches,
)
from tests.unit.test_vllm_node import (
    async_meta,
    behind_meta,
    fetching_loader,
    group_loader,
    in_flight,
    node_extra,
    pinning_worker,
    released,
    run,
    run_async_group_load,
    submit,
    wait_worker,
)
from tests.unit.vllm_connector_helpers import (
    cuda_error,
    holds,
    make_scheduler,
    make_worker,
)


def group_worker(kind=GROUP_FULL, layerwise=False):
    group = KVGroup(0, kind, "test", ("layer",), 4, 32, group_page_bytes=32)
    if kind == GROUP_FULL:
        tensor = torch.zeros(6, 2, 4, 1, 1)
    else:
        tensor = torch.zeros(6, 32, dtype=torch.uint8)
        if kind == GROUP_MAMBA:
            group = dataclasses.replace(group, state_bytes=16)
    worker = make_worker(4, 8, node_extra(maru_use_layerwise=layerwise))
    worker._multi_group = True
    worker._kv_groups = [group]
    layout = (
        worker._resolve_kv_layout({"layer": tensor}) if kind == GROUP_FULL else None
    )
    cache = GroupCache(group, (tensor,), layout, ((0, 32),))
    worker._group_caches = [cache]
    worker._group_kernel = lambda _: None
    worker._ensure_handler = lambda: None
    worker._handler = MagicMock()
    worker._handler.has_local.return_value = False
    worker._handler.retrieve_capacity.return_value = None
    worker._handler.alloc.side_effect = lambda n, **_: SimpleNamespace(
        buf=memoryview(bytearray(n))
    )
    meta = MaruReqMeta(
        "r1",
        list(range(8)),
        [1, 2],
        False,
        num_matched_chunks=1,
        group_block_ids=[[1, 2]],
        async_load=True,
    )
    return worker, cache, meta


def watch(watched, *tensors):
    watched.extend(weakref.ref(t) for t in tensors)


def assert_kept(worker, watched):
    gc.collect()
    assert watched
    assert all(ref() is not None and holds(worker, ref()) for ref in watched)


@pytest.mark.parametrize("kind", [GROUP_FULL, GROUP_MAMBA, GROUP_MLA])
def test_group_load_copy_errors_keep_the_actual_tensor_arguments(monkeypatch, kind):
    worker, cache, meta = group_worker(kind)
    nbytes = cache.group.object_bytes(8)
    read = CpuReadLease(memoryview(bytearray(nbytes)), "object", None)
    worker._group_load_plan = lambda _: (1, [(cache, 0, None)], ["object"])
    worker._batch_retrieve_all = lambda *a, **kw: [read]
    watched = []

    def fail(dst, src, *args, **kwargs):
        watch(watched, dst, src)
        raise cuda_error()

    if kind == GROUP_FULL:

        def scatter(dst, layout, slots, src):
            watch(watched, dst, slots, src)
            raise cuda_error()

        monkeypatch.setattr(connector_module, "_scatter_kv", scatter)
    else:
        monkeypatch.setattr(torch.Tensor, "copy_", fail)
    assert worker._load_group_leased(meta, torch.device("cpu"), None, False) is None
    assert worker._stopped is not None
    worker._handler.release_retrieved.assert_not_called()
    assert_kept(worker, watched)
    assert holds(worker, read)


@pytest.mark.parametrize("failure", ["kernel", "record", "none"])
def test_group_kernel_arguments_survive_launch_while_left_copying(monkeypatch, failure):
    worker, cache, meta = group_worker()
    read = CpuReadLease(memoryview(bytearray(cache.group.object_bytes(8))), "a", None)
    worker._group_load_plan = lambda _: (1, [(cache, 0, None)], ["a"])
    worker._batch_retrieve_all = lambda *a, **kw: [read]
    watched = []
    to = torch.Tensor.to
    monkeypatch.setattr(torch.Tensor, "to", lambda t, *a, **kw: to(t, *a, **kw).clone())

    def launch(src, ptrs, slots, *a, **kw):
        watch(watched, src, slots)
        if failure == "kernel":
            raise cuda_error()

    ops = SimpleNamespace(
        multi_layer_kv_transfer=launch, TransferDirection=SimpleNamespace(H2D="H2D")
    )
    worker._group_kernel = lambda _: (ops, torch.zeros(1), 6, 4, 1, "FMT")
    event = SimpleNamespace(query=lambda: False)

    def record():
        if failure == "record":
            raise cuda_error()
        return event

    stream = SimpleNamespace(record_event=record)
    worker._load_group_leased(meta, torch.device("cpu"), stream, True)
    worker._handler.release_retrieved.assert_not_called()
    if failure == "none":
        gc.collect()
        assert all(ref() is not None for ref in watched)
        event.query = lambda: True
        assert worker.get_finished_loading() == {"r1"}
        assert released(worker) == [read]
        gc.collect()
        assert all(ref() is None for ref in watched)
    else:
        assert worker._stopped is not None
        assert_kept(worker, watched)


@pytest.mark.parametrize(
    ("kind", "layerwise"),
    [(GROUP_FULL, False), (GROUP_MAMBA, False), (GROUP_MLA, False), (GROUP_MLA, True)],
)
def test_group_store_copy_errors_keep_the_actual_tensor_arguments(
    monkeypatch, kind, layerwise
):
    worker, _, meta = group_worker(kind, layerwise)
    meta = dataclasses.replace(meta, is_store=True, num_scheduled_tokens=8)
    watched = []

    def fail(dst, src, *a, **kw):
        watch(watched, dst, src)
        raise cuda_error()

    monkeypatch.setattr(torch.Tensor, "copy_", fail)
    with pytest.raises(connector_module._UnconfirmedCopyError):
        worker.wait_for_save(MaruConnectorMetadata(requests=[meta]))
    worker._handler.free.assert_not_called()
    worker._handler.store_async.assert_not_called()
    assert_kept(worker, watched)


def node_layer_writer():
    worker = _dsv41_worker(
        dsv41_kv_caches(_dsv41_shared()), extra_config={"maru_use_layerwise": True}
    )
    worker._node_mode = True
    worker._handler = MagicMock()
    worker._handler.has_local.return_value = False
    worker._handler.alloc.side_effect = lambda n, **_: SimpleNamespace(
        buf=memoryview(bytearray(n))
    )
    writes, committed = [], set()

    def store(keys, handles):
        future = Future()
        writes.append((keys, handles, future))
        return future

    worker._handler.store_async.side_effect = store
    metadata = _store_meta(list(range(80)), _dsv41_blocks(DSV41_BLOCKS_A), 0, 64)

    def complete(i, results):
        keys, _, future = writes[i]
        committed.update(k for k, ok in zip(keys, results, strict=True) if ok)
        future.set_result(results)

    return worker, metadata, writes, committed, complete


def test_node_group_markers_follow_each_chunks_write_done_and_scheduler_hits():
    worker, metadata, writes, committed, complete = node_layer_writer()
    worker.wait_for_save(metadata)
    assert len(writes) == 2 and all(len(keys) == 8 for keys, _, _ in writes)
    assert worker._handler.alloc.call_count == 18
    # Each chunk reserves its marker before any of its layers.
    assert [c.args[0] for c in worker._handler.alloc.call_args_list][::9] == [1, 1]
    scheduler = make_scheduler(
        16,
        32,
        {"maru_use_layerwise": True},
        kv_groups=describe_kv_groups(dsv41_config(side="scheduler")),
    )
    scheduler._handler = MagicMock()
    scheduler._handler.batch_exists.side_effect = lambda keys, **_: [
        key in committed for key in keys
    ]

    def lookup():
        return scheduler._count_matched_group_chunks(
            "r2", metadata.requests[0].token_ids
        )

    assert lookup() == 0
    complete(1, [True] * 8)
    assert len(writes) == 3 and writes[2][0][0].endswith("_DONE")
    assert lookup() == 0
    complete(2, [True])
    assert lookup() == 0  # the first chunk's answer has not arrived
    complete(0, [True] * 8)
    assert len(writes) == 4 and writes[3][0][0].endswith("_DONE")
    complete(3, [True])
    assert lookup() == 2
    assert not worker._pending_store_keys


def test_a_failed_group_layer_blocks_only_its_own_marker():
    worker, metadata, writes, _, complete = node_layer_writer()
    worker.wait_for_save(metadata)
    complete(0, [True] * 7 + [False])
    assert len(writes) == 2
    assert worker._handler.free.call_count == 1
    complete(1, [True] * 8)
    assert len(writes) == 3
    complete(2, [True])
    assert not worker._pending_store_keys


@pytest.mark.parametrize("success", [True, False])
def test_a_late_group_reply_after_stop_keeps_the_marker_and_waiters(success):
    worker, metadata, writes, _, complete = node_layer_writer()
    worker.wait_for_save(metadata)
    from tests.unit.test_vllm_node import wait_on

    wait_on(worker, "r2", writes[0][0][0])
    marker = worker._pending_store_keys - set(writes[0][0]) - set(writes[1][0])
    assert len(marker) == 2
    worker._stop_if_unconfirmed(cuda_error())
    complete(0, [success] * 8)
    complete(1, [success] * 8)
    assert len(writes) == 2
    worker._handler.free.assert_not_called()
    assert "r2" in worker._request_pending_store_keys
    assert not worker._finished_store_requests


@pytest.mark.parametrize("pages", [0, 4])
def test_group_marker_credit_is_reserved_before_any_layer_is_published(pages):
    worker, metadata, writes, _, _ = node_layer_writer()
    allocated = []

    def alloc(n, **kwargs):
        if len(allocated) >= pages:
            raise CreditUnavailableError("no credit")
        handle = SimpleNamespace(buf=memoryview(bytearray(n)))
        allocated.append(handle)
        return handle

    worker._handler.alloc.side_effect = alloc
    worker.wait_for_save(metadata)
    assert not writes
    assert worker._handler.free.call_count == pages
    assert not worker._pending_store_keys and worker._stopped is None


def test_shared_group_chunks_are_published_once_with_immediate_replies():
    worker, metadata, writes, _, complete = node_layer_writer()
    store = worker._handler.store_async.side_effect

    def immediate(keys, handles):
        future = store(keys, handles)
        complete(len(writes) - 1, [True] * len(keys))
        return future

    worker._handler.store_async.side_effect = immediate
    metadata.requests.append(dataclasses.replace(metadata.requests[0], req_id="r2"))
    worker.wait_for_save(metadata)
    assert len(writes) == 4 and worker._handler.alloc.call_count == 18


@pytest.mark.parametrize("group", [True, False])
@pytest.mark.parametrize("failure", ["job", "submit"])
def test_loader_failures_finish_the_request_and_its_accounting(group, failure):
    worker = pinning_worker()
    worker._multi_group = group
    meta = async_meta("r1")
    if group:
        meta = dataclasses.replace(meta, group_block_ids=[meta.block_ids])
    worker._inflight_async_load_req_ids.add("r1")
    worker._abandoned_req_ids.add("r1")
    job = MagicMock()

    if failure == "submit":
        worker._async_load_executor = MagicMock()
        worker._async_load_executor.submit.side_effect = RuntimeError("executor closed")
        worker._submit_load_job(meta, job)
    else:
        worker._loader_jobs = 1
        job.side_effect = StorageUnavailableError("catalog closed")
        worker._run_load_job(meta, job)
    assert worker._loader_jobs == 0 and not worker._inflight_async_load_req_ids
    assert not worker._abandoned_req_ids and worker._stopped is None
    assert worker.get_finished_loading() == {"r1"}
    if group:
        assert worker.take_failed_recv_req_ids({"r1"}) == {"r1"}
    else:
        assert worker.take_failed_load_blocks()
    if failure != "job":
        job.assert_not_called()


@pytest.mark.parametrize("ready", [0, 1, 2])
@pytest.mark.parametrize("cuda_drain_error", [False, True])
@pytest.mark.parametrize("group", [False, True])
def test_node_event_record_failure_drains_and_cleans_the_whole_batch(
    monkeypatch, ready, cuda_drain_error, group
):
    worker, kernel, credit, published = wait_worker(monkeypatch)
    alloc = credit.alloc
    calls = 0

    def allocate(n, **kwargs):
        nonlocal calls
        calls += 1
        if calls <= ready:
            credit.at_hand.set()
        else:
            credit.at_hand.clear()
        return alloc(n, **kwargs)

    worker._handler.alloc.side_effect = allocate

    class BadRecord:
        def record(self, stream):
            raise RuntimeError("invalid event")

    monkeypatch.setattr(torch.cuda, "Event", BadRecord)
    stream = torch.cuda.Stream()
    drains = []

    def drain():
        drains.append(True)
        if cuda_drain_error:
            raise cuda_error()

    stream.synchronize = drain
    metadata = behind_meta("r1")
    if group:
        _, cache, _ = group_worker()
        worker._group_caches = [cache]
        worker._multi_group = True
        worker._group_kernel = lambda _: kernel
        metadata.requests[0].group_block_ids = [metadata.requests[0].block_ids]
        if cuda_drain_error:
            with pytest.raises(connector_module._UnconfirmedCopyError):
                worker.wait_for_save(metadata)
        else:
            worker.wait_for_save(metadata)
    else:
        worker._store_packed_slabs_write_behind(kernel, metadata)
    assert drains == [True] and not published
    assert worker._store_executor is None
    if cuda_drain_error:
        assert worker._stopped is not None
        assert "r1" in worker._request_pending_store_keys
        assert worker._credit_wait_bytes == (2 - ready) * 64
        worker._handler.free.assert_not_called()
    else:
        assert worker._stopped is None and worker._credit_wait_bytes == 0
        assert worker._handler.free.call_count == ready
        assert not worker._pending_store_keys and not worker._store_key_waiters
        assert worker.get_finished_saving({"r1"}) == {"r1"}
        worker.shutdown()
        assert worker._credit_wait_bytes == 0


@pytest.mark.parametrize("bounded", [True, False])
@pytest.mark.parametrize("layerwise", [True, False])
def test_deepseek_step_credit_counts_only_stored_layers_and_markers(bounded, layerwise):
    extra = {
        "maru_use_layerwise": layerwise,
        "_maru_step_tokens": 64,
        "_maru_step_seqs": 1,
    }
    if bounded:
        worker = _dsv41_worker(dsv41_kv_caches(_dsv41_shared()), extra_config=extra)
        groups = describe_kv_groups(dsv41_config())
    else:
        worker = _unbounded_worker(
            unbounded_kv_caches(_dsv41_shared(), _unbounded_shared()),
            extra_config=extra,
        )
        groups = describe_kv_groups(_unbounded_config())
    worker._node_mode = True
    worker._handler = MagicMock()
    expected = (18 if bounded else 106) if layerwise else (2 if bounded else 10)
    assert worker._kv_groups == groups
    assert worker._step_objects() == expected
    worker.want_step_credit(
        _store_meta(list(range(80)), _dsv41_blocks(DSV41_BLOCKS_A), 0, 64)
    )
    worker._handler.want_credit.assert_called_once_with(expected)


@pytest.mark.parametrize("slots", range(1, 8))
def test_group_load_uses_free_slots_before_waiting_for_async_load_copies(
    monkeypatch, slots
):
    worker, stream, log, reads = group_loader(
        monkeypatch, slots=max(2, slots), pipeline=None
    )
    assert connector_module._LOAD_PIPELINE_CHUNKS == 8
    run_async_group_load(worker, "r1")
    first = stream.events[0]
    assert log == ["read 0", "copy r1"]
    worker._handler.retrieve_capacity.side_effect = lambda _wanted=None: (
        slots - len(reads) + len(released(worker))
    )
    worker._group_load_plan = lambda _: (
        1,
        [(None, i, None) for i in range(5)],
        list("abcde"),
    )
    run_async_group_load(worker, "r2")
    if slots > 2:
        assert log[2:4] == ["read 2", "copy r2"]
        assert len(worker._handler.batch_retrieve.call_args_list[1].args[0]) == min(
            5, slots - 2
        )
        if slots < 7:
            assert log.index("read 2") < log.index("wait e0")
        else:
            assert not first.ended
    else:
        assert log[2:4] == ["wait e0", "release [0, 1]"]
        assert log.index("wait e0") < log.index("read 2")
    if slots == 3:
        assert log == [
            "read 0",
            "copy r1",
            "read 2",
            "copy r2",
            "wait e0",
            "release [2]",
            "release [0, 1]",
            "read 3",
            "copy r2",
            "wait e0",
            "release [3, 4, 5]",
            "read 6",
            "copy r2",
        ]
    for event in stream.events:
        event.ended = True
    assert worker.get_finished_loading() == {"r1", "r2"}
    assert len(reads) == 7
    assert sorted(map(id, released(worker))) == sorted(map(id, reads))


def test_a_queued_copy_error_is_logged_without_a_second_drain(monkeypatch, caplog):
    worker = pinning_worker()
    meta = async_meta("r1")
    reads = [CpuReadLease(memoryview(bytearray(4)), "a", None)]
    stream = MagicMock()
    stream.synchronize.side_effect = [RuntimeError("wait failed"), None]
    worker._layerwise_streams = [stream]
    worker._layerwise_stream_for = lambda _: stream
    worker._pin_slot_mapping_for_async_h2d = lambda t: t
    worker._copy_chunkwise_layer_to_device = MagicMock(
        side_effect=RuntimeError("copy failed")
    )
    monkeypatch.setattr(torch.cuda, "set_device", lambda _: None)
    monkeypatch.setattr(torch.cuda, "stream", lambda _: contextlib.nullcontext())
    with pytest.raises(
        connector_module._QueuedCopyError, match="copy failed"
    ) as caught:
        worker._issue_layerwise_copies_offthread(
            meta,
            2,
            torch.arange(16),
            reads,
            [("layer", torch.zeros(1), 0)],
            torch.device("cpu"),
        )
    worker._async_load_failed(meta, caught.value, reads)
    assert stream.synchronize.call_count == 1
    worker._handler.release_retrieved.assert_not_called()
    assert worker.get_finished_loading() == {"r1"}
    assert "r1: copy failed" in caplog.text
    assert worker._stopped is None


def test_answered_fetches_are_copied_before_older_unanswered_fetches(monkeypatch):
    worker, _, log, _, fetches = fetching_loader(monkeypatch, depth=4)
    r1, r2, _, _ = submit(worker, "r1", "r2", "r3", "r4")
    run(worker, r1, r2)
    fetches[1].answered = True
    worker._finish_node_reads(keep=2)
    assert "copy r2" in log and "copy r1" not in log
    assert in_flight(worker) == ["r1"]


def test_default_node_group_stores_reserve_no_device_slabs_or_wait_budget():
    worker, _, meta = group_worker()
    assert not worker._write_behind
    worker._reserve_credit_wait = MagicMock()
    worker.wait_for_save(
        MaruConnectorMetadata(
            requests=[dataclasses.replace(meta, is_store=True, num_scheduled_tokens=8)]
        )
    )
    worker._reserve_credit_wait.assert_not_called()
    worker._handler.alloc.assert_called_once()
    assert worker._handler.alloc.call_args.kwargs == {}
    assert worker._credit_wait_bytes == 0
