# SPDX-License-Identifier: Apache-2.0
"""Inline connector loads use the worker's reader and retain copying slots."""

import threading
from types import SimpleNamespace

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")
import torch

from maru_handler import MaruHandler
from maru_vllm.connector import (
    MaruConnectorMetadata,
    MaruReqMeta,
    _chunk_keys,
    _node_transfer_defaults,
)
from tests.unit.ms_node_helpers import PAGE, wait_until
from tests.unit.test_ms_region_extension import extension_env  # noqa: F401
from tests.unit.test_ms_remote_hints import world  # noqa: F401
from tests.unit.test_vllm_node import node_extra
from tests.unit.vllm_connector_helpers import cuda_error, holds, make_worker


@pytest.fixture
def inline(world):  # noqa: F811
    w = world()
    writer = w.client()
    reader = w.client(load_only=True)
    worker = make_worker(
        4,
        8,
        node_extra(w.raw["socket"], maru_async_load=False),
        num_kv_heads=1,
        head_size=PAGE // 64,
    )
    handler = MaruHandler(reader.config)
    handler._cpu = reader
    worker._handler = handler
    worker._num_layers = 1
    worker._page_size_bytes = PAGE
    yield w, writer, reader, worker
    if worker._stopped is None:
        worker.shutdown()
    else:
        # Fake copies have ended; release only the test's retained resources.
        worker._stopped = None
        reader._stopped.clear()
        reader._closing.clear()
        reader._rdma.stopped.clear()
        for flight in list(reader._rdma._flights):
            for ref in list(flight.consumers):
                ref.release()
        worker.shutdown()


def request(inline, chunks=2):
    w, writer, reader, worker = inline
    tokens = list(range(chunks * 8))
    keys = _chunk_keys(tokens, 8)
    values = []
    for i, key in enumerate(keys):
        wait_until(lambda: writer.credits.unused_bytes >= PAGE)
        slab = torch.full((2, 1, 8, PAGE // 64), float(i + 1))
        handle = writer.alloc(PAGE)
        handle.buf[:] = slab.numpy().tobytes()
        assert writer.batch_store([key], [handle]) == [True]
        values.append(slab)
    meta = MaruReqMeta(
        "r",
        tokens,
        list(range(chunks * 2)),
        False,
        num_matched_chunks=chunks,
        load_start_token=0,
        load_end_token=chunks * 8,
    )
    kv = torch.zeros(2, chunks * 2, 4, 1, PAGE // 64)
    context = SimpleNamespace(
        attn_metadata=None,
        no_compile_layers={
            "model.layers.0.self_attn": SimpleNamespace(kv_cache=kv),
        },
    )
    return meta, kv, context, values


def test_inline_remote_load_reads_on_the_engine_thread_and_reuses_no_copy(
    inline, monkeypatch
):
    w, writer, reader, worker = inline
    meta, kv, context, values = request(inline, chunks=6)
    engine = threading.get_ident()
    posts, releases = [], []
    original_post = reader._rdma.transport.post_nowait
    original_release = reader._rdma.release

    def post(operation, peer, pairs):
        assert operation == "READ"
        posts.append((threading.get_ident(), len(pairs)))
        return original_post(operation, peer, pairs)

    def release(ref):
        chunk = int(torch.frombuffer(ref.view, dtype=torch.float32)[0]) - 1
        assert torch.all(kv[:, chunk * 2 : chunk * 2 + 2] == chunk + 1)
        releases.append(chunk)
        return original_release(ref)

    monkeypatch.setattr(reader._rdma.transport, "post_nowait", post)
    monkeypatch.setattr(reader._rdma, "release", release)
    for _ in range(2):
        kv.zero_()
        worker.start_load_kv(context, MaruConnectorMetadata(requests=[meta]))
        for i in range(6):
            assert torch.all(kv[:, i * 2 : i * 2 + 2] == i + 1)
    assert all(thread == engine for thread, _ in posts)
    assert sum(count for _, count in posts) == 12
    assert releases == list(range(6)) * 2
    assert reader.retrieve_capacity() == reader._rdma.slot_count
    assert not worker.take_failed_load_blocks()


@pytest.mark.parametrize("error", [None, "io", "cuda"])
def test_inline_copy_refs_and_slot_ownership_survive_errors(inline, monkeypatch, error):
    w, writer, reader, worker = inline
    meta, kv, context, _ = request(inline)
    seen = []
    original = worker._inject_slab_layers

    def inject(*args, refs=None, **kwargs):
        reads = [
            ref
            for group in refs
            if isinstance(group, list)
            for ref in group
            if hasattr(ref, "flight")
        ]
        assert reads and all(not r._released for r in reads)
        assert reader.retrieve_capacity() == reader._rdma.slot_count - 2
        seen.extend(reads)
        if error:
            raise cuda_error() if error == "cuda" else RuntimeError("bad layout")
        return original(*args, refs=refs, **kwargs)

    monkeypatch.setattr(worker, "_inject_slab_layers", inject)
    if error == "cuda":
        from maru_vllm.connector import _UnconfirmedCopyError

        with pytest.raises(_UnconfirmedCopyError):
            worker.start_load_kv(context, MaruConnectorMetadata(requests=[meta]))
        assert worker._stopped and reader._stopped.is_set()
        assert all(not r._released and holds(worker, r) for r in seen)
        assert reader._rdma._flights
    else:
        worker.start_load_kv(context, MaruConnectorMetadata(requests=[meta]))
        assert worker._stopped is None
        assert seen and all(r._released for r in seen)
        if error:
            assert worker.take_failed_load_blocks() == {0, 1, 2, 3}


def test_an_inline_remote_read_error_is_a_miss_and_does_not_stop_the_worker(inline):
    w, writer, reader, worker = inline
    meta, kv, context, _ = request(inline)
    reader._rdma.transport.fail_next()
    worker.start_load_kv(context, MaruConnectorMetadata(requests=[meta]))
    assert worker._stopped is None and not kv.any()
    assert worker.take_failed_load_blocks() == {0, 1, 2, 3}
    assert reader._rdma._flights


def test_async_scheduling_still_refuses_inline_remote_loads():
    extra = node_extra(maru_async_load=False, maru_async_store=True)
    with pytest.raises(ValueError, match="--no-async-scheduling"):
        _node_transfer_defaults(extra, async_scheduling=True)
    assert (
        _node_transfer_defaults(extra, async_scheduling=False)["maru_async_load"]
        is False
    )


def test_inline_load_does_not_wait_for_slots_between_batches(inline, monkeypatch):
    w, writer, reader, worker = inline
    meta, kv, context, _ = request(inline, chunks=6)
    calls = []
    real = worker._leased_batch_size
    original = reader.retrieve_capacity
    capacity_calls = []

    def capacity(wanted=None):
        capacity_calls.append(wanted)
        return original(wanted) if len(capacity_calls) == 1 else 0

    def size(handler, wanted, offthread):
        calls.append(offthread)
        return real(handler, wanted, offthread)

    monkeypatch.setattr(reader, "retrieve_capacity", capacity)
    monkeypatch.setattr(worker, "_leased_batch_size", size)
    worker.start_load_kv(context, MaruConnectorMetadata(requests=[meta]))
    assert calls == [False]
    assert worker.take_failed_load_blocks() == {8, 9, 10, 11}
    assert kv[:, :8].any() and not kv[:, 8:].any()
