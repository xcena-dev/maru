# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tensor parallelism and several KV cache groups on the remote_cxl backend.

Each TP rank opens its own storage session, and every session of a namespace
finds and reads what any of them stored. A read holds one of the reader's
staging slots under a lease until it is released. ``SessionServer`` does the
same at the MaruHandler boundary.

CPU tensors only: loads and stores take their per-layer paths.
"""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import torch
from vllm.distributed.kv_transfer.kv_connector.v1.base import KVConnectorRole

import maru
import maru_vllm.connector as connector_module
from maru_common.kv_shard import tp_kv_shard
from maru_vllm.connector import (
    MaruKVConnector,
    _chunk_keys,
    _cpu_engine_config,
    _create_maru_handler,
)
from maru_vllm.kv_groups import describe_kv_groups, group_object_key
from tests.unit.test_vllm_cpu_config import engine_config
from tests.unit.test_vllm_cpu_config import extra as cpu_extra
from tests.unit.test_vllm_kv_groups import (
    _FORWARD,
    B_BLOCKS,
    FAILED_PARKED,
    QWEN_BLOCKS,
    TINY_BLOCK,
    _HeldExecutor,
    _load_meta,
    _reported_failures,
    _shared,
    _state,
    _store_meta,
    _stored_qwen,
    _storing_worker,
    _token_rows,
    gpt_oss_config,
    tiny_config,
    tiny_kv_caches,
)
from tests.unit.test_vllm_remote_cxl_config import extra as remote_cxl_extra
from tests.unit.test_vllm_tp import (
    BLOCK,
    CHUNK,
    HEAD_SIZE,
    LAYER_NAMES,
    PROMPT,
    assert_same_kv,
    load_step,
    rank_kv,
    store_step,
)
from tests.unit.vllm_connector_helpers import make_scheduler, make_worker


class Lease:
    """A read lease on one object's bytes in a staging slot."""

    def __init__(self, data: bytes) -> None:
        self.view = memoryview(bytearray(data))
        self.released = False


class SessionServer:
    """MaruServer's remote CXL storage sessions, at the MaruHandler boundary.

    Every session of the namespace finds every key, whichever rank stored
    it, and the first store of a key wins. A read takes one of the reader's
    staging slots until its lease is released. Remembers the route key of
    every call, which picks the pool when there are several.
    """

    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = dict(objects or {})
        self.routes: set[str | None] = set()

    def handler(self, slots: int = 64) -> MagicMock:
        """A MaruHandler stand-in for one rank's session."""
        handler = MagicMock()
        handler.free_slots = slots
        handler.leases = []
        committed: set[str] = set()

        def alloc(nbytes, route_key=None):
            self.routes.add(route_key)
            return SimpleNamespace(buf=memoryview(bytearray(max(nbytes, 1))))

        def batch_store(keys, handles):
            for key, handle in zip(keys, handles, strict=True):
                self.objects.setdefault(key, bytes(handle.buf))
                committed.add(key)
            return [True] * len(keys)

        def batch_exists(keys, route_key=None):
            self.routes.add(route_key)
            return [key in self.objects for key in keys]

        def batch_retrieve(keys, route_key=None):
            self.routes.add(route_key)
            out = []
            for key in keys:
                if key not in self.objects or handler.free_slots == 0:
                    out.append(None)
                    continue
                handler.free_slots -= 1
                handler.leases.append(Lease(self.objects[key]))
                out.append(handler.leases[-1])
            return out

        def release_retrieved(infos):
            for info in infos:
                if info is not None and not info.released:
                    info.released = True
                    handler.free_slots += 1

        handler.alloc.side_effect = alloc
        handler.batch_store.side_effect = batch_store
        handler.batch_exists.side_effect = batch_exists
        handler.batch_retrieve.side_effect = batch_retrieve
        handler.release_retrieved.side_effect = release_retrieved
        handler.retrieve_capacity.side_effect = lambda: handler.free_slots
        handler.storage_gpu_accessible.return_value = False
        handler.has_local.side_effect = committed.__contains__
        return handler


def remote_vllm_config(
    tp_size: int = 1,
    total_heads: int = 8,
    *,
    block_size: int = BLOCK,
    extra: dict | None = None,
    **parallel,
) -> SimpleNamespace:
    """The parts of VllmConfig a remote_cxl connector reads."""
    return SimpleNamespace(
        kv_transfer_config=SimpleNamespace(
            kv_connector_extra_config=remote_cxl_extra() if extra is None else extra,
            kv_load_failure_policy="recompute",
        ),
        scheduler_config=SimpleNamespace(async_scheduling=False),
        cache_config=SimpleNamespace(
            block_size=block_size,
            num_gpu_blocks=None,
            cache_dtype="auto",
            mamba_cache_mode="align",
        ),
        parallel_config=SimpleNamespace(tensor_parallel_size=tp_size, **parallel),
        model_config=SimpleNamespace(
            use_mla=False,
            max_model_len=4096,
            enforce_eager=True,
            revision="commit",
            dtype="float16",
            hf_config=SimpleNamespace(to_dict=lambda: {"heads": total_heads}),
            get_total_num_kv_heads=lambda: total_heads,
            get_num_kv_heads=lambda pc: max(1, total_heads // pc.tensor_parallel_size),
            get_head_size=lambda: HEAD_SIZE,
        ),
        speculative_config=None,
        num_lookahead_tokens=0,
    )


# =============================================================================
# Startup
# =============================================================================


def test_each_rank_opens_its_own_session_and_the_scheduler_asks_as_rank_0(
    monkeypatch,
):
    """MaruServer keeps one open session per engine id, and answers a lookup
    only for the engine id of an open session."""
    monkeypatch.setattr(connector_module, "_tp_rank", lambda: 1)
    config = remote_vllm_config(2, 8)
    worker = MaruKVConnector(config, KVConnectorRole.WORKER)._worker
    scheduler = MaruKVConnector(config, KVConnectorRole.SCHEDULER)._scheduler
    assert worker._kv_shard.suffix == "_h4-8"
    assert worker._extra_config["maru_engine_id"] == "worker-0-tp1"
    assert connector_module._get_knob(worker._extra_config, "maru_async_load")
    assert scheduler._shard_suffixes == ("_h0-4", "_h4-8")
    assert scheduler._extra_config["maru_engine_id"] == "worker-0-tp0"
    assert (
        worker._extra_config["maru_cache_namespace"]
        == scheduler._extra_config["maru_cache_namespace"]
    )


def test_the_scheduler_looks_up_in_the_workers_namespace(monkeypatch):
    """A model can change its hf_config where a worker loads it (vLLM's
    Transformers backend writes the dtype it runs: float32 tiny_starcoder_py
    runs as bfloat16). Under TP the scheduler's process loads no model and
    computes another namespace, so it takes the workers' from vLLM's
    handshake, before it takes a request."""
    import pickle

    from maru_vllm.connector import MaruHandshakeMetadata

    monkeypatch.setattr(connector_module, "_tp_rank", lambda: 1)
    worker = MaruKVConnector(remote_vllm_config(2, 8), KVConnectorRole.WORKER)
    loaded = remote_vllm_config(2, 8)
    loaded.model_config.hf_config = SimpleNamespace(
        to_dict=lambda: {"heads": 8, "dtype": "float32"}
    )
    scheduler = MaruKVConnector(loaded, KVConnectorRole.SCHEDULER)
    ours = scheduler._scheduler._extra_config["maru_cache_namespace"]
    meta = worker.get_handshake_metadata()
    # vLLM pickles it from each worker process to the engine core.
    meta = pickle.loads(pickle.dumps(meta))
    assert isinstance(meta, MaruHandshakeMetadata)
    assert meta.cache_namespace != ours
    stale = MagicMock()
    scheduler._scheduler._handler = stale
    scheduler._scheduler._known_keys.add("kv_x")

    # vLLM 0.32 keys it by (pp rank, tp rank).
    scheduler.set_xfer_handshake_metadata_pp_aware({(0, 0): meta, (0, 1): meta})

    sched = scheduler._scheduler
    assert sched._extra_config["maru_cache_namespace"] == meta.cache_namespace
    assert sched._handler is None and sched._known_keys == set()
    stale.close.assert_called_once()


def test_ranks_storing_in_different_namespaces_stop_the_engine():
    from maru_vllm.connector import MaruHandshakeMetadata

    scheduler = MaruKVConnector(remote_vllm_config(2, 8), KVConnectorRole.SCHEDULER)
    with pytest.raises(ValueError, match="different cache namespaces"):
        scheduler.set_xfer_handshake_metadata(
            {0: MaruHandshakeMetadata("a"), 1: MaruHandshakeMetadata("b")}
        )


def test_another_connectors_handshake_is_said_and_ignored(caplog):
    """vLLM's MultiConnector passes on one connector's metadata only."""
    import logging

    scheduler = MaruKVConnector(remote_vllm_config(2, 8), KVConnectorRole.SCHEDULER)
    ours = scheduler._scheduler._extra_config["maru_cache_namespace"]
    with caplog.at_level(logging.WARNING, logger="maru_vllm.connector"):
        scheduler.set_xfer_handshake_metadata({0: object(), 1: object()})
    assert "another connector's handshake metadata" in caplog.text
    assert scheduler._scheduler._extra_config["maru_cache_namespace"] == ours


def test_the_cxl_backend_sends_no_namespace(monkeypatch):
    monkeypatch.setattr(connector_module, "_tp_rank", lambda: 1)
    config = remote_vllm_config(2, 8, extra={"maru_instance_id": "inst1"})
    assert (
        MaruKVConnector(config, KVConnectorRole.WORKER).get_handshake_metadata() is None
    )


def test_tp1_keeps_its_engine_id(monkeypatch):
    def no_rank():
        raise AssertionError("TP=1 must not query the TP group")

    monkeypatch.setattr(connector_module, "_tp_rank", no_rank)
    config = remote_vllm_config(1, 8)
    worker = MaruKVConnector(config, KVConnectorRole.WORKER)._worker
    scheduler = MaruKVConnector(config, KVConnectorRole.SCHEDULER)._scheduler
    assert worker._extra_config["maru_engine_id"] == "worker-0"
    assert scheduler._extra_config["maru_engine_id"] == "worker-0"


@pytest.mark.parametrize(
    "parallel", [{"pipeline_parallel_size": 2}, {"data_parallel_size": 2}]
)
def test_remote_cxl_still_refuses_pipeline_and_data_parallelism(parallel):
    with pytest.raises(ValueError, match="PP=DP=1"):
        MaruKVConnector(remote_vllm_config(2, 8, **parallel), KVConnectorRole.SCHEDULER)


def test_remote_cxl_takes_several_kv_cache_groups():
    connector = MaruKVConnector(
        remote_vllm_config(block_size=16), KVConnectorRole.SCHEDULER, gpt_oss_config()
    )
    assert connector._scheduler._multi_group
    assert connector._scheduler._deferred_loading


def test_cpu_storage_still_takes_one_kv_cache_group():
    config = remote_vllm_config(block_size=16, extra=cpu_extra())
    with pytest.raises(ValueError, match="one KV cache group"):
        MaruKVConnector(config, KVConnectorRole.SCHEDULER, gpt_oss_config())


def test_namespace_parts_tp_sizes_and_group_layouts_but_keeps_tp1():
    """A rank's objects hold its slice, and a model stored by group holds
    other objects than the same model stored as one group: MaruServer refuses
    a session whose objects differ from its namespace's. A one-group TP=1
    engine keeps the namespace it had."""
    config = engine_config()

    def namespace(groups=()):
        return _cpu_engine_config(remote_cxl_extra(), config, groups)[
            "maru_cache_namespace"
        ]

    tp1 = namespace()
    groups = describe_kv_groups(gpt_oss_config())
    assert namespace(groups[:1]) == tp1
    assert namespace(groups) != tp1
    config.parallel_config.tensor_parallel_size = 2
    assert namespace() != tp1
    assert namespace() == namespace()  # every rank of the engine


def test_remote_cxl_serves_the_text_requests_of_a_multimodal_model():
    """Requests with multimodal inputs skip the cache on every backend
    (_uncacheable_request), so Qwen3.5 runs on remote_cxl; CPU M1 still
    refuses it. LoRA and MLA stay refused."""
    config = engine_config()
    config.model_config.is_multimodal_model = True
    assert _cpu_engine_config(remote_cxl_extra(), config)["maru_cache_namespace"]
    with pytest.raises(ValueError, match="text-only"):
        _cpu_engine_config(cpu_extra(), config)
    config.model_config.use_mla = True
    with pytest.raises(ValueError, match="without LoRA or MLA"):
        _cpu_engine_config(remote_cxl_extra(), config)
    config.model_config.use_mla = False
    config.lora_config = object()
    with pytest.raises(ValueError, match="without LoRA or MLA"):
        _cpu_engine_config(remote_cxl_extra(), config)


def test_a_rank_that_stores_nothing_opens_a_one_page_session(monkeypatch):
    """#132 on remote_cxl: its region is one page, not maru_pool_size."""
    seen = {}

    class FakeHandler:
        def __init__(self, cfg):
            seen["cfg"] = cfg

        def connect(self):
            return True

    monkeypatch.setattr(maru, "MaruHandler", FakeHandler)
    _create_maru_handler(
        remote_cxl_extra(maru_chunk_size="1M"), pool_size_override=1024**2
    )
    assert seen["cfg"].pool_size == 1024**2
    _create_maru_handler(remote_cxl_extra(maru_chunk_size="1M"))
    assert seen["cfg"].pool_size == 16 * 1024**2


# =============================================================================
# Tensor parallelism
# =============================================================================


def session_rank(server: SessionServer, shard, slots: int = 64):
    worker = make_worker(
        BLOCK, CHUNK, remote_cxl_extra(), head_size=HEAD_SIZE, kv_shard=shard
    )
    worker._num_layers = len(LAYER_NAMES)
    worker._ensure_handler = lambda: None
    worker._handler = server.handler(slots)
    return worker


@pytest.mark.parametrize(("tp_size", "total_heads"), [(2, 4), (4, 4)])
def test_each_rank_stores_and_loads_its_own_heads(tp_size, total_heads):
    server = SessionServer()
    heads = total_heads // tp_size
    shards = [tp_kv_shard(rank, tp_size, total_heads) for rank in range(tp_size)]
    workers = [session_rank(server, shard, slots=1) for shard in shards]
    kvs = [rank_kv(rank + 1, heads) for rank in range(tp_size)]

    for worker, kv in zip(workers, kvs, strict=True):
        store_step(worker, kv)

    base = _chunk_keys(list(range(PROMPT)), CHUNK)
    assert set(server.objects) == {key + s.suffix for key in base for s in shards}
    for worker, kv in zip(workers, kvs, strict=True):
        assert_same_kv(load_step(worker, heads), kv)
        assert worker.take_failed_load_blocks() == set()
        assert worker._handler.free_slots == 1
    # Every rank's objects of the request go to the pool of its bare key.
    assert server.routes == {base[0]}


def test_lookup_counts_a_chunk_once_every_rank_slice_is_stored():
    tokens = list(range(3 * CHUNK))
    k0, k1, k2 = _chunk_keys(tokens, CHUNK)
    slices = ("_h0-4", "_h4-8")
    server = SessionServer({k + s: b"" for k in (k0, k1, k2) for s in slices})
    del server.objects[k1 + "_h4-8"]
    scheduler = make_scheduler(BLOCK, CHUNK, remote_cxl_extra(), shard_suffixes=slices)
    scheduler._handler = server.handler()

    assert scheduler._count_matched_chunks(tokens) == 1
    # Residency follows the sessions: every lookup asks again.
    assert scheduler._count_matched_chunks(tokens) == 1
    assert scheduler._handler.batch_exists.call_count == 2
    assert server.routes == {k0}
    # Keys without a slice, as every rank wrote them before #117, never hit.
    server.objects = dict.fromkeys((k0, k1, k2), b"")
    assert scheduler._count_matched_chunks(tokens) == 0


def test_a_short_lookup_reply_leaves_the_rest_missing():
    tokens = list(range(2 * CHUNK))
    scheduler = make_scheduler(
        BLOCK, CHUNK, remote_cxl_extra(), shard_suffixes=("_h0-4", "_h4-8")
    )
    scheduler._handler = MagicMock()
    scheduler._handler.batch_exists.return_value = [True, True, True]
    assert scheduler._count_matched_chunks(tokens) == 1


# =============================================================================
# Several KV cache groups
# =============================================================================


def test_group_lookup_asks_again_in_the_pool_of_the_request():
    _, _, store = _stored_qwen()
    server = SessionServer(store)
    scheduler = make_scheduler(
        TINY_BLOCK,
        TINY_BLOCK,
        remote_cxl_extra(),
        kv_groups=describe_kv_groups(tiny_config()),
    )
    scheduler._handler = server.handler()
    tokens = list(range(40))
    assert scheduler._count_matched_group_chunks("b", tokens) == 2
    assert scheduler._count_matched_group_chunks("b", tokens) == 2
    assert scheduler._handler.batch_exists.call_count == 2
    assert scheduler._known_keys == set()
    assert server.routes == {_chunk_keys(tokens, TINY_BLOCK)[0]}


def session_group_worker(server: SessionServer, slots: int, **extra):
    target = torch.zeros_like(_shared())
    worker, _ = _storing_worker(
        tiny_kv_caches("0.30", target), extra=remote_cxl_extra(**extra)
    )
    worker._handler = server.handler(slots)
    return worker, target


@pytest.mark.parametrize("slots", [1, 2, 64])
def test_a_group_load_reads_what_staging_holds_and_releases_it(slots):
    source, stored, store = _stored_qwen()
    server = SessionServer(store)
    worker, target = session_group_worker(server, slots)
    tokens = list(range(40))
    worker.start_load_kv(_FORWARD, _load_meta(tokens, B_BLOCKS, 2, 32, 8))

    src = stored._group_caches[3].tensors
    dst = worker._group_caches[3].tensors
    for a_block, b_block in ((4, 7), (5, 8)):
        for a, b in zip(src, dst, strict=True):
            assert torch.equal(
                _token_rows(b, b_block, "0.30"), _token_rows(a, a_block, "0.30")
            )
    for g in range(3):
        for b_block in B_BLOCKS[g][1:]:
            for layer in range(2):
                assert torch.equal(
                    _state(target, layer, b_block),
                    _state(source, layer, QWEN_BLOCKS[g][1]),
                )
    assert worker.take_failed_load_blocks() == set()
    handler = worker._handler
    sizes = [len(call.args[0]) for call in handler.batch_retrieve.call_args_list]
    assert sum(sizes) == 5 and max(sizes) <= slots
    assert all(lease.released for lease in handler.leases)
    assert handler.free_slots == slots
    assert server.routes == {_chunk_keys(tokens, TINY_BLOCK)[0]}


def test_a_missing_group_object_fails_the_parked_load_and_frees_its_slots():
    _, _, store = _stored_qwen()
    k1 = _chunk_keys(list(range(40)), TINY_BLOCK)[1]
    del store[group_object_key(k1, 2, TINY_BLOCK)]
    worker, target = session_group_worker(SessionServer(store), 64)
    meta = _load_meta(list(range(40)), B_BLOCKS, 2, deferred_load=True)
    worker.start_load_kv(_FORWARD, meta)
    assert _reported_failures(worker) == FAILED_PARKED
    assert worker._handler.free_slots == 64


def test_a_group_load_without_a_free_slot_fails_over_its_whole_range():
    """Every group's blocks over the tokens the load covers, not those of
    its first group only."""
    _, _, store = _stored_qwen()
    worker, _ = session_group_worker(SessionServer(store), 0)
    worker.start_load_kv(_FORWARD, _load_meta(list(range(40)), B_BLOCKS, 2, 32, 8))
    worker._handler.batch_retrieve.assert_not_called()
    assert worker.take_failed_load_blocks() == {1, 3, 5, 7, 8}


def test_group_objects_are_written_on_the_write_behind_thread():
    """remote_cxl writes staged objects by RDMA in batch_store; the engine
    thread only copies them to staging, as vLLM frees sliding-window and
    Mamba blocks while the request runs."""
    server = SessionServer()
    worker, _ = _storing_worker(
        tiny_kv_caches("0.30", _shared()),
        extra=remote_cxl_extra(maru_async_store=True),
    )
    worker._handler = server.handler()
    worker._store_executor = _HeldExecutor()
    tokens = list(range(40))
    worker.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32))
    assert server.objects == {} and worker._handler.batch_store.call_count == 0
    # No request waits for them: their blocks were copied already.
    assert worker.get_finished_saving({"r1"}) == {"r1"}
    # Another request over the same prefix does not stage them again.
    staged = worker._handler.alloc.call_count
    worker.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32, req_ids=("r2",)))
    assert worker._handler.alloc.call_count == staged == 5
    worker._store_executor.run()
    assert len(server.objects) == 5
    assert worker._pending_store_keys == set()
    assert server.routes == {_chunk_keys(tokens, TINY_BLOCK)[0]}


def test_group_registration_names_its_objects_and_sizes_staging(monkeypatch):
    worker, _ = _storing_worker(
        tiny_kv_caches("0.30", _shared()),
        extra=remote_cxl_extra(_maru_max_model_len=1000),
    )
    schema = json.loads(worker._extra_config["_maru_storage_schema"])
    assert schema["format"] == "KV_GROUPS-v1"
    assert [g["kind"] for g in schema["groups"]] == ["mamba"] * 3 + ["full"]

    seen = {}
    monkeypatch.setattr(
        connector_module, "_create_maru_handler", lambda config: seen.update(config)
    )
    del worker._ensure_handler  # the real one
    worker._handler = None
    worker._ensure_handler()
    # A staging slot per object: 63 chunks of the attention group, and one
    # state per Mamba group.
    assert seen["_maru_request_chunks"] == 63 + 3
