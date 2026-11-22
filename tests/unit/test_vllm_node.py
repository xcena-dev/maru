# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruKVConnector on maru_node: the node's StorageService and MaruServer.

Config checks, transfer defaults, rank identity, and loads and stores through
a StorageService and the fake MaruServer. CPU tensors unless marked: loads
and stores take their per-layer paths. A CUDA error of a copy or a wait stops
the worker, which then gives nothing back (as test_vllm_unconfirmed_copies
pins down for every backend).
"""

import contextlib
import dataclasses
import functools
import gc
import threading
import time
import weakref
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from types import SimpleNamespace
from unittest import mock
from unittest.mock import MagicMock, Mock

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import torch
from vllm.distributed.kv_transfer.kv_connector.v1.base import KVConnectorRole

import maru_handler.client as client_module
import maru_vllm.connector as connector_module
from maru_common.kv_shard import WHOLE_MODEL, tp_kv_shard
from maru_common.node_protocol import AttachResponse, TransferDefaults
from maru_common.storage_types import Key, Status, StorageUnavailableError
from maru_handler.client import CreditUnavailableError
from maru_handler.memory.types import MemoryInfo
from maru_handler.storage.cpu import CpuReadLease
from maru_vllm.connector import (
    _KEPT_UNTIL_EXIT,
    _NODE_KEYS_ELSEWHERE,
    MaruConnectorMetadata,
    MaruHandshakeMetadata,
    MaruKVConnector,
    MaruReqMeta,
    _chunk_keys,
    _LoadMissError,
    _node_engine_config,
    _node_transfer_defaults,
    _UnconfirmedCopyError,
    _validate_storage_config,
)
from maru_vllm.kv_groups import describe_kv_groups, group_object_key
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import (
    PAGE,
    node_config,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)
from tests.unit.test_vllm_kv_groups import (
    _FORWARD,
    B_BLOCKS,
    QWEN_BLOCKS,
    TINY_BLOCK,
    _check_loaded,
    _engine_layout,
    _load_meta,
    _shared,
    _state,
    _store_meta,
    _token_rows,
    gpt_oss_config,
    tiny_config,
    tiny_kv_caches,
)
from tests.unit.test_vllm_remote_cxl_tp_groups import remote_vllm_config
from tests.unit.test_vllm_tp import (
    BLOCK,
    CHUNK,
    HEAD_SIZE,
    LAYER_NAMES,
    NUM_BLOCKS,
    NUM_LAYERS,
    PROMPT,
    assert_same_kv,
    load_into,
    rank_kv,
)
from tests.unit.vllm_connector_helpers import (
    FailingSyncStream,
    cuda_error,
    holds,
    kept_by,
    make_flash_attn_metadata,
    make_scheduler,
    make_worker,
)

SOCKET = "/run/maru/test-node.sock"


def node_extra(socket: str = SOCKET, **kwargs):
    return {
        "maru_node": socket,
        "maru_engine_id": "eng",
        "maru_cache_namespace": "ns",
        **kwargs,
    }


@pytest.fixture(autouse=True)
def no_cached_defaults(monkeypatch):
    monkeypatch.setattr(connector_module, "_node_transfer_cache", {})


@pytest.fixture
def fake():
    server = FakeMaruServer(heartbeat_s=0.1)
    yield server
    server.close()


@pytest.fixture
def node(fake):
    sockdir = short_tmpdir()
    # Credit for a layerwise burst: every (chunk, layer) object takes a page
    budgets = {
        "credit_max_bytes": 16 * PAGE,
        "credit_total_max_bytes": 32 * PAGE,
        "credit_refill_low": 8 * PAGE,
        "credit_refill_high": 16 * PAGE,
        "commit_pending_max_bytes": 16 * PAGE,
    }
    config = node_config(fake.url, f"{sockdir}/node.sock", pages=64, budgets=budgets)
    service = start_node(config)
    yield service
    service.stop()
    remove_tree(sockdir)


@pytest.fixture
def workers(node):
    made = []

    def make(shard=WHOLE_MODEL, heads: int = 2, **extra):
        worker = make_worker(
            BLOCK,
            CHUNK,
            node_extra(node.config.socket, **extra),
            head_size=HEAD_SIZE,
            kv_shard=shard,
        )
        worker._num_layers = NUM_LAYERS
        per_layer = 2 * CHUNK * heads * HEAD_SIZE * 4
        use_layerwise = extra.get("maru_use_layerwise", False)
        worker._page_size_bytes = per_layer * (1 if use_layerwise else NUM_LAYERS)
        worker._take_node_handler(wait=True)  # as register_kv_caches does
        assert worker._handler is not None
        made.append(worker)
        return worker

    yield make
    for worker in made:
        worker.shutdown()


def store(worker, kv, start: int = 0) -> None:
    """``prefill`` once the worker has credit; fails if a store was skipped."""
    client = worker._handler._cpu
    if client._writes:
        wait_credit(worker)
    before = skipped_stores(client)
    prefill(worker, kv, start)
    assert skipped_stores(client) == before


def wait_credit(worker) -> None:
    """Credit comes from the refill thread after connect; a store without it
    is skipped."""
    client = worker._handler._cpu
    low = client.attach_info.budgets.credit_refill_low
    wait_until(lambda: client.credits.unused_bytes >= low, what="credit")


def skipped_stores(client) -> dict[str, int]:
    counters = client.local_stats()["counters"]
    return {name: n for name, n in counters.items() if name.startswith("store_")}


def prefill(worker, kv, start: int = 0) -> None:
    """One prefill step, then wait for its WRITE_DONE."""
    metadata = MaruConnectorMetadata(
        requests=[
            MaruReqMeta(
                req_id="r1",
                token_ids=list(range(PROMPT)),
                block_ids=list(range(NUM_BLOCKS)),
                is_store=True,
                num_scheduled_tokens=PROMPT - start,
                num_computed_tokens=start,
                store_start_token=start,
            )
        ]
    )
    attn = make_flash_attn_metadata()
    for name, layer in zip(LAYER_NAMES, kv, strict=True):
        worker.save_kv_layer(name, layer, attn, metadata)
    drain(worker)


def drain(worker) -> None:
    """Wait for the step's stores, and MaruServer's answers to them."""
    if worker._store_executor is not None:
        worker._store_executor.submit(lambda: None).result(10)
    worker._wait_node_stores(10)
    assert not worker._node_store_futures


def reads_held(worker) -> int:
    return len(worker._handler._cpu._reads)


# =============================================================================
# Config
# =============================================================================


@pytest.mark.parametrize("key", sorted(_NODE_KEYS_ELSEWHERE))
def test_a_key_the_storage_service_owns_is_refused_by_name(key):
    with pytest.raises(ValueError, match=f"^{key} does not go with maru_node"):
        _validate_storage_config(node_extra(**{key: "1"}))


@pytest.mark.parametrize("key", ["maru_auto_expand", "maru_eager_map"])
def test_cxl_only_keys_are_refused_with_maru_node(key):
    with pytest.raises(ValueError, match=f"^{key} does not go with maru_node"):
        _validate_storage_config(node_extra(**{key: True}))


@pytest.mark.parametrize("key", ["maru_write_hints", "maru_write_wait_ms"])
def test_write_hint_keys_point_to_the_node_config(key):
    with pytest.raises(ValueError, match="the node config holds the write hint"):
        _validate_storage_config(node_extra(**{key: 1}))


def test_read_hint_keys_go_with_maru_node(caplog):
    """Taken, and said to do nothing: no node carries read hints."""
    extra = node_extra(maru_read_hints=True, maru_read_hint_on_arrival=True)
    assert _validate_storage_config(extra) is False
    config = node_vllm_config()
    with caplog.at_level("WARNING", logger="maru_vllm.connector"):
        _node_engine_config(extra, config)
    assert "read hints have no effect on maru_node" in caplog.text
    with caplog.at_level("WARNING", logger="maru_vllm.connector"):
        caplog.clear()
        _node_engine_config(node_extra(), config)
    assert "read hints" not in caplog.text


def test_missing_identity_is_refused():
    extra = node_extra()
    del extra["maru_engine_id"]
    with pytest.raises(ValueError, match="requires maru_engine_id"):
        _validate_storage_config(extra)
    with pytest.raises(ValueError, match="path of the StorageService socket"):
        _validate_storage_config(node_extra(""))
    assert _validate_storage_config(node_extra()) is False  # not a session backend


@pytest.mark.parametrize("metadata_only", [False, True])
@pytest.mark.parametrize("custom", [False, True])
def test_lookup_timeout_and_credit_wait_config(monkeypatch, metadata_only, custom):
    import maru

    handler_class = MagicMock()
    monkeypatch.setattr(maru, "MaruHandler", handler_class)
    extra = node_extra()
    if custom:
        extra.update(maru_lookup_timeout_ms=250, maru_credit_wait_ms=20)
    handler = connector_module._create_node_handler(extra, metadata_only, False)
    config = handler_class.call_args.args[0]
    assert config.lookup_timeout_ms == (250 if custom else 100)
    assert config.credit_wait_s == (0.02 if custom else 0)
    assert config.timeout_ms == 2000
    handler.connect.assert_called_once_with(wait=False)


def node_vllm_config(tp_size: int = 1, total_heads: int = 8, **parallel):
    config = remote_vllm_config(
        tp_size,
        total_heads,
        extra=node_extra(
            maru_async_load=True,
            maru_async_store=True,
            maru_overlap_load_with_compute=True,
        ),
        **parallel,
    )
    config.model_config.enforce_eager = False  # CUDA graphs
    return config


def test_cpu_backend_refusals_are_lifted():
    """Async loads and stores, overlap, layerwise, CUDA graphs and TP>1."""
    config = node_vllm_config(2)
    extra = {
        **config.kv_transfer_config.kv_connector_extra_config,
        "maru_use_layerwise": True,
    }
    out = _node_engine_config(extra, config)
    assert out["maru_cache_namespace"] != "ns"
    assert out["maru_use_layerwise"] and out["maru_overlap_load_with_compute"]


@pytest.mark.parametrize(
    "parallel",
    [{"pipeline_parallel_size": 2}, {"prefill_context_parallel_size": 2}],
)
def test_pipeline_and_context_parallelism_stay_refused(parallel):
    config = node_vllm_config(1, **parallel)
    with pytest.raises(ValueError, match="supports tensor parallelism but not"):
        _node_engine_config(config.kv_transfer_config.kv_connector_extra_config, config)


def test_a_failed_load_must_be_recomputed():
    config = node_vllm_config()
    config.kv_transfer_config.kv_load_failure_policy = "fail"
    with pytest.raises(ValueError, match="kv_load_failure_policy='recompute'"):
        _node_engine_config(config.kv_transfer_config.kv_connector_extra_config, config)


def test_the_namespace_is_bound_to_the_geometry_and_the_path():
    from tests.unit.test_vllm_cpu_config import engine_config
    from tests.unit.test_vllm_remote_cxl_config import extra as remote_cxl_extra

    config = engine_config()
    tp1 = _node_engine_config(node_extra(), config)["maru_cache_namespace"]
    assert tp1 == _node_engine_config(node_extra(), config)["maru_cache_namespace"]
    remote = connector_module._cpu_engine_config(remote_cxl_extra(), config)
    assert remote["maru_cache_namespace"] != tp1
    config.parallel_config.tensor_parallel_size = 2
    assert _node_engine_config(node_extra(), config)["maru_cache_namespace"] != tp1


# =============================================================================
# Transfer defaults
# =============================================================================


def advise(monkeypatch, defaults: TransferDefaults | Exception):
    calls = []

    def probe(config):
        calls.append(config.maru_node)
        if isinstance(defaults, Exception):
            raise defaults
        return AttachResponse(
            status=Status.OK, node_id="gpu-a", transfer_defaults=defaults
        )

    monkeypatch.setattr(client_module, "probe_node", probe)
    # No node runs here: the scheduler does not connect at startup
    monkeypatch.setattr(
        connector_module.MaruSchedulerConnector, "connect_node", lambda *a, **k: None
    )
    return calls


def test_the_node_advises_unset_modes_once_per_process(monkeypatch):
    calls = advise(monkeypatch, TransferDefaults(async_load=True, async_store=False))
    out = _node_transfer_defaults(node_extra())
    assert out["maru_async_load"] is True and out["maru_async_store"] is False
    # Asked once: another connector of the process takes the same answer
    again = _node_transfer_defaults(node_extra(maru_async_load=False))
    assert again["maru_async_load"] is False and again["maru_async_store"] is False
    assert calls == [SOCKET]


def test_explicit_modes_win_and_need_no_node(monkeypatch):
    calls = advise(monkeypatch, StorageUnavailableError("down"))
    extra = node_extra(maru_enable_deferred_loading=False, maru_async_store=True)
    assert _node_transfer_defaults(extra) == extra
    assert calls == []


def test_an_unreachable_node_stops_the_engine_when_a_mode_is_unset(monkeypatch):
    advise(monkeypatch, StorageUnavailableError("no socket"))
    with pytest.raises(ValueError, match=f"StorageService at {SOCKET}"):
        _node_transfer_defaults(node_extra(maru_async_load=True))


def test_none_keeps_the_connector_default(monkeypatch):
    advise(monkeypatch, TransferDefaults())
    out = _node_transfer_defaults(node_extra())
    assert "maru_async_load" not in out and "maru_async_store" not in out


def test_a_dram_node_advises_the_cxl_defaults(node):
    connector_module._node_transfer_cache.clear()
    out = _node_transfer_defaults(node_extra(node.config.socket))
    assert out["maru_async_load"] is False and out["maru_async_store"] is False


# =============================================================================
# Connector roles
# =============================================================================


def test_ranks_keep_the_engine_id_and_name_their_ranks(monkeypatch):
    advise(monkeypatch, TransferDefaults(False, False))
    monkeypatch.setattr(connector_module, "_tp_rank", lambda: 1)
    config = node_vllm_config(
        2, 8, data_parallel_size=1, data_parallel_rank=0, data_parallel_index=1
    )
    worker = MaruKVConnector(config, KVConnectorRole.WORKER)
    scheduler = MaruKVConnector(config, KVConnectorRole.SCHEDULER)
    w, s = worker._worker._extra_config, scheduler._scheduler._extra_config
    assert w["maru_engine_id"] == s["maru_engine_id"] == "eng"
    assert (w["_maru_dp_index"], w["_maru_tp_rank"]) == (1, 1)
    assert s["_maru_dp_index"] == 1 and "_maru_tp_rank" not in s
    assert worker._worker._kv_shard.suffix == "_h4-8"
    assert scheduler._scheduler._shard_suffixes == ("_h0-4", "_h4-8")
    assert scheduler._scheduler._deferred_loading  # TP parks every load
    assert worker._worker._layerwise_overlap
    # The scheduler loads no model under TP: it takes the workers' namespace
    meta = worker.get_handshake_metadata()
    assert meta.cache_namespace == w["maru_cache_namespace"]


def test_tp_parks_loads_whatever_the_node_advises(monkeypatch):
    advise(monkeypatch, TransferDefaults(async_load=False, async_store=False))
    monkeypatch.setattr(connector_module, "_tp_rank", lambda: 0)
    config = node_vllm_config(2)
    config.kv_transfer_config.kv_connector_extra_config = node_extra()
    scheduler = MaruKVConnector(config, KVConnectorRole.SCHEDULER)._scheduler
    assert scheduler._deferred_loading and not scheduler._write_behind


def test_several_kv_cache_groups_are_taken(monkeypatch):
    advise(monkeypatch, TransferDefaults(False, False))
    config = node_vllm_config()
    config.cache_config.block_size = 16
    connector = MaruKVConnector(config, KVConnectorRole.SCHEDULER, gpt_oss_config())
    assert connector._scheduler._multi_group
    assert connector._scheduler._deferred_loading


def test_the_scheduler_probes_every_lookup():
    """MaruServer is the directory of record: no key is remembered."""
    tokens = list(range(2 * CHUNK))
    scheduler = make_scheduler(BLOCK, CHUNK, node_extra())
    scheduler._handler = MagicMock()
    scheduler._handler.batch_exists.side_effect = lambda keys, **_: [True] * len(keys)
    assert scheduler._count_matched_chunks(tokens) == 2
    assert scheduler._count_matched_chunks(tokens) == 2
    assert scheduler._handler.batch_exists.call_count == 2
    assert scheduler._known_keys == set()


# =============================================================================
# Loads and stores on a node
# =============================================================================


def test_store_and_load_round_trip(fake, node, workers):
    writer, reader = workers(), workers()
    kv = rank_kv(1, 2)
    writer._handler.batch_store = MagicMock(side_effect=AssertionError("waits"))
    store_async = MagicMock(wraps=writer._handler.store_async)
    writer._handler.store_async = store_async
    store(writer, kv)
    # WRITE_DONE is posted; nothing waits for MaruServer's commit
    assert store_async.call_count == 1
    keys = _chunk_keys(list(range(PROMPT)), CHUNK)
    assert {Key("ns", k) for k in keys} <= fake.keys()

    dst = [torch.zeros_like(layer) for layer in kv]
    load_into(reader, dst, make_flash_attn_metadata())
    assert_same_kv(dst, kv)
    assert reader.take_failed_load_blocks() == set()
    assert reads_held(reader) == 0
    wait_until(lambda: fake.pin_count() == 0, what="every pin released")


def load_failures(caplog) -> list:
    return [
        r
        for r in caplog.records
        if r.name == "maru_vllm.connector" and "Maru load" in r.getMessage()
    ]


def test_a_gone_key_recomputes_only_the_external_range(fake, node, workers, caplog):
    writer, reader = workers(), workers()
    kv = rank_kv(1, 2)
    store(writer, kv)
    first, second = _chunk_keys(list(range(PROMPT)), CHUNK)
    with fake._lock:
        for entry in fake.catalog.pop(Key("ns", second)):
            fake.by_loc.pop(
                (
                    entry.location.pool_id,
                    entry.location.allocation_id,
                    entry.location.generation,
                ),
                None,
            )
    dst = [torch.zeros_like(layer) for layer in kv]
    forward = SimpleNamespace(
        no_compile_layers={
            name: SimpleNamespace(kv_cache=layer)
            for name, layer in zip(LAYER_NAMES, dst, strict=True)
        },
        attn_metadata=make_flash_attn_metadata(),
    )
    # vLLM's own prefix cache holds the first block
    meta = MaruReqMeta(
        req_id="r1",
        token_ids=list(range(PROMPT)),
        block_ids=list(range(NUM_BLOCKS)),
        is_store=False,
        num_matched_chunks=2,
        load_start_token=BLOCK,
        load_end_token=PROMPT,
    )
    with caplog.at_level("INFO", logger="maru_vllm.connector"):
        reader.start_load_kv(forward, MaruConnectorMetadata(requests=[meta]))
    assert reader.take_failed_load_blocks() == {1, 2, 3}
    assert reads_held(reader) == 0
    counters = reader._handler.get_stats()["maru_node"]["counters"]
    assert counters["retrieve_not_found"] == 1
    # One line, with the reason: an expected miss is a warning
    (failure,) = load_failures(caplog)
    assert failure.levelname == "WARNING"
    assert "1 of 2 chunks not readable: {'lookup:absent': 1}" in failure.getMessage()
    wait_until(lambda: fake.pin_count() == 0, what="every pin released")


def test_an_unavailable_maruserver_fails_the_load(fake, node, workers, caplog):
    writer, reader = workers(), workers()
    kv = rank_kv(1, 2)
    store(writer, kv)
    fake.pause(hold=False)
    try:
        with caplog.at_level("INFO", logger="maru_vllm.connector"):
            load_into(reader, [torch.zeros_like(layer) for layer in kv], None)
    finally:
        fake.resume()
    assert reader.take_failed_load_blocks() == set(range(NUM_BLOCKS))
    counters = reader._handler.get_stats()["maru_node"]["counters"]
    assert counters["retrieve_unavailable"] == 2
    # Not an expected miss: an error, with its reason
    (failure,) = load_failures(caplog)
    assert failure.levelname == "ERROR"
    assert "{'lookup:unavailable': 2}" in failure.getMessage()


@pytest.mark.parametrize(
    "reasons, expected",
    [
        ({"lookup:absent": 1, "fetch:busy": 2, "fetch:over_window": 1}, True),
        ({"lookup:offline": 1, "fetch:timeout": 1}, True),
        ({"fetch:io_error": 1}, False),
        ({"lookup:absent": 1, "unknown": 1}, False),
        ({}, False),
    ],
)
def test_only_expected_miss_reasons_make_a_warning(reasons, expected):
    error = _LoadMissError(sum(reasons.values()), 4, "chunks", reasons)
    assert error.expected is expected


@pytest.mark.parametrize("start, layerwise", [(0, False), (CHUNK, False), (0, True)])
def test_a_step_asks_for_its_stores_credit(workers, monkeypatch, start, layerwise):
    """want_step_credit asks the node for the pages the step's stores take:
    one per chunk the step completes, or one per layer and its marker."""
    worker = workers(maru_use_layerwise=layerwise)
    asked = []
    monkeypatch.setattr(worker._handler, "want_credit", asked.append)
    meta = MaruReqMeta(
        req_id="r1",
        token_ids=list(range(PROMPT)),
        block_ids=list(range(NUM_BLOCKS)),
        is_store=True,
        num_scheduled_tokens=PROMPT - start,
        num_computed_tokens=start,
        store_start_token=start,
    )
    worker.want_step_credit(MaruConnectorMetadata(requests=[meta]))
    chunks = (PROMPT - start) // CHUNK
    assert asked == [chunks * (NUM_LAYERS + 1) if layerwise else chunks]
    # a step that stores nothing asks for nothing; loads do not ask
    asked.clear()
    load_only = dataclasses.replace(meta, is_store=False)
    worker.want_step_credit(MaruConnectorMetadata(requests=[load_only]))
    dst = [torch.zeros_like(layer) for layer in rank_kv(0, 2)]
    worker.start_load_kv(forward_of(dst), MaruConnectorMetadata(requests=[meta]))
    assert asked == []


def test_write_behind_asks_on_the_step_that_completes_the_prompt(workers, monkeypatch):
    """Write-behind stores a chunked prefill's chunks in its last step."""
    worker = workers(maru_async_store=True)
    assert worker._write_behind
    asked = []
    monkeypatch.setattr(worker._handler, "want_credit", asked.append)

    def step(computed: int, scheduled: int) -> None:
        meta = MaruReqMeta(
            req_id="r1",
            token_ids=list(range(PROMPT)),
            block_ids=list(range(NUM_BLOCKS)),
            is_store=True,
            num_scheduled_tokens=scheduled,
            num_computed_tokens=computed,
        )
        worker.want_step_credit(MaruConnectorMetadata(requests=[meta]))

    step(0, CHUNK)  # the prompt goes on: nothing stored yet
    assert asked == []
    step(CHUNK, PROMPT - CHUNK)  # completes it: every chunk at once
    assert asked == [PROMPT // CHUNK]


def test_the_connector_asks_when_vllm_binds_the_steps_metadata(monkeypatch):
    advise(monkeypatch, TransferDefaults(False, False))
    connector = MaruKVConnector(node_vllm_config(), KVConnectorRole.WORKER)
    seen = []
    monkeypatch.setattr(connector._worker, "want_step_credit", seen.append)
    metadata = MaruConnectorMetadata(requests=[])
    connector.bind_connector_metadata(metadata)
    assert seen == [metadata]


@pytest.mark.parametrize(("tp_size", "total_heads"), [(2, 4), (2, 1)])
def test_ranks_store_their_slices_and_a_replica_only_loads(
    fake, node, workers, tp_size, total_heads
):
    shards = [tp_kv_shard(rank, tp_size, total_heads) for rank in range(tp_size)]
    heads = max(1, total_heads // tp_size)
    ranks = [workers(shard, heads=heads) for shard in shards]
    kvs = [rank_kv(rank + 1, heads) for rank in range(tp_size)]
    writers = [s.is_writer for s in shards]
    for rank, kv in zip(ranks, kvs, strict=True):
        store(rank, kv)
    keys = _chunk_keys(list(range(PROMPT)), CHUNK)
    expected = {
        Key("ns", key + shard.suffix)
        for key in keys
        for shard in shards
        if shard.is_writer
    }
    assert fake.keys() == expected
    for rank, kv, writes in zip(ranks, kvs, writers, strict=True):
        client = rank._handler._cpu
        assert client.config.load_only is (not writes)
        assert node.pools.held_bytes(client.identity.incarnation) == 0 or writes
        dst = [torch.zeros_like(layer) for layer in kv]
        load_into(rank, dst, make_flash_attn_metadata())
        # A replica holds the same heads as the rank that stored them
        assert_same_kv(dst, kvs[0] if not writes else kv)
        assert rank.take_failed_load_blocks() == set()
    if total_heads == 1:
        assert not writers[1]


def test_layerwise_objects_and_their_done_marker(fake, node, workers):
    writer = workers(maru_use_layerwise=True)
    reader = workers(maru_use_layerwise=True)
    kv = rank_kv(1, 2)
    store(writer, kv)
    keys = _chunk_keys(list(range(PROMPT)), CHUNK)
    stored = {k.key for k in fake.keys()}
    for key in keys:
        assert {f"{key}_L{i}" for i in range(NUM_LAYERS)} | {f"{key}_DONE"} <= stored
    dst = [torch.zeros_like(layer) for layer in kv]
    load_into(reader, dst, make_flash_attn_metadata())
    assert_same_kv(dst, kv)
    assert reads_held(reader) == 0


def test_stores_behind_are_found_only_once_committed(fake, node, workers):
    """#143: a chunk is invisible to lookups until MaruServer commits it."""
    writer = workers()
    wait_credit(writer)
    held = []
    store_async = writer._handler.store_async

    def hold(keys, handles):
        future = Future()
        held.append(
            lambda: store_async(keys, handles).add_done_callback(
                lambda f: future.set_result(f.result())
            )
        )
        return future

    writer._handler.store_async = hold
    kv = rank_kv(1, 2)
    metadata = MaruConnectorMetadata(
        requests=[
            MaruReqMeta(
                req_id="r1",
                token_ids=list(range(PROMPT)),
                block_ids=list(range(NUM_BLOCKS)),
                is_store=True,
                num_scheduled_tokens=PROMPT,
            )
        ]
    )
    attn = make_flash_attn_metadata()
    for name, layer in zip(LAYER_NAMES, kv, strict=True):
        writer.save_kv_layer(name, layer, attn, metadata)
    keys = _chunk_keys(list(range(PROMPT)), CHUNK)
    assert writer._handler.batch_exists(keys) == [False, False]
    assert all(writer._store_pending(k) for k in keys)  # not stored again
    for send in held:
        send()
    drain(writer)
    assert writer._handler.batch_exists(keys) == [True, True]
    assert not any(writer._store_pending(k) for k in keys)


def node_group_worker(node, shared):
    """A worker of the tiny model with several groups, on ``node``."""
    caches = tiny_kv_caches("0.30", shared)
    worker = make_worker(
        TINY_BLOCK,
        TINY_BLOCK,
        node_extra(node.config.socket),
        num_kv_heads=2,
        head_size=8,
        kv_cache_layout=_engine_layout(caches),
        kv_groups=describe_kv_groups(tiny_config()),
    )
    worker.register_kv_caches(caches)  # attaches with the group page
    assert worker._handler is not None
    return worker


def test_several_groups_on_a_node(fake, node):
    source = _shared()
    writer = node_group_worker(node, source)
    target = torch.zeros_like(_shared())
    reader = node_group_worker(node, target)
    try:
        wait_credit(writer)
        tokens = list(range(40))
        writer.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32))
        drain(writer)
        assert len(fake.keys()) == 5
        reader.start_load_kv(_FORWARD, _load_meta(tokens, B_BLOCKS, 2, 32, 8))
        assert reader.take_failed_load_blocks() == set()
        src = writer._group_caches[3].tensors
        dst = reader._group_caches[3].tensors
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
        assert reads_held(reader) == 0
        key = _chunk_keys(tokens, TINY_BLOCK)[1]
        assert Key("ns", group_object_key(key, 3, TINY_BLOCK)) in fake.keys()
    finally:
        writer.shutdown()
        reader.shutdown()


# =============================================================================
# Read pins held across queued copies (overlap and layerwise paths)
# =============================================================================


class Done:
    def query(self):
        return True

    def synchronize(self):
        pass


class Pending(Done):
    def query(self):
        return False


def lease(name: str) -> CpuReadLease:
    return CpuReadLease(view=memoryview(bytearray(4)), location=name, _release=None)


def pinning_worker():
    worker = make_worker(BLOCK, CHUNK, node_extra())
    worker._handler = MagicMock()
    return worker


def released(worker) -> list:
    return [
        info
        for call in worker._handler.release_retrieved.call_args_list
        for info in call.args[0]
    ]


def test_queued_reads_are_released_once_their_copies_end():
    worker = pinning_worker()
    done, pending = lease("done"), lease("pending")
    worker._active_load_refs = [
        (Done(), [[done], torch.zeros(1)]),
        (Pending(), [[pending]]),
    ]
    worker._release_completed_load_refs()
    assert released(worker) == [done]
    assert len(worker._active_load_refs) == 1


def test_any_read_lease_is_released_whatever_its_class():
    """The connector keeps the read contract, not a class: a CXL read or a
    future medium's read is released the same way; a legacy MemoryInfo is not."""

    class OtherLease:
        def __init__(self):
            self.view = memoryview(bytearray(4))

        def release(self) -> None:
            pass

    worker = pinning_worker()
    other = OtherLease()
    legacy = MemoryInfo(view=memoryview(bytearray(4)))
    worker._release_reads([[other, legacy], {"x": lease("a")}])
    assert [type(r).__name__ for r in released(worker)] == [
        "OtherLease",
        "CpuReadLease",
    ]


def test_a_finished_parked_load_releases_its_reads():
    worker = pinning_worker()
    read = lease("a")
    worker._deferred_events["r1"] = Done()
    worker._deferred_refs["r1"] = [[read], torch.zeros(1)]
    assert worker.get_finished_loading() == {"r1"}
    assert released(worker) == [read]


def test_a_reclaimed_request_releases_reads_never_copied():
    worker = pinning_worker()
    retained = {0: [lease("l0")], 1: [lease("l1")]}
    worker._deferred_layerwise_loads["r1"] = ("meta", 1, torch.zeros(1), retained)
    worker._drain_layerwise_copies({"r1"}, report=False)
    assert released(worker) == retained[0] + retained[1]


def test_shutdown_releases_every_held_read_before_closing():
    worker = pinning_worker()
    handler = worker._handler
    reads = [lease(str(i)) for i in range(3)]
    worker._active_load_refs = [(Pending(), [[reads[0]]])]
    worker._deferred_refs["r1"] = [[reads[1]]]
    worker._deferred_layerwise_loads["r2"] = ("meta", 1, None, [reads[2]])
    order = []
    handler.release_retrieved.side_effect = lambda infos: order.append("release")
    handler.close.side_effect = lambda: order.append("close")
    worker.shutdown()
    assert order == ["release", "close"]
    assert {id(r) for r in handler.release_retrieved.call_args.args[0]} == {
        id(r) for r in reads
    }


class FailedWait(Done):
    """Copies whose wait raises a CUDA error: their end is unknown."""

    def synchronize(self):
        raise cuda_error()


class BrokenWait(Done):
    """A wait that fails with another error, handled as before."""

    def synchronize(self):
        raise RuntimeError("wait failed")


def is_kept(worker) -> bool:
    """Whether ``worker`` (and so all of its state) stays until exit."""
    return any(owner is worker for owner, _ in _KEPT_UNTIL_EXIT)


def page() -> SimpleNamespace:
    return SimpleNamespace(buf=memoryview(bytearray(4)))


def write_behind_worker():
    worker = pinning_worker()
    worker._write_behind = True
    return worker


def wait_on(worker, req_id: str, *keys: str) -> None:
    """``req_id`` waits for the write-behind stores of ``keys``."""
    with worker._store_lock:
        for key in keys:
            worker._pending_store_keys.add(key)
            worker._store_key_waiters.setdefault(key, set()).add(req_id)
            worker._request_pending_store_keys.setdefault(req_id, set()).add(key)


def test_a_failed_write_wait_keeps_its_pages_and_blocks():
    """The copy may still write the page and read the request's GPU blocks:
    the worker stops, neither is handed out again, nothing is published."""
    worker = write_behind_worker()
    handle = page()
    wait_on(worker, "r", "k")
    worker._finish_write_behind_store(FailedWait(), ["k"], [handle], [])
    worker._handler.free.assert_not_called()
    worker._handler.store_async.assert_not_called()
    assert "r" in worker._request_pending_store_keys  # never finishes
    assert holds(worker, handle)
    assert stopped(worker, UNCONFIRMED)


def test_another_write_wait_failure_frees_and_finishes_as_before():
    """Not a CUDA error: the page goes back, the requests finish, no stop."""
    worker = write_behind_worker()
    handle = page()
    wait_on(worker, "r", "k")
    worker._finish_write_behind_store(BrokenWait(), ["k"], [handle], [])
    worker._handler.free.assert_called_once_with(handle)
    assert worker.get_finished_saving({"r"}) == {"r"}
    assert worker._stopped is None


def test_chunks_waiting_for_credit_stay_with_an_unconfirmed_batch():
    """Their slabs were gathered by the same copies: the slabs, their room and
    their requests' blocks are kept too."""
    worker = write_behind_worker()
    worker._release_deferred = MagicMock()
    wait_on(worker, "r", "k")
    wait_on(worker, "r2", "d")
    slab = torch.zeros(4)
    worker._finish_write_behind_store(
        FailedWait(), ["k"], [page()], [], [("d", slab, {})]
    )
    assert "r" in worker._request_pending_store_keys
    assert "r2" in worker._request_pending_store_keys
    worker._release_deferred.assert_not_called()
    assert holds(worker, slab)
    assert stopped(worker, UNCONFIRMED)


@pytest.mark.parametrize("ended", [True, False])
def test_a_batch_without_a_handler_still_waits_for_its_copies(ended):
    """A gone handler: the requests finish only once the copies are known to
    have ended; the chunks waiting for credit go."""
    worker = write_behind_worker()
    wait_on(worker, "r", "k", "d")
    handler, worker._handler = worker._handler, None
    deferred = [("d", torch.zeros(4), {})]
    worker._finish_write_behind_store(
        Done() if ended else FailedWait(), ["k"], [page()], [], deferred
    )
    worker._handler = handler
    if ended:
        assert worker.get_finished_saving({"r"}) == {"r"}
        assert not deferred  # given up
    else:
        assert "r" in worker._request_pending_store_keys
        assert deferred and stopped(worker, UNCONFIRMED)


def test_a_failed_shutdown_wait_keeps_reads_and_mappings():
    """A copy may still read the pinned objects: pins and mappings stay
    until the process ends."""
    worker = pinning_worker()
    handler = worker._handler
    read = lease("read")
    worker._active_load_refs = [(Pending(), [[read]])]
    worker._load_stream = FailedWait()
    worker.shutdown()
    handler.release_retrieved.assert_not_called()
    handler.close.assert_not_called()
    assert worker._active_load_refs[0][1] == [[read]] and is_kept(worker)


def test_a_kept_copy_outlives_later_drains_shutdowns_and_the_worker():
    """Once a copy's end is unknown, a later wait that succeeds does not give
    back what it may use: no free, no release, no close of the handler."""
    worker = write_behind_worker()
    handler = worker._handler
    handle = page()
    read = lease("read")
    wait_on(worker, "r", "k")
    worker._finish_write_behind_store(FailedWait(), ["k"], [handle], [])
    worker._active_load_refs = [(Done(), [[read]])]
    worker.shutdown()  # every stream drains now
    worker.shutdown()
    handler.free.assert_not_called()
    handler.release_retrieved.assert_not_called()
    handler.close.assert_not_called()
    assert holds(worker, handle)
    alive = weakref.ref(worker)
    del worker
    gc.collect()
    assert alive() is not None


def test_store_copies_of_preempted_requests_that_cannot_be_waited_for_are_fatal():
    worker = write_behind_worker()
    staging = torch.zeros(4)
    worker._store_staging = staging
    worker._store_stream = FailedWait()
    with pytest.raises(_UnconfirmedCopyError):
        worker.handle_preemptions(MaruConnectorMetadata(preempted_req_ids={"r"}))
    assert holds(worker, staging)
    assert stopped(worker, UNCONFIRMED)


@pytest.mark.parametrize("cuda", [True, False], ids=["cuda", "other"])
def test_a_layer_copy_that_fails_keeps_its_page_only_on_a_cuda_error(monkeypatch, cuda):
    """A CUDA error of the copy to the page stops the worker and keeps the
    page; another error gives the chunk's pages back as before."""
    worker = pinning_worker()
    worker._num_layers = NUM_LAYERS
    worker._handler.has_local.return_value = False
    marker, layer = page(), page()
    pages = iter([marker, layer])
    worker._handler.alloc.side_effect = lambda *a, **k: next(pages)
    kv = MagicMock()
    kv.detach.return_value.contiguous.return_value = kv
    kv.nelement.return_value, kv.element_size.return_value = 1, 4
    kv.dtype, kv.shape = torch.float32, (1,)
    monkeypatch.setattr(worker, "_extract_kv_from_layer", lambda *a: kv)
    copy = MagicMock(side_effect=cuda_error() if cuda else RuntimeError("bad"))
    monkeypatch.setattr(
        torch,
        "frombuffer",
        lambda *a, **k: SimpleNamespace(reshape=lambda *s: SimpleNamespace(copy_=copy)),
    )
    one_chunk = dataclasses.replace(store_meta("r", 0), token_ids=list(range(CHUNK)))
    save = functools.partial(
        worker._save_layer_on_node,
        LAYER_NAMES[0],
        torch.zeros(1),
        None,
        MaruConnectorMetadata(requests=[one_chunk]),
    )
    if cuda:
        with pytest.raises(_UnconfirmedCopyError):
            save()
        worker._handler.free.assert_not_called()
        assert holds(worker, layer) and holds(worker, kv)
        assert stopped(worker, UNCONFIRMED)
    else:
        save()
        freed = [c.args[0] for c in worker._handler.free.call_args_list]
        assert freed == [layer, marker] and worker._stopped is None


def test_other_backends_hold_no_pins():
    worker = make_worker(BLOCK, CHUNK, {})
    worker._handler = MagicMock()
    worker._release_reads([[lease("x")]])
    worker._handler.release_retrieved.assert_not_called()


def test_pinned_reads_are_copied_one_by_one():
    """A read names no CXL page: never combined into a run."""
    worker = pinning_worker()
    worker._effective_page_size_bytes = 4
    reads = [lease("a"), lease("b")]
    runs = worker._chunk_runs(reads)
    assert [(start, n) for start, n, _ in runs] == [(0, 1), (1, 1)]


def test_the_kernel_follows_the_page_lock_of_the_node_pools():
    worker = pinning_worker()
    worker._handler.storage_gpu_accessible.return_value = False
    assert worker._lease_buffers_gpu_accessible() is False
    assert worker._packed_load_kernel_ctx([("l", torch.zeros(1), 0)], None) is None
    worker._handler.storage_gpu_accessible.return_value = True
    assert worker._lease_buffers_gpu_accessible() is True


def test_no_credit_is_an_expected_skip():
    worker = pinning_worker()
    assert worker._expected_store_skip(CreditUnavailableError("none"))
    assert worker._expected_store_skip(StorageUnavailableError("down"))
    assert not worker._expected_store_skip(RuntimeError("bug"))


# =============================================================================
# Async scheduling, connecting off the engine thread, refusals
# =============================================================================


def test_async_scheduling_parks_loads_whatever_the_node_advises(monkeypatch, caplog):
    calls = advise(monkeypatch, TransferDefaults(async_load=False, async_store=False))
    with caplog.at_level("INFO", logger="maru_vllm.connector"):
        out = _node_transfer_defaults(node_extra(), async_scheduling=True)
    assert out["maru_async_load"] is True and out["maru_async_store"] is False
    assert calls == [SOCKET]
    assert "overriding the node's advice" in caplog.text


def test_async_scheduling_refuses_explicit_inline_loads(monkeypatch):
    advise(monkeypatch, TransferDefaults(False, False))
    for name in ("maru_async_load", "maru_enable_deferred_loading"):
        with pytest.raises(ValueError, match="--no-async-scheduling"):
            _node_transfer_defaults(node_extra(**{name: False}), async_scheduling=True)
    out = _node_transfer_defaults(
        node_extra(maru_async_load=False), async_scheduling=False
    )
    assert out["maru_async_load"] is False


@pytest.mark.parametrize("async_load", [None, False])
def test_the_connector_follows_vllm_async_scheduling(monkeypatch, async_load):
    advise(monkeypatch, TransferDefaults(async_load=False, async_store=False))
    config = node_vllm_config()
    extra = node_extra()
    if async_load is not None:
        extra["maru_async_load"] = async_load
    config.kv_transfer_config.kv_connector_extra_config = extra
    config.scheduler_config.async_scheduling = True
    if async_load is False:
        with pytest.raises(ValueError, match="--no-async-scheduling"):
            MaruKVConnector(config, KVConnectorRole.SCHEDULER)
        return
    scheduler = MaruKVConnector(config, KVConnectorRole.SCHEDULER)._scheduler
    assert scheduler._deferred_loading


def absent_worker(sockdir, **extra):
    worker = make_worker(
        BLOCK, CHUNK, node_extra(f"{sockdir}/node.sock", **extra), head_size=HEAD_SIZE
    )
    worker._num_layers = NUM_LAYERS
    worker._page_size_bytes = 2 * CHUNK * 2 * HEAD_SIZE * 4 * NUM_LAYERS
    return worker


def test_no_engine_step_waits_for_an_absent_node(fake):
    sockdir = short_tmpdir()
    worker = absent_worker(sockdir)
    created = MagicMock(wraps=worker._create_node_worker_handler)
    worker._create_node_worker_handler = created
    kv = rank_kv(1, 2)
    try:
        for _ in range(3):
            t0 = time.monotonic()
            prefill(worker, kv)
            load_into(worker, [torch.zeros_like(layer) for layer in kv], None)
            assert time.monotonic() - t0 < 0.5
            assert worker.take_failed_load_blocks() == set(range(NUM_BLOCKS))
        assert created.call_count == 1  # created once, never again
        # The node comes up: the same handler connects and serves
        service = start_node(node_config(fake.url, f"{sockdir}/node.sock", pages=64))
        try:
            wait_until(
                lambda: worker._ensure_handler() or worker._handler, what="connect"
            )
            assert created.call_count == 1
            wait_until(lambda: worker._handler._cpu.credits.unused_bytes >= 2 * PAGE)
            # A new request: r1 computed over a failed inline load stores nothing
            save_step(worker, kv, [store_meta("r2", 0)])
            drain(worker)
            assert len(fake.keys()) == 2
        finally:
            worker.shutdown()
            service.stop()
    finally:
        remove_tree(sockdir)


def test_a_scheduler_lookup_does_not_wait_for_a_maruserver_that_is_down(
    fake, node, workers
):
    writer = workers()
    store(writer, rank_kv(1, 2))
    scheduler = make_scheduler(BLOCK, CHUNK, node_extra(node.config.socket))
    scheduler.connect_node()
    try:
        tokens = list(range(PROMPT))
        assert scheduler._count_matched_chunks(tokens) == 2
        fake.pause(hold=False)
        try:
            scheduler._count_matched_chunks(tokens)  # finds it down
            for _ in range(10):
                t0 = time.monotonic()
                assert scheduler._count_matched_chunks(tokens) == 0
                assert time.monotonic() - t0 < 0.05
        finally:
            fake.resume()
        wait_until(lambda: scheduler._count_matched_chunks(tokens) == 2, timeout=5)
    finally:
        scheduler.shutdown()


def test_a_refusal_stops_startup_but_only_disables_a_running_worker(fake, node, caplog):
    worker = make_worker(BLOCK, CHUNK, node_extra(node.config.socket))
    worker._page_size_bytes = 2 * PAGE  # an object larger than the node page
    try:
        with pytest.raises(ValueError, match="refused this worker"):
            worker._take_node_handler(wait=True)
    finally:
        worker.shutdown()
    # The same refusal later (a node restarted with smaller pages)
    worker = make_worker(BLOCK, CHUNK, node_extra(node.config.socket))
    worker._num_layers = NUM_LAYERS
    worker._page_size_bytes = 2 * PAGE
    try:
        with caplog.at_level("ERROR", logger="maru_vllm.connector"):
            worker._ensure_handler()
            wait_until(lambda: worker._ensure_handler() or worker._node_refused)
        assert "serves without the cache" in caplog.text
        kv = rank_kv(1, 2)
        prefill(worker, kv)  # skipped, no exception
        load_into(worker, [torch.zeros_like(layer) for layer in kv], None)
        assert worker.take_failed_load_blocks() == set(range(NUM_BLOCKS))
        assert fake.keys() == set()
    finally:
        worker.shutdown()


# =============================================================================
# Stores: non-blocking WRITE_DONE, chunk-atomic layerwise chunks
# =============================================================================


def store_meta(req_id: str, offset: int) -> MaruReqMeta:
    """A prefill step that stores a whole prompt of tokens from ``offset``."""
    return MaruReqMeta(
        req_id=req_id,
        token_ids=list(range(offset, offset + PROMPT)),
        block_ids=list(range(NUM_BLOCKS)),
        is_store=True,
        num_scheduled_tokens=PROMPT,
    )


def save_step(worker, kv, requests) -> None:
    metadata = MaruConnectorMetadata(requests=list(requests))
    attn = make_flash_attn_metadata()
    for name, layer in zip(LAYER_NAMES, kv, strict=True):
        worker.save_kv_layer(name, layer, attn, metadata)


def stored_keys(fake) -> set[str]:
    return {k.key for k in fake.keys()}


def chunk_keys_of(offset: int) -> list[str]:
    return _chunk_keys(list(range(offset, offset + PROMPT)), CHUNK)


@pytest.fixture
def nodes_small(fake):
    """Nodes whose workers get exactly ``credit_pages`` of credit at once."""
    started = []

    def make(credit_pages: int, total_pages: int = 8, **options):
        sockdir = short_tmpdir()
        budgets = {
            "credit_max_bytes": credit_pages * PAGE,
            "credit_total_max_bytes": total_pages * PAGE,
            "credit_refill_low": credit_pages * PAGE,
            "credit_refill_high": credit_pages * PAGE,
            "commit_pending_max_bytes": total_pages * PAGE,
        }
        service = start_node(
            node_config(fake.url, f"{sockdir}/node.sock", pages=128, budgets=budgets),
            **options,
        )
        started.append((service, sockdir))
        return service

    yield make
    for service, sockdir in started:
        service.stop()
        remove_tree(sockdir)


def layerwise_writer(node):
    writer = make_worker(
        BLOCK,
        CHUNK,
        node_extra(node.config.socket, maru_use_layerwise=True),
        head_size=HEAD_SIZE,
    )
    writer._num_layers = NUM_LAYERS
    writer._page_size_bytes = 2 * CHUNK * 2 * HEAD_SIZE * 4
    writer._take_node_handler(wait=True)
    return writer


def test_layerwise_steps_store_every_chunk_on_the_credit_they_free(fake, nodes_small):
    """Each step takes all the credit a worker gets; its WRITE_DONEs free it
    for the next step at once."""
    writer = layerwise_writer(nodes_small(credit_pages=16, total_pages=32))
    client = writer._handler._cpu
    kv = rank_kv(1, 2)
    per_step = 2  # requests: 2 x 2 chunks x (3 layers + marker) = 16 pages
    try:
        for step in range(4):
            wait_until(lambda: client.credits.unused_bytes >= 16 * PAGE, timeout=2.0)
            save_step(
                writer,
                kv,
                [
                    store_meta(f"r{step}{i}", 1000 * (step * per_step + i))
                    for i in range(per_step)
                ],
            )
        drain(writer)
        stored = stored_keys(fake)
        for n in range(4 * per_step):
            for key in chunk_keys_of(1000 * n):
                layers = {f"{key}_L{i}" for i in range(NUM_LAYERS)}
                assert layers | {f"{key}_DONE"} <= stored
        assert client.stats["store_skipped_no_credit"] == 0
        assert not writer._pending_layer_objects
    finally:
        writer.shutdown()


def test_a_chunk_short_of_credit_is_dropped_whole(fake, nodes_small, caplog):
    """3 layers and a marker need 4 pages; the worker holds 3."""
    writer = layerwise_writer(nodes_small(credit_pages=3))
    client = writer._handler._cpu
    try:
        wait_until(lambda: client.credits.unused_bytes >= 3 * PAGE)
        with caplog.at_level("WARNING", logger="maru_vllm.connector"):
            save_step(writer, rank_kv(1, 2), [store_meta("r1", 0)])
            drain(writer)
        assert fake.keys() == set()  # no layer of a dropped chunk
        assert not writer._pending_layer_objects
        assert client.local_stats()["allocations"] == 0  # every page back
        assert client.credits.unused_bytes == 3 * PAGE
        assert client.stats["store_skipped_no_credit"] >= 1
        assert "Maru store skipped" in caplog.text
    finally:
        writer.shutdown()


def test_an_unfinished_layerwise_chunk_is_dropped_at_the_step_boundary(
    fake, node, workers
):
    writer = workers(maru_use_layerwise=True)
    wait_credit(writer)
    client = writer._handler._cpu
    kv = rank_kv(1, 2)
    metadata = MaruConnectorMetadata(requests=[store_meta("r1", 0)])
    attn = make_flash_attn_metadata()
    for name, layer in list(zip(LAYER_NAMES, kv, strict=True))[:2]:
        writer.save_kv_layer(name, layer, attn, metadata)
    assert len(writer._pending_layer_objects) == 2
    writer._reclaim_stale_pending_slabs()
    assert not writer._pending_layer_objects and not writer._skipped_slabs
    assert client.local_stats()["allocations"] == 0
    drain(writer)
    assert fake.keys() == set()


def test_store_steps_do_not_wait_for_maruserver_commits(fake, nodes_small):
    """Every chunk's WRITE_DONE is in flight at once: draining takes two
    commit rounds (layers, then markers), not one per layer."""
    from maru_common.protocol import MessageType

    node = nodes_small(credit_pages=16, total_pages=32, rpc_timeout_s=3.0)
    writer = layerwise_writer(node)
    try:
        wait_until(lambda: writer._handler._cpu.credits.unused_bytes >= 8 * PAGE)
        fake.delay(MessageType.COMMIT, 0.5)
        t0 = time.monotonic()
        save_step(writer, rank_kv(1, 2), [store_meta("r1", 0)])
        assert time.monotonic() - t0 < 0.2
        drain(writer)
        assert time.monotonic() - t0 < 1.6  # one round per layer: 2 s
        assert len(fake.keys()) == 2 * (NUM_LAYERS + 1)
    finally:
        writer.shutdown()


def test_write_behind_releases_requests_once_their_blocks_are_read():
    worker = pinning_worker()
    worker._write_behind = True
    future = Future()  # MaruServer has not answered
    worker._handler.store_async.return_value = future
    wait_on(worker, "r1", "k")
    worker._handler.has_local.return_value = False
    worker._finish_write_behind_store(None, ["k"], ["handle"], [])
    assert worker.get_finished_saving({"r1"}) == {"r1"}
    assert worker._store_pending("k")  # not stored again meanwhile
    future.set_result([True])
    assert not worker._store_pending("k")


def test_a_request_joining_after_the_copy_waits_for_the_commit():
    """The copy's own waiters finish once their blocks are read; a request
    that joins the key afterwards finishes with MaruServer's answer."""
    worker = pinning_worker()
    worker._write_behind = True
    future = Future()
    worker._handler.store_async.return_value = future
    wait_on(worker, "r1", "k")
    worker._finish_write_behind_store(None, ["k"], ["handle"], [])
    wait_on(worker, "r2", "k")  # joins the key still pending
    assert worker.get_finished_saving({"r1", "r2"}) == {"r1"}
    future.set_result([True])
    assert worker.get_finished_saving(set()) == {"r2"}


def test_shutdown_waits_a_bounded_time_for_maruserver(fake, node, workers, monkeypatch):
    monkeypatch.setattr(connector_module, "_NODE_STORE_DRAIN_S", 0.5)
    writer = workers()
    wait_credit(writer)
    kv = rank_kv(1, 2)
    fake.pause(hold=True)
    try:
        save_step(writer, kv, [store_meta("r1", 0)])
        assert writer._node_store_futures
        t0 = time.monotonic()
        writer.shutdown()
        assert time.monotonic() - t0 < 6.0
    finally:
        fake.resume()


def test_no_kernel_refusal_is_cached_before_the_handler_connects():
    worker = make_worker(BLOCK, CHUNK, node_extra())
    worker._num_layers = NUM_LAYERS
    worker._kv_caches = dict(zip(LAYER_NAMES, rank_kv(1, 2), strict=True))
    assert worker._handler is None
    assert worker._packed_store_kernel_ctx(None) is None
    assert not worker._store_kernel_unusable
    layers = [(name, kv, i) for i, (name, kv) in enumerate(worker._kv_caches.items())]
    assert worker._loader_kernel_ctx(layers, None, None) is None
    assert worker._loader_kernel_cache is None


# =============================================================================
# Loads: the external range only, failures reported once
# =============================================================================


SENTINEL = -7.0


def sentinel_kv(heads: int = 2) -> list[torch.Tensor]:
    return [torch.full_like(layer, SENTINEL) for layer in rank_kv(0, heads)]


def forward_of(dst) -> SimpleNamespace:
    return SimpleNamespace(
        no_compile_layers={
            name: SimpleNamespace(kv_cache=layer)
            for name, layer in zip(LAYER_NAMES, dst, strict=True)
        },
        attn_metadata=make_flash_attn_metadata(),
    )


def external_meta(parked: bool, **kwargs) -> MaruReqMeta:
    """vLLM's prefix cache holds block 0; the prompt ends on a chunk, so the
    last block is computed, not loaded, and was not allocated for the load."""
    return MaruReqMeta(
        req_id="r1",
        token_ids=list(range(PROMPT)),
        block_ids=list(range(NUM_BLOCKS - 1)),
        is_store=False,
        num_matched_chunks=2,
        load_start_token=BLOCK,
        load_end_token=PROMPT - BLOCK,
        deferred_load=parked,
        **kwargs,
    )


def assert_only_external_blocks(dst, kv) -> None:
    for got, want in zip(dst, kv, strict=True):
        assert torch.equal(got[1:3], want[1:3])
        assert bool((got[0] == SENTINEL).all()) and bool((got[3] == SENTINEL).all())


@pytest.mark.parametrize("layerwise", [False, True])
@pytest.mark.parametrize("parked", [False, True])
def test_a_load_writes_only_the_external_range(fake, node, workers, layerwise, parked):
    extra = {"maru_use_layerwise": True} if layerwise else {}
    writer, reader = workers(**extra), workers(**extra)
    kv = rank_kv(1, 2)
    store(writer, kv)
    dst = sentinel_kv()
    reader.start_load_kv(
        forward_of(dst), MaruConnectorMetadata(requests=[external_meta(parked)])
    )
    if parked:
        assert reader.get_finished_loading() == {"r1"}
    assert reader.take_failed_load_blocks() == set()
    assert_only_external_blocks(dst, kv)
    assert reads_held(reader) == 0


@pytest.mark.parametrize("layerwise", [False, True])
def test_an_overlap_activation_writes_only_the_external_range(
    fake, node, workers, layerwise
):
    extra = {"maru_use_layerwise": True} if layerwise else {}
    writer, reader = workers(**extra), workers(**extra)
    kv = rank_kv(1, 2)
    store(writer, kv)
    meta = external_meta(True, layerwise_load=True)
    ct = CHUNK
    chunk_keys = reader._slice_chunk_keys(meta)
    plan = reader._node_load_plan(meta, chunk_keys[:2])
    assert plan is not None
    keys, slots = plan
    dst = sentinel_kv()
    layers = [
        (name, layer, i)
        for i, (name, layer) in enumerate(zip(LAYER_NAMES, dst, strict=True))
    ]
    infos = reader._batch_retrieve_all(
        connector_module._load_keys(keys, len(keys), layers, layerwise)
    )
    payload = (
        {i: infos[i * len(keys) : (i + 1) * len(keys)] for i in range(NUM_LAYERS)}
        if layerwise
        else infos
    )
    reader._deferred_layerwise_loads["r1"] = (meta, len(keys), slots, payload)
    reader._schedule_deferred_layerwise_loads(
        layers, {"r1"}, make_flash_attn_metadata()
    )
    assert reader.take_failed_load_blocks() == set()
    assert_only_external_blocks(dst, kv)
    assert reads_held(reader) == 0
    assert ct == CHUNK


def test_a_failed_overlap_activation_reports_its_external_blocks_once(
    fake, node, workers, monkeypatch
):
    writer, reader = workers(), workers()
    store(writer, rank_kv(1, 2))
    meta = external_meta(True, layerwise_load=True)
    plan = reader._node_load_plan(meta, reader._slice_chunk_keys(meta)[:2])
    keys, slots = plan
    dst = sentinel_kv()
    layers = [
        (name, layer, i)
        for i, (name, layer) in enumerate(zip(LAYER_NAMES, dst, strict=True))
    ]
    infos = reader._batch_retrieve_all(keys)
    reader._deferred_layerwise_loads["r1"] = (meta, len(keys), slots, infos)

    def boom(*args, **kwargs):
        raise RuntimeError("inject failed")

    monkeypatch.setattr(reader, "_inject_kv_into_layer", boom)
    reader._schedule_deferred_layerwise_loads(
        layers, {"r1"}, make_flash_attn_metadata()
    )
    # The external blocks only, and no second load-finished report
    assert reader.take_failed_load_blocks() == {1, 2}
    assert reader.get_finished_loading() is None
    assert "r1" in reader._unsound_kv_req_ids
    assert reads_held(reader) == 0


@pytest.mark.skipif(not torch.cuda.is_available(), reason="needs CUDA")
def test_a_failed_overlap_activation_on_cuda_reports_its_external_blocks_once(
    monkeypatch,
):
    worker = pinning_worker()
    device = torch.device("cuda")
    dst = [layer.to(device) for layer in sentinel_kv()]
    layers = [
        (name, layer, i)
        for i, (name, layer) in enumerate(zip(LAYER_NAMES, dst, strict=True))
    ]
    meta = external_meta(True, layerwise_load=True)
    slots = worker._build_slot_mapping(meta.block_ids, PROMPT - BLOCK)
    worker._deferred_layerwise_loads["r1"] = (meta, 2, slots, [lease("a"), lease("b")])

    def boom(*args, **kwargs):
        raise RuntimeError("copy failed")

    monkeypatch.setattr(worker, "_copy_chunkwise_layer_to_device", boom)
    worker._num_layers = NUM_LAYERS
    worker._schedule_deferred_layerwise_loads(layers, {"r1"}, None)
    assert worker.take_failed_load_blocks() == {1, 2}
    with worker._deferred_lock:
        assert "r1" not in worker._deferred_done
    assert "r1" in worker._unsound_kv_req_ids


@pytest.mark.skipif(not torch.cuda.is_available(), reason="needs CUDA")
def test_an_overlap_activation_that_cannot_be_drained_stops_the_worker(monkeypatch):
    worker = pinning_worker()
    device = torch.device("cuda")
    dst = [layer.to(device) for layer in sentinel_kv()]
    layers = [
        (name, layer, i)
        for i, (name, layer) in enumerate(zip(LAYER_NAMES, dst, strict=True))
    ]
    meta = external_meta(True, layerwise_load=True)
    slots = worker._build_slot_mapping(meta.block_ids, PROMPT - BLOCK)
    worker._deferred_layerwise_loads["r1"] = (meta, 2, slots, [lease("a"), lease("b")])
    worker._load_stream_device = dst[0].device
    worker._load_stream = FailingSyncStream(torch.cuda.Stream(dst[0].device))

    def boom(*args, **kwargs):
        raise RuntimeError("copy failed")

    monkeypatch.setattr(worker, "_copy_chunkwise_layer_to_device", boom)
    worker._num_layers = NUM_LAYERS
    worker._schedule_deferred_layerwise_loads(layers, {"r1"}, None)
    worker._handler.release_retrieved.assert_not_called()
    assert not worker._failed_load_blocks
    assert stopped(worker, "req r1")


def test_a_node_load_retrieves_its_keys_in_one_call():
    worker = pinning_worker()
    worker._handler.batch_retrieve.side_effect = lambda keys, **_: [None] * len(keys)
    keys = [f"k{i}" for i in range(2000)]
    assert len(worker._batch_retrieve_all(keys)) == 2000
    assert worker._handler.batch_retrieve.call_count == 1


def test_finished_loads_release_the_reads_of_completed_copies():
    worker = pinning_worker()
    read = lease("a")
    worker._active_load_refs = [(Done(), [[read]])]
    worker.get_finished_loading()
    assert released(worker) == [read]
    assert worker._active_load_refs == []


# =============================================================================
# A load copy that cannot be confirmed finished stops the worker
# =============================================================================


class FailedQuery(Done):
    def query(self):
        raise cuda_error()


UNCONFIRMED = "could not be confirmed finished"


def stopped(worker, match: str) -> bool:
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
        lambda: worker.want_step_credit(empty),
        lambda: worker.wait_for_layer_load(LAYER_NAMES[0]),
        lambda: worker.save_kv_layer(LAYER_NAMES[0], torch.zeros(1), None, empty),
        lambda: worker.wait_for_save(empty),
    )
    for call in calls:
        with pytest.raises(_UnconfirmedCopyError, match=match):
            call()
    return is_kept(worker)


def group_meta(**kwargs) -> MaruReqMeta:
    return MaruReqMeta(
        "r1",
        list(range(PROMPT)),
        [1, 2],
        False,
        num_matched_chunks=1,
        group_block_ids=[[1, 2]],
        **kwargs,
    )


@pytest.mark.parametrize("parked", [True, False], ids=["parked", "layerwise"])
def test_copies_that_cannot_be_asked_about_stop_the_worker(parked):
    worker = pinning_worker()
    read = lease("a")
    if parked:
        worker._deferred_events["r1"] = FailedQuery()
        worker._deferred_refs["r1"] = [[read]]
    else:
        worker._active_load_refs = [(FailedQuery(), [[read]])]
    with pytest.raises(_UnconfirmedCopyError):
        worker.get_finished_loading()
    worker._handler.release_retrieved.assert_not_called()
    worker._write_behind = True
    worker._queued_store_batches = [("kernel", MaruConnectorMetadata())]
    worker._store_packed_slabs_write_behind = MagicMock()
    assert stopped(worker, "req r1" if parked else UNCONFIRMED)
    # Nor does a stopped worker issue the stores the step queued
    with pytest.raises(_UnconfirmedCopyError):
        worker.get_finished_saving({"r1"})
    worker._store_packed_slabs_write_behind.assert_not_called()


def test_an_undrained_copy_into_reclaimed_blocks_stops_the_worker():
    """A request aborted while its layers load: its blocks go back to vLLM
    only once its copies are known to have ended."""
    worker = pinning_worker()
    worker._layerwise_overlap = True
    worker._deferred_layerwise_events["r1"] = {"layer": FailedWait()}
    worker._deferred_events["r1"] = Pending()
    worker._deferred_refs["r1"] = [[lease("a")]]
    with pytest.raises(_UnconfirmedCopyError, match="req r1"):
        worker.handle_preemptions(MaruConnectorMetadata(finished_req_ids={"r1"}))
    worker._handler.release_retrieved.assert_not_called()
    assert "r1" not in worker._deferred_done
    assert stopped(worker, "req r1")


def test_a_group_batch_whose_copies_cannot_be_waited_for_keeps_its_reads(
    monkeypatch,
):
    """One slot: the first of two batches waits for its copies."""
    worker = pinning_worker()
    reads = [lease("a"), lease("b")]
    worker._handler.retrieve_capacity.return_value = 1
    plan = [(None, 0, None), (None, 1, None)]
    monkeypatch.setattr(worker, "_group_load_plan", lambda m: (1, plan, ["a", "b"]))
    monkeypatch.setattr(
        worker, "_batch_retrieve_all", lambda keys, **r: reads[: len(keys)]
    )
    monkeypatch.setattr(worker, "_place_group_objects", lambda *a, **k: None)
    loaded = worker._load_group_leased(
        group_meta(), torch.device("cpu"), FailedWait(), offthread=True
    )
    assert loaded is None
    worker._handler.release_retrieved.assert_not_called()
    assert not worker._failed_load_blocks and not worker._failed_recv_req_ids
    assert holds(worker, reads[0])
    assert stopped(worker, "req r1")


@pytest.mark.parametrize("drained", [True, False])
def test_a_failed_parked_group_load_is_recomputed_only_once_drained(
    monkeypatch, drained
):
    """A load that failed before its copies, or whose copies ended, is a plain
    recompute; one whose copies cannot be waited for stops the worker."""
    worker = pinning_worker()
    monkeypatch.setattr(torch.cuda, "set_device", lambda device: None)
    monkeypatch.setattr(torch.cuda, "stream", lambda s: contextlib.nullcontext())
    worker._deferred_stream = Done() if drained else FailedWait()
    worker._deferred_stream.wait_event = lambda event: None
    worker._deferred_stream_device = torch.device("cpu")
    worker._load_group_leased = MagicMock(side_effect=RuntimeError("copy failed"))
    worker._group_load_job(group_meta(deferred_load=True), torch.device("cpu"), Done())
    if drained:
        assert worker._failed_recv_req_ids == {"r1"}
        assert worker._deferred_done == {"r1"}
        assert worker._stopped is None
    else:
        assert not worker._failed_recv_req_ids and not worker._deferred_done
        assert stopped(worker, "req r1")


def stop_at_r1(worker, calls: list):
    """A load that stops the worker at r1 and notes every request it gets."""

    def load(*args, **kwargs):
        req_meta = next(a for a in args if isinstance(a, MaruReqMeta))
        calls.append(req_meta.req_id)
        if req_meta.req_id == "r1":
            worker._stop_if_unconfirmed(cuda_error(), req_id="r1")

    return load


def three(meta: MaruReqMeta) -> MaruConnectorMetadata:
    return MaruConnectorMetadata(
        requests=[dataclasses.replace(meta, req_id=r) for r in ("r1", "r2", "r3")]
    )


def test_a_stop_leaves_the_later_loads_of_the_step_unread():
    worker = pinning_worker()
    calls = []
    worker._load_leased_request = stop_at_r1(worker, calls)
    with pytest.raises(_UnconfirmedCopyError, match="req r1"):
        worker.start_load_kv(forward_of(sentinel_kv()), three(external_meta(False)))
    assert calls == ["r1"]


def test_a_stop_leaves_the_later_group_loads_of_the_step_unread():
    worker = pinning_worker()
    worker._group_caches = [SimpleNamespace(tensors=[torch.zeros(1)])]
    calls = []
    worker._load_group_leased = stop_at_r1(worker, calls)
    worker._start_group_loads(three(group_meta()))
    assert calls == ["r1"]
    assert stopped(worker, "req r1")


def test_an_inline_parked_group_load_is_done_once_its_batches_ended():
    worker = pinning_worker()
    worker._group_caches = [SimpleNamespace(tensors=[torch.zeros(1)])]
    worker._load_group_leased = MagicMock(return_value=(1, 2))
    meta = group_meta(deferred_load=True)
    worker._start_group_loads(MaruConnectorMetadata(requests=[meta]))
    assert worker._deferred_done == {"r1"}


def test_a_queued_load_job_reads_nothing_once_the_worker_stopped():
    worker = pinning_worker()
    worker._stop_if_unconfirmed(cuda_error(), req_id="r0")
    worker._batch_retrieve_all = MagicMock()
    worker._load_group_leased = MagicMock()
    worker._inflight_deferred_req_ids.add("r1")
    worker._deferred_load_job(external_meta(True), [], torch.device("cpu"))
    worker._group_load_job(group_meta(deferred_load=True), torch.device("cpu"), Done())
    worker._batch_retrieve_all.assert_not_called()
    worker._load_group_leased.assert_not_called()
    assert not worker._inflight_deferred_req_ids and not worker._deferred_done


def test_a_group_load_stopped_during_retrieve_keeps_its_reads_unplaced(monkeypatch):
    worker = pinning_worker()
    worker._handler.retrieve_capacity.return_value = None
    reads = [lease("held")]
    monkeypatch.setattr(
        worker, "_group_load_plan", lambda meta: (1, [(None, 0)], ["key"])
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

    assert worker._stopped is not None
    worker._handler.release_retrieved.assert_not_called()  # kept once stopped
    assert holds(worker, reads[0])
    worker._place_group_objects.assert_not_called()
    stream.synchronize.assert_not_called()
    assert loaded is None
    assert not worker._failed_load_blocks and not worker._failed_recv_req_ids


def test_a_load_stopped_during_admission_keeps_its_reads_unsubmitted(monkeypatch):
    worker = pinning_worker()
    worker._node_mode = worker._leased_mode = False
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
        reads.extend(SimpleNamespace(view=memoryview(bytearray(64))) for _ in keys)
        return reads

    monkeypatch.setattr(worker, "_batch_retrieve_all", retrieve)
    with ThreadPoolExecutor(max_workers=1) as executor:
        load = executor.submit(
            worker._deferred_load_job,
            external_meta(True),
            [("layer", torch.zeros(1), 0)],
            torch.device("cpu"),
        )
        try:
            assert entered.wait(5)
            worker._stop_if_unconfirmed(cuda_error(), req_id="other")
        finally:
            resume.set()
        load.result(5)

    assert worker._stopped is not None
    worker._handler.release_retrieved.assert_not_called()  # kept once stopped
    assert holds(worker, reads[0])
    worker._inject_slab_layers.assert_not_called()
    event.assert_not_called()
    assert "r1" not in worker._deferred_events and "r1" not in worker._deferred_refs
    assert not worker._inflight_deferred_req_ids and not worker._deferred_done
    assert not worker._failed_load_blocks and not worker._failed_recv_req_ids


# =============================================================================
# A parked load does not hold the loader thread for its copies
# =============================================================================


class Copies:
    """The event after a batch's copies on a fake loader stream: they end
    when ``ended`` is set or once waited for. ``error`` fails any question
    about them, ``wait_error`` only a wait."""

    def __init__(self, log: list[str], name: str):
        self.log, self.name = log, name
        self.ended = False
        self.error: Exception | None = None
        self.wait_error: Exception | None = None

    def query(self) -> bool:
        if self.error is not None:
            raise self.error
        return self.ended

    def synchronize(self) -> None:
        self.log.append(f"wait {self.name}")
        error = self.error or self.wait_error
        if error is not None:
            raise error
        self.ended = True


class LoaderStream:
    def __init__(self, log: list[str]):
        self.log = log
        self.events: list[Copies] = []

    def record_event(self) -> Copies:
        self.events.append(Copies(self.log, f"e{len(self.events)}"))
        return self.events[-1]

    def synchronize(self) -> None:
        for event in self.events:
            event.synchronize()


def parked_loader(monkeypatch, slots: int | None = None, window: int = 0):
    """A maru_node worker whose loader thread reads one chunk per batch and
    copies it on a fake stream; ``slots`` bounds the reads it may hold, and
    ``window`` is its ``maru_load_admission_window``.

    Returns:
        The worker, its loader stream, the log, and every read in order.
    """
    monkeypatch.setattr(connector_module, "_LOAD_PIPELINE_CHUNKS", 1)
    monkeypatch.setattr(torch.cuda, "set_device", lambda device: None)
    log: list[str] = []
    stream = LoaderStream(log)
    monkeypatch.setattr(torch.cuda, "Stream", lambda device=None, priority=0: stream)
    worker = pinning_worker()
    worker._load_admission_window = window
    worker._last_attn_metadata = make_flash_attn_metadata()
    worker._pin_slot_mapping_for_async_h2d = lambda slots: slots
    reads: list[CpuReadLease] = []

    def retrieve(keys, **route):
        """LOOKUP_AND_PIN and FETCH, answered at once."""
        log.append(f"read {len(reads)}")
        reads.extend(lease(key) for key in keys)
        answered: Future = Future()
        answered.set_result(reads[-len(keys) :])
        return answered

    def copy(layers, entries, attn, stream=None, wait=True):
        log.append(f"copy {entries[0][0].req_id}")
        return set()

    worker._load_packed = copy
    handler = worker._handler
    handler.start_retrieve.side_effect = retrieve
    handler.release_retrieved.side_effect = lambda infos: log.append(
        f"release {[reads.index(info) for info in infos if info is not None]}"
    )
    handler.retrieve_capacity.side_effect = lambda _wanted=None: (
        None if slots is None else slots - len(reads) + len(released(worker))
    )
    return worker, stream, log, reads


def parked_meta(req_id: str) -> MaruReqMeta:
    """A parked load of two whole chunks."""
    return MaruReqMeta(
        req_id=req_id,
        token_ids=list(range(PROMPT)),
        block_ids=list(range(NUM_BLOCKS)),
        is_store=False,
        num_matched_chunks=2,
        load_end_token=PROMPT,
        deferred_load=True,
    )


def park(worker, req_id: str) -> MaruReqMeta:
    """Run the parked load of ``req_id`` as the loader thread does."""
    meta = parked_meta(req_id)
    worker._deferred_load_job(meta, [("layer", torch.zeros(1), 0)], torch.device("cpu"))
    return meta


def test_the_next_parked_load_starts_while_the_copies_of_the_last_run(monkeypatch):
    worker, stream, log, reads = parked_loader(monkeypatch)
    park(worker, "r1")
    park(worker, "r2")
    # Nothing waited for r1's copies before r2 was read and copied
    assert log == [
        *("read 0", "copy r1", "read 1", "copy r1"),
        *("read 2", "copy r2", "read 3", "copy r2"),
    ]
    assert not worker._deferred_done
    worker._handler.release_retrieved.assert_not_called()
    for event in stream.events:
        event.ended = True
    assert worker.get_finished_loading() == {"r1", "r2"}
    assert released(worker) == reads


def test_a_parked_load_waits_for_no_more_slots_than_one_batch(monkeypatch):
    """It reads _LOAD_PIPELINE_CHUNKS (1 here) at a time: no capacity check
    waits for room for the whole request."""
    worker, _, _, _ = parked_loader(monkeypatch, slots=4)
    capacity = worker._handler.retrieve_capacity.side_effect
    asked: list[int | None] = []

    def recorded(wanted=None):
        asked.append(wanted)
        return capacity(wanted)

    worker._handler.retrieve_capacity.side_effect = recorded
    park(worker, "r1")
    assert asked and all(wanted == 1 for wanted in asked)


def test_a_parked_load_gives_back_each_batch_once_its_copies_end(monkeypatch):
    """And is reported, its chunks loaded, once its last batch's copies did."""
    worker, stream, log, reads = parked_loader(monkeypatch)
    meta = park(worker, "r1")
    first, last = stream.events
    first.ended = True
    assert worker.get_finished_loading() is None
    assert released(worker) == reads[:1]
    assert not worker._loaded_request_keys["r1"][1]
    last.ended = True
    assert worker.get_finished_loading() == {"r1"}
    assert released(worker) == reads
    assert worker._loaded_request_keys["r1"][1] == set(worker._slice_chunk_keys(meta))
    assert not worker._parked_loads


def test_a_load_short_of_slots_waits_for_the_oldest_parked_batch(monkeypatch):
    """Two slots: each batch of r2 is read once a batch of r1 gave its slot
    back, and r1 is reported once its last one did."""
    worker, stream, log, reads = parked_loader(monkeypatch, slots=2)
    park(worker, "r1")
    park(worker, "r2")
    assert log[4:] == [
        *("wait e0", "release [0]", "read 2", "copy r2"),
        *("wait e1", "release [1]", "read 3", "copy r2"),
    ]
    assert worker.get_finished_loading() == {"r1"}
    assert not worker._failed_load_blocks


@pytest.mark.parametrize("asked", ["query", "wait"])
def test_a_parked_batch_whose_copies_cannot_be_confirmed_stops_the_worker(
    monkeypatch, asked
):
    """Asked about by vLLM's poll, or waited for by the next load short of
    slots: r1's batches keep their reads and r1 is not reported."""
    worker, stream, log, reads = parked_loader(monkeypatch, slots=2)
    park(worker, "r1")
    first, _ = stream.events
    if asked == "query":
        first.error = cuda_error()
        with pytest.raises(_UnconfirmedCopyError, match="req r1"):
            worker.get_finished_loading()
    else:
        first.wait_error = cuda_error()
        park(worker, "r2")
        assert "wait e0" in log and "read 2" not in log
    worker._handler.release_retrieved.assert_not_called()
    assert holds(worker, reads[0])
    # The parked load keeps both batches, popped by none
    assert [batch for _, batch, _ in worker._parked_loads["r1"][1]] == [
        reads[:1],
        reads[1:],
    ]
    assert not worker._deferred_done and not worker._failed_load_blocks
    assert stopped(worker, "req r1")


def test_a_parked_batch_whose_wait_fails_otherwise_is_recomputed_from_it(
    monkeypatch,
):
    """Not a CUDA error: as a leased batch's failed wait, the load is
    recomputed from that batch and its batches keep their slots."""
    worker, stream, log, reads = parked_loader(monkeypatch, slots=2)
    park(worker, "r1")
    first, _ = stream.events
    first.wait_error = RuntimeError("wait failed")
    park(worker, "r2")
    assert worker._stopped is None and "r1" not in worker._parked_loads
    assert "r1" not in [r for r in log if r.startswith("release")]
    worker._handler.release_retrieved.assert_not_called()
    assert worker._deferred_done >= {"r1"}
    assert worker.take_failed_load_blocks() == set(range(NUM_BLOCKS))


def test_a_request_aborted_while_parked_keeps_its_reads_until_its_copies_end(
    monkeypatch,
):
    """vLLM frees its blocks once its load is reported, after the copies
    writing them ended."""
    worker, stream, log, reads = parked_loader(monkeypatch)
    park(worker, "r1")
    worker.handle_preemptions(MaruConnectorMetadata(finished_req_ids={"r1"}))
    assert worker.get_finished_loading() is None
    worker._handler.release_retrieved.assert_not_called()
    for event in stream.events:
        event.ended = True
    assert worker.get_finished_loading() == {"r1"}
    assert released(worker) == reads
    assert "r1" not in worker._loaded_request_keys


@pytest.mark.parametrize("drained", [True, False])
def test_shutdown_gives_back_parked_reads_only_once_their_copies_drained(
    monkeypatch, drained
):
    worker, stream, log, reads = parked_loader(monkeypatch)
    park(worker, "r1")
    handler = worker._handler
    if not drained:
        stream.events[-1].wait_error = cuda_error()
    worker.shutdown()
    if drained:
        handler.release_retrieved.assert_called_once_with(reads)
        handler.close.assert_called_once()
    else:
        handler.release_retrieved.assert_not_called()
        handler.close.assert_not_called()
        parked = worker._parked_loads["r1"][1]
        assert [batch for _, batch, _ in parked] == [reads[:1], reads[1:]]
        assert is_kept(worker)


@pytest.mark.parametrize("window", [0, 1, 2])
def test_an_admission_window_bounds_the_parked_loads_in_flight(monkeypatch, window):
    """A load starts once fewer than ``window`` parked loads are copying, the
    oldest one's batches waited for first; 0 never waits."""
    worker, stream, log, reads = parked_loader(monkeypatch, window=window)
    for req_id in ("r1", "r2", "r3"):
        park(worker, req_id)
    r1, r2, r3 = (
        [f"read {i}", f"copy {r}", f"read {i + 1}", f"copy {r}"]
        for i, r in ((0, "r1"), (2, "r2"), (4, "r3"))
    )
    drain_r1 = ["wait e0", "release [0]", "wait e1", "release [1]"]
    drain_r2 = ["wait e2", "release [2]", "wait e3", "release [3]"]
    expected = {
        0: [*r1, *r2, *r3],
        1: [*r1, *drain_r1, *r2, *drain_r2, *r3],
        2: [*r1, *r2, *drain_r1, *r3],
    }
    assert log == expected[window]
    assert worker._deferred_done == [set(), {"r1", "r2"}, {"r1"}][window]


def test_the_loader_and_vllms_poll_give_back_each_batch_once(monkeypatch):
    """vLLM's poll gives back r1's batches while the loader waits for the
    oldest one: no batch goes back twice, and r1 is reported once."""
    worker, stream, log, reads = parked_loader(monkeypatch, slots=2)
    park(worker, "r1")
    first, last = stream.events
    waiting, resume = threading.Event(), threading.Event()
    wait = first.synchronize

    def held_wait():
        waiting.set()
        assert resume.wait(5)
        wait()

    first.synchronize = held_wait
    loader = threading.Thread(target=park, args=(worker, "r2"))
    loader.start()
    try:
        assert waiting.wait(5)
        first.ended = last.ended = True
        assert worker.get_finished_loading() == {"r1"}
    finally:
        resume.set()
        loader.join(5)
    assert not loader.is_alive()
    assert released(worker) == reads[:2]
    assert log[-5:] == ["wait e0", "read 2", "copy r2", "read 3", "copy r2"]
    for event in stream.events:
        event.ended = True
    assert worker.get_finished_loading() == {"r2"}
    assert released(worker) == reads


def kept_reads(worker) -> list[CpuReadLease]:
    """The reads a stopped worker keeps: in what it kept, or in its loads
    and the batches they wait on."""
    found: list[CpuReadLease] = []

    def walk(item) -> None:
        if isinstance(item, CpuReadLease):
            found.append(item)
        elif isinstance(item, list | tuple | deque):
            for each in item:
                walk(each)
        elif isinstance(item, dict):
            for each in item.values():
                walk(each)

    walk(kept_by(worker))
    walk(worker._parked_loads)
    walk([fetch.load.pending for fetch in worker._node_fetches])
    return found


@pytest.mark.parametrize("r2", ["loading", "parked"])
def test_a_failed_parked_batch_keeps_the_reads_of_the_loads_behind_it(monkeypatch, r2):
    """r2's batches are queued behind r1's on the stream: once r1's copies
    cannot be confirmed ended, neither can r2's."""
    worker, stream, log, reads = parked_loader(
        monkeypatch, slots=3 if r2 == "loading" else None
    )
    park(worker, "r1")
    first, _ = stream.events
    if r2 == "loading":
        # r2 copies a batch on the free slot, then waits for r1's oldest
        first.wait_error = cuda_error()
        park(worker, "r2")
        assert log[4:] == ["read 2", "copy r2", "wait e0"]
    else:
        park(worker, "r2")
        first.error = cuda_error()
        with pytest.raises(_UnconfirmedCopyError, match="req r1"):
            worker.get_finished_loading()
    worker._handler.release_retrieved.assert_not_called()
    assert {id(r) for r in kept_reads(worker)} == {id(r) for r in reads}
    assert not worker._deferred_done
    assert stopped(worker, "req r1")


# =============================================================================
# A parked group load does not hold the loader thread for its copies
# =============================================================================


def group_loader(monkeypatch, slots: int | None = None):
    """``parked_loader``'s worker on a model with several groups: a load
    reads two objects, in one batch unless ``slots`` are short."""
    worker, stream, log, reads = parked_loader(monkeypatch, slots=slots)
    monkeypatch.setattr(torch.cuda, "stream", lambda s: contextlib.nullcontext())
    stream.wait_event = lambda event: None
    plan = [(None, 0, None), (None, 1, None)]
    worker._group_load_plan = lambda meta: (1, plan, ["a", "b"])

    def retrieve(keys, **route):
        log.append(f"read {len(reads)}")
        reads.extend(lease(key) for key in keys)
        return reads[-len(keys) :]

    def place(meta, objects, device, stage, refs):
        log.append(f"copy {meta.req_id}")

    worker._handler.batch_retrieve.side_effect = retrieve
    worker._place_group_objects = place
    return worker, stream, log, reads


def park_group(worker, req_id: str) -> None:
    """Run the parked group load of ``req_id`` as the loader thread does."""
    meta = dataclasses.replace(group_meta(deferred_load=True), req_id=req_id)
    worker._group_load_job(meta, torch.device("cpu"), Done())


def test_a_parked_group_load_is_reported_once_its_copies_end(monkeypatch):
    """The loader goes on to the next load; a load's reads go back, and it is
    reported, once its own copies ended."""
    worker, stream, log, reads = group_loader(monkeypatch)
    park_group(worker, "r1")
    park_group(worker, "r2")
    assert log == ["read 0", "copy r1", "read 2", "copy r2"]
    assert set(worker._parked_loads) == {"r1", "r2"}
    assert worker.get_finished_loading() is None
    worker._handler.release_retrieved.assert_not_called()
    first, last = stream.events
    first.ended = True
    assert worker.get_finished_loading() == {"r1"}
    assert released(worker) == reads[:2]
    last.ended = True
    assert worker.get_finished_loading() == {"r2"}
    assert released(worker) == reads
    assert not worker._parked_loads
    assert not worker._failed_recv_req_ids and not worker._failed_load_blocks


def test_only_the_last_group_batch_parks(monkeypatch):
    """One slot: a load's first batch waits for its copies and gives its read
    back before the next is read, and the next load waits for the parked one
    first."""
    worker, stream, log, reads = group_loader(monkeypatch, slots=1)
    park_group(worker, "r1")
    assert log == ["read 0", "copy r1", "release [0]", "read 1", "copy r1"]
    park_group(worker, "r2")
    assert log[5:] == [
        *("wait e0", "release [1]"),
        # The first batch's wait covers the whole stream
        *("read 2", "copy r2", "wait e0", "release [2]", "read 3", "copy r2"),
    ]
    assert worker._deferred_done == {"r1"}
    assert set(worker._parked_loads) == {"r2"}
    assert worker._parked_loads["r2"][3] == 2  # batches


@pytest.mark.parametrize("asked", ["query", "wait"])
def test_a_parked_group_load_whose_copies_cannot_be_confirmed_stops_the_worker(
    monkeypatch, asked
):
    """Asked about by vLLM's poll, or waited for by the next load short of
    slots: r1 keeps its reads and is not reported, and r2 reads nothing."""
    worker, stream, log, reads = group_loader(monkeypatch, slots=2)
    park_group(worker, "r1")
    (event,) = stream.events
    if asked == "query":
        event.error = cuda_error()
        with pytest.raises(_UnconfirmedCopyError, match="req r1"):
            worker.get_finished_loading()
    else:
        event.wait_error = cuda_error()
        park_group(worker, "r2")
        assert log[-1] == "wait e0" and "read 2" not in log
    worker._handler.release_retrieved.assert_not_called()
    assert {id(r) for r in kept_reads(worker)} == {id(r) for r in reads}
    assert set(worker._parked_loads) == {"r1"} and not worker._deferred_done
    assert not worker._failed_recv_req_ids and not worker._failed_load_blocks
    assert stopped(worker, "req r1")


def test_a_group_load_aborted_while_parked_keeps_its_reads_until_its_copies_end(
    monkeypatch,
):
    """vLLM frees its blocks once its load is reported, after the copies
    writing them ended."""
    worker, stream, log, reads = group_loader(monkeypatch)
    park_group(worker, "r1")
    worker.handle_preemptions(MaruConnectorMetadata(finished_req_ids={"r1"}))
    assert worker.get_finished_loading() is None
    worker._handler.release_retrieved.assert_not_called()
    stream.events[0].ended = True
    assert worker.get_finished_loading() == {"r1"}
    assert released(worker) == reads


@pytest.mark.parametrize("drained", [True, False])
def test_shutdown_gives_back_parked_group_reads_only_once_drained(monkeypatch, drained):
    worker, stream, log, reads = group_loader(monkeypatch)
    park_group(worker, "r1")
    handler = worker._handler
    if not drained:
        stream.events[0].wait_error = cuda_error()
    worker.shutdown()
    if drained:
        handler.release_retrieved.assert_called_once_with(reads)
        handler.close.assert_called_once()
    else:
        handler.release_retrieved.assert_not_called()
        handler.close.assert_not_called()
        parked = worker._parked_loads["r1"][1]
        assert [batch[0] for _, batch, _ in parked] == [reads]
        assert is_kept(worker)


def test_a_group_miss_fails_only_its_own_request(monkeypatch):
    """r2's object is gone: r2 is recomputed, reported by its id as vLLM 0.32
    wants of a parked load with several groups, and r1 still loads."""
    worker, stream, log, reads = group_loader(monkeypatch)
    park_group(worker, "r1")
    retrieve = worker._handler.batch_retrieve.side_effect
    worker._handler.batch_retrieve.side_effect = lambda keys, **route: [
        *retrieve(keys[:1]),
        None,
    ]
    park_group(worker, "r2")
    assert "copy r2" not in log
    assert worker.get_finished_loading() == {"r1", "r2"}
    assert worker.take_failed_recv_req_ids({"r1", "r2"}) == {"r2"}
    assert not worker._failed_load_blocks and worker._stopped is None
    gone = [r for r in released(worker) if r is not None]
    assert {id(r) for r in gone} == {id(r) for r in reads} and len(gone) == 3


# =============================================================================
# A parked load's FETCH stays in flight while the next load starts
# =============================================================================


class Fetch:
    """A FETCH a fake node answers once ``answered`` is set or once waited
    for; ``error`` fails taking its answer."""

    def __init__(self, log: list[str], name: str, reads: list):
        self.log, self.name, self.reads = log, name, reads
        self.answered = False
        self.error: Exception | None = None

    def done(self) -> bool:
        return self.answered

    def result(self) -> list:
        if not self.answered:
            self.log.append(f"wait {self.name}")
            self.answered = True
        if self.error is not None:
            raise self.error
        return self.reads


def fetching_loader(monkeypatch, depth: int = 2, **kwargs):
    """parked_loader whose requests read their two chunks in one batch from
    a node that answers a FETCH once it is waited for; ``depth`` is its
    ``maru_load_fetch_depth``.

    Returns:
        The worker, its loader stream, the log, every read in order, and
        every FETCH in order.
    """
    worker, stream, log, reads = parked_loader(monkeypatch, **kwargs)
    monkeypatch.setattr(connector_module, "_LOAD_PIPELINE_CHUNKS", 2)
    worker._load_fetch_depth = depth
    fetches: list[Fetch] = []

    def start(keys, **route):
        log.append(f"read {len(reads)}")
        reads.extend(lease(key) for key in keys)
        fetches.append(Fetch(log, f"f{len(fetches)}", reads[-len(keys) :]))
        return fetches[-1]

    worker._handler.start_retrieve.side_effect = start
    return worker, stream, log, reads, fetches


def submit(worker, *req_ids: str) -> list[MaruReqMeta]:
    """Queue the parked loads of ``req_ids`` as _try_submit_deferred_load
    does; run() runs them."""
    metas = [parked_meta(req_id) for req_id in req_ids]
    with worker._loader_jobs_lock:
        worker._loader_jobs += len(metas)
    with worker._deferred_lock:
        for meta in metas:
            worker._inflight_deferred_req_ids.add(meta.req_id)
            worker._loaded_request_keys[meta.req_id] = (meta, set())
    return metas


def run(worker, *metas: MaruReqMeta) -> None:
    """Run queued loader jobs in order, on this thread."""
    for meta in metas:
        worker._in_fetch_scope(
            meta,
            worker._deferred_load_job,
            meta,
            [("layer", torch.zeros(1), 0)],
            torch.device("cpu"),
        )


def in_flight(worker) -> list[str]:
    return [load.req_meta.req_id for load in worker._node_fetches]


def loaded(worker, req_id: str) -> bool:
    return bool(worker._loaded_request_keys[req_id][1])


def test_a_fetch_depth_below_one_is_refused():
    with pytest.raises(ValueError, match="maru_load_fetch_depth"):
        make_worker(BLOCK, CHUNK, node_extra(maru_load_fetch_depth=0))
    assert make_worker(BLOCK, CHUNK, node_extra())._load_fetch_depth == 2


def test_the_next_load_sends_its_fetch_before_the_last_is_answered(monkeypatch):
    """r1's FETCH stays in flight while r2's job is queued: r2's LOOKUP and
    FETCH go out first, and each load's copies are queued only once its
    answer is taken."""
    worker, stream, log, reads, _ = fetching_loader(monkeypatch)
    r1, r2 = submit(worker, "r1", "r2")
    run(worker, r1)
    assert log == ["read 0"] and in_flight(worker) == ["r1"]
    assert "r1" in worker._inflight_deferred_req_ids
    run(worker, r2)
    assert log == ["read 0", "read 2", "wait f0", "copy r1", "wait f1", "copy r2"]
    assert not worker._node_fetches and not worker._inflight_deferred_req_ids
    for event in stream.events:
        event.ended = True
    assert worker.get_finished_loading() == {"r1", "r2"}
    assert released(worker) == reads
    assert loaded(worker, "r1") and loaded(worker, "r2")


@pytest.mark.parametrize("depth", [1, 2, 3])
def test_at_most_depth_loads_have_a_fetch_in_flight(monkeypatch, depth):
    """A load's FETCH goes out with fewer than ``depth`` others in flight,
    the oldest answer taken first; 1 takes each before the next load."""
    worker, _, log, _, _ = fetching_loader(monkeypatch, depth=depth)
    run(worker, *submit(worker, "r1", "r2", "r3"))
    r1, r2, r3 = ([f"wait f{i}", f"copy r{i + 1}"] for i in range(3))
    expected = {
        1: ["read 0", *r1, "read 2", *r2, "read 4", *r3],
        2: ["read 0", "read 2", *r1, "read 4", *r2, *r3],
        3: ["read 0", "read 2", "read 4", *r1, *r2, *r3],
    }
    assert log == expected[depth]


def test_an_answered_fetch_is_taken_before_older_ones(monkeypatch):
    """r2's copies need not wait for r1's FETCH, and r2 may finish first."""
    worker, stream, log, _, fetches = fetching_loader(monkeypatch, depth=3)
    r1, r2, r3 = submit(worker, "r1", "r2", "r3")
    run(worker, r1, r2)
    fetches[1].answered = True
    run(worker, r3)
    assert log == [
        *("read 0", "read 2", "read 4"),
        *("copy r2", "wait f0", "copy r1", "wait f2", "copy r3"),
    ]
    stream.events[0].ended = True  # r2's copies
    assert worker.get_finished_loading() == {"r2"}


@pytest.mark.parametrize("failure", ["miss", "error"])
def test_a_failed_fetch_fails_only_its_own_request(monkeypatch, failure):
    """r1's answer misses a chunk, or cannot be taken: r1 is recomputed at
    once, and r2, whose FETCH went out meanwhile, loads."""
    worker, stream, log, reads, fetches = fetching_loader(monkeypatch)
    r1, r2 = submit(worker, "r1", "r2")
    run(worker, r1)
    if failure == "miss":
        fetches[0].reads = [reads[0], None]
    else:
        fetches[0].error = StorageUnavailableError("UDS broke")
    run(worker, r2)
    taken = ["wait f0", "release [0]"] if failure == "miss" else ["wait f0"]
    assert log == ["read 0", "read 2", *taken, "wait f1", "copy r2"]
    assert worker._deferred_done == {"r1"} and worker._failed_load_blocks
    for event in stream.events:
        event.ended = True
    assert worker.get_finished_loading() == {"r1", "r2"}
    assert loaded(worker, "r2") and not loaded(worker, "r1")


@pytest.mark.parametrize("bound", ["slots", "admission"])
def test_a_load_short_of_room_takes_the_answer_in_flight_first(monkeypatch, bound):
    """Two slots, or an admission window of one load: r1's FETCH holds them,
    so r2 takes r1's answer and waits for its copies before it reads."""
    room = {"slots": 2} if bound == "slots" else {"window": 1}
    worker, _, log, _, _ = fetching_loader(monkeypatch, **room)
    r1, r2 = submit(worker, "r1", "r2")
    run(worker, r1)
    assert in_flight(worker) == ["r1"]
    run(worker, r2)
    assert log == [
        "read 0",
        *("wait f0", "copy r1", "wait e0", "release [0, 1]"),
        *("read 2", "wait f1", "copy r2"),
    ]


def test_a_stop_keeps_a_fetch_in_flight_unread(monkeypatch):
    """Its answer is not waited for nor given back: a stopped worker keeps
    the FETCH and the load it belongs to."""
    worker, _, log, _, fetches = fetching_loader(monkeypatch)
    r1, r2 = submit(worker, "r1", "r2")
    run(worker, r1)
    worker._stop_if_unconfirmed(cuda_error(), req_id="r0")
    run(worker, r2)
    assert log == ["read 0"]
    worker._handler.release_retrieved.assert_not_called()
    assert any(
        f.started is fetches[0] for f in kept_by(worker) if hasattr(f, "started")
    )
    assert not worker._node_fetches and not worker._deferred_done
    assert not worker._inflight_deferred_req_ids


def test_shutdown_takes_the_answer_of_a_fetch_in_flight_before_closing(monkeypatch):
    """The last loader job takes it and queues its copies; shutdown gives
    the reads back once they drained, then closes the handler."""
    worker, _, log, _, _ = fetching_loader(monkeypatch)
    worker._handler.close.side_effect = lambda: log.append("close")
    worker._deferred_executor = ThreadPoolExecutor(max_workers=1)
    hold, gate, waiting = threading.Event(), threading.Event(), threading.Event()

    def held() -> None:
        waiting.set()
        assert gate.wait(5)

    layers = [("layer", torch.zeros(1), 0)]
    meta = parked_meta("r1")
    worker._submit_load_job(parked_meta("hold"), hold.wait, 5)
    worker._submit_load_job(
        meta,
        worker._deferred_load_job,
        meta,
        layers,
        torch.device("cpu"),
    )
    worker._submit_load_job(parked_meta("gate"), held)
    hold.set()
    assert waiting.wait(5)
    # r1's job ended with its FETCH in flight: the gate job follows it
    assert log == ["read 0"] and in_flight(worker) == ["r1"]
    closing = threading.Thread(target=worker.shutdown)
    closing.start()
    gate.set()
    closing.join(10)
    assert not closing.is_alive()
    assert log == [
        *("read 0", "wait f0", "copy r1"),
        *("wait e0", "release [0, 1]", "close"),
    ]


def cuda_load_stream(monkeypatch, worker, stream) -> torch.device:
    """Have ``worker`` load on ``stream`` as if on a CUDA device."""
    device = torch.device("cuda")
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)
    monkeypatch.setattr(torch.cuda, "current_stream", lambda device=None: Done())
    monkeypatch.setattr(torch.cuda, "stream", lambda s: contextlib.nullcontext())
    stream.wait_stream = lambda other: None
    worker._load_stream, worker._load_stream_device = stream, device
    return device


@pytest.mark.parametrize("drained", [True, False])
def test_a_failed_group_placement_is_reported_only_once_drained(monkeypatch, drained):
    """Its first objects may be copying: vLLM recomputes into the blocks only
    once they ended, and never if they cannot be waited for."""
    worker = make_worker(BLOCK, CHUNK, {})
    worker._handler = MagicMock()
    device = cuda_load_stream(monkeypatch, worker, Done() if drained else FailedWait())
    worker._group_caches = [SimpleNamespace(tensors=[SimpleNamespace(device=device)])]
    worker._retrieve_group_objects = lambda meta: (1, [])
    worker._place_group_objects = MagicMock(side_effect=RuntimeError("place failed"))
    if drained:
        worker._start_group_loads(MaruConnectorMetadata(requests=[group_meta()]))
        assert worker.take_failed_load_blocks() == {1, 2}
    else:
        with pytest.raises(_UnconfirmedCopyError):
            worker._start_group_loads(MaruConnectorMetadata(requests=[group_meta()]))
        assert not worker._failed_load_blocks
        assert stopped(worker, "req r1")


@pytest.mark.parametrize("drained", [True, False])
def test_an_overlap_load_failing_after_its_copies_drains_them_first(drained):
    """On cxl too: a parked load that fails once its layer copies are queued
    is reported only after them."""
    worker = make_worker(BLOCK, CHUNK, {})
    worker._handler = MagicMock()
    worker._batch_retrieve_all = lambda keys, **route: [lease(k) for k in keys]
    worker._issue_layerwise_copies_offthread = lambda *a: {LAYER_NAMES[0]: Done()}
    worker._unpark_gate_event = MagicMock(side_effect=RuntimeError("no gate"))
    worker._layerwise_streams = [Done() if drained else FailedWait()]
    worker._inflight_deferred_req_ids.add("r1")
    layers = [(name, None, i) for i, name in enumerate(LAYER_NAMES)]
    meta = external_meta(True, layerwise_load=True)
    worker._deferred_load_job(meta, layers, torch.device("cpu"))
    if drained:
        assert worker._deferred_done == {"r1"} and worker._failed_load_blocks
    else:
        assert not worker._deferred_done and not worker._failed_load_blocks
        assert stopped(worker, "req r1")


@pytest.mark.parametrize("cuda", [True, False], ids=["cuda", "other"])
def test_a_failed_inline_layerwise_load_stops_only_on_a_cuda_error(cuda):
    """Its copies to the device are synchronous: another error recomputes
    the request's external blocks and its reads go back; a CUDA error stops
    the worker and keeps them."""
    worker = pinning_worker()
    worker._use_layerwise = True
    layers = [(LAYER_NAMES[0], torch.zeros(4), 0)]
    error = cuda_error() if cuda else RuntimeError("inject failed")
    worker._inject_kv_into_layer = MagicMock(side_effect=error)
    reads = [lease("a"), lease("b")]
    load = [(external_meta(False), 2, torch.arange(PROMPT), reads)]
    if cuda:
        with pytest.raises(_UnconfirmedCopyError, match="req r1"):
            worker._load_sync(layers, load, None)
        assert not worker._failed_load_blocks
        assert holds(worker, reads[0])
        assert stopped(worker, "req r1")
    else:
        assert worker._load_sync(layers, load, None) == 1
        assert worker.take_failed_load_blocks() == {1, 2}


# =============================================================================
# Handshake: the workers' transfer modes
# =============================================================================


def handshake(**modes) -> dict:
    return {0: MaruHandshakeMetadata("ns", **modes)}


def test_workers_that_transfer_otherwise_are_refused():
    scheduler = make_scheduler(BLOCK, CHUNK, node_extra(maru_async_load=True))
    scheduler.take_worker_namespace(handshake(async_load=True, async_store=False))
    scheduler.take_worker_namespace(handshake())  # says nothing: taken
    with pytest.raises(ValueError, match="maru_async_load=False"):
        scheduler.take_worker_namespace(handshake(async_load=False))
    with pytest.raises(ValueError, match="maru_async_store=True"):
        scheduler.take_worker_namespace(handshake(async_store=True))


def test_a_worker_sends_its_transfer_modes():
    worker = make_worker(BLOCK, CHUNK, node_extra(maru_async_load=True))
    meta = worker.handshake_metadata()
    assert (meta.async_load, meta.async_store) == (True, False)


# =============================================================================
# The scheduler connects at startup; no lookup waits
# =============================================================================


def test_the_first_scheduler_lookup_does_not_wait_for_an_absent_node():
    sockdir = short_tmpdir()
    scheduler = make_scheduler(BLOCK, CHUNK, node_extra(f"{sockdir}/node.sock"))
    try:
        for _ in range(3):
            t0 = time.monotonic()
            assert scheduler._count_matched_chunks(list(range(PROMPT))) == 0
            assert time.monotonic() - t0 < 0.5
        assert scheduler._node_handler is not None  # made once, connecting
    finally:
        scheduler.shutdown()
        remove_tree(sockdir)


def test_the_first_scheduler_lookup_does_not_wait_for_maruserver(fake, node):
    fake.pause(hold=False)
    scheduler = make_scheduler(BLOCK, CHUNK, node_extra(node.config.socket))
    try:
        t0 = time.monotonic()
        assert scheduler._count_matched_chunks(list(range(PROMPT))) == 0
        assert time.monotonic() - t0 < 0.5
    finally:
        fake.resume()
        scheduler.shutdown()


def test_the_scheduler_connects_when_vllm_starts(fake, node):
    config = node_vllm_config()
    # Both modes set: nothing else asks the node at startup
    config.kv_transfer_config.kv_connector_extra_config = node_extra(
        node.config.socket, maru_async_load=True, maru_async_store=True
    )
    connector = MaruKVConnector(config, KVConnectorRole.SCHEDULER)
    try:
        assert connector._scheduler._handler is not None
        assert connector._scheduler._handler.connected
    finally:
        connector.shutdown()


def test_a_namespace_from_the_workers_reopens_the_scheduler_at_startup(fake, node):
    scheduler = make_scheduler(BLOCK, CHUNK, node_extra(node.config.socket))
    scheduler.connect_node()
    old = scheduler._handler
    try:
        assert old is not None
        scheduler.take_worker_namespace({0: MaruHandshakeMetadata("other")})
        new = scheduler._handler
        assert new is not None and new is not old and not old.connected
        assert new._cpu.identity.namespace == "other"
    finally:
        scheduler.shutdown()


# =============================================================================
# Shutdown with layerwise stores in flight or unfinished
# =============================================================================


def test_shutdown_waits_for_the_markers_of_chunks_in_flight(fake, nodes_small):
    from maru_common.protocol import MessageType

    node = nodes_small(credit_pages=16, total_pages=32, rpc_timeout_s=3.0)
    writer = layerwise_writer(node)
    client = writer._handler._cpu
    wait_until(lambda: client.credits.unused_bytes >= 8 * PAGE)
    fake.delay(MessageType.COMMIT, 0.3)
    save_step(writer, rank_kv(1, 2), [store_meta("r1", 0)])
    writer.shutdown()
    stored = stored_keys(fake)
    for key in chunk_keys_of(0):
        assert {f"{key}_L{i}" for i in range(NUM_LAYERS)} | {f"{key}_DONE"} <= stored
    # No page was left being written: the session ended with WORKER_CLOSE
    assert fake.worker_session_of(client.identity.incarnation) is None


def test_shutdown_gives_back_the_pages_of_unfinished_chunks(fake, node, workers):
    writer = workers(maru_use_layerwise=True)
    wait_credit(writer)
    incarnation = writer._handler._cpu.identity.incarnation
    metadata = MaruConnectorMetadata(requests=[store_meta("r1", 0)])
    attn = make_flash_attn_metadata()
    for name, layer in list(zip(LAYER_NAMES, rank_kv(1, 2), strict=True))[:2]:
        writer.save_kv_layer(name, layer, attn, metadata)
    assert writer._pending_layer_objects
    writer.shutdown()
    assert not writer._pending_layer_objects
    assert fake.keys() == set()
    assert fake.worker_session_of(incarnation) is None  # nothing left allocated


# =============================================================================
# Loads on CUDA: the external range only
# =============================================================================


needs_cuda = pytest.mark.skipif(not torch.cuda.is_available(), reason="needs CUDA")


def cuda_reader(workers, layerwise: bool):
    reader = workers(**({"maru_use_layerwise": True} if layerwise else {}))
    dst = [layer.cuda() for layer in sentinel_kv()]
    reader._kv_caches = dict(zip(LAYER_NAMES, dst, strict=True))
    return reader, dst


def finished(reader, req_id: str = "r1") -> None:
    seen: set[str] = set()

    def done() -> bool:
        seen.update(reader.get_finished_loading() or ())
        return req_id in seen

    wait_until(done, what=f"{req_id} loaded")
    torch.cuda.synchronize()


def assert_external_on_cuda(reader, dst, kv) -> None:
    torch.cuda.synchronize()
    assert reader.take_failed_load_blocks() == set()
    assert_only_external_blocks([layer.cpu() for layer in dst], kv)
    reader._release_completed_load_refs()
    wait_until(lambda: reads_held(reader) == 0, what="reads released")


@needs_cuda
@pytest.mark.parametrize("layerwise", [False, True])
@pytest.mark.parametrize("parked", [False, True])
def test_a_cuda_load_writes_only_the_external_range(
    fake, node, workers, layerwise, parked
):
    writer = workers(**({"maru_use_layerwise": True} if layerwise else {}))
    kv = rank_kv(1, 2)
    store(writer, kv)
    reader, dst = cuda_reader(workers, layerwise)
    reader.start_load_kv(
        forward_of(dst), MaruConnectorMetadata(requests=[external_meta(parked)])
    )
    if parked:
        finished(reader)
    assert_external_on_cuda(reader, dst, kv)


@needs_cuda
def test_a_parked_cuda_group_load_is_reported_once_its_copies_end(fake, node):
    source = _shared()
    writer = node_group_worker(node, source)
    target = torch.zeros_like(source).cuda()
    reader = node_group_worker(node, target)
    reader._leased_load_done = MagicMock(wraps=reader._leased_load_done)
    try:
        wait_credit(writer)
        tokens = list(range(40))
        writer.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32))
        drain(writer)
        meta = _load_meta(tokens, B_BLOCKS, 2, deferred_load=True)
        reader.start_load_kv(_FORWARD, meta)
        finished(reader, "b")
        # Reported by _settle_parked_loads, not by the loader thread
        reader._leased_load_done.assert_called_once()
        assert reader.take_failed_recv_req_ids({"b"}) == set()
        assert reader.take_failed_load_blocks() == set()
        _check_loaded(writer, reader, source, target, in_place=False, version="0.30")
        wait_until(lambda: reads_held(reader) == 0, what="reads released")
    finally:
        writer.shutdown()
        reader.shutdown()


@needs_cuda
@pytest.mark.parametrize("layerwise", [False, True])
def test_overlap_copies_queued_while_parked_write_only_the_external_range(
    fake, node, workers, layerwise
):
    writer = workers(**({"maru_use_layerwise": True} if layerwise else {}))
    kv = rank_kv(1, 2)
    store(writer, kv)
    reader, dst = cuda_reader(workers, layerwise)
    meta = external_meta(True, layerwise_load=True)
    reader.start_load_kv(forward_of(dst), MaruConnectorMetadata(requests=[meta]))
    finished(reader)
    assert reader._deferred_layerwise_events.get("r1")  # queued off-thread
    assert_external_on_cuda(reader, dst, kv)


@needs_cuda
@pytest.mark.parametrize("layerwise", [False, True])
def test_a_cuda_overlap_activation_writes_only_the_external_range(
    fake, node, workers, layerwise
):
    writer = workers(**({"maru_use_layerwise": True} if layerwise else {}))
    kv = rank_kv(1, 2)
    store(writer, kv)
    reader, dst = cuda_reader(workers, layerwise)
    meta = external_meta(True, layerwise_load=True)
    keys, slots = reader._node_load_plan(meta, reader._slice_chunk_keys(meta)[:2])
    layers = [
        (name, layer, i) for i, (name, layer) in enumerate(reader._kv_caches.items())
    ]
    infos = reader._batch_retrieve_all(
        connector_module._load_keys(keys, len(keys), layers, layerwise)
    )
    payload = (
        {i: infos[i * len(keys) : (i + 1) * len(keys)] for i in range(NUM_LAYERS)}
        if layerwise
        else infos
    )
    reader._deferred_layerwise_loads["r1"] = (meta, len(keys), slots, payload)
    reader._schedule_deferred_layerwise_loads(
        layers, {"r1"}, make_flash_attn_metadata()
    )
    assert set(reader._layer_load_events) == set(LAYER_NAMES)
    assert_external_on_cuda(reader, dst, kv)


@needs_cuda
def test_overlap_copies_that_cannot_be_queued_fail_the_parked_load(
    fake, node, workers, monkeypatch
):
    writer = workers()
    store(writer, rank_kv(1, 2))
    reader, dst = cuda_reader(workers, False)
    monkeypatch.setattr(reader, "_issue_layerwise_copies_offthread", lambda *a: None)
    meta = external_meta(True, layerwise_load=True)
    reader.start_load_kv(forward_of(dst), MaruConnectorMetadata(requests=[meta]))
    finished(reader)
    # Failed while parked, never kept for the step that computes it
    assert reader.take_failed_load_blocks() == {1, 2}
    assert "r1" not in reader._deferred_layerwise_loads
    assert "r1" not in reader._unsound_kv_req_ids
    wait_until(lambda: reads_held(reader) == 0, what="reads released")


# =============================================================================
# Credit for one step: what a rank says it may store
# =============================================================================


def test_the_step_chunk_bound_holds_for_any_schedule():
    """Random steps: each request's completed chunks per _step_chunks."""
    import random

    from maru_vllm.connector import _step_chunk_bound, _step_chunks

    rng = random.Random(0)
    for _ in range(2000):
        ct = rng.choice([1, 2, 4, 8, 16])
        tokens, seqs = rng.randint(1, 64), rng.randint(1, 12)
        budget, completed = tokens, 0
        for _ in range(seqs):
            if budget == 0:
                break
            scheduled = rng.randint(1, budget)
            budget -= scheduled
            computed = rng.randint(0, 40)
            meta = MaruReqMeta(
                "r",
                [0] * (computed + scheduled),
                [],
                True,
                num_scheduled_tokens=scheduled,
                num_computed_tokens=computed,
            )
            completed += len(_step_chunks(meta, ct, (computed + scheduled) // ct))
        assert completed <= _step_chunk_bound(tokens, seqs, ct)
    assert _step_chunk_bound(8192, 256, 256) == 287
    assert _step_chunk_bound(8192, 1, 256) == 32
    assert _step_chunk_bound(0, 256, 256) == 0  # unknown


def step_worker(shard=WHOLE_MODEL, **extra):
    worker = make_worker(
        BLOCK,
        CHUNK,
        node_extra(_maru_step_tokens=64, _maru_step_seqs=4, **extra),
        head_size=HEAD_SIZE,
        kv_shard=shard,
    )
    worker._num_layers = NUM_LAYERS
    return worker


def test_a_rank_says_how_many_objects_one_step_may_store():
    chunks = (64 + 4 * (CHUNK - 1)) // CHUNK  # 11
    assert step_worker()._step_objects() == chunks
    # A layer each, and the _DONE marker
    assert step_worker(maru_use_layerwise=True)._step_objects() == chunks * (
        NUM_LAYERS + 1
    )
    # Every rank stores its own slice; a rank holding a copy stores nothing
    writer, replica = (tp_kv_shard(rank, 2, 1) for rank in (0, 1))
    assert step_worker(writer)._step_objects() == chunks
    assert step_worker(replica)._step_objects() == 0
    unknown = make_worker(BLOCK, CHUNK, node_extra())
    unknown._num_layers = NUM_LAYERS
    assert unknown._step_objects() == 0


@pytest.mark.parametrize("config", [gpt_oss_config, tiny_config])
def test_several_groups_store_an_object_per_attention_group_and_a_state(config):
    groups = describe_kv_groups(config())
    worker = make_worker(
        16,
        16,
        node_extra(_maru_step_tokens=64, _maru_step_seqs=4),
        kv_groups=groups,
    )
    chunks = (64 + 4 * 15) // 16
    attention = sum(g.kind != connector_module.GROUP_MAMBA for g in groups)
    mamba = len(groups) - attention
    assert worker._step_objects() == chunks * attention + 4 * mamba


def test_the_worker_sends_its_step_with_attach(fake, node):
    worker = step_worker()
    worker._extra_config["maru_node"] = node.config.socket
    worker._page_size_bytes = 2 * CHUNK * 2 * HEAD_SIZE * 4 * NUM_LAYERS
    try:
        worker._take_node_handler(wait=True)
        assert worker._handler._cpu.config.step_objects == worker._step_objects() > 0
    finally:
        worker.shutdown()


def test_vllm_scheduler_limits_reach_the_worker(monkeypatch):
    advise(monkeypatch, TransferDefaults(False, False))
    config = node_vllm_config()
    config.scheduler_config.max_num_batched_tokens = 8192
    config.scheduler_config.max_num_seqs = 256
    worker = MaruKVConnector(config, KVConnectorRole.WORKER)._worker
    extra = worker._extra_config
    assert (extra["_maru_step_tokens"], extra["_maru_step_seqs"]) == (8192, 256)


def test_one_step_of_32_chunks_is_stored_on_the_credit_it_asks_for(fake, monkeypatch):
    """The credit at hand (16 pages, as at 32 MiB pages) holds half the step;
    what the step asks for as it starts holds all of it before the stores."""
    import maru_storage_service.config as node_config_module
    from maru_storage_service.config import parse_node_config

    monkeypatch.setattr(node_config_module, "CREDIT_MAX_MIN_BYTES", 0)
    requests = [store_meta(f"r{i}", 1000 * i) for i in range(16)]  # 32 chunks
    sockdir = short_tmpdir()
    config = parse_node_config(
        {
            "node_id": "gpu-step",
            "socket": f"{sockdir}/node.sock",
            "server": fake.url,
            "page_size": PAGE,
            "tiers": [{"store": "dram", "size": 256 * PAGE, "hugepage": False}],
            "budgets": {
                "credit_total_max_bytes": 64 * PAGE,
                "commit_pending_max_bytes": 64 * PAGE,
                "transfer_reserve_bytes": PAGE,
            },
        }
    )
    service = start_node(config)
    writer = make_worker(BLOCK, CHUNK, node_extra(config.socket), head_size=HEAD_SIZE)
    writer._num_layers = NUM_LAYERS
    writer._page_size_bytes = 2 * CHUNK * 2 * HEAD_SIZE * 4 * NUM_LAYERS
    try:
        writer._take_node_handler(wait=True)
        client = writer._handler._cpu
        wait_until(lambda: client.credits.unused_bytes == 16 * PAGE, what="credit")
        writer.want_step_credit(MaruConnectorMetadata(requests=requests))
        wait_until(
            lambda: client.credits.unused_bytes == 32 * PAGE, what="the step's credit"
        )
        save_step(writer, rank_kv(1, 2), requests)
        drain(writer)
        assert len(fake.keys()) == 32
        assert client.stats["store_skipped_no_credit"] == 0
    finally:
        writer.shutdown()
        service.stop()
        remove_tree(sockdir)


# =============================================================================
# Write-behind waits for credit off the engine thread
# =============================================================================


class FakeCredit:
    """Credit of a write-behind worker's node: the engine thread finds some
    while ``at_hand`` is set; a wait for it gets some once ``comes`` is."""

    def __init__(self):
        self.at_hand = threading.Event()
        self.comes = threading.Event()
        self.stopped = threading.Event()  # stop_credit_waits()
        self.pages = []

    def alloc(self, nbytes, route_key=None, wait_s=0.0, retry_later=False):
        deadline = time.monotonic() + wait_s
        while not (self.at_hand.is_set() or self.comes.is_set()):
            if self.stopped.is_set() or time.monotonic() >= deadline:
                raise CreditUnavailableError("no credit at hand")
            time.sleep(0.005)
        page = SimpleNamespace(buf=memoryview(bytearray(nbytes)))
        self.pages.append(page)
        return page


def wait_worker(monkeypatch, blocks: int = 6):
    """A write-behind worker on a CPU stand-in for CUDA, with FakeCredit."""
    from tests.unit.test_vllm_remote_cxl_transfers import _cpu_cuda

    _cpu_cuda(monkeypatch)
    worker = make_worker(
        4, 8, node_extra(maru_async_store=True), num_kv_heads=1, head_size=1
    )
    cache = torch.arange(blocks * 2 * 4, dtype=torch.float32).reshape(
        blocks, 2, 4, 1, 1
    )
    worker._kv_caches = {"model.layers.0.self_attn": cache}
    worker._kv_layout = worker._resolve_kv_layout(worker._kv_caches)
    worker._num_layers = 1
    credit = FakeCredit()
    published = []

    def store_async(keys, handles):
        published.extend(keys)
        future = Future()
        future.set_result([True] * len(keys))
        return future

    worker._handler = MagicMock()
    worker._handler.has_local.return_value = False
    worker._handler.alloc.side_effect = credit.alloc
    worker._handler.store_async.side_effect = store_async
    worker._handler.stop_credit_waits.side_effect = credit.stopped.set

    def fill(dst, *args, **kwargs):
        dst.fill_(7.0)

    ops = SimpleNamespace(
        multi_layer_kv_transfer=fill, TransferDirection=SimpleNamespace(D2H="D2H")
    )
    kernel = (ops, torch.zeros(1, dtype=torch.int64), 16, 4, 1, "FMT")
    return worker, kernel, credit, published


def behind_meta(req_id: str, offset: int = 0, tokens: int = 16, computed: int = 0):
    return MaruConnectorMetadata(
        requests=[
            MaruReqMeta(
                req_id,
                list(range(offset, offset + tokens)),
                list(range(tokens // 4)),
                True,
                num_scheduled_tokens=tokens - computed,
                num_computed_tokens=computed,
            )
        ]
    )


def drain_behind(worker) -> None:
    """Wait for the write-behind jobs, then for the credit waits they began."""
    for name in ("_store_executor", "_credit_executor"):
        executor = getattr(worker, name)
        if executor is not None:
            executor.submit(lambda: None).result(10)


def test_write_behind_waits_for_credit_off_the_engine_thread(monkeypatch):
    worker, kernel, credit, published = wait_worker(monkeypatch)
    t0 = time.monotonic()
    worker._store_packed_slabs_write_behind(kernel, behind_meta("r1"))
    assert time.monotonic() - t0 < 0.1  # the engine thread never waits
    assert worker._deferred_bytes > 0
    threading.Timer(0.2, credit.comes.set).start()
    drain_behind(worker)
    keys = _chunk_keys(list(range(16)), 8)
    assert published == keys  # stored once credit came, none skipped
    for page in credit.pages:
        assert set(torch.frombuffer(page.buf, dtype=torch.float32).tolist()) == {7.0}
    assert not any(worker._store_pending(k) for k in keys)
    assert not worker._credit_drought and worker._deferred_bytes == 0
    worker.shutdown()


def test_a_write_behind_wait_for_credit_is_bounded(monkeypatch):
    worker, kernel, _, published = wait_worker(monkeypatch)
    t0 = time.monotonic()
    worker._store_packed_slabs_write_behind(kernel, behind_meta("r1"))
    drain_behind(worker)
    assert time.monotonic() - t0 < connector_module._STORE_CREDIT_WAIT_S + 0.5
    keys = _chunk_keys(list(range(16)), 8)
    assert published == [] and not any(worker._store_pending(k) for k in keys)
    assert worker._credit_drought and worker._deferred_bytes == 0
    # Until credit comes again chunks without it are skipped at once
    worker._store_packed_slabs_write_behind(kernel, behind_meta("r2", 100))
    assert worker._deferred_bytes == 0
    assert not any(worker._store_pending(k) for k in _chunk_keys(range(100, 116), 8))
    worker.shutdown()


def test_later_batches_do_not_wait_behind_a_credit_wait(monkeypatch):
    worker, kernel, credit, published = wait_worker(monkeypatch)
    worker._store_packed_slabs_write_behind(kernel, behind_meta("r1"))  # waits
    credit.at_hand.set()
    t0 = time.monotonic()
    worker._store_packed_slabs_write_behind(kernel, behind_meta("r2", 100))
    worker._store_executor.submit(lambda: None).result(10)
    assert worker.get_finished_saving({"r1", "r2"}) == {"r1", "r2"}
    assert set(_chunk_keys(list(range(100, 116)), 8)) <= set(published)
    assert time.monotonic() - t0 < 0.5  # not behind r1's wait for credit
    worker.shutdown()


def test_shutdown_is_not_held_by_credit_waits(monkeypatch):
    worker, kernel, _, _ = wait_worker(monkeypatch)
    for i in range(5):
        worker._store_packed_slabs_write_behind(kernel, behind_meta(f"r{i}", 100 * i))
    worker._store_executor.submit(lambda: None).result(10)
    time.sleep(0.1)  # the first wait is under way
    t0 = time.monotonic()
    worker.shutdown()
    # Neither the wait under way nor the queued ones hold it
    assert time.monotonic() - t0 < 0.5
    assert worker._deferred_bytes == 0


@pytest.mark.parametrize(
    "error",
    [
        StorageUnavailableError("closing"),
        ValueError("larger than an object"),
        RuntimeError("bug"),
    ],
)
def test_a_reservation_is_given_back_whatever_alloc_raises(monkeypatch, error):
    worker, kernel, _, published = wait_worker(monkeypatch)
    worker._handler.alloc.side_effect = error
    worker._store_packed_slabs_write_behind(kernel, behind_meta("r1"))
    assert worker._deferred_bytes == 0
    assert published == []
    assert not any(worker._store_pending(k) for k in _chunk_keys(range(16), 8))
    worker.shutdown()


def test_chunks_waiting_for_credit_stay_if_their_copies_cannot_be_tracked(
    monkeypatch,
):
    """The batch's event cannot be recorded: the gathers into the device
    slabs may still run, so the slabs, their room and the requests' blocks
    are kept and the worker stops."""
    worker, kernel, credit, _ = wait_worker(monkeypatch)

    class Broken:
        def record(self, stream=None):
            raise cuda_error()

    monkeypatch.setattr(torch.cuda, "Event", Broken)
    worker._store_packed_slabs_write_behind(kernel, behind_meta("r1"))
    assert worker._deferred_bytes > 0
    assert "r1" in worker._request_pending_store_keys
    slabs = [obj for obj in kept_by(worker) if isinstance(obj, torch.Tensor)]
    assert len(slabs) >= 2  # each chunk's device slab
    assert stopped(worker, UNCONFIRMED)
    worker.shutdown()


def test_chunks_waiting_for_credit_are_bounded_in_all(monkeypatch):
    worker, kernel, credit, _ = wait_worker(monkeypatch)
    slab = 2 * 1 * 8 * 1 * 4  # 2 planes x 1 layer x 8 tokens x 1 x float32
    monkeypatch.setattr(connector_module, "_DEFERRED_STORE_MAX_BYTES", 3 * slab)
    seen = []
    for i in range(4):  # 2 chunks each; the first wait has not timed out yet
        worker._store_packed_slabs_write_behind(kernel, behind_meta(f"r{i}", 100 * i))
        seen.append(worker._deferred_bytes)
    assert max(seen) <= 3 * slab
    credit.comes.set()
    drain_behind(worker)
    assert worker._deferred_bytes == 0
    worker.shutdown()


def test_a_write_behind_step_completing_a_long_prompt_fits_its_credit(monkeypatch):
    """A chunked prefill of 8 chunks completes in a step of 8 tokens."""
    worker, kernel, credit, published = wait_worker(monkeypatch, blocks=16)
    worker._extra_config.update(
        _maru_step_tokens=8, _maru_step_seqs=1, _maru_max_model_len=64
    )
    assert connector_module._step_chunk_bound(8, 1, 8) == 1
    assert worker._step_objects() == 8
    credit.at_hand.set()
    worker._store_packed_slabs_write_behind(
        kernel, behind_meta("r1", tokens=64, computed=56)
    )
    drain_behind(worker)
    assert len(published) == 8 <= worker._step_objects()
    worker.shutdown()


def test_write_behind_counts_one_whole_prompt_a_step():
    worker = step_worker(maru_async_store=True, _maru_max_model_len=32 * CHUNK)
    chunks = (64 + 4 * (CHUNK - 1)) // CHUNK  # 11
    assert worker._step_objects() == max(chunks, 32)
    layerwise = step_worker(
        maru_async_store=True, maru_use_layerwise=True, _maru_max_model_len=32 * CHUNK
    )
    assert layerwise._step_objects() == chunks * (NUM_LAYERS + 1)  # never behind


@needs_cuda
def test_chunks_wait_for_credit_on_the_device(monkeypatch):
    """The device-slab path on CUDA: gathered on the store stream, copied to
    its page on the credit-wait thread."""
    worker = make_worker(
        4, 8, node_extra(maru_async_store=True), num_kv_heads=1, head_size=1
    )
    cache = torch.arange(6 * 2 * 4, dtype=torch.float32).reshape(6, 2, 4, 1, 1)
    worker._kv_caches = {"model.layers.0.self_attn": cache.cuda()}
    worker._kv_layout = worker._resolve_kv_layout(worker._kv_caches)
    worker._num_layers = 1
    credit = FakeCredit()
    published = []

    def store_async(keys, handles):
        published.extend(keys)
        future = Future()
        future.set_result([True] * len(keys))
        return future

    worker._handler = MagicMock()
    worker._handler.has_local.return_value = False
    worker._handler.alloc.side_effect = credit.alloc
    worker._handler.store_async.side_effect = store_async

    def gather(dst, ptrs, slots, dev, *args, **kwargs):
        dst.copy_(slots.float().view(1, 1, -1, 1).expand(dst.shape))

    ops = SimpleNamespace(
        multi_layer_kv_transfer=gather, TransferDirection=SimpleNamespace(D2H="D2H")
    )
    kernel = (ops, torch.zeros(1, dtype=torch.int64, device="cuda"), 24, 4, 1, "FMT")
    worker._store_packed_slabs_write_behind(kernel, behind_meta("r1"))
    assert worker._deferred_bytes > 0
    credit.comes.set()
    drain_behind(worker)
    assert published == _chunk_keys(list(range(16)), 8)
    first = torch.frombuffer(credit.pages[0].buf, dtype=torch.float32).view(2, 1, 8)
    assert first[0, 0].tolist() == list(range(8))  # the chunk's own slots
    assert worker._deferred_bytes == 0
    worker.shutdown()


def test_write_behind_adds_no_wait_on_the_engine_thread(monkeypatch):
    """Only the completion thread waits for a batch's copies."""
    worker, kernel, credit, published = wait_worker(monkeypatch)
    credit.at_hand.set()
    waits = []

    class Event:
        def record(self, stream=None):
            pass

        def synchronize(self):
            waits.append(threading.get_ident())

    monkeypatch.setattr(torch.cuda, "Event", Event)
    stream = torch.cuda.current_stream()
    stream.synchronize = lambda: waits.append(threading.get_ident())
    worker._store_packed_slabs_write_behind(kernel, behind_meta("r1"))
    drain_behind(worker)
    assert len(waits) == 1 and threading.get_ident() not in waits
    assert len(published) == 2
    assert worker.get_finished_saving({"r1"}) == {"r1"}
    assert len(waits) == 1
    worker.shutdown()


@pytest.mark.parametrize("ended", [True, False])
def test_a_failed_chunk_frees_its_page_and_an_unconfirmed_batch_keeps_the_rest(
    monkeypatch, ended
):
    """Its gather failed before its D2H was queued: its page goes back at
    once. If the batch's copies cannot be waited for, the others' pages and
    the staging are kept and the worker stops."""
    worker, kernel, credit, published = wait_worker(monkeypatch)
    credit.at_hand.set()
    gathers = []

    def gather(dst, *args, **kwargs):
        gathers.append(dst)
        if len(gathers) == 1:
            raise RuntimeError("copy failed")

    kernel[0].multi_layer_kv_transfer = gather
    if not ended:

        class Unknown:
            def record(self, stream=None):
                pass

            def synchronize(self):
                raise cuda_error()

        monkeypatch.setattr(torch.cuda, "Event", Unknown)
    worker._store_packed_slabs_write_behind(kernel, behind_meta("r1"))
    drain_behind(worker)
    failed, stored = credit.pages
    freed = [c.args[0] for c in worker._handler.free.call_args_list]
    assert freed == [failed]
    if ended:
        assert len(published) == 1
        assert worker.get_finished_saving({"r1"}) == {"r1"}
    else:
        assert published == []
        assert holds(worker, stored) and holds(worker, worker._store_staging)
        assert "r1" in worker._request_pending_store_keys
        assert stopped(worker, UNCONFIRMED)
    worker.shutdown()


@pytest.mark.parametrize("queued", [True, False])
def test_a_failed_credit_copy_keeps_its_page_slab_and_room_once_queued(
    monkeypatch, queued
):
    """The first gather ended, so the request's blocks went back. A copy from
    the device slab to the page that raised a CUDA error may still write it:
    the page, the slab, their room and the chunks behind are kept and the
    worker stops. A copy that fails otherwise gives both back."""
    worker = write_behind_worker()
    slab, behind = torch.zeros(4), torch.zeros(4)
    worker._deferred_bytes = 32
    if queued:
        handle = SimpleNamespace(buf=memoryview(bytearray(16)))

        def failed(*args, **kwargs):
            raise cuda_error()

        monkeypatch.setattr(torch.Tensor, "copy_", failed)
    else:
        handle = SimpleNamespace(buf=memoryview(bytearray(8)))  # too short
    worker._handler.alloc.return_value = handle
    waiting = [("k", slab, {}), ("k2", behind, {})]
    worker._store_after_credit(worker._handler, waiting)
    if queued:
        worker._handler.free.assert_not_called()
        worker._handler.store_async.assert_not_called()
        assert worker._deferred_bytes == 32
        assert holds(worker, handle) and holds(worker, slab)
        assert waiting == [("k2", behind, {})] and holds(worker, behind)
        assert stopped(worker, UNCONFIRMED)
    else:
        assert worker._handler.free.call_args_list == [mock.call(handle)] * 2
        assert worker._deferred_bytes == 0 and worker._stopped is None


# =============================================================================
# Old nodes and shared credit
# =============================================================================


def test_a_worker_stores_on_the_node_budgets_whatever_its_step(fake, node):
    """The node sizes no credit from ATTACH: the worker gets the node's
    budgets, asks for each step's stores, and stores on them."""
    writer = step_worker()
    writer._extra_config["maru_node"] = node.config.socket
    writer._page_size_bytes = 2 * CHUNK * 2 * HEAD_SIZE * 4 * NUM_LAYERS
    try:
        writer._take_node_handler(wait=True)
        client = writer._handler._cpu
        assert client.config.step_objects > 0
        assert client.attach_info.budgets == node.config.budgets
        wait_until(lambda: client.credits.unused_bytes >= 2 * PAGE)
        store(writer, rank_kv(1, 2))
        assert len(fake.keys()) == 2
    finally:
        writer.shutdown()


def test_an_older_node_decodes_an_attach_with_step_objects():
    import dataclasses as dc

    from maru_common.node_protocol import NODE_MESSAGE_CLASSES, AttachRequest
    from maru_common.node_protocol import NodeMessageType as Code
    from maru_common.serializer import Serializer

    old_fields = [
        (f.name, f.type, f)
        for f in dc.fields(AttachRequest)
        if f.name != "step_objects"
    ]
    old_attach = dc.make_dataclass("OldAttach", old_fields)
    table = dict(NODE_MESSAGE_CLASSES)
    table[Code.ATTACH] = (old_attach, table[Code.ATTACH][1])
    new = AttachRequest(
        engine_id="e",
        dp_index=0,
        tp_rank=0,
        pid=1,
        pid_start=2,
        incarnation="i",
        namespace="ns",
        groups=[],
        protocol="p",
        step_objects=7,
    )
    sender, old_node = Serializer(NODE_MESSAGE_CLASSES), Serializer(table)
    _, decoded = old_node.decode_request(sender.encode(Code.ATTACH, new))
    assert decoded.incarnation == "i" and not hasattr(decoded, "step_objects")


# =============================================================================
# Engines a node takes; handshakes and data-parallel replicas
# =============================================================================


def test_a_node_takes_mla_and_quantized_kv_engines():
    """Workers copy to a node pool as the cxl backend copies to CXL."""
    config = node_vllm_config()
    config.model_config.use_mla = True
    config.cache_config.cache_dtype = "fp8"
    extra = config.kv_transfer_config.kv_connector_extra_config
    out = _node_engine_config(extra, config)
    assert (
        out["maru_cache_namespace"]
        != _node_engine_config(extra, node_vllm_config())["maru_cache_namespace"]
    )


def test_ranks_storing_in_different_namespaces_stop_the_engine(monkeypatch):
    advise(monkeypatch, TransferDefaults(False, False))
    scheduler = MaruKVConnector(node_vllm_config(2, 8), KVConnectorRole.SCHEDULER)
    with pytest.raises(ValueError, match="different cache namespaces"):
        scheduler.set_xfer_handshake_metadata(
            {0: MaruHandshakeMetadata("a"), 1: MaruHandshakeMetadata("b")}
        )


def test_another_connectors_handshake_is_said_and_ignored(monkeypatch, caplog):
    """vLLM's MultiConnector passes on one connector's metadata only."""
    advise(monkeypatch, TransferDefaults(False, False))
    scheduler = MaruKVConnector(node_vllm_config(2, 8), KVConnectorRole.SCHEDULER)
    ours = scheduler._scheduler._extra_config["maru_cache_namespace"]
    with caplog.at_level("WARNING", logger="maru_vllm.connector"):
        scheduler.set_xfer_handshake_metadata({0: object(), 1: object()})
    assert "another connector's handshake metadata" in caplog.text
    assert scheduler._scheduler._extra_config["maru_cache_namespace"] == ours


def test_tp1_sends_no_namespace_and_takes_none(monkeypatch, caplog):
    """At TP=1 the scheduler runs in the model's process and needs none, and
    NixlConnector beside it refuses any handshake metadata but its own."""
    advise(monkeypatch, TransferDefaults(False, False))
    config = node_vllm_config(1, 8)
    worker = MaruKVConnector(config, KVConnectorRole.WORKER)
    assert worker.get_handshake_metadata() is None
    scheduler = MaruKVConnector(config, KVConnectorRole.SCHEDULER)
    ours = scheduler._scheduler._extra_config["maru_cache_namespace"]
    with caplog.at_level("WARNING", logger="maru_vllm.connector"):
        scheduler.set_xfer_handshake_metadata({0: object()})
    assert "handshake" not in caplog.text
    assert scheduler._scheduler._extra_config["maru_cache_namespace"] == ours


@pytest.mark.parametrize("tp_size", [1, 2])
@pytest.mark.parametrize(
    "parallel",
    [
        # A model without MoE layers: vLLM runs each replica as its own
        # engine with data_parallel_size=1 and keeps its index.
        {"data_parallel_size": 1, "data_parallel_rank": 0, "data_parallel_index": 1},
        # A MoE model: one engine of several replicas.
        {"data_parallel_size": 2, "data_parallel_rank": 1, "data_parallel_index": 1},
    ],
)
def test_each_data_parallel_replica_attaches_with_its_index(
    monkeypatch, tp_size, parallel
):
    """Replicas keep the engine id and store in one namespace; ATTACH names
    the replica and the rank."""
    advise(monkeypatch, TransferDefaults(False, False))
    monkeypatch.setattr(connector_module, "_tp_rank", lambda: tp_size - 1)
    config = node_vllm_config(tp_size, 8, **parallel)
    worker = MaruKVConnector(config, KVConnectorRole.WORKER)._worker
    scheduler = MaruKVConnector(config, KVConnectorRole.SCHEDULER)._scheduler
    w, s = worker._extra_config, scheduler._extra_config
    assert w["maru_engine_id"] == s["maru_engine_id"] == "eng"
    assert (w["_maru_dp_index"], w["_maru_tp_rank"]) == (1, tp_size - 1)
    assert s["_maru_dp_index"] == 1
    first = MaruKVConnector(
        node_vllm_config(tp_size, 8), KVConnectorRole.SCHEDULER
    )._scheduler
    assert (
        s["maru_cache_namespace"]
        == first._extra_config["maru_cache_namespace"]
        == w["maru_cache_namespace"]
    )


def test_a_group_load_without_a_free_slot_fails_over_its_whole_range(monkeypatch):
    """Every group's blocks over the tokens the load covers."""
    worker = pinning_worker()
    worker._handler.retrieve_capacity.return_value = 0
    worker._group_caches = [SimpleNamespace(tensors=[torch.zeros(1)])]
    monkeypatch.setattr(
        worker, "_group_load_plan", lambda meta: (1, [(None, 0, None)], ["a"])
    )
    meta = dataclasses.replace(
        group_meta(), group_block_ids=[[1, 2], [3, 4]], load_end_token=8
    )
    worker._start_group_loads(MaruConnectorMetadata(requests=[meta]))
    worker._handler.batch_retrieve.assert_not_called()
    assert worker.take_failed_load_blocks() == {1, 2, 3, 4}


# =============================================================================
# Stores after a node load: what a load found is not stored again
# =============================================================================


def node_store_worker(monkeypatch):
    """A write-behind node worker on a CPU stand-in for CUDA: every chunk
    finds credit, and MaruServer answers its WRITE_DONE at once."""
    from tests.unit.test_vllm_remote_cxl_transfers import _cpu_cuda

    _cpu_cuda(monkeypatch)
    worker = make_worker(
        4, 8, node_extra(maru_async_store=True), num_kv_heads=1, head_size=1
    )
    cache = torch.arange(6 * 2 * 4, dtype=torch.float32).reshape(6, 2, 4, 1, 1)
    worker._kv_caches = {"model.layers.0.self_attn": cache}
    worker._kv_layout = worker._resolve_kv_layout(worker._kv_caches)
    worker._num_layers = 1
    allocs, published = [], []

    def alloc(nbytes, route_key=None, **_):
        allocs.append(route_key)
        return SimpleNamespace(buf=memoryview(bytearray(nbytes)))

    def store_async(keys, handles):
        published.append(list(keys))
        answered = Future()
        answered.set_result([True] * len(keys))
        return answered

    worker._handler = MagicMock()
    worker._handler.has_local.return_value = False
    worker._handler.alloc.side_effect = alloc
    worker._handler.store_async.side_effect = store_async
    ops = SimpleNamespace(
        multi_layer_kv_transfer=MagicMock(),
        TransferDirection=SimpleNamespace(D2H="D2H"),
    )
    kernel = (ops, torch.zeros(1, dtype=torch.int64), 16, 4, 1, "FMT")
    return worker, kernel, allocs, published


def loaded_store_worker(monkeypatch, *, tokens=24, start=0, end=16, missing=None):
    """A node worker that loaded ``[start, end)`` of a prompt of ``tokens``
    (chunk ``missing`` gone), before its stores are checked."""
    from tests.unit.test_vllm_remote_cxl_transfers import _slab, _view

    worker, kernel, allocs, published = node_store_worker(monkeypatch)
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
    keys = connector_module._req_chunk_keys(load, 8, "")
    worker._batch_retrieve_all = lambda requested, route_key=None: [
        None if keys.index(key) == missing else _view(_slab(keys.index(key)))
        for key in requested
    ]
    worker._load_leased_request([("layer", cache, 0)], load, make_flash_attn_metadata())
    return worker, kernel, allocs, published, load, keys


def resumed_store(load, *, request_id=None, computed=None):
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


def stored(published) -> list[str]:
    return [key for batch in published for key in batch]


@pytest.mark.parametrize("tokens, expected", [(16, []), (24, [2])])
def test_a_node_hit_skips_prefix_gather_and_write_behind(monkeypatch, tokens, expected):
    worker, kernel, allocs, published, load, keys = loaded_store_worker(
        monkeypatch, tokens=tokens
    )
    assert not worker.take_failed_load_blocks()
    worker._store_packed_slabs_write_behind(kernel, resumed_store(load))
    worker.shutdown()  # waits for the write-behind thread
    assert len(allocs) == kernel[0].multi_layer_kv_transfer.call_count == len(expected)
    assert stored(published) == [keys[i] for i in expected]


def test_a_partial_node_range_keeps_boundary_chunks_eligible(monkeypatch):
    worker, kernel, allocs, published, load, keys = loaded_store_worker(
        monkeypatch, start=4, end=20
    )
    assert not worker.take_failed_load_blocks()
    worker._store_packed_slabs_write_behind(kernel, resumed_store(load))
    worker.shutdown()
    assert len(allocs) == kernel[0].multi_layer_kv_transfer.call_count == 2
    assert stored(published) == [keys[0], keys[2]]


@pytest.mark.parametrize("missing", [0, 1])
def test_a_failed_node_load_keeps_recomputed_prefix_storable(monkeypatch, missing):
    worker, kernel, allocs, published, load, keys = loaded_store_worker(
        monkeypatch, end=24, missing=missing
    )
    assert worker.take_failed_load_blocks()
    assert not worker._loaded_request_keys[load.req_id][1]
    # The failed request itself stores nothing: its forward ran over blocks
    # the load never filled (#111).
    worker._store_packed_slabs_write_behind(kernel, resumed_store(load, computed=0))
    assert not allocs
    # A later request with the prefix recomputes and stores all of it.
    worker.handle_preemptions(MaruConnectorMetadata(finished_req_ids={load.req_id}))
    worker._store_packed_slabs_write_behind(
        kernel, resumed_store(load, request_id="later", computed=0)
    )
    worker.shutdown()
    assert len(allocs) == kernel[0].multi_layer_kv_transfer.call_count == 3
    assert stored(published) == keys


def test_write_behind_still_coalesces_prefill_without_a_node_load(monkeypatch):
    worker, kernel, allocs, published = node_store_worker(monkeypatch)
    load = MaruReqMeta("compute", list(range(24)), list(range(6)), False)
    first = resumed_store(load, computed=0)
    first.requests[0].num_scheduled_tokens = 8
    worker._store_packed_slabs_write_behind(kernel, first)
    assert not allocs
    worker._store_packed_slabs_write_behind(kernel, resumed_store(load, computed=8))
    worker.shutdown()
    assert len(allocs) == 3
    assert stored(published) == connector_module._req_chunk_keys(load, 8, "")


def test_write_behind_starts_at_the_first_chunk_the_request_computed(monkeypatch):
    """The chunks before it were loaded or are vLLM's prefix cache (#111)."""
    worker, kernel, allocs, published = node_store_worker(monkeypatch)
    load = MaruReqMeta("hit", list(range(24)), list(range(6)), False)
    metadata = resumed_store(load, computed=12)
    metadata.requests[0].store_start_token = 12
    worker._store_packed_slabs_write_behind(kernel, metadata)
    worker.shutdown()
    # Chunk 1 holds token 12: the request computed part of it.
    assert len(allocs) == 2
    assert stored(published) == connector_module._req_chunk_keys(load, 8, "")[1:]


def test_write_behind_skips_the_chunk_a_full_hit_holds_back(monkeypatch):
    """vLLM computes the held-back block again, but Maru has its chunk."""
    worker, kernel, allocs, published = node_store_worker(monkeypatch)
    load = MaruReqMeta("hit", list(range(24)), list(range(6)), False)
    metadata = resumed_store(load, computed=20)
    metadata.requests[0].store_start_token = 24
    worker._store_packed_slabs_write_behind(kernel, metadata)
    worker.shutdown()
    assert not allocs and not published


@pytest.mark.parametrize("request_id", ["another", "loaded"])
def test_loaded_keys_are_not_permanently_skipped_by_later_requests(
    monkeypatch, request_id
):
    worker, kernel, allocs, published, load, keys = loaded_store_worker(
        monkeypatch, tokens=16
    )
    worker._store_packed_slabs_write_behind(kernel, resumed_store(load))
    assert not allocs
    worker.get_finished_saving({load.req_id})
    assert not worker._loaded_request_keys
    # A later request looks again. If its load misses, its recomputed KV
    # must be eligible even with the same request id or the same keys.
    worker._store_packed_slabs_write_behind(
        kernel, resumed_store(load, request_id=request_id, computed=0)
    )
    worker.shutdown()
    assert len(allocs) == 2
    assert stored(published) == keys


@pytest.mark.parametrize(
    "field", ["preempted_req_ids", "finished_req_ids", "recv_only_finished_req_ids"]
)
def test_request_cleanup_discards_its_loaded_store_keys(monkeypatch, field):
    worker, _, _, _, load, _ = loaded_store_worker(monkeypatch, tokens=16)
    assert worker._loaded_request_keys[load.req_id][1]
    worker.handle_preemptions(MaruConnectorMetadata(**{field: {load.req_id}}))
    assert not worker._loaded_request_keys
    worker.shutdown()


def test_aborting_a_queued_loader_prevents_late_store_skip_state(monkeypatch):
    worker, _, _, _, load, _ = loaded_store_worker(monkeypatch, tokens=16)
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
