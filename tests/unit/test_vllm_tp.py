# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tensor parallelism in MaruKVConnector.

vLLM runs one worker connector per TP rank, each over its own slice of the
paged KV cache, and sends every rank the same connector metadata. These
tests stand several ranks side by side on one fake Maru server that keeps
the first registrant of a key and reports a losing register as stored, as
MaruServer and MaruHandler.batch_store do. Before keys named their slice,
that is exactly how every rank but one loaded another rank's KV heads.

CPU tensors only: the packed path takes its per-layer fallback.
"""

import math
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("torch", reason="torch not installed")
pytest.importorskip("vllm", reason="vllm not installed")

import torch
from vllm.distributed.kv_transfer.kv_connector.v1.base import KVConnectorRole

import maru_vllm.connector as connector_module
from maru_common.kv_shard import tp_kv_shard
from maru_vllm.connector import (
    MaruConnectorMetadata,
    MaruKVConnector,
    MaruReqMeta,
    MaruSchedulerConnector,
    MaruWorkerConnector,
    _chunk_keys,
    _req_chunk_keys,
    _validate_cxl_parallelism,
)
from tests.unit.vllm_connector_helpers import (
    FirstWinsServer,
    make_flash_attn_metadata,
    make_scheduler,
    make_worker,
    tp_model_config,
)

BLOCK = 4
CHUNK = 8
NUM_LAYERS = 3
NUM_BLOCKS = 4
PROMPT = 16  # two chunks, filling every slot of the four blocks
HEAD_SIZE = 2
LAYER_NAMES = [f"model.layers.{i}.self_attn" for i in range(NUM_LAYERS)]
# The layout name an engine reports. None stands in for vLLM < 0.29, whose
# "NHD"/"HND" leaves each backend its own shape; under a vLLM 0.29+
# KVCacheLayout name every layer's paged KV is the logical page.
KV_CACHE_LAYOUTS = [None, "LBNHC"]


def rank_kv(
    seed: int, heads: int, kv_cache_layout: str | None = None
) -> list[torch.Tensor]:
    """Paged KV per layer, in the shape vLLM hands over under ``kv_cache_layout``.

    Without a layout name, vLLM < 0.29's flash backends:
    ``(blocks, 2, block, heads, head)``. Under a vLLM 0.29+ layout name: the
    logical page ``(blocks, heads, block, K|V)``. Values depend on ``seed``
    and the layer, so a slice loaded into the wrong rank or layer is caught.
    """
    if kv_cache_layout is None:
        shape = (NUM_BLOCKS, 2, BLOCK, heads, HEAD_SIZE)
    else:
        shape = (NUM_BLOCKS, heads, BLOCK, 2 * HEAD_SIZE)
    return [
        (
            seed * 10_000
            + layer * 1_000
            + torch.arange(math.prod(shape), dtype=torch.float32)
        )
        .reshape(shape)
        .contiguous()
        for layer in range(NUM_LAYERS)
    ]


def rank_worker(
    server: FirstWinsServer,
    shard,
    kv_cache_layout: str | None = None,
    use_mla: bool = False,
    **extra,
) -> MaruWorkerConnector:
    worker = make_worker(
        BLOCK,
        CHUNK,
        extra,
        head_size=HEAD_SIZE,
        kv_cache_layout=kv_cache_layout,
        kv_shard=shard,
        use_mla=use_mla,
    )
    worker._num_layers = NUM_LAYERS
    # register_kv_caches reads this from cache_config; the round trips below
    # hand their caches over through the forward context instead.
    worker._cache_layout = kv_cache_layout
    worker._handler = server.handler()
    return worker


def store_step(
    worker: MaruWorkerConnector, kv: list[torch.Tensor], attn_metadata=None
) -> None:
    """One prefill step: vLLM calls save_kv_layer once per layer."""
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
    if attn_metadata is None:
        attn_metadata = make_flash_attn_metadata()
    for name, layer in zip(LAYER_NAMES, kv, strict=True):
        worker.save_kv_layer(name, layer, attn_metadata, metadata)


def load_step(
    worker: MaruWorkerConnector, heads: int, kv_cache_layout: str | None = None
) -> list[torch.Tensor]:
    """One forward that loads both chunks into fresh paged KV; returns it."""
    dst = [torch.zeros_like(layer) for layer in rank_kv(0, heads, kv_cache_layout)]
    load_into(worker, dst, make_flash_attn_metadata())
    return dst


def load_into(
    worker: MaruWorkerConnector,
    dst: list[torch.Tensor],
    attn_metadata,
    parked: bool = False,
):
    """One forward that loads both chunks into the paged KV ``dst``. A
    ``parked`` load is one vLLM holds in WAITING_FOR_REMOTE_KVS; with CPU
    caches it runs in this call and is reported finished at once."""
    forward = SimpleNamespace(
        no_compile_layers={
            name: SimpleNamespace(kv_cache=layer)
            for name, layer in zip(LAYER_NAMES, dst, strict=True)
        },
        attn_metadata=attn_metadata,
    )
    metadata = MaruConnectorMetadata(
        requests=[
            MaruReqMeta(
                req_id="r1",
                token_ids=list(range(PROMPT)),
                block_ids=list(range(NUM_BLOCKS)),
                is_store=False,
                num_matched_chunks=PROMPT // CHUNK,
                deferred_load=parked,
            )
        ]
    )
    worker.start_load_kv(forward, metadata)


def assert_same_kv(actual: list[torch.Tensor], expected: list[torch.Tensor]) -> None:
    for layer, (got, want) in enumerate(zip(actual, expected, strict=True)):
        assert torch.equal(got, want), f"layer {layer} differs"


# =============================================================================
# Store and load across ranks
# =============================================================================


@pytest.mark.parametrize("kv_cache_layout", KV_CACHE_LAYOUTS)
@pytest.mark.parametrize(("tp_size", "total_heads"), [(2, 4), (4, 4), (4, 8)])
def test_each_rank_loads_its_own_heads(tp_size, total_heads, kv_cache_layout):
    server = FirstWinsServer()
    heads = total_heads // tp_size
    shards = [tp_kv_shard(rank, tp_size, total_heads) for rank in range(tp_size)]
    workers = [rank_worker(server, shard, kv_cache_layout) for shard in shards]
    kvs = [rank_kv(rank + 1, heads, kv_cache_layout) for rank in range(tp_size)]

    for worker, kv in zip(workers, kvs, strict=True):
        store_step(worker, kv)

    base = _chunk_keys(list(range(PROMPT)), CHUNK)
    assert set(server.objects) == {
        key + shard.suffix for key in base for shard in shards
    }
    for worker, kv in zip(workers, kvs, strict=True):
        assert_same_kv(load_step(worker, heads, kv_cache_layout), kv)
        assert worker.take_failed_load_blocks() == set()


def test_replicated_heads_are_stored_once_and_loaded_by_every_replica():
    """TP=4 over 2 KV heads: ranks 0-1 hold head 0, ranks 2-3 hold head 1."""
    server = FirstWinsServer()
    shards = [tp_kv_shard(rank, 4, 2) for rank in range(4)]
    workers = [rank_worker(server, shard) for shard in shards]
    # Replicas compute identical KV; seed by head, not by rank.
    kvs = [rank_kv(shard.head_start + 1, 1) for shard in shards]

    for worker, kv in zip(workers, kvs, strict=True):
        store_step(worker, kv)

    for worker, shard in zip(workers, shards, strict=True):
        if shard.is_writer:
            worker._handler.batch_store.assert_called()
        else:
            worker._handler.alloc.assert_not_called()
            worker._handler.batch_store.assert_not_called()
    base = _chunk_keys(list(range(PROMPT)), CHUNK)
    assert set(server.objects) == {k + s for k in base for s in ("_h0-1", "_h1-2")}
    for worker, kv in zip(workers, kvs, strict=True):
        assert_same_kv(load_step(worker, 1), kv)


LATENT = 6  # one MLA latent row; 576 wide in DeepSeek-V2


def mla_kv(seed: int, logical_page: bool) -> list[torch.Tensor]:
    """MLA's paged latent per layer: ``[blocks, block, latent]``, or the
    logical page ``[blocks, 1, block, latent]`` that vLLM 0.29+ registers."""
    shape = (
        (NUM_BLOCKS, 1, BLOCK, LATENT) if logical_page else (NUM_BLOCKS, BLOCK, LATENT)
    )
    return [
        (
            seed * 10_000
            + layer * 1_000
            + torch.arange(math.prod(shape), dtype=torch.float32)
        ).reshape(shape)
        for layer in range(NUM_LAYERS)
    ]


def mla_metadata():
    from vllm.model_executor.layers.attention.mla_attention import MLACommonMetadata

    meta = MagicMock()
    meta.__class__ = type("MLAMetadata", (MLACommonMetadata,), {})
    return meta


@pytest.mark.parametrize("layerwise", [False, True], ids=["chunkwise", "layerwise"])
@pytest.mark.parametrize("logical_page", [False, True], ids=["legacy", "logical-page"])
def test_mla_latent_is_stored_by_rank_zero_and_loaded_by_every_rank(
    layerwise, logical_page
):
    """Both TP ranks hold the whole MLA latent: rank 0 stores it under the
    bare key, a chunkwise slab holding it as one plane per layer, and both
    ranks load it."""
    server = FirstWinsServer()
    shards = [tp_kv_shard(rank, 2, 1) for rank in range(2)]
    workers = [
        rank_worker(server, shard, use_mla=True, maru_use_layerwise=layerwise)
        for shard in shards
    ]
    kv = mla_kv(7, logical_page)

    for worker in workers:
        store_step(worker, kv, mla_metadata())

    workers[1]._handler.alloc.assert_not_called()
    base = _chunk_keys(list(range(PROMPT)), CHUNK)
    if layerwise:
        expected = {
            f"{key}_{suffix}"
            for key in base
            for suffix in [*(f"L{i}" for i in range(NUM_LAYERS)), "DONE"]
        }
    else:
        expected = set(base)
        slab = NUM_LAYERS * CHUNK * LATENT * 4  # float32
        assert {len(raw) for raw in server.objects.values()} == {slab}
    assert set(server.objects) == expected
    for worker in workers:
        dst = [torch.zeros_like(layer) for layer in kv]
        load_into(worker, dst, mla_metadata())
        assert_same_kv(dst, kv)


def test_layerwise_keys_and_done_markers_carry_the_slice():
    server = FirstWinsServer()
    worker = rank_worker(server, tp_kv_shard(1, 2, 4), maru_use_layerwise=True)
    store_step(worker, rank_kv(1, 2))

    (key, _) = _chunk_keys(list(range(PROMPT)), CHUNK)
    assert f"{key}_h2-4_L0" in server.objects
    assert f"{key}_h2-4_DONE" in server.objects
    assert not any(k.startswith(f"{key}_L") for k in server.objects)


def test_replica_that_skips_stores_still_reports_finished_requests():
    """vLLM frees a finished request's blocks only once every rank reports it."""
    worker = rank_worker(FirstWinsServer(), tp_kv_shard(1, 4, 2), maru_async_store=True)
    store_step(worker, rank_kv(1, 1))
    assert worker.get_finished_saving({"r1"}) == {"r1"}


def test_chunk_keys_follow_the_callers_slice():
    meta = MaruReqMeta(
        req_id="r1", token_ids=list(range(PROMPT)), block_ids=[0, 1], is_store=True
    )
    base = _chunk_keys(meta.token_ids, CHUNK)
    assert _req_chunk_keys(meta, CHUNK, "") == base
    assert _req_chunk_keys(meta, CHUNK, "_h0-2") == [k + "_h0-2" for k in base]
    assert _req_chunk_keys(meta, CHUNK, "_h2-4") == [k + "_h2-4" for k in base]
    assert _req_chunk_keys(meta, CHUNK, "") == base


def test_worker_refuses_a_head_count_its_key_does_not_name(monkeypatch):
    """The keys follow vLLM's head split. A vLLM that put another count on a
    rank would have split the heads some other way, so the worker refuses."""
    monkeypatch.setattr(connector_module, "_tp_rank", lambda: 0)
    config = fake_vllm_config(2, 8)  # names 4 heads per rank
    config.model_config.get_num_kv_heads = lambda pc: 2
    with pytest.raises(ValueError, match="puts 2 KV heads on TP rank 0"):
        MaruKVConnector(config, KVConnectorRole.WORKER)


# =============================================================================
# Scheduler lookup
# =============================================================================


def tp_scheduler(stored: set[str], suffixes, **extra) -> MaruSchedulerConnector:
    scheduler = make_scheduler(BLOCK, CHUNK, extra, shard_suffixes=suffixes)
    scheduler._handler = MagicMock()
    scheduler._handler.batch_exists.side_effect = lambda keys: [
        key in stored for key in keys
    ]
    return scheduler


def test_chunk_counts_only_once_every_slice_is_stored():
    tokens = list(range(3 * CHUNK))
    k0, k1, k2 = _chunk_keys(tokens, CHUNK)
    slices = ("_h0-4", "_h4-8")
    complete = {k + s for k in (k0, k1, k2) for s in slices}

    assert tp_scheduler(complete, slices)._count_matched_chunks(tokens) == 3
    # One rank's slice of the middle chunk is missing: the prefix ends there,
    # even though the last chunk is complete.
    assert (
        tp_scheduler(complete - {k1 + "_h4-8"}, slices)._count_matched_chunks(tokens)
        == 1
    )


def test_one_rpc_checks_every_slice_and_complete_chunks_are_remembered():
    tokens = list(range(2 * CHUNK))
    slices = ("_h0-2", "_h2-4")
    stored = {k + s for k in _chunk_keys(tokens, CHUNK) for s in slices}
    scheduler = tp_scheduler(stored, slices)

    assert scheduler._count_matched_chunks(tokens) == 2
    assert scheduler._count_matched_chunks(tokens) == 2
    scheduler._handler.batch_exists.assert_called_once()
    (keys,) = scheduler._handler.batch_exists.call_args.args
    assert len(keys) == 4


def test_layerwise_lookup_checks_each_slice_done_marker():
    tokens = list(range(CHUNK))
    (key,) = _chunk_keys(tokens, CHUNK)
    slices = ("_h0-1", "_h1-2")
    stored = {f"{key}_h0-1_DONE", f"{key}_h1-2_DONE"}
    scheduler = tp_scheduler(stored, slices, maru_use_layerwise=True)
    assert scheduler._count_matched_chunks(tokens) == 1
    assert (
        tp_scheduler(
            stored - {f"{key}_h1-2_DONE"}, slices, maru_use_layerwise=True
        )._count_matched_chunks(tokens)
        == 0
    )


# =============================================================================
# MaruKVConnector wiring
# =============================================================================


def fake_vllm_config(
    tp_size: int,
    total_heads: int,
    *,
    use_mla: bool = False,
    extra: dict | None = None,
    **parallel,
) -> SimpleNamespace:
    """The parts of VllmConfig the connector reads, without device probing."""
    return SimpleNamespace(
        kv_transfer_config=SimpleNamespace(
            kv_connector_extra_config={"maru_instance_id": "inst1", **(extra or {})},
            kv_load_failure_policy="recompute",
        ),
        cache_config=SimpleNamespace(block_size=BLOCK, num_gpu_blocks=None),
        parallel_config=SimpleNamespace(tensor_parallel_size=tp_size, **parallel),
        model_config=tp_model_config(total_heads, use_mla=use_mla, head_size=HEAD_SIZE),
        speculative_config=None,
        num_lookahead_tokens=0,
    )


def test_scheduler_checks_every_slice_of_the_engine():
    connector = MaruKVConnector(fake_vllm_config(2, 8), KVConnectorRole.SCHEDULER)
    assert connector._scheduler._shard_suffixes == ("_h0-4", "_h4-8")


def test_scheduler_checks_a_replicated_slice_once():
    connector = MaruKVConnector(
        fake_vllm_config(4, 1, use_mla=True), KVConnectorRole.SCHEDULER
    )
    assert connector._scheduler._shard_suffixes == ("",)


def test_worker_takes_mla_from_the_model_config():
    """Not from attention metadata, which a parked load can run without."""
    mla = MaruKVConnector(fake_vllm_config(1, 1, use_mla=True), KVConnectorRole.WORKER)
    assert mla._worker._use_mla and mla._worker._kv_planes == 1
    gqa = MaruKVConnector(fake_vllm_config(1, 8), KVConnectorRole.WORKER)
    assert not gqa._worker._use_mla and gqa._worker._kv_planes == 2


def test_worker_names_its_slice_and_scopes_its_instance_id(monkeypatch):
    monkeypatch.setattr(connector_module, "_tp_rank", lambda: 1)
    connector = MaruKVConnector(fake_vllm_config(2, 8), KVConnectorRole.WORKER)
    worker = connector._worker
    assert worker._kv_shard.suffix == "_h4-8"
    assert worker._extra_config["maru_instance_id"] == "inst1-tp1"


def test_tp1_worker_is_unchanged(monkeypatch):
    def no_rank():
        raise AssertionError("TP=1 must not query the TP group")

    monkeypatch.setattr(connector_module, "_tp_rank", no_rank)
    connector = MaruKVConnector(fake_vllm_config(1, 8), KVConnectorRole.WORKER)
    worker = connector._worker
    assert worker._kv_shard.suffix == ""
    assert worker._extra_config["maru_instance_id"] == "inst1"


def test_attention_free_model_is_refused_under_tp():
    """vLLM reports 0 KV heads for an attention-free model such as Mamba."""
    with pytest.raises(ValueError, match="attention-free"):
        MaruKVConnector(fake_vllm_config(2, 0), KVConnectorRole.SCHEDULER)


@pytest.mark.parametrize(
    ("tp_size", "extra", "parked"),
    [
        (2, {}, True),
        (2, {"maru_async_load": True}, True),
        (2, {"maru_async_load": False}, None),
        (2, {"maru_enable_deferred_loading": False}, None),
        (1, {}, False),
    ],
)
def test_tp_parks_every_load(monkeypatch, tp_size, extra, parked):
    """An inline load runs in the step that computes its request, so a rank
    whose load failed would compute from unfilled KV with the others, which
    would store the result (#135). Above TP=1 loads are parked unless told
    otherwise, and inline loads are refused; TP=1 keeps the inline default."""
    monkeypatch.setattr(connector_module, "_tp_rank", lambda: 1)
    config = fake_vllm_config(tp_size, 8, extra=extra)
    if parked is not None:
        connector = MaruKVConnector(config, KVConnectorRole.SCHEDULER)
        assert connector._scheduler._deferred_loading is parked
        connector = MaruKVConnector(config, KVConnectorRole.WORKER)
        knob = connector_module._get_knob(
            connector._worker._extra_config, "maru_async_load"
        )
        assert bool(knob) is parked
        return
    for role in (KVConnectorRole.SCHEDULER, KVConnectorRole.WORKER):
        with pytest.raises(ValueError, match="maru_async_load=false") as refusal:
            MaruKVConnector(config, role)
    assert "xcena-dev/maru#135" in str(refusal.value)


@pytest.mark.parametrize("failing_rank", [0, 1], ids=["writer", "non-writer"])
def test_a_parked_load_failing_on_one_rank_fails_it_for_every_rank(failing_rank):
    """#135: with an MLA latent, rank 0 stores and rank 1 only loads. Whichever
    rank's parked load fails, it reports the request loaded with its blocks
    failed, and vLLM merges every rank's report before the request computes:
    the request comes back finished only once both ranks report, with the
    failed blocks for vLLM to recompute on every rank. Nothing is stored
    while the request is parked."""
    from vllm.distributed.kv_transfer.kv_connector.utils import KVOutputAggregator
    from vllm.v1.outputs import KVConnectorOutput

    server = FirstWinsServer()
    workers = [
        rank_worker(server, tp_kv_shard(rank, 2, 1), use_mla=True, maru_async_load=True)
        for rank in range(2)
    ]
    kv = mla_kv(7, logical_page=False)
    for worker in workers:
        store_step(worker, kv, mla_metadata())
    stored = dict(server.objects)

    def fail_retrieve(*args, **kwargs):
        raise RuntimeError("injected retrieve failure")

    workers[failing_rank]._batch_retrieve_all = fail_retrieve
    aggregator = KVOutputAggregator(expected_finished_count=2)
    reports = []
    for worker in workers:
        dst = [torch.zeros_like(layer) for layer in kv]
        load_into(worker, dst, mla_metadata(), parked=True)
        reports.append(
            SimpleNamespace(
                kv_connector_output=KVConnectorOutput(
                    finished_recving=worker.get_finished_loading(),
                    invalid_block_ids=worker.take_failed_load_blocks(),
                )
            )
        )
    assert reports[1 - failing_rank].kv_connector_output.invalid_block_ids == set()

    merged = aggregator.aggregate(reports).kv_connector_output
    assert merged.finished_recving == {"r1"}
    assert merged.invalid_block_ids == set(range(NUM_BLOCKS))
    assert server.objects == stored


def test_tp_fails_a_layerwise_overlap_load_whose_copies_it_cannot_queue():
    """At TP=1 the resumed forward issues such a load's copies itself. Under
    TP that would load in the step that computes the request, where a rank
    whose copies failed is the only one that knows (#135), so the load fails
    while the request is parked and vLLM recomputes it on every rank."""
    from maru_common.kv_shard import tp_part_shard
    from tests.unit.vllm_connector_helpers import deferred_req_meta

    worker = make_worker(
        BLOCK,
        CHUNK,
        {"maru_async_load": True, "maru_overlap_load_with_compute": True},
        kv_shard=tp_kv_shard(1, 2, 8),
        state_shard=tp_part_shard(1, 2),
    )
    worker._num_layers = 1
    worker._handler = MagicMock()
    worker._handler.batch_retrieve.return_value = [
        SimpleNamespace(view=memoryview(bytearray(64)), region_id=0, page_index=i)
        for i in range(PROMPT // CHUNK)
    ]
    worker._issue_layerwise_copies_offthread = lambda *args, **kwargs: None
    meta = deferred_req_meta(
        token_ids=list(range(PROMPT)),
        block_ids=list(range(NUM_BLOCKS)),
        num_matched_chunks=PROMPT // CHUNK,
    )
    meta.layerwise_load = True

    worker._deferred_load_job(
        meta, [("l0", torch.zeros(2, 16, 4, 1), 0)], torch.device("cpu")
    )

    assert worker.get_finished_loading() == {"r1"}
    assert worker.take_failed_load_blocks() == set(range(NUM_BLOCKS))
    assert worker._deferred_layerwise_loads == {}


@pytest.mark.parametrize(
    ("tp_size", "parallel", "instance_id"),
    [
        # A replica of a model without MoE layers: vLLM resets its rank to 0
        # and keeps the replica's index.
        (2, {"data_parallel_rank": 0, "data_parallel_index": 1}, "inst1-dp1-tp1"),
        (2, {"data_parallel_rank": 3, "data_parallel_index": 3}, "inst1-dp3-tp1"),
        (2, {"data_parallel_rank": 0, "data_parallel_index": 0}, "inst1-tp1"),
        (1, {"data_parallel_rank": 0, "data_parallel_index": 2}, "inst1-dp2"),
        # A vLLM without data_parallel_index.
        (2, {"data_parallel_rank": 1}, "inst1-dp1-tp1"),
    ],
)
def test_worker_scopes_its_instance_id_to_its_data_parallel_replica(
    monkeypatch, tp_size, parallel, instance_id
):
    """Each data-parallel replica runs its own TP group, so rank 1 of one
    replica must not share an owner id with rank 1 of another."""
    monkeypatch.setattr(connector_module, "_tp_rank", lambda: 1)
    connector = MaruKVConnector(
        fake_vllm_config(tp_size, 8, **parallel), KVConnectorRole.WORKER
    )
    assert connector._worker._extra_config["maru_instance_id"] == instance_id


@pytest.mark.parametrize(
    "parallel",
    [
        {"pipeline_parallel_size": 2},
        {"decode_context_parallel_size": 2},
        {"prefill_context_parallel_size": 2},
    ],
)
def test_pipeline_and_context_parallelism_are_refused(parallel):
    (name,) = parallel
    with pytest.raises(ValueError, match=name):
        MaruKVConnector(fake_vllm_config(2, 8, **parallel), KVConnectorRole.SCHEDULER)
    with pytest.raises(ValueError, match=name):
        _validate_cxl_parallelism(SimpleNamespace(**parallel))
