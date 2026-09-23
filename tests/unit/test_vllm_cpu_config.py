# SPDX-License-Identifier: Apache-2.0
"""CPU opt-in configuration and requester identity at the connector boundary."""

from types import SimpleNamespace

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import torch

from maru_vllm.connector import (
    _cpu_bypass_request,
    _cpu_engine_config,
    _validate_storage_config,
)
from tests.unit.vllm_connector_helpers import make_scheduler, make_worker


def extra():
    return {
        "maru_storage_backend": "cpu",
        "maru_cpu_pool_size": "1M",
        "maru_engine_id": "one",
        "maru_cache_namespace": "model-revision-1",
    }


def mixed_extra(**kwargs):
    return {
        **extra(),
        "maru_storage_backend": "mixed",
        "maru_cxl_pool_size": "2M",
        **kwargs,
    }


@pytest.mark.parametrize("order", [["cpu", "cxl"], ["cxl", "cpu"]])
def test_mixed_accepts_independent_read_and_write_orders(order):
    assert _validate_storage_config(
        mixed_extra(maru_write_order=order, maru_read_order=list(reversed(order)))
    )
    assert make_worker(16, 16, mixed_extra())._packed_load_kernel_ctx([], None) is None


@pytest.mark.parametrize(
    "order", [["cpu"], ["cxl", "cxl"], ["ssd", "cpu"], "cpu,cxl", []]
)
def test_mixed_rejects_ambiguous_or_unsupported_orders(order):
    with pytest.raises(ValueError, match="exactly once"):
        _validate_storage_config(mixed_extra(maru_write_order=order))


def test_mixed_requires_explicit_cxl_capacity():
    with pytest.raises(ValueError, match="maru_cxl_pool_size"):
        _validate_storage_config({**extra(), "maru_storage_backend": "mixed"})


@pytest.mark.parametrize(
    "knob",
    [
        "maru_async_load",
        "maru_async_store",
        "maru_overlap_load_with_compute",
        "maru_use_layerwise",
        "maru_enable_write_behind",
        "maru_enable_deferred_loading",
    ],
)
def test_unsupported_transfer_modes_fail_at_configuration(knob):
    with pytest.raises(ValueError, match="CPU M1"):
        _validate_storage_config({**extra(), knob: True})


@pytest.mark.parametrize(
    "key", ["maru_engine_id", "maru_cache_namespace", "maru_cpu_pool_size"]
)
def test_required_cpu_settings(key):
    config = extra()
    del config[key]
    with pytest.raises(ValueError, match=key):
        _validate_storage_config(config)


def test_cpu_and_cxl_capacity_names_are_not_ambiguous():
    with pytest.raises(ValueError, match="maru_cpu_pool_size"):
        _validate_storage_config({**extra(), "maru_pool_size": "1G"})


def engine_config():
    return SimpleNamespace(
        kv_transfer_config=SimpleNamespace(kv_load_failure_policy="recompute"),
        parallel_config=SimpleNamespace(
            tensor_parallel_size=1, pipeline_parallel_size=1, data_parallel_size=1
        ),
        model_config=SimpleNamespace(
            model="weights",
            revision="commit",
            dtype="float16",
            enforce_eager=True,
            hf_config=SimpleNamespace(to_dict=lambda: {"heads": 4}),
        ),
        cache_config=SimpleNamespace(cache_dtype="auto", block_size=16),
    )


@pytest.mark.parametrize(
    "name", ["tensor_parallel_size", "pipeline_parallel_size", "data_parallel_size"]
)
def test_multiple_workers_are_rejected(name):
    config = engine_config()
    setattr(config.parallel_config, name, 2)
    with pytest.raises(ValueError, match="TP=PP=DP"):
        _cpu_engine_config(extra(), config)


def test_engine_namespace_tracks_revision_and_geometry():
    config = engine_config()
    a = _cpu_engine_config(extra(), config)["maru_cache_namespace"]
    assert a == _cpu_engine_config(extra(), config)["maru_cache_namespace"]
    config.model_config.revision = "other-commit"
    assert a != _cpu_engine_config(extra(), config)["maru_cache_namespace"]
    config = engine_config()
    config.cache_config.cache_dtype = "float32"
    assert a != _cpu_engine_config(extra(), config)["maru_cache_namespace"]


def test_cpu_requires_recompute_instead_of_failing_a_cache_miss():
    config = engine_config()
    config.kv_transfer_config.kv_load_failure_policy = "fail"
    with pytest.raises(ValueError, match="recompute"):
        _cpu_engine_config(extra(), config)


def test_cpu_requires_eager_hooks_and_unquantized_kv():
    config = engine_config()
    config.model_config.enforce_eager = False
    with pytest.raises(ValueError, match="enforce_eager"):
        _cpu_engine_config(extra(), config)
    config.model_config.enforce_eager = True
    config.cache_config.cache_dtype = "fp8"
    with pytest.raises(ValueError, match="quantized"):
        _cpu_engine_config(extra(), config)


def test_embeddings_are_bypassed_without_tensor_truth_conversion():
    assert _cpu_bypass_request(SimpleNamespace(prompt_embeds=torch.ones(2, 2)))
    assert _cpu_bypass_request(SimpleNamespace(cache_salt=""))
    assert not _cpu_bypass_request(SimpleNamespace(mm_features=[]))


def test_cpu_mode_never_dispatches_pageable_memory_to_direct_cuda_kernel():
    worker = make_worker(16, 16, extra())
    assert worker._packed_load_kernel_ctx([], None) is None


def test_cpu_scheduler_does_not_trust_legacy_known_keys():
    scheduler = make_scheduler(4, 4, extra())
    scheduler._known_keys = {"any-old-key"}
    scheduler._handler = SimpleNamespace(batch_exists=lambda _: [True, False])
    assert scheduler._count_matched_chunks(list(range(8))) == 1
    scheduler._handler.batch_exists = lambda _: [False, False]
    assert scheduler._count_matched_chunks(list(range(8))) == 0
