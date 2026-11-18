# SPDX-License-Identifier: Apache-2.0
"""Inline CXL retrieval failures must request recomputation and suppress stores."""

from types import SimpleNamespace

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import torch

from maru_vllm.connector import MaruConnectorMetadata, _chunk_keys
from tests.unit.vllm_connector_helpers import (
    attach_capturing_handler,
    capture_float32,
    deferred_req_meta,
    make_flash_attn_metadata,
    make_worker,
    store_req_meta,
)


@pytest.mark.parametrize("layerwise", [False, True])
@pytest.mark.parametrize("failure", ["exception", "short_response", "miss"])
def test_inline_retrieve_failure_is_reported_and_only_failed_stores_are_dropped(
    layerwise, failure
):
    worker = make_worker(
        block_size=4,
        kv_chunk_tokens=8,
        extra_config={"maru_use_layerwise": layerwise},
    )
    stored = attach_capturing_handler(
        worker, capture=capture_float32, min_alloc_bytes=4
    )
    worker._num_layers = 1

    def retrieve(keys, **kwargs):
        if failure == "exception":
            raise RuntimeError("RPC unavailable")
        return [None] * (len(keys) - (failure == "short_response"))

    worker._handler.batch_retrieve.side_effect = retrieve
    layer = "model.layers.0.self_attn"
    kv = torch.randn(2, 16, 4, 1)
    hit_tokens, cold_tokens = list(range(100, 164)), list(range(64))
    metadata = MaruConnectorMetadata(
        requests=[
            deferred_req_meta(
                token_ids=hit_tokens,
                block_ids=list(range(16)),
                num_matched_chunks=2,
                req_id="hit",
                deferred_load=False,
            ),
            store_req_meta(
                token_ids=hit_tokens,
                block_ids=list(range(16)),
                num_scheduled_tokens=16,
                num_computed_tokens=16,
                req_id="hit",
            ),
            store_req_meta(
                token_ids=cold_tokens,
                block_ids=list(range(16)),
                num_scheduled_tokens=64,
                req_id="cold",
            ),
        ]
    )
    attn = make_flash_attn_metadata()
    forward = SimpleNamespace(
        no_compile_layers={layer: SimpleNamespace(kv_cache=kv)},
        attn_metadata=attn,
    )
    worker.start_load_kv(forward, metadata)
    worker.save_kv_layer(layer, kv, attn, metadata)

    # Keep main's existing failure-scope and store-guard contracts.
    assert worker.take_failed_load_blocks() == set(range(16))
    assert worker._unsound_kv_req_ids == {"hit"}
    suffix = "_L0" if layerwise else ""
    assert set(stored) == {key + suffix for key in _chunk_keys(cold_tokens, 8)}
    assert not any(key.startswith(tuple(_chunk_keys(hit_tokens, 8))) for key in stored)
