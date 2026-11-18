# SPDX-License-Identifier: Apache-2.0
"""Prefix-cache hits must not leave holes in the external conversation KV."""

from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import torch

import maru
from maru_vllm.connector import (
    MaruConnectorMetadata,
    _chunk_keys,
    _cpu_engine_config,
    _create_maru_handler,
    _step_chunks,
)
from maru_vllm.kv_groups import (
    GROUP_FULL,
    GROUP_MAMBA,
    GROUP_MLA,
    GROUP_SKIP,
    GROUP_SLIDING_WINDOW,
    GroupCache,
    KVGroup,
)
from tests.unit.test_vllm_cpu_config import engine_config
from tests.unit.vllm_connector_helpers import (
    attach_capturing_handler,
    fake_cached_reqs,
    fake_new_request_data,
    fake_scheduler_output,
    make_flash_attn_metadata,
    make_scheduler,
    make_worker,
)

BLOCK = 4
CHUNK = 8


@pytest.mark.parametrize("write_behind", [False, True])
def test_retained_mla_blocks_fill_the_generated_prefix(write_behind):
    """Exercise the MLA worker copy even on vLLM without bounded-replay specs."""
    group = KVGroup(
        1, GROUP_MLA, "MLAAttentionSpec", ("kv",), BLOCK, 16, group_page_bytes=16
    )
    groups = [
        KVGroup(
            0,
            GROUP_SKIP,
            "SlidingWindowMLASpec",
            ("swa",),
            BLOCK,
            16,
            sliding_window=128,
        ),
        group,
    ]
    extra = {"maru_async_store": write_behind}
    scheduler = make_scheduler(BLOCK, CHUNK, extra, kv_groups=groups)
    worker = make_worker(BLOCK, CHUNK, extra, kv_groups=groups)
    blocks = torch.arange(12 * 16, dtype=torch.uint8).reshape(12, 16)
    worker._group_caches = (GroupCache(group, (blocks,)),)
    stored = attach_capturing_handler(worker)
    scheduler._handler = MagicMock()
    scheduler._handler.batch_exists.side_effect = lambda keys: [
        key in stored for key in keys
    ]
    for rid, end, computed in [("t1", 16, 0), ("t2", 32, 24)]:
        tokens = list(range(end))
        request = SimpleNamespace(request_id=rid, prompt_token_ids=tokens)
        assert scheduler.get_num_new_matched_tokens(request, computed) == (0, False)
        ids = list(range(1, end // BLOCK + 1))
        entry = scheduler._store_meta(
            rid, tokens, ([0] * len(ids), ids), computed, end - computed
        )
        assert entry.store_start_token == (16 if computed else 0)
        worker.wait_for_save(MaruConnectorMetadata(requests=[entry]))
        assert set(stored) == {
            group.object_key(key, CHUNK) for key in _chunk_keys(tokens, CHUNK)
        }
    generated_key = group.object_key(_chunk_keys(list(range(32)), CHUNK)[2], CHUNK)
    assert stored[generated_key] == bytes(blocks[5:7].numpy())
    scheduler._known_keys.clear()
    request = SimpleNamespace(request_id="t3", prompt_token_ids=list(range(40)))
    assert scheduler.get_num_new_matched_tokens(request, 0) == (32, False)
    worker.shutdown()


@pytest.mark.parametrize("write_behind", [False, True])
def test_a_generated_prefix_is_stored_and_reused_after_gpu_eviction(
    monkeypatch, write_behind
):
    """Two turns copy real CPU tensor bytes; a third lookup must cross O1."""
    extra = {"maru_async_store": write_behind}
    scheduler = make_scheduler(BLOCK, CHUNK, extra)
    worker = make_worker(BLOCK, CHUNK, extra)
    layer = "model.layers.0.self_attn"
    kv = torch.arange(2 * 12 * BLOCK, dtype=torch.float32).reshape(2, 12, BLOCK, 1)
    worker._kv_caches = {layer: kv}
    worker._num_layers = 1
    worker._kv_layout = worker._resolve_kv_layout(worker._kv_caches)
    if write_behind:
        # Execute the real worker gather/copy/completion path on CPU tensors;
        # only CUDA ordering objects and the placement kernel are simulated.
        monkeypatch.setattr(torch.cuda, "Stream", lambda **kwargs: MagicMock())
        monkeypatch.setattr(torch.cuda, "Event", lambda **kwargs: MagicMock())
        monkeypatch.setattr(torch.cuda, "current_stream", lambda *args: MagicMock())
        monkeypatch.setattr(torch.cuda, "stream", lambda *args: nullcontext())

        def transfer(slab, ptrs, slots, *args, **kwargs):
            slab[:, 0] = kv.reshape(2, -1, 1)[:, slots]

        ops = SimpleNamespace(
            TransferDirection=SimpleNamespace(D2H="D2H"),
            multi_layer_kv_transfer=transfer,
        )
        kernel = (ops, torch.tensor([kv.data_ptr()]), 48, BLOCK, 1, "FMT")
    stored = attach_capturing_handler(worker, capture=bytearray)
    scheduler._handler = MagicMock()
    scheduler._handler.batch_exists.side_effect = lambda keys: [
        k in stored for k in keys
    ]

    for rid, tokens, local, expected_start in [("t1", 16, 0, 0), ("t2", 32, 24, 16)]:
        request = SimpleNamespace(request_id=rid, prompt_token_ids=list(range(tokens)))
        assert scheduler.get_num_new_matched_tokens(request, local) == (0, False)
        new = fake_new_request_data(
            prompt_token_ids=request.prompt_token_ids,
            block_ids=list(range(tokens // BLOCK)),
            num_computed_tokens=local,
            req_id=rid,
        )
        meta = scheduler.build_connector_meta(
            fake_scheduler_output([new], None, {rid: tokens - local})
        )
        (entry,) = meta.requests
        assert entry.store_start_token == expected_start
        if write_behind:
            worker._store_packed_slabs_write_behind(kernel, meta)
            # Wait for registration on the real completion thread before lookup.
            import time

            deadline = time.monotonic() + 3
            while worker.get_finished_saving({rid}) != {rid}:
                assert time.monotonic() < deadline
                time.sleep(0.001)
        else:
            worker.save_kv_layer(layer, kv, make_flash_attn_metadata(), meta)
        assert set(stored) == set(_chunk_keys(request.prompt_token_ids, CHUNK))

    # The former decode output occupies [16,24); its exact K/V bytes survive.
    generated = torch.frombuffer(
        stored[_chunk_keys(list(range(32)), CHUNK)[2]], dtype=torch.float32
    )
    expected = torch.stack((kv[0].reshape(-1)[16:24], kv[1].reshape(-1)[16:24]))
    torch.testing.assert_close(generated, expected.reshape(-1))
    scheduler._known_keys.clear()
    after_eviction = SimpleNamespace(request_id="t3", prompt_token_ids=list(range(40)))
    assert scheduler.get_num_new_matched_tokens(after_eviction, 0) == (32, False)
    worker.shutdown()


@pytest.mark.parametrize("write_behind", [False, True])
@pytest.mark.parametrize("maru_chunks", [0, 2])
def test_prefix_repair_survives_chunked_prefill_without_recopying_sync_steps(
    write_behind, maru_chunks
):
    scheduler = make_scheduler(BLOCK, CHUNK, {"maru_async_store": write_behind})
    if maru_chunks:
        scheduler._maru_hits["r"] = (maru_chunks, 24)
    first = scheduler._store_meta("r", list(range(48)), (list(range(8)),), 24, 8)
    assert first.store_start_token == maru_chunks * CHUNK
    assert list(_step_chunks(first, CHUNK, 6)) == list(range(maru_chunks, 4))
    meta = scheduler.build_connector_meta(
        fake_scheduler_output(
            [], fake_cached_reqs(["r"], [([8, 9, 10, 11],)], [32]), {"r": 16}
        )
    )
    (last,) = meta.requests
    assert last.store_start_token == maru_chunks * CHUNK  # write-behind starts here
    assert not last.store_cached_prefix
    assert list(_step_chunks(last, CHUNK, 6)) == [4, 5]


@pytest.mark.parametrize("kind", [GROUP_FULL, GROUP_MAMBA, GROUP_SLIDING_WINDOW])
def test_only_groups_with_complete_history_rewind_their_store(kind):
    groups = [
        KVGroup(0, GROUP_FULL, "FullAttentionSpec", ("a",), BLOCK, 32),
        KVGroup(1, kind, "FullAttentionSpec", ("b",), BLOCK, 32),
    ]
    scheduler = make_scheduler(BLOCK, CHUNK, kv_groups=groups)
    scheduler._maru_hits["r"] = (2, 24)
    meta = scheduler._store_meta(
        "r", list(range(32)), (list(range(8)), list(range(8))), 24, 8
    )
    assert meta.store_start_token == (16 if kind == GROUP_FULL else 24)
    assert meta.store_cached_prefix is (kind == GROUP_FULL)


@pytest.mark.parametrize(
    "kind,window,eligible",
    [
        (GROUP_FULL, 128, True),
        (GROUP_MLA, None, True),
        (GROUP_MLA, 128, False),
        (GROUP_SKIP, 128, True),
        (GROUP_MAMBA, None, False),
        (GROUP_SLIDING_WINDOW, 128, False),
    ],
)
def test_prefix_repair_follows_retained_blocks(kind, window, eligible):
    groups = [
        KVGroup(0, GROUP_FULL, "FullAttentionSpec", ("a",), BLOCK, 32),
        KVGroup(1, kind, "Spec", ("b",), BLOCK, 32, sliding_window=window),
    ]
    scheduler = make_scheduler(BLOCK, CHUNK, kv_groups=groups)
    scheduler._maru_hits["r"] = (2, 24)
    entry = scheduler._store_meta(
        "r", list(range(32)), ([1, 2, 3, 4, 5, 6, 7, 8],) * 2, 24, 8
    )
    assert entry.store_start_token == (16 if eligible else 24)
    assert entry.store_cached_prefix is eligible


@pytest.mark.parametrize("backend", ["cxl", "remote_cxl"])
@pytest.mark.parametrize(
    "setting,expected",
    [
        ({}, True),
        ({"maru_auto_expand": False}, False),
        ({"maru_auto_expand": True}, True),
    ],
)
def test_vllm_passes_pool_expansion_policy_to_the_handler(
    monkeypatch, backend, setting, expected
):
    configs = []

    class Handler:
        def __init__(self, config):
            configs.append(config)

        def connect(self):
            return True

    monkeypatch.setattr(maru, "MaruHandler", Handler)
    _create_maru_handler(
        {
            "maru_storage_backend": backend,
            "maru_pool_size": "16M",
            "maru_engine_id": "e",
            "maru_cache_namespace": "n",
            **setting,
        }
    )
    assert configs[0].auto_expand is expected


@pytest.mark.parametrize("failure", ["policy", "eager", "dtype"])
def test_remote_config_errors_name_the_remote_backend(failure):
    config = engine_config()
    if failure == "policy":
        config.kv_transfer_config.kv_load_failure_policy = "fail"
    elif failure == "eager":
        config.model_config.enforce_eager = False
    else:
        config.cache_config.cache_dtype = "fp8"
    with pytest.raises(ValueError, match="Remote CXL storage"):
        _cpu_engine_config({"maru_storage_backend": "remote_cxl"}, config)
