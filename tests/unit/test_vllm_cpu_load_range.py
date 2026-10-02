# SPDX-License-Identifier: Apache-2.0
"""CPU/mixed copies and failures must preserve GPU prefix hits."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")
import torch

from maru_vllm.connector import MaruConnectorMetadata, MaruReqMeta, _req_chunk_keys
from tests.unit.test_vllm_cpu_config import extra, mixed_extra
from tests.unit.test_vllm_remote_config import remote_extra
from tests.unit.vllm_connector_helpers import (
    make_flash_attn_metadata,
    make_scheduler,
    make_worker,
)


@pytest.mark.parametrize("config", [extra(), mixed_extra(), remote_extra()])
@pytest.mark.parametrize("start,end", [(4, 12), (8, 12), (0, 12)])
@pytest.mark.parametrize(
    "failure", [None, "miss", "retrieve", "copy", "handler", "truncated", "no_layers"]
)
def test_load_only_external_tokens(config, start, end, failure):
    worker = make_worker(4, 8, config, num_kv_heads=1, head_size=1)
    cache = torch.full((8, 2, 4, 1, 1), -1.0)
    worker._kv_layout = worker._resolve_kv_layout({"layer": cache})
    worker._ensure_handler = lambda: None
    worker._handler = Mock() if failure != "handler" else None
    if worker._handler is not None:
        worker._handler.retrieve_capacity.return_value = None
    meta = MaruReqMeta(
        "load",
        list(range(16)),
        [3, 1, 5, 7],
        False,
        num_matched_chunks=2,
        load_start_token=start,
        load_end_token=end,
    )
    keys = _req_chunk_keys(meta, 8)
    slabs = [
        torch.arange(16, dtype=torch.float32).reshape(2, 1, 8, 1) + i * 100
        for i in range(2)
    ]

    def retrieve(requested):
        assert requested == keys[start // 8 : 2]
        if failure == "retrieve":
            raise RuntimeError("lost replica")
        if failure == "truncated":
            return []
        if failure == "miss":
            return [None] * len(requested)
        return [
            SimpleNamespace(view=bytearray(slabs[keys.index(key)].numpy().tobytes()))
            for key in requested
        ]

    worker._batch_retrieve_all = retrieve
    if failure == "copy":
        worker._inject_kv_into_layer = Mock(side_effect=RuntimeError("copy failed"))
    context = SimpleNamespace(
        attn_metadata=make_flash_attn_metadata(),
        no_compile_layers={"layer": SimpleNamespace(kv_cache=cache)},
    )
    if failure == "no_layers":
        context.no_compile_layers = {}
    worker.start_load_kv(context, MaruConnectorMetadata(requests=[meta]))
    assert worker.take_failed_load_blocks() == (
        set(meta.block_ids[start // 4 : end // 4]) if failure else set()
    )
    for token in range(16):
        value = cache[meta.block_ids[token // 4], :, token % 4, 0, 0]
        expected = (
            slabs[token // 8][:, 0, token % 8, 0]
            if failure is None and start <= token < end
            else torch.full((2,), -1.0)
        )
        torch.testing.assert_close(value, expected)


@pytest.mark.parametrize("config", [extra(), mixed_extra(), remote_extra()])
def test_scheduler_keeps_gpu_prefix_and_actual_allocation(config):
    scheduler = make_scheduler(4, 8, config)
    scheduler._count_matched_chunks = lambda tokens: 2
    request = SimpleNamespace(request_id="load", prompt_token_ids=list(range(16)))
    # Full Maru hit: hold back a compute block; GPU already has one block.
    assert scheduler.get_num_new_matched_tokens(request, 4) == (8, False)
    scheduler.update_state_after_alloc(request, None, 4)
    output = SimpleNamespace(
        scheduled_new_reqs=[
            SimpleNamespace(
                req_id="load",
                prompt_token_ids=request.prompt_token_ids,
                block_ids=([3, 1, 5, 7],),
            )
        ],
        scheduled_cached_reqs=SimpleNamespace(req_ids=[]),
        finished_req_ids=set(),
        preempted_req_ids=set(),
    )
    meta = scheduler.build_connector_meta(output).requests[0]
    assert (meta.load_start_token, meta.load_end_token) == (4, 8)


@pytest.mark.parametrize("config", [extra(), mixed_extra(), remote_extra()])
@pytest.mark.parametrize("other_running", [False, True])
def test_real_scheduler_preserves_other_request_sharing_prefix(
    tmp_path, config, other_running
):
    from vllm.sampling_params import SamplingParams
    from vllm.utils.hashing import sha256
    from vllm.v1.core.kv_cache_utils import get_request_block_hasher, init_none_hash
    from vllm.v1.request import Request

    from tests.unit.vllm_connector_helpers import make_vllm_scheduler

    engine = make_vllm_scheduler(tmp_path, 4, 8, config, enable_prefix_caching=True)
    init_none_hash(sha256)
    hasher = get_request_block_hasher(4, sha256)
    shared = list(range(4))
    decoding = Request(
        "decoding",
        shared + list(range(50, 63)),
        SamplingParams(max_tokens=8),
        None,
        block_hasher=hasher,
    )
    loading = Request(
        "loading",
        list(range(16)),
        SamplingParams(max_tokens=8),
        None,
        block_hasher=hasher,
    )
    manager = engine.kv_cache_manager
    assert manager.allocate_slots(decoding, 17) is not None
    decoding.num_computed_tokens = 17
    manager.cache_blocks(decoding, 16)
    blocks, hit = manager.get_computed_blocks(loading)
    assert hit == 4
    assert (
        manager.allocate_slots(
            loading,
            4,
            num_new_computed_tokens=hit,
            new_computed_blocks=blocks,
            num_external_computed_tokens=8,
        )
        is not None
    )
    loading.num_computed_tokens = 16
    ids = manager.get_block_ids(loading.request_id)[0]
    assert ids[0] == manager.get_block_ids(decoding.request_id)[0][0]
    worker = make_worker(4, 8, config)
    meta = MaruReqMeta(
        "loading",
        list(range(16)),
        ids,
        False,
        num_matched_chunks=2,
        load_start_token=4,
        load_end_token=12,
    )
    worker._fail_deferred_load(meta)
    affected, _, _ = engine._update_requests_with_invalid_blocks(
        [decoding, loading] if other_running else [loading],
        worker.take_failed_load_blocks(),
        {"loading": 4},
    )
    assert affected == {"loading"}
    assert loading.num_computed_tokens == 4
    assert decoding.num_computed_tokens == 17


@pytest.mark.parametrize("capacity", [1, 2, 3])
def test_bounded_read_buffers_load_in_batches(capacity):
    """A backend with bounded read buffers gets batches it can hold, each
    released before the next is retrieved."""
    worker = make_worker(4, 8, remote_extra(), num_kv_heads=1, head_size=1)
    cache = torch.full((12, 2, 4, 1, 1), -1.0)
    worker._kv_layout = worker._resolve_kv_layout({"layer": cache})
    worker._ensure_handler = lambda: None
    worker._handler = Mock()
    worker._handler.retrieve_capacity.return_value = capacity
    meta = MaruReqMeta(
        "load",
        list(range(24)),
        list(range(6)),
        False,
        num_matched_chunks=3,
        load_start_token=0,
        load_end_token=24,
    )
    keys = _req_chunk_keys(meta, 8)
    slabs = {
        key: torch.arange(16, dtype=torch.float32).reshape(2, 1, 8, 1) + i * 100
        for i, key in enumerate(keys)
    }
    held = []

    def retrieve(requested):
        assert len(requested) <= capacity and not held
        held.extend(requested)
        return [
            SimpleNamespace(view=bytearray(slabs[k].numpy().tobytes()))
            for k in requested
        ]

    worker._handler.release_retrieved.side_effect = lambda infos: held.clear()
    worker._batch_retrieve_all = retrieve
    context = SimpleNamespace(
        attn_metadata=make_flash_attn_metadata(),
        no_compile_layers={"layer": SimpleNamespace(kv_cache=cache)},
    )
    worker.start_load_kv(context, MaruConnectorMetadata(requests=[meta]))
    assert not worker.take_failed_load_blocks()
    assert worker._handler.release_retrieved.call_count == -(-3 // capacity)
    for token in range(24):
        value = cache[token // 4, :, token % 4, 0, 0]
        torch.testing.assert_close(value, slabs[keys[token // 8]][:, 0, token % 8, 0])


def test_a_missing_later_batch_fails_only_from_that_batch():
    worker = make_worker(4, 8, remote_extra(), num_kv_heads=1, head_size=1)
    cache = torch.full((12, 2, 4, 1, 1), -1.0)
    worker._kv_layout = worker._resolve_kv_layout({"layer": cache})
    worker._ensure_handler = lambda: None
    worker._handler = Mock()
    worker._handler.retrieve_capacity.return_value = 1
    meta = MaruReqMeta(
        "load",
        list(range(24)),
        list(range(6)),
        False,
        num_matched_chunks=3,
        load_start_token=0,
        load_end_token=24,
    )
    keys = _req_chunk_keys(meta, 8)
    slab = torch.arange(16, dtype=torch.float32).reshape(2, 1, 8, 1)
    worker._batch_retrieve_all = lambda req: (
        [None]
        if req[0] == keys[1]
        else [SimpleNamespace(view=bytearray(slab.numpy().tobytes()))]
    )
    context = SimpleNamespace(
        attn_metadata=make_flash_attn_metadata(),
        no_compile_layers={"layer": SimpleNamespace(kv_cache=cache)},
    )
    worker.start_load_kv(context, MaruConnectorMetadata(requests=[meta]))
    assert worker.take_failed_load_blocks() == {2, 3, 4, 5}  # chunk 1 onward


def test_a_failed_batch_copy_stops_the_later_batches():
    worker = make_worker(4, 8, remote_extra(), num_kv_heads=1, head_size=1)
    cache = torch.full((12, 2, 4, 1, 1), -1.0)
    worker._kv_layout = worker._resolve_kv_layout({"layer": cache})
    worker._ensure_handler = lambda: None
    worker._handler = Mock()
    worker._handler.retrieve_capacity.return_value = 1
    meta = MaruReqMeta(
        "load",
        list(range(24)),
        list(range(6)),
        False,
        num_matched_chunks=3,
        load_start_token=0,
        load_end_token=24,
    )
    requested = []

    def retrieve(keys):
        requested.append(keys)
        return [SimpleNamespace(view=bytearray(3))]  # too short to copy

    worker._batch_retrieve_all = retrieve
    context = SimpleNamespace(
        attn_metadata=make_flash_attn_metadata(),
        no_compile_layers={"layer": SimpleNamespace(kv_cache=cache)},
    )
    worker.start_load_kv(context, MaruConnectorMetadata(requests=[meta]))
    assert worker.take_failed_load_blocks() == {0, 1, 2, 3, 4, 5}
    assert len(requested) == 1  # the later batches are not read
    assert worker._handler.release_retrieved.call_count == 1
