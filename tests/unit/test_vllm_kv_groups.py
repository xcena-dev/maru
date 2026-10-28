# SPDX-License-Identifier: Apache-2.0
"""KV cache groups: description, support checks, hits, per-group block ids,
worker registration, stores and loads, and tensor parallelism.

The real-spec cases use the geometry vLLM 0.30 reports for Qwen3.5-0.8B
(Gated DeltaNet + full attention) and gpt-oss-20b (sliding window + full
attention).
"""

import dataclasses
import logging
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import torch
from vllm.distributed.kv_transfer.kv_connector.v1.base import (
    KVConnectorBase_V1,
    KVConnectorRole,
    supports_hma,
)
from vllm.v1 import kv_cache_interface
from vllm.v1.kv_cache_interface import (
    FullAttentionSpec,
    KVCacheGroupSpec,
    MambaSpec,
    SlidingWindowSpec,
)

import maru_vllm.connector as connector_module
from maru_vllm.connector import (
    MaruConnectorMetadata,
    MaruKVConnector,
    MaruReqMeta,
    _chunk_keys,
)
from maru_vllm.kv_groups import (
    GROUP_FULL,
    GROUP_MAMBA,
    GROUP_SLIDING_WINDOW,
    GROUP_UNSUPPORTED,
    _spec_kind,
    check_multi_group_support,
    chunk_tokens_for_groups,
    describe_kv_groups,
    group_object_key,
    hit_chunks,
    state_view,
    window_chunks,
)
from maru_vllm.kv_shard import WHOLE_MODEL, tp_kv_shard, tp_state_shard
from tests.unit.vllm_connector_helpers import (
    FailingSyncStream,
    FirstWinsServer,
    attach_capturing_handler,
    fake_cached_reqs,
    fake_scheduler_output,
    make_flash_attn_metadata,
    make_scheduler,
    make_vllm_scheduler,
    make_worker,
    tp_model_config,
)

QWEN35_BLOCK = 544
QWEN35_SLOT = 1_114_112
# vLLM < 0.23 sizes the async-load budget from num_blocks (_async_load_budget).
NUM_BLOCKS = 128
GPT_OSS_ATTN = {
    "block_size": 16,
    "num_kv_heads": 8,
    "head_size": 64,
    "dtype": torch.bfloat16,
}


def qwen35_config(mamba_cache_mode="align"):
    """Qwen3.5-0.8B: 18 GDN layers in three groups, 6 attention layers."""
    gdn = [i for i in range(24) if i % 4 != 3]
    mamba = MambaSpec(
        block_size=QWEN35_BLOCK,
        shapes=((3, 6144), (16, 128, 128)),
        dtypes=(torch.bfloat16, torch.float32),
        page_size_padded=QWEN35_SLOT,
        mamba_cache_mode=mamba_cache_mode,
    )
    attn = FullAttentionSpec(
        block_size=QWEN35_BLOCK, num_kv_heads=2, head_size=256, dtype=torch.bfloat16
    )
    groups = [
        KVCacheGroupSpec([f"model.layers.{i}.linear_attn" for i in gdn[g::3]], mamba)
        for g in range(3)
    ]
    groups.append(
        KVCacheGroupSpec(
            [f"model.layers.{i}.self_attn.attn" for i in range(3, 24, 4)], attn
        )
    )
    return SimpleNamespace(kv_cache_groups=groups, num_blocks=NUM_BLOCKS)


def gpt_oss_config():
    """gpt-oss-20b: 12 sliding-window and 12 full-attention layers."""
    return SimpleNamespace(
        kv_cache_groups=[
            KVCacheGroupSpec(
                [f"model.layers.{i}.attn" for i in range(0, 24, 2)],
                SlidingWindowSpec(sliding_window=128, **GPT_OSS_ATTN),
            ),
            KVCacheGroupSpec(
                [f"model.layers.{i}.attn" for i in range(1, 24, 2)],
                FullAttentionSpec(**GPT_OSS_ATTN),
            ),
        ],
        num_blocks=NUM_BLOCKS,
    )


# =============================================================================
# Description
# =============================================================================


@pytest.mark.parametrize(
    ("name", "kind"),
    [
        ("FullAttentionSpec", GROUP_FULL),
        ("SlidingWindowSpec", GROUP_SLIDING_WINDOW),
        ("MambaSpec", GROUP_MAMBA),
        # Subclasses of the three, refused until checked.
        ("MLAAttentionSpec", GROUP_UNSUPPORTED),
        ("SinkFullAttentionSpec", GROUP_UNSUPPORTED),
        ("RSWASpec", GROUP_UNSUPPORTED),
        ("KpoolTailSpec", GROUP_UNSUPPORTED),
        ("SlidingWindowMLASpec", GROUP_UNSUPPORTED),
        # Unrelated specs.
        ("ChunkedLocalAttentionSpec", GROUP_UNSUPPORTED),
        ("CrossAttentionSpec", GROUP_UNSUPPORTED),
    ],
)
def test_spec_kind_accepts_only_verified_vllm_classes(name, kind):
    spec_cls = getattr(kv_cache_interface, name, None)
    if spec_cls is None:
        pytest.skip(f"this vLLM has no {name}")
    assert _spec_kind(spec_cls) == kind


def test_describe_qwen35_groups():
    groups = describe_kv_groups(qwen35_config())
    assert [g.kind for g in groups] == [GROUP_MAMBA] * 3 + [GROUP_FULL]
    assert [len(g.layer_names) for g in groups] == [6, 6, 6, 6]
    assert {g.block_size for g in groups} == {QWEN35_BLOCK}
    # The padded Mamba state takes the attention slot size.
    assert {g.slot_bytes for g in groups} == {QWEN35_SLOT}
    assert [g.mamba_cache_mode for g in groups] == ["align"] * 3 + [None]
    # One object per (chunk, group), all the same size: 6 layers x 1 slot.
    assert {g.object_bytes(QWEN35_BLOCK) for g in groups} == {6 * QWEN35_SLOT}
    assert groups[0].object_bytes(QWEN35_BLOCK) == 6_684_672


def test_describe_gpt_oss_groups():
    groups = describe_kv_groups(gpt_oss_config())
    assert [g.kind for g in groups] == [GROUP_SLIDING_WINDOW, GROUP_FULL]
    assert [g.sliding_window for g in groups] == [128, None]
    # 256-token chunk = 16 blocks of 32 KiB for each of 12 layers.
    assert {g.object_bytes(256) for g in groups} == {12 * 32_768 * 16}


def test_mamba_mode_falls_back_to_cache_config():
    """A spec without the mode (older vLLM) takes cache_config's."""
    groups = describe_kv_groups(qwen35_config(None), default_mamba_cache_mode="align")
    assert [g.mamba_cache_mode for g in groups] == ["align"] * 3 + [None]


def test_chunk_is_one_block_with_a_mamba_group():
    qwen = describe_kv_groups(qwen35_config())
    assert chunk_tokens_for_groups(qwen, 1088, QWEN35_BLOCK) == QWEN35_BLOCK
    gpt = describe_kv_groups(gpt_oss_config())
    assert chunk_tokens_for_groups(gpt, 256, 16) == 256


# =============================================================================
# Support checks
# =============================================================================


def _check(
    groups, block_size, use_layerwise=False, speculative=False, inline_loads=False
):
    check_multi_group_support(
        groups,
        block_size=block_size,
        use_layerwise=use_layerwise,
        speculative=speculative,
        inline_loads=inline_loads,
    )


def test_supported_multi_group_models_pass():
    _check(describe_kv_groups(qwen35_config()), QWEN35_BLOCK)
    _check(describe_kv_groups(gpt_oss_config()), 16)


def test_single_group_is_always_accepted():
    single = describe_kv_groups(qwen35_config())[3:]
    _check(single, 16, use_layerwise=True, speculative=True)


def test_subclass_of_a_supported_spec_is_rejected():
    """A subclass can hold something else per block, like KpoolTailSpec's ring."""

    class ScratchRingSpec(SlidingWindowSpec):
        pass

    config = gpt_oss_config()
    config.kv_cache_groups[0] = KVCacheGroupSpec(
        config.kv_cache_groups[0].layer_names,
        ScratchRingSpec(sliding_window=128, **GPT_OSS_ATTN),
    )
    with pytest.raises(ValueError, match="ScratchRingSpec alongside other groups"):
        _check(describe_kv_groups(config), 16)


def test_layerwise_is_rejected():
    with pytest.raises(ValueError, match="maru_use_layerwise=false"):
        _check(describe_kv_groups(gpt_oss_config()), 16, use_layerwise=True)


def test_speculative_decoding_is_rejected():
    with pytest.raises(ValueError, match="speculative decoding"):
        _check(describe_kv_groups(gpt_oss_config()), 16, speculative=True)


@pytest.mark.parametrize("mode", ["none", "all"])
def test_mamba_needs_align_mode(mode):
    with pytest.raises(ValueError, match="--mamba-cache-mode align") as refused:
        _check(describe_kv_groups(qwen35_config(mode)), QWEN35_BLOCK)
    # vLLM sets "none" when prefix caching is off, whatever the flag says.
    assert "prefix caching on" in str(refused.value)


def test_block_size_must_match_cache_config():
    with pytest.raises(ValueError, match="block size 32"):
        _check(describe_kv_groups(gpt_oss_config()), 32)


# =============================================================================
# Connector
# =============================================================================


def _connector_config(block_size, **extra):
    return SimpleNamespace(
        kv_transfer_config=SimpleNamespace(kv_connector_extra_config=extra),
        cache_config=SimpleNamespace(block_size=block_size, mamba_cache_mode="align"),
        speculative_config=None,
        model_config=SimpleNamespace(max_model_len=4096),
    )


def test_hybrid_support_is_declared_from_vllm_0_30():
    """Older vLLM cannot recompute a failed load of a several-group model, so
    Maru leaves its hybrid KV cache manager off there, as before."""
    import vllm

    from maru_vllm.connector import _vllm_supports_hybrid

    assert supports_hma(MaruKVConnector) == _vllm_supports_hybrid(vllm.__version__)


@pytest.mark.parametrize(
    ("version", "declared", "warning"),
    [
        ("0.30.0", True, None),
        # vllm-project/vllm#50388 is in 0.30.0rc1, so pre-releases count.
        ("0.30.0rc1", True, None),
        ("0.30.0rc2.dev5+gabc1234", True, None),
        ("0.30.1rc1.dev12+gabc1234", True, None),
        ("0.29.2", False, "vLLM 0.29.2 is older than 0.30.0"),
        # A main build between the 0.29 and 0.30 releases.
        ("0.29.1rc1.dev40+gabc1234", False, "older than 0.30.0"),
        # vLLM reports "dev" when it cannot read its own version file.
        ("dev", False, "cannot read the vLLM version 'dev'"),
    ],
)
def test_hybrid_support_version_check_logs_when_it_says_no(
    caplog, version, declared, warning
):
    from maru_vllm.connector import _vllm_supports_hybrid

    with caplog.at_level(logging.WARNING, logger="maru_vllm.connector"):
        assert _vllm_supports_hybrid(version) is declared
    messages = [r.getMessage() for r in caplog.records]
    if warning is None:
        assert messages == []
    else:
        (message,) = messages
        assert warning in message
        assert "does not declare SupportsHMA" in message


def test_a_mamba_model_stores_one_block_per_chunk():
    connector = MaruKVConnector(
        _connector_config(QWEN35_BLOCK, maru_kv_chunk_tokens=2 * QWEN35_BLOCK),
        KVConnectorRole.SCHEDULER,
        qwen35_config(),
    )
    assert connector._kv_chunk_tokens == QWEN35_BLOCK
    assert connector._scheduler._kv_chunk_tokens == QWEN35_BLOCK
    assert connector._scheduler._multi_group


def test_connector_reports_the_setting_to_change():
    with pytest.raises(ValueError, match="maru_use_layerwise=false") as refused:
        MaruKVConnector(
            _connector_config(16, maru_use_layerwise=True),
            KVConnectorRole.SCHEDULER,
            gpt_oss_config(),
        )
    assert "--disable-hybrid-kv-cache-manager" in str(refused.value)
    # gpt-oss mixes window sizes, which FlashInfer refuses in one group.
    assert "--attention-backend TRITON_ATTN" in str(refused.value)


def _one_group_config():
    return SimpleNamespace(
        kv_cache_groups=[
            KVCacheGroupSpec(
                [f"model.layers.{i}.attn" for i in range(24)],
                FullAttentionSpec(**GPT_OSS_ATTN),
            )
        ],
        num_blocks=NUM_BLOCKS,
    )


@pytest.mark.parametrize(
    ("kv_cache_config", "extra", "parked"),
    [
        (gpt_oss_config, {}, True),
        (gpt_oss_config, {"maru_async_load": True}, True),
        (gpt_oss_config, {"maru_async_load": False}, None),
        (gpt_oss_config, {"maru_enable_deferred_loading": False}, None),
        (_one_group_config, {}, False),
    ],
)
def test_several_groups_park_every_load(kv_cache_config, extra, parked):
    """vLLM 0.31+ stops the engine on a failed inline load of a model with
    several groups (#126), and vLLM 0.30 recomputes one into wrong output
    under "recompute" (vllm-project/vllm#49250). Such a model parks its
    loads unless told otherwise, and inline loads are refused; one group
    keeps the inline default."""
    config = _connector_config(16, **extra)
    if parked is not None:
        connector = MaruKVConnector(
            config, KVConnectorRole.SCHEDULER, kv_cache_config()
        )
        assert connector._scheduler._deferred_loading is parked
        return
    for role in (KVConnectorRole.SCHEDULER, KVConnectorRole.WORKER):
        with pytest.raises(ValueError, match="maru_async_load=false") as refusal:
            MaruKVConnector(config, role, kv_cache_config())
    assert "xcena-dev/maru#126" in str(refusal.value)
    assert "vllm-project/vllm#49250" in str(refusal.value)


@pytest.mark.parametrize("declared", [True, False])
def test_a_missing_kv_cache_config_is_reported_when_hma_is_declared(
    monkeypatch, caplog, declared
):
    """vLLM 0.30 always passes the config; without it the groups are unknown."""
    import maru_vllm.connector as connector_module

    # Only whether the tuple is empty matters here.
    monkeypatch.setattr(
        connector_module, "_HYBRID_SUPPORT", (object,) if declared else ()
    )
    config = _connector_config(16)
    config.cache_config.num_gpu_blocks = NUM_BLOCKS  # read on vLLM < 0.23
    with caplog.at_level(logging.WARNING, logger="maru_vllm.connector"):
        connector = MaruKVConnector(config, KVConnectorRole.SCHEDULER, None)
    assert connector._kv_groups == []
    assert ("got no kv_cache_config" in caplog.text) == declared


def test_request_finished_all_groups_answers_per_request():
    connector = MaruKVConnector.__new__(MaruKVConnector)
    connector._scheduler = _two_group_scheduler({"maru_async_store": True})
    request = SimpleNamespace(request_id="r1")
    assert connector.request_finished_all_groups(request, ([1, 2], [3, 4])) == (
        True,
        None,
    )
    # A parked request: vLLM already keeps its blocks for the receive.
    connector._scheduler._awaiting_deferred_recv.add("r2")
    parked = SimpleNamespace(request_id="r2")
    assert connector.request_finished_all_groups(parked, ([5], [6])) == (False, None)


# =============================================================================
# Scheduler: block ids of every group
# =============================================================================


def _two_group_scheduler(extra_config=None, **kwargs):
    """gpt-oss groups: block 16, chunk 32."""
    return make_scheduler(
        16, 32, extra_config, kv_groups=describe_kv_groups(gpt_oss_config()), **kwargs
    )


def _new_req(block_ids, num_tokens=64):
    return SimpleNamespace(
        req_id="r1", prompt_token_ids=list(range(num_tokens)), block_ids=block_ids
    )


def _step(sched, scheduled, **kwargs):
    """The one request's metadata for a scheduler step of ``scheduled`` tokens."""
    meta = sched.build_connector_meta(
        fake_scheduler_output(num_scheduled_tokens={"r1": scheduled}, **kwargs)
    )
    (req,) = meta.requests
    return req


def _load_and_store(sched, scheduled, **kwargs):
    """The load and the store metadata of a step that loads ``r1`` inline."""
    meta = sched.build_connector_meta(
        fake_scheduler_output(num_scheduled_tokens={"r1": scheduled}, **kwargs)
    )
    load, store = meta.requests
    assert not load.is_store and store.is_store
    return load, store


def test_an_image_request_skips_maru_with_several_groups():
    """The Qwen3.5 image case of #116 takes this path."""
    sched = _two_group_scheduler()
    sched._handler = MagicMock()
    request = SimpleNamespace(
        request_id="r1", prompt_token_ids=list(range(96)), mm_features=[object()]
    )

    assert sched.get_num_new_matched_tokens(request, 0) == (0, False)
    sched._handler.batch_exists.assert_not_called()
    new_req = _new_req(([1, 2, 3], [11, 12, 13]), 96)
    new_req.mm_features = request.mm_features
    meta = sched.build_connector_meta(
        fake_scheduler_output(new_reqs=[new_req], num_scheduled_tokens={"r1": 96})
    )
    assert meta.requests == []
    assert "r1" not in sched._requests_need_store


def test_store_carries_every_group_across_chunked_prefill():
    sched = _two_group_scheduler()
    # 96 tokens = 3 chunks of 32. Step 1 covers 40 tokens in 3 blocks.
    req = _step(sched, 40, new_reqs=[_new_req(([1, 2, 3], [11, 12, 13]), 96)])
    assert req.block_ids == [1, 2, 3]
    assert req.group_block_ids == ([1, 2, 3], [11, 12, 13])

    # Step 2 fits in block 3: vLLM reports no new blocks.
    req = _step(sched, 8, cached=fake_cached_reqs(["r1"], [None], [40]))
    assert req.group_block_ids == ([1, 2, 3], [11, 12, 13])
    assert "r1" in sched._requests_need_store

    req = _step(
        sched, 48, cached=fake_cached_reqs(["r1"], [([4, 5, 6], [14, 15, 16])], [48])
    )
    assert req.block_ids == [1, 2, 3, 4, 5, 6]
    assert req.group_block_ids == ([1, 2, 3, 4, 5, 6], [11, 12, 13, 14, 15, 16])
    assert "r1" not in sched._requests_need_store


def test_single_group_leaves_group_block_ids_empty():
    sched = make_scheduler(16, 32)
    req = _step(sched, 64, new_reqs=[_new_req(([1, 2, 3, 4],))])
    assert req.block_ids == [1, 2, 3, 4]
    assert req.group_block_ids == ()


def test_deferred_load_carries_every_group():
    sched = _two_group_scheduler({"maru_async_load": True})
    request = SimpleNamespace(request_id="r1", prompt_token_ids=list(range(64)))
    blocks = MagicMock()
    blocks.get_block_ids.return_value = ([1, 2], [11, 12])
    sched.update_state_after_alloc(request, blocks, 32)

    req = sched.build_connector_meta(fake_scheduler_output()).requests[0]
    assert req.deferred_load
    assert req.block_ids == [1, 2]
    assert req.group_block_ids == ([1, 2], [11, 12])


def test_resumed_load_carries_every_group():
    """A request preempted and resumed with an external hit, loaded in step."""
    sched = _two_group_scheduler()
    tokens = list(range(64))
    request = SimpleNamespace(
        request_id="r1", prompt_token_ids=tokens, all_token_ids=tokens
    )
    sched.update_state_after_alloc(request, MagicMock(), 32)

    new_block_ids = ([1, 2, 3, 4], [11, 12, 13, 14])
    cached = fake_cached_reqs(["r1"], [new_block_ids], [32], resumed=["r1"])
    req, store = _load_and_store(sched, 32, cached=cached)
    assert req.block_ids == [1, 2, 3, 4]
    assert req.group_block_ids == new_block_ids
    assert store.group_block_ids == new_block_ids


# =============================================================================
# Hits across groups
# =============================================================================


def _has(objects):
    """``has_object`` over a set of (chunk index, group id)."""
    return lambda i, g: (i, g.group_id) in objects


def test_hit_ends_where_every_mamba_group_has_a_state():
    groups = describe_kv_groups(qwen35_config())
    attention = {(i, 3) for i in range(4)}
    # Prefill steps ended at chunks 2 and 3.
    states = {(i, g) for i in (2, 3) for g in range(3)}
    assert hit_chunks(groups, 4, QWEN35_BLOCK, _has(attention | states)) == (4, 4)
    assert hit_chunks(groups, 2, QWEN35_BLOCK, _has(attention | states)) == (0, 2)
    # Two of three Mamba groups are not a state.
    partial = attention | {(3, 0), (3, 1)}
    assert hit_chunks(groups, 4, QWEN35_BLOCK, _has(partial)) == (0, 4)


def test_hit_stops_at_a_missing_attention_chunk():
    groups = describe_kv_groups(qwen35_config())
    attention = {(i, 3) for i in range(4)} - {(2, 3)}
    states = {(i, g) for i in (1, 3) for g in range(3)}
    assert hit_chunks(groups, 4, QWEN35_BLOCK, _has(attention | states)) == (2, 2)


def test_sliding_window_needs_the_chunks_its_window_covers():
    groups = describe_kv_groups(gpt_oss_config())  # group 0 window 128, chunk 256
    full = {(i, 1) for i in range(4)}
    assert hit_chunks(groups, 4, 256, _has(full | {(3, 0)})) == (4, 4)
    # A window shorter than a chunk lies in the hit's last chunk alone.
    assert hit_chunks(groups, 4, 256, _has(full | {(1, 0)})) == (2, 4)
    assert hit_chunks(groups, 4, 256, _has(full)) == (0, 4)


def test_window_chunks():
    swa = describe_kv_groups(gpt_oss_config())[0]
    assert window_chunks(swa, 1024, 256) == range(3, 4)  # tokens 897..1023
    wide = dataclasses.replace(swa, sliding_window=300)
    assert window_chunks(wide, 1024, 256) == range(2, 4)  # tokens 725..1023
    assert window_chunks(wide, 256, 256) == range(0, 1)
    # The window includes the token being computed: W = C + 1 still needs
    # one chunk, W = C + 2 reaches into the one before.
    assert window_chunks(dataclasses.replace(swa, sliding_window=257), 1024, 256) == (
        range(3, 4)
    )
    assert window_chunks(dataclasses.replace(swa, sliding_window=258), 1024, 256) == (
        range(2, 4)
    )


class _Store:
    """A MaruHandler stand-in holding a set of keys."""

    def __init__(self, keys=()):
        self.keys = set(keys)
        self.queries = []

    def batch_exists(self, keys, route_key=None):
        self.queries.append(list(keys))
        return [k in self.keys for k in keys]


def _stored_by(tokens, chunk, attention_groups, state_groups, state_chunks):
    """Keys a request stores: attention objects for every chunk, states for some."""
    keys = _chunk_keys(tokens, chunk)
    stored = {group_object_key(k, g) for k in keys for g in attention_groups}
    stored |= {group_object_key(keys[i], g) for i in state_chunks for g in state_groups}
    return stored


def _qwen35_scheduler(stored, **kwargs):
    sched = make_scheduler(
        QWEN35_BLOCK,
        QWEN35_BLOCK,
        kv_groups=describe_kv_groups(qwen35_config()),
        **kwargs,
    )
    sched._handler = _Store(stored)
    return sched


def _hit(sched, tokens, num_computed_tokens=0):
    request = SimpleNamespace(request_id="b", prompt_token_ids=tokens)
    matched, _ = sched.get_num_new_matched_tokens(request, num_computed_tokens)
    return matched


def _other(n):
    """Tokens no stored prompt contains."""
    return list(range(100_000, 100_000 + n))


def test_qwen35_prompt_of_whole_chunks_hits_nothing():
    """1,088 tokens in one prefill step: the only state is at the last chunk,
    which the held-back token excludes."""
    a = list(range(1088))
    sched = _qwen35_scheduler(_stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [1]))
    assert _hit(sched, a) == 0


def test_qwen35_same_prompt_hits_up_to_the_state():
    a = list(range(1200))  # step 1 ends at token 1,088
    sched = _qwen35_scheduler(_stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [1]))
    assert _hit(sched, a) == 1088


def test_qwen35_partial_overlap_hits_only_at_a_step_end():
    """A (2,300 tokens) ended prefill steps at tokens 1,632 and 2,176."""
    a = list(range(2300))
    stored = _stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [2, 3])
    sched = _qwen35_scheduler(stored)
    assert _hit(sched, a[:1500] + _other(300)) == 0
    assert _hit(sched, a[:1700] + _other(300)) == 1632


def test_gpt_oss_hits_whole_chunks():
    sched = make_scheduler(16, 256, kv_groups=describe_kv_groups(gpt_oss_config()))
    a = list(range(1200))
    sched._handler = _Store(_stored_by(a, 256, [0, 1], [], []))
    assert _hit(sched, a) == 1024
    # A whole-chunk prompt holds back a chunk, not a block as one group would:
    # a hit inside a chunk could need window tokens freed before the store.
    assert _hit(sched, a[:1024]) == 768


def test_lookup_asks_every_group_in_one_rpc():
    a = list(range(2300))
    sched = _qwen35_scheduler(_stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [2, 3]))
    b = a[:1700] + _other(300)
    assert _hit(sched, b) == 1632
    (query,) = sched._handler.queries
    keys = _chunk_keys(b, QWEN35_BLOCK)[:3]
    assert query == [group_object_key(k, g) for k in keys for g in range(4)]


def test_known_keys_that_give_every_chunk_skip_the_rpc():
    a = list(range(2300))
    sched = _qwen35_scheduler(_stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [2, 3]))
    b = a[:1700] + _other(300)  # 3 chunks may hit, and all 3 do
    assert _hit(sched, b) == 1632
    assert _hit(sched, b) == 1632
    assert len(sched._handler.queries) == 1


def test_a_short_hit_asks_again_for_the_missing_keys_only():
    a = list(range(2300))
    stored = _stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [2, 3])
    sched = _qwen35_scheduler(stored)
    b = a[:1500] + _other(300)
    assert _hit(sched, b) == 0
    assert _hit(sched, b) == 0
    first, second = sched._handler.queries
    assert second == [k for k in first if k not in stored]


def test_a_short_reply_leaves_the_rest_missing():
    a = list(range(1200))
    stored = _stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [1])
    sched = _qwen35_scheduler(stored)
    answer = sched._handler.batch_exists
    # Only chunk 0's four keys are answered.
    sched._handler.batch_exists = lambda keys: answer(keys)[:4]
    assert _hit(sched, a) == 0
    sched._handler.batch_exists = answer
    assert _hit(sched, a) == 1088


def _load_given(sched, request):
    """Look the request up and give its hit blocks, as vLLM does."""
    matched, _ = sched.get_num_new_matched_tokens(request, 0)
    sched.update_state_after_alloc(request, MagicMock(), matched)
    return matched


def _report_failure(sched):
    """vLLM hands the worker's failed blocks to the scheduler connector."""
    sched.update_connector_output(
        SimpleNamespace(finished_recving=None, invalid_block_ids={7})
    )


def test_a_request_whose_load_failed_computes_without_maru(caplog):
    """Under kv_load_failure_policy="recompute" vLLM asks about a request again
    after its load failed. The load can fail again even while its objects
    exist (a mapping error, say), so the request gets no hit until it
    finishes, without asking MaruServer, and the log says so once."""
    a = list(range(1200))
    sched = _qwen35_scheduler(_stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [1]))
    request = SimpleNamespace(request_id="b", prompt_token_ids=a, num_preemptions=0)
    assert _load_given(sched, request) == 1088
    _report_failure(sched)
    with caplog.at_level("WARNING"):
        for _ in range(3):  # vLLM may ask each step until it fits the request
            assert sched.get_num_new_matched_tokens(request, 0)[0] == 0
    assert len(sched._handler.queries) == 1
    assert caplog.text.count("the load for req b failed") == 1
    sched.request_finished(request, [])
    assert sched.get_num_new_matched_tokens(request, 0)[0] == 1088


@pytest.mark.parametrize("report", ["blocks", "request", None])
def test_a_later_request_asks_maru_again_only_after_a_reported_failure(report):
    """Under the default "fail" the failed request ends in an error and vLLM
    never asks about it again; a later request with the prefix must not get
    the same failing hit. A failure comes by its blocks, or by its request
    for a parked load of several groups. Without one the known keys stand."""
    a = list(range(1200))
    sched = _qwen35_scheduler(_stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [1]))
    first = SimpleNamespace(request_id="b", prompt_token_ids=a)
    assert _load_given(sched, first) == 1088
    sched._handler.keys.discard(group_object_key(_chunk_keys(a, QWEN35_BLOCK)[1], 1))
    if report == "blocks":
        _report_failure(sched)
    elif report == "request":
        sched.update_connector_output(
            SimpleNamespace(
                finished_recving={"b"}, failed_recving={"b"}, invalid_block_ids=set()
            )
        )
    else:  # vLLM passes every step's connector output, mostly with no failure
        sched.update_connector_output(
            SimpleNamespace(finished_recving={"x"}, invalid_block_ids=set())
        )
    sched.request_finished(first, [])
    later = SimpleNamespace(request_id="c", prompt_token_ids=a)
    assert sched.get_num_new_matched_tokens(later, 0)[0] == (
        1088 if report is None else 0
    )


def test_a_request_asked_for_once_more_trusts_the_known_keys():
    """Asked again before any blocks (vLLM could not fit it): no extra RPC."""
    a = list(range(1200))
    sched = _qwen35_scheduler(_stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [1]))
    request = SimpleNamespace(request_id="b", prompt_token_ids=a)
    for _ in range(2):
        assert sched.get_num_new_matched_tokens(request, 0)[0] == 1088
    assert len(sched._handler.queries) == 1


def test_a_preempted_request_keeps_its_hit():
    """A preemption is not a failure: however often the request is preempted,
    it loads again, from the known keys."""
    a = list(range(1200))
    sched = _qwen35_scheduler(_stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [1]))
    request = SimpleNamespace(request_id="b", prompt_token_ids=a, num_preemptions=0)
    assert _load_given(sched, request) == 1088
    for preemptions in range(1, 4):
        request.num_preemptions = preemptions
        assert _load_given(sched, request) == 1088
    assert len(sched._handler.queries) == 1


def test_a_request_waiting_for_blocks_asks_maru_once():
    """vLLM asks a request it cannot fit again every step: after a failure
    only the first of those asks goes to MaruServer."""
    a = list(range(1200))
    sched = _qwen35_scheduler(_stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [1]))
    first = SimpleNamespace(request_id="b", prompt_token_ids=a)
    assert _load_given(sched, first) == 1088
    _report_failure(sched)
    later = SimpleNamespace(request_id="c", prompt_token_ids=a)
    for _ in range(3):
        assert sched.get_num_new_matched_tokens(later, 0)[0] == 1088
    assert len(sched._handler.queries) == 2


def test_a_failed_parked_load_computes_without_maru():
    """vLLM calls update_state_after_alloc again, with no tokens, when a parked
    load is done; that call does not hide a failure vLLM asks about later."""
    a = list(range(1200))
    sched = make_scheduler(
        QWEN35_BLOCK,
        QWEN35_BLOCK,
        {"maru_async_load": True},
        kv_groups=describe_kv_groups(qwen35_config()),
    )
    sched._handler = _Store(_stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [1]))
    request = SimpleNamespace(request_id="b", prompt_token_ids=a, num_preemptions=0)
    assert _load_given(sched, request) == 1088
    sched.update_state_after_alloc(request, MagicMock(), 0)  # the load is done
    assert sched.get_num_new_matched_tokens(request, 0)[0] == 0


def test_layerwise_done_markers_are_asked_for_again():
    a = list(range(1200))
    keys = _chunk_keys(a, 256)
    sched = make_scheduler(16, 256, {"maru_use_layerwise": True})
    sched._handler = _Store(f"{k}_DONE" for k in keys)
    request = SimpleNamespace(request_id="b", prompt_token_ids=a)
    assert _load_given(sched, request) == 1024
    sched._handler.keys.discard(f"{keys[1]}_DONE")
    _report_failure(sched)
    later = SimpleNamespace(request_id="c", prompt_token_ids=a)
    assert sched.get_num_new_matched_tokens(later, 0)[0] == 256


def test_cpu_mode_computes_without_maru_after_a_failed_load():
    """CPU mode asks MaruServer at every lookup, where the objects still are;
    a request whose load failed gets no hit all the same."""
    a = list(range(1200))
    sched = make_scheduler(16, 256)
    sched._cpu_mode = True
    sched._handler = _Store(_chunk_keys(a, 256))
    request = SimpleNamespace(request_id="b", prompt_token_ids=a)
    assert _load_given(sched, request) == 1024
    assert sched.get_num_new_matched_tokens(request, 0)[0] == 0


def test_a_failed_layerwise_overlap_load_is_not_activated():
    """A parked layerwise-overlap load that failed has nothing to activate when
    the request is given blocks to compute."""
    a = list(range(1200))
    sched = make_scheduler(
        16, 256, {"maru_async_load": True, "maru_overlap_load_with_compute": True}
    )
    sched._handler = _Store(_chunk_keys(a, 256))
    request = SimpleNamespace(request_id="b", prompt_token_ids=a)
    matched, _ = sched.get_num_new_matched_tokens(request, 0)
    assert matched == 1024
    blocks = SimpleNamespace(get_block_ids=lambda: (list(range(1, 76)),))
    sched.update_state_after_alloc(request, blocks, matched)
    sched.build_connector_meta(fake_scheduler_output())
    assert "b" in sched._deferred_layerwise_waiting
    sched._handler.keys.clear()
    _report_failure(sched)
    assert sched.get_num_new_matched_tokens(request, 0)[0] == 0
    sched.update_state_after_alloc(request, MagicMock(), 0)
    assert "b" not in sched._deferred_layerwise_ready


def test_a_single_group_request_asks_maru_again_after_a_failure():
    a = list(range(1200))
    keys = _chunk_keys(a, 256)
    sched = make_scheduler(16, 256)
    sched._handler = _Store(keys)
    first = SimpleNamespace(request_id="b", prompt_token_ids=a)
    assert _load_given(sched, first) == 1024
    sched._handler.keys.discard(keys[1])
    _report_failure(sched)
    later = SimpleNamespace(request_id="c", prompt_token_ids=a)
    assert sched.get_num_new_matched_tokens(later, 0)[0] == 256


def test_hit_counts_past_the_gpu_prefix_cache():
    a = list(range(2300))
    sched = _qwen35_scheduler(_stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [2, 3]))
    b = a[:1700] + _other(300)
    assert _hit(sched, b, num_computed_tokens=544) == 1088
    assert _hit(sched, b, num_computed_tokens=1632) == 0


def test_lookup_failure_is_a_miss():
    sched = _qwen35_scheduler(())
    sched._handler.batch_exists = MagicMock(side_effect=RuntimeError("down"))
    assert _hit(sched, list(range(1200))) == 0


# =============================================================================
# Worker registration
# =============================================================================

# A small Qwen3.5: layers [G, G, G, A] x 2, block 16. One block slot is
# 1,024 B for every layer: attention 2 (K/V) x 16 tokens x 2 heads x 8 x bf16,
# and the 224 B GDN state (conv 3x16 bf16 + ssm 2x4x4 fp32) padded up to it.
TINY_BLOCK = 16
TINY_SLOT = 1024
TINY_STATE = 3 * 16 * 2 + 2 * 4 * 4 * 4
TINY_BLOCKS = 12
TINY_GDN = [[0, 4], [1, 5], [2, 6]]
TINY_ATTN = [3, 7]
TINY_LAYER_BYTES = TINY_BLOCKS * TINY_SLOT


def _mamba_spec():
    return MambaSpec(
        block_size=TINY_BLOCK,
        shapes=((3, 16), (2, 4, 4)),
        dtypes=(torch.bfloat16, torch.float32),
        page_size_padded=TINY_SLOT,
        mamba_cache_mode="align",
    )


def _attn_kw():
    return {
        "block_size": TINY_BLOCK,
        "num_kv_heads": 2,
        "head_size": 8,
        "dtype": torch.bfloat16,
    }


def tiny_config(gdn=TINY_GDN, attn=TINY_ATTN):
    groups = [
        KVCacheGroupSpec([f"layers.{i}.linear_attn" for i in layers], _mamba_spec())
        for layers in gdn
    ]
    groups.append(
        KVCacheGroupSpec(
            [f"layers.{i}.attn" for i in attn], FullAttentionSpec(**_attn_kw())
        )
    )
    return SimpleNamespace(kv_cache_groups=groups, num_blocks=NUM_BLOCKS)


def tiny_swa_config():
    """A small gpt-oss: sliding-window layers 0, 2 and full layers 1, 3."""
    return SimpleNamespace(
        kv_cache_groups=[
            KVCacheGroupSpec(
                ["layers.0.attn", "layers.2.attn"],
                SlidingWindowSpec(sliding_window=32, **_attn_kw()),
            ),
            KVCacheGroupSpec(
                ["layers.1.attn", "layers.3.attn"], FullAttentionSpec(**_attn_kw())
            ),
        ],
        num_blocks=NUM_BLOCKS,
    )


def _shared(positions=3):
    """Every layer's block slots in one buffer, as vLLM allocates them: the
    layer at position p of each group starts at p x blocks x slot, so the
    groups overlap. Each byte holds a value derived from its offset."""
    return (
        torch.arange(positions * TINY_LAYER_BYTES, dtype=torch.int64)
        .remainder(251)
        .to(torch.uint8)
    )


def _region(shared, position):
    return shared[position * TINY_LAYER_BYTES : (position + 1) * TINY_LAYER_BYTES]


def _gdn_tensor(region, version):
    """A GDN layer as vLLM registers it: 0.23 a state list, 0.30 one int8 view."""
    if version == "0.30":
        return torch.as_strided(
            region.view(torch.int8),
            (TINY_BLOCKS, 1, 1, TINY_STATE),
            (TINY_SLOT, TINY_STATE, TINY_STATE, 1),
        )
    conv = torch.as_strided(
        region.view(torch.bfloat16), (TINY_BLOCKS, 3, 16), (TINY_SLOT // 2, 16, 1)
    )
    as_f32 = region.view(torch.float32)
    ssm = torch.as_strided(
        as_f32,
        (TINY_BLOCKS, 2, 4, 4),
        (TINY_SLOT // 4, 16, 4, 1),
        as_f32.storage_offset() + 96 // 4,
    )
    return [conv, ssm]


def _attn_tensor(region, version, kernel_block=TINY_BLOCK):
    """An attention layer: 0.23 flash (NB, 2, BS, NH, HS); 0.30 fused K/V
    (NB, NH, BS, 2 HS), split into kernel blocks when ``kernel_block`` is
    smaller than a block."""
    if version == "0.30":
        split = TINY_BLOCK // kernel_block
        return torch.as_strided(
            region.view(torch.bfloat16),
            (TINY_BLOCKS * split, 2, kernel_block, 16),
            (TINY_SLOT // 2 // split, 16, 32, 1),
        )
    return region.view(torch.bfloat16).view(TINY_BLOCKS, 2, 16, 2, 8)


def tiny_kv_caches(version, shared, gdn=TINY_GDN, attn=TINY_ATTN):
    caches = {
        f"layers.{i}.linear_attn": _gdn_tensor(_region(shared, p), version)
        for layers in gdn
        for p, i in enumerate(layers)
    }
    caches |= {
        f"layers.{i}.attn": _attn_tensor(_region(shared, p), version)
        for p, i in enumerate(attn)
    }
    return caches


def _engine_layout(kv_caches):
    """The KV cache layout vLLM reports with these tensors: "NHD" from 0.23
    (flash, rank 5), the resolved "LBNHC" from 0.30 (logical page, rank 4)."""
    attn = [t for name, t in kv_caches.items() if name.endswith(".attn")]
    if not attn:
        return None
    return "NHD" if attn[0].dim() == 5 else "LBNHC"


def _worker(config=None, chunk=TINY_BLOCK, **kwargs):
    kwargs.setdefault("num_kv_heads", 2)
    kwargs.setdefault("head_size", 8)
    worker = make_worker(
        TINY_BLOCK,
        chunk,
        kv_groups=describe_kv_groups(config or tiny_config()),
        **kwargs,
    )
    worker._ensure_handler = MagicMock()
    return worker


def _registered_worker(kv_caches, config=None, chunk=TINY_BLOCK):
    worker = _worker(config, chunk, kv_cache_layout=_engine_layout(kv_caches))
    worker.register_kv_caches(kv_caches)
    return worker


@pytest.mark.parametrize("version", ["0.23", "0.30"])
def test_worker_maps_each_vllm_format_onto_the_groups(version):
    shared = _shared()
    kv_caches = tiny_kv_caches(version, shared)
    worker = _registered_worker(kv_caches)
    caches = worker._group_caches
    assert [c.group.kind for c in caches] == [GROUP_MAMBA] * 3 + [GROUP_FULL]
    # Layers keep the group's order: index = position in the object.
    assert caches[3].group.layer_names == ("layers.3.attn", "layers.7.attn")
    assert caches[3].tensors[1] is kv_caches["layers.7.attn"]
    # Each block's state reads as the first 224 B of its slot, at the layer's
    # offset in the shared buffer.
    for p, view in enumerate(caches[1].tensors):
        region = _region(shared, p)
        assert view.shape == (TINY_BLOCKS, TINY_STATE)
        for b in range(TINY_BLOCKS):
            slot = region[b * TINY_SLOT : b * TINY_SLOT + TINY_STATE]
            assert torch.equal(view[b], slot)
    # 0.23's flash layout has a kernel format; 0.30's page has none, and its
    # K|V halves are read as a K/V axis.
    layout = caches[3].layout
    assert (layout.format_name is not None) == (version == "0.23")
    assert layout.kv_packed == (version == "0.30")
    assert (layout.num_blocks, layout.block_size) == (TINY_BLOCKS, TINY_BLOCK)
    # Two layers x one 1,024 B slot, for every group.
    assert worker._page_size_bytes == 2 * TINY_SLOT
    worker._ensure_handler.assert_called_once()


@pytest.mark.parametrize("version", ["0.23", "0.30"])
def test_state_view_writes_reach_the_gpu_state(version):
    """Load will write through the view, so it must alias vLLM's tensor."""
    shared = _shared()
    kv = _gdn_tensor(_region(shared, 1), version)
    view = state_view("x", kv, TINY_SLOT, TINY_STATE)
    view[4].fill_(0)
    parts = kv if isinstance(kv, list) else [kv]
    assert all(not t[4].any() for t in parts)
    assert all(t[3].any() for t in parts)
    # Only the state was written: the slot's padding and layer 0 are intact.
    assert _region(shared, 1)[4 * TINY_SLOT + TINY_STATE :][:8].any()
    assert torch.equal(_region(shared, 0), _region(_shared(), 0))


def test_page_is_the_largest_group_object():
    """GDN groups of 2, 3 and 2 layers: the page fits the middle one."""
    gdn = [[0, 4], [1, 5, 8], [2, 6]]
    shared = _shared()
    worker = _registered_worker(tiny_kv_caches("0.30", shared, gdn), tiny_config(gdn))
    assert [len(c.tensors) for c in worker._group_caches] == [2, 3, 2, 2]
    assert worker._page_size_bytes == 3 * TINY_SLOT


def test_attention_object_spans_the_chunk():
    shared = _shared()
    kv_caches = {
        f"layers.{i}.attn": _attn_tensor(_region(shared, i // 2), "0.30")
        for i in range(4)
    }
    worker = _registered_worker(kv_caches, tiny_swa_config(), chunk=4 * TINY_BLOCK)
    assert [c.group.kind for c in worker._group_caches] == [
        GROUP_SLIDING_WINDOW,
        GROUP_FULL,
    ]
    # Two layers x four blocks of 1,024 B.
    assert worker._page_size_bytes == 2 * 4 * TINY_SLOT


def test_split_kernel_blocks_are_refused():
    shared = _shared()
    kv_caches = tiny_kv_caches("0.30", shared)
    for p, i in enumerate(TINY_ATTN):
        kv_caches[f"layers.{i}.attn"] = _attn_tensor(
            _region(shared, p), "0.30", kernel_block=8
        )
    with pytest.raises(ValueError, match="16-token blocks, laid out as 8-token"):
        _registered_worker(kv_caches)


def test_attention_layout_needs_the_head_count():
    worker = _worker(num_kv_heads=None)
    with pytest.raises(ValueError, match="KV head count"):
        worker.register_kv_caches(tiny_kv_caches("0.30", _shared()))


def test_state_must_sit_contiguously_at_the_slot_start():
    region = _region(_shared(), 0)
    conv, _ = _gdn_tensor(region, "0.23")
    as_f32 = region.view(torch.float32)
    gap = torch.as_strided(as_f32, (TINY_BLOCKS, 2, 4, 4), (256, 16, 4, 1), 32)
    with pytest.raises(ValueError, match="contiguous state"):
        state_view("x", [conv, gap], TINY_SLOT, TINY_STATE)
    with pytest.raises(ValueError, match="contiguous state"):
        state_view("x", _gdn_tensor(region, "0.30"), 2 * TINY_SLOT, TINY_STATE)


def test_state_must_be_the_whole_spec_state():
    conv, _ = _gdn_tensor(_region(_shared(), 0), "0.23")
    with pytest.raises(ValueError, match="registers 96 B of Mamba state"):
        state_view("x", [conv], TINY_SLOT, TINY_STATE)


def test_registration_refuses_a_missing_layer():
    kv_caches = tiny_kv_caches("0.30", _shared())
    del kv_caches["layers.6.linear_attn"]
    with pytest.raises(ValueError, match="no registered tensor"):
        _registered_worker(kv_caches)


def test_attention_layers_must_match_in_dtype():
    kv_caches = tiny_kv_caches("0.23", _shared())
    kv_caches["layers.7.attn"] = kv_caches["layers.7.attn"].view(torch.float16)
    with pytest.raises(ValueError, match="differ in shape, dtype or strides"):
        _registered_worker(kv_caches)


def test_attention_block_may_not_exceed_the_spec():
    kv_caches = tiny_kv_caches("0.23", _shared())
    for i in TINY_ATTN:
        kv_caches[f"layers.{i}.attn"] = torch.zeros(
            TINY_BLOCKS, 2, 16, 2, 16, dtype=torch.bfloat16
        )
    with pytest.raises(ValueError, match="2048 B on the GPU, more than"):
        _registered_worker(kv_caches)


def test_a_padded_attention_page_is_accepted():
    """vLLM may pad an attention page; the KV then fills only part of it."""
    kv_caches = tiny_kv_caches("0.23", _shared())
    for i in TINY_ATTN:
        kv_caches[f"layers.{i}.attn"] = torch.zeros(
            TINY_BLOCKS, 2, 16, 2, 4, dtype=torch.bfloat16
        )
    worker = _registered_worker(kv_caches)
    assert worker._page_size_bytes == 2 * TINY_SLOT


def test_explicit_page_size_must_hold_the_largest_object():
    worker = make_worker(
        TINY_BLOCK,
        TINY_BLOCK,
        {"maru_chunk_size": TINY_SLOT},
        num_kv_heads=2,
        head_size=8,
        kv_groups=describe_kv_groups(tiny_config()),
    )
    worker._ensure_handler = MagicMock()
    with pytest.raises(ValueError, match="cannot hold one"):
        worker.register_kv_caches(tiny_kv_caches("0.30", _shared()))


# =============================================================================
# Stores
# =============================================================================


def _store_meta(tokens, group_block_ids, computed, scheduled, req_ids=("r1",)):
    return MaruConnectorMetadata(
        requests=[
            MaruReqMeta(
                req_id=rid,
                token_ids=tokens,
                block_ids=group_block_ids[0],
                is_store=True,
                num_scheduled_tokens=scheduled,
                num_computed_tokens=computed,
                group_block_ids=group_block_ids,
            )
            for rid in req_ids
        ]
    )


def _token_rows(kv, block, version):
    """One block's KV as ``[2, block_size, row]``, the order Maru stores.

    0.23's flash layout has a K/V axis. 0.30's page ``[B, H, N, K|V]`` holds
    each head's K and V as the halves of its last axis, which Maru splits.
    """
    if version == "0.30":
        heads = kv[block].unflatten(-1, (2, -1))  # [H, N, 2, D]
        return heads.permute(2, 1, 0, 3).reshape(2, TINY_BLOCK, -1)
    return kv[block].reshape(2, TINY_BLOCK, -1)


def _expected_slab(tensors, blocks, version):
    """KV_2LTD slab ``[2, layers, tokens, row]`` of ``blocks`` for each layer."""
    return torch.stack(
        [
            torch.stack(
                [
                    torch.cat([_token_rows(kv, b, version)[plane] for b in blocks])
                    for kv in tensors
                ]
            )
            for plane in range(2)
        ]
    )


def _slab(raw, like):
    return torch.frombuffer(bytearray(raw), dtype=torch.bfloat16).view(like.shape)


def _storing_worker(kv_caches, config=None, chunk=TINY_BLOCK, extra=None):
    worker = make_worker(
        TINY_BLOCK,
        chunk,
        extra,
        num_kv_heads=2,
        head_size=8,
        kv_cache_layout=_engine_layout(kv_caches),
        kv_groups=describe_kv_groups(config or tiny_config()),
    )
    worker._ensure_handler = MagicMock()
    worker.register_kv_caches(kv_caches)
    store = attach_capturing_handler(worker)
    return worker, store


# GDN groups 0-2 hold a state only at block position 1; attention uses 4, 5.
QWEN_BLOCKS = ([0, 1], [0, 2], [0, 3], [4, 5])


@pytest.mark.parametrize("version", ["0.23", "0.30"])
def test_a_step_ending_on_a_chunk_stores_every_group(version):
    shared = _shared()
    worker, store = _storing_worker(tiny_kv_caches(version, shared))
    tokens = list(range(40))
    worker.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32))

    k0, k1 = _chunk_keys(tokens, TINY_BLOCK)
    # Attention: both chunks the step completed. Mamba: only the chunk the
    # step ended on; chunk 0's state was overwritten within the step.
    assert set(store) == {
        group_object_key(k0, 3),
        group_object_key(k1, 3),
        *(group_object_key(k1, g) for g in range(3)),
    }
    attn = worker._group_caches[3].tensors
    for key, block in ((k0, 4), (k1, 5)):
        expected = _expected_slab(attn, [block], version)
        assert torch.equal(_slab(store[group_object_key(key, 3)], expected), expected)
    # A state object holds each layer's state at a stride of one block slot.
    for g in range(3):
        obj = store[group_object_key(k1, g)]
        assert len(obj) == 2 * TINY_SLOT
        block = QWEN_BLOCKS[g][1]
        for p in range(2):
            state = _region(shared, p)[block * TINY_SLOT :][:TINY_STATE]
            assert obj[p * TINY_SLOT :][:TINY_STATE] == bytes(state)
    # Everything stored in the step is registered in one RPC.
    assert worker._handler.batch_store.call_count == 1


def test_a_step_ending_inside_a_chunk_stores_no_state():
    worker, store = _storing_worker(tiny_kv_caches("0.30", _shared()))
    tokens = list(range(48))
    worker.wait_for_save(
        _store_meta(tokens, ([0, 0, 1], [0, 0, 2], [0, 0, 3], [4, 5, 6]), 0, 40)
    )
    keys = _chunk_keys(tokens, TINY_BLOCK)
    assert set(store) == {group_object_key(k, 3) for k in keys[:2]}


def test_a_null_state_block_stores_no_state():
    worker, store = _storing_worker(tiny_kv_caches("0.30", _shared()))
    tokens = list(range(40))
    worker.wait_for_save(_store_meta(tokens, ([0, 0], [0, 2], [0, 3], [4, 5]), 0, 32))
    k1 = _chunk_keys(tokens, TINY_BLOCK)[1]
    assert group_object_key(k1, 0) not in store
    assert group_object_key(k1, 1) in store


def test_shared_prefix_is_stored_once_per_key():
    worker, store = _storing_worker(tiny_kv_caches("0.30", _shared()))
    tokens = list(range(40))
    worker.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32, ("r1", "r2")))
    (keys, _), _ = worker._handler.batch_store.call_args
    assert len(keys) == len(set(keys)) == 5
    assert len(store) == 5


def test_sliding_window_reads_the_null_block_behind_the_window():
    """Window 32, chunk 64: when the step starting at token 48 began, vLLM
    freed block 0 (tokens 0-15), whose id may now be another request's."""
    shared = _shared()
    kv_caches = {
        f"layers.{i}.attn": _attn_tensor(_region(shared, i // 2), "0.30")
        for i in range(4)
    }
    worker, store = _storing_worker(kv_caches, tiny_swa_config(), chunk=64)
    tokens = list(range(80))
    blocks = ([7, 8, 9, 10], [3, 4, 5, 6])
    worker.wait_for_save(_store_meta(tokens, blocks, 48, 16))

    (k0,) = _chunk_keys(tokens, 64)
    swa, full = (c.tensors for c in worker._group_caches)
    expected = _expected_slab(swa, [0, 8, 9, 10], "0.30")
    assert torch.equal(_slab(store[group_object_key(k0, 0)], expected), expected)
    expected = _expected_slab(full, [3, 4, 5, 6], "0.30")
    assert torch.equal(_slab(store[group_object_key(k0, 1)], expected), expected)


def test_without_the_kernel_a_write_behind_store_is_synchronous():
    worker, store = _storing_worker(
        tiny_kv_caches("0.23", _shared()), extra={"maru_async_store": True}
    )
    tokens = list(range(40))
    worker.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32))
    assert len(store) == 5


def test_save_kv_layer_leaves_several_groups_to_wait_for_save():
    worker, store = _storing_worker(tiny_kv_caches("0.30", _shared()))
    kv = worker._group_caches[3].tensors[0]
    worker.save_kv_layer(
        "layers.3.attn", kv, None, _store_meta(list(range(40)), QWEN_BLOCKS, 0, 32)
    )
    worker._handler.alloc.assert_not_called()
    assert store == {}


def test_connector_wait_for_save_hands_several_groups_to_the_worker():
    connector = MaruKVConnector.__new__(MaruKVConnector)
    connector._kv_groups = describe_kv_groups(tiny_config())
    connector._worker = MagicMock()
    meta = MaruConnectorMetadata()
    connector._connector_metadata = meta
    connector.wait_for_save()
    connector._worker.wait_for_save.assert_called_once_with(meta)

    connector._kv_groups = connector._kv_groups[3:]
    connector._worker.reset_mock()
    connector.wait_for_save()
    connector._worker.wait_for_save.assert_not_called()


def test_a_later_step_stores_the_state_at_its_own_end():
    """Tokens 32-47 of a 56-token prompt: the state is at block position 2."""
    worker, store = _storing_worker(tiny_kv_caches("0.30", _shared()))
    tokens = list(range(56))
    blocks = ([0, 0, 1], [0, 0, 2], [0, 0, 3], [4, 5, 6])
    worker.wait_for_save(_store_meta(tokens, blocks, 32, 16))
    k2 = _chunk_keys(tokens, TINY_BLOCK)[2]
    assert set(store) == {group_object_key(k2, g) for g in range(4)}
    shared = _shared()
    obj = store[group_object_key(k2, 1)]
    assert obj[:TINY_STATE] == bytes(_region(shared, 0)[2 * TINY_SLOT :][:TINY_STATE])


def test_a_step_past_the_last_whole_chunk_stores_nothing():
    """The step ends at 48, past the prompt's two whole chunks."""
    worker, store = _storing_worker(tiny_kv_caches("0.30", _shared()))
    blocks = ([0, 0, 1], [0, 0, 2], [0, 0, 3], [4, 5, 6])
    worker.wait_for_save(_store_meta(list(range(40)), blocks, 32, 16))
    assert store == {}
    worker._handler.alloc.assert_not_called()


def test_a_failing_group_keeps_the_others_and_leaks_nothing():
    """r2 lacks the attention group's block ids: its attention store fails
    after r1's was gathered. r1's slab and both requests' states register."""
    worker, store = _storing_worker(tiny_kv_caches("0.30", _shared()))
    a, b = list(range(40)), list(range(1000, 1040))
    meta = MaruConnectorMetadata(
        requests=[
            *_store_meta(a, QWEN_BLOCKS, 0, 32).requests,
            *_store_meta(b, ([0, 6], [0, 7], [0, 8]), 0, 32, ("r2",)).requests,
        ]
    )
    worker.wait_for_save(meta)
    ka, kb = _chunk_keys(a, TINY_BLOCK), _chunk_keys(b, TINY_BLOCK)
    assert set(store) == {
        *(group_object_key(k, 3) for k in ka),
        *(group_object_key(k, g) for k in (ka[1], kb[1]) for g in range(3)),
    }
    assert worker._handler.batch_store.call_count == 1
    assert worker._handler.alloc.call_count == len(store)


def test_padded_pages_store_without_the_kernel():
    """vLLM pads a page with an as_strided view; the kernel, which knows only
    the base pointer, would read the padding as KV."""
    kv_caches = tiny_kv_caches("0.23", _shared())
    padded = _shared()
    for p, i in enumerate(TINY_ATTN):
        kv_caches[f"layers.{i}.attn"] = torch.as_strided(
            _region(padded, p).view(torch.bfloat16),
            (TINY_BLOCKS, 2, 16, 2, 4),
            (TINY_SLOT // 2, 128, 8, 4, 1),
        )
    worker = _worker()
    worker.register_kv_caches(kv_caches)
    worker._packed_load_kernel_ctx = MagicMock(return_value=("ops",) * 6)
    assert worker._build_group_kernel(worker._group_caches[3], None) is None
    worker._packed_load_kernel_ctx.assert_not_called()

    dense = _worker()
    dense.register_kv_caches(tiny_kv_caches("0.23", _shared()))
    dense._packed_load_kernel_ctx = MagicMock(return_value=("ops",) * 6)
    assert dense._build_group_kernel(dense._group_caches[3], None) == ("ops",) * 6


def _kernel_worker(extra=None):
    """A worker on CUDA whose attention group stores through maru_kv_ops."""
    from maru_vllm.connector import _resolve_kv_ops

    if not torch.cuda.is_available() or _resolve_kv_ops() is None:
        pytest.skip("needs CUDA and the maru_kv_ops kernels")
    shared = _shared().cuda()
    worker, store = _storing_worker(tiny_kv_caches("0.23", shared), extra=extra)
    worker._last_attn_metadata = make_flash_attn_metadata()
    return worker, store, shared


def _check_qwen_objects(worker, store, shared, tokens):
    k0, k1 = _chunk_keys(tokens, TINY_BLOCK)
    attn = [t.cpu() for t in worker._group_caches[3].tensors]
    for key, block in ((k0, 4), (k1, 5)):
        expected = _expected_slab(attn, [block], "0.23")
        assert torch.equal(_slab(store[group_object_key(key, 3)], expected), expected)
    for g in range(3):
        obj = store[group_object_key(k1, g)]
        block = QWEN_BLOCKS[g][1]
        for p in range(2):
            state = _region(shared, p)[block * TINY_SLOT :][:TINY_STATE].cpu()
            assert obj[p * TINY_SLOT :][:TINY_STATE] == bytes(state)


def test_kernel_store_uses_the_group_pointer_table():
    worker, store, shared = _kernel_worker()
    tokens = list(range(40))
    worker.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32))
    assert worker._group_kernels[3] is not None
    assert len(store) == 5
    _check_qwen_objects(worker, store, shared, tokens)


def test_kernel_store_registers_no_group_object_whose_copy_failed():
    """The attention group's slabs, whose copies could not be confirmed, are
    freed instead of registered; the Mamba states, copied synchronously,
    still are."""
    worker, store, shared = _kernel_worker()
    worker._sync_store_stream = FailingSyncStream(torch.cuda.Stream(shared.device))
    worker._sync_store_stream_device = shared.device
    tokens = list(range(40))
    worker.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32))
    k1 = _chunk_keys(tokens, TINY_BLOCK)[1]
    assert set(store) == {group_object_key(k1, g) for g in range(3)}
    assert worker._handler.free.call_count == 2  # the two attention slabs


def test_kernel_write_behind_stores_attention_later_and_states_now():
    worker, store, shared = _kernel_worker({"maru_async_store": True})
    tokens = list(range(40))
    worker.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32))
    k1 = _chunk_keys(tokens, TINY_BLOCK)[1]
    assert all(group_object_key(k1, g) in store for g in range(3))
    worker._store_executor.shutdown(wait=True)
    assert len(store) == 5
    _check_qwen_objects(worker, store, shared, tokens)


def test_kernel_store_reads_the_null_block_behind_the_window():
    from maru_vllm.connector import _resolve_kv_ops

    if not torch.cuda.is_available() or _resolve_kv_ops() is None:
        pytest.skip("needs CUDA and the maru_kv_ops kernels")
    shared = _shared().cuda()
    kv_caches = {
        f"layers.{i}.attn": _attn_tensor(_region(shared, i // 2), "0.23")
        for i in range(4)
    }
    worker, store = _storing_worker(kv_caches, tiny_swa_config(), chunk=64)
    worker._last_attn_metadata = make_flash_attn_metadata()
    tokens = list(range(80))
    worker.wait_for_save(_store_meta(tokens, ([7, 8, 9, 10], [3, 4, 5, 6]), 48, 16))
    assert worker._group_kernels[0] is not None

    (k0,) = _chunk_keys(tokens, 64)
    swa = [t.cpu() for t in worker._group_caches[0].tensors]
    expected = _expected_slab(swa, [0, 8, 9, 10], "0.23")
    assert torch.equal(_slab(store[group_object_key(k0, 0)], expected), expected)


class _HeldExecutor:
    """Holds submitted write-behind completions until the test runs them."""

    def __init__(self):
        self.jobs = []

    def submit(self, fn, *args):
        self.jobs.append((fn, args))

    def run(self):
        for fn, args in self.jobs:
            fn(*args)


def test_a_request_is_held_until_its_group_objects_are_registered():
    worker, store, _ = _kernel_worker({"maru_async_store": True})
    worker._store_executor = _HeldExecutor()
    tokens = list(range(40))
    worker.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32))
    # The states are in; the attention objects are still being written.
    assert len(store) == 3
    assert worker.get_finished_saving({"r1"}) is None
    worker._store_executor.run()
    assert len(store) == 5
    assert worker.get_finished_saving(set()) == {"r1"}


# =============================================================================
# Loads
# =============================================================================


def _serve(worker, store):
    """Let ``worker`` retrieve the objects ``store`` holds."""
    worker._handler.batch_retrieve.side_effect = lambda keys: [
        SimpleNamespace(view=memoryview(bytearray(store[k]))) if k in store else None
        for k in keys
    ]


def _load_meta(tokens, group_block_ids, hit, computed=0, scheduled=0, **kwargs):
    return MaruConnectorMetadata(
        requests=[
            MaruReqMeta(
                req_id="b",
                token_ids=tokens,
                block_ids=group_block_ids[0],
                is_store=False,
                num_matched_chunks=hit,
                num_computed_tokens=computed,
                num_scheduled_tokens=scheduled,
                group_block_ids=group_block_ids,
                **kwargs,
            )
        ]
    )


_FORWARD = SimpleNamespace(attn_metadata=None, no_compile_layers={})


def _stored_qwen(version="0.30"):
    """Request A's objects: 40 tokens, its first step ending at token 32."""
    source = _shared()
    worker, store = _storing_worker(tiny_kv_caches(version, source))
    worker.wait_for_save(_store_meta(list(range(40)), QWEN_BLOCKS, 0, 32))
    return source, worker, store


# B's blocks: GDN group g holds 2g+1 where the hit ends and 2g+2 where the step
# ends; attention holds 7, 8, 9.
B_BLOCKS = ([0, 1, 2], [0, 3, 4], [0, 5, 6], [7, 8, 9])


def _loading_worker(store, version="0.30", config=None, chunk=TINY_BLOCK):
    target = torch.zeros_like(_shared())
    if config is None:
        kv_caches = tiny_kv_caches(version, target)
    else:
        kv_caches = {
            f"layers.{i}.attn": _attn_tensor(_region(target, i // 2), version)
            for i in range(4)
        }
    worker, _ = _storing_worker(kv_caches, config, chunk)
    _serve(worker, store)
    return worker, target


def _state(shared, layer, block):
    return _region(shared, layer)[block * TINY_SLOT :][:TINY_STATE]


@pytest.mark.parametrize("version", ["0.23", "0.30"])
def test_an_inline_load_restores_every_group(version):
    source, stored, store = _stored_qwen(version)
    worker, target = _loading_worker(store, version)
    # B: same 40 tokens, hit 2 chunks (32 tokens), computes 32-39 this step.
    worker.start_load_kv(_FORWARD, _load_meta(list(range(40)), B_BLOCKS, 2, 32, 8))

    src = stored._group_caches[3].tensors
    dst = worker._group_caches[3].tensors
    for a_block, b_block in ((4, 7), (5, 8)):
        for a, b in zip(src, dst, strict=True):
            assert torch.equal(
                _token_rows(b, b_block, version), _token_rows(a, a_block, version)
            )
    # The state goes where the hit ends and where this step ends.
    for g in range(3):
        a_block = QWEN_BLOCKS[g][1]
        for b_block in B_BLOCKS[g][1:]:
            for layer in range(2):
                assert torch.equal(
                    _state(target, layer, b_block), _state(source, layer, a_block)
                )
    assert worker.take_failed_load_blocks() == set()


def test_a_parked_load_writes_the_state_only_where_the_hit_ends():
    """vLLM copies it on to the step's block when the request resumes."""
    source, _, store = _stored_qwen()
    worker, target = _loading_worker(store)
    meta = _load_meta(list(range(40)), B_BLOCKS, 2, deferred_load=True)
    worker.start_load_kv(_FORWARD, meta)  # no CUDA here: loads inline
    for g in range(3):
        assert torch.equal(
            _state(target, 0, B_BLOCKS[g][1]), _state(source, 0, QWEN_BLOCKS[g][1])
        )
        assert not _state(target, 0, B_BLOCKS[g][2]).any()
    assert worker.get_finished_loading() == {"b"}


def test_a_sliding_window_load_writes_only_the_window():
    """Window 32 at token 64: vLLM gave real blocks for tokens 32-63 only."""
    source = _shared()
    kv_caches = {
        f"layers.{i}.attn": _attn_tensor(_region(source, i // 2), "0.30")
        for i in range(4)
    }
    stored, store = _storing_worker(kv_caches, tiny_swa_config(), chunk=64)
    tokens = list(range(80))
    stored.wait_for_save(_store_meta(tokens, ([7, 8, 9, 10], [3, 4, 5, 6]), 0, 64))

    worker, target = _loading_worker(store, config=tiny_swa_config(), chunk=64)
    blocks = ([0, 0, 1, 2], [3, 4, 5, 6])
    worker.start_load_kv(_FORWARD, _load_meta(tokens, blocks, 1, 64, 16))
    swa_src = stored._group_caches[0].tensors
    swa_dst = worker._group_caches[0].tensors
    for a_block, b_block in ((9, 1), (10, 2)):
        for a, b in zip(swa_src, swa_dst, strict=True):
            assert torch.equal(
                _token_rows(b, b_block, "0.30"), _token_rows(a, a_block, "0.30")
            )
    # The null block is left alone.
    assert not _token_rows(swa_dst[0], 0, "0.30").any()


def _reported_failures(worker):
    """What one report tells vLLM of failed loads: the failed blocks, the
    parked loads that finished, and the failed ones among them."""
    finished = worker.get_finished_loading() or set()
    return (
        worker.take_failed_load_blocks(),
        finished,
        worker.take_failed_recv_req_ids(finished),
    )


# Every group's real blocks over tokens 0-31: the states where the hit ends
# (1, 3, 5) and attention 7, 8. Not the null block, nor the blocks the step
# computes (2, 4, 6, 9).
FAILED_INLINE = ({1, 3, 5, 7, 8}, set(), set())
# vLLM 0.32 stops the engine on a block-level failure with several groups,
# whose block ids are unique only within a group: a parked load is reported
# by its request, with its completion.
FAILED_PARKED = (set(), {"b"}, {"b"})


def test_a_failed_parked_load_is_reported_by_its_request():
    _, _, store = _stored_qwen()
    k1 = _chunk_keys(list(range(40)), TINY_BLOCK)[1]
    del store[group_object_key(k1, 2)]
    worker, target = _loading_worker(store)
    meta = _load_meta(list(range(40)), B_BLOCKS, 2, deferred_load=True)
    worker.start_load_kv(_FORWARD, meta)
    assert _reported_failures(worker) == FAILED_PARKED
    assert _reported_failures(worker) == (set(), set(), set())
    assert not target.any()


def test_connector_reports_failed_parked_loads_with_their_completion():
    if not hasattr(KVConnectorBase_V1, "get_transfer_results"):
        pytest.skip("this vLLM reports load failures by block only")
    _, _, store = _stored_qwen()
    del store[group_object_key(_chunk_keys(list(range(40)), TINY_BLOCK)[1], 2)]
    worker, _ = _loading_worker(store)
    connector = MaruKVConnector.__new__(MaruKVConnector)
    connector._worker = worker
    meta = _load_meta(list(range(40)), B_BLOCKS, 2, deferred_load=True)
    worker.start_load_kv(_FORWARD, meta)
    results = connector.get_transfer_results(set())
    assert results.finished_recving == {"b"}
    assert results.failed_recving == {"b"}
    assert connector.get_block_ids_with_load_errors() == set()


def test_a_load_skips_what_vllms_prefix_cache_holds():
    """vLLM already holds tokens 0-15 (block 7, maybe shared with running
    requests): Maru neither loads them nor reports them when it fails."""
    source, stored, store = _stored_qwen()
    worker, target = _loading_worker(store)
    meta = _load_meta(list(range(40)), B_BLOCKS, 2, 32, 8, load_start_token=16)
    worker.start_load_kv(_FORWARD, meta)
    (keys,), _ = worker._handler.batch_retrieve.call_args
    k0, k1 = _chunk_keys(list(range(40)), TINY_BLOCK)
    assert group_object_key(k0, 3) not in keys
    dst = worker._group_caches[3].tensors
    assert not _token_rows(dst[0], 7, "0.30").any()
    src = stored._group_caches[3].tensors
    assert torch.equal(_token_rows(dst[0], 8, "0.30"), _token_rows(src[0], 5, "0.30"))

    del store[group_object_key(k1, 3)]
    worker.start_load_kv(_FORWARD, meta)
    assert worker.take_failed_load_blocks() == {1, 3, 5, 8}


def test_loads_of_several_groups_carry_where_vllms_cache_ends():
    a = list(range(96))
    sched = _two_group_scheduler({"maru_async_load": True})
    sched._handler = _Store(_stored_by(a, 32, [0, 1], [], []))
    request = SimpleNamespace(request_id="r1", prompt_token_ids=a + _other(4))
    # vLLM's prefix cache holds 32 tokens; Maru adds 64.
    assert sched.get_num_new_matched_tokens(request, 32) == (64, True)
    blocks = MagicMock()
    blocks.get_block_ids.return_value = ([1, 2, 3, 4, 5, 6], [11, 12, 13, 14, 15, 16])
    sched.update_state_after_alloc(request, blocks, 64)
    req = sched.build_connector_meta(fake_scheduler_output()).requests[0]
    assert (req.load_start_token, req.load_end_token) == (32, 96)


def test_nothing_to_load_unparks_the_request():
    worker, _ = _loading_worker({})
    meta = _load_meta(list(range(40)), B_BLOCKS, 0, deferred_load=True)
    worker.start_load_kv(_FORWARD, meta)
    assert worker.get_finished_loading() == {"b"}
    worker._handler.batch_retrieve.assert_not_called()


def test_inline_loads_of_several_groups_carry_their_step():
    sched = _two_group_scheduler()
    request = SimpleNamespace(request_id="r1", prompt_token_ids=list(range(64)))
    sched.update_state_after_alloc(request, MagicMock(), 32)
    new_req = _new_req(([1, 2, 3, 4], [11, 12, 13, 14]))
    new_req.num_computed_tokens = 32
    req, _ = _load_and_store(sched, 32, new_reqs=[new_req])
    assert (req.num_computed_tokens, req.num_scheduled_tokens) == (32, 32)

    single = make_scheduler(16, 32)
    single.update_state_after_alloc(request, MagicMock(), 32)
    req, _ = _load_and_store(single, 32, new_reqs=[new_req])
    assert (req.num_computed_tokens, req.num_scheduled_tokens) == (0, 0)


def test_several_groups_load_each_request_whole():
    extra = {"maru_async_load": True, "maru_overlap_load_with_compute": True}
    assert not _two_group_scheduler(extra)._layerwise_overlap
    assert make_scheduler(16, 32, extra)._layerwise_overlap


def _cuda_loading_worker(store):
    from maru_vllm.connector import _resolve_kv_ops

    if not torch.cuda.is_available() or _resolve_kv_ops() is None:
        pytest.skip("needs CUDA and the maru_kv_ops kernels")
    target = torch.zeros_like(_shared()).cuda()
    worker, _ = _storing_worker(tiny_kv_caches("0.23", target))
    worker._last_attn_metadata = make_flash_attn_metadata()
    _serve(worker, store)
    return worker, target


def _check_loaded(stored, worker, source, target, in_place):
    src = [t.cpu() for t in stored._group_caches[3].tensors]
    dst = [t.cpu() for t in worker._group_caches[3].tensors]
    for a_block, b_block in ((4, 7), (5, 8)):
        for a, b in zip(src, dst, strict=True):
            assert torch.equal(
                _token_rows(b, b_block, "0.23"), _token_rows(a, a_block, "0.23")
            )
    for g in range(3):
        b_blocks = B_BLOCKS[g][1:] if in_place else B_BLOCKS[g][1:2]
        for b_block in b_blocks:
            for layer in range(2):
                assert torch.equal(
                    _state(target, layer, b_block).cpu(),
                    _state(source, layer, QWEN_BLOCKS[g][1]).cpu(),
                )


def test_kernel_load_restores_every_group():
    stored, store, source = _kernel_worker()
    stored.wait_for_save(_store_meta(list(range(40)), QWEN_BLOCKS, 0, 32))
    worker, target = _cuda_loading_worker(store)
    worker.start_load_kv(_FORWARD, _load_meta(list(range(40)), B_BLOCKS, 2, 32, 8))
    assert worker._group_kernels[3] is not None
    assert worker.take_failed_load_blocks() == set()
    _check_loaded(stored, worker, source, target, in_place=True)


def test_a_parked_load_runs_on_the_loader_thread():
    stored, store, source = _kernel_worker()
    stored.wait_for_save(_store_meta(list(range(40)), QWEN_BLOCKS, 0, 32))
    worker, target = _cuda_loading_worker(store)
    meta = _load_meta(list(range(40)), B_BLOCKS, 2, deferred_load=True)
    worker.start_load_kv(_FORWARD, meta)
    worker._deferred_executor.shutdown(wait=True)
    torch.cuda.synchronize()
    assert worker.get_finished_loading() == {"b"}
    assert worker.take_failed_load_blocks() == set()
    _check_loaded(stored, worker, source, target, in_place=False)


# =============================================================================
# vLLM's own scheduler
# =============================================================================


def test_vllm_scheduler_hands_every_group_to_the_connector(tmp_path):
    """Sliding window 32 + full attention, block 16, chunk 32, on vLLM's own
    scheduler: the hit, vLLM's block allocation and the finish callback."""
    if not supports_hma(MaruKVConnector):
        pytest.skip("Maru takes several KV cache groups from vLLM 0.30")
    from vllm.sampling_params import SamplingParams
    from vllm.v1.request import Request, RequestStatus

    kw = {"block_size": 16, "num_kv_heads": 1, "head_size": 1, "dtype": torch.float32}
    engine = make_vllm_scheduler(
        tmp_path,
        16,
        32,
        num_blocks=64,
        kv_cache_groups=[
            KVCacheGroupSpec(["swa"], SlidingWindowSpec(sliding_window=32, **kw)),
            KVCacheGroupSpec(["full"], FullAttentionSpec(**kw)),
        ],
        kv_load_failure_policy="fail",
    )
    tokens = list(range(100))
    connector = engine.connector
    connector._scheduler._handler = _Store(_stored_by(tokens, 32, [0, 1], [], []))
    finished = MagicMock(wraps=connector.request_finished_all_groups)
    connector.request_finished_all_groups = finished

    engine.add_request(Request("b", tokens, SamplingParams(max_tokens=1), None))
    output = engine.schedule()
    # 3 whole chunks short of the last token: 96 tokens hit, parked while
    # they load (several groups park every load).
    assert "b" not in output.num_scheduled_tokens
    (req,) = output.kv_connector_metadata.requests
    assert not req.is_store and req.deferred_load and req.num_matched_chunks == 3
    assert (req.load_start_token, req.load_end_token) == (0, 96)
    swa, full = req.group_block_ids
    # vLLM gives the hit's window (tokens 64-95) real blocks, and nothing
    # before it.
    assert swa == [0, 0, 0, 0, swa[4], swa[5]] and swa[4] and swa[5]
    assert all(full) and len(full) == 6

    engine.finish_requests("b", RequestStatus.FINISHED_ABORTED)
    finished.assert_called_once()
    assert len(finished.call_args.args[1]) == 2


def _serve_pinned(worker, store):
    """Serve objects from pinned host memory, as a registered CXL mapping."""
    pinned = {
        key: torch.frombuffer(bytearray(raw), dtype=torch.uint8).pin_memory()
        for key, raw in store.items()
    }
    worker._handler.batch_retrieve.side_effect = lambda keys: [
        SimpleNamespace(view=memoryview(pinned[k].numpy())) for k in keys
    ]


def _spy_kernel_sources(worker):
    """Record the device of every slab handed to the attention kernel."""
    cache = worker._group_caches[3]
    ops, *rest = worker._group_kernel(cache)
    sources = []

    def transfer(src, *args, **kwargs):
        sources.append(src.device.type)
        return ops.multi_layer_kv_transfer(src, *args, **kwargs)

    spy = SimpleNamespace(
        multi_layer_kv_transfer=transfer, TransferDirection=ops.TransferDirection
    )
    worker._group_kernels[3] = (spy, *rest)
    return sources


@pytest.mark.parametrize(
    ("parked", "source"), [(False, "cpu"), (True, "cuda")], ids=["inline", "parked"]
)
def test_only_a_parked_load_stages_slabs_before_the_kernel(parked, source):
    """Inline: the kernel reads pinned CXL in place. Parked: the copy engine
    moves the slab first, so the kernel does not hold SMs during the read."""
    stored, store, original = _kernel_worker()
    stored.wait_for_save(_store_meta(list(range(40)), QWEN_BLOCKS, 0, 32))
    worker, target = _cuda_loading_worker(store)
    _serve_pinned(worker, store)
    sources = _spy_kernel_sources(worker)
    if parked:
        meta = _load_meta(list(range(40)), B_BLOCKS, 2, deferred_load=True)
    else:
        meta = _load_meta(list(range(40)), B_BLOCKS, 2, 32, 8)
    worker.start_load_kv(_FORWARD, meta)
    if parked:
        worker._deferred_executor.shutdown(wait=True)
    torch.cuda.synchronize()
    assert sources == [source, source]
    _check_loaded(stored, worker, original, target, in_place=not parked)


def _backend_layers(name):
    """vLLM's static forward context, for the tiny model's attention layers."""
    backend = SimpleNamespace(get_name=lambda: name)
    return {
        f"layers.{i}.attn": SimpleNamespace(attn_backend=backend) for i in TINY_ATTN
    }


@pytest.mark.parametrize(
    ("backend", "builds"), [("TRITON_ATTN", False), ("FLASH_ATTN", True)]
)
def test_registration_decides_the_kernel_from_the_backend(backend, builds):
    """Decided before any forward, so the first load already uses it."""
    worker = _worker()
    worker._packed_load_kernel_ctx = MagicMock(return_value=("ops",) * 6)
    worker.register_kv_caches(
        tiny_kv_caches("0.23", _shared()), _backend_layers(backend)
    )
    expected = ("ops",) * 6 if builds else None
    assert worker._group_kernels == {3: expected}
    assert worker._packed_load_kernel_ctx.call_count == int(builds)
    assert worker._group_kernel(worker._group_caches[3]) == expected


def test_without_the_backend_the_first_forward_decides():
    worker = _worker()
    worker._packed_load_kernel_ctx = MagicMock(return_value=("ops",) * 6)
    worker.register_kv_caches(tiny_kv_caches("0.23", _shared()))
    cache = worker._group_caches[3]
    # No forward yet: go without the kernel, decide nothing.
    assert worker._group_kernel(cache) is None
    assert worker._group_kernels == {}
    worker._last_attn_metadata = make_flash_attn_metadata()
    assert worker._group_kernel(cache) == ("ops",) * 6
    assert worker._group_kernels == {3: ("ops",) * 6}


def test_connector_hands_the_models_layers_to_the_worker():
    connector = MaruKVConnector.__new__(MaruKVConnector)
    connector._worker = MagicMock()
    layers = {"layers.3.attn": object()}
    connector._vllm_config = SimpleNamespace(
        compilation_config=SimpleNamespace(static_forward_context=layers)
    )
    connector.register_kv_caches({})
    connector._worker.register_kv_caches.assert_called_once_with({}, layers)


def test_the_first_parked_load_uses_the_kernel_before_any_forward():
    stored, store, original = _kernel_worker()
    stored.wait_for_save(_store_meta(list(range(40)), QWEN_BLOCKS, 0, 32))
    target = torch.zeros_like(_shared()).cuda()
    worker = _worker(kv_cache_layout="NHD")
    worker.register_kv_caches(
        tiny_kv_caches("0.23", target), _backend_layers("FLASH_ATTN")
    )
    attach_capturing_handler(worker)
    assert worker._last_attn_metadata is None
    assert worker._group_kernels[3] is not None
    _serve_pinned(worker, store)
    sources = _spy_kernel_sources(worker)
    worker.start_load_kv(
        _FORWARD, _load_meta(list(range(40)), B_BLOCKS, 2, deferred_load=True)
    )
    worker._deferred_executor.shutdown(wait=True)
    torch.cuda.synchronize()
    assert sources == ["cuda", "cuda"]
    _check_loaded(stored, worker, original, target, in_place=False)


# =============================================================================
# More vLLM scheduler, GPU and failure cases
# =============================================================================


def _mamba_engine(tmp_path, extra=None):
    """vLLM's scheduler for a Mamba group and a full-attention group, block 16."""
    if not supports_hma(MaruKVConnector):
        pytest.skip("Maru takes several KV cache groups from vLLM 0.30")
    kw = {"block_size": 16, "num_kv_heads": 1, "head_size": 1, "dtype": torch.float32}
    mamba = MambaSpec(
        block_size=16,
        shapes=((32,),),
        dtypes=(torch.float32,),
        mamba_cache_mode="align",
    )
    return make_vllm_scheduler(
        tmp_path,
        16,
        16,
        extra,
        num_blocks=64,
        enable_prefix_caching=True,
        kv_load_failure_policy="fail",
        kv_cache_groups=[
            KVCacheGroupSpec(["m"], mamba),
            KVCacheGroupSpec(["full"], FullAttentionSpec(**kw)),
        ],
    )


def _add_hit_request(engine, tokens):
    """Request b over ``tokens``, whose 6 chunks Maru holds with a Mamba state
    at chunk 5, as a request ending a prefill step there would leave."""
    from vllm.sampling_params import SamplingParams
    from vllm.utils.hashing import sha256
    from vllm.v1.core.kv_cache_utils import get_request_block_hasher, init_none_hash
    from vllm.v1.request import Request

    init_none_hash(sha256)
    engine.connector._scheduler._handler = _Store(_stored_by(tokens, 16, [1], [0], [5]))
    engine.add_request(
        Request(
            "b",
            tokens,
            SamplingParams(max_tokens=1),
            None,
            block_hasher=get_request_block_hasher(16, sha256),
        )
    )


@pytest.mark.parametrize("extra", [None, {"maru_async_load": True}])
def test_vllm_scheduler_gives_a_parked_mamba_load_the_hits_last_block(tmp_path, extra):
    """Parked by default with several groups, or when asked for."""
    from vllm.v1.outputs import KVConnectorOutput, ModelRunnerOutput

    engine = _mamba_engine(tmp_path, extra)
    _add_hit_request(engine, list(range(100)))
    output = engine.schedule()
    assert "b" not in output.num_scheduled_tokens  # parked while it loads
    (req,) = output.kv_connector_metadata.requests
    assert req.deferred_load and req.num_matched_chunks == 6
    assert req.num_scheduled_tokens == 0  # no step to write a second block for
    mamba, full = req.group_block_ids
    assert mamba[:5] == [0] * 5 and mamba[5] and len(mamba) == 6

    engine.update_from_output(
        output,
        ModelRunnerOutput(
            req_ids=[],
            req_id_to_index={},
            sampled_token_ids=[],
            logprobs=None,
            prompt_logprobs_dict={},
            pooler_output=[],
            kv_connector_output=KVConnectorOutput(finished_recving={"b"}),
        ),
    )
    resumed = engine.schedule()
    assert resumed.num_scheduled_tokens["b"] == 4
    (new_req,) = resumed.scheduled_new_reqs
    assert new_req.num_computed_tokens == 96
    # vLLM copies the loaded state from position 5 into the step's block.
    assert new_req.block_ids[0][5] == mamba[5] and new_req.block_ids[0][6]


def test_kernel_sliding_window_round_trip_skips_null_blocks():
    from maru_vllm.connector import _resolve_kv_ops

    if not torch.cuda.is_available() or _resolve_kv_ops() is None:
        pytest.skip("needs CUDA and the maru_kv_ops kernels")

    def swa_caches(shared):
        return {
            f"layers.{i}.attn": _attn_tensor(_region(shared, i // 2), "0.23")
            for i in range(4)
        }

    source = _shared().cuda()
    stored, store = _storing_worker(swa_caches(source), tiny_swa_config(), chunk=64)
    stored._last_attn_metadata = make_flash_attn_metadata()
    tokens = list(range(80))
    stored.wait_for_save(_store_meta(tokens, ([7, 8, 9, 10], [3, 4, 5, 6]), 0, 64))
    assert stored._group_kernels[0] is not None

    target = torch.zeros_like(source)
    worker, _ = _storing_worker(swa_caches(target), tiny_swa_config(), chunk=64)
    worker._last_attn_metadata = make_flash_attn_metadata()
    _serve(worker, store)
    worker.start_load_kv(
        _FORWARD, _load_meta(tokens, ([0, 0, 1, 2], [3, 4, 5, 6]), 1, 64, 16)
    )
    torch.cuda.synchronize()
    swa_src, full_src = ([t.cpu() for t in c.tensors] for c in stored._group_caches)
    swa_dst, full_dst = ([t.cpu() for t in c.tensors] for c in worker._group_caches)
    for a_block, b_block in ((9, 1), (10, 2)):
        for a, b in zip(swa_src, swa_dst, strict=True):
            assert torch.equal(
                _token_rows(b, b_block, "0.23"), _token_rows(a, a_block, "0.23")
            )
    assert not _token_rows(swa_dst[0], 0, "0.23").any()
    for block in (3, 4, 5, 6):
        for a, b in zip(full_src, full_dst, strict=True):
            assert torch.equal(
                _token_rows(b, block, "0.23"), _token_rows(a, block, "0.23")
            )


@pytest.mark.parametrize("parked", [False, True], ids=["inline", "parked"])
def test_a_failure_while_placing_is_reported(parked):
    _, _, store = _stored_qwen()
    worker, _ = _loading_worker(store)
    worker._place_state = MagicMock(side_effect=RuntimeError("copy failed"))
    if parked:
        meta = _load_meta(list(range(40)), B_BLOCKS, 2, deferred_load=True)
    else:
        meta = _load_meta(list(range(40)), B_BLOCKS, 2, 32, 8)
    worker.start_load_kv(_FORWARD, meta)
    assert _reported_failures(worker) == (FAILED_PARKED if parked else FAILED_INLINE)


@pytest.mark.parametrize("parked", [False, True], ids=["inline", "parked"])
def test_an_unavailable_handler_fails_the_loads(parked):
    """The worker has no MaruHandler (it got no CXL region, say) while the
    scheduler reported a hit: nothing fills the blocks, so they are reported."""
    _, _, store = _stored_qwen()
    worker, target = _loading_worker(store)
    worker._handler = None
    worker._handler_retry_after = time.monotonic() + 60.0
    if parked:
        meta = _load_meta(list(range(40)), B_BLOCKS, 2, deferred_load=True)
    else:
        meta = _load_meta(list(range(40)), B_BLOCKS, 2, 32, 8)
    # A store in the same step is not a failed load.
    meta.requests += _store_meta(
        list(range(40)), QWEN_BLOCKS, 0, 32, req_ids=("a",)
    ).requests
    worker.start_load_kv(_FORWARD, meta)
    assert _reported_failures(worker) == (FAILED_PARKED if parked else FAILED_INLINE)
    assert not target.any()


def test_an_inline_load_of_a_missing_object_is_reported():
    _, _, store = _stored_qwen()
    k1 = _chunk_keys(list(range(40)), TINY_BLOCK)[1]
    del store[group_object_key(k1, 0)]
    worker, target = _loading_worker(store)
    worker.start_load_kv(_FORWARD, _load_meta(list(range(40)), B_BLOCKS, 2, 32, 8))
    assert _reported_failures(worker) == FAILED_INLINE
    assert not target.any()


def test_a_failure_on_the_loader_thread_unparks_and_is_reported():
    stored, store, _ = _kernel_worker()
    stored.wait_for_save(_store_meta(list(range(40)), QWEN_BLOCKS, 0, 32))
    worker, _ = _cuda_loading_worker(store)
    worker._place_state = MagicMock(side_effect=RuntimeError("copy failed"))
    meta = _load_meta(list(range(40)), B_BLOCKS, 2, deferred_load=True)
    worker.start_load_kv(_FORWARD, meta)
    worker._deferred_executor.shutdown(wait=True)
    assert _reported_failures(worker) == FAILED_PARKED


def test_a_parked_load_lands_after_work_queued_before_it():
    """Work vLLM queued on the compute stream for these blocks before the
    load (a copy-on-write, say) must not overwrite it, however late it runs."""
    stored, store, source = _kernel_worker()
    stored.wait_for_save(_store_meta(list(range(40)), QWEN_BLOCKS, 0, 32))
    worker, target = _cuda_loading_worker(store)
    _serve_pinned(worker, store)  # as CXL is: pinned copies are truly async
    meta = _load_meta(list(range(40)), B_BLOCKS, 2, deferred_load=True)

    def parked_load():
        worker.start_load_kv(_FORWARD, meta)
        worker._deferred_executor.shutdown(wait=True)
        worker._deferred_executor = None
        torch.cuda.synchronize()
        assert worker.get_finished_loading() == {"b"}

    # Warm up as a running server is: a first kernel launch or cudaMalloc
    # synchronizes the device and would hide a missing wait.
    target.fill_(0)
    parked_load()
    # The compute stream is busy, then overwrites every block. A stream of its
    # own, since the legacy default stream would order the load implicitly.
    compute = torch.cuda.Stream()
    with torch.cuda.stream(compute):
        torch.cuda._sleep(1_500_000_000)
        target.fill_(7)
        worker.start_load_kv(_FORWARD, meta)
    worker._deferred_executor.shutdown(wait=True)
    torch.cuda.synchronize()
    assert worker.get_finished_loading() == {"b"}
    _check_loaded(stored, worker, source, target, in_place=False)


# =============================================================================
# Tensor parallelism
# =============================================================================


def _rank_shared(rank):
    """``_shared()`` with every byte shifted by rank: each rank's own KV."""
    return (_shared().to(torch.int64) + 97 * rank).remainder(251).to(torch.uint8)


def _tp_shards(rank, tp_size, total_heads):
    """``rank``'s KV heads and its part of each Mamba state."""
    return {
        "kv_shard": tp_kv_shard(rank, tp_size, total_heads),
        "state_shard": tp_state_shard(rank, tp_size),
    }


def _tp_worker(kv_caches, shards, server, config=None, chunk=TINY_BLOCK, **kwargs):
    """A registered rank on ``server``, which every rank shares."""
    worker = _worker(
        config, chunk, kv_cache_layout=_engine_layout(kv_caches), **shards, **kwargs
    )
    worker.register_kv_caches(kv_caches)
    worker._handler = server.handler()
    return worker


def _registered_keys(worker):
    return {k for c in worker._handler.batch_store.call_args_list for k in c.args[0]}


@pytest.mark.parametrize("version", ["0.23", "0.30"])
def test_tp_ranks_store_and_load_their_own_slices(version):
    """TP=2 over 4 KV heads on one first-register-wins server: each rank
    stores its own heads and its own part of every Mamba state, and loads
    them back rather than the other rank's."""
    server = FirstWinsServer()
    tokens = list(range(40))
    sources, stored = [], []
    for rank in range(2):
        sources.append(_rank_shared(rank))
        worker = _tp_worker(
            tiny_kv_caches(version, sources[rank]), _tp_shards(rank, 2, 4), server
        )
        worker.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32))
        stored.append(worker)

    k0, k1 = _chunk_keys(tokens, TINY_BLOCK)
    assert set(server.objects) == {
        *(group_object_key(k + h, 3) for k in (k0, k1) for h in ("_h0-2", "_h2-4")),
        *(group_object_key(k1 + s, g) for s in ("_s0of2", "_s1of2") for g in range(3)),
    }
    for rank in range(2):
        target = torch.zeros_like(_shared())
        worker = _tp_worker(
            tiny_kv_caches(version, target), _tp_shards(rank, 2, 4), server
        )
        worker.start_load_kv(_FORWARD, _load_meta(tokens, B_BLOCKS, 2, 32, 8))
        assert worker.take_failed_load_blocks() == set()
        src = stored[rank]._group_caches[3].tensors
        dst = worker._group_caches[3].tensors
        for a_block, b_block in ((4, 7), (5, 8)):
            for a, b in zip(src, dst, strict=True):
                assert torch.equal(
                    _token_rows(b, b_block, version), _token_rows(a, a_block, version)
                )
        for g in range(3):
            for layer in range(2):
                assert torch.equal(
                    _state(target, layer, B_BLOCKS[g][1]),
                    _state(sources[rank], layer, QWEN_BLOCKS[g][1]),
                )


def _one_head_attn(region):
    """A rank's attention layer holding one KV head (flash, vLLM 0.23)."""
    return region.view(torch.bfloat16).view(TINY_BLOCKS, 2, 16, 1, 16)


def _one_head_caches(shared):
    caches = tiny_kv_caches("0.23", shared)
    for p, i in enumerate(TINY_ATTN):
        caches[f"layers.{i}.attn"] = _one_head_attn(_region(shared, p))
    return caches


def test_tp_replica_skips_attention_but_stores_its_state():
    """TP=4 over 2 KV heads: ranks 0 and 1 hold head 0, but each holds its
    own part of every Mamba state. Rank 1 stores only its state and loads
    rank 0's attention."""
    server = FirstWinsServer()
    tokens = list(range(40))
    source0 = _rank_shared(0)
    # Same attention bytes (blocks 4, 5), different states (blocks 1-3).
    source1 = source0.clone()
    for p in range(2):
        start = p * TINY_LAYER_BYTES + TINY_SLOT
        source1[start : start + 3 * TINY_SLOT] += 1
    workers = []
    for rank, source in enumerate((source0, source1)):
        worker = _tp_worker(
            _one_head_caches(source),
            _tp_shards(rank, 4, 2),
            server,
            num_kv_heads=1,
            head_size=16,
        )
        worker.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32))
        workers.append(worker)

    k0, k1 = _chunk_keys(tokens, TINY_BLOCK)
    assert _registered_keys(workers[1]) == {
        group_object_key(k1 + "_s1of4", g) for g in range(3)
    }
    assert group_object_key(k0 + "_h0-1", 3) in _registered_keys(workers[0])

    target = torch.zeros_like(_shared())
    worker = _tp_worker(
        _one_head_caches(target),
        _tp_shards(1, 4, 2),
        server,
        num_kv_heads=1,
        head_size=16,
    )
    worker.start_load_kv(_FORWARD, _load_meta(tokens, B_BLOCKS, 2, 32, 8))
    assert worker.take_failed_load_blocks() == set()
    src = workers[0]._group_caches[3].tensors
    dst = worker._group_caches[3].tensors
    for a_block, b_block in ((4, 7), (5, 8)):
        for a, b in zip(src, dst, strict=True):
            assert torch.equal(b[b_block], a[a_block])
    for g in range(3):
        for layer in range(2):
            assert torch.equal(
                _state(target, layer, B_BLOCKS[g][1]),
                _state(source1, layer, QWEN_BLOCKS[g][1]),
            )


def _swa_caches(shared):
    return {
        f"layers.{i}.attn": _attn_tensor(_region(shared, i // 2), "0.30")
        for i in range(4)
    }


def test_tp_sliding_window_ranks_load_their_own_heads():
    """A small gpt-oss at TP=2: both groups carry each rank's own heads."""
    server = FirstWinsServer()
    tokens = list(range(80))
    config = tiny_swa_config()
    stored = []
    for rank in range(2):
        worker = _tp_worker(
            _swa_caches(_rank_shared(rank)),
            _tp_shards(rank, 2, 4),
            server,
            config,
            chunk=64,
        )
        worker.wait_for_save(_store_meta(tokens, ([7, 8, 9, 10], [3, 4, 5, 6]), 0, 64))
        stored.append(worker)

    (k0,) = _chunk_keys(tokens, 64)
    assert set(server.objects) == {
        group_object_key(k0 + h, g) for h in ("_h0-2", "_h2-4") for g in (0, 1)
    }
    for rank in range(2):
        worker = _tp_worker(
            _swa_caches(torch.zeros_like(_shared())),
            _tp_shards(rank, 2, 4),
            server,
            config,
            chunk=64,
        )
        blocks = ([0, 0, 1, 2], [3, 4, 5, 6])
        worker.start_load_kv(_FORWARD, _load_meta(tokens, blocks, 1, 64, 16))
        assert worker.take_failed_load_blocks() == set()
        for gid, pairs in ((0, ((9, 1), (10, 2))), (1, ((3, 3), (6, 6)))):
            src = stored[rank]._group_caches[gid].tensors
            dst = worker._group_caches[gid].tensors
            for a_block, b_block in pairs:
                for a, b in zip(src, dst, strict=True):
                    assert torch.equal(
                        _token_rows(b, b_block, "0.30"),
                        _token_rows(a, a_block, "0.30"),
                    )


# Qwen3.5 at TP=2: the two KV heads of its attention group (group 3) on one
# rank each, and two parts of each Mamba state (groups 0-2).
QWEN35_TP2 = {
    "shard_suffixes": ("_h0-1", "_h1-2"),
    "state_suffixes": ("_s0of2", "_s1of2"),
}


def _qwen35_tp2_objects(keys):
    """What both ranks store of a 1,200-token prompt whose first step ended
    at chunk 1: attention for every chunk, states for chunk 1."""
    attention = {group_object_key(k + h, 3) for k in keys for h in ("_h0-1", "_h1-2")}
    states = {
        group_object_key(keys[1] + s, g) for s in ("_s0of2", "_s1of2") for g in range(3)
    }
    return attention | states


def test_tp_hit_needs_every_slice_of_every_group():
    a = list(range(1200))  # step 1 ends at token 1,088
    keys = _chunk_keys(a, QWEN35_BLOCK)
    stored = _qwen35_tp2_objects(keys)
    sched = _qwen35_scheduler(stored, **QWEN35_TP2)
    assert _hit(sched, a) == 1088
    # One lookup asked about every slice of every group.
    assert stored <= set(sched._handler.queries[0])

    # One rank's part of one Mamba state is missing: no chunk can resume.
    missing_state = group_object_key(keys[1] + "_s1of2", 0)
    assert _hit(_qwen35_scheduler(stored - {missing_state}, **QWEN35_TP2), a) == 0
    missing_heads = group_object_key(keys[0] + "_h1-2", 3)
    assert _hit(_qwen35_scheduler(stored - {missing_heads}, **QWEN35_TP2), a) == 0


def _tp_connector_config(block_size, tp_size, total_heads):
    config = _connector_config(block_size)
    config.parallel_config = SimpleNamespace(tensor_parallel_size=tp_size)
    config.model_config = tp_model_config(total_heads)
    return config


def test_tp_connector_wires_each_group_slice(monkeypatch):
    gpt = MaruKVConnector(
        _tp_connector_config(16, 2, 8), KVConnectorRole.SCHEDULER, gpt_oss_config()
    )
    assert gpt._scheduler._shard_suffixes == ("_h0-4", "_h4-8")

    qwen_config = _tp_connector_config(QWEN35_BLOCK, 2, 2)
    qwen = MaruKVConnector(qwen_config, KVConnectorRole.SCHEDULER, qwen35_config())
    assert qwen._scheduler._shard_suffixes == ("_h0-1", "_h1-2")
    assert qwen._scheduler._state_suffixes == ("_s0of2", "_s1of2")

    monkeypatch.setattr(connector_module, "_tp_rank", lambda: 1)
    worker = MaruKVConnector(
        qwen_config, KVConnectorRole.WORKER, qwen35_config()
    )._worker
    assert worker._kv_shard.suffix == "_h1-2"
    assert worker._state_shard.suffix == "_s1of2"


def test_tp1_several_groups_keep_bare_keys(monkeypatch):
    def no_rank():
        raise AssertionError("TP=1 must not query the TP group")

    monkeypatch.setattr(connector_module, "_tp_rank", no_rank)
    config = _tp_connector_config(16, 1, 8)
    sched = MaruKVConnector(config, KVConnectorRole.SCHEDULER, gpt_oss_config())
    assert sched._scheduler._shard_suffixes == ("",)
    assert sched._scheduler._state_suffixes == ("",)
    worker = MaruKVConnector(config, KVConnectorRole.WORKER, gpt_oss_config())
    assert worker._worker._kv_shard == WHOLE_MODEL
    assert worker._worker._state_shard == WHOLE_MODEL


class _FillOps:
    """maru_kv_ops stand-in whose D2H gather fills the slab with ``fill``."""

    class TransferDirection:
        D2H = "D2H"
        H2D = "H2D"

    def __init__(self, fill):
        self.fill = fill

    def multi_layer_kv_transfer(self, slab, *args, **kwargs):
        slab.fill_(self.fill)


@pytest.mark.parametrize("write_behind", [False, True])
def test_tp_kernel_stores_each_ranks_heads(write_behind):
    """The kernel stores of a GPU run, gathered now or written behind, name
    each rank's attention slab by its own heads."""
    if write_behind and not torch.cuda.is_available():
        pytest.skip("write-behind needs CUDA streams")
    server = FirstWinsServer()
    tokens = list(range(40))
    for rank in range(2):
        shared = _rank_shared(rank)
        worker = _tp_worker(
            tiny_kv_caches("0.23", shared.cuda() if write_behind else shared),
            _tp_shards(rank, 2, 4),
            server,
            extra_config={"maru_async_store": True} if write_behind else None,
        )
        worker._group_kernels[3] = (_FillOps(rank + 1), None, None, TINY_BLOCK, 8, "")
        worker._store_executor = _HeldExecutor()
        worker.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32))
        worker._store_executor.run()

    for k in _chunk_keys(tokens, TINY_BLOCK):
        for rank, heads in enumerate(("_h0-2", "_h2-4")):
            raw = server.objects[group_object_key(k + heads, 3)]
            slab = torch.frombuffer(bytearray(raw), dtype=torch.bfloat16)
            assert torch.all(slab == rank + 1)


def test_tp_refuses_replicated_heads_with_several_attention_groups():
    """Above TP = KV heads, several attention groups are refused
    (check_multi_group_support); one attention group is named as usual."""
    with pytest.raises(ValueError, match="several attention groups"):
        MaruKVConnector(
            _tp_connector_config(16, 16, 8),
            KVConnectorRole.SCHEDULER,
            gpt_oss_config(),
        )
    # One attention group: its heads are the model's, replicated as named.
    qwen = MaruKVConnector(
        _tp_connector_config(QWEN35_BLOCK, 4, 2),
        KVConnectorRole.SCHEDULER,
        qwen35_config(),
    )
    assert qwen._scheduler._shard_suffixes == ("_h0-1", "_h1-2")


def _qwen35_tp4_worker(monkeypatch, rank):
    """A Qwen3.5 worker at TP=4: its 2 KV heads sit on two ranks each."""
    monkeypatch.setattr(connector_module, "_tp_rank", lambda: rank)
    config = _tp_connector_config(QWEN35_BLOCK, 4, 2)
    return MaruKVConnector(config, KVConnectorRole.WORKER, qwen35_config())._worker


def test_tp_connector_wires_replicated_heads_of_a_hybrid_model(monkeypatch):
    config = _tp_connector_config(QWEN35_BLOCK, 4, 2)
    sched = MaruKVConnector(config, KVConnectorRole.SCHEDULER, qwen35_config())
    assert sched._scheduler._shard_suffixes == ("_h0-1", "_h1-2")
    assert sched._scheduler._state_suffixes == ("_s0of4", "_s1of4", "_s2of4", "_s3of4")
    worker = _qwen35_tp4_worker(monkeypatch, 1)
    # Rank 1 holds rank 0's KV head but a part of each state of its own.
    assert worker._kv_shard.suffix == "_h0-1"
    assert not worker._kv_shard.is_writer
    assert worker._state_shard.suffix == "_s1of4"
    assert worker._state_shard.is_writer


def test_tp_worker_logs_what_it_stores_of_each_group(monkeypatch, caplog):
    with caplog.at_level(logging.INFO, logger="maru_vllm.connector"):
        _qwen35_tp4_worker(monkeypatch, 1)
    messages = [r.getMessage() for r in caplog.records]
    (line,) = [m for m in messages if "TP rank 1 of 4" in m]
    assert "'_h0-1' and only loads it" in line
    assert "stores its part '_s1of4' of each Mamba-type state" in line


def test_tp_lookup_log_names_a_missing_rank_slice(caplog):
    a = list(range(1200))
    keys = _chunk_keys(a, QWEN35_BLOCK)
    stored = _qwen35_tp2_objects(keys) - {group_object_key(keys[1] + "_s1of2", 0)}
    with caplog.at_level(logging.INFO, logger="maru_vllm.connector"):
        assert _hit(_qwen35_scheduler(stored, **QWEN35_TP2), a) == 0
    assert any("a TP rank's slice" in r.getMessage() for r in caplog.records)
