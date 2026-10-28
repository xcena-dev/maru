# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for maru_common.kv_shard: naming the KV slice each TP rank holds.

The module is pure integer logic, so these tests need neither torch nor an
inference engine.
"""

import pytest

from maru_common.kv_shard import (
    WHOLE_MODEL,
    tp_kv_shard,
    tp_kv_shards,
    tp_part_shard,
    tp_part_shards,
)


@pytest.mark.parametrize(
    ("tp_size", "total_heads", "expected"),
    [
        # TP=1 keeps the bare key, as before tensor parallelism.
        (1, 8, [""]),
        (2, 8, ["_h0-4", "_h4-8"]),
        (4, 8, ["_h0-2", "_h2-4", "_h4-6", "_h6-8"]),
        (8, 8, [f"_h{r}-{r + 1}" for r in range(8)]),
        # More ranks than KV heads: vLLM gives rank r head r // (TP / heads).
        (16, 8, [f"_h{r // 2}-{r // 2 + 1}" for r in range(16)]),
        (4, 2, ["_h0-1", "_h0-1", "_h1-2", "_h1-2"]),
        (8, 1, [""] * 8),
    ],
)
def test_suffix_names_the_heads_each_rank_holds(tp_size, total_heads, expected):
    assert [
        tp_kv_shard(rank, tp_size, total_heads).suffix for rank in range(tp_size)
    ] == expected


def test_replicated_heads_are_stored_by_the_first_rank_of_each_head():
    shards = [tp_kv_shard(rank, 4, 2) for rank in range(4)]
    assert [s.is_writer for s in shards] == [True, False, True, False]
    assert [s.num_replicas for s in shards] == [2, 2, 2, 2]
    # The scheduler checks each distinct slice once.
    assert [s.suffix for s in tp_kv_shards(4, 2)] == ["_h0-1", "_h1-2"]


def test_sharded_heads_are_each_stored_by_their_own_rank():
    shards = [tp_kv_shard(rank, 2, 8) for rank in range(2)]
    assert all(s.is_writer for s in shards)
    assert [s.num_heads for s in shards] == [4, 4]
    assert tp_kv_shards(2, 8) == shards


def test_mla_latent_is_one_slice_that_rank_zero_stores():
    shards = [tp_kv_shard(rank, 8, 1) for rank in range(8)]
    assert {s.suffix for s in shards} == {""}
    assert [s.is_writer for s in shards] == [True] + [False] * 7
    assert tp_kv_shards(8, 1) == [shards[0]]


def test_engines_of_different_tp_sizes_share_only_slices_of_the_same_heads():
    def names(tp_size, heads):
        return {s.suffix for s in tp_kv_shards(tp_size, heads)}

    assert names(8, 8) == names(16, 8)  # one head per slice either way
    assert names(2, 8).isdisjoint(names(4, 8))
    assert names(1, 8).isdisjoint(names(2, 8))


def test_whole_model_keeps_the_bare_key():
    assert WHOLE_MODEL.suffix == ""
    assert WHOLE_MODEL.is_writer
    assert tp_kv_shard(0, 1, 32).suffix == ""


@pytest.mark.parametrize(
    ("rank", "tp_size", "total_heads"),
    [
        (2, 2, 8),  # rank outside the group
        (-1, 2, 8),
        (0, 0, 8),
        (0, 3, 8),  # 8 heads do not split over 3 ranks
        (0, 3, 2),  # 2 heads do not replicate over 3 ranks
        (0, 2, 0),
    ],
)
def test_rejects_splits_vllm_cannot_build(rank, tp_size, total_heads):
    with pytest.raises(ValueError):
        tp_kv_shard(rank, tp_size, total_heads)


def test_part_shard_names_its_part_of_the_tp_group():
    """vLLM splits a Mamba state by rank, not along one head axis."""
    shard = tp_part_shard(1, 2)
    assert shard.suffix == "_tp2_r1"
    assert shard.is_writer
    assert not shard.splits_heads
    assert tp_part_shard(0, 1).suffix == ""


def test_part_shards_of_different_tp_sizes_never_share_a_name():
    two = {s.suffix for s in tp_part_shards(2)}
    four = {s.suffix for s in tp_part_shards(4)}
    assert two == {"_tp2_r0", "_tp2_r1"}
    assert len(four) == 4
    assert two.isdisjoint(four)


def test_part_shard_rank_must_be_in_the_group():
    with pytest.raises(ValueError, match="outside a group of 2"):
        tp_part_shard(2, 2)


def test_splits_heads_only_for_a_head_range():
    assert tp_kv_shard(0, 2, 8).splits_heads
    assert not tp_kv_shard(1, 8, 1).splits_heads
    assert not WHOLE_MODEL.splits_heads
