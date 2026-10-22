# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Which slice of each chunk's KV one vLLM worker holds, and its key suffix.

Under tensor parallelism every worker process runs its own connector and
holds its own slice of the paged KV cache, yet all of them hash a prompt to
the same chunk keys. A key must therefore name the slice as well as the
prefix: otherwise every rank registers its heads under one key, the server
keeps whichever arrived first, and the other ranks load heads that are not
theirs.

A slice is named by what it contains — the KV heads it covers and, once
pipeline parallelism is supported, the decoder layers — never by the rank
that wrote it. Ranks holding identical KV (an MLA latent, or more TP ranks
than KV heads) then produce one key, so the server's first-register-wins
rule stores that KV once. Engines that partition the model differently can
never mistake each other's slices for their own. A slice covering the whole
model keeps the bare key, so TP=1 keys are unchanged.

Pure: integers in, names out. The connector supplies the parallel sizes and
the rank from vLLM.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class KVShard:
    """The KV slice one worker holds, and how its keys name it.

    Attributes:
        head_start: First KV head of the slice.
        head_end: One past the last KV head of the slice.
        total_heads: KV heads of the whole model; 1 for KV that every rank
            holds in full (an MLA latent).
        layer_start: First decoder layer of the slice.
        layer_end: One past the last decoder layer of the slice.
        total_layers: Decoder layers of the whole model, or 0 when the slice
            covers all of them. Nonzero only once pipeline parallelism is
            supported.
        replica_index: Position of this rank among the ranks that hold this
            same slice.
        num_replicas: Ranks that hold this same slice.
    """

    head_start: int = 0
    head_end: int = 1
    total_heads: int = 1
    layer_start: int = 0
    layer_end: int = 0
    total_layers: int = 0
    replica_index: int = 0
    num_replicas: int = 1

    @property
    def num_heads(self) -> int:
        """KV heads this slice holds."""
        return self.head_end - self.head_start

    @property
    def suffix(self) -> str:
        """What a chunk key appends to name this slice; empty for the whole.

        ``_h<start>-<end>`` names a range of KV heads and, under pipeline
        parallelism, ``_l<start>-<end>`` a range of decoder layers. Ranges,
        not rank numbers: the same name always means the same bytes.
        """
        suffix = ""
        if (self.head_start, self.head_end) != (0, self.total_heads):
            suffix += f"_h{self.head_start}-{self.head_end}"
        if self.total_layers and (self.layer_start, self.layer_end) != (
            0,
            self.total_layers,
        ):
            suffix += f"_l{self.layer_start}-{self.layer_end}"
        return suffix

    @property
    def is_writer(self) -> bool:
        """Whether this rank stores the slice; its replicas only load it."""
        return self.replica_index == 0


WHOLE_MODEL = KVShard()
"""The slice of a worker that holds all KV, as at TP=1."""


def tp_kv_shard(
    tp_rank: int, tp_size: int, total_kv_heads: int, replicated: bool = False
) -> KVShard:
    """The KV slice that tensor-parallel rank ``tp_rank`` holds.

    Follows vLLM's ``QKVParallelLinear``: with at least as many KV heads as
    ranks each rank takes a contiguous ``total_kv_heads // tp_size`` of them;
    with fewer, each head is replicated on ``tp_size // total_kv_heads``
    consecutive ranks and rank r holds head ``r // replicas``. vLLM requires
    one count to divide the other, and so does this function.

    Args:
        tp_rank: The worker's rank in its tensor-parallel group.
        tp_size: Tensor-parallel group size.
        total_kv_heads: KV heads of the whole model.
        replicated: True when every rank holds the same KV in full (MLA).

    Returns:
        The slice ``tp_rank`` holds.

    Raises:
        ValueError: On a rank outside the group, or head and rank counts
            where neither divides the other.
    """
    if tp_size < 1 or not 0 <= tp_rank < tp_size:
        raise ValueError(f"TP rank {tp_rank} is outside a group of {tp_size}")
    if replicated:
        return KVShard(replica_index=tp_rank, num_replicas=tp_size)
    if total_kv_heads < 1:
        raise ValueError(f"A model needs at least one KV head, not {total_kv_heads}")
    if total_kv_heads >= tp_size:
        if total_kv_heads % tp_size:
            raise ValueError(
                f"{total_kv_heads} KV heads cannot be split over {tp_size} TP ranks"
            )
        per_rank = total_kv_heads // tp_size
        start = tp_rank * per_rank
        return KVShard(start, start + per_rank, total_kv_heads)
    if tp_size % total_kv_heads:
        raise ValueError(
            f"{total_kv_heads} KV heads cannot be replicated over {tp_size} TP ranks"
        )
    replicas = tp_size // total_kv_heads
    head = tp_rank // replicas
    return KVShard(
        head,
        head + 1,
        total_kv_heads,
        replica_index=tp_rank % replicas,
        num_replicas=replicas,
    )


def tp_kv_shards(
    tp_size: int, total_kv_heads: int, replicated: bool = False
) -> list[KVShard]:
    """Every distinct slice of one engine's KV, in rank order.

    What the scheduler checks: a chunk is cached only once every slice of it
    is stored. Replicated ranks share a slice, so it is listed once.

    Args:
        tp_size: Tensor-parallel group size.
        total_kv_heads: KV heads of the whole model.
        replicated: True when every rank holds the same KV in full (MLA).

    Returns:
        One writer slice per distinct suffix.
    """
    shards: dict[str, KVShard] = {}
    for rank in range(tp_size):
        shard = tp_kv_shard(rank, tp_size, total_kv_heads, replicated)
        shards.setdefault(shard.suffix, shard)
    return list(shards.values())
