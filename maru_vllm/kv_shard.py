# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Which slice of each chunk's KV one vLLM worker holds, and its key suffix.

Under tensor parallelism every worker process runs its own connector and
holds its own slice of the paged KV cache, yet all of them hash a prompt to
the same chunk keys. A key must therefore name the slice as well as the
prefix: otherwise every rank registers its heads under one key, the server
keeps whichever arrived first, and the other ranks load heads that are not
theirs. Code calls a slice a shard (``KVShard``).

A slice of attention KV is named by the KV heads it covers and, once
pipeline parallelism is supported, the decoder layers, rather than by the
rank that holds it. Ranks holding identical KV (an MLA latent, or more TP
ranks than KV heads) therefore share one key: the first of them stores it
and the others only load it (``KVShard.is_writer``). Engines that partition
the model differently can never mistake each other's slices for their own.
A slice covering the whole model keeps the bare chunk key, as at TP=1.

A Mamba-type state (vLLM's ``MambaSpec``, such as Gated DeltaNet's conv and
recurrent state) is the exception, named by rank: vLLM gives each rank 1/TP
of every state tensor, and a rank's conv state gathers its share of the q,
k and v channels, which no single head range describes. Its slice is named
``_s<part>of<parts>``, the rank's part of its TP group. That still names
content, since the same model at the same TP size puts the same bytes in
each part. Engines of different TP sizes never share a state slice, and no
two ranks hold the same part, so every rank stores its own.

Pure: integers in, names out. The connector supplies the parallel sizes and
the rank from vLLM.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property


@dataclass(frozen=True)
class KVShard:
    """The KV slice one worker holds, and how its keys name it.

    Attributes:
        head_start: First KV head of the slice.
        head_end: One past the last KV head of the slice.
        total_heads: The model's KV heads when the slice is a range of them;
            1 otherwise (all KV, an MLA latent, or a Mamba-type state).
        layer_start: First decoder layer of the slice; ignored while
            ``total_layers`` is 0.
        layer_end: One past the last decoder layer of the slice; ignored
            while ``total_layers`` is 0.
        total_layers: Decoder layers of the whole model, or 0 when the slice
            covers all of them. Nonzero only once pipeline parallelism is
            supported.
        replica_index: Position of this rank among the ranks that hold this
            same slice.
        num_replicas: Ranks that hold this same slice.
        state_part: For a Mamba-type state, the rank's part of it.
        state_parts: Parts a Mamba-type state is split into (the TP size),
            or 1 for a slice of KV heads.
    """

    head_start: int = 0
    head_end: int = 1
    total_heads: int = 1
    layer_start: int = 0
    layer_end: int = 0
    total_layers: int = 0
    replica_index: int = 0
    num_replicas: int = 1
    state_part: int = 0
    state_parts: int = 1

    @property
    def num_heads(self) -> int:
        """KV heads this slice holds."""
        return self.head_end - self.head_start

    @property
    def splits_heads(self) -> bool:
        """Whether the slice holds only some of the KV heads."""
        return (self.head_start, self.head_end) != (0, self.total_heads)

    @cached_property
    def suffix(self) -> str:
        """What a chunk key appends to name this slice; empty for the whole.

        ``_h<start>-<end>`` names a range of KV heads, ``_s<part>of<parts>``
        a part of a Mamba-type state and, under pipeline parallelism,
        ``_l<start>-<end>`` a range of decoder layers. The same name always
        means the same bytes. Cached, since store paths read it per layer.
        """
        suffix = ""
        if self.splits_heads:
            suffix += f"_h{self.head_start}-{self.head_end}"
        if self.state_parts > 1:
            suffix += f"_s{self.state_part}of{self.state_parts}"
        if self.total_layers and (self.layer_start, self.layer_end) != (
            0,
            self.total_layers,
        ):
            suffix += f"_l{self.layer_start}-{self.layer_end}"
        return suffix

    @property
    def is_writer(self) -> bool:
        """Whether this rank stores the slice: the first rank holding it
        does, and the others only load it."""
        return self.replica_index == 0


WHOLE_MODEL = KVShard()
"""The slice of a worker that holds all KV, as at TP=1."""


def _check_rank(tp_rank: int, tp_size: int) -> None:
    if tp_size < 1 or not 0 <= tp_rank < tp_size:
        raise ValueError(f"TP rank {tp_rank} is outside a group of {tp_size}")


def tp_kv_shard(tp_rank: int, tp_size: int, total_kv_heads: int) -> KVShard:
    """The KV slice that tensor-parallel rank ``tp_rank`` holds.

    Follows vLLM's ``QKVParallelLinear``: with at least as many KV heads as
    ranks each rank takes a contiguous ``total_kv_heads // tp_size`` of them;
    with fewer, each head is replicated on ``tp_size // total_kv_heads``
    consecutive ranks and rank r holds head ``r // replicas``. vLLM requires
    one count to divide the other, and so does this function. KV that every
    rank holds whole, such as an MLA latent, is a single head that every
    rank replicates.

    Args:
        tp_rank: The worker's rank in its tensor-parallel group.
        tp_size: Tensor-parallel group size.
        total_kv_heads: KV heads of the whole model; 1 for an MLA latent.

    Returns:
        The slice ``tp_rank`` holds.

    Raises:
        ValueError: On a rank outside the group, or head and rank counts
            where neither divides the other.
    """
    _check_rank(tp_rank, tp_size)
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


def tp_kv_shards(tp_size: int, total_kv_heads: int) -> list[KVShard]:
    """The KV slices the scheduler checks for each chunk, in rank order.

    Ranks that share a slice list it once.

    Args:
        tp_size: Tensor-parallel group size.
        total_kv_heads: KV heads of the whole model; 1 for an MLA latent.

    Returns:
        The writer's slice for each distinct suffix.
    """
    shards: dict[str, KVShard] = {}
    for rank in range(tp_size):
        shard = tp_kv_shard(rank, tp_size, total_kv_heads)
        shards.setdefault(shard.suffix, shard)
    return list(shards.values())


def tp_state_shard(tp_rank: int, tp_size: int) -> KVShard:
    """The part of a Mamba-type state that tensor-parallel rank ``tp_rank`` holds.

    Args:
        tp_rank: The worker's rank in its tensor-parallel group.
        tp_size: Tensor-parallel group size.

    Returns:
        The slice ``tp_rank`` holds; the whole state at TP=1.

    Raises:
        ValueError: On a rank outside the group.
    """
    _check_rank(tp_rank, tp_size)
    return KVShard(state_part=tp_rank, state_parts=tp_size)


def tp_state_shards(tp_size: int) -> list[KVShard]:
    """The parts of a Mamba-type state the scheduler checks, in rank order.

    Args:
        tp_size: Tensor-parallel group size.

    Returns:
        One slice per rank; every rank stores its own part.
    """
    return [tp_state_shard(rank, tp_size) for rank in range(tp_size)]
