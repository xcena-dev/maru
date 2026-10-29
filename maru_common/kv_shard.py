# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Which slice of each chunk's KV one worker holds, and its key suffix.

Under tensor parallelism every worker process of an inference engine runs
its own connector and holds its own slice of the paged KV cache, yet all
of them hash a prompt to the same chunk keys. A key must therefore name the
slice as well as the prefix: otherwise every rank registers its heads under
one key, the server keeps whichever arrived first, and the other ranks load
heads that are not theirs. Code calls a slice a shard (``KVShard``).

A slice of attention KV is named by the KV heads it covers rather than by
the rank that holds it. Ranks holding identical KV (an MLA latent, or more
TP ranks than KV heads) therefore share one key: the first of them stores
it and the others only load it (``KVShard.is_writer``). Engines that
partition the model differently can never mistake each other's slices for
their own. A slice covering the whole model keeps the bare chunk key, as at
TP=1.

Two kinds of slice are named by rank instead, as ``_tp<parts>_r<part>``
(the TP size and the rank whose part it is):

- A Mamba-type state, such as Gated DeltaNet's conv and recurrent state.
  Each rank holds 1/TP of every state tensor, and a rank's conv state
  gathers its share of the q, k and v channels, which no single head range
  describes.
- The attention KV of a model with sliding-window and full layers at more
  TP ranks than KV heads, whether the engine keeps the two kinds in
  separate KV cache groups or in one. The engine replicates each layer's
  heads by that layer's own count, which the connector cannot always tell
  from the model's (one model sets its sliding-window layers' count in
  model code), so it cannot tell which ranks share heads. Kept in one
  group, a rank's object holds both kinds, and two ranks that share the
  full layers' head can still differ in the sliding-window one.

A part still names content, since the same model at the same TP size puts
the same bytes in it. Engines of different TP sizes never share a part, and
every rank stores its own, at the cost of storing replicated heads once per
rank.

Pure: integers in, names out, with no inference engine imported. Each
engine's connector supplies the parallel sizes and the rank.
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
            1 otherwise (all KV, an MLA latent, or a slice named by rank).
        replica_index: Position of this rank among the ranks that hold this
            same slice.
        num_replicas: Ranks that hold this same slice.
        part: For a slice named by rank, the rank's part of its TP group.
        parts: Parts of a slice named by rank (the TP size), or 1 for a
            slice named by KV heads.
    """

    head_start: int = 0
    head_end: int = 1
    total_heads: int = 1
    replica_index: int = 0
    num_replicas: int = 1
    part: int = 0
    parts: int = 1

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

        ``_h<start>-<end>`` names a range of KV heads, and
        ``_tp<parts>_r<part>`` a rank's part at that TP size. The suffix
        names only the slice: for one name to mean one set of bytes, the
        rest of the key must name the chunk's tokens, including the chunk
        length where engines can differ in it (vLLM's keys for several KV
        cache groups add ``_c<chunk tokens>``). Cached, since store paths
        read it per layer.
        """
        suffix = ""
        if self.splits_heads:
            suffix += f"_h{self.head_start}-{self.head_end}"
        if self.parts > 1:
            suffix += f"_tp{self.parts}_r{self.part}"
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

    The usual tensor-parallel split of KV heads, as vLLM's
    ``QKVParallelLinear`` makes it: with at least as many KV heads as ranks
    each rank takes a contiguous ``total_kv_heads // tp_size`` of them; with
    fewer, each head is replicated on ``tp_size // total_kv_heads``
    consecutive ranks and rank r holds head ``r // replicas``. One count
    must divide the other. KV that every rank holds whole, such as an MLA
    latent, is a single head that every rank replicates.

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


def tp_part_shard(tp_rank: int, tp_size: int) -> KVShard:
    """The part that tensor-parallel rank ``tp_rank`` holds of a slice named
    by rank: a Mamba-type state, or attention KV whose heads cannot be told
    apart (see the module docstring).

    Args:
        tp_rank: The worker's rank in its tensor-parallel group.
        tp_size: Tensor-parallel group size.

    Returns:
        The slice ``tp_rank`` holds; the whole KV at TP=1.

    Raises:
        ValueError: On a rank outside the group.
    """
    _check_rank(tp_rank, tp_size)
    return KVShard(part=tp_rank, parts=tp_size)


def tp_part_shards(tp_size: int) -> list[KVShard]:
    """The parts of a slice named by rank that the scheduler checks.

    Args:
        tp_size: Tensor-parallel group size.

    Returns:
        One slice per rank, in rank order; every rank stores its own part.
    """
    return [tp_part_shard(rank, tp_size) for rank in range(tp_size)]
