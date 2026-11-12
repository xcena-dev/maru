# SPDX-License-Identifier: Apache-2.0
"""vLLM KV cache groups, as the Maru connector sees them.

A hybrid model (Qwen3.5: Gated DeltaNet + full attention; gpt-oss: sliding
window + full attention) reaches the connector as several KV cache groups.
Each group has its own block table and spec. This module turns
``kv_cache_config.kv_cache_groups`` into plain :class:`KVGroup` records,
checks which group combinations the connector supports, decides how much
of a prompt the stored objects of every group can resume, and maps the
worker's registered tensors onto the groups. It holds no connector state and
imports no vLLM class, so it is testable without a GPU.

Storage unit: one Maru object per (chunk, group). vLLM gives every layer the
same bytes per block slot in every group (Mamba state is padded up to it), so
a group's object size is "layers in the group x slot bytes x blocks per
object". Attention-type groups keep one slot per token block of the chunk;
a Mamba group keeps only the state at the chunk's last block. An MLA group
(DeepSeek-V4.1's compressed KV and indexer keys) keeps the chunk's blocks,
copied block by block in the layout vLLM gives them. A group vLLM does not
prefix-cache is not stored. A model with one group keeps the bare chunk key;
with several, each object is keyed ``<chunk key>_c<chunk tokens>_g<group id>``
(see :func:`group_object_key`).
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import torch

from maru_vllm.kv_layout import KVLayout, _detect_block_kv_layout

GROUP_FULL = "full"
GROUP_SLIDING_WINDOW = "sliding_window"
GROUP_MAMBA = "mamba"
GROUP_MLA = "mla"
GROUP_SKIP = "skip"
GROUP_UNSUPPORTED = "unsupported"


@dataclass(frozen=True)
class KVGroup:
    """One vLLM KV cache group.

    Attributes:
        group_id: Index in ``kv_cache_config.kv_cache_groups``; also the
            index of this group's list in vLLM's per-group block ids.
        kind: ``GROUP_FULL``, ``GROUP_SLIDING_WINDOW``, ``GROUP_MAMBA``,
            ``GROUP_MLA``, ``GROUP_SKIP`` or ``GROUP_UNSUPPORTED``. A model
            with one group does not depend on it.
        spec_name: Class name of the group's ``kv_cache_spec``.
        layer_names: Layers in the group, in vLLM's order. The count can
            differ between groups.
        block_size: Tokens per block for this group.
        slot_bytes: Bytes one layer uses for one block (``page_size_bytes``).
            For a ``UniformTypeKVCacheSpecs`` group, its first member's: the
            scheduler sees such a group as that member alone.
        group_page_bytes: Bytes one block holds for all the group's layers
            when vLLM lays them out one after another in the block: a
            ``UniformTypeKVCacheSpecs``'s page (the sum of its members'), or
            a one-layer group's page. 0 when that is not known: the scheduler
            sees a ``UniformTypeKVCacheSpecs`` group as its first member, and
            each layer of a plain group of several keeps a tensor of its own.
        sliding_window: Window in tokens for a sliding-window group, or for
            an MLA group of sliding-window layers (DeepSeek-V4.1's, when vLLM
            prefix-caches them).
        mamba_cache_mode: Mamba state caching mode (``"align"`` etc.).
        state_bytes: For a Mamba group, bytes of one layer's state in one
            block (its state shapes times dtypes), which vLLM pads up to
            ``slot_bytes``. 0 otherwise.
        kv_shape: For an attention group, its KV heads per rank and the head
            size of K and of V. None otherwise.
        replay_tokens: Tokens vLLM recomputes before a prefix hit to rebuild
            this group (DeepSeek-V4.1's bounded-replay sliding window); 0 for
            most groups.
        member_specs: For a ``UniformTypeKVCacheSpecs`` group, its members'
            class names, so messages can name what it wraps. Empty otherwise.
    """

    group_id: int
    kind: str
    spec_name: str
    layer_names: tuple[str, ...]
    block_size: int
    slot_bytes: int
    group_page_bytes: int = 0
    sliding_window: int | None = None
    mamba_cache_mode: str | None = None
    state_bytes: int = 0
    kv_shape: tuple[int, int, int] | None = None
    replay_tokens: int = 0
    member_specs: tuple[str, ...] = ()

    @property
    def windowed(self) -> bool:
        """Whether a hit needs only the chunks this group's sliding window
        covers at the hit's end: a sliding-window group, or an MLA group of
        sliding-window layers."""
        return self.kind == GROUP_SLIDING_WINDOW or (
            self.kind == GROUP_MLA and self.sliding_window is not None
        )

    def stored_blocks(self, chunk_tokens: int) -> int:
        """Blocks of a chunk that this MLA group's object holds.

        Every block of the chunk, or with a sliding window only the last ones
        it reaches back over from the chunk's end: a hit there needs the
        window's other ``sliding_window - 1`` tokens, and vLLM frees the
        blocks below them.

        Args:
            chunk_tokens: Tokens per Maru chunk; a multiple of ``block_size``.
        """
        blocks = chunk_tokens // self.block_size
        if self.sliding_window is None:
            return blocks
        reach = -(-(self.sliding_window - 1) // self.block_size)
        return min(blocks, max(1, reach))

    def object_key(self, chunk_key: str, chunk_tokens: int) -> str:
        """Key of this group's object for one chunk (:func:`group_object_key`).

        An MLA group's key also names its block size. Its objects keep the
        layout vLLM gives each block, which another block size lays out
        differently even when an object's bytes add up to the same total:
        DeepSeek-V4.1 uses 64-token blocks on SM90 and SM120 and 128-token
        ones on SM100. Other groups' objects are laid out token by token.

        Args:
            chunk_key: The chunk's key, with this rank's slice suffix.
            chunk_tokens: Tokens per Maru chunk.
        """
        key = group_object_key(chunk_key, self.group_id, chunk_tokens)
        return f"{key}_b{self.block_size}" if self.kind == GROUP_MLA else key

    def object_bytes(self, chunk_tokens: int) -> int:
        """Bytes of this group's object for one chunk; 0 for a skipped group.

        Args:
            chunk_tokens: Tokens per Maru chunk; a multiple of ``block_size``.
        """
        if self.kind == GROUP_SKIP:
            return 0
        if self.kind == GROUP_MLA:
            return self.stored_blocks(chunk_tokens) * self.group_page_bytes
        blocks = 1 if self.kind == GROUP_MAMBA else chunk_tokens // self.block_size
        return len(self.layer_names) * self.slot_bytes * blocks


# The spec classes the connector is verified against, by exact class name.
# A subclass can change what a block holds: vLLM 0.30's KpoolTailSpec is a
# one-block scratch ring under SlidingWindowSpec, and RSWASpec evicts blocks
# under FullAttentionSpec. Each one is refused until it is checked. Names
# rather than imports keep this working across vLLM versions.
_SUPPORTED_SPECS = {
    "FullAttentionSpec": GROUP_FULL,
    "SlidingWindowSpec": GROUP_SLIDING_WINDOW,
    "MambaSpec": GROUP_MAMBA,
}


def _spec_kind(spec_cls: type) -> str:
    """Group kind of a vLLM KV cache spec class."""
    return _SUPPORTED_SPECS.get(spec_cls.__name__, GROUP_UNSUPPORTED)


# Spec classes whose groups the connector copies block by block, keeping
# vLLM's block layout rather than laying out each token's KV in its own
# format: an MLA spec, alone or in a UniformTypeKVCacheSpecs with others of
# its class (DeepSeek-V4.1's compressed KV and indexer keys, and its sliding
# windows when vLLM prefix-caches them).
_MLA_SPECS = frozenset({"MLAAttentionSpec", "SlidingWindowMLASpec"})


def _group_kind(spec: Any) -> str:
    """Group kind of a vLLM KV cache group's spec.

    A group vLLM does not prefix-cache (a sliding window under bounded
    replay, the KV compressor's circular buffer) is skipped: vLLM recomputes
    it after every hit, its own prefix cache's included. The worker sees a
    group whose layers keep different specs as a ``UniformTypeKVCacheSpecs``
    and the scheduler sees its first member; both are MLA groups when every
    member has one MLA spec class. A group mixing the window class with the
    plain one is refused: a hit needs the one only near its end, the other
    whole.
    """
    if not getattr(spec, "prefix_cacheable", True):
        return GROUP_SKIP
    members = getattr(spec, "kv_cache_specs", None)
    if isinstance(members, dict):
        names = {type(member).__name__ for member in members.values()}
        if len(names) == 1 and names <= _MLA_SPECS:
            return GROUP_MLA
        return GROUP_UNSUPPORTED
    if type(spec).__name__ in _MLA_SPECS:
        return GROUP_MLA
    return _spec_kind(type(spec))


def describe_kv_groups(
    kv_cache_config: Any, default_mamba_cache_mode: str | None = None
) -> list[KVGroup]:
    """Describe the KV cache groups of ``kv_cache_config``.

    Args:
        kv_cache_config: vLLM ``KVCacheConfig``.
        default_mamba_cache_mode: ``cache_config.mamba_cache_mode``, used for
            Mamba specs from vLLM versions that do not carry the mode.

    Returns:
        One :class:`KVGroup` per group, in vLLM's group order.
    """
    groups: list[KVGroup] = []
    for group_id, group in enumerate(kv_cache_config.kv_cache_groups):
        spec = group.kv_cache_spec
        kind = _group_kind(spec)
        members = getattr(spec, "kv_cache_specs", None)
        uniform = isinstance(members, dict) and bool(members)
        layer_spec = next(iter(members.values())) if uniform else spec
        groups.append(
            KVGroup(
                group_id=group_id,
                kind=kind,
                spec_name=type(spec).__name__,
                layer_names=tuple(group.layer_names),
                block_size=int(spec.block_size),
                slot_bytes=int(layer_spec.page_size_bytes),
                group_page_bytes=(
                    int(spec.page_size_bytes)
                    if uniform or len(group.layer_names) == 1
                    else 0
                ),
                sliding_window=getattr(layer_spec, "sliding_window", None),
                mamba_cache_mode=(
                    getattr(spec, "mamba_cache_mode", None) or default_mamba_cache_mode
                    if kind == GROUP_MAMBA
                    else None
                ),
                state_bytes=(
                    sum(
                        math.prod(shape) * dtype.itemsize
                        for shape, dtype in zip(spec.shapes, spec.dtypes, strict=True)
                    )
                    if kind == GROUP_MAMBA
                    else 0
                ),
                kv_shape=(
                    (
                        int(spec.num_kv_heads),
                        int(spec.head_size),
                        int(getattr(spec, "head_size_v", None) or spec.head_size),
                    )
                    if kind in (GROUP_FULL, GROUP_SLIDING_WINDOW)
                    else None
                ),
                replay_tokens=int(getattr(spec, "prefix_replay_tokens", 0) or 0),
                member_specs=_member_specs(spec),
            )
        )
    return groups


def _member_specs(spec: Any) -> tuple[str, ...]:
    """Class names of a ``UniformTypeKVCacheSpecs``'s members; () otherwise."""
    members = getattr(spec, "kv_cache_specs", None)
    if not isinstance(members, dict):
        return ()
    return tuple(sorted({type(member).__name__ for member in members.values()}))


def _group_label(group: KVGroup) -> str:
    """The group's spec class, with what it wraps for a UniformType group."""
    if group.member_specs:
        return f"{group.spec_name} of {', '.join(group.member_specs)}"
    return group.spec_name


def group_object_key(chunk_key: str, group_id: int, chunk_tokens: int) -> str:
    """Key of the (chunk, group) object for a model with several groups.

    A chunk key hashes the tokens from the prompt's start to the chunk's
    end, so it does not say where the chunk starts, and the chunk length
    does. It varies between engines: with a Mamba group a chunk is one vLLM
    block, sized from the per-rank Mamba state and attention page, so
    Qwen3.5-0.8B has 544-token chunks at TP=2 and 272-token ones at TP=4,
    where rank 0 holds the same KV head and names it alike.
    """
    return f"{chunk_key}_c{chunk_tokens}_g{group_id}"


def window_chunks(group: KVGroup, end: int, chunk_tokens: int) -> range:
    """Chunks a sliding-window group needs to resume at token ``end``.

    The token at ``end`` attends to the ``sliding_window`` tokens ending at
    itself, so the cache must hold tokens ``[end - window + 1, end)``.
    """
    assert group.sliding_window is not None
    first = max(0, end - group.sliding_window + 1) // chunk_tokens
    return range(first, -(-end // chunk_tokens))


def hit_chunks(
    groups: list[KVGroup],
    num_chunks: int,
    chunk_tokens: int,
    has_object: Callable[[int, KVGroup], bool],
) -> tuple[int, int]:
    """How many chunks a request can load when several groups must agree.

    A hit of ``k`` chunks resumes at token ``k * chunk_tokens`` and needs:

    - every full-attention or MLA group: chunks ``0 .. k-1``;
    - every sliding-window group, and every MLA group of sliding-window
      layers: the chunks its window covers there;
    - every Mamba group: chunk ``k-1``, whose object is the state there;
    - a skipped group: nothing, since vLLM rebuilds it.

    A Mamba state is stored only for a chunk that ended a scheduler step, so
    the hit is the longest full-attention prefix cut back to such a chunk.

    Args:
        groups: The model's KV cache groups.
        num_chunks: Chunks that may hit, already short of the prompt's last
            token.
        chunk_tokens: Tokens per chunk.
        has_object: Whether the object of (chunk index, group) is stored.

    Returns:
        ``(hit, prefix)``: the chunks to load, and the chunks the
        full-attention groups allow (all ``num_chunks`` without such a
        group). ``hit < prefix`` when a later chunk lacks a Mamba state or a
        sliding window.
    """
    full = [g for g in groups if g.kind in (GROUP_FULL, GROUP_MLA) and not g.windowed]
    prefix = next(
        (i for i in range(num_chunks) if not all(has_object(i, g) for g in full)),
        num_chunks,
    )

    def resumable(k: int, g: KVGroup) -> bool:
        if g.kind == GROUP_MAMBA:
            return has_object(k - 1, g)
        if g.windowed:
            window = window_chunks(g, k * chunk_tokens, chunk_tokens)
            return all(has_object(i, g) for i in window)
        return True

    for k in range(prefix, 0, -1):
        if all(resumable(k, g) for g in groups):
            return k, prefix
    return 0, prefix


def chunk_unit(groups: list[KVGroup], block_size: int) -> int:
    """Tokens a chunk is a multiple of, so it holds whole blocks of each group.

    vLLM schedules a model with several groups in the least common multiple
    of their block sizes, while ``cache_config.block_size`` is only the
    smallest block size it prefix-caches: 32 for DeepSeek-V4.1's sliding
    windows when bounded replay is off, beside its MLA group's 64.

    Args:
        groups: The model's KV cache groups.
        block_size: ``cache_config.block_size``.
    """
    return math.lcm(block_size, *(g.block_size for g in groups))


def chunk_tokens_for_groups(
    groups: list[KVGroup], chunk_tokens: int, block_size: int
) -> int:
    """Chunk size the groups allow.

    A Mamba group keeps a state snapshot only at a block end, and in align
    mode only at the block where a scheduler step ends. A chunk can carry a
    state only if it ends on a block end, and any block end can be a step
    end, so chunk = block. Attention-only groups accept any multiple of the
    block size.

    Args:
        groups: The model's KV cache groups.
        chunk_tokens: Configured chunk size, already a multiple of
            ``chunk_unit``.
        block_size: vLLM block size in tokens.
    """
    if any(g.kind == GROUP_MAMBA for g in groups):
        return block_size
    return chunk_tokens


def one_group_hint(groups: list[KVGroup]) -> str:
    """How to run a model without a Mamba group as one KV cache group instead.

    vLLM can merge sliding-window layers into full attention when its hybrid
    KV cache manager is off; Mamba layers it cannot merge, nor DeepSeek-V4.1's
    skipped and MLA groups. The attention backend must then take layers with
    different windows in one group, which FlashInfer (vLLM 0.30's default on
    Blackwell) does not.
    """
    if any(g.kind in (GROUP_MAMBA, GROUP_SKIP, GROUP_MLA) for g in groups):
        return ""
    hint = (
        "; or start vLLM with --disable-hybrid-kv-cache-manager to run the "
        "model as one KV cache group"
    )
    if any(g.kind == GROUP_SLIDING_WINDOW for g in groups):
        hint += (
            " (FlashInfer refuses one group whose layers use different sliding "
            "windows; --attention-backend TRITON_ATTN accepts it)"
        )
    return hint


def check_multi_group_support(
    groups: list[KVGroup],
    *,
    block_size: int,
    use_layerwise: bool,
    speculative: bool,
    inline_loads: bool,
) -> None:
    """Reject group combinations the connector cannot store correctly.

    A model with one group passes (``check_one_group_support`` checks it).
    Several groups are supported in chunkwise (packed) format with full
    attention, sliding window, Mamba (``align``) and MLA groups whose
    attention groups share one KV shape, but not a Mamba group beside an MLA
    group, with parked loads, without speculative decoding; in layerwise
    format only when every stored group is an MLA group (DeepSeek-V4.1).
    Every group uses vLLM's block size, but an MLA group may use another
    (DeepSeek-V4.1's 64-token MLA blocks beside its 32-token windows); the
    chunk then holds whole blocks of each (``chunk_unit``). Groups vLLM does
    not prefix-cache are skipped and take no part in the checks. The CPU and
    mixed backends reject several groups, and the remote_cxl backend MLA
    models, on their own, in ``MaruKVConnector``.

    Args:
        groups: The model's KV cache groups.
        block_size: ``cache_config.block_size``, which the connector uses.
        use_layerwise: ``maru_use_layerwise``.
        speculative: Whether speculative decoding is configured.
        inline_loads: Whether loads run inline (``maru_async_load`` set to
            false; ``MaruKVConnector`` turns it on for several groups unless
            set).

    Raises:
        ValueError: With the reason and the setting to change.
    """
    if len(groups) <= 1:
        return
    stored = [g for g in groups if g.kind != GROUP_SKIP]
    if not stored:
        raise ValueError(
            "MaruKVConnector stores only the KV cache groups vLLM prefix-caches, "
            f"and this model has none{one_group_hint(groups)}"
        )
    unsupported = [g for g in stored if g.kind == GROUP_UNSUPPORTED]
    if unsupported:
        raise ValueError(
            "MaruKVConnector does not support KV cache groups of type "
            f"{', '.join(sorted({_group_label(g) for g in unsupported}))} "
            "alongside other groups; supported are "
            f"{', '.join([*_SUPPORTED_SPECS, *sorted(_MLA_SPECS)])}"
            f"{one_group_hint(groups)}"
        )
    shaped = [g for g in stored if g.kv_shape is not None]
    if len({g.kv_shape for g in shaped}) > 1:
        # MiMo-V2 and Inkling give their sliding-window layers their own KV
        # heads and head size. vLLM then also gives those groups different
        # block sizes, to keep one page size.
        raise ValueError(
            "MaruKVConnector does not support attention groups whose KV heads "
            "differ in count or size; it stores every group with one head shape "
            "and one block size ("
            + "; ".join(f"group {g.group_id}: {_kv_shape_text(g)}" for g in shaped)
            + ")"
        )
    if use_layerwise and any(g.kind != GROUP_MLA for g in stored):
        raise ValueError(
            "MaruKVConnector supports models with several KV cache groups in "
            "layerwise format only when every group it stores is an MLA group "
            "(DeepSeek-V4.1); otherwise only in chunkwise format. Set "
            f"maru_use_layerwise=false{one_group_hint(groups)}"
        )
    if speculative:
        raise ValueError(
            "MaruKVConnector does not support speculative decoding with models "
            f"that have several KV cache groups{one_group_hint(groups)}"
        )
    kinds = {g.kind for g in stored}
    if {GROUP_MAMBA, GROUP_MLA} <= kinds:
        # A Mamba group pins the chunk to one block (chunk_tokens_for_groups),
        # which holds no whole block of an MLA group with larger blocks, and
        # the pair has not been run with equal ones either.
        raise ValueError(
            "MaruKVConnector does not support a model with both a Mamba-type "
            "and an MLA KV cache group, such as GLM-5.3-Flash"
        )
    for g in stored:
        if g.kind == GROUP_MAMBA and g.mamba_cache_mode != "align":
            # vLLM 0.30 sets "none" whenever prefix caching is off, even over
            # --mamba-cache-mode align, and defaults to "align" when it is on.
            raise ValueError(
                "MaruKVConnector needs Mamba state snapshots at block ends: "
                "keep vLLM's prefix caching on and its Mamba cache mode "
                "'align', the default with prefix caching "
                f"(--mamba-cache-mode align; got {g.mamba_cache_mode!r})"
            )
    # An MLA group is copied block by block, and a chunk (a multiple of
    # chunk_unit) holds whole blocks of it whatever its block size.
    if any(g.kind != GROUP_MLA and g.block_size != block_size for g in stored):
        raise ValueError(
            "MaruKVConnector needs every KV cache group other than an MLA group "
            f"to use the block size {block_size} (got "
            f"{sorted({g.block_size for g in stored})}){one_group_hint(groups)}"
        )
    if inline_loads:
        # A failed inline load is reported by its blocks, which vLLM 0.31+
        # refuses with several groups: block ids are unique only within a
        # group, and its per-request report takes only parked requests. vLLM
        # 0.30 takes it, but under "recompute" its V2 model runner does not
        # rewind its device-side state (vllm-project/vllm#49250).
        raise ValueError(
            "MaruKVConnector loads a model with several KV cache groups only "
            "between steps (maru_async_load=true, its default for such a "
            "model): vLLM 0.31 and later stop the engine on a failed inline "
            "load of such a model (xcena-dev/maru#126), and vLLM 0.30 "
            "recomputes one into wrong output under kv_load_failure_policy="
            "'recompute' (vllm-project/vllm#49250). Remove "
            f"maru_async_load=false{one_group_hint(groups)}"
        )


def _kv_shape_text(group: KVGroup) -> str:
    assert group.kv_shape is not None
    heads, k_size, v_size = group.kv_shape
    sizes = f"{k_size}" if v_size == k_size else f"{k_size} (V {v_size})"
    return f"{heads} KV heads per rank of size {sizes}"


def check_one_group_support(groups: list[KVGroup]) -> None:
    """Reject a single group whose layers keep different KV cache specs.

    vLLM puts layers of one kind in one group even when their KV differs in
    shape, as ``UniformTypeKVCacheSpecs``: DeepSeek-V3.2's MLA latent and its
    indexer cache, or, with the hybrid KV cache manager off, sliding-window
    and full layers whose KV heads differ. The connector handles every layer
    of a one-group model in the first layer's shape.

    Raises:
        ValueError: For such a group.
    """
    if len(groups) == 1 and groups[0].spec_name == "UniformTypeKVCacheSpecs":
        raise ValueError(
            "MaruKVConnector does not support a KV cache group whose layers "
            "keep different KV cache specs (vLLM's UniformTypeKVCacheSpecs), "
            "such as an MLA latent next to an indexer cache; it handles every "
            "layer of a one-group model in the first layer's shape"
        )


@dataclass(frozen=True, eq=False)
class GroupCache:
    """One group's registered GPU tensors, in the group's layer order.

    A layer's index in ``tensors`` is its position in the group's Maru
    object, and the same index in ``group.layer_names`` names it.

    Attributes:
        group: The group.
        tensors: For an attention group, each layer's paged KV tensor; all
            share one shape, dtype and strides. For a Mamba group, each
            layer's state as a ``uint8`` view of shape ``[num_blocks,
            state_bytes]``: row ``b`` is block ``b``'s state. For an MLA
            group, one ``uint8`` view of shape ``[num_blocks,
            group_page_bytes]`` over every layer at once: row ``b`` is block
            ``b``.
        layout: Paged KV layout of an attention group; None for Mamba and
            MLA groups.
        spans: For an MLA group, each layer's ``(offset, bytes)`` inside a
            row of ``tensors[0]``, in the group's layer order: the layerwise
            format stores one object per span. Empty otherwise.
    """

    group: KVGroup
    tensors: tuple[torch.Tensor, ...]
    layout: KVLayout | None = None
    spans: tuple[tuple[int, int], ...] = ()


def state_view(name: str, kv: Any, slot_bytes: int, state_bytes: int) -> torch.Tensor:
    """A Mamba layer's per-block state as ``uint8 [num_blocks, state_bytes]``.

    vLLM lays one layer's state for one block out contiguously at the start
    of that block's slot, and registers it either as a list of state tensors
    (``[conv_state, ssm_state]``, vLLM 0.23) or as one int8 tensor of the
    whole state (0.30). Both become the same byte view, which aliases vLLM's
    memory: writing a row writes that block's state.

    Args:
        name: Layer name, for errors.
        kv: The layer's registered value.
        slot_bytes: Bytes between consecutive blocks.
        state_bytes: Bytes of one block's state, from the group's spec.

    Raises:
        ValueError: The tensors are not one contiguous state per block slot,
            or not the whole state the spec describes.
    """
    tensors = list(kv) if isinstance(kv, (list, tuple)) else [kv]
    first = tensors[0]
    base = first.data_ptr()
    registered = 0
    for t in tensors:
        if (
            t.stride(0) * t.element_size() != slot_bytes
            or t.data_ptr() != base + registered
            or not t[0].is_contiguous()
        ):
            raise ValueError(
                f"MaruKVConnector cannot read the Mamba state of {name}: "
                f"expected one contiguous state per {slot_bytes}-byte block "
                f"slot (got shape {tuple(t.shape)}, stride {t.stride()})"
            )
        registered += t[0].numel() * t.element_size()
    if registered != state_bytes:
        raise ValueError(
            f"MaruKVConnector: {name} registers {registered} B of Mamba state "
            f"per block, but its spec describes {state_bytes} B"
        )
    storage = first.untyped_storage()
    view = torch.empty(0, dtype=torch.uint8, device=first.device)
    view.set_(
        storage,
        base - storage.data_ptr(),
        (first.shape[0], state_bytes),
        (slot_bytes, 1),
    )
    return view


def group_block_view(
    group: KVGroup, kv_caches: dict[str, Any]
) -> tuple[torch.Tensor, tuple[tuple[int, int], ...]]:
    """An MLA group's blocks as ``uint8 [num_blocks, group_page_bytes]``.

    vLLM can keep every KV cache group in one buffer that gives each block id
    one range of bytes, each group laying its layers out one after another
    from the start of that range (DeepSeek-V4.1's block holds its compressed
    KV and indexer keys side by side this way). Row ``b`` of the view is
    block ``b``'s bytes for this group. It aliases vLLM's memory: writing a
    row writes that block of every layer in the group.

    Args:
        group: An MLA group; ``group_page_bytes`` is its bytes per block.
        kv_caches: ``register_kv_caches``' argument, holding every layer of
            the group.

    Returns:
        The view, and each layer's ``(offset, bytes)`` inside a row, in the
        group's layer order.

    Raises:
        ValueError: The layers keep a tensor each (``group_page_bytes`` is
            0), do not share one buffer and block stride, or do not fit in
            the first ``group_page_bytes`` bytes of each block.
    """
    page = group.group_page_bytes
    if not page:
        raise ValueError(
            f"MaruKVConnector: the layers of KV cache group {group.group_id} "
            "keep a tensor each; Maru copies an MLA group only when vLLM lays "
            "its layers out one after another in each block"
        )
    tensors = [kv_caches[n] for n in group.layer_names]
    first = tensors[0]
    storage = first.untyped_storage()
    base = storage.data_ptr()
    if any(t.untyped_storage().data_ptr() != base for t in tensors):
        raise ValueError(
            f"MaruKVConnector: the layers of KV cache group {group.group_id} "
            "are not in one buffer; Maru copies each block of an MLA group "
            "as one range of bytes"
        )
    stride = first.stride(0) * first.element_size()
    geometry = {(t.shape[0], t.stride(0) * t.element_size()) for t in tensors}
    if len(geometry) > 1:
        raise ValueError(
            f"MaruKVConnector: the layers of KV cache group {group.group_id} "
            f"differ in block count or block stride (got {sorted(geometry)})"
        )
    offsets = [t.data_ptr() - base for t in tensors]
    ends = [
        offset + t[0].numel() * t.element_size()
        for offset, t in zip(offsets, tensors, strict=True)
    ]
    start = min(offsets)
    if (
        any(
            not 0 <= offset < stride or not t[0].is_contiguous()
            for offset, t in zip(offsets, tensors, strict=True)
        )
        or max(ends) - start > page
        or start + page > stride
    ):
        raise ValueError(
            f"MaruKVConnector: the layers of KV cache group {group.group_id} "
            f"do not fit in the first {page} bytes of each {stride}-byte block"
        )
    view = torch.empty(0, dtype=torch.uint8, device=first.device)
    view.set_(storage, start, (first.shape[0], page), (stride, 1))
    spans = tuple(
        (offset - start, end - offset)
        for offset, end in zip(offsets, ends, strict=True)
    )
    return view, spans


def group_caches(
    groups: list[KVGroup],
    kv_caches: dict[str, Any],
    *,
    kv_layout: str | None,
    num_kv_heads: int | None,
    head_size: int | None,
    num_blocks: int | None = None,
) -> list[GroupCache]:
    """Map the worker's registered tensors onto the KV cache groups.

    Every layer a stored group lists must be registered. Layers that no group
    lists (none for the supported models) are left alone, and so are the
    groups vLLM does not prefix-cache: they get no cache, so callers find a
    group's cache by ``cache.group.group_id``, not by its position. An MLA
    group gets one view of its blocks (:func:`group_block_view`). An
    attention group's layout comes from its first layer, and every layer of
    the group must share its shape, dtype and strides. When the attention
    backend takes smaller blocks, the layout describes the kernel blocks vLLM
    splits each block into (see ``_detect_block_kv_layout``). A block of KV
    may not exceed the group's ``slot_bytes``, which the object sizes are
    computed from; it is smaller when vLLM pads the page.

    Args:
        groups: The model's KV cache groups.
        kv_caches: ``register_kv_caches``' argument.
        kv_layout: What ``_vllm_kv_cache_layout`` returned: "NHD"/"HND", a
            vLLM 0.29+ layout name, or None when unknown.
        num_kv_heads: KV heads per rank. Required for attention groups: a
            fused layout read without it can take the head axis for K/V.
        head_size: Width of one head, when known.
        num_blocks: vLLM's block count, when known; a tensor in kernel blocks
            must hold ``k`` of them for each block.

    Raises:
        ValueError: A layer is missing, or its tensor does not match the
            group's spec.
    """
    caches = []
    for g in groups:
        if g.kind == GROUP_SKIP:
            continue
        missing = [n for n in g.layer_names if n not in kv_caches]
        if missing:
            raise ValueError(
                f"MaruKVConnector: KV cache group {g.group_id} lists layers "
                f"with no registered tensor: {missing}"
            )
        if g.kind == GROUP_MAMBA:
            views = tuple(
                state_view(n, kv_caches[n], g.slot_bytes, g.state_bytes)
                for n in g.layer_names
            )
            caches.append(GroupCache(g, views))
            continue
        if g.kind == GROUP_MLA:
            view, spans = group_block_view(g, kv_caches)
            caches.append(GroupCache(g, (view,), spans=spans))
            continue
        if num_kv_heads is None:
            raise ValueError(
                "MaruKVConnector needs the model's KV head count to read the "
                f"KV layout of group {g.group_id}"
            )
        tensors = tuple(kv_caches[n] for n in g.layer_names)
        first = tensors[0]
        shape = tuple(first.shape)
        if any(
            t.shape != first.shape
            or t.dtype != first.dtype
            or t.stride() != first.stride()
            for t in tensors
        ):
            raise ValueError(
                f"MaruKVConnector: layers of KV cache group {g.group_id} differ "
                "in shape, dtype or strides"
            )
        layout = _detect_block_kv_layout(
            shape, g.block_size, kv_layout, num_kv_heads, head_size, num_blocks
        )
        if layout is None:
            raise ValueError(
                f"MaruKVConnector cannot read the KV layout {shape} of group "
                f"{g.group_id}"
            )
        # Per token, so that a block split into kernel blocks counts them all.
        block_bytes = (
            first.numel()
            * first.element_size()
            * g.block_size
            // layout.page_buffer_size
        )
        if block_bytes > g.slot_bytes:
            raise ValueError(
                f"MaruKVConnector: a block of group {g.group_id} holds "
                f"{block_bytes} B on the GPU, more than its spec's {g.slot_bytes} B"
            )
        caches.append(GroupCache(g, tensors, layout))
    return caches
