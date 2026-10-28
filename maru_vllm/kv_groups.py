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
a Mamba group keeps only the state at the chunk's last block. A model with
one group keeps the bare chunk key; with several, each object is keyed
``<chunk key>_g<group id>``.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import torch

from maru_vllm.kv_layout import KVLayout, _detect_kv_layout

GROUP_FULL = "full"
GROUP_SLIDING_WINDOW = "sliding_window"
GROUP_MAMBA = "mamba"
GROUP_UNSUPPORTED = "unsupported"


@dataclass(frozen=True)
class KVGroup:
    """One vLLM KV cache group.

    Attributes:
        group_id: Index in ``kv_cache_config.kv_cache_groups``; also the
            index of this group's list in vLLM's per-group block ids.
        kind: ``GROUP_FULL``, ``GROUP_SLIDING_WINDOW``, ``GROUP_MAMBA`` or
            ``GROUP_UNSUPPORTED``. A model with one group does not depend on
            it.
        spec_name: Class name of the group's ``kv_cache_spec``.
        layer_names: Layers in the group, in vLLM's order. The count can
            differ between groups.
        block_size: Tokens per block for this group.
        slot_bytes: Bytes one layer uses for one block (``page_size_bytes``).
        sliding_window: Window in tokens for a sliding-window group.
        mamba_cache_mode: Mamba state caching mode (``"align"`` etc.).
        state_bytes: For a Mamba group, bytes of one layer's state in one
            block (its state shapes times dtypes), which vLLM pads up to
            ``slot_bytes``. 0 otherwise.
    """

    group_id: int
    kind: str
    spec_name: str
    layer_names: tuple[str, ...]
    block_size: int
    slot_bytes: int
    sliding_window: int | None = None
    mamba_cache_mode: str | None = None
    state_bytes: int = 0

    def object_bytes(self, chunk_tokens: int) -> int:
        """Bytes of this group's object for one chunk.

        Args:
            chunk_tokens: Tokens per Maru chunk; a multiple of ``block_size``.
        """
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
        kind = _spec_kind(type(spec))
        groups.append(
            KVGroup(
                group_id=group_id,
                kind=kind,
                spec_name=type(spec).__name__,
                layer_names=tuple(group.layer_names),
                block_size=int(spec.block_size),
                slot_bytes=int(spec.page_size_bytes),
                sliding_window=getattr(spec, "sliding_window", None),
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
            )
        )
    return groups


def group_object_key(chunk_key: str, group_id: int) -> str:
    """Key of the (chunk, group) object for a model with several groups."""
    return f"{chunk_key}_g{group_id}"


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

    - every full-attention group: chunks ``0 .. k-1``;
    - every sliding-window group: the chunks its window covers there;
    - every Mamba group: chunk ``k-1``, whose object is the state there.

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
    full = [g for g in groups if g.kind == GROUP_FULL]
    prefix = next(
        (i for i in range(num_chunks) if not all(has_object(i, g) for g in full)),
        num_chunks,
    )

    def resumable(k: int, g: KVGroup) -> bool:
        if g.kind == GROUP_MAMBA:
            return has_object(k - 1, g)
        if g.kind == GROUP_SLIDING_WINDOW:
            window = window_chunks(g, k * chunk_tokens, chunk_tokens)
            return all(has_object(i, g) for i in window)
        return True

    for k in range(prefix, 0, -1):
        if all(resumable(k, g) for g in groups):
            return k, prefix
    return 0, prefix


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
        chunk_tokens: Configured chunk size, already a multiple of block_size.
        block_size: vLLM block size in tokens.
    """
    if any(g.kind == GROUP_MAMBA for g in groups):
        return block_size
    return chunk_tokens


def one_group_hint(groups: list[KVGroup]) -> str:
    """How to run a model without a Mamba group as one KV cache group instead.

    vLLM can merge sliding-window layers into full attention when its hybrid
    KV cache manager is off; Mamba layers it cannot merge. The attention
    backend must then take layers with different windows in one group, which
    FlashInfer (vLLM 0.30's default on Blackwell) does not.
    """
    if any(g.kind == GROUP_MAMBA for g in groups):
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
    tp_size: int = 1,
    total_kv_heads: int = 1,
) -> None:
    """Reject group combinations the connector cannot store correctly.

    A model with one group is always accepted. Several groups are supported
    in chunkwise (packed) format with full attention, sliding window and
    Mamba (``align``) groups, with parked loads, without speculative
    decoding, and with several attention groups only at a tensor parallel
    size up to the model's KV heads. The CPU, mixed and remote_cxl backends
    reject several groups on their own, in ``MaruKVConnector``.

    Args:
        groups: The model's KV cache groups.
        block_size: ``cache_config.block_size``, which the connector uses.
        use_layerwise: ``maru_use_layerwise``.
        speculative: Whether speculative decoding is configured.
        inline_loads: Whether loads run inline (``maru_async_load`` set to
            false; ``MaruKVConnector`` turns it on for several groups unless
            set).
        tp_size: vLLM's tensor parallel size.
        total_kv_heads: The model's KV heads (1 at TP=1, where they do not
            matter).

    Raises:
        ValueError: With the reason and the setting to change.
    """
    if len(groups) <= 1:
        return
    unsupported = sorted({g.spec_name for g in groups if g.kind == GROUP_UNSUPPORTED})
    if unsupported:
        raise ValueError(
            "MaruKVConnector does not support KV cache groups of type "
            f"{', '.join(unsupported)} alongside other groups; supported are "
            f"{', '.join(_SUPPORTED_SPECS)}{one_group_hint(groups)}"
        )
    attention = [g for g in groups if g.kind in (GROUP_FULL, GROUP_SLIDING_WINDOW)]
    if len(attention) > 1 and tp_size > total_kv_heads:
        # Every attention group's keys name its heads by the model's KV head
        # count (maru_vllm.kv_shard). With more ranks than that, vLLM
        # replicates each group's heads by the group's own count, which can
        # differ: a rank holding distinct heads of a group with more of them
        # would be taken for a replica, skip its store and load another
        # rank's heads.
        raise ValueError(
            "MaruKVConnector cannot name the KV heads of several attention "
            f"groups at tensor parallel size {tp_size}, above the model's "
            f"{total_kv_heads} KV heads; use a tensor parallel size of at most "
            f"{total_kv_heads}"
        )
    if use_layerwise:
        raise ValueError(
            "MaruKVConnector supports models with several KV cache groups only "
            f"in chunkwise format; set maru_use_layerwise=false{one_group_hint(groups)}"
        )
    if speculative:
        raise ValueError(
            "MaruKVConnector does not support speculative decoding with models "
            f"that have several KV cache groups{one_group_hint(groups)}"
        )
    for g in groups:
        if g.kind == GROUP_MAMBA and g.mamba_cache_mode != "align":
            # vLLM 0.30 sets "none" whenever prefix caching is off, even over
            # --mamba-cache-mode align, and defaults to "align" when it is on.
            raise ValueError(
                "MaruKVConnector needs Mamba state snapshots at block ends: "
                "keep vLLM's prefix caching on and its Mamba cache mode "
                "'align', the default with prefix caching "
                f"(--mamba-cache-mode align; got {g.mamba_cache_mode!r})"
            )
    group_block_sizes = sorted({g.block_size for g in groups})
    if group_block_sizes != [block_size]:
        raise ValueError(
            "MaruKVConnector needs every KV cache group to use the block size "
            f"{block_size} (got {group_block_sizes}){one_group_hint(groups)}"
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
            state_bytes]``: row ``b`` is block ``b``'s state.
        layout: Paged KV layout of an attention group; None for Mamba.
    """

    group: KVGroup
    tensors: tuple[torch.Tensor, ...]
    layout: KVLayout | None = None


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


def _kernel_block_size(
    shape: tuple[int, ...],
    block_size: int,
    kv_layout: str,
    num_kv_heads: int,
    head_size: int | None,
) -> int | None:
    """Kernel block size, when vLLM split each block for the attention kernel.

    A backend that cannot take the group's block size whole (FlashInfer, or
    FlashAttention 4 with head size 256) gets each block as several
    consecutive kernel blocks, so the tensor's token axis is a divisor of the
    block size.
    """
    for kernel in range(block_size // 2, 0, -1):
        if block_size % kernel == 0 and _detect_kv_layout(
            shape, kernel, kv_layout, num_kv_heads, head_size
        ):
            return kernel
    return None


def group_caches(
    groups: list[KVGroup],
    kv_caches: dict[str, Any],
    *,
    kv_layout: str | None,
    num_kv_heads: int | None,
    head_size: int | None,
) -> list[GroupCache]:
    """Map the worker's registered tensors onto the KV cache groups.

    Every layer a group lists must be registered. Layers that no group lists
    (none for the supported models) are left alone. An attention group's
    layout comes from its first layer, and every layer of the group must
    share its shape, dtype and strides. A block of KV may not exceed the
    group's ``slot_bytes``, which the object sizes are computed from; it is
    smaller when vLLM pads the page.

    Args:
        groups: The model's KV cache groups.
        kv_caches: ``register_kv_caches``' argument.
        kv_layout: What ``_vllm_kv_cache_layout`` returned: "NHD"/"HND", a
            vLLM 0.29+ layout name, or None when unknown.
        num_kv_heads: KV heads per rank. Required for attention groups: a
            fused layout read without it can take the head axis for K/V.
        head_size: Width of one head, when known.

    Raises:
        ValueError: A layer is missing, its tensor does not match the
            group's spec, or the attention kernel split the blocks.
    """
    caches = []
    for g in groups:
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
        layout = _detect_kv_layout(
            shape, g.block_size, kv_layout, num_kv_heads, head_size
        )
        if layout is None:
            kernel = _kernel_block_size(
                shape, g.block_size, kv_layout, num_kv_heads, head_size
            )
            if kernel is not None:
                raise ValueError(
                    "MaruKVConnector does not support attention kernels that "
                    f"split blocks: group {g.group_id} uses {g.block_size}-token "
                    f"blocks, laid out as {kernel}-token kernel blocks. Pick an "
                    "attention backend that keeps whole blocks, such as "
                    "TRITON_ATTN, or FLASH_ATTN except FlashAttention 4 with "
                    "head size 256 (compute capability 10.x/11.x)."
                )
            raise ValueError(
                f"MaruKVConnector cannot read the KV layout {shape} of group "
                f"{g.group_id}"
            )
        block_bytes = first.numel() * first.element_size() // layout.num_blocks
        if block_bytes > g.slot_bytes:
            raise ValueError(
                f"MaruKVConnector: a block of group {g.group_id} holds "
                f"{block_bytes} B on the GPU, more than its spec's {g.slot_bytes} B"
            )
        caches.append(GroupCache(g, tensors, layout))
    return caches
