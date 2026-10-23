# SPDX-License-Identifier: Apache-2.0
"""Geometry of vLLM's paged KV cache tensors.

Answers one question for the connector: given a registered KV tensor, which
axis is which? Everything here is pure — shapes and engine-config numbers in,
a resolved :class:`KVLayout` out — so it is testable without a GPU and holds
no connector state. The connector consumes the result; only
``_vllm_kv_cache_layout`` touches vLLM, lazily.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import torch

try:
    from vllm.logger import init_logger

    logger = init_logger(__name__)
except ImportError:  # keeps this module importable without vLLM (pure tests)
    import logging

    logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class KVLayout:
    """Where each dimension sits in vLLM's paged KV tensor.

    The axis order comes from the attention backend, not from the NHD/HND
    cache layout: vLLM allocates with the NHD/HND permutation and then
    permutes straight back in ``gpu_model_runner``, so ``.shape`` is always
    the backend's ``get_kv_cache_shape()`` and HND changes only strides.
    vLLM 0.29+ goes further and hands every layer the same logical page
    ``[B, H, N, C]`` whatever its physical order. Indexing by shape is
    therefore stride-agnostic; only ``format_name`` needs the physical order.

    Attributes:
        num_blocks: Pages in the paged cache.
        block_size: Tokens per page.
        num_heads: KV heads (post-GQA), or 1 when the head axis is flattened.
        head_size: Width of one head.
        kv_axis: Index of the K/V axis, or None when K and V share the last
            dimension in a way that cannot be split (cpu_attn,
            flash_attn_diffkv, turboquant, MLA).
        block_axis: Index of the ``num_blocks`` axis.
        token_axis: Index of the ``block_size`` axis.
        shape: The tensor shape this layout was detected from. A resolved
            layout describes exactly this shape and no other; ``_layout_fits``
            compares against it, so a hybrid model's differently-shaped layer
            can never be read through another layer's geometry.
        format_name: ``maru_kv_ops.EngineKVFormat`` member, or None when
            this layout has no kernel form and must use per-layer transfers.
            Read only by the packed-kernel path, which walks memory itself.
        kv_packed: True when K and V are the two halves of the last dimension,
            K first, as in vLLM 0.29+'s ``[B, H, N, K|V]`` page. ``kv_axis``
            then names that dimension, which ``_canonical_paged_view`` splits
            into ``(2, head_size)``, so transfers see a real K/V axis.
    """

    num_blocks: int
    block_size: int
    num_heads: int
    head_size: int
    kv_axis: int | None
    block_axis: int
    token_axis: int
    shape: tuple[int, ...]
    format_name: str | None
    kv_packed: bool = False

    @property
    def page_buffer_size(self) -> int:
        """Total token slots in the cache (``num_blocks * block_size``)."""
        return self.num_blocks * self.block_size


def _is_logical_page_layout(kv_layout: str | None) -> bool:
    """Whether ``kv_layout`` is a vLLM 0.29+ ``KVCacheLayout`` name.

    vLLM 0.29 (RFC #42082) resolves one physical layout per model and gives
    every layer a view with the logical shape ``[B, H, N, C]`` — blocks, KV
    heads, tokens, content — in which the name changes only the strides.
    Older vLLM reports "NHD" or "HND" and lets each backend pick its own
    shape. Any other name is a member of the newer enum.
    """
    return kv_layout is not None and kv_layout not in ("NHD", "HND")


def _kv_layout_candidates(
    shape: tuple[int, ...], kv_layout: str | None, head_size: int | None = None
) -> list[KVLayout]:
    """Every reading of ``shape`` that some vLLM attention backend produces.

    In descending priority, which settles only ties the cross-checks in
    ``_detect_kv_layout`` could not. ``kv_layout`` picks ``format_name``
    (no kernel format unless it is "NHD" or "HND"), and a vLLM 0.29+ layout
    name also fixes the rank-4 axis order; ``head_size`` decides whether that
    page's content splits evenly into K and V.
    """
    rank = len(shape)
    out: list[KVLayout] = []
    hnd = kv_layout == "HND"
    # The kernel formats describe the per-backend shapes of vLLM < 0.29 in
    # their NHD or HND memory order. Under any other layout name, or none,
    # the physical order is not one the formats name, so hand the kernel
    # nothing rather than misdescribe memory to it.
    kernel_ok = kv_layout in ("NHD", "HND")

    def add(kv, blk, tok, nh, hs, fmt=None, packed=False):
        out.append(
            KVLayout(
                shape[blk],
                shape[tok],
                nh,
                hs,
                kv,
                blk,
                tok,
                shape,
                fmt if kernel_ok else None,
                packed,
            )
        )

    if _is_logical_page_layout(kv_layout) and rank == 4:
        # vLLM 0.29+: (NB, NH, BS, C) for every attention backend, with K then
        # V in C for dense attention. Splitting C exposes the K/V axis, so the
        # slab holds the same K/V-major bytes as a rank-5 flash cache did.
        # That needs C to be exactly two heads wide: diffkv's uneven split,
        # packed quantized content, and MLA's single latent (no head_size is
        # passed) keep the fused reading, whose halves are a storage
        # convention. Both readings are stride-agnostic. Neither has a kernel
        # format: the *_TWO_HS formats move whole token rows as one plane.
        if head_size is not None and shape[3] == 2 * head_size:
            add(3, 0, 2, shape[1], head_size, packed=True)
        elif shape[3] % 2 == 0:
            add(None, 0, 2, shape[1], shape[3] // 2)
        return out

    if rank == 5:
        # (NB, 2, BS, NH, HS) — flash_attn, flashinfer, flex, triton, aiter.
        # Listed before rocm_attn: when num_blocks == 2 both readings pass
        # every cross-check and the shape cannot separate them, so the tie
        # goes to the far more common backend family.
        if shape[1] == 2:
            fmt = "NL_X_NB_TWO_NH_BS_HS" if hnd else "NL_X_NB_TWO_BS_NH_HS"
            add(1, 0, 2, shape[3], shape[4], fmt)
        # (2, NB, BS, NH, HS) — rocm_attn
        if shape[0] == 2:
            fmt = "NL_X_TWO_NB_NH_BS_HS" if hnd else "NL_X_TWO_NB_BS_NH_HS"
            add(0, 1, 2, shape[3], shape[4], fmt)
    elif rank == 4:
        # Head axes flattened, hence num_heads=1. No shipped backend emits
        # this, but maru read it before layouts were detected at all.
        if shape[0] == 2:
            add(0, 1, 2, 1, shape[3])
        if shape[1] == 2:
            add(1, 0, 2, 1, shape[3])
        if shape[3] % 2 == 0:
            # K and V share the last dimension: (NB, BS, NH, 2*HS) for diffkv
            # and turboquant, (NB, NH, BS, 2*HS) for cpu_attn. No kernel
            # format — maru's fused extract stores each token's row folded to
            # [2, ntok, R/2] (K/V-half-major), while LMCache's *_TWO_HS slabs
            # are token-major rows, and the current device kernels do not
            # dispatch those constants at all (host gather/scatter path only).
            # A token-major fused slab could use them later. The per-layer
            # path never reads that dimension, which is also what keeps
            # diffkv's uneven split and turboquant's packed scales safe.
            add(None, 0, 1, shape[2], shape[3] // 2)
            add(None, 0, 2, shape[1], shape[3] // 2)
    elif rank == 3:
        # (NB, BS, HS) — MLA, one latent vector per token. No kernel format:
        # the fused transfer path stores K/V-half-major bytes, not the
        # token-major rows NL_X_NB_BS_HS describes, and sparse-MLA metadata
        # does not subclass MLACommonMetadata so it would reach the kernel.
        add(None, 0, 1, 1, shape[2])
    return out


def _detect_kv_layout(
    shape: tuple[int, ...],
    block_size: int,
    kv_layout: str | None,
    num_kv_heads: int | None = None,
    head_size: int | None = None,
) -> KVLayout | None:
    """Resolve which axis of a paged KV tensor is which, or None if unknown.

    Picks the candidate from :func:`_kv_layout_candidates` that the engine's
    own numbers agree with. A wrong pick reads a plausible page count rather
    than raising, so the cross-checks are what separate "recognized" from
    "happens to parse".

    Args:
        shape: Shape of one layer's KV tensor.
        block_size: Tokens per page; must land on the token axis. Required.
        kv_layout: What :func:`_vllm_kv_cache_layout` returned: "NHD" or
            "HND" select the kernel format, a vLLM 0.29+ layout name also
            fixes the rank-4 axis order, and None means unknown, which keeps
            every layout off the kernel.
        num_kv_heads: KV heads per rank, when known. Separates the two rank-4
            fused orders. Skip for MLA, which reports 1 either way.
        head_size: Width of one head. A tiebreak, not a requirement, since
            triton_attn appends ``scale_pad`` to it — except that a vLLM 0.29+
            page splits into K and V only when it is exactly two heads wide.

    Returns:
        The resolved layout, or None when no candidate survives. Callers keep
        their existing fallback in that case rather than guessing.
    """
    scored: list[tuple[int, int, KVLayout]] = []
    for priority, cand in enumerate(_kv_layout_candidates(shape, kv_layout, head_size)):
        if cand.block_size != block_size:
            continue
        # Flattened candidates cannot see the head axis, so they skip the head
        # check — and must therefore lose to a fused candidate that passes it.
        flattened = cand.kv_axis is not None and len(shape) == 4 and not cand.kv_packed
        score = 0
        if num_kv_heads is not None and not flattened:
            if cand.num_heads != num_kv_heads:
                continue
            score += 2
        if head_size is not None and cand.head_size == head_size:
            score += 1
        scored.append((-score, priority, cand))
    if not scored:
        return None
    # Sort on the ranking pair only; KVLayout is not orderable.
    scored.sort(key=lambda s: s[:2])
    best = scored[0][2]
    rivals = [
        c
        for neg, _, c in scored[1:]
        if neg == scored[0][0]
        and (c.kv_axis, c.block_axis, c.token_axis, c.kv_packed)
        != (best.kv_axis, best.block_axis, best.token_axis, best.kv_packed)
    ]
    if rivals:
        # Genuinely ambiguous — the readings differ and every cross-check
        # passed both. Priority picks the common backend; log the choice
        # because a wrong pick produces wrong bytes, not an error.
        logger.warning(
            "Maru KV layout ambiguous for shape %s: using kv_axis=%s, "
            "rejected kv_axis=%s",
            shape,
            best.kv_axis,
            [c.kv_axis for c in rivals],
        )
    return best


def _canonical_paged_view(t: torch.Tensor, layout: KVLayout) -> torch.Tensor:
    """Return a view of ``t`` with axes reordered to ``[2?, NB, BS, ...]``.

    Lets extract and inject share one indexing expression. Must stay a view:
    reshape copies when the cache is non-contiguous, which is what HND
    allocation produces, and an index-put would then land in the copy.
    Splitting the last dimension of a packed layout is always a view.
    """
    if layout.kv_packed:
        t = t.unflatten(-1, (2, -1))
    if layout.kv_axis is None:
        return t.movedim((layout.block_axis, layout.token_axis), (0, 1))
    return t.movedim((layout.kv_axis, layout.block_axis, layout.token_axis), (0, 1, 2))


def _mla_pages(t: torch.Tensor) -> torch.Tensor:
    """View an MLA cache as ``[num_blocks, block_size, latent...]``.

    vLLM 0.29+ registers MLA's logical page ``[B, 1, N, C]`` and binds each
    layer to its squeeze, so the registered and the bound tensor differ by
    the unit head axis. Callers index the result per slot instead of
    reshaping it, which would copy a strided page and lose an index-put.
    """
    if t.dim() == 4 and t.shape[1] == 1:
        return t.squeeze(1)
    return t


def _gather_kv(
    paged: torch.Tensor, layout: KVLayout, slots: torch.Tensor
) -> torch.Tensor:
    """Gather ``slots`` of a paged KV tensor as ``[2, ntok, row]``.

    The order Maru stores a layer's KV in, whatever the paged axis order.
    A layout without a K/V axis keeps K and V in one row per token; its row
    is halved instead, a bijection the inject side reverses, which says
    nothing about where K ends (diffkv splits it unevenly).

    Raises:
        RuntimeError: A fused row of odd width, which cannot be halved.
    """
    canon = _canonical_paged_view(paged, layout)
    blocks = slots // layout.block_size
    offsets = slots % layout.block_size
    if layout.kv_axis is not None:
        gathered = canon[:, blocks, offsets]  # [2, ntok, ...]
        return gathered.reshape(2, gathered.shape[1], -1)
    if canon.shape[2:].numel() % 2:
        # No shipped backend produces one, but refuse cleanly rather than
        # fail inside the reshape.
        raise RuntimeError(
            "Maru: cannot extract KV for layout "
            f"{tuple(paged.shape)}; odd fused K/V row width"
        )
    gathered = canon[blocks, offsets]  # [ntok, ...]
    return gathered.reshape(gathered.shape[0], 2, -1).transpose(0, 1)


def _scatter_kv(
    paged: torch.Tensor, layout: KVLayout, slots: torch.Tensor, kv: torch.Tensor
) -> None:
    """Write ``kv`` (``[2, ntok, row]``) into ``slots`` of a paged KV tensor.

    The inverse of :func:`_gather_kv`, through a view, so the writes land in
    ``paged`` itself.

    Raises:
        RuntimeError: A fused row of odd width, which cannot be halved.
    """
    canon = _canonical_paged_view(paged, layout)
    blocks = slots // layout.block_size
    offsets = slots % layout.block_size
    ntok = slots.shape[0]
    if layout.kv_axis is not None:
        canon[:, blocks, offsets] = kv.reshape(2, ntok, *canon.shape[3:])
        return
    if canon.shape[2:].numel() % 2:
        raise RuntimeError(
            "Maru: cannot inject KV for layout "
            f"{tuple(paged.shape)}; odd fused K/V row width"
        )
    # Undo the gather side's halving of each token's row.
    merged = kv.transpose(0, 1).reshape(ntok, -1)
    canon[blocks, offsets] = merged.reshape(ntok, *canon.shape[2:])


_kv_cache_layout_warned = False


def _resolved_kv_cache_layout(cache_config: Any) -> str | None:
    """The ``KVCacheLayout`` name vLLM 0.29+ resolved, or None before 0.29.

    The engine core resolves the layout once and records it on every
    worker's ``cache_config`` before the KV cache is allocated.

    Raises:
        ValueError: On vLLM 0.29+ when the layout is not resolved yet.
    """
    if cache_config is None:
        try:
            from vllm.config import get_current_vllm_config_or_none
        except ImportError:
            return None
        cache_config = getattr(get_current_vllm_config_or_none(), "cache_config", None)
    if not hasattr(cache_config, "get_resolved_kv_cache_layout"):
        return None
    return cache_config.get_resolved_kv_cache_layout().name


def _vllm_kv_cache_layout(cache_config: Any = None) -> str | None:
    """Return vLLM's resolved KV cache layout, or None when it cannot be read.

    vLLM 0.29+ resolves one ``KVCacheLayout`` per model and records its name
    ("LBNHC", "LBHNC", ...) on ``cache_config.kv_cache_layout``; older vLLM
    reports "NHD" or "HND" through ``get_kv_cache_layout``, which asserts
    outside a ``set_current_vllm_config`` context. The physical memory order,
    which no tensor reports, matters only to the packed kernel, so a layout
    that cannot be read keeps the kernel off: assuming NHD would misdescribe
    an HND cache to it. Warns once.

    Args:
        cache_config: The engine's ``CacheConfig``. Without it, the current
            config context is read, which vLLM sets only around its own calls.

    Returns:
        A vLLM 0.29+ layout name, "NHD" or "HND", or None.
    """
    global _kv_cache_layout_warned
    try:
        layout = _resolved_kv_cache_layout(cache_config)
        if layout is not None:
            return layout
        from vllm.v1.attention.backends.utils import get_kv_cache_layout

        return get_kv_cache_layout()
    except Exception as e:
        if not _kv_cache_layout_warned:
            _kv_cache_layout_warned = True
            logger.warning(
                "MaruWorkerConnector: cannot read vLLM's KV cache layout "
                "(%s: %s); KV transfers use per-layer copies, not the "
                "packed kernel.",
                type(e).__name__,
                e,
            )
        return None


def _layout_fits(layout: KVLayout, shape: tuple[int, ...]) -> bool:
    """Whether ``layout`` describes ``shape``: exact shape equality.

    Checking only the named axes was not enough — a rank-5 layout's block,
    token, and K/V positions can all land on matching dims of a rank-4 fused
    tensor with two KV heads, which would read the head axis as K/V. A layout
    is detected from one concrete shape and applies to that shape alone;
    anything else re-detects.
    """
    return tuple(shape) == layout.shape
