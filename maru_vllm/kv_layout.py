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
    Indexing by shape is therefore stride-agnostic; only ``format_name``
    needs NHD/HND.

    Attributes:
        num_blocks: Pages in the paged cache.
        block_size: Tokens per page.
        num_heads: KV heads (post-GQA), or 1 when the head axis is flattened.
        head_size: Width of one head.
        kv_axis: Index of the K/V axis, or None when K and V share the last
            dimension (cpu_attn, flash_attn_diffkv, turboquant, MLA).
        block_axis: Index of the ``num_blocks`` axis.
        token_axis: Index of the ``block_size`` axis.
        shape: The tensor shape this layout was detected from. A resolved
            layout describes exactly this shape and no other; ``_layout_fits``
            compares against it, so a hybrid model's differently-shaped layer
            can never be read through another layer's geometry.
        format_name: ``lmcache.c_ops.EngineKVFormat`` member, or None when
            this layout has no kernel form and must use per-layer transfers.
            Read only by the packed-kernel path, which walks memory itself.
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

    @property
    def page_buffer_size(self) -> int:
        """Total token slots in the cache (``num_blocks * block_size``)."""
        return self.num_blocks * self.block_size


def _kv_layout_candidates(shape: tuple[int, ...], hnd: bool) -> list[KVLayout]:
    """Every reading of ``shape`` that some vLLM attention backend produces.

    In descending priority, which settles only ties the cross-checks in
    ``_detect_kv_layout`` could not. ``hnd`` picks ``format_name``, no axis.
    """
    rank = len(shape)
    out: list[KVLayout] = []

    def add(kv, blk, tok, nh, hs, fmt=None):
        out.append(KVLayout(shape[blk], shape[tok], nh, hs, kv, blk, tok, shape, fmt))

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
    kv_layout: str,
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
        kv_layout: "NHD" or "HND". Selects the kernel format only.
        num_kv_heads: KV heads per rank, when known. Separates the two rank-4
            fused orders. Skip for MLA, which reports 1 either way.
        head_size: Width of one head. A tiebreak, not a requirement, since
            triton_attn appends ``scale_pad`` to it.

    Returns:
        The resolved layout, or None when no candidate survives. Callers keep
        their existing fallback in that case rather than guessing.
    """
    scored: list[tuple[int, int, KVLayout]] = []
    for priority, cand in enumerate(_kv_layout_candidates(shape, kv_layout == "HND")):
        if cand.block_size != block_size:
            continue
        # Flattened candidates cannot see the head axis, so they skip the head
        # check — and must therefore lose to a fused candidate that passes it.
        flattened = cand.kv_axis is not None and len(shape) == 4
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
        and (c.kv_axis, c.block_axis, c.token_axis)
        != (best.kv_axis, best.block_axis, best.token_axis)
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
    """
    if layout.kv_axis is None:
        return t.movedim((layout.block_axis, layout.token_axis), (0, 1))
    return t.movedim((layout.kv_axis, layout.block_axis, layout.token_axis), (0, 1, 2))


_kv_cache_layout_warned = False


def _vllm_kv_cache_layout() -> str:
    """Return vLLM's resolved cache layout, "NHD" or "HND".

    The physical memory order, which no tensor reports — it only matters for
    picking the kernel format. Reading it asserts outside a
    ``set_current_vllm_config`` context, and a silent "NHD" on an HND
    deployment would misdescribe memory to the kernel, so warn once.

    Returns:
        The layout, or vLLM's documented default when it cannot be read.
    """
    global _kv_cache_layout_warned
    try:
        from vllm.v1.attention.backends.utils import get_kv_cache_layout

        return get_kv_cache_layout()
    except Exception as e:
        if not _kv_cache_layout_warned:
            _kv_cache_layout_warned = True
            logger.warning(
                "MaruWorkerConnector: cannot read vLLM's KV cache layout "
                "(%s: %s); assuming NHD. Packed-kernel transfers would be "
                "wrong on an HND deployment.",
                type(e).__name__,
                e,
            )
        return "NHD"


def _layout_fits(layout: KVLayout, shape: tuple[int, ...]) -> bool:
    """Whether ``layout`` describes ``shape``: exact shape equality.

    Checking only the named axes was not enough — a rank-5 layout's block,
    token, and K/V positions can all land on matching dims of a rank-4 fused
    tensor with two KV heads, which would read the head axis as K/V. A layout
    is detected from one concrete shape and applies to that shape alone;
    anything else re-detects.
    """
    return tuple(shape) == layout.shape
