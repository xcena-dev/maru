# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""The placement kernel must write what the per-layer fallback writes.

Every other test of the vendored kernels checks the packaging — the file
hashes, the binding surface, how an unbuilt extension is reported. None of
them runs a kernel, so none would notice a refresh that changed where the
bytes land. These do: they place the same slab both ways and compare the
paged cache byte for byte.

The fallback is the reference because it indexes by the logical shape and
follows strides, which is what makes it correct for either cache layout.
The kernel is told the page size and head width instead, so a layout whose
physical order differs from its shape — HND — is where the two could part.

Needs a device and a built extension.
"""

import pytest

# Every test here launches a kernel, so the whole module needs PyTorch and
# cannot run where CI runs. Skipping at import keeps a torch-less host from
# failing collection, which aborts the entire run rather than this file.
# (``test_kv_ops.py`` must NOT do this: its provenance and format guards are
# exactly the ones that have to run on a host without PyTorch.)
torch = pytest.importorskip("torch", reason="placement kernels need PyTorch")

import maru_kv_ops  # noqa: E402
from maru_vllm.connector import _logical_page_format  # noqa: E402
from maru_vllm.kv_layout import (  # noqa: E402
    _canonical_paged_view,
    _detect_kv_layout,
    _gather_kv,
    _scatter_kv,
)

requires_device = pytest.mark.skipif(
    not torch.cuda.is_available() or not maru_kv_ops.is_available(),
    reason="needs a CUDA device and a built maru_kv_ops extension",
)

# The page size and the head count differ on purpose: reading one where the
# other belongs then changes the addresses, which is what makes a swap
# observable. Equal values would let such a defect pass.
NUM_BLOCKS, BLOCK_SIZE, NUM_HEADS, HEAD_SIZE, NUM_LAYERS = 6, 16, 4, 16, 3

_CASES = [
    (
        "NHD",
        (2, NUM_BLOCKS, BLOCK_SIZE, NUM_HEADS, HEAD_SIZE),
        False,
        "NL_X_TWO_NB_BS_NH_HS",
    ),
    (
        "NHD",
        (NUM_BLOCKS, 2, BLOCK_SIZE, NUM_HEADS, HEAD_SIZE),
        False,
        "NL_X_NB_TWO_BS_NH_HS",
    ),
    (
        "HND",
        (2, NUM_BLOCKS, BLOCK_SIZE, NUM_HEADS, HEAD_SIZE),
        True,
        "NL_X_TWO_NB_NH_BS_HS",
    ),
    (
        "HND",
        (NUM_BLOCKS, 2, BLOCK_SIZE, NUM_HEADS, HEAD_SIZE),
        True,
        "NL_X_NB_TWO_NH_BS_HS",
    ),
]


def _paged_cache(shape, hnd, device):
    """Allocate the cache the way vLLM would for that layout.

    An HND allocation is contiguous head-major and then permuted back, which
    leaves the backend's token-major shape over head-major memory.
    """
    cache = torch.zeros(shape, device=device, dtype=torch.float32)
    if hnd:
        cache = cache.permute(0, 1, 3, 2, 4).contiguous().permute(0, 1, 3, 2, 4)
        assert not cache.is_contiguous()
    return cache


def _place_with_fallback(caches, layout, slab, slots):
    """Scatter each layer's slice the way ``_inject_kv_into_layer`` does."""
    blocks = slots // layout.block_size
    offsets = slots % layout.block_size
    for layer_index, cache in enumerate(caches):
        canonical = _canonical_paged_view(cache, layout)
        source = (
            slab[:, layer_index]
            .to(cache.device)
            .reshape(2, slots.numel(), NUM_HEADS, HEAD_SIZE)
        )
        canonical[0, blocks, offsets] = source[0]
        canonical[1, blocks, offsets] = source[1]


@requires_device
@pytest.mark.parametrize(
    "kv_layout,shape,hnd,fmt", _CASES, ids=[case[3] for case in _CASES]
)
def test_kernel_places_what_the_fallback_places(kv_layout, shape, hnd, fmt):
    device = torch.device("cuda:0")
    layout = _detect_kv_layout(
        shape,
        BLOCK_SIZE,
        kv_layout,
        num_kv_heads=NUM_HEADS,
        head_size=HEAD_SIZE,
    )
    assert layout is not None, f"{shape} {kv_layout} went unrecognized"
    assert layout.format_name == fmt

    # One chunk's slab, the shape _load_packed hands the kernel whole.
    slab = torch.randn(2, NUM_LAYERS, BLOCK_SIZE, NUM_HEADS * HEAD_SIZE)
    # Two separate pages, so a wrong block/offset split also shows. Slots
    # confined to one page would hide it: block 0's base offset is zero.
    half = BLOCK_SIZE // 2
    slots = torch.cat(
        [torch.arange(half), torch.arange(3 * BLOCK_SIZE, 3 * BLOCK_SIZE + half)]
    ).to(device=device, dtype=torch.int64)

    by_kernel = [_paged_cache(shape, hnd, device) for _ in range(NUM_LAYERS)]
    by_fallback = [_paged_cache(shape, hnd, device) for _ in range(NUM_LAYERS)]

    maru_kv_ops.multi_layer_kv_transfer(
        slab,
        torch.tensor(
            [cache.data_ptr() for cache in by_kernel],
            dtype=torch.int64,
            device=device,
        ),
        slots,
        device,
        NUM_BLOCKS * BLOCK_SIZE,
        maru_kv_ops.TransferDirection.H2D,
        getattr(maru_kv_ops.EngineKVFormat, fmt),
        block_size=BLOCK_SIZE,
        head_size=HEAD_SIZE,
    )
    _place_with_fallback(by_fallback, layout, slab, slots)
    torch.cuda.synchronize()

    written = sum(int((cache != 0).sum().item()) for cache in by_fallback)
    expected = NUM_LAYERS * 2 * BLOCK_SIZE * NUM_HEADS * HEAD_SIZE
    assert written == expected, (
        f"the reference wrote {written} of {expected} elements, so the "
        "comparison would pass on an untouched cache"
    )
    for kernel_out, reference in zip(by_kernel, by_fallback, strict=True):
        torch.testing.assert_close(kernel_out, reference, rtol=0, atol=0)


# vLLM 0.29+ KVCacheLayout: the physical order of the logical axes
# [L, B, H, N, C], outermost first (vllm.v1.kv_cache_layout).
_LOGICAL_PAGE_ORDERS = {
    "LBHNC": (0, 1, 2, 3, 4),
    "LBNHC": (0, 1, 3, 2, 4),
    "LHBNC": (0, 2, 1, 3, 4),
    "BLHNC": (1, 0, 2, 3, 4),
    "BLNHC": (1, 0, 3, 2, 4),
    "BHLNC": (1, 2, 0, 3, 4),
}

# (dtype, head_size): head runs of 32, 8, 12 and 3 bytes, so the kernel's
# access width comes out 16, 8, 4 and 1 bytes.
_LOGICAL_PAGE_DTYPES = [
    (torch.bfloat16, HEAD_SIZE),
    (torch.float32, 2),
    (torch.float16, 6),
    (torch.uint8, 3),
]


def _logical_pages(layout_name, dtype, head_size, device, pad=0):
    """Every layer's logical page ``[B, H, N, K|V]`` as vLLM 0.29+ builds it.

    One buffer in the layout's physical order, viewed back in logical order
    and sliced per layer, which is what ``create_kv_cache_views`` produces.
    ``pad`` adds elements after each block's page in a layer-outermost
    layout, as a padded page (``page_size_padded``) does.
    """
    order = _LOGICAL_PAGE_ORDERS[layout_name]
    logical = (NUM_LAYERS, NUM_BLOCKS, NUM_HEADS, BLOCK_SIZE, 2 * head_size)
    if pad:
        assert order[:2] == (0, 1), "padding only modelled for L, B outermost"
        page = NUM_HEADS * BLOCK_SIZE * 2 * head_size
        raw = torch.zeros(
            NUM_LAYERS, NUM_BLOCKS, page + pad, dtype=dtype, device=device
        )
        inner = tuple(logical[d] for d in order[2:])
        full = raw[..., :page].unflatten(-1, inner)
        inverse = (0, 1, *(2 + order[2:].index(d) for d in (2, 3, 4)))
        view = full.permute(inverse)
    else:
        raw = torch.zeros(tuple(logical[d] for d in order), dtype=dtype, device=device)
        view = raw.permute(tuple(order.index(d) for d in range(5)))
    return [view[i] for i in range(NUM_LAYERS)]


def _logical_layout(layout_name, head_size):
    shape = (NUM_BLOCKS, NUM_HEADS, BLOCK_SIZE, 2 * head_size)
    layout = _detect_kv_layout(
        shape, BLOCK_SIZE, layout_name, num_kv_heads=NUM_HEADS, head_size=head_size
    )
    assert layout is not None and layout.kv_packed
    return layout


def _fill(tensor):
    """Distinct nonzero values for any dtype, so a misplaced byte shows."""
    values = torch.arange(1, tensor.numel() + 1, device=tensor.device) % 251 + 1
    tensor.copy_(values.view(tensor.shape).to(tensor.dtype))


def _slots(device):
    # Two pages, and one negative slot the kernel must skip.
    half = BLOCK_SIZE // 2
    slots = torch.cat(
        [torch.arange(half), torch.arange(3 * BLOCK_SIZE, 3 * BLOCK_SIZE + half)]
    )
    slots[5] = -1
    return slots.to(device=device, dtype=torch.int64)


def _transfer(slab, pages, slots, direction, fmt, head_size):
    device = pages[0].device
    maru_kv_ops.multi_layer_kv_transfer(
        slab,
        torch.tensor([p.data_ptr() for p in pages], dtype=torch.int64, device=device),
        slots,
        device,
        NUM_BLOCKS * BLOCK_SIZE,
        direction,
        fmt,
        block_size=BLOCK_SIZE,
        head_size=head_size,
    )
    torch.cuda.synchronize()


_LOGICAL_IDS = [
    f"{name}-{str(dtype).split('.')[-1]}-hs{hs}"
    for name in _LOGICAL_PAGE_ORDERS
    for dtype, hs in _LOGICAL_PAGE_DTYPES
]
_LOGICAL_CASES = [
    (name, dtype, hs)
    for name in _LOGICAL_PAGE_ORDERS
    for dtype, hs in _LOGICAL_PAGE_DTYPES
]


@requires_device
@pytest.mark.parametrize("slab_on", ["device", "pinned"])
@pytest.mark.parametrize(
    "layout_name,dtype,head_size", _LOGICAL_CASES, ids=_LOGICAL_IDS
)
def test_logical_page_load_places_what_the_fallback_places(
    layout_name, dtype, head_size, slab_on
):
    """H2D: the kernel writes each page what ``_scatter_kv`` writes."""
    device = torch.device("cuda:0")
    layout = _logical_layout(layout_name, head_size)
    by_kernel = _logical_pages(layout_name, dtype, head_size, device)
    by_fallback = _logical_pages(layout_name, dtype, head_size, device)
    fmt = _logical_page_format(maru_kv_ops.LogicalPageFormat, by_kernel, layout)
    assert fmt is not None

    slab = torch.empty(2, NUM_LAYERS, BLOCK_SIZE, NUM_HEADS * head_size, dtype=dtype)
    _fill(slab)
    slab = slab.to(device) if slab_on == "device" else slab.pin_memory()
    slots = _slots(device)

    _transfer(slab, by_kernel, slots, maru_kv_ops.TransferDirection.H2D, fmt, head_size)
    kept = slots >= 0
    for i, page in enumerate(by_fallback):
        _scatter_kv(page, layout, slots[kept], slab[:, i].to(device)[:, kept])
    torch.cuda.synchronize()

    for kernel_out, reference in zip(by_kernel, by_fallback, strict=True):
        assert int((reference != 0).sum()) == (
            2 * int(kept.sum()) * NUM_HEADS * head_size
        ), "the reference left the page untouched"
        assert torch.equal(kernel_out, reference)


@requires_device
@pytest.mark.parametrize(
    "layout_name,dtype,head_size", _LOGICAL_CASES, ids=_LOGICAL_IDS
)
def test_logical_page_store_gathers_what_the_fallback_gathers(
    layout_name, dtype, head_size
):
    """D2H: the kernel's slab holds what ``_gather_kv`` stores, K/V-major."""
    device = torch.device("cuda:0")
    layout = _logical_layout(layout_name, head_size)
    pages = _logical_pages(layout_name, dtype, head_size, device)
    for page in pages:
        _fill(page)
    fmt = _logical_page_format(maru_kv_ops.LogicalPageFormat, pages, layout)
    slots = _slots(device)

    slab = torch.zeros(
        2, NUM_LAYERS, BLOCK_SIZE, NUM_HEADS * head_size, dtype=dtype, device=device
    )
    _transfer(slab, pages, slots, maru_kv_ops.TransferDirection.D2H, fmt, head_size)

    kept = slots >= 0
    reference = torch.stack([_gather_kv(p, layout, slots[kept]) for p in pages], dim=1)
    assert torch.equal(slab[:, :, kept], reference)
    assert not slab[:, :, ~kept].any(), "a skipped slot was written"


@requires_device
def test_logical_page_follows_padded_pages():
    """Padding after each page is skipped by the block stride, not read."""
    device = torch.device("cuda:0")
    layout = _logical_layout("LBNHC", HEAD_SIZE)
    pages = _logical_pages("LBNHC", torch.bfloat16, HEAD_SIZE, device, pad=8)
    fallback = _logical_pages("LBNHC", torch.bfloat16, HEAD_SIZE, device, pad=8)
    fmt = _logical_page_format(maru_kv_ops.LogicalPageFormat, pages, layout)
    assert fmt.block_stride == NUM_HEADS * BLOCK_SIZE * 2 * HEAD_SIZE + 8

    slab = torch.empty(
        2, NUM_LAYERS, BLOCK_SIZE, NUM_HEADS * HEAD_SIZE, dtype=torch.bfloat16
    )
    _fill(slab)
    slab = slab.to(device)
    slots = torch.arange(3 * BLOCK_SIZE, 4 * BLOCK_SIZE, device=device)
    _transfer(slab, pages, slots, maru_kv_ops.TransferDirection.H2D, fmt, HEAD_SIZE)
    for i, page in enumerate(fallback):
        _scatter_kv(page, layout, slots, slab[:, i])
    torch.cuda.synchronize()
    # The whole buffer, padding included: the layers share one.
    assert pages[0]._base is not None and fallback[0]._base is not None
    assert torch.equal(pages[0]._base, fallback[0]._base)


@requires_device
def test_logical_page_skips_a_prefix():
    device = torch.device("cuda:0")
    layout = _logical_layout("LBNHC", HEAD_SIZE)
    pages = _logical_pages("LBNHC", torch.bfloat16, HEAD_SIZE, device)
    fmt = _logical_page_format(maru_kv_ops.LogicalPageFormat, pages, layout)
    slab = torch.empty(
        2, NUM_LAYERS, BLOCK_SIZE, NUM_HEADS * HEAD_SIZE, dtype=torch.bfloat16
    )
    _fill(slab)
    slab = slab.to(device)
    slots = torch.arange(BLOCK_SIZE, device=device)
    maru_kv_ops.multi_layer_kv_transfer(
        slab,
        torch.tensor([p.data_ptr() for p in pages], dtype=torch.int64, device=device),
        slots,
        device,
        NUM_BLOCKS * BLOCK_SIZE,
        maru_kv_ops.TransferDirection.H2D,
        fmt,
        block_size=BLOCK_SIZE,
        head_size=HEAD_SIZE,
        skip_prefix_n_tokens=4,
    )
    torch.cuda.synchronize()
    got = torch.stack([_gather_kv(p, layout, slots) for p in pages], dim=1)
    assert not got[:, :, :4].any()
    assert torch.equal(got[:, :, 4:], slab[:, :, 4:])


@requires_device
@pytest.mark.parametrize(
    "slab_shape,slots,head_size,match",
    [
        (
            (1, NUM_LAYERS, BLOCK_SIZE, NUM_HEADS * HEAD_SIZE),
            BLOCK_SIZE,
            HEAD_SIZE,
            "key_value",
        ),
        (
            (2, NUM_LAYERS, BLOCK_SIZE, NUM_HEADS * HEAD_SIZE),
            BLOCK_SIZE - 1,
            HEAD_SIZE,
            "slot_mapping",
        ),
        (
            (2, NUM_LAYERS, BLOCK_SIZE, NUM_HEADS * HEAD_SIZE),
            BLOCK_SIZE,
            0,
            "head_size",
        ),
        (
            (2, NUM_LAYERS, BLOCK_SIZE, NUM_HEADS * HEAD_SIZE),
            BLOCK_SIZE,
            7,
            "head_size",
        ),
        (
            (2, NUM_LAYERS, BLOCK_SIZE, (NUM_HEADS + 1) * HEAD_SIZE),
            BLOCK_SIZE,
            HEAD_SIZE,
            "num_heads",
        ),
    ],
    ids=["one-plane", "short-slots", "no-head-size", "uneven-head-size", "wide-row"],
)
def test_logical_page_refuses_bad_arguments(slab_shape, slots, head_size, match):
    """A mismatch raises before launch instead of writing out of bounds."""
    device = torch.device("cuda:0")
    layout = _logical_layout("LBNHC", HEAD_SIZE)
    pages = _logical_pages("LBNHC", torch.bfloat16, HEAD_SIZE, device)
    fmt = _logical_page_format(maru_kv_ops.LogicalPageFormat, pages, layout)
    slab = torch.zeros(slab_shape, dtype=torch.bfloat16, device=device)
    with pytest.raises(RuntimeError, match=match):
        maru_kv_ops.multi_layer_kv_transfer(
            slab,
            torch.tensor(
                [p.data_ptr() for p in pages], dtype=torch.int64, device=device
            ),
            torch.arange(slots, device=device),
            device,
            NUM_BLOCKS * BLOCK_SIZE,
            maru_kv_ops.TransferDirection.H2D,
            fmt,
            block_size=BLOCK_SIZE,
            head_size=head_size,
        )
