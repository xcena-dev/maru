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
from maru_vllm.kv_layout import (  # noqa: E402
    _canonical_paged_view,
    _detect_kv_layout,
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
