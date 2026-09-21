# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""The placement kernel must write what the per-layer fallback writes.

``_place_packed_layer`` has two branches that are meant to be
interchangeable, and only one of them reads the destination's geometry off
the destination itself. That makes an HND cache the case where they can
disagree: vLLM registers such a cache with the backend's token-major shape
and records the layout in the strides alone, so a kernel that trusts the
shape sees the page size and the head count the wrong way round. The
fallback indexes by the logical shape and follows strides, so it is the
reference here.

These launch real kernels against a real paged cache, so they need a device
and a built extension; everything that can be pinned without one lives in
``test_vllm_connector.py`` and ``test_kv_ops.py``.
"""

import pytest
import torch

import maru_kv_ops
from maru_vllm.kv_layout import _detect_kv_layout
from tests.unit.vllm_connector_helpers import (
    make_bare_worker,
    make_flash_attn_metadata,
)

requires_device = pytest.mark.skipif(
    not torch.cuda.is_available() or not maru_kv_ops.is_available(),
    reason="needs a CUDA device and a built maru_kv_ops extension",
)

# The page size and the head count differ on purpose: reading one where the
# other belongs then changes the addresses, which is what makes an axis swap
# observable. Equal values would let the defect pass.
NB, BS, NH, HS = 6, 16, 4, 16

_CASES = [
    ("nhd-kv-first", "NHD", (2, NB, BS, NH, HS), False, "NL_X_TWO_NB_BS_NH_HS"),
    ("nhd-block-first", "NHD", (NB, 2, BS, NH, HS), False, "NL_X_NB_TWO_BS_NH_HS"),
    ("hnd-kv-first", "HND", (2, NB, BS, NH, HS), True, "NL_X_TWO_NB_NH_BS_HS"),
    ("hnd-block-first", "HND", (NB, 2, BS, NH, HS), True, "NL_X_NB_TWO_NH_BS_HS"),
]


def _paged_cache(shape, hnd, device):
    """Allocate the cache the way vLLM would for that layout.

    An HND allocation is contiguous head-major and then permuted back, which
    leaves the backend's token-major shape over head-major memory.
    """
    t = torch.zeros(shape, device=device, dtype=torch.float32)
    if hnd:
        t = t.permute(0, 1, 3, 2, 4).contiguous().permute(0, 1, 3, 2, 4)
        assert not t.is_contiguous()
    return t


@requires_device
@pytest.mark.parametrize(
    "label,kv_layout,shape,hnd,fmt", _CASES, ids=[case[0] for case in _CASES]
)
def test_kernel_places_what_the_fallback_places(label, kv_layout, shape, hnd, fmt):
    device = torch.device("cuda:0")
    layout = _detect_kv_layout(shape, BS, kv_layout, num_kv_heads=NH, head_size=HS)
    assert layout is not None, f"{label}: layout went unrecognized"
    assert layout.format_name == fmt

    worker = make_bare_worker(block_size=BS, num_kv_heads=NH, head_size=HS)
    worker._kv_layout = layout

    num_tokens = 2 * BS
    staged = torch.randn(2, num_tokens, NH * HS, device=device)
    # Two separate pages, so a wrong block/offset split also shows.
    slots = torch.cat([torch.arange(BS), torch.arange(3 * BS, 4 * BS)]).to(
        device=device, dtype=torch.int64
    )

    by_kernel = _paged_cache(shape, hnd, device)
    by_fallback = _paged_cache(shape, hnd, device)

    kernel_ctx = (
        maru_kv_ops,
        None,
        NB * BS,
        BS,
        HS,
        getattr(maru_kv_ops.EngineKVFormat, fmt),
    )
    worker._place_packed_layer(by_kernel, staged, slots, None, "l0", kernel_ctx)
    worker._place_packed_layer(
        by_fallback, staged, slots, make_flash_attn_metadata(), "l0", None
    )
    torch.cuda.synchronize()

    written = int((by_fallback != 0).sum().item())
    assert written == 2 * num_tokens * NH * HS, (
        f"{label}: the reference wrote {written} elements, "
        "so the comparison would pass on an untouched cache"
    )
    torch.testing.assert_close(by_kernel, by_fallback, rtol=0, atol=0)
