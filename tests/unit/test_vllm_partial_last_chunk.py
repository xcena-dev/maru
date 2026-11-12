# SPDX-License-Identifier: Apache-2.0
"""Regression coverage for injecting a stored chunk into fewer token slots."""

from unittest.mock import MagicMock

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import torch

from tests.unit.test_vllm_connector import _PAGED_LAYOUTS
from tests.unit.test_vllm_logical_page import LAYOUTS, logical_pages
from tests.unit.vllm_connector_helpers import make_bare_worker, make_flash_attn_metadata


class TestInjectPartialLastChunk:
    """The deferred and layerwise loads can need fewer tokens than they read.

    For a fully cached prompt whose length is an exact multiple of the chunk
    size, one block is held back for the engine to compute (see
    TestFullPromptHitLeavesWorkForTheEngine), so the slot mapping covers one
    block less than the stored chunks. Stored chunks are always whole, so the
    injected slab holds more token rows than there are slots, and only the
    leading rows belong to the request.
    """

    NB, BS, NH, HS = 8, 4, 3, 8
    CHUNK = 8  # two blocks

    def _worker(self, shape_fn, num_kv_heads):
        kv_cache = torch.zeros(shape_fn(self.NB, self.BS, self.NH, self.HS))
        worker = make_bare_worker(
            block_size=self.BS,
            kv_caches={"layer0": kv_cache},
            num_kv_heads=num_kv_heads,
            head_size=self.HS,
        )
        worker._kv_chunk_tokens = self.CHUNK
        assert worker._kv_layout is not None
        hidden = kv_cache.numel() // (2 * self.NB * self.BS)
        return worker, kv_cache, hidden

    @pytest.mark.parametrize(
        "label,shape_fn,num_kv_heads",
        _PAGED_LAYOUTS,
        ids=[layout[0] for layout in _PAGED_LAYOUTS],
    )
    def test_single_chunk_with_held_back_block(self, label, shape_fn, num_kv_heads):
        worker, kv_cache, hidden = self._worker(shape_fn, num_kv_heads)
        src = torch.randn(2, self.CHUNK, hidden)  # one whole stored chunk
        slots = torch.arange(self.CHUNK - self.BS)  # one block held back
        meta = make_flash_attn_metadata()

        worker._inject_kv_into_layer(kv_cache, src, slots, meta, "layer0")
        got = worker._extract_kv_from_layer(kv_cache, slots, meta, "layer0")

        torch.testing.assert_close(got, src[:, : self.CHUNK - self.BS])
        rest = torch.arange(self.CHUNK - self.BS, self.CHUNK)
        assert (
            worker._extract_kv_from_layer(kv_cache, rest, meta, "layer0").abs().sum()
            == 0
        )

    @pytest.mark.parametrize(
        "label,shape_fn,num_kv_heads",
        _PAGED_LAYOUTS,
        ids=[layout[0] for layout in _PAGED_LAYOUTS],
    )
    def test_multi_chunk_with_held_back_block(self, label, shape_fn, num_kv_heads):
        worker, kv_cache, hidden = self._worker(shape_fn, num_kv_heads)
        num_chunks = 3
        chunks = torch.randn(num_chunks, 2, self.CHUNK, hidden)  # chunk-major
        ntok = num_chunks * self.CHUNK - self.BS
        slots = torch.arange(ntok)
        meta = make_flash_attn_metadata()

        worker._inject_kv_into_layer(
            kv_cache, chunks.flatten(), slots, meta, "layer0", num_chunks=num_chunks
        )
        got = worker._extract_kv_from_layer(kv_cache, slots, meta, "layer0")

        expected = chunks.permute(1, 0, 2, 3).reshape(2, -1, hidden)[:, :ntok]
        torch.testing.assert_close(got, expected)

    def test_slab_shorter_than_the_slots_is_refused(self):
        worker, kv_cache, hidden = self._worker(*_PAGED_LAYOUTS[1][1:])
        src = torch.randn(2, self.CHUNK - self.BS, hidden)
        with pytest.raises(RuntimeError):
            worker._inject_kv_into_layer(
                kv_cache,
                src,
                torch.arange(self.CHUNK),
                make_flash_attn_metadata(),
                "layer0",
            )

    @pytest.mark.parametrize("num_chunks", [1, 3])
    def test_mla_layer_with_held_back_block(self, num_chunks):
        from vllm.model_executor.layers.attention.mla_attention import (
            MLACommonMetadata,
        )

        worker, _, _ = self._worker(*_PAGED_LAYOUTS[1][1:])
        latent = 2 * self.HS
        mla = torch.zeros(self.NB, self.BS, latent)
        src = torch.randn(num_chunks * self.CHUNK, latent)  # whole stored chunks
        ntok = num_chunks * self.CHUNK - self.BS
        slots = torch.arange(ntok)

        worker._inject_kv_into_layer(
            mla,
            src,
            slots,
            MagicMock(spec=MLACommonMetadata),
            "L1",
            num_chunks=num_chunks,
        )

        flat = mla.reshape(self.NB * self.BS, latent)
        torch.testing.assert_close(flat[:ntok], src[:ntok])
        assert flat[ntok:].abs().sum() == 0

    @pytest.mark.parametrize("num_chunks", [1, 3])
    def test_legacy_triton_layer_with_held_back_block(self, num_chunks):
        from vllm.v1.attention.backends.triton_attn import TritonAttentionMetadata

        worker, _, _ = self._worker(*_PAGED_LAYOUTS[1][1:])
        triton = torch.zeros(self.NB, self.NH, self.BS, self.HS)  # rank-4
        meta = MagicMock(spec=TritonAttentionMetadata)
        src = torch.randn(num_chunks * self.CHUNK, self.NH, self.HS)
        ntok = num_chunks * self.CHUNK - self.BS
        slots = torch.arange(ntok)

        worker._inject_kv_into_layer(
            triton, src, slots, meta, "L1", num_chunks=num_chunks
        )

        got = worker._extract_kv_from_layer(triton, slots, meta, "L1")
        torch.testing.assert_close(got, src[:ntok])
        rest = torch.arange(ntok, num_chunks * self.CHUNK)
        assert worker._extract_kv_from_layer(triton, rest, meta, "L1").abs().sum() == 0

    @pytest.mark.parametrize("layout", LAYOUTS)
    @pytest.mark.parametrize("num_chunks", [1, 3])
    def test_logical_page_keeps_the_unrequested_tail_untouched(
        self, layout, num_chunks
    ):
        zeros = [torch.zeros(self.NB, self.BS, self.NH, self.HS)] * 2
        pages = logical_pages(layout, zeros, zeros)
        worker = make_bare_worker(
            block_size=self.BS,
            kv_caches={"layer0": pages[0], "layer1": pages[1]},
            num_kv_heads=self.NH,
            head_size=self.HS,
            kv_cache_layout=layout,
        )
        chunks = torch.randn(num_chunks, 2, self.CHUNK, self.NH * self.HS)
        ntok = num_chunks * self.CHUNK - self.BS
        slots = torch.arange(ntok)
        meta = make_flash_attn_metadata()
        worker._inject_kv_into_layer(
            pages[0], chunks.flatten(), slots, meta, "layer0", num_chunks=num_chunks
        )
        got = worker._extract_kv_from_layer(pages[0], slots, meta, "layer0")
        expected = chunks.permute(1, 0, 2, 3).reshape(2, -1, self.NH * self.HS)[
            :, :ntok
        ]
        torch.testing.assert_close(got, expected)
        tail = torch.arange(ntok, self.NB * self.BS)
        assert (
            worker._extract_kv_from_layer(
                pages[0], tail, meta, "layer0"
            ).count_nonzero()
            == 0
        )
        assert pages[1].count_nonzero() == 0
