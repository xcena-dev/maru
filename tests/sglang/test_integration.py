# SPDX-License-Identifier: Apache-2.0
"""Tests for MaruStorage V1 API and register_mem_pool_host.

Covers batch_get_v1 / batch_set_v1 (page_first and fallback paths) which are
not exercised in test_backend.py.  Uses MockMaruHandler — no live server needed.
"""

import ctypes
from unittest.mock import patch

import pytest
import torch

from maru_sglang.maru_storage import MaruStorage

from .conftest import MockHiCacheStorageConfig, MockMaruHandler


def _make_backend(
    handler: MockMaruHandler | None = None,
) -> MaruStorage:
    cfg = MockHiCacheStorageConfig()
    with patch.object(MaruStorage, "_connect"):
        backend = MaruStorage(cfg, {})
    backend._handler = handler or MockMaruHandler()
    backend._connected = True
    return backend


class MockMemPoolHost:
    """Minimal mem_pool_host stub for V1 API tests."""

    def __init__(
        self, page_size: int, num_pages: int, layout: str = "page_first_direct"
    ) -> None:
        self._page_size = page_size
        self._buf = (ctypes.c_char * (page_size * num_pages))()
        self.layout = layout

    def get_page_buffer_meta(self, host_indices: torch.Tensor):
        ptr_list = []
        size_list = []
        for i in host_indices.tolist():
            ptr_list.append(ctypes.addressof(self._buf) + int(i) * self._page_size)
            size_list.append(self._page_size)
        return ptr_list, size_list

    def read_page(self, idx: int) -> bytes:
        off = idx * self._page_size
        return bytes(self._buf[off : off + self._page_size])

    def write_page(self, idx: int, fill: int) -> None:
        off = idx * self._page_size
        for j in range(self._page_size):
            self._buf[off + j] = bytes([fill])


# ---------------------------------------------------------------------------
# register_mem_pool_host
# ---------------------------------------------------------------------------


class TestRegisterMemPoolHost:
    def test_sets_mem_pool_host(self):
        backend = _make_backend()
        pool = MockMemPoolHost(64, 2)
        backend.register_mem_pool_host(pool)
        assert backend.mem_pool_host is pool

    def test_non_page_first_rejected(self):
        """MaruStorage rejects non-page_first at register_mem_pool_host."""
        backend = _make_backend()
        with pytest.raises(ValueError, match="page_first"):
            backend.register_mem_pool_host(MockMemPoolHost(64, 1, layout="layer_first"))


# ---------------------------------------------------------------------------
# batch_set_v1 — page_first path
# ---------------------------------------------------------------------------


class TestBatchSetV1PageFirst:
    def test_stores_host_pages(self):
        """batch_set_v1 copies each host page into Maru via alloc+store."""
        page_size = 64
        pool = MockMemPoolHost(page_size, 2)
        pool.write_page(0, 0xAA)
        pool.write_page(1, 0xBB)

        backend = _make_backend()
        backend.register_mem_pool_host(pool)

        host_indices = torch.tensor([0, 1])
        results = backend.batch_set_v1(["k0", "k1"], host_indices)

        assert results == [True, True]
        # Keys must now be visible via legacy exists
        assert backend.exists("k0") is True
        assert backend.exists("k1") is True

    def test_data_content_preserved(self):
        """Bytes written to host pages appear in Maru store."""
        page_size = 16
        pool = MockMemPoolHost(page_size, 1)
        pool.write_page(0, 0xCC)

        backend = _make_backend()
        backend.register_mem_pool_host(pool)

        backend.batch_set_v1(["data_key"], torch.tensor([0]))
        result = backend.get("data_key")
        assert result is not None
        assert result.numel() == page_size
        assert result[0].item() == 0xCC

    def test_partial_store_failure(self):
        """If batch_store returns False for some keys, results reflect it."""
        page_size = 64
        pool = MockMemPoolHost(page_size, 2)
        pool.write_page(0, 0xAA)
        pool.write_page(1, 0xBB)

        handler = MockMaruHandler()
        orig_batch_store = handler.batch_store

        def partial_batch_store(keys, infos):
            results = orig_batch_store(keys, infos)
            if len(results) > 1:
                results[1] = False
            return results

        handler.batch_store = partial_batch_store

        backend = _make_backend(handler=handler)
        backend.register_mem_pool_host(pool)

        results = backend.batch_set_v1(["ok", "fail"], torch.tensor([0, 1]))
        assert results[0] is True
        assert results[1] is False

    def test_empty_keys(self):
        backend = _make_backend()
        backend.register_mem_pool_host(MockMemPoolHost(64, 0))
        assert backend.batch_set_v1([], torch.tensor([])) == []

    def test_disconnected_returns_false(self):
        backend = _make_backend()
        backend._handler = None
        backend._connected = False
        with patch.object(MaruStorage, "_connect", side_effect=RuntimeError):
            assert backend.batch_set_v1(["k"], torch.tensor([0])) == [False]


# ---------------------------------------------------------------------------
# batch_get_v1 — page_first path
# ---------------------------------------------------------------------------


class TestBatchGetV1PageFirst:
    def test_restores_bytes_to_host_pages(self):
        """batch_get_v1 copies Maru data back into host pool pages."""
        page_size = 16
        pool = MockMemPoolHost(page_size, 1)

        backend = _make_backend()
        backend.register_mem_pool_host(pool)

        # Store via set_v1 first
        pool.write_page(0, 0xDD)
        backend.batch_set_v1(["key"], torch.tensor([0]))

        # Zero out the host page, then restore via get_v1
        pool.write_page(0, 0x00)
        results = backend.batch_get_v1(["key"], torch.tensor([0]))
        assert results == [True]
        assert pool.read_page(0)[0] == 0xDD

    def test_miss_returns_false(self):
        backend = _make_backend()
        backend.register_mem_pool_host(MockMemPoolHost(64, 1))
        results = backend.batch_get_v1(["miss"], torch.tensor([0]))
        assert results == [False]

    def test_partial_hit(self):
        page_size = 16
        pool = MockMemPoolHost(page_size, 3)
        pool.write_page(0, 0x11)
        pool.write_page(1, 0x22)

        backend = _make_backend()
        backend.register_mem_pool_host(pool)

        backend.batch_set_v1(["a", "b"], torch.tensor([0, 1]))

        results = backend.batch_get_v1(["a", "miss", "b"], torch.tensor([0, 2, 1]))
        assert results[0] is True
        assert results[1] is False
        assert results[2] is True

    def test_empty_keys(self):
        backend = _make_backend()
        backend.register_mem_pool_host(MockMemPoolHost(64, 0))
        assert backend.batch_get_v1([], torch.tensor([])) == []

    def test_disconnected_returns_false(self):
        backend = _make_backend()
        backend._handler = None
        backend._connected = False
        with patch.object(MaruStorage, "_connect", side_effect=RuntimeError):
            assert backend.batch_get_v1(["k"], torch.tensor([0])) == [False]


# ---------------------------------------------------------------------------
# Page ownership on the write paths (#131)
# ---------------------------------------------------------------------------


class MockSplitMemPoolHost:
    """Host pool of a non-MLA model: K and V of a page in separate buffers."""

    layout = "page_first_direct"

    def __init__(self, page_size: int, num_pages: int) -> None:
        self._page_size = page_size
        self._k = (ctypes.c_char * (page_size * num_pages))()
        self._v = (ctypes.c_char * (page_size * num_pages))()

    def get_page_buffer_meta(self, host_indices: torch.Tensor):
        ptr_list, size_list = [], []
        for i in host_indices.tolist():
            for buf in (self._k, self._v):
                ptr_list.append(ctypes.addressof(buf) + int(i) * self._page_size)
                size_list.append(self._page_size)
        return ptr_list, size_list

    def fill(self, idx: int, k: int, v: int) -> None:
        off = idx * self._page_size
        self._k[off : off + self._page_size] = bytes([k]) * self._page_size
        self._v[off : off + self._page_size] = bytes([v]) * self._page_size

    def read(self, idx: int) -> tuple[bytes, bytes]:
        off = idx * self._page_size
        return (
            bytes(self._k[off : off + self._page_size]),
            bytes(self._v[off : off + self._page_size]),
        )


class TestWritePathsUseAllocatedPages:
    """Every write allocates a page, fills it and hands it to the handler."""

    def test_v1_stores_k_then_v_and_loads_them_back(self):
        page_size = 32
        pool = MockSplitMemPoolHost(page_size, 4)
        pool.fill(0, 0x11, 0x22)
        pool.fill(1, 0x33, 0x44)
        handler = MockMaruHandler()
        backend = _make_backend(handler=handler)
        backend.register_mem_pool_host(pool)

        assert backend.batch_set_v1(["a", "b"], torch.tensor([0, 1])) == [True, True]
        assert handler.allocated == {}
        stored = handler._store[backend._make_key("b")]
        assert stored == bytes([0x33]) * page_size + bytes([0x44]) * page_size

        assert backend.batch_get_v1(["a", "b"], torch.tensor([2, 3])) == [True, True]
        assert pool.read(2) == pool.read(0)
        assert pool.read(3) == pool.read(1)

    def test_legacy_writes_leave_no_page_allocated(self):
        handler = MockMaruHandler()
        backend = _make_backend(handler=handler)
        one = torch.arange(24, dtype=torch.float32)
        two = torch.arange(8, dtype=torch.int64)

        assert backend.set("one", one) is True
        assert backend.batch_set(["two", "three"], [two, one]) is True
        assert handler.allocated == {}
        assert backend.get("two").view(torch.int64).tolist() == two.tolist()
        # A key that is already stored gives its page back and reports success.
        assert backend.set("one", one) is True
        assert handler.allocated == {}

    def test_page_larger_than_a_chunk_fails_and_frees_the_others(self):
        page_size = 64
        pool = MockMemPoolHost(page_size, 2)
        handler = MockMaruHandler(chunk_size=page_size)
        backend = _make_backend(handler=handler)
        backend.register_mem_pool_host(pool)

        assert backend.set("small", torch.zeros(page_size, dtype=torch.uint8))
        too_large = torch.zeros(page_size + 1, dtype=torch.uint8)
        assert backend.set("large", too_large) is False
        fits = torch.zeros(page_size, dtype=torch.uint8)
        assert backend.batch_set(["fits", "large"], [fits, too_large]) is False
        assert handler.allocated == {}
        assert backend.exists("fits") is False

    def test_failed_register_frees_every_page(self):
        page_size = 64
        pool = MockMemPoolHost(page_size, 2)
        handler = MockMaruHandler()

        def unavailable(keys, handles):
            raise RuntimeError("Handler is closing")

        handler.batch_store = unavailable
        backend = _make_backend(handler=handler)
        backend.register_mem_pool_host(pool)

        results = backend.batch_set_v1(["k0", "k1"], torch.tensor([0, 1]))
        assert results == [False, False]
        assert handler.allocated == {}
