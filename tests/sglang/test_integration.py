# SPDX-License-Identifier: Apache-2.0
"""Tests for MaruHiCacheStorage V1 API and register_mem_pool_host.

Covers batch_get_v1 / batch_set_v1 (page_first and fallback paths) which are
not exercised in test_backend.py.  Uses MockMaruHandler — no live server needed.
"""

import ctypes
from unittest.mock import MagicMock, patch

import pytest
import torch

from maru_sglang.backend import MaruHiCacheStorage

from .conftest import MockHiCacheStorageConfig, MockMaruHandler


def _make_backend(
    is_page_first: bool = True,
    handler: MockMaruHandler | None = None,
) -> MaruHiCacheStorage:
    cfg = MockHiCacheStorageConfig(is_page_first_layout=is_page_first)
    with patch.object(MaruHiCacheStorage, "_connect"):
        backend = MaruHiCacheStorage(cfg, {})
    backend._handler = handler or MockMaruHandler()
    return backend


class MockMemPoolHost:
    """Minimal mem_pool_host stub for V1 API tests."""

    def __init__(self, page_size: int, num_pages: int) -> None:
        self._page_size = page_size
        self._buf = (ctypes.c_char * (page_size * num_pages))()

    def get_page_buffer_meta(self, host_indices: torch.Tensor):
        return [
            (ctypes.addressof(self._buf) + int(i) * self._page_size, self._page_size)
            for i in host_indices.tolist()
        ]

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

    def test_page_first_flag_set(self):
        backend = _make_backend(is_page_first=True)
        backend.register_mem_pool_host(MockMemPoolHost(64, 1))
        assert backend._is_page_first is True

    def test_page_first_flag_unset(self):
        backend = _make_backend(is_page_first=False)
        backend.register_mem_pool_host(MockMemPoolHost(64, 1))
        assert backend._is_page_first is False


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

        backend = _make_backend(is_page_first=True)
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

        backend = _make_backend(is_page_first=True)
        backend.register_mem_pool_host(pool)

        backend.batch_set_v1(["data_key"], torch.tensor([0]))
        result = backend.get("data_key")
        assert result is not None
        assert result.numel() == page_size
        assert result[0].item() == 0xCC

    def test_alloc_failure_returns_false(self):
        """If alloc raises, that key gets False in results."""
        page_size = 64
        pool = MockMemPoolHost(page_size, 1)

        handler = MockMaruHandler()
        orig_alloc = handler.alloc

        call_count = [0]

        def failing_alloc(size):
            call_count[0] += 1
            if call_count[0] >= 2:
                raise ValueError("pool full")
            return orig_alloc(size)

        handler.alloc = failing_alloc

        backend = _make_backend(is_page_first=True, handler=handler)
        backend.register_mem_pool_host(pool)

        results = backend.batch_set_v1(["ok", "fail"], torch.tensor([0, 0]))
        assert results[0] is True
        assert results[1] is False

    def test_empty_keys(self):
        backend = _make_backend(is_page_first=True)
        backend.register_mem_pool_host(MockMemPoolHost(64, 0))
        assert backend.batch_set_v1([], torch.tensor([])) == []

    def test_disconnected_returns_false(self):
        backend = _make_backend(is_page_first=True)
        backend._handler = None
        assert backend.batch_set_v1(["k"], torch.tensor([0])) == [False]


# ---------------------------------------------------------------------------
# batch_get_v1 — page_first path
# ---------------------------------------------------------------------------


class TestBatchGetV1PageFirst:
    def test_restores_bytes_to_host_pages(self):
        """batch_get_v1 copies Maru data back into host pool pages."""
        page_size = 16
        pool = MockMemPoolHost(page_size, 1)

        backend = _make_backend(is_page_first=True)
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
        backend = _make_backend(is_page_first=True)
        backend.register_mem_pool_host(MockMemPoolHost(64, 1))
        results = backend.batch_get_v1(["miss"], torch.tensor([0]))
        assert results == [False]

    def test_partial_hit(self):
        page_size = 16
        pool = MockMemPoolHost(page_size, 3)
        pool.write_page(0, 0x11)
        pool.write_page(1, 0x22)

        backend = _make_backend(is_page_first=True)
        backend.register_mem_pool_host(pool)

        backend.batch_set_v1(["a", "b"], torch.tensor([0, 1]))

        results = backend.batch_get_v1(["a", "miss", "b"], torch.tensor([0, 2, 1]))
        assert results[0] is True
        assert results[1] is False
        assert results[2] is True

    def test_empty_keys(self):
        backend = _make_backend(is_page_first=True)
        backend.register_mem_pool_host(MockMemPoolHost(64, 0))
        assert backend.batch_get_v1([], torch.tensor([])) == []

    def test_disconnected_returns_false(self):
        backend = _make_backend(is_page_first=True)
        backend._handler = None
        assert backend.batch_get_v1(["k"], torch.tensor([0])) == [False]


# ---------------------------------------------------------------------------
# batch_set_v1 / batch_get_v1 — fallback (non-page_first)
# ---------------------------------------------------------------------------


class TestV1Fallback:
    def test_batch_set_v1_fallback_all_false(self):
        """Fallback always returns False — caller should use legacy set."""
        backend = _make_backend(is_page_first=False)
        backend.register_mem_pool_host(MockMemPoolHost(64, 2))
        results = backend.batch_set_v1(["a", "b"], torch.tensor([0, 1]))
        assert results == [False, False]

    def test_batch_get_v1_fallback_hit(self):
        """Fallback returns True for stored keys, False for misses."""
        backend = _make_backend(is_page_first=False)
        backend.register_mem_pool_host(MockMemPoolHost(64, 3))

        v = torch.ones(4, dtype=torch.float32)
        backend.set("hit0", value=v)
        backend.set("hit1", value=v)

        results = backend.batch_get_v1(
            ["hit0", "hit1", "miss"], torch.tensor([0, 1, 2])
        )
        assert results == [True, True, False]

    def test_batch_get_v1_fallback_all_miss(self):
        backend = _make_backend(is_page_first=False)
        backend.register_mem_pool_host(MockMemPoolHost(64, 2))
        results = backend.batch_get_v1(["x", "y"], torch.tensor([0, 1]))
        assert results == [False, False]
