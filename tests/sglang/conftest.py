# SPDX-License-Identifier: Apache-2.0
"""Shared fixtures for Maru SGLang backend tests.

Provides mock objects so tests can run without SGLang or a live Maru server.
"""

import sys
import types
from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest


def _stub_sglang_if_missing() -> None:
    """Let ``maru_sglang`` import on a host without SGLang.

    ``MaruStorage`` only takes ``register_mem_pool_host`` from its SGLang base
    class, so a stand-in with that one method is enough for these tests. With
    SGLang installed the real class is used.
    """
    try:
        import sglang.srt.mem_cache.hicache_storage  # noqa: F401

        return
    except ImportError:
        pass

    class HiCacheStorage:
        def register_mem_pool_host(self, mem_pool_host) -> None:
            self.mem_pool_host = mem_pool_host

    names = ["sglang", "sglang.srt", "sglang.srt.mem_cache"]
    for name in names:
        sys.modules.setdefault(name, types.ModuleType(name))
    module = types.ModuleType("sglang.srt.mem_cache.hicache_storage")
    module.HiCacheStorage = HiCacheStorage
    sys.modules[module.__name__] = module


_stub_sglang_if_missing()

# ---------------------------------------------------------------------------
# Mock SGLang types (avoid importing sglang at test time)
# ---------------------------------------------------------------------------


@dataclass
class MockHiCacheStorageConfig:
    """Mirrors sglang.srt.mem_cache.hicache_storage.HiCacheStorageConfig."""

    tp_rank: int = 0
    tp_size: int = 1
    pp_rank: int = 0
    pp_size: int = 1
    is_mla_model: bool = False
    is_page_first_layout: bool = True
    model_name: str | None = "test-model"
    extra_config: dict | None = None


# ---------------------------------------------------------------------------
# Mock MaruHandler
# ---------------------------------------------------------------------------


@dataclass(eq=False)
class MockAllocHandle:
    """Mirrors maru_handler.memory.types.AllocHandle."""

    buf: memoryview
    _region_id: int
    _page_index: int
    _size: int


class MockMaruHandler:
    """In-memory mock of MaruHandler for unit testing.

    Follows the handler's write API: ``alloc`` a page, write ``handle.buf``,
    then ``store``/``batch_store`` the handle. Anything else raises, as the
    real handler does. ``allocated`` holds the pages no store has taken, so a
    test can check that none leaked.
    """

    def __init__(self, chunk_size: int = 1 << 20):
        self._store: dict[str, bytes] = {}
        self._connected = True
        self._chunk_size = chunk_size
        self._next_page = 0
        self.allocated: dict[int, MockAllocHandle] = {}

    def connect(self) -> bool:
        self._connected = True
        return True

    def close(self) -> None:
        self._connected = False

    def exists(self, key: str) -> bool:
        return key in self._store

    def batch_exists(self, keys: list[str]) -> list[bool]:
        return [k in self._store for k in keys]

    def alloc(self, size: int, route_key: str | None = None) -> MockAllocHandle:
        if size > self._chunk_size:
            raise ValueError(
                f"Requested size {size} exceeds chunk_size {self._chunk_size}"
            )
        handle = MockAllocHandle(
            buf=memoryview(bytearray(size)),
            _region_id=0,
            _page_index=self._next_page,
            _size=size,
        )
        self._next_page += 1
        self.allocated[handle._page_index] = handle
        return handle

    def free(self, handle: MockAllocHandle) -> None:
        if self.allocated.pop(handle._page_index, None) is None:
            raise ValueError("handle is not tracked")

    def _take(self, handle: MockAllocHandle) -> bytes:
        # A memoryview or tensor has no page: AttributeError, as in the handler.
        if self.allocated.pop(handle._page_index, None) is not handle:
            raise ValueError("handle is not tracked")
        return bytes(handle.buf[: handle._size])

    def store(self, key: str, handle: MockAllocHandle) -> bool:
        data = self._take(handle)
        self._store.setdefault(key, data)
        return True

    def batch_store(self, keys: list[str], handles: list) -> list[bool]:
        if len(keys) != len(handles):
            raise ValueError("keys and handles must have the same length")
        for handle in handles:  # Raise before any handle is consumed.
            if self.allocated.get(handle._page_index) is not handle:
                raise ValueError("handle is not tracked")
        return [self.store(k, h) for k, h in zip(keys, handles, strict=True)]

    def retrieve(self, key: str):
        if key not in self._store:
            return None
        data = self._store[key]
        mv = memoryview(bytearray(data))
        return MagicMock(view=mv)

    def batch_retrieve(self, keys: list[str]):
        return [self.retrieve(k) for k in keys]

    def healthcheck(self) -> bool:
        return self._connected


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_storage_config():
    """Default MockHiCacheStorageConfig."""
    return MockHiCacheStorageConfig()


@pytest.fixture
def mock_storage_config_mla():
    """MLA model config (shared keys across TP ranks)."""
    return MockHiCacheStorageConfig(is_mla_model=True, model_name="test-mla-model")


@pytest.fixture
def mock_storage_config_tp():
    """Multi-TP config."""
    return MockHiCacheStorageConfig(tp_rank=1, tp_size=4, model_name="test-tp-model")


@pytest.fixture
def mock_handler():
    """MockMaruHandler instance."""
    return MockMaruHandler()
