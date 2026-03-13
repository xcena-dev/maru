# SPDX-License-Identifier: Apache-2.0
"""Shared fixtures for Maru SGLang backend tests.

Provides mock objects so tests can run without SGLang or a live Maru server.
"""

import os

from dataclasses import dataclass, field
from typing import Optional
from unittest.mock import MagicMock

import pytest


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
    model_name: Optional[str] = "test-model"
    extra_config: Optional[dict] = None


# ---------------------------------------------------------------------------
# Mock MaruHandler
# ---------------------------------------------------------------------------


class MockMaruHandler:
    """In-memory mock of MaruHandler for unit testing."""

    def __init__(self):
        self._store: dict[str, bytes] = {}
        self._connected = True

    def connect(self) -> bool:
        self._connected = True
        return True

    def close(self) -> None:
        self._connected = False

    def exists(self, key: str) -> bool:
        return key in self._store

    def batch_exists(self, keys: list[str]) -> list[bool]:
        return [k in self._store for k in keys]

    def store(self, key: str, info=None, data=None, handle=None, **kwargs) -> bool:
        if handle is not None:
            self._store[key] = bytes(handle.buf)
            return True
        if data is not None:
            self._store[key] = bytes(data)
            return True
        if info is not None:
            view = info if isinstance(info, memoryview) else info.view
            self._store[key] = bytes(view)
            return True
        return False

    def retrieve(self, key: str):
        if key not in self._store:
            return None
        data = self._store[key]
        mv = memoryview(bytearray(data))
        return MagicMock(view=mv)

    def batch_retrieve(self, keys: list[str]):
        return [self.retrieve(k) for k in keys]

    def batch_store(self, keys: list[str], infos: list) -> list[bool]:
        results = []
        for key, info in zip(keys, infos):
            results.append(self.store(key, data=info if isinstance(info, memoryview) else None, info=info))
        return results

    def alloc(self, size: int):
        buf = bytearray(size)
        handle = MagicMock()
        handle.buf = memoryview(buf)
        handle._region_id = 0
        handle._page_index = 0
        handle._size = size
        return handle

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
