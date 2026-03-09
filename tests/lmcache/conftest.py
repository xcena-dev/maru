# SPDX-License-Identifier: Apache-2.0
"""Shared fixtures for maru_lmcache tests.

These tests require a working lmcache installation (with CUDA C extensions).
The entire module is skipped if lmcache cannot be imported.
"""

import asyncio
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def async_loop():
    """Provide an asyncio event loop for tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_maru_handler():
    """A mock MaruHandler that simulates connect/store/retrieve/etc."""
    handler = MagicMock()
    handler.connect.return_value = True
    handler.healthcheck.return_value = True
    handler.exists.return_value = True
    handler.delete.return_value = True
    handler.close.return_value = None
    return handler


@pytest.fixture
def mock_memory_info():
    """A mock MemoryInfo with a memoryview payload."""
    info = MagicMock()
    data = bytearray(1024)
    info.view = memoryview(data)
    return info


@pytest.fixture
def lmcache_config():
    """A minimal LMCacheEngineConfig for testing."""
    from lmcache.v1.config import LMCacheEngineConfig

    return LMCacheEngineConfig(
        chunk_size=256,
        remote_url="maru://localhost:5555?pool_size=1G",
        remote_storage_plugins=["maru"],
        extra_config={
            "remote_storage_plugin.maru.module_path": "maru_lmcache.adapter",
            "remote_storage_plugin.maru.class_name": "MaruConnectorAdapter",
            "maru_pool_size": "4G",
            "save_chunk_meta": False,
        },
    )
