# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit test configuration — MockShmClient for tests without a running resource manager."""

import mmap
from unittest.mock import patch

import pytest

from maru_shm import MaruHandle

# ============================================================================
# MockShmClient — replaces real MaruShmClient in unit tests
# ============================================================================

_alloc_counter = 0
_mmap_objects: list[mmap.mmap] = []


class MockShmClient:
    """Mock MaruShmClient for unit tests that don't need a running resource manager."""

    def __init__(self, *args, **kwargs):
        pass

    def _ensure_resource_manager(self):
        pass

    def stats(self):
        return []

    def alloc(self, size, pool_id=None):
        global _alloc_counter
        _alloc_counter += 1
        return MaruHandle(
            region_id=_alloc_counter, offset=0, length=size, auth_token=12345
        )

    def free(self, handle):
        pass

    def mmap(self, handle, prot, flags=0):
        obj = mmap.mmap(-1, handle.length)
        _mmap_objects.append(obj)
        return obj

    def munmap(self, handle):
        pass

    def close(self):
        pass


# ============================================================================
# Autouse fixture — patches MaruShmClient at all import sites for unit tests
# ============================================================================


@pytest.fixture(autouse=True)
def _mock_shm_client():
    """Patch MaruShmClient with MockShmClient for every unit test.

    Patches at all import sites so any code path that instantiates
    MaruShmClient() gets the mock instead.
    """
    global _alloc_counter, _mmap_objects
    _alloc_counter = 0
    _mmap_objects = []

    with (
        patch("maru_shm.client.MaruShmClient", MockShmClient),
        patch("maru_shm.MaruShmClient", MockShmClient),
        patch("maru_server.allocation_manager.MaruShmClient", MockShmClient),
        patch("maru_handler.memory.mapper.MaruShmClient", MockShmClient),
    ):
        yield

    # Cleanup mmap objects after test
    for obj in _mmap_objects:
        try:
            obj.close()
        except Exception:
            pass
    _mmap_objects = []
