# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit test configuration — MockShmClient for tests without a running resource manager."""

import mmap
import socket
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
        self._device_table = kwargs.get("device_table") or {}

    def _ensure_resource_manager(self):
        pass

    def is_running(self):
        return True

    def stats(self):
        return []

    def alloc(self, size, dax_path=""):
        global _alloc_counter
        _alloc_counter += 1
        return MaruHandle(
            region_id=_alloc_counter, offset=0, length=size, auth_token=12345
        )

    def free(self, handle):
        pass

    def get_dax_path(self, region_id):
        # Tests have a single mock pool; report a stable DEV_DAX path.
        return "/dev/dax0.0"

    def mmap(self, handle, prot):
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
        # MaruHandler.connect and the mixed backend's CXL pool scan the host's
        # DAX devices; on a CXL host that reads the real device headers. A
        # unit test sees none, as in CI. Tests of the scanner patch os calls,
        # and tests that need a table patch these names themselves.
        patch("maru_shm.device_scanner.scan_dax_devices", lambda: []),
        patch("maru_shm.client.scan_dax_devices", lambda: []),
    ):
        yield

    # Cleanup mmap objects after test
    for obj in _mmap_objects:
        try:
            obj.close()
        except Exception:
            pass
    _mmap_objects = []


@pytest.fixture
def refused_address():
    """A loopback host:port that refuses connections while the test runs.

    The port is bound but never listened on, so a connect gets ECONNREFUSED
    whatever else runs on the host (a fixed port may be in use there).
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        yield f"127.0.0.1:{s.getsockname()[1]}"
