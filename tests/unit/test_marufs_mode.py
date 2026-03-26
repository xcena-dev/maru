# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tests for marufs mode in AllocationManager, DaxMapper, MaruServer, and RpcHandlerMixin.

These tests verify that when pool_type="marufs" is provided, the marufs backend
(MarufsClient) is selected instead of the default MaruShmClient.
"""

import mmap
from unittest.mock import patch

import pytest
from conftest import _make_handle

from maru_common import ANY_POOL_ID
from maru_handler.memory import DaxMapper
from maru_server.allocation_manager import AllocationManager
from maru_server.rpc_handler_mixin import RpcHandlerMixin
from maru_server.server import MaruServer
from maru_shm import MaruHandle

# ============================================================================
# MockMarufsClient — replaces real MarufsClient in marufs mode tests
# ============================================================================

_marufs_alloc_counter = 0
_marufs_mmap_objects: list[mmap.mmap] = []


class MockMarufsClient:
    """Mock MarufsClient for unit tests without a real marufs mount."""

    def __init__(self):
        pass

    def alloc(self, size, pool_id=0):
        global _marufs_alloc_counter
        _marufs_alloc_counter += 1
        return MaruHandle(
            region_id=_marufs_alloc_counter, offset=0, length=size, auth_token=0
        )

    def free(self, handle):
        pass

    def mmap(self, handle, prot, flags=0):
        obj = mmap.mmap(-1, handle.length)
        _marufs_mmap_objects.append(obj)
        return obj

    def munmap(self, handle):
        pass

    def close(self):
        pass


@pytest.fixture(autouse=True)
def _reset_marufs_mock():
    """Reset mock state before each test."""
    global _marufs_alloc_counter, _marufs_mmap_objects
    _marufs_alloc_counter = 0
    _marufs_mmap_objects = []
    yield
    for obj in _marufs_mmap_objects:
        try:
            obj.close()
        except Exception:
            pass
    _marufs_mmap_objects = []


@pytest.fixture
def mock_marufs():
    """Patch MarufsClient at all import sites."""
    with (
        patch("marufs.MarufsClient", MockMarufsClient),
        patch("maru_server.allocation_manager.MarufsClient", MockMarufsClient),
        patch("maru_handler.memory.mapper.MarufsClient", MockMarufsClient),
    ):
        yield MockMarufsClient


# ============================================================================
# AllocationManager — marufs mode
# ============================================================================


class TestAllocationManagerMarufs:
    """Test AllocationManager selects MarufsClient when pool_type=marufs."""

    def test_has_both_clients(self, mock_marufs):
        """Constructor creates both _dax_client and _marufs_client."""
        mgr = AllocationManager()
        assert hasattr(mgr, "_dax_client")
        assert hasattr(mgr, "_marufs_client")
        assert isinstance(mgr._marufs_client, MockMarufsClient)

    def test_allocate_with_marufs(self, mock_marufs):
        """allocate(pool_type='marufs') uses MarufsClient backend."""
        mgr = AllocationManager()
        handle = mgr.allocate("instance1", 4096, pool_type="marufs")

        assert handle is not None
        assert handle.length == 4096
        assert handle.region_id == 1

    def test_allocate_multiple_with_marufs(self, mock_marufs):
        """Multiple allocations with marufs return distinct region_ids."""
        mgr = AllocationManager()
        h1 = mgr.allocate("inst1", 1024, pool_type="marufs")
        h2 = mgr.allocate("inst2", 2048, pool_type="marufs")

        assert h1.region_id != h2.region_id

    def test_release_with_marufs(self, mock_marufs):
        """release() works after marufs allocation."""
        mgr = AllocationManager()
        handle = mgr.allocate("inst1", 4096, pool_type="marufs")

        success = mgr.release("inst1", handle.region_id)
        assert success is True
        assert mgr.get_handle(handle.region_id) is None

    def test_disconnect_with_marufs(self, mock_marufs):
        """disconnect_client() frees marufs allocations."""
        mgr = AllocationManager()
        h1 = mgr.allocate("inst1", 1024, pool_type="marufs")
        h2 = mgr.allocate("inst1", 2048, pool_type="marufs")

        mgr.disconnect_client("inst1")
        assert mgr.get_handle(h1.region_id) is None
        assert mgr.get_handle(h2.region_id) is None


# ============================================================================
# DaxMapper — marufs mode
# ============================================================================


class TestDaxMapperMarufs:
    """Test DaxMapper selects MarufsClient when pool_type=marufs."""

    def test_selects_marufs_client(self, mock_marufs):
        """Constructor with pool_type='marufs' uses MarufsClient."""
        mapper = DaxMapper(pool_type="marufs")
        assert isinstance(mapper._client, MockMarufsClient)

    def test_selects_shm_client_by_default(self):
        """Constructor without pool_type uses MaruShmClient (default)."""
        mapper = DaxMapper()
        assert not isinstance(mapper._client, MockMarufsClient)

    def test_map_region_with_marufs(self, mock_marufs):
        """map_region() works with MarufsClient backend."""
        mapper = DaxMapper(pool_type="marufs")
        handle = _make_handle(1, 4096)
        region = mapper.map_region(handle)

        assert region.region_id == 1
        assert region.size == 4096
        assert region.is_mapped

    def test_map_region_idempotent_marufs(self, mock_marufs):
        """Mapping same region twice returns cached result."""
        mapper = DaxMapper(pool_type="marufs")
        handle = _make_handle(1, 4096)
        r1 = mapper.map_region(handle)
        r2 = mapper.map_region(handle)
        assert r1 is r2

    def test_unmap_region_with_marufs(self, mock_marufs):
        """unmap_region() works with MarufsClient backend."""
        mapper = DaxMapper(pool_type="marufs")
        handle = _make_handle(1, 4096)
        mapper.map_region(handle)

        assert mapper.unmap_region(1) is True
        assert mapper.get_region(1) is None

    def test_close_with_marufs(self, mock_marufs):
        """close() unmaps all regions with MarufsClient backend."""
        mapper = DaxMapper(pool_type="marufs")
        mapper.map_region(_make_handle(1, 4096))
        mapper.map_region(_make_handle(2, 2048))

        mapper.close()
        assert mapper.get_region(1) is None
        assert mapper.get_region(2) is None


# ============================================================================
# MaruServer — marufs mode
# ============================================================================


class TestMaruServerMarufs:
    """Test MaruServer pool_type pass-through."""

    def test_request_alloc_with_marufs(self, mock_marufs):
        """request_alloc(pool_type='marufs') works."""
        server = MaruServer()
        handle = server.request_alloc("inst1", 4096, pool_type="marufs")
        assert handle is not None
        assert handle.length == 4096

    def test_request_alloc_default_devdax(self):
        """request_alloc() defaults to devdax pool_type."""
        server = MaruServer()
        handle = server.request_alloc("inst1", 4096)
        assert handle is not None


# ============================================================================
# RpcHandlerMixin — pool_type in alloc request
# ============================================================================


class ConcreteHandler(RpcHandlerMixin):
    """Concrete class using the mixin for testing."""

    def __init__(self, server):
        self._server = server


class MockRequest:
    """Mock request object for testing handlers."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class TestRpcHandlerMixinMarufs:
    """Test that alloc request pool_type is forwarded correctly."""

    def test_alloc_with_marufs_pool_type(self, mock_marufs):
        """pool_type='marufs' in request is forwarded to server."""
        server = MaruServer()
        handler = ConcreteHandler(server)

        req = MockRequest(
            instance_id="inst1", size=4096, pool_id=ANY_POOL_ID, pool_type="marufs"
        )
        resp = handler._handle_request_alloc(req)

        assert resp["success"] is True
        assert "handle" in resp

    def test_alloc_default_devdax(self):
        """Request without pool_type defaults to 'devdax'."""
        server = MaruServer()
        handler = ConcreteHandler(server)

        req = MockRequest(
            instance_id="inst1", size=4096, pool_id=ANY_POOL_ID, pool_type="devdax"
        )
        resp = handler._handle_request_alloc(req)

        assert resp["success"] is True

    def test_alloc_failure_response(self, mock_marufs, monkeypatch):
        """Failed alloc response has success=False."""
        server = MaruServer()
        handler = ConcreteHandler(server)

        monkeypatch.setattr(
            server._allocation_manager,
            "allocate",
            lambda instance_id, size, pool_id=ANY_POOL_ID, pool_type="devdax": None,
        )

        req = MockRequest(
            instance_id="inst1", size=4096, pool_id=ANY_POOL_ID, pool_type="marufs"
        )
        resp = handler._handle_request_alloc(req)

        assert resp["success"] is False
