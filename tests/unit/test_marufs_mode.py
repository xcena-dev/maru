# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tests for marufs mode in AllocationManager, DaxMapper, MaruServer, and RpcHandlerMixin.

These tests verify that when mount_path is provided, the marufs backend
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

    def __init__(self, mount_path: str):
        self.mount_path = mount_path

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
    """Patch MarufsClient at the source module (lazy import target)."""
    with patch("marufs.MarufsClient", MockMarufsClient):
        yield MockMarufsClient


# ============================================================================
# AllocationManager — marufs mode
# ============================================================================


class TestAllocationManagerMarufs:
    """Test AllocationManager selects MarufsClient when mount_path is set."""

    def test_selects_marufs_client(self, mock_marufs):
        """Constructor with mount_path uses MarufsClient."""
        mgr = AllocationManager(mount_path="/mnt/marufs")
        assert isinstance(mgr._client, MockMarufsClient)
        assert mgr._client.mount_path == "/mnt/marufs"

    def test_selects_shm_client_without_mount_path(self):
        """Constructor without mount_path uses MaruShmClient (default)."""
        mgr = AllocationManager()
        # MockShmClient from conftest.py (autouse fixture)
        assert not isinstance(mgr._client, MockMarufsClient)

    def test_allocate_with_marufs(self, mock_marufs):
        """allocate() works with MarufsClient backend."""
        mgr = AllocationManager(mount_path="/mnt/marufs")
        handle = mgr.allocate("instance1", 4096)

        assert handle is not None
        assert handle.length == 4096
        assert handle.region_id == 1

    def test_allocate_multiple_with_marufs(self, mock_marufs):
        """Multiple allocations return distinct region_ids."""
        mgr = AllocationManager(mount_path="/mnt/marufs")
        h1 = mgr.allocate("inst1", 1024)
        h2 = mgr.allocate("inst2", 2048)

        assert h1.region_id != h2.region_id

    def test_release_with_marufs(self, mock_marufs):
        """release() calls MarufsClient.free()."""
        mgr = AllocationManager(mount_path="/mnt/marufs")
        handle = mgr.allocate("inst1", 4096)

        success = mgr.release("inst1", handle.region_id)
        assert success is True
        assert mgr.get_handle(handle.region_id) is None

    def test_disconnect_with_marufs(self, mock_marufs):
        """disconnect_client() frees allocations via MarufsClient."""
        mgr = AllocationManager(mount_path="/mnt/marufs")
        h1 = mgr.allocate("inst1", 1024)
        h2 = mgr.allocate("inst1", 2048)

        mgr.disconnect_client("inst1")
        assert mgr.get_handle(h1.region_id) is None
        assert mgr.get_handle(h2.region_id) is None


# ============================================================================
# DaxMapper — marufs mode
# ============================================================================


class TestDaxMapperMarufs:
    """Test DaxMapper selects MarufsClient when mount_path is set."""

    def test_selects_marufs_client(self, mock_marufs):
        """Constructor with mount_path uses MarufsClient."""
        mapper = DaxMapper(mount_path="/mnt/marufs")
        assert isinstance(mapper._client, MockMarufsClient)
        assert mapper._client.mount_path == "/mnt/marufs"

    def test_selects_shm_client_without_mount_path(self):
        """Constructor without mount_path uses MaruShmClient (default)."""
        mapper = DaxMapper()
        assert not isinstance(mapper._client, MockMarufsClient)

    def test_map_region_with_marufs(self, mock_marufs):
        """map_region() works with MarufsClient backend."""
        mapper = DaxMapper(mount_path="/mnt/marufs")
        handle = _make_handle(1, 4096)
        region = mapper.map_region(handle)

        assert region.region_id == 1
        assert region.size == 4096
        assert region.is_mapped

    def test_map_region_idempotent_marufs(self, mock_marufs):
        """Mapping same region twice returns cached result."""
        mapper = DaxMapper(mount_path="/mnt/marufs")
        handle = _make_handle(1, 4096)
        r1 = mapper.map_region(handle)
        r2 = mapper.map_region(handle)
        assert r1 is r2

    def test_unmap_region_with_marufs(self, mock_marufs):
        """unmap_region() works with MarufsClient backend."""
        mapper = DaxMapper(mount_path="/mnt/marufs")
        handle = _make_handle(1, 4096)
        mapper.map_region(handle)

        assert mapper.unmap_region(1) is True
        assert mapper.get_region(1) is None

    def test_close_with_marufs(self, mock_marufs):
        """close() unmaps all regions with MarufsClient backend."""
        mapper = DaxMapper(mount_path="/mnt/marufs")
        mapper.map_region(_make_handle(1, 4096))
        mapper.map_region(_make_handle(2, 2048))

        mapper.close()
        assert mapper.get_region(1) is None
        assert mapper.get_region(2) is None


# ============================================================================
# MaruServer — marufs mode
# ============================================================================


class TestMaruServerMarufs:
    """Test MaruServer mount_path property and pass-through."""

    def test_mount_path_property_set(self, mock_marufs):
        """mount_path property returns the configured path."""
        server = MaruServer(mount_path="/mnt/marufs")
        assert server.mount_path == "/mnt/marufs"

    def test_mount_path_property_none(self):
        """mount_path property returns None in DAX mode."""
        server = MaruServer()
        assert server.mount_path is None

    def test_request_alloc_with_marufs(self, mock_marufs):
        """request_alloc() works with marufs backend."""
        server = MaruServer(mount_path="/mnt/marufs")
        handle = server.request_alloc("inst1", 4096)
        assert handle is not None
        assert handle.length == 4096


# ============================================================================
# RpcHandlerMixin — mount_path in alloc response
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
    """Test that alloc response includes mount_path in marufs mode."""

    def test_alloc_response_includes_mount_path(self, mock_marufs):
        """Response contains mount_path when server is in marufs mode."""
        server = MaruServer(mount_path="/mnt/marufs")
        handler = ConcreteHandler(server)

        req = MockRequest(instance_id="inst1", size=4096, pool_id=ANY_POOL_ID)
        resp = handler._handle_request_alloc(req)

        assert resp["success"] is True
        assert resp["mount_path"] == "/mnt/marufs"

    def test_alloc_response_excludes_mount_path_in_dax_mode(self):
        """Response does NOT contain mount_path key in DAX mode."""
        server = MaruServer()
        handler = ConcreteHandler(server)

        req = MockRequest(instance_id="inst1", size=4096, pool_id=ANY_POOL_ID)
        resp = handler._handle_request_alloc(req)

        assert resp["success"] is True
        assert "mount_path" not in resp

    def test_alloc_failure_no_mount_path(self, mock_marufs, monkeypatch):
        """Failed alloc response does not include mount_path."""
        server = MaruServer(mount_path="/mnt/marufs")
        handler = ConcreteHandler(server)

        monkeypatch.setattr(
            server._allocation_manager,
            "allocate",
            lambda instance_id, size, pool_id=ANY_POOL_ID: None,
        )

        req = MockRequest(instance_id="inst1", size=4096, pool_id=ANY_POOL_ID)
        resp = handler._handle_request_alloc(req)

        assert resp["success"] is False
        assert "mount_path" not in resp


# ============================================================================
# MarufsClient._validate_mount_path — called from server path
# ============================================================================


class TestValidateMountPath:
    """Verify that _validate_mount_path is called when MarufsClient is created."""

    def test_validate_called_on_init(self, tmp_path):
        """_validate_mount_path is invoked during MarufsClient construction."""
        from marufs.client import MarufsClient

        with patch.object(MarufsClient, "_validate_mount_path") as mock_validate:
            MarufsClient(str(tmp_path))

        mock_validate.assert_called_once()

    def test_validate_rejects_non_directory(self):
        """Non-existent path raises ValueError."""
        from marufs.client import MarufsClient

        with pytest.raises(ValueError, match="is not a directory"):
            MarufsClient("/nonexistent/path/to/marufs")

    def test_validate_rejects_non_marufs_mount(self, tmp_path):
        """Existing directory that is not a marufs mount raises ValueError."""
        from marufs.client import MarufsClient

        with pytest.raises(ValueError, match="is not a marufs mount point"):
            MarufsClient(str(tmp_path))

    def test_server_allocation_passes_mount_path(self):
        """AllocationManager(mount_path=...) passes it to MarufsClient."""
        with patch("marufs.MarufsClient", MockMarufsClient):
            mgr = AllocationManager(mount_path="/mnt/marufs")
        assert mgr._client.mount_path == "/mnt/marufs"
