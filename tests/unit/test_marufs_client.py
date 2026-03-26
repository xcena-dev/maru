# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for MarufsClient.

MarufsClient delegates allocation/free to ResourceManagerClient and
performs mmap via standard mmap module. All RM communication is mocked.
"""

import mmap as mmap_module
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from maru_common.constants import ANY_POOL_ID
from maru_common.types import DaxType, MaruHandle

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_rm():
    """Create a mock ResourceManagerClient."""
    return MagicMock()


@pytest.fixture
def client(mock_rm):
    """Create a MarufsClient with mocked ResourceManagerClient."""
    with patch("marufs.client.ResourceManagerClient", return_value=mock_rm):
        from marufs.client import MarufsClient

        c = MarufsClient()
    yield c
    c.close()


@pytest.fixture
def temp_region_file():
    """Create a temporary file suitable for mmap testing."""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(b"\x00" * 4096)
        path = f.name
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


# ============================================================================
# Allocation / Free
# ============================================================================


class TestAllocFree:
    def test_alloc_returns_handle(self, client, mock_rm):
        """alloc() returns a MaruHandle from RM."""
        expected_handle = MaruHandle(region_id=1, offset=0, length=8192, auth_token=0)
        mock_rm.alloc.return_value = (expected_handle, 10)

        handle = client.alloc(8192)
        assert isinstance(handle, MaruHandle)
        assert handle.length == 8192
        assert handle.region_id == 1
        mock_rm.alloc.assert_called_once_with(
            8192, pool_id=ANY_POOL_ID, pool_type=DaxType.MARUFS
        )

    def test_alloc_caches_fd(self, client, mock_rm):
        """alloc() caches the fd returned by RM."""
        handle = MaruHandle(region_id=5, offset=0, length=4096, auth_token=0)
        mock_rm.alloc.return_value = (handle, 42)

        client.alloc(4096)
        assert client._fd_cache[5] == 42

    def test_alloc_unique_ids(self, client, mock_rm):
        """Multiple allocs return distinct handles."""
        h1 = MaruHandle(region_id=1, offset=0, length=1024, auth_token=0)
        h2 = MaruHandle(region_id=2, offset=0, length=1024, auth_token=0)
        mock_rm.alloc.side_effect = [(h1, 10), (h2, 11)]

        r1 = client.alloc(1024)
        r2 = client.alloc(1024)
        assert r1.region_id != r2.region_id

    def test_free_calls_rm(self, client, mock_rm):
        """free() delegates to RM and cleans caches."""
        handle = MaruHandle(region_id=3, offset=0, length=4096, auth_token=0)
        mock_rm.alloc.return_value = (handle, 10)

        client.alloc(4096)

        # Patch os.close to avoid closing an invalid fd
        with patch("marufs.client.os.close"):
            client.free(handle)

        mock_rm.free.assert_called_once_with(handle)
        assert 3 not in client._fd_cache

    def test_free_unknown_region(self, client, mock_rm):
        """free() on unknown region should not raise."""
        fake_handle = MaruHandle(region_id=9999, offset=0, length=4096, auth_token=0)
        client.free(fake_handle)
        mock_rm.free.assert_called_once_with(fake_handle)


# ============================================================================
# Memory mapping
# ============================================================================


class TestMmapMunmap:
    def test_mmap_write_and_read(self, client, mock_rm, temp_region_file):
        """mmap() returns a writable mmap object."""
        handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=0)
        fd = os.open(temp_region_file, os.O_RDWR)
        mock_rm.alloc.return_value = (handle, fd)

        client.alloc(4096)
        mm = client.mmap(handle, prot=mmap_module.PROT_READ | mmap_module.PROT_WRITE)
        mm[:4] = b"test"
        assert mm[:4] == b"test"
        client.munmap(handle)

    def test_mmap_cached(self, client, mock_rm, temp_region_file):
        """Second mmap() returns cached object."""
        handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=0)
        fd = os.open(temp_region_file, os.O_RDWR)
        mock_rm.alloc.return_value = (handle, fd)

        client.alloc(4096)
        prot = mmap_module.PROT_READ | mmap_module.PROT_WRITE
        mm1 = client.mmap(handle, prot=prot)
        mm2 = client.mmap(handle, prot=prot)
        assert mm1 is mm2
        client.munmap(handle)

    def test_mmap_prot_mismatch_raises(self, client, mock_rm, temp_region_file):
        """Remapping with different prot raises ValueError."""
        handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=0)
        fd = os.open(temp_region_file, os.O_RDWR)
        mock_rm.alloc.return_value = (handle, fd)

        client.alloc(4096)
        client.mmap(handle, prot=mmap_module.PROT_READ | mmap_module.PROT_WRITE)
        with pytest.raises(ValueError, match="cannot remap"):
            client.mmap(handle, prot=mmap_module.PROT_READ)
        client.munmap(handle)

    def test_mmap_shared_region_via_get_fd(self, client, mock_rm, temp_region_file):
        """mmap() on unallocated region requests fd via RM.get_fd()."""
        handle = MaruHandle(region_id=42, offset=0, length=4096, auth_token=0)
        fd = os.open(temp_region_file, os.O_RDONLY)
        mock_rm.get_fd.return_value = fd

        mm = client.mmap(handle, prot=mmap_module.PROT_READ)
        assert mm is not None
        mock_rm.get_fd.assert_called_once_with(handle)
        client.munmap(handle)

    def test_mmap_invalid_region_id(self, client):
        """mmap() with negative region_id raises ValueError."""
        handle = MaruHandle(region_id=-1, offset=0, length=4096, auth_token=0)
        with pytest.raises(ValueError, match="Invalid region_id"):
            client.mmap(handle, prot=mmap_module.PROT_READ)

    def test_munmap_clears_cache(self, client, mock_rm, temp_region_file):
        """munmap() removes entry from mmap cache."""
        handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=0)
        fd = os.open(temp_region_file, os.O_RDWR)
        mock_rm.alloc.return_value = (handle, fd)

        client.alloc(4096)
        client.mmap(handle, prot=mmap_module.PROT_READ | mmap_module.PROT_WRITE)
        client.munmap(handle)
        assert handle.region_id not in client._mmap_cache


# ============================================================================
# Lifecycle
# ============================================================================


class TestClose:
    def test_close_clears_all(self, client, mock_rm, temp_region_file):
        """close() clears all fd and mmap caches."""
        handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=0)
        fd = os.open(temp_region_file, os.O_RDWR)
        mock_rm.alloc.return_value = (handle, fd)

        client.alloc(4096)
        client.mmap(handle, prot=mmap_module.PROT_READ | mmap_module.PROT_WRITE)

        client.close()
        assert len(client._fd_cache) == 0
        assert len(client._mmap_cache) == 0

    def test_double_close(self, client):
        """Calling close() twice should not raise."""
        client.close()
        client.close()
