# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for MarufsClient.

MarufsClient delegates allocation/free to ResourceManagerClient and
performs mmap via standard mmap module. All RM communication is mocked.
Real file descriptors are avoided entirely — mmap_module.mmap is patched
to return anonymous mmaps so pytest's log-handler fd is never corrupted.
"""

import mmap as mmap_module
from unittest.mock import MagicMock, patch

import pytest

from maru_common.constants import ANY_POOL_ID
from maru_common.types import DaxType, MaruHandle

# Fake fd constant — never a real OS fd.
_FAKE_FD = 9999

# Save real mmap constructor before any patch replaces it.
_real_mmap = mmap_module.mmap


def _anon_mmap(size=4096):
    """Create an anonymous mmap (fd=-1) safe for tests."""
    return _real_mmap(-1, size)


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_rm():
    """Create a mock ResourceManagerClient."""
    return MagicMock()


@pytest.fixture
def client(mock_rm):
    """Create a MarufsClient with mocked RM and patched mmap/os.close."""
    with (
        patch("marufs.client.ResourceManagerClient", return_value=mock_rm),
        patch("marufs.client.os.close"),
        patch("marufs.client.mmap_module.mmap", side_effect=lambda *a, **kw: _anon_mmap()),
    ):
        from marufs.client import MarufsClient

        c = MarufsClient()
        yield c
        c.close()


# ============================================================================
# Allocation / Free
# ============================================================================


class TestAllocFree:
    def test_alloc_returns_handle(self, client, mock_rm):
        """alloc() returns a MaruHandle from RM."""
        expected_handle = MaruHandle(region_id=1, offset=0, length=8192, auth_token=0)
        mock_rm.alloc.return_value = (expected_handle, _FAKE_FD)

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
        mock_rm.alloc.return_value = (handle, _FAKE_FD)

        client.alloc(4096)
        client.free(handle)

        mock_rm.free.assert_called_once_with(handle)
        assert 3 not in client._fd_cache

    def test_alloc_with_explicit_pool_id(self, client, mock_rm):
        """alloc() forwards explicit pool_id to RM."""
        handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=0)
        mock_rm.alloc.return_value = (handle, _FAKE_FD)

        client.alloc(4096, pool_id=7)
        mock_rm.alloc.assert_called_once_with(4096, pool_id=7, pool_type=DaxType.MARUFS)

    def test_free_closes_mmap_and_fd(self, client, mock_rm):
        """free() closes cached mmap before closing fd."""
        handle = MaruHandle(region_id=3, offset=0, length=4096, auth_token=0)
        mock_rm.alloc.return_value = (handle, _FAKE_FD)

        client.alloc(4096)
        mm = client.mmap(handle, prot=mmap_module.PROT_READ | mmap_module.PROT_WRITE)
        assert not mm.closed

        client.free(handle)
        assert mm.closed
        assert 3 not in client._fd_cache
        assert 3 not in client._mmap_cache
        mock_rm.free.assert_called_once_with(handle)

    def test_free_unknown_region(self, client, mock_rm):
        """free() on unknown region should not raise."""
        fake_handle = MaruHandle(region_id=9999, offset=0, length=4096, auth_token=0)
        client.free(fake_handle)
        mock_rm.free.assert_called_once_with(fake_handle)


# ============================================================================
# Memory mapping
# ============================================================================


class TestMmapMunmap:
    def test_mmap_write_and_read(self, client, mock_rm):
        """mmap() returns a writable mmap object."""
        handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=0)
        mock_rm.alloc.return_value = (handle, _FAKE_FD)

        client.alloc(4096)
        mm = client.mmap(handle, prot=mmap_module.PROT_READ | mmap_module.PROT_WRITE)
        mm[:4] = b"test"
        assert mm[:4] == b"test"
        client.munmap(handle)

    def test_mmap_cached(self, client, mock_rm):
        """Second mmap() returns cached object."""
        handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=0)
        mock_rm.alloc.return_value = (handle, _FAKE_FD)

        client.alloc(4096)
        prot = mmap_module.PROT_READ | mmap_module.PROT_WRITE
        mm1 = client.mmap(handle, prot=prot)
        mm2 = client.mmap(handle, prot=prot)
        assert mm1 is mm2
        client.munmap(handle)

    def test_mmap_prot_mismatch_raises(self, client, mock_rm):
        """Remapping with different prot raises ValueError."""
        handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=0)
        mock_rm.alloc.return_value = (handle, _FAKE_FD)

        client.alloc(4096)
        client.mmap(handle, prot=mmap_module.PROT_READ | mmap_module.PROT_WRITE)
        with pytest.raises(ValueError, match="cannot remap"):
            client.mmap(handle, prot=mmap_module.PROT_READ)
        client.munmap(handle)

    def test_mmap_shared_region_via_get_fd(self, client, mock_rm):
        """mmap() on unallocated region requests fd via RM.get_fd()."""
        handle = MaruHandle(region_id=42, offset=0, length=4096, auth_token=0)
        mock_rm.get_fd.return_value = _FAKE_FD

        mm = client.mmap(handle, prot=mmap_module.PROT_READ)
        assert mm is not None
        mock_rm.get_fd.assert_called_once_with(handle)
        client.munmap(handle)

    def test_mmap_invalid_region_id(self, client):
        """mmap() with negative region_id raises ValueError."""
        handle = MaruHandle(region_id=-1, offset=0, length=4096, auth_token=0)
        with pytest.raises(ValueError, match="Invalid region_id"):
            client.mmap(handle, prot=mmap_module.PROT_READ)

    def test_mmap_read_only(self, client, mock_rm):
        """mmap() with PROT_READ uses ACCESS_READ."""
        handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=0)
        mock_rm.alloc.return_value = (handle, _FAKE_FD)

        client.alloc(4096)
        mm = client.mmap(handle, prot=mmap_module.PROT_READ)
        assert mm[:4] == b"\x00" * 4
        client.munmap(handle)

    def test_munmap_clears_cache(self, client, mock_rm):
        """munmap() removes entry from mmap cache."""
        handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=0)
        mock_rm.alloc.return_value = (handle, _FAKE_FD)

        client.alloc(4096)
        client.mmap(handle, prot=mmap_module.PROT_READ | mmap_module.PROT_WRITE)
        client.munmap(handle)
        assert handle.region_id not in client._mmap_cache

    def test_munmap_unmapped_region_is_noop(self, client):
        """munmap() on region not in cache does not raise."""
        handle = MaruHandle(region_id=999, offset=0, length=4096, auth_token=0)
        client.munmap(handle)  # should not raise


# ============================================================================
# Lifecycle
# ============================================================================


class TestClose:
    def test_close_clears_all(self, client, mock_rm):
        """close() clears all fd and mmap caches."""
        handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=0)
        mock_rm.alloc.return_value = (handle, _FAKE_FD)

        client.alloc(4096)
        client.mmap(handle, prot=mmap_module.PROT_READ | mmap_module.PROT_WRITE)

        client.close()
        assert len(client._fd_cache) == 0
        assert len(client._mmap_cache) == 0

    def test_close_suppresses_mmap_exception(self, client, mock_rm):
        """close() suppresses mmap.close() exception and clears cache."""
        handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=0)
        mock_rm.alloc.return_value = (handle, _FAKE_FD)

        client.alloc(4096)
        # Inject a broken mmap entry
        broken_mm = MagicMock()
        broken_mm.close.side_effect = Exception("mmap close failed")
        client._mmap_cache[1] = (broken_mm, mmap_module.PROT_READ)

        client.close()
        assert len(client._mmap_cache) == 0
        assert len(client._fd_cache) == 0

    def test_close_suppresses_fd_exception(self, client, mock_rm):
        """close() suppresses os.close() OSError and clears cache."""
        handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=0)
        mock_rm.alloc.return_value = (handle, _FAKE_FD)

        client.alloc(4096)

        # Temporarily replace the patched os.close with one that raises
        import marufs.client as _mod

        _mod.os.close = MagicMock(side_effect=OSError("bad fd"))
        client.close()
        assert len(client._fd_cache) == 0

    def test_context_manager(self, mock_rm):
        """MarufsClient supports with-statement and calls close() on exit."""
        with (
            patch("marufs.client.ResourceManagerClient", return_value=mock_rm),
            patch("marufs.client.os.close"),
        ):
            from marufs.client import MarufsClient

            with MarufsClient() as c:
                assert c is not None
                handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=0)
                mock_rm.alloc.return_value = (handle, _FAKE_FD)
                c.alloc(4096)
                assert len(c._fd_cache) == 1

            # After exiting context, close() was called by __exit__
            assert len(c._fd_cache) == 0

    def test_double_close(self, client):
        """Calling close() twice should not raise."""
        client.close()
        client.close()
