# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for MarufsClient.

VFS operations (create/open/delete/list/mmap) are tested against a real tmpdir.
ioctl operations (name_offset, find_name, perm_*) require the marufs kernel
module and are tested with mocked fcntl.ioctl.
"""

import mmap as mmap_module
import os
from unittest.mock import MagicMock, patch

import pytest

from maru_shm.types import MaruHandle


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mount_dir(tmp_path):
    """Provide a temporary directory as a fake marufs mount point."""
    return str(tmp_path)


@pytest.fixture
def client(mount_dir):
    """Create a MarufsClient pointed at a tmpdir."""
    from marufs.client import MarufsClient

    c = MarufsClient(mount_dir)
    yield c
    c.close()


# ============================================================================
# Region management (VFS operations — no kernel module needed)
# ============================================================================


class TestCreateRegion:
    def test_create_and_check_size(self, client, mount_dir):
        fd = client._create_region("test_region", 4096)
        assert fd >= 0
        path = os.path.join(mount_dir, "test_region")
        assert os.path.exists(path)
        assert os.path.getsize(path) == 4096

    def test_create_caches_fd(self, client):
        client._create_region("r1", 1024)
        assert client._get_fd("r1") is not None

    def test_create_sets_rdwr_mode(self, client):
        client._create_region("r1", 1024)
        assert client._fd_modes["r1"] is False  # False = not readonly


class TestOpenRegion:
    def test_open_readonly(self, client, mount_dir):
        # Create file first
        path = os.path.join(mount_dir, "existing")
        with open(path, "wb") as f:
            f.write(b"\x00" * 1024)

        fd = client._open_region("existing", readonly=True)
        assert fd >= 0
        assert client._fd_modes["existing"] is True

    def test_open_rdwr(self, client, mount_dir):
        path = os.path.join(mount_dir, "existing")
        with open(path, "wb") as f:
            f.write(b"\x00" * 1024)

        fd = client._open_region("existing", readonly=False)
        assert fd >= 0
        assert client._fd_modes["existing"] is False

    def test_open_cached_returns_same_fd(self, client, mount_dir):
        path = os.path.join(mount_dir, "cached")
        with open(path, "wb") as f:
            f.write(b"\x00" * 1024)

        fd1 = client._open_region("cached", readonly=True)
        fd2 = client._open_region("cached", readonly=True)
        assert fd1 == fd2

    def test_open_mode_mismatch_reopens(self, client, mount_dir):
        path = os.path.join(mount_dir, "mismatch")
        with open(path, "wb") as f:
            f.write(b"\x00" * 1024)

        client._open_region("mismatch", readonly=True)
        assert client._fd_modes["mismatch"] is True

        client._open_region("mismatch", readonly=False)
        # Mode should be updated after re-open
        assert client._fd_modes["mismatch"] is False


class TestDeleteRegion:
    def test_delete_removes_file(self, client, mount_dir):
        client._create_region("to_delete", 1024)
        path = os.path.join(mount_dir, "to_delete")
        assert os.path.exists(path)

        client._delete_region("to_delete")
        assert not os.path.exists(path)

    def test_delete_clears_fd_cache(self, client):
        client._create_region("to_delete", 1024)
        assert client._get_fd("to_delete") is not None

        client._delete_region("to_delete")
        assert client._get_fd("to_delete") is None


class TestListRegions:
    def test_list_empty(self, client):
        assert client._list_regions() == []

    def test_list_all(self, client):
        client._create_region("a_region", 1024)
        client._create_region("b_region", 1024)
        result = client._list_regions()
        assert result == ["a_region", "b_region"]

    def test_list_with_prefix(self, client):
        client._create_region("kv_1", 1024)
        client._create_region("kv_2", 1024)
        client._create_region("meta_1", 1024)
        result = client._list_regions(prefix="kv_")
        assert result == ["kv_1", "kv_2"]


class TestExists:
    def test_exists_true(self, client):
        client._create_region("present", 1024)
        assert client._exists("present") is True

    def test_exists_false(self, client):
        assert client._exists("absent") is False


# ============================================================================
# Validation
# ============================================================================


class TestValidateRegionName:
    def test_valid_names(self, client):
        for name in ["region_1", "kv-data", "ABC123", "a_b-c"]:
            client._validate_region_name(name)  # should not raise

    def test_invalid_names(self, client):
        for name in ["../etc/passwd", "foo/bar", "hello world", "a.b", ""]:
            with pytest.raises(ValueError):
                client._validate_region_name(name)

    def test_path_traversal(self, client):
        with pytest.raises(ValueError):
            client._validate_region_name("../escape")


# ============================================================================
# Memory mapping
# ============================================================================


class TestMmapRegion:
    def test_mmap_read(self, client, mount_dir):
        path = os.path.join(mount_dir, "mmap_test")
        with open(path, "wb") as f:
            f.write(b"hello" + b"\x00" * (4096 - 5))

        fd = client._open_region("mmap_test", readonly=True)
        mm = client._mmap_region(fd, 4096, prot=mmap_module.PROT_READ)
        assert mm[:5] == b"hello"
        mm.close()

    def test_mmap_write(self, client):
        fd = client._create_region("mmap_rw", 4096)
        mm = client._mmap_region(
            fd, 4096, prot=mmap_module.PROT_READ | mmap_module.PROT_WRITE
        )
        mm[:5] = b"world"
        assert mm[:5] == b"world"
        mm.close()


# ============================================================================
# MaruShmClient-compatible interface (alloc / free / mmap / munmap)
# ============================================================================


class TestAllocFree:
    @patch("marufs.client.MarufsClient.perm_set_default")
    def test_alloc_returns_handle(self, mock_perm, client):
        handle = client.alloc(8192)
        assert isinstance(handle, MaruHandle)
        assert handle.length == 8192
        assert handle.region_id > 0

    @patch("marufs.client.MarufsClient.perm_set_default")
    def test_alloc_creates_file(self, mock_perm, client, mount_dir):
        handle = client.alloc(4096)
        region_name = f"region_{handle.region_id}"
        path = os.path.join(mount_dir, region_name)
        assert os.path.exists(path)
        assert os.path.getsize(path) == 4096

    @patch("marufs.client.MarufsClient.perm_set_default")
    def test_alloc_calls_perm_set_default(self, mock_perm, client):
        client.alloc(4096)
        mock_perm.assert_called_once()

    @patch("marufs.client.MarufsClient.perm_set_default")
    def test_alloc_unique_ids(self, mock_perm, client):
        h1 = client.alloc(1024)
        h2 = client.alloc(1024)
        assert h1.region_id != h2.region_id

    @patch("marufs.client.MarufsClient.perm_set_default")
    def test_free_deletes_file(self, mock_perm, client, mount_dir):
        handle = client.alloc(4096)
        region_name = f"region_{handle.region_id}"
        path = os.path.join(mount_dir, region_name)
        assert os.path.exists(path)

        client.free(handle)
        assert not os.path.exists(path)

    @patch("marufs.client.MarufsClient.perm_set_default")
    def test_free_unknown_region(self, mock_perm, client):
        fake_handle = MaruHandle(region_id=9999, offset=0, length=4096, auth_token=0)
        # Should not raise, just log warning
        client.free(fake_handle)


class TestMmapMunmap:
    @patch("marufs.client.MarufsClient.perm_set_default")
    def test_mmap_write_and_read(self, mock_perm, client):
        handle = client.alloc(4096)
        mm = client.mmap(handle, prot=mmap_module.PROT_READ | mmap_module.PROT_WRITE)
        mm[:4] = b"test"
        assert mm[:4] == b"test"
        client.munmap(handle)

    @patch("marufs.client.MarufsClient.perm_set_default")
    def test_mmap_cached(self, mock_perm, client):
        handle = client.alloc(4096)
        mm1 = client.mmap(handle, prot=mmap_module.PROT_READ | mmap_module.PROT_WRITE)
        mm2 = client.mmap(handle, prot=mmap_module.PROT_READ | mmap_module.PROT_WRITE)
        assert mm1 is mm2
        client.munmap(handle)

    @patch("marufs.client.MarufsClient.perm_set_default")
    def test_mmap_shared_region(self, mock_perm, client, mount_dir):
        """Simulate mapping a region created by another instance."""
        # Create a file as if another instance made it
        region_name = "region_42"
        path = os.path.join(mount_dir, region_name)
        with open(path, "wb") as f:
            f.write(b"shared_data" + b"\x00" * (4096 - 11))

        # Build a handle as if from server lookup
        handle = MaruHandle(region_id=42, offset=0, length=4096, auth_token=0)
        mm = client.mmap(handle, prot=mmap_module.PROT_READ)
        assert mm[:11] == b"shared_data"
        client.munmap(handle)

    @patch("marufs.client.MarufsClient.perm_set_default")
    def test_munmap_clears_cache(self, mock_perm, client):
        handle = client.alloc(4096)
        client.mmap(handle, prot=mmap_module.PROT_READ | mmap_module.PROT_WRITE)
        client.munmap(handle)
        # Cache should be cleared
        assert handle.region_id not in client._mmap_cache


# ============================================================================
# ioctl operations (mocked — require marufs kernel module)
# ============================================================================


class TestIoctlNameOffset:
    @patch("marufs.client.fcntl.ioctl")
    def test_name_offset(self, mock_ioctl, client):
        client.name_offset(fd=5, name="key1", offset=1024)
        mock_ioctl.assert_called_once()
        args = mock_ioctl.call_args
        assert args[0][0] == 5  # fd

    @patch("marufs.client.fcntl.ioctl")
    def test_name_offset_bytes(self, mock_ioctl, client):
        client.name_offset(fd=5, name=b"key1", offset=0)
        mock_ioctl.assert_called_once()

    @patch("marufs.client.fcntl.ioctl")
    def test_name_offset_with_hash(self, mock_ioctl, client):
        client.name_offset(fd=5, name="key1", offset=0, name_hash=12345)
        mock_ioctl.assert_called_once()


class TestIoctlFindName:
    @patch("marufs.client.fcntl.ioctl")
    def test_find_name_found(self, mock_ioctl, client):
        def side_effect(fd, cmd, req):
            req.region_name = b"region_1\x00" + b"\x00" * 55
            req.offset = 4096

        mock_ioctl.side_effect = side_effect
        result = client.find_name(fd=5, name="key1")
        assert result == ("region_1", 4096)

    @patch("marufs.client.fcntl.ioctl")
    def test_find_name_not_found(self, mock_ioctl, client):
        mock_ioctl.side_effect = OSError(2, "No such file or directory")  # ENOENT
        result = client.find_name(fd=5, name="missing")
        assert result is None

    @patch("marufs.client.fcntl.ioctl")
    def test_find_name_other_error_raises(self, mock_ioctl, client):
        mock_ioctl.side_effect = OSError(13, "Permission denied")
        with pytest.raises(OSError):
            client.find_name(fd=5, name="key1")


class TestIoctlClearName:
    @patch("marufs.client.fcntl.ioctl")
    def test_clear_name(self, mock_ioctl, client):
        client.clear_name(fd=5, name="key1")
        mock_ioctl.assert_called_once()


class TestIoctlPermissions:
    @patch("marufs.client.fcntl.ioctl")
    def test_perm_set_default(self, mock_ioctl, client):
        client.perm_set_default(fd=5, perms=0x001F)
        mock_ioctl.assert_called_once()

    @patch("marufs.client.fcntl.ioctl")
    def test_perm_grant(self, mock_ioctl, client):
        client.perm_grant(fd=5, node_id=0, pid=1234, perms=0x0003)
        mock_ioctl.assert_called_once()

    @patch("marufs.client.fcntl.ioctl")
    def test_perm_revoke(self, mock_ioctl, client):
        client.perm_revoke(fd=5, node_id=0, pid=1234)
        mock_ioctl.assert_called_once()


# ============================================================================
# Batch operations (mocked ioctl)
# ============================================================================


class TestBatchNameOffset:
    @patch("marufs.client.fcntl.ioctl")
    def test_batch_empty(self, mock_ioctl, client):
        result = client.batch_name_offset(fd=5, names=[], offsets=[])
        assert result == []
        mock_ioctl.assert_not_called()

    @patch("marufs.client.fcntl.ioctl")
    def test_batch_single(self, mock_ioctl, client):
        def side_effect(fd, cmd, req):
            pass  # entries[i].status defaults to 0

        mock_ioctl.side_effect = side_effect
        result = client.batch_name_offset(
            fd=5, names=["k1", "k2"], offsets=[0, 1024]
        )
        assert len(result) == 2
        mock_ioctl.assert_called_once()


class TestBatchFindName:
    @patch("marufs.client.fcntl.ioctl")
    def test_batch_find_empty(self, mock_ioctl, client):
        result = client.batch_find_name(fd=5, names=[])
        assert result == []
        mock_ioctl.assert_not_called()

    @patch("marufs.client.fcntl.ioctl")
    def test_batch_find_single(self, mock_ioctl, client):
        def side_effect(fd, cmd, req):
            pass  # entries[i].status defaults to non-zero → None results

        mock_ioctl.side_effect = side_effect
        result = client.batch_find_name(fd=5, names=["k1"])
        assert len(result) == 1
        mock_ioctl.assert_called_once()


# ============================================================================
# Lifecycle
# ============================================================================


class TestClose:
    def test_close_clears_all_fds(self, client):
        client._create_region("r1", 1024)
        client._create_region("r2", 1024)
        assert len(client._fds) == 2

        client.close()
        assert len(client._fds) == 0
        assert len(client._fd_modes) == 0

    def test_close_dir_fd(self, client, mount_dir):
        dir_fd = client.get_dir_fd()
        assert dir_fd is not None

        client.close()
        assert client._dir_fd is None

    def test_double_close(self, client):
        client.close()
        client.close()  # should not raise


# ============================================================================
# Node ID
# ============================================================================


class TestGetNodeId:
    def test_get_node_id_not_mounted(self, client):
        """tmpdir is not a marufs mount — should raise RuntimeError."""
        with pytest.raises(RuntimeError, match="not found in /proc/mounts"):
            client.get_node_id()

    def test_get_node_id_cached(self, client):
        client._node_id = 42
        assert client.get_node_id() == 42
