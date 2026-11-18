# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Direct I/O on ext4 when the kernel predates STATX_DIOALIGN."""

import errno
import io
import os
import struct
from types import SimpleNamespace

import pytest
from ssd_test_storage import probe_direct_io

from maru_storage_service.drivers import ssd_alignment as dio
from maru_storage_service.drivers.ssd import SsdDriver, create_ssd_pool


@pytest.fixture
def legacy_ext4(monkeypatch):
    state = SimpleNamespace(block=4096, flags=0, kind="ext4", options={"rw"})

    # Linux 5.15 returns statx successfully without STATX_DIOALIGN in its mask.
    def statx(fd, path, flags, mask, result):
        return 0

    def ioctl(fd, command, buffer):
        if command == dio._XFS_IOC_DIOINFO:
            raise OSError(errno.ENOTTY, "not XFS")
        assert command == dio._FS_IOC_GETFLAGS
        return struct.pack("=I", state.flags)

    monkeypatch.setattr(dio, "_LIBC", SimpleNamespace(statx=statx))
    monkeypatch.setattr(dio.fcntl, "ioctl", ioctl)
    monkeypatch.setattr(dio, "_mount_info", lambda fd: (state.kind, state.options))
    monkeypatch.setattr(
        dio.os, "fstatvfs", lambda fd: SimpleNamespace(f_bsize=state.block)
    )
    return state


@pytest.mark.parametrize("block", [1024, 4096, 65536])
def test_old_ext4_uses_its_reported_block_size(legacy_ext4, block):
    legacy_ext4.block = block
    assert dio.direct_alignment(42) == dio.DioAlignment(block, block)


@pytest.mark.parametrize("kind", ["tmpfs", "overlay", "nfs", "xfs", "ext3"])
def test_unknown_filesystems_do_not_get_an_assumed_alignment(legacy_ext4, kind):
    legacy_ext4.kind = kind
    with pytest.raises(dio.SsdAlignmentError, match="alignment is unknown"):
        dio.direct_alignment(42)


@pytest.mark.parametrize("flags", [0x800, 0x4000, 0x100000, 0x10000000])
def test_ext4_files_that_fall_back_to_buffered_io_are_refused(legacy_ext4, flags):
    legacy_ext4.flags = flags
    with pytest.raises(dio.SsdAlignmentError, match="does not support direct I/O"):
        dio.direct_alignment(42)


def test_ext4_data_journaling_mount_is_refused(legacy_ext4):
    legacy_ext4.options.add("data=journal")
    with pytest.raises(dio.SsdAlignmentError, match="does not support direct I/O"):
        dio.direct_alignment(42)


@pytest.mark.parametrize("block", [0, 513, 1 << 21])
def test_invalid_ext4_block_size_is_refused(legacy_ext4, block):
    legacy_ext4.block = block
    with pytest.raises(dio.SsdAlignmentError, match="invalid direct I/O alignment"):
        dio.direct_alignment(42)


def test_mount_lookup_matches_the_fd_mount_id(monkeypatch):
    files = {
        "/proc/self/fdinfo/42": "pos:\t0\nflags:\t0100002\nmnt_id:\t37\nino:\t123\n",
        "/proc/self/mountinfo": (
            "36 1 0:6 / / rw - tmpfs tmpfs rw\n"
            "37 1 8:1 /runner\\040work /__w rw shared:2 - ext4 /dev/sda1 rw,data=ordered\n"
            "38 1 0:7 / /overlay rw - overlay overlay rw\n"
        ),
    }
    monkeypatch.setattr(
        dio, "open", lambda path: io.StringIO(files[path]), raising=False
    )
    assert dio._mount_info(42) == ("ext4", {"rw", "data=ordered"})


def test_missing_mount_id_does_not_guess_a_filesystem(monkeypatch):
    monkeypatch.setattr(
        dio, "open", lambda path: io.StringIO("pos:\t0\n"), raising=False
    )
    assert dio._mount_info(42) is None


def test_ext4_without_statx_can_probe_write_read_and_reopen(ssd_dir, monkeypatch):
    fd = os.open(ssd_dir, os.O_RDONLY | os.O_DIRECTORY)
    try:
        mount = dio._mount_info(fd)
        if mount is None or mount[0] != "ext4":
            pytest.skip("requires a real ext4 filesystem")
        block = os.fstatvfs(fd).f_bsize
    finally:
        os.close(fd)

    monkeypatch.setattr(dio, "_statx_alignment", lambda fd: None)
    assert probe_direct_io(ssd_dir) == dio.DioAlignment(block, block)
    page = max(16384, block)
    pool = create_ssd_pool("node", ssd_dir, 2 * page, page)
    info = pool.info()
    try:
        assert pool.alignment == dio.DioAlignment(block, block)
        with dio.aligned_buffer(page, block) as view:
            view[:5] = b"hello"
            pool.write(0, view, 5)
    finally:
        pool.close()
    pool = SsdDriver().open(info)
    try:
        with dio.aligned_buffer(page, block) as view:
            pool.read(0, 5, view)
            assert bytes(view[:5]) == b"hello"
            assert bytes(view[5:block]) == bytes(block - 5)
    finally:
        pool.close()
