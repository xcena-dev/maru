# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Linux direct I/O limits and caller-owned aligned buffers."""

import ctypes
import errno
import fcntl
import math
import mmap
import os
import struct
from contextlib import contextmanager
from dataclasses import dataclass

_STATX_DIOALIGN = 0x2000
_AT_EMPTY_PATH = 0x1000
_XFS_IOC_DIOINFO = 0x800C581E
# Linux FS_IOC_GETFLAGS is _IOR('f', 1, long); the returned flags are a u32.
_FS_IOC_GETFLAGS = 0x80006601 | (ctypes.sizeof(ctypes.c_long) << 16)
_EXT4_BUFFERED_FLAGS = (
    0x00000800  # FS_ENCRYPT_FL
    | 0x00004000  # FS_JOURNAL_DATA_FL
    | 0x00100000  # FS_VERITY_FL
    | 0x10000000  # FS_INLINE_DATA_FL
)
_LIBC = ctypes.CDLL(None, use_errno=True)


class SsdAlignmentError(RuntimeError):
    """Direct I/O alignment is unsupported or cannot be established."""


@dataclass(frozen=True)
class DioAlignment:
    memory: int
    offset: int

    def __post_init__(self):
        for value in (self.memory, self.offset):
            if value <= 0 or value & (value - 1) or value > 1 << 20:
                raise SsdAlignmentError(f"invalid direct I/O alignment: {value}")

    @property
    def step(self) -> int:
        return math.lcm(self.memory, self.offset)


def _statx_alignment(fd: int) -> DioAlignment | None:
    statx = getattr(_LIBC, "statx", None)
    if statx is None:
        return None
    statx.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_uint,
        ctypes.c_void_p,
    ]
    statx.restype = ctypes.c_int
    # Linux statx ABI: 256 bytes, with the two DIO fields at 0x98.
    result = ctypes.create_string_buffer(256)
    if statx(fd, b"", _AT_EMPTY_PATH, _STATX_DIOALIGN, result) < 0:
        error = ctypes.get_errno()
        if error in (errno.ENOSYS, errno.EINVAL, errno.EOPNOTSUPP):
            return None
        raise OSError(error, os.strerror(error))
    if not struct.unpack_from("=I", result)[0] & _STATX_DIOALIGN:
        return None
    return DioAlignment(*struct.unpack_from("=II", result, 0x98))


def _mount_info(fd: int) -> tuple[str, set[str]] | None:
    """Identify the fd's actual mount, including container bind mounts."""
    with open(f"/proc/self/fdinfo/{fd}") as info:
        mount_id = next(
            (line.split()[1] for line in info if line.startswith("mnt_id:")), None
        )
    if mount_id is None:
        return None
    with open("/proc/self/mountinfo") as mounts:
        for line in mounts:
            mount, separator, filesystem = line.partition(" - ")
            if separator and mount.split()[0] == mount_id:
                kind, _source, options = filesystem.split()
                return kind, set(options.split(","))
    return None


def _ext4_alignment(fd: int) -> DioAlignment | None:
    # Before STATX_DIOALIGN (Linux 6.1), ordinary ext4 files accept DIO
    # aligned to the filesystem block size, which is a multiple of the
    # underlying device's logical block size. Query it; do not assume 4 KiB.
    mount = _mount_info(fd)
    if mount is None or mount[0] != "ext4":
        return None
    flags = struct.unpack_from(
        "=I", fcntl.ioctl(fd, _FS_IOC_GETFLAGS, bytes(ctypes.sizeof(ctypes.c_long)))
    )[0]
    # ext4_dio_supported() on 5.15 sends these files through buffered I/O.
    if "data=journal" in mount[1] or flags & _EXT4_BUFFERED_FLAGS:
        raise SsdAlignmentError("this ext4 file does not support direct I/O")
    block = os.fstatvfs(fd).f_bsize
    return DioAlignment(block, block)


def direct_alignment(fd: int) -> DioAlignment:
    alignment = _statx_alignment(fd)
    if alignment is not None:
        return alignment
    # Older XFS kernels report the inode's limits through DIOINFO.
    try:
        result = fcntl.ioctl(fd, _XFS_IOC_DIOINFO, bytes(12))
    except OSError as e:
        if e.errno in (errno.ENOTTY, errno.EINVAL, errno.EOPNOTSUPP):
            alignment = _ext4_alignment(fd)
            if alignment is not None:
                return alignment
        raise SsdAlignmentError(
            "direct I/O alignment is unknown on this filesystem"
        ) from e
    memory, offset, maximum = struct.unpack("=III", result)
    if maximum < offset:
        raise SsdAlignmentError("invalid XFS direct I/O limits")
    return DioAlignment(memory, offset)


def round_up(size: int, alignment: int) -> int:
    return (size + alignment - 1) // alignment * alignment


def buffer_address(view: memoryview) -> int:
    return ctypes.addressof(ctypes.c_char.from_buffer(view))


@contextmanager
def aligned_buffer(size: int, alignment: int):
    """The owner must retain this context until its synchronous I/O ends."""
    if size <= 0 or alignment <= 0:
        raise ValueError("buffer size and alignment must be positive")
    with mmap.mmap(-1, size + alignment - 1) as allocation:
        with memoryview(allocation) as raw:
            start = -buffer_address(raw) % alignment
            with raw[start : start + size] as view:
                yield view
