# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Real local direct I/O, with syscall faults at the driver boundary."""

import ctypes
import errno
import fcntl
import os
import struct
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from maru_common.storage_types import PoolKind
from maru_storage_service.config import TierConfig
from maru_storage_service.drivers import PoolAccess
from maru_storage_service.drivers import ssd as driver
from maru_storage_service.drivers import ssd_alignment as dio
from maru_storage_service.drivers.ssd import SsdDriver, SsdPoolError, create_ssd_pool
from maru_storage_service.drivers.ssd_alignment import (
    DioAlignment,
    SsdAlignmentError,
    aligned_buffer,
    buffer_address,
    direct_alignment,
)

PAGE = 16 << 10


@pytest.fixture
def directory(ssd_dir):
    return Path(ssd_dir)


@pytest.fixture
def pool(directory):
    value = create_ssd_pool("node", str(directory), 4 * PAGE, PAGE)
    try:
        yield value
    finally:
        value.close()


def test_pool_contract_and_preallocation(pool, directory):
    info = pool.info()
    assert isinstance(pool, driver.SsdPool)
    assert pool.access == PoolAccess.IO_ONLY
    assert pool.worker_fd() is None
    with pytest.raises(SsdPoolError, match="descriptor"):
        pool.descriptor()
    assert info.kind == PoolKind.SSD
    assert info.path == str(directory.resolve() / "pool.data")
    assert info.pool_id == f"ssd:node:{info.path}"
    assert str(uuid.UUID(info.device_id)) == info.device_id
    assert info.pool_generation == 1
    assert list(directory.iterdir()) == [Path(pool.path)]
    details = os.fstat(pool.fd)
    assert details.st_size == pool.header_size + pool.capacity
    assert details.st_blocks * 512 >= details.st_size
    assert fcntl.fcntl(pool.fd, fcntl.F_GETFL) & os.O_DIRECT
    assert fcntl.fcntl(pool.fd, fcntl.F_GETFD) & fcntl.FD_CLOEXEC
    assert pool.header_size % pool.alignment.step == 0


@pytest.mark.parametrize("n", [1, 513, PAGE - 1, PAGE])
def test_roundtrip_and_zero_padding(pool, n):
    with (
        aligned_buffer(PAGE, pool.alignment.memory) as src,
        aligned_buffer(PAGE, pool.alignment.memory) as dst,
    ):
        src[:] = b"z" * PAGE
        src[:n] = bytes(i % 251 for i in range(n))
        expected = bytes(src[:n])
        assert pool.write(PAGE, src, n) == n
        dst[:] = b"x" * PAGE
        assert pool.read(PAGE, n, dst) == n
        size = pool.io_size(n)
        assert bytes(dst[:n]) == expected
        assert bytes(dst[n:size]) == bytes(size - n)
        assert bytes(dst[size:]) == b"x" * (PAGE - size)
        assert bytes(src[n:size]) == bytes(size - n)
        with open(pool.path, "rb") as file:
            file.seek(pool.header_size)
            assert file.read(PAGE) == bytes(PAGE)
            assert file.read(size) == expected + bytes(size - n)
            file.seek(pool.header_size + 2 * PAGE)
            assert file.read(PAGE) == bytes(PAGE)


def test_same_boot_reopen_keeps_identity_and_bytes(pool, directory, monkeypatch):
    with aligned_buffer(PAGE, pool.alignment.memory) as view:
        view[:] = b"r" * PAGE
        pool.write(3 * PAGE, view, PAGE)
    info = pool.info()
    pool.close()
    header = pool.header_size
    before = Path(info.path).read_bytes()
    inode = os.stat(info.path).st_ino
    pwritev = os.pwritev

    def forbidden(*args):
        pytest.fail("reopening must not allocate or truncate")

    def header_only(fd, buffers, offset):
        assert offset == 0 and len(buffers[0]) == header, "only the header"
        return pwritev(fd, buffers, offset)

    monkeypatch.setattr(os, "posix_fallocate", forbidden)
    monkeypatch.setattr(os, "ftruncate", forbidden)
    monkeypatch.setattr(os, "pwritev", header_only)
    tier = TierConfig(store="ssd", path=str(directory), size=4 * PAGE)
    # each create counts an open: the same identity, the next generation
    for generation in (2, 3):
        reopened = SsdDriver().create(
            tier, node_id="node", node_epoch="new", page_size=PAGE
        )
        assert reopened.info() == replace(info, pool_generation=generation)
        reopened.close()
    counted = replace(info, pool_generation=3)
    reopened = SsdDriver().open(counted)  # not counted
    try:
        assert reopened.info() == counted
        with aligned_buffer(PAGE, reopened.alignment.memory) as view:
            reopened.read(3 * PAGE, PAGE, view)
            assert bytes(view) == b"r" * PAGE
    finally:
        reopened.close()
    assert os.stat(info.path).st_ino == inode
    after = Path(info.path).read_bytes()
    assert after[header:] == before[header:]
    assert driver._HEADER.unpack_from(after)[8] == 3


def test_preallocation_is_new_file_only(directory, monkeypatch):
    allocate = os.posix_fallocate
    calls = []

    def record(fd, offset, size):
        calls.append((offset, size))
        return allocate(fd, offset, size)

    monkeypatch.setattr(os, "posix_fallocate", record)
    pool = create_ssd_pool("node", str(directory), 4 * PAGE + 7, PAGE)
    assert pool.capacity == 4 * PAGE
    assert calls == [(0, pool.header_size + 4 * PAGE)]
    pool.close()
    pool = create_ssd_pool("node", str(directory), 4 * PAGE + 7, PAGE)
    pool.close()
    assert len(calls) == 1


@pytest.mark.parametrize("stale", ["empty", "short", "garbage"])
def test_an_interrupted_init_does_not_block_the_next_start(directory, stale):
    """A start killed while building the file leaves only pool.data.init."""
    init = directory / ("pool.data" + driver._INIT_SUFFIX)
    data = {"empty": b"", "short": b"x" * 100, "garbage": os.urandom(8 * PAGE)}
    init.write_bytes(data[stale])
    pool = create_ssd_pool("node", str(directory), 4 * PAGE, PAGE)
    try:
        assert pool.pool_generation == 1
        assert sorted(p.name for p in directory.iterdir()) == ["pool.data"]
        assert os.fstat(pool.fd).st_size == pool.header_size + 4 * PAGE
    finally:
        pool.close()


def test_a_file_being_built_by_another_start_is_not_taken(directory):
    init = directory / ("pool.data" + driver._INIT_SUFFIX)
    fd = os.open(init, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(SsdPoolError) as error:
            create_ssd_pool("node", str(directory), 4 * PAGE, PAGE)
        assert isinstance(error.value.__cause__, BlockingIOError)
        assert not (directory / "pool.data").exists()
    finally:
        os.close(fd)


def test_a_file_linked_meanwhile_is_never_replaced(directory, monkeypatch):
    link = os.link

    def raced(src, dst):
        Path(dst).write_bytes(b"another start's file")  # linked first
        return link(src, dst)

    monkeypatch.setattr(os, "link", raced)
    with pytest.raises(SsdPoolError, match="exists"):
        create_ssd_pool("node", str(directory), 4 * PAGE, PAGE)
    assert (directory / "pool.data").read_bytes() == b"another start's file"


@pytest.mark.parametrize("alias", [False, True])
def test_data_inode_lock_rejects_other_node_and_hardlink(pool, directory, alias):
    target = directory
    if alias:
        target = directory / "alias"
        target.mkdir()
        os.link(pool.path, target / "pool.data")
    with pytest.raises(SsdPoolError) as error:
        create_ssd_pool("other-node", str(target), pool.capacity, PAGE)
    assert isinstance(error.value.__cause__, BlockingIOError)
    assert error.value.__cause__.errno in (errno.EWOULDBLOCK, errno.EAGAIN)


def test_lock_is_visible_to_other_process(pool):
    script = """
import fcntl, os, sys
fd = os.open(sys.argv[1], os.O_RDWR | os.O_CLOEXEC)
try:
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    sys.exit(0)
sys.exit(1)
"""
    result = subprocess.run([sys.executable, "-c", script, pool.path], timeout=5)
    assert result.returncode == 0


@pytest.mark.parametrize("field", range(9))
def test_header_mismatch_is_preserved(pool, directory, field):
    path = Path(pool.path)
    pool.close()
    data = bytearray(path.read_bytes())
    values = list(driver._HEADER.unpack_from(data))
    # field 8 counts the opens: any count from 1 is valid, 0 is not
    values[field] = (
        b"badmagic"
        if field == 0
        else bytes(16)
        if field == 2
        else 0
        if field == 8
        else values[field] + 1
    )
    driver._HEADER.pack_into(data, 0, *values)
    path.write_bytes(data)
    with pytest.raises(SsdPoolError, match="header"):
        create_ssd_pool("node", str(directory), 4 * PAGE, PAGE)
    assert path.read_bytes() == data


def test_another_node_cannot_open_the_file(pool, directory):
    info = pool.info()
    pool.close()
    path = Path(info.path)
    before = path.read_bytes()
    with pytest.raises(SsdPoolError, match="belongs to node 'node', not 'other'"):
        create_ssd_pool("other", str(directory), 4 * PAGE, PAGE)
    with pytest.raises(SsdPoolError, match="belongs to node"):
        SsdDriver().open(replace(info, node_id="other"))
    assert path.read_bytes() == before
    with pytest.raises(SsdPoolError, match="node_id"):
        create_ssd_pool("n" * 257, str(directory / "long"), 4 * PAGE, PAGE)
    # handed over by removing it: a new file, a new UUID
    path.unlink()
    taken = create_ssd_pool("other", str(directory), 4 * PAGE, PAGE)
    assert taken.device_id != info.device_id
    taken.close()


@pytest.mark.parametrize("delta", [-512, 512])
def test_size_mismatch_is_preserved(pool, directory, delta):
    path = Path(pool.path)
    pool.close()
    os.truncate(path, path.stat().st_size + delta)
    before = path.read_bytes()
    with pytest.raises(SsdPoolError, match="file size"):
        create_ssd_pool("node", str(directory), 4 * PAGE, PAGE)
    assert path.read_bytes() == before


def test_changed_configuration_and_replaced_uuid_are_rejected(pool, directory):
    info = pool.info()
    pool.close()
    with pytest.raises(SsdPoolError, match="file size"):
        create_ssd_pool("node", str(directory), 5 * PAGE, PAGE)
    with pytest.raises(SsdPoolError, match="header"):
        create_ssd_pool("node", str(directory), 4 * PAGE, PAGE // 2)
    for changed in (
        replace(info, device_id=str(uuid.uuid4())),
        replace(info, pool_generation=2),
        replace(info, pool_id="wrong"),
    ):
        with pytest.raises(SsdPoolError, match="identity"):
            SsdDriver().open(changed)
    reopened = SsdDriver().open(info)
    reopened.close()
    Path(info.path).unlink()
    replacement = create_ssd_pool("node", str(directory), 4 * PAGE, PAGE)
    assert replacement.device_id != info.device_id
    replacement.close()
    with pytest.raises(SsdPoolError, match="identity"):
        SsdDriver().open(info)


@pytest.mark.parametrize(
    "page_offset,n", [(-PAGE, 1), (1, 1), (4 * PAGE, 1), (0, 0), (0, PAGE + 1)]
)
@pytest.mark.parametrize("write", [False, True])
def test_extent_bounds_fail_before_syscall(pool, monkeypatch, page_offset, n, write):
    def forbidden(*args):
        pytest.fail("invalid extent reached the syscall")

    monkeypatch.setattr(os, "pwritev" if write else "preadv", forbidden)
    with aligned_buffer(PAGE, pool.alignment.memory) as view:
        with pytest.raises(ValueError):
            if write:
                pool.write(page_offset, view, n)
            else:
                pool.read(page_offset, n, view)


def test_bad_buffers_and_closed_pool(pool):
    with aligned_buffer(PAGE + pool.alignment.memory, pool.alignment.memory) as view:
        for bad in (view[1:], view[:1], view[::2], bytes(PAGE)):
            try:
                with pytest.raises(ValueError):
                    pool.write(0, bad, PAGE)
                with pytest.raises(ValueError):
                    pool.read(0, PAGE, bad)
            finally:
                if isinstance(bad, memoryview):
                    bad.release()
        pool.close()
        pool.close()
        with pytest.raises(SsdPoolError, match="closed"):
            pool.read(0, PAGE, view)


@pytest.mark.parametrize("write", [False, True])
@pytest.mark.parametrize("fault", ["short", "unaligned", "eintr"])
def test_short_io_and_eintr_keep_alignment(pool, monkeypatch, write, fault):
    name = "pwritev" if write else "preadv"
    syscall = getattr(os, name)
    calls = []
    start = pool.header_size + PAGE
    short = pool.alignment.step + (1 if fault == "unaligned" else 0)

    def injected(fd, buffers, offset):
        view = buffers[0]
        calls.append((offset, len(view), buffer_address(view)))
        assert offset >= start and offset + len(view) <= start + PAGE
        assert offset % pool.alignment.offset == len(view) % pool.alignment.offset == 0
        assert buffer_address(view) % pool.alignment.memory == 0
        if len(calls) == 1:
            if fault == "eintr":
                raise InterruptedError(errno.EINTR, "injected")
            if fault == "short":
                with view[:short] as partial:
                    return syscall(fd, [partial], offset)
            syscall(fd, buffers, offset)
            return short
        return syscall(fd, buffers, offset)

    with aligned_buffer(PAGE, pool.alignment.memory) as view:
        view[:] = b"s" * PAGE
        pool.write(PAGE, view, PAGE)
        monkeypatch.setattr(os, name, injected)
        if write:
            assert pool.write(PAGE, view, PAGE) == PAGE
        else:
            view[:] = bytes(PAGE)
            assert pool.read(PAGE, PAGE, view) == PAGE
            assert bytes(view) == b"s" * PAGE
    assert len(calls) == 2
    assert calls[1][0] == start + (0 if fault == "eintr" else pool.alignment.step)


def test_short_io_rewinds_for_buffer_alignment(pool, monkeypatch):
    pool.alignment = DioAlignment(4096, 512)
    syscall = os.pwritev
    addresses = []

    def injected(fd, buffers, offset):
        addresses.append((offset, buffer_address(buffers[0])))
        if len(addresses) == 1:
            with buffers[0][:512] as partial:
                return syscall(fd, [partial], offset)
        return syscall(fd, buffers, offset)

    monkeypatch.setattr(os, "pwritev", injected)
    with aligned_buffer(PAGE, 4096) as view:
        pool.write(0, view, PAGE)
    assert len(addresses) == 2 and addresses[0] == addresses[1]


@pytest.mark.parametrize("write", [False, True])
@pytest.mark.parametrize(
    "fault", [0, errno.EINVAL, errno.ENOSPC, errno.EDQUOT, errno.EIO]
)
def test_io_failure_propagates(pool, monkeypatch, write, fault):
    calls = []

    def injected(*args):
        calls.append(True)
        if fault:
            raise OSError(fault, "injected")
        return 0

    monkeypatch.setattr(os, "pwritev" if write else "preadv", injected)
    with aligned_buffer(PAGE, pool.alignment.memory) as view:
        with pytest.raises(OSError) as error:
            if write:
                pool.write(0, view, PAGE)
            else:
                pool.read(0, PAGE, view)
    assert error.value.errno == (fault or errno.EIO)
    assert len(calls) == 1


@pytest.mark.parametrize("advancing", [False, True])
def test_repeated_unaligned_short_io_is_bounded(pool, monkeypatch, advancing):
    calls = []

    def injected(*args):
        calls.append(True)
        return len(calls) if advancing else 1

    monkeypatch.setattr(os, "pwritev", injected)
    with aligned_buffer(PAGE, pool.alignment.memory) as view:
        with pytest.raises(OSError) as error:
            pool.write(0, view, PAGE)
    assert error.value.errno == errno.EIO
    assert len(calls) <= driver._MAX_REWINDS + 1


def test_partial_write_then_failure_is_not_success(pool, monkeypatch):
    syscall = os.pwritev
    calls = []

    def injected(fd, buffers, offset):
        calls.append(True)
        if len(calls) == 1:
            with buffers[0][: pool.alignment.step] as partial:
                return syscall(fd, [partial], offset)
        raise OSError(errno.ENOSPC, "injected")

    monkeypatch.setattr(os, "pwritev", injected)
    with aligned_buffer(PAGE, pool.alignment.memory) as view:
        view[:] = b"f" * PAGE
        with pytest.raises(OSError, match="injected"):
            pool.write(0, view, PAGE)
    with open(pool.path, "rb") as file:
        file.seek(pool.header_size)
        assert file.read(pool.alignment.step) == b"f" * pool.alignment.step
        assert file.read(PAGE) == bytes(PAGE)


def test_unexpected_real_eof_fails(pool):
    os.truncate(pool.path, pool.header_size + PAGE)
    with aligned_buffer(PAGE, pool.alignment.memory) as view:
        with pytest.raises(OSError) as error:
            pool.read(PAGE, PAGE, view)
    assert error.value.errno == errno.EIO


@pytest.mark.parametrize("deadline", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_deadline_is_refused(pool, deadline):
    with aligned_buffer(PAGE, pool.alignment.memory) as view:
        with pytest.raises(ValueError, match="finite"):
            pool.write(0, view, PAGE, deadline=deadline)


@pytest.mark.parametrize("fault", ["expired", "eintr", "short", "late"])
def test_deadline_prevents_success_and_unbounded_eintr(pool, monkeypatch, fault):
    now = [11.0 if fault == "expired" else 1.0]
    calls = []
    syscall = os.pwritev

    def injected(fd, buffers, offset):
        calls.append(True)
        now[0] = 11.0
        if fault == "eintr":
            raise InterruptedError(errno.EINTR, "injected")
        if fault == "short":
            with buffers[0][: pool.alignment.step] as partial:
                return syscall(fd, [partial], offset)
        return syscall(fd, buffers, offset)

    monkeypatch.setattr(driver, "time", SimpleNamespace(monotonic=lambda: now[0]))
    monkeypatch.setattr(os, "pwritev", injected)
    with aligned_buffer(PAGE, pool.alignment.memory) as view:
        if fault == "late":  # every byte moved: done, though late
            assert pool.write(0, view, PAGE, deadline=10.0) == PAGE
        else:
            with pytest.raises(TimeoutError):
                pool.write(0, view, PAGE, deadline=10.0)
    assert len(calls) == (0 if fault == "expired" else 1)


def test_close_keeps_lock_and_fd_until_blocking_io_ends(pool, directory, monkeypatch):
    entered, release, closed = threading.Event(), threading.Event(), threading.Event()
    failures = []
    syscall = os.preadv
    fd = pool.fd
    info = pool.info()

    def blocked(*args):
        entered.set()
        assert release.wait(5)
        return syscall(*args)

    def read():
        try:
            with aligned_buffer(PAGE, pool.alignment.memory) as view:
                pool.read(0, PAGE, view, deadline=time.monotonic() + 5)
        except BaseException as e:
            failures.append(e)

    def close():
        pool.close()
        closed.set()

    monkeypatch.setattr(os, "preadv", blocked)
    reader = threading.Thread(target=read)
    closer = threading.Thread(target=close)
    reader.start()
    try:
        assert entered.wait(2)
        closer.start()
        assert not closed.wait(0.05)
        assert os.fstat(fd).st_size == pool.header_size + pool.capacity
        # the owner itself is refused while the lock is held
        with pytest.raises(SsdPoolError) as error:
            create_ssd_pool("node", str(directory), pool.capacity, PAGE)
        assert isinstance(error.value.__cause__, BlockingIOError)
    finally:
        release.set()
        reader.join(2)
        if closer.ident is not None:
            closer.join(2)
    assert not reader.is_alive() and not closer.is_alive()
    assert not failures and closed.is_set() and pool.fd == -1
    reopened = SsdDriver().open(info)
    reopened.close()


@pytest.mark.parametrize("memory,offset", [(512, 512), (4096, 512), (4096, 4096)])
def test_statx_alignment_uses_reported_fields(monkeypatch, memory, offset):
    def statx(fd, path, flags, mask, result):
        assert (fd, path, flags, mask) == (
            42,
            b"",
            dio._AT_EMPTY_PATH,
            dio._STATX_DIOALIGN,
        )
        struct.pack_into("=I", result, 0, mask)
        struct.pack_into("=II", result, 0x98, memory, offset)
        return 0

    monkeypatch.setattr(dio, "_LIBC", SimpleNamespace(statx=statx))
    assert direct_alignment(42) == DioAlignment(memory, offset)


@pytest.mark.parametrize("error", [0, errno.ENOSYS, errno.EINVAL, errno.EOPNOTSUPP])
def test_statx_unavailable_uses_xfs_inode_limits(monkeypatch, error):
    def statx(*args):
        ctypes.set_errno(error)
        return -1 if error else 0

    def ioctl(fd, command, buffer):
        assert (fd, command, len(buffer)) == (42, dio._XFS_IOC_DIOINFO, 12)
        return struct.pack("=III", 4096, 512, 1 << 30)

    monkeypatch.setattr(dio, "_LIBC", SimpleNamespace(statx=statx))
    monkeypatch.setattr(dio.fcntl, "ioctl", ioctl)
    assert direct_alignment(42) == DioAlignment(4096, 512)


def test_unknown_alignment_refuses_before_preallocation(directory, monkeypatch):
    monkeypatch.setattr(dio, "_statx_alignment", lambda fd: None)

    def unsupported(*args):
        raise OSError(errno.ENOTTY, "no filesystem alignment query")

    def forbidden(*args):
        pytest.fail("unknown alignment must fail before preallocation")

    monkeypatch.setattr(dio.fcntl, "ioctl", unsupported)
    monkeypatch.setattr(os, "posix_fallocate", forbidden)
    with pytest.raises(SsdPoolError, match="alignment is unknown"):
        create_ssd_pool("node", str(directory), PAGE, PAGE)


def test_statx_explicit_unsupported_is_not_overridden(monkeypatch):
    def statx(fd, path, flags, mask, result):
        struct.pack_into("=I", result, 0, mask)
        return 0

    def forbidden(*args):
        pytest.fail("explicit zero alignment must not use a fallback")

    monkeypatch.setattr(dio, "_LIBC", SimpleNamespace(statx=statx))
    monkeypatch.setattr(dio.fcntl, "ioctl", forbidden)
    with pytest.raises(SsdAlignmentError):
        direct_alignment(42)


def test_bad_config_and_nonregular_file_are_refused(directory):
    for size, page in ((PAGE, 0), (0, PAGE), (PAGE, 513)):
        with pytest.raises(SsdPoolError):
            create_ssd_pool("node", str(directory), size, page)
    with pytest.raises(SsdPoolError):
        create_ssd_pool("node", "relative", PAGE, PAGE)
    with pytest.raises(SsdPoolError):
        SsdDriver().create(
            TierConfig("ssd"), node_id="node", node_epoch="e", page_size=PAGE
        )
    # a refused config leaves no pool.data behind
    path = directory / "pool.data"
    assert not path.exists()
    path.mkdir()
    with pytest.raises(SsdPoolError, match="regular file"):
        create_ssd_pool("node", str(directory), PAGE, PAGE)
