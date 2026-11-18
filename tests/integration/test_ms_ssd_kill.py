# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Process death during real O_DIRECT I/O on a verified local filesystem.

A returned write means that every requested byte transferred; those bytes can
be read after a process restart. It does not promise power-loss durability or
page atomicity. The driver has no persistent page-commit records, checksums, or
torn-page recovery: failed/interrupted writes can leave partial raw bytes, and
the caller must not publish those pages as committed. The short-write case
below checks this boundary without claiming to test the service's COMMIT path.

The slow case samples a real sleeping pwritev, without replacing/delaying that
syscall. Sampling and SIGKILL cannot be atomic on a stock, unprivileged kernel;
large pages widen the window. Skip that case if the window cannot be observed.
"""

import errno
import fcntl
import json
import os
import platform
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

import pytest

from maru_storage_service.drivers import ssd as driver
from maru_storage_service.drivers.ssd_alignment import aligned_buffer

NODE = "ssd-kill-test-node"
WAIT = 30


@pytest.fixture
def directory(ssd_root):
    with tempfile.TemporaryDirectory(prefix="maru-ssd-kill-", dir=ssd_root) as path:
        yield Path(path)


def _pattern(index, size):
    block = bytes((i + 37 * index) % 251 + 1 for i in range(4096))
    assert size % len(block) == 0
    return block * (size // len(block))


def _send(channel, value):
    channel.sendall(json.dumps(value).encode() + b"\n")


def _write_forever(path, page, channel_fd):
    """Only this subprocess owns the original pool fd and its flock."""
    with socket.socket(fileno=channel_fd) as channel:
        pool = driver.create_ssd_pool(NODE, path, 3 * page, page)
        try:
            assert fcntl.fcntl(pool.fd, fcntl.F_GETFL) & os.O_DIRECT
            with aligned_buffer(page, pool.alignment.memory) as view:
                for index in (0, 1):
                    expected = _pattern(index, page)
                    view[:] = expected
                    assert (
                        pool.write(
                            index * page, view, page, deadline=time.monotonic() + WAIT
                        )
                        == page
                    )
                    view[:] = bytes(page)
                    assert (
                        pool.read(
                            index * page, page, view, deadline=time.monotonic() + WAIT
                        )
                        == page
                    )
                    assert bytes(view) == expected
                del expected
                _send(
                    channel,
                    {
                        "fd": pool.fd,
                        "header_size": pool.header_size,
                        "device_id": pool.device_id,
                        "generation": pool.pool_generation,
                        "inode": os.fstat(pool.fd).st_ino,
                        "confirmed": [0, 1],
                    },
                )
                assert channel.recv(1) == b"G"
                view[:] = _pattern(2, page)
                assert (
                    pool.write(2 * page, view, page, deadline=time.monotonic() + WAIT)
                    == page
                )
                _send(channel, {"completed": 2, "returned": page})
                # Never overwrite the two read-confirmed pages. Repeating the
                # same bytes also preserves the acknowledged third page.
                while True:
                    assert (
                        pool.write(
                            2 * page, view, page, deadline=time.monotonic() + WAIT
                        )
                        == page
                    )
        finally:
            pool.close()


@contextmanager
def _writer(path, page):
    parent, child = socket.socketpair()
    try:
        env = os.environ.copy()
        root = str(Path(__file__).resolve().parents[2])
        env["PYTHONPATH"] = os.pathsep.join(filter(None, (root, env.get("PYTHONPATH"))))
        with subprocess.Popen(
            [
                sys.executable,
                "-B",
                str(Path(__file__).resolve()),
                "--writer",
                str(path),
                str(page),
                str(child.fileno()),
            ],
            pass_fds=(child.fileno(),),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        ) as proc:
            child.close()
            parent.settimeout(WAIT)
            with parent.makefile("rb") as reader:
                try:
                    yield proc, parent, reader
                finally:
                    # Kill this child's positive PID only, including on failure.
                    if proc.poll() is None:
                        os.kill(proc.pid, signal.SIGKILL)
                    proc.wait(timeout=WAIT)
    finally:
        child.close()
        parent.close()


def _receive(proc, reader):
    line = reader.readline()
    if not line:
        proc.wait(timeout=WAIT)
        pytest.fail(f"writer exited {proc.returncode}: {proc.stderr.read().decode()}")
    return json.loads(line)


def _assert_lock_error(error):
    assert isinstance(error.__cause__, BlockingIOError), repr(error)
    assert error.__cause__.errno in (errno.EAGAIN, errno.EWOULDBLOCK)


def _syscall(pid):
    try:
        return Path(f"/proc/{pid}/syscall").read_text().split()
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        # /proc can lose access to an exiting child's mm before it is reaped.
        return []


def _is_write(fields, numbers, fd, offset):
    return (
        len(fields) >= 5
        and fields[0] in {str(number) for number in numbers}
        and int(fields[1], 0) == fd
        and int(fields[3], 0) == 1
        and int(fields[4], 0) == offset
    )


def _observe_blocked_write(proc, fd, offset):
    # os.pwritev may use pwritev2 even when no RWF_* flags were requested.
    numbers = {"x86_64": (296, 328), "aarch64": (70, 287)}.get(platform.machine())
    if numbers is None:
        pytest.skip("pwritev/pwritev2 syscall numbers are unknown on this architecture")
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        fields = _syscall(proc.pid)
        state = Path(f"/proc/{proc.pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
        if state == "D" and _is_write(fields, numbers, fd, offset):
            time.sleep(0.001)
            if _is_write(_syscall(proc.pid), numbers, fd, offset):
                return numbers, Path(f"/proc/{proc.pid}/wchan").read_text().strip()
        time.sleep(0.0005)
    pytest.skip("no sleeping 128 MiB pwritev observed; write/kill timing is not atomic")


def _assert_header(pool, identity, generation):
    with aligned_buffer(pool.header_size, pool.alignment.memory) as header:
        assert os.preadv(pool.fd, [header], 0) == pool.header_size
        assert driver._HEADER.unpack_from(header) == (
            driver._MAGIC,
            driver._VERSION,
            uuid.UUID(identity).bytes,
            pool.page_size,
            pool.capacity,
            pool.alignment.memory,
            pool.alignment.offset,
            pool.header_size,
            generation,
            NODE.encode().ljust(driver._OWNER_BYTES, b"\0"),
        )


@pytest.mark.integration
@pytest.mark.parametrize("blocked", [False, True], ids=["write-loop", "sleeping-write"])
def test_sigkill_releases_lock_after_direct_io(directory, blocked, record_property):
    page = (128 if blocked else 64) << 20
    capacity = 3 * page
    with _writer(directory, page) as (proc, channel, reader):
        ready = _receive(proc, reader)
        assert ready["generation"] == 1 and ready["confirmed"] == [0, 1]
        with pytest.raises(driver.SsdPoolError) as held:
            driver.create_ssd_pool(NODE, str(directory), capacity, page)
        _assert_lock_error(held.value)
        channel.sendall(b"G")
        assert _receive(proc, reader) == {"completed": 2, "returned": page}
        syscall_numbers, wchan = (), "not sampled"
        if blocked:
            syscall_numbers, wchan = _observe_blocked_write(
                proc, ready["fd"], ready["header_size"] + 2 * page
            )

        os.kill(proc.pid, signal.SIGKILL)
        # Do not wait()/reap before polling: SIGKILL can be pending while the
        # synchronous direct I/O still owns the fd and its exclusive lock.
        deadline = time.monotonic() + WAIT
        rejected = 0
        inflight_rejections = 0
        reopened = None
        while time.monotonic() < deadline:
            try:
                reopened = driver.create_ssd_pool(NODE, str(directory), capacity, page)
            except driver.SsdPoolError as error:
                _assert_lock_error(error)
                rejected += 1
                if blocked and _is_write(
                    _syscall(proc.pid),
                    syscall_numbers,
                    ready["fd"],
                    ready["header_size"] + 2 * page,
                ):
                    inflight_rejections += 1
                time.sleep(0.0005)
            else:
                break
        assert reopened is not None, "pool lock was not released after SIGKILL"
        try:
            if blocked:
                assert inflight_rejections > 0, (
                    "missed the post-SIGKILL I/O/lock window"
                )
                assert not _is_write(
                    _syscall(proc.pid),
                    syscall_numbers,
                    ready["fd"],
                    ready["header_size"] + 2 * page,
                ), "the old owner is still executing its data write"
            # flock follows the open file description, not /proc disappearance:
            # a dead, unreaped zombie may remain after the fd has been closed.
            assert proc.wait(timeout=WAIT) == -signal.SIGKILL
            assert reopened.pool_generation == 2  # failed opens did not count
            assert reopened.device_id == ready["device_id"]
            assert os.fstat(reopened.fd).st_ino == ready["inode"]
            assert fcntl.fcntl(reopened.fd, fcntl.F_GETFL) & os.O_DIRECT
            _assert_header(reopened, ready["device_id"], 2)
            with aligned_buffer(page, reopened.alignment.memory) as view:
                for index in (0, 1, 2):
                    assert (
                        reopened.read(
                            index * page, page, view, deadline=time.monotonic() + WAIT
                        )
                        == page
                    )
                    assert bytes(view) == _pattern(index, page)
            record_property("post_sigkill_lock_rejections", rejected)
            record_property("post_sigkill_inflight_rejections", inflight_rejections)
            record_property("blocked_write_wchan", wchan)
            print(
                f"{page >> 20} MiB: wchan={wchan}, post-SIGKILL lock refusals={rejected}, "
                f"still in write syscall={inflight_rejections}"
            )
        finally:
            reopened.close()


@pytest.mark.integration
def test_partial_direct_write_is_not_a_success(directory, monkeypatch):
    """Real short O_DIRECT write + I/O error: no successful page write.

    Reopening intentionally exposes the partial raw bytes. The driver cannot
    infer committed pages from these bytes or from its pool open counter.
    """
    page = 16 << 20
    half = page // 2
    pool = driver.create_ssd_pool(NODE, str(directory), page, page)
    identity = pool.device_id
    pwritev = os.pwritev
    calls = []

    def short_then_error(fd, buffers, offset):
        calls.append((offset, len(buffers[0])))
        if len(calls) == 1:
            with buffers[0][:half] as prefix:
                count = pwritev(fd, [prefix], offset)
            assert count == half
            return count
        raise OSError(errno.EIO, "injected failure after real direct-I/O prefix")

    try:
        with aligned_buffer(page, pool.alignment.memory) as view:
            view[:] = _pattern(3, page)
            with monkeypatch.context() as patch:
                patch.setattr(os, "pwritev", short_then_error)
                with pytest.raises(
                    OSError, match="after real direct-I/O prefix"
                ) as error:
                    pool.write(0, view, page, deadline=time.monotonic() + WAIT)
            assert error.value.errno == errno.EIO
        assert calls == [(pool.header_size, page), (pool.header_size + half, half)]
    finally:
        pool.close()
    reopened = driver.create_ssd_pool(NODE, str(directory), page, page)
    try:
        _assert_header(reopened, identity, 2)
        with aligned_buffer(page, reopened.alignment.memory) as view:
            assert (
                reopened.read(0, page, view, deadline=time.monotonic() + WAIT) == page
            )
            assert bytes(view[:half]) == _pattern(3, half)
            assert bytes(view[half:]) == bytes(half)
    finally:
        reopened.close()


if __name__ == "__main__":
    assert sys.argv[1] == "--writer"
    _write_forever(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
