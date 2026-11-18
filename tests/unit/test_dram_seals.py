# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Shared DRAM stays size-sealed even when Python omits Linux constants."""

import errno
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from maru_storage_service.drivers import dram


def test_missing_python_constants_still_enforce_kernel_seals():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent("""\
            import fcntl
            import mmap
            import os

            for name in tuple(vars(fcntl)):
                if name in ('F_ADD_SEALS', 'F_GET_SEALS') or name.startswith('F_SEAL_'):
                    delattr(fcntl, name)

            from maru_storage_service.drivers.dram import create_dram_pool

            pool = create_dram_pool('n', 'e', 8192, 4096, False)
            try:
                for size in (4096, 16384):
                    try:
                        os.ftruncate(pool.fd, size)
                    except PermissionError:
                        pass
                    else:
                        raise AssertionError('shared pool could be resized')
                # Further seals, including one preventing writes, are forbidden.
                try:
                    fcntl.fcntl(pool.fd, 1033, 0x0008)
                except PermissionError:
                    pass
                else:
                    raise AssertionError('shared pool seals could be changed')
                with mmap.mmap(pool.fd, 8192) as a, mmap.mmap(pool.fd, 8192) as b:
                    a[:3] = b'kv!'
                    assert b[:3] == b'kv!'
            finally:
                pool.close()
            """),
        ],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_a_kernel_refusing_seals_fails_and_closes_the_pool(monkeypatch):
    seen = []

    def refuse(fd, *args):
        seen.append(fd)
        raise OSError(errno.EINVAL, "sealing unavailable")

    monkeypatch.setattr(dram.fcntl, "fcntl", refuse)
    with pytest.raises(dram.DramPoolError, match="cannot seal"):
        dram.create_dram_pool("n", "e", 4096, 4096, False)
    assert len(seen) == 1
    with pytest.raises(OSError) as error:
        os.fstat(seen[0])
    assert error.value.errno == errno.EBADF
