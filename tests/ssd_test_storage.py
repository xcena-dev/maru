# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Probe the actual filesystem used by SSD tests and CI."""

import os
import sys
import tempfile
from pathlib import Path

from maru_storage_service.drivers.ssd_alignment import (
    DioAlignment,
    SsdAlignmentError,
    aligned_buffer,
    direct_alignment,
    round_up,
)


def probe_direct_io(directory: Path) -> DioAlignment:
    """Require alignment reporting, preallocation and a real O_DIRECT roundtrip."""
    with tempfile.TemporaryDirectory(prefix="dio-probe-", dir=directory) as work:
        fd = os.open(
            Path(work) / "probe",
            os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_DIRECT,
            0o600,
        )
        try:
            alignment = direct_alignment(fd)
            size = round_up(4096, alignment.step)
            os.posix_fallocate(fd, 0, size)
            with aligned_buffer(size, alignment.memory) as view:
                view[:] = b"p" * size
                if os.pwritev(fd, [view], 0) != size:
                    raise SsdAlignmentError("direct I/O probe returned a short write")
                view[:] = b"\0" * size
                if os.preadv(fd, [view], 0) != size or bytes(view) != b"p" * size:
                    raise SsdAlignmentError("direct I/O probe readback failed")
            return alignment
        finally:
            os.close(fd)


if __name__ == "__main__":
    root = Path(sys.argv[1]).resolve()
    try:
        alignment = probe_direct_io(root)
    except (OSError, SsdAlignmentError) as error:
        sys.exit(f"SSD test filesystem {root} (kernel {os.uname().release}): {error}")
    print(f"SSD test filesystem {root}: direct I/O roundtrip passed, {alignment}")
