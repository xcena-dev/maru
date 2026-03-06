# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Constants for maru_shm.

Defines protection flags, mapping flags, default paths, and alignment constants.
"""

import os

# Memory protection flags (same as POSIX mmap)
PROT_NONE = 0x0
PROT_READ = 0x1
PROT_WRITE = 0x2
PROT_EXEC = 0x4

# Memory mapping flags (same as POSIX mmap)
MAP_SHARED = 0x01
MAP_PRIVATE = 0x02

# Default socket and state paths
DEFAULT_SOCKET_PATH = os.environ.get(
    "MARU_SOCKET_PATH", "/run/maru-resourced/maru-resourced.sock"
)
DEFAULT_STATE_DIR = os.environ.get("MARU_STATE_DIR", "/var/lib/maru-resourced")

# Alignment
DEFAULT_ALIGN_BYTES = 2 * 1024 * 1024  # 2 MiB (DAX hugepage alignment)

# Sentinel: allocate from any pool
ANY_POOL_ID = 0xFFFFFFFF
