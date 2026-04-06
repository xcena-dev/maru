# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""maru_shm — Shared memory client library and shared types for Maru.

This package provides the types, IPC protocol, and client for communicating
with the Maru Resource Manager.
"""

from maru_common.logging_setup import setup_package_logging

setup_package_logging("maru_shm")

from .client import MaruShmClient  # noqa: E402
from .constants import (  # noqa: E402
    DEFAULT_ADDRESS,
    DEFAULT_ALIGN_BYTES,
    DEFAULT_STATE_DIR,
    MAP_PRIVATE,
    MAP_SHARED,
    PROT_EXEC,
    PROT_NONE,
    PROT_READ,
    PROT_WRITE,
)
from .types import DaxType, MaruHandle, MaruPoolInfo  # noqa: E402

__all__ = [
    # Types
    "MaruHandle",
    "MaruPoolInfo",
    "DaxType",
    # Protection flags
    "PROT_NONE",
    "PROT_READ",
    "PROT_WRITE",
    "PROT_EXEC",
    # Mapping flags
    "MAP_SHARED",
    "MAP_PRIVATE",
    # Client
    "MaruShmClient",
    # Paths / defaults
    "DEFAULT_ADDRESS",
    "DEFAULT_STATE_DIR",
    "DEFAULT_ALIGN_BYTES",
]
