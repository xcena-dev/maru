# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruServer - Central metadata server for Maru shared memory KV cache."""

from maru_common.logging_setup import setup_package_logging  # noqa: E402

setup_package_logging("maru_server")

from .allocation_manager import AllocationInfo, AllocationManager  # noqa: E402
from .kv_manager import KVEntry, KVManager  # noqa: E402
from .rpc_server import RpcServer  # noqa: E402
from .server import MaruServer  # noqa: E402

__version__ = "0.1.0"

__all__ = [
    "MaruServer",
    "RpcServer",
    "KVManager",
    "KVEntry",
    "AllocationManager",
    "AllocationInfo",
]
