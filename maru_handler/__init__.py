# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Maru Handler package.

This package provides the main client interface for Maru shared memory KV cache.
"""

from maru_common.logging_setup import setup_package_logging

setup_package_logging("maru_handler")

from .handler import MaruHandler  # noqa: E402
from .memory import OwnedRegionManager  # noqa: E402
from .rpc_client import RpcClient  # noqa: E402

__all__ = [
    "MaruHandler",
    "OwnedRegionManager",
    "RpcClient",
]
