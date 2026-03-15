# SPDX-License-Identifier: Apache-2.0
"""
Maru LMCache integration — memory allocator and storage backend support.

Usage:
    from maru_lmcache import CxlMemoryAllocator

Deprecated modules (use MaruBackend storage backend instead):
    - maru_lmcache.connector (MaruConnector)
    - maru_lmcache.adapter (MaruConnectorAdapter)
"""

__all__ = ["CxlMemoryAllocator"]


def __getattr__(name: str):
    if name == "CxlMemoryAllocator":
        from maru_lmcache.allocator import CxlMemoryAllocator

        return CxlMemoryAllocator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
