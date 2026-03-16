# SPDX-License-Identifier: Apache-2.0
"""
Maru LMCache integration — memory adapter and storage backend support.

Usage:
    from maru_lmcache import CxlMemoryAdapter
"""

__all__ = ["CxlMemoryAdapter"]


def __getattr__(name: str):
    if name == "CxlMemoryAdapter":
        from maru_lmcache.allocator import CxlMemoryAdapter

        return CxlMemoryAdapter
    # Backward compatibility: old name still works
    if name == "CxlMemoryAllocator":
        from maru_lmcache.allocator import CxlMemoryAdapter

        return CxlMemoryAdapter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
