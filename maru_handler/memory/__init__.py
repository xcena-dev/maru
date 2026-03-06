# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Memory management components for Maru Handler.

Provides:
- DaxMapper: Memory mapping via MaruShmClient (owns all mmap/munmap)
- OwnedRegionManager: Multiple owned region management + allocation
- PagedMemoryAllocator: Fixed-size paged allocator (pure page management)
- Types: MappedRegion, MemoryInfo, OwnedRegion
"""

from .allocator import PagedMemoryAllocator
from .mapper import DaxMapper
from .owned_region_manager import OwnedRegionManager
from .types import AllocHandle, MappedRegion, MemoryInfo, OwnedRegion

__all__ = [
    "AllocHandle",
    "DaxMapper",
    "MappedRegion",
    "MemoryInfo",
    "OwnedRegion",
    "OwnedRegionManager",
    "PagedMemoryAllocator",
]
