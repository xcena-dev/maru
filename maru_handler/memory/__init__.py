# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Memory management components for Maru Handler.

Provides:
- DaxMapper: Memory mapping via MaruShmClient (RPC mode, owns all mmap/munmap)
- OwnedRegionManager: Unified region management (pure allocator, mapper-agnostic)
- PagedMemoryAllocator: Fixed-size paged allocator (pure page management)
- Types: MappedRegion, MarufsMappedRegion, MemoryInfo, OwnedRegion
"""

from .allocator import PagedMemoryAllocator
from .mapper import DaxMapper
from .owned_region_manager import OwnedRegionManager
from .types import (
    AllocHandle,
    MappedRegion,
    MarufsMappedRegion,
    MemoryInfo,
    OwnedRegion,
)

__all__ = [
    "AllocHandle",
    "DaxMapper",
    "MappedRegion",
    "MarufsMappedRegion",
    "MemoryInfo",
    "OwnedRegion",
    "OwnedRegionManager",
    "PagedMemoryAllocator",
]
