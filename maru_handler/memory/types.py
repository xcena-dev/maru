# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Type definitions for memory management components."""

import logging
import mmap as mmap_module
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from maru_shm import MaruHandle

    from .allocator import PagedMemoryAllocator


@dataclass
class MappedRegion:
    """A memory-mapped region via MaruShmClient.

    Used internally by DaxMapper to track all mapped regions.
    Eagerly creates memoryview(mmap_obj) at construction time.
    """

    region_id: int  # MaruHandle's region_id
    handle: "MaruHandle"  # Handle for mmap/munmap operations
    size: int  # Size of the region

    # Raw mmap object — kept for lifecycle (GC prevention) and munmap
    _mmap_obj: mmap_module.mmap | None = field(default=None, repr=False)
    # memoryview of the entire mmap — created eagerly in __post_init__
    _buffer_view: memoryview | None = field(default=None, repr=False, init=False)

    def __post_init__(self):
        if self._mmap_obj is not None:
            self._buffer_view = memoryview(self._mmap_obj)
            logger.debug(
                "Region %d: memoryview created, size=%d, readonly=%s",
                self.region_id,
                len(self._buffer_view),
                self._buffer_view.readonly,
            )

    @property
    def is_mapped(self) -> bool:
        """Check if the region is currently mapped."""
        return self._mmap_obj is not None

    def get_buffer_view(self, offset: int, size: int) -> memoryview | None:
        """Return a zero-copy memoryview slice.

        RW mmap -> writable memoryview, RO mmap -> readonly memoryview.
        The returned memoryview is only valid while the region remains mapped.
        """
        if self._buffer_view is None:
            return None
        if offset < 0 or size < 0 or offset + size > len(self._buffer_view):
            return None
        mv = self._buffer_view[offset : offset + size]
        return mv

    def read_bytes(self, offset: int, size: int) -> bytes:
        """Read bytes from the mapped region.

        Args:
            offset: Byte offset within the region
            size: Number of bytes to read

        Raises:
            RuntimeError: If the region is not mapped
        """
        if self._buffer_view is not None:
            return bytes(self._buffer_view[offset : offset + size])

        raise RuntimeError(f"Region {self.region_id} is not mapped")

    def release(self) -> None:
        """Release memoryview and mmap references (called before munmap).

        Calls memoryview.release() to explicitly drop the buffer export,
        allowing the underlying mmap to be closed even if caller-held
        slices (AllocHandle.buf, MemoryInfo.view) still exist in scope.
        """
        if self._buffer_view is not None:
            try:
                self._buffer_view.release()
            except ValueError:
                pass  # already released
            self._buffer_view = None
        self._mmap_obj = None


@dataclass
class OwnedRegion:
    """An owned (write-enabled) region with its own allocator.

    Used by OwnedRegionManager to track multiple owned regions.
    """

    region_id: int
    allocator: "PagedMemoryAllocator"


@dataclass(eq=False)
class AllocHandle:
    """Handle returned by MaruHandler.alloc() for zero-copy writes.

    Contains a writable memoryview into CXL mmap memory and allocation
    metadata. The caller writes directly to ``buf``, then passes the
    handle to ``store(key, handle)`` to register without copying.

    Typical zero-copy flow::

        handle = handler.alloc(size=len(data))
        handle.buf[:len(data)] = data
        handler.store(key=key, handle=handle)
    """

    buf: memoryview
    _region_id: int
    _page_index: int
    _size: int

    @property
    def region_id(self) -> int:
        """Region ID of the allocated page."""
        return self._region_id

    @property
    def page_index(self) -> int:
        """Page index within the region."""
        return self._page_index

    @property
    def size(self) -> int:
        """Requested allocation size in bytes."""
        return self._size


@dataclass
class MemoryInfo:
    """Zero-copy data descriptor using memoryview.

    Interface type between LMCache (MemoryObj) and Maru (mmap region).
    Supports both RW and RO regions via a single memoryview field.

    PUT: connector creates MemoryInfo(view=memory_obj.byte_array)
         handler writes via memoryview slice assignment (buf[off:off+n] = view)

    GET: handler returns MemoryInfo(view=memoryview_slice)
         connector creates tensor via torch.frombuffer(info.view)

    Data size is available via len(view) or view.nbytes.
    """

    view: memoryview
    region_id: int = 0
    page_index: int = 0
