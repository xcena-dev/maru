# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for memory types (MappedRegion, MemoryInfo)."""

import mmap

import pytest
from conftest import _make_handle

from maru_handler.memory.types import MappedRegion, MemoryInfo


class TestMappedRegion:
    """Test MappedRegion directly."""

    def test_get_buffer_view_valid(self):
        """get_buffer_view returns correct memoryview slice."""
        # Create anonymous mmap
        mmap_obj = mmap.mmap(-1, 4096)
        handle = _make_handle(1, 4096)

        # Write known data
        test_data = b"Hello, World!"
        mmap_obj.seek(100)
        mmap_obj.write(test_data)
        mmap_obj.seek(0)

        region = MappedRegion(
            region_id=1,
            handle=handle,
            size=4096,
            _mmap_obj=mmap_obj,
        )

        # Get buffer view at the offset where we wrote data
        view = region.get_buffer_view(100, len(test_data))
        assert view is not None
        assert bytes(view) == test_data

        # Clear local reference and release region before closing
        del view
        region.release()
        mmap_obj.close()

    def test_get_buffer_view_out_of_bounds(self):
        """get_buffer_view with out-of-bounds offset+length returns None."""
        mmap_obj = mmap.mmap(-1, 4096)
        handle = _make_handle(1, 4096)

        region = MappedRegion(
            region_id=1,
            handle=handle,
            size=4096,
            _mmap_obj=mmap_obj,
        )

        # Request beyond region size
        view = region.get_buffer_view(4000, 200)
        assert view is None

        # Negative offset
        view = region.get_buffer_view(-1, 10)
        assert view is None

        # Negative size
        view = region.get_buffer_view(0, -1)
        assert view is None

        region.release()
        mmap_obj.close()

    def test_get_buffer_view_zero_length(self):
        """get_buffer_view with zero length returns empty memoryview."""
        mmap_obj = mmap.mmap(-1, 4096)
        handle = _make_handle(1, 4096)

        region = MappedRegion(
            region_id=1,
            handle=handle,
            size=4096,
            _mmap_obj=mmap_obj,
        )

        view = region.get_buffer_view(0, 0)
        assert view is not None
        assert len(view) == 0

        del view
        region.release()
        mmap_obj.close()

    def test_read_bytes_valid(self):
        """read_bytes returns correct bytes."""
        mmap_obj = mmap.mmap(-1, 4096)
        handle = _make_handle(1, 4096)

        # Write known data
        test_data = b"Testing read_bytes"
        mmap_obj.seek(50)
        mmap_obj.write(test_data)
        mmap_obj.seek(0)

        region = MappedRegion(
            region_id=1,
            handle=handle,
            size=4096,
            _mmap_obj=mmap_obj,
        )

        result = region.read_bytes(50, len(test_data))
        assert result == test_data

        region.release()
        mmap_obj.close()

    def test_read_bytes_out_of_bounds(self):
        """read_bytes with out-of-bounds offset returns partial data."""
        mmap_obj = mmap.mmap(-1, 4096)
        handle = _make_handle(1, 4096)

        region = MappedRegion(
            region_id=1,
            handle=handle,
            size=4096,
            _mmap_obj=mmap_obj,
        )

        # Request beyond region size (memoryview slicing doesn't raise)
        result = region.read_bytes(4000, 200)
        # Should return only remaining 96 bytes
        assert len(result) == 96

        region.release()
        mmap_obj.close()

    def test_read_bytes_not_mapped(self):
        """read_bytes raises RuntimeError if region not mapped."""
        handle = _make_handle(1, 4096)

        region = MappedRegion(
            region_id=1,
            handle=handle,
            size=4096,
            _mmap_obj=None,
        )

        with pytest.raises(RuntimeError, match="not mapped"):
            region.read_bytes(0, 100)

    def test_is_mapped_property(self):
        """is_mapped returns correct status."""
        mmap_obj = mmap.mmap(-1, 4096)
        handle = _make_handle(1, 4096)

        region = MappedRegion(
            region_id=1,
            handle=handle,
            size=4096,
            _mmap_obj=mmap_obj,
        )

        assert region.is_mapped is True

        region.release()
        assert region.is_mapped is False

        mmap_obj.close()

    def test_get_buffer_view_returns_none_when_not_mapped(self):
        """get_buffer_view returns None if region not mapped."""
        handle = _make_handle(1, 4096)

        region = MappedRegion(
            region_id=1,
            handle=handle,
            size=4096,
            _mmap_obj=None,
        )

        view = region.get_buffer_view(0, 100)
        assert view is None

    def test_release_clears_buffer_and_mmap(self):
        """release() sets _buffer_view and _mmap_obj to None."""
        mmap_obj = mmap.mmap(-1, 4096)
        handle = _make_handle(1, 4096)

        region = MappedRegion(
            region_id=1,
            handle=handle,
            size=4096,
            _mmap_obj=mmap_obj,
        )

        assert region.is_mapped is True
        assert region._buffer_view is not None

        region.release()

        assert region.is_mapped is False
        assert region._buffer_view is None
        assert region._mmap_obj is None
        # get_buffer_view should return None after release
        assert region.get_buffer_view(0, 100) is None

        mmap_obj.close()


class TestMemoryInfo:
    """Test MemoryInfo."""

    def test_memory_info_from_memoryview(self):
        """MemoryInfo with memoryview returns correct view and nbytes."""
        test_data = b"test data"
        view = memoryview(test_data)

        info = MemoryInfo(view=view)

        assert info.view == view
        assert bytes(info.view) == test_data
        assert info.view.nbytes == len(test_data)

    def test_memory_info_from_bytearray(self):
        """MemoryInfo can wrap memoryview of bytearray."""
        data = bytearray(b"mutable test")
        view = memoryview(data)

        info = MemoryInfo(view=view)

        assert bytes(info.view) == b"mutable test"
        assert info.view.nbytes == len(data)

    def test_memory_info_writable_view(self):
        """MemoryInfo preserves writable memoryview."""
        data = bytearray(b"writable")
        view = memoryview(data)

        info = MemoryInfo(view=view)

        assert not info.view.readonly
        # Modify through the view
        info.view[0] = ord(b"W")
        assert bytes(info.view) == b"Writable"
