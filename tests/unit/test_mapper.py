# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tests for DaxMapper."""

import ctypes
from unittest.mock import MagicMock, patch

import pytest
from conftest import _make_handle

from maru_handler.memory import DaxMapper


class TestDaxMapperConstructor:
    """Test pool_type-based constructor."""

    def test_default_creates_shm_client(self):
        """DaxMapper() with no args creates MaruShmClient."""
        from maru_shm import MaruShmClient

        mapper = DaxMapper()
        assert isinstance(mapper._client, MaruShmClient)

    def test_devdax_creates_shm_client(self):
        """DaxMapper(pool_type='devdax') creates MaruShmClient."""
        from maru_shm import MaruShmClient

        mapper = DaxMapper(pool_type="devdax")
        assert isinstance(mapper._client, MaruShmClient)

    def test_marufs_creates_marufs_client(self):
        """DaxMapper(pool_type='marufs') creates MarufsClient."""
        from marufs import MarufsClient

        mapper = DaxMapper(pool_type="marufs")
        assert isinstance(mapper._client, MarufsClient)

    def test_unknown_pool_type_falls_back_to_shm(self):
        """Unknown pool_type falls back to MaruShmClient."""
        from maru_shm import MaruShmClient

        mapper = DaxMapper(pool_type="unknown")
        assert isinstance(mapper._client, MaruShmClient)


class TestDaxMapperMap:
    """Test map/unmap operations."""

    def test_map_region(self):
        mapper = DaxMapper()
        handle = _make_handle(1, 4096)
        region = mapper.map_region(handle)

        assert region.region_id == 1
        assert region.size == 4096
        assert region.is_mapped

    def test_map_region_idempotent(self):
        mapper = DaxMapper()
        handle = _make_handle(1, 4096)
        r1 = mapper.map_region(handle)
        r2 = mapper.map_region(handle)
        assert r1 is r2

    def test_unmap_region(self):
        mapper = DaxMapper()
        handle = _make_handle(1, 4096)
        mapper.map_region(handle)

        assert mapper.unmap_region(1) is True
        assert mapper.get_region(1) is None

    def test_unmap_unknown_region(self):
        mapper = DaxMapper()
        assert mapper.unmap_region(999) is False

    def test_map_calls_register(self):
        """map_region should call cudaHostRegister."""
        mock_torch = MagicMock()
        mock_torch.cuda.is_available.return_value = True
        mock_cudart = MagicMock()
        mock_cudart.cudaHostRegister.return_value = (0,)
        mock_torch.cuda.cudart.return_value = mock_cudart

        mapper = DaxMapper()
        handle = _make_handle(1, 4096)

        with patch.dict("sys.modules", {"torch": mock_torch}):
            region = mapper.map_region(handle)

        addr = ctypes.addressof(ctypes.c_char.from_buffer(region._buffer_view))
        mock_cudart.cudaHostRegister.assert_called_once_with(addr, 4096, 0)


class TestDaxMapperQuery:
    """Test query operations."""

    def test_get_region(self):
        mapper = DaxMapper()
        handle = _make_handle(1, 4096)
        mapper.map_region(handle)

        region = mapper.get_region(1)
        assert region is not None
        assert region.region_id == 1

    def test_get_region_unknown(self):
        mapper = DaxMapper()
        assert mapper.get_region(999) is None

    def test_get_buffer_view_convenience(self):
        """DaxMapper.get_buffer_view returns correct memoryview slice."""
        mapper = DaxMapper()
        handle = _make_handle(1, 4096)
        mapper.map_region(handle)

        # Get the mapped region and write test data
        region = mapper.get_region(1)
        test_data = b"convenience method test"
        region._mmap_obj.seek(200)
        region._mmap_obj.write(test_data)
        region._mmap_obj.seek(0)

        # Test the convenience method
        view = mapper.get_buffer_view(1, 200, len(test_data))
        assert view is not None
        assert bytes(view) == test_data

        # Test with unknown region
        view = mapper.get_buffer_view(999, 0, 100)
        assert view is None

        # Test out of bounds
        view = mapper.get_buffer_view(1, 5000, 100)
        assert view is None


class TestDaxMapperClose:
    """Test close operations."""

    def test_close_unmaps_all(self):
        mapper = DaxMapper()
        mapper.map_region(_make_handle(1, 4096))
        mapper.map_region(_make_handle(2, 2048))

        mapper.close()
        assert mapper.get_region(1) is None
        assert mapper.get_region(2) is None

    def test_close_empty(self):
        mapper = DaxMapper()
        mapper.close()  # should not raise


def _mock_torch_cuda():
    """Create a mock torch module with cuda.cudart() returning success."""
    mock_torch = MagicMock()
    mock_torch.cuda.is_available.return_value = True
    mock_cudart = MagicMock()
    mock_cudart.cudaHostRegister.return_value = (0,)
    mock_cudart.cudaHostUnregister.return_value = (0,)
    mock_torch.cuda.cudart.return_value = mock_cudart
    return mock_torch, mock_cudart


class TestDaxMapperCudaPin:
    """Test CUDA pin/unpin integrated in map/unmap."""

    def test_unmap_calls_unregister(self):
        """Unmap should call cudaHostUnregister."""
        mock_torch, mock_cudart = _mock_torch_cuda()

        mapper = DaxMapper()
        handle = _make_handle(1, 4096)

        with patch.dict("sys.modules", {"torch": mock_torch}):
            region = mapper.map_region(handle)
            pinned_addr = ctypes.addressof(
                ctypes.c_char.from_buffer(region._buffer_view)
            )

            mapper.unmap_region(1)

        mock_cudart.cudaHostUnregister.assert_called_once_with(pinned_addr)

    def test_close_calls_unregister_all(self):
        """Close should call cudaHostUnregister for all regions."""
        mock_torch, mock_cudart = _mock_torch_cuda()

        mapper = DaxMapper()

        with patch.dict("sys.modules", {"torch": mock_torch}):
            mapper.map_region(_make_handle(1, 4096))
            mapper.map_region(_make_handle(2, 2048))

            mapper.close()

        assert mock_cudart.cudaHostUnregister.call_count == 2

    def test_no_torch_graceful(self):
        """Missing torch should not raise on map_region."""
        mapper = DaxMapper()
        handle = _make_handle(1, 4096)

        with patch.dict("sys.modules", {"torch": None}):
            region = mapper.map_region(handle)

        assert region.is_mapped

    def test_cuda_unavailable_graceful(self):
        """CUDA unavailable should not raise on map_region."""
        mock_torch = MagicMock()
        mock_torch.cuda.is_available.return_value = False

        mapper = DaxMapper()
        handle = _make_handle(1, 4096)

        with patch.dict("sys.modules", {"torch": mock_torch}):
            region = mapper.map_region(handle)

        assert region.is_mapped


class TestDaxMapperErrorPaths:
    """Test error paths for 100% coverage."""

    def test_map_region_mmap_exception(self):
        """Test map_region() when _client.mmap() raises exception → RuntimeError."""
        from unittest.mock import patch

        mapper = DaxMapper()
        handle = _make_handle(1, 4096)

        # Mock the client to raise an exception on mmap
        with patch.object(mapper._client, "mmap", side_effect=OSError("mmap failed")):
            with pytest.raises(RuntimeError, match="Failed to map region 1"):
                mapper.map_region(handle)

    def test_map_cuda_pin_runtime_error_logged(self):
        """Test CUDA pin raises non-ImportError (e.g., RuntimeError) → warning logged."""
        import logging
        from unittest.mock import MagicMock, patch

        mock_torch = MagicMock()
        mock_torch.cuda.is_available.return_value = True
        mock_cudart = MagicMock()
        # Simulate RuntimeError during cudaHostRegister
        mock_cudart.cudaHostRegister.side_effect = RuntimeError("CUDA error")
        mock_torch.cuda.cudart.return_value = mock_cudart

        mapper = DaxMapper()
        handle = _make_handle(1, 4096)

        with patch.dict("sys.modules", {"torch": mock_torch}):
            with patch.object(
                logging.getLogger("maru_handler.memory.mapper"), "warning"
            ) as mock_warning:
                region = mapper.map_region(handle)

        # Should log warning for non-ImportError
        assert mock_warning.called
        assert "cudaHostRegister failed" in mock_warning.call_args[0][0]
        assert region.is_mapped

    def test_unmap_cuda_unpin_runtime_error_logged(self):
        """Test CUDA unpin raises non-ImportError (e.g., RuntimeError) → warning logged."""
        import logging
        from unittest.mock import MagicMock, patch

        mock_torch = MagicMock()
        mock_torch.cuda.is_available.return_value = True
        mock_cudart = MagicMock()
        mock_cudart.cudaHostRegister.return_value = (0,)
        # Simulate RuntimeError during cudaHostUnregister
        mock_cudart.cudaHostUnregister.side_effect = RuntimeError("CUDA unpin error")
        mock_torch.cuda.cudart.return_value = mock_cudart

        mapper = DaxMapper()
        handle = _make_handle(1, 4096)

        with patch.dict("sys.modules", {"torch": mock_torch}):
            mapper.map_region(handle)

            with patch.object(
                logging.getLogger("maru_handler.memory.mapper"), "warning"
            ) as mock_warning:
                mapper.unmap_region(1)

        # Should log warning for non-ImportError
        assert mock_warning.called
        assert "cudaHostUnregister failed" in mock_warning.call_args[0][0]

    def test_unmap_region_general_exception(self):
        """Test unmap_region() general exception during unmap → logged and returns False."""
        import logging
        from unittest.mock import patch

        mapper = DaxMapper()
        handle = _make_handle(1, 4096)
        mapper.map_region(handle)

        # Mock munmap to raise an exception
        with patch.object(
            mapper._client, "munmap", side_effect=Exception("munmap failed")
        ):
            with patch.object(
                logging.getLogger("maru_handler.memory.mapper"), "error"
            ) as mock_error:
                result = mapper.unmap_region(1)

        assert result is False
        assert mock_error.called
        # Check the format string pattern
        assert "Error unmapping region %d" in mock_error.call_args[0][0]

    def test_close_region_is_none_after_pop(self):
        """Test close() when region is None after pop (edge case)."""
        mapper = DaxMapper()

        # Manually insert None to simulate edge case
        mapper._regions[99] = None

        # Should not raise
        mapper.close()

    def test_close_cuda_unpin_non_import_error(self):
        """Test close() CUDA unpin non-ImportError exception → logged."""
        import logging
        from unittest.mock import MagicMock, patch

        mock_torch = MagicMock()
        mock_torch.cuda.is_available.return_value = True
        mock_cudart = MagicMock()
        mock_cudart.cudaHostRegister.return_value = (0,)
        # Simulate RuntimeError during cudaHostUnregister
        mock_cudart.cudaHostUnregister.side_effect = OSError("CUDA unpin error")
        mock_torch.cuda.cudart.return_value = mock_cudart

        mapper = DaxMapper()
        handle = _make_handle(1, 4096)

        with patch.dict("sys.modules", {"torch": mock_torch}):
            mapper.map_region(handle)

            with patch.object(
                logging.getLogger("maru_handler.memory.mapper"), "warning"
            ) as mock_warning:
                mapper.close()

        # Should log warning for non-ImportError
        assert mock_warning.called
        assert "cudaHostUnregister failed" in mock_warning.call_args[0][0]

    def test_close_unmap_exception(self):
        """Test close() unmap exception during close → logged."""
        import logging
        from unittest.mock import patch

        mapper = DaxMapper()
        handle = _make_handle(1, 4096)
        mapper.map_region(handle)

        # Mock munmap to raise an exception
        with patch.object(
            mapper._client, "munmap", side_effect=Exception("munmap error")
        ):
            with patch.object(
                logging.getLogger("maru_handler.memory.mapper"), "error"
            ) as mock_error:
                mapper.close()

        assert mock_error.called
        # Check the format string pattern
        assert "Failed to unmap region %d during close" in mock_error.call_args[0][0]


class TestDaxMapperPrefault:
    """Test prefault behavior."""

    def test_prefault_madvise_success(self):
        """_prefault_region succeeds via madvise path."""
        mapper = DaxMapper()
        handle = _make_handle(1, 4096)
        region = mapper.map_region(handle)

        # Region should be mapped (prefault ran without error)
        assert region.is_mapped

    def test_prefault_madvise_fallback(self):
        """_prefault_region falls back to per-page read touch on OSError."""
        mapper = DaxMapper()
        handle = _make_handle(1, 4096)

        # Use an invalid madvise constant to force OSError from the kernel,
        # triggering the per-page fallback path.
        import maru_handler.memory.mapper as mapper_mod

        original = mapper_mod._MADV_POPULATE_WRITE
        mapper_mod._MADV_POPULATE_WRITE = 9999  # invalid → OSError
        try:
            region = mapper.map_region(handle)
            assert region.is_mapped
        finally:
            mapper_mod._MADV_POPULATE_WRITE = original

    def test_prefault_disabled_env(self):
        """prefault is skipped when MARU_PREFAULT=0."""
        from unittest.mock import patch

        import maru_handler.memory.mapper as mapper_mod

        original = mapper_mod._PREFAULT_ENABLED
        mapper_mod._PREFAULT_ENABLED = False
        try:
            mapper = DaxMapper()
            handle = _make_handle(1, 4096)

            with patch.object(DaxMapper, "_prefault_region") as mock_prefault:
                mapper.map_region(handle)
                mock_prefault.assert_not_called()
        finally:
            mapper_mod._PREFAULT_ENABLED = original

    def test_prefault_false_parameter(self):
        """prefault=False skips _prefault_region entirely."""
        from unittest.mock import patch

        mapper = DaxMapper()
        handle = _make_handle(1, 4096)

        with patch.object(DaxMapper, "_prefault_region") as mock_prefault:
            region = mapper.map_region(handle, prefault=False)
            mock_prefault.assert_not_called()

        assert region.is_mapped


class TestDaxMapperMarufsIntegration:
    """Test DaxMapper with marufs pool_type."""

    def test_map_region_with_marufs(self):
        """map_region works with marufs client."""
        import mmap as mmap_module

        mock_client = MagicMock()
        # Use a real anonymous mmap so memoryview() works
        real_mmap = mmap_module.mmap(-1, 4096)
        mock_client.mmap.return_value = real_mmap

        with patch("maru_handler.memory.mapper.MarufsClient", return_value=mock_client):
            mapper = DaxMapper(pool_type="marufs")

        handle = _make_handle(1, 4096)
        region = mapper.map_region(handle, prefault=False)

        assert region.region_id == 1
        mock_client.mmap.assert_called_once()
        mapper.close()

    def test_unmap_buffer_error_deferred_gc(self):
        """BufferError on munmap is caught and deferred to GC."""
        mapper = DaxMapper()
        handle = _make_handle(1, 4096)
        mapper.map_region(handle)

        with patch.object(mapper._client, "munmap", side_effect=BufferError("held")):
            result = mapper.unmap_region(1)

        assert result is True
        assert mapper.get_region(1) is None

    def test_close_buffer_error_deferred_gc(self):
        """BufferError on munmap during close() is caught and deferred to GC."""
        mapper = DaxMapper()
        mapper.map_region(_make_handle(1, 4096))
        mapper.map_region(_make_handle(2, 2048))

        with patch.object(mapper._client, "munmap", side_effect=BufferError("held")):
            mapper.close()

        assert mapper.get_region(1) is None
        assert mapper.get_region(2) is None
