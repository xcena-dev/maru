# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for MaruHandler client interface.

Uses mocked RPC — no real ZMQ server needed.
"""

from unittest.mock import MagicMock

import pytest
from conftest import _make_handle

from maru import MaruConfig, MaruHandler
from maru_handler.handler import _gil_free_memcpy
from maru_handler.memory import MemoryInfo

# =============================================================================
# _gil_free_memcpy tests
# =============================================================================


class TestGilFreeMemcpy:
    """Unit tests for the GIL-free memcpy helper."""

    def test_copy_from_bytes(self):
        """Copy bytes into a writable memoryview."""
        dst = bytearray(16)
        src = b"hello"
        _gil_free_memcpy(memoryview(dst), src, len(src))
        assert dst[:5] == b"hello"
        assert dst[5:] == b"\x00" * 11

    def test_copy_from_writable_memoryview(self):
        """Copy from a writable memoryview (production path)."""
        dst = bytearray(16)
        src = bytearray(b"world")
        _gil_free_memcpy(memoryview(dst), memoryview(src), len(src))
        assert dst[:5] == b"world"

    def test_copy_from_readonly_memoryview(self):
        """Copy from a read-only memoryview (bytes-backed)."""
        dst = bytearray(16)
        src = memoryview(b"readonly")
        assert src.readonly
        _gil_free_memcpy(memoryview(dst), src, len(src))
        assert dst[:8] == b"readonly"

    def test_partial_copy(self):
        """Only copy nbytes, not the full source."""
        dst = bytearray(16)
        src = b"abcdefgh"
        _gil_free_memcpy(memoryview(dst), src, 3)
        assert dst[:3] == b"abc"
        assert dst[3:] == b"\x00" * 13

    def test_copy_into_offset_slice(self):
        """Copy into a memoryview slice at an offset (like store() does)."""
        dst = bytearray(16)
        prefix = b"\x01\x02"
        data = b"payload"
        mv = memoryview(dst)
        _gil_free_memcpy(mv[0:], prefix, len(prefix))
        _gil_free_memcpy(mv[2:], data, len(data))
        assert dst[:2] == b"\x01\x02"
        assert dst[2:9] == b"payload"

    def test_large_copy(self):
        """Copy a larger buffer (1MB) to verify no size issues."""
        size = 1024 * 1024
        dst = bytearray(size)
        src = bytes(range(256)) * (size // 256)
        _gil_free_memcpy(memoryview(dst), src, size)
        assert bytes(dst) == src


class TestMaruHandlerConfig:
    """Test cases for MaruConfig (no RPC needed)."""

    def test_config_defaults(self):
        """Test MaruConfig default values."""
        config = MaruConfig()
        assert config.server_url == "tcp://localhost:5555"
        assert config.instance_id is not None
        assert config.pool_size == 1024 * 1024 * 100
        assert config.chunk_size_bytes == 1024 * 1024
        assert config.auto_connect is True

    def test_config_custom(self):
        """Test MaruConfig with custom values."""
        config = MaruConfig(
            server_url="tcp://192.168.1.100:5556",
            instance_id="my-instance",
            pool_size=1024 * 1024 * 200,
            chunk_size_bytes=4096,
            auto_connect=False,
        )
        assert config.server_url == "tcp://192.168.1.100:5556"
        assert config.instance_id == "my-instance"
        assert config.pool_size == 1024 * 1024 * 200
        assert config.chunk_size_bytes == 4096
        assert config.auto_connect is False

    def test_config_invalid_chunk_size(self):
        """Test that invalid chunk_size_bytes raises ValueError."""
        with pytest.raises(ValueError, match="chunk_size_bytes must be positive"):
            MaruConfig(chunk_size_bytes=0)

    def test_config_pool_smaller_than_chunk(self):
        """Test that pool_size < chunk_size_bytes raises ValueError."""
        with pytest.raises(
            ValueError, match="pool_size.*must be >= .*chunk_size_bytes"
        ):
            MaruConfig(pool_size=512, chunk_size_bytes=1024)

    def test_config_auto_instance_id(self):
        """Test that MaruConfig generates instance_id when None."""
        from maru_common.config import MaruConfig

        config = MaruConfig(pool_size=4096, chunk_size_bytes=1024)
        assert config.instance_id is not None
        assert len(config.instance_id) > 0

        # Two configs should have different instance_ids
        config2 = MaruConfig(pool_size=4096, chunk_size_bytes=1024)
        assert config.instance_id != config2.instance_id

    def test_config_env_override_eager_mmap_true(self, monkeypatch):
        """MARU_EAGER_MAP=1 forces eager_map on."""
        monkeypatch.setenv("MARU_EAGER_MAP", "1")

        config = MaruConfig(eager_map=False)

        assert config.eager_map is True

    def test_config_env_override_eager_mmap_false(self, monkeypatch):
        """MARU_EAGER_MAP=0 forces eager_map off."""
        monkeypatch.setenv("MARU_EAGER_MAP", "0")

        config = MaruConfig(eager_map=True)

        assert config.eager_map is False

    def test_config_env_override_eager_mmap_invalid(self, monkeypatch):
        """Invalid MARU_EAGER_MAP values raise a clear error."""
        monkeypatch.setenv("MARU_EAGER_MAP", "maybe")

        with pytest.raises(ValueError, match="MARU_EAGER_MAP must be one of"):
            MaruConfig()


class TestMaruHandlerEnsureConnected:
    """Test that operations require connection."""

    def test_store_before_connect_raises(self):
        """Create handler without connect(), call store(), verify RuntimeError."""
        config = MaruConfig(auto_connect=False)
        handler = MaruHandler(config)

        # Do NOT call connect()
        assert handler.connected is False

        # Try to store — should raise RuntimeError
        with pytest.raises(RuntimeError, match="Not connected"):
            handler.store(key="1", info=MemoryInfo(view=memoryview(b"data")))


# =============================================================================
# Coverage Tests — mocked RPC, no real server needed
# =============================================================================


def _make_mock_handler(pool_size=8192, chunk_size=1024):
    """Create a MaruHandler with mocked RPC for unit testing.

    Follows the pattern from test_thread_safety.py.
    """
    from maru_common import MaruConfig
    from maru_handler.handler import MaruHandler

    config = MaruConfig(
        pool_size=pool_size,
        chunk_size_bytes=chunk_size,
        auto_connect=False,
        use_async_rpc=False,
    )
    handler = MaruHandler(config)

    mock_rpc = MagicMock()
    mock_rpc.connect = MagicMock()

    # request_alloc returns a handle
    alloc_response = MagicMock()
    alloc_response.success = True
    alloc_response.handle = _make_handle(100, pool_size)
    alloc_response.mount_path = None  # DAX mode (no marufs)
    mock_rpc.request_alloc = MagicMock(return_value=alloc_response)

    # register_kv returns True (is_new=True)
    mock_rpc.register_kv = MagicMock(return_value=True)

    # delete_kv succeeds
    mock_rpc.delete_kv = MagicMock(return_value=True)

    # exists_kv returns False by default so store() actually proceeds
    mock_rpc.exists_kv = MagicMock(return_value=False)

    # lookup_kv returns found result
    lookup_result = MagicMock()
    lookup_result.found = True
    lookup_result.handle = _make_handle(100, pool_size)
    lookup_result.kv_offset = 0
    lookup_result.kv_length = 4
    mock_rpc.lookup_kv = MagicMock(return_value=lookup_result)

    # return_alloc succeeds
    mock_rpc.return_alloc = MagicMock()

    # close succeeds
    mock_rpc.close = MagicMock()

    handler._rpc = mock_rpc
    handler.connect()

    return handler


class TestMaruHandlerCoverage:
    """Unit tests targeting every uncovered line in handler.py.

    Uses mocked RPC — no real ZMQ server needed.
    """

    # =================================================================
    # connect() paths
    # =================================================================

    def test_connect_already_connected(self):
        """L108: connect() when already connected returns True immediately."""
        handler = _make_mock_handler()
        assert handler.connected is True

        # Second call should short-circuit
        result = handler.connect()
        assert result is True

        handler.close()

    def test_connect_alloc_fails(self):
        """L126-132: request_alloc returns success=False."""
        config = MaruConfig(
            pool_size=4096,
            chunk_size_bytes=1024,
            auto_connect=False,
            use_async_rpc=False,
        )
        handler = MaruHandler(config)

        mock_rpc = MagicMock()
        mock_rpc.connect = MagicMock()

        alloc_response = MagicMock()
        alloc_response.success = False
        alloc_response.handle = None
        alloc_response.error = "no space"
        mock_rpc.request_alloc = MagicMock(return_value=alloc_response)
        mock_rpc.close = MagicMock()

        handler._rpc = mock_rpc

        result = handler.connect()
        assert result is False
        assert handler.connected is False
        assert handler._owned is None
        mock_rpc.close.assert_called_once()

    def test_connect_add_region_raises(self, monkeypatch):
        """L137-148: add_region() raises exception during connect."""
        from maru_handler.memory import OwnedRegionManager

        config = MaruConfig(
            pool_size=4096,
            chunk_size_bytes=1024,
            auto_connect=False,
            use_async_rpc=False,
        )
        handler = MaruHandler(config)

        mock_rpc = MagicMock()
        mock_rpc.connect = MagicMock()

        alloc_response = MagicMock()
        alloc_response.success = True
        alloc_response.handle = _make_handle(100, 4096)
        alloc_response.mount_path = None  # DAX mode (no marufs)
        mock_rpc.request_alloc = MagicMock(return_value=alloc_response)
        mock_rpc.return_alloc = MagicMock()
        mock_rpc.close = MagicMock()

        handler._rpc = mock_rpc

        # Patch OwnedRegionManager.add_region to raise
        def failing_add(self_mgr, handle):
            raise RuntimeError("mmap failed")

        monkeypatch.setattr(OwnedRegionManager, "add_region", failing_add)

        result = handler.connect()
        assert result is False
        assert handler.connected is False
        assert handler._owned is None
        # return_alloc should have been called to give back the allocation
        mock_rpc.return_alloc.assert_called_once()
        mock_rpc.close.assert_called_once()

    def test_connect_general_exception(self):
        """L157-159: General exception during connect (e.g., RPC connect raises)."""
        config = MaruConfig(
            pool_size=4096,
            chunk_size_bytes=1024,
            auto_connect=False,
            use_async_rpc=False,
        )
        handler = MaruHandler(config)

        mock_rpc = MagicMock()
        mock_rpc.connect = MagicMock(side_effect=ConnectionError("refused"))

        handler._rpc = mock_rpc

        result = handler.connect()
        assert result is False
        assert handler.connected is False

    # =================================================================
    # close() paths
    # =================================================================

    def test_close_when_not_connected(self):
        """L168: close() when not connected is a no-op."""
        config = MaruConfig(auto_connect=False, use_async_rpc=False)
        handler = MaruHandler(config)
        assert handler.connected is False

        # Should return without doing anything
        handler.close()
        assert handler.connected is False

    def test_close_return_alloc_raises(self):
        """L183-184: return_alloc raises during close — logged but not re-raised."""
        handler = _make_mock_handler()

        handler._rpc.return_alloc = MagicMock(
            side_effect=RuntimeError("RPC error on return")
        )

        # close() should NOT raise despite return_alloc failure
        handler.close()
        assert handler.connected is False

    def test_close_general_exception(self):
        """L192-193: General exception during close."""
        handler = _make_mock_handler()

        # Make _owned.close() raise to trigger the outer except block
        handler._owned.close = MagicMock(side_effect=RuntimeError("boom"))

        handler.close()
        # Despite exception, handler should be cleaned up
        assert handler.connected is False
        assert handler._owned is None

    # =================================================================
    # retrieve() paths
    # =================================================================

    def test_retrieve_key_not_found(self):
        """L329-330: lookup_kv returns not found."""
        handler = _make_mock_handler()

        lookup_result = MagicMock()
        lookup_result.found = False
        lookup_result.handle = None
        handler._rpc.lookup_kv = MagicMock(return_value=lookup_result)

        result = handler.retrieve(key="999")
        assert result is None

        handler.close()

    def test_retrieve_shared_region_on_demand_mapping(self):
        """L337-342: Shared region (not owned) triggers on-demand RO mapping."""
        handler = _make_mock_handler()

        # Store something first so handler is set up
        handler.store(key="1", info=MemoryInfo(view=memoryview(b"data")))

        # lookup_kv returns a handle pointing to a DIFFERENT region (shared)
        shared_handle = _make_handle(200, 4096)
        lookup_result = MagicMock()
        lookup_result.found = True
        lookup_result.handle = shared_handle
        lookup_result.kv_offset = 0
        lookup_result.kv_length = 4
        handler._rpc.lookup_kv = MagicMock(return_value=lookup_result)

        # The mapper should map the shared region on-demand
        result = handler.retrieve(key="2")
        assert result is not None

        # Verify the shared region was mapped
        assert handler._mapper.get_region(200) is not None

        handler.close()

    def test_retrieve_shared_region_map_fails(self):
        """L340-342: map_region raises exception for shared region."""
        handler = _make_mock_handler()

        shared_handle = _make_handle(300, 4096)
        lookup_result = MagicMock()
        lookup_result.found = True
        lookup_result.handle = shared_handle
        lookup_result.kv_offset = 0
        lookup_result.kv_length = 4
        handler._rpc.lookup_kv = MagicMock(return_value=lookup_result)

        # Make map_region fail for the shared region
        original_map = handler._mapper.map_region

        def failing_map(handle, read_only=False):
            if handle.region_id == 300:
                raise RuntimeError("mmap failed for shared region")
            return original_map(handle, read_only=read_only)

        handler._mapper.map_region = failing_map

        result = handler.retrieve(key="2")
        assert result is None

        handler.close()

    def test_retrieve_get_buffer_view_returns_none(self):
        """L348-349: get_buffer_view returns None."""
        handler = _make_mock_handler()

        # lookup returns a valid handle but with an invalid offset that produces None
        lookup_result = MagicMock()
        lookup_result.found = True
        lookup_result.handle = _make_handle(100, 8192)
        lookup_result.kv_offset = 99999  # out of bounds
        lookup_result.kv_length = 4
        handler._rpc.lookup_kv = MagicMock(return_value=lookup_result)

        result = handler.retrieve(key="1")
        assert result is None

        handler.close()

    # =================================================================
    # delete() paths
    # =================================================================

    def test_delete_key_not_in_local_tracking(self):
        """L389: delete_kv succeeds but key not in _key_to_location."""
        handler = _make_mock_handler()

        handler._rpc.delete_kv = MagicMock(return_value=True)

        # key 999 was never stored locally
        result = handler.delete(key="999")
        assert result is True

        handler.close()

    # =================================================================
    # batch_retrieve() paths
    # =================================================================

    def test_batch_retrieve_rpc_raises(self):
        """L454-456: batch_lookup_kv raises exception."""
        handler = _make_mock_handler()

        handler._rpc.batch_lookup_kv = MagicMock(side_effect=RuntimeError("RPC failed"))

        results = handler.batch_retrieve(keys=["1", "2", "3"])
        assert results == [None, None, None]

        handler.close()

    def test_batch_retrieve_entry_not_found(self):
        """L461-462: entry not found in batch lookup."""
        handler = _make_mock_handler()

        batch_resp = MagicMock()
        entry_found = MagicMock()
        entry_found.found = True
        entry_found.handle = _make_handle(100, 8192)
        entry_found.kv_offset = 0
        entry_found.kv_length = 4

        entry_not_found = MagicMock()
        entry_not_found.found = False
        entry_not_found.handle = None

        batch_resp.entries = [entry_found, entry_not_found, entry_found]
        handler._rpc.batch_lookup_kv = MagicMock(return_value=batch_resp)

        results = handler.batch_retrieve(keys=["1", "2", "3"])
        assert results[0] is not None
        assert results[1] is None
        assert results[2] is not None

        handler.close()

    def test_batch_retrieve_shared_region_on_demand(self):
        """L469-479: Shared region on-demand mapping in batch."""
        handler = _make_mock_handler()

        # Entry from shared region 200 (not owned)
        shared_handle = _make_handle(200, 4096)
        entry = MagicMock()
        entry.found = True
        entry.handle = shared_handle
        entry.kv_offset = 0
        entry.kv_length = 4

        batch_resp = MagicMock()
        batch_resp.entries = [entry]
        handler._rpc.batch_lookup_kv = MagicMock(return_value=batch_resp)

        results = handler.batch_retrieve(keys=["1"])
        assert results[0] is not None
        assert handler._mapper.get_region(200) is not None

        handler.close()

    def test_batch_retrieve_shared_region_map_fails(self):
        """L472-479: map_region exception in batch retrieve for shared region."""
        handler = _make_mock_handler()

        shared_handle = _make_handle(300, 4096)
        entry = MagicMock()
        entry.found = True
        entry.handle = shared_handle
        entry.kv_offset = 0
        entry.kv_length = 4

        batch_resp = MagicMock()
        batch_resp.entries = [entry]
        handler._rpc.batch_lookup_kv = MagicMock(return_value=batch_resp)

        original_map = handler._mapper.map_region

        def failing_map(handle, read_only=False):
            if handle.region_id == 300:
                raise RuntimeError("mmap failed")
            return original_map(handle, read_only=read_only)

        handler._mapper.map_region = failing_map

        results = handler.batch_retrieve(keys=["1"])
        assert results[0] is None

        handler.close()

    def test_batch_retrieve_get_buffer_view_none(self):
        """L485-487: get_buffer_view returns None in batch retrieve."""
        handler = _make_mock_handler()

        entry = MagicMock()
        entry.found = True
        entry.handle = _make_handle(100, 8192)
        entry.kv_offset = 99999  # out of bounds
        entry.kv_length = 4

        batch_resp = MagicMock()
        batch_resp.entries = [entry]
        handler._rpc.batch_lookup_kv = MagicMock(return_value=batch_resp)

        results = handler.batch_retrieve(keys=["1"])
        assert results[0] is None

        handler.close()

    # =================================================================
    # batch_store() paths
    # =================================================================

    def test_batch_store_closing_check_inside_lock(self):
        """L539: batch_store raises RuntimeError from inside write_lock when closing."""
        handler = _make_mock_handler()

        # Bypass _ensure_connected so we reach the check inside the lock
        handler._ensure_connected = lambda: None
        handler._closing.set()

        with pytest.raises(RuntimeError, match="Handler is closing"):
            handler.batch_store(
                keys=["1"],
                infos=[MemoryInfo(view=memoryview(b"data"))],
            )

        # Reset for cleanup
        handler._closing.clear()
        handler.close()

    def test_batch_store_prefixes_length_mismatch(self):
        """L535: prefixes length != keys length raises ValueError."""
        handler = _make_mock_handler()

        with pytest.raises(ValueError, match="prefixes must have the same length"):
            handler.batch_store(
                keys=["1", "2"],
                infos=[
                    MemoryInfo(view=memoryview(b"d1")),
                    MemoryInfo(view=memoryview(b"d2")),
                ],
                prefixes=[b"\x01"],  # only 1, but keys has 2
            )

        handler.close()

    def test_batch_store_format_cast(self):
        """L552: src.format != 'B' triggers cast."""
        import array

        handler = _make_mock_handler()

        # Create a memoryview with format 'i' (int) instead of 'B'
        arr = array.array("i", [1, 2, 3])
        mv = memoryview(arr)
        assert mv.format != "B"

        info = MemoryInfo(view=mv)

        # batch_exists_kv: key not on server
        batch_exists_resp = MagicMock()
        batch_exists_resp.results = [False]
        handler._rpc.batch_exists_kv = MagicMock(return_value=batch_exists_resp)

        batch_resp = MagicMock()
        batch_resp.success = True
        batch_resp.results = [True]
        handler._rpc.batch_register_kv = MagicMock(return_value=batch_resp)

        # The data size after cast to 'B' is 12 bytes (3 ints * 4 bytes)
        # chunk_size is 1024, so it should fit
        results = handler.batch_store(keys=["1"], infos=[info])
        assert results == [True]

        handler.close()

    def test_batch_store_total_size_exceeds_chunk(self):
        """L557-564: total_size exceeds chunk_size for a key."""
        handler = _make_mock_handler(chunk_size=64)

        # batch_exists_kv: key not on server so it proceeds to size check
        batch_exists_resp = MagicMock()
        batch_exists_resp.results = [False]
        handler._rpc.batch_exists_kv = MagicMock(return_value=batch_exists_resp)

        big_data = b"x" * 100  # exceeds 64-byte chunk
        results = handler.batch_store(
            keys=["1"],
            infos=[MemoryInfo(view=memoryview(big_data))],
        )
        assert results == [False]

        handler.close()

    def test_batch_store_overwrite_existing_key(self):
        """batch_store skips keys already in local map — idempotent, returns True."""
        handler = _make_mock_handler()

        # First store
        handler.store(key="42", info=MemoryInfo(view=memoryview(b"old")))
        assert "42" in handler._key_to_location

        # batch_exists_kv mock (Phase 1 check): key not on server either
        batch_exists_resp = MagicMock()
        batch_exists_resp.results = [False]
        handler._rpc.batch_exists_kv = MagicMock(return_value=batch_exists_resp)

        # batch_store with same key — should skip via local map check, return True
        results = handler.batch_store(
            keys=["42"],
            infos=[MemoryInfo(view=memoryview(b"new"))],
        )
        assert results == [True]
        # delete_kv never called — no overwrite, just skip
        handler._rpc.delete_kv.assert_not_called()

        handler.close()

    def test_batch_store_alloc_fails_expand_fails(self):
        """L577-584: allocation fails, expand fails."""
        handler = _make_mock_handler(pool_size=1024, chunk_size=1024)

        # Fill the single page
        handler.store(key="1", info=MemoryInfo(view=memoryview(b"fill")))

        # batch_exists_kv: key 2 not on server
        batch_exists_resp = MagicMock()
        batch_exists_resp.results = [False]
        handler._rpc.batch_exists_kv = MagicMock(return_value=batch_exists_resp)

        # Make expand fail
        alloc_fail = MagicMock()
        alloc_fail.success = False
        alloc_fail.handle = None
        handler._rpc.request_alloc = MagicMock(return_value=alloc_fail)

        results = handler.batch_store(
            keys=["2"],
            infos=[MemoryInfo(view=memoryview(b"data"))],
        )
        assert results == [False]

        handler.close()

    def test_batch_store_get_buffer_view_none(self):
        """L594-596: get_buffer_view returns None in batch_store."""
        handler = _make_mock_handler()

        # batch_exists_kv: key not on server
        batch_exists_resp = MagicMock()
        batch_exists_resp.results = [False]
        handler._rpc.batch_exists_kv = MagicMock(return_value=batch_exists_resp)

        # Make get_buffer_view return None
        original_get_buf = handler._mapper.get_buffer_view

        def return_none_buf(region_id, offset, size):
            return None

        handler._mapper.get_buffer_view = return_none_buf

        results = handler.batch_store(
            keys=["1"],
            infos=[MemoryInfo(view=memoryview(b"data"))],
        )
        assert results == [False]

        # Restore for cleanup
        handler._mapper.get_buffer_view = original_get_buf
        handler.close()

    def test_batch_store_register_rpc_raises(self):
        """L611-615: batch_register_kv RPC raises — free all, return [False]*len."""
        handler = _make_mock_handler()

        # batch_exists_kv: neither key on server
        batch_exists_resp = MagicMock()
        batch_exists_resp.results = [False, False]
        handler._rpc.batch_exists_kv = MagicMock(return_value=batch_exists_resp)

        handler._rpc.batch_register_kv = MagicMock(
            side_effect=RuntimeError("RPC failed")
        )

        results = handler.batch_store(
            keys=["1", "2"],
            infos=[
                MemoryInfo(view=memoryview(b"d1")),
                MemoryInfo(view=memoryview(b"d2")),
            ],
        )
        assert results == [False, False]

        handler.close()

    def test_batch_store_register_returns_failure(self):
        """L618-621: batch_register_kv returns success=False."""
        handler = _make_mock_handler()

        # batch_exists_kv: key not on server
        batch_exists_resp = MagicMock()
        batch_exists_resp.results = [False]
        handler._rpc.batch_exists_kv = MagicMock(return_value=batch_exists_resp)

        batch_resp = MagicMock()
        batch_resp.success = False
        handler._rpc.batch_register_kv = MagicMock(return_value=batch_resp)

        results = handler.batch_store(
            keys=["1"],
            infos=[MemoryInfo(view=memoryview(b"data"))],
        )
        assert results == [False]

        handler.close()

    # =================================================================
    # batch_exists() paths
    # =================================================================

    def test_batch_exists_rpc_raises(self):
        """L661-663: batch_exists_kv RPC raises → return [False]*len."""
        handler = _make_mock_handler()

        handler._rpc.batch_exists_kv = MagicMock(side_effect=RuntimeError("RPC failed"))

        results = handler.batch_exists(keys=["1", "2", "3"])
        assert results == [False, False, False]

        handler.close()

    # =================================================================
    # Properties
    # =================================================================

    def test_pool_handle_owned_is_none(self):
        """L674: pool_handle when _owned is None."""
        config = MaruConfig(auto_connect=False, use_async_rpc=False)
        handler = MaruHandler(config)

        assert handler._owned is None
        assert handler.pool_handle is None

    def test_pool_handle_first_rid_is_none(self):
        """L677: pool_handle when first_rid is None (no regions added)."""
        from maru_handler.memory import DaxMapper, OwnedRegionManager

        config = MaruConfig(auto_connect=False, use_async_rpc=False)
        handler = MaruHandler(config)
        # Set _owned to an empty OwnedRegionManager (no regions)
        handler._owned = OwnedRegionManager(mapper=DaxMapper(), chunk_size=1024)

        assert handler._owned.get_first_region_id() is None
        assert handler.pool_handle is None

    def test_allocator_owned_is_none(self):
        """L685: allocator when _owned is None."""
        config = MaruConfig(auto_connect=False, use_async_rpc=False)
        handler = MaruHandler(config)

        assert handler._owned is None
        assert handler.allocator is None

    def test_instance_id_property(self):
        """L696: instance_id property."""
        config = MaruConfig(
            instance_id="test-instance-123",
            auto_connect=False,
            use_async_rpc=False,
        )
        handler = MaruHandler(config)

        assert handler.instance_id == "test-instance-123"

    # =================================================================
    # _expand_region() paths
    # =================================================================

    def test_expand_region_rpc_raises(self):
        """L718-720: request_alloc RPC raises exception during expand."""
        handler = _make_mock_handler(pool_size=1024, chunk_size=1024)

        # Fill the single page
        handler.store(key="1", info=MemoryInfo(view=memoryview(b"fill")))

        # Make request_alloc raise
        handler._rpc.request_alloc = MagicMock(side_effect=RuntimeError("RPC timeout"))

        # Try to store another key, triggering expand
        result = handler.store(key="2", info=MemoryInfo(view=memoryview(b"data")))
        assert result is False

        handler.close()

    def test_expand_region_add_region_raises(self, monkeypatch):
        """L734-740: add_region raises during expand — catches, calls return_alloc."""
        from maru_handler.memory import OwnedRegionManager

        handler = _make_mock_handler(pool_size=1024, chunk_size=1024)

        # Fill the single page
        handler.store(key="1", info=MemoryInfo(view=memoryview(b"fill")))

        # request_alloc succeeds with a new region
        expand_response = MagicMock()
        expand_response.success = True
        expand_response.handle = _make_handle(200, 1024)
        handler._rpc.request_alloc = MagicMock(return_value=expand_response)

        # Make add_region fail
        def failing_add(self_mgr, handle):
            raise RuntimeError("mmap failed on expand")

        monkeypatch.setattr(OwnedRegionManager, "add_region", failing_add)

        result = handler.store(key="2", info=MemoryInfo(view=memoryview(b"data")))
        assert result is False
        # return_alloc should have been called for the failed region
        handler._rpc.return_alloc.assert_called()

        handler.close()

    # =================================================================
    # connect() edge: return_alloc also raises during add_region failure
    # =================================================================

    def test_connect_add_region_raises_and_return_alloc_raises(self, monkeypatch):
        """L144-145: return_alloc also raises when cleaning up after add_region failure."""
        from maru_handler.memory import OwnedRegionManager

        config = MaruConfig(
            pool_size=4096,
            chunk_size_bytes=1024,
            auto_connect=False,
            use_async_rpc=False,
        )
        handler = MaruHandler(config)

        mock_rpc = MagicMock()
        mock_rpc.connect = MagicMock()

        alloc_response = MagicMock()
        alloc_response.success = True
        alloc_response.handle = _make_handle(100, 4096)
        alloc_response.mount_path = None  # DAX mode (no marufs)
        mock_rpc.request_alloc = MagicMock(return_value=alloc_response)
        # return_alloc ALSO raises
        mock_rpc.return_alloc = MagicMock(
            side_effect=RuntimeError("return_alloc failed too")
        )
        mock_rpc.close = MagicMock()

        handler._rpc = mock_rpc

        def failing_add(self_mgr, handle):
            raise RuntimeError("mmap failed")

        monkeypatch.setattr(OwnedRegionManager, "add_region", failing_add)

        result = handler.connect()
        assert result is False
        assert handler.connected is False

    # =================================================================
    # store() happy path + edge cases (unit test with mocked RPC)
    # =================================================================

    def test_store_happy_path(self):
        """L220-308: Full store happy path (covers write_lock, allocate, write, register)."""
        handler = _make_mock_handler()

        data = b"hello world"
        info = MemoryInfo(view=memoryview(data))
        result = handler.store(key="42", info=info)
        assert result is True
        assert "42" in handler._key_to_location

        handler._rpc.register_kv.assert_called_once()

        handler.close()

    def test_store_with_memoryview(self):
        """store() accepts a raw memoryview via the info parameter."""
        handler = _make_mock_handler()

        data = b"hello memoryview"
        result = handler.store(key="100", info=memoryview(data))
        assert result is True
        assert "100" in handler._key_to_location

        handler._rpc.register_kv.assert_called_once()
        handler.close()

    def test_store_with_data_kwarg(self):
        """store() accepts a memoryview via the data keyword argument."""
        handler = _make_mock_handler()

        data = b"hello data kwarg"
        result = handler.store(key="200", data=memoryview(data))
        assert result is True
        assert "200" in handler._key_to_location

        handler._rpc.register_kv.assert_called_once()
        handler.close()

    def test_store_no_data_raises(self):
        """store() raises TypeError when neither info nor data is provided."""
        handler = _make_mock_handler()

        with pytest.raises(TypeError, match="Must provide data"):
            handler.store(key="300")

        handler.close()

    def test_store_closing_raises_inside_lock(self):
        """L222: store() raises RuntimeError from inside write_lock when closing."""
        handler = _make_mock_handler()

        # Bypass _ensure_connected so we reach the check inside the lock
        handler._ensure_connected = lambda: None
        handler._closing.set()

        with pytest.raises(RuntimeError, match="Handler is closing"):
            handler.store(key="1", info=MemoryInfo(view=memoryview(b"data")))

        handler._closing.clear()
        handler.close()

    def test_store_format_cast(self):
        """L227: src.format != 'B' triggers cast in store."""
        import array

        handler = _make_mock_handler()

        arr = array.array("i", [1, 2, 3])
        mv = memoryview(arr)
        assert mv.format != "B"

        result = handler.store(key="1", info=MemoryInfo(view=mv))
        assert result is True

        handler.close()

    def test_store_exceeds_chunk_size(self):
        """L244-249: total_size exceeds chunk_size in store."""
        handler = _make_mock_handler(chunk_size=64)

        big_data = b"x" * 100
        result = handler.store(key="1", info=MemoryInfo(view=memoryview(big_data)))
        assert result is False

        handler.close()

    def test_store_overwrite_existing_key(self):
        """store() now skips duplicates — second store is a no-op via local map check."""
        handler = _make_mock_handler()

        result1 = handler.store(key="1", info=MemoryInfo(view=memoryview(b"v1")))
        assert result1 is True
        assert "1" in handler._key_to_location

        # Second store same key: skipped via local _key_to_location check, returns True
        result2 = handler.store(key="1", info=MemoryInfo(view=memoryview(b"v2")))
        assert result2 is True
        # register_kv called only once (second store skipped before allocation)
        handler._rpc.register_kv.assert_called_once()
        # delete_kv never called — no overwrite logic
        handler._rpc.delete_kv.assert_not_called()
        assert "1" in handler._key_to_location

        handler.close()

    def test_store_expand_succeeds_but_second_alloc_none(self):
        """L265-267: expand succeeds but second allocate still returns None."""
        handler = _make_mock_handler(pool_size=1024, chunk_size=1024)

        # Fill the single page
        handler.store(key="1", info=MemoryInfo(view=memoryview(b"fill")))

        # expand succeeds but the new region also has no free pages
        expand_response = MagicMock()
        expand_response.success = True
        expand_response.handle = _make_handle(200, 1024)
        handler._rpc.request_alloc = MagicMock(return_value=expand_response)

        # Patch allocate to return None even after expand
        original_allocate = handler._owned.allocate

        call_count = [0]

        def always_none_after_first():
            call_count[0] += 1
            # Both calls return None (first triggers expand, second still None)
            return None

        handler._owned.allocate = always_none_after_first

        result = handler.store(key="2", info=MemoryInfo(view=memoryview(b"data")))
        assert result is False

        handler._owned.allocate = original_allocate
        handler.close()

    def test_store_get_buffer_view_none(self):
        """L278-279: get_buffer_view returns None in store."""
        handler = _make_mock_handler()

        original_get_buf = handler._mapper.get_buffer_view

        def return_none(region_id, offset, size):
            return None

        handler._mapper.get_buffer_view = return_none

        result = handler.store(key="1", info=MemoryInfo(view=memoryview(b"data")))
        assert result is False

        handler._mapper.get_buffer_view = original_get_buf
        handler.close()

    def test_store_with_prefix(self):
        """L284-285: prefix writing path in store."""
        handler = _make_mock_handler()

        prefix = b"\x01\x02"
        data = b"hello"
        result = handler.store(
            key="1", info=MemoryInfo(view=memoryview(data)), prefix=prefix
        )
        assert result is True

        handler.close()

    # =================================================================
    # delete() happy path
    # =================================================================

    def test_delete_closing_raises_inside_lock(self):
        """L389: delete raises RuntimeError from inside write_lock when closing."""
        handler = _make_mock_handler()

        # Bypass _ensure_connected so we reach the check inside the lock
        handler._ensure_connected = lambda: None
        handler._closing.set()

        with pytest.raises(RuntimeError, match="Handler is closing"):
            handler.delete(key="1")

        handler._closing.clear()
        handler.close()

    def test_delete_not_found_on_server(self):
        """L414-415: delete_kv returns False — key not found on server."""
        handler = _make_mock_handler()

        handler._rpc.delete_kv = MagicMock(return_value=False)

        result = handler.delete(key="999")
        assert result is False

        handler.close()

    def test_delete_with_local_tracking(self):
        """L396-397: delete key that IS in _key_to_location — frees page."""
        handler = _make_mock_handler()

        handler.store(key="1", info=MemoryInfo(view=memoryview(b"data")))
        assert "1" in handler._key_to_location

        result = handler.delete(key="1")
        assert result is True
        assert "1" not in handler._key_to_location

        handler.close()

    # =================================================================
    # get_stats()
    # =================================================================

    def test_get_stats(self):
        """L403-426: get_stats with mocked RPC."""
        handler = _make_mock_handler()

        stats_response = MagicMock()
        stats_response.kv_manager.total_entries = 5
        stats_response.kv_manager.total_size = 1000
        stats_response.allocation_manager.num_allocations = 2
        stats_response.allocation_manager.total_allocated = 8192
        stats_response.allocation_manager.active_clients = 1
        handler._rpc.get_stats = MagicMock(return_value=stats_response)

        stats = handler.get_stats()
        assert stats["kv_manager"]["total_entries"] == 5
        assert stats["kv_manager"]["total_size"] == 1000
        assert stats["allocation_manager"]["num_allocations"] == 2
        assert "store_regions" in stats
        assert "allocator" in stats

        handler.close()

    # =================================================================
    # batch_store() additional paths
    # =================================================================

    def test_batch_store_happy_path(self):
        """L541-644: Full batch_store happy path including register."""
        handler = _make_mock_handler()

        # batch_exists_kv: neither key on server
        batch_exists_resp = MagicMock()
        batch_exists_resp.results = [False, False]
        handler._rpc.batch_exists_kv = MagicMock(return_value=batch_exists_resp)

        batch_resp = MagicMock()
        batch_resp.success = True
        batch_resp.results = [True, True]
        handler._rpc.batch_register_kv = MagicMock(return_value=batch_resp)

        results = handler.batch_store(
            keys=["1", "2"],
            infos=[
                MemoryInfo(view=memoryview(b"d1")),
                MemoryInfo(view=memoryview(b"d2")),
            ],
        )
        assert results == [True, True]
        assert "1" in handler._key_to_location
        assert "2" in handler._key_to_location

        handler.close()

    def test_batch_store_with_prefixes(self):
        """L600-601: prefix writing path in batch_store."""
        handler = _make_mock_handler()

        # batch_exists_kv: key not on server
        batch_exists_resp = MagicMock()
        batch_exists_resp.results = [False]
        handler._rpc.batch_exists_kv = MagicMock(return_value=batch_exists_resp)

        batch_resp = MagicMock()
        batch_resp.success = True
        batch_resp.results = [True]
        handler._rpc.batch_register_kv = MagicMock(return_value=batch_resp)

        results = handler.batch_store(
            keys=["1"],
            infos=[MemoryInfo(view=memoryview(b"data"))],
            prefixes=[b"\x01\x02"],
        )
        assert results == [True]

        handler.close()

    def test_batch_store_expand_second_alloc_none(self):
        """L581-584: expand succeeds but second allocate returns None in batch_store."""
        handler = _make_mock_handler(pool_size=1024, chunk_size=1024)

        # Fill the single page
        handler.store(key="1", info=MemoryInfo(view=memoryview(b"fill")))

        # batch_exists_kv: key 2 not on server
        batch_exists_resp = MagicMock()
        batch_exists_resp.results = [False]
        handler._rpc.batch_exists_kv = MagicMock(return_value=batch_exists_resp)

        # expand succeeds
        expand_response = MagicMock()
        expand_response.success = True
        expand_response.handle = _make_handle(200, 1024)
        handler._rpc.request_alloc = MagicMock(return_value=expand_response)

        # Patch allocate to return None even after expand
        original_allocate = handler._owned.allocate

        def always_none():
            return None

        handler._owned.allocate = always_none

        batch_resp = MagicMock()
        batch_resp.success = True
        batch_resp.results = []
        handler._rpc.batch_register_kv = MagicMock(return_value=batch_resp)

        results = handler.batch_store(
            keys=["2"],
            infos=[MemoryInfo(view=memoryview(b"data"))],
        )
        assert results == [False]

        handler._owned.allocate = original_allocate
        handler.close()

    # =================================================================
    # batch_exists() happy path
    # =================================================================

    def test_batch_exists_happy_path(self):
        """L664: batch_exists_kv returns results normally."""
        handler = _make_mock_handler()

        batch_resp = MagicMock()
        batch_resp.results = [True, False, True]
        handler._rpc.batch_exists_kv = MagicMock(return_value=batch_resp)

        results = handler.batch_exists(keys=["1", "2", "3"])
        assert results == [True, False, True]

        handler.close()

    # =================================================================
    # Properties (happy paths)
    # =================================================================

    def test_pool_handle_normal(self):
        """L678-679: pool_handle when mapped region exists."""
        handler = _make_mock_handler()

        ph = handler.pool_handle
        assert ph is not None
        assert ph.length == 8192

        handler.close()

    def test_allocator_normal(self):
        """L686: allocator when _owned has regions."""
        handler = _make_mock_handler()

        alloc = handler.allocator
        assert alloc is not None
        assert alloc.page_count == 8

        handler.close()

    def test_owned_region_manager_property(self):
        """L691: owned_region_manager property."""
        handler = _make_mock_handler()

        mgr = handler.owned_region_manager
        assert mgr is not None

        handler.close()

    # =================================================================
    # _expand_region() happy path + edge
    # =================================================================

    def test_expand_region_happy_path(self):
        """L732-733: expand succeeds — add_region works, returns True."""
        handler = _make_mock_handler(pool_size=1024, chunk_size=1024)

        # Fill the single page
        handler.store(key="1", info=MemoryInfo(view=memoryview(b"fill")))

        # request_alloc returns a new valid region
        expand_response = MagicMock()
        expand_response.success = True
        expand_response.handle = _make_handle(200, 1024)
        handler._rpc.request_alloc = MagicMock(return_value=expand_response)

        # Store another key — triggers expansion
        result = handler.store(key="2", info=MemoryInfo(view=memoryview(b"data2")))
        assert result is True
        assert handler._owned.get_stats()["num_regions"] == 2

        handler.close()

    def test_expand_region_add_region_raises_and_return_alloc_raises(self, monkeypatch):
        """L738-739: return_alloc also raises during expand cleanup."""
        from maru_handler.memory import OwnedRegionManager

        handler = _make_mock_handler(pool_size=1024, chunk_size=1024)

        # Fill the single page
        handler.store(key="1", info=MemoryInfo(view=memoryview(b"fill")))

        expand_response = MagicMock()
        expand_response.success = True
        expand_response.handle = _make_handle(200, 1024)
        handler._rpc.request_alloc = MagicMock(return_value=expand_response)

        # return_alloc also raises
        handler._rpc.return_alloc = MagicMock(
            side_effect=RuntimeError("return_alloc failed")
        )

        def failing_add(self_mgr, handle):
            raise RuntimeError("mmap failed on expand")

        monkeypatch.setattr(OwnedRegionManager, "add_region", failing_add)

        result = handler.store(key="2", info=MemoryInfo(view=memoryview(b"data")))
        assert result is False

        handler.close()

    # =================================================================
    # Context manager
    # =================================================================

    def test_context_manager_auto_connect(self):
        """L755-757, 761: __enter__ with auto_connect and __exit__ calls close."""
        config = MaruConfig(
            pool_size=4096,
            chunk_size_bytes=1024,
            auto_connect=True,
            use_async_rpc=False,
        )
        handler = MaruHandler(config)

        mock_rpc = MagicMock()
        mock_rpc.connect = MagicMock()

        alloc_response = MagicMock()
        alloc_response.success = True
        alloc_response.handle = _make_handle(100, 4096)
        alloc_response.mount_path = None  # DAX mode (no marufs)
        mock_rpc.request_alloc = MagicMock(return_value=alloc_response)
        mock_rpc.return_alloc = MagicMock()
        mock_rpc.close = MagicMock()

        handler._rpc = mock_rpc

        with handler as h:
            assert h.connected is True

        assert handler.connected is False

    def test_connect_with_async_rpc(self):
        """Test connect() with use_async_rpc=True creates RpcAsyncClient."""
        from unittest.mock import patch

        from maru_common.config import MaruConfig
        from maru_common.protocol import RequestAllocResponse
        from maru_handler.handler import MaruHandler
        from maru_handler.rpc_async_client import RpcAsyncClient
        from maru_shm import MaruHandle

        config = MaruConfig(
            server_url="tcp://127.0.0.1:9999",
            pool_size=4096,
            chunk_size_bytes=1024,
            auto_connect=False,
            use_async_rpc=True,
        )
        handler = MaruHandler(config)

        mock_handle_dict = {
            "region_id": 1,
            "offset": 0,
            "length": 4096,
            "auth_token": 0,
        }
        mock_response = RequestAllocResponse(
            success=True,
            handle=MaruHandle.from_dict(mock_handle_dict),
        )

        with (
            patch.object(RpcAsyncClient, "connect") as mock_connect,
            patch.object(RpcAsyncClient, "request_alloc", return_value=mock_response),
            patch.object(RpcAsyncClient, "return_alloc", return_value=True),
            patch.object(RpcAsyncClient, "close"),
        ):
            success = handler.connect()
            assert success is True
            assert isinstance(handler._rpc, RpcAsyncClient)
            mock_connect.assert_called_once()

            handler.close()

    # =================================================================
    # exists() happy path
    # =================================================================

    def test_exists_happy_path(self):
        """L373-374: exists() calls _ensure_connected and returns RPC result."""
        handler = _make_mock_handler()

        handler._rpc.exists_kv = MagicMock(return_value=True)
        assert handler.exists(key="1") is True

        handler._rpc.exists_kv = MagicMock(return_value=False)
        assert handler.exists(key="999") is False

        handler.close()

    # =================================================================
    # batch_store() keys/infos mismatch
    # =================================================================

    def test_batch_store_keys_infos_mismatch(self):
        """L533: batch_store with mismatched keys/infos raises ValueError."""
        handler = _make_mock_handler()

        with pytest.raises(
            ValueError, match="keys and infos must have the same length"
        ):
            handler.batch_store(
                keys=["1", "2"],
                infos=[MemoryInfo(view=memoryview(b"only_one"))],
            )

        handler.close()

    # =================================================================
    # _ensure_connected() closing path
    # =================================================================

    def test_ensure_connected_closing_raises(self):
        """L745: _ensure_connected raises RuntimeError when _closing is set."""
        handler = _make_mock_handler()
        handler._closing.set()

        with pytest.raises(RuntimeError, match="Handler is closing"):
            handler.exists(key="1")

        handler._closing.clear()
        handler.close()


class TestMaruHandlerDuplicateSkip:
    """Test store/batch_store duplicate key skip paths."""

    def test_store_skipped_by_server_exists(self):
        """L258-259: store() skips when exists_kv returns True (server-side dup)."""
        handler = _make_mock_handler()

        # Key NOT in local map, but server says it exists
        handler._rpc.exists_kv = MagicMock(return_value=True)
        result = handler.store(key="42", info=MemoryInfo(view=memoryview(b"data")))
        assert result is True
        # Should not have allocated or registered
        handler._rpc.register_kv.assert_not_called()
        assert "42" not in handler._key_to_location

        handler.close()

    def test_store_register_race_frees_page(self):
        """L303-310: register_kv returns is_new=False (race), page is freed."""
        handler = _make_mock_handler()

        # exists_kv returns False so store proceeds past dup check
        handler._rpc.exists_kv = MagicMock(return_value=False)
        # register_kv returns False (another instance registered between check and register)
        handler._rpc.register_kv = MagicMock(return_value=False)

        result = handler.store(key="77", info=MemoryInfo(view=memoryview(b"data")))
        assert result is True
        # Key should NOT be in local map (race lost)
        assert "77" not in handler._key_to_location

        handler.close()

    def test_batch_store_server_duplicate_skip(self):
        """L606-609: batch_store skips keys that exist on server."""
        handler = _make_mock_handler()

        batch_exists_resp = MagicMock()
        # Key 1 exists on server, key 2 does not
        batch_exists_resp.results = [True, False]
        handler._rpc.batch_exists_kv = MagicMock(return_value=batch_exists_resp)

        results = handler.batch_store(
            keys=["1", "2"],
            infos=[
                MemoryInfo(view=memoryview(b"data1")),
                MemoryInfo(view=memoryview(b"data2")),
            ],
        )
        # Both should succeed (key 1 skipped as dup, key 2 stored)
        assert results == [True, True]
        # Only key 2 should be in local map
        assert "1" not in handler._key_to_location
        assert "2" in handler._key_to_location

        handler.close()

    def test_batch_store_batch_exists_rpc_failure(self):
        """L583-585: batch_exists_kv RPC fails, falls back to [False]*len."""
        handler = _make_mock_handler()

        handler._rpc.batch_exists_kv = MagicMock(side_effect=RuntimeError("RPC failed"))

        results = handler.batch_store(
            keys=["1"],
            infos=[MemoryInfo(view=memoryview(b"data"))],
        )
        # Should still succeed — fallback treats all keys as new
        assert results == [True]
        assert "1" in handler._key_to_location

        handler.close()

    def test_batch_store_some_exist_log(self):
        """L589: batch_store logs when some keys are skipped."""
        handler = _make_mock_handler()

        batch_exists_resp = MagicMock()
        # All 3 keys exist on server
        batch_exists_resp.results = [True, True, True]
        handler._rpc.batch_exists_kv = MagicMock(return_value=batch_exists_resp)

        results = handler.batch_store(
            keys=["1", "2", "3"],
            infos=[
                MemoryInfo(view=memoryview(b"a")),
                MemoryInfo(view=memoryview(b"b")),
                MemoryInfo(view=memoryview(b"c")),
            ],
        )
        # All skipped but reported as True (idempotent)
        assert results == [True, True, True]
        # None should be in local map
        assert len(handler._key_to_location) == 0

        handler.close()


class TestMaruHandlerHealthcheck:
    """Test healthcheck() method."""

    def test_healthcheck_not_connected(self):
        """L425: healthcheck returns False when not connected."""
        from maru_common import MaruConfig
        from maru_handler.handler import MaruHandler

        config = MaruConfig(auto_connect=False, use_async_rpc=False)
        handler = MaruHandler(config)
        # Not connected
        assert handler.healthcheck() is False

    def test_healthcheck_closing(self):
        """L425: healthcheck returns False when closing."""
        handler = _make_mock_handler()
        handler._closing.set()
        assert handler.healthcheck() is False
        handler._closing.clear()
        handler.close()

    def test_healthcheck_success(self):
        """L428-429: healthcheck returns True when heartbeat succeeds."""
        handler = _make_mock_handler()
        handler._rpc.heartbeat = MagicMock(return_value=True)
        assert handler.healthcheck() is True
        handler.close()

    def test_healthcheck_exception(self):
        """L430-432: healthcheck returns False when heartbeat raises."""
        handler = _make_mock_handler()
        handler._rpc.heartbeat = MagicMock(side_effect=RuntimeError("timeout"))
        assert handler.healthcheck() is False
        handler.close()


class TestMaruHandlerExpandFailure:
    """Test store behavior when expansion fails (mocked RPC)."""

    def test_store_fails_when_expansion_fails(self):
        """Pre-fill all pages, mock request_alloc to fail, verify store returns False."""
        from maru_common.protocol import RequestAllocResponse

        handler = _make_mock_handler(pool_size=2048, chunk_size=1024)

        # Fill all 2 pages in the initial region
        handler.store(key="1", info=MemoryInfo(view=memoryview(b"data1")))
        handler.store(key="2", info=MemoryInfo(view=memoryview(b"data2")))

        assert handler.allocator.num_free_pages == 0

        # Mock request_alloc to return success=False
        handler._rpc.request_alloc = MagicMock(
            return_value=RequestAllocResponse(success=False, handle=None)
        )

        # Try to store a new key — should trigger expansion and fail
        result = handler.store(key="3", info=MemoryInfo(view=memoryview(b"data3")))
        assert result is False

        handler.close()


class TestPremapSharedRegions:
    """Tests for _premap_shared_regions()."""

    def test_premap_success(self):
        """Shared regions are pre-mapped on connect when eager_map=True."""
        from maru_common import ListAllocationsResponse

        config = MaruConfig(
            pool_size=4096,
            chunk_size_bytes=1024,
            auto_connect=False,
            use_async_rpc=False,
            eager_map=True,
        )
        handler = MaruHandler(config)

        mock_rpc = MagicMock()
        mock_rpc.connect = MagicMock()

        alloc_response = MagicMock()
        alloc_response.success = True
        alloc_response.handle = _make_handle(100, 4096)
        alloc_response.mount_path = None  # DAX mode (no marufs)
        mock_rpc.request_alloc = MagicMock(return_value=alloc_response)

        # list_allocations returns 2 shared regions
        shared_h1 = _make_handle(200, 2048)
        shared_h2 = _make_handle(300, 2048)
        list_resp = ListAllocationsResponse(
            success=True, allocations=[shared_h1, shared_h2]
        )
        mock_rpc.list_allocations = MagicMock(return_value=list_resp)
        mock_rpc.close = MagicMock()

        handler._rpc = mock_rpc
        result = handler.connect()
        assert result is True

        # Shared regions should be mapped
        assert handler._mapper.get_region(200) is not None
        assert handler._mapper.get_region(300) is not None

        # list_allocations should have been called with exclude
        mock_rpc.list_allocations.assert_called_once_with(
            exclude_instance_id=config.instance_id
        )

        handler.close()

    def test_premap_list_allocations_rpc_fails(self):
        """Connection succeeds even when list_allocations RPC fails."""
        config = MaruConfig(
            pool_size=4096,
            chunk_size_bytes=1024,
            auto_connect=False,
            use_async_rpc=False,
            eager_map=True,
        )
        handler = MaruHandler(config)

        mock_rpc = MagicMock()
        mock_rpc.connect = MagicMock()

        alloc_response = MagicMock()
        alloc_response.success = True
        alloc_response.handle = _make_handle(100, 4096)
        alloc_response.mount_path = None  # DAX mode (no marufs)
        mock_rpc.request_alloc = MagicMock(return_value=alloc_response)

        # list_allocations raises an exception
        mock_rpc.list_allocations = MagicMock(side_effect=RuntimeError("RPC timeout"))
        mock_rpc.close = MagicMock()

        handler._rpc = mock_rpc
        result = handler.connect()
        # Connection should still succeed
        assert result is True
        assert handler.connected is True

        handler.close()

    def test_premap_list_allocations_returns_failure(self):
        """Connection succeeds when list_allocations returns success=False."""
        from maru_common import ListAllocationsResponse

        config = MaruConfig(
            pool_size=4096,
            chunk_size_bytes=1024,
            auto_connect=False,
            use_async_rpc=False,
            eager_map=True,
        )
        handler = MaruHandler(config)

        mock_rpc = MagicMock()
        mock_rpc.connect = MagicMock()

        alloc_response = MagicMock()
        alloc_response.success = True
        alloc_response.handle = _make_handle(100, 4096)
        alloc_response.mount_path = None  # DAX mode (no marufs)
        mock_rpc.request_alloc = MagicMock(return_value=alloc_response)

        list_resp = ListAllocationsResponse(success=False, error="internal error")
        mock_rpc.list_allocations = MagicMock(return_value=list_resp)
        mock_rpc.close = MagicMock()

        handler._rpc = mock_rpc
        result = handler.connect()
        assert result is True
        assert handler.connected is True

        handler.close()

    def test_premap_individual_map_failure_continues(self):
        """Failing to map one shared region doesn't block others."""
        from maru_common import ListAllocationsResponse

        config = MaruConfig(
            pool_size=4096,
            chunk_size_bytes=1024,
            auto_connect=False,
            use_async_rpc=False,
            eager_map=True,
        )
        handler = MaruHandler(config)

        mock_rpc = MagicMock()
        mock_rpc.connect = MagicMock()

        alloc_response = MagicMock()
        alloc_response.success = True
        alloc_response.handle = _make_handle(100, 4096)
        alloc_response.mount_path = None  # DAX mode (no marufs)
        mock_rpc.request_alloc = MagicMock(return_value=alloc_response)

        shared_h1 = _make_handle(200, 2048)
        shared_h2 = _make_handle(300, 2048)
        list_resp = ListAllocationsResponse(
            success=True, allocations=[shared_h1, shared_h2]
        )
        mock_rpc.list_allocations = MagicMock(return_value=list_resp)
        mock_rpc.close = MagicMock()

        handler._rpc = mock_rpc

        # Pre-initialize _mapper before connect() so we can patch map_region
        # before the eager_map premap phase runs inside connect().
        from maru_handler.memory import DaxMapper

        handler._mapper = DaxMapper()
        original_map = handler._mapper.map_region

        def selective_fail(handle, prefault=True):
            if handle.region_id == 200:
                raise RuntimeError("mmap failed")
            return original_map(handle, prefault=prefault)

        handler._mapper.map_region = selective_fail

        result = handler.connect()
        assert result is True

        # Region 200 failed but 300 should be mapped
        assert handler._mapper.get_region(200) is None
        assert handler._mapper.get_region(300) is not None

        handler.close()


# =============================================================================
# alloc() / free() / store(handle=) zero-copy API tests
# =============================================================================


class TestAlloc:
    """Tests for MaruHandler.alloc() zero-copy allocation."""

    def test_alloc_returns_alloc_handle(self):
        """alloc() returns AllocHandle with writable memoryview."""
        from maru_handler.memory import AllocHandle

        handler = _make_mock_handler()
        handle = handler.alloc(size=512)

        assert isinstance(handle, AllocHandle)
        assert isinstance(handle.buf, memoryview)
        assert not handle.buf.readonly
        assert len(handle.buf) >= 512
        handler.close()

    def test_alloc_buf_is_writable(self):
        """Returned buf supports direct writes."""
        handler = _make_mock_handler()
        handle = handler.alloc(size=64)

        handle.buf[:5] = b"hello"
        assert bytes(handle.buf[:5]) == b"hello"
        handler.close()

    def test_alloc_exceeds_chunk_size_raises(self):
        """Requesting size > chunk_size raises ValueError."""
        handler = _make_mock_handler(chunk_size=64)

        with pytest.raises(ValueError, match="exceeds chunk_size"):
            handler.alloc(size=128)
        handler.close()

    def test_alloc_pool_exhausted_raises(self):
        """All pages consumed raises ValueError when expansion also fails."""
        handler = _make_mock_handler(pool_size=1024, chunk_size=1024)
        handler.alloc(size=512)  # only page

        # Make expansion fail so pool stays exhausted
        fail_response = MagicMock()
        fail_response.success = False
        fail_response.handle = None
        handler._rpc.request_alloc = MagicMock(return_value=fail_response)

        with pytest.raises(ValueError, match="pool exhausted"):
            handler.alloc(size=512)
        handler.close()

    def test_alloc_before_connect_raises(self):
        """alloc() before connect() raises RuntimeError."""
        config = MaruConfig(auto_connect=False)
        handler = MaruHandler(config)

        with pytest.raises(RuntimeError):
            handler.alloc(size=64)

    def test_alloc_while_closing_raises(self):
        """alloc() during closing raises RuntimeError."""
        handler = _make_mock_handler()
        handler._closing.set()

        with pytest.raises(RuntimeError, match="closing"):
            handler.alloc(size=64)
        handler._closing.clear()
        handler.close()

    def test_multiple_alloc_independent(self):
        """Multiple alloc() return distinct, independently writable regions."""
        handler = _make_mock_handler(pool_size=4096, chunk_size=1024)
        h1 = handler.alloc(size=100)
        h2 = handler.alloc(size=100)

        assert h1._page_index != h2._page_index or h1._region_id != h2._region_id
        h1.buf[:3] = b"aaa"
        h2.buf[:3] = b"bbb"
        assert bytes(h1.buf[:3]) == b"aaa"
        assert bytes(h2.buf[:3]) == b"bbb"
        handler.close()


class TestFree:
    """Tests for MaruHandler.free() page reclamation."""

    def test_free_before_store(self):
        """free() before store() reclaims the page."""
        handler = _make_mock_handler()
        handle = handler.alloc(size=64)
        handler.free(handle)
        handler.close()

    def test_free_after_store(self):
        """free() after store removes key from _key_to_location."""
        handler = _make_mock_handler()
        handle = handler.alloc(size=64)
        handle.buf[:5] = b"hello"
        handler.store(key="42", handle=handle)
        assert "42" in handler._key_to_location

        handler.free(handle)
        assert "42" not in handler._key_to_location
        handler.close()

    def test_free_reclaims_page(self):
        """Freed page can be re-allocated."""
        handler = _make_mock_handler(pool_size=1024, chunk_size=1024)
        h1 = handler.alloc(size=64)
        handler.free(h1)

        h2 = handler.alloc(size=64)
        assert h2 is not None
        handler.close()

    def test_double_free_raises(self):
        """Freeing the same handle twice raises."""
        handler = _make_mock_handler()
        handle = handler.alloc(size=64)
        handler.free(handle)

        with pytest.raises((ValueError, KeyError)):
            handler.free(handle)
        handler.close()


class TestStoreWithHandle:
    """Tests for store() zero-copy path via handle parameter."""

    def test_store_with_handle_happy_path(self):
        """alloc -> write -> store(handle=) full flow."""
        handler = _make_mock_handler()
        handle = handler.alloc(size=64)
        handle.buf[:5] = b"hello"

        result = handler.store(key="42", handle=handle)
        assert result is True
        assert "42" in handler._key_to_location

        handler._rpc.register_kv.assert_called_once()
        handler.close()

    def test_store_with_handle_no_memcpy(self):
        """handle path does not call _gil_free_memcpy."""
        from unittest.mock import patch as mock_patch

        handler = _make_mock_handler()
        handle = handler.alloc(size=64)
        handle.buf[:5] = b"hello"

        with mock_patch("maru_handler.handler._gil_free_memcpy") as mock_memcpy:
            handler.store(key="42", handle=handle)
            mock_memcpy.assert_not_called()
        handler.close()

    def test_store_with_handle_duplicate_key_skips(self):
        """store(handle=) skips when key already exists."""
        handler = _make_mock_handler()

        handler.store(key="42", data=memoryview(b"first"))

        handle = handler.alloc(size=64)
        result = handler.store(key="42", handle=handle)
        assert result is True
        assert handler._rpc.register_kv.call_count == 1
        handler.close()

    def test_store_with_handle_register_race(self):
        """handle path frees page when register_kv returns is_new=False."""
        handler = _make_mock_handler()
        handler._rpc.register_kv = MagicMock(return_value=False)

        handle = handler.alloc(size=64)
        handle.buf[:5] = b"hello"

        result = handler.store(key="42", handle=handle)
        assert result is True
        assert "42" not in handler._key_to_location
        handler.close()

    def test_store_with_handle_and_data_raises(self):
        """Providing both handle and data raises ValueError."""
        handler = _make_mock_handler()
        handle = handler.alloc(size=64)

        with pytest.raises(ValueError, match="Cannot specify both"):
            handler.store(key="42", handle=handle, data=memoryview(b"conflict"))
        handler.close()

    def test_store_with_handle_and_info_raises(self):
        """Providing both handle and info raises ValueError."""
        handler = _make_mock_handler()
        handle = handler.alloc(size=64)

        with pytest.raises(ValueError, match="Cannot specify both"):
            handler.store(
                key="42", handle=handle, info=MemoryInfo(view=memoryview(b"x"))
            )
        handler.close()


class TestStoreWithHandleCompat:
    """Ensure store() without handle remains unaffected."""

    def test_store_without_handle(self):
        """store() without handle uses allocate+memcpy path."""
        handler = _make_mock_handler()
        result = handler.store(key="42", data=memoryview(b"hello"))
        assert result is True
        assert "42" in handler._key_to_location
        handler._rpc.register_kv.assert_called_once()
        handler.close()

    def test_store_with_prefix(self):
        """store() with prefix still works without handle."""
        handler = _make_mock_handler()
        result = handler.store(
            key="42",
            info=MemoryInfo(view=memoryview(b"data")),
            prefix=b"\x01\x02",
        )
        assert result is True
        handler.close()

    def test_mixed_store_modes(self):
        """Interleaving store with and without handle works correctly."""
        handler = _make_mock_handler(pool_size=8192, chunk_size=1024)

        handler.store(key="1", data=memoryview(b"data1"))
        assert "1" in handler._key_to_location

        h = handler.alloc(size=64)
        h.buf[:6] = b"handle"
        handler.store(key="2", handle=h)
        assert "2" in handler._key_to_location

        handler.store(key="3", data=memoryview(b"data2"))
        assert "3" in handler._key_to_location

        assert handler._rpc.register_kv.call_count == 3
        handler.close()


class TestAllocCleanup:
    """Tests for pending alloc cleanup on close()."""

    def test_close_after_alloc_without_store(self):
        """close() succeeds even if alloc'd pages were never store'd."""
        handler = _make_mock_handler()
        handler.alloc(size=64)
        handler.alloc(size=64)
        handler.close()

    def test_retrieve_after_store_with_handle(self):
        """alloc -> store(handle=) -> retrieve returns data from same mmap region."""
        handler = _make_mock_handler()
        handle = handler.alloc(size=64)
        handle.buf[:4] = b"hell"
        handler.store(key="42", handle=handle)

        # Mock lookup_kv returns kv_length=4, so retrieve returns 4 bytes
        result = handler.retrieve(key="42")
        assert result is not None
        assert bytes(result.view[:4]) == b"hell"
        handler.close()


class TestAllocThreadSafety:
    """Thread-safety tests for alloc/store zero-copy path."""

    def test_concurrent_alloc(self):
        """Concurrent alloc() calls return distinct handles."""
        import threading

        handler = _make_mock_handler(pool_size=8192, chunk_size=1024)
        handles = []
        errors = []

        def alloc_one():
            try:
                h = handler.alloc(size=64)
                handles.append(h)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=alloc_one) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(handles) == 4
        locations = [(h._region_id, h._page_index) for h in handles]
        assert len(set(locations)) == 4
        handler.close()

    def test_concurrent_alloc_and_store(self):
        """Concurrent alloc+store(handle=) complete without races."""
        import threading

        handler = _make_mock_handler(pool_size=8192, chunk_size=1024)

        def alloc_and_store(key):
            h = handler.alloc(size=64)
            h.buf[:3] = b"abc"
            handler.store(key=key, handle=h)

        threads = [
            threading.Thread(target=alloc_and_store, args=(i,)) for i in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        for i in range(4):
            assert i in handler._key_to_location
        handler.close()
