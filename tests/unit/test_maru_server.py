# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for MaruServer orchestration logic."""

from maru_server.server import MaruServer


class TestMaruBasic:
    """Test basic MaruServer operations (CRUD)."""

    def test_request_alloc(self):
        """Test allocation request."""
        server = MaruServer()
        handle = server.request_alloc("instance1", 4096)

        assert handle is not None
        assert handle.length == 4096

    def test_return_alloc(self):
        """Test allocation return."""
        server = MaruServer()
        handle = server.request_alloc("instance1", 4096)

        success = server.return_alloc("instance1", handle.region_id)
        assert success is True

    def test_register_and_lookup_kv(self):
        """Test KV registration and lookup."""
        server = MaruServer()
        handle = server.request_alloc("instance1", 4096)

        is_new = server.register_kv(
            key=12345, region_id=handle.region_id, kv_offset=0, kv_length=1024
        )
        assert is_new is True

        result = server.lookup_kv(12345)
        assert result is not None
        assert result["handle"].region_id == handle.region_id
        assert result["kv_offset"] == 0
        assert result["kv_length"] == 1024

    def test_lookup_nonexistent(self):
        """Test lookup of nonexistent key."""
        server = MaruServer()
        result = server.lookup_kv(99999)
        assert result is None

    def test_exists_kv(self):
        """Test KV existence check."""
        server = MaruServer()
        handle = server.request_alloc("instance1", 4096)

        assert server.exists_kv(123) is False

        server.register_kv(
            key=123, region_id=handle.region_id, kv_offset=0, kv_length=100
        )
        assert server.exists_kv(123) is True

    def test_delete_kv(self):
        """Test KV deletion."""
        server = MaruServer()
        handle = server.request_alloc("instance1", 4096)

        server.register_kv(
            key=123, region_id=handle.region_id, kv_offset=0, kv_length=100
        )
        assert server.exists_kv(123) is True

        existed = server.delete_kv(123)
        assert existed is True
        assert server.exists_kv(123) is False

    def test_get_stats(self):
        """Test getting server statistics."""
        server = MaruServer()
        handle = server.request_alloc("instance1", 4096)
        server.register_kv(
            key=1, region_id=handle.region_id, kv_offset=0, kv_length=100
        )

        stats = server.get_stats()
        assert "kv_manager" in stats
        assert "allocation_manager" in stats
        assert stats["kv_manager"]["total_entries"] == 1
        assert stats["allocation_manager"]["num_allocations"] == 1


class TestClientDisconnected:
    """Test cases for MaruServer.client_disconnected()."""

    def test_client_disconnected_with_kv_refs(self):
        """
        Client disconnection with KV references should keep allocation until KV deleted.

        Current behavior: allocations with kv_ref_count > 0 are kept even after
        client disconnects, because KV entries still reference them.
        """
        server = MaruServer()

        # 1. Allocate a region for instance-1
        handle = server.request_alloc("instance-1", 4096)
        assert handle is not None
        region_id = handle.region_id

        # 2. Register KV entries on that region (increments kv_ref_count)
        server.register_kv(key=100, region_id=region_id, kv_offset=0, kv_length=256)
        server.register_kv(key=200, region_id=region_id, kv_offset=256, kv_length=512)

        # Verify KV entries exist
        assert server.exists_kv(100) is True
        assert server.exists_kv(200) is True

        stats_before = server.get_stats()
        assert stats_before["allocation_manager"]["num_allocations"] == 1

        # 3. Call client_disconnected
        server.client_disconnected("instance-1")

        # 4. Allocation should remain because kv_ref_count > 0
        stats_after = server.get_stats()
        assert stats_after["allocation_manager"]["num_allocations"] == 1

        # KV entries should still exist (not auto-deleted on disconnect)
        assert server.exists_kv(100) is True
        assert server.exists_kv(200) is True

        # 5. Delete KV entries - this should free the allocation
        server.delete_kv(100)
        server.delete_kv(200)

        stats_final = server.get_stats()
        assert stats_final["allocation_manager"]["num_allocations"] == 0
        assert stats_final["kv_manager"]["total_entries"] == 0

    def test_client_disconnected_no_kv_refs(self):
        """
        Client disconnection without KV references should immediately free allocation.
        """
        server = MaruServer()

        # Allocate a region but don't register any KV entries
        handle = server.request_alloc("instance-1", 4096)
        assert handle is not None

        stats_before = server.get_stats()
        assert stats_before["allocation_manager"]["num_allocations"] == 1

        # Disconnect client - allocation should be freed immediately (kv_ref_count == 0)
        server.client_disconnected("instance-1")

        stats_after = server.get_stats()
        assert stats_after["allocation_manager"]["num_allocations"] == 0

    def test_client_disconnected_unknown_instance(self):
        """Disconnecting unknown instance should not raise (graceful no-op)."""
        server = MaruServer()

        # Should not raise
        server.client_disconnected("nonexistent")

        # Verify no side effects
        stats = server.get_stats()
        assert stats["allocation_manager"]["num_allocations"] == 0
        assert stats["kv_manager"]["total_entries"] == 0

    def test_client_disconnected_multiple_instances(self):
        """With multiple instances, only disconnecting instance's allocations are affected."""
        server = MaruServer()

        # 1. Two instances allocate
        handle1 = server.request_alloc("instance-1", 4096)
        handle2 = server.request_alloc("instance-2", 4096)
        assert handle1 is not None
        assert handle2 is not None

        region_id1 = handle1.region_id
        region_id2 = handle2.region_id

        # 2. Register KV from both on their regions
        server.register_kv(key=100, region_id=region_id1, kv_offset=0, kv_length=256)
        server.register_kv(key=200, region_id=region_id1, kv_offset=256, kv_length=512)
        server.register_kv(key=300, region_id=region_id2, kv_offset=0, kv_length=256)
        server.register_kv(key=400, region_id=region_id2, kv_offset=256, kv_length=512)

        stats_before = server.get_stats()
        assert stats_before["allocation_manager"]["num_allocations"] == 2
        assert stats_before["kv_manager"]["total_entries"] == 4

        # 3. Disconnect one instance
        server.client_disconnected("instance-1")

        # 4. Instance-1's allocation should remain (has KV refs)
        # Instance-2's allocation should remain (still connected)
        stats_after = server.get_stats()
        assert stats_after["allocation_manager"]["num_allocations"] == 2

        # All KV entries should still exist
        assert server.exists_kv(100) is True
        assert server.exists_kv(200) is True
        assert server.exists_kv(300) is True
        assert server.exists_kv(400) is True

        # 5. Delete instance-1's KV entries
        server.delete_kv(100)
        server.delete_kv(200)

        # Now instance-1's allocation should be freed
        stats_final = server.get_stats()
        assert stats_final["allocation_manager"]["num_allocations"] == 1
        assert stats_final["kv_manager"]["total_entries"] == 2

        # Instance-2's data should remain intact
        lookup_result = server.lookup_kv(300)
        assert lookup_result is not None
        assert lookup_result["handle"].region_id == region_id2


class TestMaruBatch:
    """Test batch operations on MaruServer directly."""

    def test_batch_register_kv(self):
        """Test batch_register_kv registers multiple entries."""
        server = MaruServer()
        handle = server.request_alloc("instance1", 4096)

        entries = [
            (1, handle.region_id, 0, 100),
            (2, handle.region_id, 100, 200),
            (3, handle.region_id, 300, 150),
        ]
        results = server.batch_register_kv(entries)

        assert results == [True, True, True]
        assert server.exists_kv(1) is True
        assert server.exists_kv(2) is True
        assert server.exists_kv(3) is True

    def test_batch_register_kv_with_duplicate(self):
        """Test batch_register_kv with duplicate key returns False for duplicate."""
        server = MaruServer()
        handle = server.request_alloc("instance1", 4096)

        server.register_kv(
            key=1, region_id=handle.region_id, kv_offset=0, kv_length=100
        )

        entries = [
            (1, handle.region_id, 0, 100),  # duplicate
            (2, handle.region_id, 100, 200),  # new
        ]
        results = server.batch_register_kv(entries)

        assert results == [False, True]

    def test_batch_exists_kv(self):
        """Test batch_exists_kv returns correct existence flags."""
        server = MaruServer()
        handle = server.request_alloc("instance1", 4096)

        server.register_kv(
            key=1, region_id=handle.region_id, kv_offset=0, kv_length=100
        )
        server.register_kv(
            key=3, region_id=handle.region_id, kv_offset=100, kv_length=100
        )

        results = server.batch_exists_kv([1, 2, 3, 4])

        assert results == [True, False, True, False]

    def test_batch_exists_kv_empty(self):
        """Test batch_exists_kv with empty keys list."""
        server = MaruServer()
        results = server.batch_exists_kv([])
        assert results == []


class TestMaruServerEdgeCases:
    """Test edge cases and error paths for MaruServer."""

    def test_request_alloc_returns_none(self):
        """Test request_alloc when allocation fails (warning path)."""
        server = MaruServer()

        # Mock the allocation manager to return None
        original_alloc = server._allocation_manager.allocate
        server._allocation_manager.allocate = lambda instance_id, size: None  # type: ignore

        result = server.request_alloc("instance1", 4096)
        assert result is None

        # Restore original
        server._allocation_manager.allocate = original_alloc  # type: ignore

    def test_lookup_kv_nonexistent_key(self):
        """Test lookup_kv when key doesn't exist (entry is None)."""
        server = MaruServer()

        # Lookup non-existent key
        result = server.lookup_kv(99999)
        assert result is None

    def test_lookup_kv_when_handle_is_none(self):
        """Test lookup_kv when get_handle returns None."""
        server = MaruServer()

        # Allocate and register a KV
        handle = server.request_alloc("instance1", 4096)
        assert handle is not None
        region_id = handle.region_id

        server.register_kv(key=100, region_id=region_id, kv_offset=0, kv_length=256)

        # Manually delete the allocation (simulating orphaned KV entry)
        with server._allocation_manager._lock:
            del server._allocation_manager._allocations[region_id]

        # Lookup should return None when handle is missing
        result = server.lookup_kv(100)
        assert result is None

    def test_batch_lookup_kv_with_none_handles(self):
        """Test batch_lookup_kv when some handles are None."""
        server = MaruServer()

        # Allocate and register KVs
        handle1 = server.request_alloc("instance1", 4096)
        handle2 = server.request_alloc("instance2", 4096)
        assert handle1 is not None
        assert handle2 is not None

        region_id1 = handle1.region_id
        region_id2 = handle2.region_id

        server.register_kv(key=100, region_id=region_id1, kv_offset=0, kv_length=256)
        server.register_kv(key=200, region_id=region_id2, kv_offset=256, kv_length=512)

        # Manually delete one allocation (simulating orphaned KV entry)
        with server._allocation_manager._lock:
            del server._allocation_manager._allocations[region_id1]

        # Batch lookup with mixed results
        results = server.batch_lookup_kv([100, 200, 300])

        assert len(results) == 3
        assert results[0] is None  # Handle is None
        assert results[1] is not None  # Valid
        assert results[1]["handle"].region_id == region_id2
        assert results[2] is None  # Key doesn't exist
