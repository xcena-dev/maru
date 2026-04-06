# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tests for AllocationManager."""

from maru_server.allocation_manager import AllocationManager


class TestAllocationManager:
    """Test cases for AllocationManager."""

    def test_allocate(self):
        """Test basic allocation."""
        manager = AllocationManager()
        handle = manager.allocate("instance1", 4096)

        assert handle is not None
        assert handle.length == 4096
        assert handle.region_id is not None

    def test_allocate_multiple(self):
        """Test allocating multiple regions."""
        manager = AllocationManager()
        h1 = manager.allocate("instance1", 1024)
        h2 = manager.allocate("instance2", 2048)

        assert h1 is not None
        assert h2 is not None
        assert h1.region_id != h2.region_id

    def test_get_handle(self):
        """Test getting handle by region_id."""
        manager = AllocationManager()
        handle = manager.allocate("instance1", 4096)

        result = manager.get_handle(handle.region_id)
        assert result is not None
        assert result.region_id == handle.region_id
        assert result.length == 4096

    def test_get_handle_nonexistent(self):
        """Test getting nonexistent handle."""
        manager = AllocationManager()
        result = manager.get_handle(99999)
        assert result is None

    def test_increment_kv_ref(self):
        """Test incrementing KV reference count."""
        manager = AllocationManager()
        handle = manager.allocate("instance1", 4096)

        assert manager.increment_kv_ref(handle.region_id) is True
        assert manager.increment_kv_ref(handle.region_id) is True

    def test_increment_kv_ref_nonexistent(self):
        """Test incrementing ref for nonexistent allocation."""
        manager = AllocationManager()
        assert manager.increment_kv_ref(99999) is False

    def test_decrement_kv_ref(self):
        """Test decrementing KV reference count."""
        manager = AllocationManager()
        handle = manager.allocate("instance1", 4096)
        manager.increment_kv_ref(handle.region_id)

        assert manager.decrement_kv_ref(handle.region_id) is True
        # Allocation still exists (owner connected)
        assert manager.get_handle(handle.region_id) is not None

    def test_release(self):
        """Test releasing allocation."""
        manager = AllocationManager()
        handle = manager.allocate("instance1", 4096)

        success = manager.release("instance1", handle.region_id)
        assert success is True
        # Allocation freed (no KV refs)
        assert manager.get_handle(handle.region_id) is None

    def test_release_wrong_owner(self):
        """Test releasing allocation with wrong owner."""
        manager = AllocationManager()
        handle = manager.allocate("instance1", 4096)

        success = manager.release("instance2", handle.region_id)
        assert success is False
        # Allocation still exists
        assert manager.get_handle(handle.region_id) is not None

    def test_release_with_kv_refs(self):
        """Test releasing allocation with active KV references."""
        manager = AllocationManager()
        handle = manager.allocate("instance1", 4096)
        manager.increment_kv_ref(handle.region_id)

        success = manager.release("instance1", handle.region_id)
        assert success is True
        # Allocation still exists (KV refs > 0)
        assert manager.get_handle(handle.region_id) is not None

    def test_deferred_free_after_release_and_kv_deref(self):
        """Test that allocation is freed after owner disconnects and KV refs reach 0."""
        manager = AllocationManager()
        handle = manager.allocate("instance1", 4096)
        manager.increment_kv_ref(handle.region_id)

        # Owner disconnects
        manager.release("instance1", handle.region_id)
        assert manager.get_handle(handle.region_id) is not None

        # KV ref decremented to 0
        manager.decrement_kv_ref(handle.region_id)
        assert manager.get_handle(handle.region_id) is None

    def test_disconnect_client(self):
        """Test disconnecting a client."""
        manager = AllocationManager()
        h1 = manager.allocate("instance1", 1024)
        h2 = manager.allocate("instance1", 2048)
        h3 = manager.allocate("instance2", 4096)

        manager.disconnect_client("instance1")

        # instance1's allocations freed (no KV refs)
        assert manager.get_handle(h1.region_id) is None
        assert manager.get_handle(h2.region_id) is None
        # instance2's allocation still exists
        assert manager.get_handle(h3.region_id) is not None

    def test_disconnect_client_with_kv_refs(self):
        """Test disconnecting a client with active KV refs."""
        manager = AllocationManager()
        handle = manager.allocate("instance1", 4096)
        manager.increment_kv_ref(handle.region_id)

        manager.disconnect_client("instance1")

        # Allocation still exists (KV ref > 0)
        assert manager.get_handle(handle.region_id) is not None

    def test_get_stats(self):
        """Test getting statistics."""
        manager = AllocationManager()
        manager.allocate("instance1", 1024)
        manager.allocate("instance2", 2048)

        stats = manager.get_stats()
        assert stats["num_allocations"] == 2
        assert stats["total_allocated"] == 1024 + 2048
        assert stats["active_clients"] == 2

    def test_get_stats_after_release(self):
        """Test stats after releasing allocation."""
        manager = AllocationManager()
        handle = manager.allocate("instance1", 1024)
        manager.release("instance1", handle.region_id)

        stats = manager.get_stats()
        assert stats["num_allocations"] == 0
        assert stats["total_allocated"] == 0


class TestAllocationManagerEdgeCases:
    """Test edge cases and error paths for AllocationManager."""

    def test_allocate_returns_none(self):
        """Test allocate when _client.alloc() returns None."""
        manager = AllocationManager()

        # Mock the client to return None
        original_alloc = manager._client.alloc
        manager._client.alloc = lambda size, pool_path="": None  # type: ignore

        result = manager.allocate("instance1", 4096)
        assert result is None

        # Restore original
        manager._client.alloc = original_alloc  # type: ignore

    def test_decrement_kv_ref_nonexistent(self):
        """Test decrement_kv_ref when region_id not in allocations."""
        manager = AllocationManager()
        result = manager.decrement_kv_ref(99999)
        assert result is False

    def test_decrement_kv_ref_with_zero_count(self):
        """Test decrement_kv_ref when ref_count is already 0 (warning path)."""
        manager = AllocationManager()
        handle = manager.allocate("instance1", 4096)
        assert handle is not None

        # Don't increment, try to decrement from 0
        result = manager.decrement_kv_ref(handle.region_id)
        assert result is False

        # Allocation should still exist (owner connected)
        assert manager.get_handle(handle.region_id) is not None

    def test_decrement_kv_ref_with_negative_count(self):
        """Test decrement_kv_ref when ref_count is negative (warning path)."""
        manager = AllocationManager()
        handle = manager.allocate("instance1", 4096)
        assert handle is not None

        # Manually set ref_count to negative (simulating a bug scenario)
        with manager._lock:
            manager._allocations[handle.region_id].kv_ref_count = -1

        result = manager.decrement_kv_ref(handle.region_id)
        assert result is False

    def test_release_nonexistent_region(self):
        """Test release when region_id not found."""
        manager = AllocationManager()
        result = manager.release("instance1", 99999)
        assert result is False


class TestListAllocations:
    """Test cases for list_allocations()."""

    def test_list_all(self):
        """list_allocations returns all active allocations."""
        manager = AllocationManager()
        h1 = manager.allocate("inst1", 1024)
        h2 = manager.allocate("inst2", 2048)

        handles = manager.list_allocations()
        region_ids = {h.region_id for h in handles}
        assert h1.region_id in region_ids
        assert h2.region_id in region_ids
        assert len(handles) == 2

    def test_list_exclude_instance(self):
        """list_allocations excludes caller's own allocations."""
        manager = AllocationManager()
        h1 = manager.allocate("inst1", 1024)
        h2 = manager.allocate("inst2", 2048)

        handles = manager.list_allocations(exclude_instance_id="inst1")
        region_ids = {h.region_id for h in handles}
        assert h1.region_id not in region_ids
        assert h2.region_id in region_ids
        assert len(handles) == 1

    def test_list_excludes_disconnected_owner(self):
        """list_allocations filters out regions whose owner disconnected."""
        manager = AllocationManager()
        h1 = manager.allocate("inst1", 1024)
        h2 = manager.allocate("inst2", 2048)

        # inst2 has KV refs so region survives disconnect
        manager.increment_kv_ref(h2.region_id)
        manager.disconnect_client("inst2")

        handles = manager.list_allocations()
        region_ids = {h.region_id for h in handles}
        # inst1 still connected
        assert h1.region_id in region_ids
        # inst2 disconnected — should be filtered out
        assert h2.region_id not in region_ids

    def test_list_empty(self):
        """list_allocations returns empty list when no allocations."""
        manager = AllocationManager()
        handles = manager.list_allocations()
        assert handles == []

    def test_list_exclude_nonexistent_instance(self):
        """Excluding a non-existent instance returns all allocations."""
        manager = AllocationManager()
        manager.allocate("inst1", 1024)

        handles = manager.list_allocations(exclude_instance_id="no-such-inst")
        assert len(handles) == 1


class TestAllocationManagerStatsEdgeCases:
    """Additional stats and lifecycle edge cases."""

    def test_get_stats_with_disconnected_owner(self):
        """active_clients excludes disconnected owners."""
        manager = AllocationManager()
        manager.allocate("inst1", 1024)
        h2 = manager.allocate("inst2", 2048)
        manager.increment_kv_ref(h2.region_id)

        manager.disconnect_client("inst2")

        stats = manager.get_stats()
        assert stats["num_allocations"] == 2  # h1 + h2 (kv ref kept h2 alive)
        assert stats["total_allocated"] == 1024 + 2048
        assert stats["active_clients"] == 1  # Only inst1 is active

    def test_get_stats_empty(self):
        """Stats on empty manager."""
        manager = AllocationManager()
        stats = manager.get_stats()
        assert stats["num_allocations"] == 0
        assert stats["total_allocated"] == 0
        assert stats["active_clients"] == 0

    def test_disconnect_nonexistent_client(self):
        """disconnect_client with unknown instance_id is a no-op."""
        manager = AllocationManager()
        h = manager.allocate("inst1", 1024)
        manager.disconnect_client("no-such-inst")
        # inst1's allocation should be untouched
        assert manager.get_handle(h.region_id) is not None

    def test_multiple_kv_ref_increments_and_decrements(self):
        """Multiple increment/decrement cycles before owner disconnect."""
        manager = AllocationManager()
        h = manager.allocate("inst1", 4096)

        # Increment 3 times
        manager.increment_kv_ref(h.region_id)
        manager.increment_kv_ref(h.region_id)
        manager.increment_kv_ref(h.region_id)

        # Owner disconnects
        manager.release("inst1", h.region_id)
        # Still alive (kv_ref_count = 3)
        assert manager.get_handle(h.region_id) is not None

        # Decrement to 1
        manager.decrement_kv_ref(h.region_id)
        manager.decrement_kv_ref(h.region_id)
        assert manager.get_handle(h.region_id) is not None

        # Final decrement triggers free
        manager.decrement_kv_ref(h.region_id)
        assert manager.get_handle(h.region_id) is None
