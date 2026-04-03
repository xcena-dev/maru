# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tests for KV Manager."""

from maru_server.kv_manager import DeleteResult, KVManager


class TestKVManager:
    """Test cases for KVManager."""

    def test_register_new_key(self):
        """Test registering a new KV entry."""
        manager = KVManager()
        is_new, region_id = manager.register(
            key="123", region_id=1, kv_offset=0, kv_length=1024
        )
        assert is_new is True
        assert region_id == 1
        assert manager.exists("123") is True

    def test_register_existing_key(self):
        """Test registering an existing key is idempotent — returns (False, None)."""
        manager = KVManager()
        manager.register(key="123", region_id=1, kv_offset=0, kv_length=1024)
        is_new, region_id = manager.register(
            key="123", region_id=1, kv_offset=0, kv_length=1024
        )
        assert is_new is False
        assert region_id is None  # No new allocation ref needed

    def test_lookup_existing_key(self):
        """Test looking up an existing key."""
        manager = KVManager()
        manager.register(key="456", region_id=2, kv_offset=1024, kv_length=2048)

        entry = manager.lookup("456")
        assert entry is not None
        assert entry.region_id == 2
        assert entry.kv_offset == 1024
        assert entry.kv_length == 2048

    def test_lookup_nonexistent_key(self):
        """Test looking up a nonexistent key."""
        manager = KVManager()
        entry = manager.lookup("999")
        assert entry is None

    def test_exists(self):
        """Test exists check."""
        manager = KVManager()
        assert manager.exists("123") is False

        manager.register(key="123", region_id=1, kv_offset=0, kv_length=1024)
        assert manager.exists("123") is True

    def test_delete_single_ref(self):
        """Test deleting a key with single reference."""
        manager = KVManager()
        manager.register(key="123", region_id=1, kv_offset=0, kv_length=1024)

        result, region_id = manager.delete("123")
        assert result == DeleteResult.DELETED
        assert region_id == 1  # Need to decrement alloc ref
        assert manager.exists("123") is False

    def test_delete_multiple_refs(self):
        """Test that second register is idempotent and delete removes entry entirely."""
        manager = KVManager()
        manager.register(key="123", region_id=1, kv_offset=0, kv_length=1024)
        # Second register is idempotent — returns (False, None), no ref count
        is_new, rid = manager.register(
            key="123", region_id=1, kv_offset=0, kv_length=1024
        )
        assert is_new is False
        assert rid is None

        # Delete removes entry entirely on first call
        result, region_id = manager.delete("123")
        assert result == DeleteResult.DELETED
        assert region_id == 1
        assert manager.exists("123") is False

        # Second delete on now-missing key returns NOT_FOUND
        result, region_id = manager.delete("123")
        assert result == DeleteResult.NOT_FOUND
        assert region_id is None

    def test_delete_nonexistent(self):
        """Test deleting a nonexistent key."""
        manager = KVManager()
        result, region_id = manager.delete("999")
        assert result == DeleteResult.NOT_FOUND
        assert region_id is None

    def test_get_stats(self):
        """Test getting statistics."""
        manager = KVManager()
        manager.register(key="1", region_id=1, kv_offset=0, kv_length=1000)
        manager.register(key="2", region_id=1, kv_offset=1000, kv_length=2000)

        stats = manager.get_stats()
        assert stats["total_entries"] == 2
        assert stats["total_size"] == 3000


class TestKVManagerBatch:
    """Test cases for KVManager batch operations."""

    def test_batch_register_new_keys(self):
        """Register 3 new keys via batch_register. Verify all return is_new=True and can be looked up."""
        manager = KVManager()
        entries = [
            ("100", 1, 0, 256),
            ("200", 1, 256, 512),
            ("300", 2, 0, 1024),
        ]

        results = manager.batch_register(entries)
        assert results == [True, True, True]

        # Verify all can be looked up
        for key, region_id, kv_offset, kv_length in entries:
            entry = manager.lookup(key)
            assert entry is not None
            assert entry.region_id == region_id
            assert entry.kv_offset == kv_offset
            assert entry.kv_length == kv_length

    def test_batch_register_existing_key(self):
        """Register a key, then batch_register with that key + a new one. Verify is_new flags are correct."""
        manager = KVManager()

        # Register first key
        manager.register(key="100", region_id=1, kv_offset=0, kv_length=256)

        # Batch register with existing key + new key
        entries = [
            ("100", 1, 0, 256),  # existing
            ("200", 1, 256, 512),  # new
        ]

        results = manager.batch_register(entries)
        assert results == [False, True]

        # Verify existing key is still present (idempotent, no ref_count)
        entry = manager.lookup("100")
        assert entry is not None
        assert entry.region_id == 1

        # Verify new key registered
        entry = manager.lookup("200")
        assert entry is not None

    def test_batch_lookup_mixed(self):
        """Register some keys, then batch_lookup a mix of existing and non-existing keys. Verify found flags."""
        manager = KVManager()

        # Register some keys
        manager.register(key="100", region_id=1, kv_offset=0, kv_length=256)
        manager.register(key="300", region_id=2, kv_offset=512, kv_length=1024)

        # Batch lookup: existing, missing, existing, missing
        keys = ["100", "200", "300", "400"]
        results = manager.batch_lookup(keys)

        assert len(results) == 4
        assert results[0] is not None
        assert results[0].region_id == 1
        assert results[1] is None
        assert results[2] is not None
        assert results[2].region_id == 2
        assert results[3] is None

    def test_batch_lookup_all_missing(self):
        """batch_lookup with keys that don't exist. Verify all found=False."""
        manager = KVManager()

        keys = ["100", "200", "300"]
        results = manager.batch_lookup(keys)

        assert len(results) == 3
        assert all(r is None for r in results)

    def test_batch_exists_mixed(self):
        """Register some keys, batch_exists with mixed keys. Verify boolean results."""
        manager = KVManager()

        # Register some keys
        manager.register(key="100", region_id=1, kv_offset=0, kv_length=256)
        manager.register(key="300", region_id=2, kv_offset=512, kv_length=1024)

        # Batch exists: existing, missing, existing, missing
        keys = ["100", "200", "300", "400"]
        results = manager.batch_exists(keys)

        assert results == [True, False, True, False]

    def test_batch_exists_empty_list(self):
        """batch_exists([]) should return empty list."""
        manager = KVManager()

        results = manager.batch_exists([])
        assert results == []


class TestKVManagerEdgeCases:
    """Test edge cases and error paths for KVManager."""

    def test_delete_nonexistent_key(self):
        """Test deleting a key that was never registered."""
        manager = KVManager()

        result, region_id = manager.delete("999")
        assert result == DeleteResult.NOT_FOUND
        assert region_id is None

    def test_register_then_delete_then_re_register(self):
        """Test that a key can be re-registered after deletion."""
        manager = KVManager()
        manager.register(key="123", region_id=1, kv_offset=0, kv_length=1024)

        result, region_id = manager.delete("123")
        assert result == DeleteResult.DELETED
        assert region_id == 1
        assert manager.exists("123") is False

        # Re-register should succeed as a new entry
        is_new, new_region_id = manager.register(
            key="123", region_id=2, kv_offset=0, kv_length=512
        )
        assert is_new is True
        assert new_region_id == 2
        assert manager.exists("123") is True


class TestKVManagerPin:
    """Test cases for pin/unpin operations."""

    # ---- pin() ----

    def test_pin_existing_key(self):
        """pin() on existing key returns True and increments pin_count."""
        manager = KVManager()
        manager.register(key="1", region_id=1, kv_offset=0, kv_length=100)

        assert manager.pin("1") is True
        assert manager.lookup("1").pin_count == 1

    def test_pin_increments_multiple_times(self):
        """Multiple pin() calls increment pin_count each time."""
        manager = KVManager()
        manager.register(key="1", region_id=1, kv_offset=0, kv_length=100)

        manager.pin("1")
        manager.pin("1")
        manager.pin("1")
        assert manager.lookup("1").pin_count == 3

    def test_pin_nonexistent_key(self):
        """pin() on nonexistent key returns False."""
        manager = KVManager()
        assert manager.pin("999") is False

    # ---- unpin() ----

    def test_unpin_pinned_key(self):
        """unpin() on pinned key returns True and decrements pin_count."""
        manager = KVManager()
        manager.register(key="1", region_id=1, kv_offset=0, kv_length=100)
        manager.pin("1")
        manager.pin("1")

        assert manager.unpin("1") is True
        assert manager.lookup("1").pin_count == 1

    def test_unpin_nonexistent_key(self):
        """unpin() on nonexistent key returns False."""
        manager = KVManager()
        assert manager.unpin("999") is False

    def test_unpin_underflow_protection(self):
        """unpin() on key with pin_count=0 returns False (no underflow)."""
        manager = KVManager()
        manager.register(key="1", region_id=1, kv_offset=0, kv_length=100)

        # Never pinned — pin_count is 0
        assert manager.unpin("1") is False
        assert manager.lookup("1").pin_count == 0

    def test_unpin_after_full_decrement(self):
        """unpin() returns False after pin_count reaches 0."""
        manager = KVManager()
        manager.register(key="1", region_id=1, kv_offset=0, kv_length=100)
        manager.pin("1")

        assert manager.unpin("1") is True
        assert manager.lookup("1").pin_count == 0
        # Second unpin should fail
        assert manager.unpin("1") is False

    # ---- delete() with pin ----

    def test_delete_pinned_key_refused(self):
        """delete() on pinned key returns PINNED and does not remove entry."""
        manager = KVManager()
        manager.register(key="1", region_id=1, kv_offset=0, kv_length=100)
        manager.pin("1")

        result, region_id = manager.delete("1")
        assert result == DeleteResult.PINNED
        assert region_id is None
        # Entry still exists
        assert manager.exists("1") is True
        assert manager.lookup("1").pin_count == 1

    def test_delete_after_unpin(self):
        """delete() succeeds after pin_count reaches 0 via unpin()."""
        manager = KVManager()
        manager.register(key="1", region_id=1, kv_offset=0, kv_length=100)
        manager.pin("1")
        manager.unpin("1")

        result, region_id = manager.delete("1")
        assert result == DeleteResult.DELETED
        assert region_id == 1
        assert manager.exists("1") is False


class TestKVManagerBatchPin:
    """Test cases for batch pin/unpin operations."""

    # ---- batch_pin() ----

    def test_batch_pin_all_exist(self):
        """batch_pin() pins all keys when all exist."""
        manager = KVManager()
        manager.register(key="1", region_id=1, kv_offset=0, kv_length=100)
        manager.register(key="2", region_id=1, kv_offset=100, kv_length=100)
        manager.register(key="3", region_id=1, kv_offset=200, kv_length=100)

        results = manager.batch_pin(["1", "2", "3"])
        assert results == [True, True, True]
        assert manager.lookup("1").pin_count == 1
        assert manager.lookup("2").pin_count == 1
        assert manager.lookup("3").pin_count == 1

    def test_batch_pin_prefix_stop(self):
        """batch_pin() stops at first miss — only prefix keys are pinned."""
        manager = KVManager()
        manager.register(key="1", region_id=1, kv_offset=0, kv_length=100)
        # key "2" missing
        manager.register(key="3", region_id=1, kv_offset=200, kv_length=100)

        results = manager.batch_pin(["1", "2", "3"])
        assert results == [True, False, False]
        # Only "1" should be pinned
        assert manager.lookup("1").pin_count == 1
        # "3" exists but should NOT be pinned
        assert manager.lookup("3").pin_count == 0

    def test_batch_pin_first_key_missing(self):
        """batch_pin() with first key missing returns all False."""
        manager = KVManager()
        manager.register(key="2", region_id=1, kv_offset=0, kv_length=100)

        results = manager.batch_pin(["1", "2"])
        assert results == [False, False]
        assert manager.lookup("2").pin_count == 0

    def test_batch_pin_empty_list(self):
        """batch_pin([]) returns empty list."""
        manager = KVManager()
        assert manager.batch_pin([]) == []

    # ---- batch_unpin() ----

    def test_batch_unpin_all_pinned(self):
        """batch_unpin() unpins all previously pinned keys."""
        manager = KVManager()
        manager.register(key="1", region_id=1, kv_offset=0, kv_length=100)
        manager.register(key="2", region_id=1, kv_offset=100, kv_length=100)
        manager.pin("1")
        manager.pin("2")

        results = manager.batch_unpin(["1", "2"])
        assert results == [True, True]
        assert manager.lookup("1").pin_count == 0
        assert manager.lookup("2").pin_count == 0

    def test_batch_unpin_mixed(self):
        """batch_unpin() with mix of pinned, unpinned, and missing keys."""
        manager = KVManager()
        manager.register(key="1", region_id=1, kv_offset=0, kv_length=100)
        manager.register(key="2", region_id=1, kv_offset=100, kv_length=100)
        manager.pin("1")
        # "2" registered but not pinned, "3" doesn't exist

        results = manager.batch_unpin(["1", "2", "3"])
        assert results == [True, False, False]

    def test_batch_unpin_empty_list(self):
        """batch_unpin([]) returns empty list."""
        manager = KVManager()
        assert manager.batch_unpin([]) == []
