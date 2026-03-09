# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tests for KV Manager."""

from maru_server.kv_manager import KVManager


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

        existed, region_id = manager.delete("123")
        assert existed is True
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
        existed, region_id = manager.delete("123")
        assert existed is True
        assert region_id == 1
        assert manager.exists("123") is False

        # Second delete on now-missing key returns (False, None)
        existed, region_id = manager.delete("123")
        assert existed is False
        assert region_id is None

    def test_delete_nonexistent(self):
        """Test deleting a nonexistent key."""
        manager = KVManager()
        existed, region_id = manager.delete("999")
        assert existed is False
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

        existed, region_id = manager.delete("999")
        assert existed is False
        assert region_id is None

    def test_register_then_delete_then_re_register(self):
        """Test that a key can be re-registered after deletion."""
        manager = KVManager()
        manager.register(key="123", region_id=1, kv_offset=0, kv_length=1024)

        existed, region_id = manager.delete("123")
        assert existed is True
        assert region_id == 1
        assert manager.exists("123") is False

        # Re-register should succeed as a new entry
        is_new, new_region_id = manager.register(
            key="123", region_id=2, kv_offset=0, kv_length=512
        )
        assert is_new is True
        assert new_region_id == 2
        assert manager.exists("123") is True
