# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tests for batch operations - server-side unit tests."""

from maru_server.kv_manager import KVManager
from maru_server.server import MaruServer


class TestMaruBatch:
    """Test cases for MaruServer batch-like operations."""

    def test_register_multiple_kvs(self):
        """Test registering multiple KVs through server."""
        server = MaruServer()
        handle = server.request_alloc("instance1", 4096)
        assert handle is not None

        # Register multiple KVs
        results = []
        for i in range(5):
            is_new = server.register_kv(
                key=str(i), region_id=handle.region_id, kv_offset=i * 100, kv_length=100
            )
            results.append(is_new)

        assert results == [True, True, True, True, True]

    def test_lookup_multiple_kvs(self):
        """Test looking up multiple KVs through server."""
        server = MaruServer()
        handle = server.request_alloc("instance1", 4096)

        # Register some entries
        server.register_kv(
            key="1", region_id=handle.region_id, kv_offset=0, kv_length=100
        )
        server.register_kv(
            key="3", region_id=handle.region_id, kv_offset=200, kv_length=300
        )

        # Lookup: found, not-found, found
        result1 = server.lookup_kv("1")
        result2 = server.lookup_kv("2")
        result3 = server.lookup_kv("3")

        assert result1 is not None
        assert result1["handle"].region_id == handle.region_id
        assert result1["kv_offset"] == 0
        assert result2 is None
        assert result3 is not None
        assert result3["kv_offset"] == 200

    def test_exists_multiple_kvs(self):
        """Test existence check for multiple KVs through server."""
        server = MaruServer()
        handle = server.request_alloc("instance1", 4096)

        server.register_kv(
            key="1", region_id=handle.region_id, kv_offset=0, kv_length=100
        )
        server.register_kv(
            key="3", region_id=handle.region_id, kv_offset=200, kv_length=300
        )

        results = [server.exists_kv(k) for k in ["1", "2", "3", "4"]]
        assert results == [True, False, True, False]


class TestKVManagerScaleOperations:
    """Scale tests for KV operations."""

    def test_register_large_batch(self):
        """Test registering a large number of entries."""
        manager = KVManager()
        n = 1000
        for i in range(n):
            is_new, region_id = manager.register(
                key=str(i), region_id=1, kv_offset=i * 100, kv_length=100
            )
            assert is_new is True

        assert manager.get_stats()["total_entries"] == n

    def test_lookup_large_batch(self):
        """Test looking up a large number of keys."""
        manager = KVManager()
        n = 1000

        # Register half the keys
        for i in range(0, n, 2):
            manager.register(key=str(i), region_id=1, kv_offset=i, kv_length=100)

        # Lookup all keys
        found_count = 0
        for i in range(n):
            entry = manager.lookup(str(i))
            if entry is not None:
                found_count += 1

        assert found_count == n // 2
