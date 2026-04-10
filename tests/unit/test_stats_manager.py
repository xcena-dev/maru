# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tests for StatsManager."""

import threading

from maru_server.stats_manager import StatsManager


class TestStatsManagerCounters:
    """Test in-memory counter tracking."""

    def test_record_increments_count(self):
        sm = StatsManager()
        sm.record("store", 1, 4096, 1024, 50.0)
        sm.record("store", 1, 8192, 2048, 30.0)
        stats = sm.get_stats()
        assert stats["clients"]["_all"]["operations"]["store"]["count"] == 2
        assert stats["clients"]["_all"]["operations"]["store"]["total_bytes"] == 3072
        sm.close()

    def test_hit_miss_merged(self):
        sm = StatsManager()
        sm.record("retrieve", 1, 4096, 1024, 10.0, result="hit")
        sm.record("retrieve", 0, 0, 0, 5.0, result="miss")
        sm.record("retrieve", 2, 8192, 2048, 15.0, result="hit")
        stats = sm.get_stats()
        r = stats["clients"]["_all"]["operations"]["retrieve"]
        assert r["count"] == 3
        assert r["hit_count"] == 2
        assert r["miss_count"] == 1
        sm.close()

    def test_latency_stats(self):
        sm = StatsManager()
        sm.record("alloc", 1, 0, 100, 10.0)
        sm.record("alloc", 1, 0, 100, 30.0)
        sm.record("alloc", 1, 0, 100, 20.0)
        stats = sm.get_stats()["clients"]["_all"]["operations"]["alloc"]
        assert stats["min_latency_us"] == 10.0
        assert stats["max_latency_us"] == 30.0
        assert stats["avg_latency_us"] == 20.0
        sm.close()

    def test_interval_stats_and_reset(self):
        sm = StatsManager()
        sm.record("alloc", 1, 0, 100, 10.0)
        sm.record("alloc", 1, 0, 100, 30.0)

        # First get_stats: interval has both records
        stats1 = sm.get_stats()["clients"]["_all"]["operations"]["alloc"]
        assert stats1["interval_count"] == 2
        assert stats1["interval_min_us"] == 10.0
        assert stats1["interval_max_us"] == 30.0
        assert stats1["interval_avg_us"] == 20.0

        # Second get_stats without new records: interval is reset
        stats2 = sm.get_stats()["clients"]["_all"]["operations"]["alloc"]
        assert stats2["interval_count"] == 0
        assert stats2["interval_avg_us"] == 0

        # All-time stats should remain
        assert stats2["count"] == 2
        assert stats2["avg_latency_us"] == 20.0

        # New record only in next interval
        sm.record("alloc", 1, 0, 100, 50.0)
        stats3 = sm.get_stats()["clients"]["_all"]["operations"]["alloc"]
        assert stats3["interval_count"] == 1
        assert stats3["interval_min_us"] == 50.0
        assert stats3["interval_max_us"] == 50.0
        assert stats3["count"] == 3  # all-time
        sm.close()

    def test_empty_stats(self):
        sm = StatsManager()
        stats = sm.get_stats()
        assert stats["clients"]["_all"]["operations"] == {}
        sm.close()

    def test_multiple_op_types(self):
        sm = StatsManager()
        sm.record("alloc", 1, 0, 100, 10.0)
        sm.record("store", 1, 0, 200, 20.0)
        sm.record("retrieve", 1, 0, 300, 30.0, result="hit")
        stats = sm.get_stats()
        assert len(stats["clients"]["_all"]["operations"]) == 3
        assert "alloc" in stats["clients"]["_all"]["operations"]
        assert "store" in stats["clients"]["_all"]["operations"]
        assert "retrieve" in stats["clients"]["_all"]["operations"]
        assert stats["clients"]["_all"]["operations"]["retrieve"]["hit_count"] == 1
        sm.close()

    def test_per_client_stats(self):
        sm = StatsManager()
        sm.record("store", 1, 0, 100, 10.0, client_id="client_a")
        sm.record("store", 1, 0, 200, 20.0, client_id="client_b")
        sm.record("store", 1, 0, 300, 30.0, client_id="client_a")
        stats = sm.get_stats()

        # _all has all 3
        assert stats["clients"]["_all"]["operations"]["store"]["count"] == 3

        # client_a has 2
        assert stats["clients"]["client_a"]["operations"]["store"]["count"] == 2
        assert stats["clients"]["client_a"]["operations"]["store"]["total_bytes"] == 400

        # client_b has 1
        assert stats["clients"]["client_b"]["operations"]["store"]["count"] == 1
        assert stats["clients"]["client_b"]["operations"]["store"]["total_bytes"] == 200
        sm.close()


class TestStatsManagerLifecycle:
    """Test close and idempotency."""

    def test_record_after_close_is_noop(self):
        sm = StatsManager()
        sm.record("alloc", 1, 0, 100, 10.0)
        sm.close()
        sm.record("alloc", 1, 0, 100, 10.0)  # should not raise
        stats = sm.get_stats()
        assert stats["clients"]["_all"]["operations"]["alloc"]["count"] == 1

    def test_close_idempotent(self):
        sm = StatsManager()
        sm.record("alloc", 1, 0, 100, 10.0)
        sm.close()
        sm.close()  # should not raise


class TestStatsManagerThreadSafety:
    """Test concurrent record calls."""

    def test_concurrent_records(self):
        sm = StatsManager()
        n_threads = 8
        n_per_thread = 1000

        def worker(tid: int):
            for i in range(n_per_thread):
                sm.record("alloc", tid, i * 100, 100, 1.0)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        stats = sm.get_stats()
        assert (
            stats["clients"]["_all"]["operations"]["alloc"]["count"]
            == n_threads * n_per_thread
        )
        sm.close()
