# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Server-side operation metrics collection.

Tracks per-operation counters and latency statistics per client.
"""

import logging
import threading
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class _OpStats:
    """Accumulated statistics for a single (op_type, result) pair."""

    # All-time cumulative
    count: int = 0
    total_bytes: int = 0
    latency_sum_us: float = 0.0
    latency_min_us: float = float("inf")
    latency_max_us: float = 0.0

    # Per-interval (reset on each get_stats() call)
    iv_count: int = 0
    iv_sum_us: float = 0.0
    iv_min_us: float = float("inf")
    iv_max_us: float = 0.0


class StatsManager:
    """Server-side metrics collector.

    Thread-safe. Collects per-operation counters and latency stats.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stats: dict[tuple[str, str, str], _OpStats] = {}  # (client_id, op_type, result)
        self._closed = False

    def record(
        self,
        op_type: str,
        region_id: int,
        device_offset: int,
        size: int,
        latency_us: float,
        result: str = "none",
        client_id: str = "_unknown",
    ) -> None:
        """Record an operation event.

        Args:
            op_type: Operation name (alloc, store, retrieve, etc.)
            region_id: Region identifier (0 if N/A)
            device_offset: Device byte offset (0 if N/A)
            size: Data size in bytes
            latency_us: Processing time in microseconds
            result: "hit", "miss", or "none"
            client_id: Handler instance ID
        """
        with self._lock:
            if self._closed:
                return
            key = (client_id, op_type, result)
            stats = self._stats.get(key)
            if stats is None:
                stats = _OpStats()
                self._stats[key] = stats

            stats.count += 1
            stats.total_bytes += size
            stats.latency_sum_us += latency_us
            if latency_us < stats.latency_min_us:
                stats.latency_min_us = latency_us
            if latency_us > stats.latency_max_us:
                stats.latency_max_us = latency_us

            # Interval stats
            stats.iv_count += 1
            stats.iv_sum_us += latency_us
            if latency_us < stats.iv_min_us:
                stats.iv_min_us = latency_us
            if latency_us > stats.iv_max_us:
                stats.iv_max_us = latency_us

    @staticmethod
    def _new_merged() -> dict:
        return {
            "count": 0, "hit_count": 0, "miss_count": 0,
            "total_bytes": 0,
            "latency_sum_us": 0.0,
            "latency_min_us": float("inf"),
            "latency_max_us": 0.0,
            "iv_count": 0, "iv_sum_us": 0.0,
            "iv_min_us": float("inf"), "iv_max_us": 0.0,
        }

    @staticmethod
    def _accumulate(m: dict, s: "_OpStats", result: str) -> None:
        m["count"] += s.count
        m["total_bytes"] += s.total_bytes
        m["latency_sum_us"] += s.latency_sum_us
        if s.count:
            m["latency_min_us"] = min(m["latency_min_us"], s.latency_min_us)
        m["latency_max_us"] = max(m["latency_max_us"], s.latency_max_us)
        if result == "hit":
            m["hit_count"] += s.count
        elif result == "miss":
            m["miss_count"] += s.count
        m["iv_count"] += s.iv_count
        m["iv_sum_us"] += s.iv_sum_us
        if s.iv_count:
            m["iv_min_us"] = min(m["iv_min_us"], s.iv_min_us)
        m["iv_max_us"] = max(m["iv_max_us"], s.iv_max_us)

    @staticmethod
    def _finalize(merged: dict[str, dict]) -> dict:
        ops = {}
        for op_type, m in sorted(merged.items()):
            avg_us = m["latency_sum_us"] / m["count"] if m["count"] else 0
            iv_avg = m["iv_sum_us"] / m["iv_count"] if m["iv_count"] else 0
            ops[op_type] = {
                "count": m["count"],
                "hit_count": m["hit_count"],
                "miss_count": m["miss_count"],
                "total_bytes": m["total_bytes"],
                "latency_sum_us": round(m["latency_sum_us"], 1),
                "avg_latency_us": round(avg_us, 1),
                "min_latency_us": round(m["latency_min_us"], 1) if m["count"] else 0,
                "max_latency_us": round(m["latency_max_us"], 1),
                "interval_count": m["iv_count"],
                "interval_avg_us": round(iv_avg, 1),
                "interval_min_us": round(m["iv_min_us"], 1) if m["iv_count"] else 0,
                "interval_max_us": round(m["iv_max_us"], 1),
            }
        return {"operations": ops}

    def get_stats(self) -> dict:
        """Return per-client and aggregated operation statistics."""
        with self._lock:
            per_client: dict[str, dict[str, dict]] = {}
            all_merged: dict[str, dict] = {}

            for (client_id, op_type, result), s in sorted(self._stats.items()):
                if client_id not in per_client:
                    per_client[client_id] = {}
                client_ops = per_client[client_id]
                if op_type not in client_ops:
                    client_ops[op_type] = self._new_merged()
                self._accumulate(client_ops[op_type], s, result)

                if op_type not in all_merged:
                    all_merged[op_type] = self._new_merged()
                self._accumulate(all_merged[op_type], s, result)

                # Reset interval counters
                s.iv_count = 0
                s.iv_sum_us = 0.0
                s.iv_min_us = float("inf")
                s.iv_max_us = 0.0

            clients = {"_all": self._finalize(all_merged)}
            for cid, ops in sorted(per_client.items()):
                clients[cid] = self._finalize(ops)

            return {"clients": clients}

    def close(self) -> None:
        """Mark as closed to reject future records."""
        with self._lock:
            self._closed = True
