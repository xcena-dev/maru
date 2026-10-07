# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruServer - Main server implementation."""

import argparse
import logging
import os
import signal
import sys
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from threading import RLock
from typing import TYPE_CHECKING

from maru_shm.types import MaruHandle

from .allocation_manager import AllocationManager
from .kv_manager import DeleteResult, KVManager
from .stats_manager import StatsManager

if TYPE_CHECKING:
    from .remote_access import RemoteAccess

logger = logging.getLogger(__name__)

#: Default client lease TTL in seconds. Clients renew every quarter of it.
DEFAULT_CLIENT_LEASE_TTL = 30.0

#: Ended (expired or released) lease ids remembered, so a late renewal is
#: told the lease is over instead of starting it again.
_MAX_ENDED_LEASES = 4096


class MaruServer:
    """
    Central metadata server for Maru shared memory KV cache.

    Manages:
    - KV metadata (hash -> handle mapping)
    - Memory allocation and ownership
    """

    def __init__(
        self,
        rm_address: str | None = None,
        dax_paths: list[str] | None = None,
        enable_cxl: bool = True,
        cpu_capacity_limit: int = 64 * 1024**3,
        cpu_session_ttl: float = 30,
        client_lease_ttl: float = DEFAULT_CLIENT_LEASE_TTL,
        clock: Callable[[], float] = time.monotonic,
    ):
        if client_lease_ttl < 0:
            raise ValueError("client_lease_ttl must be >= 0 (0 disables leases)")
        self._rm_address = rm_address or "127.0.0.1:9850"
        self._dax_paths = dax_paths
        from .replica_directory import ReplicaDirectory

        self._allocation_manager = (
            AllocationManager(rm_address=rm_address) if enable_cxl else None
        )
        self._kv_manager = KVManager()
        self._stats_manager = StatsManager()
        self._lock = RLock()  # Coordinates cross-manager operations
        # Owners whose regions a server component (remote access) manages:
        # local RPC calls may not return those regions or delete their keys.
        self._managed_owners: set[str] = set()
        # Client leases: a client that renews a lease gets the regions it
        # allocates under that lease reclaimed when it stops renewing (for
        # example, a process killed before close()). Clients that never
        # renew a lease keep the previous behaviour.
        self._client_lease_ttl = client_lease_ttl
        self._clock = clock
        self._leases: dict[str, tuple[str, float]] = {}  # lease -> (owner, deadline)
        self._ended_leases: OrderedDict[str, None] = OrderedDict()
        self._last_lease_activity = clock()
        # Regions of expired leases; registrations into them are refused.
        self._reaped_regions: set[int] = set()
        self.replica_directory = ReplicaDirectory(
            cpu_capacity_limit,
            cpu_session_ttl,
            reserve_cxl=self._reserve_storage_region if enable_cxl else None,
            release_cxl=self._release_storage_region if enable_cxl else None,
        )
        # TODO: Add PinMonitor daemon thread when eviction is implemented.
        # Periodically force-unpin entries that exceed a TTL to prevent
        # pin leaks from crashed clients.

        if self._dax_paths:
            self._validate_dax_paths()

        logger.info(
            "MaruServer initialized (rm_address=%s, dax_paths=%s)",
            self._rm_address,
            self._dax_paths,
        )

    def _validate_dax_paths(self) -> None:
        """Warn if any --dax-path values don't match resource manager pools."""
        try:
            pools = self._allocation_manager.pool_stats()
            available = {p.dax_path for p in pools}
            for path in self._dax_paths:
                if path not in available:
                    logger.warning(
                        "--dax-path %s not found in resource manager pools: %s",
                        path,
                        available,
                    )
        except Exception:
            logger.warning(
                "Could not validate --dax-path against resource manager",
                exc_info=True,
            )

    @property
    def rm_address(self) -> str:
        """Resource manager address used by this server."""
        return self._rm_address

    def _reserve_storage_region(self, owner: str, size: int) -> MaruHandle | None:
        """Reserve a typed pool without exposing it to legacy clients."""
        return self.request_alloc(owner, size, legacy_visible=False)

    def _release_storage_region(self, owner: str, region_id: int) -> bool:
        """Idempotent release of a typed pool, after the owner unmaps it."""
        if self._allocation_manager.get_handle(region_id) is None:
            return True
        return self.return_alloc(owner, region_id)

    # =========================================================================
    # Allocation Management
    # =========================================================================

    def add_managed_owner(self, owner: str) -> None:
        """Reserve ``owner`` for a server component.

        Local RPC calls can then neither allocate nor return regions under
        that owner, nor delete keys in its regions: the component tracks the
        pages behind those keys itself.
        """
        with self._lock:
            self._managed_owners.add(owner)

    def request_alloc(
        self,
        instance_id: str,
        size: int,
        *,
        legacy_visible: bool = True,
        managed: bool = False,
        lease_id: str = "",
    ) -> MaruHandle | None:
        """Handle allocation request from client.

        When ``--dax-path`` is configured, iterates over the server's
        dax_path list (fill-first fallback). Otherwise uses any available pool.
        ``managed`` marks a call from the component that owns ``instance_id``.
        Expired client leases are reclaimed first so their space is usable,
        and a request under an expired lease is refused.
        """
        if self._allocation_manager is None:
            return None
        if instance_id in self._managed_owners and not managed:
            logger.warning("Refused allocation under managed owner %s", instance_id)
            return None
        self.reap_expired_leases()
        if lease_id:
            with self._lock:
                if lease_id in self._ended_leases:
                    logger.warning(
                        "Refused allocation for %s: lease %s expired",
                        instance_id,
                        lease_id,
                    )
                    return None
        dax_paths_iter = self._dax_paths if self._dax_paths else [""]

        for path in dax_paths_iter:
            if legacy_visible:
                handle = self._allocation_manager.allocate(
                    instance_id, size, dax_path=path, lease_id=lease_id
                )
            else:
                handle = self._allocation_manager.allocate(
                    instance_id,
                    size,
                    dax_path=path,
                    legacy_visible=False,
                )
            if handle:
                logger.info(
                    "Allocated %d bytes for %s from %s: region_id=%d",
                    size,
                    instance_id,
                    path or "(any)",
                    handle.region_id,
                )
                with self._lock:
                    # The resource manager may reuse a reclaimed region id.
                    self._reaped_regions.discard(handle.region_id)
                return handle
            logger.debug(
                "Pool %s refused allocation for %s (%d bytes)",
                path or "(any)",
                instance_id,
                size,
            )

        logger.error("Failed to allocate %d bytes for %s", size, instance_id)
        return None

    def return_alloc(
        self, instance_id: str, region_id: int, *, managed: bool = False
    ) -> bool:
        """Handle allocation return request from client.

        ``managed`` marks a call from the component that owns ``instance_id``.
        """
        if self._allocation_manager is None:
            return False
        if instance_id in self._managed_owners and not managed:
            logger.warning("Refused return of managed region %d", region_id)
            return False
        success = self._allocation_manager.release(instance_id, region_id)
        if success:
            logger.info("Released region_id=%d by %s", region_id, instance_id)
        return success

    def list_allocations(
        self, exclude_instance_id: str | None = None
    ) -> list[MaruHandle]:
        """List all active allocation handles."""
        if self._allocation_manager is None:
            return []
        return self._allocation_manager.list_allocations(exclude_instance_id)

    # =========================================================================
    # Client Leases
    # =========================================================================

    @property
    def client_lease_ttl(self) -> float:
        """Client lease TTL in seconds (0 when leases are disabled)."""
        return self._client_lease_ttl

    def renew_lease(
        self, instance_id: str, lease_id: str, release: bool = False
    ) -> dict:
        """Renew a client lease, starting it on first use.

        Args:
            instance_id: Client instance identifier that owns the lease.
            lease_id: Lease identifier, unique per client connection.
            release: End the lease instead; sent by a clean ``close()`` after
                it returned its regions.

        Returns:
            ``lease_ttl`` and ``lease_expired`` (True when the lease already
            expired and its regions were reclaimed). An empty dict, the plain
            heartbeat reply, when leases are disabled or no lease is named.
        """
        if not self._client_lease_ttl or not instance_id or not lease_id:
            return {}
        with self._lock:
            now = self._observe_lease_time()
            if lease_id in self._ended_leases:
                return {"lease_ttl": self._client_lease_ttl, "lease_expired": True}
            if release:
                if self._leases.pop(lease_id, None) is not None:
                    logger.info("Client %s ended lease %s", instance_id, lease_id)
                # A renewal that arrives after the release must not reopen it.
                self._remember_ended_lease(lease_id)
                return {"lease_ttl": self._client_lease_ttl, "lease_expired": False}
            if lease_id not in self._leases:
                logger.info("Client %s started lease %s", instance_id, lease_id)
            # Credit this renewal before reaping, so a renewal that waited in
            # the server's queue never expires its own lease.
            self._leases[lease_id] = (instance_id, now + self._client_lease_ttl)
            self._reap_expired_leases(now)
        return {"lease_ttl": self._client_lease_ttl, "lease_expired": False}

    def reap_expired_leases(self) -> None:
        """Reclaim the regions of client leases that were not renewed in time.

        Runs on lease renewals and allocation requests, so space held by a
        client that exited without ``close()`` is reclaimed before another
        client needs it.
        """
        if not self._client_lease_ttl or self._allocation_manager is None:
            return
        with self._lock:
            self._reap_expired_leases(self._observe_lease_time())

    def _observe_lease_time(self) -> float:
        """Return now, first extending every lease across a server stall.

        Live clients renew every quarter of the TTL, so while any client is
        alive the server sees lease activity at least that often. A gap over
        half the TTL means the server itself was not processing requests (a
        long request holding the lock, a paused process), or no leased
        client was alive. Renewals may have queued during the gap, so the
        part of it beyond half the TTL does not count as lease time.

        This keeps a live lease: its last renewal was processed at most a
        quarter TTL before the gap began, so after the shift its deadline is
        still at least a quarter TTL ahead. Dead leases still advance by
        half the TTL per gap, so they expire at most half a TTL after the
        server becomes active again. Caller holds ``_lock``.
        """
        now = self._clock()
        gap = now - self._last_lease_activity
        stalled = gap - self._client_lease_ttl / 2
        if self._leases and stalled > 0:
            self._leases = {
                lease_id: (owner, deadline + stalled)
                for lease_id, (owner, deadline) in self._leases.items()
            }
            logger.info(
                "No lease activity for %.1f s; extended %d lease(s) by %.1f s",
                gap,
                len(self._leases),
                stalled,
            )
        self._last_lease_activity = now
        return now

    def _reap_expired_leases(self, now: float) -> None:
        """Expire leases whose deadline passed. Caller holds ``_lock``."""
        if self._allocation_manager is None:
            return
        expired = [
            (lease_id, owner)
            for lease_id, (owner, deadline) in self._leases.items()
            if deadline <= now
        ]
        for lease_id, owner in expired:
            del self._leases[lease_id]
            self._remember_ended_lease(lease_id)
            regions = self._allocation_manager.disconnect_lease(owner, lease_id)
            self._reaped_regions.update(regions)
            logger.warning(
                "Client %s lease %s expired (no renewal for %.0f s); "
                "reclaimed %d region(s)",
                owner,
                lease_id,
                self._client_lease_ttl,
                len(regions),
            )

    def _remember_ended_lease(self, lease_id: str) -> None:
        """Remember an expired or released lease. Caller holds ``_lock``."""
        self._ended_leases[lease_id] = None
        while len(self._ended_leases) > _MAX_ENDED_LEASES:
            self._ended_leases.popitem(last=False)

    def client_disconnected(self, instance_id: str) -> None:
        """Handle client disconnection."""
        if self._allocation_manager is None:
            return
        self._allocation_manager.disconnect_client(instance_id)
        logger.info("Client %s disconnected, released allocations", instance_id)

    # =========================================================================
    # KV Management
    # =========================================================================

    def register_kv(
        self, key: str, region_id: int, kv_offset: int, kv_length: int
    ) -> bool:
        """Register a KV entry."""
        if self._allocation_manager is None:
            return False
        with self._lock:
            if region_id in self._reaped_regions:
                logger.warning(
                    "Refused KV %s: region %d belongs to an expired lease",
                    key,
                    region_id,
                )
                return False
            is_new, alloc_to_ref = self._kv_manager.register(
                key, region_id, kv_offset, kv_length
            )

            if alloc_to_ref is not None:
                self._allocation_manager.increment_kv_ref(alloc_to_ref)

        logger.debug("Registered KV: key=%s, region_id=%d", key, region_id)
        return is_new

    def lookup_kv(self, key: str) -> dict | None:
        """Lookup a KV entry and return handle with KV location info."""
        if self._allocation_manager is None:
            return None
        with self._lock:
            entry = self._kv_manager.lookup(key)
            if entry is None:
                return None

            handle = self._allocation_manager.get_handle(entry.region_id)
            if handle is None:
                return None

            return {
                "handle": handle,
                "kv_offset": entry.kv_offset,
                "kv_length": entry.kv_length,
            }

    def exists_kv(self, key: str) -> bool:
        """Check if a KV entry exists."""
        return self._kv_manager.exists(key)

    def pin_kv(self, key: str) -> bool:
        """Check if a KV entry exists and pin it atomically."""
        return self._kv_manager.pin(key)

    def unpin(self, key: str) -> bool:
        """Unpin a KV entry, making it eligible for eviction."""
        return self._kv_manager.unpin(key)

    def delete_kv(self, key: str) -> bool:
        """Delete a KV entry.

        Keys in a managed region (see :meth:`add_managed_owner`) are refused:
        their component deletes them with :meth:`delete_kv_at`.
        """
        with self._lock:
            entry = self._kv_manager.lookup(key)
            if entry is not None and self._is_managed_region(entry.region_id):
                logger.debug("Delete refused: key=%s is in a managed region", key)
                return False
            result, region_to_deref = self._kv_manager.delete(key)

            if region_to_deref is not None:
                self._allocation_manager.decrement_kv_ref(region_to_deref)

        return result == DeleteResult.DELETED

    def delete_kv_at(self, key: str, region_id: int, kv_offset: int) -> DeleteResult:
        """Delete ``key`` only if it still names this location.

        For a component that owns the page behind its key.

        Returns:
            ``DELETED``, ``PINNED`` or ``NOT_FOUND`` (absent or elsewhere).
        """
        with self._lock:
            result, region_to_deref = self._kv_manager.delete_at(
                key, region_id, kv_offset
            )
            if region_to_deref is not None:
                self._allocation_manager.decrement_kv_ref(region_to_deref)
        return result

    def batch_register_or_lookup(
        self, entries: list[tuple[str, int, int, int]]
    ) -> list[dict | None]:
        """Register keys that are absent; report where the others already are.

        One step under the server lock, so no other register or delete runs
        between the check and the register.

        Args:
            entries: (key, region_id, kv_offset, kv_length) tuples.

        Returns:
            Per entry: None if it was registered now, else where the key
            already is (``region_id``, ``kv_offset``, ``kv_length``).
        """
        if self._allocation_manager is None:
            raise RuntimeError("CXL allocation is disabled")
        with self._lock:
            out: list[dict | None] = []
            for key, region_id, kv_offset, kv_length in entries:
                existing = self._kv_manager.lookup(key)
                if existing is not None:
                    out.append(self._entry_dict(existing))
                    continue
                _, alloc_to_ref = self._kv_manager.register(
                    key, region_id, kv_offset, kv_length
                )
                if alloc_to_ref is not None:
                    self._allocation_manager.increment_kv_ref(alloc_to_ref)
                out.append(None)
            return out

    def batch_pin_lookup(
        self, keys: list[str], region_ids: set[int]
    ) -> list[dict | None]:
        """Pin and locate the keys that live in ``region_ids``.

        Each key is pinned and located in one step, on its own (no prefix
        stop); keys elsewhere or absent are not pinned and come back as None.

        Returns:
            Per key: where a pinned entry is (``region_id``, ``kv_offset``,
            ``kv_length``), or None.
        """
        with self._lock:
            return [
                None if e is None else self._entry_dict(e)
                for e in self._kv_manager.batch_pin_in(keys, region_ids)
            ]

    # =========================================================================
    # Batch KV Operations
    # =========================================================================

    def batch_register_kv(self, entries: list[tuple[str, int, int, int]]) -> list[bool]:
        """
        Register multiple KV entries in a single operation.

        Args:
            entries: List of (key, region_id, kv_offset, kv_length) tuples

        Returns:
            List of booleans indicating if each entry was newly registered
        """
        if self._allocation_manager is None:
            return [False] * len(entries)
        with self._lock:
            results = []
            for key, region_id, kv_offset, kv_length in entries:
                if region_id in self._reaped_regions:
                    logger.warning(
                        "Refused KV %s: region %d belongs to an expired lease",
                        key,
                        region_id,
                    )
                    results.append(False)
                    continue
                is_new, alloc_to_ref = self._kv_manager.register(
                    key, region_id, kv_offset, kv_length
                )
                if alloc_to_ref is not None:
                    self._allocation_manager.increment_kv_ref(alloc_to_ref)
                results.append(is_new)
            return results

    def batch_lookup_kv(self, keys: list[str]) -> list[dict | None]:
        """
        Lookup multiple KV entries in a single operation.

        Args:
            keys: List of chunk key strings

        Returns:
            List of dicts with handle/kv_offset/kv_length, or None for each key
        """
        if self._allocation_manager is None:
            return [None] * len(keys)
        with self._lock:
            entries = self._kv_manager.batch_lookup(keys)
            results = []

            for entry in entries:
                if entry is None:
                    results.append(None)
                else:
                    handle = self._allocation_manager.get_handle(entry.region_id)
                    if handle is None:
                        results.append(None)
                    else:
                        results.append(
                            {
                                "handle": handle,
                                "kv_offset": entry.kv_offset,
                                "kv_length": entry.kv_length,
                            }
                        )

            return results

    def batch_pin_kv(self, keys: list[str]) -> list[bool]:
        """Check existence and pin multiple KV entries atomically."""
        return self._kv_manager.batch_pin(keys)

    def batch_unpin(self, keys: list[str]) -> list[bool]:
        """Unpin multiple KV entries."""
        return self._kv_manager.batch_unpin(keys)

    def batch_exists_kv(self, keys: list[str]) -> list[bool]:
        """
        Check existence of multiple KV entries in a single operation.

        Args:
            keys: List of chunk key strings

        Returns:
            List of booleans indicating if each key exists
        """
        return self._kv_manager.batch_exists(keys)

    def _is_managed_region(self, region_id: int) -> bool:
        """Whether a server component manages ``region_id``."""
        if not self._managed_owners or self._allocation_manager is None:
            return False
        return self._allocation_manager.get_owner(region_id) in self._managed_owners

    @staticmethod
    def _entry_dict(entry) -> dict:
        """Where a KV entry is: region, offset and length."""
        return {
            "region_id": entry.region_id,
            "kv_offset": entry.kv_offset,
            "kv_length": entry.kv_length,
        }

    _MAX_STATS_BATCH = 10000

    def report_stats(self, entries: list[dict]) -> None:
        """Record client-reported handler-side stats."""
        for e in entries[: self._MAX_STATS_BATCH]:
            self._stats_manager.record(
                op_type=e.get("op_type", "unknown"),
                size=e.get("size", 0),
                latency_us=e.get("latency_us", 0.0),
                result=e.get("result", "none"),
                client_id=e.get("client_id", "_unknown"),
            )

    def get_stats(self) -> dict:
        """Get server statistics."""
        pool_total, pool_free = self._pool_totals()
        return {
            "kv_manager": self._kv_manager.get_stats(),
            "allocation_manager": (
                self._allocation_manager.get_stats()
                if self._allocation_manager
                else {"num_allocations": 0, "total_allocated": 0, "active_clients": 0}
            ),
            "stats_manager": self._stats_manager.get_stats(),
            # Shared CXL device capacity from the resource manager, summed
            # across the pools this server may allocate from (--dax-path
            # scope; all pools when unrestricted). Clients use free_size to
            # size eviction watermarks against device fill instead of just
            # their owned pool.
            "cxl_pool": {"total_size": pool_total, "free_size": pool_free},
            "cpu_storage": self.replica_directory.get_usage("cpu"),
            "l1_storage": self.replica_directory.get_usage(),
        }

    def get_usage(self) -> dict:
        """Per-instance CXL usage plus shared pool totals.

        For each owner_instance_id, reports the number of regions it owns,
        the bytes it reserved (``allocated``), and the live KV bytes stored
        in those regions (``used``). ``pool_total``/``pool_free`` are the
        shared device capacity from the resource manager.
        """
        # Snapshot in-memory manager state atomically; the RM pool query is
        # done outside the lock since it performs a network round-trip.
        if self._allocation_manager is None:
            return {
                "instances": [],
                "pool_total": 0,
                "pool_free": 0,
                "cpu_storage": self.replica_directory.get_usage("cpu"),
                "l1_storage": self.replica_directory.get_usage(),
            }
        with self._lock:
            alloc = self._allocation_manager.allocated_by_instance()
            owners = self._allocation_manager.region_owners()
            used_by_region = self._kv_manager.used_by_region()

        used_by_instance: dict[str, int] = {}
        for region_id, used in used_by_region.items():
            owner = owners.get(region_id)
            if owner is not None:
                used_by_instance[owner] = used_by_instance.get(owner, 0) + used

        l1_usage = self.replica_directory.get_usage()
        for pool in l1_usage["pools"]:
            if pool["medium"] == "cxl":
                owner = pool["owner_instance_id"]
                used_by_instance[owner] = (
                    used_by_instance.get(owner, 0) + pool["ready_bytes"]
                )

        # Per-instance device breakdown (region -> dax_path, resolved outside
        # the server lock via the RM-backed cache inside the manager).
        devices_by_instance = self._allocation_manager.devices_by_instance()

        instances = [
            {
                "instance_id": instance_id,
                "regions": regions,
                "allocated": allocated,
                "used": used_by_instance.get(instance_id, 0),
                "devices": devices_by_instance.get(instance_id, {}),
            }
            for instance_id, (regions, allocated) in sorted(alloc.items())
        ]

        pool_total, pool_free = self._pool_totals()
        return {
            "instances": instances,
            "pool_total": pool_total,
            "pool_free": pool_free,
            "cpu_storage": {
                **l1_usage,
                "pools": [p for p in l1_usage["pools"] if p["medium"] == "cpu"],
            },
            "l1_storage": l1_usage,
        }

    def _pool_totals(self) -> tuple[int, int]:
        """Return (total_bytes, free_bytes) summed across resource manager pools.

        When the server is restricted to specific devices via ``--dax-path``,
        only those pools are counted: ``request_alloc`` can only claim regions
        from the configured pools, so an unrestricted sum would overstate the
        capacity this server can actually use. Clients size eviction
        watermarks on ``free_size`` — counting a foreign device's free space
        defers eviction until allocation fails instead of triggering it.
        """
        if self._allocation_manager is None:
            return (0, 0)
        try:
            pools = self._allocation_manager.pool_stats()
        except Exception:
            logger.warning("pool_stats failed during get_usage", exc_info=True)
            return (0, 0)
        if self._dax_paths:
            allowed = set(self._dax_paths)
            pools = [p for p in pools if p.dax_path in allowed]
        return (
            sum(p.total_size for p in pools),
            sum(p.free_size for p in pools),
        )

    def close(self) -> None:
        """Shutdown the server and release resources."""
        self._stats_manager.close()
        if self._allocation_manager is not None:
            self._allocation_manager.close()
        logger.info("MaruServer closed")


# =============================================================================
# CLI Utilities
# =============================================================================


def setup_logging(level: str) -> None:
    """Setup logging level for the MaruServer package."""
    log_level = getattr(logging, level.upper(), logging.INFO)
    pkg_logger = logging.getLogger("maru_server")
    pkg_logger.setLevel(log_level)
    for handler in pkg_logger.handlers:
        handler.setLevel(log_level)


def _add_remote_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the options that serve this node's CXL pool to other nodes."""
    from maru_common.sizes import parse_size

    group = parser.add_argument_group(
        "remote access",
        "Serve part of this node's CXL pool to MaruHandler's remote backend on "
        "other nodes (needs NIXL: pip install 'maru[remote]').",
    )
    group.add_argument(
        "--remote-bind",
        default=None,
        help="ZMQ endpoint for remote workers, e.g. tcp://0.0.0.0:6600 "
        "(remote access is off without it)",
    )
    group.add_argument(
        "--remote-pool-size",
        type=parse_size,
        default=None,
        help="bytes of each remote region, e.g. 8G (also the growth step)",
    )
    group.add_argument(
        "--remote-page-bytes",
        type=parse_size,
        default=None,
        help="remote page size; one page holds one KV object, e.g. 32M",
    )
    group.add_argument(
        "--remote-pool-id", default="maru", help="pool name reported to clients"
    )
    group.add_argument(
        "--remote-ucx-device", default="", help="UCX device, e.g. mlx5_1:1"
    )
    group.add_argument(
        "--remote-capacity",
        type=parse_size,
        default=None,
        help="most remote bytes to hold (default: grow until the device is full)",
    )
    group.add_argument(
        "--remote-eviction",
        choices=("lru", "none"),
        default="lru",
        help="when full: delete least recently read keys, or refuse new stores",
    )
    group.add_argument(
        "--remote-reservation-ttl", type=float, default=60.0, help="seconds"
    )
    group.add_argument("--remote-ticket-ttl", type=float, default=120.0, help="seconds")
    group.add_argument(
        "--remote-quarantine-ttl",
        type=float,
        default=600.0,
        help="seconds a timed-out WRITE's pages stay unused if never abandoned",
    )


class _RemoteEndpoint:
    """Remote access and the thread that serves its endpoint.

    The ledger and the remote state must start and end together, so if the
    endpoint thread dies after it started serving, the whole server stops
    (``failed`` is then True).
    """

    def __init__(self, access: "RemoteAccess", stop_event: threading.Event):
        self.access = access
        self.stop_event = stop_event
        self.failed = False
        self.thread: threading.Thread | None = None

    def run(
        self, bind_url: str, ready: threading.Event, on_failure: Callable[[], None]
    ) -> None:
        """Serve the endpoint; on an unexpected error, stop the whole server.

        A failure before the endpoint is bound only sets ``failed``: the
        caller of :func:`_start_remote` is still waiting and reports it.
        """
        from .remote_access import serve_remote

        try:
            serve_remote(self.access, bind_url, stop_event=self.stop_event, ready=ready)
        except Exception:
            logger.exception("Remote endpoint failed; stopping MaruServer")
            self.failed = True
            if ready.is_set():
                on_failure()

    def close(self) -> None:
        """Stop the endpoint thread, then release the remote regions."""
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=5.0)
        self.access.close()


def _start_remote(
    server: MaruServer,
    args: argparse.Namespace,
    on_failure: Callable[[], None],
) -> _RemoteEndpoint:
    """Create remote access and serve its endpoint on a separate thread.

    Args:
        server: The server whose ledger remote access shares.
        args: Parsed command line (the ``--remote-*`` options).
        on_failure: Called from the endpoint thread if it dies after it
            started serving; stops the server.

    Returns:
        The running endpoint.

    Raises:
        RuntimeError: if remote access cannot start or the endpoint does not
            bind within 10 s (everything it set up is released first).
    """
    from maru_remote.transport import NixlTransport

    from .remote_access import RemoteAccess

    transport = NixlTransport(
        f"maru-remote-{args.remote_pool_id}", ucx_device=args.remote_ucx_device
    )
    try:
        access = RemoteAccess(
            server,
            transport,
            pool_size=args.remote_pool_size,
            page_bytes=args.remote_page_bytes,
            pool_id=args.remote_pool_id,
            reservation_ttl_s=args.remote_reservation_ttl,
            ticket_ttl_s=args.remote_ticket_ttl,
            quarantine_ttl_s=args.remote_quarantine_ttl,
            capacity_bytes=args.remote_capacity,
            evict=args.remote_eviction == "lru",
        )
    except Exception:
        transport.close()
        raise
    endpoint = _RemoteEndpoint(access, threading.Event())
    ready = threading.Event()
    endpoint.thread = threading.Thread(
        target=endpoint.run,
        args=(args.remote_bind, ready, on_failure),
        name="maru-remote",
        daemon=True,
    )
    endpoint.thread.start()
    deadline = time.monotonic() + 10.0
    while not ready.wait(timeout=0.05):
        if endpoint.failed or time.monotonic() > deadline:
            endpoint.close()
            raise RuntimeError(f"remote endpoint {args.remote_bind} did not start")
    logger.info("Remote access listening on %s", args.remote_bind)
    return endpoint


def main() -> None:
    """Main entry point for the server."""
    # Import here to avoid circular import
    from .rpc_server import RpcServer

    parser = argparse.ArgumentParser(
        description="MaruServer - Central metadata server for Maru shared memory KV cache"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host to bind the server to (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5555,
        help="Port to bind the server to (default: 5555)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    parser.add_argument(
        "--rm-address",
        type=str,
        default="127.0.0.1:9850",
        help="Resource manager address (host:port, default: 127.0.0.1:9850)",
    )
    parser.add_argument(
        "--dax-path",
        action="append",
        default=None,
        dest="dax_paths",
        help=(
            "DAX device path for allocation (e.g. /dev/dax0.0). "
            "Repeat for multiple pools (fill-first fallback). "
            "If omitted, any available pool is used."
        ),
    )
    parser.add_argument(
        "--cpu-only",
        action="store_true",
        help="Run CPU metadata storage without connecting to a CXL resource manager",
    )
    parser.add_argument(
        "--cpu-capacity-limit",
        type=int,
        default=64 * 1024**3,
        help="Maximum granted CPU pool bytes per node (default: 64 GiB)",
    )
    parser.add_argument(
        "--client-lease-ttl",
        type=float,
        default=DEFAULT_CLIENT_LEASE_TTL,
        help=(
            "Seconds a client may go without renewing its lease before the "
            "regions it allocated are reclaimed; 0 disables leases "
            f"(default: {DEFAULT_CLIENT_LEASE_TTL:g})"
        ),
    )
    _add_remote_arguments(parser)
    args = parser.parse_args()
    if args.client_lease_ttl < 0:
        parser.error("--client-lease-ttl must be >= 0")
    if args.cpu_only and args.dax_paths:
        parser.error("--cpu-only cannot be combined with --dax-path")
    if args.remote_bind:
        if args.cpu_only:
            parser.error("--remote-bind needs CXL; it cannot run with --cpu-only")
        if not args.remote_pool_size or not args.remote_page_bytes:
            parser.error(
                "--remote-bind needs --remote-pool-size and --remote-page-bytes"
            )

    setup_logging(args.log_level)

    # Create server
    server = MaruServer(
        rm_address=args.rm_address,
        dax_paths=args.dax_paths,
        enable_cxl=not args.cpu_only,
        cpu_capacity_limit=args.cpu_capacity_limit,
        client_lease_ttl=args.client_lease_ttl,
    )
    rpc_server = RpcServer(server, host=args.host, port=args.port)
    remote: _RemoteEndpoint | None = None
    stopping = threading.Event()

    # Setup signal handlers (before remote access starts, so a remote
    # endpoint that dies at any point stops the server cleanly)
    def signal_handler(signum, frame):
        stopping.set()
        if remote is not None:
            remote.stop_event.set()
        rpc_server.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        if args.remote_bind:
            remote = _start_remote(
                server, args, lambda: os.kill(os.getpid(), signal.SIGTERM)
            )
        if not stopping.is_set():
            logger.info("Starting MaruServer on %s:%d", args.host, args.port)
            rpc_server.start()
    finally:
        # Shutdown order: the remote endpoint first (no new remote requests),
        # then remote access (NIXL deregistration, unmap, region return),
        # then the server itself.
        if remote is not None:
            remote.close()
        server.close()
    if remote is not None and remote.failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
