# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Allocation Manager - Server-side memory allocation lifecycle management."""

import logging
from dataclasses import dataclass
from threading import RLock

from maru_common.protocol import ANY_POOL_ID
from maru_shm import MaruHandle, MaruShmClient

logger = logging.getLogger(__name__)


@dataclass
class AllocationInfo:
    """Allocation metadata managed by server."""

    handle: MaruHandle
    owner_instance_id: str
    kv_ref_count: int = 0
    owner_connected: bool = True


class AllocationManager:
    """Manages memory allocation lifecycle."""

    def __init__(self):
        self._client = MaruShmClient()
        self._client._ensure_resource_manager()  # Ensure RM is running at startup
        self._allocations: dict[int, AllocationInfo] = {}  # region_id -> info
        self._lock = RLock()

    def allocate(
        self, instance_id: str, size: int, pool_id: int = ANY_POOL_ID
    ) -> MaruHandle | None:
        """Allocate memory via ShmClient and track ownership."""
        try:
            handle = self._client.alloc(size, pool_id=pool_id)
        except RuntimeError as e:
            logger.warning(
                "alloc failed for instance=%s size=%d pool_id=%s: %s",
                instance_id,
                size,
                pool_id,
                e,
            )
            return None
        if handle is None:
            return None

        with self._lock:
            self._allocations[handle.region_id] = AllocationInfo(
                handle=handle,
                owner_instance_id=instance_id,
                kv_ref_count=0,
                owner_connected=True,
            )
        return handle

    def get_handle(self, region_id: int) -> MaruHandle | None:
        """Get the original handle for an allocation."""
        with self._lock:
            info = self._allocations.get(region_id)
            return info.handle if info else None

    def increment_kv_ref(self, region_id: int) -> bool:
        """Increment KV reference count."""
        with self._lock:
            if region_id not in self._allocations:
                return False
            self._allocations[region_id].kv_ref_count += 1
            return True

    def decrement_kv_ref(self, region_id: int) -> bool:
        """Decrement KV reference count and free if needed."""
        with self._lock:
            if region_id not in self._allocations:
                return False

            info = self._allocations[region_id]
            if info.kv_ref_count <= 0:
                logger.warning(
                    "decrement_kv_ref called on region_id=%d with kv_ref_count=%d",
                    region_id,
                    info.kv_ref_count,
                )
                return False
            info.kv_ref_count -= 1

            if info.kv_ref_count == 0 and not info.owner_connected:
                logger.debug(
                    "[FREE] region_id=%d, owner=%s, "
                    "trigger=decrement_kv_ref (kv_ref_count reached 0, owner disconnected)",
                    region_id,
                    info.owner_instance_id,
                )
                self._client.free(info.handle)
                del self._allocations[region_id]

            return True

    def release(self, instance_id: str, region_id: int) -> bool:
        """Mark allocation as released by owner."""
        with self._lock:
            if region_id not in self._allocations:
                return False

            info = self._allocations[region_id]
            if info.owner_instance_id != instance_id:
                return False

            info.owner_connected = False

            if info.kv_ref_count <= 0:
                logger.info(
                    "[FREE] region_id=%d, owner=%s, "
                    "trigger=release (owner disconnected, kv_ref_count=%d)",
                    region_id,
                    instance_id,
                    info.kv_ref_count,
                )
                self._client.free(info.handle)
                del self._allocations[region_id]
            else:
                logger.info(
                    "[DEFERRED] region_id=%d, owner=%s, "
                    "kv_ref_count=%d (memory kept alive for KV readers)",
                    region_id,
                    instance_id,
                    info.kv_ref_count,
                )

            return True

    def disconnect_client(self, instance_id: str) -> None:
        """Handle client disconnection - release all owned allocations."""
        with self._lock:
            to_free = []
            for region_id, info in self._allocations.items():
                if info.owner_instance_id == instance_id:
                    info.owner_connected = False
                    if info.kv_ref_count <= 0:
                        to_free.append(region_id)

            for region_id in to_free:
                info = self._allocations[region_id]
                logger.info(
                    "[FREE] region_id=%d, owner=%s, "
                    "trigger=disconnect (client disconnected, kv_ref_count=%d)",
                    region_id,
                    instance_id,
                    info.kv_ref_count,
                )
                self._client.free(info.handle)
                del self._allocations[region_id]

    def list_allocations(
        self, exclude_instance_id: str | None = None
    ) -> list[MaruHandle]:
        """Return handles for all active allocations.

        Args:
            exclude_instance_id: If set, exclude allocations owned by
                                 this instance (caller's own regions).

        Returns:
            List of MaruHandle for all (or filtered) active allocations.
        """
        with self._lock:
            handles = []
            for info in self._allocations.values():
                if (
                    exclude_instance_id
                    and info.owner_instance_id == exclude_instance_id
                ):
                    continue
                if not info.owner_connected:
                    continue  # owner disconnected — region may be freed soon
                handles.append(info.handle)
            return handles

    def get_stats(self) -> dict:
        """Get allocation statistics."""
        with self._lock:
            total_allocated = sum(
                info.handle.length for info in self._allocations.values()
            )
            return {
                "num_allocations": len(self._allocations),
                "total_allocated": total_allocated,
                "active_clients": len(
                    {
                        info.owner_instance_id
                        for info in self._allocations.values()
                        if info.owner_connected
                    }
                ),
            }
