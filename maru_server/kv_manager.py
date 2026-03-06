# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""KV Manager implementation for managing KV metadata."""

import logging
from dataclasses import dataclass
from threading import RLock

logger = logging.getLogger(__name__)


@dataclass
class KVEntry:
    """KV entry referencing an allocation."""

    region_id: int  # Region ID (allocation identifier)
    kv_offset: int  # Offset within allocation (relative to handle.offset)
    kv_length: int  # Size of KV data


class KVManager:
    """
    Manages KV metadata - mapping from chunk hash to shared memory location.

    Responsibilities:
    - Store KV location metadata (hash -> location)
    - Track reference counts for each KV entry
    - Thread-safe operations
    """

    def __init__(self):
        self._store: dict[int, KVEntry] = {}
        self._lock = RLock()

    def register(
        self, key: int, region_id: int, kv_offset: int, kv_length: int
    ) -> tuple[bool, int | None]:
        """
        Register a KV entry.

        Args:
            key: The chunk hash
            region_id: Region ID of the allocation
            kv_offset: Offset within the allocation
            kv_length: Size of the KV data

        Returns:
            (is_new, region_id_to_increment) - region_id if new entry created
        """
        with self._lock:
            if key in self._store:
                # Key already exists — skip (idempotent)
                return (False, None)

            self._store[key] = KVEntry(
                region_id=region_id,
                kv_offset=kv_offset,
                kv_length=kv_length,
            )
            logger.debug("Registered KV: key=%d, region_id=%d", key, region_id)
            return (True, region_id)  # New entry, need to increment alloc ref

    def lookup(self, key: int) -> KVEntry | None:
        """
        Lookup a KV entry by its key.

        Args:
            key: The chunk hash

        Returns:
            KVEntry if found, None otherwise
        """
        with self._lock:
            return self._store.get(key)

    def exists(self, key: int) -> bool:
        """Check if a KV entry exists."""
        with self._lock:
            return key in self._store

    def delete(self, key: int) -> tuple[bool, int | None]:
        """
        Delete a KV entry.

        Returns:
            (key_existed, region_id_to_decrement)
            - (False, None): key didn't exist
            - (True, region_id): entry deleted, allocation ref needs decrement
        """
        with self._lock:
            if key not in self._store:
                return (False, None)

            region_id = self._store.pop(key).region_id
            logger.debug("Deleted KV: key=%d, region_id=%d", key, region_id)
            return (True, region_id)

    def get_stats(self) -> dict:
        """Get KV statistics."""
        with self._lock:
            return {
                "total_entries": len(self._store),
                "total_size": sum(e.kv_length for e in self._store.values()),
            }

    # =========================================================================
    # Batch Operations
    # =========================================================================

    def batch_register(self, entries: list[tuple[int, int, int, int]]) -> list[bool]:
        """
        Register multiple KV entries in a single operation.

        Args:
            entries: List of (key, region_id, kv_offset, kv_length) tuples

        Returns:
            List of booleans indicating if each entry was newly registered
        """
        results = []
        with self._lock:
            for key, region_id, kv_offset, kv_length in entries:
                if key in self._store:
                    # Duplicate key — skip
                    results.append(False)
                else:
                    self._store[key] = KVEntry(
                        region_id=region_id,
                        kv_offset=kv_offset,
                        kv_length=kv_length,
                    )
                    results.append(True)
        return results

    def batch_lookup(self, keys: list[int]) -> list[KVEntry | None]:
        """
        Lookup multiple KV entries in a single operation.

        Args:
            keys: List of chunk hashes

        Returns:
            List of KVEntry or None for each key
        """
        with self._lock:
            return [self._store.get(key) for key in keys]

    def batch_exists(self, keys: list[int]) -> list[bool]:
        """
        Check existence of multiple KV entries in a single operation.

        Args:
            keys: List of chunk hashes

        Returns:
            List of booleans indicating if each key exists
        """
        with self._lock:
            return [key in self._store for key in keys]
