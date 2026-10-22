# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""KV Manager implementation for managing KV metadata."""

import enum
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
    pin_count: int = 0  # Pin count for eviction protection
    generation: int | None = None


@dataclass
class _PageState:
    """Latest generation at an owned page; key=None marks it retired."""

    generation: int
    key: str | None


class DeleteResult(enum.Enum):
    """Result of a KV delete operation."""

    NOT_FOUND = "not_found"
    PINNED = "pinned"
    DELETED = "deleted"


class KVManager:
    """
    Manages KV metadata - mapping from chunk hash to shared memory location.

    Responsibilities:
    - Store KV location metadata (hash -> location)
    - Track reference counts for each KV entry
    - Thread-safe operations
    """

    def __init__(self):
        self._store: dict[str, KVEntry] = {}
        self._lock = RLock()
        self._pages: dict[int, dict[int, _PageState]] = {}

    def register(
        self, key: str, region_id: int, kv_offset: int, kv_length: int
    ) -> tuple[bool, int | None]:
        """
        Register a KV entry.

        Args:
            key: The chunk key string
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
            logger.debug("Registered KV: key=%s, region_id=%d", key, region_id)
            return (True, region_id)  # New entry, need to increment alloc ref

    def register_at(
        self, key: str, region_id: int, kv_offset: int, kv_length: int, generation: int
    ) -> tuple[str, int | None]:
        """Register an allocation generation without resurrecting retired pages.

        Replaying the same live registration is REGISTERED without another ref.
        A key registered elsewhere is DUPLICATE; a retired or older allocation
        is RETIRED. CONFLICT retains ownership when the page is still in use.
        """
        with self._lock:
            pages = self._pages.setdefault(region_id, {})
            page = pages.get(kv_offset)
            if page is not None:
                if generation < page.generation or (
                    generation == page.generation and page.key is None
                ):
                    return "RETIRED", None
                if page.key is not None:
                    if (
                        generation == page.generation
                        and page.key == key
                        and self._store[key].kv_length == kv_length
                    ):
                        return "REGISTERED", None
                    return "CONFLICT", None
            if key in self._store:
                # This allocation was never published; retire it so a replay
                # cannot publish it after the client has freed the duplicate.
                pages[kv_offset] = _PageState(generation, None)
                return "DUPLICATE", None
            self._store[key] = KVEntry(
                region_id, kv_offset, kv_length, generation=generation
            )
            pages[kv_offset] = _PageState(generation, key)
            return "REGISTERED", region_id

    def forget_region(self, region_id: int) -> None:
        """Discard page generations after the underlying allocation is released."""
        with self._lock:
            self._pages.pop(region_id, None)

    def lookup(self, key: str) -> KVEntry | None:
        """
        Lookup a KV entry by its key.

        Args:
            key: The chunk key string

        Returns:
            KVEntry if found, None otherwise
        """
        with self._lock:
            return self._store.get(key)

    def exists(self, key: str) -> bool:
        """Check if a KV entry exists."""
        with self._lock:
            return key in self._store

    def pin(self, key: str) -> bool:
        """Check if a KV entry exists and pin it atomically.

        Returns:
            True if key exists (and was pinned), False otherwise.
        """
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return False
            entry.pin_count += 1
            logger.debug("Pinned KV: key=%s, pin_count=%d", key, entry.pin_count)
            return True

    def unpin(self, key: str) -> bool:
        """Decrement pin_count for a KV entry.

        Returns:
            True if successfully unpinned, False if key not found or not pinned.
        """
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                logger.warning("Unpin failed: key=%s not found", key)
                return False
            if entry.pin_count <= 0:
                logger.warning("Unpin failed: key=%s pin_count already 0", key)
                return False
            entry.pin_count -= 1
            logger.debug("Unpinned KV: key=%s, pin_count=%d", key, entry.pin_count)
            return True

    def delete(self, key: str) -> tuple[DeleteResult, int | None]:
        """
        Delete a KV entry.

        Returns:
            (result, region_id_to_decrement)
            - (NOT_FOUND, None): key didn't exist
            - (PINNED, None): key exists but pinned, deletion refused
            - (DELETED, region_id): entry deleted, allocation ref needs decrement
        """
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return (DeleteResult.NOT_FOUND, None)

            if entry.pin_count > 0:
                logger.warning(
                    "Delete refused: key=%s is pinned (pin_count=%d)",
                    key,
                    entry.pin_count,
                )
                return (DeleteResult.PINNED, None)

            self._retire(entry)
            region_id = self._store.pop(key).region_id
            logger.debug("Deleted KV: key=%s, region_id=%d", key, region_id)
            return (DeleteResult.DELETED, region_id)

    def delete_at(
        self, key: str, region_id: int, kv_offset: int, generation: int | None = None
    ) -> tuple[str, int | None]:
        """Delete only the expected unpinned registration.

        A versioned ABSENT also retires that page generation, rejecting any
        register that arrives later. Older deletes never affect a newer page.
        """
        with self._lock:
            entry = self._store.get(key)
            matches = entry is not None and (
                entry.region_id,
                entry.kv_offset,
                entry.generation,
            ) == (region_id, kv_offset, generation)
            if generation is not None:
                page = self._pages.get(region_id, {}).get(kv_offset)
                if page is not None:
                    if page.generation > generation:
                        return "ABSENT", None
                    if page.key is not None and not matches:
                        raise ValueError("Page belongs to another live registration")
                if (
                    entry is not None
                    and (entry.region_id, entry.kv_offset) == (region_id, kv_offset)
                    and not matches
                ):
                    raise ValueError("Page generation does not match registration")
                if not matches:
                    self._pages.setdefault(region_id, {})[kv_offset] = _PageState(
                        generation, None
                    )
                    return "ABSENT", None
            elif entry is None or (entry.region_id, entry.kv_offset) != (
                region_id,
                kv_offset,
            ):
                return "ABSENT", None
            if entry.pin_count:
                return "PINNED", None
            self._retire(entry)
            del self._store[key]
            return "DELETED", region_id

    def _retire(self, entry: KVEntry) -> None:
        if entry.generation is not None:
            self._pages.setdefault(entry.region_id, {})[entry.kv_offset] = _PageState(
                entry.generation, None
            )

    def get_stats(self) -> dict:
        """Get KV statistics."""
        with self._lock:
            return {
                "total_entries": len(self._store),
                "total_size": sum(e.kv_length for e in self._store.values()),
            }

    def used_by_region(self) -> dict[int, int]:
        """Sum of live KV bytes per region.

        Returns:
            Mapping of region_id -> total kv_length of entries stored there.
            Used for per-instance usage accounting (joined with region owners).
        """
        with self._lock:
            out: dict[int, int] = {}
            for entry in self._store.values():
                out[entry.region_id] = out.get(entry.region_id, 0) + entry.kv_length
            return out

    # =========================================================================
    # Batch Operations
    # =========================================================================

    def batch_lookup(self, keys: list[str]) -> list[KVEntry | None]:
        """
        Lookup multiple KV entries in a single operation.

        Args:
            keys: List of chunk key strings

        Returns:
            List of KVEntry or None for each key
        """
        with self._lock:
            return [self._store.get(key) for key in keys]

    def batch_exists(self, keys: list[str]) -> list[bool]:
        """
        Check existence of multiple KV entries in a single operation.

        Checks ALL keys unconditionally (no prefix-stop).
        For prefix-stop with pinning, use batch_pin().

        Args:
            keys: List of chunk key strings

        Returns:
            List of booleans indicating if each key exists
        """
        with self._lock:
            return [key in self._store for key in keys]

    def batch_pin(self, keys: list[str]) -> list[bool]:
        """Check existence and pin prefix-contiguous KV entries atomically.

        Uses prefix-stop: stops at the first miss, only pinning the
        contiguous prefix of existing keys. This avoids pin leaks —
        if all existing keys were pinned, the caller would need to
        unpin non-prefix keys it doesn't use.

        Unlike batch_exists() which checks ALL keys, this method
        intentionally stops early because pinning has side effects.

        Returns:
            List of booleans — True if key exists (and was pinned).
            After first False, remaining entries are all False.
        """
        with self._lock:
            results = []
            for key in keys:
                entry = self._store.get(key)
                if entry is None:
                    # First miss: fill rest with False and stop
                    results.extend([False] * (len(keys) - len(results)))
                    break
                entry.pin_count += 1
                results.append(True)
            return results

    def batch_unpin(self, keys: list[str]) -> list[bool]:
        """Unpin multiple KV entries.

        Returns:
            List of booleans — True if successfully unpinned.
        """
        with self._lock:
            results = []
            for key in keys:
                entry = self._store.get(key)
                if entry is None or entry.pin_count <= 0:
                    results.append(False)
                else:
                    entry.pin_count -= 1
                    results.append(True)
            return results

    # TODO: Add pin timeout monitor (PinMonitor) when eviction is implemented.
    # Track _pin_timestamps per key, run a periodic check_pin_timeouts() in a
    # daemon thread to force-unpin entries that exceed a TTL. This prevents
    # pin leaks when clients crash without sending unpin RPCs.
