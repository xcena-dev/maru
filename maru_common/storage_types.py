# SPDX-License-Identifier: Apache-2.0
"""Storage descriptors and negotiated KV capabilities."""

from dataclasses import dataclass

KV_DELETE_AT_CAPABILITY = "kv_delete_at"
KV_PAGE_GENERATION_CAPABILITY = "kv_page_generation"
KV_PIN_SESSION_CAPABILITY = "kv_pin_session"
STORAGE_CAPABILITY = "cpu_storage_v1"
MIXED_STORAGE_CAPABILITY = "mixed_storage_v1"
MAX_STORAGE_BATCH = 1024


@dataclass(frozen=True)
class StorageLocation:
    """Owner-local allocation identity, never a process virtual address."""

    pool_id: str
    allocation_id: str
    generation: int
    offset: int
    length: int
    medium: str = "cpu"


# Compatibility for the first CPU-only protocol and callers.
CpuLocation = StorageLocation


class StorageError(RuntimeError):
    """A metadata operation was explicitly rejected by the server."""


class StorageUnavailableError(ConnectionError):
    """Outcome unknown: allocation ownership must be retained until resolved."""
