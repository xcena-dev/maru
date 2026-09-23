# SPDX-License-Identifier: Apache-2.0
"""Descriptors for the opt-in storage directory (separate from legacy DAX KV)."""

from dataclasses import dataclass

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
