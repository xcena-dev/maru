# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Owns the node's pool mappings."""

import logging
import mmap
import threading

DEFAULT_TRANSFER_BYTES_PER_S = 4 << 30

logger = logging.getLogger(__name__)


class TransferEngine:
    """Thread safe. The engine owns the mappings it is given and closes them."""

    def __init__(self, bytes_per_s: float = DEFAULT_TRANSFER_BYTES_PER_S):
        if bytes_per_s <= 0:
            raise ValueError(f"bytes_per_s must be positive, got {bytes_per_s}")
        self.bytes_per_s = bytes_per_s
        # Guards the owned mappings
        self._lock = threading.Lock()
        self._maps: dict[str, mmap.mmap] = {}

    def add_pool(self, pool_id: str, mapping: mmap.mmap) -> None:
        with self._lock:
            self._maps[pool_id] = mapping

    def has_pool(self, pool_id: str) -> bool:
        """Whether the node owns a mapping of the pool."""
        with self._lock:
            return pool_id in self._maps

    def stop(self) -> None:
        """No transfer threads run."""

    def close(self) -> set[str]:
        """Unmap the pools before their leases are released."""
        with self._lock:
            maps, self._maps = self._maps, {}
        for mapping in maps.values():
            mapping.close()
        return set()
