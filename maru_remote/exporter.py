# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Whole-pool NIC registrations; mappings outlive revocation."""

from __future__ import annotations

import logging
import mmap
import threading
from dataclasses import dataclass
from typing import Any

from .transport import NixlTransport, buffer_address

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Export:
    """A registered pool: ``base`` is the address of its byte 0 here."""

    pool_id: str
    base: int
    nbytes: int
    metadata: bytes  # partial NIXL metadata: this range and the connection


class NixlExporter:
    """Thread safe. One per node run, on the run's transport."""

    def __init__(self, transport: NixlTransport) -> None:
        self.transport = transport
        self._lock = threading.Lock()
        # Every registration this exporter holds, usable or not
        self._registrations: dict[str, Any] = {}
        # The usable ones: peers may be told about them
        self._exports: dict[str, Export] = {}

    def attach(self, pool_id: str, memory: mmap.mmap) -> None:
        """Register the whole mapping. Raises if the pool is registered
        already or the NIC refuses it; a registration it could not undo
        stays held (unusable) until detach ends it."""
        base, nbytes = buffer_address(memory), len(memory)
        with self._lock:
            if pool_id in self._registrations:
                raise ValueError(f"{pool_id} is already registered")
            registration = self.transport.register(base, nbytes, tag=pool_id)
            self._registrations[pool_id] = registration
            try:
                metadata = self.transport.partial_metadata(registration)
            except BaseException:
                self.transport.deregister(registration)  # if this raises, held
                del self._registrations[pool_id]
                raise
            self._exports[pool_id] = Export(pool_id, base, nbytes, metadata)
        logger.info("registered %s (%d bytes) with the NIC", pool_id, nbytes)

    def detach(self, pool_id: str) -> None:
        """Deregister; a pool not registered is a no-op. If this raises,
        the pool stays registered and must stay mapped."""
        with self._lock:
            registration = self._registrations.get(pool_id)
            if registration is None:
                return
            self._exports.pop(pool_id, None)  # never told to peers again
            self.transport.deregister(registration)
            del self._registrations[pool_id]
        logger.info("deregistered %s from the NIC", pool_id)

    def export(self, pool_id: str) -> Export | None:
        """The pool's usable registration, or None."""
        with self._lock:
            return self._exports.get(pool_id)

    def exports(self) -> list[Export]:
        """Every usable registration, in the order registered."""
        with self._lock:
            return list(self._exports.values())
