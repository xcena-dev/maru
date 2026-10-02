# SPDX-License-Identifier: Apache-2.0
"""Local buffers of the remote storage backend: staging slots, allocations,
read leases and the record of a timed-out transfer's buffers."""

from __future__ import annotations

import logging
import mmap
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


def release_view(view: memoryview) -> None:
    """Drop a slot view the caller no longer owns (ignored if still exported)."""
    try:
        view.release()
    except BufferError:  # a caller-held tensor still aliases it
        pass


class StagingBuffer:
    """Fixed-size slots carved from one anonymous mapping."""

    def __init__(self, capacity: int, slot_bytes: int):
        count = capacity // slot_bytes
        if count < 1:
            raise ValueError("remote staging capacity must hold at least one slot")
        self.slot_bytes = slot_bytes
        self.count = count
        self.nbytes = count * slot_bytes
        self._mapping = mmap.mmap(-1, self.nbytes)
        self._view = memoryview(self._mapping)
        from maru_remote.transport import buffer_address

        self.address = buffer_address(self._view)
        self._free: deque[int] = deque(range(count))

    def take(self, n: int = 1) -> list[int] | None:
        """Take ``n`` free slots, or None (and take nothing) if fewer remain."""
        if len(self._free) < n:
            return None
        return [self._free.popleft() for _ in range(n)]

    def give(self, slot: int) -> None:
        """Return one slot."""
        self._free.append(slot)

    def free_count(self) -> int:
        """Number of free slots."""
        return len(self._free)

    def view(self, slot: int) -> memoryview:
        """Writable view of one whole slot."""
        start = slot * self.slot_bytes
        return self._view[start : start + self.slot_bytes]

    def addr(self, slot: int) -> int:
        """Address of the first byte of a slot."""
        return self.address + slot * self.slot_bytes

    def close(self) -> None:
        """Unmap the buffer unless a caller still holds an exported view."""
        try:
            self._view.release()
            self._mapping.close()
        except BufferError:
            logger.warning("remote staging buffer still exported; leaving it mapped")


@dataclass(eq=False)
class RemoteAllocation:
    """A staging slot handed out by ``alloc``; ``buf`` is written by the caller."""

    buf: memoryview
    slot: int
    size: int
    state: str = "writing"  # writing -> submitted (batch_store) -> freed


@dataclass(eq=False)
class RemoteReadLease:
    """Staging bytes of one retrieved key, valid until :meth:`release`.

    Release after the final read or copy (or use a context manager); GC is not
    the synchronization mechanism.
    """

    view: memoryview
    key: str
    _release: Callable[[], None]
    _released: bool = False

    def release(self) -> None:
        """Return the staging slot; idempotent."""
        if self._released:
            return
        self._released = True
        try:
            self.view.release()
        except BufferError:  # a caller-held tensor still aliases the view
            logger.debug("remote lease view of %s still exported at release", self.key)
        self._release()

    def __enter__(self) -> RemoteReadLease:
        return self

    def __exit__(self, *_: Any) -> None:
        self.release()


@dataclass
class QuarantinedTransfer:
    """Buffers a timed-out transfer may still touch."""

    pending: Any
    slots: list[int]
    tickets: list[str] = field(default_factory=list)  # WRITE pages to abandon
