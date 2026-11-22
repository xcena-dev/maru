# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""CreditCache: unused credit, taken without an RPC. Credit of an
ended StorageService run is dropped, never written."""

import collections
import threading
import time

from maru_common.storage_types import Location


class CreditCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._added = threading.Condition(self._lock)
        self._credits: collections.deque[Location] = collections.deque()
        # Bytes of _credits, kept as it changes: read on every allocation
        self._bytes: int = 0
        self._closed = False
        self.node_epoch: str | None = None

    def reset(self, node_epoch: str) -> list[Location]:
        """Start holding credit of ``node_epoch``; returns what was dropped."""
        with self._lock:
            dropped = list(self._credits) if node_epoch != self.node_epoch else []
            if dropped:
                self._credits.clear()
                self._bytes = 0
            self.node_epoch = node_epoch
            return dropped

    def put(self, credits: list[Location], node_epoch: str) -> bool:
        """Add credit received from ``node_epoch``; refused for another run.
        A page already held is not added twice."""
        with self._lock:
            if node_epoch != self.node_epoch:
                return False
            held = {_page(c) for c in self._credits}
            for credit in credits:
                if _page(credit) not in held:
                    held.add(_page(credit))
                    self._credits.append(credit)
                    self._bytes += credit.length
            self._added.notify_all()
            return True

    def take(self, wait_s: float = 0.0) -> tuple[Location, str] | None:
        """One unused credit with its node epoch, waiting up to ``wait_s``
        for some to come (not once closed)."""
        deadline = time.monotonic() + wait_s
        with self._lock:
            while not self._credits or self.node_epoch is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or self._closed:
                    return None
                self._added.wait(remaining)
            credit = self._credits.popleft()
            self._bytes -= credit.length
            return credit, self.node_epoch

    def take_bytes(self, nbytes: int) -> list[Location]:
        """Unused credit worth up to ``nbytes``, for CREDIT_RETURN."""
        with self._lock:
            out = []
            total = 0
            while self._credits and total + self._credits[0].length <= nbytes:
                credit = self._credits.popleft()
                total += credit.length
                out.append(credit)
            self._bytes -= total
            return out

    def take_all(self) -> list[Location]:
        with self._lock:
            out = list(self._credits)
            self._credits.clear()
            self._bytes = 0
            return out

    def remove_pool(self, pool_id: str) -> list[Location]:
        with self._lock:
            out = [c for c in self._credits if c.pool_id == pool_id]
            self._credits = collections.deque(
                c for c in self._credits if c.pool_id != pool_id
            )
            self._bytes -= sum(c.length for c in out)
            return out

    def close(self) -> None:
        """Wake every waiting take(); later ones do not wait."""
        with self._lock:
            self._closed = True
            self._added.notify_all()

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    @property
    def unused_bytes(self) -> int:
        with self._lock:
            return self._bytes

    def __len__(self) -> int:
        with self._lock:
            return len(self._credits)


def _page(credit: Location) -> tuple[str, int, int]:
    return (credit.pool_id, credit.offset, credit.generation)
