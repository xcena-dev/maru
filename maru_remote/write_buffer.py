# SPDX-License-Identifier: Apache-2.0
"""Free pool pages loaded into device DRAM ahead of the stores that use them.

On an SSD-backed CXL device a write to a page that is not in device DRAM first
reads the page's old content from SSD: a host write then runs at a fifth of
its speed and its SSD reads slow every read of the device. The write buffer
keeps a bounded number of free pages loaded and pinned (so the reads' own
loads cannot push them out), loads more only while no read is being staged,
and hands out only loaded pages to stores; the server unpins a page when it
hands it out. A store that
finds the buffer empty is refused and skipped by the worker, so stores never
add SSD reads to the read path.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable
from typing import Any


class WriteBuffer:
    """Bounded set of loaded free pages; the server allocates, this tracks."""

    def __init__(
        self,
        pages: int,
        refill_batch: int = 2,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create a buffer of at most ``pages`` loaded pages.

        Args:
            pages: Loaded (or loading) free pages to keep.
            refill_batch: Pages started at once while refilling.
            clock: Monotonic clock (tests pass a fake one).

        Raises:
            ValueError: if ``pages`` or ``refill_batch`` is not positive.
        """
        if pages <= 0 or refill_batch <= 0:
            raise ValueError("pages and refill_batch must be positive")
        self._pages = pages
        self.capacity = pages
        self._batch = refill_batch
        self._clock = clock
        self._quiet_until = 0.0  # no refills before this (after a failed load)
        self._lock = threading.Lock()
        self._ready: deque[Any] = deque()
        self._loading: set[int] = set()  # id() of handles being loaded
        self._closed = False
        self._orphans: list[Any] = []
        # Set when a load ends or a page is handed out: time to refill.
        self.changed = threading.Event()
        self.counters: dict[str, int] = {
            "loaded": 0,
            "load_failures": 0,
            "handed_out": 0,
            "refused": 0,
        }

    def want(self) -> int:
        """Pages to start loading now (at most one refill batch).

        None for a second after a failed load: each refill may evict a cached
        object to get a page, so a load that keeps failing must not spin.
        """
        with self._lock:
            if self._clock() < self._quiet_until:
                return 0
            missing = self._pages - len(self._ready) - len(self._loading)
            return max(0, min(missing, self._batch - len(self._loading)))

    def loading(
        self, handle: Any, rng: tuple[int, int] | None
    ) -> Callable[[bool], None]:
        """Record ``handle`` (device range ``rng``) as loading; return its callback."""
        with self._lock:
            self._loading.add(id(handle))

        def done(ok: bool) -> None:
            with self._lock:
                self._loading.discard(id(handle))
                if ok and not self._closed:
                    self._ready.append((handle, rng))
                    self.counters["loaded"] += 1
                else:
                    if not ok:
                        self.counters["load_failures"] += 1
                        self._quiet_until = self._clock() + 1.0
                    # freed (and unpinned if it was loaded) by the server's thread
                    self._orphans.append((handle, rng if ok else None))
            self.changed.set()

        return done

    def take(self, n: int) -> list[tuple[Any, tuple[int, int] | None]] | None:
        """Hand out ``n`` loaded pages with their pinned ranges, or None (refused)."""
        with self._lock:
            if len(self._ready) < n:
                self.counters["refused"] += 1
                return None
            self.counters["handed_out"] += n
            out = [self._ready.popleft() for _ in range(n)]
        self.changed.set()
        return out

    def drain_orphans(self) -> list[tuple[Any, tuple[int, int] | None]]:
        """Pages whose load failed or ended after close, with the range to unpin
        (None when nothing is pinned); the caller frees them."""
        with self._lock:
            out, self._orphans = self._orphans, []
            return out

    def close(self) -> list[tuple[Any, tuple[int, int] | None]]:
        """Stop accepting loads; return loaded pages and their pinned ranges."""
        with self._lock:
            self._closed = True
            out = list(self._ready) + self._orphans
            self._ready.clear()
            self._orphans = []
            return out

    def stats(self) -> dict[str, int]:
        """Counters plus ready and loading pages."""
        with self._lock:
            return {
                **self.counters,
                "ready": len(self._ready),
                "loading": len(self._loading),
            }
