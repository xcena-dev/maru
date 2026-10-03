# SPDX-License-Identifier: Apache-2.0
"""Pool-side staging window for a pool on an SSD-backed CXL device.

An SSD-backed CXL memory device (InfiniteMemory) serves reads from its DRAM
cache and fills a missing page from SSD on access. A remote READ of a page
that is only on SSD therefore runs at the on-access fill rate, which is well
below the rate at which the device can fill pages it was asked to load ahead
(``prefetch``).

The stager loads the objects of a request ahead of the worker's reads. When
the vLLM scheduler looks up a request's prefix keys (``exists`` with
``stage``), the stager asks the device to load the first ``window`` objects in
prefix order. Each time the worker reads keys of that request (``lookup``),
the window moves past the keys read, and the next objects are asked for. At
most ``window`` objects per request are asked for ahead of the read position,
so the device DRAM is not flooded with objects that are read much later.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

#: (device address, size) of one stored object, or None for a missing key.
Range = tuple[int, int] | None


@dataclass
class _Group:
    """The prefix objects of one request, in prefix order."""

    keys: list[str]
    ranges: list[Range]
    deadline: float
    read_upto: int = 0  # objects before this index have been read
    staged_upto: int = 0  # objects before this index have been asked for
    index: dict[str, int] = field(default_factory=dict)


class Stager:
    """Ask the device to load each request's objects ahead of its reads."""

    def __init__(
        self,
        window: int,
        prefetch: Callable[[int, int], bool],
        *,
        group_ttl_s: float = 60.0,
        max_groups: int = 4096,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create a stager.

        Args:
            window: Objects per request asked for ahead of the read position.
            prefetch: Asks the device to load ``(address, size)``; returns
                False when the device refused. Must return promptly.
            group_ttl_s: A request's group is forgotten this long after its
                last lookup or read.
            max_groups: Most groups kept; the oldest is dropped beyond this.
            clock: Monotonic time source (tests inject a fake).

        Raises:
            ValueError: if ``window`` is not positive.
        """
        if window <= 0:
            raise ValueError("window must be positive")
        self._window = window
        self._prefetch = prefetch
        self._ttl = group_ttl_s
        self._max_groups = max_groups
        self._clock = clock
        self._groups: dict[tuple[str, ...], _Group] = {}
        self._by_key: dict[str, list[tuple[str, ...]]] = {}
        self.counters = {
            "groups": 0,
            "asked": 0,
            "refused": 0,
            "asked_bytes": 0,
            "expired": 0,
            "evicted": 0,
        }

    def on_lookup(self, keys: list[str], ranges: list[Range]) -> None:
        """Start staging a request whose prefix keys the scheduler looked up.

        The scheduler may look up the same waiting request several times;
        a repeat only extends the group's lifetime.

        Args:
            keys: The request's prefix keys in prefix order.
            ranges: Device range of each key, None for a missing key.
        """
        now = self._clock()
        self._expire(now)
        gid = tuple(keys)
        group = self._groups.get(gid)
        if group is not None:
            group.deadline = now + self._ttl
            return
        # Only the leading run of found keys can be reused (a prefix chain).
        n = 0
        while n < len(ranges) and ranges[n] is not None:
            n += 1
        if n == 0:
            return
        group = _Group(list(keys[:n]), list(ranges[:n]), now + self._ttl)
        group.index = {k: i for i, k in enumerate(group.keys)}
        self._groups[gid] = group
        for k in group.keys:
            self._by_key.setdefault(k, []).append(gid)
        self.counters["groups"] += 1
        self._advance(group)
        while len(self._groups) > self._max_groups:
            self._drop(next(iter(self._groups)))
            self.counters["evicted"] += 1

    def on_read(self, keys: list[str]) -> None:
        """Move the windows of the requests whose keys the worker now reads.

        Args:
            keys: Keys of one worker read (lookup) call.
        """
        now = self._clock()
        moved: dict[tuple[str, ...], int] = {}
        for k in keys:
            for gid in self._by_key.get(k, ()):
                group = self._groups.get(gid)
                if group is None:
                    continue
                i = group.index[k] + 1
                if i > moved.get(gid, 0):
                    moved[gid] = i
        for gid, upto in moved.items():
            group = self._groups[gid]
            group.read_upto = max(group.read_upto, upto)
            group.deadline = now + self._ttl
            self._advance(group)
            if group.read_upto >= len(group.keys):
                self._drop(gid)

    def stats(self) -> dict[str, int]:
        """Counters plus the number of live groups."""
        return {**self.counters, "live_groups": len(self._groups)}

    def _advance(self, group: _Group) -> None:
        """Ask for objects up to ``window`` past the read position."""
        target = min(len(group.keys), group.read_upto + self._window)
        start = max(group.staged_upto, group.read_upto)
        for i in range(start, target):
            rng = group.ranges[i]
            if rng is None:
                continue
            ok = False
            try:
                ok = self._prefetch(*rng)
            except Exception:  # a hint must never fail the request path
                logger.warning("prefetch of %s failed", group.keys[i], exc_info=True)
            self.counters["asked"] += 1
            self.counters["asked_bytes"] += rng[1]
            if not ok:
                self.counters["refused"] += 1
        group.staged_upto = max(group.staged_upto, target)

    def _expire(self, now: float) -> None:
        for gid in [g for g, grp in self._groups.items() if grp.deadline <= now]:
            self._drop(gid)
            self.counters["expired"] += 1

    def _drop(self, gid: tuple[str, ...]) -> None:
        group = self._groups.pop(gid, None)
        if group is None:
            return
        for k in group.keys:
            ids = self._by_key.get(k)
            if ids is None:
                continue
            try:
                ids.remove(gid)
            except ValueError:
                pass
            if not ids:
                del self._by_key[k]


def device_prefetcher(device_id: int) -> Callable[[int, int], bool]:
    """Return a ``prefetch(address, size)`` that issues an asynchronous
    InfiniteMemory prefetch on ``device_id`` through pyxif.

    Raises:
        ImportError: if pyxif is not installed.
    """
    import pyxif  # optional; only pools on InfiniteMemory need it

    def prefetch(address: int, size: int) -> bool:
        return (
            pyxif.memory_prefetch(device_id, address, size)
            == pyxif.MemoryStatus.Success
        )

    return prefetch
