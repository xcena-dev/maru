# SPDX-License-Identifier: Apache-2.0
"""Per-request prefetch windows that a worker reads only once they are staged.

An SSD-backed CXL memory device (InfiniteMemory) fills a page that is only on
SSD when it is accessed, at a rate well below the rate at which it fills pages
it is asked to load ahead. The KV Manager keeps, for each request, the next
``window`` objects of its prefix being loaded into device DRAM and held there
(``pin``). A worker that asks to read with ``wait_staged`` gets the objects'
locations only once all of them are staged, and after it has read them and
released its read ticket the objects are let go (``unpin``) and the window
moves on. The next objects therefore load while the worker reads the current
ones, so the SSD fill is hidden behind the RDMA read.

Device calls block their caller for the whole fill, so they run in an executor
outside the server's request loop; completions come back on executor threads
and every state change happens under one lock.
"""

from __future__ import annotations

import itertools
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

logger = logging.getLogger(__name__)

#: (device address, size) of one stored object, or None for a missing key.
Range = tuple[int, int] | None


class PinExecutor(Protocol):
    """Runs device pin/unpin calls off the request loop."""

    def pin(self, address: int, size: int, done: Callable[[bool], None]) -> None:
        """Load ``[address, address + size)`` and hold it; call ``done(ok)``."""

    def unpin(self, address: int, size: int) -> None:
        """Let a pinned range go (fire and forget)."""

    def close(self) -> None:
        """Stop the executor."""


@dataclass
class _Obj:
    """One object held for one or more requests."""

    size: int
    state: str = "filling"  # filling -> ready | failed
    refs: int = 0  # requests whose window holds it


@dataclass
class _Req:
    """The prefix objects of one request, in prefix order."""

    keys: list[str]
    ranges: list[Range]
    deadline: float
    consumed_upto: int = 0  # objects before this index were read and released
    held_upto: int = 0  # objects before this index are held (or skipped)
    index: dict[str, int] = field(default_factory=dict)


class KVManager:
    """Hold each request's next objects in device DRAM ahead of its reads."""

    def __init__(
        self,
        window: int,
        executor: PinExecutor,
        *,
        max_held_bytes: int,
        group_ttl_s: float = 60.0,
        max_groups: int = 4096,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create a KV Manager.

        Args:
            window: Objects per request held ahead of its read position.
            executor: Runs the device pin and unpin calls.
            max_held_bytes: Upper bound on bytes held at once, below the
                device's pin limit; requests beyond it wait for room.
            group_ttl_s: A request not looked up or read for this long is
                dropped and its objects let go.
            max_groups: Requests tracked at most; the oldest is dropped first.
            clock: Monotonic clock (tests pass a fake one).

        Raises:
            ValueError: if ``window`` or ``max_held_bytes`` is not positive.
        """
        if window <= 0:
            raise ValueError("window must be positive")
        if max_held_bytes <= 0:
            raise ValueError("max_held_bytes must be positive")
        self._window = window
        self._exec = executor
        self._max_held = max_held_bytes
        self._ttl = group_ttl_s
        self._max_groups = max_groups
        self._clock = clock
        self._lock = threading.Lock()
        self._reqs: dict[tuple[str, ...], _Req] = {}
        self._by_key: dict[str, list[tuple[str, ...]]] = {}
        self._objs: dict[tuple[int, int], _Obj] = {}
        self._held_bytes = 0
        self.counters: dict[str, int] = {
            "requests": 0,
            "pinned": 0,
            "pinned_bytes": 0,
            "pin_failures": 0,
            "unpinned": 0,
            "expired": 0,
            "dropped": 0,
            "waits": 0,
        }

    # ---- hooks called by the server --------------------------------------------

    def on_lookup(self, keys: list[str], ranges: list[Range]) -> None:
        """Open (or refresh) the request whose prefix is ``keys``.

        Args:
            keys: The request's prefix keys in order, as the scheduler asked.
            ranges: Device range per key, None for a missing key.
        """
        with self._lock:
            now = self._clock()
            self._expire(now)
            self._open(keys, ranges, now)
            self._pump()

    def ready(self, keys: list[str], ranges: list[Range]) -> bool:
        """Whether every found key of one worker read is staged.

        Keys that no request holds open a request of their own, so a read
        without a scheduler lookup is still loaded ahead.

        Args:
            keys: Keys of one worker read, in prefix order.
            ranges: Device range per key, None for a missing key.

        Returns:
            True when the read may go ahead.
        """
        with self._lock:
            now = self._clock()
            self._expire(now)
            if any(
                r is not None and k not in self._by_key
                for k, r in zip(keys, ranges, strict=True)
            ):
                self._open(keys, ranges, now)
            self._pump()
            for r in ranges:
                if r is None:
                    continue
                obj = self._objs.get(r)
                if obj is None or obj.state == "filling":
                    self.counters["waits"] += 1
                    return False
            return True

    def on_consumed(self, keys: list[str]) -> None:
        """Let go of the objects a worker has read and move the windows on.

        Args:
            keys: Keys of one released read ticket.
        """
        with self._lock:
            now = self._clock()
            moved: dict[tuple[str, ...], int] = {}
            for k in keys:
                for gid in self._by_key.get(k, ()):
                    req = self._reqs.get(gid)
                    if req is None:
                        continue
                    i = req.index[k] + 1
                    if i > moved.get(gid, 0):
                        moved[gid] = i
            for gid, upto in moved.items():
                req = self._reqs[gid]
                for i in range(req.consumed_upto, min(upto, req.held_upto)):
                    self._unref(req.ranges[i])
                req.consumed_upto = max(req.consumed_upto, upto)
                req.held_upto = max(req.held_upto, req.consumed_upto)
                req.deadline = now + self._ttl
                if req.consumed_upto >= len(req.keys):
                    self._drop(gid)
            self._pump()

    def stats(self) -> dict[str, int]:
        """Counters plus live requests and bytes held."""
        with self._lock:
            return {
                **self.counters,
                "live_requests": len(self._reqs),
                "held_bytes": self._held_bytes,
            }

    def close(self) -> None:
        """Let every object go and stop the executor."""
        with self._lock:
            for gid in list(self._reqs):
                self._drop(gid)
        self._exec.close()

    # ---- internals (lock held) -------------------------------------------------

    def _open(self, keys: list[str], ranges: list[Range], now: float) -> None:
        gid = tuple(keys)
        req = self._reqs.get(gid)
        if req is not None:
            req.deadline = now + self._ttl
            return
        n = 0  # only the leading run of found keys is a reusable prefix
        while n < len(ranges) and ranges[n] is not None:
            n += 1
        if n == 0:
            return
        req = _Req(list(keys[:n]), list(ranges[:n]), now + self._ttl)
        req.index = {k: i for i, k in enumerate(req.keys)}
        self._reqs[gid] = req
        for k in req.keys:
            self._by_key.setdefault(k, []).append(gid)
        self.counters["requests"] += 1
        while len(self._reqs) > self._max_groups:
            self._drop(next(iter(self._reqs)))
            self.counters["dropped"] += 1

    def _pump(self) -> None:
        """Fill every request's window, oldest request first, within the budget."""
        for req in self._reqs.values():
            target = min(len(req.keys), req.consumed_upto + self._window)
            while req.held_upto < target:
                rng = req.ranges[req.held_upto]
                assert rng is not None
                obj = self._objs.get(rng)
                if obj is None and self._held_bytes + rng[1] > self._max_held:
                    return  # no room; later requests wait too (FIFO)
                self._ref(rng)
                req.held_upto += 1

    def _ref(self, rng: tuple[int, int]) -> None:
        obj = self._objs.get(rng)
        if obj is None:
            obj = _Obj(rng[1])
            self._objs[rng] = obj
            self._held_bytes += rng[1]
            self._exec.pin(rng[0], rng[1], lambda ok, r=rng: self._on_pinned(r, ok))
        obj.refs += 1

    def _unref(self, rng: Range) -> None:
        if rng is None:
            return
        obj = self._objs.get(rng)
        if obj is None:
            return
        obj.refs -= 1
        if obj.refs > 0 or obj.state == "filling":
            return  # a filling object is let go when its pin completes
        self._release(rng, obj)

    def _release(self, rng: tuple[int, int], obj: _Obj) -> None:
        del self._objs[rng]
        self._held_bytes -= obj.size
        if obj.state == "ready":
            self._exec.unpin(rng[0], rng[1])
            self.counters["unpinned"] += 1

    def _on_pinned(self, rng: tuple[int, int], ok: bool) -> None:
        with self._lock:
            obj = self._objs.get(rng)
            if obj is None:  # cannot happen: filling objects stay until here
                if ok:
                    self._exec.unpin(rng[0], rng[1])
                return
            obj.state = "ready" if ok else "failed"
            if ok:
                self.counters["pinned"] += 1
                self.counters["pinned_bytes"] += obj.size
            else:
                self.counters["pin_failures"] += 1
            if obj.refs == 0:
                self._release(rng, obj)
                self._pump()

    def _expire(self, now: float) -> None:
        for gid in [g for g, r in self._reqs.items() if r.deadline <= now]:
            self._drop(gid)
            self.counters["expired"] += 1

    def _drop(self, gid: tuple[str, ...]) -> None:
        req = self._reqs.pop(gid, None)
        if req is None:
            return
        for i in range(req.consumed_upto, req.held_upto):
            self._unref(req.ranges[i])
        for k in req.keys:
            ids = self._by_key.get(k)
            if ids is None:
                continue
            try:
                ids.remove(gid)
            except ValueError:
                pass
            if not ids:
                del self._by_key[k]


def _pin_worker(device_id: int, tasks: Any, results: Any) -> None:
    """Run device pin/unpin calls from ``tasks`` until a None arrives."""
    import signal

    signal.signal(signal.SIGINT, signal.SIG_IGN)  # the server decides when to stop
    import pyxif

    calls = {"pin": pyxif.memory_pin, "unpin": pyxif.memory_unpin}
    while True:
        item = tasks.get()
        if item is None:
            return
        op, token, address, size = item
        try:
            ok = calls[op](device_id, address, size) == pyxif.MemoryStatus.Success
        except Exception:
            ok = False
        results.put((token, ok))


class ProcessPinExecutor:
    """Device pin/unpin in worker processes (device calls hold the GIL).

    A fixed set of processes takes calls from one queue; a thread of the
    server delivers their results. A process that dies is not replaced: its
    call never completes and the reads waiting on it go ahead after their
    wait limit.
    """

    def __init__(self, device_id: int, processes: int) -> None:
        """Start ``processes`` worker processes for ``device_id``.

        Raises:
            ValueError: if ``processes`` is not positive.
        """
        if processes <= 0:
            raise ValueError("processes must be positive")
        import multiprocessing as mp

        ctx = mp.get_context("spawn")
        self._tasks = ctx.Queue()
        self._results = ctx.Queue()
        self._procs = [
            ctx.Process(
                target=_pin_worker,
                args=(device_id, self._tasks, self._results),
                name=f"maru-remote-pin-{i}",
                daemon=True,
            )
            for i in range(processes)
        ]
        for proc in self._procs:
            proc.start()
        self._lock = threading.Lock()
        self._callbacks: dict[int, Callable[[bool], None] | None] = {}
        self._next = itertools.count()
        self._reader = threading.Thread(
            target=self._read_results, name="maru-remote-pin-results", daemon=True
        )
        self._reader.start()

    def pin(self, address: int, size: int, done: Callable[[bool], None]) -> None:
        """Pin in a worker process; ``done(ok)`` runs on the result thread."""
        self._submit("pin", address, size, done)

    def unpin(self, address: int, size: int) -> None:
        """Unpin in a worker process."""
        self._submit("unpin", address, size, None)

    def close(self, timeout_s: float = 5.0) -> None:
        """Let queued calls finish for up to ``timeout_s``, then stop the processes."""
        for _ in self._procs:
            self._tasks.put(None)
        deadline = time.monotonic() + timeout_s
        for proc in self._procs:
            proc.join(timeout=max(0.0, deadline - time.monotonic()))
        for proc in self._procs:
            if proc.is_alive():
                logger.warning("pin worker %s did not stop; killing it", proc.name)
                proc.kill()
                proc.join()
        self._results.put(None)
        self._reader.join(timeout=1.0)

    def _submit(
        self, op: str, address: int, size: int, done: Callable[[bool], None] | None
    ) -> None:
        with self._lock:
            token = next(self._next)
            self._callbacks[token] = done
        self._tasks.put((op, token, address, size))

    def _read_results(self) -> None:
        while True:
            item = self._results.get()
            if item is None:
                return
            token, ok = item
            with self._lock:
                done = self._callbacks.pop(token, None)
            if not ok and done is None:
                logger.warning("device unpin failed")
            if done is not None:
                try:
                    done(ok)
                except Exception:
                    logger.exception("pin completion handler failed")
