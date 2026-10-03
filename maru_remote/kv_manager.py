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
    """The prefix objects of one request (or one worker read), in order."""

    keys: list[str]
    ranges: list[tuple[int, int]]
    deadline: float
    read: bool = False  # opened for a worker read rather than a lookup hint
    last_read: float = float("-inf")  # last time a worker asked to read it
    consumed_upto: int = 0  # objects before this index were read and released
    held_upto: int = 0  # objects before this index are held (or skipped)
    index: dict[str, int] = field(default_factory=dict)


_Gid = tuple[tuple[str, tuple[int, int]], ...]
#: Stands in for an evicted object's range in the requests that listed it.
_GONE = (-1, 0)
#: A request a worker asked to read within this many seconds is being read.
_ACTIVE_S = 1.0


class KVManager:
    """Hold each request's next objects in device DRAM ahead of its reads.

    Requests come from the scheduler's lookups (hints) and from worker reads.
    Requests a worker is reading are served first; when the budget is short,
    hinted windows no worker reads yet are let go to make room.
    """

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
            executor: Runs the device pin and unpin calls; its owner closes it.
            max_held_bytes: Upper bound on bytes held at once, below the
                device's pin limit.
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
        self._reqs: dict[_Gid, _Req] = {}
        self._by_key: dict[str, list[_Gid]] = {}
        self._objs: dict[tuple[int, int], _Obj] = {}
        self._held_bytes = 0
        # First time each still-unready read (by its keys) was asked about.
        self._waiting_since: dict[tuple[str, ...], float] = {}
        self.counters: dict[str, int] = {
            "requests": 0,
            "read_requests": 0,
            "pinned": 0,
            "pinned_bytes": 0,
            "pin_failures": 0,
            "unpinned": 0,
            "expired": 0,
            "dropped": 0,
            "yielded": 0,
            "forgotten": 0,
            "waits": 0,
        }

    # ---- hooks called by the server --------------------------------------------

    def on_lookup(self, keys: list[str], ranges: list[Range]) -> None:
        """Open (or refresh) the request whose prefix is ``keys`` (a hint).

        Args:
            keys: The request's prefix keys in order, as the scheduler asked.
            ranges: Device range per key, None for a missing key.
        """
        with self._lock:
            now = self._clock()
            self._expire(now)
            self._open(keys, ranges, now, read=False)
            self._pump(now)

    def ready(self, keys: list[str], ranges: list[Range]) -> bool:
        """Whether every found key of one worker read is staged.

        The read's keys are held at their current ranges: requests that cover
        them move their window to the read, and keys no request holds there
        (never hinted, already read and let go by another reader, or stored
        again elsewhere) get a request of their own.

        Args:
            keys: Keys of one worker read, in prefix order.
            ranges: Device range per key, None for a missing key.

        Returns:
            True when the read may go ahead.
        """
        with self._lock:
            now = self._clock()
            self._expire(now)
            found = [(k, r) for k, r in zip(keys, ranges, strict=True) if r is not None]
            self._skip_to_reads(found, now)
            self._pump(now)
            if any(r not in self._objs for _, r in found):
                self._open([k for k, _ in found], [r for _, r in found], now, read=True)
                self._pump(now)
            for _, r in found:
                obj = self._objs.get(r)
                if obj is None or obj.state == "filling":
                    self.counters["waits"] += 1
                    self._note_wait(keys, ranges, now)
                    return False
            self._waiting_since.pop(tuple(keys), None)
            return True

    def on_consumed(self, keys: list[str]) -> None:
        """Let go of the objects a worker has read and move the windows on.

        Args:
            keys: Keys of one released read ticket.
        """
        with self._lock:
            now = self._clock()
            moved: dict[_Gid, int] = {}
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
                self._consume(req, upto)
                req.deadline = now + self._ttl
                if req.consumed_upto >= len(req.keys):
                    self._drop(gid)
            self._pump(now)

    def forget_range(self, rng: tuple[int, int]) -> None:
        """Let a device range go now: its key is gone and the page may be reused.

        Args:
            rng: Device (address, size) of an evicted object.
        """
        with self._lock:
            for req in self._reqs.values():  # nobody may let go of it again
                for j, r in enumerate(req.ranges):
                    if r == rng:
                        req.ranges[j] = _GONE
            obj = self._objs.get(rng)
            if obj is None:
                return
            self.counters["forgotten"] += 1
            obj.refs = 0
            self._release(rng, obj)  # a filling object is unpinned when its pin ends

    def busy(self) -> bool:
        """Whether an object is still being loaded for a read or a hint."""
        with self._lock:
            return any(o.state == "filling" for o in self._objs.values())

    def stats(self) -> dict[str, int]:
        """Counters plus live requests and bytes held."""
        with self._lock:
            return {
                **self.counters,
                "live_requests": len(self._reqs),
                "held_bytes": self._held_bytes,
            }

    def close(self) -> None:
        """Let every object go (the executor's owner then closes it)."""
        with self._lock:
            for gid in list(self._reqs):
                self._drop(gid)

    # ---- internals (lock held) -------------------------------------------------

    def _open(
        self, keys: list[str], ranges: list[Range], now: float, *, read: bool
    ) -> None:
        n = 0  # only the leading run of found keys is a reusable prefix
        while n < len(ranges) and ranges[n] is not None:
            n += 1
        if n == 0:
            return
        found = [(k, r) for k, r in zip(keys[:n], ranges[:n], strict=True)]
        gid: _Gid = tuple(found)  # type: ignore[arg-type]
        req = self._reqs.get(gid)
        if req is not None:
            req.deadline = now + self._ttl
            if read:
                req.last_read = now
                if req.consumed_upto:  # read again from the start (another reader)
                    for j in range(req.consumed_upto, req.held_upto):
                        self._unref(req.ranges[j])
                    req.consumed_upto = req.held_upto = 0
            return
        req = _Req(
            [k for k, _ in found],
            [r for _, r in found],  # type: ignore[misc]
            now + self._ttl,
            read=read,
            last_read=now if read else float("-inf"),
        )
        req.index = {k: i for i, k in enumerate(req.keys)}
        self._reqs[gid] = req
        for k in req.keys:
            self._by_key.setdefault(k, []).append(gid)
        self.counters["read_requests" if read else "requests"] += 1
        while len(self._reqs) > self._max_groups:
            self._drop(next(iter(self._reqs)))
            self.counters["dropped"] += 1

    def _skip_to_reads(
        self, found: list[tuple[str, tuple[int, int]]], now: float
    ) -> None:
        """Mark requests that hold the read's keys active; move their windows there.

        A worker that starts past the window (the GPU already held the first
        chunks) will not read the objects before it: let them go.
        """
        first: dict[_Gid, int] = {}
        for k, r in found:
            for gid in self._by_key.get(k, ()):
                req = self._reqs[gid]
                i = req.index[k]
                if req.ranges[i] != r or i < req.consumed_upto:
                    continue  # stale range, or already read: another request serves it
                if i < first.get(gid, len(req.keys)):
                    first[gid] = i
        for gid, i in first.items():
            req = self._reqs[gid]
            req.last_read = now
            req.deadline = now + self._ttl
            self._consume(req, i)

    def _consume(self, req: _Req, upto: int) -> None:
        """Move ``req``'s read position to ``upto``, letting go of what it passes."""
        if upto <= req.consumed_upto:
            return
        for j in range(req.consumed_upto, min(upto, req.held_upto)):
            self._unref(req.ranges[j])
        req.consumed_upto = upto
        req.held_upto = max(req.held_upto, upto)

    def _pump(self, now: float) -> None:
        """Fill windows: reads first, then hints, oldest first.

        A request a worker is reading takes the room of hinted windows nobody
        reads; a worker read's own request is held whatever the budget (one
        per load in flight, so it stays small). Hints wait for room in order.
        """
        active = [g for g, r in self._reqs.items() if now - r.last_read < _ACTIVE_S]
        idle = [g for g, r in self._reqs.items() if now - r.last_read >= _ACTIVE_S]
        for gid in active + idle:
            req = self._reqs.get(gid)
            if req is None:
                continue
            is_active = now - req.last_read < _ACTIVE_S
            # A read's own request holds the whole read, however long it is.
            span = len(req.keys) if req.read else self._window
            target = min(len(req.keys), req.consumed_upto + span)
            while req.held_upto < target:
                rng = req.ranges[req.held_upto]
                if rng == _GONE:
                    req.held_upto += 1
                    continue
                fits = rng in self._objs or self._held_bytes + rng[1] <= self._max_held
                if not fits and not (req.read and is_active):
                    if not is_active or not self._yield_idle(rng[1], now):
                        if is_active:
                            break  # this read waits; other reads may still fit
                        return  # hints wait for room in order
                self._ref(rng)
                req.held_upto += 1

    def _yield_idle(self, need: int, now: float) -> bool:
        """Let go of idle hinted windows (newest first) until ``need`` bytes fit."""
        idle = [g for g, r in self._reqs.items() if now - r.last_read >= _ACTIVE_S]
        for gid in reversed(idle):
            if self._held_bytes + need <= self._max_held:
                break
            req = self._reqs[gid]
            if req.held_upto > req.consumed_upto:
                for j in range(req.consumed_upto, req.held_upto):
                    self._unref(req.ranges[j])
                req.held_upto = req.consumed_upto
                self.counters["yielded"] += 1
        return self._held_bytes + need <= self._max_held

    def _ref(self, rng: tuple[int, int]) -> None:
        obj = self._objs.get(rng)
        if obj is None:
            obj = _Obj(rng[1])
            self._objs[rng] = obj
            self._held_bytes += rng[1]
            self._exec.pin(
                rng[0], rng[1], lambda ok, r=rng, o=obj: self._on_pinned(r, o, ok)
            )
        obj.refs += 1

    def _unref(self, rng: tuple[int, int]) -> None:
        obj = self._objs.get(rng)
        if obj is None:
            return
        obj.refs -= 1
        if obj.refs > 0 or obj.state == "filling":
            return  # a filling object is let go when its pin completes
        self._release(rng, obj)

    def _release(self, rng: tuple[int, int], obj: _Obj) -> None:
        if self._objs.get(rng) is obj:
            del self._objs[rng]
            self._held_bytes -= obj.size
        if obj.state == "ready":
            self._exec.unpin(rng[0], rng[1])
            self.counters["unpinned"] += 1

    def _on_pinned(self, rng: tuple[int, int], obj: _Obj, ok: bool) -> None:
        with self._lock:
            if self._objs.get(rng) is not obj:
                # Let go while it was loading (dropped or its key evicted).
                # Unpin unless a newer object holds the same range now.
                if ok and rng not in self._objs:
                    self._exec.unpin(rng[0], rng[1])
                    self.counters["unpinned"] += 1
                return
            obj.state = "ready" if ok else "failed"
            if ok:
                self.counters["pinned"] += 1
                self.counters["pinned_bytes"] += obj.size
            else:
                self.counters["pin_failures"] += 1
            if obj.refs <= 0:
                self._release(rng, obj)
            self._pump(self._clock())

    def _note_wait(self, keys: list[str], ranges: list[Range], now: float) -> None:
        """Log, once, the state behind a read that has waited over a second."""
        tid = tuple(keys)
        since = self._waiting_since.setdefault(tid, now)
        if now - since < 1.0 or since < 0:
            return
        self._waiting_since[tid] = -now  # logged (negative: time of logging)
        parts = []
        for k, r in zip(keys, ranges, strict=True):
            obj = self._objs.get(r) if r is not None else None
            reqs = [
                (
                    len(q.keys),
                    q.index[k],
                    q.consumed_upto,
                    q.held_upto,
                    now - q.last_read < _ACTIVE_S,
                )
                for q in (self._reqs[g] for g in self._by_key.get(k, ()))
            ]
            parts.append(
                f"{k[-12:]}: obj={None if obj is None else (obj.state, obj.refs)} "
                f"requests(len,idx,consumed,held,active)={reqs}"
            )
        logger.warning(
            "read of %d keys not staged after %.1f s; held %d/%d bytes, %d objects "
            "filling, %d requests: %s",
            len(keys),
            now - since,
            self._held_bytes,
            self._max_held,
            sum(1 for o in self._objs.values() if o.state == "filling"),
            len(self._reqs),
            "; ".join(parts),
        )

    def _expire(self, now: float) -> None:
        for gid in [g for g, r in self._reqs.items() if r.deadline <= now]:
            self._drop(gid)
            self.counters["expired"] += 1
        for tid in [t for t, s in self._waiting_since.items() if now - abs(s) > 60.0]:
            del self._waiting_since[tid]

    def _drop(self, gid: _Gid) -> None:
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

    calls = {
        "pin": pyxif.memory_pin,
        "unpin": pyxif.memory_unpin,
        "prefetch": pyxif.memory_prefetch_sync,
    }
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
        self._closed = False
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

    def prefetch(self, address: int, size: int, done: Callable[[bool], None]) -> None:
        """Load a range into device DRAM without holding it; ``done(ok)`` after."""
        self._submit("prefetch", address, size, done)

    def close(self, timeout_s: float = 5.0) -> None:
        """Let queued calls finish for up to ``timeout_s``, then stop the processes.

        Calls submitted while waiting (unpins of pins that complete now) are
        waited for too. Idempotent.
        """
        if self._closed:
            return
        self._closed = True
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            with self._lock:
                if not self._callbacks:
                    break
            time.sleep(0.01)
        for _ in self._procs:
            self._tasks.put(None)
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
                done = self._callbacks.get(token)
            if not ok and done is None:
                logger.warning("device unpin failed")
            if done is not None:
                try:
                    done(ok)  # may submit an unpin; close() waits for it too
                except Exception:
                    logger.exception("pin completion handler failed")
            with self._lock:  # registered until handled, so close() waits
                self._callbacks.pop(token, None)
