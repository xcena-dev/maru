# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""OpLedger: the receiver side of worker ops, results per origin until acked."""

import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any

from maru_common.storage_types import OpRef

# Unacknowledged ops kept per origin
OP_WINDOW = 65536
# Requests of one connection waiting for the same op (resends)
WAITERS_PER_CONNECTION = 8


class Verdict(Enum):
    NEW = auto()
    REPLAY = auto()
    STALE = auto()
    CONFLICT = auto()
    BUSY = auto()


class LedgerEntry:
    """The result of one op, or the requests waiting for it."""

    def __init__(self, seq: int, digest: bytes):
        self.seq = seq
        self.digest = digest
        self.done = threading.Event()
        self.response: Any = None
        # Waiters per connection; dropped when it closes
        self.waiters: dict[object, list[Callable]] = {}


@dataclass
class _OriginLog:
    ack: int = 0
    entries: dict[int, LedgerEntry] = field(default_factory=dict)
    dropped: bool = False


class OpLedger:
    """Receiver side of ops: results per origin until acked. Thread safe."""

    def __init__(self, window: int = OP_WINDOW):
        self.window = window
        self._lock = threading.Lock()
        self._origins: dict[str, _OriginLog] = {}

    def begin(
        self, origin: str, op: OpRef, digest: bytes
    ) -> tuple[Verdict, LedgerEntry | None]:
        with self._lock:
            log = self._origins.setdefault(origin, _OriginLog())
            if op.ack > log.ack:
                log.ack = op.ack
                for seq in [s for s in log.entries if s <= log.ack]:
                    if log.entries[seq].done.is_set():
                        del log.entries[seq]
            if op.seq <= log.ack:
                return Verdict.STALE, None
            entry = log.entries.get(op.seq)
            if entry is not None:
                if entry.digest != digest:
                    return Verdict.CONFLICT, entry
                return Verdict.REPLAY, entry
            if len(log.entries) >= self.window:
                return Verdict.BUSY, None
            entry = LedgerEntry(op.seq, digest)
            log.entries[op.seq] = entry
            return Verdict.NEW, entry

    def finish(self, origin: str, entry: LedgerEntry, response) -> list[Callable]:
        """Record the result; returns the waiters to answer."""
        with self._lock:
            entry.response = response
            entry.done.set()
            waiters = [w for ws in entry.waiters.values() for w in ws]
            entry.waiters = {}
            log = self._origins.get(origin)
            if log is not None and log.dropped:
                self._drop_if_idle(origin, log)
            return waiters

    def attach(
        self, entry: LedgerEntry, reply: Callable, key: object
    ) -> tuple[bool, Callable | None]:
        """Answer ``reply`` with the result when the entry finishes. Returns
        whether it waits (False: already finished) and, past
        WAITERS_PER_CONNECTION waiters of ``key`` (a connection), the oldest
        one, displaced."""
        with self._lock:
            if entry.done.is_set():
                return False, None
            waiters = entry.waiters.setdefault(key, [])
            waiters.append(reply)
            if len(waiters) > WAITERS_PER_CONNECTION:
                return True, waiters.pop(0)
            return True, None

    def forget_waiter(self, origin: str, key: object) -> None:
        """A connection closed: its waiters will never be answered."""
        with self._lock:
            log = self._origins.get(origin)
            if log is not None:
                for entry in log.entries.values():
                    entry.waiters.pop(key, None)

    def waiters(self, origin: str) -> int:
        with self._lock:
            log = self._origins.get(origin)
            if log is None:
                return 0
            return sum(
                len(ws) for e in log.entries.values() for ws in e.waiters.values()
            )

    def drop(self, origin: str) -> None:
        """The origin ended; drop its log once no op of it runs."""
        with self._lock:
            log = self._origins.get(origin)
            if log is not None:
                log.dropped = True
                self._drop_if_idle(origin, log)

    def finished(self, origin: str, seq: int) -> bool:
        """Whether op ``seq`` of ``origin`` has its result (or was acked)."""
        with self._lock:
            log = self._origins.get(origin)
            if log is None:
                return False
            entry = log.entries.get(seq)
            return seq <= log.ack or (entry is not None and entry.done.is_set())

    def size(self, origin: str) -> int:
        with self._lock:
            log = self._origins.get(origin)
            return len(log.entries) if log else 0

    def running(self, origin: str) -> bool:
        """An op of ``origin`` has not finished."""
        with self._lock:
            log = self._origins.get(origin)
            return log is not None and not all(
                e.done.is_set() for e in log.entries.values()
            )

    def origins(self) -> int:
        with self._lock:
            return len(self._origins)

    def _drop_if_idle(self, origin: str, log: _OriginLog) -> None:
        if all(e.done.is_set() for e in log.entries.values()):
            del self._origins[origin]
