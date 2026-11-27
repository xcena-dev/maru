# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Per-origin replay records of state-changing requests (see OpRef).

Not thread safe: callers hold the service lock.
"""

import heapq
import threading
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from maru_common.storage_types import OpRef

DEFAULT_OP_WINDOW = 65536
DEFAULT_OP_BYTES_MAX = 64 << 20

# ("node", node_epoch) or ("worker", incarnation): kinds never collide
OriginKey = tuple[str, str]


class Verdict(Enum):
    NEW = "new"  # run it, then finish() or abort()
    REPLAY = "replay"  # answer with record.result
    WAIT = "wait"  # same op still running
    STALE = "stale"  # seq <= ack
    REJECTED = "rejected"  # same seq, other digest
    BUSY = "busy"  # op_window records or op_bytes_max not acknowledged


@dataclass(eq=False)
class OpRecord:
    seq: int
    digest: bytes
    done: bool = False
    result: Any = None
    size: int = 0  # estimated bytes of result
    event: threading.Event = field(default_factory=threading.Event)


@dataclass
class _Origin:
    owner: str  # node_id that may drop it (WORKER_EXIT)
    ack: int = 0
    bytes: int = 0
    records: dict[int, OpRecord] = field(default_factory=dict)
    heap: list[int] = field(default_factory=list)  # seqs, to prune in order


class OpLedger:
    def __init__(
        self,
        op_window: int = DEFAULT_OP_WINDOW,
        op_bytes_max: int = DEFAULT_OP_BYTES_MAX,
    ):
        if op_window < 1 or op_bytes_max < 1:
            raise ValueError("op_window and op_bytes_max must be at least 1")
        self.op_window = op_window
        self.op_bytes_max = op_bytes_max
        self._origins: dict[OriginKey, _Origin] = {}
        # dropped origins whose records sweep() still has to free
        self._dropped: deque[_Origin] = deque()

    def begin(
        self, origin: OriginKey, op: OpRef, owner: str = ""
    ) -> tuple[Verdict, OpRecord | None]:
        state = self._origins.get(origin)
        if state is None:
            state = self._origins[origin] = _Origin(owner=owner)
        if op.ack > state.ack:
            state.ack = op.ack
            self._prune(state)
        if op.seq <= state.ack:
            return Verdict.STALE, None
        record = state.records.get(op.seq)
        if record is not None:
            if record.digest != op.digest:
                return Verdict.REJECTED, record
            return (Verdict.REPLAY if record.done else Verdict.WAIT), record
        if len(state.records) >= self.op_window or state.bytes >= self.op_bytes_max:
            return Verdict.BUSY, None
        record = OpRecord(seq=op.seq, digest=op.digest)
        state.records[op.seq] = record
        heapq.heappush(state.heap, op.seq)
        return Verdict.NEW, record

    def finish(
        self, origin: OriginKey, record: OpRecord, result: Any, size: int
    ) -> None:
        record.result = result
        record.done = True
        state = self._origins.get(origin)
        if state is not None and state.records.get(record.seq) is record:
            record.size = size
            state.bytes += size
        record.event.set()

    def abort(self, origin: OriginKey, record: OpRecord) -> None:
        """Forget a record whose run changed nothing."""
        state = self._origins.get(origin)
        if state is not None and state.records.get(record.seq) is record:
            del state.records[record.seq]
            state.bytes -= record.size
        record.event.set()

    def drop_origin(self, origin: OriginKey, owner: str | None = None) -> bool:
        """The run behind ``origin`` ended; with ``owner``, only if it owns it.
        Its records are freed by sweep()."""
        state = self._origins.get(origin)
        if state is None or (owner is not None and state.owner != owner):
            return False
        del self._origins[origin]
        if state.records:
            self._dropped.append(state)
        return True

    def sweep(self, budget: int) -> int:
        """Free at most ``budget`` records of dropped origins; returns how many."""
        done = 0
        while self._dropped and done < budget:
            records = self._dropped[0].records
            while records and done < budget:
                records.popitem()[1].event.set()
                done += 1
            if not records:
                self._dropped.popleft()
        return done

    def pending(self, origin: OriginKey) -> int:
        state = self._origins.get(origin)
        return len(state.records) if state else 0

    def stored_bytes(self, origin: OriginKey) -> int:
        state = self._origins.get(origin)
        return state.bytes if state else 0

    def ack(self, origin: OriginKey) -> int:
        state = self._origins.get(origin)
        return state.ack if state else 0

    def __contains__(self, origin: OriginKey) -> bool:
        return origin in self._origins

    @staticmethod
    def _prune(state: _Origin) -> None:
        while state.heap and state.heap[0] <= state.ack:
            record = state.records.pop(heapq.heappop(state.heap), None)
            if record is not None:
                state.bytes -= record.size
                record.event.set()
