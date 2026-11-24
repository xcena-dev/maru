# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""WorkerRegistry: the workers attached to this node.

A record lives from ATTACH until the process exits, or until a DETACH that
leaves nothing behind. An exit quarantines the worker's credited pages for the
GPU drain grace, and is reported through ``on_exit`` once the grace is over:
until then MaruServer keeps the worker's pins, so no page it may still read
on its GPU (a fetched copy too) is retired and reused.
"""

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TypeVar, cast

from maru_common.node_protocol import DetachResponse
from maru_common.storage_types import SessionRole, Status, WorkerRef

from ..op_ledger import OpLedger
from ..pool_manager import PoolManager, holder_tag
from ..ports import ProcessWatcher, WorkerEndpoint

logger = logging.getLogger("maru_storage_service.worker_registry")

R = TypeVar("R")


@dataclass
class AttachIdentity:
    """A checked worker; pid and pid_start in this node's pid namespace."""

    incarnation: str
    engine_id: str
    dp_index: int
    tp_rank: int
    pid: int
    pid_start: int
    namespace: str
    instance: str = ""
    staging_id: str = ""


@dataclass(eq=False)
class WorkerRecord:
    ident: AttachIdentity
    conn: WorkerEndpoint | None = None
    worker_session: str | None = None
    detached: bool = False
    exited: bool = False
    # A probe holds nothing and is forgotten with its connection
    role: SessionRole = SessionRole.WORKER
    # ATTACH capabilities agreed with this worker
    capabilities: frozenset[str] = frozenset()

    @property
    def incarnation(self) -> str:
        return self.ident.incarnation

    def ref(self) -> WorkerRef:
        i = self.ident
        return WorkerRef(
            engine_id=i.engine_id,
            dp_index=i.dp_index,
            tp_rank=i.tp_rank,
            pid=i.pid,
            pid_start=i.pid_start,
            incarnation=i.incarnation,
            worker_session=self.worker_session,
        )


@dataclass
class _ProcessWatch:
    incarnations: set[str] = field(default_factory=set)
    has_orphans: bool = False


def process_key(pid: int, pid_start: int) -> str:
    return f"process:{pid}:{pid_start}"


class WorkerRegistry:
    """Worker records and their lifecycle. Thread safe; ``on_exit`` is called
    without the lock held."""

    def __init__(
        self,
        *,
        node_epoch: str,
        pools: PoolManager,
        ledger: OpLedger,
        watcher: ProcessWatcher,
        on_exit: Callable[[WorkerRef], None],
        gpu_drain_grace_s: float,
        on_settled: Callable[[str], None] = lambda incarnation: None,
    ):
        self.node_epoch = node_epoch
        self.gpu_drain_grace_s = gpu_drain_grace_s
        self._pools = pools
        self._ledger = ledger
        self._watcher = watcher
        self._on_exit = on_exit
        # A worker whose exit is confirmed and whose GPU drain grace is over:
        # nothing of it touches the pools any more, so its region holds can go
        self._on_settled = on_settled
        # incarnation -> when its exit's GPU drain grace ends
        self._settles_at: dict[str, float] = {}
        self._lock = threading.RLock()
        self._workers: dict[str, WorkerRecord] = {}
        self._probes: dict[str, WorkerRecord] = {}
        self._processes: dict[str, _ProcessWatch] = {}

        self._stopping = False
        self._timers: set[threading.Timer] = set()

    def stop(self) -> None:
        """Cancel the grace timers; no new one starts."""
        with self._lock:
            self._stopping = True
            timers = list(self._timers)
            self._timers.clear()
        for timer in timers:
            timer.cancel()

    # ------------------------------------------------------- attach, detach

    def watch_process(
        self, pid: int, pid_start: int, worker: WorkerRecord | None = None
    ) -> bool | None:
        """Share a pidfd with the process's workers and retained orphans."""
        key = process_key(pid, pid_start)
        with self._lock:
            watch = self._processes.get(key)
            if watch is None:
                evidence = self._watcher.watch(key, pid, pid_start)
                if not evidence:
                    return evidence
                watch = self._processes[key] = _ProcessWatch()
            elif not self._watcher.watching(key):
                return False
            if worker is None:
                watch.has_orphans = True
            else:
                # A pending pidfd callback must find the worker even if ATTACH fails.
                watch.incarnations.add(worker.incarnation)
                self._workers[worker.incarnation] = worker
            return self._watcher.watching(key)

    def attach(
        self, conn: WorkerEndpoint, ident: AttachIdentity
    ) -> tuple[WorkerRecord | None, str]:
        """Register or re-bind an attaching worker; (None, reason) to refuse."""
        with self._lock:
            if ident.incarnation in self._probes:
                return None, "the incarnation is attached as a probe"
            rec = self._workers.get(ident.incarnation)
            if rec is not None:
                return self._rebind(rec, conn, ident)
            rec = WorkerRecord(ident=ident, conn=conn)
            if not self.watch_process(ident.pid, ident.pid_start, rec):
                return None, "the process has ended"
            return rec, ""

    def attach_probe(
        self, conn: WorkerEndpoint, ident: AttachIdentity
    ) -> tuple[WorkerRecord | None, str]:
        """Record a probe (a scheduler): no pidfd or credit; it is
        forgotten when its connection closes."""
        with self._lock:
            if ident.incarnation in self._workers:
                return None, "the incarnation is attached as a worker"
            rec = self._probes.get(ident.incarnation)
            if rec is not None:
                return self._rebind(rec, conn, ident)
            rec = WorkerRecord(ident=ident, conn=conn, role=SessionRole.PROBE)
            self._probes[ident.incarnation] = rec
            return rec, ""

    def probes(self) -> list[str]:
        """Incarnations of the attached probes."""
        with self._lock:
            return list(self._probes)

    def _forget_probe(self, rec: WorkerRecord, conn: WorkerEndpoint | None) -> None:
        """Caller holds the lock; ``conn`` None forgets it whatever its link."""
        if self._probes.get(rec.incarnation) is rec and (
            conn is None or rec.conn is conn
        ):
            del self._probes[rec.incarnation]
            rec.conn = None
            rec.detached = True

    def _rebind(
        self, rec: WorkerRecord, conn: WorkerEndpoint, ident: AttachIdentity
    ) -> tuple[WorkerRecord | None, str]:
        """Caller holds the lock. The same incarnation attaches again."""
        if rec.exited:
            return None, "this incarnation has ended"
        if rec.ident != ident:
            return None, (
                f"the incarnation is attached with another identity (pid {rec.ident.pid})"
            )
        old_conn, rec.conn = rec.conn, conn
        rec.detached = False
        if old_conn is not None and old_conn is not conn:
            old_conn.close()  # only shuts the socket down; no lock taken
        return rec, ""

    def connection_lost(self, rec: WorkerRecord, conn: WorkerEndpoint) -> None:
        """The UDS closed; the credit stays until the process exits. A probe
        is forgotten."""
        with self._lock:
            if rec.role == SessionRole.PROBE:
                self._forget_probe(rec, conn)
                return
            if rec.conn is conn:
                rec.conn = None
        self._ledger.forget_waiter(rec.incarnation, conn)

    def detach(
        self, rec: WorkerRecord, conn=None, staging_released=False
    ) -> DetachResponse:
        with self._lock:
            if rec.role == SessionRole.PROBE:
                self._forget_probe(rec, None)
                return DetachResponse(status=Status.OK)
            rec.detached = True
            held = self._pools.held_bytes(rec.incarnation)
        if held:
            logger.warning(
                "worker %s detached holding %d bytes of credit; they wait for its exit",
                rec.incarnation,
                held,
            )
        return DetachResponse(status=Status.OK)

    def after_detach_reply(self, rec: WorkerRecord) -> None:
        """Forget a worker that detached holding nothing: its record, pidfd
        and op records go. One with credit or an op running waits for its
        exit."""
        incarnation = rec.incarnation
        with self._lock:
            if (
                not rec.detached
                or rec.exited
                or self._workers.get(incarnation) is not rec
                or self._ledger.running(incarnation)
                or not self._pools.forget_holder(incarnation)
            ):
                return
            del self._workers[incarnation]
            self._settles_at[incarnation] = 0.0  # it maps nothing any more
            # Under the lock, so a re-attach of the incarnation (which takes
            # it) never loses its new watch or op records; neither the
            # watcher nor the ledger calls back while holding its own lock
            key = process_key(rec.ident.pid, rec.ident.pid_start)
            watch = self._processes[key]
            watch.incarnations.remove(incarnation)
            if not watch.incarnations and not watch.has_orphans:
                self._watcher.unwatch(key)
                del self._processes[key]
            self._ledger.drop(incarnation)
        logger.info("worker %s detached and is forgotten", incarnation)

    def get(self, incarnation: str) -> WorkerRecord | None:
        with self._lock:
            return self._workers.get(incarnation)

    def endpoint(self, incarnation: str) -> WorkerEndpoint | None:
        """The connection of an attached worker, if it has one."""
        with self._lock:
            rec = self._workers.get(incarnation)
            return rec.conn if rec is not None else None

    def begin_op(self, rec, op, digest):
        return self._ledger.begin(rec.incarnation, op, digest)

    def while_attached(self, rec: WorkerRecord, fn: Callable[[], R]) -> R | None:
        """``fn()`` under the lock, or None if ``rec`` ended or detached. An
        exit sets ``exited`` under the lock before it quarantines, so credit
        given in ``fn`` never goes to a holder that ended; a detached record
        may be forgotten (after_detach_reply). ``fn`` may only touch the pool
        manager and this registry: taking the manager lock or doing I/O in it
        would invert the lock order."""
        with self._lock:
            if rec.exited or rec.detached:
                return None
            return fn()

    # ------------------------------------------------------------------ exit

    def on_process_exit(self, key: str) -> set[str]:
        """Incarnations whose exit report follows their GPU drain grace."""
        with self._lock:
            watch = self._processes.get(key)
            if watch is None:
                return set()
            incarnations = watch.incarnations.intersection(self._workers)
        reported = {inc for inc in incarnations if self._worker_exited(inc, key)}
        with self._lock:
            self._processes.pop(key, None)
        return reported

    def _worker_exited(self, incarnation: str, key: str | None = None) -> bool:
        with self._lock:
            rec = self._workers.get(incarnation)
            if rec is None or rec.exited:
                return False
            if (
                key is not None
                and process_key(rec.ident.pid, rec.ident.pid_start) != key
            ):
                return False
            del self._workers[incarnation]
            self._mark_exited(rec)
            self._settles_at[incarnation] = time.monotonic() + self.gpu_drain_grace_s
            conn, rec.conn = rec.conn, None
            refs = self._pools.quarantine_holder(incarnation)
        if conn is not None:
            conn.close()
        logger.info(
            "worker %s (pid %d) ended: %d credited pages quarantined for %.1f s",
            incarnation,
            rec.ident.pid,
            len(refs),
            self.gpu_drain_grace_s,
        )
        self._after(self.gpu_drain_grace_s, self._grace_over, incarnation, rec.ref())
        self._ledger.drop(incarnation)
        return True

    def _mark_exited(self, rec):
        rec.exited = True

    def _grace_over(self, incarnation: str, ref: WorkerRef) -> None:
        self._on_exit(ref)  # its pins at MaruServer go now
        freed = self._pools.release_quarantine(holder_tag(incarnation))
        if freed:
            logger.info("freed %d pages of ended worker %s", freed, incarnation)
        self._on_settled(incarnation)

    def settled(self, incarnation: str) -> bool:
        """Detached, or ended with its GPU drain grace over: it maps and
        copies nothing of this run any more."""
        with self._lock:
            rec = self._workers.get(incarnation)
            if rec is not None and not rec.exited:
                return rec.detached
            ends = self._settles_at.get(incarnation)
        return ends is not None and time.monotonic() >= ends

    def _after(self, delay: float, fn: Callable, *args) -> None:
        def run() -> None:
            # The running timer, not a closure cell: no cycle with it
            timer = cast(threading.Timer, threading.current_thread())
            with self._lock:
                self._timers.discard(timer)
                if self._stopping:
                    return
            fn(*args)

        timer = threading.Timer(delay, run)
        timer.daemon = True
        with self._lock:
            if self._stopping:
                return
            self._timers.add(timer)
        timer.start()

    def count(self, pred: Callable[[WorkerRecord], bool]) -> int:
        """Attached workers, neither detached nor ended, that ``pred`` takes."""
        with self._lock:
            return sum(
                1
                for rec in self._workers.values()
                if not (rec.exited or rec.detached) and pred(rec)
            )

    def mapping(self) -> dict[str, WorkerEndpoint | None]:
        """Workers of this run that may map its pools, with their connection
        (None while it dropped): attached and neither detached nor ended."""
        with self._lock:
            return {
                inc: rec.conn
                for inc, rec in self._workers.items()
                if not rec.exited and not rec.detached
            }
