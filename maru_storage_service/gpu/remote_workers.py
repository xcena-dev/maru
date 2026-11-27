# SPDX-License-Identifier: Apache-2.0
"""Remote worker connections, staging admissions and write domains."""

from dataclasses import dataclass, field

from maru_common.node_protocol import DetachResponse
from maru_common.storage_types import SessionRole, Status, WriteDomain

from .worker_registry import WorkerRecord, WorkerRegistry


@dataclass(eq=False)
class RemoteWorker(WorkerRecord):
    issued: set[WriteDomain] = field(default_factory=set)
    staging_released: bool = False

    @property
    def staging_key(self):
        i = self.ident
        return i.pid, i.pid_start, i.staging_id or i.incarnation


class RemoteWorkers(WorkerRegistry):
    def __init__(self, *, staging_limit, on_terminal, node_id, **kwargs):
        super().__init__(**kwargs)
        self._staging_limit = staging_limit
        self._on_terminal = on_terminal
        self._node_id = node_id
        self._number = 0
        self.domains = {}
        self.admissions = {}

    def attach(self, conn, ident):
        with self._lock:
            if ident.incarnation in self._probes or any(
                ident.incarnation in watch.incarnations
                for watch in self._processes.values()
            ):
                return None, "remote incarnation already attached in this run"
            rec = RemoteWorker(ident, conn)
            if (
                rec.staging_key not in self.admissions
                and len(self.admissions) >= self._staging_limit
            ):
                return None, "worker staging budget exhausted"
            if not self.watch_process(ident.pid, ident.pid_start, rec):
                return None, "the process has ended"
            self.admissions.setdefault(rec.staging_key, set()).add(rec.incarnation)
            return rec, ""

    def begin_op(self, rec, op, digest):
        return self.while_attached(
            rec, lambda: self._ledger.begin(rec.incarnation, op, digest)
        )

    def reserve_domain(self, rec):
        with self._lock:
            if rec.detached or rec.exited:
                return None
            instance = rec.ident.instance
            domain = self.domains.get(instance)
            if domain is None:
                self._number += 1
                domain = WriteDomain(
                    self._node_id, self.node_epoch, instance, self._number
                )
                self.domains[instance] = domain
            return domain

    def grant(self, rec, domain):
        with self._lock:
            if (
                rec.detached
                or rec.exited
                or self.domains.get(domain.instance) != domain
            ):
                return False
            rec.issued.add(domain)
            return True

    def while_current(self, domain, fn):
        with self._lock:
            if self.domains.get(domain.instance) == domain:
                return fn()
            return None

    def withdraw(self, domain):
        with self._lock:
            if self.domains.get(domain.instance) != domain:
                return False
            del self.domains[domain.instance]
            return True

    def withdraw_unissued(self, domain):
        with self._lock:
            if any(domain in worker.issued for worker in self._workers.values()):
                return False
            return self.withdraw(domain)

    def _terminal(self, rec):
        if rec.detached:
            return
        rec.detached = True
        domains = [domain for domain in rec.issued if self.withdraw(domain)]
        rec.issued.clear()
        self._on_terminal(rec, domains)

    def _release_staging(self, rec):
        owners = self.admissions.get(rec.staging_key)
        if owners is not None:
            owners.discard(rec.incarnation)
            if not owners:
                del self.admissions[rec.staging_key]

    def detach(self, rec, conn, staging_released):
        if rec.role == SessionRole.PROBE:
            return super().detach(rec)
        with self._lock:
            if rec.conn is not conn or rec.detached or rec.exited:
                return DetachResponse(Status.REJECTED, error="worker has ended")
            if staging_released:
                rec.staging_released = True
                self._release_staging(rec)
            self._terminal(rec)
            rec.conn = None
            return DetachResponse(Status.OK)

    def after_detach_reply(self, rec):
        # Remote control cleanup checks the credit ledger before forgetting it.
        pass

    def forget_released(self, rec):
        with self._lock:
            if (
                rec.staging_released
                and self._workers.get(rec.incarnation) is rec
                and not self._ledger.running(rec.incarnation)
            ):
                self._workers.pop(rec.incarnation, None)
                self._ledger.drop(rec.incarnation)

    def connection_lost(self, rec, conn):
        if rec.role == SessionRole.PROBE:
            return super().connection_lost(rec, conn)
        with self._lock:
            if rec.conn is conn:
                self._terminal(rec)
                rec.conn = None

    def _mark_exited(self, rec):
        super()._mark_exited(rec)
        self._release_staging(rec)
        self._terminal(rec)
