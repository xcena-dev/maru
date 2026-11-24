# SPDX-License-Identifier: Apache-2.0
"""Retained Catalog pins follow process evidence across GPU node runs."""

import logging
import threading

from maru_common.node_protocol import process_identity
from maru_common.storage_types import WorkerRef

from .worker_registry import process_key

logger = logging.getLogger(__name__)


class OrphanWatch:
    def __init__(self, store, boot):
        self.store, self.boot = store, boot
        self.pid_ns = process_identity()[3]
        self.lock = threading.Lock()
        self.pending = {}
        self.by_process = {}
        self.unknown = set()
        self.unwatchable = set()

    def start(self, orphans):
        with self.lock:
            self.pending = {o.incarnation: o for o in orphans}
            for key, orphan in self.pending.copy().items():
                if (
                    self.boot
                    and orphan.host_boot_id
                    and orphan.host_boot_id != self.boot
                ):
                    self._report_exit(self.pending.pop(key))
                elif orphan.host_boot_id != self.boot or orphan.pid_ns != self.pid_ns:
                    self.unwatchable.add(key)
                    logger.error("orphan %s has no comparable process identity", key)
                else:
                    process = process_key(orphan.pid, orphan.pid_start)
                    self.by_process.setdefault(process, []).append(key)
                    self.unknown.add(key)
        self.retry()

    def retry(self):
        with self.lock:
            if not self.unknown:
                return
            for process, keys in list(self.by_process.items()):
                if keys[0] not in self.unknown:
                    continue
                orphan = self.pending[keys[0]]
                evidence = self.store.workers.watch_process(
                    orphan.pid, orphan.pid_start
                )
                if evidence is False:
                    self._process_exited(process)
                elif evidence is True:
                    self.unknown.difference_update(keys)

    def on_process_exit(self, process):
        with self.lock:
            self._process_exited(process)

    def _process_exited(self, process):
        reported = self.store.workers.on_process_exit(process)
        keys = self.by_process.pop(process, [])
        self.unknown.difference_update(keys)
        for key in keys:
            orphan = self.pending.pop(key)
            if key not in reported:
                self.store.workers.after_gpu_drain(self._report_exit, orphan)

    def _report_exit(self, orphan):
        self.store._queue_exit(
            WorkerRef("", 0, 0, orphan.pid, orphan.pid_start, orphan.incarnation)
        )
