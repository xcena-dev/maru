# SPDX-License-Identifier: Apache-2.0
"""Retained Catalog pins follow process evidence across GPU node runs."""

import logging
import threading

from maru_common.node_protocol import process_identity
from maru_common.storage_types import WorkerRef

from .peer_watcher import PeerWatcher

logger = logging.getLogger(__name__)


class OrphanWatch:
    def __init__(self, store, boot):
        self.store, self.boot = store, boot
        self.pid_ns = process_identity()[3]
        self.watcher = PeerWatcher("maru-orphan-watcher")
        self.lock = threading.RLock()
        self.pending = {}
        self.unknown = set()
        self.unwatchable = set()

    def start(self, orphans):
        with self.lock:
            self.pending = {o.incarnation: o for o in orphans}
            self.unknown = set(self.pending)
        self.watcher.start(self._exited)
        self.retry()

    def retry(self):
        with self.lock:
            for key in list(self.unknown):
                orphan = self.pending[key]
                if (
                    self.boot
                    and orphan.host_boot_id
                    and orphan.host_boot_id != self.boot
                ):
                    self._exited(key)
                elif orphan.host_boot_id != self.boot or orphan.pid_ns != self.pid_ns:
                    self.unknown.remove(key)
                    self.unwatchable.add(key)
                    logger.error("orphan %s has no comparable process identity", key)
                else:
                    evidence = self.watcher.watch(key, orphan.pid, orphan.pid_start)
                    if evidence is False:
                        self._exited(key)
                    elif evidence is True:
                        self.unknown.remove(key)

    def _exited(self, key):
        with self.lock:
            orphan = self.pending.pop(key, None)
            self.unknown.discard(key)
        if orphan is not None:
            self.store._queue_exit(
                WorkerRef(
                    "",
                    0,
                    0,
                    orphan.pid,
                    orphan.pid_start,
                    orphan.incarnation,
                )
            )

    def stop(self):
        self.watcher.stop()
