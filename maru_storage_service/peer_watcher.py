# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""PeerWatcher: pidfd per worker process.

A process is (pid, pid_start); the start time is read again after
pidfd_open so a reused pid is never mistaken for it.
"""

import logging
import os
import select
import threading
from collections.abc import Callable

from maru_common.node_protocol import process_start_time

logger = logging.getLogger(__name__)


class PeerWatcher:
    """Calls ``on_exit(key)``, given to start(), once when a watched process
    ends."""

    def __init__(self, name: str = "maru-peer-watcher"):
        self._on_exit: Callable[[str], None] = lambda key: None  # set by start()
        self._lock = threading.Lock()
        self._fds: dict[str, int] = {}
        self._wake_r, self._wake_w = os.pipe2(os.O_CLOEXEC | os.O_NONBLOCK)
        self._stopped = False
        self._thread = threading.Thread(target=self._loop, name=name, daemon=True)

    def start(self, on_exit: Callable[[str], None]) -> None:
        self._on_exit = on_exit
        self._thread.start()

    def stop(self) -> None:
        with self._lock:
            if self._stopped:
                return
            self._stopped = True
            fds = list(self._fds.values())
            self._fds.clear()
        self._wake()
        if self._thread.is_alive():
            self._thread.join(timeout=5.0)
        for fd in fds:
            os.close(fd)
        os.close(self._wake_r)
        os.close(self._wake_w)

    def watch(self, key: str, pid: int, pid_start: int) -> bool:
        """Watch (pid, pid_start) under ``key``; False if it already ended."""
        try:
            fd = os.pidfd_open(pid)
        except ProcessLookupError:
            return False
        try:
            if process_start_time(pid) != pid_start:
                os.close(fd)
                return False
        except OSError:
            os.close(fd)
            return False
        with self._lock:
            if self._stopped:
                os.close(fd)
                raise RuntimeError("PeerWatcher is stopped")
            old = self._fds.pop(key, None)
            self._fds[key] = fd
        if old is not None:
            os.close(old)
        self._wake()
        return True

    def unwatch(self, key: str) -> None:
        with self._lock:
            fd = self._fds.pop(key, None)
        if fd is not None:
            os.close(fd)
            self._wake()

    def watching(self, key: str) -> bool:
        with self._lock:
            return key in self._fds

    def _wake(self) -> None:
        try:
            os.write(self._wake_w, b"\0")
        except (BlockingIOError, OSError):
            pass

    def _loop(self) -> None:
        while True:
            poller = select.poll()
            poller.register(self._wake_r, select.POLLIN)
            with self._lock:
                if self._stopped:
                    return
                by_fd = {fd: key for key, fd in self._fds.items()}
            for fd in by_fd:
                poller.register(fd, select.POLLIN)
            try:
                events = poller.poll()
            except InterruptedError:
                continue
            ended = []
            for fd, _ in events:
                if fd == self._wake_r:
                    try:
                        while os.read(self._wake_r, 4096):
                            pass
                    except BlockingIOError:
                        pass
                    continue
                key = by_fd.get(fd)
                with self._lock:
                    if key is None or self._fds.get(key) != fd:
                        continue  # unwatched or replaced meanwhile
                    # The fd number may have been reused while polling
                    check = select.poll()
                    check.register(fd, select.POLLIN)
                    if not check.poll(0):
                        continue
                    del self._fds[key]
                os.close(fd)
                ended.append(key)
            for key in ended:
                try:
                    self._on_exit(key)
                except Exception:
                    logger.exception("handling the exit of %s failed", key)
