# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""UCX progress of the remote CXL agent's NIXL agent.

The NIXL agent runs without its progress thread, which would poll UCX
without pause for the life of the process. RDMA reads and writes through a
connection need no CPU here; UCX here has work when a worker connects (its
first transfer waits for this side to answer the connection setup) and when
a worker checks its connection with a notification.

So UCX is driven while a worker connects. A map request names the worker's
NIXL agent; the worker loads the reply and then announces itself with an
empty notification over the new connection. From the map until that
notification arrives, or :data:`CONNECT_WINDOW_S` passes, a thread drives
UCX every :data:`POLL_INTERVAL_S`. While no worker connects, no thread runs.
The service also drives UCX once on every hello, map and sweep, which drops
the notifications workers send and completes a connection setup that
outlasted its window.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

# How long a map waits for the worker's announcement; a worker loads the
# reply and announces itself within milliseconds.
CONNECT_WINDOW_S = 2.0
# Pause between two UCX progress calls while a worker connects (a worker
# polls its transfers at the same interval).
POLL_INTERVAL_S = 0.0002


class ConnectProgress:
    """Drives a NIXL transport's UCX while workers connect, and only then."""

    def __init__(
        self,
        transport: Any,
        *,
        window_s: float = CONNECT_WINDOW_S,
        poll_s: float = POLL_INTERVAL_S,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create an idle driver.

        Args:
            transport: ``NixlTransport`` of the remote CXL agent.
            window_s: How long to wait for a worker's announcement.
            poll_s: Pause between two progress calls while waiting.
            clock: Monotonic clock (tests inject one).
        """
        self._transport = transport
        self._window_s = window_s
        self._poll_s = poll_s
        self._clock = clock
        # NIXL agent name of a connecting worker -> end of its wait
        self._expected: dict[str, float] = {}
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._failed = False

    def expect(self, name: str) -> None:
        """Drive UCX until ``name`` announces itself or the window ends.

        Args:
            name: NIXL agent name the worker gave in its map request.
        """
        with self._lock:
            if self._stop.is_set():
                return
            self._expected[name] = self._clock() + self._window_s
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._run, name="nixl-connect-progress", daemon=True
                )
                self._thread.start()

    def expecting(self) -> set[str]:
        """Names of the workers whose announcement is awaited (tests, status)."""
        with self._lock:
            return set(self._expected)

    def running(self) -> bool:
        """Whether the progress thread runs now."""
        with self._lock:
            return self._thread is not None

    def drain(self) -> None:
        """Drive UCX once and drop the notifications that arrived.

        A notification from an awaited worker ends its wait.

        Raises:
            Exception: if NIXL fails.
        """
        senders = self._transport.take_notifs()
        if senders:
            with self._lock:
                for name in senders:
                    self._expected.pop(name, None)

    def close(self) -> None:
        """Stop waiting for every worker; the thread ends at its next turn."""
        self._stop.set()
        with self._lock:
            self._expected.clear()

    def _run(self) -> None:
        while True:
            try:
                self.drain()
            except Exception:
                if not self._failed:  # once while it keeps failing
                    logger.warning(
                        "driving UCX for connecting workers failed", exc_info=True
                    )
                self._failed = True
            else:
                self._failed = False
            with self._lock:
                now = self._clock()
                for name, end in list(self._expected.items()):
                    if end <= now:
                        del self._expected[name]
                        logger.info(
                            "NIXL agent %s did not announce itself within %.1f s; "
                            "its connection completes at the next sweep",
                            name,
                            self._window_s,
                        )
                if not self._expected:
                    self._thread = None
                    return
            time.sleep(self._poll_s)
