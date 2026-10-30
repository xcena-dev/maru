# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Memory waits: ask the memory side to prepare pages and wait for the answer.

Memory with a slow tier behind a fast cache (a CXL-SSD, say) serves a WRITE
of a page that is not in the cache yet only after it has brought the page
in. A hint (``hints.py``) asks for that early; a wait asks and also waits
for the answer before the transfer goes:

- ``write_wait``: a store sends its pages and waits, at most ``timeout_ms``,
  until they are ready; then it posts its WRITEs whatever the answer.

A wait is advice too: the transfer goes whatever the answer, and an agent
that does not answer only costs the wait. Each thread has its own
connection per agent, so one store's wait never holds up another's, and
none of them holds up hints or region maps.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

#: How long an agent that could not be reached is not asked again
UNREACHABLE_RETRY_S = 5.0
#: Added to a wait's own time for the reply to arrive
REPLY_MARGIN_S = 3.0


class MemoryWaits:
    """Waits on remote CXL agents, over connections of this process.

    Args:
        connect: ``(agent, timeout_s) -> RegionMapClient``, not yet
            connected.
        max_wait_ms: Longest wait this process asks for; bounds each reply.
    """

    def __init__(self, connect: Callable[[str, float], Any], max_wait_ms: int) -> None:
        self._connect = connect
        self._timeout_s = max_wait_ms / 1000 + REPLY_MARGIN_S
        self._lock = threading.Lock()
        # (agent, thread) -> connection
        self._clients: dict[tuple[str, int], Any] = {}
        self._retry_at: dict[str, float] = {}

    def takes(self, agent: str, action: str) -> bool:
        """Whether ``agent``'s region extension takes ``action``; connects
        this thread to it if needed (False if it cannot be reached)."""
        return self._client(agent, action) is not None

    def write_wait(
        self, agent: str, token: str, ranges: list[dict], timeout_ms: int
    ) -> bool:
        """Send a store's pages for ``agent`` and wait for the answer.

        Returns:
            True if the agent answered (in time or not), False if the wait
            could not be sent or answered.
        """
        client = self._client(agent, "write_wait")
        if client is None:
            return False
        message = {
            "action": "write_wait",
            "request": token,
            "ranges": ranges,
            "timeout_ms": timeout_ms,
            "epoch": client.epoch,
        }
        try:
            client.extension(message)
        except Exception:
            # Advice: the WRITEs go anyway, only slower.
            logger.debug("write_wait at %s failed", agent, exc_info=True)
            return False
        return True

    def close(self) -> None:
        """Close every connection."""
        with self._lock:
            clients, self._clients = list(self._clients.values()), {}
        for client in clients:
            client.close()

    def _client(self, agent: str, action: str) -> Any:
        """This thread's connection to ``agent``, or None if the agent's
        region extension does not take ``action`` or it cannot be reached."""
        key = (agent, threading.get_ident())
        with self._lock:
            if time.monotonic() < self._retry_at.get(agent, 0.0):
                return None
            client = self._clients.get(key)
            if client is None:
                client = self._clients[key] = self._connect(agent, self._timeout_s)
        try:
            if not client.epoch:
                client.hello()  # connects; a reconnect renews the epoch
        except Exception:
            logger.debug("memory waits: %s unreachable", agent, exc_info=True)
            with self._lock:
                self._retry_at[agent] = time.monotonic() + UNREACHABLE_RETRY_S
            return None
        return client if action in client.hint_actions else None
