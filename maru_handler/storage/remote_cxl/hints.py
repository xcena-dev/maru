# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Read hints: tell an access policy which bytes a load will read, before it starts.

A serving engine knows which stored keys a request will load well before
its worker starts the load: the request waits for the running engine step
first, and the scheduler looks its prefix up when it picks it. Memory with a
slow tier behind a fast cache (see ``access_policy``) can use that time to
bring the bytes in. A read hint carries that knowledge to the access policy:
the backend finds where the hit keys are and calls the policy's optional
``read_hint`` hook with a :class:`HintChannel` to the remote CXL agent of
each region, and ``read_hint_end`` once the request is over.

All of it runs on one background thread per client, in the order hints were
given, so the caller (an engine's input thread or its scheduler) never waits
for a lookup, a connection or a reply. A hint is advice: one that is late,
lost or wrong costs only the time it would have saved.
"""

from __future__ import annotations

import logging
import queue
import threading
from collections.abc import Callable, Sequence
from typing import Any

from ..access_policy import AccessRange

logger = logging.getLogger(__name__)

#: Hints waiting for the background thread; more are dropped.
MAX_PENDING_HINTS = 1024


class HintChannel:
    """Messages from a policy's ``read_hint`` to the remote CXL agent of a region.

    A hint names regions this process does not hold, so the agent accepts it
    only as advice: the regions must be mapped there and the message may not
    prepare a write.
    """

    def __init__(self, agent: str, send: Callable[[str, dict], dict]) -> None:
        self.agent = agent
        self._send = send

    def request(
        self, action: str, ranges: Sequence[AccessRange] = (), **payload: Any
    ) -> dict:
        """Send one hint message and return the region extension's reply.

        Runs on the hint thread; it waits for the reply, which keeps the
        messages of one agent in order.

        Raises:
            Exception: if the agent is unreachable or refused the message.
        """
        message = {
            **payload,
            "action": action,
            "ranges": [r.to_dict() for r in ranges],
            "write": False,
            "hint": True,
        }
        return self._send(self.agent, message)


class ReadHints:
    """Runs a policy's read hints on one background thread.

    Args:
        policy: The access policy; hints are skipped if it has no
            ``read_hint`` hook.
        locate: ``keys -> [(agent, ranges)]``: the leading hit keys grouped
            per region in key order, with the agent that maps each region.
            Called on the hint thread (it may ask MaruServer).
        send: ``(agent, message) -> reply`` over a connection of this
            process to that agent.
    """

    def __init__(
        self,
        policy: Any,
        locate: Callable[[list[str]], list[tuple[str, list[AccessRange]]]],
        send: Callable[[str, dict], dict],
    ) -> None:
        self._hint = getattr(policy, "read_hint", None)
        self._end = getattr(policy, "read_hint_end", None)
        self._locate = locate
        self._send = send
        self._queue: queue.Queue[tuple[str, str, list[str]] | None] = queue.Queue(
            MAX_PENDING_HINTS
        )
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        # Agents each hinted request reached, for its end
        self._agents: dict[str, list[str]] = {}

    @property
    def enabled(self) -> bool:
        """Whether the policy takes read hints."""
        return self._hint is not None

    def hint(self, request_id: str, keys: list[str]) -> None:
        """Queue a hint that ``request_id`` will read ``keys`` in this order."""
        if self._hint is not None and request_id and keys:
            self._put(("hint", request_id, list(keys)))

    def end(self, request_id: str) -> None:
        """Queue the end of ``request_id``: what was prepared for it may go."""
        if self._hint is not None and request_id:
            self._put(("end", request_id, []))

    def close(self) -> None:
        """Stop the thread after the hints already queued."""
        with self._lock:
            thread, self._thread = self._thread, None
        if thread is not None:
            self._queue.put(None)
            thread.join(timeout=5.0)

    def _put(self, item: tuple[str, str, list[str]]) -> None:
        with self._lock:
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._run, name="maru-read-hints", daemon=True
                )
                self._thread.start()
        try:
            self._queue.put_nowait(item)
        except queue.Full:
            logger.debug("read hint dropped: %d hints are waiting", MAX_PENDING_HINTS)

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                return
            kind, request_id, keys = item
            try:
                if kind == "hint":
                    self._give(request_id, keys)
                else:
                    self._finish(request_id)
            except Exception:
                # Advice only: the load reads the bytes either way.
                logger.debug(
                    "read hint %s for %s failed", kind, request_id, exc_info=True
                )

    def _give(self, request_id: str, keys: list[str]) -> None:
        if request_id in self._agents:
            return  # hinted already
        groups = self._locate(keys)
        self._agents[request_id] = [agent for agent, _ in groups]
        for agent, ranges in groups:
            self._hint(HintChannel(agent, self._send), request_id, ranges)

    def _finish(self, request_id: str) -> None:
        agents = self._agents.pop(request_id, None)
        if not agents or self._end is None:
            return
        for agent in dict.fromkeys(agents):
            self._end(HintChannel(agent, self._send), request_id)
