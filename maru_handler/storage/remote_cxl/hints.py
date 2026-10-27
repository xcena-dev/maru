# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Memory hints: tell the memory side which bytes will be read or written soon.

A serving engine knows which stored keys a request will load well before
its worker starts the load: the request waits for the running engine step
first, and the scheduler looks its prefix up when it picks it. Memory with a
slow tier behind a fast cache (a CXL-SSD, say) can use that time to bring
the bytes in. A read hint carries that knowledge to the memory side: the
backend finds where the hit keys are and sends ``read_hint`` to the remote
CXL agent of their regions, whose region extension acts on it, and
``read_hint_end`` once the request is over. Agents without a region
extension that takes hints get nothing.

A process that gives no early hints (a worker whose engine names no request
to its storage) can still hint each load as its READs start: the bytes are
known from the lease, and a long load's later chunks can be brought in while
its first ones are read.

A write hint (``write_hint``) names the pages a store is about to WRITE, in
write order, as soon as the store has its pages and before it posts the
WRITEs. Memory that would otherwise fill each written page from its slow
tier on the first WRITE can prepare the pages instead. A write hint has no
end; it never gives the memory side leave to change the pages.

All of it runs on one background thread per client, in the order hints were
given, so the caller (an engine's input thread or its scheduler) never waits
for a lookup, a connection or a reply. A hint is advice: one that is late,
lost or wrong costs only the time it would have saved. The load leases and
reads every key as it would without hints.
"""

from __future__ import annotations

import logging
import queue
import threading
from collections import OrderedDict
from collections.abc import Callable

logger = logging.getLogger(__name__)

#: Hints waiting for the background thread; more are dropped (ends are not).
MAX_PENDING_HINTS = 1024
#: A dropped hint is logged as a warning once per this many drops.
DROP_WARNING_EVERY = 1024
#: Ranges one ``read_hint`` message names; a longer hint takes several.
MAX_HINT_RANGES = 4096
#: Hinted requests kept open (not ended); past it the oldest is ended. An
#: agent lets one connection keep as many open by default.
MAX_OPEN_HINTS = 4096


class MemoryHints:
    """Sends read and write hints to remote CXL agents on one background thread.

    Args:
        locate: ``keys -> [(agent, ranges)]``: the leading hit keys, per
            agent that maps their regions, each range a
            ``{"handle", "offset", "length"}`` map in key order. Called on
            the hint thread (it may ask MaruServer).
        send: ``(agent, message) -> bool`` over a connection of this
            process to that agent; False if the agent's region extension
            does not take the message's action (it was not sent).
    """

    def __init__(
        self,
        locate: Callable[[list[str]], list[tuple[str, list[dict]]]],
        send: Callable[[str, dict], bool],
    ) -> None:
        self._locate = locate
        self._send = send
        # Unbounded so an end is never lost; _pending bounds the hints in it.
        self._queue: queue.SimpleQueue[tuple[str, str, list] | None] = (
            queue.SimpleQueue()
        )
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._pending = 0
        self._dropped = 0
        self._closed = False
        # Agents each hinted request reached, for its end, oldest first
        # (hint thread only)
        self._agents: OrderedDict[str, list[str]] = OrderedDict()

    def hint(self, request_id: str, keys: list[str]) -> None:
        """Queue a hint that ``request_id`` will read ``keys`` in this order.

        Dropped, with a warning now and then, while
        :data:`MAX_PENDING_HINTS` hints wait.
        """
        if request_id and keys:
            self._queue_hint(("hint", request_id, list(keys)))

    def hint_ranges(
        self, request_id: str, groups: list[tuple[str, list[dict]]]
    ) -> None:
        """Queue a hint whose ranges are known already (``locate``'s form)."""
        if request_id and groups:
            self._queue_hint(("ranges", request_id, groups))

    def write_hint(self, token: str, groups: list[tuple[str, list[dict]]]) -> None:
        """Queue a write hint: the pages in ``groups`` (``locate``'s form, in
        write order) are about to be written. ``token`` names the store; a
        write hint has no end. Dropped as a read hint is."""
        if token and groups:
            self._queue_hint(("write", token, groups))

    def end(self, request_id: str) -> None:
        """Queue the end of ``request_id``: what was prepared for it may go."""
        if request_id:
            self._put(("end", request_id, []))

    def close(self, timeout_s: float = 5.0) -> bool:
        """Stop the thread after the hints already queued; hints after this
        are ignored.

        Returns:
            True if the thread ended (or never started) within
            ``timeout_s``; the caller may then close what it sends on.
        """
        with self._lock:
            self._closed = True
            thread, self._thread = self._thread, None
        if thread is None:
            return True
        self._queue.put(None)
        thread.join(timeout=timeout_s)
        if thread.is_alive():
            logger.warning("read hint thread still busy after %.0f s", timeout_s)
            return False
        return True

    def _queue_hint(self, item: tuple[str, str, list]) -> None:
        with self._lock:
            if self._pending >= MAX_PENDING_HINTS:
                self._dropped += 1
                dropped = self._dropped
            else:
                self._pending += 1
                dropped = 0
        if dropped:
            if dropped % DROP_WARNING_EVERY == 1:
                logger.warning(
                    "read hints dropped (%d so far): %d are waiting to be sent",
                    dropped,
                    MAX_PENDING_HINTS,
                )
            return
        self._put(item)

    def _put(self, item: tuple[str, str, list]) -> None:
        with self._lock:
            if self._closed:
                if item[0] != "end":
                    self._pending -= 1
                return
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._run, name="maru-read-hints", daemon=True
                )
                self._thread.start()
            self._queue.put(item)

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                return
            kind, request_id, payload = item
            if kind != "end":
                with self._lock:
                    self._pending -= 1
            try:
                if kind == "hint":
                    self._give(request_id, self._locate(payload))
                elif kind == "ranges":
                    self._give(request_id, payload)
                elif kind == "write":
                    self._send_all("write_hint", request_id, payload)
                else:
                    self._finish(request_id)
            except Exception:
                # Advice only: the load reads the bytes either way.
                logger.debug("hint %s for %s failed", kind, request_id, exc_info=True)

    def _give(self, request_id: str, groups: list[tuple[str, list[dict]]]) -> None:
        if request_id in self._agents:
            return  # hinted already
        self._agents[request_id] = self._send_all("read_hint", request_id, groups)
        while len(self._agents) > MAX_OPEN_HINTS:
            # Its end never came (or comes later): end it now.
            self._finish(next(iter(self._agents)))

    def _send_all(
        self, action: str, request_id: str, groups: list[tuple[str, list[dict]]]
    ) -> list[str]:
        """Send ``action`` with each agent's ranges, in messages of at most
        :data:`MAX_HINT_RANGES`; the agents that took it."""
        reached: list[str] = []
        for agent, ranges in groups:
            try:
                for i in range(0, len(ranges), MAX_HINT_RANGES):
                    message = {
                        "action": action,
                        "request": request_id,
                        "ranges": ranges[i : i + MAX_HINT_RANGES],
                    }
                    if not self._send(agent, message):
                        break  # its region extension takes no such hint
                    if agent not in reached:
                        reached.append(agent)
            except Exception:
                logger.debug(
                    "%s for %s not sent to %s", action, request_id, agent, exc_info=True
                )
        return reached

    def _finish(self, request_id: str) -> None:
        for agent in self._agents.pop(request_id, None) or []:
            message = {"action": "read_hint_end", "request": request_id, "ranges": []}
            try:
                self._send(agent, message)
            except Exception:
                logger.debug(
                    "read hint end for %s not sent to %s",
                    request_id,
                    agent,
                    exc_info=True,
                )
