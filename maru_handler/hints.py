# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Queue advisory read hints and promotes for node storage and CXL plugins.

The background thread locates keys and sends hints in order. Dropped hints
leave reads unchanged; request ends are retained.
"""

from __future__ import annotations

import logging
import queue
import threading
from collections import OrderedDict
from collections.abc import Callable

from maru_common.storage_types import Location

logger = logging.getLogger(__name__)

#: Hints waiting for the background thread; more are dropped (ends are not).
MAX_PENDING_HINTS = 1024
#: A dropped hint is logged as a warning once per this many drops.
DROP_WARNING_EVERY = 1024
#: Ranges one hint message names per target; a longer hint takes
#: several.
MAX_HINT_RANGES = 4096
#: Hinted requests kept open (not ended); past it the oldest is ended.
MAX_OPEN_HINTS = 4096


class MemoryHints:
    """Send hints to memory nodes or handler plugins on one background thread.

    Args:
        locate: Keys to leading-hit ranges grouped by target.
        send: Deliver a message to its target; False if unsupported.
    """

    def __init__(
        self,
        locate: Callable[[list[str]], list[tuple[str, list[dict | Location]]]],
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
        # Set by abandon(): the thread drops what is still queued
        self._discard = threading.Event()
        # The thread close() stopped waiting for (abandon() joins it)
        self._closing_thread: threading.Thread | None = None
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
        self,
        request_id: str,
        groups: list[tuple[str, list[dict | Location]]],
        *,
        append: bool = False,
    ) -> None:
        """Queue known ranges; append retains chunks of a relayed hint."""
        if request_id and groups:
            self._queue_hint(
                ("append_ranges" if append else "ranges", request_id, groups)
            )

    def write_hint(
        self, token: str, groups: list[tuple[str, list[dict | Location]]]
    ) -> None:
        """Queue a write hint: the pages in ``groups`` (``locate``'s form, in
        write order) are about to be written. ``token`` names the store; a
        write hint has no end. Dropped as a read hint is."""
        if token and groups:
            self._queue_hint(("write", token, groups))

    def promote(self, request_id: str, agents: list[str]) -> None:
        """Queue a read promote: the load of ``request_id`` starts now. Sent
        to each of ``agents`` that takes it; no ranges, no end. Dropped as a
        read hint is."""
        if request_id and agents:
            self._queue_hint(("promote", request_id, list(agents)))

    def end(self, request_id: str) -> None:
        """Queue the end of ``request_id``: what was prepared for it may go."""
        if request_id:
            self._put(("end", request_id, []))

    def close(self, timeout_s: float = 5.0, *, end_open: bool = False) -> bool:
        """Stop the thread after the hints already queued; hints after this
        are ignored.

        Args:
            timeout_s: How long to wait for the thread.
            end_open: Also send ``read_hint_end`` for every request still
                open, after the queued hints, so a target that keeps state
                per request can drop it. A target that ends a closed
                connection's requests itself does not
                need it.

        Returns:
            True if the thread ended (or never started) within
            ``timeout_s``; the caller may then close what it sends on.
        """
        with self._lock:
            self._closed = True
            thread, self._thread = self._thread, None
        if thread is None:
            return True
        if end_open:
            self._queue.put(("end_open", "", []))
        self._queue.put(None)
        thread.join(timeout=timeout_s)
        if thread.is_alive():
            logger.warning("read hint thread still busy after %.0f s", timeout_s)
            with self._lock:
                self._closing_thread = thread
            return False
        return True

    def abandon(self, timeout_s: float | None = None) -> bool:
        """After a :meth:`close` that timed out: drop the messages still
        queued and wait for the thread, so the call under way (a ``locate``
        or a ``send``) is the last one.

        Args:
            timeout_s: How long to wait; None waits until that call returns.

        Returns:
            True if the thread has ended (or there is none).
        """
        self._discard.set()
        with self._lock:
            thread = self._closing_thread
        if thread is None:
            return True
        thread.join(timeout=timeout_s)
        return not thread.is_alive()

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
            if kind not in ("end", "end_open"):
                with self._lock:
                    self._pending -= 1
            if self._discard.is_set():
                continue  # abandoned at close: nothing more goes out
            try:
                if kind == "hint":
                    self._give(request_id, self._locate(payload))
                elif kind == "ranges":
                    self._give(request_id, payload)
                elif kind == "append_ranges":
                    self._give(request_id, payload, append=True)
                elif kind == "write":
                    self._send_all("write_hint", request_id, payload)
                elif kind == "promote":
                    self._send_all(
                        "read_promote", request_id, [(a, []) for a in payload]
                    )
                elif kind == "end_open":
                    for open_request in list(self._agents):
                        self._finish(open_request)
                else:
                    self._finish(request_id)
            except Exception:
                # Advice only: the load reads the bytes either way.
                logger.debug("hint %s for %s failed", kind, request_id, exc_info=True)

    def _give(
        self,
        request_id: str,
        groups: list[tuple[str, list[dict | Location]]],
        *,
        append: bool = False,
    ) -> None:
        if request_id in self._agents and not append:
            return  # hinted already
        reached = self._send_all("read_hint", request_id, groups)
        agents = self._agents.setdefault(request_id, [])
        agents.extend(agent for agent in reached if agent not in agents)
        while len(self._agents) > MAX_OPEN_HINTS:
            # Its end never came (or comes later): end it now.
            self._finish(next(iter(self._agents)))

    def _send_all(
        self,
        action: str,
        request_id: str,
        groups: list[tuple[str, list[dict | Location]]],
    ) -> list[str]:
        """Send ``action`` with each target's ranges, in messages of at most
        :data:`MAX_HINT_RANGES` (one message if there are none); the targets
        that took it."""
        reached: list[str] = []
        for agent, ranges in groups:
            try:
                for i in range(0, max(len(ranges), 1), MAX_HINT_RANGES):
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
