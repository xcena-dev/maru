# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unholds: RM holds to drop, sent on a thread of their own and sent again,
each on its own backoff, until the RM confirms each, so an RM that is briefly
down never keeps a region held for good. A drop the RM refuses is not sent
again: it stays held until the RM reaps it or an operator drops it."""

import logging
import threading
import time
from collections.abc import Callable, Hashable
from dataclasses import dataclass

from .client import HoldRefusedError

logger = logging.getLogger(__name__)

# Pause before a failed drop is sent again, doubled up to the max
RETRY_S = 1.0
RETRY_MAX_S = 30.0


@dataclass
class _Drop:
    send: Callable[[], None]
    what: str
    due: float = 0.0
    pause: float = 0.0


class Unholds:
    def __init__(self, retry_s: float = RETRY_S, max_retry_s: float = RETRY_MAX_S):
        self.retry_s = retry_s
        self.max_retry_s = max_retry_s
        self._cv = threading.Condition()
        self._queued: dict[Hashable, _Drop] = {}
        self._sending: Hashable | None = None
        self._warned: set[Hashable] = set()
        self._thread: threading.Thread | None = None

    def put(self, key: Hashable, send: Callable[[], None], what: str) -> None:
        """Queue ``send()`` (the UNHOLD of one hold, raising if it failed)."""
        with self._cv:
            self._queued[key] = _Drop(send, what)
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._loop, name="maru-unhold", daemon=True
                )
                self._thread.start()
            self._cv.notify_all()

    def take_back(self, key: Hashable) -> tuple[Callable[[], None], str] | None:
        """The hold is wanted again: remove a queued drop of it, after one
        under way ends; returns it (send, what) if there was one."""
        with self._cv:
            self._cv.wait_for(lambda: self._sending != key)
            self._warned.discard(key)
            drop = self._queued.pop(key, None)
            return None if drop is None else (drop.send, drop.what)

    def flush(self, timeout: float) -> bool:
        """Send every queued drop now, and wait until each is confirmed;
        whether they were."""
        with self._cv:
            for drop in self._queued.values():
                drop.due = 0.0
            self._cv.notify_all()
            return self._cv.wait_for(
                lambda: not self._queued and self._sending is None, timeout
            )

    def pending(self) -> int:
        with self._cv:
            return len(self._queued) + (self._sending is not None)

    def _next(self) -> tuple[Hashable, _Drop]:
        """Caller holds the lock: the next drop once it is due."""
        while True:
            if not self._queued:
                self._cv.wait()
                continue
            key = min(self._queued, key=lambda k: self._queued[k].due)
            wait = self._queued[key].due - time.monotonic()
            if wait <= 0:
                return key, self._queued.pop(key)
            self._cv.wait(wait)

    def _loop(self) -> None:
        while True:
            with self._cv:
                key, drop = self._next()
                self._sending = key
            failed: Exception | None = None
            try:
                drop.send()
            except Exception as e:
                failed = e
            with self._cv:
                self._sending = None
                refused = isinstance(failed, HoldRefusedError)
                if failed is None or refused:
                    first = key not in self._warned or refused
                    self._warned.discard(key)
                else:
                    first = key not in self._warned
                    self._warned.add(key)
                    drop.pause = min(
                        max(drop.pause * 2, self.retry_s), self.max_retry_s
                    )
                    drop.due = time.monotonic() + drop.pause
                    self._queued.setdefault(key, drop)
                self._cv.notify_all()
            if failed is not None and first:
                logger.warning(
                    "could not drop %s (%s); %s",
                    drop.what,
                    failed,
                    "the RM refused it: not sent again"
                    if refused
                    else "sending it again until the RM confirms it",
                )
