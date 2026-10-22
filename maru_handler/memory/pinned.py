# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Explicit ownership of read pins until the final copy completes."""

from collections.abc import Callable, Iterable
from threading import Lock
from typing import Any


class PinnedViews(list):
    """A list of mapped views with an idempotent, explicit pin release.

    Slices share the original lease. Dropping Python references does not unpin:
    the caller must establish GPU completion before calling release().
    """

    def __init__(self, values: Iterable[Any], release: Callable[[], None]):
        super().__init__(values)
        self._release = release
        self._lock = Lock()
        self._released = False

    def __getitem__(self, index):
        value = super().__getitem__(index)
        return PinnedViews(value, self.release) if isinstance(index, slice) else value

    def release(self) -> None:
        """Release this lease at most once, including across shared slices."""
        with self._lock:
            if self._released:
                return
            self._released = True
        self._release()
