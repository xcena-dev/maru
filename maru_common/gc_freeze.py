# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Short cycle-collector pauses for long-running service processes.

A full collection walks every tracked object: with a 100k-key catalog that
pauses the process for tens of milliseconds. A thread collects the garbage
of the objects created since its last pass and freezes the survivors, so
every collection, full ones too, walks only what one interval created.
Reference counting still frees frozen objects, but a reference cycle among
them is collected only by reclaim(): the thread runs it every ``reclaim_s``,
and a process may call it once a large object graph has ended.
"""

import gc
import threading
import time

FREEZE_INTERVAL_S = 0.25

# One freeze pass or reclaim at a time: a freeze between a reclaim's
# unfreeze and its collection would keep the garbage frozen
_lock = threading.Lock()


def start_freezer(
    interval_s: float = FREEZE_INTERVAL_S, reclaim_s: float | None = None
) -> threading.Event:
    """Collect and freeze now and every ``interval_s`` on a daemon thread,
    and reclaim every ``reclaim_s``; set the returned event to stop it."""
    stop = threading.Event()

    def run() -> None:
        reclaimed = time.monotonic()
        while True:
            if reclaim_s is not None and time.monotonic() - reclaimed >= reclaim_s:
                reclaim()
                reclaimed = time.monotonic()
            with _lock:
                gc.collect()
                gc.freeze()
            if stop.wait(interval_s):
                return

    threading.Thread(target=run, name="maru-gc-freeze", daemon=True).start()
    return stop


def reclaim() -> None:
    """Collect the frozen objects that became garbage in cycles: unfreeze
    and run a full collection (a pause as long as one without the freezer);
    the freezer freezes what survives on its next pass."""
    with _lock:
        gc.unfreeze()
        gc.collect()
