# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Short cycle-collector pauses for long-running service processes.

A full collection walks every tracked object: with a 100k-key catalog that
pauses the process for tens of milliseconds. A thread collects the garbage
of the objects created since its last pass and freezes the survivors, so
every collection, full ones too, walks only what one interval created.
Reference counting still frees frozen objects; a reference cycle among them
is collected only after reclaim(), which a process calls once a large object
graph with cycles has ended (maru-storage: between two runs).
"""

import gc
import threading

FREEZE_INTERVAL_S = 0.25


def start_freezer(interval_s: float = FREEZE_INTERVAL_S) -> threading.Event:
    """Collect and freeze now and every ``interval_s`` on a daemon thread;
    set the returned event to stop it."""
    stop = threading.Event()

    def run() -> None:
        while True:
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
    gc.unfreeze()
    gc.collect()
