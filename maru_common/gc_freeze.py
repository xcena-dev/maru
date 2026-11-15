# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Short cycle-collector pauses for long-running service processes.

A full collection walks every tracked object: with a 100k-key catalog that
pauses the process for tens of milliseconds. A thread collects the garbage
of the objects created since its last pass and freezes the survivors, so
every collection, full ones too, walks only what one interval created.
Reference counting still frees frozen objects; a reference cycle among them
is never collected.
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
