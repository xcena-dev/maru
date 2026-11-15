# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""The collector setup of the service processes."""

import gc
import threading
import time
import weakref

# bound before the autouse fixture replaces it for every other test
from maru_common.gc_freeze import start_freezer


class _Cycle:
    def __init__(self):
        self.me = self


def _frozen(obj) -> bool:
    """Tracked, but in none of the generations a collection walks."""
    return gc.is_tracked(obj) and not any(
        o is obj for generation in range(3) for o in gc.get_objects(generation)
    )


def test_the_freezer_collects_garbage_and_freezes_the_rest():
    kept = _Cycle()
    dropped = weakref.ref(_Cycle())  # garbage only a collection frees
    stop = start_freezer(interval_s=0.01)
    try:
        deadline = time.monotonic() + 5
        while dropped() is not None or not _frozen(kept):
            assert time.monotonic() < deadline, "not collected or not frozen"
            time.sleep(0.01)
        # objects created later are frozen on a later pass
        later = _Cycle()
        while not _frozen(later):
            assert time.monotonic() < deadline, "not frozen on a later pass"
            time.sleep(0.01)
    finally:
        stop.set()
        for thread in threading.enumerate():
            if thread.name == "maru-gc-freeze":
                thread.join(5)
        gc.unfreeze()
    assert not any(t.name == "maru-gc-freeze" for t in threading.enumerate())
