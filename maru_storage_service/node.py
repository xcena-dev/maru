# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Run restart loop and ownership across runs."""

import logging
import threading
import time
from collections.abc import Callable

from maru_common import gc_freeze
from maru_common.storage_types import CloseMode

from .config import NodeConfig
from .node_run import NodeRun, check_servable
from .ownership import NodeOwnership
from .ports import ServerStopped

logger = logging.getLogger("maru_storage_service.service")

NEW_RUN_PAUSE_S = 1.0
NEW_RUN_PAUSE_MAX_S = 60.0
NEW_RUN_RESET_S = 300.0
STOP_WAIT_S = 30.0


class Node:
    """Runs of the node's StorageService, one after another in this process:
    a run that ends on its own and asks for a new one is followed by a new
    run with a new node_epoch, after a pause that grows while runs end soon
    after they start. Workers re-attach to it as after a process restart."""

    def __init__(
        self,
        config: NodeConfig,
        make_service: Callable[[NodeConfig], NodeRun],
        pause_s: float = NEW_RUN_PAUSE_S,
        max_pause_s: float = NEW_RUN_PAUSE_MAX_S,
    ):
        check_servable(config)
        self.config = config
        self._make_service = make_service
        self.pause_s = pause_s
        self.max_pause_s = max_pause_s
        self._stop = threading.Event()
        self._ownership = NodeOwnership(
            config.state_dir,
            config.node_id,
            config.socket if config.role == "gpu" else None,
        )
        self.service: NodeRun | None = None
        self.runs = 0
        # Why the last run ended on its own
        self.ended: str | None = None
        # A run's teardown failed: no run follows, the process must exit
        self.failed: str | None = None

    def stop(self) -> None:
        """End the current run with restart and start no other; any thread,
        a signal handler too (no lock)."""
        self._stop.set()
        service = self.service
        if service is not None:
            service.interrupt()

    def run(self) -> bool:
        """Serve until stop() (True), or until no run may follow (False):
        another run claimed the node, or a teardown failed or did not end in
        STOP_WAIT_S after stop() (``failed``). A start that fails otherwise
        raises."""
        try:
            return self._run()
        finally:
            service = self.service
            if service is None or (
                service.wait_finished(0) and service.teardown_failed is None
            ):
                self._ownership.close()

    def _run(self) -> bool:
        pause = self.pause_s
        while True:
            if self._stop.is_set():
                return True
            service = self._make_service(self.config)
            service._ownership = self._ownership
            service._owns_ownership = False
            service.later_run = self.runs > 0
            self.service = service
            self.runs += 1
            if self._stop.is_set():  # stop() may have missed it
                service.interrupt()
            started = time.monotonic()
            try:
                service.start()
            except ServerStopped:
                pass  # stopped, or the run ended during its start
            except Exception as e:
                if self._claimed_elsewhere(service, str(e)):
                    return False
                if service.teardown_failed is not None:
                    self.failed = service.teardown_failed
                raise
            while not self._stop.wait(0.1) and not service.wait(0):
                pass
            if service.ended is None:
                # Stopped: torn down on a thread, waited for up to STOP_WAIT_S
                threading.Thread(
                    target=service.stop,
                    args=(CloseMode.RESTART,),
                    name="maru-run-stop",
                    daemon=True,
                ).start()
            if not self._finished(service):
                self.failed = f"it did not end within {STOP_WAIT_S:g} s"
                return False
            if service.teardown_failed is not None:
                self.failed = service.teardown_failed
                logger.error("no new run: the teardown failed (%s)", self.failed)
                return False
            if service.ended is None:
                return True
            self.ended = service.ended
            if not service.new_run or self._claimed_elsewhere(service, service.ended):
                return False
            if self._stop.is_set():
                return True
            if time.monotonic() - started >= NEW_RUN_RESET_S:
                pause = self.pause_s
            logger.warning(
                "starting a new run in %.1f s after: %s", pause, service.ended
            )
            # The ended run's objects: its cycles were frozen while it ran
            self.service = None
            del service
            gc_freeze.reclaim()
            if self._stop.wait(pause):
                return True
            pause = min(pause * 2, self.max_pause_s)

    def _finished(self, service: NodeRun) -> bool:
        """Wait for the run's teardown; once stopped, for STOP_WAIT_S more."""
        deadline = None
        while not service.wait_finished(0.1):
            if self._stop.is_set():
                deadline = deadline or time.monotonic() + STOP_WAIT_S
                if time.monotonic() >= deadline:
                    logger.error("the run's teardown did not end; stopping anyway")
                    return False
        return True

    def _claimed_elsewhere(self, service: NodeRun, reason: str) -> bool:
        """Another run claimed the node in the RM: never claim it back."""
        if not service.claimed_elsewhere:
            return False
        self.ended = f"another run claimed the node ({reason})"
        return True
