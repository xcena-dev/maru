# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Node across faults: the pause between runs, a teardown that fails, the
node lost to another run, a later run's start, a DRAM pool still mapped by
workers of an earlier run, and a lease no renewal reaches."""

import logging
import mmap
import os
import signal
import threading
import time

import pytest

from maru_common.protocol import MessageType
from maru_shm.types import StaleNodeEpochError
from maru_storage_service import service as service_module
from maru_storage_service.cli import EXIT_FAILED, _signal_handler, serve
from maru_storage_service.drivers import dram
from maru_storage_service.drivers.dram import DramDriver, DramPoolError
from maru_storage_service.local_server import LocalServerError
from maru_storage_service.ports import NodeOpenError, ServerStopped
from maru_storage_service.service import Node, StorageService
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import (
    FakeRm,
    anon_region,
    node_config,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)
from tests.unit.test_ms_new_run import new_run

M = 1 << 20
OPTIONS = {"rpc_timeout_s": 0.3, "evict_interval_s": 0.02, "kept_backoff_s": 0.2}


class FakeRun:
    """What Node uses of a StorageService run. ``end``: the run ends on its
    own at once, for that reason."""

    def __init__(
        self,
        end: str | None = None,
        *,
        new_run: bool = True,
        claimed: bool = False,
        teardown_failed: str | None = None,
        start_error: Exception | None = None,
        finishes: bool = True,
    ):
        self.ended = None
        self.new_run = new_run
        self.later_run = False
        self.teardown_failed = None
        self.claimed_elsewhere = claimed
        self.interrupted = threading.Event()
        self._end, self._failed, self._error = end, teardown_failed, start_error
        self._finishes = finishes
        self._stopped, self._finished = threading.Event(), threading.Event()

    def start(self) -> None:
        if self._error is not None:
            raise self._error
        if self.interrupted.is_set():
            raise ServerStopped()
        if self._end is not None:
            self.ended, self.teardown_failed = self._end, self._failed
            self.stop()

    def stop(self, mode=None) -> None:
        self._stopped.set()
        if self._finishes:
            self._finished.set()

    def interrupt(self) -> None:
        self.interrupted.set()

    def wait(self, timeout=None) -> bool:
        return self._stopped.wait(timeout)

    def wait_finished(self, timeout=None) -> bool:
        return self._finished.wait(timeout)


def config(path: str = "/tmp/maru-none/node.sock", **kwargs):
    return node_config("tcp://127.0.0.1:1", path, **kwargs)


def fake_node(runs: list[FakeRun], **kwargs) -> Node:
    """A Node whose runs are ``runs``, then one that waits to be stopped."""
    queue = list(runs)
    return Node(
        config(),
        lambda c: queue.pop(0) if queue else FakeRun(),
        **kwargs,
    )


def pauses(caplog) -> list[float]:
    unique = {id(r): r for r in caplog.records}.values()  # once each
    return [r.args[0] for r in unique if "new run in" in r.msg]


def logged(caplog, logger: str):
    log = logging.getLogger(logger)
    log.addHandler(caplog.handler)
    caplog.set_level(logging.INFO, logger=log.name)
    return log


# =============================================================================
# Node with stand-in runs
# =============================================================================


def test_the_ended_run_is_dropped_and_reclaimed_before_the_next(monkeypatch):
    seen = []
    node = fake_node([FakeRun("x"), FakeRun("x")], pause_s=0.01, max_pause_s=0.01)
    monkeypatch.setattr(
        service_module.gc_freeze, "reclaim", lambda: seen.append(node.service)
    )
    thread = threading.Thread(target=node.run)
    thread.start()
    wait_until(lambda: node.runs == 3, what="three runs")
    node.stop()
    thread.join(10)
    assert seen == [None, None]  # once per new run, the ended one let go


def test_the_pause_between_runs_grows_and_a_long_run_resets_it(caplog, monkeypatch):
    log = logged(caplog, "maru_storage_service.service")
    try:
        node = fake_node([FakeRun("x") for _ in range(4)], pause_s=0.1, max_pause_s=0.3)
        thread = threading.Thread(target=node.run)
        thread.start()
        wait_until(lambda: node.runs == 5, what="five runs")
        node.stop()
        thread.join(10)
        assert pauses(caplog) == [0.1, 0.2, 0.3, 0.3]
        caplog.clear()
        monkeypatch.setattr(service_module, "NEW_RUN_RESET_S", 0.0)  # all long
        node = fake_node([FakeRun("x") for _ in range(3)], pause_s=0.1, max_pause_s=0.3)
        thread = threading.Thread(target=node.run)
        thread.start()
        wait_until(lambda: node.runs == 4, what="four runs")
        node.stop()
        thread.join(10)
        assert pauses(caplog) == [0.1, 0.1, 0.1]
    finally:
        log.removeHandler(caplog.handler)


def test_a_failed_teardown_ends_the_node():
    node = fake_node([FakeRun("x", teardown_failed="RuntimeError: boom")])
    assert node.run() is False
    assert node.failed == "RuntimeError: boom" and node.runs == 1


def test_a_teardown_that_never_ends_does_not_hold_up_a_stop(monkeypatch):
    monkeypatch.setattr(service_module, "STOP_WAIT_S", 0.2)
    run = FakeRun(finishes=False)
    node = fake_node([run])
    thread = threading.Thread(target=node.run)
    thread.start()
    wait_until(lambda: node.runs == 1, what="a run")
    node.stop()
    thread.join(5)
    assert not thread.is_alive() and not run.wait_finished(0)


def test_another_runs_claim_after_another_end_ends_the_node():
    node = fake_node([FakeRun("MaruServer restarted", claimed=True)])
    assert node.run() is False
    assert node.runs == 1 and "another run claimed the node" in node.ended


def test_a_later_run_whose_start_fails_raises():
    node = fake_node([FakeRun("x"), FakeRun(start_error=NodeOpenError("refused"))])
    with pytest.raises(NodeOpenError):
        node.run()
    assert node.runs == 2


def test_a_claim_lost_during_a_start_ends_the_node():
    lost = FakeRun(start_error=RuntimeError("stale"), claimed=True)
    node = fake_node([lost])
    assert node.run() is False and "another run claimed" in node.ended


def test_a_stop_before_the_start_interrupts_the_new_run():
    created = []

    def make(c):
        node.stop()  # as a signal handler would, between creating and starting
        created.append(FakeRun("x"))
        return created[-1]

    node = Node(config(), make)
    assert node.run() is True
    assert created[0].interrupted.is_set() and created[0].ended is None


def test_a_failed_teardown_exits_nonzero(monkeypatch):
    class Failed:
        def __init__(self, config):
            self.failed, self.ended = "RuntimeError: boom", None

        def run(self):
            return False

        def stop(self):
            pass

    monkeypatch.setattr(service_module, "Node", Failed)
    assert serve(config()) == EXIT_FAILED


def test_a_failed_start_with_a_failed_teardown_is_recorded():
    class Run(FakeRun):
        def start(self):
            self.teardown_failed = "RuntimeError: boom"
            raise NodeOpenError("refused")

    node = fake_node([Run()])
    with pytest.raises(NodeOpenError):
        node.run()
    assert node.failed == "RuntimeError: boom"


def test_a_claim_lost_in_a_failed_start_stays_lost_not_failed():
    class Run(FakeRun):
        def start(self):
            self.teardown_failed = "RuntimeError: boom"
            raise RuntimeError("stale")

    node = fake_node([Run(claimed=True)])
    assert node.run() is False and node.failed is None


def test_a_start_failure_with_a_failed_teardown_exits_failed(monkeypatch):
    class Failed:
        def __init__(self, config):
            self.failed, self.ended = "RuntimeError: boom", None

        def run(self):
            raise NodeOpenError("refused")

    monkeypatch.setattr(service_module, "Node", Failed)
    assert serve(config()) == EXIT_FAILED


def test_serve_starts_the_gc_freezer_before_the_node_runs(monkeypatch):
    from unittest.mock import MagicMock

    from maru_common import gc_freeze

    freezer = MagicMock()
    monkeypatch.setattr(gc_freeze, "start_freezer", freezer)

    class Served:
        def __init__(self, config):
            self.failed, self.ended = None, None

        def run(self):
            freezer.assert_called_once_with()
            return True

        def stop(self):
            pass

    monkeypatch.setattr(service_module, "Node", Served)
    assert serve(config()) == 0
    freezer.assert_called_once_with()


def test_the_signal_handler_only_writes_to_the_pipe():
    rfd, wfd = os.pipe()
    os.set_blocking(wfd, False)
    try:
        handler = _signal_handler(wfd)
        handler(signal.SIGTERM, None)
        handler(signal.SIGTERM, None)
        assert os.read(rfd, 1) == b"x"
    finally:
        os.close(rfd)
        os.close(wfd)


def test_a_signal_stops_the_node_from_another_thread(monkeypatch):
    handlers, stopped = {}, []
    done = threading.Event()

    class Waiting:
        def __init__(self, config):
            self.failed, self.ended = None, None

        def stop(self):
            stopped.append(threading.current_thread())
            done.set()

        def run(self):
            assert done.wait(5)
            return True

    monkeypatch.setattr(service_module, "Node", Waiting)
    monkeypatch.setattr(
        signal, "signal", lambda sig, h: handlers.setdefault(sig, h) or signal.SIG_DFL
    )
    result = []
    thread = threading.Thread(target=lambda: result.append(serve(config())))
    thread.start()
    wait_until(lambda: signal.SIGTERM in handlers, what="the handler")
    handlers[signal.SIGTERM](signal.SIGTERM, None)
    thread.join(5)
    assert result == [0] and len(stopped) == 1
    assert stopped[0].name == "maru-signal"


# =============================================================================
# Node with StorageService runs
# =============================================================================


@pytest.fixture
def fake():
    server = FakeMaruServer(heartbeat_s=0.1)
    yield server
    server.close()


@pytest.fixture
def sockdir():
    path = short_tmpdir()
    yield path
    remove_tree(path)


@pytest.fixture
def runs(fake, sockdir):
    started = []

    def make(pause_s: float = 0.05, extra: dict | None = None, **kwargs):
        options = dict(OPTIONS, **(extra or {}))
        node = Node(
            node_config(fake.url, f"{sockdir}/node.sock", **kwargs),
            lambda c: StorageService(c, **options),
            pause_s=pause_s,
        )
        node.kept = []
        thread = threading.Thread(
            target=lambda: node.kept.append(node.run()), daemon=True
        )
        thread.start()
        started.append((node, thread))
        return node

    yield make
    for node, thread in started:
        node.stop()
        thread.join(15)


@pytest.mark.parametrize("role", ["gpu", "memory"])
def test_node_keeps_ownership_between_runs(fake, runs, monkeypatch, role):
    paused, resume = threading.Event(), threading.Event()

    def reclaim():
        paused.set()
        assert resume.wait(10)

    monkeypatch.setattr(service_module.gc_freeze, "reclaim", reclaim)
    node = runs(role=role)
    try:
        wait_until(lambda: fake.requests(MessageType.NODE_HEARTBEAT))
        fake.restart()
        assert paused.wait(10)
        assert node.service is None
        opens = len(fake.requests(MessageType.NODE_OPEN))
        with pytest.raises(LocalServerError, match="another StorageService holds"):
            start_node(node.config)
        assert len(fake.requests(MessageType.NODE_OPEN)) == opens
    finally:
        resume.set()
    wait_until(
        lambda: (
            node.runs == 2
            and node.service is not None
            and node.service.server is not None
            and node.service.server.session is not None
        )
    )
    node.stop()
    wait_until(lambda: node.kept == [True])
    replacement = start_node(node.config)
    replacement.stop()


@pytest.fixture
def short_dram(monkeypatch):
    """DramDriver.create fails as memory short ``fails[0]`` times."""
    monkeypatch.setattr(service_module, "DRAM_RETRY_S", 0.02)
    real = DramDriver.create
    fails = [0]

    def create(self, tier, **kwargs):
        if fails[0]:
            fails[0] -= 1
            raise DramPoolError("cannot allocate: ENOMEM", short=True)
        return real(self, tier, **kwargs)

    monkeypatch.setattr(DramDriver, "create", create)
    return fails


def test_a_later_run_waits_while_workers_map_the_old_pool(
    fake, runs, short_dram, caplog
):
    log = logged(caplog, "maru_storage_service.service")
    try:
        node = runs()
        first = new_run(node, None)
        pool = first.tier("dram").pool
        old = mmap.mmap(pool.fd, pool.capacity, flags=mmap.MAP_SHARED)
        short_dram[0] = 3
        fake.restart()
        second = new_run(node, first)
        old.close()
    finally:
        log.removeHandler(caplog.handler)
    assert short_dram == [0] and second.later_run and not node.kept
    hints = {id(r) for r in caplog.records if "may still map" in r.getMessage()}
    assert len(hints) == 1  # once per DRAM_LOG_S


class OldMapping:
    """An earlier run's pool, mapped by a worker until ``release``."""

    def __init__(self):
        self.pool = dram.create_dram_pool("n", "old", 2 * M, M, hugepage=False)
        self.mapping = mmap.mmap(self.pool.fd, self.pool.capacity)

    def release(self):
        self.mapping.close()

    def close(self):
        if not self.mapping.closed:
            self.mapping.close()
        self.pool.close()


@pytest.fixture
def dram_service(monkeypatch):
    """A later run's service whose DRAM allocation fails while the old
    mapping is held; (service, old mapping, creates made)."""
    monkeypatch.setattr(service_module, "DRAM_RETRY_S", 0.1)
    monkeypatch.setattr(service_module, "DRAM_RETRY_MAX_S", 0.5)
    old, creates = OldMapping(), []

    def create(self, tier, **kwargs):
        creates.append(1)
        if not old.mapping.closed:
            raise DramPoolError("cannot allocate: ENOMEM", short=True)
        return "pool"

    monkeypatch.setattr(DramDriver, "create", create)
    service = StorageService(node_config("tcp://127.0.0.1:1", "/tmp/x.sock"))
    service.later_run = True
    yield service, old, creates
    old.close()


def test_a_later_run_waits_with_a_growing_backoff_until_the_old_mapping_goes(
    dram_service,
):
    service, old, creates = dram_service
    pauses = []

    def wait(pause):
        pauses.append(pause)
        if len(pauses) == 5:
            old.release()  # the worker unmaps the old pool
        return False

    service._interrupted.wait = wait
    tier = service._make(service.config.top)
    service._open(tier)
    assert tier.pools == ["pool"]
    assert pauses == [0.1, 0.2, 0.4, 0.5, 0.5]  # doubled up to the cap
    assert len(creates) == 6


def test_a_stop_ends_the_wait_in_the_middle_of_it(dram_service):
    service, old, creates = dram_service
    service._interrupted.wait = lambda pause: len(creates) >= 2
    tier = service._make(service.config.top)
    with pytest.raises(ServerStopped):
        service._open(tier)
    assert len(creates) == 2 and not old.mapping.closed


def test_a_stop_during_a_long_wait_returns_at_once(dram_service, monkeypatch):
    service, old, creates = dram_service
    monkeypatch.setattr(service_module, "DRAM_RETRY_S", 30.0)
    tier = service._make(service.config.top)
    threading.Timer(0.1, service.interrupt).start()
    start = time.monotonic()
    with pytest.raises(ServerStopped):
        service._open(tier)
    assert time.monotonic() - start < 5


def test_a_stop_ends_the_wait_for_the_dram_pool(fake, runs, short_dram):
    node = runs()
    new_run(node, None)
    short_dram[0] = 10**6
    fake.restart()
    wait_until(lambda: node.runs == 2, what="the next run")
    node.stop()
    wait_until(lambda: node.kept, what="run() returned")
    assert node.kept == [True]


def test_the_first_run_waits_out_a_shortage_but_not_a_bad_config(
    fake, sockdir, short_dram, monkeypatch
):
    short_dram[0] = 2
    service = StorageService(node_config(fake.url, f"{sockdir}/node.sock"), **OPTIONS)
    service.start()
    assert short_dram == [0]
    service.stop()

    def bad(self, tier, **kwargs):
        raise DramPoolError("not a multiple of the huge page size")

    monkeypatch.setattr(DramDriver, "create", bad)
    service = StorageService(node_config(fake.url, f"{sockdir}/node.sock"), **OPTIONS)
    with pytest.raises(DramPoolError):
        service.start()


def test_a_teardown_exception_ends_the_node(fake, runs, monkeypatch):
    node = runs()
    first = new_run(node, None)
    real = StorageService._teardown

    def broken(self, mode):
        real(self, mode)
        raise RuntimeError("boom")

    monkeypatch.setattr(StorageService, "_teardown", broken)
    fake.restart()
    wait_until(lambda: node.kept, timeout=15, what="run() returned")
    assert node.kept == [False] and "boom" in node.failed
    assert first.wait_finished(0) and node.runs == 1


def test_no_renewal_for_half_the_lease_leaves_then_starts_a_new_run(fake, runs):
    rm = FakeRm(lease_s=0.4)
    extra = {"region_source": rm, "cxl_renew_s": 0.02, "map_region": anon_region}
    node = runs(pause_s=0.3, extra=extra, cxl_pages=4, region_pages=2)
    first = new_run(node, None)
    rm.renew_error = ConnectionRefusedError("RM down")
    wait_until(lambda: first.ended, what="the run ended")
    rm.renew_error = None
    second = new_run(node, first)
    assert "no lease renewal" in first.ended and first.new_run
    assert rm.released == [("gpu-test", first.node_epoch)]  # left
    assert rm.epochs["gpu-test"] == second.node_epoch


def test_a_claim_lost_while_allocating_regions_ends_the_node(fake, sockdir):
    rm = FakeRm()

    def stolen(node_id, node_epoch, size, dax_path=""):
        raise StaleNodeEpochError("another run claimed the node")

    rm.node_alloc = stolen
    node = Node(
        node_config(fake.url, f"{sockdir}/node.sock", cxl_pages=4, region_pages=2),
        lambda c: StorageService(
            c, region_source=rm, cxl_renew_s=0.02, map_region=anon_region, **OPTIONS
        ),
    )
    assert node.run() is False
    assert "another run claimed" in node.ended and node.runs == 1
