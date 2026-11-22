# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Workers in their own processes: sharing, kill -9, restart, the CLI."""

import json
import os
import signal
import subprocess
import sys

import pytest

from maru_common.protocol import MessageType
from maru_common.storage_types import CloseMode, Key
from maru_storage_service.cli import EXIT_START
from maru_storage_service.drivers import dram
from maru_storage_service.pool_manager import PageState
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import (
    PAGE,
    WORKTREE,
    Agent,
    listening,
    node_config,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)


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
def agents(sockdir):
    started = []

    def make(socket_path, engine="eng", read_only=False):
        agent = Agent(socket_path, engine=engine, log_dir=sockdir, read_only=read_only)
        started.append(agent)
        return agent

    yield make
    for agent in started:
        agent.stop()


def value(key: str) -> str:
    return (key * 4).encode().hex()


def store(agent, key):
    agent.call("wait_credit", bytes=PAGE)
    assert agent.call("store", key=key, value=value(key)) is True


def test_two_processes_share_one_pool(fake, sockdir, agents):
    config = node_config(fake.url, f"{sockdir}/node.sock", pages=32, high=1.0, low=0.95)
    node = start_node(config)
    try:
        a = agents(config.socket, "engine-a")
        b = agents(config.socket, "engine-b")
        a.connect()
        for i in range(18):  # more than half of the pool: nothing split in advance
            store(a, f"a{i}")
        b.connect()
        for i in range(4):
            store(b, f"b{i}")
        assert (
            a.call("state")["pools"]
            == b.call("state")["pools"]
            == [node.tier("dram").pool.pool_id]
        )
        assert node.pools.bytes_in(PageState.READY) == 22 * PAGE
        for reader, key in ((b, "a3"), (b, "a17"), (a, "b0")):
            assert reader.call("retrieve", key=key, hold=False) == value(key)
        node.pools.check()
    finally:
        node.stop()


def test_pins_a_closed_worker_holds_are_released_when_it_exits(fake, sockdir, agents):
    config = node_config(fake.url, f"{sockdir}/node.sock")
    node = start_node(config)
    try:
        w = agents(config.socket, "holder")
        w.connect()
        store(w, "h0")
        assert w.call("retrieve", key="h0") == value("h0")  # held
        assert w.call("close_holding")
        assert fake.pin_count(w.incarnation) == 1
        assert node.manager.workers.get(w.incarnation) is not None  # no DETACH
        w.stop()
        wait_until(lambda: fake.pin_count(w.incarnation) == 0, what="WORKER_EXIT")
    finally:
        node.stop()


def test_kill_9_of_a_writer(fake, sockdir, agents):
    config = node_config(fake.url, f"{sockdir}/node.sock")
    node = start_node(config, gpu_drain_grace_s=1.0)
    try:
        w = agents(config.socket, "writer")
        w.connect()
        for key in ("w0", "w1", "w2"):
            store(w, key)
        assert w.call("retrieve", key="w0") == value("w0")  # a pin it holds
        w.call("wait_credit", bytes=PAGE)
        assert w.call("alloc_hold", name="half", size=100)  # an unfinished write
        w.call("wait_credit", bytes=PAGE)  # and unused credit
        held = node.pools.held_bytes(w.incarnation)
        assert held >= 2 * PAGE
        assert fake.pin_count(w.incarnation) == 1
        w.kill()
        wait_until(
            lambda: node.pools.bytes_in(PageState.QUARANTINED) == held,
            what="quarantined",
        )
        assert node.pools.held_bytes(w.incarnation) == 0
        # its GPU may still read during the grace: its pins stay until then
        assert fake.pin_count(w.incarnation) == 1
        assert not any(r.incarnation == w.incarnation for r, _ in fake.worker_exits)
        wait_until(
            lambda: any(r.incarnation == w.incarnation for r, _ in fake.worker_exits),
            what="WORKER_EXIT",
        )
        ((ref, cause),) = [
            x for x in fake.worker_exits if x[0].incarnation == w.incarnation
        ]
        assert cause == "exit" and ref.pid == w.pid
        assert fake.pin_count(w.incarnation) == 0
        # the pages are free once the grace is over too
        wait_until(
            lambda: node.pools.bytes_in(PageState.QUARANTINED) == 0, what="freed"
        )
        # its committed KV stays and another worker reads it
        assert {Key("ns", k) for k in ("w0", "w1", "w2")} <= fake.keys()
        r = agents(config.socket, "reader")
        r.connect()
        for key in ("w0", "w1", "w2"):
            assert r.call("retrieve", key=key, hold=False) == value(key)
        node.pools.check()
    finally:
        node.stop()


def test_kill_9_of_a_reader_that_never_stored(fake, sockdir, agents):
    config = node_config(fake.url, f"{sockdir}/node.sock")
    node = start_node(config)
    try:
        w = agents(config.socket, "writer")
        w.connect()
        store(w, "k")
        r = agents(config.socket, "reader", read_only=True)
        r.connect()
        assert r.call("retrieve", key="k") == value("k")  # holds a pin
        assert node.pools.held_bytes(r.incarnation) == 0  # never asked credit
        assert fake.pin_count(r.incarnation) == 1
        r.kill()
        wait_until(
            lambda: any(x.incarnation == r.incarnation for x, _ in fake.worker_exits),
            what="WORKER_EXIT",
        )
        ((ref, cause),) = [
            x for x in fake.worker_exits if x[0].incarnation == r.incarnation
        ]
        assert ref.worker_session is None and cause == "exit"
        assert fake.pin_count(r.incarnation) == 0
    finally:
        node.stop()


def test_storage_service_restart(fake, sockdir, agents):
    """The new run serves at once: a worker attaches again by itself, and
    nothing waits for its copies into the old run's DRAM."""
    config = node_config(fake.url, f"{sockdir}/node.sock")
    first = start_node(config)
    second = None
    try:
        x = agents(config.socket, "stays")
        x.connect()
        store(x, "x0")
        x.call("wait_credit", bytes=PAGE)
        x.call("alloc_hold", name="copy", size=64)  # a copy into old credit
        first.stop(CloseMode.RESTART)
        second = start_node(config)
        wait_until(
            lambda: x.call("state")["node_epoch"] == second.node_epoch,
            what="attached again",
        )
        # DRAM locations of the old run are gone; the new pool works
        assert x.call("retrieve", key="x0") is None
        store(x, "x1")
        assert x.call("retrieve", key="x1", hold=False) == value("x1")
        x.call("free_held", name="copy")
        second.pools.check()
    finally:
        first.stop()
        if second is not None:
            second.stop()


def _cli(config_path: str, log_path: str) -> subprocess.Popen:
    env = dict(os.environ)
    env["PYTHONPATH"] = WORKTREE + os.pathsep + env.get("PYTHONPATH", "")
    with open(log_path, "wb") as log:
        return subprocess.Popen(
            [sys.executable, "-m", "maru_storage_service", "--config", config_path],
            env=env,
            cwd=WORKTREE,
            stdout=log,
            stderr=subprocess.STDOUT,
        )


def _log(path: str) -> str:
    with open(path, "rb") as f:
        return f.read().decode(errors="replace")


def _write_config(path: str, raw: dict) -> str:
    with open(path, "w") as f:
        json.dump(raw, f)
    return path


def _raw_config(fake, socket_path, hugepage=False, size=16 * PAGE):
    return {
        "node_id": "cli-node",
        "socket": socket_path,
        "server": fake.url,
        "page_size": PAGE,
        "tiers": [{"store": "dram", "size": size, "hugepage": hugepage}],
        "budgets": {
            "credit_max_bytes": 4 * PAGE,
            "credit_total_max_bytes": 8 * PAGE,
            "credit_refill_low": PAGE,
            "credit_refill_high": 4 * PAGE,
            "commit_pending_max_bytes": 4 * PAGE,
            "transfer_reserve_bytes": PAGE,
        },
    }


def test_cli_serves_and_closes_on_sigterm(fake, sockdir, agents):
    socket_path = f"{sockdir}/cli.sock"
    path = _write_config(f"{sockdir}/node.json", _raw_config(fake, socket_path))
    log_path = f"{sockdir}/node.log"
    proc = _cli(path, log_path)
    try:
        wait_until(
            lambda: listening(socket_path) or proc.poll() is not None,
            what="node listening",
        )
        assert proc.poll() is None, _log(log_path)
        a = agents(socket_path)
        a.connect()
        store(a, "cli")
        assert a.call("retrieve", key="cli", hold=False) == value("cli")
        a.call("close")
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(15) == 0
        (close,) = fake.requests(MessageType.NODE_CLOSE)
        assert close.mode == CloseMode.RESTART
        assert not os.path.exists(socket_path)
    except BaseException:
        print("maru-storage output:\n" + _log(log_path))
        raise
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def test_cli_fails_clearly_without_huge_pages(fake, sockdir):
    huge = dram.default_huge_page_size() or 2 << 20
    if dram._meminfo().get("HugePages_Total", 0) >= 8:
        pytest.skip("this host reserves huge pages: a shortage is waited out")
    raw = _raw_config(fake, f"{sockdir}/h.sock", hugepage=True, size=8 * huge)
    raw["page_size"] = huge
    raw["budgets"] = {
        "credit_max_bytes": huge,
        "credit_total_max_bytes": huge,
        "credit_refill_low": huge,
        "credit_refill_high": huge,
        "commit_pending_max_bytes": huge,
        "transfer_reserve_bytes": huge,
    }
    log_path = f"{sockdir}/h.log"
    proc = _cli(_write_config(f"{sockdir}/h.json", raw), log_path)
    assert proc.wait(30) == EXIT_START, _log(log_path)
    assert "huge pages" in _log(log_path)
    assert fake.requests(MessageType.NODE_OPEN) == []  # nothing registered
