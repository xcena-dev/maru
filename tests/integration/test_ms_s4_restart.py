# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Metadata and storage restarts with worker processes and held DRAM reads."""

import hashlib
import json
import os
import signal
import subprocess
import threading
import time

import pytest

from maru_storage_service.pool_manager import PageState
from maru_storage_service.service import Node, StorageService
from tests.integration.ms_l1_helpers import command_argv
from tests.integration.test_ms_s2_real_server import (
    RealMaruServer,
    _raw_config,
    subprocess_env,
)
from tests.unit.ms_node_helpers import (
    PAGE,
    WORKTREE,
    Agent,
    AgentError,
    listening,
    node_config,
    remove_tree,
    short_tmpdir,
    wait_until,
)
from tests.unit.test_ms_new_run import new_run

pytestmark = pytest.mark.integration

SMALL_CREDIT = {
    "credit_max_bytes": 2 * PAGE,
    "credit_total_max_bytes": 4 * PAGE,
    "credit_refill_low": 1 * PAGE,
    "credit_refill_high": 2 * PAGE,
    "commit_pending_max_bytes": 2 * PAGE,
}


@pytest.fixture
def workdir():
    path = short_tmpdir()
    yield path
    remove_tree(path)


@pytest.fixture
def servers(workdir):
    made = []

    def make(port=None):
        server = RealMaruServer(workdir, 6.0, 0.3, port=port)
        made.append(server)
        return server

    yield make
    for server in made:
        server.close()


@pytest.fixture
def nodes(workdir):
    """A Node serving its runs on a thread of its own."""
    made = []

    def make(server, **kwargs):
        node = Node(
            node_config(server.url, f"{workdir}/node.sock", **kwargs),
            lambda c: StorageService(c, rpc_timeout_s=0.5),
            pause_s=0.2,
        )
        thread = threading.Thread(target=node.run, daemon=True)
        thread.start()
        made.append((node, thread))
        new_run(node, None)
        return node

    yield make
    for node, thread in made:
        node.stop()
        thread.join(30)


@pytest.fixture
def agents(workdir):
    made = []

    def make(socket_path: str, engine: str = "eng", connect=True, **kwargs):
        agent = Agent(socket_path, engine=engine, log_dir=workdir, **kwargs)
        made.append(agent)
        if connect:
            agent.connect()
        return agent

    yield make
    for agent in made:
        if agent.proc.poll() is None:
            agent.proc.send_signal(signal.SIGCONT)
        agent.stop()


def page_value(key: str) -> bytes:
    """A whole page of bytes that depend on the key."""
    seed = hashlib.sha256(key.encode()).digest()
    return (seed * (PAGE // len(seed) + 1))[:PAGE]


def store(agent, key: str) -> bool:
    agent.call("wait_credit", bytes=PAGE)
    return agent.call("store", key=key, value=page_value(key).hex())


def try_store(agent, key: str) -> bool:
    """A store that is skipped when no credit is at hand."""
    try:
        agent.call("wait_credit", bytes=PAGE, wait_s=0.5)
    except AgentError:
        return False
    try:
        return agent.call("store", key=key, value=page_value(key).hex())
    except AgentError:
        return False


def readable(agent, key: str) -> bool:
    got = agent.call("retrieve", key=key, hold=False)
    assert got in (None, page_value(key).hex())
    return got is not None


def used_share(service) -> float:
    """Share of the DRAM pool that is occupied."""
    (pool_id,) = service.pools.pool_ids()
    capacity = service.pools.pool_info(pool_id).capacity
    return 1 - service.pools.bytes_in(PageState.FREE, pool_id) / capacity


def restart_server(server, servers):
    """SIGKILL MaruServer and start a new one on the same port."""
    port = server.port
    server.kill()
    return servers(port=port)


def on_run(agent, service) -> None:
    """The worker re-attached to ``service``."""
    wait_until(
        lambda: agent.call("state")["node_epoch"] == service.node_epoch,
        timeout=30,
        what="attached to the new run",
    )


def test_maruserver_killed_during_reads_brings_a_new_run(servers, nodes, agents):
    """Old keys miss after the workers attach to a new run."""
    server = servers()
    node = nodes(server)
    first = node.service
    writer = agents(node.config.socket, "writer")
    reader = agents(node.config.socket, "reader", read_only=True)
    keys = [f"k{i}" for i in range(10)]
    for key in keys:
        assert store(writer, key)
    assert readable(reader, keys[0])
    server = restart_server(server, servers)
    second = new_run(node, first)
    assert "restarted" in first.ended
    on_run(writer, second)
    on_run(reader, second)
    assert not any(readable(reader, key) for key in keys)
    assert store(writer, "after")
    wait_until(lambda: readable(reader, "after"), timeout=30, what="after")
    assert reader.call("state")["server_epoch"] == server.server_epoch
    second.pools.check()


def test_a_pinned_page_never_changes_until_released(servers, nodes, agents):
    """An old memfd read survives while the new run fills its pool."""
    server = servers()
    node = nodes(server, budgets=SMALL_CREDIT)
    first = node.service
    writer = agents(node.config.socket, "writer")
    reader = agents(node.config.socket, "reader", read_only=True)
    assert store(writer, "pinned")
    expected = page_value("pinned").hex()
    assert reader.call("retrieve", key="pinned", hold=True) == expected
    for i in range(4):
        assert store(writer, f"a{i}")
    server = restart_server(server, servers)
    second = new_run(node, first)
    on_run(writer, second)
    stored = 0
    for i in range(16):
        stored += try_store(writer, f"b{i}")
        assert reader.call("held", key="pinned") == expected
    assert stored > 0
    assert second.pools.bytes_in(PageState.FREE) <= PAGE
    assert not second.manager.retire_results
    assert reader.call("held", key="pinned") == expected
    reader.call("release", key="pinned")
    assert server.admin_delete("ns", ["b0"]) == {"b0": "OK"}
    wait_until(lambda: not readable(writer, "b0"), what="space returned")
    assert store(writer, "later")
    on_run(reader, second)
    wait_until(lambda: readable(reader, "later"), timeout=30, what="later")
    second.pools.check()


def test_a_silent_worker_does_not_hold_up_the_new_run(servers, nodes, agents):
    server = servers()
    node = nodes(server, budgets=SMALL_CREDIT)
    first = node.service
    writer = agents(node.config.socket, "writer")
    silent = agents(node.config.socket, "silent", read_only=True)
    for i in range(6):
        assert store(writer, f"a{i}")
    silent.proc.send_signal(signal.SIGSTOP)  # alive, never answers
    server = restart_server(server, servers)
    second = new_run(node, first)
    on_run(writer, second)
    for i in range(16):
        try_store(writer, f"b{i}")
    assert used_share(second) >= 0.9
    assert not second.manager.retire_results
    assert readable(writer, "b0")
    silent.kill()


def _storage_cli(config_path: str, log_path: str) -> subprocess.Popen:
    with open(log_path, "ab") as log:
        return subprocess.Popen(
            command_argv("maru-storage", "--config", config_path),
            env=subprocess_env(),
            cwd=WORKTREE,
            stdout=log,
            stderr=subprocess.STDOUT,
        )


def _write_config(path: str, config) -> None:
    with open(path, "w") as f:
        json.dump(_raw_config(config), f)


def test_storage_service_killed_and_back(servers, workdir, agents):
    server = servers()
    socket_path = f"{workdir}/node.sock"
    config = node_config(server.url, socket_path)
    path = os.path.join(workdir, "node.json")
    log_path = os.path.join(workdir, "maru-storage.log")
    _write_config(path, config)
    proc = _storage_cli(path, log_path)
    try:
        wait_until(lambda: listening(socket_path), timeout=30, what="listening")
        worker = agents(socket_path, "worker")
        assert store(worker, "before")
        old_epoch = worker.call("state")["node_epoch"]
        proc.kill()  # SIGKILL
        proc.wait(10)
        proc = _storage_cli(path, log_path)
        wait_until(
            lambda: worker.call("state")["node_epoch"] not in (None, old_epoch),
            timeout=30,
            what="attached to the new run",
        )
        assert not readable(worker, "before")  # DRAM of the old run is dropped
        assert store(worker, "after")
        assert readable(worker, "after")
        assert worker.call("state")["off"] is None

        # Started again with smaller pages: the worker's objects no longer fit
        old_epoch = worker.call("state")["node_epoch"]
        proc.kill()
        proc.wait(10)
        _write_config(path, node_config(server.url, socket_path, page_size=PAGE // 2))
        proc = _storage_cli(path, log_path)
        wait_until(lambda: listening(socket_path), timeout=30, what="listening")
        wait_until(lambda: worker.call("state")["off"], timeout=30, what="off")
        state = worker.call("state")
        assert "larger than the page size" in state["off"]
        with pytest.raises(AgentError, match="Maru is off"):
            worker.call("store", key="no", value=page_value("no").hex())
        assert worker.call("retrieve", key="after", hold=False) is None
        attaches = state["stats"].get("attach", 0)
        time.sleep(2.0)
        assert worker.call("state")["stats"].get("attach", 0) == attaches
        # a worker whose objects fit is served by the new run
        small = agents(socket_path, "small", chunk=PAGE // 2)
        small.call("wait_credit", bytes=PAGE // 2)
        assert small.call("store", key="s", value=(b"s" * 64).hex())
        assert small.call("retrieve", key="s", hold=False) == (b"s" * 64).hex()
    except BaseException:
        with open(log_path, "rb") as f:
            print("maru-storage output:\n" + f.read().decode(errors="replace")[-6000:])
        raise
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(10)
