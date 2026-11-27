# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Node and socket locks across starts, run gaps and teardown failures."""

import dataclasses
import errno
import os
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from maru_common.protocol import MessageType
from maru_common.storage_types import CloseMode
from maru_storage_service import node as node_module
from maru_storage_service import node_run as run_module
from maru_storage_service import ownership
from maru_storage_service.config import DEFAULT_STATE_DIR, parse_node_config
from maru_storage_service.ownership import LocalServerError
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


def probe(path, locked):
    code = "import fcntl,sys; f=open(sys.argv[1], 'r+'); fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)"
    result = subprocess.run(
        [sys.executable, "-c", code, str(path)],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == (1 if locked else 0), result.stderr
    if locked:
        assert "BlockingIOError" in result.stderr


def locks(config, locked=True):
    probe(Path(config.state_dir) / f"{config.node_id}.lock", locked)
    if config.role == "gpu":
        probe(f"{config.socket}.lock", locked)


@pytest.fixture(
    params=[
        ("gpu", "dram"),
        ("gpu", "local_cxl"),
        ("memory", "dram"),
        ("memory", "local_cxl"),
    ]
)
def setup(request, tmp_path):
    role, store = request.param
    sockdir = short_tmpdir()
    server = FakeMaruServer(heartbeat_s=0.05)
    config = dataclasses.replace(
        node_config(
            server.url,
            f"{sockdir}/node.sock",
            role=role,
            cxl_pages=16 if store == "local_cxl" else None,
        ),
        state_dir=str(tmp_path / "state" / "nodes"),
    )
    options = {
        "region_source": FakeRm(),
        "map_region": anon_region,
        "cxl_renew_s": 0.02,
    }
    yield config, server, options
    server.close()
    remove_tree(sockdir)


def test_default_state_dir():
    config = parse_node_config(
        {
            "node_id": "n",
            "server": "tcp://localhost:1",
            "page_size": "1M",
            "tiers": [{"store": "dram", "size": "64M"}],
        }
    )
    assert config.state_dir == DEFAULT_STATE_DIR == "/run/maru"


def test_node_and_socket_can_share_one_lock_file(tmp_path):
    lock = ownership.NodeOwnership(
        str(tmp_path), "node.sock", str(tmp_path / "node.sock")
    )
    try:
        lock.claim()
        probe(tmp_path / "node.sock.lock", True)
    finally:
        lock.close()
    probe(tmp_path / "node.sock.lock", False)


@pytest.mark.parametrize("operation", ["mkdir", "open"])
def test_state_dir_permission_error_names_the_fix(tmp_path, monkeypatch, operation):
    state = tmp_path / "state"
    if operation == "open":
        state.mkdir()

    def denied(*args, **kwargs):
        raise PermissionError(errno.EACCES, "Permission denied")

    with monkeypatch.context() as patch:
        patch.setattr(
            ownership.os, "makedirs" if operation == "mkdir" else "open", denied
        )
        with pytest.raises(
            LocalServerError,
            match="create a writable directory or set config.state_dir",
        ) as err:
            ownership.NodeOwnership(str(state), "n").claim()
    assert str(state) in str(err.value) and "Permission denied" in str(err.value)


def test_locks_precede_pool_creation_rm_claim_and_node_open(setup, monkeypatch):
    config, _, options = setup
    seen = []

    def guard(obj, name):
        real = getattr(obj, name)

        def checked(*args, **kwargs):
            locks(config)
            seen.append(name)
            return real(*args, **kwargs)

        monkeypatch.setattr(obj, name, checked)

    guard(StorageService, "_make")
    guard(run_module, "PoolManager")
    guard(run_module.ServerClient, "open")
    guard(options["region_source"], "node_claim")
    service = start_node(config, **options)
    try:
        assert Path(config.state_dir, f"{config.node_id}.lock").is_file()
        assert seen == (
            ["_make", "node_claim", "PoolManager", "open"]
            if config.top.store == "local_cxl"
            else ["_make", "PoolManager", "open"]
        )
        assert not os.get_inheritable(service._ownership._node._fd)
    finally:
        service.stop()
    locks(config, False)


def test_same_node_id_conflicts_with_a_different_socket(setup, monkeypatch):
    config, server, options = setup
    first = start_node(config, **options)
    try:
        second_config = dataclasses.replace(config, socket=f"{config.socket}.other")
        opens = len(server.requests(MessageType.NODE_OPEN))

        def unexpected(*args, **kwargs):
            pytest.fail("duplicate reached pool creation or RM claim")

        monkeypatch.setattr(StorageService, "_make", unexpected)
        monkeypatch.setattr(options["region_source"], "node_claim", unexpected)
        with pytest.raises(LocalServerError, match=f"{config.node_id}\\.lock"):
            start_node(second_config, **options)
        assert len(server.requests(MessageType.NODE_OPEN)) == opens
        locks(config)
    finally:
        first.stop()


def test_different_node_id_conflicts_with_the_same_gpu_socket(tmp_path):
    sockdir = short_tmpdir()
    server = FakeMaruServer()
    config = node_config(server.url, f"{sockdir}/node.sock")
    service = start_node(config)
    try:
        other = dataclasses.replace(
            config, node_id="other", state_dir=str(tmp_path / "other-state")
        )
        with pytest.raises(LocalServerError, match="node.sock.lock"):
            start_node(other)
        assert len(server.requests(MessageType.NODE_OPEN)) == 1
        probe(Path(other.state_dir) / "other.lock", False)
        locks(config)
    finally:
        service.stop()
        server.close()
        remove_tree(sockdir)


def test_locks_survive_run_gaps_and_release_after_stop(setup, monkeypatch):
    config, server, options = setup
    paused, resume = threading.Event(), threading.Event()
    result = []

    def reclaim():
        paused.set()
        assert resume.wait(10)

    monkeypatch.setattr(node_module.gc_freeze, "reclaim", reclaim)
    node = Node(config, lambda c: StorageService(c, **options), pause_s=0.01)
    thread = threading.Thread(target=lambda: result.append(node.run()))
    thread.start()
    try:
        wait_until(lambda: server.requests(MessageType.NODE_HEARTBEAT))
        locks(config)
        server.restart()
        assert paused.wait(10)
        assert node.service is None
        locks(config)
        if config.role == "gpu":
            other = dataclasses.replace(config, node_id="other")
            with pytest.raises(LocalServerError, match="node.sock.lock"):
                start_node(other, **options)
        resume.set()
        wait_until(
            lambda: (
                node.runs == 2
                and node.service is not None
                and node.service.store is not None
            )
        )
    finally:
        resume.set()
        node.stop()
        thread.join(15)
    assert result == [True]
    locks(config, False)
    replacement = start_node(config, **options)
    replacement.stop()
    locks(config, False)


@pytest.mark.parametrize("managed", [False, True])
def test_locks_survive_teardown_error(setup, monkeypatch, managed):
    config, _, options = setup
    run = StorageService(config, **options)
    node = Node(config, lambda c: run) if managed else None
    result = []
    thread = (
        threading.Thread(target=lambda: result.append(node.run())) if managed else None
    )
    if thread is None:
        run.start()
    else:
        thread.start()
        wait_until(lambda: run.store is not None and run.server.session is not None)
    real = run.role.stop_workers

    def broken():
        real()
        raise RuntimeError("boom")

    try:
        with monkeypatch.context() as patch:
            patch.setattr(run.role, "stop_workers", broken)
            if managed:
                node.stop()
                thread.join(15)
                assert result == [False] and node.runs == 1
            else:
                run.stop()
            assert run.wait_finished(0) and "boom" in run.teardown_failed
            locks(config)
    finally:
        run._teardown(CloseMode.RESTART)
        (node if managed else run)._ownership.close()
    locks(config, False)


def test_locks_survive_unfinished_teardown(setup, monkeypatch):
    config, _, options = setup
    run = StorageService(config, **options)
    node = Node(config, lambda c: run)
    entered, finish = threading.Event(), threading.Event()
    result = []
    thread = threading.Thread(target=lambda: result.append(node.run()))
    thread.start()
    wait_until(lambda: run.store is not None and run.server.session is not None)
    real = run.role.stop_workers

    def blocked():
        real()
        entered.set()
        assert finish.wait(10)

    try:
        monkeypatch.setattr(run.role, "stop_workers", blocked)
        monkeypatch.setattr(node_module, "STOP_WAIT_S", 0.01)
        node.stop()
        assert entered.wait(10)
        thread.join(5)
        assert result == [False] and not run.wait_finished(0)
        assert "did not end" in node.failed
        locks(config)
        finish.set()
        assert run.wait_finished(10)
        locks(config)
    finally:
        finish.set()
        thread.join(15)
        run.wait_finished(10)
        node._ownership.close()
