# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Role commands, config output, exit codes and process signals."""

import json
import signal
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

from maru_common.protocol import MessageType
from maru_common.storage_types import CloseMode
from maru_storage_service import service as service_module
from maru_storage_service.cli import main, memory_main
from maru_storage_service.config import load_node_config, parse_node_config
from maru_storage_service.server_client import NodeOpenError
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import (
    PAGE,
    WORKTREE,
    remove_tree,
    short_tmpdir,
    wait_until,
)


@pytest.fixture(
    params=[("gpu", "maru-storage", main), ("memory", "maru-memory-node", memory_main)]
)
def command(request):
    return request.param


@pytest.fixture
def setup(command, tmp_path):
    role, _, _ = command
    sockdir = short_tmpdir()
    server = FakeMaruServer(heartbeat_s=0.05)
    raw = {
        "node_id": "command-node",
        "state_dir": str(tmp_path / "state"),
        "server": server.url,
        "page_size": PAGE,
        "tiers": [{"store": "dram", "size": 16 * PAGE, "hugepage": False}],
    }
    if role == "gpu":
        raw["socket"] = f"{sockdir}/node.sock"
    path = tmp_path / "node.json"
    yield raw, path, server
    server.close()
    remove_tree(sockdir)


def argv(command, path, *args):
    role, name, _ = command
    with open(Path(WORKTREE) / "pyproject.toml", "rb") as f:
        entry = tomllib.load(f)["project"]["scripts"][name]
    module, function = entry.split(":")
    other = "memory" if role == "gpu" else "gpu"
    code = (
        "import sys; from importlib import import_module; "
        f"result = getattr(import_module({module!r}), {function!r})(sys.argv[1:]); "
        f"assert not any(n.startswith('maru_storage_service.{other}') for n in sys.modules); "
        "sys.exit(result)"
    )
    return [sys.executable, "-c", code, "--config", str(path), *args]


def test_check_uses_only_role_keys_without_creating_state(command, setup, capsys):
    role, _, run = command
    raw, path, _ = setup
    path.write_text(json.dumps(raw))
    assert run(["--config", str(path), "--check"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert "role" not in out and out["state_dir"] == raw["state_dir"]
    assert ("socket" in out) is (role == "gpu")
    assert ("remote_server" in out) is (role == "memory")
    assert ("credit_max_bytes" in out["budgets"]) is (role == "gpu")
    assert ("high_watermark" in out["tiers"][0]) is (role == "memory")
    assert parse_node_config(out, role=role) == load_node_config(path, role=role)
    assert not Path(raw["state_dir"]).exists()
    result = subprocess.run(
        argv(command, path, "--check"), capture_output=True, text=True, timeout=15
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == out


@pytest.mark.parametrize("check", [False, True])
@pytest.mark.parametrize("value", ["gpu", "memory", "x", None])
def test_role_key_is_rejected(command, setup, capsys, check, value):
    _, name, run = command
    raw, path, _ = setup
    raw["role"] = value
    path.write_text(json.dumps(raw))
    assert run(["--config", str(path), *(["--check"] if check else [])]) == 2
    err = capsys.readouterr().err
    assert f"{name}:" in err and "config.role: run maru-storage" in err
    assert "maru-memory-node for a memory node" in err


@pytest.mark.parametrize("check", [False, True])
def test_other_roles_keys_are_rejected(command, setup, capsys, check):
    role, _, run = command
    raw, path, _ = setup
    cases = (
        [("remote_server", None)]
        if role == "gpu"
        else [("socket", "x"), ("socket_group", None), ("local_pin", False)]
    )
    for key, value in cases:
        path.write_text(json.dumps({**raw, key: value}))
        assert run(["--config", str(path), *(["--check"] if check else [])]) == 2
        assert key in capsys.readouterr().err


@pytest.mark.parametrize(
    "key",
    [
        "credit_max_bytes",
        "credit_refill_low",
        "credit_refill_high",
        "fetch_request_max_bytes",
        "fetch_max_inflight_bytes",
    ],
)
def test_memory_command_refuses_worker_budgets(tmp_path, capsys, key):
    raw = {
        "node_id": "mem",
        "server": "tcp://localhost:1",
        "page_size": PAGE,
        "tiers": [{"store": "dram", "size": 16 * PAGE}],
        "budgets": {key: PAGE},
    }
    path = tmp_path / "node.json"
    path.write_text(json.dumps(raw))
    assert memory_main(["--config", str(path), "--check"]) == 2
    assert key in capsys.readouterr().err


@pytest.mark.parametrize("check", [False, True])
def test_unservable_config_exit(command, setup, capsys, check):
    _, name, run = command
    raw, path, _ = setup
    raw["tiers"] = [{"store": "local_cxl"}]
    raw["budgets"] = {
        "credit_total_max_bytes": PAGE,
        "commit_pending_max_bytes": PAGE,
        "transfer_reserve_bytes": PAGE,
    }
    path.write_text(json.dumps(raw))
    assert run(["--config", str(path), *(["--check"] if check else [])]) == 3
    assert f"{name}: the local_cxl tier needs a size" in capsys.readouterr().err


@pytest.mark.parametrize(
    "failed,error,expected",
    [(None, True, 4), (None, False, 5), ("boom", False, 6), ("boom", True, 6)],
)
def test_runtime_exit_codes(
    command, setup, monkeypatch, capsys, failed, error, expected
):
    _, name, run = command
    raw, path, _ = setup
    path.write_text(json.dumps(raw))

    class Ended:
        def __init__(self, config):
            self.failed, self.ended = failed, "node lost"

        def run(self):
            if error:
                raise NodeOpenError("refused")
            return False

    monkeypatch.setattr(service_module, "Node", Ended)
    assert run(["--config", str(path)]) == expected
    assert f"{name}:" in capsys.readouterr().err


def test_state_dir_error_prevents_start(command, setup, capsys):
    _, name, run = command
    raw, path, server = setup
    Path(raw["state_dir"]).write_text("not a directory")
    path.write_text(json.dumps(raw))
    assert run(["--config", str(path)]) == 4
    err = capsys.readouterr().err
    assert f"{name}: cannot start:" in err
    assert "create a writable directory or set config.state_dir" in err
    assert server.requests(MessageType.NODE_OPEN) == []


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT])
def test_command_restarts_and_stops_on_signal(command, setup, tmp_path, sig):
    role, _, _ = command
    raw, path, server = setup
    path.write_text(json.dumps(raw))
    log = tmp_path / "node.log"
    with log.open("wb") as out:
        proc = subprocess.Popen(
            argv(command, path), stdout=out, stderr=subprocess.STDOUT, cwd=WORKTREE
        )
    try:
        wait_until(
            lambda: (
                server.requests(MessageType.NODE_HEARTBEAT) or proc.poll() is not None
            )
        )
        assert proc.poll() is None, log.read_text()
        assert Path(raw["state_dir"], "command-node.lock").is_file()
        assert Path(raw.get("socket", "/nonexistent")).exists() is (role == "gpu")
        first = server.requests(MessageType.NODE_OPEN)[0].node_epoch
        server.restart()
        wait_until(
            lambda: (
                len(server.requests(MessageType.NODE_OPEN)) >= 2
                or proc.poll() is not None
            )
        )
        assert proc.poll() is None, log.read_text()
        assert server.requests(MessageType.NODE_OPEN)[-1].node_epoch != first
        wait_until(
            lambda: (
                log.read_text().count(f"serving {role} node command-node") == 2
                or proc.poll() is not None
            )
        )
        proc.send_signal(sig)
        assert proc.wait(15) == 0, log.read_text()
        assert server.requests(MessageType.NODE_CLOSE)[-1].mode == CloseMode.RESTART
        if role == "gpu":
            assert not Path(raw["socket"]).exists()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
