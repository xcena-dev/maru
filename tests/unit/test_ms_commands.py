# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Role commands, config output, exit codes and process signals."""

import json
import signal
import subprocess
import sys
import threading
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest

from maru_common.protocol import MessageType
from maru_common.storage_types import CloseMode
from maru_server.rpc_server import RpcServer
from maru_storage_service import service as service_module
from maru_storage_service.cli import main
from maru_storage_service.config import load_node_config, parse_node_config
from maru_storage_service.server_client import NodeOpenError
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import (
    PAGE,
    WORKTREE,
    free_port,
    remove_tree,
    short_tmpdir,
    wait_until,
)


@pytest.fixture(params=["gpu", "memory"])
def role(request):
    return request.param


@pytest.fixture
def setup(role, tmp_path):
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


def argv(role, path, *args):
    with open(Path(WORKTREE) / "pyproject.toml", "rb") as f:
        entry = tomllib.load(f)["project"]["scripts"]["maru-storage"]
    module, function = entry.split(":")
    other = "memory" if role == "gpu" else "gpu"
    code = (
        "import sys; from importlib import import_module; "
        f"result = getattr(import_module({module!r}), {function!r})(sys.argv[1:]); "
        f"assert not any(n.startswith('maru_storage_service.{other}') for n in sys.modules); "
        "sys.exit(result)"
    )
    return [sys.executable, "-c", code, "--role", role, "--config", str(path), *args]


def test_check_uses_only_role_keys_without_creating_state(role, setup, capsys):
    raw, path, _ = setup
    path.write_text(json.dumps(raw))
    assert main(["--role", role, "--config", str(path), "--check"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert "role" not in out and out["state_dir"] == raw["state_dir"]
    assert ("socket" in out) is (role == "gpu")
    assert ("remote_server" in out) is (role == "memory")
    assert ("credit_max_bytes" in out["budgets"]) is (role == "gpu")
    assert ("high_watermark" in out["tiers"][0]) is (role == "memory")
    assert parse_node_config(out, role=role) == load_node_config(path, role=role)
    assert not Path(raw["state_dir"]).exists()
    result = subprocess.run(
        argv(role, path, "--check"), capture_output=True, text=True, timeout=15
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == out


@pytest.mark.parametrize("check", [False, True])
@pytest.mark.parametrize("value", ["gpu", "memory", "x", None])
def test_role_key_is_rejected(role, setup, capsys, check, value):
    raw, path, _ = setup
    raw["role"] = value
    path.write_text(json.dumps(raw))
    assert (
        main(["--role", role, "--config", str(path), *(["--check"] if check else [])])
        == 2
    )
    err = capsys.readouterr().err
    assert (
        f"maru-storage --role {role}:" in err and "config.role: run maru-storage" in err
    )
    assert "--role memory" in err


@pytest.mark.parametrize("check", [False, True])
def test_other_roles_keys_are_rejected(role, setup, capsys, check):
    raw, path, _ = setup
    cases = (
        [("remote_server", None)]
        if role == "gpu"
        else [("socket", "x"), ("socket_group", None), ("local_pin", False)]
    )
    for key, value in cases:
        path.write_text(json.dumps({**raw, key: value}))
        assert (
            main(
                ["--role", role, "--config", str(path), *(["--check"] if check else [])]
            )
            == 2
        )
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
    assert main(["--role", "memory", "--config", str(path), "--check"]) == 2
    assert key in capsys.readouterr().err


@pytest.mark.parametrize("check", [False, True])
def test_unservable_config_exit(role, setup, capsys, check):
    raw, path, _ = setup
    raw["tiers"] = [{"store": "local_cxl"}]
    raw["budgets"] = {
        "credit_total_max_bytes": PAGE,
        "commit_pending_max_bytes": PAGE,
        "transfer_reserve_bytes": PAGE,
    }
    path.write_text(json.dumps(raw))
    assert (
        main(["--role", role, "--config", str(path), *(["--check"] if check else [])])
        == 3
    )
    assert (
        f"maru-storage --role {role}: the local_cxl tier needs a size"
        in capsys.readouterr().err
    )


@pytest.mark.parametrize(
    "failed,error,expected",
    [(None, True, 4), (None, False, 5), ("boom", False, 6), ("boom", True, 6)],
)
def test_runtime_exit_codes(role, setup, monkeypatch, capsys, failed, error, expected):
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
    assert main(["--role", role, "--config", str(path)]) == expected
    assert f"maru-storage --role {role}:" in capsys.readouterr().err


def test_state_dir_error_prevents_start(role, setup, capsys):
    raw, path, server = setup
    Path(raw["state_dir"]).write_text("not a directory")
    path.write_text(json.dumps(raw))
    assert main(["--role", role, "--config", str(path)]) == 4
    err = capsys.readouterr().err
    assert f"maru-storage --role {role}: cannot start:" in err
    assert "create a writable directory or set config.state_dir" in err
    assert server.requests(MessageType.NODE_OPEN) == []


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT])
def test_command_restarts_and_stops_on_signal(role, setup, tmp_path, sig):
    raw, path, server = setup
    path.write_text(json.dumps(raw))
    log = tmp_path / "node.log"
    with log.open("wb") as out:
        proc = subprocess.Popen(
            argv(role, path), stdout=out, stderr=subprocess.STDOUT, cwd=WORKTREE
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


def test_default_role_rejects_memory_config(tmp_path, capsys):
    path = tmp_path / "memory.json"
    path.write_text(json.dumps({"remote_server": {}}))
    assert main(["--config", str(path)]) == 2
    assert (
        "config.remote_server: requires maru-storage --role memory"
        in capsys.readouterr().err
    )


def test_invalid_role(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--role", "both", "--config", "node.json"])
    assert exc.value.code == 2
    assert "invalid choice" in capsys.readouterr().err


def test_cxl_server_refuses_node_service(role, setup):
    raw, path, _ = setup
    rpc = RpcServer(SimpleNamespace(rm_address="127.0.0.1:1"), port=free_port())
    raw["server"] = rpc.address
    path.write_text(json.dumps(raw))
    thread = threading.Thread(target=rpc.start)
    thread.start()
    try:
        result = subprocess.run(
            argv(role, path), capture_output=True, text=True, timeout=10
        )
        assert result.returncode == 4, result.stderr
        assert f"{rpc.address} is not a --backend node server" in result.stderr
    finally:
        rpc.stop()
        thread.join(5)
