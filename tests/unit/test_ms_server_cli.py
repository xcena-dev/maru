# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""maru-node-server options, and a run's own server epoch."""

from unittest.mock import MagicMock, patch

import pytest

from maru_server.node_server import NodeServer, main


def run_main(*argv):
    with (
        patch("sys.argv", ["maru-node-server", *argv]),
        patch("maru_server.node_server.setup_logging"),
        patch("maru_server.node_server.gc_freeze") as freeze,
        patch("maru_server.node_server.NodeServer") as server_cls,
        patch("maru_server.node_server.NodeRpcServer") as rpc_cls,
        patch("signal.signal"),
    ):
        rpc_cls.return_value = MagicMock()
        main()
    freeze.start_freezer.assert_called_once()
    return server_cls, rpc_cls


def test_defaults():
    server_cls, rpc_cls = run_main()
    kwargs = server_cls.call_args.kwargs
    assert kwargs["session_ttl_s"] == 30
    assert kwargs["heartbeat_s"] == 10
    assert kwargs["node_give_up_s"] == 600
    assert kwargs["op_window"] == 65536
    assert kwargs["op_bytes_max"] == 64 << 20
    assert kwargs["policy_file"] is None
    rpc_kwargs = rpc_cls.call_args.kwargs
    assert (rpc_kwargs["host"], rpc_kwargs["port"]) == ("127.0.0.1", 5556)
    assert rpc_kwargs["num_workers"] == 4
    rpc_cls.return_value.start.assert_called_once()
    server_cls.return_value.close.assert_called_once()


def test_options_reach_the_server():
    server_cls, rpc_cls = run_main(
        "--host",
        "0.0.0.0",
        "--port",
        "6000",
        "--workers",
        "8",
        "--session-ttl-s",
        "12",
        "--heartbeat-s",
        "3",
        "--node-give-up-s",
        "60",
        "--op-window",
        "100",
    )
    kwargs = server_cls.call_args.kwargs
    assert (kwargs["session_ttl_s"], kwargs["heartbeat_s"]) == (12, 3)
    assert (kwargs["node_give_up_s"], kwargs["op_window"]) == (60, 100)
    rpc_kwargs = rpc_cls.call_args.kwargs
    assert (rpc_kwargs["host"], rpc_kwargs["port"]) == ("0.0.0.0", 6000)
    assert rpc_kwargs["num_workers"] == 8


@pytest.mark.parametrize(
    "argv",
    [
        ["--workers", "0"],
        ["--session-ttl-s", "0"],
        ["--heartbeat-s", "30"],
        ["--heartbeat-s", "0"],
        ["--node-give-up-s", "-1"],
        ["--op-window", "0"],
        ["--op-bytes-max", "0"],
        ["--cpu-only"],  # a maru-server option
    ],
)
def test_bad_values_are_refused(argv):
    with pytest.raises(SystemExit) as info:
        run_main(*argv)
    assert info.value.code == 2


def test_each_run_has_its_own_epoch_and_times():
    server = NodeServer(session_ttl_s=9, node_give_up_s=50, op_window=7)
    other = NodeServer()
    ms = server.ms_service
    assert ms.server_epoch == server.server_epoch != other.server_epoch
    assert (ms.session_ttl_s, ms.heartbeat_s, ms.node_give_up_s) == (9, 3, 50)
    assert ms.ledger.op_window == 7
    ms.ensure_started()
    thread = ms._thread
    server.close()
    other.close()
    assert not thread.is_alive()


def test_policy_file_is_checked_at_start(tmp_path):
    good = tmp_path / "policy.json"
    good.write_text('{"preserve": [{"namespace": "ns", "key": "k"}]}')
    server_cls, _ = run_main("--policy-file", str(good), "--op-bytes-max", "4096")
    kwargs = server_cls.call_args.kwargs
    assert kwargs["policy_file"] == str(good) and kwargs["op_bytes_max"] == 4096
    bad = tmp_path / "bad.json"
    bad.write_text('{"preserve": 1}')
    for path in (bad, tmp_path / "absent.json"):
        with pytest.raises(SystemExit) as info:
            run_main("--policy-file", str(path))
        assert info.value.code == 2
