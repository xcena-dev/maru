# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tests for the GET_USAGE client parser and the usage_monitor tool."""

import importlib.util
from pathlib import Path

import pytest

from maru_common import GetUsageResponse, InstanceUsage, MessageType
from maru_handler.rpc_client_base import RpcClientBase


class _FakeClient(RpcClientBase):
    """RpcClientBase with a canned _send_request, for parser testing."""

    def __init__(self, response: dict):
        self._response = response
        self.last_msg_type = None

    def _send_request(self, msg_type, data):
        self.last_msg_type = msg_type
        return self._response


class TestGetUsageClientParse:
    """RpcClientBase.get_usage() wire-dict -> dataclass parsing."""

    def test_parses_instances_and_pool(self):
        client = _FakeClient(
            {
                "instances": [
                    {"instance_id": "vllm-0", "regions": 3, "allocated": 12, "used": 9},
                    {"instance_id": "vllm-1", "regions": 2, "allocated": 8, "used": 7},
                ],
                "pool_total": 100,
                "pool_free": 60,
            }
        )
        resp = client.get_usage()

        assert client.last_msg_type == MessageType.GET_USAGE
        assert isinstance(resp, GetUsageResponse)
        assert resp.pool_total == 100
        assert resp.pool_free == 60
        assert len(resp.instances) == 2
        first = resp.instances[0]
        assert first.instance_id == "vllm-0"
        assert first.regions == 3
        assert first.allocated == 12
        assert first.used == 9

    def test_parses_empty_response(self):
        resp = _FakeClient({}).get_usage()
        assert resp.instances == []
        assert resp.pool_total == 0
        assert resp.pool_free == 0

    def test_raises_on_error_response(self):
        """An error dict (timeout / transport / old server) must raise, not
        silently render as an empty/healthy server."""
        client = _FakeClient({"error": "timeout", "success": False})
        with pytest.raises(ConnectionError):
            client.get_usage()


# tools/ is not an installed package — load the tool module by file path.
_TOOL_PATH = Path(__file__).resolve().parents[2] / "tools" / "usage_monitor.py"
_spec = importlib.util.spec_from_file_location("usage_monitor", _TOOL_PATH)
usage_monitor = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(usage_monitor)


def _sample_usage() -> GetUsageResponse:
    return GetUsageResponse(
        instances=[
            InstanceUsage(
                instance_id="vllm-0",
                regions=3,
                allocated=12 * 1024**3,
                used=9 * 1024**3,
            ),
            InstanceUsage(
                instance_id="vllm-1",
                regions=2,
                allocated=8 * 1024**3,
                used=7 * 1024**3,
            ),
        ],
        pool_total=242 * 1024**3,
        pool_free=229 * 1024**3,
    )


class TestUsageMonitorRendering:
    """usage_monitor.py formatting helpers."""

    def test_fmt_size(self):
        assert usage_monitor._fmt_size(0) == "0B"
        assert usage_monitor._fmt_size(1024) == "1.0K"
        assert usage_monitor._fmt_size(1024**3) == "1.0G"
        assert usage_monitor._fmt_size(1024**4) == "1.0T"

    def test_fmt_size_negative(self):
        # Negative slack (broken upstream invariant) renders with a sign and
        # correct magnitude unit, not a bogus fractional-K value.
        assert usage_monitor._fmt_size(-(1024**3)) == "-1.0G"
        assert usage_monitor._fmt_size(-1024) == "-1.0K"

    def test_render_table_contains_rows(self):
        out = usage_monitor.render_table(_sample_usage(), "2026-06-15T00:00:00")
        assert "vllm-0" in out
        assert "vllm-1" in out
        assert "slack" in out
        assert "TOTAL" in out
        assert "Pool (shared)" in out

    def test_render_table_empty(self):
        out = usage_monitor.render_table(GetUsageResponse(), "2026-06-15T00:00:00")
        assert "no active instances" in out

    def test_csv_rows(self, capsys):
        usage_monitor.print_csv_header()
        usage_monitor.print_csv_rows(_sample_usage(), "2026-06-15T00:00:00")
        lines = capsys.readouterr().out.strip().splitlines()

        assert lines[0].startswith("timestamp,instance_id,regions")
        row0 = lines[1].split(",")
        assert row0[1] == "vllm-0"
        # slack column == allocated - used
        assert int(row0[5]) == int(row0[3]) - int(row0[4])
