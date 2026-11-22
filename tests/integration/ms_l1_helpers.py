# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Single-store integration configs and project command entry points."""

import math
import os
import sys
import tomllib
from pathlib import Path

import pytest

from maru_shm import MaruShmClient
from maru_storage_service.cli import config_to_dict
from maru_storage_service.config import parse_node_config
from tests.unit.ms_node_helpers import WORKTREE, node_config


def l1_config(server, socket, *, store="dram", **kwargs):
    kwargs.setdefault("node_id", f"cpa-{Path(socket).parent.name}-{Path(socket).stem}")
    config = node_config(server, socket, **kwargs)
    if store == "dram":
        return config
    assert store == "local_cxl"
    address = os.environ.get("MARU_TEST_RM_ADDRESS", "")
    dax = os.environ.get("MARU_TEST_DAX_PATH", "")
    if not address or dax != "/dev/dax2.0":
        pytest.skip("set MARU_TEST_RM_ADDRESS and MARU_TEST_DAX_PATH=/dev/dax2.0")
    try:
        host, port = address.rsplit(":", 1)
        port = int(port)
        assert host and 0 < port < 65536 and port not in (9850, 9860)
    except (ValueError, AssertionError):
        pytest.skip("needs a test RM address outside ports 9850 and 9860")
    if not os.access(dax, os.R_OK | os.W_OK):
        pytest.skip("needs writable /dev/dax2.0")
    rm = MaruShmClient(address=address)
    try:
        pools = rm.stats()
    except Exception as exc:
        pytest.skip(f"test RM unavailable: {exc}")
    finally:
        rm.close()
    # Node allocation lets the RM choose a pool.
    if not pools or any(p.dax_path != dax for p in pools):
        pytest.skip("test RM must serve only /dev/dax2.0")
    region = math.lcm(config.page_size, *(p.align_bytes for p in pools))
    size = math.ceil(config.top.size / region) * region
    if not any(p.free_size >= size for p in pools):
        pytest.skip("test RM has insufficient free DAX capacity")
    raw = config_to_dict(config)
    raw["tiers"] = [
        {"store": store, "size": size, "region_size": region, "rm": address}
    ]
    if config.role == "memory":
        raw["tiers"][0].update(
            high_watermark=config.top.high_watermark,
            low_watermark=config.top.low_watermark,
        )
    return parse_node_config(raw, role=config.role)


def command_argv(command, *args):
    """Resolve the worktree's console script without an editable install."""
    with (Path(WORKTREE) / "pyproject.toml").open("rb") as stream:
        entry = tomllib.load(stream)["project"]["scripts"][command]
    module, function = entry.split(":")
    code = (
        "import sys; from importlib import import_module; "
        f"sys.exit(getattr(import_module({module!r}), {function!r})(sys.argv[1:]))"
    )
    return [sys.executable, "-c", code, *args]


STORES = ("dram", "local_cxl")
