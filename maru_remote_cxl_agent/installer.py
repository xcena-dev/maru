# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Install the remote CXL agent as a system service started with the RM.

The remote CXL agent is a platform service like the resource manager: one per
memory node, shared by every app. ``install-maru-resource-manager`` calls
:func:`install` after it installs the resource manager, so a memory node runs
both with ``systemctl start maru-resource-manager``. The service is wanted by
the resource manager's unit and stops and restarts with it; the resource
manager's own unit is left unchanged. A memory node of a shared pool that runs
no resource manager installs it standalone (``--standalone``), so it starts at
boot on its own.

Settings come from an environment file (:data:`ENV_FILE`) that is written
once and kept across reinstalls. The remote CXL agent key (:data:`KEY_FILE`) is
created once as well; workers and MaruServer need a copy of it.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

from maru_remote.region_map import DEFAULT_KEY_FILE, write_key

SERVICE_NAME = "maru-remote-cxl-agent.service"
RM_SERVICE_NAME = "maru-resource-manager.service"
SYSTEMD_DIR = Path("/etc/systemd/system")
ENV_FILE = Path("/etc/maru/remote-cxl-agent.env")
KEY_FILE = Path(DEFAULT_KEY_FILE)

DEFAULT_ENV = """\
# Settings of the Maru remote CXL agent service (maru-remote-cxl-agent.service).
# The service starts and stops with maru-resource-manager.service.
# Restart after a change: sudo systemctl restart maru-remote-cxl-agent

# Resource manager of the pool, as this node reaches it.
MARU_RM_ADDRESS=127.0.0.1:9850

# RDMA NIC to register regions with, e.g. mlx5_1:1. Empty: the UCX default.
MARU_REMOTE_CXL_AGENT_UCX_DEVICE=

# Address and port the workers of other nodes connect to. Give maru-server
# this address with this port (--remote-cxl-agent). Empty: the address of the
# NIC above on the RDMA network (the NIC must then be set).
MARU_REMOTE_CXL_AGENT_HOST=
MARU_REMOTE_CXL_AGENT_PORT=9860

# Key that workers and maru-server prove on every connection. Copy this file
# to them, readable only by the users that run them.
MARU_REMOTE_CXL_AGENT_KEY_FILE=/etc/maru/remote-cxl-agent.key
"""

STANDALONE_ENV = """\
# Settings of the Maru remote CXL agent service (maru-remote-cxl-agent.service).
# This node runs no resource manager, so the service starts at boot on its own.
# Restart after a change: sudo systemctl restart maru-remote-cxl-agent

# Resource manager of the pool, on another memory node (required), e.g.
# 10.0.0.2:9850. It must listen on a network address.
MARU_RM_ADDRESS=

# RDMA NIC to register regions with, e.g. mlx5_1:1. Empty: the UCX default.
MARU_REMOTE_CXL_AGENT_UCX_DEVICE=

# Address and port the workers of other nodes connect to. Give maru-server
# this address with this port (--remote-cxl-agent). Empty: the address of the
# NIC above on the RDMA network (the NIC must then be set).
MARU_REMOTE_CXL_AGENT_HOST=
MARU_REMOTE_CXL_AGENT_PORT=9860

# Key that workers and maru-server prove on every connection. Copy this file
# to them, readable only by the users that run them.
MARU_REMOTE_CXL_AGENT_KEY_FILE=/etc/maru/remote-cxl-agent.key
"""


def render_unit(python: str, standalone: bool = False) -> str:
    """The systemd unit of the remote CXL agent.

    Args:
        python: Interpreter that has Maru and NIXL installed.
        standalone: For a memory node that runs no resource manager: start at
            boot on its own instead of with the resource manager.

    Returns:
        The unit file text.
    """
    if standalone:
        order = "After=network-online.target\nWants=network-online.target\n"
        install = (
            "# Started at boot: this node runs no resource manager.\n"
            "WantedBy=multi-user.target"
        )
    else:
        order = (
            f"After=network-online.target {RM_SERVICE_NAME}\n"
            "Wants=network-online.target\n"
            "# Stops and restarts with the resource manager.\n"
            f"PartOf={RM_SERVICE_NAME}\n"
        )
        install = (
            "# Started whenever the resource manager starts.\n"
            f"WantedBy={RM_SERVICE_NAME}"
        )
    return f"""\
[Unit]
Description=Maru remote CXL agent - maps CXL regions and registers them with the NIC for remote workers
{order}# Stops retrying after 5 failed starts in a minute (e.g. no RDMA NIC).
StartLimitIntervalSec=60
StartLimitBurst=5

[Service]
Type=simple
EnvironmentFile=-{ENV_FILE}
ExecStart={python} -m maru_remote_cxl_agent.cli
Restart=on-failure
RestartSec=3
# Registering a region with the NIC pins its memory.
LimitMEMLOCK=infinity
# Holds /run/maru, where the node lock lives.
RuntimeDirectory=maru
NoNewPrivileges=yes
ProtectSystem=full
ProtectHome=read-only
PrivateTmp=yes
ProtectControlGroups=yes
RestrictSUIDSGID=yes

[Install]
{install}
"""


def nixl_available() -> bool:
    """Whether this interpreter can import NIXL, which the service needs."""
    return importlib.util.find_spec("nixl") is not None


def _systemctl(*args: str) -> None:
    subprocess.run(["systemctl", *args], capture_output=True)


def install(
    python: str = sys.executable,
    systemd_dir: Path = SYSTEMD_DIR,
    env_file: Path = ENV_FILE,
    systemctl=_systemctl,
    standalone: bool = False,
    key_file: Path = KEY_FILE,
) -> bool:
    """Install the unit and, if missing, the environment file and the key;
    enable it.

    Args:
        python: Interpreter the service runs.
        systemd_dir: Directory of system units.
        env_file: Environment file of the service; an existing one is kept.
        systemctl: Runs a systemctl command.
        standalone: Start at boot on its own (a node without a resource
            manager) instead of with the resource manager.
        key_file: Remote CXL agent key; an existing one is kept.

    Returns:
        True if installed; False if NIXL is not installed or there is no
        systemd, so a remote CXL agent could not run here.
    """
    if not nixl_available():
        print(
            "Skipping the remote CXL agent service: NIXL is not installed "
            "(uv pip install nixl, or pip install nixl, to serve remote workers).",
            file=sys.stderr,
        )
        return False
    if not systemd_dir.is_dir():
        print("Skipping the remote CXL agent service: no systemd.", file=sys.stderr)
        return False
    unit = systemd_dir / SERVICE_NAME
    unit.write_text(render_unit(python, standalone))
    print(f"  Installed systemd service: {unit}", file=sys.stderr)
    if not env_file.exists():
        env_file.parent.mkdir(parents=True, exist_ok=True)
        env_file.write_text(STANDALONE_ENV if standalone else DEFAULT_ENV)
        print(f"  Wrote settings: {env_file}", file=sys.stderr)
        print(
            f"  Set MARU_REMOTE_CXL_AGENT_UCX_DEVICE in {env_file} to this node's "
            "RDMA NIC (e.g. mlx5_1:1): the service listens on that NIC's address "
            "and does not start without it.",
            file=sys.stderr,
        )
    else:
        print(f"  Kept settings: {env_file}", file=sys.stderr)
    if not key_file.exists():
        key_file.parent.mkdir(parents=True, exist_ok=True)
        write_key(str(key_file))
        print(f"  Wrote the remote CXL agent key: {key_file}", file=sys.stderr)
    else:
        print(f"  Kept the remote CXL agent key: {key_file}", file=sys.stderr)
    print(
        f"  Copy {key_file} to the worker nodes and the maru-server node of this "
        "pool, readable only by the users that run them; a shared pool's memory "
        "nodes use the same key.",
        file=sys.stderr,
    )
    systemctl("daemon-reload")
    # Drop the links of an earlier install in the other form first.
    systemctl("disable", SERVICE_NAME)
    systemctl("enable", SERVICE_NAME)
    if standalone:
        print(
            f"  Set MARU_RM_ADDRESS in {env_file} to the pool's resource "
            "manager, then:\n    sudo systemctl start maru-remote-cxl-agent",
            file=sys.stderr,
        )
    else:
        print(
            "  The remote CXL agent starts with the resource manager:\n"
            "    sudo systemctl start maru-resource-manager",
            file=sys.stderr,
        )
    return True


def uninstall(systemd_dir: Path = SYSTEMD_DIR, systemctl=_systemctl) -> bool:
    """Stop, disable and remove the unit; the environment file stays.

    Returns:
        True if a unit was removed.
    """
    systemctl("stop", SERVICE_NAME)
    systemctl("disable", SERVICE_NAME)
    unit = systemd_dir / SERVICE_NAME
    if not unit.exists():
        return False
    unit.unlink()
    systemctl("daemon-reload")
    print(f"  Removed: {unit}", file=sys.stderr)
    return True


def main(argv: list[str] | None = None) -> int:
    """``install-maru-remote-cxl-agent``: install or remove the service only."""
    parser = argparse.ArgumentParser(
        prog="install-maru-remote-cxl-agent",
        description="Install the Maru remote CXL agent as a system service that "
        "starts with maru-resource-manager.",
        epilog="Requires root. In a venv: sudo $(which install-maru-remote-cxl-agent)",
    )
    parser.add_argument("--uninstall", action="store_true", help="remove the service")
    parser.add_argument(
        "--standalone",
        action="store_true",
        help="for a memory node that runs no resource manager: start the "
        "service at boot on its own",
    )
    args = parser.parse_args(argv)
    if os.getuid() != 0:
        print(
            "Error: root privileges required.\n"
            "Hint: sudo $(which install-maru-remote-cxl-agent)",
            file=sys.stderr,
        )
        return 1
    if args.uninstall:
        uninstall()
        return 0
    return 0 if install(standalone=args.standalone) else 1


if __name__ == "__main__":
    raise SystemExit(main())
