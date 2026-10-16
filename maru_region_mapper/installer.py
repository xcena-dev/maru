# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Install the region mapper as a system service started with the RM.

The region mapper is a platform service like the resource manager: one per
memory node, shared by every app. ``install-maru-resource-manager`` calls
:func:`install` after it installs the resource manager, so a memory node runs
both with ``systemctl start maru-resource-manager``. The service is wanted by
the resource manager's unit and stops and restarts with it; the resource
manager's own unit is left unchanged. A memory node of a shared pool that runs
no resource manager installs it standalone (``--standalone``), so it starts at
boot on its own.

Settings come from an environment file (:data:`ENV_FILE`) that is written
once and kept across reinstalls.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

SERVICE_NAME = "maru-region-mapper.service"
RM_SERVICE_NAME = "maru-resource-manager.service"
SYSTEMD_DIR = Path("/etc/systemd/system")
ENV_FILE = Path("/etc/maru/region-mapper.env")

DEFAULT_ENV = """\
# Settings of the Maru region mapper service (maru-region-mapper.service).
# The service starts and stops with maru-resource-manager.service.
# Restart after a change: sudo systemctl restart maru-region-mapper

# Resource manager of the pool, as this node reaches it.
MARU_RM_ADDRESS=127.0.0.1:9850

# Address and port the workers of other nodes connect to. Give maru-server
# this node's address on the RDMA network with this port (--region-mapper).
MARU_REGION_MAPPER_HOST=0.0.0.0
MARU_REGION_MAPPER_PORT=9860

# RDMA NIC to register regions with, e.g. mlx5_1:1. Empty: the UCX default.
MARU_REGION_MAPPER_UCX_DEVICE=
"""


def render_unit(python: str, standalone: bool = False) -> str:
    """The systemd unit of the region mapper.

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
Description=Maru region mapper - maps CXL regions and registers them with the NIC for remote workers
{order}
[Service]
Type=simple
EnvironmentFile=-{ENV_FILE}
ExecStart={python} -m maru_region_mapper.cli
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
) -> bool:
    """Install the unit and, if missing, the environment file; enable it.

    Args:
        python: Interpreter the service runs.
        systemd_dir: Directory of system units.
        env_file: Environment file of the service; an existing one is kept.
        systemctl: Runs a systemctl command.
        standalone: Start at boot on its own (a node without a resource
            manager) instead of with the resource manager.

    Returns:
        True if installed; False if NIXL is not installed or there is no
        systemd, so a region mapper could not run here.
    """
    if not nixl_available():
        print(
            "Skipping the region mapper service: NIXL is not installed "
            "(pip install 'maru[rdma]' to serve remote workers).",
            file=sys.stderr,
        )
        return False
    if not systemd_dir.is_dir():
        print("Skipping the region mapper service: no systemd.", file=sys.stderr)
        return False
    unit = systemd_dir / SERVICE_NAME
    unit.write_text(render_unit(python, standalone))
    print(f"  Installed systemd service: {unit}", file=sys.stderr)
    if not env_file.exists():
        env_file.parent.mkdir(parents=True, exist_ok=True)
        env_file.write_text(DEFAULT_ENV)
        print(f"  Wrote settings: {env_file}", file=sys.stderr)
    else:
        print(f"  Kept settings: {env_file}", file=sys.stderr)
    systemctl("daemon-reload")
    systemctl("enable", SERVICE_NAME)
    if standalone:
        print(
            f"  Set MARU_RM_ADDRESS in {env_file} to the pool's resource "
            "manager, then:\n    sudo systemctl start maru-region-mapper",
            file=sys.stderr,
        )
    else:
        print(
            "  The region mapper starts with the resource manager:\n"
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
    """``install-maru-region-mapper``: install or remove the service only."""
    parser = argparse.ArgumentParser(
        prog="install-maru-region-mapper",
        description="Install the Maru region mapper as a system service that "
        "starts with maru-resource-manager.",
        epilog="Requires root. In a venv: sudo $(which install-maru-region-mapper)",
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
            "Hint: sudo $(which install-maru-region-mapper)",
            file=sys.stderr,
        )
        return 1
    if args.uninstall:
        uninstall()
        return 0
    return 0 if install(standalone=args.standalone) else 1


if __name__ == "__main__":
    raise SystemExit(main())
