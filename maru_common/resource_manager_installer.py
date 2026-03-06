# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Console script entry point for installing the Maru Resource Manager.

Wraps the cmake build + install workflow so that after `pip install -e .`,
a single `sudo $(which install-maru-resource-manager)` builds and installs
the Maru Resource Manager with systemd integration.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

_SERVICE_NAME = "maru-resourced"
_SERVICE_FILE = Path("/etc/systemd/system/maru-resourced.service")
_UDEV_RULES_FILE = Path("/etc/udev/rules.d/99-maru-resourced.rules")


def _find_resource_manager_source() -> Path:
    """Locate the maru_resource_manager source directory relative to this file.

    Walks up from this file's directory until it finds a directory containing
    ``maru_resource_manager/CMakeLists.txt``.
    """
    start = Path(__file__).resolve().parent
    for ancestor in (start, *start.parents):
        candidate = ancestor / "maru_resource_manager" / "CMakeLists.txt"
        if candidate.is_file():
            return ancestor / "maru_resource_manager"
    return Path()  # will fail the exists() check in caller


def _run(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    """Run a subprocess, forwarding output to the terminal."""
    fprintf(sys.stderr, "+ %s\n", " ".join(cmd))
    return subprocess.run(cmd, check=check)


def fprintf(stream, fmt: str, *args: object) -> None:  # noqa: N802
    """C-style fprintf helper."""
    stream.write(fmt % args if args else fmt)
    stream.flush()


def _check_root() -> int | None:
    """Return 1 if not root, None if OK."""
    if os.getuid() != 0:
        fprintf(
            sys.stderr,
            "Error: root privileges required.\n"
            "Hint: sudo $(which install-maru-resource-manager)\n",
        )
        return 1
    return None


def _do_uninstall(prefix: str) -> int:
    """Stop, disable, and remove the resource manager and its systemd/udev files."""
    binary = Path(prefix) / "bin" / "maru_resourced"

    # Stop and disable systemd service
    _run(["systemctl", "stop", _SERVICE_NAME], check=False)
    _run(["systemctl", "disable", _SERVICE_NAME], check=False)

    # Remove files
    removed = []
    for path in (_SERVICE_FILE, _UDEV_RULES_FILE, binary):
        if path.exists():
            path.unlink()
            removed.append(str(path))

    if removed:
        _run(["systemctl", "daemon-reload"], check=False)
        if str(_UDEV_RULES_FILE) in removed:
            _run(["udevadm", "control", "--reload-rules"], check=False)
            _run(["udevadm", "trigger"], check=False)
        for p in removed:
            fprintf(sys.stderr, "  Removed: %s\n", p)
    else:
        fprintf(sys.stderr, "Nothing to remove.\n")

    fprintf(sys.stderr, "Uninstall complete.\n")
    return 0


def _do_install(args: argparse.Namespace) -> int:
    """Build and install the resource manager via cmake."""
    if shutil.which("cmake") is None:
        fprintf(sys.stderr, "Error: cmake not found in PATH.\n")
        return 1

    rm_src = _find_resource_manager_source()
    if not rm_src.is_dir():
        fprintf(
            sys.stderr,
            "Error: maru_resource_manager source directory not found.\n"
            "This command requires an editable install: pip install -e .\n",
        )
        return 1

    build_dir = rm_src / "build"

    # -- clean ----------------------------------------------------------------
    if args.clean and build_dir.exists():
        fprintf(sys.stderr, "Removing build directory: %s\n", build_dir)
        shutil.rmtree(build_dir)

    build_dir.mkdir(exist_ok=True)

    # -- handle stale CMake cache ---------------------------------------------
    cmake_cache = build_dir / "CMakeCache.txt"
    if cmake_cache.is_file():
        probe = subprocess.run(
            ["cmake", "-S", str(rm_src), "-B", str(build_dir), "-N"],
            capture_output=True,
        )
        if probe.returncode != 0:
            fprintf(
                sys.stderr,
                "CMake cache mismatch detected. Removing %s\n",
                cmake_cache,
            )
            cmake_cache.unlink()

    # -- cmake configure ------------------------------------------------------
    configure_cmd = [
        "cmake",
        "-S",
        str(rm_src),
        "-B",
        str(build_dir),
        "-DCMAKE_BUILD_TYPE=Release",
    ]
    if args.no_systemd:
        configure_cmd.append("-DMARU_INSTALL_SYSTEMD=OFF")
    _run(configure_cmd)

    # -- cmake build ----------------------------------------------------------
    _run(["cmake", "--build", str(build_dir)])

    # -- verify build output --------------------------------------------------
    binary = build_dir / "maru_resourced"
    if not binary.is_file() or binary.stat().st_size == 0:
        fprintf(sys.stderr, "Error: build output not found or empty: %s\n", binary)
        return 1

    fprintf(sys.stderr, "Build completed: %s\n", binary)

    # -- cmake install --------------------------------------------------------
    _run(["cmake", "--install", str(build_dir), "--prefix", args.prefix])

    fprintf(sys.stderr, "Installation complete!\n")
    fprintf(sys.stderr, "  Binary: %s/bin/maru_resourced\n", args.prefix)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="install-maru-resource-manager",
        description="Build and install the Maru Resource Manager.",
        epilog=(
            "This command requires root privileges.\n"
            "In a venv, use: sudo $(which install-maru-resource-manager)"
        ),
    )
    parser.add_argument(
        "--prefix",
        default="/usr/local",
        help="installation prefix (default: /usr/local)",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="remove build directory before building",
    )
    parser.add_argument(
        "--no-systemd",
        action="store_true",
        help="skip systemd service and udev rules installation",
    )
    parser.add_argument(
        "--uninstall",
        action="store_true",
        help="stop, disable, and remove the installed resource manager",
    )
    args = parser.parse_args(argv)

    if (rc := _check_root()) is not None:
        return rc

    if args.uninstall:
        return _do_uninstall(args.prefix)

    return _do_install(args)


if __name__ == "__main__":
    raise SystemExit(main())
