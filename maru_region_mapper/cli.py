# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""``maru-region-mapper``: the region mapper of a memory node.

Run one per memory node (e.g. as a systemd service next to the resource
manager). Handlers on worker nodes connect to it to have regions mapped and
registered with this node's NIC.

A released region must refuse late RDMA, which holds only where the NIC
checks the remote key of every request and a deregistration really reaches
the NIC. UCX is therefore limited to its InfiniBand/RoCE transports (over TCP
or shared memory a write to a deregistered range still lands), and its
registration cache is turned off (with it, a NIXL deregistration leaves the
NIC registration in place and late writes still land).
"""

import argparse
import fcntl
import logging
import os
import signal
import socket
import threading
import uuid
from collections.abc import MutableMapping

from maru_common.logging_setup import setup_package_logging

from .mapper import DaxRegionMapper
from .server import RegionMapServer
from .service import RegionMapService

# Named, not __name__: under `python -m` the module is __main__, outside the
# package logger that main() configures.
logger = logging.getLogger("maru_region_mapper.cli")


# UCX transports where the NIC checks the remote key of each RDMA, so a
# deregistered range refuses late writes ("self" never leaves the process).
CHECKED_UCX_TRANSPORTS = frozenset(
    {
        "rc", "rc_v", "rc_x", "rc_verbs", "rc_mlx5",
        "dc", "dc_x", "dc_mlx5",
        "self",
    }
)  # fmt: skip
# UD has no one-sided RDMA, so UCX would carry RDMA over it in software. It
# passes only as an auxiliary transport (":aux"), which UCX uses to set up rc
# connections; "ib" is left out because it enables UD for RDMA too.
AUX_ONLY_UCX_TRANSPORTS = frozenset({"ud", "ud_v", "ud_x", "ud_verbs", "ud_mlx5"})
DEFAULT_UCX_TRANSPORTS = "rc,dc"
# UCX's registration cache keeps a registration alive after NIXL releases it.
# Only spellings known to turn it off pass; anything else must be set to n.
RCACHE_OFF = ("n", "no", "0")


class NodeLockedError(RuntimeError):
    """Another region mapper of this node holds the node lock."""


class UncheckedTransportError(RuntimeError):
    """UCX may use a transport that does not check remote keys."""


def require_checked_transport(
    environ: MutableMapping[str, str], allow_unchecked: bool = False
) -> str:
    """Limit UCX to transports whose NIC checks remote keys, without a
    registration cache.

    Sets ``UCX_TLS`` to :data:`DEFAULT_UCX_TRANSPORTS` and
    ``UCX_RCACHE_ENABLE`` to ``n`` when they are unset, so it must run before
    the NIXL agent is created.

    Args:
        environ: Process environment (``os.environ``).
        allow_unchecked: Accept any ``UCX_TLS`` (tests on one host).

    Returns:
        The ``UCX_TLS`` value in effect.

    Raises:
        UncheckedTransportError: if ``UCX_TLS`` names another transport or
            ``UCX_RCACHE_ENABLE`` turns the registration cache on.
    """
    rcache = environ.get("UCX_RCACHE_ENABLE", "").strip().lower()
    if not rcache:
        environ["UCX_RCACHE_ENABLE"] = "n"
    elif rcache not in RCACHE_OFF and not allow_unchecked:
        raise UncheckedTransportError(
            f"UCX_RCACHE_ENABLE={rcache!r} keeps a released region registered "
            "with the NIC, so a late write still lands; unset it or set it to n"
        )
    value = environ.get("UCX_TLS", "").strip()
    if not value:
        environ["UCX_TLS"] = DEFAULT_UCX_TRANSPORTS
        return DEFAULT_UCX_TRANSPORTS
    items = [item.strip() for item in value.split(",") if item.strip()]
    unchecked = [item for item in items if not _checked_transport(item)]
    if unchecked and not allow_unchecked:
        raise UncheckedTransportError(
            f"UCX_TLS={value!r} allows {', '.join(unchecked)}, where a released "
            "region can still be written; use InfiniBand/RoCE transports "
            f"(e.g. {DEFAULT_UCX_TRANSPORTS}) or --allow-unchecked-transport"
        )
    return value


def _checked_transport(item: str) -> bool:
    """Whether one ``UCX_TLS`` entry keeps RDMA on key-checking transports."""
    name, _, flag = item.partition(":")
    if name in CHECKED_UCX_TRANSPORTS:
        return True
    return name in AUX_ONLY_UCX_TRANSPORTS and flag == "aux"


def default_lock_file(node_id: str) -> str:
    """Return the default per-node lock file path."""
    return f"/run/maru/region-mapper-{node_id.replace(os.sep, '_')}.lock"


def hold_node_lock(path: str) -> int:
    """Take the node's region mapper lock for the life of the process.

    Two region mappers on one node would register the same region twice and
    hand out different keys for it.

    Args:
        path: Lock file; created (with its directory) if missing.

    Returns:
        The locked descriptor; keep it open until the process exits.

    Raises:
        NodeLockedError: if another process holds the lock.
        OSError: if the lock file cannot be opened.
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        raise NodeLockedError(
            f"{path} is held by another maru-region-mapper of this node"
        ) from None
    except OSError:
        os.close(fd)
        raise
    return fd


def read_boot_id(path: str = "/proc/sys/kernel/random/boot_id") -> str:
    """Return this boot of the node (empty if the kernel does not say)."""
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return ""


def build_parser() -> argparse.ArgumentParser:
    """Command-line options of ``maru-region-mapper``."""
    parser = argparse.ArgumentParser(
        prog="maru-region-mapper",
        description="Maru region mapper - maps resource-manager regions and "
        "registers them with this node's NIC for remote Handlers",
    )
    # Each default can also come from the environment, so the system service
    # takes its settings from an environment file (see installer.py).
    env = os.environ
    parser.add_argument(
        "--rm-address",
        default=env.get("MARU_RM_ADDRESS", "127.0.0.1:9850"),
        help="Resource manager address (host:port; env MARU_RM_ADDRESS; "
        "default: 127.0.0.1:9850)",
    )
    parser.add_argument(
        "--host",
        default=env.get("MARU_REGION_MAPPER_HOST", "127.0.0.1"),
        help="Bind address (env MARU_REGION_MAPPER_HOST; default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(env.get("MARU_REGION_MAPPER_PORT", "9860")),
        help="Bind port (env MARU_REGION_MAPPER_PORT; default: 9860)",
    )
    parser.add_argument(
        "--node-id", default=socket.gethostname(), help="Node name (default: hostname)"
    )
    parser.add_argument(
        "--ucx-device",
        default=env.get("MARU_REGION_MAPPER_UCX_DEVICE", ""),
        help="UCX device, e.g. mlx5_0:1 (env MARU_REGION_MAPPER_UCX_DEVICE; "
        "default: the UCX default)",
    )
    parser.add_argument(
        "--lock-file",
        default=env.get("MARU_REGION_MAPPER_LOCK_FILE") or None,
        help="Lock held for the life of the process, so one region mapper runs "
        "per node (env MARU_REGION_MAPPER_LOCK_FILE; default: "
        "/run/maru/region-mapper-<node-id>.lock)",
    )
    parser.add_argument(
        "--allow-unchecked-transport",
        action="store_true",
        help="Accept any UCX_TLS and UCX_RCACHE_ENABLE, e.g. tcp for a test on "
        "one host. A late RDMA WRITE can then land in a region after its owner "
        "released it",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the region mapper until SIGINT or SIGTERM.

    Args:
        argv: Command-line arguments (default: ``sys.argv[1:]``).

    Returns:
        0 after a signal; 1 if another region mapper holds this node or UCX
        may use a transport that does not check remote keys.
    """
    args = build_parser().parse_args(argv)
    level = getattr(logging, args.log_level)
    for package in ("maru_region_mapper", "maru_remote", "maru_shm"):
        package_logger = setup_package_logging(package)
        package_logger.setLevel(level)
        for handler in package_logger.handlers:
            handler.setLevel(level)

    try:
        transports = require_checked_transport(
            os.environ, args.allow_unchecked_transport
        )
    except UncheckedTransportError as exc:
        logger.error("%s", exc)
        return 1

    lock_file = args.lock_file or default_lock_file(args.node_id)
    try:
        lock_fd = hold_node_lock(lock_file)
    except (NodeLockedError, OSError) as exc:
        logger.error("cannot take the node lock %s: %s", lock_file, exc)
        return 1

    # Imported here: NIXL and the RM client are only needed to run the daemon.
    from maru_remote.transport import NixlTransport
    from maru_shm import MaruShmClient

    epoch = uuid.uuid4().hex
    transport = NixlTransport(
        f"maru-region-mapper-{args.node_id}-{epoch[:8]}", ucx_device=args.ucx_device
    )
    rm = MaruShmClient(address=args.rm_address)
    service = RegionMapService(
        rm,
        DaxRegionMapper(),
        transport,
        node_id=args.node_id,
        epoch=epoch,
        boot_id=read_boot_id(),
    )
    server = RegionMapServer(service, args.host, args.port)

    def stop(signum, frame):
        threading.Thread(target=server.stop, daemon=True).start()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    if args.host == "0.0.0.0":
        logger.warning(
            "Binding to 0.0.0.0: any host that reaches this port and holds a "
            "region handle can map that region"
        )
    logger.info(
        "region mapper for node %s (epoch %s) on %s:%d, resource manager %s, "
        "UCX_TLS=%s, UCX_RCACHE_ENABLE=%s",
        args.node_id,
        epoch,
        *server.address,
        args.rm_address,
        transports,
        os.environ.get("UCX_RCACHE_ENABLE"),
    )
    try:
        server.serve_forever()
    finally:
        service.close()
        transport.close()
        rm.close()
        os.close(lock_fd)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
