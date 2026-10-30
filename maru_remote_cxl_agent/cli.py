# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""``maru-remote-cxl-agent``: the remote CXL agent of a memory node.

Run one per memory node (e.g. as a systemd service next to the resource
manager). Handlers on worker nodes connect to it to have regions mapped and
registered with this node's NIC.

A released region must refuse late RDMA, which holds only where the NIC
checks the remote key of every request and a deregistration really reaches
the NIC. UCX is therefore limited to its InfiniBand/RoCE transports (over TCP
or shared memory a write to a deregistered range still lands), and its
registration cache is turned off (with it, a NIXL deregistration leaves the
NIC registration in place and late writes still land).

Who may map a region is bound as on a CXL-attached node. There, a process
needs the CXL device and permission to open its DAX file. Here, it needs to
reach the remote CXL agent, which listens on this node's RDMA network address
by default, and to read the remote CXL agent key, which every connection proves.
"""

import argparse
import fcntl
import logging
import os
import resource
import signal
import socket
import struct
import threading
import uuid
from collections.abc import Callable, MutableMapping
from pathlib import Path

from maru_common.logging_setup import setup_package_logging
from maru_remote.region_map import DEFAULT_KEY_FILE, load_key

from .mapper import DaxRegionMapper
from .server import RegionMapServer
from .service import (
    GENERATION_GRACE_S,
    GENERATION_POLL_S,
    MAX_HINT_REQUESTS,
    RegionMapService,
)

# Named, not __name__: under `python -m` the module is __main__, outside the
# package logger that main() configures.
logger = logging.getLogger("maru_remote_cxl_agent.cli")


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
# NIXL 1.5 requires CUDA memory support on hosts with NVIDIA GPUs, even when
# Maru registers only DRAM. cuda_copy is a local memory-type helper, not a
# replacement for a remote key-checking RC/DC data transport.
DEFAULT_UCX_TRANSPORTS = "rc,dc,cuda_copy"
# UCX's registration cache keeps a registration alive after NIXL releases it.
# Only spellings known to turn it off pass; anything else must be set to n.
RCACHE_OFF = ("n", "no", "0")


class NodeLockedError(RuntimeError):
    """Another remote CXL agent of this node holds the node lock."""


class UncheckedTransportError(RuntimeError):
    """UCX may use a transport that does not check remote keys."""


_SIOCGIFADDR = 0x8915


def interface_ipv4(name: str) -> str | None:
    """IPv4 address of a network interface, or None if it has none."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        try:
            packed = fcntl.ioctl(
                sock.fileno(), _SIOCGIFADDR, struct.pack("256s", name[:15].encode())
            )
        except OSError:
            return None
    return socket.inet_ntoa(packed[20:24])


def rdma_address(
    ucx_device: str,
    sysfs: str = "/sys/class/infiniband",
    ipv4: Callable[[str], str | None] = interface_ipv4,
) -> str:
    """The IPv4 address of the network interface of an RDMA device.

    Workers reach the remote CXL agent over the same network their RDMA uses,
    so this is the address it listens on when none is given.

    Args:
        ucx_device: UCX device, e.g. ``mlx5_1:1``.
        sysfs: Root of the RDMA devices in sysfs.
        ipv4: Interface name -> its IPv4 address.

    Returns:
        The address.

    Raises:
        ValueError: if no device is given, or none of its interfaces has an
            IPv4 address.
    """
    device = ucx_device.partition(":")[0].strip()
    if not device:
        raise ValueError(
            "no bind address: set MARU_REMOTE_CXL_AGENT_HOST (--host) or "
            "MARU_REMOTE_CXL_AGENT_UCX_DEVICE (--ucx-device)"
        )
    net = Path(sysfs) / device / "device" / "net"
    names = sorted(p.name for p in net.iterdir()) if net.is_dir() else []
    for name in names:
        address = ipv4(name)
        if address:
            return address
    raise ValueError(
        f"RDMA device {device} has no network interface with an IPv4 address; "
        "set MARU_REMOTE_CXL_AGENT_HOST (--host)"
    )


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
    if "cuda_copy" in items and not allow_unchecked:
        rdma = CHECKED_UCX_TRANSPORTS - {"self"}
        if not any(item in rdma for item in items):
            raise UncheckedTransportError(
                f"UCX_TLS={value!r} enables local cuda_copy without an RC/DC "
                "remote data transport; include rc or dc"
            )
    return value


def _checked_transport(item: str) -> bool:
    """Whether one ``UCX_TLS`` entry keeps RDMA on key-checking transports."""
    if item == "cuda_copy":
        return True
    name, _, flag = item.partition(":")
    if name in CHECKED_UCX_TRANSPORTS:
        return True
    return name in AUX_ONLY_UCX_TRANSPORTS and flag == "aux"


def default_lock_file(node_id: str) -> str:
    """Return the default per-node lock file path."""
    return f"/run/maru/remote-cxl-agent-{node_id.replace(os.sep, '_')}.lock"


def hold_node_lock(path: str) -> int:
    """Take the node's remote CXL agent lock for the life of the process.

    Two remote CXL agents on one node would register the same region twice and
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
            f"{path} is held by another maru-remote-cxl-agent of this node"
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
    """Command-line options of ``maru-remote-cxl-agent``."""
    parser = argparse.ArgumentParser(
        prog="maru-remote-cxl-agent",
        description="Maru remote CXL agent - maps resource-manager regions and "
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
        default=env.get("MARU_REMOTE_CXL_AGENT_HOST", "127.0.0.1"),
        help="Bind address (env MARU_REMOTE_CXL_AGENT_HOST; default: 127.0.0.1). "
        "Empty: the address of the --ucx-device network interface",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(env.get("MARU_REMOTE_CXL_AGENT_PORT", "9860")),
        help="Bind port (env MARU_REMOTE_CXL_AGENT_PORT; default: 9860)",
    )
    parser.add_argument(
        "--node-id", default=socket.gethostname(), help="Node name (default: hostname)"
    )
    parser.add_argument(
        "--ucx-device",
        default=env.get("MARU_REMOTE_CXL_AGENT_UCX_DEVICE", ""),
        help="UCX device, e.g. mlx5_0:1 (env MARU_REMOTE_CXL_AGENT_UCX_DEVICE; "
        "default: the UCX default)",
    )
    parser.add_argument(
        "--key-file",
        default=env.get("MARU_REMOTE_CXL_AGENT_KEY_FILE") or DEFAULT_KEY_FILE,
        help="Remote CXL agent key that every Handler and MaruServer connection "
        "must prove (env MARU_REMOTE_CXL_AGENT_KEY_FILE; default: "
        f"{DEFAULT_KEY_FILE})",
    )
    parser.add_argument(
        "--lock-file",
        default=env.get("MARU_REMOTE_CXL_AGENT_LOCK_FILE") or None,
        help="Lock held for the life of the process, so one remote CXL agent runs "
        "per node (env MARU_REMOTE_CXL_AGENT_LOCK_FILE; default: "
        "/run/maru/remote-cxl-agent-<node-id>.lock)",
    )
    parser.add_argument(
        "--allow-unchecked-transport",
        action="store_true",
        help="Accept any UCX_TLS and UCX_RCACHE_ENABLE, e.g. tcp for a test on "
        "one host. A late RDMA WRITE can then land in a region after its owner "
        "released it",
    )
    parser.add_argument(
        "--region-extension",
        default=env.get("MARU_REMOTE_CXL_AGENT_REGION_EXTENSION", ""),
        help="Name of a region extension an installed package registers under "
        "the maru.region_extensions entry point (e.g. the read hint handler "
        "of a CXL-SSD) (env MARU_REMOTE_CXL_AGENT_REGION_EXTENSION; default: "
        "none)",
    )
    parser.add_argument(
        "--region-extension-config",
        default=env.get("MARU_REMOTE_CXL_AGENT_REGION_EXTENSION_CONFIG", ""),
        help="Opaque string passed to the region extension, typically the path "
        "of its own configuration file (env "
        "MARU_REMOTE_CXL_AGENT_REGION_EXTENSION_CONFIG)",
    )
    parser.add_argument(
        "--max-hint-requests",
        type=int,
        default=MAX_HINT_REQUESTS,
        help="Read hint requests one connection may keep open; more are "
        f"refused. Default: {MAX_HINT_REQUESTS}",
    )
    parser.add_argument(
        "--memory-generation-poll-s",
        type=float,
        default=GENERATION_POLL_S,
        help="How often the region extension's memory generation is read; a "
        "reset of the memory is acted on within the extension's own detection "
        f"time plus this. Default: {GENERATION_POLL_S}",
    )
    parser.add_argument(
        "--memory-generation-grace-s",
        type=float,
        default=GENERATION_GRACE_S,
        help="How long the memory generation may go unanswered before every "
        "region is released and maps are refused until it answers. Default: "
        f"{GENERATION_GRACE_S}",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    return parser


def raise_file_limit() -> None:
    """Raise the soft file-descriptor limit to the hard one.

    Every Handler connection holds a descriptor; the server keeps its
    connection limit below the soft limit (``server._connection_limit``).
    """
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft != hard:
            resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    except (OSError, ValueError) as exc:
        logger.warning("cannot raise the file-descriptor limit: %s", exc)


def main(argv: list[str] | None = None) -> int:
    """Run the remote CXL agent until SIGINT or SIGTERM.

    Args:
        argv: Command-line arguments (default: ``sys.argv[1:]``).

    Returns:
        0 after a signal; 1 if the resource manager address is empty, there
        is no bind address or key, another remote CXL agent holds this node, or
        UCX may use a transport that does not check remote keys.
    """
    args = build_parser().parse_args(argv)
    level = getattr(logging, args.log_level)
    for package in ("maru_remote_cxl_agent", "maru_remote", "maru_shm"):
        package_logger = setup_package_logging(package)
        package_logger.setLevel(level)
        for handler in package_logger.handlers:
            handler.setLevel(level)

    if not args.rm_address.strip():
        logger.error(
            "no resource manager address: set MARU_RM_ADDRESS or --rm-address "
            "to the pool's resource manager (host:port)"
        )
        return 1

    try:
        transports = require_checked_transport(
            os.environ, args.allow_unchecked_transport
        )
        host = args.host.strip() or rdma_address(args.ucx_device)
        key = load_key(args.key_file)
    except (UncheckedTransportError, ValueError) as exc:
        logger.error("%s", exc)
        return 1

    boot_id = read_boot_id()
    if not boot_id:
        # Without it no worker could tell that the node rebooted and lost
        # the writes its CPU caches held; workers refuse such an agent.
        logger.error(
            "cannot read this node's boot id (/proc/sys/kernel/random/boot_id)"
        )
        return 1

    lock_file = args.lock_file or default_lock_file(args.node_id)
    try:
        lock_fd = hold_node_lock(lock_file)
    except (NodeLockedError, OSError) as exc:
        logger.error("cannot take the node lock %s: %s", lock_file, exc)
        return 1

    raise_file_limit()

    # Imported here: NIXL and the RM client are only needed to run the daemon.
    from maru_remote.transport import NixlTransport
    from maru_shm import MaruShmClient

    epoch = uuid.uuid4().hex
    extension = None
    if args.region_extension:
        from .extension import load_region_extension

        try:
            extension = load_region_extension(
                args.region_extension, epoch, args.region_extension_config
            )
        except Exception as exc:
            logger.error(
                "cannot load region extension %s: %s", args.region_extension, exc
            )
            os.close(lock_fd)
            return 1
    elif args.region_extension_config:
        os.close(lock_fd)
        build_parser().error("--region-extension-config needs --region-extension")
    transport = NixlTransport(
        f"maru-remote-cxl-agent-{args.node_id}-{epoch[:8]}", ucx_device=args.ucx_device
    )
    rm = MaruShmClient(address=args.rm_address)
    try:
        service = RegionMapService(
            rm,
            DaxRegionMapper(),
            transport,
            node_id=args.node_id,
            epoch=epoch,
            boot_id=boot_id,
            extension=extension,
            max_hint_requests=args.max_hint_requests,
            generation_grace_s=args.memory_generation_grace_s,
        )
    except Exception as exc:
        # The region extension cannot tell its memory generation.
        logger.error("cannot start the remote CXL agent: %s", exc)
        transport.close()
        rm.close()
        os.close(lock_fd)
        return 1
    server = RegionMapServer(service, host, args.port, key)

    def stop(signum, frame):
        threading.Thread(target=server.stop, daemon=True).start()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    if host == "0.0.0.0":
        logger.warning(
            "Binding to 0.0.0.0: every network of this node reaches the remote CXL "
            "agent; the key still decides who maps a region"
        )
    logger.info(
        "remote CXL agent for node %s (epoch %s) on %s:%d, resource manager %s, "
        "key %s, UCX_TLS=%s, UCX_RCACHE_ENABLE=%s",
        args.node_id,
        epoch,
        *server.address,
        args.rm_address,
        args.key_file,
        transports,
        os.environ.get("UCX_RCACHE_ENABLE"),
    )
    try:
        # Releases mappings of regions the resource manager reaped on its own.
        service.start_sweep()
        service.start_generation_watch(args.memory_generation_poll_s)
        server.serve_forever()
    finally:
        service.close()
        transport.close()
        rm.close()
        os.close(lock_fd)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
