# SPDX-License-Identifier: Apache-2.0
"""Run the remote server on a pool node.

Example::

    maru-remote-server --server-url tcp://127.0.0.1:5555 --pool-size 8G \\
        --page-bytes 4M --ctrl-url tcp://0.0.0.0:6600 \\
        --ucx-device mlx5_1:1 --pool-id pool-a

The process must not load handler plugins or CUDA: start it with
``MARU_PLUGINS=none`` and ``CUDA_VISIBLE_DEVICES=""``.
"""

from __future__ import annotations

import argparse
import logging
import re
import signal
import sys
import threading
from types import FrameType

from maru_common.config import MaruConfig
from maru_handler import MaruHandler

from .kv_manager import KVManager, ProcessPinExecutor
from .server import RemoteServer, serve_forever
from .stager import Stager, device_prefetcher
from .transport import NixlTransport

logger = logging.getLogger("maru_remote")

_SIZE_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*([KMGT]?)B?$")
_UNITS = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}


def parse_size(text: str) -> int:
    """Parse a byte size such as ``"8G"``, ``"4M"``, ``"64K"`` or ``"123"``.

    Units are binary (K = 1024). A trailing ``B`` and lower case are accepted.

    Args:
        text: The size string.

    Returns:
        The size in bytes.

    Raises:
        ValueError: if ``text`` is not a non-negative size.
    """
    match = _SIZE_RE.match(text.strip().upper())
    if match is None:
        raise ValueError(f"invalid size {text!r}; expected e.g. 8G, 4M or 123")
    value, unit = float(match.group(1)), match.group(2)
    return int(value * _UNITS[unit])


def _serve_until_signalled(server: RemoteServer, ctrl_url: str) -> None:
    """Run the REP loop on this thread until SIGINT or SIGTERM.

    Args:
        server: The server answering requests.
        ctrl_url: Endpoint to bind.
    """
    stop = threading.Event()

    def _on_signal(signum: int, _frame: FrameType | None) -> None:
        """Ask the serve loop to stop."""
        logger.info("signal %d received; stopping", signum)
        stop.set()

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)
    logger.info("remote server listening on %s", ctrl_url)
    serve_forever(server, ctrl_url, stop_event=stop)


def build_parser() -> argparse.ArgumentParser:
    """Return the command-line parser of the remote server."""
    p = argparse.ArgumentParser(
        prog="maru-remote-server",
        description="Serve a node's Maru CXL pool to remote handlers over NIXL.",
    )
    p.add_argument("--server-url", required=True, help="MaruServer ZMQ endpoint")
    p.add_argument("--pool-size", required=True, type=parse_size, help="e.g. 8G")
    p.add_argument(
        "--page-bytes",
        required=True,
        type=parse_size,
        help="pool page size; must hold one KV object (e.g. 4M)",
    )
    p.add_argument("--ctrl-url", required=True, help="endpoint to bind for clients")
    p.add_argument("--ucx-device", default="", help="UCX device, e.g. mlx5_1:1")
    p.add_argument("--pool-id", required=True, help="pool name reported to clients")
    p.add_argument("--instance-id", default=None, help="Maru instance id")
    p.add_argument("--reservation-ttl", type=float, default=60.0, help="seconds")
    p.add_argument("--ticket-ttl", type=float, default=120.0, help="seconds")
    p.add_argument(
        "--quarantine-ttl",
        type=float,
        default=600.0,
        help="seconds a timed-out WRITE's pages stay unused if never abandoned",
    )
    p.add_argument(
        "--capacity",
        type=parse_size,
        default=None,
        help="most pool bytes to hold (default: until the device is full)",
    )
    p.add_argument(
        "--eviction",
        choices=("lru", "none"),
        default="lru",
        help="when full: delete least recently read keys, or refuse new stores",
    )
    p.add_argument(
        "--stage-window",
        type=int,
        default=0,
        help="objects per request to load into the device DRAM ahead of the "
        "worker's reads (InfiniteMemory pools); 0 disables staging",
    )
    p.add_argument(
        "--stage-device",
        type=int,
        default=None,
        help="pyxif device id of the pool's InfiniteMemory device "
        "(required with --stage-window or --prefetch-window)",
    )
    p.add_argument(
        "--prefetch-window",
        type=int,
        default=0,
        help="objects per request held in the device DRAM ahead of the "
        "worker's reads; a worker reads them only once they are there "
        "(InfiniteMemory pools); 0 disables it",
    )
    p.add_argument(
        "--prefetch-workers",
        type=int,
        default=2,
        help="processes that load and hold objects for --prefetch-window",
    )
    p.add_argument(
        "--prefetch-budget",
        type=parse_size,
        default=parse_size("24G"),
        help="bytes held at once for --prefetch-window; keep below the "
        "device's pin limit (half its DRAM)",
    )
    p.add_argument("--log-level", default="INFO", help="logging level name")
    return p


def main(argv: list[str] | None = None) -> int:
    """Connect to Maru, register the pool with NIXL and serve until signalled.

    Args:
        argv: Arguments without the program name; ``sys.argv[1:]`` if None.

    Returns:
        Process exit code.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.stage_window < 0:
        parser.error("--stage-window must be >= 0")
    if args.stage_window and args.stage_device is None:
        parser.error("--stage-window needs --stage-device")
    if args.prefetch_window < 0:
        parser.error("--prefetch-window must be >= 0")
    if args.prefetch_window and args.stage_device is None:
        parser.error("--prefetch-window needs --stage-device")
    if args.prefetch_window and args.stage_window:
        parser.error("use --stage-window or --prefetch-window, not both")
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    handler = MaruHandler(
        MaruConfig(
            server_url=args.server_url,
            pool_size=args.pool_size,
            chunk_size_bytes=args.page_bytes,
            instance_id=args.instance_id,
            auto_connect=False,
            eager_map=True,
            auto_expand=True,
        )
    )
    if not handler.connect():
        logger.error("could not connect to MaruServer at %s", args.server_url)
        return 1
    try:
        transport = NixlTransport(
            f"maru-remote-{args.pool_id}", ucx_device=args.ucx_device
        )
        server = RemoteServer(
            handler,
            transport,
            pool_id=args.pool_id,
            reservation_ttl_s=args.reservation_ttl,
            ticket_ttl_s=args.ticket_ttl,
            quarantine_ttl_s=args.quarantine_ttl,
            capacity_bytes=args.capacity,
            evict=args.eviction == "lru",
            stager=(
                Stager(args.stage_window, device_prefetcher(args.stage_device))
                if args.stage_window
                else None
            ),
            kv_manager=(
                KVManager(
                    args.prefetch_window,
                    ProcessPinExecutor(args.stage_device, args.prefetch_workers),
                    max_held_bytes=args.prefetch_budget,
                    group_ttl_s=10.0,
                )
                if args.prefetch_window
                else None
            ),
        )
        try:
            _serve_until_signalled(server, args.ctrl_url)
        finally:
            server.close()
    finally:
        handler.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
