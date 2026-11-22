# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""maru-node-server: the Catalog of the node path in its own process.

StorageServices (``maru-storage``) and the workers of ``maru_node`` engines
talk to it; the storage backends keep talking to maru-server. Each run has its
own server epoch: a node or worker that sees another one starts a new session.
"""

import argparse
import logging
import os
import signal
import threading
import uuid

from maru_common import gc_freeze

from .ms_service import (
    DEFAULT_NODE_GIVE_UP_S,
    DEFAULT_SESSION_TTL_S,
    MsService,
    PolicyError,
    load_policy,
)
from .node_rpc import NodeRpcServer
from .op_ledger import DEFAULT_OP_BYTES_MAX, DEFAULT_OP_WINDOW
from .server import setup_logging

logger = logging.getLogger(__name__)

DEFAULT_PORT = 5556


class NodeServer:
    """One run of the node server: its server epoch and MsService."""

    def __init__(
        self,
        session_ttl_s: float = DEFAULT_SESSION_TTL_S,
        heartbeat_s: float | None = None,
        node_give_up_s: float = DEFAULT_NODE_GIVE_UP_S,
        op_window: int = DEFAULT_OP_WINDOW,
        op_bytes_max: int = DEFAULT_OP_BYTES_MAX,
        policy_file: str | None = None,
    ):
        self.server_epoch = uuid.uuid4().hex
        self.ms_service = MsService(
            server_epoch=self.server_epoch,
            session_ttl_s=session_ttl_s,
            heartbeat_s=heartbeat_s,
            node_give_up_s=node_give_up_s,
            op_window=op_window,
            op_bytes_max=op_bytes_max,
            policy_file=policy_file,
        )

    def close(self) -> None:
        self.ms_service.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "maru-node-server - the Catalog of Maru StorageServices (maru-storage) "
            "and maru_node engines"
        )
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind the server to (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"Port to bind the server to (default: {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    parser.add_argument(
        "--session-ttl-s",
        type=float,
        default=DEFAULT_SESSION_TTL_S,
        help=(
            "Node and worker session TTL in seconds; pins do not expire with it "
            f"(default: {DEFAULT_SESSION_TTL_S:g})"
        ),
    )
    parser.add_argument(
        "--heartbeat-s",
        type=float,
        default=None,
        help="Heartbeat interval given to nodes and workers (default: session TTL / 3)",
    )
    parser.add_argument(
        "--node-give-up-s",
        type=float,
        default=DEFAULT_NODE_GIVE_UP_S,
        help=(
            "Seconds an expired StorageService may stay away before its "
            f"locations are removed (default: {DEFAULT_NODE_GIVE_UP_S:g})"
        ),
    )
    parser.add_argument(
        "--op-window",
        type=int,
        default=DEFAULT_OP_WINDOW,
        help=(
            "Unacknowledged op results kept per origin; more get BUSY "
            f"(default: {DEFAULT_OP_WINDOW})"
        ),
    )
    parser.add_argument(
        "--op-bytes-max",
        type=int,
        default=DEFAULT_OP_BYTES_MAX,
        help=(
            "Bytes of unacknowledged op results kept per origin; more get BUSY "
            f"(default: {DEFAULT_OP_BYTES_MAX})"
        ),
    )
    parser.add_argument(
        "--policy-file",
        default=None,
        help=(
            'Policy JSON, e.g. {"preserve": [{"namespace": "ns", "key": "k"}]}; '
            "reloaded when it changes"
        ),
    )
    args = parser.parse_args()
    if args.policy_file is not None:
        try:
            load_policy(args.policy_file)
        except PolicyError as exc:
            parser.error(str(exc))
    if not args.session_ttl_s > 0:
        parser.error("--session-ttl-s must be positive")
    heartbeat_s = (
        args.session_ttl_s / 3 if args.heartbeat_s is None else args.heartbeat_s
    )
    if not 0 < heartbeat_s < args.session_ttl_s:
        parser.error("--heartbeat-s must be positive and below --session-ttl-s")
    if not args.node_give_up_s > 0:
        parser.error("--node-give-up-s must be positive")
    if args.op_window < 1:
        parser.error("--op-window must be at least 1")
    if args.op_bytes_max < 1:
        parser.error("--op-bytes-max must be at least 1")

    setup_logging(args.log_level)
    gc_freeze.start_freezer()
    server = NodeServer(
        session_ttl_s=args.session_ttl_s,
        heartbeat_s=heartbeat_s,
        node_give_up_s=args.node_give_up_s,
        op_window=args.op_window,
        op_bytes_max=args.op_bytes_max,
        policy_file=args.policy_file,
    )
    rpc = NodeRpcServer(server, host=args.host, port=args.port)
    # Only a write in the handler: stop() takes the lock start() holds while
    # it binds, which would deadlock if the signal landed then
    rfd, wfd = os.pipe()
    os.set_blocking(wfd, False)

    def on_signal(signum, frame):
        try:
            os.write(wfd, b"x")
        except OSError:  # a full pipe: a stop is pending already
            pass

    def stop_on_signal() -> None:
        try:
            woken = os.read(rfd, 1)
        finally:
            os.close(rfd)
        if woken:  # empty: main() ended
            rpc.stop()

    stopper = threading.Thread(target=stop_on_signal, name="maru-signal", daemon=True)
    stopper.start()
    previous = {
        sig: signal.signal(sig, on_signal) for sig in (signal.SIGINT, signal.SIGTERM)
    }
    logger.info("Starting maru-node-server on %s:%d", args.host, args.port)
    try:
        rpc.start()
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        os.close(wfd)  # the signal thread's read ends
        stopper.join(10)  # a stop it began ends first
        server.close()


if __name__ == "__main__":
    main()
