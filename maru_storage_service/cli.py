# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Node commands, config checks and signal handling."""

import argparse
import dataclasses
import json
import logging
import os
import signal
import sys
import threading
from enum import Enum
from typing import Any

from maru_common import gc_freeze

from .config import (
    GPU_KEYS,
    MEMORY_KEYS,
    WORKER_BUDGET_KEYS,
    NodeConfig,
    NodeConfigError,
    load_node_config,
)
from .drivers.registry import TIERS

EXIT_CONFIG = 2
EXIT_NOT_SERVING = 3
EXIT_START = 4
# Another run claimed the node in the RM; never claimed back from here
EXIT_LOST = 5
# A run could not be torn down: no run may follow it in this process
EXIT_FAILED = 6

logger = logging.getLogger(__name__)


def config_to_dict(config: NodeConfig) -> dict[str, Any]:
    """The resolved config as plain JSON values."""

    def plain(value: Any) -> Any:
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, dict):
            return {k: plain(v) for k, v in value.items()}
        if isinstance(value, list | tuple):
            return [plain(v) for v in value]
        return value

    out: dict[str, Any] = plain(
        dataclasses.asdict(dataclasses.replace(config, tiers=()))
    )
    # A store's options print as its other keys do
    out["tiers"] = [
        plain(
            {
                **{
                    f.name: getattr(t, f.name)
                    for f in dataclasses.fields(t)
                    if f.name != "options"
                    and f.name in TIERS[t.store].keys
                    and (
                        config.role == "memory"
                        or f.name not in {"high_watermark", "low_watermark"}
                    )
                },
                **t.options,
            }
        )
        for t in config.tiers
    ]
    out.pop("source", None)
    out.pop("role")
    if config.role == "memory":
        for key in GPU_KEYS:
            out.pop(key)
        for key in WORKER_BUDGET_KEYS:
            out["budgets"].pop(key)
    else:
        for key in MEMORY_KEYS:
            out.pop(key)
    return out


def main(argv: list[str] | None = None) -> int:
    return _main(argv, "gpu", "maru-storage")


def memory_main(argv: list[str] | None = None) -> int:
    return _main(argv, "memory", "maru-memory-node")


def _main(argv: list[str] | None, role: str, command: str) -> int:
    parser = argparse.ArgumentParser(
        prog=command,
        description="StorageService: the pool manager of one maru node.",
    )
    parser.add_argument("--config", required=True, help="node config file (JSON)")
    parser.add_argument(
        "--check",
        action="store_true",
        help="check the config, print it with defaults filled in, and exit",
    )
    parser.add_argument(
        "--log-level", default="INFO", help="logging level (default INFO)"
    )
    args = parser.parse_args(argv)
    try:
        config = load_node_config(args.config, role=role)
    except NodeConfigError as e:
        print(f"{command}: {e}", file=sys.stderr)
        return EXIT_CONFIG
    from .service import NotServableError, check_servable

    try:
        check_servable(config)
    except NotServableError as e:
        print(f"{command}: {e}", file=sys.stderr)
        return EXIT_NOT_SERVING
    if args.check:
        print(json.dumps(config_to_dict(config), indent=2))
        return 0
    return serve(config, args.log_level)


def _signal_handler(wfd: int):
    """A handler that only wakes the thread that stops the node."""

    def on_signal(_signum, _frame) -> None:
        try:
            os.write(wfd, b"x")
        except OSError:  # a full pipe: a stop is pending already
            pass

    return on_signal


def serve(config: NodeConfig, log_level: str = "INFO") -> int:
    """Run the node until SIGTERM or SIGINT."""
    from .drivers.cxl import CxlTierError
    from .drivers.dram import DramPoolError
    from .ownership import LocalServerError
    from .server_client import NodeOpenError
    from .service import Node, NotServableError

    command = "maru-memory-node" if config.role == "memory" else "maru-storage"
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        node = Node(config)
    except NotServableError as e:
        print(f"{command}: {e}", file=sys.stderr)
        return EXIT_NOT_SERVING

    rfd, wfd = os.pipe()
    os.set_blocking(wfd, False)
    # Only a write in the handler: stop() sets Events the main thread may be
    # waiting in, and that would deadlock if the signal landed in set()
    on_signal = _signal_handler(wfd)

    def stop_on_signal() -> None:
        try:
            woken = os.read(rfd, 1)
        finally:
            os.close(rfd)
        if woken:  # empty: serve() ended
            # Also stops a start still waiting for the RM or MaruServer
            node.stop()

    threading.Thread(target=stop_on_signal, name="maru-signal", daemon=True).start()
    gc_freeze.start_freezer()
    previous = {
        sig: signal.signal(sig, on_signal) for sig in (signal.SIGTERM, signal.SIGINT)
    }
    try:
        try:
            if node.run():
                return 0
        except (
            CxlTierError,
            DramPoolError,
            LocalServerError,
            NodeOpenError,
            OSError,
        ) as e:
            print(f"{command}: cannot start: {e}", file=sys.stderr)
            if node.failed is not None:
                return EXIT_FAILED
            return EXIT_START
        if node.failed is not None:
            print(f"{command}: a run's teardown failed: {node.failed}", file=sys.stderr)
            return EXIT_FAILED
        print(f"{command}: stopped: {node.ended}", file=sys.stderr)
        return EXIT_LOST
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        os.close(wfd)  # the signal thread's read ends
