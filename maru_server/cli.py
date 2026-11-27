# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Select the metadata server backend."""

import argparse


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="maru-server", add_help=False, allow_abbrev=False
    )
    parser.add_argument("--backend", choices=("cxl", "node"), default="cxl")
    args, remaining = parser.parse_known_args(argv)
    if args.backend == "node":
        from .node_server import main as serve
    else:
        from .server import main as serve
    serve(remaining)
