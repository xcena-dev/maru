# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""``marutop`` command entry point.

A single umbrella over the maru monitoring/admin views. With no subcommand it
runs the unified live view (:mod:`maru_tools.live`); named subcommands select a
specific view or the device admin tool::

    marutop                      # unified live pool + per-instance view
    marutop pool   [...]         # physical DAX pool gauges
    marutop usage  [...]         # per-instance allocated/used/slack
    marutop stats  [...]         # per-operation latency/throughput dashboard
    marutop device init|show|clear <dax-path>

Each subcommand forwards its remaining arguments to the corresponding module's
``main()``, so ``marutop pool --help`` shows that view's own options.
"""

import sys

from maru_tools import device, live, pool, stats, usage

_SUBCOMMANDS = {
    "live": live,
    "pool": pool,
    "usage": usage,
    "stats": stats,
    "device": device,
}

_TOP_HELP = """\
usage: marutop [<command>] [<args>]

Maru monitoring and admin tools. With no command, runs the unified live view.

commands:
  (default)   unified live view: DEVICES (pool) + INSTANCES (per-instance)
  live        same as default; run `marutop live --help` for options
  pool        physical DAX pool gauges (resource manager)
  usage       per-instance allocated/used/slack (maru server)
  stats       per-operation latency/throughput dashboard (maru server)
  device      DAX device UUID header: init | show | clear <dax-path>

Run `marutop <command> --help` for command-specific options.
"""


def main(argv: list[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)

    # Top-level help only when no subcommand precedes it; otherwise the flag
    # belongs to the default live view (e.g. `marutop --help` vs the live help).
    if argv and argv[0] in ("-h", "--help"):
        print(_TOP_HELP, end="")
        return

    if argv and argv[0] in _SUBCOMMANDS:
        _SUBCOMMANDS[argv[0]].main(argv[1:])
        return

    # No recognized subcommand → default to the unified live view, forwarding
    # any flags (e.g. `marutop -w 2`).
    live.main(argv)


if __name__ == "__main__":
    main()
