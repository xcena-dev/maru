#!/usr/bin/env python3
"""Backward-compat shim — moved to :mod:`maru_tools.device` (`marutop device`).

Prefer `marutop device init|show|clear <dax-path>` (installed console script).
This shim keeps `python tools/maru_rm_tool.py device ...` working from the repo
root. Note: `marutop device` splits the old `init --show` into an explicit
`show` subcommand (the `init --show` alias still works).
"""

from maru_tools.device import main

if __name__ == "__main__":
    main()
