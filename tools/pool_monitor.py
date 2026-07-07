#!/usr/bin/env python3
"""Backward-compat shim — moved to :mod:`maru_tools.pool` (`marutop pool`).

Prefer `marutop pool` (installed console script) or `marutop` for the unified
view. This shim keeps `python -m tools.pool_monitor` / `python tools/pool_monitor.py`
working from the repo root.
"""

from maru_tools.pool import main

if __name__ == "__main__":
    main()
