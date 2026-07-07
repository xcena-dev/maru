#!/usr/bin/env python3
"""Backward-compat shim — moved to :mod:`maru_tools.stats` (`marutop stats`).

Prefer `marutop stats` (installed console script). This shim keeps
`python -m tools.stats_monitor` working from the repo root.
"""

from maru_tools.stats import main

if __name__ == "__main__":
    main()
