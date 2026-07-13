#!/usr/bin/env python3
"""Backward-compat shim — moved to :mod:`maru_tools.usage` (`marutop usage`).

Prefer `marutop usage` (installed console script) or `marutop` for the unified
view. This shim keeps `python -m tools.usage_monitor` working from the repo root.
"""

from maru_tools.usage import main

if __name__ == "__main__":
    main()
