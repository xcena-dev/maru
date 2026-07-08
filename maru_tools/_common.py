# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Shared formatting helpers for the marutop views.

These were previously duplicated verbatim across ``pool_monitor.py`` and
``usage_monitor.py``; consolidating them here removes the drift risk.
"""

import sys


def fmt_size(nbytes: int) -> str:
    """Format a byte count as a human-readable string (negative-aware).

    Slack (allocated - used) can go negative transiently, so the sign is
    handled explicitly rather than assumed non-negative.
    """
    if nbytes < 0:
        return "-" + fmt_size(-nbytes)
    if nbytes == 0:
        return "0B"
    if nbytes >= 1024**4:
        return f"{nbytes / 1024**4:.1f}T"
    if nbytes >= 1024**3:
        return f"{nbytes / 1024**3:.1f}G"
    if nbytes >= 1024**2:
        return f"{nbytes / 1024**2:.1f}M"
    return f"{nbytes / 1024:.1f}K"


def usage_bar(used: int, total: int, width: int = 30) -> str:
    """Render a ``[####------] 42.0%`` style usage bar."""
    if total == 0:
        return "[" + "?" * width + "]"
    ratio = used / total
    filled = int(ratio * width)
    return "[" + "#" * filled + "-" * (width - filled) + f"] {ratio * 100:.1f}%"


def clear_screen() -> None:
    """Move cursor home and clear the terminal (top-style refresh)."""
    sys.stdout.write("\033[2J\033[H")
    sys.stdout.flush()
