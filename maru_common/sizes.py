# SPDX-License-Identifier: Apache-2.0
"""Byte sizes written as text, as command-line options use them."""

from __future__ import annotations

import re

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
