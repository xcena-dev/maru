# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Maru - Shared memory KV cache for LLM inference.

Maru (마루, /mɑːruː/) — named after the central open floor
in traditional Korean architecture where all rooms connect
and family members freely gather and share.

This package provides a public API re-export from maru_common and maru_handler.
For implementation details, see maru_common/ and maru_handler/.
"""

from maru_common.logging_setup import setup_package_logging  # noqa: E402

setup_package_logging("maru")

from maru_common import MaruConfig  # noqa: E402
from maru_handler import MaruHandler  # noqa: E402
from maru_handler.memory import AllocHandle  # noqa: E402

__version__ = "0.1.0"

__all__ = [
    "AllocHandle",
    "MaruConfig",
    "MaruHandler",
]
