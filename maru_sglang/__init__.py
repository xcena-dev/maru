# SPDX-License-Identifier: Apache-2.0
"""Maru SGLang HiCache integration package.

Provides MaruStorage as an L3 storage backend for SGLang's
hierarchical KV cache (HiCache) system.
"""

from maru_common.logging_setup import setup_package_logging  # noqa: E402

setup_package_logging("maru_sglang")

from .config import MaruSGLangConfig  # noqa: E402


def __getattr__(name):
    """Lazy import for MaruStorage (requires sglang)."""
    if name == "MaruStorage":
        from .maru_storage import MaruStorage

        return MaruStorage
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "MaruStorage",
    "MaruSGLangConfig",
]
