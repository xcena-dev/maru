# SPDX-License-Identifier: Apache-2.0
"""Maru SGLang HiCache integration package.

Provides MaruHiCacheStorage as an L3 storage backend for SGLang's
hierarchical KV cache (HiCache) system.
"""

from maru_common.logging_setup import setup_package_logging  # noqa: E402

setup_package_logging("maru_sglang")

from .config import MaruSGLangConfig


def __getattr__(name):
    """Lazy import for MaruHiCacheStorage (requires sglang)."""
    if name == "MaruHiCacheStorage":
        from .backend import MaruHiCacheStorage

        return MaruHiCacheStorage
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "MaruHiCacheStorage",
    "MaruSGLangConfig",
]
