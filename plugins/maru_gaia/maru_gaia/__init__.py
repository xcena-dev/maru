# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Gaia (XCENA InfiniteMemory) prefetch plugin for Maru."""

from maru_common.logging_setup import setup_package_logging

# Install a captured StreamHandler for the maru_gaia logger tree (propagate=
# False, level from MARU_LOG_LEVEL/LMCACHE_LOG_LEVEL) so the plugin's
# gaia_prefetch hint lines land in the same stderr stream as maru core — vLLM's
# root logger otherwise filters this out-of-tree package's INFO logs.
setup_package_logging("maru_gaia")

from maru_gaia.plugin import GaiaPrefetchPlugin  # noqa: E402

__all__ = ["GaiaPrefetchPlugin"]
