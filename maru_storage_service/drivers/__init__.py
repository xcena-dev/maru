# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Resources behind the pools workers map."""

from enum import StrEnum


class PoolAccess(StrEnum):
    """How workers reach a pool's bytes."""

    MAPPED = "mapped"  # workers map the pool and copy themselves (DRAM, CXL)
