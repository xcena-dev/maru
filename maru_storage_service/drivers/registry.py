# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""The stores a tier may name, in the order config errors list them."""

from ..config_fields import TierSpec
from . import cxl, dram, remote

TIERS: dict[str, TierSpec] = {s.store: s for s in (dram.SPEC, cxl.SPEC, remote.SPEC)}
