# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Maru remote CXL agent: maps CXL regions for Handlers on other nodes.

A remote CXL agent runs on every memory node. It does for a remote Handler what
``DaxMapper`` does inside a Handler on a CXL-attached node (check the handle
with the resource manager, mmap the region) and registers the mapping with
the node's NIC so the Handler reaches it by one-sided RDMA.
"""

from .mapper import DaxRegionMapper
from .server import RegionMapServer
from .service import MapDenied, RegionMapService

__all__ = ["DaxRegionMapper", "MapDenied", "RegionMapServer", "RegionMapService"]
