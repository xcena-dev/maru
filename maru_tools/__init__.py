# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Maru monitoring and admin tooling.

A single ``marutop`` command umbrella (see :mod:`maru_tools.cli`) that fuses the
former standalone ``tools/*.py`` scripts:

- ``marutop``          — unified live pool + per-instance view (default)
- ``marutop pool``     — physical DAX pool gauges (Resource Manager)
- ``marutop usage``    — per-instance allocated/used/slack (MaruServer)
- ``marutop stats``    — per-operation latency/throughput dashboard (MaruServer)
- ``marutop device``   — DAX device UUID header init/clear/show (Resource Manager)
"""
