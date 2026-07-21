# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Gaia (XCENA InfiniteMemory) prefetch plugin for MaruHandler.

Out-of-tree device plugin: issues SSD->DRAM prefetch hints to a gaia CXL device
via ``pyxif``, so KV chunks are warm in the device's DRAM cache by the time the
demand read runs. Maru core stays vendor-neutral and never imports pyxif — this
package registers itself under the ``maru.handler_plugins`` entry point and is
invoked only at the handler's defined seams.

Two seams are implemented:

- :meth:`on_batch_retrieve` — reactive hint issued during a demand read. This is
  the baseline behavior (a hint at read time gives little lead, so retrieve
  bandwidth stays near the SSD ceiling).
- :meth:`on_prefetch` — lookahead hint fired *ahead* of demand by smart-prefetch's
  arrival-hint, turning a request's admission wait into the device's fill window.

Both target the payload address the handler exposes for a found key:
``entry.handle.offset + entry.kv_offset`` for ``entry.kv_length`` bytes, and only
for regions that are already mapped (an unmapped region has no live address and
is prefaulted on its own ``map_region`` at demand time).

Environment:
    MARU_GAIA_PREFETCH_COALESCE=0  Disable coalescing of contiguous
        ``(device_id, addr)`` ranges into one ``memory_prefetch`` call.
        Default on: without it a single arrival burst expands to thousands of
        per-chunk hints that overrun the device's concurrent prefetch budget.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

import pyxif

if TYPE_CHECKING:
    from maru_common import BatchLookupKVResponse
    from maru_handler import MaruHandler

logger = logging.getLogger(__name__)


class GaiaPrefetchPlugin:
    """Issues gaia device prefetch hints at MaruHandler's retrieve seams."""

    def __init__(self) -> None:
        self._coalesce = os.environ.get("MARU_GAIA_PREFETCH_COALESCE", "1") == "1"
        # The experiment always names the single gaia device via
        # MARU_GAIA_DEVICE_ID (same env naru/gaia-bench use to set DRAM
        # capacity). Prefer it: pyxif's auto-enumeration (get_device_list) is
        # not reliable across SDK builds — it returns [] on the current host,
        # which silently made every hint a no-op. The dax-path scan is kept
        # only as a multi-device fallback for when the env is unset.
        dev_env = os.environ.get("MARU_GAIA_DEVICE_ID")
        self._device_id: int | None = int(dev_env) if dev_env else None
        # dax device path -> pyxif device id, built lazily on first hint (the
        # scan is not free and a handler may never prefetch). Fallback only.
        self._dax_to_device: dict[str, int] | None = None
        # Cumulative counters surfaced via contribute_stats.
        self._issued = 0
        self._failed = 0
        self._skipped = 0
        logger.info(
            "Gaia prefetch plugin loaded (coalesce=%s, device_id=%s)",
            "on" if self._coalesce else "off",
            self._device_id if self._device_id is not None else "auto-scan",
        )

    # -- MaruHandlerPlugin seams -------------------------------------------

    def on_batch_retrieve(
        self,
        handler: MaruHandler,
        keys: list[str],
        batch_resp: BatchLookupKVResponse,
    ) -> None:
        """Reactive hint at demand read time (baseline behavior)."""
        self._issue(handler, batch_resp, source="retrieve")

    def on_prefetch(
        self,
        handler: MaruHandler,
        keys: list[str],
        batch_resp: BatchLookupKVResponse,
    ) -> None:
        """Lookahead hint fired ahead of demand by arrival-hint."""
        self._issue(handler, batch_resp, source="prefetch")

    def contribute_stats(self) -> dict:
        """Cumulative prefetch counters for ``MaruHandler.get_stats``."""
        return {
            "issued": self._issued,
            "failed": self._failed,
            "skipped": self._skipped,
            "coalesce": self._coalesce,
        }

    # -- internals ----------------------------------------------------------

    def _resolve_device_id(self, handler: MaruHandler, region_id: int) -> int | None:
        """Resolve a mapped region to its gaia pyxif device id.

        Returns the env-configured ``MARU_GAIA_DEVICE_ID`` when set (the single
        gaia device this experiment targets); otherwise falls back to the
        dax-path scan map (multi-device, only usable where pyxif enumeration
        works).
        """
        if self._device_id is not None:
            return self._device_id
        dax_path = handler.get_region_dax_path(region_id)
        if dax_path is None:
            return None
        return self._device_map().get(dax_path)

    def _device_map(self) -> dict[str, int]:
        """Return the dax-path -> device-id map, built once via pyxif (fallback)."""
        if self._dax_to_device is None:
            dax_map: dict[str, int] = {}
            for device_id in pyxif.get_device_list():
                for region in pyxif.cxl_get_regions(device_id):
                    dax_map[region.dax_device] = device_id
            self._dax_to_device = dax_map
            logger.info("gaia_prefetch: dax_to_device_map=%s", dax_map)
        return self._dax_to_device

    def _issue(
        self,
        handler: MaruHandler,
        batch_resp: BatchLookupKVResponse,
        source: str,
    ) -> None:
        """Resolve mapped found entries to device ranges and prefetch them.

        Skips entries that are not found, carry no handle, or whose region is
        not currently mapped (only a mapped region has a live address to hint
        against). Contiguous ranges on the same device are merged when
        coalescing is enabled before dispatch.
        """
        eligible: list[tuple[int, int, int]] = []
        skipped = 0
        for entry in batch_resp.entries:
            if not entry.found or entry.handle is None:
                continue
            region_id = entry.handle.region_id
            if not handler.is_region_mapped(region_id):
                skipped += 1
                continue
            device_id = self._resolve_device_id(handler, region_id)
            if device_id is None:
                skipped += 1
                continue
            device_addr = entry.handle.offset + entry.kv_offset
            eligible.append((device_id, device_addr, entry.kv_length))

        chunk_count = len(eligible)
        ranges = self._coalesce_ranges(eligible) if self._coalesce else eligible

        issued = 0
        failed = 0
        for device_id, device_addr, size in ranges:
            status = pyxif.memory_prefetch(device_id, device_addr, size)
            if status == pyxif.MemoryStatus.Success:
                issued += 1
            else:
                failed += 1
                logger.warning(
                    "gaia_prefetch failed: device=%d addr=0x%x size=%d status=%s",
                    device_id,
                    device_addr,
                    size,
                    status,
                )

        self._issued += issued
        self._failed += failed
        self._skipped += skipped
        if issued or failed:
            logger.info(
                "gaia_prefetch(%s): chunks=%d, ranges=%d (issued=%d, failed=%d), "
                "skipped=%d, coalesce=%s",
                source,
                chunk_count,
                len(ranges),
                issued,
                failed,
                skipped,
                self._coalesce,
            )

    @staticmethod
    def _coalesce_ranges(
        chunks: list[tuple[int, int, int]],
    ) -> list[tuple[int, int, int]]:
        """Merge contiguous ``(device_id, addr, size)`` ranges.

        Sorts by ``(device_id, addr)`` and folds adjacent entries where
        ``prev_end == next_addr`` on the same device into one range, so a
        physically contiguous slab of chunks becomes a single prefetch call.
        """
        if not chunks:
            return []
        ordered = sorted(chunks, key=lambda c: (c[0], c[1]))
        merged: list[tuple[int, int, int]] = []
        cur_dev, cur_addr, cur_len = ordered[0]
        for dev, addr, length in ordered[1:]:
            if dev == cur_dev and addr == cur_addr + cur_len:
                cur_len += length
            else:
                merged.append((cur_dev, cur_addr, cur_len))
                cur_dev, cur_addr, cur_len = dev, addr, length
        merged.append((cur_dev, cur_addr, cur_len))
        return merged
