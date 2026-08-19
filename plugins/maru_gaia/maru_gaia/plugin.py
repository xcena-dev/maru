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
import threading
import time
from typing import TYPE_CHECKING

import pyxif

from maru_handler import StageResult

if TYPE_CHECKING:
    from maru_common import BatchLookupKVResponse
    from maru_handler import MaruHandler

logger = logging.getLogger(__name__)


class GaiaPrefetchPlugin:
    """Issues gaia device prefetch hints at MaruHandler's retrieve seams."""

    def __init__(self) -> None:
        self._coalesce = os.environ.get("MARU_GAIA_PREFETCH_COALESCE", "1") == "1"
        # Read gate (MARU_GAIA_PREFETCH_SYNC=1): at the demand-read boundary
        # (on_batch_retrieve) block until the range is device-DRAM resident via
        # memory_prefetch_sync instead of the fire-and-forget async hint. The
        # arrival lookahead (on_prefetch) stays async regardless — a sync call
        # there would block and defeat the overlap with admission wait.
        self._sync_gate = os.environ.get("MARU_GAIA_PREFETCH_SYNC", "0") == "1"
        # HyMCache-local mode stages explicit request windows. The ordinary
        # on_batch_retrieve hook would otherwise fire one whole-request
        # reactive hint before the bounded pipeline starts and contaminate the
        # transport-substitution experiment.
        self._hymcache_local = (
            int(os.environ.get("MARU_HYMCACHE_WINDOW_BYTES", "0") or 0) > 0
        )
        # Upper bound on how long one read gate may block
        # (MARU_GAIA_PREFETCH_SYNC_BUDGET_MS, 0 = unbounded). Ranges left when
        # the budget runs out are hinted asynchronously instead.
        self._sync_budget_ms = max(
            0.0, float(os.environ.get("MARU_GAIA_PREFETCH_SYNC_BUDGET_MS", "0") or 0)
        )
        # The experiment always names the single gaia device via
        # MARU_GAIA_DEVICE_ID (same env naru/gaia-bench use to set DRAM
        # capacity). Prefer it: pyxif's auto-enumeration (get_device_list) is
        # not reliable across SDK builds — it returns [] on the current host,
        # which silently made every hint a no-op. The dax-path scan is kept
        # only as a multi-device fallback for when the env is unset.
        dev_env = os.environ.get("MARU_GAIA_DEVICE_ID")
        self._device_id: int | None = int(dev_env) if dev_env else None
        self._gaia_devices: dict[int, bool] = {}
        # dax device path -> pyxif device id, built lazily on first hint (the
        # scan is not free and a handler may never prefetch). Fallback only.
        self._dax_to_device: dict[str, int] | None = None
        # Stage pin lease (MARU_GAIA_STAGE_PIN=1): on_stage materializes via
        # memory_pin instead of memory_prefetch_sync, so the staged bytes stay
        # DRAM-resident until on_stage_release unpins them (the READY ->
        # consume protection window). Pins are tracked per key batch so the
        # release and the on_close leak guard unpin exactly what was taken.
        self._stage_pin = os.environ.get("MARU_GAIA_STAGE_PIN", "0") == "1"
        # Issue stage calls in sub-ranges of at most this many bytes
        # (MARU_GAIA_STAGE_SPLIT_BYTES, 0 = whole coalesced ranges). Bounded
        # calls serve two purposes: very large single pins can time out the
        # firmware ioctl, and the pyxif binding holds the GIL for the whole
        # blocking call — sub-ranges open GIL windows so the caller process's
        # Python threads keep running during a multi-hundred-ms fill. Pin
        # defaults to 1 GiB when unset; prefetch_sync keeps whole ranges.
        split_env = int(os.environ.get("MARU_GAIA_STAGE_SPLIT_BYTES", "0") or 0)
        if split_env <= 0 and self._stage_pin:
            split_env = 1 << 30
        self._stage_split_bytes = max(0, split_env)
        # Pin admission budget (MARU_GAIA_PIN_BUDGET_BYTES, 0 = unlimited).
        # The firmware pin capacity is bounded (~half the DRAM cache) and an
        # over-capacity pin ioctl observed on-device blocks ~61 s per call
        # before failing — freezing the caller process via the GIL. Pieces
        # that would exceed the budget are therefore never pinned; they
        # degrade to memory_prefetch_sync (resident but evictable), keeping
        # the stage's readiness contract without touching the pin limit.
        self._pin_budget_bytes = max(
            0, int(os.environ.get("MARU_GAIA_PIN_BUDGET_BYTES", "0") or 0)
        )
        self._pin_lock = threading.Lock()
        self._pinned: dict[tuple[str, ...], list[tuple[int, int, int]]] = {}
        self._pinned_bytes = 0
        self._stage_pinned_ranges = 0
        self._stage_unpinned_ranges = 0
        self._stage_unpin_failed = 0
        self._stage_pin_degraded = 0
        # Cumulative counters surfaced via contribute_stats.
        self._issued = 0
        self._failed = 0
        self._skipped = 0
        self._sync_wait_us = 0  # cumulative sync read-gate block time
        self._stage_requests = 0
        self._stage_ready = 0
        self._stage_bytes = 0
        self._stage_wait_us = 0
        logger.info(
            "Gaia prefetch plugin loaded (coalesce=%s, device_id=%s, "
            "read_gate=%s, stage_api=%s)",
            "on" if self._coalesce else "off",
            self._device_id if self._device_id is not None else "auto-scan",
            "sync" if self._sync_gate else "async",
            "memory_pin" if self._stage_pin else "memory_prefetch_sync",
        )

    # -- MaruHandlerPlugin seams -------------------------------------------

    def on_batch_retrieve(
        self,
        handler: MaruHandler,
        keys: list[str],
        batch_resp: BatchLookupKVResponse,
    ) -> None:
        """Demand-read boundary. With MARU_GAIA_PREFETCH_SYNC=1 this is a sync
        read-gate (block until DRAM-resident); otherwise the async reactive
        hint (baseline)."""
        if self._hymcache_local:
            return
        self._issue(
            handler,
            keys,
            batch_resp,
            source="retrieve",
            sync=self._sync_gate,
        )

    def on_prefetch(
        self,
        handler: MaruHandler,
        keys: list[str],
        batch_resp: BatchLookupKVResponse,
    ) -> None:
        """Lookahead hint fired ahead of demand by arrival-hint — always async
        (a sync call here would block and defeat the overlap with admission
        wait)."""
        self._issue(handler, keys, batch_resp, source="prefetch", sync=False)

    def on_stage(
        self,
        handler: MaruHandler,
        keys: list[str],
        batch_resp: BatchLookupKVResponse,
    ) -> StageResult:
        """Block on SSD-to-device-DRAM materialization for a stage worker.

        ``MaruHandler.stage_batch`` invokes this hook only from a dedicated
        executor. Unlike the demand read-gate, no wall-time budget degrades
        remaining ranges to asynchronous hints: returning is the readiness
        contract consumed by the request's ``StageTicket``.
        """
        result = self._issue(
            handler,
            keys,
            batch_resp,
            source="stage",
            sync=True,
            apply_sync_budget=False,
            pin=self._stage_pin,
        )
        self._stage_requests += 1
        self._stage_ready += int(result.ready)
        self._stage_bytes += result.prepared_bytes
        self._stage_wait_us += int(result.wait_ms * 1000.0)
        return result

    def on_stage_release(
        self,
        handler: MaruHandler,
        keys: list[str],
    ) -> None:
        """Unpin the device ranges a prior pinned stage of ``keys`` holds.

        Idempotent: a batch that never pinned (pin lease off, stage failed
        before any pin, or already released) is a cheap dictionary miss.
        """
        with self._pin_lock:
            ranges = self._pinned.pop(tuple(keys), None)
        if ranges:
            self._unpin_ranges(ranges)

    def on_close(self, handler: MaruHandler) -> None:
        """Leak guard: unpin every range still held when the handler closes."""
        with self._pin_lock:
            leftover = list(self._pinned.values())
            self._pinned.clear()
        for ranges in leftover:
            self._unpin_ranges(ranges)
        with self._pin_lock:
            self._pinned_bytes = 0

    def contribute_stats(self) -> dict:
        """Cumulative prefetch counters for ``MaruHandler.get_stats``."""
        return {
            "issued": self._issued,
            "failed": self._failed,
            "skipped": self._skipped,
            "coalesce": self._coalesce,
            "read_gate": "sync" if self._sync_gate else "async",
            "sync_gate_wait_ms": round(self._sync_wait_us / 1000.0, 1),
            "stage_requests": self._stage_requests,
            "stage_ready": self._stage_ready,
            "stage_bytes": self._stage_bytes,
            "stage_wait_ms": round(self._stage_wait_us / 1000.0, 1),
            "stage_pin": self._stage_pin,
            "stage_split_bytes": self._stage_split_bytes,
            "stage_pinned_ranges": self._stage_pinned_ranges,
            "stage_unpinned_ranges": self._stage_unpinned_ranges,
            "stage_unpin_failed": self._stage_unpin_failed,
            "stage_pin_degraded": self._stage_pin_degraded,
            "pin_budget_bytes": self._pin_budget_bytes,
            "pinned_bytes": self._pinned_bytes,
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
            return self._device_id if self._is_gaia_device(self._device_id) else None
        dax_path = handler.get_region_dax_path(region_id)
        if dax_path is None:
            return None
        return self._device_map().get(dax_path)

    def _device_map(self) -> dict[str, int]:
        """Return the dax-path -> device-id map, built once via pyxif (fallback)."""
        if self._dax_to_device is None:
            dax_map: dict[str, int] = {}
            for device_id in pyxif.get_device_list():
                if not self._is_gaia_device(device_id):
                    continue
                for region in pyxif.cxl_get_regions(device_id):
                    dax_map[region.dax_device] = device_id
            self._dax_to_device = dax_map
            logger.info("gaia_prefetch: dax_to_device_map=%s", dax_map)
        return self._dax_to_device

    def _is_gaia_device(self, device_id: int) -> bool:
        """Return whether ``device_id`` implements InfiniteMemory commands."""
        if device_id in self._gaia_devices:
            return self._gaia_devices[device_id]
        try:
            info = pyxif.get_device_info(device_id)
            enabled = info is not None and bool(info.gaia_enabled())
        except Exception:
            enabled = False
            logger.warning(
                "gaia_prefetch: failed to validate device %d; ignoring it",
                device_id,
                exc_info=True,
            )
        self._gaia_devices[device_id] = enabled
        if not enabled:
            logger.error(
                "gaia_prefetch: device %d is not gaia-enabled; no InfiniteMemory "
                "command will be issued",
                device_id,
            )
        return enabled

    def _issue(
        self,
        handler: MaruHandler,
        keys: list[str],
        batch_resp: BatchLookupKVResponse,
        source: str,
        sync: bool = False,
        apply_sync_budget: bool = True,
        pin: bool = False,
    ) -> StageResult:
        """Resolve mapped found entries to device ranges and prefetch them.

        Skips entries that are not found, carry no handle, or whose region is
        not currently mapped (only a mapped region has a live address to hint
        against). Contiguous ranges on the same device are merged when
        coalescing is enabled before dispatch.

        With ``sync`` the ranges go through ``memory_prefetch_sync``, which
        returns only once the range is device-DRAM resident (a read gate). The
        total block time is the fill the caller's lead did not hide, logged and
        accumulated as the direct signal of lookahead benefit. Without ``sync``
        the async ``memory_prefetch`` returns on submission (no completion).
        """
        eligible: list[tuple[int, int, int]] = []
        skipped = 0
        for entry in batch_resp.entries:
            if not entry.found:
                continue
            if entry.handle is None:
                skipped += 1
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
        if source == "stage" and self._stage_split_bytes > 0:
            ranges = self._split_ranges(ranges, self._stage_split_bytes)

        issued = 0
        prepared_ranges = 0
        failed = 0
        degraded = 0
        prepared_bytes = 0
        pinned: list[tuple[int, int, int]] = []
        t0 = time.monotonic()
        for device_id, device_addr, size in ranges:
            # The read gate blocks the single deferred-load thread, so a cold
            # gate does not just delay this request — it delays every request
            # queued behind it. Spend at most the budget blocking, then finish
            # the batch asynchronously and let the transfer read what it finds.
            gated = sync
            if gated and apply_sync_budget and self._sync_budget_ms > 0:
                spent_ms = (time.monotonic() - t0) * 1000.0
                if spent_ms >= self._sync_budget_ms:
                    gated = False
                    degraded += 1
            pin_this = pin and gated
            if pin_this and self._pin_budget_bytes > 0:
                with self._pin_lock:
                    over_budget = self._pinned_bytes + size > self._pin_budget_bytes
                if over_budget:
                    # Never issue an over-budget pin (the ioctl can block for
                    # a minute per call); keep readiness via the evictable
                    # sync fill instead.
                    pin_this = False
                    self._stage_pin_degraded += 1
            if pin_this:
                # memory_pin returns at DRAM-load completion AND protects the
                # range from eviction until the matching unpin — the stage's
                # residency lease.
                prefetch_fn = pyxif.memory_pin
            elif gated:
                prefetch_fn = pyxif.memory_prefetch_sync
            else:
                prefetch_fn = pyxif.memory_prefetch
            try:
                status = prefetch_fn(device_id, device_addr, size)
            except Exception:
                failed += 1
                logger.warning(
                    "gaia_prefetch raised: device=%d addr=0x%x size=%d sync=%s pin=%s",
                    device_id,
                    device_addr,
                    size,
                    sync,
                    pin,
                    exc_info=True,
                )
                continue
            if status == pyxif.MemoryStatus.Success:
                issued += 1
                if gated:
                    prepared_ranges += 1
                    prepared_bytes += size
                    if pin_this:
                        pinned.append((device_id, device_addr, size))
                        with self._pin_lock:
                            self._pinned_bytes += size
            else:
                failed += 1
                logger.warning(
                    "gaia_prefetch failed: device=%d addr=0x%x size=%d status=%s "
                    "sync=%s pin=%s",
                    device_id,
                    device_addr,
                    size,
                    status,
                    sync,
                    pin,
                )
        wait_us = int((time.monotonic() - t0) * 1e6) if sync else 0
        if pinned:
            batch = tuple(keys)
            with self._pin_lock:
                # A re-stage of a still-held batch replaces its lease; unpin
                # the old ranges outside the lock below.
                previous = self._pinned.pop(batch, None)
                self._pinned[batch] = pinned
            self._stage_pinned_ranges += len(pinned)
            if previous:
                self._unpin_ranges(previous)

        self._issued += issued
        self._failed += failed
        self._skipped += skipped
        self._sync_wait_us += wait_us
        if issued or failed:
            if sync:
                logger.info(
                    "gaia_prefetch_sync(%s): chunks=%d, ranges=%d "
                    "(issued=%d, failed=%d), skipped=%d, gate_wait_ms=%.1f, "
                    "over_budget=%d",
                    source,
                    chunk_count,
                    len(ranges),
                    issued,
                    failed,
                    skipped,
                    wait_us / 1000.0,
                    degraded,
                )
            else:
                logger.info(
                    "gaia_prefetch(%s): chunks=%d, ranges=%d "
                    "(issued=%d, failed=%d), skipped=%d, coalesce=%s",
                    source,
                    chunk_count,
                    len(ranges),
                    issued,
                    failed,
                    skipped,
                    self._coalesce,
                )

        found = sum(1 for entry in batch_resp.entries if entry.found)
        result = StageResult(
            requested_keys=len(keys),
            found_keys=found,
            eligible_keys=chunk_count,
            prepared_bytes=prepared_bytes,
            issued_ranges=prepared_ranges,
            failed_ranges=failed,
            skipped_keys=skipped,
            wait_ms=wait_us / 1000.0,
        )
        if sync and source == "stage":
            logger.info(
                "gaia_stage: ready=%s, keys=%d/%d, bytes=%d, wait_ms=%.1f",
                result.ready,
                result.eligible_keys,
                result.requested_keys,
                result.prepared_bytes,
                result.wait_ms,
            )
        return result

    def _unpin_ranges(self, ranges: list[tuple[int, int, int]]) -> None:
        """Unpin previously pinned ranges, counting (not raising) failures."""
        for device_id, device_addr, size in ranges:
            try:
                status = pyxif.memory_unpin(device_id, device_addr, size)
            except Exception:
                self._stage_unpin_failed += 1
                logger.warning(
                    "gaia_stage unpin raised: device=%d addr=0x%x size=%d",
                    device_id,
                    device_addr,
                    size,
                    exc_info=True,
                )
                continue
            if status == pyxif.MemoryStatus.Success:
                self._stage_unpinned_ranges += 1
                with self._pin_lock:
                    self._pinned_bytes = max(0, self._pinned_bytes - size)
            else:
                self._stage_unpin_failed += 1
                logger.warning(
                    "gaia_stage unpin failed: device=%d addr=0x%x size=%d status=%s",
                    device_id,
                    device_addr,
                    size,
                    status,
                )

    @staticmethod
    def _split_ranges(
        ranges: list[tuple[int, int, int]],
        max_bytes: int,
    ) -> list[tuple[int, int, int]]:
        """Split ``(device_id, addr, size)`` ranges into ≤ ``max_bytes`` pieces."""
        pieces: list[tuple[int, int, int]] = []
        for device_id, addr, size in ranges:
            offset = 0
            while offset < size:
                piece = min(max_bytes, size - offset)
                pieces.append((device_id, addr + offset, piece))
                offset += piece
        return pieces

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
