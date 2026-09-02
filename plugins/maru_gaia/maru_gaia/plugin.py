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
import itertools
import os
import subprocess
import threading
import time
from typing import TYPE_CHECKING

import pyxif

from maru_handler import StageResult

if TYPE_CHECKING:
    from maru_common import BatchLookupKVResponse
    from maru_handler import MaruHandler

logger = logging.getLogger(__name__)


# 고정에 쓸 예산을 장치 상한의 몇 할까지 잡을지. 상한에 딱 붙이면 다른 프로세스나
# 아직 안 풀린 고정과 겹쳐 거절이 나므로 한 뼘 남긴다.
_PIN_BUDGET_FRACTION = 0.9
# 고정 호출 하나의 크기 상한 기본값 (MARU_GAIA_STAGE_SPLIT_BYTES 미지정 시).
_PIN_SPLIT_DEFAULT = 16 << 20
_XCENA_CLI = "xcena_cli"


def _read_pin_threshold(device_id: "int | None", *, cli: str = _XCENA_CLI) -> int:
    """장치가 허용하는 고정 총량을 바이트로 읽는다 (못 읽으면 0).

    ``xcena_cli im get-smart <id> -v`` 의 ``pin_threshold`` 줄을 쓴다. pyxif 의
    ``get_device_info`` 는 이 값을 노출하지 않는다. 실패는 조용히 0 을 돌려주고
    호출자가 예산 없음으로 이어가게 한다 — 이 값을 못 읽는다고 적재를 막을 일은
    아니기 때문이다.
    """
    if device_id is None:
        return 0
    try:
        out = subprocess.run(
            [cli, "im", "get-smart", str(device_id), "-v"],
            capture_output=True, text=True, timeout=20.0,
        )
    except (OSError, subprocess.TimeoutExpired):
        return 0
    if out.returncode != 0:
        return 0
    for line in (out.stdout or "").splitlines():
        parts = line.split(":")
        if len(parts) == 2 and parts[0].strip() == "pin_threshold":
            try:
                return int(parts[1].strip())
            except ValueError:
                return 0
    return 0


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
        # Ordered layer-major hints (prefetch_grouped) fill in consumption
        # order. The demand-read hook's whole-request hint coalesces by
        # address and would race that order, so callers that fire grouped
        # hints turn this hook off (MARU_GAIA_RETRIEVE_HINT=0).
        self._retrieve_hint = os.environ.get("MARU_GAIA_RETRIEVE_HINT", "1") == "1"
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
        # Python threads keep running during a multi-hundred-ms fill.
        #
        # Pin defaults to 16 MiB when unset; prefetch_sync keeps whole ranges.
        # The default used to be 1 GiB, and at that size the engine died in
        # five of ten measured pin cells: a GPU-to-CXL write-behind copy
        # overlapping such a call failed with CUDA "unspecified launch
        # failure" (or the engine spun without progress), ending the cell
        # inside 21 requests. No cell issuing 16 MiB pins has reproduced it.
        split_env = int(os.environ.get("MARU_GAIA_STAGE_SPLIT_BYTES", "0") or 0)
        if split_env <= 0 and self._stage_pin:
            split_env = _PIN_SPLIT_DEFAULT
        self._stage_split_bytes = max(0, split_env)
        # Demand-yield (MARU_GAIA_STAGE_DEMAND_YIELD_MS, 0 = off): between
        # stage sub-calls, pause while a demand read ran within the last
        # N ms. The device serves fill commands in arrival order, so an
        # unyielding stage sub-call train makes concurrent demand fills
        # queue behind it — measured as the whole loss of the imminent
        # campaign (requests overlapping a stage window p50 334 ms vs 212 ms
        # clean; experiment note §5.7.1). Yielding moves stage work into
        # demand-idle windows; the stage stretches, which the admission
        # policy's deadline/expiry manages. STAGE_YIELD_BUDGET_MS caps the
        # cumulative pause per stage batch so a busy device cannot starve a
        # stage forever (default 2000 ms).
        # 조각 사이에 두는 무조건 간격(ms). 양보(_stage_yield_ms)와 달리 demand
        # 진행 여부를 묻지 않는다 — 장치 명령 큐를 주기적으로 비워, 도착한
        # 읽기가 기다리는 최대 시간을 채움 전체가 아니라 조각 하나로 줄이는 것이
        # 목적이다. 신호원이 없어도 성립한다.
        self._stage_pace_ms = max(
            0.0, float(os.environ.get("MARU_GAIA_STAGE_PACE_MS", "0") or 0.0)
        )
        # 한 방 발사(MARU_GAIA_STAGE_ASYNC=1). 한 턴 몫을 코얼레스된 범위 그대로
        # 비동기 memory_prefetch 로 제출하고 완료를 기다리지 않는다. 제출만 하므로
        # 호스트가 GIL 을 붙드는 시간이 사라지고, 조각 분할·조각 간 간격·양보가
        # 모두 필요 없어진다. 이 모드에서 StageResult.ready 는 「적재 완료」가
        # 아니라 「발사 완료」를 뜻하고, 도착한 요청은 티켓을 기다리지 않는다.
        # 대신 도착 시점에 적재가 덜 끝났으면 그만큼을 장치에서 읽는다.
        self._stage_async = os.environ.get("MARU_GAIA_STAGE_ASYNC", "0") == "1"
        # 적재 직후 재확인(MARU_GAIA_STAGE_REPROBE=1, 측정 전용). 적재가 끝난
        # 직후에 같은 범위를 한 번 더 memory_prefetch_sync 로 부르고 그 시간을
        # 로그에 남긴다. 이미 장치 DRAM 에 있으면 그 호출은 거의 즉시 돌아오고
        # (접두사가 전부 상주한 셀에서 16.2 ms 로 관측), 없으면 다시 전송
        # 시간만큼 걸린다. 곧 「적재가 데이터를 상주시켰는가」를 시간으로 묻는
        # 유일한 경로다 — pyxif 에 상주 여부를 묻는 호출이 없다.
        #
        # 값은 표본 간격이다 — 0 은 끔, N 은 N 번째 적재마다 한 번. 매 적재마다
        # 걸면 장치의 적재 부하가 두 배가 되어 파이프라인 자체가 흔들리므로,
        # 기준선을 유지한 채 재려면 띄엄띄엄 재야 한다.
        #
        # 주의: 재확인 자체가 적재이므로 재확인이 걸린 턴의 밀려남은 기준선과
        # 비교할 수 없다. 읽어야 하는 값은 reprobe_ms 뿐이다.
        self._stage_reprobe_every = max(
            0, int(os.environ.get("MARU_GAIA_STAGE_REPROBE", "0") or 0)
        )
        self._stage_reprobe_seq = itertools.count()
        # 상주 지도(MARU_GAIA_MAP_EVERY=N, 측정 전용). 요구 읽기 경계에서 접두사를
        # 16 MiB 조각으로 쪼개 조각마다 memory_prefetch_sync 를 걸고 각 호출에
        # 걸린 시간을 기록한다. 상주한 조각은 0.1 ms 안에, 없는 조각은 조각 하나를
        # 전송하는 시간(16 MiB / 약 11 GB/s = 1.5 ms)만큼 걸리므로, 시간의 자리가
        # 곧 「접두사 안에서 어디가 비어 있는지」의 지도가 된다.
        #
        # 주의: 지도를 뜨는 것 자체가 없는 조각을 채우므로, 지도가 걸린 요청의
        # 읽기 시간과 첫 토큰은 그 셀의 다른 요청과 비교할 수 없다. N 번째 요청에만
        # 걸어 나머지를 온전히 남긴다.
        self._map_every = max(0, int(os.environ.get("MARU_GAIA_MAP_EVERY", "0") or 0))
        self._map_chunk = max(
            1, int(os.environ.get("MARU_GAIA_MAP_CHUNK_BYTES", str(16 << 20)) or 1))
        # 상주/부재를 가르는 컷. 위 두 값 사이가 한 자리 이상 벌어지므로 넉넉하다.
        self._map_thr_ms = float(os.environ.get("MARU_GAIA_MAP_THRESHOLD_MS", "0.5"))
        self._map_seq = itertools.count()
        # 발사만 하는 모드에서는 완료를 기다릴 대상이 없으므로 고정 상주(pin)와
        # 섞어 쓸 수 없다. pin 은 완료 시점에 돌아오는 호출이기 때문이다.
        if self._stage_async and self._stage_pin:
            logger.warning(
                "gaia_prefetch: STAGE_ASYNC=1 overrides STAGE_PIN=1 (pin blocks)"
            )
            self._stage_pin = False
        self._stage_yield_ms = max(
            0.0,
            float(os.environ.get("MARU_GAIA_STAGE_DEMAND_YIELD_MS", "0") or 0),
        )
        self._stage_yield_budget_ms = max(
            0.0,
            float(os.environ.get("MARU_GAIA_STAGE_YIELD_BUDGET_MS", "2000") or 0),
        )
        self._last_demand_at = 0.0
        self._stage_yield_us = 0
        self._stage_yield_budget_hits = 0
        self._stage_probe_checks = 0
        self._stage_probe_hits = 0
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
        # 예산을 안 주면 장치가 알려 주는 상한에서 잡는다. 상한은 캐시 용량의
        # 절반이 기본값이고 최대가 캐시 − 8 GiB 여서, 미리 채워 둘 자리 수만 보고
        # 고정하면 접두사가 자라는 순간 상한을 넘는다 (자리 4 개 × 접두사 최대
        # 1.91 GiB = 7.62 GiB 대 상한 8 GiB 에서 거절 855 건). 호스트가 세는 수와
        # 장치가 두는 선을 맞춰 둔다.
        if self._stage_pin and self._pin_budget_bytes == 0:
            ceiling = _read_pin_threshold(self._device_id)
            if ceiling:
                self._pin_budget_bytes = int(ceiling * _PIN_BUDGET_FRACTION)
                logger.info(
                    "gaia_prefetch: pin budget from device ceiling "
                    "(device=%s, ceiling=%d B, budget=%d B)",
                    self._device_id,
                    ceiling,
                    self._pin_budget_bytes,
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
            "read_gate=%s, stage_api=%s, stage_yield_ms=%s)",
            "on" if self._coalesce else "off",
            self._device_id if self._device_id is not None else "auto-scan",
            "sync" if self._sync_gate else "async",
            "memory_prefetch (발사만)"
            if self._stage_async
            else ("memory_pin" if self._stage_pin else "memory_prefetch_sync"),
            self._stage_yield_ms or "off",
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
        # Demand-activity mark for the stage yield loop — the demand fill the
        # stage must not queue-block starts here, whether or not this seam
        # goes on to issue a hint of its own.
        self._last_demand_at = time.monotonic()
        if self._map_every and next(self._map_seq) % self._map_every == 0:
            self._residency_map(handler, batch_resp)
        if self._hymcache_local or not self._retrieve_hint:
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

    def on_prefetch_grouped(
        self,
        handler: MaruHandler,
        groups: list[list[tuple[str, int | None, int | None]]],
        entries: dict,
    ) -> None:
        """Ordered lookahead over key byte ranges, group by group.

        Groups are issued strictly in list order and coalescing folds only
        list-adjacent contiguous ranges, so the device's fill follows the
        caller's consumption order (its command queue is FIFO) instead of the
        address order the sorted coalesce imposes. A triple's offset/length
        select a byte range inside the key's object; ``None`` means whole.
        Always asynchronous — this is a lookahead, never a gate.
        """
        t0 = time.monotonic()
        commands = 0
        issued_bytes = 0
        skipped = 0
        group_count = 0
        # A request's thousands of keys live in a handful of regions, and
        # mapped-ness plus the device id are region properties — resolving
        # them once per region instead of once per triple is what keeps this
        # loop off the loader thread's critical path (measured: ~6 us per
        # triple resolve, ~23 ms at 3,776 layerwise keys).
        region_device: dict[int, int | None] = {}
        for group in groups:
            eligible: list[tuple[int, int, int]] = []
            for key, offset, length in group:
                entry = entries.get(key)
                if entry is None or not entry.found or entry.handle is None:
                    skipped += 1
                    continue
                region_id = entry.handle.region_id
                if region_id not in region_device:
                    region_device[region_id] = (
                        self._resolve_device_id(handler, region_id)
                        if handler.is_region_mapped(region_id)
                        else None
                    )
                device_id = region_device[region_id]
                if device_id is None:
                    skipped += 1
                    continue
                base = entry.handle.offset + entry.kv_offset
                start = base + (offset or 0)
                size = entry.kv_length if length is None else length
                # Clamp to the object so a caller's stale layout math can
                # never hint past the allocation.
                size = max(0, min(size, entry.kv_length - (offset or 0)))
                if size == 0:
                    skipped += 1
                    continue
                eligible.append((device_id, start, size))
            if not eligible:
                continue
            group_count += 1
            ranges = self._coalesce_ordered(eligible) if self._coalesce else eligible
            for device_id, device_addr, size in ranges:
                try:
                    status = pyxif.memory_prefetch(device_id, device_addr, size)
                except Exception:
                    self._failed += 1
                    logger.warning(
                        "gaia grouped prefetch raised: device=%d addr=0x%x size=%d",
                        device_id,
                        device_addr,
                        size,
                        exc_info=True,
                    )
                    continue
                if status != pyxif.MemoryStatus.Success:
                    self._failed += 1
                    continue
                self._issued += 1
                commands += 1
                issued_bytes += size
        self._skipped += skipped
        logger.info(
            "gaia_prefetch(grouped): groups=%d, cmds=%d, bytes=%d, skipped=%d, "
            "issue_ms=%.2f",
            group_count,
            commands,
            issued_bytes,
            skipped,
            (time.monotonic() - t0) * 1000.0,
        )

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
        if self._stage_async:
            result = self._issue(
                handler,
                keys,
                batch_resp,
                source="stage",
                sync=False,
                apply_sync_budget=False,
                pin=False,
                fire_and_forget=True,
            )
        else:
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
        # 고정 임대가 실제로 걸렸는지는 이 카운터로만 알 수 있는데, 여기까지는
        # contribute_stats 로만 나가 아무 로그에도 실리지 않았다. 그래서 STAGE_PIN=1
        # 로 돌린 옛 잡들이 「고정이 효과 없다」인지 「고정이 안 걸렸다」인지 사후에
        # 가릴 수 없었다. 실행 끝에 한 줄 남긴다.
        logger.info(
            "gaia_stage_pin summary: pin=%s asked=%d unpinned=%d unpin_failed=%d "
            "degraded=%d leftover_batches=%d split_bytes=%d budget_bytes=%d",
            self._stage_pin,
            self._stage_pinned_ranges,
            self._stage_unpinned_ranges,
            self._stage_unpin_failed,
            self._stage_pin_degraded,
            len(leftover),
            self._stage_split_bytes,
            self._pin_budget_bytes,
        )

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
            "stage_yield_ms": self._stage_yield_ms,
            "stage_yield_wait_ms": round(self._stage_yield_us / 1000.0, 1),
            "stage_yield_budget_hits": self._stage_yield_budget_hits,
            "stage_probe_checks": self._stage_probe_checks,
            "stage_probe_hits": self._stage_probe_hits,
            "stage_wait_ms": round(self._stage_wait_us / 1000.0, 1),
            "stage_pin": self._stage_pin,
            "stage_split_bytes": self._stage_split_bytes,
            "stage_async": self._stage_async,
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
        fire_and_forget: bool = False,
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
        if source == "stage" and not fire_and_forget and self._stage_split_bytes > 0:
            ranges = self._split_ranges(ranges, self._stage_split_bytes)

        issued = 0
        prepared_ranges = 0
        failed = 0
        degraded = 0
        prepared_bytes = 0
        pinned: list[tuple[int, int, int]] = []
        yielded_ms = 0.0
        probe_checks = 0
        probe_hits = 0
        t0 = time.monotonic()
        paced_ms = 0.0
        for range_index, (device_id, device_addr, size) in enumerate(ranges):
            if (
                source == "stage"
                and not fire_and_forget
                and self._stage_pace_ms > 0
                and range_index > 0
            ):
                # 요청한 값이 아니라 **실제로 쉰 시간**을 더한다. sub-ms 는
                # 스케줄러 해상도 때문에 요청보다 길게 잘 수 있어, 요청값을
                # 그대로 기록하면 순 전송(= 적재 소요 − 쉼)이 왜곡된다.
                t_sleep = time.monotonic()
                time.sleep(self._stage_pace_ms / 1000.0)
                paced_ms += (time.monotonic() - t_sleep) * 1000.0
            if source == "stage" and not fire_and_forget and self._stage_yield_ms > 0:
                waited_ms, checks, hits = self._yield_to_demand(handler, yielded_ms)
                yielded_ms += waited_ms
                probe_checks += checks
                probe_hits += hits
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
                if gated or fire_and_forget:
                    # 발사 전용 모드에서는 제출 성공을 준비로 센다 — 기다릴 완료
                    # 신호가 없으므로 ready 의 뜻이 「발사 완료」로 바뀐다.
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
                # 고정이 거절되면 그 범위를 그냥 버리고 있었다. 고정만 못 한 것이
                # 아니라 바이트가 DRAM 으로 올라오지도 않아, 읽기가 그 조각만큼
                # SSD 로 내려갔다 (거절 855 건이 난 셀에서 접두사의 5.7 % 가 그렇게
                # 비었다). 축출 가능한 적재로 한 번 되돌아가 적어도 상주는 남긴다.
                if pin_this:
                    try:
                        fb = pyxif.memory_prefetch_sync(device_id, device_addr, size)
                    except Exception:
                        fb = None
                        logger.warning(
                            "gaia_prefetch fallback raised: device=%d addr=0x%x size=%d",
                            device_id,
                            device_addr,
                            size,
                            exc_info=True,
                        )
                    if fb == pyxif.MemoryStatus.Success:
                        self._stage_pin_degraded += 1
                        issued += 1
                        failed -= 1
                        if gated or fire_and_forget:
                            prepared_ranges += 1
                            prepared_bytes += size
        wait_us = int((time.monotonic() - t0) * 1e6) if sync else 0
        reprobe_ms = -1.0
        if (
            self._stage_reprobe_every
            and next(self._stage_reprobe_seq) % self._stage_reprobe_every == 0
            and sync
            and source == "stage"
            and not fire_and_forget
            and ranges
        ):
            t_rp = time.monotonic()
            for device_id, device_addr, size in ranges:
                try:
                    pyxif.memory_prefetch_sync(device_id, device_addr, size)
                except Exception:
                    logger.warning(
                        "gaia stage reprobe raised: device=%d addr=0x%x size=%d",
                        device_id,
                        device_addr,
                        size,
                        exc_info=True,
                    )
            reprobe_ms = (time.monotonic() - t_rp) * 1000.0
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
                    "over_budget=%d, paced_ms=%.1f, reprobe_ms=%.1f",
                    source,
                    chunk_count,
                    len(ranges),
                    issued,
                    failed,
                    skipped,
                    wait_us / 1000.0,
                    degraded,
                    paced_ms,
                    reprobe_ms,
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
        self._stage_yield_us += int(yielded_ms * 1000)
        self._stage_probe_checks += probe_checks
        self._stage_probe_hits += probe_hits
        result = StageResult(
            requested_keys=len(keys),
            found_keys=found,
            eligible_keys=chunk_count,
            prepared_bytes=prepared_bytes,
            issued_ranges=prepared_ranges,
            failed_ranges=failed,
            skipped_keys=skipped,
            wait_ms=wait_us / 1000.0,
            yielded_ms=yielded_ms,
            probe_checks=probe_checks,
            probe_hits=probe_hits,
        )
        if sync and source == "stage":
            logger.info(
                "gaia_stage: ready=%s, keys=%d/%d, bytes=%d, wait_ms=%.1f, "
                "yielded_ms=%.1f",
                result.ready,
                result.eligible_keys,
                result.requested_keys,
                result.prepared_bytes,
                result.wait_ms,
                result.yielded_ms,
            )
        return result

    def _yield_to_demand(
        self, handler: MaruHandler, yielded_so_far_ms: float
    ) -> tuple[float, int, int]:
        """Pause while a demand read is recent or in flight.

        Returns:
            ``(waited_ms, probe_checks, probe_hits)`` — the pause plus the
            wiring echo: how many times the in-flight probe was consulted and
            how many consultations saw an active demand read. A zero check
            count in a run's stage lines means the probe is not wired at all.

        Two signals gate the stage sub-call train (hybrid — either pauses):

        - **Recent start**: a demand read seam fired within the last
          ``_stage_yield_ms`` — covers the phase before the read's copy batch
          (and its completion event) exists.
        - **In flight**: ``handler.demand_active()`` — the serving
          integration's live probe over scheduled copy batches. This closes
          the recent-start rule's blind side: a demand fill that started
          longer than the window ago but is still running (measured to
          collide with the stage's transfer, experiment note §5.7.3).

        The pause ends when both signals clear or the batch's cumulative
        yield budget (``_stage_yield_budget_ms``) is spent, whichever comes
        first. The budget is the starvation guard: on a device that is never
        quiet the stage proceeds anyway and the admission policy's deadline
        decides whether the result still matters.
        """
        demand_active = getattr(handler, "demand_active", None)
        waited_ms = 0.0
        checks = 0
        hits = 0
        while True:
            quiet_ms = (time.monotonic() - self._last_demand_at) * 1000.0
            if quiet_ms >= self._stage_yield_ms:
                in_flight = False
                if demand_active is not None:
                    checks += 1
                    try:
                        in_flight = bool(demand_active())
                    except Exception:
                        in_flight = False  # fail open — never block staging
                if in_flight:
                    hits += 1
                else:
                    return waited_ms, checks, hits
            if yielded_so_far_ms + waited_ms >= self._stage_yield_budget_ms:
                self._stage_yield_budget_hits += 1
                return waited_ms, checks, hits
            pause_s = 0.002
            time.sleep(pause_s)
            waited_ms += pause_s * 1000.0

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

    def _residency_map(
        self, handler: MaruHandler, batch_resp: BatchLookupKVResponse
    ) -> None:
        """Time a per-chunk prefetch over the prefix and log where it is absent.

        Splits the prefix into ``MARU_GAIA_MAP_CHUNK_BYTES`` pieces, issues one
        blocking ``memory_prefetch_sync`` per piece, and classifies each piece
        by how long that call took: a resident piece returns within
        ``MARU_GAIA_MAP_THRESHOLD_MS``, an absent one takes as long as
        transferring the piece. The logged map preserves prefix order, so the
        pattern of absences is readable directly.

        Measurement only: the probe itself fills the absent pieces, so the
        read and first-token times of a probed request are not comparable to
        the cell's other requests.
        """
        eligible: list[tuple[int, int, int]] = []
        for entry in batch_resp.entries:
            if not entry.found or entry.handle is None:
                continue
            region_id = entry.handle.region_id
            if not handler.is_region_mapped(region_id):
                continue
            device_id = self._resolve_device_id(handler, region_id)
            if device_id is None:
                continue
            eligible.append(
                (device_id, entry.handle.offset + entry.kv_offset, entry.kv_length)
            )
        if not eligible:
            return
        ranges = self._split_ranges(
            self._coalesce_ordered(eligible) if self._coalesce else eligible,
            self._map_chunk,
        )
        marks: list[str] = []
        absent_ms = 0.0
        total_ms = 0.0
        for device_id, device_addr, size in ranges:
            t = time.monotonic()
            try:
                pyxif.memory_prefetch_sync(device_id, device_addr, size)
            except Exception:
                marks.append("?")
                continue
            took = (time.monotonic() - t) * 1000.0
            total_ms += took
            if took >= self._map_thr_ms:
                marks.append("#")
                absent_ms += took
            else:
                marks.append(".")
        absent = sum(1 for m in marks if m == "#")
        logger.info(
            "gaia_resmap: chunks=%d, absent=%d (%.1f%%), thr=%.2fms, "
            "probe_ms=%.1f, absent_ms=%.1f, map=%s",
            len(marks),
            absent,
            100.0 * absent / len(marks) if marks else 0.0,
            self._map_thr_ms,
            total_ms,
            absent_ms,
            "".join(marks),
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
    def _coalesce_ordered(
        chunks: list[tuple[int, int, int]],
    ) -> list[tuple[int, int, int]]:
        """Merge contiguous ranges without reordering them.

        Folds an entry into its list predecessor only when they are contiguous
        on the same device. Unlike :meth:`_coalesce_ranges` this never sorts,
        so the issue order — and therefore the device's fill order — stays the
        caller's consumption order.
        """
        merged: list[tuple[int, int, int]] = []
        for dev, addr, length in chunks:
            if merged:
                pdev, paddr, plen = merged[-1]
                if pdev == dev and paddr + plen == addr:
                    merged[-1] = (pdev, paddr, plen + length)
                    continue
            merged.append((dev, addr, length))
        return merged

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
