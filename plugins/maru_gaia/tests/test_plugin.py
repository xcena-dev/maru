# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for the gaia prefetch plugin (device address math + coalesce).

``pyxif`` is replaced by a fake so the tests run without a device: they pin the
address computation (``handle.offset + kv_offset`` for ``kv_length``), range
coalescing, and the unmapped-region skip.
"""

import itertools
from types import SimpleNamespace

import pytest

# The plugin imports pyxif (the XCENA device binding) at module load, so on a
# host without the device SDK this whole suite is skipped rather than erroring
# at collection.
pytest.importorskip("pyxif")

from maru_gaia.plugin import GaiaPrefetchPlugin  # noqa: E402


class _FakePyxif:
    """Records memory_prefetch calls; one device covering one dax path."""

    MemoryStatus = SimpleNamespace(Success="SUCCESS")

    def __init__(self, dax_path: str = "/dev/dax0.0", device_id: int = 0):
        self._dax_path = dax_path
        self._device_id = device_id
        self.calls: list[tuple[int, int, int]] = []
        self.sync_calls: list[tuple[int, int, int]] = []

    def get_device_list(self):
        return [self._device_id]

    def get_device_info(self, device_id):
        return SimpleNamespace(gaia_enabled=lambda: True)

    def cxl_get_regions(self, device_id):
        return [SimpleNamespace(dax_device=self._dax_path)]

    def memory_prefetch(self, device_id, addr, size):
        self.calls.append((device_id, addr, size))
        return self.MemoryStatus.Success

    def memory_prefetch_sync(self, device_id, addr, size):
        self.sync_calls.append((device_id, addr, size))
        return self.MemoryStatus.Success


def _handler(mapped: bool = True, dax_path: str = "/dev/dax0.0") -> SimpleNamespace:
    """Stub exposing only the stable plugin API the plugin uses."""
    return SimpleNamespace(
        is_region_mapped=lambda region_id: mapped,
        get_region_dax_path=lambda region_id: dax_path,
    )


def _entry(offset: int, kv_offset: int, kv_length: int, region_id: int = 7):
    return SimpleNamespace(
        found=True,
        handle=SimpleNamespace(region_id=region_id, offset=offset),
        kv_offset=kv_offset,
        kv_length=kv_length,
    )


def _resp(*entries):
    return SimpleNamespace(entries=list(entries))


class TestCoalesce:
    def test_merges_contiguous_same_device(self):
        merged = GaiaPrefetchPlugin._coalesce_ranges(
            [(0, 0x1000, 0x100), (0, 0x1100, 0x100)]
        )
        assert merged == [(0, 0x1000, 0x200)]

    def test_keeps_gap_and_other_device_separate(self):
        merged = GaiaPrefetchPlugin._coalesce_ranges(
            [(0, 0x1000, 0x100), (0, 0x2000, 0x100), (1, 0x1100, 0x100)]
        )
        assert merged == [(0, 0x1000, 0x100), (0, 0x2000, 0x100), (1, 0x1100, 0x100)]

    def test_empty(self):
        assert GaiaPrefetchPlugin._coalesce_ranges([]) == []


class TestIssue:
    @pytest.fixture(autouse=True)
    def _scan_path(self, monkeypatch):
        """Default the issue tests to the dax-scan path (env device id unset)."""
        monkeypatch.delenv("MARU_GAIA_DEVICE_ID", raising=False)

    def test_address_is_handle_offset_plus_kv_offset(self, monkeypatch):
        """A single found+mapped entry hints handle.offset+kv_offset/kv_length."""
        fake = _FakePyxif()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.delenv("MARU_GAIA_PREFETCH_COALESCE", raising=False)
        plugin = GaiaPrefetchPlugin()

        plugin.on_prefetch(_handler(), ["k0"], _resp(_entry(0x1000, 0x200, 0x100)))

        assert fake.calls == [(0, 0x1200, 0x100)]

    def test_env_device_id_used_without_scan(self, monkeypatch):
        """MARU_GAIA_DEVICE_ID resolves the device even when pyxif enumerates
        nothing (the real-host failure mode: get_device_list() -> [])."""

        class _EmptyEnumPyxif(_FakePyxif):
            def get_device_list(self):
                return []

        fake = _EmptyEnumPyxif()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.setenv("MARU_GAIA_DEVICE_ID", "2")
        plugin = GaiaPrefetchPlugin()

        plugin.on_prefetch(_handler(), ["k0"], _resp(_entry(0x1000, 0x200, 0x100)))

        assert fake.calls == [(2, 0x1200, 0x100)]  # device 2 from env, not scan

    def test_non_gaia_env_device_is_rejected(self, monkeypatch):
        fake = _FakePyxif()
        fake.get_device_info = lambda device_id: SimpleNamespace(
            gaia_enabled=lambda: False
        )
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.setenv("MARU_GAIA_DEVICE_ID", "2")
        plugin = GaiaPrefetchPlugin()

        plugin.on_prefetch(_handler(), ["k0"], _resp(_entry(0x1000, 0x200, 0x100)))

        assert fake.calls == []
        assert plugin._skipped == 1

    def test_auto_scan_ignores_non_gaia_devices(self, monkeypatch):
        fake = _FakePyxif()
        fake.get_device_list = lambda: [0, 1]
        fake.get_device_info = lambda device_id: SimpleNamespace(
            gaia_enabled=lambda: device_id == 1
        )
        fake.cxl_get_regions = lambda device_id: [
            SimpleNamespace(dax_device=f"/dev/dax{device_id}.0")
        ]
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        plugin = GaiaPrefetchPlugin()

        plugin.on_prefetch(
            _handler(dax_path="/dev/dax0.0"),
            ["k0"],
            _resp(_entry(0x1000, 0, 0x100)),
        )
        plugin.on_prefetch(
            _handler(dax_path="/dev/dax1.0"),
            ["k1"],
            _resp(_entry(0x2000, 0, 0x100)),
        )

        assert fake.calls == [(1, 0x2000, 0x100)]

    def test_contiguous_chunks_coalesce_to_one_call(self, monkeypatch):
        fake = _FakePyxif()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.setenv("MARU_GAIA_PREFETCH_COALESCE", "1")
        plugin = GaiaPrefetchPlugin()

        plugin.on_prefetch(
            _handler(),
            ["k0", "k1"],
            _resp(_entry(0x1000, 0, 0x100), _entry(0x1000, 0x100, 0x100)),
        )

        assert fake.calls == [(0, 0x1000, 0x200)]

    def test_coalesce_off_issues_per_chunk(self, monkeypatch):
        fake = _FakePyxif()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.setenv("MARU_GAIA_PREFETCH_COALESCE", "0")
        plugin = GaiaPrefetchPlugin()

        plugin.on_prefetch(
            _handler(),
            ["k0", "k1"],
            _resp(_entry(0x1000, 0, 0x100), _entry(0x1000, 0x100, 0x100)),
        )

        assert fake.calls == [(0, 0x1000, 0x100), (0, 0x1100, 0x100)]

    def test_unmapped_region_is_skipped(self, monkeypatch):
        fake = _FakePyxif()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        plugin = GaiaPrefetchPlugin()

        plugin.on_prefetch(
            _handler(mapped=False), ["k0"], _resp(_entry(0x1000, 0, 0x100))
        )

        assert fake.calls == []
        assert plugin._skipped == 1

    def test_not_found_entry_is_ignored(self, monkeypatch):
        fake = _FakePyxif()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        plugin = GaiaPrefetchPlugin()

        miss = SimpleNamespace(found=False, handle=None, kv_offset=0, kv_length=0)
        plugin.on_prefetch(_handler(), ["k0"], _resp(miss))

        assert fake.calls == []


class TestSyncReadGate:
    """MARU_GAIA_PREFETCH_SYNC routes the read boundary through the sync API."""

    @pytest.fixture(autouse=True)
    def _clear_device_env(self, monkeypatch):
        monkeypatch.delenv("MARU_GAIA_DEVICE_ID", raising=False)

    def test_retrieve_uses_sync_when_gate_on(self, monkeypatch):
        """on_batch_retrieve → memory_prefetch_sync (block-until-resident)."""
        fake = _FakePyxif()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.setenv("MARU_GAIA_PREFETCH_SYNC", "1")
        plugin = GaiaPrefetchPlugin()

        plugin.on_batch_retrieve(_handler(), ["k0"], _resp(_entry(0x1000, 0, 0x100)))

        assert fake.sync_calls == [(0, 0x1000, 0x100)]  # went through sync path
        assert fake.calls == []  # not the async path
        assert plugin.contribute_stats()["read_gate"] == "sync"

    def test_retrieve_is_async_when_gate_off(self, monkeypatch):
        """Default: on_batch_retrieve stays async (baseline reactive hint)."""
        fake = _FakePyxif()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.delenv("MARU_GAIA_PREFETCH_SYNC", raising=False)
        plugin = GaiaPrefetchPlugin()

        plugin.on_batch_retrieve(_handler(), ["k0"], _resp(_entry(0x1000, 0, 0x100)))

        assert fake.calls == [(0, 0x1000, 0x100)]
        assert fake.sync_calls == []

    def test_arrival_lookahead_stays_async_even_with_gate_on(self, monkeypatch):
        """on_prefetch must never block — always async regardless of the gate."""
        fake = _FakePyxif()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.setenv("MARU_GAIA_PREFETCH_SYNC", "1")
        plugin = GaiaPrefetchPlugin()

        plugin.on_prefetch(_handler(), ["k0"], _resp(_entry(0x1000, 0, 0x100)))

        assert fake.calls == [(0, 0x1000, 0x100)]  # async
        assert fake.sync_calls == []

    def test_hymcache_local_skips_whole_request_reactive_hint(self, monkeypatch):
        fake = _FakePyxif()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.setenv("MARU_HYMCACHE_WINDOW_BYTES", "128")
        plugin = GaiaPrefetchPlugin()

        plugin.on_batch_retrieve(_handler(), ["k0"], _resp(_entry(0x1000, 0, 0x100)))

        assert fake.calls == []
        assert fake.sync_calls == []


class TestStage:
    """Completion-returning stage always uses the unbudgeted sync API."""

    @pytest.fixture(autouse=True)
    def _stage_env(self, monkeypatch):
        monkeypatch.delenv("MARU_GAIA_DEVICE_ID", raising=False)
        monkeypatch.setenv("MARU_GAIA_PREFETCH_COALESCE", "0")
        monkeypatch.setenv("MARU_GAIA_PREFETCH_SYNC_BUDGET_MS", "0.001")

    def test_stage_syncs_every_range_and_returns_ready(self, monkeypatch):
        fake = _FakePyxif()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        plugin = GaiaPrefetchPlugin()
        response = _resp(_entry(0, 0, 32), _entry(0, 1024, 32))

        result = plugin.on_stage(_handler(), ["a", "b"], response)

        assert len(fake.sync_calls) == 2
        assert fake.calls == []
        assert result.ready
        assert result.prepared_bytes == 64
        assert result.issued_ranges == 2
        assert plugin.contribute_stats()["stage_ready"] == 1

    def test_stage_does_not_pin_by_default(self, monkeypatch):
        """The measured smart-prefetch setting calls prefetch_sync only.

        Every 2026-08-11 campaign run reported pin/unpin deltas of 0, and the
        next comparison keeps that contract, so a default-on pin would
        silently change what the setting means.
        """
        fake = _FakePyxif()
        pin_calls: list[tuple[int, int, int]] = []
        fake.memory_pin = lambda device_id, addr, size: pin_calls.append(
            (device_id, addr, size)
        )
        monkeypatch.delenv("MARU_GAIA_STAGE_PIN", raising=False)
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        plugin = GaiaPrefetchPlugin()

        result = plugin.on_stage(
            _handler(), ["a", "b"], _resp(_entry(0, 0, 32), _entry(0, 1024, 32))
        )

        assert result.ready
        assert len(fake.sync_calls) == 2
        assert pin_calls == []

    def test_stage_miss_is_not_ready(self, monkeypatch):
        fake = _FakePyxif()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        plugin = GaiaPrefetchPlugin()
        miss = SimpleNamespace(found=False, handle=None, kv_offset=0, kv_length=0)

        result = plugin.on_stage(_handler(), ["missing"], _resp(miss))

        assert not result.ready
        assert result.found_keys == 0
        assert result.prepared_bytes == 0

    def test_stage_split_bounds_sync_call_size(self, monkeypatch):
        fake = _FakePyxif()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.setenv("MARU_GAIA_STAGE_SPLIT_BYTES", "64")
        plugin = GaiaPrefetchPlugin()
        response = _resp(_entry(0, 0, 160))

        result = plugin.on_stage(_handler(), ["a"], response)

        assert [size for (_, _, size) in fake.sync_calls] == [64, 64, 32]
        assert result.ready
        assert result.prepared_bytes == 160


class TestStagePinLease:
    """MARU_GAIA_STAGE_PIN=1: on_stage pins; release/close unpin the lease."""

    @pytest.fixture(autouse=True)
    def _pin_env(self, monkeypatch):
        monkeypatch.delenv("MARU_GAIA_DEVICE_ID", raising=False)
        monkeypatch.setenv("MARU_GAIA_PREFETCH_COALESCE", "1")
        monkeypatch.setenv("MARU_GAIA_STAGE_PIN", "1")
        # Small split so the tests exercise sub-range pins deterministically.
        monkeypatch.setenv("MARU_GAIA_STAGE_SPLIT_BYTES", "64")

    def _pin_fake(self, monkeypatch):
        fake = _FakePyxif()
        fake.pin_calls = []
        fake.unpin_calls = []

        def memory_pin(device_id, addr, size):
            fake.pin_calls.append((device_id, addr, size))
            return fake.MemoryStatus.Success

        def memory_unpin(device_id, addr, size):
            fake.unpin_calls.append((device_id, addr, size))
            return fake.MemoryStatus.Success

        fake.memory_pin = memory_pin
        fake.memory_unpin = memory_unpin
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        return fake

    def test_stage_pins_split_ranges_and_release_unpins_them(self, monkeypatch):
        fake = self._pin_fake(monkeypatch)
        plugin = GaiaPrefetchPlugin()
        response = _resp(_entry(0, 0, 96))

        result = plugin.on_stage(_handler(), ["a"], response)

        assert result.ready
        assert fake.pin_calls == [(0, 0, 64), (0, 64, 32)]
        assert fake.sync_calls == []  # pin replaces prefetch_sync

        plugin.on_stage_release(_handler(), ["a"])
        assert fake.unpin_calls == fake.pin_calls

    def test_release_is_idempotent(self, monkeypatch):
        fake = self._pin_fake(monkeypatch)
        plugin = GaiaPrefetchPlugin()
        plugin.on_stage(_handler(), ["a"], _resp(_entry(0, 0, 32)))

        plugin.on_stage_release(_handler(), ["a"])
        plugin.on_stage_release(_handler(), ["a"])

        assert len(fake.unpin_calls) == 1

    def test_release_of_unknown_batch_is_noop(self, monkeypatch):
        fake = self._pin_fake(monkeypatch)
        plugin = GaiaPrefetchPlugin()

        plugin.on_stage_release(_handler(), ["never-staged"])

        assert fake.unpin_calls == []

    def test_close_unpins_leftover_leases(self, monkeypatch):
        fake = self._pin_fake(monkeypatch)
        plugin = GaiaPrefetchPlugin()
        plugin.on_stage(_handler(), ["a"], _resp(_entry(0, 0, 32)))
        plugin.on_stage(_handler(), ["b"], _resp(_entry(0, 1024, 32)))
        plugin.on_stage_release(_handler(), ["a"])

        plugin.on_close(_handler())

        assert (0, 1024, 32) in fake.unpin_calls
        assert len(fake.unpin_calls) == 2
        stats = plugin.contribute_stats()
        assert stats["stage_pinned_ranges"] == 2
        assert stats["stage_unpinned_ranges"] == 2
        assert stats["stage_unpin_failed"] == 0

    def test_pin_budget_degrades_overflow_to_prefetch_sync(self, monkeypatch):
        fake = self._pin_fake(monkeypatch)
        monkeypatch.setenv("MARU_GAIA_PIN_BUDGET_BYTES", "96")
        plugin = GaiaPrefetchPlugin()

        # First lease: 64 fits, next 64-piece would exceed 96 -> degraded.
        result = plugin.on_stage(_handler(), ["a"], _resp(_entry(0, 0, 128)))

        assert result.ready  # readiness holds via the sync fill
        assert fake.pin_calls == [(0, 0, 64)]
        assert fake.sync_calls == [(0, 64, 64)]
        assert plugin.contribute_stats()["stage_pin_degraded"] == 1
        assert plugin.contribute_stats()["pinned_bytes"] == 64

        # Second batch while the first lease is held: everything degrades.
        result2 = plugin.on_stage(_handler(), ["b"], _resp(_entry(0, 1024, 64)))
        assert result2.ready
        assert fake.pin_calls == [(0, 0, 64)]
        assert fake.sync_calls == [(0, 64, 64), (0, 1024, 64)]

        # Releasing the first lease returns its budget.
        plugin.on_stage_release(_handler(), ["a"])
        assert plugin.contribute_stats()["pinned_bytes"] == 0
        plugin.on_stage(_handler(), ["c"], _resp(_entry(0, 2048, 64)))
        assert fake.pin_calls == [(0, 0, 64), (0, 2048, 64)]

    def test_restage_replaces_lease_and_unpins_old_ranges(self, monkeypatch):
        fake = self._pin_fake(monkeypatch)
        plugin = GaiaPrefetchPlugin()
        plugin.on_stage(_handler(), ["a"], _resp(_entry(0, 0, 32)))

        plugin.on_stage(_handler(), ["a"], _resp(_entry(0, 2048, 32)))

        assert fake.unpin_calls == [(0, 0, 32)]
        plugin.on_stage_release(_handler(), ["a"])
        assert fake.unpin_calls == [(0, 0, 32), (0, 2048, 32)]

    def test_partial_pin_exception_keeps_successful_lease_releasable(self, monkeypatch):
        fake = self._pin_fake(monkeypatch)

        def memory_pin(device_id, addr, size):
            fake.pin_calls.append((device_id, addr, size))
            if addr == 64:
                raise TimeoutError("injected timeout")
            return fake.MemoryStatus.Success

        fake.memory_pin = memory_pin
        plugin = GaiaPrefetchPlugin()

        result = plugin.on_stage(_handler(), ["a"], _resp(_entry(0, 0, 96)))

        assert not result.ready
        assert fake.pin_calls == [(0, 0, 64), (0, 64, 32)]
        plugin.on_stage_release(_handler(), ["a"])
        assert fake.unpin_calls == [(0, 0, 64)]
        assert plugin.contribute_stats()["pinned_bytes"] == 0


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))


class TestSyncGateBudget:
    """MARU_GAIA_PREFETCH_SYNC_BUDGET_MS bounds how long one gate may block.

    The gate blocks the connector's single deferred-load thread, so an
    unbounded cold gate delays every request queued behind this one.
    """

    @pytest.fixture(autouse=True)
    def _gate_on(self, monkeypatch):
        monkeypatch.delenv("MARU_GAIA_DEVICE_ID", raising=False)
        monkeypatch.setenv("MARU_GAIA_PREFETCH_SYNC", "1")
        monkeypatch.setenv("MARU_GAIA_PREFETCH_COALESCE", "0")

    def _three_chunks(self):
        # Non-contiguous so coalescing cannot merge them into one range.
        return _resp(_entry(0, 0, 32), _entry(0, 1024, 32), _entry(0, 2048, 32))

    def test_unbudgeted_gate_blocks_on_every_range(self, monkeypatch):
        fake = _FakePyxif()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.setenv("MARU_GAIA_PREFETCH_SYNC_BUDGET_MS", "0")
        plugin = GaiaPrefetchPlugin()

        plugin.on_batch_retrieve(_handler(), ["a", "b", "c"], self._three_chunks())

        assert len(fake.sync_calls) == 3
        assert fake.calls == []

    def test_exhausted_budget_finishes_the_batch_asynchronously(self, monkeypatch):
        """Once the budget is spent the remaining ranges go out as async hints."""
        fake = _FakePyxif()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.setenv("MARU_GAIA_PREFETCH_SYNC_BUDGET_MS", "5")

        # First three reads (demand mark, batch t0, range-1 budget check) sit
        # at 0.0 so range 1 is gated; every later read sees 10 ms — past the
        # 5 ms budget — so ranges 2..3 degrade to async.
        clock = itertools.chain([0.0, 0.0, 0.0], itertools.repeat(0.010))
        monkeypatch.setattr("maru_gaia.plugin.time.monotonic", lambda: next(clock))
        plugin = GaiaPrefetchPlugin()

        plugin.on_batch_retrieve(_handler(), ["a", "b", "c"], self._three_chunks())

        # First range inside the budget; the 10 ms elapsed then exceeds 5 ms.
        assert len(fake.sync_calls) == 1
        assert len(fake.calls) == 2

    def test_budget_does_not_apply_to_the_arrival_lookahead(self, monkeypatch):
        """on_prefetch is async regardless — it must never block the caller."""
        fake = _FakePyxif()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.setenv("MARU_GAIA_PREFETCH_SYNC_BUDGET_MS", "0")
        plugin = GaiaPrefetchPlugin()

        plugin.on_prefetch(_handler(), ["a", "b", "c"], self._three_chunks())

        assert fake.sync_calls == []
        assert len(fake.calls) == 3


class _FakeTime:
    """Deterministic clock: sleep advances monotonic, nothing really waits."""

    def __init__(self):
        self.now = 1000.0

    def monotonic(self):
        return self.now

    def sleep(self, dt):
        self.now += dt

    def time(self):
        return self.now


class TestStageDemandYield:
    """Stage sub-calls pause while a demand read is recent (device yield)."""

    @pytest.fixture(autouse=True)
    def _env(self, monkeypatch):
        monkeypatch.delenv("MARU_GAIA_DEVICE_ID", raising=False)
        monkeypatch.setenv("MARU_GAIA_PREFETCH_COALESCE", "0")

    def test_yield_off_by_default(self, monkeypatch):
        fake = _FakePyxif()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.delenv("MARU_GAIA_STAGE_DEMAND_YIELD_MS", raising=False)
        plugin = GaiaPrefetchPlugin()

        result = plugin.on_stage(_handler(), ["a"], _resp(_entry(0, 0, 32)))

        assert result.ready
        assert result.yielded_ms == 0.0
        assert plugin.contribute_stats()["stage_yield_wait_ms"] == 0.0

    def test_stage_waits_out_the_demand_quiet_window(self, monkeypatch):
        fake = _FakePyxif()
        clock = _FakeTime()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.setattr("maru_gaia.plugin.time", clock)
        monkeypatch.setenv("MARU_GAIA_STAGE_DEMAND_YIELD_MS", "50")
        plugin = GaiaPrefetchPlugin()
        plugin.on_batch_retrieve(_handler(), ["d"], _resp(_entry(0, 4096, 32)))

        result = plugin.on_stage(_handler(), ["a"], _resp(_entry(0, 0, 32)))

        assert result.ready
        assert 48 <= result.yielded_ms <= 54
        assert plugin.contribute_stats()["stage_yield_wait_ms"] >= 48

    def test_yield_budget_caps_the_pause(self, monkeypatch):
        fake = _FakePyxif()
        clock = _FakeTime()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.setattr("maru_gaia.plugin.time", clock)
        monkeypatch.setenv("MARU_GAIA_STAGE_DEMAND_YIELD_MS", "1000000")
        monkeypatch.setenv("MARU_GAIA_STAGE_YIELD_BUDGET_MS", "30")
        plugin = GaiaPrefetchPlugin()
        plugin.on_batch_retrieve(_handler(), ["d"], _resp(_entry(0, 4096, 32)))

        result = plugin.on_stage(_handler(), ["a"], _resp(_entry(0, 0, 32)))

        assert result.ready  # the stage proceeds; starvation guard, not a fail
        assert 28 <= result.yielded_ms <= 34
        assert plugin.contribute_stats()["stage_yield_budget_hits"] >= 1

    def test_retrieve_marks_demand_even_when_hint_disabled(self, monkeypatch):
        fake = _FakePyxif()
        clock = _FakeTime()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.setattr("maru_gaia.plugin.time", clock)
        monkeypatch.setenv("MARU_GAIA_RETRIEVE_HINT", "0")
        monkeypatch.setenv("MARU_GAIA_STAGE_DEMAND_YIELD_MS", "50")
        plugin = GaiaPrefetchPlugin()

        plugin.on_batch_retrieve(_handler(), ["d"], _resp(_entry(0, 0, 32)))

        assert plugin._last_demand_at == clock.monotonic()
        assert fake.calls == [] and fake.sync_calls == []

    def test_in_flight_probe_extends_the_pause(self, monkeypatch):
        """A demand fill older than the window but still running keeps the
        stage paused — the in-flight probe closes the recent-start blind side."""
        fake = _FakePyxif()
        clock = _FakeTime()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.setattr("maru_gaia.plugin.time", clock)
        monkeypatch.setenv("MARU_GAIA_STAGE_DEMAND_YIELD_MS", "50")
        plugin = GaiaPrefetchPlugin()
        # Demand started long ago (timestamp rule sees quiet) but its copy is
        # still in flight for another 30 ms of fake time.
        plugin._last_demand_at = clock.monotonic() - 10.0
        in_flight_until = clock.monotonic() + 0.030
        handler = _handler()
        handler.demand_active = lambda: clock.monotonic() < in_flight_until

        result = plugin.on_stage(handler, ["a"], _resp(_entry(0, 0, 32)))

        assert result.ready
        assert 28 <= result.yielded_ms <= 34

    def test_probe_failure_fails_open(self, monkeypatch):
        fake = _FakePyxif()
        clock = _FakeTime()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.setattr("maru_gaia.plugin.time", clock)
        monkeypatch.setenv("MARU_GAIA_STAGE_DEMAND_YIELD_MS", "50")
        plugin = GaiaPrefetchPlugin()
        plugin._last_demand_at = clock.monotonic() - 10.0
        handler = _handler()

        def broken() -> bool:
            raise RuntimeError("probe died")

        handler.demand_active = broken

        result = plugin.on_stage(handler, ["a"], _resp(_entry(0, 0, 32)))

        assert result.ready
        assert result.yielded_ms == 0.0

    def test_probe_absent_uses_timestamp_rule_only(self, monkeypatch):
        fake = _FakePyxif()
        clock = _FakeTime()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.setattr("maru_gaia.plugin.time", clock)
        monkeypatch.setenv("MARU_GAIA_STAGE_DEMAND_YIELD_MS", "50")
        plugin = GaiaPrefetchPlugin()
        plugin._last_demand_at = clock.monotonic() - 10.0

        result = plugin.on_stage(_handler(), ["a"], _resp(_entry(0, 0, 32)))

        assert result.ready
        assert result.yielded_ms == 0.0
