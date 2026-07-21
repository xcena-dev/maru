# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for the gaia prefetch plugin (device address math + coalesce).

``pyxif`` is replaced by a fake so the tests run without a device: they pin the
address computation (``handle.offset + kv_offset`` for ``kv_length``), range
coalescing, and the unmapped-region skip.
"""

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

    def get_device_list(self):
        return [self._device_id]

    def cxl_get_regions(self, device_id):
        return [SimpleNamespace(dax_device=self._dax_path)]

    def memory_prefetch(self, device_id, addr, size):
        self.calls.append((device_id, addr, size))
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
    def test_address_is_handle_offset_plus_kv_offset(self, monkeypatch):
        """A single found+mapped entry hints handle.offset+kv_offset/kv_length."""
        fake = _FakePyxif()
        monkeypatch.setattr("maru_gaia.plugin.pyxif", fake)
        monkeypatch.delenv("MARU_GAIA_PREFETCH_COALESCE", raising=False)
        plugin = GaiaPrefetchPlugin()

        plugin.on_prefetch(_handler(), ["k0"], _resp(_entry(0x1000, 0x200, 0x100)))

        assert fake.calls == [(0, 0x1200, 0x100)]

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


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
