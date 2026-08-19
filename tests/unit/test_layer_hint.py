# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for ordered layer-major fill hints (MARU_LAYER_HINT).

Three seams: the handler's ``prefetch_grouped`` (one lookup, ordered
dispatch), the gaia plugin's ``on_prefetch_grouped`` (in-order coalescing and
sub-object ranges), and the connector's group builders plus the deferred-load
routing that lets layerwise storage take the overlap path.
"""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from maru import MaruConfig, MaruHandler
from maru_vllm.connector import (
    MaruSchedulerConnector,
    MaruWorkerConnector,
    _layerwise_key_groups,
    _packed_layer_span_groups,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "plugins" / "maru_gaia"))
# Third Party
from maru_gaia.plugin import GaiaPrefetchPlugin  # noqa: E402

MIB = 1024**2


class TestGroupBuilders:
    def test_layerwise_groups_are_one_layer_each_in_run_order(self):
        groups = _layerwise_key_groups(["c0", "c1"], [0, 5, 31])
        assert [len(g) for g in groups] == [2, 2, 2]
        assert groups[0] == [("c0_L0", None, None), ("c1_L0", None, None)]
        assert groups[1][0] == ("c0_L5", None, None)
        assert groups[2][1] == ("c1_L31", None, None)

    def test_packed_span_groups_hit_both_planes_of_each_layer(self):
        plane = 256 * 1024
        groups = _packed_layer_span_groups(["c0", "c1"], 32, plane, 1)
        assert len(groups) == 32
        # Layer 0: K plane at offset 0, V plane after all 32 K planes.
        assert groups[0] == [
            ("c0", 0, plane),
            ("c0", 32 * plane, plane),
            ("c1", 0, plane),
            ("c1", 32 * plane, plane),
        ]
        # Layer 7: offsets shift by 7 planes in each half.
        assert groups[7][0] == ("c0", 7 * plane, plane)
        assert groups[7][1] == ("c0", 39 * plane, plane)

    def test_packed_span_grouping_widens_the_sliver(self):
        plane = 256 * 1024
        groups = _packed_layer_span_groups(["c0"], 32, plane, 8)
        assert len(groups) == 4
        assert groups[0] == [("c0", 0, 8 * plane), ("c0", 32 * plane, 8 * plane)]
        assert groups[3] == [
            ("c0", 24 * plane, 8 * plane),
            ("c0", 56 * plane, 8 * plane),
        ]

    def test_packed_span_last_group_clamps(self):
        plane = 256 * 1024
        groups = _packed_layer_span_groups(["c0"], 10, plane, 4)
        assert len(groups) == 3
        assert groups[2] == [
            ("c0", 8 * plane, 2 * plane),
            ("c0", 18 * plane, 2 * plane),
        ]


class _Entry(SimpleNamespace):
    pass


def _entry(found=True, region=1, offset=0, kv_offset=0, kv_length=16 * MIB):
    handle = SimpleNamespace(region_id=region, offset=offset) if found else None
    return _Entry(found=found, handle=handle, kv_offset=kv_offset, kv_length=kv_length)


class TestHandlerPrefetchGrouped:
    def _handler(self) -> MaruHandler:
        handler = MaruHandler(MaruConfig(auto_connect=False))
        handler._connected = True
        handler._owned = MagicMock()
        handler._rpc = MagicMock()
        return handler

    def test_one_lookup_covers_deduped_keys_and_dispatches_groups(self):
        handler = self._handler()
        handler._rpc.batch_lookup_kv.return_value = SimpleNamespace(
            entries=[_entry(), _entry(found=False)]
        )
        recorder = MagicMock()
        handler._plugins = [recorder]
        groups = [
            [("k0", None, None), ("k1", 0, 4096)],
            [("k0", 4096, 4096)],  # k0 repeats; the lookup must not
        ]

        found = handler.prefetch_grouped(groups)

        assert found == 1
        handler._rpc.batch_lookup_kv.assert_called_once_with(["k0", "k1"])
        (called_handler, called_groups, entries) = (
            recorder.on_prefetch_grouped.call_args.args
        )
        assert called_handler is handler
        assert called_groups is groups
        assert set(entries) == {"k0", "k1"}
        assert entries["k0"].found and not entries["k1"].found

    def test_rpc_failure_returns_zero_without_dispatch(self):
        handler = self._handler()
        handler._rpc.batch_lookup_kv.side_effect = RuntimeError("down")
        recorder = MagicMock()
        handler._plugins = [recorder]
        assert handler.prefetch_grouped([[("k0", None, None)]]) == 0
        recorder.on_prefetch_grouped.assert_not_called()

    def test_empty_groups_are_free(self):
        handler = self._handler()
        assert handler.prefetch_grouped([]) == 0
        handler._rpc.batch_lookup_kv.assert_not_called()


class TestOrderedCoalesce:
    def test_folds_only_list_adjacent_contiguous(self):
        merged = GaiaPrefetchPlugin._coalesce_ordered(
            [(0, 0, 10), (0, 10, 10), (0, 100, 5), (0, 50, 5), (0, 55, 5)]
        )
        assert merged == [(0, 0, 20), (0, 100, 5), (0, 50, 10)]

    def test_never_reorders_across_devices(self):
        merged = GaiaPrefetchPlugin._coalesce_ordered(
            [(1, 0, 10), (0, 10, 10), (1, 10, 10)]
        )
        assert merged == [(1, 0, 10), (0, 10, 10), (1, 10, 10)]


class TestPluginGrouped:
    def _plugin(self, monkeypatch, calls):
        monkeypatch.setenv("MARU_GAIA_DEVICE_ID", "0")
        plugin = GaiaPrefetchPlugin()
        plugin._gaia_devices = {0: True}
        # First Party
        import maru_gaia.plugin as mod

        monkeypatch.setattr(
            mod.pyxif,
            "memory_prefetch",
            lambda dev, addr, size: calls.append((dev, addr, size)) or 0,
        )
        return plugin

    def _handler(self):
        handler = MagicMock()
        handler.is_region_mapped.return_value = True
        return handler

    def test_groups_issue_in_order_with_subranges(self, monkeypatch):
        calls: list = []
        plugin = self._plugin(monkeypatch, calls)
        entries = {"c0": _entry(offset=0), "c1": _entry(offset=64 * MIB)}
        plane = 256 * 1024
        groups = [
            [("c0", 0, plane), ("c1", 0, plane)],  # layer 0
            [("c0", plane, plane), ("c1", plane, plane)],  # layer 1
        ]

        plugin.on_prefetch_grouped(self._handler(), groups, entries)

        assert calls == [
            (0, 0, plane),
            (0, 64 * MIB, plane),
            (0, plane, plane),
            (0, 64 * MIB + plane, plane),
        ]

    def test_contiguous_whole_objects_coalesce_within_a_group(self, monkeypatch):
        calls: list = []
        plugin = self._plugin(monkeypatch, calls)
        entries = {
            "c0": _entry(offset=0),
            "c1": _entry(offset=16 * MIB),
            "c2": _entry(offset=48 * MIB),
        }
        groups = [[("c0", None, None), ("c1", None, None), ("c2", None, None)]]

        plugin.on_prefetch_grouped(self._handler(), groups, entries)

        assert calls == [(0, 0, 32 * MIB), (0, 48 * MIB, 16 * MIB)]

    def test_length_clamps_to_the_object(self, monkeypatch):
        calls: list = []
        plugin = self._plugin(monkeypatch, calls)
        entries = {"c0": _entry(kv_length=MIB)}
        plugin.on_prefetch_grouped(
            self._handler(), [[("c0", MIB // 2, 4 * MIB)]], entries
        )
        assert calls == [(0, MIB // 2, MIB // 2)]

    def test_missing_or_unmapped_keys_are_skipped(self, monkeypatch):
        calls: list = []
        plugin = self._plugin(monkeypatch, calls)
        handler = self._handler()
        handler.is_region_mapped.return_value = False
        plugin.on_prefetch_grouped(
            handler,
            [[("c0", None, None), ("missing", None, None)]],
            {"c0": _entry()},
        )
        assert calls == []

    def test_retrieve_hint_env_suppresses_the_demand_hook(self, monkeypatch):
        monkeypatch.setenv("MARU_GAIA_RETRIEVE_HINT", "0")
        monkeypatch.setenv("MARU_GAIA_DEVICE_ID", "0")
        plugin = GaiaPrefetchPlugin()
        issued: list = []
        plugin._issue = lambda *a, **k: issued.append(a)
        plugin.on_batch_retrieve(MagicMock(), ["k"], SimpleNamespace(entries=[]))
        assert issued == []

    def test_retrieve_hint_defaults_on(self, monkeypatch):
        monkeypatch.delenv("MARU_GAIA_RETRIEVE_HINT", raising=False)
        monkeypatch.setenv("MARU_GAIA_DEVICE_ID", "0")
        plugin = GaiaPrefetchPlugin()
        assert plugin._retrieve_hint is True


OVERLAP_CONFIG = {
    "maru_async_load": True,
    "maru_overlap_load_with_compute": True,
    "maru_use_layerwise": True,
}


class TestLayerwiseOverlapGate:
    def test_layerwise_storage_no_longer_disables_overlap(self, monkeypatch):
        monkeypatch.delenv("MARU_HYMCACHE_WINDOW_BYTES", raising=False)
        sched = MaruSchedulerConnector(
            block_size=16, kv_chunk_tokens=128, extra_config=dict(OVERLAP_CONFIG)
        )
        worker = MaruWorkerConnector(
            block_size=16, kv_chunk_tokens=128, extra_config=dict(OVERLAP_CONFIG)
        )
        assert sched._layerwise_overlap is True
        assert worker._layerwise_overlap is True
        worker.shutdown()

    def test_overlap_still_requires_the_async_load(self, monkeypatch):
        monkeypatch.delenv("MARU_HYMCACHE_WINDOW_BYTES", raising=False)
        config = dict(OVERLAP_CONFIG)
        config["maru_async_load"] = False
        sched = MaruSchedulerConnector(
            block_size=16, kv_chunk_tokens=128, extra_config=config
        )
        assert sched._layerwise_overlap is False

    def test_window_still_supersedes(self, monkeypatch):
        monkeypatch.setenv("MARU_HYMCACHE_WINDOW_BYTES", str(128 * MIB))
        sched = MaruSchedulerConnector(
            block_size=16, kv_chunk_tokens=128, extra_config=dict(OVERLAP_CONFIG)
        )
        assert sched._layerwise_overlap is False


class TestDeferredLayerwiseRouting:
    def _worker(self, monkeypatch, **env) -> MaruWorkerConnector:
        for key, value in env.items():
            monkeypatch.setenv(key, value)
        return MaruWorkerConnector(
            block_size=16, kv_chunk_tokens=128, extra_config=dict(OVERLAP_CONFIG)
        )

    def test_submit_declines_without_registered_caches(self, monkeypatch):
        worker = self._worker(monkeypatch)
        assert worker._try_submit_deferred_layerwise_load(SimpleNamespace()) is False
        worker.shutdown()

    def test_submit_declines_under_a_window(self, monkeypatch):
        worker = self._worker(monkeypatch, MARU_HYMCACHE_WINDOW_BYTES=str(128 * MIB))
        worker._kv_caches = {"l0": MagicMock()}
        worker._num_layers = 32
        assert worker._try_submit_deferred_layerwise_load(SimpleNamespace()) is False
        worker.shutdown()

    def test_layer_hint_env_knobs(self, monkeypatch):
        worker = self._worker(
            monkeypatch, MARU_LAYER_HINT="1", MARU_LAYER_HINT_GROUP="8"
        )
        assert worker._layer_hint_enabled is True
        assert worker._layer_hint_group == 8
        worker.shutdown()

    def test_layer_hint_defaults_off(self, monkeypatch):
        monkeypatch.delenv("MARU_LAYER_HINT", raising=False)
        monkeypatch.delenv("MARU_LAYER_HINT_GROUP", raising=False)
        worker = self._worker(monkeypatch)
        assert worker._layer_hint_enabled is False
        assert worker._layer_hint_group == 1
        worker.shutdown()


class TestRetrievePiggyback:
    """hint_groups ride batch_retrieve's own lookup — no extra RPC."""

    def _handler(self) -> MaruHandler:
        handler = MaruHandler(MaruConfig(auto_connect=False))
        handler._connected = True
        handler._owned = MagicMock()
        handler._owned.is_owned.return_value = True
        handler._rpc = MagicMock()
        return handler

    def test_groups_dispatch_before_any_extra_rpc(self):
        handler = self._handler()
        # Not-found entries keep the retrieval tail (region mmap) out of
        # the test; the hint dispatch happens regardless and the plugin is
        # the one that filters unusable entries.
        handler._rpc.batch_lookup_kv.return_value = SimpleNamespace(
            entries=[_entry(found=False), _entry(found=False)]
        )
        recorder = MagicMock()
        handler._plugins = [recorder]

        assert handler.batch_retrieve(
            ["k0", "k1"], hint_groups=[[(1, None, None)], [(0, 0, 4096)]]
        ) == [None, None]

        assert handler._rpc.batch_lookup_kv.call_count == 1
        (_, groups, entries) = recorder.on_prefetch_grouped.call_args.args
        assert groups == [[("k1", None, None)], [("k0", 0, 4096)]]
        assert set(entries) == {"k0", "k1"}

    def test_no_groups_means_no_dispatch(self):
        handler = self._handler()
        handler._rpc.batch_lookup_kv.return_value = SimpleNamespace(
            entries=[_entry(found=False)]
        )
        recorder = MagicMock()
        handler._plugins = [recorder]
        handler.batch_retrieve(["k0"])
        recorder.on_prefetch_grouped.assert_not_called()

    def test_batched_retrieve_splits_groups_at_chunk_boundaries(self, monkeypatch):
        worker = MaruWorkerConnector(
            block_size=16, kv_chunk_tokens=128, extra_config={}
        )
        calls: list = []

        class _Handler:
            def batch_retrieve(self, keys, hint_groups=None):
                calls.append((list(keys), hint_groups))
                return [None] * len(keys)

        worker._handler = _Handler()
        keys = [f"k{i}" for i in range(5)]
        # One group straddles the 2-key chunk boundary; order must survive.
        groups = [
            [(0, None, None), (1, None, None), (2, None, None)],
            [(3, 0, 64), (4, None, None)],
        ]

        worker._batch_retrieve_all(keys, batch_size=2, hint_groups=groups)

        assert [c[0] for c in calls] == [["k0", "k1"], ["k2", "k3"], ["k4"]]
        assert calls[0][1] == [[(0, None, None), (1, None, None)]]
        assert calls[1][1] == [[(0, None, None)], [(1, 0, 64)]]
        assert calls[2][1] == [[(0, None, None)]]
        worker.shutdown()
