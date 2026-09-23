# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Record-order lookahead in the gaia plugin (MARU_GAIA_LOOKAHEAD_DEPTH).

The plugin records the order in which demand reads ask for their keys and,
when a recorded batch is read again, hands the batch ``depth`` positions later
to ``MaruHandler.prefetch_batch``. Maru core carries none of this: the plugin
works from the keys the ``on_batch_retrieve`` seam already passes it.
"""

from unittest.mock import MagicMock

from maru_gaia.plugin import GaiaPrefetchPlugin


def _plugin(monkeypatch, depth: str | None) -> GaiaPrefetchPlugin:
    """A plugin with the knob set, and every device path stubbed out."""
    if depth is None:
        monkeypatch.delenv("MARU_GAIA_LOOKAHEAD_DEPTH", raising=False)
    else:
        monkeypatch.setenv("MARU_GAIA_LOOKAHEAD_DEPTH", depth)
    monkeypatch.setenv("MARU_GAIA_RETRIEVE_HINT", "0")
    monkeypatch.setenv("MARU_GAIA_SMART_MARK", "0")
    plugin = GaiaPrefetchPlugin()
    plugin._device_id = None
    return plugin


def _retrieve(plugin, handler, keys):
    """Drive the demand-read seam the way MaruHandler does."""
    plugin.on_batch_retrieve(handler, keys, MagicMock())


class TestKnob:
    def test_depth_is_read_from_the_environment(self, monkeypatch):
        assert _plugin(monkeypatch, "4")._lookahead_depth == 4

    def test_default_is_off(self, monkeypatch):
        plugin = _plugin(monkeypatch, None)
        assert plugin._lookahead_depth == 0
        assert plugin._seen_order == []


class TestRecordAndFire:
    def test_first_pass_only_records(self, monkeypatch):
        plugin, handler = _plugin(monkeypatch, "1"), MagicMock()
        _retrieve(plugin, handler, ["a0", "a1"])
        _retrieve(plugin, handler, ["b0"])
        assert plugin._seen_order == [["a0", "a1"], ["b0"]]
        handler.prefetch_batch.assert_not_called()

    def test_replay_fires_the_batch_ahead(self, monkeypatch):
        plugin, handler = _plugin(monkeypatch, "1"), MagicMock()
        for batch in (["a0"], ["b0"], ["c0"]):
            _retrieve(plugin, handler, batch)
        handler.prefetch_batch.reset_mock()

        _retrieve(plugin, handler, ["a0"])
        handler.prefetch_batch.assert_called_once_with(["b0"])

    def test_depth_selects_how_far_ahead(self, monkeypatch):
        plugin, handler = _plugin(monkeypatch, "2"), MagicMock()
        for batch in (["a0"], ["b0"], ["c0"]):
            _retrieve(plugin, handler, batch)
        handler.prefetch_batch.reset_mock()

        _retrieve(plugin, handler, ["a0"])
        handler.prefetch_batch.assert_called_once_with(["c0"])

    def test_tail_of_the_order_fires_nothing(self, monkeypatch):
        plugin, handler = _plugin(monkeypatch, "1"), MagicMock()
        _retrieve(plugin, handler, ["a0"])
        _retrieve(plugin, handler, ["b0"])
        handler.prefetch_batch.reset_mock()

        _retrieve(plugin, handler, ["b0"])
        handler.prefetch_batch.assert_not_called()

    def test_replay_does_not_grow_the_record(self, monkeypatch):
        plugin, handler = _plugin(monkeypatch, "1"), MagicMock()
        for batch in (["a0"], ["b0"], ["a0"], ["b0"]):
            _retrieve(plugin, handler, batch)
        assert plugin._seen_order == [["a0"], ["b0"]]

    def test_disabled_records_nothing(self, monkeypatch):
        plugin, handler = _plugin(monkeypatch, "0"), MagicMock()
        _retrieve(plugin, handler, ["a0"])
        _retrieve(plugin, handler, ["a0"])
        assert plugin._seen_order == []
        handler.prefetch_batch.assert_not_called()

    def test_handler_error_never_escapes_the_read_seam(self, monkeypatch):
        plugin, handler = _plugin(monkeypatch, "1"), MagicMock()
        _retrieve(plugin, handler, ["a0"])
        _retrieve(plugin, handler, ["b0"])
        handler.prefetch_batch.side_effect = RuntimeError("boom")

        _retrieve(plugin, handler, ["a0"])  # must not raise

    def test_fired_count_is_reported(self, monkeypatch):
        plugin, handler = _plugin(monkeypatch, "1"), MagicMock()
        handler.prefetch_batch.return_value = 1
        for batch in (["a0"], ["b0"], ["a0"]):
            _retrieve(plugin, handler, batch)
        stats = plugin.contribute_stats()
        assert stats["lookahead_depth"] == 1 and stats["lookahead_fired"] == 1


class TestSeamIndependence:
    def test_lookahead_runs_with_the_read_time_hint_off(self, monkeypatch):
        """The two hints are separate axes; neither gates the other."""
        plugin, handler = _plugin(monkeypatch, "1"), MagicMock()
        assert plugin._retrieve_hint is False
        _retrieve(plugin, handler, ["a0"])
        _retrieve(plugin, handler, ["b0"])
        handler.prefetch_batch.reset_mock()

        _retrieve(plugin, handler, ["a0"])
        handler.prefetch_batch.assert_called_once_with(["b0"])

    def test_hymcache_local_mode_skips_the_seam(self, monkeypatch):
        monkeypatch.setenv("MARU_HYMCACHE_WINDOW_BYTES", "4096")
        plugin, handler = _plugin(monkeypatch, "1"), MagicMock()
        _retrieve(plugin, handler, ["a0"])
        assert plugin._seen_order == []
