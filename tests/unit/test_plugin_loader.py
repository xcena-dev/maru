# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for the OOT handler-plugin mechanism (maru_handler.plugin).

Covers the loader contract (soft-fail, name allowlist, env selection) and the
handler-side dispatch contract (partial protocol, per-plugin failure
isolation). Uses real ``importlib.metadata.EntryPoint`` objects pointing at
stdlib callables so the actual ``.load()`` path is exercised without needing a
separately installed plugin package.
"""

import logging
from collections import OrderedDict, defaultdict
from importlib.metadata import EntryPoint

from maru import MaruConfig, MaruHandler
from maru_handler.plugin import PLUGIN_GROUP, load_handler_plugins


def _ep(name: str, value: str) -> EntryPoint:
    """Build a real EntryPoint in the handler-plugin group."""
    return EntryPoint(name=name, value=value, group=PLUGIN_GROUP)


class _FakeEP:
    """Duck-typed entry point (name + load()) for edge cases a real EntryPoint
    can't express with a stdlib target — e.g. a factory that returns None."""

    def __init__(self, name, factory):
        self.name = name
        self._factory = factory

    def load(self):
        return self._factory


class TestLoadHandlerPlugins:
    """Loader discovery, instantiation, and failure-isolation contract."""

    def test_loads_and_instantiates(self):
        """A resolvable entry point is loaded and its factory invoked."""
        plugins = load_handler_plugins(
            entry_points_iter=[_ep("od", "collections:OrderedDict")]
        )
        assert len(plugins) == 1
        assert isinstance(plugins[0], OrderedDict)

    def test_missing_module_is_skipped_not_raised(self):
        """An unresolvable entry point is logged and skipped, never raised."""
        plugins = load_handler_plugins(
            entry_points_iter=[_ep("bad", "no_such_module_zzz:whatever")]
        )
        assert plugins == []

    def test_factory_raising_is_skipped(self):
        """A factory that raises on call is isolated — datetime() needs args."""
        plugins = load_handler_plugins(
            entry_points_iter=[_ep("raises", "datetime:datetime")]
        )
        assert plugins == []

    def test_one_bad_plugin_does_not_block_the_others(self):
        """Soft-fail: a broken entry point never prevents good ones loading."""
        plugins = load_handler_plugins(
            entry_points_iter=[
                _ep("bad", "no_such_module_zzz:x"),
                _ep("od", "collections:OrderedDict"),
                _ep("dd", "collections:defaultdict"),
            ]
        )
        assert [type(p) for p in plugins] == [OrderedDict, defaultdict]

    def test_allowlist_filters_by_name(self):
        """An explicit allowlist loads only the named plugins."""
        plugins = load_handler_plugins(
            entry_points_iter=[
                _ep("od", "collections:OrderedDict"),
                _ep("dd", "collections:defaultdict"),
            ],
            allowlist={"od"},
        )
        assert [type(p) for p in plugins] == [OrderedDict]

    def test_env_allowlist_is_consulted(self, monkeypatch):
        """MARU_PLUGINS selects plugins when no explicit allowlist is passed."""
        monkeypatch.setenv("MARU_PLUGINS", "dd")
        plugins = load_handler_plugins(
            entry_points_iter=[
                _ep("od", "collections:OrderedDict"),
                _ep("dd", "collections:defaultdict"),
            ]
        )
        assert [type(p) for p in plugins] == [defaultdict]

    def test_empty_env_loads_all(self, monkeypatch):
        """An empty/whitespace MARU_PLUGINS means 'no filter', not 'none'."""
        monkeypatch.setenv("MARU_PLUGINS", "  ")
        plugins = load_handler_plugins(
            entry_points_iter=[_ep("od", "collections:OrderedDict")]
        )
        assert len(plugins) == 1

    def test_factory_returning_none_is_skipped(self, caplog, monkeypatch):
        """A factory that returns None is dropped, not kept as a dead plugin."""
        # maru_handler logger has propagate=False (own handler); let records
        # reach caplog's root handler for the duration of the test.
        monkeypatch.setattr(logging.getLogger("maru_handler"), "propagate", True)
        with caplog.at_level("WARNING", logger="maru_handler.plugin"):
            plugins = load_handler_plugins(
                entry_points_iter=[_FakeEP("noop", lambda: None)]
            )
        assert plugins == []
        assert "returned None" in caplog.text

    def test_duplicate_names_first_wins(self, caplog, monkeypatch):
        """Two entries with the same name: keep the first, warn, drop the rest."""
        monkeypatch.setattr(logging.getLogger("maru_handler"), "propagate", True)
        with caplog.at_level("WARNING", logger="maru_handler.plugin"):
            plugins = load_handler_plugins(
                entry_points_iter=[
                    _ep("dup", "collections:OrderedDict"),
                    _ep("dup", "collections:defaultdict"),
                ]
            )
        assert [type(p) for p in plugins] == [OrderedDict]  # first wins
        assert "duplicate handler plugin name" in caplog.text

    def test_allowlist_typo_warns(self, caplog, monkeypatch):
        """An allowlist name matching no installed plugin is surfaced, not silent."""
        monkeypatch.setattr(logging.getLogger("maru_handler"), "propagate", True)
        with caplog.at_level("WARNING", logger="maru_handler.plugin"):
            plugins = load_handler_plugins(
                entry_points_iter=[_ep("od", "collections:OrderedDict")],
                allowlist={"od", "typo"},
            )
        assert [type(p) for p in plugins] == [OrderedDict]
        assert "matched no installed plugin" in caplog.text
        assert "typo" in caplog.text


# ---------------------------------------------------------------------------
# Handler-side dispatch: partial protocol + per-plugin failure isolation.
# ---------------------------------------------------------------------------


class _RecordingPlugin:
    """Implements only on_batch_retrieve — exercises partial-protocol support."""

    def __init__(self):
        self.batches: list[tuple[list[str], object]] = []

    def on_batch_retrieve(self, handler, keys, batch_resp):
        self.batches.append((keys, batch_resp))


class _ExplodingPlugin:
    """Every hook raises — must never propagate out of dispatch."""

    def on_init(self, handler):
        raise RuntimeError("boom-init")

    def on_batch_retrieve(self, handler, keys, batch_resp):
        raise RuntimeError("boom-retrieve")

    def contribute_stats(self):
        raise RuntimeError("boom-stats")


class _StatsPlugin:
    def contribute_stats(self):
        return {"answer": 42}


class TestHandlerDispatch:
    """MaruHandler._dispatch_plugins contract (no connection required)."""

    def _handler(self):
        return MaruHandler(MaruConfig(auto_connect=False))

    def test_dispatch_isolates_failure_and_continues(self):
        """A raising plugin does not stop a later well-behaved plugin."""
        handler = self._handler()
        recorder = _RecordingPlugin()
        handler._plugins = [_ExplodingPlugin(), recorder]

        # Must not raise despite _ExplodingPlugin.on_batch_retrieve blowing up.
        handler._dispatch_plugins("on_batch_retrieve", handler, ["k1"], "resp")

        assert recorder.batches == [(["k1"], "resp")]

    def test_missing_hook_is_skipped(self):
        """A plugin lacking the requested hook is silently skipped."""
        handler = self._handler()
        recorder = _RecordingPlugin()  # has no on_close
        handler._plugins = [recorder]

        handler._dispatch_plugins("on_close", handler)  # no-op, no error

    def test_get_stats_merges_plugin_contributions(self):
        """contribute_stats is namespaced under result['plugins'][ClassName]."""
        handler = self._handler()
        handler._rpc = _make_stats_rpc()
        handler._connected = True
        handler._owned = _make_owned_stub()  # satisfy _ensure_connected
        handler._plugins = [_StatsPlugin(), _ExplodingPlugin()]

        stats = handler.get_stats()

        # Good plugin contributes; exploding plugin is isolated (absent).
        assert stats["plugins"]["_StatsPlugin"] == {"answer": 42}
        assert "_ExplodingPlugin" not in stats["plugins"]


def _make_stats_rpc():
    """Minimal RPC stub returning the shape get_stats() consumes."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    rpc = MagicMock()
    rpc.get_stats.return_value = SimpleNamespace(
        kv_manager=SimpleNamespace(total_entries=0, total_size=0),
        allocation_manager=SimpleNamespace(
            num_allocations=0, total_allocated=0, active_clients=0
        ),
        stats_manager={},
        cxl_pool={},
    )
    return rpc


def _make_owned_stub():
    """Minimal owned-region-manager stub so _ensure_connected passes."""
    from unittest.mock import MagicMock

    owned = MagicMock()
    owned.get_stats.return_value = {"regions": []}
    return owned


# ---------------------------------------------------------------------------
# Plugin API contract — pins the public surface out-of-tree plugins depend on.
# If any assertion here fails, a change broke a documented plugin contract:
# fix the change or run a deprecation cycle. See docs api_reference/plugins.md.
# ---------------------------------------------------------------------------


class TestPluginApiContract:
    def test_handler_accessor_surface_is_stable(self):
        """MaruHandler exposes the two stable accessor methods with their signature."""
        import inspect

        for name in ("is_region_mapped", "get_region_dax_path"):
            method = getattr(MaruHandler, name, None)
            assert callable(method), f"MaruHandler.{name} removed — breaks plugins"
            params = list(inspect.signature(method).parameters)
            assert params == ["self", "region_id"], (
                f"MaruHandler.{name} signature drifted to {params}"
            )

    def test_plugin_hook_names_are_stable(self):
        """The four documented hook names exist on the MaruHandlerPlugin protocol."""
        from maru_handler.plugin import MaruHandlerPlugin

        for hook in ("on_init", "on_batch_retrieve", "on_close", "contribute_stats"):
            assert hasattr(MaruHandlerPlugin, hook), (
                f"hook {hook} removed from protocol"
            )

    def test_entry_point_group_name_is_stable(self):
        """The entry-point group name is itself part of the contract."""
        assert PLUGIN_GROUP == "maru.handler_plugins"

    def test_on_batch_retrieve_payload_fields_are_stable(self):
        """The batch_resp/handle fields plugins read are the core contract.

        These are what every field plugin actually depends on in
        on_batch_retrieve; renaming them in maru_common would pass maru CI green
        while silently breaking every installed plugin. Pin them here too.
        """
        from maru_common import BatchLookupKVResponse, LookupResult
        from maru_shm import MaruHandle

        assert "entries" in BatchLookupKVResponse.__dataclass_fields__
        for f in ("found", "handle", "kv_offset", "kv_length"):
            assert f in LookupResult.__dataclass_fields__, (
                f"LookupResult.{f} removed — breaks plugins"
            )
        for f in ("region_id", "offset"):
            assert f in MaruHandle.__dataclass_fields__, (
                f"MaruHandle.{f} removed — breaks plugins"
            )
