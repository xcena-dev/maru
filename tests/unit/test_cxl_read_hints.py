# SPDX-License-Identifier: Apache-2.0
"""Read hints of the cxl backend: the handler gives the remote_cxl hint
messages, in the same form, to the handler plugins that take them."""

import threading

import pytest

from maru_common import MaruConfig
from maru_common.protocol import BatchLookupKVResponse, LookupResult
from maru_handler import MaruHandler
from maru_shm.types import MaruHandle

GIB = 1 << 30


class HintPlugin:
    """A plugin that records the hint messages it takes."""

    hint_actions = ("read_hint", "read_hint_end", "read_promote")

    def __init__(self):
        self.messages = []
        self.got = threading.Event()

    def on_memory_hint(self, handler, message):
        self.messages.append(message)
        self.got.set()


class Rpc:
    """MaruServer stand-in: keys named ``hit*`` are stored, the rest miss."""

    def __init__(self):
        self.lookups = []

    def batch_lookup_kv(self, keys):
        self.lookups.append(list(keys))
        entries = []
        for i, key in enumerate(keys):
            if key.startswith("hit"):
                handle = MaruHandle(region_id=7, offset=4 * GIB, length=GIB)
                entries.append(
                    LookupResult(
                        found=True, handle=handle, kv_offset=i << 21, kv_length=1 << 21
                    )
                )
            else:
                entries.append(LookupResult(found=False))
        return BatchLookupKVResponse(entries=entries)


def _handler(monkeypatch, plugins, **config):
    monkeypatch.setattr("maru_handler.handler.load_handler_plugins", lambda: plugins)
    handler = MaruHandler(MaruConfig(auto_connect=False, **config))
    handler._rpc = Rpc()
    handler._connected = True
    return handler


def _drain(handler):
    assert handler._local_hints.close()


def test_the_config_takes_read_hints_on_the_cxl_backend():
    MaruConfig(storage_backend="cxl", read_hints=True, read_promotes=True)
    with pytest.raises(ValueError, match="write_hints"):
        MaruConfig(storage_backend="cxl", write_hints=True)


def test_a_read_hint_names_the_leading_hits_bytes_in_the_device(monkeypatch):
    plugin = HintPlugin()
    handler = _handler(monkeypatch, [plugin], metadata_only=True, read_hints=True)
    assert handler.read_hints_enabled()

    handler.hint_read("r1", ["hit0", "hit1", "miss2", "hit3"])
    handler.end_read_hint("r1")
    _drain(handler)

    hint, end = plugin.messages
    assert hint["action"] == "read_hint" and hint["request"] == "r1"
    # The load reads the leading hits only; the hit after a miss is left out.
    handle = {"region_id": 7, "offset": 4 * GIB, "length": GIB}
    assert hint["ranges"] == [
        {"handle": handle, "offset": 0, "length": 1 << 21},
        {"handle": handle, "offset": 1 << 21, "length": 1 << 21},
    ]
    assert end == {"action": "read_hint_end", "request": "r1", "ranges": []}


def test_a_request_whose_first_key_misses_is_not_hinted(monkeypatch):
    plugin = HintPlugin()
    handler = _handler(monkeypatch, [plugin], metadata_only=True, read_hints=True)
    handler.hint_read("r1", ["miss0", "hit1"])
    handler.end_read_hint("r1")
    _drain(handler)
    assert plugin.messages == []  # no hint, so no end either


def test_the_worker_promotes_a_load_its_scheduler_hinted(monkeypatch):
    plugin = HintPlugin()
    handler = _handler(monkeypatch, [plugin], read_promotes=True)
    # A worker sends no read hints of its own.
    assert not handler.read_hints_enabled()
    handler.hint_read("r1", ["hit0"])
    handler.promote_read("r1")
    _drain(handler)
    assert plugin.messages == [
        {"action": "read_promote", "request": "r1", "ranges": []}
    ]
    assert handler._rpc.lookups == []


def test_hints_go_only_to_plugins_that_take_their_action(monkeypatch):
    class PromoteOnly(HintPlugin):
        hint_actions = ("read_promote",)

    class Plain:
        def on_batch_retrieve(self, handler, keys, resp):
            pass

    promote_only, takes_all = PromoteOnly(), HintPlugin()
    handler = _handler(
        monkeypatch,
        [Plain(), promote_only, takes_all],
        metadata_only=True,
        read_hints=True,
    )
    handler.hint_read("r1", ["hit0"])
    _drain(handler)
    assert promote_only.messages == []
    assert [m["action"] for m in takes_all.messages] == ["read_hint"]


def test_a_failing_plugin_costs_the_others_nothing(monkeypatch):
    class Failing(HintPlugin):
        def on_memory_hint(self, handler, message):
            raise RuntimeError("boom")

    good = HintPlugin()
    handler = _handler(
        monkeypatch, [Failing(), good], metadata_only=True, read_hints=True
    )
    handler.hint_read("r1", ["hit0"])
    _drain(handler)
    assert [m["action"] for m in good.messages] == ["read_hint"]


def test_without_a_hint_plugin_no_hint_is_sent(monkeypatch, caplog):
    import logging

    monkeypatch.setattr(logging.getLogger("maru_handler"), "propagate", True)
    caplog.set_level("WARNING", logger="maru_handler.handler")
    handler = _handler(monkeypatch, [], metadata_only=True, read_hints=True)
    assert not handler.read_hints_enabled()
    handler.hint_read("r1", ["hit0"])
    assert handler._rpc.lookups == []
    assert "no handler plugin takes them" in caplog.text


def test_a_hint_plugin_hears_nothing_unless_the_config_asks(monkeypatch):
    plugin = HintPlugin()
    handler = _handler(monkeypatch, [plugin], metadata_only=True)
    assert not handler.read_hints_enabled()
    handler.hint_read("r1", ["hit0"])
    handler.promote_read("r1")
    assert handler._local_hints is None
    assert plugin.messages == []


def test_close_sends_the_hints_already_queued(monkeypatch):
    plugin = HintPlugin()
    handler = _handler(monkeypatch, [plugin], metadata_only=True, read_hints=True)
    handler._rpc.close = lambda: None
    handler.hint_read("r1", ["hit0"])
    handler.close()
    assert [m["action"] for m in plugin.messages] == ["read_hint"]
    handler.hint_read("r2", ["hit0"])  # after close: ignored
    assert len(plugin.messages) == 1
