# SPDX-License-Identifier: Apache-2.0
"""The scheduler connector gives storage read hints: once per request, before
its load, and ends them when the request finishes."""

import time
from types import SimpleNamespace

import maru
from maru_vllm.connector import _chunk_keys, _create_maru_handler
from tests.unit.test_vllm_remote_cxl_config import extra
from tests.unit.vllm_connector_helpers import make_scheduler


class Handler:
    """Scheduler-side MaruHandler stand-in that records hints."""

    def __init__(self, hits):
        self.hits = hits
        self.hints = []
        self.ends = []

    def batch_exists(self, keys, route_key=None):
        return [i < self.hits for i in range(len(keys))]

    def hint_read(self, request_id, keys, route_key=None):
        self.hints.append((request_id, list(keys), route_key))

    def end_read_hint(self, request_id):
        self.ends.append(request_id)


def _request(rid, tokens=64):
    return SimpleNamespace(
        request_id=rid,
        prompt_token_ids=list(range(tokens)),
        num_tokens=tokens,
        sampling_params=None,
    )


def _scheduler(hits, **config):
    sched = make_scheduler(4, 8, extra(maru_async_load=True, **config))
    sched._handler = Handler(hits)
    return sched


def test_the_first_hit_hints_the_keys_the_load_reads_once():
    sched = _scheduler(hits=5)
    request = _request("r1")
    keys = _chunk_keys(request.prompt_token_ids, 8)
    matched, load_async = sched.get_num_new_matched_tokens(request, 8)
    assert load_async and matched > 0
    sched.get_num_new_matched_tokens(request, 8)
    (hint,) = sched._handler.hints
    # Computed tokens 0-7 are chunk 0: the load reads chunks 1-4.
    assert hint == ("r1", keys[1:5], keys[1])
    sched.request_finished(request, [])
    assert sched._handler.ends == ["r1"]


def test_a_request_with_nothing_to_load_is_not_hinted():
    sched = _scheduler(hits=0)
    request = _request("r1")
    assert sched.get_num_new_matched_tokens(request, 0) == (0, False)
    sched.request_finished(request, [])
    assert sched._handler.hints == [] and sched._handler.ends == []


def test_an_arrival_hint_is_sent_off_the_caller_and_never_after_the_end():
    sched = _scheduler(hits=5)
    request = _request("r1")
    sched.request_arrived(request)
    deadline = time.monotonic() + 5
    while not sched._handler.hints:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    keys = _chunk_keys(request.prompt_token_ids, 8)
    assert sched._handler.hints == [("r1", keys, keys[0])]
    sched.get_num_new_matched_tokens(request, 0)
    assert len(sched._handler.hints) == 1  # the first hit finds it hinted

    late = _request("r2")
    sched._arrived.add("r2")
    sched.request_finished(late, [])  # ended before its hint was ready
    sched._hint_on_arrival(late)
    assert [h[0] for h in sched._handler.hints] == ["r1"]


def test_the_scheduler_gets_the_access_policy_too(monkeypatch):
    seen = []

    class Recorder:
        def __init__(self, config):
            seen.append(config)

        def connect(self):
            return True

    monkeypatch.setattr(maru, "MaruHandler", Recorder)
    config = extra(
        maru_remote_cxl_agent_key_file="/k",
        maru_access_policy="device",
        maru_access_policy_config="/etc/device.toml",
    )
    _create_maru_handler(config, metadata_only=True)
    assert (seen[0].access_policy, seen[0].access_policy_config) == (
        "device",
        "/etc/device.toml",
    )


def test_the_engine_hands_each_received_request_to_its_scheduler(monkeypatch):
    import sys
    import types

    import vllm.v1.engine as engine_pkg

    import maru_vllm.connector as connector

    class EngineCore:
        def preprocess_add_request(self, request):
            return (SimpleNamespace(request_id=request), 0)

    core = types.ModuleType("vllm.v1.engine.core")
    core.EngineCore = EngineCore
    monkeypatch.setitem(sys.modules, "vllm.v1.engine.core", core)
    monkeypatch.setattr(engine_pkg, "core", core, raising=False)
    monkeypatch.setattr(connector, "_ARRIVAL_HINTER", None)
    sched = _scheduler(hits=5, maru_read_hint_on_arrival=True)
    arrived = []
    sched.request_arrived = lambda request: arrived.append(request.request_id)
    engine = EngineCore()
    engine.scheduler = SimpleNamespace(connector=SimpleNamespace(_scheduler=sched))
    result = engine.preprocess_add_request("r1")
    assert result[0].request_id == "r1" and arrived == ["r1"]
    # Installing again does not wrap twice.
    connector._install_arrival_hint(sched)
    engine.preprocess_add_request("r2")
    assert arrived == ["r1", "r2"]
