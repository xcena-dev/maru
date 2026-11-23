# SPDX-License-Identifier: Apache-2.0
"""The scheduler connector gives storage read hints: once per request, before
its load, and ends them when the request finishes."""

import time
from types import SimpleNamespace

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import maru
from maru_vllm.connector import _chunk_keys, _create_maru_handler
from tests.unit.test_vllm_remote_cxl_config import extra
from tests.unit.vllm_connector_helpers import make_scheduler


class Handler:
    """Scheduler-side MaruHandler stand-in that records hints."""

    def __init__(self, hits, hints_enabled=True):
        self.hits = hits
        self.hints = []
        self.ends = []
        self.enabled = hints_enabled

    def read_hints_enabled(self):
        return self.enabled

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


def _drain(sched):
    """Wait for the calls the hint thread has queued (and those they queue)."""
    for _ in range(2):
        if sched._hint_pool is not None:
            sched._hint_pool.submit(lambda: None).result(timeout=5)


def test_the_first_hit_hints_the_keys_the_load_reads_once():
    sched = _scheduler(hits=5)
    request = _request("r1")
    keys = _chunk_keys(request.prompt_token_ids, 8)
    matched, load_async = sched.get_num_new_matched_tokens(request, 8)
    assert load_async and matched > 0
    sched.get_num_new_matched_tokens(request, 8)
    _drain(sched)
    (hint,) = sched._handler.hints
    # Computed tokens 0-7 are chunk 0: the load reads chunks 1-4, from the
    # pool of the request's first chunk key, where the load goes too.
    assert hint == ("r1", keys[1:5], keys[0])
    sched.request_finished(request, [])
    _drain(sched)
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
    _drain(sched)
    assert len(sched._handler.hints) == 1  # the first hit finds it hinted

    late = _request("r2")
    sched._arrived["r2"] = None
    sched.request_finished(late, [])  # ended before its hint was ready
    sched._hint_on_arrival(late)
    _drain(sched)
    assert [h[0] for h in sched._handler.hints] == ["r1"]


def test_only_the_scheduler_sends_read_hints(monkeypatch):
    seen = []

    class Recorder:
        def __init__(self, config):
            seen.append(config)

        def connect(self):
            return True

    monkeypatch.setattr(maru, "MaruHandler", Recorder)
    config = extra(
        maru_remote_cxl_agent_key_file="/k", maru_read_hints=True, maru_async_load=True
    )
    _create_maru_handler(config, metadata_only=True)
    _create_maru_handler(config, metadata_only=False)
    # The scheduler hints each load before its worker reads it; the worker
    # hints its stores, as read hints are on.
    assert [c.read_hints for c in seen] == [True, False]
    assert [c.write_hints for c in seen] == [False, True]
    # The worker promotes each load its scheduler hinted as it starts.
    assert [c.read_promotes for c in seen] == [False, True]
    assert seen[1].write_wait_ms == 50
    seen.clear()
    _create_maru_handler({**config, "maru_write_wait_ms": 0}, metadata_only=False)
    assert seen[0].write_wait_ms == 0
    seen.clear()
    # Without asynchronous loads the scheduler hints nothing to promote.
    _create_maru_handler({**config, "maru_async_load": False}, metadata_only=False)
    assert not seen[0].read_promotes
    seen.clear()
    _create_maru_handler({**config, "maru_write_hints": False}, metadata_only=False)
    _create_maru_handler(
        {**extra(maru_remote_cxl_agent_key_file="/k"), "maru_write_hints": True},
        metadata_only=False,
    )
    assert [c.write_hints for c in seen] == [False, True]


def test_a_hint_ends_when_its_load_is_done_and_only_once():
    sched = _scheduler(hits=5)
    request = _request("r1")
    sched.get_num_new_matched_tokens(request, 8)
    _drain(sched)
    assert [h[0] for h in sched._handler.hints] == ["r1"]
    done = SimpleNamespace(
        finished_recving={"r1", "other"}, invalid_block_ids=None, failed_recving=None
    )
    sched.update_connector_output(done)
    _drain(sched)
    assert sched._handler.ends == ["r1"]  # "other" had no hint
    sched.update_connector_output(done)
    sched.request_finished(request, [])
    _drain(sched)
    assert sched._handler.ends == ["r1"]


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


def test_an_engine_core_of_another_shape_is_left_alone(monkeypatch):
    import sys
    import types

    import vllm.v1.engine as engine_pkg

    import maru_vllm.connector as connector

    class EngineCore:
        def preprocess_add_request(self, request, priority):
            return (SimpleNamespace(request_id=request), 0)

    original = EngineCore.preprocess_add_request
    core = types.ModuleType("vllm.v1.engine.core")
    core.EngineCore = EngineCore
    monkeypatch.setitem(sys.modules, "vllm.v1.engine.core", core)
    monkeypatch.setattr(engine_pkg, "core", core, raising=False)
    monkeypatch.setattr(connector, "_ARRIVAL_HINTER", None)
    _scheduler(hits=5, maru_read_hint_on_arrival=True)
    assert EngineCore.preprocess_add_request is original


def test_the_scheduler_bounds_the_requests_it_keeps_hint_state_for(monkeypatch):
    import maru_vllm.connector as connector

    monkeypatch.setattr(connector, "_MAX_HINTED_REQUESTS", 2)
    sched = _scheduler(hits=5)
    for rid in ("r1", "r2", "r3"):
        sched._arrived[rid] = None
        sched._hint(rid, ["k"], arrival=True)
    _drain(sched)
    # The oldest went, and storage heard its end.
    assert list(sched._hinted) == ["r2", "r3"]
    assert set(sched._arrival_keys) <= {"r1", "r2", "r3"}
    assert sched._handler.ends == ["r1"]
    for rid in ("a1", "a2", "a3"):
        sched.request_arrived(_request(rid))
    assert len(sched._arrived) == 2


def test_storage_without_hints_costs_the_scheduler_nothing():
    sched = make_scheduler(4, 8, extra(maru_async_load=True))
    sched._handler = Handler(hits=5, hints_enabled=False)
    request = _request("r1")
    matched, load_async = sched.get_num_new_matched_tokens(request, 8)
    assert load_async and matched > 0
    sched.request_arrived(request)
    sched.request_finished(request, [])
    assert sched._handler.hints == [] and sched._handler.ends == []
    assert not sched._hinted and not sched._arrived and sched._hint_pool is None


def test_the_lookup_reuses_the_keys_an_arrival_hint_computed():
    sched = _scheduler(hits=5)
    request = _request("r1")
    sched.request_arrived(request)
    deadline = time.monotonic() + 5
    while not sched._handler.hints:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    seen = []
    sched._count_matched_chunks = lambda tokens, keys=None: seen.append(keys) or 5
    sched.get_num_new_matched_tokens(request, 8)
    assert seen == [sched._handler.hints[0][1]]
    sched.request_finished(request, [])
    assert not sched._arrival_keys


def test_arrival_hints_need_asynchronous_loading(monkeypatch):
    import maru_vllm.connector as connector

    installed, warnings = [], []
    monkeypatch.setattr(connector, "_install_arrival_hint", installed.append)
    monkeypatch.setattr(
        connector.logger, "warning", lambda msg, *a: warnings.append(msg % a)
    )
    make_scheduler(4, 8, extra(maru_read_hint_on_arrival=True))
    assert installed == []
    assert any("requires maru_async_load" in w for w in warnings)


def test_shutdown_stops_arrival_hints_and_creates_no_new_handler(monkeypatch):
    import maru_vllm.connector as connector

    sched = _scheduler(hits=5)
    closed = []
    sched._handler.close = lambda: closed.append(True)
    sched.request_arrived(_request("r1"))
    pool = sched._hint_pool
    assert pool is not None
    created = []
    monkeypatch.setattr(
        connector, "_create_maru_handler", lambda *a, **k: created.append(a)
    )
    sched.shutdown()
    assert closed == [True] and sched._handler is None
    assert sched._hint_pool is None and pool._shutdown
    # A request that arrives afterwards: no hint, no pool, no new handler.
    sched.request_arrived(_request("r2"))
    sched._ensure_handler()
    assert sched._hint_pool is None and sched._handler is None
    assert created == []


def test_the_cxl_backend_takes_read_hints_and_no_write_hints(monkeypatch):
    from maru_vllm.connector import _validate_storage_config

    seen = []

    class Recorder:
        def __init__(self, config):
            seen.append(config)

        def connect(self):
            return True

    monkeypatch.setattr(maru, "MaruHandler", Recorder)
    config = {
        "maru_storage_backend": "cxl",
        "maru_pool_size": "16M",
        "maru_read_hints": True,
        "maru_async_load": True,
    }
    assert _validate_storage_config(config) is False
    _create_maru_handler(config, metadata_only=True)
    _create_maru_handler(config, metadata_only=False)
    # Its hints go to handler plugins: the scheduler hints, the worker
    # promotes; write hints stay with remote_cxl.
    assert [c.read_hints for c in seen] == [True, False]
    assert [c.read_promotes for c in seen] == [False, True]
    assert [c.write_hints for c in seen] == [False, False]
    for knob in ("maru_write_hints", "maru_rdma_device"):
        with pytest.raises(ValueError, match="remote_cxl"):
            _validate_storage_config({**config, knob: True})


class StoredRpc:
    """MaruServer stand-in that holds ``stored`` keys, 2 MiB apart."""

    def __init__(self, stored):
        self.stored = list(stored)

    def batch_exists_kv(self, keys):
        from maru_common.protocol import BatchExistsKVResponse

        return BatchExistsKVResponse(results=[k in self.stored for k in keys])

    def batch_lookup_kv(self, keys):
        from maru_common.protocol import BatchLookupKVResponse, LookupResult
        from maru_shm.types import MaruHandle

        handle = MaruHandle(region_id=7, offset=0, length=1 << 30)
        return BatchLookupKVResponse(
            entries=[
                LookupResult(
                    found=True,
                    handle=handle,
                    kv_offset=self.stored.index(k) << 21,
                    kv_length=1 << 21,
                )
                if k in self.stored
                else LookupResult(found=False)
                for k in keys
            ]
        )

    def close(self):
        pass


@pytest.mark.parametrize("layerwise", [False, True], ids=["chunkwise", "layerwise"])
@pytest.mark.parametrize("suffixes", [("",), ("_h0-1", "_h1-2")], ids=["tp1", "tp2"])
def test_a_cxl_hint_plugin_gets_the_bytes_of_the_objects_the_workers_load(
    monkeypatch, layerwise, suffixes
):
    """The scheduler finds a hit by each TP slice's key (and, layerwise, its
    _DONE marker); the hint names the objects the workers load, so the
    plugin of a cxl handler hears where their bytes are."""
    from maru_common import MaruConfig
    from maru_handler import MaruHandler

    class HintPlugin:
        hint_actions = ("read_hint", "read_hint_end")

        def __init__(self):
            self.messages = []

        def on_memory_hint(self, handler, message):
            self.messages.append(message)

    request = _request("r1")
    keys = _chunk_keys(request.prompt_token_ids, 8)
    layers = (0, 1)
    loaded = {
        ci: [
            f"{keys[ci]}{s}_L{layer}" if layerwise else keys[ci] + s
            for layer in (layers if layerwise else (None,))
            for s in suffixes
        ]
        for ci in range(len(keys))
    }
    stored = [k for ci in loaded for k in loaded[ci]]
    if layerwise:
        stored += [f"{key}{s}_DONE" for key in keys for s in suffixes]

    plugin = HintPlugin()
    monkeypatch.setattr("maru_handler.handler.load_handler_plugins", lambda: [plugin])
    handler = MaruHandler(
        MaruConfig(
            auto_connect=False, metadata_only=True, read_hints=True, pool_size=16 << 20
        )
    )
    handler._rpc = StoredRpc(stored)
    handler._connected = True
    sched = make_scheduler(
        4,
        8,
        {
            "maru_storage_backend": "cxl",
            "maru_pool_size": "16M",
            "maru_read_hints": True,
            "maru_async_load": True,
            "maru_use_layerwise": layerwise,
        },
        shard_suffixes=suffixes,
        layer_indices=layers,
    )
    sched._handler = handler

    matched, load_async = sched.get_num_new_matched_tokens(request, 8)
    assert load_async and matched > 0
    _drain(sched)
    sched.shutdown()  # ends the open hint, then closes the plugins

    hint, end = plugin.messages
    assert (hint["action"], end["action"]) == ("read_hint", "read_hint_end")
    # Chunk 0 is computed; the load reads chunks 1-7, every slice (and
    # layer) of a chunk before the next chunk's.
    objects = [k for ci in range(1, 8) for k in loaded[ci]]
    assert [r["offset"] for r in hint["ranges"]] == [
        stored.index(k) << 21 for k in objects
    ]


def test_a_hint_of_several_kv_cache_groups_names_the_objects_the_hit_loads():
    from maru_vllm.kv_groups import (
        GROUP_FULL,
        GROUP_MAMBA,
        GROUP_SLIDING_WINDOW,
        KVGroup,
    )

    def group(gid, kind, **kw):
        return KVGroup(
            group_id=gid,
            kind=kind,
            spec_name="Spec",
            layer_names=(f"layers.{gid}.attn",),
            block_size=4,
            slot_bytes=8,
            **kw,
        )

    full = group(0, GROUP_FULL)
    mamba = group(1, GROUP_MAMBA)
    window = group(2, GROUP_SLIDING_WINDOW, sliding_window=8)
    sched = make_scheduler(
        4,
        8,
        extra(maru_async_load=True),
        kv_groups=[full, mamba, window],
        shard_suffixes=("_s0", "_s1"),
        state_suffixes=("_p0",),
    )
    keys = [f"kv{i}" for i in range(6)]

    def obj(g, ci, suffix):
        return g.object_key(keys[ci] + suffix, 8)

    # A hit of chunks 1-3: full attention every chunk, the window the last
    # chunk it covers, Mamba the last chunk's state; chunk by chunk.
    assert sched._hint_object_keys(keys, 1, 4) == [
        obj(full, 1, "_s0"),
        obj(full, 1, "_s1"),
        obj(full, 2, "_s0"),
        obj(full, 2, "_s1"),
        obj(full, 3, "_s0"),
        obj(full, 3, "_s1"),
        obj(mamba, 3, "_p0"),
        obj(window, 3, "_s0"),
        obj(window, 3, "_s1"),
    ]
    # Before the lookup the hit is unknown: the groups every hit chunk needs.
    assert sched._hint_object_keys(keys, 0, None) == [
        obj(full, ci, s) for ci in range(6) for s in ("_s0", "_s1")
    ]


def test_the_scheduler_names_layers_as_the_workers_do():
    from maru_vllm.connector import _scheduler_layer_indices
    from maru_vllm.kv_groups import GROUP_FULL, KVGroup

    names = ("model.layers.3.self_attn.attn", "model.layers.7.self_attn.attn", "x")
    one = KVGroup(
        group_id=0,
        kind=GROUP_FULL,
        spec_name="FullAttentionSpec",
        layer_names=names,
        block_size=4,
        slot_bytes=8,
    )
    # By the index in each name, else by position, as the worker keys them.
    assert _scheduler_layer_indices([one], None) == [3, 7, 2]
    # Without the groups, the model's layer count.
    config = SimpleNamespace(
        model_config=SimpleNamespace(get_num_layers=lambda parallel: 4),
        parallel_config=None,
    )
    assert _scheduler_layer_indices([], config) == [0, 1, 2, 3]
    assert _scheduler_layer_indices([], None) == []


@pytest.mark.parametrize("cpu_mode", [False, True], ids=["cxl", "storage-session"])
def test_a_group_load_promotes_its_request_before_its_first_read(cpu_mode):
    """Several KV cache groups load in _group_load_job; it promotes the
    hinted request before it reads, as the one-group load does."""
    import threading

    from maru_vllm.connector import MaruWorkerConnector

    worker = object.__new__(MaruWorkerConnector)
    calls = []
    worker._cpu_mode = cpu_mode
    worker._handler = SimpleNamespace(
        promote_read=lambda rid, **route: calls.append(("promote", rid, route))
    )
    worker._route = lambda meta: {"route_key": "k0"} if cpu_mode else {}
    worker._retrieve_group_objects = lambda meta: calls.append(("read",))
    worker._load_group_leased = lambda *a, **k: calls.append(("read",))
    worker._fail_load = lambda meta, exc: calls.append(("failed",))
    worker._async_load_stream = None
    worker._async_load_lock = threading.Lock()
    worker._inflight_async_load_req_ids = set()
    worker._abandoned_req_ids = set()
    meta = SimpleNamespace(req_id="r1")
    # The storage-session path goes on to CUDA, which this test has none of.
    worker._group_load_job(meta, None, None)
    route = {"route_key": "k0"} if cpu_mode else {}
    assert calls[0] == ("promote", "r1", route)
    assert calls[1] in {("read",), ("failed",)}
