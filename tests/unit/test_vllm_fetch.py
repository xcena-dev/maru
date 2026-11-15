# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Parked loads on a node with an SSD tier: the worker's loader thread
FETCHes what only the SSD holds, within its window, and copies it as a
memory hit; the scheduler probe counts such a key as found; the engine
thread never sends or waits for a FETCH. A real MaruServer in process; the
loads run on CUDA."""

import concurrent.futures
import shutil
import socket
import tempfile
import threading
import time

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import torch

import maru_vllm.connector as connector_module
from maru_common.config import MaruConfig
from maru_common.storage_types import Key, PoolKind
from maru_handler import MaruHandler
from maru_handler.client.maru_client import MaruClient
from maru_server.node_rpc import NodeRpcServer
from maru_server.node_server import NodeServer
from maru_storage_service.pool_manager import PageState
from maru_vllm.connector import MaruConnectorMetadata
from tests.unit.ms_node_helpers import (
    PAGE,
    node_config,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)
from tests.unit.test_vllm_node import (
    LAYER_NAMES,
    assert_external_on_cuda,
    external_meta,
    finished,
    forward_of,
    node_extra,
    rank_kv,
    reads_held,
    sentinel_kv,
    store,
)
from tests.unit.test_vllm_tp import HEAD_SIZE, NUM_LAYERS
from tests.unit.vllm_connector_helpers import make_worker

needs_cuda = pytest.mark.skipif(not torch.cuda.is_available(), reason="needs CUDA")
CREDIT = {
    "credit_max_bytes": 16 * PAGE,
    "credit_total_max_bytes": 32 * PAGE,
    "credit_refill_low": 8 * PAGE,
    "credit_refill_high": 16 * PAGE,
    "commit_pending_max_bytes": 16 * PAGE,
}


@pytest.fixture(autouse=True)
def no_cached_defaults(monkeypatch):
    monkeypatch.setattr(connector_module, "_node_transfer_cache", {})


@pytest.fixture
def real_server():
    server = NodeServer(session_ttl_s=6.0, heartbeat_s=0.3)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    rpc = NodeRpcServer(server, host="127.0.0.1", port=port, num_workers=2)
    thread = threading.Thread(target=rpc.start, daemon=True)
    thread.start()
    time.sleep(0.2)
    yield server, f"tcp://127.0.0.1:{port}"
    rpc.stop()
    thread.join(5)
    server.close()


@pytest.fixture
def nodes(real_server):
    sockdir = short_tmpdir()
    ssd_dir = tempfile.mkdtemp(prefix="maru-vllm-fetch-", dir="/var/tmp")
    made = []

    def make(**budgets):
        config = node_config(
            real_server[1],
            f"{sockdir}/node.sock",
            pages=64,
            budgets=CREDIT | budgets,
            ssd_path=ssd_dir,
            ssd_pages=64,
        )
        node = start_node(config, host_boot_id="boot-1")
        made.append(node)
        return node

    yield make
    for node in made:
        node.stop()
    remove_tree(sockdir)
    shutil.rmtree(ssd_dir, ignore_errors=True)


@pytest.fixture
def workers():
    made = []

    def make(node, **extra):
        worker = make_worker(
            4, 8, node_extra(node.config.socket, **extra), head_size=HEAD_SIZE
        )
        worker._num_layers = NUM_LAYERS
        worker._page_size_bytes = 2 * 8 * 2 * HEAD_SIZE * 4 * NUM_LAYERS
        worker._take_node_handler(wait=True)
        assert worker._handler is not None
        made.append(worker)
        return worker

    yield make
    for worker in made:
        worker.shutdown()


def ssd_only(server) -> list[Key]:
    service = server.ms_service
    with service.lock:
        out = []
        for key in list(service.catalog._by_key):
            live = list(service.catalog.live_locations(key))
            if live and all(e.location.kind == PoolKind.SSD for e in live):
                out.append(key)
        return out


def stored_on_ssd(server, node, workers):
    """A writer's two chunks, moved down to the SSD (a demand for all of
    DRAM evicts them)."""
    writer = workers(node)
    kv = rank_kv(1, 2)
    store(writer, kv)
    dram = node.tier("dram").pool.pool_id
    node.manager.want_room(PoolKind.DRAM, node.tier("dram").pool.capacity)
    wait_until(
        lambda: node.pools.bytes_in(PageState.READY, dram) == 0, what="moved down"
    )
    wait_until(lambda: len(ssd_only(server)) == 2, what="only on the SSD")
    return kv


def parked_reader(node, workers):
    reader = workers(node, maru_async_load=True)
    dst = [layer.cuda() for layer in sentinel_kv()]
    reader._kv_caches = dict(zip(LAYER_NAMES, dst, strict=True))
    return reader, dst


def settled(node) -> bool:
    fetches = node.manager.fetches
    return fetches.flights() == 0 and fetches.inflight_bytes == 0


@needs_cuda
def test_a_parked_load_fetches_what_only_the_ssd_holds(real_server, nodes, workers):
    server, _ = real_server
    node = nodes()
    kv = stored_on_ssd(server, node, workers)
    reader, dst = parked_reader(node, workers)
    client = reader._handler._cpu
    assert client.retrieve_capacity() is None  # the engine thread fetches nothing
    reader.start_load_kv(
        forward_of(dst), MaruConnectorMetadata(requests=[external_meta(True)])
    )
    finished(reader)
    assert_external_on_cuda(reader, dst, kv)
    assert client.stats["fetch_hit"] == 2
    assert node.manager.fetches.results["reads"] == 2
    wait_until(lambda: settled(node), what="the fetches ended")
    # its pins went with its reads; the copies stay in DRAM as cache
    service = server.ms_service
    with service.lock:
        assert service.pins.held_by(client._server.session) == []
    assert not ssd_only(server)
    node.pools.check()


@needs_cuda
def test_the_window_bounds_the_fetched_reads(real_server, nodes, workers, monkeypatch):
    """A window of one page: the load goes in batches of one key."""
    server, _ = real_server
    node = nodes(fetch_request_max_bytes=PAGE, fetch_max_inflight_bytes=2 * PAGE)
    kv = stored_on_ssd(server, node, workers)
    reader, dst = parked_reader(node, workers)
    sizes = []
    fetch = MaruClient._fetch

    def counted(self, scope, items, max_bytes):
        with self._lock:
            sizes.append((len(items), self._fetch_used()))
        return fetch(self, scope, items, max_bytes)

    monkeypatch.setattr(MaruClient, "_fetch", counted)
    reader.start_load_kv(
        forward_of(dst), MaruConnectorMetadata(requests=[external_meta(True)])
    )
    finished(reader)
    assert_external_on_cuda(reader, dst, kv)
    # Each FETCH holds only its own slot: the first read went back before
    assert sizes == [(1, 1), (1, 1)]


def test_an_inline_load_never_fetches(real_server, nodes, workers):
    """On the engine thread a key only the SSD holds is a miss: recomputed."""
    server, _ = real_server
    node = nodes()
    stored_on_ssd(server, node, workers)
    reader = workers(node, maru_async_load=True)
    dst = sentinel_kv()
    reader.start_load_kv(
        forward_of(dst), MaruConnectorMetadata(requests=[external_meta(False)])
    )
    assert reader.take_failed_load_blocks() == {1, 2}
    assert not node.manager.fetches.results["reads"]
    assert reads_held(reader) == 0


@pytest.mark.parametrize("fetch_loads", [True, False])
def test_the_scheduler_probe_counts_a_fetch_only_for_parked_loads(
    real_server, nodes, workers, fetch_loads
):
    server, _ = real_server
    node = nodes()
    stored_on_ssd(server, node, workers)
    extra = node_extra(node.config.socket, maru_async_load=fetch_loads)
    made = connector_module._create_node_handler(extra, True, False)
    try:
        assert made._cpu.config.fetch_loads is fetch_loads
    finally:
        made.close()
    probe = MaruHandler(
        MaruConfig(
            maru_node=node.config.socket,
            engine_id="eng",
            cache_namespace="ns",
            metadata_only=True,
            auto_connect=False,
            fetch_loads=fetch_loads,
        )
    )
    probe.connect()
    try:
        keys = [k.key for k in ssd_only(server)]
        assert probe.batch_exists(keys) == [fetch_loads] * len(keys)
    finally:
        probe.close()


@pytest.mark.parametrize(
    "mode",
    [{}, {"maru_use_layerwise": True}, {"maru_overlap_load_with_compute": True}],
)
def test_the_probe_counts_a_fetch_only_for_leased_loads(mode):
    """A layerwise or overlap load reads its prefix at once, so a prefix past
    the window would miss: its probe counts no FETCH. Workers still fetch;
    the deadline knobs reach both."""
    extra = node_extra(
        "/nonexistent/node.sock",
        maru_async_load=True,
        maru_fetch_deadline_ms=400,
        maru_fetch_deadline_per_page_ms=0.5,
        **mode,
    )
    for metadata_only in (True, False):
        made = connector_module._create_node_handler(extra, metadata_only, False)
        try:
            config = made._cpu.config
            assert config.fetch_loads is not (metadata_only and bool(mode))
            assert config.fetch_deadline_ms == 400
            assert config.fetch_deadline_per_page_ms == 0.5
        finally:
            made.close()


def test_an_unset_per_page_fetch_deadline_is_left_to_the_node_page():
    extra = node_extra("/nonexistent/node.sock", maru_async_load=True)
    made = connector_module._create_node_handler(extra, False, False)
    try:
        assert made._cpu.config.fetch_deadline_per_page_ms is None
    finally:
        made.close()


@needs_cuda
def test_the_engine_thread_never_waits_for_a_fetch(
    real_server, nodes, workers, monkeypatch
):
    """Calls that block, made on the engine thread while a parked load
    fetches: no synchronize, no Future.result, no join."""
    server, _ = real_server
    node = nodes()
    kv = stored_on_ssd(server, node, workers)
    reader, dst = parked_reader(node, workers)
    engine = threading.get_ident()
    blocking = []

    def spy(owner, name):
        original = getattr(owner, name)

        def call(*args, **kwargs):
            if threading.get_ident() == engine:
                blocking.append(f"{owner.__name__}.{name}")
            return original(*args, **kwargs)

        monkeypatch.setattr(owner, name, call)

    spy(torch.cuda.Stream, "synchronize")
    spy(torch.cuda.Event, "synchronize")
    spy(concurrent.futures.Future, "result")
    spy(threading.Thread, "join")
    synchronize = torch.cuda.synchronize
    monkeypatch.setattr(
        torch.cuda,
        "synchronize",
        lambda *a: (
            blocking.append("torch.cuda.synchronize")
            if threading.get_ident() == engine
            else synchronize(*a)
        ),
    )
    reader.start_load_kv(
        forward_of(dst), MaruConnectorMetadata(requests=[external_meta(True)])
    )
    seen: set[str] = set()

    def done() -> bool:
        seen.update(reader.get_finished_loading() or ())
        return "r1" in seen

    wait_until(done, what="r1 loaded")
    assert blocking == []
    assert reader._handler._cpu.stats["fetch_hit"] == 2
    monkeypatch.undo()
    assert_external_on_cuda(reader, dst, kv)
