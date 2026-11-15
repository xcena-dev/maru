# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""A fake L2 store registered under its own store name runs a [dram, fake]
node end to end with no service change: DRAM pressure stores pages through
submit_store (COMMIT with a finalizer, FINALIZE_MOVE), a FETCH loads one back
through submit_load, a late store counts as a failure but is published, a
late load is a hit, a failed store publishes nothing, and stop drains the
tier before it detaches and closes. FETCH needs the real MaruServer (the
fake one has no SOURCE_PIN or FETCH_COMPLETE)."""

import dataclasses
import errno
import logging
import socket
import threading
import time

import pytest

from maru_common.config import MaruConfig
from maru_common.protocol import MessageType
from maru_common.storage_types import ItemResult, Key, Location, PoolKind
from maru_handler import MaruHandler
from maru_server.node_rpc import NodeRpcServer
from maru_server.node_server import NodeServer
from maru_storage_service import service as service_module
from maru_storage_service.config import parse_node_config
from maru_storage_service.config_fields import TierSpec
from maru_storage_service.drivers.registry import TIERS
from maru_storage_service.page_table import PageRef
from maru_storage_service.pool_manager import PageState
from maru_storage_service.service import StorageService
from maru_storage_service.tiers import Level
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import (
    PAGE,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)
from tests.unit.test_ms_demotion import SMALL_CREDIT, finalized, kinds, put, value_of
from tests.unit.test_ms_fetch import Loader, locations, settled
from tests.unit.test_ms_ssd_tier import _free_port, no_io
from tests.unit.test_ms_tier_plugin import StoreL2

NS = "ns"
WAIT = 5
STORE = "fake"
KEYS = frozenset({"store", "size", "demotion", "high_watermark", "low_watermark"})


class FlowL2(StoreL2):
    """StoreL2 with its pool's path (MaruServer checks the pool id by it),
    recording what it was given. ``fail_stores``: that many next stores
    write half their bytes, close the gate and fail. ``late_stores`` and
    ``late_loads``: every byte moves, past the deadline."""

    def __init__(self, tier, env):
        super().__init__(tier, env)
        self.info = dataclasses.replace(self.info, path="/fake")
        self.stores: list[Location] = []
        self.loads: list[Location] = []
        self.results: list = []
        self.fail_stores = 0
        self.failed: list[PageRef] = []
        self.late_stores = False
        self.late_loads = False
        self.draining = threading.Event()
        self.mapped_at_detach: dict[str, bool] = {}

    def submit_store(self, src, dst, *, refs, callback=None, deadline=None):
        self.stores.append(dst)
        if self.fail_stores:
            self.fail_stores -= 1
            self.failed.append(refs[0])

            def torn() -> None:
                view = self.memory[src.pool_id]
                half = src.offset + src.nbytes // 2
                self.pages[dst.offset] = bytes(view[src.offset : half])
                self.gate.clear()  # later transfers wait
                raise OSError(errno.EIO, "injected")

            return self._submit(torn, refs, callback, deadline)
        if self.late_stores:
            deadline = time.monotonic() - 1
        return super().submit_store(
            src, dst, refs=refs, callback=callback, deadline=deadline
        )

    def submit_load(self, src, dst, *, refs, callback=None, deadline=None):
        self.loads.append(src)
        if self.late_loads:
            deadline = time.monotonic() - 1
        return super().submit_load(
            src, dst, refs=refs, callback=callback, deadline=deadline
        )

    def _submit(self, work, refs, callback, deadline):
        result = super()._submit(work, refs, callback, deadline)
        self.results.append(result)
        return result

    def drain(self) -> None:
        self.draining.set()
        super().drain()

    def detach(self, pool_id: str) -> None:
        self.mapped_at_detach[pool_id] = not self.memory[pool_id].closed
        super().detach(pool_id)


def flow_config(url: str, sockdir: str):
    return parse_node_config(
        {
            "node_id": "gpu-test",
            "socket": f"{sockdir}/node.sock",
            "server": url,
            "page_size": PAGE,
            "tiers": [
                {
                    "store": "dram",
                    "size": 16 * PAGE,
                    "hugepage": False,
                    "high_watermark": 0.5,
                    "low_watermark": 0.25,
                },
                {"store": STORE, "size": 32 * PAGE},
            ],
            "budgets": SMALL_CREDIT | {"transfer_reserve_bytes": PAGE},
        }
    )


def accepting(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
        return True
    except OSError:
        return False


def dram_id(node: StorageService) -> str:
    (pool_id,) = [p for p in node.pools.pool_ids() if node.pools.tier_of(p) == "dram"]
    return pool_id


def commits_into(fake, pool_id: str) -> list:
    return [
        i
        for r in fake.requests(MessageType.COMMIT)
        for i in r.items
        if i.location.pool_id == pool_id
    ]


def put_all(handler, count: int) -> list[str]:
    names = [f"k{i}" for i in range(count)]
    for name in names:
        assert put(handler, name)
    return names


@pytest.fixture
def fake():
    server = FakeMaruServer(heartbeat_s=0.1)
    yield server
    server.close()


@pytest.fixture
def real_server():
    server = NodeServer(session_ttl_s=6.0, heartbeat_s=0.3)
    port = _free_port()
    rpc = NodeRpcServer(server, host="127.0.0.1", port=port, num_workers=2)
    thread = threading.Thread(target=rpc.start, daemon=True)
    thread.start()
    wait_until(lambda: accepting(port), what="MaruServer listening")
    yield server, f"tcp://127.0.0.1:{port}"
    rpc.stop()
    thread.join(5)
    server.close()


@pytest.fixture
def sockdir():
    path = short_tmpdir()
    yield path
    remove_tree(path)


@pytest.fixture
def nodes(monkeypatch, sockdir):
    """Starts a [dram, fake] node: the store is only a registry entry."""
    made: list[FlowL2] = []

    def make_tier(tier, env):
        made.append(FlowL2(tier, env))
        return made[-1]

    spec = TierSpec(STORE, Level.L2, True, PoolKind.SSD, KEYS, make=make_tier)
    monkeypatch.setitem(TIERS, STORE, spec)
    monkeypatch.setattr(service_module, "SERVED_TIERS", service_module._served_tiers())
    started = []

    def make(url: str):
        node = start_node(flow_config(url, sockdir))
        started.append((node, made[-1]))
        return started[-1]

    yield make
    for node, tier in started:
        tier.gate.set()
        node.stop()


@pytest.fixture
def handlers():
    made = []

    def make(node):
        handler = MaruHandler(
            MaruConfig(
                maru_node=node.config.socket,
                engine_id="eng",
                cache_namespace=NS,
                chunk_size_bytes=PAGE,
                pool_size=PAGE,
                pin_pools=False,
                timeout_ms=300,
                auto_connect=False,
                store_timeout_s=10.0,
            )
        )
        handler.connect()
        made.append(handler)
        return handler

    yield make
    for handler in made:
        try:
            handler.close()
        except Exception:
            pass


# =============================================================================
# Demotion into the fake pool (FakeMaruServer)
# =============================================================================


def test_dram_pressure_stores_pages_into_the_fake_pool(fake, nodes, handlers):
    node, tier = nodes(fake.url)
    pool, dram = tier.info.pool_id, dram_id(node)
    names = put_all(handlers(node), 10)
    wait_until(lambda: node.manager.move_results["RETIRED"] >= 3, what="demoted")
    moved = [n for n in names if kinds(fake, n) == {PoolKind.SSD}]
    assert len(moved) >= 3
    for name in moved:
        (location,) = fake.locations(Key(NS, name))
        assert location.pool_id == pool
        assert tier.pages[location.offset] == value_of(name)
        ref = node.pools.find(location)
        assert ref is not None and node.pools.page(ref).state == PageState.READY
    # Each one stored through the tier, COMMITted with a finalizer, then
    # FINALIZE_MOVEd off DRAM
    finals = finalized(fake)
    assert {(i.source.pool_id, i.dest.pool_id) for i in finals} == {(dram, pool)}
    stored = {(d.pool_id, d.offset) for d in tier.stores}
    assert {(i.dest.pool_id, i.dest.offset) for i in finals} <= stored
    commits = commits_into(fake, pool)
    assert len(commits) >= len(moved) and all(i.finalizer for i in commits)
    assert not node.manager.move_results["COPY_FAILED"]
    wait_until(lambda: no_io(node), what="every store ended")
    node.pools.check()


def test_a_late_store_counts_as_a_failure_yet_is_published(
    fake, nodes, handlers, monkeypatch, caplog
):
    monkeypatch.setattr("maru_storage_service.storage_manager.IO_PAUSE_S", 60.0)
    caplog.set_level(logging.ERROR, logger="maru_storage_service.storage_manager")
    node, tier = nodes(fake.url)
    pool = tier.info.pool_id
    tier.late_stores = True
    put_all(handlers(node), 12)
    # Three in a row pause the pool, as failed writes do
    wait_until(lambda: pool in node.manager._io_paused, what="the pool paused")
    assert "past its deadline" in caplog.text

    def published() -> bool:
        dests = {(i.dest.pool_id, i.dest.offset) for i in finalized(fake)}
        return {(d.pool_id, d.offset) for d in tier.stores} <= dests

    # Every byte moved, so every store is still COMMITted and FINALIZE_MOVEd
    wait_until(published, what="every late store published")
    assert len(tier.stores) >= 3
    assert all(r.ok and r.late for r in tier.results)
    for item in finalized(fake):
        assert fake.locations(item.key) == [item.dest]
        assert tier.pages[item.dest.offset] == value_of(item.key.key)
    assert not node.manager.move_results["COPY_FAILED"]
    node.pools.check()


def test_a_failed_store_publishes_nothing_and_leaves_the_source(fake, nodes, handlers):
    node, tier = nodes(fake.url)
    pool, dram = tier.info.pool_id, dram_id(node)
    tier.fail_stores = 1
    names = put_all(handlers(node), 10)
    wait_until(
        lambda: node.manager.move_results["COPY_FAILED"] == 1, what="the failure"
    )
    # Later stores wait at the gate: the source is READY, still its key's only
    # location with its bytes, and nothing of the fake pool is published
    (src,) = tier.failed
    page = node.pools.page(src)
    assert page is not None and page.state == PageState.READY
    assert page.location.pool_id == dram
    assert fake.locations(page.key) == [page.location]
    start = page.location.offset
    data = tier.memory[dram][start : start + page.location.length]
    assert data == value_of(page.key.key)
    assert not commits_into(fake, pool)
    tier.gate.set()
    wait_until(lambda: node.manager.move_results["RETIRED"] >= 3, what="moved")
    assert node.manager.move_results["COPY_FAILED"] == 1
    assert not node.manager.move_results["DROPPED"]
    # The torn bytes never count: every key keeps a location, and each one in
    # the fake pool holds its key's bytes
    for name in names:
        held = fake.locations(Key(NS, name))
        assert held, name
        for location in held:
            if location.pool_id == pool:
                assert tier.pages[location.offset] == value_of(name), name
    node.pools.check()


def test_stop_drains_the_tier_before_it_detaches_and_closes(fake, nodes, handlers):
    node, tier = nodes(fake.url)
    dram = dram_id(node)
    tier.gate.clear()
    put_all(handlers(node), 10)
    assert tier.entered.wait(WAIT)  # a store under way, held at the gate
    mapping = tier.memory[dram]
    stopping = threading.Thread(target=node.stop)
    stopping.start()
    try:
        assert tier.draining.wait(WAIT)
        # The drain waits for it: nothing detached, stopped or closed yet
        assert not tier.mapped_at_detach and not mapping.closed
        assert not {"drained", "stop", "close"} & set(tier.events)
    finally:
        tier.gate.set()
        stopping.join(WAIT)
    assert not stopping.is_alive() and node.teardown_failed is None
    running, *queued = tier.results
    assert running.ok and all(r.cancelled and not r.ok for r in queued)
    assert all(r.done.is_set() for r in tier.results)
    steps = ["drained", "stop", f"detach {dram}", "close"]
    assert sorted(steps, key=tier.events.index) == steps
    assert tier.events.count("close") == 1
    assert tier.mapped_at_detach == {dram: True} and mapping.closed


# =============================================================================
# FETCH through the fake pool (real MaruServer)
# =============================================================================


@pytest.fixture
def moved(real_server, nodes, handlers):
    """A node whose first keys moved to the fake pool."""
    server, url = real_server
    node, tier = nodes(url)
    pool = tier.info.pool_id

    def in_pool() -> list[str]:
        return [
            n
            for n in names
            if (held := locations(server, n)) and all(h.pool_id == pool for h in held)
        ]

    h = handlers(node)
    names = put_all(h, 12)
    wait_until(lambda: len(in_pool()) >= 3, what="moved to the fake pool")
    h.close()
    return server, node, tier, in_pool()


def test_a_fetch_loads_the_page_back_through_the_tier(moved):
    server, node, tier, names = moved
    name = names[0]
    (source,) = locations(server, name)
    assert source.pool_id == tier.info.pool_id and source.kind == PoolKind.SSD
    loader = Loader(node, server)
    try:
        (item,) = loader.items(name)  # LOOKUP answers FETCH
        assert item.plan.source == source
        assert item.plan.source_node == node.config.node_id
        (outcome,) = loader.send(loader.request([item])).result(10).items
        assert outcome.result == ItemResult.HIT
        assert outcome.location.pool_id == dram_id(node)
        assert loader.read(outcome.location) == value_of(name)
        assert tier.loads == [source]
        assert node.manager.fetches.results["reads"] == 1
        loader.unpin(outcome.pin_id)
        wait_until(lambda: settled(node, server), what="the fetch settled")
        node.pools.check()
    finally:
        loader.close()


def test_a_late_load_is_still_a_hit(moved):
    server, node, tier, names = moved
    tier.late_loads = True
    name = names[0]
    loader = Loader(node, server)
    try:
        (outcome,) = loader.fetch(name).items
        assert outcome.result == ItemResult.HIT
        assert loader.read(outcome.location) == value_of(name)
        loads = [r for r in tier.results if r.src.pool_id == tier.info.pool_id]
        assert [(r.ok, r.late) for r in loads] == [(True, True)]
        assert node.manager.fetches.results["reads"] == 1
        loader.unpin(outcome.pin_id)
        wait_until(lambda: settled(node, server), what="the fetch settled")
        node.pools.check()
    finally:
        loader.close()
