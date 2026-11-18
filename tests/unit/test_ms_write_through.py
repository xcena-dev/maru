# SPDX-License-Identifier: Apache-2.0
"""Write-through (demotion "through"): a page that becomes READY in such a
tier is copied to the tier below at once (no finalizer, the source stays),
copies chain through tiers, the in-flight cap skips, and pressure later
only finalizes the source onto its copy."""

import collections
import shutil
import tempfile
import threading
import time

import pytest

from maru_common.storage_types import Key, PoolKind
from maru_storage_service.config import parse_node_config
from maru_storage_service.pool_manager import PageState
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import (
    PAGE,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)
from tests.unit.remote_fakes import reset_fake_agents
from tests.unit.test_ms_cxl_tier import FakeRm
from tests.unit.test_ms_demotion import SMALL_CREDIT, Regions, put, value_of

NS = "ns"


@pytest.fixture(autouse=True)
def _fresh_agents():
    reset_fake_agents()
    yield
    reset_fake_agents()


@pytest.fixture
def fake():
    server = FakeMaruServer(heartbeat_s=0.1)
    yield server
    server.close()


@pytest.fixture
def env(fake, monkeypatch, ssd_root):
    """Builds nodes from tier lists; closes them and their handlers."""
    from maru_common.config import MaruConfig
    from maru_handler import MaruHandler
    from maru_handler.client import pool_maps

    sockdir = short_tmpdir()
    ssd_dir = tempfile.mkdtemp(prefix="maru-through-", dir=ssd_root)
    regions = Regions()
    monkeypatch.setattr(pool_maps, "CxlRegionMapping", regions.worker_map)
    made, handlers = [], []

    def node(tiers: list[dict], **budgets):
        config = parse_node_config(
            {
                "node_id": "gpu-test",
                "socket": f"{sockdir}/node.sock",
                "server": fake.url,
                "page_size": PAGE,
                "tiers": tiers,
                "budgets": SMALL_CREDIT | {"transfer_reserve_bytes": PAGE} | budgets,
            }
        )
        n = start_node(
            config,
            region_source=FakeRm(),
            cxl_renew_s=0.05,
            map_region=regions.node_map,
        )
        made.append(n)
        return n

    def handler(n):
        h = MaruHandler(
            MaruConfig(
                maru_node=n.config.socket,
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
        h.connect()
        handlers.append(h)
        return h

    yield node, handler, ssd_dir
    for h in handlers:
        try:
            h.close()
        except Exception:
            pass
    for n in made:
        n.stop()
    regions.close()
    remove_tree(sockdir)
    shutil.rmtree(ssd_dir, ignore_errors=True)


def dram(**kw):
    return {
        "store": "dram",
        "size": 16 * PAGE,
        "hugepage": False,
        "high_watermark": 0.5,
        "low_watermark": 0.25,
        "demotion": "through",
    } | kw


def cxl(**kw):
    return {
        "store": "cxl",
        "size": 16 * PAGE,
        "region_size": 8 * PAGE,
        "rm": "127.0.0.1:1",
    } | kw


def ssd(path: str):
    return {"store": "ssd", "size": 32 * PAGE, "path": path}


def kinds_of(fake, key: str) -> list[PoolKind]:
    return sorted(loc.kind for loc in fake.locations(Key(NS, key)))


def test_a_written_page_is_copied_below_at_once(fake, env):
    node, handler, _ = env
    n = node([dram(), cxl()])
    h = handler(n)
    for i in range(3):
        assert put(h, f"k{i}")
    for i in range(3):
        wait_until(
            lambda i=i: kinds_of(fake, f"k{i}") == [PoolKind.CXL, PoolKind.DRAM],
            what="a copy in cxl",
        )
    stats = n.manager.through_results
    # The COMMIT answer is applied after the Catalog shows the copy
    wait_until(
        lambda: stats["COMMIT:ADDED"] >= 3 and len(n.manager._copies) >= 3,
        what="copies noted",
    )
    assert stats["COPIED"] >= 3 * len(value_of("k0"))
    assert n.manager.move_results["RETIRED"] == 0  # no pressure: nothing moved
    # The copy is the source's copy below, READY with the key's bytes
    assert len(n.manager._copies) >= 3
    for src, dest in n.manager._copies.items():
        assert n.pools.page(src).state == PageState.READY
        assert n.pools.page(dest).state == PageState.READY
    n.pools.check()


def test_pressure_only_finalizes_onto_the_copy(fake, env):
    node, handler, _ = env
    n = node([dram(), cxl()])
    h = handler(n)
    keys = [f"k{i}" for i in range(12)]
    for key in keys:
        assert put(h, key)
    wait_until(lambda: n.manager.move_results["RETIRED"] >= 3, what="pressure moved")
    moved = [k for k in keys if kinds_of(fake, k) == [PoolKind.CXL]]
    assert moved
    # Moved pages were finalized onto their copies: no demotion copy ran
    assert n.manager.move_results["COPY_FAILED"] == 0
    copies = n.manager.through_results["COMMIT:ADDED"]
    assert copies >= len(moved)
    for key in keys:
        assert fake.locations(Key(NS, key))  # nothing lost
    n.pools.check()


def test_copies_chain_through_tiers(fake, env):
    node, handler, ssd_dir = env
    n = node([dram(), cxl(demotion="through"), ssd(ssd_dir)])
    h = handler(n)
    for i in range(3):
        assert put(h, f"k{i}")
    for i in range(3):
        wait_until(
            lambda i=i: (
                kinds_of(fake, f"k{i}") == [PoolKind.CXL, PoolKind.DRAM, PoolKind.SSD]
            ),
            what="copies in cxl and ssd",
        )
    wait_until(lambda: n.manager._writing == 0, what="writes ended")
    n.pools.check()


def test_the_in_flight_cap_skips_copies(fake, env, monkeypatch):
    node, handler, ssd_dir = env
    n = node([dram(through_max_inflight=PAGE), ssd(ssd_dir)])
    tier = n.tier("ssd")
    real = tier.submit_store
    held = []

    def hold(*args, **kwargs):  # writes never end while held
        held.append((args, kwargs))
        raise RuntimeError("held")

    monkeypatch.setattr(tier, "submit_store", hold)
    h = handler(n)
    for i in range(4):
        assert put(h, f"k{i}")
    wait_until(
        lambda: n.manager.through_results["COPY_FAILED"] >= 1, what="a copy tried"
    )
    monkeypatch.setattr(tier, "submit_store", real)
    stats = n.manager.through_results
    assert stats["COPY_FAILED"] + stats["SKIPPED"] + stats["COMMIT:ADDED"] >= 1
    assert n.manager._through_inflight["dram"] == 0  # every copy accounted


def test_a_skipped_page_is_demoted_back_later(fake, env):
    node, handler, _ = env
    n = node([dram(through_max_inflight=PAGE), cxl()])
    n.manager._through_inflight["dram"] = PAGE  # the cap is reached
    h = handler(n)
    keys = [f"k{i}" for i in range(12)]
    for key in keys:
        assert put(h, key)
    wait_until(lambda: n.manager.move_results["RETIRED"] >= 3, what="pressure moved")
    assert n.manager.through_results["SKIPPED"] >= 1
    assert n.manager.through_results["COMMIT:ADDED"] == 0
    assert n.manager.move_results["COMMIT:ADDED"] >= 3  # back demotion copied
    n.pools.check()


def test_pressure_never_writes_a_source_whose_copy_is_in_flight(fake, env, monkeypatch):
    node, handler, ssd_dir = env
    n = node([dram(through_max_inflight=PAGE), ssd(ssd_dir)])
    # Room for one copy in flight: a demotion write slot stays free
    n.manager._through_inflight["dram"] = PAGE - len(value_of("k0"))
    tier = n.tier("ssd")
    real = tier.submit_store
    gate = threading.Event()
    writes: collections.Counter = collections.Counter()

    def slow(src, dst, *, refs, callback=None, **kwargs):
        first = not writes
        writes[refs[0]] += 1
        if not first:
            return real(src, dst, refs=refs, callback=callback, **kwargs)

        def later(result):  # the first copy stays in flight until the gate
            threading.Thread(target=lambda: (gate.wait(10), callback(result))).start()

        return real(src, dst, refs=refs, callback=later, **kwargs)

    monkeypatch.setattr(tier, "submit_store", slow)
    h = handler(n)
    keys = [f"k{i}" for i in range(10)]  # past the high watermark
    for key in keys:
        assert put(h, key)
    wait_until(lambda: n.manager.move_results["RETIRED"] >= 3, what="pressure moved")
    time.sleep(0.3)  # more eviction passes under pressure meanwhile
    assert n.manager.through_results["SKIPPED"] >= 1
    assert max(writes.values()) == 1
    gate.set()
    wait_until(lambda: not n.manager._through_active, what="copies settled")
    assert max(writes.values()) == 1  # also the copied source: finalized
    assert n.manager.move_results["COPY_FAILED"] == 0
    for key in keys:
        assert fake.locations(Key(NS, key))
    n.pools.check()
