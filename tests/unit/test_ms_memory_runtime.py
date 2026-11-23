# SPDX-License-Identifier: Apache-2.0
"""Memory-node recovery, Catalog reads and LRU eviction."""

import dataclasses
import mmap

from maru_common.protocol import NodeHeartbeatResponse, RecoverItem
from maru_common.remote_protocol import TransferEnd
from maru_common.storage_types import (
    AccessHints,
    CloseMode,
    ItemResult,
    Key,
    Status,
    Visibility,
)
from maru_storage_service.config import TierConfig
from maru_storage_service.pool_manager import PageState
from tests.unit.ms_node_helpers import PAGE, FakeRm, MetadataServer, wait_until
from tests.unit.test_ms_memory_role import DOMAIN
from tests.unit.test_ms_memory_role import memory as memory
from tests.unit.test_ms_peer_ops import fail_fence, fence, state


def test_lru_uses_catalog_read_hints_and_retires_before_reuse(memory, monkeypatch):
    node = memory.node
    node.store.placement.high, node.store.placement.low = 0.75, 0.5
    evict = node.store._evict_once
    monkeypatch.setattr(node.store, "_evict_once", lambda: None)
    offered = []
    for i in range(13):
        credits = memory.reserve().credits
        assert memory.write(credits, key=f"k{i}-").items[0].result == ItemResult.ADDED
        offered.extend(credits)
    # Reading the oldest page makes it recent; an invalid generation is ignored.
    first = offered[0]
    hints = AccessHints.pack(
        [
            (first.pool_id, first.offset, first.generation),
            (offered[1].pool_id, offered[1].offset, 0),
        ]
    )
    node.store.on_heartbeat(NodeHeartbeatResponse(Status.OK, access=hints))
    evict()
    assert Key("ns", "k0-0") in memory.fake.keys()
    assert Key("ns", "k1-0") not in memory.fake.keys()
    assert len(memory.fake.keys()) == 8
    assert node.store.retire_results["RETIRED"] == 5
    assert node.pools.bytes_in(PageState.READY) == 8 * PAGE
    node.pools.check()


def test_lru_keeps_catalog_pins_and_only_unconfirmed_writes_are_quarantined(
    memory, monkeypatch
):
    memory.fake.preserve(Key("ns", "kept0"))
    ready = memory.reserve().credits
    assert memory.write(ready, key="kept").items[0].result == ItemResult.ADDED
    pending = memory.reserve().credits
    with monkeypatch.context() as patch:
        fail_fence(memory, patch)
        memory.write(pending, end=TransferEnd.UNKNOWN)
        assert state(memory, ready[0]) == PageState.READY
        assert state(memory, pending[0]) == PageState.QUARANTINED
        memory.node.store.placement.high, memory.node.store.placement.low = 0.01, 0.001
        memory.node.store._evict_once()
        assert state(memory, ready[0]) == PageState.READY
        assert memory.node.store.retire_results["KEPT:PINNED"] == 1
    assert fence(memory).result == "success"


def test_local_cxl_restart_adopts_committed_bytes_and_reclaims_q2_after_revocation(
    memory, monkeypatch
):
    memory.node.stop()
    rm = FakeRm()
    backing = open(f"{memory.directory}/region", "w+b")
    backing.truncate(16 * PAGE)

    def map_pool(pool):
        return mmap.mmap(backing.fileno(), pool.capacity)

    memory.config = dataclasses.replace(
        memory.config,
        tiers=(
            TierConfig(
                store="local_cxl",
                size=16 * PAGE,
                region_size=16 * PAGE,
                rm="127.0.0.1:1",
                visibility=Visibility.COHERENT,
                high_watermark=0.9,
                low_watermark=0.8,
            ),
        ),
        budgets=dataclasses.replace(
            memory.config.budgets, transfer_reserve_bytes={"local_cxl": PAGE}
        ),
    )
    node = memory.start(region_source=rm, map_region=map_pool)
    memory.node, memory.parts, memory.ops = node, node.role.parts, node.role.ops
    offered = memory.reserve(2).credits
    pool = memory.parts._maps[offered[0].pool_id]
    pool[offered[0].offset : offered[0].offset + PAGE] = bytes([5]) * PAGE
    assert memory.write(offered[:1]).items[0].result == ItemResult.ADDED
    with monkeypatch.context() as patch:
        fail_fence(memory, patch)
        memory.write(offered[1:], end=TransferEnd.UNKNOWN)
        assert state(memory, offered[1]) == PageState.QUARANTINED
    old_name = memory.parts.name
    old_writer = memory.parts._writers[DOMAIN].transport
    node.stop(CloseMode.RESTART)
    assert node.teardown_failed is None and old_writer.agent.regs == []
    committed = memory.fake.locations(Key("ns", "k0"))[0]
    info = node.pools.pool_info(committed.pool_id)
    memory.fake.set_adopted(
        node.config.node_id,
        [(info, [RecoverItem(Key("ns", "k0"), committed, PAGE, False)])],
    )
    replacement = memory.start(region_source=rm, map_region=map_pool)
    try:
        assert replacement.adopted == [info.pool_id]
        assert replacement.role.parts.name != old_name
        assert replacement.pools.bytes_in(PageState.READY) == PAGE
        assert replacement.pools.bytes_in(PageState.QUARANTINED) == 0
        assert replacement.pools.bytes_in(PageState.FREE) == 15 * PAGE
        assert (
            replacement.role.parts._maps[info.pool_id][
                committed.offset : committed.offset + PAGE
            ]
            == bytes([5]) * PAGE
        )
        replacement.pools.check()
    finally:
        replacement.stop()
        backing.close()


def test_recovery_waits_for_an_earlier_registration_holder_without_a_time_release(
    memory,
):
    memory.node.stop()
    rm = FakeRm()
    region = rm.region(16 * PAGE)
    rm.regions[memory.config.node_id] = [region]
    rm.hold(region, "write-registration@old", "old")
    memory.config = dataclasses.replace(
        memory.config,
        tiers=(
            TierConfig(
                store="local_cxl",
                size=16 * PAGE,
                region_size=16 * PAGE,
                rm="127.0.0.1:1",
                visibility=Visibility.COHERENT,
                high_watermark=0.9,
                low_watermark=0.8,
            ),
        ),
        budgets=dataclasses.replace(
            memory.config.budgets, transfer_reserve_bytes={"local_cxl": PAGE}
        ),
    )
    node = memory.start(
        region_source=rm,
        map_region=lambda pool: mmap.mmap(-1, pool.capacity),
        cxl_renew_s=0.01,
    )
    (pool_id,) = node.pools.pool_ids()
    assert node.pools.bytes_in(PageState.QUARANTINED) == 16 * PAGE
    wait_until(lambda: rm.calls.count("holds") >= 2)
    assert node.pools.bytes_in(PageState.FREE) == 0
    rm.unhold(region.handle, "write-registration@old", "write-registration@old")
    wait_until(lambda: node.pools.bytes_in(PageState.FREE) == 16 * PAGE)
    assert not node.pools.pool_quarantined(pool_id)


def test_real_catalog_exposes_only_committed_remote_locations_and_keeps_orphan_pin(
    memory,
):
    from maru_common.storage_types import (
        MEMORY_CAPABILITY,
        REMOTE_CAPABILITY,
        WORKER_RDMA_CAPABILITY,
    )
    from tests.unit.ms_fakes import FakeNode, FakeWorker

    memory.node.stop()
    metadata = MetadataServer()
    memory.config = dataclasses.replace(memory.config, server=metadata.address)
    node = memory.start()
    memory.node, memory.parts, memory.ops = node, node.role.parts, node.role.ops
    wire = metadata.wire()
    gpu = FakeNode(wire, "gpu-reader")
    gpu.capabilities = [REMOTE_CAPABILITY, WORKER_RDMA_CAPABILITY]
    assert gpu.open(pools=[gpu.dram()], pid_ns=101).status == Status.OK
    worker = FakeWorker(wire, gpu)
    assert worker.open().status == Status.OK
    offered = memory.reserve().credits
    key = Key("ns", "k0")
    try:
        assert worker.lookup(key).items[0].result == ItemResult.MISS
        assert memory.write(offered).items[0].result == ItemResult.ADDED
        hit = worker.lookup(key).items[0]
        assert (
            hit.result == ItemResult.HIT and hit.location.pool_id == offered[0].pool_id
        )
        assert hit.pool_registration == node.pool_registrations[offered[0].pool_id] > 0
        service = metadata.server.ms_service
        assert service.pins.get(hit.pin_id) is not None
        gpu.close(CloseMode.RESTART)
        assert service.pins.get(hit.pin_id) is not None
        assert service.registry.capabilities_of(node.config.node_id) == (
            MEMORY_CAPABILITY,
        )
    finally:
        node.stop()
        metadata.close()
