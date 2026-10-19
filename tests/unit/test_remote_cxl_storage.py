# SPDX-License-Identifier: Apache-2.0
"""Remote CXL storage client against the real directory and a real region mapper
served on loopback TCP; the CXL device, the resource manager and NIXL are
fakes."""

import threading
import time

import pytest

import maru_handler.storage.remote_cxl.datapath as datapath
from maru_common.config import MaruConfig
from maru_common.storage_types import (
    REMOTE_CXL_STORAGE_CAPABILITY,
    STORAGE_CAPABILITY,
    StorageError,
    StorageUnavailableError,
)
from maru_handler.storage.remote_cxl import RemoteCxlStorageClient, pick_mapper
from maru_remote.region_map import RegionMapError, read_handle
from maru_remote.transport import NixlTransport
from maru_server.region_keeper import RegionKeeper
from maru_server.replica_directory import ReplicaDirectory
from tests.unit.remote_fakes import (
    FakeClock,
    FakeCxl,
    FakeMemoryNode,
    FakeNixlAgent,
    key_file,
    reset_fake_agents,
    shared_key,
)

PAGE = 4096  # a region must start on a page for mmap


class Rpc:
    def __init__(self, directory, mappers):
        self.directory = directory
        self.mappers = mappers
        self.lost_replies: set[str] = set()  # actions that run but never reply
        self.unreachable: set[str] = set()  # actions that never reach the server

    def connect(self):
        pass

    def close(self):
        pass

    def handshake(self):
        return {
            "capabilities": [STORAGE_CAPABILITY, REMOTE_CXL_STORAGE_CAPABILITY],
            "server_epoch": self.directory.server_epoch,
            "rm_address": "mem1:9850",
            "region_mappers": list(self.mappers),
        }

    def storage(self, action, payload):
        if action in self.unreachable:
            raise StorageUnavailableError(f"{action} did not reach MaruServer")
        result = self.directory.execute(action, payload)
        if action in self.lost_replies:
            raise StorageUnavailableError(f"reply lost after {action}")
        if not result["success"]:
            raise StorageError(result["error"])
        return result["result"]


class World:
    def __init__(self, nodes=1):
        reset_fake_agents()
        self.clock = FakeClock()
        self.cxl = FakeCxl()
        self.directory = ReplicaDirectory(
            reserve_cxl=self.cxl.reserve,
            release_cxl=self.cxl.release,
            clock=self.clock,
            session_ttl=30,
        )
        self.nodes = [FakeMemoryNode(self.cxl, f"mem{i + 1}") for i in range(nodes)]
        self.clients: list[RemoteCxlStorageClient] = []
        # MaruServer's side of the regions whose owner left; tests run it
        # one pass at a time.
        self.keeper = RegionKeeper(
            self.directory,
            [n.address for n in self.nodes],
            shared_key(),
            clock=self.clock,
        )

    @property
    def node(self) -> FakeMemoryNode:
        return self.nodes[0]

    def client(self, engine, pages=4, staging_pages=8, metadata_only=False, **kwargs):
        config = MaruConfig(
            storage_backend="remote_cxl",
            engine_id=engine,
            cache_namespace="ns",
            node_id="worker",
            **{
                "pool_size": pages * PAGE,
                "chunk_size_bytes": PAGE,
                "rdma_staging_size": staging_pages * PAGE,
                "metadata_only": metadata_only,
                "region_mapper_key_file": key_file(),
                **kwargs,
            },
        )
        c = RemoteCxlStorageClient(
            config,
            Rpc(self.directory, [n.address for n in self.nodes]),
            transport_factory=lambda name: NixlTransport(
                name, agent=FakeNixlAgent(name)
            ),
        )
        c.connect()
        self.clients.append(c)
        return c

    def close(self):
        for c in self.clients:
            if c.connected or c.regions:
                try:
                    c.close()
                except RuntimeError:
                    pass
        self.keeper.stop()
        for node in self.nodes:
            node.stop()
        reset_fake_agents()


@pytest.fixture(autouse=True)
def _short_backoff(monkeypatch):
    """A region mapper that failed is tried again after 50 ms, not 5 s."""
    monkeypatch.setattr(datapath, "DOWN_BACKOFF_S", 0.05)


@pytest.fixture
def world():
    w = World()
    yield w
    w.close()


@pytest.fixture
def shared():
    w = World(nodes=2)
    yield w
    w.close()


def put(c, key, data):
    handle = c.alloc(len(data))
    handle.buf[: len(data)] = data
    return c.batch_store([key], [handle])[0], handle


def read(c, keys):
    leases = c.batch_retrieve(keys)
    out = [bytes(lease.view) if lease else None for lease in leases]
    for lease in leases:
        if lease:
            lease.release()
    return out


def worker_agent(engine):
    name = next(
        n for n in FakeNixlAgent.registry if n.startswith(f"maru-worker-{engine}")
    )
    return FakeNixlAgent.registry[name]


def only_region(c):
    (region,) = c.regions.values()
    return region


def only_region_on(c, index):
    return list(c.regions.values())[index]


# ---------------------------------------------------------------- data path


def test_bytes_go_to_the_remote_page_and_come_back(world):
    a = world.client("a")
    ok, handle = put(a, "k", b"hello")
    assert ok and handle.state == "ready"
    region = only_region(a)
    offset = region.handle["offset"] + handle.location.offset
    assert world.cxl.page(offset, 5) == b"hello"
    assert a.batch_exists(["k", "x"]) == [True, False]
    assert read(a, ["k", "x"]) == [b"hello", None]


def test_the_region_mapper_checks_a_region_once_and_shares_its_registration(world):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"shared")
    region = only_region(a)
    assert read(b, ["k"]) == [b"shared"]
    # a's map asked the RM; b's map only compared the handle.
    assert world.cxl.accesses.count(region.region_id) == 1
    holders = world.node.service.holders()
    assert len(holders[region.region_id]) == 2
    scheduler = world.client("b-sched", metadata_only=True)
    assert scheduler.batch_exists(["k"]) == [True]


def test_a_reader_gets_a_read_handle_and_cannot_take_the_region_away(world):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"shared")
    region = only_region(a)
    leases = {}
    for c in (a, b):
        reply = world.directory.execute("acquire", {**c._credentials, "keys": ["k"]})
        leases[c] = reply["result"]
    # The owner gets its region's handle; a reader only a read handle.
    assert leases[a]["regions"][region.pool_id] == region.handle
    assert leases[b]["regions"][region.pool_id] == read_handle(region.handle)
    for c, lease in leases.items():
        world.directory.execute(
            "release", {**c._credentials, "lease_id": lease["lease_id"]}
        )
    assert read(b, ["k"]) == [b"shared"]
    # A reader that tries to release the owner's region is refused, and the
    # owner keeps writing to it.
    with pytest.raises(RegionMapError) as info:
        b._data._client(world.node.address).release(
            leases[b]["regions"][region.pool_id]
        )
    assert info.value.code == "denied"
    assert put(a, "k2", b"more")[0]
    assert read(b, ["k2"]) == [b"more"]


def test_a_key_another_worker_committed_first_returns_the_page(world):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"first")
    ok, handle = put(b, "k", b"second")
    assert ok and handle.state == "freed"
    assert only_region(b).usage()["allocated_bytes"] == 0
    assert read(b, ["k"]) == [b"first"]


def test_reads_beyond_free_staging_slots_are_misses(world):
    a = world.client("a", staging_pages=2)
    put(a, "k1", b"1")
    put(a, "k2", b"2")
    leases = a.batch_retrieve(["k1", "k2"])
    assert a.batch_retrieve(["k1"]) == [None]
    for lease in leases:
        lease.release()
    assert read(a, ["k1"]) == [b"1"]


def test_stores_leave_the_load_reserve_free(world):
    # Asynchronous transfers: stores leave half of the staging slots to loads.
    a = world.client("a", pages=8, staging_pages=4, rdma_load_reserve=0.5)
    held = [a.alloc(1), a.alloc(1)]
    with pytest.raises(MemoryError, match="no slot for stores"):
        a.alloc(1)
    assert a.retrieve_capacity() == 2
    for handle in held:
        a.free(handle)
    put(a, "k1", b"1")
    put(a, "k2", b"2")
    leases = a.batch_retrieve(["k1", "k2"])  # loads may take the reserve
    assert [bytes(lease.view) for lease in leases] == [b"1", b"2"]
    assert a.retrieve_capacity() == 2  # the two other slots are free
    for lease in leases:
        lease.release()


def test_without_a_reserve_loads_and_stores_share_every_slot(world):
    a = world.client("a", pages=8, staging_pages=2)
    held = [a.alloc(1), a.alloc(1)]
    assert a.retrieve_capacity() == 0
    for handle in held:
        a.free(handle)
    assert a.retrieve_capacity() == 2


def test_gpu_access_follows_the_staging_registration(world, monkeypatch):
    a = world.client("a")
    a._staging.cuda_registered = False
    assert a.storage_gpu_accessible() is False
    a._staging.cuda_registered = True
    assert a.storage_gpu_accessible() is True


# -------------------------------------------------------------- regions, eviction


def test_a_full_worker_gets_one_more_region(world):
    a = world.client("a", pages=2)
    put(a, "k1", b"1")
    put(a, "k2", b"2")
    assert len(a.regions) == 1
    ok, handle = put(a, "k3", b"3")
    assert ok and len(a.regions) == 2
    assert handle.location.pool_id != next(iter(a.regions))
    assert read(a, ["k1", "k3"]) == [b"1", b"3"]


def test_when_the_rm_is_full_the_worker_evicts_its_oldest_key(world):
    world.cxl.size = 4 * PAGE  # room for two regions of two pages
    a, b = world.client("a", pages=2), world.client("b", pages=2)
    put(a, "old", b"1")
    world.clock.advance(1)
    put(a, "new", b"2")
    world.clock.advance(1)
    assert read(b, ["new"]) == [b"2"]  # a read by b keeps "new" recent
    ok, _ = put(a, "third", b"3")
    assert ok and len(a.regions) == 1
    assert read(b, ["old", "new", "third"]) == [None, b"2", b"3"]


def test_without_auto_expand_the_worker_evicts_instead(world):
    a = world.client("a", pages=2, auto_expand=False)
    put(a, "k1", b"1")
    put(a, "k2", b"2")
    ok, _ = put(a, "k3", b"3")
    assert ok and len(a.regions) == 1


def test_a_lost_remove_reply_frees_the_pages_once_a_retry_confirms(world):
    world.cxl.size = 2 * PAGE
    a = world.client("a", pages=2)
    put(a, "k1", b"1")
    put(a, "k2", b"2")
    a.rpc.lost_replies.add("remove")
    with pytest.raises(MemoryError):
        a.alloc(1)
    # The keys may be gone, so they no longer count as stored here.
    assert not a.has_local("k1")
    a.rpc.lost_replies.clear()
    # The retry learns k1 is gone and reuses its page; k2 stays cached.
    handle = a.alloc(1)
    a.free(handle)
    assert a.has_local("k2")
    assert a.batch_exists(["k1", "k2"]) == [False, True]


# ------------------------------------------------------- lost replies, transfers


def test_lost_commit_reply_is_resolved_without_reusing_the_page(world):
    a = world.client("a")
    a.rpc.lost_replies.add("commit")
    ok, handle = put(a, "k", b"v")
    assert not ok and handle.state == "committing"
    a.free(handle)
    a.rpc.lost_replies.clear()
    a._resolve_pending()
    assert handle.state == "ready" and a.has_local("k")


def test_a_lost_acquire_reply_leaves_no_lease_behind(world):
    a, b = world.client("a", pages=1), world.client("b")
    put(a, "k", b"v")
    b.rpc.lost_replies.add("acquire")
    with pytest.raises(StorageUnavailableError):
        b.batch_retrieve(["k"])
    # An unreleased lease would keep "k" from ever being evicted.
    victims = world.directory.execute("victims", {**a._credentials, "count": 1})
    assert [v["key"] for v in victims["result"]["entries"]] == ["k"]


def test_unconfirmed_write_keeps_pages_and_slots_until_released(world):
    a = world.client("a", pages=2, rdma_timeout_s=0.01)
    agent = worker_agent("a")
    agent.stall = True
    handle = a.alloc(4)
    handle.buf[:4] = b"late"
    assert a.batch_store(["k"], [handle]) == [False]
    assert handle.state == "isolated"
    a.free(handle)  # the connector's cleanup must not reuse it
    region = only_region(a)
    assert region.usage()["quarantined_bytes"] == PAGE
    free_slots = a._staging.free_count()
    agent.stall = False
    agent.finish_stalled()
    a._reap_isolated()
    assert region.usage() == {"allocated_bytes": 0, "quarantined_bytes": 0}
    assert a._staging.free_count() == free_slots + 1


def test_a_failed_write_reconnects_and_maps_the_region_again(world):
    a = world.client("a")
    agent = worker_agent("a")
    peer = world.node.agent.name
    agent.broken.add(peer)  # a remote access error left the connection broken
    ok, handle = put(a, "k", b"x")
    assert not ok and handle.state == "freed"
    # The broken connection was dropped and the region mapped again at once.
    assert peer in agent.peers and peer not in agent.broken
    assert only_region(a).reachable
    ok, _ = put(a, "k", b"y")
    assert ok and read(a, ["k"]) == [b"y"]


def test_the_page_of_a_failed_write_is_not_used_again(world):
    # The WRITE may still be in the network, and the memory node's NIC still
    # accepts the region's key: its page must not get another key's bytes.
    a = world.client("a", pages=2)
    agent = worker_agent("a")
    agent.fail_next = True
    ok, failed = put(a, "k1", b"x")
    assert not ok
    region = only_region(a)
    assert region.usage()["quarantined_bytes"] == PAGE
    ok, stored = put(a, "k2", b"y")
    assert ok and stored.location.offset != failed.location.offset
    # The staging slot came back: the failed WRITE's end of the connection
    # is gone.
    assert a._staging.free_count() == a._staging.count


def test_a_lost_expand_reply_gets_the_same_region_on_the_next_try(world):
    a = world.client("a", pages=1)
    put(a, "k1", b"1")
    a.rpc.lost_replies.add("expand")
    assert put(a, "k2", b"2")[0] is True  # no new region known: k1 was evicted
    assert len(a.regions) == 1 and len(world.cxl.regions) == 2
    a.rpc.lost_replies.clear()
    assert put(a, "k3", b"3")[0] is True
    # The resent expand got the region MaruServer gave the lost one.
    assert len(a.regions) == 2 and len(world.cxl.regions) == 2
    assert read(a, ["k2", "k3"]) == [b"2", b"3"]


def test_a_map_reply_names_only_that_region(world):
    a, b = world.client("a"), world.client("b")
    peer = world.node.agent.name
    ra, rb = only_region(a), only_region(b)
    for engine, region in (("a", ra), ("b", rb)):
        known = {(addr, size) for addr, size, _key in worker_agent(engine).known[peer]}
        access = (engine == "a" and a or b)._data.region(
            region.pool_id, region.handle, [world.node.address]
        )
        assert known == {(access.base_addr, region.handle["length"])}


def test_heartbeat_renews_while_a_transfer_holds_the_client_lock(world):
    a = world.client("a")
    held, done = threading.Event(), threading.Event()

    def long_transfer():
        with a._lock:
            held.set()
            done.wait(5)

    worker = threading.Thread(target=long_transfer)
    worker.start()
    try:
        held.wait(5)
        a._heartbeat()  # would block until the transfer ends if it took the lock
    finally:
        done.set()
        worker.join()


# ------------------------------------------------------------ return rule (5.3)


def test_after_its_owner_closes_a_region_with_keys_stays_readable(world):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"v")
    region = only_region(a)
    assert read(b, ["k"]) == [b"v"]  # b maps a's region to read it
    a.close()
    world.keeper.run_once()
    # As a cxl Handler's region stays while keys refer to it: MaruServer
    # holds the registration instead of the owner.
    assert world.cxl.released == []
    assert len(world.node.service.holders()[region.region_id]) == 2  # b, keeper
    assert read(b, ["k"]) == [b"v"]
    c = world.client("c")  # a worker that starts later reads it too
    assert read(c, ["k"]) == [b"v"]
    a2 = world.client("a")  # so does the restarted worker
    assert read(a2, ["k"]) == [b"v"]


def test_a_kept_region_goes_back_only_after_its_release(world):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"v")
    region = only_region(a)
    assert read(b, ["k"]) == [b"v"]
    a.close()
    world.keeper.run_once()
    seen = []
    release = world.node.service.release

    def record(handle):
        # At the release the region is still allocated: not yet at the RM.
        seen.append(handle["region_id"] in world.cxl.regions)
        return release(handle)

    world.node.service.release = record
    world.directory.give_back(region.pool_id, "test")
    assert read(b, ["k"]) == [None]  # its keys stop being found at once
    world.keeper.run_once()
    assert seen == [True]
    assert world.cxl.released == [region.region_id]
    # Released whoever else held it, and b cannot map it again.
    assert region.region_id not in world.node.service.holders()
    b._data.forget(region.pool_id)
    with pytest.raises(StorageUnavailableError, match="released"):
        b._data.region(region.pool_id, read_handle(region.handle), b.region_mappers)


def test_a_full_rm_gives_back_the_kept_region_read_longest_ago(world):
    world.cxl.size = 4 * PAGE  # room for two regions of two pages
    a = world.client("a", pages=2)
    put(a, "k", b"v")
    region = only_region(a)
    a.close()
    world.keeper.run_once()
    b = world.client("b", pages=2)
    put(b, "b1", b"1")
    put(b, "b2", b"2")
    # b's region is full and the RM has no room: b evicts its own key now,
    # and the keeper gives a's kept region back for b's next request.
    assert put(b, "b3", b"3")[0]
    world.keeper.run_once()
    world.keeper.run_once()
    assert world.cxl.released == [region.region_id]
    assert read(b, ["k"]) == [None]
    assert put(b, "b4", b"4")[0] and len(b.regions) == 2


def test_a_write_still_in_flight_at_close_cannot_land_on_the_next_owner(world):
    a = world.client("a", rdma_timeout_s=0.01)
    agent = worker_agent("a")
    agent.stall = True
    assert put(a, "k", b"late")[0] is False
    old = only_region(a)
    a.close()
    # The region holds no key: the keeper releases it, then frees it.
    world.keeper.run_once()
    assert world.cxl.released == [old.region_id]
    # The same bytes of CXL go to the next owner under a new region id.
    c = world.client("c")
    new = only_region(c)
    assert new.handle["offset"] == old.handle["offset"]
    assert new.region_id != old.region_id
    assert put(c, "mine", b"mine")[0] is True
    # The WRITE a sent before its close reaches the memory node only now.
    assert agent.finish_stalled() == 0
    assert world.cxl.page(new.handle["offset"], 4) == b"mine"
    assert read(c, ["mine"]) == [b"mine"]


def test_an_unreachable_region_mapper_keeps_the_regions(world):
    a = world.client("a")
    put(a, "k", b"v")
    region = only_region(a)
    world.node.stop()
    a.close()
    world.directory.give_back(region.pool_id, "test")
    world.keeper.run_once()
    # No release, so the region stays allocated.
    assert world.cxl.released == []
    (usage,) = world.directory.get_usage("remote_cxl")["pools"]
    assert usage["pool_id"] == region.pool_id


def test_a_failed_deregistration_keeps_the_regions(world):
    a = world.client("a")
    put(a, "k", b"v")
    region = only_region(a)
    a.close()
    world.keeper.run_once()

    def refuse(registration):
        raise RuntimeError("NIC busy")

    world.node.transport.deregister = refuse
    world.directory.give_back(region.pool_id, "test")
    world.keeper.run_once()
    # Still registered, so it must not reach the RM; the keeper tries again.
    assert world.cxl.released == []
    assert [pool_id for pool_id, *_ in world.directory.departed()] == [region.pool_id]


def test_a_restarted_region_mapper_confirms_the_release(world):
    a = world.client("a")
    put(a, "k", b"v")
    region = only_region(a)
    a.close()
    world.keeper.run_once()
    world.node.restart()  # every registration went with the old process
    world.directory.give_back(region.pool_id, "test")
    world.keeper.run_once()
    assert world.cxl.released == [region.region_id]


def test_the_keeper_maps_a_kept_region_again_after_its_mapper_restarts(world):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"v")
    region = only_region(a)
    a.close()
    world.keeper.run_once()
    world.node.restart()
    world.clock.advance(11)  # the keeper's next check of its holds
    world.keeper.run_once()
    assert region.region_id in world.node.service.holders()
    assert read(b, ["k"]) == [b"v"]


def test_a_kept_region_on_a_rebooted_node_goes_back(world):
    a = world.client("a")
    put(a, "k", b"v")
    region = only_region(a)
    a._heartbeat()  # reports the boot of the region's memory node
    a.close()
    world.keeper.run_once()
    world.node.restart(reboot=True)  # bytes in its CPU cache may be gone
    world.clock.advance(11)
    world.keeper.run_once()
    world.keeper.run_once()
    assert world.cxl.released == [region.region_id]
    b = world.client("b")
    assert read(b, ["k"]) == [None]


def test_an_expired_owner_leaves_and_its_keys_stay_readable(world):
    a = world.client("a")
    put(a, "k", b"v")
    region = only_region(a)
    world.clock.advance(31)
    with pytest.raises(StorageError):
        a._heartbeat()
    a._disable("session expired")
    assert not a.connected
    with pytest.raises(StorageUnavailableError):
        a.alloc(1)
    world.keeper.run_once()
    assert world.cxl.released == []
    b = world.client("b")
    assert read(b, ["k"]) == [b"v"]
    assert region.region_id in world.node.service.holders()
    a.close()


def test_a_crashed_owners_keys_stay_readable(world):
    a = world.client("a")
    put(a, "k", b"v")
    a._data.close()  # the process is gone: its connections close
    a._stop.set()
    world.clock.advance(31)
    world.keeper.run_once()  # the session expired: MaruServer maps the region
    b = world.client("b")
    assert read(b, ["k"]) == [b"v"]
    assert world.cxl.released == []
    world.clients.remove(a)


def test_a_reader_finishing_after_a_give_back_keeps_its_bytes(world):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"v")
    region = only_region(a)
    leases = b.batch_retrieve(["k"])
    a.close()
    world.keeper.run_once()
    world.directory.give_back(region.pool_id, "test")
    world.keeper.run_once()
    # b released its server lease right after the READ; the bytes b read
    # are in its own staging buffer.
    assert world.cxl.released == [region.region_id]
    assert bytes(leases[0].view) == b"v"
    leases[0].release()


# ---------------------------------------------------------- region mapper failures


def test_a_region_mapper_restart_maps_the_regions_again(world):
    a, b = world.client("a"), world.client("b")
    put(a, "before", b"1")
    world.node.restart()
    results = [put(a, "after", b"2")[0] for _ in range(3)]
    assert results[-1] is True
    # The bytes stored before the restart stay in the CXL memory.
    got = None
    for _ in range(3):
        got = read(b, ["before"])
        if got == [b"1"]:
            break
    assert got == [b"1"]


def test_an_unreachable_region_is_set_aside_and_its_keys_removed(shared):
    a = shared.client("a", pages=2)
    put(a, "k1", b"1")
    first = only_region(a)
    down = next(
        n
        for n in shared.nodes
        if n.address == pick_mapper(a.region_mappers, first.region_id)
    )
    down.stop()
    ok = False
    for _ in range(12):  # the next regions may also fall on the stopped node
        try:
            ok, handle = put(a, "k2", b"2")
        except MemoryError:
            continue  # a new region on the stopped node went back at once
        if ok:
            break
    assert ok
    assert not first.reachable
    # Regions nobody could map went back: only the first and the used one stay.
    assert len(shared.cxl.regions) == 2
    # Its key no longer reports a hit that cannot be read.
    assert a.batch_exists(["k1", "k2"]) == [False, True]
    assert handle.location.pool_id != first.pool_id
    down_regions = [r for r in a.regions.values() if not r.reachable]
    assert len(down_regions) <= len(a.region_mappers)
    # The node comes back on the same address: the region is used again.
    down._start()
    time.sleep(0.1)  # the back-off after the failed map ends
    a._retry_unreachable()
    assert first.reachable


def test_a_down_node_keeps_only_its_regions_from_going_back(shared):
    a = shared.client("a", pages=1)
    put(a, "k1", b"1")
    while (
        len({pick_mapper(a.region_mappers, r.region_id) for r in a.regions.values()})
        < 2
    ):
        put(a, f"k{len(a.regions) + 1}", b"x")  # a region on each node
    down_address = pick_mapper(a.region_mappers, only_region_on(a, 0).region_id)
    down = next(n for n in shared.nodes if n.address == down_address)
    kept = {
        r.region_id
        for r in a.regions.values()
        if pick_mapper(a.region_mappers, r.region_id) == down_address
    }
    down.stop()
    a.close()
    for pool_id, *_ in shared.directory.departed():
        shared.directory.give_back(pool_id, "test")
    shared.keeper.run_once()
    # Regions on the live node went back; the down node's stay allocated.
    assert set(shared.cxl.regions) == kept


def test_a_region_no_mapper_ever_mapped_goes_back_without_a_release(world):
    a = world.client("a", pages=1)
    put(a, "k", b"v")
    first = only_region(a)
    world.node.stop()
    a.rpc.unreachable.add("return_region")  # keep the new region until close
    # The full region asks for a new one it cannot map, then evicts "k".
    assert put(a, "k2", b"w")[0] is False
    (never,) = [r for r in a.regions.values() if r is not first]
    assert not never.mapped
    a.rpc.unreachable.clear()
    a.close()
    # The first region was mapped and cannot be released: MaruServer keeps
    # it. No RDMA was ever sent to the new one, so it went back.
    assert world.cxl.released == [never.region_id]
    assert first.region_id in world.cxl.regions


def test_a_new_region_on_a_down_node_goes_back_at_once(world, monkeypatch):
    monkeypatch.setattr(datapath, "DOWN_BACKOFF_S", 60.0)
    a = world.client("a", pages=1)
    put(a, "k", b"v")
    world.node.stop()
    # The new region cannot be mapped and goes back; "k" is evicted for room
    # and the WRITE to the stopped node fails, which sets the region aside.
    assert put(a, "k2", b"w")[0] is False
    assert len(a.regions) == 1 and world.cxl.released == [2]
    assert not only_region(a).reachable
    # Every mapper of the pool is down: no more regions are asked for.
    with pytest.raises(MemoryError):
        put(a, "k3", b"x")
    assert world.cxl.released == [2] and len(world.cxl.regions) == 1


def test_a_region_mapped_without_an_access_is_released_before_it_goes_back(world):
    a = world.client("a", pages=1)
    put(a, "k", b"v")
    first = only_region(a)
    transport = a._data.transport
    add_peer = transport.add_peer

    def refuse(metadata):
        raise RuntimeError("NIXL refused the metadata")

    transport.add_peer = refuse
    held_at_return = []
    storage = a.rpc.storage

    def watch(action, payload):
        if action == "return_region":
            held_at_return.append(set(world.node.service.holders()))
        return storage(action, payload)

    a.rpc.storage = watch
    # The new region maps at the mapper but gives no access, so it is given
    # up; its mapper must not hold it any more when it goes back.
    put(a, "k2", b"w")
    transport.add_peer = add_peer
    assert held_at_return == [{first.region_id}]


def test_a_region_the_mapper_failed_to_map_is_not_asked_again_at_once(
    world, monkeypatch
):
    monkeypatch.setattr(datapath, "DOWN_BACKOFF_S", 60.0)
    calls = []
    register = world.node.transport.register

    def refuse(*args, **kwargs):
        calls.append(1)
        raise RuntimeError("NIC out of resources")

    world.node.transport.register = refuse
    a = world.client("a")
    region = only_region(a)
    assert not region.reachable and calls == [1]
    world.node.transport.register = register
    a._retry_unreachable()  # within the back-off: the mapper is not asked
    assert not region.reachable and calls == [1]
    # Only that region waits; the mapper itself is not counted as down.
    assert not a._data.is_down(world.node.address)


def test_a_failed_release_does_not_stop_the_other_regions_of_its_mapper(world):
    a = world.client("a", pages=1)
    put(a, "k", b"v")
    put(a, "k2", b"w")  # the full region gets a second one
    first, second = a.regions.values()
    release = world.node.service.release

    def fail_first(handle):
        if handle["region_id"] == first.region_id:
            raise RuntimeError("deregistering failed")
        return release(handle)

    world.node.service.release = fail_first
    a.close()
    for pool_id, *_ in world.directory.departed():
        world.directory.give_back(pool_id, "test")
    world.keeper.run_once()
    assert world.cxl.released == [second.region_id]
    assert first.region_id in world.cxl.regions


def test_a_rebooted_memory_node_voids_the_keys_of_its_regions(world):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"v")
    assert b.batch_exists(["k"]) == [True]
    world.node.restart(reboot=True)
    assert put(a, "k2", b"w")[0] is False  # the old registration is gone
    assert put(a, "k2", b"w")[0] is True  # mapped again on the new boot
    # A WRITE the node's NIC had completed may have been lost with its cache.
    assert b.batch_exists(["k", "k2"]) == [False, True]


def test_a_restarted_region_mapper_keeps_the_keys(world):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"v")
    world.node.restart()  # the process only: the node's caches stay
    put(a, "k2", b"w")
    assert put(a, "k2", b"w")[0] is True
    assert b.batch_exists(["k"]) == [True]


def test_a_peer_is_connected_anew_after_its_mapper_connection_broke(world):
    """A region mapper that stopped answering for a while comes back.

    The workers' requests to it timed out, so their connections broke, and
    UCX failed their connections to its NIXL agent on its own. The agent is
    the same process's, so mapping a region again must not load its metadata
    into the failed peer (NIXL crashes there); the peer is connected anew.
    """
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"v")
    assert read(b, ["k"]) == [b"v"]
    peer = world.node.agent.name
    for c, engine in ((a, "a"), (b, "b")):
        worker_agent(engine).fail_endpoint(peer)
        client = c._data._client(world.node.address)
        before = client.generation
        client._drop()  # a request timed out
        assert c._data.boots([world.node.address])  # the heartbeat reconnects
        assert client.generation == before + 1
    assert put(a, "k2", b"w")[0] is True
    assert read(b, ["k", "k2"]) == [b"v", b"w"]
    assert peer in worker_agent("a").peers and peer in worker_agent("b").peers


def test_expanding_resumes_once_the_back_off_ends(world):
    a = world.client("a", pages=1)
    put(a, "k", b"v")
    world.node.restart()  # same address; the first map after it fails over
    world.node.stop()
    assert put(a, "k2", b"w")[0] is False
    assert not a._may_expand()  # the only region mapper is down
    world.node._start()
    time.sleep(0.1)
    assert a._may_expand()
    assert put(a, "k3", b"x")[0] is True


def test_reads_of_a_down_node_miss_without_waiting_for_it(world, monkeypatch):
    monkeypatch.setattr(datapath, "DOWN_BACKOFF_S", 60.0)
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"v")
    assert read(b, ["k"]) == [b"v"]
    world.node.stop()
    assert read(b, ["k"]) == [None]  # the READ fails and the peer is dropped
    assert read(b, ["k"]) == [None]  # the map fails: the mapper is now down
    client = b._data._client(world.node.address)

    def no_connect(handle):
        raise AssertionError("a down region mapper was asked again")

    client.map = no_connect
    free = b._staging.free_count()
    assert read(b, ["k"]) == [None]
    assert b._staging.free_count() == free


def test_a_read_error_drops_the_connection_and_the_next_read_works(world):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"v")
    assert read(b, ["k"]) == [b"v"]
    peer = world.node.agent.name
    worker_agent("b").broken.add(peer)
    assert read(b, ["k"]) == [None]
    assert peer not in worker_agent("b").peers
    assert read(b, ["k"]) == [b"v"]


def test_a_lost_lease_discards_the_bytes_read(world):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"v")
    storage = b.rpc.storage

    def lose_lease(action, payload):
        result = storage(action, payload)
        return {"held": False} if action == "release" else result

    b.rpc.storage = lose_lease
    free = b._staging.free_count()
    assert read(b, ["k"]) == [None]
    assert b._staging.free_count() == free


def test_an_unexpected_error_while_reading_keeps_no_slot_or_lease(world, monkeypatch):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"v")
    free = b._staging.free_count()

    def broken(*args, **kwargs):
        raise RuntimeError("bug")

    monkeypatch.setattr(b._data, "region", broken)
    assert read(b, ["k"]) == [None]
    assert b._staging.free_count() == free
    # No lease of b pins "k": its owner can still evict it.
    victims = world.directory.execute("victims", {**a._credentials, "count": 1})
    assert [v["key"] for v in victims["result"]["entries"]] == ["k"]


def test_a_read_error_returns_each_slot_once_if_closing_the_connection_fails(
    world, monkeypatch
):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"v")
    assert read(b, ["k"]) == [b"v"]
    worker_agent("b").broken.add(world.node.agent.name)

    def refuse(peers):
        raise RuntimeError("NIXL refused to remove the peer")

    monkeypatch.setattr(b._data, "disconnect", refuse)
    free = b._staging.free_count()
    assert read(b, ["k"]) == [None]
    assert b._staging.free_count() == free
    assert len(set(b._staging._free)) == len(b._staging._free)


def test_an_interrupt_during_a_read_keeps_its_slot_taken(world, monkeypatch):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"v")
    assert read(b, ["k"]) == [b"v"]

    def interrupt(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(b._data.transport, "read", interrupt)
    free = b._staging.free_count()
    with pytest.raises(KeyboardInterrupt):
        b.batch_retrieve(["k"])
    # The READ may still run: its slot is not handed out again.
    assert b._staging.free_count() == free - 1
    # No lease of b pins "k": its owner can still evict it.
    victims = world.directory.execute("victims", {**a._credentials, "count": 1})
    assert [v["key"] for v in victims["result"]["entries"]] == ["k"]


def test_a_reboot_is_noticed_without_writing_to_the_region(world):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"v")
    world.node.restart(reboot=True)
    a._check_boots()  # what the background remap does every beat
    assert b.batch_exists(["k"]) == [False]


def test_a_wrong_handle_is_refused_by_the_region_mapper(world):
    a = world.client("a")
    region = only_region(a)
    a._data.forget(region.pool_id)
    forged = {**region.handle, "auth_token": 1}
    with pytest.raises(StorageUnavailableError, match="could not be mapped"):
        a._data.region("forged", forged, a.region_mappers)


# ----------------------------------------------------------------- shared pool


def test_a_shared_pool_spreads_regions_and_readers_follow_the_owner(shared):
    owners = [shared.client(f"w{i}") for i in range(4)]
    for i, c in enumerate(owners):
        put(c, f"k{i}", bytes([i]))
    reader = shared.client("reader")
    assert read(reader, [f"k{i}" for i in range(4)]) == [bytes([i]) for i in range(4)]
    for c in owners:
        region = only_region(c)
        node = pick_mapper(c.region_mappers, region.region_id)
        # The owner and the reader map the region through the same node.
        holder = next(n for n in shared.nodes if n.address == node)
        assert len(holder.service.holders()[region.region_id]) == 2
        other = next(n for n in shared.nodes if n.address != node)
        assert region.region_id not in other.service.holders()


def test_regions_spread_over_the_region_mappers():
    mappers = ["mem2:9860", "mem3:9860"]
    picks = [pick_mapper(mappers, region_id) for region_id in range(1, 201)]
    assert 70 <= picks.count("mem2:9860") <= 130
    # Every Handler with the same list picks the same node for a region.
    assert picks == [pick_mapper(list(reversed(mappers)), r) for r in range(1, 201)]


# -------------------------------------------------------------------- setup


def test_a_server_without_remote_cxl_or_region_mappers_is_rejected(world):
    class OldRpc(Rpc):
        def handshake(self):
            return {**super().handshake(), "capabilities": [STORAGE_CAPABILITY]}

    class NoMapperRpc(Rpc):
        def handshake(self):
            return {**super().handshake(), "region_mappers": []}

    config = MaruConfig(
        storage_backend="remote_cxl",
        engine_id="a",
        cache_namespace="ns",
        pool_size=PAGE,
        chunk_size_bytes=PAGE,
    )
    for rpc_class in (OldRpc, NoMapperRpc):
        c = RemoteCxlStorageClient(
            config,
            rpc_class(world.directory, [world.node.address]),
            transport_factory=lambda name: NixlTransport(
                name, agent=FakeNixlAgent(name)
            ),
        )
        with pytest.raises(StorageError, match="region-mapper"):
            c.connect()


def test_an_open_refusal_that_time_heals_is_retryable(world):
    world.client("a")
    # The session of the same engine_id is still alive (a quick restart).
    with pytest.raises(StorageUnavailableError, match="one active worker"):
        world.client("a")
    world.cxl.size = world.cxl.used  # the resource manager is full
    with pytest.raises(StorageUnavailableError, match="reservation failed"):
        world.client("b")
    # Nothing heavy was set up for a refused open.
    assert not any(n.startswith("maru-worker-b") for n in FakeNixlAgent.registry)


def test_an_open_refusal_of_the_configuration_is_final(world):
    world.client("a")
    with pytest.raises(StorageError) as info:
        world.client("b", chunk_size_bytes=2 * PAGE, pool_size=8 * PAGE)
    assert not isinstance(info.value, StorageUnavailableError)


def test_a_refused_open_leaves_no_session_tombstone(world):
    world.client("a")
    tombstones = len(world.directory._closed_tokens)
    for _ in range(3):  # the connector retries a refusal that time heals
        with pytest.raises(StorageUnavailableError, match="one active worker"):
            world.client("a")
    with pytest.raises(StorageError):
        world.client("b", chunk_size_bytes=2 * PAGE, pool_size=8 * PAGE)
    assert len(world.directory._closed_tokens) == tombstones


def test_an_open_whose_reply_was_lost_is_closed(world):
    rpc = Rpc(world.directory, [world.node.address])
    rpc.lost_replies.add("open")
    config = MaruConfig(
        storage_backend="remote_cxl",
        engine_id="a",
        cache_namespace="ns",
        pool_size=PAGE,
        chunk_size_bytes=PAGE,
        rdma_staging_size=4 * PAGE,
    )
    c = RemoteCxlStorageClient(
        config,
        rpc,
        transport_factory=lambda name: NixlTransport(name, agent=FakeNixlAgent(name)),
    )
    with pytest.raises(StorageUnavailableError, match="reply lost"):
        c.connect()
    # MaruServer had granted a region; the close handed it over and the
    # keeper gave it back (it holds no key).
    world.keeper.run_once()
    assert world.cxl.released and not world.cxl.regions


def test_config_requires_session_identity_and_remote_cxl_backend_for_rdma_settings():
    with pytest.raises(ValueError, match="engine_id and cache_namespace"):
        MaruConfig(storage_backend="remote_cxl", pool_size=PAGE, chunk_size_bytes=PAGE)
    with pytest.raises(ValueError, match="require the remote_cxl storage backend"):
        MaruConfig(rdma_device="mlx5_0:1")
