# SPDX-License-Identifier: Apache-2.0
"""Remote CXL storage client against the real directory and a real remote CXL agent
served on loopback TCP; the CXL device, the resource manager and NIXL are
fakes."""

import dataclasses
import threading
import time

import pytest

import maru_handler.storage.remote_cxl.datapath as datapath
import maru_handler.storage.remote_cxl.lifecycle as lifecycle
from maru_common.config import MaruConfig
from maru_common.storage_types import (
    REMOTE_CXL_STORAGE_CAPABILITY,
    STORAGE_CAPABILITY,
    StorageError,
    StorageUnavailableError,
)
from maru_handler.storage.remote_cxl import RemoteCxlStorageClient, pick_agent
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
    def __init__(self, directory, agents):
        self.directory = directory
        self.agents = agents
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
            "remote_cxl_agents": list(self.agents),
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
                "remote_cxl_agent_key_file": key_file(),
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
    """A remote CXL agent that failed is tried again after 50 ms, not 5 s."""
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


def test_the_remote_cxl_agent_checks_a_region_once_and_shares_its_registration(world):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"shared")
    region = only_region(a)
    assert read(b, ["k"]) == [b"shared"]
    # a's map asked the RM; b's map only compared the handle.
    assert world.cxl.accesses.count(region.region_id) == 1
    holders = world.node.service.holders()
    assert len(holders[region.region_id]) == 2
    # The connector gives a worker's scheduler-side handler the worker's engine id.
    scheduler = world.client("b", metadata_only=True)
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


def test_an_interrupted_write_keeps_its_pages_and_slots_until_it_ends(
    world, monkeypatch
):
    a = world.client("a", pages=2)
    agent = worker_agent("a")
    agent.stall = True

    def interrupted(handle):
        raise KeyboardInterrupt

    monkeypatch.setattr(agent, "check_xfer_state", interrupted)
    free_slots = a._staging.free_count()
    handle = a.alloc(4)
    handle.buf[:4] = b"late"
    with pytest.raises(KeyboardInterrupt):
        a.batch_store(["k"], [handle])
    region = only_region(a)
    # The NIC may still write: the page and the slot stay out of use.
    assert handle.state == "isolated"
    assert region.usage()["quarantined_bytes"] == PAGE
    assert a._staging.free_count() == free_slots - 1
    assert a.has_unconfirmed_transfers()
    monkeypatch.undo()
    agent.stall = False
    agent.finish_stalled()
    a._reap_isolated()
    assert region.usage() == {"allocated_bytes": 0, "quarantined_bytes": 0}
    assert a._staging.free_count() == free_slots


def test_an_interrupted_read_keeps_its_slots_until_it_ends(world, monkeypatch):
    a = world.client("a", pages=2)
    put(a, "k", b"v")
    agent = worker_agent("a")
    agent.stall = True

    def interrupted(handle):
        raise KeyboardInterrupt

    monkeypatch.setattr(agent, "check_xfer_state", interrupted)
    free_slots = a._staging.free_count()
    with pytest.raises(KeyboardInterrupt):
        a.batch_retrieve(["k"])
    assert a._staging.free_count() < free_slots and a.has_unconfirmed_transfers()
    monkeypatch.undo()
    agent.stall = False
    agent.finish_stalled()
    a._reap_isolated()
    assert not a.has_unconfirmed_transfers()


def test_an_interrupt_before_any_transfer_gives_the_buffers_back(world, monkeypatch):
    # NIXL had no transfer yet: nothing was sent, so nothing is kept.
    a = world.client("a", pages=2)
    put(a, "k", b"v")
    agent = worker_agent("a")

    def interrupted(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(agent, "initialize_xfer", interrupted)
    free_slots = a._staging.free_count()
    handle = a.alloc(4)
    handle.buf[:4] = b"none"
    with pytest.raises(KeyboardInterrupt):
        a.batch_store(["k2"], [handle])
    assert handle.state == "freed"
    with pytest.raises(KeyboardInterrupt):
        a.batch_retrieve(["k"])
    assert a._staging.free_count() == free_slots
    assert not a.has_unconfirmed_transfers()
    assert only_region(a).usage()["quarantined_bytes"] == 0


def test_a_write_that_ends_in_an_error_after_its_deadline_keeps_its_page(world):
    """Released after an error, a WRITE may still be in the network."""
    a = world.client("a", pages=2, rdma_timeout_s=0.01)
    agent = worker_agent("a")
    agent.stall = True
    handle = a.alloc(4)
    handle.buf[:4] = b"late"
    assert a.batch_store(["k"], [handle]) == [False]
    region = only_region(a)
    free_slots = a._staging.free_count()
    agent.stall = False
    agent.broken.add(world.node.agent.name)  # the WRITE ends in ERR
    assert agent.finish_stalled() == 0
    a._reap_isolated()
    assert a._isolated == []
    assert a._staging.free_count() == free_slots + 1  # NIXL let the slot go
    assert region.usage() == {"allocated_bytes": PAGE, "quarantined_bytes": PAGE}
    assert not a._transport.peer_usable(world.node.agent.name)


def test_a_read_longer_than_a_staging_slot_is_a_miss(world):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"hello")
    slot_bytes = b._staging.slot_bytes
    b._staging.slot_bytes = 4  # a location this long would spill into the next slot
    try:
        assert read(b, ["k"]) == [None]
    finally:
        b._staging.slot_bytes = slot_bytes
    assert read(b, ["k"]) == [b"hello"]


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
        b._data.region(region.pool_id, read_handle(region.handle), b.remote_cxl_agents)


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


def test_the_room_a_kept_region_makes_goes_to_a_worker_without_one(world):
    """A worker that could not open its session gets the room before another
    worker that already has a region grows into it."""
    world.cxl.size = 4 * PAGE  # room for two regions of two pages
    a = world.client("a", pages=2)
    put(a, "k", b"v")
    a.close()
    world.keeper.run_once()  # a's region is kept
    b = world.client("b", pages=2)
    with pytest.raises(StorageUnavailableError, match="reservation failed"):
        world.client("c", pages=2)  # its engine retries a few seconds later
    world.keeper.run_once()
    world.keeper.run_once()  # a's region is back at the RM
    put(b, "b1", b"1")
    put(b, "b2", b"2")
    assert put(b, "b3", b"3")[0] and len(b.regions) == 1  # b evicted instead
    c = world.client("c", pages=2)
    assert put(c, "c1", b"1")[0] is True


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


def test_an_unreachable_remote_cxl_agent_keeps_the_regions(world):
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


def test_a_restarted_remote_cxl_agent_confirms_the_release(world):
    a = world.client("a")
    put(a, "k", b"v")
    region = only_region(a)
    a.close()
    world.keeper.run_once()
    world.node.restart()  # every registration went with the old process
    world.directory.give_back(region.pool_id, "test")
    world.keeper.run_once()
    assert world.cxl.released == [region.region_id]


def test_the_keeper_maps_a_kept_region_again_after_its_agent_restarts(world):
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


# ------------------------------------------------- transfers outside the lock


def hold_transfers(c, op):
    """Make c's next RDMA ``op`` ("read" or "write") wait for the returned event."""
    transport = c._data.transport
    original = getattr(transport, op)
    entered, go = threading.Event(), threading.Event()

    def held(*args, **kwargs):
        entered.set()
        assert go.wait(10)
        return original(*args, **kwargs)

    setattr(transport, op, held)
    return entered, go


def test_a_write_under_way_holds_up_neither_alloc_nor_a_load(world):
    # alloc runs on the engine thread: a WRITE that waits on a slow memory
    # node must not stall every running request.
    a = world.client("a", pages=8, rdma_nonblocking_alloc=True)
    put(a, "old", b"o")
    entered, go = hold_transfers(a, "write")
    handle = a.alloc(1)
    handle.buf[:1] = b"n"
    store = threading.Thread(target=a.batch_store, args=(["new"], [handle]))
    store.start()
    try:
        assert entered.wait(5)
        started = time.monotonic()
        other = a.alloc(1)
        a.free(other)
        assert a.has_local("old")
        assert read(a, ["old"]) == [b"o"]
        assert time.monotonic() - started < 1
    finally:
        go.set()
        store.join(5)
    assert read(a, ["new"]) == [b"n"]


def test_a_read_under_way_does_not_hold_up_alloc(world):
    a = world.client("a", pages=8, rdma_nonblocking_alloc=True)
    put(a, "k", b"v")
    entered, go = hold_transfers(a, "read")
    got = {}
    load = threading.Thread(target=lambda: got.setdefault("k", read(a, ["k"])))
    load.start()
    try:
        assert entered.wait(5)
        started = time.monotonic()
        handle = a.alloc(1)
        a.free(handle)
        assert time.monotonic() - started < 1
    finally:
        go.set()
        load.join(5)
    assert got["k"] == [b"v"]


def test_a_nonblocking_alloc_never_waits_and_its_store_gets_a_page(world):
    # alloc takes only a staging slot, even while the refill thread asks for
    # a region; the store then waits for that region instead of being skipped.
    a = world.client("a", pages=1, rdma_nonblocking_alloc=True)
    storage = a.rpc.storage
    asked, answer = threading.Event(), threading.Event()
    askers = []

    def slow_expand(action, payload):
        if action == "expand":
            askers.append(threading.current_thread().name)
            asked.set()
            answer.wait(5)
        return storage(action, payload)

    a.rpc.storage = slow_expand
    assert put(a, "k0", b"x")[0] is True  # the only page: the refill thread asks
    assert asked.wait(5)
    started = time.monotonic()
    handle = a.alloc(1)
    handle.buf[:1] = b"y"
    assert time.monotonic() - started < 0.5 and not handle.placed
    answer.set()
    assert a.batch_store(["k1"], [handle]) == [True]
    assert len(a.regions) >= 2  # the refill thread may already ask for a third
    assert read(a, ["k0", "k1"]) == [b"x", b"y"]
    assert set(askers) == {"maru-remote-cxl-refill"}  # never the allocating thread


def test_a_store_that_finds_no_page_gives_its_slots_back(world):
    a = world.client("a", pages=4, rdma_nonblocking_alloc=True)
    free_slots = a._staging.free_count()
    first, second = a.alloc(1), a.alloc(1)
    first.buf[:1] = b"x"
    original, asked = a._take_page, []

    def one_page(size):
        asked.append(size)
        if len(asked) > 1:
            raise MemoryError("Remote CXL regions are full")
        return original(size)

    a._take_page = one_page
    assert a.batch_store(["k1", "k2"], [first, second]) == [True, False]
    assert second.state == "freed" and not second.placed
    assert a._staging.free_count() == free_slots
    assert read(a, ["k1", "k2"]) == [b"x", None]


def test_the_refill_thread_evicts_ahead_when_no_region_is_left(world):
    a = world.client("a", pages=2, rdma_nonblocking_alloc=True, auto_expand=False)
    assert put(a, "k0", b"x")[0] is True
    assert put(a, "k1", b"y")[0] is True  # full: the refill thread evicts
    deadline = time.monotonic() + 5
    while not only_region(a).has_free():
        assert time.monotonic() < deadline
        time.sleep(0.01)
    assert put(a, "k2", b"z")[0] is True


def test_an_error_escaping_a_write_gives_its_pages_and_slots_back(world):
    # The handles are being sent, so the caller's free cannot return them.
    a = world.client("a", pages=4, rdma_nonblocking_alloc=True)
    free_slots = a._staging.free_count()
    handle = a.alloc(1)

    def broken(group):
        raise RuntimeError("unexpected")

    a._write_group = broken
    with pytest.raises(RuntimeError):
        a.batch_store(["k"], [handle])
    assert handle.state == "freed"
    assert only_region(a).free_count() == 4
    assert a._staging.free_count() == free_slots
    assert not a.has_unconfirmed_transfers()


def test_an_error_placing_a_page_gives_the_slots_and_pages_back(world):
    # Placement may now evict on the store thread: a malformed MaruServer
    # reply there must not leave the batch's slots or pages taken.
    a = world.client("a", pages=2, rdma_nonblocking_alloc=True, auto_expand=False)
    free_slots = a._staging.free_count()
    handles = [a.alloc(1) for _ in range(3)]
    for h, byte in zip(handles, b"xyz", strict=True):
        h.buf[:1] = bytes([byte])

    def malformed():
        raise ValueError("zip() argument 2 is shorter than argument 1")

    a._evict = malformed
    assert a.batch_store(["k0", "k1", "k2"], handles) == [True, True, False]
    for h in handles:
        a.free(h)  # the connector's cleanup: nothing left to give back
    assert [h.state for h in handles][2] == "freed"
    assert a._staging.free_count() == free_slots
    assert read(a, ["k0", "k1", "k2"]) == [b"x", b"y", None]


def test_a_batch_that_gets_no_page_starts_no_transfer(world):
    a = world.client("a", pages=4, rdma_nonblocking_alloc=True)
    free_slots = a._staging.free_count()
    handle = a.alloc(1)

    def full(size):
        raise MemoryError("Remote CXL regions are full")

    a._take_page = full
    assert a.batch_store(["k"], [handle]) == [False]
    assert handle.state == "freed" and a._transfers == 0
    assert a._staging.free_count() == free_slots


def test_an_interrupted_placement_gives_the_batch_back(world):
    a = world.client("a", pages=4, rdma_nonblocking_alloc=True)
    free_slots = a._staging.free_count()
    handles = [a.alloc(1) for _ in range(2)]
    original, asked = a._take_page, []

    def interrupted(size):
        asked.append(size)
        if len(asked) > 1:
            raise KeyboardInterrupt
        return original(size)

    a._take_page = interrupted
    with pytest.raises(KeyboardInterrupt):
        a.batch_store(["k0", "k1"], handles)
    assert [h.state for h in handles] == ["freed", "freed"]
    assert a._staging.free_count() == free_slots
    assert only_region(a).free_count() == 4


def test_a_free_while_the_client_is_busy_does_not_wait(world):
    a = world.client("a", pages=4, rdma_nonblocking_alloc=True)
    free_slots = a._staging.free_count()
    handle = a.alloc(1)
    holding, done = threading.Event(), threading.Event()

    def hold():
        with a._lock:
            holding.set()
            done.wait(5)

    holder = threading.Thread(target=hold)
    holder.start()
    assert holding.wait(5)
    started = time.monotonic()
    a.free(handle)  # no page yet: only its slot goes back
    assert time.monotonic() - started < 0.5 and handle.state == "freed"
    assert a._staging.free_count() == free_slots
    done.set()
    holder.join(5)
    assert only_region(a).free_count() == 4


def test_the_last_transfer_after_close_stops_the_data_path(world):
    # close gave up waiting for a WRITE and left the data path registered;
    # that WRITE ending stops it.
    a = world.client("a", pages=4, rdma_nonblocking_alloc=True, rdma_timeout_s=0.1)
    stopped = []
    original = a._stop_data_path

    def stop():
        stopped.append(1)
        original()

    a._stop_data_path = stop
    entered, go = hold_transfers(a, "write")
    handle = a.alloc(1)
    handle.buf[:1] = b"z"
    store = threading.Thread(target=a.batch_store, args=(["k"], [handle]))
    store.start()
    assert entered.wait(5)
    a.close()
    assert not stopped and a.has_unconfirmed_transfers()
    go.set()
    store.join(5)
    deadline = time.monotonic() + 5
    while not stopped:
        assert time.monotonic() < deadline
        time.sleep(0.05)
    assert stopped == [1] and not a.has_unconfirmed_transfers()
    world.clients.remove(a)


def test_a_write_unconfirmed_at_close_stops_the_data_path_once_released(world):
    # The heartbeat thread that reaps released transfers is gone after close.
    a = world.client("a", pages=2, rdma_timeout_s=0.01)
    agent = worker_agent("a")
    agent.stall = True
    handle = a.alloc(4)
    handle.buf[:4] = b"late"
    assert a.batch_store(["k"], [handle]) == [False]
    stopped = []
    original = a._stop_data_path

    def stop():
        stopped.append(1)
        original()

    a._stop_data_path = stop
    a.close()
    assert not stopped and a._data.staging is not None
    agent.stall = False
    agent.finish_stalled()
    deadline = time.monotonic() + 5
    while not stopped:
        assert time.monotonic() < deadline
        time.sleep(0.05)
    assert a._data.staging is None
    world.clients.remove(a)


def test_close_waits_for_a_transfer_under_way(world):
    a = world.client("a", pages=4, rdma_nonblocking_alloc=True)
    entered, go = hold_transfers(a, "write")
    handle = a.alloc(1)
    handle.buf[:1] = b"z"
    done = {}
    store = threading.Thread(
        target=lambda: done.setdefault("ok", a.batch_store(["k"], [handle]))
    )
    store.start()
    assert entered.wait(5)
    threads = list(a._threads)
    closer = threading.Thread(target=a.close)
    closer.start()
    for t in threads:
        t.join(5)
    time.sleep(0.2)
    # Its own threads are gone; close waits for the WRITE under way.
    assert closer.is_alive() and a.regions and a.has_unconfirmed_transfers()
    go.set()
    store.join(5)
    closer.join(5)
    assert not closer.is_alive() and done["ok"] == [True]
    world.clients.remove(a)


def test_a_region_given_back_during_its_write_keeps_no_key(world):
    # The give-back releases the region first, so the WRITE either landed
    # before or fails at the NIC; the commit must not leave its key found.
    a, b = world.client("a", pages=4, rdma_nonblocking_alloc=True), world.client("b")
    region = only_region(a)
    entered, go = hold_transfers(a, "write")
    handle = a.alloc(1)
    handle.buf[:1] = b"z"
    store = threading.Thread(target=a.batch_store, args=(["k"], [handle]))
    store.start()
    try:
        assert entered.wait(5)
        assert a._lock.acquire(timeout=1)  # the WRITE does not hold it
        try:
            a._give_back_rebooted(region)
        finally:
            a._lock.release()
    finally:
        go.set()
        store.join(5)
    assert "k" not in a._keys
    assert read(b, ["k"]) == [None]


# ---------------------------------------------------------- remote CXL agent failures


def test_a_remote_cxl_agent_restart_maps_the_regions_again(world):
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


def test_an_unreachable_region_is_set_aside_and_its_keys_removed(shared, monkeypatch):
    monkeypatch.setattr(lifecycle, "UNREACHABLE_KEYS_S", 0.05)
    a = shared.client("a", pages=2)
    put(a, "k1", b"1")
    first = only_region(a)
    down = next(
        n
        for n in shared.nodes
        if n.address == pick_agent(a.remote_cxl_agents, first.region_id)
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
    # Its key stays for a while, then no longer reports a hit that cannot
    # be read.
    assert a.batch_exists(["k1", "k2"]) == [True, True]
    time.sleep(0.1)
    a._retry_unreachable()
    assert a.batch_exists(["k1", "k2"]) == [False, True]
    assert handle.location.pool_id != first.pool_id
    down_regions = [r for r in a.regions.values() if not r.reachable]
    assert len(down_regions) <= len(a.remote_cxl_agents)
    # The node comes back on the same address: the region is used again.
    down._start()
    time.sleep(0.1)  # the back-off after the failed map ends
    a._retry_unreachable()
    assert first.reachable


def test_a_down_node_keeps_only_its_regions_from_going_back(shared):
    a = shared.client("a", pages=1)
    put(a, "k1", b"1")
    while (
        len({pick_agent(a.remote_cxl_agents, r.region_id) for r in a.regions.values()})
        < 2
    ):
        put(a, f"k{len(a.regions) + 1}", b"x")  # a region on each node
    down_address = pick_agent(a.remote_cxl_agents, only_region_on(a, 0).region_id)
    down = next(n for n in shared.nodes if n.address == down_address)
    kept = {
        r.region_id
        for r in a.regions.values()
        if pick_agent(a.remote_cxl_agents, r.region_id) == down_address
    }
    down.stop()
    a.close()
    for pool_id, *_ in shared.directory.departed():
        shared.directory.give_back(pool_id, "test")
    shared.keeper.run_once()
    # Regions on the live node went back; the down node's stay allocated.
    assert set(shared.cxl.regions) == kept


def test_a_region_no_agent_ever_mapped_goes_back_without_a_release(world):
    a = world.client("a", pages=1)
    put(a, "k", b"v")
    first = only_region(a)
    world.node.stop()
    a.rpc.unreachable.add("return_region")  # keep the new region until close
    # The full region asks for a new one it cannot map; the store is skipped.
    with pytest.raises(MemoryError):
        put(a, "k2", b"w")
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
    # The new region cannot be mapped and goes back. Every mapper of the pool
    # is down now, so "k" is not evicted for a page no WRITE could reach.
    with pytest.raises(MemoryError):
        put(a, "k2", b"w")
    assert len(a.regions) == 1 and world.cxl.released == [2]
    assert a.batch_exists(["k"]) == [True]
    # No more regions are asked for.
    with pytest.raises(MemoryError):
        put(a, "k3", b"x")
    assert world.cxl.released == [2] and len(world.cxl.regions) == 1


def test_a_failed_nixl_cleanup_keeps_the_staging_buffer_for_a_retry(world):
    # NIXL may still have the staging buffer registered with the NIC.
    a = world.client("a", pages=1)
    put(a, "k", b"v")
    data = a._data
    transport, staging = data.transport, data.staging
    close = transport.close

    def fail():
        raise RuntimeError("NIXL cleanup failed")

    transport.close = fail
    data.close()
    assert data.transport is transport and data.staging is staging
    transport.close = close
    data.close()
    assert data.transport is None and data.staging is None


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


def test_a_region_the_agent_failed_to_map_is_not_asked_again_at_once(
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


def test_a_failed_release_does_not_stop_the_other_regions_of_its_agent(world):
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


def test_a_restarted_remote_cxl_agent_keeps_the_keys(world):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"v")
    world.node.restart()  # the process only: the node's caches stay
    put(a, "k2", b"w")
    assert put(a, "k2", b"w")[0] is True
    assert b.batch_exists(["k"]) == [True]


def test_a_peer_is_connected_anew_after_its_agent_connection_broke(world):
    """A remote CXL agent that stopped answering for a while comes back.

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
    for engine in ("a", "b"):
        agent = worker_agent(engine)
        assert peer in agent.peers and peer not in agent.failed


def test_a_failed_peer_is_connected_anew_while_its_agent_connection_holds(world):
    """UCX failed a reader's connection, and no request broke the connection
    to the remote CXL agent; mapping another region of that node loads its
    metadata into a new connection."""
    a, b = world.client("a", pages=1), world.client("b")
    put(a, "k1", b"1")
    assert read(b, ["k1"]) == [b"1"]
    peer = world.node.agent.name
    worker_agent("b").fail_endpoint(peer)
    generation = b._data._client(world.node.address).generation
    assert put(a, "k2", b"2")[0] is True  # a second region of a, on the same node
    assert read(b, ["k2"]) == [b"2"]  # its first map by b
    assert b._data._client(world.node.address).generation == generation
    assert peer not in worker_agent("b").failed
    assert read(b, ["k1"]) == [b"1"]  # the first region maps again


def test_the_remote_cxl_agent_drops_the_connection_checks(world):
    a = world.client("a", pages=1)
    put(a, "k1", b"1")
    put(a, "k2", b"2")  # a second region: its map checks the loaded peer
    assert world.node.agent.notifs
    a._data.boots([world.node.address])
    assert world.node.agent.notifs == []


def test_a_restarted_remote_cxl_agents_old_peer_is_dropped(world):
    a = world.client("a")
    put(a, "k", b"1")
    old = world.node.agent.name
    world.node.restart()
    results = [put(a, "k2", b"2")[0] for _ in range(3)]
    assert results[-1] is True
    assert old not in worker_agent("a").peers
    assert world.node.agent.name in worker_agent("a").peers


def test_a_peer_nixl_keeps_is_not_loaded_into(world, monkeypatch):
    a = world.client("a")
    put(a, "k", b"1")
    client = a._data._client(world.node.address)
    client._drop()
    a._data.boots([world.node.address])  # a new connection to the mapper
    monkeypatch.setattr(a._data.transport, "disconnect", lambda name: False)
    assert put(a, "k2", b"2")[0] is False
    assert only_region(a).pool_id in a._data._map_failed  # tried again later
    monkeypatch.undo()
    time.sleep(0.1)
    assert put(a, "k3", b"3")[0] is True


def test_a_map_reply_from_an_ended_connection_is_not_loaded(world):
    a = world.client("a")
    put(a, "k", b"1")
    region = only_region(a)
    peer, generation = a._data._loaded[world.node.address]
    with pytest.raises(StorageUnavailableError, match="earlier connection"):
        a._data._load_peer(
            world.node.address, generation - 1, b"", region.pool_id, region.region_id
        )
    assert a._data._loaded[world.node.address] == (peer, generation)


def test_a_map_reply_from_an_ended_connection_is_mapped_again(world, monkeypatch):
    """Another thread loaded the peer over a newer connection after this
    reply came: the mapping it names went with the old connection."""
    a = world.client("a", pages=1)
    put(a, "k1", b"1")
    client = a._data._client(world.node.address)
    real_map = client.map
    generations = []

    def map_first_reply_stale(handle):
        reply = real_map(handle)
        generations.append(reply.generation)
        if len(generations) == 1:
            return dataclasses.replace(reply, generation=reply.generation - 1)
        return reply

    monkeypatch.setattr(client, "map", map_first_reply_stale)
    assert put(a, "k2", b"2")[0] is True  # a new region: mapped twice
    assert generations == [client.generation] * 2
    assert read(world.client("b"), ["k1", "k2"]) == [b"1", b"2"]


def test_a_reply_from_an_ended_connection_is_not_used_before_any_newer_load(
    world, monkeypatch
):
    a = world.client("a", pages=1)
    put(a, "k1", b"1")
    a._data._loaded.clear()  # no load over a newer connection yet
    client = a._data._client(world.node.address)
    real_map = client.map
    generations = []

    def map_first_reply_stale(handle):
        reply = real_map(handle)
        generations.append(reply.generation)
        if len(generations) == 1:
            return dataclasses.replace(reply, generation=reply.generation - 1)
        return reply

    monkeypatch.setattr(client, "map", map_first_reply_stale)
    real_load = a._data._load_peer
    loaded = []

    def record_load(address, generation, *args):
        loaded.append(generation)
        return real_load(address, generation, *args)

    monkeypatch.setattr(a._data, "_load_peer", record_load)
    assert put(a, "k2", b"2")[0] is True
    assert len(generations) == 2  # the stale reply was mapped again
    assert loaded == [client.generation]  # and never loaded


def test_keys_on_a_down_node_are_not_evicted_for_room_on_another(shared):
    """One of two remote CXL agents of the pool is down: eviction frees pages
    only in regions on the node that answers."""
    a = shared.client("a", pages=1)
    by_node = {}
    # One page per region: one region per key. Which node serves a region
    # follows a hash of its id and the agents' addresses, so store until both
    # nodes serve one (eight keys almost always suffice).
    for i in range(64):
        put(a, f"k{i}", bytes([i]))
        by_node = {}
        for region in a.regions.values():
            by_node.setdefault(
                pick_agent(a.remote_cxl_agents, region.region_id), []
            ).append(region)
        if i >= 7 and len(by_node) == 2:
            break
    assert len(by_node) == 2
    first = a._keys["k0"].pool_id  # the least recently used key
    down = next(
        n
        for n in shared.nodes
        if n.address == pick_agent(a.remote_cxl_agents, a.regions[first].region_id)
    )
    down.stop()
    a._data.boots([down.address])  # the heartbeat finds it down
    time.sleep(0.1)  # the back-off ends; the node still does not answer
    a.config.auto_expand = False
    assert put(a, "new", b"n")[0] is True
    on_down = [
        k
        for k, loc in a._keys.items()
        if loc.pool_id in {r.pool_id for r in by_node[down.address]}
    ]
    assert "k0" in on_down
    assert all(a.batch_exists(on_down))
    # The node answers again: its regions take pages again.
    down._start()
    time.sleep(0.1)
    a._data.boots([down.address])
    assert a._data.answering(down.address)
    assert all(a._writable(r) for r in by_node[down.address])


def test_a_store_does_not_evict_while_its_remote_cxl_agent_is_down(world):
    """The freed page could not be written, so the keys stay."""
    a = world.client("a", pages=1)
    put(a, "k", b"v")
    world.node.restart()
    world.node.stop()
    with pytest.raises(MemoryError):  # the connector skips this store
        put(a, "k2", b"w")
    world.node._start()
    assert a.batch_exists(["k"]) == [True]


def test_expanding_resumes_once_the_remote_cxl_agent_answers(world):
    a = world.client("a", pages=1)
    put(a, "k", b"v")
    world.node.restart()  # same address; the first map after it fails over
    world.node.stop()
    with pytest.raises(MemoryError):
        put(a, "k2", b"w")
    assert not a._may_expand()  # the only remote CXL agent is down
    time.sleep(0.1)  # the back-off ends; no store waits on the node meanwhile
    assert not a._may_expand()
    world.node._start()
    time.sleep(0.1)
    a._check_boots()  # the heartbeat's boot check gets an answer
    assert a._may_expand()
    assert put(a, "k3", b"x")[0] is True


def test_a_region_whose_agent_restarts_in_time_keeps_its_keys(world):
    """The owner's map of its region fails while the remote CXL agent restarts:
    the region is set aside, and the retry maps it again within the grace,
    so its keys stay and another worker reads them."""
    a, b = world.client("a", pages=2), world.client("b")
    put(a, "k1", b"1")
    first = only_region(a)
    world.node.restart()  # a's access to the region is stale now
    world.node.stop()
    assert put(a, "k2", b"2")[0] is False  # the map fails: set aside
    assert not first.reachable
    assert a.batch_exists(["k1"]) == [True]  # kept during the grace
    world.node._start()
    time.sleep(0.1)
    a._retry_unreachable()
    assert first.reachable
    assert read(b, ["k1"]) == [b"1"]


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
        raise AssertionError("a down remote CXL agent was asked again")

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


def test_an_interrupt_before_a_read_is_posted_frees_its_slot_and_lease(
    world, monkeypatch
):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"v")
    assert read(b, ["k"]) == [b"v"]

    def interrupt(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(b._data.transport, "read", interrupt)
    free = b._staging.free_count()
    with pytest.raises(KeyboardInterrupt):
        b.batch_retrieve(["k"])
    # No transfer was posted: the slot goes back (an interrupt while the
    # READ runs keeps it: test_an_interrupted_read_keeps_its_slots_until_it_ends).
    assert b._staging.free_count() == free
    # No lease of b pins "k": its owner can still evict it.
    victims = world.directory.execute("victims", {**a._credentials, "count": 1})
    assert [v["key"] for v in victims["result"]["entries"]] == ["k"]


def test_a_reboot_ends_the_leases_on_the_keys_of_its_regions(world):
    """A reader holding a lease learns the bytes may be lost; nobody finds them."""
    a, b, c = world.client("a"), world.client("b"), world.client("c")
    put(a, "k", b"v")
    region = only_region(a)
    lease = world.directory.execute("acquire", {**b._credentials, "keys": ["k"]})
    lease_id = lease["result"]["lease_id"]
    world.node.restart(reboot=True)
    a._check_boots()
    assert c.batch_exists(["k"]) == [False]
    released = world.directory.execute(
        "release", {**b._credentials, "lease_id": lease_id}
    )
    assert released["result"] == {"held": False}
    assert region.region_id in world.cxl.released
    assert a.regions == {}
    assert put(a, "k2", b"w")[0] is True  # a new region
    assert read(c, ["k2"]) == [b"w"]


def test_a_reboot_a_store_notices_ends_the_leases_on_the_keys_of_its_regions(world):
    """The store's map of its region sees the new boot, as the heartbeat does."""
    a, b, c = world.client("a"), world.client("b"), world.client("c")
    put(a, "k", b"v")
    region = only_region(a)
    lease = world.directory.execute("acquire", {**b._credentials, "keys": ["k"]})
    lease_id = lease["result"]["lease_id"]
    world.node.restart(reboot=True)
    a._check_boots = lambda: pytest.fail("the store must notice the reboot")
    assert put(a, "k2", b"w")[0] is False  # the old registration is gone
    assert put(a, "k2", b"w")[0] is True  # a new region on the new boot
    assert c.batch_exists(["k", "k2"]) == [False, True]
    released = world.directory.execute(
        "release", {**b._credentials, "lease_id": lease_id}
    )
    assert released["result"] == {"held": False}
    assert region.region_id in world.cxl.released
    assert region.pool_id not in a.regions


def test_a_worker_whose_only_region_went_back_closes_its_session(world):
    a = world.client("a")
    put(a, "k", b"v")
    world.node.restart(reboot=True)
    a._check_boots()
    assert a.regions == {}
    a.close()
    assert "a" not in world.directory._engines
    again = world.client("a")  # the engine id is free at once
    assert put(again, "k2", b"w")[0] is True


def test_a_rebooted_region_is_replaced_without_auto_expand(world):
    a = world.client("a", auto_expand=False)
    put(a, "k", b"v")
    world.node.restart(reboot=True)
    a._check_boots()
    assert put(a, "k2", b"w")[0] is True
    assert len(a.regions) == 1


def test_a_rebooted_region_that_could_not_go_back_is_given_back_later(world):
    a, c = world.client("a"), world.client("c")
    put(a, "k", b"v")
    region = only_region(a)
    a.rpc.unreachable.add("return_region")
    world.node.restart(reboot=True)
    a._check_boots()
    assert region.retired and a.regions == {region.pool_id: region}
    assert c.batch_exists(["k"]) == [False]  # its unleased keys were removed
    a.rpc.unreachable.clear()
    a._retry_returns()
    assert a.regions == {} and a._unreturned == []
    assert region.region_id in world.cxl.released


def test_a_commit_confirmed_after_its_region_went_back_records_no_key(world):
    a, c = world.client("a"), world.client("c")
    a.rpc.lost_replies.add("commit")
    assert put(a, "k", b"v")[0] is False  # outcome unknown: kept pending
    a.rpc.lost_replies.clear()
    world.node.restart(reboot=True)
    a._check_boots()
    a._resolve_pending()
    assert a._pending == {} and "k" not in a._keys
    assert c.batch_exists(["k"]) == [False]


def test_an_unconfirmed_lease_release_is_sent_again(world):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"v")
    b.rpc.lost_replies.add("release")
    assert read(b, ["k"]) == [None]  # the lease's end was not confirmed
    b.rpc.lost_replies.clear()
    assert b._unreleased
    b._retry_lease_releases()
    assert b._unreleased == []
    victims = world.directory.execute("victims", {**a._credentials, "count": 1})
    assert [v["key"] for v in victims["result"]["entries"]] == ["k"]


def test_a_lease_release_that_never_arrived_is_sent_again(world):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"v")
    b.rpc.unreachable.add("release")
    assert read(b, ["k"]) == [None]
    victims = world.directory.execute("victims", {**a._credentials, "count": 1})
    assert victims["result"]["entries"] == []  # the lease still pins the key
    b.rpc.unreachable.clear()
    b._retry_lease_releases()
    victims = world.directory.execute("victims", {**a._credentials, "count": 1})
    assert [v["key"] for v in victims["result"]["entries"]] == ["k"]


def test_a_reboot_is_noticed_without_writing_to_the_region(world):
    a, b = world.client("a"), world.client("b")
    put(a, "k", b"v")
    world.node.restart(reboot=True)
    a._check_boots()  # what the background remap does every beat
    assert b.batch_exists(["k"]) == [False]


def test_a_wrong_handle_is_refused_by_the_remote_cxl_agent(world):
    a = world.client("a")
    region = only_region(a)
    a._data.forget(region.pool_id)
    forged = {**region.handle, "auth_token": 1}
    with pytest.raises(StorageUnavailableError, match="could not be mapped"):
        a._data.region("forged", forged, a.remote_cxl_agents)


# ----------------------------------------------------------------- shared pool


def test_a_shared_pool_spreads_regions_and_readers_follow_the_owner(shared):
    owners = [shared.client(f"w{i}") for i in range(4)]
    for i, c in enumerate(owners):
        put(c, f"k{i}", bytes([i]))
    reader = shared.client("reader")
    assert read(reader, [f"k{i}" for i in range(4)]) == [bytes([i]) for i in range(4)]
    for c in owners:
        region = only_region(c)
        node = pick_agent(c.remote_cxl_agents, region.region_id)
        # The owner and the reader map the region through the same node.
        holder = next(n for n in shared.nodes if n.address == node)
        assert len(holder.service.holders()[region.region_id]) == 2
        other = next(n for n in shared.nodes if n.address != node)
        assert region.region_id not in other.service.holders()


def test_regions_spread_over_the_remote_cxl_agents():
    agents = ["mem2:9860", "mem3:9860"]
    picks = [pick_agent(agents, region_id) for region_id in range(1, 201)]
    assert 70 <= picks.count("mem2:9860") <= 130
    # Every Handler with the same list picks the same node for a region.
    assert picks == [pick_agent(list(reversed(agents)), r) for r in range(1, 201)]


# -------------------------------------------------------------------- setup


def test_a_server_without_remote_cxl_or_remote_cxl_agents_is_rejected(world):
    class OldRpc(Rpc):
        def handshake(self):
            return {**super().handshake(), "capabilities": [STORAGE_CAPABILITY]}

    class NoAgentRpc(Rpc):
        def handshake(self):
            return {**super().handshake(), "remote_cxl_agents": []}

    config = MaruConfig(
        storage_backend="remote_cxl",
        engine_id="a",
        cache_namespace="ns",
        pool_size=PAGE,
        chunk_size_bytes=PAGE,
    )
    for rpc_class in (OldRpc, NoAgentRpc):
        c = RemoteCxlStorageClient(
            config,
            rpc_class(world.directory, [world.node.address]),
            transport_factory=lambda name: NixlTransport(
                name, agent=FakeNixlAgent(name)
            ),
        )
        with pytest.raises(StorageError, match="remote-cxl-agent"):
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
