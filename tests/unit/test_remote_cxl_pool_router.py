# SPDX-License-Identifier: Apache-2.0
"""Several remote CXL pools behind one worker: routing, grouping and shared staging."""

import pytest

import maru_handler.storage.remote_cxl.router as router_module
from maru_common.config import MaruConfig
from maru_common.storage_types import StorageUnavailableError
from maru_handler.storage.remote_cxl import PoolRouter
from maru_remote.transport import NixlTransport
from maru_server.replica_directory import ReplicaDirectory
from tests.unit.remote_fakes import (
    FakeClock,
    FakeCxl,
    FakeMemoryNode,
    FakeNixlAgent,
    key_file,
    reset_fake_agents,
)
from tests.unit.test_remote_cxl_storage import PAGE, Rpc

URLS = ["tcp://meta-a:5555", "tcp://meta-b:5555"]


class DownRpc(Rpc):
    """A MaruServer that does not answer until ``up`` is set."""

    up = False

    def handshake(self):
        if not self.up:
            raise StorageUnavailableError("MaruServer is not reachable")
        return super().handshake()


@pytest.fixture
def pools():
    """Two pools, each with its own CXL, RM, memory node and MaruServer."""
    reset_fake_agents()
    clock = FakeClock()
    out = []
    for name in ("a", "b"):
        cxl = FakeCxl()
        directory = ReplicaDirectory(
            reserve_cxl=cxl.reserve,
            release_cxl=cxl.release,
            clock=clock,
            session_ttl=30,
        )
        out.append((directory, FakeMemoryNode(cxl, f"mem-{name}")))
    yield out
    for _, node in out:
        node.stop()
    reset_fake_agents()


def router(pools, engine="w", metadata_only=False, pages=8, rpcs=None, **kwargs):
    config = MaruConfig(
        storage_backend="remote_cxl",
        server_url=",".join(URLS),
        engine_id=engine,
        cache_namespace="ns",
        node_id="worker",
        pool_size=pages * PAGE,
        chunk_size_bytes=PAGE,
        rdma_staging_size=16 * PAGE,
        metadata_only=metadata_only,
        remote_cxl_agent_key_file=key_file(),
        **kwargs,
    )
    r = PoolRouter(
        config,
        rpcs or [Rpc(directory, [node.address]) for directory, node in pools],
        URLS,
        transport_factory=lambda name: NixlTransport(name, agent=FakeNixlAgent(name)),
    )
    r.connect()
    return r


def pool_index(r, handle):
    for i, c in enumerate(r.clients):
        if handle.location.pool_id in c.regions:
            return i
    raise AssertionError("allocation outside every pool")


def route_keys_per_pool(r):
    """One route key that lands in each pool."""
    found = {}
    for i in range(200):
        handle = r.alloc(1, route_key=f"req-{i}")
        found.setdefault(pool_index(r, handle), f"req-{i}")
        r.free(handle)
        if len(found) == len(r.clients):
            return [found[i] for i in range(len(r.clients))]
    raise AssertionError("a pool never received a route key")


def route_key_of(r, index):
    for i in range(200):
        key = f"req-{i}"
        if r._pick(key) is r.clients[index]:
            return key
    raise AssertionError("no route key for that pool")


def store(r, route_key, keys):
    handles = []
    for key in keys:
        handle = r.alloc(len(key), route_key=route_key)
        handle.buf[: len(key)] = key.encode()
        handles.append(handle)
    return r.batch_store(keys, handles), handles


def read(r, keys, route_key):
    leases = r.batch_retrieve(keys, route_key=route_key)
    out = [bytes(lease.view) if lease else None for lease in leases]
    for lease in leases:
        if lease:
            lease.release()
    return out


def test_a_request_stays_in_the_pool_its_route_key_picks(pools):
    r = router(pools)
    try:
        for route_key in route_keys_per_pool(r):
            ok, handles = store(r, route_key, [f"{route_key}-0", f"{route_key}-1"])
            assert ok == [True, True]
            assert len({h.location.pool_id for h in handles}) == 1
            assert read(r, [f"{route_key}-0", f"{route_key}-1"], route_key) == [
                f"{route_key}-0".encode(),
                f"{route_key}-1".encode(),
            ]
    finally:
        r.close()


def test_one_store_batch_spanning_two_pools_commits_each_to_its_server(pools):
    r = router(pools)
    try:
        first, second = route_keys_per_pool(r)
        h1 = r.alloc(2, route_key=first)
        h2 = r.alloc(2, route_key=second)
        h1.buf[:2], h2.buf[:2] = b"p1", b"p2"
        assert r.batch_store(["k1", "k2"], [h1, h2]) == [True, True]
        for (directory, _), key, other in zip(
            pools, ["k1", "k2"], ["k2", "k1"], strict=True
        ):
            reply = directory.execute(
                "probe", {"engine_id": "w", "namespace": "ns", "keys": [key, other]}
            )
            assert reply["result"]["results"] == [True, False]
    finally:
        r.close()


def test_a_nonblocking_alloc_is_stored_and_freed_in_its_own_pool(pools):
    # The engine thread's alloc takes only a staging slot: the router routes
    # it by the client that allocated it, before it has a page.
    r = router(pools, rdma_nonblocking_alloc=True)
    try:
        free_slots = r.data.staging.free_count()
        first, second = route_key_of(r, 0), route_key_of(r, 1)
        h1 = r.alloc(2, route_key=first)
        h2 = r.alloc(2, route_key=second)
        dropped = r.alloc(2, route_key=second)
        assert not (h1.placed or h2.placed)
        h1.buf[:2], h2.buf[:2] = b"p1", b"p2"
        assert r.batch_store(["k1", "k2"], [h1, h2]) == [True, True]
        assert [pool_index(r, h1), pool_index(r, h2)] == [0, 1]
        r.free(dropped)
        assert dropped.state == "freed"
        assert r.data.staging.free_count() == free_slots
        assert read(r, ["k1"], first) == [b"p1"]
        assert read(r, ["k2"], second) == [b"p2"]
    finally:
        r.close()


def test_another_worker_and_the_scheduler_pick_the_same_pool(pools):
    # The connector builds a worker's scheduler-side handler with the
    # worker's own engine id, so its probes see the worker's session.
    writer, reader = router(pools, "w1"), router(pools, "w2")
    scheduler = router(pools, "w2", metadata_only=True)
    try:
        for route_key in route_keys_per_pool(writer):
            store(writer, route_key, [route_key])
            assert scheduler.batch_exists([route_key], route_key=route_key) == [True]
            assert read(reader, [route_key], route_key) == [route_key.encode()]
    finally:
        writer.close()
        reader.close()
        scheduler.close()


def test_alloc_needs_a_route_key_and_probes_fall_back_to_the_first_key(pools):
    r = router(pools)
    try:
        with pytest.raises(ValueError, match="route key"):
            r.alloc(1)
        route_key = route_keys_per_pool(r)[1]
        store(r, route_key, [route_key])
        assert r.batch_exists([route_key, "missing"]) == [True, False]
        assert r.batch_exists([]) == [] and r.batch_retrieve([]) == []
    finally:
        r.close()


def test_pools_share_one_staging_buffer_and_one_nixl_agent(pools):
    r = router(pools)
    try:
        assert all(c._data is r.data for c in r.clients)
        workers = [n for n in FakeNixlAgent.registry if n.startswith("maru-worker")]
        assert len(workers) == 1
        free_slots = r.data.staging.free_count()
        first, second = route_keys_per_pool(r)
        handles = [r.alloc(1, route_key=first), r.alloc(1, route_key=second)]
        assert r.data.staging.free_count() == free_slots - 2
        for handle in handles:
            r.free(handle)
        assert r.data.staging.free_count() == free_slots
    finally:
        r.close()


def test_the_pools_share_one_load_reserve(pools):
    # 16 staging slots, half kept for loads: the stores of both pools
    # together get 8.
    r = router(pools, pages=16, rdma_load_reserve=0.5)
    try:
        first, second = route_keys_per_pool(r)
        handles = [r.alloc(1, route_key=k) for k in [first, second] * 4]
        with pytest.raises(MemoryError):
            r.alloc(1, route_key=first)
        assert r.retrieve_capacity() == 8
        r.data.staging.cuda_registered = True
        assert r.storage_gpu_accessible() is True
        for handle in handles:
            r.free(handle)
    finally:
        r.close()


def test_close_keeps_staging_while_a_late_transfer_may_land(pools):
    r = router(pools, rdma_timeout_s=0.01)
    agent = FakeNixlAgent.registry[
        next(n for n in FakeNixlAgent.registry if n.startswith("maru-worker"))
    ]
    route_key = route_keys_per_pool(r)[0]
    agent.stall = True
    ok, _ = store(r, route_key, ["k"])
    assert ok == [False]
    r.close()
    # Unmapping now would let the NIC write into freed host memory.
    assert r.data.staging is not None
    agent.finish_stalled()


def test_router_rejects_mismatched_rpcs_and_urls(pools):
    config = MaruConfig(
        storage_backend="remote_cxl",
        server_url=",".join(URLS),
        engine_id="e",
        cache_namespace="ns",
        pool_size=PAGE,
        chunk_size_bytes=PAGE,
    )
    with pytest.raises(ValueError, match="distinct MaruServer URL"):
        PoolRouter(config, [Rpc(pools[0][0], [])], URLS)
    with pytest.raises(ValueError, match="distinct MaruServer URL"):
        PoolRouter(
            config,
            [Rpc(pools[0][0], []), Rpc(pools[1][0], [])],
            [URLS[0], URLS[0]],
        )


def test_a_pool_down_at_start_misses_until_it_answers(pools, monkeypatch):
    monkeypatch.setattr(router_module, "POOL_RETRY_S", 0.05)
    (dir_a, node_a), (dir_b, node_b) = pools
    down = DownRpc(dir_b, [node_b.address])
    r = router(pools, rpcs=[Rpc(dir_a, [node_a.address]), down])
    try:
        assert r.connected and not r.clients[1].connected
        key = route_key_of(r, 1)
        assert r.batch_exists([key]) == [False]
        assert r.batch_retrieve([key]) == [None]
        with pytest.raises(MemoryError, match="unavailable"):
            r.alloc(1, route_key=key)
        other = route_key_of(r, 0)
        assert store(r, other, [other])[0] == [True]  # the other pool works
        down.up = True
        r._retry.join(timeout=5)
        assert r.clients[1].connected
        assert store(r, key, [key])[0] == [True]
    finally:
        r.close()


def test_the_router_reports_no_pool_available(pools):
    rpcs = [DownRpc(dir_, [node.address]) for dir_, node in pools]
    rpcs[0].up = True
    r = router(pools, rpcs=rpcs)
    try:
        r._ensure()  # one pool is enough
        r.clients[0].close()
        with pytest.raises(StorageUnavailableError):
            r._ensure()
    finally:
        r.close()


def test_no_pool_answering_fails_the_connect(pools):
    rpcs = [DownRpc(directory, [node.address]) for directory, node in pools]
    with pytest.raises(StorageUnavailableError):
        router(pools, rpcs=rpcs)
