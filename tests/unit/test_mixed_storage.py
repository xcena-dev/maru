# SPDX-License-Identifier: Apache-2.0
"""Mixed placement, typed CXL ownership and failure handling (mock RM only)."""

from dataclasses import asdict

import pytest

from maru_common.config import MaruConfig
from maru_common.storage_types import (
    MIXED_STORAGE_CAPABILITY,
    StorageError,
    StorageUnavailableError,
)
from maru_handler.storage.client import CpuStorageClient
from maru_server import MaruServer
from tests.unit.test_cpu_storage import DirectRpc, put


class MixedRpc(DirectRpc):
    def handshake(self):
        return {
            **super().handshake(),
            "capabilities": [
                *super().handshake()["capabilities"],
                MIXED_STORAGE_CAPABILITY,
            ],
        }


def mixed_client(
    server, write_order=("cpu", "cxl"), read_order=("cpu", "cxl"), **kwargs
):
    config = MaruConfig(
        storage_backend="mixed",
        engine_id="mixed-engine",
        cache_namespace="revision-1",
        pool_size=128,
        cxl_pool_size=128,
        chunk_size_bytes=64,
        write_order=write_order,
        read_order=read_order,
        **kwargs,
    )
    client = CpuStorageClient(config, MixedRpc(server.replica_directory))
    client.connect()
    return client


@pytest.fixture
def server():
    instance = MaruServer()
    yield instance
    instance.close()


@pytest.mark.parametrize("order", [("cpu", "cxl"), ("cxl", "cpu")])
def test_fixed_order_spills_into_other_pool_and_restores_in_request_order(
    server, order
):
    client = mixed_client(server, write_order=order)
    try:
        handles = [put(client, f"key-{i}", bytes([i])) for i in range(4)]
        assert [h.location.medium for h in handles] == [order[0]] * 2 + [order[1]] * 2
        with pytest.raises(MemoryError, match="Both CPU and CXL"):
            client.alloc(1)
        keys = ["key-3", "key-0", "missing", "key-2", "key-1"]
        assert client.batch_exists(keys) == [True, True, False, True, True]
        infos = client.batch_retrieve(keys)
        assert [bytes(i.view) if i else None for i in infos] == [
            b"\x03",
            b"\x00",
            None,
            b"\x02",
            b"\x01",
        ]
        for info in infos:
            if info:
                info.release()
        usage = server.get_usage()
        assert {p["medium"] for p in usage["l1_storage"]["pools"]} == {"cpu", "cxl"}
        assert len(usage["cpu_storage"]["pools"]) == 1
        assert sum(i["used"] for i in usage["instances"]) == 2
        assert client._credentials["session_token"] not in repr(usage)
        assert server._kv_manager.get_stats()["total_entries"] == 0
    finally:
        client.close()
    assert server._allocation_manager.get_stats()["num_allocations"] == 0


@pytest.mark.parametrize("read_order", [("cpu", "cxl"), ("cxl", "cpu")])
def test_read_order_selects_replica_independently_of_write_order(server, read_order):
    client = mixed_client(
        server, write_order=tuple(reversed(read_order)), read_order=read_order
    )
    try:
        for medium in ("cpu", "cxl"):
            handle = client.pool.pools[medium].alloc(2)
            handle.buf[:] = b"kv"
            assert client.batch_store(["same-key"], [handle]) == [True]
        info = client.batch_retrieve(["same-key"])[0]
        assert info.location.medium == read_order[0]
        assert bytes(info.view) == b"kv"
        info.release()
        assert [
            p["replicas"] for p in server.replica_directory.get_usage()["pools"]
        ] == [1, 1]
    finally:
        client.close()


def test_mixed_unknown_commit_keeps_both_backings_until_resolution(server):
    client = mixed_client(server)
    try:
        handles = [client.pool.pools[m].alloc(2) for m in ("cpu", "cxl")]
        for handle in handles:
            handle.buf[:] = b"kv"
        client.rpc.drop_commit = True
        assert client.batch_store(["cpu-key", "cxl-key"], handles) == [False, False]
        for handle in handles:
            client.free(handle)
            assert handle.state == "quarantined"
        client.rpc.drop_commit = False
        client._resolve_pending()
        infos = client.batch_retrieve(["cxl-key", "cpu-key"])
        assert [i.location.medium for i in infos] == ["cxl", "cpu"]
        for info in infos:
            info.release()
    finally:
        client.close()


def test_mixed_close_cannot_return_cxl_while_an_exported_view_exists(server):
    client = mixed_client(server, write_order=("cxl", "cpu"))
    handle = put(client)
    exported = handle.buf[:]
    region = client.pool.pools["cxl"]._handle.region_id
    with pytest.raises(RuntimeError, match="exported CXL buffers"):
        client.close()
    assert server._allocation_manager.get_handle(region) is not None
    exported.release()
    client.close()
    assert server._allocation_manager.get_handle(region) is None


def test_mixed_expiry_hides_both_media_without_reusing_live_cxl(server):
    client = mixed_client(server)
    try:
        for medium in ("cpu", "cxl"):
            h = client.pool.pools[medium].alloc(1)
            assert client.batch_store([medium], [h]) == [True]
        server.replica_directory._sessions[
            client._credentials["session_token"]
        ].deadline = 0
        assert client.batch_exists(["cpu", "cxl"]) == [False, False]
        assert server._allocation_manager.get_stats()["num_allocations"] == 1
        with pytest.raises(StorageError, match="expired"):
            client.batch_retrieve(["cpu", "cxl"])
    finally:
        client.close()


def test_mixed_open_is_idempotent_and_lost_reply_cleanup_releases_both_pools(server):
    rpc = MixedRpc(server.replica_directory)
    original = rpc.storage

    def lose_open(action, payload):
        result = original(action, payload)
        if action == "open":
            assert original(action, payload) == result
            assert server._allocation_manager.get_stats()["num_allocations"] == 1
            raise StorageUnavailableError("open reply lost")
        return result

    rpc.storage = lose_open
    config = MaruConfig(
        storage_backend="mixed",
        engine_id="a",
        cache_namespace="n",
        pool_size=128,
        cxl_pool_size=128,
        chunk_size_bytes=64,
    )
    with pytest.raises(StorageUnavailableError):
        CpuStorageClient(config, rpc).connect()
    assert not server.replica_directory.get_usage()["pools"]
    assert server._allocation_manager.get_stats()["num_allocations"] == 0


def test_mixed_location_cannot_use_a_cpu_pool_id_as_cxl(server):
    client = mixed_client(server)
    try:
        handle = client.alloc(1)
        location = {**asdict(handle.location), "medium": "cxl"}
        with pytest.raises(StorageError, match="another pool"):
            client._call(
                "commit", op_id="bad", entries=[{"key": "bad", "location": location}]
            )
        assert not client.batch_exists(["bad"])[0]
    finally:
        client.close()


def test_mixed_read_lease_prevents_either_pool_from_being_retired(server):
    client = mixed_client(server, write_order=("cxl", "cpu"))
    put(client)
    info = client.batch_retrieve(["prefix"])[0]
    try:
        with pytest.raises(RuntimeError, match="read leases"):
            client.close()
        assert bytes(info.view) == b"kv"
        assert len(server.replica_directory.get_usage()["pools"]) == 2
    finally:
        info.release()
        client.close()


def test_mixed_map_failure_rolls_back_server_reservations(server, monkeypatch):
    def fail_map(*args, **kwargs):
        raise PermissionError("DAX mapping not permitted")

    monkeypatch.setattr("maru_handler.storage.mixed.CxlPool", fail_map)
    with pytest.raises(PermissionError):
        mixed_client(server)
    assert not server.replica_directory.get_usage()["pools"]
    assert server._allocation_manager.get_stats()["num_allocations"] == 0


def test_mixed_metadata_only_client_has_no_pools(server):
    client = mixed_client(server, metadata_only=True)
    try:
        assert client.pool is None
        assert not server.replica_directory.get_usage()["pools"]
        assert server._allocation_manager.get_stats()["num_allocations"] == 0
    finally:
        client.close()


def test_mixed_requires_explicit_server_capability():
    server = MaruServer(enable_cxl=False)
    config = MaruConfig(
        storage_backend="mixed",
        engine_id="a",
        cache_namespace="n",
        cxl_pool_size=1024**2,
    )
    try:
        with pytest.raises(StorageError, match="CXL-enabled server"):
            CpuStorageClient(config, DirectRpc(server.replica_directory)).connect()
        assert not server.replica_directory.get_usage()["pools"]
    finally:
        server.close()
