# SPDX-License-Identifier: Apache-2.0
"""CPU directory and allocation failures without a GPU or resource manager."""

from dataclasses import asdict

import pytest

from maru_common.config import MaruConfig
from maru_common.storage_types import (
    STORAGE_CAPABILITY,
    CpuLocation,
    StorageError,
    StorageUnavailableError,
)
from maru_handler.storage.client import CpuStorageClient
from maru_handler.storage.cpu import CpuPool
from maru_server.replica_directory import ReplicaDirectory


class DirectRpc:
    """Exercise real directory semantics while injecting reply loss."""

    def __init__(self, directory):
        self.directory = directory
        self.drop_commit = False
        self.reject_commit = False

    def connect(self):
        pass

    def close(self):
        pass

    def handshake(self):
        return {
            "capabilities": [STORAGE_CAPABILITY],
            "server_epoch": self.directory.server_epoch,
        }

    def storage(self, action, payload):
        if action == "commit" and self.reject_commit:
            raise StorageError("rejected before mutation")
        result = self.directory.execute(action, payload)
        if action == "commit" and self.drop_commit:
            raise StorageUnavailableError("reply lost after commit")
        if not result["success"]:
            raise StorageError(result["error"])
        return result["result"]


def cpu_client(directory, engine="a", **kwargs):
    config = MaruConfig(
        storage_backend="cpu",
        engine_id=engine,
        cache_namespace="weights-revision-v1",
        node_id="node",
        pool_size=128,
        chunk_size_bytes=64,
        **kwargs,
    )
    client = CpuStorageClient(config, DirectRpc(directory))
    client.connect()
    return client


def put(client, key="prefix", value=b"kv"):
    handle = client.alloc(len(value))
    handle.buf[:] = value
    assert client.batch_store([key], [handle]) == [True]
    return handle


def test_owner_scoped_hits_and_multiple_cpu_replicas():
    directory = ReplicaDirectory()
    a = cpu_client(directory)
    b = cpu_client(directory, "b")
    scheduler = cpu_client(directory, metadata_only=True)
    try:
        put(a)
        assert scheduler.batch_exists(["prefix", "absent"]) == [True, False]
        assert b.batch_exists(["prefix"]) == [False]
        assert b.batch_retrieve(["prefix"]) == [None]
        put(b)  # Another CPU owner is not a duplicate placement.
        assert len(directory.get_usage()["pools"]) == 2
        for client in (a, b):
            info = client.batch_retrieve(["prefix"])[0]
            assert bytes(info.view) == b"kv"
            info.release()
    finally:
        scheduler.close()
        a.close()
        b.close()


def test_pool_full_duplicate_and_read_lease_close():
    directory = ReplicaDirectory()
    client = cpu_client(directory)
    put(client)
    put(client)  # Duplicate allocation is returned to the local allocator.
    assert client.pool.usage()["allocated_bytes"] == 64
    put(client, "second")
    with pytest.raises(MemoryError):
        client.alloc(1)
    infos = client.batch_retrieve(["prefix", "second"])
    with pytest.raises(RuntimeError, match="Release CPU read leases"):
        client.close()
    infos[0].release()
    infos[0].release()  # Idempotent local release.
    assert directory.get_usage()["pools"][0]["read_leases"] == 1
    infos[1].release()
    assert directory.get_usage()["pools"][0]["read_leases"] == 0
    client.close()
    client.close()
    assert directory.get_usage()["pools"] == []


def test_commit_reply_loss_retains_bytes_and_resolves_once():
    directory = ReplicaDirectory()
    client = cpu_client(directory)
    try:
        handle = client.alloc(2)
        handle.buf[:] = b"kv"
        client.rpc.drop_commit = True
        assert client.batch_store(["prefix"], [handle]) == [False]
        client.free(handle)  # Connector best-effort cleanup cannot reuse it.
        assert handle.state == "quarantined"
        assert client.pool.usage()["quarantined_bytes"] == 64
        assert client.batch_exists(["prefix"]) == [True]
        # Discovery can see the committed key, but local resolve must refuse
        # it until the unknown commit's ownership has been reconciled.
        with pytest.raises(ValueError, match="not ready"):
            client.batch_retrieve(["prefix"])
        client.rpc.drop_commit = False
        client._resolve_pending()
        assert handle.state == "ready"
        assert client.has_local("prefix")
        info = client.batch_retrieve(["prefix"])[0]
        assert bytes(info.view) == b"kv"
        info.release()
        assert directory.get_usage()["pools"][0]["replicas"] == 1
    finally:
        client.close()


def test_explicit_commit_rejection_frees_page():
    client = cpu_client(ReplicaDirectory())
    try:
        handle = client.alloc(2)
        client.rpc.reject_commit = True
        assert client.batch_store(["prefix"], [handle]) == [False]
        assert client.pool.usage()["allocated_bytes"] == 0
    finally:
        client.close()


def test_generation_and_pool_identity_reject_stale_locations():
    pool = CpuPool("pool", 64, 64)
    first = pool.alloc(8)
    location = first.location
    pool.free(first)
    second = pool.alloc(8)
    assert second.location.offset == location.offset
    assert second.location.generation > location.generation
    second.state = "ready"
    with pytest.raises(ValueError, match="Stale or foreign"):
        pool.resolve(location)
    with pytest.raises(ValueError, match="does not belong"):
        pool.free(first)
    pool.close()


def test_expired_owner_is_not_a_hit_and_grant_stays_charged(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("maru_server.replica_directory.time.monotonic", lambda: now[0])
    directory = ReplicaDirectory(capacity_limit=128, session_ttl=30)
    client = cpu_client(directory)
    scheduler = cpu_client(directory, metadata_only=True)
    try:
        put(client)
        now[0] += 31
        assert scheduler.batch_exists(["prefix"]) == [False]
        assert not directory.get_usage()["pools"][0]["active"]
        with pytest.raises(StorageError, match="host capacity"):
            cpu_client(directory, "b")
        with pytest.raises(StorageError, match="expired"):
            client.batch_retrieve(["prefix"])
    finally:
        client.close()  # Drained owner ACK also works after timeout.
        scheduler.close()
    assert not directory.get_usage()["pools"]


def test_worker_restart_and_namespace_mismatch_are_misses():
    directory = ReplicaDirectory()
    client = cpu_client(directory)
    scheduler = cpu_client(directory, metadata_only=True)
    put(client)
    old_pool = client.pool.pool_id
    client.close()
    replacement = cpu_client(directory)
    try:
        assert replacement.pool.pool_id != old_pool
        assert scheduler.batch_exists(["prefix"]) == [False]
        put(replacement)
        scheduler.config.cache_namespace = "different-model"
        assert scheduler.batch_exists(["prefix"]) == [False]
    finally:
        scheduler.close()
        replacement.close()


def test_server_restart_fences_old_session():
    directory = ReplicaDirectory()
    client = cpu_client(directory)
    put(client)
    client.rpc.directory = ReplicaDirectory()
    try:
        with pytest.raises(StorageError, match="epoch"):
            client.batch_retrieve(["prefix"])
    finally:
        client.close()


def test_expired_client_cannot_reconnect_over_its_existing_pool():
    directory = ReplicaDirectory()
    client = cpu_client(directory)
    # Drive the heartbeat deterministically after stopping its background loop.
    client._stop.set()
    client._thread.join()
    client._thread = None
    client._stop.clear()
    try:
        put(client)
        pool = client.pool
        directory._sessions[client._credentials["session_token"]].deadline = 0
        client._heartbeat_loop(0)
        assert not client.connected
        with pytest.raises(RuntimeError, match="fresh CPU handler"):
            client.connect()
        assert client.pool is pool
        assert len(directory.get_usage()["pools"]) == 1
    finally:
        client.close()
    assert not directory.get_usage()["pools"]


def test_duplicate_active_engine_is_rejected():
    directory = ReplicaDirectory()
    client = cpu_client(directory)
    try:
        with pytest.raises(StorageError, match="one active worker"):
            cpu_client(directory)
        assert len(directory.get_usage()["pools"]) == 1
    finally:
        client.close()


def test_replayed_commit_is_idempotent_and_cannot_change_content():
    directory = ReplicaDirectory()
    client = cpu_client(directory)
    try:
        handle = client.alloc(2)
        entries = [{"key": "p", "location": asdict(handle.location)}]
        result = client._call("commit", op_id="same", entries=entries)
        assert result == client._call("commit", op_id="same", entries=entries)
        assert directory.get_usage()["pools"][0]["replicas"] == 1
        with pytest.raises(StorageError, match="op_id reused"):
            client._call("commit", op_id="same", entries=[{**entries[0], "key": "q"}])
    finally:
        client.close()


@pytest.mark.parametrize(
    "changes",
    [
        {"offset": 1},
        {"length": 65},
        {"offset": 128},
        {"generation": 0},
        {"medium": "cxl"},
        {"pool_id": "foreign"},
        {"length": -1},
    ],
)
def test_invalid_location_never_becomes_a_hit(changes):
    directory = ReplicaDirectory()
    client = cpu_client(directory)
    try:
        location = asdict(CpuLocation(client.pool.pool_id, "allocation", 1, 0, 2))
        location.update(changes)
        with pytest.raises(StorageError):
            client._call(
                "commit", op_id="bad", entries=[{"key": "p", "location": location}]
            )
        assert client.batch_exists(["p"]) == [False]
    finally:
        client.close()


def test_different_key_cannot_claim_an_existing_page():
    directory = ReplicaDirectory()
    client = cpu_client(directory)
    try:
        handle = put(client)
        location = {**asdict(handle.location), "allocation_id": "different"}
        assert client._call(
            "commit", op_id="bad", entries=[{"key": "q", "location": location}]
        ) == {"statuses": ["REJECTED"]}
        assert client.batch_exists(["q"]) == [False]
    finally:
        client.close()


def test_old_server_is_explicitly_unsupported():
    rpc = DirectRpc(ReplicaDirectory())
    rpc.handshake = lambda: {"rm_address": "unused"}
    config = MaruConfig(storage_backend="cpu", engine_id="a", cache_namespace="n")
    with pytest.raises(StorageError, match="does not support"):
        CpuStorageClient(config, rpc).connect()


def test_metadata_only_config_needs_no_pool():
    assert MaruConfig(metadata_only=True, pool_size=0).pool_size == 0
    with pytest.raises(ValueError, match="engine_id"):
        MaruConfig(storage_backend="cpu")


def test_late_open_cannot_resurrect_closed_session():
    directory = ReplicaDirectory()
    payload = {
        "server_epoch": directory.server_epoch,
        "session_token": "late",
        "engine_id": "a",
        "namespace": "n",
        "node_id": "node",
        "schema": "bytes",
        "capacity": 128,
        "page_size": 64,
    }
    assert directory.execute("close", {**payload, "drained": True})["success"]
    assert not directory.execute("open", payload)["success"]
    assert not directory.get_usage()["pools"]


def test_exported_view_retains_grant_until_mapping_is_really_closed():
    directory = ReplicaDirectory()
    client = cpu_client(directory)
    handle = client.alloc(4)
    exported = handle.buf[:]
    with pytest.raises(RuntimeError, match="exported CPU buffers"):
        client.close()
    assert len(directory.get_usage()["pools"]) == 1
    exported.release()
    client.close()
    assert not directory.get_usage()["pools"]


def test_lost_request_retries_with_same_operation_id():
    directory = ReplicaDirectory()
    client = cpu_client(directory)
    original = client.rpc.storage
    operations = []

    def lose_request(action, payload):
        if action == "commit":
            operations.append(payload["op_id"])
            if len(operations) == 1:
                raise StorageUnavailableError("request was not delivered")
        return original(action, payload)

    client.rpc.storage = lose_request
    try:
        handle = client.alloc(2)
        handle.buf[:] = b"kv"
        assert client.batch_store(["p"], [handle]) == [False]
        assert not client.batch_exists(["p"])[0]
        client._resolve_pending()
        assert operations[0] == operations[1]
        assert client.has_local("p")
    finally:
        client.close()


def test_later_batch_failure_releases_previously_acquired_reads():
    directory = ReplicaDirectory()
    client = cpu_client(directory)
    put(client)
    original = client.rpc.storage
    calls = 0

    def fail_second(action, payload):
        nonlocal calls
        if action == "acquire":
            calls += 1
            if calls == 2:
                raise StorageUnavailableError("later batch failed")
        return original(action, payload)

    client.rpc.storage = fail_second
    try:
        with pytest.raises(StorageUnavailableError):
            client.batch_retrieve(["prefix"] * 1025)
        assert directory.get_usage()["pools"][0]["read_leases"] == 0
        assert client._reads == 0
    finally:
        client.close()
