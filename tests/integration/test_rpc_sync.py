# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Integration tests for sync RPC client-server communication.

Tests cover:
- Basic RPC operations (connect, alloc, KV CRUD)
- Batch operations (batch_register, batch_lookup, batch_exists)
- Multiple clients (sequential and concurrent)
- Error handling
- Server statistics
- Stress tests
- Client offline behavior

NOTE: These tests require real ZMQ connectivity (not mocked).
Run with: pytest -m integration
"""

import gc
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from maru_handler.rpc_client import RpcClient
from maru_server import MaruServer, RpcServer

pytestmark = pytest.mark.integration

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def server_port():
    """Use a unique available port for tests."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def rpc_server(server_port):
    """Start RPC server for the test module."""
    maru_server = MaruServer()
    server = RpcServer(maru_server, host="127.0.0.1", port=server_port)

    thread = threading.Thread(target=server.start, daemon=True)
    thread.start()
    time.sleep(0.1)

    yield server

    server.stop()
    thread.join(timeout=5.0)


@pytest.fixture
def client(server_port, rpc_server):
    """Create a connected RPC client."""
    c = RpcClient(f"tcp://127.0.0.1:{server_port}", timeout_ms=5000)
    c.connect()
    yield c
    c.close()


@pytest.fixture
def client_with_alloc(client):
    """Create a client with an allocated region."""
    response = client.request_alloc(
        instance_id=f"test_{random.randint(1000, 9999)}",
        size=1024 * 1024,  # 1MB
    )
    assert response.success is True
    assert response.handle is not None
    return client, response.handle


# =============================================================================
# Basic Connection Tests
# =============================================================================


class TestBasicConnection:
    """Test basic client-server connection."""

    def test_connect_disconnect(self, server_port, rpc_server):
        """Test client can connect and disconnect cleanly."""
        client = RpcClient(f"tcp://127.0.0.1:{server_port}")
        client.connect()

        stats = client.get_stats()
        assert stats is not None
        assert stats.kv_manager is not None
        assert stats.allocation_manager is not None

        client.close()

    def test_context_manager(self, server_port, rpc_server):
        """Test client works with context manager."""
        with RpcClient(f"tcp://127.0.0.1:{server_port}") as client:
            stats = client.get_stats()
            assert stats is not None

    def test_multiple_connects(self, server_port, rpc_server):
        """Test multiple sequential connections."""
        for _i in range(5):
            with RpcClient(f"tcp://127.0.0.1:{server_port}") as client:
                stats = client.get_stats()
                assert stats is not None

    def test_heartbeat(self, client):
        """Test heartbeat."""
        result = client.heartbeat()
        assert result is True


# =============================================================================
# Allocation Management Tests
# =============================================================================


class TestAllocationManagement:
    """Test memory allocation and management."""

    def test_allocate(self, client):
        """Test basic allocation."""
        response = client.request_alloc(instance_id="test_alloc_1", size=4096)

        assert response.success is True
        assert response.handle is not None
        assert response.handle.region_id is not None
        assert response.handle.length >= 4096

    def test_allocate_multiple(self, client):
        """Test allocating multiple regions."""
        responses = []
        for i in range(5):
            resp = client.request_alloc(instance_id=f"test_multi_{i}", size=1024)
            assert resp.success is True
            responses.append(resp)

        # All region IDs should be unique
        region_ids = [r.handle.region_id for r in responses]
        assert len(set(region_ids)) == len(region_ids)

    def test_return_alloc(self, client):
        """Test returning an allocation."""
        response = client.request_alloc("test_return", 4096)
        assert response.success is True

        success = client.return_alloc("test_return", response.handle.region_id)
        assert success is True

    def test_return_wrong_instance(self, client):
        """Test returning allocation from wrong instance."""
        resp = client.request_alloc("instance_a", 4096)
        assert resp.success is True

        # Try to return from different instance
        success = client.return_alloc("instance_b", resp.handle.region_id)
        assert success is False


# =============================================================================
# KV Operations Tests
# =============================================================================


class TestKVOperations:
    """Test KV operations (register, lookup, exists, delete)."""

    def test_register_kv(self, client_with_alloc):
        """Test registering a KV entry."""
        client, handle = client_with_alloc

        is_new = client.register_kv(
            key="1001", region_id=handle.region_id, kv_offset=0, kv_length=256
        )
        assert is_new is True

    def test_register_duplicate_kv(self, client_with_alloc):
        """Test registering duplicate key."""
        client, handle = client_with_alloc
        key = "2001"

        is_new1 = client.register_kv(key, handle.region_id, 0, 256)
        assert is_new1 is True

        # Second registration (same key)
        is_new2 = client.register_kv(key, handle.region_id, 256, 128)
        assert is_new2 is False

    def test_lookup_kv(self, client_with_alloc):
        """Test looking up a KV entry."""
        client, handle = client_with_alloc
        key = "3001"

        client.register_kv(key, handle.region_id, 100, 512)

        result = client.lookup_kv(key)
        assert result.found is True
        assert result.handle is not None
        assert result.handle.region_id == handle.region_id
        assert result.kv_offset == 100
        assert result.kv_length == 512

    def test_lookup_nonexistent_kv(self, client):
        """Test looking up non-existent key."""
        result = client.lookup_kv("999999")
        assert result.found is False

    def test_exists_kv(self, client_with_alloc):
        """Test checking KV existence."""
        client, handle = client_with_alloc
        key = "4001"

        assert client.exists_kv(key) is False

        client.register_kv(key, handle.region_id, 0, 100)

        assert client.exists_kv(key) is True

    def test_delete_kv(self, client_with_alloc):
        """Test deleting a KV entry."""
        client, handle = client_with_alloc
        key = "5001"

        client.register_kv(key, handle.region_id, 0, 100)
        assert client.exists_kv(key) is True

        success = client.delete_kv(key)
        assert success is True

        assert client.exists_kv(key) is False

    def test_delete_nonexistent_kv(self, client):
        """Test deleting non-existent key."""
        success = client.delete_kv("999998")
        assert success is False


# =============================================================================
# Batch Operations Tests
# =============================================================================


class TestBatchOperations:
    """Test batch KV operations."""

    def test_batch_register_kv(self, client_with_alloc):
        """Test batch registration."""
        client, handle = client_with_alloc

        entries = [(str(10001 + i), handle.region_id, i * 100, 100) for i in range(10)]
        response = client.batch_register_kv(entries)

        assert response.success is True
        assert len(response.results) == 10
        assert all(r for r in response.results)

    def test_batch_lookup_kv(self, client_with_alloc):
        """Test batch lookup with mix of found and not found."""
        client, handle = client_with_alloc

        # Register some keys
        for i in [1, 3, 5]:
            client.register_kv(
                key=str(20000 + i),
                region_id=handle.region_id,
                kv_offset=i * 100,
                kv_length=100,
            )

        # Batch lookup (mix of found and not found)
        keys = ["20001", "20002", "20003", "20004", "20005"]
        response = client.batch_lookup_kv(keys)

        assert len(response.entries) == 5
        assert response.entries[0].found is True  # key 20001
        assert response.entries[1].found is False  # key 20002
        assert response.entries[2].found is True  # key 20003
        assert response.entries[3].found is False  # key 20004
        assert response.entries[4].found is True  # key 20005

    def test_batch_exists_kv(self, client_with_alloc):
        """Test batch existence check."""
        client, handle = client_with_alloc

        # Register some keys
        for i in [1, 3, 5]:
            client.register_kv(
                key=str(30000 + i),
                region_id=handle.region_id,
                kv_offset=i * 100,
                kv_length=100,
            )

        # Batch exists
        keys = ["30001", "30002", "30003", "30004", "30005"]
        response = client.batch_exists_kv(keys)

        assert response.results == [True, False, True, False, True]


# =============================================================================
# Multiple Clients Tests
# =============================================================================


class TestMultipleClients:
    """Test multiple clients accessing the server."""

    def test_multiple_clients_sequential(self, server_port, rpc_server):
        """Test multiple clients accessing server sequentially."""
        results = []

        for i in range(5):
            with RpcClient(f"tcp://127.0.0.1:{server_port}") as client:
                resp = client.request_alloc(f"client_{i}", 1024)
                assert resp.success is True

                client.register_kv(str(40000 + i), resp.handle.region_id, 0, 100)
                results.append(client.exists_kv(str(40000 + i)))

        assert all(results)

    def test_multiple_clients_concurrent(self, server_port, rpc_server):
        """Test multiple clients accessing server concurrently."""
        num_clients = 5

        def client_task(client_id):
            with RpcClient(f"tcp://127.0.0.1:{server_port}") as client:
                resp = client.request_alloc(f"concurrent_{client_id}", 1024)
                if not resp.success:
                    return False

                key = str(50000 + client_id)
                client.register_kv(key, resp.handle.region_id, 0, 100)
                return client.exists_kv(key)

        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(client_task, i) for i in range(num_clients)]
            results = [f.result() for f in as_completed(futures)]

        assert all(results)

    def test_clients_share_kv_visibility(self, server_port, rpc_server):
        """Test that KV entries are visible across clients."""
        key = "60001"

        # Client 1: Register KV
        with RpcClient(f"tcp://127.0.0.1:{server_port}") as client1:
            resp = client1.request_alloc("client1", 1024)
            client1.register_kv(key, resp.handle.region_id, 0, 100)
            assert client1.exists_kv(key) is True

        # Client 2: Should see the same KV
        with RpcClient(f"tcp://127.0.0.1:{server_port}") as client2:
            assert client2.exists_kv(key) is True
            result = client2.lookup_kv(key)
            assert result.found is True


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Test error handling scenarios."""

    def test_lookup_with_large_key(self, client):
        """Test lookup returns not found for any integer key."""
        result = client.lookup_kv(str(2**60))
        assert result.found is False

    def test_return_nonexistent_region(self, client):
        """Test returning allocation with non-existent region_id."""
        success = client.return_alloc(instance_id="test_instance", region_id=999999)
        assert success is False

    def test_double_return_alloc(self, client):
        """Test returning the same allocation twice."""
        response = client.request_alloc("test_double_return", 4096)
        assert response.success is True

        # First return should succeed
        success1 = client.return_alloc("test_double_return", response.handle.region_id)
        assert success1 is True

        # Second return should fail
        success2 = client.return_alloc("test_double_return", response.handle.region_id)
        assert success2 is False

    def test_double_delete_kv(self, client_with_alloc):
        """Test deleting the same KV entry twice."""
        client, handle = client_with_alloc
        key = "100001"

        # Register and delete first time
        client.register_kv(key, handle.region_id, 0, 100)
        success1 = client.delete_kv(key)
        assert success1 is True

        # Second delete should fail
        success2 = client.delete_kv(key)
        assert success2 is False

    def test_exists_nonexistent_kv(self, client):
        """Test exists_kv for a key that was never registered."""
        result = client.exists_kv("999997")
        assert result is False

    def test_batch_lookup_all_nonexistent(self, client):
        """Test batch_lookup_kv with all non-existent keys."""
        keys = ["100002", "100003", "100004", "100005", "100006"]
        response = client.batch_lookup_kv(keys)

        assert len(response.entries) == 5
        assert all(not entry.found for entry in response.entries)

    def test_batch_exists_all_nonexistent(self, client):
        """Test batch_exists_kv with all non-existent keys."""
        keys = ["100007", "100008", "100009", "100010", "100011"]
        response = client.batch_exists_kv(keys)

        assert len(response.results) == 5
        assert all(not exists for exists in response.results)


# =============================================================================
# Stats Tests
# =============================================================================


class TestServerStats:
    """Test server statistics reporting."""

    def test_get_stats(self, client):
        """Test getting server stats."""
        stats = client.get_stats()

        assert stats.kv_manager is not None
        assert stats.allocation_manager is not None

    def test_stats_reflect_operations(self, client_with_alloc):
        """Test that stats reflect operations."""
        client, handle = client_with_alloc

        stats_before = client.get_stats()
        initial_count = stats_before.kv_manager.total_entries

        # Add some entries
        for i in range(5):
            client.register_kv(str(70000 + i), handle.region_id, i * 100, 100)

        stats_after = client.get_stats()
        final_count = stats_after.kv_manager.total_entries

        assert final_count >= initial_count + 5


# =============================================================================
# Stress Tests
# =============================================================================


class TestStress:
    """Stress tests for RPC server."""

    @pytest.mark.slow
    def test_many_kv_operations(self, client_with_alloc):
        """Test many KV operations."""
        client, handle = client_with_alloc
        num_ops = 100

        base_key = 80000

        # Register many entries
        for i in range(num_ops):
            client.register_kv(
                key=str(base_key + i),
                region_id=handle.region_id,
                kv_offset=i * 64,
                kv_length=64,
            )

        # Verify all exist
        for i in range(num_ops):
            assert client.exists_kv(str(base_key + i)) is True

        # Lookup all
        for i in range(num_ops):
            result = client.lookup_kv(str(base_key + i))
            assert result.found is True

    @pytest.mark.slow
    def test_rapid_connect_disconnect(self, server_port, rpc_server):
        """Test rapid connect/disconnect cycles."""
        for _ in range(20):
            client = RpcClient(f"tcp://127.0.0.1:{server_port}")
            client.connect()
            client.get_stats()
            client.close()

    @pytest.mark.slow
    def test_concurrent_kv_operations(self, server_port, rpc_server):
        """Test concurrent KV operations from multiple threads."""
        num_threads = 5
        ops_per_thread = 10

        def kv_task(thread_id):
            results = []
            with RpcClient(f"tcp://127.0.0.1:{server_port}") as client:
                resp = client.request_alloc(f"stress_{thread_id}", 1024 * 100)
                if not resp.success:
                    return [False]

                for i in range(ops_per_thread):
                    key = str(90000 + thread_id * 1000 + i)
                    client.register_kv(
                        key=key,
                        region_id=resp.handle.region_id,
                        kv_offset=i * 64,
                        kv_length=64,
                    )
                    results.append(client.exists_kv(key))

            return results

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(kv_task, i) for i in range(num_threads)]
            all_results = []
            for f in as_completed(futures):
                all_results.extend(f.result())

        success_rate = sum(all_results) / len(all_results)
        assert success_rate >= 0.95


# =============================================================================
# Client Offline Tests
# =============================================================================


class TestClientOffline:
    """Test RpcClient behavior without server."""

    @pytest.fixture(autouse=True)
    def _zmq_cleanup(self):
        """Wait for previous tests' ZMQ contexts to fully terminate."""
        gc.collect()
        time.sleep(0.3)

    def test_connect_timeout(self):
        """Test connection timeout when server is not running."""
        client = RpcClient(
            server_url="tcp://127.0.0.1:59999",  # Non-existent server
            timeout_ms=100,
        )
        client.connect()

        # Request should timeout
        response = client.request_alloc(instance_id="test", size=1024)

        assert response.success is False
        assert (
            "timeout" in (response.error or "").lower()
            or response.error == "Unknown error"
        )

        client.close()

    def test_context_manager(self):
        """Test client context manager."""
        with RpcClient(server_url="tcp://127.0.0.1:59998", timeout_ms=100) as client:
            response = client.request_alloc(instance_id="test", size=1024)
            # Should handle gracefully
            assert response.success is False

    def test_heartbeat_timeout(self):
        """Test heartbeat timeout when server is not running."""
        client = RpcClient(
            server_url="tcp://127.0.0.1:59997",  # Non-existent server
            timeout_ms=100,
        )
        client.connect()

        # Heartbeat should timeout and return False
        result = client.heartbeat()
        assert result is False

        client.close()

    def test_kv_operations_timeout(self):
        """Test KV operations timeout when server is not running."""
        client = RpcClient(
            server_url="tcp://127.0.0.1:59996",  # Non-existent server
            timeout_ms=100,
        )
        client.connect()

        # exists_kv should timeout and return False
        result = client.exists_kv("1")
        assert result is False

        client.close()
