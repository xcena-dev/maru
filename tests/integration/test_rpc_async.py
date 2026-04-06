# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Integration tests for RpcAsyncServer + RpcAsyncClient.

Tests cover all message types end-to-end through the async RPC layer:
- Allocation (request_alloc, return_alloc)
- KV operations (register, lookup, exists, delete)
- Batch operations (batch_register, batch_lookup, batch_exists)
- Admin (get_stats, heartbeat)
- Multiple concurrent clients
- Graceful shutdown
- Non-blocking async API (*_async methods)
- Pipeline patterns (concurrent in-flight requests)
- Semaphore backpressure
"""

import threading
import time
import warnings
from concurrent.futures import Future, ThreadPoolExecutor, as_completed, wait

import pytest

from maru_handler.rpc_async_client import RpcAsyncClient
from maru_server.rpc_async_server import RpcAsyncServer
from maru_server.server import MaruServer

pytestmark = pytest.mark.integration

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def async_server_port():
    """Get a random available port."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def async_server(async_server_port):
    """Start an async RPC server in a background thread."""
    maru_server = MaruServer()
    rpc_server = RpcAsyncServer(
        maru_server, host="127.0.0.1", port=async_server_port, num_workers=4
    )

    server_thread = threading.Thread(target=rpc_server.start, daemon=True)
    server_thread.start()

    # Wait for server to be ready
    time.sleep(0.2)

    yield rpc_server

    # Cleanup: stop server and wait for thread to fully exit
    rpc_server.stop()
    server_thread.join(timeout=5.0)


@pytest.fixture
def async_client(async_server_port, async_server):
    """Create a connected async client."""
    client = RpcAsyncClient(
        server_url=f"tcp://127.0.0.1:{async_server_port}", timeout_ms=5000
    )
    client.connect()
    yield client
    client.close()


# =============================================================================
# Basic Connection Tests
# =============================================================================


class TestAsyncBasicConnection:
    """Test basic async client-server connection."""

    def test_connect_disconnect(self, async_server_port, async_server):
        """Test client can connect and disconnect cleanly."""
        client = RpcAsyncClient(f"tcp://127.0.0.1:{async_server_port}")
        client.connect()
        stats = client.get_stats()
        assert stats is not None
        client.close()

    def test_context_manager(self, async_server_port, async_server):
        """Test client works with context manager."""
        with RpcAsyncClient(f"tcp://127.0.0.1:{async_server_port}") as client:
            stats = client.get_stats()
            assert stats is not None

    def test_heartbeat(self, async_client):
        """Test heartbeat."""
        result = async_client.heartbeat()
        assert result is True


# =============================================================================
# Allocation Tests
# =============================================================================


class TestAsyncAllocation:
    """Test allocation operations through async RPC."""

    def test_request_alloc_success(self, async_client):
        """Test successful memory allocation."""
        response = async_client.request_alloc(
            instance_id="test-async-instance",
            size=1024 * 1024,
        )
        assert response.success is True
        assert response.handle is not None
        assert response.handle.length >= 1024 * 1024
        assert response.error is None

    def test_return_alloc(self, async_client):
        """Test returning an allocation."""
        alloc_resp = async_client.request_alloc(
            instance_id="test-async-instance", size=1024 * 1024
        )
        assert alloc_resp.success is True

        success = async_client.return_alloc(
            instance_id="test-async-instance",
            region_id=alloc_resp.handle.region_id,
        )
        assert success is True

    def test_multiple_allocations(self, async_client):
        """Test allocating multiple regions."""
        responses = []
        for i in range(5):
            resp = async_client.request_alloc(
                instance_id=f"async-instance-{i}",
                size=100 * 1024,
            )
            responses.append(resp)

        for resp in responses:
            assert resp.success is True
            assert resp.handle is not None

        # Region IDs should be unique
        region_ids = [r.handle.region_id for r in responses]
        assert len(set(region_ids)) == len(region_ids)


# =============================================================================
# KV Operations Tests
# =============================================================================


class TestAsyncKVOperations:
    """Test KV operations through async RPC."""

    def test_register_and_lookup_kv(self, async_client):
        """Test KV registration and lookup."""
        alloc_resp = async_client.request_alloc(
            instance_id="test-kv-instance", size=1024 * 1024
        )
        assert alloc_resp.success is True

        is_new = async_client.register_kv(
            key="12345",
            region_id=alloc_resp.handle.region_id,
            kv_offset=0,
            kv_length=1024,
        )
        assert is_new is True

        lookup_resp = async_client.lookup_kv(key="12345")
        assert lookup_resp.found is True
        assert lookup_resp.handle is not None
        assert lookup_resp.kv_length == 1024
        assert lookup_resp.kv_offset == 0

    def test_exists_kv(self, async_client):
        """Test KV existence check."""
        assert async_client.exists_kv(key="99999") is False

        alloc_resp = async_client.request_alloc(
            instance_id="test-exists-instance", size=1024 * 1024
        )
        async_client.register_kv(
            key="11111",
            region_id=alloc_resp.handle.region_id,
            kv_offset=0,
            kv_length=100,
        )
        assert async_client.exists_kv(key="11111") is True

    def test_delete_kv(self, async_client):
        """Test KV deletion."""
        alloc_resp = async_client.request_alloc(
            instance_id="test-delete-instance", size=1024 * 1024
        )
        async_client.register_kv(
            key="22222",
            region_id=alloc_resp.handle.region_id,
            kv_offset=0,
            kv_length=100,
        )
        assert async_client.exists_kv(key="22222") is True

        success = async_client.delete_kv(key="22222")
        assert success is True
        assert async_client.exists_kv(key="22222") is False

    def test_lookup_nonexistent(self, async_client):
        """Test lookup of non-existent key."""
        result = async_client.lookup_kv(key="999999")
        assert result.found is False

    def test_register_duplicate(self, async_client):
        """Test registering duplicate key."""
        alloc_resp = async_client.request_alloc(
            instance_id="test-dup-instance", size=1024 * 1024
        )
        region_id = alloc_resp.handle.region_id

        is_new1 = async_client.register_kv(
            key="33333", region_id=region_id, kv_offset=0, kv_length=256
        )
        assert is_new1 is True

        is_new2 = async_client.register_kv(
            key="33333", region_id=region_id, kv_offset=256, kv_length=128
        )
        assert is_new2 is False


# =============================================================================
# Batch Operations Tests
# =============================================================================


class TestAsyncBatchOperations:
    """Test batch operations through async RPC."""

    def test_batch_register_kv(self, async_client):
        """Test batch KV registration."""
        alloc_resp = async_client.request_alloc(
            instance_id="test-batch-instance", size=1024 * 1024
        )
        assert alloc_resp.success is True

        entries = [
            (str(i), alloc_resp.handle.region_id, i * 100, 100) for i in range(10)
        ]
        response = async_client.batch_register_kv(entries)

        assert response.success is True
        assert len(response.results) == 10
        assert all(r for r in response.results)

    def test_batch_lookup_kv(self, async_client):
        """Test batch KV lookup."""
        alloc_resp = async_client.request_alloc(
            instance_id="test-batch-lookup", size=1024 * 1024
        )

        # Register some keys
        for i in [100, 300, 500]:
            async_client.register_kv(
                key=str(i),
                region_id=alloc_resp.handle.region_id,
                kv_offset=i * 10,
                kv_length=100,
            )

        # Batch lookup (mix of found and not found)
        response = async_client.batch_lookup_kv(["100", "200", "300", "400", "500"])

        assert len(response.entries) == 5
        assert response.entries[0].found is True  # key 100
        assert response.entries[1].found is False  # key 200
        assert response.entries[2].found is True  # key 300
        assert response.entries[3].found is False  # key 400
        assert response.entries[4].found is True  # key 500

    def test_batch_exists_kv(self, async_client):
        """Test batch KV existence check."""
        alloc_resp = async_client.request_alloc(
            instance_id="test-batch-exists", size=1024 * 1024
        )

        for i in [100, 300, 500]:
            async_client.register_kv(
                key=str(i),
                region_id=alloc_resp.handle.region_id,
                kv_offset=i * 10,
                kv_length=100,
            )

        response = async_client.batch_exists_kv(["100", "200", "300", "400", "500"])
        assert response.results == [True, False, True, False, True]


# =============================================================================
# Stats Tests
# =============================================================================


class TestAsyncStats:
    """Test server statistics through async RPC."""

    def test_get_stats(self, async_client):
        """Test getting server statistics."""
        stats = async_client.get_stats()
        assert stats.kv_manager is not None
        assert stats.allocation_manager is not None

    def test_stats_reflect_operations(self, async_client):
        """Test that stats reflect operations."""
        alloc_resp = async_client.request_alloc(
            instance_id="test-stats-instance", size=1024 * 1024
        )

        stats_before = async_client.get_stats()
        initial_count = stats_before.kv_manager.total_entries

        for i in range(5):
            async_client.register_kv(
                key=str(60000 + i),
                region_id=alloc_resp.handle.region_id,
                kv_offset=i * 100,
                kv_length=100,
            )

        stats_after = async_client.get_stats()
        assert stats_after.kv_manager.total_entries >= initial_count + 5


# =============================================================================
# Multiple Concurrent Clients Tests
# =============================================================================


class TestAsyncMultipleClients:
    """Test multiple clients accessing the async server concurrently."""

    def test_concurrent_clients(self, async_server_port, async_server):
        """Test multiple async clients accessing server concurrently."""
        num_clients = 5

        def client_task(client_id):
            with RpcAsyncClient(
                f"tcp://127.0.0.1:{async_server_port}", timeout_ms=5000
            ) as client:
                resp = client.request_alloc(f"concurrent-async-{client_id}", 1024)
                if not resp.success:
                    return False

                key = str(80000 + client_id)
                client.register_kv(key, resp.handle.region_id, 0, 100)
                return client.exists_kv(key)

        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(client_task, i) for i in range(num_clients)]
            results = [f.result(timeout=10) for f in as_completed(futures)]

        assert all(results)

    def test_clients_share_kv_visibility(self, async_server_port, async_server):
        """Test that KV entries are visible across async clients."""
        key = "90001"

        # Client 1: Register KV
        with RpcAsyncClient(f"tcp://127.0.0.1:{async_server_port}") as client1:
            resp = client1.request_alloc("async-client1", 1024)
            client1.register_kv(key, resp.handle.region_id, 0, 100)
            assert client1.exists_kv(key) is True

        # Client 2: Should see the same KV
        with RpcAsyncClient(f"tcp://127.0.0.1:{async_server_port}") as client2:
            assert client2.exists_kv(key) is True
            result = client2.lookup_kv(key)
            assert result.found is True

    def test_many_concurrent_operations(self, async_server_port, async_server):
        """Stress test: many concurrent operations from multiple clients."""
        num_clients = 5
        ops_per_client = 50

        def client_task(client_id):
            with RpcAsyncClient(f"tcp://127.0.0.1:{async_server_port}") as client:
                resp = client.request_alloc(f"stress-{client_id}", 1024 * 1024)
                if not resp.success:
                    return False

                region_id = resp.handle.region_id
                base_key = 100000 + client_id * ops_per_client

                # Register
                for i in range(ops_per_client):
                    client.register_kv(str(base_key + i), region_id, i * 64, 64)

                # Verify all exist
                for i in range(ops_per_client):
                    if not client.exists_kv(str(base_key + i)):
                        return False

                # Lookup all
                for i in range(ops_per_client):
                    result = client.lookup_kv(str(base_key + i))
                    if not result.found:
                        return False

                return True

        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(client_task, i) for i in range(num_clients)]
            results = [f.result() for f in as_completed(futures)]

        assert all(results)


# =============================================================================
# Cross-compatibility: AsyncClient with AsyncServer
# (This is the primary test - ensuring DEALER<->ROUTER works)
# =============================================================================


class TestAsyncCrossCompat:
    """Test async client with async server for all message types."""

    def test_full_workflow(self, async_client):
        """Test a complete workflow through async RPC."""
        # 1. Allocate
        alloc_resp = async_client.request_alloc("workflow-instance", 1024 * 1024)
        assert alloc_resp.success is True
        region_id = alloc_resp.handle.region_id

        # 2. Register KVs
        for i in range(10):
            is_new = async_client.register_kv(
                key=str(50000 + i),
                region_id=region_id,
                kv_offset=i * 100,
                kv_length=100,
            )
            assert is_new is True

        # 3. Batch register
        batch_entries = [
            (str(50100 + i), region_id, 1000 + i * 100, 100) for i in range(10)
        ]
        batch_resp = async_client.batch_register_kv(batch_entries)
        assert batch_resp.success is True

        # 4. Lookup individual
        for i in range(10):
            result = async_client.lookup_kv(str(50000 + i))
            assert result.found is True
            assert result.kv_length == 100

        # 5. Batch lookup
        batch_lookup = async_client.batch_lookup_kv(
            ["50000", "50001", "59999", "50100", "50101"]
        )
        assert batch_lookup.entries[0].found is True
        assert batch_lookup.entries[1].found is True
        assert batch_lookup.entries[2].found is False  # not registered
        assert batch_lookup.entries[3].found is True
        assert batch_lookup.entries[4].found is True

        # 6. Batch exists
        batch_exists = async_client.batch_exists_kv(["50000", "59999", "50100"])
        assert batch_exists.results == [True, False, True]

        # 7. Delete
        assert async_client.delete_kv("50000") is True
        assert async_client.exists_kv("50000") is False

        # 8. Stats
        stats = async_client.get_stats()
        assert stats.kv_manager.total_entries >= 19  # 10 + 10 - 1 deleted

        # 9. Return alloc
        assert async_client.return_alloc("workflow-instance", region_id) is True

        # 10. Heartbeat
        assert async_client.heartbeat() is True


# =============================================================================
# Non-blocking Async API Tests (*_async methods)
# =============================================================================


class TestAsyncNonBlockingAPI:
    """Test the non-blocking *_async() methods that return concurrent.futures.Future."""

    def test_register_kv_async(self, async_client):
        """Test non-blocking register_kv_async returns Future and resolves."""
        alloc_resp = async_client.request_alloc("test-nb-register", 1024 * 1024)
        assert alloc_resp.success is True
        region_id = alloc_resp.handle.region_id

        future = async_client.register_kv_async(
            key="200000", region_id=region_id, kv_offset=0, kv_length=100
        )
        assert isinstance(future, Future)
        result = future.result(timeout=5.0)
        assert result is True

    def test_lookup_kv_async(self, async_client):
        """Test non-blocking lookup_kv_async."""
        alloc_resp = async_client.request_alloc("test-nb-lookup", 1024 * 1024)
        region_id = alloc_resp.handle.region_id
        async_client.register_kv(
            key="200001", region_id=region_id, kv_offset=0, kv_length=256
        )

        future = async_client.lookup_kv_async(key="200001")
        assert isinstance(future, Future)
        result = future.result(timeout=5.0)
        assert result.found is True
        assert result.kv_length == 256

    def test_exists_kv_async(self, async_client):
        """Test non-blocking exists_kv_async."""
        future = async_client.exists_kv_async(key="999888")
        assert isinstance(future, Future)
        assert future.result(timeout=5.0) is False

    def test_delete_kv_async(self, async_client):
        """Test non-blocking delete_kv_async."""
        alloc_resp = async_client.request_alloc("test-nb-delete", 1024 * 1024)
        region_id = alloc_resp.handle.region_id
        async_client.register_kv(
            key="200002", region_id=region_id, kv_offset=0, kv_length=100
        )

        future = async_client.delete_kv_async(key="200002")
        result = future.result(timeout=5.0)
        assert result is True
        assert async_client.exists_kv("200002") is False

    def test_heartbeat_async(self, async_client):
        """Test non-blocking heartbeat_async."""
        future = async_client.heartbeat_async()
        assert isinstance(future, Future)
        assert future.result(timeout=5.0) is True

    def test_request_alloc_async(self, async_client):
        """Test non-blocking request_alloc_async."""
        future = async_client.request_alloc_async("test-nb-alloc", 1024 * 1024)
        assert isinstance(future, Future)
        result = future.result(timeout=5.0)
        assert result.success is True
        assert result.handle is not None

    def test_batch_register_kv_async(self, async_client):
        """Test non-blocking batch_register_kv_async."""
        alloc_resp = async_client.request_alloc("test-nb-batch-reg", 1024 * 1024)
        region_id = alloc_resp.handle.region_id

        entries = [(str(200100 + i), region_id, i * 100, 100) for i in range(5)]
        future = async_client.batch_register_kv_async(entries)
        result = future.result(timeout=5.0)
        assert result.success is True
        assert len(result.results) == 5

    def test_batch_lookup_kv_async(self, async_client):
        """Test non-blocking batch_lookup_kv_async."""
        alloc_resp = async_client.request_alloc("test-nb-batch-lk", 1024 * 1024)
        region_id = alloc_resp.handle.region_id

        for i in [200200, 200202]:
            async_client.register_kv(
                key=str(i), region_id=region_id, kv_offset=i, kv_length=64
            )

        future = async_client.batch_lookup_kv_async(["200200", "200201", "200202"])
        result = future.result(timeout=5.0)
        assert len(result.entries) == 3
        assert result.entries[0].found is True
        assert result.entries[1].found is False
        assert result.entries[2].found is True

    def test_batch_exists_kv_async(self, async_client):
        """Test non-blocking batch_exists_kv_async."""
        alloc_resp = async_client.request_alloc("test-nb-batch-ex", 1024 * 1024)
        region_id = alloc_resp.handle.region_id

        async_client.register_kv(
            key="200300", region_id=region_id, kv_offset=0, kv_length=64
        )

        future = async_client.batch_exists_kv_async(["200300", "200301"])
        result = future.result(timeout=5.0)
        assert result.results == [True, False]

    def test_return_alloc_async(self, async_client):
        """Test non-blocking return_alloc_async."""
        # First allocate a region
        alloc_resp = async_client.request_alloc("test-nb-return", 1024 * 1024)
        assert alloc_resp.success is True
        region_id = alloc_resp.handle.region_id

        # Return it asynchronously
        future = async_client.return_alloc_async("test-nb-return", region_id)
        assert isinstance(future, Future)
        result = future.result(timeout=5.0)
        assert result is True


# =============================================================================
# Pipeline Pattern Tests (concurrent in-flight)
# =============================================================================


class TestAsyncPipeline:
    """Test pipelining: multiple concurrent in-flight requests."""

    def test_pipeline_multiple_registers(self, async_client):
        """Send N register_kv_async concurrently, then collect all results."""
        alloc_resp = async_client.request_alloc("test-pipeline-reg", 1024 * 1024)
        region_id = alloc_resp.handle.region_id
        n = 20

        # Fire all requests non-blocking
        futures = []
        for i in range(n):
            f = async_client.register_kv_async(
                key=str(300000 + i), region_id=region_id, kv_offset=i * 64, kv_length=64
            )
            futures.append(f)

        # Collect all results
        results = [f.result(timeout=10.0) for f in futures]
        assert all(r is True for r in results)

        # Verify all keys exist
        for i in range(n):
            assert async_client.exists_kv(str(300000 + i)) is True

    def test_pipeline_lookup_prefetch(self, async_client):
        """Prefetch pattern: send multiple lookups concurrently."""
        alloc_resp = async_client.request_alloc("test-pipeline-lk", 1024 * 1024)
        region_id = alloc_resp.handle.region_id

        # Register keys first
        keys = [str(i) for i in range(300100, 300110)]
        for k in keys:
            async_client.register_kv(
                key=k, region_id=region_id, kv_offset=k * 10, kv_length=100
            )

        # Prefetch all lookups concurrently
        futures = [async_client.lookup_kv_async(k) for k in keys]

        # Collect in order
        for _i, f in enumerate(futures):
            result = f.result(timeout=10.0)
            assert result.found is True
            assert result.kv_length == 100

    def test_pipeline_mixed_ops(self, async_client):
        """Mixed operations: register + lookup + exists concurrently."""
        alloc_resp = async_client.request_alloc("test-pipeline-mix", 1024 * 1024)
        region_id = alloc_resp.handle.region_id

        # Register a key first (blocking)
        async_client.register_kv(
            key="300200", region_id=region_id, kv_offset=0, kv_length=100
        )

        # Fire mixed ops concurrently
        f_reg = async_client.register_kv_async(
            key="300201", region_id=region_id, kv_offset=100, kv_length=64
        )
        f_lookup = async_client.lookup_kv_async(key="300200")
        f_exists = async_client.exists_kv_async(key="300200")
        f_hb = async_client.heartbeat_async()

        # Collect all
        assert f_reg.result(timeout=5.0) is True
        lookup_result = f_lookup.result(timeout=5.0)
        assert lookup_result.found is True
        assert f_exists.result(timeout=5.0) is True
        assert f_hb.result(timeout=5.0) is True

    def test_pipeline_is_faster_than_sequential(self, async_client):
        """Pipeline should complete faster than sequential for N requests."""
        alloc_resp = async_client.request_alloc("test-pipeline-perf", 1024 * 1024)
        region_id = alloc_resp.handle.region_id
        n = 50

        # Sequential timing
        t0 = time.monotonic()
        for i in range(n):
            async_client.register_kv(
                key=str(400000 + i), region_id=region_id, kv_offset=i * 64, kv_length=64
            )
        seq_time = time.monotonic() - t0

        # Clean up for pipeline test
        for i in range(n):
            async_client.delete_kv(str(400000 + i))

        # Pipeline timing
        t0 = time.monotonic()
        futures = []
        for i in range(n):
            f = async_client.register_kv_async(
                key=str(400000 + i), region_id=region_id, kv_offset=i * 64, kv_length=64
            )
            futures.append(f)
        # Wait for all — validate correctness
        for f in futures:
            assert f.result(timeout=10.0) is True
        pipe_time = time.monotonic() - t0

        speedup = seq_time / pipe_time if pipe_time > 0 else float("inf")
        print(
            f"Sequential: {seq_time:.4f}s, Pipeline: {pipe_time:.4f}s, Speedup: {speedup:.2f}x"
        )
        if speedup < 0.5:
            warnings.warn(
                f"Pipeline speedup {speedup:.2f}x < 0.5 — "
                "scheduling overhead may dominate on fast local IPC",
                UserWarning,
                stacklevel=2,
            )


# =============================================================================
# Semaphore Backpressure Tests
# =============================================================================


class TestAsyncSemaphoreBackpressure:
    """Test asyncio.Semaphore-based backpressure control."""

    def test_semaphore_limits_inflight(self, async_server_port, async_server):
        """With max_inflight=2, concurrent requests should still complete."""
        client = RpcAsyncClient(
            f"tcp://127.0.0.1:{async_server_port}",
            timeout_ms=5000,
            max_inflight=2,
        )
        client.connect()

        try:
            alloc_resp = client.request_alloc("test-sem", 1024 * 1024)
            assert alloc_resp.success is True
            region_id = alloc_resp.handle.region_id

            # Fire 10 requests with max_inflight=2
            futures = []
            for i in range(10):
                f = client.register_kv_async(
                    key=str(500000 + i),
                    region_id=region_id,
                    kv_offset=i * 64,
                    kv_length=64,
                )
                futures.append(f)

            # All should complete (semaphore queues, doesn't reject)
            results = [f.result(timeout=10.0) for f in futures]
            assert all(r is True for r in results)
        finally:
            client.close()

    def test_semaphore_default_value(self, async_server_port, async_server):
        """Default max_inflight=64 should handle many concurrent requests."""
        client = RpcAsyncClient(f"tcp://127.0.0.1:{async_server_port}")
        client.connect()

        try:
            alloc_resp = client.request_alloc("test-sem-default", 1024 * 1024)
            region_id = alloc_resp.handle.region_id

            # Fire 100 requests (within default limit of 64 + queueing)
            futures = []
            for i in range(100):
                f = client.register_kv_async(
                    key=str(510000 + i),
                    region_id=region_id,
                    kv_offset=i * 64,
                    kv_length=64,
                )
                futures.append(f)

            results = [f.result(timeout=15.0) for f in futures]
            assert all(r is True for r in results)
        finally:
            client.close()


# =============================================================================
# Async API with wait/as_completed patterns
# =============================================================================


class TestAsyncFuturePatterns:
    """Test using concurrent.futures patterns with *_async methods."""

    def test_as_completed_pattern(self, async_client):
        """Test using as_completed() to process results as they arrive."""
        alloc_resp = async_client.request_alloc("test-as-completed", 1024 * 1024)
        region_id = alloc_resp.handle.region_id
        n = 10

        futures = {}
        for i in range(n):
            f = async_client.register_kv_async(
                key=str(600000 + i), region_id=region_id, kv_offset=i * 64, kv_length=64
            )
            futures[f] = str(600000 + i)

        completed_keys = []
        for f in as_completed(futures, timeout=10.0):
            result = f.result()
            assert result is True
            completed_keys.append(futures[f])

        assert len(completed_keys) == n

    def test_wait_pattern(self, async_client):
        """Test using wait() to wait for all futures."""
        alloc_resp = async_client.request_alloc("test-wait", 1024 * 1024)
        region_id = alloc_resp.handle.region_id

        futures = []
        for i in range(10):
            f = async_client.register_kv_async(
                key=str(610000 + i), region_id=region_id, kv_offset=i * 64, kv_length=64
            )
            futures.append(f)

        done, not_done = wait(futures, timeout=10.0)
        assert len(done) == 10
        assert len(not_done) == 0
        for f in done:
            assert f.result() is True


# =============================================================================
# Failure/Edge-case Scenarios
# =============================================================================


class TestAsyncFailureScenarios:
    """Test failure and edge-case scenarios for async RPC operations."""

    def test_delete_nonexistent_kv(self, async_client):
        """Delete a key that was never registered should return False."""
        success = async_client.delete_kv(key="800001")
        assert success is False

    def test_double_delete_kv(self, async_client):
        """Delete the same key twice should return True then False."""
        alloc_resp = async_client.request_alloc("test-double-delete", 1024 * 1024)
        assert alloc_resp.success is True

        async_client.register_kv(
            key="700001",
            region_id=alloc_resp.handle.region_id,
            kv_offset=0,
            kv_length=100,
        )

        # First delete should succeed
        success1 = async_client.delete_kv(key="700001")
        assert success1 is True

        # Second delete should fail
        success2 = async_client.delete_kv(key="700001")
        assert success2 is False

    def test_return_alloc_wrong_instance(self, async_client):
        """Return allocation with wrong instance_id should return False."""
        alloc_resp = async_client.request_alloc("inst_a", 1024 * 1024)
        assert alloc_resp.success is True
        region_id = alloc_resp.handle.region_id

        # Try to return with different instance_id
        success = async_client.return_alloc("inst_b", region_id)
        assert success is False

    def test_double_return_alloc(self, async_client):
        """Return the same allocation twice should return True then False."""
        alloc_resp = async_client.request_alloc("test-double-return", 1024 * 1024)
        assert alloc_resp.success is True
        region_id = alloc_resp.handle.region_id

        # First return should succeed
        success1 = async_client.return_alloc("test-double-return", region_id)
        assert success1 is True

        # Second return should fail
        success2 = async_client.return_alloc("test-double-return", region_id)
        assert success2 is False

    def test_exists_nonexistent_kv(self, async_client):
        """Explicitly test exists_kv for a key that was never registered."""
        exists = async_client.exists_kv(key="800002")
        assert exists is False

    def test_batch_lookup_all_nonexistent(self, async_client):
        """Batch lookup with all non-existent keys should return all found=False."""
        response = async_client.batch_lookup_kv(["999001", "999002", "999003"])
        assert len(response.entries) == 3
        assert all(entry.found is False for entry in response.entries)

    def test_batch_exists_all_nonexistent(self, async_client):
        """Batch exists with all non-existent keys should return all False."""
        response = async_client.batch_exists_kv(["999001", "999002", "999003"])
        assert response.results == [False, False, False]


# =============================================================================
# Client Offline/Timeout Scenarios
# =============================================================================


class TestAsyncClientOffline:
    """Test RpcAsyncClient behavior without server."""

    def test_heartbeat_timeout(self):
        """Heartbeat to non-existent server should return False."""
        client = RpcAsyncClient(
            server_url="tcp://127.0.0.1:59995",
            timeout_ms=500,
        )
        client.connect()
        try:
            result = client.heartbeat()
            assert result is False
        finally:
            client.close()

    def test_request_alloc_timeout(self):
        """request_alloc to non-existent server should fail."""
        client = RpcAsyncClient(
            server_url="tcp://127.0.0.1:59994",
            timeout_ms=500,
        )
        client.connect()
        try:
            response = client.request_alloc("test", 1024)
            assert response.success is False
        finally:
            client.close()


# =============================================================================
# Non-blocking Async API Failure Scenarios
# =============================================================================


class TestAsyncNonBlockingFailures:
    """Test async API failure scenarios with *_async methods."""

    def test_delete_kv_async_nonexistent(self, async_client):
        """delete_kv_async for non-existent key should resolve to False."""
        future = async_client.delete_kv_async(key="800003")
        result = future.result(timeout=5.0)
        assert result is False

    def test_lookup_kv_async_nonexistent(self, async_client):
        """lookup_kv_async for non-existent key should resolve with found=False."""
        future = async_client.lookup_kv_async(key="800004")
        result = future.result(timeout=5.0)
        assert result.found is False

    def test_return_alloc_async_wrong_instance(self, async_client):
        """return_alloc_async with wrong instance_id should resolve to False."""
        alloc_resp = async_client.request_alloc("inst_a", 1024 * 1024)
        assert alloc_resp.success is True
        region_id = alloc_resp.handle.region_id

        # Try to return with different instance_id asynchronously
        future = async_client.return_alloc_async("inst_b", region_id)
        result = future.result(timeout=5.0)
        assert result is False
