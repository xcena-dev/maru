# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Thread-safety tests for MaruHandler, OwnedRegionManager, and DaxMapper.

Unit tests — no RPC server needed.
Validates internal lock correctness under concurrent access.
"""

import threading
import time
from unittest.mock import MagicMock

import pytest
from conftest import _make_handle

from maru_handler.memory import DaxMapper, MemoryInfo, OwnedRegionManager

# =============================================================================
# Helpers
# =============================================================================


def _run_threads(target, args_list, num_threads=None):
    """Run target function in multiple threads and collect results.

    Args:
        target: callable(index, *args) -> result
        args_list: list of arg tuples, one per thread
        num_threads: if set, overrides len(args_list)

    Returns:
        list of results, one per thread
    """
    if num_threads is None:
        num_threads = len(args_list)

    results = [None] * num_threads
    errors = [None] * num_threads
    barrier = threading.Barrier(num_threads)

    def worker(idx):
        try:
            barrier.wait(timeout=5)
            results[idx] = target(idx, *args_list[idx])
        except Exception as e:
            errors[idx] = e

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    for i, err in enumerate(errors):
        if err is not None:
            raise AssertionError(f"Thread {i} raised: {err}") from err

    return results


# =============================================================================
# OwnedRegionManager concurrency tests
# =============================================================================


class TestConcurrentAllocateFree:
    """Concurrent allocate/free on OwnedRegionManager — no double alloc."""

    @pytest.fixture
    def mgr(self):
        mapper = DaxMapper()
        mgr = OwnedRegionManager(chunk_size=1024)
        # Add a region with 8 pages
        handle = _make_handle(10, length=8192)
        mapper.map_region(handle)
        mgr.add_region(10, 8192)
        return mgr

    def test_concurrent_allocate_no_duplicate(self, mgr):
        """Multiple threads allocating simultaneously get distinct pages."""
        num_threads = 8  # exactly 8 pages available

        def allocate_one(idx):
            return mgr.allocate()

        results = _run_threads(allocate_one, [() for _ in range(num_threads)])

        # All should succeed
        assert all(r is not None for r in results), (
            f"Some allocations failed: {results}"
        )

        # All (region_id, page_index) pairs must be unique
        assert len(set(results)) == num_threads, f"Duplicate allocations: {results}"

    def test_concurrent_allocate_exhaustion(self, mgr):
        """More threads than pages — some get None, no duplicates."""
        num_threads = 12  # 8 pages, 12 threads

        def allocate_one(idx):
            return mgr.allocate()

        results = _run_threads(allocate_one, [() for _ in range(num_threads)])

        successful = [r for r in results if r is not None]
        assert len(successful) == 8  # exactly 8 pages
        assert len(set(successful)) == 8  # all unique

    def test_concurrent_allocate_and_free(self, mgr):
        """Concurrent allocate + free doesn't corrupt state."""
        # Pre-allocate 4 pages
        pre_allocs = []
        for _ in range(4):
            r = mgr.allocate()
            assert r is not None
            pre_allocs.append(r)

        # 4 threads free, 4 threads allocate simultaneously
        def worker(idx):
            if idx < 4:
                rid, pid = pre_allocs[idx]
                mgr.free(rid, pid)
                return "freed"
            else:
                return mgr.allocate()

        _run_threads(worker, [() for _ in range(8)])

        # After: 4 freed + some re-allocated. Total pages = 8.
        # Just verify no exception and allocator is consistent.
        stats = mgr.get_stats()
        total = stats["total_allocated_pages"] + stats["total_free_pages"]
        assert total == 8


# =============================================================================
# DaxMapper concurrency tests
# =============================================================================


class TestConcurrentMapRegion:
    """Concurrent map_region on DaxMapper — no double mmap."""

    def test_concurrent_map_same_region(self):
        """Multiple threads mapping the same region — only 1 mmap call."""
        mapper = DaxMapper()
        handle = _make_handle(1, 4096)

        num_threads = 8

        def map_one(idx):
            return mapper.map_region(handle)

        results = _run_threads(map_one, [() for _ in range(num_threads)])

        # All should return the same MappedRegion object (idempotent)
        assert all(r is results[0] for r in results)
        assert mapper.get_region(1) is results[0]

    def test_concurrent_map_different_regions(self):
        """Multiple threads mapping different regions — all succeed."""
        mapper = DaxMapper()
        handles = [_make_handle(i, 1024) for i in range(8)]

        def map_one(idx):
            return mapper.map_region(handles[idx])

        results = _run_threads(map_one, [() for _ in range(8)])

        # All should succeed with distinct regions
        assert all(r is not None for r in results)
        region_ids = {r.region_id for r in results}
        assert len(region_ids) == 8


# =============================================================================
# MaruHandler thread-safety tests (mocked RPC)
# =============================================================================


def _make_mock_handler():
    """Create a MaruHandler with mocked RPC for thread-safety testing."""
    from maru_common import MaruConfig
    from maru_handler.handler import MaruHandler

    config = MaruConfig(
        pool_size=8192,
        chunk_size_bytes=1024,
        auto_connect=False,
        use_async_rpc=False,  # use sync RPC for simpler mocking
    )
    handler = MaruHandler(config)

    # Mock RPC client
    mock_rpc = MagicMock()
    mock_rpc.connect = MagicMock()

    # request_alloc returns a handle
    alloc_response = MagicMock()
    alloc_response.success = True
    alloc_response.handle = _make_handle(100, 8192)
    mock_rpc.request_alloc = MagicMock(return_value=alloc_response)

    # register_kv succeeds
    mock_rpc.register_kv = MagicMock()

    # delete_kv succeeds
    mock_rpc.delete_kv = MagicMock(return_value=True)

    # exists_kv — default False so store() duplicate-skip doesn't interfere
    mock_rpc.exists_kv = MagicMock(return_value=False)

    # lookup_kv returns found result
    lookup_result = MagicMock()
    lookup_result.found = True
    lookup_result.handle = _make_handle(100, 8192)
    lookup_result.kv_offset = 0
    lookup_result.kv_length = 4
    mock_rpc.lookup_kv = MagicMock(return_value=lookup_result)

    # return_alloc succeeds
    mock_rpc.return_alloc = MagicMock()

    # close succeeds
    mock_rpc.close = MagicMock()

    handler._rpc = mock_rpc
    handler.connect()

    return handler


class TestConcurrentStore:
    """Concurrent store operations — data integrity."""

    def test_concurrent_store_unique_keys(self):
        """Multiple threads storing different keys simultaneously."""
        handler = _make_mock_handler()
        num_threads = 8  # 8 pages available

        def store_one(idx):
            return handler.store(key=idx, info=MemoryInfo(view=memoryview(b"data")))

        results = _run_threads(store_one, [() for _ in range(num_threads)])

        assert all(r is True for r in results)

        # All keys tracked
        assert len(handler._key_to_location) == num_threads

        # All locations unique
        locations = list(handler._key_to_location.values())
        assert len(set(locations)) == num_threads

        handler.close()

    def test_concurrent_store_same_key(self):
        """Multiple threads storing the same key — last writer wins, no crash."""
        handler = _make_mock_handler()
        num_threads = 4

        def store_one(idx):
            return handler.store(
                key="42", info=MemoryInfo(view=memoryview(f"v{idx}".encode()))
            )

        results = _run_threads(store_one, [() for _ in range(num_threads)])

        # All should succeed (overwrite)
        assert all(r is True for r in results)

        # Exactly one location tracked for key 42
        assert "42" in handler._key_to_location

        handler.close()


class TestConcurrentRetrieve:
    """Concurrent retrieve — no blocking by writes."""

    def test_concurrent_retrieve(self):
        """Multiple threads retrieving simultaneously."""
        handler = _make_mock_handler()

        # Store some data first
        handler.store(key="1", info=MemoryInfo(view=memoryview(b"test")))

        num_threads = 8

        def retrieve_one(idx):
            return handler.retrieve(key="1")

        results = _run_threads(retrieve_one, [() for _ in range(num_threads)])

        # All should return data (mock returns fixed result)
        assert all(r is not None for r in results)

        handler.close()


class TestConcurrentExists:
    """Concurrent exists — lock-free."""

    def test_concurrent_exists(self):
        """Multiple threads calling exists simultaneously."""
        handler = _make_mock_handler()

        num_threads = 8

        def exists_one(idx):
            return handler.exists(key="1")

        results = _run_threads(exists_one, [() for _ in range(num_threads)])

        # Mock returns False; we're testing concurrent access, not return value
        assert all(r is False for r in results)

        handler.close()


class TestStoreAndRetrieveConcurrent:
    """Store and retrieve running concurrently — retrieve not blocked."""

    def test_retrieve_not_blocked_by_store(self):
        """retrieve() can proceed while store() holds _write_lock."""
        handler = _make_mock_handler()

        # Store initial data
        handler.store(key="1", info=MemoryInfo(view=memoryview(b"init")))

        store_started = threading.Event()
        store_proceed = threading.Event()

        original_register = handler._rpc.register_kv

        def slow_register(*args, **kwargs):
            store_started.set()
            store_proceed.wait(timeout=5)
            return original_register(*args, **kwargs)

        handler._rpc.register_kv = slow_register

        retrieve_done = threading.Event()
        retrieve_result = [None]

        def do_store():
            handler.store(key="2", info=MemoryInfo(view=memoryview(b"slow")))

        def do_retrieve():
            store_started.wait(timeout=5)
            # retrieve should NOT be blocked by _write_lock
            result = handler.retrieve(key="1")
            retrieve_result[0] = result
            retrieve_done.set()

        t_store = threading.Thread(target=do_store)
        t_retrieve = threading.Thread(target=do_retrieve)

        t_store.start()
        t_retrieve.start()

        # retrieve should complete while store is still in progress
        retrieve_done.wait(timeout=3)
        assert retrieve_result[0] is not None, "retrieve was blocked by store"

        # Let store finish
        store_proceed.set()
        t_store.join(timeout=5)
        t_retrieve.join(timeout=5)

        handler.close()


class TestCloseThreadSafety:
    """close() thread-safety — waits for writes, rejects new ops."""

    def test_store_rejected_during_close(self):
        """store() after _closing=True raises RuntimeError."""
        handler = _make_mock_handler()

        close_entered = threading.Event()
        close_proceed = threading.Event()

        original_owned_close = handler._owned.close

        def slow_close():
            close_entered.set()
            close_proceed.wait(timeout=5)
            return original_owned_close()

        handler._owned.close = slow_close

        store_error = [None]

        def do_close():
            handler.close()

        def do_store():
            close_entered.wait(timeout=5)
            # _closing should be True now, but close hasn't finished
            time.sleep(0.01)  # small delay to ensure _closing is set
            try:
                handler.store(key="99", info=MemoryInfo(view=memoryview(b"rejected")))
            except RuntimeError as e:
                store_error[0] = e

        t_close = threading.Thread(target=do_close)
        t_store = threading.Thread(target=do_store)

        t_close.start()
        t_store.start()

        # Let close finish after store attempt
        time.sleep(0.1)
        close_proceed.set()

        t_close.join(timeout=5)
        t_store.join(timeout=5)

        assert store_error[0] is not None
        assert "closing" in str(store_error[0]).lower()

    def test_close_waits_for_store(self):
        """close() waits for in-flight store to complete before teardown."""
        handler = _make_mock_handler()

        store_started = threading.Event()
        store_proceed = threading.Event()

        original_register = handler._rpc.register_kv
        register_called = [False]

        def slow_register(*args, **kwargs):
            store_started.set()
            store_proceed.wait(timeout=5)
            register_called[0] = True
            return original_register(*args, **kwargs)

        handler._rpc.register_kv = slow_register

        close_done = threading.Event()

        def do_store():
            handler.store(key="1", info=MemoryInfo(view=memoryview(b"data")))

        def do_close():
            store_started.wait(timeout=5)
            handler.close()
            close_done.set()

        t_store = threading.Thread(target=do_store)
        t_close = threading.Thread(target=do_close)

        t_store.start()
        t_close.start()

        # close should NOT complete until store finishes
        assert not close_done.wait(timeout=0.5), "close completed before store finished"

        # Let store finish
        store_proceed.set()
        t_store.join(timeout=5)
        t_close.join(timeout=5)

        assert register_called[0], "store's register_kv was never called"
        assert close_done.is_set(), "close never completed"
        assert not handler._connected
