# SPDX-License-Identifier: Apache-2.0
"""Tests for MaruBackend storage backend."""

import asyncio
import mmap
import threading
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip(
    "lmcache.v1.storage_backend",
    reason="lmcache not importable (CUDA C extensions required)",
)

import torch
from lmcache.utils import CacheEngineKey
from lmcache.v1.memory_management import (
    MemoryFormat,
    TensorMemoryObj,
)

from maru_handler.memory import AllocHandle
from maru_handler.memory.types import MappedRegion, MemoryInfo
from maru_lmcache.allocator import CxlMemoryAdapter

# =========================================================================
# Fixtures
# =========================================================================

# Match real KV cache: [2, 32, 256, 128] float16 = 4MB chunk
# For tests: use small shape that matches chunk_size
# chunk_size=1024, dtype=float32(4B) → 256 elements → shape=[256]
TEST_CHUNK_SIZE = 1024
TEST_DTYPE = torch.float32
TEST_SHAPE = torch.Size([256])  # 256 * 4 = 1024 bytes = chunk_size


def _make_mock_handler(pool_size=4096, chunk_size=TEST_CHUNK_SIZE):
    """Create a mock MaruHandler with facade API and mmap-backed regions."""
    handler = MagicMock()
    handler._connected = True

    region_id = 100
    page_count = pool_size // chunk_size

    # Real mmap for buffer views
    mmap_obj = mmap.mmap(-1, pool_size)
    mapped_region = MappedRegion(
        region_id=region_id,
        handle=MagicMock(region_id=region_id, length=pool_size),
        size=pool_size,
        _mmap_obj=mmap_obj,
    )

    # Facade methods
    handler.get_buffer_view.side_effect = (
        lambda rid, offset, size: mapped_region.get_buffer_view(offset, size)
        if rid == region_id
        else None
    )
    handler.get_region_page_count.side_effect = (
        lambda rid: page_count if rid == region_id else None
    )
    handler.get_owned_region_ids.return_value = [region_id]
    handler.get_chunk_size.return_value = chunk_size

    # set_on_region_added: capture callback and replay for existing regions
    def mock_set_on_region_added(callback):
        if callback is not None:
            callback(region_id, page_count)

    handler.set_on_region_added.side_effect = mock_set_on_region_added

    page_counter = [0]

    def mock_alloc(size):
        idx = page_counter[0]
        page_counter[0] += 1
        buf = mapped_region.get_buffer_view(idx * chunk_size, size)
        return AllocHandle(buf=buf, _region_id=region_id, _page_index=idx, _size=size)

    handler.alloc.side_effect = mock_alloc
    handler.free = MagicMock()
    handler.connect.return_value = True
    handler.close.return_value = None
    handler.store.return_value = True
    handler.retrieve.return_value = None
    handler.exists.return_value = False
    handler.delete.return_value = True

    return handler


def _make_cache_key(chunk_hash: int = 12345) -> CacheEngineKey:
    """Create a CacheEngineKey for testing."""
    return CacheEngineKey(
        model_name="test-model",
        world_size=1,
        worker_id=0,
        chunk_hash=chunk_hash,
        dtype=torch.float32,
    )


def _make_memory_obj(adapter: CxlMemoryAdapter) -> TensorMemoryObj:
    """Allocate a real TensorMemoryObj from the adapter."""
    obj = adapter.allocate(TEST_SHAPE, TEST_DTYPE)
    assert obj is not None
    return obj


@pytest.fixture
def async_loop():
    """Provide an asyncio event loop running in a background thread."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    yield loop
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=5)
    loop.close()


@pytest.fixture
def mock_handler():
    return _make_mock_handler()


@pytest.fixture
def adapter(mock_handler):
    return CxlMemoryAdapter(
        handler=mock_handler,
        shapes=[TEST_SHAPE],
        dtypes=[TEST_DTYPE],
        fmt=MemoryFormat.KV_2LTD,
        chunk_size=TEST_CHUNK_SIZE,
    )


@pytest.fixture
def backend(mock_handler, adapter, async_loop):
    """Create a MaruBackend with mocked internals."""
    from lmcache.v1.storage_backend.maru_backend import MaruBackend

    with patch.object(MaruBackend, "initialize_allocator", return_value=adapter):
        backend = MaruBackend.__new__(MaruBackend)
        backend.dst_device = "cpu"
        backend.config = MagicMock()
        backend.loop = async_loop
        backend.memory_allocator = adapter
        backend._handler = mock_handler

        # Chunk metadata
        backend._full_chunk_size_bytes = TEST_CHUNK_SIZE
        backend._single_token_size = TEST_CHUNK_SIZE // 256  # 4 bytes per token
        backend._mla_worker_id_as0_mode = False

        backend.put_lock = threading.Lock()
        backend.put_tasks = set()
    return backend


# =========================================================================
# Tests
# =========================================================================


class TestMaruBackendAllocate:
    def test_allocate_returns_memory_obj(self, backend):
        obj = backend.allocate(TEST_SHAPE, TEST_DTYPE)
        assert obj is not None
        assert obj.tensor is not None
        assert obj.metadata.dtype == TEST_DTYPE

    def test_batched_allocate_returns_list(self, backend):
        objs = backend.batched_allocate(TEST_SHAPE, TEST_DTYPE, batch_size=3)
        assert objs is not None
        assert len(objs) == 3


class TestMaruBackendPut:
    def test_submit_put_task_returns_future(self, backend, adapter):
        obj = _make_memory_obj(adapter)
        obj.parent_allocator = None
        key = _make_cache_key()

        future = backend.submit_put_task(key, obj)
        assert future is not None

        future.result(timeout=5)

        backend._handler.store.assert_called_once()

    def test_submit_put_task_tracks_in_flight(self, backend, adapter):
        obj = _make_memory_obj(adapter)
        obj.parent_allocator = None
        key = _make_cache_key()

        assert not backend.exists_in_put_tasks(key)

        future = backend.submit_put_task(key, obj)
        future.result(timeout=5)

        assert not backend.exists_in_put_tasks(key)

    def test_batched_submit_put_task(self, backend, adapter):
        keys = [_make_cache_key(i) for i in range(3)]
        objs = [_make_memory_obj(adapter) for _ in range(3)]
        for obj in objs:
            obj.parent_allocator = None

        futures = backend.batched_submit_put_task(keys, objs)
        assert futures is not None
        assert len(futures) == 3

        for future in futures:
            future.result(timeout=5)

        assert backend._handler.store.call_count == 3

    def test_submit_put_calls_callback(self, backend, adapter):
        obj = _make_memory_obj(adapter)
        obj.parent_allocator = None
        key = _make_cache_key()
        callback_called = []

        def callback(k):
            callback_called.append(k)

        future = backend.submit_put_task(key, obj, on_complete_callback=callback)
        future.result(timeout=5)

        assert len(callback_called) == 1
        assert callback_called[0] == key

    def test_ref_count_managed_during_put(self, backend, adapter):
        obj = _make_memory_obj(adapter)
        obj.parent_allocator = None
        key = _make_cache_key()
        initial_ref = obj.get_ref_count()

        future = backend.submit_put_task(key, obj)
        future.result(timeout=5)

        assert obj.get_ref_count() == initial_ref


class TestMaruBackendGet:
    def test_get_blocking_from_maru_server(self, backend, adapter):
        key = _make_cache_key()

        # Mock retrieve to return a MemoryInfo with rid/pid
        data_size = TEST_CHUNK_SIZE
        data = bytearray(data_size)
        mock_info = MemoryInfo(
            view=memoryview(data),
            region_id=100,
            page_index=0,
        )
        backend._handler.retrieve.return_value = mock_info

        result = backend.get_blocking(key)
        assert result is not None
        backend._handler.retrieve.assert_called_once()

    def test_get_blocking_not_found(self, backend):
        key = _make_cache_key()
        backend._handler.retrieve.return_value = None

        result = backend.get_blocking(key)
        assert result is None


class TestMaruBackendContains:
    def test_contains_true(self, backend):
        key = _make_cache_key()
        backend._handler.exists.return_value = True

        assert backend.contains(key) is True
        backend._handler.exists.assert_called_once_with(key.to_string())

    def test_contains_false(self, backend):
        key = _make_cache_key()
        backend._handler.exists.return_value = False

        assert backend.contains(key) is False


def _run_async(loop, coro):
    """Submit a coroutine to a running event loop and wait for result."""
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=5)


class TestMaruBackendAsyncLookup:
    """Tests for batched_async_contains and batched_get_non_blocking.

    These mirror the connector-era tests in test_connector.py::TestBatchOperations
    that were lost during the MaruBackend transition.
    """

    def test_batched_async_contains_all_hit(self, backend, async_loop):
        keys = [_make_cache_key(i) for i in range(3)]
        backend._handler.exists.return_value = True

        result = _run_async(
            async_loop, backend.batched_async_contains("lookup-1", keys)
        )
        assert result == 3

    def test_batched_async_contains_partial_prefix(self, backend, async_loop):
        keys = [_make_cache_key(i) for i in range(3)]
        backend._handler.exists.side_effect = [True, True, False]

        result = _run_async(
            async_loop, backend.batched_async_contains("lookup-2", keys)
        )
        assert result == 2

    def test_batched_async_contains_first_miss(self, backend, async_loop):
        keys = [_make_cache_key(i) for i in range(3)]
        backend._handler.exists.return_value = False

        result = _run_async(
            async_loop, backend.batched_async_contains("lookup-3", keys)
        )
        assert result == 0

    def test_batched_async_contains_empty_keys(self, backend, async_loop):
        result = _run_async(async_loop, backend.batched_async_contains("lookup-4", []))
        assert result == 0

    def test_batched_get_non_blocking_all_hit(self, backend, adapter, async_loop):
        keys = [_make_cache_key(i) for i in range(2)]

        # Pre-store: allocate objects and mock retrieve to return MemoryInfo
        objs = [_make_memory_obj(adapter) for _ in range(2)]
        infos = []
        for obj in objs:
            rid, pid = CxlMemoryAdapter.decode_address(obj.metadata.address)
            infos.append(
                MemoryInfo(
                    view=memoryview(bytearray(TEST_CHUNK_SIZE)),
                    region_id=rid,
                    page_index=pid,
                )
            )
        backend._handler.retrieve.side_effect = infos

        results = _run_async(
            async_loop, backend.batched_get_non_blocking("lookup-5", keys)
        )
        assert len(results) == 2
        for obj in results:
            assert obj is not None

    def test_batched_get_non_blocking_prefix_stop_on_miss(
        self, backend, adapter, async_loop
    ):
        """Second key is a miss → only first returned (prefix semantics)."""
        keys = [_make_cache_key(i) for i in range(3)]

        obj = _make_memory_obj(adapter)
        rid, pid = CxlMemoryAdapter.decode_address(obj.metadata.address)
        info = MemoryInfo(
            view=memoryview(bytearray(TEST_CHUNK_SIZE)),
            region_id=rid,
            page_index=pid,
        )
        # hit, miss, hit → should return only [hit]
        backend._handler.retrieve.side_effect = [info, None, info]

        results = _run_async(
            async_loop, backend.batched_get_non_blocking("lookup-6", keys)
        )
        assert len(results) == 1

    def test_batched_get_non_blocking_empty_keys(self, backend, async_loop):
        results = _run_async(
            async_loop, backend.batched_get_non_blocking("lookup-7", [])
        )
        assert results == []


class TestMaruBackendRemove:
    def test_remove_existing_key(self, backend):
        key = _make_cache_key()
        backend._handler.delete.return_value = True

        result = backend.remove(key)
        assert result is True
        backend._handler.delete.assert_called_once_with(key.to_string())

    def test_remove_nonexistent_key(self, backend):
        key = _make_cache_key()
        backend._handler.delete.return_value = False

        result = backend.remove(key)
        assert result is False


class TestMaruBackendLifecycle:
    def test_close_calls_handler(self, backend):
        backend.close()
        backend._handler.close.assert_called_once()

    def test_str_representation(self, backend):
        assert str(backend) == "MaruBackend"

    def test_get_allocator_backend_returns_self(self, backend):
        assert backend.get_allocator_backend() is backend

    def test_get_memory_allocator_returns_adapter(self, backend, adapter):
        assert backend.get_memory_allocator() is adapter


class TestMaruBackendStoreHandle:
    def test_store_handle_roundtrip(self, backend, adapter):
        """AllocHandle from create_store_handle should match original."""
        obj = _make_memory_obj(adapter)
        obj.parent_allocator = None

        handle = adapter.create_store_handle(obj)
        assert handle.region_id == 100
        assert handle.page_index == 0
        assert handle._size == obj.metadata.phy_size
