# SPDX-License-Identifier: Apache-2.0
"""Tests for MaruBackend storage backend."""

import asyncio
import mmap
import threading
from unittest.mock import MagicMock, PropertyMock, patch

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

from maru_handler.memory import AllocHandle, OwnedRegionManager, PagedMemoryAllocator
from maru_handler.memory.mapper import DaxMapper
from maru_handler.memory.types import MappedRegion, MemoryInfo, OwnedRegion
from maru_lmcache.allocator import CxlMemoryAllocator

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
    """Create a mock MaruHandler with mmap-backed regions."""
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

    # DaxMapper mock
    mapper = MagicMock(spec=DaxMapper)
    mapper.get_buffer_view.side_effect = (
        lambda rid, offset, size: mapped_region.get_buffer_view(offset, size)
    )
    mapper.get_region.side_effect = (
        lambda rid: mapped_region if rid == region_id else None
    )
    type(handler).mapper = PropertyMock(return_value=mapper)

    # OwnedRegionManager mock
    allocator_mock = MagicMock(spec=PagedMemoryAllocator)
    allocator_mock.page_count = page_count

    region = OwnedRegion(region_id=region_id, allocator=allocator_mock)

    orm = MagicMock(spec=OwnedRegionManager)
    orm.get_region_ids.return_value = [region_id]
    orm.get_owned_region.side_effect = lambda rid: region if rid == region_id else None
    orm.get_chunk_size.return_value = chunk_size

    type(handler).owned_region_manager = PropertyMock(return_value=orm)

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


def _make_memory_obj(allocator: CxlMemoryAllocator) -> TensorMemoryObj:
    """Allocate a real TensorMemoryObj from the allocator."""
    obj = allocator.allocate(TEST_SHAPE, TEST_DTYPE)
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
def allocator(mock_handler):
    return CxlMemoryAllocator(
        handler=mock_handler,
        shapes=[TEST_SHAPE],
        dtypes=[TEST_DTYPE],
        fmt=MemoryFormat.KV_2LTD,
        chunk_size=TEST_CHUNK_SIZE,
    )


@pytest.fixture
def backend(mock_handler, allocator, async_loop):
    """Create a MaruBackend with mocked internals."""
    from lmcache.v1.storage_backend.maru_backend import MaruBackend

    with patch.object(MaruBackend, "initialize_allocator", return_value=allocator):
        backend = MaruBackend.__new__(MaruBackend)
        backend.dst_device = "cpu"
        backend.config = MagicMock()
        backend.loop = async_loop
        backend.memory_allocator = allocator
        backend._handler = mock_handler

        # Chunk metadata
        backend._full_chunk_size_bytes = TEST_CHUNK_SIZE
        backend._single_token_size = TEST_CHUNK_SIZE // 256  # 4 bytes per token
        backend._mla_worker_id_as0_mode = False

        backend.data = {}
        backend.data_lock = threading.Lock()
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
    def test_submit_put_task_returns_future(self, backend, allocator):
        obj = _make_memory_obj(allocator)
        obj.parent_allocator = None
        key = _make_cache_key()

        future = backend.submit_put_task(key, obj)
        assert future is not None

        future.result(timeout=5)

        assert key in backend.data
        backend._handler.store.assert_called_once()

    def test_submit_put_task_tracks_in_flight(self, backend, allocator):
        obj = _make_memory_obj(allocator)
        obj.parent_allocator = None
        key = _make_cache_key()

        assert not backend.exists_in_put_tasks(key)

        future = backend.submit_put_task(key, obj)
        future.result(timeout=5)

        assert not backend.exists_in_put_tasks(key)

    def test_batched_submit_put_task(self, backend, allocator):
        keys = [_make_cache_key(i) for i in range(3)]
        objs = [_make_memory_obj(allocator) for _ in range(3)]
        for obj in objs:
            obj.parent_allocator = None

        futures = backend.batched_submit_put_task(keys, objs)
        assert futures is not None
        assert len(futures) == 3

        for future in futures:
            future.result(timeout=5)

        assert len(backend.data) == 3

    def test_submit_put_calls_callback(self, backend, allocator):
        obj = _make_memory_obj(allocator)
        obj.parent_allocator = None
        key = _make_cache_key()
        callback_called = []

        def callback(k):
            callback_called.append(k)

        future = backend.submit_put_task(key, obj, on_complete_callback=callback)
        future.result(timeout=5)

        assert len(callback_called) == 1
        assert callback_called[0] == key

    def test_ref_count_managed_during_put(self, backend, allocator):
        obj = _make_memory_obj(allocator)
        obj.parent_allocator = None
        key = _make_cache_key()
        initial_ref = obj.get_ref_count()

        future = backend.submit_put_task(key, obj)
        future.result(timeout=5)

        assert obj.get_ref_count() == initial_ref


class TestMaruBackendGet:
    def test_get_blocking_from_local_data(self, backend, allocator):
        obj = _make_memory_obj(allocator)
        obj.parent_allocator = None
        key = _make_cache_key()
        backend.data[key] = obj

        result = backend.get_blocking(key)
        assert result is obj

    def test_get_blocking_from_maru_server(self, backend, allocator):
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

        # Should be cached in data dict
        assert key in backend.data

    def test_get_blocking_not_found(self, backend):
        key = _make_cache_key()
        backend._handler.retrieve.return_value = None

        result = backend.get_blocking(key)
        assert result is None


class TestMaruBackendContains:
    def test_contains_local(self, backend, allocator):
        obj = _make_memory_obj(allocator)
        obj.parent_allocator = None
        key = _make_cache_key()
        backend.data[key] = obj

        assert backend.contains(key) is True

    def test_contains_remote(self, backend):
        key = _make_cache_key()
        backend._handler.exists.return_value = True

        assert backend.contains(key) is True
        backend._handler.exists.assert_called_once_with(key.to_string())

    def test_not_contains(self, backend):
        key = _make_cache_key()
        backend._handler.exists.return_value = False

        assert backend.contains(key) is False

    def test_contains_with_pin(self, backend, allocator):
        obj = _make_memory_obj(allocator)
        obj.parent_allocator = None
        key = _make_cache_key()
        backend.data[key] = obj
        initial_ref = obj.get_ref_count()

        backend.contains(key, pin=True)
        assert obj.get_ref_count() == initial_ref + 1


class TestMaruBackendPinUnpin:
    def test_pin_existing_key(self, backend, allocator):
        obj = _make_memory_obj(allocator)
        obj.parent_allocator = None
        key = _make_cache_key()
        backend.data[key] = obj
        initial_ref = obj.get_ref_count()

        assert backend.pin(key) is True
        assert obj.get_ref_count() == initial_ref + 1

    def test_pin_nonexistent_key(self, backend):
        key = _make_cache_key()
        assert backend.pin(key) is False

    def test_unpin_existing_key(self, backend, allocator):
        obj = _make_memory_obj(allocator)
        obj.parent_allocator = None
        key = _make_cache_key()
        backend.data[key] = obj
        obj.ref_count_up()  # pin
        pinned_ref = obj.get_ref_count()

        assert backend.unpin(key) is True
        assert obj.get_ref_count() == pinned_ref - 1

    def test_unpin_nonexistent_key(self, backend):
        key = _make_cache_key()
        assert backend.unpin(key) is False


class TestMaruBackendRemove:
    def test_remove_existing_key(self, backend, allocator):
        obj = _make_memory_obj(allocator)
        obj.parent_allocator = None
        key = _make_cache_key()
        backend.data[key] = obj

        result = backend.remove(key)
        assert result is True
        assert key not in backend.data
        backend._handler.delete.assert_called_once_with(key.to_string())

    def test_remove_nonexistent_key(self, backend):
        key = _make_cache_key()
        backend._handler.delete.return_value = False

        result = backend.remove(key)
        assert result is False


class TestMaruBackendLifecycle:
    def test_close_clears_state(self, backend, allocator):
        obj = _make_memory_obj(allocator)
        obj.parent_allocator = None
        key = _make_cache_key()
        backend.data[key] = obj

        backend.close()

        assert len(backend.data) == 0
        backend._handler.close.assert_called_once()

    def test_str_representation(self, backend):
        assert str(backend) == "MaruBackend"

    def test_get_allocator_backend_returns_self(self, backend):
        assert backend.get_allocator_backend() is backend

    def test_get_memory_allocator_returns_allocator(self, backend, allocator):
        assert backend.get_memory_allocator() is allocator


class TestMaruBackendStoreHandle:
    def test_store_handle_roundtrip(self, backend, allocator):
        """AllocHandle from create_store_handle should match original."""
        obj = _make_memory_obj(allocator)
        obj.parent_allocator = None

        handle = allocator.create_store_handle(obj)
        assert handle.region_id == 100
        assert handle.page_index == 0
        assert handle._size == obj.metadata.phy_size
