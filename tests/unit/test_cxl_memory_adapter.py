# SPDX-License-Identifier: Apache-2.0
"""Tests for CxlMemoryAdapter (pool-based)."""

import mmap
from unittest.mock import MagicMock

import torch
from lmcache.v1.memory_management import MemoryFormat

from maru_handler.memory import AllocHandle
from maru_handler.memory.types import MappedRegion
from maru_lmcache.adapter import CxlMemoryAdapter

# =========================================================================
# Fixtures
# =========================================================================


def _make_mock_handler(pool_size=4096, chunk_size=1024):
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
    handler.get_buffer_view.side_effect = lambda rid, offset, size: (
        mapped_region.get_buffer_view(offset, size) if rid == region_id else None
    )
    handler.get_region_page_count.side_effect = lambda rid: (
        page_count if rid == region_id else None
    )
    handler.get_owned_region_ids.return_value = [region_id]
    handler.get_chunk_size.return_value = chunk_size

    # set_on_region_added: capture callback and replay for existing regions
    _callback_holder = [None]

    def mock_set_on_region_added(callback):
        _callback_holder[0] = callback
        if callback is not None:
            callback(region_id, page_count)

    handler.set_on_region_added.side_effect = mock_set_on_region_added
    handler._callback_holder = _callback_holder

    # alloc returns incrementing page indices
    page_counter = [0]

    def mock_alloc(size):
        idx = page_counter[0]
        page_counter[0] += 1
        buf = mapped_region.get_buffer_view(idx * chunk_size, size)
        return AllocHandle(buf=buf, _region_id=region_id, _page_index=idx, _size=size)

    handler.alloc.side_effect = mock_alloc
    handler.free = MagicMock()

    # Store extra refs for tests that need expansion
    handler._mapped_region = mapped_region
    handler._page_counter = page_counter

    return handler


def _make_adapter(handler):
    """Create a CxlMemoryAdapter with standard test params."""
    chunk_size = handler.get_chunk_size()
    dtype = torch.float32
    num_elements = chunk_size // dtype.itemsize
    shape = torch.Size([num_elements])

    return CxlMemoryAdapter(
        handler=handler,
        shapes=[shape],
        dtypes=[dtype],
        fmt=MemoryFormat.KV_2LTD,
        chunk_size=chunk_size,
    )


# =========================================================================
# Tests
# =========================================================================


class TestAddressEncoding:
    def test_encode_decode_roundtrip(self):
        for rid, pid in [(0, 0), (1, 5), (100, 3), (0xFFFF, 0xFFFFFFFF)]:
            encoded = CxlMemoryAdapter.encode_address(rid, pid)
            decoded_rid, decoded_pid = CxlMemoryAdapter.decode_address(encoded)
            assert decoded_rid == rid
            assert decoded_pid == pid

    def test_encode_is_deterministic(self):
        assert CxlMemoryAdapter.encode_address(1, 2) == (1 << 32) | 2


class TestPoolCreation:
    def test_pool_built_on_init(self):
        handler = _make_mock_handler()
        adapter = _make_adapter(handler)

        assert 100 in adapter._pool
        assert len(adapter._pool[100]) == 4

    def test_pool_objects_have_correct_address(self):
        handler = _make_mock_handler()
        adapter = _make_adapter(handler)

        for pid, obj in enumerate(adapter._pool[100]):
            rid, decoded_pid = CxlMemoryAdapter.decode_address(obj.metadata.address)
            assert rid == 100
            assert decoded_pid == pid

    def test_pool_built_via_callback(self):
        """Pool is built through set_on_region_added callback, not direct access."""
        handler = _make_mock_handler()
        _make_adapter(handler)

        # Verify callback was registered
        handler.set_on_region_added.assert_called_once()
        # Verify facade methods were used (not internal accessors)
        handler.get_buffer_view.assert_called()


class TestRegionExpansionCallback:
    def test_callback_builds_new_region_pool(self):
        """Simulates region expansion: callback builds pool for new region."""
        handler = _make_mock_handler(pool_size=4096, chunk_size=1024)
        adapter = _make_adapter(handler)

        # Initial pool has region 100
        assert 100 in adapter._pool
        assert 200 not in adapter._pool

        # Create a new mmap for the expanded region
        new_mmap = mmap.mmap(-1, 2048)
        new_region = MappedRegion(
            region_id=200,
            handle=MagicMock(region_id=200, length=2048),
            size=2048,
            _mmap_obj=new_mmap,
        )

        # Update handler mock to include new region
        original_get_buffer_view = handler.get_buffer_view.side_effect

        def updated_get_buffer_view(rid, offset, size):
            if rid == 200:
                return new_region.get_buffer_view(offset, size)
            return original_get_buffer_view(rid, offset, size)

        handler.get_buffer_view.side_effect = updated_get_buffer_view

        # Fire the callback (simulating _expand_region)
        callback = handler._callback_holder[0]
        assert callback is not None
        callback(200, 2)  # 2 pages in new region

        # Verify new pool was built
        assert 200 in adapter._pool
        assert len(adapter._pool[200]) == 2

    def test_allocate_after_expansion(self):
        """After expansion callback, allocate works on new region pages."""
        handler = _make_mock_handler(pool_size=4096, chunk_size=1024)
        adapter = _make_adapter(handler)

        # Setup new region
        new_mmap = mmap.mmap(-1, 1024)
        new_region = MappedRegion(
            region_id=200,
            handle=MagicMock(region_id=200, length=1024),
            size=1024,
            _mmap_obj=new_mmap,
        )
        original_get_buffer_view = handler.get_buffer_view.side_effect

        def updated_get_buffer_view(rid, offset, size):
            if rid == 200:
                return new_region.get_buffer_view(offset, size)
            return original_get_buffer_view(rid, offset, size)

        handler.get_buffer_view.side_effect = updated_get_buffer_view

        # Fire callback for new region
        callback = handler._callback_holder[0]
        callback(200, 1)

        # Override alloc to return from new region
        handler.alloc.side_effect = lambda size: AllocHandle(
            buf=new_region.get_buffer_view(0, size),
            _region_id=200,
            _page_index=0,
            _size=size,
        )

        obj = adapter.allocate(torch.Size([256]), torch.float32)
        assert obj is not None
        assert obj is adapter._pool[200][0]


class TestAllocate:
    def test_allocate_returns_tensor_memory_obj(self):
        handler = _make_mock_handler()
        adapter = _make_adapter(handler)

        obj = adapter.allocate(torch.Size([256]), torch.float32)

        assert obj is not None
        assert obj.tensor is not None
        assert obj.metadata.ref_count == 1
        assert obj.metadata.dtype == torch.float32
        assert obj.metadata.phy_size == 1024  # chunk_size

    def test_allocate_returns_pooled_object(self):
        handler = _make_mock_handler()
        adapter = _make_adapter(handler)

        obj = adapter.allocate(torch.Size([256]), torch.float32)
        assert obj is adapter._pool[100][0]

    def test_allocate_address_encodes_rid_pid(self):
        handler = _make_mock_handler()
        adapter = _make_adapter(handler)

        obj1 = adapter.allocate(torch.Size([8]), torch.float32)
        obj2 = adapter.allocate(torch.Size([8]), torch.float32)

        rid1, pid1 = CxlMemoryAdapter.decode_address(obj1.metadata.address)
        rid2, pid2 = CxlMemoryAdapter.decode_address(obj2.metadata.address)

        assert rid1 == 100 and pid1 == 0
        assert rid2 == 100 and pid2 == 1

    def test_allocate_zero_size_returns_none(self):
        handler = _make_mock_handler()
        adapter = _make_adapter(handler)

        obj = adapter.allocate(torch.Size([0]), torch.float32)
        assert obj is None

    def test_allocate_handler_failure_returns_none(self):
        handler = _make_mock_handler()
        handler.alloc.side_effect = ValueError("pool exhausted")
        adapter = _make_adapter(handler)

        obj = adapter.allocate(torch.Size([8]), torch.float32)
        assert obj is None

    def test_allocate_tensor_writable(self):
        """Tensor backed by CXL memoryview should be writable."""
        handler = _make_mock_handler()
        adapter = _make_adapter(handler)

        obj = adapter.allocate(torch.Size([256]), torch.float32)
        assert obj is not None
        obj.tensor[:] = torch.ones(256, dtype=torch.float32)
        assert obj.tensor[0].item() == 1.0


class TestBatchedAllocate:
    def test_batched_allocate_returns_list(self):
        handler = _make_mock_handler()
        adapter = _make_adapter(handler)

        objs = adapter.batched_allocate(torch.Size([8]), torch.float32, batch_size=3)
        assert objs is not None
        assert len(objs) == 3
        addresses = [o.metadata.address for o in objs]
        assert len(set(addresses)) == 3

    def test_batched_allocate_rollback_on_failure(self):
        handler = _make_mock_handler()
        call_count = [0]
        original_alloc = handler.alloc.side_effect

        def fail_on_third(size):
            call_count[0] += 1
            if call_count[0] == 3:
                raise ValueError("exhausted")
            return original_alloc(size)

        handler.alloc.side_effect = fail_on_third
        adapter = _make_adapter(handler)

        objs = adapter.batched_allocate(torch.Size([8]), torch.float32, batch_size=4)
        assert objs is None
        # free() is a no-op on CxlMemoryAdapter (pages managed by MaruBackend.remove)


class TestFree:
    def test_free_is_noop(self):
        """free() is a no-op — pages managed by MaruBackend.remove()."""
        handler = _make_mock_handler()
        adapter = _make_adapter(handler)

        obj = adapter.allocate(torch.Size([8]), torch.float32)
        assert obj is not None

        adapter.free(obj)
        # No handler.free call since adapter.free is a no-op

    def test_ref_count_lifecycle(self):
        """ref_count up/down tracking works correctly."""
        handler = _make_mock_handler()
        adapter = _make_adapter(handler)

        obj = adapter.allocate(torch.Size([8]), torch.float32)
        assert obj is not None
        assert obj.metadata.ref_count == 1

        obj.ref_count_up()
        assert obj.metadata.ref_count == 2

        obj.parent_allocator = None
        obj.ref_count_down()
        assert obj.metadata.ref_count == 1
        obj.ref_count_down()
        assert obj.metadata.ref_count == 0


class TestCreateStoreHandle:
    def test_create_store_handle_roundtrip(self):
        handler = _make_mock_handler()
        adapter = _make_adapter(handler)

        obj = adapter.allocate(torch.Size([8]), torch.float32)
        assert obj is not None

        handle = adapter.create_store_handle(obj)
        assert handle.region_id == 100
        assert handle.page_index == 0
        assert handle._size == obj.metadata.phy_size


class TestGetByLocation:
    def test_get_by_location_full_chunk(self):
        handler = _make_mock_handler(pool_size=4096, chunk_size=1024)
        adapter = _make_adapter(handler)

        obj = adapter.get_by_location(
            region_id=100,
            page_index=2,
            actual_size=1024,
            single_token_size=64,
        )
        assert obj is not None
        assert obj is adapter._pool[100][2]

    def test_get_by_location_partial_chunk(self):
        # Use 4D shape matching chunk_size for realistic partial chunk test
        # chunk_size=1024, dtype=float32(4B) → 256 elements
        # shape=[2, 2, 32, 2] → 256 elements, token_dim=shape[2]=32
        handler = _make_mock_handler(pool_size=4096, chunk_size=1024)
        chunk_size = 1024
        dtype = torch.float32
        shape = torch.Size([2, 2, 32, 2])
        single_token_size = chunk_size // 32  # 32 bytes per token

        adapter = CxlMemoryAdapter(
            handler=handler,
            shapes=[shape],
            dtypes=[dtype],
            fmt=MemoryFormat.KV_2LTD,
            chunk_size=chunk_size,
        )

        # Request half the tokens (16 tokens × 32 bytes = 512 bytes)
        obj = adapter.get_by_location(
            region_id=100,
            page_index=1,
            actual_size=512,
            single_token_size=single_token_size,
        )
        assert obj is not None
        assert obj is not adapter._pool[100][1]
        assert obj.metadata.phy_size == 512
        # Token dim should be halved: 32 → 16
        assert obj.metadata.shape[2] == 16

    def test_get_by_location_invalid_region(self):
        handler = _make_mock_handler()
        adapter = _make_adapter(handler)

        obj = adapter.get_by_location(
            region_id=999,
            page_index=0,
            actual_size=1024,
            single_token_size=64,
        )
        assert obj is None


class TestClose:
    def test_close_clears_pool_and_unregisters_callback(self):
        handler = _make_mock_handler()
        adapter = _make_adapter(handler)

        assert len(adapter._pool) > 0
        adapter.close()
        assert len(adapter._pool) == 0
        # Callback should be unregistered (set to None)
        assert handler.set_on_region_added.call_count == 2  # init + close
