# SPDX-License-Identifier: Apache-2.0
"""Tests for CxlMemoryAllocator (pool-based)."""

import mmap
from unittest.mock import MagicMock, PropertyMock

import torch
from lmcache.v1.memory_management import MemoryFormat

from maru_handler.memory import AllocHandle, OwnedRegionManager, PagedMemoryAllocator
from maru_handler.memory.mapper import DaxMapper
from maru_handler.memory.types import MappedRegion, OwnedRegion
from maru_lmcache.allocator import CxlMemoryAllocator

# =========================================================================
# Fixtures
# =========================================================================


def _make_mock_handler(pool_size=4096, chunk_size=1024):
    """Create a mock MaruHandler with real mmap-backed regions for pool."""
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

    # DaxMapper mock that returns real buffer views
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

    # alloc returns incrementing page indices
    page_counter = [0]

    def mock_alloc(size):
        idx = page_counter[0]
        page_counter[0] += 1
        buf = mapped_region.get_buffer_view(idx * chunk_size, size)
        return AllocHandle(buf=buf, _region_id=region_id, _page_index=idx, _size=size)

    handler.alloc.side_effect = mock_alloc
    handler.free = MagicMock()

    return handler


def _make_allocator(handler):
    """Create a CxlMemoryAllocator with standard test params.

    Shape is derived from chunk_size to match real LMCache behavior
    where chunk_size = product(shape) * dtype.itemsize.
    """
    chunk_size = handler.owned_region_manager.get_chunk_size()
    dtype = torch.float32
    num_elements = chunk_size // dtype.itemsize
    shape = torch.Size([num_elements])

    return CxlMemoryAllocator(
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
            encoded = CxlMemoryAllocator.encode_address(rid, pid)
            decoded_rid, decoded_pid = CxlMemoryAllocator.decode_address(encoded)
            assert decoded_rid == rid
            assert decoded_pid == pid

    def test_encode_is_deterministic(self):
        assert CxlMemoryAllocator.encode_address(1, 2) == (1 << 32) | 2


class TestPoolCreation:
    def test_pool_built_on_init(self):
        handler = _make_mock_handler()
        allocator = _make_allocator(handler)

        assert 100 in allocator._pool
        assert len(allocator._pool[100]) == 4

    def test_pool_objects_have_correct_address(self):
        handler = _make_mock_handler()
        allocator = _make_allocator(handler)

        for pid, obj in enumerate(allocator._pool[100]):
            rid, decoded_pid = CxlMemoryAllocator.decode_address(obj.metadata.address)
            assert rid == 100
            assert decoded_pid == pid


class TestAllocate:
    def test_allocate_returns_tensor_memory_obj(self):
        handler = _make_mock_handler()
        allocator = _make_allocator(handler)

        obj = allocator.allocate(torch.Size([256]), torch.float32)

        assert obj is not None
        assert obj.tensor is not None
        assert obj.metadata.ref_count == 1
        assert obj.metadata.dtype == torch.float32
        assert obj.metadata.phy_size == 1024  # chunk_size

    def test_allocate_returns_pooled_object(self):
        handler = _make_mock_handler()
        allocator = _make_allocator(handler)

        obj = allocator.allocate(torch.Size([256]), torch.float32)
        assert obj is allocator._pool[100][0]

    def test_allocate_address_encodes_rid_pid(self):
        handler = _make_mock_handler()
        allocator = _make_allocator(handler)

        obj1 = allocator.allocate(torch.Size([8]), torch.float32)
        obj2 = allocator.allocate(torch.Size([8]), torch.float32)

        rid1, pid1 = CxlMemoryAllocator.decode_address(obj1.metadata.address)
        rid2, pid2 = CxlMemoryAllocator.decode_address(obj2.metadata.address)

        assert rid1 == 100 and pid1 == 0
        assert rid2 == 100 and pid2 == 1

    def test_allocate_zero_size_returns_none(self):
        handler = _make_mock_handler()
        allocator = _make_allocator(handler)

        obj = allocator.allocate(torch.Size([0]), torch.float32)
        assert obj is None

    def test_allocate_handler_failure_returns_none(self):
        handler = _make_mock_handler()
        handler.alloc.side_effect = ValueError("pool exhausted")
        allocator = _make_allocator(handler)

        obj = allocator.allocate(torch.Size([8]), torch.float32)
        assert obj is None

    def test_allocate_tensor_writable(self):
        """Tensor backed by CXL memoryview should be writable."""
        handler = _make_mock_handler()
        allocator = _make_allocator(handler)

        obj = allocator.allocate(torch.Size([256]), torch.float32)
        assert obj is not None
        obj.tensor[:] = torch.ones(256, dtype=torch.float32)
        assert obj.tensor[0].item() == 1.0


class TestBatchedAllocate:
    def test_batched_allocate_returns_list(self):
        handler = _make_mock_handler()
        allocator = _make_allocator(handler)

        objs = allocator.batched_allocate(torch.Size([8]), torch.float32, batch_size=3)
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
        allocator = _make_allocator(handler)

        objs = allocator.batched_allocate(torch.Size([8]), torch.float32, batch_size=4)
        assert objs is None
        assert handler.free.call_count >= 2


class TestFree:
    def test_free_calls_handler_free(self):
        handler = _make_mock_handler()
        allocator = _make_allocator(handler)

        obj = allocator.allocate(torch.Size([8]), torch.float32)
        assert obj is not None

        allocator.free(obj)
        handler.free.assert_called_once()
        freed_handle = handler.free.call_args[0][0]
        assert freed_handle.region_id == 100
        assert freed_handle.page_index == 0

    def test_ref_count_lifecycle(self):
        """ref_count up/down tracking works correctly."""
        handler = _make_mock_handler()
        allocator = _make_allocator(handler)

        obj = allocator.allocate(torch.Size([8]), torch.float32)
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
        allocator = _make_allocator(handler)

        obj = allocator.allocate(torch.Size([8]), torch.float32)
        assert obj is not None

        handle = allocator.create_store_handle(obj)
        assert handle.region_id == 100
        assert handle.page_index == 0
        assert handle._size == obj.metadata.phy_size


class TestGetByLocation:
    def test_get_by_location_full_chunk(self):
        handler = _make_mock_handler(pool_size=4096, chunk_size=1024)
        allocator = _make_allocator(handler)

        obj = allocator.get_by_location(
            region_id=100,
            page_index=2,
            actual_size=1024,
            single_token_size=64,
        )
        assert obj is not None
        assert obj is allocator._pool[100][2]

    def test_get_by_location_partial_chunk(self):
        # Use 4D shape matching chunk_size for realistic partial chunk test
        # chunk_size=1024, dtype=float32(4B) → 256 elements
        # shape=[2, 2, 32, 2] → 256 elements, token_dim=shape[2]=32
        handler = _make_mock_handler(pool_size=4096, chunk_size=1024)
        chunk_size = 1024
        dtype = torch.float32
        shape = torch.Size([2, 2, 32, 2])
        single_token_size = chunk_size // 32  # 32 bytes per token

        allocator = CxlMemoryAllocator(
            handler=handler,
            shapes=[shape],
            dtypes=[dtype],
            fmt=MemoryFormat.KV_2LTD,
            chunk_size=chunk_size,
        )

        # Request half the tokens (16 tokens × 32 bytes = 512 bytes)
        obj = allocator.get_by_location(
            region_id=100,
            page_index=1,
            actual_size=512,
            single_token_size=single_token_size,
        )
        assert obj is not None
        assert obj is not allocator._pool[100][1]
        assert obj.metadata.phy_size == 512
        # Token dim should be halved: 32 → 16
        assert obj.metadata.shape[2] == 16

    def test_get_by_location_invalid_region(self):
        handler = _make_mock_handler()
        allocator = _make_allocator(handler)

        obj = allocator.get_by_location(
            region_id=999,
            page_index=0,
            actual_size=1024,
            single_token_size=64,
        )
        assert obj is None


class TestClose:
    def test_close_clears_pool(self):
        handler = _make_mock_handler()
        allocator = _make_allocator(handler)

        assert len(allocator._pool) > 0
        allocator.close()
        assert len(allocator._pool) == 0
