# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Integration tests for MaruHandler client interface."""

import pytest

from maru import MaruConfig, MaruHandler
from maru_handler.memory import MemoryInfo

pytestmark = pytest.mark.integration


class TestMaruHandler:
    """Test cases for MaruHandler (requires real ZMQ)."""

    def test_handle_connect(self, server_thread, server_port):
        """Test MaruHandler connection."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
            auto_connect=False,
        )
        handler = MaruHandler(config)

        assert handler.connected is False

        success = handler.connect()
        assert success is True
        assert handler.connected is True
        assert handler.pool_handle is not None
        assert handler.pool_handle.length >= 4096
        assert handler.allocator is not None
        assert handler.allocator.page_count == handler.pool_handle.length // 1024

        handler.close()
        assert handler.connected is False

    def test_handle_context_manager(self, server_thread, server_port):
        """Test MaruHandler as context manager."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            assert handler.connected is True
            stats = handler.get_stats()
            assert "kv_manager" in stats
            assert "allocation_manager" in stats
            assert "allocator" in stats

        assert handler.connected is False

    def test_handle_get_stats(self, server_thread, server_port):
        """Test getting server statistics."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            stats = handler.get_stats()

            assert stats["kv_manager"]["total_entries"] == 0
            assert stats["allocation_manager"]["num_allocations"] >= 1
            expected_pages = handler.pool_handle.length // 1024
            assert stats["allocator"]["page_count"] == expected_pages
            assert stats["allocator"]["free_pages"] == expected_pages

    def test_store_and_retrieve(self, server_thread, server_port):
        """Test store and retrieve round-trip."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            data = b"hello world"
            info = MemoryInfo(view=memoryview(data))
            assert handler.store(key="12345", info=info) is True
            assert handler.exists(key="12345") is True

            result = handler.retrieve(key="12345")
            assert result is not None
            assert bytes(result.view[: len(data)]) == data

    def test_store_and_delete_frees_page(self, server_thread, server_port):
        """Test that delete frees the page."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            handler.store(key="1", info=MemoryInfo(view=memoryview(b"data1")))
            assert handler.allocator.num_allocated == 1

            handler.delete(key="1")
            assert handler.allocator.num_allocated == 0
            assert handler.allocator.num_free_pages == handler.allocator.page_count

    def test_store_auto_expansion(self, server_thread, server_port):
        """Test that store auto-expands to a new region when exhausted."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            # Fill all pages in first region
            page_count = handler.allocator.page_count
            for i in range(page_count):
                assert (
                    handler.store(key=str(i), info=MemoryInfo(view=memoryview(b"x" * 100)))
                    is True
                )

            # Next store triggers auto-expansion to new region
            overflow_data = b"overflow"
            assert (
                handler.store(
                    key=str(page_count + 1),
                    info=MemoryInfo(view=memoryview(overflow_data)),
                )
                is True
            )

            # Verify 2 regions exist
            assert handler.owned_region_manager is not None
            stats = handler.owned_region_manager.get_stats()
            assert stats["num_regions"] == 2

            # Data should be retrievable
            result = handler.retrieve(key=str(page_count + 1))
            assert result is not None
            assert bytes(result.view[: len(overflow_data)]) == overflow_data

    def test_store_delete_reuse(self, server_thread, server_port):
        """Test that delete followed by store reuses the page."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            # Fill all pages
            page_count = handler.allocator.page_count
            for i in range(page_count):
                handler.store(key=str(i), info=MemoryInfo(view=memoryview(b"data")))

            assert handler.allocator.num_free_pages == 0

            # Delete one
            handler.delete(key="2")
            assert handler.allocator.num_free_pages == 1

            # Store new key — should succeed using freed page
            new_key = str(page_count + 100)  # key not in range(page_count)
            assert (
                handler.store(
                    key=new_key, info=MemoryInfo(view=memoryview(b"new data"))
                )
                is True
            )
            assert handler.allocator.num_free_pages == 0

    def test_store_duplicate_key_is_skipped(self, server_thread, server_port):
        """Test that storing with same key is skipped (idempotent)."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            v1 = b"version1"
            handler.store(key="1", info=MemoryInfo(view=memoryview(v1)))
            assert handler.allocator.num_allocated == 1

            # Second store with same key is skipped
            handler.store(key="1", info=MemoryInfo(view=memoryview(b"version2")))
            assert handler.allocator.num_allocated == 1  # still 1 page

            # Original value is preserved
            result = handler.retrieve(key="1")
            assert result is not None
            assert bytes(result.view[: len(v1)]) == v1

    def test_store_exceeds_chunk_size(self, server_thread, server_port):
        """Test that store fails when data exceeds chunk_size."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            data = b"x" * 1025  # exceeds 1024
            assert handler.store(key="1", info=MemoryInfo(view=memoryview(data))) is False


class TestMaruHandlerMultiRegion:
    """Test cases for multi-region store behavior."""

    def test_retrieve_from_expanded_region(self, server_thread, server_port):
        """Test data integrity across multiple store regions."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=2048,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            # Fill all pages in first region
            page_count = handler.allocator.page_count
            d1, d2, d3 = b"region1_data1", b"region1_data2", b"region2_data1"
            handler.store(key="1", info=MemoryInfo(view=memoryview(d1)))
            handler.store(key="2", info=MemoryInfo(view=memoryview(d2)))
            for i in range(3, page_count + 1):
                handler.store(key=str(i), info=MemoryInfo(view=memoryview(b"filler")))

            # Next store triggers auto-expand to region 2
            overflow_key = str(page_count + 1)
            handler.store(key=overflow_key, info=MemoryInfo(view=memoryview(d3)))

            assert handler.owned_region_manager.get_stats()["num_regions"] == 2

            # All data should be retrievable
            assert bytes(handler.retrieve(key="1").view[: len(d1)]) == d1
            assert bytes(handler.retrieve(key="2").view[: len(d2)]) == d2
            assert bytes(handler.retrieve(key=overflow_key).view[: len(d3)]) == d3

    def test_delete_from_expanded_region(self, server_thread, server_port):
        """Test delete from second store region."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=1024,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            # Fill all pages in first region
            page_count = handler.allocator.page_count
            handler.store(key="1", info=MemoryInfo(view=memoryview(b"data1")))
            for i in range(2, page_count + 1):
                handler.store(key=str(i), info=MemoryInfo(view=memoryview(b"filler")))

            # Next store triggers expansion
            overflow_key = str(page_count + 1)
            handler.store(
                key=overflow_key,
                info=MemoryInfo(view=memoryview(b"data2")),
            )

            stats = handler.owned_region_manager.get_stats()
            assert stats["num_regions"] == 2
            total_before = stats["total_allocated_pages"]

            # Delete from second region
            handler.delete(key=overflow_key)
            stats = handler.owned_region_manager.get_stats()
            assert stats["total_allocated_pages"] == total_before - 1

    def test_duplicate_key_across_regions_is_skipped(self, server_thread, server_port):
        """Test that storing a duplicate key is skipped even across regions."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=1024,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            v1 = b"version1"
            handler.store(key="1", info=MemoryInfo(view=memoryview(v1)))
            # Second store with same key is skipped
            handler.store(key="1", info=MemoryInfo(view=memoryview(b"version2")))

            # Original value is preserved
            result = handler.retrieve(key="1")
            assert result is not None
            assert bytes(result.view[: len(v1)]) == v1

    def test_close_returns_all_regions(self, server_thread, server_port):
        """Test that close returns all regions to server."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=1024,
            chunk_size_bytes=1024,
            auto_connect=False,
        )

        handler = MaruHandler(config)
        handler.connect()

        # Fill first region and trigger expansion
        page_count = handler.allocator.page_count
        for i in range(page_count):
            handler.store(key=str(i), info=MemoryInfo(view=memoryview(b"filler")))
        # Next store triggers expansion
        handler.store(
            key=str(page_count + 1),
            info=MemoryInfo(view=memoryview(b"overflow")),
        )

        assert handler.owned_region_manager.get_stats()["num_regions"] == 2

        handler.close()
        assert handler.connected is False
        assert handler.owned_region_manager is None

    def test_backward_compat_properties(self, server_thread, server_port):
        """Test backward compatibility: pool_handle and allocator properties."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            # pool_handle returns initial handle
            assert handler.pool_handle is not None
            assert handler.pool_handle.length >= 4096

            # allocator returns first region's allocator
            assert handler.allocator is not None
            assert handler.allocator.page_count == handler.pool_handle.length // 1024

            handler.store(key="1", info=MemoryInfo(view=memoryview(b"test")))
            assert handler.allocator.num_allocated == 1

    def test_stats_with_multiple_regions(self, server_thread, server_port):
        """Test stats include multi-region info and backward compat."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=1024,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            # Fill all pages in first region
            page_count = handler.allocator.page_count
            for i in range(page_count):
                handler.store(key=str(i), info=MemoryInfo(view=memoryview(b"filler")))
            # Next store triggers expansion
            handler.store(
                key=str(page_count + 1),
                info=MemoryInfo(view=memoryview(b"overflow")),
            )

            stats = handler.get_stats()

            # New multi-region stats
            assert "store_regions" in stats
            assert stats["store_regions"]["num_regions"] == 2

            # Backward compat: "allocator" key with first region stats
            assert "allocator" in stats
            assert stats["allocator"]["page_count"] == page_count


class TestMaruHandlerStorePrefix:
    """Test store with prefix parameter."""

    def test_store_with_prefix(self, server_thread, server_port):
        """Store with prefix parameter, verify prefix+data concatenated."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            prefix = b"\x01\x02"
            data = b"hello"
            info = MemoryInfo(view=memoryview(data))

            assert handler.store(key="1", info=info, prefix=prefix) is True

            # Retrieve and verify prefix+data layout
            result = handler.retrieve(key="1")
            assert result is not None
            expected = prefix + data
            assert bytes(result.view[: len(expected)]) == expected

    def test_store_with_empty_prefix(self, server_thread, server_port):
        """Store with empty prefix, verify it works the same as prefix=None."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            data = b"test data"
            info = MemoryInfo(view=memoryview(data))

            # Store with empty prefix
            assert handler.store(key="1", info=info, prefix=b"") is True

            # Retrieve and verify data only
            result = handler.retrieve(key="1")
            assert result is not None
            assert bytes(result.view[: len(data)]) == data


class TestMaruHandlerBatch:
    """Test handler-level batch operations."""

    def test_batch_store_and_batch_retrieve(self, server_thread, server_port):
        """Call batch_store then batch_retrieve, verify round-trip."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            keys = ["1", "2", "3"]
            data = [b"data1", b"data2", b"data3"]
            infos = [MemoryInfo(view=memoryview(d)) for d in data]

            # Batch store
            results = handler.batch_store(keys=keys, infos=infos)
            assert results == [True, True, True]

            # Batch retrieve
            retrieved = handler.batch_retrieve(keys=keys)
            assert len(retrieved) == 3
            for i, result in enumerate(retrieved):
                assert result is not None
                assert bytes(result.view[: len(data[i])]) == data[i]

    def test_batch_store_with_prefixes(self, server_thread, server_port):
        """Call batch_store with prefixes parameter, verify prefix+data layout."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            keys = ["1", "2"]
            data = [b"data1", b"data2"]
            prefixes = [b"\x01", b"\x02\x03"]
            infos = [MemoryInfo(view=memoryview(d)) for d in data]

            # Batch store with prefixes
            results = handler.batch_store(keys=keys, infos=infos, prefixes=prefixes)
            assert results == [True, True]

            # Verify prefix+data layout
            for i, key in enumerate(keys):
                result = handler.retrieve(key=key)
                assert result is not None
                expected = prefixes[i] + data[i]
                assert bytes(result.view[: len(expected)]) == expected

    def test_batch_store_mismatched_lengths(self, server_thread, server_port):
        """Call batch_store with mismatched keys/infos lengths, should raise ValueError."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            keys = ["1", "2"]
            infos = [MemoryInfo(view=memoryview(b"data1"))]

            with pytest.raises(
                ValueError, match="keys and infos must have the same length"
            ):
                handler.batch_store(keys=keys, infos=infos)

    def test_batch_exists(self, server_thread, server_port):
        """Store some keys, call batch_exists, verify correct True/False results."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            # Store keys 1 and 3
            handler.store(key="1", info=MemoryInfo(view=memoryview(b"data1")))
            handler.store(key="3", info=MemoryInfo(view=memoryview(b"data3")))

            # Check existence of keys 1, 2, 3
            results = handler.batch_exists(keys=["1", "2", "3"])
            assert results == [True, False, True]


class TestMaruHandlerWithAsyncServer:
    """Test MaruHandler against async RPC server (production setup)."""

    @pytest.fixture
    def async_server_port(self):
        import socket

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    @pytest.fixture
    def async_server_thread(self, async_server_port):
        import threading
        import time

        from maru_server.rpc_async_server import RpcAsyncServer
        from maru_server.server import MaruServer

        maru_server = MaruServer()
        rpc_server = RpcAsyncServer(
            maru_server, host="127.0.0.1", port=async_server_port, num_workers=4
        )
        thread = threading.Thread(target=rpc_server.start, daemon=True)
        thread.start()
        time.sleep(0.2)
        yield rpc_server
        rpc_server.stop()
        thread.join(timeout=5.0)

    def test_connect_to_async_server(self, async_server_thread, async_server_port):
        """Test MaruHandler connects to RpcAsyncServer."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{async_server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
            use_async_rpc=True,
        )

        with MaruHandler(config) as handler:
            assert handler.connected is True
            assert handler.pool_handle is not None
            assert handler.pool_handle.length >= 4096

    def test_store_and_retrieve_with_async_server(
        self, async_server_thread, async_server_port
    ):
        """Test store and retrieve round-trip with async server."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{async_server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
            use_async_rpc=True,
        )

        with MaruHandler(config) as handler:
            data = b"hello async world"
            info = MemoryInfo(view=memoryview(data))
            assert handler.store(key="42", info=info) is True
            assert handler.exists(key="42") is True

            result = handler.retrieve(key="42")
            assert result is not None
            assert bytes(result.view[: len(data)]) == data

    def test_delete_with_async_server(self, async_server_thread, async_server_port):
        """Test delete operation with async server."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{async_server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
            use_async_rpc=True,
        )

        with MaruHandler(config) as handler:
            handler.store(key="1", info=MemoryInfo(view=memoryview(b"data1")))
            assert handler.exists(key="1") is True

            handler.delete(key="1")
            assert handler.exists(key="1") is False

    def test_auto_expansion_with_async_server(
        self, async_server_thread, async_server_port
    ):
        """Test auto-expansion with async server."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{async_server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
            use_async_rpc=True,
        )

        with MaruHandler(config) as handler:
            # Fill all pages in first region
            page_count = handler.allocator.page_count
            for i in range(page_count):
                assert (
                    handler.store(key=str(i), info=MemoryInfo(view=memoryview(b"x" * 100)))
                    is True
                )

            # Trigger expansion
            overflow_data = b"overflow"
            overflow_key = str(page_count + 1)
            assert (
                handler.store(
                    key=overflow_key,
                    info=MemoryInfo(view=memoryview(overflow_data)),
                )
                is True
            )

            # Verify expansion happened
            stats = handler.owned_region_manager.get_stats()
            assert stats["num_regions"] == 2

            # Verify data retrievable
            result = handler.retrieve(key=overflow_key)
            assert result is not None
            assert bytes(result.view[: len(overflow_data)]) == overflow_data

    def test_batch_operations_with_async_server(
        self, async_server_thread, async_server_port
    ):
        """Test batch store and retrieve with async server."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{async_server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
            use_async_rpc=True,
        )

        with MaruHandler(config) as handler:
            keys = ["10", "20", "30"]
            data = [b"batch1", b"batch2", b"batch3"]
            infos = [MemoryInfo(view=memoryview(d)) for d in data]

            results = handler.batch_store(keys=keys, infos=infos)
            assert results == [True, True, True]

            retrieved = handler.batch_retrieve(keys=keys)
            assert len(retrieved) == 3
            for i, result in enumerate(retrieved):
                assert result is not None
                assert bytes(result.view[: len(data[i])]) == data[i]

    def test_stats_with_async_server(self, async_server_thread, async_server_port):
        """Test get_stats with async server."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{async_server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
            use_async_rpc=True,
        )

        with MaruHandler(config) as handler:
            stats = handler.get_stats()
            assert "kv_manager" in stats
            assert "allocation_manager" in stats
            assert "allocator" in stats


class TestMaruHandlerSyncRpc:
    """Test MaruHandler with sync RPC client."""

    def test_connect_with_sync_rpc(self, server_thread, server_port):
        """Test MaruHandler connects using sync RpcClient."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
            use_async_rpc=False,
        )

        with MaruHandler(config) as handler:
            assert handler.connected is True
            assert handler.pool_handle is not None
            assert handler.pool_handle.length >= 4096

    def test_store_and_retrieve_with_sync_rpc(self, server_thread, server_port):
        """Test store and retrieve with sync RPC client."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
            use_async_rpc=False,
        )

        with MaruHandler(config) as handler:
            data = b"sync rpc data"
            info = MemoryInfo(view=memoryview(data))
            assert handler.store(key="100", info=info) is True
            assert handler.exists(key="100") is True

            result = handler.retrieve(key="100")
            assert result is not None
            assert bytes(result.view[: len(data)]) == data

    def test_delete_with_sync_rpc(self, server_thread, server_port):
        """Test delete with sync RPC client."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
            use_async_rpc=False,
        )

        with MaruHandler(config) as handler:
            handler.store(key="1", info=MemoryInfo(view=memoryview(b"data1")))
            assert handler.exists(key="1") is True

            handler.delete(key="1")
            assert handler.exists(key="1") is False

    def test_batch_operations_with_sync_rpc(self, server_thread, server_port):
        """Test batch store and retrieve with sync RPC client."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
            use_async_rpc=False,
        )

        with MaruHandler(config) as handler:
            keys = ["50", "60", "70"]
            data = [b"sync1", b"sync2", b"sync3"]
            infos = [MemoryInfo(view=memoryview(d)) for d in data]

            results = handler.batch_store(keys=keys, infos=infos)
            assert results == [True, True, True]

            retrieved = handler.batch_retrieve(keys=keys)
            assert len(retrieved) == 3
            for i, result in enumerate(retrieved):
                assert result is not None
                assert bytes(result.view[: len(data[i])]) == data[i]

    def test_stats_with_sync_rpc(self, server_thread, server_port):
        """Test get_stats with sync RPC client."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
            use_async_rpc=False,
        )

        with MaruHandler(config) as handler:
            stats = handler.get_stats()
            assert "kv_manager" in stats
            assert "allocation_manager" in stats
            assert "allocator" in stats


class TestCrossHandlerSharing:
    """Test two MaruHandler instances sharing KV metadata via the registry.

    With real MaruShmClient and CXL DAX hardware, handlers share physical
    memory regions. This tests both metadata sharing through the KV registry
    and actual data visibility across handlers.
    """

    def test_metadata_visibility_across_handlers(self, server_thread, server_port):
        """Handler B can see KV metadata (exists) for keys stored by Handler A."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        # Both handlers alive concurrently share the KV registry
        with MaruHandler(config) as handler_a, MaruHandler(config) as handler_b:
            # Handler A stores data
            data = b"shared metadata"
            info = MemoryInfo(view=memoryview(data))
            assert handler_a.store(key="999", info=info) is True

            # Handler B can see the key exists in KV registry
            assert handler_b.exists(key="999") is True

            # Handler A deletes the key
            handler_a.delete(key="999")

            # Handler B sees it's gone
            assert handler_b.exists(key="999") is False

    def test_batch_exists_across_handlers(self, server_thread, server_port):
        """Handler B checks existence of multiple keys stored by Handler A."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        # Both handlers alive concurrently
        with MaruHandler(config) as handler_a, MaruHandler(config) as handler_b:
            # Handler A stores keys 2000 and 2002
            handler_a.store(key="2000", info=MemoryInfo(view=memoryview(b"data2000")))
            handler_a.store(key="2002", info=MemoryInfo(view=memoryview(b"data2002")))

            # Handler B checks existence via shared KV registry
            results = handler_b.batch_exists(keys=["2000", "2001", "2002"])
            assert results == [True, False, True]

    def test_concurrent_stores_different_keys(self, server_thread, server_port):
        """Two handlers can store to different keys concurrently."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler_a, MaruHandler(config) as handler_b:
            # Each handler stores to its own keys
            data_a = b"from handler A"
            data_b = b"from handler B"

            assert (
                handler_a.store(key="100", info=MemoryInfo(view=memoryview(data_a)))
                is True
            )
            assert (
                handler_b.store(key="200", info=MemoryInfo(view=memoryview(data_b)))
                is True
            )

            # Both keys visible to both handlers
            assert handler_a.exists(key="100") is True
            assert handler_a.exists(key="200") is True
            assert handler_b.exists(key="100") is True
            assert handler_b.exists(key="200") is True

    def test_read_only_mapping_code_path(self, server_thread, server_port):
        """Verify retrieve uses read-only mapping for non-owned regions.

        With real CXL shared memory, handler B reads handler A's data via
        read-only mmap. This exercises the mapper.map_region(read_only=True)
        code path with actual shared memory.
        """
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler_a, MaruHandler(config) as handler_b:
            # Handler A stores data
            data = b"test read-only mapping"
            info = MemoryInfo(view=memoryview(data))
            assert handler_a.store(key="3000", info=info) is True

            # Handler B retrieves via read-only mapping of handler A's region
            result = handler_b.retrieve(key="3000")
            assert result is not None
            # With real CXL shared memory, data should match
            assert bytes(result.view[: len(data)]) == data


class TestMaruHandlerFailureScenarios:
    """Test failure and edge-case scenarios for MaruHandler."""

    def test_retrieve_nonexistent_key(self, server_thread, server_port):
        """Retrieve a key that was never stored returns None."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            result = handler.retrieve(key="99999")
            assert result is None

    def test_delete_nonexistent_key(self, server_thread, server_port):
        """Delete a key that was never stored returns False."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            result = handler.delete(key="99999")
            assert result is False

    def test_exists_nonexistent_key(self, server_thread, server_port):
        """Exists for a key that was never stored returns False."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            result = handler.exists(key="99999")
            assert result is False

    def test_double_delete(self, server_thread, server_port):
        """Store a key, delete it (True), delete again (False)."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            # Store a key
            handler.store(key="1", info=MemoryInfo(view=memoryview(b"data")))

            # First delete succeeds
            assert handler.delete(key="1") is True

            # Second delete fails (key already gone)
            assert handler.delete(key="1") is False

    def test_batch_retrieve_partial(self, server_thread, server_port):
        """Store keys 1,3 but batch_retrieve [1,2,3] returns [not None, None, not None]."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
        )

        with MaruHandler(config) as handler:
            # Store only keys 1 and 3
            data1 = b"data1"
            data3 = b"data3"
            handler.store(key="1", info=MemoryInfo(view=memoryview(data1)))
            handler.store(key="3", info=MemoryInfo(view=memoryview(data3)))

            # Batch retrieve keys 1, 2, 3
            results = handler.batch_retrieve(keys=["1", "2", "3"])
            assert len(results) == 3

            # Key 1 exists
            assert results[0] is not None
            assert bytes(results[0].view[: len(data1)]) == data1

            # Key 2 does not exist
            assert results[1] is None

            # Key 3 exists
            assert results[2] is not None
            assert bytes(results[2].view[: len(data3)]) == data3

    def test_store_after_close(self, server_thread, server_port):
        """Connect handler, close it, then try to store raises RuntimeError."""
        config = MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=4096,
            chunk_size_bytes=1024,
            auto_connect=False,
        )

        handler = MaruHandler(config)
        handler.connect()
        handler.close()

        with pytest.raises(RuntimeError):
            handler.store(key="1", info=MemoryInfo(view=memoryview(b"data")))
