# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for RpcAsyncClient to achieve 100% coverage."""

import asyncio
import threading
from unittest.mock import MagicMock, patch

import pytest

from maru_common import (
    BatchExistsKVResponse,
    BatchLookupKVResponse,
    BatchRegisterKVResponse,
    LookupKVResponse,
    MessageType,
    RequestAllocResponse,
)
from maru_handler.rpc_async_client import RpcAsyncClient


class TestRpcAsyncClientEdgeCases:
    """Test edge cases for RpcAsyncClient to achieve 100% coverage."""

    def test_close_cleanup_exception(self):
        """Test close() cleanup exception → caught and continues."""
        client = RpcAsyncClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)

        # Mock the loop and thread without actually connecting
        client._loop = MagicMock()
        client._loop.is_running.return_value = True
        client._loop_thread = MagicMock()
        client._loop_thread.is_alive.return_value = False
        client._running = True

        # Mock run_coroutine_threadsafe to raise exception
        with patch("asyncio.run_coroutine_threadsafe") as mock_run:
            mock_future = MagicMock()
            mock_future.result.side_effect = Exception("cleanup error")
            mock_run.return_value = mock_future

            # Should not raise
            client.close()

            # Close the unawaited coroutine passed to the mocked function
            if mock_run.call_args:
                coro = mock_run.call_args[0][0]
                coro.close()

    def test_async_cleanup_listener_cancel_raises(self):
        """Test _async_cleanup listener cancel raises CancelledError → caught."""

        async def test_coro():
            client = RpcAsyncClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)
            client._loop = asyncio.get_event_loop()
            client._running = True

            # Create a listener task
            async def simple_listener():
                await asyncio.sleep(10)

            client._listener_task = asyncio.ensure_future(simple_listener())
            client._pending = {}
            client._socket = MagicMock()

            # Should catch CancelledError
            await client._async_cleanup()

        asyncio.run(test_coro())

    def test_async_cleanup_pending_futures(self):
        """Test _async_cleanup pending futures → all failed with RuntimeError."""

        async def test_coro():
            client = RpcAsyncClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)
            client._loop = asyncio.get_event_loop()
            client._running = True
            client._listener_task = None
            client._socket = MagicMock()

            # Create pending futures
            fut1 = client._loop.create_future()
            fut2 = client._loop.create_future()
            client._pending = {1: fut1, 2: fut2}

            await client._async_cleanup()

            # Both futures should be set with exception
            assert fut1.done()
            assert fut2.done()
            with pytest.raises(RuntimeError, match="Client shutting down"):
                fut1.result()
            with pytest.raises(RuntimeError, match="Client shutting down"):
                fut2.result()

        asyncio.run(test_coro())

    def test_response_listener_no_pending_future_for_seq(self):
        """Test _response_listener no pending future for seq → warning logged."""

        async def test_coro():
            import logging

            from maru_common import Serializer
            from maru_common.protocol import MessageType

            client = RpcAsyncClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)
            client._loop = asyncio.get_event_loop()
            client._running = True
            client._serializer = Serializer()
            client._pending = {}

            # Mock socket to return a response
            serializer = Serializer()
            response_data = serializer.encode(
                MessageType.HEARTBEAT, {"success": True}, seq=999
            )

            async def mock_recv():
                client._running = False  # Stop after one iteration
                return [b"", response_data]

            mock_socket = MagicMock()
            mock_socket.recv_multipart = mock_recv
            client._socket = mock_socket

            with patch.object(
                logging.getLogger("maru_handler.rpc_async_client"), "warning"
            ) as mock_warning:
                await client._response_listener()

            # Should log warning about missing future
            assert mock_warning.called
            assert "No pending future for seq=%d" in mock_warning.call_args[0][0]

        asyncio.run(test_coro())

    def test_response_listener_zmq_error(self):
        """Test _response_listener ZMQ error → logged and breaks."""
        import logging

        import zmq

        async def test_coro():
            client = RpcAsyncClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)
            client._loop = asyncio.get_event_loop()
            client._running = True

            # Mock socket to raise ZMQError
            async def mock_recv():
                raise zmq.ZMQError()

            mock_socket = MagicMock()
            mock_socket.recv_multipart = mock_recv
            client._socket = mock_socket

            with patch.object(
                logging.getLogger("maru_handler.rpc_async_client"), "error"
            ) as mock_error:
                await client._response_listener()

            assert mock_error.called
            assert "ZMQ error" in mock_error.call_args[0][0]

        asyncio.run(test_coro())

    def test_response_listener_general_exception(self):
        """Test _response_listener general exception → logged and continues."""
        import logging

        async def test_coro():
            client = RpcAsyncClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)
            client._loop = asyncio.get_event_loop()
            client._running = True

            # Mock socket to raise a general exception, then stop
            call_count = [0]

            async def mock_recv():
                call_count[0] += 1
                if call_count[0] == 1:
                    raise ValueError("test error")
                else:
                    # Second call stops the loop
                    client._running = False
                    raise asyncio.CancelledError()

            mock_socket = MagicMock()
            mock_socket.recv_multipart = mock_recv
            client._socket = mock_socket

            with patch.object(
                logging.getLogger("maru_handler.rpc_async_client"), "error"
            ) as mock_error:
                await client._response_listener()

            assert mock_error.called
            assert "Response listener error" in mock_error.call_args[0][0]

        asyncio.run(test_coro())

    def test_send_async_cancelled(self):
        """Test _send_async cancelled → pending entry removed and raises."""

        async def test_coro():
            from maru_common import Serializer
            from maru_common.protocol import MessageType

            client = RpcAsyncClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)
            client._loop = asyncio.get_event_loop()
            client._running = True
            client._serializer = Serializer()
            client._inflight_sem = asyncio.Semaphore(64)
            client._pending = {}

            # Mock socket to hang so we can cancel
            async def mock_send(x):
                await asyncio.sleep(10)

            mock_socket = MagicMock()
            mock_socket.send_multipart = mock_send
            client._socket = mock_socket

            # Start _send_async and cancel it
            task = asyncio.ensure_future(client._send_async(MessageType.HEARTBEAT, {}))
            await asyncio.sleep(0.01)  # Let it start
            task.cancel()

            with pytest.raises(asyncio.CancelledError):
                await task

            # Pending should be empty (cleaned up)
            assert len(client._pending) == 0

        asyncio.run(test_coro())

    def test_send_request_not_running(self):
        """Test _send_request not running → RuntimeError."""
        client = RpcAsyncClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)
        # Don't call connect() so _running is False

        from maru_common.protocol import MessageType

        with pytest.raises(RuntimeError, match="Client not connected"):
            client._send_request(MessageType.HEARTBEAT, {})

    def test_send_request_timeout(self):
        """Test _send_request timeout → returns error dict."""
        import logging

        from maru_common.protocol import MessageType

        client = RpcAsyncClient(server_url="tcp://127.0.0.1:9999", timeout_ms=50)
        client._running = True
        client._loop = MagicMock()

        # Mock run_coroutine_threadsafe to return a future that times out
        mock_future = MagicMock()
        mock_future.result.side_effect = TimeoutError()

        with patch(
            "asyncio.run_coroutine_threadsafe", return_value=mock_future
        ) as mock_run:
            with patch.object(
                logging.getLogger("maru_handler.rpc_async_client"), "error"
            ) as mock_error:
                result = client._send_request(MessageType.HEARTBEAT, {})

            # Close the unawaited coroutine to suppress warning
            if mock_run.call_args:
                coro = mock_run.call_args[0][0]
                coro.close()

        assert result["error"] == "timeout"
        assert result["success"] is False
        assert mock_error.called
        assert "Timeout waiting for response" in mock_error.call_args[0][0]

    def test_send_request_nonblocking_not_running(self):
        """Test _send_request_nonblocking not running → RuntimeError."""
        client = RpcAsyncClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)
        # Don't call connect() so _running is False

        from maru_common.protocol import MessageType

        with pytest.raises(RuntimeError, match="Client not connected"):
            client._send_request_nonblocking(MessageType.HEARTBEAT, {})

    def test_send_request_nonblocking_success_path(self):
        """Test _send_request_nonblocking success path (L285)."""
        from maru_common.protocol import MessageType

        client = RpcAsyncClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)
        client._running = True
        client._loop = MagicMock()

        mock_future = MagicMock()
        with patch(
            "asyncio.run_coroutine_threadsafe", return_value=mock_future
        ) as mock_run:
            result = client._send_request_nonblocking(MessageType.HEARTBEAT, {})

        assert result is mock_future

        # Close the unawaited coroutine to suppress warning
        if mock_run.call_args:
            coro = mock_run.call_args[0][0]
            coro.close()

    def test_parse_request_alloc_failure_response(self):
        """Test _parse_request_alloc with failure response."""
        response = {"success": False, "error": "no memory"}
        result = RpcAsyncClient._parse_request_alloc(response)

        assert result.success is False
        assert result.error == "no memory"
        assert result.handle is None


class TestRpcAsyncClientApiMethods:
    """Test all blocking API methods of RpcAsyncClient by mocking _send_request."""

    def _make_client(self):
        """Create a client with mocked _send_request."""
        client = RpcAsyncClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)
        client._send_request = MagicMock()
        return client

    # =========================================================================
    # Blocking API Methods (mock _send_request, verify call args and parsing)
    # =========================================================================

    def test_request_alloc(self):
        """Test request_alloc calls _send_request and uses _parse_request_alloc."""
        client = self._make_client()
        handle_dict = {"region_id": 1, "offset": 0, "length": 4096, "auth_token": 0}
        client._send_request.return_value = {"success": True, "handle": handle_dict}

        result = client.request_alloc("instance-1", 4096)

        client._send_request.assert_called_once_with(
            MessageType.REQUEST_ALLOC,
            {"instance_id": "instance-1", "size": 4096, "pool_path": ""},
        )
        assert result.success is True
        assert result.handle is not None
        assert result.handle.region_id == 1

    def test_return_alloc(self):
        """Test return_alloc calls _send_request and returns success."""
        client = self._make_client()
        client._send_request.return_value = {"success": True}

        result = client.return_alloc("instance-1", 1)

        client._send_request.assert_called_once_with(
            MessageType.RETURN_ALLOC, {"instance_id": "instance-1", "region_id": 1}
        )
        assert result is True

    def test_register_kv(self):
        """Test register_kv calls _send_request and returns is_new."""
        client = self._make_client()
        client._send_request.return_value = {"is_new": True}

        result = client.register_kv(key="100", region_id=1, kv_offset=0, kv_length=64)

        client._send_request.assert_called_once_with(
            MessageType.REGISTER_KV,
            {"key": "100", "region_id": 1, "kv_offset": 0, "kv_length": 64},
        )
        assert result is True

    def test_lookup_kv(self):
        """Test lookup_kv calls _send_request and uses _parse_lookup_kv."""
        client = self._make_client()
        handle_dict = {"region_id": 1, "offset": 0, "length": 4096, "auth_token": 0}
        client._send_request.return_value = {
            "found": True,
            "handle": handle_dict,
            "kv_offset": 128,
            "kv_length": 64,
        }

        result = client.lookup_kv("100")

        client._send_request.assert_called_once_with(
            MessageType.LOOKUP_KV, {"key": "100"}
        )
        assert result.found is True
        assert result.handle is not None
        assert result.kv_offset == 128
        assert result.kv_length == 64

    def test_exists_kv(self):
        """Test exists_kv calls _send_request and returns exists."""
        client = self._make_client()
        client._send_request.return_value = {"exists": True}

        result = client.exists_kv("100")

        client._send_request.assert_called_once_with(
            MessageType.EXISTS_KV, {"key": "100"}
        )
        assert result is True

    def test_delete_kv(self):
        """Test delete_kv calls _send_request and returns success."""
        client = self._make_client()
        client._send_request.return_value = {"success": True}

        result = client.delete_kv("100")

        client._send_request.assert_called_once_with(
            MessageType.DELETE_KV, {"key": "100"}
        )
        assert result is True

    def test_batch_register_kv(self):
        """Test batch_register_kv calls _send_request and returns BatchRegisterKVResponse."""
        client = self._make_client()
        client._send_request.return_value = {
            "success": True,
            "results": [True, False, True],
        }

        entries = [("100", 1, 0, 64), ("101", 1, 64, 64), ("102", 1, 128, 64)]
        result = client.batch_register_kv(entries)

        expected_entries = [
            {"key": "100", "region_id": 1, "kv_offset": 0, "kv_length": 64},
            {"key": "101", "region_id": 1, "kv_offset": 64, "kv_length": 64},
            {"key": "102", "region_id": 1, "kv_offset": 128, "kv_length": 64},
        ]
        client._send_request.assert_called_once_with(
            MessageType.BATCH_REGISTER_KV, {"entries": expected_entries}
        )
        assert result.success is True
        assert result.results == [True, False, True]

    def test_batch_lookup_kv(self):
        """Test batch_lookup_kv calls _send_request and uses _parse_batch_lookup_kv."""
        client = self._make_client()
        handle_dict = {"region_id": 1, "offset": 0, "length": 4096, "auth_token": 0}
        client._send_request.return_value = {
            "entries": [
                {"found": True, "handle": handle_dict, "kv_offset": 0, "kv_length": 64},
                {"found": False, "kv_offset": 0, "kv_length": 0},
            ]
        }

        result = client.batch_lookup_kv(["100", "101"])

        client._send_request.assert_called_once_with(
            MessageType.BATCH_LOOKUP_KV, {"keys": ["100", "101"]}
        )
        assert len(result.entries) == 2
        assert result.entries[0].found is True
        assert result.entries[0].handle is not None
        assert result.entries[1].found is False

    def test_batch_exists_kv(self):
        """Test batch_exists_kv calls _send_request and returns BatchExistsKVResponse."""
        client = self._make_client()
        client._send_request.return_value = {"results": [True, False, True]}

        result = client.batch_exists_kv(["100", "101", "102"])

        client._send_request.assert_called_once_with(
            MessageType.BATCH_EXISTS_KV, {"keys": ["100", "101", "102"]}
        )
        assert result.results == [True, False, True]

    def test_get_stats(self):
        """Test get_stats calls _send_request and returns GetStatsResponse."""
        client = self._make_client()
        client._send_request.return_value = {
            "kv_manager": {"total_entries": 100, "total_size": 4096},
            "allocation_manager": {
                "num_allocations": 5,
                "total_allocated": 20480,
                "active_clients": 3,
            },
        }

        result = client.get_stats()

        client._send_request.assert_called_once_with(MessageType.GET_STATS, {})
        assert result.kv_manager.total_entries == 100
        assert result.kv_manager.total_size == 4096
        assert result.allocation_manager.num_allocations == 5
        assert result.allocation_manager.total_allocated == 20480
        assert result.allocation_manager.active_clients == 3

    def test_heartbeat(self):
        """Test heartbeat calls _send_request and returns True if no error."""
        client = self._make_client()
        client._send_request.return_value = {"status": "ok"}

        result = client.heartbeat()

        client._send_request.assert_called_once_with(MessageType.HEARTBEAT, {})
        assert result is True

    def test_heartbeat_with_error(self):
        """Test heartbeat returns False if response contains error."""
        client = self._make_client()
        client._send_request.return_value = {"error": "server busy"}

        result = client.heartbeat()

        assert result is False

    # =========================================================================
    # Static Parse Methods (test directly)
    # =========================================================================

    def test_parse_request_alloc_success(self):
        """Test _parse_request_alloc with success response."""
        handle_dict = {"region_id": 1, "offset": 0, "length": 4096, "auth_token": 0}
        response = {"success": True, "handle": handle_dict}

        result = RpcAsyncClient._parse_request_alloc(response)

        assert result.success is True
        assert result.handle is not None
        assert result.handle.region_id == 1
        assert result.handle.offset == 0
        assert result.handle.length == 4096

    def test_parse_request_alloc_failure(self):
        """Test _parse_request_alloc with failure response."""
        response = {"success": False, "error": "no memory"}

        result = RpcAsyncClient._parse_request_alloc(response)

        assert result.success is False
        assert result.error == "no memory"
        assert result.handle is None

    def test_parse_lookup_kv_found(self):
        """Test _parse_lookup_kv with found entry."""
        handle_dict = {"region_id": 1, "offset": 0, "length": 4096, "auth_token": 0}
        response = {
            "found": True,
            "handle": handle_dict,
            "kv_offset": 128,
            "kv_length": 64,
        }

        result = RpcAsyncClient._parse_lookup_kv(response)

        assert result.found is True
        assert result.handle is not None
        assert result.handle.region_id == 1
        assert result.kv_offset == 128
        assert result.kv_length == 64

    def test_parse_lookup_kv_not_found(self):
        """Test _parse_lookup_kv with not found entry."""
        response = {"found": False}

        result = RpcAsyncClient._parse_lookup_kv(response)

        assert result.found is False
        assert result.handle is None

    def test_parse_batch_lookup_kv_mixed(self):
        """Test _parse_batch_lookup_kv with mixed found/not-found entries."""
        handle_dict1 = {"region_id": 1, "offset": 0, "length": 4096, "auth_token": 0}
        handle_dict2 = {"region_id": 2, "offset": 4096, "length": 4096, "auth_token": 1}
        response = {
            "entries": [
                {
                    "found": True,
                    "handle": handle_dict1,
                    "kv_offset": 0,
                    "kv_length": 64,
                },
                {"found": False, "kv_offset": 0, "kv_length": 0},
                {
                    "found": True,
                    "handle": handle_dict2,
                    "kv_offset": 128,
                    "kv_length": 128,
                },
            ]
        }

        result = RpcAsyncClient._parse_batch_lookup_kv(response)

        assert len(result.entries) == 3
        assert result.entries[0].found is True
        assert result.entries[0].handle.region_id == 1
        assert result.entries[0].kv_offset == 0
        assert result.entries[1].found is False
        assert result.entries[1].handle is None
        assert result.entries[2].found is True
        assert result.entries[2].handle.region_id == 2
        assert result.entries[2].kv_offset == 128

    def test_parse_batch_lookup_kv_empty(self):
        """Test _parse_batch_lookup_kv with empty entries."""
        response = {"entries": []}

        result = RpcAsyncClient._parse_batch_lookup_kv(response)

        assert len(result.entries) == 0

    def test_parse_list_allocations_success(self):
        """Test _parse_list_allocations with success response (lines 329-337)."""
        response = {
            "success": True,
            "allocations": [
                {"region_id": 1, "offset": 0, "length": 4096, "auth_token": 0},
                {"region_id": 2, "offset": 0, "length": 8192, "auth_token": 0},
            ],
        }

        result = RpcAsyncClient._parse_list_allocations(response)

        assert result.success is True
        assert len(result.allocations) == 2
        assert result.allocations[0].region_id == 1
        assert result.allocations[1].region_id == 2

    def test_parse_list_allocations_failure(self):
        """Test _parse_list_allocations with failure response."""
        response = {"success": False, "error": "server error"}

        result = RpcAsyncClient._parse_list_allocations(response)

        assert result.success is False
        assert result.error == "server error"

    def test_list_allocations_blocking(self):
        """Test list_allocations blocking path (line 377)."""
        client = self._make_client()
        client._send_request.return_value = {
            "success": True,
            "allocations": [
                {"region_id": 1, "offset": 0, "length": 4096, "auth_token": 0},
            ],
        }

        result = client.list_allocations(exclude_instance_id="inst1")

        client._send_request.assert_called_once_with(
            MessageType.LIST_ALLOCATIONS, {"exclude_instance_id": "inst1"}
        )
        assert result.success is True
        assert len(result.allocations) == 1

    def test_list_allocations_no_exclude(self):
        """Test list_allocations without exclude sends empty data."""
        client = self._make_client()
        client._send_request.return_value = {
            "success": True,
            "allocations": [],
        }

        result = client.list_allocations()

        client._send_request.assert_called_once_with(MessageType.LIST_ALLOCATIONS, {})
        assert result.success is True


class TestRpcAsyncClientLifecycle:
    """Test connection lifecycle methods for 100% coverage."""

    def test_connect_lifecycle(self):
        """Test connect() creates loop, thread, and initializes async."""

        client = RpcAsyncClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)

        mock_loop = MagicMock()
        mock_loop.is_running.return_value = True
        mock_thread = MagicMock()
        mock_future = MagicMock()

        with patch("asyncio.new_event_loop", return_value=mock_loop) as mock_new_loop:
            with patch(
                "threading.Thread", return_value=mock_thread
            ) as mock_thread_class:
                with patch(
                    "asyncio.run_coroutine_threadsafe", return_value=mock_future
                ) as mock_run:
                    client.connect()

        # Verify loop created
        mock_new_loop.assert_called_once()
        assert client._loop is mock_loop
        assert client._running is True

        # Verify thread created and started
        mock_thread_class.assert_called_once()
        call_kwargs = mock_thread_class.call_args[1]
        assert call_kwargs["target"] == client._run_event_loop
        assert call_kwargs["name"] == "rpc-asyncio"
        assert call_kwargs["daemon"] is True
        mock_thread.start.assert_called_once()

        # Verify async init called
        mock_run.assert_called_once()
        assert client._loop_thread is mock_thread

        # Close the unawaited coroutine
        if mock_run.call_args:
            coro = mock_run.call_args[0][0]
            coro.close()

    def test_close_full_lifecycle(self):
        """Test close() full path with thread join and context.term()."""
        client = RpcAsyncClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)

        # Set up client state
        mock_loop = MagicMock()
        mock_loop.is_running.return_value = True
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = True
        mock_context = MagicMock()

        client._loop = mock_loop
        client._loop_thread = mock_thread
        client._context = mock_context
        client._running = True

        mock_future = MagicMock()
        with patch(
            "asyncio.run_coroutine_threadsafe", return_value=mock_future
        ) as mock_run:
            client.close()

        # Verify cleanup called
        assert client._running is False
        mock_run.assert_called_once()
        mock_loop.call_soon_threadsafe.assert_called_once()

        # Verify thread join called (line 130)
        mock_thread.join.assert_called_once_with(timeout=2.0)

        # Verify context.term() called (lines 134-135)
        mock_context.term.assert_called_once()
        assert client._context is None

        # Verify loop closed
        mock_loop.close.assert_called_once()
        assert client._loop is None

        # Close the unawaited coroutine
        if mock_run.call_args:
            coro = mock_run.call_args[0][0]
            coro.close()

    def test_run_event_loop(self):
        """Test _run_event_loop sets loop and runs forever."""
        client = RpcAsyncClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)
        mock_loop = MagicMock()
        client._loop = mock_loop

        with patch("asyncio.set_event_loop") as mock_set_loop:
            client._run_event_loop()

        mock_set_loop.assert_called_once_with(mock_loop)
        mock_loop.run_forever.assert_called_once()

    def test_async_init(self):
        """Test _async_init creates context, socket, semaphore, and listener."""

        async def test_coro():
            import zmq.asyncio

            client = RpcAsyncClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)

            mock_context = MagicMock()
            mock_socket = MagicMock()
            mock_context.socket.return_value = mock_socket
            mock_task = MagicMock()

            with patch("zmq.asyncio.Context", return_value=mock_context):
                with patch(
                    "asyncio.ensure_future", return_value=mock_task
                ) as mock_ensure_future:
                    await client._async_init()

            # Verify context and socket created
            assert client._context is mock_context
            mock_context.socket.assert_called_once_with(zmq.DEALER)

            # Verify socket options set (LINGER and IDENTITY)
            assert mock_socket.setsockopt.call_count == 2
            mock_socket.connect.assert_called_once_with("tcp://127.0.0.1:9999")

            # Verify semaphore created
            assert client._inflight_sem is not None
            assert isinstance(client._inflight_sem, asyncio.Semaphore)

            # Verify listener task created
            mock_ensure_future.assert_called_once()
            assert client._listener_task is mock_task

            # Close the unawaited coroutine to suppress RuntimeWarning
            coro = mock_ensure_future.call_args[0][0]
            coro.close()

        asyncio.run(test_coro())

    def test_context_manager(self):
        """Test context manager calls connect on entry and close on exit."""
        client = RpcAsyncClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)

        with patch.object(client, "connect") as mock_connect:
            with patch.object(client, "close") as mock_close:
                with client as ctx_client:
                    assert ctx_client is client
                    mock_connect.assert_called_once()
                    mock_close.assert_not_called()

                # Verify close called on exit
                mock_close.assert_called_once()


class TestRpcAsyncClientAsyncMethods:
    """Test all *_async methods for 100% coverage."""

    def _setup_client_with_loop(self):
        """Helper to create client with running event loop."""
        loop = asyncio.new_event_loop()
        t = threading.Thread(target=loop.run_forever, daemon=True)
        t.start()

        client = RpcAsyncClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)
        client._loop = loop
        client._running = True

        return client, loop, t

    def _teardown_loop(self, loop, thread):
        """Helper to stop event loop and join thread."""
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=2.0)
        loop.close()

    def test_request_alloc_async(self):
        """Test request_alloc_async returns Future[RequestAllocResponse]."""
        client, loop, thread = self._setup_client_with_loop()

        async def mock_send_async(msg_type, data):
            return {
                "success": True,
                "handle": {
                    "region_id": 1,
                    "offset": 0,
                    "length": 4096,
                    "auth_token": 0,
                },
            }

        client._send_async = mock_send_async

        future = client.request_alloc_async("inst-1", 4096)
        result = future.result(timeout=2.0)

        assert isinstance(result, RequestAllocResponse)
        assert result.success is True
        assert result.handle is not None
        assert result.handle.region_id == 1

        self._teardown_loop(loop, thread)

    def test_return_alloc_async(self):
        """Test return_alloc_async returns Future[bool]."""
        client, loop, thread = self._setup_client_with_loop()

        async def mock_send_async(msg_type, data):
            return {"success": True}

        client._send_async = mock_send_async

        future = client.return_alloc_async("inst-1", 1)
        result = future.result(timeout=2.0)

        assert result is True

        self._teardown_loop(loop, thread)

    def test_list_allocations_async(self):
        """Test list_allocations_async returns Future[ListAllocationsResponse] (lines 501-508)."""
        from maru_common import ListAllocationsResponse

        client, loop, thread = self._setup_client_with_loop()

        async def mock_send_async(msg_type, data):
            return {
                "success": True,
                "allocations": [
                    {"region_id": 1, "offset": 0, "length": 4096, "auth_token": 0},
                ],
            }

        client._send_async = mock_send_async

        future = client.list_allocations_async(exclude_instance_id="inst1")
        result = future.result(timeout=2.0)

        assert isinstance(result, ListAllocationsResponse)
        assert result.success is True
        assert len(result.allocations) == 1
        assert result.allocations[0].region_id == 1

        self._teardown_loop(loop, thread)

    def test_list_allocations_async_no_exclude(self):
        """Test list_allocations_async without exclude_instance_id."""
        from maru_common import ListAllocationsResponse

        client, loop, thread = self._setup_client_with_loop()

        async def mock_send_async(msg_type, data):
            return {"success": True, "allocations": []}

        client._send_async = mock_send_async

        future = client.list_allocations_async()
        result = future.result(timeout=2.0)

        assert isinstance(result, ListAllocationsResponse)
        assert result.success is True
        assert len(result.allocations) == 0

        self._teardown_loop(loop, thread)

    def test_register_kv_async(self):
        """Test register_kv_async returns Future[bool]."""
        client, loop, thread = self._setup_client_with_loop()

        async def mock_send_async(msg_type, data):
            return {"is_new": True}

        client._send_async = mock_send_async

        future = client.register_kv_async("100", 1, 0, 64)
        result = future.result(timeout=2.0)

        assert result is True

        self._teardown_loop(loop, thread)

    def test_lookup_kv_async(self):
        """Test lookup_kv_async returns Future[LookupKVResponse]."""
        client, loop, thread = self._setup_client_with_loop()

        async def mock_send_async(msg_type, data):
            return {
                "found": True,
                "handle": {
                    "region_id": 1,
                    "offset": 0,
                    "length": 4096,
                    "auth_token": 0,
                },
                "kv_offset": 128,
                "kv_length": 64,
            }

        client._send_async = mock_send_async

        future = client.lookup_kv_async(100)
        result = future.result(timeout=2.0)

        assert isinstance(result, LookupKVResponse)
        assert result.found is True
        assert result.handle is not None
        assert result.kv_offset == 128

        self._teardown_loop(loop, thread)

    def test_exists_kv_async(self):
        """Test exists_kv_async returns Future[bool]."""
        client, loop, thread = self._setup_client_with_loop()

        async def mock_send_async(msg_type, data):
            return {"exists": True}

        client._send_async = mock_send_async

        future = client.exists_kv_async(100)
        result = future.result(timeout=2.0)

        assert result is True

        self._teardown_loop(loop, thread)

    def test_delete_kv_async(self):
        """Test delete_kv_async returns Future[bool]."""
        client, loop, thread = self._setup_client_with_loop()

        async def mock_send_async(msg_type, data):
            return {"success": True}

        client._send_async = mock_send_async

        future = client.delete_kv_async(100)
        result = future.result(timeout=2.0)

        assert result is True

        self._teardown_loop(loop, thread)

    def test_batch_register_kv_async(self):
        """Test batch_register_kv_async returns Future[BatchRegisterKVResponse]."""
        client, loop, thread = self._setup_client_with_loop()

        async def mock_send_async(msg_type, data):
            return {"success": True, "results": [True, False, True]}

        client._send_async = mock_send_async

        entries = [("100", 1, 0, 64), ("101", 1, 64, 64), ("102", 1, 128, 64)]
        future = client.batch_register_kv_async(entries)
        result = future.result(timeout=2.0)

        assert isinstance(result, BatchRegisterKVResponse)
        assert result.success is True
        assert result.results == [True, False, True]

        self._teardown_loop(loop, thread)

    def test_batch_lookup_kv_async(self):
        """Test batch_lookup_kv_async returns Future[BatchLookupKVResponse]."""
        client, loop, thread = self._setup_client_with_loop()

        async def mock_send_async(msg_type, data):
            return {
                "entries": [
                    {
                        "found": True,
                        "handle": {
                            "region_id": 1,
                            "offset": 0,
                            "length": 4096,
                            "auth_token": 0,
                        },
                        "kv_offset": 0,
                        "kv_length": 64,
                    },
                    {"found": False, "kv_offset": 0, "kv_length": 0},
                ]
            }

        client._send_async = mock_send_async

        future = client.batch_lookup_kv_async([100, 101])
        result = future.result(timeout=2.0)

        assert isinstance(result, BatchLookupKVResponse)
        assert len(result.entries) == 2
        assert result.entries[0].found is True
        assert result.entries[1].found is False

        self._teardown_loop(loop, thread)

    def test_batch_exists_kv_async(self):
        """Test batch_exists_kv_async returns Future[BatchExistsKVResponse]."""
        client, loop, thread = self._setup_client_with_loop()

        async def mock_send_async(msg_type, data):
            return {"results": [True, False, True]}

        client._send_async = mock_send_async

        future = client.batch_exists_kv_async([100, 101, 102])
        result = future.result(timeout=2.0)

        assert isinstance(result, BatchExistsKVResponse)
        assert result.results == [True, False, True]

        self._teardown_loop(loop, thread)

    def test_heartbeat_async(self):
        """Test heartbeat_async returns Future[bool]."""
        client, loop, thread = self._setup_client_with_loop()

        async def mock_send_async(msg_type, data):
            return {"status": "ok"}

        client._send_async = mock_send_async

        future = client.heartbeat_async()
        result = future.result(timeout=2.0)

        assert result is True

        self._teardown_loop(loop, thread)

    def test_heartbeat_async_with_error(self):
        """Test heartbeat_async returns False if response contains error."""
        client, loop, thread = self._setup_client_with_loop()

        async def mock_send_async(msg_type, data):
            return {"error": "server busy"}

        client._send_async = mock_send_async

        future = client.heartbeat_async()
        result = future.result(timeout=2.0)

        assert result is False

        self._teardown_loop(loop, thread)


class TestRpcAsyncClientFullAsyncFlow:
    """Test full async flow for lines 209 and 250 (100% coverage)."""

    def test_full_async_send_receive_flow(self):
        """Test complete async send/receive to cover lines 209 and 250."""

        async def test_coro():
            from maru_common import Serializer
            from maru_common.protocol import MessageType

            client = RpcAsyncClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)
            client._loop = asyncio.get_event_loop()
            client._running = True
            client._serializer = Serializer()
            client._inflight_sem = asyncio.Semaphore(64)
            client._pending = {}

            # Mock socket that simulates send/receive
            sent_messages = []

            async def mock_send_multipart(frames):
                # Store sent message for later verification
                sent_messages.append(frames)

            async def mock_recv_multipart():
                # Wait a bit to simulate network delay
                await asyncio.sleep(0.01)

                # Get the sent message to extract sequence number
                if sent_messages:
                    sent_data = sent_messages[0][-1]
                    header, _ = client._serializer.decode(sent_data)
                    seq = header.sequence

                    # Create a response with the same sequence number
                    response_data = client._serializer.encode(
                        MessageType.HEARTBEAT, {"success": True}, seq=seq
                    )
                    return [b"", response_data]

                # If no messages, wait forever
                await asyncio.sleep(10)
                return [b"", b""]

            mock_socket = MagicMock()
            mock_socket.send_multipart = mock_send_multipart
            mock_socket.recv_multipart = mock_recv_multipart
            client._socket = mock_socket

            # Start the response listener
            listener_task = asyncio.ensure_future(client._response_listener())

            # Send a request - this covers line 250 (return await fut)
            response = await client._send_async(MessageType.HEARTBEAT, {})

            # Verify response received (line 209: fut.set_result(payload) was called)
            assert response["success"] is True
            assert len(sent_messages) == 1

            # Stop listener
            client._running = False
            listener_task.cancel()
            try:
                await listener_task
            except asyncio.CancelledError:
                pass

        asyncio.run(test_coro())
