# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for RpcClient to achieve 100% coverage."""

from unittest.mock import MagicMock, patch

import pytest
import zmq

from maru_common import (
    AllocationManagerStats,
    BatchExistsKVResponse,
    BatchLookupKVResponse,
    BatchRegisterKVResponse,
    GetStatsResponse,
    KVManagerStats,
    LookupKVResponse,
    MessageType,
    RequestAllocResponse,
)
from maru_handler.rpc_client import RpcClient
from maru_shm import MaruHandle


class TestRpcClientEdgeCases:
    """Test edge cases for RpcClient to achieve 100% coverage."""

    def test_send_request_socket_is_none(self):
        """Test _send_request() when self._socket is None → RuntimeError."""
        client = RpcClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)
        # Don't call connect() so socket is None

        with pytest.raises(RuntimeError, match="Client not connected"):
            client._send_request(0x01, {})


class TestRpcClientSendRequest:
    """Test _send_request poll/recv paths with mocked ZMQ."""

    def _make_client(self):
        """Create a RpcClient with mocked socket and serializer."""
        client = RpcClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)
        client._socket = MagicMock()
        client._context = MagicMock()
        client._serializer = MagicMock()
        client._serializer.encode.return_value = b"encoded"
        return client

    def test_poll_timeout_resets_socket(self):
        """When poll returns no events, _reset_socket is called and timeout dict returned."""
        client = self._make_client()
        original_socket = client._socket  # Save before _reset_socket replaces it

        with patch("zmq.Poller") as mock_poller:
            poller_inst = MagicMock()
            mock_poller.return_value = poller_inst
            # poll returns empty → timeout
            poller_inst.poll.return_value = []

            result = client._send_request(MessageType.HEARTBEAT, {})

        assert result == {"error": "timeout", "success": False}
        # Original socket was sent the encoded message before timeout
        original_socket.send.assert_called_once_with(b"encoded")

    def test_poll_timeout_reset_socket_fails(self):
        """When poll times out and _reset_socket also fails, still returns timeout dict."""
        client = self._make_client()
        # Make _reset_socket raise
        client._context.socket.side_effect = zmq.ZMQError("context terminated")

        with patch("zmq.Poller") as mock_poller:
            poller_inst = MagicMock()
            mock_poller.return_value = poller_inst
            poller_inst.poll.return_value = []

            result = client._send_request(MessageType.HEARTBEAT, {})

        assert result == {"error": "timeout", "success": False}

    def test_successful_poll_and_recv(self):
        """When poll shows data ready, recv(NOBLOCK) succeeds and payload is returned."""
        client = self._make_client()
        mock_socket = client._socket
        expected_payload = {"success": True, "handle": {"region_id": 1}}
        client._serializer.decode.return_value = (
            MessageType.REQUEST_ALLOC,
            expected_payload,
        )

        with patch("zmq.Poller") as mock_poller:
            poller_inst = MagicMock()
            mock_poller.return_value = poller_inst
            # poll returns our socket as ready
            poller_inst.poll.return_value = [(mock_socket, zmq.POLLIN)]

            mock_socket.recv.return_value = b"response_data"

            result = client._send_request(MessageType.REQUEST_ALLOC, {"size": 4096})

        assert result == expected_payload
        mock_socket.recv.assert_called_once_with(zmq.NOBLOCK)
        client._serializer.decode.assert_called_once_with(b"response_data")

    def test_zmq_error_on_send_resets_socket(self):
        """When socket.send raises ZMQError, _reset_socket is called."""
        client = self._make_client()
        client._socket.send.side_effect = zmq.ZMQError("connection refused")

        result = client._send_request(MessageType.HEARTBEAT, {})

        assert result == {"error": "timeout", "success": False}

    def test_zmq_error_on_recv_resets_socket(self):
        """When socket.recv raises ZMQError after successful poll, still returns timeout."""
        client = self._make_client()
        mock_socket = client._socket

        with patch("zmq.Poller") as mock_poller:
            poller_inst = MagicMock()
            mock_poller.return_value = poller_inst
            poller_inst.poll.return_value = [(mock_socket, zmq.POLLIN)]
            mock_socket.recv.side_effect = zmq.ZMQError("interrupted")

            result = client._send_request(MessageType.HEARTBEAT, {})

        assert result == {"error": "timeout", "success": False}

    def test_zmq_error_and_reset_socket_fails(self):
        """When ZMQError occurs and _reset_socket also fails, still returns timeout dict."""
        client = self._make_client()
        client._socket.send.side_effect = zmq.ZMQError("send failed")
        # Make _reset_socket raise too
        client._context.socket.side_effect = zmq.ZMQError("context terminated")

        result = client._send_request(MessageType.HEARTBEAT, {})

        assert result == {"error": "timeout", "success": False}


class TestRpcClientResetSocket:
    """Test _reset_socket directly."""

    def test_reset_socket_closes_and_reconnects(self):
        """_reset_socket closes existing socket and creates a new one."""
        client = RpcClient(server_url="tcp://127.0.0.1:9999", timeout_ms=200)
        old_socket = MagicMock()
        client._socket = old_socket
        client._context = MagicMock()
        new_socket = MagicMock()
        client._context.socket.return_value = new_socket

        client._reset_socket()

        old_socket.close.assert_called_once()
        client._context.socket.assert_called_once_with(zmq.REQ)
        new_socket.connect.assert_called_once_with("tcp://127.0.0.1:9999")
        assert client._socket is new_socket

    def test_reset_socket_when_socket_is_none(self):
        """_reset_socket skips close if socket is None."""
        client = RpcClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)
        client._socket = None
        client._context = MagicMock()
        new_socket = MagicMock()
        client._context.socket.return_value = new_socket

        client._reset_socket()

        client._context.socket.assert_called_once_with(zmq.REQ)
        assert client._socket is new_socket


class TestRpcClientConnectClose:
    """Test connect/close and context manager."""

    def test_close_without_connect(self):
        """close() when never connected doesn't raise."""
        client = RpcClient()
        client.close()  # Should not raise

    def test_context_manager(self):
        """Context manager calls connect/close."""
        with (
            patch.object(RpcClient, "connect") as mock_connect,
            patch.object(RpcClient, "close") as mock_close,
        ):
            with RpcClient() as client:
                mock_connect.assert_called_once()
                assert isinstance(client, RpcClient)
            mock_close.assert_called_once()

    def test_connect_creates_socket_and_connects(self):
        """connect() creates zmq.Context, socket, sets options, and connects."""
        client = RpcClient(server_url="tcp://127.0.0.1:9999", timeout_ms=200)

        with patch("zmq.Context") as mock_context:
            mock_context_inst = MagicMock()
            mock_context.return_value = mock_context_inst
            mock_socket = MagicMock()
            mock_context_inst.socket.return_value = mock_socket

            client.connect()

            # Verify zmq.Context() was called
            mock_context.assert_called_once()
            # Verify context.socket(zmq.REQ) was called
            mock_context_inst.socket.assert_called_once_with(zmq.REQ)
            # Verify socket options were set
            assert mock_socket.setsockopt.call_count == 3
            mock_socket.setsockopt.assert_any_call(zmq.RCVTIMEO, 200)
            mock_socket.setsockopt.assert_any_call(zmq.SNDTIMEO, 200)
            mock_socket.setsockopt.assert_any_call(zmq.LINGER, 0)
            # Verify socket.connect was called with server_url
            mock_socket.connect.assert_called_once_with("tcp://127.0.0.1:9999")
            # Verify client state was updated
            assert client._context is mock_context_inst
            assert client._socket is mock_socket

    def test_close_with_socket_and_context(self):
        """close() with both socket and context calls close() and term()."""
        client = RpcClient()
        mock_socket = MagicMock()
        mock_context = MagicMock()
        client._socket = mock_socket
        client._context = mock_context

        client.close()

        mock_socket.close.assert_called_once()
        mock_context.term.assert_called_once()

    def test_close_with_socket_only(self):
        """close() with socket but no context doesn't raise."""
        client = RpcClient()
        mock_socket = MagicMock()
        client._socket = mock_socket
        client._context = None

        client.close()  # Should not raise

        mock_socket.close.assert_called_once()

    def test_close_with_context_only(self):
        """close() with context but no socket doesn't raise."""
        client = RpcClient()
        mock_context = MagicMock()
        client._socket = None
        client._context = mock_context

        client.close()  # Should not raise

        mock_context.term.assert_called_once()


class TestRpcClientApiMethods:
    """Test all 11 API methods by mocking _send_request."""

    def _make_client_with_mock(self):
        """Create a client with _send_request mocked."""
        client = RpcClient(server_url="tcp://127.0.0.1:9999", timeout_ms=100)
        mock_send = MagicMock()
        client._send_request = mock_send
        return client, mock_send

    # =========================================================================
    # 1. request_alloc
    # =========================================================================

    def test_request_alloc_success(self):
        """request_alloc returns RequestAllocResponse with handle on success."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {
            "success": True,
            "handle": {"region_id": 1, "offset": 0, "length": 4096, "auth_token": 0},
        }

        result = client.request_alloc(instance_id="test_instance", size=4096)

        mock_send.assert_called_once_with(
            MessageType.REQUEST_ALLOC, {"instance_id": "test_instance", "size": 4096, "pool_id": 0xFFFFFFFF}
        )
        assert isinstance(result, RequestAllocResponse)
        assert result.success is True
        assert isinstance(result.handle, MaruHandle)
        assert result.handle.region_id == 1
        assert result.handle.offset == 0
        assert result.handle.length == 4096

    def test_request_alloc_failure(self):
        """request_alloc returns RequestAllocResponse with error on failure."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {"success": False, "error": "Out of memory"}

        result = client.request_alloc(instance_id="test_instance", size=999999)

        mock_send.assert_called_once_with(
            MessageType.REQUEST_ALLOC, {"instance_id": "test_instance", "size": 999999, "pool_id": 0xFFFFFFFF}
        )
        assert isinstance(result, RequestAllocResponse)
        assert result.success is False
        assert result.error == "Out of memory"
        assert result.handle is None

    def test_request_alloc_no_handle(self):
        """request_alloc handles success without handle data."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {"success": True, "handle": {}}

        result = client.request_alloc(instance_id="test_instance", size=1024)

        assert result.success is True
        assert result.handle is None

    # =========================================================================
    # 2. return_alloc
    # =========================================================================

    def test_return_alloc_success(self):
        """return_alloc returns True when server confirms success."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {"success": True}

        result = client.return_alloc(instance_id="test_instance", region_id=1)

        mock_send.assert_called_once_with(
            MessageType.RETURN_ALLOC, {"instance_id": "test_instance", "region_id": 1}
        )
        assert result is True

    def test_return_alloc_failure(self):
        """return_alloc returns False on failure."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {"success": False}

        result = client.return_alloc(instance_id="test_instance", region_id=999)

        assert result is False

    # =========================================================================
    # 3. register_kv
    # =========================================================================

    def test_register_kv_new_entry(self):
        """register_kv returns True when entry is newly registered."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {"is_new": True}

        result = client.register_kv(
            key="12345", region_id=1, kv_offset=100, kv_length=200
        )

        mock_send.assert_called_once_with(
            MessageType.REGISTER_KV,
            {"key": "12345", "region_id": 1, "kv_offset": 100, "kv_length": 200},
        )
        assert result is True

    def test_register_kv_existing_entry(self):
        """register_kv returns False when entry already exists."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {"is_new": False}

        result = client.register_kv(
            key="12345", region_id=1, kv_offset=100, kv_length=200
        )

        assert result is False

    # =========================================================================
    # 4. lookup_kv
    # =========================================================================

    def test_lookup_kv_found(self):
        """lookup_kv returns LookupKVResponse with handle when key is found."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {
            "found": True,
            "handle": {"region_id": 2, "offset": 128, "length": 512, "auth_token": 0},
            "kv_offset": 100,
            "kv_length": 200,
        }

        result = client.lookup_kv(key="54321")

        mock_send.assert_called_once_with(MessageType.LOOKUP_KV, {"key": "54321"})
        assert isinstance(result, LookupKVResponse)
        assert result.found is True
        assert isinstance(result.handle, MaruHandle)
        assert result.handle.region_id == 2
        assert result.kv_offset == 100
        assert result.kv_length == 200

    def test_lookup_kv_not_found(self):
        """lookup_kv returns LookupKVResponse with found=False when key not found."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {"found": False}

        result = client.lookup_kv(key="99999")

        mock_send.assert_called_once_with(MessageType.LOOKUP_KV, {"key": "99999"})
        assert isinstance(result, LookupKVResponse)
        assert result.found is False
        assert result.handle is None

    # =========================================================================
    # 5. exists_kv
    # =========================================================================

    def test_exists_kv_true(self):
        """exists_kv returns True when key exists."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {"exists": True}

        result = client.exists_kv(key="12345")

        mock_send.assert_called_once_with(MessageType.EXISTS_KV, {"key": "12345"})
        assert result is True

    def test_exists_kv_false(self):
        """exists_kv returns False when key doesn't exist."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {"exists": False}

        result = client.exists_kv(key="99999")

        assert result is False

    # =========================================================================
    # 6. delete_kv
    # =========================================================================

    def test_delete_kv_success(self):
        """delete_kv returns True when deletion is successful."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {"success": True}

        result = client.delete_kv(key="12345")

        mock_send.assert_called_once_with(MessageType.DELETE_KV, {"key": "12345"})
        assert result is True

    def test_delete_kv_failure(self):
        """delete_kv returns False on failure."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {"success": False}

        result = client.delete_kv(key="99999")

        assert result is False

    # =========================================================================
    # 7. batch_register_kv
    # =========================================================================

    def test_batch_register_kv_success(self):
        """batch_register_kv returns BatchRegisterKVResponse with results."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {
            "success": True,
            "results": [True, False, True],
        }

        entries = [("1", 1, 100, 200), ("2", 1, 300, 400), ("3", 2, 500, 600)]
        result = client.batch_register_kv(entries)

        expected_entries = [
            {"key": "1", "region_id": 1, "kv_offset": 100, "kv_length": 200},
            {"key": "2", "region_id": 1, "kv_offset": 300, "kv_length": 400},
            {"key": "3", "region_id": 2, "kv_offset": 500, "kv_length": 600},
        ]
        mock_send.assert_called_once_with(
            MessageType.BATCH_REGISTER_KV, {"entries": expected_entries}
        )
        assert isinstance(result, BatchRegisterKVResponse)
        assert result.success is True
        assert result.results == [True, False, True]

    # =========================================================================
    # 8. batch_lookup_kv
    # =========================================================================

    def test_batch_lookup_kv_mixed_results(self):
        """batch_lookup_kv returns BatchLookupKVResponse with found and not-found entries."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {
            "entries": [
                {
                    "found": True,
                    "handle": {
                        "region_id": 1,
                        "offset": 0,
                        "length": 1024,
                        "auth_token": 0,
                    },
                    "kv_offset": 100,
                    "kv_length": 200,
                },
                {"found": False},
                {
                    "found": True,
                    "handle": {
                        "region_id": 2,
                        "offset": 128,
                        "length": 2048,
                        "auth_token": 0,
                    },
                    "kv_offset": 300,
                    "kv_length": 400,
                },
            ]
        }

        keys = ["111", "222", "333"]
        result = client.batch_lookup_kv(keys)

        mock_send.assert_called_once_with(MessageType.BATCH_LOOKUP_KV, {"keys": keys})
        assert isinstance(result, BatchLookupKVResponse)
        assert len(result.entries) == 3

        # First entry: found
        assert result.entries[0].found is True
        assert isinstance(result.entries[0].handle, MaruHandle)
        assert result.entries[0].handle.region_id == 1
        assert result.entries[0].kv_offset == 100
        assert result.entries[0].kv_length == 200

        # Second entry: not found
        assert result.entries[1].found is False
        assert result.entries[1].handle is None

        # Third entry: found
        assert result.entries[2].found is True
        assert isinstance(result.entries[2].handle, MaruHandle)
        assert result.entries[2].handle.region_id == 2

    def test_batch_lookup_kv_no_handle(self):
        """batch_lookup_kv handles found entries without handle data."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {
            "entries": [{"found": True, "handle": None, "kv_offset": 0, "kv_length": 0}]
        }

        result = client.batch_lookup_kv(["123"])

        assert len(result.entries) == 1
        assert result.entries[0].found is True
        assert result.entries[0].handle is None

    # =========================================================================
    # 9. batch_exists_kv
    # =========================================================================

    def test_batch_exists_kv_success(self):
        """batch_exists_kv returns BatchExistsKVResponse with results."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {"results": [True, False, True, False]}

        keys = ["10", "20", "30", "40"]
        result = client.batch_exists_kv(keys)

        mock_send.assert_called_once_with(MessageType.BATCH_EXISTS_KV, {"keys": keys})
        assert isinstance(result, BatchExistsKVResponse)
        assert result.results == [True, False, True, False]

    # =========================================================================
    # list_allocations
    # =========================================================================

    def test_list_allocations_success(self):
        """list_allocations returns ListAllocationsResponse with handles on success."""
        from maru_common import ListAllocationsResponse

        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {
            "success": True,
            "allocations": [
                {"region_id": 1, "offset": 0, "length": 4096, "auth_token": 0},
                {"region_id": 2, "offset": 0, "length": 8192, "auth_token": 0},
            ],
        }

        result = client.list_allocations(exclude_instance_id="inst1")

        mock_send.assert_called_once_with(
            MessageType.LIST_ALLOCATIONS, {"exclude_instance_id": "inst1"}
        )
        assert isinstance(result, ListAllocationsResponse)
        assert result.success is True
        assert len(result.allocations) == 2
        assert result.allocations[0].region_id == 1
        assert result.allocations[1].region_id == 2

    def test_list_allocations_no_exclude(self):
        """list_allocations without exclude_instance_id sends empty data."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {
            "success": True,
            "allocations": [],
        }

        result = client.list_allocations()

        mock_send.assert_called_once_with(MessageType.LIST_ALLOCATIONS, {})
        assert result.success is True
        assert len(result.allocations) == 0

    def test_list_allocations_failure(self):
        """list_allocations returns error on failure."""
        from maru_common import ListAllocationsResponse

        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {"success": False, "error": "server error"}

        result = client.list_allocations()

        assert isinstance(result, ListAllocationsResponse)
        assert result.success is False
        assert result.error == "server error"

    # =========================================================================
    # 10. get_stats
    # =========================================================================

    def test_get_stats_success(self):
        """get_stats returns GetStatsResponse with KV and allocation manager stats."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {
            "kv_manager": {"total_entries": 42, "total_size": 102400},
            "allocation_manager": {
                "num_allocations": 10,
                "total_allocated": 409600,
                "active_clients": 3,
            },
        }

        result = client.get_stats()

        mock_send.assert_called_once_with(MessageType.GET_STATS, {})
        assert isinstance(result, GetStatsResponse)
        assert isinstance(result.kv_manager, KVManagerStats)
        assert result.kv_manager.total_entries == 42
        assert result.kv_manager.total_size == 102400
        assert isinstance(result.allocation_manager, AllocationManagerStats)
        assert result.allocation_manager.num_allocations == 10
        assert result.allocation_manager.total_allocated == 409600
        assert result.allocation_manager.active_clients == 3

    def test_get_stats_empty_response(self):
        """get_stats handles empty response with default values."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {}

        result = client.get_stats()

        assert isinstance(result, GetStatsResponse)
        assert result.kv_manager.total_entries == 0
        assert result.kv_manager.total_size == 0
        assert result.allocation_manager.num_allocations == 0
        assert result.allocation_manager.total_allocated == 0
        assert result.allocation_manager.active_clients == 0

    # =========================================================================
    # 11. heartbeat
    # =========================================================================

    def test_heartbeat_success(self):
        """heartbeat returns True when server responds without error."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {"status": "ok"}

        result = client.heartbeat()

        mock_send.assert_called_once_with(MessageType.HEARTBEAT, {})
        assert result is True

    def test_heartbeat_failure(self):
        """heartbeat returns False when response contains error."""
        client, mock_send = self._make_client_with_mock()
        mock_send.return_value = {"error": "timeout"}

        result = client.heartbeat()

        assert result is False
