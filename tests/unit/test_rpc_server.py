# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for RpcServer and RpcAsyncServer handler dispatch logic."""

import logging
from unittest.mock import MagicMock, patch

import pytest
import zmq

from maru_common import MessageHeader, MessageType, Serializer

from maru_server.rpc_async_server import RpcAsyncServer
from maru_server.rpc_server import RpcServer
from maru_server.server import MaruServer


class MockRequest:
    """Mock request object for testing handlers."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


@pytest.mark.parametrize(
    "server_cls", [RpcServer, RpcAsyncServer], ids=["sync", "async"]
)
class TestRpcServerHandlerDispatch:
    """Test RpcServer/RpcAsyncServer._handle_message() dispatch logic."""

    def test_handle_message_unknown_type(self, server_cls):
        """Test _handle_message with unknown message type."""
        server = MaruServer()
        if server_cls is RpcServer:
            rpc = RpcServer(server, host="127.0.0.1", port=5555)
        else:
            rpc = RpcAsyncServer(server, host="127.0.0.1", port=5555, num_workers=2)

        # Unknown message type
        response = rpc._handle_message(99999, {})
        assert "error" in response
        assert "Unknown message type" in response["error"]

    def test_handle_request_alloc_failure(self, server_cls, monkeypatch):
        """Test _handle_request_alloc when allocation returns None."""
        server = MaruServer()
        if server_cls is RpcServer:
            rpc = RpcServer(server, host="127.0.0.1", port=5555)
        else:
            rpc = RpcAsyncServer(server, host="127.0.0.1", port=5555, num_workers=2)

        # Mock allocation to fail
        monkeypatch.setattr(
            server._allocation_manager,
            "allocate",
            lambda instance_id, size, pool_path="": None,
        )

        request = MockRequest(instance_id="instance1", size=4096, pool_path="")
        response = rpc._handle_request_alloc(request)

        assert response["success"] is False
        assert "error" in response
        assert response["error"] == "Allocation failed"


class TestRpcServerStartLoop:
    """Test RpcServer.start() main loop error handling for 100% coverage."""

    def test_start_socket_none_break(self):
        """L56-57: start() breaks when _socket becomes None mid-loop."""
        server = MaruServer()
        rpc = RpcServer(server, host="127.0.0.1", port=5555)

        mock_ctx = MagicMock()
        mock_sock = MagicMock()
        mock_ctx.socket.return_value = mock_sock

        def recv_side_effect():
            rpc._socket = None  # next iteration hits L56 break
            raise zmq.ZMQError(errno=zmq.EAGAIN)

        mock_sock.recv.side_effect = recv_side_effect

        with patch.object(zmq, "Context", return_value=mock_ctx):
            rpc.start()

        assert rpc._socket is None

    def test_start_zmq_error_non_eagain(self):
        """L73-74: Non-EAGAIN ZMQ error is logged."""
        server = MaruServer()
        rpc = RpcServer(server, host="127.0.0.1", port=5555)

        mock_ctx = MagicMock()
        mock_sock = MagicMock()
        mock_ctx.socket.return_value = mock_sock

        calls = [0]

        def recv_side_effect():
            calls[0] += 1
            if calls[0] == 1:
                raise zmq.ZMQError(zmq.ETERM)  # non-EAGAIN
            rpc._running = False
            raise zmq.ZMQError(errno=zmq.EAGAIN)

        mock_sock.recv.side_effect = recv_side_effect

        with (
            patch.object(zmq, "Context", return_value=mock_ctx),
            patch.object(
                logging.getLogger("maru_server.rpc_server"), "error"
            ) as mock_err,
        ):
            rpc.start()

        assert mock_err.called
        assert "ZMQ error" in mock_err.call_args[0][0]

    def test_start_general_exception_header_none(self):
        """L75-89: General exception with header=None sends error response."""
        server = MaruServer()
        rpc = RpcServer(server, host="127.0.0.1", port=5555)

        mock_ctx = MagicMock()
        mock_sock = MagicMock()
        mock_ctx.socket.return_value = mock_sock

        calls = [0]

        def recv_side_effect():
            calls[0] += 1
            if calls[0] == 1:
                return b"garbage"  # decode_request will fail
            rpc._running = False
            raise zmq.ZMQError(errno=zmq.EAGAIN)

        mock_sock.recv.side_effect = recv_side_effect

        with (
            patch.object(zmq, "Context", return_value=mock_ctx),
            patch.object(
                logging.getLogger("maru_server.rpc_server"), "error"
            ) as mock_err,
        ):
            rpc.start()

        assert mock_err.called
        assert "Error handling message" in mock_err.call_args[0][0]
        # Error response was sent via socket
        assert mock_sock.send.called

    def test_start_general_exception_with_valid_header(self):
        """L75-87: General exception after decode preserves header in error response."""
        server = MaruServer()
        rpc = RpcServer(server, host="127.0.0.1", port=5555)

        mock_ctx = MagicMock()
        mock_sock = MagicMock()
        mock_ctx.socket.return_value = mock_sock

        calls = [0]

        def recv_side_effect():
            calls[0] += 1
            if calls[0] == 1:
                return b"dummy"
            rpc._running = False
            raise zmq.ZMQError(errno=zmq.EAGAIN)

        mock_sock.recv.side_effect = recv_side_effect

        mock_header = MessageHeader(msg_type=MessageType.HEARTBEAT.value)

        with (
            patch.object(zmq, "Context", return_value=mock_ctx),
            patch.object(
                rpc._serializer,
                "decode_request",
                return_value=(mock_header, {}),
            ),
            patch.object(
                rpc,
                "_handle_message",
                side_effect=RuntimeError("handler boom"),
            ),
            patch.object(logging.getLogger("maru_server.rpc_server"), "error"),
        ):
            rpc.start()

        # Error response sent with the original header (not fallback)
        assert mock_sock.send.called

    def test_start_general_exception_error_send_fails(self):
        """L88-89: Error response send failure is silently caught."""
        server = MaruServer()
        rpc = RpcServer(server, host="127.0.0.1", port=5555)

        mock_ctx = MagicMock()
        mock_sock = MagicMock()
        mock_ctx.socket.return_value = mock_sock

        calls = [0]

        def recv_side_effect():
            calls[0] += 1
            if calls[0] == 1:
                return b"garbage"
            rpc._running = False
            raise zmq.ZMQError(errno=zmq.EAGAIN)

        mock_sock.recv.side_effect = recv_side_effect
        mock_sock.send.side_effect = Exception("send failed")

        with patch.object(zmq, "Context", return_value=mock_ctx):
            rpc.start()  # should not raise


class TestRpcAsyncServerProxyRoutine:
    """Test RpcAsyncServer._proxy_routine() poll-based proxy for coverage."""

    def test_proxy_routine_poll_zmq_error_breaks(self):
        """poller.poll() raises ZMQError → breaks out of loop."""
        server = MaruServer()
        srv = RpcAsyncServer(server, host="127.0.0.1", port=5556, num_workers=1)
        srv._running = True
        srv._frontend = MagicMock()
        srv._backend = MagicMock()

        mock_poller = MagicMock()
        mock_poller.poll.side_effect = zmq.ZMQError(zmq.ETERM)

        with patch.object(zmq, "Poller", return_value=mock_poller):
            srv._proxy_routine()  # should not raise

    def test_proxy_routine_forwards_frontend_to_backend(self):
        """Frontend message is forwarded to backend."""
        server = MaruServer()
        srv = RpcAsyncServer(server, host="127.0.0.1", port=5556, num_workers=1)
        srv._running = True
        srv._frontend = MagicMock()
        srv._backend = MagicMock()

        msg = [b"id", b"", b"data"]
        srv._frontend.recv_multipart.return_value = msg

        calls = [0]
        mock_poller = MagicMock()

        def poll_side_effect(_timeout):
            calls[0] += 1
            if calls[0] == 1:
                return [(srv._frontend, zmq.POLLIN)]
            srv._running = False
            return []

        mock_poller.poll.side_effect = poll_side_effect

        with patch.object(zmq, "Poller", return_value=mock_poller):
            srv._proxy_routine()

        srv._backend.send_multipart.assert_called_once_with(msg)

    def test_proxy_routine_forwards_backend_to_frontend(self):
        """Backend message is forwarded to frontend."""
        server = MaruServer()
        srv = RpcAsyncServer(server, host="127.0.0.1", port=5556, num_workers=1)
        srv._running = True
        srv._frontend = MagicMock()
        srv._backend = MagicMock()

        msg = [b"id", b"", b"reply"]
        srv._backend.recv_multipart.return_value = msg

        calls = [0]
        mock_poller = MagicMock()

        def poll_side_effect(_timeout):
            calls[0] += 1
            if calls[0] == 1:
                return [(srv._backend, zmq.POLLIN)]
            srv._running = False
            return []

        mock_poller.poll.side_effect = poll_side_effect

        with patch.object(zmq, "Poller", return_value=mock_poller):
            srv._proxy_routine()

        srv._frontend.send_multipart.assert_called_once_with(msg)

    def test_proxy_routine_recv_zmq_error_breaks(self):
        """recv_multipart raises ZMQError → breaks out of loop."""
        server = MaruServer()
        srv = RpcAsyncServer(server, host="127.0.0.1", port=5556, num_workers=1)
        srv._running = True
        srv._frontend = MagicMock()
        srv._backend = MagicMock()

        srv._frontend.recv_multipart.side_effect = zmq.ZMQError(zmq.ETERM)

        mock_poller = MagicMock()
        mock_poller.poll.return_value = [(srv._frontend, zmq.POLLIN)]

        with patch.object(zmq, "Poller", return_value=mock_poller):
            srv._proxy_routine()  # should not raise

    def test_proxy_routine_running_false_exits(self):
        """_running=False causes loop to exit."""
        server = MaruServer()
        srv = RpcAsyncServer(server, host="127.0.0.1", port=5556, num_workers=1)
        srv._running = False
        srv._frontend = MagicMock()
        srv._backend = MagicMock()

        mock_poller = MagicMock()

        with patch.object(zmq, "Poller", return_value=mock_poller):
            srv._proxy_routine()

        mock_poller.poll.assert_not_called()


class TestRpcAsyncServerWorkerLoop:
    """Test RpcAsyncServer._worker_routine() error handling for 100% coverage."""

    def test_worker_zmq_error_non_eagain(self):
        """L194-196: Non-EAGAIN ZMQ error in worker logs and breaks."""
        server = MaruServer()
        srv = RpcAsyncServer(server, host="127.0.0.1", port=5556, num_workers=1)
        srv._running = True
        srv._context = MagicMock()
        mock_sock = MagicMock()
        srv._context.socket.return_value = mock_sock

        mock_sock.recv.side_effect = zmq.ZMQError(zmq.ETERM)

        with patch.object(
            logging.getLogger("maru_server.rpc_async_server"), "error"
        ) as mock_err:
            srv._worker_routine(0)

        assert mock_err.called
        assert "ZMQ error" in mock_err.call_args[0][0]
        mock_sock.close.assert_called_once()

    def test_worker_general_exception_header_none(self):
        """L197-210: General exception with header=None sends error response."""
        server = MaruServer()
        srv = RpcAsyncServer(server, host="127.0.0.1", port=5556, num_workers=1)
        srv._running = True
        srv._context = MagicMock()
        mock_sock = MagicMock()
        srv._context.socket.return_value = mock_sock

        calls = [0]

        def recv_side_effect():
            calls[0] += 1
            if calls[0] == 1:
                return b"garbage"  # decode_request will fail
            srv._running = False
            raise zmq.ZMQError(errno=zmq.EAGAIN)

        mock_sock.recv.side_effect = recv_side_effect

        with patch.object(
            logging.getLogger("maru_server.rpc_async_server"), "error"
        ) as mock_err:
            srv._worker_routine(0)

        assert mock_err.called
        assert "error handling message" in mock_err.call_args[0][0]
        assert mock_sock.send.called

    def test_worker_general_exception_with_valid_header(self):
        """L197-208: General exception after decode preserves header."""
        server = MaruServer()
        srv = RpcAsyncServer(server, host="127.0.0.1", port=5556, num_workers=1)
        srv._running = True
        srv._context = MagicMock()
        mock_sock = MagicMock()
        srv._context.socket.return_value = mock_sock

        calls = [0]

        def recv_side_effect():
            calls[0] += 1
            if calls[0] == 1:
                return b"dummy"
            srv._running = False
            raise zmq.ZMQError(errno=zmq.EAGAIN)

        mock_sock.recv.side_effect = recv_side_effect

        mock_header = MessageHeader(msg_type=MessageType.HEARTBEAT.value)
        mock_serializer = MagicMock()
        mock_serializer.decode_request.return_value = (mock_header, {})

        with (
            patch(
                "maru_server.rpc_async_server.Serializer",
                return_value=mock_serializer,
            ),
            patch.object(
                srv,
                "_handle_message",
                side_effect=RuntimeError("handler boom"),
            ),
            patch.object(logging.getLogger("maru_server.rpc_async_server"), "error"),
        ):
            srv._worker_routine(0)

        assert mock_sock.send.called

    def test_worker_general_exception_error_send_fails(self):
        """L209-210: Error response send failure is silently caught."""
        server = MaruServer()
        srv = RpcAsyncServer(server, host="127.0.0.1", port=5556, num_workers=1)
        srv._running = True
        srv._context = MagicMock()
        mock_sock = MagicMock()
        srv._context.socket.return_value = mock_sock

        calls = [0]

        def recv_side_effect():
            calls[0] += 1
            if calls[0] == 1:
                return b"garbage"
            srv._running = False
            raise zmq.ZMQError(errno=zmq.EAGAIN)

        mock_sock.recv.side_effect = recv_side_effect
        mock_sock.send.side_effect = Exception("send failed")

        with patch.object(logging.getLogger("maru_server.rpc_async_server"), "error"):
            srv._worker_routine(0)  # should not raise

        mock_sock.close.assert_called_once()


@pytest.mark.parametrize(
    "server_cls", [RpcServer, RpcAsyncServer], ids=["sync", "async"]
)
class TestRpcHandlerCoverage:
    """Test additional handler paths for coverage."""

    def test_all_message_types_dispatch(self, server_cls):
        """Test that all message types are handled correctly."""
        server = MaruServer()
        if server_cls is RpcServer:
            rpc = RpcServer(server, host="127.0.0.1", port=5555)
        else:
            rpc = RpcAsyncServer(server, host="127.0.0.1", port=5555, num_workers=2)

        # Allocate a region for testing
        handle = server.request_alloc("instance1", 4096)
        assert handle is not None
        region_id = handle.region_id

        # Register KV for testing
        server.register_kv(key="100", region_id=region_id, kv_offset=0, kv_length=256)

        # Test REQUEST_ALLOC
        req = MockRequest(instance_id="instance2", size=2048, pool_path="")
        resp = rpc._handle_message(MessageType.REQUEST_ALLOC.value, req)
        assert resp["success"] is True

        # Test LIST_ALLOCATIONS
        req = MockRequest(exclude_instance_id=None)
        resp = rpc._handle_message(MessageType.LIST_ALLOCATIONS.value, req)
        assert resp["success"] is True
        assert len(resp["allocations"]) >= 1

        # Test RETURN_ALLOC
        req = MockRequest(instance_id="instance1", region_id=region_id)
        resp = rpc._handle_message(MessageType.RETURN_ALLOC.value, req)
        assert resp["success"] is True

        # Test REGISTER_KV
        req = MockRequest(key="200", region_id=region_id, kv_offset=256, kv_length=512)
        resp = rpc._handle_message(MessageType.REGISTER_KV.value, req)
        assert resp["success"] is True

        # Test LOOKUP_KV
        req = MockRequest(key="100")
        resp = rpc._handle_message(MessageType.LOOKUP_KV.value, req)
        assert resp["found"] is True

        # Test EXISTS_KV
        req = MockRequest(key="100")
        resp = rpc._handle_message(MessageType.EXISTS_KV.value, req)
        assert resp["exists"] is True

        # Test DELETE_KV
        req = MockRequest(key="100")
        resp = rpc._handle_message(MessageType.DELETE_KV.value, req)
        assert resp["success"] is True

        # Test BATCH_REGISTER_KV
        entries = [
            MockRequest(key="300", region_id=region_id, kv_offset=0, kv_length=128),
            MockRequest(key="400", region_id=region_id, kv_offset=128, kv_length=256),
        ]
        req = MockRequest(entries=entries)
        resp = rpc._handle_message(MessageType.BATCH_REGISTER_KV.value, req)
        assert resp["success"] is True

        # Test BATCH_LOOKUP_KV
        req = MockRequest(keys=["300", "400", "500"])
        resp = rpc._handle_message(MessageType.BATCH_LOOKUP_KV.value, req)
        assert "entries" in resp
        assert len(resp["entries"]) == 3

        # Test BATCH_EXISTS_KV
        req = MockRequest(keys=["300", "400", "500"])
        resp = rpc._handle_message(MessageType.BATCH_EXISTS_KV.value, req)
        assert "results" in resp
        assert len(resp["results"]) == 3

        # Test GET_STATS
        resp = rpc._handle_message(MessageType.GET_STATS.value, {})
        assert "kv_manager" in resp
        assert "allocation_manager" in resp

        # Test HEARTBEAT
        resp = rpc._handle_message(MessageType.HEARTBEAT.value, {})
        assert resp == {}


class TestRpcServerAdditionalCoverage:
    """Additional tests for RpcServer to achieve 100% coverage."""

    def test_address_property(self):
        """L42: Test address property returns correct TCP address."""
        server = MaruServer()
        rpc = RpcServer(server, host="127.0.0.1", port=5555)
        assert rpc.address == "tcp://127.0.0.1:5555"

    def test_start_successful_request_response(self):
        """L69-70: Successful decode → handle → encode_response → send."""
        server = MaruServer()
        rpc = RpcServer(server, host="127.0.0.1", port=5555)

        mock_ctx = MagicMock()
        mock_sock = MagicMock()
        mock_ctx.socket.return_value = mock_sock

        # Build a valid serialized HEARTBEAT request
        serializer = Serializer()
        request_data = serializer.encode(MessageType.HEARTBEAT, {})

        calls = [0]

        def recv_side_effect():
            calls[0] += 1
            if calls[0] == 1:
                return request_data  # Valid request
            rpc._running = False
            raise zmq.ZMQError(errno=zmq.EAGAIN)

        mock_sock.recv.side_effect = recv_side_effect

        with patch.object(zmq, "Context", return_value=mock_ctx):
            rpc.start()

        # Verify send was called with encoded response
        assert mock_sock.send.called

    def test_stop_cleans_up(self):
        """L98-107: Test stop() cleans up socket and context."""
        server = MaruServer()
        rpc = RpcServer(server, host="127.0.0.1", port=5555)
        rpc._running = True
        rpc._stopped_event.set()  # Pre-set so wait returns immediately
        mock_socket = MagicMock()
        mock_context = MagicMock()
        rpc._socket = mock_socket
        rpc._context = mock_context

        rpc.stop()

        assert rpc._running is False
        mock_socket.close.assert_called_once()
        assert rpc._socket is None
        mock_context.term.assert_called_once()
        assert rpc._context is None

    def test_stop_without_socket_context(self):
        """L101-106: Test stop() when socket and context are None."""
        server = MaruServer()
        rpc = RpcServer(server, host="127.0.0.1", port=5555)
        rpc._stopped_event.set()
        rpc._socket = None
        rpc._context = None

        rpc.stop()  # Should not raise
        assert rpc._running is False

    def test_handle_lookup_kv_not_found(self):
        """L180-181: Test _handle_lookup_kv not-found path."""
        server = MaruServer()
        rpc = RpcServer(server, host="127.0.0.1", port=5555)

        req = MockRequest(key="99999")  # Non-existent key
        response = rpc._handle_lookup_kv(req)
        assert response == {"found": False}


class TestRpcAsyncServerAdditionalCoverage:
    """Additional tests for RpcAsyncServer to achieve 100% coverage."""

    def test_async_server_address_property(self):
        """L74: Test async server address property."""
        server = MaruServer()
        srv = RpcAsyncServer(server, host="127.0.0.1", port=5556)
        assert srv.address == "tcp://127.0.0.1:5556"

    def test_start_and_stop(self):
        """L78-118: Test start() sets up sockets, workers, proxy, then stop() cleans up."""
        server = MaruServer()
        srv = RpcAsyncServer(server, host="127.0.0.1", port=5556, num_workers=2)

        mock_ctx = MagicMock()
        mock_frontend = MagicMock()
        mock_backend = MagicMock()
        sockets = [mock_frontend, mock_backend]
        mock_ctx.socket.side_effect = sockets

        # Make _worker_routine and _proxy_routine no-ops
        with (
            patch.object(zmq, "Context", return_value=mock_ctx),
            patch.object(srv, "_worker_routine"),
            patch.object(srv, "_proxy_routine"),
        ):
            # Start in a thread (it blocks on _stop_event.wait())
            import threading

            t = threading.Thread(target=srv.start, daemon=True)
            t.start()

            import time

            time.sleep(0.2)  # Let it set up

            # Verify state
            assert srv._running is True
            assert srv._frontend is not None
            assert srv._backend is not None

            # Stop
            srv.stop()
            t.join(timeout=2.0)

        assert srv._running is False
        assert srv._context is None

    def test_async_server_stop_cleanup(self):
        """L122-150: Test stop() independently with full cleanup."""
        server = MaruServer()
        srv = RpcAsyncServer(server, host="127.0.0.1", port=5556, num_workers=1)
        srv._running = True
        mock_frontend = MagicMock()
        mock_backend = MagicMock()
        mock_context = MagicMock()
        srv._frontend = mock_frontend
        srv._backend = mock_backend
        srv._context = mock_context
        srv._proxy_thread = MagicMock()
        srv._proxy_thread.is_alive.return_value = True
        worker = MagicMock()
        worker.is_alive.return_value = True
        srv._worker_threads = [worker]

        srv.stop()

        assert srv._running is False
        worker.join.assert_called_once_with(timeout=2.0)
        assert len(srv._worker_threads) == 0
        mock_frontend.close.assert_called_once()
        assert srv._frontend is None
        mock_backend.close.assert_called_once()
        assert srv._backend is None
        srv._proxy_thread.join.assert_called_once_with(timeout=2.0)
        mock_context.term.assert_called_once()
        assert srv._context is None

    def test_worker_successful_request_response(self):
        """L198-199: Worker receives valid request, handles it, sends response."""
        server = MaruServer()
        srv = RpcAsyncServer(server, host="127.0.0.1", port=5556, num_workers=1)
        srv._running = True
        srv._context = MagicMock()
        mock_sock = MagicMock()
        srv._context.socket.return_value = mock_sock

        serializer = Serializer()
        request_data = serializer.encode(MessageType.HEARTBEAT, {})

        calls = [0]

        def recv_side_effect():
            calls[0] += 1
            if calls[0] == 1:
                return request_data
            srv._running = False
            raise zmq.ZMQError(errno=zmq.EAGAIN)

        mock_sock.recv.side_effect = recv_side_effect

        srv._worker_routine(0)

        assert mock_sock.send.called
        mock_sock.close.assert_called_once()

    def test_async_handle_lookup_kv_not_found(self):
        """L301-302: Test async server _handle_lookup_kv not-found path."""
        server = MaruServer()
        srv = RpcAsyncServer(server, host="127.0.0.1", port=5556, num_workers=1)

        req = MockRequest(key="99999")  # Non-existent key
        response = srv._handle_lookup_kv(req)
        assert response == {"found": False}
