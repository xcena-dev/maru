# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tests for MaruShmClient: is_running, _ensure_conn, persistent connection,
free edge cases, __del__, and other coverage gaps.

These tests exercise the real MaruShmClient class (not the MockShmClient from
conftest), using mock TCP servers and targeted patching.
"""

import socket
import threading
from unittest.mock import patch

import pytest

from maru_shm.client import MaruShmClient
from maru_shm.ipc import (
    HEADER_SIZE,
    FreeResp,
    MsgHeader,
    MsgType,
)
from maru_shm.types import MaruHandle
from maru_shm.uds_helpers import read_full, write_full

# =============================================================================
# Helpers
# =============================================================================


def _recv_request(sock):
    """Read request header + payload from socket."""
    hdr = MsgHeader.unpack(read_full(sock, HEADER_SIZE))
    payload = read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
    return hdr, payload


def _send_response(sock, msg_type, resp):
    """Pack resp and send as a simple response."""
    payload = resp.pack()
    hdr = MsgHeader(msg_type=msg_type, payload_len=len(payload))
    write_full(sock, hdr.pack() + payload)


class MockTcpServer:
    """Mini mock resource manager that serves requests on a TCP socket."""

    def __init__(self, handler):
        self._handler = handler
        self._sock = None
        self._port = 0

    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._port = self._sock.getsockname()[1]
        self._sock.listen(4)
        self._thread = threading.Thread(target=self._accept, daemon=True)
        self._thread.start()
        return f"127.0.0.1:{self._port}"

    def _accept(self):
        while True:
            try:
                client, _ = self._sock.accept()
            except OSError:
                break
            try:
                self._handler(client)
            except Exception:
                pass
            finally:
                client.close()

    def stop(self):
        if self._sock:
            self._sock.close()


# =============================================================================
# is_running tests
# =============================================================================


class TestIsRunning:
    def test_true_when_server_listening(self):
        """is_running() returns True when server is listening."""

        def handler(sock):
            pass

        server = MockTcpServer(handler)
        addr = server.start()
        try:
            client = MaruShmClient(address=addr)
            assert client.is_running() is True
        finally:
            server.stop()

    def test_false_when_no_server(self):
        """is_running() returns False when no server is listening."""
        client = MaruShmClient(address="127.0.0.1:19999")
        assert client.is_running() is False


# =============================================================================
# _ensure_conn tests
# =============================================================================


class TestEnsureConn:
    def test_creates_connection(self):
        """_ensure_conn creates a TCP connection on first call."""

        def handler(sock):
            pass

        server = MockTcpServer(handler)
        addr = server.start()
        try:
            client = MaruShmClient(address=addr)
            sock = client._ensure_conn()
            assert sock is not None
            assert client._sock is sock
            client.close()
        finally:
            server.stop()

    def test_reuses_connection(self):
        """_ensure_conn returns same socket on subsequent calls."""

        def handler(sock):
            pass

        server = MockTcpServer(handler)
        addr = server.start()
        try:
            client = MaruShmClient(address=addr)
            sock1 = client._ensure_conn()
            sock2 = client._ensure_conn()
            assert sock1 is sock2
            client.close()
        finally:
            server.stop()

    def test_raises_connection_error_when_not_running(self):
        """_ensure_conn raises ConnectionError when server is not running."""
        client = MaruShmClient(address="127.0.0.1:19999")
        with pytest.raises(ConnectionError, match="Resource manager is not running"):
            client._ensure_conn()


# =============================================================================
# Persistent connection — reconnect on error
# =============================================================================


class TestReconnect:
    def test_rpc_reconnects_after_server_drop(self):
        """_rpc retries with a fresh connection if the first attempt fails."""
        call_count = 0

        def handler(sock):
            nonlocal call_count
            call_count += 1
            # Handle the request: read header + payload, send FREE_RESP
            hdr, _ = _recv_request(sock)
            if hdr.msg_type == MsgType.FREE_REQ:
                _send_response(sock, MsgType.FREE_RESP, FreeResp(status=0))

        server = MockTcpServer(handler)
        addr = server.start()
        try:
            client = MaruShmClient(address=addr)
            handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=123)

            # First call — establishes connection and succeeds
            client.free(handle)
            assert call_count == 1

            # Simulate server dropping the connection
            client._close_conn()

            # Second call — should reconnect and succeed
            client.free(handle)
            assert call_count == 2

            client.close()
        finally:
            server.stop()


# =============================================================================
# free edge cases
# =============================================================================


class TestFreeEdgeCases:
    def test_free_without_cached_resources(self):
        """free() works for a handle with no cached path/mmap."""

        def handler(sock):
            hdr, _ = _recv_request(sock)
            if hdr.msg_type == MsgType.FREE_REQ:
                _send_response(sock, MsgType.FREE_RESP, FreeResp(status=0))

        server = MockTcpServer(handler)
        addr = server.start()
        try:
            client = MaruShmClient(address=addr)
            handle = MaruHandle(region_id=999, offset=0, length=4096, auth_token=123)
            client.free(handle)
            assert 999 not in client._path_cache
            assert 999 not in client._mmap_cache
            client.close()
        finally:
            server.stop()


# =============================================================================
# __del__ test
# =============================================================================


class TestDel:
    def test_del_calls_close(self):
        """__del__ delegates to close()."""
        client = MaruShmClient(address="127.0.0.1:19999")
        with patch.object(client, "close") as mock_close:
            client.__del__()
            mock_close.assert_called_once()
