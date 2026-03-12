# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tests for MaruShmClient: _ensure_resource_manager, _try_connect, _connect,
read-only mmap, __del__, and other coverage gaps.

These tests exercise the real MaruShmClient class (not the MockShmClient from
conftest), using mock UDS servers and targeted patching.
"""

import os
import socket
import tempfile
import threading

import pytest

from maru_shm.client import MaruShmClient
from maru_shm.constants import MAP_SHARED, PROT_READ, PROT_WRITE
from maru_shm.ipc import (
    HEADER_SIZE,
    AllocResp,
    FreeResp,
    MsgHeader,
    MsgType,
)
from maru_shm.types import MaruHandle
from maru_shm.uds_helpers import read_full, send_with_fd, write_full

from unittest.mock import patch


# =============================================================================
# Helpers (same pattern as test_shm_client.py)
# =============================================================================


def _recv_request(sock):
    """Read request header + payload from socket."""
    hdr = MsgHeader.unpack(read_full(sock, HEADER_SIZE))
    payload = read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
    return hdr, payload


def _send_response(sock, msg_type, resp):
    """Pack resp and send as a simple (no-FD) response."""
    payload = resp.pack()
    hdr = MsgHeader(msg_type=msg_type, payload_len=len(payload))
    write_full(sock, hdr.pack() + payload)


def _make_temp_fd(size=4096, fill=b"\x00"):
    """Create a temp file and return (fd, path). Caller must close/unlink."""
    tmp_fd, tmp_path = tempfile.mkstemp()
    os.write(tmp_fd, fill * size)
    os.close(tmp_fd)
    fd = os.open(tmp_path, os.O_RDWR)
    return fd, tmp_path


def _send_response_with_fd(sock, msg_type, resp, *, size=4096, fill=b"\x00"):
    """Pack resp, create a temp FD, and send header + payload-with-FD."""
    fd, tmp_path = _make_temp_fd(size, fill)
    payload = resp.pack()
    hdr = MsgHeader(msg_type=msg_type, payload_len=len(payload))
    write_full(sock, hdr.pack())
    send_with_fd(sock, payload, fd)
    os.close(fd)
    os.unlink(tmp_path)


class MockResourceManagerServer:
    """Mini mock resource manager that serves requests on a UDS socket."""

    def __init__(self, handler):
        self._handler = handler
        self._sock = None

    def start(self, tmpdir):
        path = os.path.join(tmpdir, "test.sock")
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.bind(path)
        self._sock.listen(4)
        self._thread = threading.Thread(target=self._accept, daemon=True)
        self._thread.start()
        return path

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
# _try_connect tests
# =============================================================================


class TestTryConnect:
    def test_success_when_server_listening(self):
        """_try_connect returns True when server is listening."""

        def handler(sock):
            pass  # Accept and close

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                assert client._try_connect() is True
            finally:
                server.stop()

    def test_failure_when_no_server(self):
        """_try_connect returns False when no server is listening."""
        client = MaruShmClient(
            socket_path="/tmp/_maru_nonexistent_test_sock.sock"
        )
        assert client._try_connect() is False


# =============================================================================
# _connect tests
# =============================================================================


class TestConnect:
    def test_direct_connection(self):
        """_connect succeeds on first attempt when server is running."""

        def handler(sock):
            pass

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                sock = client._connect()
                assert sock is not None
                sock.close()
            finally:
                server.stop()

    def test_auto_restart_on_failure(self):
        """_connect auto-starts resource manager when first attempt fails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sock_path = os.path.join(tmpdir, "test.sock")
            client = MaruShmClient(socket_path=sock_path)

            srv_sock = None

            def fake_ensure():
                nonlocal srv_sock
                srv_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                srv_sock.bind(sock_path)
                srv_sock.listen(1)

            try:
                with patch.object(
                    client, "_ensure_resource_manager", side_effect=fake_ensure
                ):
                    conn = client._connect()
                    assert conn is not None
                    conn.close()
            finally:
                if srv_sock:
                    srv_sock.close()


# =============================================================================
# _ensure_resource_manager tests
# =============================================================================


class TestEnsureResourceManager:
    def test_already_running(self):
        """Returns immediately if server is already connectable."""

        def handler(sock):
            pass

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                # Should return without starting anything
                client._ensure_resource_manager()
            finally:
                server.stop()

    def test_starts_server_successfully(self):
        """Starts server via Popen and polls until connectable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sock_path = os.path.join(tmpdir, "rm.sock")
            client = MaruShmClient(socket_path=sock_path)

            # Quick check fails, re-check after lock fails, poll succeeds
            call_results = iter([False, False, True])

            with (
                patch.object(
                    client,
                    "_try_connect",
                    side_effect=lambda: next(call_results),
                ),
                patch("subprocess.Popen") as mock_popen,
                patch("time.sleep"),
            ):
                client._ensure_resource_manager()
                mock_popen.assert_called_once()
                # Verify Popen was called with correct args
                args = mock_popen.call_args[0][0]
                assert "maru-resource-manager" in args
                assert "--socket-path" in args
                assert sock_path in args

    def test_timeout_raises_runtime_error(self):
        """Raises RuntimeError if server doesn't start within timeout."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sock_path = os.path.join(tmpdir, "rm.sock")
            client = MaruShmClient(socket_path=sock_path)

            with (
                patch.object(client, "_try_connect", return_value=False),
                patch("subprocess.Popen"),
                patch("time.sleep"),
            ):
                with pytest.raises(RuntimeError, match="failed to start within 5s"):
                    client._ensure_resource_manager()

    def test_another_process_starts_server(self):
        """When re-check after lock finds server already running (started by another process)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sock_path = os.path.join(tmpdir, "rm.sock")
            client = MaruShmClient(socket_path=sock_path)

            # Quick check fails, but re-check after lock succeeds
            call_results = iter([False, True])

            with (
                patch.object(
                    client,
                    "_try_connect",
                    side_effect=lambda: next(call_results),
                ),
                patch("subprocess.Popen") as mock_popen,
            ):
                client._ensure_resource_manager()
                # Popen should NOT be called — server was found on re-check
                mock_popen.assert_not_called()


# =============================================================================
# mmap edge cases
# =============================================================================


class TestMmapEdgeCases:
    def test_mmap_read_only(self):
        """mmap with PROT_READ uses ACCESS_READ."""

        def handler(sock):
            hdr, _ = _recv_request(sock)
            if hdr.msg_type == MsgType.ALLOC_REQ:
                handle = MaruHandle(
                    region_id=1, offset=0, length=4096, auth_token=999
                )
                resp = AllocResp(status=0, handle=handle, requested_size=4096)
                _send_response_with_fd(sock, MsgType.ALLOC_RESP, resp)

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                handle = client.alloc(4096)

                # Read-only mmap — hits ACCESS_READ branch
                mm = client.mmap(handle, PROT_READ)
                assert mm is not None
                assert len(mm) == 4096
                # Read should work
                _ = mm[0]

                client.close()
            finally:
                server.stop()

    def test_mmap_with_explicit_flags(self):
        """mmap with non-zero flags skips MAP_SHARED default."""

        def handler(sock):
            hdr, _ = _recv_request(sock)
            if hdr.msg_type == MsgType.ALLOC_REQ:
                handle = MaruHandle(
                    region_id=1, offset=0, length=4096, auth_token=999
                )
                resp = AllocResp(status=0, handle=handle, requested_size=4096)
                _send_response_with_fd(sock, MsgType.ALLOC_RESP, resp)

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                handle = client.alloc(4096)

                # Explicit flags — skips `if flags == 0: flags = MAP_SHARED`
                mm = client.mmap(handle, PROT_READ | PROT_WRITE, flags=MAP_SHARED)
                assert mm is not None
                assert len(mm) == 4096

                client.close()
            finally:
                server.stop()


# =============================================================================
# free edge cases
# =============================================================================


class TestFreeEdgeCases:
    def test_free_without_cached_resources(self):
        """free() works for a handle with no cached FD/mmap."""

        def handler(sock):
            hdr, _ = _recv_request(sock)
            if hdr.msg_type == MsgType.FREE_REQ:
                _send_response(sock, MsgType.FREE_RESP, FreeResp(status=0))

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                # Free without prior alloc — no cached FD/mmap
                handle = MaruHandle(
                    region_id=999, offset=0, length=4096, auth_token=123
                )
                client.free(handle)
                assert 999 not in client._fd_cache
                assert 999 not in client._mmap_cache
            finally:
                server.stop()


# =============================================================================
# __del__ test
# =============================================================================


class TestDel:
    def test_del_calls_close(self):
        """__del__ delegates to close()."""
        with tempfile.TemporaryDirectory() as tmpdir:
            client = MaruShmClient(
                socket_path=os.path.join(tmpdir, "fake.sock")
            )
            with patch.object(client, "close") as mock_close:
                client.__del__()
                mock_close.assert_called_once()
