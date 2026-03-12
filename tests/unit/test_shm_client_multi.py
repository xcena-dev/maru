# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tests for multiple MaruShmClient instances sharing the same resource manager.

Validates:
- Independent alloc/free from multiple clients on the same UDS server
- Client isolation (one client closing doesn't affect others)
- Concurrent _ensure_resource_manager flock serialization
"""

import os
import socket
import tempfile
import threading

import pytest

from maru_shm.client import MaruShmClient
from maru_shm.ipc import (
    HEADER_SIZE,
    AllocReq,
    AllocResp,
    FreeReq,
    FreeResp,
    MsgHeader,
    MsgType,
    StatsResp,
)
from maru_shm.types import MaruHandle, MaruPoolInfo, DaxType
from maru_shm.uds_helpers import read_full, send_with_fd, write_full

from unittest.mock import MagicMock, patch


# =============================================================================
# Helpers
# =============================================================================


def _recv_request(sock):
    hdr = MsgHeader.unpack(read_full(sock, HEADER_SIZE))
    payload = read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
    return hdr, payload


def _send_response(sock, msg_type, resp):
    payload = resp.pack()
    hdr = MsgHeader(msg_type=msg_type, payload_len=len(payload))
    write_full(sock, hdr.pack() + payload)


def _make_temp_fd(size=4096, fill=b"\x00"):
    tmp_fd, tmp_path = tempfile.mkstemp()
    os.write(tmp_fd, fill * size)
    os.close(tmp_fd)
    fd = os.open(tmp_path, os.O_RDWR)
    return fd, tmp_path


def _send_response_with_fd(sock, msg_type, resp, *, size=4096, fill=b"\x00"):
    fd, tmp_path = _make_temp_fd(size, fill)
    payload = resp.pack()
    hdr = MsgHeader(msg_type=msg_type, payload_len=len(payload))
    write_full(sock, hdr.pack())
    send_with_fd(sock, payload, fd)
    os.close(fd)
    os.unlink(tmp_path)


class MockResourceManagerServer:
    """Mock server with atomic region_id counter for multi-client tests."""

    def __init__(self, handler):
        self._handler = handler
        self._sock = None

    def start(self, tmpdir):
        path = os.path.join(tmpdir, "test.sock")
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.bind(path)
        self._sock.listen(16)
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
# Multiple clients — independent alloc/free
# =============================================================================


class TestMultiClientAllocFree:
    def test_two_clients_alloc_independently(self):
        """Two clients can alloc from the same resource manager independently."""
        region_counter = {"val": 0}
        lock = threading.Lock()

        def handler(sock):
            hdr, payload = _recv_request(sock)
            if hdr.msg_type == MsgType.ALLOC_REQ:
                with lock:
                    region_counter["val"] += 1
                    rid = region_counter["val"]
                handle = MaruHandle(
                    region_id=rid, offset=0, length=4096, auth_token=rid * 100
                )
                resp = AllocResp(status=0, handle=handle, requested_size=4096)
                _send_response_with_fd(sock, MsgType.ALLOC_RESP, resp)

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client_a = MaruShmClient(socket_path=sock_path)
                client_b = MaruShmClient(socket_path=sock_path)

                handle_a = client_a.alloc(4096)
                handle_b = client_b.alloc(4096)

                assert handle_a.region_id != handle_b.region_id
                assert handle_a.region_id == 1
                assert handle_b.region_id == 2

                client_a.close()
                client_b.close()
            finally:
                server.stop()

    def test_two_clients_alloc_and_free(self):
        """Two clients can alloc and free independently."""
        region_counter = {"val": 0}
        free_log = []
        lock = threading.Lock()

        def handler(sock):
            hdr, payload = _recv_request(sock)
            if hdr.msg_type == MsgType.ALLOC_REQ:
                with lock:
                    region_counter["val"] += 1
                    rid = region_counter["val"]
                handle = MaruHandle(
                    region_id=rid, offset=0, length=4096, auth_token=rid * 100
                )
                resp = AllocResp(status=0, handle=handle, requested_size=4096)
                _send_response_with_fd(sock, MsgType.ALLOC_RESP, resp)
            elif hdr.msg_type == MsgType.FREE_REQ:
                req = FreeReq.unpack(payload)
                with lock:
                    free_log.append(req.handle.region_id)
                _send_response(sock, MsgType.FREE_RESP, FreeResp(status=0))

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client_a = MaruShmClient(socket_path=sock_path)
                client_b = MaruShmClient(socket_path=sock_path)

                ha = client_a.alloc(4096)
                hb = client_b.alloc(4096)

                # Free in reverse order
                client_b.free(hb)
                client_a.free(ha)

                assert sorted(free_log) == [1, 2]

                client_a.close()
                client_b.close()
            finally:
                server.stop()

    def test_concurrent_alloc_from_threads(self):
        """Multiple threads alloc concurrently on the same server."""
        region_counter = {"val": 0}
        lock = threading.Lock()

        def handler(sock):
            hdr, payload = _recv_request(sock)
            if hdr.msg_type == MsgType.ALLOC_REQ:
                with lock:
                    region_counter["val"] += 1
                    rid = region_counter["val"]
                handle = MaruHandle(
                    region_id=rid, offset=0, length=4096, auth_token=rid * 100
                )
                resp = AllocResp(status=0, handle=handle, requested_size=4096)
                _send_response_with_fd(sock, MsgType.ALLOC_RESP, resp)

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                results = []
                errors = []

                def alloc_thread(idx):
                    try:
                        client = MaruShmClient(socket_path=sock_path)
                        handle = client.alloc(4096)
                        results.append(handle.region_id)
                        client.close()
                    except Exception as e:
                        errors.append(e)

                threads = [
                    threading.Thread(target=alloc_thread, args=(i,))
                    for i in range(4)
                ]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join(timeout=10)

                assert not errors, f"Thread errors: {errors}"
                assert len(results) == 4
                assert len(set(results)) == 4  # All unique region_ids
            finally:
                server.stop()


# =============================================================================
# Client isolation — one client closing doesn't affect others
# =============================================================================


class TestClientIsolation:
    def test_client_close_does_not_affect_other(self):
        """One client closing doesn't prevent the other from operating."""
        region_counter = {"val": 0}
        lock = threading.Lock()

        def handler(sock):
            hdr, payload = _recv_request(sock)
            if hdr.msg_type == MsgType.ALLOC_REQ:
                with lock:
                    region_counter["val"] += 1
                    rid = region_counter["val"]
                handle = MaruHandle(
                    region_id=rid, offset=0, length=4096, auth_token=rid * 100
                )
                resp = AllocResp(status=0, handle=handle, requested_size=4096)
                _send_response_with_fd(sock, MsgType.ALLOC_RESP, resp)
            elif hdr.msg_type == MsgType.STATS_REQ:
                pool = MaruPoolInfo(
                    pool_id=0,
                    dax_type=DaxType.DEV_DAX,
                    total_size=1 << 30,
                    free_size=1 << 29,
                    align_bytes=2 << 20,
                )
                _send_response(
                    sock, MsgType.STATS_RESP, StatsResp(pools=[pool])
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client_a = MaruShmClient(socket_path=sock_path)
                client_b = MaruShmClient(socket_path=sock_path)

                # Client A allocs then closes
                ha = client_a.alloc(4096)
                assert ha.region_id == 1
                client_a.close()

                # Client B should still work fine
                hb = client_b.alloc(4096)
                assert hb.region_id == 2

                pools = client_b.stats()
                assert len(pools) == 1

                client_b.close()
            finally:
                server.stop()

    def test_client_error_does_not_affect_other(self):
        """One client getting an error doesn't affect the other."""
        call_count = {"val": 0}
        lock = threading.Lock()

        def handler(sock):
            hdr, payload = _recv_request(sock)
            if hdr.msg_type == MsgType.ALLOC_REQ:
                with lock:
                    call_count["val"] += 1
                    n = call_count["val"]

                if n == 1:
                    # First alloc fails (error response)
                    from maru_shm.ipc import ErrorResp

                    _send_response(
                        sock,
                        MsgType.ERROR_RESP,
                        ErrorResp(status=-12, message="pool full"),
                    )
                else:
                    # Subsequent allocs succeed
                    handle = MaruHandle(
                        region_id=n, offset=0, length=4096, auth_token=n * 100
                    )
                    resp = AllocResp(
                        status=0, handle=handle, requested_size=4096
                    )
                    _send_response_with_fd(sock, MsgType.ALLOC_RESP, resp)

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client_a = MaruShmClient(socket_path=sock_path)
                client_b = MaruShmClient(socket_path=sock_path)

                # Client A gets an error
                with pytest.raises(RuntimeError, match="pool full"):
                    client_a.alloc(4096)

                # Client B should still succeed
                hb = client_b.alloc(4096)
                assert hb is not None
                assert hb.region_id == 2

                client_a.close()
                client_b.close()
            finally:
                server.stop()


# =============================================================================
# Concurrent _ensure_resource_manager — flock serialization
# =============================================================================


class TestConcurrentEnsureResourceManager:
    def test_only_one_popen_called(self):
        """Multiple concurrent _ensure_resource_manager calls — Popen called once."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sock_path = os.path.join(tmpdir, "rm.sock")
            popen_calls = []
            srv_socks = []  # Keep references to prevent GC

            def fake_popen(*args, **kwargs):
                popen_calls.append(1)
                # Create a real server socket to simulate RM starting
                srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                srv.bind(sock_path)
                srv.listen(16)
                srv_socks.append(srv)
                return MagicMock()

            barrier = threading.Barrier(4, timeout=10)
            errors = []

            def ensure_thread():
                try:
                    client = MaruShmClient(socket_path=sock_path)
                    barrier.wait()
                    client._ensure_resource_manager()
                except Exception as e:
                    errors.append(e)

            with (
                patch("subprocess.Popen", side_effect=fake_popen),
                patch("time.sleep"),
            ):
                threads = [
                    threading.Thread(target=ensure_thread) for _ in range(4)
                ]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join(timeout=15)

            # Cleanup
            for s in srv_socks:
                s.close()

            assert not errors, f"Thread errors: {errors}"
            assert len(popen_calls) == 1

    def test_flock_prevents_double_start(self):
        """Second caller finds server already running after flock acquisition."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sock_path = os.path.join(tmpdir, "rm.sock")
            popen_calls = []
            srv_socks = []

            def fake_popen(*args, **kwargs):
                popen_calls.append(1)
                srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                srv.bind(sock_path)
                srv.listen(4)
                srv_socks.append(srv)
                return MagicMock()

            with (
                patch("subprocess.Popen", side_effect=fake_popen),
                patch("time.sleep"),
            ):
                # First client starts the server
                client_a = MaruShmClient(socket_path=sock_path)
                client_a._ensure_resource_manager()

                # Second client finds it already running
                client_b = MaruShmClient(socket_path=sock_path)
                client_b._ensure_resource_manager()

            for s in srv_socks:
                s.close()

            # Only one Popen call
            assert len(popen_calls) == 1
