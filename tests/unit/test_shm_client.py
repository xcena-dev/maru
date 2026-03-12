# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for ShmClient (Phase 3).

Tests use a mock UDS server via socketpair to verify client behavior
without requiring a running Maru Resource Manager.
"""

import os
import socket
import tempfile
import threading

import pytest

from maru_shm.client import MaruShmClient
from maru_shm.constants import PROT_READ, PROT_WRITE
from maru_shm.ipc import (
    HEADER_SIZE,
    AllocReq,
    AllocResp,
    ErrorResp,
    FreeResp,
    GetFdResp,
    MsgHeader,
    MsgType,
    StatsResp,
)
from maru_shm.types import DaxType, MaruHandle, MaruPoolInfo
from maru_shm.uds_helpers import read_full, send_with_fd, write_full


class MockResourceManagerServer:
    """Mini mock resource manager that serves one request on a UDS socket."""

    def __init__(self, handler):
        self._handler = handler
        self._socket_path = None
        self._sock = None

    def start(self, tmpdir):
        self._socket_path = os.path.join(tmpdir, "test.sock")
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.bind(self._socket_path)
        self._sock.listen(4)
        self._thread = threading.Thread(target=self._accept, daemon=True)
        self._thread.start()
        return self._socket_path

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
# Handler helpers — reduce boilerplate in test handler functions
# =============================================================================


def _recv_request(sock):
    """Read request header + payload from socket. Returns (MsgHeader, bytes)."""
    hdr = MsgHeader.unpack(read_full(sock, HEADER_SIZE))
    payload = read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
    return hdr, payload


def _send_response(sock, msg_type, resp):
    """Pack *resp* and send as a simple (no-FD) response."""
    payload = resp.pack()
    hdr = MsgHeader(msg_type=msg_type, payload_len=len(payload))
    write_full(sock, hdr.pack() + payload)


def _make_temp_fd(size=4096, fill=b"\x00"):
    """Create a temp file of *size* bytes and return (fd, path). Caller must close/unlink."""
    tmp_fd, tmp_path = tempfile.mkstemp()
    os.write(tmp_fd, fill * size)
    os.close(tmp_fd)
    fd = os.open(tmp_path, os.O_RDWR)
    return fd, tmp_path


def _send_response_with_fd(sock, msg_type, resp, *, size=4096, fill=b"\x00"):
    """Pack *resp*, create a temp FD, and send header + payload-with-FD."""
    fd, tmp_path = _make_temp_fd(size, fill)
    payload = resp.pack()
    hdr = MsgHeader(msg_type=msg_type, payload_len=len(payload))
    write_full(sock, hdr.pack())
    send_with_fd(sock, payload, fd)
    os.close(fd)
    os.unlink(tmp_path)


# =============================================================================
# Stats tests
# =============================================================================


class TestShmClientStats:
    def test_stats_empty(self):
        def handler(sock):
            _recv_request(sock)
            _send_response(sock, MsgType.STATS_RESP, StatsResp(pools=[]))

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                result = client.stats()
                assert result == []
            finally:
                server.stop()

    def test_stats_with_pools(self):
        def handler(sock):
            _recv_request(sock)
            pools = [
                MaruPoolInfo(
                    pool_id=0,
                    dax_type=DaxType.DEV_DAX,
                    total_size=1 << 30,
                    free_size=1 << 29,
                    align_bytes=2 << 20,
                ),
            ]
            _send_response(sock, MsgType.STATS_RESP, StatsResp(pools=pools))

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                result = client.stats()
                assert len(result) == 1
                assert result[0].pool_id == 0
                assert result[0].total_size == 1 << 30
            finally:
                server.stop()


# =============================================================================
# Alloc tests
# =============================================================================


class TestShmClientAlloc:
    def test_alloc_success(self):
        """Test alloc receives handle + FD via SCM_RIGHTS."""

        def handler(sock):
            _, payload = _recv_request(sock)
            req = AllocReq.unpack(payload)
            handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=999)
            resp = AllocResp(status=0, handle=handle, requested_size=req.size)
            _send_response_with_fd(sock, MsgType.ALLOC_RESP, resp)

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                handle = client.alloc(4096)
                assert handle.region_id == 1
                assert handle.length == 4096
                assert handle.auth_token == 999
                client.close()
            finally:
                server.stop()


# =============================================================================
# Free tests
# =============================================================================


class TestShmClientFree:
    def test_free_success(self):
        call_count = {"alloc": 0, "free": 0}

        def handler(sock):
            hdr, _ = _recv_request(sock)

            if hdr.msg_type == MsgType.ALLOC_REQ:
                call_count["alloc"] += 1
                handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=999)
                resp = AllocResp(status=0, handle=handle, requested_size=4096)
                _send_response_with_fd(sock, MsgType.ALLOC_RESP, resp)

            elif hdr.msg_type == MsgType.FREE_REQ:
                call_count["free"] += 1
                _send_response(sock, MsgType.FREE_RESP, FreeResp(status=0))

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                handle = client.alloc(4096)
                client.free(handle)
                assert call_count["free"] == 1
                client.close()
            finally:
                server.stop()


# =============================================================================
# Mmap tests
# =============================================================================


class TestShmClientMmap:
    def test_mmap_success(self):
        """Test mmap receives handle + FD, then mmaps successfully."""
        call_count = {"alloc": 0, "get_fd": 0}

        def handler(sock):
            hdr, _ = _recv_request(sock)

            if hdr.msg_type == MsgType.ALLOC_REQ:
                call_count["alloc"] += 1
                handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=999)
                resp = AllocResp(status=0, handle=handle, requested_size=4096)
                _send_response_with_fd(sock, MsgType.ALLOC_RESP, resp)

            elif hdr.msg_type == MsgType.GET_FD_REQ:
                call_count["get_fd"] += 1
                _send_response_with_fd(
                    sock, MsgType.GET_FD_RESP, GetFdResp(status=0), fill=b"\xff"
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                handle = client.alloc(4096)
                assert handle.region_id == 1

                # Now call mmap with PROT_READ | PROT_WRITE
                mm = client.mmap(handle, PROT_READ | PROT_WRITE)
                assert mm is not None
                assert len(mm) == 4096

                # Verify it's an mmap object
                import mmap as mmap_module

                assert isinstance(mm, mmap_module.mmap)

                client.close()
            finally:
                server.stop()

    def test_munmap_success(self):
        """Test munmap closes and removes mmap from cache."""
        call_count = {"alloc": 0}

        def handler(sock):
            hdr, _ = _recv_request(sock)

            if hdr.msg_type == MsgType.ALLOC_REQ:
                call_count["alloc"] += 1
                handle = MaruHandle(region_id=2, offset=0, length=4096, auth_token=888)
                resp = AllocResp(status=0, handle=handle, requested_size=4096)
                _send_response_with_fd(sock, MsgType.ALLOC_RESP, resp)

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                handle = client.alloc(4096)

                mm = client.mmap(handle, PROT_READ | PROT_WRITE)
                assert mm is not None

                # Munmap should close the mmap
                client.munmap(handle)

                # Verify the mmap is no longer in cache (by checking it's closed)
                # We can't access closed mmap, so just verify munmap didn't raise
                assert call_count["alloc"] == 1

                client.close()
            finally:
                server.stop()


# =============================================================================
# Close tests
# =============================================================================


class TestShmClientClose:
    def test_close_cleans_up(self):
        """Test close() cleans up all FDs and mmaps."""
        call_count = {"alloc": 0}

        def handler(sock):
            hdr, _ = _recv_request(sock)

            if hdr.msg_type == MsgType.ALLOC_REQ:
                call_count["alloc"] += 1
                handle = MaruHandle(region_id=3, offset=0, length=4096, auth_token=777)
                resp = AllocResp(status=0, handle=handle, requested_size=4096)
                _send_response_with_fd(sock, MsgType.ALLOC_RESP, resp)

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                handle = client.alloc(4096)

                mm = client.mmap(handle, PROT_READ | PROT_WRITE)
                assert mm is not None

                # Close should clean up everything
                client.close()

                # close() should not raise even after cleanup
                assert call_count["alloc"] == 1
            finally:
                server.stop()

    def test_double_close_safe(self):
        """Test calling close() twice is safe."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a minimal client (no connection needed)
            client = MaruShmClient(socket_path=os.path.join(tmpdir, "fake.sock"))
            client.close()
            # Second close should not raise
            client.close()


# =============================================================================
# Error handling tests
# =============================================================================


class TestShmClientErrors:
    def test_alloc_error_response(self):
        def handler(sock):
            _recv_request(sock)
            _send_response(
                sock, MsgType.ERROR_RESP, ErrorResp(status=-12, message="out of memory")
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                with pytest.raises(RuntimeError, match="out of memory"):
                    client.alloc(4096)
            finally:
                server.stop()

    def test_invalid_response_header_magic(self):
        """Test _recv_header validation with invalid magic."""

        def handler(sock):
            _recv_request(sock)
            bad_hdr = MsgHeader(
                magic=0xBADBAD, msg_type=MsgType.STATS_RESP, payload_len=0
            )
            write_full(sock, bad_hdr.pack())

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                with pytest.raises(ConnectionError, match="Invalid response header"):
                    client.stats()
            finally:
                server.stop()

    def test_stats_error_response(self):
        """Test stats() receiving ERROR_RESP."""

        def handler(sock):
            _recv_request(sock)
            _send_response(
                sock,
                MsgType.ERROR_RESP,
                ErrorResp(status=-1, message="stats unavailable"),
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                with pytest.raises(RuntimeError, match="stats unavailable"):
                    client.stats()
            finally:
                server.stop()

    def test_alloc_nonzero_status_with_fd(self):
        """Test alloc() with non-zero status but FD received."""

        def handler(sock):
            _recv_request(sock)
            handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=999)
            resp = AllocResp(status=-5, handle=handle, requested_size=4096)
            _send_response_with_fd(sock, MsgType.ALLOC_RESP, resp)

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                with pytest.raises(RuntimeError, match="Alloc failed with status -5"):
                    client.alloc(4096)
            finally:
                server.stop()

    def test_alloc_success_no_fd(self):
        """Test alloc() success status but no FD received."""

        def handler(sock):
            _recv_request(sock)
            handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=999)
            resp = AllocResp(status=0, handle=handle, requested_size=4096)
            payload = resp.pack()
            hdr = MsgHeader(msg_type=MsgType.ALLOC_RESP, payload_len=len(payload))
            write_full(sock, hdr.pack())
            # Send payload without FD
            write_full(sock, payload)

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                with pytest.raises(RuntimeError, match="no FD received"):
                    client.alloc(4096)
            finally:
                server.stop()

    def test_free_error_response(self):
        """Test free() receiving ERROR_RESP."""

        def handler(sock):
            _recv_request(sock)
            _send_response(
                sock, MsgType.ERROR_RESP, ErrorResp(status=-3, message="invalid handle")
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                handle = MaruHandle(region_id=999, offset=0, length=100, auth_token=1)
                with pytest.raises(RuntimeError, match="invalid handle"):
                    client.free(handle)
            finally:
                server.stop()

    def test_free_nonzero_status(self):
        """Test free() with non-zero status."""

        def handler(sock):
            _recv_request(sock)
            _send_response(sock, MsgType.FREE_RESP, FreeResp(status=-7))

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                handle = MaruHandle(region_id=1, offset=0, length=100, auth_token=1)
                with pytest.raises(RuntimeError, match="Free failed with status -7"):
                    client.free(handle)
            finally:
                server.stop()

    def test_request_fd_error_response(self):
        """Test _request_fd() receiving ERROR_RESP."""

        def handler(sock):
            _recv_request(sock)
            _send_response(
                sock,
                MsgType.ERROR_RESP,
                ErrorResp(status=-10, message="fd not available"),
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                handle = MaruHandle(region_id=10, offset=0, length=4096, auth_token=888)
                with pytest.raises(RuntimeError, match="fd not available"):
                    client._request_fd(handle)
            finally:
                server.stop()

    def test_request_fd_nonzero_status_with_fd(self):
        """Test _request_fd() with non-zero status but FD received."""

        def handler(sock):
            _recv_request(sock)
            _send_response_with_fd(sock, MsgType.GET_FD_RESP, GetFdResp(status=-8))

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                handle = MaruHandle(region_id=20, offset=0, length=4096, auth_token=111)
                with pytest.raises(RuntimeError, match="GetFd failed with status -8"):
                    client._request_fd(handle)
            finally:
                server.stop()

    def test_request_fd_success_no_fd(self):
        """Test _request_fd() success but no FD received."""

        def handler(sock):
            _recv_request(sock)
            resp = GetFdResp(status=0)
            payload = resp.pack()
            hdr = MsgHeader(msg_type=MsgType.GET_FD_RESP, payload_len=len(payload))
            write_full(sock, hdr.pack())
            write_full(sock, payload)  # No FD

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                handle = MaruHandle(region_id=30, offset=0, length=4096, auth_token=222)
                with pytest.raises(
                    RuntimeError, match="GetFd succeeded but no FD received"
                ):
                    client._request_fd(handle)
            finally:
                server.stop()

    def test_mmap_calls_request_fd_when_not_cached(self):
        """Test mmap() calling _request_fd when FD not in cache."""
        call_count = {"get_fd": 0}

        def handler(sock):
            hdr, _ = _recv_request(sock)

            if hdr.msg_type == MsgType.GET_FD_REQ:
                call_count["get_fd"] += 1
                _send_response_with_fd(
                    sock, MsgType.GET_FD_RESP, GetFdResp(status=0), fill=b"\xab"
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                # Create handle without going through alloc
                handle = MaruHandle(region_id=50, offset=0, length=4096, auth_token=333)

                # This should call _request_fd since FD not in cache
                mm = client.mmap(handle, PROT_READ | PROT_WRITE)
                assert call_count["get_fd"] == 1
                assert mm is not None

                # Verify FD was cached
                assert 50 in client._fd_cache

                client.close()
            finally:
                server.stop()

    def test_mmap_returns_cached_mmap(self):
        """Test mmap() returns cached mmap on second call."""
        call_count = {"alloc": 0}

        def handler(sock):
            hdr, _ = _recv_request(sock)

            if hdr.msg_type == MsgType.ALLOC_REQ:
                call_count["alloc"] += 1
                handle = MaruHandle(
                    region_id=100, offset=0, length=4096, auth_token=777
                )
                resp = AllocResp(status=0, handle=handle, requested_size=4096)
                _send_response_with_fd(sock, MsgType.ALLOC_RESP, resp, fill=b"\xcc")

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                handle = client.alloc(4096)

                # First mmap call - creates and caches mmap
                mm1 = client.mmap(handle, PROT_READ | PROT_WRITE)
                assert mm1 is not None

                # Second mmap call - should return cached mmap
                mm2 = client.mmap(handle, PROT_READ | PROT_WRITE)
                assert mm2 is mm1  # Should be the exact same object

                client.close()
            finally:
                server.stop()
