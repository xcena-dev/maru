# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tests for multiple MaruShmClient instances sharing the same resource manager.

Validates:
- Independent alloc/free from multiple clients on the same TCP server
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
    AllocResp,
    FreeReq,
    FreeResp,
    MsgHeader,
    MsgType,
    StatsResp,
)
from maru_shm.types import DaxType, MaruHandle, MaruPoolInfo
from maru_shm.uds_helpers import read_full, write_full

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


def _make_temp_file(size=4096, fill=b"\x00"):
    """Create a temp file of *size* bytes and return its path. Caller must unlink."""
    tmp_fd, tmp_path = tempfile.mkstemp()
    os.write(tmp_fd, fill * size)
    os.close(tmp_fd)
    return tmp_path


def _send_alloc_resp_with_path(sock, handle, requested_size, tmp_path):
    """Send an AllocResp with device_path pointing to a real temp file."""
    resp = AllocResp(
        status=0, handle=handle, requested_size=requested_size, device_path=tmp_path
    )
    payload = resp.pack()
    hdr = MsgHeader(msg_type=MsgType.ALLOC_RESP, payload_len=len(payload))
    write_full(sock, hdr.pack() + payload)


class MockResourceManagerServer:
    """Mock server with atomic region_id counter for multi-client tests."""

    def __init__(self, handler):
        self._handler = handler
        self._sock = None

    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(16)
        _, port = self._sock.getsockname()
        self._thread = threading.Thread(target=self._accept, daemon=True)
        self._thread.start()
        return f"127.0.0.1:{port}"

    def _accept(self):
        while True:
            try:
                client, _ = self._sock.accept()
            except OSError:
                break
            threading.Thread(
                target=self._handle_client, args=(client,), daemon=True
            ).start()

    def _handle_client(self, client):
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
        tmp_paths = []

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break
                if hdr.msg_type == MsgType.ALLOC_REQ:
                    with lock:
                        region_counter["val"] += 1
                        rid = region_counter["val"]
                    handle = MaruHandle(
                        region_id=rid, offset=0, length=4096, auth_token=rid * 100
                    )
                    tmp_path = _make_temp_file(size=4096)
                    with lock:
                        tmp_paths.append(tmp_path)
                    _send_alloc_resp_with_path(sock, handle, 4096, tmp_path)

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client_a = MaruShmClient(address=address)
            client_b = MaruShmClient(address=address)

            handle_a = client_a.alloc(4096)
            handle_b = client_b.alloc(4096)

            assert handle_a.region_id != handle_b.region_id
            assert handle_a.region_id == 1
            assert handle_b.region_id == 2

            client_a.close()
            client_b.close()
        finally:
            server.stop()
            for p in tmp_paths:
                try:
                    os.unlink(p)
                except OSError:
                    pass

    def test_two_clients_alloc_and_free(self):
        """Two clients can alloc and free independently."""
        region_counter = {"val": 0}
        free_log = []
        lock = threading.Lock()
        tmp_paths = []

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break
                if hdr.msg_type == MsgType.ALLOC_REQ:
                    with lock:
                        region_counter["val"] += 1
                        rid = region_counter["val"]
                    handle = MaruHandle(
                        region_id=rid, offset=0, length=4096, auth_token=rid * 100
                    )
                    tmp_path = _make_temp_file(size=4096)
                    with lock:
                        tmp_paths.append(tmp_path)
                    _send_alloc_resp_with_path(sock, handle, 4096, tmp_path)
                elif hdr.msg_type == MsgType.FREE_REQ:
                    req = FreeReq.unpack(payload)
                    with lock:
                        free_log.append(req.handle.region_id)
                    _send_response(sock, MsgType.FREE_RESP, FreeResp(status=0))

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client_a = MaruShmClient(address=address)
            client_b = MaruShmClient(address=address)

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
            for p in tmp_paths:
                try:
                    os.unlink(p)
                except OSError:
                    pass

    def test_concurrent_alloc_from_threads(self):
        """Multiple threads alloc concurrently on the same server."""
        region_counter = {"val": 0}
        lock = threading.Lock()
        tmp_paths = []

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break
                if hdr.msg_type == MsgType.ALLOC_REQ:
                    with lock:
                        region_counter["val"] += 1
                        rid = region_counter["val"]
                    handle = MaruHandle(
                        region_id=rid, offset=0, length=4096, auth_token=rid * 100
                    )
                    tmp_path = _make_temp_file(size=4096)
                    with lock:
                        tmp_paths.append(tmp_path)
                    _send_alloc_resp_with_path(sock, handle, 4096, tmp_path)

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            results = []
            errors = []

            def alloc_thread(idx):
                try:
                    client = MaruShmClient(address=address)
                    handle = client.alloc(4096)
                    results.append(handle.region_id)
                    client.close()
                except Exception as e:
                    errors.append(e)

            threads = [
                threading.Thread(target=alloc_thread, args=(i,)) for i in range(4)
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
            for p in tmp_paths:
                try:
                    os.unlink(p)
                except OSError:
                    pass


# =============================================================================
# Client isolation — one client closing doesn't affect others
# =============================================================================


class TestClientIsolation:
    def test_client_close_does_not_affect_other(self):
        """One client closing doesn't prevent the other from operating."""
        region_counter = {"val": 0}
        lock = threading.Lock()
        tmp_paths = []

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break
                if hdr.msg_type == MsgType.ALLOC_REQ:
                    with lock:
                        region_counter["val"] += 1
                        rid = region_counter["val"]
                    handle = MaruHandle(
                        region_id=rid, offset=0, length=4096, auth_token=rid * 100
                    )
                    tmp_path = _make_temp_file(size=4096)
                    with lock:
                        tmp_paths.append(tmp_path)
                    _send_alloc_resp_with_path(sock, handle, 4096, tmp_path)
                elif hdr.msg_type == MsgType.STATS_REQ:
                    pool = MaruPoolInfo(
                        pool_id=0,
                        dax_type=DaxType.DEV_DAX,
                        total_size=1 << 30,
                        free_size=1 << 29,
                        align_bytes=2 << 20,
                    )
                    _send_response(sock, MsgType.STATS_RESP, StatsResp(pools=[pool]))

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client_a = MaruShmClient(address=address)
            client_b = MaruShmClient(address=address)

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
            for p in tmp_paths:
                try:
                    os.unlink(p)
                except OSError:
                    pass

    def test_client_error_does_not_affect_other(self):
        """One client getting an error doesn't affect the other."""
        call_count = {"val": 0}
        lock = threading.Lock()
        tmp_paths = []

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break
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
                        tmp_path = _make_temp_file(size=4096)
                        with lock:
                            tmp_paths.append(tmp_path)
                        _send_alloc_resp_with_path(sock, handle, 4096, tmp_path)

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client_a = MaruShmClient(address=address)
            client_b = MaruShmClient(address=address)

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
            for p in tmp_paths:
                try:
                    os.unlink(p)
                except OSError:
                    pass


# =============================================================================
# Concurrent _ensure_resource_manager — flock serialization
# =============================================================================
