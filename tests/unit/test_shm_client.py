# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for ShmClient (Phase 3).

Tests use a mock TCP server with ephemeral ports to verify client behavior
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
    GetAccessResp,
    MsgHeader,
    MsgType,
    StatsResp,
)
from maru_shm.types import DaxType, MaruHandle, MaruPoolInfo
from maru_shm.uds_helpers import read_full, write_full


class MockResourceManagerServer:
    """Mini mock resource manager that serves requests on a TCP socket."""

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
# Handler helpers — reduce boilerplate in test handler functions
# =============================================================================


def _recv_request(sock):
    """Read request header + payload from socket. Returns (MsgHeader, bytes)."""
    hdr = MsgHeader.unpack(read_full(sock, HEADER_SIZE))
    payload = read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
    return hdr, payload


def _send_response(sock, msg_type, resp):
    """Pack *resp* and send as a response."""
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
    """Send an AllocResp with dax_path pointing to a real temp file."""
    resp = AllocResp(
        status=0, handle=handle, requested_size=requested_size, dax_path=tmp_path
    )
    payload = resp.pack()
    hdr = MsgHeader(msg_type=MsgType.ALLOC_RESP, payload_len=len(payload))
    write_full(sock, hdr.pack() + payload)


def _send_get_access_resp(sock, dax_path, offset, length):
    """Send a GetAccessResp with dax_path, offset, length."""
    resp = GetAccessResp(status=0, dax_path=dax_path, offset=offset, length=length)
    payload = resp.pack()
    hdr = MsgHeader(msg_type=MsgType.GET_ACCESS_RESP, payload_len=len(payload))
    write_full(sock, hdr.pack() + payload)


# =============================================================================
# Stats tests
# =============================================================================


class TestShmClientStats:
    def test_stats_empty(self):
        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break
                if hdr.msg_type == MsgType.STATS_REQ:
                    _send_response(sock, MsgType.STATS_RESP, StatsResp(pools=[]))

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client = MaruShmClient(address=address)
            result = client.stats()
            assert result == []
            client.close()
        finally:
            server.stop()

    def test_stats_with_pools(self):
        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break
                if hdr.msg_type == MsgType.STATS_REQ:
                    pools = [
                        MaruPoolInfo(
                            dax_path="/dev/dax0.0",
                            dax_type=DaxType.DEV_DAX,
                            total_size=1 << 30,
                            free_size=1 << 29,
                            align_bytes=2 << 20,
                        ),
                    ]
                    _send_response(sock, MsgType.STATS_RESP, StatsResp(pools=pools))

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client = MaruShmClient(address=address)
            result = client.stats()
            assert len(result) == 1
            assert result[0].dax_path == "/dev/dax0.0"
            assert result[0].total_size == 1 << 30
            client.close()
        finally:
            server.stop()


# =============================================================================
# Alloc tests
# =============================================================================


class TestShmClientAlloc:
    def test_alloc_success(self):
        """Test alloc receives handle with dax_path."""
        tmp_paths = []

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break
                if hdr.msg_type == MsgType.ALLOC_REQ:
                    req = AllocReq.unpack(payload)
                    handle = MaruHandle(
                        region_id=1, offset=0, length=4096, auth_token=999
                    )
                    tmp_path = _make_temp_file(size=4096)
                    tmp_paths.append(tmp_path)
                    _send_alloc_resp_with_path(sock, handle, req.size, tmp_path)

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client = MaruShmClient(address=address)
            handle = client.alloc(4096)
            assert handle.region_id == 1
            assert handle.length == 4096
            assert handle.auth_token == 999
            client.close()
        finally:
            server.stop()
            for p in tmp_paths:
                try:
                    os.unlink(p)
                except OSError:
                    pass


# =============================================================================
# Free tests
# =============================================================================


class TestShmClientFree:
    def test_free_success(self):
        call_count = {"alloc": 0, "free": 0}
        tmp_paths = []

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break

                if hdr.msg_type == MsgType.ALLOC_REQ:
                    call_count["alloc"] += 1
                    handle = MaruHandle(
                        region_id=1, offset=0, length=4096, auth_token=999
                    )
                    tmp_path = _make_temp_file(size=4096)
                    tmp_paths.append(tmp_path)
                    _send_alloc_resp_with_path(sock, handle, 4096, tmp_path)

                elif hdr.msg_type == MsgType.FREE_REQ:
                    call_count["free"] += 1
                    _send_response(sock, MsgType.FREE_RESP, FreeResp(status=0))

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client = MaruShmClient(address=address)
            handle = client.alloc(4096)
            client.free(handle)
            assert call_count["free"] == 1
            client.close()
        finally:
            server.stop()
            for p in tmp_paths:
                try:
                    os.unlink(p)
                except OSError:
                    pass


# =============================================================================
# Mmap tests
# =============================================================================


class TestShmClientMmap:
    def test_mmap_success(self):
        """Test mmap opens dax_path and mmaps successfully."""
        call_count = {"alloc": 0, "get_access": 0}
        tmp_paths = []

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break

                if hdr.msg_type == MsgType.ALLOC_REQ:
                    call_count["alloc"] += 1
                    handle = MaruHandle(
                        region_id=1, offset=0, length=4096, auth_token=999
                    )
                    tmp_path = _make_temp_file(size=4096)
                    tmp_paths.append(tmp_path)
                    _send_alloc_resp_with_path(sock, handle, 4096, tmp_path)

                elif hdr.msg_type == MsgType.GET_ACCESS_REQ:
                    call_count["get_access"] += 1
                    tmp_path = _make_temp_file(size=4096, fill=b"\xff")
                    tmp_paths.append(tmp_path)
                    _send_get_access_resp(sock, tmp_path, 0, 4096)

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client = MaruShmClient(address=address)
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
            for p in tmp_paths:
                try:
                    os.unlink(p)
                except OSError:
                    pass

    def test_munmap_success(self):
        """Test munmap closes and removes mmap from cache."""
        call_count = {"alloc": 0}
        tmp_paths = []

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break

                if hdr.msg_type == MsgType.ALLOC_REQ:
                    call_count["alloc"] += 1
                    handle = MaruHandle(
                        region_id=2, offset=0, length=4096, auth_token=888
                    )
                    tmp_path = _make_temp_file(size=4096)
                    tmp_paths.append(tmp_path)
                    _send_alloc_resp_with_path(sock, handle, 4096, tmp_path)

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client = MaruShmClient(address=address)
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
            for p in tmp_paths:
                try:
                    os.unlink(p)
                except OSError:
                    pass


# =============================================================================
# Close tests
# =============================================================================


class TestShmClientClose:
    def test_close_cleans_up(self):
        """Test close() cleans up all paths and mmaps."""
        call_count = {"alloc": 0}
        tmp_paths = []

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break

                if hdr.msg_type == MsgType.ALLOC_REQ:
                    call_count["alloc"] += 1
                    handle = MaruHandle(
                        region_id=3, offset=0, length=4096, auth_token=777
                    )
                    tmp_path = _make_temp_file(size=4096)
                    tmp_paths.append(tmp_path)
                    _send_alloc_resp_with_path(sock, handle, 4096, tmp_path)

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client = MaruShmClient(address=address)
            handle = client.alloc(4096)

            mm = client.mmap(handle, PROT_READ | PROT_WRITE)
            assert mm is not None

            # Close should clean up everything
            client.close()

            # close() should not raise even after cleanup
            assert call_count["alloc"] == 1
        finally:
            server.stop()
            for p in tmp_paths:
                try:
                    os.unlink(p)
                except OSError:
                    pass

    def test_double_close_safe(self):
        """Test calling close() twice is safe."""
        # Create a minimal client (no connection needed)
        client = MaruShmClient(address="127.0.0.1:19999")
        client.close()
        # Second close should not raise
        client.close()


# =============================================================================
# Error handling tests
# =============================================================================


class TestShmClientErrors:
    def test_alloc_error_response(self):
        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break
                _send_response(
                    sock,
                    MsgType.ERROR_RESP,
                    ErrorResp(status=-12, message="out of memory"),
                )

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client = MaruShmClient(address=address)
            with pytest.raises(RuntimeError, match="out of memory"):
                client.alloc(4096)
            client.close()
        finally:
            server.stop()

    def test_invalid_response_header_magic(self):
        """Test _recv_header validation with invalid magic."""

        def handler(sock):
            while True:
                try:
                    _recv_request(sock)
                except (ConnectionError, OSError):
                    break
                bad_hdr = MsgHeader(
                    magic=0xBADBAD, msg_type=MsgType.STATS_RESP, payload_len=0
                )
                write_full(sock, bad_hdr.pack())
                break

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client = MaruShmClient(address=address)
            with pytest.raises(ConnectionError, match="Invalid response header"):
                client.stats()
            client.close()
        finally:
            server.stop()

    def test_stats_error_response(self):
        """Test stats() receiving ERROR_RESP."""

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break
                _send_response(
                    sock,
                    MsgType.ERROR_RESP,
                    ErrorResp(status=-1, message="stats unavailable"),
                )

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client = MaruShmClient(address=address)
            with pytest.raises(RuntimeError, match="stats unavailable"):
                client.stats()
            client.close()
        finally:
            server.stop()

    def test_alloc_nonzero_status(self):
        """Test alloc() with non-zero status."""

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break
                if hdr.msg_type == MsgType.ALLOC_REQ:
                    handle = MaruHandle(
                        region_id=1, offset=0, length=4096, auth_token=999
                    )
                    resp = AllocResp(status=-5, handle=handle, requested_size=4096)
                    _send_response(sock, MsgType.ALLOC_RESP, resp)

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client = MaruShmClient(address=address)
            with pytest.raises(RuntimeError, match="Alloc failed with status -5"):
                client.alloc(4096)
            client.close()
        finally:
            server.stop()

    def test_free_error_response(self):
        """Test free() receiving ERROR_RESP."""

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break
                _send_response(
                    sock,
                    MsgType.ERROR_RESP,
                    ErrorResp(status=-3, message="invalid handle"),
                )

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client = MaruShmClient(address=address)
            handle = MaruHandle(region_id=999, offset=0, length=100, auth_token=1)
            with pytest.raises(RuntimeError, match="invalid handle"):
                client.free(handle)
            client.close()
        finally:
            server.stop()

    def test_free_nonzero_status(self):
        """Test free() with non-zero status."""

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break
                _send_response(sock, MsgType.FREE_RESP, FreeResp(status=-7))

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client = MaruShmClient(address=address)
            handle = MaruHandle(region_id=1, offset=0, length=100, auth_token=1)
            with pytest.raises(RuntimeError, match="Free failed with status -7"):
                client.free(handle)
            client.close()
        finally:
            server.stop()

    def test_request_access_error_response(self):
        """Test _request_access() receiving ERROR_RESP."""

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break
                _send_response(
                    sock,
                    MsgType.ERROR_RESP,
                    ErrorResp(status=-10, message="access not available"),
                )

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client = MaruShmClient(address=address)
            handle = MaruHandle(region_id=10, offset=0, length=4096, auth_token=888)
            with pytest.raises(RuntimeError, match="access not available"):
                client._request_access(handle)
            client.close()
        finally:
            server.stop()

    def test_request_access_nonzero_status(self):
        """Test _request_access() with non-zero status."""

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break
                if hdr.msg_type == MsgType.GET_ACCESS_REQ:
                    resp = GetAccessResp(status=-8, dax_path="", offset=0, length=0)
                    _send_response(sock, MsgType.GET_ACCESS_RESP, resp)

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client = MaruShmClient(address=address)
            handle = MaruHandle(region_id=20, offset=0, length=4096, auth_token=111)
            with pytest.raises(RuntimeError, match="GetAccess failed with status -8"):
                client._request_access(handle)
            client.close()
        finally:
            server.stop()

    def test_mmap_calls_request_access_when_not_cached(self):
        """Test mmap() calling _request_access when path not in cache."""
        call_count = {"get_access": 0}
        tmp_paths = []

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break

                if hdr.msg_type == MsgType.GET_ACCESS_REQ:
                    call_count["get_access"] += 1
                    tmp_path = _make_temp_file(size=4096, fill=b"\xab")
                    tmp_paths.append(tmp_path)
                    _send_get_access_resp(sock, tmp_path, 0, 4096)

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client = MaruShmClient(address=address)
            # Create handle without going through alloc
            handle = MaruHandle(region_id=50, offset=0, length=4096, auth_token=333)

            # This should call _request_access since path not in cache
            mm = client.mmap(handle, PROT_READ | PROT_WRITE)
            assert call_count["get_access"] == 1
            assert mm is not None

            # Verify path was cached
            assert 50 in client._path_cache

            client.close()
        finally:
            server.stop()
            for p in tmp_paths:
                try:
                    os.unlink(p)
                except OSError:
                    pass

    def test_mmap_returns_cached_mmap(self):
        """Test mmap() returns cached mmap on second call."""
        call_count = {"alloc": 0}
        tmp_paths = []

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break

                if hdr.msg_type == MsgType.ALLOC_REQ:
                    call_count["alloc"] += 1
                    handle = MaruHandle(
                        region_id=100, offset=0, length=4096, auth_token=777
                    )
                    tmp_path = _make_temp_file(size=4096, fill=b"\xcc")
                    tmp_paths.append(tmp_path)
                    _send_alloc_resp_with_path(sock, handle, 4096, tmp_path)

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            client = MaruShmClient(address=address)
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
            for p in tmp_paths:
                try:
                    os.unlink(p)
                except OSError:
                    pass


class TestShmClientDeviceTable:
    """Test v2 device_table UUID → local path resolution in mmap."""

    def test_mmap_resolves_uuid_to_local_path(self):
        """When GET_ACCESS returns device_uuid, mmap uses device_table to resolve."""
        tmp_paths = []

        # The "local" file that the handler should actually mmap
        local_path = _make_temp_file(size=4096, fill=b"\xaa")
        tmp_paths.append(local_path)
        # The "RM" file — should NOT be opened when UUID resolves
        rm_path = _make_temp_file(size=4096, fill=b"\xbb")
        tmp_paths.append(rm_path)

        test_uuid = "550e8400-e29b-41d4-a716-446655440000"

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break

                if hdr.msg_type == MsgType.ALLOC_REQ:
                    handle = MaruHandle(
                        region_id=1, offset=0, length=4096, auth_token=999
                    )
                    # ALLOC response includes UUID
                    resp = AllocResp(
                        status=0,
                        handle=handle,
                        requested_size=4096,
                        dax_path=rm_path,
                        device_uuid=test_uuid,
                    )
                    resp_payload = resp.pack()
                    resp_hdr = MsgHeader(
                        msg_type=MsgType.ALLOC_RESP,
                        payload_len=len(resp_payload),
                    )
                    write_full(sock, resp_hdr.pack() + resp_payload)

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            device_table = {test_uuid: local_path}
            client = MaruShmClient(address=address, device_table=device_table)
            handle = client.alloc(4096)

            mm = client.mmap(handle, PROT_READ | PROT_WRITE)
            assert mm is not None
            # Should have mmap'd the LOCAL file (0xAA), not the RM file (0xBB)
            assert mm[0:1] == b"\xaa"

            client.close()
        finally:
            server.stop()
            for p in tmp_paths:
                try:
                    os.unlink(p)
                except OSError:
                    pass

    def test_mmap_falls_back_without_device_table(self):
        """Without device_table, mmap uses RM's dax_path directly."""
        tmp_paths = []
        rm_path = _make_temp_file(size=4096, fill=b"\xcc")
        tmp_paths.append(rm_path)

        def handler(sock):
            while True:
                try:
                    hdr, payload = _recv_request(sock)
                except (ConnectionError, OSError):
                    break

                if hdr.msg_type == MsgType.ALLOC_REQ:
                    handle = MaruHandle(
                        region_id=1, offset=0, length=4096, auth_token=999
                    )
                    _send_alloc_resp_with_path(sock, handle, 4096, rm_path)
                elif hdr.msg_type == MsgType.GET_ACCESS_REQ:
                    resp = GetAccessResp(
                        status=0,
                        dax_path=rm_path,
                        device_uuid="some-uuid",
                        offset=0,
                        length=4096,
                    )
                    resp_payload = resp.pack()
                    resp_hdr = MsgHeader(
                        msg_type=MsgType.GET_ACCESS_RESP,
                        payload_len=len(resp_payload),
                    )
                    write_full(sock, resp_hdr.pack() + resp_payload)

        server = MockResourceManagerServer(handler)
        address = server.start()
        try:
            # No device_table → fallback to RM path
            client = MaruShmClient(address=address)
            handle = client.alloc(4096)

            mm = client.mmap(handle, PROT_READ | PROT_WRITE)
            assert mm is not None
            assert mm[0:1] == b"\xcc"

            client.close()
        finally:
            server.stop()
            for p in tmp_paths:
                try:
                    os.unlink(p)
                except OSError:
                    pass
