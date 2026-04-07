# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruShmClient — shared memory client for the Maru Resource Manager.

Communicates with the resource manager over a persistent TCP connection
using the binary IPC protocol defined in maru_shm.ipc.
"""

import logging
import mmap as mmap_module
import os
import socket
import threading

from .constants import DEFAULT_ADDRESS
from .ipc import (
    HEADER_SIZE,
    AllocReq,
    AllocResp,
    ErrorResp,
    FreeReq,
    FreeResp,
    GetAccessReq,
    GetAccessResp,
    MsgHeader,
    MsgType,
    StatsReq,
    StatsResp,
)
from .types import MaruHandle, MaruPoolInfo
from .uds_helpers import read_full, write_full

logger = logging.getLogger(__name__)


def _make_client_id() -> str:
    """Build a client_id string: 'hostname:pid'."""
    import platform

    return f"{platform.node()}:{os.getpid()}"


# Module-level request ID counter shared across all MaruShmClient instances.
# Prevents idempotency cache collisions when multiple instances in the same
# process have the same client_id (hostname:pid).
_request_id_counter = 0
_request_id_lock = threading.Lock()


def _next_request_id() -> int:
    """Generate a monotonically increasing request ID (process-global)."""
    global _request_id_counter
    with _request_id_lock:
        _request_id_counter += 1
        return _request_id_counter


class MaruShmClient:
    """Client for the Maru Resource Manager.

    Maintains a persistent TCP connection to the resource manager.
    Multiple RPC calls reuse the same connection. If the connection
    drops, it is transparently re-established on the next call.

    Device paths received from alloc/get_access are cached by region_id.
    mmap() opens the device path directly to create Python mmap objects.
    """

    def __init__(self, address: str | None = None):
        self._address = address or DEFAULT_ADDRESS
        self._path_cache: dict[int, str] = {}  # region_id -> device path
        self._mmap_cache: dict[int, mmap_module.mmap] = {}  # region_id -> mmap
        self._lock = threading.Lock()
        self._client_id = _make_client_id()
        self._sock: socket.socket | None = None
        self._conn_lock = threading.Lock()

    @staticmethod
    def _parse_address(address: str) -> tuple[str, int]:
        """Parse 'host:port' string."""
        host, _, port_str = address.rpartition(":")
        if not host:
            host = "127.0.0.1"
        return host, int(port_str)

    def is_running(self) -> bool:
        """Check if the resource manager is reachable."""
        host, port = self._parse_address(self._address)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((host, port))
            sock.close()
            return True
        except OSError:
            sock.close()
            return False

    def _ensure_conn(self) -> socket.socket:
        """Return the persistent connection, creating it if needed.

        Must be called with _conn_lock held.
        """
        if self._sock is not None:
            return self._sock

        host, port = self._parse_address(self._address)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(5.0)  # 5s connect timeout
            sock.connect((host, port))
            sock.settimeout(
                10.0
            )  # 10s RPC timeout (prevents infinite block on server hang)
        except OSError as e:
            sock.close()
            raise ConnectionError(
                f"Resource manager is not running "
                f"(address: {self._address}).\n"
                f"Start it first: maru-resource-manager "
                f"--host {host} --port {port}"
            ) from e

        # Warn when connecting to a remote host over plaintext TCP
        if host not in ("127.0.0.1", "localhost", "::1"):
            logger.warning(
                "Connecting to remote host %s over PLAINTEXT TCP. "
                "Auth tokens will be transmitted without encryption. "
                "Use an encrypted tunnel for production deployments.",
                host,
            )

        # Disable Nagle for low-latency RPC
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._sock = sock
        return self._sock

    def _close_conn(self) -> None:
        """Close the persistent connection."""
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def _rpc(self, msg_type: MsgType, payload: bytes) -> tuple[MsgHeader, bytes]:
        """Execute a single RPC: send request, receive response.

        Thread-safe: the entire send+recv cycle is serialized by _conn_lock.
        On connection error, closes and retries once with a fresh connection.
        Idempotency for alloc/free is guaranteed by request_id in the payload;
        the server deduplicates by request_id and returns cached responses.

        Returns:
            (response_header, response_payload)
        """
        for attempt in range(2):
            with self._conn_lock:
                sock = self._ensure_conn()
                try:
                    # Send
                    hdr = MsgHeader(msg_type=msg_type, payload_len=len(payload))
                    write_full(sock, hdr.pack())
                    if payload:
                        write_full(sock, payload)

                    # Receive
                    resp_data = read_full(sock, HEADER_SIZE)
                    resp_hdr = MsgHeader.unpack(resp_data)
                    if not resp_hdr.validate():
                        raise ConnectionError(
                            f"Invalid response header: magic=0x{resp_hdr.magic:08X}"
                        )

                    resp_payload = b""
                    if resp_hdr.payload_len > 0:
                        resp_payload = read_full(sock, resp_hdr.payload_len)

                    return resp_hdr, resp_payload

                except (ConnectionError, OSError):
                    self._close_conn()
                    if attempt == 1:
                        raise

    def _check_error(self, hdr: MsgHeader, payload: bytes, context: str) -> None:
        """Raise RuntimeError if response is an ERROR_RESP."""
        if hdr.msg_type == MsgType.ERROR_RESP:
            err = ErrorResp.unpack(payload)
            raise RuntimeError(f"{context} ({err.status}): {err.message}")

    # =========================================================================
    # Public API
    # =========================================================================

    def stats(self) -> list[MaruPoolInfo]:
        """Query pool statistics from the resource manager."""
        hdr, payload = self._rpc(MsgType.STATS_REQ, StatsReq().pack())
        self._check_error(hdr, payload, "Stats failed")
        resp = StatsResp.unpack(payload)
        return resp.pools or []

    def alloc(self, size: int, dax_path: str = "") -> MaruHandle:
        """Allocate shared memory from the resource manager.

        Args:
            size: Requested allocation size in bytes.
            dax_path: DAX device path (e.g. "/dev/dax0.0"), or "" for any pool.

        Returns:
            Handle for the allocation.

        Raises:
            RuntimeError: On allocation failure.
        """
        req = AllocReq(
            size=size,
            dax_path=dax_path,
            client_id=self._client_id,
            request_id=_next_request_id(),
        )
        hdr, payload = self._rpc(MsgType.ALLOC_REQ, req.pack())
        self._check_error(hdr, payload, "Alloc failed")

        resp = AllocResp.unpack(payload)
        if resp.status != 0:
            raise RuntimeError(f"Alloc failed with status {resp.status}")

        handle = resp.handle
        if resp.device_path:
            with self._lock:
                self._path_cache[handle.region_id] = resp.device_path

        logger.debug(
            "alloc(size=%d, dax_path=%s) -> region_id=%d path=%s",
            size,
            dax_path,
            handle.region_id,
            resp.device_path,
        )
        return handle

    def free(self, handle: MaruHandle) -> None:
        """Free a previously allocated handle.

        Args:
            handle: Handle from a previous alloc() call.
        """
        req = FreeReq(
            handle=handle,
            client_id=self._client_id,
            request_id=_next_request_id(),
        )
        hdr, payload = self._rpc(MsgType.FREE_REQ, req.pack())
        self._check_error(hdr, payload, "Free failed")

        resp = FreeResp.unpack(payload)
        if resp.status != 0:
            raise RuntimeError(f"Free failed with status {resp.status}")

        with self._lock:
            self._close_region_locked(handle.region_id)
        logger.debug("free(region_id=%d)", handle.region_id)

    def _request_access(self, handle: MaruHandle) -> GetAccessResp:
        """Request access info from the resource manager via GET_ACCESS_REQ."""
        req = GetAccessReq(handle=handle, client_id=self._client_id)
        hdr, payload = self._rpc(MsgType.GET_ACCESS_REQ, req.pack())
        self._check_error(hdr, payload, "GetAccess failed")

        resp = GetAccessResp.unpack(payload)
        if resp.status != 0:
            raise RuntimeError(f"GetAccess failed with status {resp.status}")
        return resp

    def mmap(self, handle: MaruHandle, prot: int) -> mmap_module.mmap:
        """Memory-map a handle into the calling process.

        Opens the device path directly and creates an mmap.

        Args:
            handle: Handle from alloc() or lookup.
            prot: Protection flags (PROT_READ | PROT_WRITE).

        Returns:
            Python mmap object with buffer protocol support.
        """
        # Fast path: check cache
        with self._lock:
            if handle.region_id in self._mmap_cache:
                return self._mmap_cache[handle.region_id]
            path = self._path_cache.get(handle.region_id)

        # Slow path: network RPC outside lock
        if path is None:
            access_resp = self._request_access(handle)
            path = access_resp.device_path

        # Create mmap and update cache
        with self._lock:
            # Double-check: another thread may have created it
            if handle.region_id in self._mmap_cache:
                return self._mmap_cache[handle.region_id]

            self._path_cache[handle.region_id] = path

            access = mmap_module.ACCESS_READ
            if prot & 0x2:  # PROT_WRITE
                access = mmap_module.ACCESS_WRITE

            fd = os.open(path, os.O_RDWR)
            try:
                mm = mmap_module.mmap(
                    fd,
                    handle.length,
                    access=access,
                    offset=handle.offset,
                )
            finally:
                os.close(fd)

            self._mmap_cache[handle.region_id] = mm

        logger.debug(
            "mmap(region_id=%d, length=%d, offset=%d, path=%s)",
            handle.region_id,
            handle.length,
            handle.offset,
            path,
        )
        return mm

    def munmap(self, handle: MaruHandle) -> None:
        """Unmap a previously mapped handle."""
        with self._lock:
            mm = self._mmap_cache.pop(handle.region_id, None)
        if mm is not None:
            mm.close()
        logger.debug("munmap(region_id=%d)", handle.region_id)

    def _close_region_locked(self, region_id: int) -> None:
        """Close mmap and path cache for a region (must hold self._lock)."""
        mm = self._mmap_cache.pop(region_id, None)
        if mm is not None:
            mm.close()
        self._path_cache.pop(region_id, None)

    def close(self) -> None:
        """Close persistent connection and all cached mmaps."""
        with self._conn_lock:
            self._close_conn()
        with self._lock:
            num_mmaps = len(self._mmap_cache)
            for region_id in list(self._mmap_cache.keys()):
                self._close_region_locked(region_id)
            self._mmap_cache.clear()
            self._path_cache.clear()
        logger.debug("close(): released %d mmaps", num_mmaps)

    def __del__(self) -> None:
        self.close()
