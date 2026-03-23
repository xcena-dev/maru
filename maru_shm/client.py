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

from .constants import ANY_POOL_ID, DEFAULT_ADDRESS, MAP_SHARED
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
    RegisterServerReq,
    RegisterServerResp,
    StatsReq,
    StatsResp,
    UnregisterServerReq,
    UnregisterServerResp,
)
from .types import MaruHandle, MaruPoolInfo
from .uds_helpers import read_full, write_full

logger = logging.getLogger(__name__)


def _make_client_id() -> str:
    """Build a client_id string: 'hostname:pid'."""
    import platform

    return f"{platform.node()}:{os.getpid()}"


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

        Thread-safe: protected by _conn_lock.
        Raises ConnectionError if the resource manager is not reachable.
        """
        with self._conn_lock:
            if self._sock is not None:
                return self._sock

            host, port = self._parse_address(self._address)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect((host, port))
            except OSError:
                sock.close()
                raise ConnectionError(
                    f"Resource manager is not running "
                    f"(address: {self._address}).\n"
                    f"Start it first: maru-resource-manager "
                    f"--host {host} --port {port}"
                )

            # Disable Nagle for low-latency RPC
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._sock = sock
            return self._sock

    def _close_conn(self) -> None:
        """Close the persistent connection."""
        with self._conn_lock:
            if self._sock is not None:
                try:
                    self._sock.close()
                except OSError:
                    pass
                self._sock = None

    def _rpc(self, msg_type: MsgType, payload: bytes) -> tuple[MsgHeader, bytes]:
        """Execute a single RPC: send request, receive response.

        Uses the persistent connection. On connection error, closes and
        retries once with a fresh connection.

        Returns:
            (response_header, response_payload)
        """
        for attempt in range(2):
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

    def register_server(self) -> None:
        """Register this process as an active MaruServer with the resource manager.

        Prevents idle shutdown while the server is running.
        """
        hdr, payload = self._rpc(
            MsgType.REGISTER_SERVER_REQ,
            RegisterServerReq(client_id=self._client_id).pack(),
        )
        self._check_error(hdr, payload, "Register server failed")
        resp = RegisterServerResp.unpack(payload)
        if resp.status != 0:
            raise RuntimeError(f"Register server failed with status {resp.status}")
        logger.info("Registered as active server with resource manager")

    def unregister_server(self) -> None:
        """Unregister this process as an active MaruServer.

        Allows idle shutdown to proceed if no allocations remain.
        """
        hdr, payload = self._rpc(
            MsgType.UNREGISTER_SERVER_REQ,
            UnregisterServerReq(client_id=self._client_id).pack(),
        )
        self._check_error(hdr, payload, "Unregister server failed")
        resp = UnregisterServerResp.unpack(payload)
        if resp.status != 0:
            raise RuntimeError(f"Unregister server failed with status {resp.status}")
        logger.info("Unregistered from resource manager")

    def alloc(self, size: int, pool_id: int = ANY_POOL_ID) -> MaruHandle:
        """Allocate shared memory from the resource manager.

        Args:
            size: Requested allocation size in bytes.
            pool_id: Specific pool ID, or ANY_POOL_ID for any.

        Returns:
            Handle for the allocation.

        Raises:
            RuntimeError: On allocation failure.
        """
        req = AllocReq(size=size, pool_id=pool_id, client_id=self._client_id)
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
            "alloc(size=%d, pool_id=%d) -> region_id=%d path=%s",
            size,
            pool_id,
            handle.region_id,
            resp.device_path,
        )
        return handle

    def free(self, handle: MaruHandle) -> None:
        """Free a previously allocated handle.

        Args:
            handle: Handle from a previous alloc() call.
        """
        req = FreeReq(handle=handle)
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
        req = GetAccessReq(handle=handle)
        hdr, payload = self._rpc(MsgType.GET_ACCESS_REQ, req.pack())
        self._check_error(hdr, payload, "GetAccess failed")

        resp = GetAccessResp.unpack(payload)
        if resp.status != 0:
            raise RuntimeError(f"GetAccess failed with status {resp.status}")
        return resp

    def mmap(self, handle: MaruHandle, prot: int, flags: int = 0) -> mmap_module.mmap:
        """Memory-map a handle into the calling process.

        Opens the device path directly and creates an mmap.

        Args:
            handle: Handle from alloc() or lookup.
            prot: Protection flags (PROT_READ | PROT_WRITE).
            flags: Mapping flags (defaults to MAP_SHARED).

        Returns:
            Python mmap object with buffer protocol support.
        """
        with self._lock:
            if handle.region_id in self._mmap_cache:
                return self._mmap_cache[handle.region_id]

            path = self._path_cache.get(handle.region_id)
            if path is None:
                access_resp = self._request_access(handle)
                path = access_resp.device_path
                self._path_cache[handle.region_id] = path

            if flags == 0:
                flags = MAP_SHARED

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
