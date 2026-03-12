# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruShmClient — shared memory client for the Maru Resource Manager.

Communicates with the resource manager over UDS using the binary IPC protocol
defined in maru_shm.ipc.
"""

import logging
import mmap as mmap_module
import os
import socket
import threading

from .constants import ANY_POOL_ID, DEFAULT_SOCKET_PATH, MAP_SHARED
from .ipc import (
    HEADER_SIZE,
    AllocReq,
    AllocResp,
    ErrorResp,
    FreeReq,
    FreeResp,
    GetFdReq,
    GetFdResp,
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
from .uds_helpers import read_full, recv_with_fd, write_full

logger = logging.getLogger(__name__)


class MaruShmClient:
    """Client for the Maru Resource Manager.

    Each RPC creates a new UDS connection, sends a request,
    receives a response, and closes the connection.

    FDs received from alloc/get_fd are cached by region_id.
    mmap() returns Python mmap objects (buffer protocol).
    """

    def __init__(self, socket_path: str | None = None):
        self._socket_path = socket_path or DEFAULT_SOCKET_PATH
        self._fd_cache: dict[int, int] = {}  # region_id -> fd
        self._mmap_cache: dict[int, mmap_module.mmap] = {}  # region_id -> mmap
        self._lock = threading.Lock()

    def _ensure_resource_manager(self) -> None:
        """Ensure the resource manager is running, starting it if needed.

        Uses flock to prevent multiple processes from starting the resource
        manager simultaneously. After acquiring the lock, re-checks connectivity
        in case another process already started it.
        """
        import fcntl
        import subprocess
        import time
        from pathlib import Path

        # Quick check — maybe it's already running
        if self._try_connect():
            return

        lock_path = Path(self._socket_path).parent / "rm.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)

        lock_fd = open(lock_path, "w")
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)

            # Re-check after acquiring lock (another process may have started it)
            if self._try_connect():
                return

            # Start resource manager in background
            logger.info(
                "Starting maru-resource-manager (socket: %s)", self._socket_path
            )
            subprocess.Popen(
                ["maru-resource-manager", "--socket-path", self._socket_path],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            # Wait for socket to become available
            for _ in range(50):  # max 5 seconds, 100ms intervals
                time.sleep(0.1)
                if self._try_connect():
                    logger.info("maru-resource-manager is ready")
                    return

            raise RuntimeError(
                f"maru-resource-manager failed to start within 5s "
                f"(socket: {self._socket_path})"
            )
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            lock_fd.close()

    def _try_connect(self) -> bool:
        """Test if the resource manager socket is connectable."""
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.connect(self._socket_path)
            sock.close()
            return True
        except OSError:
            sock.close()
            return False

    def _connect(self) -> socket.socket:
        """Create a new UDS connection to the resource manager.

        If connection fails, attempts to auto-start the resource manager
        and retries once.
        """
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.connect(self._socket_path)
        except OSError:
            sock.close()
            # Connection failed — resource manager may not be running or crashed
            self._ensure_resource_manager()
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(self._socket_path)
        return sock

    def _send_request(
        self, sock: socket.socket, msg_type: MsgType, payload: bytes
    ) -> None:
        """Send a header + payload to the resource manager."""
        hdr = MsgHeader(msg_type=msg_type, payload_len=len(payload))
        write_full(sock, hdr.pack())
        if payload:
            write_full(sock, payload)

    def _recv_header(self, sock: socket.socket) -> MsgHeader:
        """Receive and validate a response header."""
        data = read_full(sock, HEADER_SIZE)
        hdr = MsgHeader.unpack(data)
        if not hdr.validate():
            raise ConnectionError(f"Invalid response header: magic=0x{hdr.magic:08X}")
        return hdr

    def stats(self) -> list[MaruPoolInfo]:
        """Query pool statistics from the resource manager."""
        sock = self._connect()
        try:
            self._send_request(sock, MsgType.STATS_REQ, StatsReq().pack())
            hdr = self._recv_header(sock)

            if hdr.msg_type == MsgType.ERROR_RESP:
                payload = (
                    read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
                )
                err = ErrorResp.unpack(payload)
                raise RuntimeError(
                    f"Resource manager error ({err.status}): {err.message}"
                )

            payload = read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
            resp = StatsResp.unpack(payload)
            return resp.pools or []
        finally:
            sock.close()

    def register_server(self) -> None:
        """Register this process as an active MaruServer with the resource manager.

        Prevents idle shutdown while the server is running.
        PID is automatically extracted from the UDS connection (SO_PEERCRED).
        """
        sock = self._connect()
        try:
            self._send_request(
                sock, MsgType.REGISTER_SERVER_REQ, RegisterServerReq().pack()
            )
            hdr = self._recv_header(sock)

            if hdr.msg_type == MsgType.ERROR_RESP:
                payload = (
                    read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
                )
                err = ErrorResp.unpack(payload)
                raise RuntimeError(
                    f"Register server failed ({err.status}): {err.message}"
                )

            payload = read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
            resp = RegisterServerResp.unpack(payload)
            if resp.status != 0:
                raise RuntimeError(f"Register server failed with status {resp.status}")

            logger.info("Registered as active server with resource manager")
        finally:
            sock.close()

    def unregister_server(self) -> None:
        """Unregister this process as an active MaruServer.

        Allows idle shutdown to proceed if no allocations remain.
        """
        sock = self._connect()
        try:
            self._send_request(
                sock, MsgType.UNREGISTER_SERVER_REQ, UnregisterServerReq().pack()
            )
            hdr = self._recv_header(sock)

            if hdr.msg_type == MsgType.ERROR_RESP:
                payload = (
                    read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
                )
                err = ErrorResp.unpack(payload)
                raise RuntimeError(
                    f"Unregister server failed ({err.status}): {err.message}"
                )

            payload = read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
            resp = UnregisterServerResp.unpack(payload)
            if resp.status != 0:
                raise RuntimeError(
                    f"Unregister server failed with status {resp.status}"
                )

            logger.info("Unregistered from resource manager")
        finally:
            sock.close()

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
        req = AllocReq(size=size, pool_id=pool_id)
        sock = self._connect()
        try:
            self._send_request(sock, MsgType.ALLOC_REQ, req.pack())
            hdr = self._recv_header(sock)

            if hdr.msg_type == MsgType.ERROR_RESP:
                payload = (
                    read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
                )
                err = ErrorResp.unpack(payload)
                raise RuntimeError(f"Alloc failed ({err.status}): {err.message}")

            # Receive payload with FD via SCM_RIGHTS
            payload, recv_fd = recv_with_fd(sock, hdr.payload_len)
            resp = AllocResp.unpack(payload)

            if resp.status != 0:
                if recv_fd is not None:
                    os.close(recv_fd)
                raise RuntimeError(f"Alloc failed with status {resp.status}")

            if recv_fd is None:
                raise RuntimeError("Alloc succeeded but no FD received")

            handle = resp.handle
            with self._lock:
                self._fd_cache[handle.region_id] = recv_fd

            logger.debug(
                "alloc(size=%d, pool_id=%d) -> region_id=%d",
                size,
                pool_id,
                handle.region_id,
            )
            return handle
        finally:
            sock.close()

    def free(self, handle: MaruHandle) -> None:
        """Free a previously allocated handle.

        Args:
            handle: Handle from a previous alloc() call.
        """
        req = FreeReq(handle=handle)
        sock = self._connect()
        try:
            self._send_request(sock, MsgType.FREE_REQ, req.pack())
            hdr = self._recv_header(sock)

            if hdr.msg_type == MsgType.ERROR_RESP:
                payload = (
                    read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
                )
                err = ErrorResp.unpack(payload)
                raise RuntimeError(f"Free failed ({err.status}): {err.message}")

            payload = read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
            resp = FreeResp.unpack(payload)

            if resp.status != 0:
                raise RuntimeError(f"Free failed with status {resp.status}")

            # Close cached FD and mmap
            with self._lock:
                self._close_region_locked(handle.region_id)

            logger.debug("free(region_id=%d)", handle.region_id)
        finally:
            sock.close()

    def _request_fd(self, handle: MaruHandle) -> int:
        """Request an FD from the resource manager via GET_FD_REQ + SCM_RIGHTS."""
        req = GetFdReq(handle=handle)
        sock = self._connect()
        try:
            self._send_request(sock, MsgType.GET_FD_REQ, req.pack())
            hdr = self._recv_header(sock)

            if hdr.msg_type == MsgType.ERROR_RESP:
                payload = (
                    read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
                )
                err = ErrorResp.unpack(payload)
                raise RuntimeError(f"GetFd failed ({err.status}): {err.message}")

            payload, recv_fd = recv_with_fd(sock, hdr.payload_len)
            resp = GetFdResp.unpack(payload)

            if resp.status != 0:
                if recv_fd is not None:
                    os.close(recv_fd)
                raise RuntimeError(f"GetFd failed with status {resp.status}")

            if recv_fd is None:
                raise RuntimeError("GetFd succeeded but no FD received")

            return recv_fd
        finally:
            sock.close()

    def mmap(self, handle: MaruHandle, prot: int, flags: int = 0) -> mmap_module.mmap:
        """Memory-map a handle into the calling process.

        Args:
            handle: Handle from alloc() or lookup.
            prot: Protection flags (PROT_READ | PROT_WRITE).
            flags: Mapping flags (defaults to MAP_SHARED).

        Returns:
            Python mmap object with buffer protocol support.
        """
        with self._lock:
            # Return cached mmap if available
            if handle.region_id in self._mmap_cache:
                return self._mmap_cache[handle.region_id]

            # Get FD from cache or request from resource manager
            fd = self._fd_cache.get(handle.region_id)
            if fd is None:
                fd = self._request_fd(handle)
                self._fd_cache[handle.region_id] = fd

            if flags == 0:
                flags = MAP_SHARED

            # Convert our prot/flags to Python mmap access mode
            access = mmap_module.ACCESS_READ
            if prot & 0x2:  # PROT_WRITE
                access = mmap_module.ACCESS_WRITE

            mm = mmap_module.mmap(
                fd,
                handle.length,
                access=access,
                offset=handle.offset,
            )

            self._mmap_cache[handle.region_id] = mm

        logger.debug(
            "mmap(region_id=%d, length=%d, offset=%d)",
            handle.region_id,
            handle.length,
            handle.offset,
        )
        return mm

    def munmap(self, handle: MaruHandle) -> None:
        """Unmap a previously mapped handle.

        Args:
            handle: Handle to unmap.
        """
        with self._lock:
            mm = self._mmap_cache.pop(handle.region_id, None)
        if mm is not None:
            mm.close()
        logger.debug("munmap(region_id=%d)", handle.region_id)

    def _close_region_locked(self, region_id: int) -> None:
        """Close mmap and FD for a region (must hold self._lock)."""
        mm = self._mmap_cache.pop(region_id, None)
        if mm is not None:
            mm.close()
        fd = self._fd_cache.pop(region_id, None)
        if fd is not None:
            os.close(fd)

    def close(self) -> None:
        """Close all cached FDs and mmaps."""
        with self._lock:
            num_mmaps = len(self._mmap_cache)
            num_fds = len(self._fd_cache)
            for region_id in list(self._mmap_cache.keys()):
                self._close_region_locked(region_id)
            # Close any remaining FDs without mmaps
            for _region_id, fd in list(self._fd_cache.items()):
                os.close(fd)
            self._fd_cache.clear()
            self._mmap_cache.clear()
        logger.debug("close(): released %d mmaps, %d fds", num_mmaps, num_fds)

    def __del__(self) -> None:
        self.close()
