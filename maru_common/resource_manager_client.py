# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Common resource manager client for UDS IPC communication.

Provides base UDS transport and marufs permission/chown operations.
Used by both MaruShmClient and MarufsClient.
"""

import logging
import os
import socket

from maru_common.ipc import (
    ANY_POOL_ID,
    DEFAULT_SOCKET_PATH,
    HEADER_SIZE,
    AllocReq,
    AllocResp,
    ChownReq,
    ErrorResp,
    FreeReq,
    FreeResp,
    GetFdReq,
    GetFdResp,
    MsgHeader,
    MsgType,
    PermGrantReq,
    PermResp,
    PermRevokeReq,
    PermSetDefaultReq,
    StatsReq,
    StatsResp,
)
from maru_common.types import DaxType, MaruHandle, MaruPoolInfo
from maru_common.uds_helpers import read_full, recv_with_fd, write_full

logger = logging.getLogger(__name__)


class ResourceManagerClient:
    """Base client for communicating with the Maru Resource Manager over UDS.

    Provides low-level transport (connect/send/recv) and marufs permission
    delegation methods. MaruShmClient and MarufsClient can use this directly.
    """

    def __init__(self, socket_path: str | None = None):
        self._socket_path = socket_path or DEFAULT_SOCKET_PATH

    def _connect(self) -> socket.socket:
        """Create a new UDS connection to the resource manager."""
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.connect(self._socket_path)
        except OSError:
            sock.close()
            raise
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
            raise ConnectionError(
                f"Invalid response header: magic=0x{hdr.magic:08X}"
            )
        return hdr

    def _check_error(self, sock: socket.socket, hdr: MsgHeader, op: str) -> None:
        """Check if response is an error and raise if so."""
        if hdr.msg_type == MsgType.ERROR_RESP:
            payload = (
                read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
            )
            err = ErrorResp.unpack(payload)
            raise RuntimeError(f"{op} failed ({err.status}): {err.message}")

    def _perm_request(self, msg_type: MsgType, payload: bytes, op: str) -> int:
        """Send a perm/chown request and return status."""
        sock = self._connect()
        try:
            self._send_request(sock, msg_type, payload)
            hdr = self._recv_header(sock)
            self._check_error(sock, hdr, op)
            resp_data = (
                read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
            )
            resp = PermResp.unpack(resp_data)
            if resp.status != 0:
                raise RuntimeError(f"{op} failed with status {resp.status}")
            return resp.status
        finally:
            sock.close()

    # ── stats ─────────────────────────────────────────────────────────

    def stats(self) -> list[MaruPoolInfo]:
        """Query pool statistics from the resource manager."""
        sock = self._connect()
        try:
            self._send_request(sock, MsgType.STATS_REQ, StatsReq().pack())
            hdr = self._recv_header(sock)
            self._check_error(sock, hdr, "stats")
            payload = (
                read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
            )
            resp = StatsResp.unpack(payload)
            return resp.pools or []
        finally:
            sock.close()

    # ── allocation ─────────────────────────────────────────────────────

    def alloc(self, size: int, pool_id: int = ANY_POOL_ID, pool_type: int = DaxType.ANY) -> tuple[MaruHandle, int]:
        """Allocate a region via the resource manager.

        Returns:
            (handle, fd) — MaruHandle and the open file descriptor for the region.
        """
        req = AllocReq(size=size, pool_id=pool_id, pool_type=pool_type)
        sock = self._connect()
        try:
            self._send_request(sock, MsgType.ALLOC_REQ, req.pack())
            hdr = self._recv_header(sock)
            self._check_error(sock, hdr, "alloc")
            payload, recv_fd = recv_with_fd(sock, hdr.payload_len)
            resp = AllocResp.unpack(payload)
            if resp.status != 0:
                if recv_fd is not None:
                    os.close(recv_fd)
                raise RuntimeError(f"alloc failed with status {resp.status}")
            if recv_fd is None:
                raise RuntimeError("alloc succeeded but no FD received")
            return resp.handle, recv_fd
        finally:
            sock.close()

    def free(self, handle: MaruHandle) -> None:
        """Free a region via the resource manager."""
        req = FreeReq(handle=handle)
        sock = self._connect()
        try:
            self._send_request(sock, MsgType.FREE_REQ, req.pack())
            hdr = self._recv_header(sock)
            self._check_error(sock, hdr, "free")
            payload = read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
            resp = FreeResp.unpack(payload)
            if resp.status != 0:
                raise RuntimeError(f"free failed with status {resp.status}")
        finally:
            sock.close()

    def get_fd(self, handle: MaruHandle) -> int:
        """Request an FD for a region via GET_FD_REQ + SCM_RIGHTS.

        Returns:
            Open file descriptor for the region.
        """
        req = GetFdReq(handle=handle)
        sock = self._connect()
        try:
            self._send_request(sock, MsgType.GET_FD_REQ, req.pack())
            hdr = self._recv_header(sock)
            self._check_error(sock, hdr, "get_fd")
            payload, recv_fd = recv_with_fd(sock, hdr.payload_len)
            resp = GetFdResp.unpack(payload)
            if resp.status != 0:
                if recv_fd is not None:
                    os.close(recv_fd)
                raise RuntimeError(f"get_fd failed with status {resp.status}")
            if recv_fd is None:
                raise RuntimeError("get_fd succeeded but no FD received")
            return recv_fd
        finally:
            sock.close()

    # ── marufs permission delegation ──────────────────────────────────

    def perm_grant(
        self, region_id: int, node_id: int, pid: int, perms: int
    ) -> None:
        """Grant permissions on a marufs region via the resource manager."""
        req = PermGrantReq(
            region_id=region_id, node_id=node_id, pid=pid, perms=perms
        )
        self._perm_request(MsgType.PERM_GRANT_REQ, req.pack(), "perm_grant")

    def perm_revoke(self, region_id: int, node_id: int, pid: int) -> None:
        """Revoke permissions on a marufs region via the resource manager."""
        req = PermRevokeReq(region_id=region_id, node_id=node_id, pid=pid)
        self._perm_request(MsgType.PERM_REVOKE_REQ, req.pack(), "perm_revoke")

    def perm_set_default(self, region_id: int, perms: int) -> None:
        """Set default permissions on a marufs region via the resource manager."""
        req = PermSetDefaultReq(region_id=region_id, perms=perms)
        self._perm_request(
            MsgType.PERM_SET_DEFAULT_REQ, req.pack(), "perm_set_default"
        )

    def chown(self, region_id: int) -> None:
        """Transfer ownership of a marufs region via the resource manager."""
        req = ChownReq(region_id=region_id)
        self._perm_request(MsgType.CHOWN_REQ, req.pack(), "chown")
