# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Binary IPC protocol for resource manager <-> client communication.

This protocol is used over UDS (Unix Domain Sockets) between the Maru Resource Manager
and MaruShmClient. It is separate from the maru RPC protocol in
maru_common/protocol.py which uses MessagePack over ZMQ.

Wire format::

    [MsgHeader (12 bytes)] [payload (variable)]
"""

import struct
from dataclasses import dataclass
from enum import IntEnum

from .constants import ANY_POOL_ID
from .types import DaxType, MaruHandle, MaruPoolInfo

# Protocol constants
PROTOCOL_MAGIC = 0x4D415255  # 'MARU' in ASCII
PROTOCOL_VERSION = 2

# Maximum payload size (DoS prevention)
MAX_PAYLOAD_SIZE = 8192


class MsgType(IntEnum):
    """IPC message types (resource manager <-> client)."""

    ALLOC_REQ = 1
    ALLOC_RESP = 2
    FREE_REQ = 3
    FREE_RESP = 4
    STATS_REQ = 5
    STATS_RESP = 6
    REGISTER_SERVER_REQ = 7
    REGISTER_SERVER_RESP = 8
    GET_ACCESS_REQ = 9
    GET_ACCESS_RESP = 10
    UNREGISTER_SERVER_REQ = 11
    UNREGISTER_SERVER_RESP = 12
    ERROR_RESP = 255


# MsgHeader: magic(u32) + version(u16) + type(u16) + payload_len(u32) = 12 bytes
_HEADER_FORMAT = "=IHHI"
HEADER_SIZE = struct.calcsize(_HEADER_FORMAT)  # 12


@dataclass
class MsgHeader:
    """IPC message header (12 bytes).

    Wire layout (native byte order)::

        magic:       4 bytes (0x4D415255)
        version:     2 bytes
        msg_type:    2 bytes (MsgType)
        payload_len: 4 bytes
    """

    magic: int = PROTOCOL_MAGIC
    version: int = PROTOCOL_VERSION
    msg_type: int = 0
    payload_len: int = 0

    def pack(self) -> bytes:
        """Pack header to 12 bytes."""
        return struct.pack(
            _HEADER_FORMAT,
            self.magic,
            self.version,
            self.msg_type,
            self.payload_len,
        )

    @classmethod
    def unpack(cls, data: bytes) -> "MsgHeader":
        """Unpack header from 12 bytes."""
        if len(data) < HEADER_SIZE:
            raise ValueError(f"Header too short: {len(data)} < {HEADER_SIZE}")
        magic, version, msg_type, payload_len = struct.unpack(
            _HEADER_FORMAT, data[:HEADER_SIZE]
        )
        return cls(
            magic=magic,
            version=version,
            msg_type=msg_type,
            payload_len=payload_len,
        )

    def validate(self) -> bool:
        """Validate header fields."""
        return self.magic == PROTOCOL_MAGIC and self.version == PROTOCOL_VERSION


# =============================================================================
# Request / Response payloads
# =============================================================================

# AllocReq: size(u64) + pool_id(u32) + reserved(u32) = 16 bytes
_ALLOC_REQ_FORMAT = "=QII"
_ALLOC_REQ_SIZE = struct.calcsize(_ALLOC_REQ_FORMAT)


@dataclass
class AllocReq:
    """Allocation request payload."""

    size: int = 0
    pool_id: int = ANY_POOL_ID
    reserved: int = 0
    client_id: str = ""

    def pack(self) -> bytes:
        fixed = struct.pack(
            _ALLOC_REQ_FORMAT, self.size, self.pool_id, self.reserved
        )
        if self.client_id:
            id_bytes = self.client_id.encode("utf-8")
            return fixed + struct.pack("=H", len(id_bytes)) + id_bytes
        return fixed

    @classmethod
    def unpack(cls, data: bytes) -> "AllocReq":
        if len(data) < _ALLOC_REQ_SIZE:
            raise ValueError(f"AllocReq too short: {len(data)} < {_ALLOC_REQ_SIZE}")
        size, pool_id, reserved = struct.unpack(
            _ALLOC_REQ_FORMAT, data[:_ALLOC_REQ_SIZE]
        )
        client_id = ""
        off = _ALLOC_REQ_SIZE
        if off + 2 <= len(data):
            (id_len,) = struct.unpack("=H", data[off : off + 2])
            off += 2
            if id_len > 0 and off + id_len <= len(data):
                client_id = data[off : off + id_len].decode("utf-8")
        return cls(size=size, pool_id=pool_id, reserved=reserved, client_id=client_id)


# AllocResp: status(i32) + accessType(u32) + Handle(32) + requested_size(u64) = 48 bytes
# accessType: 0=LOCAL (fd via SCM_RIGHTS), 1=REMOTE (future multi-node)
_ALLOC_RESP_FORMAT = "=iIQQQQQ"
_ALLOC_RESP_SIZE = struct.calcsize(_ALLOC_RESP_FORMAT)


@dataclass
class AllocResp:
    """Allocation response payload."""

    status: int = 0
    handle: MaruHandle | None = None
    requested_size: int = 0
    device_path: str = ""

    def pack(self) -> bytes:
        h = self.handle or MaruHandle()
        fixed = struct.pack(
            _ALLOC_RESP_FORMAT,
            self.status,
            0,  # accessType (LOCAL)
            h.region_id,
            h.offset,
            h.length,
            h.auth_token,
            self.requested_size,
        )
        path_bytes = self.device_path.encode("utf-8")
        path_header = struct.pack("=I", len(path_bytes))
        return fixed + path_header + path_bytes

    @classmethod
    def unpack(cls, data: bytes) -> "AllocResp":
        if len(data) < _ALLOC_RESP_SIZE:
            raise ValueError(f"AllocResp too short: {len(data)} < {_ALLOC_RESP_SIZE}")
        vals = struct.unpack(_ALLOC_RESP_FORMAT, data[:_ALLOC_RESP_SIZE])
        status = vals[0]
        # vals[1] is accessType (0=LOCAL, 1=REMOTE)
        handle = MaruHandle(
            region_id=vals[2], offset=vals[3], length=vals[4], auth_token=vals[5]
        )
        requested_size = vals[6]
        # Parse optional device_path extension
        device_path = ""
        offset = _ALLOC_RESP_SIZE
        if offset + 4 <= len(data):
            (path_len,) = struct.unpack("=I", data[offset : offset + 4])
            offset += 4
            if path_len > 0 and offset + path_len <= len(data):
                device_path = data[offset : offset + path_len].decode("utf-8")
        return cls(
            status=status,
            handle=handle,
            requested_size=requested_size,
            device_path=device_path,
        )


# FreeReq: Handle(32) = 32 bytes
_FREE_REQ_FORMAT = "=QQQQ"
_FREE_REQ_SIZE = struct.calcsize(_FREE_REQ_FORMAT)


@dataclass
class FreeReq:
    """Free request payload."""

    handle: MaruHandle | None = None

    def pack(self) -> bytes:
        h = self.handle or MaruHandle()
        return h.pack()

    @classmethod
    def unpack(cls, data: bytes) -> "FreeReq":
        handle = MaruHandle.unpack(data)
        return cls(handle=handle)


# FreeResp: status(i32) = 4 bytes
_FREE_RESP_FORMAT = "=i"
_FREE_RESP_SIZE = struct.calcsize(_FREE_RESP_FORMAT)


@dataclass
class FreeResp:
    """Free response payload."""

    status: int = 0

    def pack(self) -> bytes:
        return struct.pack(_FREE_RESP_FORMAT, self.status)

    @classmethod
    def unpack(cls, data: bytes) -> "FreeResp":
        if len(data) < _FREE_RESP_SIZE:
            raise ValueError(f"FreeResp too short: {len(data)} < {_FREE_RESP_SIZE}")
        (status,) = struct.unpack(_FREE_RESP_FORMAT, data[:_FREE_RESP_SIZE])
        return cls(status=status)


# GetAccessReq: Handle(32) = 32 bytes
@dataclass
class GetAccessReq:
    """Get access info request payload."""

    handle: MaruHandle | None = None

    def pack(self) -> bytes:
        h = self.handle or MaruHandle()
        return h.pack()

    @classmethod
    def unpack(cls, data: bytes) -> "GetAccessReq":
        handle = MaruHandle.unpack(data)
        return cls(handle=handle)


# GetAccessResp: status(i32) + pathLen(u32) + path + offset(u64) + length(u64)
_GET_ACCESS_RESP_HEADER_FORMAT = "=iI"
_GET_ACCESS_RESP_HEADER_SIZE = struct.calcsize(_GET_ACCESS_RESP_HEADER_FORMAT)


@dataclass
class GetAccessResp:
    """Get access info response — device path, offset, length."""

    status: int = 0
    device_path: str = ""
    offset: int = 0
    length: int = 0

    def pack(self) -> bytes:
        path_bytes = self.device_path.encode("utf-8")
        header = struct.pack(
            _GET_ACCESS_RESP_HEADER_FORMAT, self.status, len(path_bytes)
        )
        tail = struct.pack("=QQ", self.offset, self.length)
        return header + path_bytes + tail

    @classmethod
    def unpack(cls, data: bytes) -> "GetAccessResp":
        if len(data) < _GET_ACCESS_RESP_HEADER_SIZE:
            raise ValueError("GetAccessResp too short for header")
        status, path_len = struct.unpack(
            _GET_ACCESS_RESP_HEADER_FORMAT, data[:_GET_ACCESS_RESP_HEADER_SIZE]
        )
        off = _GET_ACCESS_RESP_HEADER_SIZE
        device_path = ""
        if path_len > 0 and off + path_len <= len(data):
            device_path = data[off : off + path_len].decode("utf-8")
            off += path_len
        offset = 0
        length = 0
        if off + 16 <= len(data):
            offset, length = struct.unpack("=QQ", data[off : off + 16])
        return cls(
            status=status, device_path=device_path, offset=offset, length=length
        )


# StatsReq: empty payload (0 bytes)
@dataclass
class StatsReq:
    """Stats request payload (empty)."""

    def pack(self) -> bytes:
        return b""

    @classmethod
    def unpack(cls, data: bytes) -> "StatsReq":
        return cls()


# StatsResp: num_pools(u32) + PoolInfo[num_pools]
_STATS_RESP_HEADER_FORMAT = "=I"
_STATS_RESP_HEADER_SIZE = struct.calcsize(_STATS_RESP_HEADER_FORMAT)

# PoolInfo wire format for stats: pool_id(u32) + dax_type(u32) + total(u64) + free(u64) + align(u64)
_STATS_POOL_FORMAT = "=IIQQQ"
_STATS_POOL_SIZE = struct.calcsize(_STATS_POOL_FORMAT)


@dataclass
class StatsResp:
    """Stats response payload."""

    pools: list[MaruPoolInfo] | None = None

    def pack(self) -> bytes:
        pools = self.pools or []
        parts = [struct.pack(_STATS_RESP_HEADER_FORMAT, len(pools))]
        for p in pools:
            parts.append(
                struct.pack(
                    _STATS_POOL_FORMAT,
                    p.pool_id,
                    int(p.dax_type),
                    p.total_size,
                    p.free_size,
                    p.align_bytes,
                )
            )
        return b"".join(parts)

    @classmethod
    def unpack(cls, data: bytes) -> "StatsResp":
        if len(data) < _STATS_RESP_HEADER_SIZE:
            raise ValueError("StatsResp too short for header")
        (num_pools,) = struct.unpack(
            _STATS_RESP_HEADER_FORMAT, data[:_STATS_RESP_HEADER_SIZE]
        )
        offset = _STATS_RESP_HEADER_SIZE
        pools: list[MaruPoolInfo] = []
        for _ in range(num_pools):
            if offset + _STATS_POOL_SIZE > len(data):
                raise ValueError("StatsResp truncated")
            pool_id, dax_type, total_size, free_size, align_bytes = struct.unpack(
                _STATS_POOL_FORMAT, data[offset : offset + _STATS_POOL_SIZE]
            )
            pools.append(
                MaruPoolInfo(
                    pool_id=pool_id,
                    dax_type=DaxType(dax_type),
                    total_size=total_size,
                    free_size=free_size,
                    align_bytes=align_bytes,
                )
            )
            offset += _STATS_POOL_SIZE
        return cls(pools=pools)


# RegisterServerReq: optional client_id for TCP (PID no longer from SO_PEERCRED)
@dataclass
class RegisterServerReq:
    """Register server request payload."""

    client_id: str = ""

    def pack(self) -> bytes:
        if self.client_id:
            id_bytes = self.client_id.encode("utf-8")
            return struct.pack("=H", len(id_bytes)) + id_bytes
        return b""

    @classmethod
    def unpack(cls, data: bytes) -> "RegisterServerReq":
        client_id = ""
        if len(data) >= 2:
            (id_len,) = struct.unpack("=H", data[:2])
            if id_len > 0 and 2 + id_len <= len(data):
                client_id = data[2 : 2 + id_len].decode("utf-8")
        return cls(client_id=client_id)


# RegisterServerResp: status(i32) = 4 bytes
_REGISTER_SERVER_RESP_FORMAT = "=i"
_REGISTER_SERVER_RESP_SIZE = struct.calcsize(_REGISTER_SERVER_RESP_FORMAT)


@dataclass
class RegisterServerResp:
    """Register server response payload."""

    status: int = 0

    def pack(self) -> bytes:
        return struct.pack(_REGISTER_SERVER_RESP_FORMAT, self.status)

    @classmethod
    def unpack(cls, data: bytes) -> "RegisterServerResp":
        if len(data) < _REGISTER_SERVER_RESP_SIZE:
            raise ValueError(
                f"RegisterServerResp too short: {len(data)} < {_REGISTER_SERVER_RESP_SIZE}"
            )
        (status,) = struct.unpack(
            _REGISTER_SERVER_RESP_FORMAT, data[:_REGISTER_SERVER_RESP_SIZE]
        )
        return cls(status=status)


# UnregisterServerReq: optional client_id for TCP
@dataclass
class UnregisterServerReq:
    """Unregister server request payload."""

    client_id: str = ""

    def pack(self) -> bytes:
        if self.client_id:
            id_bytes = self.client_id.encode("utf-8")
            return struct.pack("=H", len(id_bytes)) + id_bytes
        return b""

    @classmethod
    def unpack(cls, data: bytes) -> "UnregisterServerReq":
        client_id = ""
        if len(data) >= 2:
            (id_len,) = struct.unpack("=H", data[:2])
            if id_len > 0 and 2 + id_len <= len(data):
                client_id = data[2 : 2 + id_len].decode("utf-8")
        return cls(client_id=client_id)


# UnregisterServerResp: status(i32) = 4 bytes
_UNREGISTER_SERVER_RESP_FORMAT = "=i"
_UNREGISTER_SERVER_RESP_SIZE = struct.calcsize(_UNREGISTER_SERVER_RESP_FORMAT)


@dataclass
class UnregisterServerResp:
    """Unregister server response payload."""

    status: int = 0

    def pack(self) -> bytes:
        return struct.pack(_UNREGISTER_SERVER_RESP_FORMAT, self.status)

    @classmethod
    def unpack(cls, data: bytes) -> "UnregisterServerResp":
        if len(data) < _UNREGISTER_SERVER_RESP_SIZE:
            raise ValueError(
                f"UnregisterServerResp too short: {len(data)} < {_UNREGISTER_SERVER_RESP_SIZE}"
            )
        (status,) = struct.unpack(
            _UNREGISTER_SERVER_RESP_FORMAT, data[:_UNREGISTER_SERVER_RESP_SIZE]
        )
        return cls(status=status)


# ErrorResp: status(i32) + msg_len(u32) + message(variable)
_ERROR_RESP_HEADER_FORMAT = "=iI"
_ERROR_RESP_HEADER_SIZE = struct.calcsize(_ERROR_RESP_HEADER_FORMAT)


@dataclass
class ErrorResp:
    """Error response payload."""

    status: int = 0
    message: str = ""

    def pack(self) -> bytes:
        msg_bytes = self.message.encode("utf-8")
        return (
            struct.pack(_ERROR_RESP_HEADER_FORMAT, self.status, len(msg_bytes))
            + msg_bytes
        )

    @classmethod
    def unpack(cls, data: bytes) -> "ErrorResp":
        if len(data) < _ERROR_RESP_HEADER_SIZE:
            raise ValueError("ErrorResp too short for header")
        status, msg_len = struct.unpack(
            _ERROR_RESP_HEADER_FORMAT, data[:_ERROR_RESP_HEADER_SIZE]
        )
        msg_start = _ERROR_RESP_HEADER_SIZE
        message = data[msg_start : msg_start + msg_len].decode(
            "utf-8", errors="replace"
        )
        return cls(status=status, message=message)
