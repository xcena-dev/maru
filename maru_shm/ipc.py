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

from .types import DaxType, MaruHandle, MaruPoolInfo

# Protocol constants
PROTOCOL_MAGIC = 0x4D415255  # 'MARU' in ASCII
PROTOCOL_VERSION = 1

# Maximum payload size (DoS prevention)
MAX_PAYLOAD_SIZE = 1024


class MsgType(IntEnum):
    """IPC message types (resource manager <-> client)."""

    ALLOC_REQ = 1
    ALLOC_RESP = 2
    FREE_REQ = 3
    FREE_RESP = 4
    STATS_REQ = 5
    STATS_RESP = 6
    GET_FD_REQ = 9
    GET_FD_RESP = 10
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
    pool_id: int = 0xFFFFFFFF  # ANY_POOL_ID
    reserved: int = 0

    def pack(self) -> bytes:
        return struct.pack(_ALLOC_REQ_FORMAT, self.size, self.pool_id, self.reserved)

    @classmethod
    def unpack(cls, data: bytes) -> "AllocReq":
        if len(data) < _ALLOC_REQ_SIZE:
            raise ValueError(f"AllocReq too short: {len(data)} < {_ALLOC_REQ_SIZE}")
        size, pool_id, reserved = struct.unpack(
            _ALLOC_REQ_FORMAT, data[:_ALLOC_REQ_SIZE]
        )
        return cls(size=size, pool_id=pool_id, reserved=reserved)


# AllocResp: status(i32) + pad(4) + Handle(32) + requested_size(u64) = 48 bytes
_ALLOC_RESP_FORMAT = "=iIQQQQQ"
_ALLOC_RESP_SIZE = struct.calcsize(_ALLOC_RESP_FORMAT)


@dataclass
class AllocResp:
    """Allocation response payload."""

    status: int = 0
    handle: MaruHandle | None = None
    requested_size: int = 0

    def pack(self) -> bytes:
        h = self.handle or MaruHandle()
        return struct.pack(
            _ALLOC_RESP_FORMAT,
            self.status,
            0,  # pad
            h.region_id,
            h.offset,
            h.length,
            h.auth_token,
            self.requested_size,
        )

    @classmethod
    def unpack(cls, data: bytes) -> "AllocResp":
        if len(data) < _ALLOC_RESP_SIZE:
            raise ValueError(f"AllocResp too short: {len(data)} < {_ALLOC_RESP_SIZE}")
        vals = struct.unpack(_ALLOC_RESP_FORMAT, data[:_ALLOC_RESP_SIZE])
        status = vals[0]
        # vals[1] is pad
        handle = MaruHandle(
            region_id=vals[2], offset=vals[3], length=vals[4], auth_token=vals[5]
        )
        requested_size = vals[6]
        return cls(status=status, handle=handle, requested_size=requested_size)


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


# GetFdReq: Handle(32) = 32 bytes
@dataclass
class GetFdReq:
    """Get FD request payload (for SCM_RIGHTS)."""

    handle: MaruHandle | None = None

    def pack(self) -> bytes:
        h = self.handle or MaruHandle()
        return h.pack()

    @classmethod
    def unpack(cls, data: bytes) -> "GetFdReq":
        handle = MaruHandle.unpack(data)
        return cls(handle=handle)


# GetFdResp: status(i32) = 4 bytes
_GET_FD_RESP_FORMAT = "=i"
_GET_FD_RESP_SIZE = struct.calcsize(_GET_FD_RESP_FORMAT)


@dataclass
class GetFdResp:
    """Get FD response payload. The actual FD is sent via SCM_RIGHTS ancillary."""

    status: int = 0

    def pack(self) -> bytes:
        return struct.pack(_GET_FD_RESP_FORMAT, self.status)

    @classmethod
    def unpack(cls, data: bytes) -> "GetFdResp":
        if len(data) < _GET_FD_RESP_SIZE:
            raise ValueError(f"GetFdResp too short: {len(data)} < {_GET_FD_RESP_SIZE}")
        (status,) = struct.unpack(_GET_FD_RESP_FORMAT, data[:_GET_FD_RESP_SIZE])
        return cls(status=status)


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
