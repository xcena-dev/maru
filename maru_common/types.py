# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Shared memory types for maru_shm.

Defines MaruHandle, MaruPoolInfo, and PoolType — the core data structures
shared between the Maru Resource Manager (C++) and maru_shm clients.
"""

import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import Any


class DaxType(IntEnum):
    """Pool type."""

    DEV_DAX = 0  # Character device (/dev/daxX.Y)
    FS_DAX = 1  # File-based DAX (mounted filesystem)
    MARUFS = 2  # marufs kernel filesystem (CXL shared memory)
    ANY = 0xFFFFFFFF


# Handle binary layout: 4 x uint64 = 32 bytes, native byte order
_HANDLE_FORMAT = "=QQQQ"
_HANDLE_SIZE = struct.calcsize(_HANDLE_FORMAT)  # 32


@dataclass
class MaruHandle:
    """Handle for an allocation within a pool-backed DAX device.

    Attributes:
        region_id: Global unique region identifier.
        offset: mmap offset (DEV_DAX: real offset, FS_DAX: 0).
        length: Allocation length in bytes (aligned size).
        auth_token: Authorization token for FD requests.
    """

    region_id: int = 0
    offset: int = 0
    length: int = 0
    auth_token: int = 0

    def pack(self) -> bytes:
        """Pack to 32 bytes (native byte order)."""
        return struct.pack(
            _HANDLE_FORMAT,
            self.region_id,
            self.offset,
            self.length,
            self.auth_token,
        )

    @classmethod
    def unpack(cls, data: bytes) -> "MaruHandle":
        """Unpack from 32 bytes."""
        if len(data) < _HANDLE_SIZE:
            raise ValueError(f"Handle data too short: {len(data)} < {_HANDLE_SIZE}")
        region_id, offset, length, auth_token = struct.unpack(
            _HANDLE_FORMAT, data[:_HANDLE_SIZE]
        )
        return cls(
            region_id=region_id,
            offset=offset,
            length=length,
            auth_token=auth_token,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for RPC serialization."""
        return {
            "region_id": self.region_id,
            "offset": self.offset,
            "length": self.length,
            "auth_token": self.auth_token,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "MaruHandle":
        """Create from dict."""
        return cls(
            region_id=d["region_id"],
            offset=d["offset"],
            length=d["length"],
            auth_token=d["auth_token"],
        )

    def __repr__(self) -> str:
        return (
            f"<MaruHandle region_id={self.region_id} offset={self.offset} "
            f"length={self.length} auth_token=***>"
        )


# PoolInfo binary layout: u32 + u32 + u64 + u64 + u64 = 32 bytes
_POOL_INFO_FORMAT = "=IIQQQ"
_POOL_INFO_SIZE = struct.calcsize(_POOL_INFO_FORMAT)  # 32


@dataclass
class MaruPoolInfo:
    """Pool statistics and attributes.

    Attributes:
        pool_id: Pool identifier.
        pool_type: Pool type (DEV_DAX, FS_DAX, or MARUFS).
        total_size: Total pool size in bytes.
        free_size: Free size in bytes.
        align_bytes: Alignment size in bytes used by the pool.
    """

    pool_id: int = 0
    dax_type: DaxType = DaxType.DEV_DAX
    total_size: int = 0
    free_size: int = 0
    align_bytes: int = 0

    def pack(self) -> bytes:
        """Pack to 32 bytes (native byte order)."""
        return struct.pack(
            _POOL_INFO_FORMAT,
            self.pool_id,
            int(self.dax_type),
            self.total_size,
            self.free_size,
            self.align_bytes,
        )

    @classmethod
    def unpack(cls, data: bytes) -> "MaruPoolInfo":
        """Unpack from 32 bytes."""
        if len(data) < _POOL_INFO_SIZE:
            raise ValueError(
                f"PoolInfo data too short: {len(data)} < {_POOL_INFO_SIZE}"
            )
        pool_id, dax_type, total_size, free_size, align_bytes = struct.unpack(
            _POOL_INFO_FORMAT, data[:_POOL_INFO_SIZE]
        )
        return cls(
            pool_id=pool_id,
            dax_type=DaxType(dax_type),
            total_size=total_size,
            free_size=free_size,
            align_bytes=align_bytes,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for RPC serialization."""
        return {
            "pool_id": self.pool_id,
            "dax_type": int(self.dax_type),
            "total_size": self.total_size,
            "free_size": self.free_size,
            "align_bytes": self.align_bytes,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "MaruPoolInfo":
        """Create from dict."""
        return cls(
            pool_id=d["pool_id"],
            dax_type=DaxType(d.get("dax_type", 0)),
            total_size=d["total_size"],
            free_size=d["free_size"],
            align_bytes=d.get("align_bytes", 0),
        )

    def __repr__(self) -> str:
        return (
            f"<MaruPoolInfo pool_id={self.pool_id} dax_type={self.dax_type.name} "
            f"total_size={self.total_size} free_size={self.free_size}>"
        )
