# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Shared memory types for maru_shm.

Defines MaruHandle, MaruPoolInfo, and DaxType — the core data structures
shared between the Maru Resource Manager (C++) and maru_shm clients.
"""

import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import Any


class DaxType(IntEnum):
    """DAX device type."""

    DEV_DAX = 0  # Character device (/dev/daxX.Y)
    FS_DAX = 1  # File-based DAX (mounted filesystem)


# Handle binary layout: 4 x uint64 = 32 bytes, little-endian byte order
_HANDLE_FORMAT = "<QQQQ"
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
_POOL_INFO_FORMAT = "<IIQQQ"
_POOL_INFO_SIZE = struct.calcsize(_POOL_INFO_FORMAT)  # 32


@dataclass
class MaruPoolInfo:
    """Pool statistics and attributes.

    Attributes:
        device_path: DAX device path (e.g. '/dev/dax0.0').
        dax_type: DAX device type (DEV_DAX or FS_DAX).
        total_size: Total pool size in bytes.
        free_size: Free size in bytes.
        align_bytes: Alignment size in bytes used by the pool.
    """

    device_path: str = ""
    dax_type: DaxType = DaxType.DEV_DAX
    total_size: int = 0
    free_size: int = 0
    align_bytes: int = 0

    def pack(self) -> bytes:
        """Pack to fixed header (32 bytes) + variable device path bytes."""
        path_bytes = self.device_path.encode("utf-8")
        return struct.pack(
            _POOL_INFO_FORMAT,
            len(path_bytes),
            int(self.dax_type),
            self.total_size,
            self.free_size,
            self.align_bytes,
        ) + path_bytes

    @classmethod
    def unpack(cls, data: bytes) -> "MaruPoolInfo":
        """Unpack from fixed header (32 bytes) + variable device path bytes."""
        if len(data) < _POOL_INFO_SIZE:
            raise ValueError(
                f"PoolInfo data too short: {len(data)} < {_POOL_INFO_SIZE}"
            )
        dev_path_len, dax_type, total_size, free_size, align_bytes = struct.unpack(
            _POOL_INFO_FORMAT, data[:_POOL_INFO_SIZE]
        )
        device_path = ""
        if dev_path_len > 0:
            if len(data) < _POOL_INFO_SIZE + dev_path_len:
                raise ValueError(
                    f"PoolInfo device_path truncated: need {dev_path_len} bytes"
                )
            device_path = data[_POOL_INFO_SIZE : _POOL_INFO_SIZE + dev_path_len].decode(
                "utf-8"
            )
        return cls(
            device_path=device_path,
            dax_type=DaxType(dax_type),
            total_size=total_size,
            free_size=free_size,
            align_bytes=align_bytes,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for RPC serialization."""
        return {
            "device_path": self.device_path,
            "dax_type": int(self.dax_type),
            "total_size": self.total_size,
            "free_size": self.free_size,
            "align_bytes": self.align_bytes,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "MaruPoolInfo":
        """Create from dict."""
        return cls(
            device_path=d.get("device_path", ""),
            dax_type=DaxType(d.get("dax_type", 0)),
            total_size=d["total_size"],
            free_size=d["free_size"],
            align_bytes=d.get("align_bytes", 0),
        )

    def __repr__(self) -> str:
        return (
            f"<MaruPoolInfo device_path={self.device_path!r} dax_type={self.dax_type.name} "
            f"total_size={self.total_size} free_size={self.free_size}>"
        )
