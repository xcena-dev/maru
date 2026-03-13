# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Maru Protocol - Message definitions for RPC communication.

This module defines the protocol for communication between Maru
clients and the MaruServer using binary format (MessagePack).

Binary Format::

    [Header (16 bytes)] [Payload (MessagePack encoded)]

See docs/protocol.md for full specification.
"""

import struct
from dataclasses import dataclass, field
from enum import IntEnum, IntFlag

from maru_shm.constants import ANY_POOL_ID
from maru_shm.types import MaruHandle

# =============================================================================
# Protocol Constants
# =============================================================================

PROTOCOL_MAGIC = 0xCF00  # "CF" for Maru-Frame
PROTOCOL_VERSION = 1
HEADER_SIZE = 16
HEADER_FORMAT = "!HBBBBBBII"  # network byte order, big-endian
# Layout: magic(2) + version(1) + msg_type(1) + flags(1) + reserved(3) + seq(4) + len(4)


# =============================================================================
# Message Types
# =============================================================================


class MessageType(IntEnum):
    """Message type codes for the protocol."""

    # Allocation Management (0x01 - 0x0F)
    REQUEST_ALLOC = 0x01
    RETURN_ALLOC = 0x03
    LIST_ALLOCATIONS = 0x04

    # KV Operations (0x10 - 0x1F)
    REGISTER_KV = 0x10
    LOOKUP_KV = 0x11
    EXISTS_KV = 0x12
    DELETE_KV = 0x13

    # Batch Operations (0x20 - 0x2F)
    BATCH_REGISTER_KV = 0x20
    BATCH_LOOKUP_KV = 0x21
    BATCH_EXISTS_KV = 0x22

    # Admin (0xF0 - 0xFF)
    GET_STATS = 0xF0
    HEARTBEAT = 0xF1
    HANDSHAKE = 0xFE
    SHUTDOWN = 0xFF

    # Response flag (OR with request type)
    RESPONSE = 0x80

    @classmethod
    def is_response(cls, msg_type: int) -> bool:
        """Check if message type is a response."""
        return bool(msg_type & cls.RESPONSE)

    @classmethod
    def get_request_type(cls, msg_type: int) -> int:
        """Get the request type from a response type."""
        return msg_type & ~cls.RESPONSE


class MessageFlags(IntFlag):
    """Message flags bitmap (1 byte)."""

    NONE = 0x00
    BATCH = 0x01  # Batch request
    COMPRESSED = 0x02  # zstd compressed payload
    URGENT = 0x04  # Priority processing
    NO_REPLY = 0x08  # Fire-and-forget (no response expected)


# =============================================================================
# Message Header
# =============================================================================


@dataclass
class MessageHeader:
    """
    Protocol message header (16 bytes).

    Format (network byte order)::

        magic: 2 bytes - 0xCF00
        version: 1 byte - Protocol version (currently 1)
        msg_type: 1 byte - MessageType code
        flags: 1 byte - MessageFlags bitmap
        reserved: 3 bytes - Future use (set to 0)
        sequence: 4 bytes - Request/response matching
        payload_length: 4 bytes - Payload size in bytes
    """

    magic: int = PROTOCOL_MAGIC
    version: int = PROTOCOL_VERSION
    msg_type: int = 0
    flags: int = MessageFlags.NONE
    sequence: int = 0
    payload_length: int = 0

    def pack(self) -> bytes:
        """Pack header to bytes."""
        return struct.pack(
            HEADER_FORMAT,
            self.magic,
            self.version,
            self.msg_type,
            self.flags,
            0,
            0,
            0,  # reserved (3 bytes)
            self.sequence,
            self.payload_length,
        )

    @classmethod
    def unpack(cls, data: bytes) -> "MessageHeader":
        """Unpack header from bytes."""
        if len(data) < HEADER_SIZE:
            raise ValueError(f"Header too short: {len(data)} < {HEADER_SIZE}")
        values = struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])
        return cls(
            magic=values[0],
            version=values[1],
            msg_type=values[2],
            flags=values[3],
            # values[4], [5], [6] are reserved
            sequence=values[7],
            payload_length=values[8],
        )

    def validate(self) -> bool:
        """Validate header fields."""
        return self.magic == PROTOCOL_MAGIC and self.version == PROTOCOL_VERSION


# =============================================================================
# Allocation Management Messages (0x01 - 0x0F)
# =============================================================================


@dataclass
class RequestAllocRequest:
    """REQUEST_ALLOC (0x01) - Request memory allocation."""

    instance_id: str
    size: int
    pool_id: int = ANY_POOL_ID


@dataclass
class RequestAllocResponse:
    """Response for REQUEST_ALLOC."""

    success: bool
    handle: MaruHandle | None = None
    error: str | None = None


@dataclass
class ReturnAllocRequest:
    """RETURN_ALLOC (0x03) - Return allocation to server."""

    instance_id: str
    region_id: int


@dataclass
class ReturnAllocResponse:
    """Response for RETURN_ALLOC."""

    success: bool
    error: str | None = None


@dataclass
class ListAllocationsRequest:
    """LIST_ALLOCATIONS (0x04) - List all active allocations."""

    exclude_instance_id: str | None = None


@dataclass
class ListAllocationsResponse:
    """Response for LIST_ALLOCATIONS."""

    success: bool
    allocations: list[MaruHandle] = field(default_factory=list)
    error: str | None = None


# =============================================================================
# KV Operations Messages (0x10 - 0x1F)
# =============================================================================


@dataclass
class RegisterKVRequest:
    """REGISTER_KV (0x10) - Register KV entry."""

    key: str  # chunk key string (e.g. CacheEngineKey.to_string())
    region_id: int  # Handle's region_id
    kv_offset: int  # offset within allocation
    kv_length: int  # KV data size


@dataclass
class RegisterKVResponse:
    """Response for REGISTER_KV."""

    success: bool
    is_new: bool = False  # True if newly registered


@dataclass
class LookupKVRequest:
    """LOOKUP_KV (0x11) - Lookup KV entry by key."""

    key: str


@dataclass
class LookupKVResponse:
    """Response for LOOKUP_KV."""

    found: bool
    handle: MaruHandle | None = None  # Original allocation handle (for mmap)
    kv_offset: int = 0  # Offset within allocation
    kv_length: int = 0  # KV data size


@dataclass
class ExistsKVRequest:
    """EXISTS_KV (0x12) - Check if KV entry exists."""

    key: str


@dataclass
class ExistsKVResponse:
    """Response for EXISTS_KV."""

    exists: bool


@dataclass
class DeleteKVRequest:
    """DELETE_KV (0x13) - Delete KV entry."""

    key: str


@dataclass
class DeleteKVResponse:
    """Response for DELETE_KV."""

    success: bool


# =============================================================================
# Batch Operations Messages (0x20 - 0x2F)
# =============================================================================


@dataclass
class BatchKVEntry:
    """Single KV entry for batch operations."""

    key: str
    region_id: int
    kv_offset: int
    kv_length: int


@dataclass
class BatchRegisterKVRequest:
    """BATCH_REGISTER_KV (0x20) - Batch register KV entries."""

    entries: list[BatchKVEntry] = field(default_factory=list)


@dataclass
class BatchRegisterKVResponse:
    """Response for BATCH_REGISTER_KV."""

    success: bool
    results: list[bool] = field(default_factory=list)  # is_new for each entry


@dataclass
class BatchLookupKVRequest:
    """BATCH_LOOKUP_KV (0x21) - Batch lookup KV entries."""

    keys: list[str] = field(default_factory=list)


@dataclass
class LookupResult:
    """Single lookup result."""

    found: bool
    handle: MaruHandle | None = None
    kv_offset: int = 0
    kv_length: int = 0


@dataclass
class BatchLookupKVResponse:
    """Response for BATCH_LOOKUP_KV."""

    entries: list[LookupResult] = field(default_factory=list)


@dataclass
class BatchExistsKVRequest:
    """BATCH_EXISTS_KV (0x22) - Batch check KV existence."""

    keys: list[str] = field(default_factory=list)


@dataclass
class BatchExistsKVResponse:
    """Response for BATCH_EXISTS_KV."""

    results: list[bool] = field(default_factory=list)


# =============================================================================
# Admin Messages (0xF0 - 0xFF)
# =============================================================================


@dataclass
class GetStatsRequest:
    """GET_STATS (0xF0) - Request server statistics."""

    pass


@dataclass
class KVManagerStats:
    """KV manager statistics."""

    total_entries: int = 0
    total_size: int = 0


@dataclass
class AllocationManagerStats:
    """Allocation manager statistics."""

    num_allocations: int = 0
    total_allocated: int = 0
    active_clients: int = 0


@dataclass
class GetStatsResponse:
    """Response for GET_STATS."""

    kv_manager: KVManagerStats = field(default_factory=KVManagerStats)
    allocation_manager: AllocationManagerStats = field(
        default_factory=AllocationManagerStats
    )


@dataclass
class HeartbeatRequest:
    """HEARTBEAT (0xF1) - Connection keepalive."""

    pass


@dataclass
class HeartbeatResponse:
    """Response for HEARTBEAT."""

    pass


@dataclass
class HandshakeRequest:
    """HANDSHAKE (0xFE) - Initial connection handshake."""

    client_version: int = PROTOCOL_VERSION
    instance_id: str | None = None


@dataclass
class HandshakeResponse:
    """Response for HANDSHAKE."""

    success: bool
    server_version: int = PROTOCOL_VERSION
    error: str | None = None


@dataclass
class ShutdownRequest:
    """SHUTDOWN (0xFF) - Request server shutdown."""

    pass


@dataclass
class ShutdownResponse:
    """Response for SHUTDOWN."""

    success: bool


# =============================================================================
# Message Type Mapping
# =============================================================================

# Map message types to request/response classes
MESSAGE_CLASSES = {
    # Allocation Management
    MessageType.REQUEST_ALLOC: (RequestAllocRequest, RequestAllocResponse),
    MessageType.RETURN_ALLOC: (ReturnAllocRequest, ReturnAllocResponse),
    MessageType.LIST_ALLOCATIONS: (ListAllocationsRequest, ListAllocationsResponse),
    # KV Operations
    MessageType.REGISTER_KV: (RegisterKVRequest, RegisterKVResponse),
    MessageType.LOOKUP_KV: (LookupKVRequest, LookupKVResponse),
    MessageType.EXISTS_KV: (ExistsKVRequest, ExistsKVResponse),
    MessageType.DELETE_KV: (DeleteKVRequest, DeleteKVResponse),
    # Batch Operations
    MessageType.BATCH_REGISTER_KV: (BatchRegisterKVRequest, BatchRegisterKVResponse),
    MessageType.BATCH_LOOKUP_KV: (BatchLookupKVRequest, BatchLookupKVResponse),
    MessageType.BATCH_EXISTS_KV: (BatchExistsKVRequest, BatchExistsKVResponse),
    # Admin
    MessageType.GET_STATS: (GetStatsRequest, GetStatsResponse),
    MessageType.HEARTBEAT: (HeartbeatRequest, HeartbeatResponse),
    MessageType.HANDSHAKE: (HandshakeRequest, HandshakeResponse),
    MessageType.SHUTDOWN: (ShutdownRequest, ShutdownResponse),
}
