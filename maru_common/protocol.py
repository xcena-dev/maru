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
from enum import IntEnum, IntFlag, unique

from maru_shm.types import MaruHandle

from .storage_types import (
    AccessHints,
    BlockRequest,
    CloseMode,
    ExitCause,
    ItemOutcome,
    Key,
    KeyLocation,
    Location,
    OpRef,
    OrphanWorker,
    PoolInfo,
    PoolUsage,
    RmDomain,
    SessionRole,
    Status,
    WorkerRef,
)

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


@unique
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
    PIN_KV = 0x14
    UNPIN_KV = 0x15

    # Batch Operations (0x20 - 0x2F)
    BATCH_REGISTER_KV = 0x20
    BATCH_LOOKUP_KV = 0x21
    BATCH_EXISTS_KV = 0x22
    BATCH_PIN_KV = 0x23
    BATCH_UNPIN_KV = 0x24

    # 0x30 reserved: removed storage-session protocol

    # Multi-storage (0x40 - 0x5F)
    NODE_OPEN = 0x40
    NODE_HEARTBEAT = 0x41
    NODE_CLOSE = 0x42
    NODE_RECOVER_LIST = 0x44
    WORKER_EXIT = 0x45
    ADMIN_DELETE = 0x46
    NODE_ORPHANS = 0x47
    WORKER_OPEN = 0x48
    WORKER_HEARTBEAT = 0x49
    WORKER_CLOSE = 0x4A
    PROBE = 0x50
    LOOKUP_AND_PIN = 0x51
    UNPIN = 0x52
    # 0x53: reserved; never reuse
    COMMIT = 0x58
    RETIRE = 0x59
    # 0x5B: reserved; never reuse
    # 0x5A: reserved (FINALIZE_MOVE)
    # 0x43, 0x5C, 0x5E, 0x5F: retired (NODE_REREGISTER, RELEASE_FINALIZER,
    # REGION_REQUEST, REGION_RETURN); never reuse them

    # Admin (0xF0 - 0xFF)
    GET_STATS = 0xF0
    HEARTBEAT = 0xF1
    REPORT_STATS = 0xF2
    GET_USAGE = 0xF3
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


@dataclass
class PinKVRequest:
    """PIN_KV (0x14) - Check if KV entry exists and pin it atomically.

    If the key exists, increments the entry's pin_count to protect it from
    eviction. This is an atomic operation to avoid race conditions between
    existence check and pinning.
    """

    key: str


@dataclass
class PinKVResponse:
    """Response for PIN_KV."""

    exists: bool


@dataclass
class UnpinKVRequest:
    """UNPIN_KV (0x15) - Unpin a KV entry.

    Decrements the entry's pin_count. When pin_count reaches 0, the entry
    becomes eligible for eviction again.
    """

    key: str


@dataclass
class UnpinKVResponse:
    """Response for UNPIN_KV."""

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
    """BATCH_EXISTS_KV (0x22) - Batch check KV existence (checks all keys)."""

    keys: list[str] = field(default_factory=list)


@dataclass
class BatchExistsKVResponse:
    """Response for BATCH_EXISTS_KV."""

    results: list[bool] = field(default_factory=list)


@dataclass
class BatchPinKVRequest:
    """BATCH_PIN_KV (0x23) - Batch check existence and pin (prefix-stop)."""

    keys: list[str] = field(default_factory=list)


@dataclass
class BatchPinKVResponse:
    """Response for BATCH_PIN_KV."""

    results: list[bool] = field(default_factory=list)


@dataclass
class BatchUnpinKVRequest:
    """BATCH_UNPIN_KV (0x24) - Batch unpin KV entries."""

    keys: list[str] = field(default_factory=list)


@dataclass
class BatchUnpinKVResponse:
    """Response for BATCH_UNPIN_KV."""

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
    stats_manager: dict = field(default_factory=dict)
    # Shared CXL device capacity {"total_size", "free_size"} summed across
    # resource-manager pools; empty when the server omits it (older servers).
    cxl_pool: dict = field(default_factory=dict)


@dataclass
class GetUsageRequest:
    """GET_USAGE (0xF3) - Request per-instance CXL usage."""

    pass


@dataclass
class InstanceUsage:
    """Per-instance (owner_instance_id) CXL memory usage.

    ``allocated`` is the sum of region sizes the instance reserved;
    ``used`` is the sum of live KV bytes stored in those regions.
    Slack (allocated - used) is derived by the consumer.
    ``devices`` maps each DAX device path the instance holds regions on to the
    bytes it reserved there (sums to ``allocated``); empty from older servers.
    """

    instance_id: str
    regions: int = 0
    allocated: int = 0
    used: int = 0
    devices: dict[str, int] = field(default_factory=dict)


@dataclass
class GetUsageResponse:
    """Response for GET_USAGE.

    ``pool_total`` / ``pool_free`` are the shared device capacity reported
    by the resource manager (summed across pools); any instance can draw
    from the free space.
    """

    instances: list[InstanceUsage] = field(default_factory=list)
    pool_total: int = 0
    pool_free: int = 0


@dataclass
class HeartbeatRequest:
    """HEARTBEAT (0xF1) - Connection keepalive."""

    pass


@dataclass
class HeartbeatResponse:
    """Response for HEARTBEAT."""

    pass


@dataclass
class ReportStatsRequest:
    """REPORT_STATS (0xF2) - Client reports handler-side latency."""

    entries: list[dict] = field(default_factory=list)
    # Each entry: {"op_type": str, "result": str, "size": int, "latency_us": float}


@dataclass
class ReportStatsResponse:
    """Response for REPORT_STATS."""

    success: bool = True


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
    rm_address: str | None = None
    error: str | None = None
    capabilities: list[str] = field(default_factory=list)
    server_epoch: str | None = None


@dataclass
class ShutdownRequest:
    """SHUTDOWN (0xFF) - Request server shutdown."""

    pass


@dataclass
class ShutdownResponse:
    """Response for SHUTDOWN."""

    success: bool


# =============================================================================
# Multi-storage Messages (0x40 - 0x5F)
# =============================================================================
# Session requests carry ``session`` and ``server_epoch``; state changes carry
# ``op`` (see OpRef). Response ``items`` follow the request's order.


@dataclass
class RecoverItem:
    key: Key
    location: Location
    logical_bytes: int
    pinned: bool


@dataclass
class CommitItem:
    key: Key
    location: Location
    logical_bytes: int


@dataclass
class NodeOpenRequest:
    """NODE_OPEN (0x40) - A StorageService opens its node session."""

    node_id: str
    node_epoch: str
    host_boot_id: str
    protocol: str
    capabilities: list[str] = field(default_factory=list)
    pools: list[PoolInfo] = field(default_factory=list)
    rm_domains: list[RmDomain] = field(default_factory=list)
    pid_ns: int = 0


@dataclass
class NodeOpenResponse:
    status: Status
    error: str | None = None
    server_epoch: str = ""
    node_session: str = ""
    epoch_seq: int = 0
    heartbeat_s: float = 0.0
    session_ttl_s: float = 0.0
    adopted: list[PoolInfo] = field(default_factory=list)
    # Those of the request MaruServer accepted; none from an older server
    capabilities: list[str] = field(default_factory=list)
    orphans: list[OrphanWorker] = field(default_factory=list)
    next_cursor: str | None = None
    pool_registrations: dict[str, int] = field(default_factory=dict)


@dataclass
class NodeOrphansRequest:
    """NODE_ORPHANS (0x47) - Pages of this session's orphan listing."""

    session: str
    server_epoch: str
    cursor: str | None = None


@dataclass
class NodeOrphansResponse:
    status: Status
    error: str | None = None
    orphans: list[OrphanWorker] = field(default_factory=list)
    next_cursor: str | None = None


@dataclass
class NodeHeartbeatRequest:
    """NODE_HEARTBEAT (0x41)."""

    session: str
    server_epoch: str
    usage: list[PoolUsage] = field(default_factory=list)
    memory_peer: tuple[str, str] | None = None  # connected node_id and node_epoch


@dataclass
class NodeHeartbeatResponse:
    status: Status
    error: str | None = None
    access: AccessHints = field(default_factory=AccessHints)
    blocks: list[BlockRequest] = field(default_factory=list)
    policy_version: int = 0


@dataclass
class NodeCloseRequest:
    """NODE_CLOSE (0x42)."""

    session: str
    server_epoch: str
    mode: CloseMode


@dataclass
class NodeCloseResponse:
    status: Status
    error: str | None = None


@dataclass
class NodeRecoverListRequest:
    """NODE_RECOVER_LIST (0x44) - Committed locations of an adopted pool."""

    session: str
    server_epoch: str
    pool_id: str
    cursor: str | None = None


@dataclass
class NodeRecoverListResponse:
    status: Status
    error: str | None = None
    items: list[RecoverItem] = field(default_factory=list)
    next_cursor: str | None = None


@dataclass
class WorkerExitRequest:
    """WORKER_EXIT (0x45) - A node reports workers it saw end."""

    session: str
    server_epoch: str
    op: OpRef
    workers: list[WorkerRef]
    cause: ExitCause


@dataclass
class WorkerExitResponse:
    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


@dataclass
class AdminDeleteRequest:
    """ADMIN_DELETE (0x46) - Remove keys through block requests to their
    nodes; idempotent, so no session or op."""

    items: list[Key]


@dataclass
class AdminDeleteResponse:
    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


@dataclass
class WorkerOpenRequest:
    """WORKER_OPEN (0x48).

    ``pid``/``pid_start`` are the node-view values from the ATTACH reply.
    """

    node_id: str
    node_epoch: str
    engine_id: str
    dp_index: int
    tp_rank: int
    pid: int
    pid_start: int
    incarnation: str
    namespace: str
    role: SessionRole
    protocol: str
    capabilities: list[str] = field(default_factory=list)


@dataclass
class WorkerOpenResponse:
    status: Status
    error: str | None = None
    server_epoch: str = ""
    worker_session: str = ""
    heartbeat_s: float = 0.0
    session_ttl_s: float = 0.0


@dataclass
class WorkerHeartbeatRequest:
    """WORKER_HEARTBEAT (0x49)."""

    session: str
    server_epoch: str


@dataclass
class WorkerHeartbeatResponse:
    status: Status
    error: str | None = None


@dataclass
class WorkerCloseRequest:
    """WORKER_CLOSE (0x4A) - Ends the session and releases its pins."""

    session: str
    server_epoch: str


@dataclass
class WorkerCloseResponse:
    status: Status
    error: str | None = None


@dataclass
class ProbeRequest:
    """PROBE (0x50) - Hit hints for the scheduler; no pin. With
    ``with_locations`` a HIT item names the location it found (a
    read hint's range), else none does."""

    session: str
    server_epoch: str
    items: list[Key]
    with_locations: bool = False


@dataclass
class ProbeResponse:
    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


@dataclass
class LookupAndPinRequest:
    """LOOKUP_AND_PIN (0x51)."""

    session: str
    server_epoch: str
    op: OpRef
    items: list[Key]


@dataclass
class LookupAndPinResponse:
    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


@dataclass
class UnpinRequest:
    """UNPIN (0x52) - Idempotent by pin_id."""

    session: str
    server_epoch: str
    pin_ids: list[str]


@dataclass
class UnpinResponse:
    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


@dataclass
class CommitRequest:
    """COMMIT (0x58) - An item that asks for a finalizer is REJECTED (none
    is kept here)."""

    session: str
    server_epoch: str
    op: OpRef
    items: list[CommitItem]


@dataclass
class CommitResponse:
    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


@dataclass
class RetireRequest:
    """RETIRE (0x59)."""

    session: str
    server_epoch: str
    op: OpRef
    items: list[KeyLocation]


@dataclass
class RetireResponse:
    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


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
    MessageType.PIN_KV: (PinKVRequest, PinKVResponse),
    MessageType.UNPIN_KV: (UnpinKVRequest, UnpinKVResponse),
    # Batch Operations
    MessageType.BATCH_REGISTER_KV: (BatchRegisterKVRequest, BatchRegisterKVResponse),
    MessageType.BATCH_LOOKUP_KV: (BatchLookupKVRequest, BatchLookupKVResponse),
    MessageType.BATCH_EXISTS_KV: (BatchExistsKVRequest, BatchExistsKVResponse),
    MessageType.BATCH_PIN_KV: (
        BatchPinKVRequest,
        BatchPinKVResponse,
    ),
    MessageType.BATCH_UNPIN_KV: (BatchUnpinKVRequest, BatchUnpinKVResponse),
    # Admin
    MessageType.GET_STATS: (GetStatsRequest, GetStatsResponse),
    MessageType.GET_USAGE: (GetUsageRequest, GetUsageResponse),
    MessageType.HEARTBEAT: (HeartbeatRequest, HeartbeatResponse),
    MessageType.REPORT_STATS: (ReportStatsRequest, ReportStatsResponse),
    MessageType.HANDSHAKE: (HandshakeRequest, HandshakeResponse),
    MessageType.SHUTDOWN: (ShutdownRequest, ShutdownResponse),
}

# Multi-storage messages, served by the Catalog; the legacy table knows
# their codes too
MS_MESSAGE_CLASSES = {
    MessageType.NODE_OPEN: (NodeOpenRequest, NodeOpenResponse),
    MessageType.NODE_ORPHANS: (NodeOrphansRequest, NodeOrphansResponse),
    MessageType.NODE_HEARTBEAT: (NodeHeartbeatRequest, NodeHeartbeatResponse),
    MessageType.NODE_CLOSE: (NodeCloseRequest, NodeCloseResponse),
    MessageType.NODE_RECOVER_LIST: (NodeRecoverListRequest, NodeRecoverListResponse),
    MessageType.WORKER_EXIT: (WorkerExitRequest, WorkerExitResponse),
    MessageType.ADMIN_DELETE: (AdminDeleteRequest, AdminDeleteResponse),
    MessageType.WORKER_OPEN: (WorkerOpenRequest, WorkerOpenResponse),
    MessageType.WORKER_HEARTBEAT: (WorkerHeartbeatRequest, WorkerHeartbeatResponse),
    MessageType.WORKER_CLOSE: (WorkerCloseRequest, WorkerCloseResponse),
    MessageType.PROBE: (ProbeRequest, ProbeResponse),
    MessageType.LOOKUP_AND_PIN: (LookupAndPinRequest, LookupAndPinResponse),
    MessageType.UNPIN: (UnpinRequest, UnpinResponse),
    MessageType.COMMIT: (CommitRequest, CommitResponse),
    MessageType.RETIRE: (RetireRequest, RetireResponse),
}
_overlap = set(MESSAGE_CLASSES) & set(MS_MESSAGE_CLASSES)
if _overlap:
    raise RuntimeError(f"message codes defined twice: {sorted(_overlap)}")
MESSAGE_CLASSES.update(MS_MESSAGE_CLASSES)
