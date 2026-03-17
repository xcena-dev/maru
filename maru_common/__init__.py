# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Maru Common - Shared types and utilities for Maru shared memory KV cache system."""

from .logging_setup import setup_package_logging  # noqa: E402

setup_package_logging("maru_common")

from .config import MaruConfig  # noqa: E402
from .protocol import (  # noqa: E402
    ANY_POOL_ID,
    HEADER_SIZE,
    MESSAGE_CLASSES,
    PROTOCOL_MAGIC,
    PROTOCOL_VERSION,
    AllocationManagerStats,
    BatchExistsKVRequest,
    BatchExistsKVResponse,
    BatchKVEntry,
    BatchLookupKVRequest,
    BatchLookupKVResponse,
    BatchRegisterKVRequest,
    BatchRegisterKVResponse,
    DeleteKVRequest,
    DeleteKVResponse,
    ExistsKVRequest,
    ExistsKVResponse,
    GetStatsRequest,
    GetStatsResponse,
    HandshakeRequest,
    HandshakeResponse,
    KVManagerStats,
    ListAllocationsRequest,
    ListAllocationsResponse,
    LookupKVRequest,
    LookupKVResponse,
    LookupResult,
    MessageFlags,
    MessageHeader,
    MessageType,
    RegisterKVRequest,
    RegisterKVResponse,
    RequestAllocRequest,
    RequestAllocResponse,
    ReturnAllocRequest,
    ReturnAllocResponse,
)
from .serializer import Serializer, create_serializer  # noqa: E402

__all__ = [
    # Logging
    "setup_package_logging",
    # Config
    "MaruConfig",
    # Pool ID constant
    "ANY_POOL_ID",
    # Protocol constants
    "PROTOCOL_MAGIC",
    "PROTOCOL_VERSION",
    "HEADER_SIZE",
    # Protocol enums
    "MessageType",
    "MessageFlags",
    "MessageHeader",
    # Allocation messages
    "RequestAllocRequest",
    "RequestAllocResponse",
    "ReturnAllocRequest",
    "ReturnAllocResponse",
    # KV messages
    "RegisterKVRequest",
    "RegisterKVResponse",
    "LookupKVRequest",
    "LookupKVResponse",
    "ExistsKVRequest",
    "ExistsKVResponse",
    "DeleteKVRequest",
    "DeleteKVResponse",
    # Batch messages
    "BatchRegisterKVRequest",
    "BatchRegisterKVResponse",
    "BatchLookupKVRequest",
    "BatchLookupKVResponse",
    "BatchExistsKVRequest",
    "BatchExistsKVResponse",
    "BatchKVEntry",
    "LookupResult",
    # Admin messages
    "GetStatsRequest",
    "GetStatsResponse",
    "KVManagerStats",
    "AllocationManagerStats",
    # List allocations messages
    "ListAllocationsRequest",
    "ListAllocationsResponse",
    # Handshake messages
    "HandshakeRequest",
    "HandshakeResponse",
    # Message class mapping
    "MESSAGE_CLASSES",
    # Serializer
    "Serializer",
    "create_serializer",
]
