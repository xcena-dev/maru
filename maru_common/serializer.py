# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Message serialization for RPC protocol.

Binary-only serialization using MessagePack with protocol header.
"""

import itertools
import logging
from dataclasses import asdict, is_dataclass
from typing import Any, TypeVar

import msgpack
from dacite import Config, from_dict

from maru_shm.types import MaruHandle, MaruPoolInfo

from .protocol import (
    HEADER_SIZE,
    MESSAGE_CLASSES,
    PROTOCOL_MAGIC,
    PROTOCOL_VERSION,
    MessageFlags,
    MessageHeader,
    MessageType,
)

T = TypeVar("T")

logger = logging.getLogger(__name__)


def _to_serializable(obj: Any) -> Any:
    """Convert object to serializable format, handling nested objects with to_dict()."""
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    elif is_dataclass(obj) and not isinstance(obj, type):
        return {k: _to_serializable(v) for k, v in asdict(obj).items()}
    elif isinstance(obj, dict):
        return {k: _to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list | tuple):
        return [_to_serializable(item) for item in obj]
    else:
        return obj


class Serializer:
    """
    Binary message serializer using MessagePack.

    Message format::

        [Header 16 bytes][MessagePack payload]
    """

    def __init__(self):
        """Initialize serializer."""
        self._seq_counter = itertools.count(1)
        # dacite config for nested dataclass conversion with type hooks for maru_shm types
        self._dacite_config = Config(
            check_types=False,
            type_hooks={
                MaruHandle: lambda d: (
                    MaruHandle.from_dict(d) if isinstance(d, dict) else d
                ),
                MaruPoolInfo: lambda d: (
                    MaruPoolInfo.from_dict(d) if isinstance(d, dict) else d
                ),
            },
        )

    def _next_seq(self) -> int:
        """Get next sequence number (thread-safe via itertools.count)."""
        return next(self._seq_counter)

    # =========================================================================
    # Encoding
    # =========================================================================

    def encode(
        self,
        msg_type: MessageType,
        data: Any,
        flags: int = MessageFlags.NONE,
        seq: int | None = None,
    ) -> bytes:
        """
        Encode a message to bytes.

        Args:
            msg_type: Message type enum
            data: Payload data (dict or dataclass)
            flags: Message flags
            seq: Sequence number (auto-generated if None)

        Returns:
            Encoded message bytes (header + msgpack payload)
        """
        # Convert to serializable dict
        payload_dict = _to_serializable(data)
        if not isinstance(payload_dict, dict):
            payload_dict = {"value": payload_dict}

        # Encode payload
        payload = msgpack.packb(payload_dict, use_bin_type=True)

        # Create header
        header = MessageHeader(
            magic=PROTOCOL_MAGIC,
            version=PROTOCOL_VERSION,
            msg_type=msg_type,
            flags=flags,
            sequence=seq if seq is not None else self._next_seq(),
            payload_length=len(payload),
        )

        return header.pack() + payload

    # =========================================================================
    # Decoding
    # =========================================================================

    def decode(self, data: bytes) -> tuple[MessageHeader, dict[str, Any]]:
        """
        Decode message bytes to dict payload (low-level).

        Args:
            data: Raw message bytes

        Returns:
            Tuple of (header, payload_dict)

        Raises:
            ValueError: If message format is invalid
        """
        if len(data) < HEADER_SIZE:
            raise ValueError(f"Message too short: {len(data)} bytes")

        # Parse header
        header = MessageHeader.unpack(data[:HEADER_SIZE])

        if not header.validate():
            raise ValueError(f"Invalid magic number: 0x{header.magic:04X}")

        # Parse payload
        payload_data = data[HEADER_SIZE : HEADER_SIZE + header.payload_length]
        if len(payload_data) < header.payload_length:
            raise ValueError(
                f"Payload truncated: {len(payload_data)} < {header.payload_length}"
            )

        payload = msgpack.unpackb(payload_data, raw=False)

        return header, payload

    def decode_request(self, data: bytes) -> tuple[MessageHeader, Any]:
        """
        Decode message bytes to typed request dataclass.

        Args:
            data: Raw message bytes

        Returns:
            Tuple of (header, request_dataclass)

        Raises:
            ValueError: If message format is invalid or unknown message type
        """
        header, payload = self.decode(data)

        msg_type = header.msg_type
        if msg_type not in MESSAGE_CLASSES:
            raise ValueError(f"Unknown message type: 0x{msg_type:02X}")

        request_cls, _ = MESSAGE_CLASSES[msg_type]
        request = from_dict(
            data_class=request_cls, data=payload, config=self._dacite_config
        )

        return header, request

    def decode_response(self, data: bytes) -> tuple[MessageHeader, Any]:
        """
        Decode message bytes to typed response dataclass.

        Args:
            data: Raw message bytes

        Returns:
            Tuple of (header, response_dataclass)

        Raises:
            ValueError: If message format is invalid or unknown message type
        """
        header, payload = self.decode(data)

        msg_type = header.msg_type

        # First try the raw msg_type (handles admin messages where 0x80 bit is already set)
        if msg_type in MESSAGE_CLASSES:
            _, response_cls = MESSAGE_CLASSES[msg_type]
        # Then try extracting request type from response type (for regular messages)
        elif MessageType.is_response(msg_type):
            request_type = MessageType.get_request_type(msg_type)
            if request_type in MESSAGE_CLASSES:
                _, response_cls = MESSAGE_CLASSES[request_type]
            else:
                raise ValueError(f"Unknown message type: 0x{msg_type:02X}")
        else:
            raise ValueError(f"Unknown message type: 0x{msg_type:02X}")

        response = from_dict(
            data_class=response_cls, data=payload, config=self._dacite_config
        )

        return header, response

    def decode_as(self, data: bytes, data_class: type[T]) -> tuple[MessageHeader, T]:
        """
        Decode message bytes to specified dataclass type.

        Args:
            data: Raw message bytes
            data_class: Target dataclass type

        Returns:
            Tuple of (header, dataclass_instance)
        """
        header, payload = self.decode(data)
        result = from_dict(
            data_class=data_class, data=payload, config=self._dacite_config
        )
        return header, result

    # =========================================================================
    # Response Encoding
    # =========================================================================

    def encode_response(
        self,
        request_header: MessageHeader,
        data: Any,
    ) -> bytes:
        """
        Encode a response message.

        Args:
            request_header: Original request header (for seq matching)
            data: Response payload

        Returns:
            Encoded response bytes
        """
        # Use int for response type (request_type | RESPONSE flag)
        response_type = request_header.msg_type | MessageType.RESPONSE
        return self._encode_raw(
            msg_type=response_type,
            data=data,
            flags=request_header.flags,
            seq=request_header.sequence,
        )

    def _encode_raw(
        self,
        msg_type: int,
        data: Any,
        flags: int = MessageFlags.NONE,
        seq: int | None = None,
    ) -> bytes:
        """Internal encode with raw int msg_type."""
        # Convert to serializable dict
        payload_dict = _to_serializable(data)
        if not isinstance(payload_dict, dict):
            payload_dict = {"value": payload_dict}

        # Encode payload
        payload = msgpack.packb(payload_dict, use_bin_type=True)

        # Create header
        header = MessageHeader(
            magic=PROTOCOL_MAGIC,
            version=PROTOCOL_VERSION,
            msg_type=msg_type,
            flags=flags,
            sequence=seq if seq is not None else self._next_seq(),
            payload_length=len(payload),
        )

        return header.pack() + payload


# =============================================================================
# Convenience functions
# =============================================================================


def create_serializer() -> Serializer:
    """Create a serializer instance."""
    return Serializer()
