# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for serializer module."""

import pytest

from maru_common.protocol import (
    HEADER_SIZE,
    PROTOCOL_MAGIC,
    LookupKVRequest,
    LookupKVResponse,
    MessageFlags,
    MessageHeader,
    MessageType,
    RegisterKVRequest,
    RequestAllocRequest,
)
from maru_common.serializer import Serializer, create_serializer
from maru_shm.types import MaruHandle


class TestSerializer:
    """Test Serializer class."""

    def test_create_serializer(self):
        serializer = create_serializer()
        assert serializer is not None
        assert isinstance(serializer, Serializer)

    def test_sequence_counter(self):
        serializer = Serializer()
        seq1 = serializer._next_seq()
        seq2 = serializer._next_seq()
        seq3 = serializer._next_seq()
        assert seq1 == 1
        assert seq2 == 2
        assert seq3 == 3


class TestEncode:
    """Test encoding methods."""

    def test_encode_dict_payload(self):
        serializer = Serializer()
        data = {"key": "12345", "value": "test"}
        encoded = serializer.encode(MessageType.REGISTER_KV, data)

        assert len(encoded) > HEADER_SIZE
        # Check header magic
        assert encoded[0:2] == bytes([0xCF, 0x00])

    def test_encode_dataclass_payload(self):
        serializer = Serializer()
        request = RegisterKVRequest(
            key="12345", region_id=1, kv_offset=4096, kv_length=1024
        )
        encoded = serializer.encode(MessageType.REGISTER_KV, request)

        assert len(encoded) > HEADER_SIZE
        header = MessageHeader.unpack(encoded[:HEADER_SIZE])
        assert header.msg_type == MessageType.REGISTER_KV

    def test_encode_with_flags(self):
        serializer = Serializer()
        data = {"test": 1}
        encoded = serializer.encode(
            MessageType.REQUEST_ALLOC, data, flags=MessageFlags.URGENT
        )

        header = MessageHeader.unpack(encoded[:HEADER_SIZE])
        assert header.flags == MessageFlags.URGENT

    def test_encode_with_sequence(self):
        serializer = Serializer()
        data = {"test": 1}
        encoded = serializer.encode(MessageType.REQUEST_ALLOC, data, seq=9999)

        header = MessageHeader.unpack(encoded[:HEADER_SIZE])
        assert header.sequence == 9999

    def test_encode_auto_sequence(self):
        serializer = Serializer()
        data = {"test": 1}

        encoded1 = serializer.encode(MessageType.REQUEST_ALLOC, data)
        encoded2 = serializer.encode(MessageType.REQUEST_ALLOC, data)

        header1 = MessageHeader.unpack(encoded1[:HEADER_SIZE])
        header2 = MessageHeader.unpack(encoded2[:HEADER_SIZE])

        assert header1.sequence == 1
        assert header2.sequence == 2


class TestDecode:
    """Test decoding methods."""

    def test_decode_roundtrip(self):
        serializer = Serializer()
        original_data = {"key": "12345", "value": "hello"}
        encoded = serializer.encode(MessageType.LOOKUP_KV, original_data)

        header, payload = serializer.decode(encoded)

        assert header.magic == PROTOCOL_MAGIC
        assert header.msg_type == MessageType.LOOKUP_KV
        assert payload["key"] == "12345"
        assert payload["value"] == "hello"

    def test_decode_dataclass_roundtrip(self):
        serializer = Serializer()
        request = RegisterKVRequest(
            key="99999", region_id=1, kv_offset=4096, kv_length=1024
        )
        encoded = serializer.encode(MessageType.REGISTER_KV, request)

        header, payload = serializer.decode(encoded)

        assert header.msg_type == MessageType.REGISTER_KV
        assert payload["key"] == "99999"
        assert payload["region_id"] == 1
        assert payload["kv_offset"] == 4096
        assert payload["kv_length"] == 1024

    def test_decode_too_short(self):
        serializer = Serializer()
        with pytest.raises(ValueError, match="Message too short"):
            serializer.decode(b"\x00" * 10)

    def test_decode_invalid_magic(self):
        serializer = Serializer()
        # Create invalid header with wrong magic
        invalid_data = b"\x00\x00" + b"\x00" * 14 + b"\x00" * 10
        with pytest.raises(ValueError, match="Invalid magic"):
            serializer.decode(invalid_data)

    def test_decode_truncated_payload(self):
        serializer = Serializer()
        data = {"test": "value"}
        encoded = serializer.encode(MessageType.REQUEST_ALLOC, data)

        # Truncate payload
        truncated = encoded[: HEADER_SIZE + 2]
        with pytest.raises(ValueError, match="Payload truncated"):
            serializer.decode(truncated)


class TestEncodeResponse:
    """Test response encoding."""

    def test_encode_response(self):
        serializer = Serializer()

        # First encode a request
        request_data = {"key": "123"}
        request_encoded = serializer.encode(MessageType.REGISTER_KV, request_data)
        request_header, _ = serializer.decode(request_encoded)

        # Then encode response
        response_data = {"success": True, "is_new": True}
        response_encoded = serializer.encode_response(request_header, response_data)

        # Decode and verify
        response_header, response_payload = serializer.decode(response_encoded)

        assert MessageType.is_response(response_header.msg_type)
        assert response_header.msg_type == (
            MessageType.REGISTER_KV | MessageType.RESPONSE
        )
        assert response_header.sequence == request_header.sequence
        assert response_payload["success"] is True
        assert response_payload["is_new"] is True

    def test_encode_response_preserves_flags(self):
        serializer = Serializer()

        request_encoded = serializer.encode(
            MessageType.REQUEST_ALLOC, {}, flags=MessageFlags.URGENT
        )
        request_header, _ = serializer.decode(request_encoded)

        response_encoded = serializer.encode_response(request_header, {"success": True})
        response_header, _ = serializer.decode(response_encoded)

        assert response_header.flags == MessageFlags.URGENT


class TestComplexPayloads:
    """Test with complex nested payloads."""

    def test_nested_handle(self):
        serializer = Serializer()
        handle = MaruHandle(
            region_id=1,
            offset=1024 * 1024,
            length=4096,
            auth_token=0xDEADBEEF,
        )
        data = {
            "instance_id": "test-instance",
            "handle": handle.__dict__,
        }
        encoded = serializer.encode(MessageType.RETURN_ALLOC, data)
        header, payload = serializer.decode(encoded)

        assert payload["instance_id"] == "test-instance"
        assert payload["handle"]["region_id"] == 1
        assert payload["handle"]["auth_token"] == 0xDEADBEEF

    def test_list_of_handles(self):
        serializer = Serializer()
        handles = [
            {"region_id": i, "offset": i * 100, "length": 50, "auth_token": i}
            for i in range(5)
        ]
        data = {"entries": handles}
        encoded = serializer.encode(MessageType.BATCH_LOOKUP_KV, data)
        header, payload = serializer.decode(encoded)

        assert len(payload["entries"]) == 5
        assert payload["entries"][0]["region_id"] == 0
        assert payload["entries"][4]["region_id"] == 4

    def test_binary_data(self):
        serializer = Serializer()
        data = {"raw_bytes": b"\x00\x01\x02\x03\xff\xfe\xfd"}
        encoded = serializer.encode(MessageType.GET_STATS, data)
        header, payload = serializer.decode(encoded)

        assert payload["raw_bytes"] == b"\x00\x01\x02\x03\xff\xfe\xfd"


class TestDecodeTyped:
    """Test typed decode methods (decode_request, decode_response, decode_as)."""

    def test_decode_request_register_kv(self):
        serializer = Serializer()
        request = RegisterKVRequest(
            key="12345", region_id=1, kv_offset=4096, kv_length=1024
        )
        encoded = serializer.encode(MessageType.REGISTER_KV, request)

        header, decoded_request = serializer.decode_request(encoded)

        assert isinstance(decoded_request, RegisterKVRequest)
        assert decoded_request.key == "12345"
        assert decoded_request.region_id == 1
        assert decoded_request.kv_offset == 4096
        assert decoded_request.kv_length == 1024

    def test_decode_request_from_dict(self):
        serializer = Serializer()
        data = {"instance_id": "test-client", "size": 1024}
        encoded = serializer.encode(MessageType.REQUEST_ALLOC, data)

        header, decoded_request = serializer.decode_request(encoded)

        assert isinstance(decoded_request, RequestAllocRequest)
        assert decoded_request.instance_id == "test-client"
        assert decoded_request.size == 1024

    def test_decode_response(self):
        serializer = Serializer()
        handle = MaruHandle(region_id=2, offset=8192, length=2048, auth_token=456)
        response = LookupKVResponse(
            found=True, handle=handle, kv_offset=100, kv_length=512
        )

        # Encode as response
        request_header = MessageHeader(msg_type=MessageType.LOOKUP_KV, sequence=100)
        encoded = serializer.encode_response(request_header, response)

        header, decoded_response = serializer.decode_response(encoded)

        assert isinstance(decoded_response, LookupKVResponse)
        assert decoded_response.found is True
        assert decoded_response.handle.region_id == 2
        assert decoded_response.handle.offset == 8192
        assert decoded_response.kv_offset == 100
        assert decoded_response.kv_length == 512

    def test_decode_as_specific_type(self):
        serializer = Serializer()
        data = {"key": "99999"}
        encoded = serializer.encode(MessageType.LOOKUP_KV, data)

        header, decoded = serializer.decode_as(encoded, LookupKVRequest)

        assert isinstance(decoded, LookupKVRequest)
        assert decoded.key == "99999"

    def test_decode_request_unknown_type(self):
        import msgpack
        import pytest

        serializer = Serializer()
        data = {"test": 1}

        # Create message with invalid type
        payload = msgpack.packb(data, use_bin_type=True)
        header = MessageHeader(msg_type=0x99, sequence=1, payload_length=len(payload))
        encoded = header.pack() + payload

        with pytest.raises(ValueError, match="Unknown message type"):
            serializer.decode_request(encoded)


class TestSerializerEdgeCases:
    """Test edge cases for serializer to achieve 100% coverage."""

    def test_to_serializable_with_to_dict_method(self):
        """Test _to_serializable() with object that has to_dict() method."""
        from dataclasses import dataclass

        @dataclass
        class CustomObject:
            value: int

            def to_dict(self):
                return {"custom_value": self.value * 2}

        serializer = Serializer()
        obj = CustomObject(value=42)
        data = {"obj": obj}

        # encode() calls _to_serializable internally
        encoded = serializer.encode(MessageType.REGISTER_KV, data)
        header, payload = serializer.decode(encoded)

        assert payload["obj"]["custom_value"] == 84

    def test_encode_non_dict_payload_wrapping(self):
        """Test encode() when payload_dict is not a dict after serialization → wraps in {"value": ...}."""
        serializer = Serializer()

        # Pass a primitive type (not a dict or dataclass)
        encoded = serializer.encode(MessageType.HEARTBEAT, 12345)
        header, payload = serializer.decode(encoded)

        assert "value" in payload
        assert payload["value"] == 12345

    def test_encode_response_non_dict_payload_wrapping(self):
        """Test _encode_raw() when payload_dict is not a dict → wraps in {"value": ...}."""
        serializer = Serializer()

        request_header = MessageHeader(msg_type=MessageType.HEARTBEAT, sequence=100)
        # Pass a primitive type
        encoded = serializer.encode_response(request_header, 999)
        header, payload = serializer.decode(encoded)

        assert "value" in payload
        assert payload["value"] == 999

    def test_decode_response_with_raw_msg_type_in_message_classes(self):
        """Test decode_response() with raw msg_type already in MESSAGE_CLASSES (admin types)."""
        serializer = Serializer()

        # GET_STATS is an admin message where response type is the same as request type
        data = {
            "kv_manager": {"total_entries": 100, "total_size": 1024},
            "allocation_manager": {
                "num_allocations": 10,
                "total_allocated": 2048,
                "active_clients": 5,
            },
        }
        encoded = serializer.encode(MessageType.GET_STATS, data)

        # decode_response should handle this by checking raw msg_type first
        header, response = serializer.decode_response(encoded)
        assert header.msg_type == MessageType.GET_STATS

    def test_decode_response_unknown_message_type_no_response_bit(self):
        """Test decode_response() with unknown message type (no RESPONSE bit) → ValueError (L207)."""
        import msgpack

        serializer = Serializer()
        data = {"test": 1}

        # Use 0x09 which does NOT have the 0x80 response bit and is NOT in MESSAGE_CLASSES
        # This hits the else branch at L206-207
        payload = msgpack.packb(data, use_bin_type=True)
        header = MessageHeader(msg_type=0x09, sequence=1, payload_length=len(payload))
        encoded = header.pack() + payload

        with pytest.raises(ValueError, match="Unknown message type"):
            serializer.decode_response(encoded)

    def test_decode_response_unknown_message_type_with_response_bit(self):
        """Test decode_response() with unknown request type (has RESPONSE bit) → ValueError."""
        import msgpack

        serializer = Serializer()
        data = {"test": 1}

        # Create message with invalid type (0x99 | 0x80 = 0x99 with RESPONSE bit)
        invalid_response_type = 0x99 | MessageType.RESPONSE
        payload = msgpack.packb(data, use_bin_type=True)
        header = MessageHeader(
            msg_type=invalid_response_type, sequence=1, payload_length=len(payload)
        )
        encoded = header.pack() + payload

        with pytest.raises(ValueError, match="Unknown message type"):
            serializer.decode_response(encoded)
