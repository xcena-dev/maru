# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for protocol module."""

import pytest

from maru_common.protocol import (
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
)
from maru_shm.types import MaruHandle


class TestProtocolConstants:
    """Test protocol constants."""

    def test_magic_number(self):
        assert PROTOCOL_MAGIC == 0xCF00

    def test_version(self):
        assert PROTOCOL_VERSION == 1

    def test_header_size(self):
        assert HEADER_SIZE == 16


class TestMessageType:
    """Test MessageType enum."""

    def test_allocation_management_codes(self):
        assert MessageType.REQUEST_ALLOC == 0x01
        assert MessageType.RETURN_ALLOC == 0x03

    def test_kv_operation_codes(self):
        assert MessageType.REGISTER_KV == 0x10
        assert MessageType.LOOKUP_KV == 0x11
        assert MessageType.EXISTS_KV == 0x12
        assert MessageType.DELETE_KV == 0x13

    def test_batch_operation_codes(self):
        assert MessageType.BATCH_REGISTER_KV == 0x20
        assert MessageType.BATCH_LOOKUP_KV == 0x21
        assert MessageType.BATCH_EXISTS_KV == 0x22

    def test_admin_codes(self):
        assert MessageType.GET_STATS == 0xF0
        assert MessageType.HEARTBEAT == 0xF1
        assert MessageType.HANDSHAKE == 0xFE
        assert MessageType.SHUTDOWN == 0xFF

    def test_response_flag(self):
        assert MessageType.RESPONSE == 0x80

    def test_is_response(self):
        assert MessageType.is_response(0x81) is True
        assert MessageType.is_response(0x90) is True
        assert MessageType.is_response(0x01) is False
        assert MessageType.is_response(0x10) is False

    def test_get_request_type(self):
        assert MessageType.get_request_type(0x81) == 0x01
        assert MessageType.get_request_type(0x90) == 0x10
        assert MessageType.get_request_type(0x01) == 0x01


class TestMessageFlags:
    """Test MessageFlags enum."""

    def test_flag_values(self):
        assert MessageFlags.NONE == 0x0000
        assert MessageFlags.BATCH == 0x0001
        assert MessageFlags.COMPRESSED == 0x0002
        assert MessageFlags.URGENT == 0x0004
        assert MessageFlags.NO_REPLY == 0x0008

    def test_flag_combination(self):
        combined = MessageFlags.BATCH | MessageFlags.URGENT
        assert combined == 0x0005
        assert MessageFlags.BATCH in combined
        assert MessageFlags.URGENT in combined
        assert MessageFlags.COMPRESSED not in combined


class TestMessageHeader:
    """Test MessageHeader dataclass."""

    def test_default_values(self):
        header = MessageHeader()
        assert header.magic == PROTOCOL_MAGIC
        assert header.version == PROTOCOL_VERSION
        assert header.msg_type == 0
        assert header.flags == MessageFlags.NONE
        assert header.sequence == 0
        assert header.payload_length == 0

    def test_pack_unpack(self):
        header = MessageHeader(
            msg_type=MessageType.REQUEST_ALLOC,
            flags=MessageFlags.URGENT,
            sequence=12345,
            payload_length=100,
        )
        packed = header.pack()

        assert len(packed) == HEADER_SIZE

        unpacked = MessageHeader.unpack(packed)
        assert unpacked.magic == PROTOCOL_MAGIC
        assert unpacked.version == PROTOCOL_VERSION
        assert unpacked.msg_type == MessageType.REQUEST_ALLOC
        assert unpacked.flags == MessageFlags.URGENT
        assert unpacked.sequence == 12345
        assert unpacked.payload_length == 100

    def test_validate_valid(self):
        header = MessageHeader()
        assert header.validate() is True

    def test_validate_invalid_magic(self):
        header = MessageHeader(magic=0x1234)
        assert header.validate() is False

    def test_validate_invalid_version(self):
        header = MessageHeader(version=99)
        assert header.validate() is False

    def test_unpack_too_short(self):
        with pytest.raises(ValueError, match="Header too short"):
            MessageHeader.unpack(b"\x00" * 10)


class TestAllocMessages:
    """Test allocation management messages."""

    def test_request_alloc_request(self):
        req = RequestAllocRequest(instance_id="test-client", size=1024)
        assert req.instance_id == "test-client"
        assert req.size == 1024

    def test_request_alloc_response_success(self):
        handle = MaruHandle(region_id=1, offset=0, length=1024, auth_token=123)
        resp = RequestAllocResponse(success=True, handle=handle)
        assert resp.success is True
        assert resp.handle is not None
        assert resp.handle.region_id == 1
        assert resp.error is None

    def test_request_alloc_response_failure(self):
        resp = RequestAllocResponse(success=False, error="No memory available")
        assert resp.success is False
        assert resp.handle is None
        assert resp.error == "No memory available"

    def test_return_alloc_request(self):
        req = ReturnAllocRequest(instance_id="test-client", region_id=1)
        assert req.instance_id == "test-client"
        assert req.region_id == 1


class TestKVMessages:
    """Test KV operation messages."""

    def test_register_kv_request(self):
        req = RegisterKVRequest(
            key="12345", region_id=1, kv_offset=4096, kv_length=1024
        )
        assert req.key == "12345"
        assert req.region_id == 1
        assert req.kv_offset == 4096
        assert req.kv_length == 1024

    def test_register_kv_response(self):
        resp = RegisterKVResponse(success=True, is_new=True)
        assert resp.success is True
        assert resp.is_new is True

    def test_lookup_kv_request(self):
        req = LookupKVRequest(key="12345")
        assert req.key == "12345"

    def test_lookup_kv_response_found(self):
        handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=456)
        resp = LookupKVResponse(
            found=True, handle=handle, kv_offset=100, kv_length=1024
        )
        assert resp.found is True
        assert resp.handle is not None
        assert resp.kv_offset == 100
        assert resp.kv_length == 1024

    def test_lookup_kv_response_not_found(self):
        resp = LookupKVResponse(found=False)
        assert resp.found is False
        assert resp.handle is None

    def test_exists_kv(self):
        req = ExistsKVRequest(key="12345")
        resp = ExistsKVResponse(exists=True)
        assert req.key == "12345"
        assert resp.exists is True

    def test_delete_kv(self):
        req = DeleteKVRequest(key="12345")
        resp = DeleteKVResponse(success=True)
        assert req.key == "12345"
        assert resp.success is True


class TestBatchMessages:
    """Test batch operation messages."""

    def test_batch_register_kv_request(self):
        entries = [
            BatchKVEntry(key="1", region_id=1, kv_offset=0, kv_length=100),
            BatchKVEntry(key="2", region_id=1, kv_offset=100, kv_length=200),
        ]
        req = BatchRegisterKVRequest(entries=entries)
        assert len(req.entries) == 2
        assert req.entries[0].key == "1"

    def test_batch_register_kv_response(self):
        resp = BatchRegisterKVResponse(success=True, results=[True, True, False])
        assert resp.success is True
        assert len(resp.results) == 3

    def test_batch_lookup_kv_request(self):
        req = BatchLookupKVRequest(keys=["1", "2", "3"])
        assert len(req.keys) == 3

    def test_batch_lookup_kv_response(self):
        handle = MaruHandle(region_id=1, offset=0, length=100, auth_token=1)
        entries = [
            LookupResult(found=True, handle=handle, kv_offset=0, kv_length=100),
            LookupResult(found=False),
        ]
        resp = BatchLookupKVResponse(entries=entries)
        assert len(resp.entries) == 2
        assert resp.entries[0].found is True
        assert resp.entries[1].found is False

    def test_batch_exists_kv(self):
        req = BatchExistsKVRequest(keys=["1", "2", "3"])
        resp = BatchExistsKVResponse(results=[True, False, True])
        assert len(req.keys) == 3
        assert resp.results == [True, False, True]


class TestAdminMessages:
    """Test admin messages."""

    def test_get_stats_request(self):
        req = GetStatsRequest()
        assert req is not None

    def test_get_stats_response(self):
        resp = GetStatsResponse(
            kv_manager=KVManagerStats(total_entries=100, total_size=5000),
            allocation_manager=AllocationManagerStats(
                num_allocations=2, total_allocated=1024 * 1024, active_clients=2
            ),
        )
        assert resp.kv_manager.total_entries == 100
        assert resp.allocation_manager.num_allocations == 2

    def test_handshake_request(self):
        req = HandshakeRequest(client_version=1, instance_id="client-1")
        assert req.client_version == 1
        assert req.instance_id == "client-1"

    def test_handshake_response(self):
        resp = HandshakeResponse(success=True, server_version=1)
        assert resp.success is True
        assert resp.server_version == 1


class TestMessageClasses:
    """Test MESSAGE_CLASSES mapping."""

    def test_all_message_types_mapped(self):
        expected_types = [
            MessageType.REQUEST_ALLOC,
            MessageType.RETURN_ALLOC,
            MessageType.REGISTER_KV,
            MessageType.LOOKUP_KV,
            MessageType.EXISTS_KV,
            MessageType.DELETE_KV,
            MessageType.BATCH_REGISTER_KV,
            MessageType.BATCH_LOOKUP_KV,
            MessageType.BATCH_EXISTS_KV,
            MessageType.GET_STATS,
            MessageType.HEARTBEAT,
            MessageType.HANDSHAKE,
            MessageType.SHUTDOWN,
        ]
        for msg_type in expected_types:
            assert msg_type in MESSAGE_CLASSES
            req_class, resp_class = MESSAGE_CLASSES[msg_type]
            assert req_class is not None
            assert resp_class is not None
