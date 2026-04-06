# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for maru_shm package (Phase 1).

Tests cover:
- Handle / PoolInfo pack/unpack roundtrip
- MsgHeader and IPC message roundtrip
- TCP helpers (read_full / write_full)
"""

import socket

import pytest

from maru_shm.constants import (
    ANY_POOL_ID,
    MAP_PRIVATE,
    MAP_SHARED,
    PROT_EXEC,
    PROT_NONE,
    PROT_READ,
    PROT_WRITE,
)
from maru_shm.ipc import (
    HEADER_SIZE,
    PROTOCOL_MAGIC,
    PROTOCOL_VERSION,
    AllocReq,
    AllocResp,
    ErrorResp,
    FreeReq,
    FreeResp,
    GetAccessReq,
    GetAccessResp,
    MsgHeader,
    MsgType,
    StatsReq,
    StatsResp,
)
from maru_shm.types import DaxType, MaruHandle, MaruPoolInfo
from maru_shm.uds_helpers import (
    read_full,
    write_full,
)

# =============================================================================
# Handle tests
# =============================================================================


class TestHandle:
    def test_pack_unpack_roundtrip(self):
        h = MaruHandle(region_id=42, offset=1024, length=4096, auth_token=0xDEADBEEF)
        data = h.pack()
        assert len(data) == 32
        h2 = MaruHandle.unpack(data)
        assert h2.region_id == 42
        assert h2.offset == 1024
        assert h2.length == 4096
        assert h2.auth_token == 0xDEADBEEF

    def test_pack_unpack_zero(self):
        h = MaruHandle()
        data = h.pack()
        h2 = MaruHandle.unpack(data)
        assert h2.region_id == 0
        assert h2.offset == 0
        assert h2.length == 0
        assert h2.auth_token == 0

    def test_pack_unpack_max_values(self):
        max_u64 = (1 << 64) - 1
        h = MaruHandle(
            region_id=max_u64,
            offset=max_u64,
            length=max_u64,
            auth_token=max_u64,
        )
        h2 = MaruHandle.unpack(h.pack())
        assert h2.region_id == max_u64
        assert h2.offset == max_u64
        assert h2.length == max_u64
        assert h2.auth_token == max_u64

    def test_to_dict_from_dict_roundtrip(self):
        h = MaruHandle(region_id=10, offset=20, length=30, auth_token=40)
        d = h.to_dict()
        h2 = MaruHandle.from_dict(d)
        assert h2.region_id == h.region_id
        assert h2.offset == h.offset
        assert h2.length == h.length
        assert h2.auth_token == h.auth_token

    def test_unpack_too_short(self):
        with pytest.raises(ValueError, match="too short"):
            MaruHandle.unpack(b"\x00" * 10)

    def test_repr(self):
        h = MaruHandle(region_id=1, offset=2, length=3, auth_token=4)
        r = repr(h)
        assert "region_id=1" in r
        assert "offset=2" in r


# =============================================================================
# PoolInfo tests
# =============================================================================


class TestPoolInfo:
    def test_pack_unpack_roundtrip(self):
        p = MaruPoolInfo(
            pool_id=1,
            dax_type=DaxType.FS_DAX,
            total_size=1 << 30,
            free_size=1 << 29,
            align_bytes=2 * 1024 * 1024,
        )
        data = p.pack()
        assert len(data) == 32
        p2 = MaruPoolInfo.unpack(data)
        assert p2.pool_id == 1
        assert p2.dax_type == DaxType.FS_DAX
        assert p2.total_size == 1 << 30
        assert p2.free_size == 1 << 29
        assert p2.align_bytes == 2 * 1024 * 1024

    def test_to_dict_from_dict_roundtrip(self):
        p = MaruPoolInfo(
            pool_id=5,
            dax_type=DaxType.DEV_DAX,
            total_size=100,
            free_size=50,
            align_bytes=4096,
        )
        d = p.to_dict()
        p2 = MaruPoolInfo.from_dict(d)
        assert p2.pool_id == p.pool_id
        assert p2.dax_type == p.dax_type
        assert p2.total_size == p.total_size

    def test_unpack_too_short(self):
        with pytest.raises(ValueError, match="too short"):
            MaruPoolInfo.unpack(b"\x00" * 5)


# =============================================================================
# DaxType tests
# =============================================================================


class TestDaxType:
    def test_values(self):
        assert DaxType.DEV_DAX == 0
        assert DaxType.FS_DAX == 1

    def test_int_conversion(self):
        assert int(DaxType.DEV_DAX) == 0
        assert DaxType(1) == DaxType.FS_DAX


# =============================================================================
# MsgHeader tests
# =============================================================================


class TestMsgHeader:
    def test_pack_unpack_roundtrip(self):
        hdr = MsgHeader(
            magic=PROTOCOL_MAGIC,
            version=PROTOCOL_VERSION,
            msg_type=MsgType.ALLOC_REQ,
            payload_len=16,
        )
        data = hdr.pack()
        assert len(data) == HEADER_SIZE
        hdr2 = MsgHeader.unpack(data)
        assert hdr2.magic == PROTOCOL_MAGIC
        assert hdr2.version == PROTOCOL_VERSION
        assert hdr2.msg_type == MsgType.ALLOC_REQ
        assert hdr2.payload_len == 16

    def test_validate_good(self):
        hdr = MsgHeader()
        assert hdr.validate()

    def test_validate_bad_magic(self):
        hdr = MsgHeader(magic=0xBAAD)
        assert not hdr.validate()

    def test_validate_bad_version(self):
        hdr = MsgHeader(version=99)
        assert not hdr.validate()

    def test_unpack_too_short(self):
        with pytest.raises(ValueError, match="too short"):
            MsgHeader.unpack(b"\x00" * 5)


# =============================================================================
# IPC message roundtrip tests
# =============================================================================


class TestAllocReq:
    def test_roundtrip(self):
        req = AllocReq(size=4096, pool_id=1)
        data = req.pack()
        req2 = AllocReq.unpack(data)
        assert req2.size == 4096
        assert req2.pool_id == 1

    def test_default_pool_id(self):
        req = AllocReq(size=1024)
        assert req.pool_id == ANY_POOL_ID


class TestAllocResp:
    def test_roundtrip(self):
        h = MaruHandle(region_id=10, offset=0, length=4096, auth_token=999)
        resp = AllocResp(status=0, handle=h, requested_size=4096)
        data = resp.pack()
        resp2 = AllocResp.unpack(data)
        assert resp2.status == 0
        assert resp2.handle.region_id == 10
        assert resp2.handle.length == 4096
        assert resp2.handle.auth_token == 999
        assert resp2.requested_size == 4096

    def test_error_status(self):
        resp = AllocResp(status=-12)  # ENOMEM
        data = resp.pack()
        resp2 = AllocResp.unpack(data)
        assert resp2.status == -12


class TestFreeReqResp:
    def test_free_req_roundtrip(self):
        h = MaruHandle(region_id=5, offset=0, length=2048, auth_token=123)
        req = FreeReq(handle=h)
        data = req.pack()
        req2 = FreeReq.unpack(data)
        assert req2.handle.region_id == 5
        assert req2.handle.auth_token == 123

    def test_free_resp_roundtrip(self):
        resp = FreeResp(status=0)
        data = resp.pack()
        resp2 = FreeResp.unpack(data)
        assert resp2.status == 0


class TestGetAccessReqResp:
    def test_get_access_req_roundtrip(self):
        h = MaruHandle(region_id=7, offset=100, length=500, auth_token=42)
        req = GetAccessReq(handle=h, client_id="host-a:1234")
        data = req.pack()
        req2 = GetAccessReq.unpack(data)
        assert req2.handle.region_id == 7
        assert req2.handle.offset == 100
        assert req2.client_id == "host-a:1234"

    def test_get_access_req_no_client_id(self):
        """GetAccessReq without client_id is backward-compatible."""
        h = MaruHandle(region_id=7, offset=100, length=500, auth_token=42)
        req = GetAccessReq(handle=h)
        data = req.pack()
        req2 = GetAccessReq.unpack(data)
        assert req2.handle.region_id == 7
        assert req2.client_id == ""

    def test_get_access_resp_roundtrip(self):
        resp = GetAccessResp(status=0, device_path="/dev/dax0.0", offset=0, length=4096)
        data = resp.pack()
        resp2 = GetAccessResp.unpack(data)
        assert resp2.status == 0
        assert resp2.device_path == "/dev/dax0.0"
        assert resp2.offset == 0
        assert resp2.length == 4096


class TestStatsReqResp:
    def test_stats_req_roundtrip(self):
        req = StatsReq()
        data = req.pack()
        assert data == b""
        req2 = StatsReq.unpack(data)
        assert isinstance(req2, StatsReq)

    def test_stats_resp_empty(self):
        resp = StatsResp(pools=[])
        data = resp.pack()
        resp2 = StatsResp.unpack(data)
        assert resp2.pools == []

    def test_stats_resp_with_pools(self):
        pools = [
            MaruPoolInfo(
                pool_id=0,
                dax_type=DaxType.DEV_DAX,
                total_size=1 << 30,
                free_size=1 << 29,
                align_bytes=2 << 20,
            ),
            MaruPoolInfo(
                pool_id=1,
                dax_type=DaxType.FS_DAX,
                total_size=1 << 31,
                free_size=1 << 30,
                align_bytes=4096,
            ),
        ]
        resp = StatsResp(pools=pools)
        data = resp.pack()
        resp2 = StatsResp.unpack(data)
        assert len(resp2.pools) == 2
        assert resp2.pools[0].pool_id == 0
        assert resp2.pools[0].dax_type == DaxType.DEV_DAX
        assert resp2.pools[1].pool_id == 1
        assert resp2.pools[1].dax_type == DaxType.FS_DAX


class TestErrorResp:
    def test_roundtrip(self):
        resp = ErrorResp(status=-1, message="out of memory")
        data = resp.pack()
        resp2 = ErrorResp.unpack(data)
        assert resp2.status == -1
        assert resp2.message == "out of memory"

    def test_empty_message(self):
        resp = ErrorResp(status=-2, message="")
        data = resp.pack()
        resp2 = ErrorResp.unpack(data)
        assert resp2.status == -2
        assert resp2.message == ""


# =============================================================================
# TCP helpers tests (read_full / write_full)
# =============================================================================


class TestTcpHelpers:
    def test_read_write_full(self):
        s1, s2 = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            msg = b"hello world" * 100
            write_full(s1, msg)
            result = read_full(s2, len(msg))
            assert result == msg
        finally:
            s1.close()
            s2.close()

    def test_read_full_connection_closed(self):
        s1, s2 = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        s1.close()
        with pytest.raises(ConnectionError):
            read_full(s2, 10)
        s2.close()


# =============================================================================
# Constants tests
# =============================================================================


class TestConstants:
    def test_prot_flags(self):
        assert PROT_NONE == 0x0
        assert PROT_READ == 0x1
        assert PROT_WRITE == 0x2
        assert PROT_EXEC == 0x4

    def test_map_flags(self):
        assert MAP_SHARED == 0x01
        assert MAP_PRIVATE == 0x02

    def test_any_pool_id(self):
        assert ANY_POOL_ID == 0xFFFFFFFF


# =============================================================================
# IPC unpack validation tests (for 100% coverage)
# =============================================================================


class TestIPCUnpackValidation:
    """Test unpack() validation error paths for 100% coverage."""

    def test_alloc_req_unpack_too_short(self):
        """Test AllocReq.unpack with data too short."""
        with pytest.raises(ValueError, match="AllocReq too short"):
            AllocReq.unpack(b"\x00" * 10)

    def test_alloc_resp_unpack_too_short(self):
        """Test AllocResp.unpack with data too short."""
        with pytest.raises(ValueError, match="AllocResp too short"):
            AllocResp.unpack(b"\x00" * 20)

    def test_free_resp_unpack_too_short(self):
        """Test FreeResp.unpack with data too short."""
        with pytest.raises(ValueError, match="FreeResp too short"):
            FreeResp.unpack(b"\x00" * 2)

    def test_get_access_resp_unpack_too_short(self):
        """Test GetAccessResp.unpack with data too short."""
        with pytest.raises(ValueError, match="GetAccessResp too short for header"):
            GetAccessResp.unpack(b"\x00" * 2)

    def test_stats_resp_unpack_too_short_header(self):
        """Test StatsResp.unpack with truncated header."""
        with pytest.raises(ValueError, match="StatsResp too short for header"):
            StatsResp.unpack(b"\x00" * 2)

    def test_stats_resp_unpack_truncated_pools(self):
        """Test StatsResp.unpack with truncated pool data."""
        import struct

        # Pack a header saying we have 2 pools
        header = struct.pack("=I", 2)
        # But only provide data for 1 pool (32 bytes) instead of 2 pools (64 bytes)
        pool_data = b"\x00" * 32
        incomplete_data = header + pool_data

        with pytest.raises(ValueError, match="StatsResp truncated"):
            StatsResp.unpack(incomplete_data)

    def test_error_resp_unpack_too_short(self):
        """Test ErrorResp.unpack with data too short."""
        with pytest.raises(ValueError, match="ErrorResp too short for header"):
            ErrorResp.unpack(b"\x00" * 4)


# =============================================================================
# MaruPoolInfo __repr__ test (for 100% coverage)
# =============================================================================


class TestIPCNoneDefaults:
    """Test pack() with None defaults for 100% branch coverage."""

    def test_free_req_none_handle(self):
        """FreeReq with handle=None uses default MaruHandle."""
        req = FreeReq(handle=None)
        data = req.pack()
        req2 = FreeReq.unpack(data)
        assert req2.handle.region_id == 0
        assert req2.handle.length == 0

    def test_get_access_req_none_handle(self):
        """GetAccessReq with handle=None uses default MaruHandle."""
        req = GetAccessReq(handle=None, client_id="host:1")
        data = req.pack()
        req2 = GetAccessReq.unpack(data)
        assert req2.handle.region_id == 0
        assert req2.client_id == "host:1"

    def test_alloc_resp_none_handle(self):
        """AllocResp with handle=None uses default MaruHandle."""
        resp = AllocResp(status=0, handle=None, requested_size=1024)
        data = resp.pack()
        resp2 = AllocResp.unpack(data)
        assert resp2.handle.region_id == 0
        assert resp2.requested_size == 1024

    def test_stats_resp_none_pools(self):
        """StatsResp with pools=None packs as empty list."""
        resp = StatsResp(pools=None)
        data = resp.pack()
        resp2 = StatsResp.unpack(data)
        assert resp2.pools == []


class TestPoolInfoFromDictDefaults:
    """Test MaruPoolInfo.from_dict with missing optional fields."""

    def test_missing_dax_type_defaults_to_dev_dax(self):
        d = {"pool_id": 1, "total_size": 1000, "free_size": 500}
        p = MaruPoolInfo.from_dict(d)
        assert p.dax_type == DaxType.DEV_DAX

    def test_missing_align_bytes_defaults_to_zero(self):
        d = {"pool_id": 2, "total_size": 2000, "free_size": 1000}
        p = MaruPoolInfo.from_dict(d)
        assert p.align_bytes == 0

    def test_all_optional_fields_missing(self):
        d = {"pool_id": 3, "total_size": 3000, "free_size": 1500}
        p = MaruPoolInfo.from_dict(d)
        assert p.pool_id == 3
        assert p.dax_type == DaxType.DEV_DAX
        assert p.align_bytes == 0


class TestMsgTypeValues:
    """Test MsgType enum values match C++ header."""

    def test_message_type_values(self):
        assert MsgType.ALLOC_REQ == 1
        assert MsgType.ALLOC_RESP == 2
        assert MsgType.FREE_REQ == 3
        assert MsgType.FREE_RESP == 4
        assert MsgType.STATS_REQ == 5
        assert MsgType.STATS_RESP == 6
        assert MsgType.GET_ACCESS_REQ == 9
        assert MsgType.GET_ACCESS_RESP == 10
        assert MsgType.ERROR_RESP == 255


class TestMaruPoolInfoRepr:
    """Test MaruPoolInfo.__repr__ for 100% coverage."""

    def test_pool_info_repr(self):
        """Test MaruPoolInfo.__repr__ returns expected format."""
        pool = MaruPoolInfo(
            pool_id=5,
            dax_type=DaxType.FS_DAX,
            total_size=1000000,
            free_size=500000,
            align_bytes=4096,
        )
        repr_str = repr(pool)
        assert "<MaruPoolInfo" in repr_str
        assert "pool_id=5" in repr_str
        assert "dax_type=FS_DAX" in repr_str
        assert "total_size=1000000" in repr_str
        assert "free_size=500000" in repr_str
