# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Integration test for v2 client-side device UUID mapping.

Tests that:
    - HANDSHAKE does NOT carry device info (no hostname/devices fields)
    - Handler scans devices locally and builds a device table
    - DaxMapper resolves UUID → local path using the device table
"""

import pytest

from maru_shm.device_scanner import (
    clear_device_header,
    read_device_uuid,
    write_device_header,
)
from maru_shm.ipc import AllocResp, GetAccessResp

pytestmark = pytest.mark.integration


# =============================================================================
# AllocResp / GetAccessResp UUID round-trip
# =============================================================================


class TestAllocRespUuid:
    """AllocResp packs and unpacks device_uuid correctly."""

    def test_alloc_resp_with_uuid(self):
        from maru_shm.types import MaruHandle

        resp = AllocResp(
            status=0,
            handle=MaruHandle(region_id=1, offset=0, length=4096, auth_token=42),
            requested_size=4096,
            dax_path="/dev/dax0.0",
            device_uuid="550e8400-e29b-41d4-a716-446655440000",
        )
        data = resp.pack()
        parsed = AllocResp.unpack(data)

        assert parsed.status == 0
        assert parsed.dax_path == "/dev/dax0.0"
        assert parsed.device_uuid == "550e8400-e29b-41d4-a716-446655440000"
        assert parsed.handle.region_id == 1

    def test_alloc_resp_without_uuid(self):
        from maru_shm.types import MaruHandle

        resp = AllocResp(
            status=0,
            handle=MaruHandle(region_id=2, offset=0, length=8192, auth_token=0),
            requested_size=8192,
            dax_path="/dev/dax1.0",
            device_uuid="",
        )
        data = resp.pack()
        parsed = AllocResp.unpack(data)

        assert parsed.dax_path == "/dev/dax1.0"
        assert parsed.device_uuid == ""


class TestGetAccessRespUuid:
    """GetAccessResp packs and unpacks device_uuid correctly."""

    def test_get_access_resp_with_uuid(self):
        resp = GetAccessResp(
            status=0,
            dax_path="/dev/dax0.0",
            device_uuid="550e8400-e29b-41d4-a716-446655440000",
            offset=2097152,
            length=4096,
        )
        data = resp.pack()
        parsed = GetAccessResp.unpack(data)

        assert parsed.dax_path == "/dev/dax0.0"
        assert parsed.device_uuid == "550e8400-e29b-41d4-a716-446655440000"
        assert parsed.offset == 2097152
        assert parsed.length == 4096

    def test_get_access_resp_without_uuid(self):
        resp = GetAccessResp(
            status=0,
            dax_path="/dev/dax1.0",
            device_uuid="",
            offset=0,
            length=1024,
        )
        data = resp.pack()
        parsed = GetAccessResp.unpack(data)

        assert parsed.dax_path == "/dev/dax1.0"
        assert parsed.device_uuid == ""
        assert parsed.offset == 0
        assert parsed.length == 1024


# =============================================================================
# Device header write / read / clear round-trip (using temp file)
# =============================================================================


class TestDeviceHeaderRoundTrip:
    """Write, read, and clear device headers on a regular file (mock device)."""

    @pytest.fixture
    def tmp_device(self, tmp_path):
        """Create a temp file simulating a 4KB DAX device."""
        p = tmp_path / "mock_dax"
        p.write_bytes(b"\x00" * 4096)
        return str(p)

    def test_write_and_read(self, tmp_device):
        uuid_str = write_device_header(tmp_device)
        assert len(uuid_str) == 36  # "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

        read_back = read_device_uuid(tmp_device)
        assert read_back == uuid_str

    def test_clear(self, tmp_device):
        write_device_header(tmp_device)
        assert read_device_uuid(tmp_device) is not None

        clear_device_header(tmp_device)
        assert read_device_uuid(tmp_device) is None

    def test_force_regenerate(self, tmp_device):
        uuid1 = write_device_header(tmp_device)
        uuid2 = write_device_header(tmp_device)
        assert uuid1 != uuid2  # Each call generates a new UUID
