# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for maru_shm.device_scanner."""

import struct
from unittest import mock

from maru_shm.device_scanner import (
    _HEADER_FORMAT,
    _HEADER_MAGIC,
    _uuid_to_string,
    read_device_uuid,
    scan_dax_devices,
)


# Helper to build a valid 32-byte header
def _make_header(
    magic=_HEADER_MAGIC,
    version=1,
    uuid_bytes=b"\x55\x0e\x84\x00\xe2\x9b\x41\xd4\xa7\x16\x44\x66\x55\x44\x00\x00",
    reserved=0,
):
    return struct.pack(_HEADER_FORMAT, magic, version, uuid_bytes, reserved)


SAMPLE_UUID_BYTES = (
    b"\x55\x0e\x84\x00\xe2\x9b\x41\xd4\xa7\x16\x44\x66\x55\x44\x00\x00"
)
SAMPLE_UUID_STR = "550e8400-e29b-41d4-a716-446655440000"


# ── _uuid_to_string ──────────────────────────────────────────────────────────


class TestUuidToString:
    def test_known_uuid(self):
        assert _uuid_to_string(SAMPLE_UUID_BYTES) == SAMPLE_UUID_STR

    def test_all_zeros(self):
        assert _uuid_to_string(b"\x00" * 16) == "00000000-0000-0000-0000-000000000000"

    def test_all_ff(self):
        assert _uuid_to_string(b"\xff" * 16) == "ffffffff-ffff-ffff-ffff-ffffffffffff"


# ── read_device_uuid ─────────────────────────────────────────────────────────


class TestReadDeviceUuid:
    def test_valid_header(self, tmp_path):
        dev = tmp_path / "dax0.0"
        dev.write_bytes(_make_header())
        assert read_device_uuid(str(dev)) == SAMPLE_UUID_STR

    def test_bad_magic(self, tmp_path):
        dev = tmp_path / "dax0.0"
        dev.write_bytes(_make_header(magic=b"BADMAGIC"))
        assert read_device_uuid(str(dev)) is None

    def test_short_data(self, tmp_path):
        dev = tmp_path / "dax0.0"
        dev.write_bytes(b"\x00" * 16)  # too short
        assert read_device_uuid(str(dev)) is None

    def test_nonexistent_path(self):
        assert read_device_uuid("/dev/dax_nonexistent_999") is None

    def test_version_field_ignored(self, tmp_path):
        """Version field is currently not validated — any version returns UUID."""
        dev = tmp_path / "dax0.0"
        dev.write_bytes(_make_header(version=99))
        assert read_device_uuid(str(dev)) == SAMPLE_UUID_STR

    def test_os_error_returns_none(self):
        with mock.patch(
            "maru_shm.device_scanner.os.open", side_effect=OSError("perm")
        ):
            assert read_device_uuid("/dev/dax0.0") is None


# ── scan_dax_devices ─────────────────────────────────────────────────────────


class TestScanDaxDevices:
    def test_no_sysfs_dir(self):
        with mock.patch("maru_shm.device_scanner.os.path.isdir", return_value=False):
            assert scan_dax_devices() == []

    def test_listdir_oserror(self):
        with mock.patch(
            "maru_shm.device_scanner.os.path.isdir", return_value=True
        ), mock.patch(
            "maru_shm.device_scanner.os.listdir", side_effect=OSError
        ):
            assert scan_dax_devices() == []

    def test_scans_devices_with_valid_headers(self):
        uuid_a = b"\x01" * 16
        uuid_b = b"\x02" * 16

        with mock.patch(
            "maru_shm.device_scanner.os.path.isdir", return_value=True
        ), mock.patch(
            "maru_shm.device_scanner.os.listdir",
            return_value=["dax0.0", "dax1.0"],
        ), mock.patch(
            "maru_shm.device_scanner.os.path.exists", return_value=True
        ), mock.patch(
            "maru_shm.device_scanner.read_device_uuid"
        ) as mock_read:
            mock_read.side_effect = [
                _uuid_to_string(uuid_a),
                _uuid_to_string(uuid_b),
            ]
            results = scan_dax_devices()

        assert len(results) == 2
        assert results[0] == (_uuid_to_string(uuid_a), "/dev/dax0.0")
        assert results[1] == (_uuid_to_string(uuid_b), "/dev/dax1.0")

    def test_skips_device_without_valid_header(self):
        with mock.patch(
            "maru_shm.device_scanner.os.path.isdir", return_value=True
        ), mock.patch(
            "maru_shm.device_scanner.os.listdir", return_value=["dax0.0"]
        ), mock.patch(
            "maru_shm.device_scanner.os.path.exists", return_value=True
        ), mock.patch(
            "maru_shm.device_scanner.read_device_uuid", return_value=None
        ):
            assert scan_dax_devices() == []

    def test_skips_nonexistent_dev_path(self):
        with mock.patch(
            "maru_shm.device_scanner.os.path.isdir", return_value=True
        ), mock.patch(
            "maru_shm.device_scanner.os.listdir", return_value=["dax0.0"]
        ), mock.patch(
            "maru_shm.device_scanner.os.path.exists", return_value=False
        ):
            assert scan_dax_devices() == []
