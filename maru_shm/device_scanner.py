# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Scan local DEV_DAX devices and read UUID headers."""

import logging
import mmap
import os
import struct

logger = logging.getLogger(__name__)

# Must match C++ device_header.h
_HEADER_MAGIC = b"MARUDEV\x00"
_HEADER_SIZE = 32
_HEADER_FORMAT = "<8sI16sI"  # magic(8) + version(u32) + uuid(16) + reserved(u32)
_DEFAULT_MAP_SIZE = 2 * 1024 * 1024  # 2 MiB fallback


def _get_dax_align(dax_path: str) -> int:
    """Read device alignment from sysfs. DEV_DAX requires mmap size >= alignment."""
    dev_name = os.path.basename(dax_path)
    align_path = f"/sys/bus/dax/devices/{dev_name}/align"
    try:
        with open(align_path) as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return _DEFAULT_MAP_SIZE


def read_device_uuid(dax_path: str) -> str | None:
    """Read UUID from a DEV_DAX device header.

    Returns UUID string (e.g. "550e8400-e29b-...") or None if no valid header.
    """
    map_size = _get_dax_align(dax_path)
    try:
        fd = os.open(dax_path, os.O_RDONLY)
        try:
            # For regular files (e.g. tests), clamp to file size
            file_size = os.fstat(fd).st_size
            if file_size > 0 and file_size < map_size:
                map_size = file_size
            mm = mmap.mmap(fd, map_size, mmap.MAP_SHARED, mmap.PROT_READ)
            try:
                data = mm[:_HEADER_SIZE]
            finally:
                mm.close()
        finally:
            os.close(fd)
    except (OSError, ValueError):
        logger.debug("Cannot read device header from %s", dax_path, exc_info=True)
        return None

    if len(data) < _HEADER_SIZE:
        return None

    magic, version, uuid_bytes, _ = struct.unpack(_HEADER_FORMAT, data)
    if magic != _HEADER_MAGIC:
        return None

    return _uuid_to_string(uuid_bytes)


def scan_dax_devices() -> list[tuple[str, str]]:
    """Scan local DEV_DAX devices and read their UUIDs.

    Returns list of (uuid, dax_path) for devices with valid headers.
    """
    sysfs_dir = "/sys/bus/dax/devices"
    if not os.path.isdir(sysfs_dir):
        return []

    results = []
    try:
        entries = sorted(os.listdir(sysfs_dir))
    except OSError:
        return []

    for entry in entries:
        dev_path = f"/dev/{entry}"
        if not os.path.exists(dev_path):
            continue
        uuid = read_device_uuid(dev_path)
        if uuid:
            results.append((uuid, dev_path))
            logger.debug("Scanned %s: UUID=%s", dev_path, uuid)

    return results


def _uuid_to_string(uuid_bytes: bytes) -> str:
    """Format 16 UUID bytes as xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx."""
    b = uuid_bytes
    return (
        f"{b[0]:02x}{b[1]:02x}{b[2]:02x}{b[3]:02x}-"
        f"{b[4]:02x}{b[5]:02x}-"
        f"{b[6]:02x}{b[7]:02x}-"
        f"{b[8]:02x}{b[9]:02x}-"
        f"{b[10]:02x}{b[11]:02x}{b[12]:02x}{b[13]:02x}{b[14]:02x}{b[15]:02x}"
    )
