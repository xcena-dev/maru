"""Device occupancy must not be overwritten by cumulative command counters."""

import pytest

from maru_gaia.plugin import _decode_smart
import pyxif


@pytest.mark.parametrize("version,status_offset", [(2, 144), (3, 272)])
def test_pin_occupancy_and_command_bytes_are_distinct(version, status_offset):
    log = bytearray(4096)
    base = pyxif.GAIA_SMART_STATS_OFFSET
    log[base:base + 8] = version.to_bytes(8, "little")
    occupancy_offset = base + status_offset + 16
    log[occupancy_offset:occupancy_offset + 8] = (123456).to_bytes(8, "little")
    command_offset = base + 1536 + 16
    log[command_offset:command_offset + 16] = (2**90 + 17).to_bytes(16, "little")
    decoded = _decode_smart(bytes(log))
    assert decoded["pin_bytes"] == 123456
    assert decoded["pin_command_bytes"] == 2**90 + 17
