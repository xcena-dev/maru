# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Guards for MaruShmClient.get_dax_path on the *real* client.

The unit-test autouse fixture swaps MaruShmClient for MockShmClient, which can
hide a method that exists only on the mock. We bind the real class at import
time (before the fixture patches the module attribute) so these tests exercise
the production class — the server's GET_USAGE device breakdown depends on it.
"""

from maru_shm.client import MaruShmClient as RealShmClient


def test_real_client_defines_get_dax_path():
    # Regression: get_dax_path must exist on the real client, not just the mock
    # (server.get_usage -> AllocationManager.devices_by_instance calls it).
    assert hasattr(RealShmClient, "get_dax_path")


def test_get_dax_path_reads_path_cache():
    client = RealShmClient(address="127.0.0.1:9850")
    client._path_cache[42] = "/dev/dax0.0"
    assert client.get_dax_path(42) == "/dev/dax0.0"
    assert client.get_dax_path(999) is None  # cache miss -> None, not an error
