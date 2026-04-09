# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""pytest configuration and fixtures."""

import sys
from pathlib import Path

# Make tests/ importable so `from conftest import _make_handle` works
# in subdirectories under importlib import mode.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import socket
import tempfile
import threading
import time
from collections.abc import Generator
from unittest.mock import patch

import pytest

from maru_shm import MaruHandle


def _make_handle(region_id: int, length: int = 4096) -> MaruHandle:
    """Create a MaruHandle for testing (shared across test modules)."""
    return MaruHandle(region_id=region_id, offset=0, length=length, auth_token=12345)


# =============================================================================
# Common Type Fixtures
# =============================================================================


@pytest.fixture
def sample_handle() -> MaruHandle:
    """Create a sample MaruHandle for testing."""
    return MaruHandle(
        region_id=1,
        offset=4096,
        length=1024,
        auth_token=0xDEADBEEF,
    )


# =============================================================================
# Network Fixtures
# =============================================================================


@pytest.fixture
def server_port() -> int:
    """Get a random available port for testing."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def server_thread(server_port):
    """Start RPC server in a background thread."""
    from maru_server import MaruServer, RpcServer

    with patch("maru_server.server.scan_dax_devices", return_value=[]):
        maru_server = MaruServer()
    rpc_server = RpcServer(maru_server, host="127.0.0.1", port=server_port)

    thread = threading.Thread(target=rpc_server.start, daemon=True)
    thread.start()
    time.sleep(0.05)

    yield rpc_server

    rpc_server.stop()


# =============================================================================
# Temp File Fixtures
# =============================================================================


@pytest.fixture
def temp_mmap_file() -> Generator[str, None, None]:
    """Create a temporary file suitable for mmap testing."""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        # Write some zeros to create a file of reasonable size
        f.write(b"\x00" * 4096)
        temp_path = f.name

    yield temp_path

    import os

    try:
        os.unlink(temp_path)
    except OSError:
        pass


@pytest.fixture
def temp_large_mmap_file() -> Generator[str, None, None]:
    """Create a larger temporary file for mmap testing (1MB)."""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(b"\x00" * (1024 * 1024))
        temp_path = f.name

    yield temp_path

    import os

    try:
        os.unlink(temp_path)
    except OSError:
        pass


# =============================================================================
# pytest Configuration
# =============================================================================


def pytest_configure(config):
    """Add custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
