# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Integration tests for resource manager lifecycle.

Requires a pre-installed maru-resource-manager binary.

Run with: pytest tests/integration/test_resource_manager_lifecycle.py -m integration

Validates:
- Stats on empty pool
- Multiple clients on same server
- Client error when server is not running
- Graceful shutdown via SIGTERM
"""

import os
import shutil
import signal
import subprocess
import tempfile
import time

import pytest

from maru_shm.client import MaruShmClient

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def rm_binary():
    """Locate the installed maru-resource-manager binary.

    Expects the binary to already be installed (e.g. via ./install.sh).
    Skips all tests if the binary is not found.
    """
    binary = shutil.which("maru-resource-manager")
    if binary is None:
        pytest.skip("maru-resource-manager not installed — run ./install.sh first")
    return binary


def _find_free_port():
    """Find an available TCP port."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_server(host, port, timeout=5.0):
    """Wait until the TCP server is connectable."""
    import socket

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((host, port))
            s.close()
            return True
        except OSError:
            s.close()
        time.sleep(0.1)
    return False


def _start_rm(binary, state_dir, port):
    """Start the resource manager binary with custom config."""
    proc = subprocess.Popen(
        [
            binary,
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--state-dir",
            state_dir,
            "--log-level",
            "debug",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return proc


class TestGracefulShutdown:
    """Test graceful shutdown via SIGTERM."""

    def test_sigterm_shutdown(self, rm_binary):
        """Server exits cleanly on SIGTERM."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = os.path.join(tmpdir, "state")
            os.makedirs(state_dir)
            port = _find_free_port()

            proc = _start_rm(rm_binary, state_dir, port)
            try:
                assert _wait_for_server("127.0.0.1", port), "Server failed to start"
                assert proc.poll() is None, "Server exited prematurely"

                proc.send_signal(signal.SIGTERM)
                proc.wait(timeout=5)
                assert proc.returncode == 0
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=5)

    def test_stats_on_empty_pool(self, rm_binary):
        """Stats works on a server with no DAX devices (empty pool)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = os.path.join(tmpdir, "state")
            os.makedirs(state_dir)
            port = _find_free_port()

            proc = _start_rm(rm_binary, state_dir, port)
            try:
                assert _wait_for_server("127.0.0.1", port), "Server failed to start"

                client = MaruShmClient(address=f"127.0.0.1:{port}")
                pools = client.stats()

                # No DAX devices -> empty or has pools depending on environment
                assert isinstance(pools, list)
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=5)


class TestManualRestart:
    """Test manual restart after shutdown."""

    def test_restart_after_shutdown(self, rm_binary):
        """After SIGTERM, a new binary can be started on the same port."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = os.path.join(tmpdir, "state")
            os.makedirs(state_dir)
            port = _find_free_port()

            # Start and stop via SIGTERM
            proc1 = _start_rm(rm_binary, state_dir, port)
            try:
                assert _wait_for_server("127.0.0.1", port), "Server failed to start"
                proc1.send_signal(signal.SIGTERM)
                proc1.wait(timeout=5)
                assert proc1.returncode == 0
            finally:
                if proc1.poll() is None:
                    proc1.terminate()
                    proc1.wait(timeout=5)

            # Client should fail with ConnectionError before restart
            client = MaruShmClient(address=f"127.0.0.1:{port}")
            with pytest.raises(ConnectionError):
                client.stats()

            # Start a new instance on the same port
            proc2 = _start_rm(rm_binary, state_dir, port)
            try:
                assert _wait_for_server("127.0.0.1", port), (
                    "Restarted server failed to start"
                )

                pools = client.stats()
                assert isinstance(pools, list)
            finally:
                if proc2.poll() is None:
                    proc2.terminate()
                    proc2.wait(timeout=5)


class TestMultipleClientsIntegration:
    """Test multiple clients connecting to the same real resource manager."""

    def test_two_clients_stats(self, rm_binary):
        """Two clients can query stats independently."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = os.path.join(tmpdir, "state")
            os.makedirs(state_dir)
            port = _find_free_port()

            proc = _start_rm(rm_binary, state_dir, port)
            try:
                assert _wait_for_server("127.0.0.1", port), "Server failed to start"

                address = f"127.0.0.1:{port}"
                client_a = MaruShmClient(address=address)
                client_b = MaruShmClient(address=address)

                pools_a = client_a.stats()
                pools_b = client_b.stats()

                assert isinstance(pools_a, list)
                assert isinstance(pools_b, list)
                # Both should see the same pool state
                assert len(pools_a) == len(pools_b)
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=5)

    def test_client_disconnect_does_not_crash_server(self, rm_binary):
        """One client disconnecting doesn't crash the server."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = os.path.join(tmpdir, "state")
            os.makedirs(state_dir)
            port = _find_free_port()

            proc = _start_rm(rm_binary, state_dir, port)
            try:
                assert _wait_for_server("127.0.0.1", port), "Server failed to start"

                address = f"127.0.0.1:{port}"

                # Client A connects and disconnects abruptly
                client_a = MaruShmClient(address=address)
                client_a.stats()
                client_a.close()

                # Server should still be running
                assert proc.poll() is None

                # Client B should work fine
                client_b = MaruShmClient(address=address)
                pools = client_b.stats()
                assert isinstance(pools, list)
                client_b.close()

                # Server still alive
                assert proc.poll() is None
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=5)
