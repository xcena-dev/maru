# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Integration tests for resource manager lifecycle.

Builds the maru-resource-manager binary from source via cmake, then runs
lifecycle tests against it. Requires cmake and a C++17 compiler.

Run with: pytest tests/integration/test_resource_manager_lifecycle.py -m integration

Validates:
- Idle timeout auto-shutdown
- Auto-restart after shutdown
- Stats on empty pool
- Multiple clients on same server
"""

import os
import shutil
import subprocess
import tempfile
import time

import pytest

from maru_shm.client import MaruShmClient

# Source directory for the C++ resource manager
RM_SOURCE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "maru_resource_manager")
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def rm_binary(tmp_path_factory):
    """Build the maru-resource-manager binary from source via cmake.

    This is session-scoped so the binary is built only once per test session.
    Skips all tests if cmake is not available or the build fails.
    """
    cmake = shutil.which("cmake")
    if cmake is None:
        pytest.skip("cmake not found — cannot build maru-resource-manager")
    assert cmake is not None  # type narrowing for Pylance

    cmake_file = os.path.join(RM_SOURCE_DIR, "CMakeLists.txt")
    if not os.path.isfile(cmake_file):
        pytest.skip(
            f"maru_resource_manager source not found at {RM_SOURCE_DIR}"
        )

    build_dir = str(tmp_path_factory.mktemp("rm_build"))

    # Configure
    try:
        subprocess.run(
            [cmake, "-S", RM_SOURCE_DIR, "-B", build_dir],
            check=True,
            capture_output=True,
            timeout=60,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        pytest.skip(f"cmake configure failed: {exc}")

    # Build
    try:
        subprocess.run(
            [cmake, "--build", build_dir, "-j"],
            check=True,
            capture_output=True,
            timeout=120,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        pytest.skip(f"cmake build failed: {exc}")

    binary = os.path.join(build_dir, "maru-resource-manager")
    if not os.path.isfile(binary):
        pytest.skip(f"Binary not produced at {binary}")

    return binary


def _wait_for_socket(sock_path, timeout=5.0):
    """Wait until the UDS socket exists and is connectable."""
    import socket

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if os.path.exists(sock_path):
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                s.connect(sock_path)
                s.close()
                return True
            except OSError:
                s.close()
        time.sleep(0.1)
    return False


def _start_rm(binary, sock_path, state_dir, idle_timeout=3):
    """Start the resource manager binary with custom config."""
    proc = subprocess.Popen(
        [
            binary,
            "--socket-path",
            sock_path,
            "--state-dir",
            state_dir,
            "--idle-timeout",
            str(idle_timeout),
            "--log-level",
            "debug",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return proc


class TestIdleTimeout:
    """Test idle timeout auto-shutdown."""

    def test_auto_exit_when_idle(self, rm_binary):
        """Server exits after idle timeout with no active allocations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sock_path = os.path.join(tmpdir, "rm.sock")
            state_dir = os.path.join(tmpdir, "state")
            os.makedirs(state_dir)

            proc = _start_rm(rm_binary, sock_path, state_dir, idle_timeout=2)
            try:
                assert _wait_for_socket(sock_path), "Server failed to start"
                assert proc.poll() is None, "Server exited prematurely"

                # Wait for idle timeout (2s + buffer)
                time.sleep(4)

                # Server should have auto-exited
                assert proc.poll() is not None, (
                    "Server did not exit after idle timeout"
                )
                assert proc.returncode == 0
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=5)

    def test_stats_on_empty_pool(self, rm_binary):
        """Stats works on a server with no DAX devices (empty pool)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sock_path = os.path.join(tmpdir, "rm.sock")
            state_dir = os.path.join(tmpdir, "state")
            os.makedirs(state_dir)

            proc = _start_rm(rm_binary, sock_path, state_dir, idle_timeout=10)
            try:
                assert _wait_for_socket(sock_path), "Server failed to start"

                client = MaruShmClient(socket_path=sock_path)
                pools = client.stats()

                # No DAX devices -> empty or has pools depending on environment
                assert isinstance(pools, list)
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=5)


class TestAutoRestart:
    """Test auto-restart after idle exit."""

    def test_restart_after_idle_exit(self, rm_binary):
        """After idle exit, a new binary can be started on the same socket."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sock_path = os.path.join(tmpdir, "rm.sock")
            state_dir = os.path.join(tmpdir, "state")
            os.makedirs(state_dir)

            # Start and let it die via idle timeout
            proc1 = _start_rm(rm_binary, sock_path, state_dir, idle_timeout=2)
            try:
                assert _wait_for_socket(sock_path), "Server failed to start"
                time.sleep(4)
                assert proc1.poll() is not None, "Server did not exit"
            finally:
                if proc1.poll() is None:
                    proc1.terminate()
                    proc1.wait(timeout=5)

            # Stale socket file may remain -- new server should handle it
            # Start a new instance on the same socket
            proc2 = _start_rm(
                rm_binary, sock_path, state_dir, idle_timeout=10
            )
            try:
                assert _wait_for_socket(sock_path), (
                    "Restarted server failed to start"
                )

                client = MaruShmClient(socket_path=sock_path)
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
            sock_path = os.path.join(tmpdir, "rm.sock")
            state_dir = os.path.join(tmpdir, "state")
            os.makedirs(state_dir)

            proc = _start_rm(rm_binary, sock_path, state_dir, idle_timeout=10)
            try:
                assert _wait_for_socket(sock_path), "Server failed to start"

                client_a = MaruShmClient(socket_path=sock_path)
                client_b = MaruShmClient(socket_path=sock_path)

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
            sock_path = os.path.join(tmpdir, "rm.sock")
            state_dir = os.path.join(tmpdir, "state")
            os.makedirs(state_dir)

            proc = _start_rm(rm_binary, sock_path, state_dir, idle_timeout=10)
            try:
                assert _wait_for_socket(sock_path), "Server failed to start"

                # Client A connects and disconnects abruptly
                client_a = MaruShmClient(socket_path=sock_path)
                client_a.stats()
                client_a.close()

                # Server should still be running
                assert proc.poll() is None

                # Client B should work fine
                client_b = MaruShmClient(socket_path=sock_path)
                pools = client_b.stats()
                assert isinstance(pools, list)
                client_b.close()

                # Server still alive
                assert proc.poll() is None
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=5)
