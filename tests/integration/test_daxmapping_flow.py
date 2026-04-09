# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Integration test for multi-node device mapping flow.

Tests the end-to-end data flow:
    Handler → HANDSHAKE(devices) → MetaServer → NODE_REGISTER → RM

Uses a mock TCP server in place of the real Resource Manager to capture
the NODE_REGISTER message and verify its contents.
"""

import socket
import struct
import threading
import time
from unittest.mock import patch

import pytest

from maru_handler.rpc_client import RpcClient
from maru_server import MaruServer, RpcServer
from maru_shm.ipc import (
    HEADER_SIZE,
    MsgHeader,
    MsgType,
    NodeRegisterResp,
)

pytestmark = pytest.mark.integration


# =============================================================================
# Mock Resource Manager (TCP server)
# =============================================================================


class MockRM:
    """Minimal TCP server that handles is_running probe and NODE_REGISTER."""

    def __init__(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(5)
        self._sock.settimeout(5.0)
        self.port = self._sock.getsockname()[1]
        self.address = f"127.0.0.1:{self.port}"
        self._running = True
        self._thread = None
        # Captured NODE_REGISTER data
        self.node_register_calls: list[list[tuple[str, list[tuple[str, str]]]]] = []
        self._lock = threading.Lock()

    def start(self):
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        self._sock.close()
        if self._thread:
            self._thread.join(timeout=5.0)

    def _serve(self):
        while self._running:
            try:
                conn, _ = self._sock.accept()
            except (TimeoutError, OSError):
                continue
            threading.Thread(
                target=self._handle_conn, args=(conn,), daemon=True
            ).start()

    def _handle_conn(self, conn: socket.socket):
        conn.settimeout(5.0)
        try:
            while self._running:
                # Read header (12 bytes)
                hdr_data = self._recv_exact(conn, HEADER_SIZE)
                if not hdr_data:
                    break
                hdr = MsgHeader.unpack(hdr_data)

                # Read payload
                payload = b""
                if hdr.payload_len > 0:
                    payload = self._recv_exact(conn, hdr.payload_len)
                    if not payload:
                        break

                if hdr.msg_type == MsgType.STATS_REQ:
                    # is_running() probe sends STATS_REQ — respond with empty stats
                    self._send_response(conn, MsgType.STATS_RESP, struct.pack("<I", 0))
                elif hdr.msg_type == MsgType.NODE_REGISTER_REQ:
                    self._handle_node_register(conn, payload)
                else:
                    # Unknown message — send error
                    self._send_response(
                        conn,
                        MsgType.ERROR_RESP,
                        struct.pack("<i", -1) + b"unknown",
                    )
        except (TimeoutError, OSError):
            pass
        finally:
            conn.close()

    def _handle_node_register(self, conn: socket.socket, payload: bytes):
        # Parse the NODE_REGISTER payload manually
        nodes = []
        offset = 0
        (num_nodes,) = struct.unpack_from("<I", payload, offset)
        offset += 4
        for _ in range(num_nodes):
            (nid_len,) = struct.unpack_from("<H", payload, offset)
            offset += 2
            node_id = payload[offset : offset + nid_len].decode()
            offset += nid_len
            (num_devices,) = struct.unpack_from("<I", payload, offset)
            offset += 4
            devices = []
            for _ in range(num_devices):
                (uuid_len,) = struct.unpack_from("<H", payload, offset)
                offset += 2
                uuid_str = payload[offset : offset + uuid_len].decode()
                offset += uuid_len
                (path_len,) = struct.unpack_from("<H", payload, offset)
                offset += 2
                path_str = payload[offset : offset + path_len].decode()
                offset += path_len
                devices.append((uuid_str, path_str))
            nodes.append((node_id, devices))

        with self._lock:
            self.node_register_calls.append(nodes)

        # Respond with success
        resp = NodeRegisterResp(status=0, matched=0, total=num_nodes)
        resp_data = struct.pack("<iII", resp.status, resp.matched, resp.total)
        self._send_response(conn, MsgType.NODE_REGISTER_RESP, resp_data)

    def _send_response(self, conn: socket.socket, msg_type: int, payload: bytes):
        hdr = MsgHeader(msg_type=msg_type, payload_len=len(payload))
        conn.sendall(hdr.pack() + payload)

    @staticmethod
    def _recv_exact(conn: socket.socket, n: int) -> bytes | None:
        buf = b""
        while len(buf) < n:
            chunk = conn.recv(n - len(buf))
            if not chunk:
                return None
            buf += chunk
        return buf


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_rm():
    rm = MockRM()
    rm.start()
    yield rm
    rm.stop()


@pytest.fixture
def zmq_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def meta_server(mock_rm, zmq_port):
    """Start MaruServer + RpcServer backed by mock RM."""
    # Patch scan_dax_devices so MetaServer sees its own "local" devices
    meta_devices = [
        ("uuid-A", "/dev/dax0.0"),
        ("uuid-B", "/dev/dax1.0"),
    ]
    with (
        patch("maru_server.server.scan_dax_devices", return_value=meta_devices),
        patch("maru_server.server.platform") as mock_platform,
    ):
        mock_platform.node.return_value = "meta-node"
        server = MaruServer(rm_address=mock_rm.address)

    rpc = RpcServer(server, host="127.0.0.1", port=zmq_port)
    thread = threading.Thread(target=rpc.start, daemon=True)
    thread.start()
    time.sleep(0.1)

    yield server

    rpc.stop()
    thread.join(timeout=5.0)


@pytest.fixture
def handler_client(zmq_port, meta_server):
    """Create a connected RPC client (simulating a handler)."""
    c = RpcClient(f"tcp://127.0.0.1:{zmq_port}", timeout_ms=5000)
    c.connect()
    yield c
    c.close()


# =============================================================================
# Tests
# =============================================================================


class TestDaxMappingFlow:
    """End-to-end tests for the device mapping flow."""

    def test_handshake_with_devices_triggers_node_register(
        self, handler_client, meta_server, mock_rm
    ):
        """Handler HANDSHAKE with devices → MetaServer → NODE_REGISTER to RM."""
        handshake_data = {
            "hostname": "handler-node",
            "devices": [
                {"uuid": "uuid-A", "dax_path": "/dev/dax1.0"},
                {"uuid": "uuid-B", "dax_path": "/dev/dax0.0"},
            ],
        }
        resp = handler_client.handshake(extra=handshake_data)

        assert resp["success"] is True
        assert "rm_address" in resp

        # Wait briefly for async NODE_REGISTER to be sent
        time.sleep(0.3)

        # Verify NODE_REGISTER was received by mock RM
        assert len(mock_rm.node_register_calls) >= 1
        last_call = mock_rm.node_register_calls[-1]

        # Should contain both meta-node and handler-node
        node_ids = {nid for nid, _ in last_call}
        assert "meta-node" in node_ids
        assert "handler-node" in node_ids

        # Verify handler-node device mappings
        handler_devices = dict(
            next(devs for nid, devs in last_call if nid == "handler-node")
        )
        assert handler_devices["uuid-A"] == "/dev/dax1.0"
        assert handler_devices["uuid-B"] == "/dev/dax0.0"

        # Verify meta-node device mappings (different paths for same UUIDs)
        meta_devices = dict(next(devs for nid, devs in last_call if nid == "meta-node"))
        assert meta_devices["uuid-A"] == "/dev/dax0.0"
        assert meta_devices["uuid-B"] == "/dev/dax1.0"

    def test_multiple_handlers_aggregate(self, handler_client, meta_server, mock_rm):
        """Two handlers register → both appear in NODE_REGISTER."""
        # First handler
        handler_client.handshake(
            extra={
                "hostname": "node-0",
                "devices": [{"uuid": "uuid-A", "dax_path": "/dev/dax2.0"}],
            }
        )
        time.sleep(0.2)

        # Second handler (reuse same client for simplicity)
        handler_client.handshake(
            extra={
                "hostname": "node-1",
                "devices": [{"uuid": "uuid-A", "dax_path": "/dev/dax3.0"}],
            }
        )
        time.sleep(0.2)

        # Last NODE_REGISTER should contain all nodes
        last_call = mock_rm.node_register_calls[-1]
        node_ids = {nid for nid, _ in last_call}
        assert "node-0" in node_ids
        assert "node-1" in node_ids
        assert "meta-node" in node_ids

    def test_handshake_without_devices_no_node_register(
        self, handler_client, meta_server, mock_rm
    ):
        """Legacy HANDSHAKE without devices should not trigger NODE_REGISTER
        (unless MetaServer already has its own devices)."""
        initial_count = len(mock_rm.node_register_calls)

        resp = handler_client.handshake(extra={})

        assert resp["success"] is True
        time.sleep(0.2)

        # No new NODE_REGISTER should be sent (no new node was added)
        assert len(mock_rm.node_register_calls) == initial_count

    def test_handshake_returns_rm_address(self, handler_client, meta_server, mock_rm):
        """HANDSHAKE response includes RM address for direct handler→RM access."""
        resp = handler_client.handshake(
            extra={
                "hostname": "test-node",
                "devices": [],
            }
        )
        assert resp["rm_address"] == mock_rm.address
