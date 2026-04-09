# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for HANDSHAKE device list and MetaServer aggregation."""

from unittest.mock import MagicMock, patch

import pytest

from maru_common.protocol import HandshakeRequest
from maru_server.rpc_handler_mixin import RpcHandlerMixin
from maru_shm.ipc import NodeRegisterResp


# ── _handle_handshake (RpcHandlerMixin) ──────────────────────────────────────


class TestHandleHandshake:
    """Test that _handle_handshake extracts device mappings and forwards them."""

    def _make_mixin(self):
        mixin = object.__new__(RpcHandlerMixin)
        mixin._server = MagicMock()
        mixin._server.rm_address = "10.0.0.1:9850"
        return mixin

    def test_with_hostname_and_devices(self):
        mixin = self._make_mixin()
        devices = [
            {"uuid": "uuid-A", "dax_path": "/dev/dax0.0"},
            {"uuid": "uuid-B", "dax_path": "/dev/dax1.0"},
        ]
        req = HandshakeRequest(hostname="node-0", devices=devices)
        resp = mixin._handle_handshake(req)

        assert resp["success"] is True
        assert resp["rm_address"] == "10.0.0.1:9850"
        mixin._server.register_handler_devices.assert_called_once_with(
            "node-0", devices
        )

    def test_empty_devices_list(self):
        mixin = self._make_mixin()
        req = HandshakeRequest(hostname="node-0", devices=[])
        resp = mixin._handle_handshake(req)

        assert resp["success"] is True
        mixin._server.register_handler_devices.assert_called_once_with("node-0", [])

    def test_no_hostname_skips_registration(self):
        mixin = self._make_mixin()
        req = HandshakeRequest(
            devices=[{"uuid": "uuid-A", "dax_path": "/dev/dax0.0"}]
        )
        resp = mixin._handle_handshake(req)

        assert resp["success"] is True
        mixin._server.register_handler_devices.assert_not_called()

    def test_no_devices_key_skips_registration(self):
        mixin = self._make_mixin()
        req = HandshakeRequest(hostname="node-0")
        resp = mixin._handle_handshake(req)

        assert resp["success"] is True
        mixin._server.register_handler_devices.assert_not_called()

    def test_default_request_skips_registration(self):
        mixin = self._make_mixin()
        req = HandshakeRequest()
        resp = mixin._handle_handshake(req)

        assert resp["success"] is True
        mixin._server.register_handler_devices.assert_not_called()

    def test_non_dataclass_request_skips_registration(self):
        mixin = self._make_mixin()
        resp = mixin._handle_handshake(None)

        assert resp["success"] is True
        mixin._server.register_handler_devices.assert_not_called()


# ── MaruServer.register_handler_devices + _send_node_register ────────────────


class TestServerDeviceAggregation:
    """Test MaruServer stores handler devices and sends NODE_REGISTER to RM."""

    def _make_server(self, local_devices=None):
        """Create a MaruServer with mocked dependencies."""
        from maru_server.server import MaruServer

        with patch.object(MaruServer, "__init__", lambda self: None):
            server = MaruServer.__new__(MaruServer)

        server._node_devices = {}
        server._hostname = "meta-node"
        server._allocation_manager = MagicMock()
        server._rm_address = "10.0.0.1:9850"

        if local_devices:
            server._node_devices[server._hostname] = local_devices

        return server

    def test_register_stores_device_list(self):
        server = self._make_server()
        devices = [
            {"uuid": "uuid-A", "dax_path": "/dev/dax0.0"},
            {"uuid": "uuid-B", "dax_path": "/dev/dax1.0"},
        ]
        mock_resp = NodeRegisterResp(status=0, matched=2, total=2)
        server._allocation_manager._client.register_node.return_value = mock_resp

        server.register_handler_devices("node-0", devices)

        assert "node-0" in server._node_devices
        assert server._node_devices["node-0"] == [
            ("uuid-A", "/dev/dax0.0"),
            ("uuid-B", "/dev/dax1.0"),
        ]

    def test_register_sends_node_register_to_rm(self):
        server = self._make_server(
            local_devices=[("uuid-A", "/dev/dax1.0")]
        )
        mock_resp = NodeRegisterResp(status=0, matched=1, total=2)
        server._allocation_manager._client.register_node.return_value = mock_resp

        devices = [{"uuid": "uuid-A", "dax_path": "/dev/dax0.0"}]
        server.register_handler_devices("handler-node", devices)

        call_args = server._allocation_manager._client.register_node.call_args[0][0]
        hostnames = {node_id for node_id, _ in call_args}
        assert hostnames == {"meta-node", "handler-node"}

    def test_multiple_handlers_aggregate(self):
        server = self._make_server()
        mock_resp = NodeRegisterResp(status=0, matched=0, total=0)
        server._allocation_manager._client.register_node.return_value = mock_resp

        server.register_handler_devices(
            "node-0", [{"uuid": "uuid-A", "dax_path": "/dev/dax0.0"}]
        )
        server.register_handler_devices(
            "node-1", [{"uuid": "uuid-A", "dax_path": "/dev/dax1.0"}]
        )

        assert len(server._node_devices) == 2
        # Last NODE_REGISTER call should include both nodes
        call_args = server._allocation_manager._client.register_node.call_args[0][0]
        hostnames = {node_id for node_id, _ in call_args}
        assert hostnames == {"node-0", "node-1"}

    def test_send_node_register_empty_devices_noop(self):
        server = self._make_server()
        server._send_node_register()
        server._allocation_manager._client.register_node.assert_not_called()

    def test_send_node_register_exception_logged(self):
        server = self._make_server(
            local_devices=[("uuid-A", "/dev/dax0.0")]
        )
        server._allocation_manager._client.register_node.side_effect = (
            ConnectionRefusedError("RM down")
        )
        # Should not raise
        server._send_node_register()


# ── Handler.connect() device scanning ────────────────────────────────────────


class TestHandlerDeviceScan:
    """Test that handler scans devices and includes them in HANDSHAKE."""

    def test_connect_sends_devices_in_handshake(self):
        """Verify scan_dax_devices result is passed to rpc.handshake()."""
        mock_devices = [
            ("uuid-A", "/dev/dax0.0"),
            ("uuid-B", "/dev/dax1.0"),
        ]
        with patch(
            "maru_shm.device_scanner.scan_dax_devices", return_value=mock_devices
        ), patch("platform.node", return_value="test-node"):
            from maru_handler.handler import MaruHandler

            handler = object.__new__(MaruHandler)
            handler._connected = False
            handler._rpc = MagicMock()
            handler._rpc.handshake.return_value = {"rm_address": "10.0.0.1:9850"}
            handler._config = MagicMock()
            handler._config.rm_address = "10.0.0.1:9850"
            handler._config.pool_size = 2 * 1024 * 1024
            handler._config.chunk_size_bytes = 4096
            handler._config.instance_id = "test"
            handler._mapper = None
            handler._owned = None

            # Mock the alloc step to isolate handshake testing
            handler._rpc.request_alloc.return_value = MagicMock(
                success=False, error="test"
            )

            handler.connect()

            handshake_call = handler._rpc.handshake.call_args
            extra = handshake_call[1].get("extra") or handshake_call[0][0]
            assert extra["hostname"] == "test-node"
            assert len(extra["devices"]) == 2
            assert extra["devices"][0] == {
                "uuid": "uuid-A",
                "dax_path": "/dev/dax0.0",
            }
