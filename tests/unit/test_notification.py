# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for PUB/SUB allocation notification (Phase 2)."""

import threading
import time
from unittest.mock import MagicMock, patch

import pytest
import zmq

from maru_common import MaruConfig, MessageType, Serializer
from maru_common.protocol import NewAllocationNotification
from maru_handler.handler import MaruHandler
from maru_server.rpc_server import RpcServer
from maru_server.server import MaruServer
from maru_shm import MaruHandle


class TestProtocolExtension:
    """Test NOTIFY_NEW_ALLOCATION protocol additions."""

    def test_message_type_value(self):
        assert MessageType.NOTIFY_NEW_ALLOCATION == 0x05

    def test_notification_dataclass(self):
        handle = MaruHandle(region_id=42, offset=0, length=4096, auth_token=123)
        notif = NewAllocationNotification(instance_id="test-inst", handle=handle)
        assert notif.instance_id == "test-inst"
        assert notif.handle.region_id == 42

    def test_serializer_encode_decode(self):
        """Verify notification can be serialized and deserialized."""
        serializer = Serializer()
        data = {
            "instance_id": "inst-1",
            "handle": MaruHandle(
                region_id=10, offset=0, length=1024, auth_token=99
            ).to_dict(),
        }
        encoded = serializer.encode(MessageType.NOTIFY_NEW_ALLOCATION, data, seq=0)
        header, payload = serializer.decode(encoded)
        assert header.msg_type == MessageType.NOTIFY_NEW_ALLOCATION
        assert payload["instance_id"] == "inst-1"
        assert payload["handle"]["region_id"] == 10


class TestServerNotificationCallback:
    """Test MaruServer notification callback wiring."""

    def test_set_notification_callback(self):
        server = MaruServer()
        callback = MagicMock()
        server.set_notification_callback(callback)
        assert server._notification_callback is callback

    def test_callback_called_on_alloc(self):
        """request_alloc should invoke the notification callback."""
        server = MaruServer()
        callback = MagicMock()
        server.set_notification_callback(callback)

        handle = server.request_alloc(instance_id="inst-1", size=4096)
        assert handle is not None

        callback.assert_called_once()
        call_kwargs = callback.call_args
        assert call_kwargs.kwargs["instance_id"] == "inst-1"
        assert call_kwargs.kwargs["handle"].region_id == handle.region_id

    def test_callback_not_called_on_alloc_failure(self, monkeypatch):
        """No notification if allocation fails."""
        server = MaruServer()
        callback = MagicMock()
        server.set_notification_callback(callback)

        monkeypatch.setattr(
            server._allocation_manager, "allocate", lambda *a, **kw: None
        )
        result = server.request_alloc(instance_id="inst-1", size=4096)
        assert result is None
        callback.assert_not_called()

    def test_callback_exception_does_not_break_alloc(self):
        """If callback raises, alloc should still succeed."""
        server = MaruServer()
        callback = MagicMock(side_effect=RuntimeError("boom"))
        server.set_notification_callback(callback)

        handle = server.request_alloc(instance_id="inst-1", size=4096)
        assert handle is not None  # alloc succeeded despite callback failure


class TestRpcServerPubSocket:
    """Test PUB socket in RpcServer."""

    def test_publish_notification(self):
        """RpcServer.publish_notification sends on PUB socket."""
        server = MaruServer()
        rpc = RpcServer(server, host="127.0.0.1", port=15555)

        # Mock the PUB socket
        mock_pub = MagicMock()
        rpc._pub_socket = mock_pub
        rpc._pub_serializer = Serializer()

        handle = MaruHandle(region_id=42, offset=0, length=4096, auth_token=123)
        rpc.publish_notification(instance_id="inst-1", handle=handle)

        mock_pub.send.assert_called_once()
        sent_data = mock_pub.send.call_args[0][0]
        # Verify it's a valid encoded message
        serializer = Serializer()
        header, payload = serializer.decode(sent_data)
        assert header.msg_type == MessageType.NOTIFY_NEW_ALLOCATION
        assert payload["instance_id"] == "inst-1"
        assert payload["handle"]["region_id"] == 42

    def test_publish_noop_when_no_pub_socket(self):
        """publish_notification is a no-op when PUB socket is None."""
        server = MaruServer()
        rpc = RpcServer(server, host="127.0.0.1", port=15555)
        rpc._pub_socket = None
        # Should not raise
        handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=1)
        rpc.publish_notification(instance_id="inst-1", handle=handle)


class TestHandlerOnNewAllocation:
    """Test MaruHandler._on_new_allocation callback."""

    def _make_handler(self, instance_id="handler-1"):
        config = MaruConfig(
            auto_connect=False,
            instance_id=instance_id,
            enable_notifications=False,  # don't create SUB socket
        )
        handler = MaruHandler(config)
        handler._connected = True
        return handler

    def test_skip_own_allocation(self):
        handler = self._make_handler(instance_id="my-inst")
        handler._mapper = MagicMock()

        notif = NewAllocationNotification(
            instance_id="my-inst",
            handle=MaruHandle(region_id=1, offset=0, length=4096, auth_token=1),
        )
        handler._on_new_allocation(notif)
        handler._mapper.map_region.assert_not_called()

    def test_skip_already_mapped(self):
        handler = self._make_handler()
        handler._mapper = MagicMock()
        handler._mapper.get_region.return_value = MagicMock()  # already mapped

        notif = NewAllocationNotification(
            instance_id="other-inst",
            handle=MaruHandle(region_id=1, offset=0, length=4096, auth_token=1),
        )
        handler._on_new_allocation(notif)
        handler._mapper.map_region.assert_not_called()

    def test_maps_new_shared_region(self):
        handler = self._make_handler()
        handler._mapper = MagicMock()
        handler._mapper.get_region.return_value = None  # not mapped yet

        handle = MaruHandle(region_id=42, offset=0, length=4096, auth_token=1)
        notif = NewAllocationNotification(
            instance_id="other-inst",
            handle=handle,
        )
        handler._on_new_allocation(notif)
        handler._mapper.map_region.assert_called_once_with(handle, prefault=False)

    def test_map_failure_does_not_raise(self):
        handler = self._make_handler()
        handler._mapper = MagicMock()
        handler._mapper.get_region.return_value = None
        handler._mapper.map_region.side_effect = RuntimeError("mmap failed")

        notif = NewAllocationNotification(
            instance_id="other-inst",
            handle=MaruHandle(region_id=1, offset=0, length=4096, auth_token=1),
        )
        # Should not raise
        handler._on_new_allocation(notif)

    def test_skip_when_not_connected(self):
        handler = self._make_handler()
        handler._connected = False
        handler._mapper = MagicMock()

        notif = NewAllocationNotification(
            instance_id="other-inst",
            handle=MaruHandle(region_id=1, offset=0, length=4096, auth_token=1),
        )
        handler._on_new_allocation(notif)
        handler._mapper.map_region.assert_not_called()

    def test_skip_when_closing(self):
        handler = self._make_handler()
        handler._closing.set()
        handler._mapper = MagicMock()

        notif = NewAllocationNotification(
            instance_id="other-inst",
            handle=MaruHandle(region_id=1, offset=0, length=4096, auth_token=1),
        )
        handler._on_new_allocation(notif)
        handler._mapper.map_region.assert_not_called()


class TestConfigNotification:
    """Test MaruConfig notification settings."""

    def test_default_enabled(self):
        config = MaruConfig(auto_connect=False)
        assert config.enable_notifications is True

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("MARU_ENABLE_NOTIFICATIONS", "false")
        config = MaruConfig(auto_connect=False)
        assert config.enable_notifications is False

    def test_explicit_disable(self):
        config = MaruConfig(auto_connect=False, enable_notifications=False)
        assert config.enable_notifications is False


class TestAsyncClientNotifyPort:
    """Test RpcAsyncClient notify_port parameter."""

    def test_notify_url_derived(self):
        from maru_handler.rpc_async_client import RpcAsyncClient

        client = RpcAsyncClient(
            server_url="tcp://localhost:5555", notify_port=5556
        )
        assert client._notify_url == "tcp://localhost:5556"

    def test_notify_url_none_when_no_port(self):
        from maru_handler.rpc_async_client import RpcAsyncClient

        client = RpcAsyncClient(server_url="tcp://localhost:5555")
        assert client._notify_url is None

    def test_handler_derives_notify_port(self):
        config = MaruConfig(
            auto_connect=False,
            server_url="tcp://localhost:11001",
            enable_notifications=True,
        )
        handler = MaruHandler(config)
        assert handler._rpc._notify_url == "tcp://localhost:11002"

    def test_handler_no_notify_when_disabled(self):
        config = MaruConfig(
            auto_connect=False,
            server_url="tcp://localhost:11001",
            enable_notifications=False,
        )
        handler = MaruHandler(config)
        assert handler._rpc._notify_url is None
