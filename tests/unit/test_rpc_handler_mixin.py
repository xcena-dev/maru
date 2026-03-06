# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA
"""Unit tests for RpcHandlerMixin to achieve 100% coverage."""

from unittest.mock import MagicMock

from maru_common import MessageType
from maru_server.rpc_handler_mixin import RpcHandlerMixin
from maru_server.server import MaruServer


class MockRequest:
    """Mock request object for testing handlers."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class ConcreteHandler(RpcHandlerMixin):
    """Concrete class using the mixin for testing."""

    def __init__(self, server):
        self._server = server


class TestRpcHandlerMixin:
    """Test RpcHandlerMixin dispatch and all handler methods."""

    def _make_handler(self):
        server = MaruServer()
        handler = ConcreteHandler(server)
        return handler, server

    def test_get_handlers_cached(self):
        """_get_handlers builds dict once and caches it."""
        handler, _ = self._make_handler()
        h1 = handler._get_handlers()
        h2 = handler._get_handlers()
        assert h1 is h2

    def test_handle_message_unknown_type(self):
        """_handle_message returns error for unknown type."""
        handler, _ = self._make_handler()
        resp = handler._handle_message(99999, {})
        assert "error" in resp
        assert "Unknown message type" in resp["error"]

    def test_handle_message_valid_dispatch(self):
        """_handle_message dispatches to correct handler (line 52)."""
        handler, _ = self._make_handler()
        resp = handler._handle_message(MessageType.HEARTBEAT.value, {})
        assert resp == {}

    def test_handle_request_alloc_success(self):
        handler, server = self._make_handler()
        req = MockRequest(instance_id="inst1", size=4096)
        resp = handler._handle_request_alloc(req)
        assert resp["success"] is True
        assert "handle" in resp

    def test_handle_request_alloc_failure(self, monkeypatch):
        handler, server = self._make_handler()
        monkeypatch.setattr(
            server._allocation_manager, "allocate", lambda instance_id, size: None
        )
        req = MockRequest(instance_id="inst1", size=4096)
        resp = handler._handle_request_alloc(req)
        assert resp["success"] is False
        assert resp["error"] == "Allocation failed"

    def test_handle_return_alloc(self):
        handler, server = self._make_handler()
        handle = server.request_alloc("inst1", 4096)
        req = MockRequest(instance_id="inst1", region_id=handle.region_id)
        resp = handler._handle_return_alloc(req)
        assert resp["success"] is True

    def test_handle_list_allocations(self):
        handler, server = self._make_handler()
        server.request_alloc("inst1", 4096)
        req = MockRequest(exclude_instance_id=None)
        resp = handler._handle_list_allocations(req)
        assert resp["success"] is True
        assert len(resp["allocations"]) >= 1

    def test_handle_list_allocations_with_exclude(self):
        handler, server = self._make_handler()
        server.request_alloc("inst1", 4096)
        req = MockRequest(exclude_instance_id="inst1")
        resp = handler._handle_list_allocations(req)
        assert resp["success"] is True
        assert len(resp["allocations"]) == 0

    def test_handle_register_kv(self):
        handler, server = self._make_handler()
        handle = server.request_alloc("inst1", 4096)
        req = MockRequest(
            key=100, region_id=handle.region_id, kv_offset=0, kv_length=256
        )
        resp = handler._handle_register_kv(req)
        assert resp["success"] is True
        assert resp["is_new"] is True

    def test_handle_lookup_kv_found(self):
        handler, server = self._make_handler()
        handle = server.request_alloc("inst1", 4096)
        server.register_kv(
            key=100, region_id=handle.region_id, kv_offset=0, kv_length=256
        )
        req = MockRequest(key=100)
        resp = handler._handle_lookup_kv(req)
        assert resp["found"] is True
        assert "handle" in resp

    def test_handle_lookup_kv_not_found(self):
        handler, _ = self._make_handler()
        req = MockRequest(key=99999)
        resp = handler._handle_lookup_kv(req)
        assert resp["found"] is False

    def test_handle_exists_kv(self):
        handler, server = self._make_handler()
        handle = server.request_alloc("inst1", 4096)
        server.register_kv(
            key=100, region_id=handle.region_id, kv_offset=0, kv_length=256
        )
        req = MockRequest(key=100)
        resp = handler._handle_exists_kv(req)
        assert resp["exists"] is True

    def test_handle_delete_kv(self):
        handler, server = self._make_handler()
        handle = server.request_alloc("inst1", 4096)
        server.register_kv(
            key=100, region_id=handle.region_id, kv_offset=0, kv_length=256
        )
        req = MockRequest(key=100)
        resp = handler._handle_delete_kv(req)
        assert resp["success"] is True

    def test_handle_batch_register_kv(self):
        handler, server = self._make_handler()
        handle = server.request_alloc("inst1", 4096)
        entries = [
            MockRequest(
                key=200, region_id=handle.region_id, kv_offset=0, kv_length=128
            ),
            MockRequest(
                key=201, region_id=handle.region_id, kv_offset=128, kv_length=128
            ),
        ]
        req = MockRequest(entries=entries)
        resp = handler._handle_batch_register_kv(req)
        assert resp["success"] is True
        assert len(resp["results"]) == 2

    def test_handle_batch_lookup_kv_mixed(self):
        handler, server = self._make_handler()
        handle = server.request_alloc("inst1", 4096)
        server.register_kv(
            key=300, region_id=handle.region_id, kv_offset=0, kv_length=128
        )
        req = MockRequest(keys=[300, 999])
        resp = handler._handle_batch_lookup_kv(req)
        assert len(resp["entries"]) == 2
        assert resp["entries"][0]["found"] is True
        assert resp["entries"][1]["found"] is False

    def test_handle_batch_exists_kv(self):
        handler, server = self._make_handler()
        handle = server.request_alloc("inst1", 4096)
        server.register_kv(
            key=400, region_id=handle.region_id, kv_offset=0, kv_length=128
        )
        req = MockRequest(keys=[400, 999])
        resp = handler._handle_batch_exists_kv(req)
        assert resp["results"] == [True, False]

    def test_handle_get_stats(self):
        handler, _ = self._make_handler()
        resp = handler._handle_get_stats({})
        assert "kv_manager" in resp
        assert "allocation_manager" in resp

    def test_handle_heartbeat(self):
        handler, _ = self._make_handler()
        resp = handler._handle_heartbeat({})
        assert resp == {}

    def test_all_message_types_dispatched(self):
        """Verify all MessageType values are in the handler dispatch table."""
        handler, _ = self._make_handler()
        handlers = handler._get_handlers()
        expected_types = [
            MessageType.REQUEST_ALLOC,
            MessageType.RETURN_ALLOC,
            MessageType.LIST_ALLOCATIONS,
            MessageType.REGISTER_KV,
            MessageType.LOOKUP_KV,
            MessageType.EXISTS_KV,
            MessageType.DELETE_KV,
            MessageType.BATCH_REGISTER_KV,
            MessageType.BATCH_LOOKUP_KV,
            MessageType.BATCH_EXISTS_KV,
            MessageType.GET_STATS,
            MessageType.HEARTBEAT,
        ]
        for msg_type in expected_types:
            assert msg_type.value in handlers
