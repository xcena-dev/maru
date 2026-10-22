# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Read leases protect CXL locations until the consumer finishes copying."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from maru_common import MessageType
from maru_common.protocol import (
    BatchLookupKVResponse,
    BatchPinKVResponse,
    BatchPinLookupKVRequest,
    BatchPinLookupKVResponse,
    BatchUnpinSessionKVRequest,
    HandshakeRequest,
    LookupResult,
    ReleaseSessionRequest,
)
from maru_common.serializer import Serializer
from maru_common.storage_types import KV_PIN_SESSION_CAPABILITY
from maru_handler.memory.pinned import PinnedViews
from maru_server.kv_manager import DeleteResult, KVManager
from maru_server.rpc_handler_mixin import RpcHandlerMixin
from maru_server.server import MaruServer
from tests.unit.test_maru_handler import _make_mock_handler


@pytest.mark.parametrize(
    "mode,expected",
    [("prefix", [True, False, False]), ("independent", [True, False, True])],
)
def test_pin_lookup_mode_and_counts(mode, expected):
    kv = KVManager()
    kv.register("a", 1, 0, 4)
    kv.register("b", 1, 4, 4)
    kv.open_session("reader")
    result = kv.pin_lookup("reader", ["a", "missing", "b"], mode, [2, 1, 3])
    assert [entry is not None for entry in result] == expected
    assert kv.lookup("a").pin_count == 2
    assert kv.lookup("b").pin_count == (3 if mode == "independent" else 0)
    kv.release_session("reader")
    assert kv.lookup("a").pin_count == kv.lookup("b").pin_count == 0


def test_sessions_and_legacy_unpin_cannot_release_another_reader():
    kv = KVManager()
    kv.register("a", 1, 0, 4)
    for session in ["reader1", "reader2"]:
        kv.open_session(session)
        kv.pin_lookup(session, ["a"])
    assert kv.unpin("a") is False
    assert kv.batch_unpin(["a"]) == [False]
    assert kv.unpin_session("stranger", ["a"]) == [False]
    assert kv.unpin_session("reader1", ["a", "a"]) == [True, False]
    assert kv.delete_at("a", 1, 0)[0] == "PINNED"
    assert kv.pin("a") is True
    kv.release_session("reader2")
    assert kv.delete("a")[0] == DeleteResult.PINNED
    assert kv.unpin("a") is True
    assert kv.delete_at("a", 1, 0)[0] == "DELETED"


def test_session_close_fences_a_delayed_pin_request():
    kv = KVManager()
    kv.register("a", 1, 0, 4)
    kv.open_session("reader")
    kv.pin_lookup("reader", ["a", "a"])
    kv.release_session("reader")
    kv.release_session("reader")
    with pytest.raises(ValueError, match="not connected"):
        kv.pin_lookup("reader", ["a"])
    assert kv.lookup("a").pin_count == 0


@pytest.mark.parametrize(
    "mode,counts",
    [("invalid", []), ("prefix", [0]), ("prefix", [True]), ("prefix", [1, 2])],
)
def test_invalid_batch_does_not_partially_pin(mode, counts):
    kv = KVManager()
    kv.register("a", 1, 0, 4)
    kv.open_session("reader")
    with pytest.raises(ValueError):
        kv.pin_lookup("reader", ["a"], mode, counts)
    assert kv.lookup("a").pin_count == 0


def test_new_messages_survive_wire_dispatch_and_protect_delete():
    server = MaruServer()
    rpc = RpcHandlerMixin()
    rpc._server = server
    serializer = Serializer()
    handle = server.request_alloc("writer", 4096)
    server.register_kv("a", handle.region_id, 0, 4)

    def send(kind, request):
        header, payload = serializer.decode_request(serializer.encode(kind, request))
        return rpc._handle_message(header.msg_type, payload)

    handshake = send(MessageType.HANDSHAKE, HandshakeRequest(session_id="reader"))
    assert KV_PIN_SESSION_CAPABILITY in handshake["capabilities"]
    response = send(
        MessageType.BATCH_PIN_LOOKUP_KV,
        BatchPinLookupKVRequest("reader", ["a", "miss"]),
    )
    assert response["success"]
    assert [entry["found"] for entry in response["entries"]] == [True, False]
    assert server.delete_kv("a") is False
    assert send(
        MessageType.BATCH_UNPIN_SESSION_KV, BatchUnpinSessionKVRequest("reader", ["a"])
    )["held"] == [True]
    assert send(MessageType.RELEASE_SESSION, ReleaseSessionRequest("reader"))["success"]
    assert not send(
        MessageType.BATCH_PIN_LOOKUP_KV, BatchPinLookupKVRequest("reader", ["a"])
    )["success"]
    assert server.delete_kv("a") is True


def _reader(session=True):
    handler = _make_mock_handler()
    handler._capabilities = {KV_PIN_SESSION_CAPABILITY} if session else set()
    handler._session_id = "reader"
    return handler


def test_handler_holds_mapped_view_until_explicit_idempotent_release():
    handler = _reader()
    handler._rpc.batch_pin_lookup_kv.return_value = BatchPinLookupKVResponse(
        True, [LookupResult(True, handler._rpc.request_alloc.return_value.handle, 0, 4)]
    )
    views = handler.batch_retrieve_pinned(["a"])
    assert len(views) == 1 and views[0] is not None
    handler._rpc.batch_unpin_session.assert_not_called()
    views[:].release()
    views.release()
    handler._rpc.batch_unpin_session.assert_called_once_with("reader", ["a"])
    handler.close()
    handler._rpc.release_session.assert_called_once_with("reader")


def test_mapping_failure_and_truncated_reply_release_known_pins():
    handler = _reader()
    handler._rpc.batch_pin_lookup_kv.return_value = BatchPinLookupKVResponse(
        True, [LookupResult(True, handler._rpc.request_alloc.return_value.handle, 0, 4)]
    )
    with pytest.raises(ConnectionError, match="length"):
        handler.batch_retrieve_pinned(["a", "b"])
    handler._rpc.batch_unpin_session.assert_called_once_with("reader", ["a"])
    handler.close()


def test_unknown_pin_outcome_is_retained_until_session_close():
    handler = _reader()
    handler._rpc.batch_pin_lookup_kv.return_value = BatchPinLookupKVResponse(
        False, [], "timeout"
    )
    with pytest.raises(ConnectionError, match="timeout"):
        handler.batch_retrieve_pinned(["a"])
    handler._rpc.batch_unpin_session.assert_not_called()
    handler.close()
    handler._rpc.release_session.assert_called_once_with("reader")


def test_legacy_pin_before_lookup_never_exposes_unpinned_result():
    handler = _reader(session=False)
    entry = LookupResult(True, handler._rpc.request_alloc.return_value.handle, 0, 4)
    handler._rpc.batch_pin_kv.return_value = BatchPinKVResponse([True, False])
    handler._rpc.batch_lookup_kv.return_value = BatchLookupKVResponse(
        [entry, LookupResult(True, entry.handle, 4, 4)]
    )
    views = handler.batch_retrieve_pinned(["a", "miss"])
    assert views[0] is not None and views[1] is None
    views.release()
    handler._rpc.batch_unpin.assert_called_once_with(["a"])
    assert [call[0] for call in handler._rpc.mock_calls][-3:] == [
        "batch_pin_kv",
        "batch_lookup_kv",
        "batch_unpin",
    ]
    handler.close()


def test_lease_is_not_released_by_python_collection():
    release = MagicMock()
    lease = PinnedViews([SimpleNamespace()], release)
    del lease
    release.assert_not_called()


def test_late_release_keeps_the_original_session_identity():
    handler = _reader()
    handle = handler._rpc.request_alloc.return_value.handle
    handler._rpc.batch_pin_lookup_kv.return_value = BatchPinLookupKVResponse(
        True, [LookupResult(True, handle, 0, 4)]
    )
    views = handler.batch_retrieve_pinned(["a"])
    handler._session_id = "new-connection"
    views.release()
    handler._rpc.batch_unpin_session.assert_called_once_with("reader", ["a"])
    handler.close()


@pytest.mark.parametrize("async_client", [False, True])
def test_client_pin_api_parses_locations_and_releases_own_session(async_client):
    from conftest import _make_handle

    from maru_handler.rpc_async_client import RpcAsyncClient
    from maru_handler.rpc_client import RpcClient

    client = (RpcAsyncClient if async_client else RpcClient)("tcp://127.0.0.1:5555")
    client._send_request = MagicMock(
        return_value={
            "success": True,
            "entries": [
                {
                    "found": True,
                    "handle": _make_handle(1).to_dict(),
                    "kv_offset": 0,
                    "kv_length": 4,
                }
            ],
        }
    )
    response = client.batch_pin_lookup_kv("reader", ["a"], "independent", [2])
    assert response.success and response.entries[0].handle.region_id == 1
    client._send_request.assert_called_once_with(
        MessageType.BATCH_PIN_LOOKUP_KV,
        {"session_id": "reader", "keys": ["a"], "mode": "independent", "times": [2]},
    )
    client._send_request.return_value = {"success": True, "held": [True, False]}
    assert client.batch_unpin_session("reader", ["a", "miss"]).held == [True, False]
    client._send_request.return_value = {"success": True}
    assert client.release_session("reader")
    client._send_request.return_value = {"success": False, "error": "timeout"}
    response = client.batch_pin_lookup_kv("reader", ["a"])
    assert not response.success and response.entries == []
