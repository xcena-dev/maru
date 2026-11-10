# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Node ownership messages and MaruShmClient's node_* methods (no RM needed)."""

import errno
import struct
import subprocess
import sys

import pytest

from maru_shm import client as client_module
from maru_shm.client import MaruShmClient as RealShmClient  # before conftest's patch
from maru_shm.client import (
    NodeHostConflictError,
    NodeOwnershipUnsupportedError,
    RetryableNodeError,
    StaleNodeEpochError,
)
from maru_shm.ipc import (
    AllocResp,
    ErrorResp,
    FreeResp,
    GetAccessResp,
    MsgHeader,
    MsgType,
    NodeAllocReq,
    NodeClaimResp,
    NodeFreeReq,
    NodeHoldReq,
    NodeReleaseResp,
    NodeRenewResp,
    NodeReq,
)
from maru_shm.types import MaruHandle, NodeRegion, RegionHolder

HANDLE = MaruHandle(region_id=7, offset=2 << 20, length=4 << 20, auth_token=42)


class TestNodeMessages:
    """Byte layout the resource manager parses (include/ipc.h)."""

    def test_message_type_values(self):
        assert [
            MsgType.NODE_CLAIM_REQ,
            MsgType.NODE_CLAIM_RESP,
            MsgType.NODE_ALLOC_REQ,
            MsgType.NODE_ALLOC_RESP,
            MsgType.NODE_FREE_REQ,
            MsgType.NODE_FREE_RESP,
            MsgType.NODE_RENEW_REQ,
            MsgType.NODE_RENEW_RESP,
            MsgType.NODE_RELEASE_REQ,
            MsgType.NODE_RELEASE_RESP,
            MsgType.NODE_HOLD_REQ,
            MsgType.NODE_HOLD_RESP,
        ] == list(range(17, 29))

    def test_node_req_layout(self):
        data = NodeReq(
            node_id="gb1", node_epoch="e1", client_id="host:1", request_id=9
        ).pack()
        assert struct.unpack_from("<II", data) == (3, 2)
        assert data[8:13] == b"gb1e1"
        assert struct.unpack_from("<H", data, 13) == (6,)
        assert data[15:21] == b"host:1"
        assert struct.unpack_from("<Q", data, 21) == (9,)
        assert len(data) == 29

    def test_node_alloc_req_layout(self):
        data = NodeAllocReq(
            size=1 << 30,
            node_id="gb1",
            node_epoch="e1",
            dax_path="/dev/dax2.0",
            client_id="h:1",
            request_id=3,
        ).pack()
        assert struct.unpack_from("<QIIII", data) == (1 << 30, 3, 2, 11, 0)
        assert data[24:40] == b"gb1e1/dev/dax2.0"
        assert struct.unpack_from("<H", data, 40) == (3,)
        assert data[42:45] == b"h:1"
        assert struct.unpack_from("<Q", data, 45) == (3,)

    def test_node_free_req_layout(self):
        data = NodeFreeReq(
            handle=HANDLE, node_id="gb1", node_epoch="e1", client_id="h:1"
        ).pack()
        assert data[:32] == HANDLE.pack()
        assert struct.unpack_from("<II", data, 32) == (3, 2)
        assert data[40:45] == b"gb1e1"
        assert struct.unpack_from("<H", data, 45) == (3,)

    def test_node_hold_req_layout(self):
        data = NodeHoldReq(
            handle=HANDLE,
            node_id="gb1",
            node_epoch="e1",
            holder="w-1",
            boot_id="b" * 36,
            pid=4242,
            pid_start=99,
            pid_ns=4026531836,
            client_id="h:1",
            request_id=5,
        ).pack()
        assert data[:32] == HANDLE.pack()
        assert struct.unpack_from("<IIIIIIQQ", data, 32) == (
            3,
            2,
            3,
            36,
            4242,
            0,
            99,
            4026531836,
        )
        assert data[72:80] == b"gb1e1w-1"
        assert data[80:116] == b"b" * 36
        assert struct.unpack_from("<H", data, 116) == (3,)
        assert data[118:121] == b"h:1"
        assert struct.unpack_from("<Q", data, 121) == (5,)
        assert len(data) == 129

    def test_claim_resp_roundtrip(self):
        worker = RegionHolder("w-1", "e1", "gb1", "b" * 36, 4242, 99, 7)
        agent = RegionHolder("agent", "", "", "", 0, 0, 0)
        regions = [
            NodeRegion(HANDLE, 3 << 20, "/dev/dax2.0", "u" * 36, [worker, agent]),
            NodeRegion(MaruHandle(region_id=8), 1, "", ""),  # pool not served
        ]
        resp = NodeClaimResp(
            status=0, rm_domain="ab" * 16, lease_ms=900000, regions=regions
        )
        data = resp.pack()
        assert struct.unpack_from("<iIQII", data) == (0, 32, 900000, 2, 0)
        # Each region: Handle, requested size, path, UUID and holder counts
        # (56 bytes), path, UUID, then each holder (40 bytes and its strings).
        off = 24 + 32
        assert struct.unpack_from("<QQQQQIIII", data, off) == (
            7,
            2 << 20,
            4 << 20,
            42,
            3 << 20,
            11,
            36,
            2,
            0,
        )
        off += 56 + 11 + 36
        assert struct.unpack_from("<IIIIIIQQ", data, off) == (
            3,
            2,
            3,
            36,
            4242,
            0,
            99,
            7,
        )
        assert data[off + 40 : off + 48] == b"w-1e1gb1"
        assert NodeClaimResp.unpack(data) == resp
        for cut in (10, 40, 24 + 32 + 50, off + 20, off + 45, len(data) - 1):
            with pytest.raises(ValueError):
                NodeClaimResp.unpack(data[:cut])

    def test_types_live_in_types_and_client_reexports_them(self):
        from maru_shm import client, types

        for name in (
            "NodeClaim",
            "NodeRegion",
            "StaleNodeEpochError",
            "NodeHostConflictError",
            "NodeOwnershipUnsupportedError",
            "RetryableNodeError",
        ):
            assert getattr(client, name) is getattr(types, name)
        holder = RegionHolder("w", "e", "h", "b", 1, 2, 3)
        with pytest.raises(AttributeError):
            holder.pid = 4  # frozen
        assert NodeRegion(HANDLE).holders == []

    def test_renew_and_release_resp(self):
        renew = NodeRenewResp.unpack(struct.pack("<iIQ", 0, 0, 1500))
        assert (renew.status, renew.lease_ms) == (0, 1500)
        release = NodeReleaseResp.unpack(struct.pack("<iI", 0, 3))
        assert (release.status, release.released) == (0, 3)
        with pytest.raises(ValueError):
            NodeRenewResp.unpack(b"\0" * 8)
        with pytest.raises(ValueError):
            NodeReleaseResp.unpack(b"\0" * 4)


def _reply(msg_type: MsgType, payload: bytes):
    return MsgHeader(msg_type=msg_type, payload_len=len(payload)), payload


class FakeRm:
    """Answer each request type with a canned payload; record what was sent."""

    def __init__(self, replies):
        self.replies = replies
        self.sent = []

    def __call__(self, msg_type, payload):
        self.sent.append((msg_type, payload))
        reply = self.replies[msg_type]
        return reply() if callable(reply) else reply


@pytest.fixture
def client():
    c = RealShmClient(address="127.0.0.1:1")
    yield c
    c.close()


class TestNodeClient:
    def test_claim_alloc_free_renew_release(self, client):
        region = NodeRegion(HANDLE, 3 << 20, "/dev/dax2.0", "")
        alloc = AllocResp(
            status=0, handle=HANDLE, requested_size=3 << 20, dax_path="/dev/dax2.0"
        )
        rm = FakeRm(
            {
                MsgType.NODE_CLAIM_REQ: _reply(
                    MsgType.NODE_CLAIM_RESP,
                    NodeClaimResp(0, "ab" * 16, 900000, [region]).pack(),
                ),
                MsgType.NODE_ALLOC_REQ: _reply(MsgType.NODE_ALLOC_RESP, alloc.pack()),
                MsgType.NODE_FREE_REQ: _reply(
                    MsgType.NODE_FREE_RESP, FreeResp(0).pack()
                ),
                MsgType.NODE_RENEW_REQ: _reply(
                    MsgType.NODE_RENEW_RESP, struct.pack("<iIQ", 0, 0, 900000)
                ),
                MsgType.NODE_RELEASE_REQ: _reply(
                    MsgType.NODE_RELEASE_RESP, struct.pack("<iI", 0, 2)
                ),
            }
        )
        client._rpc = rm
        claim = client.node_claim("gb1", "e1")
        assert claim.rm_domain == "ab" * 16
        assert claim.lease_s == 900.0
        assert claim.regions == [region]
        assert client.get_dax_path(HANDLE.region_id) == "/dev/dax2.0"
        assert client.node_alloc("gb1", "e1", 3 << 20, "/dev/dax2.0") == region
        client.node_free("gb1", "e1", HANDLE)
        assert client.get_dax_path(HANDLE.region_id) is None
        assert client.node_renew("gb1", "e1") == 900.0
        assert client.node_release("gb1", "e1") == 2

        types = [t for t, _ in rm.sent]
        assert types == [
            MsgType.NODE_CLAIM_REQ,
            MsgType.NODE_ALLOC_REQ,
            MsgType.NODE_FREE_REQ,
            MsgType.NODE_RENEW_REQ,
            MsgType.NODE_RELEASE_REQ,
        ]
        # Every request carries this client's ID and a fresh request ID.
        ids = set()
        for _, payload in rm.sent:
            assert client._client_id.encode() in payload
            ids.add(struct.unpack_from("<Q", payload, len(payload) - 8)[0])
        assert len(ids) == 5

    @pytest.mark.parametrize(
        "status, error, retryable_error",
        [
            (-errno.ESTALE, StaleNodeEpochError, StaleNodeEpochError),
            (-errno.EEXIST, NodeHostConflictError, NodeHostConflictError),
            (-errno.EINVAL, RuntimeError, RuntimeError),
            (-errno.EACCES, RuntimeError, RuntimeError),
            (-errno.EPERM, RuntimeError, RuntimeError),
            (-errno.ENOENT, RuntimeError, RuntimeError),
            (-errno.ENOSYS, RuntimeError, RuntimeError),
            (-errno.EIO, RuntimeError, RetryableNodeError),
            (-errno.ENOSPC, RuntimeError, RetryableNodeError),
            (-errno.EAGAIN, RuntimeError, RetryableNodeError),
        ],
    )
    def test_status_errors(self, client, status, error, retryable_error):
        # Claim, renew and release may be sent again; alloc and free not.
        rm = FakeRm(
            {
                MsgType.NODE_CLAIM_REQ: _reply(
                    MsgType.NODE_CLAIM_RESP, NodeClaimResp(status).pack()
                ),
                MsgType.NODE_ALLOC_REQ: _reply(
                    MsgType.NODE_ALLOC_RESP, AllocResp(status=status).pack()
                ),
                MsgType.NODE_FREE_REQ: _reply(
                    MsgType.NODE_FREE_RESP, FreeResp(status).pack()
                ),
                MsgType.NODE_RENEW_REQ: _reply(
                    MsgType.NODE_RENEW_RESP, struct.pack("<iIQ", status, 0, 0)
                ),
                MsgType.NODE_RELEASE_REQ: _reply(
                    MsgType.NODE_RELEASE_RESP, struct.pack("<iI", status, 0)
                ),
                MsgType.NODE_HOLD_REQ: _reply(
                    MsgType.NODE_HOLD_RESP, GetAccessResp(status=status).pack()
                ),
            }
        )
        client._rpc = rm
        hold = {"boot_id": "b", "pid": 1, "pid_start": 2, "pid_ns": 3}
        calls = [
            (
                lambda: client.node_hold("gb1", "e1", HANDLE, "w", **hold),
                retryable_error,
            ),
            (lambda: client.node_claim("gb1", "e1"), retryable_error),
            (lambda: client.node_alloc("gb1", "e1", 1), error),
            (lambda: client.node_free("gb1", "e1", HANDLE), error),
            (lambda: client.node_renew("gb1", "e1"), retryable_error),
            (lambda: client.node_release("gb1", "e1"), retryable_error),
        ]
        for call, expected in calls:
            with pytest.raises(expected) as info:
                call()
            assert type(info.value) is expected  # Not one of the others.
            assert isinstance(info.value, RuntimeError)

    def test_an_rm_without_node_ownership_is_named(self, client):
        err = ErrorResp(status=-errno.ENOSYS, message="unknown request").pack()
        client._rpc = lambda *a: _reply(MsgType.ERROR_RESP, err)
        for call in (
            lambda: client.node_claim("gb1", "e1"),
            lambda: client.node_alloc("gb1", "e1", 1),
            lambda: client.node_free("gb1", "e1", HANDLE),
            lambda: client.node_renew("gb1", "e1"),
            lambda: client.node_release("gb1", "e1"),
            lambda: client.node_hold(
                "gb1", "e1", HANDLE, "w", boot_id="b", pid=1, pid_start=2, pid_ns=3
            ),
        ):
            with pytest.raises(NodeOwnershipUnsupportedError, match="does not support"):
                call()

    def test_other_error_replies_stay_runtime_errors(self, client):
        err = ErrorResp(status=-errno.EPROTO, message="bad node req").pack()
        client._rpc = lambda *a: _reply(MsgType.ERROR_RESP, err)
        with pytest.raises(RuntimeError, match="bad node req") as info:
            client.node_claim("gb1", "e1")
        assert type(info.value) is RuntimeError


class TestNodeHold:
    def test_node_hold_sends_the_identity_and_returns_access(self, client):
        access = GetAccessResp(
            status=0, dax_path="/dev/dax2.0", device_uuid="u", offset=1, length=2
        )
        rm = FakeRm(
            {MsgType.NODE_HOLD_REQ: _reply(MsgType.NODE_HOLD_RESP, access.pack())}
        )
        client._rpc = rm
        got = client.node_hold(
            "gb1", "e1", HANDLE, "w-1", boot_id="b", pid=7, pid_start=8, pid_ns=9
        )
        assert got == access
        ((msg_type, payload),) = rm.sent
        assert msg_type == MsgType.NODE_HOLD_REQ
        assert payload[:32] == HANDLE.pack()
        assert struct.unpack_from("<IIIIIIQQ", payload, 32) == (3, 2, 3, 1, 7, 0, 8, 9)
        assert payload[72:81] == b"gb1e1w-1b"
        with pytest.raises(TypeError):
            client.node_hold("gb1", "e1", HANDLE, "w-1", "b", 7, 8, 9)  # keyword-only


class TestRequestIds:
    """A process restarted with the same hostname:pid must not reuse the
    request IDs of the one before it (the RM caches replies by them)."""

    def test_each_process_starts_at_a_random_id(self):
        code = "from maru_shm import client; print(client._next_request_id())"
        first = [
            int(
                subprocess.run(
                    [sys.executable, "-c", code],
                    capture_output=True,
                    text=True,
                    check=True,
                ).stdout.split()[-1]
            )
            for _ in range(2)
        ]
        assert first[0] != first[1]
        assert all(0 < i < 1 << 64 for i in first)

    def test_ids_are_nonzero_u64_and_wrap(self, monkeypatch):
        monkeypatch.setattr(client_module, "_request_id_counter", (1 << 64) - 2)
        ids = [client_module._next_request_id() for _ in range(3)]
        assert ids == [(1 << 64) - 1, 1, 2]
