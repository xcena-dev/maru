# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for NodeRegisterReq/Resp pack/unpack roundtrip."""

import struct

import pytest

from maru_shm.ipc import NodeRegisterReq, NodeRegisterResp

# ── NodeRegisterReq.pack ─────────────────────────────────────────────────────


class TestNodeRegisterReqPack:
    def test_empty_nodes(self):
        req = NodeRegisterReq()
        data = req.pack()
        assert data == struct.pack("<I", 0)

    def test_single_node_single_device(self):
        req = NodeRegisterReq(nodes=[("node-0", [("uuid-aaa", "/dev/dax0.0")])])
        data = req.pack()

        offset = 0
        (num_nodes,) = struct.unpack_from("<I", data, offset)
        offset += 4
        assert num_nodes == 1

        (nid_len,) = struct.unpack_from("<H", data, offset)
        offset += 2
        node_id = data[offset : offset + nid_len].decode()
        offset += nid_len
        assert node_id == "node-0"

        (num_devices,) = struct.unpack_from("<I", data, offset)
        offset += 4
        assert num_devices == 1

        (uuid_len,) = struct.unpack_from("<H", data, offset)
        offset += 2
        uuid_str = data[offset : offset + uuid_len].decode()
        offset += uuid_len
        assert uuid_str == "uuid-aaa"

        (path_len,) = struct.unpack_from("<H", data, offset)
        offset += 2
        path_str = data[offset : offset + path_len].decode()
        offset += path_len
        assert path_str == "/dev/dax0.0"

        assert offset == len(data)

    def test_multi_node_multi_device(self):
        req = NodeRegisterReq(
            nodes=[
                (
                    "node-0",
                    [
                        ("uuid-A", "/dev/dax0.0"),
                        ("uuid-B", "/dev/dax1.0"),
                    ],
                ),
                (
                    "node-1",
                    [("uuid-A", "/dev/dax1.0")],
                ),
            ]
        )
        data = req.pack()

        offset = 0
        (num_nodes,) = struct.unpack_from("<I", data, offset)
        offset += 4
        assert num_nodes == 2

        # node-0
        (nid_len,) = struct.unpack_from("<H", data, offset)
        offset += 2
        assert data[offset : offset + nid_len].decode() == "node-0"
        offset += nid_len
        (num_devices,) = struct.unpack_from("<I", data, offset)
        offset += 4
        assert num_devices == 2
        # skip device entries
        for _ in range(num_devices):
            (ul,) = struct.unpack_from("<H", data, offset)
            offset += 2 + ul
            (pl,) = struct.unpack_from("<H", data, offset)
            offset += 2 + pl

        # node-1
        (nid_len,) = struct.unpack_from("<H", data, offset)
        offset += 2
        assert data[offset : offset + nid_len].decode() == "node-1"
        offset += nid_len
        (num_devices,) = struct.unpack_from("<I", data, offset)
        offset += 4
        assert num_devices == 1

    def test_node_with_no_devices(self):
        req = NodeRegisterReq(nodes=[("node-0", [])])
        data = req.pack()

        offset = 0
        (num_nodes,) = struct.unpack_from("<I", data, offset)
        offset += 4
        assert num_nodes == 1

        (nid_len,) = struct.unpack_from("<H", data, offset)
        offset += 2 + nid_len
        (num_devices,) = struct.unpack_from("<I", data, offset)
        offset += 4
        assert num_devices == 0
        assert offset == len(data)

    def test_default_nodes_is_empty_list(self):
        req = NodeRegisterReq()
        assert req.nodes == []


# ── NodeRegisterResp.unpack ──────────────────────────────────────────────────


class TestNodeRegisterRespUnpack:
    def test_success(self):
        data = struct.pack("<iII", 0, 3, 4)
        resp = NodeRegisterResp.unpack(data)
        assert resp.status == 0
        assert resp.matched == 3
        assert resp.total == 4

    def test_error_status(self):
        data = struct.pack("<iII", -22, 0, 5)
        resp = NodeRegisterResp.unpack(data)
        assert resp.status == -22
        assert resp.matched == 0
        assert resp.total == 5

    def test_too_short_raises(self):
        with pytest.raises(ValueError, match="too short"):
            NodeRegisterResp.unpack(b"\x00" * 4)

    def test_extra_bytes_ignored(self):
        data = struct.pack("<iII", 0, 2, 2) + b"\xff" * 10
        resp = NodeRegisterResp.unpack(data)
        assert resp.status == 0
        assert resp.matched == 2
        assert resp.total == 2
