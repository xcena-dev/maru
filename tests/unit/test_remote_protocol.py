# SPDX-License-Identifier: Apache-2.0
"""Framing of the remote-storage control channel."""

import msgpack
import pytest

from maru_remote import protocol


def test_round_trip_keeps_binary_fields():
    data = protocol.encode("hello", client_id="w", nixl_md=b"\x00\xff")
    msg = protocol.decode(data)
    assert msg == {"op": "hello", "client_id": "w", "nixl_md": b"\x00\xff"}


@pytest.mark.parametrize(
    "raw",
    [b"\xc1", msgpack.packb([1, 2]), msgpack.packb({"op": 3}), msgpack.packb({})],
)
def test_malformed_messages_are_rejected(raw):
    with pytest.raises(ValueError):
        protocol.decode(raw)


def test_protocol_version_is_an_int():
    assert isinstance(protocol.PROTOCOL_VERSION, int)
