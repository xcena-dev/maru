# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""RemoteServer wire: codes, round trips, schema skew and op digests; the
capabilities NODE_OPEN answers."""

import dataclasses
import typing

import pytest

from maru_common.protocol import (
    MessageType,
    NodeOpenRequest,
    NodeOpenResponse,
)
from maru_common.remote_protocol import (
    LEDGER_CODES,
    REMOTE_MESSAGE_CLASSES,
    REMOTE_WIRE_VERSION,
    CancelRequest,
    HelloRefusal,
    HelloRequest,
    HelloResponse,
    RemoteHintRequest,
    RemoteMessageType,
    RemotePool,
    RemoteWriteDoneRequest,
    ReserveRequest,
    ReserveResponse,
    RoomLimit,
    TransferEnd,
    Written,
    peer_origin,
)
from maru_common.serializer import (
    RESPONSE_BIT,
    MessageDecodeError,
    Serializer,
    op_digest,
)
from maru_common.storage_types import (
    FETCH_CAPABILITY,
    MEMORY_CAPABILITY,
    REMOTE_CAPABILITY,
    HintAction,
    Key,
    Location,
    OpRef,
    PoolInfo,
    PoolKind,
    Status,
    make_generation,
)
from tests.unit.ms_samples import sample

CODES = sorted(REMOTE_MESSAGE_CLASSES)
LEDGER = sorted(LEDGER_CODES)


def _name(code: int) -> str:
    return RemoteMessageType(code).name


def _ser() -> Serializer:
    return Serializer(REMOTE_MESSAGE_CLASSES)


def _location() -> Location:
    return Location(
        "dram:mem1:e1", "a7", make_generation(2, 9), 4096, 8192, PoolKind.DRAM
    )


def _write_done(end: TransferEnd = TransferEnd.DONE) -> RemoteWriteDoneRequest:
    return RemoteWriteDoneRequest(
        op=OpRef(peer_origin("gpu1", "e9"), 5, b"", 4),
        items=[Written(Key("ns", "k"), _location(), 8000, end)],
    )


def test_codes_are_unique_and_below_the_response_bit():
    assert len(set(CODES)) == len(CODES) == len(RemoteMessageType) == 7
    assert all(code & RESPONSE_BIT == 0 for code in CODES)
    # reserved (DROP, STAGE, UNSTAGE): never reused
    assert not {0x13, 0x18, 0x19} & set(CODES)


def test_every_code_decodes_strictly():
    assert _ser()._strict_codes == {int(c) for c in CODES}


def test_ledger_ops_are_the_requests_with_an_op():
    with_op = set()
    for code, (request_cls, _) in REMOTE_MESSAGE_CLASSES.items():
        hints = typing.get_type_hints(request_cls)
        if "op" in hints:
            assert hints["op"] is OpRef
            with_op.add(code)
    assert with_op == LEDGER_CODES
    assert {_name(c) for c in LEDGER_CODES} == {"RESERVE", "WRITE_DONE", "CANCEL"}


def test_origin_names_the_peer_run():
    assert peer_origin("gpu1", "e9") == "peer:gpu1@e9"
    assert peer_origin("gpu1", "e9") != peer_origin("gpu1", "e10")


def test_capability_names():
    names = (REMOTE_CAPABILITY, MEMORY_CAPABILITY, FETCH_CAPABILITY)
    assert names == ("remote_v1", "memory_v1", "fetch_v1")


@pytest.mark.parametrize("minimal", [False, True], ids=["all-set", "optionals-empty"])
@pytest.mark.parametrize("code", CODES, ids=_name)
def test_round_trip(code, minimal):
    request_cls, response_cls = REMOTE_MESSAGE_CLASSES[code]
    ser = _ser()
    request = sample(request_cls, minimal=minimal)
    header, decoded = ser.decode_request(ser.encode(code, request))
    assert header.msg_type == code
    assert decoded == request
    response = sample(response_cls, minimal=minimal)
    back_header, back = ser.decode_response(ser.encode_response(header, response))
    assert back_header.msg_type == code | RESPONSE_BIT
    assert back_header.sequence == header.sequence
    assert back == response


@pytest.mark.parametrize("code", CODES, ids=_name)
def test_a_response_with_its_status_alone_gets_the_defaults(code):
    request_cls, response_cls = REMOTE_MESSAGE_CLASSES[code]
    ser = _ser()
    header, _ = ser.decode(ser.encode(code, sample(request_cls)))
    _, back = ser.decode_response(ser.encode_response(header, {"status": "BUSY"}))
    assert back == response_cls(status=Status.BUSY)


def test_left_out_request_defaults():
    ser = _ser()
    hello = dataclasses.asdict(sample(HelloRequest))
    del hello["capabilities"], hello["ack"]
    _, decoded = ser.decode_request(ser.encode(RemoteMessageType.HELLO, hello))
    assert decoded.capabilities == [] and decoded.ack == 0
    cancel = {"op": dataclasses.asdict(OpRef("o", 2, b"d", 1)), "reserve_seq": 1}
    _, decoded = ser.decode_request(ser.encode(RemoteMessageType.CANCEL, cancel))
    assert decoded == CancelRequest(op=OpRef("o", 2, b"d", 1), reserve_seq=1)


def test_enums_and_bytes_travel_as_plain_values():
    ser = _ser()
    _, payload = ser.decode(ser.encode(RemoteMessageType.WRITE_DONE, _write_done()))
    end = payload["items"][0]["end"]
    assert type(end) is str and end == "done"
    response = sample(HelloResponse)
    header, _ = ser.decode(ser.encode(RemoteMessageType.HELLO, sample(HelloRequest)))
    _, payload = ser.decode(ser.encode_response(header, response))
    assert payload["refusal"] == response.refusal.value
    assert type(payload["pools"][0]["metadata"]) is bytes


@pytest.mark.parametrize("code", CODES, ids=_name)
def test_unknown_fields_are_ignored(code):
    request_cls, response_cls = REMOTE_MESSAGE_CLASSES[code]
    ser = _ser()
    request = sample(request_cls)
    payload = dataclasses.asdict(request)
    payload["added_later"] = {"x": 1}
    if "op" in payload:
        payload["op"]["also_new"] = 2
    header, decoded = ser.decode_request(ser.encode(code, payload))
    assert decoded == request
    response = sample(response_cls)
    payload = dataclasses.asdict(response)
    payload["added_later"] = [1, 2]
    _, back = ser.decode_response(ser.encode_response(header, payload))
    assert back == response


def test_unknown_fields_of_items_are_ignored():
    ser = _ser()
    request = _write_done()
    payload = dataclasses.asdict(request)
    payload["items"][0]["also_new"] = 1
    payload["items"][0]["location"]["also_new"] = 2
    _, decoded = ser.decode_request(ser.encode(RemoteMessageType.WRITE_DONE, payload))
    assert decoded == request
    header, _ = ser.decode(ser.encode(RemoteMessageType.HELLO, sample(HelloRequest)))
    response = sample(HelloResponse)
    payload = dataclasses.asdict(response)
    payload["pools"][0]["also_new"] = 3
    _, back = ser.decode_response(ser.encode_response(header, payload))
    assert back == response


def _corrupt(code, path, value):
    payload = dataclasses.asdict(sample(REMOTE_MESSAGE_CLASSES[code][0]))
    node = payload
    for key in path[:-1]:
        node = node[key]
    if value is _MISSING:
        del node[path[-1]]
    else:
        node[path[-1]] = value
    return payload


_MISSING = object()
_BAD: list[tuple[RemoteMessageType, list, object]] = [
    (RemoteMessageType.HELLO, ["version"], "1"),
    (RemoteMessageType.HELLO, ["agent_metadata"], "not-bytes"),
    (RemoteMessageType.HELLO, ["version"], _MISSING),
    (RemoteMessageType.RESERVE, ["pages"], True),
    (RemoteMessageType.RESERVE, ["pages"], "3"),
    (RemoteMessageType.RESERVE, ["op"], _MISSING),
    (RemoteMessageType.WRITE_DONE, ["items"], "abc"),
    (RemoteMessageType.WRITE_DONE, ["items", 0, "end"], "maybe"),
    (RemoteMessageType.WRITE_DONE, ["items", 0, "location", "kind"], "tape"),
    (RemoteMessageType.CANCEL, ["op", "digest"], "x"),
    (RemoteMessageType.CANCEL, ["locations"], [1]),
    (RemoteMessageType.RESERVE, ["hint"], "write_soon"),
    (RemoteMessageType.RESERVE, ["wait_ms"], 1.5),
    (RemoteMessageType.HINT, ["action"], "read_soon"),
    (RemoteMessageType.HINT, ["ranges", 0, "generation"], _MISSING),
    (RemoteMessageType.HINT, ["request"], None),
]


@pytest.mark.parametrize(
    "code,path,value",
    _BAD,
    ids=[f"{_name(c)}-{'.'.join(map(str, p))}" for c, p, _ in _BAD],
)
def test_bad_request_is_one_error_with_the_header(code, path, value):
    ser = _ser()
    frame = ser.encode(code, _corrupt(code, path, value), seq=77)
    with pytest.raises(MessageDecodeError) as info:
        ser.decode_request_payload(frame)
    assert info.value.header.msg_type == code
    assert info.value.header.sequence == 77


def test_bad_response_is_a_decode_error():
    ser = _ser()
    header, _ = ser.decode(ser.encode(RemoteMessageType.HELLO, sample(HelloRequest)))
    for payload in (
        {"status": "OK", "refusal": "nope"},
        {"status": "OK", "pools": [{"pool_id": "p"}]},
        {"status": "MAYBE"},
        5,
    ):
        with pytest.raises(MessageDecodeError):
            ser.decode_response(ser.encode_response(header, payload))


def test_unknown_code_keeps_the_header():
    ser = _ser()
    frame = ser.encode(0x7E, {"x": 1}, seq=5)
    with pytest.raises(MessageDecodeError) as info:
        ser.decode_request_payload(frame)
    assert info.value.header.sequence == 5


def test_refusals_and_limits():
    assert [r.value for r in HelloRefusal] == [
        "version",
        "server_epoch",
        "page_size",
        "retired",
        "not_ready",
    ]
    assert [r.value for r in RoomLimit] == ["quarantine"]
    assert [e.value for e in TransferEnd] == ["done", "not_posted", "unknown"]


class TestOpDigest:
    @pytest.mark.parametrize("code", LEDGER, ids=_name)
    def test_sender_and_receiver_agree(self, code):
        ser = _ser()
        request = sample(REMOTE_MESSAGE_CLASSES[code][0])
        decoded = ser.decode_request_payload(ser.encode(code, request))
        assert op_digest(decoded.payload) == op_digest(request)
        assert op_digest(decoded.request) == op_digest(request)
        assert len(op_digest(request)) == 16

    def test_it_is_stable(self):
        assert op_digest(_write_done()).hex() == "d98360ced34371b86a504d98b20b7749"

    @pytest.mark.parametrize("code", LEDGER, ids=_name)
    def test_the_op_is_not_part_of_it(self, code):
        request = sample(REMOTE_MESSAGE_CLASSES[code][0])
        resent = dataclasses.replace(
            request, op=OpRef("peer:other@e", request.op.seq, b"x", request.op.ack + 3)
        )
        assert op_digest(resent) == op_digest(request)

    def test_key_order_does_not_matter(self):
        payload = dataclasses.asdict(_write_done())
        assert op_digest(dict(reversed(list(payload.items())))) == op_digest(payload)

    def test_what_the_op_does_is_part_of_it(self):
        assert op_digest(_write_done()) != op_digest(_write_done(TransferEnd.UNKNOWN))
        reserve = sample(ReserveRequest)
        for changed in (
            dataclasses.replace(reserve, pages=reserve.pages + 1),
            dataclasses.replace(reserve, hint=None),
            dataclasses.replace(reserve, wait_ms=reserve.wait_ms + 1),
        ):
            assert op_digest(changed) != op_digest(reserve)
        cancel = sample(CancelRequest)
        assert op_digest(cancel) != op_digest(
            dataclasses.replace(cancel, reserve_seq=None)
        )

    def test_receiver_uses_the_raw_payload(self):
        ser = _ser()
        sent = dataclasses.asdict(_write_done())
        sent["items"][0]["added_later"] = 1  # a field this receiver does not know
        decoded = ser.decode_request_payload(
            ser.encode(RemoteMessageType.WRITE_DONE, sent)
        )
        assert op_digest(decoded.payload) == op_digest(sent)
        assert op_digest(decoded.request) != op_digest(sent)


class TestNodeOpenCapabilities:
    @staticmethod
    def _header(ser: Serializer):
        frame = ser.encode(MessageType.NODE_OPEN, sample(NodeOpenRequest))
        return ser.decode(frame)[0]

    def test_accepted_ones_come_back(self):
        ser = Serializer()
        header = self._header(ser)
        response = NodeOpenResponse(
            status=Status.OK, capabilities=[REMOTE_CAPABILITY, FETCH_CAPABILITY]
        )
        _, back = ser.decode_response(ser.encode_response(header, response))
        assert back == response

    def test_an_older_server_answers_none(self):
        ser = Serializer()
        header = self._header(ser)
        payload = dataclasses.asdict(sample(NodeOpenResponse))
        del payload["capabilities"]
        _, back = ser.decode_response(ser.encode_response(header, payload))
        assert back.capabilities == []
        assert back.adopted == sample(NodeOpenResponse).adopted

    def test_an_older_node_ignores_them(self):
        @dataclasses.dataclass
        class OldNodeOpenResponse:
            status: Status
            error: str | None = None
            server_epoch: str = ""
            node_session: str = ""
            epoch_seq: int = 0
            heartbeat_s: float = 0.0
            session_ttl_s: float = 0.0
            adopted: list[PoolInfo] = dataclasses.field(default_factory=list)

        new = Serializer()
        old = Serializer(
            {MessageType.NODE_OPEN: (NodeOpenRequest, OldNodeOpenResponse)}
        )
        header = self._header(new)
        response = sample(NodeOpenResponse)
        _, back = old.decode_response(new.encode_response(header, response))
        fields = dataclasses.asdict(response)
        del fields["capabilities"]
        assert dataclasses.asdict(back) == fields


def test_hello_carries_the_run_and_its_registrations():
    hello = HelloRequest(
        version=REMOTE_WIRE_VERSION,
        node_id="gpu1",
        node_epoch="e9",
        host_boot_id="boot",
        pid=4242,
        server_epoch="s1",
        agent_name="maru-gpu-gpu1-e9",
        agent_metadata=b"\x01md",
        page_size=2 << 20,
        conn_gen=2,
        capabilities=[REMOTE_CAPABILITY],
        ack=11,
    )
    ser = _ser()
    header, decoded = ser.decode_request(ser.encode(RemoteMessageType.HELLO, hello))
    assert decoded == hello
    response = HelloResponse(
        status=Status.OK,
        node_id="mem1",
        node_epoch="e1",
        server_epoch="s1",
        capabilities=[MEMORY_CAPABILITY],
        agent_name="maru-mem-mem1-e1",
        pools=[RemotePool("dram:mem1:e1", PoolKind.DRAM, 1, 1 << 30, 0x7F00, b"pm")],
        room=64 << 20,
        boot="0b5e:gen-7~2",
        hint_actions=["read_hint", "read_hint_end", "write_wait"],
    )
    _, back = ser.decode_response(ser.encode_response(header, response))
    assert back == response
    # an older memory node says neither: no generation, no hints
    older = dataclasses.asdict(response)
    del older["boot"], older["hint_actions"]
    _, back = ser.decode_response(ser.encode_response(header, older))
    assert (back.boot, back.hint_actions) == ("", [])
    # a hint action this side does not know is kept as a name, not refused
    newer = dataclasses.asdict(response)
    newer["hint_actions"] = ["read_hint", "read_soon"]
    _, back = ser.decode_response(ser.encode_response(header, newer))
    assert back.hint_actions == ["read_hint", "read_soon"]
    refused = HelloResponse(
        status=Status.BUSY, refusal=HelloRefusal.NOT_READY, limit=RoomLimit.QUARANTINE
    )
    _, back = ser.decode_response(ser.encode_response(header, refused))
    assert back == refused


def test_write_hints_ride_on_reserve_and_read_hints_have_their_own_message():
    ser = _ser()
    reserve = ReserveRequest(
        op=OpRef(peer_origin("gpu1", "e9"), 3, b"", 2),
        pages=8,
        hint=HintAction.WRITE_WAIT,
        wait_ms=50,
    )
    header, decoded = ser.decode_request(ser.encode(RemoteMessageType.RESERVE, reserve))
    assert decoded == reserve
    # an older sender: no hint, no wait
    payload = dataclasses.asdict(reserve)
    del payload["hint"], payload["wait_ms"]
    _, decoded = ser.decode_request(ser.encode(RemoteMessageType.RESERVE, payload))
    assert (decoded.hint, decoded.wait_ms) == (None, 0)
    capped = ReserveResponse(status=Status.OK, room=0, limit=RoomLimit.QUARANTINE)
    _, back = ser.decode_response(ser.encode_response(header, capped))
    assert back == capped
    hint = RemoteHintRequest(HintAction.READ_HINT, "req-1", [_location()])
    assert RemoteMessageType.HINT == 0x1A
    assert RemoteMessageType.HINT not in LEDGER_CODES
    frame = ser.encode(RemoteMessageType.HINT, hint)
    assert ser.decode_request(frame)[1] == hint
    end = RemoteHintRequest(HintAction.READ_HINT_END, "req-1")
    assert ser.decode_request(ser.encode(RemoteMessageType.HINT, end))[1] == end
