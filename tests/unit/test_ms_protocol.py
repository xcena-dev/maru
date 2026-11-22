# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Multi-storage MaruServer messages: codes, round trips and op digests."""

import dataclasses
import itertools

import pytest

from maru_common.node_protocol import (
    NODE_MESSAGE_CLASSES,
    FetchRequest,
    NodeMessageType,
)
from maru_common.protocol import (
    MESSAGE_CLASSES,
    MS_MESSAGE_CLASSES,
    CommitItem,
    CommitRequest,
    FetchCompleteRequest,
    MessageType,
    NodeOpenRequest,
    NodeOpenResponse,
    ProbeRequest,
    SourcePinRequest,
)
from maru_common.serializer import (
    RESPONSE_BIT,
    MessageDecodeError,
    Serializer,
    op_digest,
)
from maru_common.storage_types import (
    EPOCH_SEQ_MAX,
    GENERATION_COUNTER_MAX,
    Key,
    Location,
    OpRef,
    PoolKind,
    generation_epoch_seq,
    make_generation,
)
from tests.unit.ms_samples import sample

MS_CODES = sorted(MS_MESSAGE_CLASSES)


def test_codes_are_in_the_multi_storage_range_and_unique():
    assert len(set(MS_CODES)) == len(MS_CODES) == 17
    assert all(0x40 <= code <= 0x5F for code in MS_CODES)
    # reserved (FINALIZE_MOVE) and retired (NODE_REREGISTER,
    # RELEASE_FINALIZER, REGION_REQUEST, REGION_RETURN): never reused
    assert not {0x43, 0x5A, 0x5C, 0x5E, 0x5F} & {int(code) for code in MessageType}
    # the response code keeps the request code below the response bit
    assert all(code & RESPONSE_BIT == 0 for code in MS_CODES)


def test_maruserver_table_serves_both_backends():
    for code, classes in MS_MESSAGE_CLASSES.items():
        assert MESSAGE_CLASSES[code] == classes
    assert MessageType.STORAGE in MESSAGE_CLASSES


@pytest.mark.parametrize("code", MS_CODES, ids=lambda c: MessageType(c).name)
def test_request_round_trip(code):
    request_cls, _ = MS_MESSAGE_CLASSES[code]
    request = sample(request_cls)
    ser = Serializer()
    header, decoded = ser.decode_request(ser.encode(code, request))
    assert header.msg_type == code
    assert decoded == request


@pytest.mark.parametrize("code", MS_CODES, ids=lambda c: MessageType(c).name)
def test_response_round_trip(code):
    _, response_cls = MS_MESSAGE_CLASSES[code]
    response = sample(response_cls)
    ser = Serializer()
    request_frame = ser.encode(code, sample(MS_MESSAGE_CLASSES[code][0]))
    request_header, _ = ser.decode(request_frame)
    frame = ser.encode_response(request_header, response)
    header, decoded = ser.decode_response(frame)
    assert header.msg_type == code | RESPONSE_BIT
    assert header.sequence == request_header.sequence
    assert decoded == response


def test_enums_travel_as_plain_strings():
    ser = Serializer()
    request = sample(CommitRequest)
    _, payload = ser.decode(ser.encode(MessageType.COMMIT, request))
    kind = payload["items"][0]["location"]["kind"]
    assert type(kind) is str and kind == request.items[0].location.kind.value


def test_unknown_fields_are_ignored():
    ser = Serializer()
    payload = dataclasses.asdict(sample(CommitRequest))
    payload["added_later"] = {"x": 1}
    payload["items"][0]["also_new"] = 2
    _, decoded = ser.decode_request(ser.encode(MessageType.COMMIT, payload))
    assert decoded == sample(CommitRequest)


def test_missing_required_field_is_an_error():
    ser = Serializer()
    payload = dataclasses.asdict(sample(CommitRequest))
    del payload["op"]
    with pytest.raises(MessageDecodeError):
        ser.decode_request(ser.encode(MessageType.COMMIT, payload))


def _commit(offset: int = 0) -> CommitRequest:
    loc = Location("dram:n:e", "a1", make_generation(2, 9), offset, 4096, PoolKind.DRAM)
    return CommitRequest(
        session="s",
        server_epoch="e",
        op=OpRef(origin="node-epoch", seq=5, digest=b"", ack=4),
        items=[CommitItem(key=Key("ns", "k"), location=loc, logical_bytes=4000)],
    )


class TestOpDigest:
    def test_sender_and_receiver_agree(self):
        request = _commit()
        ser = Serializer()
        _, payload = ser.decode(ser.encode(MessageType.COMMIT, request))
        assert op_digest(request) == op_digest(payload)
        assert len(op_digest(request)) == 16

    def test_op_field_is_not_part_of_it(self):
        a = _commit()
        b = dataclasses.replace(a, op=OpRef(origin="other", seq=99, digest=b"x", ack=0))
        assert op_digest(a) == op_digest(b)

    def test_payload_change_changes_it(self):
        assert op_digest(_commit(0)) != op_digest(_commit(4096))

    def test_key_order_does_not_matter(self):
        payload = dataclasses.asdict(_commit())
        reordered = dict(reversed(list(payload.items())))
        assert op_digest(payload) == op_digest(reordered)

    def test_needs_a_request(self):
        with pytest.raises(TypeError):
            op_digest([1, 2])


class TestGeneration:
    def test_epoch_seq_is_in_the_high_bits(self):
        g = make_generation(5, 123)
        assert generation_epoch_seq(g) == 5
        assert g & GENERATION_COUNTER_MAX == 123

    def test_a_later_run_is_always_above(self):
        assert make_generation(6, 0) > make_generation(5, GENERATION_COUNTER_MAX)

    @pytest.mark.parametrize(
        "epoch_seq,counter", [(-1, 0), (1, -1), (1, GENERATION_COUNTER_MAX + 1)]
    )
    def test_out_of_range(self, epoch_seq, counter):
        with pytest.raises(ValueError):
            make_generation(epoch_seq, counter)


def test_cpu_is_the_old_name_of_dram():
    assert PoolKind.parse("cpu") is PoolKind.DRAM
    assert PoolKind.parse("cxl") is PoolKind.CXL
    with pytest.raises(ValueError):
        PoolKind.parse("remote")


# ---------------------------------------------------------------------------
# Optional and first-member branches, strict decoding, schema skew
# ---------------------------------------------------------------------------

LEGACY_CODES = {
    MessageType.STORAGE,
    MessageType.REQUEST_ALLOC,
    MessageType.RETURN_ALLOC,
    MessageType.LIST_ALLOCATIONS,
    MessageType.REGISTER_KV,
    MessageType.LOOKUP_KV,
    MessageType.EXISTS_KV,
    MessageType.DELETE_KV,
    MessageType.PIN_KV,
    MessageType.UNPIN_KV,
    MessageType.BATCH_REGISTER_KV,
    MessageType.BATCH_LOOKUP_KV,
    MessageType.BATCH_EXISTS_KV,
    MessageType.BATCH_PIN_KV,
    MessageType.BATCH_UNPIN_KV,
    MessageType.GET_STATS,
    MessageType.GET_USAGE,
    MessageType.HEARTBEAT,
    MessageType.REPORT_STATS,
    MessageType.HANDSHAKE,
    MessageType.SHUTDOWN,
}


def test_legacy_table_is_unchanged():
    assert set(MESSAGE_CLASSES) - set(MS_MESSAGE_CLASSES) == LEGACY_CODES
    # legacy messages keep the lenient decode
    assert Serializer()._strict_codes == {int(c) for c in MS_MESSAGE_CLASSES}


@pytest.mark.parametrize("code", MS_CODES, ids=lambda c: MessageType(c).name)
def test_minimal_round_trip(code):
    ser = Serializer()
    request_cls, response_cls = MS_MESSAGE_CLASSES[code]
    request = sample(request_cls, minimal=True)
    header, decoded = ser.decode_request(ser.encode(code, request))
    assert decoded == request
    response = sample(response_cls, minimal=True)
    _, decoded = ser.decode_response(ser.encode_response(header, response))
    assert decoded == response


def test_cpu_on_the_wire_is_dram():
    ser = Serializer()
    payload = dataclasses.asdict(sample(CommitRequest))
    payload["items"][0]["location"]["kind"] = "cpu"
    _, decoded = ser.decode_request(ser.encode(MessageType.COMMIT, payload))
    assert decoded.items[0].location.kind is PoolKind.DRAM


def _corrupt(path, value):
    payload = dataclasses.asdict(sample(CommitRequest))
    node = payload
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    return payload


@pytest.mark.parametrize(
    "payload",
    [
        _corrupt(["items"], "abc"),
        _corrupt(["items"], [1, 2]),
        _corrupt(["op"], "x"),
        _corrupt(["items", 0, "logical_bytes"], "x"),
        _corrupt(["items", 0, "logical_bytes"], True),
        _corrupt(["items", 0, "location", "generation"], 1.5),
        _corrupt(["items", 0, "location", "kind"], "tape"),
        _corrupt(["op", "digest"], "not-bytes"),
        {
            k: v
            for k, v in dataclasses.asdict(sample(CommitRequest)).items()
            if k != "op"
        },
        5,
    ],
    ids=[
        "items-str",
        "items-ints",
        "op-str",
        "int-as-str",
        "int-as-bool",
        "float-as-int",
        "bad-kind",
        "digest-str",
        "missing-op",
        "not-a-map",
    ],
)
def test_bad_payload_is_one_error_with_the_header(payload):
    ser = Serializer()
    frame = ser.encode(MessageType.COMMIT, payload, seq=77)
    with pytest.raises(MessageDecodeError) as info:
        ser.decode_request_payload(frame)
    assert info.value.header.msg_type == MessageType.COMMIT
    assert info.value.header.sequence == 77


def test_int_is_a_valid_float():
    ser = Serializer()
    payload = dataclasses.asdict(sample(NodeOpenResponse))
    payload["heartbeat_s"] = 10
    request_header, _ = ser.decode(
        ser.encode(MessageType.NODE_OPEN, sample(NodeOpenRequest))
    )
    _, decoded = ser.decode_response(ser.encode_response(request_header, payload))
    assert decoded.heartbeat_s == 10.0 and isinstance(decoded.heartbeat_s, float)


def test_unreadable_message_has_no_header():
    with pytest.raises(MessageDecodeError) as info:
        Serializer().decode_request_payload(b"\x00" * 4)
    assert info.value.header is None


def test_unknown_code_keeps_the_header():
    ser = Serializer()
    frame = ser.encode(0x7E, {"x": 1}, seq=5)
    with pytest.raises(MessageDecodeError) as info:
        ser.decode_request_payload(frame)
    assert info.value.header.sequence == 5


class TestDigestAcrossVersions:
    def test_receiver_uses_the_raw_payload(self):
        ser = Serializer()
        sent = dataclasses.asdict(sample(FetchCompleteRequest))
        sent["added_later"] = [1, 2]  # a field this receiver does not know
        del sent["items"][0]["source_worker"]  # an older sender leaves a default out
        frame = ser.encode(MessageType.FETCH_COMPLETE, sent)
        decoded = ser.decode_request_payload(frame)
        assert op_digest(decoded.payload) == op_digest(sent)
        # the dataclass lost the unknown field and gained the default
        assert op_digest(decoded.request) != op_digest(sent)

    def test_envelope_fields_are_left_out(self):
        request = sample(SourcePinRequest)
        reopened = dataclasses.replace(
            request, session="new", server_epoch="new", worker_session="new"
        )
        assert op_digest(request) == op_digest(reopened)

    def test_a_resent_fetch_may_carry_less_of_its_deadline(self):
        ser = Serializer(NODE_MESSAGE_CLASSES)
        request = sample(FetchRequest)
        later = dataclasses.replace(request, deadline_ms=request.deadline_ms // 2 + 1)
        decoded = ser.decode_request_payload(ser.encode(NodeMessageType.FETCH, later))
        assert decoded.request == later
        assert op_digest(decoded.payload) == op_digest(request)
        # what it fetches is still part of the op
        assert op_digest(dataclasses.replace(request, max_bytes=1)) != op_digest(
            request
        )


def test_header_sequence_wraps_and_skips_zero():
    ser = Serializer()
    ser._seq_counter = itertools.count((1 << 32) - 1)
    assert ser._next_seq() == (1 << 32) - 1
    assert ser._next_seq() == 1


def test_epoch_seq_fits_64_bit_generations():
    assert make_generation(EPOCH_SEQ_MAX, GENERATION_COUNTER_MAX) == (1 << 64) - 1
    with pytest.raises(ValueError):
        make_generation(EPOCH_SEQ_MAX + 1, 0)


def test_fields_this_side_does_not_serve_stay_in_the_raw_payload():
    """A sender's finalizer or hold_copy is not a field here: the request
    decodes without it, and the raw payload keeps it for the server to
    refuse."""
    ser = Serializer()
    commit = dataclasses.asdict(sample(CommitRequest))
    commit["items"][0]["finalizer"] = True
    decoded = ser.decode_request_payload(ser.encode(MessageType.COMMIT, commit))
    assert not hasattr(decoded.request.items[0], "finalizer")
    assert decoded.payload["items"][0]["finalizer"] is True
    done = dataclasses.asdict(sample(FetchCompleteRequest))
    done["items"][0]["hold_copy"] = True
    frame = ser.encode(MessageType.FETCH_COMPLETE, done)
    assert ser.decode_request_payload(frame).payload["items"][0]["hold_copy"]


def test_fetch_complete_names_no_copy_by_default():
    from maru_common.protocol import FetchCompleteItem, FetchCompleteResponse
    from maru_common.storage_types import ItemOutcome, ItemResult, Status

    ser = Serializer()
    item = FetchCompleteItem(Key("ns", "k"), "src", 8)
    assert item.copy is None and item.source is None and item.source_worker == ""
    request = FetchCompleteRequest("s", "e", OpRef("o", 1, b"d" * 16, 0), [item])
    frame = ser.encode(MessageType.FETCH_COMPLETE, request)
    assert ser.decode_request(frame)[1] == request
    # a copy is carried (the server refuses it) and is part of the op
    copy = Location("dram:n:e", "a1", make_generation(3, 7), 0, 8, PoolKind.DRAM)
    named = dataclasses.replace(request, items=[dataclasses.replace(item, copy=copy)])
    assert ser.decode_request(ser.encode(MessageType.FETCH_COMPLETE, named))[1] == named
    assert op_digest(named) != op_digest(request)
    # a failed read: logical_bytes 0
    failed = dataclasses.replace(item, logical_bytes=0)
    assert op_digest(dataclasses.replace(request, items=[failed])) != op_digest(request)
    reply = FetchCompleteResponse(
        status=Status.OK,
        items=[ItemOutcome(ItemResult.OK), ItemOutcome(ItemResult.REJECTED)],
    )
    request_header, _ = ser.decode(frame)
    _, back = ser.decode_response(ser.encode_response(request_header, reply))
    assert back == reply


def test_probe_names_locations_only_when_asked():
    from maru_common.protocol import ProbeResponse
    from maru_common.storage_types import ItemOutcome, ItemResult, Status

    ser = Serializer()
    probe = ProbeRequest("s", "e", [Key("ns", "k")], with_locations=True)
    frame = ser.encode(MessageType.PROBE, probe)
    assert ser.decode_request(frame)[1] == probe
    payload = dataclasses.asdict(probe)
    del payload["with_locations"]  # an older sender: no locations
    _, decoded = ser.decode_request(ser.encode(MessageType.PROBE, payload))
    assert decoded.with_locations is False
    where = Location("dram:m:e", "a1", make_generation(3, 7), 0, 8, PoolKind.DRAM)
    reply = ProbeResponse(
        status=Status.OK,
        items=[
            ItemOutcome(ItemResult.FETCH, location=where),
            ItemOutcome(ItemResult.MISS),
        ],
    )
    request_header, _ = ser.decode(frame)
    _, back = ser.decode_response(ser.encode_response(request_header, reply))
    assert back == reply


def test_plan_source_pin_fields_round_trip_and_default_off():
    from maru_common.protocol import (
        FetchCompleteItem,
        LookupAndPinRequest,
        LookupAndPinResponse,
        UnpinRequest,
    )
    from maru_common.storage_types import FetchPlan, ItemOutcome, ItemResult, Status

    source = Location("dram:m:e", "a1", make_generation(3, 7), 0, 8, PoolKind.DRAM)
    ser = Serializer()
    lookup = LookupAndPinRequest(
        "s", "e", OpRef("o", 1, b"d" * 16, 0), [Key("ns", "k")], pin_sources=True
    )
    frame = ser.encode(MessageType.LOOKUP_AND_PIN, lookup)
    assert ser.decode_request(frame)[1] == lookup
    # part of the op; an older sender leaves it out: no source pins
    assert op_digest(dataclasses.replace(lookup, pin_sources=False)) != op_digest(
        lookup
    )
    payload = dataclasses.asdict(lookup)
    del payload["pin_sources"]
    _, decoded = ser.decode_request(ser.encode(MessageType.LOOKUP_AND_PIN, payload))
    assert decoded.pin_sources is False
    plan = FetchPlan(source, "n", 8, pin_id="src")
    reply = LookupAndPinResponse(
        status=Status.OK, items=[ItemOutcome(ItemResult.FETCH, plan=plan)]
    )
    request_header, _ = ser.decode(frame)
    raw = ser.encode_response(request_header, reply)
    assert ser.decode_response(raw)[1] == reply
    payload = dataclasses.asdict(reply)
    del payload["items"][0]["plan"]["pin_id"]
    _, back = ser.decode_response(ser.encode_response(request_header, payload))
    assert back.items[0].plan.pin_id is None
    # FETCH_COMPLETE names the location read; an older sender does not
    request = FetchCompleteRequest(
        session="s",
        server_epoch="e",
        op=OpRef("o", 2, b"d" * 16, 1),
        items=[
            FetchCompleteItem(
                Key("ns", "k"), "src", 8, source=source, source_worker="inc"
            )
        ],
    )
    frame = ser.encode(MessageType.FETCH_COMPLETE, request)
    assert ser.decode_request(frame)[1] == request
    payload = dataclasses.asdict(request)
    del payload["items"][0]["source"]
    del payload["items"][0]["source_worker"]
    _, decoded = ser.decode_request(ser.encode(MessageType.FETCH_COMPLETE, payload))
    assert decoded.items[0].source is None and decoded.items[0].source_worker == ""
    # a node's UNPIN names the worker whose plans brought the source pins
    unpin = UnpinRequest("s", "e", ["src"], worker_incarnation="inc")
    frame = ser.encode(MessageType.UNPIN, unpin)
    assert ser.decode_request(frame)[1] == unpin
    payload = dataclasses.asdict(unpin)
    del payload["worker_incarnation"]
    _, decoded = ser.decode_request(ser.encode(MessageType.UNPIN, payload))
    assert decoded.worker_incarnation == ""


class TestAccessHints:
    READS = [
        ("cxl:rm1:7", 7 << 21, make_generation(3, 9)),
        ("dram:mem-1:5f0c1e2a9b8d", 0, make_generation(EPOCH_SEQ_MAX, 0)),
        ("cxl:rm1:7", (1 << 64) - 1, (1 << 64) - 1),
    ]

    def test_reads_round_trip_through_the_codec_as_four_values(self):
        from maru_common.protocol import NodeHeartbeatRequest, NodeHeartbeatResponse
        from maru_common.storage_types import AccessHints, Status

        hints = AccessHints.pack(self.READS)
        assert len(hints) == 3
        # each pool once; the arrays are bytes, not a value per read
        assert hints.pool_ids == (self.READS[0][0], self.READS[1][0])
        assert (len(hints.pools), len(hints.offsets)) == (6, 24)
        ser = Serializer()
        request = sample(NodeHeartbeatRequest)
        request_header, _ = ser.decode(ser.encode(MessageType.NODE_HEARTBEAT, request))
        reply = NodeHeartbeatResponse(status=Status.OK, access=hints)
        frame = ser.encode_response(request_header, reply)
        _, payload = ser.decode(frame)
        assert {k: type(v) for k, v in payload["access"].items()} == {
            "pool_ids": list,
            "pools": bytes,
            "offsets": bytes,
            "generations": bytes,
        }
        _, back = ser.decode_response(frame)
        assert back == reply
        assert list(back.access.reads()) == self.READS

    def test_none_is_empty(self):
        from maru_common.storage_types import AccessHints

        assert AccessHints.pack([]) == AccessHints()
        assert not AccessHints() and list(AccessHints().reads()) == []

    @pytest.mark.parametrize(
        "change",
        [
            {"pools": b"\0\0"},
            {"offsets": b"\0" * 23},
            {"generations": b"\0" * 32},
            {"pool_ids": ("one",)},
        ],
        ids=["pools", "offsets", "generations", "unknown-pool"],
    )
    def test_arrays_that_do_not_agree_are_an_error(self, change):
        from maru_common.storage_types import AccessHints

        hints = dataclasses.replace(AccessHints.pack(self.READS), **change)
        with pytest.raises(ValueError):
            list(hints.reads())


@pytest.mark.parametrize(
    "code,field,value",
    [
        (MessageType.NODE_OPEN, "pid_ns", True),
        (MessageType.NODE_OPEN, "pid_ns", "101"),
        (MessageType.NODE_ORPHANS, "cursor", 1),
    ],
)
def test_orphan_requests_decode_strictly(code, field, value):
    ser = Serializer()
    payload = dataclasses.asdict(sample(MS_MESSAGE_CLASSES[code][0]))
    payload[field] = value
    with pytest.raises(MessageDecodeError):
        ser.decode_request_payload(ser.encode(code, payload))


def test_node_open_orphan_response_decodes_strictly_and_defaults_for_legacy():
    ser = Serializer()
    header, _ = ser.decode_request(
        ser.encode(MessageType.NODE_OPEN, sample(NodeOpenRequest))
    )
    payload = dataclasses.asdict(sample(NodeOpenResponse))
    payload["orphans"][0]["pid_ns"] = True
    with pytest.raises(MessageDecodeError):
        ser.decode_response(ser.encode_response(header, payload))
    payload = dataclasses.asdict(sample(NodeOpenResponse))
    payload["pool_registrations"]["p"] = True
    with pytest.raises(MessageDecodeError):
        ser.decode_response(ser.encode_response(header, payload))
    del payload["orphans"], payload["next_cursor"], payload["pool_registrations"]
    back = ser.decode_response(ser.encode_response(header, payload))[1]
    assert back.orphans == [] and back.next_cursor is None
    assert back.pool_registrations == {}
    payload = dataclasses.asdict(sample(NodeOpenRequest))
    del payload["pid_ns"]
    assert ser.decode_request(ser.encode(MessageType.NODE_OPEN, payload))[1].pid_ns == 0
