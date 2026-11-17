# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Strict decoders: type checks, field paths and the shapes the messages use."""

from __future__ import annotations

import dataclasses
import enum
import typing
from dataclasses import dataclass, field

import pytest

from maru_common.node_protocol import NODE_MESSAGE_CLASSES
from maru_common.protocol import MS_MESSAGE_CLASSES
from maru_common.remote_protocol import REMOTE_MESSAGE_CLASSES
from maru_common.serializer import MessageDecodeError, Serializer
from maru_common.strict_codec import StrictDecodeError, decoder_for
from maru_shm.types import MaruHandle


class Color(enum.StrEnum):
    RED = "RED"
    BLUE = "BLUE"


@dataclass
class Inner:
    n: int
    color: Color = Color.RED


@dataclass
class Outer:
    name: str
    items: list[Inner]
    ratio: float = 0.0
    tags: tuple[str, ...] = ()
    pair: tuple[int, str] | None = None
    extra: dict[str, int] = field(default_factory=dict)
    flag: bool = False
    blob: bytes | None = None
    handle: MaruHandle | None = None


@dataclass
class Node:
    value: int
    child: Node | None = None


@dataclass
class Derived:
    a: int
    b: int = dataclasses.field(init=False, default=0)


@dataclass
class MaybeNoDefault:
    a: int | None
    b: int


@dataclass
class Forward:
    child: Unresolved  # noqa: F821


@dataclass
class QualifiedForward:
    child: typing.NoSuchType  # type: ignore[name-defined]


@pytest.mark.parametrize(
    "table", [MS_MESSAGE_CLASSES, NODE_MESSAGE_CLASSES, REMOTE_MESSAGE_CLASSES]
)
def test_every_strict_message_has_a_decoder(table):
    for request_cls, response_cls in table.values():
        decoder_for(request_cls)
        decoder_for(response_cls)


def decode(tp, value):
    return decoder_for(tp)(value)


def test_builds_nested_dataclasses():
    out = decode(
        Outer,
        {
            "name": "x",
            "items": [{"n": 1}, {"n": 2, "color": "BLUE"}],
            "ratio": 3,
            "tags": ["a", "b"],
            "pair": [7, "q"],
            "extra": {"k": 1},
            "flag": True,
            "blob": b"\x00",
            "handle": {"region_id": 1, "offset": 0, "length": 4096, "auth_token": 9},
        },
    )
    assert out == Outer(
        name="x",
        items=[Inner(1), Inner(2, Color.BLUE)],
        ratio=3.0,
        tags=("a", "b"),
        pair=(7, "q"),
        extra={"k": 1},
        flag=True,
        blob=b"\x00",
        handle=MaruHandle(region_id=1, offset=0, length=4096, auth_token=9),
    )
    assert type(out.ratio) is float
    assert out.items[1].color is Color.BLUE


def test_defaults_fill_missing_optional_fields_and_unknown_fields_are_ignored():
    assert decode(Outer, {"name": "x", "items": [], "later": 1}) == Outer("x", [])


@pytest.mark.parametrize(
    "payload, path",
    [
        ({"items": []}, "missing value for field 'name'"),
        ({"name": 1, "items": []}, "name: expected str"),
        ({"name": "x", "items": [{"n": True}]}, "items: [0]: n: expected int"),
        ({"name": "x", "items": [{"n": 1, "color": "GREEN"}]}, "items: [0]: color:"),
        ({"name": "x", "items": [], "ratio": False}, "ratio: expected float"),
        ({"name": "x", "items": [], "tags": "ab"}, "tags: expected list"),
        ({"name": "x", "items": [], "tags": {"a": 1}}, "tags: expected list"),
        ({"name": "x", "items": [], "pair": [1]}, "pair:"),
        ({"name": "x", "items": [], "extra": [1]}, "extra: expected map"),
        ({"name": "x", "items": [], "extra": {"k": "v"}}, "extra: ['k']: expected int"),
        ({"name": "x", "items": [], "flag": 1}, "flag: expected bool"),
        ({"name": "x", "items": [], "blob": "s"}, "blob:"),
        ({"name": "x", "items": [], "handle": {"region_id": 1}}, "handle:"),
        ({"name": "x", "items": {}}, "items: expected list"),
    ],
)
def test_rejects_a_wrong_value_naming_its_field(payload, path):
    with pytest.raises(StrictDecodeError) as e:
        decode(Outer, payload)
    assert path in str(e.value)


def test_a_dataclass_needs_a_map():
    with pytest.raises(StrictDecodeError, match="expected a Inner map"):
        decode(list[Inner], [[1]])


def test_recursive_types():
    assert decode(Node, {"value": 1, "child": {"value": 2}}) == Node(1, Node(2))
    with pytest.raises(StrictDecodeError, match="child: child: value"):
        decode(Node, {"value": 1, "child": {"value": 2, "child": {"value": "x"}}})


def test_a_missing_optional_without_default_is_none():
    assert decode(MaybeNoDefault, {"b": 2}) == MaybeNoDefault(None, 2)
    with pytest.raises(StrictDecodeError, match="missing value for field 'b'"):
        decode(MaybeNoDefault, {"a": 1})


def test_init_false_fields_or_unresolved_annotations_have_no_decoder():
    with pytest.raises(TypeError, match="Derived.b"):
        decoder_for(Derived)
    with pytest.raises(TypeError, match="Unresolved"):
        decoder_for(Forward)
    with pytest.raises(TypeError, match="NoSuchType"):
        decoder_for(QualifiedForward)


def test_a_message_without_decoder_is_a_decode_error_with_its_header():
    ser = Serializer({1: (Forward, Forward)})
    with pytest.raises(MessageDecodeError) as e:
        ser.decode_request_payload(ser.encode(1, {"child": {}}, seq=42))
    assert e.value.header is not None and e.value.header.sequence == 42


def test_an_unsupported_annotation_is_a_type_error():
    @dataclass
    class Odd:
        s: set[int]

    with pytest.raises(TypeError):
        decoder_for(Odd)
