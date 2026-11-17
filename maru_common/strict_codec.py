# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Strict decoding of message dataclasses from MessagePack values.

Each dataclass gets a decoder built once from its type hints. A decoder
checks every value against its annotation and builds the dataclass: an int
is no bool, a float takes an int, an Enum is built from its value, a tuple
takes a list, a missing Optional field without a default is None, any other
missing field without a default is an error and an unknown field is ignored.
Errors are ValueError naming the field path. MaruHandle and MaruPoolInfo are
built by their own from_dict(), as before. A dataclass with init=False fields
or an unresolved annotation has no decoder (TypeError).
"""

import dataclasses
import enum
import threading
import types
import typing
from collections.abc import Callable
from typing import Any

from maru_shm.types import MaruHandle, MaruPoolInfo

Decoder = Callable[[Any], Any]

_decoders: dict[Any, Decoder] = {}
_lock = threading.Lock()

# A missing field value is an error, or takes the field's default
_REQUIRED = object()
_DEFAULT = object()


class StrictDecodeError(ValueError):
    pass


def decoder_for(tp: Any) -> Decoder:
    """The decoder of ``tp``, built on first use; TypeError for an annotation
    it does not support."""
    decoder = _decoders.get(tp)
    if decoder is None:
        with _lock:
            decoder = _decoders.get(tp)
            if decoder is None:
                decoder = _build(tp, {})
                _decoders[tp] = decoder
    return decoder


def _type_name(value: Any) -> str:
    return type(value).__name__


def _fail(expected: str, value: Any) -> StrictDecodeError:
    return StrictDecodeError(f"expected {expected}, got {_type_name(value)}")


def _str(value: Any) -> Any:
    if isinstance(value, str):
        return value
    raise _fail("str", value)


def _int(value: Any) -> Any:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise _fail("int", value)


def _float(value: Any) -> Any:
    if isinstance(value, float):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return float(value)
    raise _fail("float", value)


def _bool(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    raise _fail("bool", value)


def _bytes(value: Any) -> Any:
    if isinstance(value, bytes):
        return value
    raise _fail("bytes", value)


def _any(value: Any) -> Any:
    return value


_SCALARS: dict[Any, Decoder] = {
    str: _str,
    int: _int,
    float: _float,
    bool: _bool,
    bytes: _bytes,
    Any: _any,
}


def _build(tp: Any, building: dict[Any, Decoder]) -> Decoder:
    """``building`` holds the dataclasses being built, for recursive types."""
    if tp in _SCALARS:
        return _SCALARS[tp]
    if tp in _decoders:
        return _decoders[tp]
    origin = typing.get_origin(tp)
    if origin in (typing.Union, types.UnionType):
        return _union(tp, building)
    if origin is list:
        return _list(tp, building)
    if origin is tuple:
        return _tuple(tp, building)
    if origin is dict:
        return _dict(tp, building)
    if isinstance(tp, type) and issubclass(tp, enum.Enum):
        return _enum(tp)
    if tp in (MaruHandle, MaruPoolInfo):
        return _from_dict(tp)
    if dataclasses.is_dataclass(tp) and isinstance(tp, type):
        return _dataclass(tp, building)
    raise TypeError(f"no strict decoder for {tp!r}")


def _enum(tp: type[enum.Enum]) -> Decoder:
    def decode(value: Any) -> Any:
        try:
            return tp(value)
        except ValueError:
            raise StrictDecodeError(f"{value!r} is no {tp.__name__} value") from None

    return decode


def _from_dict(tp: Any) -> Decoder:
    def decode(value: Any) -> Any:
        if isinstance(value, dict):
            try:
                value = tp.from_dict(value)
            except (KeyError, TypeError, ValueError) as e:
                raise StrictDecodeError(f"bad {tp.__name__}: {e!r}") from None
        if not isinstance(value, tp):
            raise _fail(tp.__name__, value)
        return value

    return decode


def _union(tp: Any, building: dict[Any, Decoder]) -> Decoder:
    args = typing.get_args(tp)
    optional = type(None) in args
    members = [_build(a, building) for a in args if a is not type(None)]
    if len(members) == 1:
        (member,) = members

        def decode_optional(value: Any) -> Any:
            return None if value is None else member(value)

        return decode_optional

    def decode(value: Any) -> Any:
        if value is None and optional:
            return None
        for member in members:
            try:
                return member(value)
            except (TypeError, ValueError):
                continue
        raise _fail(str(tp), value)

    return decode


def _list(tp: Any, building: dict[Any, Decoder]) -> Decoder:
    (item_tp,) = typing.get_args(tp) or (Any,)
    item = _build(item_tp, building)

    def decode(value: Any) -> Any:
        if not isinstance(value, list):
            raise _fail("list", value)
        out = []
        for i, v in enumerate(value):
            try:
                out.append(item(v))
            except (TypeError, ValueError) as e:
                raise StrictDecodeError(f"[{i}]: {e}") from None
        return out

    return decode


def _tuple(tp: Any, building: dict[Any, Decoder]) -> Decoder:
    args = typing.get_args(tp)
    if len(args) == 2 and args[1] is Ellipsis:
        item = _build(args[0], building)

        def decode_any_length(value: Any) -> Any:
            if not isinstance(value, list | tuple):
                raise _fail("list", value)
            out = []
            for i, v in enumerate(value):
                try:
                    out.append(item(v))
                except (TypeError, ValueError) as e:
                    raise StrictDecodeError(f"[{i}]: {e}") from None
            return tuple(out)

        return decode_any_length
    items = [_build(a, building) for a in args]

    def decode(value: Any) -> Any:
        if not isinstance(value, list | tuple) or len(value) != len(items):
            raise _fail(f"{len(items)} items", value)
        out = []
        for i, (item, v) in enumerate(zip(items, value, strict=True)):
            try:
                out.append(item(v))
            except (TypeError, ValueError) as e:
                raise StrictDecodeError(f"[{i}]: {e}") from None
        return tuple(out)

    return decode


def _dict(tp: Any, building: dict[Any, Decoder]) -> Decoder:
    key_tp, value_tp = typing.get_args(tp) or (Any, Any)
    key, item = _build(key_tp, building), _build(value_tp, building)

    def decode(value: Any) -> Any:
        if not isinstance(value, dict):
            raise _fail("map", value)
        out = {}
        for k, v in value.items():
            try:
                out[key(k)] = item(v)
            except (TypeError, ValueError) as e:
                raise StrictDecodeError(f"[{k!r}]: {e}") from None
        return out

    return decode


def _dataclass(tp: type, building: dict[Any, Decoder]) -> Decoder:
    if tp in building:
        return building[tp]
    # (name, decoder, what a missing value takes: _REQUIRED, _DEFAULT or None)
    fields: list[tuple[str, Decoder, Any]] = []

    def decode(value: Any) -> Any:
        if not isinstance(value, dict):
            raise _fail(f"a {tp.__name__} map", value)
        kwargs = {}
        for name, field_decoder, missing in fields:
            if name in value:
                try:
                    kwargs[name] = field_decoder(value[name])
                except (TypeError, ValueError) as e:
                    raise StrictDecodeError(f"{name}: {e}") from None
            elif missing is _REQUIRED:
                raise StrictDecodeError(f"missing value for field {name!r}")
            elif missing is not _DEFAULT:
                kwargs[name] = missing
        return tp(**kwargs)

    building[tp] = decode  # a field of this type refers back to it
    try:
        hints = typing.get_type_hints(tp)
    except (NameError, AttributeError, SyntaxError) as e:  # unresolved
        raise TypeError(f"{tp.__name__}: {e}") from None
    for f in dataclasses.fields(tp):
        if not f.init:
            raise TypeError(f"{tp.__name__}.{f.name} is not an __init__ field")
        hint = hints[f.name]
        if (
            f.default is not dataclasses.MISSING
            or f.default_factory is not dataclasses.MISSING
        ):
            missing = _DEFAULT
        elif type(None) in typing.get_args(hint):
            missing = None  # as dacite did
        else:
            missing = _REQUIRED
        fields.append((f.name, _build(hint, building), missing))
    return decode
