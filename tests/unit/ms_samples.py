# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Fully populated sample values for message round-trip tests.

Default: every optional field set, two list items, last enum member.
``minimal=True``: optional fields None, one item, first enum member.
"""

import dataclasses
import enum
import types
import typing

from maru_shm.types import MaruHandle


def sample(tp, path: str = "v", minimal: bool = False):
    origin = typing.get_origin(tp)
    if origin in (typing.Union, types.UnionType):
        args = typing.get_args(tp)
        if minimal and type(None) in args:
            return None
        members = [a for a in args if a is not type(None)]
        return sample(members[0], path, minimal)
    if origin is list:
        (item,) = typing.get_args(tp)
        count = 1 if minimal else 2
        return [sample(item, f"{path}[{i}]", minimal) for i in range(count)]
    if origin is tuple:
        item = typing.get_args(tp)[0]
        count = 1 if minimal else 2
        return tuple(sample(item, f"{path}[{i}]", minimal) for i in range(count))
    if origin is dict:
        _, value = typing.get_args(tp)
        return {
            "dram": sample(value, f"{path}.dram", minimal),
            "cxl": sample(value, f"{path}.cxl", minimal),
        }
    if tp is MaruHandle:
        return MaruHandle(region_id=7, offset=4096, length=1 << 30, auth_token=12345)
    if isinstance(tp, type) and issubclass(tp, enum.Enum):
        return list(tp)[0 if minimal else -1]
    if dataclasses.is_dataclass(tp):
        hints = typing.get_type_hints(tp)
        return tp(
            **{
                f.name: sample(hints[f.name], f"{path}.{f.name}", minimal)
                for f in dataclasses.fields(tp)
            }
        )
    if tp is str:
        return f"s:{path}"
    if tp is bool:
        return True
    if tp is int:
        return 3 + 7 * len(path)
    if tp is float:
        return 1.5
    if tp is bytes:
        return bytes(range(16))
    raise TypeError(f"no sample for {tp!r} at {path}")
