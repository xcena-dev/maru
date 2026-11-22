# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Node config pieces the tier drivers share with the config: the error,
sizes, field readers, a tier's parsed config and a store's TierSpec.

A leaf module: it imports neither the config nor the drivers.
"""

import math
import re
from collections.abc import Callable, Iterator, Mapping, Set
from dataclasses import dataclass, field
from typing import Any

from maru_common.storage_types import PoolKind, Visibility

from .tiers import Level, Tier, TierEnv


class NodeConfigError(ValueError):
    """The node config breaks a rule; the message names the field."""


class NotServableError(RuntimeError):
    """The config asks for something not supported."""


_SIZE_RE = re.compile(r"^\s*(\d+)\s*([KMGT]?)(?:i?B)?\s*$", re.IGNORECASE)
_UNITS = {"": 1, "K": 1 << 10, "M": 1 << 20, "G": 1 << 30, "T": 1 << 40}


def parse_size(value: Any, what: str) -> int:
    """Bytes from an int or a string like "32M" or "1GiB" (binary units)."""
    if isinstance(value, bool):
        raise NodeConfigError(f"{what}: expected a size, got {value!r}")
    if isinstance(value, int):
        if value < 0:
            raise NodeConfigError(f"{what}: must not be negative, got {value}")
        return value
    if isinstance(value, str):
        m = _SIZE_RE.match(value)
        if m:
            return int(m.group(1)) * _UNITS[m.group(2).upper()]
    raise NodeConfigError(f'{what}: not a size: {value!r} (e.g. 4096, "32M", "1G")')


def _require_keys(obj: Any, allowed: Set[str], what: str) -> dict:
    if not isinstance(obj, dict):
        raise NodeConfigError(f"{what}: expected an object, got {type(obj).__name__}")
    unknown = sorted(set(obj) - allowed)
    if unknown:
        raise NodeConfigError(
            f"{what}: unknown keys {unknown} (allowed: {sorted(allowed)})"
        )
    return obj


def _str(obj: dict, key: str, what: str, default: str | None = None) -> str:
    value = obj.get(key, default)
    if value is None:
        raise NodeConfigError(f"{what}.{key}: required")
    if not isinstance(value, str) or not value.strip():
        raise NodeConfigError(
            f"{what}.{key}: expected a non-empty string, got {value!r}"
        )
    return value


def _bool(obj: dict, key: str, what: str, default: bool) -> bool:
    value = obj.get(key, default)
    if not isinstance(value, bool):
        raise NodeConfigError(f"{what}.{key}: expected true or false, got {value!r}")
    return value


def _number(obj: dict, key: str, what: str, default: float) -> float:
    value = obj.get(key, default)
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not math.isfinite(value)
    ):
        raise NodeConfigError(f"{what}.{key}: expected a finite number, got {value!r}")
    return float(value)


class Options(Mapping[str, Any]):
    """A store's other keys: read-only, but copied and pickled like a dict."""

    __slots__ = ("_items",)

    def __init__(self, items: Mapping[str, Any] | None = None):
        self._items = dict(items or {})

    def __getitem__(self, key: str) -> Any:
        return self._items[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __reduce__(self) -> tuple[Any, ...]:
        return (Options, (self._items,))

    def __repr__(self) -> str:
        return f"Options({self._items!r})"


@dataclass(frozen=True)
class TierConfig:
    store: str
    size: int | None = None
    high_watermark: float | None = None
    low_watermark: float | None = None
    hugepage: bool | None = None
    region_size: int | None = None
    rm: str | None = None
    visibility: Visibility | None = None
    # Keys of a store that has no field above (read-only)
    options: Mapping[str, Any] = field(default_factory=Options, repr=False, hash=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "options", Options(self.options))

    @property
    def is_memory(self) -> bool:
        """Workers map its pools (an L1 store)."""
        # The registry imports the drivers, which import this module
        from .drivers.registry import TIERS

        spec = TIERS.get(self.store)
        return spec is not None and spec.level is Level.L1


@dataclass(frozen=True)
class TierContext:
    """What a store's parse gets besides its raw object: where the tier is,
    and the common fields as the config read them."""

    what: str  # the tier in error messages, e.g. "tiers[0] (local_cxl)"
    index: int
    page_size: int
    last: bool
    size: int | None  # in bytes; None when the store's size is optional


def _no_fields(raw: dict, ctx: TierContext) -> dict[str, Any]:
    return {}


def _always_servable(tier: TierConfig) -> None:
    pass


@dataclass(frozen=True)
class TierSpec:
    """A store: its config keys and rules, and the tier it makes. A medium
    on an existing wire kind is a driver module with its SPEC and one entry
    in drivers/registry.py; a new wire kind also needs a PoolKind value and
    a POOL_KINDS row."""

    store: str
    level: Level
    local: bool  # its pools are this node's (in the PoolManager)
    kind: PoolKind | None  # the wire kind of its pools; None if not local
    keys: frozenset[str]  # the keys its config object may carry
    common: bool = True  # size and watermarks are read for it
    size_optional: bool = False
    last_only: bool = False  # it must be the last tier
    gpu_only: bool = False  # a memory node cannot have it
    # Its own keys, after the common ones: TierConfig fields (the rest in
    # "options"); raises NodeConfigError
    parse: Callable[[dict, TierContext], dict[str, Any]] = _no_fields
    # Raises NotServableError for a tier it cannot run
    check_servable: Callable[[TierConfig], None] = _always_servable
    # The tier the StorageService runs; None: parsed, not served
    make: Callable[[TierConfig, TierEnv], Tier] | None = None
    # It makes a Leased tier: claimed once every other tier is open
    leased: bool = False

    @property
    def served(self) -> bool:
        return self.make is not None
