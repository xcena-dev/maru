# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Reference pins on exact locations.

Pins never expire by time. Not thread safe: callers hold the service lock.
"""

import itertools
import uuid
from collections.abc import Callable
from dataclasses import dataclass, replace
from enum import StrEnum, unique

from maru_common.storage_types import Key, Location


@unique
class HolderKind(StrEnum):
    WORKER = "worker"  # LOOKUP_AND_PIN


@dataclass(frozen=True)
class Pin:
    pin_id: str
    key: Key
    location: Location
    holder: str  # session token
    holder_kind: HolderKind
    keep_on_departure: bool = True


class PinManager:
    def __init__(self) -> None:
        # unique across MaruServer runs: random prefix and a counter
        self._prefix = uuid.uuid4().hex[:12]
        self._counter = itertools.count(1)
        self._pins: dict[str, Pin] = {}
        # Pins retained when their holder leaves its node run are indexed apart.
        self._by_holder: dict[str, set[str]] = {}
        self._retained_by_holder: dict[str, set[str]] = {}
        # pin ids by the pool of their location: a node given up drops them
        self._by_pool: dict[str, set[str]] = {}
        self._counts: dict[tuple[Key, Location], int] = {}
        # called with (key, location) when its last pin goes
        self.on_last_unpin: Callable[[Key, Location], None] | None = None

    def __len__(self) -> int:
        return len(self._pins)

    def pin(
        self,
        key: Key,
        location: Location,
        holder: str,
        holder_kind: HolderKind,
        *,
        keep_on_departure: bool = True,
    ) -> str:
        pin_id = f"{self._prefix}-{next(self._counter):x}"
        pin = Pin(
            pin_id,
            key,
            location,
            holder,
            holder_kind,
            keep_on_departure and holder_kind == HolderKind.WORKER,
        )
        self._pins[pin_id] = pin
        self._index(pin).setdefault(holder, set()).add(pin_id)
        self._by_pool.setdefault(location.pool_id, set()).add(pin_id)
        target = (key, location)
        self._counts[target] = self._counts.get(target, 0) + 1
        return pin_id

    def get(self, pin_id: str) -> Pin | None:
        return self._pins.get(pin_id)

    def count(self, key: Key, location: Location) -> int:
        return self._counts.get((key, location), 0)

    def held_by(self, holder: str) -> list[Pin]:
        return [
            self._pins[p]
            for index in (self._by_holder, self._retained_by_holder)
            for p in index.get(holder, ())
        ]

    def holds(self, holder: str, retained: bool = True) -> bool:
        """Whether the holder has pins, including retained ones if requested."""
        return holder in self._by_holder or (
            retained and holder in self._retained_by_holder
        )

    def holds_retained(self, holder: str) -> bool:
        return holder in self._retained_by_holder

    def unpin(self, pin_id: str) -> Pin | None:
        pin = self._pins.pop(pin_id, None)
        if pin is None:
            return None
        index = self._index(pin)
        held = index.get(pin.holder)
        if held is not None:
            held.discard(pin_id)
            if not held:
                del index[pin.holder]
        self._drop_count(pin)
        return pin

    def take_pool(self, pool_id: str) -> set[str]:
        """The ids of every pin on ``pool_id``'s locations, which the caller
        unpins; no new pin may come on them (its pool is being removed)."""
        return self._by_pool.pop(pool_id, set())

    def release_holder(
        self, holder: str, limit: int | None = None, keep_retained: bool = False
    ) -> int:
        """Release at most ``limit`` pins, optionally keeping departure pins."""
        released = 0
        indexes = (
            (self._by_holder,)
            if keep_retained
            else (self._by_holder, self._retained_by_holder)
        )
        for index in indexes:
            held = index.get(holder)
            while held and (limit is None or released < limit):
                pin = self._pins.pop(held.pop())
                self._drop_count(pin)
                released += 1
            if held is not None and not held:
                del index[holder]
        return released

    def move_holder(self, old: str, new: str) -> int:
        """Give session ``old``'s pins to ``new`` (same-incarnation reopen)."""
        count = 0
        for index in (self._by_holder, self._retained_by_holder):
            moved = index.pop(old, set())
            for pin_id in moved:
                pin = self._pins[pin_id]
                self._pins[pin_id] = replace(pin, holder=new)
            if moved:
                index.setdefault(new, set()).update(moved)
            count += len(moved)
        return count

    def _index(self, pin: Pin) -> dict[str, set[str]]:
        if pin.keep_on_departure:
            return self._retained_by_holder
        return self._by_holder

    def _drop_count(self, pin: Pin) -> None:
        on_pool = self._by_pool.get(pin.location.pool_id)
        if on_pool is not None:
            on_pool.discard(pin.pin_id)
            if not on_pool:
                del self._by_pool[pin.location.pool_id]
        target = (pin.key, pin.location)
        left = self._counts[target] - 1
        if left:
            self._counts[target] = left
            return
        del self._counts[target]
        if self.on_last_unpin is not None:
            self.on_last_unpin(pin.key, pin.location)
