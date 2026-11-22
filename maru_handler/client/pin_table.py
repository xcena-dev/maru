# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""PinTable: held pins, each released once. Pins of an ended server epoch are
never unpinned: a restarted MaruServer holds none of them."""

import threading
from dataclasses import dataclass

from maru_common.storage_types import Key, Location


@dataclass
class PinEntry:
    pin_id: str
    key: Key
    location: Location
    server_epoch: str
    released: bool = False


class PinTable:
    def __init__(self):
        self._cv = threading.Condition()
        self._pins: dict[str, PinEntry] = {}

    def add(self, pin_id: str, key: Key, location: Location, server_epoch: str) -> None:
        with self._cv:
            if pin_id in self._pins:
                raise ValueError(f"pin {pin_id} held twice")
            self._pins[pin_id] = PinEntry(pin_id, key, location, server_epoch)

    def release(self, pin_ids: list[str], server_epoch: str | None) -> list[str]:
        """Mark pins released; returns the ones of ``server_epoch`` to UNPIN."""
        with self._cv:
            out = []
            for pin_id in pin_ids:
                entry = self._pins.pop(pin_id, None)
                if entry is None or entry.released:
                    continue
                entry.released = True
                if entry.server_epoch == server_epoch:
                    out.append(pin_id)
            self._cv.notify_all()
            return out

    def holds(self, pin_id: str) -> bool:
        with self._cv:
            return pin_id in self._pins

    def drop_epoch(self, server_epoch: str) -> list[str]:
        """Drop the pins of an ended server epoch, never to be unpinned;
        returns their ids. The reads holding them finish on their own."""
        with self._cv:
            dropped = [
                p.pin_id for p in self._pins.values() if p.server_epoch == server_epoch
            ]
            for pin_id in dropped:
                self._pins.pop(pin_id).released = True
            self._cv.notify_all()
            return dropped

    def outstanding(
        self, *, server_epoch: str | None = None, pool_id: str | None = None
    ) -> int:
        with self._cv:
            return sum(
                1
                for p in self._pins.values()
                if (server_epoch is None or p.server_epoch == server_epoch)
                and (pool_id is None or p.location.pool_id == pool_id)
            )

    def wait_none(
        self,
        *,
        server_epoch: str | None = None,
        pool_id: str | None = None,
        timeout: float | None = None,
    ) -> bool:
        """Wait until no such pin is held."""
        with self._cv:
            return bool(
                self._cv.wait_for(
                    lambda: (
                        self.outstanding(server_epoch=server_epoch, pool_id=pool_id)
                        == 0
                    ),
                    timeout,
                )
            )

    def __len__(self) -> int:
        with self._cv:
            return len(self._pins)
