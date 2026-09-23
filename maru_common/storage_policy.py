# SPDX-License-Identifier: Apache-2.0
"""Deterministic L1 selection; capacity checks are supplied by each backend."""

from dataclasses import dataclass


@dataclass(frozen=True)
class FixedOrderPolicy:
    order: tuple[str, ...]

    def candidates(self, available: set[str]) -> list[str]:
        return [medium for medium in self.order if medium in available]


def validate_order(order) -> tuple[str, ...]:
    if (
        not isinstance(order, (list, tuple))
        or len(order) != 2
        or not all(isinstance(medium, str) for medium in order)
        or set(order) != {"cpu", "cxl"}
    ):
        raise ValueError("Order must contain cpu and cxl exactly once")
    return tuple(order)
