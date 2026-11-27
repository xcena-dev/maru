# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Device hooks for a memory node's local CXL regions.

Handles retain MaruHandle.to_dict() keys. Hints preserve contents and are advice.
The extension is thread-safe: generation, retire and ordinary hint hooks answer
from local state without waiting. Only write_wait may wait, up to timeout_ms.
Mapping hooks may block. Retire follows NIC revocation and precedes RM release.
"""

from __future__ import annotations

import importlib.metadata
from collections.abc import Iterable
from typing import Any, Protocol, runtime_checkable

REGION_EXTENSION_GROUP = "maru.region_extensions"
DEFAULT_HINT_ACTIONS = ("read_hint", "read_hint_end")
HINT_ACTIONS = (*DEFAULT_HINT_ACTIONS, "read_promote", "write_hint", "write_wait")


class RegionExtensionError(RuntimeError):
    """A region extension could not start or map memory."""


@runtime_checkable
class RegionExtension(Protocol):
    """Required region hooks, compatible with the remote CXL agent."""

    def validate_access(self, handle: dict, access: Any) -> None:
        """Refuse a region before mapping; access carries its path and UUID."""
        ...

    def mapped(self, handle: dict, access: Any, *, fresh: bool) -> None:
        """NIC registration finished; fresh means newly allocated, without KV."""
        ...

    def hint(self, conn: int, message: dict, ranges: list[dict]) -> dict | None:
        """Receive checked handle/offset/length ranges outside service locks."""
        ...

    def retire(self, handle: dict) -> bool:
        """Allow release after NIC revocation; unknown regions return True."""
        ...

    def close(self) -> None:
        """The memory node run is ending."""
        ...


class OptionalRegionHooks(Protocol):
    """Hooks called only when the extension defines them."""

    hint_actions: tuple[str, ...]

    def memory_generation(self) -> str:
        """Local generation; changes whenever memory may have lost contents.

        Calls are serialized on control and watch threads. Startup errors refuse
        service; runtime errors retain the last generation for the grace period.
        """
        ...

    def connection_closed(self, conn: int) -> None:
        """Discard this connection's hints without waiting."""
        ...


def load_region_extension(
    name: str,
    epoch: str,
    config: str = "",
    *,
    entry_points: Iterable[Any] | None = None,
) -> RegionExtension:
    """Call the named entry-point factory with the run epoch and opaque config."""
    found = entry_points
    if found is None:
        found = importlib.metadata.entry_points(group=REGION_EXTENSION_GROUP)
    matches = [ep for ep in found if ep.name == name]
    if not matches:
        raise ValueError(
            f"no package registers region extension {name!r} "
            f"(entry-point group {REGION_EXTENSION_GROUP})"
        )
    extension = matches[0].load()(epoch, config)
    if not isinstance(extension, RegionExtension):
        raise ValueError(f"region extension {name!r} does not implement every hook")
    return extension
