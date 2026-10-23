# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Region extensions: an out-of-tree counterpart of an access policy on the memory node.

A remote CXL agent can load one region extension, registered by a separately
installed package under the ``maru.region_extensions`` entry-point group and
named on the command line (``--region-extension``). It is the memory-side
half of a worker's access policy (``maru_handler.storage.access_policy``):

- It sees the regions this agent maps: :meth:`RegionExtension.validate_access`
  may refuse one before it is mapped, :meth:`RegionExtension.mapped` learns
  one once the NIC has it, and :meth:`RegionExtension.retire` is asked before
  one goes back. While it answers False the region stays here, out of use and
  no longer registered with the NIC, and the agent asks again on its sweep.
- It receives the messages policies send through their channel, after the
  agent checked that every range in a message belongs to a region the
  sending connection holds, matches its handle, fits in it, and that a read
  handle does not prepare a write. A read hint (``message["hint"]`` true)
  is advice that reads of its ranges will follow; its regions only need to
  be mapped and held here, by any connection, since the scheduler that
  sends it holds none, and it never prepares a write. The agent cannot
  read ``action``, so the extension must treat every message with ``hint``
  true as read-only, whatever its action. A hint's ``conn`` holds no
  region and may close without telling: state kept for a hint ends with
  the hint's own end message or the extension's expiry, never with a
  release of that connection. The extension may ignore any hint.

The agent owns mapping, NIC registration and the checks above; the extension
owns whatever it does to the memory of those regions.

Threads and locks: ``request`` and ``retire`` run under the agent's lock, so
every other request of every worker waits for them; they must return
promptly, and work that waits on hardware belongs on the extension's own
threads. ``validate_access`` and ``mapped`` run on the threads that map
regions, possibly several at once. The extension must be thread-safe.

``retire`` may be asked about a region ``mapped`` never saw (its mapping
failed half way); the extension answers True for a region it does not know.
"""

from __future__ import annotations

import importlib.metadata
from collections.abc import Iterable
from typing import Any, Protocol, runtime_checkable

#: Entry-point group scanned for region extensions.
REGION_EXTENSION_GROUP = "maru.region_extensions"


@runtime_checkable
class RegionExtension(Protocol):
    """Hooks a remote CXL agent calls around the regions it maps."""

    def validate_access(self, handle: dict, access: Any) -> None:
        """Raise to refuse mapping a region (``access`` is the resource
        manager's answer for it: device path and UUID)."""
        ...

    def mapped(self, handle: dict, access: Any) -> None:
        """A region is mapped and registered with the NIC."""
        ...

    def request(self, conn: int, message: dict, ranges: list[dict]) -> dict:
        """Answer a policy message whose ``ranges`` the agent checked."""
        ...

    def retire(self, handle: dict) -> bool:
        """Whether a region the NIC no longer reaches may go back now."""
        ...

    def close(self) -> None:
        """The agent is ending."""
        ...


def load_region_extension(
    name: str,
    epoch: str,
    config: str = "",
    *,
    entry_points: Iterable[Any] | None = None,
) -> RegionExtension:
    """Load the region extension registered under ``name``.

    Args:
        name: Entry-point name in :data:`REGION_EXTENSION_GROUP`.
        epoch: Identity of this agent process; messages from a worker carry
            the epoch the worker saw.
        config: Opaque string passed to the factory (typically the path of
            the extension's own configuration file).
        entry_points: Entry points to search instead of the installed ones
            (tests).

    Returns:
        The extension the factory built.

    Raises:
        ValueError: if no installed package registers ``name``, or the
            object built lacks a hook.
    """
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
