# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Region extensions: out-of-tree code next to the memory a remote CXL agent maps.

A remote CXL agent can load one region extension, registered by a separately
installed package under the ``maru.region_extensions`` entry-point group and
named on the command line (``--region-extension``). It serves memory that
answers every address but not always at the same speed, such as a CXL-SSD
with a DRAM cache in front of its flash: Maru's KV data path stays as it is,
and the extension only hears what helps the device serve it sooner.

- It sees the regions this agent maps: :meth:`RegionExtension.validate_access`
  may refuse one before it is mapped, :meth:`RegionExtension.mapped` learns
  one once the NIC has it, and :meth:`RegionExtension.retire` is asked before
  one goes back. While it answers False the region stays here, out of use and
  no longer registered with the NIC, and the agent asks again on its sweep.
- It receives read hints (:meth:`RegionExtension.hint`): ``read_hint`` names
  the ranges a request will read soon, in read order, and ``read_hint_end``
  says the request is over. The agent checks first that every range lies in
  a region mapped here and held by some connection, under a matching
  handle. The connection that sends a hint need hold no region (a serving
  engine's scheduler holds none) and may close without telling: state kept
  for a hint ends with its end, with the connection (optional
  ``connection_closed``) or with the extension's own expiry. Hints are
  advice; the extension may ignore any of them. The agent lets one
  connection keep a bounded number of requests open (hinted, not ended);
  the extension bounds what it keeps per request and in all, too.

The agent owns mapping, NIC registration and the checks above; the extension
owns whatever it does to the memory of those regions.

Threads, locks and latency: ``retire`` runs under the agent's lock, so every
other request of every worker waits for it. ``hint`` runs right after the
agent's checks, outside the lock, on the connection's request thread. Both
answer from the extension's local state within microseconds: ``hint`` only
queues the hint for the extension's own thread, in a bounded queue that
drops hints (never ends) when full, and neither waits on hardware or on
another process. ``validate_access`` and ``mapped`` run on the threads that
map regions, possibly several at once, and may wait. ``connection_closed``
runs outside the lock and must not block. The extension must be thread-safe.

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
        """A region is mapped and registered with the NIC.

        Raising makes the map fail and keeps the region out of use.
        """
        ...

    def hint(self, conn: int, message: dict, ranges: list[dict]) -> None:
        """Queue a read hint whose ``ranges`` the agent checked.

        ``message`` has ``action`` (``read_hint`` or ``read_hint_end``),
        ``request`` and ``epoch``; ``ranges`` are ``{"handle", "offset",
        "length"}`` maps in read order (empty for an end). Runs outside the
        agent's lock, after its checks; a region may have gone in between,
        so a hint may name one that is no longer mapped. It only queues,
        within microseconds, and its return value is ignored.
        """
        ...

    def retire(self, handle: dict) -> bool:
        """Whether a region the NIC no longer reaches may go back now
        (False while a command that changes its memory is still running).

        Runs under the agent's lock: answers from local state within
        microseconds."""
        ...

    def close(self) -> None:
        """The agent is ending."""
        ...


class OptionalRegionHooks(Protocol):
    """Hooks a region extension may also have; the agent calls each only if
    the extension defines it."""

    def connection_closed(self, conn: int) -> None:
        """Connection ``conn`` closed: end what the hints it sent started.

        Called after the agent dropped the connection's holds, for every
        connection that closes, whether or not it sent a hint.
        """
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
        epoch: Identity of this agent process; read hints carry the epoch
            their sender saw.
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
