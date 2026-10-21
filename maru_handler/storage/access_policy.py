# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Access policies: out-of-tree control over when a storage backend touches memory.

Some memory answers every address but not always at the same speed: a device
may keep a small fast cache in front of slower media, so data has to be
brought in before it is read and written back after it is written. Maru keeps
no knowledge of such devices. A separately installed package registers an
access policy under the ``maru.access_policies`` entry-point group, and a
worker selects it by name in ``MaruConfig.access_policy``. Installing a
package selects nothing; only the configured name is loaded.

What Maru owns and what the policy owns:

- Maru decides which keys hit, which pages hold them, which bytes move and
  whether a key is published. The policy never sees keys and never moves
  bytes.
- The policy decides *when* the bytes of a read move (it calls the transfer
  Maru hands it, in order), whether a page may be written now, and whether a
  page given back may be reused yet.
- The policy talks to its own counterpart next to the memory through
  :class:`AccessChannel`. Maru carries those messages over the connection it
  already authenticated, and the memory side checks that every range in a
  message belongs to a region the sending connection holds before the
  counterpart sees it.

Every hook may block; the storage backend calls it without its lock held. A
hook that raises makes the operation take an outcome Maru already has:

=====================  ===================================================
hook raises             outcome
=====================  ===================================================
``read``                the hits load as misses (the engine recomputes)
``write_batch_limit``   the pages are not written; the keys are not stored
``prepare_write``       the pages are not written; the keys are not stored
``written``             the keys are not stored
``completed_write``     the late WRITE's pages stay out of use; retried
``reusable``            the page stays out of use; asked again later
=====================  ===================================================
"""

from __future__ import annotations

import importlib.metadata
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict, dataclass
from typing import Any, Protocol, runtime_checkable

from maru_common.storage_types import StorageLocation

#: Entry-point group scanned for access policies.
ACCESS_POLICY_GROUP = "maru.access_policies"


@dataclass(frozen=True)
class AccessRange:
    """One page of a region that an access touches.

    Attributes:
        handle: The region capability the worker maps the region with: its
            owner handle, or the read handle MaruServer gave with a lease.
        location: Where the page lies in the region.
    """

    handle: dict
    location: StorageLocation

    def to_dict(self) -> dict:
        """Wire form: the handle and the location's fields."""
        return {"handle": dict(self.handle), **asdict(self.location)}


class AccessChannel(Protocol):
    """Messages from a policy to its counterpart next to the memory."""

    def request(
        self,
        action: str,
        ranges: Sequence[AccessRange] = (),
        *,
        write: bool = False,
        **payload: Any,
    ) -> dict:
        """Send one message and return the counterpart's reply.

        Args:
            action: Policy-defined action name; Maru does not interpret it.
            ranges: Pages the message is about. The memory side checks each
                before the counterpart sees it.
            write: True if the message prepares a write; a read handle may
                not prepare one.
            **payload: Further policy-defined fields (msgpack-encodable).

        Returns:
            The counterpart's reply.

        Raises:
            StorageUnavailableError: if the connection that mapped the region
                changed, so the counterpart may not know the region.
            Exception: if the memory side refused the message.

        A message may be delivered more than once when a connection breaks;
        actions must be idempotent.
        """
        ...


@runtime_checkable
class AccessPolicy(Protocol):
    """Hooks a storage backend calls around its data transfers."""

    def read(
        self,
        channel: AccessChannel,
        ranges: Sequence[AccessRange],
        transfer: Callable[[int, int], None],
    ) -> None:
        """Move the bytes of ``ranges`` by calling ``transfer``.

        ``transfer(start, end)`` reads ``ranges[start:end]``. The policy must
        call it in order, with each ``start`` equal to the previous ``end``,
        until every range is read; it decides only when each call happens.
        An exception ``transfer`` raises must propagate.
        """
        ...

    def write_batch_limit(self, channel: AccessChannel) -> int:
        """How many pages of one region may be written in one batch."""
        ...

    def prepare_write(
        self, channel: AccessChannel, ranges: Sequence[AccessRange]
    ) -> Any:
        """Make ``ranges`` ready to be written; return a token for :meth:`written`."""
        ...

    def written(self, channel: AccessChannel, token: Any) -> None:
        """The WRITE prepared under ``token`` completed."""
        ...

    def completed_write(self, locations: Sequence[StorageLocation]) -> None:
        """A WRITE whose completion was uncertain turned out to have completed."""
        ...

    def reusable(self, location: StorageLocation) -> bool:
        """Whether a page given back may hold new data now."""
        ...


def load_access_policy(
    name: str,
    config: str = "",
    *,
    entry_points: Iterable[Any] | None = None,
) -> AccessPolicy:
    """Load the access policy registered under ``name``.

    Args:
        name: Entry-point name in :data:`ACCESS_POLICY_GROUP`.
        config: Opaque string passed to the policy factory (typically the
            path of the policy's own configuration file).
        entry_points: Entry points to search instead of the installed ones
            (tests).

    Returns:
        The policy the factory built.

    Raises:
        ValueError: if no installed package registers ``name``, or the
            object built lacks a hook.
    """
    found = entry_points
    if found is None:
        found = importlib.metadata.entry_points(group=ACCESS_POLICY_GROUP)
    matches = [ep for ep in found if ep.name == name]
    if not matches:
        raise ValueError(
            f"no package registers access policy {name!r} "
            f"(entry-point group {ACCESS_POLICY_GROUP})"
        )
    policy = matches[0].load()(config)
    if not isinstance(policy, AccessPolicy):
        raise ValueError(f"access policy {name!r} does not implement every hook")
    return policy
