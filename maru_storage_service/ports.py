# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Ports for node stores, roles, metadata and workers."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Protocol

from maru_common.node_protocol import Notify
from maru_common.protocol import (
    CommitItem,
    CommitResponse,
    NodeHeartbeatResponse,
    RetireResponse,
    WorkerExitResponse,
)
from maru_common.storage_types import (
    ExitCause,
    KeptCause,
    KeyLocation,
    PoolUsage,
    WorkerRef,
)

if TYPE_CHECKING:
    from .config import TierTraits
    from .node_run import NodeRun
    from .node_store import NodeStore
    from .placement import PlacementPolicy
    from .pool_manager import PageRef
    from .tiers import L1Pool, Leased


class LocalPinPort(Protocol):
    """Local-reader state used when removing pages."""

    def block(self, refs: Iterable[PageRef]) -> None: ...

    def unblock(self, refs: Iterable[PageRef]) -> None: ...

    def after_kept(self, ref: PageRef, cause: KeptCause) -> None: ...

    def forget(self, ref: PageRef) -> None: ...

    def readers(self, ref: PageRef) -> int: ...


class NodeRole(Protocol):
    """Role phases called in a node run's lifecycle order."""

    name: str

    def capabilities(self) -> list[str]: ...

    def bind(self, run: NodeRun) -> None: ...

    def make_store(
        self, run: NodeRun, placement: PlacementPolicy, traits: dict[str, TierTraits]
    ) -> NodeStore: ...

    def serve(self, run: NodeRun) -> None: ...

    def unmap_leased(
        self, run: NodeRun, tiers: Sequence[Leased], pools: Sequence[L1Pool]
    ) -> set[str]: ...

    def stop_workers(self) -> None: ...

    def worker_settled(self, run: NodeRun, incarnation: str) -> None: ...


class ServerStopped(Exception):  # noqa: N818 - a state, not a failure
    """The client is stopping; the request was given up unresolved."""


class EpochChanged(Exception):  # noqa: N818 - a state, not a failure
    """MaruServer no longer knows the node session: this run is over."""


class NodeOpenError(RuntimeError):
    """MaruServer refused NODE_OPEN."""


class ReplyError(RuntimeError):
    """MaruServer answered OK with the wrong number of items; the op is resolved."""


class MetadataPort(Protocol):
    """The node session with MaruServer. Each op is resent with the same op
    until its result is known (OK, REJECTED or DISABLED); EpochChanged,
    ServerStopped and ReplyError end it otherwise. After EpochChanged no op
    is sent."""

    # server_epoch of the session; when the session was last confirmed
    # (monotonic)
    server_epoch: str | None
    session_ttl_s: float
    last_heartbeat_ok: float

    @property
    def epoch_lost(self) -> bool:
        """MaruServer no longer knows the session."""
        ...

    def start_heartbeat(
        self,
        usage: Callable[[], list[PoolUsage]],
        on_response: Callable[[NodeHeartbeatResponse], None],
        on_epoch: Callable[[str], None],
    ) -> None: ...

    def commit(self, items: list[CommitItem]) -> CommitResponse: ...

    def retire(self, items: list[KeyLocation]) -> RetireResponse: ...

    def worker_exit(
        self, workers: list[WorkerRef], cause: ExitCause
    ) -> WorkerExitResponse: ...

    def stop(self) -> None:
        """Give up unresolved ops and stop the heartbeat."""
        ...


class WorkerEndpoint(Protocol):
    """A worker's connection, as used outside its own requests."""

    def close(self) -> None: ...

    def post_notify(self, notify: Notify) -> None:
        """Queue ``notify`` without blocking; any thread may call it. Notices
        to one connection arrive in the order they were posted; those posted
        while it is closed are dropped."""
        ...


class ProcessWatcher(Protocol):
    """Watches processes by (pid, pid_start) under a key and calls
    ``on_exit(key)`` once when one ends."""

    def start(self, on_exit: Callable[[str], None]) -> None: ...

    def stop(self) -> None: ...

    def watch(self, key: str, pid: int, pid_start: int) -> bool:
        """False if the process already ended."""
        ...

    def unwatch(self, key: str) -> None: ...

    def watching(self, key: str) -> bool: ...
