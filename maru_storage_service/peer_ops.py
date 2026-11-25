# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Per-origin credits and instance write-registration fences."""

from __future__ import annotations

import collections
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from maru_common.protocol import CommitItem
from maru_common.remote_protocol import (
    AgentsRequest,
    AgentsResponse,
    CancelRequest,
    CancelResponse,
    FenceDomainRequest,
    FenceDomainResponse,
    FenceResult,
    RemoteHintRequest,
    RemoteHintResponse,
    RemoteWriteDoneRequest,
    RemoteWriteDoneResponse,
    ReserveRequest,
    ReserveResponse,
    RoomLimit,
    TransferEnd,
    peer_origin,
)
from maru_common.storage_types import (
    MAX_BATCH,
    ItemOutcome,
    ItemResult,
    Location,
    Status,
    WriteAgent,
    WriteDomain,
)

from .node_store import NodeStore
from .pool_manager import PageRef

logger = logging.getLogger(__name__)
SWEEP_S = 1.0


def peer_node(origin: str) -> str:
    return origin.removeprefix("peer:").rpartition("@")[0]


class WriterLimitError(ValueError):
    """No writer slot is available to start registration."""


class WriteRegistrations(Protocol):
    def writer(self, domain: WriteDomain) -> WriteAgent: ...
    def fence(self, domain: WriteDomain) -> None: ...
    def agents(self, origin: str, request: AgentsRequest) -> None: ...
    def forget_agents(self, origin: str) -> None: ...


@dataclass
class _Domain:
    tag: str
    state: str = "open"
    credits: dict[tuple[str, int, int], Location] = field(default_factory=dict)


def _page_key(location: Location) -> tuple[str, int, int]:
    return location.pool_id, location.offset, location.generation


class PeerOps:
    """WRITE_DONE admission and fence run under one lock."""

    def __init__(
        self,
        manager: NodeStore,
        registrations: WriteRegistrations,
        *,
        reserve_ttl_s: float,
        peer_quarantine_max: int,
        quarantine_max: int,
        clock: Callable[[], float] = time.monotonic,
        check_memory: Callable[[], bool] = lambda: True,
    ):
        self.manager, self.registrations = manager, registrations
        self.pools = manager.pools
        self.page_size = self.pools.page_size
        self.reserve_ttl_s = reserve_ttl_s
        self.peer_quarantine_max, self.quarantine_max = (
            peer_quarantine_max,
            quarantine_max,
        )
        self._clock = clock
        self.check_memory = check_memory
        self._lock = threading.Lock()
        self.quarantined: collections.Counter[str] = collections.Counter()
        self._domains: dict[WriteDomain, _Domain] = {}
        self._last_received: dict[str, float] = {}
        self._reserves: dict[tuple[str, int], tuple[WriteDomain, list[Location]]] = {}
        self._tombstones: dict[tuple[str, int], WriteDomain] = {}
        self._retired: set[str] = set()
        self._stop = threading.Event()
        self._sweeper: threading.Thread | None = None

    def start(self) -> None:
        self._sweeper = threading.Thread(
            target=self._sweep_loop, name="maru-peer-ttl", daemon=True
        )
        self._sweeper.start()

    def stop(self) -> None:
        self._stop.set()
        if (
            self._sweeper is not None
            and self._sweeper is not threading.current_thread()
        ):
            self._sweeper.join(timeout=5.0)

    def _valid(self, origin: str, domain: WriteDomain | None) -> bool:
        return (
            domain is not None
            and bool(domain.instance)
            and domain.number > 0
            and peer_origin(domain.node_id, domain.node_epoch) == origin
            and origin not in self._retired
            and not self._stop.is_set()
        )

    def _capped(self, node_id: str) -> bool:
        return (
            self.quarantined[node_id] >= self.peer_quarantine_max
            or sum(self.quarantined.values()) >= self.quarantine_max
        )

    def room(self, origin: str) -> tuple[int, RoomLimit | None]:
        with self._lock:
            return self._room(origin)

    def _room(self, origin: str) -> tuple[int, RoomLimit | None]:
        if self._capped(peer_node(origin)):
            return 0, RoomLimit.QUARANTINE
        if origin in self._retired or self._stop.is_set():
            return 0, None
        return self.pools.credit_room(origin), None

    def reserve(
        self,
        origin: str,
        request: ReserveRequest,
        done: Callable[[ReserveResponse], None],
    ) -> None:
        if not self.check_memory():
            done(
                ReserveResponse(
                    Status.BUSY, error="memory generation changed or unknown"
                )
            )
            return
        with self._lock:
            answer = self._reserve(origin, request)
        done(answer)

    def _reserve(self, origin: str, request: ReserveRequest) -> ReserveResponse:
        domain = request.domain
        if not self._valid(origin, domain) or not 0 < request.pages <= MAX_BATCH:
            return ReserveResponse(Status.REJECTED, error="invalid reserve/domain")
        assert domain is not None
        self._last_received[origin] = self._clock()
        self._acked(origin, request.op.ack)
        if (origin, request.op.seq) in self._tombstones:
            return ReserveResponse(Status.OK)
        room, limit = self._room(origin)
        if limit is not None:
            return ReserveResponse(Status.OK, room=room, limit=limit)
        entry = self._domains.get(domain)
        if entry is not None and entry.state != "open":
            return ReserveResponse(
                Status.REJECTED,
                error="write domain closed",
                domain_fenced=entry.state == "fenced",
            )
        credits = self.pools.credit(origin, request.pages * self.page_size)
        if not credits:
            room, limit = self._room(origin)
            return ReserveResponse(Status.OK, room=room, limit=limit)
        try:
            agent = self.registrations.writer(domain)
        except WriterLimitError:
            for credit in credits:
                self.pools.abort(origin, credit)
            return ReserveResponse(Status.OK)
        except Exception:
            logger.exception("write registration failed")
            agent = None
        if entry is None:
            entry = self._domains[domain] = _Domain(f"write:{len(self._domains)}")
        for credit in credits:
            entry.credits[_page_key(credit)] = credit
        if agent is None:
            result = self._fence(domain, entry)
            return ReserveResponse(
                Status.REJECTED,
                error="write registration failed",
                domain_fenced=result == FenceResult.SUCCESS,
            )
        self._reserves[origin, request.op.seq] = domain, list(credits)
        room, limit = self._room(origin)
        return ReserveResponse(
            Status.OK, credits=credits, room=room, limit=limit, write_agent=agent
        )

    def _owns(self, entry: _Domain, location: Location) -> bool:
        return entry.credits.get(_page_key(location)) == location

    def write_done(
        self,
        origin: str,
        request: RemoteWriteDoneRequest,
        done: Callable[[RemoteWriteDoneResponse], None],
    ) -> None:
        if not self.check_memory():
            done(
                RemoteWriteDoneResponse(
                    Status.BUSY, error="memory generation changed or unknown"
                )
            )
            return
        outcomes: list[ItemOutcome | None] = [ItemOutcome(ItemResult.REJECTED)] * len(
            request.items
        )
        pages: dict[int, tuple[PageRef, CommitItem]] = {}
        with self._lock:
            domain = request.domain
            entry = self._domains.get(domain)
            if (
                not self._valid(origin, domain)
                or entry is None
                or entry.state != "open"
                or not 0 < len(request.items) <= MAX_BATCH
            ):
                answer = RemoteWriteDoneResponse(
                    Status.REJECTED, error="invalid write/domain"
                )
            else:
                self._acked(origin, request.op.ack)
                uncertain = {
                    _page_key(item.location)
                    for item in request.items
                    if item.end == TransferEnd.UNKNOWN
                    and self._owns(entry, item.location)
                }
                for i, item in enumerate(request.items):
                    if not self._owns(entry, item.location):
                        continue
                    if item.end == TransferEnd.UNKNOWN:
                        outcomes[i] = ItemOutcome(ItemResult.OK)
                    elif (
                        item.end == TransferEnd.DONE
                        and _page_key(item.location) not in uncertain
                    ):
                        ref, location, _ = self.pools.write_done(
                            origin, item.location, item.key, item.logical_bytes
                        )
                        if ref is not None and location is not None:
                            del entry.credits[_page_key(item.location)]
                            outcomes[i] = None
                            pages[i] = (
                                ref,
                                CommitItem(item.key, location, item.logical_bytes),
                            )
                # Accepted PENDING pages belong to their COMMIT tickets.
                if uncertain:
                    self._fence(domain, entry)
                answer = None
        if answer is not None:
            done(answer)
        else:
            self.manager.commit_written(
                outcomes,
                pages,
                lambda items: done(RemoteWriteDoneResponse(Status.OK, items=items)),
            )

    def cancel(
        self,
        origin: str,
        request: CancelRequest,
        done: Callable[[CancelResponse], None],
    ) -> None:
        with self._lock:
            domain = request.domain
            if not self._valid(origin, domain):
                answer = CancelResponse(Status.REJECTED, error="invalid cancel/domain")
            else:
                assert domain is not None
                self._acked(origin, request.op.ack)
                locations = list(request.locations)
                reserved = self._reserves.get((origin, request.reserve_seq))
                if request.reserve_seq is not None:
                    if reserved is None:
                        self._tombstones[origin, request.reserve_seq] = domain
                    elif reserved[0] == domain:
                        locations += reserved[1]
                        del self._reserves[origin, request.reserve_seq]
                entry = self._domains.get(domain)
                freed = 0
                if entry is not None and entry.state == "open":
                    for location in locations:
                        if (
                            self._owns(entry, location)
                            and self.pools.abort(origin, location)[0]
                        ):
                            del entry.credits[_page_key(location)]
                            freed += 1
                answer = CancelResponse(Status.OK, freed=freed)
        done(answer)

    def _acked(self, origin: str, ack: int) -> None:
        for table in (self._reserves, self._tombstones):
            for key in [key for key in table if key[0] == origin and key[1] <= ack]:
                del table[key]

    def _fence(self, domain: WriteDomain, entry: _Domain) -> FenceResult:
        if entry.state == "fenced":
            return FenceResult.SUCCESS
        entry.state = "fencing"
        origin = peer_origin(domain.node_id, domain.node_epoch)
        # Keep only unconfirmed credits in Q2; PENDING is never reclaimed here.
        for location in entry.credits.values():
            if self.pools.quarantine_credit(origin, location, entry.tag) is not None:
                self.quarantined[domain.node_id] += self.page_size
        try:
            self.registrations.fence(domain)
        except Exception:
            logger.exception("write domain revocation failed")
            entry.state = "failed"
            return FenceResult.FAILURE
        freed = self.pools.release_quarantine(entry.tag)
        self.quarantined[domain.node_id] -= freed * self.page_size
        entry.credits.clear()
        entry.state = "fenced"
        return FenceResult.SUCCESS

    def fence_domain(
        self,
        origin: str,
        request: FenceDomainRequest,
        done: Callable[[FenceDomainResponse], None],
    ) -> None:
        with self._lock:
            domain = request.domain
            if not self._valid(origin, domain):
                answer = FenceDomainResponse(
                    Status.REJECTED, error="invalid fence/domain"
                )
            else:
                entry = self._domains.setdefault(
                    domain, _Domain(f"write:{len(self._domains)}")
                )
                result = self._fence(domain, entry)
                answer = FenceDomainResponse(Status.OK, result=result)
        done(answer)

    def agents(
        self,
        origin: str,
        request: AgentsRequest,
        done: Callable[[AgentsResponse], None],
    ) -> None:
        with self._lock:
            try:
                if origin in self._retired or self._stop.is_set():
                    raise ValueError("the run ended")
                self.registrations.agents(origin, request)
                answer = AgentsResponse(Status.OK)
            except Exception:
                answer = AgentsResponse(Status.REJECTED, error="agent update refused")
        done(answer)

    def hint(
        self,
        origin: str,
        request: RemoteHintRequest,
        done: Callable[[RemoteHintResponse], None],
    ) -> None:
        done(RemoteHintResponse(Status.REJECTED, error="hint action unsupported"))

    def peer_received(self, origin: str) -> None:
        with self._lock:
            if origin not in self._retired and not self._stop.is_set():
                self._last_received[origin] = self._clock()

    def peer_lost(self, origin: str) -> None:
        with self._lock:
            for domain, entry in self._domains.items():
                if (
                    peer_origin(domain.node_id, domain.node_epoch) == origin
                    and entry.state == "open"
                ):
                    self._fence(domain, entry)

    def peer_retired(self, origin: str) -> None:
        with self._lock:
            self._retired.add(origin)
            for domain, entry in self._domains.items():
                if (
                    peer_origin(domain.node_id, domain.node_epoch) == origin
                    and entry.state == "open"
                ):
                    self._fence(domain, entry)
            self._acked(origin, float("inf"))
            self._last_received.pop(origin, None)
            self.registrations.forget_agents(origin)
        self.pools.forget_holder(origin)

    def sweep(self) -> int:
        if not self.check_memory():
            return 0
        with self._lock:
            now = self._clock()
            expired = [
                (domain, entry)
                for domain, entry in self._domains.items()
                if entry.state == "open"
                and entry.credits
                and now
                - self._last_received[peer_origin(domain.node_id, domain.node_epoch)]
                >= self.reserve_ttl_s
            ]
            count = sum(len(entry.credits) for _, entry in expired)
            for domain, entry in expired:
                self._fence(domain, entry)
            return count

    def _sweep_loop(self) -> None:
        while not self._stop.wait(min(SWEEP_S, self.reserve_ttl_s / 4)):
            try:
                self.sweep()
            except Exception:
                logger.exception("credit TTL sweep failed")
