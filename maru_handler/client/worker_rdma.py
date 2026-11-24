# SPDX-License-Identifier: Apache-2.0
"""Worker-owned RDMA staging and explicit GPU consumer ownership."""

from __future__ import annotations

import copy
import logging
import os
import threading
import time
import uuid
from collections import Counter, deque
from collections.abc import Callable
from dataclasses import dataclass, field

from maru_common.remote_protocol import TransferEnd
from maru_common.storage_types import (
    ItemOutcome,
    Location,
    PoolKind,
    ReadPeer,
    StorageLocation,
    WriteAgent,
)
from maru_handler.storage.cpu import CpuReadLease
from maru_handler.storage.staging import StagingBuffer
from maru_remote.transport import NixlTransport

logger = logging.getLogger(__name__)
_KEPT_UNTIL_EXIT: list[object] = []
_GATE_WAIT_S = 0.002


@dataclass(frozen=True)
class PeerAccess:
    node_id: str
    node_epoch: str
    agent_name: str
    boot: str
    conn_gen: int
    generation_known: bool
    pool_registration: int
    pool_generation: int
    base: int
    capacity: int
    kind: PoolKind
    metadata: bytes


@dataclass(eq=False)
class ReadRequest:
    owner: WorkerRdma
    outcome: ItemOutcome
    generation: int
    unpin: Callable[[list[str]], None]
    deadline: float
    flight: Flight | None = None
    reason: str = ""
    decided: bool = False
    _decision: dict[str, str] = field(default_factory=dict, repr=False)
    ready: threading.Event = field(default_factory=threading.Event)

    def done(self) -> bool:
        return self.ready.is_set() or time.monotonic() >= self.deadline

    def result(self) -> RemoteRead | None:
        if not self.owner.stopped.is_set():
            self.ready.wait(max(0, self.deadline - time.monotonic()))
        return self.owner.finish(self)

    def cancel(self) -> None:
        self.reason = "cancelled"
        self.ready.set()


@dataclass(eq=False)
class Flight:
    location: Location
    registration: int
    peer: PeerAccess
    generation: int
    slot: int
    current: Callable[[], bool] = lambda: True
    pending: object = None
    state: str = "reserved"
    waiters: list[ReadRequest] = field(default_factory=list)
    consumers: set[RemoteRead] = field(default_factory=set)


@dataclass(eq=False)
class RemoteRead(CpuReadLease):
    owner: WorkerRdma = None
    flight: Flight = None

    def release(self) -> None:
        self.owner.release(self)


@dataclass(eq=False)
class WriteBuffer:
    view: memoryview
    slot: int
    location: Location
    agent: WriteAgent
    size: int
    released: bool = False


def _transport(config, name):
    from maru_remote.checked import require_checked_transport

    require_checked_transport(os.environ)
    from maru_remote.native_agent import NativeNixlAgent

    agent = NativeNixlAgent(name, progress_thread=True, event_timeout_us=1000)
    params = {"ucx_devices": config.rdma_device} if config.rdma_device else {}
    agent.create_backend("UCX", params)
    return NixlTransport(name, agent=agent)


class WorkerRdma:
    def __init__(
        self,
        config,
        page_size,
        *,
        transport_factory=None,
        progress=True,
        quarantine_max=None,
    ):
        self.config = config
        self.gate = threading.Lock()
        self.generation = 0
        self.stopped = threading.Event()
        self.closed = False
        self.stats = Counter()
        self.name = f"maru-direct-{uuid.uuid4().hex}"
        self.staging = StagingBuffer(
            config.rdma_staging_size or max(1 << 30, 64 * page_size), page_size
        )
        self.kept_limit = max(
            1,
            min(
                self.staging.count // 2,
                (quarantine_max if quarantine_max is not None else self.staging.nbytes)
                // page_size,
            ),
        )
        self._free = deque(range(self.staging.count))
        self._flights: set[Flight] = set()
        self._joining: dict[tuple, Flight] = {}
        self._releases: deque[RemoteRead] = deque()
        self._peers: dict[str, PeerAccess] = {}
        self._ready_peers: dict[str, PeerAccess] = {}
        self._peer_lock = threading.Lock()
        self._loaded: dict[str, bytes] = {}
        self._credit_agents: dict[Location, WriteAgent] = {}
        self._writing: set[WriteBuffer] = set()
        self._retired_domains = set()
        self._domain_agents = {}
        self._agent_domains = {}
        self._write_pending: list[object] = []
        self._wake = threading.Event()
        self._thread = None
        self.transport = None
        try:
            self.staging.cuda_register()
            self.transport = (
                transport_factory or (lambda name: _transport(config, name))
            )(self.name)
            self.transport.register(
                self.staging.address, self.staging.nbytes, "worker-staging"
            )
        except BaseException:
            try:
                if self.transport is not None:
                    self.transport.close()
                self.staging.close()
            except Exception:
                _KEPT_UNTIL_EXIT.append(self)
            raise
        if progress:
            self._thread = threading.Thread(
                target=self._run, name="maru-rdma-progress", daemon=True
            )
            self._thread.start()

    @property
    def slot_count(self):
        return self.staging.count

    def _acquire_gate(self, deadline=None):
        timeout = _GATE_WAIT_S
        if deadline is not None:
            timeout = min(timeout, max(0, deadline - time.monotonic()))
        return self.gate.acquire(timeout=timeout)

    def capacity(self):
        if not self._acquire_gate():
            return 0
        try:
            if self.stopped.is_set() or self.closed:
                return 0
            if sum(f.state == "kept" for f in self._flights) >= self.kept_limit:
                return 0
            return len(self._free)
        finally:
            self.gate.release()

    def snapshot(self):
        if not self.gate.acquire(False):
            return {"slots": self.slot_count, "busy": True}
        try:
            kept = sum(f.state == "kept" for f in self._flights)
            return {
                "slots": self.slot_count,
                "free_slots": len(self._free),
                "kept_slots": kept,
                "kept_bytes": kept * self.staging.slot_bytes,
                "consumers": sum(len(f.consumers) for f in self._flights),
                "write_slots": len(self._writing),
            }
        finally:
            self.gate.release()

    def capture(self):
        if not self._acquire_gate():
            return None
        try:
            return None if self.closed or self.stopped.is_set() else self.generation
        finally:
            self.gate.release()

    def advance(self, *, drop_credits=False):
        """Invalidate reads before a lifecycle operation drops their pins."""
        with self.gate:
            self.generation += 1
            if drop_credits:
                self._credit_agents.clear()
            for flight in self._flights:
                for request in flight.waiters:
                    request.reason = "generation"
                    request.ready.set()
        self._wake.set()

    def stop(self):
        """Record CUDA stop immediately, including during a post."""
        self.stopped.set()
        for flight in tuple(self._flights):
            for request in tuple(flight.waiters):
                request.ready.set()
        self._wake.set()

    def _load(self, name, metadata):
        if self._loaded.get(name) == metadata:
            return True
        if name in self._loaded and not self.transport.disconnect(name):
            return False
        try:
            loaded = self.transport.add_peer(metadata, peer=name)
            if loaded != name:
                return False
        except Exception:
            logger.exception("cannot load RDMA peer %s", name)
            return False
        self._loaded[name] = metadata
        return True

    def notify_peers(self, peers: list[ReadPeer]):
        """Publish invalidation before installing changed metadata."""
        if self.closed or self.stopped.is_set():
            return
        previous = self._peers
        entries = {}
        for peer in peers:
            for pool in peer.pools:
                access = PeerAccess(
                    peer.node_id,
                    peer.node_epoch,
                    peer.agent_name,
                    peer.boot,
                    peer.conn_gen,
                    peer.generation_known,
                    pool.pool_registration,
                    pool.pool_generation,
                    pool.base,
                    pool.capacity,
                    pool.kind,
                    peer.agent_metadata,
                )
                old = previous.get(pool.pool_id)
                if (
                    old is not None
                    and old.node_id == access.node_id
                    and old.node_epoch == access.node_epoch
                    and access.conn_gen < old.conn_gen
                ):
                    entries[pool.pool_id] = old
                else:
                    entries[pool.pool_id] = old if old == access else access
        self._peers = entries
        for flight in tuple(self._flights):
            if entries.get(flight.location.pool_id) is flight.peer:
                continue
            for request in tuple(flight.waiters):
                if not request.decided:
                    request.reason = request._decision.setdefault(
                        "reason", "generation"
                    )
                    request.ready.set()
        self._wake.set()
        ready = {}
        with self._peer_lock:
            for pool_id, access in entries.items():
                if (
                    access.generation_known
                    and access.pool_registration > 0
                    and access.metadata
                    and self._load(access.agent_name, access.metadata)
                ):
                    ready[pool_id] = access
            if self._peers is entries:
                self._ready_peers = ready
        self._wake.set()

    def _valid(self, flight):
        return (
            not self.stopped.is_set()
            and not self.closed
            and flight.generation == self.generation
            and flight.current()
            and self._peers.get(flight.location.pool_id) is flight.peer
            and self._ready_peers.get(flight.location.pool_id) is flight.peer
        )

    def start(
        self,
        outcome,
        generation,
        unpin,
        *,
        deadline=None,
        current=lambda: True,
        batch=None,
    ):
        request = ReadRequest(
            self,
            outcome,
            generation,
            unpin,
            deadline
            if deadline is not None
            else time.monotonic() + self.config.rdma_timeout_s,
        )
        if not self._acquire_gate(request.deadline):
            request.reason = "gate_busy"
            request.ready.set()
            return request
        log_kept_limit = False
        try:
            loc = outcome.location
            peer = self._ready_peers.get(loc.pool_id) if loc else None
            reason = ""
            if (
                self.closed
                or self.stopped.is_set()
                or generation != self.generation
                or not current()
            ):
                reason = "generation"
            elif (
                not outcome.pin_id
                or not peer
                or self._peers.get(loc.pool_id) is not peer
            ):
                reason = "no_peer"
            elif outcome.pool_registration != peer.pool_registration:
                reason = "stale_reg"
            elif (
                loc.kind != peer.kind
                or loc.offset < 0
                or loc.length <= 0
                or loc.offset + loc.length > peer.capacity
                or loc.length > self.staging.slot_bytes
            ):
                reason = "bad_location"
            if reason:
                request.reason = reason
                request.ready.set()
                return request
            join_key = (loc, outcome.pool_registration, id(peer), generation)
            flight = self._joining.get(join_key)
            if (
                flight is not None
                and flight.state == "reading"
                and not flight.pending.completed
            ):
                request.flight = flight
                flight.waiters.append(request)
                return request
            kept = sum(f.state == "kept" for f in self._flights)
            if kept >= self.kept_limit or not self._free:
                request.reason = "kept_limit" if kept >= self.kept_limit else "no_slot"
                self.stats[request.reason] += 1
                log_kept_limit = (
                    request.reason == "kept_limit" and self.stats[request.reason] == 1
                )
                request.ready.set()
                return request
            flight = Flight(
                loc,
                outcome.pool_registration,
                peer,
                generation,
                self._free.popleft(),
                current,
            )
            request.flight = flight
            flight.waiters.append(request)
            self._flights.add(flight)
            self._joining[join_key] = flight
            # Ownership is visible before the transport can post.
            if batch is not None:
                batch.append(flight)
            else:
                self._post_reads([flight])
        finally:
            self.gate.release()
            if log_kept_limit:
                logger.error("RDMA kept slot limit reached")
        self._wake.set()
        return request

    def post_reads(self, batch):
        """Post reserved reads by peer, with ownership and checks under the gate."""
        if not batch:
            return
        deadline = min(r.deadline for flight in batch for r in flight.waiters)
        if not self._acquire_gate(deadline):
            for flight in batch:
                for request in flight.waiters:
                    request.reason = "gate_busy"
                    request.ready.set()
            return
        try:
            by_peer = {}
            for flight in batch:
                by_peer.setdefault(flight.peer.agent_name, []).append(flight)
            for flights in by_peer.values():
                self._post_reads(flights)
        finally:
            self.gate.release()
        self._wake.set()

    def _post_reads(self, flights):
        valid = []
        for flight in flights:
            if flight.state != "reserved" or flight not in self._flights:
                continue
            current = self._valid(flight)
            waiting = []
            now = time.monotonic()
            for request in flight.waiters:
                if not current:
                    request.reason = "generation"
                elif not request.reason and now >= request.deadline:
                    request.reason = "timeout"
                if request.reason or request.decided:
                    request.flight = None
                    request.ready.set()
                else:
                    waiting.append(request)
            flight.waiters = waiting
            if waiting:
                valid.append(flight)
            else:
                flight.state = "done"
                self._reclaim(flight)
        if not valid:
            return
        pending = None
        state, reason = "done", "transport_busy"
        try:
            pending = self.transport.post_nowait(
                "READ",
                valid[0].peer.agent_name,
                [
                    (
                        self.staging.addr(f.slot),
                        f.peer.base + f.location.offset,
                        f.location.length,
                    )
                    for f in valid
                ],
            )
            if pending is not None:
                state, reason = "reading", ""
        except BaseException as exc:
            pending = getattr(exc, "pending_transfer", None)
            state, reason = "kept" if pending is not None else "done", "io_error"
            if not isinstance(exc, Exception):
                raise
        finally:
            for flight in valid:
                flight.pending, flight.state = pending, state
                for request in flight.waiters:
                    if not self._valid(flight):
                        request.reason = "generation"
                    elif reason:
                        request.reason = reason
                    if request.reason:
                        request.ready.set()

    def progress(self):
        if self.stopped.is_set() or not (
            self._flights or self._releases or self._write_pending
        ):
            return False
        if not self.gate.acquire(False):
            return True
        active = False
        try:
            polled = set()
            for _ in range(len(self._releases)):
                self._release_read(self._releases.popleft())
            for flight in list(self._flights):
                if flight.consumers:
                    continue
                pending = flight.pending
                if (
                    pending is not None
                    and not pending.finished
                    and id(pending) not in polled
                ):
                    self.transport.poll_nowait(pending)
                    polled.add(id(pending))
                if pending is not None and pending.finished:
                    flight.state = "done" if pending.completed else "kept"
                if flight.state != "reading":
                    self._remove_join(flight)
                for request in flight.waiters:
                    if request.decided:
                        continue
                    if not self._valid(flight):
                        request.reason = "generation"
                    elif time.monotonic() >= request.deadline:
                        request.reason = "timeout"
                    elif flight.state == "kept":
                        request.reason = "io_error"
                    if request.reason or flight.state == "done":
                        request.ready.set()
                self._reclaim(flight)
                active = active or (pending is not None and not pending.finished)
            for pending in list(self._write_pending):
                if self.transport.poll_nowait(pending):
                    self._write_pending.remove(pending)
            return active or bool(self._write_pending)
        finally:
            self.gate.release()

    def _remove_join(self, flight):
        key = (flight.location, flight.registration, id(flight.peer), flight.generation)
        if self._joining.get(key) is flight:
            self._joining.pop(key)

    def _reclaim(self, flight):
        if (
            self.stopped.is_set()
            or flight.consumers
            or any(not r.decided for r in flight.waiters)
        ):
            return
        if flight.pending is not None and (
            not flight.pending.finished or not flight.pending.completed
        ):
            flight.state = "kept"
            return
        self._remove_join(flight)
        self._flights.discard(flight)
        self._free.append(flight.slot)

    def finish(self, request):
        if request.decided:
            return None
        result = None
        if not self._acquire_gate(request.deadline):
            request.reason = request._decision.setdefault("reason", "gate_busy")
            request.decided = True
            self._wake.set()
        else:
            try:
                flight = request.flight
                if self.stopped.is_set():
                    return None
                if not request.reason and time.monotonic() >= request.deadline:
                    request.reason = "timeout"
                if not request.reason and (flight is None or not self._valid(flight)):
                    request.reason = "generation"
                if not request.reason and (
                    flight.pending is None
                    or not flight.pending.finished
                    or not flight.pending.completed
                ):
                    request.reason = "timeout"
                if not request.reason:
                    loc = flight.location
                    result = RemoteRead(
                        view=self.staging.view(flight.slot)[: loc.length],
                        location=StorageLocation(
                            loc.pool_id,
                            loc.allocation_id,
                            loc.generation,
                            loc.offset,
                            loc.length,
                            loc.kind.value,
                        ),
                        _release=lambda: None,
                        owner=self,
                        flight=flight,
                    )
                    flight.consumers.add(result)
                # One setdefault orders peer invalidation and the HIT decision.
                request.reason = request._decision.setdefault("reason", request.reason)
                request.decided = True
                if self.stopped.is_set():
                    return None
                if result is not None:
                    if request.reason:
                        result.view.release()
                        flight.consumers.discard(result)
                        result = None
                    else:
                        self._remove_join(flight)
                if flight is not None:
                    self._reclaim(flight)
            finally:
                self.gate.release()
        if not self.stopped.is_set() and request.outcome.pin_id:
            request.unpin([request.outcome.pin_id])
        return None if self.stopped.is_set() else result

    def release(self, read):
        if self.stopped.is_set() or read._released:
            return
        if not self.gate.acquire(False):
            self._releases.append(read)
            self._wake.set()
            return
        try:
            self._release_read(read)
        finally:
            self.gate.release()

    def _release_read(self, read):
        if self.stopped.is_set() or read._released:
            return
        read._released = True
        read.view.release()
        read.flight.consumers.discard(read)
        self._reclaim(read.flight)

    def bind_credits(self, credits, agent, node_id, node_epoch):
        agent = copy.deepcopy(agent)
        domain = agent.domain
        old = self._domain_agents.get(domain)
        if old is not None and old != agent:
            return False
        if self._agent_domains.get(agent.agent_name, domain) != domain:
            return False
        if (
            domain in self._retired_domains
            or domain.node_id != node_id
            or domain.node_epoch != node_epoch
            or domain.instance != self.config.instance_id
            or domain.number <= 0
        ):
            return False
        with self._peer_lock:
            if not self._load(agent.agent_name, agent.agent_metadata):
                return False
        pools = {p.pool_id: p for p in agent.pools}
        with self.gate:
            if self.stopped.is_set() or self.closed:
                return False
            for credit in credits:
                pool = pools.get(credit.pool_id)
                if (
                    pool is None
                    or pool.pool_registration <= 0
                    or credit.offset < 0
                    or credit.length <= 0
                    or credit.offset + credit.length > pool.capacity
                    or credit.length > self.staging.slot_bytes
                    or pool.kind != credit.kind
                ):
                    return False
            if any(
                c in self._credit_agents and self._credit_agents[c].domain != domain
                for c in credits
            ):
                return False
            self._domain_agents[domain] = agent
            self._agent_domains[agent.agent_name] = domain
            self._credit_agents.update((credit, agent) for credit in credits)
        return True

    def owns_credit(self, credit):
        return credit in self._credit_agents

    def forget_credits(self, credits):
        with self.gate:
            for credit in credits:
                self._credit_agents.pop(credit, None)

    def discard_retired_credit(self, credit):
        """Called by the exclusive owner of an unused credit."""
        agent = self._credit_agents.get(credit)
        if agent is None or agent.domain not in self._retired_domains:
            return False
        self._credit_agents.pop(credit, None)
        return True

    def alloc_write(self, credit, size):
        if not self._acquire_gate():
            return None
        try:
            agent = self._credit_agents.get(credit)
            reserve = int(self.slot_count * self.config.rdma_load_reserve)
            if (
                self.stopped.is_set()
                or self.closed
                or agent is None
                or agent.domain in self._retired_domains
                or len(self._free) <= reserve
            ):
                return None
            slot = self._free.popleft()
            buffer = WriteBuffer(
                self.staging.view(slot)[:size], slot, credit, agent, size
            )
            self._writing.add(buffer)
            return buffer
        finally:
            self.gate.release()

    def release_write(self, buffer):
        with self.gate:
            if self.stopped.is_set() or buffer.released:
                return
            buffer.released = True
            buffer.view.release()
            self._writing.discard(buffer)
            self._free.append(buffer.slot)

    def write(self, buffer):
        """Called after D2H completion; UNKNOWN pages require domain fencing."""
        pending = None
        with self.gate:
            if self.stopped.is_set() or buffer.released:
                return TransferEnd.NOT_POSTED
            self._credit_agents.pop(buffer.location, None)
            if self.closed or buffer.agent.domain in self._retired_domains:
                return TransferEnd.NOT_POSTED
            pool = next(
                p for p in buffer.agent.pools if p.pool_id == buffer.location.pool_id
            )
            try:
                pending = self.transport.post_nowait(
                    "WRITE",
                    buffer.agent.agent_name,
                    [
                        (
                            self.staging.addr(buffer.slot),
                            pool.base + buffer.location.offset,
                            buffer.size,
                        )
                    ],
                )
            except Exception as exc:
                pending = getattr(exc, "pending_transfer", None)
                if pending is None:
                    self.release_write_unlocked(buffer)
                    return TransferEnd.NOT_POSTED
            if pending is None:
                self.release_write_unlocked(buffer)
                return TransferEnd.NOT_POSTED
        deadline = time.monotonic() + self.config.rdma_timeout_s
        while (
            not self.stopped.is_set()
            and not pending.finished
            and time.monotonic() < deadline
        ):
            self.transport.poll_nowait(pending)
            if not pending.finished:
                time.sleep(0.0002)
        with self.gate:
            if self.stopped.is_set():
                self._write_pending.append(pending)
                return TransferEnd.UNKNOWN
            if not pending.finished:
                self._write_pending.append(pending)
                self._wake.set()
            end = TransferEnd.DONE if pending.completed else TransferEnd.UNKNOWN
            if end == TransferEnd.UNKNOWN:
                self._retired_domains.add(buffer.agent.domain)
            self.release_write_unlocked(buffer)
            return end

    def release_write_unlocked(self, buffer):
        if not buffer.released and not self.stopped.is_set():
            buffer.released = True
            buffer.view.release()
            self._writing.discard(buffer)
            self._free.append(buffer.slot)

    def close(self):
        self.advance(drop_credits=not self.stopped.is_set())
        with self.gate:
            self.closed = True
            keep = bool(
                self.stopped.is_set()
                or self._flights
                or self._writing
                or self._write_pending
            )
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=1)
        if not keep:
            try:
                self.transport.close()
                self.staging.close()
                return True
            except Exception:
                logger.exception("RDMA staging cleanup failed")
        _KEPT_UNTIL_EXIT.append(self)
        return False

    def _run(self):
        while not self.closed:
            active = self.progress()
            self._wake.wait(0.0002 if active else None)
            self._wake.clear()
