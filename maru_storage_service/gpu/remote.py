# SPDX-License-Identifier: Apache-2.0
"""One GPU run's memory-node control stream and worker credit ledger."""

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from maru_common.ms_rpc import PLACEHOLDER_OP, OpOrigin
from maru_common.node_protocol import (
    AbortResponse,
    CreditResponse,
    CreditReturnResponse,
    HintResponse,
    Peers,
    ReadJoinResponse,
    WorkerStaging,
    WriteDoneResponse,
)
from maru_common.remote_protocol import (
    REMOTE_WIRE_VERSION,
    AgentMetadata,
    AgentsRequest,
    CancelRequest,
    FenceDomainRequest,
    FenceResult,
    HelloRequest,
    PingRequest,
    RemoteHintRequest,
    RemoteWriteDoneRequest,
    ReserveRequest,
    TransferEnd,
    Written,
    peer_origin,
)
from maru_common.remote_protocol import (
    RemoteMessageType as Code,
)
from maru_common.storage_types import (
    MAX_BATCH,
    MEMORY_CAPABILITY,
    HintAction,
    ItemOutcome,
    ItemResult,
    ReadPeer,
    Status,
    WriteDomain,
)
from maru_handler.hints import MAX_HINT_RANGES, MemoryHints
from maru_remote.auth import load_key

from ..op_ledger import LedgerEntry
from ..remote_client import RemoteConnection
from .workers import WorkerOps

logger = logging.getLogger(__name__)


class RemoteControl:
    def __init__(self, run, notify):
        self.run, self.notify = run, notify
        self.options = run.config.top.options
        self.staging = WorkerStaging(
            self.options["staging_size"],
            self.options["rdma_device"],
            self.options["timeout_s"],
            self.options["quarantine_max"],
        )
        self.origin = OpOrigin(peer_origin(run.config.node_id, run.node_epoch))
        self.peers = []
        self.hint_actions = []
        self._wait_retry_at = 0.0
        self.connection = None
        self.lock = threading.RLock()
        self.stopping = threading.Event()
        self.thread = None

    def open(self):
        run = self.run
        request = HelloRequest(
            REMOTE_WIRE_VERSION,
            run.config.node_id,
            run.node_epoch,
            run.server.host_boot_id,
            os.getpid(),
            run.server.server_epoch,
            "",
            b"",
            run.config.page_size,
            1,
            run.capabilities(),
        )
        conn, hello = RemoteConnection.open(
            self.options["url"],
            load_key(self.options["key_file"]),
            request,
            self.options["timeout_s"],
        )
        self.connection = conn
        if (
            hello.status != Status.OK
            or hello.server_epoch != run.server.server_epoch
            or not hello.node_id
            or not hello.node_epoch
            or MEMORY_CAPABILITY not in hello.capabilities
            or not hello.pools
            or not hello.boot
            or any(p.pool_registration <= 0 for p in hello.pools)
        ):
            conn.close()
            raise ConnectionError("memory node HELLO refused or incomplete")
        # Full agent metadata includes every registered pool.
        self.hint_actions = hello.hint_actions
        self.peers = [
            ReadPeer(
                hello.node_id,
                hello.node_epoch,
                hello.agent_name,
                hello.agent_metadata or hello.pools[0].metadata,
                hello.pools,
                hello.boot,
                1,
                True,
            )
        ]
        run.server.memory_peer = (hello.node_id, hello.node_epoch)
        conn.on_close(self.lost)
        self.thread = threading.Thread(
            target=self._ping, name="maru-remote-ping", daemon=True
        )
        self.thread.start()

    def lost(self):
        self.run.server.memory_peer = None
        self.peers = []
        self.hint_actions = []
        self.notify(Peers([]))
        if not self.stopping.is_set():
            self.run.store.end_run("memory node control connection lost")

    def call(self, code, request, *, op=False):
        if self.connection is None or self.stopping.is_set():
            raise ConnectionError("memory node control unavailable")
        if op:
            request = self.origin.stamp(request)
        try:
            timeout = self.options["timeout_s"]
            if code == Code.RESERVE and request.hint == HintAction.WRITE_WAIT:
                timeout = max(timeout, request.wait_ms / 1000 + 3)
            answer = self.connection.call(code, request, timeout)
        except (ConnectionError, TimeoutError):
            # A lost RESERVE reply never makes its pages available to a worker.
            if code == Code.RESERVE and not self.connection.closed:
                cancel = self.origin.stamp(
                    CancelRequest(
                        PLACEHOLDER_OP,
                        reserve_seq=request.op.seq,
                        domain=request.domain,
                    )
                )
                self.connection.submit(Code.CANCEL, cancel, lambda response: None)
            self.connection.close()
            raise
        if op:
            self.origin.resolved(request.op.seq)
        return answer

    def _ping(self):
        while not self.stopping.wait(min(1.0, self.options["timeout_s"])):
            try:
                with self.lock:
                    if self.call(Code.PING, PingRequest()).status != Status.OK:
                        self.connection.close()
                        return
            except (ConnectionError, TimeoutError):
                return

    def reserve_hint(self):
        if not self.options["write_hints"]:
            return None
        if (
            self.options["write_wait_ms"]
            and "write_wait" in self.hint_actions
            and time.monotonic() >= self._wait_retry_at
        ):
            return HintAction.WRITE_WAIT
        return HintAction.WRITE_HINT if "write_hint" in self.hint_actions else None

    def close(self):
        self.stopping.set()
        if self.connection is not None:
            self.connection.close()
        if self.thread is not None:
            self.thread.join(timeout=self.options["timeout_s"] + 1)


class RemoteWorkerOps(WorkerOps):
    def __init__(self, *, run, **kwargs):
        super().__init__(
            **kwargs,
            on_worker_ended=self._worker_ended,
            retain_detached=True,
            on_worker_lost=self._worker_lost,
        )
        self.remote = RemoteControl(run, self._notify)
        self._hints = MemoryHints(lambda keys: [], self._hint_send)
        self._admitted = {}
        self._agents = {}
        self._domains = {}
        self._closed_domains = set()
        self._number = 0
        # Location -> (worker incarnation, write domain); PENDING stays charged.
        self._credits = {}
        self._reserves = {}
        self._control_tasks = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="maru-remote-control"
        )

    def start_heartbeat(self):
        self.remote.open()
        response = self.remote.run.server.heartbeat(self.pools.usage())
        if response.status != Status.OK:
            raise ConnectionError("Catalog refused memory peer")
        self.on_heartbeat(response)
        super().start_heartbeat()

    def hint(self, rec, request):
        if request.action not in (
            HintAction.READ_HINT,
            HintAction.READ_HINT_END,
            HintAction.READ_PROMOTE,
        ):
            return HintResponse(Status.REJECTED, error="read hints only")
        if not request.request or len(request.ranges) > MAX_HINT_RANGES:
            return HintResponse(Status.REJECTED, error="invalid hint request")
        if request.action != HintAction.READ_HINT and request.ranges:
            return HintResponse(Status.REJECTED, error="unexpected hint ranges")
        engine = rec.ident.engine_id
        token = f"{len(engine)}:{engine}:{rec.ident.dp_index}:{request.request}"
        if request.action == HintAction.READ_HINT:
            self._hints.hint_ranges(token, [("memory", request.ranges)], append=True)
        elif request.action == HintAction.READ_HINT_END:
            self._hints.end(token)
        else:
            self._hints.promote(token, ["memory"])
        return HintResponse(Status.OK)

    def _hint_send(self, target, message):
        action = message["action"]
        remote = self.remote
        if action not in remote.hint_actions or remote.stopping.is_set():
            return False
        pools = {p.pool_id for peer in remote.peers for p in peer.pools}
        ranges = [r for r in message["ranges"] if r.pool_id in pools]
        if action == "read_hint" and not ranges:
            return False
        if remote.connection is None:
            return False
        answer = remote.connection.call(
            Code.HINT,
            RemoteHintRequest(HintAction(action), message["request"], ranges),
            remote.options["timeout_s"],
        )
        return answer.status == Status.OK

    def admit(self, ident):
        with self.remote.lock:
            if ident.incarnation in self._admitted:
                return self._admitted[ident.incarnation] == ident
            limit = self.remote.options["staging_max_bytes"]
            if (len(self._admitted) + 1) * self.remote.staging.size > limit:
                return False
            self._admitted[ident.incarnation] = ident
            return True

    def refuse_admission(self, incarnation):
        with self.remote.lock:
            if self.workers.get(incarnation) is None:
                self._admitted.pop(incarnation, None)

    def _notify(self, notice):
        for conn in self.workers.mapping().values():
            if conn is not None:
                conn.post_notify(notice)

    def read_join(self, rec, request):
        with self.remote.lock:
            old = self._agents.get(rec.incarnation)
            if old is not None and old != request.agent_name:
                return ReadJoinResponse(Status.REJECTED, error="read agent changed")
            if request.agent_name in self._agents.values() and old is None:
                return ReadJoinResponse(
                    Status.REJECTED, error="read agent already owned"
                )
            try:
                answer = self.remote.call(
                    Code.AGENTS,
                    AgentsRequest(
                        add=[AgentMetadata(request.agent_name, request.agent_metadata)]
                    ),
                )
                if answer.status == Status.OK:
                    self._agents[rec.incarnation] = request.agent_name
                return ReadJoinResponse(answer.status, error=answer.error)
            except (ConnectionError, TimeoutError):
                return ReadJoinResponse(
                    Status.REJECTED, error="memory node unavailable"
                )

    def _acked(self, rec, op):
        for key in list(self._reserves):
            if key[0] == rec.incarnation and key[1] <= op.ack:
                del self._reserves[key]

    def _domain(self, rec):
        instance = rec.ident.instance
        domain = self._domains.get(instance)
        if domain is None:
            self._number += 1
            domain = WriteDomain(
                self.remote.run.config.node_id, self.node_epoch, instance, self._number
            )
            self._domains[instance] = domain
        return domain

    def credit(self, rec, decoded):
        req = decoded.request
        if req.want_bytes <= 0 or not req.worker_session or "kind" in decoded.payload:
            return CreditResponse(Status.REJECTED, error="invalid credit request")
        entry = self._begin_sync(
            rec,
            req.op,
            decoded,
            CreditResponse,
            lambda: CreditResponse(Status.REJECTED, error="stale op"),
        )
        if not isinstance(entry, LedgerEntry):
            return entry
        answer = CreditResponse(Status.REJECTED, error="memory node unavailable")
        try:
            with self.remote.lock:
                self._acked(rec, req.op)
                if rec.exited or rec.detached or rec.incarnation not in self._agents:
                    answer = CreditResponse(Status.REJECTED, error="worker not ready")
                else:
                    rec.worker_session = req.worker_session
                    budgets = self.remote.run.config.budgets
                    held = sum(
                        owner == rec.incarnation for owner, _ in self._credits.values()
                    )
                    pages = min(
                        MAX_BATCH,
                        (req.want_bytes + self.pools.page_size - 1)
                        // self.pools.page_size,
                        budgets.credit_max_bytes // self.pools.page_size - held,
                        budgets.credit_total_max_bytes // self.pools.page_size
                        - len(self._credits),
                    )
                    if pages <= 0:
                        answer = CreditResponse(Status.OK)
                    else:
                        domain = self._domain(rec)
                        # Keep the worker op paired with the RESERVE sequence before sending.
                        hint = self.remote.reserve_hint()
                        request = self.remote.origin.stamp(
                            ReserveRequest(
                                PLACEHOLDER_OP,
                                pages,
                                hint=hint,
                                wait_ms=self.remote.options["write_wait_ms"]
                                if hint == HintAction.WRITE_WAIT
                                else 0,
                                domain=domain,
                            )
                        )
                        locations = []
                        self._reserves[rec.incarnation, req.op.seq] = (
                            request.op.seq,
                            domain,
                            locations,
                        )
                        started = time.monotonic()
                        try:
                            got = self.remote.call(Code.RESERVE, request)
                        finally:
                            if (
                                hint == HintAction.WRITE_WAIT
                                and time.monotonic() - started >= request.wait_ms / 1000
                            ):
                                self.remote._wait_retry_at = time.monotonic() + 5.0
                        self.remote.origin.resolved(request.op.seq)
                        locations.extend(got.credits)
                        if got.status == Status.OK and got.credits:
                            if (
                                got.write_agent is None
                                or got.write_agent.domain != domain
                            ):
                                self.remote.connection.close()
                                raise ConnectionError("invalid write agent")
                            self._credits.update(
                                (c, (rec.incarnation, domain)) for c in got.credits
                            )
                        answer = CreditResponse(
                            got.status,
                            error=got.error,
                            credits=got.credits,
                            write_agent=got.write_agent,
                        )
        except (ConnectionError, TimeoutError):
            pass
        self.ledger.finish(rec.incarnation, entry, answer)
        return answer

    def _cancel(self, rec, credits):
        if not credits or len(credits) > MAX_BATCH:
            return [ItemOutcome(ItemResult.REJECTED)] * len(credits)
        outcomes = []
        with self.remote.lock:
            for credit in credits:
                owner = self._credits.get(credit)
                if owner is None or owner[0] != rec.incarnation:
                    outcomes.append(ItemOutcome(ItemResult.REJECTED))
                    continue
                try:
                    got = self.remote.call(
                        Code.CANCEL,
                        CancelRequest(
                            PLACEHOLDER_OP,
                            locations=[credit],
                            domain=owner[1],
                        ),
                        op=True,
                    )
                    ok = got.status == Status.OK and got.freed == 1
                except (ConnectionError, TimeoutError):
                    ok = False
                if ok:
                    del self._credits[credit]
                outcomes.append(
                    ItemOutcome(ItemResult.OK if ok else ItemResult.REJECTED)
                )
        return outcomes

    def credit_return(self, rec, req):
        return CreditReturnResponse(Status.OK, items=self._cancel(rec, req.credits))

    def abort(self, rec, decoded):
        req = decoded.request
        entry = self._begin_sync(
            rec,
            req.op,
            decoded,
            AbortResponse,
            lambda: AbortResponse(
                Status.OK, items=[ItemOutcome(ItemResult.STALE)] * len(req.credits)
            ),
        )
        if not isinstance(entry, LedgerEntry):
            return entry
        with self.remote.lock:
            self._acked(rec, req.op)
        answer = AbortResponse(Status.OK, items=self._cancel(rec, req.credits))
        self.ledger.finish(rec.incarnation, entry, answer)
        return answer

    def write_done(self, rec, decoded, reply, waiter=None):
        # COMMIT may block. The UDS reader remains available for DETACH.
        self._control_tasks.submit(self._write_done, rec, decoded, reply)

    def _write_done(self, rec, decoded, reply):
        req = decoded.request
        entry = self._begin_sync(
            rec,
            req.op,
            decoded,
            WriteDoneResponse,
            lambda: WriteDoneResponse(Status.REJECTED, error="stale op"),
        )
        if not isinstance(entry, LedgerEntry):
            reply(entry)
            return
        outcomes = [ItemOutcome(ItemResult.REJECTED)] * len(req.items)
        try:
            with self.remote.lock:
                self._acked(rec, req.op)
                domain = req.items[0].domain if req.items else None
                valid = (
                    0 < len(req.items) <= MAX_BATCH
                    and domain is not None
                    and all(
                        i.domain == domain
                        and self._credits.get(i.credit) == (rec.incarnation, domain)
                        for i in req.items
                    )
                    and len({i.credit for i in req.items}) == len(req.items)
                )
                if valid:
                    got = self.remote.call(
                        Code.WRITE_DONE,
                        RemoteWriteDoneRequest(
                            PLACEHOLDER_OP,
                            [
                                Written(i.key, i.credit, i.logical_bytes, i.end)
                                for i in req.items
                            ],
                            domain,
                        ),
                        op=True,
                    )
                    if got.status == Status.OK and len(got.items) == len(req.items):
                        outcomes = got.items
                        for item in req.items:
                            self._credits.pop(item.credit, None)
                    if any(i.end == TransferEnd.UNKNOWN for i in req.items):
                        self._fence(domain)
        except (ConnectionError, TimeoutError):
            pass
        answer = WriteDoneResponse(Status.OK, items=outcomes)
        self.ledger.finish(rec.incarnation, entry, answer)
        reply(answer)

    def _fence(self, domain):
        if domain in self._closed_domains:
            return
        self._closed_domains.add(domain)
        answer = self.remote.call(
            Code.FENCE_DOMAIN, FenceDomainRequest(PLACEHOLDER_OP, domain), op=True
        )
        if answer.status != Status.OK or answer.result != FenceResult.SUCCESS:
            self.remote.connection.close()
            return
        for credit, (_, held_domain) in list(self._credits.items()):
            if held_domain == domain:
                del self._credits[credit]
        if self._domains.get(domain.instance) == domain:
            del self._domains[domain.instance]

    def remote_detach(self, rec):
        self._control_tasks.submit(self._end_worker, rec.incarnation, False)

    def _worker_lost(self, incarnation):
        self._control_tasks.submit(self._end_worker, incarnation, False)

    def _worker_ended(self, incarnation):
        self._control_tasks.submit(self._end_worker, incarnation, True)

    def _end_worker(self, incarnation, exited):
        with self.remote.lock:
            ident = self._admitted.get(incarnation)
            if ident is None:
                return
            try:
                domain = self._domains.get(ident.instance)
                if domain is not None:
                    self._fence(domain)
                name = self._agents.pop(incarnation, None)
                if name is not None:
                    self.remote.call(Code.AGENTS, AgentsRequest(remove=[name]))
            except (ConnectionError, TimeoutError):
                pass
            if exited:
                self._admitted.pop(incarnation, None)
                for key in [key for key in self._reserves if key[0] == incarnation]:
                    del self._reserves[key]

    def stats(self):
        result = super().stats()
        if self.orphans is not None:
            result.update(
                orphan_unwatchable=len(self.orphans.unwatchable),
                orphan_unknown=len(self.orphans.unknown),
                orphan_watched=len(self.orphans.pending),
            )
        result["pending_exits"] = self.pending_exits()
        return result

    def stop(self):
        self.remote.close()
        self._hints.close(timeout_s=1, end_open=False)
        self._control_tasks.shutdown(wait=True)
        super().stop()
