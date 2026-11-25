# SPDX-License-Identifier: Apache-2.0
"""One GPU run's memory-node control stream and worker credit ledger."""

import logging
import os
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from maru_common.ms_rpc import PLACEHOLDER_OP, OpOrigin, RpcUnknownError
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
)
from maru_handler.hints import MAX_HINT_RANGES, MemoryHints
from maru_remote.auth import load_key

from ..op_ledger import LedgerEntry
from ..ports import ServerStopped
from ..remote_client import RemoteConnection
from .remote_workers import RemoteWorkers
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
            deadline = time.monotonic() + timeout
            answer = self.connection.call(code, request, timeout)
        except (ConnectionError, TimeoutError):
            # A lost RESERVE reply never makes its pages available to a worker.
            try:
                if (
                    code == Code.RESERVE
                    and not self.connection.closed
                    and time.monotonic() < deadline
                ):
                    cancel = self.origin.stamp(
                        CancelRequest(
                            PLACEHOLDER_OP,
                            reserve_seq=request.op.seq,
                            domain=request.domain,
                        )
                    )
                    self.connection.submit(
                        Code.CANCEL, cancel, lambda response: None, deadline=deadline
                    )
            finally:
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
        self.remote = RemoteControl(run, self._notify)
        super().__init__(
            **kwargs,
            registry_factory=partial(
                RemoteWorkers,
                staging_limit=self.remote.options["staging_max_bytes"]
                // self.remote.staging.size,
                node_id=run.config.node_id,
                on_terminal=self._queue_cleanup,
            ),
        )
        self._hints = MemoryHints(lambda keys: [], self._hint_send)
        self._agents = {}
        # Location -> (worker incarnation, write domain); PENDING stays charged.
        self._credits = {}
        self._reserves = {}
        self._control_tasks = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="maru-remote-control"
        )

    def start_heartbeat(self):
        self.remote.open()
        for attempt in range(2):
            try:
                response = self.remote.run.server.heartbeat(self.pools.usage())
                break
            except RpcUnknownError as exc:
                if attempt:
                    raise ConnectionError(
                        "Catalog did not confirm memory peer"
                    ) from exc
        if self.remote.connection.closed:
            self.end_run("memory node control connection lost")
            raise ServerStopped(self.ended)
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

    def _notify(self, notice):
        for conn in self.workers.mapping().values():
            if conn is not None:
                conn.post_notify(notice)

    def read_join(self, rec, request):
        with self.remote.lock:
            old = self._agents.get(rec.incarnation)

            def register():
                if old not in (None, request.agent_name):
                    return False
                if old is None and request.agent_name in self._agents.values():
                    return False
                self._agents[rec.incarnation] = request.agent_name
                return True

            if not self.workers.while_attached(rec, register):
                return ReadJoinResponse(Status.REJECTED, error="read agent refused")
            try:
                answer = self.remote.call(
                    Code.AGENTS,
                    AgentsRequest(
                        add=[AgentMetadata(request.agent_name, request.agent_metadata)]
                    ),
                )
                if answer.status != Status.OK and old is None:
                    self._agents.pop(rec.incarnation, None)
                return ReadJoinResponse(answer.status, error=answer.error)
            except (ConnectionError, TimeoutError):
                return ReadJoinResponse(
                    Status.REJECTED, error="memory node unavailable"
                )

    def _acked(self, rec, op):
        for key in list(self._reserves):
            if key[0] == rec.incarnation and key[1] <= op.ack:
                del self._reserves[key]

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
                        answer = self._reserve(rec, req.op.seq, pages)
        except (ConnectionError, TimeoutError):
            pass
        return self._finish_op(rec, entry, answer)

    def _reserve(self, rec, seq, pages):
        domain = self.workers.reserve_domain(rec)
        if domain is None:
            return CreditResponse(Status.REJECTED, error="worker has ended")
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
        key = (rec.incarnation, seq)
        locations = []
        self._reserves[key] = (
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
            if got.write_agent is None or got.write_agent.domain != domain:
                self.remote.connection.close()
                raise ConnectionError("invalid write agent")
            self._credits.update((c, (rec.incarnation, domain)) for c in got.credits)
            if not self.workers.grant(rec, domain):
                # Only unexposed pages of a still-open domain can be cancelled.
                self._cancel(rec, got.credits)
                if not any(c in self._credits for c in got.credits):
                    self._reserves.pop(key, None)
                    if self.workers.withdraw_unissued(domain):
                        self._fence(domain)
                return CreditResponse(Status.REJECTED, error="write domain ended")
        else:
            self._reserves.pop(key, None)
            if got.domain_fenced:
                self.workers.withdraw(domain)
                self._forget_fenced(domain)
        return CreditResponse(
            got.status,
            error=got.error,
            credits=got.credits,
            write_agent=got.write_agent,
        )

    def _cancel(self, rec, credits):
        if not credits or len(credits) > MAX_BATCH:
            return [ItemOutcome(ItemResult.REJECTED)] * len(credits)
        outcomes = []
        with self.remote.lock:
            for credit in credits:
                owner = self._credits.get(credit)
                if (
                    owner is None
                    or owner[0] != rec.incarnation
                    or self.workers.domains.get(owner[1].instance) != owner[1]
                ):
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
                    self.workers.while_current(
                        owner[1], partial(self._credits.pop, credit, None)
                    )
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
        return self._finish_op(rec, entry, answer)

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
                if 0 < len(req.items) <= MAX_BATCH:
                    counts = Counter(i.credit for i in req.items)
                    groups = {}
                    for index, item in enumerate(req.items):
                        domain = item.domain
                        if (
                            counts[item.credit] == 1
                            and domain is not None
                            and self._credits.get(item.credit)
                            == (rec.incarnation, domain)
                            and self.workers.domains.get(domain.instance) == domain
                        ):
                            groups.setdefault(domain, []).append((index, item))
                    for domain, group in groups.items():
                        self._write_group(domain, group, outcomes)
        except (ConnectionError, TimeoutError):
            pass
        answer = WriteDoneResponse(Status.OK, items=outcomes)
        reply(self._finish_op(rec, entry, answer))

    def _write_group(self, domain, group, outcomes):
        got = self.remote.call(
            Code.WRITE_DONE,
            RemoteWriteDoneRequest(
                PLACEHOLDER_OP,
                [Written(i.key, i.credit, i.logical_bytes, i.end) for _, i in group],
                domain,
            ),
            op=True,
        )
        if got.status == Status.OK and len(got.items) == len(group):
            consumed = set(got.consumed)
            for (index, _), outcome in zip(group, got.items, strict=True):
                outcomes[index] = outcome

            def settle():
                for pos, (index, item) in enumerate(group):
                    if item.end == TransferEnd.DONE and (
                        pos in consumed
                        or outcomes[index].result
                        in (ItemResult.ADDED, ItemResult.DUPLICATE)
                    ):
                        self._credits.pop(item.credit, None)

            self.workers.while_current(domain, settle)
        if any(
            i.end == TransferEnd.UNKNOWN for _, i in group
        ) and self.workers.withdraw(domain):
            self._fence(domain)

    def _fence(self, domain):
        answer = self.remote.call(
            Code.FENCE_DOMAIN, FenceDomainRequest(PLACEHOLDER_OP, domain), op=True
        )
        if answer.status != Status.OK or answer.result != FenceResult.SUCCESS:
            self.remote.connection.close()
            return
        self._forget_fenced(domain)

    def _forget_fenced(self, domain):
        for credit, (_, held_domain) in list(self._credits.items()):
            if held_domain == domain:
                del self._credits[credit]
        for key, (_, held_domain, _) in list(self._reserves.items()):
            if held_domain == domain:
                del self._reserves[key]

    def _queue_cleanup(self, rec, domains):
        self._control_tasks.submit(self._end_worker, rec, domains)

    def _end_worker(self, rec, domains):
        with self.remote.lock:
            try:
                for domain in domains:
                    self._fence(domain)
                name = self._agents.pop(rec.incarnation, None)
                if name is not None:
                    self.remote.call(Code.AGENTS, AgentsRequest(remove=[name]))
            except (ConnectionError, TimeoutError):
                pass
            self._forget_released(rec)

    def _finish_op(self, rec, entry, answer):
        self.ledger.finish(rec.incarnation, entry, answer)
        if rec.staging_released:
            with self.remote.lock:
                self._forget_released(rec)
        return answer

    def _forget_released(self, rec):
        if (
            rec.staging_released
            and rec.incarnation not in self._agents
            and all(owner != rec.incarnation for owner, _ in self._credits.values())
            and all(owner != rec.incarnation for owner, _ in self._reserves)
        ):
            self.workers.forget_released(rec)

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
