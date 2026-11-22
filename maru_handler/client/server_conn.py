# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""ServerConn: the worker session with MaruServer.

Reopening with the same incarnation keeps pins unless the server epoch
changed. A lookup or probe never waits for a reopen and is never resent: on
EPOCH it misses, and the heartbeat thread reopens the session. A lookup with
an unknown result is resent with the same op in the background and its pins
released. While MaruServer is down, or the session is to reopen or was
refused, lookups and probes answer at once without an RPC.
"""

import dataclasses
import logging
import threading
import time

from maru_common.ms_rpc import PLACEHOLDER_OP, MsRpcClient, OpOrigin, RpcUnknownError
from maru_common.protocol import (
    LookupAndPinRequest,
    LookupAndPinResponse,
    MessageType,
    ProbeRequest,
    ProbeResponse,
    UnpinRequest,
    WorkerCloseRequest,
    WorkerHeartbeatRequest,
    WorkerHeartbeatResponse,
    WorkerOpenRequest,
)
from maru_common.storage_types import (
    MAX_BATCH,
    MS_PROTOCOL,
    ItemOutcome,
    ItemResult,
    Key,
    SessionRole,
    Status,
    StorageUnavailableError,
)

logger = logging.getLogger(__name__)

MAX_UNRESOLVED_LOOKUPS = 32
# How long a worker waits quietly for MaruServer to know its node run (until
# a WORKER_OPEN told the session TTL)
NODE_RUN_WARN_S = 30.0
# Heartbeat interval while MaruServer does not answer
DOWN_POLL_S = 0.5
# The heartbeat logs an unexpected error of one type this often at most
LOOP_ERROR_LOG_S = 30.0


@dataclasses.dataclass(frozen=True)
class WorkerIdentity:
    engine_id: str
    dp_index: int
    tp_rank: int
    incarnation: str
    namespace: str


class NodeRunUnknownError(StorageUnavailableError):
    """A new MaruServer does not know the node run this worker is attached
    to yet; the session opens once the node opens its run there."""


class ServerConn:
    def __init__(
        self,
        url: str,
        identity: WorkerIdentity,
        *,
        node_id: str,
        node_epoch: str,
        pid: int,
        pid_start: int,
        timeout_s: float,
        lookup_timeout_s: float = 0.1,
        role: SessionRole = SessionRole.WORKER,
    ):
        self.url = url
        self.identity = identity
        # A probe session (a scheduler) may only PROBE
        self.role = role
        self.node_id = node_id
        self.node_epoch = node_epoch
        self.pid = pid
        self.pid_start = pid_start
        self._rpc = MsRpcClient(url, timeout_s=timeout_s, name=f"maru-{role.value}-rpc")
        self._lookup_timeout_s = lookup_timeout_s
        self._lock = threading.RLock()
        # One WORKER_OPEN at a time, from request to applied response; the
        # stale-node callback may reopen from inside one (same thread)
        self._open_lock = threading.RLock()
        self.session: str | None = None
        self.server_epoch: str | None = None
        # The node run the open session is attached to
        self.session_node_epoch: str | None = None
        self.heartbeat_s = 0.0
        self.session_ttl_s = 0.0
        self._origin: OpOrigin | None = None
        # Lookups whose result is unknown: op seq -> request
        self._unresolved: dict[int, LookupAndPinRequest] = {}
        # Pins to unpin that MaruServer has not confirmed yet
        self._unpins: list[str] = []
        self._frozen = threading.Event()
        self._stop = threading.Event()
        self._beat = threading.Event()  # wakes the heartbeat loop
        self._reopen = False
        # The last WORKER_OPEN was refused: lookups miss without an RPC until
        # the heartbeat reopens the session
        self._refused = False
        # Maru is off on this worker: the session is never opened again
        self._no_reopen = False
        self._opens = 0  # successful WORKER_OPENs
        self._thread: threading.Thread | None = None
        self._resolver: threading.Thread | None = None
        self._wake = threading.Event()
        # MaruServer left an RPC unanswered: lookups and probes report it
        # unavailable at once until a heartbeat is answered again
        self._down = False
        self.on_server_epoch: list = []
        # Called with the node epoch MaruServer no longer accepts
        self.on_stale_node: list = []
        # Called once MaruServer answered UNPINs
        self.on_unpinned: list = []
        self.late_hits = 0

    # ---------------------------------------------------------------- session

    def open(self, if_session: str | None = None) -> None:
        """WORKER_OPEN; StorageUnavailableError if unreachable or refused
        (NodeRunUnknownError while a new MaruServer does not know the node
        run yet). Opens run one at a time, so a late answer never takes the server
        epoch back. With ``if_session``, only if that is still the session
        once it is this open's turn."""
        with self._open_lock:
            if if_session is not None and self.session != if_session:
                return
            self._open()

    def _open(self) -> None:
        if self._frozen.is_set():
            raise StorageUnavailableError("worker stopped")
        self.prepare_read_thread()
        if self._no_reopen:
            raise StorageUnavailableError("the session is not opened again")
        request = WorkerOpenRequest(
            node_id=self.node_id,
            node_epoch=self.node_epoch,
            engine_id=self.identity.engine_id,
            dp_index=self.identity.dp_index,
            tp_rank=self.identity.tp_rank,
            pid=self.pid,
            pid_start=self.pid_start,
            incarnation=self.identity.incarnation,
            namespace=self.identity.namespace,
            role=self.role,
            protocol=MS_PROTOCOL,
            capabilities=[],
        )
        try:
            response = self._rpc.call(MessageType.WORKER_OPEN, request)
        except RpcUnknownError as e:
            self._set_down(True, e)
            raise StorageUnavailableError(f"WORKER_OPEN: {e}") from e
        self._set_down(False)
        if response.status == Status.EPOCH:
            # Refused for this node epoch: the stale-node callback may reopen
            # on the run the UDS is attached to
            with self._lock:
                self._reopen = True
                generation = self._opens
            for callback in self.on_stale_node:
                callback(request.node_epoch)
            with self._lock:
                if self._opens != generation:
                    return  # a nested open succeeded
        if response.status != Status.OK:
            with self._lock:
                self._refused = True
                # A MaruServer other than ours, and our node run is still the
                # one attached: the node has not opened its run there yet
                unknown_run = (
                    response.status == Status.EPOCH
                    and bool(response.server_epoch)
                    and response.server_epoch != self.server_epoch
                    and self.node_epoch == request.node_epoch
                )
            self._poll_soon()
            error = NodeRunUnknownError if unknown_run else StorageUnavailableError
            raise error(f"WORKER_OPEN refused: {response.status} {response.error}")
        with self._lock:
            self._reopen = self._refused = False
            self._opens += 1
            old_epoch = self.server_epoch
            self.session = response.worker_session
            self.server_epoch = response.server_epoch
            self.session_node_epoch = request.node_epoch
            self.heartbeat_s = response.heartbeat_s
            self.session_ttl_s = response.session_ttl_s
            if old_epoch != response.server_epoch:
                # A new MaruServer has no record of earlier ops
                self._origin = OpOrigin(self.identity.incarnation)
                self._unresolved.clear()
                self._unpins.clear()
        if old_epoch is not None and old_epoch != response.server_epoch:
            logger.warning(
                "MaruServer restarted (epoch %s -> %s); pins of the old epoch "
                "are gone with it",
                old_epoch,
                response.server_epoch,
            )
            for callback in self.on_server_epoch:
                callback(old_epoch, response.server_epoch)

    def move_to(self, node_epoch: str, pid: int, pid_start: int) -> None:
        """Reopen under a new node epoch (pins move); retried until it works."""
        with self._lock:
            self.node_epoch, self.pid, self.pid_start = node_epoch, pid, pid_start
            self._reopen = True
        try:
            self.open()
        except NodeRunUnknownError as e:
            logger.info("the node run is not open at MaruServer yet: %s", e)
        except StorageUnavailableError as e:
            logger.warning("reopening the worker session: %s", e)

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._loop, name="maru-worker-heartbeat", daemon=True
        )
        self._resolver = threading.Thread(
            target=self._resolve_loop, name="maru-worker-resolve", daemon=True
        )
        self._thread.start()
        self._resolver.start()

    def stop_reopening(self) -> None:
        """Never open the session again (Maru is off on this worker);
        heartbeats and UNPINs go on while it lasts."""
        with self._lock:
            self._no_reopen = True
        self._beat.set()

    def freeze(self):
        """Keep pins and stop background ownership changes after CUDA failure."""
        self._frozen.set()
        self._no_reopen = True
        self._stop.set()
        self._wake.set()
        self._beat.set()

    def close(self, goodbye: bool = True) -> bool:
        """Flush UNPINs and stop; with ``goodbye`` also WORKER_CLOSE (only after
        every GPU copy ended). Neither waits on a MaruServer that is down.

        Returns:
            Whether MaruServer holds nothing of the session: WORKER_CLOSE was
            answered OK, or no session was opened.
        """
        if self._frozen.is_set():
            self._rpc.close()
            return False
        self._stop.set()
        self._wake.set()
        self._beat.set()
        for thread in (self._thread, self._resolver):
            if thread is not None and thread is not threading.current_thread():
                thread.join(timeout=5.0)
        session, epoch = self.session, self.server_epoch
        if self._down:
            self._rpc.close()
            return session is None
        if session is not None:
            self._flush_unpins()
        ended = session is None
        if goodbye and session is not None and epoch is not None:
            try:
                response = self._rpc.call(
                    MessageType.WORKER_CLOSE,
                    WorkerCloseRequest(session=session, server_epoch=epoch),
                )
                ended = response.status == Status.OK
            except RpcUnknownError as e:
                logger.warning("WORKER_CLOSE unanswered: %s", e)
        self._rpc.close()
        return ended

    def _loop(self) -> None:
        """Heartbeat only; lookups and UNPINs are resolved on their own thread.
        Polls faster while MaruServer is down or the session is to reopen."""
        failing = False
        unknown_since: float | None = None  # MaruServer has not known the run since
        warned = False
        last_logged: dict[type, float] = {}
        while True:
            self._beat.wait(
                DOWN_POLL_S if self._down or self._reopen else self.heartbeat_s or 1.0
            )
            self._beat.clear()
            if self._stop.is_set():
                return
            if self._no_reopen and self._reopen:
                return  # the session is gone and is not opened again
            try:
                if self._reopen:
                    self.open()
                failing = False
                unknown_since, warned = None, False
                session, epoch = self._ids()
                try:
                    response = self._rpc.call(
                        MessageType.WORKER_HEARTBEAT,
                        WorkerHeartbeatRequest(session=session, server_epoch=epoch),
                    )
                except RpcUnknownError as e:
                    self._set_down(True, e)
                    continue
                self._set_down(False)
                if response.status == Status.EPOCH:
                    with self._open_lock:  # the check and the open are one step
                        with self._lock:
                            moved = self.session != session
                            if not moved:
                                self._reopen = True
                        if not moved:
                            self.open()  # expired, or MaruServer restarted
            except (RpcUnknownError, StorageUnavailableError) as e:
                if isinstance(e, NodeRunUnknownError):
                    now = time.monotonic()
                    if unknown_since is None:
                        unknown_since = now
                        logger.info("waiting for the node run at MaruServer: %s", e)
                    elif not warned and now - unknown_since > (
                        self.session_ttl_s or NODE_RUN_WARN_S
                    ):
                        warned = True
                        logger.warning(
                            "the node run has not opened at MaruServer for %.0f s; "
                            "lookups miss until it does",
                            now - unknown_since,
                        )
                elif not self._down and not failing:
                    logger.warning("worker heartbeat: %s", e)
                failing = True
            except Exception as e:
                # A callback failed: the thread goes on, or lookups would miss
                # for good while the session is to reopen
                now = time.monotonic()
                kind = type(e)
                if now - last_logged.get(kind, -LOOP_ERROR_LOG_S) >= LOOP_ERROR_LOG_S:
                    last_logged[kind] = now
                    logger.exception("worker heartbeat failed")

    def heartbeat_once(self) -> bool:
        """One WORKER_HEARTBEAT; whether it was answered OK."""
        try:
            session, epoch = self._ids()
            response: WorkerHeartbeatResponse = self._rpc.call(
                MessageType.WORKER_HEARTBEAT,
                WorkerHeartbeatRequest(session=session, server_epoch=epoch),
            )
        except RpcUnknownError as e:
            self._set_down(True, e)
            return False
        except StorageUnavailableError:
            return False
        self._set_down(False)
        return response.status == Status.OK

    @property
    def down(self) -> bool:
        """Whether MaruServer left the last RPC unanswered."""
        return self._down

    def _poll_soon(self) -> None:
        """Cut the heartbeat's long wait short; the loop itself waits briefly
        anyway while down or reopening."""
        if threading.current_thread() is not self._thread:
            self._beat.set()

    def _set_down(self, down: bool, error: Exception | None = None) -> None:
        with self._lock:
            changed, self._down = self._down != down, down
        if changed and down:
            logger.warning(
                "MaruServer does not answer (%s); lookups miss until it does", error
            )
            self._poll_soon()
        elif changed:
            logger.info("MaruServer answers again")
            self._wake.set()  # resolve and unpin what waited

    def _ids(self) -> tuple[str, str]:
        """(session, server_epoch) of the open session."""
        with self._lock:
            session, epoch = self.session, self.server_epoch
        if session is None or epoch is None:
            raise StorageUnavailableError("the worker session is not open")
        return session, epoch

    def _resolve_loop(self) -> None:
        while not self._stop.is_set():
            self._wake.wait(self.heartbeat_s or 1.0)
            self._wake.clear()
            if self._stop.is_set():
                return
            if self._down:
                continue  # the heartbeat finds out when to go on
            try:
                self._resolve_lookups()
                self._flush_unpins()
            except Exception:
                logger.exception("resolving lookups failed")

    def _reopen_after_epoch(self, epoch: str, session: str) -> bool:
        """Reopen after EPOCH to a background request of ``epoch`` on
        ``session``, unless the session moved on already; whether the session
        is at that server epoch still."""
        self.open(if_session=session)
        with self._lock:
            return self.server_epoch == epoch

    def _reopen_soon(self) -> None:
        """EPOCH to a lookup or probe: the heartbeat thread reopens the
        session; until then they miss without an RPC."""
        with self._lock:
            self._reopen = True
        self._poll_soon()

    # ----------------------------------------------------------------- lookup

    def prepare_read_thread(self) -> None:
        """Connect the calling thread's PROBE and LOOKUP_AND_PIN socket."""
        try:
            self._rpc.prepare_thread()
        except RpcUnknownError as e:
            raise StorageUnavailableError(f"read socket: {e}") from e

    def lookup_and_pin(
        self, keys: list[Key]
    ) -> tuple[list[ItemOutcome], str, str | None] | None:
        """One LOOKUP_AND_PIN: the outcomes, the server epoch whose pins they
        hold and the node run they were answered under; None when unknown or
        unavailable. One RPC at most: never resent, never waits for a reopen.
        """
        if not 0 < len(keys) <= MAX_BATCH:
            raise ValueError(f"{len(keys)} keys; a lookup carries 1..{MAX_BATCH}")
        with self._lock:
            if self._down or self._refused or self._reopen:
                return None
            if len(self._unresolved) >= MAX_UNRESOLVED_LOOKUPS:
                # MaruServer is not answering: no new ops until these resolve
                return None
            origin = self._origin
            if origin is None or self.session is None or self.server_epoch is None:
                return None  # not open
            node_epoch = self.session_node_epoch
            request = origin.stamp(
                LookupAndPinRequest(
                    session=self.session,
                    server_epoch=self.server_epoch,
                    op=PLACEHOLDER_OP,
                    items=list(keys),
                )
            )
        return self._send_lookup(origin, request, node_epoch, background=False)

    def _send_lookup(
        self,
        origin: OpOrigin,
        request: LookupAndPinRequest,
        node_epoch: str | None,
        background: bool,
    ) -> tuple[list[ItemOutcome], str, str | None] | None:
        """Send a lookup; only a ``background`` resend reopens the session on
        EPOCH and sends it again."""
        seq = request.op.seq
        for _ in range(2):
            if self._frozen.is_set():
                return None
            try:
                response: LookupAndPinResponse = self._rpc.call(
                    MessageType.LOOKUP_AND_PIN,
                    request,
                    timeout_s=None if background else self._lookup_timeout_s,
                )
            except RpcUnknownError as e:
                logger.warning("LOOKUP_AND_PIN op %d: result unknown (%s)", seq, e)
                with self._lock:
                    if origin is self._origin:
                        self._unresolved[seq] = request
                self._set_down(True, e)
                return None
            if response.status == Status.EPOCH and not background:
                # Not run, so it holds no pins
                origin.resolved(seq)
                self._reopen_soon()
                return None
            if response.status == Status.EPOCH:
                try:
                    self._reopen_after_epoch(request.server_epoch, request.session)
                except StorageUnavailableError as e:
                    if isinstance(e, NodeRunUnknownError):
                        logger.debug("reopening the worker session: %s", e)
                    else:
                        logger.warning("reopening the worker session: %s", e)
                    with self._lock:
                        if origin is self._origin:
                            self._unresolved[seq] = request
                    return None
                with self._lock:
                    # Only to the MaruServer and ledger that stamped it
                    same = (
                        origin is self._origin
                        and self.server_epoch == request.server_epoch
                    )
                    session, node_epoch = self.session, self.session_node_epoch
                if not same or session is None:
                    return None  # the op and its pins are gone with the server
                # The digest leaves out the session
                request = dataclasses.replace(request, session=session)
                continue
            if response.status == Status.BUSY:
                with self._lock:
                    if origin is self._origin:
                        self._unresolved[seq] = request
                return None
            with self._lock:
                if origin is self._origin:  # not a newer ledger's seq
                    self._unresolved.pop(seq, None)
            origin.resolved(seq)
            if response.status != Status.OK or len(response.items) != len(
                request.items
            ):
                logger.error(
                    "LOOKUP_AND_PIN op %d: %s %s (%d results for %d keys)",
                    seq,
                    response.status,
                    response.error,
                    len(response.items),
                    len(request.items),
                )
                if response.status == Status.OK:
                    self.unpin(
                        [o.pin_id for o in response.items if o.pin_id is not None]
                    )
                return None
            return response.items, request.server_epoch, node_epoch
        # EPOCH again after reopening: not run; resent in the background
        with self._lock:
            if origin is self._origin:
                self._unresolved[seq] = request
        return None

    def _resolve_lookups(self) -> None:
        """Resend unresolved lookups with the same op, one at a time while
        answers come back, and unpin their HITs;
        nobody reads them (not counted as hits)."""
        while not self._stop.is_set():
            with self._lock:
                origin = self._origin
                if not self._unresolved or origin is None:
                    return
                session, epoch = self._ids()
                node_epoch = self.session_node_epoch
                request = origin.refresh(
                    dataclasses.replace(
                        next(iter(self._unresolved.values())),
                        session=session,
                        server_epoch=epoch,
                    )
                )
            answered = self._send_lookup(origin, request, node_epoch, background=True)
            if answered is None:
                return  # still no answer; try again next beat
            pins = [
                o.pin_id
                for o in answered[0]
                if o.result == ItemResult.HIT and o.pin_id is not None
            ]
            self.late_hits += len(pins)
            if pins:
                self.unpin(pins)

    def unresolved_lookups(self) -> int:
        with self._lock:
            return len(self._unresolved)

    def probe(self, keys: list[Key]) -> list[ItemOutcome] | None:
        """PROBE (no pins); None when unavailable. One RPC at most: never
        resent, never waits for a reopen."""
        if not 0 < len(keys) <= MAX_BATCH:
            raise ValueError(f"{len(keys)} keys; a probe carries 1..{MAX_BATCH}")
        if self._down or self._refused or self._reopen:
            return None
        try:
            session, epoch = self._ids()
            response: ProbeResponse = self._rpc.call(
                MessageType.PROBE,
                ProbeRequest(session=session, server_epoch=epoch, items=list(keys)),
                timeout_s=self._lookup_timeout_s,
            )
        except RpcUnknownError as e:
            self._set_down(True, e)
            return None
        except StorageUnavailableError as e:
            logger.warning("PROBE: %s", e)
            return None
        if response.status == Status.EPOCH:
            self._reopen_soon()
            return None
        if response.status != Status.OK or len(response.items) != len(keys):
            logger.error("PROBE: %s %s", response.status, response.error)
            return None
        return response.items

    # ------------------------------------------------------------------ unpin

    def unpin(self, pin_ids: list[str]) -> None:
        """Queue pins for UNPIN; the resolver thread sends them at once, so
        the caller never waits on MaruServer."""
        if pin_ids and not self._frozen.is_set():
            with self._lock:
                self._unpins.extend(pin_ids)
            self._wake.set()

    def _flush_unpins(self) -> bool:
        if self._frozen.is_set():
            return False
        with self._lock:
            pins = list(self._unpins)
            session, epoch = self.session, self.server_epoch
        if not pins:
            return True
        if session is None or epoch is None:
            return False  # not open: sent once it is
        for i in range(0, len(pins), MAX_BATCH):
            if self._frozen.is_set():
                return False
            batch = pins[i : i + MAX_BATCH]
            try:
                response = self._rpc.call(
                    MessageType.UNPIN,
                    UnpinRequest(session=session, server_epoch=epoch, pin_ids=batch),
                )
            except RpcUnknownError as e:
                logger.warning("UNPIN: %s; sent again later", e)
                return False
            if response.status == Status.EPOCH:
                try:
                    same = self._reopen_after_epoch(epoch, session)
                except StorageUnavailableError:
                    return False
                if not same:
                    return True  # open() dropped them: gone with the old server
                return self._flush_unpins()
            if response.status != Status.OK:
                logger.error("UNPIN refused: %s %s", response.status, response.error)
            else:
                refused = [
                    pin
                    for pin, outcome in zip(batch, response.items, strict=False)
                    if outcome.result != ItemResult.OK
                ]
                if refused:
                    # Not this session's pins: nothing to retry
                    logger.error(
                        "UNPIN refused for %d pins: %s", len(refused), refused[:4]
                    )
            with self._lock:
                done = set(batch)
                self._unpins = [p for p in self._unpins if p not in done]
            for callback in self.on_unpinned:
                callback()
        return True

    def pending_unpins(self) -> int:
        with self._lock:
            return len(self._unpins)

    def unpin_pending(self, pin_ids: set[str]) -> set[str]:
        """Those of ``pin_ids`` still queued for UNPIN (not answered yet)."""
        with self._lock:
            return pin_ids.intersection(self._unpins)
