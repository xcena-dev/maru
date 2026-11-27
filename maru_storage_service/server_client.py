# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""ServerClient: the node session with MaruServer.

Ops whose result is unknown are resent with the same op. On EPOCH a HANDSHAKE
tells an expired session, reopened with the same node_epoch, from a MaruServer
restart or a run MaruServer ended; after those the session is lost for good
and the run is over.
"""

import dataclasses
import logging
import threading
import time
from collections.abc import Callable

from maru_common.ms_rpc import PLACEHOLDER_OP, MsRpcClient, OpOrigin, RpcUnknownError
from maru_common.node_protocol import process_identity
from maru_common.protocol import (
    MESSAGE_CLASSES,
    MS_MESSAGE_CLASSES,
    CommitItem,
    CommitRequest,
    CommitResponse,
    HandshakeRequest,
    MessageType,
    NodeCloseRequest,
    NodeCloseResponse,
    NodeHeartbeatRequest,
    NodeHeartbeatResponse,
    NodeOpenRequest,
    NodeOpenResponse,
    NodeOrphansRequest,
    NodeRecoverListRequest,
    RecoverItem,
    RetireRequest,
    RetireResponse,
    WorkerExitRequest,
    WorkerExitResponse,
)
from maru_common.serializer import MessageDecodeError
from maru_common.storage_types import (
    EPOCH_SEQ_MAX,
    MAX_BATCH,
    MS_PROTOCOL,
    CloseMode,
    ExitCause,
    KeyLocation,
    PoolInfo,
    PoolUsage,
    RmDomain,
    Status,
    WorkerRef,
)

from .ports import EpochChanged, NodeOpenError, ReplyError, ServerStopped

logger = logging.getLogger(__name__)

BOOT_ID_PATH = "/proc/sys/kernel/random/boot_id"
RESEND_PAUSE_S = 0.05
RESEND_PAUSE_MAX_S = 1.0


def read_host_boot_id(path: str = BOOT_ID_PATH) -> str:
    with open(path) as f:
        return f.read().strip()


class ServerClient:
    """Implements MetadataPort."""

    def __init__(
        self,
        url: str,
        node_id: str,
        node_epoch: str,
        *,
        rpc_timeout_s: float = 2.0,
        host_boot_id: str | None = None,
    ):
        self.url = url
        self.node_id = node_id
        self.node_epoch = node_epoch
        self.host_boot_id = host_boot_id or read_host_boot_id()
        # The legacy HANDSHAKE tells an expired session from a restart
        self._rpc = MsRpcClient(
            url,
            timeout_s=rpc_timeout_s,
            message_classes=MESSAGE_CLASSES,
            strict_codes=MS_MESSAGE_CLASSES,
            name="maru-node-rpc",
        )
        self._origin = OpOrigin(node_epoch)
        self._stop = threading.Event()
        self._epoch_lost = threading.Event()
        self.session: str | None = None
        self.server_epoch: str | None = None
        self.epoch_seq: int | None = None
        self.memory_peer: tuple[str, str] | None = None
        self.heartbeat_s = 0.0
        self.session_ttl_s = 0.0
        self._heartbeat: threading.Thread | None = None
        self._on_epoch: Callable[[str], None] | None = None
        self._open_request: NodeOpenRequest | None = None
        self._renew_lock = threading.Lock()
        self.resends = 0
        self.renewals = 0
        self.last_heartbeat_ok = 0.0  # monotonic time of the last OK heartbeat
        # Why the session was lost
        self.lost_reason: str | None = None

    # ---------------------------------------------------------------- session

    def open(
        self,
        pools: list[PoolInfo],
        rm_domains: list[RmDomain] | None = None,
        capabilities: list[str] | None = None,
    ) -> NodeOpenResponse:
        """NODE_OPEN until answered; NodeOpenError if refused."""
        request = NodeOpenRequest(
            node_id=self.node_id,
            node_epoch=self.node_epoch,
            host_boot_id=self.host_boot_id,
            protocol=MS_PROTOCOL,
            capabilities=list(capabilities or []),
            pools=list(pools),
            rm_domains=list(rm_domains or []),
            pid_ns=process_identity()[3],
        )
        self._open_request = request
        response = self._open_until_answered(request)
        with self._renew_lock:
            self._take_session(response)
        logger.info(
            "Node session open: node %s epoch %s epoch_seq %d (server epoch %s)",
            self.node_id,
            self.node_epoch,
            self.epoch_seq,
            self.server_epoch,
        )
        return response

    def orphans(self, opened):
        items = list(opened.orphans)
        cursor = opened.next_cursor
        while cursor is not None:
            session, epoch = self._ids()
            answer = self._rpc.call(
                MessageType.NODE_ORPHANS, NodeOrphansRequest(session, epoch, cursor)
            )
            if answer.status != Status.OK:
                raise NodeOpenError("NODE_ORPHANS refused")
            items.extend(answer.orphans)
            cursor = answer.next_cursor
        return items

    def _open_until_answered(self, request: NodeOpenRequest) -> NodeOpenResponse:
        pause = RESEND_PAUSE_S
        while True:
            if self._stop.is_set():
                raise ServerStopped("stopped before the node session opened")
            try:
                response: NodeOpenResponse = self._rpc.call(
                    MessageType.NODE_OPEN, request
                )
            except RpcUnknownError as e:
                if (
                    isinstance(e.__cause__, MessageDecodeError)
                    and self._is_cxl_server()
                ):
                    raise NodeOpenError(
                        f"{self.url} is not a --backend node server"
                    ) from e
                logger.warning("NODE_OPEN to %s: %s; trying again", self.url, e)
                self._stop.wait(pause)
                pause = min(pause * 2, RESEND_PAUSE_MAX_S)
                continue
            if response.status == Status.BUSY:
                self._stop.wait(pause)
                pause = min(pause * 2, RESEND_PAUSE_MAX_S)
                continue
            if response.status != Status.OK:
                raise NodeOpenError(
                    f"MaruServer refused NODE_OPEN: {response.status} {response.error}"
                )
            if not 0 <= response.epoch_seq <= EPOCH_SEQ_MAX:
                raise NodeOpenError(f"epoch_seq {response.epoch_seq} is out of range")
            if response.heartbeat_s <= 0 or response.session_ttl_s <= 0:
                raise NodeOpenError(
                    f"heartbeat_s {response.heartbeat_s} and session_ttl_s "
                    f"{response.session_ttl_s} must be positive"
                )
            return response

    def _is_cxl_server(self) -> bool:
        try:
            hello = self._rpc.call(MessageType.HANDSHAKE, HandshakeRequest())
        except RpcUnknownError:
            return False
        return (
            hello.success
            and hello.rm_address is not None
            and hello.server_epoch is None
        )

    def _take_session(self, response: NodeOpenResponse) -> None:
        """Caller holds _renew_lock."""
        self.session = response.node_session
        self.server_epoch = response.server_epoch
        self.epoch_seq = response.epoch_seq
        self.heartbeat_s = response.heartbeat_s
        self.session_ttl_s = response.session_ttl_s

    @property
    def epoch_lost(self) -> bool:
        """MaruServer answered EPOCH to this session."""
        return self._epoch_lost.is_set()

    def close_session(self, mode: CloseMode) -> bool:
        """NODE_CLOSE, one attempt; whether it was answered OK."""
        if self.session is None or self.server_epoch is None or self.epoch_lost:
            return False
        try:
            response: NodeCloseResponse = self._rpc.call(
                MessageType.NODE_CLOSE,
                NodeCloseRequest(
                    session=self.session, server_epoch=self.server_epoch, mode=mode
                ),
            )
        except RpcUnknownError as e:
            logger.warning("NODE_CLOSE(%s) unanswered: %s", mode.value, e)
            return False
        return response.status == Status.OK

    def stop(self) -> None:
        """Give up unresolved ops and stop the heartbeat (NODE_CLOSE still works)."""
        self._stop.set()
        if (
            self._heartbeat is not None
            and self._heartbeat is not threading.current_thread()
        ):
            self._heartbeat.join(timeout=5.0)

    def close(self) -> None:
        self.stop()
        self._rpc.close()

    # -------------------------------------------------------------- heartbeat

    def start_heartbeat(
        self,
        usage: Callable[[], list[PoolUsage]],
        on_response: Callable[[NodeHeartbeatResponse], None],
        on_epoch: Callable[[str], None],
    ) -> None:
        self._on_epoch = on_epoch

        def loop() -> None:
            while not self._stop.wait(self.heartbeat_s):
                if self.epoch_lost:
                    return  # the run is over
                session = self.session
                try:
                    response = self.heartbeat(usage())
                except RpcUnknownError as e:
                    logger.warning("NODE_HEARTBEAT unanswered: %s", e)
                    continue
                except Exception:
                    logger.exception("NODE_HEARTBEAT failed")
                    continue
                if response.status == Status.EPOCH:
                    if self._renew(session) is False:
                        self._lose_epoch(response.error)
                    continue
                if response.status != Status.OK:
                    logger.error(
                        "NODE_HEARTBEAT: %s %s", response.status, response.error
                    )
                    continue
                self.last_heartbeat_ok = time.monotonic()
                try:
                    on_response(response)
                except Exception:
                    logger.exception("handling a heartbeat response failed")

        heartbeat = threading.Thread(
            target=loop, name="maru-node-heartbeat", daemon=True
        )
        heartbeat.start()
        # Published once started: stop() may run meanwhile and joins only a
        # started thread; the loop sees _stop and ends.
        self._heartbeat = heartbeat

    def heartbeat(self, usage: list[PoolUsage]) -> NodeHeartbeatResponse:
        """One NODE_HEARTBEAT."""
        session, epoch = self._ids()
        response: NodeHeartbeatResponse = self._rpc.call(
            MessageType.NODE_HEARTBEAT,
            NodeHeartbeatRequest(session, epoch, usage, memory_peer=self.memory_peer),
        )
        return response

    def clear_memory_peer(self) -> None:
        """Withdraw the peer once the periodic heartbeat has stopped."""
        if self.session is None or self.epoch_lost:
            return
        self.memory_peer = None
        try:
            response = self.heartbeat([])
        except Exception as exc:
            logger.warning("Could not withdraw memory peer: %s", exc)
            return
        if response.status != Status.OK:
            logger.warning("Memory peer withdrawal refused: %s", response.status)

    def _ids(self) -> tuple[str, str]:
        """(session, server_epoch) of the open session."""
        session, epoch = self.session, self.server_epoch
        if session is None or epoch is None:
            raise RuntimeError("the node session is not open")
        return session, epoch

    def _renew(self, failed_session: str | None) -> bool | None:
        """After EPOCH: True if the session is usable again (reopened in this
        run, or already by another thread), False if MaruServer restarted or
        ended the run (``lost_reason`` says which), None if it could not be
        told yet."""
        with self._renew_lock:
            if self.epoch_lost:
                return False
            if self.session != failed_session:
                return True
            try:
                hello = self._rpc.call(MessageType.HANDSHAKE, HandshakeRequest())
            except RpcUnknownError as e:
                logger.warning("HANDSHAKE after EPOCH: %s", e)
                return None
            if hello.server_epoch != self.server_epoch:
                self.lost_reason = (
                    f"MaruServer restarted (server epoch {self.server_epoch} -> "
                    f"{hello.server_epoch})"
                )
                return False
            assert self._open_request is not None
            try:
                response = self._rpc.call(MessageType.NODE_OPEN, self._open_request)
            except RpcUnknownError as e:
                logger.warning("renewing the node session: %s", e)
                return None
            if response.status == Status.BUSY:
                return None
            if (
                response.status == Status.OK
                and response.server_epoch != self.server_epoch
            ):
                # Restarted after the HANDSHAKE: that open made a run there
                # that the next run's NODE_OPEN ends
                self.lost_reason = (
                    f"MaruServer restarted (server epoch {self.server_epoch} -> "
                    f"{response.server_epoch})"
                )
                return False
            if response.status != Status.OK or response.epoch_seq != self.epoch_seq:
                self.lost_reason = (
                    f"MaruServer ended this run: {response.status} {response.error} "
                    f"(epoch_seq {response.epoch_seq})"
                )
                return False
            self.session = response.node_session
            self.heartbeat_s = response.heartbeat_s or self.heartbeat_s
            self.session_ttl_s = response.session_ttl_s or self.session_ttl_s
            self.renewals += 1
            logger.warning("node session expired and was renewed (same node epoch)")
            return True

    def _lose_epoch(self, error) -> None:
        """MaruServer no longer knows the session: tell the heartbeat's
        ``on_epoch`` (called on every loss seen, so idempotent)."""
        with self._renew_lock:
            first = not self._epoch_lost.is_set()
            self._epoch_lost.set()
            reason = self.lost_reason or f"MaruServer answered EPOCH: {error}"
        if first:
            logger.error("node session %s lost: %s", self.session, reason)
        on_epoch = self._on_epoch
        if on_epoch is not None:
            on_epoch(reason)

    # -------------------------------------------------------------------- ops

    def commit(self, items: list[CommitItem]) -> CommitResponse:
        """COMMIT until its result is known."""
        session, epoch = self._ids()
        response: CommitResponse = self._call_op(
            MessageType.COMMIT,
            CommitRequest(
                session=session,
                server_epoch=epoch,
                op=PLACEHOLDER_OP,
                items=items,
            ),
            len(items),
        )
        return response

    def retire(self, items: list[KeyLocation]) -> RetireResponse:
        """RETIRE until its result is known."""
        session, epoch = self._ids()
        response: RetireResponse = self._call_op(
            MessageType.RETIRE,
            RetireRequest(
                session=session,
                server_epoch=epoch,
                op=PLACEHOLDER_OP,
                items=items,
            ),
            len(items),
        )
        return response

    def worker_exit(
        self, workers: list[WorkerRef], cause: ExitCause
    ) -> WorkerExitResponse:
        """WORKER_EXIT until its result is known."""
        session, epoch = self._ids()
        response: WorkerExitResponse = self._call_op(
            MessageType.WORKER_EXIT,
            WorkerExitRequest(
                session=session,
                server_epoch=epoch,
                op=PLACEHOLDER_OP,
                workers=workers,
                cause=cause,
            ),
            len(workers),
        )
        return response

    def recover_list(self, pool_id: str) -> list[RecoverItem]:
        """All committed locations of an adopted pool (NODE_RECOVER_LIST)."""
        items: list[RecoverItem] = []
        cursor = None
        pause = RESEND_PAUSE_S
        while True:
            if self._stop.is_set():
                raise ServerStopped("stopped while listing an adopted pool")
            session, epoch = self._ids()
            try:
                response = self._rpc.call(
                    MessageType.NODE_RECOVER_LIST,
                    NodeRecoverListRequest(
                        session=session,
                        server_epoch=epoch,
                        pool_id=pool_id,
                        cursor=cursor,
                    ),
                )
            except RpcUnknownError as e:
                logger.warning("NODE_RECOVER_LIST %s: %s; trying again", pool_id, e)
                self._stop.wait(pause)
                pause = min(pause * 2, RESEND_PAUSE_MAX_S)
                continue
            if response.status == Status.BUSY:
                self._stop.wait(pause)
                continue
            if response.status == Status.EPOCH:
                if self._renew(session) is False:
                    self._lose_epoch(response.error)
                    raise EpochChanged(response.error)
                self._stop.wait(pause)
                continue
            if response.status != Status.OK:
                raise NodeOpenError(
                    f"NODE_RECOVER_LIST {pool_id}: {response.status} {response.error}"
                )
            items.extend(response.items)
            if response.next_cursor is None:
                return items
            cursor = response.next_cursor

    def _call_op(
        self, msg_type: MessageType, request, count: int, *, tracked: bool = True
    ):
        """Stamp a new op and send it until a result (OK, REJECTED, DISABLED);
        unknown results and BUSY are resent with the same op while the
        session lasts (it never moves to another MaruServer run).
        UNPIN resends pin ids without an op."""
        if not 1 <= count <= MAX_BATCH:
            raise ValueError(f"{count} items; a request carries 1..{MAX_BATCH}")
        if self.epoch_lost:
            raise EpochChanged("the node session is gone")
        if tracked:
            request = self._origin.stamp(request)
        seq = request.op.seq if tracked else None
        pause = RESEND_PAUSE_S
        attempt = 0
        while True:
            if self._stop.is_set():
                raise ServerStopped(f"{msg_type.name} op {seq} left unresolved")
            if attempt:
                # may wait while _renew holds the lock across its two RPCs
                with self._renew_lock:
                    session = self.session
                if self.epoch_lost:
                    if tracked:
                        self._origin.resolved(seq)
                    raise EpochChanged(f"{msg_type.name} op {seq}: session lost")
                if session != request.session:  # renewed meanwhile
                    request = dataclasses.replace(request, session=session)
                self.resends += 1
                if tracked:
                    request = self._origin.refresh(request)
            attempt += 1
            try:
                response = self._rpc.call(msg_type, request)
            except RpcUnknownError as e:
                logger.warning(
                    "%s op %s: result unknown (%s); sending the same request again",
                    msg_type.name,
                    seq,
                    e,
                )
                self._stop.wait(pause)
                pause = min(pause * 2, RESEND_PAUSE_MAX_S)
                continue
            if response.status == Status.BUSY:
                self._stop.wait(pause)
                pause = min(pause * 2, RESEND_PAUSE_MAX_S)
                continue
            if response.status == Status.EPOCH:
                renewed = self._renew(request.session)
                if renewed is False:
                    if tracked:
                        self._origin.resolved(seq)  # a new server keeps no record
                    self._lose_epoch(response.error)
                    raise EpochChanged(response.error)
                if renewed is None:
                    self._stop.wait(pause)
                    pause = min(pause * 2, RESEND_PAUSE_MAX_S)
                # The same op under a renewed session (the digest leaves it out)
                continue
            if tracked:
                self._origin.resolved(seq)
            if response.status == Status.OK and len(response.items) != count:
                logger.error(
                    "%s op %s: %d results for %d items",
                    msg_type.name,
                    seq,
                    len(response.items),
                    count,
                )
                raise ReplyError(f"{msg_type.name}: {len(response.items)} results")
            return response

    @property
    def op_origin(self) -> OpOrigin:
        return self._origin
