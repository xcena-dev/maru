# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Authenticated memory-node control requests and operation replay."""

from __future__ import annotations

import collections
import itertools
import logging
import socket
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import Any, Protocol

from maru_common.node_protocol import recv_frame, send_frame
from maru_common.protocol import MessageHeader
from maru_common.remote_protocol import (
    LEDGER_CODES,
    REMOTE_MESSAGE_CLASSES,
    REMOTE_WIRE_VERSION,
    AgentsResponse,
    CancelRequest,
    CancelResponse,
    GoodbyeResponse,
    HelloRefusal,
    HelloRequest,
    HelloResponse,
    PingResponse,
    RemoteHintResponse,
    RemoteMessageType,
    RemotePool,
    RemoteWriteDoneRequest,
    RemoteWriteDoneResponse,
    ReserveRequest,
    ReserveResponse,
    RoomLimit,
    peer_origin,
)
from maru_common.serializer import (
    DecodedRequest,
    MessageDecodeError,
    Serializer,
    op_digest,
)
from maru_common.storage_types import (
    REMOTE_CAPABILITY,
    WORKER_RDMA_CAPABILITY,
    ItemOutcome,
    ItemResult,
    OpRef,
    Status,
)
from maru_remote.auth import (
    AUTH_TIMEOUT_S,
    MAX_CONNECTIONS,
    MAX_PER_SOURCE,
    NONCE_BYTES,
    ConnectionAuth,
    connection_limit,
    set_keepalive,
)
from maru_remote.framing import parse_address
from maru_remote.framing import recv_frame as recv_proof
from maru_remote.framing import send_frame as send_proof
from maru_remote.progress import ConnectProgress

from .op_ledger import LedgerEntry, OpLedger, Verdict

logger = logging.getLogger(__name__)
_JOIN_TIMEOUT_S = 2.0
_ACCEPT_POLL_S = 0.05
_MAX_PENDING_HINTS = 64


class RemoteOps(Protocol):
    """Page operations; each done is called exactly once, from any thread."""

    def reserve(
        self,
        origin: str,
        request: ReserveRequest,
        done: Callable[[ReserveResponse], None],
    ) -> None:
        """Credit pages to this peer run."""
        ...

    def write_done(
        self,
        origin: str,
        request: RemoteWriteDoneRequest,
        done: Callable[[RemoteWriteDoneResponse], None],
    ) -> None:
        """Commit completed writes and quarantine uncertain ones."""
        ...

    def cancel(
        self,
        origin: str,
        request: CancelRequest,
        done: Callable[[CancelResponse], None],
    ) -> None:
        """Return credits never posted to, including by reserve sequence."""
        ...

    def fence_domain(self, origin, request, done) -> None: ...
    def agents(self, origin, request, done) -> None: ...
    def hint(self, origin, request, done) -> None: ...

    def room(self, origin: str) -> tuple[int, RoomLimit | None]:
        """Bytes this peer could reserve now, and any quarantine limit."""
        ...

    def peer_lost(self, origin: str) -> None:
        """The peer run has no live connection; outstanding pages need fencing."""
        ...

    def peer_retired(self, origin: str) -> None:
        """A new epoch or GOODBYE ended this origin permanently."""
        ...


class RemoteServer(ABC):
    """One endpoint per memory node run, ready last and stopped first."""

    @abstractmethod
    def start(self) -> None:
        """Bind and listen, raising OSError if the address is taken."""

    @abstractmethod
    def ready(self, server_epoch: str, pools: list[RemotePool]) -> None:
        """Serve HELLOs for the open run and its registered pools."""

    @abstractmethod
    def stop(self) -> None:
        """Refuse new operations and close every connection. Idempotent."""


class _Connection:
    """A control reader, response writer and bounded hint worker for one stream."""

    def __init__(self, server: TcpRemoteServer, sock: socket.socket, source: str):
        self.server, self.sock, self.source = server, sock, source
        self.extension_id = next(server._connection_ids)
        self.auth = ConnectionAuth(server._key)
        self.started = time.monotonic()
        self.hello: HelloRequest | None = None
        self.origin = ""
        self.closed = threading.Event()
        self.draining = False
        self._outbox: collections.deque[tuple[bytes, bool]] = collections.deque()
        self._hints: collections.deque[DecodedRequest] = collections.deque()
        self._cv = threading.Condition()
        self._reader = threading.Thread(
            target=self._read_loop, name="maru-remote-read", daemon=True
        )
        # Made once proven: a thread never started keeps its target, a
        # cycle with this connection
        self._writer: threading.Thread | None = None
        self._hint_thread: threading.Thread | None = None

    def start(self) -> None:
        self._reader.start()

    def post_response(
        self, header: MessageHeader, response: Any, *, close: bool = False
    ) -> None:
        frame = self.server.serializer.encode_response(header, response)
        with self._cv:
            if self.closed.is_set() or self.draining:
                return
            self.draining = close
            self._outbox.append((frame, close))
            self._cv.notify_all()

    def enqueue_hint(self, decoded: DecodedRequest) -> None:
        with self._cv:
            if self.closed.is_set() or self.draining:
                return
            if len(self._hints) >= _MAX_PENDING_HINTS:
                self.post_response(
                    decoded.header,
                    RemoteHintResponse(Status.BUSY, error="hint queue full"),
                )
                return
            self._hints.append(decoded)
            if self._hint_thread is None:
                self._hint_thread = threading.Thread(
                    target=self._hint_loop, name="maru-remote-hint", daemon=True
                )
                self._hint_thread.start()
            self._cv.notify_all()

    def close(self) -> None:
        with self._cv:
            if self.closed.is_set():
                return
            self.closed.set()
            self._outbox.clear()
            self._hints.clear()
            self._cv.notify_all()
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()
        self.server._connection_closed(self)

    def join(self, deadline: float) -> None:
        for thread in (self._reader, self._writer, self._hint_thread):
            if (
                thread is not None
                and thread.is_alive()
                and thread is not threading.current_thread()
            ):
                thread.join(max(0, deadline - time.monotonic()))

    def _authenticate(self) -> bool:
        self.sock.settimeout(AUTH_TIMEOUT_S)
        send_proof(self.sock, {"nonce": self.auth.nonce})
        message = recv_proof(self.sock)
        if (
            message is None
            or message.get("op") != "auth"
            or not isinstance(message.get("nonce"), bytes)
            or len(message["nonce"]) != NONCE_BYTES
        ):
            return False
        reply = self.auth.check(message)
        send_proof(self.sock, reply)
        if not self.auth.proved:
            return False
        self.sock.settimeout(None)
        return True

    def _read_loop(self) -> None:
        try:
            if not self._authenticate():
                return
            self._writer = threading.Thread(
                target=self._write_loop, name="maru-remote-write", daemon=True
            )
            self._writer.start()
            while not self.closed.is_set() and not self.draining:
                got = recv_frame(self.sock)
                if got is None:
                    break
                try:
                    decoded = self.server.serializer.decode_request_payload(got[0])
                except MessageDecodeError as exc:
                    if exc.header is None:
                        break
                    if exc.header.msg_type == RemoteMessageType.HELLO:
                        logger.info("remote HELLO refused: invalid payload: %s", exc)
                    self.post_response(
                        exc.header,
                        {"status": Status.REJECTED, "error": str(exc)},
                        close=self.hello is None,
                    )
                    continue
                self.server._dispatch(self, decoded)
        except Exception:
            logger.debug("remote connection ended", exc_info=True)
        finally:
            if not self.draining:
                self.close()

    def _write_loop(self) -> None:
        try:
            while True:
                with self._cv:
                    self._cv.wait_for(lambda: self._outbox or self.closed.is_set())
                    if self.closed.is_set():
                        return
                    frame, close = self._outbox.popleft()
                send_frame(self.sock, frame)
                if close:
                    return
        except (OSError, ValueError):
            logger.debug("remote response failed", exc_info=True)
        finally:
            self.close()

    def _hint_loop(self) -> None:
        while True:
            with self._cv:
                self._cv.wait_for(lambda: self._hints or self.closed.is_set())
                if self.closed.is_set():
                    return
                decoded = self._hints.popleft()
            done = partial(self.post_response, decoded.header)
            self.server._invoke(
                self.server.extension_hint,
                (self.extension_id, decoded.request, done, self.closed.is_set),
                RemoteHintResponse,
                done,
            )


@dataclass
class _Peer:
    epoch: str
    origin: str
    connection: _Connection | None


class TcpRemoteServer(RemoteServer):
    """TCP control plane with a run-wide operation ledger."""

    def __init__(
        self,
        listen: str,
        key: bytes,
        *,
        node_id: str,
        node_epoch: str,
        capabilities: list[str],
        page_size: int,
        agent_name: str,
        progress: ConnectProgress,
        ops: RemoteOps,
        boot: str = "",
        agent_metadata: Callable[[], bytes] = lambda: b"",
        op_window: int = 4096,
        check_memory: Callable[[], bool] = lambda: True,
        hint_actions: list[str] | None = None,
        extension_hint: Callable | None = None,
        extension_reserve_hint: Callable | None = None,
        connection_closed: Callable[[int], None] | None = None,
    ):
        if not key:
            raise ValueError("a remote server requires a key")
        if op_window < 1:
            raise ValueError("ledger bound must be positive")
        self._listen, self._key = listen, key
        self.node_id, self.node_epoch = node_id, node_epoch
        self.capabilities, self.page_size = list(capabilities), page_size
        self.agent_name, self.progress, self.ops = agent_name, progress, ops
        self.serializer = Serializer(REMOTE_MESSAGE_CLASSES)
        self.ledger = OpLedger(op_window)
        self.boot = boot
        self.check_memory = check_memory
        self.hint_actions = hint_actions or []
        self.extension_hint = extension_hint
        self.extension_reserve_hint = extension_reserve_hint
        self.connection_closed = connection_closed
        self.agent_metadata = agent_metadata
        self._cv = threading.Condition(threading.RLock())
        self._stopped = threading.Event()
        self._sock: socket.socket | None = None
        self.address: tuple[str, int] | None = None
        self._accept_thread: threading.Thread | None = None
        self._connections: set[_Connection] = set()
        self._connection_ids = itertools.count(1)
        self._peers: dict[str, _Peer] = {}
        self._retired: set[tuple[str, str]] = set()
        self._server_epoch: str | None = None
        self._pools: list[RemotePool] = []
        self._limit = min(MAX_CONNECTIONS, connection_limit())

    def start(self) -> None:
        with self._cv:
            if self._stopped.is_set():
                raise RuntimeError("remote server is stopped")
            if self._sock is not None:
                return
            sock = socket.create_server(parse_address(self._listen), reuse_port=False)
            sock.settimeout(_ACCEPT_POLL_S)
            self._sock = sock
            self.address = sock.getsockname()[:2]
            self._accept_thread = threading.Thread(
                target=self._accept_loop, name="maru-remote-accept", daemon=True
            )
            self._accept_thread.start()

    def ready(self, server_epoch: str, pools: list[RemotePool]) -> None:
        with self._cv:
            if not self._stopped.is_set():
                self._server_epoch, self._pools = server_epoch, list(pools)

    def stop(self) -> None:
        deadline = time.monotonic() + _JOIN_TIMEOUT_S
        with self._cv:
            if self._stopped.is_set():
                return
            self._stopped.set()
            if self._sock is not None:
                self._sock.close()
            connections = list(self._connections)
            self._cv.notify_all()
        for conn in connections:
            conn.close()
        self.progress.close()
        threads = [self._accept_thread]
        for thread in threads:
            if thread is not None and thread is not threading.current_thread():
                thread.join(max(0, deadline - time.monotonic()))
        for conn in connections:
            conn.join(deadline)

    def _accept_loop(self) -> None:
        assert self._sock is not None
        while not self._stopped.is_set():
            with self._cv:
                unproven = [
                    c
                    for c in self._connections
                    if not c.auth.proved
                    and time.monotonic() - c.started >= AUTH_TIMEOUT_S
                ]
            for conn in unproven:
                conn.close()
            try:
                sock, address = self._sock.accept()
            except TimeoutError:
                continue
            except OSError:
                if not self._stopped.is_set():
                    logger.debug("remote accept failed", exc_info=True)
                    self._stopped.wait(_ACCEPT_POLL_S)
                continue
            with self._cv:
                mine = sum(
                    c.source == address[0] and not c.auth.proved
                    for c in self._connections
                )
                if (
                    self._stopped.is_set()
                    or len(self._connections) >= self._limit
                    or mine >= MAX_PER_SOURCE
                ):
                    sock.close()
                    continue
                try:
                    set_keepalive(sock)
                    conn = _Connection(self, sock, address[0])
                    self._connections.add(conn)
                    conn.start()
                except OSError:
                    sock.close()
                    logger.debug("remote accept setup failed", exc_info=True)

    def _connection_closed(self, conn: _Connection) -> None:
        with self._cv:
            self._connections.discard(conn)
            if conn.hello is not None:
                self.ledger.forget_waiter(conn.origin, conn)
                peer = self._peers.get(conn.hello.node_id)
                if peer is not None and peer.connection is conn:
                    peer.connection = None
                    logger.info("remote peer lost: %s", peer.origin)
                    self.ops.peer_lost(peer.origin)
        if conn.auth.proved and self.connection_closed is not None:
            self.connection_closed(conn.extension_id)

    def _retire(self, node_id: str, peer: _Peer, *, goodbye: bool = False) -> None:
        self._retired.add((node_id, peer.epoch))
        self._peers.pop(node_id, None)
        if peer.connection is not None:
            self.ledger.forget_waiter(peer.origin, peer.connection)
            if not goodbye:
                peer.connection.close()
        logger.info("remote peer retired: %s", peer.origin)
        self.ops.peer_retired(peer.origin)
        self.ledger.drop(peer.origin)

    def _ack(self, origin: str, ack: int) -> None:
        # A seq at its own ack is STALE and applies the watermark without an entry.
        self.ledger.begin(origin, OpRef(origin, ack, b"", ack), b"")

    def _hello(self, conn: _Connection, decoded: DecodedRequest) -> None:
        req: HelloRequest = decoded.request
        refusal = None
        status, error = Status.REJECTED, ""
        if req.version != REMOTE_WIRE_VERSION:
            refusal = HelloRefusal.VERSION
        elif self._server_epoch is None:
            status, refusal = Status.BUSY, HelloRefusal.NOT_READY
        elif req.server_epoch != self._server_epoch:
            status, refusal = Status.BUSY, HelloRefusal.SERVER_EPOCH
        elif req.page_size != self.page_size:
            refusal = HelloRefusal.PAGE_SIZE
        elif (req.node_id, req.node_epoch) in self._retired:
            refusal = HelloRefusal.RETIRED
        elif not {REMOTE_CAPABILITY, WORKER_RDMA_CAPABILITY} <= set(
            req.capabilities
        ) or set(req.capabilities) - {REMOTE_CAPABILITY, WORKER_RDMA_CAPABILITY}:
            error = "incompatible capabilities"
        else:
            origin = peer_origin(req.node_id, req.node_epoch)
            old = self._peers.get(req.node_id)
            if old is not None and old.epoch != req.node_epoch:
                self._retire(req.node_id, old)
            conn.hello, conn.origin = req, origin
            self._peers[req.node_id] = _Peer(req.node_epoch, origin, conn)
            if old is not None and old.epoch == req.node_epoch:
                if old.connection is not None:
                    old.connection.close()
            self._ack(origin, req.ack)
            self.progress.drain()
            room, limit = self.ops.room(origin)
            conn.post_response(
                decoded.header,
                HelloResponse(
                    status=Status.OK,
                    node_id=self.node_id,
                    node_epoch=self.node_epoch,
                    server_epoch=self._server_epoch,
                    capabilities=self.capabilities,
                    agent_name=self.agent_name,
                    agent_metadata=self.agent_metadata(),
                    pools=self._pools,
                    room=room,
                    limit=limit,
                    boot=self.boot,
                    hint_actions=self.hint_actions,
                ),
            )
            logger.info("remote HELLO accepted: %s", origin)
            return
        error = error or str(refusal)
        logger.info(
            "remote HELLO refused: %s@%s: %s", req.node_id, req.node_epoch, error
        )
        conn.post_response(
            decoded.header,
            HelloResponse(status=status, refusal=refusal, error=error),
            close=True,
        )

    def _dispatch(self, conn: _Connection, decoded: DecodedRequest) -> None:
        code, header = decoded.header.msg_type, decoded.header
        response_cls = REMOTE_MESSAGE_CLASSES[RemoteMessageType(code)][1]
        invoke = None
        with self._cv:
            if self._stopped.is_set() or conn.closed.is_set():
                return
        if not self.check_memory():
            fields = {"boot": self.boot} if code == RemoteMessageType.HELLO else {}
            conn.post_response(
                header,
                response_cls(
                    status=Status.BUSY,
                    error="memory generation changed or unknown",
                    **fields,
                ),
                close=True,
            )
            return
        if (
            code == RemoteMessageType.HINT
            and conn.hello is not None
            and self.extension_hint is not None
        ):
            conn.enqueue_hint(decoded)
            return
        with self._cv:
            if self._stopped.is_set() or conn.closed.is_set():
                return
            if conn.hello is None:
                if code == RemoteMessageType.HELLO:
                    self._hello(conn, decoded)
                else:
                    conn.post_response(
                        header,
                        response_cls(
                            status=Status.REJECTED, error="HELLO must be first"
                        ),
                        close=True,
                    )
                return
            if code == RemoteMessageType.HELLO:
                logger.info("remote HELLO refused: %s: already greeted", conn.origin)
                conn.post_response(
                    header,
                    HelloResponse(status=Status.REJECTED, error="already greeted"),
                )
            elif code == RemoteMessageType.PING:
                self.progress.drain()
                room, limit = self.ops.room(conn.origin)
                conn.post_response(
                    header, PingResponse(Status.OK, room=room, limit=limit)
                )
            elif code == RemoteMessageType.GOODBYE:
                conn.post_response(header, GoodbyeResponse(Status.OK), close=True)
                node_id = conn.hello.node_id
                self._retire(node_id, self._peers[node_id], goodbye=True)
            elif code == RemoteMessageType.AGENTS:
                self._invoke(
                    self.ops.agents,
                    (
                        conn.origin,
                        decoded.request,
                        lambda reply: conn.post_response(header, reply),
                    ),
                    AgentsResponse,
                    lambda reply: conn.post_response(header, reply),
                )
            elif code == RemoteMessageType.HINT:
                self._invoke(
                    self.ops.hint,
                    (
                        conn.origin,
                        decoded.request,
                        lambda reply: conn.post_response(header, reply),
                    ),
                    RemoteHintResponse,
                    lambda reply: conn.post_response(header, reply),
                )
            elif code in LEDGER_CODES:
                invoke = self._op(conn, decoded)
        if invoke is not None:
            self._invoke(*invoke)

    def _op(self, conn: _Connection, decoded: DecodedRequest) -> tuple | None:
        code, req, header = decoded.header.msg_type, decoded.request, decoded.header
        response_cls = REMOTE_MESSAGE_CLASSES[RemoteMessageType(code)][1]
        origin, op = conn.origin, req.op
        digest = op_digest(decoded.payload)
        if op.origin != origin or op.digest != digest:
            conn.post_response(
                header,
                response_cls(status=Status.REJECTED, error="origin/digest mismatch"),
            )
            return
        self._ack(origin, op.ack)
        verdict, entry = self.ledger.begin(origin, op, digest)
        if verdict == Verdict.STALE:
            if code == RemoteMessageType.WRITE_DONE:
                response = RemoteWriteDoneResponse(
                    Status.OK, items=[ItemOutcome(ItemResult.STALE)] * len(req.items)
                )
            else:
                response = response_cls(status=Status.REJECTED, error="stale op")
            conn.post_response(header, response)
            return
        if verdict in (Verdict.CONFLICT, Verdict.BUSY):
            conn.post_response(
                header,
                response_cls(
                    status=Status.REJECTED
                    if verdict == Verdict.CONFLICT
                    else Status.BUSY,
                    error="op digest conflict"
                    if verdict == Verdict.CONFLICT
                    else "op window full",
                ),
            )
            return
        assert entry is not None
        waiting, displaced = self.ledger.attach(
            entry, lambda response: conn.post_response(header, response), conn
        )
        if not waiting:
            conn.post_response(header, entry.response)
        if displaced is not None:
            displaced(
                response_cls(status=Status.BUSY, error="too many resends of this op")
            )
        if verdict == Verdict.REPLAY:
            return
        done = self._completion(origin, entry)
        if (
            code == RemoteMessageType.RESERVE
            and self.extension_reserve_hint is not None
        ):
            finish = done

            def done(response):
                if response.credits:
                    self.extension_reserve_hint(
                        conn.extension_id, req, response.credits, conn.closed.is_set
                    )
                finish(response)

        method = getattr(self.ops, RemoteMessageType(code).name.lower())
        return method, (origin, req, done), response_cls, done

    def _completion(self, origin: str, entry: LedgerEntry) -> Callable[[Any], None]:
        lock = threading.Lock()

        def done(response: Any) -> None:
            with lock:
                if self._stopped.is_set() or entry.done.is_set():
                    return
                replies = self.ledger.finish(origin, entry, response)
            for reply in replies:
                reply(response)

        return done

    @staticmethod
    def _invoke(
        method: Callable, args: tuple, response_cls: type, done: Callable
    ) -> None:
        try:
            method(*args)
        except Exception:
            logger.debug("remote operation failed", exc_info=True)
            done(response_cls(status=Status.REJECTED, error="operation failed"))
