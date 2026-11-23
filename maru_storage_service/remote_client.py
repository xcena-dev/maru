# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""One authenticated remote stream; callbacks receive a response or ConnectionError."""

from __future__ import annotations

import hmac
import logging
import secrets
import socket
import threading
import time
from collections.abc import Callable
from typing import Any

from maru_common.node_protocol import recv_frame, send_frame
from maru_common.protocol import MessageHeader
from maru_common.remote_protocol import (
    REMOTE_MESSAGE_CLASSES,
    HelloRequest,
    HelloResponse,
    RemoteMessageType,
)
from maru_common.serializer import RESPONSE_BIT, Serializer
from maru_remote.auth import NONCE_BYTES, prove, set_keepalive
from maru_remote.framing import parse_address
from maru_remote.framing import recv_frame as recv_proof
from maru_remote.framing import send_frame as send_proof

logger = logging.getLogger(__name__)
Reply = Callable[[Any], None]


class RemoteConnection:
    """Seq-matched requests, without reconnects or op sequence bookkeeping.

    Callbacks run on the response reader (or the thread detecting failure)
    and must return promptly. A blocking call timeout only removes its waiter.
    """

    def __init__(self, sock: socket.socket, serializer: Serializer):
        self._sock, self._serializer = sock, serializer
        self._send_lock = threading.Lock()
        self._lock = threading.Lock()
        self._pending: dict[int, tuple[int, Reply]] = {}
        self._closed = threading.Event()
        self._on_close: list[Callable[[], None]] = []
        self._reader = threading.Thread(
            target=self._read_loop, name="maru-remote-client", daemon=True
        )
        self._reader.start()

    @classmethod
    def open(
        cls,
        address: str | tuple[str, int],
        key: bytes,
        hello: HelloRequest,
        timeout_s: float,
    ) -> tuple[RemoteConnection, HelloResponse]:
        """Prove both keys and exchange HELLO, including a refused HELLO response."""
        deadline = time.monotonic() + timeout_s
        sock = socket.create_connection(
            parse_address(address) if isinstance(address, str) else address, timeout_s
        )
        try:
            set_keepalive(sock)
            challenge = recv_proof(sock)
            nonce = None if challenge is None else challenge.get("nonce")
            if not isinstance(nonce, bytes) or len(nonce) != NONCE_BYTES:
                raise ConnectionError("missing remote key challenge")
            mine = secrets.token_bytes(NONCE_BYTES)
            sock.settimeout(max(0.001, deadline - time.monotonic()))
            send_proof(
                sock,
                {"op": "auth", "nonce": mine, "proof": prove(key, "handler", nonce)},
            )
            reply = recv_proof(sock)
            proof = None if reply is None else reply.get("proof")
            if (
                reply is None
                or not reply.get("ok")
                or not isinstance(proof, bytes)
                or not hmac.compare_digest(proof, prove(key, "agent", mine))
            ):
                raise ConnectionError("remote key proof failed")
            serializer = Serializer(REMOTE_MESSAGE_CLASSES)
            frame = serializer.encode(RemoteMessageType.HELLO, hello)
            sock.settimeout(max(0.001, deadline - time.monotonic()))
            send_frame(sock, frame)
            got = recv_frame(sock)
            if got is None:
                raise ConnectionError("connection closed before HELLO")
            header, response = serializer.decode_response(got[0])
            if (
                header.sequence != MessageHeader.unpack(frame).sequence
                or header.msg_type != RemoteMessageType.HELLO | RESPONSE_BIT
            ):
                raise ConnectionError("unexpected HELLO response")
            sock.settimeout(None)
            return cls(sock, serializer), response
        except BaseException:
            sock.close()
            raise

    @property
    def closed(self) -> bool:
        return self._closed.is_set()

    def submit(self, msg_type: int, request: Any, callback: Reply) -> int:
        """Send a request; answer callback once with its response or a connection error."""
        failure: ConnectionError | None = None
        registered = False
        with self._send_lock:
            frame = self._serializer.encode(msg_type, request)
            seq = MessageHeader.unpack(frame).sequence
            with self._lock:
                if self.closed:
                    failure = ConnectionError("remote connection is closed")
                else:
                    self._pending[seq] = (msg_type | RESPONSE_BIT, callback)
                    registered = True
            if failure is None:
                try:
                    send_frame(self._sock, frame)
                except (OSError, ValueError) as exc:
                    failure = ConnectionError(str(exc))
        if failure is not None:
            if registered:
                # The reader sees the stream end and answers every pending
                # callback there, not on this caller's thread (it may hold
                # locks those callbacks take)
                try:
                    self._sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            else:
                self._reply(callback, failure)
        return seq

    def call(self, msg_type: int, request: Any, timeout_s: float) -> Any:
        """Wait for one seq-matched response; raise on timeout or connection failure."""
        answered = threading.Event()
        result: list[Any] = []

        def done(response: Any) -> None:
            result.append(response)
            answered.set()

        seq = self.submit(msg_type, request, done)
        if not answered.wait(timeout_s):
            with self._lock:
                self._pending.pop(seq, None)
            raise TimeoutError("remote request timed out")
        response = result.pop()
        if isinstance(response, ConnectionError):
            # Its traceback holds this frame: neither may hold it then
            try:
                raise response
            finally:
                del response
        return response

    def on_close(self, callback: Callable[[], None]) -> None:
        """``callback`` once the stream ends, before the pending callbacks
        get their error (at once if it has ended)."""
        with self._lock:
            if not self.closed:
                self._on_close.append(callback)
                return
        self._closed_hook(callback)

    def close(self) -> None:
        """Close the stream and fail all pending callbacks exactly once."""
        self._fail(ConnectionError("remote connection closed"))
        if threading.current_thread() is not self._reader:
            self._reader.join(timeout=2)

    @staticmethod
    def _reply(callback: Reply, response: Any) -> None:
        try:
            callback(response)
        except Exception:
            logger.debug("remote callback failed", exc_info=True)

    @staticmethod
    def _closed_hook(hook: Callable[[], None]) -> None:
        try:
            hook()
        except Exception:
            logger.debug("remote close hook failed", exc_info=True)

    def _fail(self, error: ConnectionError) -> None:
        with self._lock:
            if self.closed:
                return
            self._closed.set()
            pending, self._pending = self._pending, {}
            closing, self._on_close = self._on_close, []
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._sock.close()
        for hook in closing:
            self._closed_hook(hook)
        for _, callback in pending.values():
            self._reply(callback, error)

    def _read_loop(self) -> None:
        try:
            while not self.closed:
                got = recv_frame(self._sock)
                if got is None:
                    raise ConnectionError("remote connection ended")
                header, response = self._serializer.decode_response(got[0])
                with self._lock:
                    pending = self._pending.get(header.sequence)
                    if pending is not None:
                        if header.msg_type != pending[0]:
                            raise ConnectionError("remote response code mismatch")
                        del self._pending[header.sequence]
                if pending is not None:
                    self._reply(pending[1], response)
        except Exception as exc:
            self._fail(ConnectionError(str(exc)))
