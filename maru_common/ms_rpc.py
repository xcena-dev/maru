# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Typed MaruServer RPC for the multi-storage messages and the sender side of
ops (see OpRef)."""

import collections
import dataclasses
import logging
import os
import threading
from typing import Any

import zmq

from .protocol import HEADER_SIZE, MS_MESSAGE_CLASSES, MessageHeader, MessageType
from .serializer import RESPONSE_BIT, MessageDecodeError, Serializer, op_digest
from .storage_types import OpRef

logger = logging.getLogger(__name__)

# Stand-in until stamped; op_digest ignores the op field
PLACEHOLDER_OP = OpRef(origin="", seq=0, digest=b"", ack=0)
_UNSENT_RETRY_S = 0.01


class RpcUnknownError(Exception):
    """No valid response in time: the request may or may not have run (UNKNOWN)."""


class OpOrigin:
    """Sender side of ops: seq from 1; ack never passes an unresolved seq."""

    def __init__(self, origin: str):
        self.origin = origin
        self._lock = threading.Lock()
        self._last = 0
        self._outstanding: set[int] = set()

    def stamp(self, request: Any) -> Any:
        """``request`` with the next op."""
        digest = op_digest(request)
        with self._lock:
            ack = self._ack_locked()
            self._last += 1
            seq = self._last
            self._outstanding.add(seq)
        return dataclasses.replace(
            request, op=OpRef(origin=self.origin, seq=seq, digest=digest, ack=ack)
        )

    def refresh(self, request: Any) -> Any:
        """``request`` with its op's ack brought up to date for a resend (the
        ack is not in the digest; it lets the receiver free its window)."""
        return dataclasses.replace(
            request, op=dataclasses.replace(request.op, ack=self.ack)
        )

    def resolved(self, seq: int) -> None:
        """The result of ``seq`` is known, or will never be needed."""
        with self._lock:
            self._outstanding.discard(seq)

    @property
    def ack(self) -> int:
        with self._lock:
            return self._ack_locked()

    @property
    def outstanding(self) -> set[int]:
        with self._lock:
            return set(self._outstanding)

    def _ack_locked(self) -> int:
        if self._outstanding:
            return min(self._outstanding) - 1
        return self._last


class _Waiter:
    __slots__ = ("event", "frame", "error")

    def __init__(self):
        self.event = threading.Event()
        self.frame: bytes | None = None
        self.error: str | None = None


class MsRpcClient:
    """A DEALER connection to MaruServer shared by many threads.

    ZMQ_IMMEDIATE: a request never waits for a connection past its own
    timeout, so MaruServer never runs it long after the sender gave up.
    """

    def __init__(
        self,
        url: str,
        *,
        timeout_s: float = 2.0,
        message_classes: dict | None = None,
        strict_codes=None,
        name: str = "maru-ms-rpc",
    ):
        if timeout_s <= 0:
            raise ValueError("timeout_s must be positive")
        self.url = url
        self.timeout_s = timeout_s
        self._serializer = Serializer(
            MS_MESSAGE_CLASSES if message_classes is None else message_classes,
            strict_codes,
        )
        self._lock = threading.Lock()
        self._pending: dict[int, _Waiter] = {}
        self._outbox: collections.deque[tuple[int, bytes]] = collections.deque()
        self._closed = False
        self._wake_r, self._wake_w = os.pipe()
        os.set_blocking(self._wake_r, False)
        os.set_blocking(self._wake_w, False)
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.DEALER)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.setsockopt(zmq.IMMEDIATE, 1)
        self._socket.connect(url)
        self._thread = threading.Thread(target=self._io_loop, name=name, daemon=True)
        self._thread.start()

    def call(
        self, msg_type: MessageType, request: Any, timeout_s: float | None = None
    ) -> Any:
        """Send ``request`` and return the response; RpcUnknownError otherwise."""

        frame = self._serializer.encode(msg_type, request)
        seq = MessageHeader.unpack(frame[:HEADER_SIZE]).sequence
        waiter = _Waiter()
        with self._lock:
            if self._closed:
                raise RpcUnknownError("the MaruServer connection is closed")
            self._pending[seq] = waiter
            self._outbox.append((seq, frame))
        self._wake()
        if not waiter.event.wait(self.timeout_s if timeout_s is None else timeout_s):
            with self._lock:
                self._pending.pop(seq, None)
                self._outbox = collections.deque(
                    item for item in self._outbox if item[0] != seq
                )
            raise RpcUnknownError(
                f"no response from {self.url} (0x{int(msg_type):02X})"
            )
        if waiter.error is not None or waiter.frame is None:
            raise RpcUnknownError(waiter.error or "no response")
        try:
            header, response = self._serializer.decode_response(waiter.frame)
        except (MessageDecodeError, ValueError) as e:
            raise RpcUnknownError(f"undecodable response: {e}") from e
        if header.msg_type != (int(msg_type) | RESPONSE_BIT):
            raise RpcUnknownError(
                f"response code 0x{header.msg_type:02X} to request 0x{int(msg_type):02X}"
            )
        return response

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            waiters = list(self._pending.values())
            self._pending.clear()
            self._outbox.clear()
        for waiter in waiters:
            waiter.error = "the MaruServer connection is closed"
            waiter.event.set()
        self._wake()
        self._thread.join(timeout=5.0)
        self._socket.close()
        self._context.term()
        os.close(self._wake_r)
        os.close(self._wake_w)

    def _wake(self) -> None:
        try:
            os.write(self._wake_w, b"\0")
        except (BlockingIOError, OSError):
            pass  # a wake-up is already pending, or the client is closing

    def _io_loop(self) -> None:
        poller = zmq.Poller()
        poller.register(self._socket, zmq.POLLIN)
        poller.register(self._wake_r, zmq.POLLIN)
        while True:
            with self._lock:
                if self._closed:
                    return
                unsent = bool(self._outbox)
            timeout_ms = int(_UNSENT_RETRY_S * 1000) if unsent else None
            try:
                events = dict(poller.poll(timeout_ms))
            except zmq.ZMQError:
                return
            if self._wake_r in events:
                try:
                    while os.read(self._wake_r, 4096):
                        pass
                except BlockingIOError:
                    pass
            if self._socket in events:
                self._receive_all()
            self._send_all()

    def _send_all(self) -> None:
        while True:
            with self._lock:
                if self._closed or not self._outbox:
                    return
                seq, frame = self._outbox[0]
            try:
                self._socket.send_multipart([b"", frame], flags=zmq.NOBLOCK)
            except zmq.Again:
                return  # no connection yet; tried again on the next poll
            except zmq.ZMQError as e:
                with self._lock:
                    waiter = self._pending.pop(seq, None)
                    if self._outbox and self._outbox[0][0] == seq:
                        self._outbox.popleft()
                if waiter is not None:
                    waiter.error = f"send failed: {e}"
                    waiter.event.set()
                continue
            with self._lock:
                if self._outbox and self._outbox[0][0] == seq:
                    self._outbox.popleft()

    def _receive_all(self) -> None:
        while True:
            try:
                frames = self._socket.recv_multipart(flags=zmq.NOBLOCK)
            except zmq.Again:
                return
            except zmq.ZMQError:
                return
            frame = frames[-1]
            if len(frame) < HEADER_SIZE:
                logger.warning("MaruServer sent a frame shorter than a header")
                continue
            seq = MessageHeader.unpack(frame[:HEADER_SIZE]).sequence
            with self._lock:
                waiter = self._pending.pop(seq, None)
            if waiter is None:
                logger.debug("late or unknown response (seq=%d) dropped", seq)
                continue
            waiter.frame = frame
            waiter.event.set()
