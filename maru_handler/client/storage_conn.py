# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageConn: the UDS connection to the node's StorageService.

Several requests may be in flight, matched by header sequence; NOTIFY is
handled on its own thread. OpTracker keeps unresolved ops for a resend after
reconnecting to the same run.
"""

import collections
import errno
import logging
import os
import select
import socket
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future
from typing import Any

from maru_common.ms_rpc import OpOrigin
from maru_common.node_protocol import (
    MAX_NODE_FDS,
    NODE_MESSAGE_CLASSES,
    NOTIFY_SEQUENCE,
    AttachRequest,
    AttachResponse,
    NodeMessageType,
    Notify,
    recv_frame,
    send_frame,
    send_frame_nowait,
)
from maru_common.protocol import HEADER_SIZE, MessageHeader
from maru_common.serializer import RESPONSE_BIT, MessageDecodeError, Serializer
from maru_common.storage_types import (
    Status,
    StorageUnavailableError,
)

logger = logging.getLogger(__name__)


class NodeNotListeningError(StorageUnavailableError):
    """No StorageService listens on the socket yet (ENOENT, ECONNREFUSED)."""


class AttachRefusedError(StorageUnavailableError):
    """The node refused the ATTACH (a config or version mismatch, not a
    passing outage)."""


def close_fds(fds: list[int]) -> None:
    """Close the fds; -1 marks one already handed on."""
    for fd in fds:
        if fd < 0:
            continue
        try:
            os.close(fd)
        except OSError:
            pass


class StorageConn:
    """One connection to the StorageService; make a new one to reconnect."""

    def __init__(
        self,
        path: str,
        *,
        on_notify: Callable[[Notify, "StorageConn"], None],
        on_lost: Callable[["StorageConn"], None],
    ):
        self.path = path
        self._on_notify = on_notify
        self._on_lost = on_lost
        self._serializer = Serializer(NODE_MESSAGE_CLASSES)
        self._sock: socket.socket | None = None
        self._send_lock = threading.Lock()
        self._lock = threading.Lock()
        self._pending: dict[int, tuple[int, Future]] = {}
        self._closed = threading.Event()
        self._receiver: threading.Thread | None = None
        self._notifier: threading.Thread | None = None
        self._notices: collections.deque[Notify] = collections.deque()
        self._notices_cv = threading.Condition()
        self.attach_response: AttachResponse | None = None

    # ------------------------------------------------------------- lifecycle

    def connect(
        self, request: AttachRequest, timeout_s: float
    ) -> tuple[AttachResponse, list[int]]:
        """Connect and ATTACH within ``timeout_s``; returns the reply and its
        memfds (caller owns them). StorageUnavailableError if unreachable,
        refused, malformed, timed out or closed meanwhile."""
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM | socket.SOCK_CLOEXEC)
        with self._lock:
            if self._closed.is_set():
                sock.close()
                raise StorageUnavailableError("the connection was closed")
            self._sock = sock  # close() shuts it down
        try:
            sock.settimeout(timeout_s)
            sock.connect(self.path)
        except OSError as e:
            sock.close()
            error = (
                NodeNotListeningError
                if e.errno in (errno.ENOENT, errno.ECONNREFUSED)
                else StorageUnavailableError
            )
            raise error(f"cannot reach the StorageService at {self.path}: {e}") from e
        fds: list[int] = []
        try:
            frame = self._serializer.encode(NodeMessageType.ATTACH, request)
            send_frame(sock, frame)
            seq = MessageHeader.unpack(frame[:HEADER_SIZE]).sequence
            got = recv_frame(sock, max_fds=MAX_NODE_FDS)
            if got is None:
                raise StorageUnavailableError(
                    "the StorageService closed the connection"
                )
            reply, fds = got
            header, response = self._serializer.decode_response(reply)
            if header.sequence != seq or header.msg_type != (
                NodeMessageType.ATTACH | RESPONSE_BIT
            ):
                raise StorageUnavailableError("the ATTACH reply does not match")
            if response.status != Status.OK:
                raise AttachRefusedError(
                    f"ATTACH refused: {response.status} {response.error}"
                )
            wanted = sorted(d.memfd for d in response.pools if d.memfd is not None)
            if wanted != list(range(len(fds))):
                raise StorageUnavailableError(
                    f"the ATTACH reply carried {len(fds)} descriptors for memfd "
                    f"indexes {wanted}"
                )
            sock.settimeout(None)
            with self._lock:
                if self._closed.is_set():
                    raise StorageUnavailableError("closed")
                self.attach_response = response
        except (OSError, ValueError, MessageDecodeError) as e:
            # A timeout leaves the stream out of step: the socket goes too
            close_fds(fds)
            sock.close()
            if self._closed.is_set():
                raise StorageUnavailableError("closed while attaching") from e
            if isinstance(e, StorageUnavailableError):
                raise
            raise StorageUnavailableError(f"ATTACH failed: {e}") from e
        except BaseException:
            close_fds(fds)
            sock.close()
            raise
        self._receiver = threading.Thread(
            target=self._receive_loop, name="maru-storage-recv", daemon=True
        )
        self._notifier = threading.Thread(
            target=self._notify_loop, name="maru-storage-notify", daemon=True
        )
        self._receiver.start()
        return response, fds

    def start_notifications(self) -> None:
        """Start handling NOTIFY (queued until then)."""
        if self._notifier is not None and not self._notifier.is_alive():
            self._notifier.start()

    @property
    def closed(self) -> bool:
        return self._closed.is_set()

    def close(self) -> None:
        """Close the connection; requests in flight fail as unavailable."""
        self._fail("the connection was closed", notify=False)

    # -------------------------------------------------------------- requests

    def submit(
        self, code: NodeMessageType, request: Any, *, deadline: float | None = None
    ) -> Future:
        """Send; the future fails with StorageUnavailableError on a break."""
        future: Future = Future()
        frame = self._serializer.encode(code, request)
        seq = MessageHeader.unpack(frame[:HEADER_SIZE]).sequence
        with self._lock:
            if self._closed.is_set() or self._sock is None:
                future.set_exception(
                    StorageUnavailableError("not connected to the StorageService")
                )
                return future
            self._pending[seq] = (int(code), future)
            sock = self._sock
        sent = 0
        acquired = False
        pending = None
        failed = None
        reason = ""
        try:
            acquired = (
                self._send_lock.acquire()
                if deadline is None
                else self._send_lock.acquire(
                    timeout=max(0.0, deadline - time.monotonic())
                )
            )
            if not acquired:
                raise TimeoutError("send lock deadline")
            if deadline is None:
                send_frame(sock, frame)
            else:
                view = memoryview(frame)
                while sent < len(frame):
                    left = deadline - time.monotonic()
                    if left <= 0:
                        raise TimeoutError("send deadline")
                    try:
                        count = (
                            sock.send(view[sent:], socket.MSG_DONTWAIT)
                            if sent
                            else send_frame_nowait(sock, frame)
                        )
                    except BlockingIOError:
                        select.select([], [sock], [], left)
                        continue
                    if count == 0:
                        select.select([], [sock], [], left)
                        continue
                    sent += count
        except TimeoutError as e:
            reason = f"send failed: {e}"
            if sent:
                pending = self._disconnect()
            else:
                with self._lock:
                    entry = self._pending.pop(seq, None)
                if entry is not None:
                    failed = entry[1]
        except (OSError, ValueError) as e:
            reason = f"send failed: {e}"
            pending = self._disconnect()
        finally:
            if acquired:
                self._send_lock.release()
        if pending is not None:
            self._finish_failure(pending, reason)
        if failed is not None:
            failed.set_exception(StorageUnavailableError(reason))
        return future

    def request(
        self, code: NodeMessageType, request: Any, timeout: float | None = None
    ) -> Any:
        """Send and wait for the response."""
        return self.submit(code, request).result(timeout)

    # -------------------------------------------------------------- receiving

    def _receive_loop(self) -> None:
        reason = "the StorageService closed the connection"
        sock = self._sock
        try:
            while sock is not None and not self._closed.is_set():
                got = recv_frame(sock, max_fds=0)
                if got is None:
                    break
                frame, _ = got
                header = MessageHeader.unpack(frame[:HEADER_SIZE])
                if header.sequence == NOTIFY_SEQUENCE:
                    if header.msg_type not in (
                        NodeMessageType.NOTIFY,
                        NodeMessageType.PEERS,
                    ):
                        reason = f"message 0x{header.msg_type:02X} with sequence 0"
                        break
                    _, notify = self._serializer.decode_request(frame)
                    with self._notices_cv:
                        self._notices.append(notify)
                        self._notices_cv.notify()
                    continue
                with self._lock:
                    entry = self._pending.pop(header.sequence, None)
                if entry is None:
                    logger.warning(
                        "response to an unknown request (seq %d)", header.sequence
                    )
                    continue
                code, future = entry
                if header.msg_type != (code | RESPONSE_BIT):
                    reason = f"response 0x{header.msg_type:02X} to request 0x{code:02X}"
                    future.set_exception(StorageUnavailableError(reason))
                    break
                try:
                    _, response = self._serializer.decode_response(frame)
                except (MessageDecodeError, ValueError) as e:
                    future.set_exception(StorageUnavailableError(f"bad response: {e}"))
                    reason = f"bad response: {e}"
                    break
                future.set_result(response)
        except (OSError, ValueError, MessageDecodeError) as e:
            reason = f"the connection broke: {e}"
        self._fail(reason)

    def _notify_loop(self) -> None:
        while True:
            with self._notices_cv:
                while not self._notices and not self._closed.is_set():
                    self._notices_cv.wait()
                if not self._notices:
                    return
                notify = self._notices.popleft()
            try:
                self._on_notify(notify, self)
            except Exception:
                logger.exception("handling node notification failed")

    def _fail(self, reason: str, notify: bool = True) -> None:
        pending = self._disconnect()
        if pending is not None:
            self._finish_failure(pending, reason, notify)

    def _disconnect(self) -> list[tuple[int, Future]] | None:
        """Close the stream before another sender can use it."""
        with self._lock:
            if self._closed.is_set():
                return None
            self._closed.set()
            pending = list(self._pending.values())
            self._pending.clear()
            sock = self._sock
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
        return pending

    def _finish_failure(
        self, pending: list[tuple[int, Future]], reason: str, notify: bool = True
    ) -> None:
        """Notify callers without holding the send lock."""
        for _, future in pending:
            if not future.done():
                future.set_exception(StorageUnavailableError(reason))
        with self._notices_cv:
            self._notices_cv.notify_all()
        logger.info("StorageService connection closed: %s", reason)
        if notify:
            self._on_lost(self)


class OpTracker:
    """Unresolved ops of one StorageService run."""

    def __init__(self, incarnation: str, node_epoch: str):
        self.origin = OpOrigin(incarnation)
        self.node_epoch = node_epoch
        self._lock = threading.Lock()
        self._unresolved: dict[int, tuple[NodeMessageType, Any, Callable]] = {}

    def track(self, code: NodeMessageType, request: Any, on_result: Callable) -> None:
        with self._lock:
            self._unresolved[request.op.seq] = (code, request, on_result)

    def update(self, request: Any) -> bool:
        """Store a refreshed copy for a resend; False if already resolved."""
        with self._lock:
            entry = self._unresolved.get(request.op.seq)
            if entry is None:
                return False
            self._unresolved[request.op.seq] = (entry[0], request, entry[2])
            return True

    def resolved(self, seq: int) -> bool:
        """Mark ``seq`` resolved; True only for the first result."""
        with self._lock:
            first = self._unresolved.pop(seq, None) is not None
        self.origin.resolved(seq)
        return first

    def unresolved(self) -> list[tuple[NodeMessageType, Any, Callable]]:
        with self._lock:
            return list(self._unresolved.values())
