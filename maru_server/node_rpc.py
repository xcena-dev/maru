# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""NodeRpcServer: the node server's ZeroMQ front end.

A ROUTER socket faces StorageServices and workers. A proxy thread puts each
request on one queue; a worker thread takes the next request only once it is
done with its last, so a slow request delays only the requests that find
every worker busy, and a full queue is answered BUSY at once. Workers push
their replies, envelope kept, back to the proxy.

Multi-storage requests (0x40-0x5F) go to the MsService, HANDSHAKE tells the
server epoch, and any other request is refused: the storage backends are
served by maru-server.
"""

import logging
import queue
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import zmq

from maru_common import MessageHeader, MessageType, Serializer
from maru_common.protocol import HEADER_SIZE, MS_MESSAGE_CLASSES
from maru_common.serializer import MessageDecodeError
from maru_common.storage_types import Status

if TYPE_CHECKING:
    from .ms_service import MsService
    from .node_server import NodeServer

logger = logging.getLogger(__name__)

_MS_CODES = frozenset(int(code) for code in MS_MESSAGE_CLASSES)
_REPLY_ENDPOINT = "inproc://node-replies"
WORKERS = 4
# requests held while every worker is busy
QUEUE_MAX = 10000
# frames the proxy moves from one socket before it polls again
DRAIN_MAX = 64
# how long start() waits for every worker to connect
CONNECT_TIMEOUT_S = 10.0


def is_ms_frame(frame: bytes) -> bool:
    """A multi-storage request (0x40-0x5F), by its header's msg_type byte."""
    return len(frame) >= HEADER_SIZE and frame[3] in _MS_CODES


def ms_handlers(ms: "MsService") -> dict[int, Callable[[Any, dict], Any]]:
    """MsService handlers by message code; each takes (request, raw
    payload) and returns the response."""
    return {
        MessageType.NODE_OPEN.value: ms.node_open,
        MessageType.NODE_HEARTBEAT.value: ms.node_heartbeat,
        MessageType.NODE_CLOSE.value: ms.node_close,
        MessageType.NODE_RECOVER_LIST.value: ms.node_recover_list,
        MessageType.WORKER_EXIT.value: ms.worker_exit,
        MessageType.ADMIN_DELETE.value: ms.admin_delete,
        MessageType.WORKER_OPEN.value: ms.worker_open,
        MessageType.WORKER_HEARTBEAT.value: ms.worker_heartbeat,
        MessageType.WORKER_CLOSE.value: ms.worker_close,
        MessageType.PROBE.value: ms.probe,
        MessageType.LOOKUP_AND_PIN.value: ms.lookup_and_pin,
        MessageType.UNPIN.value: ms.unpin,
        MessageType.SOURCE_PIN.value: ms.source_pin,
        MessageType.COMMIT.value: ms.commit,
        MessageType.RETIRE.value: ms.retire,
        MessageType.FINALIZE_MOVE.value: ms.finalize_move,
        MessageType.FETCH_COMPLETE.value: ms.fetch_complete,
    }


class NodeRpcServer:
    """ROUTER frontend, a request queue its idle workers take from, and a
    PULL the workers push replies to. ``server.ms_service`` must be thread
    safe; once started, the proxy thread owns the frontend and the PULL."""

    def __init__(
        self,
        server: "NodeServer",
        host: str = "127.0.0.1",
        port: int = 5556,
        num_workers: int = WORKERS,
        queue_max: int = QUEUE_MAX,
        connect_timeout_s: float = CONNECT_TIMEOUT_S,
    ):
        if num_workers < 1:
            raise ValueError("the node server needs at least one worker thread")
        self._server = server
        self._host = host
        self._port = port
        self._num_workers = num_workers
        self._queue_max = queue_max
        self._connect_timeout_s = connect_timeout_s
        self._context: zmq.Context | None = None
        self._frontend: zmq.Socket | None = None
        self._replies: zmq.Socket | None = None
        self._owns_sockets = False  # set by start(): the proxy closes them
        self._running = False
        self._stop_event = threading.Event()
        self._proxy_thread: threading.Thread | None = None
        self._worker_threads: list[threading.Thread] = []
        self._proxy_serializer = Serializer()
        self._connected = threading.Semaphore(0)  # one release per worker
        self._requests: queue.Queue[list[bytes]] = queue.Queue()
        self._ms_handlers: dict[int, Callable[[Any, dict], Any]] | None = None
        # start() and stop() change these together; a stop before the start
        # is kept, so a signal during start-up is not lost
        self._state = threading.Lock()
        self._stop_requested = False

    @property
    def address(self) -> str:
        return f"tcp://{self._host}:{self._port}"

    def start(self) -> None:
        """Serve until stop() is called; after a stop() it returns at once."""
        with self._state:
            if self._stop_requested:
                return
            self._context = zmq.Context()
            try:
                self._frontend = self._context.socket(zmq.ROUTER)
                self._frontend.setsockopt(zmq.LINGER, 0)
                self._frontend.bind(self.address)
                self._replies = self._context.socket(zmq.PULL)
                self._replies.setsockopt(zmq.LINGER, 0)
                self._replies.bind(_REPLY_ENDPOINT)
            except BaseException:
                self._close_sockets()
                self._context.term()
                self._context = None
                raise
            self._owns_sockets = True
            self._running = True
            self._stop_event.clear()
            self._connected = threading.Semaphore(0)
            self._requests = queue.Queue()
            for i in range(self._num_workers):
                t = threading.Thread(
                    target=self._worker_routine,
                    args=(i,),
                    name=f"node-rpc-worker-{i}",
                    daemon=True,
                )
                self._worker_threads.append(t)
                t.start()

        # Forward only once every worker is connected; requests wait at the
        # frontend meanwhile
        deadline = time.monotonic() + self._connect_timeout_s
        for _ in range(self._num_workers):
            while self._running and not self._connected.acquire(timeout=0.05):
                if time.monotonic() >= deadline:
                    self.stop()
                    raise RuntimeError(
                        f"RPC workers did not connect within "
                        f"{self._connect_timeout_s:g} s"
                    )
        if not self._running:  # stopped while starting
            self._stop_event.wait()
            return

        self._proxy_thread = threading.Thread(
            target=self._proxy_routine, name="node-rpc-proxy", daemon=True
        )
        self._proxy_thread.start()
        logger.info(
            "node server started on %s with %d workers", self.address, self._num_workers
        )
        self._stop_event.wait()

    def stop(self) -> None:
        with self._state:
            self._stop_requested = True
            self._running = False
        for t in self._worker_threads:
            if t.is_alive():
                t.join(timeout=2.0)
        self._worker_threads.clear()

        if self._proxy_thread and self._proxy_thread.is_alive():
            self._proxy_thread.join(timeout=2.0)
        proxy_alive = bool(self._proxy_thread and self._proxy_thread.is_alive())
        if proxy_alive and self._owns_sockets:
            # its sockets are its own to close: leave them and the context
            logger.warning("RPC proxy thread did not stop; leaving its sockets")
            self._stop_event.set()
            return

        self._close_sockets()
        # Unblock start() before context.term(), which would wait for it
        self._stop_event.set()
        if self._context:
            self._context.term()
            self._context = None
        logger.info("node server RPC stopped")

    # =========================================================================
    # Proxy and workers
    # =========================================================================

    def _close_sockets(self) -> None:
        for name in ("_frontend", "_replies"):
            sock = getattr(self, name)
            if sock is not None:
                sock.close()
                setattr(self, name, None)

    def _proxy_routine(self) -> None:
        try:
            self._proxy_loop()
        finally:
            if self._owns_sockets:
                self._close_sockets()

    def _proxy_loop(self) -> None:
        frontend, replies, requests = self._frontend, self._replies, self._requests
        assert frontend is not None and replies is not None
        poller = zmq.Poller()
        poller.register(frontend, zmq.POLLIN)
        poller.register(replies, zmq.POLLIN)

        def more(sock: zmq.Socket) -> bool:
            return bool(int(sock.getsockopt(zmq.EVENTS)) & zmq.POLLIN)

        while self._running:
            try:
                socks: dict[Any, int] = dict(poller.poll(100))
            except zmq.ZMQError:
                break
            try:
                # up to DRAIN_MAX frames per socket and poll: fewer GIL
                # hand-offs under load
                if replies in socks:
                    for _ in range(DRAIN_MAX):
                        frontend.send_multipart(replies.recv_multipart(zmq.NOBLOCK))
                        if not more(replies):
                            break
                if frontend in socks:
                    for _ in range(DRAIN_MAX):
                        msg = frontend.recv_multipart(zmq.NOBLOCK)
                        if requests.qsize() >= self._queue_max:
                            self._reply_busy(frontend, msg)
                        else:
                            requests.put(msg)
                        if not more(frontend):
                            break
            except zmq.ZMQError:
                break

    def _reply_busy(self, frontend: zmq.Socket, msg: list[bytes]) -> None:
        """The queue is full: BUSY for multi-storage, an error otherwise."""
        frame = msg[-1] if msg else b""
        if len(frame) < HEADER_SIZE:
            return
        header = MessageHeader.unpack(frame[:HEADER_SIZE])
        if is_ms_frame(frame):
            _, response_cls = MS_MESSAGE_CLASSES[MessageType(header.msg_type)]
            reply = response_cls(status=Status.BUSY, error="server busy; resend later")
        else:
            reply = {"error": "server busy"}
        body = self._proxy_serializer.encode_response(header, reply)
        frontend.send_multipart(msg[:-1] + [body])

    def _worker_routine(self, worker_id: int) -> None:
        """Take the next request whenever idle; push the reply, envelope
        kept, to the proxy."""
        assert self._context is not None
        socket = self._context.socket(zmq.PUSH)
        socket.setsockopt(zmq.LINGER, 0)
        # a PUSH without a reader blocks: give up once the proxy is gone
        socket.setsockopt(zmq.SNDTIMEO, 100)
        socket.connect(_REPLY_ENDPOINT)
        self._connected.release()
        serializer = Serializer()

        while self._running:
            try:
                *envelope, raw_data = self._requests.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                reply = self._reply(serializer, raw_data)
                if reply is not None:
                    socket.send_multipart([*envelope, reply])
            except zmq.Again:
                logger.warning("worker %d: the proxy took no reply", worker_id)
            except zmq.ZMQError as e:
                if self._running:
                    logger.error("worker %d ZMQ error: %s", worker_id, e)
                break
            except Exception:
                logger.error("worker %d: request failed", worker_id, exc_info=True)
        socket.close()

    # =========================================================================
    # Requests
    # =========================================================================

    def _reply(self, serializer: Serializer, raw_data: bytes) -> bytes | None:
        """The encoded reply; None for a frame with no readable header."""
        if is_ms_frame(raw_data):
            return self._process_ms_frame(serializer, raw_data)
        if len(raw_data) < HEADER_SIZE:
            return None
        header = MessageHeader.unpack(raw_data[:HEADER_SIZE])
        response: Any
        if header.msg_type == MessageType.HANDSHAKE.value:
            response = {
                "success": True,
                "server_epoch": self._server.server_epoch,
                "capabilities": [],
            }
        else:
            response = {
                "error": f"message 0x{header.msg_type:02X} is not served here: "
                "the storage backends use maru-server"
            }
        return serializer.encode_response(header, response)

    def _get_ms_handlers(self) -> dict[int, Callable[[Any, dict], Any]]:
        if self._ms_handlers is None:
            self._ms_handlers = ms_handlers(self._server.ms_service)
        return self._ms_handlers

    def _process_ms_frame(self, serializer: Serializer, raw_data: bytes) -> bytes:
        """Strict decode; a malformed payload or a failed handler gets the
        request's response with REJECTED. Raises only for a header that is
        not a valid Maru header."""
        try:
            decoded = serializer.decode_request_payload(raw_data)
        except MessageDecodeError as e:
            # A payload that is no MessagePack, or shorter than its header
            # says, comes without the header: read it here
            header = e.header or MessageHeader.unpack(raw_data[:HEADER_SIZE])
            if not header.validate() or header.msg_type not in _MS_CODES:
                raise
            logger.debug("rejecting a malformed request: %s", e)
            _, response_cls = MS_MESSAGE_CLASSES[MessageType(header.msg_type)]
            return serializer.encode_response(
                header, response_cls(status=Status.REJECTED, error=str(e))
            )
        try:
            self._server.ms_service.ensure_started()
            handler = self._get_ms_handlers()[decoded.header.msg_type]
            response = handler(decoded.request, decoded.payload)
        except Exception as e:
            logger.error(
                "multi-storage request 0x%02X failed",
                decoded.header.msg_type,
                exc_info=True,
            )
            _, response_cls = MS_MESSAGE_CLASSES[MessageType(decoded.header.msg_type)]
            response = response_cls(
                status=Status.REJECTED, error=str(e) or "internal server error"
            )
        return serializer.encode_response(decoded.header, response)
