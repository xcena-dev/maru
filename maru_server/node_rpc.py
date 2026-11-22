# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""NodeRpcServer: the node server's ZeroMQ front end.

One thread owns a ROUTER socket facing StorageServices and workers and
answers each request in the order it came. The MsService serializes requests
under one lock, so more threads would only trade the GIL and add hand-offs.
A request waits in the socket while earlier ones are answered: a worker that
gives up first counts a miss, a node sends its op again.

Multi-storage requests (0x40-0x5F) go to the MsService, HANDSHAKE tells the
server epoch, and any other request is refused: the storage backends are
served by maru-server.
"""

import logging
import threading
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
# how long the loop waits for a request before it looks at stop() again
POLL_MS = 100


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
        MessageType.FETCH_COMPLETE.value: ms.fetch_complete,
    }


class NodeRpcServer:
    """Serves on the thread that calls start(); stop() may come from any
    thread, before start() too. ``server.ms_service`` must be thread safe
    (its maintenance thread runs beside this one)."""

    def __init__(
        self,
        server: "NodeServer",
        host: str = "127.0.0.1",
        port: int = 5556,
    ):
        self._server = server
        self._host = host
        self._port = port
        self._serializer = Serializer()
        self._ms_handlers: dict[int, Callable[[Any, dict], Any]] | None = None
        # start() and stop() change these together; a stop before the start
        # is kept, so a signal during start-up is not lost
        self._state = threading.Lock()
        self._stop_requested = False
        self._running = False

    @property
    def address(self) -> str:
        return f"tcp://{self._host}:{self._port}"

    def start(self) -> None:
        """Serve until stop() is called; after a stop() it returns at once.
        The socket and its context are closed when it returns."""
        with self._state:
            if self._stop_requested:
                return
            context = zmq.Context()
            try:
                frontend = context.socket(zmq.ROUTER)
            except BaseException:
                context.term()
                raise
            try:
                frontend.setsockopt(zmq.LINGER, 0)
                frontend.bind(self.address)
            except BaseException:
                frontend.close()
                context.term()
                raise
            self._running = True
        logger.info("node server started on %s", self.address)
        try:
            self._serve(frontend)
        finally:
            frontend.close()
            context.term()
            logger.info("node server RPC stopped")

    def stop(self) -> None:
        """start() returns within POLL_MS, after the request in hand."""
        with self._state:
            self._stop_requested = True
            self._running = False

    def _serve(self, frontend: zmq.Socket) -> None:
        while self._running:
            try:
                if not frontend.poll(POLL_MS):
                    continue
                # every frame that is ready, then poll again
                while self._running:
                    try:
                        *envelope, raw_data = frontend.recv_multipart(zmq.NOBLOCK)
                    except zmq.Again:
                        break
                    reply = self._answer(raw_data)
                    if reply is not None:
                        frontend.send_multipart([*envelope, reply])
            except zmq.ZMQError as e:
                logger.error("node server ZMQ error: %s", e)
                break

    def _answer(self, raw_data: bytes) -> bytes | None:
        try:
            return self._reply(self._serializer, raw_data)
        except Exception:
            logger.error("node server: request failed", exc_info=True)
            return None

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
