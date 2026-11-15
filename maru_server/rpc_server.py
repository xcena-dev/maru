# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""RpcServer - ZeroMQ-based RPC server for MaruServer."""

import logging
import threading
from typing import TYPE_CHECKING

import zmq

from maru_common import MessageHeader, Serializer

from .rpc_handler_mixin import RpcHandlerMixin

if TYPE_CHECKING:
    from .server import MaruServer

logger = logging.getLogger(__name__)


class RpcServer(RpcHandlerMixin):
    """
    ZeroMQ-based RPC server for MaruServer.

    Handles incoming requests and dispatches them to the appropriate
    MaruServer methods. Uses binary protocol (MessagePack).
    """

    def __init__(self, server: "MaruServer", host: str = "127.0.0.1", port: int = 5555):
        self._server = server
        self._host = host
        self._port = port
        self._context: zmq.Context | None = None
        self._socket: zmq.Socket | None = None
        self._running = False
        self._stopped_event = threading.Event()
        self._serializer = Serializer()

    @property
    def address(self) -> str:
        """Return the server address."""
        return f"tcp://{self._host}:{self._port}"

    def start(self) -> None:
        """Start the RPC server."""
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.REP)
        self._socket.setsockopt(zmq.RCVTIMEO, 100)  # 100ms timeout to check _running
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.bind(self.address)
        self._running = True

        logger.info("RPC Server started on %s", self.address)

        while self._running:
            header = None
            try:
                if self._socket is None:
                    break

                # Receive and decode request
                raw_data = self._socket.recv()
                header, request = self._serializer.decode_request(raw_data)

                # Handle the message
                response = self._handle_message(header.msg_type, request)

                # Send response
                response_data = self._serializer.encode_response(header, response)
                self._socket.send(response_data)

            except zmq.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    continue
                if self._running:
                    logger.error("ZMQ error: %s", e)
            except Exception:
                logger.error("Error handling message", exc_info=True)
                if self._socket:
                    # Send error response preserving sequence from request
                    error_response = {"error": "internal server error"}
                    try:
                        error_header = (
                            header if header is not None else MessageHeader(msg_type=0)
                        )
                        error_data = self._serializer.encode_response(
                            error_header, error_response
                        )
                        self._socket.send(error_data)
                    except Exception:
                        logger.debug("Failed to send error response", exc_info=True)

        # Signal that the loop has exited
        self._stopped_event.set()

    def stop(self) -> None:
        """Stop the RPC server."""
        self._running = False
        # Wait for server loop to exit (it checks _running every 100ms)
        self._stopped_event.wait(timeout=2.0)
        if self._socket:
            self._socket.close()
            self._socket = None
        if self._context:
            self._context.term()
            self._context = None
        logger.info("RPC Server stopped")
