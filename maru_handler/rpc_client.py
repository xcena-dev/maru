# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""RPC Client for connecting to MaruServer."""

import logging
from typing import Any

import zmq

from maru_common import MessageType, Serializer

from .rpc_client_base import RpcClientBase

logger = logging.getLogger(__name__)


class RpcClient(RpcClientBase):
    """
    ZeroMQ-based RPC client for communicating with MaruServer.

    This client uses binary format (MessagePack) for RPC communication.
    The server returns Handle objects for memory locations.
    """

    def __init__(
        self,
        server_url: str = "tcp://localhost:5555",
        timeout_ms: int = 5000,
    ):
        """
        Initialize the RPC client.

        Args:
            server_url: URL of the MaruServer (e.g., "tcp://localhost:5555")
            timeout_ms: Socket timeout in milliseconds (default: 5000ms)
        """
        self._server_url = server_url
        self._timeout_ms = timeout_ms
        self._context: zmq.Context | None = None
        self._socket: zmq.Socket | None = None
        self._serializer = Serializer()

    def connect(self) -> None:
        """Connect to the server."""
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.REQ)
        self._socket.setsockopt(zmq.RCVTIMEO, self._timeout_ms)
        self._socket.setsockopt(zmq.SNDTIMEO, self._timeout_ms)
        self._socket.setsockopt(zmq.LINGER, 0)  # Don't wait on close
        self._socket.connect(self._server_url)
        logger.info("Connected to MaruServer at %s", self._server_url)

    def close(self) -> None:
        """Close the connection."""
        if self._socket:
            self._socket.close()
        if self._context:
            self._context.term()
        logger.info("Disconnected from MaruServer")

    def _send_request(
        self, msg_type: MessageType, data: dict[str, Any]
    ) -> dict[str, Any]:
        """Send a request and wait for response using poll-based timeout."""
        if self._socket is None:
            raise RuntimeError("Client not connected. Call connect() first.")

        try:
            # Encode and send binary message
            encoded = self._serializer.encode(msg_type, data)
            self._socket.send(encoded)

            # Poll for response instead of relying on RCVTIMEO exception
            poller = zmq.Poller()
            poller.register(self._socket, zmq.POLLIN)
            events = dict(poller.poll(self._timeout_ms))

            if self._socket not in events:
                # Timeout: no response within deadline
                logger.error(
                    "Timeout waiting for response from server (msg_type=%s)",
                    msg_type,
                )
                try:
                    self._reset_socket()
                except Exception:
                    logger.warning("Failed to reset socket after timeout")
                return {"error": "timeout", "success": False}

            # Data is ready — recv won't block
            response_data = self._socket.recv(zmq.NOBLOCK)
            _, payload = self._serializer.decode(response_data)
            return payload
        except zmq.ZMQError:
            logger.error("ZMQ error (msg_type=%s)", msg_type, exc_info=True)
            try:
                self._reset_socket()
            except Exception:
                logger.warning("Failed to reset socket after error")
            return {"error": "timeout", "success": False}

    def _reset_socket(self) -> None:
        """Reset socket after timeout (REQ socket needs reset after timeout)."""
        if self._socket:
            self._socket.close()
        self._socket = self._context.socket(zmq.REQ)
        self._socket.setsockopt(zmq.RCVTIMEO, self._timeout_ms)
        self._socket.setsockopt(zmq.SNDTIMEO, self._timeout_ms)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.connect(self._server_url)

    # =========================================================================
    # Context Manager
    # =========================================================================

    def __enter__(self) -> "RpcClient":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
