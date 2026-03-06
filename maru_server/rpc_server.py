# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""RpcServer - ZeroMQ-based RPC server for MaruServer."""

import logging
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import zmq

from maru_common import MessageHeader, MessageType, Serializer

if TYPE_CHECKING:
    from .server import MaruServer

logger = logging.getLogger(__name__)


class RpcServer:
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

    def _handle_message(self, msg_type: int, request: Any) -> dict:
        """Dispatch message to appropriate handler."""
        handlers: dict[int, Callable[[Any], dict]] = {
            MessageType.REQUEST_ALLOC.value: self._handle_request_alloc,
            MessageType.RETURN_ALLOC.value: self._handle_return_alloc,
            MessageType.LIST_ALLOCATIONS.value: self._handle_list_allocations,
            MessageType.REGISTER_KV.value: self._handle_register_kv,
            MessageType.LOOKUP_KV.value: self._handle_lookup_kv,
            MessageType.EXISTS_KV.value: self._handle_exists_kv,
            MessageType.DELETE_KV.value: self._handle_delete_kv,
            # Batch operations
            MessageType.BATCH_REGISTER_KV.value: self._handle_batch_register_kv,
            MessageType.BATCH_LOOKUP_KV.value: self._handle_batch_lookup_kv,
            MessageType.BATCH_EXISTS_KV.value: self._handle_batch_exists_kv,
            # Admin
            MessageType.GET_STATS.value: self._handle_get_stats,
            MessageType.HEARTBEAT.value: self._handle_heartbeat,
        }

        handler = handlers.get(msg_type)
        if handler is None:
            return {"error": f"Unknown message type: {msg_type}"}

        return handler(request)

    # =========================================================================
    # Allocation Handlers
    # =========================================================================

    def _handle_request_alloc(self, req: Any) -> dict:
        handle = self._server.request_alloc(
            instance_id=req.instance_id,
            size=req.size,
        )
        if handle is None:
            logger.debug(
                "[REQUEST_ALLOC] instance=%s, size=%d -> FAILED",
                req.instance_id,
                req.size,
            )
            return {"success": False, "error": "Allocation failed"}
        logger.debug(
            "[REQUEST_ALLOC] instance=%s, size=%d -> region_id=%d",
            req.instance_id,
            req.size,
            handle.region_id,
        )
        return {"success": True, "handle": handle.to_dict()}

    def _handle_return_alloc(self, req: Any) -> dict:
        success = self._server.return_alloc(
            instance_id=req.instance_id,
            region_id=req.region_id,
        )
        return {"success": success}

    def _handle_list_allocations(self, req: Any) -> dict:
        handles = self._server.list_allocations(
            exclude_instance_id=req.exclude_instance_id,
        )
        logger.debug(
            "[LIST_ALLOCATIONS] exclude=%s -> %d regions",
            req.exclude_instance_id,
            len(handles),
        )
        return {
            "success": True,
            "allocations": [h.to_dict() for h in handles],
        }

    # =========================================================================
    # KV Handlers
    # =========================================================================

    def _handle_register_kv(self, req: Any) -> dict:
        logger.debug(
            "[PUT] key=%d, region_id=%d, kv_offset=%d, kv_length=%d",
            req.key,
            req.region_id,
            req.kv_offset,
            req.kv_length,
        )
        is_new = self._server.register_kv(
            key=req.key,
            region_id=req.region_id,
            kv_offset=req.kv_offset,
            kv_length=req.kv_length,
        )
        return {"success": True, "is_new": is_new}

    def _handle_lookup_kv(self, req: Any) -> dict:
        result = self._server.lookup_kv(key=req.key)
        if result is None:
            logger.debug("[GET] key=%d -> NOT FOUND", req.key)
            return {"found": False}
        logger.debug(
            "[GET] key=%d -> region_id=%d, kv_offset=%d, kv_length=%d",
            req.key,
            result["handle"].region_id,
            result["kv_offset"],
            result["kv_length"],
        )
        return {
            "found": True,
            "handle": result["handle"].to_dict(),
            "kv_offset": result["kv_offset"],
            "kv_length": result["kv_length"],
        }

    def _handle_exists_kv(self, req: Any) -> dict:
        exists = self._server.exists_kv(key=req.key)
        return {"exists": exists}

    def _handle_delete_kv(self, req: Any) -> dict:
        success = self._server.delete_kv(key=req.key)
        return {"success": success}

    # =========================================================================
    # Batch KV Handlers
    # =========================================================================

    def _handle_batch_register_kv(self, req: Any) -> dict:
        """Handle batch register KV request."""
        entries = [(e.key, e.region_id, e.kv_offset, e.kv_length) for e in req.entries]
        logger.debug("[BATCH_PUT] %d entries", len(entries))
        results = self._server.batch_register_kv(entries)
        return {"success": True, "results": results}

    def _handle_batch_lookup_kv(self, req: Any) -> dict:
        """Handle batch lookup KV request."""
        keys = req.keys
        logger.debug("[BATCH_GET] %d keys", len(keys))
        results = self._server.batch_lookup_kv(keys)

        entries = []
        for result in results:
            if result is None:
                entries.append({"found": False})
            else:
                entries.append(
                    {
                        "found": True,
                        "handle": result["handle"].to_dict(),
                        "kv_offset": result["kv_offset"],
                        "kv_length": result["kv_length"],
                    }
                )
        return {"entries": entries}

    def _handle_batch_exists_kv(self, req: Any) -> dict:
        """Handle batch exists KV request."""
        keys = req.keys
        logger.debug("[BATCH_EXISTS] %d keys", len(keys))
        results = self._server.batch_exists_kv(keys)
        return {"results": results}

    # =========================================================================
    # Admin Handlers
    # =========================================================================

    def _handle_get_stats(self, _req: Any) -> dict:
        stats = self._server.get_stats()
        return stats

    def _handle_heartbeat(self, _req: Any) -> dict:
        return {}
