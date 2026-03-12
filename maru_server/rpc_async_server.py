# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""RpcAsyncServer - Async ZeroMQ RPC server using ROUTER/DEALER proxy with worker pool.

Uses zmq.proxy to distribute incoming requests across N worker threads,
enabling concurrent request handling while maintaining the same interface
as the synchronous RpcServer.

Architecture::

    ROUTER socket (tcp://host:port, frontend)
        |
        zmq.proxy() — runs in proxy_thread (blocking)
        |
    DEALER socket (inproc://workers, backend)
        |
        ├── Worker 0 (REP socket, own Serializer)
        ├── Worker 1 (REP socket, own Serializer)
        └── Worker N-1 (REP socket, own Serializer)
"""

import logging
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import zmq

from maru_common import MessageHeader, MessageType, Serializer
from maru_common.protocol import NewAllocationNotification

if TYPE_CHECKING:
    from maru_shm import MaruHandle

    from .server import MaruServer

logger = logging.getLogger(__name__)

_WORKER_ENDPOINT = "inproc://workers"


class RpcAsyncServer:
    """
    Async ZeroMQ RPC server with ROUTER/DEALER proxy and worker pool.

    Handles incoming requests via a ROUTER socket, distributes them
    through zmq.proxy to a pool of REP worker threads, each with its
    own Serializer instance. Provides the same interface as RpcServer.

    Thread-safety contract:
        The ``server`` (MaruServer) passed to __init__ MUST be
        thread-safe, as worker threads call its methods concurrently.
        MaruServer uses an internal RLock for KV operations.
        ``request_alloc`` / ``return_alloc`` delegate to AllocationManager
        which must also be thread-safe (protected by its own lock).
    """

    def __init__(
        self,
        server: "MaruServer",
        host: str = "127.0.0.1",
        port: int = 5555,
        num_workers: int = 4,
    ):
        self._server = server
        self._host = host
        self._port = port
        self._num_workers = num_workers
        self._context: zmq.Context | None = None
        self._frontend: zmq.Socket | None = None
        self._backend: zmq.Socket | None = None
        self._pub_socket: zmq.Socket | None = None
        self._pub_serializer: Serializer | None = None
        self._running = False
        self._stop_event = threading.Event()
        self._proxy_thread: threading.Thread | None = None
        self._worker_threads: list[threading.Thread] = []

    @property
    def address(self) -> str:
        """Return the server address."""
        return f"tcp://{self._host}:{self._port}"

    def start(self) -> None:
        """Start the RPC async server (blocks until stop() is called)."""
        self._context = zmq.Context()

        # Frontend: ROUTER socket facing clients
        self._frontend = self._context.socket(zmq.ROUTER)
        self._frontend.setsockopt(zmq.LINGER, 0)
        self._frontend.bind(self.address)

        # Backend: DEALER socket facing workers
        self._backend = self._context.socket(zmq.DEALER)
        self._backend.setsockopt(zmq.LINGER, 0)
        self._backend.bind(_WORKER_ENDPOINT)

        # PUB socket for allocation notifications (port + 1)
        self._pub_socket = self._context.socket(zmq.PUB)
        self._pub_socket.setsockopt(zmq.LINGER, 0)
        pub_address = f"tcp://{self._host}:{self._port + 1}"
        self._pub_socket.bind(pub_address)
        self._pub_serializer = Serializer()
        logger.info("PUB notification socket bound on %s", pub_address)

        # Wire up MaruServer → PUB socket notification
        self._server.set_notification_callback(self.publish_notification)

        self._running = True
        self._stop_event.clear()

        # Start worker threads
        for i in range(self._num_workers):
            t = threading.Thread(
                target=self._worker_routine,
                args=(i,),
                name=f"rpc-worker-{i}",
                daemon=True,
            )
            self._worker_threads.append(t)
            t.start()

        # Start proxy in a separate thread
        self._proxy_thread = threading.Thread(
            target=self._proxy_routine,
            name="rpc-proxy",
            daemon=True,
        )
        self._proxy_thread.start()

        logger.info(
            "RPC Async Server started on %s with %d workers",
            self.address,
            self._num_workers,
        )

        # Block until stop() is called
        self._stop_event.wait()

    def stop(self) -> None:
        """Gracefully stop the RPC async server."""
        self._running = False

        # 1. Wait for worker threads to notice _running=False and exit
        for t in self._worker_threads:
            if t.is_alive():
                t.join(timeout=2.0)
        self._worker_threads.clear()

        # 2. Close sockets (unblocks proxy poll loop)
        if self._frontend:
            self._frontend.close()
            self._frontend = None
        if self._backend:
            self._backend.close()
            self._backend = None
        if self._pub_socket:
            self._pub_socket.close()
            self._pub_socket = None

        # 3. Wait for proxy thread to exit
        if self._proxy_thread and self._proxy_thread.is_alive():
            self._proxy_thread.join(timeout=2.0)

        # 4. Unblock start() BEFORE context.term() to prevent hang
        self._stop_event.set()

        # 5. Terminate ZMQ context
        if self._context:
            self._context.term()
            self._context = None

        logger.info("RPC Async Server stopped")

    # =========================================================================
    # PUB/SUB Notification
    # =========================================================================

    def publish_notification(
        self, instance_id: str, handle: "MaruHandle"
    ) -> None:
        """Publish a new-allocation notification to all subscribers.

        Called by MaruServer after a successful request_alloc().
        Thread-safe: PUB socket send is atomic in ZMQ.

        Args:
            instance_id: Instance that received the allocation.
            handle: The newly allocated region handle.
        """
        if self._pub_socket is None or self._pub_serializer is None:
            return
        try:
            data = {"instance_id": instance_id, "handle": handle.to_dict()}
            encoded = self._pub_serializer.encode(
                MessageType.NOTIFY_NEW_ALLOCATION, data, seq=0
            )
            self._pub_socket.send(encoded, zmq.NOBLOCK)
            logger.debug(
                "Published new-allocation notification: instance=%s region=%d",
                instance_id,
                handle.region_id,
            )
        except zmq.ZMQError as e:
            logger.warning("Failed to publish notification: %s", e)

    # =========================================================================
    # Internal Routines
    # =========================================================================

    def _proxy_routine(self) -> None:
        """Poll-based proxy bridging frontend ROUTER to backend DEALER."""
        poller = zmq.Poller()
        poller.register(self._frontend, zmq.POLLIN)
        poller.register(self._backend, zmq.POLLIN)

        while self._running:
            try:
                socks = dict(poller.poll(100))
            except zmq.ZMQError:
                break
            try:
                if self._frontend in socks:
                    msg = self._frontend.recv_multipart(zmq.NOBLOCK)
                    self._backend.send_multipart(msg)
                if self._backend in socks:
                    msg = self._backend.recv_multipart(zmq.NOBLOCK)
                    self._frontend.send_multipart(msg)
            except zmq.ZMQError:
                break

    def _worker_routine(self, worker_id: int) -> None:
        """Worker thread: connect REP socket to backend and process requests."""
        socket = self._context.socket(zmq.REP)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.RCVTIMEO, 100)  # 100ms timeout to check _running
        socket.connect(_WORKER_ENDPOINT)

        serializer = Serializer()

        logger.debug("Worker %d started", worker_id)

        while self._running:
            header = None
            try:
                raw_data = socket.recv()
                header, request = serializer.decode_request(raw_data)

                # Dispatch to handler
                response = self._handle_message(header.msg_type, request)

                # Send response
                response_data = serializer.encode_response(header, response)
                socket.send(response_data)

            except zmq.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    # Receive timeout — loop back to check _running
                    continue
                if self._running:
                    logger.error("Worker %d ZMQ error: %s", worker_id, e)
                break
            except Exception:
                logger.error(
                    "Worker %d error handling message", worker_id, exc_info=True
                )
                # Send error response preserving sequence from request
                error_response = {"error": "internal server error"}
                try:
                    error_header = (
                        header if header is not None else MessageHeader(msg_type=0)
                    )
                    error_data = serializer.encode_response(
                        error_header, error_response
                    )
                    socket.send(error_data)
                except Exception:
                    logger.debug(
                        "Worker %d: failed to send error response",
                        worker_id,
                        exc_info=True,
                    )

        socket.close()
        logger.debug("Worker %d stopped", worker_id)

    # =========================================================================
    # Message Dispatch
    # =========================================================================

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
