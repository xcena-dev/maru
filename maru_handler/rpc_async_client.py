# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Async RPC Client for connecting to MaruServer (v1 — asyncio).

Uses a background asyncio event loop with zmq.asyncio DEALER socket.
Supports both blocking (_send_request) and non-blocking (_send_request_nonblocking)
call patterns, enabling pipelining for concurrent in-flight requests.

Architecture::

    Caller Thread (MaruHandler)
        |
        v
    run_coroutine_threadsafe(coro, loop)
        |
        v
    Event Loop Thread (asyncio.run_forever)
        ├── _send_async(): sem.acquire → encode → send_multipart → await future
        └── _response_listener(): await recv_multipart → match seq → future.set_result
        |
        v
    Caller: concurrent.futures.Future.result() or store for later
"""

import asyncio
import logging
import threading
import uuid
from concurrent.futures import Future
from typing import Any

import zmq
import zmq.asyncio

from maru_common import (
    ANY_POOL_ID,
    AllocationManagerStats,
    BatchExistsKVResponse,
    BatchLookupKVResponse,
    BatchRegisterKVResponse,
    GetStatsResponse,
    KVManagerStats,
    ListAllocationsResponse,
    LookupKVResponse,
    LookupResult,
    MessageType,
    RequestAllocResponse,
    Serializer,
)
from maru_common.protocol import HEADER_SIZE, MessageHeader
from maru_shm import MaruHandle

logger = logging.getLogger(__name__)


class RpcAsyncClient:
    """
    ZeroMQ-based async RPC client using asyncio event loop.

    Unlike v0 (IO Thread + Queue + Signal Socket), this version uses a
    background asyncio event loop with zmq.asyncio for native async I/O.
    No locks are needed since all socket/state access happens on the
    single event loop thread.

    Supports two calling patterns:
    - Blocking: register_kv() — same as sync RpcClient
    - Non-blocking: register_kv_async() → concurrent.futures.Future
    """

    def __init__(
        self,
        server_url: str = "tcp://localhost:5555",
        timeout_ms: int = 5000,
        max_inflight: int = 64,
    ):
        self._server_url = server_url
        self._timeout_ms = timeout_ms
        self._max_inflight = max_inflight

        # Event loop (created in connect)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None

        # ZMQ (created on event loop thread via _async_init)
        self._context: zmq.asyncio.Context | None = None
        self._socket: zmq.asyncio.Socket | None = None

        # State
        self._running = False
        self._serializer = Serializer()
        self._pending: dict[int, asyncio.Future] = {}
        self._inflight_sem: asyncio.Semaphore | None = None
        self._listener_task: asyncio.Task | None = None

    # =========================================================================
    # Connection Lifecycle
    # =========================================================================

    def connect(self) -> None:
        """Connect to the server and start the event loop thread."""
        self._loop = asyncio.new_event_loop()
        self._running = True

        # Start event loop in background daemon thread
        self._loop_thread = threading.Thread(
            target=self._run_event_loop, name="rpc-asyncio", daemon=True
        )
        self._loop_thread.start()

        # Initialize ZMQ + listener on the event loop thread
        init_future = asyncio.run_coroutine_threadsafe(self._async_init(), self._loop)
        init_future.result(timeout=5.0)

        logger.info("Connected to MaruServer at %s", self._server_url)

    def close(self) -> None:
        """Close the connection and stop the event loop."""
        self._running = False

        if self._loop and self._loop.is_running():
            # Cleanup on the event loop thread
            cleanup_future = asyncio.run_coroutine_threadsafe(
                self._async_cleanup(), self._loop
            )
            try:
                cleanup_future.result(timeout=2.0)
            except Exception:
                logger.debug("Async cleanup failed", exc_info=True)

            # Stop the event loop
            self._loop.call_soon_threadsafe(self._loop.stop)

        # Wait for thread to exit
        if self._loop_thread and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=2.0)

        # Close ZMQ context (socket already closed in _async_cleanup)
        if self._context:
            self._context.term()
            self._context = None

        if self._loop:
            self._loop.close()
            self._loop = None

        logger.info("Disconnected from MaruServer")

    # =========================================================================
    # Event Loop Thread
    # =========================================================================

    def _run_event_loop(self) -> None:
        """Run the asyncio event loop (target for background thread)."""
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    async def _async_init(self) -> None:
        """Initialize ZMQ sockets and start listener (runs on event loop)."""
        self._context = zmq.asyncio.Context()
        self._socket = self._context.socket(zmq.DEALER)
        self._socket.setsockopt(zmq.LINGER, 0)
        identity = uuid.uuid4().bytes[:8]
        self._socket.setsockopt(zmq.IDENTITY, identity)
        self._socket.connect(self._server_url)

        self._inflight_sem = asyncio.Semaphore(self._max_inflight)
        self._listener_task = asyncio.ensure_future(self._response_listener())

    async def _async_cleanup(self) -> None:
        """Close socket, wait for listener to exit, and fail pending futures.

        Socket is closed FIRST so that the listener's recv_multipart raises
        ZMQError and exits naturally. This avoids CancelledError interrupting
        the Cython recv mid-operation (which leaves orphaned state that causes
        KeyError: '__builtins__' during GC).
        """
        # 1. Close socket — causes recv_multipart to raise ZMQError
        if self._socket:
            self._socket.close()
            self._socket = None

        # 2. Wait for listener to exit via ZMQError (cancel as fallback)
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except (asyncio.CancelledError, Exception):
                pass

        # 3. Fail all pending futures
        for _seq, fut in self._pending.items():
            if not fut.done():
                fut.set_exception(RuntimeError("Client shutting down"))
        self._pending.clear()

        # 4. Drain event loop so cancelled coroutines are collected
        await asyncio.sleep(0)

    # =========================================================================
    # Response Listener
    # =========================================================================

    async def _response_listener(self) -> None:
        """Continuously receive responses and resolve pending futures."""
        while self._running:
            try:
                frames = await self._socket.recv_multipart()
                response_data = frames[-1]
                header, payload = self._serializer.decode(response_data)
                seq = header.sequence

                fut = self._pending.pop(seq, None)
                if fut is not None and not fut.done():
                    fut.set_result(payload)
                else:
                    logger.warning("No pending future for seq=%d", seq)

            except asyncio.CancelledError:
                break
            except zmq.ZMQError as e:
                if self._running:
                    logger.error("ZMQ error in response listener: %s", e)
                break
            except Exception:
                logger.error("Response listener error", exc_info=True)
                continue

    # =========================================================================
    # Core Send Methods
    # =========================================================================

    async def _send_async(
        self, msg_type: MessageType, data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Coroutine: encode, send, and await response.

        Runs on the event loop thread. No locks needed.

        Note:
            Requires Python 3.11+ for correct semaphore release on
            CancelledError (PEP 654 ExceptionGroup semantics ensure
            ``async with`` __aexit__ runs properly on cancellation).
        """
        async with self._inflight_sem:
            encoded = self._serializer.encode(msg_type, data)
            header = MessageHeader.unpack(encoded[:HEADER_SIZE])
            seq = header.sequence

            fut = self._loop.create_future()
            self._pending[seq] = fut

            try:
                await self._socket.send_multipart([b"", encoded])
                return await fut
            except asyncio.CancelledError:
                self._pending.pop(seq, None)
                raise

    def _send_request(
        self, msg_type: MessageType, data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Send a request and block until response (sync wrapper).

        Compatible with v0 API — same signature and return type.
        """
        if not self._running:
            raise RuntimeError("Client not connected. Call connect() first.")

        cf_future = asyncio.run_coroutine_threadsafe(
            self._send_async(msg_type, data), self._loop
        )
        try:
            return cf_future.result(timeout=self._timeout_ms / 1000.0)
        except TimeoutError:
            cf_future.cancel()
            logger.error(
                "Timeout waiting for response from server (msg_type=%s)",
                msg_type,
            )
            return {"error": "timeout", "success": False}

    def _send_request_nonblocking(
        self, msg_type: MessageType, data: dict[str, Any]
    ) -> Future:
        """
        Send a request and return a Future immediately (non-blocking).

        Returns:
            concurrent.futures.Future that resolves to dict[str, Any]
        """
        if not self._running:
            raise RuntimeError("Client not connected. Call connect() first.")

        return asyncio.run_coroutine_threadsafe(
            self._send_async(msg_type, data), self._loop
        )

    # =========================================================================
    # Response Parsing Helpers
    # =========================================================================

    @staticmethod
    def _parse_request_alloc(response: dict) -> RequestAllocResponse:
        if not response.get("success"):
            return RequestAllocResponse(
                success=False, error=response.get("error", "Unknown error")
            )
        handle_data = response.get("handle", {})
        handle = MaruHandle.from_dict(handle_data) if handle_data else None
        return RequestAllocResponse(success=True, handle=handle)

    @staticmethod
    def _parse_lookup_kv(response: dict) -> LookupKVResponse:
        found = response.get("found", False)
        if not found:
            return LookupKVResponse(found=False)
        handle = None
        if "handle" in response:
            handle = MaruHandle.from_dict(response["handle"])
        return LookupKVResponse(
            found=True,
            handle=handle,
            kv_offset=response.get("kv_offset", 0),
            kv_length=response.get("kv_length", 0),
        )

    @staticmethod
    def _parse_list_allocations(response: dict) -> ListAllocationsResponse:
        if not response.get("success"):
            return ListAllocationsResponse(
                success=False, error=response.get("error", "Unknown error")
            )
        handles = [
            MaruHandle.from_dict(alloc_data)
            for alloc_data in response.get("allocations", [])
        ]
        return ListAllocationsResponse(success=True, allocations=handles)

    @staticmethod
    def _parse_batch_lookup_kv(response: dict) -> BatchLookupKVResponse:
        entries = []
        for entry_data in response.get("entries", []):
            found = entry_data.get("found", False)
            handle = None
            if found and "handle" in entry_data and entry_data["handle"]:
                handle = MaruHandle.from_dict(entry_data["handle"])
            entries.append(
                LookupResult(
                    found=found,
                    handle=handle,
                    kv_offset=entry_data.get("kv_offset", 0),
                    kv_length=entry_data.get("kv_length", 0),
                )
            )
        return BatchLookupKVResponse(entries=entries)

    # =========================================================================
    # Allocation Management (blocking)
    # =========================================================================

    def request_alloc(
        self, instance_id: str, size: int, pool_id: int = ANY_POOL_ID
    ) -> RequestAllocResponse:
        """Request a new memory allocation."""
        response = self._send_request(
            MessageType.REQUEST_ALLOC,
            {"instance_id": instance_id, "size": size, "pool_id": pool_id},
        )
        return self._parse_request_alloc(response)

    def list_allocations(
        self, exclude_instance_id: str | None = None
    ) -> ListAllocationsResponse:
        """List all active allocations from the server."""
        data: dict = {}
        if exclude_instance_id:
            data["exclude_instance_id"] = exclude_instance_id
        response = self._send_request(MessageType.LIST_ALLOCATIONS, data)
        return self._parse_list_allocations(response)

    def return_alloc(self, instance_id: str, region_id: int) -> bool:
        """Return an allocation back to the server."""
        response = self._send_request(
            MessageType.RETURN_ALLOC,
            {"instance_id": instance_id, "region_id": region_id},
        )
        return response.get("success", False)

    # =========================================================================
    # KV Operations (blocking)
    # =========================================================================

    def register_kv(
        self, key: str, region_id: int, kv_offset: int, kv_length: int
    ) -> bool:
        """Register a KV entry."""
        response = self._send_request(
            MessageType.REGISTER_KV,
            {
                "key": key,
                "region_id": region_id,
                "kv_offset": kv_offset,
                "kv_length": kv_length,
            },
        )
        return response.get("is_new", False)

    def lookup_kv(self, key: str) -> LookupKVResponse:
        """Lookup a KV entry by key."""
        response = self._send_request(MessageType.LOOKUP_KV, {"key": key})
        return self._parse_lookup_kv(response)

    def exists_kv(self, key: str) -> bool:
        """Check if a KV entry exists."""
        response = self._send_request(MessageType.EXISTS_KV, {"key": key})
        return response.get("exists", False)

    def delete_kv(self, key: str) -> bool:
        """Delete a KV entry."""
        response = self._send_request(MessageType.DELETE_KV, {"key": key})
        return response.get("success", False)

    # =========================================================================
    # Batch KV Operations (blocking)
    # =========================================================================

    def batch_register_kv(
        self, entries: list[tuple[str, int, int, int]]
    ) -> BatchRegisterKVResponse:
        """Register multiple KV entries in a single RPC call."""
        entries_data = [
            {
                "key": key,
                "region_id": region_id,
                "kv_offset": kv_offset,
                "kv_length": kv_length,
            }
            for key, region_id, kv_offset, kv_length in entries
        ]
        response = self._send_request(
            MessageType.BATCH_REGISTER_KV, {"entries": entries_data}
        )
        return BatchRegisterKVResponse(
            success=response.get("success", False),
            results=response.get("results", []),
        )

    def batch_lookup_kv(self, keys: list[str]) -> BatchLookupKVResponse:
        """Lookup multiple KV entries in a single RPC call."""
        response = self._send_request(MessageType.BATCH_LOOKUP_KV, {"keys": keys})
        return self._parse_batch_lookup_kv(response)

    def batch_exists_kv(self, keys: list[str]) -> BatchExistsKVResponse:
        """Check existence of multiple KV entries in a single RPC call."""
        response = self._send_request(MessageType.BATCH_EXISTS_KV, {"keys": keys})
        return BatchExistsKVResponse(results=response.get("results", []))

    # =========================================================================
    # Admin Operations (blocking)
    # =========================================================================

    def get_stats(self) -> GetStatsResponse:
        """Get server statistics."""
        response = self._send_request(MessageType.GET_STATS, {})
        kv_data = response.get("kv_manager", {})
        alloc_data = response.get("allocation_manager", {})
        return GetStatsResponse(
            kv_manager=KVManagerStats(
                total_entries=kv_data.get("total_entries", 0),
                total_size=kv_data.get("total_size", 0),
            ),
            allocation_manager=AllocationManagerStats(
                num_allocations=alloc_data.get("num_allocations", 0),
                total_allocated=alloc_data.get("total_allocated", 0),
                active_clients=alloc_data.get("active_clients", 0),
            ),
        )

    def heartbeat(self) -> bool:
        """Send heartbeat to server."""
        response = self._send_request(MessageType.HEARTBEAT, {})
        return "error" not in response

    # =========================================================================
    # Non-blocking Async API (*_async methods)
    # =========================================================================

    def request_alloc_async(
        self, instance_id: str, size: int, pool_id: int = ANY_POOL_ID
    ) -> Future:
        """Non-blocking request_alloc. Returns Future[RequestAllocResponse]."""

        async def _coro():
            response = await self._send_async(
                MessageType.REQUEST_ALLOC,
                {"instance_id": instance_id, "size": size, "pool_id": pool_id},
            )
            return self._parse_request_alloc(response)

        return asyncio.run_coroutine_threadsafe(_coro(), self._loop)

    def list_allocations_async(self, exclude_instance_id: str | None = None) -> Future:
        """Non-blocking list_allocations. Returns Future[ListAllocationsResponse]."""

        async def _coro():
            data: dict = {}
            if exclude_instance_id:
                data["exclude_instance_id"] = exclude_instance_id
            response = await self._send_async(MessageType.LIST_ALLOCATIONS, data)
            return self._parse_list_allocations(response)

        return asyncio.run_coroutine_threadsafe(_coro(), self._loop)

    def return_alloc_async(self, instance_id: str, region_id: int) -> Future:
        """Non-blocking return_alloc. Returns Future[bool]."""

        async def _coro():
            response = await self._send_async(
                MessageType.RETURN_ALLOC,
                {"instance_id": instance_id, "region_id": region_id},
            )
            return response.get("success", False)

        return asyncio.run_coroutine_threadsafe(_coro(), self._loop)

    def register_kv_async(
        self, key: str, region_id: int, kv_offset: int, kv_length: int
    ) -> Future:
        """Non-blocking register_kv. Returns Future[bool]."""

        async def _coro():
            response = await self._send_async(
                MessageType.REGISTER_KV,
                {
                    "key": key,
                    "region_id": region_id,
                    "kv_offset": kv_offset,
                    "kv_length": kv_length,
                },
            )
            return response.get("is_new", False)

        return asyncio.run_coroutine_threadsafe(_coro(), self._loop)

    def lookup_kv_async(self, key: str) -> Future:
        """Non-blocking lookup_kv. Returns Future[LookupKVResponse]."""

        async def _coro():
            response = await self._send_async(MessageType.LOOKUP_KV, {"key": key})
            return self._parse_lookup_kv(response)

        return asyncio.run_coroutine_threadsafe(_coro(), self._loop)

    def exists_kv_async(self, key: str) -> Future:
        """Non-blocking exists_kv. Returns Future[bool]."""

        async def _coro():
            response = await self._send_async(MessageType.EXISTS_KV, {"key": key})
            return response.get("exists", False)

        return asyncio.run_coroutine_threadsafe(_coro(), self._loop)

    def delete_kv_async(self, key: str) -> Future:
        """Non-blocking delete_kv. Returns Future[bool]."""

        async def _coro():
            response = await self._send_async(MessageType.DELETE_KV, {"key": key})
            return response.get("success", False)

        return asyncio.run_coroutine_threadsafe(_coro(), self._loop)

    def batch_register_kv_async(
        self, entries: list[tuple[str, int, int, int]]
    ) -> Future:
        """Non-blocking batch_register_kv. Returns Future[BatchRegisterKVResponse]."""

        async def _coro():
            entries_data = [
                {
                    "key": key,
                    "region_id": region_id,
                    "kv_offset": kv_offset,
                    "kv_length": kv_length,
                }
                for key, region_id, kv_offset, kv_length in entries
            ]
            response = await self._send_async(
                MessageType.BATCH_REGISTER_KV, {"entries": entries_data}
            )
            return BatchRegisterKVResponse(
                success=response.get("success", False),
                results=response.get("results", []),
            )

        return asyncio.run_coroutine_threadsafe(_coro(), self._loop)

    def batch_lookup_kv_async(self, keys: list[str]) -> Future:
        """Non-blocking batch_lookup_kv. Returns Future[BatchLookupKVResponse]."""

        async def _coro():
            response = await self._send_async(
                MessageType.BATCH_LOOKUP_KV, {"keys": keys}
            )
            return self._parse_batch_lookup_kv(response)

        return asyncio.run_coroutine_threadsafe(_coro(), self._loop)

    def batch_exists_kv_async(self, keys: list[str]) -> Future:
        """Non-blocking batch_exists_kv. Returns Future[BatchExistsKVResponse]."""

        async def _coro():
            response = await self._send_async(
                MessageType.BATCH_EXISTS_KV, {"keys": keys}
            )
            return BatchExistsKVResponse(results=response.get("results", []))

        return asyncio.run_coroutine_threadsafe(_coro(), self._loop)

    def heartbeat_async(self) -> Future:
        """Non-blocking heartbeat. Returns Future[bool]."""

        async def _coro():
            response = await self._send_async(MessageType.HEARTBEAT, {})
            return "error" not in response

        return asyncio.run_coroutine_threadsafe(_coro(), self._loop)

    # =========================================================================
    # Context Manager
    # =========================================================================

    def __enter__(self) -> "RpcAsyncClient":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
