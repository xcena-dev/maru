# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""RPC Client for connecting to MaruServer."""

import logging
from typing import Any

import zmq

from maru_common import (
    AllocationManagerStats,
    BatchExistsKVResponse,
    BatchLookupKVResponse,
    BatchPinKVResponse,
    BatchRegisterKVResponse,
    BatchUnpinKVResponse,
    GetStatsResponse,
    KVManagerStats,
    ListAllocationsResponse,
    LookupKVResponse,
    LookupResult,
    MessageType,
    RequestAllocResponse,
    Serializer,
)
from maru_shm import MaruHandle

logger = logging.getLogger(__name__)


class RpcClient:
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
    # Allocation Management
    # =========================================================================

    def request_alloc(self, instance_id: str, size: int) -> RequestAllocResponse:
        """
        Request a new memory allocation.

        Args:
            instance_id: Client instance identifier
            size: Requested size in bytes

        Returns:
            RequestAllocResponse with handle on success
        """
        response = self._send_request(
            MessageType.REQUEST_ALLOC,
            {
                "instance_id": instance_id,
                "size": size,
            },
        )

        if not response.get("success"):
            return RequestAllocResponse(
                success=False,
                error=response.get("error", "Unknown error"),
            )

        handle_data = response.get("handle", {})
        handle = MaruHandle.from_dict(handle_data) if handle_data else None

        return RequestAllocResponse(success=True, handle=handle)

    def list_allocations(
        self, exclude_instance_id: str | None = None
    ) -> ListAllocationsResponse:
        """List all active allocations from the server.

        Args:
            exclude_instance_id: Exclude own allocations.

        Returns:
            ListAllocationsResponse with list of MaruHandle.
        """
        data: dict = {}
        if exclude_instance_id:
            data["exclude_instance_id"] = exclude_instance_id

        response = self._send_request(MessageType.LIST_ALLOCATIONS, data)

        if not response.get("success"):
            return ListAllocationsResponse(
                success=False,
                error=response.get("error", "Unknown error"),
            )

        handles = [
            MaruHandle.from_dict(alloc_data)
            for alloc_data in response.get("allocations", [])
        ]
        return ListAllocationsResponse(success=True, allocations=handles)

    def return_alloc(self, instance_id: str, region_id: int) -> bool:
        """
        Return an allocation back to the server.

        Args:
            instance_id: Client instance identifier
            region_id: Region ID to return

        Returns:
            True if successful
        """
        response = self._send_request(
            MessageType.RETURN_ALLOC,
            {
                "instance_id": instance_id,
                "region_id": region_id,
            },
        )
        return response.get("success", False)

    # =========================================================================
    # KV Operations
    # =========================================================================

    def register_kv(
        self, key: str, region_id: int, kv_offset: int, kv_length: int
    ) -> bool:
        """
        Register a KV entry.

        Args:
            key: Chunk key string
            region_id: Region ID of the allocation
            kv_offset: Offset within the allocation
            kv_length: Size of the KV data

        Returns:
            True if newly registered, False if already existed
        """
        response = self._send_request(
            MessageType.REGISTER_KV,
            {
                "key": key,
                "region_id": region_id,
                "kv_offset": kv_offset,
                "kv_length": kv_length,
            },
        )
        if "error" in response:
            raise ConnectionError(f"register_kv RPC failed: {response['error']}")
        return response.get("is_new", False)

    def lookup_kv(self, key: str) -> LookupKVResponse:
        """
        Lookup a KV entry by key.

        Args:
            key: Chunk key string

        Returns:
            LookupKVResponse with handle and kv location if found
        """
        response = self._send_request(MessageType.LOOKUP_KV, {"key": key})

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

    def exists_kv(self, key: str) -> bool:
        """
        Check if a KV entry exists.

        Args:
            key: Chunk key string

        Returns:
            True if exists
        """
        response = self._send_request(MessageType.EXISTS_KV, {"key": key})
        return response.get("exists", False)

    def pin_kv(self, key: str) -> bool:
        """
        Check if a KV entry exists and pin it atomically.

        Args:
            key: Chunk key string

        Returns:
            True if exists (and was pinned)
        """
        response = self._send_request(MessageType.PIN_KV, {"key": key})
        return response.get("exists", False)

    def unpin(self, key: str) -> bool:
        """
        Unpin a KV entry, making it eligible for eviction.

        Args:
            key: Chunk key string

        Returns:
            True if unpinned successfully
        """
        response = self._send_request(MessageType.UNPIN_KV, {"key": key})
        return response.get("success", False)

    def delete_kv(self, key: str) -> bool:
        """
        Delete a KV entry.

        Args:
            key: Chunk key string

        Returns:
            True if deleted successfully
        """
        response = self._send_request(MessageType.DELETE_KV, {"key": key})
        return response.get("success", False)

    # =========================================================================
    # Batch KV Operations
    # =========================================================================

    def batch_register_kv(
        self, entries: list[tuple[str, int, int, int]]
    ) -> BatchRegisterKVResponse:
        """
        Register multiple KV entries in a single RPC call.

        Args:
            entries: List of (key, region_id, kv_offset, kv_length) tuples

        Returns:
            BatchRegisterKVResponse with results for each entry
        """
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
            success=response.get("success", False), results=response.get("results", [])
        )

    def batch_lookup_kv(self, keys: list[str]) -> BatchLookupKVResponse:
        """
        Lookup multiple KV entries in a single RPC call.

        Args:
            keys: List of chunk key strings

        Returns:
            BatchLookupKVResponse with results for each key
        """
        response = self._send_request(MessageType.BATCH_LOOKUP_KV, {"keys": keys})

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

    def batch_exists_kv(self, keys: list[str]) -> BatchExistsKVResponse:
        """
        Check existence of multiple KV entries in a single RPC call.

        Args:
            keys: List of chunk key strings

        Returns:
            BatchExistsKVResponse with results for each key
        """
        response = self._send_request(MessageType.BATCH_EXISTS_KV, {"keys": keys})

        return BatchExistsKVResponse(results=response.get("results", []))

    def batch_pin_kv(self, keys: list[str]) -> BatchPinKVResponse:
        """Check existence and pin multiple KV entries in a single RPC call."""
        response = self._send_request(MessageType.BATCH_PIN_KV, {"keys": keys})
        return BatchPinKVResponse(results=response.get("results", []))

    def batch_unpin(self, keys: list[str]) -> BatchUnpinKVResponse:
        """Unpin multiple KV entries in a single RPC call."""
        response = self._send_request(MessageType.BATCH_UNPIN_KV, {"keys": keys})
        return BatchUnpinKVResponse(results=response.get("results", []))

    # =========================================================================
    # Admin Operations
    # =========================================================================

    def get_stats(self) -> GetStatsResponse:
        """
        Get server statistics.

        Returns:
            GetStatsResponse with KV and allocation manager stats
        """
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
            stats_manager=response.get("stats_manager", {}),
        )

    def report_timings(self, entries: list[dict]) -> None:
        """Report client-side handler timings to server."""
        self._send_request(MessageType.REPORT_TIMINGS, {"entries": entries})

    def heartbeat(self) -> bool:
        """
        Send heartbeat to server.

        Returns:
            True if server responded
        """
        response = self._send_request(MessageType.HEARTBEAT, {})
        return "error" not in response

    def handshake(self) -> dict:
        """Perform handshake with server. Returns server config (rm_address, etc.)."""
        return self._send_request(MessageType.HANDSHAKE, {})

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
