# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruServer - Main server implementation."""

import argparse
import logging
import signal
from threading import RLock

from maru_shm.types import MaruHandle

from .allocation_manager import AllocationManager
from .kv_manager import DeleteResult, KVManager

logger = logging.getLogger(__name__)


class MaruServer:
    """
    Central metadata server for Maru shared memory KV cache.

    Manages:
    - KV metadata (hash -> handle mapping)
    - Memory allocation and ownership
    """

    def __init__(self, rm_address: str | None = None):
        self._rm_address = rm_address or "127.0.0.1:9850"
        self._allocation_manager = AllocationManager(rm_address=rm_address)
        self._kv_manager = KVManager()
        self._lock = RLock()  # Coordinates cross-manager operations
        # TODO: Add PinMonitor daemon thread when eviction is implemented.
        # Periodically force-unpin entries that exceed a TTL to prevent
        # pin leaks from crashed clients.
        logger.info("MaruServer initialized (rm_address=%s)", self._rm_address)

    @property
    def rm_address(self) -> str:
        """Resource manager address used by this server."""
        return self._rm_address

    # =========================================================================
    # Allocation Management
    # =========================================================================

    def request_alloc(
        self, instance_id: str, size: int, dax_path: str = ""
    ) -> MaruHandle | None:
        """Handle allocation request from client."""
        handle = self._allocation_manager.allocate(instance_id, size, dax_path=dax_path)
        if handle:
            logger.info(
                "Allocated %d bytes for %s: region_id=%d",
                size,
                instance_id,
                handle.region_id,
            )
        else:
            logger.error("Failed to allocate %d bytes for %s", size, instance_id)
        return handle

    def return_alloc(self, instance_id: str, region_id: int) -> bool:
        """Handle allocation return request from client."""
        success = self._allocation_manager.release(instance_id, region_id)
        if success:
            logger.info("Released region_id=%d by %s", region_id, instance_id)
        return success

    def list_allocations(
        self, exclude_instance_id: str | None = None
    ) -> list[MaruHandle]:
        """List all active allocation handles."""
        return self._allocation_manager.list_allocations(exclude_instance_id)

    def client_disconnected(self, instance_id: str) -> None:
        """Handle client disconnection."""
        self._allocation_manager.disconnect_client(instance_id)
        logger.info("Client %s disconnected, released allocations", instance_id)

    # =========================================================================
    # KV Management
    # =========================================================================

    def register_kv(
        self, key: str, region_id: int, kv_offset: int, kv_length: int
    ) -> bool:
        """Register a KV entry."""
        with self._lock:
            is_new, alloc_to_ref = self._kv_manager.register(
                key, region_id, kv_offset, kv_length
            )

            if alloc_to_ref is not None:
                self._allocation_manager.increment_kv_ref(alloc_to_ref)

        logger.debug("Registered KV: key=%s, region_id=%d", key, region_id)
        return is_new

    def lookup_kv(self, key: str) -> dict | None:
        """Lookup a KV entry and return handle with KV location info."""
        with self._lock:
            entry = self._kv_manager.lookup(key)
            if entry is None:
                return None

            handle = self._allocation_manager.get_handle(entry.region_id)
            if handle is None:
                return None

            return {
                "handle": handle,
                "kv_offset": entry.kv_offset,
                "kv_length": entry.kv_length,
            }

    def exists_kv(self, key: str) -> bool:
        """Check if a KV entry exists."""
        return self._kv_manager.exists(key)

    def pin_kv(self, key: str) -> bool:
        """Check if a KV entry exists and pin it atomically."""
        return self._kv_manager.pin(key)

    def unpin(self, key: str) -> bool:
        """Unpin a KV entry, making it eligible for eviction."""
        return self._kv_manager.unpin(key)

    def delete_kv(self, key: str) -> bool:
        """Delete a KV entry."""
        with self._lock:
            result, region_to_deref = self._kv_manager.delete(key)

            if region_to_deref is not None:
                self._allocation_manager.decrement_kv_ref(region_to_deref)

        return result == DeleteResult.DELETED

    # =========================================================================
    # Batch KV Operations
    # =========================================================================

    def batch_register_kv(self, entries: list[tuple[str, int, int, int]]) -> list[bool]:
        """
        Register multiple KV entries in a single operation.

        Args:
            entries: List of (key, region_id, kv_offset, kv_length) tuples

        Returns:
            List of booleans indicating if each entry was newly registered
        """
        with self._lock:
            results = []
            for key, region_id, kv_offset, kv_length in entries:
                is_new, alloc_to_ref = self._kv_manager.register(
                    key, region_id, kv_offset, kv_length
                )
                if alloc_to_ref is not None:
                    self._allocation_manager.increment_kv_ref(alloc_to_ref)
                results.append(is_new)
            return results

    def batch_lookup_kv(self, keys: list[str]) -> list[dict | None]:
        """
        Lookup multiple KV entries in a single operation.

        Args:
            keys: List of chunk key strings

        Returns:
            List of dicts with handle/kv_offset/kv_length, or None for each key
        """
        with self._lock:
            entries = self._kv_manager.batch_lookup(keys)
            results = []

            for entry in entries:
                if entry is None:
                    results.append(None)
                else:
                    handle = self._allocation_manager.get_handle(entry.region_id)
                    if handle is None:
                        results.append(None)
                    else:
                        results.append(
                            {
                                "handle": handle,
                                "kv_offset": entry.kv_offset,
                                "kv_length": entry.kv_length,
                            }
                        )

            return results

    def batch_pin_kv(self, keys: list[str]) -> list[bool]:
        """Check existence and pin multiple KV entries atomically."""
        return self._kv_manager.batch_pin(keys)

    def batch_unpin(self, keys: list[str]) -> list[bool]:
        """Unpin multiple KV entries."""
        return self._kv_manager.batch_unpin(keys)

    def batch_exists_kv(self, keys: list[str]) -> list[bool]:
        """
        Check existence of multiple KV entries in a single operation.

        Args:
            keys: List of chunk key strings

        Returns:
            List of booleans indicating if each key exists
        """
        return self._kv_manager.batch_exists(keys)

    def get_stats(self) -> dict:
        """Get server statistics."""
        return {
            "kv_manager": self._kv_manager.get_stats(),
            "allocation_manager": self._allocation_manager.get_stats(),
        }

    def close(self) -> None:
        """Shutdown the server and release resources."""
        self._allocation_manager.close()
        logger.info("MaruServer closed")


# =============================================================================
# CLI Utilities
# =============================================================================


def setup_logging(level: str) -> None:
    """Setup logging level for the MaruServer package."""
    log_level = getattr(logging, level.upper(), logging.INFO)
    pkg_logger = logging.getLogger("maru_server")
    pkg_logger.setLevel(log_level)
    for handler in pkg_logger.handlers:
        handler.setLevel(log_level)


def main() -> None:
    """Main entry point for the server."""
    # Import here to avoid circular import
    from .rpc_server import RpcServer

    parser = argparse.ArgumentParser(
        description="MaruServer - Central metadata server for Maru shared memory KV cache"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host to bind the server to (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5555,
        help="Port to bind the server to (default: 5555)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    parser.add_argument(
        "--rm-address",
        type=str,
        default="127.0.0.1:9850",
        help="Resource manager address (host:port, default: 127.0.0.1:9850)",
    )
    args = parser.parse_args()

    setup_logging(args.log_level)

    # Create server
    server = MaruServer(rm_address=args.rm_address)
    rpc_server = RpcServer(server, host=args.host, port=args.port)

    # Setup signal handlers
    def signal_handler(signum, frame):
        rpc_server.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start server
    logger.info("Starting MaruServer on %s:%d", args.host, args.port)
    try:
        rpc_server.start()
    finally:
        server.close()


if __name__ == "__main__":
    main()
