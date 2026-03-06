# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruServer - Main server implementation."""

import argparse
import logging
import signal
from threading import RLock

from maru_shm.types import MaruHandle

from .allocation_manager import AllocationManager
from .kv_manager import KVManager

logger = logging.getLogger(__name__)


class MaruServer:
    """
    Central metadata server for Maru shared memory KV cache.

    Manages:
    - KV metadata (hash -> handle mapping)
    - Memory allocation and ownership
    """

    def __init__(self):
        self._allocation_manager = AllocationManager()
        self._kv_manager = KVManager()
        self._lock = RLock()  # Coordinates cross-manager operations
        logger.info("MaruServer initialized")

    # =========================================================================
    # Allocation Management
    # =========================================================================

    def request_alloc(self, instance_id: str, size: int) -> MaruHandle | None:
        """Handle allocation request from client."""
        handle = self._allocation_manager.allocate(instance_id, size)
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
        self, key: int, region_id: int, kv_offset: int, kv_length: int
    ) -> bool:
        """Register a KV entry."""
        with self._lock:
            is_new, alloc_to_ref = self._kv_manager.register(
                key, region_id, kv_offset, kv_length
            )

            if alloc_to_ref is not None:
                self._allocation_manager.increment_kv_ref(alloc_to_ref)

        logger.debug("Registered KV: key=%d, region_id=%d", key, region_id)
        return is_new

    def lookup_kv(self, key: int) -> dict | None:
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

    def exists_kv(self, key: int) -> bool:
        """Check if a KV entry exists."""
        return self._kv_manager.exists(key)

    def delete_kv(self, key: int) -> bool:
        """Delete a KV entry."""
        with self._lock:
            existed, region_to_deref = self._kv_manager.delete(key)

            if region_to_deref is not None:
                self._allocation_manager.decrement_kv_ref(region_to_deref)

        return existed

    # =========================================================================
    # Batch KV Operations
    # =========================================================================

    def batch_register_kv(self, entries: list[tuple[int, int, int, int]]) -> list[bool]:
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

    def batch_lookup_kv(self, keys: list[int]) -> list[dict | None]:
        """
        Lookup multiple KV entries in a single operation.

        Args:
            keys: List of chunk hashes

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

    def batch_exists_kv(self, keys: list[int]) -> list[bool]:
        """
        Check existence of multiple KV entries in a single operation.

        Args:
            keys: List of chunk hashes

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


# =============================================================================
# CLI Utilities
# =============================================================================


def setup_logging(level: str) -> None:
    """Setup logging level for the MaruServer package."""
    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.getLogger("maru_server").setLevel(log_level)


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
    args = parser.parse_args()

    setup_logging(args.log_level)

    # Create server
    server = MaruServer()
    rpc_server = RpcServer(server, host=args.host, port=args.port)

    # Setup signal handlers
    def signal_handler(signum, frame):
        rpc_server.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start server
    logger.info("Starting MaruServer on %s:%d", args.host, args.port)
    rpc_server.start()


if __name__ == "__main__":
    main()
