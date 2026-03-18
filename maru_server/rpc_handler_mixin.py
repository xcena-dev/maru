"""RpcHandlerMixin - Shared message dispatch and handler methods for RPC servers."""

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from maru_common import MessageType

if TYPE_CHECKING:
    from .server import MaruServer

logger = logging.getLogger(__name__)


class RpcHandlerMixin:
    """
    Mixin providing shared message dispatch and handler methods for RPC servers.

    Requires the subclass to define ``self._server`` of type ``MaruServer``.
    """

    _server: "MaruServer"
    _handlers: dict[int, Callable[[Any], dict]] | None = None

    def _get_handlers(self) -> dict[int, Callable[[Any], dict]]:
        """Return cached handler dispatch dict (built once per instance)."""
        if self._handlers is None:
            self._handlers = {
                MessageType.REQUEST_ALLOC.value: self._handle_request_alloc,
                MessageType.RETURN_ALLOC.value: self._handle_return_alloc,
                MessageType.LIST_ALLOCATIONS.value: self._handle_list_allocations,
                MessageType.REGISTER_KV.value: self._handle_register_kv,
                MessageType.LOOKUP_KV.value: self._handle_lookup_kv,
                MessageType.EXISTS_KV.value: self._handle_exists_kv,
                MessageType.DELETE_KV.value: self._handle_delete_kv,
                MessageType.EXISTS_AND_PIN_KV.value: self._handle_exists_and_pin_kv,
                MessageType.UNPIN_KV.value: self._handle_unpin_kv,
                # Batch operations
                MessageType.BATCH_REGISTER_KV.value: self._handle_batch_register_kv,
                MessageType.BATCH_LOOKUP_KV.value: self._handle_batch_lookup_kv,
                MessageType.BATCH_EXISTS_KV.value: self._handle_batch_exists_kv,
                MessageType.BATCH_EXISTS_AND_PIN_KV.value: self._handle_batch_exists_and_pin_kv,
                MessageType.BATCH_UNPIN_KV.value: self._handle_batch_unpin_kv,
                # Admin
                MessageType.GET_STATS.value: self._handle_get_stats,
                MessageType.HEARTBEAT.value: self._handle_heartbeat,
            }
        return self._handlers

    def _handle_message(self, msg_type: int, request: Any) -> dict:
        """Dispatch message to appropriate handler."""
        handler = self._get_handlers().get(msg_type)
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
            pool_id=req.pool_id,
        )
        if handle is None:
            logger.debug(
                "[REQUEST_ALLOC] instance=%s, size=%d, pool_id=%s -> FAILED",
                req.instance_id,
                req.size,
                req.pool_id,
            )
            return {"success": False, "error": "Allocation failed"}
        logger.debug(
            "[REQUEST_ALLOC] instance=%s, size=%d, pool_id=%s -> region_id=%d",
            req.instance_id,
            req.size,
            req.pool_id,
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
            "[PUT] key=%s, region_id=%d, kv_offset=%d, kv_length=%d",
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
            logger.debug("[GET] key=%s -> NOT FOUND", req.key)
            return {"found": False}
        logger.debug(
            "[GET] key=%s -> region_id=%d, kv_offset=%d, kv_length=%d",
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

    def _handle_exists_and_pin_kv(self, req: Any) -> dict:
        exists = self._server.exists_and_pin_kv(key=req.key)
        logger.debug("[EXISTS_AND_PIN] key=%s -> %s", req.key, exists)
        return {"exists": exists}

    def _handle_unpin_kv(self, req: Any) -> dict:
        success = self._server.unpin_kv(key=req.key)
        logger.debug("[UNPIN] key=%s -> %s", req.key, success)
        return {"success": success}

    # =========================================================================
    # Batch KV Handlers
    # =========================================================================

    def _handle_batch_register_kv(self, req: Any) -> dict:
        """Handle batch register KV request."""
        entries = [(e.key, e.region_id, e.kv_offset, e.kv_length) for e in req.entries]
        logger.debug("[BATCH_PUT] %d entries", len(entries))
        for e in req.entries:
            logger.debug(
                "[BATCH_PUT] key=%s, region_id=%d, kv_offset=%d, kv_length=%d",
                e.key,
                e.region_id,
                e.kv_offset,
                e.kv_length,
            )
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
                logger.debug(
                    "[BATCH_GET] region_id=%d, kv_offset=%d, kv_length=%d",
                    result["handle"].region_id,
                    result["kv_offset"],
                    result["kv_length"],
                )
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
        results = self._server.batch_exists_kv(keys)
        hits = sum(results)
        logger.debug("[BATCH_EXISTS] %d keys, %d hits", len(keys), hits)
        return {"results": results}

    def _handle_batch_exists_and_pin_kv(self, req: Any) -> dict:
        """Handle batch exists and pin KV request."""
        keys = req.keys
        results = self._server.batch_exists_and_pin_kv(keys)
        hits = sum(results)
        logger.debug(
            "[BATCH_EXISTS_AND_PIN] %d keys, %d hits, %d pinned (prefix-stop)",
            len(keys),
            hits,
            hits,
        )
        return {"results": results}

    def _handle_batch_unpin_kv(self, req: Any) -> dict:
        """Handle batch unpin KV request."""
        keys = req.keys
        results = self._server.batch_unpin_kv(keys)
        ok = sum(results)
        logger.debug("[BATCH_UNPIN] %d keys, %d ok", len(keys), ok)
        return {"results": results}

    # =========================================================================
    # Admin Handlers
    # =========================================================================

    def _handle_get_stats(self, _req: Any) -> dict:
        stats = self._server.get_stats()
        return stats

    def _handle_heartbeat(self, _req: Any) -> dict:
        return {}
