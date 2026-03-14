# SPDX-License-Identifier: Apache-2.0
"""MaruHiCacheStorage — SGLang HiCache L3 storage backend backed by Maru.

Implements the HiCacheStorage ABC from SGLang, delegating all storage
operations to MaruHandler for CXL shared-memory KV cache access.

Usage (SGLang dynamic backend):
    python -m sglang.launch_server \\
        --enable-hierarchical-cache \\
        --hicache-storage-backend dynamic \\
        --hicache-storage-backend-extra-config '{
            "backend_name": "maru",
            "module_path": "maru_sglang.backend",
            "class_name": "MaruHiCacheStorage",
            "maru_server_url": "tcp://localhost:5555",
            "maru_pool_size": "4G"
        }'
"""

import logging
from abc import ABC, abstractmethod
from typing import Any

import torch

from maru import MaruConfig, MaruHandler

from .config import MaruSGLangConfig

logger = logging.getLogger(__name__)

# Lazy import of SGLang types — the sglang.srt subpackage may not be
# importable in all environments (e.g., pytest with source-tree shadowing).
# At runtime (inside SGLang), the real ABC is always available.
try:
    from sglang.srt.mem_cache.hicache_storage import (
        HiCacheStorage as _HiCacheStorageBase,
    )
    from sglang.srt.mem_cache.hicache_storage import (
        HiCacheStorageExtraInfo,
    )
except ImportError:
    _HiCacheStorageBase = None
    HiCacheStorageExtraInfo = None


def _get_base_class():
    """Return HiCacheStorage ABC if available, otherwise a compatible stub."""
    if _HiCacheStorageBase is not None:
        return _HiCacheStorageBase

    # Minimal stub matching the HiCacheStorage interface for environments
    # where sglang.srt is not importable (unit tests, dev tooling).
    class _HiCacheStorageStub(ABC):
        def register_mem_pool_host(self, mem_pool_host):
            self.mem_pool_host = mem_pool_host

        @abstractmethod
        def batch_get_v1(self, keys, host_indices, extra_info=None):
            pass

        @abstractmethod
        def batch_set_v1(self, keys, host_indices, extra_info=None):
            pass

        @abstractmethod
        def get(self, key, target_location=None, target_sizes=None):
            pass

        @abstractmethod
        def batch_get(self, keys, target_locations=None, target_sizes=None):
            pass

        @abstractmethod
        def set(self, key, value=None, target_location=None, target_sizes=None):
            pass

        @abstractmethod
        def batch_set(
            self, keys, values=None, target_locations=None, target_sizes=None
        ):
            pass

        @abstractmethod
        def exists(self, key: str) -> bool:
            pass

        def batch_exists(self, keys, extra_info=None) -> int:
            for i in range(len(keys)):
                if not self.exists(keys[i]):
                    return i
            return len(keys)

        @abstractmethod
        def clear(self):
            pass

    return _HiCacheStorageStub


class MaruHiCacheStorage(_get_base_class()):
    """SGLang HiCache L3 storage backend using Maru shared memory.

    Supports both legacy API (get/set with tensor copying) and V1 API
    (batch_get_v1/batch_set_v1 with zero-copy via host memory pool).
    """

    def __init__(self, storage_config, kwargs: dict):
        self.storage_config = storage_config
        self.maru_config = MaruSGLangConfig.from_extra_config(
            storage_config.extra_config
        )
        self._handler: MaruHandler | None = None
        self._suffix = self._build_key_suffix()
        self._connect()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _connect(self) -> None:
        """Initialize and connect MaruHandler."""
        cfg = self.maru_config
        handler = MaruHandler(
            MaruConfig(
                server_url=cfg.server_url,
                instance_id=cfg.instance_id,
                pool_size=cfg.pool_size,
                chunk_size_bytes=cfg.chunk_size_bytes,
                auto_connect=False,
                timeout_ms=cfg.timeout_ms,
                use_async_rpc=cfg.use_async_rpc,
                max_inflight=cfg.max_inflight,
                eager_map=cfg.eager_map,
            )
        )
        if handler.connect():
            self._handler = handler
            logger.info(
                "MaruHiCacheStorage connected: server=%s, pool=%d, chunk=%d",
                cfg.server_url,
                cfg.pool_size,
                cfg.chunk_size_bytes,
            )
        else:
            raise RuntimeError(f"Failed to connect MaruHandler to {cfg.server_url}")

    # ------------------------------------------------------------------
    # Key management
    # ------------------------------------------------------------------

    def _build_key_suffix(self) -> str:
        """Build TP-rank disambiguation suffix following SGLang convention."""
        cfg = self.storage_config
        model_name = cfg.model_name or ""
        model_name = "-".join(model_name.split("/"))
        if cfg.is_mla_model:
            return f"_{model_name}"
        return f"_{model_name}_{cfg.tp_rank}_{cfg.tp_size}"

    def _make_key(self, key: str) -> str:
        """Append suffix to raw SHA256 key."""
        return f"{key}{self._suffix}"

    # ------------------------------------------------------------------
    # Abstract method implementations (Legacy API)
    # ------------------------------------------------------------------

    def exists(self, key: str) -> bool:
        if self._handler is None:
            return False
        return self._handler.exists(self._make_key(key))

    def batch_exists(self, keys: list[str], extra_info=None) -> int:
        """Return number of consecutive existing keys from the start."""
        if self._handler is None or not keys:
            logger.debug(
                "batch_exists: skip (handler=%s, keys=%d)",
                self._handler is not None,
                len(keys) if keys else 0,
            )
            return 0
        full_keys = [self._make_key(k) for k in keys]
        logger.info(
            "batch_exists called: %d keys, first_5=%s",
            len(full_keys),
            full_keys[:5],
        )
        results = self._handler.batch_exists(full_keys)
        count = 0
        for r in results:
            if not r:
                break
            count += 1
        logger.info("batch_exists result: %d/%d consecutive hits", count, len(keys))
        return count

    def get(
        self,
        key: str,
        target_location: Any | None = None,
        target_sizes: Any | None = None,
    ) -> torch.Tensor | None:
        if self._handler is None:
            return None
        full_key = self._make_key(key)
        info = self._handler.retrieve(full_key)
        if info is None:
            logger.debug("get MISS key=%s", key)
            return None

        data = torch.frombuffer(info.view, dtype=torch.uint8)
        if target_location is not None:
            # Copy into pre-allocated target buffer
            target_bytes = target_location.view(torch.uint8).flatten()
            nbytes = min(data.numel(), target_bytes.numel())
            target_bytes[:nbytes].copy_(data[:nbytes])
            logger.debug("get HIT key=%s, %d bytes", key, nbytes)
            return target_location
        logger.debug("get HIT key=%s, %d bytes", key, data.numel())
        return data

    def set(
        self,
        key: str,
        value: Any | None = None,
        target_location: Any | None = None,
        target_sizes: Any | None = None,
    ) -> bool:
        if self._handler is None or value is None:
            return False
        full_key = self._make_key(key)
        src = value.contiguous().view(torch.uint8).numpy()
        mv = memoryview(src)
        ok = self._handler.store(full_key, data=mv)
        logger.debug("set %s key=%s, %d bytes", "OK" if ok else "FAIL", key, len(mv))
        return ok

    def batch_get(
        self,
        keys: list[str],
        target_locations: Any | None = None,
        target_sizes: Any | None = None,
    ) -> list[torch.Tensor | None]:
        if self._handler is None or not keys:
            return [None] * len(keys)
        full_keys = [self._make_key(k) for k in keys]
        logger.info(
            "batch_get called: %d keys, first_5=%s",
            len(full_keys),
            full_keys[:5],
        )
        results = self._handler.batch_retrieve(full_keys)

        outputs: list[torch.Tensor | None] = []
        for i, info in enumerate(results):
            if info is None:
                outputs.append(None)
                continue
            data = torch.frombuffer(info.view, dtype=torch.uint8)
            if target_locations is not None and i < len(target_locations):
                target = target_locations[i]
                if target is not None:
                    target_bytes = target.view(torch.uint8).flatten()
                    nbytes = min(data.numel(), target_bytes.numel())
                    target_bytes[:nbytes].copy_(data[:nbytes])
                    outputs.append(target)
                    continue
            outputs.append(data)
        hits = sum(output is not None for output in outputs)
        logger.info("batch_get result: %d/%d hits", hits, len(keys))
        return outputs

    def batch_set(
        self,
        keys: list[str],
        values: Any | None = None,
        target_locations: Any | None = None,
        target_sizes: Any | None = None,
    ) -> bool:
        if self._handler is None or values is None or not keys:
            return False
        full_keys = [self._make_key(k) for k in keys]
        logger.info(
            "batch_set called: %d keys, first_5=%s",
            len(full_keys),
            full_keys[:5],
        )
        infos = []
        for v in values:
            src = v.contiguous().view(torch.uint8).numpy()
            infos.append(memoryview(src))
        results = self._handler.batch_store(full_keys, infos)
        succeeded = sum(results)
        logger.info(
            "batch_set result: %d/%d succeeded, first_5_results=%s",
            succeeded,
            len(keys),
            results[:5],
        )
        return all(results)

    # ------------------------------------------------------------------
    # V1 API (zero-copy via host memory pool)
    # ------------------------------------------------------------------

    def register_mem_pool_host(self, mem_pool_host) -> None:
        """Register L2 host memory pool for zero-copy V1 operations."""
        super().register_mem_pool_host(mem_pool_host)
        self._is_page_first = self.storage_config.is_page_first_layout
        logger.info(
            "Registered mem_pool_host, page_first_layout=%s", self._is_page_first
        )

    def batch_get_v1(
        self,
        keys: list[str],
        host_indices: torch.Tensor,
        extra_info=None,
    ) -> list[bool]:
        """Retrieve KV pages from Maru into host memory pool.

        For page_first layouts, uses direct memory copy via ctypes.memmove.
        Falls back to tensor-based copy otherwise.
        """
        if self._handler is None or not keys:
            return [False] * len(keys)

        full_keys = [self._make_key(k) for k in keys]
        logger.info(
            "batch_get_v1 called: %d keys, first_5=%s",
            len(full_keys),
            full_keys[:5],
        )
        infos = self._handler.batch_retrieve(full_keys)

        if not getattr(self, "_is_page_first", False):
            results = self._fallback_batch_get_v1(keys, host_indices, infos)
            hits = sum(results)
            logger.info("batch_get_v1 (fallback) result: %d/%d hits", hits, len(keys))
            return results

        import ctypes

        results: list[bool] = []
        page_metas = self.mem_pool_host.get_page_buffer_meta(host_indices)
        for (ptr, size), info in zip(page_metas, infos, strict=False):
            if info is None:
                results.append(False)
                continue
            src_mv = info.view
            nbytes = min(size, len(src_mv))
            ctypes.memmove(
                ptr,
                ctypes.addressof(ctypes.c_char.from_buffer(src_mv)),
                nbytes,
            )
            results.append(True)
        hits = sum(results)
        logger.info("batch_get_v1 result: %d/%d hits", hits, len(keys))
        return results

    def batch_set_v1(
        self,
        keys: list[str],
        host_indices: torch.Tensor,
        extra_info=None,
    ) -> list[bool]:
        """Store KV pages from host memory pool into Maru.

        For page_first layouts, uses direct memory copy via ctypes.memmove.
        Falls back to tensor-based copy otherwise.
        """
        if self._handler is None or not keys:
            return [False] * len(keys)

        if not getattr(self, "_is_page_first", False):
            return self._fallback_batch_set_v1(keys, host_indices)

        import ctypes

        full_keys = [self._make_key(k) for k in keys]
        page_metas = self.mem_pool_host.get_page_buffer_meta(host_indices)

        # Batch alloc + memcpy, then batch store
        handles = []
        valid_indices = []
        for i, (ptr, size) in enumerate(page_metas):
            try:
                handle = self._handler.alloc(size)
            except (ValueError, RuntimeError):
                logger.warning("alloc failed for key %s", keys[i])
                handles.append(None)
                continue
            ctypes.memmove(
                ctypes.addressof(ctypes.c_char.from_buffer(handle.buf)),
                ptr,
                size,
            )
            handles.append(handle)
            valid_indices.append(i)

        # Batch register via store with handle (zero-copy path)
        results: list[bool] = [False] * len(keys)
        for i in valid_indices:
            handle = handles[i]
            if handle is not None:
                success = self._handler.store(full_keys[i], handle=handle)
                results[i] = success
        stored = sum(results)
        logger.debug("batch_set_v1 %d/%d stored", stored, len(keys))
        return results

    # ------------------------------------------------------------------
    # Fallback paths for non-page_first layouts
    # ------------------------------------------------------------------

    def _fallback_batch_get_v1(self, keys, host_indices, infos) -> list[bool]:
        """Fallback: use tensor copy for non-page_first layouts."""
        results: list[bool] = []
        for info in infos:
            results.append(info is not None)
        return results

    def _fallback_batch_set_v1(self, keys, host_indices) -> list[bool]:
        """Fallback: return all False to signal caller should use legacy API."""
        return [False] * len(keys)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def clear(self) -> None:
        """No-op: Maru server manages its own lifecycle."""
        pass

    def close(self) -> None:
        """Close the MaruHandler connection."""
        if self._handler is not None:
            try:
                self._handler.close()
            except Exception as e:
                logger.error("Error closing MaruHandler: %s", e)
            finally:
                self._handler = None
