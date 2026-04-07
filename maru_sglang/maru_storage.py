# SPDX-License-Identifier: Apache-2.0
"""MaruStorage — SGLang HiCache L3 storage backend backed by Maru.

Implements the HiCacheStorage ABC from SGLang, delegating all storage
operations to MaruHandler for CXL shared-memory KV cache access.

TODO(CXL-as-L2): Place kv_buffer directly in CXL for L2=L3 zero-copy.
Blocker: HiCache slot lifecycle (protect freed slots from remote reads).

TODO(KV-split-pool): Non-MLA models keep K and V in separate host buffer
pools (non-contiguous).  Maru stores one contiguous block per key, so
batch_set_v1 must concat-copy K+V into a temp buffer before passing to
batch_store.  Adding separate K/V CXL pools would let each key map to
two regions, eliminating this intermediate copy.
See: _design/sglang-hicache-maru/260326_kv-memory-layout-mla-vs-non-mla.md

Usage (SGLang dynamic backend):
    python -m sglang.launch_server \\
        --enable-hierarchical-cache \\
        --hicache-storage-backend dynamic \\
        --hicache-storage-backend-extra-config '{
            "backend_name": "maru",
            "module_path": "maru_sglang.maru_storage",
            "class_name": "MaruStorage",
            "maru_server_url": "tcp://localhost:5555",
            "maru_pool_size": "4G"
        }'
"""

import logging
from typing import Any

import torch
from sglang.srt.mem_cache.hicache_storage import HiCacheStorage

from maru import MaruConfig, MaruHandler

from .config import MaruSGLangConfig

logger = logging.getLogger(__name__)


class MaruStorage(HiCacheStorage):
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
        self._connected: bool = False
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
                dax_path=cfg.dax_path,
            )
        )
        if handler.connect():
            self._handler = handler
            self._connected = True
            logger.info(
                "MaruStorage connected: server=%s, pool=%d, chunk=%d",
                cfg.server_url,
                cfg.pool_size,
                cfg.chunk_size_bytes,
            )
        else:
            self._connected = False
            raise RuntimeError(f"Failed to connect MaruHandler to {cfg.server_url}")

    def _ensure_connected(self) -> bool:
        """Check connection and attempt reconnect if needed."""
        if self._connected and self._handler is not None:
            return True
        try:
            self._connect()
            return self._connected
        except RuntimeError:
            logger.warning("Reconnection attempt failed")
            return False

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
        if not self._ensure_connected():
            return False
        try:
            return self._handler.exists(self._make_key(key))
        except Exception as e:
            logger.error("exists failed: %s", e)
            return False

    def batch_exists(self, keys: list[str], extra_info=None) -> int:
        """Return number of consecutive existing keys from the start."""
        if not self._ensure_connected() or not keys:
            return 0
        full_keys = [self._make_key(k) for k in keys]
        logger.debug(
            "batch_exists: %d keys",
            len(full_keys),
            # first_5=%s, full_keys[:5],
        )
        try:
            results = self._handler.batch_exists(full_keys)
        except Exception as e:
            logger.error("batch_exists failed: %s", e)
            return 0
        count = 0
        for r in results:
            if not r:
                break
            count += 1
        logger.debug("batch_exists result: %d/%d consecutive hits", count, len(keys))
        return count

    def get(
        self,
        key: str,
        target_location: Any | None = None,
        target_sizes: Any | None = None,
    ) -> torch.Tensor | None:
        if not self._ensure_connected():
            return None
        full_key = self._make_key(key)
        try:
            info = self._handler.retrieve(full_key)
        except Exception as e:
            logger.error("get failed: %s", e)
            return None
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
        if not self._ensure_connected() or value is None:
            return False
        full_key = self._make_key(key)
        try:
            src = value.contiguous().view(torch.uint8).numpy()
            mv = memoryview(src)
            ok = self._handler.store(full_key, data=mv)
        except Exception as e:
            logger.error("set failed: %s", e)
            return False
        logger.debug("set %s key=%s, %d bytes", "OK" if ok else "FAIL", key, len(mv))
        return ok

    def batch_get(
        self,
        keys: list[str],
        target_locations: Any | None = None,
        target_sizes: Any | None = None,
    ) -> list[torch.Tensor | None]:
        if not self._ensure_connected() or not keys:
            return [None] * len(keys)
        full_keys = [self._make_key(k) for k in keys]
        logger.debug(
            "batch_get: %d keys",
            len(full_keys),
        )
        try:
            results = self._handler.batch_retrieve(full_keys)
        except Exception as e:
            logger.error("batch_get failed: %s", e)
            return [None] * len(keys)

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
        logger.debug("batch_get result: %d/%d hits", hits, len(keys))
        return outputs

    def batch_set(
        self,
        keys: list[str],
        values: Any | None = None,
        target_locations: Any | None = None,
        target_sizes: Any | None = None,
    ) -> bool:
        if not self._ensure_connected() or values is None or not keys:
            return False
        full_keys = [self._make_key(k) for k in keys]
        logger.debug(
            "batch_set: %d keys",
            len(full_keys),
        )
        try:
            infos = []
            for v in values:
                src = v.contiguous().view(torch.uint8).numpy()
                infos.append(memoryview(src))
            results = self._handler.batch_store(full_keys, infos)
        except Exception as e:
            logger.error("batch_set failed: %s", e)
            return False
        succeeded = sum(results)
        logger.debug(
            "batch_set result: %d/%d succeeded",
            succeeded,
            len(keys),
        )
        return all(results)

    # ------------------------------------------------------------------
    # V1 API (zero-copy via host memory pool)
    # ------------------------------------------------------------------

    def register_mem_pool_host(self, mem_pool_host) -> None:
        """Register L2 host memory pool for zero-copy V1 operations."""
        layout = getattr(mem_pool_host, "layout", "")
        if not layout.startswith("page_first"):
            raise ValueError(
                f"MaruStorage requires page_first memory layout, got {layout!r}. "
                "Set --hicache-mem-layout page_first_direct"
            )
        super().register_mem_pool_host(mem_pool_host)
        logger.info("Registered mem_pool_host (layout=%s)", layout)

    def batch_get_v1(
        self,
        keys: list[str],
        host_indices: torch.Tensor,
        extra_info=None,
    ) -> list[bool]:
        """Retrieve KV pages from Maru into host memory pool via batch RPC."""
        if not self._ensure_connected() or not keys:
            return [False] * len(keys)

        full_keys = [self._make_key(k) for k in keys]
        logger.debug(
            "batch_get_v1: %d keys",
            len(full_keys),
        )
        try:
            infos = self._handler.batch_retrieve(full_keys)
        except Exception as e:
            logger.error("batch_get_v1 failed: %s", e)
            return [False] * len(keys)

        import ctypes

        host_ptrs, host_sizes = self.mem_pool_host.get_page_buffer_meta(host_indices)

        # Determine chunks-per-key ratio (non-MLA: 2 for K+V, MLA: 1).
        n_ptrs = len(host_ptrs)
        n_keys = len(keys)
        chunks_per_key = n_ptrs // n_keys
        expected = 1 if self.storage_config.is_mla_model else 2
        if chunks_per_key != expected:
            logger.warning(
                "chunks_per_key=%d but is_mla_model=%s (expected %d)",
                chunks_per_key,
                self.storage_config.is_mla_model,
                expected,
            )

        results: list[bool] = []
        for key_idx, info in enumerate(infos):
            if info is None:
                results.append(False)
                continue
            src_mv = info.view
            src_addr = ctypes.addressof(ctypes.c_char.from_buffer(src_mv))
            base = key_idx * chunks_per_key
            src_offset = 0
            for c in range(chunks_per_key):
                dst_ptr = host_ptrs[base + c]
                dst_size = host_sizes[base + c]
                nbytes = min(dst_size, len(src_mv) - src_offset)
                if nbytes <= 0:
                    break
                ctypes.memmove(dst_ptr, src_addr + src_offset, nbytes)
                src_offset += dst_size
            results.append(True)
        hits = sum(results)
        logger.debug("batch_get_v1 result: %d/%d hits", hits, n_keys)
        return results

    def batch_set_v1(
        self,
        keys: list[str],
        host_indices: torch.Tensor,
        extra_info=None,
    ) -> list[bool]:
        """Store KV pages from host memory pool into Maru.

        Collects host page data as zero-copy memoryviews and delegates
        to batch_store for a single batch RPC.
        """
        if not self._ensure_connected() or not keys:
            return [False] * len(keys)

        import ctypes

        full_keys = [self._make_key(k) for k in keys]
        try:
            host_ptrs, host_sizes = self.mem_pool_host.get_page_buffer_meta(
                host_indices
            )
        except Exception as e:
            logger.error("batch_set_v1 get_page_buffer_meta failed: %s", e)
            return [False] * len(keys)

        # Determine chunks-per-key ratio.
        # Non-MLA models return separate K and V entries (2 per key);
        # MLA models return a single combined entry (1 per key).
        n_ptrs = len(host_ptrs)
        n_keys = len(keys)
        chunks_per_key = n_ptrs // n_keys
        expected = 1 if self.storage_config.is_mla_model else 2
        if chunks_per_key != expected:
            logger.warning(
                "chunks_per_key=%d but is_mla_model=%s (expected %d)",
                chunks_per_key,
                self.storage_config.is_mla_model,
                expected,
            )

        # Collect host page data as memoryviews for batch_store.
        # MLA (1 chunk/key): zero-copy view from host page.
        # Non-MLA (K+V in separate buffer pools): concatenate into owned buffer.
        infos: list[memoryview] = []
        for key_idx in range(n_keys):
            base = key_idx * chunks_per_key
            if chunks_per_key == 1:
                src_size = host_sizes[base]
                src_view = (ctypes.c_char * src_size).from_address(host_ptrs[base])
                infos.append(memoryview(src_view))
            else:
                total_size = sum(host_sizes[base + c] for c in range(chunks_per_key))
                dst_buf = (ctypes.c_char * total_size)()
                dst_offset = 0
                for c in range(chunks_per_key):
                    src_ptr = host_ptrs[base + c]
                    src_size = host_sizes[base + c]
                    ctypes.memmove(
                        ctypes.addressof(dst_buf) + dst_offset, src_ptr, src_size
                    )
                    dst_offset += src_size
                infos.append(memoryview(dst_buf))

        try:
            results = self._handler.batch_store(full_keys, infos)
        except Exception as e:
            logger.error("batch_set_v1 batch_store failed: %s", e)
            return [False] * len(keys)

        stored = sum(results)
        logger.debug("batch_set_v1 %d/%d stored", stored, n_keys)
        return results

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
                logger.info("MaruStorage closed")
            except Exception as e:
                logger.error("Error closing MaruHandler: %s", e)
            finally:
                self._handler = None
                self._connected = False
