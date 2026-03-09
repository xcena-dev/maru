# SPDX-License-Identifier: Apache-2.0
"""
MaruConnector — bridges upstream LMCache's RemoteConnector interface to
Maru's MaruHandler for CXL shared-memory KV cache storage.

Key design points:
- Key conversion: CacheEngineKey → string key (via to_string())
- Zero-copy bridging: MemoryInfo (memoryview) ↔ MemoryObj (torch tensor)
- Async wrapping: asyncio.to_thread() around MaruHandler's sync API
- Batch operations: batch_retrieve / batch_store / batch_exists
"""

import asyncio
import builtins
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import parse_qs, urlparse

import torch
from lmcache.utils import CacheEngineKey
from lmcache.v1.config import LMCacheEngineConfig
from lmcache.v1.memory_management import MemoryObj
from lmcache.v1.metadata import LMCacheMetadata
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector

logger = logging.getLogger(__name__)

_PERF_ENABLED = os.environ.get("LMCACHE_PERF_LOG", "0") == "1"


def _perf_log(elapsed_ms: float, msg: str) -> None:
    if _PERF_ENABLED:
        print(f"[PERF][{elapsed_ms:.2f}ms][maru_connector]: {msg}", flush=True)


# ---------------------------------------------------------------------------
# Size parsing
# ---------------------------------------------------------------------------


def parse_size(size_str: str) -> int:
    """Parse human-readable size string (e.g., '1G', '500M') to bytes."""
    if isinstance(size_str, int):
        return size_str
    match = re.match(r"^(\d+(?:\.\d+)?)\s*([KMGT]?)B?$", str(size_str).upper())
    if not match:
        return int(size_str)
    value, unit = float(match.group(1)), match.group(2)
    multipliers = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    return int(value * multipliers.get(unit, 1))


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class MaruConnectorConfig:
    """Configuration for the Maru connector."""

    server_url: str = "tcp://localhost:5555"
    pool_size: int = 1024 * 1024 * 1024  # 1 GB
    instance_id: str | None = None
    auto_connect: bool = True
    connection_timeout: float = 30.0
    operation_timeout: float = 10.0
    timeout_ms: int = 2000
    use_async_rpc: bool = True
    max_inflight: int = 64
    eager_map: bool | None = None

    @staticmethod
    def from_url(url: str) -> "MaruConnectorConfig":
        """Parse ``maru://host:port?pool_size=1G&timeout=30``."""
        parsed = urlparse(url)
        host = parsed.hostname or "localhost"
        port = parsed.port or 5555
        params = parse_qs(parsed.query)
        return MaruConnectorConfig(
            server_url=f"tcp://{host}:{port}",
            pool_size=parse_size(params.get("pool_size", ["1G"])[0]),
            instance_id=params.get("instance_id", [None])[0],
            connection_timeout=float(params.get("timeout", ["30.0"])[0]),
            operation_timeout=float(params.get("op_timeout", ["10.0"])[0]),
        )

    @staticmethod
    def from_lmcache_config(
        config: LMCacheEngineConfig,
        fallback: Optional["MaruConnectorConfig"] = None,
    ) -> "MaruConnectorConfig":
        """Build from ``extra_config``, falling back to *fallback* for unset keys."""
        extra = config.extra_config or {}
        fb = fallback or MaruConnectorConfig()

        raw_pool = extra.get("maru_pool_size", fb.pool_size)
        pool_size = parse_size(raw_pool) if isinstance(raw_pool, str) else int(raw_pool)

        return MaruConnectorConfig(
            server_url=extra.get("maru_server_url", fb.server_url),
            pool_size=pool_size,
            instance_id=extra.get("maru_instance_id", fb.instance_id),
            auto_connect=extra.get("maru_auto_connect", fb.auto_connect),
            operation_timeout=float(
                extra.get("maru_operation_timeout", fb.operation_timeout)
            ),
            timeout_ms=int(extra.get("maru_timeout_ms", fb.timeout_ms)),
            use_async_rpc=extra.get("maru_use_async_rpc", fb.use_async_rpc),
            max_inflight=int(extra.get("maru_max_inflight", fb.max_inflight)),
            eager_map=extra.get("maru_eager_map", fb.eager_map),
        )


# ---------------------------------------------------------------------------
# Key conversion
# ---------------------------------------------------------------------------


def cache_key_to_str(key: CacheEngineKey) -> str:
    """Convert CacheEngineKey to string key for Maru storage."""
    return key.to_string()


# ---------------------------------------------------------------------------
# Ping error codes
# ---------------------------------------------------------------------------

PING_SUCCESS = 0
PING_NOT_CONNECTED = 1
PING_RPC_ERROR = 2


# ---------------------------------------------------------------------------
# MaruConnector
# ---------------------------------------------------------------------------


class MaruConnector(RemoteConnector):
    """
    Upstream-LMCache-compatible connector backed by Maru shared memory.

    This class inherits from upstream ``RemoteConnector`` and delegates all
    storage operations to ``maru.MaruHandler``.
    """

    def __init__(
        self,
        url: str,
        loop: asyncio.AbstractEventLoop,
        config: LMCacheEngineConfig,
        metadata: LMCacheMetadata,
        maru_config: MaruConnectorConfig,
    ):
        logger.info("Initializing MaruConnector for url=%s", url)
        super().__init__(config, metadata)

        self.url = url
        self.loop = loop
        self.maru_config = maru_config

        # MaruHandler (lazy init)
        self._handle = None
        self._connected = False

        if self.maru_config.auto_connect:
            self._init_handle()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _init_handle(self) -> bool:
        try:
            from maru import MaruConfig, MaruHandler
        except ImportError:
            logger.warning("maru package not installed. Install with: pip install maru")
            return False

        try:
            cfg_kwargs = {
                "server_url": self.maru_config.server_url,
                "instance_id": self.maru_config.instance_id,
                "pool_size": self.maru_config.pool_size,
                "chunk_size_bytes": self.full_chunk_size_bytes,
                "auto_connect": False,
                "timeout_ms": self.maru_config.timeout_ms,
                "use_async_rpc": self.maru_config.use_async_rpc,
                "max_inflight": self.maru_config.max_inflight,
            }
            if self.maru_config.eager_map is not None:
                cfg_kwargs["eager_map"] = self.maru_config.eager_map

            handle = MaruHandler(MaruConfig(**cfg_kwargs))
            self._handle = handle
            if handle.connect():
                self._connected = True
                logger.info("MaruHandler connected successfully")
                return True
            else:
                logger.warning("MaruHandler.connect() returned False")
                self._handle = None
                return False
        except Exception as e:
            logger.warning("Failed to initialize MaruHandler: %s", e)
            self._handle = None
            return False

    def _ensure_connected(self) -> bool:
        if self._connected and self._handle is not None:
            return True
        return self._init_handle()

    # ------------------------------------------------------------------
    # Zero-copy encode / decode
    # ------------------------------------------------------------------

    def _decode_memory_obj(self, info) -> MemoryObj | None:
        """MemoryInfo (memoryview) → TensorMemoryObj (zero-copy)."""
        from lmcache.v1.memory_management import MemoryObjMetadata, TensorMemoryObj

        mv = info.view
        raw_data = torch.frombuffer(mv, dtype=torch.uint8)

        meta = MemoryObjMetadata(
            shape=self.meta_shapes[0],
            dtype=self.meta_dtypes[0],
            address=0,
            phy_size=raw_data.numel(),
            ref_count=1,
            pin_count=0,
            fmt=self.meta_fmt,
            shapes=self.meta_shapes,
            dtypes=self.meta_dtypes,
        )

        return TensorMemoryObj(
            raw_data=raw_data,
            metadata=meta,
            parent_allocator=None,
        )

    @staticmethod
    def _encode_memory_obj(memory_obj: MemoryObj):
        """MemoryObj → MemoryInfo (zero-copy via byte_array)."""
        from maru_handler.memory import MemoryInfo

        return MemoryInfo(view=memory_obj.byte_array)

    # ------------------------------------------------------------------
    # Core operations (abstract method implementations)
    # ------------------------------------------------------------------

    async def exists(self, key: CacheEngineKey) -> bool:
        if not self._ensure_connected():
            return False
        assert self._handle is not None
        key_hash = cache_key_to_str(key)
        try:
            t0 = time.perf_counter()
            result = await asyncio.wait_for(
                asyncio.to_thread(self._handle.exists, key_hash),
                timeout=self.maru_config.operation_timeout,
            )
            _perf_log(
                (time.perf_counter() - t0) * 1000,
                f"exists key_hash={key_hash} result={result}",
            )
            return result
        except TimeoutError:
            logger.warning("exists timed out for key_hash=%s", key_hash)
            return False
        except Exception as e:
            logger.error("exists failed: %s", e)
            return False

    def exists_sync(self, key: CacheEngineKey) -> bool:
        if not self._ensure_connected():
            return False
        assert self._handle is not None
        key_hash = cache_key_to_str(key)
        try:
            return self._handle.exists(key_hash)
        except Exception as e:
            logger.error("exists_sync failed: %s", e)
            return False

    async def get(self, key: CacheEngineKey) -> MemoryObj | None:
        if not self._ensure_connected():
            return None
        assert self._handle is not None
        key_hash = cache_key_to_str(key)
        try:
            t0 = time.perf_counter()
            info = await asyncio.wait_for(
                asyncio.to_thread(self._handle.retrieve, key_hash),
                timeout=self.maru_config.operation_timeout,
            )
            if info is None:
                _perf_log(
                    (time.perf_counter() - t0) * 1000,
                    f"get key_hash={key_hash} MISS",
                )
                return None

            data_size = len(info.view)
            memory_obj = self._decode_memory_obj(info)
            if memory_obj is not None:
                memory_obj = self.reshape_partial_chunk(memory_obj, data_size)
            _perf_log(
                (time.perf_counter() - t0) * 1000,
                f"get key_hash={key_hash} bytes={data_size}",
            )
            return memory_obj
        except TimeoutError:
            logger.warning("get timed out for key_hash=%s", key_hash)
            return None
        except Exception as e:
            logger.error("get failed: %s", e)
            return None

    async def put(self, key: CacheEngineKey, memory_obj: MemoryObj) -> None:
        if not self._ensure_connected():
            raise RuntimeError("MaruConnector not connected")
        assert self._handle is not None
        key_hash = cache_key_to_str(key)

        t0 = time.perf_counter()
        info = self._encode_memory_obj(memory_obj)
        data_size = len(info.view)
        _perf_log((time.perf_counter() - t0) * 1000, f"put encode bytes={data_size}")

        try:
            t1 = time.perf_counter()
            success = await asyncio.wait_for(
                asyncio.to_thread(self._handle.store, key_hash, info),
                timeout=self.maru_config.operation_timeout,
            )
            _perf_log(
                (time.perf_counter() - t1) * 1000,
                f"put RPC key_hash={key_hash} bytes={data_size} ok={success}",
            )
            if not success:
                logger.warning("put failed for key_hash=%s", key_hash)
        except TimeoutError:
            logger.warning("put timed out for key_hash=%s", key_hash)
        except Exception as e:
            logger.error("put failed: %s", e)
            raise

    async def list(self) -> list[str]:
        logger.warning("list() not supported by Maru connector")
        return []

    async def close(self) -> None:
        logger.info("MaruConnector.close called")
        if self._handle is not None:
            try:
                self._handle.close()
            except Exception as e:
                logger.error("Error closing MaruHandler: %s", e)
            finally:
                self._handle = None
                self._connected = False

    # ------------------------------------------------------------------
    # Optional: remove
    # ------------------------------------------------------------------

    def remove_sync(self, key: CacheEngineKey) -> bool:
        if not self._ensure_connected():
            return False
        assert self._handle is not None
        key_hash = cache_key_to_str(key)
        try:
            return self._handle.delete(key_hash)
        except Exception as e:
            logger.error("remove_sync failed: %s", e)
            return False

    # ------------------------------------------------------------------
    # Optional: ping
    # ------------------------------------------------------------------

    def support_ping(self) -> bool:
        return True

    async def ping(self) -> int:
        if not self._connected or self._handle is None:
            return PING_NOT_CONNECTED
        try:
            healthy = await asyncio.wait_for(
                asyncio.to_thread(self._handle.healthcheck),
                timeout=self.maru_config.operation_timeout,
            )
            return PING_SUCCESS if healthy else PING_RPC_ERROR
        except Exception:
            return PING_RPC_ERROR

    # ------------------------------------------------------------------
    # Batch operations
    # ------------------------------------------------------------------

    def support_batched_get(self) -> bool:
        return True

    def support_batched_put(self) -> bool:
        return True

    def support_batched_async_contains(self) -> bool:
        return True

    def support_batched_contains(self) -> bool:
        return True

    def support_batched_get_non_blocking(self) -> bool:
        return True

    def batched_contains(self, keys: builtins.list[CacheEngineKey]) -> int:
        if not self._ensure_connected() or not keys:
            return 0
        assert self._handle is not None
        key_hashes = [cache_key_to_str(k) for k in keys]
        try:
            results = self._handle.batch_exists(key_hashes)
            count = 0
            for exists in results:
                if not exists:
                    break
                count += 1
            return count
        except Exception as e:
            logger.error("batched_contains failed: %s", e)
            return 0

    async def batched_async_contains(
        self,
        lookup_id: str,
        keys: builtins.list[CacheEngineKey],
        pin: bool = False,
    ) -> int:
        if not self._ensure_connected() or not keys:
            return 0
        assert self._handle is not None
        key_hashes = [cache_key_to_str(k) for k in keys]
        try:
            t0 = time.perf_counter()
            results = await asyncio.wait_for(
                asyncio.to_thread(self._handle.batch_exists, key_hashes),
                timeout=self.maru_config.operation_timeout,
            )
            count = 0
            for exists in results:
                if not exists:
                    break
                count += 1
            _perf_log(
                (time.perf_counter() - t0) * 1000,
                f"batch_contains n={len(keys)} hits={count}",
            )
            return count
        except TimeoutError:
            logger.warning("batched_async_contains timed out")
            return 0
        except Exception as e:
            logger.error("batched_async_contains failed: %s", e)
            return 0

    async def batched_get(
        self, keys: builtins.list[CacheEngineKey]
    ) -> builtins.list[MemoryObj | None]:
        if not self._ensure_connected() or not keys:
            return [None] * len(keys)
        assert self._handle is not None
        key_hashes = [cache_key_to_str(k) for k in keys]
        try:
            t0 = time.perf_counter()
            raw_results = await asyncio.wait_for(
                asyncio.to_thread(self._handle.batch_retrieve, key_hashes),
                timeout=self.maru_config.operation_timeout,
            )
            objs: list[MemoryObj | None] = []
            for info in raw_results:
                if info is None:
                    objs.append(None)
                    continue
                obj = self._decode_memory_obj(info)
                if obj is not None:
                    obj = self.reshape_partial_chunk(obj, len(info.view))
                objs.append(obj)
            hits = sum(1 for r in raw_results if r is not None)
            _perf_log(
                (time.perf_counter() - t0) * 1000,
                f"batch_get n={len(keys)} hits={hits}",
            )
            return objs
        except TimeoutError:
            logger.warning("batched_get timed out for %d keys", len(keys))
            return [None] * len(keys)
        except Exception as e:
            logger.error("batched_get failed: %s", e)
            return [None] * len(keys)

    async def batched_put(
        self,
        keys: builtins.list[CacheEngineKey],
        memory_objs: builtins.list[MemoryObj],
    ) -> None:
        if not self._ensure_connected() or not keys:
            return
        assert self._handle is not None
        key_hashes = [cache_key_to_str(k) for k in keys]

        t0 = time.perf_counter()
        infos = [self._encode_memory_obj(obj) for obj in memory_objs]
        total_bytes = sum(len(info.view) for info in infos)
        _perf_log(
            (time.perf_counter() - t0) * 1000,
            f"batch_put encode n={len(keys)} bytes={total_bytes}",
        )

        try:
            t1 = time.perf_counter()
            results = await asyncio.wait_for(
                asyncio.to_thread(self._handle.batch_store, key_hashes, infos),
                timeout=self.maru_config.operation_timeout,
            )
            stored = sum(results) if results else 0
            _perf_log(
                (time.perf_counter() - t1) * 1000,
                f"batch_put RPC n={len(keys)} stored={stored} bytes={total_bytes}",
            )
            if stored < len(keys):
                logger.warning(
                    "batch_put partial: stored %d/%d keys", stored, len(keys)
                )
        except TimeoutError:
            logger.warning("batched_put timed out for %d keys", len(keys))
        except Exception as e:
            logger.error("batched_put failed: %s", e)
            raise

    async def batched_get_non_blocking(
        self,
        lookup_id: str,
        keys: builtins.list[CacheEngineKey],
    ) -> builtins.list[MemoryObj]:
        if not self._ensure_connected() or not keys:
            return []
        assert self._handle is not None
        key_hashes = [cache_key_to_str(k) for k in keys]
        try:
            t0 = time.perf_counter()
            raw_results = await asyncio.wait_for(
                asyncio.to_thread(self._handle.batch_retrieve, key_hashes),
                timeout=self.maru_config.operation_timeout,
            )
            # Consecutive prefix of hits only
            objs: list[MemoryObj] = []
            for info in raw_results:
                if info is None:
                    break
                obj = self._decode_memory_obj(info)
                if obj is None:
                    break
                obj = self.reshape_partial_chunk(obj, len(info.view))
                objs.append(obj)
            _perf_log(
                (time.perf_counter() - t0) * 1000,
                f"batch_get_nb n={len(keys)} hits={len(objs)}",
            )
            return objs
        except TimeoutError:
            logger.warning("batched_get_non_blocking timed out")
            return []
        except Exception as e:
            logger.error("batched_get_non_blocking failed: %s", e)
            return []

    def __repr__(self) -> str:
        return (
            f"<MaruConnector server_url={self.maru_config.server_url}, "
            f"pool_size={self.maru_config.pool_size}, "
            f"connected={self._connected}>"
        )
