# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruKVConnector - vLLM KV Connector using Maru CXL shared memory.

This connector allows vLLM instances on the same node to share KV cache
through CXL shared memory via Maru, bypassing network-based transfer.

Architecture:
    vLLM Scheduler/Worker -> MaruKVConnector -> MaruHandler -> CXL (zero-copy)

The connector has two roles (instantiated separately by vLLM):
    - SCHEDULER: Tracks which tokens have cached KV in Maru, builds metadata
    - WORKER: Performs actual GPU <-> CXL data transfers
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import torch

from vllm.distributed.kv_transfer.kv_connector.v1.base import (
    KVConnectorBase_V1,
    KVConnectorMetadata,
    KVConnectorRole,
)
from vllm.logger import init_logger
from vllm.v1.attention.backend import AttentionMetadata
from vllm.v1.core.sched.output import SchedulerOutput

if TYPE_CHECKING:
    from vllm.config import VllmConfig
    from vllm.forward_context import ForwardContext
    from vllm.v1.core.kv_cache_manager import KVCacheBlocks
    from vllm.v1.kv_cache_interface import KVCacheConfig
    from vllm.v1.request import Request

logger = init_logger(__name__)


def _parse_size(size_str: str | int) -> int:
    """Parse human-readable size string (e.g., '4G', '500M') to bytes."""
    if isinstance(size_str, int):
        return size_str
    match = re.match(r"^(\d+(?:\.\d+)?)\s*([KMGT]?)B?$", str(size_str).upper())
    if not match:
        return int(size_str)
    value, unit = float(match.group(1)), match.group(2)
    multipliers = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    return int(value * multipliers.get(unit, 1))


def _align_to_block_size(num_tokens: int, block_size: int) -> int:
    """Align the number of tokens down to the block size boundary."""
    return (num_tokens - 1) // block_size * block_size


def _token_hash(token_ids: torch.Tensor) -> str:
    """Generate a stable hash string from token IDs for use as maru key."""
    from vllm.utils.hashing import safe_hash
    token_bytes = token_ids.numpy().tobytes()
    return safe_hash(token_bytes, usedforsecurity=False).hexdigest()


# ============================================================================
# Metadata: passed from scheduler to worker each step
# ============================================================================


@dataclass
class MaruReqMeta:
    """Metadata for a single request's KV cache operation."""
    req_id: str
    token_ids: torch.Tensor
    slot_mapping: torch.Tensor
    is_store: bool  # True = save to maru, False = load from maru

    @staticmethod
    def make(
        req_id: str,
        token_ids: list[int],
        block_ids: list[int],
        block_size: int,
        is_store: bool,
    ) -> MaruReqMeta:
        valid_num_tokens = _align_to_block_size(len(token_ids), block_size)
        token_ids_tensor = torch.tensor(token_ids)[:valid_num_tokens]
        block_ids_tensor = torch.tensor(block_ids)
        num_blocks = block_ids_tensor.shape[0]
        block_offsets = torch.arange(0, block_size)
        slot_mapping = (
            block_offsets.reshape((1, block_size))
            + block_ids_tensor.reshape((num_blocks, 1)) * block_size
        )
        slot_mapping = slot_mapping.flatten()[:valid_num_tokens]
        return MaruReqMeta(
            req_id=req_id,
            token_ids=token_ids_tensor,
            slot_mapping=slot_mapping,
            is_store=is_store,
        )


@dataclass
class MaruConnectorMetadata(KVConnectorMetadata):
    """Metadata communicated from scheduler to worker each step."""
    requests: list[MaruReqMeta] = field(default_factory=list)

    def add_request(
        self,
        req_id: str,
        token_ids: list[int],
        block_ids: list[int],
        block_size: int,
        is_store: bool,
    ) -> None:
        self.requests.append(
            MaruReqMeta.make(req_id, token_ids, block_ids, block_size, is_store)
        )


# ============================================================================
# Main Connector
# ============================================================================


class MaruKVConnector(KVConnectorBase_V1):
    """vLLM KV Connector that uses Maru CXL shared memory for KV cache sharing.

    Supports same-node KV cache sharing between multiple vLLM instances
    through CXL shared memory. Data path is zero-copy on the CXL side;
    the only copies are GPU <-> CPU (unavoidable with current hardware).

    Configuration via kv_connector_extra_config:
        maru_server_url: str    - MaruServer address (default: tcp://localhost:5555)
        maru_pool_size: str|int - CXL pool size (default: 1G, supports '4G', '500M')
        maru_instance_id: str   - Unique instance ID (default: auto-generated)
        maru_chunk_size: str|int - Chunk size for maru pages (default: 4M)
        maru_eager_map: bool    - Pre-map shared regions on connect (default: true)
    """

    def __init__(
        self,
        vllm_config: VllmConfig,
        role: KVConnectorRole,
        kv_cache_config: KVCacheConfig | None = None,
    ):
        super().__init__(vllm_config, role, kv_cache_config)

        self._block_size = vllm_config.cache_config.block_size
        extra = self._kv_transfer_config.kv_connector_extra_config

        if role == KVConnectorRole.SCHEDULER:
            self._scheduler = MaruSchedulerConnector(
                block_size=self._block_size,
                extra_config=extra,
            )
            self._worker = None
        elif role == KVConnectorRole.WORKER:
            self._scheduler = None
            self._worker = MaruWorkerConnector(
                block_size=self._block_size,
                extra_config=extra,
            )

    # ==================================
    # Scheduler-side methods
    # ==================================

    def get_num_new_matched_tokens(
        self,
        request: Request,
        num_computed_tokens: int,
    ) -> tuple[int | None, bool]:
        assert self._scheduler is not None
        return self._scheduler.get_num_new_matched_tokens(
            request, num_computed_tokens
        )

    def update_state_after_alloc(
        self,
        request: Request,
        blocks: KVCacheBlocks,
        num_external_tokens: int,
    ):
        assert self._scheduler is not None
        self._scheduler.update_state_after_alloc(
            request, blocks, num_external_tokens
        )

    def build_connector_meta(
        self,
        scheduler_output: SchedulerOutput,
    ) -> KVConnectorMetadata:
        assert self._scheduler is not None
        return self._scheduler.build_connector_meta(scheduler_output)

    # ==================================
    # Worker-side methods
    # ==================================

    def register_kv_caches(self, kv_caches: dict[str, torch.Tensor]):
        assert self._worker is not None
        self._worker.register_kv_caches(kv_caches)

    def start_load_kv(
        self, forward_context: ForwardContext, **kwargs: Any
    ) -> None:
        assert self._worker is not None
        metadata = self._get_connector_metadata()
        assert isinstance(metadata, MaruConnectorMetadata)
        self._worker.start_load_kv(forward_context, metadata)

    def wait_for_layer_load(self, layer_name: str) -> None:
        # Maru loads all layers at once in start_load_kv, no per-layer wait
        pass

    def save_kv_layer(
        self,
        layer_name: str,
        kv_layer: torch.Tensor,
        attn_metadata: AttentionMetadata,
        **kwargs: Any,
    ) -> None:
        assert self._worker is not None
        metadata = self._get_connector_metadata()
        assert isinstance(metadata, MaruConnectorMetadata)
        self._worker.save_kv_layer(layer_name, kv_layer, attn_metadata, metadata)

    def wait_for_save(self):
        # All saves are synchronous (CXL mmap write is immediate)
        pass

    def shutdown(self):
        if self._worker is not None:
            self._worker.shutdown()


# ============================================================================
# Scheduler-side implementation
# ============================================================================


class MaruSchedulerConnector:
    """Scheduler-side logic: tracks what's cached in Maru, builds metadata."""

    def __init__(self, block_size: int, extra_config: dict[str, Any]):
        self._block_size = block_size

        # Lazy-init MaruHandler for exists checks
        self._handler = None
        self._extra_config = extra_config

        # Requests that need KV loaded from maru
        self._requests_need_load: dict[str, Request] = {}

        # Local cache of known keys (avoid repeated RPC)
        self._known_keys: set[str] = set()

    def _ensure_handler(self):
        """Lazily initialize MaruHandler for metadata queries."""
        if self._handler is not None:
            return

        from maru import MaruConfig, MaruHandler

        server_url = self._extra_config.get(
            "maru_server_url", "tcp://localhost:5555"
        )
        pool_size = _parse_size(
            self._extra_config.get("maru_pool_size", 1024**3)
        )
        chunk_size = _parse_size(
            self._extra_config.get("maru_chunk_size", 4 * 1024 * 1024)
        )
        instance_id = self._extra_config.get("maru_instance_id")
        eager_map = self._extra_config.get("maru_eager_map", True)

        cfg = MaruConfig(
            server_url=server_url,
            pool_size=pool_size,
            chunk_size_bytes=chunk_size,
            instance_id=instance_id,
            auto_connect=False,
            eager_map=eager_map,
        )
        self._handler = MaruHandler(cfg)
        if not self._handler.connect():
            logger.error("MaruSchedulerConnector: failed to connect to MaruServer")
            self._handler = None

    def _make_key(self, token_ids: list[int]) -> str:
        """Generate a maru key from token IDs (block-aligned prefix)."""
        num_tokens = _align_to_block_size(len(token_ids), self._block_size)
        t = torch.tensor(token_ids)[:num_tokens]
        return f"vllm_kv_{_token_hash(t)}"

    def _has_cached_kv(self, request: Request) -> bool:
        """Check if KV cache for this request's prompt exists in Maru."""
        token_ids = list(request.prompt_token_ids or [])
        if len(token_ids) <= 1:
            return False

        key = self._make_key(token_ids)
        if key in self._known_keys:
            return True

        self._ensure_handler()
        if self._handler is None:
            return False

        # Check first layer as sentinel (if first layer exists, all do)
        sentinel_key = f"{key}_layer0"
        try:
            exists = self._handler.exists(sentinel_key)
            if exists:
                self._known_keys.add(key)
            return exists
        except Exception as e:
            logger.warning("Maru exists check failed: %s", e)
            return False

    def get_num_new_matched_tokens(
        self,
        request: Request,
        num_computed_tokens: int,
    ) -> tuple[int | None, bool]:
        if not self._has_cached_kv(request):
            return 0, False

        logger.info(
            "Maru KV cache hit for request %s", request.request_id
        )

        token_ids = list(request.prompt_token_ids or [])
        num_tokens_to_check = _align_to_block_size(
            len(token_ids) - 1, self._block_size
        )
        matched = num_tokens_to_check - num_computed_tokens
        if matched <= 0:
            return 0, False

        return matched, False

    def update_state_after_alloc(
        self,
        request: Request,
        blocks: KVCacheBlocks,
        num_external_tokens: int,
    ):
        if num_external_tokens > 0:
            self._requests_need_load[request.request_id] = request

    def build_connector_meta(
        self,
        scheduler_output: SchedulerOutput,
    ) -> KVConnectorMetadata:
        meta = MaruConnectorMetadata()

        for new_req in scheduler_output.scheduled_new_reqs:
            token_ids = list(new_req.prompt_token_ids or [])
            if new_req.req_id in self._requests_need_load:
                # Load from maru
                meta.add_request(
                    req_id=new_req.req_id,
                    token_ids=token_ids,
                    block_ids=new_req.block_ids[0],
                    block_size=self._block_size,
                    is_store=False,
                )
            else:
                # Store to maru (new prompt, not yet cached)
                key = self._make_key(token_ids)
                if key not in self._known_keys:
                    meta.add_request(
                        req_id=new_req.req_id,
                        token_ids=token_ids,
                        block_ids=new_req.block_ids[0],
                        block_size=self._block_size,
                        is_store=True,
                    )

        # Handle resumed requests
        cached_reqs = scheduler_output.scheduled_cached_reqs
        for i, req_id in enumerate(cached_reqs.req_ids):
            resumed = req_id in cached_reqs.resumed_req_ids
            if not resumed or req_id not in self._requests_need_load:
                continue

            num_computed_tokens = cached_reqs.num_computed_tokens[i]
            num_new_tokens = scheduler_output.num_scheduled_tokens[req_id]
            new_block_ids = cached_reqs.new_block_ids[i]

            request = self._requests_need_load[req_id]
            total_tokens = num_computed_tokens + num_new_tokens
            token_ids = list(request.all_token_ids[:total_tokens])

            assert new_block_ids is not None
            block_ids = new_block_ids[0]

            meta.add_request(
                req_id=req_id,
                token_ids=token_ids,
                block_ids=block_ids,
                block_size=self._block_size,
                is_store=False,
            )

        self._requests_need_load.clear()
        return meta


# ============================================================================
# Worker-side implementation
# ============================================================================


class MaruWorkerConnector:
    """Worker-side logic: performs actual GPU <-> CXL data transfers."""

    def __init__(self, block_size: int, extra_config: dict[str, Any]):
        self._block_size = block_size
        self._extra_config = extra_config

        # Lazy-init MaruHandler
        self._handler = None

        # Registered GPU KV caches (layer_name -> tensor)
        self._kv_caches: dict[str, torch.Tensor] = {}

        # Track stored keys to avoid re-storing
        self._stored_keys: set[str] = set()

    def _ensure_handler(self):
        """Lazily initialize MaruHandler for data operations."""
        if self._handler is not None:
            return

        from maru import MaruConfig, MaruHandler

        server_url = self._extra_config.get(
            "maru_server_url", "tcp://localhost:5555"
        )
        pool_size = _parse_size(
            self._extra_config.get("maru_pool_size", 1024**3)
        )
        chunk_size = _parse_size(
            self._extra_config.get("maru_chunk_size", 4 * 1024 * 1024)
        )
        instance_id = self._extra_config.get("maru_instance_id")
        eager_map = self._extra_config.get("maru_eager_map", True)

        cfg = MaruConfig(
            server_url=server_url,
            pool_size=pool_size,
            chunk_size_bytes=chunk_size,
            instance_id=instance_id,
            auto_connect=False,
            eager_map=eager_map,
        )
        self._handler = MaruHandler(cfg)
        if not self._handler.connect():
            logger.error("MaruWorkerConnector: failed to connect to MaruServer")
            self._handler = None

    def register_kv_caches(self, kv_caches: dict[str, torch.Tensor]):
        """Register GPU KV cache tensors for later access."""
        self._kv_caches = kv_caches
        logger.info(
            "MaruWorkerConnector: registered %d KV cache layers", len(kv_caches)
        )

    def _make_key(self, token_ids: torch.Tensor) -> str:
        """Generate base key from token IDs."""
        return f"vllm_kv_{_token_hash(token_ids)}"

    def start_load_kv(
        self,
        forward_context: ForwardContext,
        metadata: MaruConnectorMetadata,
    ) -> None:
        """Load KV caches from Maru CXL into GPU paged buffers."""
        self._ensure_handler()
        if self._handler is None:
            logger.error("Cannot load KV: MaruHandler not connected")
            return

        attn_metadata = forward_context.attn_metadata
        if attn_metadata is None:
            return

        for req_meta in metadata.requests:
            if req_meta.is_store:
                continue

            base_key = self._make_key(req_meta.token_ids)
            num_loaded = 0

            for layer_idx, layer_name in enumerate(
                forward_context.no_compile_layers
            ):
                layer = forward_context.no_compile_layers[layer_name]
                kv_cache_attr = getattr(layer, "kv_cache", None)
                if kv_cache_attr is None:
                    continue

                kv_cache_layer = kv_cache_attr[forward_context.virtual_engine]
                maru_key = f"{base_key}_layer{layer_idx}"

                try:
                    info = self._handler.retrieve(maru_key)
                    if info is None:
                        logger.warning(
                            "Maru load: key %s not found for layer %s",
                            maru_key, layer_name,
                        )
                        continue

                    # CXL memoryview -> CPU tensor -> GPU
                    kv_data = torch.frombuffer(
                        info.view, dtype=kv_cache_layer.dtype
                    ).cuda()

                    self._inject_kv_into_layer(
                        kv_cache_layer,
                        kv_data,
                        req_meta.slot_mapping,
                        attn_metadata,
                        layer_name,
                    )
                    num_loaded += 1
                except Exception as e:
                    logger.error(
                        "Maru load failed for %s layer %s: %s",
                        maru_key, layer_name, e,
                    )

            if num_loaded > 0:
                logger.info(
                    "Maru: loaded %d layers of KV cache (%d tokens) for req %s",
                    num_loaded,
                    len(req_meta.token_ids),
                    req_meta.req_id,
                )

    def save_kv_layer(
        self,
        layer_name: str,
        kv_layer: torch.Tensor,
        attn_metadata: AttentionMetadata,
        metadata: MaruConnectorMetadata,
    ) -> None:
        """Save a single layer's KV cache from GPU to Maru CXL."""
        self._ensure_handler()
        if self._handler is None:
            return

        # Determine layer index from layer_name
        layer_idx = self._get_layer_index(layer_name)

        for req_meta in metadata.requests:
            if not req_meta.is_store:
                continue

            base_key = self._make_key(req_meta.token_ids)
            maru_key = f"{base_key}_layer{layer_idx}"

            if maru_key in self._stored_keys:
                continue

            try:
                # Extract KV from GPU paged buffer -> CPU
                kv_data = self._extract_kv_from_layer(
                    kv_layer,
                    req_meta.slot_mapping,
                    attn_metadata,
                )
                cpu_data = kv_data.detach().cpu().contiguous()

                # CPU tensor -> bytes -> Maru CXL store
                data_bytes = cpu_data.numpy().tobytes()
                data_mv = memoryview(data_bytes)

                success = self._handler.store(maru_key, data=data_mv)
                if success:
                    self._stored_keys.add(maru_key)

                    # If this is layer 0, it serves as sentinel for
                    # scheduler-side exists checks
                    if layer_idx == 0:
                        logger.debug(
                            "Maru: stored sentinel key %s (%d bytes)",
                            maru_key, len(data_bytes),
                        )
                else:
                    logger.warning("Maru store failed for %s", maru_key)

            except Exception as e:
                logger.error(
                    "Maru save failed for %s: %s", maru_key, e
                )

    def shutdown(self):
        """Clean up MaruHandler connection."""
        if self._handler is not None:
            try:
                self._handler.close()
            except Exception as e:
                logger.error("Error closing MaruHandler: %s", e)
            self._handler = None

    # ==============================
    # Helper methods
    # ==============================

    def _get_layer_index(self, layer_name: str) -> int:
        """Extract numeric layer index from layer name.

        Layer names follow patterns like:
          'model.layers.0.self_attn' -> 0
          'model.layers.15.self_attn' -> 15
        Falls back to sequential index in registered kv_caches.
        """
        import re as re_mod
        match = re_mod.search(r"layers\.(\d+)", layer_name)
        if match:
            return int(match.group(1))

        # Fallback: use position in kv_caches dict
        for idx, name in enumerate(self._kv_caches):
            if name == layer_name:
                return idx
        return 0

    def _inject_kv_into_layer(
        self,
        dst_kv_cache: torch.Tensor,
        src_kv_data: torch.Tensor,
        slot_mapping: torch.Tensor,
        attn_metadata: AttentionMetadata,
        layer_name: str,
    ) -> None:
        """Inject loaded KV cache data into the paged GPU buffer.

        Handles different attention backend layouts:
        - MLA: [num_pages, page_size, ...]
        - Triton: [num_blocks, num_kv_heads, block_size, head_dim]
        - Default (Flash): [2, num_pages, page_size, ...]
        """
        from vllm.model_executor.layers.attention.mla_attention import (
            MLACommonMetadata,
        )
        from vllm.v1.attention.backends.triton_attn import (
            TritonAttentionMetadata,
        )

        # Resolve per-layer metadata if attn_metadata is a dict
        layer_meta = (
            attn_metadata[layer_name]
            if isinstance(attn_metadata, dict)
            else attn_metadata
        )

        if isinstance(layer_meta, MLACommonMetadata):
            num_pages = dst_kv_cache.shape[0]
            page_size = dst_kv_cache.shape[1]
            flat = dst_kv_cache.reshape(num_pages * page_size, -1)
            # Reshape src to match
            src_reshaped = src_kv_data.reshape(slot_mapping.shape[0], -1)
            flat[slot_mapping] = src_reshaped
        elif isinstance(layer_meta, TritonAttentionMetadata):
            block_idxs = slot_mapping // self._block_size
            offsets = slot_mapping % self._block_size
            src_reshaped = src_kv_data.reshape(
                slot_mapping.shape[0],
                dst_kv_cache.shape[1],  # num_kv_heads
                -1,
            )
            dst_kv_cache[block_idxs, :, offsets] = src_reshaped
        else:
            # Default: Flash attention layout [2, num_pages, page_size, ...]
            num_pages = dst_kv_cache.shape[1]
            page_size = dst_kv_cache.shape[2]
            flat = dst_kv_cache.reshape(2, num_pages * page_size, -1)
            src_reshaped = src_kv_data.reshape(2, slot_mapping.shape[0], -1)
            flat[:, slot_mapping] = src_reshaped

    def _extract_kv_from_layer(
        self,
        kv_layer: torch.Tensor,
        slot_mapping: torch.Tensor,
        attn_metadata: AttentionMetadata,
    ) -> torch.Tensor:
        """Extract KV cache data from GPU paged buffer for given slots.

        Returns a contiguous tensor with the KV data for the given slots.
        """
        from vllm.model_executor.layers.attention.mla_attention import (
            MLACommonMetadata,
        )
        from vllm.v1.attention.backends.triton_attn import (
            TritonAttentionMetadata,
        )

        if isinstance(attn_metadata, MLACommonMetadata):
            num_pages, page_size = kv_layer.shape[0], kv_layer.shape[1]
            flat = kv_layer.reshape(num_pages * page_size, -1)
            return flat[slot_mapping]
        elif isinstance(attn_metadata, TritonAttentionMetadata):
            block_idxs = slot_mapping // self._block_size
            offsets = slot_mapping % self._block_size
            return kv_layer[block_idxs, :, offsets]
        else:
            # Flash attention: [2, num_pages, page_size, ...]
            num_pages, page_size = kv_layer.shape[1], kv_layer.shape[2]
            flat = kv_layer.reshape(2, num_pages * page_size, -1)
            return flat[:, slot_mapping]
