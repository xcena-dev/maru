# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruKVConnector - vLLM KV Connector using Maru CXL shared memory.

This connector allows vLLM instances on the same node to share KV cache
through CXL shared memory via Maru, bypassing network-based transfer.

Architecture:
    vLLM Scheduler/Worker -> MaruKVConnector -> MaruHandler -> CXL (zero-copy)

KV cache is stored in token-chunk granularity (default 256 tokens per chunk).
Each chunk is keyed by hash(prefix_tokens_up_to_chunk), enabling partial
prefix reuse across requests.

The connector has two roles (instantiated separately by vLLM):
    - SCHEDULER: Checks chunk-by-chunk which prefix is cached, builds metadata
    - WORKER: Performs actual GPU <-> CXL data transfers per chunk
"""

from __future__ import annotations

import re
import time
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

# Default number of tokens per chunk for KV cache storage
DEFAULT_KV_CHUNK_TOKENS = 256


# ============================================================================
# Utilities
# ============================================================================


def _parse_size(size_str: str | int) -> int:
    """Parse human-readable size string (e.g., '4G', '500M') to bytes."""
    if isinstance(size_str, int):
        return size_str
    match = re.match(r"^(\d+(?:\.\d+)?)\s*([KMGT]?)B?$", str(size_str).upper())
    if not match:
        try:
            return int(size_str)
        except ValueError:
            raise ValueError(
                f"Invalid size string: {size_str!r}. "
                f"Expected format: integer or human-readable like '4G', '500M'"
            ) from None
    value, unit = float(match.group(1)), match.group(2)
    multipliers = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    return int(value * multipliers.get(unit, 1))


def _align_down(num_tokens: int, block_size: int) -> int:
    """Align the number of tokens down to the block size boundary."""
    return (num_tokens // block_size) * block_size


def _token_hash(token_ids: torch.Tensor) -> str:
    """Generate a stable hash string from token IDs.

    Uses vLLM's safe_hash (hashlib-based), which is deterministic
    regardless of PYTHONHASHSEED.
    """
    from vllm.utils.hashing import safe_hash

    token_bytes = token_ids.numpy().tobytes()
    return safe_hash(token_bytes, usedforsecurity=False).hexdigest()[:16]


def _chunk_keys(token_ids: list[int], chunk_tokens: int) -> list[str]:
    """Generate maru keys for each chunk of the token prefix.

    Each chunk's key = hash of all tokens from the beginning up to
    the end of that chunk. This means chunk N's key encodes the
    full prefix context, not just the chunk's own tokens.

    Args:
        token_ids: Full list of prompt token IDs
        chunk_tokens: Number of tokens per chunk

    Returns:
        List of maru key strings, one per chunk
    """
    # TODO: O(n²) memory from repeated prefix slicing. For long prompts
    # (e.g. 10K tokens, ~39 chunks), consider incremental hashing
    # (e.g. blake2b update) to reduce to O(n). Profile before optimizing.
    keys = []
    t = torch.tensor(token_ids)
    num_full_chunks = len(token_ids) // chunk_tokens
    for i in range(num_full_chunks):
        end = (i + 1) * chunk_tokens
        prefix = t[:end]
        keys.append(f"kv_{_token_hash(prefix)}")
    return keys


def _create_maru_handler(
    extra_config: dict[str, Any],
    *,
    pool_size_override: int | None = None,
):
    """Create and connect a MaruHandler from extra_config.

    Args:
        extra_config: vLLM kv_connector_extra_config dict.
        pool_size_override: If set, use this pool size instead of the
            configured value. Used by the scheduler to avoid wasting
            CXL memory on a metadata-only connection.
    """
    from maru import MaruConfig, MaruHandler

    server_url = extra_config.get("maru_server_url", "tcp://localhost:5555")
    pool_size = (
        pool_size_override
        if pool_size_override is not None
        else _parse_size(extra_config.get("maru_pool_size", 1024**3))
    )
    chunk_size = _parse_size(extra_config.get("maru_chunk_size", 4 * 1024 * 1024))
    instance_id = extra_config.get("maru_instance_id")
    eager_map = extra_config.get("maru_eager_map", True)

    cfg = MaruConfig(
        server_url=server_url,
        pool_size=pool_size,
        chunk_size_bytes=chunk_size,
        instance_id=instance_id,
        auto_connect=False,
        eager_map=eager_map,
    )
    handler = MaruHandler(cfg)
    if not handler.connect():
        logger.error("Failed to connect to MaruServer at %s", server_url)
        return None
    logger.info("Connected to MaruServer at %s (pool=%d)", server_url, pool_size)
    return handler


# ============================================================================
# Metadata: passed from scheduler to worker each step
# ============================================================================


@dataclass
class MaruReqMeta:
    """Metadata for a single request's KV cache operation."""

    req_id: str
    token_ids: list[int]  # Full prompt token IDs
    block_ids: list[int]  # vLLM block IDs for this request
    is_store: bool  # True = save to maru, False = load from maru
    num_matched_chunks: int = 0  # For load: how many chunks to load
    num_scheduled_tokens: int = 0  # For store: tokens covered this step
    num_computed_tokens: int = 0  # For store: tokens already computed before this step


@dataclass
class MaruConnectorMetadata(KVConnectorMetadata):
    """Metadata communicated from scheduler to worker each step."""

    requests: list[MaruReqMeta] = field(default_factory=list)


# ============================================================================
# Main Connector
# ============================================================================


class MaruKVConnector(KVConnectorBase_V1):
    """vLLM KV Connector that uses Maru CXL shared memory for KV cache sharing.

    Supports same-node KV cache sharing between multiple vLLM instances
    through CXL shared memory. Data path is zero-copy on the CXL side;
    the only copies are GPU <-> CPU (unavoidable with current hardware).

    KV cache is stored in chunk granularity (default 256 tokens per chunk).
    Partial prefix reuse is supported: if the first N chunks of a prompt
    are cached, only the remaining tokens need to be computed.

    Configuration via kv_connector_extra_config:
        maru_server_url: str    - MaruServer address (default: tcp://localhost:5555)
        maru_pool_size: str|int - CXL pool size (default: 1G, supports '4G', '500M')
        maru_instance_id: str   - Unique instance ID (default: auto-generated)
        maru_chunk_size: str|int - Maru page size for CXL pages (default: 4M)
        maru_eager_map: bool    - Pre-map shared regions on connect (default: true)
        maru_kv_chunk_tokens: int - Tokens per KV chunk (default: 256)
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
        self._kv_chunk_tokens = int(
            extra.get("maru_kv_chunk_tokens", DEFAULT_KV_CHUNK_TOKENS)
        )

        # Ensure chunk_tokens is a multiple of block_size
        if self._kv_chunk_tokens % self._block_size != 0:
            old = self._kv_chunk_tokens
            self._kv_chunk_tokens = (
                self._kv_chunk_tokens // self._block_size
            ) * self._block_size
            if self._kv_chunk_tokens == 0:
                self._kv_chunk_tokens = self._block_size
            logger.warning(
                "maru_kv_chunk_tokens %d not aligned to block_size %d, adjusted to %d",
                old,
                self._block_size,
                self._kv_chunk_tokens,
            )

        if role == KVConnectorRole.SCHEDULER:
            self._scheduler = MaruSchedulerConnector(
                block_size=self._block_size,
                kv_chunk_tokens=self._kv_chunk_tokens,
                extra_config=extra,
            )
            self._worker = None
        elif role == KVConnectorRole.WORKER:
            self._scheduler = None
            self._worker = MaruWorkerConnector(
                block_size=self._block_size,
                kv_chunk_tokens=self._kv_chunk_tokens,
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
        return self._scheduler.get_num_new_matched_tokens(request, num_computed_tokens)

    def update_state_after_alloc(
        self,
        request: Request,
        blocks: KVCacheBlocks,
        num_external_tokens: int,
    ):
        assert self._scheduler is not None
        self._scheduler.update_state_after_alloc(request, blocks, num_external_tokens)

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

    def start_load_kv(self, forward_context: ForwardContext, **kwargs: Any) -> None:
        assert self._worker is not None
        metadata = self._get_connector_metadata()
        assert isinstance(metadata, MaruConnectorMetadata)
        self._worker.start_load_kv(forward_context, metadata)

    def wait_for_layer_load(self, layer_name: str) -> None:
        # Maru loads all layers at once in start_load_kv
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
        # CXL mmap write is synchronous
        pass

    def shutdown(self):
        if self._worker is not None:
            self._worker.shutdown()
        if self._scheduler is not None and self._scheduler._handler is not None:
            try:
                self._scheduler._handler.close()
            except Exception as e:
                logger.error("Error closing scheduler MaruHandler: %s", e)
            self._scheduler._handler = None


# ============================================================================
# Scheduler-side implementation
# ============================================================================


class MaruSchedulerConnector:
    """Scheduler-side: checks chunk-by-chunk which prefix is cached in Maru."""

    def __init__(
        self,
        block_size: int,
        kv_chunk_tokens: int,
        extra_config: dict[str, Any],
    ):
        self._block_size = block_size
        self._kv_chunk_tokens = kv_chunk_tokens
        self._extra_config = extra_config

        # Lazy-init MaruHandler for exists checks
        self._handler = None

        # Requests that need KV loaded from maru
        self._requests_need_load: dict[str, tuple[Request, int]] = {}
        # req_id -> (request, num_matched_chunks)

        # Cached match results from get_num_new_matched_tokens,
        # consumed by update_state_after_alloc to avoid redundant RPC.
        self._last_match_result: dict[str, int] = {}

        # Requests that need continued store across chunked prefill steps.
        # req_id -> token_ids (full prompt)
        self._requests_need_store: dict[str, list[int]] = {}

        # Local cache of keys known to exist (avoid repeated RPC).
        # TODO: add max size / eviction when long-running deployments
        # accumulate enough keys to matter.
        self._known_keys: set[str] = set()

        # Backoff timer to avoid reconnect storms when server is down.
        self._handler_retry_after: float = 0.0

    def _ensure_handler(self):
        if self._handler is not None:
            return
        if time.monotonic() < self._handler_retry_after:
            return
        # Scheduler only needs metadata lookups (batch_exists), not
        # data storage. Use the minimum pool that satisfies MaruConfig's
        # pool_size >= chunk_size_bytes constraint.
        # A metadata-only connect mode in MaruHandler would eliminate
        # this waste entirely.
        chunk_size = _parse_size(
            self._extra_config.get("maru_chunk_size", 4 * 1024 * 1024)
        )
        try:
            self._handler = _create_maru_handler(
                self._extra_config, pool_size_override=chunk_size
            )
        except Exception:
            self._handler_retry_after = time.monotonic() + 5.0
            logger.warning("Scheduler MaruHandler creation failed, backing off 5s")

    def _count_matched_chunks(self, token_ids: list[int]) -> int:
        """Count how many consecutive prefix chunks are cached in Maru.

        Uses batch_exists for efficiency: single RPC call checks all chunks.

        Returns:
            Number of consecutive cached chunks from the beginning.
        """
        keys = _chunk_keys(token_ids, self._kv_chunk_tokens)
        if not keys:
            return 0

        # Check local cache first - find longest prefix of known keys.
        # We use a "_DONE" marker (written after all layers are stored)
        # rather than checking a single layer, to avoid false positives
        # when a partial layer store failure leaves only some layers.
        local_hits = 0
        for key in keys:
            sentinel = f"{key}_DONE"
            if sentinel in self._known_keys:
                local_hits += 1
            else:
                break

        if local_hits == len(keys):
            return local_hits

        # Need to check remaining chunks via RPC
        self._ensure_handler()
        if self._handler is None:
            return local_hits

        # Check all unchecked chunks at once via batch_exists
        remaining_keys = [f"{k}_DONE" for k in keys[local_hits:]]
        try:
            results = self._handler.batch_exists(remaining_keys)
        except Exception as e:
            logger.warning("Maru batch_exists failed: %s", e)
            return local_hits

        # Count consecutive hits
        rpc_hits = 0
        for exists in results:
            if not exists:
                break
            rpc_hits += 1

        # Cache the newly discovered keys
        for i in range(rpc_hits):
            self._known_keys.add(remaining_keys[i])

        return local_hits + rpc_hits

    def get_num_new_matched_tokens(
        self,
        request: Request,
        num_computed_tokens: int,
    ) -> tuple[int | None, bool]:
        token_ids = list(request.prompt_token_ids or [])
        if len(token_ids) < self._kv_chunk_tokens:
            return 0, False

        num_matched_chunks = self._count_matched_chunks(token_ids)
        if num_matched_chunks == 0:
            return 0, False

        matched_tokens = num_matched_chunks * self._kv_chunk_tokens
        # Align to block size
        matched_tokens = _align_down(matched_tokens, self._block_size)
        new_matched = matched_tokens - num_computed_tokens

        if new_matched <= 0:
            return 0, False

        logger.info(
            "Maru KV hit: req=%s, %d chunks (%d tokens), new=%d beyond computed=%d",
            request.request_id,
            num_matched_chunks,
            matched_tokens,
            new_matched,
            num_computed_tokens,
        )

        # Cache the result so update_state_after_alloc can reuse it
        # without a redundant _count_matched_chunks call.
        self._last_match_result[request.request_id] = num_matched_chunks

        return new_matched, False

    def update_state_after_alloc(
        self,
        request: Request,
        blocks: KVCacheBlocks,
        num_external_tokens: int,
    ):
        if num_external_tokens > 0:
            num_chunks = self._last_match_result.pop(request.request_id, 0)
            self._requests_need_load[request.request_id] = (request, num_chunks)

    def build_connector_meta(
        self,
        scheduler_output: SchedulerOutput,
    ) -> KVConnectorMetadata:
        meta = MaruConnectorMetadata()

        for new_req in scheduler_output.scheduled_new_reqs:
            token_ids = list(new_req.prompt_token_ids or [])

            if new_req.req_id in self._requests_need_load:
                # Load cached chunks from maru
                _, num_chunks = self._requests_need_load[new_req.req_id]
                meta.requests.append(
                    MaruReqMeta(
                        req_id=new_req.req_id,
                        token_ids=token_ids,
                        # Currently only supports a single KV cache group
                        # (index 0). Multi-group configs (e.g. MLA) will
                        # silently ignore additional groups.
                        block_ids=new_req.block_ids[0],
                        is_store=False,
                        num_matched_chunks=num_chunks,
                    )
                )
            else:
                # Store new prompt chunks to maru
                num_scheduled = scheduler_output.num_scheduled_tokens.get(
                    new_req.req_id, len(token_ids)
                )
                meta.requests.append(
                    MaruReqMeta(
                        req_id=new_req.req_id,
                        token_ids=token_ids,
                        # Single KV cache group (see note above)
                        block_ids=new_req.block_ids[0],
                        is_store=True,
                        num_scheduled_tokens=num_scheduled,
                    )
                )
                # If chunked prefill means not all chunks are covered,
                # track for store continuation in subsequent steps.
                num_full_chunks = len(token_ids) // self._kv_chunk_tokens
                stored_chunks = num_scheduled // self._kv_chunk_tokens
                if stored_chunks < num_full_chunks:
                    self._requests_need_store[new_req.req_id] = token_ids

        # Handle resumed requests (chunked prefill continuations).
        cached_reqs = scheduler_output.scheduled_cached_reqs
        for i, req_id in enumerate(cached_reqs.req_ids):
            resumed = req_id in cached_reqs.resumed_req_ids
            if not resumed:
                continue

            num_new = scheduler_output.num_scheduled_tokens.get(req_id, 0)
            new_block_ids = cached_reqs.new_block_ids[i]
            if new_block_ids is None:
                continue

            if req_id in self._requests_need_load:
                # Still needs load from maru
                request, num_chunks = self._requests_need_load[req_id]
                num_computed = cached_reqs.num_computed_tokens[i]
                total_tokens = num_computed + num_new
                token_ids = list(request.all_token_ids[:total_tokens])

                meta.requests.append(
                    MaruReqMeta(
                        req_id=req_id,
                        token_ids=token_ids,
                        block_ids=new_block_ids[0],
                        is_store=False,
                        num_matched_chunks=num_chunks,
                    )
                )
            elif req_id in self._requests_need_store:
                # Chunked prefill continuation — store newly computed chunks
                token_ids = self._requests_need_store[req_id]
                num_computed = cached_reqs.num_computed_tokens[i]
                meta.requests.append(
                    MaruReqMeta(
                        req_id=req_id,
                        token_ids=token_ids,
                        block_ids=new_block_ids[0],
                        is_store=True,
                        num_scheduled_tokens=num_new,
                        num_computed_tokens=num_computed,
                    )
                )
                # Check if all chunks are now covered
                num_computed = cached_reqs.num_computed_tokens[i]
                total_scheduled = num_computed + num_new
                num_full_chunks = len(token_ids) // self._kv_chunk_tokens
                if total_scheduled // self._kv_chunk_tokens >= num_full_chunks:
                    del self._requests_need_store[req_id]

        # Clean up state for finished/preempted requests to prevent
        # unbounded memory growth during long-running deployments.
        stale_ids = scheduler_output.finished_req_ids
        if scheduler_output.preempted_req_ids:
            stale_ids = stale_ids | scheduler_output.preempted_req_ids
        for rid in stale_ids:
            self._requests_need_store.pop(rid, None)
            self._requests_need_load.pop(rid, None)

        self._requests_need_load.clear()
        return meta


# ============================================================================
# Worker-side implementation
# ============================================================================


class MaruWorkerConnector:
    """Worker-side: performs GPU <-> CXL data transfers in chunk granularity."""

    def __init__(
        self,
        block_size: int,
        kv_chunk_tokens: int,
        extra_config: dict[str, Any],
    ):
        self._block_size = block_size
        self._kv_chunk_tokens = kv_chunk_tokens
        self._extra_config = extra_config
        self._handler = None
        self._kv_caches: dict[str, torch.Tensor] = {}
        # TODO: _stored_keys grows unbounded and may become stale if CXL
        # eviction removes the actual data. Consider TTL-based expiry or
        # periodic batch_exists validation. Also risks memory growth in
        # long-running deployments (num_chunks × num_layers keys).
        self._stored_keys: set[str] = set()
        # Track per-chunk layer completion for writing _DONE markers.
        # chunk_base_key -> set of stored layer indices
        self._chunk_layer_progress: dict[str, set[int]] = {}
        self._num_layers: int = 0

        # Backoff timer to avoid reconnect storms when server is down.
        self._handler_retry_after: float = 0.0

    def _ensure_handler(self):
        if self._handler is not None:
            return
        if time.monotonic() < self._handler_retry_after:
            return
        try:
            self._handler = _create_maru_handler(self._extra_config)
        except Exception:
            self._handler_retry_after = time.monotonic() + 5.0
            logger.warning("Worker MaruHandler creation failed, backing off 5s")

    def register_kv_caches(self, kv_caches: dict[str, torch.Tensor]):
        self._kv_caches = kv_caches
        self._num_layers = len(kv_caches)
        logger.info(
            "MaruWorkerConnector: registered %d KV cache layers", len(kv_caches)
        )

    def _build_slot_mapping(
        self, block_ids: list[int], num_tokens: int
    ) -> torch.Tensor:
        """Build slot mapping from block IDs and token count."""
        block_ids_t = torch.tensor(block_ids)
        num_blocks = block_ids_t.shape[0]
        offsets = torch.arange(0, self._block_size)
        slot_mapping = (
            offsets.reshape((1, self._block_size))
            + block_ids_t.reshape((num_blocks, 1)) * self._block_size
        )
        return slot_mapping.flatten()[:num_tokens]

    def start_load_kv(
        self,
        forward_context: ForwardContext,
        metadata: MaruConnectorMetadata,
    ) -> None:
        """Load KV caches from Maru CXL into GPU paged buffers, chunk by chunk."""
        self._ensure_handler()
        if self._handler is None:
            return

        attn_metadata = forward_context.attn_metadata
        if attn_metadata is None:
            return

        for req_meta in metadata.requests:
            if req_meta.is_store or req_meta.num_matched_chunks == 0:
                continue

            chunk_keys = _chunk_keys(req_meta.token_ids, self._kv_chunk_tokens)
            num_chunks = min(req_meta.num_matched_chunks, len(chunk_keys))
            total_tokens = num_chunks * self._kv_chunk_tokens

            # Build slot mapping for the tokens we're loading
            slot_mapping = self._build_slot_mapping(req_meta.block_ids, total_tokens)

            num_layers_loaded = 0
            for layer_name in forward_context.no_compile_layers:
                layer = forward_context.no_compile_layers[layer_name]
                kv_cache_attr = getattr(layer, "kv_cache", None)
                if kv_cache_attr is None:
                    continue

                kv_cache_layer = kv_cache_attr[forward_context.virtual_engine]
                # Use the same layer index derivation as save path
                # to ensure key consistency (enumerate index may diverge
                # when no_compile_layers contains non-attention layers).
                actual_layer_idx = self._get_layer_index(layer_name)

                # Load and inject each chunk independently to preserve
                # K/V layout (concatenating 1D bytes breaks interleaving)
                success = True
                for ci in range(num_chunks):
                    maru_key = f"{chunk_keys[ci]}_L{actual_layer_idx}"
                    try:
                        info = self._handler.retrieve(maru_key)
                        if info is None:
                            logger.warning(
                                "Maru load miss: %s (layer %s, chunk %d)",
                                maru_key,
                                layer_name,
                                ci,
                            )
                            success = False
                            break
                        # CXL mmap is already cudaHostRegistered by
                        # DaxMapper — no clone() needed, .cuda() uses
                        # DMA directly from pinned CXL memory
                        chunk_tensor = torch.frombuffer(
                            info.view, dtype=kv_cache_layer.dtype
                        )

                        # Inject this chunk directly into the KV cache
                        chunk_start = ci * self._kv_chunk_tokens
                        chunk_end = chunk_start + self._kv_chunk_tokens
                        chunk_slots = slot_mapping[chunk_start:chunk_end]
                        self._inject_kv_into_layer(
                            kv_cache_layer,
                            chunk_tensor.to(kv_cache_layer.device),
                            chunk_slots.to(kv_cache_layer.device),
                            attn_metadata,
                            layer_name,
                        )
                    except Exception as e:
                        logger.error("Maru load error: %s: %s", maru_key, e)
                        success = False
                        break

                if not success:
                    # Abort loading remaining layers for this request
                    # to avoid corrupted KV cache (missing layer data).
                    logger.warning(
                        "Maru: aborting load for req %s — layer %s chunk "
                        "miss (loaded %d layers before failure)",
                        req_meta.req_id,
                        layer_name,
                        num_layers_loaded,
                    )
                    break
                num_layers_loaded += 1

            if num_layers_loaded > 0:
                logger.info(
                    "Maru: loaded %d layers x %d chunks (%d tokens) for req %s",
                    num_layers_loaded,
                    num_chunks,
                    total_tokens,
                    req_meta.req_id,
                )

    def save_kv_layer(
        self,
        layer_name: str,
        kv_layer: torch.Tensor,
        attn_metadata: AttentionMetadata,
        metadata: MaruConnectorMetadata,
    ) -> None:
        """Save KV cache to Maru CXL in chunk granularity."""
        self._ensure_handler()
        if self._handler is None:
            return

        layer_idx = self._get_layer_index(layer_name)

        for req_meta in metadata.requests:
            if not req_meta.is_store:
                continue

            chunk_keys = _chunk_keys(req_meta.token_ids, self._kv_chunk_tokens)
            if not chunk_keys:
                continue

            # In chunked prefill, only store chunks fully covered by
            # this step's scheduled tokens / block_ids. Use
            # num_computed_tokens as offset so step 2+ doesn't
            # re-store chunks from earlier steps.
            if req_meta.num_scheduled_tokens > 0:
                offset = req_meta.num_computed_tokens // self._kv_chunk_tokens
                max_chunks = req_meta.num_scheduled_tokens // self._kv_chunk_tokens
                chunk_keys = chunk_keys[offset : offset + max_chunks]
            if not chunk_keys:
                continue

            total_tokens = len(chunk_keys) * self._kv_chunk_tokens
            slot_mapping = self._build_slot_mapping(req_meta.block_ids, total_tokens)

            for ci, base_key in enumerate(chunk_keys):
                maru_key = f"{base_key}_L{layer_idx}"
                if maru_key in self._stored_keys:
                    continue

                # Extract this chunk's slots
                chunk_start = ci * self._kv_chunk_tokens
                chunk_end = chunk_start + self._kv_chunk_tokens
                chunk_slots = slot_mapping[chunk_start:chunk_end]

                handle = None
                try:
                    chunk_slots_gpu = chunk_slots.to(kv_layer.device)
                    kv_data = self._extract_kv_from_layer(
                        kv_layer,
                        chunk_slots_gpu,
                        attn_metadata,
                        layer_name,
                    )
                    kv_contig = kv_data.detach().contiguous()
                    nbytes = kv_contig.nelement() * kv_contig.element_size()

                    # Zero-copy path: alloc CXL page, GPU→CXL direct copy
                    handle = self._handler.alloc(nbytes)
                    dst = torch.frombuffer(
                        handle.buf[:nbytes], dtype=kv_contig.dtype
                    ).reshape(kv_contig.shape)
                    dst.copy_(kv_contig)  # GPU→CXL mmap (single cudaMemcpy)

                    success = self._handler.store(maru_key, handle=handle)
                    if success:
                        self._stored_keys.add(maru_key)
                        handle = None  # ownership transferred to store
                        # Track layer completion; write _DONE marker
                        # once all layers for this chunk are stored.
                        progress = self._chunk_layer_progress.setdefault(
                            base_key, set()
                        )
                        progress.add(layer_idx)
                        if self._num_layers > 0 and len(progress) >= self._num_layers:
                            # TODO: alloc(1) still allocates a full CXL page
                            # (chunk_size_bytes, default 4MB). A metadata-only
                            # store in MaruHandler would eliminate this waste.
                            done_key = f"{base_key}_DONE"
                            done_h = None
                            try:
                                done_h = self._handler.alloc(1)
                                self._handler.store(done_key, handle=done_h)
                                done_h = None  # ownership transferred
                            except Exception as e:
                                logger.warning(
                                    "Failed to store _DONE marker for %s: %s",
                                    base_key,
                                    e,
                                )
                            finally:
                                if done_h is not None:
                                    try:
                                        self._handler.free(done_h)
                                    except Exception:
                                        pass
                            del self._chunk_layer_progress[base_key]
                    else:
                        logger.warning("Maru store failed: %s", maru_key)
                except Exception as e:
                    logger.error("Maru save error: %s: %s", maru_key, e)
                finally:
                    if handle is not None:
                        try:
                            self._handler.free(handle)
                        except Exception:
                            pass

    def shutdown(self):
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
        """Extract numeric layer index from layer name."""
        match = re.search(r"layers\.(\d+)", layer_name)
        if match:
            return int(match.group(1))
        for idx, name in enumerate(self._kv_caches):
            if name == layer_name:
                return idx
        logger.warning(
            "Could not determine layer index for %s, defaulting to 0. "
            "This may cause key collisions across layers.",
            layer_name,
        )
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

        layer_meta = (
            attn_metadata[layer_name]
            if isinstance(attn_metadata, dict)
            else attn_metadata
        )

        if isinstance(layer_meta, MLACommonMetadata):
            num_pages = dst_kv_cache.shape[0]
            page_size = dst_kv_cache.shape[1]
            flat = dst_kv_cache.reshape(num_pages * page_size, -1)
            src = src_kv_data.reshape(slot_mapping.shape[0], -1)
            flat[slot_mapping] = src
        elif isinstance(layer_meta, TritonAttentionMetadata):
            block_idxs = slot_mapping // self._block_size
            offsets = slot_mapping % self._block_size
            src = src_kv_data.reshape(slot_mapping.shape[0], dst_kv_cache.shape[1], -1)
            dst_kv_cache[block_idxs, :, offsets] = src
        else:
            # Flash attention: [2, num_pages, page_size, ...]
            num_pages = dst_kv_cache.shape[1]
            page_size = dst_kv_cache.shape[2]
            flat = dst_kv_cache.reshape(2, num_pages * page_size, -1)
            src = src_kv_data.reshape(2, slot_mapping.shape[0], -1)
            flat[:, slot_mapping] = src

    def _extract_kv_from_layer(
        self,
        kv_layer: torch.Tensor,
        slot_mapping: torch.Tensor,
        attn_metadata: AttentionMetadata,
        layer_name: str = "",
    ) -> torch.Tensor:
        """Extract KV cache data from GPU paged buffer for given slots."""
        from vllm.model_executor.layers.attention.mla_attention import (
            MLACommonMetadata,
        )
        from vllm.v1.attention.backends.triton_attn import (
            TritonAttentionMetadata,
        )

        # Handle per-layer dict metadata (same as _inject_kv_into_layer)
        layer_meta = (
            attn_metadata[layer_name]
            if isinstance(attn_metadata, dict)
            else attn_metadata
        )

        if isinstance(layer_meta, MLACommonMetadata):
            num_pages, page_size = kv_layer.shape[0], kv_layer.shape[1]
            flat = kv_layer.reshape(num_pages * page_size, -1)
            return flat[slot_mapping]
        elif isinstance(layer_meta, TritonAttentionMetadata):
            block_idxs = slot_mapping // self._block_size
            offsets = slot_mapping % self._block_size
            return kv_layer[block_idxs, :, offsets]
        else:
            num_pages, page_size = kv_layer.shape[1], kv_layer.shape[2]
            flat = kv_layer.reshape(2, num_pages * page_size, -1)
            return flat[:, slot_mapping]
