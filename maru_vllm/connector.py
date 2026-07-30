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

import hashlib
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
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

_cuda_runtime: Any = None
_cuda_memcpy2d_async: Any = None
_cuda_memcpy2d_unavailable = False


# ============================================================================
# Utilities
# ============================================================================


def _emit_timing(msg: str) -> None:
    """Write a diagnostic timing line to stderr.

    The connector's vLLM logger (``maru_vllm.connector``) sits outside the
    ``vllm.*`` handler namespace, so its records are never captured in the
    engine logs. Timing diagnostics therefore go straight to stderr, which
    the process log does capture.
    """
    import sys

    print(f"Maru timing: {msg}", file=sys.stderr, flush=True)


def _get_cuda_memcpy2d_async() -> Any:
    """Resolve ``cudaMemcpy2DAsync`` lazily for pitched packed-slab DMA.

    PyTorch makes a non-contiguous host tensor contiguous before H2D, which
    would synchronously copy the whole CXL payload through host DRAM. The CUDA
    runtime's pitched copy can gather one layer plane from consecutive packed
    pages directly on the copy engine instead. Resolution is lazy so CPU-only
    imports do not require a CUDA runtime library.
    """
    global _cuda_runtime, _cuda_memcpy2d_async, _cuda_memcpy2d_unavailable
    if _cuda_memcpy2d_async is not None:
        return _cuda_memcpy2d_async
    if _cuda_memcpy2d_unavailable:
        return None
    try:
        import ctypes
        import ctypes.util

        library = ctypes.util.find_library("cudart")
        if library is None:
            raise OSError("libcudart not found")
        _cuda_runtime = ctypes.CDLL(library)
        function = _cuda_runtime.cudaMemcpy2DAsync
        function.argtypes = [
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_size_t,
            ctypes.c_size_t,
            ctypes.c_int,
            ctypes.c_void_p,
        ]
        function.restype = ctypes.c_int
        _cuda_memcpy2d_async = function
        return function
    except (AttributeError, OSError) as error:
        _cuda_memcpy2d_unavailable = True
        logger.warning("cudaMemcpy2DAsync unavailable: %s", error)
        return None


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


def _chunk_keys(token_ids: list[int], chunk_tokens: int) -> list[str]:
    """Generate maru keys for each chunk of the token prefix.

    Each chunk's key = hash of all tokens from the beginning up to the end of
    that chunk, so chunk N's key encodes the full prefix context (not just its
    own tokens) — prefixes that diverge produce different keys from the
    divergence point on.

    Uses one **incremental** blake2b: each chunk feeds only its own tokens and
    the running digest reflects the whole prefix so far. This is O(n) total,
    replacing the previous O(n²) (re-hash the growing prefix per chunk), which
    measured ~5 ms on a 15k-token prompt on the scheduler's TTFT critical path
    (design note: if1 gap decomposition). The digest is deterministic
    (hashlib, PYTHONHASHSEED-independent) and self-consistent across the store
    and load paths; it is opaque to Maru so the algorithm choice is free.

    Args:
        token_ids: Full list of prompt token IDs.
        chunk_tokens: Number of tokens per chunk.

    Returns:
        List of maru key strings, one per full chunk.
    """
    num_full_chunks = len(token_ids) // chunk_tokens
    if num_full_chunks == 0:
        return []
    arr = torch.tensor(token_ids[: num_full_chunks * chunk_tokens]).numpy()
    hasher = hashlib.blake2b(digest_size=8)  # 8 bytes → 16 hex chars
    keys = []
    for i in range(num_full_chunks):
        hasher.update(arr[i * chunk_tokens : (i + 1) * chunk_tokens].tobytes())
        keys.append(f"kv_{hasher.hexdigest()}")
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
    # vLLM block IDs covering the request from token 0 in token order.
    # For chunked-prefill store continuations the scheduler accumulates
    # each step's new blocks so slot lookup by absolute token offset works.
    block_ids: list[int]
    is_store: bool  # True = save to maru, False = load from maru
    num_matched_chunks: int = 0  # For load: how many chunks to load
    num_scheduled_tokens: int = 0  # For store: tokens covered this step
    num_computed_tokens: int = 0  # For store: tokens already computed before this step
    # For load: the request is parked in WAITING_FOR_REMOTE_KVS and the worker
    # loads between scheduler steps, reporting completion via get_finished().
    deferred_load: bool = False
    # Adaptive packed-layerwise mode: the loader thread stops after retrieve
    # and the resumed forward pipelines this request by layer. Scheduler sets
    # this only for a singleton deferred-admission batch; at higher admission
    # concurrency the completed whole-request background DMA remains faster.
    layerwise_load: bool = False
    # Per-step memo of _chunk_keys(token_ids). The same MaruReqMeta object is
    # shared across a step's per-layer save_kv_layer calls (and the step's
    # start_load_kv), so computing chunk keys once here avoids recomputing the
    # O(n) hash num_layers times per store. Lives only as long as the step's
    # metadata object. compare=False so it never affects equality/repr.
    _chunk_keys_memo: list[str] | None = field(default=None, compare=False, repr=False)


def _req_chunk_keys(req_meta: MaruReqMeta, chunk_tokens: int) -> list[str]:
    """Chunk keys for a request, memoized on the (per-step) req_meta object.

    Mirrors LMCache's per-session hash cache: the O(n) key hash is computed
    once per request per step instead of once per layer on the store path.
    """
    if req_meta._chunk_keys_memo is None:
        req_meta._chunk_keys_memo = _chunk_keys(req_meta.token_ids, chunk_tokens)
    return req_meta._chunk_keys_memo


@dataclass
class MaruConnectorMetadata(KVConnectorMetadata):
    """Metadata communicated from scheduler to worker each step."""

    requests: list[MaruReqMeta] = field(default_factory=list)
    # A write-behind store may still be reading blocks that the scheduler
    # preempted after the previous step. The worker drains its store stream
    # before the next forward can reuse those block IDs.
    preempted_req_ids: set[str] = field(default_factory=set)
    # Deferred packed loads whose metadata RPC completed between steps. The
    # resumed forward consumes their CXL views layer-by-layer, so layer k+1 H2D
    # can overlap layer k compute without changing the packed storage format.
    layerwise_load_req_ids: set[str] = field(default_factory=set)


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
        maru_enable_async_loading: bool - Overlap CXL->GPU load with compute
            through a dedicated CUDA stream (default: false)
        maru_enable_deferred_loading: bool - Load matched KV between scheduler
            steps: the request is parked in WAITING_FOR_REMOTE_KVS while a
            background loader thread performs the whole load (Maru retrieve
            RPC + CXL->GPU transfer on a dedicated stream), so neither the
            RPC wait nor the copy ever blocks the engine's forward passes —
            the in-process analog of the MP server's separate-process
            retrieve (default: false)
        maru_load_admission_window: int - With deferred loading, cap how many
            requests' packed loads may be enqueued on the deferred stream but
            not yet complete. The loader thread blocks before each GPU
            enqueue until fewer than this many loads are outstanding,
            bounding same-CUDA-context interference with model steps.
            Default: 0 (submit all loads immediately). Set a positive value
            to enable the cap as a fallback safety guard.
        maru_enable_write_behind: bool - Gather completed prompt chunks into a
            reusable GPU staging slab after the forward, then copy them to CXL
            with asynchronous D2H DMA. Metadata registration finishes on a
            background thread; finished requests retain their GPU blocks until
            get_finished() reports the store complete (default: false)
        maru_enable_layerwise_overlap: bool - With deferred loading and packed
            storage, park only through the Maru retrieve RPC. The resumed
            forward transfers each layer on a dedicated CUDA stream and waits
            on per-layer events, overlapping layer k+1 load with layer k
            compute. Requires maru_enable_deferred_loading=true and
            maru_use_layerwise=false (default: false)
        maru_enable_fused_load: bool - Replace the cudaMemcpy H2D + inject
            two-stage load with one fused gather-scatter kernel that reads
            the CXL pages directly (LMCache single_layer_kv_transfer).
            Requires maru_enable_async_loading and the Flash KV layout;
            falls back to memcpy otherwise (default: false)
        maru_use_layerwise: bool - Store one CXL object per (chunk, layer)
            (True) or one packed object per chunk holding all layers (False,
            default — mirrors LMCache use_layerwise=False). Packed cuts
            retrieve metadata 32x (1,888->59 keys/req) and drops the _DONE
            marker. Both packed transfers use LMCache's
            multi_layer_kv_transfer kernel directly on the pinned CXL slab
            (no staging) when available — load scatters a whole slab into the
            paged cache per chunk, store gathers one D2H transfer per chunk —
            falling back to per-layer copies otherwise. See design note P6.
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

        self._scheduler: MaruSchedulerConnector | None
        self._worker: MaruWorkerConnector | None
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
        assert self._worker is not None
        self._worker.wait_for_layer_load(layer_name)

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
        # Synchronous stores complete inside save_kv_layer. Write-behind stores
        # intentionally outlive this forward; request/block lifetime is
        # tracked through request_finished() + get_finished().
        return

    def handle_preemptions(self, kv_connector_metadata: KVConnectorMetadata) -> None:
        """Protect blocks preempted while a write-behind D2H is in flight."""
        assert self._worker is not None
        assert isinstance(kv_connector_metadata, MaruConnectorMetadata)
        self._worker.handle_preemptions(kv_connector_metadata)

    def get_finished(
        self, finished_req_ids: set[str]
    ) -> tuple[set[str] | None, set[str] | None]:
        """Report requests whose deferred (between-step) KV loads completed.

        Args:
            finished_req_ids: requests that finished generating. In
                write-behind mode their blocks remain owned by the connector
                until every queued store that reads them is complete.

        Returns:
            ``(finished_sending, finished_recving)`` per the connector
            contract.
        """
        assert self._worker is not None
        return (
            self._worker.get_finished_saving(finished_req_ids),
            self._worker.get_finished_loading(),
        )

    def request_finished(
        self,
        request: Request,
        block_ids: list[int],
    ) -> tuple[bool, dict[str, Any] | None]:
        """Keep finished-request blocks alive while write-behind reads them."""
        assert self._scheduler is not None
        return self._scheduler.request_finished(request, block_ids)

    def get_block_ids_with_load_errors(self) -> set[int]:
        """Return block ids whose deferred load failed (vLLM recomputes them)."""
        assert self._worker is not None
        return self._worker.take_failed_load_blocks()

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

        # Deferred (between-step) loading: matched KV is transferred while the
        # request waits in WAITING_FOR_REMOTE_KVS instead of stalling its
        # first forward pass. Mirrors LMCache's enable_async_loading.
        self._deferred_loading = bool(
            extra_config.get("maru_enable_deferred_loading", False)
        )
        self._write_behind = bool(extra_config.get("maru_enable_write_behind", False))
        self._use_layerwise = bool(extra_config.get("maru_use_layerwise", False))
        self._layerwise_overlap = bool(
            extra_config.get("maru_enable_layerwise_overlap", False)
            and self._deferred_loading
            and not self._use_layerwise
        )
        if extra_config.get("maru_enable_layerwise_overlap", False) and not (
            self._deferred_loading and not self._use_layerwise
        ):
            logger.warning(
                "Maru packed-layerwise overlap requires deferred loading and "
                "maru_use_layerwise=false; disabling it"
            )
        # Deferred loads registered by update_state_after_alloc, emitted once
        # by the next build_connector_meta. req_id -> (request,
        # num_matched_chunks, block_ids).
        self._pending_deferred_loads: dict[str, tuple[Request, int, list[int]]] = {}
        # Deferred-hit requests that are still live in the scheduler. Pending
        # admissions alone are not a concurrency signal: a burst may arrive
        # one request per scheduler step while earlier requests are decoding.
        # Keep the live set until finish/preemption so packed-layerwise is used
        # only when the serving workload is actually singleton.
        self._active_deferred_req_ids: set[str] = set()
        # Requests emitted for deferred packed loading remain here until vLLM's
        # second update_state_after_alloc call says they are ready to resume.
        # The next connector metadata then activates the worker's retained CXL
        # views for per-layer transfer.
        self._deferred_layerwise_waiting: set[str] = set()
        self._deferred_layerwise_ready: set[str] = set()

        # Cached match results from get_num_new_matched_tokens,
        # consumed by update_state_after_alloc to avoid redundant RPC.
        self._last_match_result: dict[str, int] = {}

        # Requests that need continued store across chunked prefill steps.
        # req_id -> (full prompt token_ids, block ids accumulated from the
        # first step in token order). The full block list lets the worker
        # map any absolute token offset to its GPU slot — per-step new
        # blocks alone cannot, because step boundaries are not chunk-aligned.
        self._requests_need_store: dict[str, tuple[list[int], list[int]]] = {}

        # Local cache of keys known to exist (avoid repeated RPC).
        # TODO: add max size / eviction when long-running deployments
        # accumulate enough keys to matter.
        self._known_keys: set[str] = set()

        # Backoff timer to avoid reconnect storms when server is down.
        self._handler_retry_after: float = 0.0

        # Opt-in per-request phase timing (diagnostics only).
        self._timing = bool(extra_config.get("maru_log_timing", False))

    def _chunk_exists_key(self, base_key: str) -> str:
        """Key whose presence means a chunk is fully stored across layers.

        Packed (default): the chunk object itself is registered only after
        every layer is written, so its own key is the completion marker.
        Layerwise: a separate ``_DONE`` marker is written after the last
        per-layer object.
        """
        return base_key if not self._use_layerwise else f"{base_key}_DONE"

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
            sentinel = self._chunk_exists_key(key)
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
        remaining_keys = [self._chunk_exists_key(k) for k in keys[local_hits:]]
        try:
            _t0 = time.monotonic()
            results = self._handler.batch_exists(remaining_keys)
            if self._timing:
                _emit_timing(
                    f"lookup batch_exists {len(remaining_keys)} keys = "
                    f"{(time.monotonic() - _t0) * 1000:.2f} ms"
                )
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

        _t0 = time.monotonic()
        num_matched_chunks = self._count_matched_chunks(token_ids)
        if self._timing:
            _emit_timing(
                f"get_num_new_matched (incl _chunk_keys) {len(token_ids)} tok = "
                f"{(time.monotonic() - _t0) * 1000:.2f} ms"
            )
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

        return new_matched, self._deferred_loading

    def update_state_after_alloc(
        self,
        request: Request,
        blocks: KVCacheBlocks,
        num_external_tokens: int,
    ):
        if num_external_tokens <= 0:
            # Second call after a deferred load completed (extra blocks for
            # the tail). Packed layerwise overlap uses it as the scheduler-side
            # handoff: the next scheduled forward must activate the CXL views
            # retained by the worker.
            if (
                self._layerwise_overlap
                and request.request_id in self._deferred_layerwise_waiting
            ):
                self._deferred_layerwise_ready.add(request.request_id)
            return
        num_chunks = self._last_match_result.pop(request.request_id, 0)
        if self._deferred_loading:
            self._active_deferred_req_ids.add(request.request_id)
            self._pending_deferred_loads[request.request_id] = (
                request,
                num_chunks,
                list(blocks.get_block_ids()[0]),
            )
        else:
            self._requests_need_load[request.request_id] = (request, num_chunks)

    def build_connector_meta(
        self,
        scheduler_output: SchedulerOutput,
    ) -> KVConnectorMetadata:
        meta = MaruConnectorMetadata(
            preempted_req_ids=set(scheduler_output.preempted_req_ids or ())
        )

        # Deferred loads first: these requests are parked in
        # WAITING_FOR_REMOTE_KVS (not scheduled), so their load metadata is
        # emitted exactly once here, from state stashed at allocation time.
        layerwise_singleton = (
            self._layerwise_overlap
            and len(self._pending_deferred_loads) == 1
            and len(self._active_deferred_req_ids) == 1
        )
        for req_id, (
            request,
            num_chunks,
            block_ids,
        ) in self._pending_deferred_loads.items():
            if layerwise_singleton:
                self._deferred_layerwise_waiting.add(req_id)
            meta.requests.append(
                MaruReqMeta(
                    req_id=req_id,
                    token_ids=list(request.prompt_token_ids or []),
                    block_ids=block_ids,
                    is_store=False,
                    num_matched_chunks=num_chunks,
                    deferred_load=True,
                    layerwise_load=layerwise_singleton,
                )
            )
        self._pending_deferred_loads.clear()

        for new_req in scheduler_output.scheduled_new_reqs:
            token_ids = list(new_req.prompt_token_ids or [])

            if new_req.req_id in self._deferred_layerwise_ready:
                meta.layerwise_load_req_ids.add(new_req.req_id)
                self._deferred_layerwise_ready.discard(new_req.req_id)
                self._deferred_layerwise_waiting.discard(new_req.req_id)

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
                # Store new prompt chunks to maru. num_computed_tokens is
                # non-zero when a prefix was already covered externally (a
                # request resuming after a deferred load) — only the chunks
                # completed beyond it are stored.
                num_scheduled = scheduler_output.num_scheduled_tokens.get(
                    new_req.req_id, len(token_ids)
                )
                num_computed = getattr(new_req, "num_computed_tokens", 0) or 0
                meta.requests.append(
                    MaruReqMeta(
                        req_id=new_req.req_id,
                        token_ids=token_ids,
                        # Single KV cache group (see note above)
                        block_ids=new_req.block_ids[0],
                        is_store=True,
                        num_scheduled_tokens=num_scheduled,
                        num_computed_tokens=num_computed,
                    )
                )
                # If chunked prefill means not all chunks are covered,
                # track for store continuation in subsequent steps.
                num_full_chunks = len(token_ids) // self._kv_chunk_tokens
                stored_chunks = (num_computed + num_scheduled) // self._kv_chunk_tokens
                if stored_chunks < num_full_chunks:
                    self._requests_need_store[new_req.req_id] = (
                        token_ids,
                        list(new_req.block_ids[0]),
                    )

        # Cached requests: chunked-prefill continuations and resumed loads.
        # Do NOT gate the store on ``resumed`` — chunked-prefill requests
        # reappear here every step un-resumed, so gating stored only the first
        # chunk. See design note 20260624_maru-vllm-direct-chunked-prefill-store-bug.
        cached_reqs = scheduler_output.scheduled_cached_reqs
        for i, req_id in enumerate(cached_reqs.req_ids):
            num_new = scheduler_output.num_scheduled_tokens.get(req_id, 0)
            new_block_ids = cached_reqs.new_block_ids[i]
            num_computed = cached_reqs.num_computed_tokens[i]
            resumed = req_id in cached_reqs.resumed_req_ids

            if req_id in self._deferred_layerwise_ready:
                meta.layerwise_load_req_ids.add(req_id)
                self._deferred_layerwise_ready.discard(req_id)
                self._deferred_layerwise_waiting.discard(req_id)

            if resumed and req_id in self._requests_need_load:
                if new_block_ids is None:
                    continue
                # Resumed from preemption and still needs its KV loaded.
                request, num_chunks = self._requests_need_load[req_id]
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
                # Chunked-prefill continuation (normal progression OR
                # resumed): store the chunks completed by this step. Extend
                # the accumulated block list first so the worker always sees
                # blocks covering the request from token 0 — this is what
                # lets it store a chunk straddling the previous step boundary
                # from the correct slots. A step that allocated no new blocks
                # still emits store metadata over the blocks accumulated so
                # far.
                token_ids, block_ids = self._requests_need_store[req_id]
                if new_block_ids is not None:
                    block_ids = block_ids + list(new_block_ids[0])
                    self._requests_need_store[req_id] = (token_ids, block_ids)
                meta.requests.append(
                    MaruReqMeta(
                        req_id=req_id,
                        token_ids=token_ids,
                        block_ids=block_ids,
                        is_store=True,
                        num_scheduled_tokens=num_new,
                        num_computed_tokens=num_computed,
                    )
                )
                # Drop tracking once all full chunks have been stored.
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
            self._pending_deferred_loads.pop(rid, None)
            self._active_deferred_req_ids.discard(rid)
            self._deferred_layerwise_waiting.discard(rid)
            self._deferred_layerwise_ready.discard(rid)

        self._requests_need_load.clear()
        return meta

    def request_finished(
        self,
        request: Request,
        block_ids: list[int],
    ) -> tuple[bool, dict[str, Any] | None]:
        """Transfer block ownership to the worker for write-behind stores."""
        return self._write_behind, None


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
        self._async_loading = bool(extra_config.get("maru_enable_async_loading", False))
        self._load_stream: torch.cuda.Stream | None = None
        self._load_stream_device: torch.device | None = None
        self._layer_load_events: dict[str, torch.cuda.Event] = {}
        self._effective_page_size_bytes: int | None = None
        # Keep mmap-backed MemoryInfo and pinned/device slot mappings alive
        # while their queued H2D copies may still read them: one (event, refs)
        # entry per scheduled batch, with the event recorded after the batch's
        # last copy. Entries are released only once the event has completed —
        # under vLLM async scheduling the next step's start_load_kv can run
        # while the previous step's copies are still queued on _load_stream,
        # so a time-based clear would free memory those copies still read
        # (the CXL mmap views are outside every CUDA allocator's tracking).
        self._active_load_refs: list[tuple[torch.cuda.Event, list[Any]]] = []
        # Deferred (between-step) loads in flight: req_id -> completion event
        # on the load stream, plus refs keeping mmap/GPU buffers alive until
        # completion is observed via get_finished_loading(). _deferred_done
        # collects synchronously finished (CPU fallback) or failed requests;
        # _failed_load_blocks feeds get_block_ids_with_load_errors so vLLM
        # recomputes instead of consuming unloaded KV.
        self._deferred_events: dict[str, torch.cuda.Event] = {}
        self._deferred_refs: dict[str, list[Any]] = {}
        self._deferred_done: set[str] = set()
        self._failed_load_blocks: set[int] = set()
        # True-async deferred loads (packed path): a single background thread
        # runs the whole load — Maru retrieve RPC + H2D on _deferred_stream —
        # so neither the RPC wait nor the copy blocks the engine thread's
        # forward passes (the in-process analog of MP's separate-process
        # retrieve). _deferred_lock guards every _deferred_*/failed structure;
        # both the loader thread and the engine thread mutate them.
        self._deferred_lock = threading.Lock()
        self._deferred_executor: ThreadPoolExecutor | None = None
        self._deferred_stream: torch.cuda.Stream | None = None
        self._deferred_stream_device: torch.device | None = None
        # Optional admission window: cap request loads enqueued on the
        # deferred stream but not yet complete. Slot mappings are pinned
        # before asynchronous H2D, so the default submit-all path no longer
        # triggers the pageable-copy driver stall. A positive value keeps the
        # previous request-level backpressure available as a fallback guard.
        self._load_admission_window = int(
            extra_config.get("maru_load_admission_window", 0)
        )
        # Packed layerwise overlap keeps the packed Maru object format. The
        # loader thread resolves only the RPC/mmap metadata; the resumed
        # forward consumes these entries layer-by-layer on _load_stream.
        self._deferred_layerwise_loads: dict[
            str, tuple[MaruReqMeta, int, torch.Tensor, list[Any]]
        ] = {}
        # Last non-None attention metadata; deferred loads run between steps
        # (possibly with no forward pass) and reuse it for layout dispatch.
        self._last_attn_metadata: Any = None
        # P5: fused UVA gather-scatter load. SMs read the host-registered CXL
        # pages directly and scatter into the paged KV cache in one kernel
        # (LMCache single_layer_kv_transfer), replacing the two-stage
        # cudaMemcpy H2D + inject path whose DMA tops out ~20 GiB/s.
        self._fused_load = bool(extra_config.get("maru_enable_fused_load", False))
        self._lmc_ops: Any = None

        # P6: storage granularity. Default (off) packs all layers of a chunk
        # into one CXL object with one key — matching LMCache use_layerwise=
        # False — so a request resolves num_chunks keys instead of
        # num_chunks x num_layers. Layerwise=True keeps the per-(chunk,layer)
        # objects and the layer-wise async overlap path.
        self._use_layerwise = bool(extra_config.get("maru_use_layerwise", False))
        self._layerwise_overlap = bool(
            extra_config.get("maru_enable_layerwise_overlap", False)
            and extra_config.get("maru_enable_deferred_loading", False)
            and not self._use_layerwise
        )
        if extra_config.get("maru_enable_layerwise_overlap", False) and not (
            extra_config.get("maru_enable_deferred_loading", False)
            and not self._use_layerwise
        ):
            logger.warning(
                "Maru packed-layerwise overlap requires deferred loading and "
                "maru_use_layerwise=false; disabling it"
            )
        # Packed store accumulates a chunk's per-layer slices across the
        # per-layer save_kv_layer calls of one step: base_key -> (handle,
        # layers_written). Registered once when all layers are present.
        # (Fallback path only — the kernel store writes whole slabs at once.)
        self._pending_slabs: dict[str, tuple[Any, set[int]]] = {}
        # Slab handles a step-boundary sweep could not free because the
        # handler was down. Retried by the next sweep; kept out of
        # _pending_slabs so no later layer resumes writing into them.
        self._orphan_slab_handles: list[Any] = []
        # Coalesced packed store: multi_layer_kv_transfer ctx built from the
        # registered KV caches, cached because the pointer table is static.
        # None = unresolved, False = kernel unusable (per-layer fallback).
        self._store_kernel_ctx: tuple | None = None
        self._store_kernel_unusable = False
        # Distinct layer indices whose save_kv_layer call was seen this step;
        # the kernel store dispatches when the set completes (order-agnostic).
        self._store_layers_seen: set[int] = set()
        # Dedicated stream for the kernel store's per-chunk D2H transfers.
        self._store_stream: torch.cuda.Stream | None = None
        self._store_stream_device: torch.device | None = None
        # Opt-in packed-store write-behind. After the forward, the engine thread
        # queues a short GPU gather plus copy-engine D2H on a dedicated stream;
        # a background thread waits for its event and registers the ready keys.
        # All state below is protected by _store_lock because the engine and
        # completion thread both update key/request lifetimes.
        self._write_behind = bool(extra_config.get("maru_enable_write_behind", False))
        self._store_executor: ThreadPoolExecutor | None = None
        self._store_lock = threading.Lock()
        self._pending_store_keys: set[str] = set()
        self._store_key_waiters: dict[str, set[str]] = {}
        self._request_pending_store_keys: dict[str, set[str]] = {}
        self._finished_store_requests: set[str] = set()
        self._store_staging: torch.Tensor | None = None
        # save_kv_layer runs inside the model forward. In write-behind mode it
        # records completed packed-store batches here; get_finished() launches
        # them after the forward has produced its output.
        self._queued_store_batches: list[tuple[tuple, MaruConnectorMetadata]] = []
        # CXL page size (bytes) auto-derived from the registered KV caches so a
        # single (chunk x layer) object exactly fills one page instead of
        # rounding up into the larger default page (~4x space waste). Set in
        # register_kv_caches; consumed by _ensure_handler. None -> keep the
        # configured/default page size.
        self._page_size_bytes: int | None = None

        # Backoff timer to avoid reconnect storms when server is down.
        self._handler_retry_after: float = 0.0

        # Opt-in per-request phase timing (diagnostics only).
        self._timing = bool(extra_config.get("maru_log_timing", False))

    def _ensure_handler(self):
        if self._handler is not None:
            return
        if time.monotonic() < self._handler_retry_after:
            return
        try:
            extra_config = self._extra_config
            # Auto-size the CXL page to the KV object so a 1 MiB object does not
            # occupy the larger default page. An explicit maru_chunk_size in
            # extra_config always wins (deployer override).
            if (
                self._page_size_bytes is not None
                and "maru_chunk_size" not in extra_config
            ):
                extra_config = {
                    **extra_config,
                    "maru_chunk_size": self._page_size_bytes,
                }
            self._effective_page_size_bytes = _parse_size(
                extra_config.get("maru_chunk_size", 4 * 1024 * 1024)
            )
            self._handler = _create_maru_handler(extra_config)
        except Exception:
            self._handler_retry_after = time.monotonic() + 5.0
            logger.warning("Worker MaruHandler creation failed, backing off 5s")

    def register_kv_caches(self, kv_caches: dict[str, torch.Tensor]):
        self._kv_caches = kv_caches
        self._num_layers = len(kv_caches)
        # Derive the CXL page size from the model's KV geometry so each
        # (chunk x layer) object fills exactly one page (no page-rounding waste).
        # Runs before the first _ensure_handler (start_load_kv / save_kv_layer),
        # so both the retrieve and store paths get the right page size.
        # Packed (default): one CXL object holds all layers of a chunk, so the
        # page is num_layers x the per-(chunk,layer) size.
        per_layer = self._chunk_object_bytes()
        if per_layer is not None and not self._use_layerwise and self._num_layers > 0:
            self._page_size_bytes = per_layer * self._num_layers
        else:
            self._page_size_bytes = per_layer
        logger.info(
            "MaruWorkerConnector: registered %d KV cache layers "
            "(auto CXL page size: %s bytes)",
            len(kv_caches),
            self._page_size_bytes if self._page_size_bytes is not None else "default",
        )
        # Connecting the worker eagerly is a performance requirement, not just
        # an initialization preference.  For a large CXL pool connect() maps,
        # prefaults, and CUDA-registers the owned region; a 200 GiB pool takes
        # several seconds.  Deferring that work to the first save_kv_layer()
        # puts it directly on the first populate request's TTFT critical path
        # (and stalls the whole first inflight wave).  register_kv_caches runs
        # during engine startup, after the model reveals the packed page size,
        # so it is the earliest safe point to pay this one-time cost.
        #
        # _ensure_handler remains best-effort. If this eager attempt fails,
        # clear its backoff so the first real load/store keeps the pre-existing
        # immediate retry behavior once MaruServer becomes ready.
        if kv_caches:
            self._ensure_handler()
            if self._handler is None:
                self._handler_retry_after = 0.0

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

    @staticmethod
    def _pin_slot_mapping_for_async_h2d(
        slot_mapping: torch.Tensor,
    ) -> torch.Tensor:
        """Return a page-locked CPU slot mapping for asynchronous H2D.

        Callers invoke this helper only after selecting a CUDA load path.
        Keeping the policy outside :meth:`_build_slot_mapping` leaves CPU
        fallbacks and store-only paths unchanged.
        """
        if slot_mapping.device.type != "cpu":
            raise ValueError("slot mapping for H2D must be a CPU tensor")
        if slot_mapping.is_pinned():
            return slot_mapping
        return slot_mapping.pin_memory()

    def start_load_kv(
        self,
        forward_context: ForwardContext,
        metadata: MaruConnectorMetadata,
    ) -> None:
        """Load KV caches from Maru CXL into GPU paged buffers.

        P1 (batch retrieve): all ``(layer x chunk)`` keys of a request are
        fetched with a single batched ``batch_retrieve`` (payload-bounded RPC
        chunks) instead of one RPC per ``(layer, chunk)``. This collapses
        ``num_layers x num_chunks`` single retrieves (e.g. ~8,000 for a 64k
        prompt on a 32-layer model) into a few batched calls; that per-op RPC
        round-trip dominated cache-hit TTFT/TPOT.
        """
        # Step boundary: per-step store state must not leak into this step,
        # even when this method bails out early below. A carried-over
        # _store_layers_seen would let a later step's packed store fire before
        # its last layers have written the paged cache (stale KV registered as
        # valid); an incomplete fallback slab can never complete once the step
        # that allocated it ended.
        self._store_layers_seen.clear()
        self._reclaim_stale_pending_slabs()

        self._ensure_handler()
        if self._handler is None:
            self._abort_deferred_loads(metadata)
            return

        attn_metadata = forward_context.attn_metadata
        if attn_metadata is not None:
            self._last_attn_metadata = attn_metadata

        # Attention layers (name, GPU kv tensor, layer index), computed once for
        # this forward pass. Layer index uses the same derivation as the save
        # path so keys match (enumerate index can diverge when
        # no_compile_layers holds non-attention layers).
        layers: list[tuple[str, torch.Tensor, int]] = []
        for layer_name in forward_context.no_compile_layers:
            layer = forward_context.no_compile_layers[layer_name]
            kv_cache_attr = getattr(layer, "kv_cache", None)
            if kv_cache_attr is None:
                continue
            if isinstance(kv_cache_attr, (list, tuple)):
                virtual_engine = getattr(forward_context, "virtual_engine", 0)
                kv_cache_layer = kv_cache_attr[virtual_engine]
            else:
                kv_cache_layer = kv_cache_attr
            layers.append(
                (layer_name, kv_cache_layer, self._get_layer_index(layer_name))
            )
        if not layers:
            self._abort_deferred_loads(metadata)
            return

        prepared_requests: list[tuple[MaruReqMeta, int, torch.Tensor, list[Any]]] = []
        self._layer_load_events.clear()
        self._release_completed_load_refs()
        if metadata.layerwise_load_req_ids:
            self._schedule_deferred_packed_layerwise_loads(
                layers,
                metadata.layerwise_load_req_ids,
                attn_metadata,
            )

        for req_meta in metadata.requests:
            if req_meta.is_store:
                continue
            if req_meta.num_matched_chunks == 0:
                # Nothing to load, but a deferred request is still parked in
                # WAITING_FOR_REMOTE_KVS and must be reported. The scheduler
                # emits num_matched_chunks=0 whenever its match result is
                # missing at allocation time (update_state_after_alloc
                # defaults to 0), so this is the third way a parked request
                # can reach the worker with no load to perform.
                # _fail_deferred_load ignores inline requests.
                self._fail_deferred_load(req_meta)
                continue

            # Packed deferred loads run off-thread. The default path performs
            # retrieve + whole-request H2D there; packed layerwise overlap
            # performs only retrieve and lets the resumed forward activate
            # per-layer H2D events. Falls through to the synchronous packed
            # path when the async prerequisites are missing.
            if (
                req_meta.deferred_load
                and not self._use_layerwise
                and self._try_submit_deferred_packed_load(req_meta)
            ):
                continue

            chunk_keys = _req_chunk_keys(req_meta, self._kv_chunk_tokens)
            num_chunks = min(req_meta.num_matched_chunks, len(chunk_keys))
            if num_chunks == 0:
                self._fail_deferred_load(req_meta)
                continue
            total_tokens = num_chunks * self._kv_chunk_tokens
            slot_mapping = self._build_slot_mapping(req_meta.block_ids, total_tokens)

            # Packed (default): one key per chunk (num_chunks keys). Layerwise:
            # one key per (chunk, layer), layer-major (num_chunks x num_layers).
            if self._use_layerwise:
                keys = [
                    f"{chunk_keys[ci]}_L{layer_idx}"
                    for (_, _, layer_idx) in layers
                    for ci in range(num_chunks)
                ]
            else:
                keys = [chunk_keys[ci] for ci in range(num_chunks)]
            try:
                _t0 = time.monotonic()
                infos = self._batch_retrieve_all(keys)
                if self._timing:
                    _emit_timing(
                        f"retrieve batch {len(keys)} keys = "
                        f"{(time.monotonic() - _t0) * 1000:.2f} ms (req {req_meta.req_id})"
                    )
            except Exception as e:
                logger.error(
                    "Maru batch_retrieve failed for req %s: %s", req_meta.req_id, e
                )
                self._fail_deferred_load(req_meta)
                continue

            # All-or-nothing: a miss (chunk evicted between the scheduler's
            # exists-check and this load) aborts the request's load so we never
            # inject a partially-populated (corrupt) KV cache — vLLM recomputes.
            miss = next((i for i, v in enumerate(infos) if v is None), -1)
            if miss >= 0:
                logger.warning(
                    "Maru load miss: %s — aborting load for req %s (recompute)",
                    keys[miss],
                    req_meta.req_id,
                )
                self._fail_deferred_load(req_meta)
                continue

            # Packed (default): keep the per-chunk slab infos whole (num_chunks
            # infos). Layerwise: infos are already per-(layer,chunk).
            prepared_requests.append((req_meta, num_chunks, slot_mapping, infos))

        if not prepared_requests:
            return

        # Packed load: one large H2D per contiguous slab, freed per chunk (no
        # GPU-staging retention → no OOM at concurrency, unlike the reverted
        # v2 staging attempt), preserving slab contiguity instead of v1's
        # per-(layer,chunk) copies. See design note "P6 v2 시도 2".
        if not self._use_layerwise:
            _t0 = time.monotonic()
            self._load_packed(layers, prepared_requests, attn_metadata)
            if self._timing:
                _emit_timing(
                    f"packed-load wall {len(prepared_requests)} req = "
                    f"{(time.monotonic() - _t0) * 1000:.2f} ms"
                )
            return

        deferred = [e for e in prepared_requests if e[0].deferred_load]
        inline = [e for e in prepared_requests if not e[0].deferred_load]

        if deferred:
            self._schedule_deferred_loads(layers, deferred)

        if not inline:
            return
        if attn_metadata is None:
            # Inline loads always accompany a forward pass; without attention
            # metadata the layout dispatch cannot run.
            logger.error(
                "Maru: %d inline load(s) without attention metadata; skipping",
                len(inline),
            )
            return

        async_scheduled = False
        if self._async_loading:
            async_scheduled = self._schedule_async_loads(layers, inline, attn_metadata)
        if not async_scheduled:
            self._load_sync(layers, inline, attn_metadata)

        mode = "async-scheduled" if async_scheduled else "loaded"
        for req_meta, num_chunks, _, _ in inline:
            logger.info(
                "Maru: batch-%s %d layers x %d chunks (%d tokens) for req %s",
                mode,
                len(layers),
                num_chunks,
                num_chunks * self._kv_chunk_tokens,
                req_meta.req_id,
            )

    def _try_submit_deferred_packed_load(self, req_meta: MaruReqMeta) -> bool:
        """Hand a packed deferred load to the background loader thread.

        Returns:
            True when the load was submitted. False when the async path
            cannot run (KV caches not registered yet, non-CUDA or
            multi-device layout) — the caller then falls back to the
            synchronous packed load, which reports the request finished
            immediately.
        """
        if not self._kv_caches or self._num_layers <= 0:
            return False
        if not torch.cuda.is_available():
            return False
        devices = {kv.device for kv in self._kv_caches.values()}
        if len(devices) != 1:
            return False
        device = next(iter(devices))
        if device.type != "cuda":
            return False
        layers = [
            (name, kv_cache, self._get_layer_index(name))
            for name, kv_cache in self._kv_caches.items()
        ]
        if self._deferred_executor is None:
            self._deferred_executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="maru-deferred-load"
            )
        self._deferred_executor.submit(
            self._deferred_packed_load_job, req_meta, layers, device
        )
        return True

    def _deferred_packed_load_job(
        self,
        req_meta: MaruReqMeta,
        layers: list[tuple[str, torch.Tensor, int]],
        device: torch.device,
    ) -> None:
        """Prepare one parked request's packed KV off-thread.

        Runs on the deferred-load thread. The Maru RPC client serializes
        socket use internally, so retrieving here while the engine thread
        stores is safe. The default mode also performs H2D and records a CUDA
        event. Packed-layerwise mode stops after retrieve and retains the CXL
        views for the resumed forward. Any failure reports the request's
        blocks through ``take_failed_load_blocks`` so vLLM recomputes instead
        of consuming unloaded KV.
        """
        try:
            handler = self._handler
            chunk_keys = _req_chunk_keys(req_meta, self._kv_chunk_tokens)
            num_chunks = min(req_meta.num_matched_chunks, len(chunk_keys))
            if handler is None or num_chunks == 0:
                self._fail_deferred_load(req_meta)
                return
            total_tokens = num_chunks * self._kv_chunk_tokens
            slot_mapping = self._build_slot_mapping(req_meta.block_ids, total_tokens)
            keys = [chunk_keys[ci] for ci in range(num_chunks)]

            _t0 = time.monotonic()
            infos = self._batch_retrieve_all(keys)
            if self._timing:
                _emit_timing(
                    f"deferred retrieve batch {len(keys)} keys = "
                    f"{(time.monotonic() - _t0) * 1000:.2f} ms (req {req_meta.req_id})"
                )
            miss = next((i for i, v in enumerate(infos) if v is None), -1)
            if miss >= 0:
                logger.warning(
                    "Maru deferred load miss: %s — recompute for req %s",
                    keys[miss],
                    req_meta.req_id,
                )
                self._fail_deferred_load(req_meta)
                return

            if req_meta.layerwise_load:
                # The RPC/mmap part is complete. Unpark the request now and
                # retain the packed CXL views for its resumed forward, where
                # H2D is issued layer-major and gated by per-layer events.
                # This preserves packed keys/pages while making the bulk load
                # overlap the request's own compute.
                with self._deferred_lock:
                    self._deferred_layerwise_loads[req_meta.req_id] = (
                        req_meta,
                        num_chunks,
                        slot_mapping,
                        infos,
                    )
                    self._deferred_done.add(req_meta.req_id)
                logger.info(
                    "Maru: deferred retrieve ready for packed-layerwise load "
                    "%d chunks (%d tokens) for req %s",
                    num_chunks,
                    total_tokens,
                    req_meta.req_id,
                )
                return

            slot_mapping = self._pin_slot_mapping_for_async_h2d(slot_mapping)
            torch.cuda.set_device(device)
            if self._deferred_stream is None or self._deferred_stream_device != device:
                # Highest priority: parked requests' TTFT gates on these
                # copies.
                self._deferred_stream = torch.cuda.Stream(device=device, priority=-1)
                self._deferred_stream_device = device
            stream = self._deferred_stream

            if self._load_admission_window > 0:
                self._wait_for_load_admission(req_meta.req_id)

            attn = self._last_attn_metadata
            # Content-identical to _packed_load_kernel_ctx, but built from the
            # registered KV caches and cached across calls.
            kernel = self._packed_store_kernel_ctx(attn)
            dtype = layers[0][1].dtype
            ct = self._kv_chunk_tokens
            num_layers = len(layers)
            with torch.cuda.stream(stream):
                slot_gpu = slot_mapping.to(device, non_blocking=True)
                for ci in range(num_chunks):
                    chunk_slots = slot_gpu[ci * ct : (ci + 1) * ct]
                    slab_host = torch.frombuffer(infos[ci].view, dtype=dtype).view(
                        2, num_layers, ct, -1
                    )
                    if kernel is not None:
                        # Stage the slab through the copy engine (async DMA —
                        # the CXL mapping is cudaHostRegister'ed by the
                        # mapper), then scatter the device-resident slab with
                        # one brief kernel. Running the UVA kernel directly on
                        # the host slab would occupy SMs for the whole
                        # CXL-read duration and stall concurrent decode — the
                        # very stall this deferred path exists to remove
                        # (measured: TPOT stuck at the inline path's level,
                        # while MP's media-invariant TPOT shows its bulk
                        # bytes ride the copy engine).
                        ops, ptrs, pbs, block_size, head_size, fmt = kernel
                        slab_dev = slab_host.to(device, non_blocking=True)
                        ops.multi_layer_kv_transfer(
                            slab_dev,
                            ptrs,
                            chunk_slots,
                            device,
                            pbs,
                            ops.TransferDirection.H2D,
                            fmt,
                            block_size=block_size,
                            head_size=head_size,
                        )
                    else:
                        slab_dev = slab_host.to(device, non_blocking=True)
                        for layer_name, kv_cache_layer, true_idx in layers:
                            self._inject_kv_into_layer(
                                kv_cache_layer,
                                slab_dev[:, true_idx],
                                chunk_slots,
                                attn,
                                layer_name,
                                num_chunks=1,
                            )
                event = torch.cuda.Event()
                event.record(stream)
            with self._deferred_lock:
                self._deferred_events[req_meta.req_id] = event
                self._deferred_refs[req_meta.req_id] = [
                    infos,
                    slot_mapping,
                    slot_gpu,
                ]
            logger.info(
                "Maru: deferred-load scheduled %d layers x %d chunks "
                "(%d tokens) for req %s (packed, off-thread)",
                num_layers,
                num_chunks,
                total_tokens,
                req_meta.req_id,
            )
        except Exception as e:
            logger.error("Maru deferred load failed for req %s: %s", req_meta.req_id, e)
            # Copies may already be queued referencing the CXL views; drain
            # the stream before dropping them so no kernel reads freed memory.
            if self._deferred_stream is not None:
                try:
                    self._deferred_stream.synchronize()
                except Exception:
                    pass
            self._fail_deferred_load(req_meta)

    def _wait_for_load_admission(self, req_id: str) -> None:
        """Block until fewer than the admission window of loads are in flight.

        Runs on the loader thread just before a request's GPU enqueue. Counts
        the deferred loads whose completion event has not fired yet and, while
        the count is at or above ``maru_load_admission_window``, waits on the
        oldest one. The deferred stream is FIFO, so dict insertion order is
        completion order and waiting on the oldest event is the shortest wait.
        The gated GPU work is already enqueued and depends on nothing from
        this thread, so the wait always terminates.

        Args:
            req_id: Request whose enqueue is being admitted (log label only).
        """
        wait_t0 = time.monotonic()
        waited = False
        while True:
            with self._deferred_lock:
                pending = [
                    event
                    for event in self._deferred_events.values()
                    if not event.query()
                ]
            if len(pending) < self._load_admission_window:
                break
            waited = True
            pending[0].synchronize()
        if self._timing and waited:
            _emit_timing(
                f"admission wait {(time.monotonic() - wait_t0) * 1000:.2f} ms "
                f"(req {req_id})"
            )

    def _schedule_deferred_packed_layerwise_loads(
        self,
        layers: list[tuple[str, torch.Tensor, int]],
        req_ids: set[str],
        attn_metadata: AttentionMetadata | None,
    ) -> None:
        """Activate retrieved packed slabs as a layer-major H2D pipeline.

        The background loader already completed Maru RPC/mmap lookup. This
        method runs at the beginning of the resumed forward: for layer k it
        copies only that layer's K/V slices from every packed CXL slab, injects
        them into paged KV on ``_load_stream``, and records an event. Attention
        layer k waits for only that event, leaving layer k+1 transfer queued in
        parallel with layer k compute. No device-wide synchronize is used.
        """
        entries: list[tuple[MaruReqMeta, int, torch.Tensor, list[Any]]] = []
        missing: list[str] = []
        with self._deferred_lock:
            for req_id in req_ids:
                entry = self._deferred_layerwise_loads.pop(req_id, None)
                if entry is None:
                    missing.append(req_id)
                else:
                    entries.append(entry)
        if missing:
            logger.warning(
                "Maru packed-layerwise activation missing retained load(s): %s",
                ", ".join(sorted(missing)),
            )
        if not entries:
            return

        devices = {layer.device for _, layer, _ in layers}
        device = next(iter(devices)) if len(devices) == 1 else None
        if device is None or device.type != "cuda" or not torch.cuda.is_available():
            # Correctness fallback for CPU/mixed layouts. It preserves packed
            # storage but cannot overlap because there is no common CUDA stream.
            for _, num_chunks, slot_mapping, infos in entries:
                for layer_name, kv_cache_layer, true_idx in layers:
                    for ci in range(num_chunks):
                        slab_host = torch.frombuffer(
                            infos[ci].view, dtype=kv_cache_layer.dtype
                        ).view(2, self._num_layers, self._kv_chunk_tokens, -1)
                        chunk_start = ci * self._kv_chunk_tokens
                        chunk_slots = slot_mapping[
                            chunk_start : chunk_start + self._kv_chunk_tokens
                        ]
                        self._inject_kv_into_layer(
                            kv_cache_layer,
                            slab_host[:, true_idx].to(kv_cache_layer.device),
                            chunk_slots.to(kv_cache_layer.device),
                            attn_metadata,
                            layer_name,
                        )
            return

        if self._load_stream is None or self._load_stream_device != device:
            self._load_stream = torch.cuda.Stream(device=device, priority=-1)
            self._load_stream_device = device
        load_stream = self._load_stream
        load_stream.wait_stream(torch.cuda.current_stream(device))
        entries = [
            (
                req_meta,
                num_chunks,
                self._pin_slot_mapping_for_async_h2d(slot_mapping),
                infos,
            )
            for req_meta, num_chunks, slot_mapping, infos in entries
        ]

        _t0 = time.monotonic()
        try:
            with torch.cuda.stream(load_stream):
                slot_mappings_gpu = [
                    slot_mapping.to(device, non_blocking=True)
                    for _, _, slot_mapping, _ in entries
                ]
                for layer_name, kv_cache_layer, true_idx in layers:
                    for (_, num_chunks, _, infos), slot_gpu in zip(
                        entries, slot_mappings_gpu, strict=True
                    ):
                        layer_dev = self._copy_packed_layer_to_device(
                            infos,
                            num_chunks,
                            true_idx,
                            kv_cache_layer,
                            load_stream,
                        )
                        self._inject_kv_into_layer(
                            kv_cache_layer,
                            layer_dev,
                            slot_gpu[: num_chunks * self._kv_chunk_tokens],
                            attn_metadata,
                            layer_name,
                            num_chunks=num_chunks,
                        )
                    event = torch.cuda.Event()
                    event.record(load_stream)
                    self._layer_load_events[layer_name] = event
        except Exception as e:
            logger.error("Maru packed-layerwise activation failed: %s", e)
            try:
                load_stream.synchronize()
            except Exception:
                pass
            with self._deferred_lock:
                for req_meta, *_ in entries:
                    self._failed_load_blocks.update(req_meta.block_ids)
            self._layer_load_events.clear()
            return

        # CXL views and slot mappings must outlive the queued H2D copies: a
        # batch event recorded after every copy gates the release of these
        # refs (next start_load_kv). CUDA's caching allocator tracks the
        # short-lived layer_dev tensors on load_stream, allowing their storage
        # to be safely reused in stream order without retaining
        # num_layers x num_chunks device buffers.
        batch_event = torch.cuda.Event()
        batch_event.record(load_stream)
        refs: list[Any] = [infos for _, _, _, infos in entries]
        refs.extend(slot_mapping for _, _, slot_mapping, _ in entries)
        refs.extend(slot_mappings_gpu)
        self._active_load_refs.append((batch_event, refs))
        logger.info(
            "Maru: packed-layerwise scheduled %d layers x %d requests on load stream",
            len(layers),
            len(entries),
        )
        if self._timing:
            _emit_timing(
                f"packed-layerwise schedule {len(layers)}L x {len(entries)}r = "
                f"{(time.monotonic() - _t0) * 1000:.2f} ms"
            )

    def _copy_packed_layer_to_device(
        self,
        infos: list[Any],
        num_chunks: int,
        layer_idx: int,
        kv_cache_layer: torch.Tensor,
        stream: torch.cuda.Stream,
    ) -> torch.Tensor:
        """DMA one layer from packed chunk pages into one device tensor.

        One packed page is ``[2, layers, tokens, hidden]``. For a contiguous
        page run, each layer's K (or V) plane is a fixed-width row separated
        by one page pitch. Two ``cudaMemcpy2DAsync`` calls gather every chunk
        directly from registered CXL into ``[chunks, 2, tokens, hidden]``.
        The caller can then inject all chunks with one kernel. Gapped page
        allocations become multiple pitched runs but keep the same output.
        """
        if not infos or num_chunks <= 0:
            raise ValueError("packed layer copy requires at least one chunk")
        copy_2d = _get_cuda_memcpy2d_async()
        if copy_2d is None:
            raise RuntimeError("cudaMemcpy2DAsync is unavailable")

        first_slab = torch.frombuffer(infos[0].view, dtype=kv_cache_layer.dtype).view(
            2, self._num_layers, self._kv_chunk_tokens, -1
        )
        hidden = first_slab.shape[-1]
        layer_dev = torch.empty(
            (num_chunks, 2, self._kv_chunk_tokens, hidden),
            dtype=kv_cache_layer.dtype,
            device=kv_cache_layer.device,
        )
        plane_bytes = self._kv_chunk_tokens * hidden * kv_cache_layer.element_size()
        slab_bytes = 2 * self._num_layers * plane_bytes
        destination_pitch = 2 * plane_bytes

        for chunk_start, run_chunks, run_view in self._chunk_runs(infos[:num_chunks]):
            run_host = torch.frombuffer(run_view, dtype=kv_cache_layer.dtype)
            source_pitch = (
                self._effective_page_size_bytes
                if run_chunks > 1 and self._effective_page_size_bytes is not None
                else slab_bytes
            )
            for kv_idx in range(2):
                destination = (
                    layer_dev.data_ptr()
                    + chunk_start * destination_pitch
                    + kv_idx * plane_bytes
                )
                source = (
                    run_host.data_ptr()
                    + (kv_idx * self._num_layers + layer_idx) * plane_bytes
                )
                error = copy_2d(
                    destination,
                    destination_pitch,
                    source,
                    source_pitch,
                    plane_bytes,
                    run_chunks,
                    1,  # cudaMemcpyHostToDevice
                    stream.cuda_stream,
                )
                if error != 0:
                    raise RuntimeError(f"cudaMemcpy2DAsync failed with code {error}")
        return layer_dev

    def _fail_deferred_load(self, req_meta: MaruReqMeta) -> None:
        """Mark a deferred load as failed so the scheduler recomputes it.

        The request's blocks are reported through
        ``get_block_ids_with_load_errors`` (vLLM resets its computed-token
        count) and the request id through ``get_finished_loading`` (vLLM
        unparks it from WAITING_FOR_REMOTE_KVS). Inline loads need neither —
        the sync path simply recomputes.
        """
        if not req_meta.deferred_load:
            return
        with self._deferred_lock:
            self._failed_load_blocks.update(req_meta.block_ids)
            self._deferred_done.add(req_meta.req_id)

    def _abort_deferred_loads(self, metadata: MaruConnectorMetadata) -> None:
        """Degrade every deferred load in ``metadata`` to recompute.

        Called before ``start_load_kv`` returns without iterating
        ``metadata.requests`` (worker handler unavailable, no attention
        layers). The scheduler has already parked these requests in
        WAITING_FOR_REMOTE_KVS; unless they are reported through
        ``get_finished_loading`` (with their blocks through
        ``get_block_ids_with_load_errors``), vLLM keeps them parked forever
        and never frees their blocks. Inline requests are unaffected —
        ``_fail_deferred_load`` ignores them.

        Args:
            metadata: The step's connector metadata.
        """
        for req_meta in metadata.requests:
            self._fail_deferred_load(req_meta)

    def _schedule_deferred_loads(
        self,
        layers: list[tuple[str, torch.Tensor, int]],
        entries: list[tuple[MaruReqMeta, int, torch.Tensor, list[Any]]],
    ) -> None:
        """Run between-step loads on the dedicated stream, one event per request.

        These requests are parked in WAITING_FOR_REMOTE_KVS, so the copies
        never compete with the current batch's compute; completion is polled
        via ``get_finished_loading``. CPU or mixed-device layouts fall back to
        an immediate synchronous load reported as instantly finished.
        """
        attn_metadata = self._last_attn_metadata
        devices = {layer.device for _, layer, _ in layers}
        device = next(iter(devices)) if len(devices) == 1 else None
        if device is None or device.type != "cuda" or not torch.cuda.is_available():
            self._load_sync(layers, entries, attn_metadata)
            with self._deferred_lock:
                for req_meta, *_ in entries:
                    self._deferred_done.add(req_meta.req_id)
            return

        if self._load_stream is None or self._load_stream_device != device:
            # Highest priority: cache-hit loads gate TTFT directly.
            self._load_stream = torch.cuda.Stream(device=device, priority=-1)
            self._load_stream_device = device
        load_stream = self._load_stream

        for req_meta, num_chunks, slot_mapping, infos in entries:
            slot_mapping = self._pin_slot_mapping_for_async_h2d(slot_mapping)
            with torch.cuda.stream(load_stream):
                slot_mapping_gpu = slot_mapping.to(device, non_blocking=True)
                for li, (layer_name, kv_cache_layer, _) in enumerate(layers):
                    base = li * num_chunks
                    for chunk_start, run_chunks, run_view in self._chunk_runs(
                        infos[base : base + num_chunks]
                    ):
                        chunk_tensor = torch.frombuffer(
                            run_view, dtype=kv_cache_layer.dtype
                        )
                        token_start = chunk_start * self._kv_chunk_tokens
                        token_end = (chunk_start + run_chunks) * self._kv_chunk_tokens
                        self._inject_kv_into_layer(
                            kv_cache_layer,
                            chunk_tensor.to(device, non_blocking=True),
                            slot_mapping_gpu[token_start:token_end],
                            attn_metadata,
                            layer_name,
                            num_chunks=run_chunks,
                        )
                event = torch.cuda.Event()
                event.record(load_stream)
            with self._deferred_lock:
                self._deferred_events[req_meta.req_id] = event
                self._deferred_refs[req_meta.req_id] = [
                    infos,
                    slot_mapping,
                    slot_mapping_gpu,
                ]
            logger.info(
                "Maru: deferred-load scheduled %d layers x %d chunks "
                "(%d tokens) for req %s",
                len(layers),
                num_chunks,
                num_chunks * self._kv_chunk_tokens,
                req_meta.req_id,
            )

    def get_finished_loading(self) -> set[str] | None:
        """Return req ids whose deferred loads completed since the last call."""
        with self._deferred_lock:
            done = set(self._deferred_done)
            self._deferred_done.clear()
            for req_id, event in list(self._deferred_events.items()):
                if event.query():
                    done.add(req_id)
                    del self._deferred_events[req_id]
                    self._deferred_refs.pop(req_id, None)
        return done or None

    def get_finished_saving(self, finished_req_ids: set[str]) -> set[str] | None:
        """Return finished requests whose write-behind stores have drained.

        ``request_finished`` transfers ownership of a finished request's GPU
        blocks to the connector. A request ID may be presented only once by
        vLLM, so remember it until every key associated with that request has
        finished its D2H and ``batch_store`` registration. Store failures also
        release the request: they make that cache entry unavailable, but must
        never leak GPU blocks.
        """
        if not self._write_behind:
            return None
        queued = self._queued_store_batches
        self._queued_store_batches = []
        for kernel, metadata in queued:
            self._store_packed_slabs_write_behind(kernel, metadata)
        with self._store_lock:
            self._finished_store_requests.update(finished_req_ids)
            done = {
                req_id
                for req_id in self._finished_store_requests
                if req_id not in self._request_pending_store_keys
            }
            self._finished_store_requests.difference_update(done)
        return done or None

    def handle_preemptions(self, metadata: MaruConnectorMetadata) -> None:
        """Drain D2H before preempted block IDs can be reused by a forward.

        Metadata registration may continue in the background because it no
        longer reads the GPU cache. Only the store stream must be complete at
        this boundary.
        """
        if (
            self._write_behind
            and metadata.preempted_req_ids
            and self._store_stream is not None
        ):
            self._store_stream.synchronize()
        if self._layerwise_overlap and metadata.preempted_req_ids:
            # Retained entries have not queued H2D yet, so their CXL views can
            # be dropped immediately. A resumed request will receive fresh
            # blocks and run the normal match/load handshake again.
            with self._deferred_lock:
                for req_id in metadata.preempted_req_ids:
                    self._deferred_layerwise_loads.pop(req_id, None)

    def take_failed_load_blocks(self) -> set[int]:
        """Return (and clear) block ids whose deferred load failed."""
        with self._deferred_lock:
            failed = self._failed_load_blocks
            self._failed_load_blocks = set()
        return failed

    def _load_sync(
        self,
        layers: list[tuple[str, torch.Tensor, int]],
        prepared_requests: list[tuple[MaruReqMeta, int, torch.Tensor, list[Any]]],
        attn_metadata: AttentionMetadata,
    ) -> None:
        """Inject prepared CXL chunks synchronously on the current stream."""
        for _, num_chunks, slot_mapping, infos in prepared_requests:
            for li, (layer_name, kv_cache_layer, _) in enumerate(layers):
                base = li * num_chunks
                for ci in range(num_chunks):
                    info = infos[base + ci]
                    chunk_tensor = torch.frombuffer(
                        info.view, dtype=kv_cache_layer.dtype
                    )
                    chunk_start = ci * self._kv_chunk_tokens
                    chunk_slots = slot_mapping[
                        chunk_start : chunk_start + self._kv_chunk_tokens
                    ]
                    self._inject_kv_into_layer(
                        kv_cache_layer,
                        chunk_tensor.to(kv_cache_layer.device),
                        chunk_slots.to(kv_cache_layer.device),
                        attn_metadata,
                        layer_name,
                    )

    def _schedule_async_loads(
        self,
        layers: list[tuple[str, torch.Tensor, int]],
        prepared_requests: list[tuple[MaruReqMeta, int, torch.Tensor, list[Any]]],
        attn_metadata: AttentionMetadata,
    ) -> bool:
        """Schedule layer-major CXL->GPU loads on a dedicated CUDA stream.

        One event is recorded after every layer. ``wait_for_layer_load`` makes
        the model's current stream wait only for that layer, allowing later
        layer transfers to overlap with attention compute.

        Returns:
            True when asynchronous loads were scheduled. CPU or mixed-device
            layouts return False and use the synchronous fallback.
        """
        devices = {layer.device for _, layer, _ in layers}
        if len(devices) != 1:
            logger.warning(
                "Maru async load requires one CUDA device; falling back to sync"
            )
            return False
        device = next(iter(devices))
        if device.type != "cuda" or not torch.cuda.is_available():
            logger.warning(
                "Maru async load requires CUDA tensors; falling back to sync"
            )
            return False

        if self._load_stream is None or self._load_stream_device != device:
            # Highest priority: cache-hit loads gate TTFT directly.
            self._load_stream = torch.cuda.Stream(device=device, priority=-1)
            self._load_stream_device = device

        load_stream = self._load_stream
        load_stream.wait_stream(torch.cuda.current_stream(device))
        slot_mappings_host = [
            self._pin_slot_mapping_for_async_h2d(slot_mapping)
            for _, _, slot_mapping, _ in prepared_requests
        ]
        with torch.cuda.stream(load_stream):
            slot_mappings_gpu = [
                slot_mapping.to(device, non_blocking=True)
                for slot_mapping in slot_mappings_host
            ]
            # Layer-major ordering is essential for inflight >1: layer 0 for
            # every request must become ready before work for later layers.
            for li, (layer_name, kv_cache_layer, _) in enumerate(layers):
                use_fused = self._use_fused_load(attn_metadata, layer_name)
                for (_, num_chunks, _, infos), slot_mapping_gpu in zip(
                    prepared_requests, slot_mappings_gpu, strict=True
                ):
                    base = li * num_chunks
                    for chunk_start, run_chunks, run_view in self._chunk_runs(
                        infos[base : base + num_chunks]
                    ):
                        token_start = chunk_start * self._kv_chunk_tokens
                        token_end = (chunk_start + run_chunks) * self._kv_chunk_tokens
                        chunk_slots = slot_mapping_gpu[token_start:token_end]
                        if use_fused and self._fused_run_transfer(
                            kv_cache_layer, run_view, run_chunks, chunk_slots
                        ):
                            continue
                        chunk_tensor = torch.frombuffer(
                            run_view, dtype=kv_cache_layer.dtype
                        )
                        self._inject_kv_into_layer(
                            kv_cache_layer,
                            chunk_tensor.to(device, non_blocking=True),
                            chunk_slots,
                            attn_metadata,
                            layer_name,
                            num_chunks=run_chunks,
                        )
                event = torch.cuda.Event()
                event.record(load_stream)
                self._layer_load_events[layer_name] = event

        # A batch event recorded after every copy gates the release of these
        # refs (next start_load_kv).
        batch_event = torch.cuda.Event()
        batch_event.record(load_stream)
        refs: list[Any] = [infos for _, _, _, infos in prepared_requests]
        refs.extend(slot_mappings_host)
        refs.extend(slot_mappings_gpu)
        self._active_load_refs.append((batch_event, refs))
        return True

    def _ensure_fused_ops(self) -> bool:
        """Resolve ``lmcache.c_ops`` lazily; disable fused load if absent."""
        if not self._fused_load:
            return False
        if self._lmc_ops is not None:
            return True
        try:
            import lmcache.c_ops as lmc_ops

            self._lmc_ops = lmc_ops
            return True
        except ImportError:
            logger.warning(
                "maru_enable_fused_load: lmcache.c_ops unavailable; "
                "using memcpy+inject path"
            )
            self._fused_load = False
            return False

    def _use_fused_load(self, attn_metadata: Any, layer_name: str) -> bool:
        """Fused load applies only to the Flash paged-KV layout.

        MLA/Triton layouts keep the memcpy+inject path; layout dispatch
        mirrors ``_inject_kv_into_layer``.
        """
        if not self._ensure_fused_ops():
            return False
        from vllm.model_executor.layers.attention.mla_attention import (
            MLACommonMetadata,
        )
        from vllm.v1.attention.backends.triton_attn import TritonAttentionMetadata

        layer_meta = (
            attn_metadata[layer_name]
            if isinstance(attn_metadata, dict)
            else attn_metadata
        )
        return not isinstance(layer_meta, (MLACommonMetadata, TritonAttentionMetadata))

    def _fused_run_transfer(
        self,
        kv_cache_layer: torch.Tensor,
        run_view: memoryview,
        run_chunks: int,
        chunk_slots: torch.Tensor,
    ) -> bool:
        """Scatter a CXL page run into paged KV with one kernel per chunk.

        The kernel (LMCache ``single_layer_kv_transfer``) reads the
        host-registered CXL bytes directly from device (UVA) — no staging
        copy. Sources per chunk are ``[K/V, chunk_tokens, hidden]``
        (``token_major=False``); the vLLM Flash layer is
        ``[2, num_blocks, block_size, heads, head_size]``
        (``NL_X_TWO_NB_BS_NH_HS``).

        Returns:
            True when the run was handled; False to fall back to memcpy.
        """
        if not self._fused_load:
            return False
        obj_bytes = self._chunk_object_bytes()
        if obj_bytes is None or run_view.nbytes < run_chunks * obj_bytes:
            return False
        ops = self._lmc_ops
        ct = self._kv_chunk_tokens
        try:
            for i in range(run_chunks):
                src = torch.frombuffer(
                    run_view[i * obj_bytes : (i + 1) * obj_bytes],
                    dtype=kv_cache_layer.dtype,
                ).view(2, ct, -1)
                ops.single_layer_kv_transfer(
                    src,
                    kv_cache_layer,
                    chunk_slots[i * ct : (i + 1) * ct],
                    ops.TransferDirection.H2D,
                    ops.EngineKVFormat.NL_X_TWO_NB_BS_NH_HS,
                    token_major=False,
                )
        except Exception as e:
            logger.error("Maru fused load failed (%s); disabling fused path", e)
            self._fused_load = False
            return False
        return True

    def _load_packed(
        self,
        layers: list[tuple[str, torch.Tensor, int]],
        prepared_requests: list[tuple[MaruReqMeta, int, torch.Tensor, list[Any]]],
        attn_metadata: AttentionMetadata,
    ) -> None:
        """Load per-chunk slabs with no GPU staging (P6 v2, packed).

        The KV_2LTD slab (``[2, num_layers, chunk_tokens, hidden]``) is handed
        whole to LMCache's ``multi_layer_kv_transfer``, which reads the pinned
        CXL host memory directly (UVA) and scatters all layers into the paged
        GPU cache in one kernel per chunk — the same no-staging path LMCache's
        ``VLLMPagedMemGPUConnectorV2.to_gpu`` uses. This avoids both v1's 1,888
        tiny copies and the reverted GPU-staging OOM. Non-Flash layouts, CPU,
        or a missing ``lmcache.c_ops`` fall back to a per-layer inject that
        reads the same slab slices.

        Deferred (between-step) requests are marked finished immediately (the
        transfer completes on the current stream before this returns).
        """
        attn = attn_metadata if attn_metadata is not None else self._last_attn_metadata
        num_layers = len(layers)
        ct = self._kv_chunk_tokens
        kernel = self._packed_load_kernel_ctx(layers, attn)

        # Queue all per-chunk transfers on a dedicated high-priority stream and
        # sync once at the end — mirrors LMCache batched_to_gpu (per-chunk
        # multi_layer_kv_transfer on its load_stream, one synchronize). Keeps
        # the 59 launches pipelined instead of serializing on the compute
        # stream. Falls back to the current stream on CPU/non-CUDA.
        dev = layers[0][1].device
        use_stream = kernel is not None or (
            dev.type == "cuda" and torch.cuda.is_available()
        )
        if use_stream:
            if self._load_stream is None or self._load_stream_device != dev:
                self._load_stream = torch.cuda.Stream(device=dev, priority=-1)
                self._load_stream_device = dev
            self._load_stream.wait_stream(torch.cuda.current_stream(dev))
            stream_ctx = torch.cuda.stream(self._load_stream)
        else:
            import contextlib

            stream_ctx = contextlib.nullcontext()

        dtype = layers[0][1].dtype
        with stream_ctx:
            for req_meta, num_chunks, slot_mapping, slab_infos in prepared_requests:
                if use_stream:
                    slot_mapping = self._pin_slot_mapping_for_async_h2d(slot_mapping)
                slot_gpu = slot_mapping.to(dev, non_blocking=use_stream)
                for ci in range(num_chunks):
                    slab_view = slab_infos[ci].view
                    chunk_slots = slot_gpu[ci * ct : (ci + 1) * ct]
                    # KV_2LTD host tensor aliasing pinned CXL: [2, L, tokens, h]
                    slab_host = torch.frombuffer(slab_view, dtype=dtype).view(
                        2, num_layers, ct, -1
                    )
                    if kernel is not None:
                        ops, ptrs, pbs, block_size, head_size, fmt = kernel
                        ops.multi_layer_kv_transfer(
                            slab_host,
                            ptrs,
                            chunk_slots,
                            dev,
                            pbs,
                            ops.TransferDirection.H2D,
                            fmt,
                            block_size=block_size,
                            head_size=head_size,
                        )
                    else:
                        # Fallback: per-layer slice -> inject (same slab).
                        slab_dev = slab_host.to(dev, non_blocking=use_stream)
                        for layer_name, kv_cache_layer, true_idx in layers:
                            self._inject_kv_into_layer(
                                kv_cache_layer,
                                slab_dev[:, true_idx],
                                chunk_slots,
                                attn,
                                layer_name,
                                num_chunks=1,
                            )
                if req_meta.deferred_load:
                    with self._deferred_lock:
                        self._deferred_done.add(req_meta.req_id)
        if use_stream:
            # Loads must complete before the forward pass reads the paged cache
            # (packed makes wait_for_layer_load a no-op).
            assert self._load_stream is not None
            self._load_stream.synchronize()

        for req_meta, num_chunks, _, _ in prepared_requests:
            mode = "kernel" if kernel is not None else "fallback"
            logger.info(
                "Maru: packed-loaded %d layers x %d chunks (%d tokens) for req %s (%s)",
                num_layers,
                num_chunks,
                num_chunks * ct,
                req_meta.req_id,
                mode,
            )
            if self._timing:
                _emit_timing(
                    f"packed-load {mode} {num_layers}L x {num_chunks}c (req {req_meta.req_id})"
                )

    def _packed_load_kernel_ctx(
        self, layers: list[tuple[str, torch.Tensor, int]], attn_metadata: Any
    ) -> tuple | None:
        """Prepare multi_layer_kv_transfer args, or None to use the fallback.

        Returns ``(ops, kv_cache_pointers, page_buffer_size, block_size,
        head_size, engine_kv_format)`` when the fused no-staging kernel is
        usable: Flash layout, CUDA, and ``lmcache.c_ops`` importable. This is
        the DEFAULT packed load whenever available (not gated on
        maru_enable_fused_load — that flag is the separate P5 single-layer
        experiment). The pointer table is indexed by each layer's true
        ``_get_layer_index`` so it aligns with the slab's layer dimension.
        Paged buffers must be the vLLM Flash tensor
        ``[2, num_blocks, block_size, num_heads, head_size]``.
        """
        device = layers[0][1].device
        if device.type != "cuda" or not torch.cuda.is_available():
            return None
        sample = layers[0][1]
        if sample.dim() != 5:  # [2, num_blocks, block_size, num_heads, head_size]
            return None
        # Flash layout only (exclude MLA/Triton).
        from vllm.model_executor.layers.attention.mla_attention import (
            MLACommonMetadata,
        )
        from vllm.v1.attention.backends.triton_attn import TritonAttentionMetadata

        layer_meta = (
            attn_metadata[layers[0][0]]
            if isinstance(attn_metadata, dict)
            else attn_metadata
        )
        if isinstance(layer_meta, (MLACommonMetadata, TritonAttentionMetadata)):
            return None
        # Resolve lmcache.c_ops (independent of maru_enable_fused_load).
        if self._lmc_ops is None:
            try:
                import lmcache.c_ops as lmc_ops

                self._lmc_ops = lmc_ops
            except ImportError:
                logger.warning(
                    "Maru packed load: lmcache.c_ops unavailable; using "
                    "per-layer inject fallback"
                )
                return None
        num_blocks, block_size, num_heads, head_size = sample.shape[1:]
        page_buffer_size = num_blocks * block_size
        # Pointer table indexed by true layer index (== slab layer dim).
        ptrs = torch.empty(len(layers), dtype=torch.int64, device="cpu")
        for _, kv_cache_layer, true_idx in layers:
            ptrs[true_idx] = kv_cache_layer.data_ptr()
        ops = self._lmc_ops
        return (
            ops,
            ptrs.to(device),
            page_buffer_size,
            block_size,
            head_size,
            ops.EngineKVFormat.NL_X_TWO_NB_BS_NH_HS,
        )

    def _chunk_runs(self, infos: list[Any]) -> list[tuple[int, int, memoryview]]:
        """Return contiguous page runs as combined memoryviews.

        Chunked prefill commonly stores a layer in two or more allocation
        bursts. Within each burst the `(layer, chunk)` pages are consecutive,
        so one large H2D copy can replace one copy per chunk. A run is combined
        only when each KV object exactly fills its CXL page; otherwise page
        padding would appear between objects and each chunk is returned alone.

        Returns:
            ``(chunk_start, run_chunks, view)`` tuples covering every info.
        """
        if not infos:
            return []
        page_size = self._effective_page_size_bytes
        runs: list[tuple[int, int, memoryview]] = []
        start = 0
        while start < len(infos):
            first = infos[start]
            run_end = start + 1
            if page_size is not None and first.view.nbytes == page_size:
                while run_end < len(infos):
                    prev = infos[run_end - 1]
                    current = infos[run_end]
                    if (
                        current.region_id != first.region_id
                        or current.page_index != prev.page_index + 1
                        or current.view.nbytes != page_size
                        or current.view.obj is not first.view.obj
                    ):
                        break
                    run_end += 1

            run_chunks = run_end - start
            run_view = first.view
            if run_chunks > 1:
                assert page_size is not None
                try:
                    region_view = memoryview(first.view.obj).cast("B")
                    byte_start = first.page_index * page_size
                    byte_end = byte_start + run_chunks * page_size
                    combined = region_view[byte_start:byte_end]
                    if combined.nbytes == run_chunks * page_size:
                        run_view = combined
                    else:
                        run_chunks = 1
                        run_end = start + 1
                except (TypeError, ValueError):
                    run_chunks = 1
                    run_end = start + 1

            runs.append((start, run_chunks, run_view))
            start = run_end
        return runs

    def wait_for_layer_load(self, layer_name: str) -> None:
        """Make the model stream wait for an asynchronously loaded layer."""
        event = self._layer_load_events.get(layer_name)
        if event is not None:
            torch.cuda.current_stream().wait_event(event)

    def _release_completed_load_refs(self) -> None:
        """Drop load-batch refs whose queued copies have completed.

        Each ``_active_load_refs`` entry pairs the refs of one scheduled load
        batch with the event recorded after that batch's last copy. Entries
        whose event has not completed are retained: under vLLM async
        scheduling this method (called from the next step's
        ``start_load_kv``) can run while the previous step's copies are still
        queued, and the pinned slot mappings and CXL mmap views must outlive
        those copies.
        """
        if not self._active_load_refs:
            return
        self._active_load_refs = [
            entry for entry in self._active_load_refs if not entry[0].query()
        ]

    def _batch_retrieve_all(self, keys: list[str], batch_size: int = 1024) -> list:
        """``batch_retrieve`` over ``keys`` in payload-bounded chunks (ordered).

        A single request can produce num_layers x num_chunks keys (thousands),
        so the batch RPC is split into ``batch_size``-key chunks to bound
        payload while keeping RPC count ~O(len(keys) / batch_size).
        """
        handler = self._handler
        assert handler is not None
        if len(keys) <= batch_size:
            return list(handler.batch_retrieve(keys))
        out: list[Any] = []
        for i in range(0, len(keys), batch_size):
            out.extend(handler.batch_retrieve(keys[i : i + batch_size]))
        return out

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
        if attn_metadata is not None:
            self._last_attn_metadata = attn_metadata

        if not self._use_layerwise:
            self._save_kv_layer_packed(layer_name, kv_layer, attn_metadata, metadata)
            return

        layer_idx = self._get_layer_index(layer_name)

        for req_meta in metadata.requests:
            if not req_meta.is_store:
                continue

            chunk_keys = _req_chunk_keys(req_meta, self._kv_chunk_tokens)
            if not chunk_keys:
                continue

            # In chunked prefill, store every chunk that became complete in
            # this step: absolute chunk indices [computed // chunk_tokens,
            # (computed + scheduled) // chunk_tokens). Flooring both ends
            # keeps consecutive steps contiguous even when step boundaries
            # are not chunk-aligned — a chunk straddling a boundary is
            # stored by the step that finishes it. req_meta.block_ids covers
            # the request from token 0, so slots are indexed by absolute
            # token position.
            if req_meta.num_scheduled_tokens > 0:
                start_chunk = req_meta.num_computed_tokens // self._kv_chunk_tokens
                end_chunk = (
                    req_meta.num_computed_tokens + req_meta.num_scheduled_tokens
                ) // self._kv_chunk_tokens
            else:
                start_chunk, end_chunk = 0, len(chunk_keys)
            end_chunk = min(end_chunk, len(chunk_keys))
            if start_chunk >= end_chunk:
                continue

            needed_tokens = end_chunk * self._kv_chunk_tokens
            slot_mapping = self._build_slot_mapping(req_meta.block_ids, needed_tokens)
            if slot_mapping.shape[0] < needed_tokens:
                logger.error(
                    "Maru store: block_ids covers %d tokens but chunks up to "
                    "%d need %d for req %s; skipping store this step",
                    slot_mapping.shape[0],
                    end_chunk,
                    needed_tokens,
                    req_meta.req_id,
                )
                continue

            # P2 (batch store) — Phase 1: extract each chunk's KV into a
            # freshly-alloc'd CXL page (GPU->CXL copy), collecting
            # (key, base_key, handle) for one batched register instead of a
            # store RPC per chunk.
            pending_keys: list[str] = []
            pending_bases: list[str] = []
            pending_handles: list = []
            for ci in range(start_chunk, end_chunk):
                base_key = chunk_keys[ci]
                maru_key = f"{base_key}_L{layer_idx}"
                if maru_key in self._stored_keys:
                    continue

                # Extract this chunk's slots (absolute token positions)
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
                except Exception as e:
                    logger.error("Maru save prepare error: %s: %s", maru_key, e)
                    if handle is not None:
                        try:
                            self._handler.free(handle)
                        except Exception:
                            pass
                    continue

                pending_keys.append(maru_key)
                pending_bases.append(base_key)
                pending_handles.append(handle)

            if not pending_keys:
                continue

            # Phase 2: single batched register. batch_store takes ownership of
            # every handle (it frees duplicates/failures internally); it only
            # raises before consuming any, so the except path frees them all.
            try:
                results = self._handler.batch_store(pending_keys, pending_handles)
            except Exception as e:
                logger.error(
                    "Maru batch_store failed for req %s: %s", req_meta.req_id, e
                )
                self._free_handles_best_effort(pending_handles)
                continue

            # Phase 3: mark stored; write each chunk's _DONE marker once all
            # layers for that chunk have been stored.
            for maru_key, base_key, ok in zip(
                pending_keys, pending_bases, results, strict=False
            ):
                if not ok:
                    logger.warning("Maru store failed: %s", maru_key)
                    continue
                self._stored_keys.add(maru_key)
                progress = self._chunk_layer_progress.setdefault(base_key, set())
                progress.add(layer_idx)
                if self._num_layers > 0 and len(progress) >= self._num_layers:
                    self._write_done_marker(base_key)
                    del self._chunk_layer_progress[base_key]

    def _save_kv_layer_packed(
        self,
        layer_name: str,
        kv_layer: torch.Tensor,
        attn_metadata: AttentionMetadata,
        metadata: MaruConnectorMetadata,
    ) -> None:
        """Store all layers of a chunk in one CXL object (P6, default).

        vLLM calls this once per layer. Each chunk completed in this step is
        written as one slab in **KV_2LTD layout** ``[2, num_layers,
        chunk_tokens, hidden]`` — the format LMCache's
        ``multi_layer_kv_transfer`` consumes on load, so the packed load can
        hand the whole slab to that kernel (no GPU staging). The slab (one key
        = ``base_key``) is registered once complete, so its key presence is
        the completion marker (no ``_DONE``).

        Two paths fill the slab:

        - **Kernel (default when usable)**: every per-layer call only records
          its layer index until the step has seen all of them — i.e. the
          physically last call, whatever the order — when the whole step's KV
          is already in the paged buffers; ``_store_packed_slabs`` then
          writes each chunk's slab with one ``multi_layer_kv_transfer(D2H)``
          — the store-side mirror of ``_load_packed``, collapsing
          ``(chunk x layer)`` GPU->CXL copies into one transfer per chunk.
        - **Fallback** (non-Flash layout, CPU, or no ``lmcache.c_ops``): each
          layer's Flash extract ``[2, chunk_tokens, hidden]`` is written to
          ``slab[:, layer_idx]`` as before. Non-Flash extracts have a
          different rank and will raise (caught below → chunk skipped →
          recompute).
        """
        if self._num_layers <= 0:
            return
        handler = self._handler
        assert handler is not None
        layer_idx = self._get_layer_index(layer_name)

        kernel = self._packed_store_kernel_ctx(
            attn_metadata if attn_metadata is not None else self._last_attn_metadata
        )
        if kernel is not None:
            # Order-independent dispatch (mirrors the fallback's per-slab
            # `written` set): fire once the step has seen every distinct layer
            # index, whatever the physical call order. A mis-indexed layer
            # name collapses indices so the set never completes — the step's
            # store is silently skipped (recompute on retrieve) instead of
            # firing early on a stale paged cache.
            self._store_layers_seen.add(layer_idx)
            if len(self._store_layers_seen) >= self._num_layers:
                self._store_layers_seen.clear()
                if self._write_behind:
                    # Launch after the forward in get_finished_saving(), not
                    # from the final attention layer. This keeps store work off
                    # the current step's first-token critical path.
                    self._queued_store_batches.append((kernel, metadata))
                else:
                    self._store_packed_slabs(kernel, metadata)
            return

        for req_meta in metadata.requests:
            if not req_meta.is_store:
                continue
            chunk_keys = _req_chunk_keys(req_meta, self._kv_chunk_tokens)
            if not chunk_keys:
                continue

            if req_meta.num_scheduled_tokens > 0:
                start_chunk = req_meta.num_computed_tokens // self._kv_chunk_tokens
                end_chunk = (
                    req_meta.num_computed_tokens + req_meta.num_scheduled_tokens
                ) // self._kv_chunk_tokens
            else:
                start_chunk, end_chunk = 0, len(chunk_keys)
            end_chunk = min(end_chunk, len(chunk_keys))
            if start_chunk >= end_chunk:
                continue

            needed_tokens = end_chunk * self._kv_chunk_tokens
            slot_mapping = self._build_slot_mapping(req_meta.block_ids, needed_tokens)
            if slot_mapping.shape[0] < needed_tokens:
                logger.error(
                    "Maru packed store: block_ids covers %d tokens but chunks up "
                    "to %d need %d for req %s; skipping",
                    slot_mapping.shape[0],
                    end_chunk,
                    needed_tokens,
                    req_meta.req_id,
                )
                continue

            ready_keys: list[str] = []
            ready_handles: list = []
            for ci in range(start_chunk, end_chunk):
                base_key = chunk_keys[ci]
                if base_key in self._stored_keys:
                    continue

                chunk_slots = slot_mapping[
                    ci * self._kv_chunk_tokens : (ci + 1) * self._kv_chunk_tokens
                ]
                try:
                    chunk_slots_gpu = chunk_slots.to(kv_layer.device)
                    kv_data = self._extract_kv_from_layer(
                        kv_layer, chunk_slots_gpu, attn_metadata, layer_name
                    )
                    kv_contig = kv_data.detach().contiguous()
                    # Flash extract: [2, chunk_tokens, hidden] → KV_2LTD slab.
                    kv2, ntok, hidden = kv_contig.shape
                    layer_bytes = kv_contig.nelement() * kv_contig.element_size()

                    pending = self._pending_slabs.get(base_key)
                    if pending is None:
                        handle = handler.alloc(layer_bytes * self._num_layers)
                        written: set[int] = set()
                        self._pending_slabs[base_key] = (handle, written)
                    else:
                        handle, written = pending

                    # Whole slab as [2, num_layers, tokens, hidden]; write this
                    # layer's [2, tokens, hidden] plane (strided K/V write).
                    slab_bytes = layer_bytes * self._num_layers
                    slab = torch.frombuffer(
                        handle.buf[:slab_bytes], dtype=kv_contig.dtype
                    ).view(kv2, self._num_layers, ntok, hidden)
                    slab[:, layer_idx].copy_(kv_contig)  # GPU->CXL
                    written.add(layer_idx)
                except Exception as e:
                    logger.error("Maru packed save error: %s: %s", base_key, e)
                    self._discard_pending_slab(base_key)
                    continue

                if len(written) >= self._num_layers:
                    ready_keys.append(base_key)
                    ready_handles.append(handle)
                    del self._pending_slabs[base_key]

            if not ready_keys:
                continue
            try:
                results = handler.batch_store(ready_keys, ready_handles)
            except Exception as e:
                logger.error(
                    "Maru packed batch_store failed for req %s: %s", req_meta.req_id, e
                )
                self._free_handles_best_effort(ready_handles)
                continue
            for base_key, ok in zip(ready_keys, results, strict=False):
                if ok:
                    self._stored_keys.add(base_key)
                else:
                    logger.warning("Maru packed store failed: %s", base_key)

    def _packed_store_kernel_ctx(self, attn_metadata: Any) -> tuple | None:
        """Kernel ctx for the coalesced packed store, or None for the fallback.

        Same contract as ``_packed_load_kernel_ctx`` but built from the
        registered KV caches (``save_kv_layer`` only sees one layer per call)
        and cached — the pointer table is static after ``register_kv_caches``.
        The kernel/fallback decision is stable across a step's per-layer calls
        because every gate (device, tensor rank, layout, c_ops import) is
        static, so the two paths never mix within one step. An unusable kernel
        is cached as ``False`` to avoid re-probing (e.g. re-attempting the
        import) on every per-layer call.
        """
        if self._store_kernel_unusable:
            return None
        if self._store_kernel_ctx is not None:
            return self._store_kernel_ctx
        if not self._kv_caches or self._num_layers <= 0:
            return None  # not registered yet; resolve on a later call
        layers = [
            (name, kv_cache, self._get_layer_index(name))
            for name, kv_cache in self._kv_caches.items()
        ]
        ctx = self._packed_load_kernel_ctx(layers, attn_metadata)
        if ctx is None:
            self._store_kernel_unusable = True
        else:
            self._store_kernel_ctx = ctx
        if ctx is not None:
            logger.info(
                "Maru packed store: coalesced kernel D2H enabled (%d layers)",
                self._num_layers,
            )
        else:
            logger.info("Maru packed store: kernel unusable; per-layer fallback")
        return ctx

    def _store_packed_slabs(
        self, kernel: tuple, metadata: MaruConnectorMetadata
    ) -> None:
        """Write each chunk completed this step with one kernel D2H transfer.

        Runs once per step, from the last layer's ``save_kv_layer`` call —
        every layer's KV for the step is in the paged buffers by then. Each
        chunk's whole KV_2LTD slab is written by one
        ``multi_layer_kv_transfer(D2H)`` reading the paged GPU cache directly
        into the pinned CXL slab (no staging), queued on a dedicated stream
        that first waits on the compute stream so the step's KV writes are
        visible. One synchronize gates ``batch_store`` so a key is never
        registered before its slab bytes are fully in CXL.
        """
        ops, ptrs, pbs, block_size, head_size, fmt = kernel
        handler = self._handler
        assert handler is not None
        per_layer_bytes = self._chunk_object_bytes()
        if per_layer_bytes is None:
            logger.error("Maru packed store: cannot size slab; skipping step")
            return
        slab_bytes = per_layer_bytes * self._num_layers
        sample = next(iter(self._kv_caches.values()))
        dev, dtype = sample.device, sample.dtype
        ct = self._kv_chunk_tokens

        use_stream = dev.type == "cuda" and torch.cuda.is_available()
        if use_stream:
            if self._store_stream is None or self._store_stream_device != dev:
                self._store_stream = torch.cuda.Stream(device=dev)
                self._store_stream_device = dev
            self._store_stream.wait_stream(torch.cuda.current_stream(dev))
            stream_ctx = torch.cuda.stream(self._store_stream)
        else:
            import contextlib

            stream_ctx = contextlib.nullcontext()

        ready_keys: list[str] = []
        ready_handles: list = []
        # Requests sharing a prefix can yield the same base_key within one
        # step; the single deferred batch_store no longer dedupes them the way
        # the per-request fallback's eager _stored_keys updates did.
        seen_keys: set[str] = set()
        with stream_ctx:
            for req_meta in metadata.requests:
                if not req_meta.is_store:
                    continue
                chunk_keys = _req_chunk_keys(req_meta, ct)
                if not chunk_keys:
                    continue
                if req_meta.num_scheduled_tokens > 0:
                    start_chunk = req_meta.num_computed_tokens // ct
                    end_chunk = (
                        req_meta.num_computed_tokens + req_meta.num_scheduled_tokens
                    ) // ct
                else:
                    start_chunk, end_chunk = 0, len(chunk_keys)
                end_chunk = min(end_chunk, len(chunk_keys))
                if start_chunk >= end_chunk:
                    continue

                needed_tokens = end_chunk * ct
                slot_mapping = self._build_slot_mapping(
                    req_meta.block_ids, needed_tokens
                )
                if slot_mapping.shape[0] < needed_tokens:
                    logger.error(
                        "Maru packed store: block_ids covers %d tokens but chunks "
                        "up to %d need %d for req %s; skipping",
                        slot_mapping.shape[0],
                        end_chunk,
                        needed_tokens,
                        req_meta.req_id,
                    )
                    continue
                slot_dev = slot_mapping.to(dev)

                for ci in range(start_chunk, end_chunk):
                    base_key = chunk_keys[ci]
                    if base_key in self._stored_keys or base_key in seen_keys:
                        continue
                    seen_keys.add(base_key)
                    handle = None
                    try:
                        handle = handler.alloc(slab_bytes)
                        slab_host = torch.frombuffer(
                            handle.buf[:slab_bytes], dtype=dtype
                        ).view(2, self._num_layers, ct, -1)
                        ops.multi_layer_kv_transfer(
                            slab_host,
                            ptrs,
                            slot_dev[ci * ct : (ci + 1) * ct],
                            dev,
                            pbs,
                            ops.TransferDirection.D2H,
                            fmt,
                            block_size=block_size,
                            head_size=head_size,
                        )
                    except Exception as e:
                        logger.error("Maru packed save error: %s: %s", base_key, e)
                        if handle is not None:
                            try:
                                handler.free(handle)
                            except Exception:
                                pass
                        continue
                    ready_keys.append(base_key)
                    ready_handles.append(handle)

        if not ready_keys:
            return
        if use_stream:
            assert self._store_stream is not None
            self._store_stream.synchronize()
        # batch_store takes ownership of every handle (frees duplicates and
        # failures internally); it only raises before consuming any, so the
        # except path must free them all.
        try:
            results = handler.batch_store(ready_keys, ready_handles)
        except Exception as e:
            logger.error("Maru packed batch_store failed: %s", e)
            self._free_handles_best_effort(ready_handles)
            return
        for base_key, ok in zip(ready_keys, results, strict=False):
            if ok:
                self._stored_keys.add(base_key)
            else:
                logger.warning("Maru packed store failed: %s", base_key)
        if self._timing:
            _emit_timing(
                f"packed-store kernel {self._num_layers}L x {len(ready_keys)}c"
            )

    def _store_packed_slabs_write_behind(
        self, kernel: tuple, metadata: MaruConnectorMetadata
    ) -> None:
        """Queue packed stores without blocking the engine's forward path.

        The gather kernel writes one reusable device slab, followed by a
        non-blocking device-to-CXL copy. Reuse is safe because all operations
        share one ordered CUDA stream. Once the final D2H event fires, the
        completion thread registers every key with Maru. Keys are reserved
        before allocation so overlapping requests/steps never issue duplicate
        stores while an earlier copy is still in flight.
        """
        handler = self._handler
        if handler is None:
            logger.error("Maru write-behind store: handler unavailable; skipping step")
            return
        ops, ptrs, pbs, block_size, head_size, fmt = kernel
        per_layer_bytes = self._chunk_object_bytes()
        if per_layer_bytes is None:
            logger.error("Maru write-behind store: cannot size slab; skipping step")
            return
        slab_bytes = per_layer_bytes * self._num_layers
        sample = next(iter(self._kv_caches.values()))
        dev, dtype = sample.device, sample.dtype
        ct = self._kv_chunk_tokens

        # First collect request-to-key ownership on CPU. Shared-prefix keys
        # retain every request ID even though only one physical copy is queued.
        key_entries: dict[str, tuple[torch.Tensor, set[str]]] = {}
        for req_meta in metadata.requests:
            if not req_meta.is_store:
                continue
            chunk_keys = _req_chunk_keys(req_meta, ct)
            if not chunk_keys:
                continue
            if req_meta.num_scheduled_tokens > 0:
                start_chunk = req_meta.num_computed_tokens // ct
                end_chunk = (
                    req_meta.num_computed_tokens + req_meta.num_scheduled_tokens
                ) // ct
            else:
                start_chunk, end_chunk = 0, len(chunk_keys)
            end_chunk = min(end_chunk, len(chunk_keys))
            # Chunked-prefill intermediate steps have not produced a first
            # token yet. Defer them and, on the prompt-completing step, store
            # the whole prefix from the full accumulated block list. This
            # avoids competing with the remaining prefill and coalesces all
            # chunks into one write-behind batch.
            if end_chunk < len(chunk_keys):
                continue
            start_chunk = 0
            if start_chunk >= end_chunk:
                continue

            needed_tokens = end_chunk * ct
            slot_mapping = self._build_slot_mapping(req_meta.block_ids, needed_tokens)
            if slot_mapping.shape[0] < needed_tokens:
                logger.error(
                    "Maru write-behind store: block_ids covers %d tokens but "
                    "chunks up to %d need %d for req %s; skipping",
                    slot_mapping.shape[0],
                    end_chunk,
                    needed_tokens,
                    req_meta.req_id,
                )
                continue
            for ci in range(start_chunk, end_chunk):
                base_key = chunk_keys[ci]
                chunk_slots = slot_mapping[ci * ct : (ci + 1) * ct]
                entry = key_entries.get(base_key)
                if entry is None:
                    key_entries[base_key] = (chunk_slots, {req_meta.req_id})
                else:
                    entry[1].add(req_meta.req_id)

        # Reserve new keys and attach request waiters to both new and already
        # pending keys. Completion of the original job wakes late joiners too.
        to_schedule: list[tuple[str, torch.Tensor]] = []
        with self._store_lock:
            for base_key, (chunk_slots, req_ids) in key_entries.items():
                if base_key in self._stored_keys:
                    continue
                waiters = self._store_key_waiters.setdefault(base_key, set())
                waiters.update(req_ids)
                for req_id in req_ids:
                    self._request_pending_store_keys.setdefault(req_id, set()).add(
                        base_key
                    )
                if base_key not in self._pending_store_keys:
                    self._pending_store_keys.add(base_key)
                    to_schedule.append((base_key, chunk_slots))

        if not to_schedule:
            return

        if self._store_stream is None or self._store_stream_device != dev:
            self._store_stream = torch.cuda.Stream(device=dev)
            self._store_stream_device = dev
        store_stream = self._store_stream
        store_stream.wait_stream(torch.cuda.current_stream(dev))

        ready_keys: list[str] = []
        ready_handles: list[Any] = []
        slot_refs: list[torch.Tensor] = []
        with torch.cuda.stream(store_stream):
            for base_key, chunk_slots in to_schedule:
                handle = None
                try:
                    handle = handler.alloc(slab_bytes)
                    slab_host = torch.frombuffer(
                        handle.buf[:slab_bytes], dtype=dtype
                    ).view(2, self._num_layers, ct, -1)
                    if (
                        self._store_staging is None
                        or self._store_staging.shape != slab_host.shape
                        or self._store_staging.dtype != dtype
                        or self._store_staging.device != dev
                    ):
                        self._store_staging = torch.empty_like(slab_host, device=dev)
                    chunk_slots_dev = chunk_slots.to(dev, non_blocking=True)
                    slot_refs.append(chunk_slots_dev)
                    ops.multi_layer_kv_transfer(
                        self._store_staging,
                        ptrs,
                        chunk_slots_dev,
                        dev,
                        pbs,
                        ops.TransferDirection.D2H,
                        fmt,
                        block_size=block_size,
                        head_size=head_size,
                    )
                    # Maru cudaHostRegister's the CXL mapping, so this uses the
                    # copy engine rather than keeping SMs busy for the D2H.
                    slab_host.copy_(self._store_staging, non_blocking=True)
                except Exception as e:
                    logger.error("Maru write-behind prepare error: %s: %s", base_key, e)
                    if handle is not None:
                        try:
                            handler.free(handle)
                        except Exception:
                            pass
                    self._complete_write_behind_keys([base_key], [False])
                    continue
                ready_keys.append(base_key)
                ready_handles.append(handle)

            if not ready_keys:
                return
            event = torch.cuda.Event()
            event.record(store_stream)

        if self._store_executor is None:
            self._store_executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="maru-write-behind"
            )
        try:
            self._store_executor.submit(
                self._finish_write_behind_store,
                event,
                ready_keys,
                ready_handles,
                slot_refs,
            )
        except Exception as e:
            # Executor creation/submission failures are exceptional. Drain the
            # queued copies before freeing their destination handles.
            logger.error("Maru write-behind submit failed: %s", e)
            event.synchronize()
            self._free_handles_best_effort(ready_handles)
            self._complete_write_behind_keys(ready_keys, [False] * len(ready_keys))

    def _finish_write_behind_store(
        self,
        event: torch.cuda.Event,
        keys: list[str],
        handles: list[Any],
        refs: list[Any],
    ) -> None:
        """Wait for D2H and register a write-behind batch off-thread."""
        handler = self._handler
        if handler is None:
            logger.error("Maru write-behind completion: handler unavailable")
            self._complete_write_behind_keys(keys, [False] * len(keys))
            return
        try:
            event.synchronize()
        except Exception as e:
            logger.error("Maru write-behind D2H failed: %s", e)
            self._free_handles_best_effort(handles)
            self._complete_write_behind_keys(keys, [False] * len(keys))
            return
        refs.clear()  # safe to release stream inputs after the event

        try:
            results = list(handler.batch_store(keys, handles))
        except Exception as e:
            logger.error("Maru write-behind batch_store failed: %s", e)
            self._free_handles_best_effort(handles)
            self._complete_write_behind_keys(keys, [False] * len(keys))
            return

        # Treat a malformed short response as failure rather than leaving keys
        # and request-owned blocks pending forever.
        if len(results) < len(keys):
            results.extend([False] * (len(keys) - len(results)))
        self._complete_write_behind_keys(keys, results[: len(keys)])
        if self._timing:
            _emit_timing(
                f"write-behind packed-store {self._num_layers}L x {len(keys)}c"
            )

    def _complete_write_behind_keys(self, keys: list[str], results: list[bool]) -> None:
        """Publish key outcomes and release all request waiters atomically."""
        with self._store_lock:
            for base_key, ok in zip(keys, results, strict=True):
                if ok:
                    self._stored_keys.add(base_key)
                else:
                    logger.warning("Maru write-behind store failed: %s", base_key)
                self._pending_store_keys.discard(base_key)
                for req_id in self._store_key_waiters.pop(base_key, set()):
                    pending = self._request_pending_store_keys.get(req_id)
                    if pending is None:
                        continue
                    pending.discard(base_key)
                    if not pending:
                        del self._request_pending_store_keys[req_id]

    def _discard_pending_slab(self, base_key: str) -> None:
        """Drop a half-filled slab entry after an error and free its handle.

        The entry always leaves ``_pending_slabs`` so a later layer of the
        same chunk cannot resume writing into a discarded slab. A handle that
        cannot be freed yet (no live handler) is parked in
        ``_orphan_slab_handles`` for the next sweep instead of being dropped
        on the floor.
        """
        entry = self._pending_slabs.pop(base_key, None)
        if entry is None or entry[0] is None:
            return
        self._free_slab_handle(entry[0])

    def _free_slab_handle(self, handle: Any) -> None:
        """Free one slab handle, parking it for retry if no handler is up."""
        handler = self._handler
        if handler is None:
            self._orphan_slab_handles.append(handle)
            return
        try:
            handler.free(handle)
        except Exception:
            pass

    def _reclaim_stale_pending_slabs(self) -> None:
        """Free fallback slabs left incomplete by a previous step.

        ``_pending_slabs`` entries are strictly intra-step: every layer of a
        chunk writes its plane within one step, and complete entries are
        registered and removed immediately. Any entry still present at a step
        boundary is unfinishable — a mid-chunk error discarded the original
        slab and a later layer re-created the entry with a ``written`` set
        that can never reach ``_num_layers``, or part of the step's
        ``save_kv_layer`` calls were skipped while the handler was
        unavailable. Left alone, each such entry pins a slab-sized CXL
        allocation forever.

        Handles orphaned by an earlier sweep that ran without a handler are
        retried here first.
        """
        if self._orphan_slab_handles and self._handler is not None:
            orphans = self._orphan_slab_handles
            self._orphan_slab_handles = []
            for handle in orphans:
                self._free_slab_handle(handle)
        if not self._pending_slabs:
            return
        stale = list(self._pending_slabs)
        for base_key in stale:
            self._discard_pending_slab(base_key)
        logger.warning(
            "Maru packed store: reclaimed %d stale slab(s) at step boundary",
            len(stale),
        )

    def _free_handles_best_effort(self, handles: list) -> None:
        """Free alloc handles after a ``batch_store`` call raised.

        ``batch_store`` owns the handles once it runs (it frees duplicates and
        failures internally); every exception it lets escape is raised before
        any handle is consumed (connection/closing/length checks), so freeing
        them here cannot double-free. Without this, a single RPC hiccup during
        a store step would leak every not-yet-registered CXL page.
        """
        handler = self._handler
        if handler is None:
            return
        for handle in handles:
            try:
                handler.free(handle)
            except Exception:
                pass

    def _write_done_marker(self, base_key: str) -> None:
        """Write a chunk's ``_DONE`` marker once all its layers are stored.

        The scheduler's exists-check keys on ``_DONE`` to know a chunk is fully
        populated across layers. (TODO: alloc(1) still consumes a full CXL page;
        a metadata-only store in MaruHandler would remove this waste.)
        """
        handler = self._handler
        if handler is None:
            return
        done_key = f"{base_key}_DONE"
        done_h = None
        try:
            done_h = handler.alloc(1)
            handler.store(done_key, handle=done_h)
            done_h = None  # ownership transferred to store
        except Exception as e:
            logger.warning("Failed to store _DONE marker for %s: %s", base_key, e)
        finally:
            if done_h is not None:
                try:
                    handler.free(done_h)
                except Exception:
                    pass

    def shutdown(self):
        # An exceptional engine exit can skip get_finished() after the final
        # save hooks. Launch those recorded batches while CUDA and the handler
        # are still alive, then drain them below.
        queued = self._queued_store_batches
        self._queued_store_batches = []
        for kernel, metadata in queued:
            self._store_packed_slabs_write_behind(kernel, metadata)
        # Write-behind jobs own CXL handles and use the handler. Drain them
        # before closing either the CUDA stream or MaruHandler.
        if self._store_executor is not None:
            self._store_executor.shutdown(wait=True)
            self._store_executor = None
        if self._store_stream is not None:
            try:
                self._store_stream.synchronize()
            except Exception as e:
                logger.error("Error synchronizing Maru store stream: %s", e)
            self._store_stream = None
            self._store_staging = None
        # Drain the loader thread before touching the handler or streams:
        # an in-flight job may still be retrieving/queueing copies.
        if self._deferred_executor is not None:
            self._deferred_executor.shutdown(wait=True)
            self._deferred_executor = None
        if self._deferred_stream is not None:
            try:
                self._deferred_stream.synchronize()
            except Exception as e:
                logger.error("Error synchronizing Maru deferred-load stream: %s", e)
            self._deferred_stream = None
        if self._load_stream is not None:
            try:
                self._load_stream.synchronize()
            except Exception as e:
                logger.error("Error synchronizing Maru load stream: %s", e)
            self._load_stream = None
            self._layer_load_events.clear()
            self._active_load_refs.clear()
        with self._deferred_lock:
            self._deferred_layerwise_loads.clear()
            self._deferred_events.clear()
            self._deferred_refs.clear()
            self._deferred_done.clear()
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
        attn_metadata: AttentionMetadata | None,
        layer_name: str,
        num_chunks: int = 1,
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
            if num_chunks == 1:
                src = src_kv_data.reshape(2, slot_mapping.shape[0], -1)
            else:
                tokens_per_chunk = slot_mapping.shape[0] // num_chunks
                src = (
                    src_kv_data.reshape(num_chunks, 2, tokens_per_chunk, -1)
                    .permute(1, 0, 2, 3)
                    .reshape(2, slot_mapping.shape[0], -1)
                )
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

    def _chunk_object_bytes(self) -> int | None:
        """Return the byte size of one ``(chunk x layer)`` KV object, or None.

        A stored object holds ``kv_chunk_tokens`` tokens of one layer's KV. Its
        size is ``kv_chunk_tokens x per_token_per_layer_bytes``, where the
        per-token footprint is read from a registered KV cache tensor:
        ``per_token_bytes = layer.numel() * element_size / token_slots`` and
        ``token_slots = num_blocks x block_size``.

        The token-slot dimensions differ per attention layout (mirrors
        ``_extract_kv_from_layer``); the block/page dimension is identified by
        matching ``block_size``:

        - Flash (default): ``[2, num_blocks, block_size, ...]``  -> slots = d1*d2
        - Triton: ``[num_blocks, num_kv_heads, block_size, head_dim]`` -> d0*d2
        - MLA: ``[num_blocks, block_size, ...]``                 -> slots = d0*d1

        Returns None for an unrecognized layout so the caller keeps the
        configured/default page size rather than guessing wrong.
        """
        if not self._kv_caches:
            return None
        layer = next(iter(self._kv_caches.values()))
        shape = tuple(layer.shape)
        bs = self._block_size
        token_slots: int | None = None
        if len(shape) >= 3 and shape[0] == 2 and shape[2] == bs:
            token_slots = shape[1] * shape[2]  # Flash
        elif len(shape) == 4 and shape[2] == bs:
            token_slots = shape[0] * shape[2]  # Triton
        elif len(shape) >= 2 and shape[1] == bs:
            token_slots = shape[0] * shape[1]  # MLA
        if not token_slots:
            logger.warning(
                "MaruWorkerConnector: unrecognized KV layout %s (block_size=%d); "
                "keeping default CXL page size",
                shape,
                bs,
            )
            return None
        total_bytes = int(layer.numel()) * int(layer.element_size())
        per_token_bytes = total_bytes // token_slots
        return int(per_token_bytes * self._kv_chunk_tokens)
