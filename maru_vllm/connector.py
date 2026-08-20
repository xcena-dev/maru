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
import os
import re
import threading
import time
from collections.abc import Iterable
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

from maru_handler import StageResult
from maru_vllm.kv_layout import (
    KVLayout,
    _canonical_paged_view,
    _detect_kv_layout,
    _layout_fits,
    _vllm_kv_cache_layout,
)
from maru_vllm.staging_prefetch import (
    DeadlineStagePolicy,
    FifoStagePolicy,
    HymCacheObject,
    HymCacheRollingPipeline,
    StagePlan,
    StageTicket,
    build_hymcache_objects,
)

if TYPE_CHECKING:
    from vllm.config import VllmConfig
    from vllm.forward_context import ForwardContext
    from vllm.v1.core.kv_cache_manager import KVCacheBlocks
    from vllm.v1.kv_cache_interface import KVCacheConfig
    from vllm.v1.request import Request

logger = init_logger(__name__)

# Default number of tokens per chunk for KV cache storage
DEFAULT_KV_CHUNK_TOKENS = 256

# Knobs renamed to name the axis the deployer actually chooses, mapped to the
# name each one replaced. The former names stay accepted for one release so
# existing recipes and launch scripts keep working; _get_knob warns when one
# is used. "async"/"deferred" previously named two different things — the
# retired maru_enable_async_loading held the "async" name while
# maru_enable_deferred_loading was the mechanism vLLM itself calls async
# loading (the request parks in WAITING_FOR_REMOTE_KVS).
_RENAMED_KNOBS: dict[str, str] = {
    "maru_async_load": "maru_enable_deferred_loading",
    "maru_async_store": "maru_enable_write_behind",
    "maru_overlap_load_with_compute": "maru_enable_layerwise_overlap",
}

# Parked-request transfers all share one stream so each runs at full CXL
# bandwidth. Splitting them across streams makes the per-layer transfer time
# scale with the number of loading requests (measured: about 2 ms alone,
# 17 ms with eight), which overruns the roughly 3 ms of per-layer prefill
# compute and stalls the batch at every layer. One stream also means a later
# request's first layer only lands once the earlier one has finished, so
# requests take their turn without any extra admission mechanism.
_LAYERWISE_STREAM_COUNT = 1

# A parked request is released once this many of its layers have landed. The
# rest arrive while its own attention runs. Waiting for more only delays the
# start: the transfer already outruns compute at full bandwidth.
_LAYERWISE_RELEASE_AFTER_LAYERS = 1

_cuda_runtime: Any = None
_cuda_memcpy2d_async: Any = None
_cuda_memcpy2d_unavailable = False


# ============================================================================
# Utilities
# ============================================================================


def _get_knob(extra_config: dict[str, Any], key: str, default: Any = False) -> Any:
    """Read a connector knob, accepting the deprecated name it replaced.

    The current name wins whenever it is present, so a config carrying both
    behaves the same as one carrying only the current name.

    Args:
        extra_config: vLLM kv_connector_extra_config dict.
        key: Current knob name; see ``_RENAMED_KNOBS`` for the ones that
            have a deprecated alias.
        default: Value returned when neither name is present.

    Returns:
        The configured value, or ``default``.
    """
    if key in extra_config:
        return extra_config[key]
    legacy = _RENAMED_KNOBS.get(key)
    if legacy is not None and legacy in extra_config:
        logger.warning(
            "Maru: %s is deprecated and will be removed; use %s instead",
            legacy,
            key,
        )
        return extra_config[legacy]
    return default


def _emit_timing(msg: str) -> None:
    """Write a diagnostic timing line to stderr.

    The connector's vLLM logger (``maru_vllm.connector``) sits outside the
    ``vllm.*`` handler namespace, so its records are never captured in the
    engine logs. Timing diagnostics therefore go straight to stderr, which
    the process log does capture.
    """
    import sys

    print(f"Maru timing: {msg}", file=sys.stderr, flush=True)


def _drain_events(events: Iterable[Any]) -> None:
    """Wait out queued copies whose destination blocks are being reclaimed.

    Best effort: a device already torn down raises here, and there is nothing
    left to protect in that case.
    """
    for event in events:
        try:
            event.synchronize()
        except Exception as e:
            logger.warning("Maru: could not drain a queued layer copy: %s", e)


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


_HINT_PLAN_PREFIX = "maru-hint:"


def _hint_plan_id(session_id: str) -> str:
    """Stage-plan id for a session hint (namespaced apart from vLLM req ids)."""
    return f"{_HINT_PLAN_PREFIX}{session_id}"


def _request_session_params(request: Request) -> tuple[str | None, str | None]:
    """Extract (session_id, imminent_session) from a request's transfer params.

    Both ride on the OpenAI ``extra_body.kv_transfer_params`` dict, which
    vLLM carries verbatim on the Request object. Returns ``None`` for absent
    or malformed values — session hints are strictly opt-in per request.
    """
    params = getattr(request, "kv_transfer_params", None)
    if not isinstance(params, dict):
        return None, None
    session = params.get("maru_session_id")
    imminent = params.get("maru_imminent_session")
    return (
        str(session) if session else None,
        str(imminent) if imminent else None,
    )


def _layerwise_key_groups(
    chunk_keys: list[str], layer_indices: list[int]
) -> list[list[tuple[str, None, None]]]:
    """Hint groups for layerwise storage, one group per layer in run order.

    Each group holds that layer's per-chunk object keys whole (offset/length
    ``None``), so ``prefetch_grouped`` fills layer 0's objects first, then
    layer 1's — the order the resumed forward will consume them.
    """
    return [
        [(f"{ck}_L{idx}", None, None) for ck in chunk_keys] for idx in layer_indices
    ]


def _packed_layer_span_groups(
    chunk_keys: list[str],
    num_layers: int,
    plane_bytes: int,
    group_layers: int,
) -> list[list[tuple[str, int, int]]]:
    """Hint groups for packed storage: each layer's planes inside every object.

    A packed object is ``[K, layer 0..L-1][V, layer 0..L-1]`` with one
    ``plane_bytes`` plane per (kv, layer). Layer ``l``'s bytes therefore sit at
    offsets ``l*plane`` and ``(L+l)*plane`` of every chunk object — scattered,
    one 2*plane sliver per object. ``group_layers`` widens each sliver to
    cover that many adjacent layers, trading finer fill order for fewer,
    larger device commands (adjacent layers' planes are contiguous).
    """
    groups: list[list[tuple[str, int, int]]] = []
    for start in range(0, num_layers, group_layers):
        span = min(group_layers, num_layers - start) * plane_bytes
        group: list[tuple[str, int, int]] = []
        for chunk_key in chunk_keys:
            group.append((chunk_key, start * plane_bytes, span))
            group.append((chunk_key, (num_layers + start) * plane_bytes, span))
        groups.append(group)
    return groups


def _slab_nbytes(view: Any) -> int:
    """Byte size of one packed object's host view."""
    return view.nbytes if hasattr(view, "nbytes") else len(view)


@dataclass
class PackedObjectRecord:
    """One object's measured CXL->GPU copy time."""

    req_id: str
    index: int
    total: int
    nbytes: int
    cxl_gpu_ms: float


class PackedObjectTimer:
    """Per-object CUDA timing for the demand-load packed path.

    Events are recorded around each object's enqueue on the load stream and
    read only after the caller's single end-of-batch synchronize. There is
    deliberately no per-object synchronize: draining the stream between
    objects would serialize the very pipelining this path is measured for,
    so the instrumented run would no longer measure the uninstrumented one.

    ``event_factory`` exists so tests can drive the bookkeeping without CUDA.
    """

    def __init__(self, event_factory: Any = None) -> None:
        self._event_factory = event_factory
        self._pending: list[tuple[str, int, int, int, Any, Any]] = []
        self._expected: dict[str, int] = {}

    def _new_event(self) -> Any:
        if self._event_factory is not None:
            return self._event_factory()
        return torch.cuda.Event(enable_timing=True)

    def expect(self, req_id: str, count: int) -> None:
        """Declare how many objects a request should produce."""
        self._expected[req_id] = count

    def begin(self, req_id: str, index: int, total: int, nbytes: int) -> Any:
        """Record the start event; returns a handle for :meth:`end`."""
        start_event = self._new_event()
        start_event.record()
        handle = (req_id, index, total, nbytes, start_event)
        return handle

    def end(self, handle: Any) -> None:
        """Record the end event for a handle from :meth:`begin`."""
        req_id, index, total, nbytes, start_event = handle
        end_event = self._new_event()
        end_event.record()
        self._pending.append((req_id, index, total, nbytes, start_event, end_event))

    def collect(self) -> tuple[list[PackedObjectRecord], list[str]]:
        """Read elapsed times after the caller's single stream synchronize.

        Returns the records plus a list of problem descriptions (a duplicate
        (request, object) pair, or a request short of its declared count).
        """
        records: list[PackedObjectRecord] = []
        problems: list[str] = []
        seen: set[tuple[str, int]] = set()
        for req_id, index, total, nbytes, start_event, end_event in self._pending:
            key = (req_id, index)
            if key in seen:
                problems.append(f"duplicate object record req={req_id} idx={index}")
                continue
            seen.add(key)
            records.append(
                PackedObjectRecord(
                    req_id=req_id,
                    index=index,
                    total=total,
                    nbytes=nbytes,
                    cxl_gpu_ms=start_event.elapsed_time(end_event),
                )
            )
        for req_id, count in self._expected.items():
            measured = sum(1 for r in records if r.req_id == req_id)
            if measured != count:
                problems.append(
                    f"incomplete object records req={req_id} "
                    f"measured={measured} expected={count}"
                )
        return records, problems


def _format_kv_object_timing(
    *,
    req_id: str,
    index: int,
    total: int,
    nbytes: int,
    cxl_gpu_ms: float,
    prefetched: bool,
) -> str:
    """Format one per-object GPU-read record.

    Both the demand-load path (no Stage 1) and the HyMCache window path emit
    this exact shape so a single parser reads either run. ``prefetch=1`` means
    ``memory_prefetch_sync`` ran for that object before the CXL->GPU copy.
    """
    return (
        f"kv-object idx={index}/{total} bytes={nbytes} "
        f"cxl_gpu_ms={cxl_gpu_ms:.2f} prefetch={int(prefetched)} (req {req_id})"
    )


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
    # Requests that finished or were aborted between the previous and the
    # current step. A request aborted while parked keeps its blocks only until
    # the connector reports its load complete, so the worker must drain the
    # copies it queued for that request before that report goes out.
    finished_req_ids: set[str] = field(default_factory=set)
    # Deferred packed loads whose metadata RPC completed between steps. The
    # resumed forward consumes their CXL views layer-by-layer, so layer k+1 H2D
    # can overlap layer k compute without changing the packed storage format.
    layerwise_load_req_ids: set[str] = field(default_factory=set)
    # --- smart-prefetch / HyMCache relay (ported onto #70) ---
    arrival_hint_keys: list[str] = field(default_factory=list)
    stage_plans: list[StagePlan] = field(default_factory=list)
    stage_aliases: dict[str, str] = field(default_factory=dict)
    stage_release_ids: list[str] = field(default_factory=list)


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

    Configuration via kv_connector_extra_config.

    Deployment wiring:
        maru_server_url: str    - MaruServer address (default: tcp://localhost:5555)
        maru_pool_size: str|int - CXL pool size (default: 1G, supports '4G', '500M')
        maru_instance_id: str   - Unique instance ID (default: auto-generated)
        maru_chunk_size: str|int - Maru page size for CXL pages (default: 4M)
        maru_eager_map: bool    - Pre-map shared regions on connect (default: true)
        maru_kv_chunk_tokens: int - Tokens per KV chunk (default: 256)

    Performance axes — the knobs a deployer actually chooses between:
        maru_async_load: bool - Load matched KV between scheduler steps: the
            request is parked in WAITING_FOR_REMOTE_KVS while a background
            loader thread performs the whole load (Maru retrieve RPC +
            CXL->GPU transfer on a dedicated stream), so neither the RPC wait
            nor the copy ever blocks the engine's forward passes — the
            in-process analog of the MP server's separate-process retrieve.
            This is what vLLM itself calls an async load
            (get_num_new_matched_tokens returns async_load=True).
            (default: false; was maru_enable_deferred_loading)
        maru_async_store: bool - Gather completed prompt chunks into a
            reusable GPU staging slab after the forward, then copy them to CXL
            with asynchronous D2H DMA. Metadata registration finishes on a
            background thread; finished requests retain their GPU blocks until
            get_finished() reports the store complete
            (default: false; was maru_enable_write_behind)
        maru_overlap_load_with_compute: bool - With maru_async_load and packed
            storage, queue a parked request's per-layer transfers while it
            waits and release it once its first layer has landed, so the
            remaining layers arrive during its own attention. The transfers
            share one stream, which keeps each at full bandwidth and makes a
            later request's first layer land only after the earlier one has
            finished. Applies at any concurrency. Requires
            maru_async_load=true and maru_use_layerwise=false
            (default: false; was maru_enable_layerwise_overlap)

    Storage format — how a request's KV is grouped into CXL objects:
        maru_use_layerwise: bool - False (default) is chunkwise: one CXL
            object per chunk holding every layer, keyed by the chunk key,
            registered only once all layers are written (the key is its own
            completion marker). True is layerwise: one object per
            (chunk, layer), keyed <chunk>_L<idx>, with a separate _DONE
            marker. Chunkwise resolves one key per chunk instead of
            chunks x layers — 59 vs 1,888 keys for a 64k prompt on 32 layers
            — which is why it is the default; the same ratio applies to
            retrieve metadata RPC volume. Chunkwise transfers use LMCache's
            multi_layer_kv_transfer kernel directly on the pinned CXL slab
            (no staging) when available — load scatters a whole slab into the
            paged cache per chunk, store gathers one D2H per chunk — falling
            back to per-layer copies otherwise. See design note P6.

    Diagnostics and fallback guards — leave these alone in normal operation:
        maru_load_admission_window: int - With maru_async_load, cap how many
            requests' packed loads may be enqueued on the deferred stream but
            not yet complete. The loader thread blocks before each GPU
            enqueue until fewer than this many loads are outstanding,
            bounding same-CUDA-context interference with model steps.
            Default: 0 (submit all loads immediately). Set a positive value
            to enable the cap as a fallback safety guard.
        maru_log_timing: bool - Emit per-request timing diagnostics to stderr
            (default: false)

    The former names listed above are still accepted and log a deprecation
    warning; see ``_RENAMED_KNOBS``.
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
            # Read here, where vllm_config is in hand: get_current_vllm_config()
            # asserts outside a config context. get_num_kv_heads is per rank, so
            # it matches the paged tensor; MLA reports 1 either way, so skip it.
            num_kv_heads: int | None = None
            head_size: int | None = None
            try:
                model_config = vllm_config.model_config
                if model_config is not None and not model_config.use_mla:
                    num_kv_heads = model_config.get_num_kv_heads(
                        vllm_config.parallel_config
                    )
                    head_size = model_config.get_head_size()
            except Exception as e:
                logger.warning(
                    "Maru: cannot read KV geometry from vllm_config (%s: %s); "
                    "layout detection will cross-check block_size only",
                    type(e).__name__,
                    e,
                )
            self._worker = MaruWorkerConnector(
                block_size=self._block_size,
                kv_chunk_tokens=self._kv_chunk_tokens,
                extra_config=extra,
                num_kv_heads=num_kv_heads,
                head_size=head_size,
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

    def on_new_request(self, request: Request) -> None:
        """Queue arrival-hint prefetch keys when a request enters the queue.

        Called by the vLLM scheduler right after the request is enqueued into
        the waiting queue -- typically well before it is scheduled -- so the
        admission wait can serve as the device's SSD->DRAM fill window. No-op
        unless ``MARU_ARRIVAL_HINT=1`` (handled scheduler-side).

        Args:
            request: The newly arrived vLLM request.
        """
        if self._scheduler is not None:
            self._scheduler.on_new_request(request)


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

        # Asynchronous load (between-step): matched KV is transferred while the
        # request waits in WAITING_FOR_REMOTE_KVS instead of stalling its
        # first forward pass. This is the mechanism vLLM itself calls async
        # loading — get_num_new_matched_tokens returns async_load=True.
        self._deferred_loading = bool(_get_knob(extra_config, "maru_async_load"))
        self._write_behind = bool(_get_knob(extra_config, "maru_async_store"))
        self._use_layerwise = bool(extra_config.get("maru_use_layerwise", False))
        overlap_requested = bool(
            _get_knob(extra_config, "maru_overlap_load_with_compute")
        )
        # Overlap works in both storage layouts: packed objects go through the
        # pitched per-layer gather, per-(chunk,layer) objects through the
        # layerwise-storage loader. Only the asynchronous load is a
        # prerequisite — the release gate lives on the parked request.
        self._layerwise_overlap = bool(overlap_requested and self._deferred_loading)
        if overlap_requested and not self._deferred_loading:
            logger.warning(
                "Maru layerwise overlap requires maru_async_load=true; disabling it"
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
        # Smart-prefetch arrival-hint (MARU_ARRIVAL_HINT=1): chunk base keys of
        # requests that entered the waiting queue since the last step, drained
        # into the next connector metadata for the worker to fire (the worker
        # owns the region mappings, so firing happens there).
        self._arrival_hint_enabled = os.environ.get("MARU_ARRIVAL_HINT", "0") == "1"
        # Completion-returning staging pipeline. The scheduler is deliberately
        # only a bounded FIFO brain; the worker owns the blocking device call.
        self._stage_enabled = os.environ.get("MARU_STAGE_PIPELINE", "0") == "1"
        self._hymcache_window_bytes = max(
            0, int(os.environ.get("MARU_HYMCACHE_WINDOW_BYTES", "0") or 0)
        )
        # Split admission into hint-then-wait so window depth reaches the
        # device. Off by default: the blocking-only path is what every
        # campaign through 2026-08-19 measured.
        self._hymcache_async_issue = (
            os.environ.get("MARU_HYMCACHE_ASYNC_ISSUE", "0") or "0"
        ).strip().lower() not in ("", "0", "false", "no")
        if self._hymcache_window_bytes > 0:
            if self._arrival_hint_enabled or self._stage_enabled:
                logger.warning(
                    "MARU_HYMCACHE_WINDOW_BYTES supersedes arrival/session/request "
                    "staging; disabling those non-HyMCache hint paths"
                )
            self._arrival_hint_enabled = False
            self._stage_enabled = False
            if self._layerwise_overlap:
                # The two consume the same bytes in opposite orders and cannot
                # both be in effect. The window walks whole packed objects
                # (every layer of chunk c, then chunk c+1) so an object can be
                # released and its slot refilled; layerwise overlap walks
                # layers (layer k of every chunk, then k+1) so attention layer
                # k can start while layer k+1 is still transferring. Under the
                # window order every layer completes at the same moment, so
                # there is no per-layer event to overlap; under the layer order
                # layer 0 already touches every object, so no window bounds
                # anything. The window wins: it is the mode the caller asked
                # for by setting a byte budget.
                logger.warning(
                    "MARU_HYMCACHE_WINDOW_BYTES supersedes "
                    "maru_overlap_load_with_compute (object-order vs "
                    "layer-order consumption); disabling the overlap"
                )
            self._layerwise_overlap = False
        # Admission policy (MARU_STAGE_POLICY):
        #   fifo     — oldest first under request/byte budgets (baseline).
        #   deadline — the session-staging coordinator's admission layer:
        #              earliest arrival estimate first, late plans expire
        #              instead of staging, re-enqueue replaces a queued plan.
        #              Deadline defaults to enqueue + MARU_STAGE_DEADLINE_MS;
        #              a plan older than deadline + MARU_STAGE_GRACE_MS is
        #              dropped (its target already arrived — demand path owns
        #              the data, a late fill only burns fill bandwidth).
        self._stage_policy: FifoStagePolicy | DeadlineStagePolicy | None = None
        self._stage_policy_kind = (
            os.environ.get("MARU_STAGE_POLICY", "fifo").strip().lower() or "fifo"
        )
        if self._stage_policy_kind not in ("fifo", "deadline"):
            logger.warning(
                "Unknown MARU_STAGE_POLICY=%r; falling back to 'fifo'",
                self._stage_policy_kind,
            )
            self._stage_policy_kind = "fifo"
        self._stage_expired_seen = 0
        if self._stage_enabled:
            max_requests = max(
                1, int(os.environ.get("MARU_STAGE_MAX_REQUESTS", "1") or 1)
            )
            max_bytes = max(
                0,
                int(os.environ.get("MARU_STAGE_MAX_BYTES", str(8 * 1024**3)) or 0),
            )
            estimated_bytes_per_key = max(
                1,
                int(
                    os.environ.get("MARU_STAGE_EST_BYTES_PER_KEY", str(32 * 1024**2))
                    or 1
                ),
            )
            if self._stage_policy_kind == "deadline":
                self._stage_policy = DeadlineStagePolicy(
                    max_requests=max_requests,
                    max_bytes=max_bytes,
                    estimated_bytes_per_key=estimated_bytes_per_key,
                    deadline_s=max(
                        0.001,
                        float(os.environ.get("MARU_STAGE_DEADLINE_MS", "500") or 500)
                        / 1000.0,
                    ),
                    grace_s=max(
                        0.0,
                        float(os.environ.get("MARU_STAGE_GRACE_MS", "200") or 200)
                        / 1000.0,
                    ),
                )
            else:
                self._stage_policy = FifoStagePolicy(
                    max_requests=max_requests,
                    max_bytes=max_bytes,
                    estimated_bytes_per_key=estimated_bytes_per_key,
                )
        if self._stage_enabled and self._arrival_hint_enabled:
            logger.warning(
                "MARU_STAGE_PIPELINE=1 supersedes MARU_ARRIVAL_HINT=1; "
                "disabling the fire-and-forget arrival hint"
            )
            self._arrival_hint_enabled = False
        # (req_id, chunk keys) awaiting release, oldest arrival first.
        self._pending_arrival_hints: list[tuple[str, list[str]]] = []
        # Released but not yet consumed by a load. MARU_ARRIVAL_HINT_DEPTH caps
        # how many requests may be outstanding at once: the device fills hinted
        # ranges concurrently, so hinting every queued request at once splits
        # fill bandwidth across all of them and the one about to be read is no
        # likelier to be resident than the last. A small window keeps the fill
        # ordered by how soon the bytes are needed. 0 disables the cap.
        self._arrival_hint_depth = max(
            0, int(os.environ.get("MARU_ARRIVAL_HINT_DEPTH", "0") or 0)
        )
        self._arrival_hint_inflight: set[str] = set()
        # Arrival-hint fires the packed chunk base key, which is a real data key
        # only in packed mode. In layerwise mode the data lives at
        # f"{base}_L{idx}" and the base name has no object, so every hint would
        # miss — disable it there (with a visible warning) rather than burn one
        # guaranteed-miss lookup RPC per request arrival.
        if self._arrival_hint_enabled and self._use_layerwise:
            logger.warning(
                "Maru arrival-hint (MARU_ARRIVAL_HINT=1) is unsupported with "
                "layerwise storage (maru_use_layerwise=True); disabling it. The "
                "packed chunk key is not a data key in layerwise mode, so every "
                "hint would miss."
            )
            self._arrival_hint_enabled = False
        if self._stage_enabled and self._use_layerwise:
            logger.warning(
                "Maru stage pipeline is unsupported with layerwise storage; "
                "disabling it because packed chunk base keys are not data keys"
            )
            self._stage_enabled = False
            self._stage_policy = None
        if self._arrival_hint_enabled:
            logger.info(
                "Maru arrival-hint prefetch enabled (MARU_ARRIVAL_HINT=1, depth=%s)",
                self._arrival_hint_depth or "unlimited",
            )
        if self._stage_enabled:
            logger.info(
                "Maru SSD-to-DRAM stage pipeline enabled "
                "(policy=%s, requests=%s, bytes=%s, estimate/key=%s)",
                self._stage_policy_kind,
                os.environ.get("MARU_STAGE_MAX_REQUESTS", "1"),
                os.environ.get("MARU_STAGE_MAX_BYTES", str(8 * 1024**3)),
                os.environ.get("MARU_STAGE_EST_BYTES_PER_KEY", str(32 * 1024**2)),
            )
        # What fires a StagePlan (MARU_STAGE_TRIGGER):
        #   match    — at the arriving request's verified match (the staging
        #              P0 behavior; stage cost lands inside this request's
        #              TTFT). Default.
        #   turn_end — at request completion, re-stage the session's own
        #              confirmed prefix for its next turn.
        #   imminent — when a request carries a gateway hint that another
        #              session's turn is about to arrive, stage that
        #              session's confirmed prefix from the registry.
        # turn_end/imminent identify sessions via
        # ``kv_transfer_params.maru_session_id`` (OpenAI extra_body) and join
        # the arriving request to its hint ticket through a req-id alias
        # relayed in the connector metadata.
        self._stage_trigger = os.environ.get("MARU_STAGE_TRIGGER", "match")
        if self._stage_trigger not in ("match", "turn_end", "imminent"):
            logger.warning(
                "Unknown MARU_STAGE_TRIGGER=%r; falling back to 'match'",
                self._stage_trigger,
            )
            self._stage_trigger = "match"
        if self._stage_enabled and self._stage_trigger != "match":
            logger.info(
                "Maru stage trigger: %s (session-hint mode)", self._stage_trigger
            )
        if self._timing:
            # The connector logger never reaches the engine log (see
            # _emit_timing); staging state must be visible there to audit runs.
            _emit_timing(
                f"stage init: enabled={self._stage_enabled} "
                f"trigger={self._stage_trigger} policy={self._stage_policy_kind}"
            )
        # session_id -> confirmed prefix chunk keys, recorded at request
        # completion (the just-stored keys ARE the prefix the session's next
        # turn will re-read; no future request content is used).
        self._session_keys: dict[str, tuple[str, ...]] = {}
        # Arriving req_id -> hint plan id, until relayed to the worker.
        self._pending_stage_aliases: dict[str, str] = {}
        # Relayed aliases, kept until their request finishes: a hint plan
        # admitted in the same step its target is scheduled re-enters the
        # policy's inflight window after the consumed-set was applied, so its
        # slot (and worker ticket, on failure paths) is reclaimed at finish.
        self._relayed_stage_aliases: dict[str, str] = {}
        # Hint plan ids whose worker tickets must be dropped (their target
        # request finished or was preempted without consuming the ticket).
        self._pending_stage_releases: list[str] = []

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
        return self._count_matched_chunk_keys(keys)

    def get_num_new_matched_tokens(
        self,
        request: Request,
        num_computed_tokens: int,
    ) -> tuple[int | None, bool]:
        # Session-hint bookkeeping must run before any early return: a
        # request's alias (and the imminent-session stage it carries) matter
        # even when this request itself has nothing to load.
        if self._stage_enabled and self._stage_trigger != "match":
            self._process_session_hints(request)
        token_ids = list(request.prompt_token_ids or [])
        if len(token_ids) < self._kv_chunk_tokens:
            return 0, False

        _t0 = time.monotonic()
        chunk_keys = _chunk_keys(token_ids, self._kv_chunk_tokens)
        num_matched_chunks = self._count_matched_chunk_keys(chunk_keys)
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
        if self._stage_enabled and self._stage_trigger == "match":
            assert self._stage_policy is not None
            matched_keys = chunk_keys[:num_matched_chunks]
            self._stage_policy.enqueue(request.request_id, matched_keys)
            logger.debug(
                "Maru stage: queued %d verified chunk keys for req %s",
                len(matched_keys),
                request.request_id,
            )

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
            preempted_req_ids=set(scheduler_output.preempted_req_ids or ()),
            finished_req_ids=set(scheduler_output.finished_req_ids or ()),
        )

        # Deferred loads first: these requests are parked in
        # WAITING_FOR_REMOTE_KVS (not scheduled), so their load metadata is
        # emitted exactly once here, from state stashed at allocation time.
        # Every deferred hit takes the layerwise path. The transfers are
        # serialised on one stream, so a request's own layers arrive faster
        # than its attention consumes them however many requests are loading.
        layerwise_load = self._layerwise_overlap
        for req_id, (
            request,
            num_chunks,
            block_ids,
        ) in self._pending_deferred_loads.items():
            if layerwise_load:
                self._deferred_layerwise_waiting.add(req_id)
            meta.requests.append(
                MaruReqMeta(
                    req_id=req_id,
                    token_ids=list(request.prompt_token_ids or []),
                    block_ids=block_ids,
                    is_store=False,
                    num_matched_chunks=num_chunks,
                    deferred_load=True,
                    layerwise_load=layerwise_load,
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

            # Store the chunks this forward completes even when the same
            # request first loads an external prefix. One metadata batch may
            # carry both entries for one request: start_load_kv consumes the
            # load entry before the forward, save_kv_layer the store entry
            # after it. Only the inline load path reaches here with a pending
            # load — an asynchronous load parks the request and emits its
            # store on resume — and making the two exclusive left every inline
            # cache hit load-only, so a conversation's later turns never
            # published their newly computed suffix and the matched prefix
            # could not grow past the first turn.
            # num_computed_tokens is non-zero when a prefix was already covered
            # externally; only the chunks completed beyond it are stored.
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
            # If chunked prefill means not all chunks are covered, track for
            # store continuation in subsequent steps.
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

        if self._arrival_hint_enabled:
            # A request whose transfer metadata is in this step has consumed its
            # lookahead; one that finished or was preempted will never consume
            # it. Both free a window slot, so neither can strand the window.
            consumed = {req.req_id for req in meta.requests} | set(stale_ids)
            if self._pending_arrival_hints:
                self._pending_arrival_hints = [
                    entry
                    for entry in self._pending_arrival_hints
                    if entry[0] not in stale_ids
                ]
            meta.arrival_hint_keys = self._release_arrival_hints(consumed)
        if self._stage_enabled:
            assert self._stage_policy is not None
            consumed = {req.req_id for req in meta.requests}
            canceled = set(stale_ids)
            released: set[str] = set()
            if self._stage_trigger != "match":
                # A scheduled load joins its session's hint ticket: the alias
                # is relayed to the worker and the hint's policy slot is
                # consumed. Store-only metas leave the alias pending — a
                # request that never loads resolves at finish/preempt below,
                # where the ticket is dropped so a pinned stage cannot
                # outlive its consumer.
                for req in meta.requests:
                    if req.is_store or req.num_matched_chunks <= 0:
                        continue
                    alias = self._pending_stage_aliases.pop(req.req_id, None)
                    if alias is not None:
                        meta.stage_aliases[req.req_id] = alias
                        consumed.add(alias)
                        self._relayed_stage_aliases[req.req_id] = alias
                for rid in stale_ids:
                    pending = self._pending_stage_aliases.pop(rid, None)
                    if pending is not None and self._stage_trigger == "imminent":
                        # A hinted request died before loading: its queued
                        # stage is garbage — cancel it and drop the ticket.
                        canceled.add(pending)
                        self._pending_stage_releases.append(pending)
                    relayed = self._relayed_stage_aliases.pop(rid, None)
                    if relayed is not None:
                        # The stage was consumed during the request's life:
                        # free the policy slot and drop the worker ticket,
                        # but leave the queue alone — in turn_end mode the
                        # same id already holds the NEXT turn's plan, queued
                        # at this request's completion.
                        released.add(relayed)
                        self._pending_stage_releases.append(relayed)
                if self._pending_stage_releases:
                    meta.stage_release_ids = list(
                        dict.fromkeys(self._pending_stage_releases)
                    )
                    self._pending_stage_releases.clear()
            meta.stage_plans = self._stage_policy.advance(
                consumed=consumed,
                canceled=canceled,
                released=released,
            )
            if self._timing and isinstance(self._stage_policy, DeadlineStagePolicy):
                expired_total = self._stage_policy.expired_total
                if expired_total != self._stage_expired_seen:
                    _emit_timing(
                        f"stage expired: +{expired_total - self._stage_expired_seen} "
                        f"total={expired_total} t={time.time():.6f}"
                    )
                    self._stage_expired_seen = expired_total
            if meta.stage_plans:
                logger.debug(
                    "Maru stage: admitted %d reqs, %d inflight, %d queued",
                    len(meta.stage_plans),
                    self._stage_policy.inflight_requests,
                    self._stage_policy.queued_requests,
                )
                if self._timing:
                    step_tokens = sum(
                        getattr(
                            scheduler_output, "num_scheduled_tokens", {}
                        ).values()
                    )
                    _emit_timing(
                        f"stage advance: admitted="
                        f"{[p.req_id for p in meta.stage_plans]} "
                        f"step_tokens={step_tokens}"
                    )

        self._requests_need_load.clear()
        return meta

    def request_finished(
        self,
        request: Request,
        block_ids: list[int],
    ) -> tuple[bool, dict[str, Any] | None]:
        """Transfer block ownership to the worker for write-behind stores."""
        if self._stage_enabled and self._stage_trigger != "match":
            self._record_session_prefix(request)
        return self._write_behind, None

    def _count_matched_chunk_keys(self, keys: list[str]) -> int:
        """Count a cached prefix from precomputed packed chunk keys."""
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

    def _process_session_hints(self, request: Request) -> None:
        """Register hint bookkeeping for an arriving session request.

        Aliases the arriving request to its own session's hint plan id so the
        worker's demand join finds the staged ticket, and — in imminent
        mode — fires a stage for the gateway-hinted next session using the
        confirmed prefix recorded in the session registry. Idempotent across
        scheduling retries of the same request.
        """
        assert self._stage_policy is not None
        session_id, imminent = _request_session_params(request)
        if session_id:
            first_seen = request.request_id not in self._pending_stage_aliases
            self._pending_stage_aliases.setdefault(
                request.request_id, _hint_plan_id(session_id)
            )
            if first_seen and self._timing:
                _emit_timing(
                    f"stage arrive: session={session_id} t={time.time():.6f} "
                    f"(req {request.request_id})"
                )
        if self._stage_trigger != "imminent" or not imminent:
            return
        keys = self._session_keys.get(imminent)
        queued = bool(keys) and self._stage_policy.enqueue(
            _hint_plan_id(imminent), list(keys)
        )
        if queued:
            logger.debug(
                "Maru stage: queued %d hint keys for session %s (hinted by req %s)",
                len(keys or ()),
                imminent,
                request.request_id,
            )
        if self._timing:
            _emit_timing(
                f"stage imminent: session={imminent} keys={len(keys or ())} "
                f"queued={queued} t={time.time():.6f} "
                f"(carrier {request.request_id})"
            )

    def _record_session_prefix(self, request: Request) -> None:
        """Record the finished turn's confirmed prefix for its session.

        The keys just stored for this request are exactly the prefix the
        session's next turn will re-read — future request content is never
        consulted. In turn_end mode the prefix is also staged immediately.
        """
        session_id, _ = _request_session_params(request)
        if not session_id:
            if self._timing:
                _emit_timing("stage turn_end: finished request carries no session id")
            return
        token_ids = list(request.prompt_token_ids or [])
        keys = _chunk_keys(token_ids, self._kv_chunk_tokens)
        if not keys:
            return
        # Bounded registry: long deployments cycle many sessions; evict the
        # oldest entry rather than growing without limit.
        if session_id not in self._session_keys and len(self._session_keys) >= 16384:
            self._session_keys.pop(next(iter(self._session_keys)))
        self._session_keys[session_id] = tuple(keys)
        if self._stage_trigger == "turn_end":
            assert self._stage_policy is not None
            queued = self._stage_policy.enqueue(_hint_plan_id(session_id), keys)
            if queued:
                logger.debug(
                    "Maru stage: queued %d turn-end keys for session %s",
                    len(keys),
                    session_id,
                )
            if self._timing:
                _emit_timing(
                    f"stage turn_end: session={session_id} keys={len(keys)} "
                    f"queued={queued}"
                )

    def _release_arrival_hints(self, consumed: set[str]) -> list[str]:
        """Retire hints whose loads were just issued, then release the next ones.

        Retirement comes first so a load emitted this step frees its window slot
        for the request behind it in the same step.

        Args:
            consumed: Request ids whose load or store metadata is in this step's
                connector metadata — their lookahead has served its purpose.

        Returns:
            Chunk keys to relay to the worker, oldest arrival first.
        """
        self._arrival_hint_inflight -= consumed
        if self._arrival_hint_depth > 0:
            budget = self._arrival_hint_depth - len(self._arrival_hint_inflight)
        else:
            budget = len(self._pending_arrival_hints)
        if budget <= 0 or not self._pending_arrival_hints:
            return []

        released = self._pending_arrival_hints[:budget]
        self._pending_arrival_hints = self._pending_arrival_hints[budget:]
        keys: list[str] = []
        for req_id, req_keys in released:
            self._arrival_hint_inflight.add(req_id)
            keys.extend(req_keys)
        logger.debug(
            "Maru arrival-hint: released %d reqs (%d keys), %d inflight, %d queued",
            len(released),
            len(keys),
            len(self._arrival_hint_inflight),
            len(self._pending_arrival_hints),
        )
        return keys

    def on_new_request(self, request: Request) -> None:
        """Queue this request's chunk keys for worker-side arrival prefetch.

        Computes the chunk base keys from the prompt tokens and queues them;
        ``build_connector_meta`` releases them to the worker on a later step
        (arrival -> fire latency is ~one scheduler step, negligible against the
        0.2-2.1 s admission wait this hint exploits). The keys are the packed
        chunk keys (one per chunk) — the worker fires them as-is, no per-layer
        expansion, because packed storage keeps one object per chunk.

        Completion-returning staging is deliberately *not* started here. A
        preceding turn's write-behind registration may still be in flight at
        raw arrival, so an immediate lookup can falsely miss every reusable
        key. Staging is queued only after ``_count_matched_chunks`` confirms
        the prefix exists.

        Args:
            request: The newly arrived vLLM request.
        """
        if not self._arrival_hint_enabled:
            return
        token_ids = list(request.prompt_token_ids or [])
        if len(token_ids) < self._kv_chunk_tokens:
            return
        keys = _chunk_keys(token_ids, self._kv_chunk_tokens)
        if keys:
            self._pending_arrival_hints.append((request.request_id, keys))
            logger.debug(
                "Maru arrival-hint: queued %d chunk keys for req %s",
                len(keys),
                request.request_id,
            )


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
        num_kv_heads: int | None = None,
        head_size: int | None = None,
    ):
        self._block_size = block_size
        self._kv_chunk_tokens = kv_chunk_tokens
        self._extra_config = extra_config
        # Layout cross-checks. Optional; without them detection still verifies
        # block_size, but cannot separate the two rank-4 fused orders.
        self._num_kv_heads = num_kv_heads
        self._head_size = head_size
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
        # Resolved once in register_kv_caches; None when the layout is not
        # recognized, which keeps every caller on its existing fallback.
        self._kv_layout: KVLayout | None = None
        self._load_stream: torch.cuda.Stream | None = None
        self._load_stream_device: torch.device | None = None
        # layer_name -> events to wait on before that layer's compute. A
        # list because pre-issued requests each contribute their own event.
        self._layer_load_events: dict[str, list[torch.cuda.Event]] = {}
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
        # Loads the background thread has taken but not yet accounted for, and
        # those among them whose request was abandoned meanwhile. A request
        # aborted while parked keeps its blocks until this connector reports
        # its load complete; these two sets are what let that report wait for
        # copies queued after the abort was seen.
        self._inflight_deferred_req_ids: set[str] = set()
        self._abandoned_req_ids: set[str] = set()
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
        # Per-layer copies queued by the loader thread while the request was
        # parked; the resumed forward waits on these instead of issuing
        # anything. req_id -> {layer_name: event}.
        self._deferred_layerwise_events: dict[str, dict[str, torch.cuda.Event]] = {}
        self._layerwise_streams: list[torch.cuda.Stream] = []
        self._layerwise_stream_device: torch.device | None = None
        self._layerwise_stream_rr = 0
        # maru_log_timing only. Per-layer transfer spans of the layerwise
        # overlap path, so a run can show whether layer k's transfer really
        # ran while layer k-1 computed instead of asserting that it did.
        # req_id -> (done_event, epoch_event, [(layer_idx, start, end, nbytes)])
        self._layerwise_spans: dict[
            str,
            tuple[
                torch.cuda.Event,
                torch.cuda.Event,
                list[tuple[int, Any, Any, int]],
            ],
        ] = {}
        # (layer_name, before-wait, after-wait) pairs on the compute stream,
        # one per wait_for_layer_load call. Their gap is the time the forward
        # actually stalled on that layer's transfer — the quantity that says
        # whether the overlap worked, rather than that it was enabled.
        self._layer_wait_spans: list[
            tuple[str, torch.cuda.Event, torch.cuda.Event]
        ] = []
        # Last non-None attention metadata; deferred loads run between steps
        # (possibly with no forward pass) and reuse it for layout dispatch.
        self._last_attn_metadata: Any = None
        # Resolved lazily by _packed_load_kernel_ctx / _packed_store_kernel_ctx
        # for LMCache's multi_layer_kv_transfer kernel on the packed path.
        self._lmc_ops: Any = None

        # P6: storage granularity. Default (off) packs all layers of a chunk
        # into one CXL object with one key — matching LMCache use_layerwise=
        # False — so a request resolves num_chunks keys instead of
        # num_chunks x num_layers. Layerwise=True keeps the per-(chunk,layer)
        # objects and the layer-wise async overlap path.
        self._use_layerwise = bool(extra_config.get("maru_use_layerwise", False))
        async_load = bool(_get_knob(extra_config, "maru_async_load"))
        overlap_requested = bool(
            _get_knob(extra_config, "maru_overlap_load_with_compute")
        )
        # Mirrors the scheduler: overlap is layout-agnostic, async load is the
        # prerequisite (see the scheduler-side comment).
        self._layerwise_overlap = bool(overlap_requested and async_load)
        if overlap_requested and not async_load:
            logger.warning(
                "Maru layerwise overlap requires maru_async_load=true; disabling it"
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
        self._write_behind = bool(_get_knob(extra_config, "maru_async_store"))
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
        # Per-request CXL->GPU transfer events, drained by
        # get_finished_loading: req_id -> (start, end, nbytes, nchunks).
        self._deferred_load_bw: dict[
            str, tuple[torch.cuda.Event, torch.cuda.Event, int, int]
        ] = {}
        # Smart-prefetch arrival-hint (MARU_ARRIVAL_HINT=1): fire a lookahead
        # prefetch for the chunk keys relayed from the scheduler at arrival.
        self._arrival_hint_enabled = os.environ.get("MARU_ARRIVAL_HINT", "0") == "1"
        self._stage_enabled = os.environ.get("MARU_STAGE_PIPELINE", "0") == "1"
        self._hymcache_window_bytes = max(
            0, int(os.environ.get("MARU_HYMCACHE_WINDOW_BYTES", "0") or 0)
        )
        # Split admission into hint-then-wait so window depth reaches the
        # device. Off by default: the blocking-only path is what every
        # campaign through 2026-08-19 measured.
        self._hymcache_async_issue = (
            os.environ.get("MARU_HYMCACHE_ASYNC_ISSUE", "0") or "0"
        ).strip().lower() not in ("", "0", "false", "no")
        if self._hymcache_window_bytes > 0:
            if self._use_layerwise:
                logger.warning(
                    "HyMCache local substitution requires packed KV objects; "
                    "disabling MARU_HYMCACHE_WINDOW_BYTES for layerwise storage"
                )
                self._hymcache_window_bytes = 0
            else:
                if self._arrival_hint_enabled or self._stage_enabled:
                    logger.warning(
                        "HyMCache local substitution supersedes arrival/session/"
                        "request staging; disabling those hint paths"
                    )
                self._arrival_hint_enabled = False
                self._stage_enabled = False
                if self._layerwise_overlap:
                    logger.warning(
                        "MARU_HYMCACHE_WINDOW_BYTES supersedes "
                        "maru_overlap_load_with_compute (object-order vs "
                        "layer-order consumption); disabling the overlap"
                    )
                self._layerwise_overlap = False
                logger.info(
                    "HyMCache local CXL pipeline enabled (window_bytes=%d)",
                    self._hymcache_window_bytes,
                )
        if self._stage_enabled and self._arrival_hint_enabled:
            self._arrival_hint_enabled = False
        if self._stage_enabled and self._use_layerwise:
            self._stage_enabled = False
        self._stage_executor: ThreadPoolExecutor | None = None
        self._hymcache_executor: ThreadPoolExecutor | None = None
        self._stage_lock = threading.Lock()
        self._stage_tickets: dict[str, StageTicket] = {}
        self._stage_aliases: dict[str, str] = {}
        self._stage_pin_enabled = os.environ.get("MARU_GAIA_STAGE_PIN", "0") == "1"
        # Ordered layer-major fill hints (MARU_LAYER_HINT=1). The deferred
        # loaders fire one hint group per layer (prefetch_grouped) so the
        # device fills bytes in the order attention will consume them instead
        # of address order. MARU_LAYER_HINT_GROUP batches that many adjacent
        # layers per group — on packed storage a group of G layers is one
        # contiguous G*plane sliver per object, so larger G means fewer,
        # larger device commands.
        self._layer_hint_enabled = os.environ.get("MARU_LAYER_HINT", "0") == "1"
        self._layer_hint_group = max(
            1, int(os.environ.get("MARU_LAYER_HINT_GROUP", "1") or 1)
        )

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
        self._kv_layout = self._resolve_kv_layout(kv_caches)
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
        if self._timing:
            self._emit_layerwise_timing()

        self._ensure_handler()
        if self._handler is None:
            self._abort_deferred_loads(metadata)
            return

        # Fire arrival-hint prefetch before any early return below: hints must
        # flow even on steps that have nothing to load (a request's hint often
        # arrives a step before the request is scheduled).
        if self._arrival_hint_enabled and metadata.arrival_hint_keys:
            self._fire_arrival_hints(metadata.arrival_hint_keys)
        if self._stage_enabled:
            self._cancel_stage_requests(metadata.preempted_req_ids)
            if metadata.stage_aliases:
                self._stage_aliases.update(metadata.stage_aliases)
            # Hint tickets whose target request will never join: drop them
            # (and their device pins) instead of stranding READY work.
            # Releases run BEFORE submits — a session's fresh plan can arrive
            # in the same metadata as its previous ticket's release under the
            # same plan id, and must not be swallowed by the dying ticket.
            for hint_id in metadata.stage_release_ids:
                self._release_stage_ticket(hint_id)
            if metadata.stage_plans and self._timing:
                _emit_timing(
                    f"stage worker: received "
                    f"{[p.req_id for p in metadata.stage_plans]}"
                )
            for plan in metadata.stage_plans:
                self._submit_stage_plan(plan)

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
            # Requests whose copies the loader thread already queued hand over
            # their per-layer events; the rest are issued here as before.
            issue_here: set[str] = set()
            with self._deferred_lock:
                for req_id in metadata.layerwise_load_req_ids:
                    pre_issued = self._deferred_layerwise_events.pop(req_id, None)
                    if pre_issued is None:
                        issue_here.add(req_id)
                        continue
                    for layer_name, event in pre_issued.items():
                        self._layer_load_events.setdefault(layer_name, []).append(event)
            if issue_here:
                if self._use_layerwise:
                    # The layerwise-storage loader always publishes its events
                    # from the loader thread; a request reaching activation
                    # without them already went through the failure path.
                    logger.warning(
                        "Maru layerwise activation without pre-issued events "
                        "for req(s) %s; recompute already reported",
                        ", ".join(sorted(issue_here)),
                    )
                else:
                    self._schedule_deferred_packed_layerwise_loads(
                        layers,
                        issue_here,
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
            # Layerwise storage takes its own off-thread loader when overlap
            # is on: per-(chunk,layer) objects are copied layer by layer with
            # an event after each, so the resumed forward waits per layer.
            if (
                req_meta.deferred_load
                and self._use_layerwise
                and self._layerwise_overlap
                and self._try_submit_deferred_layerwise_load(req_meta)
            ):
                continue

            chunk_keys = _req_chunk_keys(req_meta, self._kv_chunk_tokens)
            num_chunks = min(req_meta.num_matched_chunks, len(chunk_keys))
            if num_chunks == 0:
                self._fail_deferred_load(req_meta)
                self._release_stage_ticket(req_meta.req_id)
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
            self._await_stage(req_meta.req_id)
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
                self._release_stage_ticket(req_meta.req_id)
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
                self._release_stage_ticket(req_meta.req_id)
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
            try:
                self._load_packed(layers, prepared_requests, attn_metadata)
            finally:
                for req_meta, _, _, _ in prepared_requests:
                    self._release_stage_ticket(req_meta.req_id)
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

        self._load_sync(layers, inline, attn_metadata)

        for req_meta, num_chunks, _, _ in inline:
            logger.info(
                "Maru: batch-loaded %d layers x %d chunks (%d tokens) for req %s",
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
        # HyMCache-faithful mode owns the request-local issue/wait/release loop
        # in _load_packed. Sending the request to this whole-request loader
        # would bypass its bounded windows and change the pipeline contract.
        if self._hymcache_window_bytes > 0:
            return False
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
        # Marked before the job can run: until it queues its copies there is
        # nothing for handle_preemptions to drain, so an abort arriving in
        # that window is recorded against this id instead.
        with self._deferred_lock:
            self._inflight_deferred_req_ids.add(req_meta.req_id)
        self._deferred_executor.submit(
            self._deferred_packed_load_job, req_meta, layers, device
        )
        return True

    def _try_submit_deferred_layerwise_load(self, req_meta: MaruReqMeta) -> bool:
        """Hand a layerwise-storage deferred load to the background thread.

        Returns:
            True when the load was submitted. False when the async path
            cannot run (KV caches not registered yet, non-CUDA or
            multi-device layout) — the caller then falls back to the
            synchronous layerwise load, which reports the request finished
            immediately.
        """
        if self._hymcache_window_bytes > 0:
            return False
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
        with self._deferred_lock:
            self._inflight_deferred_req_ids.add(req_meta.req_id)
        self._deferred_executor.submit(
            self._deferred_layerwise_store_load_job, req_meta, layers, device
        )
        return True

    def _deferred_layerwise_store_load_job(
        self,
        req_meta: MaruReqMeta,
        layers: list[tuple[str, torch.Tensor, int]],
        device: torch.device,
    ) -> None:
        """Load one parked request's per-(chunk,layer) objects, layer by layer.

        Runs on the deferred-load thread. Retrieves every (chunk, layer)
        object, optionally re-orders the device fill with one hint group per
        layer, then queues each layer's H2D copies with an event after the
        layer — the request unparks once the gate layer has landed and the
        resumed forward waits per layer, exactly like the packed overlap.
        Unlike the packed path there is no pitched gather: an object already
        holds a single layer's slice, so the copy is a plain per-run H2D.
        """
        try:
            handler = self._handler
            chunk_keys = _req_chunk_keys(req_meta, self._kv_chunk_tokens)
            num_chunks = min(req_meta.num_matched_chunks, len(chunk_keys))
            if handler is None or num_chunks == 0:
                self._fail_deferred_load(req_meta)
                return
            chunk_keys = chunk_keys[:num_chunks]
            ordered_layers = sorted(layers, key=lambda item: item[2])
            keys = [
                f"{ck}_L{idx}" for (_, _, idx) in ordered_layers for ck in chunk_keys
            ]
            total_tokens = num_chunks * self._kv_chunk_tokens
            slot_mapping = self._build_slot_mapping(req_meta.block_ids, total_tokens)

            _job_t0 = time.monotonic()
            hint_groups = None
            if self._layer_hint_enabled:
                # One hint group per layer, whole objects, piggybacked on the
                # retrieve's own metadata lookup (no extra RPC): the fill
                # follows the layer run order instead of address order, and
                # it starts before the region mmaps. Requires the retrieve
                # hint off (MARU_GAIA_RETRIEVE_HINT=0) or the whole-request
                # address-order hint races this one.
                nc = num_chunks
                hint_groups = [
                    [(li * nc + ci, None, None) for ci in range(nc)]
                    for li in range(len(ordered_layers))
                ]
            _hint_done = time.monotonic()

            if hint_groups:
                infos = self._batch_retrieve_all(keys, hint_groups=hint_groups)
            else:
                infos = self._batch_retrieve_all(keys)
            _retrieve_done = time.monotonic()
            if self._timing:
                _emit_timing(
                    f"deferred retrieve batch {len(keys)} keys = "
                    f"{(_retrieve_done - _hint_done) * 1000:.2f} ms "
                    f"(req {req_meta.req_id})"
                )
                _emit_timing(
                    "job-milestones "
                    f"t0={_job_t0:.6f} "
                    f"hint_done={_hint_done:.6f} "
                    f"retrieve_done={_retrieve_done:.6f} "
                    f"(req {req_meta.req_id})"
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

            slot_mapping = self._pin_slot_mapping_for_async_h2d(slot_mapping)
            torch.cuda.set_device(device)
            stream = self._layerwise_stream_for(device)
            attn = self._last_attn_metadata
            ct = self._kv_chunk_tokens
            events: dict[str, torch.cuda.Event] = {}
            spans: list[tuple[int, Any, Any, int]] = []
            epoch_event: torch.cuda.Event | None = None
            with torch.cuda.stream(stream):
                slot_gpu = slot_mapping.to(device, non_blocking=True)
                if self._timing:
                    epoch_event = torch.cuda.Event(enable_timing=True)
                    epoch_event.record(stream)
                for li, (layer_name, kv_cache_layer, true_idx) in enumerate(
                    ordered_layers
                ):
                    base = li * num_chunks
                    layer_bytes = 0
                    if self._timing:
                        span_start = torch.cuda.Event(enable_timing=True)
                        span_start.record(stream)
                    for chunk_start, run_chunks, run_view in self._chunk_runs(
                        infos[base : base + num_chunks]
                    ):
                        run_host = torch.frombuffer(
                            run_view, dtype=kv_cache_layer.dtype
                        )
                        layer_bytes += _slab_nbytes(run_view)
                        token_start = chunk_start * ct
                        token_end = (chunk_start + run_chunks) * ct
                        self._inject_kv_into_layer(
                            kv_cache_layer,
                            run_host.to(device, non_blocking=True),
                            slot_gpu[token_start:token_end],
                            attn,
                            layer_name,
                            num_chunks=run_chunks,
                        )
                    if self._timing:
                        span_end = torch.cuda.Event(enable_timing=True)
                        span_end.record(stream)
                        spans.append((true_idx, span_start, span_end, layer_bytes))
                    event = torch.cuda.Event()
                    event.record(stream)
                    events[layer_name] = event
                done_event = torch.cuda.Event()
                done_event.record(stream)
            gate = self._unpark_gate_event(events, layers)
            with self._deferred_lock:
                abandoned = req_meta.req_id in self._abandoned_req_ids
                self._abandoned_req_ids.discard(req_meta.req_id)
                if not abandoned:
                    self._deferred_layerwise_events[req_meta.req_id] = events
                    self._active_load_refs.append(
                        (done_event, [infos, slot_mapping, slot_gpu])
                    )
                    if gate is not None:
                        self._deferred_events[req_meta.req_id] = gate
                    else:
                        self._deferred_done.add(req_meta.req_id)
                    if spans and epoch_event is not None:
                        self._layerwise_spans[req_meta.req_id] = (
                            done_event,
                            epoch_event,
                            spans,
                        )
            if abandoned:
                logger.info(
                    "Maru: draining abandoned layerwise-storage load for req %s",
                    req_meta.req_id,
                )
                _drain_events(events.values())
                with self._deferred_lock:
                    self._active_load_refs.append(
                        (done_event, [infos, slot_mapping, slot_gpu])
                    )
                    self._deferred_done.add(req_meta.req_id)
                return
            if self._timing:
                _emit_timing(
                    f"job-queued t={time.monotonic():.6f} (req {req_meta.req_id})"
                )
            logger.info(
                "Maru: deferred layerwise-storage load queued %d layers x %d "
                "chunks (%d tokens) for req %s",
                len(ordered_layers),
                num_chunks,
                total_tokens,
                req_meta.req_id,
            )
        except Exception as e:
            logger.error(
                "Maru deferred layerwise load failed for req %s: %s",
                req_meta.req_id,
                e,
            )
            self._fail_deferred_load(req_meta)
        finally:
            with self._deferred_lock:
                self._inflight_deferred_req_ids.discard(req_meta.req_id)
                self._abandoned_req_ids.discard(req_meta.req_id)

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

            hint_groups = None
            if self._layer_hint_enabled:
                # Re-order the device fill to match consumption: one hint
                # group per layer(-group), each group the scattered slivers of
                # that layer inside every packed object, piggybacked on the
                # retrieve's own metadata lookup (no extra RPC). The
                # whole-request retrieve hint must be off
                # (MARU_GAIA_RETRIEVE_HINT=0) or its address-order fill races
                # this one. Plane geometry comes from the registered caches —
                # the slabs are not retrieved yet.
                object_bytes = self._chunk_object_bytes()
                if object_bytes:
                    plane_bytes = object_bytes // (2 * self._num_layers)
                    span_groups = _packed_layer_span_groups(
                        list(range(len(keys))),
                        self._num_layers,
                        plane_bytes,
                        self._layer_hint_group,
                    )
                    hint_groups = span_groups

            _t0 = time.monotonic()
            if hint_groups:
                infos = self._batch_retrieve_all(keys, hint_groups=hint_groups)
            else:
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
                # The RPC/mmap part is complete. Either queue the per-layer
                # copies here, while the request is still parked, or retain
                # the packed CXL views so the resumed forward issues them.
                # Either way the packed keys/pages are preserved and the bulk
                # load overlaps the request's own compute.
                events = self._issue_layerwise_copies_offthread(
                    req_meta, num_chunks, slot_mapping, infos, layers, device
                )
                gate = (
                    self._unpark_gate_event(events, layers)
                    if events is not None
                    else None
                )
                with self._deferred_lock:
                    # Checked and published under one lock: a drain running in
                    # between would otherwise mark the request abandoned after
                    # this thread had already decided it was live.
                    abandoned = req_meta.req_id in self._abandoned_req_ids
                    self._abandoned_req_ids.discard(req_meta.req_id)
                    if not abandoned:
                        if events is not None:
                            self._deferred_layerwise_events[req_meta.req_id] = events
                            if gate is not None:
                                # Hold the request until enough layers have
                                # landed. get_finished_loading promotes it when
                                # the gate event fires, so nothing blocks the
                                # loader thread.
                                self._deferred_events[req_meta.req_id] = gate
                        else:
                            self._deferred_layerwise_loads[req_meta.req_id] = (
                                req_meta,
                                num_chunks,
                                slot_mapping,
                                infos,
                            )
                        if events is None or gate is None:
                            self._deferred_done.add(req_meta.req_id)
                if abandoned:
                    # The request was aborted while this load ran, so no
                    # forward will wait per layer and the completion report is
                    # what frees its blocks. The copies finish first, on this
                    # thread rather than the engine's.
                    logger.info(
                        "Maru: draining abandoned layerwise load for req %s",
                        req_meta.req_id,
                    )
                    _drain_events(events.values() if events else ())
                    with self._deferred_lock:
                        self._deferred_done.add(req_meta.req_id)
                    return
                logger.info(
                    "Maru: deferred retrieve ready for packed-layerwise load "
                    "%d chunks (%d tokens) for req %s (%s)",
                    num_chunks,
                    total_tokens,
                    req_meta.req_id,
                    "copies queued off-thread"
                    if events is not None
                    else "issue in resumed forward",
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
            load_bytes = 0
            bw_events: tuple[torch.cuda.Event, torch.cuda.Event] | None = None
            with torch.cuda.stream(stream):
                slot_gpu = slot_mapping.to(device, non_blocking=True)
                if self._timing:
                    # Bracket only the CXL->GPU bytes so the measured interval
                    # is the media read, not the RPC or the admission wait.
                    bw_start = torch.cuda.Event(enable_timing=True)
                    bw_start.record(stream)
                for ci in range(num_chunks):
                    chunk_slots = slot_gpu[ci * ct : (ci + 1) * ct]
                    slab_host = torch.frombuffer(infos[ci].view, dtype=dtype).view(
                        2, num_layers, ct, -1
                    )
                    load_bytes += slab_host.numel() * slab_host.element_size()
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
                if self._timing:
                    bw_end = torch.cuda.Event(enable_timing=True)
                    bw_end.record(stream)
                    bw_events = (bw_start, bw_end)
                event = torch.cuda.Event()
                event.record(stream)
            with self._deferred_lock:
                self._deferred_events[req_meta.req_id] = event
                self._deferred_refs[req_meta.req_id] = [
                    infos,
                    slot_mapping,
                    slot_gpu,
                ]
                if bw_events is not None:
                    self._deferred_load_bw[req_meta.req_id] = (
                        bw_events[0],
                        bw_events[1],
                        load_bytes,
                        num_chunks,
                    )
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
        finally:
            # This load is accounted for now, however it ended. An abort
            # arriving after this point finds the queued copies themselves.
            with self._deferred_lock:
                self._inflight_deferred_req_ids.discard(req_meta.req_id)
                self._abandoned_req_ids.discard(req_meta.req_id)

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

    def _unpark_gate_event(
        self,
        events: dict[str, torch.cuda.Event],
        layers: list[tuple[str, torch.Tensor, int]],
    ) -> torch.cuda.Event | None:
        """Pick the layer whose arrival releases the request, or None.

        Waking a request the moment its copies are queued pushes the whole
        transfer into the forward, where every request in the batch waits for
        it. Holding it until every layer has landed is the background path and
        overlaps nothing. Releasing after a fraction of the layers keeps the
        part that cannot hide behind compute in the parked wait, where it
        blocks nobody, and lets only the tail overlap.

        Args:
            events: layer_name -> completion event for this request.
            layers: (layer_name, kv tensor, layer index) for every layer;
                the index gives execution order, which the dict does not.

        Returns:
            The event to gate on, or None to release immediately.
        """
        if not events:
            return None
        ordered = [name for name, _, _ in sorted(layers, key=lambda item: item[2])]
        ordered = [name for name in ordered if name in events]
        if not ordered:
            return None
        count = min(_LAYERWISE_RELEASE_AFTER_LAYERS, len(ordered))
        return events[ordered[count - 1]]

    def _layerwise_stream_for(self, device: torch.device) -> torch.cuda.Stream:
        """Hand out one of the pooled layerwise streams, round-robin.

        Requests need independent ordering: on a shared stream, a request
        queued second would not reach its layer 0 until the first request's
        layer 31 had finished, which defeats the pipeline. Only the loader
        thread calls this, so the round-robin cursor needs no lock.
        """
        if self._layerwise_stream_device != device or not self._layerwise_streams:
            self._layerwise_streams = [
                torch.cuda.Stream(device=device, priority=-1)
                for _ in range(_LAYERWISE_STREAM_COUNT)
            ]
            self._layerwise_stream_device = device
            self._layerwise_stream_rr = 0
        stream = self._layerwise_streams[
            self._layerwise_stream_rr % len(self._layerwise_streams)
        ]
        self._layerwise_stream_rr += 1
        return stream

    def _issue_layerwise_copies_offthread(
        self,
        req_meta: MaruReqMeta,
        num_chunks: int,
        slot_mapping: torch.Tensor,
        infos: list[Any],
        layers: list[tuple[str, torch.Tensor, int]],
        device: torch.device,
    ) -> dict[str, torch.cuda.Event] | None:
        """Queue one parked request's per-layer H2D from the loader thread.

        The request is parked in WAITING_FOR_REMOTE_KVS with its blocks
        already allocated, so its paged KV can be written now — this is what
        the default deferred path does for the whole request. Here the copies
        are issued layer by layer with an event after each, so the resumed
        forward waits per layer instead of issuing anything itself. The
        transfer also gets a head start over the forward.

        Args:
            req_meta: Metadata of the parked request.
            num_chunks: Chunks matched for this request.
            slot_mapping: Host slot mapping covering those chunks.
            infos: Retrieved CXL views, one per chunk.
            layers: (layer_name, paged kv tensor, layer index) for every layer.
            device: CUDA device holding the paged KV.

        Returns:
            layer_name -> event, or None when the copies could not be queued.
            On None the caller retains the CXL views so the resumed forward
            issues them the old way.
        """
        stream: torch.cuda.Stream | None = None
        try:
            torch.cuda.set_device(device)
            stream = self._layerwise_stream_for(device)
            pinned = self._pin_slot_mapping_for_async_h2d(slot_mapping)
            attn = self._last_attn_metadata
            tokens = num_chunks * self._kv_chunk_tokens
            _t0 = time.monotonic()
            events: dict[str, torch.cuda.Event] = {}
            spans: list[tuple[int, Any, Any, int]] = []
            epoch_event: torch.cuda.Event | None = None
            with torch.cuda.stream(stream):
                slot_gpu = pinned.to(device, non_blocking=True)
                if self._timing:
                    epoch_event = torch.cuda.Event(enable_timing=True)
                    epoch_event.record(stream)
                for layer_name, kv_cache_layer, true_idx in layers:
                    if self._timing:
                        span_start = torch.cuda.Event(enable_timing=True)
                        span_start.record(stream)
                    layer_dev = self._copy_packed_layer_to_device(
                        infos, num_chunks, true_idx, kv_cache_layer, stream
                    )
                    if self._timing:
                        copy_done = torch.cuda.Event(enable_timing=True)
                        copy_done.record(stream)
                        spans.append(
                            (true_idx, span_start, copy_done, layer_dev.nbytes)
                        )
                    self._inject_kv_into_layer(
                        kv_cache_layer,
                        layer_dev,
                        slot_gpu[:tokens],
                        attn,
                        layer_name,
                        num_chunks=num_chunks,
                    )
                    event = torch.cuda.Event()
                    event.record(stream)
                    events[layer_name] = event
            # The CXL views and slot mappings must outlive the queued copies;
            # release rides on the last layer's completion.
            done_event = torch.cuda.Event()
            done_event.record(stream)
            with self._deferred_lock:
                self._active_load_refs.append((done_event, [infos, pinned, slot_gpu]))
                if spans and epoch_event is not None:
                    self._layerwise_spans[req_meta.req_id] = (
                        done_event,
                        epoch_event,
                        spans,
                    )
            if self._timing:
                _emit_timing(
                    f"offthread layerwise issue {len(layers)}L x 1r = "
                    f"{(time.monotonic() - _t0) * 1000:.2f} ms "
                    f"(req {req_meta.req_id})"
                )
            return events
        except Exception as e:
            logger.error(
                "Maru off-thread layerwise issue failed for req %s: %s — "
                "falling back to in-forward issue",
                req_meta.req_id,
                e,
            )
            if stream is not None:
                try:
                    stream.synchronize()
                except Exception:
                    pass
            return None

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
            for req_meta, num_chunks, slot_mapping, infos in entries:
                try:
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
                except Exception as e:
                    self._fail_load(req_meta, e)
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
        # Collected locally and published only once every layer is queued.
        # _layer_load_events already holds the events of requests whose copies
        # the loader thread issued ahead of this forward, and a failure here
        # says nothing about those: they are loading correctly and their
        # forward must still wait on them.
        issued: dict[str, torch.cuda.Event] = {}
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
                    issued[layer_name] = event
        except Exception as e:
            logger.error("Maru packed-layerwise activation failed: %s", e)
            try:
                load_stream.synchronize()
            except Exception:
                pass
            with self._deferred_lock:
                for req_meta, *_ in entries:
                    self._failed_load_blocks.update(req_meta.block_ids)
            return

        for layer_name, event in issued.items():
            self._layer_load_events.setdefault(layer_name, []).append(event)

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

    def _fail_load(self, req_meta: MaruReqMeta, exc: Exception) -> None:
        """Contain one request's load failure instead of letting it raise.

        vLLM calls ``start_load_kv`` outside its own try, so an exception
        from a load path (e.g. an unrecognized-layout refusal from inject)
        would kill the engine. Reporting the blocks through
        ``get_block_ids_with_load_errors`` makes vLLM recompute them, which
        is the contract the refusal was designed for; a deferred request is
        additionally marked done so it unparks.
        """
        logger.error(
            "Maru load failed for req %s (%s); recomputing", req_meta.req_id, exc
        )
        with self._deferred_lock:
            self._failed_load_blocks.update(req_meta.block_ids)
            if req_meta.deferred_load:
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
            try:
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
                            token_end = (
                                chunk_start + run_chunks
                            ) * self._kv_chunk_tokens
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
            except Exception as e:
                self._fail_load(req_meta, e)
                continue
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
        finished_bw = []
        with self._deferred_lock:
            done = set(self._deferred_done)
            self._deferred_done.clear()
            for req_id, event in list(self._deferred_events.items()):
                if event.query():
                    done.add(req_id)
                    del self._deferred_events[req_id]
                    self._deferred_refs.pop(req_id, None)
                    bw = self._deferred_load_bw.pop(req_id, None)
                    if bw is not None:
                        finished_bw.append((req_id, bw))
        # Outside the lock: elapsed_time is safe here because the completion
        # event was recorded after the end marker, so both have fired.
        if self._timing and done:
            now = time.monotonic()
            for req_id in done:
                _emit_timing(f"unpark t={now:.6f} (req {req_id})")
        for req_id, (bw_start, bw_end, nbytes, nchunks) in finished_bw:
            self._emit_load_bandwidth(req_id, bw_start, bw_end, nbytes, nchunks)
        for req_id in done:
            self._release_stage_ticket(req_id)
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
        """Drain transfers before the scheduler reclaims the blocks they write.

        vLLM calls this before the step's forward and before
        ``get_finished_loading``, which makes it the one place that covers both
        ways a parked request's blocks are taken away:

        - Preemption returns the blocks to the pool right away.
        - A request that finished or was aborted while parked keeps its blocks
          until the connector reports its load complete, and vLLM frees them
          the moment that report arrives.

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

        if not self._layerwise_overlap:
            return
        if metadata.preempted_req_ids:
            self._drain_layerwise_copies(metadata.preempted_req_ids, report=False)
        if metadata.finished_req_ids:
            self._drain_layerwise_copies(metadata.finished_req_ids, report=True)

    def _drain_layerwise_copies(self, req_ids: set[str], report: bool) -> None:
        """Complete the queued copies of requests losing their blocks.

        Entries whose copies were never issued are simply dropped — a
        preempted request redoes the match/load handshake on fresh blocks, and
        a finished one wants nothing. Copies already queued off-thread are
        writing into blocks about to be reassigned, so they are drained first.
        Preemption and abort are both rare, so paying a synchronize is the
        right trade.

        Args:
            req_ids: Requests whose blocks the scheduler is reclaiming.
            report: Whether the reclaim is waiting on a completion report.
                The gate releases a request once its first layer lands, so a
                request that will never run a forward still owes its remaining
                layers before that report may go out. The report is still
                owed — withholding it strands the blocks — so it is queued
                here, now that the copies are complete. A request whose gate
                fired in an earlier step is drained without reporting again:
                vLLM has already freed it and would reject a second report.
                A load still running on the background thread has no copies to
                drain yet, so the abort is recorded against it instead and the
                loader honours it once it has queued them.
        """
        drain: list[torch.cuda.Event] = []
        owed: set[str] = set()
        with self._deferred_lock:
            for req_id in req_ids:
                self._deferred_layerwise_loads.pop(req_id, None)
                pre_issued = self._deferred_layerwise_events.pop(req_id, None)
                if not pre_issued:
                    if report and req_id in self._inflight_deferred_req_ids:
                        self._abandoned_req_ids.add(req_id)
                    continue
                drain.extend(pre_issued.values())
                if report and self._deferred_events.pop(req_id, None) is not None:
                    self._deferred_refs.pop(req_id, None)
                    owed.add(req_id)
        _drain_events(drain)
        if owed:
            with self._deferred_lock:
                self._deferred_done |= owed

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
        for req_meta, num_chunks, slot_mapping, infos in prepared_requests:
            try:
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
            except Exception as e:
                self._fail_load(req_meta, e)

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
        if self._hymcache_window_bytes > 0:
            self._load_packed_hymcache(
                layers,
                prepared_requests,
                attn_metadata,
            )
            return

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
                try:
                    if use_stream:
                        slot_mapping = self._pin_slot_mapping_for_async_h2d(
                            slot_mapping
                        )
                    slot_gpu = slot_mapping.to(dev, non_blocking=use_stream)
                    for ci in range(num_chunks):
                        slab_view = slab_infos[ci].view
                        chunk_slots = slot_gpu[ci * ct : (ci + 1) * ct]
                        # KV_2LTD host tensor aliasing pinned CXL:
                        # [2, L, tokens, h]
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
                except Exception as e:
                    self._fail_load(req_meta, e)
                    continue
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
        the DEFAULT packed load whenever available — it is not gated on any
        configuration knob. The pointer table is indexed by each layer's true
        ``_get_layer_index`` so it aligns with the slab's layer dimension.
        Works with whatever paged axis order vLLM chose: dimensions and the
        engine KV format both come from the layout resolved at registration.
        """
        device = layers[0][1].device
        if device.type != "cuda" or not torch.cuda.is_available():
            return None
        # No resolved layout, or one with no kernel form (the fused orders),
        # means we must not hand raw pointers to the kernel.
        layout = self._kv_layout
        if layout is None or layout.format_name is None:
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
        # Resolve lmcache.c_ops lazily.
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
        # Dimensions and format come from one layout, so they cannot disagree.
        # The kernel takes raw pointers, so torch would not catch it if they did.
        block_size = layout.block_size
        head_size = layout.head_size
        page_buffer_size = layout.page_buffer_size
        # Pointer table indexed by true layer index (== slab layer dim).
        ptrs = torch.empty(len(layers), dtype=torch.int64, device="cpu")
        for _, kv_cache_layer, true_idx in layers:
            ptrs[true_idx] = kv_cache_layer.data_ptr()
        ops = self._lmc_ops
        # An older c_ops build may not carry every format; take the per-layer
        # fallback rather than raising out of the load path.
        kv_format = getattr(ops.EngineKVFormat, layout.format_name, None)
        if kv_format is None:
            logger.warning(
                "Maru packed load: kernel has no format %s; using per-layer "
                "inject fallback",
                layout.format_name,
            )
            return None
        return (
            ops,
            ptrs.to(device),
            page_buffer_size,
            block_size,
            head_size,
            kv_format,
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
        """Make the model stream wait for an asynchronously loaded layer.

        One event per request contributed a copy for this layer, so all of
        them are awaited. Enqueuing a wait costs far less than issuing the
        copy itself, which is what lets this path run with several requests
        loading at once.
        """
        events = self._layer_load_events.get(layer_name)
        if not events:
            return
        stream = torch.cuda.current_stream()
        if not self._timing:
            for event in events:
                stream.wait_event(event)
            return
        if not self._layer_wait_spans:
            # First gated layer of this forward: a wall stamp here, joined
            # with the loader's job-milestones and the unpark stamp, splits
            # the span between unparking and the forward actually reaching
            # its first attention layer.
            _emit_timing(f"first-wait t={time.monotonic():.6f} layer={layer_name}")
        # Bracket the waits on the compute stream itself: the gap between the
        # two marks is exactly how long this layer's attention was held back
        # by its transfer. Near zero for layers the overlap covered.
        before = torch.cuda.Event(enable_timing=True)
        before.record(stream)
        for event in events:
            stream.wait_event(event)
        after = torch.cuda.Event(enable_timing=True)
        after.record(stream)
        self._layer_wait_spans.append((layer_name, before, after))

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
        with self._deferred_lock:
            if not self._active_load_refs:
                return
            self._active_load_refs = [
                entry for entry in self._active_load_refs if not entry[0].query()
            ]

    def _batch_retrieve_all(
        self,
        keys: list[str],
        batch_size: int = 1024,
        hint_groups: list[list[tuple[int, int | None, int | None]]] | None = None,
    ) -> list:
        """``batch_retrieve`` over ``keys`` in payload-bounded chunks (ordered).

        A single request can produce num_layers x num_chunks keys (thousands),
        so the batch RPC is split into ``batch_size``-key chunks to bound
        payload while keeping RPC count ~O(len(keys) / batch_size).

        ``hint_groups`` (global key indices) piggyback on each chunk's own
        lookup: a group is split at a chunk boundary into per-chunk
        sub-groups, which preserves the overall issue order because chunks
        are retrieved in key order.
        """
        handler = self._handler
        assert handler is not None
        if len(keys) <= batch_size:
            if hint_groups:
                return list(handler.batch_retrieve(keys, hint_groups=hint_groups))
            return list(handler.batch_retrieve(keys))
        out: list[Any] = []
        for i in range(0, len(keys), batch_size):
            chunk = keys[i : i + batch_size]
            local_groups = []
            if hint_groups:
                for group in hint_groups:
                    local = [
                        (gi - i, offset, length)
                        for gi, offset, length in group
                        if i <= gi < i + batch_size
                    ]
                    if local:
                        local_groups.append(local)
            if local_groups:
                out.extend(handler.batch_retrieve(chunk, hint_groups=local_groups))
            else:
                out.extend(handler.batch_retrieve(chunk))
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
        # Copies the loader thread queued read pinned buffers and CXL views
        # that the lines below drop and unmap. A run where every request took
        # the off-thread path never created _load_stream, so these streams are
        # the only thing still holding that memory.
        for stream in self._layerwise_streams:
            try:
                stream.synchronize()
            except Exception as e:
                logger.error("Error synchronizing Maru layerwise stream: %s", e)
        self._layerwise_streams = []
        self._layerwise_stream_device = None
        self._layer_load_events.clear()
        self._active_load_refs.clear()
        with self._deferred_lock:
            self._deferred_layerwise_loads.clear()
            self._deferred_layerwise_events.clear()
            self._deferred_events.clear()
            self._deferred_refs.clear()
            self._deferred_done.clear()
            self._inflight_deferred_req_ids.clear()
            self._abandoned_req_ids.clear()
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

        The exact inverse of ``_extract_kv_from_layer``, branch for branch:
        - MLA: [num_blocks, block_size, ...]
        - Triton: [num_blocks, num_kv_heads, block_size, head_dim]
        - Everything else: whatever axis order the backend chose, resolved at
          registration and canonicalized before the scatter.
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
        elif (
            isinstance(layer_meta, TritonAttentionMetadata) and dst_kv_cache.dim() == 4
        ):
            # Legacy rank-4 Triton; rank-5 takes the layout branch (see
            # _extract_kv_from_layer). This reshape assumes dim 1 is the head
            # axis and would misplace rank-5's K/V axis.
            block_idxs = slot_mapping // self._block_size
            offsets = slot_mapping % self._block_size
            src = src_kv_data.reshape(slot_mapping.shape[0], dst_kv_cache.shape[1], -1)
            dst_kv_cache[block_idxs, :, offsets] = src
        else:
            layout = self._layout_for(dst_kv_cache)
            if layout is None:
                # Same refusal as extract; the caller recomputes instead.
                raise RuntimeError(
                    "Maru: cannot inject KV for layout "
                    f"{tuple(dst_kv_cache.shape)}; unrecognized paged KV tensor"
                )
            # A view, so the index-put below lands in dst_kv_cache itself.
            canon = _canonical_paged_view(dst_kv_cache, layout)
            ntok = slot_mapping.shape[0]
            # Slab ordering, independent of the paged axis order.
            if num_chunks == 1:
                src = src_kv_data.reshape(2, ntok, -1)
            else:
                tokens_per_chunk = ntok // num_chunks
                src = (
                    src_kv_data.reshape(num_chunks, 2, tokens_per_chunk, -1)
                    .permute(1, 0, 2, 3)
                    .reshape(2, ntok, -1)
                )
            blocks = slot_mapping // layout.block_size
            offsets = slot_mapping % layout.block_size
            if layout.kv_axis is not None:
                canon[:, blocks, offsets] = src.reshape(2, ntok, *canon.shape[3:])
            else:
                if canon.shape[2:].numel() % 2:
                    raise RuntimeError(
                        "Maru: cannot inject KV for layout "
                        f"{tuple(dst_kv_cache.shape)}; odd fused K/V row width"
                    )
                # Undo the extract side's halving of each token's row.
                merged = src.transpose(0, 1).reshape(ntok, -1)
                canon[blocks, offsets] = merged.reshape(ntok, *canon.shape[2:])

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
        elif isinstance(layer_meta, TritonAttentionMetadata) and kv_layer.dim() == 4:
            # Legacy rank-4 Triton (NB, NH, BS, HD). Rank-5 Triton is the
            # common (NB, 2, BS, NH, HS) and takes the layout branch, which
            # also keeps its slab bytes K/V-major like every other backend.
            block_idxs = slot_mapping // self._block_size
            offsets = slot_mapping % self._block_size
            return kv_layer[block_idxs, :, offsets]
        else:
            # Callers expect [2, num_tokens, hidden]. Gather the wanted slots
            # from the canonical view; permuting the whole cache would copy it.
            layout = self._layout_for(kv_layer)
            if layout is None:
                # Refusing degrades the request to recompute. Guessing an axis
                # order would silently register scrambled KV instead.
                raise RuntimeError(
                    "Maru: cannot extract KV for layout "
                    f"{tuple(kv_layer.shape)}; unrecognized paged KV tensor"
                )
            canon = _canonical_paged_view(kv_layer, layout)
            blocks = slot_mapping // layout.block_size
            offsets = slot_mapping % layout.block_size
            if layout.kv_axis is not None:
                gathered = canon[:, blocks, offsets]  # [2, ntok, ...]
                return gathered.reshape(2, gathered.shape[1], -1)
            # Fused: the leading 2 is a storage convention, not a K/V split.
            # Halving each row is a bijection inject reverses, and claims
            # nothing about where K ends — diffkv splits it unevenly.
            if canon.shape[2:].numel() % 2:
                # Cannot halve an odd row. No shipped backend produces one,
                # but refuse cleanly rather than fail inside the reshape.
                raise RuntimeError(
                    "Maru: cannot extract KV for layout "
                    f"{tuple(kv_layer.shape)}; odd fused K/V row width"
                )
            gathered = canon[blocks, offsets]  # [ntok, ...]
            ntok = gathered.shape[0]
            return gathered.reshape(ntok, 2, -1).transpose(0, 1)

    def _layout_for(self, kv_layer: torch.Tensor) -> KVLayout | None:
        """Layout for ``kv_layer``: the registered one, else detect in place.

        The registered layout describes the first registered layer, so it is
        applied only when it actually fits this tensor — ``movedim`` with a
        rank-5 layout succeeds on a rank-3 tensor and returns wrong bytes, so
        a hybrid model's odd layers must be re-detected, not assumed. The
        detection fallback also covers caches attached without
        register_kv_caches.
        """
        shape = tuple(kv_layer.shape)
        layout = self._kv_layout
        if layout is not None and _layout_fits(layout, shape):
            return layout
        return _detect_kv_layout(
            shape,
            self._block_size,
            _vllm_kv_cache_layout(),
            self._num_kv_heads,
            self._head_size,
        )

    def _resolve_kv_layout(self, kv_caches: dict[str, torch.Tensor]) -> KVLayout | None:
        """Detect the paged KV axis order once, at registration.

        Args:
            kv_caches: The registered per-layer KV tensors. The first is taken
                as representative; hybrid models whose layers differ in shape
                are not supported, as before.

        Returns:
            The resolved layout, or None when it is unrecognized. Callers then
            keep the default CXL page size and per-layer copies, exactly as
            before this detection existed.
        """
        if not kv_caches:
            return None
        sample = next(iter(kv_caches.values()))
        if not hasattr(sample, "shape"):
            return None
        kv_layout = _vllm_kv_cache_layout()
        layout = _detect_kv_layout(
            tuple(sample.shape),
            self._block_size,
            kv_layout,
            self._num_kv_heads,
            self._head_size,
        )
        if layout is None:
            logger.warning(
                "MaruWorkerConnector: unrecognized KV layout %s "
                "(block_size=%d, kv_layout=%s); keeping default CXL page size "
                "and per-layer transfers",
                tuple(sample.shape),
                self._block_size,
                kv_layout,
            )
            return None
        logger.info(
            "MaruWorkerConnector: KV layout %s (%s) num_blocks=%d block_size=%d "
            "num_heads=%d head_size=%d",
            layout.format_name,
            kv_layout,
            layout.num_blocks,
            layout.block_size,
            layout.num_heads,
            layout.head_size,
        )
        return layout

    def _chunk_object_bytes(self) -> int | None:
        """Return the byte size of one ``(chunk x layer)`` KV object, or None.

        A stored object holds ``kv_chunk_tokens`` tokens of one layer's KV, so
        its size is ``kv_chunk_tokens x per_token_per_layer_bytes``. The
        per-token footprint divides the layer tensor by its token slots, which
        ``register_kv_caches`` already resolved for whatever axis order vLLM
        chose.

        Returns:
            Object size in bytes, or None when the layout was unrecognized, so
            the caller keeps the default CXL page size rather than guessing.
        """
        if not self._kv_caches:
            return None
        # Normally resolved in register_kv_caches; resolve here as well so a
        # connector that had caches attached directly still sizes correctly.
        layout = self._kv_layout or self._resolve_kv_layout(self._kv_caches)
        if layout is None:
            return None
        layer = next(iter(self._kv_caches.values()))
        total_bytes = int(layer.numel()) * int(layer.element_size())
        per_token_bytes = total_bytes // layout.page_buffer_size
        return int(per_token_bytes * self._kv_chunk_tokens)

    def _await_stage(self, req_id: str) -> StageResult | None:
        """Join a request's preparation at its demand-read boundary.

        Session-hint staging keys the ticket by hint plan id; the arriving
        request resolves through its metadata-relayed alias.
        """
        with self._stage_lock:
            ticket_id = self._stage_aliases.get(req_id, req_id)
            ticket = self._stage_tickets.get(ticket_id)
        if ticket is None:
            return None
        wait_t0 = time.monotonic()
        result = ticket.wait()
        demand_wait_ms = (time.monotonic() - wait_t0) * 1000.0
        if result is None:
            logger.warning(
                "Maru stage not ready for req %s (%s); using demand read",
                req_id,
                ticket.error or ticket.state.value,
            )
            self._release_stage_ticket(req_id)
            return None
        logger.info(
            "Maru stage consumed for req %s: bytes=%d, stage_ms=%s, "
            "lead_ms=%s, demand_wait_ms=%.1f",
            req_id,
            result.prepared_bytes,
            f"{ticket.stage_ms:.1f}" if ticket.stage_ms is not None else "n/a",
            (
                f"{ticket.ready_age_ms:.1f}"
                if ticket.ready_age_ms is not None
                else "n/a"
            ),
            demand_wait_ms,
        )
        if self._timing:
            stage_ms = ticket.stage_ms
            ready_age_ms = ticket.ready_age_ms
            _emit_timing(
                "stage-consume "
                f"stage_ms={stage_ms:.2f} "
                f"lead_ms={ready_age_ms:.2f} "
                f"demand_wait_ms={demand_wait_ms:.2f} "
                f"(req {req_id})"
            )
        return result

    def _cancel_stage_requests(self, req_ids: set[str]) -> None:
        """Cancel or discard stage work for preempted requests."""
        if not req_ids:
            return
        with self._stage_lock:
            tickets = []
            for req_id in req_ids:
                ticket_id = self._stage_aliases.pop(req_id, req_id)
                tickets.append(self._stage_tickets.pop(ticket_id, None))
        for ticket in tickets:
            if ticket is not None:
                ticket.cancel()
                self._dispatch_stage_release(ticket)

    def _dispatch_stage_release(self, ticket: StageTicket) -> None:
        """Queue the device-pin release for a dropped ticket.

        Runs on the single-worker stage executor, which serializes it after
        any still-running stage of the same plan — so an unpin can never
        race the pin it undoes. No-op unless the pin lease is enabled.
        """
        if not self._stage_pin_enabled or self._stage_executor is None:
            return
        keys = list(ticket.plan.keys)

        def _release_job() -> None:
            handler = self._handler
            if handler is None:
                return
            try:
                handler.stage_release(keys)
            except Exception:
                logger.warning(
                    "Maru stage release failed (plan %s)",
                    ticket.plan.req_id,
                    exc_info=True,
                )

        try:
            self._stage_executor.submit(_release_job)
        except RuntimeError:
            # Executor already shut down; the plugin's on_close unpins
            # anything left as the final leak guard.
            pass

    def _emit_layerwise_timing(self) -> None:
        """Emit the layerwise overlap's two timelines (maru_log_timing only).

        ``kv-layer-transfer`` gives each layer's CXL->GPU transfer span on the
        load stream, on one axis per request. ``kv-layer-stall`` gives how long
        the forward's attention was held at each layer's load wait. Together
        they separate "the transfer was issued per layer" from "the transfer
        was actually hidden", which duration totals alone cannot.

        Called at a step boundary, so the events being read belong to work the
        device has already finished. Entries whose events have not completed
        are kept for a later call.
        """
        with self._deferred_lock:
            ready = [
                (req_id, entry)
                for req_id, entry in self._layerwise_spans.items()
                if entry[0].query()
            ]
            for req_id, _ in ready:
                del self._layerwise_spans[req_id]
        for req_id, (_, epoch, spans) in ready:
            for layer_idx, start, end, nbytes in spans:
                try:
                    start_ms = epoch.elapsed_time(start)
                    end_ms = epoch.elapsed_time(end)
                except Exception:
                    break
                _emit_timing(
                    "kv-layer-transfer "
                    f"layer={layer_idx} "
                    f"start_ms={start_ms:.3f} "
                    f"end_ms={end_ms:.3f} "
                    f"bytes={nbytes} "
                    f"(req {req_id})"
                )

        pending: list[tuple[str, torch.cuda.Event, torch.cuda.Event]] = []
        for layer_name, before, after in self._layer_wait_spans:
            if not after.query():
                pending.append((layer_name, before, after))
                continue
            try:
                stall_ms = before.elapsed_time(after)
            except Exception:
                continue
            _emit_timing(
                "kv-layer-stall "
                f"layer={self._get_layer_index(layer_name)} "
                f"stall_ms={stall_ms:.3f}"
            )
        self._layer_wait_spans = pending

    @staticmethod
    def _emit_load_bandwidth(
        req_id: str,
        bw_start: torch.cuda.Event,
        bw_end: torch.cuda.Event,
        nbytes: int,
        nchunks: int,
    ) -> None:
        """Emit one request's CXL->GPU transfer bandwidth.

        This is the tier signal smart-prefetch is judged on: the same bytes
        read out of device DRAM versus filled from SSD on demand differ by
        about a factor of two, and TTFT at concurrency is too noisy to resolve
        that. Reported per request so a partially warmed batch is visible as a
        spread rather than averaged away.
        """
        try:
            ms = bw_start.elapsed_time(bw_end)
        except Exception:
            return
        gbps = (nbytes / (ms / 1000.0)) / 1e9 if ms > 0 else 0.0
        _emit_timing(
            f"packed-load {nchunks} chunks {nbytes / 2**20:.0f} MiB in "
            f"{ms:.2f} ms = {gbps:.2f} GB/s (req {req_id})"
        )

    def _enqueue_packed_chunk(
        self,
        *,
        layers: list[tuple[str, torch.Tensor, int]],
        kernel: tuple | None,
        attn: Any,
        slab_view: memoryview,
        chunk_slots: torch.Tensor,
        dev: torch.device,
        use_stream: bool,
        dtype: torch.dtype,
        num_layers: int,
    ) -> None:
        """Enqueue one packed CXL object into its GPU KV-cache slots."""
        ct = self._kv_chunk_tokens
        slab_host = torch.frombuffer(slab_view, dtype=dtype).view(2, num_layers, ct, -1)
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
            return

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

    def _fire_arrival_hints(self, chunk_keys: list[str]) -> None:
        """Issue a lookahead prefetch for newly arrived requests' chunks.

        Fires one ``MaruHandler.prefetch_batch`` for the chunk keys relayed
        from the scheduler at arrival (a lookup-only call that lets a loaded
        device plugin start the SSD->DRAM fill). Packed storage keeps one
        object per chunk, so the chunk keys are fired as-is with no per-layer
        expansion. Keys not stored yet are skipped by the lookup, so hints for
        cold requests are a cheap no-op. Best-effort: a failure never disturbs
        the load path this runs ahead of.

        Args:
            chunk_keys: Chunk base keys relayed from the scheduler at arrival.
        """
        _t0 = time.monotonic()
        try:
            found = self._handler.prefetch_batch(chunk_keys)
            logger.info(
                "Maru arrival-hint: prefetch_batch found %d/%d chunk keys",
                found,
                len(chunk_keys),
            )
        except Exception:
            logger.warning("Maru arrival-hint prefetch failed", exc_info=True)
        if self._timing:
            # This runs on the engine thread inside start_load_kv, so whatever
            # it costs is added to the step -- and therefore to every
            # co-scheduled request's inter-token latency.
            _emit_timing(
                f"arrival-hint fire {len(chunk_keys)} keys = "
                f"{(time.monotonic() - _t0) * 1000:.2f} ms"
            )

    def _load_packed_hymcache(
        self,
        layers: list[tuple[str, torch.Tensor, int]],
        prepared_requests: list[tuple[MaruReqMeta, int, torch.Tensor, list[Any]]],
        attn_metadata: AttentionMetadata,
    ) -> None:
        """Run HyMCache's bounded two-stage pipeline with local CXL reads.

        Each request keeps an actual-byte rolling window of ordered packed KV
        objects. Objects ahead of the consumer are synchronously materialized
        from SSD into device DRAM on a helper thread while the current object
        is read from mapped CXL memory into the GPU KV cache. Each object is
        released immediately after its CUDA transfer completes and replaced
        at the tail, matching HyMCache's per-object prefetch/read-token
        lifecycle while substituting local CXL->GPU for RDMA GET.
        """
        attn = attn_metadata if attn_metadata is not None else self._last_attn_metadata
        num_layers = len(layers)
        ct = self._kv_chunk_tokens
        kernel = self._packed_load_kernel_ctx(layers, attn)
        dev = layers[0][1].device
        use_stream = kernel is not None or (
            dev.type == "cuda" and torch.cuda.is_available()
        )
        if use_stream:
            if self._load_stream is None or self._load_stream_device != dev:
                self._load_stream = torch.cuda.Stream(device=dev, priority=-1)
                self._load_stream_device = dev
            self._load_stream.wait_stream(torch.cuda.current_stream(dev))

        if self._hymcache_executor is None:
            self._hymcache_executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="maru-hymcache-stage",
            )
        pipeline = HymCacheRollingPipeline[StageResult](
            self._hymcache_executor,
            window_bytes=self._hymcache_window_bytes,
        )
        dtype = layers[0][1].dtype
        mode = "kernel" if kernel is not None else "fallback"

        request_objects: list[list[HymCacheObject]] = []
        contexts: dict[str, tuple[MaruReqMeta, torch.Tensor, list[Any]]] = {}
        gpu_ms: dict[tuple[str, int], float] = {}
        # Objects whose Stage 1 failed open and were therefore read on demand,
        # exactly like the W=0 baseline reads them.
        demand_read: set[tuple[str, int]] = set()
        for req_meta, num_chunks, slot_mapping, slab_infos in prepared_requests:
            if use_stream:
                slot_mapping = self._pin_slot_mapping_for_async_h2d(slot_mapping)
                assert self._load_stream is not None
                with torch.cuda.stream(self._load_stream):
                    slot_gpu = slot_mapping.to(dev, non_blocking=True)
            else:
                slot_gpu = slot_mapping.to(dev)

            keys = _req_chunk_keys(req_meta, self._kv_chunk_tokens)[:num_chunks]
            key_sizes: list[int] = []
            for info in slab_infos:
                view = info.view
                key_sizes.append(view.nbytes if hasattr(view, "nbytes") else len(view))
            objects = build_hymcache_objects(
                req_meta.req_id,
                keys,
                key_sizes,
            )
            if req_meta.req_id in contexts:
                raise ValueError(f"duplicate HyMCache request id: {req_meta.req_id}")
            contexts[req_meta.req_id] = (req_meta, slot_gpu, slab_infos)
            request_objects.append(objects)

        rolling_requests = 0
        tail_admissions = 0
        for objects in request_objects:
            initial_bytes = 0
            initial_objects = 0
            for obj in objects:
                if (
                    initial_bytes
                    and initial_bytes + obj.nbytes > self._hymcache_window_bytes
                ):
                    break
                initial_bytes += obj.nbytes
                initial_objects += 1
            tail_objects = len(objects) - initial_objects
            if tail_objects:
                rolling_requests += 1
                tail_admissions += tail_objects

        def _stage(obj: HymCacheObject) -> StageResult:
            handler = self._handler
            if handler is None:
                return StageResult(
                    requested_keys=1,
                    found_keys=0,
                    error="worker handler unavailable",
                )
            return handler.stage_batch([obj.key])

        def _consume(obj: HymCacheObject, result: StageResult) -> None:
            _, slot_gpu, slab_infos = contexts[obj.req_id]
            if not result.ready:
                demand_read.add((obj.req_id, obj.index))
                logger.warning(
                    "HyMCache-local stage failed open for req %s object %d: %s; "
                    "using demand CXL read",
                    obj.req_id,
                    obj.index,
                    result.error or "partial preparation",
                )

            if use_stream:
                assert self._load_stream is not None
                start_event = torch.cuda.Event(enable_timing=True)
                end_event = torch.cuda.Event(enable_timing=True)
                stream_ctx = torch.cuda.stream(self._load_stream)
            else:
                import contextlib

                start_event = None
                end_event = None
                stream_ctx = contextlib.nullcontext()

            with stream_ctx:
                if start_event is not None:
                    start_event.record()
                chunk_slots = slot_gpu[obj.index * ct : (obj.index + 1) * ct]
                self._enqueue_packed_chunk(
                    layers=layers,
                    kernel=kernel,
                    attn=attn,
                    slab_view=slab_infos[obj.index].view,
                    chunk_slots=chunk_slots,
                    dev=dev,
                    use_stream=use_stream,
                    dtype=dtype,
                    num_layers=num_layers,
                )
                if end_event is not None:
                    end_event.record()
            if end_event is not None:
                end_event.synchronize()
                assert start_event is not None
                gpu_ms[(obj.req_id, obj.index)] = start_event.elapsed_time(end_event)

        def _issue(obj: HymCacheObject) -> None:
            """Fire the non-blocking device hint that ``_stage`` then waits on.

            ``prefetch_batch`` resolves the key and hands it to the plugin's
            lookahead hook, which submits the migration without waiting for
            completion. Failures are not fatal: the blocking ``_stage`` that
            follows still brings the object in, just without the head start.
            """
            handler = self._handler
            if handler is None:
                return
            try:
                handler.prefetch_batch([obj.key])
            except Exception:
                logger.warning(
                    "HyMCache-local async issue failed for req %s object %d",
                    obj.req_id,
                    obj.index,
                    exc_info=True,
                )

        def _release(obj: HymCacheObject) -> None:
            handler = self._handler
            if handler is None:
                return
            try:
                handler.stage_release([obj.key])
            except Exception:
                logger.warning(
                    "HyMCache-local release failed for req %s object %d",
                    obj.req_id,
                    obj.index,
                    exc_info=True,
                )

        timings = pipeline.run(
            request_objects,
            stage=_stage,
            consume=_consume,
            release=_release,
            issue=_issue if self._hymcache_async_issue else None,
        )
        logger.info(
            "Maru: HyMCache-local rolling summary requests=%d "
            "rolling_requests=%d tail_admissions=%d max_objects=%d "
            "window_bytes=%d",
            len(request_objects),
            rolling_requests,
            tail_admissions,
            max(len(objects) for objects in request_objects),
            self._hymcache_window_bytes,
        )

        timings_by_req: dict[str, list[Any]] = {}
        for timing in timings:
            timings_by_req.setdefault(timing.object.req_id, []).append(timing)
        # One origin for the whole batch so spans from different requests are
        # comparable on the same axis.
        epoch = min((t.submitted_at for t in timings), default=0.0)

        for objects in request_objects:
            req_id = objects[0].req_id
            req_meta, _, _ = contexts[req_id]
            if req_meta.deferred_load:
                with self._deferred_lock:
                    self._deferred_done.add(req_meta.req_id)

            logger.info(
                "Maru: HyMCache-local loaded %d layers x %d objects "
                "for req %s (%s, window=%d bytes)",
                num_layers,
                len(objects),
                req_meta.req_id,
                mode,
                self._hymcache_window_bytes,
            )
            if self._timing:
                for timing in timings_by_req.get(req_id, []):
                    obj = timing.object
                    copy_ms = gpu_ms.get((req_id, obj.index), timing.consume_ms)
                    # Stage 1 and Stage 2 on one time axis, relative to the
                    # first submit in this batch. Durations alone cannot show
                    # whether the two lanes overlapped; these can.
                    _emit_timing(
                        "kv-object-span "
                        f"idx={obj.index}/{len(objects)} "
                        f"submit_ms={(timing.submitted_at - epoch) * 1000.0:.3f} "
                        f"stage_start_ms={(timing.stage_started_at - epoch) * 1000.0:.3f} "
                        f"stage_end_ms={(timing.stage_completed_at - epoch) * 1000.0:.3f} "
                        f"copy_start_ms={(timing.consume_started_at - epoch) * 1000.0:.3f} "
                        f"copy_end_ms={(timing.consume_completed_at - epoch) * 1000.0:.3f} "
                        f"queue_ms={timing.queue_ms:.3f} "
                        f"stage_ms={timing.stage_ms:.3f} "
                        f"ready_age_ms={timing.ready_age_ms:.3f} "
                        f"(req {req_meta.req_id})"
                    )
                    # Kept for the analysis scripts written against the
                    # 2026-08-11 campaign logs.
                    _emit_timing(
                        "hymcache-object "
                        f"idx={obj.index}/{len(objects)} "
                        f"bytes={obj.nbytes} "
                        f"demand_wait_ms={timing.demand_wait_ms:.2f} "
                        f"cxl_gpu_ms={copy_ms:.2f} "
                        f"(req {req_meta.req_id})"
                    )
                    # Shared shape: the demand-load path emits the same record
                    # so one parser reads either setting.
                    _emit_timing(
                        _format_kv_object_timing(
                            req_id=req_meta.req_id,
                            index=obj.index,
                            total=len(objects),
                            nbytes=obj.nbytes,
                            cxl_gpu_ms=copy_ms,
                            prefetched=(req_id, obj.index) not in demand_read,
                        )
                    )

    def _release_stage_ticket(self, req_id: str) -> None:
        """Release and remove a ticket after fallback or dependent H2D."""
        with self._stage_lock:
            ticket_id = self._stage_aliases.pop(req_id, req_id)
            ticket = self._stage_tickets.pop(ticket_id, None)
        if ticket is not None:
            ticket.release()
            self._dispatch_stage_release(ticket)

    def _run_stage(self, ticket: StageTicket) -> StageResult:
        """Run blocking device preparation on the dedicated stage thread."""
        ticket.mark_running()
        handler = self._handler
        if handler is None:
            return StageResult(
                requested_keys=len(ticket.plan.keys),
                found_keys=0,
                error="worker handler unavailable",
            )
        result = handler.stage_batch(list(ticket.plan.keys))
        if self._timing:
            _emit_timing(
                f"stage ready={result.ready} "
                f"{result.prepared_bytes / 2**20:.0f} MiB "
                f"in {result.wait_ms:.2f} ms t={time.time():.6f} "
                f"(req {ticket.plan.req_id}) "
                f"yield={result.yielded_ms:.0f} ms"
            )
        return result

    def _submit_stage_plan(self, plan: StagePlan) -> None:
        """Submit one plan to the SSD-to-DRAM worker without blocking vLLM."""
        if self._timing:
            _emit_timing(
                f"stage submit: plan={plan.req_id} keys={len(plan.keys)} "
                f"t={time.time():.6f}"
            )
        with self._stage_lock:
            if plan.req_id in self._stage_tickets:
                return
            if self._stage_executor is None:
                self._stage_executor = ThreadPoolExecutor(
                    max_workers=1,
                    thread_name_prefix="maru-im-stage",
                )
            ticket = StageTicket(plan)
            self._stage_tickets[plan.req_id] = ticket
            future = self._stage_executor.submit(self._run_stage, ticket)
            ticket.bind(future)
