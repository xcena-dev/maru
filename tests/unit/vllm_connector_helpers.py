# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Shared scaffolding for tests/unit/test_vllm_connector.py.

Holds the factories that several test classes need: connector instances,
the Flash attention metadata stand-in, request metadata, a handler mock
that captures stored objects, and the vLLM scheduler-output stand-ins.

torch and maru_vllm.connector are imported inside the functions that need
them, so importing this module never requires torch to be installed.
"""

from __future__ import annotations

from collections.abc import Callable
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

if TYPE_CHECKING:
    import torch

    from maru_vllm.connector import (
        MaruConnectorMetadata,
        MaruReqMeta,
        MaruSchedulerConnector,
        MaruWorkerConnector,
    )


# =============================================================================
# Connector factories
# =============================================================================


def make_worker(
    block_size: int,
    kv_chunk_tokens: int,
    extra_config: dict[str, Any] | None = None,
    num_kv_heads: int | None = None,
    head_size: int | None = None,
) -> MaruWorkerConnector:
    """Construct a MaruWorkerConnector (no CXL hardware or GPU required)."""
    from maru_vllm.connector import MaruWorkerConnector

    return MaruWorkerConnector(
        block_size=block_size,
        kv_chunk_tokens=kv_chunk_tokens,
        extra_config={} if extra_config is None else extra_config,
        num_kv_heads=num_kv_heads,
        head_size=head_size,
    )


def make_bare_worker(
    block_size: int = 16,
    kv_caches: dict[str, Any] | None = None,
    num_kv_heads: int | None = None,
    head_size: int | None = None,
) -> MaruWorkerConnector:
    """Allocate a MaruWorkerConnector without running __init__.

    For tests of pure layout helpers that need only ``_block_size`` (and
    optionally ``_kv_caches``) rather than a fully wired connector.
    """
    from maru_vllm.connector import MaruWorkerConnector

    worker = MaruWorkerConnector.__new__(MaruWorkerConnector)
    worker._block_size = block_size
    worker._kv_caches = {}
    worker._num_kv_heads = num_kv_heads
    worker._head_size = head_size
    # Resolved in register_kv_caches on a real connector; do it here so layout
    # aware helpers see the same value they would in production.
    worker._kv_layout = None
    if kv_caches is not None:
        worker._kv_caches = kv_caches
        worker._kv_layout = worker._resolve_kv_layout(kv_caches)
    return worker


def make_scheduler(
    block_size: int,
    kv_chunk_tokens: int,
    extra_config: dict[str, Any] | None = None,
) -> MaruSchedulerConnector:
    """Construct a MaruSchedulerConnector."""
    from maru_vllm.connector import MaruSchedulerConnector

    return MaruSchedulerConnector(
        block_size=block_size,
        kv_chunk_tokens=kv_chunk_tokens,
        extra_config={} if extra_config is None else extra_config,
    )


def make_flash_attn_metadata() -> MagicMock:
    """attn_metadata that routes to the Flash attention layout branch.

    The connector dispatches on the metadata class name, so a MagicMock
    reclassed as ``FlashMetadata`` selects Flash (not MLA or Triton).
    """
    attn_metadata = MagicMock()
    attn_metadata.__class__ = type("FlashMetadata", (), {})
    return attn_metadata


# =============================================================================
# Handler mock that captures stored objects
# =============================================================================


def capture_bytes(buf: Any) -> bytes:
    """Capture a stored slab as raw bytes."""
    return bytes(buf)


def capture_float32(buf: Any) -> torch.Tensor:
    """Capture a stored slab as a detached float32 tensor."""
    import torch

    return torch.frombuffer(bytes(buf), dtype=torch.float32).clone()


def attach_capturing_handler(
    worker: MaruWorkerConnector,
    capture: Callable[[Any], Any] = capture_bytes,
    min_alloc_bytes: int = 0,
) -> dict[str, Any]:
    """Give ``worker`` a handler mock backed by an in-memory store dict.

    ``alloc`` hands out ``SimpleNamespace(buf=memoryview(bytearray(...)))``
    handles and ``batch_store`` records ``capture(handle.buf)`` under each
    key. Returns the dict the stores land in.
    """

    store: dict[str, Any] = {}

    def _alloc(nbytes):
        return SimpleNamespace(buf=memoryview(bytearray(max(nbytes, min_alloc_bytes))))

    def _batch_store(keys, handles):
        for key, handle in zip(keys, handles, strict=True):
            store[key] = capture(handle.buf)
        return [True] * len(keys)

    worker._handler = MagicMock()
    worker._handler.alloc.side_effect = _alloc
    worker._handler.batch_store.side_effect = _batch_store
    return store


# =============================================================================
# Request metadata builders
# =============================================================================


def store_req_meta(
    *,
    token_ids: list[int],
    block_ids: list[int],
    num_scheduled_tokens: int,
    req_id: str = "r1",
    num_computed_tokens: int = 0,
) -> MaruReqMeta:
    """Store-side request metadata (``is_store=True``)."""
    from maru_vllm.connector import MaruReqMeta

    return MaruReqMeta(
        req_id=req_id,
        token_ids=token_ids,
        block_ids=block_ids,
        is_store=True,
        num_scheduled_tokens=num_scheduled_tokens,
        num_computed_tokens=num_computed_tokens,
    )


def store_metadata(**kwargs: Any) -> MaruConnectorMetadata:
    """Single-request store metadata; kwargs go to :func:`store_req_meta`."""
    from maru_vllm.connector import MaruConnectorMetadata

    return MaruConnectorMetadata(requests=[store_req_meta(**kwargs)])


def deferred_req_meta(
    *,
    token_ids: list[int],
    block_ids: list[int],
    num_matched_chunks: int,
    req_id: str = "r1",
    deferred_load: bool = True,
) -> MaruReqMeta:
    """Load-side request metadata for a deferred (between-step) load."""
    from maru_vllm.connector import MaruReqMeta

    return MaruReqMeta(
        req_id=req_id,
        token_ids=token_ids,
        block_ids=block_ids,
        is_store=False,
        num_matched_chunks=num_matched_chunks,
        deferred_load=deferred_load,
    )


def deferred_metadata(**kwargs: Any) -> MaruConnectorMetadata:
    """Single-request load metadata; kwargs go to :func:`deferred_req_meta`."""
    from maru_vllm.connector import MaruConnectorMetadata

    return MaruConnectorMetadata(requests=[deferred_req_meta(**kwargs)])


# =============================================================================
# vLLM scheduler stand-ins
# =============================================================================


def fake_cached_reqs(
    req_ids: Any = (),
    new_block_ids: Any = (),
    num_computed_tokens: Any = (),
    resumed: Any = (),
) -> SimpleNamespace:
    """Stand-in for SchedulerOutput.scheduled_cached_reqs."""
    return SimpleNamespace(
        req_ids=list(req_ids),
        new_block_ids=list(new_block_ids),
        num_computed_tokens=list(num_computed_tokens),
        resumed_req_ids=set(resumed),
    )


def fake_scheduler_output(
    new_reqs: Any = (),
    cached: SimpleNamespace | None = None,
    num_scheduled_tokens: dict[str, int] | None = None,
) -> SimpleNamespace:
    """Stand-in for a vLLM SchedulerOutput with no finished/preempted ids."""
    return SimpleNamespace(
        scheduled_new_reqs=list(new_reqs),
        scheduled_cached_reqs=fake_cached_reqs() if cached is None else cached,
        num_scheduled_tokens=dict(num_scheduled_tokens or {}),
        finished_req_ids=set(),
        preempted_req_ids=set(),
    )


def fake_new_request_data(
    prompt_token_ids: list[int],
    block_ids: list[int],
    req_id: str = "r1",
) -> SimpleNamespace:
    """Stand-in for a SchedulerOutput.scheduled_new_reqs entry.

    ``block_ids`` is wrapped in the single-KV-group tuple vLLM passes.
    """
    return SimpleNamespace(
        req_id=req_id,
        prompt_token_ids=prompt_token_ids,
        block_ids=(list(block_ids),),
    )
