# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""A chunk whose object another client deleted is stored again (#110 C).

The worker skips the chunks whose keys it stored (``_stored_keys``). That set
does not learn of a delete by another client, or of an eviction by MaruServer,
so the keys a store skipped are checked with MaruServer after the step.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import torch

from maru_vllm.connector import _chunk_keys
from tests.unit.vllm_connector_helpers import (
    attach_capturing_handler,
    fake_new_request_data,
    fake_scheduler_output,
    make_flash_attn_metadata,
    make_scheduler,
    make_worker,
)

BLOCK = 4
CHUNK = 8
TOKENS = 32
LAYER = "model.layers.0.self_attn"


def _engine():
    """A scheduler and a worker over one in-memory store, sync stores."""
    extra = {"maru_async_store": False}
    scheduler = make_scheduler(BLOCK, CHUNK, extra)
    worker = make_worker(BLOCK, CHUNK, extra)
    kv = torch.arange(2 * 12 * BLOCK, dtype=torch.float32).reshape(2, 12, BLOCK, 1)
    worker._kv_caches = {LAYER: kv}
    worker._num_layers = 1
    worker._kv_layout = worker._resolve_kv_layout(worker._kv_caches)
    stored = attach_capturing_handler(worker, capture=bytearray)
    worker._handler.batch_exists.side_effect = lambda keys, strict=False: [
        k in stored for k in keys
    ]
    scheduler._handler = MagicMock()
    scheduler._handler.batch_exists.side_effect = lambda keys: [
        k in stored for k in keys
    ]
    return scheduler, worker, kv, stored


def _compute(scheduler, worker, kv, rid):
    """Serve the prompt from scratch as request ``rid``: one worker step."""
    tokens = list(range(TOKENS))
    request = SimpleNamespace(request_id=rid, prompt_token_ids=tokens)
    scheduler._known_keys.clear()
    matched, _ = scheduler.get_num_new_matched_tokens(request, 0)
    new = fake_new_request_data(
        prompt_token_ids=tokens,
        block_ids=list(range(TOKENS // BLOCK)),
        num_computed_tokens=0,
        req_id=rid,
    )
    meta = scheduler.build_connector_meta(
        fake_scheduler_output([new], None, {rid: TOKENS})
    )
    worker.save_kv_layer(LAYER, kv, make_flash_attn_metadata(), meta)
    worker.confirm_skipped_stores()  # As get_finished() does after each step.
    return matched


def test_a_deleted_chunk_is_stored_again_when_next_computed():
    scheduler, worker, kv, stored = _engine()
    keys = _chunk_keys(list(range(TOKENS)), CHUNK)
    assert _compute(scheduler, worker, kv, "r1") == 0
    assert set(stored) == set(keys)
    first = bytes(stored[keys[0]])

    del stored[keys[0]]  # Another client deletes the first chunk.

    # The lookup finds nothing, so the prompt is computed again. The worker
    # still skips the chunk in this step, and learns after it that it is gone.
    assert _compute(scheduler, worker, kv, "r2") == 0
    assert keys[0] not in stored
    assert keys[0] not in worker._stored_keys
    assert set(keys[1:]) <= worker._stored_keys

    assert _compute(scheduler, worker, kv, "r3") == 0
    assert bytes(stored[keys[0]]) == first
    worker.shutdown()


def test_stored_keys_are_not_asked_about_when_nothing_was_skipped():
    scheduler, worker, kv, stored = _engine()
    assert _compute(scheduler, worker, kv, "r1") == 0
    worker._handler.batch_exists.assert_not_called()
    worker.shutdown()


def test_a_skipped_key_the_server_still_has_stays_known():
    scheduler, worker, kv, stored = _engine()
    keys = _chunk_keys(list(range(TOKENS)), CHUNK)
    _compute(scheduler, worker, kv, "r1")
    worker._is_stored(keys[1])
    worker.confirm_skipped_stores()
    worker._handler.batch_exists.assert_called_once_with([keys[1]], strict=True)
    assert keys[1] in worker._stored_keys
    worker.confirm_skipped_stores()  # Nothing skipped since: no request.
    assert worker._handler.batch_exists.call_count == 1
    worker.shutdown()


def test_skipped_keys_are_kept_when_the_server_cannot_be_asked():
    scheduler, worker, kv, stored = _engine()
    keys = _chunk_keys(list(range(TOKENS)), CHUNK)
    _compute(scheduler, worker, kv, "r1")
    known = set(worker._stored_keys)
    worker._handler.batch_exists.side_effect = ConnectionError("MaruServer is down")

    _compute(scheduler, worker, kv, "r2")  # Every chunk is skipped, then asked.

    worker._handler.batch_exists.assert_called_once_with(sorted(keys), strict=True)
    assert worker._stored_keys == known
    worker.shutdown()
