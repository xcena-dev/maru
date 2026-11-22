# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Real vLLM recomputes a chunk removed between PROBE and an L1 load."""

import gc
import logging
import os
import random
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import torch

from maru_common.storage_types import CloseMode, ItemResult, KeyLocation
from maru_storage_service.pool_manager import PageState
from maru_vllm.connector import _chunk_keys
from tests.integration.ms_l1_helpers import STORES, l1_config
from tests.integration.test_ms_s2_real_server import RealMaruServer
from tests.unit.ms_node_helpers import (
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)

pytestmark = pytest.mark.integration

MODEL = os.environ.get("MARU_TEST_VLLM_MODEL", "Qwen/Qwen2.5-0.5B")
NS = "e2e-miss"
CHUNK = 32  # tokens; a load reads 8 chunks a batch
CHUNKS = 12
PROMPT = CHUNKS * CHUNK + 5
MAX_TOKENS = 48


def _model_path() -> str:
    if Path(MODEL).is_dir():
        return MODEL
    from huggingface_hub import snapshot_download

    try:
        return snapshot_download(MODEL, local_files_only=True)
    except Exception as e:
        pytest.skip(f"{MODEL} not in the local HF cache: {e}")


def _page_bytes(path: str) -> int:
    """One chunk of every layer's K and V, bf16."""
    from transformers import AutoConfig

    config = AutoConfig.from_pretrained(path)
    heads = config.num_key_value_heads
    head_dim = getattr(config, "head_dim", None) or (
        config.hidden_size // config.num_attention_heads
    )
    raw = 2 * CHUNK * heads * head_dim * 2 * config.num_hidden_layers
    return -(-raw // 4096) * 4096


class Records(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.lines: list[str] = []

    def emit(self, record):
        self.lines.append(record.getMessage())


class Engine:
    """vLLM with MaruKVConnector on a node, and what its connectors did."""

    def __init__(self, llm, node, records: Records):
        from vllm.distributed.kv_transfer import get_kv_transfer_group

        self.llm, self.node, self.records = llm, node, records
        self.scheduler = llm.llm_engine.engine_core.engine_core.scheduler.connector
        self.worker = get_kv_transfer_group()
        assert self.scheduler._scheduler is not None
        assert self.worker._worker is not None
        self.matched: list[tuple[str, int]] = []
        self.load_errors: list[set[int]] = []
        self.recving: list[str] = []
        self.doom: dict | None = None
        self._wrap()

    def _wrap(self) -> None:
        sched, worker = self.scheduler, self.worker
        matched = sched.get_num_new_matched_tokens
        errors = worker.get_block_ids_with_load_errors
        finished = worker.get_finished

        def on_match(request, num_computed_tokens):
            result = matched(request, num_computed_tokens)
            self.matched.append((request.request_id, result[0] or 0))
            doom = self.doom
            if (
                doom is not None
                and "outcome" not in doom
                and result[0]
                and list(request.prompt_token_ids) == doom["ids"]
            ):
                # After the PROBE, before the worker's load
                doom["req_id"] = request.request_id
                doom["outcome"] = retire(self.node, doom["key"])
            return result

        def on_errors():
            blocks = errors()
            if blocks:
                self.load_errors.append(set(blocks))
            return blocks

        def on_finished(finished_req_ids):
            sending, recving = finished(finished_req_ids)
            self.recving.extend(recving or ())
            return sending, recving

        sched.get_num_new_matched_tokens = on_match
        worker.get_block_ids_with_load_errors = on_errors
        worker.get_finished = on_finished

    def generate(self, ids: list[int]):
        from vllm import SamplingParams
        from vllm.inputs import TokensPrompt

        params = SamplingParams(temperature=0.0, max_tokens=MAX_TOKENS)
        (out,) = self.llm.generate(
            [TokensPrompt(prompt_token_ids=ids)], params, use_tqdm=False
        )
        return out

    def settle(self) -> None:
        """Stores answered, reads released, unpins sent."""
        self.worker._worker._wait_node_stores(10)
        for handler in (
            self.worker._worker._handler,
            self.scheduler._scheduler._handler,
        ):
            client = handler._cpu
            wait_until(lambda c=client: not c._reads, what="reads released")
            wait_until(
                lambda c=client: c.server_conn.pending_unpins() == 0,
                what="unpins answered",
            )


def ready_page(node, key: str):
    """Find a chunk in the connector's hashed namespace."""
    for pool_id in node.pools.pool_ids():
        for ref in node.pools.pages_in(pool_id, PageState.READY):
            page = node.pools.page(ref)
            if page is not None and page.key is not None and page.key.key == key:
                return ref, page
    return None


def retire(node, key: str):
    """Retire one chunk and return its outcome."""
    found = ready_page(node, key)
    assert found is not None, key
    ref, page = found
    (outcome,) = node.manager.retire([ref], [KeyLocation(page.key, page.location)])
    return outcome


def prompt(seed: int, tokenizer) -> list[int]:
    words = (
        "river stone lantern harvest copper meadow signal orchard winter canal "
        "engine ledger valley thunder paper garden market bridge forest tide"
    ).split()
    rng = random.Random(seed)
    text = []
    while len(tokenizer.encode(" ".join(text))) < PROMPT + 16:
        text.append(f"The {rng.choice(words)} near the {rng.choice(words)} was")
        text.append(f"{rng.choice(words)} and {rng.choice(words)}.")
    return tokenizer.encode(" ".join(text))[:PROMPT]


@pytest.fixture(scope="module")
def workdir():
    path = short_tmpdir()
    yield path
    remove_tree(path)


@pytest.fixture(scope="module", params=STORES)
def engine(request, workdir):
    if not torch.cuda.is_available():
        pytest.skip("needs a GPU")
    path = _model_path()
    page = _page_bytes(path)
    budgets = {
        "credit_max_bytes": 32 * page,
        "credit_total_max_bytes": 64 * page,
        "credit_refill_low": 8 * page,
        "credit_refill_high": 32 * page,
        "commit_pending_max_bytes": 32 * page,
    }
    config = l1_config(
        "tcp://127.0.0.1:1",
        f"{workdir}/node.sock",
        store=request.param,
        page_size=page,
        pages=256,
        budgets=budgets,
    )
    with pytest.MonkeyPatch.context() as patch, ExitStack() as cleanup:
        patch.setenv("VLLM_ENABLE_V1_MULTIPROCESSING", "0")
        patch.setenv("HF_HUB_OFFLINE", "1")
        server = RealMaruServer(workdir, ttl_s=6.0, heartbeat_s=0.5)
        cleanup.callback(server.close)
        node = start_node(replace(config, server=server.url), rpc_timeout_s=0.5)
        cleanup.callback(node.stop, CloseMode.LEAVE)
        llm = None
        records = Records()
        connector_log = logging.getLogger("maru_vllm.connector")
        connector_log.addHandler(records)
        try:
            from vllm import LLM
            from vllm.config import KVTransferConfig

            llm = LLM(
                model=path,
                enforce_eager=True,
                gpu_memory_utilization=0.3,
                max_model_len=2048,
                enable_prefix_caching=False,  # every hit comes from Maru
                seed=0,
                kv_transfer_config=KVTransferConfig(
                    kv_connector="MaruKVConnector",
                    kv_connector_module_path="maru_vllm",
                    kv_role="kv_both",
                    kv_load_failure_policy="recompute",
                    kv_connector_extra_config={
                        "maru_node": node.config.socket,
                        "maru_engine_id": "e2e",
                        "maru_cache_namespace": NS,
                        "maru_kv_chunk_tokens": CHUNK,
                    },
                ),
            )
            yield Engine(llm, node, records)
        finally:
            connector_log.removeHandler(records)
            if llm is not None:
                llm.llm_engine.engine_core.shutdown()
                del llm
            gc.collect()
            try:
                from vllm.distributed.parallel_state import (
                    destroy_distributed_environment,
                    destroy_model_parallel,
                )

                destroy_model_parallel()
                destroy_distributed_environment()
            except Exception:
                pass
            torch.cuda.empty_cache()


def stored(engine, keys: list[str]) -> None:
    engine.settle()
    wait_until(
        lambda: all(ready_page(engine.node, k) for k in keys), what="chunks stored"
    )


@pytest.mark.parametrize(
    "doomed",
    [0, CHUNKS // 2, 10],
    ids=["first_chunk", "middle_chunk", "second_batch_chunk"],
)
def test_a_chunk_retired_after_the_probe_is_recomputed(engine, doomed):
    ids = prompt(100 + doomed, engine.llm.get_tokenizer())
    keys = _chunk_keys(ids, CHUNK)
    assert len(keys) == CHUNKS
    # vLLM recomputes from the first chunk of the batch that missed
    first = doomed // 8 * 8

    # Nothing stored: all computed (the recompute-only run); all stored
    seen = len(engine.matched)
    recomputed = engine.generate(ids)
    assert all(m == 0 for _, m in engine.matched[seen:])
    stored(engine, keys)
    expected = recomputed
    if first:
        # The chunks before that batch loaded and the rest computed, with
        # no miss: hit numerics differ from a full prefill in bf16
        for key in keys[first:]:
            assert retire(engine.node, key).result == ItemResult.RETIRED
        seen = len(engine.matched)
        expected = engine.generate(ids)
        assert [m for _, m in engine.matched[seen:] if m] == [first * CHUNK]
        stored(engine, keys)

    engine.doom = {"ids": ids, "key": keys[doomed]}
    lines, errors = len(engine.records.lines), len(engine.load_errors)
    recving = len(engine.recving)
    try:
        out = engine.generate(ids)
    finally:
        doom, engine.doom = engine.doom, None
    assert doom["outcome"].result == ItemResult.RETIRED
    rid = doom["req_id"]
    # Loaded once: a load of every chunk, none asked again
    assert [m for r, m in engine.matched if r == rid and m] == [CHUNKS * CHUNK]

    assert list(out.outputs[0].token_ids) == list(expected.outputs[0].token_ids)

    # One load failure, with its reason, reported once
    failed = [
        line
        for line in engine.records.lines[lines:]
        if line.startswith(f"Maru load failed for req {rid} ")
    ]
    assert len(failed) == 1, engine.records.lines[lines:]
    assert "{'lookup:absent': 1}" in failed[0], failed[0]
    reported = engine.load_errors[errors:]
    assert len(reported) == 1
    assert len(reported[0]) == (CHUNKS - first) * CHUNK // 16  # 16-token blocks
    assert engine.recving[recving:].count(rid) == 1
    assert engine.worker._worker._stopped is None

    # Nothing held: no read in either client, no pin in MaruServer
    engine.settle()
    for key in keys:
        if ready_page(engine.node, key) is not None:
            assert retire(engine.node, key).result == ItemResult.RETIRED, key
    engine.node.pools.check()
