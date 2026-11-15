# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruKVConnector on maru_node against the real MaruServer.

MaruServer runs as ``python -m maru_server.node_server`` and the StorageService
in-process; a scheduler (probe) and workers run in this process, as vLLM runs
them at TP=1. CPU tensors only.
"""

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import torch

from maru_common.kv_shard import WHOLE_MODEL, tp_kv_shard
from maru_common.protocol import LookupAndPinRequest, MessageType
from maru_common.storage_types import Key, SessionRole, Status
from maru_vllm.connector import (
    MaruConnectorMetadata,
    MaruReqMeta,
)
from tests.integration.test_ms_s2_real_server import RealMaruServer
from tests.unit.ms_node_helpers import (
    PAGE,
    node_config,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)
from tests.unit.test_vllm_tp import (
    BLOCK,
    CHUNK,
    HEAD_SIZE,
    LAYER_NAMES,
    NUM_BLOCKS,
    NUM_LAYERS,
    PROMPT,
    assert_same_kv,
    load_into,
    rank_kv,
)
from tests.unit.vllm_connector_helpers import (
    make_flash_attn_metadata,
    make_scheduler,
    make_worker,
)

pytestmark = pytest.mark.integration

BUDGETS = {
    "credit_max_bytes": 8 * PAGE,
    "credit_total_max_bytes": 16 * PAGE,
    "credit_refill_low": 2 * PAGE,
    "credit_refill_high": 8 * PAGE,
    "commit_pending_max_bytes": 8 * PAGE,
}


def extra(node, **kwargs):
    return {
        "maru_node": node.config.socket,
        "maru_engine_id": "eng",
        "maru_cache_namespace": "ns",
        **kwargs,
    }


@pytest.fixture
def engine():
    """A real MaruServer and node, and the connectors made on them."""
    workdir = short_tmpdir()
    server = RealMaruServer(workdir, ttl_s=6.0, heartbeat_s=0.5)
    node = start_node(
        node_config(server.url, f"{workdir}/node.sock", pages=32, budgets=BUDGETS),
        rpc_timeout_s=0.5,
    )
    made = []

    def worker(shard=WHOLE_MODEL, heads=2, tp_rank=0):
        w = make_worker(
            BLOCK,
            CHUNK,
            {**extra(node), "_maru_tp_rank": tp_rank},
            head_size=HEAD_SIZE,
            kv_shard=shard,
        )
        w._num_layers = NUM_LAYERS
        w._page_size_bytes = 2 * CHUNK * heads * HEAD_SIZE * 4 * NUM_LAYERS
        w._take_node_handler(wait=True)  # as register_kv_caches does
        assert w._handler is not None
        made.append(w)
        return w

    def scheduler(suffixes=("",)):
        s = make_scheduler(BLOCK, CHUNK, extra(node), shard_suffixes=suffixes)
        s.connect_node()  # as MaruKVConnector does at startup
        made.append(s)
        return s

    yield server, node, worker, scheduler
    for each in made:
        each.shutdown()
    node.stop()
    server.close()
    remove_tree(workdir)


def store(worker, kv) -> None:
    metadata = MaruConnectorMetadata(
        requests=[
            MaruReqMeta(
                req_id="r1",
                token_ids=list(range(PROMPT)),
                block_ids=list(range(NUM_BLOCKS)),
                is_store=True,
                num_scheduled_tokens=PROMPT,
            )
        ]
    )
    attn = make_flash_attn_metadata()
    for name, layer in zip(LAYER_NAMES, kv, strict=True):
        worker.save_kv_layer(name, layer, attn, metadata)
    worker._wait_node_stores(10)


def test_scheduler_probe_and_workers_share_one_node(engine):
    server, node, worker, scheduler = engine
    writer, reader = worker(), worker()
    sched = scheduler()
    tokens = list(range(PROMPT))
    assert sched._count_matched_chunks(tokens) == 0
    probe = sched._handler._cpu
    assert probe.role == SessionRole.PROBE and probe.pools.pool_ids() == []
    assert len(node.manager.workers.probes()) == 1

    kv = rank_kv(1, 2)
    store(writer, kv)
    assert sched._count_matched_chunks(tokens) == 2
    dst = [torch.zeros_like(layer) for layer in kv]
    load_into(reader, dst, make_flash_attn_metadata())
    assert_same_kv(dst, kv)
    assert reader.take_failed_load_blocks() == set()
    assert len(reader._handler._cpu._reads) == 0
    wait_until(
        lambda: reader._handler._cpu.server_conn.pending_unpins() == 0,
        what="unpins sent",
    )
    counters = sched._handler.get_stats()["maru_node"]["counters"]
    assert counters["probe_not_found"] == 2 and counters["probe_hit"] == 2


def test_a_probe_session_may_only_probe(engine):
    from maru_common.ms_rpc import PLACEHOLDER_OP

    server, node, worker, scheduler = engine
    sched = scheduler()
    sched._count_matched_chunks(list(range(PROMPT)))
    conn = sched._handler._cpu.server_conn
    request = LookupAndPinRequest(
        session=conn.session,
        server_epoch=conn.server_epoch,
        op=PLACEHOLDER_OP,
        items=[Key("ns", "k")],
    )
    response = conn._rpc.call(MessageType.LOOKUP_AND_PIN, conn._origin.stamp(request))
    assert response.status == Status.REJECTED


def test_ranks_of_one_engine_share_its_id(engine):
    server, node, worker, scheduler = engine
    shards = [tp_kv_shard(rank, 2, 4) for rank in range(2)]
    ranks = [worker(shard, heads=2, tp_rank=r) for r, shard in enumerate(shards)]
    kvs = [rank_kv(r + 1, 2) for r in range(2)]
    for rank, kv in zip(ranks, kvs, strict=True):
        store(rank, kv)
    sched = scheduler(tuple(s.suffix for s in shards))
    assert sched._count_matched_chunks(list(range(PROMPT))) == 2
    for r, rank in enumerate(ranks):
        ident = node.manager.workers.get(rank._handler._cpu.identity.incarnation).ident
        assert (ident.engine_id, ident.tp_rank) == ("eng", r)
        dst = [torch.zeros_like(layer) for layer in kvs[r]]
        load_into(rank, dst, make_flash_attn_metadata())
        assert_same_kv(dst, kvs[r])


def test_a_stopped_maruserver_is_unavailable_then_hits_again(engine):
    server, node, worker, scheduler = engine
    writer, reader = worker(), worker()
    sched = scheduler()
    kv = rank_kv(1, 2)
    store(writer, kv)
    tokens = list(range(PROMPT))
    assert sched._count_matched_chunks(tokens) == 2  # opens the probe
    server.pause()
    try:
        assert sched._count_matched_chunks(tokens) == 0
        load_into(reader, [torch.zeros_like(layer) for layer in kv], None)
        assert reader.take_failed_load_blocks() == set(range(NUM_BLOCKS))
    finally:
        server.resume()
    probe = sched._handler.get_stats()["maru_node"]["counters"]
    assert probe["probe_unavailable"] >= 1
    worker_counters = reader._handler.get_stats()["maru_node"]["counters"]
    assert worker_counters["retrieve_unavailable"] >= 1
    wait_until(
        lambda: sched._count_matched_chunks(tokens) == 2,
        timeout=30,
        what="hits after MaruServer resumed",
    )
    dst = [torch.zeros_like(layer) for layer in kv]
    load_into(reader, dst, make_flash_attn_metadata())
    assert_same_kv(dst, kv)
