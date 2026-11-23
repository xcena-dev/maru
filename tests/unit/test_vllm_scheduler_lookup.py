# SPDX-License-Identifier: Apache-2.0
"""Scheduler lookup costs and GPU-prefix store bookkeeping."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

from maru_vllm import connector
from maru_vllm.kv_groups import GROUP_FULL, GROUP_MAMBA, KVGroup
from tests.unit.vllm_connector_helpers import (
    fake_cached_reqs,
    fake_new_request_data,
    fake_scheduler_output,
    make_scheduler,
)

CONFIGS = [
    pytest.param({}, id="cxl"),
    pytest.param(
        {"maru_node": "/unused", "maru_engine_id": "e", "maru_cache_namespace": "ns"},
        id="node",
    ),
]
GROUPS = [
    KVGroup(0, GROUP_FULL, "Full", ("a",), 4, 4),
    KVGroup(1, GROUP_MAMBA, "Mamba", ("b",), 4, 4),
]


def scheduler(config, **kwargs):
    sched = make_scheduler(4, 8, dict(config), **kwargs)
    sched._handler = Mock()
    sched._handler.batch_exists.side_effect = lambda keys, **_: [True] * len(keys)
    return sched


def request(tokens=40, req_id="r"):
    return SimpleNamespace(
        request_id=req_id, prompt_token_ids=list(range(tokens)), num_preemptions=0
    )


def step(sched, req, computed, scheduled, *, resumed=False):
    blocks = list(range(-(-len(req.prompt_token_ids) // 4)))
    if resumed:
        output = fake_scheduler_output(
            [],
            fake_cached_reqs(
                [req.request_id], [(blocks,)], [computed], [req.request_id]
            ),
            {req.request_id: scheduled},
        )
    else:
        new = fake_new_request_data(
            req.prompt_token_ids,
            blocks,
            req_id=req.request_id,
            num_computed_tokens=computed,
        )
        output = fake_scheduler_output([new], None, {req.request_id: scheduled})
    return sched.build_connector_meta(output)


@pytest.mark.parametrize("config", CONFIGS)
@pytest.mark.parametrize("groups,computed", [((), 36), (GROUPS, 32)])
def test_gpu_prefix_skips_hashes_and_probe(config, groups, computed, monkeypatch):
    sched = scheduler(config, kv_groups=groups)
    monkeypatch.setattr(
        connector, "_chunk_keys", Mock(side_effect=AssertionError("hash"))
    )
    req = request()
    assert sched.get_num_new_matched_tokens(req, computed) == (0, False)
    assert sched.get_num_new_matched_tokens(req, computed) == (0, False)
    assert not sched._maru_hits and not sched._last_match_result
    sched._handler.batch_exists.assert_not_called()


@pytest.mark.parametrize(
    "tokens,computed,matched", [(40, 32, 4), (41, 36, 4), (39, 28, 4)]
)
def test_prefix_short_of_reusable_range_still_probes(tokens, computed, matched):
    sched = scheduler(CONFIGS[-1].values[0])
    assert sched.get_num_new_matched_tokens(request(tokens), computed) == (
        matched,
        False,
    )
    assert sched._handler.batch_exists.call_count == 1


@pytest.mark.parametrize("config", CONFIGS)
@pytest.mark.parametrize("groups", [(), GROUPS])
def test_repeated_request_reuses_hashes_across_passes_and_preemption(
    config, groups, monkeypatch
):
    sched = scheduler(config, kv_groups=groups)
    hashes = Mock(wraps=connector._chunk_keys)
    monkeypatch.setattr(connector, "_chunk_keys", hashes)
    req = request()
    assert sched.get_num_new_matched_tokens(req, 0)[0] > 0
    assert sched.get_num_new_matched_tokens(req, 0)[0] > 0
    output = fake_scheduler_output([], None, {})
    output.preempted_req_ids = {req.request_id}
    sched.build_connector_meta(output)
    req.num_preemptions += 1
    assert sched.get_num_new_matched_tokens(req, 0)[0] > 0
    assert hashes.call_count == 1
    if sched._node_mode:
        assert sched._handler.batch_exists.call_count == (2 if sched._node_mode else 3)
    sched.request_finished(req, [])
    assert req.request_id not in sched._request_hashes


@pytest.mark.parametrize("config", CONFIGS)
@pytest.mark.parametrize("hits,expected", [(0, 0), (2, 16), (5, 40)])
def test_gpu_hit_checks_residency_for_stores(config, hits, expected):
    sched = scheduler(config)
    sched._handler.batch_exists.side_effect = lambda keys, **_: [
        i < hits for i in range(len(keys))
    ]
    req = request()
    assert sched.get_num_new_matched_tokens(req, 36) == (0, False)
    sched._handler.batch_exists.assert_not_called()
    sched.update_state_after_alloc(req, None, 0)
    stores = step(sched, req, 36, 4).requests
    if expected == 40:
        assert stores == []
    else:
        assert len(stores) == 1 and stores[0].is_store
        assert stores[0].store_start_token == expected
        assert stores[0].store_cached_prefix
    assert sched._handler.batch_exists.call_count == 1
    assert not sched._store_lookups and not sched._probe_results


@pytest.mark.parametrize("config", CONFIGS)
@pytest.mark.parametrize("resumed", [False, True])
def test_gpu_hit_after_preemption_keeps_original_store_start(config, resumed):
    sched = scheduler(config)
    req = request()
    sched._handler.batch_exists.side_effect = lambda keys, **_: [False] * len(keys)
    assert sched.get_num_new_matched_tokens(req, 0) == (0, False)
    step(sched, req, 0, 8)
    output = fake_scheduler_output([], None, {})
    output.preempted_req_ids = {req.request_id}
    sched.build_connector_meta(output)
    req.num_preemptions += 1
    assert sched.get_num_new_matched_tokens(req, 36) == (0, False)
    (store,) = step(sched, req, 36, 4, resumed=resumed).requests
    assert store.is_store and store.store_start_token == 0
    assert not sched._requests_need_store


def test_same_pass_probes_are_shared_but_namespace_and_key_order_are_distinct():
    sched = scheduler(CONFIGS[-1].values[0])
    assert sched._stored_keys(["a", "b"]) == {"a", "b"}
    assert sched._stored_keys(["a", "b"]) == {"a", "b"}
    assert sched._handler.batch_exists.call_count == 1
    sched._stored_keys(["b", "a"])
    sched._extra_config["maru_cache_namespace"] = "other"
    sched._stored_keys(["a", "b"])
    assert sched._handler.batch_exists.call_count == 3
    sched.build_connector_meta(fake_scheduler_output([], None, {}))
    sched._handler.batch_exists.side_effect = lambda keys, **_: [False] * len(keys)
    assert sched._stored_keys(["a", "b"]) == set()
    assert sched._handler.batch_exists.call_count == 4


@pytest.mark.parametrize("same_pass", [False, True])
def test_eight_requests_share_hashes_and_probe_without_inventing_store_hits(
    monkeypatch, same_pass
):
    sched = scheduler(CONFIGS[-1].values[0])
    hashes = Mock(wraps=connector._chunk_keys)
    monkeypatch.setattr(connector, "_chunk_keys", hashes)
    first = request(req_id="first")
    assert sched.get_num_new_matched_tokens(first, 0) == (36, False)
    if not same_pass:
        step(sched, first, 36, 4)
    followers = [request(req_id=f"r{i}") for i in range(7)]
    for req in followers:
        assert sched.get_num_new_matched_tokens(req, 36) == (0, False)
    assert hashes.call_count == 1
    assert sched._handler.batch_exists.call_count == 1
    output = fake_scheduler_output(
        [
            fake_new_request_data(
                req.prompt_token_ids,
                list(range(10)),
                req_id=req.request_id,
                num_computed_tokens=36,
            )
            for req in followers
        ],
        None,
        {req.request_id: 4 for req in followers},
    )
    assert sched.build_connector_meta(output).requests == []
    assert hashes.call_count == (1 if same_pass else 2)
    assert sched._handler.batch_exists.call_count == (1 if same_pass else 2)
    assert not sched._pass_hashes and not sched._probe_results


def test_failure_report_discards_residency_hints_in_the_same_pass():
    sched = scheduler(CONFIGS[-1].values[0])
    assert sched._stored_keys(["a"]) == {"a"}
    sched.update_connector_output(
        SimpleNamespace(finished_recving=set(), failed_recving={"r"})
    )
    sched._handler.batch_exists.side_effect = lambda keys, **_: [False] * len(keys)
    assert sched._stored_keys(["a"]) == set()
    assert sched._handler.batch_exists.call_count == 2


@pytest.mark.parametrize("answer", [[], [False], RuntimeError("unavailable")])
def test_short_failed_and_negative_probes_are_misses_without_retry(answer):
    sched = scheduler(CONFIGS[-1].values[0])
    sched._handler.batch_exists.side_effect = (
        answer if isinstance(answer, Exception) else None
    )
    sched._handler.batch_exists.return_value = answer
    assert sched._stored_keys(["a", "b"]) == set()
    assert sched._stored_keys(["a", "b"]) == set()
    assert sched._handler.batch_exists.call_count == 1


@pytest.mark.parametrize("config", CONFIGS)
def test_failed_load_clears_probe_hints_and_recomputes_for_store(config):
    sched = scheduler(config)
    req = request()
    matched, _ = sched.get_num_new_matched_tokens(req, 0)
    sched.update_state_after_alloc(req, None, matched)
    step(sched, req, 36, 4)
    sched.update_connector_output(
        SimpleNamespace(finished_recving=set(), invalid_block_ids={0})
    )
    assert not sched._probe_results
    assert sched.get_num_new_matched_tokens(req, 36) == (0, False)
    assert not sched._store_lookups
    (store,) = step(sched, req, 0, 40).requests
    assert store.is_store and store.store_start_token == 0
    assert sched._handler.batch_exists.call_count == 1


def test_changed_prompt_replaces_hashes_and_finished_output_drops_them(monkeypatch):
    sched = scheduler(CONFIGS[-1].values[0])
    hashes = Mock(wraps=connector._chunk_keys)
    monkeypatch.setattr(connector, "_chunk_keys", hashes)
    req = request()
    sched.get_num_new_matched_tokens(req, 0)
    req.prompt_token_ids[0] = 100
    sched.get_num_new_matched_tokens(req, 0)
    assert hashes.call_count == 2
    output = fake_scheduler_output([], None, {})
    output.finished_req_ids = {req.request_id}
    sched.build_connector_meta(output)
    assert not sched._request_hashes and not sched._store_lookups
