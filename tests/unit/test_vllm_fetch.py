# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""What the connector asks of a node's FETCH: which handlers may FETCH, and
the deadline settings they take."""

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import maru_vllm.connector as connector_module
from tests.unit.test_vllm_node import node_extra


@pytest.fixture(autouse=True)
def no_cached_defaults(monkeypatch):
    monkeypatch.setattr(connector_module, "_node_transfer_cache", {})


@pytest.mark.parametrize(
    "mode",
    [{}, {"maru_use_layerwise": True}, {"maru_overlap_load_with_compute": True}],
)
def test_the_probe_counts_a_fetch_only_for_leased_loads(mode):
    """A layerwise or overlap load reads its prefix at once, so a prefix past
    the window would miss: its probe counts no FETCH. Workers still fetch;
    the deadline knobs reach both."""
    extra = node_extra(
        "/nonexistent/node.sock",
        maru_async_load=True,
        maru_fetch_deadline_ms=400,
        maru_fetch_deadline_per_page_ms=0.5,
        **mode,
    )
    for metadata_only in (True, False):
        made = connector_module._create_node_handler(extra, metadata_only, False)
        try:
            config = made._cpu.config
            assert config.fetch_loads is not (metadata_only and bool(mode))
            assert config.fetch_deadline_ms == 400
            assert config.fetch_deadline_per_page_ms == 0.5
        finally:
            made.close()


def test_an_unset_per_page_fetch_deadline_is_left_to_the_node_page():
    extra = node_extra("/nonexistent/node.sock", maru_async_load=True)
    made = connector_module._create_node_handler(extra, False, False)
    try:
        assert made._cpu.config.fetch_deadline_per_page_ms is None
    finally:
        made.close()


def test_an_inline_load_asks_no_fetch():
    """Only a parked load's loader thread may FETCH."""
    extra = node_extra("/nonexistent/node.sock", maru_async_load=False)
    for metadata_only in (True, False):
        made = connector_module._create_node_handler(extra, metadata_only, False)
        try:
            assert made._cpu.config.fetch_loads is False
        finally:
            made.close()
