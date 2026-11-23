# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageService node config: example configs, defaults and every rule."""

import copy
import dataclasses
import json

import pytest

from maru_common.storage_types import PoolKind, Visibility
from maru_storage_service.cli import (
    EXIT_CONFIG,
    EXIT_NOT_SERVING,
    config_to_dict,
    main,
    memory_main,
)
from maru_storage_service.config import (
    DEFAULT_RM,
    NodeConfigError,
    TierConfig,
    load_node_config,
    parse_node_config,
    parse_size,
)
from maru_storage_service.config_fields import TierSpec
from maru_storage_service.drivers.registry import TIERS
from maru_storage_service.node_run import SERVED_TIERS, NotServableError, check_servable
from maru_storage_service.tiers import Level

M = 1 << 20
G = 1 << 30
T = 1 << 40

GPU_NODE = {
    "node_id": "gpu-a",
    "socket": "/run/maru/node.sock",
    "server": "tcp://maru-node-server:5556",
    "page_size": "32M",
    "tiers": [{"store": "dram", "size": "64G"}],
}
LOCAL_CXL_NODE = {**GPU_NODE, "tiers": [{"store": "local_cxl", "size": "256G"}]}
MEMORY_NODE = {k: v for k, v in LOCAL_CXL_NODE.items() if k != "socket"}
MEMORY_NODE["node_id"] = "mem-1"


def edit(base, **changes):
    raw = copy.deepcopy(base)
    raw.update(changes)
    return raw


def with_tier(base, index, **changes):
    raw = copy.deepcopy(base)
    raw["tiers"][index].update(changes)
    return raw


def rejects(raw, match, role="gpu"):
    with pytest.raises(NodeConfigError, match=match):
        parse_node_config(raw, role=role)


class TestSize:
    @pytest.mark.parametrize(
        "text,expected",
        [
            (4096, 4096),
            ("4096", 4096),
            ("32M", 32 * M),
            ("32MiB", 32 * M),
            ("1g", G),
            ("2T", 2 * T),
            ("8 K", 8 << 10),
        ],
    )
    def test_parses(self, text, expected):
        assert parse_size(text, "x") == expected

    @pytest.mark.parametrize("bad", ["", "M", "1.5G", "-1", -1, True, None, "10X"])
    def test_rejects(self, bad):
        with pytest.raises(NodeConfigError):
            parse_size(bad, "x")


class TestExamples:
    def test_explicit_cap_bounds_the_credit_at_hand(self):
        raw = edit(
            GPU_NODE,
            page_size="1M",
            tiers=[{"store": "dram", "size": "32G"}],
            budgets={"credit_max_bytes": "64M"},
        )
        b = parse_node_config(raw).budgets
        assert (b.credit_max_bytes, b.credit_refill_high) == (64 * M, 64 * M)
        assert b.credit_refill_low == 16 * M
        raw["budgets"] = {"credit_refill_low": "8M", "credit_refill_high": "100M"}
        b = parse_node_config(raw).budgets
        assert (b.credit_refill_low, b.credit_refill_high) == (8 * M, 100 * M)
        assert b.credit_max_bytes == b.credit_total_max_bytes // M * M


class TestRules:
    def test_required_fields(self):
        for key in ("node_id", "server", "page_size", "tiers"):
            raw = copy.deepcopy(GPU_NODE)
            del raw[key]
            rejects(raw, key)

    def test_unknown_keys(self):
        rejects(edit(GPU_NODE, nodeid="x"), "unknown keys")
        rejects(with_tier(GPU_NODE, 0, region_size="1G"), "unknown keys")
        rejects(edit(GPU_NODE, budgets={"credit_max": 1}), "unknown keys")

    def test_local_pin_is_rejected(self):
        rejects(edit(GPU_NODE, local_pin=True), "local pin")


class TestFileAndCli:
    def test_load(self, tmp_path):
        path = tmp_path / "node.json"
        path.write_text(json.dumps(GPU_NODE))
        assert load_node_config(path).source == str(path)

    def test_load_names_the_file(self, tmp_path):
        path = tmp_path / "node.json"
        path.write_text("{not json")
        with pytest.raises(NodeConfigError, match="node.json"):
            load_node_config(path)
        with pytest.raises(NodeConfigError, match="cannot read"):
            load_node_config(tmp_path / "missing.json")

    def test_bad_config_exit_code(self, tmp_path, capsys):
        path = tmp_path / "node.json"
        path.write_text(json.dumps(edit(GPU_NODE, role="x")))
        assert main(["--config", str(path), "--check"]) == EXIT_CONFIG
        assert "role" in capsys.readouterr().err


class TestReviewEdges:
    def test_small_top_tier_starts_with_defaults(self):
        raw = edit(GPU_NODE, tiers=[{"store": "dram", "size": "4G"}])
        b = parse_node_config(raw).budgets
        # 25% of 4 GiB is 1 GiB: 32 pages of 32 MiB, 16 of them at hand
        assert b.credit_total_max_bytes == G
        assert b.credit_max_bytes == 32 * 32 * M
        assert b.credit_refill_high == 16 * 32 * M

    def test_top_tier_too_small_for_one_page_names_the_defaults(self):
        raw = edit(GPU_NODE, tiers=[{"store": "dram", "size": "64M"}])
        rejects(raw, "defaults")

    def test_a_memory_only_node_does_not_check_the_fetch_caps(self):
        raw = edit(GPU_NODE, page_size="2G", tiers=[{"store": "dram", "size": "64G"}])
        assert parse_node_config(raw).page_size == 2 * G

    def test_nan_in_the_file(self, tmp_path):
        path = tmp_path / "node.json"
        path.write_text(json.dumps(GPU_NODE).replace('"64G"', "NaN"))
        with pytest.raises(NodeConfigError, match="NaN"):
            load_node_config(path)

    def test_duplicate_keys(self, tmp_path):
        path = tmp_path / "node.json"
        path.write_text(json.dumps(GPU_NODE)[:-1] + ', "node_id": "other"}')
        with pytest.raises(NodeConfigError, match="appears twice"):
            load_node_config(path)

    def test_not_utf8(self, tmp_path):
        path = tmp_path / "node.json"
        path.write_bytes(b'{"node_id": "\xff"}')
        with pytest.raises(NodeConfigError, match="UTF-8"):
            load_node_config(path)

    @pytest.mark.parametrize("node_id", ["gpu:a", "gpu a", "-gpu", "gpu/a"])
    def test_node_id_characters(self, node_id):
        rejects(edit(GPU_NODE, node_id=node_id), "node_id")


class TestRegistry:
    def test_options_are_read_only(self):
        tier = TierConfig(store="dram", options={"a": 1})
        with pytest.raises(TypeError):
            tier.options["a"] = 2  # type: ignore[index]
        assert TierConfig(store="dram").options == {}


class TestTierConfigCopies:
    def test_a_config_pickles_and_copies_with_its_options(self):
        import pickle

        tier = TierConfig(store="dram", size=1 << 20, options={"x": 1})
        for clone in (pickle.loads(pickle.dumps(tier)), copy.deepcopy(tier)):
            assert clone == tier and dict(clone.options) == {"x": 1}
            with pytest.raises(TypeError):
                clone.options["x"] = 2  # type: ignore[index]
        assert dataclasses.asdict(tier)["options"] == {"x": 1}

    @pytest.mark.parametrize("store", [[], ["dram"], {}, {"a": 1}])
    def test_a_store_that_is_not_a_string_is_a_config_error(self, store):
        with pytest.raises(NodeConfigError, match=r"tiers\[0\]\.store: one of"):
            parse_node_config({**GPU_NODE, "tiers": [{"store": store, "size": "64M"}]})


@pytest.mark.parametrize(
    "raw,role,store,kind",
    [
        (GPU_NODE, "gpu", "dram", PoolKind.DRAM),
        (LOCAL_CXL_NODE, "gpu", "local_cxl", PoolKind.CXL),
        (MEMORY_NODE, "memory", "local_cxl", PoolKind.CXL),
    ],
)
def test_single_store_examples(raw, role, store, kind):
    c = parse_node_config(raw, role=role)
    check_servable(c)
    assert len(c.tiers) == 1 and c.top.store == store
    assert c.traits()[store].kind is kind
    assert c.traits()[store].evicts is (c.role == "memory")
    if store == "local_cxl":
        assert c.top.rm == DEFAULT_RM and c.top.region_size == G
        assert c.top.visibility is Visibility.COHERENT
    else:
        assert c.top.hugepage is True
    assert c.transfer_defaults().async_load is False


@pytest.mark.parametrize("role", ["gpu", "memory"])
@pytest.mark.parametrize("count", [0, 2, 3])
def test_a_node_serves_one_store(role, count):
    raw = edit(
        GPU_NODE if role == "gpu" else MEMORY_NODE,
        tiers=[{"store": "dram", "size": "64G"}] * count,
    )
    with pytest.raises(NodeConfigError) as err:
        parse_node_config(raw, role=role)
    assert (
        str(err.value)
        == f"tiers: a node serves one store (dram, local_cxl or remote), got {count}"
    )


def test_cxl_points_to_local_cxl():
    with pytest.raises(NodeConfigError) as err:
        parse_node_config(with_tier(GPU_NODE, 0, store="cxl"))
    assert (
        str(err.value)
        == 'tiers[0].store: "cxl" names the shared CXL backend; a node\'s own CXL store is "local_cxl"'
    )


def test_cpu_points_to_dram():
    rejects(with_tier(GPU_NODE, 0, store="cpu"), '"dram"')


def test_ssd_is_not_a_store():
    rejects(with_tier(GPU_NODE, 0, store="ssd"), "store: one of")


@pytest.mark.parametrize("raw", [GPU_NODE, LOCAL_CXL_NODE])
@pytest.mark.parametrize("key", ["high_watermark", "low_watermark"])
def test_a_gpu_node_takes_no_watermark(raw, key):
    rejects(with_tier(raw, 0, **{key: 0.5}), "unknown keys")


@pytest.mark.parametrize("store", ["dram", "local_cxl"])
@pytest.mark.parametrize("low,high", [(0.9, 0.9), (0.95, 0.9), (0, 0.5), (0.5, 1.1)])
def test_memory_watermarks(store, low, high):
    raw = edit(
        MEMORY_NODE,
        tiers=[
            {
                "store": store,
                "size": "64G",
                "low_watermark": low,
                "high_watermark": high,
            }
        ],
    )
    rejects(raw, "watermark", role="memory")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), True])
def test_watermarks_are_finite_numbers(value):
    rejects(with_tier(MEMORY_NODE, 0, high_watermark=value), "finite", role="memory")


@pytest.mark.parametrize(
    "raw,key",
    [
        (GPU_NODE, "region_size"),
        (GPU_NODE, "rm"),
        (GPU_NODE, "visibility"),
        (LOCAL_CXL_NODE, "hugepage"),
    ],
)
def test_store_specific_keys(raw, key):
    rejects(with_tier(raw, 0, **{key: True}), "unknown keys")


@pytest.mark.parametrize(
    "key", ["demotion", "through_max_inflight", "credit_fallback", "path"]
)
@pytest.mark.parametrize("raw", [GPU_NODE, LOCAL_CXL_NODE])
def test_unserved_keys_are_rejected(raw, key):
    rejects(with_tier(raw, 0, **{key: True}), "unknown keys")


def test_sizes_and_visibility():
    rejects(with_tier(GPU_NODE, 0, size=None), "size: required")
    rejects(with_tier(GPU_NODE, 0, size="1M"), "smaller than page_size")
    rejects(with_tier(LOCAL_CXL_NODE, 0, region_size="48M"), "multiple of page_size")
    rejects(with_tier(LOCAL_CXL_NODE, 0, size="512M"), "smaller than region_size")
    rejects(with_tier(LOCAL_CXL_NODE, 0, visibility="fenced"), "visibility")
    assert (
        parse_node_config(
            with_tier(LOCAL_CXL_NODE, 0, visibility="flush")
        ).top.visibility
        is Visibility.FLUSH
    )


def test_local_cxl_without_size_needs_explicit_budgets():
    raw = edit(LOCAL_CXL_NODE, tiers=[{"store": "local_cxl"}])
    rejects(raw, "credit_total_max_bytes: required")
    raw["budgets"] = {"credit_total_max_bytes": "4G", "commit_pending_max_bytes": "2G"}
    rejects(raw, "transfer_reserve_bytes: required")
    raw["budgets"]["transfer_reserve_bytes"] = "1G"
    c = parse_node_config(raw)
    with pytest.raises(NotServableError, match="local_cxl tier needs a size"):
        check_servable(c)


@pytest.mark.parametrize(
    "raw,role", [(GPU_NODE, "gpu"), (LOCAL_CXL_NODE, "gpu"), (MEMORY_NODE, "memory")]
)
def test_budget_defaults_and_reserve_names(raw, role):
    c = parse_node_config(raw, role=role)
    b, size = c.budgets, c.top.size
    assert b.credit_total_max_bytes == int(size * 0.25)
    assert b.commit_pending_max_bytes == b.credit_total_max_bytes
    assert b.transfer_reserve_bytes == {c.top.store: int(size * 0.05)}
    c = parse_node_config(
        edit(raw, budgets={"transfer_reserve_bytes": {c.top.store: "1G"}}), role=role
    )
    assert c.budgets.transfer_reserve_bytes == {c.top.store: G}
    rejects(
        edit(raw, budgets={"transfer_reserve_bytes": {"other": "1G"}}),
        "no memory tier",
        role=role,
    )


@pytest.mark.parametrize(
    "page,pages", [("1M", 512), ("3M", 170), ("32M", 16), ("64M", 16)]
)
def test_credit_at_hand_is_16_pages_or_512m(page, pages):
    b = parse_node_config(edit(GPU_NODE, page_size=page)).budgets
    ps = parse_size(page, "page")
    assert b.credit_refill_high == pages * ps
    assert b.credit_refill_low == int(pages * ps * 0.25)


def test_budget_order_and_capacity():
    rejects(
        edit(GPU_NODE, budgets={"credit_max_bytes": "16M"}), "smaller than page_size"
    )
    rejects(edit(GPU_NODE, budgets={"credit_refill_low": "1G"}), "credit_refill_low <=")
    rejects(
        edit(
            GPU_NODE,
            budgets={"credit_max_bytes": "512M", "credit_total_max_bytes": "64M"},
        ),
        "credit_max_bytes <= credit_total",
    )
    rejects(
        edit(GPU_NODE, budgets={"credit_total_max_bytes": "60G"}),
        "exceed the tiers credit comes from",
    )
    b = parse_node_config(
        edit(GPU_NODE, budgets={"credit_total_max_bytes": "64M"})
    ).budgets
    assert b.credit_max_bytes == 64 * M


def test_served_stores_are_explicit_per_role():
    assert list(TIERS) == ["dram", "local_cxl", "remote"]
    assert SERVED_TIERS == {
        "gpu": (["dram"], ["local_cxl"], ["remote"]),
        "memory": (["dram"], ["local_cxl"]),
    }
    c = parse_node_config(GPU_NODE)
    with pytest.raises(NotServableError):
        check_servable(dataclasses.replace(c, tiers=c.tiers * 2))


REMOTE_NODE = {
    **GPU_NODE,
    "tiers": [{"store": "remote", "url": "localhost:5600", "key_file": "~/maru.key"}],
}


def test_remote_uses_worker_staging_and_no_local_pool():
    config = parse_node_config(REMOTE_NODE)
    check_servable(config)
    assert config.top.options["write_hints"] is False
    assert config.top.options["write_wait_ms"] == 50
    assert config.top.size is None
    assert config.top.options["staging_size"] == max(1 << 30, 64 * config.page_size)
    assert config.budgets.transfer_reserve_bytes == {}


@pytest.mark.parametrize(
    "page_mib,total_mib,worker_mib", [(3, 512, 510), (32, 512, 512), (64, 1024, 1024)]
)
def test_remote_credit_defaults(page_mib, total_mib, worker_mib):
    page = page_mib * M
    raw = with_tier(REMOTE_NODE, 0, staging_size=32 * page)
    budgets = parse_node_config(edit(raw, page_size=page)).budgets
    assert (
        budgets.credit_total_max_bytes
        == budgets.commit_pending_max_bytes
        == total_mib * M
    )
    assert budgets.credit_max_bytes == budgets.credit_refill_high == worker_mib * M
    assert budgets.credit_refill_low == worker_mib * M // 2


def test_remote_credit_overrides_still_bound_the_refill():
    raw = edit(
        REMOTE_NODE,
        budgets={"credit_total_max_bytes": "256M", "credit_max_bytes": "128M"},
    )
    assert parse_node_config(raw).budgets.credit_refill_high == 128 * M
    raw["budgets"].update(credit_refill_low="32M", credit_refill_high="64M")
    budgets = parse_node_config(raw).budgets
    assert budgets.credit_refill_low == 32 * M and budgets.credit_refill_high == 64 * M


@pytest.mark.parametrize(
    "key,value",
    [
        ("url", "missing-port"),
        ("url", "host:0"),
        ("rdma_device", False),
        ("timeout_s", 0),
        ("timeout_s", float("nan")),
        ("staging_size", "32M"),
        ("staging_size", 64 * M + 1),
        ("staging_max_bytes", "1M"),
        ("quarantine_max", 0),
        ("buffer_size", "1G"),
        ("hugepage", True),
        ("size", "1G"),
        ("high_watermark", 0.9),
        ("write_hints", "true"),
        ("write_wait_ms", True),
        ("write_wait_ms", 10001),
    ],
)
def test_remote_rejects_invalid_staging_and_removed_buffer_keys(key, value):
    with pytest.raises(NodeConfigError):
        parse_node_config(with_tier(REMOTE_NODE, 0, **{key: value}))


def test_remote_is_only_a_gpu_store():
    raw = {k: v for k, v in REMOTE_NODE.items() if k != "socket"}
    rejects(raw, "memory node cannot have a remote", role="memory")


@pytest.mark.parametrize("check", [True, False])
def test_cli_and_start_refuse_unservable_config(tmp_path, capsys, check):
    raw = edit(
        LOCAL_CXL_NODE,
        tiers=[{"store": "local_cxl"}],
        budgets={
            "credit_total_max_bytes": "4G",
            "commit_pending_max_bytes": "2G",
            "transfer_reserve_bytes": "1G",
        },
    )
    path = tmp_path / "node.json"
    path.write_text(json.dumps(raw))
    assert (
        main(["--config", str(path), *(["--check"] if check else [])])
        == EXIT_NOT_SERVING
    )
    assert "local_cxl tier needs a size" in capsys.readouterr().err


@pytest.mark.parametrize(
    "raw,role,command",
    [
        (GPU_NODE, "gpu", main),
        (LOCAL_CXL_NODE, "gpu", main),
        (MEMORY_NODE, "memory", memory_main),
    ],
)
def test_check_prints_resolved_config(tmp_path, capsys, raw, role, command):
    path = tmp_path / "node.json"
    path.write_text(json.dumps(raw))
    assert command(["--config", str(path), "--check"]) == 0
    c = json.loads(capsys.readouterr().out)
    assert c["tiers"][0]["store"] == raw["tiers"][0]["store"]
    assert c["page_size"] == 32 * M
    assert parse_node_config(c, role=role) == load_node_config(path, role=role)


@pytest.mark.parametrize("key", ["stage_workers", "stage_queue"])
def test_remote_server_refuses_unserved_keys(key):
    raw = edit(
        MEMORY_NODE,
        remote_server={"listen": "localhost:5600", "key_file": "/tmp/key", key: 1},
    )
    rejects(raw, "unknown keys", role="memory")


def test_remote_server_settings_are_not_silently_ignored():
    raw = edit(
        MEMORY_NODE,
        tiers=[{"store": "local_cxl", "size": "4G"}],
        remote_server={"listen": "localhost:5600", "key_file": "/tmp/key"},
    )
    config = parse_node_config(raw, role="memory")
    check_servable(config)
    assert config.budgets.credit_max_bytes == G
    assert config.remote_server.peer_quarantine_max == 2 * G
    raw["remote_server"]["peer_quarantine_max"] = "64M"
    assert (
        parse_node_config(raw, role="memory").remote_server.peer_quarantine_max
        == 64 * M
    )


def test_a_registered_store_is_parsed_by_its_spec(monkeypatch):
    def parse(raw, ctx):
        return {"options": {"depth": raw.get("depth", 1), "size_seen": ctx.size}}

    spec = TierSpec(
        "fake",
        Level.L1,
        True,
        PoolKind.CXL,
        frozenset({"store", "size", "depth"}),
        parse=parse,
    )
    monkeypatch.setitem(TIERS, "fake", spec)
    raw = edit(GPU_NODE, tiers=[{"store": "fake", "size": "64G", "depth": 3}])
    config = parse_node_config(raw)
    assert config.top.options == {"depth": 3, "size_seen": 64 * G}
    assert config.traits()["fake"].kind is PoolKind.CXL
    assert config_to_dict(config)["tiers"][0]["depth"] == 3
    rejects(with_tier(raw, 0, wrong=True), "unknown keys")


def test_remote_write_hints_round_trip():
    from maru_storage_service.cli import config_to_dict

    raw = with_tier(REMOTE_NODE, 0, write_hints=True, write_wait_ms=10000)
    config = parse_node_config(raw)
    assert config.top.options["write_hints"] is True
    assert config.top.options["write_wait_ms"] == 10000
    assert parse_node_config(config_to_dict(config)) == config
