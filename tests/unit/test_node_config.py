# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageService node config: example configs, defaults and every rule."""

import copy
import dataclasses
import json

import pytest

from maru_common.storage_types import PoolKind, Visibility
from maru_storage_service.cli import EXIT_CONFIG, EXIT_NOT_SERVING, config_to_dict, main
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
from maru_storage_service.service import SERVED_TIERS, NotServableError, check_servable
from maru_storage_service.tiers import Level

M = 1 << 20
G = 1 << 30
T = 1 << 40

# Example configs: a GPU node and a memory node
GPU_NODE = {
    "node_id": "gpu-a",
    "socket": "/run/maru/node.sock",
    "server": "tcp://maru-server:5555",
    "page_size": "32M",
    "tiers": [
        {"store": "dram", "size": "64G", "demotion": "back"},
        {"store": "cxl", "size": "256G", "rm": "127.0.0.1:9850", "demotion": "back"},
        {"store": "ssd", "size": "2T", "path": "/mnt/nvme0/maru"},
    ],
}
MEMORY_NODE = {
    "node_id": "mem-1",
    "role": "memory",
    "server": "tcp://maru-server:5555",
    "page_size": "32M",
    "remote_server": {"listen": "tcp://0.0.0.0:5600", "key_file": "~/.maru/agent.key"},
    "tiers": [
        {"store": "dram", "size": "256G"},
        {"store": "cxl", "size": "1T", "rm": "127.0.0.1:9850"},
        {"store": "ssd", "size": "8T", "path": "/mnt/nvme0/maru"},
    ],
}


def edit(base, **changes):
    raw = copy.deepcopy(base)
    raw.update(changes)
    return raw


def with_tier(base, index, **changes):
    raw = copy.deepcopy(base)
    raw["tiers"][index].update(changes)
    return raw


def rejects(raw, match):
    with pytest.raises(NodeConfigError, match=match):
        parse_node_config(raw)


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
    def test_gpu_node(self):
        c = parse_node_config(GPU_NODE)
        assert (c.node_id, c.role, c.page_size, c.local_pin) == (
            "gpu-a",
            "gpu",
            32 * M,
            False,
        )
        assert [t.store for t in c.tiers] == ["dram", "cxl", "ssd"]
        dram, cxl, ssd = c.tiers
        assert (dram.size, dram.demotion, dram.hugepage) == (64 * G, "back", True)
        assert (cxl.size, cxl.region_size, cxl.rm, cxl.visibility) == (
            256 * G,
            G,
            DEFAULT_RM,
            Visibility.COHERENT,
        )
        assert (ssd.size, ssd.demotion, ssd.path) == (2 * T, None, "/mnt/nvme0/maru")
        assert (dram.high_watermark, dram.low_watermark) == (0.9, 0.8)
        assert c.remote_server is None

    def test_memory_node(self):
        c = parse_node_config(MEMORY_NODE)
        assert c.role == "memory"
        assert c.socket == "/run/maru/node.sock"
        assert c.remote_server.listen == "0.0.0.0:5600"

    def test_budget_defaults(self):
        b = parse_node_config(GPU_NODE).budgets
        # credit comes from the DRAM and, short of it, the CXL tier
        assert b.credit_total_max_bytes == int((64 + 256) * G * 0.25)
        assert b.credit_max_bytes == b.credit_total_max_bytes
        assert (b.credit_refill_low, b.credit_refill_high) == (4 * 32 * M, 16 * 32 * M)
        # what waits for its commit came from credit
        assert b.commit_pending_max_bytes == b.credit_total_max_bytes
        assert b.transfer_reserve_bytes == {
            "dram": int(64 * G * 0.05),
            "cxl": int(256 * G * 0.05),
        }
        # two fetch caps; the batch cap was dropped (the leased window bounds it)
        assert (b.fetch_request_max_bytes, b.fetch_max_inflight_bytes) == (G, 4 * G)
        assert not hasattr(b, "fetch_max_batch_bytes")

    @pytest.mark.parametrize(
        "page, pages",
        [("1M", 512), ("3M", 170), ("32M", 16), ("64M", 16)],
    )
    def test_credit_at_hand_is_16_pages_or_512m(self, page, pages):
        raw = edit(GPU_NODE, page_size=page)
        raw["tiers"][1]["region_size"] = "192M"  # a multiple of every page
        b = parse_node_config(raw).budgets
        size = parse_size(page, "page")
        assert b.credit_refill_low == int(pages * size * 0.25)
        assert b.credit_refill_high == pages * size
        assert b.credit_max_bytes == b.credit_total_max_bytes // size * size

    def test_without_fallback_credit_comes_from_the_top_tier(self):
        raw = with_tier(GPU_NODE, 1, credit_fallback=False)
        c = parse_node_config(raw)
        assert c.tiers[1].credit_fallback is False
        assert c.budgets.credit_total_max_bytes == int(64 * G * 0.25)
        traits = c.traits()
        assert not traits[PoolKind.CXL].credit_fallback

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

    def test_explicit_credit_max_wins_over_the_default(self):
        raw = edit(GPU_NODE, page_size="1M", budgets={"credit_max_bytes": "8M"})
        raw["tiers"][1]["region_size"] = "192M"
        assert parse_node_config(raw).budgets.credit_max_bytes == 8 * M

    def test_remote_tier(self):
        raw = copy.deepcopy(GPU_NODE)
        raw["tiers"][2]["demotion"] = "through"
        raw["tiers"].append(
            {"store": "remote", "url": "tcp://mem-1:5600, tcp://mem-2:5600"}
        )
        remote = parse_node_config(raw).tiers[-1]
        assert remote.url == ("tcp://mem-1:5600", "tcp://mem-2:5600")
        assert (remote.timeout_s, remote.rdma_device) == (30.0, None)


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

    def test_store_twice(self):
        raw = copy.deepcopy(GPU_NODE)
        raw["tiers"].insert(1, {"store": "dram", "size": "1G"})
        rejects(raw, "appears twice")

    def test_top_tier_is_memory(self):
        raw = copy.deepcopy(GPU_NODE)
        raw["tiers"] = [{"store": "ssd", "size": "1T", "path": "/x"}]
        rejects(raw, "first tier")

    def test_remote_is_an_l2_tier_and_gpu_only(self):
        raw = copy.deepcopy(GPU_NODE)
        raw["tiers"].insert(1, {"store": "remote", "url": "tcp://m:1"})
        with pytest.raises(NotServableError, match="serving the tiers"):
            check_servable(parse_node_config(raw))
        raw = copy.deepcopy(MEMORY_NODE)
        raw["tiers"][-1]["demotion"] = "back"
        raw["tiers"].append({"store": "remote", "url": "tcp://m:1"})
        rejects(raw, "memory node cannot have a remote")

    def test_last_tier_has_no_demotion(self):
        rejects(with_tier(GPU_NODE, 2, demotion="back"), "last tier")

    def test_demotion_values(self):
        rejects(with_tier(GPU_NODE, 0, demotion="later"), "demotion")
        assert (
            parse_node_config(with_tier(GPU_NODE, 0, demotion="through"))
            .tiers[0]
            .demotion
            == "through"
        )

    @pytest.mark.parametrize(
        "low,high", [(0.9, 0.9), (0.95, 0.9), (0, 0.5), (0.5, 1.1)]
    )
    def test_watermarks(self, low, high):
        rejects(
            with_tier(GPU_NODE, 0, low_watermark=low, high_watermark=high), "watermark"
        )

    def test_sizes(self):
        rejects(with_tier(GPU_NODE, 0, size=None), "size: required")
        rejects(with_tier(GPU_NODE, 0, size="1M"), "smaller than page_size")
        rejects(with_tier(GPU_NODE, 1, region_size="48M"), "multiple of page_size")
        rejects(with_tier(GPU_NODE, 1, size="512M"), "smaller than region_size")
        rejects(with_tier(GPU_NODE, 2, path=""), "path")

    def test_cxl_without_size_needs_explicit_budgets(self):
        raw = copy.deepcopy(GPU_NODE)
        raw["tiers"] = [{"store": "cxl"}]
        rejects(raw, "credit_total_max_bytes: required")
        raw["budgets"] = {
            "credit_total_max_bytes": "4G",
            "commit_pending_max_bytes": "2G",
        }
        rejects(raw, "transfer_reserve_bytes: required")
        raw["budgets"]["transfer_reserve_bytes"] = "1G"
        c = parse_node_config(raw)
        assert c.tiers[0].size is None and c.budgets.transfer_reserve_bytes == {
            "cxl": G
        }

    def test_visibility(self):
        assert (
            parse_node_config(with_tier(GPU_NODE, 1, visibility="flush"))
            .tiers[1]
            .visibility
            is Visibility.FLUSH
        )
        rejects(with_tier(GPU_NODE, 1, visibility="fenced"), "visibility")

    def test_local_pin_is_rejected(self):
        rejects(edit(GPU_NODE, local_pin=True), "local pin")

    def test_role(self):
        rejects(edit(GPU_NODE, role="cpu"), "role")
        raw = copy.deepcopy(MEMORY_NODE)
        del raw["remote_server"]
        rejects(raw, "remote_server: required")

    def test_budget_order(self):
        rejects(
            edit(GPU_NODE, budgets={"credit_max_bytes": "16M"}),
            "smaller than page_size",
        )
        rejects(
            edit(GPU_NODE, budgets={"credit_refill_low": "1G"}), "credit_refill_low <="
        )
        rejects(
            edit(
                GPU_NODE,
                budgets={"credit_max_bytes": "512M", "credit_total_max_bytes": "64M"},
            ),
            "credit_max_bytes <= credit_total",
        )
        # with only the total set, the per-connection default shrinks to fit
        c = parse_node_config(edit(GPU_NODE, budgets={"credit_total_max_bytes": "64M"}))
        assert c.budgets.credit_max_bytes == 64 * M
        rejects(
            edit(GPU_NODE, budgets={"fetch_request_max_bytes": "16M"}),
            "page_size <= fetch_request_max_bytes",
        )
        rejects(
            edit(GPU_NODE, budgets={"fetch_request_max_bytes": "8G"}),
            "fetch_request_max_bytes <= fetch_max_inflight_bytes",
        )
        rejects(
            edit(GPU_NODE, budgets={"fetch_max_batch_bytes": "256M"}),
            r"unknown keys \['fetch_max_batch_bytes'\]",
        )
        c = parse_node_config(
            edit(GPU_NODE, budgets={"fetch_request_max_bytes": "32M"})
        )
        assert c.budgets.fetch_request_max_bytes == 32 * M

    def test_budgets_fit_the_tiers_credit_comes_from(self):
        # dram + cxl hold 320 GiB, less the pending budget and both reserves
        rejects(
            edit(GPU_NODE, budgets={"credit_total_max_bytes": "310G"}),
            r"exceed the tiers credit comes from \(dram \+ cxl",
        )
        no_fallback = with_tier(GPU_NODE, 1, credit_fallback=False)
        rejects(
            edit(no_fallback, budgets={"credit_total_max_bytes": "60G"}),
            r"exceed the tiers credit comes from \(dram:",
        )

    def test_fallback_is_for_a_tier_below_the_top(self):
        raw = copy.deepcopy(GPU_NODE)
        raw["tiers"] = [{"store": "cxl", "size": "32G", "credit_fallback": True}]
        rejects(raw, "only a tier below the top")

    def test_transfer_reserve_per_tier(self):
        c = parse_node_config(
            edit(GPU_NODE, budgets={"transfer_reserve_bytes": {"dram": "1G"}})
        )
        assert c.budgets.transfer_reserve_bytes == {
            "dram": G,
            "cxl": int(256 * G * 0.05),
        }
        rejects(
            edit(GPU_NODE, budgets={"transfer_reserve_bytes": {"ssd": "1G"}}),
            "no memory tier",
        )


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

    def test_check_prints_resolved_config(self, tmp_path, capsys):
        path = tmp_path / "node.json"
        path.write_text(json.dumps(GPU_NODE))
        assert main(["--config", str(path), "--check"]) == 0
        out = json.loads(capsys.readouterr().out)
        assert out["page_size"] == 32 * M
        assert out["tiers"][1]["visibility"] == "coherent"
        assert out["budgets"]["transfer_reserve_bytes"]["dram"] == int(64 * G * 0.05)
        assert out["budgets"]["credit_refill_high"] == 512 * M
        assert out["tiers"][1]["credit_fallback"] is True

    def test_check_prints_the_credit_default_of_small_pages(self, tmp_path, capsys):
        raw = edit(GPU_NODE, page_size="1M")
        raw["tiers"][1]["region_size"] = "192M"
        path = tmp_path / "node.json"
        path.write_text(json.dumps(raw))
        assert main(["--config", str(path), "--check"]) == 0
        budgets = json.loads(capsys.readouterr().out)["budgets"]
        assert budgets["credit_max_bytes"] == budgets["credit_total_max_bytes"]
        assert budgets["credit_refill_low"] == 128 * M
        assert budgets["credit_refill_high"] == 512 * M

    def test_bad_config_exit_code(self, tmp_path, capsys):
        path = tmp_path / "node.json"
        path.write_text(json.dumps(edit(GPU_NODE, role="x")))
        assert main(["--config", str(path), "--check"]) == EXIT_CONFIG
        assert "role" in capsys.readouterr().err

    def test_does_not_pretend_to_serve(self, tmp_path, capsys):
        raw = copy.deepcopy(GPU_NODE)
        raw["tiers"][1] = {"store": "ssd", "size": "1T", "path": "/mnt/a"}
        raw["tiers"][2] = {"store": "remote", "url": "tcp://mem-1:5600"}
        raw["budgets"] = {}
        path = tmp_path / "node.json"
        path.write_text(json.dumps(raw))
        assert main(["--config", str(path)]) == EXIT_NOT_SERVING
        assert "not implemented" in capsys.readouterr().err


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
        b = parse_node_config(raw).budgets
        assert (b.fetch_request_max_bytes, b.fetch_max_inflight_bytes) == (G, 4 * G)

    def test_a_fetch_node_holds_a_page_and_its_bounce_in_flight(self):
        caps = {"fetch_request_max_bytes": "32M", "fetch_max_inflight_bytes": "32M"}
        rejects(edit(GPU_NODE, budgets=caps), "one page and its bounce buffer")
        caps["fetch_max_inflight_bytes"] = "64M"
        c = parse_node_config(edit(GPU_NODE, budgets=caps))
        assert c.budgets.fetch_max_inflight_bytes == 64 * M
        # with 2 GiB pages the default caps are too small
        tiers = [
            {"store": "dram", "size": "64G"},
            {"store": "ssd", "size": "1T", "path": "/x"},
        ]
        raw = edit(GPU_NODE, page_size="2G", tiers=tiers)
        rejects(raw, "page_size <= fetch_request_max_bytes")
        caps = {"fetch_request_max_bytes": "2G", "fetch_max_inflight_bytes": "2G"}
        rejects(edit(raw, budgets=caps), "one page and its bounce buffer")
        caps["fetch_max_inflight_bytes"] = "4G"
        b = parse_node_config(edit(raw, budgets=caps)).budgets
        assert (b.fetch_request_max_bytes, b.fetch_max_inflight_bytes) == (2 * G, 4 * G)

    def test_a_fetch_node_keeps_a_page_to_fetch_into(self):
        tiers = [
            {"store": "dram", "size": "512M"},
            {"store": "ssd", "size": "1T", "path": "/x"},
        ]
        raw = edit(GPU_NODE, tiers=tiers)
        # 5% of 512 MiB is less than a 32 MiB page: the page is the default
        b = parse_node_config(raw).budgets
        assert b.transfer_reserve_bytes == {"dram": 32 * M}
        rejects(
            edit(raw, budgets={"transfer_reserve_bytes": "16M"}), "less than one page"
        )
        # a memory-only node keeps its 5%
        b = parse_node_config(edit(raw, tiers=tiers[:1])).budgets
        assert b.transfer_reserve_bytes == {"dram": int(512 * M * 0.05)}

    def test_cpu_is_read_as_dram(self):
        raw = with_tier(GPU_NODE, 0, store="cpu")
        assert parse_node_config(raw).tiers[0].store == "dram"

    @pytest.mark.parametrize("value", [float("nan"), float("inf")])
    def test_numbers_must_be_finite(self, value):
        rejects(with_tier(GPU_NODE, 0, high_watermark=value), "finite")
        raw = copy.deepcopy(GPU_NODE)
        raw["tiers"][2]["demotion"] = "back"
        raw["tiers"].append({"store": "remote", "url": "tcp://m:1", "timeout_s": value})
        rejects(raw, "finite")

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

    def test_ssd_path_is_absolute_and_normalized(self):
        assert (
            parse_node_config(with_tier(GPU_NODE, 2, path="/mnt/x/")).tiers[2].path
            == "/mnt/x"
        )
        rejects(with_tier(GPU_NODE, 2, path="mnt/x"), "absolute")

    @pytest.mark.parametrize("node_id", ["gpu:a", "gpu a", "-gpu", "gpu/a"])
    def test_node_id_characters(self, node_id):
        rejects(edit(GPU_NODE, node_id=node_id), "node_id")

    def test_key_file_home_is_expanded(self):
        c = parse_node_config(MEMORY_NODE)
        assert not c.remote_server.key_file.startswith("~")


class TestRegistry:
    def test_stores_and_their_wire_kinds(self):
        assert [(s.store, s.level, s.local, s.kind) for s in TIERS.values()] == [
            ("dram", Level.L1, True, PoolKind.DRAM),
            ("cxl", Level.L1, True, PoolKind.CXL),
            ("ssd", Level.L2, True, PoolKind.SSD),
            ("remote", Level.L2, False, None),
        ]

    def test_served_tiers_are_the_twelve_combinations_in_order(self):
        assert SERVED_TIERS == (
            ["dram"],
            ["cxl"],
            ["dram", "cxl"],
            ["dram", "ssd"],
            ["cxl", "ssd"],
            ["dram", "cxl", "ssd"],
            ["dram", "remote"],
            ["cxl", "remote"],
            ["dram", "cxl", "remote"],
            ["dram", "remote", "ssd"],
            ["cxl", "remote", "ssd"],
            ["dram", "cxl", "remote", "ssd"],
        )

    def test_servable_checks_keep_their_order(self):
        raw = copy.deepcopy(GPU_NODE)
        raw["tiers"][0]["demotion"] = "through"
        del raw["tiers"][1]["size"]
        raw["budgets"] = {"transfer_reserve_bytes": {"cxl": "1G"}}
        config = parse_node_config(raw)
        dram, cxl, ssd = config.tiers
        # combination, then the cxl size; write-through is served
        for tiers, match in (
            ((cxl, dram), "serving the tiers"),
            ((dram, cxl, ssd), "the cxl tier needs a size"),
        ):
            with pytest.raises(NotServableError, match=match):
                check_servable(dataclasses.replace(config, tiers=tiers))
        check_servable(
            dataclasses.replace(
                config, tiers=(dram, dataclasses.replace(cxl, size=T), ssd)
            )
        )
        # A memory node serves the same tier combinations
        with pytest.raises(NotServableError, match="serving the tiers"):
            check_servable(
                dataclasses.replace(config, role="memory", tiers=(cxl, dram))
            )

    def test_traits_carry_the_wire_kind(self):
        traits = parse_node_config(GPU_NODE).traits()
        assert {n: t.kind for n, t in traits.items()} == {
            "dram": PoolKind.DRAM,
            "cxl": PoolKind.CXL,
            "ssd": PoolKind.SSD,
        }

    def test_options_are_read_only(self):
        tier = TierConfig(store="dram", options={"a": 1})
        with pytest.raises(TypeError):
            tier.options["a"] = 2  # type: ignore[index]
        assert TierConfig(store="dram").options == {}


def two_l2(stores, **top):
    """GPU_NODE's tiers in the order of ``stores``, with a remote tier."""
    by_store = {t["store"]: t for t in copy.deepcopy(GPU_NODE)["tiers"]}
    by_store["remote"] = {"store": "remote", "url": "tcp://mem-1:5600"}
    tiers = [by_store[s] for s in stores]
    tiers[0].update(top)
    return edit(GPU_NODE, tiers=tiers)


class TestRemoteThenSsd:
    @pytest.mark.parametrize(
        "stores",
        [["dram", "remote", "ssd"], ["cxl", "remote", "ssd"]],
    )
    def test_the_bottom_l1_tier_demotes_to_remote_then_ssd(self, stores):
        config = parse_node_config(two_l2(stores))
        check_servable(config)
        top, remote, ssd = config.traits().values()
        assert top.demotes_to == ("remote", "ssd") and top.copies_to == ()
        assert (remote.evicts, remote.credit, remote.demotes_to) == (False, False, ())
        assert (ssd.evicts, ssd.credit, ssd.demotes_to) == (True, False, ())
        assert remote.io_only and ssd.io_only

    def test_over_cxl_only_cxl_demotes_to_both(self):
        config = parse_node_config(two_l2(["dram", "cxl", "remote", "ssd"]))
        check_servable(config)
        traits = config.traits()
        assert traits["dram"].demotes_to == ("cxl",)
        assert traits["cxl"].demotes_to == ("remote", "ssd")
        assert traits["ssd"].evicts and not traits["remote"].evicts

    def test_write_through_copies_to_remote_then_ssd(self):
        config = parse_node_config(
            two_l2(["dram", "remote", "ssd"], demotion="through")
        )
        check_servable(config)
        dram = config.traits()["dram"]
        assert dram.copies_to == dram.demotes_to == ("remote", "ssd")

    @pytest.mark.parametrize(
        "stores",
        [
            ["dram", "ssd", "remote"],
            ["dram", "cxl", "ssd", "remote"],
            ["dram", "remote", "cxl", "ssd"],
        ],
    )
    def test_other_orders_are_refused_at_start(self, stores):
        config = parse_node_config(two_l2(stores))
        with pytest.raises(NotServableError, match="serving the tiers"):
            check_servable(config)

    def test_neither_l2_tier_takes_a_demotion(self):
        raw = two_l2(["dram", "remote", "ssd"])
        rejects(with_tier(raw, 2, demotion="back"), "last tier")
        rejects(with_tier(raw, 1, demotion="back"), "unknown keys")

    def test_one_l2_keeps_its_traits(self):
        for below in ("remote", "ssd"):
            traits = parse_node_config(two_l2(["dram", below])).traits()
            assert traits["dram"].demotes_to == (below,)
            assert traits[below].evicts == (below == "ssd")


def _fake_spec(**changes):
    def parse(raw, ctx):
        return {"options": {"depth": raw.get("depth", 1), "size_seen": ctx.size}}

    spec = TierSpec(
        store="fake",
        level=Level.L2,
        local=True,
        kind=PoolKind.SSD,
        keys=frozenset({"store", "size", "demotion", "depth"}),
        parse=parse,
    )
    return dataclasses.replace(spec, **changes)


class TestAStoreFromTheRegistry:
    def test_a_registered_store_is_parsed_by_its_spec(self, monkeypatch):
        monkeypatch.setitem(TIERS, "fake", _fake_spec())
        raw = edit(
            GPU_NODE,
            tiers=[
                {"store": "dram", "size": "64G"},
                {"store": "fake", "size": "1T", "depth": 3},
            ],
        )
        config = parse_node_config(raw)
        fake = config.tiers[1]
        assert dict(fake.options) == {"depth": 3, "size_seen": T}
        assert not fake.is_memory
        assert config.traits()["fake"].kind == PoolKind.SSD
        assert config_to_dict(config)["tiers"][1]["depth"] == 3
        rejects(with_tier(raw, 1, path="/x"), r"tiers\[1\] \(fake\): unknown keys")

    def test_two_local_tiers_of_one_kind_are_refused(self, monkeypatch):
        monkeypatch.setitem(TIERS, "fake", _fake_spec())
        raw = copy.deepcopy(GPU_NODE)
        raw["tiers"][2]["demotion"] = "back"
        raw["tiers"].append({"store": "fake", "size": "1T"})
        rejects(raw, r"two local tiers have pools of one kind: \['ssd'\]")

    def test_a_tier_that_is_not_local_takes_demotions_and_never_evicts(
        self, monkeypatch
    ):
        monkeypatch.setitem(TIERS, "fake", _fake_spec(local=False))
        raw = copy.deepcopy(GPU_NODE)
        raw["tiers"][2]["demotion"] = "back"
        raw["tiers"].append({"store": "fake", "size": "1T"})
        traits = parse_node_config(raw).traits()
        fake = traits["fake"]
        assert fake.kind is None and not fake.credit and not fake.evicts
        assert traits["ssd"].demotes_to == ("fake",)


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
