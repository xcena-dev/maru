# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Single-store placement and absence of local transfers."""

from maru_storage_service.config import parse_node_config
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import (
    FakeRm,
    node_config,
    remove_tree,
    short_tmpdir,
    start_node,
)


def test_one_tier_nodes_drop_as_before():
    for role in ("gpu", "memory"):
        for store in ("dram", "local_cxl"):
            config = parse_node_config(
                {
                    "node_id": "n",
                    "server": "tcp://localhost:5556",
                    "page_size": "1M",
                    "tiers": [
                        {
                            "store": store,
                            "size": "64M",
                            **({"region_size": "4M"} if store == "local_cxl" else {}),
                        }
                    ],
                },
                role=role,
            )
            traits = config.traits()[store]
            assert traits.credit
            assert traits.evicts is (role == "memory")


def test_one_tier_nodes_have_no_engine():
    fake, sockdir = FakeMaruServer(heartbeat_s=0.1), short_tmpdir()
    try:
        for store in ("dram", "local_cxl"):
            config = node_config(
                fake.url,
                f"{sockdir}/node.sock",
                cxl_pages=16 if store == "local_cxl" else None,
            )
            node = start_node(config, region_source=FakeRm())
            try:
                assert node.transfers is None
            finally:
                node.stop()
    finally:
        fake.close()
        remove_tree(sockdir)
