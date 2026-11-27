# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Single-store placement and absence of local transfers."""

from maru_storage_service.config import parse_node_config


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
