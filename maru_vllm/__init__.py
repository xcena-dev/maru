# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Maru KV Connector for vLLM - Direct CXL shared memory KV cache integration.

Usage with vLLM:
    vllm serve <model> --kv-transfer-config '{
        "kv_connector": "MaruKVConnector",
        "kv_connector_module_path": "maru_vllm",
        "kv_role": "kv_both",
        "kv_connector_extra_config": {
            "maru_server_url": "tcp://localhost:5555",
            "maru_pool_size": "4G"
        }
    }'
"""

from maru_vllm.connector import MaruKVConnector

__all__ = ["MaruKVConnector"]
