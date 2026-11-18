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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from maru_vllm.connector import MaruKVConnector

__all__ = ["MaruKVConnector"]


def __getattr__(name: str):
    # Utilities such as copy_tracker do not need torch or vLLM. Load the
    # connector only when requested, then use normal attribute lookup.
    if name == "MaruKVConnector":
        from maru_vllm.connector import MaruKVConnector

        globals()[name] = MaruKVConnector
        return MaruKVConnector
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
