# SPDX-License-Identifier: Apache-2.0
"""
Maru LMCache Plugin — external remote storage connector for upstream LMCache.

Install:
    pip install maru[lmcache]

LMCache YAML config:
    remote_url: "maru://localhost:5555?pool_size=1G"
    remote_storage_plugins: ["maru"]
    extra_config:
      remote_storage_plugin.maru.module_path: maru_lmcache.adapter
      remote_storage_plugin.maru.class_name: MaruConnectorAdapter
"""

__all__ = ["MaruConnectorAdapter", "MaruConnector"]


def __getattr__(name: str):
    if name == "MaruConnectorAdapter":
        from maru_lmcache.adapter import MaruConnectorAdapter

        return MaruConnectorAdapter
    if name == "MaruConnector":
        from maru_lmcache.connector import MaruConnector

        return MaruConnector
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
