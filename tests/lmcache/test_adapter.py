# SPDX-License-Identifier: Apache-2.0
"""Tests for MaruConnectorAdapter and plugin discovery."""

import pytest

pytest.importorskip(
    "lmcache.v1.storage_backend.connector",
    reason="lmcache not importable (CUDA C extensions required)",
)

from maru_lmcache.adapter import MaruConnectorAdapter


class TestMaruConnectorAdapter:
    """Unit tests for the adapter."""

    def test_schema_is_maru(self):
        adapter = MaruConnectorAdapter()
        assert adapter.schema == "maru://"

    def test_can_parse_maru_url(self):
        adapter = MaruConnectorAdapter()
        assert adapter.can_parse("maru://localhost:5555")
        assert adapter.can_parse("maru://10.0.0.1:5555?pool_size=2G")

    def test_cannot_parse_other_schemes(self):
        adapter = MaruConnectorAdapter()
        assert not adapter.can_parse("redis://localhost:6379")
        assert not adapter.can_parse("s3://bucket")
        assert not adapter.can_parse("")

    def test_create_connector_requires_config_and_metadata(self, async_loop):
        from lmcache.v1.storage_backend.connector import ConnectorContext

        adapter = MaruConnectorAdapter()
        context = ConnectorContext(
            url="maru://localhost:5555",
            loop=async_loop,
            local_cpu_backend=None,
            config=None,
            metadata=None,
        )
        with pytest.raises(ValueError, match="requires config and metadata"):
            adapter.create_connector(context)


class TestPluginDiscovery:
    """Test that upstream LMCache discovers our adapter via plugin system."""

    def test_connector_manager_loads_maru_adapter(self, async_loop, lmcache_config):
        from lmcache.v1.storage_backend.connector import ConnectorManager

        manager = ConnectorManager(
            url="maru://localhost:5555?pool_size=1G",
            loop=async_loop,
            local_cpu_backend=None,
            config=lmcache_config,
        )

        adapter_names = [a.__class__.__name__ for a in manager.adapters]
        assert "MaruConnectorAdapter" in adapter_names

    def test_connector_manager_can_parse_maru_url(self, async_loop, lmcache_config):
        from lmcache.v1.storage_backend.connector import ConnectorManager

        manager = ConnectorManager(
            url="maru://localhost:5555?pool_size=1G",
            loop=async_loop,
            local_cpu_backend=None,
            config=lmcache_config,
        )

        matched = [a for a in manager.adapters if a.can_parse("maru://localhost:5555")]
        assert len(matched) == 1
        assert matched[0].__class__.__name__ == "MaruConnectorAdapter"

    def test_not_loaded_without_remote_storage_plugins(self, async_loop):
        """Without remote_storage_plugins config, Maru adapter is not loaded."""
        from lmcache.v1.config import LMCacheEngineConfig
        from lmcache.v1.storage_backend.connector import ConnectorManager

        config = LMCacheEngineConfig(
            chunk_size=256,
            remote_url="redis://localhost:6379",
        )

        manager = ConnectorManager(
            url="redis://localhost:6379",
            loop=async_loop,
            local_cpu_backend=None,
            config=config,
        )

        adapter_names = [a.__class__.__name__ for a in manager.adapters]
        assert "MaruConnectorAdapter" not in adapter_names
