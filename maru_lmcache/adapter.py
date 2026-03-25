# SPDX-License-Identifier: Apache-2.0
"""
MaruConnectorAdapter — registers the ``maru://`` URL scheme with LMCache's
plugin discovery system (``remote_storage_plugins``).

This module is referenced in LMCache YAML config as::

    remote_storage_plugins: ["maru"]
    extra_config:
      remote_storage_plugin.maru.module_path: maru_lmcache.adapter
      remote_storage_plugin.maru.class_name: MaruConnectorAdapter
"""

import logging

from lmcache.v1.storage_backend.connector import (
    ConnectorAdapter,
    ConnectorContext,
    parse_remote_url,
)
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector

logger = logging.getLogger(__name__)


class MaruConnectorAdapter(ConnectorAdapter):
    """Adapter that registers the ``maru://`` URL scheme."""

    def __init__(self) -> None:
        super().__init__("maru://")

    def create_connector(self, context: ConnectorContext) -> RemoteConnector:
        logger.info("Creating Maru connector for URL: %s", context.url)

        # Validate URL format (requires host:port)
        _ = parse_remote_url(context.url)

        from maru_lmcache.connector import MaruConnector, MaruConnectorConfig

        maru_config = MaruConnectorConfig.from_url(context.url)

        # Override with extra_config if present
        if context.config and context.config.extra_config:
            maru_config = MaruConnectorConfig.from_lmcache_config(
                context.config, fallback=maru_config
            )

        logger.info(
            "Maru config: server_url=%s, pool_size=%s, pool_id=%s, instance_id=%s",
            maru_config.server_url,
            maru_config.pool_size,
            maru_config.pool_id,
            maru_config.instance_id,
        )

        if context.config is None or context.metadata is None:
            raise ValueError("Maru connector requires config and metadata")

        return MaruConnector(
            url=context.url,
            loop=context.loop,
            config=context.config,
            metadata=context.metadata,
            maru_config=maru_config,
        )
