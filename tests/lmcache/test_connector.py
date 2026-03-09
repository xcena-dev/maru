# SPDX-License-Identifier: Apache-2.0
"""Tests for MaruConnector and MaruConnectorConfig."""

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip(
    "lmcache.v1.storage_backend.connector",
    reason="lmcache not importable (CUDA C extensions required)",
)

from maru_lmcache.connector import (
    MaruConnector,
    MaruConnectorConfig,
    cache_key_to_int,
    parse_size,
)

# ---------------------------------------------------------------------------
# parse_size
# ---------------------------------------------------------------------------


class TestParseSize:
    @pytest.mark.parametrize(
        "input_val, expected",
        [
            ("1G", 1024**3),
            ("2g", 2 * 1024**3),
            ("500M", 500 * 1024**2),
            ("1024K", 1024 * 1024),
            ("4GB", 4 * 1024**3),
            ("100", 100),
            (42, 42),
        ],
    )
    def test_valid_sizes(self, input_val, expected):
        assert parse_size(input_val) == expected


# ---------------------------------------------------------------------------
# MaruConnectorConfig
# ---------------------------------------------------------------------------


class TestMaruConnectorConfig:
    def test_from_url_defaults(self):
        cfg = MaruConnectorConfig.from_url("maru://localhost:5555")
        assert cfg.server_url == "tcp://localhost:5555"
        assert cfg.pool_size == 1024**3  # 1G default
        assert cfg.instance_id is None

    def test_from_url_with_params(self):
        cfg = MaruConnectorConfig.from_url(
            "maru://10.0.0.1:7777?pool_size=4G&instance_id=worker-0&timeout=60"
        )
        assert cfg.server_url == "tcp://10.0.0.1:7777"
        assert cfg.pool_size == 4 * 1024**3
        assert cfg.instance_id == "worker-0"
        assert cfg.connection_timeout == 60.0

    def test_from_lmcache_config(self):
        from lmcache.v1.config import LMCacheEngineConfig

        config = LMCacheEngineConfig(
            chunk_size=256,
            remote_url="maru://localhost:5555",
            extra_config={
                "maru_server_url": "tcp://10.0.0.2:6666",
                "maru_pool_size": "2G",
                "maru_instance_id": "test-instance",
            },
        )
        cfg = MaruConnectorConfig.from_lmcache_config(config)
        assert cfg.server_url == "tcp://10.0.0.2:6666"
        assert cfg.pool_size == 2 * 1024**3
        assert cfg.instance_id == "test-instance"

    def test_from_lmcache_config_with_fallback(self):
        from lmcache.v1.config import LMCacheEngineConfig

        config = LMCacheEngineConfig(
            chunk_size=256,
            remote_url="maru://localhost:5555",
            extra_config={"maru_pool_size": "8G"},
        )
        fallback = MaruConnectorConfig(
            server_url="tcp://fallback:9999",
            instance_id="fallback-id",
        )
        cfg = MaruConnectorConfig.from_lmcache_config(config, fallback=fallback)
        # pool_size from extra_config
        assert cfg.pool_size == 8 * 1024**3
        # server_url/instance_id from fallback
        assert cfg.server_url == "tcp://fallback:9999"
        assert cfg.instance_id == "fallback-id"


# ---------------------------------------------------------------------------
# cache_key_to_int
# ---------------------------------------------------------------------------


class TestCacheKeyToInt:
    def test_deterministic(self):
        key = MagicMock()
        key.to_string.return_value = "model|layer|token_range|fmt"
        h1 = cache_key_to_int(key)
        h2 = cache_key_to_int(key)
        assert h1 == h2

    def test_different_keys_differ(self):
        k1, k2 = MagicMock(), MagicMock()
        k1.to_string.return_value = "key_a"
        k2.to_string.return_value = "key_b"
        assert cache_key_to_int(k1) != cache_key_to_int(k2)

    def test_returns_unsigned_int(self):
        key = MagicMock()
        key.to_string.return_value = "test_key"
        result = cache_key_to_int(key)
        assert isinstance(result, int)
        assert result >= 0


# ---------------------------------------------------------------------------
# MaruConnector (with mocked MaruHandler)
# ---------------------------------------------------------------------------


def _make_connector(async_loop, mock_handler):
    """Create a MaruConnector with a pre-injected mock handler."""
    from lmcache.v1.config import LMCacheEngineConfig
    from lmcache.v1.metadata import LMCacheMetadata

    config = LMCacheEngineConfig(
        chunk_size=256,
        remote_url="maru://localhost:5555",
        extra_config={"save_chunk_meta": False},
    )

    # Mock metadata to avoid needing a real vLLM model config
    import torch

    metadata = MagicMock(spec=LMCacheMetadata)
    metadata.get_shapes.return_value = [torch.Size([2, 32, 256, 128])]
    metadata.get_dtypes.return_value = [torch.float16]
    metadata.use_mla = False
    metadata.chunk_size = 256
    metadata.get_num_groups.return_value = 1

    maru_config = MaruConnectorConfig(auto_connect=False)

    with (
        patch(
            "lmcache.v1.storage_backend.connector.base_connector.get_size_bytes",
            return_value=256 * 2 * 32 * 128 * 2,  # fake chunk size
        ),
        patch(
            "lmcache.v1.storage_backend.connector.base_connector.init_remote_metadata_info"
        ),
        patch(
            "lmcache.v1.storage_backend.connector.base_connector.get_remote_metadata_bytes",
            return_value=0,
        ),
    ):
        connector = MaruConnector(
            url="maru://localhost:5555",
            loop=async_loop,
            config=config,
            metadata=metadata,
            maru_config=maru_config,
        )

    # Inject mock handler
    connector._handle = mock_handler
    connector._connected = True
    return connector


class TestMaruConnector:
    def test_exists(self, async_loop, mock_maru_handler):
        connector = _make_connector(async_loop, mock_maru_handler)
        mock_maru_handler.exists.return_value = True

        key = MagicMock()
        key.to_string.return_value = "test_key"

        result = async_loop.run_until_complete(connector.exists(key))
        assert result is True
        mock_maru_handler.exists.assert_called_once()

    def test_exists_returns_false_when_disconnected(self, async_loop):
        maru_config = MaruConnectorConfig(auto_connect=False)

        # Can't create a real connector without metadata, so test the
        # _ensure_connected path directly
        connector = MagicMock(spec=MaruConnector)
        connector._connected = False
        connector._handle = None
        connector._ensure_connected = MagicMock(return_value=False)
        connector.maru_config = maru_config

        # Call the real exists method
        result = async_loop.run_until_complete(
            MaruConnector.exists(connector, MagicMock())
        )
        assert result is False

    def test_exists_sync(self, async_loop, mock_maru_handler):
        connector = _make_connector(async_loop, mock_maru_handler)
        mock_maru_handler.exists.return_value = False

        key = MagicMock()
        key.to_string.return_value = "test_key"

        result = connector.exists_sync(key)
        assert result is False

    def test_put_and_get(self, async_loop, mock_maru_handler, mock_memory_info):
        connector = _make_connector(async_loop, mock_maru_handler)
        mock_maru_handler.store.return_value = True
        mock_maru_handler.retrieve.return_value = mock_memory_info

        key = MagicMock()
        key.to_string.return_value = "test_key"

        # put
        memory_obj = MagicMock()
        memory_obj.byte_array = memoryview(bytearray(1024))

        with patch("maru_lmcache.connector.MaruConnector._encode_memory_obj") as enc:
            enc.return_value = mock_memory_info
            async_loop.run_until_complete(connector.put(key, memory_obj))

        mock_maru_handler.store.assert_called_once()

        # get
        with (
            patch.object(connector, "_decode_memory_obj") as dec,
            patch.object(connector, "reshape_partial_chunk") as reshape,
        ):
            dec.return_value = MagicMock()
            reshape.return_value = dec.return_value
            result = async_loop.run_until_complete(connector.get(key))

        assert result is not None
        mock_maru_handler.retrieve.assert_called_once()

    def test_close(self, async_loop, mock_maru_handler):
        connector = _make_connector(async_loop, mock_maru_handler)

        async_loop.run_until_complete(connector.close())
        mock_maru_handler.close.assert_called_once()
        assert connector._handle is None
        assert connector._connected is False

    def test_ping_success(self, async_loop, mock_maru_handler):
        connector = _make_connector(async_loop, mock_maru_handler)
        mock_maru_handler.healthcheck.return_value = True

        result = async_loop.run_until_complete(connector.ping())
        assert result == 0  # PING_SUCCESS

    def test_ping_not_connected(self, async_loop, mock_maru_handler):
        connector = _make_connector(async_loop, mock_maru_handler)
        connector._connected = False

        result = async_loop.run_until_complete(connector.ping())
        assert result == 1  # PING_NOT_CONNECTED

    def test_remove_sync(self, async_loop, mock_maru_handler):
        connector = _make_connector(async_loop, mock_maru_handler)
        mock_maru_handler.delete.return_value = True

        key = MagicMock()
        key.to_string.return_value = "test_key"
        assert connector.remove_sync(key) is True

    def test_list_returns_empty(self, async_loop, mock_maru_handler):
        connector = _make_connector(async_loop, mock_maru_handler)
        result = async_loop.run_until_complete(connector.list())
        assert result == []

    def test_repr(self, async_loop, mock_maru_handler):
        connector = _make_connector(async_loop, mock_maru_handler)
        r = repr(connector)
        assert "MaruConnector" in r
        assert "connected=True" in r


# ---------------------------------------------------------------------------
# Batch operations
# ---------------------------------------------------------------------------


class TestBatchOperations:
    def test_support_flags(self, async_loop, mock_maru_handler):
        connector = _make_connector(async_loop, mock_maru_handler)
        assert connector.support_batched_get() is True
        assert connector.support_batched_put() is True
        assert connector.support_batched_async_contains() is True
        assert connector.support_batched_contains() is True
        assert connector.support_batched_get_non_blocking() is True
        assert connector.support_ping() is True

    def test_batched_contains(self, async_loop, mock_maru_handler):
        connector = _make_connector(async_loop, mock_maru_handler)
        mock_maru_handler.batch_exists.return_value = [True, True, False]

        keys = [MagicMock() for _ in range(3)]
        for i, k in enumerate(keys):
            k.to_string.return_value = f"key_{i}"

        result = connector.batched_contains(keys)
        assert result == 2  # 2 consecutive hits

    def test_batched_async_contains(self, async_loop, mock_maru_handler):
        connector = _make_connector(async_loop, mock_maru_handler)
        mock_maru_handler.batch_exists.return_value = [True, True, True]

        keys = [MagicMock() for _ in range(3)]
        for i, k in enumerate(keys):
            k.to_string.return_value = f"key_{i}"

        result = async_loop.run_until_complete(
            connector.batched_async_contains("lookup-1", keys)
        )
        assert result == 3

    def test_batched_get(self, async_loop, mock_maru_handler, mock_memory_info):
        connector = _make_connector(async_loop, mock_maru_handler)
        mock_maru_handler.batch_retrieve.return_value = [
            mock_memory_info,
            None,
            mock_memory_info,
        ]

        keys = [MagicMock() for _ in range(3)]
        for i, k in enumerate(keys):
            k.to_string.return_value = f"key_{i}"

        with (
            patch.object(connector, "_decode_memory_obj") as dec,
            patch.object(connector, "reshape_partial_chunk") as reshape,
        ):
            obj = MagicMock()
            dec.return_value = obj
            reshape.return_value = obj
            results = async_loop.run_until_complete(connector.batched_get(keys))

        assert len(results) == 3
        assert results[0] is not None
        assert results[1] is None
        assert results[2] is not None

    def test_batched_put(self, async_loop, mock_maru_handler, mock_memory_info):
        connector = _make_connector(async_loop, mock_maru_handler)
        mock_maru_handler.batch_store.return_value = [True, True]

        keys = [MagicMock() for _ in range(2)]
        for i, k in enumerate(keys):
            k.to_string.return_value = f"key_{i}"

        objs = [MagicMock() for _ in range(2)]
        for obj in objs:
            obj.byte_array = memoryview(bytearray(1024))

        with patch("maru_lmcache.connector.MaruConnector._encode_memory_obj") as enc:
            enc.return_value = mock_memory_info
            async_loop.run_until_complete(connector.batched_put(keys, objs))

        mock_maru_handler.batch_store.assert_called_once()

    def test_batched_get_non_blocking_consecutive_prefix(
        self, async_loop, mock_maru_handler, mock_memory_info
    ):
        connector = _make_connector(async_loop, mock_maru_handler)
        # Second key is a miss → only first returned
        mock_maru_handler.batch_retrieve.return_value = [
            mock_memory_info,
            None,
            mock_memory_info,
        ]

        keys = [MagicMock() for _ in range(3)]
        for i, k in enumerate(keys):
            k.to_string.return_value = f"key_{i}"

        with (
            patch.object(connector, "_decode_memory_obj") as dec,
            patch.object(connector, "reshape_partial_chunk") as reshape,
        ):
            obj = MagicMock()
            dec.return_value = obj
            reshape.return_value = obj
            results = async_loop.run_until_complete(
                connector.batched_get_non_blocking("lookup-1", keys)
            )

        # Only first item (before the None) should be returned
        assert len(results) == 1
