# SPDX-License-Identifier: Apache-2.0
"""Tests for MaruBackend integration with LMCache config and storage manager."""

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip(
    "lmcache.v1.storage_backend",
    reason="lmcache not importable (CUDA C extensions required)",
)

from lmcache.v1.config import LMCacheEngineConfig


class TestConfigFields:
    """Verify maru_path and maru_pool_size config fields."""

    def test_maru_path_default_none(self):
        config = LMCacheEngineConfig(chunk_size=256)
        assert config.maru_path is None

    def test_maru_pool_size_default(self):
        config = LMCacheEngineConfig(chunk_size=256)
        assert config.maru_pool_size == 4.0

    def test_maru_path_set(self):
        config = LMCacheEngineConfig(
            chunk_size=256,
            maru_path="tcp://localhost:5555",
        )
        assert config.maru_path == "tcp://localhost:5555"

    def test_maru_pool_size_set(self):
        config = LMCacheEngineConfig(
            chunk_size=256,
            maru_pool_size=8.0,
        )
        assert config.maru_pool_size == 8.0


class TestCreateStorageBackends:
    """Verify MaruBackend is created/skipped based on config."""

    def test_no_maru_backend_without_maru_path(self):
        """maru_path=None → MaruBackend not created."""
        import asyncio

        from lmcache.v1.metadata import LMCacheMetadata
        from lmcache.v1.storage_backend import CreateStorageBackends

        config = LMCacheEngineConfig(
            chunk_size=256,
            max_local_cpu_size=0,
        )
        metadata = MagicMock(spec=LMCacheMetadata)
        metadata.role = "scheduler"
        loop = asyncio.new_event_loop()

        try:
            backends = CreateStorageBackends(config, metadata, loop, dst_device="cpu")
            assert "MaruBackend" not in backends
        finally:
            loop.close()

    def test_maru_backend_created_with_maru_path(self):
        """maru_path set → MaruBackend created (with mocked handler)."""
        import asyncio

        from lmcache.v1.metadata import LMCacheMetadata
        from lmcache.v1.storage_backend import CreateStorageBackends

        config = LMCacheEngineConfig(
            chunk_size=256,
            max_local_cpu_size=0,
            maru_path="tcp://localhost:5555",
            maru_pool_size=1.0,
        )
        metadata = MagicMock(spec=LMCacheMetadata)
        metadata.role = "scheduler"
        loop = asyncio.new_event_loop()

        try:
            with patch(
                "lmcache.v1.storage_backend.maru_backend.MaruBackend.__init__",
                return_value=None,
            ) as mock_init:
                mock_init.return_value = None
                CreateStorageBackends(config, metadata, loop, dst_device="cpu")
                # MaruBackend.__init__ was called
                mock_init.assert_called_once()
        finally:
            loop.close()

    def test_maru_backend_skipped_when_in_skip_set(self):
        """MaruBackend in skip_backends → not created."""
        import asyncio

        from lmcache.v1.metadata import LMCacheMetadata
        from lmcache.v1.storage_backend import CreateStorageBackends

        config = LMCacheEngineConfig(
            chunk_size=256,
            max_local_cpu_size=0,
            maru_path="tcp://localhost:5555",
        )
        metadata = MagicMock(spec=LMCacheMetadata)
        metadata.role = "scheduler"
        loop = asyncio.new_event_loop()

        try:
            backends = CreateStorageBackends(
                config,
                metadata,
                loop,
                dst_device="cpu",
                skip_backends={"MaruBackend"},
            )
            assert "MaruBackend" not in backends
        finally:
            loop.close()

    def test_other_backends_unaffected_without_maru(self):
        """Without maru, existing backends work normally."""
        import asyncio

        from lmcache.v1.metadata import LMCacheMetadata
        from lmcache.v1.storage_backend import CreateStorageBackends

        config = LMCacheEngineConfig(
            chunk_size=256,
            max_local_cpu_size=0,
        )
        metadata = MagicMock(spec=LMCacheMetadata)
        metadata.role = "scheduler"
        loop = asyncio.new_event_loop()

        try:
            # Should not raise even though maru is not configured
            backends = CreateStorageBackends(config, metadata, loop, dst_device="cpu")
            assert isinstance(backends, dict)
        finally:
            loop.close()
