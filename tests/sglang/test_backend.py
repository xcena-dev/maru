# SPDX-License-Identifier: Apache-2.0
"""Tests for MaruStorage backend.

Uses MockMaruHandler to test without a live Maru server or SGLang.
"""

from unittest.mock import patch

import torch

from maru_sglang.maru_storage import MaruStorage

from .conftest import MockHiCacheStorageConfig, MockMaruHandler


def _make_backend(
    storage_config=None,
    handler=None,
) -> MaruStorage:
    """Create a MaruStorage with a mocked handler."""
    if storage_config is None:
        storage_config = MockHiCacheStorageConfig()

    # Patch _connect to inject mock handler
    with patch.object(MaruStorage, "_connect"):
        backend = MaruStorage(storage_config, {})

    backend._handler = handler or MockMaruHandler()
    backend._connected = True
    return backend


class TestKeySuffix:
    def test_standard_model(self):
        cfg = MockHiCacheStorageConfig(
            tp_rank=0, tp_size=2, model_name="meta-llama/Llama-3.1-8B"
        )
        backend = _make_backend(cfg)
        assert backend._suffix == "_meta-llama-Llama-3.1-8B_0_2"

    def test_mla_model(self):
        cfg = MockHiCacheStorageConfig(
            is_mla_model=True, model_name="deepseek-ai/DeepSeek-V3"
        )
        backend = _make_backend(cfg)
        assert backend._suffix == "_deepseek-ai-DeepSeek-V3"

    def test_model_name_none(self):
        """model_name=None is safely handled — no '_None' in suffix."""
        cfg = MockHiCacheStorageConfig(model_name=None, tp_rank=0, tp_size=1)
        backend = _make_backend(cfg)
        assert "None" not in backend._suffix
        assert backend._suffix == "__0_1"

    def test_make_key(self):
        backend = _make_backend()
        key = backend._make_key("abc123")
        assert key == "abc123_test-model_0_1"


class TestExists:
    def test_exists_miss(self):
        backend = _make_backend()
        assert backend.exists("nonexistent") is False

    def test_exists_after_set(self):
        backend = _make_backend()
        value = torch.ones(10, dtype=torch.float32)
        backend.set("key1", value=value)
        assert backend.exists("key1") is True

    def test_batch_exists_consecutive(self):
        backend = _make_backend()
        v = torch.ones(4, dtype=torch.float32)
        backend.set("k0", value=v)
        backend.set("k1", value=v)
        # k0, k1 exist; k2 does not
        count = backend.batch_exists(["k0", "k1", "k2"])
        assert count == 2

    def test_batch_exists_empty(self):
        backend = _make_backend()
        assert backend.batch_exists([]) == 0


class TestGetSet:
    def test_set_and_get(self):
        backend = _make_backend()
        value = torch.tensor([1.0, 2.0, 3.0, 4.0], dtype=torch.float32)
        assert backend.set("key1", value=value) is True

        result = backend.get("key1")
        assert result is not None
        assert result.dtype == torch.uint8
        assert result.numel() == value.numel() * value.element_size()

    def test_get_miss(self):
        backend = _make_backend()
        assert backend.get("nonexistent") is None

    def test_set_none_value(self):
        backend = _make_backend()
        assert backend.set("key1", value=None) is False

    def test_get_with_target_location(self):
        backend = _make_backend()
        value = torch.tensor([1.0, 2.0, 3.0, 4.0], dtype=torch.float32)
        backend.set("key1", value=value)

        target = torch.zeros(4, dtype=torch.float32)
        result = backend.get("key1", target_location=target)
        assert result is target
        # Verify data was copied into target
        assert torch.equal(result, value)


class TestBatchOperations:
    def test_batch_set_and_get(self):
        backend = _make_backend()
        values = [
            torch.tensor([1.0, 2.0], dtype=torch.float32),
            torch.tensor([3.0, 4.0], dtype=torch.float32),
        ]
        assert backend.batch_set(["a", "b"], values=values) is True

        results = backend.batch_get(["a", "b", "c"])
        assert results[0] is not None
        assert results[1] is not None
        assert results[2] is None

    def test_batch_set_none_values(self):
        backend = _make_backend()
        assert backend.batch_set(["a", "b"], values=None) is False

    def test_batch_set_empty(self):
        backend = _make_backend()
        assert backend.batch_set([], values=[]) is False

    def test_batch_get_with_targets(self):
        backend = _make_backend()
        v = torch.tensor([1.0, 2.0], dtype=torch.float32)
        backend.batch_set(["a"], values=[v])

        target = torch.zeros(2, dtype=torch.float32)
        results = backend.batch_get(["a"], target_locations=[target])
        assert results[0] is target
        assert torch.equal(results[0], v)


class TestDisconnected:
    @staticmethod
    def _disconnect(backend):
        """Simulate a disconnected backend that cannot reconnect."""
        backend._handler = None
        backend._connected = False

    def test_exists_disconnected(self):
        backend = _make_backend()
        self._disconnect(backend)
        with patch.object(MaruStorage, "_connect", side_effect=RuntimeError):
            assert backend.exists("key1") is False

    def test_get_disconnected(self):
        backend = _make_backend()
        self._disconnect(backend)
        with patch.object(MaruStorage, "_connect", side_effect=RuntimeError):
            assert backend.get("key1") is None

    def test_set_disconnected(self):
        backend = _make_backend()
        self._disconnect(backend)
        with patch.object(MaruStorage, "_connect", side_effect=RuntimeError):
            assert backend.set("key1", value=torch.ones(4)) is False

    def test_batch_exists_disconnected(self):
        backend = _make_backend()
        self._disconnect(backend)
        with patch.object(MaruStorage, "_connect", side_effect=RuntimeError):
            assert backend.batch_exists(["k"]) == 0


class TestClose:
    def test_close(self):
        backend = _make_backend()
        assert backend._handler is not None
        backend.close()
        assert backend._handler is None

    def test_close_idempotent(self):
        backend = _make_backend()
        backend.close()
        backend.close()  # should not raise
