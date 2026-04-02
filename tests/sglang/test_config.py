# SPDX-License-Identifier: Apache-2.0
"""Tests for MaruSGLangConfig parsing."""

import pytest

from maru_sglang.config import MaruSGLangConfig, parse_size


class TestParseSize:
    def test_int_passthrough(self):
        assert parse_size(1024) == 1024

    def test_string_number(self):
        assert parse_size("1024") == 1024

    def test_gigabytes(self):
        assert parse_size("4G") == 4 * 1024**3

    def test_megabytes(self):
        assert parse_size("500M") == 500 * 1024**2

    def test_kilobytes(self):
        assert parse_size("128K") == 128 * 1024

    def test_terabytes(self):
        assert parse_size("1T") == 1024**4

    def test_with_b_suffix(self):
        assert parse_size("4GB") == 4 * 1024**3

    def test_lowercase(self):
        assert parse_size("4g") == 4 * 1024**3

    def test_invalid(self):
        with pytest.raises(ValueError):
            parse_size("not_a_size")


class TestMaruSGLangConfig:
    def test_defaults(self):
        cfg = MaruSGLangConfig()
        assert cfg.server_url == "tcp://localhost:5555"
        assert cfg.pool_size == 4 * 1024**3
        assert cfg.chunk_size_bytes == 1024 * 1024
        assert cfg.eager_map is True

    def test_from_extra_config_none(self):
        cfg = MaruSGLangConfig.from_extra_config(None)
        assert cfg.server_url == "tcp://localhost:5555"

    def test_from_extra_config_empty(self):
        cfg = MaruSGLangConfig.from_extra_config({})
        assert cfg.pool_size == 4 * 1024**3

    def test_from_extra_config_full(self):
        extra = {
            "maru_server_url": "tcp://10.0.0.1:6666",
            "maru_pool_size": "8G",
            "maru_chunk_size_bytes": "2M",
            "maru_instance_id": "test-instance",
            "maru_timeout_ms": 5000,
            "maru_use_async_rpc": False,
            "maru_max_inflight": 32,
            "maru_eager_map": False,
        }
        cfg = MaruSGLangConfig.from_extra_config(extra)
        assert cfg.server_url == "tcp://10.0.0.1:6666"
        assert cfg.pool_size == 8 * 1024**3
        assert cfg.chunk_size_bytes == 2 * 1024**2
        assert cfg.instance_id == "test-instance"
        assert cfg.timeout_ms == 5000
        assert cfg.use_async_rpc is False
        assert cfg.max_inflight == 32
        assert cfg.eager_map is False

    def test_from_extra_config_partial(self):
        extra = {"maru_server_url": "tcp://custom:1234"}
        cfg = MaruSGLangConfig.from_extra_config(extra)
        assert cfg.server_url == "tcp://custom:1234"
        assert cfg.pool_size == 4 * 1024**3  # default preserved

    def test_pool_size_int(self):
        extra = {"maru_pool_size": 2147483648}
        cfg = MaruSGLangConfig.from_extra_config(extra)
        assert cfg.pool_size == 2147483648
