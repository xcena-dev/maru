# SPDX-License-Identifier: Apache-2.0
"""Configuration for Maru SGLang HiCache backend."""

import re
from dataclasses import dataclass


def parse_size(size_str: str | int | float) -> int:
    """Parse a human-readable size string into bytes.

    Examples: "4G" -> 4294967296, "512M" -> 536870912, "1.5G" -> 1610612736
    """
    if isinstance(size_str, int | float):
        return int(size_str)
    s = str(size_str).strip().upper()
    m = re.fullmatch(r"([\d.]+)\s*([KMGT]?)B?", s)
    if not m:
        raise ValueError(f"Invalid size string: {size_str!r}")
    value = float(m.group(1))
    unit = m.group(2)
    multiplier = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    return int(value * multiplier[unit])


@dataclass
class MaruSGLangConfig:
    """Configuration for the Maru HiCache storage backend.

    Parsed from SGLang's ``--hicache-storage-backend-extra-config`` JSON.
    """

    server_url: str = "tcp://localhost:5555"
    pool_size: int = 4 * 1024**3  # 4GB
    chunk_size_bytes: int = 1024 * 1024  # 1MB
    instance_id: str | None = None
    timeout_ms: int = 2000
    use_async_rpc: bool = True
    max_inflight: int = 64
    eager_map: bool = True
    dax_path: list[str] | str | None = None  # None = any pool

    @staticmethod
    def from_extra_config(extra: dict | None) -> "MaruSGLangConfig":
        """Parse from ``--hicache-storage-backend-extra-config`` JSON dict.

        Recognized keys (all prefixed with ``maru_``):
            maru_server_url, maru_pool_size, maru_chunk_size_bytes,
            maru_instance_id, maru_timeout_ms, maru_use_async_rpc,
            maru_max_inflight, maru_eager_map, maru_dax_path
        """
        if not extra:
            return MaruSGLangConfig()

        defaults = MaruSGLangConfig()

        raw_pool = extra.get("maru_pool_size", defaults.pool_size)
        pool_size = parse_size(raw_pool) if isinstance(raw_pool, str) else int(raw_pool)

        raw_chunk = extra.get("maru_chunk_size_bytes", defaults.chunk_size_bytes)
        chunk_size = (
            parse_size(raw_chunk) if isinstance(raw_chunk, str) else int(raw_chunk)
        )

        return MaruSGLangConfig(
            server_url=extra.get("maru_server_url", defaults.server_url),
            pool_size=pool_size,
            chunk_size_bytes=chunk_size,
            instance_id=extra.get("maru_instance_id", defaults.instance_id),
            timeout_ms=int(extra.get("maru_timeout_ms", defaults.timeout_ms)),
            use_async_rpc=extra.get("maru_use_async_rpc", defaults.use_async_rpc),
            max_inflight=int(extra.get("maru_max_inflight", defaults.max_inflight)),
            eager_map=extra.get("maru_eager_map", defaults.eager_map),
            dax_path=extra.get("maru_dax_path", defaults.dax_path),
        )
