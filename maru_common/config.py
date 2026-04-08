# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
import os
from dataclasses import dataclass


def _parse_env_bool(name: str) -> bool | None:
    """Parse an optional boolean env var.

    Returns:
        - True/False if the env var is set to a recognized boolean value
        - None if the env var is unset

    Raises:
        ValueError: If the env var is set to an invalid boolean value
    """
    raw = os.environ.get(name)
    if raw is None:
        return None

    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False

    raise ValueError(
        f"{name} must be one of: 1/0, true/false, yes/no, on/off (got {raw!r})"
    )


@dataclass
class MaruConfig:
    """
    Configuration for Maru client.

    Attributes:
        server_url: URL of the MaruServer (e.g., "tcp://localhost:5555")
        instance_id: Unique identifier for this client instance
        pool_size: Default pool size to request (in bytes)
        auto_connect: Whether to automatically connect on initialization
    """

    server_url: str = "tcp://localhost:5555"
    instance_id: str | None = None
    pool_size: int = 1024 * 1024 * 100  # 100MB default
    chunk_size_bytes: int = 1024 * 1024  # 1MB default
    auto_connect: bool = True
    timeout_ms: int = 2000  # Socket timeout in milliseconds
    use_async_rpc: bool = True  # Use async DEALER-ROUTER RPC (RpcAsyncClient)
    max_inflight: int = 64  # Max concurrent in-flight async requests (backpressure)
    eager_map: bool = True  # Pre-map all shared regions on connect
    auto_expand: bool = True  # Auto-expand when pool is exhausted
    expand_size: int | None = None  # Expansion size in bytes (None means use pool_size)
    rm_address: str = "127.0.0.1:9850"  # Resource manager TCP address (host:port)

    def __post_init__(self):
        """Generate instance_id if not provided. Validate config."""
        if self.instance_id is None:
            import uuid

            self.instance_id = str(uuid.uuid4())

        # Optional env override for eager shared-region pre-mapping.
        env_eager_map = _parse_env_bool("MARU_EAGER_MAP")
        if env_eager_map is not None:
            self.eager_map = env_eager_map

        if self.chunk_size_bytes <= 0:
            raise ValueError(
                f"chunk_size_bytes must be positive, got {self.chunk_size_bytes}"
            )
        if self.pool_size < self.chunk_size_bytes:
            raise ValueError(
                f"pool_size ({self.pool_size}) must be >= "
                f"chunk_size_bytes ({self.chunk_size_bytes})"
            )

        if self.expand_size is not None:
            if not self.auto_expand:
                raise ValueError("expand_size requires auto_expand=True")
            if self.expand_size < self.chunk_size_bytes:
                raise ValueError(
                    f"expand_size ({self.expand_size}) must be >= "
                    f"chunk_size_bytes ({self.chunk_size_bytes})"
                )
