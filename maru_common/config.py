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
    pool_id: list[int] | int | None = None  # None means any pool (ANY_POOL_ID)

    # Auth (optional — all must be set to enable mTLS auth)
    auth_server_addr: str | None = None  # gRPC auth server address (e.g., "localhost:50051")
    client_cert: str | None = None       # Client cert path (with SPIFFE ID in SAN)
    client_key: str | None = None        # Client private key path
    ca_cert: str | None = None           # CA cert path (trust bundle)

    def __post_init__(self):
        """Generate instance_id if not provided. Validate config."""
        if self.instance_id is None:
            import uuid

            self.instance_id = str(uuid.uuid4())

        # Optional env override for eager shared-region pre-mapping.
        env_eager_map = _parse_env_bool("MARU_EAGER_MAP")
        if env_eager_map is not None:
            self.eager_map = env_eager_map

        # Auth config from environment (set by naru/launcher per instance)
        if self.auth_server_addr is None:
            self.auth_server_addr = os.environ.get("MARU_AUTH_SERVER_ADDR")
        if self.client_cert is None:
            self.client_cert = os.environ.get("MARU_CLIENT_CERT")
        if self.client_key is None:
            self.client_key = os.environ.get("MARU_CLIENT_KEY")
        if self.ca_cert is None:
            self.ca_cert = os.environ.get("MARU_CA_CERT")

        # Normalize pool_id to list[int] | None
        if self.pool_id is None:
            pass  # None stays None (means ANY_POOL_ID)
        elif isinstance(self.pool_id, (list, tuple)) and len(self.pool_id) == 0:
            self.pool_id = None  # empty list/tuple → any pool
        elif isinstance(self.pool_id, int):
            if not (0 <= self.pool_id <= 0xFFFFFFFE):
                raise ValueError(
                    f"pool_id must be in range [0, 0xFFFFFFFE], got {self.pool_id}. "
                    "Use None (or omit) to allow any pool."
                )
            self.pool_id = [self.pool_id]
        else:
            # list[int]
            for pid in self.pool_id:
                if not (0 <= pid <= 0xFFFFFFFE):
                    raise ValueError(
                        f"pool_id must be in range [0, 0xFFFFFFFE], got {pid}. "
                        "Use None (or omit) to allow any pool."
                    )
            self.pool_id = list(self.pool_id)

        if self.chunk_size_bytes <= 0:
            raise ValueError(
                f"chunk_size_bytes must be positive, got {self.chunk_size_bytes}"
            )
        if self.pool_size < self.chunk_size_bytes:
            raise ValueError(
                f"pool_size ({self.pool_size}) must be >= "
                f"chunk_size_bytes ({self.chunk_size_bytes})"
            )
