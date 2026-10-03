# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
import os
from dataclasses import dataclass

from .storage_policy import validate_order


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
        pool_size: Default pool size to request (in bytes). For the CPU and
            remote backends it is the local buffer capacity: the CPU pool, or
            the remote backend's staging buffer.
        auto_connect: Whether to automatically connect on initialization
        storage_backend: Where KV bytes live: "cxl" (local DAX mapping, the
            default), "cpu" (worker DRAM), "mixed" (CPU and CXL) or "remote"
            (a CXL pool on another node, reached over RDMA).
        remote_url: Control endpoint of the pool node's maru-remote-server
            (remote backend only), e.g. "tcp://pool-node:6600".
        remote_ucx_device: UCX device of the local RDMA NIC, e.g.
            "mlx5_0:1"; empty for the UCX default.
        remote_transfer_timeout_s: Deadline of one RDMA READ/WRITE batch.
        remote_retry_s: How long the remote backend stops calling the server
            after a failed or timed-out control request.
        remote_load_reserve: Share of the remote staging slots that stores
            leave free for loads (0 to 1). A store that would take one of
            them is skipped, so a slow pool makes stores, not loads, give way.
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
    enable_stats: bool = False  # Enable handler-side stats reporting to server
    storage_backend: str = "cxl"
    metadata_only: bool = False
    engine_id: str | None = None  # Shared by one scheduler and one CPU worker
    cache_namespace: str | None = None
    node_id: str | None = None  # Defaults to hostname; used for CPU host budgets
    storage_schema: str = "opaque-bytes-v1"
    cxl_pool_size: int | None = None  # Fixed CXL budget in mixed mode
    write_order: tuple[str, ...] = ("cpu", "cxl")
    read_order: tuple[str, ...] = ("cpu", "cxl")
    remote_url: str | None = None
    remote_ucx_device: str = ""
    remote_transfer_timeout_s: float = 30.0
    remote_retry_s: float = 30.0
    remote_load_reserve: float = 0.5

    def __post_init__(self):
        """Generate instance_id if not provided. Validate config."""
        if self.instance_id is None:
            import uuid

            self.instance_id = str(uuid.uuid4())

        # Optional env overrides
        env_eager_map = _parse_env_bool("MARU_EAGER_MAP")
        if env_eager_map is not None:
            self.eager_map = env_eager_map

        env_stats = _parse_env_bool("MARU_STAT")
        if env_stats is not None:
            self.enable_stats = env_stats

        if self.chunk_size_bytes <= 0:
            raise ValueError(
                f"chunk_size_bytes must be positive, got {self.chunk_size_bytes}"
            )
        if self.storage_backend not in {"cpu", "cxl", "mixed", "remote"}:
            raise ValueError(
                "storage_backend must be 'cpu', 'cxl', 'mixed' or 'remote'"
            )
        if self.storage_backend == "remote":
            self._validate_remote()
        elif self.remote_url is not None:
            raise ValueError("remote_url is only supported by remote storage")
        if self.storage_backend == "mixed":
            if (
                type(self.cxl_pool_size) is not int
                or self.cxl_pool_size <= 0
                or (
                    not self.metadata_only
                    and self.cxl_pool_size < self.chunk_size_bytes
                )
            ):
                raise ValueError(
                    "mixed storage requires cxl_pool_size >= chunk_size_bytes"
                )
            if self.cxl_pool_size // self.chunk_size_bytes > 1_000_000:
                raise ValueError("CXL pool supports at most 1000000 pages")
            for name in ("write_order", "read_order"):
                setattr(self, name, validate_order(getattr(self, name)))
        elif self.cxl_pool_size is not None:
            raise ValueError("cxl_pool_size is only supported by mixed storage")
        if self.storage_backend in {"cpu", "mixed"}:
            if not self.engine_id or not self.cache_namespace:
                raise ValueError("CPU storage requires engine_id and cache_namespace")
            if self.expand_size is not None:
                raise ValueError("CPU M1 uses a fixed pool; expand_size is unsupported")
            if (
                not self.metadata_only
                and self.pool_size // self.chunk_size_bytes > 1_000_000
            ):
                raise ValueError("CPU pool supports at most 1000000 pages")
        if self.pool_size < 0:
            raise ValueError("pool_size must be nonnegative")
        if not self.metadata_only and self.pool_size < self.chunk_size_bytes:
            raise ValueError(
                f"pool_size ({self.pool_size}) must be >= "
                f"chunk_size_bytes ({self.chunk_size_bytes})"
            )

        if self.expand_size is not None:
            if self.storage_backend == "remote":
                raise ValueError("remote storage uses a fixed staging buffer")
            if not self.auto_expand:
                raise ValueError("expand_size requires auto_expand=True")
            if self.expand_size < self.chunk_size_bytes:
                raise ValueError(
                    f"expand_size ({self.expand_size}) must be >= "
                    f"chunk_size_bytes ({self.chunk_size_bytes})"
                )

    def _validate_remote(self) -> None:
        """Check the settings the remote backend needs.

        Raises:
            ValueError: if a required setting is missing or out of range.
        """
        if not self.remote_url:
            raise ValueError("remote storage requires remote_url")
        if not self.cache_namespace:
            raise ValueError(
                "remote storage requires cache_namespace (the key sharing scope)"
            )
        if self.cxl_pool_size is not None:
            raise ValueError("cxl_pool_size is only supported by mixed storage")
        for name in ("remote_transfer_timeout_s", "remote_retry_s"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, int | float)
                or value <= 0
            ):
                raise ValueError(f"{name} must be a positive number")
        reserve = self.remote_load_reserve
        if (
            isinstance(reserve, bool)
            or not isinstance(reserve, int | float)
            or not 0 <= reserve < 1
        ):
            raise ValueError("remote_load_reserve must be in [0, 1)")
