# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
import os
from dataclasses import dataclass, field


def reject_removed_backend(name: str, backend: str) -> None:
    replacement = {
        "cpu": ("a dram store", "quick_start.md"),
        "mixed": ("a dram or local_cxl store", "quick_start.md"),
        "remote_cxl": ("a remote store", "deployments/memory_node.md"),
    }.get(backend)
    if replacement:
        store, guide = replacement
        raise ValueError(
            f"{name}={backend!r} was removed; use maru_node with {store} "
            f"(docs/source/getting_started/{guide})"
        )


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
        auto_connect: Whether to connect on Handler context-manager entry;
            construction alone does not connect
    """

    server_url: str = "tcp://localhost:5555"
    instance_id: str | None = None
    pool_size: int = 1024 * 1024 * 100  # 100MB default
    chunk_size_bytes: int = 1024 * 1024  # 1MB default
    auto_connect: bool = True
    timeout_ms: int = 2000  # Socket timeout in milliseconds
    # Foreground PROBE and LOOKUP_AND_PIN timeout; keyword-only, so the
    # positions of the fields after it stay as they were
    lookup_timeout_ms: int = field(default=100, kw_only=True)
    use_async_rpc: bool = True  # Use async DEALER-ROUTER RPC (RpcAsyncClient)
    max_inflight: int = 64  # Max concurrent in-flight async requests (backpressure)
    eager_map: bool = True  # Pre-map all shared regions on connect
    auto_expand: bool = True  # Auto-expand when pool is exhausted
    expand_size: int | None = None  # Expansion size in bytes (None means use pool_size)
    rm_address: str = "127.0.0.1:9850"  # Resource manager TCP address (host:port)
    enable_stats: bool = False  # Enable handler-side stats reporting to server
    storage_backend: str = "cxl"
    metadata_only: bool = False
    engine_id: str | None = None  # Shared by one scheduler and its workers
    cache_namespace: str | None = None
    storage_schema: str = "opaque-bytes-v1"
    pin_pools: bool = True  # Register mapped node pools with CUDA
    rdma_device: str = ""  # RDMA NIC for NIXL/UCX, e.g. "mlx5_0:1"; empty = default
    rdma_staging_size: int | None = None  # Host staging buffer; None = derived
    rdma_timeout_s: float = 30.0  # Deadline of one RDMA batch
    # Share of staging slots that stores leave free for loads; set when loads
    # and stores run on separate threads (asynchronous transfers)
    rdma_load_reserve: float = 0.0
    read_hints: bool = False
    read_promotes: bool = False
    # UDS of this node's StorageService; set, the handler uses MaruClient
    maru_node: str | None = None
    # maru_node: how long store/batch_store wait for publication
    store_timeout_s: float = 10.0
    # maru_node: how long a step's stores wait in all for the credit the
    # step asked for when it started (0: never)
    credit_wait_s: float = 0.0
    # maru_node: how long the first connect waits for the node to listen
    node_connect_timeout_s: float = 30.0
    # maru_node: this worker's data-parallel index and tensor-parallel rank
    dp_index: int = 0
    tp_rank: int = 0
    # maru_node: a worker that never stores asks no credit
    load_only: bool = False
    # maru_node: objects this worker may store in one engine step (0: unknown)
    step_objects: int = 0

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
        if type(self.lookup_timeout_ms) is not int or self.lookup_timeout_ms <= 0:
            raise ValueError("lookup_timeout_ms must be a positive integer")
        reject_removed_backend("storage_backend", self.storage_backend)
        if self.storage_backend != "cxl":
            raise ValueError("storage_backend must be 'cxl'")
        if self.maru_node is not None:
            if not isinstance(self.maru_node, str) or not self.maru_node:
                raise ValueError("maru_node must be the path of the node socket")
            if not self.engine_id or not self.cache_namespace:
                raise ValueError("maru_node requires engine_id and cache_namespace")
            for name in ("dp_index", "tp_rank", "step_objects"):
                value = getattr(self, name)
                if type(value) is not int or value < 0:
                    raise ValueError(f"{name} must be an integer >= 0")
            if not 0 < self.store_timeout_s < float("inf"):
                raise ValueError("store_timeout_s must be a positive number")
            if not 0 <= self.credit_wait_s < float("inf"):
                raise ValueError("credit_wait_s must be a number >= 0")
            if not 0 <= self.node_connect_timeout_s < float("inf"):
                raise ValueError("node_connect_timeout_s must be a number >= 0")
        if "," in self.server_url:
            raise ValueError("server_url must name one MaruServer")
        if self.maru_node:
            if self.rdma_timeout_s <= 0:
                raise ValueError("rdma_timeout_s must be positive")
            if not 0 <= self.rdma_load_reserve < 1:
                raise ValueError("rdma_load_reserve must be in [0, 1)")
            if (
                self.rdma_staging_size is not None
                and self.rdma_staging_size < self.chunk_size_bytes
            ):
                raise ValueError("rdma_staging_size must hold at least one page")
        elif (
            self.rdma_staging_size is not None
            or self.rdma_device
            or self.rdma_load_reserve
        ):
            raise ValueError("RDMA settings require maru_node")
        if self.pool_size < 0:
            raise ValueError("pool_size must be nonnegative")
        # maru_node: the StorageService sizes the pool
        if (
            not self.metadata_only
            and self.maru_node is None
            and self.pool_size < self.chunk_size_bytes
        ):
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
