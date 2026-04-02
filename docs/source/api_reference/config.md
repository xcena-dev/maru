# Configuration

## MaruConfig

Client-side configuration dataclass.

```python
from maru import MaruConfig
```

```python
@dataclass
class MaruConfig:
    server_url: str = "tcp://localhost:5555"
    instance_id: str | None = None
    pool_size: int = 104_857_600          # 100MB
    chunk_size_bytes: int = 1_048_576     # 1MB
    auto_connect: bool = True
    timeout_ms: int = 2000
    use_async_rpc: bool = True
    max_inflight: int = 64
    eager_map: bool = True
    pool_id: list[int] | int | None = None
```

### Parameters

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `server_url` | `str` | `"tcp://localhost:5555"` | ZeroMQ endpoint of MaruServer |
| `instance_id` | `str \| None` | `None` (auto-generated UUID) | Unique client identifier |
| `pool_size` | `int` | `104_857_600` (100MB) | Initial memory pool size to request (bytes) |
| `chunk_size_bytes` | `int` | `1_048_576` (1MB) | Page size for paged allocation (bytes) |
| `auto_connect` | `bool` | `True` | Connect automatically on `MaruHandler.__init__` |
| `timeout_ms` | `int` | `2000` | RPC socket timeout in milliseconds |
| `use_async_rpc` | `bool` | `True` | Use async DEALER-ROUTER RPC client |
| `max_inflight` | `int` | `64` | Max concurrent in-flight async RPC requests |
| `eager_map` | `bool` | `True` | Pre-map all shared regions on connect (can be overridden by `MARU_EAGER_MAP` env var) |
| `pool_id` | `list[int] \| int \| None` | `None` (any pool) | Pin allocations to specific DAX device pool(s). A single `int` pins to one pool; a `list[int]` enables ordered fallback — when the first pool is exhausted, the next is tried. `None` lets the resource manager choose. Valid range per element: `[0, 0xFFFFFFFE]` |

### Examples

**Default configuration:**

```python
config = MaruConfig()
with MaruHandler(config) as handler:
    ...
```

**Custom configuration:**

```python
config = MaruConfig(
    server_url="tcp://10.0.0.1:5555",
    instance_id="worker-0",
    pool_size=1024 * 1024 * 1024,    # 1GB
    chunk_size_bytes=2 * 1024 * 1024, # 2MB pages
    timeout_ms=5000,
)
```

**Pin to a specific DAX pool:**

```python
config = MaruConfig(
    server_url="tcp://10.0.0.1:5555",
    pool_size=1024 * 1024 * 1024,    # 1GB
    pool_id=1,                        # Use pool 1 (/dev/dax1.0)
)
```

**Multi-pool fallback (try pool 0 first, then pool 1):**

```python
config = MaruConfig(
    server_url="tcp://10.0.0.1:5555",
    pool_size=1024 * 1024 * 1024,    # 1GB
    pool_id=[0, 1],                   # Try pool 0 first, fall back to pool 1
)
```

**Synchronous RPC (for debugging):**

```python
config = MaruConfig(
    use_async_rpc=False,
    timeout_ms=10000,
)
```

---

## MaruServer Configuration

The MaruServer is configured via CLI arguments.

```bash
maru-server [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `--host` | `127.0.0.1` | Bind address |
| `--port` | `5555` | Bind port |
| `--log-level` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |

### Examples

```bash
# Default
maru-server

# Production
maru-server --host 0.0.0.0 --port 5555 --log-level WARNING

# Debug
maru-server --host 127.0.0.1 --port 5556 --log-level DEBUG
```

---

## vLLM KV Connector Configuration

When using MaruKVConnector with vLLM, configuration is passed via `kv_connector_extra_config` in the `--kv-transfer-config` JSON:

```bash
vllm serve <model> --kv-transfer-config '{
    "kv_connector": "MaruKVConnector",
    "kv_connector_module_path": "maru_vllm",
    "kv_role": "kv_both",
    "kv_connector_extra_config": {
        "maru_server_url": "tcp://localhost:5555",
        "maru_pool_size": "4G"
    }
}'
```

### Parameters

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `maru_server_url` | `str` | `tcp://localhost:5555` | MaruServer address |
| `maru_pool_size` | `str \| int` | `1G` | CXL memory pool size (`4G`, `500M`, or integer bytes) |
| `maru_chunk_size` | `str \| int` | `4M` | Maru page size (CXL allocation unit) |
| `maru_instance_id` | `str` | auto-generated UUID | Unique client instance identifier |
| `maru_eager_map` | `bool` | `true` | Pre-map other instances' CXL regions on connect |
| `maru_kv_chunk_tokens` | `int` | `256` | KV cache chunk granularity (in tokens). Auto-aligned to vLLM `block_size` |

For full architecture and data path details, see [vLLM](../integration/vllm.md#configuration).

---

## LMCache Configuration

For LMCache YAML configuration (plugin settings, `extra_config` parameters), see [LMCache](../integration/lmcache.md#configuration).
