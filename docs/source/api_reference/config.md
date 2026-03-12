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
    pool_id: int | None = None
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
| `pool_id` | `int \| None` | `None` (any pool) | Pin allocations to a specific DAX device pool. `None` lets the resource manager choose. Valid range: `[0, 0xFFFFFFFE]` |

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

## LMCache Configuration

For LMCache YAML configuration (plugin settings, `extra_config` parameters), see [LMCache Integration](../integration/lmcache.md#configuration).
