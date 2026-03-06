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

## LMCache Integration

When using Maru as an LMCache backend, configuration is done via the LMCache YAML config file.

```yaml
chunk_size: 256
local_cpu: True
max_local_cpu_size: 5
enable_async_loading: True

# Disable P2P for Maru shared storage mode
enable_p2p: False
enable_controller: False

# Maru backend — format: maru://<host>:<port>
remote_url: "maru://localhost:5555"
remote_serde: "naive"

extra_config:
  maru_pool_size: "4G"              # CXL memory pool size ("1G", "500M", etc.)
  save_chunk_meta: False
  lookup_backoff_time: 0.001
  # maru_instance_id: "my-id"       # Unique client ID (default: auto UUID)
  # maru_operation_timeout: 10.0    # Per-operation timeout in seconds
  # maru_timeout_ms: 2000           # ZMQ socket timeout (ms)
  # maru_use_async_rpc: true        # Async DEALER-ROUTER RPC
  # maru_max_inflight: 64           # Max in-flight async requests
```

### Maru extra_config Parameters

| Parameter | Default | Description |
| --- | --- | --- |
| `maru_pool_size` | `"1G"` | CXL memory pool size. Supports human-readable strings (`"4G"`, `"500M"`) or integer bytes |
| `maru_instance_id` | auto-generated UUID | Unique client instance identifier |
| `maru_operation_timeout` | `10.0` | Timeout in seconds for individual KV operations |
| `maru_timeout_ms` | `2000` | ZMQ socket timeout in milliseconds for RPC communication |
| `maru_use_async_rpc` | `true` | Use async DEALER-ROUTER pattern for higher throughput |
| `maru_max_inflight` | `64` | Max concurrent in-flight async RPC requests |
| `maru_server_url` | (from `remote_url`) | Override server URL. Normally not needed |
| `maru_auto_connect` | `true` | Auto-connect to MaruServer on initialization |
| `maru_eager_map` | `true` | Pre-map all shared regions on connect |
