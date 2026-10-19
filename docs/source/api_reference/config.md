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
    pool_size: int = 104_857_600  # 100MB
    chunk_size_bytes: int = 1_048_576  # 1MB
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
    pool_size=1024 * 1024 * 1024,  # 1GB
    chunk_size_bytes=2 * 1024 * 1024,  # 2MB pages
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
| `--dax-path` | any pool | DAX device path for allocation. Repeat for multiple pools (fill-first fallback). |
| `--region-mapper` | none | Region mapper (`host:port`) of a memory node of this pool, for `remote_cxl` workers. Repeat or separate with commas for every memory node of a shared pool |
| `--region-mapper-key-file` | `/etc/maru/region-mapper.key` | Copy of the region mapper key; read when `--region-mapper` is given |

### Examples

```bash
# Default (any available pool)
maru-server

# Pin to a specific DAX device
maru-server --dax-path /dev/dax0.0

# Multiple pools (fill-first fallback)
maru-server --dax-path /dev/dax0.0 --dax-path /dev/dax1.0

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

### Remote CXL (`maru_storage_backend: "remote_cxl"`)

Engines on nodes without CXL keep KV in the CXL memory of memory nodes and move
it over RDMA (see the [vLLM guide](../integration/vllm.md)). These settings
apply to that backend only:

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `maru_storage_backend` | `str` | `cxl` | `remote_cxl` selects the backend |
| `maru_engine_id` | `str` | required | Names the engine's storage session; unique among the engines of one maru-server |
| `maru_cache_namespace` | `str` | required | Sharing scope: engines with the same namespace and model shape share KV, whichever path they load the weights from |
| `maru_pool_size` | `str \| int` | required | Region size; the engine gets one more region whenever its regions are full |
| `maru_region_mapper_key_file` | `str` | `/etc/maru/region-mapper.key` | Copy of the memory nodes' region mapper key, readable only by the engine's user |
| `maru_rdma_device` | `str` | UCX default | The engine's RDMA NIC, e.g. `mlx5_0:1` |
| `maru_rdma_staging_size` | `str \| int` | two `max_model_len` requests, at least 1 GiB | Host staging buffer registered with the NIC and CUDA |
| `maru_rdma_timeout_s` | `float` | `30` | Deadline of one RDMA batch |

### Region mapper (`maru-region-mapper`)

One per memory node, installed as `maru-region-mapper.service` with NIXL. Each
option also comes from the environment variable in parentheses, which the
service reads from `/etc/maru/region-mapper.env`.

| Option | Default | Description |
| --- | --- | --- |
| `--rm-address` (`MARU_RM_ADDRESS`) | `127.0.0.1:9850` | Resource manager of the pool |
| `--ucx-device` (`MARU_REGION_MAPPER_UCX_DEVICE`) | UCX default | RDMA NIC to register regions with, e.g. `mlx5_1:1` |
| `--host` (`MARU_REGION_MAPPER_HOST`) | `127.0.0.1`; the service: empty | Bind address; empty means the address of the `--ucx-device` interface |
| `--port` (`MARU_REGION_MAPPER_PORT`) | `9860` | Bind port |
| `--key-file` (`MARU_REGION_MAPPER_KEY_FILE`) | `/etc/maru/region-mapper.key` | Key every connection must prove; the installer creates it |
| `--lock-file` (`MARU_REGION_MAPPER_LOCK_FILE`) | `/run/maru/region-mapper-<node>.lock` | Keeps one region mapper per node |
| `--allow-unchecked-transport` | off | Accept UCX transports that do not check remote keys (tests on one host only) |

---

## LMCache Configuration

For LMCache YAML configuration (plugin settings, `extra_config` parameters), see [LMCache](../integration/lmcache.md#configuration).
