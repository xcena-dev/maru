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
    storage_backend: str = "cxl"  # "cpu", "cxl", "mixed" or "remote_cxl"
    engine_id: str | None = None
    cache_namespace: str | None = None
    rdma_device: str = ""
    rdma_staging_size: int | None = None
    rdma_timeout_s: float = 30.0
    rdma_load_reserve: float = 0.0
    remote_cxl_agent_key_file: str = "~/.maru/remote-cxl-agent.key"
    read_hints: bool = False
```

### Parameters

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `server_url` | `str` | `"tcp://localhost:5555"` | ZeroMQ endpoint of MaruServer. With `storage_backend="remote_cxl"`, a comma-separated list of endpoints, one maru-server per pool |
| `instance_id` | `str \| None` | `None` (auto-generated UUID) | Unique client identifier |
| `pool_size` | `int` | `104_857_600` (100MB) | Initial memory pool size to request (bytes) |
| `chunk_size_bytes` | `int` | `1_048_576` (1MB) | Page size for paged allocation (bytes) |
| `auto_connect` | `bool` | `True` | Connect automatically on `MaruHandler.__init__` |
| `timeout_ms` | `int` | `2000` | RPC socket timeout in milliseconds |
| `use_async_rpc` | `bool` | `True` | Use async DEALER-ROUTER RPC client |
| `max_inflight` | `int` | `64` | Max concurrent in-flight async RPC requests |
| `eager_map` | `bool` | `True` | Pre-map all shared regions on connect (can be overridden by `MARU_EAGER_MAP` env var) |
| `storage_backend` | `str` | `"cxl"` | Where KV lives: `"cxl"` (CXL shared memory mapped on this node), `"cpu"`, `"mixed"`, or `"remote_cxl"` (CXL memory of memory nodes over RDMA) |
| `engine_id` | `str \| None` | `None` | Engine identity of a storage session; required for `cpu`, `mixed` and `remote_cxl` |
| `cache_namespace` | `str \| None` | `None` | Namespace whose keys a storage session shares; required for `cpu`, `mixed` and `remote_cxl` |
| `rdma_device` | `str` | `""` (UCX default) | `remote_cxl` only: RDMA NIC for NIXL/UCX, e.g. `"mlx5_0:1"` |
| `rdma_staging_size` | `int \| None` | `None` (derived) | `remote_cxl` only: host staging buffer registered with the NIC and CUDA (bytes); `None` uses the larger of 64 pages and 1 GiB |
| `rdma_timeout_s` | `float` | `30.0` | `remote_cxl` only: deadline of one RDMA batch in seconds |
| `rdma_load_reserve` | `float` | `0.0` | `remote_cxl` only: share of staging slots, in `[0, 1)`, that stores leave free for loads; set when loads and stores run on separate threads |
| `remote_cxl_agent_key_file` | `str` | `"~/.maru/remote-cxl-agent.key"` | `remote_cxl` only: copy of the memory nodes' remote CXL agent key, readable by the worker's user only |
| `read_hints` | `bool` | `False` | `remote_cxl` only: send [read hints](../design_doc/read_hints.md) to agents whose region extension takes them. A metadata-only client sends what `MaruHandler.hint_read` names; a client that loads hints each load as its READs start |

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
| `--remote-cxl-agent` | none | Remote CXL agent (`host:port`) of a memory node of this pool, for `remote_cxl` workers. Repeat or separate with commas for every memory node of a shared pool |
| `--remote-cxl-agent-key-file` | `~/.maru/remote-cxl-agent.key` | Copy of the remote CXL agent key; read when `--remote-cxl-agent` is given |

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
| `maru_server_url` | `str` | `tcp://localhost:5555` | MaruServer address. With `remote_cxl`, a comma-separated list, one maru-server per pool |
| `maru_pool_size` | `str \| int` | `1G` | CXL memory an engine gets at a time (`4G`, `500M`, or integer bytes): its first allocation, and each further one when its memory is full. Required with `remote_cxl`, where each allocation is one region; `cpu` and `mixed` use `maru_cpu_pool_size` and `maru_cxl_pool_size` |
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
| `maru_pool_size` | `str \| int` | required | CXL memory the engine gets at a time, as with `cxl`: its first region, and one more of this size whenever its regions are full |
| `maru_remote_cxl_agent_key_file` | `str` | `~/.maru/remote-cxl-agent.key` | Copy of the memory nodes' remote CXL agent key, readable only by the engine's user |
| `maru_rdma_device` | `str` | UCX default | The engine's RDMA NIC, e.g. `mlx5_0:1` |
| `maru_rdma_staging_size` | `str \| int` | two `max_model_len` requests, at least 1 GiB | Host staging buffer registered with the NIC and CUDA |
| `maru_rdma_timeout_s` | `float` | `30` | Deadline of one RDMA batch |
| `maru_read_hints` | `bool` | `false` | The scheduler tells the memory nodes which pages each load will read before it starts, so a device with a cache in front of slower media can prefetch them; see [Read hints](../design_doc/read_hints.md). Needs `maru_async_load` (no hints with a warning otherwise) |
| `maru_read_hint_on_arrival` | `bool` | `false` | Send the read hint when vLLM receives a request, not when the scheduler first looks it up; needs `maru_async_load` (off with a warning otherwise) |

### Remote CXL agent (`maru-remote-cxl-agent`)

One per memory node, installed as `maru-remote-cxl-agent.service` with NIXL. An
option with an environment variable in parentheses also takes its value from
it, and the service reads these variables from `/etc/maru/remote-cxl-agent.env`.

| Option | Default | Description |
| --- | --- | --- |
| `--rm-address` (`MARU_RM_ADDRESS`) | `127.0.0.1:9850` | Resource manager of the pool |
| `--ucx-device` (`MARU_REMOTE_CXL_AGENT_UCX_DEVICE`) | UCX default | RDMA NIC to register regions with, e.g. `mlx5_1:1` |
| `--host` (`MARU_REMOTE_CXL_AGENT_HOST`) | `127.0.0.1`; the service: empty | Bind address; empty means the address of the `--ucx-device` interface |
| `--port` (`MARU_REMOTE_CXL_AGENT_PORT`) | `9860` | Bind port |
| `--key-file` (`MARU_REMOTE_CXL_AGENT_KEY_FILE`) | `/etc/maru/remote-cxl-agent.key` | Key every connection must prove; the installer creates it |
| `--node-id` | hostname | Name of the memory node; names the default lock file |
| `--lock-file` (`MARU_REMOTE_CXL_AGENT_LOCK_FILE`) | `/run/maru/remote-cxl-agent-<node>.lock` | Keeps one remote CXL agent per node |
| `--region-extension` (`MARU_REMOTE_CXL_AGENT_REGION_EXTENSION`) | none | Name of an installed region extension (entry point in `maru.region_extensions`) that takes read hints and hears region maps; see [Read hints](../design_doc/read_hints.md). A package that provides one can set it for the service with a drop-in on `maru-remote-cxl-agent.service` |
| `--region-extension-config` (`MARU_REMOTE_CXL_AGENT_REGION_EXTENSION_CONFIG`) | empty | Opaque string given to that extension |
| `--max-hint-requests` | `4096` | Read hint requests one connection may keep open; more are refused |
| `--memory-generation-poll-s` | `0.1` | How often the extension's memory generation is read; a reset is acted on within the extension's detection time plus this |
| `--memory-generation-grace-s` | `2.0` | How long the memory generation may go unanswered before every region is released and maps are refused until it answers |
| `--allow-unchecked-transport` | off | Accept UCX transports that do not check remote keys (tests on one host only) |
| `--log-level` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |

---

## LMCache Configuration

For LMCache YAML configuration (plugin settings, `extra_config` parameters), see [LMCache](../integration/lmcache.md#configuration).
