# Configuration

The native vLLM connector uses the node backend (selected by `maru_node`) or the directly mapped `cxl` backend. Set node capacity, page size, the `server` address and RDMA settings in service JSON.

## Node services

| Process and command | Purpose | Defaults and options |
|---|---|---|
| MaruServer (`maru-server --backend node`) | Tracks node metadata | `--host 127.0.0.1`, `--port 5556`, `--log-level INFO` |
| GPU StorageService (`maru-storage --role gpu`) | Serves local workers (default role) | Requires `--config FILE`. `--check` prints resolved defaults. Log level defaults to `INFO`. |
| memory StorageService (`maru-storage --role memory`) | Serves a remote pool | Requires `--config FILE`. Same check and logging options. |

Options for the node MaruServer:

| Option | Default | Meaning |
|---|---|---|
| `--host`, `--port` | `127.0.0.1`, `5556` | Bind address and port |
| `--log-level` | `INFO` | `DEBUG`, `INFO`, `WARNING` or `ERROR` |
| `--session-ttl-s` | `30` | Positive node/worker session TTL; source pins do not expire with it |
| `--heartbeat-s` | Session TTL / 3 | Positive interval below the session TTL |
| `--node-give-up-s` | `600` | Positive time before an expired node's locations are removed |
| `--op-window` | `65536` | Unacknowledged operation results retained per origin |
| `--op-bytes-max` | `67108864` | Byte limit for those results |
| `--policy-file` | None | JSON preserve policy, reloaded on changes |

The command selects the role. JSON rejects `role`, unknown keys and duplicate keys. `--check` validates configuration and store support without allocating pools, loading the extension or starting RDMA. For the MaruServer address, `--check` only verifies a nonempty string. It does not check that a key file, device or endpoint can be used at runtime.

| Configuration | Required | Main optional settings |
|---|---|---|
| Common | `node_id`, `server`, `page_size`, exactly one `tiers` entry | `state_dir` (`/run/maru`), `budgets` |
| GPU service | Store `dram`, `local_cxl` or `remote` | `socket` (`/run/maru/node.sock`), `socket_group`, `local_pin` (only `false`) |
| Memory service | Store `dram` or `local_cxl` | `remote_server`, `region_extension` |
| `dram` | `size` | `hugepage` (`true`) |
| `local_cxl` | `size` to serve | `region_size` (`max(1GiB, ceil(size / 1024))`, rounded up to a multiple of `page_size`), `rm` (`127.0.0.1:9850`), `visibility` (`coherent` or `flush`) |
| `remote` | One `url`, `key_file` | `rdma_device` (empty), `timeout_s` (`30`), `staging_size`, `staging_max_bytes`, `quarantine_max`, `write_hints` (`false`), `write_wait_ms` (`50`) |
| `remote_server` | `listen`, `key_file` | `rdma_device`, `reserve_ttl_s` (`60`), `peer_quarantine_max`, `quarantine_max` |
| `region_extension` | `name`; memory `local_cxl` only | `config` (empty), `generation_poll_s` (`0.1`), `generation_grace_s` (`2.0`), `hint_requests_max` (`4096`) |

`high_watermark` (`0.9`) and `low_watermark` (`0.8`) are memory-store options only. Remote `staging_size` defaults to `max(1GiB, 64 * page_size)`, rounded up to a multiple of `page_size`, per worker; `staging_max_bytes` defaults to that same amount for the whole GPU node. Increase the total for simultaneous workers.

See {doc}`../design_doc/node_storage_service` for every store and budget validation rule, derived defaults and complete JSON examples. See {doc}`../design_doc/read_hints` for extension hooks and memory generations.

(node-connector-settings)=

## vLLM node configuration

```json
{
  "kv_connector": "MaruKVConnector",
  "kv_connector_module_path": "maru_vllm",
  "kv_role": "kv_both",
  "kv_load_failure_policy": "recompute",
  "kv_connector_extra_config": {
    "maru_node": "/run/maru/node.sock",
    "maru_engine_id": "engine-1",
    "maru_cache_namespace": "model-immutable-revision",
    "maru_async_load": true
  }
}
```

| Connector key | Default | Meaning |
|---|---|---|
| `maru_node` | Required for node stores | GPU service socket path |
| `maru_engine_id` | Required | Engine identity; ranks share it |
| `maru_cache_namespace` | Required | Immutable weights revision; connector binds it to KV geometry |
| `maru_kv_chunk_tokens` | `256` | Tokens per stored chunk, aligned to vLLM blocks |
| `maru_async_load` | Node advice (`false`) | Deferred load; required for async scheduling, TP > 1 or multiple KV groups |
| `maru_async_store` | Node advice (`false`) | Asynchronous store completion |
| `maru_lookup_timeout_ms` | `100` | Foreground Catalog lookup timeout |
| `maru_credit_wait_ms` | `0` | Engine-thread store credit wait budget per step |
| `maru_load_read_depth` | `2` | Deferred reads in flight; minimum `1` |
| `maru_read_hints` | `false` | Advisory scheduler read hints to remote extensions |
| `maru_read_hint_on_arrival` | `false` | Hint on request arrival with the compatible vLLM hook |

`kv_load_failure_policy` must be `recompute`. Remote stores reject `maru_use_layerwise` and `maru_overlap_load_with_compute`. Synchronous loads require TP=1, one KV group and async scheduling off. See {doc}`../integration/vllm` for transfer modes and model restrictions.

| Configuration | Keys the connector rejects with `maru_node` |
|---|---|
| Store capacity and order | `maru_pool_size`, `maru_cpu_pool_size`, `maru_cxl_pool_size`, `maru_auto_expand`, `maru_write_order`, `maru_read_order` |
| Page size and mapping | `maru_chunk_size`, `maru_pin_pools`, `maru_eager_map` |
| Server, identity and backend | `maru_server_url`, `maru_node_id`, `maru_instance_id`, `maru_storage_backend` |
| Remote credentials and write hints | `maru_remote_cxl_agent_key_file`, `maru_write_hints`, `maru_write_wait_ms` |
| Remote staging and RDMA | `maru_rdma_device`, `maru_rdma_staging_size`, `maru_rdma_timeout_s` |

Configure remote staging size, RDMA device and timeout on the GPU StorageService. ATTACH supplies the effective values to the worker.

## MaruConfig

The Python client configuration is `maru_common.MaruConfig`:

```python
from maru import MaruConfig, MaruHandler

config = MaruConfig(
    maru_node="/run/maru/node.sock",
    engine_id="engine-1",
    cache_namespace="model-immutable-revision",
)
```

| Field | Default | Meaning |
|---|---|---|
| `maru_node` | `None` | Set to use a node service |
| `engine_id`, `cache_namespace` | `None` | Required when using a node service |
| `server_url` | `tcp://localhost:5555` | Address of MaruServer (`maru-server --backend cxl`) for directly mapped CXL. Node services supply their MaruServer address through `server`. |
| `instance_id` | Generated UUID | `cxl` client identity |
| `pool_size` | `100MiB` | Initial `cxl` region allocation |
| `chunk_size_bytes` | `1MiB` | Required object/page size on the Python interface |
| `auto_connect` | `True` | Connect on context-manager entry |
| `auto_expand` | `True` | Expand `cxl` allocation when full |
| `timeout_ms` | `2000` | Metadata socket timeout |
| `lookup_timeout_ms` | `100` | Node foreground lookup timeout |
| `use_async_rpc` | `True` | Async DEALER/ROUTER RPC |
| `max_inflight` | `64` | Maximum in-flight async RPCs |
| `eager_map` | `True` | Premap shared `cxl` regions |
| `rm_address` | `127.0.0.1:9850` | Resource Manager address |
| `store_timeout_s` | `10` | Python store publication wait on node stores |
| `credit_wait_s` | `0` | Node store credit wait budget |
| `node_connect_timeout_s` | `30` | Initial node connection wait |
| `read_hints`, `read_promotes` | `False` | Advisory hints; see {doc}`plugins` and {doc}`../design_doc/read_hints` |

`MARU_EAGER_MAP` and `MARU_STAT` override eager mapping and handler statistics respectively. Boolean environment values accept `1/0`, `true/false`, `yes/no` and `on/off`.

## Direct CXL configuration

Running `maru-server` without `--backend` selects `cxl`. MaruServer accepts these options for `cxl`:

| Option | Default | Meaning |
|---|---|---|
| `--host` | `127.0.0.1` | Bind address |
| `--port` | `5555` | Bind port |
| `--log-level` | `INFO` | Logging level |
| `--rm-address` | `127.0.0.1:9850` | Resource Manager address |
| `--dax-path` | Any pool | Repeat for multiple DAX pools |

The native vLLM `cxl` configuration uses `maru_server_url` (`tcp://localhost:5555`), `maru_pool_size` (`1G`), `maru_chunk_size` (`4M` unless sized from the model), `maru_instance_id` (generated) and `maru_eager_map` (`true`). These are separate from service JSON.

For LMCache YAML settings, see {doc}`../integration/lmcache`.
