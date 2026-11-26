# Moving to maru_node

This guide is for native vLLM users of the removed `cpu`, `mixed` and `remote_cxl` backends. Select a node-owned store with `maru_node`. The directly mapped `cxl` backend, `maru-server`, LMCache and SGLang integrations keep their existing configuration.

## Process replacement

| Previous deployment | Current deployment |
|---|---|
| `maru-server --cpu-only` and vLLM with `cpu` | One `maru-node-server`, `maru-storage` on each GPU host with a `dram` store, and vLLM with `maru_node` |
| `maru-server` and vLLM with `mixed` | One `maru-node-server`, `maru-storage` with either `dram` or `local_cxl`, and vLLM with `maru_node` |
| `maru-server`, memory-node `maru-remote-cxl-agent`, and vLLM with `remote_cxl` | One `maru-node-server`, `maru-memory-node` on the memory host with `dram` or `local_cxl` and `remote_server`, GPU-host `maru-storage` with `remote`, and vLLM with `maru_node` |
| Agent and key installed with the Resource Manager | Install the Resource Manager for CXL; create and distribute the deployment key explicitly |

Stop the old engines and agent services before replacing their configuration. Verify that their worker processes have exited and their registrations are gone before reusing memory. Existing cache metadata is not imported. Keep the Resource Manager when using `local_cxl`; DRAM stores do not need it.

The removed commands are `maru-remote-cxl-agent` and `install-maru-remote-cxl-agent`. Agent-specific environment files and systemd drop-ins do not configure the memory-node service. `maru-server --cpu-only`, `--cpu-capacity-limit` and `--remote-cxl-agent*` are replaced by node configuration.

The command selects the service role; do not put `role` in JSON. Use the examples and complete validation rules in {doc}`../design_doc/node_storage_service`.

## Configure and start

1. Choose unique, stable `node_id` values. Set the same Catalog URL and `page_size` on connected nodes. The page must hold the largest chunk or (chunk, group) object stored by any worker.
2. For remote storage, create a key once and install the same contents on GPU services and memory nodes. Each service's user must be able to read its configured file; the file must not be world-readable. The Catalog and vLLM workers do not need copies.
3. Validate each JSON file with the matching service's `--check`, then start the Catalog, memory nodes, GPU services and vLLM, in that order.

Generate a key in an operator-owned directory, then install it at each configured `key_file` path with restricted permissions:

```bash
umask 077
openssl rand -hex 32 > maru.key
chmod 600 maru.key
```

Existing agent keys can be reused by copying their contents to the configured paths. `/etc/maru/maru.key` is the deployment path used in these examples; both remote configuration objects require an explicit `key_file`.

For a remote deployment:

```bash
maru-memory-node --config memory.json --check
maru-storage --config gpu.json --check
maru-node-server --host 192.0.2.1 --port 5556
```

In separate processes on the corresponding hosts:

```bash
maru-memory-node --config memory.json
maru-storage --config gpu.json
```

Each remote worker owns registered staging. Set `staging_max_bytes` to cover simultaneous TP and DP workers: its default admits only one worker. Workers and their GPU service must share a PID namespace. Configure checked RC/DC transport and disabled registration caching as described in {doc}`installation`.

Launch vLLM with a socket path, an engine name and an immutable weights namespace:

```bash
vllm serve <model> --kv-transfer-config '{
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
}'
```

All ranks use the same `maru_engine_id`; attachment identifies TP rank and DP index separately. The connector derives the sharing namespace from the supplied revision and model, dtype, chunk, TP and group geometry. A different namespace or incompatible geometry produces misses.

## Configuration mapping

Keys in the first column describe old configurations. Use only the current column with `maru_node`.

| Previous key or option | Current setting | Meaning |
|---|---|---|
| `maru_storage_backend` = `cpu`, `mixed`, `remote_cxl` | `maru_node` socket path | Store choice is in the GPU node's single `tiers` entry; omit `maru_storage_backend` |
| `maru_server_url` | Node `server` | One Catalog URL; default Catalog port is `5556`, compared with `5555` for `maru-server` |
| Comma-separated `maru_server_url` / `server_urls` | No equivalent | One Catalog; `remote.url` is one memory endpoint |
| `maru_cpu_pool_size` | `dram.size` | One node-wide pool, shared by local engines and workers |
| `maru_pool_size` for remote regions | Memory-node `local_cxl.size` and `region_size` | Fixed store capacity and region size belong to the memory node |
| `maru_cxl_pool_size` in `mixed` | `local_cxl.size` when choosing local CXL | A node has one store; it does not combine DRAM and CXL |
| `maru_write_order`, `maru_read_order` | No equivalent | One store |
| `maru_chunk_size` | Node `page_size` | Explicit page size on every node; rejected in connector configuration |
| `maru_auto_expand` | No equivalent | Size the node store explicitly |
| `maru_instance_id`, `maru_node_id` | Node `node_id` | Node identity belongs to the service |
| `maru_engine_id`, `maru_cache_namespace` | Same connector keys, both required | Engine/rank identity is separate from the sharing namespace |
| `maru_pin_pools`, `maru_eager_map` | Omit from connector | Workers attempt CUDA registration of local node pools; failed registration uses pageable copies. `dram.hugepage` defaults to `true` and can be `false`; it does not guarantee CUDA registration |
| `maru_rdma_staging_size` | GPU `remote.staging_size` | Per-worker size; default `max(1GiB, 64 * page_size)`; use `staging_max_bytes` for the node total |
| `rdma_load_reserve` | No equivalent | Worker staging and credit have fixed ownership rules; local-store `budgets.transfer_reserve_bytes` is not a remote load reserve |
| `rdma_nonblocking_alloc` | No equivalent | Credit is prepared outside the read path; unavailable capacity causes skip or MISS |
| `maru_rdma_device`, `maru_rdma_timeout_s` | GPU `remote.rdma_device`, `timeout_s` | Empty device means UCX choice; positive timeout defaults to `30` seconds |
| `maru_remote_cxl_agent_key_file` | GPU `remote.key_file` and memory `remote_server.key_file` | Required paths containing the same deployment key |
| `maru_read_hints`, `maru_read_hint_on_arrival` | Same connector keys | Remote advisory read hints; no receiver on node `dram` or `local_cxl` |
| `maru_write_hints`, `maru_write_wait_ms` | GPU `remote.write_hints`, `write_wait_ms` | Defaults `false` and `50`; write hints no longer follow the read-hint setting |
| Agent `--host`, `--port` | Memory `remote_server.listen` | Required `host:port`; no implicit listen address or port |
| Agent `--rm-address`, `--ucx-device`, `--key-file` | Memory `local_cxl.rm`, `remote_server.rdma_device`, `remote_server.key_file` | RM defaults to `127.0.0.1:9850`; device defaults to UCX choice |
| Agent `--node-id`, `--lock-file` | Node `node_id`, `state_dir` | `state_dir` defaults to `/run/maru`; no lock-file option |
| Agent `--region-extension`, `--region-extension-config` | Memory `region_extension.name`, `.config` | Installed `maru.region_extensions` entry point; requires `local_cxl` |
| Agent `--memory-generation-poll-s`, `--memory-generation-grace-s` | `region_extension.generation_poll_s`, `.generation_grace_s` | Positive intervals; defaults `0.1` and `2.0` seconds |
| Agent `--max-hint-requests` | `region_extension.hint_requests_max` | Positive integer, default `4096` |
| Agent `--allow-unchecked-transport` | No equivalent | Registration revocation requires checked transport |
| `maru_async_load`, `maru_async_store` | Same connector keys | Node advice defaults both to `false`; async scheduling, TP or multiple KV groups require async loads |

Use `maru_lookup_timeout_ms` (default `100`) for foreground lookup RPC timeouts, `maru_credit_wait_ms` (default `0`) for the per-step engine-thread store credit wait, and `maru_load_read_depth` (default `2`, minimum `1`) for async-load reads in flight.

## Support matrix

The previous column identifies the replaced behavior; it is not a list of available backends.

| Configuration or feature | Previous behavior | Node service support |
|---|---|---|
| GPU `dram` | `cpu`: worker-private pool | Yes; local node sharing |
| GPU `local_cxl` | `mixed` CXL portion | Yes; node-owned CXL regions |
| GPU `remote`, memory `local_cxl` | `remote_cxl` | Yes; worker direct RDMA |
| GPU `remote`, memory `dram` | No equivalent | Yes |
| Several GPU nodes using one memory node | Remote sharing | Yes; compatible namespace and geometry |
| Several engines on one GPU node | Private for `cpu` and `mixed` | Yes; shared node store |
| Several memory endpoints per GPU service | Multiple remote agents/pools | No; one `url` |
| Several memory nodes mapping the same region | Shared-pool remote agents | No; each memory node owns its regions |
| Several Catalogs per deployment | Remote server URL list | No |
| TP, DP, multiple KV groups | Restricted on `cpu` and `mixed` | Yes; TP and multiple groups require async loads |
| Synchronous load | Available with restrictions | Yes with one group and TP=1, with async scheduling off |
| Async load and async store | Remote only among removed backends | Yes; async load requires vLLM 0.16+ |
| Layerwise format and overlapped load | Rejected by removed backends | `dram` and `local_cxl` only; remote rejects both at startup |
| CUDA graphs | Removed backends required eager execution | Yes |
| Pipeline or context parallelism | Rejected | Rejected |
| Model layouts | Backend-specific restrictions | See {doc}`../integration/supported_models`; remote uses chunkwise format |
| Quantized (FP8) KV cache | Rejected by `cpu` and `remote_cxl` | Yes, except scales calculated at runtime (`calculate_kv_scales`), which are rejected at startup |
| Multimodal, LoRA, salted or embedding requests | Uncacheable | Computed without Maru caching |
| Read hints and promote | Agent extension | Remote memory-node extension; `cxl` keeps handler plugins |
| Write hint and bounded write wait | Agent extension | Remote reservation; configured on GPU service |
| Region extension and memory generation | Agent lifecycle | Memory-node `local_cxl` lifecycle |
| Several stores on one node, SSD | Mixed pools or no equivalent | Unsupported |
| Usage monitoring | Storage-session rows | `marutop usage` describes `cxl`; inspect service errors and worker read/store outcome logs for node stores |

## Visible behavior and recovery

Node-local keys can be read by other engines on that node when their namespace and geometry match. Remote keys belong to the memory node and can survive a storing GPU worker or GPU service exiting. The `cxl` registry and node Catalog do not exchange keys.

Remote keys become visible only after the memory node commits them. Reads use worker-owned registered staging; there is no GPU-node read cache. Concurrent reads can share an in-progress READ within one worker, without sharing staging across workers.

A full GPU `dram` or `local_cxl` store skips new stores. A full memory-node store evicts unpinned keys using one shared LRU. Source pins protect reads; a GPU-service restart preserves orphan pins for old live workers until their reads end or their exit is verified. If a `remote` worker's connection to its GPU service ends while that service keeps running, the worker stops remote loads and stores until its vLLM process restarts, and keeps in-flight reads until they finish. After a GPU-service restart, workers attach to the new run and recover. `dram` and `local_cxl` workers attach again to the same run.

A Catalog restart loses all keys. Service runs using DRAM lose their pools. Local CXL reuse requires matching Catalog, boot and memory generation plus safe region adoption. A memory reset invalidates that memory node's old keys.

The memory node fences writes per vLLM instance within a GPU-node run, revoking only that domain's write registration before reclaiming its unconfirmed credit. Ranks on another GPU node or run have a separate domain. A deadline alone does not free memory. Failed fencing retains quarantined pages. An unconfirmed READ retains its staging destination until the worker process exits. A WRITE starts after D2H completes and releases its source slot when it reports DONE, UNKNOWN or NOT_POSTED; UNKNOWN retires the write domain, and the destination credit requires fencing before reuse. A stopped worker returns no slots. If a READ remains unconfirmed or the worker stops, restart the entire affected instance and verify old workers have exited. See {doc}`../design_doc/memory_node` and {ref}`vllm-load-failures`.
