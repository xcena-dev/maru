# vLLM with a remote CXL pool

The remote backend keeps KV in a CXL pool on **another node** and moves it over
RDMA. Workers do not open a DAX device; one pool node serves every worker that
can reach it, so engines on different servers reuse each other's prefixes.

```
worker node                                 pool node
vLLM + MaruKVConnector                      maru-server + resource manager
  MaruHandler(storage_backend="remote")     maru-remote-server
    staging buffer  <==== RDMA (NIXL) ====>   CXL regions (registered with NIXL)
    control client  ----- ZMQ ---------->     reserve / publish / lookup / release
```

The worker-side backend implements the same handler contract as the CPU
backend: `alloc` returns a writable staging slot, `batch_store` publishes it,
and `batch_retrieve` returns read leases that the connector releases after its
copy. The connector therefore uses the same synchronous chunkwise path as
[CPU mode](../cpu/README.md), with the same limits: `TP=PP=DP=1`,
`--enforce-eager`, `--no-async-scheduling`, unquantized KV, one KV cache group,
and `kv_load_failure_policy="recompute"`. Async load/store, layerwise storage and
the `maru_kv_ops` kernels are not used with this backend. A request's chunks are
loaded in batches the staging buffer can hold, each copied to the GPU and
released before the next.

## Requirements

- An RDMA NIC on each node (RoCE or InfiniBand) and [NIXL](https://github.com/ai-dynamo/nixl)
  with its UCX backend: `pip install 'maru[remote]'`.
- On the pool node: a CXL/DAX device, the resource manager and MaruServer, as in
  a local CXL deployment.
- Every worker must reach the pool node's control port and RDMA address.

## Run

Pool node (replace the DAX path and NIC):

```bash
install-maru-resource-manager           # once, as for local CXL
maru-server --host 127.0.0.1 --port 5555 --dax-path /dev/dax1.0
MARU_PLUGINS=none CUDA_VISIBLE_DEVICES= maru-remote-server \
  --server-url tcp://127.0.0.1:5555 --pool-size 8G --page-bytes 4M \
  --ctrl-url tcp://0.0.0.0:6600 --ucx-device mlx5_1:1 --pool-id pool-a
```

`--page-bytes` must hold one KV object: all layers of one
`maru_kv_chunk_tokens` chunk (3 MiB for Qwen2.5-0.5B with 256-token chunks).
The pool grows past `--pool-size` region by region as Maru's CXL pool does.
`MARU_PLUGINS=none` keeps device plugins out of the server process and the empty
`CUDA_VISIBLE_DEVICES` keeps it from registering the pool with CUDA.

Each worker node (replace the model, namespace, address and NIC):

```bash
vllm serve <model> --enforce-eager --no-async-scheduling \
  --kv-transfer-config '{
    "kv_connector": "MaruKVConnector",
    "kv_connector_module_path": "maru_vllm",
    "kv_role": "kv_both",
    "kv_load_failure_policy": "recompute",
    "kv_connector_extra_config": {
      "maru_storage_backend": "remote",
      "maru_remote_url": "tcp://pool-node:6600",
      "maru_remote_ucx_device": "mlx5_0:1",
      "maru_cache_namespace": "my-model-immutable-weights-revision",
      "maru_kv_chunk_tokens": 256
    }
  }'
```

`maru_cache_namespace` is the sharing scope. The connector combines it with the
model configuration, dtype, block size and chunk size, so engines share KV only
when all of these match; where a node keeps the weights and its transformers
version do not count. Use a namespace that identifies the exact weights. Each
engine logs the resulting value at start-up (`cache namespace <hash>`), so nodes
can be compared. `maru_engine_id` is not needed.

| Setting | Default | Meaning |
|---|---|---|
| `maru_remote_url` | required | Control endpoint of `maru-remote-server` |
| `maru_remote_ucx_device` | UCX default | Local RDMA NIC for NIXL, e.g. `mlx5_0:1` |
| `maru_remote_staging_size` | 64 KV objects, at least `1G` | Local staging buffer (RDMA source and target) |
| `maru_remote_timeout_s` | `30` | Deadline of one RDMA batch |
| `maru_remote_retry_s` | `30` | How long the worker stops calling the pool after a failure |

`maru_pool_size`, `maru_cpu_pool_size` and the mixed-mode settings do not apply.

## Behaviour

- **Store.** Chunks completed in a step are copied GPU → staging, then one
  reserve, one RDMA WRITE batch and one publish per request. A key that already
  exists keeps its first value; the store still counts as present.
- **Load.** The scheduler asks the pool which prefix chunks exist. The worker
  pins them, RDMA READs them into staging, unpins them, copies staging → GPU and
  releases the leases. A missing chunk or any failure is reported to vLLM as a
  load error and the tokens are recomputed.
- **Pool unreachable at start.** The engine starts without the cache and retries
  every 5 seconds. A pool whose pages are smaller than one KV object, or whose
  reservation lifetime is shorter than twice `maru_remote_timeout_s`, is rejected
  at start.
- **Outage.** After a failed or timed-out control request the worker stops
  calling the pool for `maru_remote_retry_s`: lookups miss, stores are skipped and
  requests compute normally. It then reconnects.
- **Pool restart.** Every request and reply carries the server's start-up
  generation; the server refuses requests addressed to an earlier run without
  executing them. On a change the worker reloads the pool's NIXL metadata,
  forgets which keys it stored and retries the call once. A worker that has not
  talked to the pool for 5 s confirms the run before trusting what it stored.
- **Keys of an earlier pool run.** They stay in regions the new run does not own
  and cannot expose over RDMA; the pool reports them missing and the next store
  of the same key replaces them. Their regions hold device capacity until then,
  so run `maru-server` for `maru-remote-server` alone and restart the two
  together; the server logs a warning when it finds regions it does not own.
- **Full pool.** Without eviction, a full pool answers reservations with
  `POOL_FULL`; the worker then skips stores (not loads) for `maru_remote_retry_s`.
- **Timed-out transfers.** Memory a timed-out RDMA transfer may still reach is
  not reused: its staging slots stay isolated until NIXL reports the transfer
  ended, and the pool keeps the pages of a timed-out WRITE out of circulation
  until the worker confirms the end (or, if the worker died, for
  `--quarantine-ttl`, 600 s by default).

## Trust model

The control channel is not authenticated and the pool's NIXL metadata exposes
its whole registered region, so any host that reaches the control port can
write anywhere in the pool. Run it only inside a trusted cluster network.

## Handler usage

```python
from maru import MaruConfig, MaruHandler

config = MaruConfig(
    storage_backend="remote",
    remote_url="tcp://pool-node:6600",
    remote_ucx_device="mlx5_0:1",
    cache_namespace="opaque-bytes-v1",
    pool_size=64 * 1024 * 1024,      # staging buffer
    chunk_size_bytes=4 * 1024 * 1024,
)
with MaruHandler(config) as handler:
    allocation = handler.alloc(4)
    allocation.buf[:] = b"data"
    assert handler.store("key", allocation)
    with handler.retrieve("key") as lease:
        assert bytes(lease.view) == b"data"
```

`pin`, `unpin` and `delete` are unsupported; reads use leases.

## Validation

```bash
PYTHONPATH="$PWD" python -m pytest -q tests/unit/test_remote_*.py \
  tests/unit/test_vllm_remote_config.py tests/unit/test_vllm_cpu_load_range.py
# On a worker node, against a running pool node:
PYTHONPATH="$PWD" python tools/remote_storage_g0.py --remote-url tcp://pool-node:6600 \
  --ucx-device mlx5_0:1 --objects 64 --object-bytes 4M --rounds 3
```
