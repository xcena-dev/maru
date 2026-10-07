# vLLM with a remote CXL pool

The remote backend keeps KV in a CXL pool on **another node** and moves it over
RDMA. Workers do not open a DAX device; one pool node serves every worker that
can reach it, so engines on different servers reuse each other's prefixes.

```
worker node                                 pool node
vLLM + MaruKVConnector                      resource manager
  MaruHandler(storage_backend="remote")     maru-server (remote endpoint)
    staging buffer  <==== RDMA (NIXL) ====>   CXL regions (registered with NIXL)
    control client  ----- ZMQ ---------->     reserve / publish / lookup / release
```

The worker-side backend implements the same handler contract as the CPU
backend: `alloc` returns a writable staging slot, `batch_store` publishes it,
and `batch_retrieve` returns read leases that the connector releases after its
copy. Unlike CPU mode, the staging buffer is page-locked for CUDA, so the
connector uses its default transfer path: the `maru_kv_ops` copy kernels (when
built) and asynchronous loads and stores. The remaining limits are CPU mode's:
`TP=PP=DP=1`, `--enforce-eager`, unquantized KV, one KV cache group, chunkwise
storage (no layerwise storage or layerwise overlap) and
`kv_load_failure_policy="recompute"`.

Enable `maru_async_load` and `maru_async_store` (recommended). A hit request
then waits while a loader thread reads its chunks in batches the staging buffer
can hold, and stores run after the forward on a completion thread, so a request
that misses the pool costs no more than recomputing it. vLLM's async scheduling
(its default) can stay on. With synchronous loads the connector requires
`--no-async-scheduling`: a failed synchronous load is reported after the
forward, when async scheduling has already scheduled the request's next step.

## Requirements

- An RDMA NIC on each node (RoCE or InfiniBand) and [NIXL](https://github.com/ai-dynamo/nixl)
  with its UCX backend: `pip install 'maru[remote]'`.
- On the pool node: a CXL/DAX device, the resource manager and MaruServer, as in
  a local CXL deployment. MaruServer serves the remote workers itself.
- Every worker must reach the pool node's remote endpoint and RDMA address.
- maru built with its KV placement kernels (`./install.sh`), as for the other
  vLLM examples, and vLLM 0.16 or later on each worker node (the launcher
  enables `maru_async_load`, which needs it).

## Quick start (one host)

`remote_example.sh` runs the pool node and two workers on one host. They talk
through NIXL as they would across nodes (on one host UCX may pick a
shared-memory transport instead of the NIC), so the example needs 2 GPUs, an
RDMA NIC and a CXL/DAX device on that host, and keeps the remote endpoint on
loopback.

```bash
sudo systemctl start maru-resource-manager    # installed by ./install.sh
MARU_DAX_PATH=/dev/dax0.0 MARU_POOL_UCX_DEVICE=mlx5_0:1 \
MARU_WORKER_UCX_DEVICE=mlx5_0:1 ./remote_example.sh [model]
```

This will:
1. Start `maru-server` with its remote endpoint (`remote_pool_server.sh`)
2. Launch two vLLM engines with `storage_backend: "remote"` (`remote_vllm_launcher.sh`)
3. Send a new prompt to instance 1, which computes it and stores its KV in the
   pool, then the same prompt to instance 2, which loads the KV from the pool
4. Clean up all processes

Expected output: instance 2 reports most of the prompt as cached tokens and
`Remote hit: Yes`. Instance 2 never saw the prompt, so those tokens were found
in the remote pool. vLLM counts them when it schedules the request; if the
RDMA read then fails, the connector logs `Maru load failed for req ...;
recomputing` after a line with the cause (such as `Maru batch_retrieve
failed`) and vLLM recomputes the tokens, so the script also checks `inst2.log`
for these lines. A load that fails from its first chunk can also make vLLM
report 0 cached tokens.
For a model as small as Qwen2.5-0.5B the TTFT gain is small, because its
prefill is already fast. Larger models and longer prompts gain more; for
Llama-3.1-8B set `MARU_REMOTE_PAGE_BYTES=32M GPU_MEM_UTIL=0.5`.

## Two nodes (step by step)

### 1. Pool node

```bash
sudo systemctl start maru-resource-manager    # installed by ./install.sh
MARU_DAX_PATH=/dev/dax0.0 MARU_POOL_UCX_DEVICE=mlx5_1:1 ./remote_pool_server.sh
```

The script prints the remote endpoint port that the workers need. The
example's default ports derive from the user ID, which can differ between
nodes, so give the workers the pool's endpoint explicitly. The script runs
(replace the DAX path and NIC):

```bash
maru-server --host 127.0.0.1 --port $MARU_SERVER_PORT --dax-path /dev/dax0.0 \
  --remote-bind tcp://0.0.0.0:$MARU_REMOTE_PORT --remote-pool-size 8G \
  --remote-page-bytes 4M --remote-capacity 8G --remote-ucx-device mlx5_1:1
```

with the ports from `env.sh` (`10000 + uid` and `11000 + uid` by default).

`--remote-bind` turns remote access on: MaruServer maps remote regions,
registers them with NIXL and answers remote workers on that endpoint from its
own thread, while local clients keep using `--host`/`--port`. Remote keys go
into the same ledger as local ones, so the two restart together.
`--remote-page-bytes` must hold one KV object: all layers of one
`maru_kv_chunk_tokens` chunk, `2 x layers x kv_heads x head_dim x dtype_bytes x
chunk_tokens` (3 MiB for Qwen2.5-0.5B and 32 MiB for Llama-3.1-8B with
256-token chunks). The remote pool grows past `--remote-pool-size` region by
region, up to `--remote-capacity` (or until the device is full). Mapping and
registering a region takes about 21 ms per GiB (1.35 s for 64 GiB, 2.7 s for
128 GiB), and the remote endpoint serves nothing else meanwhile. Workers'
requests wait for it, so an engine's scheduler can pause until the region is
added. If that takes longer than about 2 s, the waiting requests time out and
workers stop calling the pool; they reconnect at the first answered ping,
within about a second after the region is added. Meanwhile lookups miss,
stores are skipped and requests compute normally. The example sets the
capacity equal to the pool size, so the whole pool is registered at start-up. A reservation that would exceed the
capacity evicts the least recently read remote keys; keys being read are
pinned and never evicted. `--remote-eviction none` instead refuses new stores
when full. Workers skip storing keys they stored or read in the current server
run. Every reply carries the server's eviction count; when it changes, a
worker's load, store or maintenance thread asks which keys were evicted and
forgets only those, so evicted prefixes are stored again. The maintenance
thread pings the server once a second without blocking the engine or the
scheduler; it stops calls when the server stops answering and reconnects once
it answers. A store also asks which keys are present first and writes only the
missing ones.

### 2. Worker nodes

On each worker node (any number of them), point the engine at the pool node:

```bash
export MARU_REMOTE_URL=tcp://<pool node address>:<remote port> MARU_WORKER_UCX_DEVICE=mlx5_0:1
./remote_vllm_launcher.sh inst1 [model]                    # on node A, GPU 0
MARU_INST2_GPU=0 ./remote_vllm_launcher.sh inst2 [model]   # on node B, GPU 0
```

which runs (replace the model, namespace, address and NIC):

```bash
vllm serve <model> --enforce-eager --enable-prompt-tokens-details \
  --kv-transfer-config '{
    "kv_connector": "MaruKVConnector",
    "kv_connector_module_path": "maru_vllm",
    "kv_role": "kv_both",
    "kv_load_failure_policy": "recompute",
    "kv_connector_extra_config": {
      "maru_storage_backend": "remote",
      "maru_remote_url": "tcp://pool-node:<remote port>",
      "maru_remote_ucx_device": "mlx5_0:1",
      "maru_cache_namespace": "my-model-immutable-weights-revision",
      "maru_kv_chunk_tokens": 256,
      "maru_async_load": true,
      "maru_async_store": true
    }
  }'
```

`maru_cache_namespace` is the sharing scope. The connector combines it with the
model configuration, dtype, block size and chunk size, so engines share KV only
when all of these match; where a node keeps the weights and its transformers
version do not count. Use a namespace that identifies the exact weights.
`maru_engine_id` is not needed. `--enable-prompt-tokens-details`
only makes vLLM report cached tokens per request, which the test scripts read.

### 3. Test

From any host that reaches both engines (`run_benchmark.py` needs the
`openai` package, which vLLM installs):

```bash
export MARU_INST1_URL=http://<worker 1>:<port> MARU_INST2_URL=http://<worker 2>:<port>
./run_simple_query.sh            # one prompt on each engine, with cached tokens
./run_benchmark.sh               # TTFT and tokens found in the pool
```

Each engine's port is printed by its launcher (`vLLM Port`). Then check
instance 2's log: no `Maru load failed for req` line means its loads from the
pool succeeded.

## Configuration

All example settings are in `env.sh`. Override them with environment variables:

| Variable | Default | Description |
|---|---|---|
| `MARU_REMOTE_URL` | `tcp://$MARU_POOL_HOST:$MARU_REMOTE_PORT` | Pool's remote endpoint as workers see it; set it on worker nodes |
| `MARU_POOL_HOST` | `127.0.0.1` | Address workers use to reach the pool node |
| `MARU_REMOTE_PORT` | `11000 + uid` | Remote endpoint port on the pool node |
| `MARU_REMOTE_BIND_HOST` | `0.0.0.0` (`127.0.0.1` in `remote_example.sh`) | Interface the remote endpoint listens on |
| `MARU_SERVER_PORT` | `10000 + uid` | MaruServer's local RPC port on the pool node |
| `MARU_DAX_PATH` | any pool | DAX device that backs the remote pool |
| `MARU_POOL_UCX_DEVICE` | UCX default | RDMA NIC on the pool node, e.g. `mlx5_1:1` |
| `MARU_REMOTE_POOL_SIZE` | `8G` | Size of each remote region |
| `MARU_REMOTE_CAPACITY` | pool size | Most bytes the remote pool holds |
| `MARU_REMOTE_PAGE_BYTES` | `4M` | Page size; must hold one KV object |
| `MARU_WORKER_UCX_DEVICE` | UCX default | RDMA NIC on each worker, e.g. `mlx5_0:1` |
| `MARU_CACHE_NAMESPACE` | `maru-remote-example` | Sharing scope of the engines |
| `MARU_KV_CHUNK_TOKENS` | `256` | Tokens per KV chunk |
| `MARU_INST1_GPU`, `MARU_INST2_GPU` | `0`, `1` | GPU of each engine |
| `MARU_INST1_PORT`, `MARU_INST2_PORT` | `12000 + uid + 10/11` | vLLM ports |
| `MARU_INST1_URL`, `MARU_INST2_URL` | `http://localhost:<port>` | Where the test scripts reach each engine |
| `MAX_MODEL_LEN` | `8192` | vLLM `--max-model-len`; also sizes the staging buffer |
| `GPU_MEM_UTIL` | `0.1` | vLLM GPU memory utilization |

## Files

| File | Description |
|---|---|
| `env.sh` | Example settings (ports, pool, NICs, namespace) |
| `remote_pool_server.sh` | Pool node: `maru-server` with its remote endpoint |
| `remote_vllm_launcher.sh` | Worker: one vLLM engine (inst1/inst2) on the remote pool |
| `remote_example.sh` | One-host run of all of the above plus the test |
| `run_simple_query.sh` | One prompt on each engine, with its cached tokens |
| `run_benchmark.sh`, `run_benchmark.py` | TTFT and tokens found in the pool |

The `*_UCX_DEVICE` settings are passed to NIXL. If UCX still uses other NICs of
a node, also export `UCX_NET_DEVICES=<device>` (for example `mlx5_0:1`) before
starting that node's process.

## Connector settings

| Setting | Default | Meaning |
|---|---|---|
| `maru_remote_url` | required | Remote endpoint of the pool node's `maru-server` (`--remote-bind`) |
| `maru_remote_ucx_device` | UCX default | Local RDMA NIC for NIXL, e.g. `mlx5_0:1` |
| `maru_remote_staging_size` | one `max_model_len` prompt divided by `1 - maru_remote_load_reserve`, at least 64 KV objects and `1G` | Local staging buffer (RDMA source and target), page-locked for CUDA |
| `maru_remote_load_reserve` | `0.5` | Share of the staging buffer that stores leave free for loads |
| `maru_async_load`, `maru_async_store` | `false` | Load and store on background threads (recommended) |
| `maru_remote_timeout_s` | `30` | Deadline of one RDMA batch |
| `maru_remote_retry_s` | `30` | How long the worker stops calling the pool after a failure |

`maru_pool_size`, `maru_cpu_pool_size` and the mixed-mode settings do not apply.

## Behaviour

- **Store.** A prompt's completed chunks are copied GPU → staging (with
  `maru_async_store`, after the forward), then one reserve, one RDMA WRITE batch
  and one publish. A key that already exists keeps its first value; the store
  still counts as present. Stores use only the staging slots outside the load
  reserve, so a slow pool makes stores, not loads, give way. An async store
  stages a whole prompt, so if that share is smaller than the longest prompt
  the excess chunks are skipped.
- **Load.** The scheduler asks the pool which prefix chunks exist. The worker
  pins them, RDMA READs them into staging, unpins them, copies staging → GPU and
  releases the leases, in batches the staging buffer can hold. A missing chunk
  or any failure is reported to vLLM as a load error and the tokens are
  recomputed. A request whose async load failed is recomputed without asking
  the pool again.
- **Pool unreachable at start.** The engine starts without the cache and retries
  every `maru_remote_retry_s`. A pool whose pages are smaller than one KV object,
  or whose reservation or read-protection lifetime is shorter than twice
  `maru_remote_timeout_s`, is rejected at start.
- **Outage.** After a failed or timed-out control request the worker stops
  calling the pool for `maru_remote_retry_s`: lookups miss, stores are skipped and
  requests compute normally. It then reconnects.
- **Pool restart.** Restarting `maru-server` starts a new run: the ledger and
  the remote state start empty together. Every reply, and every request except
  the connection handshake and the maintenance thread's ping, carries the run's
  start-up generation;
  the server refuses requests addressed to an earlier run without executing
  them. On a change the worker reloads the pool's NIXL metadata, forgets which
  keys it stored and retries the call once. The maintenance thread's ping
  every second also reports the run, so an idle worker notices a restart
  within about a second.
- **Local clients on the pool node.** Keys that local Maru clients keep in
  their own regions are reported missing to remote workers, and a remote store
  of the same key is refused. Local clients cannot delete remote keys or return
  remote regions; remote regions are hidden from their region list. A local
  client that looks up a remote key maps that whole region on first access.
- **Full pool.** The pool evicts least recently read keys (see above). When
  nothing can be evicted (every key pinned, or `--remote-eviction none`) it answers
  reservations with `POOL_FULL`; the worker then skips stores (not loads) for
  `maru_remote_retry_s`.
- **Refused keys.** A key the pool refuses to publish (for example a key a
  local client on the pool node already holds) is not written again for
  `maru_remote_retry_s`.
- **Timed-out transfers.** Memory a timed-out RDMA transfer may still reach is
  not reused: its staging slots stay isolated until NIXL reports the transfer
  ended, and the pool keeps the pages of a timed-out WRITE out of circulation
  until the worker confirms the end (or, if the worker died, for
  `--remote-quarantine-ttl`, 600 s by default). If the pool restarts while such a
  transfer is still pending, the worker drops the old pool's NIXL peer with
  the isolated transfer outstanding; that ordering has not been exercised on
  hardware.

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
    pool_size=64 * 1024 * 1024,  # staging buffer
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
PYTHONPATH="$PWD" python tools/remote_storage_roundtrip.py --remote-url tcp://pool-node:6600 \
  --ucx-device mlx5_0:1 --objects 64 --object-bytes 4M --rounds 3
```
