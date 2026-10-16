# vLLM Examples

Three runnable examples of the Maru-vLLM direct connector (`MaruKVConnector`),
which moves KV cache through CXL shared memory without LMCache in the serving
path. All live under `examples/vllm/`:

| Example | Directory | What it shows |
|---------|-----------|---------------|
| Single-instance verification | `examples/vllm/single/` | One instance stores KV and reuses it on a repeat request — the cheapest connector check |
| P2P KV cache sharing | `examples/vllm/p2p_sharing/` | Two instances share KV cache, so the second skips prefill |
| Remote KV pool | `examples/vllm/remote/` | Instances on other nodes share KV through one node's CXL pool over RDMA |

Start with the single-instance example: it needs one GPU and isolates the
store and load paths before any cross-instance behavior is involved.

## Prerequisites

- 1 GPU for the single-instance example, 2+ for P2P
- Maru installed with its KV placement kernels: `./install.sh` from the Maru
  source tree, or `uv pip install -e /path/to/maru --no-build-isolation` in an
  environment that already has PyTorch and the CUDA toolkit. The connector
  carries its own CUDA kernels for the coalesced multi-layer transfers and
  falls back to a transfer per layer without them, which is materially slower.
  `python -c "import maru_kv_ops; print(maru_kv_ops.is_available())"` says
  which path it will take.
- vLLM v0.14+ installed
- `maru-server` binary available
- For the remote KV pool: vLLM 0.16 or later (the launcher enables
  `maru_async_load`), an RDMA NIC (RoCE or InfiniBand) on every node,
  [NIXL](https://github.com/ai-dynamo/nixl) with its UCX backend
  (`pip install 'maru[remote]'`), and on the pool node a CXL/DAX device with
  the Maru resource manager running

## Single-instance verification

One vLLM instance stores KV to Maru on the first request and loads it back on
a repeat. vLLM's own prefix cache is disabled in the launcher so that Maru is
the only cache source — otherwise the repeated prompt would be served from the
GPU-resident prefix cache and neither connector path would run.

```
            vLLM (GPU 0, prefix cache OFF)
                      |
                MaruKVConnector
                      |
                 MaruHandler ── CXL Shared Memory
                      |
                 MaruServer (metadata)

1st request → cold: full prefill, KV stored to Maru
2nd request → warm: prefix found in Maru, KV loaded, prefill skipped
```

### Automated

```bash
cd examples/vllm/single
./single_example.sh                              # Default: Qwen/Qwen2.5-0.5B
./single_example.sh --model meta-llama/Llama-3-8B
```

The script starts `maru-server` and one vLLM instance, then sends the same
prompt twice. `Cache Hit: Yes` with a TTFT speedup confirms both paths. It
exits non-zero on a cache miss; check `single.log` for connector errors (a
missing `Maru: loaded N layers` message means the worker load path failed).

### Step-by-step

```bash
cd examples/vllm/single

# 1. Start maru-server
source env.sh
maru-server --port $MARU_SERVER_PORT

# 2. Launch the instance (GPU 0, prefix cache OFF)
./single_vllm_launcher.sh Qwen/Qwen2.5-0.5B

# 3. Verify
./run_simple_query.sh                            # same prompt twice
./run_benchmark.sh --model Qwen/Qwen2.5-0.5B     # TTFT: cold vs warm
```

### Configuration

Settings live in `examples/vllm/single/env.sh` and can be overridden through
the environment:

| Variable | Default | Description |
|----------|---------|-------------|
| `MARU_SERVER_PORT` | `10000 + uid` | MaruServer port |
| `MARU_INST_PORT` | `12000 + uid + 20` | vLLM instance port |
| `MARU_POOL_SIZE` | `4G` | CXL shared memory pool size |
| `MARU_KV_CHUNK_TOKENS` | `256` | Tokens per KV cache chunk |
| `GPU_MEM_UTIL` | `0.1` | vLLM GPU memory utilization |

## P2P KV cache sharing

Two vLLM instances share KV cache through CXL shared memory. The first stores
the prefix, the second loads it and skips prefill.

```
Instance 1 (GPU 0)                    Instance 2 (GPU 1)
     vLLM                                  vLLM
       |                                     |
  MaruKVConnector                      MaruKVConnector
       |                                     |
       +----------- MaruHandler -------------+
                        |
                   CXL Shared Memory
                        |
                   MaruServer (metadata)
```

### Automated

```bash
cd examples/vllm/p2p_sharing
./p2p_example.sh                                 # Default: Qwen/Qwen2.5-0.5B
./p2p_example.sh meta-llama/Llama-3-8B
```

The script starts `maru-server`, launches both instances, runs the sharing
test, and cleans up every process it started.

### Step-by-step

```bash
cd examples/vllm/p2p_sharing

# 1. Start maru-server
source env.sh
maru-server --port $MARU_SERVER_PORT

# 2. Launch the instances (separate terminals)
./p2p_vllm_launcher.sh inst1 Qwen/Qwen2.5-0.5B   # GPU 0
./p2p_vllm_launcher.sh inst2 Qwen/Qwen2.5-0.5B   # GPU 1

# 3. Run the test
./run_simple_query.sh                            # prompt + output verification
./run_benchmark.sh --model Qwen/Qwen2.5-0.5B     # TTFT measurement
```

Instance 2 should report a lower TTFT and `Cache Hit: Yes`, because it loads
the KV cache from CXL instead of recomputing prefill.

### Configuration

Settings live in `examples/vllm/p2p_sharing/env.sh`:

| Variable | Default | Description |
|----------|---------|-------------|
| `MARU_SERVER_PORT` | `10000 + uid` | MaruServer port |
| `MARU_INST1_PORT` | `12000 + uid + 10` | vLLM instance 1 port |
| `MARU_INST2_PORT` | `12000 + uid + 11` | vLLM instance 2 port |
| `MARU_POOL_SIZE` | `4G` | CXL shared memory pool size |
| `MARU_KV_CHUNK_TOKENS` | `256` | Tokens per KV cache chunk |
| `GPU_MEM_UTIL` | `0.1` | vLLM GPU memory utilization |

The two examples use different instance ports, so they do not collide when run
on the same machine.

### Troubleshooting

**Instance 2 TTFT is not faster.** Check the Instance 2 log for
`Maru: loaded N layers`. If it is absent, confirm `maru-server` is running and
that both instances connect to it. For very small models with short prompts the
CXL retrieve overhead can exceed the prefill it saves.

**Garbage output on Instance 2.** KV cache corruption — confirm the load is
using per-chunk injection rather than a concatenated 1D load.

## Remote KV pool

vLLM instances on different servers share KV cache through one CXL pool. Only
the pool node has the CXL device. Its `maru-server` opens a remote endpoint
(`--remote-bind`), maps part of the pool and registers it with the node's RDMA
NIC. A worker moves KV bytes over RDMA (NIXL) directly between its staging
buffer and the pool, and sends only small control requests (reserve, publish,
lookup) to the remote endpoint. On the worker it is the same connector with
`maru_storage_backend: "remote"`; the worker opens no DAX device.

```
Worker node A                    Pool node                     Worker node B
vLLM + MaruKVConnector           resource manager              vLLM + MaruKVConnector
  MaruHandler (remote)           maru-server                     MaruHandler (remote)
     |  control (ZMQ) -------->    remote endpoint  <-------- control (ZMQ)  |
  staging buffer <=== RDMA ===>    CXL pool         <=== RDMA ===> staging buffer

Instance 1 (node A) → computes the prompt, stores its KV in the pool
Instance 2 (node B) → finds the prefix in the pool, loads it over RDMA, skips its prefill
```

### Automated (one host)

`remote_example.sh` runs the pool node and both instances on one host. The
instances reach the pool through NIXL as instances on other nodes would (on one
host UCX may pick a shared-memory transport instead of the NIC), so the host
needs 2 GPUs, an RDMA NIC and a CXL/DAX device. The script keeps the remote
endpoint on loopback.

```bash
cd examples/vllm/remote
sudo systemctl start maru-resource-manager       # installed by ./install.sh
MARU_DAX_PATH=/dev/dax0.0 MARU_POOL_UCX_DEVICE=mlx5_0:1 \
MARU_WORKER_UCX_DEVICE=mlx5_0:1 ./remote_example.sh   # Default: Qwen/Qwen2.5-0.5B
```

The script starts `maru-server` with its remote endpoint, launches both
instances, and sends a new prompt first to instance 1 and then to instance 2.
Instance 2 has never seen the prompt, so every cached token it reports was
found in the remote pool. The script prints how many there were and
`Remote hit: Yes` when at least one chunk was found. vLLM counts these tokens
when it schedules the request; if the RDMA read then fails, the connector logs
`Maru load failed for req ...; recomputing` after a line with the cause (such
as `Maru batch_retrieve failed`) and vLLM recomputes the tokens, so the script
also checks instance 2's log for these lines. A load that fails from its first
chunk can also make vLLM report 0 cached tokens. It exits non-zero on a miss or a failed
load, and stops every process it started. For example, with Qwen2.5-0.5B on
two H200 GPUs of one host:

```
  Instance 1 (store): TTFT = 164.0 ms
  Instance 2 (load):  TTFT = 94.1 ms, 1792 of 1838 prompt tokens found in the pool
  TTFT ratio:         1.74x
  Remote hit:         Yes
```

The TTFT gain is small for a model this small, because its prefill is already
fast. Larger models and longer prompts gain more; for Llama-3.1-8B set
`MARU_REMOTE_PAGE_BYTES=32M GPU_MEM_UTIL=0.5` (see Configuration).

### Step-by-step (two nodes)

```bash
cd examples/vllm/remote

# 1. Pool node: resource manager, then maru-server with the remote endpoint
sudo systemctl start maru-resource-manager       # installed by ./install.sh
MARU_DAX_PATH=/dev/dax0.0 MARU_POOL_UCX_DEVICE=mlx5_1:1 ./remote_pool_server.sh
#    prints: On each worker node: export MARU_REMOTE_URL=tcp://<this node's address>:<port>

# 2. Each worker node: point the instance at the pool's remote endpoint
export MARU_REMOTE_URL=tcp://<pool node>:<port> MARU_WORKER_UCX_DEVICE=mlx5_0:1
./remote_vllm_launcher.sh inst1 Qwen/Qwen2.5-0.5B                    # on node A, GPU 0
MARU_INST2_GPU=0 ./remote_vllm_launcher.sh inst2 Qwen/Qwen2.5-0.5B   # on node B, GPU 0

# 3. From any host that reaches both instances
export MARU_INST1_URL=http://<node A>:<port> MARU_INST2_URL=http://<node B>:<port>
./run_simple_query.sh                            # one prompt on each, with cached tokens
./run_benchmark.sh --model Qwen/Qwen2.5-0.5B     # TTFT and tokens found in the pool
```

The example's default ports derive from the user ID, which can differ between
nodes. Give the workers the pool's endpoint with `MARU_REMOTE_URL`, as printed
by `remote_pool_server.sh`, and take each instance's port from its launcher
output (`vLLM Port`). Across nodes the two instances may run on different
GPUs, so the TTFT ratio compares different hardware; the cached tokens of
instance 2 show the remote hit, and a log of instance 2 without
`Maru load failed for req` shows that the loads succeeded. `run_benchmark.py`
needs the `openai` package, which vLLM installs.

The launcher starts vLLM with the settings the remote backend needs:
`--enforce-eager`, `kv_load_failure_policy: "recompute"`, and asynchronous
loads and stores (`maru_async_load`, `maru_async_store`), which keep the engine
stepping while KV moves over RDMA. `--enable-prompt-tokens-details` only makes
vLLM report cached tokens per request, which the test scripts read.

### Configuration

Settings live in `examples/vllm/remote/env.sh`:

| Variable | Default | Description |
|----------|---------|-------------|
| `MARU_REMOTE_URL` | `tcp://$MARU_POOL_HOST:$MARU_REMOTE_PORT` | Pool's remote endpoint as the workers see it; set it on worker nodes |
| `MARU_POOL_HOST` | `127.0.0.1` | Address workers use to reach the pool node |
| `MARU_REMOTE_PORT` | `11000 + uid` | Remote endpoint port on the pool node |
| `MARU_REMOTE_BIND_HOST` | `0.0.0.0` (`127.0.0.1` in `remote_example.sh`) | Interface the remote endpoint listens on |
| `MARU_SERVER_PORT` | `10000 + uid` | MaruServer's local RPC port on the pool node |
| `MARU_DAX_PATH` | any pool | DAX device that backs the remote pool |
| `MARU_POOL_UCX_DEVICE` | UCX default | RDMA NIC of the pool node, e.g. `mlx5_1:1` |
| `MARU_REMOTE_POOL_SIZE` | `8G` | Size of each remote region |
| `MARU_REMOTE_CAPACITY` | pool size | Most bytes the remote pool holds |
| `MARU_REMOTE_PAGE_BYTES` | `4M` | Remote page size; one page holds one KV object |
| `MARU_WORKER_UCX_DEVICE` | UCX default | RDMA NIC of each worker, e.g. `mlx5_0:1` |
| `MARU_CACHE_NAMESPACE` | `maru-remote-example` | Sharing scope; instances share KV only when it matches |
| `MARU_KV_CHUNK_TOKENS` | `256` | Tokens per KV cache chunk |
| `MARU_INST1_GPU`, `MARU_INST2_GPU` | `0`, `1` | GPU of each instance |
| `MARU_INST1_PORT`, `MARU_INST2_PORT` | `12000 + uid + 10/11` | vLLM instance ports |
| `MARU_INST1_URL`, `MARU_INST2_URL` | `http://localhost:<port>` | Where the test scripts reach each instance |
| `MAX_MODEL_LEN` | `8192` | vLLM `--max-model-len`; also sizes each worker's staging buffer |
| `GPU_MEM_UTIL` | `0.1` | vLLM GPU memory utilization |

One page holds one KV object, all layers of one chunk:
`2 × layers × kv_heads × head_dim × dtype bytes × chunk tokens`. With 256-token
chunks that is 3 MiB for Qwen2.5-0.5B (the default `4M` fits) and 32 MiB for
Llama-3.1-8B (`MARU_REMOTE_PAGE_BYTES=32M`).

The `*_UCX_DEVICE` settings are passed to NIXL. If UCX still uses other NICs of
a node, also export `UCX_NET_DEVICES=<device>` before starting that node's
process.

The example sets the pool's capacity equal to one region, so `maru-server`
maps and registers the whole pool at start-up. When a pool with a larger
capacity grows while serving, registering the new region takes about 21 ms per
GiB, and the remote endpoint answers nothing else meanwhile. Workers' requests
wait for it, so an engine's scheduler can pause until the region is added. If
that takes longer than about 2 s, the waiting requests time out and workers
stop calling the pool; they reconnect at the first answered ping, within about
a second after the region is added. Meanwhile lookups miss, stores are skipped
and requests compute normally.

### Troubleshooting

**`remote server tcp://... is unreachable: remote hello timed out` in an
instance log.** The instance cannot reach the pool's remote endpoint. Check
`MARU_REMOTE_URL`: the port comes from `remote_pool_server.sh` on the pool
node, not from the worker's own user ID. The instance keeps serving without
the pool and retries every 30 s.

**`Maru load failed for req ...; recomputing` in instance 2's log.** Instance 2
found the prefix in the pool but could not load it, so vLLM recomputed it. The
line before it gives the cause: `Maru batch_retrieve failed` with an RDMA
error, `Maru load miss for req` for a key evicted in between, or
`Maru: no read buffer free`. Check that
both nodes' NICs reach each other over RDMA (`MARU_POOL_UCX_DEVICE`,
`MARU_WORKER_UCX_DEVICE`, and `UCX_NET_DEVICES` if set).

**`Remote hit: No` with no cached tokens on instance 2.** First look for
`Maru load failed for req` in instance 2's log: a load that fails from its
first chunk also resets the cached-token count (see the entry above).
Otherwise check that both instance logs show `remote storage connected to tcp://...`, that both use the
same `MARU_CACHE_NAMESPACE` and model, and that instance 1's store had time to
reach the pool before instance 2's request (`--wait-time`, 3 s by default;
stores finish after the response).

**`remote pool pages (...) are smaller than one KV object`.** Raise
`MARU_REMOTE_PAGE_BYTES` on the pool node to at least the KV object size above
and restart `remote_pool_server.sh`.

**Instance fails to start on an RTX PRO 6000 (Blackwell) GPU with a FlashInfer
sampler error.** `env.sh` sets `VLLM_USE_FLASHINFER_SAMPLER=0` on such GPUs; if
you set the variable yourself, set it to `0`.

## Further reading

Each example directory has a README with its full file listing. The remote
example's README also describes how the pool behaves when it is full,
unreachable or restarted, and its trust model (the control channel is not
authenticated; run it only inside a trusted cluster network). For connector
configuration — including the asynchronous load and store settings and the
remote backend's settings — see [vLLM](../../../integration/vllm.md).
