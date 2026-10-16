# vLLM with CXL on memory nodes over RDMA

The `rdma` storage backend lets vLLM engines on nodes without CXL keep their
KV cache in the CXL memory of other nodes, the memory nodes, and move it over
RDMA. Resources flow as for local CXL: the resource manager gives regions to
maru-server, and maru-server gives them to each worker's MaruHandler. What
moves to the memory node is the mapping: a region mapper on every memory node
maps the regions Handlers ask for and registers them with that node's NIC. The
worker then reads and writes them with one-sided RDMA; the bytes do not pass
through any Maru process.

```
worker node (any number)          metadata node              memory node (each node of the pool)
vLLM + MaruKVConnector            maru-server                region mapper
  MaruHandler(storage_backend       storage sessions,          maps a region, registers it
             ="rdma")               regions, keys    <--alloc-- with the NIC   resource manager
    session RPC  ---------------->                              ^       (one per pool)
    map / release  ---------------------------------------------+
    staging  <======= one-sided RDMA (NIXL) =======>  NIC ==> CXL memory
```

A worker opens a storage session with maru-server and gets one region of
`maru_pool_size`; when its regions are full it gets one more, as `auto_expand`
does for the cxl backend. Other engines read its keys under a server lease,
mapping the region through the same region mapper. Before a worker gives a
region back, its region mapper deregisters the region from the NIC, so a WRITE
still in flight from the old owner fails at the NIC instead of landing in the
next owner's KV.

The backend stores KV chunkwise and needs `TP=PP=DP=1`, `--enforce-eager`,
unquantized KV and `kv_load_failure_policy="recompute"`. Loads and stores
run on the engine thread by default; `maru_async_load` loads cache hits
between steps and `maru_async_store` stores after the forward
(`MARU_ASYNC=1` on the launcher sets both). Synchronous loads need
`--no-async-scheduling`, which the launcher always passes. The KV moves
between the GPU and the worker's staging buffer, host memory registered with
the NIC and with CUDA; with `maru_kv_ops` built, one copy kernel per chunk
moves it, as for the cxl backend.

## Requirements

- An RDMA NIC (RoCE or InfiniBand) on every worker and memory node, and NIXL
  with its UCX backend: `pip install 'maru[rdma]'`.
- On the memory nodes: the CXL/DAX device of the pool, visible on every memory
  node of the pool, and the resource manager on one of them, as for a local CXL
  deployment. Use a devdax device. Install Maru with NIXL
  (`pip install 'maru[rdma]'`, then `./install.sh`): the installer also sets up
  the region mapper as a system service that starts and stops with the
  resource manager, so a memory node runs it without a separate step.
- Every worker must reach maru-server and each memory node's region mapper and
  NIC.
- The region mapper runs UCX on the RDMA transports only (`UCX_TLS=rc,dc`) with
  its registration cache off (`UCX_RCACHE_ENABLE=n`). It sets both itself and
  refuses to start if they are set otherwise: with the TCP transport or the
  registration cache, a deregistered region can still be written.

## Quick start (one host)

`rdma_example.sh` runs maru-server and two workers on one host, and uses the
region mapper service when it runs there (otherwise it starts a region mapper
itself). The workers still go through the region mapper's NIC
registration and one-sided RDMA, so the example needs 2 GPUs, an RDMA NIC and
a CXL/DAX device on that host.

```bash
sudo systemctl start maru-resource-manager    # installed by ./install.sh
MARU_DAX_PATH=/dev/dax0.0 MARU_MAPPER_UCX_DEVICE=mlx5_0:1 \
MARU_WORKER_UCX_DEVICE=mlx5_0:1 ./rdma_example.sh [model]
```

This will:
1. Use the region mapper service, or start a region mapper (`region_mapper.sh`)
2. Start maru-server with that region mapper (`maru_server.sh`)
3. Launch two vLLM engines with `storage_backend: "rdma"` (`rdma_vllm_launcher.sh`)
4. Send a new prompt to instance 1, which computes it and stores its KV in the
   pool, then the same prompt to instance 2, which loads the KV from the pool
5. Clean up all processes

Expected output: instance 2 reports most of the prompt as cached tokens and
`Pool hit: Yes`. Only whole chunks are stored, so the cached tokens stop at the
last full `maru_kv_chunk_tokens` chunk. `Same answer` compares instance 1's
answer, computed from the whole prompt, with instance 2's, which reuses the
stored KV and computes only the rest. vLLM itself can answer differently when it
recomputes part of a prompt instead of reusing it, so `Same answer: No` after
some tokens is not a load failure; the KV from the pool is checked against
vLLM's own prefix cache at the same reused length, which gives the same answer. Instance 2
never saw the prompt, so those tokens were found in the pool. vLLM counts them
when it schedules the request; if the RDMA read then fails, the connector logs
`Maru load failed for req ...; recomputing` after a line with the cause (such
as `Maru batch_retrieve failed`) and vLLM recomputes the tokens, so the script
also checks `inst2.log` for these lines.

For Llama-3.1-8B set `GPU_MEM_UTIL=0.5`. Set `MARU_NUMA_NODE` to bind each
engine to the socket of its GPU and NIC.

## Several nodes (step by step)

The example below uses one memory node that also runs maru-server, and two
workers on another node. Replace the addresses, NICs and DAX device.

### 1. Memory nodes

Each memory node of the pool runs a region mapper, a platform service like the
resource manager: one per memory node, shared by every app that uses the pool.
With Maru installed with NIXL, `./install.sh` installs it as
`maru-region-mapper.service`, which starts and stops with the resource
manager. Set the node's RDMA NIC in `/etc/maru/region-mapper.env`; the defaults
listen on all addresses at port 9860 and use the resource manager on the same
node:

```bash
sudo sed -i 's/^MARU_REGION_MAPPER_UCX_DEVICE=.*/MARU_REGION_MAPPER_UCX_DEVICE=mlx5_1:1/' \
  /etc/maru/region-mapper.env
sudo systemctl start maru-resource-manager    # also starts the region mapper
systemctl status maru-region-mapper
```

On a memory node that does not run the resource manager of the pool, set
`MARU_RM_ADDRESS` in that file to the node that does. On a host without the
service (a test host, or Maru installed without NIXL), `./region_mapper.sh`
runs a region mapper in the foreground instead:

```bash
MARU_MEMORY_NODE_HOST=10.0.0.2 MARU_MAPPER_UCX_DEVICE=mlx5_1:1 ./region_mapper.sh
```

### 2. Metadata node

Start maru-server with the region mappers of all memory nodes of the pool, on
an address the workers reach:

```bash
MARU_SERVER_BIND_HOST=10.0.0.2 MARU_DAX_PATH=/dev/dax1.0 \
MARU_REGION_MAPPERS=10.0.0.2:9860 ./maru_server.sh
```

which runs

```bash
maru-server --host 10.0.0.2 --port $MARU_SERVER_PORT --rm-address 127.0.0.1:9850 \
  --region-mapper 10.0.0.2:9860 --dax-path /dev/dax1.0
```

maru-server allocates each worker's regions from the resource manager, as for
local CXL, and hands the region mapper list to the workers. A shared pool lists
every memory node, `--region-mapper host1:port,host2:port`; each region is then
served by one of them, picked from the region number, so every worker reaches
a region through the same node. maru-server runs once per app and pool.

### 3. Worker nodes

On each worker node, point the engines at maru-server:

```bash
export MARU_SERVER_URL=tcp://10.0.0.2:<maru-server port> MARU_WORKER_UCX_DEVICE=mlx5_0:1
MARU_INST1_GPU=0 ./rdma_vllm_launcher.sh inst1 [model]
MARU_INST2_GPU=1 ./rdma_vllm_launcher.sh inst2 [model]
```

The example's default ports derive from the user ID, which can differ between
nodes, so give the workers the port `maru_server.sh` printed. The launcher
runs (replace the model, namespace, address and NIC):

```bash
vllm serve <model> --enforce-eager --no-async-scheduling --enable-prompt-tokens-details \
  --kv-transfer-config '{
    "kv_connector": "MaruKVConnector",
    "kv_connector_module_path": "maru_vllm",
    "kv_role": "kv_both",
    "kv_load_failure_policy": "recompute",
    "kv_connector_extra_config": {
      "maru_server_url": "tcp://10.0.0.2:<maru-server port>",
      "maru_storage_backend": "rdma",
      "maru_rdma_device": "mlx5_0:1",
      "maru_pool_size": "4G",
      "maru_engine_id": "<unique per engine>",
      "maru_cache_namespace": "my-model-immutable-weights-revision",
      "maru_kv_chunk_tokens": 256
    }
  }'
```

`maru_cache_namespace` is the sharing scope: engines share KV only when it and
the model shape match, so use a namespace that identifies the exact weights.
`maru_engine_id` names the engine's storage session and must be unique among
the engines of one maru-server; the launcher uses `<hostname>-<instance>`.
`maru_pool_size` is the region size. `--enable-prompt-tokens-details` only
makes vLLM report cached tokens per request, which the test scripts read.

### 4. Test

From any host that reaches both engines (`run_benchmark.py` needs the
`openai` package, which vLLM installs):

```bash
export MARU_INST1_URL=http://<worker 1>:<port> MARU_INST2_URL=http://<worker 2>:<port>
./run_simple_query.sh [model]    # one prompt on each engine, with cached tokens
./run_benchmark.sh --model <model>   # TTFT, cached tokens and same answer
```

Then check instance 2's log: no `Maru load failed for req` line means its loads
from the pool succeeded. `MARU_LOG_TIMING=1` on the launcher adds the
connector's per-request load and store timings to the engine logs.

## Configuration

All example settings are in `env.sh`. Override them with environment variables:

| Variable | Default | Description |
|---|---|---|
| `MARU_RM_ADDRESS` | `127.0.0.1:9850` | Resource manager of the pool |
| `MARU_MEMORY_NODE_HOST` | `127.0.0.1` | `region_mapper.sh`: memory node's address on the RDMA network; the region mapper listens there |
| `MARU_MAPPER_PORT` | `10500 + uid` | `region_mapper.sh`: region mapper port |
| `MARU_MAPPER_UCX_DEVICE` | UCX default | `region_mapper.sh`: RDMA NIC the region mapper registers regions with, e.g. `mlx5_1:1` |
| `MARU_MAPPER_LOCK_FILE` | `/tmp/maru-region-mapper.lock` | `region_mapper.sh`: keeps one region mapper per node |
| `MARU_SERVER_BIND_HOST` | `127.0.0.1` | Interface maru-server listens on |
| `MARU_SERVER_PORT` | `10000 + uid` | maru-server port |
| `MARU_DAX_PATH` | any pool | DAX device of the pool |
| `MARU_REGION_MAPPERS` | this node's region mapper (the service's, if it runs) | Region mappers of the pool, `host:port`, comma separated |
| `MARU_SERVER_URL` | `tcp://$MARU_SERVER_HOST:$MARU_SERVER_PORT` | maru-server as workers see it; set it on worker nodes |
| `MARU_POOL_SIZE` | `4G` | Region size; a worker gets one more region each time its regions are full |
| `MARU_WORKER_UCX_DEVICE` | UCX default | RDMA NIC on each worker, e.g. `mlx5_0:1` |
| `MARU_CACHE_NAMESPACE` | `maru-rdma-example` | Sharing scope of the engines |
| `MARU_KV_CHUNK_TOKENS` | `256` | Tokens per KV chunk |
| `MARU_INST1_GPU` / `MARU_INST2_GPU` | `0` / `1` | GPU of each engine |
| `MARU_NUMA_NODE` | none | NUMA node to bind each engine to |
| `MARU_LOG_TIMING` | off | Log the connector's load and store timings |
| `MARU_ASYNC` | off | Load cache hits between steps and store after the forward |
| `GPU_MEM_UTIL` | `0.1` | vLLM GPU memory utilization |

## Troubleshooting

- **The region mapper refuses to start with `UCX_TLS` or `UCX_RCACHE_ENABLE`**:
  unset them, or set `UCX_TLS` to RDMA transports (`rc`, `dc`) and
  `UCX_RCACHE_ENABLE=n`. Only for a test on a host without an RDMA NIC,
  `--allow-unchecked-transport` lets it run on other transports; a late RDMA
  WRITE can then land in a region after its owner gave it back.
- **`cannot take the node lock`**: another region mapper runs on this node,
  usually the service. One region mapper serves every app on the node; give
  maru-server its address instead of starting another one.
- **Engines start but never hit**: check that both engines use the same
  `maru_cache_namespace` and `maru_server_url`, and that the prompt is longer
  than one chunk.
- **maru-server keeps a stopped engine's regions** (the pool stays used after
  the engines are gone): an engine process that gets the stop signal itself,
  for example from a `kill` of its whole process group, can end before its
  regions go back. Stop an engine with Ctrl-C in the launcher's terminal or
  `kill -TERM` on the launcher; the launcher passes the signal on to vLLM's
  API server only. maru-server frees the regions it keeps when it stops.
- **Engines on another node cannot connect**: maru-server and the region
  mapper listen on `127.0.0.1` by default. Set `MARU_SERVER_BIND_HOST` and
  `MARU_MEMORY_NODE_HOST` to addresses the workers reach.
