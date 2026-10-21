# vLLM with CXL on memory nodes over RDMA

The `remote_cxl` storage backend lets vLLM engines on nodes without CXL keep their
KV cache in the CXL memory of other nodes, the memory nodes, and move it over
RDMA. Resources flow as for local CXL: the resource manager gives regions to
maru-server, and maru-server gives them to each worker's MaruHandler. What
moves to the memory node is the mapping: a remote CXL agent on every memory node
maps the regions Handlers ask for and registers them with that node's NIC. The
worker then reads and writes them with one-sided RDMA; the bytes do not pass
through any Maru process.

```
worker node (any number)          metadata node              memory node (each node of the pool)
vLLM + MaruKVConnector            maru-server                remote CXL agent
  MaruHandler                     storage sessions,          maps a region, registers it
  (storage_backend="remote_cxl")    regions, keys    <--alloc-- with the NIC   resource manager
    session RPC  ---------------->    |                         ^       (one per pool)
    map / release  ---------------------------------------------+
                                      +-- hold / release -------+  (regions of workers that left)
    staging  <======= one-sided RDMA (NIXL) =======>  NIC ==> CXL memory
```

A worker opens a storage session with maru-server and gets one region of
`maru_pool_size`; when its regions are full it gets one more, as `auto_expand`
does for the cxl backend. Other engines read its keys under a server lease,
mapping the region through the same remote CXL agent.

When a worker stops, its keys stay, as a cxl Handler's keys stay after it
closes: maru-server keeps the worker's regions that hold keys and holds their
mapping at the remote CXL agent, so other engines, and the worker when it starts
again, keep reading them. maru-server gives a kept region back when the
resource manager is full (the one read longest ago first). Every region goes
back to the resource manager only after its remote CXL agent deregistered it
from the NIC, so a WRITE still in flight from the old owner fails at the NIC
instead of landing in the next owner's KV.

Who may map a region is bound as on a CXL-attached node, where a process
needs the CXL device and permission to open its DAX file. The remote CXL agent
listens on the memory node's RDMA network address, and every worker and
maru-server proves the remote CXL agent key on each connection. Readers get a
read handle, which maps a region but cannot release it, so only a region's
owner and maru-server can take a region away.

> **Security:** the key proves who connects, but the region map protocol, like
> the resource manager's, is not encrypted: a host that can watch the traffic
> sees region handles and remote keys. Keep the RDMA network to the hosts of
> the deployment, as the CXL fabric is.

The backend stores KV chunkwise and needs `TP=PP=DP=1`, `--enforce-eager`,
unquantized KV and `kv_load_failure_policy="recompute"`. By default the
backend loads cache hits between steps (`maru_async_load`) and stores after
the forward (`maru_async_store`), so an RDMA transfer that waits for a slow
memory node does not hold up the engine; asynchronous loads run with vLLM's
asynchronous scheduling on. Setting both to `false` (`MARU_SYNC=1` on the
launcher) runs them on the engine thread, and synchronous loads need
`--no-async-scheduling`, which the launcher then passes. The KV moves
between the GPU and the worker's staging buffer, host memory registered with
the NIC and with CUDA; with `maru_kv_ops` built, one copy kernel per chunk
moves it, as for the cxl backend.

## Requirements

- An RDMA NIC (RoCE or InfiniBand) on every worker and memory node, and NIXL
  with its UCX backend in the environment Maru is installed in. For the
  validated vLLM 0.22.1 / Torch 2.11.0 / CUDA 13 stack, install from the Maru
  source root using the constraints for this example:
  `uv pip install -c examples/vllm/remote_cxl/constraints-vllm022.txt nixl`
  (`pip install` in place of `uv pip install` without uv).
- On the memory nodes: the CXL/DAX device of the pool, visible on every memory
  node of the pool, and the resource manager on one of them, as for a local CXL
  deployment. Use a devdax device. Install Maru with NIXL (NIXL first,
  then `./install.sh` from the Maru source tree): the installer also
  sets up the remote CXL agent as a system service that starts and stops with the
  resource manager, so a memory node runs it without a separate step.
- Every worker must reach maru-server and each memory node's remote CXL agent and
  NIC, and so must maru-server reach the remote CXL agents.
- A pool that several memory nodes share: apply the multi-host settings of
  Maru's [System Setup](../../../docs/source/getting_started/bios_setup.md) on
  each of those memory nodes, as on hosts that share CXL memory.
- A copy of the remote CXL agent key (`/etc/maru/remote-cxl-agent.key` on the memory
  nodes, created by the installer) on every worker node and the maru-server
  node, readable only by the user that runs them; they read
  `~/.maru/remote-cxl-agent.key` by default. The memory nodes of a shared
  pool use the same key.
- The remote CXL agent uses `UCX_TLS=rc,dc,cuda_copy` with its registration
  cache off (`UCX_RCACHE_ENABLE=n`). RC/DC carries the remote DRAM bytes;
  `cuda_copy` supplies local CUDA memory-type support required by NIXL 1.5
  on GPU hosts. The agent sets both defaults and rejects TCP, shared-memory
  and CUDA IPC transports, or a registration cache that would keep released
  regions registered.

The serving checks use NIXL 1.2.0 and its matching CUDA wheel. Maru's native
API and CXL-to-host-to-GPU correctness checks also pass with NIXL 1.5.0,
but that does not validate the full vLLM stack. In the tested CUDA 13 wheel,
NIXL 1.5's optional EP plugin has no Torch 2.11 binary, and vLLM 0.22.1
fails during startup when it imports that plugin. Keep the example's
constraints when using this serving stack; select newer dependencies only
after checking them together.

## Quick start (one host)

`remote_cxl_example.sh` runs maru-server and two workers on one host, and uses the
remote CXL agent service when it runs there (otherwise it starts a remote CXL agent
itself). The workers still go through the remote CXL agent's NIC
registration and one-sided RDMA, so the example needs 2 GPUs, an RDMA NIC and
a CXL/DAX device on that host.

```bash
sudo systemctl start maru-resource-manager    # installed by ./install.sh
MARU_DAX_PATH=/dev/dax0.0 MARU_AGENT_UCX_DEVICE=mlx5_0:1 \
MARU_WORKER_UCX_DEVICE=mlx5_0:1 ./remote_cxl_example.sh [model]
```

This will:
1. Use the remote CXL agent service, or start a remote CXL agent (`remote_cxl_agent.sh`)
2. Start maru-server with that remote CXL agent (`maru_server.sh`)
3. Launch two vLLM engines with `storage_backend: "remote_cxl"` (`remote_cxl_vllm_launcher.sh`)
4. Send a new prompt to instance 1, which computes it and stores its KV in the
   pool, then the same prompt to instance 2, which loads the KV from the pool,
   and once more to instance 1, which answers from vLLM's own prefix cache
5. Clean up all processes

Expected output: instance 2 reports most of the prompt as cached tokens and
`Pool hit: Yes`. Only whole chunks are stored, so the cached tokens stop at the
last full `maru_kv_chunk_tokens` chunk. `Same answer` compares instance 2's
answer, which reuses the KV from the pool, with instance 1's answer to the same
prompt from vLLM's own prefix cache; both reuse a prefix of the same length, so
they should match. Instance 1's first answer, computed from the whole prompt,
can differ from both after some tokens: vLLM itself answers differently when it
reuses a prefix instead of computing it. Instance 2
never saw the prompt, so those tokens were found in the pool. vLLM counts them
when it schedules the request; if the RDMA read then fails, the connector logs
`Maru load failed for req ...; recomputing` after a line with the cause (such
as `Maru batch_retrieve failed`) and vLLM recomputes the tokens, so the script
also checks `inst2.log` for these lines.

For Llama-3.1-8B set `GPU_MEM_UTIL=0.5`. Set `MARU_NUMA_NODE` to bind each
engine to the socket of its GPU and NIC.

## Several nodes (step by step)

Three roles start the processes, in this order: the platform operator runs
the resource manager on each memory node, which starts the remote CXL agent with
it (step 1); the app launcher starts maru-server (step 2) and then vLLM on each
worker node (step 3); the app's users send requests (step 4). A prompt stored
by one worker is then loaded by any other worker of the app.

![One app on one pool. 1, the operator runs the resource manager on the memory node, and systemd starts the remote CXL agent with it. 2, the app launcher starts maru-server on the metadata node; it gets regions from the resource manager as workers ask for them. 3, the app launcher starts vLLM on each worker node; each MaruHandler opens a storage session with maru-server and maps its region at the remote CXL agent. 4, request 1 with prompt P goes to worker 1, which stores P's KV in its region R1 by RDMA; request 2 with the same prompt goes to worker 2, which maps R1 at the remote CXL agent with a read handle and loads P's KV from R1 by RDMA](../../../docs/source/image/remote_cxl_user_flow.png)

The example below uses one memory node that also runs maru-server, and two
workers on another node. Replace the addresses, NICs and DAX device.

### 1. Memory nodes

Each memory node of the pool runs a remote CXL agent, a platform service like the
resource manager: one per memory node, shared by every app that uses the pool.
With Maru installed with NIXL, `./install.sh` installs it as
`maru-remote-cxl-agent.service`, which starts and stops with the resource
manager. Set the node's RDMA NIC in `/etc/maru/remote-cxl-agent.env`; the defaults
listen on that NIC's address at port 9860 and use the resource manager on the
same node:

```bash
sudo sed -i 's/^MARU_REMOTE_CXL_AGENT_UCX_DEVICE=.*/MARU_REMOTE_CXL_AGENT_UCX_DEVICE=mlx5_1:1/' \
  /etc/maru/remote-cxl-agent.env
sudo systemctl start maru-resource-manager    # also starts the remote CXL agent
systemctl status maru-remote-cxl-agent
```

The installer also created the remote CXL agent key, `/etc/maru/remote-cxl-agent.key`.
Copy it to every worker node and to the maru-server node, owned by the user
that runs them (the example scripts read `~/.maru/remote-cxl-agent.key`):

```bash
sudo install -D -m 0600 -o "$USER" /etc/maru/remote-cxl-agent.key ~/.maru/remote-cxl-agent.key
scp ~/.maru/remote-cxl-agent.key worker1:.maru/remote-cxl-agent.key   # keep it 0600
```

When maru-server or another memory node reaches the resource manager over the
network, the resource manager listens on a network address, as in Maru's
[multi-node configuration](../../../docs/source/getting_started/installation.md#configuration).
It then answers only on that address, so the remote CXL agent is pointed at it
too:

```bash
sudo systemctl edit maru-resource-manager
#   [Service]
#   ExecStart=
#   ExecStart=/usr/local/bin/maru-resource-manager --host 10.0.0.2 --port 9850
sudo sed -i 's/^MARU_RM_ADDRESS=.*/MARU_RM_ADDRESS=10.0.0.2:9850/' /etc/maru/remote-cxl-agent.env
sudo systemctl restart maru-resource-manager   # restarts the remote CXL agent with it
```

A memory node of a shared pool that does not run the resource manager installs
the remote CXL agent service alone (`--standalone`: it starts at boot on its own)
and points it at the node that does:

```bash
./install.sh --no-rm                          # Maru with NIXL, no resource manager
sudo "$(which install-maru-remote-cxl-agent)" --standalone
sudo sed -i 's/^MARU_RM_ADDRESS=.*/MARU_RM_ADDRESS=10.0.0.2:9850/' /etc/maru/remote-cxl-agent.env
sudo sed -i 's/^MARU_REMOTE_CXL_AGENT_UCX_DEVICE=.*/MARU_REMOTE_CXL_AGENT_UCX_DEVICE=mlx5_1:1/' \
  /etc/maru/remote-cxl-agent.env
sudo install -m 0600 ./remote-cxl-agent.key /etc/maru/remote-cxl-agent.key   # the pool's key
sudo systemctl start maru-remote-cxl-agent
```

On a host without the service (a test host, or Maru installed without NIXL),
`./remote_cxl_agent.sh` runs a remote CXL agent in the foreground instead:

```bash
MARU_MEMORY_NODE_HOST=10.0.0.2 MARU_AGENT_UCX_DEVICE=mlx5_1:1 ./remote_cxl_agent.sh
```

### 2. Metadata node

Start maru-server with the remote CXL agents of all memory nodes of the pool, on
an address the workers reach:

```bash
MARU_SERVER_BIND_HOST=10.0.0.2 MARU_DAX_PATH=/dev/dax1.0 \
MARU_REMOTE_CXL_AGENTS=10.0.0.2:9860 ./maru_server.sh
```

which runs

```bash
maru-server --host 10.0.0.2 --port $MARU_SERVER_PORT --rm-address 127.0.0.1:9850 \
  --remote-cxl-agent 10.0.0.2:9860 --remote-cxl-agent-key-file ~/.maru/remote-cxl-agent.key \
  --dax-path /dev/dax1.0
```

With the resource manager on a network address (step 1), also give
`MARU_RM_ADDRESS=10.0.0.2:9850`; maru-server can then run on any node that
reaches it. maru-server allocates each worker's regions from the resource
manager, as for local CXL, and hands the remote CXL agent list to the workers.
It also talks to the remote CXL agents itself for the regions of workers that
left, so it needs the key. A shared pool lists
every memory node, `--remote-cxl-agent host1:port,host2:port`; each region is then
served by one of them, picked by a hash of the region number over that list, so
every worker reaches a region through the same node. maru-server runs once per
app and pool.

To use several pools, run one maru-server per pool, each with that pool's
resource manager and remote CXL agents, and list them all in the workers'
`maru_server_url`, comma separated
(`MARU_SERVER_URL=tcp://10.0.0.2:<port>,tcp://10.0.0.3:<port>`). A worker opens
a storage session with each maru-server and sends each request to one pool,
picked by a hash of the request's first chunk key.

### 3. Worker nodes

Install Maru with NIXL and without the resource manager (`./install.sh --no-rm`):
a plain `./install.sh` would also set up a remote CXL agent and a key of the
worker's own. Then point the engines at maru-server:

```bash
export MARU_SERVER_URL=tcp://10.0.0.2:<maru-server port> MARU_WORKER_UCX_DEVICE=mlx5_0:1
MARU_INST1_GPU=0 ./remote_cxl_vllm_launcher.sh inst1 [model]
MARU_INST2_GPU=1 ./remote_cxl_vllm_launcher.sh inst2 [model]
```

The example's default ports derive from the user ID, which can differ between
nodes, so give the workers the port `maru_server.sh` printed. The launcher
runs (replace the model, namespace, address and NIC):

```bash
vllm serve <model> --enforce-eager --enable-prompt-tokens-details \
  --kv-transfer-config '{
    "kv_connector": "MaruKVConnector",
    "kv_connector_module_path": "maru_vllm",
    "kv_role": "kv_both",
    "kv_load_failure_policy": "recompute",
    "kv_connector_extra_config": {
      "maru_server_url": "tcp://10.0.0.2:<maru-server port>",
      "maru_storage_backend": "remote_cxl",
      "maru_remote_cxl_agent_key_file": "/home/<user>/.maru/remote-cxl-agent.key",
      "maru_rdma_device": "mlx5_0:1",
      "maru_pool_size": "4G",
      "maru_engine_id": "<unique per engine>",
      "maru_cache_namespace": "my-model-immutable-weights-revision",
      "maru_kv_chunk_tokens": 256
    }
  }'
```

`maru_cache_namespace` is the sharing scope: engines share KV when it and the
model shape match, whichever path each node loads the weights from, so use a
namespace that identifies the exact weights.
`maru_engine_id` names the engine's storage session and must be unique among
the engines of one maru-server; the launcher uses `<hostname>-<instance>`.
`maru_pool_size` is the CXL memory the engine gets at a time (one region), as
with the cxl backend. `--enable-prompt-tokens-details` only
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
| `MARU_MEMORY_NODE_HOST` | `127.0.0.1` | `remote_cxl_agent.sh`: memory node's address on the RDMA network; the remote CXL agent listens there |
| `MARU_AGENT_PORT` | `10500 + uid` | `remote_cxl_agent.sh`: remote CXL agent port |
| `MARU_AGENT_UCX_DEVICE` | UCX default | `remote_cxl_agent.sh`: RDMA NIC the remote CXL agent registers regions with, e.g. `mlx5_1:1` |
| `MARU_AGENT_LOCK_FILE` | `/tmp/maru-remote-cxl-agent.lock` | `remote_cxl_agent.sh`: keeps one remote CXL agent per node |
| `MARU_REMOTE_CXL_AGENT_KEY_FILE` | `~/.maru/remote-cxl-agent.key` | Remote CXL agent key; `remote_cxl_agent.sh` creates it if missing, maru-server and the workers read it |
| `MARU_SERVER_BIND_HOST` | `127.0.0.1` | Interface maru-server listens on |
| `MARU_SERVER_PORT` | `10000 + uid` | maru-server port |
| `MARU_DAX_PATH` | any pool | DAX device of the pool |
| `MARU_REMOTE_CXL_AGENTS` | this node's remote CXL agent (the service's, if it runs) | Remote CXL agents of the pool, `host:port`, comma separated |
| `MARU_SERVER_URL` | `tcp://$MARU_SERVER_HOST:$MARU_SERVER_PORT` | maru-server as workers see it; set it on worker nodes |
| `MARU_POOL_SIZE` | `4G` | Region size; a worker gets one more region each time its regions are full |
| `MARU_WORKER_UCX_DEVICE` | UCX default | RDMA NIC on each worker, e.g. `mlx5_0:1` |
| `MARU_CACHE_NAMESPACE` | `maru-remote-cxl-example` | Sharing scope of the engines |
| `MARU_KV_CHUNK_TOKENS` | `256` | Tokens per KV chunk |
| `MARU_INST1_GPU` / `MARU_INST2_GPU` | `0` / `1` | GPU of each engine |
| `MARU_NUMA_NODE` | none | NUMA node to bind each engine to |
| `MARU_LOG_TIMING` | off | Log the connector's load and store timings |
| `MARU_SYNC` | off | Load and store on the engine thread instead of between steps and after the forward |
| `GPU_MEM_UTIL` | `0.1` | vLLM GPU memory utilization |

## Troubleshooting

- **The remote CXL agent refuses to start with `UCX_TLS` or `UCX_RCACHE_ENABLE`**:
  unset them, or include an RDMA transport (`rc`, `dc`) and `cuda_copy` in
  `UCX_TLS`, with
  `UCX_RCACHE_ENABLE=n`. Only for a test on a host without an RDMA NIC,
  `--allow-unchecked-transport` lets it run on other transports; a late RDMA
  WRITE can then land in a region after its owner gave it back.
- **`cannot take the node lock`**: another remote CXL agent runs on this node,
  usually the service. One remote CXL agent serves every app on the node; give
  maru-server its address instead of starting another one.
- **Engines start but never hit**: check that both engines use the same
  `maru_cache_namespace` and `maru_server_url`, and that the prompt is longer
  than one chunk.
- **The pool stays used after the engines stop**: maru-server keeps the
  regions of stopped engines that still hold keys, so other engines and
  restarted ones read them. It gives them back when the resource manager is
  full; `maru-server` logs `giving back remote CXL region`.
- **`key check failed`** (in the engine or maru-server log): the process has
  no copy of the remote CXL agent key, or another one. Copy the memory nodes'
  `/etc/maru/remote-cxl-agent.key` to `~/.maru/remote-cxl-agent.key` of the
  process's user (0600), or point `maru_remote_cxl_agent_key_file` or
  `--remote-cxl-agent-key-file` at another copy. A worker installed with a plain
  `./install.sh` also runs an agent with a key of its own in `/etc/maru`; remove
  that service with `sudo "$(which install-maru-remote-cxl-agent)" --uninstall`.
- **Engines on another node cannot connect**: maru-server listens on
  `127.0.0.1` by default, and so does `remote_cxl_agent.sh`; the remote CXL agent
  service listens on its RDMA NIC's address. Set `MARU_SERVER_BIND_HOST` and
  `MARU_MEMORY_NODE_HOST` to addresses the workers reach.
