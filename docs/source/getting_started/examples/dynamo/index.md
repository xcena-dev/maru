# Dynamo Examples

Run Dynamo's vLLM backend with Maru to share KV cache between workers.
The scripts live in `examples/dynamo/single_node/`.

## Prerequisites

- A local CXL DEV_DAX pool and a running `maru-resource-manager` (see {ref}`single-host quick start <quickstart-single-host>`). The example starts its own MaruServer, so you do not need to start one.
- Two GPUs by default, or enough memory on one GPU for two small-model workers.
- Maru installed with its KV placement kernels (see {doc}`../../installation`).
- Dynamo's vLLM backend installed in the **same Python environment** as Maru
  (see [Dynamo's vLLM installation guide](https://docs.nvidia.com/dynamo/backends/v-llm)).

## P2P KV Cache Sharing

One Dynamo frontend routes requests to two `dynamo.vllm` workers on a single
host. The first worker stores KV cache in Maru; the second reuses it for the
same prompt. The launcher disables vLLM's local prefix cache for this check.

This example runs on a single host only. A multi-host Dynamo example is coming soon.

### Automated

```bash
cd examples/dynamo/single_node
./single_node_example.sh                     # Qwen/Qwen2.5-0.5B; GPUs 0 and 1

# Both small-model workers on GPU 0
W0_GPU=0 W1_GPU=0 ./single_node_example.sh

# Another model on the default two GPUs
./single_node_example.sh meta-llama/Llama-3.1-8B-Instruct
```

The script starts MaruServer, the frontend, and both workers, runs the sharing
check, and stops the processes it started. Logs are saved in the example
directory as `maru_server.log`, `frontend.log`, `w0.log`, and `w1.log`.

**Success:** both workers return the same answer, and the second worker reports
a nonzero external prefix-cache hit rate. The script exits nonzero if either
check fails. Latency may not improve for the small default model.

### Step-by-step

In **every terminal**, activate the same environment and run the setup below.
Choose a new, unused discovery directory for each run and use the same path
in every terminal:

```bash
cd examples/dynamo/single_node
export MODEL=Qwen/Qwen2.5-0.5B
export DYN_FILE_KV=/tmp/maru-dynamo-manual-run-1
source env.sh
```

Run each service in a separate terminal, in this order:

1. Start MaruServer:

   ```bash
   maru-server --port "$MARU_SERVER_PORT"
   ```

2. Start the frontend and wait for its health endpoint to respond:

   ```bash
   ./dynamo_launcher.sh frontend
   ```

   From another terminal with the same environment:

   ```bash
   curl --fail "http://localhost:${DYN_HTTP_PORT}/health"
   ```

3. Start each worker in its own terminal. Wait for `w0` to initialize before
   starting `w1`. Keep the logs as shown; the verification script reads them:

   ```bash
   set -o pipefail
   ./dynamo_launcher.sh worker w0 2>&1 | tee w0.log
   ```

   In the other worker terminal:

   ```bash
   set -o pipefail
   ./dynamo_launcher.sh worker w1 2>&1 | tee w1.log
   ```

4. Check that the frontend lists both workers. The response should contain two `instances` entries whose `endpoint` is `generate`:

   ```bash
   curl -s "http://localhost:${DYN_HTTP_PORT}/health"
   ```

   Then run:

   ```bash
   ./run_simple_query.sh
   ```

   The frontend can return HTTP 503 for a short time after the workers register. If the query script fails with a 503, wait a few seconds and run it again.

The query script sends the same prompt to each worker through the frontend
and checks both the answers and the second worker's external cache hit rate.
Stop the services with Ctrl+C when finished.

### Configuration

Settings live in `examples/dynamo/single_node/env.sh` and can be overridden
through the environment:

| Variable | Default | Description |
|----------|---------|-------------|
| `DYN_HTTP_PORT` | `14000 + uid` | Frontend HTTP port |
| `DYN_W0_SYSTEM_PORT`, `DYN_W1_SYSTEM_PORT` | Frontend port + `81`, + `82` | Worker health/metrics ports |
| `MARU_SERVER_PORT` | `10000 + uid` | Local MaruServer port |
| `MARU_POOL_SIZE` | `4G` | Initial CXL allocation per handler |
| `MARU_KV_CHUNK_TOKENS` | `256` | Tokens in a complete stored chunk |
| `W0_GPU`, `W1_GPU` | `0`, `1` | Worker GPU assignment |
| `GPU_MEM_UTIL` | `0.3` | vLLM memory utilization per worker |
| `DYN_FILE_KV` | `/tmp/dynamo_kv_<uid>` | Shared local discovery directory |

Send API requests to the frontend; worker system ports serve health and metrics.
The automated script clears `DYN_FILE_KV` for file discovery at startup, so use
a directory dedicated to this example.

## Further reading

- {doc}`../../../integration/dynamo` — Integration architecture
- {doc}`../../../integration/vllm` — Shared connector settings
- [Example README](https://github.com/xcena-dev/maru/blob/main/examples/dynamo/single_node/README.md)
  — Discovery backends, etcd setup, and troubleshooting
