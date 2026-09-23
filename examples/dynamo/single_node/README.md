# Maru × Dynamo — Single Node, Multi Instance

Cross-instance KV cache sharing through a real serving stack: an OpenAI-compatible [NVIDIA Dynamo](https://github.com/ai-dynamo/dynamo) frontend routes requests to two `dynamo.vllm` workers, and the workers share KV through one Maru CXL pool. A prompt prefilled on one worker is served from the shared pool when it later lands on the other.

## Architecture

```
                 client (OpenAI HTTP)
                        |
              dynamo.frontend :DYN_HTTP_PORT
              (--router-mode direct)
                /                \
   dynamo.vllm w0 (GPU 0)   dynamo.vllm w1 (GPU 1)
   no OpenAI port —          no OpenAI port —
   health on :SYSTEM_PORT    health on :SYSTEM_PORT
        |                         |
   MaruKVConnector           MaruKVConnector
        +---------- CXL shared pool ----------+
                        |
                 maru-server (metadata)
```

Differences from the plain [`examples/vllm`](../../vllm) example:

- Requests enter **only through the frontend** — workers expose no OpenAI port (their assigned port serves `/health` via `DYN_SYSTEM_PORT`).
- The frontend and workers find each other through **file-backed discovery** (`DYN_FILE_KV`, single node).
- The test pins each request to a specific worker with `{"nvext": {"backend_instance_id": N}}` (frontend runs `--router-mode direct`), which makes the store-on-A / retrieve-on-B split deterministic.

## Prerequisites

- 1+ NVIDIA GPU (two small-model workers fit on one GPU; see below)
- maru installed: `pip install -e /path/to/maru`
- vLLM and Dynamo installed: `pip install "ai-dynamo[vllm]"`
- maru-server binary available, maru-resource-manager running

## Quick Start (Automated)

```bash
./single_node_example.sh [model]

# Examples:
./single_node_example.sh                     # Default: Qwen/Qwen2.5-0.5B
W0_GPU=0 W1_GPU=0 ./single_node_example.sh   # both workers on one GPU
```

This will:
1. Start `maru-server`
2. Start the Dynamo frontend (**before any worker** — see the ordering rule below)
3. Launch two `dynamo.vllm` workers with `MaruKVConnector`
4. Wait until both workers register and the frontend serves completions
5. Run the cross-instance sharing test and clean everything up

Expected output shape: worker A answers the long prompt with a cold prefill; worker B — which has never seen the prompt — retrieves A's KV chunks from the CXL pool instead of recomputing them (prefix caching is disabled on the workers, so Maru is the only cache source). The test prints both answers (identical), the per-request latency, and the definitive proof: worker B's `External prefix cache hit rate` counter goes nonzero. With the tiny default model the prefill is only a few ms, so the latency delta is visible only with larger models / longer prompts.

## Step-by-Step (Manual)

```bash
source env.sh

# 1. maru-server
maru-server --port $MARU_SERVER_PORT

# 2. Frontend — MUST be watching discovery before any worker registers
rm -rf $DYN_FILE_KV
./dynamo_launcher.sh frontend

# 3. Workers (any order, after the frontend)
./dynamo_launcher.sh worker w0
./dynamo_launcher.sh worker w1

# 4. Test
./run_simple_query.sh
```

## Discovery backend: file vs etcd

`DISCOVERY_BACKEND` selects how the frontend and workers find each other:

- **`file` (default)** — zero dependencies: registrations are files under `DYN_FILE_KV` with mtime-based leases and an inotify watcher. Convenient, but the implementation can intermittently miss or drop a worker's `generate` registration (see troubleshooting) — the example scripts carry retries for this.
- **`etcd`** — dynamo's production backend (real leases, lossless watches); reliable, needs a reachable etcd:

```bash
docker run -d --rm --name dynamo-etcd -p 2379:2379 quay.io/coreos/etcd:v3.5.21 \
    etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://localhost:2379

DISCOVERY_BACKEND=etcd ETCD_ENDPOINTS=http://localhost:2379 ./single_node_example.sh
```

Prefer `etcd` whenever reliability matters more than zero setup (benchmarks, CI, anything unattended).

## The frontend-first ordering rule

With file discovery, a worker's `generate` registration is **lease-based and expires within seconds** unless a watching frontend renews it. A frontend started after the workers sees no live endpoints and answers `503 Model not ready` for every request, forever. Always start the frontend first; `single_node_example.sh` encodes this.

Related: readiness can also flap 503 briefly right after registration (probe with a real 1-token completion, not `/health`), and discovery can occasionally miss or expire a worker's `generate` registration even after a correct start (the worker's log shows `Registered endpoint ...generate` but the frontend registry never lists it, or the frontend log shows `storage::kv::file: Expired`) — restarting just that worker recovers; the frontend and maru-server can stay up. `single_node_example.sh` retries each worker once automatically for this reason.

## Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| Every request 503s forever | Frontend started after the workers (lease expired) — restart the workers, or restart everything in the documented order |
| 503 only for the first seconds | Normal readiness flap — retry |
| Frontend log shows `Expired ... generate ...` and requests keep failing | Discovery dropped a worker's registration — restart that worker only |
| Both workers on one GPU OOM | Lower `GPU_MEM_UTIL` (default 0.3) or use a smaller model |

## Multi node

Planned as `examples/dynamo/multi_node/`: file discovery is single-node only — cross-host deployment switches to etcd/NATS discovery and a Maru resource manager reachable from every node.
