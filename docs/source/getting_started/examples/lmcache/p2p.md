# P2P KV Cache Sharing

This example demonstrates how to share KV cache across multiple vLLM instances using Maru as a shared storage backend.

## Overview

When multiple vLLM instances serve the same or similar prompts, they redundantly compute and store the same KV cache. By sharing the KV cache through Maru's CXL shared memory, Instance 2 can skip the prefill computation entirely and directly read the KV cache that Instance 1 already stored.

## Prerequisites

- At least **2 GPUs**
- [LMCache](https://github.com/LMCache/LMCache) installed (`pip install lmcache`)
- [vLLM](https://docs.vllm.ai/) installed
- Maru installed (see {doc}`../../installation`)

## Configuration

Both instances share a single configuration file (`maru-config.yaml`):

```yaml
chunk_size: 256
local_cpu: True
max_local_cpu_size: 5
enable_async_loading: True

enable_p2p: False
enable_controller: False

remote_url: "maru://localhost:${MARU_SERVER_PORT}"
remote_serde: "naive"

extra_config:
  maru_pool_size: "4G"
  save_chunk_meta: False
  lookup_backoff_time: 0.001
```

The key setting is `remote_url: "maru://..."` — this tells LMCache to use Maru as the shared storage backend. With `enable_p2p: False` and `enable_controller: False`, no controller or direct peer connections are needed. All sharing happens through CXL shared memory.

## How to Run

(Optional) Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 1. Launch two vLLM instances

The launcher script starts MaruServer and both vLLM instances automatically:

```bash
cd examples/lmcache/p2p_sharing
./p2p_example.sh
```

Wait until you see:

```
All servers are up. You can send request now...
```

### 2. Send requests

Open a new terminal and send requests:

```bash
cd examples/lmcache/p2p_sharing

# Send the same prompt to Instance 1 (store KV cache) then Instance 2 (retrieve KV cache)
./run_request.sh
```

### 3. Verify cache hits

Check `inst2.log` for cache hit messages:

```
LMCache INFO: Retrieved 1002 out of total 1002 tokens. size: 0.1223 gb, cost 60.3595 ms, throughput: 2.0264 GB/s
```

This confirms Instance 2 retrieved the KV cache from CXL shared memory instead of recomputing it.

Press `Ctrl+C` in the first terminal to stop all servers.
