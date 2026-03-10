# P2P KV Cache Sharing

This example demonstrates how to share KV cache across multiple vLLM instances using Maru as a shared storage backend.

## Overview

When multiple vLLM instances serve the same or similar prompts, they redundantly compute and store the same KV cache. By sharing the KV cache through Maru's CXL shared memory, Instance 2 can skip the prefill computation entirely and directly read the KV cache that Instance 1 already stored.

## Prerequisites

- At least **2 GPUs**
- [LMCache](https://github.com/LMCache/LMCache) **>= v0.3.14** installed (`pip install lmcache`)
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

### 2. Try a simple query

Open a new terminal and send a single prompt to both instances:

```bash
cd examples/lmcache/p2p_sharing

# Send a prompt to Instance 1 (store KV cache), then the same prompt to Instance 2 (retrieve)
./run_simple_query.sh
```

You'll see the prompt and both instances' responses printed directly. Check `inst2.log` for cache hit messages:

```
LMCache INFO: [req_id=cmpl-a5a94ea4577d4025-0] Retrieved 256 out of 256 required tokens (from 256 total tokens). size: 0.0029 gb, cost 3.0579 ms, throughput: 0.9581 GB/s; (cache_engine.py:874:lmcache.v1.cache_engine)
```

### 3. Run a benchmark

Once you've confirmed cache sharing works, measure the TTFT (Time-To-First-Token) speedup:

```bash
./run_benchmark.sh
```

This sends streaming requests to both instances and reports the TTFT speedup from KV cache reuse:

```
==========================================================
  P2P KV Cache Sharing - Results
==========================================================
  Session 1 (store):    TTFT = 1234.5 ms
  Session 2 (retrieve): TTFT = 56.7 ms
  TTFT Speedup:         21.77x
  Cache Hit:            Yes
==========================================================
```

Press `Ctrl+C` in the first terminal to stop all servers.
