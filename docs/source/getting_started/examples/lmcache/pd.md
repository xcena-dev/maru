# Disaggregated Prefill (PD)

This example demonstrates how to run disaggregated prefill with vLLM and LMCache using Maru as the shared KV cache backend.

## Overview

Disaggregated prefill separates the compute-intensive **prefill** phase from the memory-intensive **decode** phase across different GPU instances. The prefiller generates KV cache and stores it into CXL shared memory via Maru. The decoder retrieves it directly from shared CXL memory. This eliminates network transfer overhead between prefiller and decoder, as both access the same shared memory directly.

## Prerequisites

- At least **2 GPUs**
- [LMCache](https://github.com/LMCache/LMCache) **>= v0.3.14** installed (`pip install lmcache`)
- [vLLM](https://docs.vllm.ai/) installed
- Maru installed (see {doc}`../../installation`)

## Configuration

### Prefiller / Decoder config

Both prefiller and decoder use the same configuration:

```yaml
enable_pd: False
chunk_size: 256
local_cpu: False
save_unfull_chunk: True
# Maru backend
maru_path: "maru://localhost:${MARU_SERVER_PORT}"
maru_pool_size: 4

extra_config:
  save_chunk_meta: False
  lookup_backoff_time: 0.001
```

Maru is loaded as an LMCache [storage backend](https://docs.lmcache.ai/kv_cache/storage_backends/index.html). For details on each configuration field, see {doc}`../../../integration/lmcache`.

## How to Run

(Optional) Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 1. Launch the PD setup

The launcher script starts MaruServer, prefiller, decoder, and proxy automatically:

```bash
cd examples/lmcache/disagg_prefill/1p1d
./disagg_example_1p1d.sh
```

Wait until you see:

```
All servers are up. You can send request now...
```

### 2. Try a simple query

Open a new terminal and send a single prompt through the proxy:

```bash
cd examples/lmcache/disagg_prefill/1p1d

# Send a prompt — the proxy routes it to prefiller (KV generation) then decoder (token generation)
./run_simple_query.sh
```

You'll see the prompt and the generated response printed directly. Check `decoder.log` for cache hit messages:

```
LMCache INFO: [req_id=cmpl-a5a94ea4577d4025-0] Retrieved 256 out of 256 required tokens (from 256 total tokens). size: 0.0029 gb, cost 3.0579 ms, throughput: 0.9581 GB/s; (cache_engine.py:874:lmcache.v1.cache_engine)
```

### 3. Run a benchmark

Once you've confirmed the setup works, measure throughput with a larger workload:

```bash
./run_benchmark.sh
```

This runs `vllm bench serve` with 30 random prompts against the proxy, measuring request throughput and latency under disaggregated inference.

Press `Ctrl+C` in the first terminal to stop all servers.