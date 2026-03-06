# Disaggregated Prefill (PD)

This example demonstrates how to run disaggregated prefill with vLLM and LMCache using Maru as the shared KV cache backend.

## Overview

Disaggregated prefill separates the compute-intensive **prefill** phase from the memory-intensive **decode** phase across different GPU instances. The prefiller generates KV cache and stores it into CXL shared memory via Maru. The decoder retrieves it directly from shared CXL memory. This eliminates network transfer overhead between prefiller and decoder, as both access the same shared memory directly.

## Prerequisites

- At least **2 GPUs**
- [LMCache](https://github.com/LMCache/LMCache) installed (`pip install lmcache`)
- [vLLM](https://docs.vllm.ai/) installed
- Maru installed (see {doc}`../../installation`)

## Configuration

### Prefiller / Decoder config

Both prefiller and decoder use the same configuration:

```yaml
enable_pd: False
chunk_size: 256
remote_url: "maru://localhost:${MARU_SERVER_PORT}"
remote_serde: "naive"
local_cpu: False
max_local_cpu_size: 100
save_unfull_chunk: True

extra_config:
  maru_pool_size: "4G"
  save_chunk_meta: False
  lookup_backoff_time: 0.001
```

The key setting is `remote_url: "maru://..."` — this tells LMCache to use Maru as the shared storage backend instead of nixl or Mooncake. All Maru-specific parameters (pool size, etc.) are configured via `extra_config`.

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

### 2. Send a request

Open a new terminal and run the benchmark script:

```bash
cd examples/lmcache/disagg_prefill/1p1d
./run_request.sh
```

This sends benchmark requests to the proxy, which routes them to the prefiller and decoder for disaggregated inference.

Press `Ctrl+C` in the first terminal to stop all servers.