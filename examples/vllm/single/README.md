# Maru-vLLM Direct Integration — Single-Instance Verification

The cheapest way to verify the Maru-vLLM direct connector (`MaruKVConnector`):
one vLLM instance that stores KV to Maru on the first request and reuses it on a
repeat. Use this for a basic connector check at the maru level before running
the cross-instance [`../p2p_sharing/`](../p2p_sharing/) example or the naru
benchmark harness.

## How it works

```
            vLLM (GPU 0, prefix cache OFF)
                      |
                MaruKVConnector
                      |
                 MaruHandler ── CXL Shared Memory
                      |
                 MaruServer (metadata)

1st request → cold: full prefill, KV stored to Maru
2nd request → warm: prefix found in Maru, KV loaded, prefill skipped (cache hit)
```

vLLM's own prefix cache is **disabled** (`--no-enable-prefix-caching` in
`single_vllm_launcher.sh`) so that Maru is the only cache source — otherwise a
repeated prompt would be served from the GPU-resident prefix cache and the
connector's store/load paths would never run.

## Prerequisites

- 1+ NVIDIA GPU
- maru installed: `pip install -e /path/to/maru`
- vLLM v0.14+ installed
- maru-server binary available
- LMCache is optional. When its Python package is installed, the direct
  connector reuses `lmcache.c_ops` for coalesced multi-layer CUDA transfers;
  it does not run the LMCache connector or service. Without `c_ops`, Maru uses
  the compatible per-layer fallback.

## Quick Start (Automated)

```bash
./single_example.sh [--model MODEL]

# Example:
./single_example.sh                     # Default: Qwen/Qwen2.5-0.5B
./single_example.sh --model meta-llama/Llama-3-8B
```

This starts `maru-server` + one vLLM instance and sends the same prompt twice.
`Cache Hit: Yes` with a TTFT speedup confirms both the store and load paths. The
script exits non-zero on a cache miss — check `single.log` for connector errors
(a missing `Maru: loaded N layers` message means the worker load path failed).

## Step-by-Step (Manual)

```bash
# 1. Start maru-server
source env.sh
maru-server --port $MARU_SERVER_PORT

# 2. Launch the single instance (GPU 0, prefix cache OFF)
./single_vllm_launcher.sh Qwen/Qwen2.5-0.5B

# 3. Verify
./run_simple_query.sh                       # prompt + output (same query twice)
./run_benchmark.sh --model Qwen/Qwen2.5-0.5B # TTFT: cold vs warm speedup
```

## Configuration

All settings are in `env.sh`:

| Variable | Default | Description |
|----------|---------|-------------|
| `MARU_SERVER_PORT` | `10000 + uid` | MaruServer port |
| `MARU_INST_PORT` | `12000 + uid + 20` | vLLM instance port |
| `MARU_POOL_SIZE` | `4G` | CXL shared memory pool size |
| `MARU_KV_CHUNK_TOKENS` | `256` | Tokens per KV cache chunk |
| `GPU_MEM_UTIL` | `0.1` | vLLM GPU memory utilization |

## Files

| File | Description |
|------|-------------|
| `env.sh` | Environment variables (ports, pool size, chunk tokens) |
| `single_vllm_launcher.sh` | Launch one vLLM instance with MaruKVConnector, prefix cache OFF |
| `single_example.sh` | Full automated example (server + 1x vLLM + verification) |
| `run_simple_query.sh` | Simple query test: same prompt twice (store then reuse) |
| `run_benchmark.sh` | Benchmark wrapper (around run_benchmark.py) |
| `run_benchmark.py` | TTFT measurement: cold store vs warm Maru reuse |
