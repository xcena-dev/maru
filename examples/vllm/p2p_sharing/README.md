# Maru-vLLM Direct Integration — P2P KV Cache Sharing

P2P KV cache sharing between two vLLM instances via CXL shared memory, bypassing
LMCache.

## Architecture

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

## Prerequisites

- 2+ NVIDIA GPUs
- maru installed: `uv pip install -e /path/to/maru`
- vLLM v0.14+ installed
- maru-server binary available
- LMCache is optional. When its Python package is installed, the direct
  connector reuses `lmcache.c_ops` for coalesced multi-layer CUDA transfers;
  it does not run the LMCache connector or service. Without `c_ops`, Maru uses
  the compatible per-layer fallback.

## Quick Start (Automated)

```bash
./p2p_example.sh [model]

# Examples:
./p2p_example.sh                        # Default: Qwen/Qwen2.5-0.5B
./p2p_example.sh meta-llama/Llama-3-8B
```

This will:
1. Start `maru-server`
2. Launch two vLLM instances with `MaruKVConnector`
3. Run the P2P KV cache sharing test
4. Clean up all processes

## Step-by-Step (Manual)

### 1. Start maru-server

```bash
source env.sh
maru-server --port $MARU_SERVER_PORT
```

### 2. Launch vLLM instances

```bash
# Terminal 1: Instance 1 (GPU 0)
./p2p_vllm_launcher.sh inst1 Qwen/Qwen2.5-0.5B

# Terminal 2: Instance 2 (GPU 1)
./p2p_vllm_launcher.sh inst2 Qwen/Qwen2.5-0.5B
```

### 3. Run the test

```bash
./run_simple_query.sh                       # prompt + output verification
./run_benchmark.sh --model Qwen/Qwen2.5-0.5B # TTFT measurement
```

## Configuration

All settings are in `env.sh`. Override via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MARU_SERVER_PORT` | `10000 + uid` | MaruServer port |
| `MARU_INST1_PORT` | `12000 + uid + 10` | vLLM instance 1 port |
| `MARU_INST2_PORT` | `12000 + uid + 11` | vLLM instance 2 port |
| `MARU_POOL_SIZE` | `4G` | CXL shared memory pool size |
| `MARU_KV_CHUNK_TOKENS` | `256` | Tokens per KV cache chunk |
| `GPU_MEM_UTIL` | `0.1` | vLLM GPU memory utilization |

## Expected Output

Instance 2 should show lower TTFT (and `Cache Hit: Yes`) because it loads the KV
cache from CXL instead of recomputing prefill.

## Troubleshooting

**Instance 2 TTFT is slower, not faster:**
- Check Instance 2 logs for `Maru: loaded N layers` messages
- If missing, verify `maru-server` is running and both instances connect to it
- For very small models (0.5B) with short prompts, CXL retrieve overhead may
  exceed prefill savings

**Garbage output on Instance 2:**
KV cache data corruption. Ensure per-chunk injection is used (not concatenated
1D load).

## Files

| File | Description |
|------|-------------|
| `env.sh` | Environment variables (ports, pool size, chunk tokens) |
| `p2p_vllm_launcher.sh` | Launch one vLLM instance (inst1/inst2) with MaruKVConnector |
| `p2p_example.sh` | Full automated example (server + 2x vLLM + test) |
| `run_simple_query.sh` | Simple query test: prompt + output verification |
| `run_benchmark.sh` | Benchmark wrapper (around run_benchmark.py) |
| `run_benchmark.py` | TTFT measurement: store on inst1, retrieve on inst2 |

For a single-instance basic connector check, see [`../single/`](../single/).
