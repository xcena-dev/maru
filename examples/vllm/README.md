# Maru-vLLM Direct Integration Example

P2P KV cache sharing between two vLLM instances via CXL shared memory, bypassing LMCache.

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
- maru installed: `pip install -e /path/to/maru`
- vLLM v0.14+ installed
- maru-server binary available

## Quick Start (Automated)

Run everything with a single script:

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
./launch_vllm.sh inst1 Qwen/Qwen2.5-0.5B

# Terminal 2: Instance 2 (GPU 1)
./launch_vllm.sh inst2 Qwen/Qwen2.5-0.5B
```

### 3. Run the test

```bash
./run_test.sh --model Qwen/Qwen2.5-0.5B
```

Or directly:

```bash
python run_request.py \
    --model Qwen/Qwen2.5-0.5B \
    --port1 $MARU_INST1_PORT \
    --port2 $MARU_INST2_PORT \
    --max-tokens 64
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
| `VLLM_LOG_LEVEL` | `DEBUG` | vLLM log level |

## Expected Output

```
[Session 1] Instance 1 (port 13019) - Store KV
  [store] iter 1/1: TTFT=103.0 ms, total=234.3 ms
  [store] answer: ...

[Session 2] Instance 2 (port 13020) - Retrieve KV
  [retrieve] iter 1/1: TTFT=42.7 ms, total=177.2 ms
  [retrieve] answer: ...

============================================================
  Maru-vLLM Direct P2P KV Cache Sharing
============================================================
  Instance 1 (store):    TTFT = 103.0 ms
  Instance 2 (retrieve): TTFT = 42.7 ms
  TTFT Speedup:         2.41x
  Cache Hit:            Yes
============================================================
```

Instance 2 should show lower TTFT because it loads KV cache from CXL instead of recomputing prefill.

## Test Options

```bash
python run_request.py --help

Options:
  --model MODEL          Model name (default: Qwen/Qwen2.5-0.5B)
  --port1 PORT           Instance 1 port
  --port2 PORT           Instance 2 port
  --max-tokens N         Max tokens to generate (default: 64)
  --repeat-count N       Repeat test N times (default: 1)
  --wait-time SEC        Wait between sessions for CXL propagation (default: 3.0)
```

## Troubleshooting

**BFloat16 errors in logs:**
Ensure you have the latest `maru_vllm/connector.py` — the save path uses `torch.untyped_storage()` to bypass numpy's lack of bf16 support.

**Instance 2 TTFT is slower, not faster:**
- Check Instance 2 logs for `Maru: loaded N layers` messages
- If missing, verify `maru-server` is running and both instances connect to it
- For very small models (0.5B) with short prompts, CXL retrieve overhead may exceed prefill savings

**Garbage output on Instance 2:**
KV cache data corruption. Ensure per-chunk injection is used (not concatenated 1D load).

**Connection errors:**
```bash
# Verify maru-server is running
curl -s http://localhost:$MARU_SERVER_PORT/health

# Check logs
tail -f maru_server.log
```

## Files

| File | Description |
|------|-------------|
| `env.sh` | Environment variables (ports, pool size, chunk tokens) |
| `launch_vllm.sh` | Launch a single vLLM instance with MaruKVConnector |
| `run_test.sh` | Run P2P test (wrapper around run_request.py) |
| `run_request.py` | TTFT measurement: store on inst1, retrieve on inst2 |
| `p2p_example.sh` | Full automated example (server + 2x vLLM + test) |
