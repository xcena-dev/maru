# KV Cache P2P Sharing with SGLang + Maru

Two SGLang instances share KV cache through Maru CXL shared memory.
Instance 1 computes and stores KV cache; Instance 2 retrieves it from the
shared L3 pool, reducing TTFT on repeated prompts.

## Prerequisites

1. Install Maru: `pip install -e .` (from the `maru/` root)
2. SGLang with [sgl-project/sglang#20560](https://github.com/sgl-project/sglang/pull/20560)
3. At least 2 GPUs available

## Running the Example

```bash
# Start MaruServer + 2 SGLang instances
bash p2p_example.sh start

# In another terminal:
bash run_simple_query.sh        # Send queries to both instances
bash run_p2p_test.sh            # Detailed 4-step P2P verification
bash run_benchmark.sh           # TTFT benchmark with cache hit detection

# Stop all processes
bash p2p_example.sh stop
```

## How P2P Sharing Works

1. **Instance 1 — first query**: Prefill generates KV cache, stored in GPU L1
   (not yet written to L3).
2. **Instance 1 — second query**: L1 cache hit increments the hit counter
   past the write-through threshold → async flush: GPU → Host DRAM (L2) → Maru CXL (L3).
3. **Instance 2 — query**: Calls `batch_exists()` against Maru, finds KV in L3,
   retrieves via `batch_get()` → skips prefill computation.

## Verifying Cache Hits

Check the benchmark output for `cache_hit: true` and TTFT speedup > 1.5x.
Alternatively, inspect Instance 2 logs for Maru retrieval messages.

## Files

| File | Description |
|------|-------------|
| `p2p_example.sh` | Orchestrator (start/stop/restart) |
| `sglang_launcher.sh` | Per-instance launcher |
| `run_simple_query.sh` | Query both instances |
| `run_p2p_test.sh` | 4-step P2P verification with explanations |
| `run_benchmark.sh` | TTFT benchmark (cache hit detection) |
| `configs/maru-sglang-p2p.json` | Maru backend config template (envsubst) |
| `env.sh` | Port, model, GPU, and log level configuration |

## Port Configuration

All ports are derived from your UID to avoid collisions. Override via
environment variables or edit [env.sh](env.sh).

## Logs

- Maru server: `maru_server.log`
- Instance 1: `inst1.log`
- Instance 2: `inst2.log`
