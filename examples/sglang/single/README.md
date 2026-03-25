# Single SGLang Instance with Maru

Run a single SGLang instance with Maru HiCache L3 backend (CXL shared memory).

## Prerequisites

1. Install Maru: `pip install -e .` (from the `maru/` root)
2. SGLang with [sgl-project/sglang#20560](https://github.com/sgl-project/sglang/pull/20560)
3. At least 1 GPU available

## Usage

```bash
# Start (launches MaruServer + SGLang automatically)
bash single_example.sh

# Override the model (default: Qwen/Qwen2.5-0.5B)
bash single_example.sh --model Qwen/Qwen2.5-7B

# In another terminal, after the server is ready:
bash run_simple_query.sh             # Quick query test
bash run_benchmark.sh                # TTFT benchmark

# Press Ctrl-C to stop all servers
```

## Files

| File | Description |
|------|-------------|
| `single_example.sh` | Main launcher (MaruServer + SGLang) |
| `run_simple_query.sh` | Simple query test |
| `run_benchmark.sh` | TTFT benchmark (two sessions, same prompt) |
| `env.sh` | Port, model, and GPU configuration |

## Port Configuration

All ports are derived from your UID to avoid collisions. Override via
environment variables or edit [env.sh](env.sh).

## Logs

- SGLang server: `sglang_server.log`
- Maru server: `maru_server.log`
