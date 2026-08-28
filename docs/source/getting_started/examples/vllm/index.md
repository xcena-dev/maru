# vLLM Examples

Two runnable examples of the Maru-vLLM direct connector (`MaruKVConnector`),
which moves KV cache through CXL shared memory without LMCache in the serving
path. Both live under `examples/vllm/`:

| Example | Directory | What it shows |
|---------|-----------|---------------|
| Single-instance verification | `examples/vllm/single/` | One instance stores KV and reuses it on a repeat request — the cheapest connector check |
| P2P KV cache sharing | `examples/vllm/p2p_sharing/` | Two instances share KV cache, so the second skips prefill |

Start with the single-instance example: it needs one GPU and isolates the
store and load paths before any cross-instance behavior is involved.

## Prerequisites

- 1 GPU for the single-instance example, 2+ for P2P
- Maru installed: `uv pip install -e /path/to/maru`
- vLLM v0.14+ installed
- `maru-server` binary available

LMCache is optional. When its Python package is present the direct connector
reuses `lmcache.c_ops` for coalesced multi-layer CUDA transfers; it does not
run the LMCache connector or service. Without `c_ops` the connector falls back
to a per-layer transfer.

## Single-instance verification

One vLLM instance stores KV to Maru on the first request and loads it back on
a repeat. vLLM's own prefix cache is disabled in the launcher so that Maru is
the only cache source — otherwise the repeated prompt would be served from the
GPU-resident prefix cache and neither connector path would run.

```
            vLLM (GPU 0, prefix cache OFF)
                      |
                MaruKVConnector
                      |
                 MaruHandler ── CXL Shared Memory
                      |
                 MaruServer (metadata)

1st request → cold: full prefill, KV stored to Maru
2nd request → warm: prefix found in Maru, KV loaded, prefill skipped
```

### Automated

```bash
cd examples/vllm/single
./single_example.sh                              # Default: Qwen/Qwen2.5-0.5B
./single_example.sh --model meta-llama/Llama-3-8B
```

The script starts `maru-server` and one vLLM instance, then sends the same
prompt twice. `Cache Hit: Yes` with a TTFT speedup confirms both paths. It
exits non-zero on a cache miss; check `single.log` for connector errors (a
missing `Maru: loaded N layers` message means the worker load path failed).

### Step-by-step

```bash
cd examples/vllm/single

# 1. Start maru-server
source env.sh
maru-server --port $MARU_SERVER_PORT

# 2. Launch the instance (GPU 0, prefix cache OFF)
./single_vllm_launcher.sh Qwen/Qwen2.5-0.5B

# 3. Verify
./run_simple_query.sh                            # same prompt twice
./run_benchmark.sh --model Qwen/Qwen2.5-0.5B     # TTFT: cold vs warm
```

### Configuration

Settings live in `examples/vllm/single/env.sh` and can be overridden through
the environment:

| Variable | Default | Description |
|----------|---------|-------------|
| `MARU_SERVER_PORT` | `10000 + uid` | MaruServer port |
| `MARU_INST_PORT` | `12000 + uid + 20` | vLLM instance port |
| `MARU_POOL_SIZE` | `4G` | CXL shared memory pool size |
| `MARU_KV_CHUNK_TOKENS` | `256` | Tokens per KV cache chunk |
| `GPU_MEM_UTIL` | `0.1` | vLLM GPU memory utilization |

## P2P KV cache sharing

Two vLLM instances share KV cache through CXL shared memory. The first stores
the prefix, the second loads it and skips prefill.

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

### Automated

```bash
cd examples/vllm/p2p_sharing
./p2p_example.sh                                 # Default: Qwen/Qwen2.5-0.5B
./p2p_example.sh meta-llama/Llama-3-8B
```

The script starts `maru-server`, launches both instances, runs the sharing
test, and cleans up every process it started.

### Step-by-step

```bash
cd examples/vllm/p2p_sharing

# 1. Start maru-server
source env.sh
maru-server --port $MARU_SERVER_PORT

# 2. Launch the instances (separate terminals)
./p2p_vllm_launcher.sh inst1 Qwen/Qwen2.5-0.5B   # GPU 0
./p2p_vllm_launcher.sh inst2 Qwen/Qwen2.5-0.5B   # GPU 1

# 3. Run the test
./run_simple_query.sh                            # prompt + output verification
./run_benchmark.sh --model Qwen/Qwen2.5-0.5B     # TTFT measurement
```

Instance 2 should report a lower TTFT and `Cache Hit: Yes`, because it loads
the KV cache from CXL instead of recomputing prefill.

### Configuration

Settings live in `examples/vllm/p2p_sharing/env.sh`:

| Variable | Default | Description |
|----------|---------|-------------|
| `MARU_SERVER_PORT` | `10000 + uid` | MaruServer port |
| `MARU_INST1_PORT` | `12000 + uid + 10` | vLLM instance 1 port |
| `MARU_INST2_PORT` | `12000 + uid + 11` | vLLM instance 2 port |
| `MARU_POOL_SIZE` | `4G` | CXL shared memory pool size |
| `MARU_KV_CHUNK_TOKENS` | `256` | Tokens per KV cache chunk |
| `GPU_MEM_UTIL` | `0.1` | vLLM GPU memory utilization |

The two examples use different instance ports, so they do not collide when run
on the same machine.

### Troubleshooting

**Instance 2 TTFT is not faster.** Check the Instance 2 log for
`Maru: loaded N layers`. If it is absent, confirm `maru-server` is running and
that both instances connect to it. For very small models with short prompts the
CXL retrieve overhead can exceed the prefill it saves.

**Garbage output on Instance 2.** KV cache corruption — confirm the load is
using per-chunk injection rather than a concatenated 1D load.

## Further reading

Each example directory has a README with its full file listing. For connector
configuration — including the asynchronous load and store settings — see
[vLLM](../../../integration/vllm.md).
