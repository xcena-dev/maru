# KV Cache Sharing with Maru

This example demonstrates KV cache sharing between multiple vLLM instances using Maru shared storage.

Maru provides CXL shared memory backed storage, eliminating the need for P2P transport or a separate controller process.

---

## Prerequisites

1. Install maru package

```bash
cd ~/maru
uv pip install -e .
```

2. At least 2 GPUs available

---

## Running the Test

```bash
# Navigate to the p2p_sharing example directory
cd examples/lmcache/p2p_sharing

# Start the environment (MaruServer + 2 vLLM instances)
bash p2p_example.sh

# Run a simple query test (in another terminal)
bash run_request.sh
```

### Verifying Cache Hits via log

Check `inst2.log` for cache hit messages. You should see logs like:
```
LMCache INFO: Retrieved 1002 out of total 1002 tokens. size: 0.1223 gb, cost 60.3595 ms, throughput: 2.0264 GB/s
```

This indicates that Instance 2 successfully retrieved the KV cache from CXL shared storage that was stored by Instance 1.

---

## Port Configuration

You can configure the ports used by the example in [env.sh](env.sh). By default, all ports are calculated based on `LMCACHE_PORT_BASE` (default: `9000 + UID`).
If you encounter port conflicts, change `LMCACHE_PORT_BASE` in [env.sh](env.sh).
