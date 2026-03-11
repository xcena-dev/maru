# Maru KV Connector for vLLM

A connector that enables direct KV cache sharing between vLLM instances
through Maru CXL shared memory, bypassing LMCache middleware.

## Architecture

```
Previous (via LMCache):
  vLLM → LMCacheConnector → LMCache Engine → StorageManager → MaruConnector → MaruHandler → CXL

New (direct connection):
  vLLM → MaruKVConnector → MaruHandler → CXL
```

## Prerequisites

1. **Maru installed** (includes maru-server, maru-resourced)
2. **vLLM v0.14+** (KVConnectorBase_V1 support)
3. **CXL DAX device** with `maru-resourced` daemon running

## Quick Start

### 1. Install Maru

```bash
cd /path/to/maru
pip install -e .
```

### 2. Verify Maru Resource Manager

```bash
sudo systemctl status maru-resourced
```

### 3. Start Maru Server

```bash
maru-server
# Listens on tcp://0.0.0.0:5555 by default
```

### 4. Launch vLLM

**Option A: Dynamic loading (no vLLM code changes required)**

```bash
vllm serve <model> \
    --kv-transfer-config '{
        "kv_connector": "MaruKVConnector",
        "kv_connector_module_path": "maru_vllm",
        "kv_role": "kv_both",
        "kv_connector_extra_config": {
            "maru_server_url": "tcp://localhost:5555",
            "maru_pool_size": "4G"
        }
    }'
```

**Option B: Factory registration (when using vLLM `feat/maru-kv-connector` branch)**

```bash
vllm serve <model> \
    --kv-transfer-config '{
        "kv_connector": "MaruKVConnector",
        "kv_role": "kv_both",
        "kv_connector_extra_config": {
            "maru_server_url": "tcp://localhost:5555",
            "maru_pool_size": "4G"
        }
    }'
```

### 5. Second vLLM Instance (same node)

Launch with the same config — it will automatically read KV cache from CXL
that was stored by the first instance.

```bash
vllm serve <model> \
    --port 8001 \
    --kv-transfer-config '{
        "kv_connector": "MaruKVConnector",
        "kv_connector_module_path": "maru_vllm",
        "kv_role": "kv_both",
        "kv_connector_extra_config": {
            "maru_server_url": "tcp://localhost:5555",
            "maru_pool_size": "4G"
        }
    }'
```

## Configuration

Settings in `kv_connector_extra_config`:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `maru_server_url` | str | `tcp://localhost:5555` | MaruServer address |
| `maru_pool_size` | str/int | `1G` | CXL memory pool size (`4G`, `500M`, etc.) |
| `maru_chunk_size` | str/int | `4M` | Maru page size (CXL allocation unit) |
| `maru_instance_id` | str | auto | Unique instance ID (default: auto-generated UUID) |
| `maru_eager_map` | bool | `true` | Pre-map other instances' CXL regions on connect |
| `maru_kv_chunk_tokens` | int | `256` | KV cache chunk granularity (in tokens) |

### maru_kv_chunk_tokens

Controls how many tokens per chunk when storing KV cache.

- **Smaller** (64, 128): Finer prefix reuse granularity, more maru keys
- **Larger** (512, 1024): Fewer keys, but coarser reuse granularity
- **Default 256**: Good balance for most use cases
- **Auto-aligned**: Automatically adjusted to a multiple of vLLM block_size

### maru_pool_size

CXL memory allocated per instance.

Capacity estimation:
```
pool_size ≈ num_layers × kv_head_dim × 2(K+V) × max_cached_tokens × dtype_bytes
```

Example (Llama 7B, fp16):
```
32 layers × 128 head_dim × 32 heads × 2(K+V) × 4096 tokens × 2 bytes
≈ 2GB
```

## KV Cache Sharing Behavior

### Chunk-Based Storage

Tokens are divided into fixed-size chunks (default 256) for storage:

```
Prompt: [tok0..tok255 | tok256..tok511 | tok512..tok767 | tok768..tok900]
         chunk 0        chunk 1          chunk 2          (incomplete, not stored)
```

Each chunk key = `kv_{hash(tok0..end)}_L{layer}` (rolling prefix hash)

### Partial Prefix Reuse

```
Instance A:
  Request: "The quick brown fox jumps over the lazy dog. Once upon a time..."
  → Stores chunk 0, 1, 2

Instance B:
  Request: "The quick brown fox jumps over the lazy dog. In a galaxy far away..."
  → chunk 0, 1 hit (common prefix), chunk 2 miss
  → Loads chunk 0, 1 from CXL, computes the rest
```

### Data Flow

```
[Save Path]
  vLLM forward pass
    → save_kv_layer() called per layer
      → Extract chunk slots from GPU KV tensor
        → handler.alloc() → dst.copy_(gpu_tensor)  # GPU→CXL DMA
          → handler.store(key, handle=handle)       # register only

[Load Path]
  Scheduler: batch_exists() checks chunk hits
    → Reports matched chunk count
  Worker: start_load_kv() called
    → handler.retrieve(key) per chunk per layer
      → CXL mmap read (zero-copy, CUDA pinned)
        → torch.frombuffer() → .cuda()  # CXL→GPU DMA
          → Inject into GPU KV buffer
```

## Troubleshooting

### MaruServer Connection Failure

```
ERROR: Failed to connect to MaruServer at tcp://localhost:5555
```
→ Verify `maru-server` is running

### CXL Memory Exhausted

```
ERROR: Cannot allocate page for key ...
```
→ Increase `maru_pool_size` or restart maru-server

### chunk_tokens Alignment Warning

```
WARNING: maru_kv_chunk_tokens 300 not aligned to block_size 16, adjusted to 288
```
→ Normal behavior. Automatically adjusted to a multiple of block_size.

## Comparison with LMCache Path

| Aspect | Via LMCache | Direct (this connector) |
|--------|-------------|------------------------|
| Dependencies | vLLM + LMCache + maru | vLLM + maru |
| Middleware | LMCache Engine, StorageManager, RemoteBackend | None |
| Serialization | LMCache MemoryObj conversion | torch tensor ↔ bytes direct |
| Prefix matching | LMCache CacheEngineKey hashing | vLLM token prefix hashing |
| Configuration | LMCACHE_CONFIG_FILE YAML | kv_connector_extra_config JSON |
| Save path | GPU → CPU → bytes → alloc + memcpy → CXL | GPU → CXL (single DMA via alloc) |
| Load path | CXL → clone → CPU → GPU | CXL → GPU (single DMA, pinned) |
