# vLLM with CPU DRAM and CXL together (M2a)

One vLLM worker opens both fixed L1 pools. A prefix can have some KV chunks in
CPU DRAM and others in CXL; a single load restores them in token order. SSD is
not part of this mode.

## Run

Start a CXL-enabled MaruServer against your existing Resource Manager. The worker
must have read/write access to the DAX device configured in that RM:

```bash
maru-server --host 127.0.0.1 --port 5555 --rm-address 127.0.0.1:9850
```

Do not use `--cpu-only` for mixed mode. An optional `--dax-path /dev/daxX.Y`
restricts the server's CXL allocation to that RM pool.

```bash
vllm serve <model> --enforce-eager \
  --kv-transfer-config '{
    "kv_connector": "MaruKVConnector",
    "kv_connector_module_path": "maru_vllm",
    "kv_role": "kv_both",
    "kv_load_failure_policy": "recompute",
    "kv_connector_extra_config": {
      "maru_storage_backend": "mixed",
      "maru_server_url": "tcp://127.0.0.1:5555",
      "maru_cpu_pool_size": "8G",
      "maru_cxl_pool_size": "32G",
      "maru_write_order": ["cpu", "cxl"],
      "maru_read_order": ["cpu", "cxl"],
      "maru_engine_id": "inference-a",
      "maru_cache_namespace": "my-model-immutable-weights-revision",
      "maru_kv_chunk_tokens": 256
    }
  }'
```

Replace the model and namespace with the exact checkpoint/revision. Each engine
needs a unique `maru_engine_id`. Use `maru_cxl_pool_size` in mixed mode; the legacy
`maru_pool_size` setting is rejected to avoid ambiguous capacity limits.

## Placement and reads

| Setting | Effect |
| --- | --- |
| `maru_write_order: ["cpu", "cxl"]` | Allocate a new chunk in CPU; use CXL when the CPU pool has no free page. |
| `maru_write_order: ["cxl", "cpu"]` | Allocate in CXL first, then CPU. |
| `maru_read_order` | Independently choose which existing replica to read first. A key present in only one medium is read there. |

Both orders default to `["cpu", "cxl"]` and must contain each medium exactly once.
If both pools are full, new cache stores are skipped and existing entries remain
readable. There is no automatic pool growth or eviction. Allocation chooses one
destination; it does **not** automatically copy the chunk into both pools.

The server's typed directory records each pool, replica location, capacity and
read lease. It can represent a replica in each medium independently. The current
connector skips storing a key it already owns; automatic replication/promotion
is a later feature. Static selection is isolated in `FixedOrderPolicy` so later
policies can use the same pool and replica descriptors.

## Scope and lifetime

M2a retains the CPU baseline's single-worker, text-only, homogeneous unquantized
KV, synchronous chunkwise and eager requirements. CPU and CXL transfers both
use synchronous copies; this path does not require the optional CUDA extension
or depend on pinning the host mapping.

**Both pools are scoped to the owning worker in this version.** Mixed CXL regions
and keys are separate from the legacy shared CXL cache. Another engine cannot
read either of these typed pools yet. Cross-engine CXL access, sharing-triggered
CPU-to-CXL replication, eviction and SSD remain subsequent milestones.

Mixed startup requires both pool reservations and a successful local CXL map.
It does not silently become CPU-only when CXL setup fails. Capacity is rounded
down to whole KV-object pages. The RM may reserve a larger aligned CXL region;
only the configured page capacity is allocatable by the worker. `reserved_bytes`
reports that physical reservation separately.

Heartbeat expiry hides replicas in **both** media, but does not reclaim either
pool: the process might still hold buffers. A drained close must first release
all read leases and exported views, unmap both pools, and only then return the
CXL region. Unknown commit replies keep their pages quarantined until the same
operation is resolved; they cannot be overwritten by a later allocation.

Stop workers gracefully before restarting MaruServer. A server restart requires
fresh worker sessions. Abruptly dead owners can leave CPU grants and RM CXL
allocations reserved; automatic reclamation of those allocations is not
implemented. Resetting CPU metadata alone does not reclaim an RM CXL allocation.

`marutop usage` shows a row for each L1 medium; the existing CXL allocation table
also accounts for typed CXL KV bytes. `RpcClient.get_usage().l1_storage` contains
pool capacities, physical reservations, usage, selected policies, lease counts
and cumulative acquired-object counts. `cpu_storage` remains CPU-only.

## Validate

```bash
PYTHONPATH="$PWD" python -m pytest -q \
  tests/unit/test_mixed_storage.py tests/unit/test_vllm_cpu_config.py

# Requires a running RM and DAX access for the test process.
PYTHONPATH="$PWD" python -m pytest -q tests/integration/test_mixed_storage.py
```

The integration test allocates a small temporary CXL region from the existing
RM, starts its own metadata server and returns the allocation on graceful
cleanup. It covers both write priorities, both RPC clients, and exact CPU/GPU
restoration of a prefix split across CPU and CXL. CUDA cases skip without a GPU;
DAX tests skip if the RM or device access is unavailable. Set
`MARU_TEST_RM_ADDRESS` for a non-default RM address.

See the [CPU-only example](../cpu/README.md) for handler read-lease usage and the
[tiered-cache design](../../../docs/source/design_doc/tiered_kv_cache.md) for later
sharing, eviction and SSD milestones.
