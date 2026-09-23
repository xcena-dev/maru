# vLLM with a process-local CPU DRAM cache (M1)

The CPU backend stores KV in the vLLM worker's DRAM. MaruServer tracks its
locations, capacity and owner session. This mode needs neither a CXL device nor
the Maru Resource Manager. The model still runs on a GPU.

This first version supports one worker (`TP=PP=DP=1`), homogeneous, unquantized
KV layers, chunkwise storage and synchronous transfers under `--enforce-eager`.
It has a fixed capacity: once full, new cache stores are skipped and existing
entries remain reusable. To use CPU and CXL in the same engine, use the
[mixed L1 mode](../mixed/README.md). Automatic eviction, host CPU sharing,
layerwise/async transfer and SSD are later milestones.

## Run

In an environment with a compatible vLLM installed, install this checkout:

```bash
MARU_SKIP_KV_OPS=1 uv pip install -e .
```

M1 uses synchronous pageable host copies, so it does not require the optional
`maru_kv_ops` CUDA extension. This is a correctness baseline, not the final
pinned-memory performance path.

Start the metadata server:

```bash
python -m maru_server --cpu-only --host 127.0.0.1 --port 5555
```

Start vLLM with your text model (replace the model and namespace):

```bash
vllm serve <model> --enforce-eager \
  --kv-transfer-config '{
    "kv_connector": "MaruKVConnector",
    "kv_connector_module_path": "maru_vllm",
    "kv_role": "kv_both",
    "kv_load_failure_policy": "recompute",
    "kv_connector_extra_config": {
      "maru_storage_backend": "cpu",
      "maru_server_url": "tcp://127.0.0.1:5555",
      "maru_cpu_pool_size": "8G",
      "maru_engine_id": "inference-a",
      "maru_cache_namespace": "my-model-immutable-weights-revision",
      "maru_kv_chunk_tokens": 256
    }
  }'
```

Use a namespace that identifies the exact weights/revision, including changes
to local checkpoint files. The connector additionally fingerprints the model
configuration, dtype and KV geometry. LoRA, multimodal models, MLA, quantized KV,
multiple KV cache groups and parallel workers are rejected. Requests with a
cache salt or prompt embeddings bypass CPU caching.

`maru_engine_id` binds the scheduler to its one worker. Use a different value for
each independent vLLM engine. A second live worker with the same ID is rejected.
An engine cannot read another engine's process-local CPU replica.

`kv_load_failure_policy="recompute"` is required: a replica disappearing after
scheduler discovery must lead to recomputation rather than a failed request.
`maru_async_load`, `maru_async_store`, `maru_overlap_load_with_compute` and
`maru_use_layerwise` must stay false. Existing CXL settings/defaults are unchanged;
use `maru_cpu_pool_size` instead of the CXL-specific `maru_pool_size` in CPU mode.

Send a prompt with at least one complete KV chunk, then send it again. The
scheduler rechecks CPU residency on each lookup. A fully cached prompt still
leaves a block of compute, following vLLM's scheduler contract. For measurements
of this backend, disable vLLM's own GPU prefix caching so it does not hide CPU hits.

## Capacity and lifetime

- Capacity is **per worker**, rounded down to a whole number of KV object pages.
  The default page size is computed from the model's complete chunkwise KV object.
- The server grants at most 64 GiB of CPU pools per `node_id` by default. Override
  it with `--cpu-capacity-limit <bytes>`. The client defaults `node_id` to the
  hostname; set `maru_node_id` consistently for processes sharing the same host.
- Worker heartbeats keep their sessions live. After 30 seconds without one,
  replicas stop being discoverable. A timeout does not prove the process has
  freed memory, so its pool grant remains charged until a drained close.
- Stop CPU workers gracefully before restarting the metadata server. M1 does
  not recover an old CPU session after a server restart; restart the workers to
  create fresh pools. For an abruptly killed worker whose grant cannot be
  acknowledged, stop the other CPU workers and restart the metadata server.
- A store with an unknown RPC outcome retains its allocation until the same
  operation is resolved/retried. It cannot overwrite a potentially visible KV.
- M1 bounds session/operation ledgers. Admission stops if a ledger limit is
  reached; it does not discard replay protection to keep accepting stores.

Observe CPU pools with:

```bash
marutop usage --host 127.0.0.1 -p 5555
```

The CPU table shows capacity, page allocations, logical KV bytes and allocations
with uncertain commit outcomes. `RpcClient.get_usage().cpu_storage` additionally
includes replica/read-lease counts and cumulative `acquired_objects`. Live
uncommitted allocation totals are heartbeat snapshots; committed bytes are
updated on commit. The existing CSV export and unified CXL dashboard retain
their CXL schema in M1; use the CPU table or RPC for CPU metrics.

## Handler usage

CPU reads carry an explicit lease. Release it after the last read/GPU copy, or
use a context manager for synchronous access:

```python
from maru import MaruConfig, MaruHandler

config = MaruConfig(
    storage_backend="cpu",
    engine_id="python-example",
    cache_namespace="opaque-bytes-v1",
    pool_size=1024 * 1024,
    chunk_size_bytes=4096,
)
with MaruHandler(config) as handler:
    allocation = handler.alloc(4)
    allocation.buf[:] = b"data"
    assert handler.store("key", allocation)
    with handler.retrieve("key") as result:
        assert bytes(result.view) == b"data"
```

`store`/`batch_store` take ownership of submitted allocations, including uncertain
commits; do not write through their buffers afterward. Release exported buffers
before closing the handler. Legacy `pin`, `unpin` and `delete` are unsupported for
the M1 CPU backend; reads use leases and committed pages stay resident until close.

## Validation

```bash
PYTHONPATH="$PWD" python -m pytest -q \
  tests/unit/test_cpu_storage.py tests/unit/test_vllm_cpu_config.py \
  tests/integration/test_cpu_storage.py
```

The integration tests start a real CPU-only metadata server process against an
unreachable RM address and exercise both RPC clients, requester isolation,
capacity limits, GPU/CPU byte round trips and a hit lost before load. The CUDA
case skips on machines without a GPU. No model download is needed for these tests.

See the [tiered cache design](../../../docs/source/design_doc/tiered_kv_cache.md)
for the CPU/CXL placement and sharing milestones.
