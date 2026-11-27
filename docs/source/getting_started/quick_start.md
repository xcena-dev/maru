(quickstart-node-local)=

# Quick Start

Two vLLM engines on one GPU node share KV cache through its DRAM store. For shared CXL memory, LMCache or SGLang, see {doc}`Shared CXL <deployments/shared_cxl>`. For several GPU nodes that share a memory node's DRAM or CXL pool over RDMA, see {doc}`Memory Node over RDMA <deployments/memory_node>`.

## Prerequisites

Use a node with at least one NVIDIA GPU. Install vLLM 0.16 or later for deferred loading. See the {ref}`vLLM prerequisites <vllm-prerequisites>`. Then follow {doc}`installation` and activate the Maru environment in each terminal. The engines use `Qwen/Qwen3-0.6B`. For another {doc}`supported model <../integration/supported_models>`, its largest stored KV object must fit in `page_size`.

Create `/run/maru` for the GPU StorageService's node lock and worker socket.

```bash
sudo install -d -o "$USER" -g "$(id -gn)" /run/maru
```

## 1. Start the services

Start MaruServer in its own terminal. It tracks keys for the node backend.

```bash
maru-server --backend node --host 127.0.0.1 --port 5556
```

The GPU StorageService owns the DRAM store. Write its configuration to `gpu.json`.

```json
{
  "node_id": "gpu-1",
  "server": "tcp://127.0.0.1:5556",
  "page_size": "32MiB",
  "tiers": [{"store": "dram", "size": "16GiB", "hugepage": false}]
}
```

Check the configuration, then start the GPU StorageService in another terminal. The GPU role is the default for `maru-storage`.

```bash
maru-storage --role gpu --config gpu.json --check
maru-storage --role gpu --config gpu.json
```

(quickstart-node-clients)=

## 2. Check without a GPU (optional)

Run two Python processes on this node. Each connects to the GPU StorageService socket with its own engine ID. Both use the same cache namespace.

Save the producer as `producer.py`. It stores 1 MiB under one key.

```python
import sys

from maru import MaruConfig, MaruHandler

config = MaruConfig(
    maru_node=sys.argv[1],
    engine_id="producer",
    cache_namespace="quickstart",
    pin_pools=False,
)
with MaruHandler(config) as handler:
    data = b"A" * (1024 * 1024)
    handle = handler.alloc(len(data))
    handle.buf[:] = data
    if not handler.store("quickstart-demo", handle):
        raise RuntimeError("Could not store quickstart-demo")
```

Save the consumer as `consumer.py`. It reads the same key and releases the read when it leaves the `with result` block.

```python
import sys

from maru import MaruConfig, MaruHandler

config = MaruConfig(
    maru_node=sys.argv[1],
    engine_id="consumer",
    cache_namespace="quickstart",
    pin_pools=False,
)
with MaruHandler(config) as handler:
    result = handler.retrieve("quickstart-demo")
    if result is None:
        raise RuntimeError("Run the producer first")
    with result:
        if result.view != b"A" * (1024 * 1024):
            raise ValueError("The stored bytes differ")
        print("Success: read 1 MiB from the producer")
```

Run the producer first. Once it exits, the GPU StorageService still owns the stored data, so the consumer can read it.

```bash
CUDA_VISIBLE_DEVICES="" python3 producer.py /run/maru/node.sock
CUDA_VISIBLE_DEVICES="" python3 consumer.py /run/maru/node.sock
```

The consumer should print `Success: read 1 MiB from the producer`.

## 3. Start two vLLM engines

Start two engines in separate terminals. They connect to the default socket `/run/maru/node.sock`. Give each engine a distinct `maru_engine_id`. Pin the same immutable revision in both engine commands and include it in their shared namespace so a cache key identifies the same weights on both engines.

```bash
CUDA_VISIBLE_DEVICES=0 vllm serve Qwen/Qwen3-0.6B --revision c1899de289a04d12100db370d81485cdf75e47ca --port 8000 --kv-transfer-config '{
  "kv_connector": "MaruKVConnector",
  "kv_connector_module_path": "maru_vllm",
  "kv_role": "kv_both",
  "kv_load_failure_policy": "recompute",
  "kv_connector_extra_config": {
    "maru_node": "/run/maru/node.sock",
    "maru_engine_id": "engine-1",
    "maru_cache_namespace": "Qwen/Qwen3-0.6B@c1899de289a04d12100db370d81485cdf75e47ca",
    "maru_async_load": true
  }
}'
```

```bash
CUDA_VISIBLE_DEVICES=1 vllm serve Qwen/Qwen3-0.6B --revision c1899de289a04d12100db370d81485cdf75e47ca --port 8001 --kv-transfer-config '{
  "kv_connector": "MaruKVConnector",
  "kv_connector_module_path": "maru_vllm",
  "kv_role": "kv_both",
  "kv_load_failure_policy": "recompute",
  "kv_connector_extra_config": {
    "maru_node": "/run/maru/node.sock",
    "maru_engine_id": "engine-2",
    "maru_cache_namespace": "Qwen/Qwen3-0.6B@c1899de289a04d12100db370d81485cdf75e47ca",
    "maru_async_load": true
  }
}'
```

With one GPU, start both engines on GPU 0 and add `--gpu-memory-utilization 0.4` to each.

(quickstart-node-verify)=

## 4. Verify sharing

Save this client as `send.py`. It sends the same prompt to each engine.

```python
import json
import sys
import urllib.request

prompt = " ".join(
    f"Line {i}: Maru shares KV cache between engines." for i in range(200)
)
body = {
    "model": "Qwen/Qwen3-0.6B",
    "prompt": prompt,
    "max_tokens": 16,
    "temperature": 0,
}
request = urllib.request.Request(
    f"{sys.argv[1]}/v1/completions",
    data=json.dumps(body).encode(),
    headers={"Content-Type": "application/json"},
)
with urllib.request.urlopen(request) as response:
    print(json.load(response)["choices"][0]["text"])
```

From a client terminal, check that both engines are ready.

```bash
engine_a=http://127.0.0.1:8000
engine_b=http://127.0.0.1:8001
curl -fsS "$engine_a/health" && curl -fsS "$engine_b/health"
```

Once both health checks succeed, send the same prompt to each engine. Keep engine B idle between samples so the counter changes belong to this request.

```bash
metrics() {
    curl -fsS "$engine_b/metrics" | grep -E '^vllm:(external_prefix_cache_hits_total|prompt_tokens_by_source_total[{][^}]*source="external_kv_transfer")'
}
python3 send.py "$engine_a"
metrics
python3 send.py "$engine_b"
metrics
```

A rise in `vllm:external_prefix_cache_hits_total` counts tokens matched by the lookup. Check that `vllm:prompt_tokens_by_source_total{source="external_kv_transfer"}` also increases and that the worker reports a completed load. With `kv_load_failure_policy` set to `recompute`, a cache miss or recoverable load failure triggers recomputation. Check the worker log for the reason. A CUDA error that leaves a copy unconfirmed stops the worker.

## 5. Stop

Press Ctrl+C in the engine terminals, then in the GPU StorageService terminal and finally in the MaruServer terminal.

## Local CXL

To use local CXL instead of DRAM, complete {doc}`bios_setup`, start the Resource Manager (`sudo systemctl start maru-resource-manager`) and use `{"store": "local_cxl", "size": "16GiB"}` as the tier.

## Next steps

- {doc}`deployments/index` — Choose a setup for your nodes
- {doc}`examples/index` — Run integration examples
- {doc}`../design_doc/node_storage_service` — Configuration keys, budgets, eviction and recovery
- {doc}`../integration/vllm` — Connector settings, tensor parallelism and hybrid models
