# Examples

The {doc}`../quick_start` shows how two vLLM engines share KV cache on one GPU node. See {doc}`../deployments/index` for Shared CXL and Memory Node over RDMA setups. The examples below cover vLLM, LMCache and Dynamo.

On an idle receiving engine, compare the counters before and after its request. An increase in `vllm:external_prefix_cache_hits_total` shows a lookup hit. Check `vllm:prompt_tokens_by_source_total{source="external_kv_transfer"}` and the worker load outcome to confirm reuse. Compare the generated output under equivalent decoding and cached-token boundaries. Measure TTFT separately.

```{toctree}
:maxdepth: 2

vllm/index
lmcache/index
dynamo/index
```
