# Examples

Start with {doc}`backend selection in Quick Start <../quick_start>`. {doc}`../quick_start_cxl` uses a Python producer and consumer on directly mapped CXL; {doc}`../quick_start_node` runs vLLM engines on node DRAM, local CXL and remote stores. The examples below cover vLLM, LMCache and Dynamo.

On an idle receiving engine, compare the counters before and after its request. An increase in `vllm:external_prefix_cache_hits_total` shows a lookup hit. Check `vllm:prompt_tokens_by_source_total{source="external_kv_transfer"}` and the worker load outcome to confirm reuse. Compare the generated output under equivalent decoding and cached-token boundaries. Measure TTFT separately.

```{toctree}
:maxdepth: 2

vllm/index
lmcache/index
dynamo/index
```
