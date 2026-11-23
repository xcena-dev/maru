# Examples

Start with {doc}`path selection in Quick Start <../quick_start>`. Its Python producer/consumer uses directly mapped CXL. For node DRAM, local CXL and remote stores, use {doc}`the native vLLM service setup <../../integration/vllm>`. The examples below cover vLLM, LMCache and Dynamo.

For a functional check, verify the worker's completed external restore and the generated output under equivalent decoding and cached-token boundaries. An HTTP success or scheduler cache-hit count alone can include fallback recomputation. Check failure/recompute logs as well. Record TTFT separately; there is no fixed speedup that proves correctness on every model or prompt.

```{toctree}
:maxdepth: 2

vllm/index
lmcache/index
dynamo/index
```
