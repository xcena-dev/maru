# Examples

Start with the {doc}`backend selection in Quick Start <../quick_start>`:
its Python producer/consumer use directly mapped CXL, while remote CXL,
CPU-only storage and mixed storage have their own prerequisites and examples.
The examples below cover vLLM, LMCache, and Dynamo serving configurations.

For a functional check, verify the worker's completed external restore and the
generated output under equivalent decoding and cached-token boundaries.
An HTTP success or scheduler cache-hit count alone can include fallback
recomputation. Check failure/recompute logs as well. Record TTFT separately;
there is no fixed speedup that proves correctness on every model or prompt.

```{toctree}
:maxdepth: 2

vllm/index
lmcache/index
dynamo/index
```
