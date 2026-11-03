# Supported Models

Whether Maru can store and reuse a model's KV cache depends on the integration
and on the structure of the KV cache that the inference engine builds for the
model. This page lists, for the vLLM, LMCache and SGLang integrations, the
structures Maru stores, the models they were tested with, and the structures
it refuses. A structure or setting that is not listed here has not been tested.

(vllm-model-support)=
## vLLM

MaruKVConnector stores the KV cache for vLLM. On the CXL storage backend (the
default), at any tensor parallel size:

| KV cache structure | Models | Status | Tested with |
|---|---|---|---|
| Full attention (MHA, GQA), one KV cache group | Qwen3 (dense and MoE), GLM-4.5, GLM-4.6 | Supported | Llama-3.1-8B, TP 1 and 2 |
| Sliding window + full attention | gpt-oss | Supported on vLLM 0.30+: as several KV cache groups, or as one group (see {ref}`vllm-hybrid-one-group`) | gpt-oss-20b, TP 1 and 2, both ways |
| Gated DeltaNet (Mamba-type) + full attention | Qwen3.5, Qwen3.6, Qwen3.8, Qwen3-Next | Supported on vLLM 0.30+; on vLLM 0.32.1rc0 and later, start vLLM with `--gdn-prefill-backend triton` (see {ref}`vllm-gdn-prefill-backend`) | Qwen3.5-0.8B, TP 1 and 2 |
| Attention groups whose KV heads differ in count or size | Gemma 4, MiMo-V2, Inkling | Refused at startup: vLLM gives such groups different block sizes, while the connector stores every group with one block size and one KV shape | |
| DSA: MLA latent + indexer cache | DeepSeek-V3.2, GLM-5 | Refused at startup: vLLM 0.32 keeps the latent and the indexer cache, which differ in shape, in one group, while the connector stores every layer of a one-group model in the first layer's shape | |
| MLA with other kinds of group | GLM-5.3-Flash, DeepSeek-V4.1-Flash | Refused at startup: the connector stores several groups only for full attention, sliding window and Mamba-type layers | |
| Chunked local + full attention | Llama 4 | Refused at startup as several KV cache groups. It can run as one group (see {ref}`vllm-hybrid-one-group`), which has not been tested | |

GPU tests ran on vLLM 0.30 and 0.32 with the CXL backend, with each model's
released weights. For the models in "Tested with", every KV block loaded from
Maru matched, byte for byte, what the engine had computed, or the greedy output
matched a run without Maru. The other models in a row share its KV cache
structure but have not been run.

### Startup checks

The connector checks the KV cache groups that vLLM builds for a model, and the
KV shapes of its attention groups, when the engine starts, and refuses a
structure it cannot store with what it found. A model that passes these checks
can still be refused when the worker registers its KV cache, if the tensors do
not match the groups' specs. Models with several KV cache groups also need the
settings listed under {ref}`vllm-hybrid-models`.

### Storage backends

The table holds for the `cxl` backend. The `cpu`, `mixed` and `remote_cxl`
backends take only text-only models with one KV cache group and without MLA,
at TP=1 and without data parallelism, and refuse LoRA and quantized KV caches.

### Requests Maru does not cache

Requests with images or other multimodal inputs, a LoRA adapter, a `cache_salt`
or prompt embeddings neither load from nor store to Maru; see
{ref}`vllm-uncached-requests`.

## LMCache

With the LMCache integration, Maru is LMCache's remote storage (`maru://`), and
LMCache's connector stores the KV cache for vLLM, so model support on this path
follows LMCache. Models with several KV cache groups, such as the hybrid models
above, are not supported on this path.

## SGLang HiCache

Maru is the L3 storage of SGLang's HiCache and stores the KV cache of
full-attention (MHA, GQA) and MLA models. Under tensor parallelism each rank
keys its KV with its rank and the TP size, following SGLang's convention, and
the ranks of an MLA model share one copy of the latent.

## Reporting results

If you run a model that this page does not list, please
[open an issue](https://github.com/xcena-dev/maru/issues) with the model, the
versions of the inference engine and Maru, the parallel settings and the
result.
