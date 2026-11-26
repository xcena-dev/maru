(vllm-model-support)=

# Supported Models

Whether MaruKVConnector can store and reuse a model's KV cache in vLLM depends on the structure of the KV cache that vLLM builds for the model. This page lists the structures it stores and the ones it refuses; a structure or setting that is not listed is outside this support description.

The structure table describes the native connector; node store restrictions are listed below:

```{table}
:class: maru-supported-models

| KV cache structure | Models | Single GPU | Tensor parallel |
|---|---|---|---|
| Full attention (MHA, GQA), one KV cache group | Llama 3.1, Qwen3 (dense and MoE) | 🟢 | 🟢 |
| Sliding window + full attention | gpt-oss | 🟢 | 🟢 |
| Gated DeltaNet (Mamba-type) + full attention | Qwen3.5, Qwen3.6, Qwen3.8 (except Flash-Next), Qwen3-Next | 🟢 | 🟢 |
| Sliding-window MLA + compressed MLA latent and indexer cache | DeepSeek-V4.1-Flash | 🟢 | 🟢 |
| Attention groups whose KV heads differ in count or size | Gemma 4, MiMo-V2 | ❌ | ❌ |
| DSA: MLA latent + indexer cache | DeepSeek-V3.2, GLM-5 | ❌ | ❌ |
| Kimi Delta Attention (Mamba-type) + DSA | GLM-5.3-Flash | ❌ | ❌ |
| Chunked local + full attention | Llama 4 | ❌ | ❌ |
```

🟢 means that Maru stores and loads the KV cache of such models, and ❌ that the connector refuses them at startup. Each model family in a 🟢 row has been run with its own weights while Maru stored and loaded its KV cache. Models with several KV cache groups also need the settings in {ref}`vllm-hybrid-models`; DeepSeek-V4.1-Flash has notes of its own in {ref}`vllm-deepseek-v41`.

Pipeline parallelism and decode or prefill context parallelism are refused at startup. See {ref}`vllm-tensor-parallelism` for how each rank stores its part of the KV cache.

## Storage backends

`cxl` and node-local `dram` and `local_cxl` use the connector's mapped-pool transfer modes. Remote nodes use worker-owned registered staging and direct RDMA, and accept chunkwise storage only: layerwise format and overlapped loads are rejected. The node namespace includes model and KV geometry; different TP sizes do not share node keys.

TP and multiple KV groups require async loads. Requests containing multimodal inputs, LoRA adapters, cache salts or prompt embeddings are computed without Maru caching. These requests can still run in the engine. See {ref}`vllm-hybrid-models` for group and MLA format restrictions and {doc}`../getting_started/migrating_to_maru_node` for the store support matrix.

## Reporting results

If you run a model that this page does not list, please [open an issue](https://github.com/xcena-dev/maru/issues) with the model, the versions of vLLM and Maru, the parallel settings and the result.
