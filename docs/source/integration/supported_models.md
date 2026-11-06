(vllm-model-support)=
# Supported Models

Whether MaruKVConnector can store and reuse a model's KV cache in vLLM depends
on the structure of the KV cache that vLLM builds for the model. This page
lists the structures it stores and the ones it refuses; a structure or setting
that is not listed has not been tested. Support for a range of models on
SGLang HiCache and NVIDIA Dynamo, as on vLLM, is planned.

On the `cxl` storage backend (the default) and the `remote_cxl` backend:

```{eval-rst}
.. table::
   :class: maru-supported-models
   :widths: auto

   +--------------------------------+------------------------------------+-----------------+-----------------+---------------------+
   | KV cache structure             | Models                             | Supported status                  | Tested with         |
   |                                |                                    +-----------------+-----------------+                     |
   |                                |                                    | Single GPU      | Tensor parallel |                     |
   +================================+====================================+=================+=================+=====================+
   | Full attention (MHA, GQA), one | Qwen3 (dense and MoE), GLM-4.5,    | 🟢              | 🟢              | Llama-3.1-8B        |
   | KV cache group                 | GLM-4.6                            |                 |                 |                     |
   +--------------------------------+------------------------------------+-----------------+-----------------+---------------------+
   | Sliding window + full          | gpt-oss                            | 🟢              | 🟢              | gpt-oss-20b         |
   | attention                      |                                    |                 |                 |                     |
   +--------------------------------+------------------------------------+-----------------+-----------------+---------------------+
   | Gated DeltaNet (Mamba-type) +  | Qwen3.5, Qwen3.6, Qwen3.8,         | 🟢              | 🟢              | Qwen3.5-0.8B        |
   | full attention                 | Qwen3-Next                         |                 |                 |                     |
   +--------------------------------+------------------------------------+-----------------+-----------------+---------------------+
   | Sliding-window MLA +           | DeepSeek-V4.1-Flash                | 🟢              | 🟢              | DeepSeek-V4.1-Flash |
   | compressed MLA latent and      |                                    |                 |                 |                     |
   | indexer cache                  |                                    |                 |                 |                     |
   +--------------------------------+------------------------------------+-----------------+-----------------+---------------------+
   | Attention groups whose KV      | Gemma 4, MiMo-V2                   | ❌              | ❌              |                     |
   | heads differ in count or size  |                                    |                 |                 |                     |
   +--------------------------------+------------------------------------+-----------------+-----------------+---------------------+
   | DSA: MLA latent + indexer      | DeepSeek-V3.2, GLM-5               | ❌              | ❌              |                     |
   | cache                          |                                    |                 |                 |                     |
   +--------------------------------+------------------------------------+-----------------+-----------------+---------------------+
   | Kimi Delta Attention           | GLM-5.3-Flash                      | ❌              | ❌              |                     |
   | (Mamba-type) + DSA             |                                    |                 |                 |                     |
   +--------------------------------+------------------------------------+-----------------+-----------------+---------------------+
   | Chunked local + full attention | Llama 4                            | ❌              | ❌              |                     |
   +--------------------------------+------------------------------------+-----------------+-----------------+---------------------+

```

🟢 means that Maru stores and loads the KV cache of such models, and ❌ that
the connector refuses them at startup. Models other than the one in "Tested
with" share their row's KV cache structure but have not been run. Models with
several KV cache groups also need the settings in {ref}`vllm-hybrid-models`;
DeepSeek-V4.1-Flash has notes of its own in {ref}`vllm-deepseek-v41`.

Pipeline parallelism and decode or prefill context parallelism are refused at
startup. See {ref}`vllm-tensor-parallelism` for how each rank stores its part
of the KV cache.

## Storage backends

The table holds for the `cxl` and `remote_cxl` backends, except that
`remote_cxl` takes no MLA model. `remote_cxl` was tested with gpt-oss-20b and
Qwen3.5-0.8B on a single GPU and at TP=2, with Llama-3.1-8B at TP=2, and with
all three at DP=2. The `cpu` and `mixed` backends take only text-only models
with one KV cache group and without MLA, at TP=1 and without data parallelism.
The `remote_cxl`, `cpu` and `mixed` backends refuse LoRA and quantized KV
caches.

## Reporting results

If you run a model that this page does not list, please
[open an issue](https://github.com/xcena-dev/maru/issues) with the model, the
versions of vLLM and Maru, the parallel settings and the result.
