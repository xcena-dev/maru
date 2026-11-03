# Supported Models

Whether Maru can store and reuse a model's KV cache depends on the integration
and on the structure of the KV cache that the inference engine builds for the
model. This page lists, for the vLLM, LMCache and SGLang integrations, the
structures Maru stores, the models they were tested with, and the structures
it refuses. A structure or setting that is not listed here has not been tested.

(vllm-model-support)=
## vLLM

MaruKVConnector stores the KV cache for vLLM. On the CXL storage backend (the
default):

```{eval-rst}
.. table::
   :class: maru-supported-models
   :widths: auto

   +--------------------------------+------------------------------------+-----------------------------------+----------------+
   | KV cache structure             | Models                             | Supported status                  | Tested with    |
   |                                |                                    +-----------------+-----------------+                |
   |                                |                                    | Single GPU      | Tensor parallel |                |
   +================================+====================================+=================+=================+================+
   | Full attention (MHA, GQA), one | Qwen3 (dense and MoE), GLM-4.5,    | 🟢              | 🟢              | Llama-3.1-8B   |
   | KV cache group                 | GLM-4.6                            |                 |                 |                |
   +--------------------------------+------------------------------------+-----------------+-----------------+----------------+
   | Sliding window + full          | gpt-oss                            | 🟢 [1]_         | 🟢              | gpt-oss-20b    |
   | attention                      |                                    |                 |                 |                |
   +--------------------------------+------------------------------------+-----------------+-----------------+----------------+
   | Gated DeltaNet (Mamba-type) +  | Qwen3.5, Qwen3.6, Qwen3.8,         | 🟢 [2]_         | 🟢 [3]_         | Qwen3.5-0.8B   |
   | full attention                 | Qwen3-Next                         |                 |                 |                |
   +--------------------------------+------------------------------------+-----------------+-----------------+----------------+
   | Attention groups whose KV      | Gemma 4, MiMo-V2, Inkling          | ❌              | ❌              |                |
   | heads differ in count or size  |                                    |                 |                 |                |
   +--------------------------------+------------------------------------+-----------------+-----------------+----------------+
   | DSA: MLA latent + indexer      | DeepSeek-V3.2, GLM-5               | ❌              | ❌              |                |
   | cache                          |                                    |                 |                 |                |
   +--------------------------------+------------------------------------+-----------------+-----------------+----------------+
   | MLA with other kinds of group  | GLM-5.3-Flash, DeepSeek-V4.1-Flash | ❌              | ❌              |                |
   +--------------------------------+------------------------------------+-----------------+-----------------+----------------+
   | Chunked local + full attention | Llama 4                            | ❌ [4]_         | ❌              |                |
   +--------------------------------+------------------------------------+-----------------+-----------------+----------------+

.. [1] As several KV cache groups on vLLM 0.30 and later, or as one group
   (see :ref:`vllm-hybrid-one-group`). gpt-oss-20b was tested both ways.
.. [2] vLLM 0.30 and later. On vLLM 0.32.1rc0 and later, start vLLM with
   ``--gdn-prefill-backend triton`` (see :ref:`vllm-gdn-prefill-backend`).
.. [3] An engine hits only what an engine of the same TP size stored.
.. [4] Refused at startup as several KV cache groups. It can run as one
   group (see :ref:`vllm-hybrid-one-group`), which has not been tested.

```

🟢 means that Maru stores and loads the KV cache of such models, and ❌ that
the connector refuses them at startup (see
{ref}`Startup checks <supported-models-startup-checks>`).

GPU tests ran on vLLM 0.30 and 0.32 with the CXL backend, with each model's
released weights. For the models in "Tested with", every KV block loaded from
Maru matched, byte for byte, what the engine had computed, or the greedy output
matched a run without Maru. The other models in a row share its KV cache
structure but have not been run.

Pipeline parallelism and decode or prefill context parallelism are refused at
startup. See {ref}`vllm-tensor-parallelism` for how each rank stores its part
of the KV cache.

(supported-models-startup-checks)=
### Startup checks

The connector checks the KV cache groups that vLLM builds for a model, and the
KV shapes of its attention groups, when the engine starts. It refuses a
structure it cannot store and names what it found:

- **Attention groups whose KV heads differ in count or size**: vLLM gives such
  groups different block sizes, while the connector stores every group with one
  block size and one KV shape.
- **DSA**: vLLM 0.32 keeps the MLA latent and the indexer cache, which differ in
  shape, in one group, while the connector stores every layer of a one-group
  model in the first layer's shape.
- **MLA or chunked local attention next to other groups**: the connector stores
  several groups only for full attention, sliding window and Mamba-type layers.

A model that passes these checks can still be refused when the worker registers
its KV cache, if the tensors do not match the groups' specs. Models with several
KV cache groups also need the settings listed under {ref}`vllm-hybrid-models`.

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
