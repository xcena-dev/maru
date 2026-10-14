# NVIDIA Dynamo

Dynamo connects to Maru through its **vLLM backend**. Configure each
`dynamo.vllm` worker with the same `MaruKVConnector` and `--kv-transfer-config`
used for standalone vLLM. Dynamo routes requests to the workers, and the
connector handles KV cache sharing through Maru.

```mermaid
flowchart TB
    Client["OpenAI API client"] --> FE["Dynamo frontend"]
    subgraph W0["Dynamo worker A"]
        V0["vLLM"] --> C0["MaruKVConnector"]
    end
    subgraph W1["Dynamo worker B"]
        V1["vLLM"] --> C1["MaruKVConnector"]
    end
    FE --> V0 & V1
    C0 & C1 -.->|"metadata"| MS["MaruServer"]
    C0 & C1 <-->|"KV payload"| CXL[("CXL shared memory")]
```

See {doc}`vllm` for connector settings and the
{doc}`Dynamo example <../getting_started/examples/dynamo/index>` for setup and
execution instructions.
