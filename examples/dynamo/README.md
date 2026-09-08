# Maru × NVIDIA Dynamo Examples

Serving Maru's CXL-shared KV cache behind the [NVIDIA Dynamo](https://github.com/ai-dynamo/dynamo) serving stack: an OpenAI-compatible `dynamo.frontend` router in front of `dynamo.vllm` workers, each running `MaruKVConnector`.

- [`single_node/`](single_node/) — one node, one frontend + two workers sharing KV through one Maru CXL pool. Start here.
- `multi_node/` — planned (etcd/NATS discovery across hosts).
