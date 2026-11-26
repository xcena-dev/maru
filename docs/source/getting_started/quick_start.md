# Quick Start

Maru has two backends, and a deployment uses one of them. Find your setup and start on its page.

![Three setups, each with one MaruServer (maru-server) as the Catalog. Shared CXL: two nodes attach to one CXL memory through a CXL switch, the vLLM engine on each node maps the shared pool directly, and one Resource Manager manages the pool; MaruServer runs as maru-server --backend cxl. Node Store on one GPU node: two vLLM engines map and copy to one DRAM or local CXL pool that maru-storage owns and attach to it over a Unix socket; a local CXL pool also needs a Resource Manager on that node; MaruServer runs as maru-server --backend node. Node Store with a memory node: the vLLM engine on each GPU node reads and writes the memory node's DRAM or CXL pool over RDMA, maru-storage on each GPU node exchanges control messages with maru-storage --role memory, and a CXL pool needs a Resource Manager on the memory node; MaruServer runs as maru-server --backend node.](../image/quick_start_setups.png)

| Your setup | Start here |
|---|---|
| Nodes attached to the same CXL memory, or LMCache and SGLang | {doc}`Shared CXL <quick_start_cxl>` (`cxl`) |
| vLLM engines on one GPU node sharing its DRAM or local CXL | {ref}`Node Store: one GPU node <quickstart-node-local>` |
| vLLM engines on GPU nodes without CXL sharing a memory node over RDMA | {ref}`Node Store: memory node <quickstart-node-remote>` |

`maru-server` without `--backend` runs the `cxl` backend, so existing `cxl` setups need no change. Complete {doc}`installation` first.

```{toctree}
:hidden:

Shared CXL <quick_start_cxl>
Node Store <quick_start_node>
```
