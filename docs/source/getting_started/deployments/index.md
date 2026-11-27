# Deployments

Each deployment uses one backend, `node` or `cxl`.

Run one MaruServer per deployment.

![Three setups, each with one MaruServer (maru-server) as the Catalog. Shared CXL: two nodes attach to one CXL memory through a CXL switch, the vLLM engine on each node maps the shared pool directly, and one Resource Manager manages the pool; MaruServer runs as maru-server --backend cxl. Node Store on one GPU node: two vLLM engines map and copy to one DRAM or local CXL pool that maru-storage owns and attach to it over a Unix socket; a local CXL pool also needs a Resource Manager on that node; MaruServer runs as maru-server --backend node. Node Store with a memory node: the vLLM engine on each GPU node reads and writes the memory node's DRAM or CXL pool over RDMA, maru-storage on each GPU node exchanges control messages with maru-storage --role memory, and a CXL pool needs a Resource Manager on the memory node; MaruServer runs as maru-server --backend node.](../../image/quick_start_setups.png)

| Your setup | Start here |
|---|---|
| vLLM engines on one GPU node sharing its DRAM or local CXL | {doc}`Quick Start <../quick_start>` (`node`) |
| Nodes attached to the same CXL memory, or CXL-backed LMCache or SGLang | {doc}`Shared CXL <shared_cxl>` (`cxl`) |
| vLLM engines on several GPU nodes sharing a memory node's DRAM or CXL pool over RDMA | {doc}`Memory Node over RDMA <memory_node>` (`node`, `remote` store) |

`maru-server` without `--backend` runs the `cxl` backend. Complete {doc}`../installation` before starting the services.

```{toctree}
:hidden:

Shared CXL <shared_cxl>
Memory Node over RDMA <memory_node>
```
