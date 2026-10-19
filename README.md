<p align="center">
  <img src="docs/source/image/maru.png" alt="Maru" height="120">
</p>

<h3 align="center">Maru: High-Performance KV Cache Storage Engine on CXL Shared Memory</h3>

<p align="center">
  <a href="https://xcena-dev.github.io/maru/"><img src="https://img.shields.io/badge/docs-live-blue" alt="Documentation"></a>
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache_2.0-green.svg" alt="License"></a>
</p>

**Maru** is a high-performance **KV cache storage engine built on CXL shared memory**, designed for LLM inference scenarios where multiple instances need to share a KV cache with minimal latency.

Transfer-based sharing copies the KV across the network to every node that reuses it, byte by byte. As models get larger and contexts get longer, those copies become a structural bottleneck. On nodes attached to CXL, Maru takes the opposite approach: **don't move data, share the memory.** Instances on those nodes read and write KV cache data directly in CXL shared memory, and only lightweight metadata (tens of bytes) travels between components. GPU nodes without CXL reach the CXL memory of memory nodes with one-sided RDMA: the NICs move the bytes, and no process on the memory node handles them.

Node A stores the KV it computes in a shared KV cache, and nodes B and C load it instead of recomputing. Nodes attached to CXL share a CXL memory pool through a CXL switch; GPU nodes without CXL share the CXL memory of memory nodes over an RDMA network with the `remote_cxl` backend. A node shares KV with the other nodes that use the same pool and backend.

<p align="center">
  <img src="docs/source/image/maru_shared_kv_cache.png" alt="Node A stores the KV of request 1 in a shared KV cache, and nodes B and C load that KV for requests 2 and 3. The shared KV cache holds a CXL memory pool, CXL memory behind a CXL switch, and an RDMA memory pool, CXL memory in memory pool nodes reached over an RDMA network">
</p>

| [**Documentation**](https://xcena-dev.github.io/maru/) |

## Why Maru?

- **Zero-Copy Sharing** — Transfer-based systems — whether CPU-mediated or GPU-direct — require the receiver to allocate staging buffers and move data across an interconnect. On nodes attached to CXL, Maru eliminates this entire path: every instance reads from the same shared memory region directly. No buffer allocation, no data copy, no serialization. Nodes that reach the pool over RDMA move KV through a host staging buffer: the GPU copies it to and from the buffer, and the NIC moves it to and from the CXL memory with one-sided RDMA.

- **Scales with Context Length and Concurrency** — Network-based sharing degrades as contexts grow and more consumers hit the same KV. On nodes attached to CXL, Maru never fans out KV payloads — scaling is bounded by shared-memory bandwidth, not network transfer.

- **Higher Hardware Utilization** — Instead of duplicating KV caches per instance, all instances draw from a shared CXL pool, whether they reach it over CXL or over RDMA. Less duplication means more usable memory and higher effective cache capacity.

- **Lower System Energy** — On nodes attached to CXL, eliminating bulk data transfer cuts NIC and CPU power draw, and shorter data paths reduce GPU idle time per request.

- **GPU Nodes without CXL** — With the `remote_cxl` storage backend, vLLM workers on GPU nodes without CXL keep their KV in the CXL memory of memory nodes. A remote CXL agent on each memory node maps regions and registers them with the node's NIC, and workers read and write them with one-sided RDMA. Several memory nodes can share one CXL pool, and a worker can use several pools. See the [remote CXL example](examples/vllm/remote_cxl/README.md).

## Overview
```mermaid
flowchart TB
    subgraph S1["Server 1"]
        direction TB
        I1(["LLM Instance"])
        H1{{"MaruHandler"}}
        I1 --- H1
    end
    subgraph S2["Server 2"]
        direction TB
        I2(["LLM Instance"])
        H2{{"MaruHandler"}}
        I2 --- H2
    end
    subgraph S3["Server 3"]
        direction TB
        I3(["LLM Instance"])
        H3{{"MaruHandler"}}
        I3 --- H3
    end

    M["Maru Control Plane"]

    subgraph CXL["CXL Shared Memory"]
        KV["KV Cache"]
    end

    H1 & H2 & H3 <-.->|"store/retrieve"| M
    H1 & H2 & H3 <==>|"direct read/write"| CXL
    M -.->|"manage"| CXL
```

> **Control Plane** (dashed arrows) — KV metadata operations and region allocation.
>
> **Data Plane** (solid arrows) — on nodes attached to CXL, direct access to CXL shared memory, zero-copy. The data path is identical regardless of control plane mode.

GPU nodes without CXL use the `remote_cxl` storage backend. Their KV stays in the CXL memory of memory nodes: a remote CXL agent on each memory node maps the regions a worker asks for and registers them with the node's NIC, and the worker reads and writes them with one-sided RDMA (NIXL/UCX). Only control messages go to maru-server and the remote CXL agent. See [Remote CXL](https://xcena-dev.github.io/maru/source/design_doc/architecture_overview.html#remote-cxl-cxl-memory-of-other-nodes-over-rdma) in the architecture overview.

## Quick Start

### System Components

Maru consists of two server components and a client library. A deployment with GPU nodes without CXL adds a remote CXL agent on each memory node:

| Component | Role | Package |
|-----------|------|---------|
| **Resource Manager** | Manages the CXL memory pool | `maru-resource-manager` (C++) |
| **Metadata Server** | Manages KV metadata | `maru-server` (Python) |
| **MaruHandler** | Client library embedded in LLM instances | `maru` Python package |
| **Remote CXL Agent** | On memory nodes: maps CXL regions for LLM instances on nodes without CXL and registers them with the NIC (`remote_cxl` backend) | `maru-remote-cxl-agent` (needs NIXL) |

All components run on the same machine in a single-node setup. For multi-node deployment, see the [installation guide](https://xcena-dev.github.io/maru/source/getting_started/installation.html#multi-node-configuration).


### Prerequisites

- OS: Ubuntu 24.04 LTS+
- Python: 3.12+
- gcc: 13.3.0+, cmake: 3.28.3+
- [uv](https://docs.astral.sh/uv/) (recommended; `install.sh` falls back to pip)
- CXL DAX device (`/dev/dax*`) or emulation environment, on the nodes that hold the pool and the nodes that read it over CXL
- For the `remote_cxl` backend:
  - An RDMA NIC (RoCE or InfiniBand) on every worker and memory node; the maru-server node must reach the remote CXL agents on the memory nodes' RDMA network addresses
  - NIXL in the same environment, before `./install.sh`: `uv pip install nixl` (`pip install nixl` without uv)

```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip git \
    build-essential cmake libnuma-dev

# uv — used for all Python installs below
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Installation

```bash
git clone https://github.com/xcena-dev/maru
cd maru

uv venv --python 3.12 .venv
source .venv/bin/activate
./install.sh
```

### 1. Start the Resource Manager

The resource manager must be running before any other Maru service.

**Production** — start as a systemd daemon:

```bash
# Default (127.0.0.1:9850)
sudo systemctl start maru-resource-manager

# With custom host/port
sudo systemctl edit maru-resource-manager

[Service]
ExecStart=
ExecStart=/usr/local/bin/maru-resource-manager --host <ip> --port <port>

sudo systemctl restart maru-resource-manager
```

**Development/debugging** — run directly with custom options:

```bash
# Default (127.0.0.1:9850)
sudo maru-resource-manager --log-level debug

# With custom host/port
sudo maru-resource-manager --host <ip> --port <port> --log-level debug
```

> If you change the RM port, pass the same address to `maru-server`: `maru-server --rm-address 127.0.0.1:9851`

### 2. Start the Metadata Server

```bash
# Default (127.0.0.1:5555, connects to resource manager at 127.0.0.1:9850)
maru-server

# With custom host/port
maru-server --host <ip> --port <port>

# With custom resource manager address (default: 127.0.0.1:9850)
maru-server --rm-address <rm-ip>:<rm-port>
```

### Basic Usage

```python
from maru import MaruConfig, MaruHandler

config = MaruConfig(
    server_url="tcp://localhost:5555",
    pool_size=1024 * 1024 * 100,  # 100MB
)

with MaruHandler(config) as handler:
    data = b"A" * (1024 * 1024)  # 1MB KV chunk

    # 1. Allocate a page in CXL shared memory
    handle = handler.alloc(size=len(data))

    # 2. Write directly to CXL memory (mmap — no intermediate buffer)
    handle.buf[:] = data

    # 3. Register the key — only metadata (key → region, offset) is sent
    handler.store(key=42, handle=handle)

    # Retrieve: returns a memoryview pointing into CXL memory
    result = handler.retrieve(key=42)
    assert result is not None
    assert bytes(result.view[:5]) == b"AAAAA"
```

## LMCache Integration

Maru works as a native [LMCache](https://github.com/LMCache/LMCache) storage backend via the `maru_path` and `maru_pool_size` config fields. Requires LMCache [`5502419`](https://github.com/LMCache/LMCache/commit/550241961be6edae295cd0a374db1e46f96fe388) (2026-04-03) or later. It supports both **P2P KV cache sharing** and **disaggregated prefill** scenarios.

```yaml
# LMCache config
maru_path: "maru://localhost:${MARU_SERVER_PORT}"
maru_pool_size: 4
```

For details on LMCache integration, see the [documentation](https://xcena-dev.github.io/maru/source/integration/lmcache.html).

LMCache integration runs on nodes attached to the CXL memory; GPU nodes without CXL use the vLLM connector with `maru_storage_backend: "remote_cxl"`.

## vLLM Integration

`MaruKVConnector` is a KV connector that plugs Maru into vLLM directly, without LMCache. vLLM loads it from the `maru_vllm` package, and its settings go in `kv_connector_extra_config`. Requires vLLM v0.14 or later.

```bash
vllm serve <model> --kv-transfer-config '{
    "kv_connector": "MaruKVConnector",
    "kv_connector_module_path": "maru_vllm",
    "kv_role": "kv_both",
    "kv_connector_extra_config": {"maru_server_url": "tcp://localhost:5555", "maru_pool_size": "4G"}
}'
```

On GPU nodes without CXL, set `"maru_storage_backend": "remote_cxl"` to keep the KV in the CXL memory of memory nodes; the [remote CXL example](examples/vllm/remote_cxl/README.md) lists the settings it needs. For details, see the [vLLM guide](https://xcena-dev.github.io/maru/source/integration/vllm.html).


## Plugins

Maru core is vendor-neutral. Hardware- or vendor-specific behaviour lives in
separate packages that register a `maru.handler_plugins` entry point;
`MaruHandler` discovers them at construction time and calls their hooks —
loading is soft-fail, so a missing or broken plugin never breaks KV-cache
operation. Select plugins at runtime with `MARU_PLUGINS` (unset → load all).

```python
# In the plugin package's pyproject.toml
[project.entry-points."maru.handler_plugins"]
my_plugin = "my_pkg.plugin:MyPlugin"
```

A plugin implements any subset of `MaruHandlerPlugin` (`on_init`,
`on_batch_retrieve`, `on_close`, `contribute_stats`). See
[`maru_handler/plugin.py`](maru_handler/plugin.py) for the interface.

## Tools

`marutop` — a single command (installed with the package) for monitoring and
device admin. With no arguments it shows a unified `top`-style view combining
physical DAX pools and per-instance usage; subcommands select a focused view or
the device admin tool:

```bash
marutop                    # unified live view: DEVICES (pool) + INSTANCES
marutop pool               # physical DAX pool gauges (CSV export with --csv)
marutop usage              # per-instance allocated/used/slack
marutop stats              # per-operation latency/throughput dashboard
marutop device init /dev/dax0.0   # DAX device UUID header: init | show | clear
```

See [`tools/README.md`](tools/README.md) for full options. (The older
`python -m tools.pool_monitor` invocations still work via thin shims.)

## License

Copyright 2026 [XCENA Inc.](https://xcena.com) Licensed under the Apache License 2.0.
