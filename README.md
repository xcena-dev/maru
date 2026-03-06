<p align="center">
  <img src="docs/source/image/maru.png" alt="Maru" height="120">
</p>

<h3 align="center">Maru: High-Performance KV Cache Storage Engine on CXL Shared Memory</h3>

<p align="center">
  <a href="https://xcena-dev.github.io/maru/"><img src="https://img.shields.io/badge/docs-live-blue" alt="Documentation"></a>
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache_2.0-green.svg" alt="License"></a>
</p>

**Maru** is a high-performance **KV cache storage engine built on CXL shared memory**, designed for LLM inference scenarios where multiple instances need to share a KV cache with minimal latency.

Every existing KV cache sharing solution assumes that sharing means transferring — copying data across the network, byte by byte. As models get larger and contexts get longer, that assumption becomes a structural bottleneck. Maru rejects the premise entirely: **don't move data, share the memory.** Instances read and write KV cache data directly in CXL shared memory. Only lightweight metadata (tens of bytes) travels between components.

The left shows how KV cache is shared without Maru; the right shows how it works with Maru. No copies — just direct access to CXL shared memory.

<p align="center">
  <img src="docs/_static/kvcache_all.gif" alt="KV Cache Sharing: Without Maru (left) vs With Maru (right)" width="720">
</p>

| [**Documentation**](https://xcena-dev.github.io/maru/) |

## Why Maru?

- **Zero-Copy Sharing** — Transfer-based systems — whether CPU-mediated or GPU-direct — require the receiver to allocate staging buffers and move data across an interconnect. Maru eliminates this entire path: every instance reads from the same shared memory region directly. No buffer allocation, no data copy, no serialization.

- **Scales with Context Length and Concurrency** — Network-based sharing degrades as contexts grow and more consumers hit the same KV. Maru never fans out KV payloads — scaling is bounded by shared-memory bandwidth, not network transfer.

- **Higher Hardware Utilization** — Instead of duplicating KV caches per instance, all instances draw from a shared CXL pool. Less duplication means more usable memory and higher effective cache capacity.

- **Lower System Energy** — Eliminating bulk data transfer cuts NIC and CPU power draw. Shorter data paths also reduce GPU idle time per request.

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
> **Data Plane** (solid arrows) — direct access to CXL shared memory, zero-copy. The data path is identical regardless of control plane mode.

## Quick Start

### Prerequisites

- OS: Ubuntu 24.04 LTS+
- Python: 3.12+
- gcc: 13.3.0+, cmake: 3.28.3+
- CXL DAX device (`/dev/dax*`) or emulation environment

```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip git \
    build-essential cmake libnuma-dev
```

### Installation

```bash
git clone https://github.com/xcena-dev/maru
cd maru

python3 -m venv .venv
source .venv/bin/activate
./install.sh
```

Verify the Maru Resource Manager daemon is running:

```bash
systemctl status maru-resourced
```

### Start Services

```bash
# Start MaruServer (metadata server)
maru-server

# With custom host/port
maru-server --host 0.0.0.0 --port 5555
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

Maru works as a drop-in remote storage backend for [LMCache](https://github.com/LMCache/LMCache) via the `maru://` URL scheme. It supports both **P2P KV cache sharing** and **disaggregated prefill** scenarios.

```yaml
# LMCache config
remote_url: "maru://localhost:5555"
extra_config:
  maru_pool_size: "4G"
```

For full configuration details, see the [documentation](https://xcena-dev.github.io/maru-dev/source/getting_started/examples/lmcache/).


## License

Copyright 2026 [XCENA Inc.](https://xcena.com) Licensed under the Apache License 2.0.
