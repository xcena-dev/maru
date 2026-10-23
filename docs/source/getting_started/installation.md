# Installation

This document describes how to build and install Maru.

## System Components

Maru consists of two server components and a client library. A deployment with GPU nodes without CXL (the `remote_cxl` backend) adds a remote CXL agent on each memory node:

| Component | Role | Package |
|-----------|------|---------|
| **Resource Manager** | Manages the CXL memory of a memory pool | `maru-resource-manager` (C++) |
| **Metadata Server** | Manages KV metadata | `maru-server` (Python) |
| **MaruHandler** | Client library embedded in LLM instances | `maru` Python package |
| **Remote CXL Agent** | On memory nodes: maps CXL regions for LLM instances on nodes without CXL and registers them with the NIC (`remote_cxl` backend) | `maru-remote-cxl-agent` (needs NIXL) |

In a single-node setup, all components run on the same machine. In a multi-node setup, designate one node as the orchestrator to run the Resource Manager and Metadata Server. Other nodes run LLM instances with MaruHandler. With the default `cxl` backend, MaruHandler connects to both the Resource Manager and Metadata Server over the network. With the `remote_cxl` backend, it connects to the Metadata Server and to the remote CXL agent of each memory node instead; the remote CXL agents reach the Resource Manager, which runs on a memory node.

## Prerequisites

- OS: Ubuntu 24.04 LTS+
- Python: 3.12+
- gcc: 13.3.0+
- cmake: 3.28.3+
- git
- [uv](https://docs.astral.sh/uv/) — recommended Python package installer
- CXL DAX device (`/dev/dax*`) or emulation environment, on the nodes that hold the pool and the nodes that read it over CXL
  - **Multi-node:** All nodes that share the pool over CXL must be connected to the shared CXL memory pool (e.g., via CXL switch). GPU nodes without CXL reach it over RDMA instead (`remote_cxl` backend).
- For the `remote_cxl` backend: an RDMA NIC (RoCE or InfiniBand) on every worker and memory node, a route from the maru-server node to the remote CXL agents on the memory nodes' RDMA network addresses, and NIXL in the same environment, installed before `./install.sh` (`uv pip install nixl`, or `pip install nixl` without uv)

See {doc}`bios_setup` for platform BIOS settings and DEV_DAX verification, and {doc}`../design_doc/consistency_and_safety` for the cache-visibility requirements of cross-host sharing.

```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip git \
    build-essential cmake libnuma-dev

# uv — used for all Python installs in this guide
curl -LsSf https://astral.sh/uv/install.sh | sh
```

> **Note:** `install.sh` uses `uv` when available and falls back to `pip` otherwise, so uv is recommended but not required.

<br/>

## 1. Installation from Source Code
### 1.1 Getting the Source
The Maru source code for released versions can be obtained from our GitHub repository: [https://github.com/xcena-dev/maru](https://github.com/xcena-dev/maru)
```bash
git clone https://github.com/xcena-dev/maru
```

<br/>

### 1.2 Installation

(Optional) Create a virtual environment and activate it:

```bash
uv venv --python 3.12 .venv
source .venv/bin/activate
```

> Pass `--python 3.12` explicitly so the interpreter matches the one your LLM
> engine (vLLM, SGLang) was built against. Without it, uv may pick any
> interpreter satisfying `requires-python >= 3.12`.

Install all components (Python package + Resource Manager):

```bash
./install.sh
```

To install **without the Resource Manager** (e.g., on nodes that only run LLM instances with MaruHandler, and always on `remote_cxl` worker nodes, which would otherwise get a remote CXL agent and a key of their own):

```bash
./install.sh --no-rm
```

> **Note:** With the default `cxl` backend, client nodes still require CXL device access (`/dev/dax*`) for direct mmap. The `--no-rm` flag skips building the Resource Manager binary — the `maru` Python package (including MaruHandler) is still installed and will connect to the remote Resource Manager. Workers that use the `remote_cxl` backend need no CXL device: they reach the CXL memory of the memory nodes over RDMA.

With NIXL installed (`uv pip install nixl` before `./install.sh`), the
installer also sets up the remote CXL agent as `maru-remote-cxl-agent.service`, which
starts and stops with the Resource Manager, and creates the remote CXL agent key
`/etc/maru/remote-cxl-agent.key`. Set the node's RDMA NIC in
`/etc/maru/remote-cxl-agent.env` (`MARU_REMOTE_CXL_AGENT_UCX_DEVICE`): the service
listens on that NIC's address. Workers and maru-server need a copy of the key,
readable only by the users that run them; they read
`~/.maru/remote-cxl-agent.key` by default. The
[remote CXL example](https://github.com/xcena-dev/maru/blob/main/examples/vllm/remote_cxl/README.md)
walks through a deployment.

The installer checks for NIXL, not for an RDMA NIC, when selecting this service.
It enables the unit without starting it immediately. Starting the Resource
Manager later also starts the agent; with the default empty NIC and listen
address, the agent fails to start until its RDMA interface is configured. The
unit stops retrying after five failed starts in a minute. This agent failure
does not stop the Resource Manager.

A memory node used only for the local `cxl` backend does not need the agent.
Without NIXL the installer skips its service. If NIXL is installed for another
purpose on such a node, disable only the unused agent:

```bash
sudo systemctl disable --now maru-remote-cxl-agent.service
```

Reinstalling with NIXL enables the unit again. `./install.sh --no-rm` skips both
platform services, so it is the worker-node installation path rather than a
way to keep the Resource Manager without its agent on a memory node.

<br/>

### 1.3 KV Placement Kernels (vLLM connector)

The vLLM connector moves a request's KV cache between a Maru CXL object and vLLM's paged cache with the CUDA kernels in `maru_kv_ops`. Those kernels are a build product of this package, and building them needs **PyTorch importable by the interpreter that will run the engine, plus the CUDA toolkit (`nvcc`)**. On a host without either, Maru installs anyway and the connector copies one layer at a time — correct, but materially slower, and the connector logs a warning naming this step.

`install.sh` handles this: it looks for PyTorch in the target environment and, when it finds it, installs with `--no-build-isolation` so the build can see it, then reports whether the kernels are callable.

Installing by hand needs that flag passed explicitly. An isolated build gets only the packages in `[build-system].requires`, so it never sees PyTorch and skips the kernels silently:

```bash
uv pip install setuptools wheel          # the build backend, in this environment
uv pip install -e . --no-build-isolation
```

Check the result at any time:

```bash
python -c "import maru_kv_ops; print(maru_kv_ops.is_available(), maru_kv_ops.import_error())"
```

`True None` means the connector takes the kernel path. Anything else prints why it cannot, and `MARU_SKIP_KV_OPS=1` skips the build outright when that is what you want.

<br/>

### 1.4 Development Install

To work on Maru itself, install the `dev` extra (pytest, mypy, ruff, pre-commit) instead of running `install.sh`:

```bash
uv venv --python 3.12 .venv
source .venv/bin/activate
uv pip install -e ".[dev]"
```

Without uv, the same install works through pip:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Install the git hooks once, so lint and format run on every commit:

```bash
pre-commit install
```

Then run the unit tests (these need neither a CXL device nor a running service):

```bash
pytest -m "not integration" --ignore=tests/sglang
```

To reproduce the lint job exactly, run the hooks against the whole tree. This
uses the ruff version pinned in `.pre-commit-config.yaml` — the same version CI
runs:

```bash
pre-commit run --all-files
```

<br/>

(installation-verify)=
## 2. Verify Installation

Verify that the Maru Python package is installed:

```bash
python3 -c "import maru_shm; print('ok')"
```

Verify the x86 cache-flush extension used by the {doc}`quick_start` producer and consumer. It is an optional build product, so the install succeeds without it when no C compiler is available:

```bash
python3 -c "from maru_shm._cxl_flush import HAVE_CLFLUSH; print(HAVE_CLFLUSH)"
```

`1` means the extension is available. An `ImportError` means it was not built: install a C compiler (`build-essential`) and the headers for your Python interpreter (`python3-dev` for the system Python), then rerun `install.sh`.

If you installed with the Resource Manager, verify the binary:

```bash
which maru-resource-manager
```

Once the Resource Manager runs, `sudo maru-rm-ctl status` shows the devices it serves and why any other device is not served. See [`maru-rm-ctl`](../api_reference/maru_rm_ctl.md).

Once installation is verified, choose the single-host or multi-host setup in
{doc}`quick_start` to start services and verify KV sharing.

<br/>

## 3. Multi-Node Configuration

In a multi-node deployment, the Resource Manager, Metadata Server, and MaruHandler communicate over the network.

```
   Node A (Orchestrator)             Node B
  ┌─────────────────────────┐       ┌─────────────────────┐
  │ LLM Engine              │       │ LLM Engine          │
  │ MaruHandler             │       │ MaruHandler         │
  │                         │       │                     │
  │ maru-server             │◄─RPC─►│                     │
  │ maru-resource-manager   │       │                     │
  └────────────┬────────────┘       └──────────┬──────────┘
               │                               │
               └──────── CXL Memory Pool ──────┘
```

> `maru-server` and `maru-resource-manager` do not need to run on the same node. The diagram above shows the simplest configuration; each can be deployed independently as long as MaruHandler can reach both over the network.

(installation-multi-node-config)=
### Configuration

Multi-node requires changing default bind addresses from `127.0.0.1` to a network-accessible address:

- **Resource Manager**: change `--host` to accept remote connections
- **Metadata Server**: change `--host` and set `--rm-address` to the Resource Manager's externally reachable address
- **MaruHandler**: set `server_url` to the Metadata Server's address. The Resource Manager address is received automatically via handshake.
- **Remote CXL Agent** (`remote_cxl` backend): set `MARU_RM_ADDRESS` in `/etc/maru/remote-cxl-agent.env` to the address the Resource Manager listens on; a Resource Manager bound to a network address answers only there.

To run the Resource Manager as the systemd service on a network address, override its command line:

```bash
sudo systemctl edit maru-resource-manager
```

```ini
[Service]
ExecStart=
ExecStart=/usr/local/bin/maru-resource-manager --host <node-a-ip> --port 9850
```

```bash
sudo systemctl restart maru-resource-manager
```

> **Security:** When binding to a non-loopback address, auth tokens and device paths are transmitted in plaintext. Use an encrypted tunnel (WireGuard, SSH tunnel, IPsec) in production.

Follow the {ref}`multi-host quickstart <quickstart-multi-host>` to start the
services and share data between a Python producer and consumer on two hosts.
