# Installation

This document describes how to build and install Maru.

## System Components

Node deployments run one Catalog (`maru-node-server`), a GPU-host service (`maru-storage`) and vLLM workers. A remote store adds `maru-memory-node`, which owns DRAM or local CXL and its RDMA registrations. The Resource Manager is needed for CXL stores, not for DRAM.

The `cxl` backend uses `maru-server`, the Resource Manager and CXL-attached clients. LMCache and SGLang use this directly mapped path. See {doc}`quick_start` to choose a path and {doc}`../design_doc/node_storage_service` for node configuration.

## Prerequisites

- OS: Ubuntu 24.04 LTS+
- Python: 3.12+
- gcc: 13.3.0+
- cmake: 3.28.3+
- git
- [uv](https://docs.astral.sh/uv/) — recommended Python package installer
- For CXL stores: a CXL DAX device (`/dev/dax*`) or emulation environment on hosts that map the memory
- For remote stores: an RDMA NIC and compatible NIXL/UCX runtime on GPU workers and memory nodes

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

(nixl-stack)=

#### NIXL stack

For a node remote store, install compatible NIXL, Torch and CUDA runtimes on GPU workers and memory nodes. Maru requires NIXL `>=1.2.0,<2` with its public native APIs. Select the wheels together with the serving engine; the NIXL version alone does not establish Torch/CUDA compatibility.

```bash
uv pip install 'nixl>=1.2.0,<2'
```

Remote processes use `UCX_TLS=rc,dc,cuda_copy` and `UCX_RCACHE_ENABLE=n` by default. Explicit settings must use key-checked RC/DC transports and disable registration caching. `cuda_copy` is a local CUDA helper, not the remote payload transport. TCP, shared-memory transports and the broad `cuda` alias are rejected.

Install all components (Python package + Resource Manager):

```bash
./install.sh
```

To install the Python package without building the Resource Manager, such as on a GPU host using a remote store:

```bash
./install.sh --no-rm
```

A directly mapped `cxl` client still needs DAX access and connectivity to its Resource Manager. A remote worker needs no CXL device; the memory node owns its store and registrations.

Start `maru-storage` and `maru-memory-node` explicitly with their JSON configurations. Create and distribute the deployment key as described in {doc}`migrating_to_maru_node`; set `key_file` explicitly on both services. The Catalog does not require the key. See {doc}`../integration/vllm` for launch commands.

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

`True None` means the extension can be imported, not that every request uses its packed kernel. The resolved KV layout, chunk completeness and buffer registration also select the path. vLLM 0.29+ logical pages use the stride-based placement bindings when available; otherwise transfers use per-layer copies. Check the engine's transfer-path logs as well. Other output reports the import failure; `MARU_SKIP_KV_OPS=1` skips the build explicitly.

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
pytest -m "not integration"
```

To reproduce the lint job exactly, run the hooks against the whole tree. This uses the ruff version pinned in `.pre-commit-config.yaml` — the same version CI runs:

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

Once installation is verified, choose the single-host or multi-host setup in {doc}`quick_start` to start services and verify KV sharing.

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
- **Node services**: set `server` to the Catalog URL, GPU `remote.url` to the memory endpoint and memory `remote_server.listen` to its reachable bind address. Set `local_cxl.rm` to the Resource Manager of that store. Workers and their GPU service must share a PID namespace.

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

Follow the {ref}`multi-host quickstart <quickstart-multi-host>` to start the services and share data between a Python producer and consumer on two hosts.
