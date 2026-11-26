# Installation

This document describes how to build and install Maru.

## System Components

The node backend (selected by `maru_node`) uses one MaruServer (`maru-server --backend node`) and a GPU StorageService (`maru-storage --role gpu`) on each GPU node. A remote store adds a memory StorageService (`maru-storage --role memory`) that registers its pool for RDMA. A node that owns local CXL regions also runs a Resource Manager.

The `cxl` backend uses MaruServer (`maru-server --backend cxl`), the Resource Manager and CXL-attached clients. LMCache and SGLang use this directly mapped backend. See {doc}`quick_start` to choose a backend and {doc}`../design_doc/node_storage_service` for node configuration.

## Prerequisites

- OS: Ubuntu 24.04 LTS+
- Python: 3.12+
- gcc: 13.3.0+
- cmake: 3.28.3+
- git
- [uv](https://docs.astral.sh/uv/) — recommended Python package installer
- For CXL stores: a CXL DAX device (`/dev/dax*`) or emulation environment on hosts that map the memory
- For remote stores: an RDMA NIC on each GPU and memory node

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

> Pass `--python 3.12` explicitly so the interpreter matches the one your LLM engine (vLLM, SGLang) was built against. Without it, uv may pick any interpreter satisfying `requires-python >= 3.12`.

Run `./install.sh` to install the Python package and the Resource Manager. Use `./install.sh --no-rm` for the Python package alone. A directly mapped `cxl` client needs DAX access and a connection to its Resource Manager.

#### Node Store requirements

| Node | Store | Install command |
| --- | --- | --- |
| MaruServer | All | `./install.sh --no-rm` |
| GPU | `dram` or `remote` | `./install.sh --no-rm` |
| GPU | `local_cxl` | `./install.sh` |
| Memory | `dram` | `./install.sh --no-rm` |
| Memory | `local_cxl` | `./install.sh` |

On each GPU node, install vLLM 0.16 or later for deferred loading before installing Maru. See the {ref}`vLLM prerequisites <vllm-prerequisites>`. For a `remote` store, install the {ref}`NIXL stack <nixl-stack>` on both GPU and memory nodes. The GPU node can use the remote pool without a CXL device.

**Locked memory for RDMA.** With a `remote` store, the memory StorageService registers its pool and each vLLM worker registers its staging buffer with the NIC. Give the accounts running the memory StorageService and vLLM a sufficient memlock limit; with the common 8 MiB default, registration fails with `Out of memory`. For example, a `<user> - memlock unlimited` entry in `/etc/security/limits.d/maru.conf` sets both limits for login sessions. For systemd services, set `LimitMEMLOCK=infinity` in the `[Service]` section.

**DRAM huge pages.** A `dram` store defaults to `"hugepage": true`. The StorageService refuses to start the store if it cannot allocate the full pool. Reserve enough free huge pages for the store or set `"hugepage": false` in its tier. For example, a node dedicated to a single 1 GiB store needs 512 pages when `Hugepagesize` in `/proc/meminfo` is 2048 kB.

```bash
sudo sysctl -w vm.nr_hugepages=512
```

**Runtime directory.** The default `state_dir` is `/run/maru`. The GPU StorageService listens on `/run/maru/node.sock` by default. Since `/run` is a tmpfs, recreate the writable directory at boot with this entry in `/etc/tmpfiles.d/maru.conf`. Replace the placeholders with the StorageService account and group.

```text
d /run/maru 0755 <user> <group> -
```

If vLLM and the GPU StorageService run as different users, set `socket_group` to a group both users belong to. The socket mode is `0660`.

Start each StorageService with its own JSON configuration. For a remote store, follow the {ref}`deployment key instructions <quickstart-deployment-key>` and set `key_file` on both services. MaruServer does not require the key. See {doc}`../integration/vllm` for launch commands.

(nixl-stack)=

#### NIXL stack

Maru requires NIXL `>=1.2.0,<2` with its public native APIs. Choose NIXL, Torch and CUDA runtimes compatible with each other and with the serving engine.

```bash
uv pip install 'nixl>=1.2.0,<2'
```

Remote processes use `UCX_TLS=rc,dc,cuda_copy` and `UCX_RCACHE_ENABLE=n` by default. Explicit settings must use key-checked RC/DC transports and disable registration caching. `cuda_copy` handles local CUDA copies. Maru rejects TCP, shared-memory transports and the broad `cuda` alias.

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

Verify the x86 cache-flush extension used by the {doc}`quick_start_cxl` producer and consumer. It is an optional build product, so the install succeeds without it when no C compiler is available:

```bash
python3 -c "from maru_shm._cxl_flush import HAVE_CLFLUSH; print(HAVE_CLFLUSH)"
```

`1` means the extension is available. An `ImportError` means it was not built: install a C compiler (`build-essential`) and the headers for your Python interpreter (`python3-dev` for the system Python), then rerun `install.sh`.

If you installed with the Resource Manager, verify the binary:

```bash
which maru-resource-manager
```

Once the Resource Manager runs, `sudo maru-rm-ctl status` shows the devices it serves and why any other device is not served. See [`maru-rm-ctl`](../api_reference/maru_rm_ctl.md).

Once installation is verified, choose a setup in {doc}`quick_start` to start services and verify KV sharing.

<br/>

## 3. Multi-Node Configuration

In a multi-node deployment, the Resource Manager, MaruServer, and MaruHandler communicate over the network.

```
   Node A (Orchestrator)             Node B
  ┌─────────────────────────┐       ┌─────────────────────┐
  │ LLM Engine              │       │ LLM Engine          │
  │ MaruHandler             │       │ MaruHandler         │
  │                         │       │                     │
  │ MaruServer              │◄─RPC─►│                     │
  │ maru-resource-manager   │       │                     │
  └────────────┬────────────┘       └──────────┬──────────┘
               │                               │
               └──────── CXL Memory Pool ──────┘
```

> MaruServer and `maru-resource-manager` do not need to run on the same node. The diagram above shows the simplest configuration; each can be deployed independently as long as MaruHandler can reach both over the network.

(installation-multi-node-config)=

### Configuration

Multi-node requires changing default bind addresses from `127.0.0.1` to a network-accessible address:

- **Resource Manager**: change `--host` to accept remote connections
- **The `cxl` MaruServer**: change `--host` and set `--rm-address` to the Resource Manager's externally reachable address
- **MaruHandler**: set `server_url` to the MaruServer address. The Resource Manager address is received automatically via handshake.
- **Node services**: set `server` to the node MaruServer URL, GPU `remote.url` to the memory endpoint and memory `remote_server.listen` to its reachable bind address. Set `local_cxl.rm` to the Resource Manager of that store. Workers and their GPU service must share a PID namespace.

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
