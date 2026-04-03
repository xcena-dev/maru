# Installation

This document describes how to build and install Maru.

## System Components

Maru consists of two server components and a client library:

| Component | Role | Package |
|-----------|------|---------|
| **Resource Manager** | Manages the CXL memory pool | `maru-resource-manager` (C++) |
| **Metadata Server** | Manages KV metadata | `maru-server` (Python) |
| **MaruHandler** | Client library embedded in LLM instances | `maru` Python package |

In a single-node setup, all components run on the same machine. In a multi-node setup, designate one node as the orchestrator to run the Resource Manager and Metadata Server. Other nodes run LLM instances with MaruHandler, which connects to both the Resource Manager and Metadata Server over the network. See {doc}`quick_start` and {doc}`../design_doc/architecture_overview` for multi-node configuration details.

## Prerequisites

- OS: Ubuntu 24.04 LTS+
- Python: 3.12+
- gcc: 13.3.0+
- cmake: 3.28.3+
- git
- CXL DAX device (`/dev/dax*`) or emulation environment
  - **Multi-node:** All participating nodes must be connected to a shared CXL memory pool (e.g., via CXL switch).

```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip git \
    build-essential cmake libnuma-dev
```

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
python3 -m venv .venv
source .venv/bin/activate
```

Install all components (Python package + Resource Manager):

```bash
./install.sh
```

To install **without the Resource Manager** (e.g., on nodes that only run LLM instances with MaruHandler):

```bash
./install.sh --no-rm
```

> **Note:** Client nodes still require CXL device access (`/dev/dax*`) for direct mmap. The `--no-rm` flag skips building the Resource Manager binary — the `maru` Python package (including MaruHandler) is still installed and will connect to the remote Resource Manager.

<br/>

## 2. Verify Installation

Verify that the Maru Python package is installed:

```bash
python3 -c "import maru_shm; print('ok')"
```

If you installed with the Resource Manager, verify the binary:

```bash
which maru-resource-manager
```

Once installation is verified, proceed to the {doc}`quick_start` guide to start services and run your first store/retrieve.
