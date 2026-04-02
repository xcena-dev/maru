# Installation

This document describes how to build and install Maru.

## System Components

Maru consists of two server components:

| Component | Role | Binary |
|-----------|------|--------|
| **Resource Manager** | Manages the CXL memory pool | `maru-resource-manager` (C++) |
| **Metadata Server** | Manages KV metadata | `maru-server` (Python) |

Both must be running for Maru to operate. In a single-node setup, both run on the same machine. In a multi-node setup, all nodes must have access to the same CXL memory pool. The Resource Manager runs on one node to manage the memory pool, and other nodes connect to it over TCP.

## Prerequisites

- OS: Ubuntu 24.04 LTS+
- Python: 3.12+
- gcc: 13.3.0+
- cmake: 3.28.3+
- git
- CXL DAX device (`/dev/dax*`) or emulation environment

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

To install **without the Resource Manager** (e.g., on client nodes that only run LLM instances):

```bash
./install.sh --no-rm
```

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
