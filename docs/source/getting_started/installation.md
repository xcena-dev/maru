# Installation

This document describes how to build and install Maru.

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
### 1.1. Getting the Source
The Maru source code for released versions can be obtained from our GitHub repository: [https://github.com/xcena-dev/maru](https://github.com/xcena-dev/maru)
```bash
git clone https://github.com/xcena-dev/maru
```

<br/>

### 1.2. Installation

(Optional) Create a virtual environment and activate it:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install the Maru Python package and resource manager:

```bash
./install.sh
```

<br/>

## 2. Verify Installation

Verify that the Maru Resource Manager daemon is running:

```bash
systemctl status maru-resourced
```

> **Deprecation Notice**: The local systemd daemon (`maru-resourced`) will be replaced by an RPC-based Resource Manager server in a future release. This change enables multi-node resource management without requiring a daemon on each node.

Once installation is verified, proceed to the {doc}`quick_start` guide to start services and run your first store/retrieve.
