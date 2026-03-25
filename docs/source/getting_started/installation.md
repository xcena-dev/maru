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

Verify that the Maru Resource Manager binary is installed:

```bash
which maru-resource-manager
```

The resource manager must be started manually before launching `MaruServer`. Start it with root privileges (required for CXL/DAX device access):

```bash
sudo maru-resource-manager
```

To verify it is running, check pool status from another terminal:

```bash
maru_test_client stats
```

> **Note:** The resource manager runs in the foreground. Use `&` or a separate terminal to keep it running while starting other services. See {doc}`quick_start` for the full startup sequence.

Once installation is verified, proceed to the {doc}`quick_start` guide to start services and run your first store/retrieve.
