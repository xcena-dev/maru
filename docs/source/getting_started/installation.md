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

The resource manager must be running before launching `MaruServer`. Start it as a systemd service:

```bash
sudo systemctl start maru-resource-manager
```

To verify it is running:

```bash
sudo systemctl status maru-resource-manager
maru_test_client stats
```

To start automatically on boot:

```bash
sudo systemctl enable maru-resource-manager
```

> **Note:** You can also run the binary directly for development/debugging: `sudo maru-resource-manager --log-level debug`. If the systemd service is already running, the direct command will report a port conflict.

Once installation is verified, proceed to the {doc}`quick_start` guide to start services and run your first store/retrieve.
