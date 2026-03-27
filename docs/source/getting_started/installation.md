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

The resource manager must be running before launching `MaruServer`.

### Daemon Mode (recommended for production)

Start as a systemd service:

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

To customize host/port or log level, edit the systemd service override:

```bash
sudo systemctl edit maru-resource-manager
```

Add the following in the editor:

```ini
[Service]
ExecStart=
ExecStart=/usr/local/bin/maru-resource-manager --host 0.0.0.0 --port 9850 --log-level debug
```

Then restart the service:

```bash
sudo systemctl restart maru-resource-manager
```

### Direct Mode (for development/debugging)

Run the binary directly with custom options:

```bash
sudo maru-resource-manager --log-level debug
```

Available CLI options:

| Option | Default | Description |
|--------|---------|-------------|
| `--host`, `-H` | `0.0.0.0` | TCP bind address |
| `--port`, `-p` | `9850` | TCP port |
| `--state-dir`, `-d` | `/var/lib/maru-resourced` | State directory for WAL and metadata |
| `--log-level`, `-l` | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `--num-workers`, `-w` | `32` | Worker thread pool size |

Example with custom port:

```bash
sudo maru-resource-manager --port 9851 --log-level debug
```

> **Note:** If the systemd service is already running, the direct command will report a port conflict. Stop the service first: `sudo systemctl stop maru-resource-manager`

> **Important:** If you change the resource manager port from the default (`9850`), you must also pass the same address to `maru-server`:
> ```bash
> maru-server --rm-address 127.0.0.1:9851
> ```

Once installation is verified, proceed to the {doc}`quick_start` guide to start services and run your first store/retrieve.
