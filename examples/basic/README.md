# Basic Examples

Demonstrates the default `cxl` backend: write into a mapped CXL page, publish its key, and retrieve a memoryview of that page. Writing the initial payload still copies bytes. The `remote_cxl` backend instead uses host staging and RDMA; see the [remote CXL example](../vllm/remote_cxl/README.md).

## Prerequisites

- Maru installed (`uv pip install -e .`)
- Maru Resource Manager running (`systemctl status maru-resourced`)
- CXL DAX device available (`/dev/dax*`)

For setup instructions, see the [Installation Guide](../../docs/source/getting_started/installation.md).

## Examples

### 1. Single Instance (`single_instance.py`)

Single-instance example showing the alloc → write → store → retrieve cycle.

```bash
# Start the metadata server
maru-server

# In another terminal
python examples/basic/single_instance.py
```

### 2. Cross-Instance Sharing (`producer.py` + `consumer.py`)

Two separate processes using the same CXL mapping. Retrieving a memoryview avoids an additional host copy; converting it to `bytes` copies the payload.

```bash
# Terminal 1: start the metadata server
maru-server

# Terminal 2: store data into CXL memory
python examples/basic/producer.py

# Terminal 3: retrieve data from CXL memory (zero copy)
python examples/basic/consumer.py
```

### 3. Cross-Instance Sharing — Single Script (`cross_instance.py`)

Same as above but runs producer and consumer as threads in a single script.

```bash
# Start the metadata server
maru-server

# In another terminal
python examples/basic/cross_instance.py
```
