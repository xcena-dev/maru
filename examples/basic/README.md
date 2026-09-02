# Basic Examples

Demonstrates Maru's core functionality: zero-copy KV store on CXL shared memory.

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

Two separate processes sharing KV data through CXL shared memory — zero copy.

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
