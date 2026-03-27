# Quickstart

## 1. Start the Resource Manager

The resource manager must be running before any other Maru service.

**Production** — start as a systemd daemon:

```bash
sudo systemctl start maru-resource-manager
```

**Development/debugging** — run directly with custom options:

```bash
# Default (0.0.0.0:9850)
sudo maru-resource-manager --log-level debug

# Custom port
sudo maru-resource-manager --port 9851 --log-level debug
```

> If you change the RM port, pass the same address to `maru-server`: `maru-server --rm-address 127.0.0.1:9851`
>
> See {doc}`installation` for the full list of CLI options and systemd configuration.

## 2. Start the Metadata Server

```bash
# Default (localhost:5555, connects to resource manager at 127.0.0.1:9850)
maru-server

# With custom host/port
maru-server --host 0.0.0.0 --port 5556

# With debug logging
maru-server --log-level DEBUG
```

## 3. Run a Client Example

> Both `maru-resource-manager` and `maru-server` must be running before proceeding.

### Zero-Copy Store & Retrieve

```python
from maru import MaruConfig, MaruHandler

config = MaruConfig(
    server_url="tcp://localhost:5555",
    pool_size=1024 * 1024 * 100,  # 100MB
)

with MaruHandler(config) as handler:
    data = b"A" * (1024 * 1024)  # 1MB KV chunk

    # 1. Allocate a page in CXL shared memory
    handle = handler.alloc(size=len(data))

    # 2. Write directly to CXL memory (mmap — no intermediate buffer)
    handle.buf[:] = data

    # 3. Register the key — only metadata (key → region, offset) is sent
    handler.store(key=42, handle=handle)

    # Retrieve: returns a memoryview pointing into CXL memory
    result = handler.retrieve(key=42)
    assert result is not None
    assert bytes(result.view[:5]) == b"AAAAA"
```

The three steps — `alloc()`, write to `handle.buf`, `store(handle=)` — make Maru's control/data plane separation explicit:

- **Control plane**: `alloc()` requests a memory region from the Resource Manager; `store()` registers the key's location metadata (~tens of bytes) with MaruServer.
- **Data plane**: `handle.buf[:]` reads/writes directly on mmap'd CXL memory. No server involved.

> Full runnable example: `examples/basic/single_instance.py`

### Cross-Instance Sharing

This is the core of Maru — two independent processes sharing KV cache through CXL shared memory with zero data copy. **Metadata travels, data doesn't.**

```mermaid
flowchart LR
    P[Producer] -->|"alloc + write"| CXL[(CXL Memory)]
    P -->|"store(key)"| S[MaruServer]
    C[Consumer] -->|"retrieve(key)"| S
    S -.->|"location"| C
    C -->|"read"| CXL
```

**Terminal 1** — Producer: allocate, write, register metadata:

```python
from maru import MaruConfig, MaruHandler

config = MaruConfig(
    server_url="tcp://localhost:5555",
    instance_id="producer",
    pool_size=1024 * 1024 * 100,
)

with MaruHandler(config) as handler:
    for i, key in enumerate([1001, 1002, 1003]):
        data = bytes([ord("A") + i]) * (1024 * 1024)

        handle = handler.alloc(size=len(data))
        handle.buf[:] = data
        handler.store(key=key, handle=handle)

    input("Press Enter to exit...")
```

**Terminal 2** — Consumer: retrieve from CXL (zero copy):

```python
from maru import MaruConfig, MaruHandler

config = MaruConfig(
    server_url="tcp://localhost:5555",
    instance_id="consumer",
    pool_size=1024 * 1024 * 100,
)

with MaruHandler(config) as handler:
    for key in [1001, 1002, 1003]:
        result = handler.retrieve(key=key)

        # result.view points directly into Producer's CXL region (mapped read-only).
        # No data was copied — consumer reads the same physical memory.
        assert result is not None
        print(f"key={key}: {len(result.view)} bytes, char={chr(result.view[0])!r}")
```

> Full runnable examples: `examples/basic/producer.py` + `examples/basic/consumer.py`
>
> Single-script version: `examples/basic/cross_instance.py`



## Next Steps

- [LMCache Integration](examples/lmcache/index.md) — Use Maru as a shared KV cache backend for LMCache/vLLM
- [Architecture Overview](../design_doc/architecture_overview.md) — System architecture and component interactions
- [API Reference](../api_reference/api.md) — Python API documentation
- [Configuration](../api_reference/config.md) — Configuration options
