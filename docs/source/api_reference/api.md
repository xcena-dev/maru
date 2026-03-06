# Python API

## MaruHandler

The main client interface for Maru. Handles storing, retrieving, and deleting KV cache entries in CXL shared memory via MaruServer.

```python
from maru import MaruConfig, MaruHandler
```

### Usage

```python
config = MaruConfig(server_url="tcp://localhost:5555", pool_size=1024 * 1024 * 100)

with MaruHandler(config) as handler:
    data = b"hello world"

    # Store: alloc → write → register
    handle = handler.alloc(size=len(data))
    handle.buf[:len(data)] = data
    handler.store(key=12345, handle=handle)

    # Retrieve (zero-copy memoryview into CXL memory)
    result = handler.retrieve(key=12345)

    # Check existence
    handler.exists(key=12345)

    # Delete
    handler.delete(key=12345)
```

### Key Behaviors

- **Auto-connect**: Connects automatically on init when `auto_connect=True` (default)
- **Auto-expand**: Automatically allocates new memory regions when current ones are full
- **Deduplication**: Skips store if the key already exists
- **Zero-copy retrieve**: Returns memoryview pointing directly into mmap region
- **Thread-safe**: Read operations (retrieve, exists) are lock-free; write operations (store, delete) are serialized by an internal write lock

### Methods

```{eval-rst}
.. autoclass:: maru_handler.MaruHandler
   :members: connect, close, alloc, store, retrieve, exists, delete,
             batch_store, batch_retrieve, batch_exists,
             healthcheck, get_stats
   :noindex:
   :no-undoc-members:
   :class-doc-from: init
```

---

## AllocHandle

Handle returned by `alloc()` for zero-copy writes. The caller writes data directly to `buf` (a writable memoryview into CXL shared memory), then passes the handle to `store()` to register the key.

```python
# 1. Allocate a page
handle = handler.alloc(size=1024 * 1024)

# 2. Write directly to CXL memory (zero-copy)
handle.buf[:len(data)] = data

# 3. Register the key (only metadata is sent)
handler.store(key=42, handle=handle)
```

| Property | Type | Description |
|----------|------|-------------|
| `buf` | `memoryview` | Writable view into the allocated CXL memory page |
| `size` | `int` | Requested allocation size in bytes |
| `region_id` | `int` | Region ID of the allocated page |
| `page_index` | `int` | Page index within the region |

```{eval-rst}
.. autoclass:: maru_handler.memory.AllocHandle
   :members:
   :no-undoc-members:
   :class-doc-from: init
```

---

## MemoryInfo

Zero-copy view into CXL shared memory, returned by `retrieve()`. Wraps a memoryview pointing directly into an mmap'd CXL region.

```python
result = handler.retrieve(key=42)
if result is not None:
    # result.view is a memoryview into CXL shared memory
    data = bytes(result.view)
    length = len(result.view)
```

| Property | Type | Description |
|----------|------|-------------|
| `view` | `memoryview` | Zero-copy view into the CXL memory region (read-only for cross-instance retrieves) |

```{eval-rst}
.. autoclass:: maru_handler.memory.MemoryInfo
   :members:
   :no-undoc-members:
   :class-doc-from: init
```
