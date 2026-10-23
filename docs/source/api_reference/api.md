# Python API

## MaruHandler

The main client interface for Maru. It stores and retrieves shared KV objects, with metadata managed by MaruServer. The selected storage backend determines where allocation and read buffers live.

```python
from maru import MaruConfig, MaruHandler
```

### Usage

This example and the `AllocHandle`/`MemoryInfo` descriptions below use the default **`cxl` backend**. A returned memoryview points into a directly mapped CXL region. The `remote_cxl` backend instead allocates worker-side CPU staging, transfers payloads over RDMA, and returns a read lease over staging memory; it does not expose a memoryview of remote CXL on the worker.

```python
config = MaruConfig(server_url="tcp://localhost:5555", pool_size=1024 * 1024 * 100)

with MaruHandler(config) as handler:
    data = b"hello world"

    # Store: alloc → write → register
    handle = handler.alloc(size=len(data))
    handle.buf[: len(data)] = data
    handler.store(key=12345, handle=handle)

    # Retrieve (zero-copy memoryview into CXL memory)
    result = handler.retrieve(key=12345)

    # Check existence
    handler.exists(key=12345)

    # Delete
    handler.delete(key=12345)
```

### Key Behaviors

The following behaviors describe the default `cxl` path:

- **Auto-connect**: Connects automatically on init when `auto_connect=True` (default)
- **Auto-expand**: Automatically allocates new memory regions when current ones are full
- **Deduplication**: Skips store if the key already exists
- **Zero-copy retrieve**: Returns memoryview pointing directly into mmap region
- **Thread-safe**: Read operations (retrieve, exists) are lock-free; write operations (store, delete) are serialized by an internal write lock

For `remote_cxl`, retain the returned read lease until the last CPU read or GPU copy has completed, then release it with `handler.release_retrieved([result])`. Returning it early would allow its staging slot to be reused. See {doc}`config` and the {doc}`vLLM guide <../integration/vllm>` for backend configuration and buffer lifetimes.

### Methods

```{eval-rst}
.. autoclass:: maru_handler.MaruHandler
   :members: connect, close, alloc, free, store, retrieve, exists, pin, unpin, delete,
             batch_store, batch_retrieve, batch_exists, batch_pin, batch_unpin,
             healthcheck, get_stats, release_retrieved
   :noindex:
   :no-undoc-members:
   :class-doc-from: init
```

---

## AllocHandle

With `cxl`, `alloc()` returns a writable memoryview into a mapped CXL page. The caller fills `buf`, then passes the handle to `store()` to register the key without an additional payload copy. Filling `buf` from an existing byte array or GPU tensor still copies bytes. With `remote_cxl`, the allocation is a `RemoteCxlAllocation` backed by staging, and storing it also transfers the bytes by RDMA.

```python
# 1. Allocate a page
handle = handler.alloc(size=1024 * 1024)

# 2. Copy the payload into the mapped CXL page (cxl backend)
handle.buf[: len(data)] = data

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

A `cxl` read result from `retrieve()`, with a memoryview pointing directly into an mmap'd CXL region. Obtaining the view adds no payload copy; converting the view to `bytes`, as below, does. A `remote_cxl` read instead returns a lease over a staging slot populated by RDMA.

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
