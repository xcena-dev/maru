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
    handler.store(key="example-key", handle=handle)

    # Retrieve (zero-copy memoryview into CXL memory)
    result = handler.retrieve(key="example-key")

    # Check existence
    handler.exists(key="example-key")

    # Delete
    handler.delete(key="example-key")
```

### Key Behaviors

The following behaviors describe the default `cxl` path:

- **Context-manager connection**: Entering `with MaruHandler(config)` connects when `auto_connect=True` (default). Construction alone does not connect; outside a context manager call `connect()` explicitly.
- **Auto-expand**: Automatically allocates new memory regions when current ones are full
- **Deduplication**: Skips store if the key already exists
- **Zero-copy retrieve**: Returns memoryview pointing directly into mmap region
- **Concurrency**: The `cxl` write lock serializes store/delete. Reads can still perform RPC, mapping and synchronization; this is not a promise of lock-free execution.

For `remote_cxl`, retain the returned read lease until the last CPU read or GPU copy has completed, then release it with `handler.release_retrieved([result])`. Returning it early would allow its staging slot to be reused. See {doc}`config` and the {doc}`vLLM guide <../integration/vllm>` for backend configuration and buffer lifetimes.

### Backend-specific operations

| Operation | `cxl` | `cpu`, `mixed`, `remote_cxl` |
|---|---|---|
| `alloc`, `store`, `retrieve`, `exists` and their batch forms | Directly mapped pages | Delegated to the selected storage client; reads carry leases |
| `delete`, `pin`, `unpin` and batch pin/unpin | Legacy key registry operations | The Handler raises `NotImplementedError`; a storage-session read lease is not a legacy key pin |
| DAX mapping accessors | Inspect the Handler's local CXL mappings | Do not expose the remote CXL mapping or a staging address as a DAX mapping |
| `release_retrieved` | No storage-session lease to release | Release each non-None result after its final read or GPU copy |

For multiple `remote_cxl` pools, pass the request's first chunk key as the same
`route_key` to allocation, store, lookup and retrieve batch calls. A region ID
alone is not an identifier across independent Resource Managers.

`healthcheck()` checks the metadata connection/heartbeat. It does not establish
that RDMA, a DAX payload read or a GPU restore completed successfully.

### Methods

```{eval-rst}
.. autoclass:: maru_handler.MaruHandler
   :members:
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
handler.store(key="example-key", handle=handle)
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

`MemoryInfo` is a view, not a storage-session read lease. Keep the key and
region valid through the last CPU read or GPU copy; the view object alone
does not pin the registered key.

```python
result = handler.retrieve(key="example-key")
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
