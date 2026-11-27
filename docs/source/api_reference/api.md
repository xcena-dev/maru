# Python API

## MaruHandler

MaruHandler stores and retrieves shared KV objects. MaruServer manages metadata for the selected backend (`maru-server --backend cxl` or `maru-server --backend node`). The backend determines where allocation and read buffers live.

```python
from maru import MaruConfig, MaruHandler
```

### Usage

This example and the `AllocHandle`/`MemoryInfo` descriptions below use the default **`cxl` backend**. A returned memoryview points into a directly mapped CXL region. On the node backend (selected by `maru_node`), the StorageService owns the pool. A remote worker reads directly into its registered staging and returns a result that must stay owned through the final GPU copy. See {doc}`../design_doc/node_storage_service`.

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

The following behaviors describe the default `cxl` backend:

- **Context-manager connection**: Entering `with MaruHandler(config)` connects when `auto_connect=True` (default). Construction alone does not connect; outside a context manager call `connect()` explicitly.
- **Auto-expand**: Automatically allocates new memory regions when current ones are full
- **Deduplication**: Skips store if the key already exists
- **Zero-copy retrieve**: Returns memoryview pointing directly into mmap region
- **Concurrency**: The `cxl` write lock serializes store/delete. Reads can still perform RPC, mapping and synchronization; this is not a promise of lock-free execution.

For node reads, retain each result until its final CPU read or GPU copy completes, then call `handler.release_retrieved([result])`.

### Backend-specific operations

| Operation | `cxl` | node |
|---|---|---|
| `alloc`, `store`, `retrieve`, `exists` and batch forms | Directly mapped CXL | Service credit, Catalog operations on keys, and a mapped pool or worker RDMA |
| `delete`, `pin`, `unpin` and batch pin/unpin | Key-registry operations | Handler raises `NotImplementedError`; Catalog source pins are managed by the read path |
| DAX mapping accessors | Handler's CXL mappings | Do not expose a remote DAX mapping |
| `release_retrieved` | No result ownership to release | Release results after the final read or GPU copy |

`healthcheck()` checks the metadata connection/heartbeat. It does not establish that RDMA, a DAX payload read or a GPU restore completed successfully.

### Methods

```{eval-rst}
.. autoclass:: maru_handler.MaruHandler
   :members:
   :no-undoc-members:
   :class-doc-from: init
```

---

## AllocHandle

With `cxl`, `alloc()` returns a writable memoryview into a mapped CXL page. The caller fills `buf`, then passes the handle to `store()` to register the key without an additional payload copy. Filling `buf` from an existing byte array or GPU tensor still copies bytes. Node allocations use service credit; remote node allocations expose worker staging and storing transfers those bytes with direct RDMA WRITE.

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

A `cxl` read result from `retrieve()`, with a memoryview pointing directly into an mmap'd CXL region. Obtaining the view adds no payload copy; converting the view to `bytes`, as below, does. A remote node read returns an owned result over a worker staging slot populated by RDMA READ.

`MemoryInfo` describes the `cxl` mapping. Keep the key and region valid through the last CPU read or GPU copy; the view object alone does not pin the registered key.

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
