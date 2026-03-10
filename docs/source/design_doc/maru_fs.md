# marufs — Shared Filesystem Mode

> **Status**: Under active development, release coming soon.

## Motivation

Maru's architecture separates the **data plane** (direct zero-copy access to CXL shared memory) from the **control plane** (KV metadata registry and region lifecycle management). The control plane is pluggable — its implementation can change without affecting how data is read or written.

The first control plane implementation, **Remote mode**, uses a centralized MaruServer and Maru Resource Manager communicating over RPC:

```
┌─────────────┐     RPC      ┌─────────────┐     RPC      ┌──────────────────────┐
│  LMCache    │ ──────────── │  MaruServer  │ ──────────── │ Maru Resource Manager│
│  (Client)   │   (ZMQ)      │  (Python)    │              │         │
└─────────────┘              └─────────────┘              └──────────────────────┘
```

While functional, this design carries operational burdens:

| Problem | Description |
|---------|-------------|
| **Multi-process management** | MaruServer and Maru Resource Manager must be deployed and monitored separately |
| **RPC overhead** | Every KV lookup and region allocation goes through network serialization |
| **Single point of failure** | If MaruServer crashes, all KV metadata is lost |
| **No access control** | No authentication or authorization on the RPC endpoint — any process that can reach MaruServer can read, write, or delete any KV entry and region |
| **IPC complexity** | Multi-hop RPC protocol between MaruServer and Resource Manager is hard to debug and maintain |
| **Single-node only** | Remote mode assumes all instances share one CXL pool behind a single MaruServer; there is no built-in mechanism for cross-node KV sharing or federated metadata |

**Shared Filesystem mode (marufs)** is the second control plane implementation. It replaces all server-side processes with a single Linux kernel filesystem module (`marufs.ko`), enforcing memory access control at the kernel level for stronger security than user-space RPC.

```
┌─────────────┐   VFS + ioctl   ┌─────────────────┐
│  LMCache    │ ──────────────── │  marufs (kernel) │ ─── CXL Memory
│  (Client)   │   (syscall)      │                  │
└─────────────┘                  └─────────────────┘
```

The data plane remains identical — clients still access CXL memory directly via mmap. Only the control plane changes.

---

## Key Improvements over Remote Mode

### 1. Serverless Control Plane

Three separate server-side processes are consolidated into a single kernel module:

| Remote Mode Component | Role | Filesystem Mode Replacement |
|-------------|------|---------------|
| MaruServer (KVManager) | KV metadata registry | marufs global partitioned index (ioctl) |
| MaruServer (AllocationManager) | Region allocation tracking | POSIX VFS (`open` / `ftruncate` / `unlink`) |
| Maru Resource Manager | CXL memory pool, GC | marufs kernel module (GC kthread) |
| MaruShmClient (RPC) | Region handle issuance, memory mapping | MarufsClient (VFS + ioctl) |

Deployment is as simple as loading `marufs.ko` and mounting. No server processes to manage.

### 2. Kernel-level Access Control

Remote mode enforces memory access control in user-space via RPC — any process that can reach the server can potentially bypass authorization. Filesystem mode moves access control into the kernel:

- **Owner identification**: Each region tracks its owner by `(node_id, pid, birth_time)` triple. The kernel verifies the caller's identity on every access — a process cannot impersonate another instance even if it knows the region name.
- **Permission flags** (`PERM_READ`, `PERM_WRITE`, `PERM_DELETE`, `PERM_ADMIN`, `PERM_IOCTL`) are enforced at the VFS layer
- **Explicit grant model**: Non-owner access requires an explicit `perm_grant(node_id, pid, perms)` call from a process that holds `PERM_ADMIN`. Default permissions for new accessors are set via `perm_set_default`.
- **`PERM_ADMIN` delegation**: Owners implicitly hold all permissions including `PERM_ADMIN`. A process that receives `PERM_ADMIN` via grant can itself grant permissions to other processes, enabling delegation chains.
- **Default permissions** are set at region creation via `perm_set_default`
- The kernel mediates all region access — no way to bypass without kernel privilege

### 3. Kernel-managed Global KV Index

In Remote mode, KV metadata lives in a Python dictionary inside MaruServer — volatile and bottlenecked by RPC.

In Filesystem mode, the kernel maintains a **global partitioned index** directly in CXL memory:

- **O(1) lookup**: A single ioctl call returns `(region_name, byte_offset)`
- **Lock-free concurrency**: CAS-based hash table allows safe multi-instance concurrent access
- **Global search**: One ioctl searches across all regions — no region scanning needed
- **Batch operations**: Up to 512 keys per ioctl call for bulk lookup/registration

### 4. File-based Region Management

Region lifecycle is handled through ordinary file operations, eliminating the RPC round-trips:

| Operation | Remote Mode | Filesystem Mode |
|-----------|-------------|-----------------|
| Create region | RPC → MaruServer → Resource Manager | `open(O_CREAT)` + `ftruncate(size)` |
| Delete region | RPC → MaruServer → Resource Manager | `unlink(path)` |
| List regions | RPC → MaruServer | `listdir(mount_path)` |
| Map region | RPC → Resource Manager → handle → mmap | `open()` → `mmap()` |

---

## Metadata Structure and KV Cache Lookup

### On-disk Layout

marufs presents CXL shared memory as a flat directory of region files:

```
/mnt/marufs/                          ← mount point
├── maru_a1b2c3d4e5f6_0000           ← owned region (instance A, 1st region)
├── maru_a1b2c3d4e5f6_0001           ← owned region (instance A, 2nd region)
├── maru_f7e8d9c0b1a2_0000           ← owned region (instance B)
└── ...
```

Each region file is a contiguous CXL memory allocation, divided into fixed-size pages (chunks). A page holds exactly one KV cache entry. Region files are created with `open(O_CREAT) + ftruncate(pool_size)` and memory-mapped directly for zero-copy access.

### Global Partitioned Index

The kernel maintains a **global partitioned index** in CXL memory — a lock-free hash table that maps KV cache keys to their physical locations. Each entry (name-ref) contains:

```
┌──────────────────────────────────────────────────────────────────────┐
│  name-ref entry                                                      │
├───────────────────┬──────────────────────┬──────────────┬────────────┤
│ name (64B)        │ region_name (64B)    │ offset (8B)  │ hash (8B)  │
│ KV cache key      │ owning region file   │ byte offset  │ shard key  │
│ e.g. "model@1@0  │ e.g. "maru_a1b2c3   │ in region    │ for index  │
│  @3f8a...@fp16"   │  d4e5f6_0000"        │              │ partition  │
└───────────────────┴──────────────────────┴──────────────┴────────────┘
```

- **name**: The KV cache key string (`{model}@{token_range}@{world_id}@{chunk_hash}@{dtype}`). Keys ≤ 63 bytes are stored directly; longer keys are truncated with a SHA-256 hash suffix (`prefix#hash16`).
- **region_name**: The region file that holds the data.
- **offset**: Byte offset within the region where the data starts.
- **hash** (`name_hash`): Pre-computed hash used for shard selection. Upper 16 bits determine the index partition for concurrent access. Derived from `chunk_hash` field in the key with bit spreading to ensure non-zero upper bits.

The index is partitioned by `name_hash` upper bits, allowing CAS-based concurrent insertion and lookup without global locks.

### How KV Cache Lookup Works

A key lookup follows a three-level hierarchy:

```
┌─────────────────────────────────────────────────────────────┐
│ Level 1: Local Cache (_key_to_location)           O(1)      │
│   In-process dict: key → (region_name, page_index, hash)    │
│   Hit → skip ioctl, go directly to mmap read               │
├─────────────────────────────────────────────────────────────┤
│ Level 2: Kernel Global Index (ioctl)              O(1)      │
│   find_name(dir_fd, key, hash) → (region_name, offset)      │
│   Searches the CAS hash table in CXL memory                │
│   Result cached in Level 1 for future hits                  │
├─────────────────────────────────────────────────────────────┤
│ Level 3: CXL Memory (mmap)                        O(1)      │
│   map_shared_region(region_name) if first access            │
│   Direct memoryview read from mmap'd region at offset       │
└─────────────────────────────────────────────────────────────┘
```

**Retrieve path:**

1. **Local cache hit** → Return cached `(region_name, page_index)` without any syscall.
2. **Local cache miss** → Issue `MARUFS_IOC_FIND_NAME` ioctl. The kernel searches the global partitioned index and returns `(region_name, byte_offset)`. Cache the result locally.
3. **Region mapping** → If the region is not yet mmap'd, `open()` + `mmap()` it (one-time cost per region). The mapping is reused for all subsequent accesses to the same region.
4. **Data read** → Compute a memoryview slice into the mmap'd region at the given offset. Zero-copy — no data movement.

**Store path:**

1. **Allocate page** → `OwnedRegionManagerFs` returns `(region_name, page_index)` from the active owned region's free-list (O(1)).
2. **Write data** → `memmove()` into the mmap'd region at `page_index * chunk_size`. GIL-released via ctypes for concurrent I/O.
3. **Register name-ref** → `MARUFS_IOC_NAME_OFFSET` ioctl inserts the key into the global index via CAS. The key becomes visible to other instances only after this step completes.

**Batch operations** (`batch_store`, `batch_retrieve`, `batch_exists`) use `MARUFS_IOC_BATCH_FIND_NAME` and `MARUFS_IOC_BATCH_NAME_OFFSET` to process up to 32 keys per ioctl call (512 for registration), amortizing syscall overhead.

---

## Data Flows

The data plane (zero-copy mmap access to CXL memory) is shared across both modes. The diagrams below show Filesystem mode's control plane interactions.

### Store (saving KV cache)

```mermaid
sequenceDiagram
    participant C as LMCache
    participant H as MaruHandlerFs
    participant ORM as OwnedRegionManagerFs
    participant XM as MarufsMapper
    participant XC as MarufsClient
    participant K as marufs (kernel)
    participant CXL as CXL Memory

    C->>H: store(key, data)
    H->>ORM: allocate()
    ORM-->>H: (region_name, page_index)

    Note over H,CXL: Step 1 — Write to CXL (zero-copy, same as Remote mode)
    H->>XM: get_buffer_view(region_name, offset, size)
    XM-->>H: memoryview slice
    H->>CXL: buf[:] = data

    Note over H,K: Step 2 — Register in kernel global index
    H->>XC: name_offset(fd, key, byte_offset)
    XC->>K: ioctl(fd, MARUFS_IOC_NAME_OFFSET)
    Note over K: CAS insert into global index

    H-->>C: True
```

### Retrieve (cross-instance KV cache lookup)

```mermaid
sequenceDiagram
    participant C as LMCache (Instance B)
    participant H as MaruHandlerFs (B)
    participant XC as MarufsClient
    participant XM as MarufsMapper
    participant K as marufs (kernel)
    participant CXL as CXL Memory

    C->>H: retrieve(key)

    Note over H: Step 1 — Local cache miss
    H->>H: _key_to_location.get(key) → None

    Note over H,K: Step 2 — Kernel global index lookup
    H->>XC: find_name(dir_fd, key)
    XC->>K: ioctl(dir_fd, MARUFS_IOC_FIND_NAME)
    K-->>XC: (region_name, byte_offset)
    XC-->>H: (region_name, byte_offset)

    Note over H: Cache locally for future hits

    Note over H,CXL: Step 3 — Map shared region (first access only)
    H->>XM: map_shared_region(region_name)
    XM->>XC: open_region(name, readonly=True)
    XM->>XC: mmap_region(fd, size)

    Note over H,CXL: Step 4 — Read from CXL (zero-copy, same as Remote mode)
    H->>XM: get_buffer_view(region_name, offset, size)
    XM-->>H: memoryview
    H-->>C: MemoryInfo(view)
```

Subsequent retrievals of the same key hit the local cache (0 ioctl).
Subsequent accesses to the same region reuse the cached mmap (0 open/mmap).

### Delete

```mermaid
sequenceDiagram
    participant C as LMCache
    participant H as MaruHandlerFs
    participant XC as MarufsClient
    participant ORM as OwnedRegionManagerFs
    participant K as marufs (kernel)

    C->>H: delete(key)
    H->>H: _key_to_location.get(key) → (region_name, page_index)

    H->>XC: clear_name(fd, key)
    XC->>K: ioctl(fd, MARUFS_IOC_CLEAR_NAME)

    H->>ORM: free(region_name, page_index)
    H->>H: _key_to_location.pop(key)
    H-->>C: True
```

### Close / Cleanup

```mermaid
sequenceDiagram
    participant H as MaruHandlerFs
    participant XC as MarufsClient
    participant XM as MarufsMapper
    participant ORM as OwnedRegionManagerFs
    participant K as marufs (kernel)

    Note over H: Step 1 — Clear own name-refs from global index
    loop Each own key in _key_to_location
        H->>XC: clear_name(region_fd, key)
    end

    Note over H: Step 2 — Close allocators
    H->>ORM: close()

    Note over H: Step 3 — Unmap all regions
    H->>XM: close()
    Note over XM: cudaHostUnregister + munmap + close(fd)

    Note over H: Step 4 — Delete owned region files
    loop Each owned region
        H->>XC: delete_region(name)
        XC->>K: unlink(name)
        Note over K: GC reclaims CXL memory
    end

    Note over H: Step 5 — Close remaining fds
    H->>XC: close()
```

---

## Component Overview

Filesystem mode introduces three components that replace their Remote mode counterparts:

| Component | Replaces | Role |
|-----------|----------|------|
| **MarufsClient** | RpcClient + MaruShmClient | Wraps kernel filesystem interface — region CRUD via VFS, global index operations via ioctl, permission management. Caches file descriptors internally and validates region names against path traversal. |
| **MarufsMapper** | DaxMapper | Memory-mapping lifecycle manager. All regions are mapped with CUDA pinning for owned regions; shared region size is auto-detected via fstat. Performs bulk unmap on close. |
| **OwnedRegionManagerFs** | OwnedRegionManager | Page-level allocator for multiple owned regions, using the same O(1) free-list strategy as Remote mode. Allocation follows the same fast-path: active region → scan others → create new. |

---

## Concurrency Guarantees

All components are thread-safe. Write operations (store, delete, region map/unmap, allocation/free) are serialized. Read operations (retrieve, buffer view access) are lock-free. This follows the same concurrency model as Remote mode.

> **See also:** [Consistency and Safety](consistency_and_safety.md) — detailed concurrency semantics and guarantees across both modes

---

## Kernel Interface Reference

The marufs kernel module exposes its interface through standard VFS operations (open, close, mmap, unlink) and a set of ioctl commands for global index and permission management.

<details>
<summary>ioctl Command Table (click to expand)</summary>

| ioctl | nr | Direction | Size | Description |
|-------|----|-----------|------|-------------|
| `MARUFS_IOC_NAME_OFFSET` | 1 | `_IOW` | 80B | Register name-ref in global index |
| `MARUFS_IOC_FIND_NAME` | 2 | `_IOWR` | 144B | Global name lookup → (region_name, offset) |
| `MARUFS_IOC_CLEAR_NAME` | 3 | `_IOW` | 80B | Remove name-ref from global index |
| `MARUFS_IOC_BATCH_FIND_NAME` | 4 | `_IOWR` | 16B | Batch name lookup (up to 512/call) |
| `MARUFS_IOC_BATCH_NAME_OFFSET` | 6 | `_IOWR` | 16B | Batch name-ref registration (up to 512/call) |
| `MARUFS_IOC_PERM_GRANT` | 10 | `_IOW` | 16B | Grant permissions |
| `MARUFS_IOC_PERM_SET_DEFAULT` | 13 | `_IOW` | 16B | Set default permissions |
| `MARUFS_IOC_DMABUF_EXPORT` | 0x50 | `_IOWR` | 16B | DMA-BUF export |

Magic byte: `0x58` (ASCII `'X'`).

</details>

<details>
<summary>Key Structures (click to expand)</summary>

```c
#define MARUFS_NAME_MAX 63    // max name length

struct marufs_name_offset_req {           // 80 bytes
    char     name[MARUFS_NAME_MAX + 1];   // 64B — key string
    __le64   offset;                      // 8B  — byte offset in region
    __le64   name_hash;                   // 8B  — pre-computed hash (0 = djb2 fallback)
};

struct marufs_find_name_req {             // 144 bytes
    char     name[MARUFS_NAME_MAX + 1];         // 64B — input: search key
    char     region_name[MARUFS_NAME_MAX + 1];  // 64B — output: region filename
    __le64   offset;                            // 8B  — output: byte offset
    __le64   name_hash;                         // 8B  — pre-computed hash
};

struct marufs_perm_req {                  // 16 bytes
    __le32   node_id;                     // CXL node ID
    __le32   pid;                         // target process ID
    __le32   perms;                       // permission flags
    __le32   reserved;                    // alignment padding
};
```

</details>

<details>
<summary>Batch Structures (click to expand)</summary>

```c
struct marufs_batch_find_entry {          // per-entry
    char     name[MARUFS_NAME_MAX + 1];
    char     region_name[MARUFS_NAME_MAX + 1];
    __le64   offset;
    __le64   name_hash;
    __le32   status;                      // 0 = found, -ENOENT = not found
    __u8     _pad[4];
};

struct marufs_batch_find_req {            // header (16B)
    __le32   count;                       // number of entries
    __le32   found;                       // output: number found
    __le64   entries;                     // pointer to entry array
};
```

Batch limit: **512 entries** per ioctl call. The Python client automatically splits larger batches.

</details>

<details>
<summary>Permission Flags (click to expand)</summary>

| Flag | Value | Description |
|------|-------|-------------|
| `PERM_READ` | `0x0001` | Read access |
| `PERM_WRITE` | `0x0002` | Write access |
| `PERM_DELETE` | `0x0004` | Delete access |
| `PERM_ADMIN` | `0x0008` | Permission management — required for `perm_grant` and `perm_set_default` |
| `PERM_IOCTL` | `0x0010` | ioctl access |
| `PERM_ALL` | `0x001F` | All permissions |

</details>

---

## Known Issues

### 1. No exclusive open on `/dev/dax`

The Linux DAX device driver does not support exclusive open — multiple processes can open the same `/dev/dax` device simultaneously, bypassing marufs's permission model.

**Workaround:** Restrict `/dev/dax` device file permissions to root only (`chmod 600 /dev/dax*`). Only the marufs kernel module (running in kernel context) accesses the DAX device directly; user-space processes access CXL memory through marufs region files, where kernel-level permission enforcement applies.

### 2. `cudaHostRegister` requires read-write mapping

CUDA does not support `cudaHostRegisterReadOnly` — `cudaHostRegister` requires the host memory region to be mapped with both read and write permissions (`PROT_READ | PROT_WRITE`).

**Workaround:** When granting shared region access to a consumer instance, both `PERM_READ` and `PERM_WRITE` are granted, and the region is mmap'd with `PROT_READ | PROT_WRITE`. This means a consumer holds write permission to another instance's owned region at the mmap level. While marufs's ioctl-level operations (store/delete) are still gated by the handler logic, a buggy consumer could theoretically corrupt shared data through direct memory writes. This trade-off is accepted for CUDA compatibility until `cudaHostRegisterReadOnly` or an equivalent mechanism becomes available.

### 3. Cross-node duplicate key in prefix caching

In multi-node prefix caching, multiple nodes may independently compute and store the same KV cache key (e.g., a shared prompt prefix). The global index maps each name to exactly one `(region_name, offset)` — the second `NAME_OFFSET` ioctl silently overwrites the first entry, leaving the original node's page allocated but unreferenced (memory leak on that node).

**Approach:** Include node-identifying information in the key name (e.g., `"{original_key}:{instance_id}"`) so that each node's entry is distinct in the global index. This avoids kernel changes while keeping keys unique. The alternative — allowing the global index to store multiple entries per name (`FIND_NAME` returning an array) — would require significant kernel UAPI changes and is considered overkill for this use case.
