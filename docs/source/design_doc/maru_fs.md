# marufs — Shared Filesystem Mode

> **Status**: VFS backend mode implemented and operational.

## Motivation

Maru's architecture separates the **data plane** (direct zero-copy access to CXL shared memory) from the **control plane** (KV metadata registry and region lifecycle management).

The first data plane implementation, **DAX mode**, uses MaruShmClient to access CXL memory via `/dev/dax` devices through the Maru Resource Manager daemon:

```mermaid
graph LR
    A[LMCache<br/>Client] -->|RPC<br/>ZMQ| B[MaruServer]
    B -->|RPC| C[Maru Resource Manager]
    C --- D["/dev/dax<br/>CXL Memory"]
```

### Why a filesystem?

The fundamental limitation of DAX mode is **security**. Clients access CXL shared memory by directly `mmap`-ing `/dev/dax` devices. The Linux DAX driver provides no isolation — any process that can open the device has unrestricted read-write access to the entire CXL memory pool. There is no way to enforce per-region or per-instance access control at the hardware or driver level.

A **filesystem** is the right solution because Maru's data model maps naturally onto it: each CXL memory region is a file, and each file has its own inode. The VFS layer already provides per-inode ownership, permission checks on `open` and `mmap`, and fd-scoped access — exactly the per-region security granularity Maru needs.

The underlying `/dev/dax` device is still used — but only the marufs kernel module accesses it directly. User-space processes access CXL memory through marufs region files, and the kernel mediates every `open`, `mmap`, and `ioctl`.

### DAX Mode Limitations

| Label | Problem | Description |
|-------|---------|-------------|
| **M1** | **No access control** | `/dev/dax` direct mmap — no per-region security enforcement |
| **M2** | **Multi-process management** | MaruServer and Maru Resource Manager must be deployed separately |

**marufs mode** replaces the DAX device path with a kernel filesystem, enabling **per-region kernel-level access control** while keeping the same RPC control plane.

```mermaid
graph LR
    A[LMCache<br/>Client] -->|RPC<br/>ZMQ| B[MaruServer]
    B -->|VFS| C[marufs<br/>kernel module]
    C --- D[CXL Memory]
    A -->|mmap<br/>VFS| C
```

---

## Architecture

### What changes from DAX mode

| Concern | DAX Mode | marufs Mode |
|---------|----------|-------------|
| Region allocation | MaruShmClient → Resource Manager → `/dev/dax` | MarufsClient → `open(O_CREAT)` + `ftruncate(size)` |
| Region mapping | MaruShmClient.mmap() → Resource Manager | MarufsClient → `open(region_file)` + `mmap(fd)` |
| Region deletion | MaruShmClient → Resource Manager | `close(fd)` + `unlink(path)` |
| Access control | **None** (`/dev/dax` wide open) | **Kernel VFS** — per-region permission checks on `open`/`mmap` |
| Server process | MaruServer + Resource Manager | MaruServer only (Resource Manager 불필요) — **addresses M2** |
| KV metadata | MaruServer KVManager | MaruServer KVManager — **동일** |

### Mode Auto-Detection

Server signals its backend mode via the `mount_path` field in `request_alloc` RPC response:

- `mount_path = None` → DAX mode (client uses MaruShmClient)
- `mount_path = "/mnt/marufs"` → marufs mode (client uses MarufsClient)

Clients don't need any configuration change — DaxMapper automatically selects the appropriate backend based on server response.

### Component Mapping

| Component | DAX Mode | marufs Mode |
|-----------|----------|-------------|
| **MaruServer** | AllocationManager(MaruShmClient) | AllocationManager(**MarufsClient**) |
| **DaxMapper** | MaruShmClient | **MarufsClient** (auto-detected) |
| **RPC** | ZMQ client-server | ZMQ client-server — 동일 |
| **OwnedRegionManager** | PagedMemoryAllocator | PagedMemoryAllocator — 동일 |
| **MaruHandler** | Same handler | Same handler — 동일 |

---

## Kernel-level Access Control — addresses M1

DAX mode has no access control — any process can mmap `/dev/dax`. marufs moves access control into the kernel:

- **Per-region permissions**: Each region file has its own permission state, enforced by the kernel on `open` and `mmap`
- **Owner identification**: Each region tracks its owner by `(node_id, pid, birth_time)` triple. The kernel verifies the caller's identity on every access
- **Permission flags**: `PERM_READ`, `PERM_WRITE`, `PERM_DELETE`, `PERM_ADMIN`, `PERM_IOCTL` — enforced at the VFS layer
- **Default permissions**: Set at region creation via `perm_set_default` ioctl. Current implementation uses `PERM_ALL` for simplicity; future work will implement least-privilege defaults with explicit grants
- **Explicit grant model**: `perm_grant(node_id, pid, perms)` allows fine-grained per-process permission delegation. Requires `PERM_ADMIN`
- **`PERM_ADMIN` delegation**: Owners implicitly hold all permissions. A process that receives `PERM_ADMIN` via grant can itself grant permissions to other processes

### Current Permission Model

In the current implementation, `perm_set_default(PERM_ALL)` is called during region allocation — all processes can access all regions. This is acceptable for single-tenant deployments.

### Planned Permission Model

For multi-tenant or security-sensitive environments, the planned model is:

1. Region created with `perm_set_default(PERM_READ)` or no default permissions
2. Authenticated instances receive explicit `perm_grant` after certificate verification
3. See [Instance Authentication Design](instance_auth_design.md) for the authentication design

---

## Operation Flows

### Init (connect)

```mermaid
sequenceDiagram
    participant C as LMCache
    participant H as MaruHandler
    participant RPC as RpcClient
    participant S as MaruServer
    participant AM as AllocationManager
    participant XC as MarufsClient
    participant K as marufs (kernel)
    participant CXL as CXL Memory

    C->>H: connect()

    Note over H,S: Step 1 — Request initial region via RPC
    H->>RPC: request_alloc(instance_id, size)
    RPC->>S: ZMQ request
    S->>AM: request_alloc(instance_id, size)
    AM->>XC: alloc(size)
    XC->>K: open(O_CREAT | O_RDWR) + ftruncate(size)
    Note over K,CXL: Allocate contiguous CXL memory
    XC->>K: perm_set_default(fd, PERM_ALL)
    AM-->>S: MaruHandle
    S-->>RPC: {handle, mount_path="/mnt/marufs"}

    Note over H: Step 2 — Auto-detect mode from mount_path
    H->>H: DaxMapper(mount_path="/mnt/marufs") → MarufsClient

    Note over H,CXL: Step 3 — Map owned region
    H->>H: DaxMapper.map_region(handle)
    H->>XC: open(region_file) + mmap(fd, size)
    H->>H: OwnedRegionManager.add_region(handle)

    H-->>C: True
```

### Store (saving KV cache)

```mermaid
sequenceDiagram
    participant C as LMCache
    participant H as MaruHandler
    participant ORM as OwnedRegionManager
    participant DM as DaxMapper
    participant RPC as RpcClient
    participant S as MaruServer
    participant CXL as CXL Memory

    C->>H: store(key, data)
    H->>ORM: allocate()
    ORM-->>H: (region_id, page_index)

    Note over H,CXL: Step 1 — Write to CXL (zero-copy via mmap)
    H->>DM: get_buffer_view(region_id, offset, size)
    DM-->>H: memoryview slice
    H->>CXL: _gil_free_memcpy(buf, data)

    Note over H,S: Step 2 — Register KV metadata via RPC
    H->>RPC: register_kv(key, region_id, offset, length)
    RPC->>S: ZMQ request
    S->>S: KVManager.register(key, region_id, offset, length)

    H-->>C: True
```

### Retrieve (cross-instance KV cache lookup)

```mermaid
sequenceDiagram
    participant C as LMCache (Instance B)
    participant H as MaruHandler (B)
    participant DM as DaxMapper
    participant XC as MarufsClient
    participant RPC as RpcClient
    participant S as MaruServer
    participant K as marufs (kernel)
    participant CXL as CXL Memory

    C->>H: retrieve(key)

    Note over H,S: Step 1 — KV metadata lookup via RPC
    H->>RPC: lookup_kv(key)
    RPC->>S: ZMQ request
    S->>S: KVManager.lookup(key)
    S-->>RPC: {handle, kv_offset, kv_length}

    Note over H,K: Step 2 — Map shared region (first access only)
    H->>DM: map_region(handle)
    DM->>XC: open(region_file) + mmap(fd, size)
    Note over K: Kernel checks permissions on open/mmap

    Note over H,CXL: Step 3 — Read from CXL (zero-copy)
    H->>DM: get_buffer_view(region_id, offset, size)
    DM-->>H: memoryview
    H-->>C: MemoryInfo(view)
```

Subsequent accesses to the same region reuse the cached mmap (0 open/mmap overhead).

### Close / Cleanup

```mermaid
sequenceDiagram
    participant H as MaruHandler
    participant ORM as OwnedRegionManager
    participant DM as DaxMapper
    participant RPC as RpcClient
    participant S as MaruServer

    Note over H: Step 1 — Close allocators
    H->>ORM: close() → list of region_ids

    Note over H: Step 2 — Return allocations to server
    loop Each owned region_id
        H->>RPC: return_alloc(instance_id, region_id)
    end

    Note over H: Step 3 — Unmap all regions + close client
    H->>DM: close()
    Note over DM: munmap + close(fd) for all regions

    Note over H: Step 4 — Disconnect RPC
    H->>RPC: close()
```

---

## Component Overview

```mermaid
graph TB
    subgraph "LLM Instance (vLLM)"
        LM[LMCache]
        subgraph "Maru Handler"
            H[MaruHandler]
            ORM[OwnedRegionManager<br/>page allocator]
            DM[DaxMapper<br/>mmap + CUDA pin]
            XC["MarufsClient<br/>(or MaruShmClient)"]
            RPC[RpcClient<br/>ZMQ]
        end
    end

    subgraph "Server Process"
        S[MaruServer]
        KV[KVManager<br/>in-memory dict]
        AM[AllocationManager<br/>MarufsClient]
    end

    subgraph "Kernel"
        K[marufs.ko<br/>kernel module]
    end

    CXL[(CXL Memory)]

    LM --> H
    H --> ORM
    H --> DM
    H --> RPC
    ORM --> DM
    DM --> XC
    RPC -->|ZMQ| S
    S --> KV
    S --> AM
    AM -->|VFS| K
    XC -->|VFS| K
    K --- CXL
```

---

## Kernel Interface Reference

### VFS Operations

| Operation | Usage |
|-----------|-------|
| `open(O_CREAT \| O_RDWR)` | Create region file (server-side allocation) |
| `ftruncate(fd, size)` | Set region size |
| `open(O_RDWR)` | Open existing region (client-side mapping) |
| `mmap(fd, size, prot)` | Map region into process address space |
| `munmap(addr, size)` | Unmap region |
| `close(fd)` | Close file descriptor |
| `unlink(path)` | Delete region file |

### Permission ioctl

| ioctl | Description |
|-------|-------------|
| `MARUFS_IOC_PERM_SET_DEFAULT` | Set default permissions for new accessors |
| `MARUFS_IOC_PERM_GRANT` | Grant permissions to specific process (node_id, pid) |

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

<details>
<summary>Permission Structures (click to expand)</summary>

```c
struct marufs_perm_req {                  // 16 bytes
    __le32   node_id;                     // CXL node ID
    __le32   pid;                         // target process ID
    __le32   perms;                       // permission flags
    __le32   reserved;                    // alignment padding
};
```

</details>

---

## On-disk Layout

marufs presents CXL shared memory as a flat directory of region files:

```
/mnt/marufs/                          ← mount point
├── region_0                          ← owned region (instance A)
├── region_1                          ← owned region (instance A, 2nd)
├── region_2                          ← owned region (instance B)
└── ...
```

Each region file is a physically contiguous CXL memory allocation. A single region holds multiple KV cache entries, each identified by a byte offset within the region. Region files are created with `open(O_CREAT) + ftruncate(size)` and memory-mapped directly for zero-copy access.

---

## Multi-Node Considerations

### CXL Shared Memory Across Nodes

CXL Type 3 장치는 여러 노드가 동일한 물리 메모리 풀을 공유할 수 있다. marufs는 **모든 메타데이터(inode, dentry, global index)를 CXL 메모리에 저장**하므로, 각 노드에서 동일한 CXL 디바이스로 marufs를 마운트하면 모든 region 파일이 보인다.

```
[CXL Shared Memory Pool]
├── marufs 메타데이터 (inode, dentry)   ← CXL에 저장
└── region 데이터 (KV cache)            ← CXL에 저장

[Node A] mount -t marufs /dev/dax0.0 /mnt/marufs  → 전체 파일 보임
[Node B] mount -t marufs /dev/dax1.0 /mnt/marufs  → 전체 파일 보임 (같은 CXL 풀)
```

### 잠재적 문제와 해결

멀티 노드에서 동일 CXL 메모리에 동시 접근할 때 세 가지 문제가 발생할 수 있다:

| 문제 | 설명 | 해결 |
|------|------|------|
| **CAS atomicity** | 여러 노드가 동시에 메타데이터를 수정하면 CXL fabric을 넘는 CAS 연산이 atomic하지 않을 수 있음 | MaruServer 단일 인스턴스가 모든 메타데이터 수정을 직렬화 |
| **캐시 코히런시** | 한 노드의 CPU 캐시에 있는 CXL 데이터가 다른 노드의 수정을 반영하지 못할 수 있음 | `clflushopt`/`clwb` 매크로로 CXL 메모리에 명시적 flush |
| **VFS 캐시 stale** | Node A가 생성한 파일을 Node B의 커널 VFS 캐시가 인식하지 못할 수 있음 | `d_revalidate() → 0`으로 항상 CXL에서 재검색 (아래 참조) |

### VFS 캐시 무효화 설계

marufs 커널 모듈은 **"No metadata caching"** 원칙으로 설계되어 cross-node visibility 문제를 원천 차단한다:

- **dentry 캐시**: `d_revalidate()`가 항상 0을 리턴 → VFS가 매 `open()`마다 `marufs_lookup()`을 호출하여 CXL 메모리에서 직접 검색
- **inode 캐시**: `marufs_iget()`이 매번 CXL의 hot/cold entry에서 직접 읽음 → DRAM 캐시 없음

```c
// dir.c — d_revalidate always returns 0, forcing re-lookup from CXL memory
static int marufs_d_revalidate(...)
{
    return 0;  // Always invalid → VFS re-does lookup from CXL
}
```

### 멀티 노드 동작 흐름

```mermaid
sequenceDiagram
    participant S as MaruServer (Node A)
    participant K_A as marufs kernel (Node A)
    participant CXL as CXL Shared Memory
    participant K_B as marufs kernel (Node B)
    participant C as Client (Node B)

    Note over S,CXL: Step 1 — Server creates region (Node A)
    S->>K_A: open(O_CREAT) + ftruncate(size)
    K_A->>CXL: 메타데이터 + 데이터 영역 할당
    S->>K_A: perm_set_default(PERM_ALL)
    S->>K_A: clflushopt (CXL에 flush)

    Note over S,C: Step 2 — Client gets handle via RPC
    S-->>C: {handle, mount_path} (ZMQ response)
    Note over C: RPC 응답 수신 = 서버 flush 완료 보장 (ordering barrier)

    Note over C,CXL: Step 3 — Client opens region (Node B)
    C->>K_B: open("/mnt/marufs/region_42")
    K_B->>K_B: d_revalidate() → 0 (캐시 무효)
    K_B->>CXL: marufs_lookup() — CXL에서 직접 검색
    K_B-->>C: fd
    C->>K_B: mmap(fd, size)
    Note over C,CXL: Zero-copy CXL 메모리 접근
```

**핵심 보장**: RPC 응답 자체가 ordering barrier 역할 — Client가 `open()`하는 시점은 Server의 `open + ftruncate + flush`가 반드시 완료된 이후이므로, CXL 메타데이터는 항상 최신 상태다.

### 전제 조건

- MaruServer는 **단일 인스턴스**로 운영 — 여러 MaruServer가 동시에 파일을 생성하면 CAS 직렬화 보장이 깨짐
- 모든 노드에서 동일한 CXL 디바이스(풀)로 marufs를 마운트
- MaruServer의 RPC 주소는 TCP로 설정 (UDS는 싱글 노드 전용)

---

## Known Issues

### 1. No exclusive open on `/dev/dax`

The Linux DAX device driver does not support exclusive open — multiple processes can open the same `/dev/dax` device simultaneously, bypassing marufs's permission model.

**Workaround:** Restrict `/dev/dax` device file permissions to root only (`chmod 600 /dev/dax*`). Only the marufs kernel module (running in kernel context) accesses the DAX device directly; user-space processes access CXL memory through marufs region files.

### 2. `cudaHostRegister` requires read-write mapping

CUDA does not support `cudaHostRegisterReadOnly` — `cudaHostRegister` requires `PROT_READ | PROT_WRITE`.

**Current behavior:** All regions are mapped with `PROT_READ | PROT_WRITE`. Fine-grained read-only mapping for shared regions is planned.

### 3. CXL pool fragmentation

Region creation and deletion over time can fragment the CXL memory pool. Within a region, page-level allocation/free does not cause external fragmentation (pages are fixed-size). However, at the pool level, repeated region create/delete cycles can fragment the CXL address space.
