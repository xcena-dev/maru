# Architecture Overview

Maru manages KV cache data in CXL shared memory, enabling cross-instance sharing across multiple nodes without data transfer.

---

## System Architecture

```mermaid
%%{ init: { "flowchart": { "curve": "linear" } } }%%
flowchart TB
    subgraph Instances[" "]
        direction LR
        subgraph SN["Server N"]
            direction TB
            V1(["LLM Instance"])
            H1{{"MaruHandler"}}
            V1 --- H1
        end
        subgraph S2["Server 2"]
            direction TB
            V2(["LLM Instance"])
            H2{{"MaruHandler"}}
            V2 --- H2
        end
        subgraph S1["Server 1"]
            direction TB
            V3(["LLM Instance"])
            H3{{"MaruHandler"}}
            V3 --- H3
        end
    end

    MS["MaruServer"]:::maru

    H1 <-.->|"metadata RPC"| MS
    H2 <-.->|"metadata RPC"| MS
    H3 <-.->|"metadata RPC"| MS

    subgraph Backend["Memory Backend"]
        direction LR
        subgraph DAX_Mode["DAX Mode (default)"]
            direction TB
            SHM["MaruShmClient"]:::shm
            RM["MaruResourceManager"]:::shm
            SHM -->|"IPC"| RM
        end
        subgraph Marufs_Mode["marufs Mode"]
            direction TB
            MFS["MarufsClient"]:::fs
            KFS["marufs kernel"]:::fs
            MFS -->|"syscall"| KFS
        end
    end

    MS -.->|"alloc / free"| SHM
    MS -.->|"alloc / free"| MFS

    subgraph CXL["CXL Shared Memory"]
        direction LR
        R0["Region 0"] ~~~ R1["Region 1"] ~~~ R2["Region 2"]
    end

    RM -.->|"manage"| CXL
    KFS -.->|"manage"| CXL

    H1 <==>|"mmap read / write"| CXL
    H2 <==>|"mmap read / write"| CXL
    H3 <==>|"mmap read / write"| CXL

    classDef maru fill:#f8cecc,stroke:#b85450,font-weight:bold
    classDef fs fill:#dae8fc,stroke:#6c8ebf,font-weight:bold
    classDef shm fill:#d5e8d4,stroke:#82b366,font-weight:bold
```

> **Control Plane** (dashed arrows) — KV metadata operations and region allocation.
>
> **Data Plane** (solid arrows) — direct access to CXL shared memory, zero-copy. The data path is identical regardless of control plane mode.

The system has three layers:

| Layer | Role | Components |
|-------|------|------------|
| **Client** | KV operations, page allocation, region mapping | MaruHandler |
| **Metadata** | Key registry, allocation lifecycle | MaruServer |
| **Memory** | Physical region alloc/free, mmap | MaruShmClient → MaruResourceManager (DAX mode) |
| | | MarufsClient → marufs kernel (marufs mode) |

Both the server (AllocationManager) and clients (DaxMapper) use the same memory backend. The backend is selected by the server's `mount_path` configuration: when set, `MarufsClient` is used; otherwise, `MaruShmClient` communicates with `MaruResourceManager`. In marufs mode, MaruResourceManager is not used — the marufs kernel manages CXL memory directly. The choice is transparent to MaruHandler and all upper layers.

---

## Key Design Properties

**Zero-copy data path.** Clients access KV data directly in shared memory — no server process ever touches the data path (dashed arrows in the diagram). The only traffic on the control plane is lightweight metadata; the data itself never moves. This strict control/data plane separation means data-path performance is bounded by memory bandwidth, not by software overhead.

**Per-application control plane.** Each application group runs its own MaruServer for metadata isolation (e.g., app A with 2 instances, app B with 3 instances). In DAX mode, a single MaruResourceManager manages the shared memory pool across all groups. In marufs mode, the marufs kernel manages CXL memory directly.

```mermaid
%%{ init: { "flowchart": { "curve": "linear" } } }%%
flowchart LR
    subgraph AppA["App A"]
        direction TB
        A1(["Instance 1"])
        A2(["Instance 2"])
    end

    subgraph AppB["App B"]
        direction TB
        B1(["Instance 1"])
        B2(["Instance 2"])
        B3(["Instance 3"])
    end

    MSA["MaruServer A"]:::maru
    MSB["MaruServer B"]:::maru

    A1 & A2 --> MSA
    B1 & B2 & B3 --> MSB

    subgraph Backend["Memory Backend"]
        direction TB
        RM["MaruResourceManager<br/>(DAX mode)"]:::rm
        KFS["marufs kernel<br/>(marufs mode)"]:::fs
    end

    MSA & MSB --> Backend

    subgraph CXL["CXL Shared Memory Pool"]
        direction TB
        R0["Region 0"] ~~~ R1["Region 1"] ~~~ R2["Region 2"] ~~~ R3["Region 3"]
    end

    Backend --> CXL

    classDef maru fill:#f8cecc,stroke:#b85450,font-weight:bold
    classDef rm fill:#d5e8d4,stroke:#82b366,font-weight:bold
    classDef fs fill:#dae8fc,stroke:#6c8ebf,font-weight:bold
```

**Pluggable memory backend.** The data access layer is isolated behind the `DaxMapper` abstraction, so the memory backend can change without affecting the control plane or upper layers. The default backend (`MaruShmClient`) accesses CXL devices directly via DAX. When the server is configured with a marufs mount path, `MarufsClient` is selected instead, accessing CXL memory through the marufs VFS — adding kernel-level permission control (via `perm_set_default` / `perm_grant`) without changing the data flow.

**Capability-based memory access.** In DAX mode, clients never open shared memory devices directly — the Resource Manager acts as a capability broker, issuing authorized handles that grant access to specific memory regions. In marufs mode, the kernel enforces access control via `perm_set_default` and `perm_grant`, restricting which processes can open and mmap region files. In both cases, clients are decoupled from the underlying memory technology.

---

## Data Flow

### Store

```mermaid
sequenceDiagram
    participant C as Caller
    participant H as MaruHandler
    participant CP as Control Plane
    participant CXL as CXL Shared Memory

    C->>H: store(key, data)
    H->>H: allocate page from owned region
    H->>CXL: write data directly (zero-copy)
    H->>CP: register key → location
    CP-->>H: OK
    H-->>C: success
```

Data is written to shared memory **before** the key is registered. Other instances can never observe a partial write — the key only becomes visible after the data is fully committed.

### Retrieve (cross-instance)

```mermaid
sequenceDiagram
    participant C as Caller
    participant H as MaruHandler
    participant CP as Control Plane
    participant CXL as CXL Shared Memory

    C->>H: retrieve(key)
    H->>CP: lookup key
    CP-->>H: location (region, offset, length)
    H->>H: map region if not yet mapped
    H->>CXL: direct read (zero-copy)
    H-->>C: data
```

Every retrieve requires one metadata lookup via the control plane. Once a region is mapped, the mapping is cached for subsequent accesses to the same region — only the first access to a given region incurs the mmap cost.

---

## Extensibility

MaruHandler is **framework-independent**. Its interface operates on string keys and memory views — a minimal, framework-neutral contract. Any inference framework can integrate with Maru by writing a thin adapter layer (typically under 200 lines) that converts framework-specific cache keys to strings and delegates to MaruHandler's store/retrieve API.

```mermaid
graph LR
    A[LMCache] -->|MaruConnector| D[MaruHandler]
    B[SGLang] -->|Future Adapter| D
    C[Other Framework] -->|Custom Adapter| D
    D --> E[MaruServer]
    D --> F[CXL Shared Memory]
```

> **See also:** [LMCache Integration](../integration/lmcache.md),
> [MaruHandler Design](maru_handler.md)

```{toctree}
:hidden:

Maru Handler <maru_handler>
Maru Server <maru_server>
Maru Resource Manager <maru_resource_manager>
Maru FS <maru_fs>
```
