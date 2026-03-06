# MaruHandler Architecture

The `MaruHandler` is the client-side library that applications (e.g., LMCache/vLLM) use to interact with Maru. It serves two roles:

1. **KV cache client** — provides `store`/`retrieve`/`exists`/`delete` APIs.
2. **Memory manager** — owns CXL memory regions, manages paged allocation within them, and auto-expands when space runs out.

Data flows directly between the handler and CXL shared memory via memory-mapped regions — neither the MaruServer nor the resource manager is involved in the data path.

## 1. Component Hierarchy

```mermaid
graph TB
    subgraph MaruHandler["MaruHandler"]
        direction TB

        RW["Data Read / Write"]

        subgraph RPC_box["RpcClient"]
            ZMQ["ZMQ Socket"]
            SER["Serializer"]
        end

        subgraph DM_box["DaxMapper"]
            SHM["Shared Memory Client"]
            MR["Mapped Regions"]
        end

        subgraph ORM_box["OwnedRegionManager"]
            ACTIVE["Active Region Tracking"]
            subgraph regions["Owned Regions"]
                OR1["Region 1"]
                OR2["Region 2"]
                ORN["Region N"]
            end
            subgraph allocators["Page Allocators"]
                PA1["Allocator 1"]
                PA2["Allocator 2"]
                PAN["Allocator N"]
            end
        end
    end

    OR1 --- PA1
    OR2 --- PA2
    ORN --- PAN

    ORM_box -->|"map region"| DM_box
    RW -->|"get buffer view"| DM_box
    RPC_box <-->|"ZMQ REQ/REP"| Server["MaruServer"]
    SHM -->|"mmap/munmap"| CXL["CXL Memory"]

    style RW fill:#fff3e0,stroke:#e65100
    style DM_box fill:#e8f5e9,stroke:#4a9
    style ORM_box fill:#dcedc8,stroke:#4a9
    style RPC_box fill:#e3f2fd,stroke:#1565c0
    style CXL fill:#fce4ec,stroke:#c62828
```

**MaruHandler** orchestrates the overall data flow. It is the sole owner of `RpcClient` and coordinates region expansion, deallocation, shared region mapping, and data read/write.

**RpcClient** handles all communication with the MaruServer over ZeroMQ. It serializes requests using a binary header plus MessagePack payload, and supports both synchronous and asynchronous modes. The handler is the sole owner of the RPC client — no other component communicates with the server directly.

**DaxMapper** manages the mmap/munmap lifecycle of CXL regions. It creates mapped region objects that provide buffer views for direct memory access. It performs bulk unmap of all regions (both owned and shared) on `close()`.

**OwnedRegionManager** tracks the set of regions owned by this client. Each region is paired 1:1 with a page allocator that manages free pages. The manager exposes add, allocate, and free operations, and returns all region IDs on `close()` so the handler can release them back to the MaruServer.

---

## 2. Memory Management

Each CXL region is divided into fixed-size pages. A page allocator tracks free pages using a free-list, providing O(1) allocation and deallocation.

```
Region (e.g. 256 MB)
│
├── Page 0   ← KV Entry A
├── Page 1   ← KV Entry B
├── Page 2   ← (free)
├── Page 3   ← KV Entry C
├── ...
└── Page N-1
```

### Multi-Region Expansion

A single handler can own multiple CXL regions. When the active region runs out of free pages, the OwnedRegionManager scans other owned regions. If all are full, the handler requests a new region from the MaruServer.

```mermaid
graph LR
    subgraph Client["MaruHandler"]
        subgraph ORM["OwnedRegionManager"]
            R1["Region 1<br/><b>FULL</b>"]
            R2["Region 2<br/><b>ACTIVE</b>"]
            R3["Region 3<br/><b>EMPTY</b>"]
        end
    end

    R1 --> CXL1["CXL Region 1 (256MB)"]
    R2 --> CXL2["CXL Region 2 (256MB)"]
    R3 --> CXL3["CXL Region 3 (256MB)"]

    style R1 fill:#ffcdd2,stroke:#c62828
    style R2 fill:#c8e6c9,stroke:#2e7d32
    style R3 fill:#e3f2fd,stroke:#1565c0
```

The allocation strategy follows a fast-path pattern: try the active region first, scan other regions if it is full, and expand via RPC only as a last resort.

---

## 3. Owned vs Shared Regions

Each handler owns one or more regions for writing, and may map other clients' regions on demand for cross-instance retrieval.

```mermaid
graph TB
    subgraph Client_A["Client A (MaruHandler)"]
        ORM_A["OwnedRegionManager<br/>Region 1"]
        Shared_A["DaxMapper<br/>Region 2 (on-demand)"]
    end

    subgraph Client_B["Client B (MaruHandler)"]
        ORM_B["OwnedRegionManager<br/>Region 2"]
        Shared_B["DaxMapper<br/>Region 1 (on-demand)"]
    end

    ORM_A -->|"mmap"| CXL1["CXL Region 1"]
    Shared_A -.->|"mmap (lazy)"| CXL2["CXL Region 2"]

    ORM_B -->|"mmap"| CXL2
    Shared_B -.->|"mmap (lazy)"| CXL1

    Server["MaruServer<br/>Global key-to-location registry"]

    Client_A <-->|"RPC"| Server
    Client_B <-->|"RPC"| Server

    style ORM_A fill:#c8e6c9,stroke:#2e7d32
    style ORM_B fill:#c8e6c9,stroke:#2e7d32
    style Shared_A fill:#fff3e0,stroke:#e65100
    style Shared_B fill:#fff3e0,stroke:#e65100
```

Owned regions are mapped during connection or region expansion, and managed by OwnedRegionManager with a page allocator for each. Shared regions are mapped by DaxMapper on demand during retrieve, when the requested key resides in another client's region. On `close()`, owned regions are returned to the MaruServer, while all mapped regions (both owned and shared) are bulk-unmapped by DaxMapper.

---

## 4. Data Flows

### 4.1 Store

```mermaid
sequenceDiagram
    participant C as Caller
    participant H as MaruHandler
    participant ORM as OwnedRegionManager
    participant DM as DaxMapper
    participant CP as MaruServer
    participant CXL as CXL Shared Memory

    C->>H: store(key, data)
    H->>H: Check if key already exists
    alt Key exists (local cache or server)
        H-->>C: success (skip, no overwrite)
    end
    H->>ORM: Allocate page
    alt All regions full
        H->>CP: Request new region
        CP-->>H: Region handle
        H->>DM: Map new region
    end
    ORM-->>H: (region, page)
    H->>DM: Get writable buffer view
    H->>CXL: Write data (zero-copy)
    H->>CP: Register key → location
    CP-->>H: OK
    H-->>C: success
```

If the key already exists (in the local cache or on the server), the store is **skipped without overwrite** — same key implies same content (content-addressed). Otherwise, data is written to shared memory **before** the key is registered. Other instances can never observe a partial write — the key only becomes visible after the data is fully committed.

### 4.2 Retrieve

```mermaid
sequenceDiagram
    participant C as Caller
    participant H as MaruHandler
    participant CP as MaruServer
    participant DM as DaxMapper
    participant CXL as CXL Shared Memory

    C->>H: retrieve(key)
    H->>CP: Lookup key
    CP-->>H: (region, offset, length)
    alt Region not yet mapped
        H->>DM: Map shared region
    end
    H->>DM: Get buffer view
    H->>CXL: Direct read (zero-copy)
    H-->>C: data
```

If the region is owned by this client, it is already mapped and can be used directly. If the region belongs to another client, DaxMapper maps it on demand. The region mapping is cached for subsequent accesses.

### 4.3 Delete

```mermaid
sequenceDiagram
    participant C as Caller
    participant H as MaruHandler
    participant ORM as OwnedRegionManager
    participant CP as MaruServer

    C->>H: delete(key)
    H->>CP: Delete KV entry
    CP->>CP: Decrement ref count
    CP-->>H: OK
    H->>H: Remove from local cache
    alt Key stored by this client
        H->>ORM: Free page for reuse
    end
    H-->>C: success
```

> **See also:** [Architecture Overview](architecture_overview.md) — system-level data flows;
> [Memory Model](memory_model.md) — page allocation and region lifecycle;
> [KV Cache Management](kv_cache_management.md) — key semantics and location model;
> [Consistency and Safety](consistency_and_safety.md) — write-then-register ordering guarantees
