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

---

(maru-handler-remote-cxl)=
## 5. Remote CXL Backend

With `storage_backend="remote_cxl"` the Handler keeps its KV in the CXL memory
of memory nodes and moves it over RDMA. It gets regions the way a cxl Handler
does, but through a storage session: one region when the session opens and one
more (`expand`) whenever all of them are full. The region mapper on a region's
memory node maps the region and registers it with that node's NIC; the Handler
reaches the region with one-sided RDMA.

![Components of the remote_cxl backend: worker nodes, the metadata node and the memory nodes of a shared CXL pool](../image/remote_cxl_sw_structure.png)

| Component | Role |
|-----------|------|
| `RemoteCxlStorageClient` | The storage session: regions, pages, stores, loads, eviction |
| `RdmaDataPath` | The worker's staging buffer, its NIXL agent and its connections to the region mappers |
| `RegionMapClient` | One connection to a region mapper: proves the region mapper key, then `map`, `unmap`, `release` |
| `StagingBuffer` | Host memory registered with the NIC and with CUDA; KV moves GPU ↔ staging by copy, staging ↔ region by RDMA |
| `PoolRouter` | Several pools (one MaruServer each): routes a request to one pool by its first chunk key |

**Store.** The Handler takes a staging slot and a free page of an own region,
the Connector copies the KV into the slot, and the Handler WRITEs it to the
page. It commits the key only after the WRITE completed. A WRITE whose end is
not confirmed keeps its page and slot out of use until NIXL releases the
transfer, and the page of a WRITE that ended in an error stays out of use
while the Handler owns the region (that WRITE may still be in the network,
and the memory node's NIC still accepts the region's key), so a late WRITE
never lands on reused memory.

**Retrieve.** The Handler takes a server lease on the keys (`acquire`), which
returns each key's location and, for another worker's region, a *read handle*.
It maps that region at its region mapper on first use, READs the pages into
staging slots, and ends the lease (`release`). It uses the bytes only if the
lease was held to the end. A location longer than a staging slot, or past the
end of its region, loads as a miss without a READ. A `release` whose reply is
lost is sent again by the heartbeat thread, so the lease does not keep the
owner from evicting the key.

**Eviction.** When its regions are full and the Resource Manager has no room,
the Handler asks MaruServer for its least recently used unleased keys
(`victims`) and removes them. A page is reused only after MaruServer
confirmed that no key refers to it.

**A memory node that stops answering.** A region whose region mapper cannot
map it is set aside, and a background retry maps it again. If the retry
succeeds within 30 seconds (a region mapper restart), the region is used
again with its keys; otherwise its keys are removed, so probes stop reporting
hits that would fail to read, as MaruServer does for a region it took over.
A region mapper that left a request unanswered stays so until a later request
to it (a map, or the heartbeat's boot check every 10 seconds) is answered.
Until then stores take no pages in its regions, eviction picks no keys there
and no new region is asked for; with no other region the store is skipped,
since no WRITE could reach the freed pages. When the memory node answers again, the Handler maps its regions anew.
Before it loads a map reply into a NIXL peer (NIXL's record of a remote agent:
its metadata and its UCX connection) that it already knows, it checks that
the connection has not failed and otherwise drops the peer and connects anew.
UCX fails an unanswered connection on its own, even an idle one, and NIXL
1.2.0 keeps the closed connection in the peer, so loading into it would crash
the process:

![A worker maps region R2 after its region mapper stopped answering and answers again. (a) Without the check, R2's metadata is loaded into the peer that still holds the closed connection, and the worker process crashes. (b) The Handler checks the peer first; the check is refused, so it drops the peer, connects anew and loads R2's metadata, and RDMA of R2 works](../image/remote_cxl_failed_peer.png)

**Giving regions back.** A region goes back to the Resource Manager only after
its region mapper deregistered it from the NIC (`release`):

![The resource flow is the same for both backends; what keeps a returned region safe from late writes differs. A cxl Handler's copies are done when it returns a region. A remote_cxl Handler's RDMA WRITE may still be in flight, so the region mapper deregisters the region first and a late WRITE fails at the NIC](../image/remote_cxl_return_rule.png)

- A region the Handler gives back while it runs (for example a new region
  whose memory node it cannot reach) is released first, then returned with
  `return_region`.
- A region whose memory node rebooted (its region mapper reports a new boot
  id) goes back the same way at once, since bytes that sat in the node's CPU
  cache may be lost. MaruServer then stops finding its keys and ends the
  leases on them, so a reader holding one drops what it read; the next store
  gets a new region.
- When the Handler leaves (it closes, or its session expired), it waits for
  its WRITEs, closes the session drained and drops its holds at the region
  mappers. MaruServer takes its regions over: it keeps those that hold keys,
  so other workers and the restarted worker keep reading them, and releases
  the others before it frees them.
- If MaruServer restarted, nobody takes the regions over, so the Handler
  releases them itself.

**Staging slots.** Loads and stores share the staging buffer. With
asynchronous transfers, stores leave a share of the slots free for loads:

![Staging slots: half of the slots are kept free for loads; stores take the rest](../image/remote_cxl_staging_slots.png)

> **See also:** [Architecture Overview](architecture_overview.md),
> [Memory Model](memory_model.md),
> [KV Cache Management](kv_cache_management.md),
> [Consistency and Safety](consistency_and_safety.md)
