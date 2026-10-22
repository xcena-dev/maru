# Consistency and Safety

---

## 1. Data Visibility

Maru publishes an object's location after the writer finishes writing its
payload. Once `store()` returns successfully, other instances can look up that
location through the metadata server.

This relies on **write-then-register** ordering. Every store completes these steps in sequence:

1. **Write** — the caller writes the payload into a page from `alloc()`, by CPU through `handle.buf` or by GPU DMA.
2. **Register** — `store()` records in the metadata registry that the key now maps to that page.

The caller must finish its write before calling `store()`. Publishing only after the write completes prevents readers from discovering an in-progress object through the registry, provided the written data is visible to the reader (see {ref}`cross-host-visibility`).

```mermaid
sequenceDiagram
    participant W as Writer (Instance A)
    participant CXL as CXL Shared Memory
    participant Meta as Metadata Registry
    participant R as Reader (Instance B)

    W->>CXL: 1. Write data to page (flush CPU writes)
    Note over CXL: Payload reached the device
    W->>Meta: 2. Register key -> location
    Note over Meta: Key now globally visible

    R->>Meta: 3. Lookup key
    Meta-->>R: location (region, offset)
    R->>CXL: 4. Read data (zero-copy, invalidate before CPU reads)
    Note over R: Complete data
```

The visibility point is when `register_kv` RPC completes. The server holds the
key in an in-memory registry protected by a lock, ensuring that concurrent
lookups always see a fully committed entry or no entry at all.

(cross-host-visibility)=
### Cross-Host Visibility

The ordering above assumes that writes reach the shared memory device and readers cannot consume stale host-cache copies. On a non-coherent multi-host CXL platform, metadata ordering alone does not establish those conditions. See {doc}`../getting_started/bios_setup` for the platform settings that GPU DMA writes depend on.

On Intel GNR, the BIOS option `Allocating Write Flows` controls where device writes to CXL memory, such as GPU DMA writes, land. With the default `Allocating` policy, a write can stay in the writing host's cache, so other hosts read stale data. `Non-Allocating` stops these writes from allocating new lines in the host cache. It does not change lines the host already caches, for example after CPU access to the same memory, so CPU access still needs the explicit write-back and invalidation below.

CPU access to a shared payload can leave cached copies that other hosts do not invalidate, so CPU writers and readers must write back and invalidate explicitly. `flush_range` from `maru_shm._cxl_flush` does both: it runs `clflush` over each cache line of the buffer, then `mfence`.

1. **Writer:** write the payload to `handle.buf`, call `flush_range(handle.buf)`, then `store()`.
2. **Reader:** `retrieve()`, call `flush_range(result.view)` to drop local cached copies, then read `result.view`.

The {doc}`../getting_started/quick_start` producer and consumer follow this pattern. The CPU-based `examples/basic/producer.py` and `consumer.py` are single-host examples and do not flush.

For multi-node use, validate the deployed transfer path across repeated updates and memory reuse, including any CPU access performed by the runtime or driver.

> **See also:** [MaruHandler Architecture](maru_handler.md)

---

## 2. Concurrency

Maru serializes at two levels:

```mermaid
flowchart LR
    subgraph Instance_A["Instance A"]
        SA["store / delete"] -->|"_write_lock"| RPC_A["RPC"]
        RA["retrieve"] --> RPC_A
    end

    subgraph Instance_B["Instance B"]
        SB["store / delete"] -->|"_write_lock"| RPC_B["RPC"]
        RB["retrieve"] --> RPC_B
    end

    subgraph Server["MaruServer"]
        RPC_A & RPC_B -->|"RLock"| Meta["KV Registry +\nAllocation Manager"]
    end

    style SA fill:#fff3e0,stroke:#e65100
    style SB fill:#fff3e0,stroke:#e65100
    style RA fill:#e8f5e9,stroke:#4a9
    style RB fill:#e8f5e9,stroke:#4a9
```

- **Client**: `_write_lock` serializes all writes (store, delete) within a
  single handler instance. Retrieve is lock-free.
- **Server**: A single `RLock` serializes all metadata mutations (register,
  delete, ref-count updates) across instances.

These two locks, combined with write-then-register ordering (Section 1),
produce the following guarantees:

| Scenario | Behavior |
|----------|----------|
| Same-key store (cross-instance) | First registration wins; losing writer frees its page |
| Different-key store (cross-instance) | Parallel — independent pages, independent server calls |
| Store within one instance | Serialized by client write lock |
| Store + retrieve (same key) | No partial read — key invisible until data committed |
| Concurrent retrieve | Lock-free on client; shared mappings need no synchronization |

### Registration timeouts and page reuse

A register timeout leaves its outcome unknown: the server may have committed
it or a worker may still be waiting to process it. The handler quarantines the
submitted page and rejects `free()` until the outcome is resolved. Missing
batch results have the same treatment. Unsubmitted pages can still be discarded
with `free()`.

When the server advertises `kv_page_generation`, the handler uses
`BATCH_REGISTER_AT_KV` with a monotonically increasing generation for each
submitted page. `BATCH_DELETE_AT_KV` includes the expected generation and region
allocation token. The server checks the generation, changes the directory and
updates allocation references under its locks. A successful deletion or
versioned `ABSENT` retires that generation even if the register has not arrived:

1. `delete(A, page=7, generation=1)` returns `ABSENT` and retires generation 1.
2. A delayed `register(A, page=7, generation=1)` returns `RETIRED`.
3. A reused page can register generation 2. Replaying the generation 1 deletion
   cannot remove generation 2, even when the key is also A.

Pinned registrations return `PINNED`; their pages remain allocated. Replaying a
live registration returns `REGISTERED` without incrementing its allocation
reference again. `DUPLICATE` means another allocation owns the key, so the
unpublished page can be freed; that generation is also retired against replay.
`CONFLICT`, an RPC failure or a missing result keeps the page quarantined.

The server retains one generation record per used page offset until its region
is actually freed, including deferred frees. Unknown regions and mismatched
allocation tokens reject versioned requests. This binds retirement records to
the allocation lifetime without retaining process session tombstones.

Servers without `kv_page_generation` use the existing registration RPCs.
Quarantined legacy registrations cannot be reclaimed from an `ABSENT` response,
since a late register could still publish them. The handler retains these pages
until close. The new conditional delete API requires `kv_delete_at`; legacy
key-only deletion retains its existing semantics.

(cxl-read-pins)=

### Read pins and deletion

The vLLM worker acquires a read pin immediately before mapping a CXL object.
A scheduler hit is a residency hint: an object deleted before the worker pins
it is reported as a load failure. Configure vLLM's
`kv_load_failure_policy="recompute"` to recompute that range.

With `kv_pin_session`, `BATCH_PIN_LOOKUP_KV` checks existence, increments the
session's pin count and returns locations under the KVManager lock. `prefix`
stops at the first miss; `independent` handles every key separately. The latter
also supports multiple pins per key through `times`. Anonymous legacy pins
have a separate count, so legacy unpins cannot consume session-owned pins.

```mermaid
sequenceDiagram
    participant W as vLLM worker
    participant S as MaruServer
    participant D as Deleting handler
    participant G as CUDA load stream
    W->>S: Pin lookup (session, keys)
    S-->>W: Pinned locations or misses
    W->>G: Wait for producing compute, then copy
    D->>S: Conditional delete
    S-->>D: PINNED (keep the page)
    G-->>W: Final copy event completed
    W->>S: Unpin this session's keys
    D->>S: Retry conditional delete
    S-->>D: DELETED (page may be reused)
```

`batch_retrieve_pinned()` returns `PinnedViews`, whose explicit `release()` is
idempotent. Python collection does not unpin: a CUDA kernel may still read the
mapping after the Python call returns. Inline packed loads release after their
load stream synchronizes; asynchronous and layerwise loads retain the lease
until the final copy event completes. Abort and shutdown drain copies before
release. The worker never performs an unpin RPC under its load-bookkeeping lock.

Unpins affect only the issuing session. `RELEASE_SESSION` removes its remaining
counts at handler close and rejects queued late pin requests. A timed-out pin
or unpin is not retried ambiguously; unresolved counts remain until session
close. There is no forced pin expiry or heartbeat timeout in this change:
crashed readers may leave pins until the server restarts. Legacy servers use
pin-before-lookup and retain unknown anonymous pin outcomes conservatively.

Load failures invalidate only the externally allocated token interval, excluding
the shared GPU prefix, compute tail and null block. When a contiguous prefix is
available in every layer, the worker copies it before reporting the missing
tail. A copy error invalidates the entire external interval. Scheduler probes
and per-step store probes recheck server residency; a deleted key can therefore
be stored again after recomputation. Two failed load attempts stop further Maru
loads for that request. Automatic eviction remains a separate feature.

---

## 3. Crash Recovery

Maru is designed so that metadata can be reconstructed after a crash. The
recovery strategy differs by component.

### 3.1 Recovery Sequence

```mermaid
sequenceDiagram
    participant RM as Resource Manager

    Note over RM: Resource Manager restart detected

    rect rgb(240, 248, 255)
        RM->>RM: Load last checkpoint (allocation map)
        RM->>RM: Replay WAL entries since checkpoint
        RM->>RM: Derive each pool's free list from its device and allocations
        RM->>RM: Verify CRC32 integrity on all metadata
        Note over RM: Resource Manager ready
    end
```

**Resource Manager** persists every allocation and free operation to a write-ahead log (WAL) before modifying in-memory state. Every 100 operations, a checkpoint is taken: the global allocation map of every pool, served or not, is saved with CRC32 integrity verification. On restart, the last checkpoint is loaded and any subsequent WAL entries are replayed, fully reconstructing the allocation state; each pool's free list is its device's data range minus its allocations. See the Resource Manager design document, §4, for how each device is then judged.

### 3.2 Client Crash Recovery

When a client process terminates unexpectedly (crash, kill, network partition),
its allocated memory regions must be reclaimed to prevent leaks.

| Component | Detection | Reclamation |
|-----------|-----------|-------------|
| Resource Manager | Reaper polls process liveness every 1 second | Orphaned regions returned to free list; WAL records the free operation |
| MaruServer | Deferred freeing state machine | Region freed only when both owner disconnected **and** KV reference count reaches zero |

The reaper defends against PID reuse by caching each client's process start
time at allocation time. If the PID is recycled by the OS, the start-time
mismatch triggers reclamation.

> **See also:** [MaruResourceManager Architecture](maru_resource_manager.md),
> [MaruServer Architecture](maru_server.md)

---

## 4. Failure Modes

| Failure | Impact | Recovery | Data Loss |
|---------|--------|----------|-----------|
| Client crash | Owned regions orphaned; keys become stale | Reaper + deferred freeing | None |
| Resource Manager crash | New region allocation blocked | WAL + checkpoint replay on restart | None |
| Network partition (client-server) | Affected client cannot store/retrieve | Client reconnects when network recovers | None |
| CXL device failure | All data on the device is lost | Not supported -- no cross-device replication | **Total** |
