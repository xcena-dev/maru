# Maru KV Cache Management

Maru stores KV cache data as fixed-size units called **KV cache chunks**. Each chunk occupies exactly one page in shared memory and corresponds to a single key. The chunk size is determined by Maru's `chunk_size_bytes` configuration, which must match the caller's KV chunk size.

## Backend, identity and sharing

The numbered sections below describe the legacy `cxl` key registry. Storage
sessions (`cpu`, `mixed`, `remote_cxl`) use `ReplicaDirectory` instead of
`LOOKUP_KV` and can represent typed replicas and read leases.

| Backend | Which cached object can be reused? | Lookup and read lifetime |
|---|---|---|
| `cxl` | A key in the legacy registry whose CXL region the reader can map | Lookup returns a region handle and byte range |
| `remote_cxl` | A compatible object in the same pool and cache namespace | `probe` finds a candidate; `acquire` grants a server lease, RDMA fills staging, and a local lease protects the copy to the GPU |
| `cpu`, current `mixed` | A replica owned by the requesting worker | Same-namespace objects on another worker are not automatically accessible |

For storage sessions, `cache_namespace` must identify the exact immutable
weights revision. Equal token text and model shape are insufficient if weights
differ. The schema/layout and chunk geometry must also match. A multi-pool
`remote_cxl` request uses the same first-chunk `route_key` for stores and reads;
there is no global search or replication across independent pools.

Region IDs are assigned within one Resource Manager, not across every Maru
deployment. Keep the pool/server identity with a location. Reusing the same
physical CXL memory through different backends does not merge their registries.

---

## 1. Key Semantics

Maru treats keys as **opaque identifiers** — it does not interpret or validate their content. The caller (e.g., LMCache) is fully responsible for key generation and uniqueness.

| Property | Description |
|----------|-------------|
| Key type | `str` |
| Uniqueness | Caller-guaranteed |
| Duplicate handling | Idempotent — first writer wins, subsequent registrations are no-ops |
| Deletion | Explicit `delete` by the owning client |

**Write-once semantics**: When a key is registered for the first time, its location is recorded. If the same key is registered again, the second registration is silently ignored. The caller must explicitly delete a key before re-registering it at a new location.

> See also: [MaruServer — KV Registry](maru_server.md#3-kv-registry)

---

## 2. Location Model

Each KV cache chunk is located by a triple stored in the server's `KVEntry` metadata record: **(region_id, kv_offset, kv_length)**. This is the information a client needs to read data directly from shared memory, without server involvement.

- `region_id` (int): identifier unique within the allocating Resource Manager, not across independent RMs.
- `kv_offset` (int): byte offset within the region, computed as `page_index * chunk_size_bytes`.
- `kv_length` (int): size of the stored data in bytes.

The server returns the full location plus a capability-based handle that authorizes the client to map the region.

| Lookup Request | Lookup Response |
|---------------|-----------------|
| `LOOKUP_KV(key: str)` | `(handle, kv_offset, kv_length)` |

> See also: [MaruHandler — Data Flows](maru_handler.md#4-data-flows)
