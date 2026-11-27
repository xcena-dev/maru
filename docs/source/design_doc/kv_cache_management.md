# Maru KV Cache Management

Maru stores KV cache data as fixed-size units called **KV cache chunks**. Each chunk occupies exactly one page in shared memory and corresponds to a single key. The chunk size is determined by Maru's `chunk_size_bytes` configuration, which must match the caller's KV chunk size.

## Backend, identity and sharing

The numbered sections below describe the `cxl` key registry on MaruServer (`maru-server --backend cxl`). On the node backend (selected by `maru_node`), MaruServer (`maru-server --backend node`) keeps keys in the Catalog.

| Backend | Reusable object | Read lifetime |
|---|---|---|
| `cxl` | A registered key whose CXL region the reader can map | Lookup returns a region handle and byte range; the caller keeps the key and region valid while reading |
| node: `dram`, `local_cxl` | A published object on the same GPU node with matching namespace and geometry | A Catalog source pin and worker mapping protect the read through GPU copy completion |
| node: `remote` | A memory-node object with matching namespace and geometry | A source pin protects the RDMA READ; worker staging stays owned through GPU copy completion |

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
