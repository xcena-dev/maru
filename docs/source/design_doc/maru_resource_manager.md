# MaruResourceManager Architecture

The `MaruResourceManager` is a server that manages physical CXL DAX device pools for the Maru system. It provides memory allocation, deallocation, and region handle issuance to clients via TCP RPC. It handles two device types — DEV_DAX (character devices) and FS_DAX (filesystem-backed DAX mounts) — and ensures durability through write-ahead logging and periodic checkpoints. A background reaper automatically reclaims leaked allocations from terminated client processes.

## 1. Component Architecture

```mermaid
flowchart TB
    subgraph RM["Maru Resource Manager"]
        Main["Main<br/>CLI parsing<br/>Signal handling"]
        TCP["TcpServer<br/>TCP :9850<br/>Binary IPC protocol"]
        RH["RequestHandler<br/>Business logic<br/>Alloc / Free / Stats"]
        PM["PoolManager<br/>Pool management<br/>Free list tracking"]
        Reaper["Reaper<br/>Dead process detection<br/>Auto-cleanup"]
        Meta["MetadataStore<br/>Checkpoint save/load<br/>CRC32 verification"]
        WAL["WalStore<br/>Write-ahead log<br/>Crash recovery"]
    end

    Main --> TCP
    Main --> PM
    Main --> Reaper
    TCP --> RH
    RH --> PM
    PM --> Meta
    PM --> WAL

    subgraph Clients["Python Clients (maru_shm)"]
        C1["MaruShmClient #1"]
        C2["MaruShmClient #2"]
    end

    C1 <-->|"TCP"| TCP
    C2 <-->|"TCP"| TCP

    subgraph Storage["CXL Memory"]
        D1["/dev/dax0.0<br/>(DEV_DAX)"]
        D2["/mnt/pmem0/<br/>(FS_DAX)"]
    end

    PM -.->|"sysfs scan"| Storage
```

`PoolManager` is the central component that owns all shared state — pool metadata, allocation maps, and free lists. It performs device discovery by scanning sysfs for DEV_DAX and FS_DAX devices, and supports hot-plug via signal-triggered rescans.

`TcpServer` accepts TCP client connections and dispatches requests to `RequestHandler`. It handles transport concerns including client identity parsing from the `client_id` field in requests.

`RequestHandler` contains all business logic — allocation, deallocation, access info lookup, and stats queries. It depends only on `PoolManager` and has no transport code.

`Reaper` runs a background thread that periodically detects terminated client processes and reclaims their leaked allocations through `PoolManager`.

`MetadataStore` persists per-pool free lists and the global allocation map as checkpoint files with integrity verification. `WalStore` provides crash recovery by recording every allocation and free operation to a write-ahead log before modifying in-memory state.

---

---

## 3. IPC Protocol

All messages use a fixed-size binary header (12 bytes) containing protocol magic, version (v2), message type, and payload length, followed by a type-specific payload. Communication is over TCP.

| Type | Direction | Description |
|------|-----------|-------------|
| `ALLOC_REQ` / `ALLOC_RESP` | client ↔ server | Allocate shared memory; response includes handle + device path |
| `FREE_REQ` / `FREE_RESP` | client ↔ server | Free allocation (requires valid auth token) |
| `GET_ACCESS_REQ` / `GET_ACCESS_RESP` | client ↔ server | Request device path + offset + length for an existing allocation |
| `STATS_REQ` / `STATS_RESP` | client ↔ server | Query per-pool statistics |
| `ERROR_RESP` | server → client | Error with status code and message |

Every allocation returns a **Handle** containing the region ID (globally unique), mmap offset, allocation length, and a cryptographic auth token. The Handle serves as both the allocation identifier and the authorization credential — clients must present it for free and access operations.

### Memory access via device path

Instead of passing file descriptors via `SCM_RIGHTS` (which only works over UDS), the server returns a **device path string** in `ALLOC_RESP` and `GET_ACCESS_RESP`. The client opens the path directly with `open()` and creates an `mmap()`.

| DAX type | Device path example | Offset | Length |
|----------|-------------------|--------|--------|
| DEV_DAX | `/dev/dax0.0` | Real byte offset within device | Aligned allocation size |
| FS_DAX | `/mnt/pmem0/maru_42.dat` | 0 (file per allocation) | File size |

### Client identity

Requests that require owner tracking (`ALLOC_REQ`, `FREE_REQ`) include a `client_id` field in the payload (`hostname:pid` format). The server extracts the PID for allocation ownership and Reaper tracking.

The `STATS_RESP` returns per-pool statistics. Each pool entry contains:

| Field | Type | Description |
|-------|------|-------------|
| `pool_id` | uint32 | Pool identifier |
| `dax_type` | enum | `DEV_DAX` (character device) or `FS_DAX` (filesystem DAX) |
| `total_size` | uint64 | Total pool capacity in bytes |
| `free_size` | uint64 | Currently available space in bytes |
| `align_bytes` | uint64 | Alignment requirement in bytes |

Stats can be queried via `MaruShmClient.stats()` in Python or the `maru_test_client stats` CLI tool.

---

## 4. Memory Management

Each discovered CXL device becomes a **pool** with a sorted free list of extents (offset + length pairs). The allocation algorithm uses **first-fit with alignment**: it scans the free list for the first extent that can accommodate the aligned request size, splits the extent into residual fragments if needed, and returns a Handle pointing to the allocated region.

For **DEV_DAX** pools, a single character device (`/dev/daxX.Y`) is shared by all allocations. The Handle's offset field contains the real byte offset within the device. For **FS_DAX** pools, each allocation creates a dedicated file (`<mountpoint>/maru_<regionId>.dat`) of the requested size, and the Handle's offset is always zero. The file is unlinked on free.

All allocation sizes are rounded up to the pool's alignment boundary. For DEV_DAX, the alignment is read from the sysfs `align` attribute (typically 2 MiB). For FS_DAX, it is determined from the block device's logical block size, with a 2 MiB minimum.

---

## 5. Persistence & Recovery

The Resource Manager ensures durability through a combination of write-ahead logging and periodic checkpoints.

Every allocation and free operation is first appended to the **WAL** before modifying in-memory state. Periodically, a **checkpoint** is triggered: per-pool free lists and the global allocation map are saved atomically. The WAL is then cleared.

On startup, the **recovery sequence** proceeds as: (1) scan for current hardware, (2) initialize pools with device sizes, (3) restore state from the last checkpoint, (4) replay any WAL records written since that checkpoint, and (5) recompute free sizes from the reconstituted free lists.

---

## 6. Reaper

The Reaper periodically checks the liveness of each allocation's owner process. If the process no longer exists, its allocations are reclaimed — extents are returned to the free list and the allocation is removed from the map.

To defend against **PID reuse**, the server caches each client's process start time at allocation time. If the OS reports the process as alive but the current start time differs from the cached value, the PID has been recycled by the kernel, and the allocations are reclaimed.

For **remote clients** (different hostname), the server monitors TCP connection state. When a remote client disconnects, a grace period begins. If the client does not reconnect within the grace period, its allocations are reclaimed. This prevents memory leaks from crashed remote clients while tolerating transient network interruptions.

---

## 7. Security

Every allocation receives a **cryptographic auth token** derived from the Handle fields and a server-side secret. Free and access requests must present a valid token; invalid tokens are rejected.

The secret is generated on first start and persisted to the state directory. On restart, if allocations exist from a previous run, the secret is loaded; if it is missing, startup is aborted to prevent token verification failures.

**Owner verification** ensures that clients can only free their own allocations — the `client_id` (`hostname:pid`) is recorded at allocation time and must match the freeing client's identity.

**Device permissions:** Since clients open device paths directly (instead of receiving FDs from the server), client processes must have read/write access to the CXL device files. Configure via `chmod` or group-based permissions.

---

## 8. Server Configuration

The server is configured via CLI arguments. On startup, the resource manager writes its resolved configuration to `rm.conf` in the state directory (atomic tmp+rename) for reference.

| Option | Default | Description |
|--------|---------|-------------|
| `--host`, `-H` | `0.0.0.0` | TCP bind address |
| `--port`, `-p` | `9850` | TCP port |
| `--state-dir`, `-d` | `/var/lib/maru-resourced` | State directory for WAL and checkpoints |
| `--log-level`, `-l` | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `--num-workers`, `-w` | `32` | Worker thread pool size |

### Daemon mode (production)

The recommended way to run the resource manager is as a systemd service:

```bash
sudo systemctl start maru-resource-manager
```

To customize options, use `sudo systemctl edit maru-resource-manager` and override `ExecStart`.

### Direct mode (development/debugging)

```bash
maru-resource-manager --host 0.0.0.0 --port 9850 \
                      --state-dir /var/lib/maru \
                      --log-level debug
```

### RM address propagation

MaruServer and MaruHandler need to know the resource manager's address:

```bash
# MaruServer — pass RM address
maru-server --port 5555 --rm-address 192.168.1.100:9850

# MaruHandler — configure via MaruConfig
# maru_rm_address: "192.168.1.100:9850"
```

Default `127.0.0.1:9850` works for single-node setups without explicit configuration.
