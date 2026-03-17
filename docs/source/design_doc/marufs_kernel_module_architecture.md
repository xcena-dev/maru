# marufs Kernel Module Architecture

## CXL Memory Layout

```mermaid
block-beta
    columns 1
    GSB["Global Superblock (4 KB) — 0x0000"]
    ST["Shard Table (num_shards × 64B) — 0x1000"]
    IP["Index Pool — 0x2000\nPer-shard: buckets (u32[]) | hot entries (32B[]) | cold entries (96B[])"]
    RAT["RAT: 4KB header + 256 × 256B entries\n(2 MB aligned)"]
    RHP["Region Header Pool: 256 × 4KB = 1MB\n(2 MB aligned)"]
    RD["Region Data Areas (2 MB aligned)\nRegion 0 | Region 1 | ... | Region N"]

    style GSB fill:#e3f2fd,stroke:#1565c0
    style ST fill:#e8f5e9,stroke:#2e7d32
    style IP fill:#fff3e0,stroke:#e65100
    style RAT fill:#fce4ec,stroke:#c62828
    style RHP fill:#f3e5f5,stroke:#7b1fa2
    style RD fill:#e0f2f1,stroke:#00695c
```

## Module Components

```mermaid
graph TB
    subgraph VFS["VFS Layer"]
        super["super.c<br/>mount / umount / format"]
        dir["dir.c<br/>readdir / lookup<br/>create / unlink"]
        inode["inode.c<br/>iget / new_inode<br/>evict"]
        file["file.c<br/>mmap / ftruncate<br/>ioctl dispatch"]
    end

    subgraph Data["Data Layer"]
        index["index.c<br/>CAS hash index<br/>insert / lookup / delete"]
        region["region.c<br/>RAT allocator<br/>alloc / free / contiguous"]
    end

    subgraph Security["Security Layer"]
        acl["acl.c<br/>perm_grant<br/>perm_set_default<br/>chown"]
    end

    subgraph Maintenance["Maintenance"]
        gc["gc.c<br/>tombstone sweep<br/>dead process reap"]
        cache["cache.c<br/>entry cache (DRAM)"]
        sysfs["sysfs.c<br/>/sys/fs/marufs/ stats"]
    end

    super --> dir & inode & file
    dir --> index
    file --> index & region & acl & gc
    inode --> index

    style VFS fill:#e3f2fd,stroke:#1565c0
    style Data fill:#fff3e0,stroke:#e65100
    style Security fill:#fce4ec,stroke:#c62828
    style Maintenance fill:#e0f2f1,stroke:#00695c
```

## Source Files

| File | Role |
|------|------|
| `super.c` | Module init, mount/umount, DAX device setup, mkfs (format) |
| `dir.c` | Directory operations: readdir, lookup, create, unlink, d_revalidate |
| `inode.c` | Inode lifecycle: iget (from CXL index), new_inode, evict |
| `file.c` | File operations: mmap (DAX fault), ftruncate (region alloc), ioctl dispatch |
| `index.c` | Global partitioned index: CAS-based insert/lookup/delete, hash chain walk |
| `region.c` | RAT (Region Allocation Table): contiguous space finder, alloc/free entries |
| `acl.c` | Permission enforcement: delegation table check, perm_grant, perm_set_default, chown |
| `gc.c` | Background GC: tombstone sweep, dead process region reaping |
| `cache.c` | Entry cache: DRAM-side cache for frequently accessed index entries |
| `sysfs.c` | sysfs interface: `/sys/fs/marufs/` stats and configuration |

## Key Data Structures

| Structure | Size | Location | Purpose |
|-----------|------|----------|---------|
| `marufs_superblock` | 4 KB | CXL offset 0 | Global layout descriptor |
| `marufs_shard_header` | 64 B | Shard table | Per-shard index geometry |
| `marufs_index_entry_hot` | 32 B | Index pool | Chain walk: state, hash, region_id (2 per cache line) |
| `marufs_index_entry_cold` | 96 B | Index pool | Name verify + metadata (accessed only on hash match) |
| `marufs_rat_entry` | 256 B | RAT | Region allocation: phys_offset, size, owner, perms |
| `marufs_region_header` | 4 KB | Header pool | Region metadata + delegation table (124 entries) |
| `marufs_deleg_entry` | 32 B | Region header | Per-(node_id, pid) permission grant |

## Concurrency Model

```mermaid
stateDiagram-v2
    [*] --> EMPTY
    EMPTY --> INSERTING: CAS (lock-free)
    INSERTING --> VALID: CAS (commit)
    VALID --> TOMBSTONE: CAS (delete)
    TOMBSTONE --> EMPTY: GC sweep
```

- **Lock-free index**: CAS on `entry_hot.state` (EMPTY → INSERTING → VALID, VALID → TOMBSTONE)
- **Shard partitioning**: SHA-256 upper bits select shard → independent bucket arrays reduce contention
- **RAT alloc lock**: CAS spinlock for region physical allocation (serializes ftruncate across nodes)
- **CXL 2.0 compat**: Optional `clwb`/`clflushopt` barriers via `CONFIG_MARUFS_CXL2_COMPAT`

## Permission Model

```mermaid
graph TD
    Owner["Owner (creator)<br/>Implicit: all permissions"]
    Owner -->|ADMIN| AdminOps["chown<br/>perm_set_default"]
    Owner -->|GRANT| GrantOps["perm_grant to third parties<br/>(cannot escalate to ADMIN/GRANT)"]
    Owner --> DT["Delegation Table<br/>(per-region, up to 124 entries)"]
    DT --> E1["(node=1, pid=4521) → READ|WRITE"]
    DT --> E2["(node=2, pid=8832) → READ"]
    DT --> E3["..."]

    subgraph Enforcement["Kernel Enforcement"]
        open_op["open() → always allowed"]
        mmap_op["mmap() → check PERM_READ / PERM_WRITE"]
        read_op["read() → check PERM_READ"]
    end

    style Owner fill:#c8e6c9,stroke:#2e7d32
    style Enforcement fill:#fce4ec,stroke:#c62828
```

## ioctl Interface

| ioctl | Cmd | Description |
|-------|-----|-------------|
| `MARUFS_IOC_NAME_OFFSET` | X:1 | Register name-ref (name → region:offset) |
| `MARUFS_IOC_FIND_NAME` | X:2 | Global name lookup → (region_name, offset) |
| `MARUFS_IOC_CLEAR_NAME` | X:3 | Remove name-ref |
| `MARUFS_IOC_BATCH_FIND_NAME` | X:4 | Batch lookup (up to 32) |
| `MARUFS_IOC_BATCH_NAME_OFFSET` | X:6 | Batch register (up to 32) |
| `MARUFS_IOC_PERM_GRANT` | X:10 | Grant permissions to (node_id, pid) |
| `MARUFS_IOC_PERM_SET_DEFAULT` | X:13 | Set default permissions for non-owner |
| `MARUFS_IOC_CHOWN` | X:14 | Transfer ownership to caller |
| `MARUFS_IOC_DMABUF_EXPORT` | X:0x50 | Export DMA-BUF (DAXHEAP mode) |
