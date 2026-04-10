# marufs Kernel Module Architecture

## Module Components

```mermaid
graph TB
    subgraph VFS["VFS Layer"]
        super["super.c<br/>mount / umount / format"]
        dir["dir.c<br/>readdir / lookup<br/>create / unlink"]
        inode["inode.c<br/>iget / new_inode / evict"]
        file["file.c<br/>mmap / ftruncate / ioctl"]
    end

    subgraph Data["Data Layer"]
        index["index.c<br/>hash index<br/>insert / lookup / delete"]
        region["region.c<br/>RAT allocator<br/>alloc / free"]
        nrht_m["nrht.c<br/>Name-Ref Hash Table<br/>name_offset / find_name"]
    end

    subgraph Security["Security Layer"]
        acl["acl.c<br/>perm check / grant<br/>chown / set_default"]
    end

    subgraph Maint["Maintenance"]
        gc["gc.c<br/>4-phase GC sweep"]
        sysfs["sysfs.c<br/>/sys/fs/marufs/ stats"]
    end

    super --> dir & inode & file
    dir --> index
    file --> index & region & acl & nrht_m
    inode --> index
    gc -.-> index & region & nrht_m

    style VFS fill:#1B4F72,stroke:#1B4F72,stroke-width:2px,color:#fff
    style Data fill:#145A32,stroke:#145A32,stroke-width:2px,color:#fff
    style Security fill:#78281F,stroke:#78281F,stroke-width:2px,color:#fff
    style Maint fill:#2C3E50,stroke:#2C3E50,stroke-width:2px,color:#fff
```

| Layer | File | Role |
|-------|------|------|
| VFS | `super.c` | Module init, mount/umount, DAX device setup, mkfs (format) |
| VFS | `dir.c` | Directory operations: readdir, lookup, create, unlink, d_revalidate |
| VFS | `inode.c` | Inode lifecycle: iget (from CXL index), new_inode, evict |
| VFS | `file.c` | File operations: mmap (DAX fault), ftruncate (region alloc), ioctl dispatch |
| Data | `index.c` | Global partitioned index: CAS-based insert/lookup/delete, hash chain walk |
| Data | `region.c` | RAT (Region Allocation Table): contiguous space finder, alloc/free entries |
| Data | `nrht.c` | Name-Ref Hash Table: name_offset, find_name, batch operations |
| Security | `acl.c` | Permission enforcement: delegation table check, perm_grant, chown |
| Maintenance | `gc.c` | Background GC: 4-phase sweep (dead process, stale index, local tracker, NRHT) |
| Maintenance | `sysfs.c` | sysfs interface: `/sys/fs/marufs/` stats and configuration |

## CXL Memory Layout

```mermaid
block-beta
  columns 4

  sb_label["◼ Global Superblock"]:4
  sb["Superblock (256B)"]:4

  space:4

  gi_label["◼ Global Index"]:4
  sh0["Shard Header 0 (64B)"]
  sh1["Shard Header 1 (64B)"]
  sh2["Shard Header 2 (64B)"]
  sh3["Shard Header 3 (64B)"]
  bk0["Buckets 0 (256 × 4B)"]
  bk1["Buckets 1 (256 × 4B)"]
  bk2["Buckets 2 (256 × 4B)"]
  bk3["Buckets 3 (256 × 4B)"]
  en0["Entries 0 (256 × 64B)"]
  en1["Entries 1 (256 × 64B)"]
  en2["Entries 2 (256 × 64B)"]
  en3["Entries 3 (256 × 64B)"]

  space:4

  rat_label["◼ Region Allocation Table"]:4
  rat_hdr["RAT Header (128B)"]:4
  r0["RAT Entry 0 (2 KB)"]
  r1["RAT Entry 1 (2 KB)"]
  r_dot["... (× 253)"]
  r255["RAT Entry 255 (2 KB)"]

  space:4

  rg["Region 0, 1, 2, ... (2 MB aligned each)"]:4

  style sb_label fill:#1B4F72,color:#fff,font-weight:bold
  style sb fill:#2E86C1,color:#fff
  style gi_label fill:#145A32,color:#fff,font-weight:bold
  style sh0 fill:#1E8449,color:#fff
  style sh1 fill:#1E8449,color:#fff
  style sh2 fill:#1E8449,color:#fff
  style sh3 fill:#1E8449,color:#fff
  style bk0 fill:#27AE60,color:#fff
  style bk1 fill:#27AE60,color:#fff
  style bk2 fill:#27AE60,color:#fff
  style bk3 fill:#27AE60,color:#fff
  style en0 fill:#52BE80,color:#fff
  style en1 fill:#52BE80,color:#fff
  style en2 fill:#52BE80,color:#fff
  style en3 fill:#52BE80,color:#fff
  style rat_label fill:#78281F,color:#fff,font-weight:bold
  style rat_hdr fill:#C0392B,color:#fff
  style r0 fill:#E74C3C,color:#fff
  style r1 fill:#E74C3C,color:#fff
  style r_dot fill:#E74C3C,color:#fff
  style r255 fill:#E74C3C,color:#fff
  style rg fill:#2C3E50,color:#fff
```

| Block | Size | Description |
|-------|------|-------------|
| Superblock | 256B (4 CL) | FS geometry, shard count, offsets, mounted node bitmask (`active_nodes`) |
| Shard Header | 64B (1 CL) × 4 | Per-shard bucket/entry array offsets (immutable after format) |
| Buckets | 4B × 256 per shard | Hash chain head pointers (`head_entry_idx` or `BUCKET_END`) |
| Entries | 64B (1 CL) × 256 per shard | Index entries: state, name_hash, region_id, next_in_bucket |
| RAT Header | 128B (2 CL) | max_entries, alloc_lock (CAS spinlock), allocation stats |
| RAT Entry | 2 KB (32 CL) × 256 | CL0: phys_offset/size, CL1: name, CL2: ACL, CL3-31: delegation |
| Region Data | 2 MB aligned each | Actual file data, variable size |

## ACL (Access Control List)

```mermaid
flowchart TD
  subgraph GI_path["Global Index"]
    gi_path["filename → Index Entry<br/>(hash → shard → bucket → chain)"]
  end

  subgraph RAT_path["RAT Entry (via region_id)"]
    cl0["CL0: phys_offset, size"]
    cl2["CL2: Owner + default_perms"]
    cl3["CL3-31: Delegation Table<br/>(up to 29 entries)"]
  end

  subgraph RD_path["Region Data"]
    region["mmap / read / write / unlink<br/>(open is always allowed)"]
  end

  gi_path -->|"region_id"| RAT_path
  cl0 -->|"phys_offset"| region
  cl2 -.->|"owner / default"| region
  cl3 -.->|"delegated perms"| region

  style GI_path fill:#145A32,stroke:#145A32,stroke-width:2px,color:#fff
  style RAT_path fill:#1B4F72,stroke:#1B4F72,stroke-width:2px,color:#fff
  style RD_path fill:#2C3E50,stroke:#2C3E50,stroke-width:2px,color:#fff
```

- **Data path** (solid): `filename → Index Entry → region_id → CL0.phys_offset → Region`
- **Permission path** (dotted): Owner (implicit all) → default_perms (non-owner baseline) → Delegation Table (per-node/pid grants)
- open() always allowed — permission check at mmap / read / write / unlink
- Delegations stored on CXL — immediately visible cross-node

## NRHT (Name-Ref Hash Table)

```mermaid
flowchart TD
  subgraph GI["Global Index"]
    gi_entry["filename → region_id<br/>(file-level lookup)"]
  end

  subgraph NRHT_F["NRHT File"]
    nrht_entry["name → (offset, target_region_id)<br/>(application-level reference)"]
  end

  subgraph Regions["Region files"]
    r0["Region 0 (data)"]
    r1["Region 1 (data)"]
    r2["Region 2 (data)"]
  end

  gi_entry -->|"region_id=0"| r0
  gi_entry -->|"region_id=1"| r1
  gi_entry -->|"region_id=2"| r2
  gi_entry -->|"region_id=5 (NRHT)"| NRHT_F

  nrht_entry -->|"target=0, offset=0x1000"| r0
  nrht_entry -->|"target=1, offset=0x2000"| r1
  nrht_entry -->|"target=0, offset=0x3000"| r0

  style GI fill:#145A32,stroke:#145A32,stroke-width:2px,color:#fff
  style NRHT_F fill:#1B4F72,stroke:#1B4F72,stroke-width:2px,color:#fff
  style Regions fill:#2C3E50,stroke:#2C3E50,stroke-width:2px,color:#fff
```

- **Global Index**: `filename → region_id` — filesystem-level file lookup
- **NRHT**: `name → (offset, target_region_id)` — application-level intra-region references (e.g., KV cache keys)
- A single NRHT can freely reference **multiple regions** (N:M relationship)
- NRHT files are regular regions registered in the Global Index (own RAT entry)
