/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * marufs_layout.h - MARUFS Partitioned Global Index on-disk structures
 *
 * CAS-based lock-free distributed model: all nodes can concurrently
 * read/write the global index. No GCS (Global Chunk Server) needed.
 *
 * Memory Layout:
 * ┌─────────────────────────────────────────────────────────────┐
 * │ Global Superblock (4 KB)                                    │  0x0000_0000
 * ├─────────────────────────────────────────────────────────────┤
 * │ Shard Table (num_shards × 64B)                              │  0x0000_1000
 * ├─────────────────────────────────────────────────────────────┤
 * │ Global Index Entry Pool                                      │  0x0000_2000
 * │   Per-shard: [Buckets] [Hot entries (32B)] [Cold entries (96B)]│
 * ├──────────────── 2MB aligned ────────────────────────────────┤
 * │ RAT (4KB header + 256 × 256B entries)                        │
 * ├──────────────── 2MB aligned ────────────────────────────────┤
 * │ Region Header Pool (256 × 4KB = 1MB)                          │
 * ├──────────────── 2MB aligned ────────────────────────────────┤
 * │ Region 0 [Data area (2MB aligned)]                             │
 * │ Region 1 [Data area (2MB aligned)]                             │
 * └─────────────────────────────────────────────────────────────┘
 */

#ifndef _MARUFS_LAYOUT_H
#define _MARUFS_LAYOUT_H

#include "marufs_uapi.h"

#include <linux/types.h>
#include <linux/unaligned.h>
#include <crypto/sha2.h>

/* ── Magic Numbers ──────────────────────────────────────────────────── */

#define MARUFS_MAGIC 0x4D415255 /* "MARU" */
#define MARUFS_VERSION 2
#define MARUFS_SHARD_MAGIC 0x4D534844  /* "MSHD" */
#define MARUFS_REGION_MAGIC 0x4D52474E /* "MRGN" */

/* ── Index entry state (CAS target) ──────────────────────────────── */

#define MARUFS_ENTRY_EMPTY 0
#define MARUFS_ENTRY_VALID 1
#define MARUFS_ENTRY_TOMBSTONE 2
#define MARUFS_ENTRY_INSERTING 3

/* ── Region state ─────────────────────────────────────────────────── */

#define MARUFS_REGION_INACTIVE 0
#define MARUFS_REGION_ACTIVE 1

/* ── Region flags ─────────────────────────────────────────────────── */

#define MARUFS_REGION_IN_PROGRESS 0x00000001 /* Region allocation/release in progress */

/* ── RAT entry state ──────────────────────────────────────────────── */

#define MARUFS_RAT_ENTRY_FREE 0       /* Entry available */
#define MARUFS_RAT_ENTRY_ALLOCATING 1 /* Being initialized (fields not yet valid) */
#define MARUFS_RAT_ENTRY_ALLOCATED 2  /* Entry in use (fields fully initialized) */
#define MARUFS_RAT_ENTRY_DELETING 3   /* Being deleted */

/* MARUFS_PERM_* constants are defined in marufs_uapi.h */

/* ── Delegation entry state ───────────────────────────────────────── */

#define MARUFS_DELEG_MAGIC 0x44454C47 /* "DELG" */
#define MARUFS_DELEG_EMPTY 0
#define MARUFS_DELEG_GRANTING 1 /* Intermediate: fields being written */
#define MARUFS_DELEG_ACTIVE 2
#define MARUFS_DELEG_MAX_ENTRIES 124

/* ── Sentinels / Limits ─────────────────────────────────────────────── */

#define MARUFS_BUCKET_END 0xFFFFFFFF
/* MARUFS_NAME_MAX is defined in marufs_uapi.h */

/* ── Defaults ──────────────────────────────────────────────────────── */

#define MARUFS_DEFAULT_NUM_SHARDS 64
#define MARUFS_DEFAULT_ENTRIES_PER_SHARD 16384
#define MARUFS_MAX_REGIONS 256

/* ── Structure / Layout sizes ──────────────────────────────────────── */

#define MARUFS_GSB_SIZE 4096
#define MARUFS_SHARD_HEADER_SIZE 64
#define MARUFS_HOT_ENTRY_SIZE 32  /* v2: hot entry (chain walk, half CL) */
#define MARUFS_COLD_ENTRY_SIZE 96 /* v2: cold entry (name + metadata) */
#define MARUFS_REGION_HEADER_SIZE 4096
#define MARUFS_REGION_HDR_POOL_SIZE (MARUFS_MAX_REGIONS * MARUFS_REGION_HEADER_SIZE) /* 256 × 4KB = 1MB */

/* ── Fixed offsets from CXL memory start ────────────────────────── */

#define MARUFS_SHARD_TABLE_OFFSET 0x1000
#define MARUFS_INDEX_POOL_OFFSET 0x2000

/* ── Alignment ─────────────────────────────────────────────────────── */

#define MARUFS_ALIGN_2MB (2ULL * 1024 * 1024)

/* ── On-disk structures ────────────────────────────────────────────── */

/*
 * Global Superblock — first 4 KB of CXL memory.
 * Single instance; describes entire partitioned index layout.
 */
struct marufs_superblock
{
    __le32 magic;                  /* MARUFS_MAGIC               */
    __le32 version;                /* 1                        */
    __le64 total_size;             /* Total CXL memory size    */
    __le32 num_shards;             /* Number of shards (pow2)  */
    __le32 entries_per_shard;      /* Index entries per shard   */
    __le64 shard_table_offset;     /* Shard header array offset */
    __le64 index_pool_offset;      /* Index entry pool offset  */
    __le64 rat_offset;             /* RAT (Region Allocation Table) offset */
    __le64 created_at;             /* ns since epoch           */
    __le64 modified_at;            /* ns since epoch           */
    __le32 flags;                  /* Reserved flags           */
    __le32 checksum;               /* CRC32 of bytes 0..4091   */
    __le64 active_nodes;           /* Bitmask of mounted node_ids (bit N = node_id N+1), CAS-managed */
    __le64 region_hdr_pool_offset; /* Region header pool offset (v2) */
    __u8 reserved[4008];           /* Padding to 4096          */
} __attribute__((packed));

/*
 * Shard Header — 64 bytes each, stored in shard table.
 * One per shard; describes that shard's hash-bucket and entry-array
 * layout within global index pool.
 */
struct marufs_shard_header
{
    __le32 magic;                   /* MARUFS_SHARD_MAGIC                    */
    __le32 shard_id;                /* Shard index [0..num_shards)         */
    __le32 num_buckets;             /* Hash bucket count (pow2)            */
    __le32 num_entries;             /* Max entries in this shard            */
    __le64 bucket_array_offset;     /* Absolute offset in device           */
    __le64 hot_entry_array_offset;  /* v2: hot entries (32B each)           */
    __le64 cold_entry_array_offset; /* v2: cold entries (96B each)          */
    __le32 active_entries;          /* VALID count (approximate, CAS updated) */
    __le32 tombstone_entries;       /* TOMBSTONE count (approximate)       */
    __u8 reserved[16];              /* Padding to 64                       */
} __attribute__((packed));

/*
 * Index Entry — 256 bytes each, stored in global index entry pool.
 * Each entry represents either a region file or a name-ref mapping.
 * `state` field is CAS target for lock-free insert/delete.
 *
 * Entry types (distinguished by flags):
 *   - Region file: flags == 0, region_id = RAT entry, file_size = data size
 *   - Name ref:    flags & NAME_REF, region_id = target RAT entry,
 *                  file_size = offset within region's data area
 */
#define MARUFS_ENTRY_FLAG_NAME_REF 0x0001

/*
 * Hot Index Entry — 32 bytes (v2).
 * Accessed on every chain walk. Name moved to cold entry so chain walk
 * only touches hash comparison — 2 hot entries fit per 64B cache line.
 *   Half-CL (0-31): state + next + hash + region_id + flags + file_size
 */
struct marufs_index_entry_hot
{
    __le32 state;          /*  0: CAS target (EMPTY/VALID/TOMBSTONE/INSERTING) */
    __le32 next_in_bucket; /*  4: hash chain link (MARUFS_BUCKET_END = end) */
    __le64 name_hash;      /*  8: 64-bit SHA-256 truncated hash */
    __le32 region_id;      /* 16: RAT entry ID */
    __le16 flags;          /* 20: MARUFS_ENTRY_FLAG_* bits */
    __le16 _pad0;          /* 22: alignment */
    __le64 file_size;      /* 24: logical size / name-ref offset */
} __attribute__((packed)); /* Total: 32 bytes */

/*
 * Cold Index Entry — 96 bytes (v2).
 * Name at offset 0 so name verification reads exactly 1 cold CL (64B).
 * Accessed only when hash matches (name verify), at insert, and at iget.
 */
struct marufs_index_entry_cold
{
    char name[MARUFS_NAME_MAX + 1]; /*  0: null-terminated name (64B) */
    __le32 uid;                     /* 64: owner UID */
    __le32 gid;                     /* 68: owner GID */
    __le16 mode;                    /* 72: POSIX mode bits */
    __le16 _pad0;                   /* 74: alignment */
    __le64 created_at;              /* 76: ns since epoch */
    __le64 modified_at;             /* 84: ns since epoch */
    __u8 reserved[4];               /* 92: padding to 96B */
} __attribute__((packed));          /* Total: 96 bytes */

/*
 * Delegation Entry — 32 bytes each, stored in region header delegation table.
 * Each entry grants specific permissions to a (node_id, pid) pair.
 */
struct marufs_deleg_entry
{
    __le32 state;      /* MARUFS_DELEG_EMPTY(0) / MARUFS_DELEG_ACTIVE(1) */
    __le32 node_id;    /* Target node (0 = any node) */
    __le32 pid;        /* Target PID (0 = all processes on node) */
    __le32 perms;      /* Permission bitmask (MARUFS_PERM_*) */
    __le64 birth_time; /* PID reuse protection (0 if pid=0) */
    __le64 granted_at; /* Grant timestamp (ns since epoch) */
} __attribute__((packed));

/*
 * Delegation Table — 4000 bytes, stored in region header reserved area.
 * 16B header + 124 × 32B entries + 16B padding = 4000B.
 */
struct marufs_deleg_table
{
    __le32 magic;                                                /* MARUFS_DELEG_MAGIC */
    __le32 num_entries;                                          /* Active delegation count */
    __le32 max_entries;                                          /* MARUFS_DELEG_MAX_ENTRIES (124) */
    __u8 reserved_hdr[4];                                        /* Padding to 16B header */
    struct marufs_deleg_entry entries[MARUFS_DELEG_MAX_ENTRIES]; /* 124 × 32B = 3968B */
    __u8 reserved_pad[16];                                       /* Padding to 4000B total */
} __attribute__((packed));

/*
 * Region Header — 4 KB each, stored in region header pool.
 * Describes the data area. Name lookups are handled by the global index
 * (entries with MARUFS_ENTRY_FLAG_NAME_REF).
 *
 * Headers are stored in a consolidated pool (256 × 4KB = 1MB) to avoid
 * per-region 2MB padding waste. Region data starts directly at phys_offset.
 */
struct marufs_region_header
{
    __le32 magic;                          /* MARUFS_REGION_MAGIC                   */
    __le32 region_id;                      /* Region index (= RAT entry ID)       */
    __le32 state;                          /* INACTIVE(0) / ACTIVE(1)             */
    __le32 flags;                          /* Region flags (bit 0 = in-progress)  */
    __le64 data_size;                      /* User-requested data size (bytes)    */
    __le64 last_heartbeat;                 /* ns since epoch                      */
    __le64 created_at;                     /* ns since epoch                      */
    __le64 modified_at;                    /* ns since epoch                      */
    __u8 reserved_region[48];              /* Padding to maintain 4096B total     */
    struct marufs_deleg_table deleg_table; /* Permission delegation (4000B) */
} __attribute__((packed));

/* ── Region Allocation Table (RAT) Support ──────────────────── */

/*
 * Region Allocation Entry - Tracks variable-sized region files
 * Each entry describes one region file with physically contiguous allocation.
 * Size: 256 bytes
 *
 * Two-phase lifecycle:
 *   open(O_CREAT): state=ALLOCATED, phys_offset=0, size=0 (reservation)
 *   ftruncate(N):  phys_offset and size filled in (physical allocation)
 */
struct marufs_rat_entry
{
    /* State */
    __le32 state;        /* 0=FREE, 1=ALLOCATED, 2=DELETING */
    __le32 rat_entry_id; /* Index in RAT array */

    /* Identity */
    char name[MARUFS_NAME_MAX + 1]; /* Region file name (64 bytes) */

    /* Physical layout */
    __le64 phys_offset; /* Physical offset in device (0=not yet allocated) */
    __le64 size;        /* Total region size in bytes (2MB aligned, 0=reserved) */

    /* 3-stage ownership (ACL) */
    __le32 owner_node_id;    /* Node ownership */
    __le32 owner_pid;        /* Process ownership */
    __le64 owner_birth_time; /* Birth time verification */

    /* Timestamps */
    __le64 alloc_time;  /* Allocation timestamp (ns) */
    __le64 modified_at; /* Last modification */

    /* Permission delegation */
    __le32 default_perms; /* Default non-owner permissions (0 = owner-only) */

    /* Name-ref tracking */
    __le32 name_ref_count; /* Number of name-ref index entries for this region */

    /* Reserved */
    __u8 reserved[128]; /* Padding to 256 bytes */
} __attribute__((packed));

/*
 * Region Allocation Table - Global allocator for variable-sized regions
 * Stored after index pool, before first region.
 * Size: 4KB header + (256 × 256 bytes entries) = 69KB total
 */
#define MARUFS_RAT_MAGIC 0x4D524154 /* "MRAT" */
#define MARUFS_MAX_RAT_ENTRIES 256

struct marufs_rat
{
    /* Header */
    __le32 magic;       /* MARUFS_RAT_MAGIC */
    __le32 version;     /* 1 */
    __le32 num_entries; /* Number of allocated entries */
    __le32 max_entries; /* MARUFS_MAX_RAT_ENTRIES */

    /* Device info */
    __le64 device_size;   /* Total device size */
    __le64 rat_offset;    /* Offset of this RAT */
    __le64 regions_start; /* Where region files start */

    /* Allocation stats */
    __le64 total_allocated; /* Total allocated bytes */
    __le64 total_free;      /* Remaining free bytes */

    /* Global allocation lock (CAS spinlock for region_init) */
    __le32 alloc_lock; /* 0=unlocked, 1=locked */

    /* Reserved */
    __u8 reserved[4036]; /* Padding to 4KB (4036 + 60 bytes above = 4096) */

    /* Entry array follows immediately */
    struct marufs_rat_entry entries[MARUFS_MAX_RAT_ENTRIES];
} __attribute__((packed));

/* ── Compile-time size validation ────────────────────────────────── */

#define MARUFS_BUILD_BUG_ON(cond) \
    ((void)sizeof(char[1 - 2 * !!(cond)]))

static inline void __marufs_verify_structs(void)
{
    MARUFS_BUILD_BUG_ON(sizeof(struct marufs_superblock) != 4096);
    MARUFS_BUILD_BUG_ON(sizeof(struct marufs_shard_header) != 64);
    MARUFS_BUILD_BUG_ON(sizeof(struct marufs_index_entry_hot) != 32);
    MARUFS_BUILD_BUG_ON(sizeof(struct marufs_index_entry_cold) != 96);
    MARUFS_BUILD_BUG_ON(sizeof(struct marufs_deleg_entry) != 32);
    MARUFS_BUILD_BUG_ON(sizeof(struct marufs_deleg_table) != 4000);
    MARUFS_BUILD_BUG_ON(sizeof(struct marufs_region_header) != 4096);
    MARUFS_BUILD_BUG_ON(sizeof(struct marufs_rat_entry) != 256);
    MARUFS_BUILD_BUG_ON(sizeof(struct marufs_rat) != (4096 + 256 * MARUFS_MAX_RAT_ENTRIES));
}

/* ============================================================================
 * Endian conversion + memory barrier helpers
 * ============================================================================
 *
 * CXL shared memory requires both endian conversion (on-disk = little-endian)
 * and compiler barriers (READ_ONCE/WRITE_ONCE) to prevent optimization bugs.
 *
 * These macros combine both operations for common field types.
 */

/* Read little-endian field with compiler barrier */
#define READ_LE32(field) le32_to_cpu(READ_ONCE(field))
#define READ_LE64(field) le64_to_cpu(READ_ONCE(field))
#define READ_LE16(field) le16_to_cpu(READ_ONCE(field))

/* Write little-endian field with compiler barrier */
#define WRITE_LE32(field, val) WRITE_ONCE(field, cpu_to_le32(val))
#define WRITE_LE64(field, val) WRITE_ONCE(field, cpu_to_le64(val))
#define WRITE_LE16(field, val) WRITE_ONCE(field, cpu_to_le16(val))

/* Atomic compare-and-swap on little-endian u32 field
 * Returns old value in CPU byte order */
#define CAS_LE32(ptr, old, new) \
    le32_to_cpu(cmpxchg((u32*)(ptr), cpu_to_le32(old), cpu_to_le32(new)))

#define CAS_LE64(ptr, old, new) \
    le64_to_cpu(cmpxchg((u64*)(ptr), cpu_to_le64(old), cpu_to_le64(new)))

/* ============================================================================
 * CXL multi-node memory barriers
 * ============================================================================
 *
 * CXL 3.0: Hardware cache coherence is guaranteed across hosts.
 *          Standard wmb()/rmb() suffice for cross-node visibility.
 *
 * CXL 2.0: No cross-host cache coherence in the spec.
 *          Explicit clwb/clflushopt required to flush/invalidate CPU caches
 *          so that writes reach CXL memory and reads fetch fresh data.
 *
 * MARUFS_CXL_WMB(addr, len) - Call after writing CXL fields.
 *                              Flushes data from CPU cache to CXL memory.
 * MARUFS_CXL_RMB(addr, len) - Call before reading CXL fields.
 *                              Invalidates local cache to ensure fresh read.
 *
 * Enable CXL 2.0 mode by defining CONFIG_MARUFS_CXL2_COMPAT at build time.
 */
#ifdef CONFIG_MARUFS_CXL2_COMPAT

static inline void __marufs_cxl_flush_range(const void *addr, size_t len)
{
    volatile char *p = (volatile char *)((unsigned long)addr & ~63UL);
    const char *end = (const char *)addr + len;

    for (; p < (volatile char *)end; p += 64)
        clwb(p);
    wmb();
}

static inline void __marufs_cxl_invalidate_range(const void *addr, size_t len)
{
    volatile char *p = (volatile char *)((unsigned long)addr & ~63UL);
    const char *end = (const char *)addr + len;

    for (; p < (volatile char *)end; p += 64)
        clflushopt(p);
    mb();
}

#define MARUFS_CXL_WMB(addr, len) \
    do { wmb(); __marufs_cxl_flush_range(addr, len); } while (0)

#define MARUFS_CXL_RMB(addr, len) \
    do { __marufs_cxl_invalidate_range(addr, len); rmb(); } while (0)

#else /* CXL 3.0: hardware coherence guaranteed across hosts */

#define MARUFS_CXL_WMB(addr, len) wmb()
#define MARUFS_CXL_RMB(addr, len) rmb()

#endif /* CONFIG_MARUFS_CXL2_COMPAT */

/* ── Little-endian constant macros for CAS operations ─────────────── */

#define MARUFS_ENTRY_EMPTY_LE cpu_to_le32(MARUFS_ENTRY_EMPTY)
#define MARUFS_ENTRY_VALID_LE cpu_to_le32(MARUFS_ENTRY_VALID)
#define MARUFS_ENTRY_TOMBSTONE_LE cpu_to_le32(MARUFS_ENTRY_TOMBSTONE)
#define MARUFS_ENTRY_INSERTING_LE cpu_to_le32(MARUFS_ENTRY_INSERTING)
#define MARUFS_REGION_ACTIVE_LE cpu_to_le32(MARUFS_REGION_ACTIVE)
#define MARUFS_REGION_INACTIVE_LE cpu_to_le32(MARUFS_REGION_INACTIVE)

/* ── Inline helpers ────────────────────────────────────────────────── */

/*
 * marufs_le32_cas_inc/dec - CAS-based atomic increment/decrement for __le32 on CXL.
 * Safe for concurrent multi-node access. Underflow-safe (clamps to 0).
 */
static inline void marufs_le32_cas_inc(__le32* p)
{
    __le32 old_le, new_le;

    do
    {
        old_le = READ_ONCE(*p);
        new_le = cpu_to_le32(le32_to_cpu(old_le) + 1);
        if (cmpxchg(p, old_le, new_le) == old_le)
            return;
        cpu_relax();
    } while (1);
}

static inline void marufs_le32_cas_dec(__le32* p, u32 n)
{
    __le32 old_le, new_le;
    u32 old_val;

    do
    {
        old_le = READ_ONCE(*p);
        old_val = le32_to_cpu(old_le);
        new_le = cpu_to_le32(old_val > n ? old_val - n : 0);
        if (cmpxchg(p, old_le, new_le) == old_le)
            return;
        cpu_relax();
    } while (1);
}

/*
 * marufs_shard_id - select shard from 64-bit name hash.
 * Uses upper 16 bits (bits 63..48) masked with shard_mask.
 * @name_hash: 64-bit hash of filename
 * @shard_mask: num_shards - 1 (num_shards must be power of 2)
 */
static inline u32 marufs_shard_id(u64 name_hash, u32 shard_mask)
{
    return (u32)((name_hash >> 48) & shard_mask);
}

/*
 * marufs_bucket_idx - select bucket within shard.
 * Modulo bits 47..32 of hash by bucket count.
 * @name_hash: 64-bit hash of filename
 * @num_buckets: hash bucket count in shard
 *
 * Returns 0 if num_buckets is 0 (corrupted shard) to prevent divide by zero.
 */
static inline u32 marufs_bucket_idx(u64 name_hash, u32 num_buckets)
{
    if (unlikely(num_buckets == 0))
    {
        pr_err(MARUFS_MODULE_NAME ": BUG - num_buckets is 0, shard table corrupted!\n");
        return 0;
    }
    return (u32)((name_hash >> 32) % num_buckets);
}

/*
 * marufs_ino - synthesize inode number from region_id and slot_idx.
 * Layout: [region_id (upper bits)] [slot_idx + 2 (lower 20 bits)]
 * Reserves ino 0 (null) and ino 1 (root).
 */
static inline unsigned long marufs_ino(u32 region_id, u32 slot_idx)
{
    return ((unsigned long)region_id << 20) | (slot_idx + 2);
}

/*
 * marufs_ino_region - extract region_id from inode number.
 */
static inline u32 marufs_ino_region(unsigned long ino)
{
    return (u32)(ino >> 20);
}

/*
 * marufs_align_up - align @val to next @align boundary.
 * @align must be power of 2.
 */
static inline u64 marufs_align_up(u64 val, u64 align)
{
    return (val + align - 1) & ~(align - 1);
}

/*
 * marufs_hash_name - SHA-256 truncated hash for filename.
 * @name: filename string
 * @len:  filename length
 *
 * Uses SHA-256 for strong collision resistance, truncated to 64 bits.
 * Upper bits used for shard selection, middle bits for bucket index.
 */
static inline u64 marufs_hash_name(const char* name, size_t len)
{
    u8 digest[SHA256_DIGEST_SIZE];

    sha256((const u8*)name, len, digest);
    return get_unaligned_le64(digest);
}

/* ioctl structures and commands are defined in marufs_uapi.h */

#endif /* _MARUFS_LAYOUT_H */
