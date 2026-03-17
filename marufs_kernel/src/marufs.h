/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * marufs.h - MARUFS Partitioned Global Index in-memory structures
 *
 * ============================================================================
 * MARUFS Architecture: CAS-based Lock-free Distributed Model
 * ============================================================================
 *
 * All nodes can concurrently read/write the global index using CAS
 * (Compare-And-Swap) operations. No GCS (Global Chunk Server) needed.
 *
 * CXL Memory Layout:
 * ┌─────────────────────────────────────────────────────────────┐
 * │ Global Superblock (4 KB)                                    │  0x0000
 * ├─────────────────────────────────────────────────────────────┤
 * │ Shard Table (num_shards × 64B headers)                      │  0x1000
 * ├─────────────────────────────────────────────────────────────┤
 * │ Index Pool                                                   │  0x2000
 * │   Per-shard: [bucket array (u32[])] [entry array (128B[])]   │
 * ├──────────────── 2 MB aligned ────────────────────────────────┤
 * │ RAT (4KB header + 256 × 256B entries)                        │
 * ├──────────────── 2 MB aligned ────────────────────────────────┤
 * │ Region Header Pool (256 × 4KB = 1MB)                          │
 * ├──────────────── 2 MB aligned ────────────────────────────────┤
 * │ Region 0 [Data area]                                          │
 * │ Region 1 [Data area]                                          │
 * │ ...                                                          │
 * └─────────────────────────────────────────────────────────────┘
 *
 * Key Concepts:
 *   - Shards partition the global namespace via hash (upper bits)
 *   - Each shard has its own bucket array + entry array
 *   - Regions are per-node data stores using bitmap slot allocation
 *   - Index entries reference (region_id, slot_idx) for data location
 *   - Lock-free insert/delete via CAS on entry.state
 */

#ifndef _MARUFS_H
#define _MARUFS_H

/* ============================================================================
 * Module name configuration
 * ============================================================================
 * MARUFS_MODULE_NAME is defined via compiler flag in Makefile (-DMARUFS_MODULE_NAME).
 * Change MODULE_NAME in Makefile to load multiple instances without conflicts.
 * This affects: filesystem type name, sysfs path, and kernel module name.
 */
#ifndef MARUFS_MODULE_NAME
#define MARUFS_MODULE_NAME "marufs" /* Fallback if not defined by Makefile */
#endif

/* pr_fmt: 모든 pr_* 로그에 모듈 이름 자동 접두사 */
#undef pr_fmt
#define pr_fmt(fmt) MARUFS_MODULE_NAME ": " fmt

#include <linux/atomic.h>
#include <linux/fs.h>
#include <linux/spinlock.h>
#include <linux/types.h>

#ifdef CONFIG_DAXHEAP
#include <linux/iosys-map.h>
struct dma_buf;
#endif

/* On-disk structure definitions */
#include "marufs_layout.h"

/* ============================================================================
 * Filesystem constants
 * ============================================================================ */

#define MARUFS_ROOT_INO 1 /* Root directory ino */

/* Stale entry timeout: INSERTING, GRANTING, ALLOCATING states older than
 * this are considered abandoned and eligible for GC reclaim. */
#define MARUFS_STALE_TIMEOUT_NS (30ULL * NSEC_PER_SEC)

/* VFS blocksize (for sb->s_blocksize) */
#define MARUFS_VFS_BLOCK_SIZE 4096
#define MARUFS_VFS_BLOCK_SIZE_BITS 12

/* Sector size for i_blocks calculation */
#define MARUFS_SECTOR_SIZE 512
#define MARUFS_SECTOR_SHIFT 9

/* Root directory mode */
#define MARUFS_ROOT_DIR_MODE 0755

/* ============================================================================
 * DAX mode enumeration
 * ============================================================================ */

enum marufs_dax_mode
{
    MARUFS_DAX_DEV = 0,  /* DEV_DAX: via dax character device */
    MARUFS_DAX_HEAP = 1, /* DAXHEAP: via daxheap buffer (WC mmap for GPU) */
};

/* Forward declarations */
struct marufs_entry_cache;

/* ============================================================================
 * Local DRAM cache for shard metadata (read-only after format)
 * ============================================================================
 *
 * Eliminates per-operation CXL reads for shard header fields that never
 * change: bucket/entry array pointers and counts.  Populated once at mount
 * from CXL shard_table, then accessed from local DRAM on every index op.
 */

struct marufs_shard_cache
{
    u32* buckets;                               /* CXL bucket array pointer */
    struct marufs_index_entry_hot* hot_entries;   /* CXL hot entry array (32B each) */
    struct marufs_index_entry_cold* cold_entries; /* CXL cold entry array (96B each) */
    u32 num_buckets;
    u32 num_entries;
};

/* ============================================================================
 * In-memory superblock info
 * ============================================================================
 *
 * Combines device/DAX state with CXL memory layout pointers.
 * One instance per mounted filesystem, stored in sb->s_fs_info.
 */

struct marufs_sb_info
{
    /* ---- Device and DAX state ---- */
    void __iomem* base;            /* CXL memory base (compatibility) */
    void* dax_base;                /* DAX direct access base (virtual) */
    phys_addr_t phys_base;         /* Physical base address for DAX mmap */
    long dax_nr_pages;             /* DAX mapped page count */
    u64 total_size;                /* Total device size (bytes) */
    u32 node_id;                   /* This node's ID */
    bool use_dax;                  /* DAX enabled flag */
    struct task_struct* gc_thread; /* Background GC thread */
    atomic_t gc_active;            /* 0 = GC should self-terminate */
    atomic_t gc_paused;            /* 1 = GC temporarily paused (skip sweep) */
    atomic_t gc_epoch;             /* GC cycle counter (liveness check) */
    enum marufs_dax_mode dax_mode;   /* DEV_DAX vs DAXHEAP */
    char daxdev_path[128];         /* DEV_DAX device path */
    struct file* dax_filp;         /* DEV_DAX device file for mmap delegation */

#ifdef CONFIG_DAXHEAP
    struct dma_buf* heap_dmabuf; /* daxheap dma_buf handle */
    struct iosys_map heap_map;   /* kernel vmap (WB) for metadata access */
#endif

    /* ---- Global Superblock ---- */
    struct marufs_superblock* gsb; /* Pointer in CXL memory */

    /* ---- Shard Table ---- */
    struct marufs_shard_header* shard_table; /* Shard header array (CXL, READ-ONLY after format) */
    u32 num_shards;                        /* Number of shards */
    u32 shard_mask;                        /* num_shards - 1 (for masking) */
    u32 entries_per_shard;                 /* Entries per shard */

    /* Per-shard kernel RAM hint (not for correctness) */
    atomic_t* shard_free_hint;  /* per-shard next-free scan start */

    /* ---- Index Pool ---- */
    void* index_pool_base; /* Start of bucket+entry arrays */

    /* ---- RAT (Region Allocation Table) ---- */
    struct marufs_rat* rat;
    u64 rat_offset;
    u64 regions_start_offset;

    /* ---- Region Header Pool (v2: consolidated headers) ---- */
    struct marufs_region_header* region_hdr_pool; /* 256 × 4KB in CXL memory */
    u64 region_hdr_pool_offset;

    /* ---- Shard Cache (local DRAM, read-only after mount) ---- */
    struct marufs_shard_cache* shard_cache; /* Per-shard cached pointers + geometry */

    /* ---- Cache ---- */
    struct marufs_entry_cache* entry_cache; /* File entry cache */

    /* ---- Page mapping mode ---- */
    bool has_struct_pages; /* ZONE_DEVICE struct pages exist (VM_MIXEDMAP capable) */

    /* ---- Synchronization ---- */
    spinlock_t lock;
};

/* ============================================================================
 * In-memory inode info
 * ============================================================================
 *
 * Tracks mapping from VFS inode to global index entry and region/slot
 * where file data resides.
 *
 * vfs_inode must be last field (container_of requirement).
 */

struct marufs_inode_info
{
    u32 region_id; /* Region where data is stored (= RAT entry ID) */
    u32 entry_idx; /* Global index entry index (for writeback) */
    u32 shard_id;  /* Shard this entry belongs to */
    u32 rat_entry_id;
    u64 region_offset;
    u32 owner_node_id;
    u32 owner_pid;
    u64 owner_birth_time;
    u64 data_phys_offset;   /* Cached: region phys_offset (= data start) */
    struct inode vfs_inode; /* VFS inode (must be last!) */
};

/* ============================================================================
 * Helper macros
 * ============================================================================ */

static inline struct marufs_inode_info* MARUFS_I(struct inode* inode)
{
    return container_of(inode, struct marufs_inode_info, vfs_inode);
}

/* Zero-initialize all marufs-specific fields of inode_info.
 * Called from alloc_inode (super.c) and new_inode (inode.c). */
static inline void marufs_inode_info_init(struct marufs_inode_info* xi)
{
    xi->region_id = 0;
    xi->entry_idx = 0;
    xi->shard_id = 0;
    xi->rat_entry_id = 0;
    xi->region_offset = 0;
    xi->owner_node_id = 0;
    xi->owner_pid = 0;
    xi->owner_birth_time = 0;
    xi->data_phys_offset = 0;
}

static inline struct marufs_sb_info* MARUFS_SB(struct super_block* sb)
{
    return sb->s_fs_info;
}

/* ============================================================================
 * DAX abstraction API
 * ============================================================================
 *
 * All filesystem code uses these helpers to access device memory.
 * Transparently handles DEV_DAX and DAXHEAP modes.
 */

/* Check if DAX is active (returns true for both DEV_DAX and DAXHEAP) */
static inline bool marufs_dax_active(struct marufs_sb_info* sbi)
{
    return sbi->use_dax;
}

/* Return current DAX mode */
static inline enum marufs_dax_mode marufs_dax_get_mode(struct marufs_sb_info* sbi)
{
    return sbi->dax_mode;
}

/* Get direct pointer to device memory at @offset (DAX only) */
static inline void* marufs_dax_ptr(struct marufs_sb_info* sbi, u64 offset)
{
    return sbi->use_dax ? (void*)((char*)sbi->dax_base + offset) : NULL;
}

/* Return total device size in bytes */
static inline u64 marufs_dax_size(struct marufs_sb_info* sbi)
{
    return sbi->total_size;
}

/* Validate that a physical offset is within the DAX mapping */
static inline bool marufs_valid_phys_offset(struct marufs_sb_info* sbi, u64 offset)
{
    return offset > 0 && offset < sbi->total_size && sbi->dax_base;
}

/* Safe RAT entry accessor - returns NULL if id is out of bounds or RAT unavailable */
static inline struct marufs_rat_entry* marufs_rat_get(struct marufs_sb_info* sbi, u32 id)
{
    if (unlikely(!sbi->rat || id >= MARUFS_MAX_RAT_ENTRIES))
        return NULL;
    return &sbi->rat->entries[id];
}

/*
 * marufs_rat_name_ref_adjust - atomically adjust name_ref_count on a RAT entry
 * @rat_e: RAT entry pointer (must not be NULL)
 * @delta: signed adjustment (positive = increment, negative = decrement)
 *
 * Uses CAS loop. On decrement, clamps to zero to prevent underflow.
 */
static inline void marufs_rat_name_ref_adjust(struct marufs_rat_entry* rat_e,
                                              s32 delta)
{
    u32 old_count, new_count;

    do {
        old_count = READ_LE32(rat_e->name_ref_count);
        if (delta < 0 && old_count < (u32)(-delta))
        {
            /* Underflow: clamp to zero */
            new_count = 0;
        }
        else
        {
            new_count = (u32)((s32)old_count + delta);
        }
        if (old_count == new_count)
            break;
    } while (CAS_LE32(&rat_e->name_ref_count, old_count,
                       new_count) != old_count);
}

/* Safe region header accessor - returns header from consolidated pool by region_id */
static inline struct marufs_region_header* marufs_region_hdr(struct marufs_sb_info* sbi, u32 region_id)
{
    if (unlikely(!sbi->region_hdr_pool || region_id >= MARUFS_MAX_RAT_ENTRIES))
        return NULL;
    return &sbi->region_hdr_pool[region_id];
}

/* Validate that [offset, offset+size) is within DAX mapping (overflow-safe) */
static inline bool marufs_dax_range_valid(struct marufs_sb_info* sbi, u64 offset, u64 size)
{
    if (unlikely(!sbi->dax_base || size == 0))
        return false;
    if (unlikely(offset >= sbi->total_size))
        return false;
    if (unlikely(size > sbi->total_size - offset)) /* overflow-safe subtraction */
        return false;
    return true;
}

/* Validate data_phys_offset + access range is within DAX mapping */
static inline bool marufs_validate_slot_addr(struct marufs_sb_info* sbi,
                                           u64 data_phys_offset, u64 access_size)
{
    if (unlikely(data_phys_offset == 0))
        return false;
    return marufs_dax_range_valid(sbi, data_phys_offset, access_size);
}

/* ============================================================================
 * Helper inline functions
 * ============================================================================ */

/* Shard geometry sanity multiplier: max valid = ENTRIES_PER_SHARD * this */
#define MARUFS_SHARD_SANITY_MULT 4

/* Validate shard geometry is non-zero and within sane bounds */
static inline bool marufs_shard_geometry_valid(u32 num_buckets, u32 num_entries)
{
    u32 limit = MARUFS_DEFAULT_ENTRIES_PER_SHARD * MARUFS_SHARD_SANITY_MULT;
    return num_buckets > 0 && num_entries > 0 &&
           num_buckets <= limit && num_entries <= limit;
}

/* Return shard's bucket array pointer (from local DRAM shard_cache) */
static inline u32* marufs_shard_buckets(struct marufs_sb_info* sbi, u32 shard_id)
{
    if (unlikely(shard_id >= sbi->num_shards))
        return NULL;
    return sbi->shard_cache[shard_id].buckets;
}

/* Return shard's hot entry array pointer (from local DRAM shard_cache) */
static inline struct marufs_index_entry_hot*
marufs_shard_hot_entries(struct marufs_sb_info* sbi, u32 shard_id)
{
    if (unlikely(shard_id >= sbi->num_shards))
        return NULL;
    return sbi->shard_cache[shard_id].hot_entries;
}

/* Return shard's cold entry array pointer (from local DRAM shard_cache) */
static inline struct marufs_index_entry_cold*
marufs_shard_cold_entries(struct marufs_sb_info* sbi, u32 shard_id)
{
    if (unlikely(shard_id >= sbi->num_shards))
        return NULL;
    return sbi->shard_cache[shard_id].cold_entries;
}

/* Safe hot entry access: validate shard_id and entry_idx within shard bounds */
static inline struct marufs_index_entry_hot*
marufs_shard_hot_entry(struct marufs_sb_info* sbi, u32 shard_id, u32 entry_idx)
{
    struct marufs_shard_cache* sc;

    if (unlikely(shard_id >= sbi->num_shards))
        return NULL;
    sc = &sbi->shard_cache[shard_id];
    if (unlikely(entry_idx >= sc->num_entries ||
                 !marufs_shard_geometry_valid(1, sc->num_entries)))
        return NULL;
    if (unlikely(!sc->hot_entries))
        return NULL;
    return &sc->hot_entries[entry_idx];
}

/* Safe cold entry access: validate shard_id and entry_idx within shard bounds */
static inline struct marufs_index_entry_cold*
marufs_shard_cold_entry(struct marufs_sb_info* sbi, u32 shard_id, u32 entry_idx)
{
    struct marufs_shard_cache* sc;

    if (unlikely(shard_id >= sbi->num_shards))
        return NULL;
    sc = &sbi->shard_cache[shard_id];
    if (unlikely(entry_idx >= sc->num_entries ||
                 !marufs_shard_geometry_valid(1, sc->num_entries)))
        return NULL;
    if (unlikely(!sc->cold_entries))
        return NULL;
    return &sc->cold_entries[entry_idx];
}

/* ============================================================================
 * Function declarations - super.c
 * ============================================================================ */

int marufs_fill_super(struct super_block* sb, void* data, int silent);

/* ============================================================================
 * Function declarations - region.c (RAT allocator)
 * ============================================================================ */

/* RAT (Region Allocation Table) management */
int marufs_find_contiguous_space(struct marufs_sb_info* sbi, u64 size,
                               u64* out_offset);
int marufs_rat_alloc_entry(struct marufs_sb_info* sbi, const char* name,
                         u64 size, u64 offset, u32* out_rat_entry_id);
void marufs_rat_free_entry(struct marufs_sb_info* sbi, u32 rat_entry_id);

/* Region initialization (called from ftruncate path) */
int marufs_region_init(struct marufs_sb_info* sbi, u32 rat_entry_id,
                     u64 data_size);

/* Directory ioctl (MARUFS_IOC_FIND_NAME on root dir fd) */
long marufs_dir_ioctl(struct file* file, unsigned int cmd, unsigned long arg);

/* ============================================================================
 * Function declarations - index.c (Global index CAS operations)
 * ============================================================================ */

int marufs_index_insert(struct marufs_sb_info* sbi, const char* name,
                      size_t namelen, u32 region_id,
                      u64 file_size, u32 uid, u32 gid, u16 mode, u16 flags,
                      u32* out_entry_idx);
int marufs_index_insert_hash(struct marufs_sb_info* sbi, const char* name,
                           size_t namelen, u64 name_hash, u32 region_id,
                           u64 file_size, u32 uid, u32 gid, u16 mode,
                           u16 flags, u32* out_entry_idx);
int marufs_index_lookup(struct marufs_sb_info* sbi, const char* name,
                      size_t namelen, struct marufs_index_entry_hot** out_entry);
int marufs_index_lookup_hash(struct marufs_sb_info* sbi, const char* name,
                           size_t namelen, u64 name_hash,
                           struct marufs_index_entry_hot** out_entry);
int marufs_index_delete(struct marufs_sb_info* sbi, const char* name,
                      size_t namelen);
int marufs_index_delete_by_region(struct marufs_sb_info* sbi, u32 rat_entry_id);
int marufs_index_iterate(struct marufs_sb_info* sbi, u32 start_shard,
                       u32 start_entry, u32* next_shard, u32* next_entry,
                       struct marufs_index_entry_hot** out_entry,
                       struct marufs_index_entry_cold** out_cold);

/* ============================================================================
 * Function declarations - inode.c
 * ============================================================================ */

struct inode* marufs_iget(struct super_block* sb,
                        struct marufs_index_entry_hot* hot,
                        struct marufs_index_entry_cold* cold,
                        u32 shard_id, u32 entry_idx);
struct inode* marufs_new_inode(struct super_block* sb, umode_t mode);
int marufs_write_inode(struct inode* inode, struct writeback_control* wbc);
void marufs_evict_inode(struct inode* inode);

extern const struct inode_operations marufs_file_inode_ops;
extern const struct inode_operations marufs_dir_inode_ops;

/* ============================================================================
 * Function declarations - file.c
 * ============================================================================ */

extern const struct file_operations marufs_file_ops;
extern const struct address_space_operations marufs_aops;

/* ============================================================================
 * Function declarations - dir.c
 * ============================================================================ */

extern const struct file_operations marufs_dir_ops;

/* ============================================================================
 * Function declarations - acl.c (Region-level access control)
 * ============================================================================ */

struct marufs_deleg_table*
marufs_deleg_table_get(struct marufs_sb_info* sbi,
                      u32 rat_entry_id,
                      u32* out_num_entries);
void marufs_deleg_entry_clear(struct marufs_deleg_entry* de);
int marufs_check_permission(struct marufs_sb_info* sbi, u32 rat_entry_id,
                          u32 required_perms);
int marufs_deleg_grant(struct marufs_sb_info* sbi, u32 rat_entry_id,
                     struct marufs_perm_req* req);
bool marufs_owner_is_dead(u32 owner_pid, u64 owner_birth_time);

/* ============================================================================
 * Function declarations - cache.c
 * ============================================================================ */

int marufs_cache_init(struct marufs_sb_info* sbi);
void marufs_cache_destroy(struct marufs_sb_info* sbi);

/* ============================================================================
 * Function declarations - sysfs.c
 * ============================================================================ */

int marufs_sysfs_init(void);
void marufs_sysfs_exit(void);
int marufs_sysfs_register(struct marufs_sb_info* sbi);
void marufs_sysfs_unregister(struct marufs_sb_info* sbi);

/* ============================================================================
 * Function declarations - gc.c (Tombstone GC)
 * ============================================================================ */

int marufs_gc_tombstone_sweep(struct marufs_sb_info* sbi, u32 shard_id);
bool marufs_gc_needs_sweep(struct marufs_sb_info* sbi, u32 shard_id);
int marufs_gc_dead_process_regions(struct marufs_sb_info* sbi);
bool marufs_gc_has_active_delegations(struct marufs_sb_info* sbi,
                                    struct marufs_rat_entry* rat_entry,
                                    u32 rat_entry_id);
bool marufs_is_orphaned(struct marufs_sb_info* sbi, u32 rat_entry_id);
int marufs_gc_start(struct marufs_sb_info* sbi);
void marufs_gc_stop(struct marufs_sb_info* sbi);
int marufs_gc_restart(struct marufs_sb_info* sbi);

#endif /* _MARUFS_H */
