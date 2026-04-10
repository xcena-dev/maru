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
 * │ RAT (4KB header + 256 × 2048B entries)                        │
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
#define MARUFS_STALE_TIMEOUT_NS (5ULL * NSEC_PER_SEC)

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

enum marufs_dax_mode {
	MARUFS_DAX_DEV = 0, /* DEV_DAX: via dax character device */
	MARUFS_DAX_HEAP = 1, /* DAXHEAP: via daxheap buffer (WC mmap for GPU) */
};

/* Forward declarations */
struct marufs_entry_cache;

/* ============================================================================
 * GC orphan tracker types
 * ============================================================================ */
#define MARUFS_GC_ORPHAN_MAX 64

enum marufs_orphan_type {
	MARUFS_ORPHAN_INDEX, /* stale INSERTING index entry */
	MARUFS_ORPHAN_DELEG, /* stale GRANTING delegation entry */
	MARUFS_ORPHAN_DELEG_UNBOUND, /* ACTIVE deleg, birth_time not yet bound */
	MARUFS_ORPHAN_RAT, /* stuck ALLOCATING RAT entry */
	MARUFS_ORPHAN_NRHT, /* stale INSERTING NRHT entry */
};

struct marufs_orphan_tracker {
	void *entry;
	u64 discovered_at;
	enum marufs_orphan_type type;
};

/* ============================================================================
 * Local DRAM cache for shard metadata (read-only after format)
 * ============================================================================
 *
 * Eliminates per-operation CXL reads for shard header fields that never
 * change: bucket/entry array pointers and counts.  Populated once at mount
 * from CXL shard_table, then accessed from local DRAM on every index op.
 */

struct marufs_shard_cache {
	u32 *buckets; /* CXL bucket array pointer */
	struct marufs_index_entry *entries; /* CXL entry array (32B each) */
	struct marufs_shard_header *header; /* CXL shard header (READ-ONLY after format) */
	atomic_t free_hint; /* Next-free scan start (not for correctness) */
	spinlock_t insert_lock; /* Local lock: serializes insert within this node */
};

/* ============================================================================
 * In-memory superblock info
 * ============================================================================
 *
 * Combines device/DAX state with CXL memory layout pointers.
 * One instance per mounted filesystem, stored in sb->s_fs_info.
 */

struct marufs_sb_info {
	/* ---- Device and DAX state ---- */
	void __iomem *base; /* CXL memory base (compatibility) */
	void *dax_base; /* DAX direct access base (virtual) */
	phys_addr_t phys_base; /* Physical base address for DAX mmap */
	long dax_nr_pages; /* DAX mapped page count */
	u64 total_size; /* Total device size (bytes) */
	u32 node_id; /* This node's ID */
	bool use_dax; /* DAX enabled flag */
	struct task_struct *gc_thread; /* Background GC thread */
	atomic_t gc_active; /* 0 = GC should self-terminate */
	atomic_t gc_paused; /* 1 = GC temporarily paused (skip sweep) */
	atomic_t gc_epoch; /* GC cycle counter (liveness check) */
	u32 gc_next_shard; /* Round-robin sweep position */
	enum marufs_dax_mode dax_mode; /* DEV_DAX vs DAXHEAP */
	char daxdev_path[128]; /* DEV_DAX device path */
	struct file *dax_filp; /* DEV_DAX device file for mmap delegation */

#ifdef CONFIG_DAXHEAP
	struct dma_buf *heap_dmabuf; /* daxheap dma_buf handle */
	struct iosys_map heap_map; /* kernel vmap (WB) for metadata access */
#endif

	/* ---- Global Superblock ---- */
	struct marufs_superblock *gsb; /* Pointer in CXL memory */

	/* ---- Shard Table ---- */
	u32 num_shards; /* Number of shards */
	u32 shard_mask; /* num_shards - 1 (for masking) */
	u32 buckets_per_shard; /* Buckets per shard */
	u32 bucket_mask; /* bucket_mask - 1 (for masking) */
	u32 entries_per_shard; /* Entries per shard */

	/* ---- RAT (Region Allocation Table) ---- */
	struct marufs_rat *rat;
	u64 rat_offset;
	u64 regions_start_offset;

	/* ---- Shard Cache (local DRAM, read-only after mount) ---- */
	struct marufs_shard_cache
		*shard_cache; /* Per-shard cached pointers + geometry */

	/* ---- Cache ---- */
	struct marufs_entry_cache *entry_cache; /* File entry cache */

	/* ---- Page mapping mode ---- */
	bool has_struct_pages; /* ZONE_DEVICE struct pages exist (VM_MIXEDMAP capable) */

	/* ---- GC: orphan tracker (DRAM) ---- */
	struct marufs_orphan_tracker gc_orphans[MARUFS_GC_ORPHAN_MAX];
	u32 gc_orphan_count;

	/* ---- GC: NRHT region tracker (DRAM) ---- */
	DECLARE_BITMAP(gc_nrht_bitmap, MARUFS_MAX_RAT_ENTRIES);

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

struct marufs_inode_info {
	u32 region_id; /* Region where data is stored (= RAT entry ID) */
	u32 entry_idx; /* Global index entry index (for writeback) */
	u32 shard_id; /* Shard this entry belongs to */
	u32 rat_entry_id;
	u64 region_offset;
	u32 owner_node_id;
	u32 owner_pid;
	u64 owner_birth_time;
	u64 data_phys_offset; /* Cached: region phys_offset (= data start) */
	struct inode vfs_inode; /* VFS inode (must be last!) */
};

/* ============================================================================
 * Helper macros
 * ============================================================================ */

static inline struct marufs_inode_info *marufs_inode_get(struct inode *inode)
{
	return container_of(inode, struct marufs_inode_info, vfs_inode);
}

/* Zero-initialize all marufs-specific fields of inode_info.
 * Called from alloc_inode (super.c) and new_inode (inode.c). */
static inline void marufs_inode_info_init(struct marufs_inode_info *xi)
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

static inline struct marufs_sb_info *marufs_sb_get(struct super_block *sb)
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
static inline bool marufs_dax_active(struct marufs_sb_info *sbi)
{
	return sbi->use_dax;
}

/* Return current DAX mode */
static inline enum marufs_dax_mode
marufs_dax_get_mode(struct marufs_sb_info *sbi)
{
	return sbi->dax_mode;
}

/* Get direct pointer to device memory at @offset (DAX only) */
static inline void *marufs_dax_ptr(struct marufs_sb_info *sbi, u64 offset)
{
	return sbi->use_dax ? (void *)((char *)sbi->dax_base + offset) : NULL;
}

/* Return total device size in bytes */
static inline u64 marufs_dax_size(struct marufs_sb_info *sbi)
{
	return sbi->total_size;
}

/* Validate that a physical offset is within the DAX mapping */
static inline bool marufs_valid_phys_offset(struct marufs_sb_info *sbi,
					    u64 offset)
{
	return offset > 0 && offset < sbi->total_size && sbi->dax_base;
}

/* Global superblock accessor — returns GSB pointer from CXL memory */
static inline struct marufs_superblock *
marufs_gsb_get(struct marufs_sb_info *sbi)
{
	if (unlikely(!sbi || !sbi->dax_base))
		return NULL;

	struct marufs_superblock *gsb =
		(struct marufs_superblock *)sbi->dax_base;
	MARUFS_CXL_RMB(gsb, sizeof(*gsb) - sizeof(gsb->reserved));
	return gsb;
}

/* RAT accessor — returns RAT pointer (NULL if not loaded) */
static inline struct marufs_rat *marufs_rat_get(struct marufs_sb_info *sbi)
{
	if (unlikely(!sbi))
		return NULL;

	MARUFS_CXL_RMB(sbi->rat, 64); /* Invalidate CL0 (hot I/O metadata) */
	return sbi->rat;
}

/* RAT entry accessor — CL0 RMB included, caller adds CL2 RMB if needed */
static inline struct marufs_rat_entry *
marufs_rat_entry_get(struct marufs_sb_info *sbi, u32 id)
{
	if (unlikely(!sbi || !sbi->rat || id >= MARUFS_MAX_RAT_ENTRIES))
		return NULL;

	struct marufs_rat_entry *e = &sbi->rat->entries[id];
	MARUFS_CXL_RMB(e, 64); /* Invalidate CL0 (hot I/O metadata) */
	return e;
}

/* Direct delegation entry array accessor from RAT entry */
static inline struct marufs_deleg_entry *
marufs_rat_deleg_entries(struct marufs_rat_entry *entry)
{
	return entry->deleg_entries;
}

/* Safe single delegation entry accessor with bounds check + RMB */
static inline struct marufs_deleg_entry *
marufs_rat_deleg_entry(struct marufs_rat_entry *entry, u32 idx)
{
	if (unlikely(idx >= MARUFS_DELEG_MAX_ENTRIES))
		return NULL;

	struct marufs_deleg_entry *de = &entry->deleg_entries[idx];
	MARUFS_CXL_RMB(de, sizeof(*de));
	return de;
}

/* Validate that [offset, offset+size) is within DAX mapping (overflow-safe) */
static inline bool marufs_dax_range_valid(struct marufs_sb_info *sbi,
					  u64 offset, u64 size)
{
	if (unlikely(!sbi->dax_base || size == 0))
		return false;
	if (unlikely(offset >= sbi->total_size))
		return false;
	if (unlikely(size >
		     sbi->total_size - offset)) /* overflow-safe subtraction */
		return false;
	return true;
}

/*
 * marufs_rat_name_matches - check if RAT entry name matches given name
 * Invalidates CXL cache, then compares with null-terminator awareness.
 * Returns true if names match.
 */
static inline bool marufs_rat_name_matches(struct marufs_sb_info *sbi,
					   u32 region_id,
					   const char *name, size_t namelen)
{
	struct marufs_rat_entry *rat_e = marufs_rat_entry_get(sbi, region_id);
	if (!rat_e)
		return false;
	MARUFS_CXL_RMB(rat_e->name, sizeof(rat_e->name));
	return strncmp(rat_e->name, name, namelen) == 0 &&
	       (namelen >= sizeof(rat_e->name) || rat_e->name[namelen] == '\0');
}

/* Validate data_phys_offset + access range is within DAX mapping */
static inline bool marufs_validate_region_addr(struct marufs_sb_info *sbi,
					       u64 data_phys_offset,
					       u64 access_size)
{
	if (unlikely(data_phys_offset == 0))
		return false;
	return marufs_dax_range_valid(sbi, data_phys_offset, access_size);
}

/* ============================================================================
 * Helper inline functions
 * ============================================================================ */

/* Safe shard header accessor with bounds check + RMB */
static inline struct marufs_shard_header *
marufs_shard_header_get(struct marufs_sb_info *sbi, u32 shard_id)
{
	if (unlikely(!sbi || shard_id >= sbi->num_shards))
		return NULL;

	struct marufs_shard_header *sh = sbi->shard_cache[shard_id].header;
	if (unlikely(!sh))
		return NULL;
	MARUFS_CXL_RMB(sh, sizeof(*sh));
	return sh;
}

/* Validate shard geometry is non-zero and within sane bounds */
static inline bool marufs_shard_geometry_valid(u32 num_buckets, u32 num_entries)
{
	u32 limit = MARUFS_REGION_ENTRIES_PER_SHARD;
	return num_buckets > 0 && num_entries > 0 && num_buckets <= limit &&
	       num_entries <= limit;
}

/* Return shard's bucket array pointer (from local DRAM shard_cache) */
static inline u32 *marufs_shard_buckets(struct marufs_sb_info *sbi,
					u32 shard_id)
{
	if (unlikely(shard_id >= sbi->num_shards))
		return NULL;
	return sbi->shard_cache[shard_id].buckets;
}

/* Safe bucket access: validate shard_id and bucket_idx within shard bounds */
static inline u32 *marufs_shard_bucket(struct marufs_sb_info *sbi, u32 shard_id,
				       u32 bucket_idx)
{
	if (unlikely(shard_id >= sbi->num_shards))
		return NULL;
	struct marufs_shard_cache *sc = &sbi->shard_cache[shard_id];
	if (unlikely(bucket_idx >= sbi->buckets_per_shard))
		return NULL;
	if (unlikely(!sc->buckets))
		return NULL;

	u32 *bucket = &sc->buckets[bucket_idx];
	MARUFS_CXL_RMB(bucket, sizeof(*bucket));
	return bucket;
}

/* Return shard's entry array pointer (from local DRAM shard_cache) */
static inline struct marufs_index_entry *
marufs_shard_entries(struct marufs_sb_info *sbi, u32 shard_id)
{
	if (unlikely(shard_id >= sbi->num_shards))
		return NULL;
	return sbi->shard_cache[shard_id].entries;
}

/* Safe entry access: validate shard_id and entry_idx within shard bounds */
static inline struct marufs_index_entry *
marufs_shard_entry(struct marufs_sb_info *sbi, u32 shard_id, u32 entry_idx)
{
	if (unlikely(shard_id >= sbi->num_shards))
		return NULL;
	struct marufs_shard_cache *sc = &sbi->shard_cache[shard_id];
	if (unlikely(entry_idx >= sbi->entries_per_shard ||
		     !marufs_shard_geometry_valid(1, sbi->entries_per_shard)))
		return NULL;
	if (unlikely(!sc->entries))
		return NULL;

	struct marufs_index_entry *e = &sc->entries[entry_idx];
	MARUFS_CXL_RMB(e, sizeof(*e));
	return e;
}

/* ============================================================================
 * Function declarations - super.c
 * ============================================================================ */

int marufs_fill_super(struct super_block *sb, void *data, int silent);

/* ============================================================================
 * Function declarations - region.c (RAT allocator)
 * ============================================================================ */

/* RAT (Region Allocation Table) management */
int marufs_rat_alloc_entry(struct marufs_sb_info *sbi, const char *name,
			   u64 size, u64 offset, u32 *out_rat_entry_id);
void marufs_rat_free_entry(struct marufs_rat_entry *entry);

/* Region initialization (called from ftruncate path) */
int marufs_region_init(struct marufs_sb_info *sbi, u32 rat_entry_id,
		       u64 data_size);

/* ============================================================================
 * Function declarations - index.c (Global index CAS operations)
 * ============================================================================ */

int marufs_index_insert(struct marufs_sb_info *sbi, const char *name,
			size_t namelen, u32 region_id, u32 *out_entry_idx);
int marufs_index_lookup(struct marufs_sb_info *sbi, const char *name,
			size_t namelen, struct marufs_index_entry **out_entry);
int marufs_index_delete(struct marufs_sb_info *sbi, const char *name,
			size_t namelen);

/* ============================================================================
 * Function declarations - inode.c
 * ============================================================================ */

struct inode *marufs_iget(struct super_block *sb,
			  struct marufs_index_entry *entry, u32 shard_id,
			  u32 entry_idx);
struct inode *marufs_new_inode(struct super_block *sb, umode_t mode);
int marufs_write_inode(struct inode *inode, struct writeback_control *wbc);
void marufs_evict_inode(struct inode *inode);

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

void marufs_deleg_entry_clear(struct marufs_deleg_entry *de);
int marufs_check_permission(struct marufs_sb_info *sbi, u32 rat_entry_id,
			    u32 required_perms);
int marufs_deleg_grant(struct marufs_sb_info *sbi, u32 rat_entry_id,
		       struct marufs_perm_req *req);
bool marufs_owner_is_dead(u32 owner_pid, u64 owner_birth_time);

/* ============================================================================
 * Function declarations - cache.c
 * ============================================================================ */

int marufs_cache_init(struct marufs_sb_info *sbi);
void marufs_cache_destroy(struct marufs_sb_info *sbi);

/* ============================================================================
 * Function declarations - sysfs.c
 * ============================================================================ */

int marufs_sysfs_init(void);
void marufs_sysfs_exit(void);
int marufs_sysfs_register(struct marufs_sb_info *sbi);
void marufs_sysfs_unregister(struct marufs_sb_info *sbi);

/* ============================================================================
 * Function declarations - nrht.c (Independent Name-Ref Hash Table)
 * ============================================================================ */

int marufs_nrht_init(struct marufs_sb_info *sbi, u32 nrht_region_id,
		     u32 max_entries, u32 num_shards, u32 num_buckets);
int marufs_nrht_insert(struct marufs_sb_info *sbi, u32 nrht_region_id,
		       const char *name, size_t namelen, u64 name_hash,
		       u64 offset, u32 target_region_id);
int marufs_nrht_lookup(struct marufs_sb_info *sbi, u32 nrht_region_id,
		       const char *name, size_t namelen, u64 name_hash,
		       u64 *out_offset, u32 *out_target_region_id);
int marufs_nrht_delete(struct marufs_sb_info *sbi, u32 nrht_region_id,
		       const char *name, size_t namelen, u64 name_hash);
int marufs_nrht_gc_sweep_all(struct marufs_sb_info *sbi);

/* ============================================================================
 * Function declarations - gc.c (Tombstone GC)
 * ============================================================================ */

void marufs_gc_track_orphan(struct marufs_sb_info *sbi, void *entry,
			    enum marufs_orphan_type type);
int marufs_gc_reclaim_dead_regions(struct marufs_sb_info *sbi);
bool marufs_can_force_unlink(struct marufs_sb_info *sbi, u32 rat_entry_id);
int marufs_gc_start(struct marufs_sb_info *sbi);
void marufs_gc_stop(struct marufs_sb_info *sbi);
int marufs_gc_restart(struct marufs_sb_info *sbi);

#endif /* _MARUFS_H */
