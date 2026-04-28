/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * marufs.h - MARUFS in-memory core (sbi, DAX/RAT/shard helpers).
 *
 * ============================================================================
 * MARUFS Architecture: CAS-based Lock-free Distributed Model
 * ============================================================================
 *
 * All nodes can concurrently read/write the global index using CAS
 * (Compare-And-Swap) operations. No GCS (Global Chunk Server) needed.
 *
 * On-disk layout lives in marufs_layout.h (umbrella over per-domain
 * layout headers). Per-module entry points live in their own headers
 * (gc.h, acl.h, region.h, index.h, nrht.h, cache.h, inode.h, super.h,
 * file.h, dir.h). This file owns the in-memory `marufs_sb_info` plus
 * inline DAX/RAT/shard accessors that all subsystems reuse.
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
#include <linux/list.h>
#include <linux/mutex.h>
#include <linux/spinlock.h>
#include <linux/types.h>

#ifdef CONFIG_DAXHEAP
#include <linux/iosys-map.h>
struct dma_buf;
#endif

/* On-disk layout + ME types (sbi fields reference both) */
#include "marufs_layout.h"
#include "me.h"

/* GC orphan tracker types — referenced by sbi fields below */
#include "gc.h"

/* Bootstrap slot management */
#include "bootstrap.h"

/* Per-module entry points (kept here so .c files including only marufs.h
 * still see all subsystem APIs — preserves existing include patterns). */
#include "acl.h"
#include "cache.h"
#include "dir.h"
#include "file.h"
#include "index.h"
#include "inode.h"
#include "nrht.h"
#include "region.h"
#include "super.h"

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
struct marufs_me_instance;
struct marufs_nrht_stats_pcpu;

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
	struct marufs_shard_header *header; /* CXL shard header */
	atomic_t free_hint; /* Next-free scan start (not for correctness) */
};

/* ============================================================================
 * In-memory superblock info
 * ============================================================================
 *
 * Combines device/DAX state with CXL memory layout pointers.
 * One instance per mounted filesystem, stored in sb->s_fs_info.
 */

struct marufs_sb_info {
	/* ── Storage backend (DEV_DAX / DAXHEAP) ─────────────────────────── */
	void *dax_base; /* DAX direct access base (virtual) */
	phys_addr_t phys_base; /* Physical base address for DAX mmap */
	long dax_nr_pages; /* DAX mapped page count */
	u64 total_size; /* Total device size (bytes) */
	enum marufs_dax_mode dax_mode; /* DEV_DAX vs DAXHEAP */
	char daxdev_path[128]; /* DEV_DAX device path */
	struct file *dax_filp; /* DEV_DAX device file for mmap delegation */
#ifdef CONFIG_DAXHEAP
	struct dma_buf *heap_dmabuf; /* daxheap dma_buf handle */
	struct iosys_map heap_map; /* kernel vmap (WB) for metadata access */
#endif

	/* ── Node identity ────────────────────────────────────────────────── */
	u32 node_id;

	/* ── Cached on-disk layout pointers (set at mount, valid until umount) */
	struct marufs_superblock *gsb; /* Global superblock */
	struct marufs_bootstrap_slot *bootstrap_slots; /* slot base array */
	struct marufs_rat *rat; /* Region allocation table array */
	void *me_area; /* ME membership area base */

	/* ── Geometry (read from GSB at mount time) ───────────────────────── */
	u32 num_shards;
	u32 shard_mask; /* num_shards - 1 */
	u32 buckets_per_shard;
	u32 bucket_mask; /* buckets_per_shard - 1 */
	u32 entries_per_shard;

	/* ── Per-shard DRAM cache (read-only after mount) ─────────────────── */
	struct marufs_shard_cache *shard_cache;
	bool has_struct_pages; /* ZONE_DEVICE pages exist (VM_MIXEDMAP) */

	/* ── Entry cache ──────────────────────────────────────────────────── */
	struct marufs_entry_cache *entry_cache;

	/* ── Mutual Exclusion (ME) ────────────────────────────────────────── */
	struct marufs_me_instance *me; /* Global ME */
	struct marufs_me_instance *nrht_me[MARUFS_MAX_RAT_ENTRIES];
	struct mutex nrht_me_lock; /* serializes nrht_me[] creation */
	/* Unified poll thread: serves Global ME + all NRHT MEs */
	struct list_head me_list;
	struct mutex me_list_lock;
	struct task_struct *me_poll_thread;

	/* ── NRHT stats (per-CPU) ─────────────────────────────────────────── */
	/* May be NULL before alloc; recorders guard for that. */
	struct marufs_nrht_stats_pcpu __percpu *nrht_stats;

	/* ── Garbage collector ────────────────────────────────────────────── */
	struct task_struct *gc_thread;
	atomic_t gc_active; /* 0 = GC should self-terminate */
	atomic_t gc_paused; /* 1 = GC temporarily paused */
	atomic_t gc_epoch; /* GC cycle counter (liveness check) */
	u32 gc_next_shard; /* Round-robin sweep position */
	struct marufs_orphan_tracker gc_orphans[MARUFS_GC_ORPHAN_MAX];
	u32 gc_orphan_count;
	DECLARE_BITMAP(gc_nrht_bitmap, MARUFS_MAX_RAT_ENTRIES);

	/* ── Bootstrap auto-mount ─────────────────────────────────────────── */
	int bootstrap_slot_idx; /* -1 = not claimed; 0..7 = slot index */
	u64 bootstrap_token; /* our random_token (for verification) */

	/* ── Admin role (lowest active node_id, refreshed dynamically) ───── */
	u32 cached_admin_node_id;
};

/* ============================================================================
 * sbi accessor
 * ============================================================================ */

static inline struct marufs_sb_info *marufs_sb_get(struct super_block *sb)
{
	return sb->s_fs_info;
}

/* ── Bootstrap slot pointer accessor ──────────────────────────────────────
 *
 * marufs_bootstrap_slot_get - return pointer to bootstrap slot @idx.
 * Defined here (after marufs_sb_info) because bootstrap.h is included before
 * the struct definition so it cannot host the inline body.
 */
static inline struct marufs_bootstrap_slot *
marufs_bootstrap_slot_get(struct marufs_sb_info *sbi, int idx)
{
	if (WARN_ON_ONCE(idx < 0 || idx >= MARUFS_BOOTSTRAP_MAX_SLOTS))
		return NULL;
	if (unlikely(!sbi || !sbi->bootstrap_slots))
		return NULL;
	return &sbi->bootstrap_slots[idx];
}

/*
 * marufs_bootstrap_promote_claimed - write CLAIMED to slot[0] with WMB.
 * Convenience for the 3 super.c sites that promote slot[0] after format.
 * Calls marufs_bootstrap_set_status() (declared in bootstrap.h).
 */
static inline void marufs_bootstrap_promote_claimed(struct marufs_sb_info *sbi)
{
	marufs_bootstrap_set_status(sbi, 0, MARUFS_BS_CLAIMED);
}

/* ============================================================================
 * DAX abstraction API
 * ============================================================================
 *
 * All filesystem code uses these helpers to access device memory.
 * Transparently handles DEV_DAX and DAXHEAP modes.
 */

/* Return current DAX mode */
static inline enum marufs_dax_mode
marufs_dax_get_mode(struct marufs_sb_info *sbi)
{
	return sbi->dax_mode;
}

/* Get direct pointer to device memory at @offset (DAX only) */
static inline void *marufs_dax_ptr(struct marufs_sb_info *sbi, u64 offset)
{
	return (void *)((char *)sbi->dax_base + offset);
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

/*
 * marufs_gsb_get - GSB pointer with RMB.
 *
 * Returns sbi->gsb (cached after marufs_read_superblock).
 * marufs_read_superblock itself computes the pointer directly from dax_base
 * and assigns sbi->gsb — it does not call this helper.
 */
static inline struct marufs_superblock *
marufs_gsb_get(struct marufs_sb_info *sbi)
{
	if (unlikely(!sbi || !sbi->gsb))
		return NULL;
	MARUFS_CXL_RMB(sbi->gsb,
		       sizeof(*sbi->gsb) - sizeof(sbi->gsb->reserved));
	return sbi->gsb;
}

/*
 * marufs_rat_get - cached RAT pointer with CL0 RMB.
 *
 * sbi->rat is set by marufs_load_rat before any runtime caller reaches here.
 */
static inline struct marufs_rat *marufs_rat_get(struct marufs_sb_info *sbi)
{
	if (unlikely(!sbi || !sbi->rat))
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

/* ── Typed DAX region accessors ──────────────────────────────────────────── */

/*
 * marufs_shard_header_get - shard header pointer for @shard_id.
 *
 * Returns the cached header pointer from sbi->shard_cache[shard_id].header.
 * Cache is allocated and header pointers populated in
 * marufs_sbi_init_layout_ptrs() at mount start, BEFORE format/read_superblock,
 * so this helper is valid for both format-side writes and post-mount reads.
 */
static inline struct marufs_shard_header *
marufs_shard_header_get(struct marufs_sb_info *sbi, u32 shard_id)
{
	if (unlikely(!sbi || !sbi->shard_cache ||
		     shard_id >= MARUFS_REGION_NUM_SHARDS))
		return NULL;

	struct marufs_shard_header *sh = sbi->shard_cache[shard_id].header;
	if (unlikely(!sh))
		return NULL;

	MARUFS_CXL_RMB(sh, sizeof(*sh));
	return sh;
}

/*
 * marufs_me_area_get - cached ME membership area base.
 * sbi->me_area is set in marufs_fill_super_common before marufs_me_create.
 */
static inline void *marufs_me_area_get(struct marufs_sb_info *sbi)
{
	if (unlikely(!sbi || !sbi->me_area))
		return NULL;
	return sbi->me_area;
}

/*
 * marufs_file_data_at - file data pointer at (data_phys_offset + pos).
 *
 * data_phys_offset is the per-inode physical base; pos is the read offset
 * within that region.  Caller must RMB before accessing the returned pointer.
 */
static inline void *marufs_file_data_at(struct marufs_sb_info *sbi,
					u64 data_phys_offset, loff_t pos)
{
	return marufs_dax_ptr(sbi, data_phys_offset + (u64)pos);
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
					   u32 region_id, const char *name,
					   size_t namelen)
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
 * Shard helpers
 * ============================================================================ */

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
 * sysfs entry points (sysfs.c)
 * ============================================================================ */

int marufs_sysfs_init(void);
void marufs_sysfs_exit(void);
int marufs_sysfs_register(struct marufs_sb_info *sbi);
void marufs_sysfs_unregister(struct marufs_sb_info *sbi);

#endif /* _MARUFS_H */
