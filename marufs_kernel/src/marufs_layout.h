/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * marufs_layout.h - MARUFS Partitioned Global Index on-disk structures
 *
 * CAS-based lock-free distributed model: all nodes can concurrently
 * read/write the global index. No GCS (Global Chunk Server) needed.
 *
 * See enum marufs_layout below for the CXL memory layout diagram.
 */

#ifndef _MARUFS_LAYOUT_H
#define _MARUFS_LAYOUT_H

#include "marufs_uapi.h"

#include <linux/types.h>
#include <linux/unaligned.h>
#include <crypto/sha2.h>

/* ── Magic Numbers / Version ───────────────────────────────────────── */
enum marufs_magic {
	MARUFS_MAGIC = 0x4D415255, /* "MARU" */
	MARUFS_SHARD_MAGIC = 0x4D534844, /* "MSHD" */
	MARUFS_RAT_MAGIC = 0x4D524154, /* "MRAT" */
	MARUFS_NRHT_MAGIC = 0x4E524854, /* "NRHT" */

	MARUFS_VERSION = 1,
	MARUFS_NRHT_VERSION = 1,
};

/* ── Index entry state (CAS target) ──────────────────────────────── */
enum marufs_entry_state {
	MARUFS_ENTRY_EMPTY = 0,
	MARUFS_ENTRY_INSERTING = 1,
	MARUFS_ENTRY_TENTATIVE = 2,
	MARUFS_ENTRY_VALID = 3,
	MARUFS_ENTRY_TOMBSTONE = 4,
};

/* ── Region type (stored in RAT entry CL0) ───────────────────────── */
enum marufs_region_type {
	MARUFS_REGION_DATA = 0, /* normal data region (default) */
	MARUFS_REGION_NRHT = 1, /* NRHT name-ref hash table */
};

/* ── RAT entry state ──────────────────────────────────────────────── */
enum marufs_rat_entry_state {
	MARUFS_RAT_ENTRY_FREE = 0,
	MARUFS_RAT_ENTRY_ALLOCATING = 1,
	MARUFS_RAT_ENTRY_ALLOCATED = 2,
	MARUFS_RAT_ENTRY_DELETING = 3,
};

/* ── Delegation entry state ───────────────────────────────────────── */
enum marufs_deleg_state {
	MARUFS_DELEG_EMPTY = 0,
	MARUFS_DELEG_GRANTING = 1,
	MARUFS_DELEG_ACTIVE = 2,
};

#define MARUFS_DELEG_MAX_ENTRIES MARUFS_DELEG_MAX /* from uapi */

/* ── Sentinels / Limits ─────────────────────────────────────────────── */
#define MARUFS_BUCKET_END 0xFFFFFFFF
/* MARUFS_NAME_MAX is defined in marufs_uapi.h */

/* ── Region index defaults ─────────────────────────────────────────── */

enum marufs_region_config {
	MARUFS_REGION_NUM_SHARDS = 4,
	MARUFS_REGION_BUCKETS_PER_SHARD = 256,
	MARUFS_REGION_ENTRIES_PER_SHARD = 256,
	MARUFS_MAX_REGIONS = 256, /* max files */
	MARUFS_MAX_RAT_ENTRIES = 256,
};

/* ── NRHT defaults ────────────────────────────────────────────────── */

enum marufs_nrht_config {
	MARUFS_NRHT_DEFAULT_NUM_SHARDS = 64,
	MARUFS_NRHT_MAX_NUM_SHARDS = 64,
	MARUFS_NRHT_DEFAULT_ENTRIES = 8192,
	MARUFS_NRHT_MAX_ENTRIES = 8192,
	MARUFS_NRHT_DEFAULT_LOAD_FACTOR = 4, /* entries per bucket */
};

/* ── Structure sizes ──────────────────────────────────────────────── */
enum marufs_struct_size {
	MARUFS_GSB_SIZE = 256, /* Global superblock */
	MARUFS_SHARD_HEADER_SIZE = 64, /* Per-shard header in shard table */
	MARUFS_INDEX_ENTRY_SIZE = 64, /* Hash chain entry (half CL) */
	MARUFS_DELEG_ENTRY_SIZE = 64, /* Single delegation entry */
	MARUFS_RAT_ENTRY_SIZE =
		2048, /* RAT entry (metadata + delegation, 32 CL = 2KB) */
	MARUFS_NRHT_HEADER_SIZE = 64, /* NRHT file header (1 CL) */
	MARUFS_NRHT_SHARD_HEADER_SIZE = 64, /* NRHT per-shard header (1 CL) */
	MARUFS_NRHT_ENTRY_SIZE = 128, /* NRHT unified entry (2 CL) */
};

/*
 * CXL Memory Layout (packed, only regions are 2MB-aligned):
 *
 * ┌──────────────────────────────────────────────┐
 * │ Superblock (256B)                            │  0x00000
 * ├──────────────────────────────────────────────┤
 * │ Shard Table (4 × 64B = 256B)                │  0x00100
 * ├──────────────────────────────────────────────┤
 * │ Bucket Arrays (4 × 256 × 4B = 4KB)          │  0x00200
 * ├──────────────────────────────────────────────┤
 * │ Entry Arrays (4 × 256 × 64B = 64KB)         │  0x01200
 * ├──────────────────────────────────────────────┤
 * │ RAT (hdr + 256 × 2KB ≈ 512KB)               │  0x11200
 * ├──────────── 2MB aligned ─────────────────────┤
 * │ Region 0 data, Region 1 data, ...           │  0x200000
 * └──────────────────────────────────────────────┘
 */
enum marufs_layout {
	MARUFS_ALIGN_2MB = 2 * 1024 * 1024,

	MARUFS_GSB_OFFSET = 0,
	MARUFS_SHARD_TABLE_OFFSET = MARUFS_GSB_OFFSET + MARUFS_GSB_SIZE,
	MARUFS_INDEX_BUCKET_OFFSET =
		MARUFS_SHARD_TABLE_OFFSET +
		MARUFS_REGION_NUM_SHARDS * MARUFS_SHARD_HEADER_SIZE,
	MARUFS_INDEX_ENTRY_OFFSET =
		MARUFS_INDEX_BUCKET_OFFSET +
		MARUFS_REGION_NUM_SHARDS * MARUFS_REGION_BUCKETS_PER_SHARD * 4,
	MARUFS_RAT_OFFSET = MARUFS_INDEX_ENTRY_OFFSET +
			    MARUFS_REGION_NUM_SHARDS *
				    MARUFS_REGION_ENTRIES_PER_SHARD *
				    MARUFS_INDEX_ENTRY_SIZE,
	MARUFS_REGION_OFFSET = MARUFS_ALIGN_2MB,
};

/* ── On-disk structures ────────────────────────────────────────────── */

/*
 * Global Superblock — first 256 bytes of CXL memory.
 * Single instance; describes entire partitioned index layout.
 */
struct marufs_superblock {
	/* ── CL0: identity + layout + integrity (bytes 0–63) ────────── */
	__le32 magic; /* MARUFS_MAGIC */
	__le32 version; /* 1 */
	__le64 total_size; /* Total CXL memory size */
	__le64 shard_table_offset; /* Shard header array offset */
	__le64 rat_offset; /* RAT (Region Allocation Table) offset */
	__le32 num_shards; /* Number of shards (pow2) */
	__le32 buckets_per_shard; /* buckets per shard */
	__le32 entries_per_shard; /* Index entries per shard */
	__le32 checksum; /* CRC32 */

	/* ── CL1–CL3: reserved (bytes 56–255) ────────────────────────── */
	__u8 reserved[208]; /* Padding to 256 */
} __attribute__((packed));

/*
 * Shard Header — 64 bytes each, stored in shard table.
 * One per shard; describes that shard's hash-bucket and entry-array
 * layout within global index pool.
 */
struct marufs_shard_header {
	__le32 magic; /* MARUFS_SHARD_MAGIC */
	__le32 shard_id; /* Shard index [0..num_shards) */
	__le32 num_buckets; /* Hash bucket count (pow2) */
	__le32 num_entries; /* Max entries in this shard */
	__le64 bucket_array_offset; /* Absolute offset in device */
	__le64 entry_array_offset; /* Index entries (64B each) */
	__u8 reserved[32]; /* Padding to 64 */
} __attribute__((packed)); /* Total: 64 bytes */

/*
 * Index Entry — 64 bytes (1 CL).
 * Chain linkage + region_id for hash-based file lookup.
 * Name, file_size, uid/gid/mode, timestamps are all in RAT entry
 * (single source of truth).
 */
struct marufs_index_entry {
	__le32 state; /*  0: CAS target (EMPTY/INSERTING/TENTATIVE/VALID/TOMBSTONE) */
	__le32 next_in_bucket; /*  4: hash chain link (MARUFS_BUCKET_END = end) */
	__le64 name_hash; /*  8: 64-bit SHA-256 truncated hash */
	__le32 region_id; /* 16: RAT entry ID */
	__le32 node_id; /* 20: inserter node (stale INSERTING detection) */
	__le64 created_at; /* 24: ns since epoch (stale INSERTING detection) */
	__u8 reserved[32]; /* Padding to 64 */
} __attribute__((packed)); /* Total: 64 bytes */

/*
 * Delegation Entry — 32 bytes each, stored in region header delegation table.
 * Each entry grants specific permissions to a (node_id, pid) pair.
 */
struct marufs_deleg_entry {
	__le32 state; /* MARUFS_DELEG_EMPTY(0) / MARUFS_DELEG_GRANTING(1) / MARUFS_DELEG_ACTIVE(2) */
	__le32 node_id; /* Target node (0 = any node) */
	__le32 pid; /* Target PID (0 = all processes on node) */
	__le32 perms; /* Permission bitmask (MARUFS_PERM_*) */
	__le64 birth_time; /* PID reuse protection (0 if pid=0) */
	__le64 granted_at; /* Grant timestamp (ns since epoch) */
	__u8 reserved[32]; /* Padding to 64 */
} __attribute__((packed)); /* Total: 64 bytes */

/* ── NRHT (Independent Name-Ref Hash Table) ─────────────────── */

/*
 * NRHT Header — first 64 bytes (1 CL) of an NRHT file.
 * Describes the entire hash table layout within the region's data area.
 */
struct marufs_nrht_header {
	__le32 magic; /*  0: MARUFS_NRHT_MAGIC (0x4E524854) */
	__le32 version; /*  4: format version (1) */
	__le32 num_shards; /*  8: shard count (power of 2) */
	__le32 buckets_per_shard; /* 12: buckets per shard (power of 2) */
	__le32 entries_per_shard; /* 16: max entries per shard */
	__le32 owner_region_id; /* 20: RAT entry ID of this NRHT file */
	__le64 table_size; /* 24: total NRHT allocation size (bytes) */
	__u8 reserved[32]; /* 32: padding to 64B */
} __attribute__((packed)); /* Total: 64 bytes */

/*
 * NRHT Shard Header — 64 bytes each, stored after NRHT header.
 * Contains per-shard geometry and absolute device offsets for
 * bucket and entry arrays.
 */
struct marufs_nrht_shard_header {
	__le32 num_entries; /*  0: max entries in this shard */
	__le32 num_buckets; /*  4: bucket count in this shard */
	__le64 bucket_array_offset; /*  8: absolute offset in device */
	__le64 entry_array_offset; /* 16: absolute offset in device */
	__le32 free_hint; /* 24: flat scan start hint (best-effort, no CAS) */
	__le32 lock;
	__u8 reserved[32]; /* 28: padding to 64B */
} __attribute__((packed)); /* Total: 64 bytes */

/*
 * NRHT Entry — 128 bytes (2 CL).
 * CL0 (bytes 0-63): hot path — accessed on every chain walk.
 * CL1 (bytes 64-127): cold path — accessed only on hash match for name verification.
 * CPU only fetches CLs actually accessed, so chain walks that miss on hash
 * still touch only 1 CL per hop.
 */
struct marufs_nrht_entry {
	/* ── CL0: hot path (bytes 0–63) ────────────────────────────── */
	__le32 state; /*  0: EMPTY(0) / INSERTING(1) / TENTATIVE(2) / VALID(3) / TOMBSTONE(4) */
	__le32 next_in_bucket; /*  4: chain link (BUCKET_END = 0xFFFFFFFF) */
	__le64 name_hash; /*  8: 64-bit SHA-256 truncated hash */
	__le64 offset; /* 16: offset within target region's data area */
	__le32 target_region_id; /* 24: which region this offset refers to */
	__le32 inserter_node; /* 28: node_id that created this entry */
	__le64 created_at; /* 32: ns since epoch (stale INSERTING detection) */
	__u8 reserved0[24]; /* 40: padding to 64B CL boundary */

	/* ── CL1: cold path (bytes 64–127) ─────────────────────────── */
	char name[MARUFS_NAME_MAX + 1]; /* 64: null-terminated name (64B) */
} __attribute__((packed)); /* Total: 128 bytes */

/* ── Region Allocation Table (RAT) Support ──────────────────── */

/*
 * Region Allocation Entry - Tracks variable-sized region files
 * Each entry describes one region file with physically contiguous allocation.
 * Size: 2048 bytes (32 cache lines, 2KB)
 *
 * Embeds delegation table directly (no separate region header pool).
 *
 * Two-phase lifecycle:
 *   open(O_CREAT): state=ALLOCATED, phys_offset=0, size=0 (reservation)
 *   ftruncate(N):  phys_offset and size filled in (physical allocation)
 */
struct marufs_rat_entry {
	/* ── CL0: Hot I/O metadata (64B) ────────────────────────────── */
	__le32 state; /* lifecycle (FREE/ALLOCATING/ALLOCATED/DELETING) */
	__le32 region_type; /* MARUFS_REGION_DATA(0) / MARUFS_REGION_NRHT(1) */
	__le64 phys_offset; /* data area offset (0 = not yet allocated) */
	__le64 size; /* region size in bytes (2MB aligned) */
	__le64 alloc_time; /* allocation timestamp (ns) */
	__le64 modified_at; /* last modification (ns) */
	__u8 reserved0[24];

	/* ── CL1: Name (64B) — cold, read on hash match only ────────── */
	char name[MARUFS_NAME_MAX + 1]; /* null-terminated filename */

	/* ── CL2: ACL — ownership + perms + delegation header (64B) ── */
	__le16 default_perms; /* default non-owner perms (MARUFS_PERM_*) */
	__le16 owner_node_id; /* node ownership (max 64) */
	__le32 owner_pid; /* process ownership */
	__le64 owner_birth_time; /* PID reuse protection */
	__le32 uid; /* POSIX owner UID */
	__le32 gid; /* POSIX owner GID */
	__le16 mode; /* POSIX mode bits */
	__le16 deleg_num_entries; /* active delegation count (max 29) */
	__u8 reserved2[36];

	/* ── CL3-CL31: Delegation entries (29 × 64B = 1856B) ────────── */
	struct marufs_deleg_entry deleg_entries[MARUFS_DELEG_MAX_ENTRIES];
} __attribute__((packed)); /* Total: 2048B (32 CL = 2KB) */

/*
 * Region Allocation Table - Global allocator for variable-sized regions
 * Stored after index pool, before first region.
 * Size: 4KB header + (256 × 2KB entries) = 516KB total
 */

struct marufs_rat {
	/* Header */
	__le32 magic; /* MARUFS_RAT_MAGIC */
	__le32 version; /* 1 */
	__le32 num_entries; /* Number of allocated entries */
	__le32 max_entries; /* MARUFS_MAX_RAT_ENTRIES */

	/* Device info */
	__le64 device_size; /* Total device size */
	__le64 rat_offset; /* Offset of this RAT */
	__le64 regions_start; /* Where region files start */

	/* Allocation stats */
	__le64 total_allocated; /* Total allocated bytes */
	__le64 total_free; /* Remaining free bytes */

	/* Global allocation lock (CAS spinlock for region_init) */
	__le32 alloc_lock; /* 0=unlocked, 1=locked */

	/* Reserved */
	__u8 reserved[68]; /* Padding to 4KB (4036 + 60 bytes above = 4096) */

	/* Entry array follows immediately */
	struct marufs_rat_entry entries[MARUFS_MAX_RAT_ENTRIES];
} __attribute__((packed));

/* ── Compile-time size validation ────────────────────────────────── */

#define MARUFS_BUILD_BUG_ON(cond) ((void)sizeof(char[1 - 2 * !!(cond)]))

static inline void __marufs_verify_structs(void)
{
	MARUFS_BUILD_BUG_ON(sizeof(struct marufs_superblock) !=
			    MARUFS_GSB_SIZE);
	MARUFS_BUILD_BUG_ON(sizeof(struct marufs_shard_header) !=
			    MARUFS_SHARD_HEADER_SIZE);
	MARUFS_BUILD_BUG_ON(sizeof(struct marufs_index_entry) !=
			    MARUFS_INDEX_ENTRY_SIZE);
	MARUFS_BUILD_BUG_ON(sizeof(struct marufs_deleg_entry) !=
			    MARUFS_DELEG_ENTRY_SIZE);
	MARUFS_BUILD_BUG_ON(sizeof(struct marufs_rat_entry) !=
			    MARUFS_RAT_ENTRY_SIZE);
	MARUFS_BUILD_BUG_ON(sizeof(struct marufs_nrht_header) !=
			    MARUFS_NRHT_HEADER_SIZE);
	MARUFS_BUILD_BUG_ON(sizeof(struct marufs_nrht_shard_header) !=
			    MARUFS_NRHT_SHARD_HEADER_SIZE);
	MARUFS_BUILD_BUG_ON(sizeof(struct marufs_nrht_entry) !=
			    MARUFS_NRHT_ENTRY_SIZE);
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

/* Read little-endian field with compiler barrier (DRAM / general use) */
#define READ_LE32(field) le32_to_cpu(READ_ONCE(field))
#define READ_LE64(field) le64_to_cpu(READ_ONCE(field))
#define READ_LE16(field) le16_to_cpu(READ_ONCE(field))

/* Read little-endian field from CXL memory (caller must issue RMB first) */
#define READ_CXL_LE32(field) le32_to_cpu(field)
#define READ_CXL_LE64(field) le64_to_cpu(field)
#define READ_CXL_LE16(field) le16_to_cpu(field)

/* Write little-endian field with compiler barrier */
#define WRITE_LE32(field, val) WRITE_ONCE(field, cpu_to_le32(val))
#define WRITE_LE64(field, val) WRITE_ONCE(field, cpu_to_le64(val))
#define WRITE_LE16(field, val) WRITE_ONCE(field, cpu_to_le16(val))

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

#include <linux/libnvdimm.h>

#define MARUFS_CXL_WMB(addr, len)                          \
	do {                                               \
		wmb();                                     \
		arch_wb_cache_pmem((void *)(addr), (len)); \
	} while (0)

#define MARUFS_CXL_RMB(addr, len)                            \
	do {                                                 \
		arch_invalidate_pmem((void *)(addr), (len)); \
		rmb();                                       \
	} while (0)

#else /* CXL 3.0: hardware coherence guaranteed across hosts */

#define MARUFS_CXL_WMB(addr, len) wmb()
#define MARUFS_CXL_RMB(addr, len) rmb()

#endif /* CONFIG_MARUFS_CXL2_COMPAT */

/*
 * marufs_le32_cas/marufs_le64_cas - Atomic compare-and-swap on little-endian CXL fields.
 * Returns old value in CPU byte order.
 * On success, flushes the write from CPU cache to CXL memory via WMB
 * for cross-node visibility on CXL 2.0 (no hardware coherence).
 * On failure (no write occurred), WMB is skipped.
 *
 * CXL 2.0 post-CAS verification: after WMB (clwb), invalidate the cache line
 * (clflushopt) and re-read from CXL device to confirm no other host overwrote
 * between our cmpxchg and the flush.  If mismatch, report as CAS failure with
 * the actual current value.  CXL 3.0 (hardware coherence) skips this entirely.
 */
static inline u16 marufs_le16_cas(__le16 *ptr, u16 old, u16 new)
{
	u16 exp = cpu_to_le16(old);
	u16 ret = cmpxchg((u16 *)ptr, exp, cpu_to_le16(new));

	if (ret == exp) {
		MARUFS_CXL_WMB(ptr, sizeof(*ptr));
#ifdef CONFIG_MARUFS_CXL2_COMPAT
		MARUFS_CXL_RMB(ptr, sizeof(*ptr));
		if (unlikely(READ_CXL_LE16(*ptr) != new))
			return READ_CXL_LE16(*ptr);
#endif
	}
	return le16_to_cpu(ret);
}

static inline u32 marufs_le32_cas(__le32 *ptr, u32 old, u32 new)
{
	u32 exp = cpu_to_le32(old);
	u32 ret = cmpxchg((u32 *)ptr, exp, cpu_to_le32(new));

	if (ret == exp) {
		MARUFS_CXL_WMB(ptr, sizeof(*ptr));
#ifdef CONFIG_MARUFS_CXL2_COMPAT
		MARUFS_CXL_RMB(ptr, sizeof(*ptr));
		if (unlikely(READ_CXL_LE32(*ptr) != new))
			return READ_CXL_LE32(*ptr);
#endif
	}
	return le32_to_cpu(ret);
}

static inline u64 marufs_le64_cas(__le64 *ptr, u64 old, u64 new)
{
	u64 exp = cpu_to_le64(old);
	u64 ret = cmpxchg((u64 *)ptr, exp, cpu_to_le64(new));

	if (ret == exp) {
		MARUFS_CXL_WMB(ptr, sizeof(*ptr));
#ifdef CONFIG_MARUFS_CXL2_COMPAT
		MARUFS_CXL_RMB(ptr, sizeof(*ptr));
		if (unlikely(READ_CXL_LE64(*ptr) != new))
			return READ_CXL_LE64(*ptr);
#endif
	}
	return le64_to_cpu(ret);
}

/* ── Inline helpers ────────────────────────────────────────────────── */

/*
 * marufs_le32_cas_inc/dec - CAS-based atomic increment/decrement for __le32 on CXL.
 * Safe for concurrent multi-node access. Underflow-safe (clamps to 0).
 */
static inline void marufs_le16_cas_inc(__le16 *p)
{
	u16 old_val;
	do {
		old_val = READ_CXL_LE16(*p);
		if (old_val == U16_MAX)
			return; /* overflow guard */
	} while (marufs_le16_cas(p, old_val, old_val + 1) != old_val);
}

static inline void marufs_le16_cas_dec(__le16 *p)
{
	u16 old_val;
	do {
		old_val = READ_CXL_LE16(*p);
		if (old_val == 0)
			return; /* underflow guard */
	} while (marufs_le16_cas(p, old_val, old_val - 1) != old_val);
}

static inline void marufs_le32_cas_inc(__le32 *p)
{
	u32 old_val;
	do {
		old_val = READ_CXL_LE32(*p);
		if (old_val == U32_MAX)
			return; /* overflow guard */
	} while (marufs_le32_cas(p, old_val, old_val + 1) != old_val);
}

static inline void marufs_le32_cas_dec(__le32 *p)
{
	u32 old_val;
	do {
		old_val = READ_CXL_LE32(*p);
		if (old_val == 0)
			return; /* underflow guard */
	} while (marufs_le32_cas(p, old_val, old_val - 1) != old_val);
}

/*
 * marufs_shard_id - select shard from 64-bit name hash.
 * Uses upper 16 bits (bits 63..48) masked with shard_mask.
 * @name_hash: 64-bit hash of filename
 * @shard_mask: num_shards - 1 (num_shards must be power of 2)
 */
static inline u32 marufs_shard_idx(u64 name_hash, u32 shard_mask)
{
	return (u32)((name_hash >> 48) & shard_mask);
}

/*
 * marufs_bucket_idx - select bucket within shard.
 * Uses bits 47..32 of hash masked with bucket_mask.
 * @name_hash: 64-bit hash of filename
 * @bucket_mask: num_buckets - 1 (num_buckets must be power of 2)
 */
static inline u32 marufs_bucket_idx(u64 name_hash, u32 bucket_mask)
{
	return (u32)((name_hash >> 32) & bucket_mask);
}

/*
 * marufs_make_ino - synthesize VFS inode number from region_id.
 * +2 to skip ino 0 (null/invalid) and ino 1 (root directory).
 */
static inline unsigned long marufs_make_ino(u32 region_id)
{
	return (unsigned long)region_id + 2;
}

/*
 * marufs_ino_to_region - extract region_id from VFS inode number.
 */
static inline u32 marufs_ino_to_region(unsigned long ino)
{
	return (u32)(ino - 2);
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
static inline u64 marufs_hash_name(const char *name, size_t len)
{
	u8 digest[SHA256_DIGEST_SIZE];
	sha256((const u8 *)name, len, digest);
	return get_unaligned_le64(digest);
}

/* ioctl structures and commands are defined in marufs_uapi.h */

#endif /* _MARUFS_LAYOUT_H */
