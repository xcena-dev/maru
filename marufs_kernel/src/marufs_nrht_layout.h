/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * marufs_nrht_layout.h - Independent Name-Ref Hash Table on-disk format.
 *
 * Per-region hash table mapping (name, name_hash) -> (target_region_id,
 * offset). Header + per-shard headers + bucket arrays + entry arrays
 * all live within the region's data area.
 */

#ifndef _MARUFS_NRHT_LAYOUT_H
#define _MARUFS_NRHT_LAYOUT_H

#include <linux/types.h>

#include "marufs_uapi.h" /* MARUFS_NAME_MAX */

/* ── NRHT defaults ────────────────────────────────────────────────── */
enum marufs_nrht_config {
	MARUFS_NRHT_DEFAULT_NUM_SHARDS = 64,
	MARUFS_NRHT_MAX_NUM_SHARDS = 64,
	MARUFS_NRHT_DEFAULT_ENTRIES = 8192,
	MARUFS_NRHT_MAX_ENTRIES = 8192,
	MARUFS_NRHT_DEFAULT_LOAD_FACTOR = 4, /* entries per bucket */
};

/* ── Sizes ────────────────────────────────────────────────────────── */
enum {
	MARUFS_NRHT_HEADER_SIZE = 64, /* 1 CL */
	MARUFS_NRHT_SHARD_HEADER_SIZE = 64, /* 1 CL */
	MARUFS_NRHT_ENTRY_SIZE = 128, /* 2 CL */
};

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
 * Contains per-shard geometry and absolute device offsets for bucket
 * and entry arrays.
 */
struct marufs_nrht_shard_header {
	__le32 num_entries; /*  0: max entries in this shard */
	__le32 num_buckets; /*  4: bucket count in this shard */
	__le64 bucket_array_offset; /*  8: absolute offset in device */
	__le64 entry_array_offset; /* 16: absolute offset in device */
	__le32 free_hint; /* 24: flat scan start hint (best-effort, no CAS) */
	__u8 reserved[36]; /* 28: padding to 64B */
} __attribute__((packed)); /* Total: 64 bytes */

/*
 * NRHT Entry — 128 bytes (2 CL).
 * CL0: hot path — accessed on every chain walk.
 * CL1: cold path — accessed only on hash match for name verification.
 * CPU only fetches CLs actually accessed, so chain walks that miss on
 * hash still touch only 1 CL per hop.
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
	__le32 ref_count; /* 40: user-managed reference count (REF_INC/DEC ioctls) */
	__le32 pin_count; /* 44: user-managed pin count (PIN_INC/DEC ioctls) */
	__u8 reserved0[16]; /* 48: padding to 64B CL boundary */

	/* ── CL1: cold path (bytes 64–127) ─────────────────────────── */
	char name[MARUFS_NAME_MAX + 1]; /* 64: null-terminated name (64B) */
} __attribute__((packed)); /* Total: 128 bytes */

#endif /* _MARUFS_NRHT_LAYOUT_H */
