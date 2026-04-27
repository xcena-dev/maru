/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * marufs_index_layout.h - Global hash index on-disk format.
 *
 * Shard table headers and per-bucket-chain index entries.
 * Lock-free CAS protocol on `state` field drives insert/delete.
 */

#ifndef _MARUFS_INDEX_LAYOUT_H
#define _MARUFS_INDEX_LAYOUT_H

#include <linux/types.h>

/* ── Index entry state (CAS target) ──────────────────────────────── */
enum marufs_entry_state {
	MARUFS_ENTRY_EMPTY = 0,
	MARUFS_ENTRY_INSERTING = 1,
	MARUFS_ENTRY_TENTATIVE = 2,
	MARUFS_ENTRY_VALID = 3,
	MARUFS_ENTRY_TOMBSTONE = 4,
};

/* ── Region index defaults ─────────────────────────────────────────── */
enum marufs_region_config {
	MARUFS_REGION_NUM_SHARDS = 4,
	MARUFS_REGION_BUCKETS_PER_SHARD = 256,
	MARUFS_REGION_ENTRIES_PER_SHARD = 256,
};

/* ── Sentinels / Sizes ─────────────────────────────────────────────── */
#define MARUFS_BUCKET_END 0xFFFFFFFF

enum {
	MARUFS_SHARD_HEADER_SIZE = 64,
	MARUFS_INDEX_ENTRY_SIZE = 64,
};

/*
 * Shard Header — 64 bytes each, stored in shard table.
 * One per shard; describes bucket and entry-array layout within the
 * global index pool.
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

#endif /* _MARUFS_INDEX_LAYOUT_H */
