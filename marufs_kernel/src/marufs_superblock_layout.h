/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * marufs_superblock_layout.h - Global Superblock on-disk format.
 */

#ifndef _MARUFS_SUPERBLOCK_LAYOUT_H
#define _MARUFS_SUPERBLOCK_LAYOUT_H

#include <linux/types.h>

enum {
	MARUFS_GSB_SIZE = 256, /* Global superblock total size */
};

/*
 * Global Superblock — first 256 bytes of CXL memory.
 * Single instance; describes entire partitioned index layout.
 */
struct marufs_superblock {
	/* ── CL0: identity + layout + integrity (bytes 0–63) ────────── */
	__le32 magic; /*  0: MARUFS_MAGIC */
	__le32 uuid; /*  4: Filesystem UUID (per-format random) */
	__le32 version; /*  8: 1 */
	__le32 _pad0; /* 12: align total_size to 8B */
	__le64 total_size; /* 16: Total CXL memory size */
	__le64 shard_table_offset; /* 24: Shard header array offset */
	__le64 rat_offset; /* 32: RAT (Region Allocation Table) offset */
	__le32 num_shards; /* 40: Number of shards (pow2) */
	__le32 buckets_per_shard; /* 44: buckets per shard */
	__le32 entries_per_shard; /* 48: Index entries per shard */
	__le32 checksum; /* 52: CRC32 */
	__le64 me_area_offset; /* 56: ME area offset (0 = ME disabled) */

	/* ── CL1–CL3: reserved (bytes 64–255) ────────────────────────── */
	__u8 reserved[192]; /* Padding to 256 */
} __attribute__((packed));

#endif /* _MARUFS_SUPERBLOCK_LAYOUT_H */
