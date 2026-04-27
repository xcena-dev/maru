/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * marufs_layout.h - MARUFS on-disk layout (umbrella header).
 *
 * CAS-based lock-free distributed model: all nodes can concurrently
 * read/write the global index. No GCS (Global Chunk Server) needed.
 *
 * This header collects the master layout description: magic numbers,
 * ME area sizing, the global offset table, and compile-time size
 * validators. Per-domain on-disk structs are defined in dedicated
 * headers (superblock/index/rat/nrht), each focused on one subsystem.
 *
 * CXL Memory Layout (packed; only regions are 2MB-aligned):
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
 * ├──────────────────────────────────────────────┤
 * │ ME Area (hdr + CBs + membership ≈ 4KB)      │  dynamic (gsb->me_area_offset)
 * ├──────────── 2MB aligned ─────────────────────┤
 * │ Region 0 data, Region 1 data, ...           │  0x200000
 * └──────────────────────────────────────────────┘
 */

#ifndef _MARUFS_LAYOUT_H
#define _MARUFS_LAYOUT_H

#include <linux/types.h>

#include "marufs_uapi.h"
#include "marufs_endian.h"
#include "marufs_hash.h"

#include "marufs_superblock_layout.h"
#include "marufs_index_layout.h"
#include "marufs_rat_layout.h"
#include "marufs_nrht_layout.h"

/* ── Magic Numbers / Version ───────────────────────────────────────── */
enum marufs_magic {
	MARUFS_MAGIC = 0x4D415255, /* "MARU" */
	MARUFS_SHARD_MAGIC = 0x4D534844, /* "MSHD" */
	MARUFS_RAT_MAGIC = 0x4D524154, /* "MRAT" */
	MARUFS_NRHT_MAGIC = 0x4E524854, /* "NRHT" */
	MARUFS_ME_MAGIC = 0x4D454C4B, /* "MELK" */

	MARUFS_VERSION = 1,
	MARUFS_NRHT_VERSION = 1,
	MARUFS_ME_VERSION = 1,
};

/* ── ME area sizing (referenced by layout offsets and ME init) ────── */
enum {
	MARUFS_ME_HEADER_SIZE = 64, /* ME area header (1 CL) */
	MARUFS_ME_CB_SIZE = 64, /* ME control block (1 CL) */
	MARUFS_ME_MEMBERSHIP_SLOT_SIZE = 64, /* ME membership slot (1 CL) */
	MARUFS_ME_SLOT_SIZE = 64, /* ME per-(shard, node) slot (1 CL) */
};

/*
 * marufs_me_area_size - compute total ME area size in bytes.
 * Layout: [Header 64B] [CB S×64B] [Membership N×64B] [Slot S×N×64B]
 *
 * Slot region is allocated for BOTH strategies:
 *   - order-driven: per-node doorbell (token_seq)
 *   - request-driven: per-node hand-raise + doorbell
 */
static inline u64 marufs_me_area_size(u32 num_shards, u32 max_nodes)
{
	u64 size = MARUFS_ME_HEADER_SIZE;

	size += (u64)num_shards * MARUFS_ME_CB_SIZE;
	size += (u64)max_nodes * MARUFS_ME_MEMBERSHIP_SLOT_SIZE;
	size += (u64)num_shards * max_nodes * MARUFS_ME_SLOT_SIZE;
	return size;
}

/* ── Global offset table ──────────────────────────────────────────── */
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
	MARUFS_ME_AREA_OFFSET = MARUFS_RAT_OFFSET + MARUFS_RAT_HEADER_SIZE +
				MARUFS_MAX_RAT_ENTRIES * MARUFS_RAT_ENTRY_SIZE,
	MARUFS_REGION_OFFSET = MARUFS_ALIGN_2MB,
};

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

#endif /* _MARUFS_LAYOUT_H */
