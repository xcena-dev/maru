/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * marufs_rat_layout.h - Region Allocation Table on-disk format.
 *
 * RAT entries embed the per-region delegation table directly (no separate
 * region header pool). One RAT slot tracks one region file.
 */

#ifndef _MARUFS_RAT_LAYOUT_H
#define _MARUFS_RAT_LAYOUT_H

#include <linux/types.h>

#include "marufs_uapi.h" /* MARUFS_NAME_MAX, MARUFS_DELEG_MAX */

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

/* ── Capacity ─────────────────────────────────────────────────────── */
enum {
	MARUFS_MAX_REGIONS = 256, /* max files */
	MARUFS_MAX_RAT_ENTRIES = 256,
};

/* ── Sizes ────────────────────────────────────────────────────────── */
enum {
	MARUFS_DELEG_ENTRY_SIZE = 64,
	MARUFS_RAT_ENTRY_SIZE = 2048, /* 32 CL = 2KB */
	MARUFS_RAT_HEADER_SIZE = 128,
};

/*
 * Delegation Entry — 64 bytes each, stored in region header delegation table.
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

/*
 * Region Allocation Entry - tracks variable-sized region files.
 * Each entry describes one region file with physically contiguous allocation.
 * Size: 2048 bytes (32 cache lines, 2KB).
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
 * Region Allocation Table - global allocator for variable-sized regions.
 * Stored after index pool, before first region.
 * Size: 4KB header + (256 × 2KB entries) = 516KB total.
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

#endif /* _MARUFS_RAT_LAYOUT_H */
