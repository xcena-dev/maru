/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * gc.h - Tombstone / dead-process GC types and entry points.
 *
 * Orphan tracker stored in DRAM on each sbi (gc_orphans[]). GC sweeps
 * stale INSERTING/GRANTING/ALLOCATING entries that were abandoned by
 * dead processes or aborted operations.
 */

#ifndef _MARUFS_GC_H
#define _MARUFS_GC_H

#include <linux/types.h>

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

struct marufs_sb_info;

void marufs_gc_track_orphan(struct marufs_sb_info *sbi, void *entry,
			    enum marufs_orphan_type type);
int marufs_gc_reclaim_dead_regions(struct marufs_sb_info *sbi);
bool marufs_can_force_unlink(struct marufs_sb_info *sbi, u32 rat_entry_id);
int marufs_gc_start(struct marufs_sb_info *sbi);
void marufs_gc_stop(struct marufs_sb_info *sbi);
int marufs_gc_restart(struct marufs_sb_info *sbi);

#endif /* _MARUFS_GC_H */
