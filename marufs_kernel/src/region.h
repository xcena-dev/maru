/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * region.h - RAT (Region Allocation Table) and region init entry points.
 */

#ifndef _MARUFS_REGION_H
#define _MARUFS_REGION_H

#include <linux/types.h>

struct marufs_sb_info;
struct marufs_rat_entry;

int marufs_rat_alloc_entry(struct marufs_sb_info *sbi, const char *name,
			   u64 size, u64 offset, u32 *out_rat_entry_id);
void marufs_rat_free_entry(struct marufs_rat_entry *entry);

/* Region initialization (called from ftruncate path) */
int marufs_region_init(struct marufs_sb_info *sbi, u32 rat_entry_id,
		       u64 data_size);

#endif /* _MARUFS_REGION_H */
