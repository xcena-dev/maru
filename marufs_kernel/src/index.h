/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * index.h - Global CAS hash index entry points.
 */

#ifndef _MARUFS_INDEX_H
#define _MARUFS_INDEX_H

#include <linux/types.h>

struct marufs_sb_info;
struct marufs_index_entry;

int marufs_index_insert(struct marufs_sb_info *sbi, const char *name,
			size_t namelen, u32 region_id, u32 *out_entry_idx);
int marufs_index_lookup(struct marufs_sb_info *sbi, const char *name,
			size_t namelen, struct marufs_index_entry **out_entry);
int marufs_index_delete(struct marufs_sb_info *sbi, const char *name,
			size_t namelen);

#endif /* _MARUFS_INDEX_H */
