/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * cache.h - File entry cache lifecycle.
 */

#ifndef _MARUFS_CACHE_H
#define _MARUFS_CACHE_H

struct marufs_sb_info;

int marufs_cache_init(struct marufs_sb_info *sbi);
void marufs_cache_destroy(struct marufs_sb_info *sbi);

#endif /* _MARUFS_CACHE_H */
