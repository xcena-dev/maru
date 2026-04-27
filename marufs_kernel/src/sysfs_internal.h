/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * sysfs_internal.h - shared state and helpers across MARUFS sysfs sources.
 *
 * sysfs.c owns the registered-sbi list and the lock that protects it.
 * Domain-split files (sysfs_me.c, sysfs_gc.c, sysfs_nrht.c, sysfs_debug.c)
 * include this header to access them without a forest of duplicated
 * extern declarations.
 */

#ifndef _MARUFS_SYSFS_INTERNAL_H
#define _MARUFS_SYSFS_INTERNAL_H

#include <linux/mutex.h>

#include "marufs.h"

#define MARUFS_MAX_MOUNTS MARUFS_MAX_NODE_ID

extern struct marufs_sb_info *marufs_sysfs_sbi_list[MARUFS_MAX_MOUNTS];
extern struct mutex marufs_sysfs_lock;

/* Return first registered sbi (any is fine for shared-CXL reads). */
struct marufs_sb_info *marufs_sysfs_get_sbi(void);

/* Find sbi by node_id; NULL if not registered. */
struct marufs_sb_info *marufs_sysfs_find_by_node(u32 node_id);

#endif /* _MARUFS_SYSFS_INTERNAL_H */
