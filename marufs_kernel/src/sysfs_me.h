/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * sysfs_me.h - ME (Mutual Exclusion) sysfs attributes.
 *
 * Surfaces ME state inspection (me_info) and per-CPU stats counters
 * (me_poll_stats, me_fine_stats, me_per_shard_acquire, me_poll_thread_cpu).
 * Production-safe — read-mostly, write paths are reset-only.
 */

#ifndef _MARUFS_SYSFS_ME_H
#define _MARUFS_SYSFS_ME_H

#include <linux/sysfs.h>

extern struct kobj_attribute me_info_attr;
extern struct kobj_attribute me_poll_stats_attr;
extern struct kobj_attribute me_fine_stats_attr;
extern struct kobj_attribute me_per_shard_acquire_attr;
extern struct kobj_attribute me_poll_thread_cpu_attr;

#endif /* _MARUFS_SYSFS_ME_H */
