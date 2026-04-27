/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * sysfs_gc.h - GC monitoring sysfs attributes.
 *
 * Read-only / config-only knobs that surface GC state for operators:
 *   deleg_info  - per-region delegation table dump
 *   gc_status   - per-node GC thread liveness + epoch counter
 *
 * Manual GC control (trigger/stop/pause/restart) lives in sysfs_debug.c.
 */

#ifndef _MARUFS_SYSFS_GC_H
#define _MARUFS_SYSFS_GC_H

#include <linux/sysfs.h>

extern struct kobj_attribute deleg_info_attr;
extern struct kobj_attribute gc_status_attr;

#endif /* _MARUFS_SYSFS_GC_H */
