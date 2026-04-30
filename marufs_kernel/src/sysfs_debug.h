/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * sysfs_debug.h - Debug / test-only sysfs attributes for MARUFS.
 *
 * Two categories:
 *   1. Fault injection (me_freeze_heartbeat, me_sync_is_holder) for
 *      reproducible ME crash-detection tests.
 *   2. Manual GC control (gc_trigger, gc_stop, gc_pause, gc_restart) —
 *      not used in steady-state operation; reserved for operators
 *      forcing GC sweeps or pausing reaping during diagnostics.
 *
 * Read-only GC monitoring (gc_status) and delegation inspection
 * (deleg_info) live in sysfs_gc.c — those are production-safe.
 */

#ifndef _MARUFS_SYSFS_DEBUG_H
#define _MARUFS_SYSFS_DEBUG_H

#include <linux/sysfs.h>

/* Fault injection attrs. */
extern struct kobj_attribute me_freeze_heartbeat_attr;
extern struct kobj_attribute me_sync_is_holder_attr;

/* Manual GC control attrs. */
extern struct kobj_attribute gc_trigger_attr;
extern struct kobj_attribute gc_stop_attr;
extern struct kobj_attribute gc_pause_attr;
extern struct kobj_attribute gc_restart_attr;

/* Bootstrap dump attr (inject_stuck is now a module param, not sysfs). */
extern struct kobj_attribute bootstrap_dump_attr;

#endif /* _MARUFS_SYSFS_DEBUG_H */
