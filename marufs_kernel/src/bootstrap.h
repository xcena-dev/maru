/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * bootstrap.h - MARUFS bootstrap slot API.
 *
 * Bootstrap is ONLY for mount-time election + format gate.
 * Runtime liveness is owned entirely by ME.
 *
 * Public surface consumed by super.c, sysfs_debug.c.
 */

#ifndef _MARUFS_BOOTSTRAP_H
#define _MARUFS_BOOTSTRAP_H

#include <linux/types.h>
#include "marufs_bootstrap_layout.h"

struct marufs_sb_info;

/* ── Core bootstrap API ─────────────────────────────────────────── */

/*
 * marufs_bootstrap_init_area - no-op documentation call; bootstrap area is
 * zero by construction (fresh device) or by graceful umount EMPTY writes.
 */
void marufs_bootstrap_init_area(void *base);

/*
 * marufs_bootstrap_claim - claim the first available slot.
 *
 * Reusable slots: !magic_ok || status==EMPTY ||
 *                 (status==CLAIMED && me_node_is_dead(idx+1)).
 * FORMATTING slots are never reusable here (steal path handles that).
 *
 * Write order: magic → token → status=CLAIMED → wmb → settle → rmb → verify.
 * Returns 0 with *out_slot_idx set on success.
 * Returns -EAGAIN if lost the race (caller retries from top).
 * Returns -EBUSY  if all slots occupied and live.
 */
int marufs_bootstrap_claim(struct marufs_sb_info *sbi, int *out_slot_idx);

/*
 * marufs_bootstrap_claim_explicit - claim slot[node_id-1] for manual mount.
 *
 * Returns -EBUSY if slot is live (CLAIMED + ME alive).
 */
int marufs_bootstrap_claim_explicit(struct marufs_sb_info *sbi, u32 node_id);

/*
 * marufs_bootstrap_release - write status=EMPTY (clean umount).
 * No kthread to stop. No heartbeat write. Just the EMPTY transition.
 */
void marufs_bootstrap_release(struct marufs_sb_info *sbi);

/*
 * marufs_bootstrap_wait_for_format - joiner polls GSB magic until format done.
 *
 * If slot[0].status==FORMATTING and bootstrap_format_timeout_ms elapses
 * without GSB magic appearing, returns -EAGAIN so caller can steal.
 * Returns 0 when GSB magic is valid.
 */
int marufs_bootstrap_wait_for_format(struct marufs_sb_info *sbi);

/*
 * marufs_bootstrap_steal_stuck_slot0 - recover from a crashed formatter.
 *
 * Overwrites slot[0] with our token, writes status=FORMATTING, settle+verify.
 * Zeroes format area (preserving bootstrap area) on success.
 *
 * Returns 0 if we took ownership of slot[0].
 * Returns -EAGAIN if another node won the steal race.
 */
int marufs_bootstrap_steal_stuck_slot0(struct marufs_sb_info *sbi);

/*
 * marufs_bootstrap_set_status - write status field to slot @slot_idx with WMB.
 * Use marufs_bootstrap_promote_claimed() for the common slot[0] CLAIMED case.
 */
void marufs_bootstrap_set_status(struct marufs_sb_info *sbi, int slot_idx,
				 enum marufs_bootstrap_status status);

/* ── Sysfs debug helpers ────────────────────────────────────────── */

/*
 * marufs_bootstrap_dump_slots - print slot table to @buf, bounded by @bufsize.
 */
ssize_t marufs_bootstrap_dump_slots(struct marufs_sb_info *sbi, char *buf,
				    size_t bufsize);

/*
 * marufs_bootstrap_should_inject_stuck - true when the
 * bootstrap_inject_stuck_formatter module param is non-zero.
 *
 * Must be checked via this helper (not sbi field) because the mount path
 * needs to read it before sbi is fully constructed.
 */
bool marufs_bootstrap_should_inject_stuck(void);

#endif /* _MARUFS_BOOTSTRAP_H */
