/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * marufs_bootstrap_layout.h - Bootstrap slot area on-disk format.
 *
 * Bootstrap area sits at MARUFS_BOOTSTRAP_AREA_OFFSET (immediately after the 256B
 * Global Superblock) and provides a node-election table for auto-mount.
 * Each slot is exactly one CL (64B). Slot i corresponds to node_id (i+1).
 *
 * Bootstrap is ONLY for mount-time election and format gate.
 * Runtime liveness is owned entirely by ME (not bootstrap heartbeat).
 *
 * Slot write order (publication gate):
 *   magic → random_token → status
 *
 * Status field is the publication gate: peers treat a slot as claimed only
 * when status != EMPTY AND random_token != 0.
 */

#ifndef _MARUFS_BOOTSTRAP_LAYOUT_H
#define _MARUFS_BOOTSTRAP_LAYOUT_H

#include <linux/types.h>

/*
 * Bootstrap slot status values.
 * EMPTY = 0 is ABI-pinned: zero-init via memset must equal EMPTY.
 *
 * Reuse rule:
 *   - EMPTY slots are always claimable.
 *   - CLAIMED slots whose ME node is dead (me_node_is_dead) are claimable.
 *   - FORMATTING slots are NOT claimable (format in progress on slot[0]).
 *     Stuck-formatter steal handles recovery via timeout + token overwrite.
 * Graceful umount writes EMPTY directly.
 */
enum marufs_bootstrap_status {
	MARUFS_BS_EMPTY = 0, /* available for new claim */
	MARUFS_BS_CLAIMED = 1, /* node mounted and active */
	MARUFS_BS_FORMATTING = 2, /* slot[0] only: format in progress */
};

/*
 * Bootstrap slot — exactly 64B (one CL).
 *
 *  0: magic        — MARUFS_BOOTSTRAP_MAGIC when slot is in use
 *  4: status       — enum marufs_bootstrap_status (publication gate)
 *  8: random_token — per-claim nonce for race verification (0 reserved)
 * 16..63: reserved (former heartbeat + claim_ts folded here)
 */
struct marufs_bootstrap_slot {
	__le32 magic; /*  0 */
	__le32 status; /*  4 */
	__le64 random_token; /*  8 */
	__u8 reserved[48]; /* 16..63 */
} __attribute__((packed));

#endif /* _MARUFS_BOOTSTRAP_LAYOUT_H */
