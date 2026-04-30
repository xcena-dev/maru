/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * me_layout.h - On-disk layout for the MARUFS Cross-node Mutual Exclusion
 *               (ME) area.
 *
 * Holds CXL-resident structs, their identifying magic values, and pointer-
 * arithmetic helpers used by format/create paths and bootstrap. Runtime
 * state (DRAM types, inline helpers, public API) lives in me.h.
 */

#ifndef _MARUFS_ME_LAYOUT_H
#define _MARUFS_ME_LAYOUT_H

#include <linux/types.h>

/* No-holder sentinel for cb->holder. */
#define MARUFS_ME_HOLDER_NONE ((u32)0xFFFFFFFF)

/* ── Membership slot status ─────────────────────────────────────────── */
enum marufs_me_membership_status {
	MARUFS_ME_NONE = 0,
	MARUFS_ME_ACTIVE = 1,
};

/*
 * Per-type magics — prefix each CXL-resident ME struct so writers can
 * verify they are actually addressing the intended record type. Guards
 * against stale instance writes after a peer reformat changed the ME
 * area layout (cached offsets now fall inside a different struct).
 */
enum marufs_me_struct_magic {
	MARUFS_ME_CB_MAGIC = 0x4352454D, /* "MERC" */
	MARUFS_ME_MS_MAGIC = 0x534D454D, /* "MEMS" */
	MARUFS_ME_SLOT_MAGIC = 0x4C534D45, /* "EMSL" */
};

/*
 * ME Area Header (64B, 1 cacheline) — describes ME area layout.
 * Stored once at the start of the ME area.
 */
struct marufs_me_header {
	__le32 magic; /*  0: MARUFS_ME_MAGIC */
	__le32 version; /*  4: format version */
	__le32 strategy; /*  8: MARUFS_ME_ORDER / MARUFS_ME_REQUEST */
	__le32 num_shards; /* 12: CB count (Global ME: 1, NRHT ME: N_shard) */
	__le32 max_nodes; /* 16: max nodes supported */
	__le32 poll_interval_us; /* 20: polling interval in microseconds */
	__le64 cb_array_offset; /* 24: CB array offset from ME area start */
	__le64 membership_offset; /* 32: membership slot array offset */
	__le64 request_offset; /* 40: request slot array (0 if order-driven) */
	__le64 total_size; /* 48: total ME area size in bytes */
	__le64 format_generation; /* 56: bumped on every marufs_me_format() —
				   * used to invalidate per-sbi cached ME
				   * instances when the underlying CXL area
				   * has been reformatted (e.g. nrht_init
				   * called again on a reused RAT slot). */
} __attribute__((packed));

/*
 * ME Control Block (64B, 1 cacheline) — per-shard mutual exclusion state.
 * Read-mostly; written only on holder transition. Hot path polling happens
 * on per-node slot doorbell, not here.
 */
struct marufs_me_cb {
	__le32 magic; /*  0: MARUFS_ME_CB_MAGIC */
	__le32 holder; /*  4: current holder node_id (MARUFS_ME_HOLDER_NONE if vacant) */
	__le32 state; /*  8: ME_FREE(0) / ME_HELD(1) / ME_RELEASING(2) */
	__le32 _pad; /* 12 */
	__le64 generation; /* 16: monotonic, bumped on every holder change */
	__le64 acquire_count; /* 24: total acquisitions (stats) */
	__u8 reserved[32]; /* 32: pad to 64B */
} __attribute__((packed));

/*
 * ME Membership Slot (64B, 1 cacheline) — per-node membership + liveness.
 *
 * Each node writes ONLY its own slot. Holder scans slot[holder+1..] to
 * find next ACTIVE node for token pass. Heartbeat lives here (distributed
 * per-node) instead of on the shared CB, so liveness ticks don't
 * invalidate a shared hot CL.
 */
struct marufs_me_membership_slot {
	__le32 magic; /*  0: MARUFS_ME_MS_MAGIC */
	__le32 status; /*  4: MARUFS_ME_NONE / MARUFS_ME_ACTIVE */
	__le32 node_id; /*  8: self node_id (validation) */
	__le32 _pad; /* 12 */
	__le64 joined_at; /* 16: ktime_get_ns() at status change */
	__le64 heartbeat; /* 24: self-tick counter (poll thread advances) */
	__le64 heartbeat_ts; /* 32: ktime_get_ns() at last tick */
	/* 40: per-shard pending bitmap — bit s set ⇒ this node has a hand
	 * up on shard s. Single-writer (owning node). Holder ORs peers' masks
	 * to skip scans. Width caps at MARUFS_NRHT_MAX_NUM_SHARDS (64).
	 */
	__le64 pending_shards_mask; /* 40 */
	__u8 reserved[16]; /* 48: pad to 64B */
} __attribute__((packed));

/*
 * ME Slot (64B, 1 cacheline) — per-(shard, node) slot. Interpretation
 * depends on strategy:
 *
 *   Order-driven (doorbell):
 *     Single-writer: current token holder. Owner NEVER writes its own slot.
 *     Holder bumps @token_seq + snapshots @cb_gen_at_write when passing the
 *     token; owner polls @token_seq locally and enters CS on change.
 *
 *   Request-driven (hand-raise):
 *     Writer ownership is split by @requesting value:
 *       requesting == 0  → owner may write (sets requesting=1 to raise hand)
 *       requesting == 1  → holder may write (sets granted_at, flips to 0)
 *     Token handoff to the granted node is ALSO rung via @token_seq so the
 *     granted node polls a per-node CL instead of the shared CB.
 *
 * Layout: slots[shard_id * max_nodes + internal_idx]
 */
struct marufs_me_slot {
	__le32 magic; /*  0: MARUFS_ME_SLOT_MAGIC */
	__le32 from_node; /*  4: last writer node_id (observability) */

	/* Order-driven doorbell (holder-written) */
	__le64 token_seq; /*  8: monotonic; owner polls this */
	__le64 cb_gen_at_write; /* 16: cb->generation snapshot at pass time */

	/* Request-driven hand-raise (writer-split by @requesting) */
	__le32 requesting; /* 24: 0=idle (self writes), 1=requesting (holder writes) */
	__le32 sequence; /* 28: monotonic seq# (stale request fencing) */
	__le64 requested_at; /* 32: self writes when raising hand */
	__le64 granted_at; /* 40: holder writes on grant */

	__u8 reserved[16]; /* 48: pad to 64B */
} __attribute__((packed));

/* ── CXL address helpers ────────────────────────────────────────────── */

/*
 * Compute CXL addresses of CB / membership / slot entries from
 * (me_area_base, offset, index). Return void* so callers can cast to the
 * typed pointer once. Used during format/create (before typed arrays are
 * cached in the instance) and from nrht bootstrap.
 */
static inline void *marufs_me_cb_at(void *me_area_base, u64 cb_off, u32 s)
{
	return (u8 *)me_area_base + cb_off +
	       (u64)s * sizeof(struct marufs_me_cb);
}

static inline void *marufs_me_membership_at(void *me_area_base, u64 mem_off,
					    u32 n)
{
	return (u8 *)me_area_base + mem_off +
	       (u64)n * sizeof(struct marufs_me_membership_slot);
}

static inline void *marufs_me_slot_at(void *me_area_base, u64 slot_off,
				      u32 max_nodes, u32 s, u32 n)
{
	return (u8 *)me_area_base + slot_off +
	       ((u64)s * max_nodes + n) * sizeof(struct marufs_me_slot);
}

#endif /* _MARUFS_ME_LAYOUT_H */
