/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * me_inline.h - Inline helpers for the MARUFS Cross-node ME.
 *
 * Included by me.h after the DRAM types (marufs_me_shard /
 * marufs_me_instance) are defined — these helpers dereference instance
 * fields and need full struct visibility. NOT a standalone header.
 */

#ifndef _MARUFS_ME_INLINE_H
#define _MARUFS_ME_INLINE_H

/* ── Shard state primitives (DRAM only) ─────────────────────────────── */

/*
 * me_shard_unhold - release-path counterpart of holding++. wmb() orders
 * prior CXL writes (RAT, index, etc.) before the dec becomes visible to
 * the poll thread, so the next holder sees a fully published CXL state.
 * smp_mb() pairs with poll_cycle's smp_mb() for DRAM visibility.
 */
static inline void me_shard_unhold(struct marufs_me_shard *sh)
{
	wmb();
	int v = atomic_dec_if_positive(&sh->holding);
	WARN_ON_ONCE(v < 0); /* unbalanced UNHOLD — caller bug */
	smp_mb();
}

/* Force holding=0 (leave/cleanup path only). Avoids underflow from blind dec. */
static inline void me_shard_reset_holding(struct marufs_me_shard *sh)
{
	atomic_set(&sh->holding, 0);
	smp_mb();
}

/*
 * me_shard_become_holder - publish "we are now the token holder".
 *
 * Callers MUST write DRAM baselines (last_cb_gen / last_token_seq /
 * poll_last_slot_seq) BEFORE this. smp_wmb() pairs with smp_rmb() in
 * me_shard_is_holder: once a reader observes is_holder=true, all prior
 * baseline stores are guaranteed visible.
 */
static inline void me_shard_become_holder(struct marufs_me_shard *sh)
{
	smp_wmb();
	sh->is_holder = true;
}

/*
 * me_shard_lose_holder - "we gave the token away".
 *
 * No barrier needed: a reader observing is_holder=false falls to the
 * wait_for_token loop, which performs its own CB RMB to re-verify
 * ownership.
 */
static inline void me_shard_lose_holder(struct marufs_me_shard *sh)
{
	sh->is_holder = false;
}

/*
 * me_shard_is_holder - reader pair for me_shard_become_holder. smp_rmb
 * ensures any baseline stores (last_cb_gen / last_token_seq /
 * poll_last_slot_seq) published by the writer are visible once
 * is_holder reads true.
 */
static inline bool me_shard_is_holder(const struct marufs_me_shard *sh)
{
	smp_rmb();
	return sh->is_holder;
}

/*
 * me_shard_passable - this node holds the token but no CS is in flight
 * and no local thread is waiting. Used by poll/release to decide whether
 * to forward the token instead of keeping it.
 */
static inline bool me_shard_passable(struct marufs_me_instance *me,
				     u32 shard_id)
{
	struct marufs_me_shard *sh = &me->shards[shard_id];
	smp_mb();
	return atomic_read(&sh->holding) == 0 &&
	       atomic_read(&sh->local_waiters) == 0;
}

/* ── Topology / validity ────────────────────────────────────────────── */

/*
 * marufs_me_is_valid_node - true iff @node_id names a real slot.
 *
 * Valid external node_ids are [1, max_nodes]. Excludes 0 (ORPHAN sentinel)
 * and MARUFS_ME_HOLDER_NONE (0xFFFFFFFF, trivially > max_nodes).
 */
static inline bool marufs_me_is_valid_node(const struct marufs_me_instance *me,
					   u32 node_id)
{
	return node_id >= 1 && node_id <= me->max_nodes;
}

/* ── CXL fetch helpers ──────────────────────────────────────────────── */

/* DRAM shard state — no RMB. Grouped with the CXL fetchers so callers
 * find one site for "give me X for shard_id". */
static inline struct marufs_me_shard *
me_shard_get(struct marufs_me_instance *me, u32 shard_id)
{
	return &me->shards[shard_id];
}

/* Fetch CB for @shard_id with RMB. CXL-resident — every access must
 * invalidate own cache to defeat stale reads from prior holder. */
static inline struct marufs_me_cb *me_cb_get(struct marufs_me_instance *me,
					     u32 shard_id)
{
	struct marufs_me_cb *cb = &me->cbs[shard_id];
	MARUFS_CXL_RMB(cb, sizeof(*cb));
	return cb;
}

/*
 * me_cb_snapshot - RMB CB and return (holder, generation).
 * Pass NULL for @out_gen when only the holder is needed. A single RMB
 * covers both fields (same CL).
 */
static inline u32 me_cb_snapshot(struct marufs_me_cb *cb, u64 *out_gen)
{
	MARUFS_CXL_RMB(cb, sizeof(*cb));
	if (out_gen)
		*out_gen = READ_CXL_LE64(cb->generation);
	return READ_CXL_LE32(cb->holder);
}

/* me_cb_bump_acquire_count - holder records a successful acquire. */
static inline void me_cb_bump_acquire_count(struct marufs_me_cb *cb)
{
	WRITE_LE64(cb->acquire_count, READ_CXL_LE64(cb->acquire_count) + 1);
	MARUFS_CXL_WMB(&cb->acquire_count, sizeof(cb->acquire_count));
}

/* Per-(shard, internal_idx) slot lookup with RMB. Slot fits in one
 * cacheline, so the full RMB is no costlier than a partial one. */
static inline struct marufs_me_slot *me_slot_of(struct marufs_me_instance *me,
						u32 shard_id, u32 idx)
{
	struct marufs_me_slot *slot =
		&me->slots[(u64)shard_id * me->max_nodes + idx];
	MARUFS_CXL_RMB(slot, sizeof(*slot));
	return slot;
}

/* Self-slot shortcut (idx = me->me_idx). */
static inline struct marufs_me_slot *me_my_slot(struct marufs_me_instance *me,
						u32 shard_id)
{
	return me_slot_of(me, shard_id, me->me_idx);
}

/* Membership slot fetch with RMB. Heartbeat / pending mask / status all
 * live here — must be re-fetched from CXL each call. */
static inline struct marufs_me_membership_slot *
me_membership_get(struct marufs_me_instance *me, u32 node_idx)
{
	struct marufs_me_membership_slot *ms = &me->membership[node_idx];
	MARUFS_CXL_RMB(ms, sizeof(*ms));
	return ms;
}

/* Self-membership shortcut. */
static inline struct marufs_me_membership_slot *
me_my_membership_get(struct marufs_me_instance *me)
{
	return me_membership_get(me, me->me_idx);
}

/* Header fetch with RMB. format_generation is the only frequently re-read
 * field; callers compare it against me->cached_generation to detect peer
 * reformat. */
static inline struct marufs_me_header *
me_header_get(struct marufs_me_instance *me)
{
	struct marufs_me_header *header = me->header;
	MARUFS_CXL_RMB(header, sizeof(*header));
	return header;
}

#endif /* _MARUFS_ME_INLINE_H */
