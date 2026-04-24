// SPDX-License-Identifier: GPL-2.0-only
/*
 * me_request.c - Request-driven ME strategy
 *
 * Nodes signal lock demand via per-(shard, node) request slots.
 * Holder scans request slots and grants to a requester by
 * transferring the token (writing holder=requester in CB).
 *
 * When no requests are pending, the holder retains the token
 * (unlike order-driven which always circulates).
 *
 * Request slot layout: slots[shard_id * max_nodes + node_id]
 */

#include <linux/delay.h>
#include <linux/timekeeping.h>

#include "marufs.h"
#include "me.h"
#include "me_stats.h"

/* ── Helpers ──────────────────────────────────────────────────────── */

/*
 * request_scan_and_grant - holder scans request slots, grants to first
 * requester. Scan starts at (me_idx+1) wrapping — round-robin fairness
 * prevents low-id starvation under sustained contention.
 * Caller must be current holder, idle (hold counter 0).
 * Returns true iff token was granted.
 *
 * Full scan variant — used by release path where mask pre-collection has
 * no amortization benefit.
 */
static bool request_scan_and_grant(struct marufs_me_instance *me, u32 shard_id)
{
	for (u32 i = 1; i < me->max_nodes; i++) {
		u32 idx = (me->me_idx + i) % me->max_nodes;
		struct marufs_me_slot *rs = me_slot_of(me, shard_id, idx);
		MARUFS_CXL_RMB(rs, sizeof(*rs));
		atomic64_inc(&me->poll_rmb_slot);

		if (READ_CXL_LE32(rs->requesting)) {
			u64 now = ktime_get_ns();
			u64 requested_at = READ_CXL_LE64(rs->requested_at);
			WRITE_LE64(rs->granted_at, now);
			MARUFS_CXL_WMB(&rs->granted_at, sizeof(rs->granted_at));
			me_stats_record_grant_age(me, now, requested_at);
			me_pass_token(me, shard_id, idx + 1); /* ext node_id */
			return true;
		}
	}
	return false;
}

/*
 * request_scan_and_grant_masked - filtered scan using pre-collected
 * pending-mask snapshots from peers' membership CLs. Skips nodes whose
 * bit for @shard_id is clear, avoiding the slot RMB entirely.
 *
 * Invariant: a set bit was written AFTER slot.requesting=1 with WMB, so
 * reading requesting==0 on a node with bit set can only mean the peer
 * just cleared its hand (post-CS) — benign, continue scan.
 */
static bool request_scan_and_grant_masked(struct marufs_me_instance *me,
					  u32 shard_id, const u64 *node_pending)
{
	u64 bit = 1ULL << shard_id;

	for (u32 i = 1; i < me->max_nodes; i++) {
		u32 idx = (me->me_idx + i) % me->max_nodes;
		if (!(node_pending[idx] & bit))
			continue;

		struct marufs_me_slot *rs = me_slot_of(me, shard_id, idx);
		MARUFS_CXL_RMB(rs, sizeof(*rs));
		atomic64_inc(&me->poll_rmb_slot);

		if (READ_CXL_LE32(rs->requesting)) {
			u64 now = ktime_get_ns();
			u64 requested_at = READ_CXL_LE64(rs->requested_at);
			WRITE_LE64(rs->granted_at, now);
			MARUFS_CXL_WMB(&rs->granted_at, sizeof(rs->granted_at));
			me_stats_record_grant_age(me, now, requested_at);
			me_pass_token(me, shard_id, idx + 1);
			return true;
		}
	}
	return false;
}

static inline void request_clear_own(struct marufs_me_instance *me,
				     u32 shard_id)
{
	struct marufs_me_slot *rs = me_my_slot(me, shard_id);
	WRITE_LE32(rs->requesting, 0);
	MARUFS_CXL_WMB(rs, sizeof(*rs));

	me_membership_clear_pending(me, shard_id);
}

/* ── Poll cycle ───────────────────────────────────────────────────── */

static void request_poll_cycle(struct marufs_me_instance *me)
{
	bool ticked_hb = false;
	u64 node_pending[MARUFS_ME_MAX_NODES] = { 0 };
	u64 peers_pending = 0;
	u32 successor = me->node_id;
	bool successor_found = false;

	u64 t0 = ktime_get_ns();

	/* Single membership pass: gather pending masks + pick round-robin
	 * successor. S×N → N membership RMBs/cycle vs. per-shard next_active.
	 */
	for (u32 i = 1; i <= me->max_nodes; i++) {
		u32 idx = (me->me_idx + i) % me->max_nodes;
		struct marufs_me_membership_slot *ms = &me->membership[idx];
		MARUFS_CXL_RMB(ms, sizeof(*ms));
		atomic64_inc(&me->poll_rmb_membership);

		if (READ_CXL_LE32(ms->status) != MARUFS_ME_ACTIVE)
			continue;

		node_pending[idx] = READ_CXL_LE64(ms->pending_shards_mask);
		if (idx == me->me_idx)
			continue;

		peers_pending |= node_pending[idx];
		if (!successor_found) {
			successor = idx + 1;
			successor_found = true;
		}
	}

	u64 t1 = ktime_get_ns();
	u64 scan_ns_total = 0;

	for (u32 s = 0; s < me->num_shards; s++) {
		struct marufs_me_shard *sh = &me->shards[s];
		sh->cached_successor = successor;

		/* Receiver doorbell: bump ⇒ peer granted token. */
		struct marufs_me_slot *my_slot = me_my_slot(me, s);
		MARUFS_CXL_RMB(&my_slot->token_seq, sizeof(my_slot->token_seq));
		atomic64_inc(&me->poll_rmb_slot);
		u64 cur_seq = READ_CXL_LE64(my_slot->token_seq);

		if (cur_seq != sh->poll_last_slot_seq) {
			sh->poll_last_slot_seq = cur_seq;
			ME_BECOME_HOLDER(sh);
		}

		if (!sh->is_holder)
			continue;

		/* Holder path: tick heartbeat once, grant if passable. */
		if (!ticked_hb) {
			me_membership_tick_heartbeat(me);
			ticked_hb = true;
		}
		if (me_shard_passable(me, s) && (peers_pending & (1ULL << s))) {
			u64 ts0 = ktime_get_ns();
			request_scan_and_grant_masked(me, s, node_pending);
			scan_ns_total += ktime_get_ns() - ts0;
		}
	}

	u64 t2 = ktime_get_ns();
	struct marufs_me_stats_pcpu *st = this_cpu_ptr(me->stats);
	st->poll_ns_membership += t1 - t0;
	st->poll_ns_scan += scan_ns_total;
	st->poll_ns_doorbell += (t2 - t1) - scan_ns_total;
}

/* ── Acquire: write request slot, wait for grant ──────────────────── */

static int request_acquire(struct marufs_me_instance *me, u32 shard_id)
{
	/* Intra-node serialization (see order_acquire comment). */
	struct marufs_me_shard *sh = &me->shards[shard_id];
	atomic_inc(&sh->local_waiters);
	mutex_lock(&sh->local_lock);
	atomic_dec(&sh->local_waiters);
	me_stats_lock_acquired(sh);
	me_stats_bump_shard_acquire(me, shard_id);

	/* Fast path: previous release kept the token. ME_HOLD's smp_mb()
	 * pairs with poll_cycle's to close the holding/holder read race.
	 */
	ME_HOLD(me, shard_id);

	struct marufs_me_cb *cb = &me->cbs[shard_id];
	MARUFS_CXL_RMB(cb, sizeof(*cb));
	if (READ_CXL_LE32(cb->holder) == me->node_id) {
		me_cb_bump_acquire_count(cb);
		return 0;
	}

	/* Raise hand, wait for grant. */
	struct marufs_me_slot *rs = me_my_slot(me, shard_id);
	WRITE_LE32(rs->sequence, READ_CXL_LE32(rs->sequence) + 1);
	WRITE_LE32(rs->requesting, 1);
	WRITE_LE64(rs->requested_at, ktime_get_ns());
	MARUFS_CXL_WMB(rs, sizeof(*rs));
	me_membership_set_pending(me, shard_id);

	int ret = marufs_me_wait_for_token(me, shard_id);
	request_clear_own(me, shard_id);

	if (ret == 0) {
		me_cb_bump_acquire_count(cb);
		return 0;
	}

	ME_UNHOLD(me, shard_id);
	me_stats_lock_released(me, sh);
	mutex_unlock(&sh->local_lock);
	return ret;
}

/* ── Release ──────────────────────────────────────────────────────── */

static void request_release(struct marufs_me_instance *me, u32 shard_id)
{
	ME_UNHOLD(me, shard_id);

	if (me_shard_passable(me, shard_id))
		request_scan_and_grant(me, shard_id);

	struct marufs_me_shard *sh = &me->shards[shard_id];
	me_stats_lock_released(me, sh);
	mutex_unlock(&sh->local_lock);
}

const struct marufs_me_ops marufs_me_request_ops = {
	.acquire = request_acquire,
	.release = request_release,
	.poll_cycle = request_poll_cycle,
	.join = marufs_me_common_join,
	.leave = marufs_me_common_leave,
};
