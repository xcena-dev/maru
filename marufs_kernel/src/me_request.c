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
 * request_pass_token - if @node_idx is requesting, grant + pass token to it.
 *
 * Common tail of full-scan and masked-scan grant loops. Returns true iff
 * the token was actually transferred.
 */
static bool request_pass_token(struct marufs_me_instance *me, u32 shard_id,
			       u32 node_idx)
{
	struct marufs_me_slot *rs = me_slot_of(me, shard_id, node_idx);
	if (READ_CXL_LE32(rs->requesting)) {
		u64 now = ktime_get_ns();
		u64 requested_at = READ_CXL_LE64(rs->requested_at);
		WRITE_LE64(rs->granted_at, now);
		MARUFS_CXL_WMB(&rs->granted_at, sizeof(rs->granted_at));
		me_stats_record_grant_age(me, now, requested_at);
		marufs_me_pass_token(me, shard_id, node_idx + 1);
		return true;
	}
	return false;
}
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
		u32 node_idx = (me->me_idx + i) % me->max_nodes;
		if (request_pass_token(me, shard_id, node_idx)) {
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
		u32 node_idx = (me->me_idx + i) % me->max_nodes;
		if (!(node_pending[node_idx] & bit))
			continue;

		if (request_pass_token(me, shard_id, node_idx)) {
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
	marufs_me_membership_clear_pending(me, shard_id);
}

/* ── Poll cycle ───────────────────────────────────────────────────── */

static void request_poll_cycle(struct marufs_me_instance *me)
{
	/* Fault injection: simulate a crashed node — skip the entire cycle
	 * (no heartbeat, no grant, no doorbell handling).
	 */
	if (atomic_read(&me->debug_freeze_poll))
		return;

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
		struct marufs_me_membership_slot *ms =
			me_membership_get(me, idx);

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
		struct marufs_me_shard *sh = me_shard_get(me, s);
		sh->cached_successor = successor;

		/* Receiver doorbell: bump ⇒ peer granted token. Use the
		 * slot's cb_gen_at_write (already RMB'd via me_my_slot) to
		 * verify freshness — avoids hammering the shared CB cacheline. */
		struct marufs_me_slot *my_slot = me_my_slot(me, s);
		u64 cur_seq = READ_CXL_LE64(my_slot->token_seq);
		if (cur_seq != sh->poll_last_slot_seq) {
			sh->poll_last_slot_seq = cur_seq;
			me_shard_become_holder(sh);
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
	marufs_me_hold(me, shard_id);

	struct marufs_me_cb *cb = me_cb_get(me, shard_id);
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
	marufs_me_membership_set_pending(me, shard_id);

	int ret = marufs_me_wait_for_token(me, shard_id);
	request_clear_own(me, shard_id);

	if (ret == 0) {
		me_cb_bump_acquire_count(cb);
		return 0;
	}

	marufs_me_unhold(me, shard_id);
	return ret;
}

/* ── Release ──────────────────────────────────────────────────────── */

static void request_release(struct marufs_me_instance *me, u32 shard_id)
{
	/* Cannot use marufs_me_unhold(): scan_and_grant must execute between
	 * unhold (so passable check sees holding=0) and mutex_unlock (so a
	 * concurrent local acquire doesn't interleave). */
	struct marufs_me_shard *sh = me_shard_get(me, shard_id);
	me_shard_unhold(sh);

	if (me_shard_passable(me, shard_id))
		request_scan_and_grant(me, shard_id);

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
