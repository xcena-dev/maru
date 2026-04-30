// SPDX-License-Identifier: GPL-2.0-only
/*
 * me_order.c - Order-driven (token ring) ME strategy
 *
 * Token circulates among ACTIVE nodes in ring order.
 *
 * Poll thread roles:
 *   Holder: increment heartbeat, pass token if not holding.
 *   Non-holder: monitor heartbeat for crash detection,
 *               pre-compute cached_successor for O(0) token pass.
 *
 * Acquire/release:
 *   acquire() sets holding[shard]=true, waits for token arrival.
 *   release() sets holding[shard]=false, poll thread passes token.
 */

#include <linux/delay.h>
#include <linux/timekeeping.h>

#include "marufs.h"
#include "me.h"
#include "me_stats.h"

/* ── Poll cycle ───────────────────────────────────────────────────── */

static void order_poll_cycle(struct marufs_me_instance *me)
{
	/* Fault injection: simulate a crashed node — skip the entire cycle
	 * (no heartbeat, no grant, no doorbell handling).
	 */
	if (atomic_read(&me->debug_freeze_poll))
		return;

	bool ticked_hb = false;
	u64 t0 = ktime_get_ns();
	u32 successor = marufs_me_next_active(me, me->node_id);
	u64 t1 = ktime_get_ns();
	u64 scan_ns_total = 0;

	for (u32 s = 0; s < me->num_shards; s++) {
		/* Receiver doorbell: bump ⇒ peer passed token. */
		struct marufs_me_slot *my_slot = me_my_slot(me, s);
		u64 cur_seq = READ_CXL_LE64(my_slot->token_seq);

		struct marufs_me_shard *sh = me_shard_get(me, s);
		sh->cached_successor = successor;
		if (cur_seq != sh->poll_last_slot_seq) {
			sh->poll_last_slot_seq = cur_seq;
			me_shard_become_holder(sh);
		}

		if (!sh->is_holder)
			continue;

		if (!ticked_hb) {
			me_membership_tick_heartbeat(me);
			ticked_hb = true;
		}
		if (me_shard_passable(me, s) && successor != me->node_id) {
			u64 ts0 = ktime_get_ns();
			marufs_me_pass_token(me, s, successor);
			scan_ns_total += ktime_get_ns() - ts0;
		}
	}

	u64 t2 = ktime_get_ns();
	struct marufs_me_stats_pcpu *st = this_cpu_ptr(me->stats);
	st->poll_ns_membership += t1 - t0;
	st->poll_ns_scan += scan_ns_total;
	st->poll_ns_doorbell += (t2 - t1) - scan_ns_total;
}

/* ── Acquire: wait for token arrival ──────────────────────────────── */

static int order_acquire(struct marufs_me_instance *me, u32 shard_id)
{
	marufs_me_hold(me, shard_id);

	int ret = marufs_me_wait_for_token(me, shard_id);
	if (ret == 0) {
		me_cb_bump_acquire_count(me_cb_get(me, shard_id));
		return 0;
	}

	marufs_me_unhold(me, shard_id);
	return ret;
}

/* ── Release: pass token directly if idle ─────────────────────────── */
/*
 * Direct pass bypasses the poll thread — rapid acquire/release cycles
 * would otherwise starve other nodes (poll can't catch the brief idle
 * window). Keep the token if local waiters exist to avoid cross-node
 * ping-pong when one node dominates this shard.
 */
static void order_release(struct marufs_me_instance *me, u32 shard_id)
{
	struct marufs_me_shard *sh = me_shard_get(me, shard_id);
	me_shard_unhold(sh);

	if (me_shard_passable(me, shard_id)) {
		u32 succ = sh->cached_successor;

		if (succ == me->node_id) {
			succ = marufs_me_next_active(me, me->node_id);
			sh->cached_successor = succ;
		}
		if (succ != me->node_id)
			marufs_me_pass_token(me, shard_id, succ);
	}

	me_stats_lock_released(me, sh);
	mutex_unlock(&sh->local_lock);
}

/* ── Leave: token-gated cleanup ───────────────────────────────────── */

/*
 * order_leave - own the token on each shard (if possible), then pass.
 *
 * Success path: acquire forces sole-writer status for this node's slot,
 * so handoff races only with our own code.
 * Failure path: CB may still list us as holder (spurious timeout / ring
 * stuck). If so we rewrite CB directly — the generation bump makes any
 * in-flight doorbell from peers be discarded as stale.
 */
static void order_leave(struct marufs_me_instance *me)
{
	for (u32 s = 0; s < me->num_shards; s++) {
		int ret = me->ops->acquire(me, s);
		bool acquired = (ret == 0);
		u32 holder;

		if (acquired)
			holder = me->node_id;
		else
			holder = me_cb_snapshot(&me->cbs[s], NULL);

		if (holder == me->node_id)
			marufs_me_pass_token(me, s, me_leave_successor(me));

		if (acquired)
			marufs_me_unhold(me, s);

		me_shard_reset_holding(me_shard_get(me, s));
	}

	/* Clear membership last — after this, no peer will target our slot. */
	struct marufs_me_membership_slot *slot = me_my_membership_get(me);
	WRITE_LE32(slot->status, MARUFS_ME_NONE);
	WRITE_LE64(slot->joined_at, 0);
	MARUFS_CXL_WMB(slot, sizeof(*slot));

	pr_info("me: node %u left ring (order-driven)\n", me->node_id);
}

const struct marufs_me_ops marufs_me_order_ops = {
	.acquire = order_acquire,
	.release = order_release,
	.poll_cycle = order_poll_cycle,
	.join = marufs_me_common_join,
	.leave = order_leave,
};
