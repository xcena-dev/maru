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

/* ── Poll cycle ───────────────────────────────────────────────────── */

static void order_poll_cycle(struct marufs_me_instance *me)
{
	bool ticked_hb = false;

	for (u32 s = 0; s < me->num_shards; s++) {
		struct marufs_me_cb *cb = &me->cbs[s];

		MARUFS_CXL_RMB(cb, sizeof(*cb));
		u32 holder = READ_CXL_LE32(cb->holder);

		me->cached_successor[s] =
			marufs_me_next_active(me, me->node_id);

		if (holder == me->node_id) {
			/* Heartbeat only needs to tick when we're holder —
			 * peers observe the holder's slot to judge liveness.
			 * Once per cycle is enough regardless of shard count.
			 */
			if (!ticked_hb) {
				me_membership_tick_heartbeat(me);
				ticked_hb = true;
			}
			/* Skip self-pass when alone: next_active returns self,
			 * and bumping CB gen against ourselves is a no-op.
			 */
			if (me_shard_passable(me, s) &&
			    me->cached_successor[s] != me->node_id)
				me_pass_token(me, s, me->cached_successor[s]);
		} else if (me->cached_successor[s] == me->node_id) {
			/* Only the successor polls holder heartbeat — O(1)
			 * crash-detection cost per shard across the ring.
			 */
			marufs_me_check_heartbeat(me, s, holder);
		}
	}
}

/* ── Acquire: wait for token arrival ──────────────────────────────── */

static int order_acquire(struct marufs_me_instance *me, u32 shard_id)
{
	/* Intra-node serialization; cross-node via CXL token (cb->holder).
	 * local_waiters lets release() decide keep-token vs. pass-to-ring.
	 */
	atomic_inc(&me->local_waiters[shard_id]);
	mutex_lock(&me->local_locks[shard_id]);
	atomic_dec(&me->local_waiters[shard_id]);

	ME_HOLD(me, shard_id);

	int ret = marufs_me_wait_for_token(me, shard_id);
	if (ret == 0) {
		me_cb_bump_acquire_count(&me->cbs[shard_id]);
		return 0;
	}

	ME_UNHOLD(me, shard_id);
	mutex_unlock(&me->local_locks[shard_id]);
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
	ME_UNHOLD(me, shard_id);

	if (me_shard_passable(me, shard_id)) {
		u32 succ = me->cached_successor[shard_id];

		if (succ == me->node_id) {
			succ = marufs_me_next_active(me, me->node_id);
			me->cached_successor[shard_id] = succ;
		}
		if (succ != me->node_id)
			me_pass_token(me, shard_id, succ);
	}

	mutex_unlock(&me->local_locks[shard_id]);
}

/* ── Leave: token-gated cleanup ───────────────────────────────────── */

/*
 * Diagnostic dump for the acquire-failed leave path.
 * Emitted only when we can't grab our own shard's token within timeout;
 * helps root-cause ring stalls / phantom timeouts.
 */
static void order_leave_dump(struct marufs_me_instance *me, u32 s, int ret,
			     u32 holder)
{
	struct marufs_me_cb *cb = &me->cbs[s];
	u64 cb_gen = READ_CXL_LE64(cb->generation);
	u32 holder_status = 0;

	if (marufs_me_is_valid_node(me, holder)) {
		struct marufs_me_membership_slot *hs =
			&me->membership[holder - 1];

		MARUFS_CXL_RMB(hs, sizeof(*hs));
		holder_status = READ_CXL_LE32(hs->status);
	}

	struct marufs_me_slot *ms = me_my_slot(me, s);

	MARUFS_CXL_RMB(ms, sizeof(*ms));
	pr_debug(
		"me: leave shard %u acquire failed (%d) holder=%u hstatus=%u cb_gen=%llu my_seq=%llu last_seq=%llu last_gen=%llu succ=%u, forcing handoff\n",
		s, ret, holder, holder_status, cb_gen,
		READ_CXL_LE64(ms->token_seq), me->last_token_seq[s],
		me->last_cb_gen[s], me->cached_successor[s]);
}

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

		if (acquired) {
			holder = me->node_id;
		} else {
			struct marufs_me_cb *cb = &me->cbs[s];

			MARUFS_CXL_RMB(cb, sizeof(*cb));
			holder = READ_CXL_LE32(cb->holder);
			order_leave_dump(me, s, ret, holder);
		}

		if (holder == me->node_id)
			me_pass_token(me, s, me_leave_successor(me));

		if (acquired) {
			ME_UNHOLD(me, s);
			mutex_unlock(&me->local_locks[s]);
		}
		ME_RESET_HOLDING(me, s);
	}

	/* Clear membership last — after this, no peer will target our slot. */
	struct marufs_me_membership_slot *slot = &me->membership[me->me_idx];
	WRITE_LE32(slot->status, MARUFS_ME_NONE);
	WRITE_LE64(slot->joined_at, 0);
	MARUFS_CXL_WMB(slot, sizeof(*slot));

	pr_info("me: node %u left ring (order-driven)\n", me->node_id);
}

const struct marufs_me_ops marufs_me_order_ops = {
	.acquire = order_acquire,
	.release = order_release,
	.try_acquire = marufs_me_common_try_acquire,
	.poll_cycle = order_poll_cycle,
	.join = marufs_me_common_join,
	.leave = order_leave,
};
