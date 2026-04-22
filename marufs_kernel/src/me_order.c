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
	for (u32 s = 0; s < me->num_shards; s++) {
		struct marufs_me_cb *cb = &me->cbs[s];
		MARUFS_CXL_RMB(cb, sizeof(*cb));

		u32 holder = READ_CXL_LE32(cb->holder);

		/* Always refresh successor (detect new joiners / leavers) */
		me->cached_successor[s] =
			marufs_me_next_active(me, me->node_id);

		if (holder == me->node_id) {
			me_cb_tick_heartbeat(cb);

			/* Pass token only if idle AND no local waiters.
			 * Prevents token ping-pong when this node has pending
			 * work — locals get served first, then token moves.
			 * smp_mb() in ME_IS_IDLE pairs with acquire's smp_mb().
			 */
			if (me_shard_passable(me, s))
				me_cb_write_holder(cb, me->cached_successor[s]);
		} else {
			marufs_me_check_heartbeat(me, cb, s, holder);
		}
	}
}

/* ── Acquire: wait for token arrival ──────────────────────────────── */

static int order_acquire(struct marufs_me_instance *me, u32 shard_id)
{
	/* Intra-node serialization — only one thread on this node enters
	 * the critical section at a time. Cross-node serialization is via
	 * the CXL token (holder field). Without the local lock, NRHT bucket
	 * CAS and similar primitives would contend heavily on the same
	 * shard between same-node threads.
	 *
	 * local_waiters tracks threads waiting for the mutex on this node,
	 * letting release() decide whether to keep the token (local demand)
	 * or pass it to the ring (no local demand).
	 */
	atomic_inc(&me->local_waiters[shard_id]);
	mutex_lock(&me->local_locks[shard_id]);
	atomic_dec(&me->local_waiters[shard_id]);

	/* Pair with poll_cycle's smp_mb() before reading holding */
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

/* ── Release: signal poll thread to pass token ────────────────────── */
static void order_release(struct marufs_me_instance *me, u32 shard_id)
{
	ME_UNHOLD(me, shard_id);

	/* Direct token pass if idle — don't rely on poll thread.
	 * Without this, rapid acquire/release cycles starve other nodes
	 * because the poll thread can't catch the brief holding==0 window.
	 *
	 * Keep the token if other local threads are waiting — avoids
	 * ping-ponging the token between nodes when only one node has
	 * active work on this shard. Poll thread will pass the token
	 * later if locals quiet down.
	 */
	if (me_shard_passable(me, shard_id)) {
		u32 succ = me->cached_successor[shard_id];

		if (succ == me->node_id) {
			succ = marufs_me_next_active(me, me->node_id);
			me->cached_successor[shard_id] = succ;
		}
		if (succ != me->node_id)
			me_cb_write_holder(&me->cbs[shard_id], succ);
	}

	mutex_unlock(&me->local_locks[shard_id]);
}

const struct marufs_me_ops marufs_me_order_ops = {
	.acquire = order_acquire,
	.release = order_release,
	.try_acquire = marufs_me_common_try_acquire,
	.poll_cycle = order_poll_cycle,
	.join = marufs_me_common_join,
	.leave = marufs_me_common_leave,
};
