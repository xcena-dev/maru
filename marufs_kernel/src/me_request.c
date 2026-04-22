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

/* ── Helpers ──────────────────────────────────────────────────────── */

/*
 * request_slot - locate the request slot for (shard_id, internal idx).
 * idx must be 0..max_nodes-1 (use me->me_idx for self, candidate idx for scan).
 */
static inline struct marufs_me_request_slot *
request_slot(struct marufs_me_instance *me, u32 shard_id, u32 idx)
{
	return &me->slots[(u64)shard_id * me->max_nodes + idx];
}

/*
 * request_scan_and_grant - scan request slots, grant token to first requester.
 * Caller must be the current holder and not actively holding.
 *
 * Scan order: (node_id+1) mod max_nodes, wrapping around, skipping self.
 * A naïve 0..N-1 sweep biases toward low node_ids — under sustained
 * contention, node 0 could starve higher ids. Rotating the start point
 * approximates round-robin fairness similar to the order-driven ring.
 *
 * Returns true if token was granted.
 */
static bool request_scan_and_grant(struct marufs_me_instance *me, u32 shard_id)
{
	struct marufs_me_cb *cb = &me->cbs[shard_id];

	for (u32 i = 1; i < me->max_nodes; i++) {
		u32 idx = (me->me_idx + i) % me->max_nodes;
		struct marufs_me_request_slot *rs =
			request_slot(me, shard_id, idx);
		MARUFS_CXL_RMB(rs, sizeof(*rs));
		if (READ_CXL_LE32(rs->requesting)) {
			me_cb_write_holder(cb, idx + 1); /* external node_id */
			return true;
		}
	}
	return false;
}

static inline void request_clear_own(struct marufs_me_instance *me,
				     u32 shard_id)
{
	struct marufs_me_request_slot *rs =
		request_slot(me, shard_id, me->me_idx);
	WRITE_LE32(rs->requesting, 0);
	MARUFS_CXL_WMB(rs, sizeof(*rs));
}

/* ── Poll cycle ───────────────────────────────────────────────────── */

static void request_poll_cycle(struct marufs_me_instance *me)
{
	for (u32 s = 0; s < me->num_shards; s++) {
		struct marufs_me_cb *cb = &me->cbs[s];
		MARUFS_CXL_RMB(cb, sizeof(*cb));

		u32 holder = READ_CXL_LE32(cb->holder);

		/* Always refresh successor (for crash recovery path) */
		me->cached_successor[s] =
			marufs_me_next_active(me, me->node_id);

		if (holder == me->node_id) {
			me_cb_tick_heartbeat(cb);

			/* If not holding, scan for requests and grant.
			 * smp_mb() in ME_IS_IDLE pairs with acquire's smp_mb().
			 */
			if (me_shard_passable(me, s))
				request_scan_and_grant(me, s);
		} else {
			marufs_me_check_heartbeat(me, cb, s, holder);
		}
	}
}

/* ── Acquire: write request slot, wait for grant ──────────────────── */

static int request_acquire(struct marufs_me_instance *me, u32 shard_id)
{
	/* Intra-node serialization (see order_acquire comment). */
	atomic_inc(&me->local_waiters[shard_id]);
	mutex_lock(&me->local_locks[shard_id]);
	atomic_dec(&me->local_waiters[shard_id]);

	/* Fast path: we already hold the token (e.g. previous release kept it).
	* smp_mb() after holding=1 pairs with poll_cycle's smp_mb() before
	* reading holding — ensures if poll sees holding==0 it already wrote
	* cb->holder, and if we see cb->holder==us then poll won't pass it.
	*/
	ME_HOLD(me, shard_id);

	struct marufs_me_cb *cb = &me->cbs[shard_id];
	MARUFS_CXL_RMB(cb, sizeof(*cb));
	if (READ_CXL_LE32(cb->holder) == me->node_id) {
		me_cb_bump_acquire_count(cb);
		return 0;
	}

	/* Post request and wait for grant */
	struct marufs_me_request_slot *rs =
		request_slot(me, shard_id, me->me_idx);
	WRITE_LE32(rs->sequence, READ_CXL_LE32(rs->sequence) + 1);
	WRITE_LE32(rs->requesting, 1);
	WRITE_LE64(rs->requested_at, ktime_get_ns());
	MARUFS_CXL_WMB(rs, sizeof(*rs));

	int ret = marufs_me_wait_for_token(me, shard_id);
	request_clear_own(me, shard_id);

	if (ret == 0) {
		me_cb_bump_acquire_count(cb);
		return 0;
	}

	ME_UNHOLD(me, shard_id);
	mutex_unlock(&me->local_locks[shard_id]);
	return ret;
}

/* ── Release ──────────────────────────────────────────────────────── */

static void request_release(struct marufs_me_instance *me, u32 shard_id)
{
	ME_UNHOLD(me, shard_id);

	/* Keep token if local threads are waiting — avoids cross-node
	 * ping-pong for single-node-dominant workloads.
	 */
	if (me_shard_passable(me, shard_id))
		request_scan_and_grant(me, shard_id);

	mutex_unlock(&me->local_locks[shard_id]);
}

const struct marufs_me_ops marufs_me_request_ops = {
	.acquire = request_acquire,
	.release = request_release,
	.try_acquire = marufs_me_common_try_acquire,
	.poll_cycle = request_poll_cycle,
	.join = marufs_me_common_join,
	.leave = marufs_me_common_leave,
};
