// SPDX-License-Identifier: GPL-2.0-only
/*
 * me.c - MARUFS Cross-node Mutual Exclusion common infrastructure
 *
 * Provides: instance lifecycle, poll thread, membership scan helper.
 * Strategy-specific logic lives in me_order.c / me_request.c.
 */

#include <linux/delay.h>
#include <linux/kthread.h>
#include <linux/slab.h>
#include <linux/timekeeping.h>

#include "marufs.h"
#include "me.h"

/* ── next_active: scan membership slots for next ACTIVE node ──────── */

/*
 * marufs_me_next_active - find next ACTIVE node after @current.
 * Scans slot[current+1..] wrapping around. O(N), N <= max_nodes.
 * Returns @current if no other active node found.
 */
u32 marufs_me_next_active(struct marufs_me_instance *me, u32 from)
{
	/* @from is an external node_id (1..max_nodes) or HOLDER_NONE.
	 * Internal array indexing is 0-based; convert, scan, convert back.
	 * HOLDER_NONE: treat as starting before slot 0 so the loop scans
	 * every slot.
	 */
	u32 from_idx = (from == MARUFS_ME_HOLDER_NONE) ? me->max_nodes - 1 :
							 from - 1;

	for (u32 i = 1; i < me->max_nodes; i++) {
		u32 idx = (from_idx + i) % me->max_nodes;
		struct marufs_me_membership_slot *slot = &me->membership[idx];
		MARUFS_CXL_RMB(slot, sizeof(*slot));
		atomic64_inc(&me->poll_rmb_membership);
		if (READ_CXL_LE32(slot->status) == MARUFS_ME_ACTIVE)
			return idx + 1; /* external node_id */
	}
	return from;
}

/* ── Shared strategy primitives ───────────────────────────────────── */

/*
 * marufs_me_check_heartbeat - non-holder crash detection.
 *
 * Reads the holder's membership slot heartbeat (per-node, distributed)
 * rather than a shared CB heartbeat — this keeps the CB CL cold.
 *
 * First observation seeds the baseline without triggering timeout —
 * otherwise last_heartbeat_time=0 (kcalloc) makes elapsed == uptime and
 * triggers false crash detection / split-brain takeover.
 */
void marufs_me_check_heartbeat(struct marufs_me_instance *me, u32 shard_id,
			       u32 holder)
{
	if (!marufs_me_is_valid_node(me, holder))
		return;

	struct marufs_me_membership_slot *hslot = &me->membership[holder - 1];
	MARUFS_CXL_RMB(hslot, sizeof(*hslot));
	atomic64_inc(&me->poll_rmb_membership);
	u64 hb = READ_CXL_LE64(hslot->heartbeat);

	if (me->last_heartbeat_time[shard_id] == 0) {
		me->last_heartbeat[shard_id] = hb;
		me->last_heartbeat_time[shard_id] = ktime_get_ns();
		return;
	}

	if (hb == me->last_heartbeat[shard_id]) {
		u64 elapsed =
			ktime_get_ns() - me->last_heartbeat_time[shard_id];
		if (elapsed > MARUFS_ME_TIMEOUT_NS) {
			u32 succ = marufs_me_next_active(me, holder);
			if (succ == me->node_id) {
				pr_warn("me: crash detected (holder=%u shard=%u), taking over\n",
					holder, shard_id);
				me_pass_token(me, shard_id, me->node_id);
			}
		}
	} else {
		me->last_heartbeat[shard_id] = hb;
		me->last_heartbeat_time[shard_id] = ktime_get_ns();
	}
}

/*
 * marufs_me_wait_for_token - poll own doorbell slot until token arrives.
 *
 * Reader order (acquire-side, mirrors me_pass_token's writer order):
 *   1. Read my slot->token_seq. If unchanged → continue polling.
 *   2. RMB. Read cb->holder + cb->generation.
 *   3. Double-check: holder == me && generation > last_seen → success.
 *      Otherwise phantom/stale wakeup — reset snapshot and keep polling.
 */
int marufs_me_wait_for_token(struct marufs_me_instance *me, u32 shard_id)
{
	/* Fast path: already holder (prior CS kept token, or poll thread
	 * hasn't passed yet). Sync baselines and return.
	 */
	struct marufs_me_slot *my_slot = me_my_slot(me, shard_id);
	struct marufs_me_cb *cb = &me->cbs[shard_id];
	MARUFS_CXL_RMB(cb, sizeof(*cb));

	if (READ_CXL_LE32(cb->holder) == me->node_id) {
		me->last_cb_gen[shard_id] = READ_CXL_LE64(cb->generation);
		MARUFS_CXL_RMB(&my_slot->token_seq, sizeof(my_slot->token_seq));
		me->last_token_seq[shard_id] =
			READ_CXL_LE64(my_slot->token_seq);
		return 0;
	}

	u64 deadline = ktime_get_ns() + MARUFS_ME_ACQUIRE_TIMEOUT_NS;
	u32 spins = 0;
	while (ktime_get_ns() < deadline) {
		MARUFS_CXL_RMB(&my_slot->token_seq, sizeof(my_slot->token_seq));
		u64 cur_seq = READ_CXL_LE64(my_slot->token_seq);

		if (cur_seq != me->last_token_seq[shard_id]) {
			/* Acquire ordering: subsequent CB read must not be
			 * reordered before the slot read above.
			 */
			MARUFS_CXL_RMB(cb, sizeof(*cb));
			u32 holder = READ_CXL_LE32(cb->holder);
			u64 cb_gen = READ_CXL_LE64(cb->generation);

			if (holder == me->node_id &&
			    cb_gen > me->last_cb_gen[shard_id]) {
				me->last_token_seq[shard_id] = cur_seq;
				me->last_cb_gen[shard_id] = cb_gen;
				return 0;
			}
			/* Phantom: seq advanced but CB not for us. Advance
			 * last_token_seq only; keeping last_cb_gen lets the
			 * gen-monotonicity filter still reject stale passes
			 * from prior generations.
			 */
			me->last_token_seq[shard_id] = cur_seq;
		}

		if (++spins < MARUFS_ME_SPIN_COUNT)
			cpu_relax();
		else
			usleep_range(me->poll_interval_us / 2,
				     me->poll_interval_us);
	}
	return -ETIMEDOUT;
}

int marufs_me_common_try_acquire(struct marufs_me_instance *me, u32 shard_id)
{
	if (!mutex_trylock(&me->local_locks[shard_id]))
		return -EBUSY;

	ME_HOLD(me, shard_id);

	struct marufs_me_cb *cb = &me->cbs[shard_id];
	MARUFS_CXL_RMB(cb, sizeof(*cb));

	if (READ_CXL_LE32(cb->holder) == me->node_id) {
		me_cb_bump_acquire_count(cb);
		return 0;
	}
	ME_UNHOLD(me, shard_id);
	mutex_unlock(&me->local_locks[shard_id]);
	return -EBUSY;
}

/*
 * marufs_me_common_join - claim any HOLDER_NONE shards on entry.
 *
 * The NONE-claim covers both cases uniformly:
 *   - First node: format() set every CB to HOLDER_NONE → we take all.
 *   - Later node: the prior holder left without a successor (wrote NONE)
 *     → we resurrect that shard.
 * If a crashed holder left its id stale in the CB, the heartbeat-timeout
 * takeover path (not join) handles that.
 */
int marufs_me_common_join(struct marufs_me_instance *me)
{
	struct marufs_me_membership_slot *slot = &me->membership[me->me_idx];
	WRITE_LE32(slot->status, MARUFS_ME_ACTIVE);
	WRITE_LE32(slot->node_id, me->node_id);
	WRITE_LE64(slot->joined_at, ktime_get_ns());
	WRITE_LE64(slot->heartbeat, 0);
	WRITE_LE64(slot->heartbeat_ts, ktime_get_ns());
	WRITE_LE64(slot->pending_shards_mask, 0);
	MARUFS_CXL_WMB(slot, sizeof(*slot));

	/* Seed per-shard baselines BEFORE claiming, so our own NONE-claim
	 * below isn't mistaken for a phantom wakeup.
	 */
	for (u32 s = 0; s < me->num_shards; s++) {
		struct marufs_me_slot *my_slot = me_my_slot(me, s);

		MARUFS_CXL_RMB(my_slot, sizeof(*my_slot));
		me->last_token_seq[s] = READ_CXL_LE64(my_slot->token_seq);

		struct marufs_me_cb *cb = &me->cbs[s];

		MARUFS_CXL_RMB(cb, sizeof(*cb));
		me->last_cb_gen[s] = READ_CXL_LE64(cb->generation);
	}

	u32 claimed = 0;
	for (u32 s = 0; s < me->num_shards; s++) {
		struct marufs_me_cb *cb = &me->cbs[s];

		MARUFS_CXL_RMB(cb, sizeof(*cb));
		if (READ_CXL_LE32(cb->holder) == MARUFS_ME_HOLDER_NONE) {
			me_pass_token(me, s, me->node_id);
			claimed++;
		}
	}

	for (u32 s = 0; s < me->num_shards; s++)
		me->cached_successor[s] =
			marufs_me_next_active(me, me->node_id);

	if (claimed == me->num_shards)
		pr_info("me: node %u joined as first node (token holder)\n",
			me->node_id);
	else if (claimed)
		pr_info("me: node %u joined ring, claimed %u vacant shard(s)\n",
			me->node_id, claimed);
	else
		pr_info("me: node %u joined ring\n", me->node_id);

	return 0;
}

/*
 * marufs_me_common_leave - hand off held shards, clear membership.
 * NONE fallback when alone lets a later joiner NONE-claim in common_join.
 */
void marufs_me_common_leave(struct marufs_me_instance *me)
{
	for (u32 s = 0; s < me->num_shards; s++) {
		struct marufs_me_cb *cb = &me->cbs[s];

		MARUFS_CXL_RMB(cb, sizeof(*cb));
		if (READ_CXL_LE32(cb->holder) == me->node_id)
			me_pass_token(me, s, me_leave_successor(me));
		ME_RESET_HOLDING(me, s);
	}

	struct marufs_me_membership_slot *slot = &me->membership[me->me_idx];
	WRITE_LE32(slot->status, MARUFS_ME_NONE);
	WRITE_LE64(slot->joined_at, 0);
	WRITE_LE64(slot->pending_shards_mask, 0);
	MARUFS_CXL_WMB(slot, sizeof(*slot));

	pr_info("me: node %u left ring\n", me->node_id);
}

/* ── Poll thread ──────────────────────────────────────────────────── */

/*
 * Unified poll thread: iterates all registered ME instances and
 * calls poll_cycle() on each. Used for Global ME + all NRHT MEs.
 */
static int marufs_me_registry_poll_fn(void *data)
{
	struct marufs_sb_info *sbi = data;
	u32 poll_us = MARUFS_ME_DEFAULT_POLL_US;

	pr_info("me: registry poll thread started (node=%u)\n", sbi->node_id);

	while (!kthread_should_stop()) {
		struct marufs_me_instance *me;

		usleep_range(poll_us, poll_us + poll_us / 4);

		mutex_lock(&sbi->me_list_lock);
		list_for_each_entry(me, &sbi->me_list, list_node) {
			if (!atomic_read(&me->active))
				continue;

			u64 t0 = ktime_get_ns();
			me->ops->poll_cycle(me);
			atomic64_add(ktime_get_ns() - t0, &me->poll_ns_total);
			atomic64_inc(&me->poll_cycles);
		}
		mutex_unlock(&sbi->me_list_lock);
	}

	pr_info("me: registry poll thread exiting (node=%u)\n", sbi->node_id);
	return 0;
}

void marufs_me_registry_init(struct marufs_sb_info *sbi)
{
	INIT_LIST_HEAD(&sbi->me_list);
	mutex_init(&sbi->me_list_lock);
	mutex_init(&sbi->nrht_me_lock);
	sbi->me_poll_thread = NULL;
}

int marufs_me_registry_start(struct marufs_sb_info *sbi)
{
	if (sbi->me_poll_thread)
		return -EEXIST;

	sbi->me_poll_thread =
		kthread_run(marufs_me_registry_poll_fn, sbi, "marufs_me_poll");
	if (IS_ERR(sbi->me_poll_thread)) {
		int ret = PTR_ERR(sbi->me_poll_thread);

		sbi->me_poll_thread = NULL;
		return ret;
	}
	return 0;
}

void marufs_me_registry_stop(struct marufs_sb_info *sbi)
{
	if (!sbi->me_poll_thread)
		return;

	kthread_stop(sbi->me_poll_thread);
	sbi->me_poll_thread = NULL;
}

void marufs_me_register(struct marufs_sb_info *sbi,
			struct marufs_me_instance *me)
{
	mutex_lock(&sbi->me_list_lock);
	list_add_tail(&me->list_node, &sbi->me_list);
	atomic_set(&me->active, 1);
	mutex_unlock(&sbi->me_list_lock);
}

void marufs_me_unregister(struct marufs_sb_info *sbi,
			  struct marufs_me_instance *me)
{
	mutex_lock(&sbi->me_list_lock);
	atomic_set(&me->active, 0);
	list_del_init(&me->list_node);
	mutex_unlock(&sbi->me_list_lock);
}

void marufs_me_teardown(struct marufs_sb_info *sbi,
			struct marufs_me_instance *me)
{
	if (!me)
		return;

	/* Reformat detection: underlying CXL area was wiped by a fresh
	 * nrht_init while this instance was cached. Skip leave — CBs /
	 * membership now belong to a foreign ring.
	 */
	struct marufs_me_header *h = me->header;
	MARUFS_CXL_RMB(h, sizeof(*h));
	if (READ_CXL_LE64(h->format_generation) != me->cached_generation) {
		pr_info("me: teardown skip leave (format_gen mismatch)\n");
		marufs_me_invalidate(sbi, me);
		return;
	}

	/* leave() BEFORE unregister: poll thread must keep ticking heartbeat
	 * and passing tokens during handoff. leave() clears membership last.
	 */
	me->ops->leave(me);
	marufs_me_unregister(sbi, me);
	marufs_me_destroy(me);
}

void marufs_me_invalidate(struct marufs_sb_info *sbi,
			  struct marufs_me_instance *me)
{
	if (!me)
		return;
	marufs_me_unregister(sbi, me);
	marufs_me_destroy(me);
}

/* ── Instance lifecycle ───────────────────────────────────────────── */

/*
 * marufs_me_create - allocate and initialize ME instance.
 * @me_area_base: CXL virtual address of ME area start (header location)
 * @num_shards: number of CB entries (Global ME: 1, NRHT ME: N_shard)
 * @max_nodes: max node count
 * @node_id: this node's ID
 * @poll_interval_us: poll thread interval
 * @strategy: order-driven or request-driven
 *
 * Parses CXL header to locate CB array, membership slots, request slots.
 * Allocates DRAM arrays for holding/heartbeat tracking.
 */
struct marufs_me_instance *marufs_me_create(void *me_area_base, u32 num_shards,
					    u32 max_nodes, u32 node_id,
					    u32 poll_interval_us,
					    enum marufs_me_strategy strategy)
{
	struct marufs_me_instance *me = kzalloc(sizeof(*me), GFP_KERNEL);
	if (!me)
		return ERR_PTR(-ENOMEM);

	/* CXL pointers from header offsets */
	struct marufs_me_header *hdr = me_area_base;
	me->header = hdr;
	MARUFS_CXL_RMB(hdr, sizeof(*hdr));
	me->cached_generation = READ_CXL_LE64(hdr->format_generation);
	me->cbs = marufs_me_cb_at(me_area_base,
				  READ_CXL_LE64(hdr->cb_array_offset), 0);
	me->membership = marufs_me_membership_at(
		me_area_base, READ_CXL_LE64(hdr->membership_offset), 0);

	/* Slot region is now always allocated (per-(shard, node) doorbell +
	 * request hand-raise). Fall back to NULL only for legacy headers
	 * with request_offset == 0, in which case strategy-specific code
	 * must tolerate it (should not happen after format).
	 */
	if (READ_CXL_LE64(hdr->request_offset) != 0)
		me->slots = marufs_me_slot_at(
			me_area_base, READ_CXL_LE64(hdr->request_offset),
			max_nodes, 0, 0);
	else
		me->slots = NULL;

	/* Configuration */
	me->num_shards = num_shards;
	me->max_nodes = max_nodes;
	me->node_id = node_id;
	me->me_idx = node_id - 1; /* external 1..N → internal idx 0..N-1 */
	me->poll_interval_us = poll_interval_us;
	me->strategy = strategy;
	me->ops = marufs_me_get_ops(strategy);

	/* DRAM arrays */
	me->holding = kcalloc(num_shards, sizeof(atomic_t), GFP_KERNEL);
	me->local_waiters = kcalloc(num_shards, sizeof(atomic_t), GFP_KERNEL);
	me->local_locks = kcalloc(num_shards, sizeof(struct mutex), GFP_KERNEL);
	me->cached_successor = kcalloc(num_shards, sizeof(u32), GFP_KERNEL);
	me->last_heartbeat = kcalloc(num_shards, sizeof(u64), GFP_KERNEL);
	me->last_heartbeat_time = kcalloc(num_shards, sizeof(u64), GFP_KERNEL);
	me->last_token_seq = kcalloc(num_shards, sizeof(u64), GFP_KERNEL);
	me->last_cb_gen = kcalloc(num_shards, sizeof(u64), GFP_KERNEL);

	if (!me->holding || !me->local_waiters || !me->local_locks ||
	    !me->cached_successor || !me->last_heartbeat ||
	    !me->last_heartbeat_time || !me->last_token_seq ||
	    !me->last_cb_gen) {
		marufs_me_destroy(me);
		return ERR_PTR(-ENOMEM);
	}

	for (u32 s = 0; s < num_shards; s++)
		mutex_init(&me->local_locks[s]);

	atomic_set(&me->active, 0);
	INIT_LIST_HEAD(&me->list_node);

	return me;
}

void marufs_me_destroy(struct marufs_me_instance *me)
{
	if (!me)
		return;

	/* Caller must have unregistered first; this is a safety net. */
	WARN_ON_ONCE(!list_empty(&me->list_node));

	kfree(me->holding);
	kfree(me->local_waiters);
	kfree(me->local_locks);
	kfree(me->cached_successor);
	kfree(me->last_heartbeat);
	kfree(me->last_heartbeat_time);
	kfree(me->last_token_seq);
	kfree(me->last_cb_gen);
	kfree(me);
}

/* ── Format: initialize ME area in CXL memory ────────────────────── */

/*
 * marufs_me_format - write initial ME structures to CXL memory.
 *
 * Layout from me_area_base:
 *   [Header 64B] [CB array S×64B] [Membership N×64B] [Request S×N×64B]
 */
int marufs_me_format(void *me_area_base, u32 num_shards, u32 max_nodes,
		     u32 poll_interval_us, enum marufs_me_strategy strategy)
{
	/* Compute offsets — slot region is now allocated for both strategies */
	u64 cb_off = sizeof(struct marufs_me_header);
	u64 mem_off = cb_off + (u64)num_shards * sizeof(struct marufs_me_cb);
	u64 slot_off =
		mem_off +
		(u64)max_nodes * sizeof(struct marufs_me_membership_slot);
	u64 total = slot_off +
		    (u64)num_shards * max_nodes * sizeof(struct marufs_me_slot);

	/* Write header */
	struct marufs_me_header *hdr = me_area_base;
	WRITE_LE32(hdr->magic, MARUFS_ME_MAGIC);
	WRITE_LE32(hdr->version, MARUFS_ME_VERSION);
	WRITE_LE32(hdr->strategy, strategy);
	WRITE_LE32(hdr->num_shards, num_shards);
	WRITE_LE32(hdr->max_nodes, max_nodes);
	WRITE_LE32(hdr->poll_interval_us, poll_interval_us);
	WRITE_LE64(hdr->cb_array_offset, cb_off);
	WRITE_LE64(hdr->membership_offset, mem_off);
	WRITE_LE64(hdr->request_offset, slot_off);
	WRITE_LE64(hdr->total_size, total);
	/* Bump generation so any prior cached ME instance on another sbi
	 * detects the reformat and drops its stale state on next me_get.
	 */
	WRITE_LE64(hdr->format_generation, ktime_get_real_ns());
	MARUFS_CXL_WMB(hdr, sizeof(*hdr));

	/* Initialize CBs — holder=NONE (no valid node), state=FREE */
	for (u32 s = 0; s < num_shards; s++) {
		struct marufs_me_cb *cb =
			marufs_me_cb_at(me_area_base, cb_off, s);

		memset(cb, 0, sizeof(*cb));
		WRITE_LE32(cb->magic, MARUFS_ME_CB_MAGIC);
		WRITE_LE32(cb->holder, MARUFS_ME_HOLDER_NONE);
		MARUFS_CXL_WMB(cb, sizeof(*cb));
	}

	/* Initialize membership slots.
	 * slot[i] is reserved for external node_id (i + 1).
	 */
	for (u32 n = 0; n < max_nodes; n++) {
		struct marufs_me_membership_slot *slot =
			marufs_me_membership_at(me_area_base, mem_off, n);

		WRITE_LE32(slot->magic, MARUFS_ME_MS_MAGIC);
		WRITE_LE32(slot->status, MARUFS_ME_NONE);
		WRITE_LE32(slot->node_id, n + 1);
		WRITE_LE64(slot->joined_at, 0);
		WRITE_LE64(slot->heartbeat, 0);
		WRITE_LE64(slot->heartbeat_ts, 0);
		WRITE_LE64(slot->pending_shards_mask, 0);
		memset(slot->reserved, 0, sizeof(slot->reserved));
		MARUFS_CXL_WMB(slot, sizeof(*slot));
	}

	/* Initialize per-(shard, node) slots — tag each with magic; batch
	 * the WMB since format is a cold path (one big flush > N*S small).
	 */
	for (u32 s = 0; s < num_shards; s++) {
		for (u32 n = 0; n < max_nodes; n++) {
			struct marufs_me_slot *sl = marufs_me_slot_at(
				me_area_base, slot_off, max_nodes, s, n);
			memset(sl, 0, sizeof(*sl));
			WRITE_LE32(sl->magic, MARUFS_ME_SLOT_MAGIC);
		}
	}

	struct marufs_me_slot *slots_base =
		marufs_me_slot_at(me_area_base, slot_off, max_nodes, 0, 0);
	u64 slots_bytes =
		(u64)num_shards * max_nodes * sizeof(struct marufs_me_slot);
	MARUFS_CXL_WMB(slots_base, slots_bytes);

	pr_info("me: formatted area (%s, shards=%u, nodes=%u, size=%llu)\n",
		strategy == MARUFS_ME_ORDER ? "order" : "request", num_shards,
		max_nodes, total);
	return 0;
}
