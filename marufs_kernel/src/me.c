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
	/* Fast path: token already ours — no CB RMB needed. Cross-node CB
	* can flip only via heartbeat-timeout takeover, blocked while we
	* keep ticking heartbeat in poll_cycle.
	*/
	struct marufs_me_shard *sh = &me->shards[shard_id];
	if (ME_IS_HOLDER(sh))
		return 0;

	struct marufs_me_slot *my_slot = me_my_slot(me, shard_id);
	struct marufs_me_cb *cb = &me->cbs[shard_id];
	u64 deadline = ktime_get_ns() + MARUFS_ME_ACQUIRE_TIMEOUT_NS;
	u32 spins = 0;
	while (ktime_get_ns() < deadline) {
		MARUFS_CXL_RMB(&my_slot->token_seq, sizeof(my_slot->token_seq));
		u64 cur_seq = READ_CXL_LE64(my_slot->token_seq);

		if (cur_seq != sh->last_token_seq) {
			/* Acquire ordering: subsequent CB read must not be
			 * reordered before the slot read above.
			 */
			u64 cb_gen;
			u32 holder = me_cb_snapshot(cb, &cb_gen);

			if (holder == me->node_id && cb_gen > sh->last_cb_gen) {
				sh->last_token_seq = cur_seq;
				sh->last_cb_gen = cb_gen;
				ME_BECOME_HOLDER(sh);
				return 0;
			}
			/* Phantom: seq advanced but CB not for us. Advance
			 * last_token_seq only; keeping last_cb_gen lets the
			 * gen-monotonicity filter still reject stale passes
			 * from prior generations.
			 */
			sh->last_token_seq = cur_seq;
		}

		if (++spins < MARUFS_ME_SPIN_COUNT)
			cpu_relax();
		else
			usleep_range(me->poll_interval_us / 2,
				     me->poll_interval_us);
	}

	/* Deadline expired — sole crash-recovery trigger. Take over iff
	 * holder's heartbeat has stalled past the timeout.
	 */
	u32 holder = me_cb_snapshot(cb, NULL);
	if (!marufs_me_is_valid_node(me, holder) || holder == me->node_id)
		return -ETIMEDOUT;

	struct marufs_me_membership_slot *hs = &me->membership[holder - 1];
	MARUFS_CXL_RMB(hs, sizeof(*hs));
	u64 hb_ts = READ_CXL_LE64(hs->heartbeat_ts);
	u64 elapsed = ktime_get_ns() - hb_ts;

	if (elapsed < MARUFS_ME_TIMEOUT_NS)
		return -ETIMEDOUT;

	pr_warn("me: acquire timeout on shard %u, holder=%u heartbeat stalled %llums — taking over\n",
		shard_id, holder, elapsed / 1000000);
	me_pass_token(me, shard_id, me->node_id);
	return 0;
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
		u64 seq = READ_CXL_LE64(my_slot->token_seq);

		struct marufs_me_shard *sh = &me->shards[s];
		sh->last_token_seq = seq;
		sh->poll_last_slot_seq = seq;
		(void)me_cb_snapshot(&me->cbs[s], &sh->last_cb_gen);
	}

	u32 claimed = 0;
	for (u32 s = 0; s < me->num_shards; s++) {
		u32 cur = me_cb_snapshot(&me->cbs[s], NULL);

		if (cur == MARUFS_ME_HOLDER_NONE) {
			me_pass_token(me, s, me->node_id);
			claimed++;
		} else {
			/* Seed holder cache so subsequent poll_cycles can
			 * skip the CB RMB for this shard when we are neither
			 * holder nor successor. Baselines were already seeded
			 * in the prior loop, so publishing via the macro is safe.
			 */
			struct marufs_me_shard *sh = &me->shards[s];

			if (cur == me->node_id)
				ME_BECOME_HOLDER(sh);
			else
				ME_LOSE_HOLDER(sh);
		}
	}

	u32 succ = marufs_me_next_active(me, me->node_id);
	for (u32 s = 0; s < me->num_shards; s++)
		me->shards[s].cached_successor = succ;

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
		if (me_cb_snapshot(&me->cbs[s], NULL) == me->node_id)
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

	/* Single per-shard DRAM allocation — kcalloc zeroes all fields, so
	 * atomic counters, is_holder, sequence shadows start at 0 / false.
	 */
	me->shards = kcalloc(num_shards, sizeof(*me->shards), GFP_KERNEL);
	if (!me->shards) {
		marufs_me_destroy(me);
		return ERR_PTR(-ENOMEM);
	}
	for (u32 s = 0; s < num_shards; s++)
		mutex_init(&me->shards[s].local_lock);

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

	kfree(me->shards);
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
