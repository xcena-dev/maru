/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * me.h - MARUFS Cross-node Mutual Exclusion API
 *
 * Strategy Pattern: common interface with pluggable implementations.
 *   - Order-driven (token ring): token circulates among ACTIVE nodes
 *   - Request-driven: holder scans request slots, grants to requester
 *
 * Two ME domains:
 *   - Global ME (S=1): single token for Index insert + RAT alloc
 *   - NRHT ME (S=N_shard): per-shard token, opt-in membership
 */

#ifndef _MARUFS_ME_H
#define _MARUFS_ME_H

#include <linux/atomic.h>
#include <linux/kthread.h>
#include <linux/list.h>
#include <linux/mutex.h>
#include <linux/timekeeping.h>
#include <linux/types.h>

/* ── Configuration ────────────────────────────────────────────────── */
enum marufs_me_config {
	MARUFS_ME_MAX_NODES = MARUFS_MAX_NODE_ID,
	MARUFS_ME_GLOBAL_SHARDS = 1, /* Global ME: single shard */
	MARUFS_ME_GLOBAL_SHARD_ID = 0, /* Global ME: shard index */
	MARUFS_ME_DEFAULT_POLL_US = 10, /* default poll interval (us) */
	MARUFS_ME_SPIN_COUNT = 100, /* spins before usleep in acquire */
};

/* CXL on-disk layout (structs, magics, address helpers). */
#include "me_layout.h"

/* Typed constants — kept as #define so their width is explicit. */
#define MARUFS_ME_ACQUIRE_TIMEOUT_NS (5ULL * NSEC_PER_SEC)
/* Liveness probe window for second-chance crash check (observer-local).
 * On acquire deadline, sample holder's heartbeat counter, sleep this long
 * on local clock, resample. Unchanged counter after this window ⇒ crash.
 * Avoids cross-node ktime_get_ns() subtraction (local monotonic clocks
 * have per-node boot-time zero points and can't be subtracted meaningfully
 * across nodes sharing CXL memory). 100ms = 10000× default poll interval:
 * even pathological scheduler stalls should still produce a tick within it.
 */
#define MARUFS_ME_LIVENESS_PROBE_NS (100ULL * NSEC_PER_MSEC)

/* ── Forward declarations ─────────────────────────────────────────── */

struct marufs_me_instance;
struct marufs_sb_info;

/* ── Strategy Operations (vtable) ─────────────────────────────────── */

struct marufs_me_ops {
	/*
	 * acquire - block until ME is acquired for shard_id.
	 * Returns 0 on success, -EINTR if interrupted, -ETIMEDOUT on timeout.
	 */
	int (*acquire)(struct marufs_me_instance *me, u32 shard_id);

	/*
	 * release - release ME for shard_id. Must be called by current holder.
	 */
	void (*release)(struct marufs_me_instance *me, u32 shard_id);

	/*
	 * poll_cycle - called periodically by poll thread.
	 * Holder: heartbeat update, token pass if not holding.
	 * Non-holder: heartbeat monitoring, crash detection,
	 *             pre-compute cached_successor.
	 */
	void (*poll_cycle)(struct marufs_me_instance *me);

	/*
	 * join - register this node as ACTIVE in membership slot.
	 */
	int (*join)(struct marufs_me_instance *me);

	/*
	 * leave - deregister this node. Pass held tokens to successor first.
	 */
	void (*leave)(struct marufs_me_instance *me);
};

/* ── Per-shard DRAM state ─────────────────────────────────────────── */

/*
 * struct marufs_me_shard - per-shard local bookkeeping.
 *
 * Laid out as a single array in marufs_me_instance::shards[num_shards],
 * one struct per shard. Fields touched together (hot path: holding,
 * is_holder, last_token_seq) naturally share a cache line.
 *
 * holding / local_waiters : atomic counters for intra-node coordination.
 * local_lock              : intra-node serialization (sleepable).
 * cached_successor        : precomputed next ACTIVE node_id (ring).
 * is_holder               : DRAM "am I the token holder?" flag (replaces
 *                           per-cycle CB hot-polling).
 * poll_last_slot_seq      : poll-thread's shadow of own slot token_seq;
 *                           drives receiver-side is_holder flip on bump.
 * last_token_seq          : wait_for_token phantom filter baseline.
 * last_cb_gen             : cb->generation snapshot for stale-pass fence.
 */
struct marufs_me_shard {
	atomic_t holding;
	atomic_t local_waiters;
	struct mutex local_lock;
	u32 cached_successor;
	bool is_holder;
	u64 poll_last_slot_seq;
	u64 last_token_seq;
	u64 last_cb_gen;

	/* Timestamp captured by acquire() just after mutex_lock; consumed by
	 * release() to record CS hold time. Valid only while the local_lock
	 * is held (single owner). Non-volatile; torn reads are impossible
	 * because only the owner reads/writes it.
	 */
	u64 lock_hold_start_ns;
};

/* Fine-grained per-CPU stats — full layout and helpers in me_stats.h.
 * Forward-declared here so marufs_me_instance can hold the pointer
 * without pulling in the stats machinery.
 */
struct marufs_me_stats_pcpu;

/* ── ME Instance (per-mount, DRAM) ────────────────────────────────── */

struct marufs_me_instance {
	/* CXL pointers */
	struct marufs_me_header *header;
	struct marufs_me_cb *cbs;
	struct marufs_me_membership_slot *membership;
	struct marufs_me_slot
		*slots; /* per-(shard, node) slots, always allocated */

	/* Configuration */
	u32 num_shards; /* Global ME: 1, NRHT ME: N_shard */
	u32 max_nodes;
	u32 node_id; /* external 1..MARUFS_MAX_NODE_ID (0 = ORPHAN reserved) */
	u32 me_idx; /* internal 0..max_nodes-1 = node_id - 1 */
	u32 poll_interval_us;
	enum marufs_me_strategy strategy;
	const struct marufs_me_ops *ops;

	/* Per-shard DRAM state — one struct per shard, allocated as a single
	 * contiguous array (avoids 8 separate kcalloc/kfree cycles).
	 */
	struct marufs_me_shard *shards;

	/* Poll-path cost counters exposed via /sys/fs/marufs/me_poll_stats.
	 * Incremented only from poll_cycle; app-thread traffic excluded.
	 */
	atomic64_t poll_cycles;
	atomic64_t poll_ns_total;
	atomic64_t poll_rmb_cb;
	atomic64_t poll_rmb_slot;
	atomic64_t poll_rmb_membership;

	/* Fine-grained per-CPU stats (latency histos, hit buckets, per-shard).
	 * Allocated in marufs_me_create, freed in marufs_me_destroy. See
	 * struct marufs_me_stats_pcpu for field layout.
	 */
	struct marufs_me_stats_pcpu __percpu *stats;

	/* Test-only fault injection. When non-zero, the poll thread treats
	 * this ME as dead: no heartbeat tick, no grant scan, no doorbell
	 * handling. Peers' acquire deadline path then exercises the
	 * counter-based liveness probe and self-takeover end-to-end.
	 * Toggled via /sys/fs/marufs/me_freeze_heartbeat (node_id-scoped).
	 * Overhead: one atomic_read per poll cycle — negligible.
	 */
	atomic_t debug_freeze_poll;

	/* Active flag — cleared on destroy, checked by registry poll thread */
	atomic_t active; /* 0 = shutdown, 1 = serving polls */

	/* Cached CXL format_generation at create time. Compared by
	 * marufs_nrht_me_get() fast-path to detect a re-format of the
	 * underlying CXL area (RAT slot reused → new nrht_init wiped
	 * ME state) and discard this stale instance.
	 */
	u64 cached_generation;

	/* Registry link (sbi->me_list) */
	struct list_head list_node;
};

/* Inline helpers — depend on the DRAM types defined above. */
#include "me_inline.h"

/* ── Instance lifecycle ─────────────────────────────────────────────── */

struct marufs_me_instance *marufs_me_create(void *me_area_base, u32 num_shards,
					    u32 max_nodes, u32 node_id,
					    u32 poll_interval_us,
					    enum marufs_me_strategy strategy);
void marufs_me_destroy(struct marufs_me_instance *me);
int marufs_me_format(void *me_area_base, u32 num_shards, u32 max_nodes,
		     u32 poll_interval_us, enum marufs_me_strategy strategy);

/* ── Registry (sbi-managed poll thread) ─────────────────────────────── */

/*
 * Registry API — unified poll thread per sbi.
 *
 * Lifecycle:
 *   mount:  marufs_me_registry_init(sbi)
 *           → marufs_me_registry_start(sbi)  // starts the poll thread
 *           → [for each ME] marufs_me_register(sbi, me)
 *   unmount: [for each ME] marufs_me_unregister(sbi, me)
 *           → marufs_me_registry_stop(sbi)
 */
void marufs_me_registry_init(struct marufs_sb_info *sbi);
int marufs_me_registry_start(struct marufs_sb_info *sbi);
void marufs_me_registry_stop(struct marufs_sb_info *sbi);

void marufs_me_register(struct marufs_sb_info *sbi,
			struct marufs_me_instance *me);
void marufs_me_unregister(struct marufs_sb_info *sbi,
			  struct marufs_me_instance *me);

/*
 * marufs_me_teardown - graceful teardown on unmount.
 * unregister + ops->leave (pass held tokens, clear membership slot) + destroy.
 * No-op if @me is NULL.
 */
void marufs_me_teardown(struct marufs_sb_info *sbi,
			struct marufs_me_instance *me);

/*
 * marufs_me_invalidate - drop a stale instance without leaving the ring.
 * unregister + destroy only. Use when the CXL state has already been
 * reformatted by a peer (membership slots/holder are no longer ours to
 * release), so calling ops->leave would touch foreign state.
 * No-op if @me is NULL.
 */
void marufs_me_invalidate(struct marufs_sb_info *sbi,
			  struct marufs_me_instance *me);

/* ── Acquire / release primitives (me.c) ────────────────────────────── */

void marufs_me_hold(struct marufs_me_instance *me, u32 shard_id);
void marufs_me_unhold(struct marufs_me_instance *me, u32 shard_id);
void marufs_me_pass_token(struct marufs_me_instance *me, u32 shard_id,
			  u32 new_holder);
void marufs_me_membership_set_pending(struct marufs_me_instance *me,
				      u32 shard_id);
void marufs_me_membership_clear_pending(struct marufs_me_instance *me,
					u32 shard_id);

/*
 * marufs_me_wait_for_token - spin-then-sleep until cb->holder == self or
 * deadline expires. Caller must hold local_locks[shard_id] and have
 * incremented `holding`. Returns 0 on success, -ETIMEDOUT on deadline.
 */
int marufs_me_wait_for_token(struct marufs_me_instance *me, u32 shard_id);

/* ── Shared strategy primitives ─────────────────────────────────────── */

/*
 * marufs_me_common_* - strategy-independent ops implementations used by
 * both order-driven and request-driven vtables.
 */
int marufs_me_common_join(struct marufs_me_instance *me);
void marufs_me_common_leave(struct marufs_me_instance *me);

/* Topology / membership — definitions in me.c. */
u32 marufs_me_next_active(struct marufs_me_instance *me, u32 from);
u32 me_leave_successor(struct marufs_me_instance *me);
void me_membership_tick_heartbeat(struct marufs_me_instance *me);

/* ── Strategy implementations ───────────────────────────────────────── */

extern const struct marufs_me_ops marufs_me_order_ops;
extern const struct marufs_me_ops marufs_me_request_ops;

static inline const struct marufs_me_ops *
marufs_me_get_ops(enum marufs_me_strategy strategy)
{
	if (strategy == MARUFS_ME_REQUEST)
		return &marufs_me_request_ops;
	return &marufs_me_order_ops;
}

#endif /* _MARUFS_ME_H */
