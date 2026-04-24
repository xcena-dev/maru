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

/* Typed constants — kept as #define so their width is explicit. */
#define MARUFS_ME_HOLDER_NONE ((u32)0xFFFFFFFF) /* no holder sentinel */
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

/* ── CXL Shared Memory Structures ─────────────────────────────────── */
/* ME membership slot status */
enum marufs_me_membership_status {
	MARUFS_ME_NONE = 0,
	MARUFS_ME_ACTIVE = 1,
};

/*
 * ME Area Header (64B, 1 cacheline)
 *
 * Describes ME area layout. Stored once at the start of ME area.
 */
struct marufs_me_header {
	__le32 magic; /*  0: MARUFS_ME_MAGIC */
	__le32 version; /*  4: format version */
	__le32 strategy; /*  8: MARUFS_ME_ORDER / MARUFS_ME_REQUEST */
	__le32 num_shards; /* 12: CB count (Global ME: 1, NRHT ME: N_shard) */
	__le32 max_nodes; /* 16: max nodes supported */
	__le32 poll_interval_us; /* 20: polling interval in microseconds */
	__le64 cb_array_offset; /* 24: CB array offset from ME area start */
	__le64 membership_offset; /* 32: membership slot array offset */
	__le64 request_offset; /* 40: request slot array (0 if order-driven) */
	__le64 total_size; /* 48: total ME area size in bytes */
	__le64 format_generation; /* 56: bumped on every marufs_me_format() —
				   * used to invalidate per-sbi cached ME
				   * instances when the underlying CXL area
				   * has been reformatted (e.g. nrht_init
				   * called again on a reused RAT slot). */
} __attribute__((packed));

/*
 * ME Control Block (64B, 1 cacheline)
 *
 * Per-shard mutual exclusion state. Read-mostly; written only on holder
 * transition. Hot path polling happens on per-node slot doorbell, not here.
 */
/* Per-type magics — prefix each CXL-resident ME struct so writers can
 * verify they are actually addressing the intended record type. Guards
 * against stale instance writes after a peer reformat changed the ME
 * area layout (cached offsets now fall inside a different struct).
 */
enum marufs_me_struct_magic {
	MARUFS_ME_CB_MAGIC = 0x4352454D, /* "MERC" */
	MARUFS_ME_MS_MAGIC = 0x534D454D, /* "MEMS" */
	MARUFS_ME_SLOT_MAGIC = 0x4C534D45, /* "EMSL" */
};

struct marufs_me_cb {
	__le32 magic; /*  0: MARUFS_ME_CB_MAGIC */
	__le32 holder; /*  4: current holder node_id (MARUFS_ME_HOLDER_NONE if vacant) */
	__le32 state; /*  8: ME_FREE(0) / ME_HELD(1) / ME_RELEASING(2) */
	__le32 _pad; /* 12 */
	__le64 generation; /* 16: monotonic, bumped on every holder change */
	__le64 acquire_count; /* 24: total acquisitions (stats) */
	__u8 reserved[32]; /* 32: pad to 64B */
} __attribute__((packed));

/*
 * ME Membership Slot (64B, 1 cacheline)
 *
 * Per-node membership + liveness. Each node writes ONLY its own slot.
 * Holder scans slot[holder+1..] to find next ACTIVE node for token pass.
 * Heartbeat lives here (distributed per-node) instead of on the shared CB,
 * so liveness ticks don't invalidate a shared hot CL.
 */
struct marufs_me_membership_slot {
	__le32 magic; /*  0: MARUFS_ME_MS_MAGIC */
	__le32 status; /*  4: MARUFS_ME_NONE / MARUFS_ME_ACTIVE */
	__le32 node_id; /*  8: self node_id (validation) */
	__le32 _pad; /* 12 */
	__le64 joined_at; /* 16: ktime_get_ns() at status change */
	__le64 heartbeat; /* 24: self-tick counter (poll thread advances) */
	__le64 heartbeat_ts; /* 32: ktime_get_ns() at last tick */
	/* 40: per-shard pending bitmap — bit s set ⇒ this node has a hand
	 * up on shard s. Single-writer (owning node). Holder ORs peers' masks
	 * to skip scans. Width caps at MARUFS_NRHT_MAX_NUM_SHARDS (64).
	 */
	__le64 pending_shards_mask; /* 40 */
	__u8 reserved[16]; /* 48: pad to 64B */
} __attribute__((packed));

/*
 * ME Slot (64B, 1 cacheline)
 *
 * Per-(shard, node) slot. Interpretation depends on strategy:
 *
 *   Order-driven (doorbell):
 *     Single-writer: current token holder. Owner NEVER writes its own slot.
 *     Holder bumps @token_seq + snapshots @cb_gen_at_write when passing the
 *     token; owner polls @token_seq locally and enters CS on change.
 *
 *   Request-driven (hand-raise):
 *     Writer ownership is split by @requesting value:
 *       requesting == 0  → owner may write (sets requesting=1 to raise hand)
 *       requesting == 1  → holder may write (sets granted_at, flips to 0)
 *     Token handoff to the granted node is ALSO rung via @token_seq so the
 *     granted node polls a per-node CL instead of the shared CB.
 *
 * Layout: slots[shard_id * max_nodes + internal_idx]
 */
struct marufs_me_slot {
	__le32 magic; /*  0: MARUFS_ME_SLOT_MAGIC */
	__le32 from_node; /*  4: last writer node_id (observability) */

	/* Order-driven doorbell (holder-written) */
	__le64 token_seq; /*  8: monotonic; owner polls this */
	__le64 cb_gen_at_write; /* 16: cb->generation snapshot at pass time */

	/* Request-driven hand-raise (writer-split by @requesting) */
	__le32 requesting; /* 24: 0=idle (self writes), 1=requesting (holder writes) */
	__le32 sequence; /* 28: monotonic seq# (stale request fencing) */
	__le64 requested_at; /* 32: self writes when raising hand */
	__le64 granted_at; /* 40: holder writes on grant */

	__u8 reserved[16]; /* 48: pad to 64B */
} __attribute__((packed));

/* ── CXL Address Helpers ──────────────────────────────────────────────
 *
 * Compute CXL addresses of CB / membership / slot entries from
 * (me_area_base, offset, index). Return void* so callers can cast to the
 * typed pointer once. Used during format/create (before typed arrays are
 * cached in the instance) and from nrht bootstrap.
 */
static inline void *marufs_me_cb_at(void *me_area_base, u64 cb_off, u32 s)
{
	return (u8 *)me_area_base + cb_off +
	       (u64)s * sizeof(struct marufs_me_cb);
}

static inline void *marufs_me_membership_at(void *me_area_base, u64 mem_off,
					    u32 n)
{
	return (u8 *)me_area_base + mem_off +
	       (u64)n * sizeof(struct marufs_me_membership_slot);
}

static inline void *marufs_me_slot_at(void *me_area_base, u64 slot_off,
				      u32 max_nodes, u32 s, u32 n)
{
	return (u8 *)me_area_base + slot_off +
	       ((u64)s * max_nodes + n) * sizeof(struct marufs_me_slot);
}

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

/*
 * ME_HOLD - increment hold counter (acquire path).
 * smp_mb() pairs with poll_cycle's smp_mb() to prevent store→load reordering.
 * Multiple threads on the same node can hold simultaneously; poll thread
 * only passes the token when the counter reaches 0.
 */
#define ME_HOLD(me, shard)                                  \
	do {                                                \
		atomic_inc(&(me)->shards[(shard)].holding); \
		smp_mb();                                   \
	} while (0)

/*
 * ME_UNHOLD - decrement hold counter (release path).
 * wmb() ensures all prior CXL writes (RAT, index, etc.) from this CPU
 * are ordered before the counter decrement becomes visible to the poll thread.
 * Without this, the poll thread (different CPU) can pass the token before
 * this CPU's CXL flushes are globally visible — causing the next holder
 * to read stale data.
 * smp_mb() pairs with poll_cycle's smp_mb() for DRAM visibility.
 */
#define ME_UNHOLD(me, shard)                                                \
	do {                                                                \
		wmb();                                                      \
		int __v = atomic_dec_if_positive(                           \
			&(me)->shards[(shard)].holding);                    \
		WARN_ON_ONCE(__v < 0); /* unbalanced UNHOLD — caller bug */ \
		smp_mb();                                                   \
	} while (0)

/*
 * ME_RESET_HOLDING - force counter to 0 (leave/cleanup path only).
 * Used during node leave to prevent underflow from blind dec.
 */
#define ME_RESET_HOLDING(me, shard)                            \
	do {                                                   \
		atomic_set(&(me)->shards[(shard)].holding, 0); \
		smp_mb();                                      \
	} while (0)

/*
 * ME_IS_IDLE - check if no one on this node holds the shard.
 * smp_mb() before read pairs with ME_HOLD/ME_UNHOLD's smp_mb().
 */
#define ME_IS_IDLE(me, shard)                                     \
	({                                                        \
		smp_mb();                                         \
		atomic_read(&(me)->shards[(shard)].holding) == 0; \
	})

/*
 * ME_BECOME_HOLDER - publish "we are now the token holder" to other CPUs.
 *
 * Callers MUST write any DRAM baselines (last_cb_gen / last_token_seq /
 * poll_last_slot_seq) BEFORE invoking this macro. The smp_wmb() pairs with
 * the smp_rmb() in wait_for_token's fast path: once a reader observes
 * is_holder=true, all prior baseline stores are guaranteed visible.
 * Sole publish point for is_holder=true — keeps the barrier rule in one
 * place instead of scattered across call sites.
 */
#define ME_BECOME_HOLDER(sh)            \
	do {                            \
		smp_wmb();              \
		(sh)->is_holder = true; \
	} while (0)

/*
 * ME_LOSE_HOLDER - counterpart for "we gave the token away".
 *
 * No barrier needed: a reader that observes is_holder=false falls to the
 * wait_for_token loop path, which performs its own CB RMB to re-verify
 * ownership. Kept as a symmetric macro so every is_holder mutation goes
 * through this header.
 */
#define ME_LOSE_HOLDER(sh)               \
	do {                             \
		(sh)->is_holder = false; \
	} while (0)

/*
 * ME_IS_HOLDER - reader pair for ME_BECOME_HOLDER. smp_rmb ensures any
 * baseline stores published by the writer are visible once is_holder
 * reads true.
 */
#define ME_IS_HOLDER(sh)         \
	({                       \
		smp_rmb();       \
		(sh)->is_holder; \
	})

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

/* me_cb_bump_acquire_count - holder records a successful acquire. */
static inline void me_cb_bump_acquire_count(struct marufs_me_cb *cb)
{
	WRITE_LE64(cb->acquire_count, READ_CXL_LE64(cb->acquire_count) + 1);
	MARUFS_CXL_WMB(&cb->acquire_count, sizeof(cb->acquire_count));
}

/*
 * me_cb_snapshot - RMB the CB cacheline and return (holder, generation).
 * Pass NULL for @out_gen when only the holder is needed. A single RMB
 * covers both fields (same CL), so callers wanting either one or both
 * should prefer this helper over separate reads.
 */
static inline u32 me_cb_snapshot(struct marufs_me_cb *cb, u64 *out_gen)
{
	MARUFS_CXL_RMB(cb, sizeof(*cb));
	if (out_gen)
		*out_gen = READ_CXL_LE64(cb->generation);
	return READ_CXL_LE32(cb->holder);
}

/* Per-(shard, internal_idx) slot lookup in the instance's cached array. */
static inline struct marufs_me_slot *me_slot_of(struct marufs_me_instance *me,
						u32 shard_id, u32 idx)
{
	return &me->slots[(u64)shard_id * me->max_nodes + idx];
}

/* Self-slot shortcut (idx = me->me_idx). */
static inline struct marufs_me_slot *me_my_slot(struct marufs_me_instance *me,
						u32 shard_id)
{
	return me_slot_of(me, shard_id, me->me_idx);
}

/*
 * Leave-path successor: next ACTIVE node or HOLDER_NONE if we're alone.
 * Lets leave paths vacate CB so a later joiner NONE-claims the shard.
 */
/* Forward decl — needed by me_leave_successor inline below. */
u32 marufs_me_next_active(struct marufs_me_instance *me, u32 from);
static inline u32 me_leave_successor(struct marufs_me_instance *me)
{
	u32 succ = marufs_me_next_active(me, me->node_id);
	return (succ == me->node_id) ? MARUFS_ME_HOLDER_NONE : succ;
}

/*
 * me_membership_{set,clear}_pending - flip bit @shard_id in own
 * pending_shards_mask with bounded CAS retry.
 *
 * Cross-node: sole writer of own membership slot — no remote contention.
 * Intra-node: multiple threads may race on different bits of the same word;
 * CAS serializes them.
 *
 * Must be called AFTER the slot.requesting WMB so holders observing the bit
 * see a fully-written request slot.
 *
 * Bounded retry + cpu_relax + WARN_ONCE on exhaustion: release-path full
 * scan covers a missing set-bit under real traffic; a stale set-bit only
 * costs one wasted poll-path slot RMB (self-skipped during scan).
 * Correctness preserved in either case.
 */
#define MARUFS_ME_PENDING_CAS_RETRIES 64

static inline void me_membership_set_pending(struct marufs_me_instance *me,
					     u32 shard_id)
{
	struct marufs_me_membership_slot *ms = &me->membership[me->me_idx];
	u64 bit = 1ULL << shard_id;

	for (int r = 0; r < MARUFS_ME_PENDING_CAS_RETRIES; r++) {
		u64 old = READ_CXL_LE64(ms->pending_shards_mask);

		if (old & bit)
			return;
		if (marufs_le64_cas(&ms->pending_shards_mask, old, old | bit) ==
		    old)
			return;
		cpu_relax();
	}
	WARN_ONCE(1, "me: pending_shards_mask set stuck (shard=%u)\n",
		  shard_id);
}

static inline void me_membership_clear_pending(struct marufs_me_instance *me,
					       u32 shard_id)
{
	struct marufs_me_membership_slot *ms = &me->membership[me->me_idx];
	u64 bit = 1ULL << shard_id;

	for (int r = 0; r < MARUFS_ME_PENDING_CAS_RETRIES; r++) {
		u64 old = READ_CXL_LE64(ms->pending_shards_mask);

		if (!(old & bit))
			return;
		if (marufs_le64_cas(&ms->pending_shards_mask, old,
				    old & ~bit) == old)
			return;
		cpu_relax();
	}
	WARN_ONCE(1, "me: pending_shards_mask clear stuck (shard=%u)\n",
		  shard_id);
}

/*
 * me_membership_tick_heartbeat - advance own heartbeat; deactivate ME if
 * the cached membership slot pointer no longer addresses a real slot
 * (peer reformatted the area — magic mismatch).
 */
static inline void me_membership_tick_heartbeat(struct marufs_me_instance *me)
{
	struct marufs_me_membership_slot *ms = &me->membership[me->me_idx];
	MARUFS_CXL_RMB(ms, sizeof(*ms));
	atomic64_inc(&me->poll_rmb_membership);
	if (READ_CXL_LE32(ms->magic) != MARUFS_ME_MS_MAGIC) {
		atomic_set(&me->active, 0);
		return;
	}
	WRITE_LE64(ms->heartbeat, READ_CXL_LE64(ms->heartbeat) + 1);
	WRITE_LE64(ms->heartbeat_ts, ktime_get_ns());
	MARUFS_CXL_WMB(&ms->heartbeat, 16);
}

/*
 * me_pass_token - transfer token to @new_holder with correct ordering.
 *
 * Writer order (critical for reader double-check):
 *   1. Update CB first (holder + generation), WMB so CB is globally visible.
 *   2. Ring the doorbell (slot->cb_gen_at_write + slot->token_seq), WMB.
 *
 * The CB being visible BEFORE the doorbell guarantees that any reader who
 * observes a new token_seq can safely cross-check cb->holder / generation
 * without seeing a stale CB.
 *
 * @new_holder may be MARUFS_ME_HOLDER_NONE — in that case only CB is written
 * (no valid slot to ring).
 *
 * Caller must currently own the token (or be in a bootstrap/recovery path
 * where the write is protected by other means, e.g. first-node init).
 */
static inline void me_pass_token(struct marufs_me_instance *me, u32 shard_id,
				 u32 new_holder)
{
	/* Reject invalid node_id before touching CB: only HOLDER_NONE or
	 * [1, max_nodes] are valid inputs. Corrupt CB would mislead all
	 * readers about ownership.
	 */
	if (new_holder != MARUFS_ME_HOLDER_NONE &&
	    !marufs_me_is_valid_node(me, new_holder)) {
		WARN_ONCE(1,
			  "marufs_me: invalid new_holder=%u (max_nodes=%u)\n",
			  new_holder, me->max_nodes);
		return;
	}

	/* Step 1: CB update — must become visible before the doorbell.
	 * Magic check: deactivate self if the cached cb pointer no longer
	 * addresses a CB record (stale layout after peer reformat).
	 */
	struct marufs_me_cb *cb = &me->cbs[shard_id];
	MARUFS_CXL_RMB(cb, sizeof(*cb));
	atomic64_inc(&me->poll_rmb_cb);
	if (READ_CXL_LE32(cb->magic) != MARUFS_ME_CB_MAGIC) {
		atomic_set(&me->active, 0);
		return;
	}

	u64 new_gen = READ_CXL_LE64(cb->generation) + 1;
	WRITE_LE32(cb->holder, new_holder);
	WRITE_LE64(cb->generation, new_gen);
	MARUFS_CXL_WMB(cb, sizeof(*cb));

	struct marufs_me_shard *sh = &me->shards[shard_id];

	/* Step 2: doorbell (skip when clearing ownership — no slot to ring). */
	if (new_holder == MARUFS_ME_HOLDER_NONE) {
		ME_LOSE_HOLDER(sh);
		return;
	}

	struct marufs_me_slot *slot = me_slot_of(me, shard_id, new_holder - 1);
	MARUFS_CXL_RMB(slot, sizeof(*slot));
	atomic64_inc(&me->poll_rmb_slot);
	if (READ_CXL_LE32(slot->magic) != MARUFS_ME_SLOT_MAGIC) {
		atomic_set(&me->active, 0);
		return;
	}

	u64 new_seq = READ_CXL_LE64(slot->token_seq) + 1;
	WRITE_LE32(slot->from_node, me->node_id);
	WRITE_LE64(slot->cb_gen_at_write, new_gen);
	WRITE_LE64(slot->token_seq, new_seq);
	MARUFS_CXL_WMB(slot, sizeof(*slot));

	if (new_holder == me->node_id) {
		sh->last_cb_gen = new_gen;
		sh->last_token_seq = new_seq;
		sh->poll_last_slot_seq = new_seq;
		ME_BECOME_HOLDER(sh);
	} else {
		ME_LOSE_HOLDER(sh);
	}
}

/*
 * me_shard_passable - true iff this node currently holds the token but has
 * no local work pending. Used by poll/release paths to decide whether to
 * forward the token instead of keeping it.
 */
static inline bool me_shard_passable(struct marufs_me_instance *me,
				     u32 shard_id)
{
	return ME_IS_IDLE(me, shard_id) &&
	       atomic_read(&me->shards[shard_id].local_waiters) == 0;
}

/* ── Common infrastructure (me.c) ─────────────────────────────────── */

struct marufs_me_instance *marufs_me_create(void *me_area_base, u32 num_shards,
					    u32 max_nodes, u32 node_id,
					    u32 poll_interval_us,
					    enum marufs_me_strategy strategy);
void marufs_me_destroy(struct marufs_me_instance *me);

/* Registry API — unified poll thread managed by sbi.
 *
 * Lifecycle:
 *   mount:  marufs_me_registry_init(sbi)
 *           → marufs_me_registry_start(sbi)  // starts single poll thread
 *           → [for each ME] marufs_me_register(sbi, me)
 *   unmount: [for each ME] marufs_me_unregister(sbi, me)
 *           → marufs_me_registry_stop(sbi)
 */
struct marufs_sb_info;

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

u32 marufs_me_next_active(struct marufs_me_instance *me, u32 from);
int marufs_me_format(void *me_area_base, u32 num_shards, u32 max_nodes,
		     u32 poll_interval_us, enum marufs_me_strategy strategy);

/* ── Shared strategy primitives ───────────────────────────────────── */

/*
 * marufs_me_wait_for_token - spin-then-sleep until cb->holder == self or
 * deadline expires. Caller must hold local_locks[shard_id] and have ME_HOLD'd.
 * Returns 0 on success, -ETIMEDOUT on deadline.
 */
int marufs_me_wait_for_token(struct marufs_me_instance *me, u32 shard_id);

/*
 * marufs_me_common_* - strategy-independent ops implementations used by
 * both order-driven and request-driven vtables.
 */
int marufs_me_common_join(struct marufs_me_instance *me);
void marufs_me_common_leave(struct marufs_me_instance *me);

/* ── Strategy implementations ─────────────────────────────────────── */

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
