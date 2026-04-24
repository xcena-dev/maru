/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * me_stats.h - MARUFS ME fine-grained performance counters
 *
 * All per-CPU stats counters and their accessor helpers live here.
 * Kept separate from me.h to stop the core ME header from ballooning
 * as the instrumentation surface grows.
 *
 * Layering:
 *   me.h        — core layout, forward-declares struct marufs_me_stats_pcpu
 *                 and holds only the per-CPU pointer on the instance.
 *   me_stats.h  — defines struct marufs_me_stats_pcpu and all helpers,
 *                 includes me.h for struct marufs_me_shard/instance.
 *   TU files    — include "me_stats.h" wherever they read/update stats.
 *
 * Overhead:
 *   Updates are non-atomic (`this_cpu_ptr` + plain `++`). A preempt /
 *   migration during update can lose or double-count a single event,
 *   which is acceptable for statistics. Sysfs readers aggregate over
 *   `for_each_possible_cpu`.
 *
 * Bucketing:
 *   Latency histograms use log2(ns) bucketing (12 buckets, <128ns ..
 *   >=128ms). Coarse enough for hot paths (fls64 + index), fine enough
 *   to separate p50/p99 regimes.
 */

#ifndef _MARUFS_ME_STATS_H
#define _MARUFS_ME_STATS_H

#include <linux/log2.h>
#include <linux/percpu.h>
#include <linux/sched.h>
#include <linux/timekeeping.h>
#include <linux/types.h>

#include "me.h"

/* ── Latency histogram layout ──────────────────────────────────────── */

/*
 * 12 buckets covers [<128ns] .. [>=128ms]. Bucket i holds counts where
 * ns is in [2^(i+6), 2^(i+7)); bucket 0 catches all ns < 128, bucket 11
 * saturates for ns >= 128ms.
 */
#define MARUFS_ME_LAT_BUCKETS 12
#define MARUFS_ME_LAT_BUCKET_BASE_SHIFT 7 /* first bucket top = 2^7 = 128 */

/* ── Per-CPU stats struct ──────────────────────────────────────────── */

struct marufs_me_stats_pcpu {
	/* wait_for_token */
	u64 wait_count;
	u64 wait_wall_ns;
	u64 wait_cpu_ns;
	u64 wait_spin_hit;
	u64 wait_sleep_hit;
	u64 wait_deadline_hit;
	u64 wait_fast_hit; /* ME_IS_HOLDER early-return (no token wait) */
	u64 wait_lat_buckets[MARUFS_ME_LAT_BUCKETS];

	/* poll_cycle phase breakdown (ns per invocation, summed) */
	u64 poll_ns_membership;
	u64 poll_ns_doorbell;
	u64 poll_ns_scan;

	/* CS lock hold time (mutex_lock .. mutex_unlock span) */
	u64 lock_hold_count;
	u64 lock_hold_ns_total;
	u64 lock_hold_buckets[MARUFS_ME_LAT_BUCKETS];

	/* Request-mode grant age (granted_at - requested_at) */
	u64 grant_age_count;
	u64 grant_age_buckets[MARUFS_ME_LAT_BUCKETS];

	/* Per-shard acquire count (hotspot detection). Capped to
	 * MARUFS_NRHT_MAX_NUM_SHARDS; larger shard_ids fold into the last
	 * bin rather than overflow.
	 */
	u64 per_shard_acquire[MARUFS_NRHT_MAX_NUM_SHARDS];
};

enum marufs_me_wait_hit {
	MARUFS_ME_WAIT_SPIN = 0,
	MARUFS_ME_WAIT_SLEEP,
	MARUFS_ME_WAIT_DEADLINE,
};

/*
 * me_stats_cpu_ns - return current task's cumulative on-CPU ns.
 *
 * Reads struct sched_entity::sum_exec_runtime directly, which is updated
 * at each scheduler tick (HZ granularity — ~1ms on typical configs).
 * The proper accessor `task_sched_runtime()` forces a refresh but is not
 * exported to modules, so we accept the tick-bounded staleness; the
 * error caps at one tick per wait window, which is negligible compared
 * to acquire deadlines measured in µs .. s.
 */
static inline u64 me_stats_cpu_ns(void)
{
	return current->se.sum_exec_runtime;
}

/* ── Bucket indexing ───────────────────────────────────────────────── */

/*
 * me_stats_lat_bucket - map ns → log2 bucket index in [0, nbuckets-1].
 * Bucket 0 covers ns < 2^(BASE_SHIFT) (i.e., < 128ns); bucket n-1
 * saturates at the top.
 */
static inline u32 me_stats_lat_bucket(u64 ns)
{
	if (ns < (1ULL << MARUFS_ME_LAT_BUCKET_BASE_SHIFT))
		return 0;
	u32 b = fls64(ns) - MARUFS_ME_LAT_BUCKET_BASE_SHIFT;
	return min_t(u32, b, MARUFS_ME_LAT_BUCKETS - 1);
}

/* ── Accessor helpers ──────────────────────────────────────────────── */

/*
 * me_stats_wait_fast_hit - wait_for_token early exit via ME_IS_HOLDER.
 * Called before any token-wait work; paired with me_stats_wait_done
 * which handles the full-wait exit paths. Tracking the split reveals
 * how often the intra-node fast path avoids ME traffic entirely.
 */
static inline void me_stats_wait_fast_hit(struct marufs_me_instance *me)
{
	struct marufs_me_stats_pcpu *st = this_cpu_ptr(me->stats);
	st->wait_fast_hit++;
}

/*
 * me_stats_wait_done - common exit accounting for wait_for_token.
 * Records wall + on-CPU duration, slots the wall delta into the log2
 * bucket, and bumps the phase counter for @hit.
 */
static inline void me_stats_wait_done(struct marufs_me_instance *me,
				      u64 wall_start, u64 cpu_start,
				      enum marufs_me_wait_hit hit)
{
	u64 wall_ns = ktime_get_ns() - wall_start;
	u64 cpu_ns = me_stats_cpu_ns() - cpu_start;

	/* Clamp cpu_ns to wall_ns: sum_exec_runtime updates at scheduler
	 * tick boundaries (~1ms) so a tick firing during a short wait may
	 * add pre-wait CS time into the measured delta, inflating cpu_ns
	 * above wall_ns and pushing the aggregate cpu_util above 100%.
	 * Clamping gives a physically valid upper bound; samples are still
	 * biased upward for sub-tick waits but no longer nonsensical.
	 */
	if (cpu_ns > wall_ns)
		cpu_ns = wall_ns;

	struct marufs_me_stats_pcpu *st = this_cpu_ptr(me->stats);

	st->wait_count++;
	st->wait_wall_ns += wall_ns;
	st->wait_cpu_ns += cpu_ns;
	st->wait_lat_buckets[me_stats_lat_bucket(wall_ns)]++;
	switch (hit) {
	case MARUFS_ME_WAIT_SPIN:
		st->wait_spin_hit++;
		break;
	case MARUFS_ME_WAIT_SLEEP:
		st->wait_sleep_hit++;
		break;
	case MARUFS_ME_WAIT_DEADLINE:
		st->wait_deadline_hit++;
		break;
	}
}

/*
 * me_stats_lock_acquired - stash ktime just after mutex_lock on the
 * shard struct. Paired with me_stats_lock_released. Valid only while
 * the shard's local_lock is held (single owner).
 */
static inline void me_stats_lock_acquired(struct marufs_me_shard *sh)
{
	sh->lock_hold_start_ns = ktime_get_ns();
}

/*
 * me_stats_lock_released - consume the paired timestamp and contribute
 * a lock-hold-time sample. Must be called while still holding the
 * lock, right before mutex_unlock.
 */
static inline void me_stats_lock_released(struct marufs_me_instance *me,
					  struct marufs_me_shard *sh)
{
	u64 hold_ns = ktime_get_ns() - sh->lock_hold_start_ns;
	struct marufs_me_stats_pcpu *st = this_cpu_ptr(me->stats);

	st->lock_hold_count++;
	st->lock_hold_ns_total += hold_ns;
	st->lock_hold_buckets[me_stats_lat_bucket(hold_ns)]++;
}

/*
 * me_stats_bump_shard_acquire - per-shard acquire hotspot counter.
 * Capped at MARUFS_NRHT_MAX_NUM_SHARDS - 1; over-sized shard_ids fold
 * into the last bin rather than overflow.
 */
static inline void me_stats_bump_shard_acquire(struct marufs_me_instance *me,
					       u32 shard_id)
{
	struct marufs_me_stats_pcpu *st = this_cpu_ptr(me->stats);
	u32 idx = min_t(u32, shard_id, MARUFS_NRHT_MAX_NUM_SHARDS - 1);
	st->per_shard_acquire[idx]++;
}

/*
 * me_stats_record_grant_age - request-mode grant path. @requested_at
 * is read from the requester's slot under RMB; @now is the ktime
 * captured at grant time.
 */
static inline void me_stats_record_grant_age(struct marufs_me_instance *me,
					     u64 now, u64 requested_at)
{
	if (now <= requested_at)
		return; /* clock skew or empty slot — skip */
	u64 age_ns = now - requested_at;
	struct marufs_me_stats_pcpu *st = this_cpu_ptr(me->stats);

	st->grant_age_count++;
	st->grant_age_buckets[me_stats_lat_bucket(age_ns)]++;
}

#endif /* _MARUFS_ME_STATS_H */
