/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * nrht_stats.h - MARUFS NRHT per-CPU performance counters
 *
 * Currently tracks bucket-chain walk depth observed by nrht_find_chain.
 * The counters live on struct marufs_sb_info (not per-region) because
 * chain depth is a property of the hash distribution over the NRHT as a
 * whole; aggregating across regions hides nothing for the dominant user
 * (single large NRHT per mount) and keeps allocation simple.
 *
 * Overhead model: non-atomic per-CPU updates (see me_stats.h header for
 * the same trade-off). Sysfs reads sum across for_each_possible_cpu.
 */

#ifndef _MARUFS_NRHT_STATS_H
#define _MARUFS_NRHT_STATS_H

#include <linux/log2.h>
#include <linux/percpu.h>
#include <linux/types.h>

/*
 * 8 buckets covers depth [0] .. [>=128]. Bucket i holds counts where
 * depth is in [2^(i-1), 2^i); bucket 0 is depth 0/1, bucket 7 saturates
 * for depth >= 128. In practice >16 is already pathological — the tail
 * bucket exists mostly to make overflow visible rather than silent.
 */
#define MARUFS_NRHT_DEPTH_BUCKETS 8

struct marufs_nrht_stats_pcpu {
	u64 find_chain_count;
	u64 find_chain_steps_total;
	u64 chain_depth_buckets[MARUFS_NRHT_DEPTH_BUCKETS];
};

/*
 * nrht_stats_depth_bucket - map walk depth → bucket in [0, 7].
 * depth==0 lands in bucket 0 alongside depth 1 (fls(0) returns 0).
 */
static inline u32 nrht_stats_depth_bucket(u32 depth)
{
	if (depth <= 1)
		return 0;
	u32 b = fls(depth); /* depth=2→2, =3→2, =4→3, =128→8 */
	return min_t(u32, b, MARUFS_NRHT_DEPTH_BUCKETS - 1);
}

/*
 * nrht_stats_record_chain_depth - add a sample after a bucket-chain
 * walk. Call from nrht_find_chain regardless of hit/miss. @stats may
 * be NULL before sbi init completes; caller doesn't need to guard.
 */
static inline void
nrht_stats_record_chain_depth(struct marufs_nrht_stats_pcpu __percpu *stats,
			      u32 steps)
{
	if (!stats)
		return;

	struct marufs_nrht_stats_pcpu *st = this_cpu_ptr(stats);
	st->find_chain_count++;
	st->find_chain_steps_total += steps;
	st->chain_depth_buckets[nrht_stats_depth_bucket(steps)]++;
}

#endif /* _MARUFS_NRHT_STATS_H */
