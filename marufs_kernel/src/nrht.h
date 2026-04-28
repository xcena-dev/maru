/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * nrht.h - Independent Name-Ref Hash Table entry points.
 *
 * Per-region hash table for name -> (region_id, offset) bindings.
 * Multi-region NRHT chaining handled via marufs_nrht_join.
 */

#ifndef _MARUFS_NRHT_H
#define _MARUFS_NRHT_H

#include <linux/types.h>

#include "me.h" /* enum marufs_me_strategy */

struct marufs_sb_info;

int marufs_nrht_init(struct marufs_sb_info *sbi, u32 nrht_region_id,
		     u32 max_entries, u32 num_shards, u32 num_buckets,
		     enum marufs_me_strategy me_strategy);

/*
 * marufs_nrht_join - explicit pre-warm: create this sbi's NRHT ME instance
 * and join the ring for @nrht_region_id. Idempotent (cached on re-call).
 * Backup path is lazy-init on first insert.
 */
int marufs_nrht_join(struct marufs_sb_info *sbi, u32 nrht_region_id);

int marufs_nrht_insert(struct marufs_sb_info *sbi, u32 nrht_region_id,
		       const char *name, size_t namelen, u64 name_hash,
		       u64 offset, u32 target_region_id);
/*
 * marufs_nrht_lookup - find VALID entry by name.
 * @out_ref_count, @out_pin_count: optional (NULL ok) — receive snapshot
 * of user-managed ref/pin counts for the entry.
 */
int marufs_nrht_lookup(struct marufs_sb_info *sbi, u32 nrht_region_id,
		       const char *name, size_t namelen, u64 name_hash,
		       u64 *out_offset, u32 *out_target_region_id,
		       u32 *out_ref_count, u32 *out_pin_count);
int marufs_nrht_delete(struct marufs_sb_info *sbi, u32 nrht_region_id,
		       const char *name, size_t namelen, u64 name_hash);

/*
 * Per-entry ref/pin counter ops. Each acquires the NRHT shard ME for the
 * resolved (region, name), looks up a VALID entry, RMWs the counter, and
 * releases. @out_count receives the post-op value.
 *
 * Returns 0 on success, -ENOENT if entry missing/non-VALID,
 * -EINVAL on dec-from-zero, -EOVERFLOW on inc-from-UINT32_MAX.
 */
int marufs_nrht_ref_inc(struct marufs_sb_info *sbi, u32 nrht_region_id,
			const char *name, size_t namelen, u64 name_hash,
			u32 *out_count);
int marufs_nrht_ref_dec(struct marufs_sb_info *sbi, u32 nrht_region_id,
			const char *name, size_t namelen, u64 name_hash,
			u32 *out_count);
int marufs_nrht_pin_inc(struct marufs_sb_info *sbi, u32 nrht_region_id,
			const char *name, size_t namelen, u64 name_hash,
			u32 *out_count);
int marufs_nrht_pin_dec(struct marufs_sb_info *sbi, u32 nrht_region_id,
			const char *name, size_t namelen, u64 name_hash,
			u32 *out_count);

int marufs_nrht_gc_sweep_all(struct marufs_sb_info *sbi);

#endif /* _MARUFS_NRHT_H */
