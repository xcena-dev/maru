// SPDX-License-Identifier: GPL-2.0-only
/*
 * index.c - MARUFS Global Index CAS operations
 *
 * Lock-free global index for file metadata. Index is sharded by filename hash,
 * with each shard containing a hash-bucket array and flat entry array.
 * All mutations use CAS (cmpxchg) on entry state fields and bucket head
 * pointers for multi-node safety.
 *
 * Entry state machine:
 *   EMPTY --CAS--> INSERTING --> TENTATIVE --> VALID --CAS--> TOMBSTONE
 *
 * Bucket chains are singly-linked via next_in_bucket, terminating with
 * MARUFS_BUCKET_END (0xFFFFFFFF).
 */

#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/prefetch.h>
#include <linux/slab.h>
#include <linux/string.h>
#include <linux/types.h>

#include "marufs.h"

/* Maximum CAS retries for bucket chain insertion */

static inline void index_shard_lock(struct marufs_shard_cache *sc)
{
	spin_lock(&sc->insert_lock);
}

static inline void index_shard_unlock(struct marufs_shard_cache *sc)
{
	spin_unlock(&sc->insert_lock);
}

/* CAS current state → INSERTING and stamp inserter identity for GC. */
static inline bool marufs_index_claim_entry(struct marufs_sb_info *sbi,
					    struct marufs_index_entry *e)
{
	u32 st = READ_LE32(e->state);
	if (st != MARUFS_ENTRY_EMPTY && st != MARUFS_ENTRY_TOMBSTONE)
		return false;
	if (marufs_le32_cas(&e->state, st, MARUFS_ENTRY_INSERTING) != st)
		return false;

	WRITE_LE64(e->created_at, ktime_get_real_ns());
	WRITE_LE32(e->node_id, sbi->node_id);
	/* WMB deferred to caller (step 4 / link_and_publish) */
	return true;
}

/*
 * marufs_index_check_duplicate - check for duplicate name in bucket chain
 * @sbi: superblock info
 * @entries: shard hot entry array
 * @bucket_head: pointer to bucket head entry
 * @hash: name hash to match
 * @name: filename to match
 * @namelen: filename length
 * @reuse_idx: output — index of first reusable entry (TOMBSTONE or EMPTY)
 *             found in chain, or MARUFS_BUCKET_END if none.  Caller can
 *             reuse this slot instead of scanning the entry array.
 *
 * Return: 0 if no duplicate, -EEXIST if name already exists
 */
static int marufs_index_check_duplicate(struct marufs_sb_info *sbi,
					struct marufs_index_entry *entries,
					u32 *bucket_head, u64 hash,
					const char *name, size_t namelen,
					u32 *reuse_idx)
{
	MARUFS_CXL_RMB(bucket_head, sizeof(*bucket_head));
	u32 cur = READ_CXL_LE32(*bucket_head);
	u32 steps = 0;
	u32 *prev_next = bucket_head;
	u32 num_entries = sbi->entries_per_shard;
	*reuse_idx = MARUFS_BUCKET_END;

	while (cur != MARUFS_BUCKET_END && cur < num_entries) {
		if (++steps > num_entries) {
			pr_err("bucket chain cycle detected (dup check)\n");
			return -EIO;
		}

		/* Direct access + manual RMB to avoid per-iteration bounds check
		 * in marufs_shard_entry() — caller already validated shard geometry. */
		struct marufs_index_entry *e = &entries[cur];
		MARUFS_CXL_RMB(e, sizeof(*e));
		u32 st = READ_CXL_LE32(e->state);

		if (st == MARUFS_ENTRY_TOMBSTONE || st == MARUFS_ENTRY_EMPTY) {
			/* Pre-read successor before any modification */
			u32 next = READ_CXL_LE32(e->next_in_bucket);

			if (*reuse_idx == MARUFS_BUCKET_END) {
				/* Keep first dead entry for in-place reuse */
				*reuse_idx = cur;
				prev_next = (u32 *)&e->next_in_bucket;
			} else {
				/*
				 * Inline unlink: skip this dead entry by
				 * pointing prev directly to cur's successor.
				 * Best-effort — CAS failure is harmless.
				 * On success, reclaim to EMPTY so flat scan
				 * can safely reuse (no chain pollution since
				 * entry is no longer linked).
				 */
				if (marufs_le32_cas(prev_next, cur, next) ==
				    cur) {
					/* Unlinked — reclaim to EMPTY.
					 * Stale name/hash are harmless:
					 * flat scan only checks state,
					 * and the claimer overwrites all
					 * fields before publishing VALID.
					 * marufs_le32_cas includes CXL WMB. */
					marufs_le32_cas(&e->state, st,
							MARUFS_ENTRY_EMPTY);
				}
				/* Don't advance prev_next — it now points
				 * to the successor, re-read from there. */
			}
			cur = next;
			continue;
		}

		/*
		 * Only VALID, TENTATIVE, or INSERTING remain (dead handled
		 * above).  Skip INSERTING/TENTATIVE — not yet committed;
		 * concurrent duplicates caught by post-insert dedup (step 7),
		 * stale entries cleaned up by GC.
		 */
		if (st == MARUFS_ENTRY_VALID &&
		    READ_CXL_LE64(e->name_hash) == hash &&
		    marufs_rat_name_matches(sbi, READ_CXL_LE32(e->region_id),
					    name, namelen)) {
			pr_debug("index_insert: name '%.*s' already exists\n",
				 (int)namelen, name);
			return -EEXIST;
		}

		/* Live entry — advance prev_next past it */
		prev_next = (u32 *)&e->next_in_bucket;
		cur = READ_LE32(e->next_in_bucket);
	}

	return 0;
}

/*
 * marufs_index_post_insert_dedup - detect concurrent duplicate under shard lock.
 * Called after link_to_bucket while holding the shard lock.  Walks the
 * bucket chain looking for another VALID entry with the same name.
 * If found, returns -EEXIST so the caller can tombstone and release.
 */
static int marufs_index_post_insert_dedup(struct marufs_sb_info *sbi,
					  struct marufs_index_entry *entries,
					  u32 *bucket_head,
					  struct marufs_index_entry *entry,
					  u32 entry_idx, u64 hash,
					  const char *name, size_t namelen)
{
	MARUFS_CXL_RMB(bucket_head, sizeof(*bucket_head));
	u32 cur = READ_CXL_LE32(*bucket_head);
	u32 num_entries = sbi->entries_per_shard;
	u32 steps = 0;

	while (cur != MARUFS_BUCKET_END && cur < num_entries) {
		if (++steps > num_entries) {
			pr_err("bucket chain cycle detected (post-insert dedup)\n");
			break;
		}

		struct marufs_index_entry *e = &entries[cur];
		MARUFS_CXL_RMB(e, sizeof(*e));

		if (cur != entry_idx &&
		    READ_CXL_LE32(e->state) == MARUFS_ENTRY_VALID &&
		    READ_CXL_LE64(e->name_hash) == hash &&
		    marufs_rat_name_matches(sbi, READ_CXL_LE32(e->region_id),
					    name, namelen)) {
			if (entry_idx > cur) {
				pr_info("index_insert: post-insert dup '%.*s' "
					"(entry %u loses to %u)\n",
					(int)namelen, name, entry_idx, cur);
				return -EEXIST;
			}
		}
		cur = READ_LE32(e->next_in_bucket);
	}

	return 0;
}

/*
 * marufs_index_link_to_bucket - prepend entry to bucket chain via CAS.
 * Self-loop guard: if entry is already the head (stale link from previous
 * life), skip linking to avoid infinite chain walk.
 * Return: 0 on success, -EAGAIN if CAS retries exhausted
 */
static int marufs_index_link_to_bucket(struct marufs_index_entry *entry,
				       u32 entry_idx, u32 *bucket_head)
{
	const int max_retries = 32;
	int retries = 0;
	for (;;) {
		MARUFS_CXL_RMB(bucket_head, sizeof(*bucket_head));
		u32 old_head = READ_CXL_LE32(*bucket_head);

		if (unlikely(old_head == entry_idx)) {
			/* Already the bucket head (stale link from previous life).
			 * Chain successor is intact — just skip linking. */
			break;
		}

		WRITE_LE32(entry->next_in_bucket, old_head);
		MARUFS_CXL_WMB(entry, sizeof(*entry));

		if (marufs_le32_cas(bucket_head, old_head, entry_idx) ==
		    old_head)
			break;

		if (++retries >= max_retries) {
			/*
			 * Extremely rare: revert entry to EMPTY for slot reuse.
			 */
			pr_err("index_insert: bucket CAS failed after %d retries, reverting\n",
			       retries);
			WRITE_LE32(entry->state, MARUFS_ENTRY_EMPTY);
			MARUFS_CXL_WMB(entry, sizeof(*entry));
			return -EAGAIN;
		}
		cpu_relax();
	}

	return 0;
}

/*
 * marufs_index_insert - insert new file into global index
 * @sbi: superblock info
 * @name: filename
 * @namelen: filename length
 * @region_id: region where data is allocated (= RAT entry ID)
 * @out_entry_idx: output - global entry index within shard
 *
 * Insert protocol (shard-lock serialized):
 *   1. Pre-insert dup check (lock-free, best-effort)
 *   2. CAS entry state EMPTY/TOMBSTONE -> INSERTING (reserve slot)
 *   3. Fill entry fields
 *   4. WRITE state to TENTATIVE (visible to dedup, not to lookup)
 *   5. Acquire shard lock
 *   6. CAS bucket head to link entry into hash chain
 *   7. Post-insert dedup → loser TOMBSTONE, winner VALID (publish)
 *   8. Release shard lock
 *
 * Return: 0 on success, -ENOSPC if shard full, -EEXIST if name exists
 */
/* Internal insert with caller-supplied hash */
static int __marufs_index_insert(struct marufs_sb_info *sbi, const char *name,
				 size_t namelen, u64 hash, u32 region_id,
				 u32 *out_entry_idx)
{
	/* Step 1: select shard + bucket from hash (shard_cache = local DRAM) */
	u32 shard_id = marufs_shard_idx(hash, sbi->shard_mask);
	u32 bucket_id = marufs_bucket_idx(hash, sbi->bucket_mask);
	struct marufs_shard_cache *sc = &sbi->shard_cache[shard_id];
	if (!sc->entries || !sc->buckets) {
		pr_err("index_insert: NULL entries/buckets for shard %u\n",
		       shard_id);
		return -EINVAL;
	}

	struct marufs_index_entry *entries = sc->entries;
	u32 *bucket_head = &sc->buckets[bucket_id];

	/*
	 * Step 2: check for duplicate name before reserving slot.
	 * Walk bucket chain to find existing VALID entry with same name.
	 * Also records the first dead entry (TOMBSTONE/EMPTY) for reuse.
	 *
	 * NOTE: This pre-insert check has a TOCTOU window — two nodes can both
	 * pass it and insert the same name. Post-insert dedup after step 6
	 * detects and resolves this: the loser (higher entry_idx) rolls back
	 * to TOMBSTONE and returns -EEXIST.
	 */
	u32 chain_reuse_idx = MARUFS_BUCKET_END;
	int ret = marufs_index_check_duplicate(sbi, entries, bucket_head, hash,
					       name, namelen, &chain_reuse_idx);
	if (ret)
		return ret;

	/*
	 * Step 3a: try to reuse a dead entry (TOMBSTONE or EMPTY) found in
	 * the bucket chain.  The entry is already linked, so we skip
	 * link_and_publish later.  Try CAS with both possible states.
	 */
	struct marufs_index_entry *entry = NULL;
	u32 entry_idx;
	bool reused_chain_entry = false;

	/* check_duplicate guarantees reuse_idx was dead; CAS-claim it */
	if (chain_reuse_idx != MARUFS_BUCKET_END) {
		struct marufs_index_entry *e = &entries[chain_reuse_idx];

		if (marufs_index_claim_entry(sbi, e)) {
			entry_idx = chain_reuse_idx;
			entry = e;
			reused_chain_entry = true;
			pr_debug(
				"index_insert: reusing chain entry %u in shard %u\n",
				entry_idx, shard_id);
		}
	}

	/*
	 * Step 3b: fallback — scan hot entry array for EMPTY slot if no
	 * tombstone was reused.  Start from shard_free_hint to skip
	 * known-occupied prefix (H-P1: O(1) amortized vs O(n) linear).
	 */
	u32 num_entries = sbi->entries_per_shard;
	if (!entry) {
		u32 hint = (u32)atomic_read(&sc->free_hint);
		if (hint >= num_entries)
			hint = 0;

		for (u32 scan = 0; scan < num_entries; scan++) {
			entry_idx = hint + scan;
			if (entry_idx >= num_entries)
				entry_idx -= num_entries;

			struct marufs_index_entry *e = &entries[entry_idx];
			MARUFS_CXL_RMB(e, sizeof(*e));
			if (READ_CXL_LE32(e->state) != MARUFS_ENTRY_EMPTY)
				continue;

			if (marufs_index_claim_entry(sbi, e)) {
				hint = entry_idx + 1;
				if (hint >= num_entries)
					hint = 0;

				entry = e;
				atomic_set(&sc->free_hint, hint);
				break;
			}
		}
	}

	if (!entry) {
		pr_debug("index_insert: shard %u full (%u entries)\n", shard_id,
			 num_entries);
		return -ENOSPC;
	}

	/* Step 4: fill entry fields (INSERTING — exclusively owned) */
	WRITE_LE64(entry->name_hash, hash);
	WRITE_LE32(entry->region_id, region_id);
	MARUFS_CXL_WMB(entry, sizeof(*entry));

	/* Step 5: INSERTING → TENTATIVE (visible to dedup, not to lookup) */
	WRITE_LE32(entry->state, MARUFS_ENTRY_TENTATIVE);
	MARUFS_CXL_WMB(entry, sizeof(*entry));

	/* Step 6: acquire shard lock — serializes link + dedup within node */
	index_shard_lock(sc);

	/* Step 7: link to bucket (skip if reused — already in chain) */
	if (!reused_chain_entry) {
		ret = marufs_index_link_to_bucket(entry, entry_idx,
						  bucket_head);
		if (ret) {
			WRITE_LE32(entry->state, MARUFS_ENTRY_TOMBSTONE);
			MARUFS_CXL_WMB(entry, sizeof(*entry));
			goto unlock;
		}
	}

	/* Step 8: post-insert dedup — concurrent inserts resolved here */
	ret = marufs_index_post_insert_dedup(sbi, entries, bucket_head, entry,
					     entry_idx, hash, name, namelen);
	if (ret) {
		WRITE_LE32(entry->state, MARUFS_ENTRY_TOMBSTONE);
		MARUFS_CXL_WMB(entry, sizeof(*entry));
		goto unlock;
	}

	/* Step 9: TENTATIVE → VALID (now visible to lookup) */
	WRITE_LE32(entry->state, MARUFS_ENTRY_VALID);
	MARUFS_CXL_WMB(entry, sizeof(*entry));

	*out_entry_idx = entry_idx;

unlock:
	index_shard_unlock(sc);
	return ret;
}

int marufs_index_insert(struct marufs_sb_info *sbi, const char *name,
			size_t namelen, u32 region_id, u32 *out_entry_idx)
{
	u64 hash;

	if (!sbi || !name || !out_entry_idx)
		return -EINVAL;
	if (namelen == 0 || namelen > MARUFS_NAME_MAX)
		return -ENAMETOOLONG;
	if (unlikely(!marufs_shard_geometry_valid(sbi->buckets_per_shard,
						  sbi->entries_per_shard))) {
		pr_err("shard corrupted (buckets=%u entries=%u) - reformat needed\n",
		       sbi->buckets_per_shard, sbi->entries_per_shard);
		return -EIO;
	}

	hash = marufs_hash_name(name, namelen);
	return __marufs_index_insert(sbi, name, namelen, hash, region_id,
				     out_entry_idx);
}

/*
 * marufs_index_lookup - lookup file by name in global index
 * @sbi: superblock info
 * @name: filename to search
 * @namelen: filename length
 * @out_entry: output pointer to found entry (NULL if not found)
 *
 * Lock-free read: walks bucket chain checking for VALID entries with matching
 * name_hash and name. Uses rmb() after state check before reading entry fields.
 *
 * Return: 0 on success, -ENOENT if not found
 */
/* Lookup result — shared between lookup and delete */
struct marufs_index_result {
	struct marufs_index_entry *entry; /* matched CXL entry pointer */
	u32 *prev_next; /* pointer to prev's next_in_bucket (or bucket head) */
	u32 shard_id; /* shard containing the entry */
	u32 entry_idx; /* entry index within the shard */
};

/*
 * __marufs_index_lookup - internal chain walk for lookup/delete
 * @sbi: superblock info
 * @name: filename to search
 * @namelen: filename length
 * @result: output — entry pointer, prev link, shard/entry indices
 *
 * Return: 0 on success, -ENOENT if not found
 */
static int __marufs_index_lookup(struct marufs_sb_info *sbi, const char *name,
				 size_t namelen,
				 struct marufs_index_result *result)
{
	u64 hash;
	u32 shard_id, bucket_idx, num_entries;
	u32 cur, steps = 0;
	u32 *prev_next;

	if (!sbi || !name || !result)
		return -EINVAL;
	if (namelen == 0 || namelen > MARUFS_NAME_MAX)
		return -ENOENT;

	hash = marufs_hash_name(name, namelen);
	shard_id = marufs_shard_idx(hash, sbi->shard_mask);
	num_entries = sbi->entries_per_shard;
	bucket_idx = marufs_bucket_idx(hash, sbi->bucket_mask);
	prev_next = marufs_shard_bucket(sbi, shard_id, bucket_idx);
	if (!prev_next)
		return -ENOENT;

	cur = READ_CXL_LE32(*prev_next);

	while (cur != MARUFS_BUCKET_END && cur < num_entries) {
		struct marufs_index_entry *entry =
			marufs_shard_entry(sbi, shard_id, cur);
		u32 next, state;

		if (!entry)
			return -EIO;

		if (++steps > num_entries) {
			pr_err("bucket chain cycle detected (lookup, shard %u bucket %u)\n",
			       shard_id, bucket_idx);
			return -EIO;
		}

		state = READ_CXL_LE32(entry->state);
		next = READ_CXL_LE32(entry->next_in_bucket);

		if (state == MARUFS_ENTRY_VALID &&
		    READ_CXL_LE64(entry->name_hash) == hash &&
		    marufs_rat_name_matches(sbi,
					    READ_CXL_LE32(entry->region_id),
					    name, namelen)) {
			result->entry = entry;
			result->prev_next = prev_next;
			result->shard_id = shard_id;
			result->entry_idx = cur;
			return 0;
		}

		prev_next = (u32 *)&entry->next_in_bucket;
		cur = next;
	}

	return -ENOENT;
}

int marufs_index_lookup(struct marufs_sb_info *sbi, const char *name,
			size_t namelen, struct marufs_index_entry **out_entry)
{
	struct marufs_index_result result;
	int ret;

	if (!out_entry)
		return -EINVAL;

	ret = __marufs_index_lookup(sbi, name, namelen, &result);
	*out_entry = ret ? NULL : result.entry;
	return ret;
}

/*
 * marufs_index_delete - delete file from global index
 * @sbi: superblock info
 * @name: filename to delete
 * @namelen: filename length
 *
 * CAS VALID → TOMBSTONE (logical delete). Entry stays in chain for
 * in-place reuse by the next insert on the same bucket (step 3a).
 *
 * Return: 0 on success, -ENOENT if not found
 */
int marufs_index_delete(struct marufs_sb_info *sbi, const char *name,
			size_t namelen)
{
	struct marufs_index_result result;
	int ret = __marufs_index_lookup(sbi, name, namelen, &result);
	if (ret)
		return ret;

	/* CAS VALID → TOMBSTONE (stays in chain for reuse) */
	if (marufs_le32_cas(&result.entry->state, MARUFS_ENTRY_VALID,
			    MARUFS_ENTRY_TOMBSTONE) != MARUFS_ENTRY_VALID) {
		pr_debug("index_delete: CAS failed for '%.*s'\n", (int)namelen,
			 name);
		return -ENOENT;
	}
	/* CAS includes implicit full barrier — no extra WMB needed */

	pr_debug("index_delete: '%.*s' deleted\n", (int)namelen, name);
	return 0;
}
