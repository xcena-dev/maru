// SPDX-License-Identifier: GPL-2.0-only
/*
 * nrht.c - MARUFS Independent Name-Ref Hash Table (NRHT)
 *
 * NRHT is an independent CXL file that maps names to (offset, region_id)
 * pairs. Unlike the global index where name-refs and region files share
 * the same shards, each NRHT file has its own dedicated hash table with
 * clean lifecycle management.
 *
 * Physical layout (within region data area):
 *   [NRHT Header 64B] [Shard Headers N×64B] [Shard 0 Data] [Shard 1 Data] ...
 *
 * Per-shard data:
 *   [Bucket Array (CL-aligned)] [Entry Array (128B each)]
 *
 * Each entry is 128B (2 CL):
 *   CL0 (bytes 0-63):  hot path — state, next, hash, offset, region_id
 *   CL1 (bytes 64-127): cold path — name (accessed only on hash match)
 *
 * Entry state machine (same as global index):
 *   EMPTY --CAS--> INSERTING --WMB--> VALID --CAS--> TOMBSTONE
 *
 * Stale INSERTING detection: node_id + created_at timeout.
 * Same-node or uninitialized (node==0) entries are reclaimed after timeout.
 * Other nodes' entries are skipped — their GC handles them.
 */

#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/log2.h>
#include <linux/prefetch.h>
#include <linux/sched.h>
#include <linux/string.h>
#include <linux/types.h>

#include "marufs.h"

/* ============================================================================
 * Shard view — transient CXL pointer set for one shard
 * ============================================================================ */

struct nrht_shard_ctx {
	struct marufs_nrht_shard_header *header;
	struct marufs_nrht_entry *entries;
	u32 *bucket_head;
	u32 num_entries;
	u32 shard_id; /* needed for ME acquire/release */
};

/* ============================================================================
 * Low-level helpers
 * ============================================================================ */

static inline struct marufs_nrht_shard_header *
nrht_shard_header(void *nrht_base, u32 shard_id)
{
	struct marufs_nrht_shard_header *sh =
		(struct marufs_nrht_shard_header
			 *)((char *)nrht_base +
			    sizeof(struct marufs_nrht_header) +
			    (u64)shard_id * sizeof(*sh));
	MARUFS_CXL_RMB(sh, sizeof(*sh));
	return sh;
}

/*
 * nrht_name_matches - check if entry matches (hash, name).
 * Caller must have issued RMB on both CL0 and CL1 before calling.
 */
static inline bool nrht_name_matches(struct marufs_nrht_entry *e, u64 hash,
				     const char *name, size_t namelen)
{
	MARUFS_CXL_RMB(e->name, sizeof(e->name));
	if (READ_CXL_LE64(e->name_hash) != hash)
		return false;
	if (strncmp(e->name, name, namelen) != 0)
		return false;
	if (namelen < sizeof(e->name) && e->name[namelen] != '\0')
		return false;
	return true;
}

/*
 * 1 - pure staleness check for INSERTING entry.
 *
 * Only handles the clear-cut case: same-node + created_at > 0 + timeout.
 * No side effects — caller handles CAS reclaim and chain unlink.
 * Indeterminate cases (node==0, created_at==0) return 0; the GC sweep
 * in marufs_nrht_gc_sweep_all() tracks them via marufs_gc_track_orphan().
 *
 * Returns:
 *   1  = stale, caller should reclaim
 *   0  = not stale or indeterminate (orphan/timestamp not visible)
 *  -1  = not this node's entry
 */
static int nrht_is_stale(struct marufs_sb_info *sbi,
			 struct marufs_nrht_entry *e)
{
	u32 ins_node = READ_CXL_LE32(e->inserter_node);
	if (ins_node == 0)
		return (sbi->node_id == 1) ?
			       0 :
			       -1; /* Only admin node tracks orphans */

	if (ins_node != sbi->node_id)
		return -1;

	u64 created_at = READ_CXL_LE64(e->created_at);
	if (created_at == 0)
		return 0; /* timestamp not yet visible — tracked by GC */

	u64 now = ktime_get_real_ns();
	if (now <= created_at || (now - created_at) <= MARUFS_STALE_TIMEOUT_NS)
		return 0;

	return 1;
}

/* CAS current state → INSERTING and stamp inserter identity for GC. */
static inline bool nrht_claim_entry(struct marufs_sb_info *sbi,
				    struct marufs_nrht_entry *e)
{
	u32 st = READ_LE32(e->state);
	if (st != MARUFS_ENTRY_EMPTY && st != MARUFS_ENTRY_TOMBSTONE)
		return false;
	if (marufs_le32_cas(&e->state, st, MARUFS_ENTRY_INSERTING) != st)
		return false;

	WRITE_LE64(e->created_at, ktime_get_real_ns());
	WRITE_LE32(e->inserter_node, sbi->node_id);
	return true;
}

/* ============================================================================
 * Header / shard resolution
 * ============================================================================ */

static struct marufs_nrht_header *nrht_get_header(struct marufs_sb_info *sbi,
						  u32 nrht_region_id)
{
	struct marufs_rat_entry *rat_e =
		marufs_rat_entry_get(sbi, nrht_region_id);
	if (!rat_e)
		return NULL;

	if (READ_CXL_LE32(rat_e->state) != MARUFS_RAT_ENTRY_ALLOCATED)
		return NULL;

	u64 phys_offset = READ_CXL_LE64(rat_e->phys_offset);
	u64 region_size = READ_CXL_LE64(rat_e->size);
	struct marufs_nrht_header *hdr = marufs_dax_ptr(sbi, phys_offset);
	if (phys_offset == 0 || region_size < sizeof(*hdr) || !hdr ||
	    !marufs_dax_range_valid(sbi, phys_offset, sizeof(*hdr)))
		return NULL;

	MARUFS_CXL_RMB(hdr, sizeof(*hdr));
	if (READ_CXL_LE32(hdr->magic) != MARUFS_NRHT_MAGIC ||
	    READ_CXL_LE32(hdr->version) != MARUFS_NRHT_VERSION ||
	    READ_CXL_LE64(hdr->table_size) > region_size) {
		pr_err("nrht: invalid header (magic=0x%x ver=%u table_size=%llu region=%llu)\n",
		       READ_CXL_LE32(hdr->magic), READ_CXL_LE32(hdr->version),
		       READ_CXL_LE64(hdr->table_size), region_size);
		return NULL;
	}

	return hdr;
}

static int nrht_get_shard_ctx(struct marufs_sb_info *sbi,
			      struct marufs_nrht_header *nrht, u32 shard_id,
			      struct nrht_shard_ctx *out)
{
	struct marufs_nrht_shard_header *sh = nrht_shard_header(nrht, shard_id);

	out->header = sh;
	out->num_entries = READ_CXL_LE32(sh->num_entries);

	u32 num_buckets = READ_CXL_LE32(sh->num_buckets);
	if (num_buckets == 0 || out->num_entries == 0 ||
	    out->num_entries >
		    MARUFS_NRHT_MAX_ENTRIES * MARUFS_NRHT_MAX_NUM_SHARDS) {
		pr_err("nrht: shard %u invalid (buckets=%u entries=%u)\n",
		       shard_id, num_buckets, out->num_entries);
		return -EIO;
	}

	u64 entry_off = READ_CXL_LE64(sh->entry_array_offset);
	u64 entry_size =
		(u64)out->num_entries * sizeof(struct marufs_nrht_entry);

	if (entry_off == 0 ||
	    !marufs_dax_range_valid(sbi, entry_off, entry_size)) {
		pr_err("nrht: shard %u invalid entry offset 0x%llx\n", shard_id,
		       entry_off);
		return -EIO;
	}

	out->entries = marufs_dax_ptr(sbi, entry_off);
	return 0;
}

/*
 * nrht_resolve_bucket - common prologue for all NRHT operations.
 * Resolves header, selects shard, populates ctx including bucket_head.
 * Auto-computes hash if name_hash is 0.
 */
static int nrht_resolve_bucket(struct marufs_sb_info *sbi, u32 nrht_region_id,
			       const char *name, size_t namelen, u64 *name_hash,
			       struct nrht_shard_ctx *ctx)
{
	if (*name_hash == 0)
		*name_hash = marufs_hash_name(name, namelen);

	struct marufs_nrht_header *nrht = nrht_get_header(sbi, nrht_region_id);
	if (!nrht)
		return -ENOENT;

	u32 shard_mask = READ_CXL_LE32(nrht->num_shards) - 1;
	u32 shard_id = marufs_shard_idx(*name_hash, shard_mask);
	int ret = nrht_get_shard_ctx(sbi, nrht, shard_id, ctx);
	if (ret)
		return ret;
	ctx->shard_id = shard_id;

	/* Resolve bucket head pointer */
	u32 num_buckets = READ_CXL_LE32(ctx->header->num_buckets);
	u64 bucket_off = READ_CXL_LE64(ctx->header->bucket_array_offset);
	u64 bucket_size = marufs_align_up((u64)num_buckets * sizeof(u32), 64);
	if (bucket_off == 0 ||
	    !marufs_dax_range_valid(sbi, bucket_off, bucket_size))
		return -EIO;

	u32 *buckets = marufs_dax_ptr(sbi, bucket_off);
	u32 bucket_idx = marufs_bucket_idx(*name_hash, num_buckets - 1);
	ctx->bucket_head = &buckets[bucket_idx];

	return 0;
}

/* ============================================================================
 * Chain walk operations
 * ============================================================================ */

/*
 * nrht_check_duplicate - walk bucket chain, check for duplicate name.
 *
 * Records first dead entry (TOMBSTONE or EMPTY) as in-place reuse candidate.
 * Additional dead entries are inline-unlinked and reclaimed to EMPTY.
 * Consistent with index.c check_duplicate pattern.
 *
 * @reuse_idx: output — first reusable entry index, or BUCKET_END.
 * Return: 0 if no duplicate, -EEXIST if exists, -EIO on chain cycle.
 */
static int nrht_check_duplicate(struct marufs_sb_info *sbi,
				struct nrht_shard_ctx *ctx, u64 hash,
				const char *name, size_t namelen,
				u32 *reuse_idx)
{
	u32 *prev_next = ctx->bucket_head;
	MARUFS_CXL_RMB(prev_next, sizeof(*prev_next));
	u32 cur = READ_CXL_LE32(*prev_next);
	*reuse_idx = MARUFS_BUCKET_END;
	u32 steps = 0;

	while (cur != MARUFS_BUCKET_END && cur < ctx->num_entries) {
		if (++steps > ctx->num_entries) {
			pr_err("nrht: chain cycle detected\n");
			return -EIO;
		}

		struct marufs_nrht_entry *e = &ctx->entries[cur];
		MARUFS_CXL_RMB(e, 64);
		u32 st = READ_CXL_LE32(e->state);
		u32 next = READ_CXL_LE32(e->next_in_bucket);

		/* Dead entry (TOMBSTONE or EMPTY): reuse first, unlink rest */
		if (st == MARUFS_ENTRY_TOMBSTONE || st == MARUFS_ENTRY_EMPTY) {
			if (*reuse_idx == MARUFS_BUCKET_END) {
				*reuse_idx = cur;
				prev_next = (u32 *)&e->next_in_bucket;
			} else {
				if (marufs_le32_cas(prev_next, cur, next) ==
				    cur)
					marufs_le32_cas(&e->state, st,
							MARUFS_ENTRY_EMPTY);
			}
			cur = next;
			continue;
		}

		if (st == MARUFS_ENTRY_VALID &&
		    READ_CXL_LE64(e->name_hash) == hash &&
		    nrht_name_matches(e, hash, name, namelen))
			return -EEXIST;

		prev_next = (u32 *)&e->next_in_bucket;
		cur = next;
	}

	return 0;
}

/*
 * nrht_find_chain - walk bucket chain to find a VALID entry by name.
 * Returns entry pointer and sets *out_idx, *out_prev_next. NULL if not found.
 */
static struct marufs_nrht_entry *nrht_find_chain(struct nrht_shard_ctx *ctx,
						 u64 hash, const char *name,
						 size_t namelen, u32 *out_idx,
						 u32 **out_prev_next)
{
	u32 *prev_next = ctx->bucket_head;
	MARUFS_CXL_RMB(prev_next, sizeof(*prev_next));
	u32 cur = READ_CXL_LE32(*prev_next);
	u32 steps = 0;

	while (cur != MARUFS_BUCKET_END && cur < ctx->num_entries) {
		if (++steps > ctx->num_entries) {
			pr_err("nrht: chain cycle detected\n");
			return NULL;
		}

		struct marufs_nrht_entry *e = &ctx->entries[cur];
		MARUFS_CXL_RMB(e, 64);
		u32 next = READ_CXL_LE32(e->next_in_bucket);
		u32 state = READ_CXL_LE32(e->state);

		if (next != MARUFS_BUCKET_END && next < ctx->num_entries)
			prefetch(&ctx->entries[next]);

		if (state == MARUFS_ENTRY_VALID) {
			if (nrht_name_matches(e, hash, name, namelen)) {
				if (out_idx)
					*out_idx = cur;
				if (out_prev_next)
					*out_prev_next = prev_next;
				return e;
			}
		}

		prev_next = (u32 *)&e->next_in_bucket;
		cur = next;
	}

	return NULL;
}

/* ============================================================================
 * Slot acquisition + link + publish
 * ============================================================================ */

/*
 * marufs_nrht_me_get - lazily create/return per-NRHT ME instance for this node.
 * The first access by this node creates the instance, joins membership,
 * and registers with the sbi-level poll thread.
 */
static struct marufs_me_instance *marufs_nrht_me_get(struct marufs_sb_info *sbi,
						     u32 nrht_region_id)
{
	struct marufs_me_instance *nme;
	struct marufs_rat_entry *rat_e;
	void *base;
	struct marufs_nrht_header *hdr;
	void *me_base;
	u32 num_shards;
	int ret;

	if (nrht_region_id >= MARUFS_MAX_RAT_ENTRIES)
		return ERR_PTR(-EINVAL);

	/* Fast path: already initialized AND CXL format generation matches.
	 * If the underlying CXL ME area has been reformatted (e.g. a peer
	 * mount re-ran nrht_init on a recycled RAT slot), our cached
	 * instance is stale and must be dropped so we re-join the fresh ring.
	 */
	nme = READ_ONCE(sbi->nrht_me[nrht_region_id]);
	if (nme) {
		struct marufs_me_header *h = nme->header;
		MARUFS_CXL_RMB(h, sizeof(*h));
		if (READ_CXL_LE64(h->format_generation) ==
		    nme->cached_generation)
			return nme;
	}

	mutex_lock(&sbi->nrht_me_lock);
	nme = sbi->nrht_me[nrht_region_id];
	if (nme) {
		struct marufs_me_header *h = nme->header;
		MARUFS_CXL_RMB(h, sizeof(*h));
		if (READ_CXL_LE64(h->format_generation) ==
		    nme->cached_generation) {
			mutex_unlock(&sbi->nrht_me_lock);
			return nme;
		}
		/* Stale: drop it and fall through to recreate. */
		pr_info("nrht: ME instance for region %u stale (gen changed), reinit\n",
			nrht_region_id);
		sbi->nrht_me[nrht_region_id] = NULL;
		marufs_me_invalidate(sbi, nme);
		nme = NULL;
	}

	/* Resolve NRHT base + ME area */
	rat_e = marufs_rat_entry_get(sbi, nrht_region_id);
	if (!rat_e ||
	    READ_CXL_LE32(rat_e->state) != MARUFS_RAT_ENTRY_ALLOCATED) {
		mutex_unlock(&sbi->nrht_me_lock);
		return ERR_PTR(-ENOENT);
	}
	base = marufs_dax_ptr(sbi, READ_CXL_LE64(rat_e->phys_offset));
	if (!base) {
		mutex_unlock(&sbi->nrht_me_lock);
		return ERR_PTR(-EIO);
	}
	hdr = base;
	MARUFS_CXL_RMB(hdr, sizeof(*hdr));
	num_shards = READ_CXL_LE32(hdr->num_shards);
	me_base = (char *)base + sizeof(struct marufs_nrht_header) +
		  (u64)num_shards * sizeof(struct marufs_nrht_shard_header);

	/* Read strategy from ME header (formatted during nrht_init) */
	struct marufs_me_header *me_hdr = me_base;
	MARUFS_CXL_RMB(me_hdr, sizeof(*me_hdr));
	enum marufs_me_strategy strat = READ_CXL_LE32(me_hdr->strategy);

	nme = marufs_me_create(me_base, num_shards, MARUFS_ME_MAX_NODES,
			       sbi->node_id, MARUFS_ME_DEFAULT_POLL_US, strat);
	if (IS_ERR(nme)) {
		mutex_unlock(&sbi->nrht_me_lock);
		return nme;
	}

	ret = nme->ops->join(nme);
	if (ret) {
		marufs_me_destroy(nme);
		mutex_unlock(&sbi->nrht_me_lock);
		return ERR_PTR(ret);
	}

	marufs_me_register(sbi, nme);

	/* Publish via WRITE_ONCE; fast path uses READ_ONCE */
	WRITE_ONCE(sbi->nrht_me[nrht_region_id], nme);
	mutex_unlock(&sbi->nrht_me_lock);

	pr_info("nrht: ME instance created for region %u (strategy=%s, shards=%u)\n",
		nrht_region_id, strat == MARUFS_ME_ORDER ? "order" : "request",
		num_shards);
	return nme;
}

/*
 * Caller MUST hold the NRHT ME shard lock — no other mutator can touch
 * bucket_head concurrently:
 *   - link_to_bucket / check_duplicate unlink / post_insert_dedup: all under
 *     the same ME shard lock in the insert path (this thread).
 *   - lookup: read-only on bucket chain.
 *   - delete: only CAS-es entry->state, does not touch bucket_head / next_in_bucket.
 *   - gc: does not touch bucket structure.
 * So the CAS on bucket_head can be a plain WRITE.
 */
static int nrht_link_to_bucket(struct marufs_nrht_entry *entry, u32 entry_idx,
			       u32 *bucket_head)
{
	MARUFS_CXL_RMB(bucket_head, sizeof(*bucket_head));
	u32 old_head = READ_CXL_LE32(*bucket_head);

	if (unlikely(old_head == entry_idx)) {
		/* Already the bucket head (stale link from previous life).
		 * Chain successor is intact — just skip linking. */
		return 0;
	}

	WRITE_LE32(entry->next_in_bucket, old_head);
	MARUFS_CXL_WMB(entry, 64);

	WRITE_LE32(*bucket_head, entry_idx);
	MARUFS_CXL_WMB(bucket_head, sizeof(*bucket_head));
	return 0;
}

/*
 * nrht_post_insert_dedup - detect duplicate under shard lock.
 * Called after link_to_bucket while holding the shard lock.  Walks the
 * bucket chain looking for another VALID entry with the same name.
 * If found, returns -EEXIST so the caller can tombstone and release.
 */
static int nrht_post_insert_dedup(struct nrht_shard_ctx *ctx,
				  struct marufs_nrht_entry *entry,
				  u32 entry_idx, u64 hash, const char *name,
				  size_t namelen)
{
	u32 *head = ctx->bucket_head;
	MARUFS_CXL_RMB(head, sizeof(*head));
	u32 cur = READ_CXL_LE32(*head);
	u32 steps = 0;

	while (cur != MARUFS_BUCKET_END && cur < ctx->num_entries) {
		if (++steps > ctx->num_entries) {
			pr_err("nrht: chain cycle detected\n");
			return -EIO;
		}

		struct marufs_nrht_entry *e = &ctx->entries[cur];
		MARUFS_CXL_RMB(e, 64);
		u32 next = READ_CXL_LE32(e->next_in_bucket);

		if (entry_idx == cur) {
			cur = next;
			continue;
		}

		if (next != MARUFS_BUCKET_END && next < ctx->num_entries)
			prefetch(&ctx->entries[next]);

		u32 state = READ_CXL_LE32(e->state);
		if (state == MARUFS_ENTRY_VALID &&
		    nrht_name_matches(e, hash, name, namelen)) {
			return -EEXIST;
		}

		cur = next;
	}

	return 0;
}

/* ============================================================================
 * Public API
 * ============================================================================ */
int marufs_nrht_init(struct marufs_sb_info *sbi, u32 nrht_region_id,
		     u32 max_entries, u32 num_shards, u32 num_buckets,
		     enum marufs_me_strategy me_strategy)
{
	struct marufs_rat_entry *rat_e =
		marufs_rat_entry_get(sbi, nrht_region_id);
	if (!rat_e)
		return -ENOENT;
	if (READ_CXL_LE32(rat_e->state) != MARUFS_RAT_ENTRY_ALLOCATED)
		return -EINVAL;

	/* ── Step 1: validate params + compute total size ────────────── */
	if (num_shards == 0)
		num_shards = MARUFS_NRHT_DEFAULT_NUM_SHARDS;
	if (max_entries == 0)
		max_entries = MARUFS_NRHT_DEFAULT_NUM_SHARDS *
			      MARUFS_NRHT_DEFAULT_ENTRIES;

	if (!is_power_of_2(num_shards) ||
	    (num_shards > MARUFS_NRHT_MAX_NUM_SHARDS))
		return -EINVAL;

	if (!is_power_of_2(max_entries) ||
	    (max_entries >
	     MARUFS_NRHT_MAX_ENTRIES * MARUFS_NRHT_MAX_NUM_SHARDS)) {
		pr_err("nrht_init: max_entries %u exceeds limit\n",
		       max_entries);
		return -EINVAL;
	}

	u32 entries_per_shard = max_entries / num_shards;
	if (entries_per_shard == 0)
		return -EINVAL;

	u32 buckets_per_shard;
	if (num_buckets != 0) {
		buckets_per_shard = num_buckets / num_shards;
		if (buckets_per_shard == 0)
			buckets_per_shard = 1;
	} else {
		buckets_per_shard =
			entries_per_shard / MARUFS_NRHT_DEFAULT_LOAD_FACTOR;
		if (buckets_per_shard == 0)
			buckets_per_shard = 1;
	}
	if (buckets_per_shard > MARUFS_NRHT_MAX_ENTRIES)
		return -EINVAL;
	buckets_per_shard = roundup_pow_of_two(buckets_per_shard);
	if (buckets_per_shard == 0 ||
	    buckets_per_shard > MARUFS_NRHT_MAX_ENTRIES)
		return -EINVAL;

	u64 bucket_array_size =
		marufs_align_up((u64)buckets_per_shard * sizeof(u32), 64);
	u64 per_shard_size =
		bucket_array_size +
		(u64)entries_per_shard * sizeof(struct marufs_nrht_entry);
	u64 shard_headers_end =
		sizeof(struct marufs_nrht_header) +
		(u64)num_shards * sizeof(struct marufs_nrht_shard_header);

	/* NRHT ME area placed between shard headers and shard data.
	 * Always sized as REQUEST to support both strategies without re-format.
	 */
	u64 me_area_size = marufs_me_area_size(num_shards, MARUFS_ME_MAX_NODES);
	u64 me_area_offset = shard_headers_end;
	u64 shard_data_start = me_area_offset + me_area_size;

	u64 total_needed = shard_data_start + (u64)num_shards * per_shard_size;

	/* ── Step 2: allocate physical memory if not yet done ────────── */
	u64 phys_offset = READ_CXL_LE64(rat_e->phys_offset);
	u64 region_size = READ_CXL_LE64(rat_e->size);
	bool freshly_allocated = false;
	if (phys_offset == 0 || region_size == 0) {
		/* No ftruncate — allocate from init directly */
		int ret = marufs_region_init(sbi, nrht_region_id, total_needed);
		if (ret) {
			pr_err("nrht_init: region_init failed: %d\n", ret);
			return ret;
		}

		/* Re-read RAT entry after allocation */
		rat_e = marufs_rat_entry_get(sbi, nrht_region_id);
		if (!rat_e)
			return -EIO;

		phys_offset = READ_CXL_LE64(rat_e->phys_offset);
		region_size = READ_CXL_LE64(rat_e->size);
		freshly_allocated = true;
	}

	if (total_needed > region_size) {
		pr_err("nrht_init: need %llu bytes, have %llu\n", total_needed,
		       region_size);
		return -ENOSPC;
	}

	/* Register in DRAM bitmap for fast GC enumeration */
	set_bit(nrht_region_id, sbi->gc_nrht_bitmap);

	/* ── Step 3: resolve base pointer + double-init check ────────── */
	if (!marufs_dax_range_valid(sbi, phys_offset, region_size))
		return -EINVAL;

	void *base = marufs_dax_ptr(sbi, phys_offset);
	if (!base)
		return -EINVAL;

	/* Double-init protection: check the RAT entry's region_type rather than
	 * probing physical data for magic.  rat_entry_reset() zeroes region_type
	 * to MARUFS_REGION_DATA on every alloc/free, so REGION_NRHT here means
	 * nrht_init already ran on THIS lifecycle — genuine double-init.
	 * Stale NRHT magic in recycled CXL physical space is safely ignored.
	 * NOTE: region_type is set to NRHT *after* format (below), so this
	 * check only triggers on a second call, not the first. */
	if (!freshly_allocated &&
	    READ_CXL_LE32(rat_e->region_type) == MARUFS_REGION_NRHT) {
		pr_err("nrht_init: region %u already formatted\n",
		       nrht_region_id);
		return -EEXIST;
	}

	/* Invalidate any stale per-node ME instance cached for this rat_id.
	 * Happens when a RAT entry is freed and later reallocated for a new
	 * NRHT file — without this, sbi->nrht_me[rat_id] points to a stale
	 * instance whose DRAM state (holding, heartbeat, cached_successor)
	 * does not match the freshly-formatted CXL area.
	 */
	mutex_lock(&sbi->nrht_me_lock);
	{
		struct marufs_me_instance *stale = sbi->nrht_me[nrht_region_id];
		sbi->nrht_me[nrht_region_id] = NULL;
		marufs_me_invalidate(sbi, stale);
	}
	mutex_unlock(&sbi->nrht_me_lock);

	/* Zero + format */
	memset(base, 0, (size_t)total_needed);
	MARUFS_CXL_WMB(base, total_needed);

	struct marufs_nrht_header *hdr = base;
	WRITE_LE32(hdr->magic, MARUFS_NRHT_MAGIC);
	WRITE_LE32(hdr->version, MARUFS_NRHT_VERSION);
	WRITE_LE32(hdr->num_shards, num_shards);
	WRITE_LE32(hdr->buckets_per_shard, buckets_per_shard);
	WRITE_LE32(hdr->entries_per_shard, entries_per_shard);
	WRITE_LE32(hdr->owner_region_id, nrht_region_id);
	WRITE_LE64(hdr->table_size, total_needed);
	MARUFS_CXL_WMB(hdr, sizeof(*hdr));

	/* Format NRHT ME area (between shard headers and shard data) */
	int me_ret = marufs_me_format((char *)base + me_area_offset, num_shards,
				      MARUFS_ME_MAX_NODES,
				      MARUFS_ME_DEFAULT_POLL_US, me_strategy);
	if (me_ret) {
		pr_err("nrht_init: me_format failed: %d\n", me_ret);
		return me_ret;
	}

	/* Make the initiator the first holder and ACTIVE member
		 * atomically from any peer's perspective — peers that race
		 * through me_get will see a non-empty ring and will not
		 * self-elect as first_node. This removes the start-up window
		 * where holder=NONE would be visible.
		 */
	char *me_base = (char *)base + me_area_offset;
	struct marufs_me_header *mh = (struct marufs_me_header *)me_base;
	MARUFS_CXL_RMB(mh, sizeof(*mh));
	u64 cb_off = READ_CXL_LE64(mh->cb_array_offset);
	u64 mem_off = READ_CXL_LE64(mh->membership_offset);
	u64 slot_off = READ_CXL_LE64(mh->request_offset);
	u32 mnodes = READ_CXL_LE32(mh->max_nodes);

	for (u32 s = 0; s < num_shards; s++) {
		struct marufs_me_cb *cb = marufs_me_cb_at(me_base, cb_off, s);
		u64 new_gen = READ_CXL_LE64(cb->generation) + 1;
		WRITE_LE32(cb->holder, sbi->node_id);
		WRITE_LE64(cb->generation, new_gen);
		MARUFS_CXL_WMB(cb, sizeof(*cb));

		/* Ring the initiator's doorbell so the first acquire's
		 * wait_for_token fast path sees a fresh seq + generation.
		 */
		struct marufs_me_slot *ms = marufs_me_slot_at(
			me_base, slot_off, mnodes, s, sbi->node_id - 1);
		WRITE_LE32(ms->from_node, sbi->node_id);
		WRITE_LE64(ms->cb_gen_at_write, new_gen);
		WRITE_LE64(ms->token_seq, READ_CXL_LE64(ms->token_seq) + 1);
		MARUFS_CXL_WMB(ms, sizeof(*ms));
	}
	/* slot[i] is for external node_id (i+1); index by (node_id - 1). */
	struct marufs_me_membership_slot *my_slot =
		marufs_me_membership_at(me_base, mem_off, sbi->node_id - 1);
	WRITE_LE32(my_slot->status, MARUFS_ME_ACTIVE);
	WRITE_LE32(my_slot->node_id, sbi->node_id);
	WRITE_LE64(my_slot->joined_at, ktime_get_ns());
	WRITE_LE64(my_slot->heartbeat, 0);
	WRITE_LE64(my_slot->heartbeat_ts, ktime_get_ns());
	MARUFS_CXL_WMB(my_slot, sizeof(*my_slot));

	u64 offset = shard_data_start;
	for (u32 s = 0; s < num_shards; s++) {
		struct marufs_nrht_shard_header *sh =
			nrht_shard_header(base, s);
		if (!sh)
			return -EINVAL;

		u64 shard_bucket_off = phys_offset + offset;
		u64 shard_entry_off = shard_bucket_off + bucket_array_size;

		WRITE_LE32(sh->num_entries, entries_per_shard);
		WRITE_LE32(sh->num_buckets, buckets_per_shard);
		WRITE_LE64(sh->bucket_array_offset, shard_bucket_off);
		WRITE_LE64(sh->entry_array_offset, shard_entry_off);
		MARUFS_CXL_WMB(sh, sizeof(*sh));

		u32 *bkts = (u32 *)((char *)base + offset);
		for (u32 i = 0; i < buckets_per_shard; i++)
			WRITE_LE32(bkts[i], MARUFS_BUCKET_END);
		MARUFS_CXL_WMB(bkts, bucket_array_size);

		offset += per_shard_size;
	}

	/* Tag region as NRHT for GC discovery and double-init protection.
	 * Must be AFTER format so the double-init check above doesn't
	 * see our own write on the first call. */
	WRITE_LE32(rat_e->region_type, MARUFS_REGION_NRHT);
	MARUFS_CXL_WMB(rat_e, 64);

	pr_info("nrht_init: region %u, %u shards, %u entries/shard, %u buckets/shard, %llu bytes\n",
		nrht_region_id, num_shards, entries_per_shard,
		buckets_per_shard, total_needed);

	/* Join the initiating node to the freshly-formatted ring so the
	 * first insert (on any node) has at least one ACTIVE member and
	 * the initial token holder is a real node_id — not left as NONE.
	 * Without this, insert from a peer mount would race through
	 * first_node detection on an empty ring, which on a very short
	 * retry window can miss a concurrent joiner. Also ensures crash
	 * recovery (next_active) has a candidate.
	 */
	{
		struct marufs_me_instance *nme =
			marufs_nrht_me_get(sbi, nrht_region_id);
		if (IS_ERR(nme))
			pr_warn("nrht_init: initiator join failed for region %u: %ld\n",
				nrht_region_id, PTR_ERR(nme));
	}

	return 0;
}

int marufs_nrht_join(struct marufs_sb_info *sbi, u32 nrht_region_id)
{
	struct marufs_me_instance *nme =
		marufs_nrht_me_get(sbi, nrht_region_id);

	return IS_ERR(nme) ? PTR_ERR(nme) : 0;
}

int marufs_nrht_insert(struct marufs_sb_info *sbi, u32 nrht_region_id,
		       const char *name, size_t namelen, u64 name_hash,
		       u64 offset, u32 target_region_id)
{
	if (!sbi || !name)
		return -EINVAL;
	if (namelen == 0 || namelen > MARUFS_NAME_MAX)
		return -ENAMETOOLONG;

	/* Validate target region */
	struct marufs_rat_entry *tr =
		marufs_rat_entry_get(sbi, target_region_id);
	if (!tr || READ_CXL_LE32(tr->state) != MARUFS_RAT_ENTRY_ALLOCATED)
		return -EINVAL;

	u64 target_size = READ_CXL_LE64(tr->size);
	if (target_size > 0 && offset >= target_size)
		return -EINVAL;

	struct nrht_shard_ctx ctx;
	int ret = nrht_resolve_bucket(sbi, nrht_region_id, name, namelen,
				      &name_hash, &ctx);
	if (ret)
		return ret;

	/* Step 1: duplicate check (also inline-unlinks TOMBSTONE, records EMPTY reuse) */
	u32 chain_reuse_idx = MARUFS_BUCKET_END;
	ret = nrht_check_duplicate(sbi, &ctx, name_hash, name, namelen,
				   &chain_reuse_idx);
	if (ret)
		return ret;

	/* Step 2a: reuse dead chain entry — TOMBSTONE or EMPTY (skip flat scan + link) */
	bool reused_chain_entry = false;
	u32 entry_idx;
	struct marufs_nrht_entry *entry = NULL;
	if (chain_reuse_idx != MARUFS_BUCKET_END) {
		struct marufs_nrht_entry *e = &ctx.entries[chain_reuse_idx];
		if (nrht_claim_entry(sbi, e)) {
			entry_idx = chain_reuse_idx;
			entry = e;
			reused_chain_entry = true;
		}
	}

	/* Step 2b: flat scan for EMPTY (start from free_hint) */
	if (!entry) {
		MARUFS_CXL_RMB(&ctx.header->free_hint,
			       sizeof(ctx.header->free_hint));
		u32 hint = READ_CXL_LE32(ctx.header->free_hint);
		if (hint >= ctx.num_entries)
			hint = 0;

		for (u32 scan = 0; scan < ctx.num_entries; scan++) {
			u32 idx = hint + scan;
			if (idx >= ctx.num_entries)
				idx -= ctx.num_entries;

			struct marufs_nrht_entry *e = &ctx.entries[idx];
			MARUFS_CXL_RMB(e, 64);

			if (READ_CXL_LE32(e->state) != MARUFS_ENTRY_EMPTY)
				continue;

			if (nrht_claim_entry(sbi, e)) {
				entry_idx = idx;
				entry = e;
				WRITE_LE32(e->next_in_bucket,
					   MARUFS_BUCKET_END);
				/* Advance hint past claimed entry */
				u32 next_hint = idx + 1;
				if (next_hint >= ctx.num_entries)
					next_hint = 0;
				WRITE_LE32(ctx.header->free_hint, next_hint);
				MARUFS_CXL_WMB(&ctx.header->free_hint,
					       sizeof(ctx.header->free_hint));
				break;
			}
		}
	}

	if (!entry)
		return -ENOSPC;

	/* Step 3: fill fields (both CL0 and CL1) */
	WRITE_LE64(entry->name_hash, name_hash);
	WRITE_LE64(entry->offset, offset);
	WRITE_LE32(entry->target_region_id, target_region_id);
	WRITE_LE32(entry->inserter_node, sbi->node_id);
	WRITE_LE64(entry->created_at, ktime_get_real_ns());

	size_t copy_len = min(namelen, sizeof(entry->name) - 1);
	memset(entry->name, 0, sizeof(entry->name));
	memcpy(entry->name, name, copy_len);

	MARUFS_CXL_WMB(entry, sizeof(*entry)); /* flush both CL0 + CL1 */

	/* Step 4: INSERTING → TENTATIVE (visible in chain but not yet VALID) */
	WRITE_LE32(entry->state, MARUFS_ENTRY_TENTATIVE);
	MARUFS_CXL_WMB(entry, 64);

	/* Step 5: acquire NRHT ME token — cross-node shard lock (FT via heartbeat) */
	struct marufs_me_instance *nme =
		marufs_nrht_me_get(sbi, nrht_region_id);
	if (IS_ERR(nme)) {
		WRITE_LE32(entry->state, MARUFS_ENTRY_TOMBSTONE);
		MARUFS_CXL_WMB(entry, 64);
		return PTR_ERR(nme);
	}
	ret = nme->ops->acquire(nme, ctx.shard_id);
	if (ret) {
		WRITE_LE32(entry->state, MARUFS_ENTRY_TOMBSTONE);
		MARUFS_CXL_WMB(entry, 64);
		return ret;
	}

	/* Step 6: link to bucket (skip if reused — already in chain) */
	if (!reused_chain_entry) {
		ret = nrht_link_to_bucket(entry, entry_idx, ctx.bucket_head);
		if (ret) {
			WRITE_LE32(entry->state, MARUFS_ENTRY_TOMBSTONE);
			MARUFS_CXL_WMB(entry, 64);
			goto unlock;
		}
	}

	/* Step 7: post-insert dedup — concurrent inserts resolved here */
	ret = nrht_post_insert_dedup(&ctx, entry, entry_idx, name_hash, name,
				     namelen);
	if (ret) {
		WRITE_LE32(entry->state, MARUFS_ENTRY_TOMBSTONE);
		MARUFS_CXL_WMB(entry, 64);
		goto unlock;
	}

	/* Step 8: TENTATIVE → VALID (entry is now queryable) */
	WRITE_LE32(entry->state, MARUFS_ENTRY_VALID);
	MARUFS_CXL_WMB(entry, 64);

unlock:
	nme->ops->release(nme, ctx.shard_id);
	return ret;
}

int marufs_nrht_lookup(struct marufs_sb_info *sbi, u32 nrht_region_id,
		       const char *name, size_t namelen, u64 name_hash,
		       u64 *out_offset, u32 *out_target_region_id)
{
	if (!sbi || !name || !out_offset || !out_target_region_id)
		return -EINVAL;
	if (namelen == 0 || namelen > MARUFS_NAME_MAX)
		return -ENOENT;

	struct nrht_shard_ctx ctx;
	int ret = nrht_resolve_bucket(sbi, nrht_region_id, name, namelen,
				      &name_hash, &ctx);
	if (ret)
		return ret;

	struct marufs_nrht_entry *e =
		nrht_find_chain(&ctx, name_hash, name, namelen, NULL, NULL);
	if (!e)
		return -ENOENT;

	*out_offset = READ_CXL_LE64(e->offset);
	*out_target_region_id = READ_CXL_LE32(e->target_region_id);
	return 0;
}

int marufs_nrht_delete(struct marufs_sb_info *sbi, u32 nrht_region_id,
		       const char *name, size_t namelen, u64 name_hash)
{
	if (!sbi || !name)
		return -EINVAL;
	if (namelen == 0 || namelen > MARUFS_NAME_MAX)
		return -ENOENT;

	struct nrht_shard_ctx ctx;
	int ret = nrht_resolve_bucket(sbi, nrht_region_id, name, namelen,
				      &name_hash, &ctx);
	if (ret)
		return ret;

	u32 entry_idx;
	struct marufs_nrht_entry *e = nrht_find_chain(
		&ctx, name_hash, name, namelen, &entry_idx, NULL);
	if (!e)
		return -ENOENT;

	/* CAS VALID → TOMBSTONE (logical delete, stays in chain for reuse) */
	if (marufs_le32_cas(&e->state, MARUFS_ENTRY_VALID,
			    MARUFS_ENTRY_TOMBSTONE) != MARUFS_ENTRY_VALID)
		return -ENOENT;
	/* CAS includes implicit full barrier — no extra WMB needed */
	pr_debug("nrht: deleted '%.*s'\n", (int)namelen, name);
	return 0;
}

/* ============================================================================
 * GC support — called from gc.c Phase 4
 * ============================================================================ */

/*
 * marufs_nrht_gc_sweep_all - sweep stale INSERTING entries across all NRHT regions
 * @sbi: superblock info
 *
 * Iterates DRAM bitmap of NRHT regions, sweeping ~25% of shards per cycle
 * (round-robin via gc_epoch). Same-node stale entries are reclaimed to EMPTY;
 * indeterminate cases (node==0, created_at==0) are tracked in the DRAM
 * orphan array for two-stage reclaim.
 *
 * Returns total number of reclaimed entries.
 */
int marufs_nrht_gc_sweep_all(struct marufs_sb_info *sbi)
{
	int reclaimed = 0;
	u32 epoch = (u32)atomic_read(&sbi->gc_epoch);
	u32 region_id;

	for_each_set_bit(region_id, sbi->gc_nrht_bitmap,
			 MARUFS_MAX_RAT_ENTRIES) {
		struct marufs_nrht_header *nrht =
			nrht_get_header(sbi, region_id);
		if (!nrht) {
			/* Region gone — clear stale bitmap bit */
			clear_bit(region_id, sbi->gc_nrht_bitmap);
			continue;
		}

		u32 num_shards = READ_CXL_LE32(nrht->num_shards);
		u32 shards_per_cycle = max(num_shards / 4, 1U);
		u32 start = (epoch * shards_per_cycle) % num_shards;

		for (u32 si = 0; si < shards_per_cycle; si++) {
			u32 s = (start + si) % num_shards;
			struct nrht_shard_ctx ctx;
			if (nrht_get_shard_ctx(sbi, nrht, s, &ctx))
				continue;

			for (u32 j = 0; j < ctx.num_entries; j++) {
				struct marufs_nrht_entry *e = &ctx.entries[j];
				MARUFS_CXL_RMB(e, 64);

				if (READ_CXL_LE32(e->state) !=
				    MARUFS_ENTRY_INSERTING)
					continue;

				int ret = nrht_is_stale(sbi, e);
				if (ret == 1) {
					WRITE_LE64(e->name_hash, 0);
					MARUFS_CXL_WMB(e, 64);
					if (marufs_le32_cas(
						    &e->state,
						    MARUFS_ENTRY_INSERTING,
						    MARUFS_ENTRY_TOMBSTONE) ==
					    MARUFS_ENTRY_INSERTING)
						reclaimed++;
				} else if (ret == 0) {
					marufs_gc_track_orphan(
						sbi, e, MARUFS_ORPHAN_NRHT);
				}
			}
		}
	}

	if (reclaimed > 0)
		pr_debug("nrht gc: reclaimed %d stale INSERTING entries\n",
			 reclaimed);

	return reclaimed;
}
