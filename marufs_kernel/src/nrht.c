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
		return (sbi->node_id == 1) ? 0 : -1; /* Only admin node tracks orphans */

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

static int nrht_link_to_bucket(struct marufs_nrht_entry *entry, u32 entry_idx,
			       u32 *bucket_head)
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
		MARUFS_CXL_WMB(entry, 64);

		if (marufs_le32_cas(bucket_head, old_head, entry_idx) ==
		    old_head)
			break;

		if (++retries >= max_retries) {
			pr_err("nrht: bucket CAS failed after %d retries\n",
			       retries);
			WRITE_LE32(entry->state, MARUFS_ENTRY_EMPTY);
			MARUFS_CXL_WMB(entry, 64);
			return -EAGAIN;
		}
		cpu_relax();
	}

	return 0;
}

/*
 * nrht_post_insert_dedup - detect concurrent duplicate after publish.
 * If another VALID entry with the same name exists, higher entry_idx loses
 * and rolls back to TOMBSTONE.
 * Return: 0 if unique, -EEXIST if this entry lost dedup.
 */
static int nrht_post_insert_dedup(struct nrht_shard_ctx *ctx,
				  struct marufs_nrht_entry *entry,
				  u32 entry_idx, u64 hash, const char *name,
				  size_t namelen)
{
	u32 winner_idx;
	struct marufs_nrht_entry *winner =
		nrht_find_chain(ctx, hash, name, namelen, &winner_idx, NULL);
	if (winner && winner != entry && entry_idx > winner_idx) {
		WRITE_LE32(entry->state, MARUFS_ENTRY_TOMBSTONE);
		MARUFS_CXL_WMB(entry, 64);
		return -EEXIST;
	}

	return 0;
}

/* ============================================================================
 * Public API
 * ============================================================================ */
int marufs_nrht_init(struct marufs_sb_info *sbi, u32 nrht_region_id,
		     u32 max_entries, u32 num_shards, u32 num_buckets)
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
	buckets_per_shard = roundup_pow_of_two(buckets_per_shard);

	u64 bucket_array_size =
		marufs_align_up((u64)buckets_per_shard * sizeof(u32), 64);
	u64 per_shard_size =
		bucket_array_size +
		(u64)entries_per_shard * sizeof(struct marufs_nrht_entry);
	u64 shard_headers_end =
		sizeof(struct marufs_nrht_header) +
		(u64)num_shards * sizeof(struct marufs_nrht_shard_header);
	u64 total_needed = shard_headers_end + (u64)num_shards * per_shard_size;

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

	/* Tag region as NRHT for GC discovery via RAT scan */
	WRITE_LE32(rat_e->region_type, MARUFS_REGION_NRHT);
	MARUFS_CXL_WMB(rat_e, 64);

	/* Register in DRAM bitmap for fast GC enumeration */
	set_bit(nrht_region_id, sbi->gc_nrht_bitmap);

	/* ── Step 3: resolve base pointer + double-init check ────────── */
	if (!marufs_dax_range_valid(sbi, phys_offset, region_size))
		return -EINVAL;

	void *base = marufs_dax_ptr(sbi, phys_offset);
	if (!base)
		return -EINVAL;

	/* Double-init protection: only for pre-existing regions (ftruncate path).
	 * Freshly allocated memory may contain stale magic from recycled regions. */
	if (!freshly_allocated) {
		struct marufs_nrht_header *existing = base;
		MARUFS_CXL_RMB(existing, sizeof(*existing));
		if (READ_CXL_LE32(existing->magic) == MARUFS_NRHT_MAGIC) {
			pr_err("nrht_init: region %u already formatted\n",
			       nrht_region_id);
			return -EEXIST;
		}
	}

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

	u64 offset = shard_headers_end;
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

	pr_info("nrht_init: region %u, %u shards, %u entries/shard, %u buckets/shard, %llu bytes\n",
		nrht_region_id, num_shards, entries_per_shard,
		buckets_per_shard, total_needed);

	return 0;
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
	size_t copy_len = min(namelen, sizeof(entry->name) - 1);

	WRITE_LE64(entry->name_hash, name_hash);
	WRITE_LE64(entry->offset, offset);
	WRITE_LE32(entry->target_region_id, target_region_id);
	WRITE_LE32(entry->inserter_node, sbi->node_id);
	WRITE_LE64(entry->created_at, ktime_get_real_ns());

	memset(entry->name, 0, sizeof(entry->name));
	memcpy(entry->name, name, copy_len);

	MARUFS_CXL_WMB(entry, sizeof(*entry)); /* flush both CL0 + CL1 */

	/* Step 4: link to bucket (skip if reused — already in chain) */
	if (!reused_chain_entry) {
		ret = nrht_link_to_bucket(entry, entry_idx, ctx.bucket_head);
		if (ret)
			return ret;
	}

	/* Step 5: publish INSERTING → VALID */
	WRITE_LE32(entry->state, MARUFS_ENTRY_VALID);
	MARUFS_CXL_WMB(entry, 64);

	/* Step 6: post-insert dedup — concurrent inserts resolved here */
	ret = nrht_post_insert_dedup(&ctx, entry, entry_idx, name_hash, name,
				     namelen);
	if (ret)
		return ret;

	pr_debug("nrht: insert '%.*s' -> entry %u (region %u offset %llu)\n",
		 (int)namelen, name, entry_idx, target_region_id, offset);

	return 0;
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
