// SPDX-License-Identifier: GPL-2.0-only
/*
 * dir.c - MARUFS flat namespace directory operations
 *
 * Root directory is a flat namespace containing all files across all regions.
 * Files are accessed through global sharded index. No chunk directories exist.
 *
 * Two-phase create:
 *   open(O_CREAT) -> reserve RAT entry + insert index (lightweight, no space)
 *   ftruncate(N)  -> allocate physical region (in inode.c setattr)
 */

#include <linux/fs.h>
#include <linux/mm.h>
#include <linux/prefetch.h>
#include <linux/slab.h>
#include <linux/string.h>
#include <linux/uaccess.h>

#include "compat.h"
#include "marufs.h"

/* readdir position encoding: (shard << 20) | entry */
#define MARUFS_READDIR_SHARD_SHIFT 20
#define MARUFS_READDIR_ENTRY_MASK 0xFFFFF

/* ============================================================================
 * marufs_lookup - Global index hash lookup
 * ============================================================================ */

static struct dentry *marufs_lookup(struct inode *dir, struct dentry *dentry,
				    unsigned int flags)
{
	struct super_block *sb = dir->i_sb;
	struct marufs_sb_info *sbi = MARUFS_SB(sb);
	struct marufs_index_entry_hot *hot;
	struct inode *inode = NULL;
	int ret;

	if (dir->i_ino != MARUFS_ROOT_INO)
		return ERR_PTR(-ENOENT);

	pr_debug("lookup '%.*s' in global index\n", (int)dentry->d_name.len,
		 dentry->d_name.name);

	/* Search in global index */
	ret = marufs_index_lookup(sbi, dentry->d_name.name, dentry->d_name.len,
				  &hot);
	if (ret == 0) {
		u64 hash;
		u32 shard_id;
		u32 entry_idx;
		struct marufs_index_entry_hot *base;
		struct marufs_index_entry_cold *cold;

		/* Name-ref entries are not files — skip them in VFS lookup */
		if (le16_to_cpu(hot->flags) & MARUFS_ENTRY_FLAG_NAME_REF)
			return d_splice_alias(NULL, dentry);

		/* Found - determine shard_id and entry_idx for inode creation */
		hash = le64_to_cpu(hot->name_hash);
		shard_id = marufs_shard_id(hash, sbi->shard_mask);

		base = marufs_shard_hot_entries(sbi, shard_id);
		if (!base)
			return ERR_PTR(-EIO);
		entry_idx = (u32)(hot - base);

		cold = marufs_shard_cold_entry(sbi, shard_id, entry_idx);
		if (!cold)
			return ERR_PTR(-EIO);

		inode = marufs_iget(sb, hot, cold, shard_id, entry_idx);
		if (IS_ERR(inode))
			return ERR_CAST(inode);

		pr_debug("found '%.*s' region=%u\n", (int)dentry->d_name.len,
			 dentry->d_name.name, le32_to_cpu(hot->region_id));
	}

	return d_splice_alias(inode, dentry);
}

/* ============================================================================
 * marufs_create - lightweight file creation (two-phase model)
 * ============================================================================
 *
 * Phase 1 only: reserves RAT entry and inserts index entry.
 * No physical space is allocated. i_size = 0, data_phys_offset = 0.
 * Physical allocation happens in marufs_setattr() via ftruncate().
 */

static int marufs_create(MARUFS_IDMAP_PARAM_COMMA struct inode *dir,
			 struct dentry *dentry, umode_t mode, bool excl)
{
	struct super_block *sb = dir->i_sb;
	struct marufs_sb_info *sbi = MARUFS_SB(sb);
	struct inode *inode;
	struct marufs_inode_info *xi;
	struct marufs_index_entry_hot *entry;
	u32 rat_entry_id;
	u32 entry_idx;
	u64 hash;
	u32 shard_id;
	int ret;

	if (dir->i_ino != MARUFS_ROOT_INO)
		return -ENOENT;

	if (dentry->d_name.len > MARUFS_NAME_MAX)
		return -ENAMETOOLONG;

	pr_debug("creating file '%.*s' (lightweight, no physical space)\n",
		 (int)dentry->d_name.len, dentry->d_name.name);

	/* Check if RAT is available */
	if (!sbi->rat) {
		pr_err("RAT not initialized\n");
		return -ENOSPC;
	}

	/* Step 1: Reserve RAT entry (size=0, offset=0) */
	ret = marufs_rat_alloc_entry(sbi, dentry->d_name.name, 0, 0,
				     &rat_entry_id);
	if (ret) {
		pr_err("RAT entry reservation failed: %d\n", ret);
		return ret;
	}

	/* Step 2: Insert into global index (file_size=0) */
	hash = marufs_hash_name(dentry->d_name.name, dentry->d_name.len);
	shard_id = marufs_shard_id(hash, sbi->shard_mask);

	ret = marufs_index_insert(sbi, dentry->d_name.name, dentry->d_name.len,
				  rat_entry_id, 0, /* region_id, file_size=0 */
				  current_uid().val, current_gid().val,
				  mode & 0777, 0, &entry_idx);
	if (ret) {
		pr_err("index insert failed: %d\n", ret);
		marufs_rat_free_entry(sbi, rat_entry_id);
		return ret;
	}

	/* Resolve entry pointer directly — no redundant lookup needed */
	entry = marufs_shard_hot_entry(sbi, shard_id, entry_idx);
	if (!entry) {
		pr_err("post-insert entry resolve failed\n");
		marufs_index_delete(sbi, dentry->d_name.name,
				    dentry->d_name.len);
		marufs_rat_free_entry(sbi, rat_entry_id);
		return -EIO;
	}

	inode = marufs_new_inode(sb, mode);
	if (IS_ERR(inode)) {
		pr_err("inode creation failed: %ld\n", PTR_ERR(inode));
		marufs_index_delete(sbi, dentry->d_name.name,
				    dentry->d_name.len);
		marufs_rat_free_entry(sbi, rat_entry_id);
		return PTR_ERR(inode);
	}

	xi = MARUFS_I(inode);
	xi->region_id = rat_entry_id;
	xi->entry_idx = entry_idx;
	xi->shard_id = shard_id;
	xi->rat_entry_id = rat_entry_id;
	xi->region_offset = 0; /* No physical region yet */
	xi->owner_node_id = sbi->node_id;
	xi->owner_pid = current->pid;
	xi->owner_birth_time = ktime_to_ns(current->start_boottime);
	xi->data_phys_offset = 0; /* Will be set by ftruncate */

	inode->i_ino = marufs_ino(rat_entry_id, 0);
	inode->i_size = 0;
	inode->i_op = &marufs_file_inode_ops;
	inode->i_fop = &marufs_file_ops;

	d_instantiate(dentry, inode);

	pr_debug("reserved RAT entry %u for '%.*s' (ftruncate pending)\n",
		 rat_entry_id, (int)dentry->d_name.len, dentry->d_name.name);

	return 0;
}

/* ============================================================================
 * marufs_unlink - deletion via global index tombstone
 * ============================================================================ */

/*
 * marufs_unlink_cleanup_region - invalidate region header and free RAT entry
 * @sbi: superblock info
 * @rat_entry_id: RAT entry ID to clean up
 */
static void marufs_unlink_cleanup_region(struct marufs_sb_info *sbi,
					 u32 rat_entry_id)
{
	struct marufs_rat_entry *rat_e;
	u32 old_state;

	rat_e = marufs_rat_get(sbi, rat_entry_id);
	if (!rat_e)
		return;
	MARUFS_CXL_RMB(rat_e, sizeof(*rat_e));

	/* CAS: ALLOCATED → DELETING (preempt — prevent race with GC) */
	old_state = CAS_LE32(&rat_e->state, MARUFS_RAT_ENTRY_ALLOCATED,
			     MARUFS_RAT_ENTRY_DELETING);
	if (old_state != MARUFS_RAT_ENTRY_ALLOCATED)
		return; /* GC already preempted or already FREE */

	/* Invalidate region header in pool */
	{
		struct marufs_region_header *rhdr;

		rhdr = marufs_region_hdr(sbi, rat_entry_id);
		if (rhdr) {
			WRITE_LE32(rhdr->magic, 0);
			WRITE_LE32(rhdr->state, 0);
			MARUFS_CXL_WMB(rhdr, sizeof(*rhdr));
		}
	}

	marufs_rat_free_entry(sbi, rat_entry_id);
}

static int marufs_unlink(struct inode *dir, struct dentry *dentry)
{
	struct inode *inode = d_inode(dentry);
	struct marufs_inode_info *xi = MARUFS_I(inode);
	struct marufs_sb_info *sbi = MARUFS_SB(inode->i_sb);
	int ret;

	pr_debug("unlink '%.*s' rat_entry=%u\n", (int)dentry->d_name.len,
		 dentry->d_name.name, xi->rat_entry_id);

	/* Permission check: DELETE required */
	ret = marufs_check_permission(sbi, xi->rat_entry_id,
				      MARUFS_PERM_DELETE);
	if (ret) {
		/* Dead owner with no active delegations → allow GC-style delete */
		if (!marufs_is_orphaned(sbi, xi->rat_entry_id)) {
			pr_err("no delete permission for rat_entry %u\n",
			       xi->rat_entry_id);
			return -EACCES;
		}
	}

	/* Tombstone name-ref entries first (before region entry, to avoid stale lookups) */
	marufs_index_delete_by_region(sbi, xi->rat_entry_id);

	/* Delete region entry from global index (mark as TOMBSTONE) */
	ret = marufs_index_delete(sbi, dentry->d_name.name, dentry->d_name.len);
	if (ret) {
		pr_err("index delete failed: %d\n", ret);
		return ret;
	}

	/* Region cleanup: only if RAT entry still ALLOCATED (skip if GC freed it) */
	marufs_unlink_cleanup_region(sbi, xi->rat_entry_id);

	drop_nlink(inode);
	inode_set_ctime_to_ts(inode, current_time(inode));

	pr_debug("unlinked '%.*s'\n", (int)dentry->d_name.len,
		 dentry->d_name.name);

	return 0;
}

/* ============================================================================
 * marufs_iterate - scan all shards for readdir
 * ============================================================================ */

static int marufs_iterate(struct file *file, struct dir_context *ctx)
{
	struct inode *inode = file_inode(file);
	struct marufs_sb_info *sbi = MARUFS_SB(inode->i_sb);
	struct marufs_rat_entry *rat_e;
	u32 i;
	char name_buf[MARUFS_NAME_MAX + 1];

	/* "." entry */
	if (ctx->pos == 0) {
		if (!dir_emit_dot(file, ctx))
			return 0;
		ctx->pos = 1;
	}

	/* ".." entry */
	if (ctx->pos == 1) {
		if (!dir_emit_dotdot(file, ctx))
			return 0;
		ctx->pos = 2;
	}

	/*
	 * RAT-based readdir: scan 256 RAT entries instead of 64*16384 index entries.
	 *
	 * ctx->pos encoding:
	 *   0 = "."
	 *   1 = ".."
	 *   2 + i = RAT entry[i]  (i = 0..255)
	 *
	 * On VFS re-entry after buffer full, pos resumes from the next RAT slot.
	 */
	for (i = ctx->pos - 2; i < MARUFS_MAX_RAT_ENTRIES; i++) {
		unsigned long ino;
		size_t name_len;

		rat_e = &sbi->rat->entries[i];
		MARUFS_CXL_RMB(rat_e, sizeof(*rat_e));

		if (READ_LE32(rat_e->state) != MARUFS_RAT_ENTRY_ALLOCATED)
			continue;

		memcpy(name_buf, rat_e->name, MARUFS_NAME_MAX);
		name_buf[MARUFS_NAME_MAX] = '\0';
		name_len = strnlen(name_buf, MARUFS_NAME_MAX);

		if (name_len == 0)
			continue;

		ino = marufs_ino(READ_LE32(rat_e->rat_entry_id), 0);

		if (!dir_emit(ctx, name_buf, name_len, ino, DT_REG))
			return 0;

		ctx->pos = i + 3; /* next iteration resumes from i+1 */
	}

	return 0;
}

/* ============================================================================
 * marufs_dir_ioctl - global name lookup (MARUFS_IOC_FIND_NAME)
 * ============================================================================
 *
 * Callable on root directory fd or any file fd.
 * Looks up a name-ref entry in the global index and returns the
 * associated region filename (from RAT) and data offset.
 */

/* ── ioctl handler: FIND_NAME (single lookup) ───────────────────────── */
static long marufs_ioctl_find_name(struct marufs_sb_info *sbi,
				   unsigned long arg)
{
	struct marufs_find_name_req req;
	struct marufs_index_entry_hot *entry;
	struct marufs_rat_entry *rat_e;
	size_t name_len;
	u32 region_id;
	int ret;

	if (copy_from_user(&req, (void __user *)arg, sizeof(req)))
		return -EFAULT;

	req.name[sizeof(req.name) - 1] = '\0';
	name_len = strnlen(req.name, sizeof(req.name));

	ret = marufs_index_lookup_hash(sbi, req.name, name_len, req.name_hash,
				       &entry);
	if (ret)
		return -ENOENT;

	if (!(le16_to_cpu(entry->flags) & MARUFS_ENTRY_FLAG_NAME_REF))
		return -ENOENT;

	region_id = le32_to_cpu(entry->region_id);
	req.offset = le64_to_cpu(entry->file_size);

	/* Read region name directly from CXL RAT entry */
	rat_e = marufs_rat_get(sbi, region_id);
	if (!rat_e)
		return -EIO;
	MARUFS_CXL_RMB(rat_e, sizeof(*rat_e));
	memset(req.region_name, 0, sizeof(req.region_name));
	memcpy(req.region_name, rat_e->name,
	       min(sizeof(req.region_name) - 1, sizeof(rat_e->name)));

	if (copy_to_user((void __user *)arg, &req, sizeof(req)))
		return -EFAULT;
	return 0;
}

/* ── ioctl handler: BATCH_FIND_NAME (batched lookup) ────────────────── */
static long marufs_ioctl_batch_find_name(struct marufs_sb_info *sbi,
					 unsigned long arg)
{
	struct marufs_batch_find_req breq;
	struct marufs_batch_find_entry *bent;
	size_t buf_size;
	u32 i;

	if (copy_from_user(&breq, (void __user *)arg, sizeof(breq)))
		return -EFAULT;

	if (breq.count == 0 || breq.count > MARUFS_BATCH_FIND_MAX)
		return -EINVAL;

	buf_size = (size_t)breq.count * sizeof(*bent);
	bent = kvmalloc(buf_size, GFP_KERNEL);
	if (!bent)
		return -ENOMEM;

	if (copy_from_user(bent, (void __user *)u64_to_user_ptr(breq.entries),
			   buf_size)) {
		kvfree(bent);
		return -EFAULT;
	}

	/*
	 * Optimized 4-phase batch lookup with CXL latency hiding:
	 *   Phase 1:  Prefetch bucket heads + save computed hash
	 *   Phase 1b: Prefetch first chain entries (bucket heads now in cache)
	 *   Phase 2:  Lookup all entries (defer RAT reads)
	 *   Phase 3:  Prefetch RAT entries for successful lookups
	 *   Phase 4:  Copy region_name from prefetched RAT entries
	 */

	/* Temporary region_id storage for deferred RAT reads */
	{
		u32 *rids;

		rids = kvmalloc_array(breq.count, sizeof(u32), GFP_KERNEL);
		if (!rids) {
			kvfree(bent);
			return -ENOMEM;
		}

		/* Phase 1: Prefetch bucket heads, save computed hash */
		for (i = 0; i < breq.count; i++) {
			u64 h = bent[i].name_hash;
			u32 sid, bid;
			struct marufs_shard_cache *sc;

			bent[i].name[sizeof(bent[i].name) - 1] = '\0';
			if (h == 0) {
				h = marufs_hash_name(
					bent[i].name,
					strnlen(bent[i].name,
						sizeof(bent[i].name)));
				bent[i].name_hash = h;
			}
			sid = marufs_shard_id(h, sbi->shard_mask);
			sc = &sbi->shard_cache[sid];
			bid = marufs_bucket_idx(h, sc->num_buckets);
			prefetch(&sc->buckets[bid]);
		}

		/* Phase 1b: Prefetch first chain entries (bucket heads now in cache) */
		for (i = 0; i < breq.count; i++) {
			u64 h = bent[i].name_hash;
			u32 sid, bid, head;
			struct marufs_shard_cache *sc;

			sid = marufs_shard_id(h, sbi->shard_mask);
			sc = &sbi->shard_cache[sid];
			bid = marufs_bucket_idx(h, sc->num_buckets);
			head = READ_LE32(sc->buckets[bid]);
			if (head != MARUFS_BUCKET_END && head < sc->num_entries)
				prefetch(&sc->hot_entries[head]);
		}

		/* Phase 2: Lookup all entries (defer RAT reads) */
		breq.found = 0;
		for (i = 0; i < breq.count; i++) {
			struct marufs_index_entry_hot *ie;
			size_t nlen;
			int r;

			nlen = strnlen(bent[i].name, sizeof(bent[i].name));

			r = marufs_index_lookup_hash(sbi, bent[i].name, nlen,
						     bent[i].name_hash, &ie);
			if (r || !(le16_to_cpu(ie->flags) &
				   MARUFS_ENTRY_FLAG_NAME_REF)) {
				bent[i].status = -ENOENT;
				rids[i] = MARUFS_MAX_RAT_ENTRIES; /* sentinel */
				continue;
			}

			rids[i] = le32_to_cpu(ie->region_id);
			bent[i].offset = le64_to_cpu(ie->file_size);
			bent[i].status = 0;
			breq.found++;
		}

		/* Phase 3: Prefetch RAT entries for successful lookups */
		for (i = 0; i < breq.count; i++) {
			if (rids[i] < MARUFS_MAX_RAT_ENTRIES) {
				struct marufs_rat_entry *rat_e =
					marufs_rat_get(sbi, rids[i]);
				if (rat_e)
					prefetch(rat_e);
			}
		}

		/* Phase 4: Copy region_name from prefetched RAT entries */
		for (i = 0; i < breq.count; i++) {
			struct marufs_rat_entry *rat_e;

			if (rids[i] >= MARUFS_MAX_RAT_ENTRIES)
				continue;

			rat_e = marufs_rat_get(sbi, rids[i]);
			if (!rat_e) {
				bent[i].status = -EIO;
				breq.found--;
				continue;
			}
			MARUFS_CXL_RMB(rat_e, sizeof(*rat_e));
			memset(bent[i].region_name, 0,
			       sizeof(bent[i].region_name));
			memcpy(bent[i].region_name, rat_e->name,
			       min(sizeof(bent[i].region_name) - 1,
				   sizeof(rat_e->name)));
		}

		kvfree(rids);
	}

	if (copy_to_user((void __user *)u64_to_user_ptr(breq.entries), bent,
			 buf_size)) {
		kvfree(bent);
		return -EFAULT;
	}

	kvfree(bent);

	if (copy_to_user((void __user *)arg, &breq, sizeof(breq)))
		return -EFAULT;
	return 0;
}

long marufs_dir_ioctl(struct file *file, unsigned int cmd, unsigned long arg)
{
	struct inode *inode = file_inode(file);
	struct marufs_sb_info *sbi = MARUFS_SB(inode->i_sb);

	switch (cmd) {
	case MARUFS_IOC_FIND_NAME:
		return marufs_ioctl_find_name(sbi, arg);

	case MARUFS_IOC_BATCH_FIND_NAME:
		return marufs_ioctl_batch_find_name(sbi, arg);

	default:
		return -ENOTTY;
	}
}

/* ============================================================================
 * Operations tables
 * ============================================================================ */

const struct file_operations marufs_dir_ops = {
	.owner = THIS_MODULE,
	.llseek = generic_file_llseek,
	.read = generic_read_dir,
	.iterate_shared = marufs_iterate,
	.unlocked_ioctl = marufs_dir_ioctl,
	.fsync = noop_fsync,
};

const struct inode_operations marufs_dir_inode_ops = {
	.lookup = marufs_lookup,
	.create = marufs_create,
	.unlink = marufs_unlink,
};
