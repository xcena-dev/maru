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
#include "me.h"

/* ============================================================================
 * marufs_lookup - Global index hash lookup
 * ============================================================================ */

static struct dentry *marufs_lookup(struct inode *dir, struct dentry *dentry,
				    unsigned int flags)
{
	struct super_block *sb = dir->i_sb;
	struct marufs_sb_info *sbi = marufs_sb_get(sb);
	struct marufs_index_entry *entry;
	struct inode *inode = NULL;
	int ret;

	if (dir->i_ino != MARUFS_ROOT_INO)
		return ERR_PTR(-ENOENT);

	pr_debug("lookup '%.*s' in global index\n", (int)dentry->d_name.len,
		 dentry->d_name.name);

	/* Search in global index */
	ret = marufs_index_lookup(sbi, dentry->d_name.name, dentry->d_name.len,
				  &entry);
	if (ret == 0) {
		u64 hash;
		u32 shard_id;
		u32 entry_idx;
		struct marufs_index_entry *base;

		/* Found - determine shard_id and entry_idx for inode creation */
		hash = READ_CXL_LE64(entry->name_hash);
		shard_id = marufs_shard_idx(hash, sbi->shard_mask);

		base = marufs_shard_entries(sbi, shard_id);
		if (!base)
			return ERR_PTR(-EIO);
		entry_idx = (u32)(entry - base);

		inode = marufs_iget(sb, entry, shard_id, entry_idx);
		if (IS_ERR(inode))
			return ERR_CAST(inode);

		pr_debug("found '%.*s' region=%u\n", (int)dentry->d_name.len,
			 dentry->d_name.name, READ_CXL_LE32(entry->region_id));
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

/*
 * __marufs_create_locked - critical section: RAT alloc + index insert.
 * Caller holds Global ME. On success, *out_rat_entry_id and *out_entry_idx
 * are set. On failure, any partial allocation is cleaned up internally.
 */
static int __marufs_create_locked(struct marufs_sb_info *sbi,
				  struct dentry *dentry, umode_t mode,
				  u32 *out_rat_entry_id, u32 *out_entry_idx)
{
	/* Step 1: Reserve RAT entry (size=0, offset=0) */
	u32 rat_entry_id;
	int ret = marufs_rat_alloc_entry(sbi, dentry->d_name.name, 0, 0,
					 &rat_entry_id);
	if (ret) {
		pr_err("RAT entry reservation failed: %d\n", ret);
		return ret;
	}

	/* Write uid/gid/mode to RAT entry (single source of truth) */
	struct marufs_rat_entry *rat_e =
		marufs_rat_entry_get(sbi, rat_entry_id);
	if (rat_e) {
		WRITE_LE32(rat_e->uid, current_uid().val);
		WRITE_LE32(rat_e->gid, current_gid().val);
		WRITE_LE16(rat_e->mode, mode & 0777);
		MARUFS_CXL_WMB(rat_e, sizeof(*rat_e));
	}

	/* Step 2: Insert into global index */
	ret = marufs_index_insert(sbi, dentry->d_name.name, dentry->d_name.len,
				  rat_entry_id, out_entry_idx);
	if (ret) {
		pr_err("index insert failed: %d\n", ret);
		marufs_rat_free_entry(marufs_rat_entry_get(sbi, rat_entry_id));
		return ret;
	}

	*out_rat_entry_id = rat_entry_id;
	return 0;
}

static int marufs_create(MARUFS_IDMAP_PARAM_COMMA struct inode *dir,
			 struct dentry *dentry, umode_t mode, bool excl)
{
	if (dir->i_ino != MARUFS_ROOT_INO)
		return -ENOENT;

	if (dentry->d_name.len > MARUFS_NAME_MAX)
		return -ENAMETOOLONG;

	pr_debug("creating file '%.*s' (lightweight, no physical space)\n",
		 (int)dentry->d_name.len, dentry->d_name.name);

	struct super_block *sb = dir->i_sb;
	struct marufs_sb_info *sbi = marufs_sb_get(sb);
	if (!sbi->rat) {
		pr_err("RAT not initialized\n");
		return -ENOSPC;
	}

	/* Global ME: serialize RAT alloc + index insert across nodes */
	int ret = sbi->me->ops->acquire(sbi->me, MARUFS_ME_GLOBAL_SHARD_ID);
	if (ret)
		return ret;
	u32 rat_entry_id, entry_idx;
	ret = __marufs_create_locked(sbi, dentry, mode, &rat_entry_id,
				     &entry_idx);
	sbi->me->ops->release(sbi->me, MARUFS_ME_GLOBAL_SHARD_ID);
	if (ret)
		return ret;

	u64 hash = marufs_hash_name(dentry->d_name.name, dentry->d_name.len);
	u32 shard_id = marufs_shard_idx(hash, sbi->shard_mask);

	/* Resolve entry pointer directly — no redundant lookup needed */
	struct marufs_index_entry *entry =
		marufs_shard_entry(sbi, shard_id, entry_idx);
	if (!entry) {
		pr_err("post-insert entry resolve failed\n");
		marufs_index_delete(sbi, dentry->d_name.name,
				    dentry->d_name.len);
		marufs_rat_free_entry(marufs_rat_entry_get(sbi, rat_entry_id));
		return -EIO;
	}

	struct inode *inode = marufs_new_inode(sb, mode);
	if (IS_ERR(inode)) {
		pr_err("inode creation failed: %ld\n", PTR_ERR(inode));
		marufs_index_delete(sbi, dentry->d_name.name,
				    dentry->d_name.len);
		marufs_rat_free_entry(marufs_rat_entry_get(sbi, rat_entry_id));
		return PTR_ERR(inode);
	}

	struct marufs_inode_info *xi = marufs_inode_get(inode);
	xi->region_id = rat_entry_id;
	xi->entry_idx = entry_idx;
	xi->shard_id = shard_id;
	xi->rat_entry_id = rat_entry_id;
	xi->region_offset = 0; /* No physical region yet */
	xi->owner_node_id = sbi->node_id;
	xi->owner_pid = current->pid;
	xi->owner_birth_time = ktime_to_ns(current->start_boottime);
	xi->data_phys_offset = 0; /* Will be set by ftruncate */

	inode->i_ino = marufs_make_ino(rat_entry_id);
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
	struct marufs_rat_entry *rat_e =
		marufs_rat_entry_get(sbi, rat_entry_id);
	if (!rat_e)
		return;

	/* CAS: ALLOCATED → DELETING (preempt — prevent race with GC) */
	u32 old_state = marufs_le32_cas(&rat_e->state,
					MARUFS_RAT_ENTRY_ALLOCATED,
					MARUFS_RAT_ENTRY_DELETING);
	if (old_state != MARUFS_RAT_ENTRY_ALLOCATED)
		return; /* GC already preempted or already FREE */

	marufs_rat_free_entry(rat_e);
}

static int marufs_unlink(struct inode *dir, struct dentry *dentry)
{
	struct inode *inode = d_inode(dentry);
	struct marufs_inode_info *xi = marufs_inode_get(inode);
	struct marufs_sb_info *sbi = marufs_sb_get(inode->i_sb);
	int ret;

	pr_debug("unlink '%.*s' rat_entry=%u\n", (int)dentry->d_name.len,
		 dentry->d_name.name, xi->rat_entry_id);

	/* Permission check: DELETE required */
	ret = marufs_check_permission(sbi, xi->rat_entry_id,
				      MARUFS_PERM_DELETE);
	if (ret) {
		/* Dead owner with no active delegations → allow force delete */
		if (!marufs_can_force_unlink(sbi, xi->rat_entry_id)) {
			pr_err("no delete permission for rat_entry %u\n",
			       xi->rat_entry_id);
			return -EACCES;
		}
	}

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
	struct marufs_sb_info *sbi = marufs_sb_get(inode->i_sb);
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

		ino = marufs_make_ino(i);

		if (!dir_emit(ctx, name_buf, name_len, ino, DT_REG))
			return 0;

		ctx->pos = i + 3; /* next iteration resumes from i+1 */
	}

	return 0;
}

/* ============================================================================
 * Operations tables
 * ============================================================================ */

const struct file_operations marufs_dir_ops = {
	.owner = THIS_MODULE,
	.llseek = generic_file_llseek,
	.read = generic_read_dir,
	.iterate_shared = marufs_iterate,
	.fsync = noop_fsync,
};

const struct inode_operations marufs_dir_inode_ops = {
	.lookup = marufs_lookup,
	.create = marufs_create,
	.unlink = marufs_unlink,
};
