// SPDX-License-Identifier: GPL-2.0-only
/*
 * inode.c - MARUFS inode operations
 *
 * VFS inode operations implementation for MARUFS partitioned global index.
 * Maps index entries to VFS inodes and handles metadata updates.
 *
 * Two-phase model:
 *   marufs_iget:    reads RAT entry, sets data_phys_offset if region initialized
 *   marufs_setattr: ftruncate triggers marufs_region_init (first-time only, WORM)
 */

#include <linux/fs.h>
#include <linux/io.h>
#include <linux/pagemap.h>
#include <linux/slab.h>
#include <linux/time.h>
#include <linux/writeback.h>

#include "compat.h"
#include "marufs.h"

/* ============================================================================
 * inode read operations (index entry → VFS inode)
 * ============================================================================ */

/*
 * marufs_resolve_rat_entry - read RAT entry fields into inode info
 * @sbi: superblock info
 * @xi: inode info to populate
 * @rat_entry_id: RAT entry ID to resolve
 *
 * Reads ownership, region_offset, and data_phys_offset from RAT entry.
 * v2: data starts directly at phys_offset (no header in region).
 *
 * Return: 0 on success, -ENOENT if RAT entry not allocated, -EINVAL if missing
 */
static int marufs_resolve_rat_entry(struct marufs_sb_info *sbi,
				    struct marufs_inode_info *xi,
				    u32 rat_entry_id)
{
	struct marufs_rat_entry *rat_entry;
	struct marufs_region_header *rh;
	u64 phys_offset;

	rat_entry = marufs_rat_get(sbi, rat_entry_id);
	if (!rat_entry)
		return -EINVAL;

	MARUFS_CXL_RMB(rat_entry, sizeof(*rat_entry));
	if (READ_LE32(rat_entry->state) != MARUFS_RAT_ENTRY_ALLOCATED)
		return -ENOENT;

	phys_offset = READ_LE64(rat_entry->phys_offset);
	xi->region_offset = phys_offset;
	xi->owner_node_id = READ_LE32(rat_entry->owner_node_id);
	xi->owner_pid = READ_LE32(rat_entry->owner_pid);
	xi->owner_birth_time = READ_LE64(rat_entry->owner_birth_time);

	if (!marufs_valid_phys_offset(sbi, phys_offset))
		goto no_data;

	/* v2: data starts directly at phys_offset (header is in pool) */
	rh = marufs_region_hdr(sbi, rat_entry_id);
	if (!rh || READ_LE32(rh->magic) != MARUFS_REGION_MAGIC)
		goto no_data;

	MARUFS_CXL_RMB(rh, sizeof(*rh));
	xi->data_phys_offset = phys_offset;
	return 0;

no_data:
	xi->data_phys_offset = 0;
	return 0;
}

/*
 * marufs_inode_fill_from_entry - populate VFS inode fields from index entry
 * @inode: target inode
 * @ie: source index entry (CXL memory)
 *
 * Reads mode, uid, gid, file_size from index entry and updates VFS inode.
 * Used by both new inode creation and cached inode refresh paths.
 */
static void marufs_inode_fill_from_entry(struct inode *inode,
					 struct marufs_index_entry_hot *hot,
					 struct marufs_index_entry_cold *cold)
{
	inode->i_mode = S_IFREG | le16_to_cpu(cold->mode);
	inode->i_uid = make_kuid(&init_user_ns, le32_to_cpu(cold->uid));
	inode->i_gid = make_kgid(&init_user_ns, le32_to_cpu(cold->gid));
	inode->i_size = le64_to_cpu(hot->file_size);
	inode->i_blocks = (inode->i_size + MARUFS_SECTOR_SIZE - 1) >>
			  MARUFS_SECTOR_SHIFT;
	set_nlink(inode, 1);
}

/*
 * marufs_iget - read inode from global index entry
 * @sb: Superblock
 * @ie: Index entry pointer
 * @shard_id: Shard ID this entry belongs to
 * @entry_idx: Entry index within shard's entry array
 *
 * Creates or updates VFS inode from index entry. Reads latest file_size
 * from shared CXL memory to support cross-node visibility.
 *
 * Two-phase awareness:
 *   phys_offset > 0: region initialized, data_phys_offset = phys_offset
 *   phys_offset == 0: region not yet initialized (ftruncate pending)
 *
 * Return: inode pointer on success, ERR_PTR on error
 */
struct inode *marufs_iget(struct super_block *sb,
			  struct marufs_index_entry_hot *hot,
			  struct marufs_index_entry_cold *cold, u32 shard_id,
			  u32 entry_idx)
{
	struct marufs_inode_info *xi;
	struct inode *inode;
	u32 region_id = le32_to_cpu(hot->region_id);
	unsigned long ino = marufs_ino(region_id, 0);

	/* Read fresh data from shared memory */
	MARUFS_CXL_RMB(hot, sizeof(*hot));

	/* Check if entry is VALID */
	if (READ_LE32(hot->state) != MARUFS_ENTRY_VALID)
		return ERR_PTR(-ENOENT);

	inode = iget_locked(sb, ino);
	if (!inode)
		return ERR_PTR(-ENOMEM);

	if (!(inode->i_state & I_NEW)) {
		/*
                 * Already cached — refresh from CXL memory for cross-node
                 * visibility.  When a RAT entry is reused (file deleted then
                 * new file allocated to the same entry), the cached inode
                 * carries stale data_phys_offset / owner fields.  Re-read
                 * everything that can change between uses of the same ino.
                 */
		struct marufs_sb_info *sbi = MARUFS_SB(sb);

		xi = MARUFS_I(inode);
		xi->region_id = region_id;
		xi->entry_idx = entry_idx;
		xi->shard_id = shard_id;
		xi->rat_entry_id = region_id;

		marufs_inode_fill_from_entry(inode, hot, cold);

		/* Re-validate RAT entry: GC may have freed it since caching */
		{
			int rat_ret =
				marufs_resolve_rat_entry(sbi, xi, region_id);
			if (rat_ret == -ENOENT) {
				iput(inode);
				return ERR_PTR(-ESTALE);
			}
		}

		return inode;
	}

	/* New inode - populate from index entry */
	xi = MARUFS_I(inode);
	xi->region_id = region_id;
	xi->entry_idx = entry_idx;
	xi->shard_id = shard_id;

	/* Restore fields from RAT entry */
	{
		struct marufs_sb_info *sbi = MARUFS_SB(sb);
		int rat_ret;

		xi->rat_entry_id = region_id;

		rat_ret = marufs_resolve_rat_entry(sbi, xi, region_id);
		if (rat_ret == -ENOENT) {
			iget_failed(inode);
			return ERR_PTR(-ENOENT);
		}
		if (rat_ret == -EINVAL) {
			/* Invalid RAT entry ID */
			xi->region_offset = 0;
			xi->owner_node_id = 0;
			xi->owner_pid = 0;
			xi->owner_birth_time = 0;
			xi->data_phys_offset = 0;
		}
	}

	marufs_inode_fill_from_entry(inode, hot, cold);

	/* Timestamps from cold entry */
	{
		struct timespec64 ts;
		ts.tv_sec = le64_to_cpu(cold->created_at) / NSEC_PER_SEC;
		ts.tv_nsec = le64_to_cpu(cold->created_at) % NSEC_PER_SEC;
		inode_set_atime_to_ts(inode, ts);

		ts.tv_sec = le64_to_cpu(cold->modified_at) / NSEC_PER_SEC;
		ts.tv_nsec = le64_to_cpu(cold->modified_at) % NSEC_PER_SEC;
		inode_set_mtime_to_ts(inode, ts);
		inode_set_ctime_to_ts(inode, ts);
	}

	inode->i_op = &marufs_file_inode_ops;
	inode->i_fop = &marufs_file_ops;
	inode->i_mapping->a_ops = &marufs_aops;

	unlock_new_inode(inode);
	return inode;
}

/* ============================================================================
 * New inode creation
 * ============================================================================ */

/*
 * marufs_new_inode - create new VFS inode
 * @sb: Superblock
 * @mode: file mode (S_IFREG | permissions)
 *
 * Creates new in-memory inode before writing to index. Caller must fill in
 * region_id, slot_idx, entry_idx, shard_id.
 *
 * Return: inode pointer on success, ERR_PTR on error
 */
struct inode *marufs_new_inode(struct super_block *sb, umode_t mode)
{
	struct inode *inode;
	struct marufs_inode_info *xi;

	inode = new_inode(sb);
	if (!inode)
		return ERR_PTR(-ENOMEM);

	xi = MARUFS_I(inode);
	marufs_inode_info_init(xi);

	inode->i_ino = 0;
	inode->i_mode = mode;
	inode->i_uid = current_fsuid();
	inode->i_gid = current_fsgid();

	{
		struct timespec64 ts = current_time(inode);
		inode_set_atime_to_ts(inode, ts);
		inode_set_mtime_to_ts(inode, ts);
		inode_set_ctime_to_ts(inode, ts);
	}

	inode->i_blocks = 0;
	set_nlink(inode, 1);

	if (S_ISREG(mode)) {
		inode->i_op = &marufs_file_inode_ops;
		inode->i_fop = &marufs_file_ops;
		inode->i_mapping->a_ops = &marufs_aops;
	} else if (S_ISDIR(mode)) {
		inode->i_op = &marufs_dir_inode_ops;
		inode->i_fop = &marufs_dir_ops;
		set_nlink(inode, 2);
	}

	return inode;
}

/* ============================================================================
 * inode writeback
 * ============================================================================ */

/*
 * marufs_write_inode - write inode metadata back to index entry
 * @inode: inode to write
 * @wbc: writeback control
 *
 * Updates index entry with current file size and timestamp. Called by VFS
 * during writeback or sync operations.
 *
 * Return: 0 on success, negative error code on failure
 */
/*
 * marufs_index_entry_sync_size - sync file size and timestamp to CXL index
 * @sbi: superblock info
 * @xi: marufs inode info
 * @hot: hot entry pointer (must not be NULL)
 * @size: file size to write
 *
 * Writes file_size to hot entry, modified_at to cold entry, then issues WMB.
 */
static void marufs_index_entry_sync_size(struct marufs_sb_info *sbi,
					 struct marufs_inode_info *xi,
					 struct marufs_index_entry_hot *hot,
					 loff_t size)
{
	struct marufs_index_entry_cold *cold_e;

	WRITE_LE64(hot->file_size, size);
	cold_e = marufs_shard_cold_entry(sbi, xi->shard_id, xi->entry_idx);
	if (cold_e)
		WRITE_LE64(cold_e->modified_at, ktime_get_real_ns());
	MARUFS_CXL_WMB(hot, sizeof(*hot));
}

int marufs_write_inode(struct inode *inode, struct writeback_control *wbc)
{
	struct marufs_inode_info *xi = MARUFS_I(inode);
	struct marufs_sb_info *sbi = MARUFS_SB(inode->i_sb);
	struct marufs_index_entry_hot *hot;

	if (inode->i_ino == MARUFS_ROOT_INO)
		return 0;

	/* Acquire index entries (bounds-checked) */
	hot = marufs_shard_hot_entry(sbi, xi->shard_id, xi->entry_idx);
	if (!hot)
		return -EIO;

	/* Update file size (hot) and timestamp (cold) */
	marufs_index_entry_sync_size(sbi, xi, hot, inode->i_size);

	return 0;
}

/* ============================================================================
 * inode eviction
 * ============================================================================ */

/*
 * marufs_evict_inode - evict inode from cache
 * @inode: inode to evict
 *
 * Called when inode is removed from cache. Cleans up page cache and marks
 * inode as clean.
 */
void marufs_evict_inode(struct inode *inode)
{
	truncate_inode_pages_final(&inode->i_data);
	clear_inode(inode);
}

/* ============================================================================
 * File attribute operations
 * ============================================================================ */

/*
 * marufs_getattr - get file attributes
 *
 * Reads latest file size from index entry to ensure cross-node consistency,
 * then fills stat structure.
 */
static int marufs_getattr(MARUFS_IDMAP_PARAM_COMMA const struct path *path,
			  struct kstat *stat, u32 request_mask,
			  unsigned int query_flags)
{
	struct inode *inode = d_inode(path->dentry);
	struct marufs_inode_info *xi = MARUFS_I(inode);
	struct super_block *sb = inode->i_sb;
	struct marufs_sb_info *sbi = MARUFS_SB(sb);
	struct marufs_index_entry_hot *hot;

	if (inode->i_ino != MARUFS_ROOT_INO) {
		/* Read latest file_size from hot entry (bounds-checked) */
		hot = marufs_shard_hot_entry(sbi, xi->shard_id, xi->entry_idx);
		if (hot && READ_LE32(hot->state) == MARUFS_ENTRY_VALID) {
			inode->i_size = le64_to_cpu(hot->file_size);
			inode->i_blocks =
				(inode->i_size + MARUFS_SECTOR_SIZE - 1) >>
				MARUFS_SECTOR_SHIFT;
		}
	}

	marufs_generic_fillattr(MARUFS_IDMAP_ARG_COMMA request_mask, inode,
				stat);
	return 0;
}

/*
 * marufs_setattr - set file attributes (ftruncate triggers region allocation)
 *
 * WORM enforcement:
 *   - First ftruncate (i_size == 0 -> new size): allocates physical region
 *   - Second ftruncate (i_size > 0): rejected with -EACCES
 *
 * This is the key function in the two-phase create model:
 *   open(O_CREAT) creates lightweight entry (i_size=0)
 *   ftruncate(size) triggers marufs_region_init() here
 */
static int marufs_setattr(MARUFS_IDMAP_PARAM_COMMA struct dentry *dentry,
			  struct iattr *attr)
{
	struct inode *inode = d_inode(dentry);
	int ret;

	ret = marufs_setattr_prepare(MARUFS_IDMAP_ARG_COMMA dentry, attr);
	if (ret)
		return ret;

	if (attr->ia_valid & ATTR_SIZE) {
		struct marufs_inode_info *xi = MARUFS_I(inode);
		struct marufs_sb_info *sbi = MARUFS_SB(inode->i_sb);
		struct marufs_index_entry_hot *hot;

		/* MARUFS permission check before region allocation */
		ret = marufs_check_permission(sbi, xi->rat_entry_id,
					      MARUFS_PERM_WRITE);
		if (ret)
			return ret;

		/* WORM: reject size changes if file already has data */
		if (inode->i_size > 0)
			return -EACCES;

		/* No-op if setting to 0 */
		if (attr->ia_size == 0)
			return 0;

		/*
                 * First-time size set: allocate physical region.
                 * marufs_region_init finds contiguous space, initializes region header,
                 * and updates RAT entry with phys_offset and size.
                 */
		ret = marufs_region_init(sbi, xi->rat_entry_id, attr->ia_size);
		if (ret) {
			pr_err("region_init failed for rat_entry %u: %d\n",
			       xi->rat_entry_id, ret);
			return ret;
		}

		/* Update inode from initialized region */
		{
			struct marufs_rat_entry *rat_entry;
			u64 region_offset;

			rat_entry = marufs_rat_get(sbi, xi->rat_entry_id);
			if (!rat_entry)
				return -EIO;
			MARUFS_CXL_RMB(rat_entry, sizeof(*rat_entry));

			region_offset = READ_LE64(rat_entry->phys_offset);
			xi->region_offset = region_offset;

			/* v2: data starts directly at phys_offset */
			xi->data_phys_offset = region_offset;
		}

		/* Set inode size */
		truncate_setsize(inode, attr->ia_size);

		{
			struct timespec64 now = current_time(inode);
			inode_set_mtime_to_ts(inode, now);
			inode_set_ctime_to_ts(inode, now);
		}

		/* Update index entries (bounds-checked) */
		hot = marufs_shard_hot_entry(sbi, xi->shard_id, xi->entry_idx);
		if (hot)
			marufs_index_entry_sync_size(sbi, xi, hot,
						     inode->i_size);

		mark_inode_dirty(inode);

		pr_debug("ftruncate rat=%u size=%lld slot_base=0x%llx\n",
			 xi->rat_entry_id, inode->i_size, xi->data_phys_offset);
	}

	return 0;
}

/* ============================================================================
 * File inode operations table
 * ============================================================================ */

const struct inode_operations marufs_file_inode_ops = {
	.getattr = marufs_getattr,
	.setattr = marufs_setattr,
};
