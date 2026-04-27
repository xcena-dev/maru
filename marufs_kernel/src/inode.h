/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * inode.h - In-memory inode state and inode entry points.
 *
 * `marufs_inode_info` lives here so any layer needing per-file metadata
 * (region/shard/owner) can include just this header. `vfs_inode` MUST
 * remain the last field (container_of contract).
 */

#ifndef _MARUFS_INODE_H
#define _MARUFS_INODE_H

#include <linux/fs.h>
#include <linux/types.h>

struct marufs_index_entry;

struct marufs_inode_info {
	u32 region_id; /* Region where data is stored (= RAT entry ID) */
	u32 entry_idx; /* Global index entry index (for writeback) */
	u32 shard_id; /* Shard this entry belongs to */
	u32 rat_entry_id;
	u64 region_offset;
	u32 owner_node_id;
	u32 owner_pid;
	u64 owner_birth_time;
	u64 data_phys_offset; /* Cached: region phys_offset (= data start) */
	struct inode vfs_inode; /* VFS inode (must be last!) */
};

static inline struct marufs_inode_info *marufs_inode_get(struct inode *inode)
{
	return container_of(inode, struct marufs_inode_info, vfs_inode);
}

/* Zero-initialize all marufs-specific fields of inode_info. */
static inline void marufs_inode_info_init(struct marufs_inode_info *xi)
{
	xi->region_id = 0;
	xi->entry_idx = 0;
	xi->shard_id = 0;
	xi->rat_entry_id = 0;
	xi->region_offset = 0;
	xi->owner_node_id = 0;
	xi->owner_pid = 0;
	xi->owner_birth_time = 0;
	xi->data_phys_offset = 0;
}

struct inode *marufs_iget(struct super_block *sb,
			  struct marufs_index_entry *entry, u32 shard_id,
			  u32 entry_idx);
struct inode *marufs_new_inode(struct super_block *sb, umode_t mode);
int marufs_write_inode(struct inode *inode, struct writeback_control *wbc);
void marufs_evict_inode(struct inode *inode);

extern const struct inode_operations marufs_file_inode_ops;
extern const struct inode_operations marufs_dir_inode_ops;

#endif /* _MARUFS_INODE_H */
