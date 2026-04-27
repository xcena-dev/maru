/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * acl.h - Region-level access control entry points.
 */

#ifndef _MARUFS_ACL_H
#define _MARUFS_ACL_H

#include <linux/types.h>

struct marufs_sb_info;
struct marufs_deleg_entry;
struct marufs_perm_req;

void marufs_deleg_entry_clear(struct marufs_deleg_entry *de);
int marufs_check_permission(struct marufs_sb_info *sbi, u32 rat_entry_id,
			    u32 required_perms);
int marufs_deleg_grant(struct marufs_sb_info *sbi, u32 rat_entry_id,
		       struct marufs_perm_req *req);
bool marufs_owner_is_dead(u32 owner_pid, u64 owner_birth_time);

#endif /* _MARUFS_ACL_H */
