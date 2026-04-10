// SPDX-License-Identifier: GPL-2.0-only
/*
 * acl.c - MARUFS Permission Delegation System
 *
 * Permission model:
 *   1. Owner (node_id + pid + birth_time) → implicit ALL permissions
 *   2. RAT default_perms → baseline for non-owners
 *   3. Delegation entries in RAT entry → per-entity grants
 *
 * Check order (fast path first):
 *   1. Owner? → allow (same as existing 3-stage ACL)
 *   2. RAT default_perms sufficient? → allow
 *   3. Delegation table match? → allow
 *   4. → deny (-EACCES)
 */

#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/pid.h>
#include <linux/sched.h>

#include "marufs.h"

/*
 * marufs_owner_is_dead - check if RAT entry owner process is dead
 * @owner_pid: PID from RAT entry
 * @owner_birth_time: birth time from RAT entry (le64 raw value)
 *
 * Returns true if the process is dead or PID was reused.
 */
bool marufs_owner_is_dead(u32 owner_pid, u64 owner_birth_time)
{
	struct pid *pid_s;
	struct task_struct *task;
	bool dead = true;

	if (owner_pid == 0)
		return false; /* pid not yet written (ALLOCATING) — find_get_pid(0) returns init */

	pid_s = find_get_pid(owner_pid);
	if (!pid_s)
		return true;

	task = get_pid_task(pid_s, PIDTYPE_PID);
	if (task) {
		u64 bt = ktime_to_ns(task->start_boottime);
		if (bt == owner_birth_time)
			dead = false;
		put_task_struct(task);
	}
	put_pid(pid_s);
	return dead;
}

/*
 * marufs_is_owner - check if current process is the owner of a RAT entry
 * @sbi: superblock info
 * @rat_entry: RAT entry to check
 *
 * 3-stage ACL: node_id → pid → birth_time
 * Returns true if current process is the owner.
 */
static bool marufs_is_owner(struct marufs_sb_info *sbi,
			    struct marufs_rat_entry *rat_entry)
{
	u32 owner_node, owner_pid;
	u64 owner_birth_time, current_birth_time;

	/* Invalidate CL2 (ACL fields) before reading */
	MARUFS_CXL_RMB(&rat_entry->default_perms, 64);

	/* Stage 1: node_id check */
	owner_node = READ_LE16(rat_entry->owner_node_id);
	if (owner_node != sbi->node_id)
		return false;

	/* Stage 2: pid check */
	owner_pid = READ_LE32(rat_entry->owner_pid);
	if (owner_pid != current->pid)
		return false;

	/* Stage 3: birth_time check (PID reuse protection) */
	owner_birth_time = READ_LE64(rat_entry->owner_birth_time);
	current_birth_time = ktime_to_ns(current->start_boottime);

	return owner_birth_time == current_birth_time;
}

/*
 * marufs_deleg_matches - check if delegation entry grants required perms
 *
 * Exact match only: (node_id, pid) with birth_time verification.
 * pid=0 wildcard matching is no longer supported (rejected at grant time).
 */
static bool marufs_deleg_matches(struct marufs_deleg_entry *de, u32 node_id,
				 u32 required_perms)
{
	u32 de_node = READ_LE32(de->node_id);
	u32 de_pid = READ_LE32(de->pid);
	u32 de_perms = READ_LE32(de->perms);

	/* Permission bits must be sufficient */
	if ((de_perms & required_perms) != required_perms)
		return false;

	/* Exact match: (node_id, pid) with birth_time verification */
	if (de_node != node_id || de_pid != current->pid)
		return false;

	/* PID reuse protection via birth_time */
	{
		u64 de_birth = READ_LE64(de->birth_time);
		u64 cur_birth = ktime_to_ns(current->start_boottime);

		if (de_birth != 0 && de_birth != cur_birth)
			return false;
	}

	return true;
}

/*
 * marufs_deleg_entry_clear - zero all data fields of a delegation entry
 * @de: delegation entry pointer
 *
 * Clears node_id, pid, perms, birth_time, granted_at and issues a WMB.
 * Does NOT touch de->state — caller is responsible for the state transition
 * (CAS or direct write depending on ownership context).
 */
void marufs_deleg_entry_clear(struct marufs_deleg_entry *de)
{
	WRITE_LE32(de->node_id, 0);
	WRITE_LE32(de->pid, 0);
	WRITE_LE32(de->perms, 0);
	WRITE_LE64(de->birth_time, 0);
	WRITE_LE64(de->granted_at, 0);
	MARUFS_CXL_WMB(de, sizeof(*de));
}

/*
 * marufs_check_permission - unified permission check
 * @sbi: superblock info
 * @rat_entry_id: RAT entry ID
 * @required_perms: permission bitmask (MARUFS_PERM_*)
 *
 * Check order:
 *   1. Owner → allow ALL (fast path, same as existing 3-stage ACL)
 *   2. RAT default_perms → allow if sufficient
 *   3. Delegation table → scan for matching entry
 *   4. Deny
 *
 * Returns 0 if allowed, -EACCES if denied, -EINVAL on bad input.
 */
int marufs_check_permission(struct marufs_sb_info *sbi, u32 rat_entry_id,
			    u32 required_perms)
{
	struct marufs_rat_entry *rat_entry;
	u32 default_perms;
	u32 i;

	if (!sbi)
		return -EINVAL;

	rat_entry = marufs_rat_entry_get(sbi, rat_entry_id);
	if (!rat_entry)
		return -EINVAL;

	/* Verify RAT entry is still allocated before reading fields */
	if (READ_LE32(rat_entry->state) != MARUFS_RAT_ENTRY_ALLOCATED)
		return -EACCES;

	/* Fast path: owner has ALL permissions */
	if (marufs_is_owner(sbi, rat_entry))
		return 0;

	/* Invalidate CL2 again to cover deleg_num_entries and deleg_entries */
	MARUFS_CXL_RMB(&rat_entry->default_perms, 64);

	/* Check RAT default_perms */
	default_perms = READ_LE16(rat_entry->default_perms);
	if ((default_perms & required_perms) == required_perms)
		return 0;

	/* Check delegation entries directly in RAT entry */
	for (i = 0; i < MARUFS_DELEG_MAX_ENTRIES; i++) {
		struct marufs_deleg_entry *de =
			marufs_rat_deleg_entry(rat_entry, i);
		if (!de)
			continue;
		if (READ_LE32(de->state) != MARUFS_DELEG_ACTIVE)
			continue;

		if (marufs_deleg_matches(de, sbi->node_id, required_perms)) {
			/* Lazy birth_time init on first access by delegated process */
			if (READ_LE64(de->birth_time) == 0) {
				marufs_le64_cas(
					&de->birth_time, 0,
					ktime_to_ns(current->start_boottime));
				MARUFS_CXL_WMB(de, sizeof(*de));
			}
			return 0;
		}
	}

	return -EACCES;
}

/*
 * marufs_deleg_grant - grant permissions to (node_id, pid)
 * @sbi: superblock info
 * @rat_entry_id: RAT entry ID
 * @req: permission request (node_id, pid, birth_time, perms)
 *
 * Upsert: if matching entry exists, OR the new perms in.
 * Otherwise, find an empty slot and create a new entry.
 *
 * Only owner or ADMIN-delegated callers should invoke this
 * (caller must check before calling).
 *
 * Returns 0 on success, negative error code on failure.
 */
/*
 * marufs_deleg_try_upsert - scan entries for existing match or first free slot
 * @deleg_entries: delegation entry array (from rat_entry->deleg_entries)
 * @num_entries: max entries in table
 * @req: permission request
 * @out_free_idx: output first free slot index (MARUFS_DELEG_MAX_ENTRIES if none)
 *
 * Return: 0 if upserted existing entry, 1 if free slot found, -ENOSPC if full
 */
static int marufs_deleg_try_upsert(struct marufs_deleg_entry *deleg_entries,
				   u32 num_entries, struct marufs_perm_req *req,
				   u32 *out_free_idx)
{
	u32 i;
	u32 free_idx = MARUFS_DELEG_MAX_ENTRIES;

	for (i = 0; i < num_entries; i++) {
		struct marufs_deleg_entry *de = &deleg_entries[i];
		u32 state;

		MARUFS_CXL_RMB(de, sizeof(*de));
		state = READ_LE32(de->state);

		if (state == MARUFS_DELEG_EMPTY) {
			if (free_idx == MARUFS_DELEG_MAX_ENTRIES)
				free_idx = i;
			continue;
		}

		if (state != MARUFS_DELEG_ACTIVE)
			continue;

		/* Check for existing match: same (node_id, pid) */
		if (READ_LE32(de->node_id) == req->node_id &&
		    READ_LE32(de->pid) == req->pid) {
			/* Upsert: CAS loop to atomically OR in new permissions */
			u32 old_perms, new_perms;
			do {
				old_perms = READ_LE32(de->perms);
				new_perms = old_perms | req->perms;
				if (new_perms == old_perms)
					break; /* Already has all requested bits */
			} while (marufs_le32_cas(&de->perms, old_perms,
						 new_perms) != old_perms);
			WRITE_LE64(de->granted_at, ktime_get_real_ns());
			MARUFS_CXL_WMB(de, sizeof(*de));
			return 0;
		}
	}

	*out_free_idx = free_idx;
	return (free_idx < num_entries) ? 1 : -ENOSPC;
}

#define MARUFS_DELEG_GRANT_MAX_RETRIES 3

int marufs_deleg_grant(struct marufs_sb_info *sbi, u32 rat_entry_id,
		       struct marufs_perm_req *req)
{
	struct marufs_rat_entry *rat_entry;
	int attempt;

	if (!sbi || !req)
		return -EINVAL;

	if (req->perms == 0 || (req->perms & ~MARUFS_PERM_ALL))
		return -EINVAL;

	if (req->node_id == 0 || req->pid == 0)
		return -EINVAL;

	rat_entry = marufs_rat_entry_get(sbi, rat_entry_id);
	if (!rat_entry)
		return -EINVAL;

	if (READ_LE32(rat_entry->state) != MARUFS_RAT_ENTRY_ALLOCATED)
		return -EINVAL;

	MARUFS_CXL_RMB(&rat_entry->default_perms, 64);

	for (attempt = 0; attempt < MARUFS_DELEG_GRANT_MAX_RETRIES; attempt++) {
		struct marufs_deleg_entry *de;
		u32 free_idx;
		u32 old_state;
		int ret;

		/* Scan for existing match (upsert) or free slot */
		ret = marufs_deleg_try_upsert(rat_entry->deleg_entries,
					      MARUFS_DELEG_MAX_ENTRIES, req,
					      &free_idx);
		if (ret == 0)
			return 0; /* Upserted existing entry */
		if (ret == -ENOSPC)
			return -ENOSPC;

		/* ret == 1: free slot found — try to claim it */
		de = marufs_rat_deleg_entry(rat_entry, free_idx);
		if (!de)
			return -EINVAL;

		/* CAS: EMPTY → GRANTING (reserve slot, fields not yet visible) */
		old_state = marufs_le32_cas(&de->state, MARUFS_DELEG_EMPTY,
					    MARUFS_DELEG_GRANTING);
		if (old_state != MARUFS_DELEG_EMPTY) {
			cpu_relax();
			continue; /* Lost race — rescan */
		}

		/* Slot claimed in GRANTING state — fill fields before publish */
		WRITE_LE32(de->node_id, req->node_id);
		WRITE_LE32(de->pid, req->pid);
		WRITE_LE32(de->perms, req->perms);
		WRITE_LE64(de->birth_time,
			   0); /* Filled on first access by delegated process */
		WRITE_LE64(de->granted_at, ktime_get_real_ns());
		MARUFS_CXL_WMB(
			de,
			sizeof(*de)); /* Ensure all fields visible before state transition */

		/* Publish: GRANTING → ACTIVE (now safe for readers) */
		WRITE_LE32(de->state, MARUFS_DELEG_ACTIVE);
		MARUFS_CXL_WMB(de, sizeof(*de));

		marufs_le16_cas_inc(&rat_entry->deleg_num_entries);

		return 0;
	}

	return -EAGAIN;
}
