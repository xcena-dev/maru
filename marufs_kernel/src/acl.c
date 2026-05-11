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
#include <linux/kdev_t.h>
#include <linux/kernel.h>
#include <linux/mm_types.h>
#include <linux/pid.h>
#include <linux/sched.h>
#include <linux/sched/mm.h>

#include "marufs.h"
#include "me.h"

/*
 * marufs_get_exe_id - read current process's exe binary identity.
 *
 * Used by post-exec privilege retention defense: bind ownership /
 * delegation to (inode, dev) of the exe binary so that an execve() into
 * a different binary fails subsequent permission checks.
 *
 * Returns true with @out_ino / @out_dev populated, or false if there is
 * no exe (kernel thread) — caller should treat as deny.
 */
bool marufs_get_exe_id(u64 *out_ino, u32 *out_dev)
{
	struct mm_struct *mm = current->mm;
	struct file *exe = mm ? READ_ONCE(mm->exe_file) : NULL;

	if (!exe || !exe->f_inode)
		return false;

	*out_ino = exe->f_inode->i_ino;
	*out_dev = new_encode_dev(exe->f_inode->i_sb->s_dev);
	return true;
}

/*
 * marufs_check_exe_id - check current process's exe identity matches.
 *
 * Returns true iff @ino and @dev equal the current task's exe inode/dev.
 * Returns false if no exe (kernel thread) or mismatch.
 */
bool marufs_check_exe_id(u64 ino, u32 dev)
{
	u64 cur_ino;
	u32 cur_dev;

	if (!marufs_get_exe_id(&cur_ino, &cur_dev))
		return false;
	return cur_ino == ino && cur_dev == dev;
}

/*
 * marufs_get_pid_identity - read identity (birth_time + exe id) of an
 * arbitrary local PID.
 *
 * Same-node only: looks up the target task on the current host and reads
 * (start_boottime, exe inode, exe dev). Used by delegation grant to
 * eagerly bind a same-node delegation to the target's identity (closes
 * the lazy-init timing window where an attacker could hijack the target
 * between grant and first mmap). Cross-node delegations cannot use this —
 * caller must fall back to lazy-init.
 *
 * Returns true with all out params populated on success. False if the
 * PID is not found, the task has no mm, or no exe.
 */
bool marufs_get_pid_identity(u32 target_pid, u64 *out_birth, u64 *out_ino,
			     u32 *out_dev)
{
	if (target_pid == 0)
		return false;

	struct pid *pid_s = find_get_pid(target_pid);
	if (!pid_s)
		return false;

	bool ok = false;
	struct task_struct *task = get_pid_task(pid_s, PIDTYPE_PID);
	if (task) {
		if (task->mm) {
			struct file *exe = READ_ONCE(task->mm->exe_file);
			if (exe && exe->f_inode) {
				*out_birth = ktime_to_ns(task->start_boottime);
				*out_ino = exe->f_inode->i_ino;
				*out_dev = new_encode_dev(
					exe->f_inode->i_sb->s_dev);
				ok = true;
			}
		}
		put_task_struct(task);
	}
	put_pid(pid_s);
	return ok;
}

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
 * 4-stage ACL: node_id → pid → birth_time → exe_inode
 * The exe_inode stage defends against post-exec privilege retention:
 * an attacker that execve()s a different binary keeps PID + birth_time
 * but ends up with a different exe inode/dev — fails the check.
 *
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
	if (owner_birth_time != current_birth_time)
		return false;

	/* Stage 4: exe_inode check (post-exec privilege retention defense) */
	u64 owner_ino = READ_LE64(rat_entry->owner_exe_inode_ino);
	u32 owner_dev = READ_LE32(rat_entry->owner_exe_inode_dev);

	return marufs_check_exe_id(owner_ino, owner_dev);
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
	WRITE_LE64(de->exe_inode_ino, 0);
	WRITE_LE32(de->exe_inode_dev, 0);
	MARUFS_CXL_WMB(de, sizeof(*de));
}

/*
 * marufs_check_permission_any - compute granted subset of candidate perms
 * @sbi: superblock info
 * @rat_entry_id: RAT entry ID
 * @candidate: permission bitmask to test (MARUFS_PERM_*, OR-combined)
 * @out_granted: granted subset (intersection of candidate with caller's rights)
 *
 * ANY-semantics: returns the actual rights held by the caller within the
 * requested candidate set. Caller branches on which bits matched (e.g.,
 * PERM_GRANT: ADMIN can grant anything, GRANT cannot propagate ADMIN/GRANT).
 *
 * Aggregation:
 *   1. Owner → *out_granted = candidate (full match, fast path)
 *   2. Otherwise: (default_perms ∪ matched deleg entries) & candidate
 *
 * Returns 0 on success, -EINVAL on bad input. State != ALLOCATED yields
 * 0 with *out_granted=0 (caller maps to -EACCES).
 *
 * marufs_check_permission() is a thin AND-semantics wrapper over this.
 */
int marufs_check_permission_any(struct marufs_sb_info *sbi, u32 rat_entry_id,
				u32 candidate, u32 *out_granted)
{
	struct marufs_rat_entry *rat_entry =
		marufs_rat_entry_get(sbi, rat_entry_id);
	if (!rat_entry || !out_granted || candidate == 0)
		return -EINVAL;

	/* Verify RAT entry is still allocated before reading fields */
	if (READ_LE32(rat_entry->state) != MARUFS_RAT_ENTRY_ALLOCATED)
		return 0;

	/* Fast path: owner has ALL permissions */
	*out_granted = 0;
	if (marufs_is_owner(sbi, rat_entry)) {
		*out_granted = candidate;
		return 0;
	}

	/* Invalidate CL2 to cover default_perms, deleg_num_entries, deleg_entries */
	MARUFS_CXL_RMB(&rat_entry->default_perms, 64);
	u32 default_perms = READ_LE16(rat_entry->default_perms);
	u32 have = 0;
	have |= default_perms & candidate;
	if (have == candidate) {
		*out_granted = have;
		return 0;
	}

	for (u32 i = 0; i < rat_entry->deleg_num_entries; i++) {
		struct marufs_deleg_entry *de =
			marufs_rat_deleg_entry(rat_entry, i);
		if (!de)
			continue;
		if (READ_LE32(de->state) != MARUFS_DELEG_ACTIVE)
			continue;

		u32 de_node = READ_LE32(de->node_id);
		u32 de_pid = READ_LE32(de->pid);
		if (de_node != sbi->node_id || de_pid != current->pid)
			continue;

		u64 de_birth = READ_LE64(de->birth_time);
		u64 cur_birth = ktime_to_ns(current->start_boottime);
		if (de_birth == 0) {
			/* Lazy init on first access by matching process */
			marufs_le64_cas(&de->birth_time, 0, cur_birth);
			MARUFS_CXL_WMB(de, sizeof(*de));
		} else if (de_birth != cur_birth) {
			continue; /* PID reuse */
		}

		/*
		 * Bind delegation to the caller's exe binary identity.
		 * Defends against post-exec privilege retention: an attacker
		 * that execve()s a different binary (same PID + birth_time,
		 * possibly inherited fd) ends up with a different exe
		 * inode/dev — entry no longer matches.
		 *
		 * Lazy-init mirrors birth_time: de->exe_inode_ino == 0 ⇒
		 * "first match"; capture and CAS-bind. Subsequent calls
		 * compare; mismatch ⇒ skip this entry.
		 */
		u64 cur_ino;
		u32 cur_dev;
		if (!marufs_get_exe_id(&cur_ino, &cur_dev))
			continue; /* kernel thread / no exe — never matches */

		u64 de_ino = READ_LE64(de->exe_inode_ino);
		u32 de_dev = READ_LE32(de->exe_inode_dev);
		if (de_ino == 0) {
			marufs_le64_cas(&de->exe_inode_ino, 0, cur_ino);
			marufs_le32_cas(&de->exe_inode_dev, 0, cur_dev);
			MARUFS_CXL_WMB(de, sizeof(*de));
		} else if (de_ino != cur_ino || de_dev != cur_dev) {
			continue; /* exe binary changed (post-exec attack) */
		}

		u32 de_perms = READ_LE32(de->perms);
		u32 grant = de_perms & candidate;
		if (!grant)
			continue;

		have |= grant;
		if (have == candidate)
			break;
	}

	*out_granted = have;
	return 0;
}

/*
 * marufs_check_permission - AND-semantics permission check
 *
 * Returns 0 if every bit in @required_perms is granted, -EACCES if any
 * bit missing, -EINVAL on bad input.
 *
 * Thin wrapper over marufs_check_permission_any().
 */
int marufs_check_permission(struct marufs_sb_info *sbi, u32 rat_entry_id,
			    u32 required_perms)
{
	u32 have;
	int ret = marufs_check_permission_any(sbi, rat_entry_id, required_perms,
					      &have);
	if (ret)
		return ret;
	return have == required_perms ? 0 : -EACCES;
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

/*
 * Caller MUST hold the Global ME for MARUFS_ME_GLOBAL_SHARD_ID. With the ME
 * held, this function is the sole state-field mutator for this rat_entry's
 * delegation array — no retry/CAS-dance is needed.
 *
 * Readers (check_permission) only CAS `de->birth_time` and are gated by the
 * state-machine (GRANTING → WMB → ACTIVE), so plain writes + the intermediate
 * WMB are sufficient for visibility.
 */
int marufs_deleg_grant(struct marufs_sb_info *sbi, u32 rat_entry_id,
		       struct marufs_perm_req *req)
{
	if (!sbi || !req)
		return -EINVAL;

	if (req->perms == 0 || (req->perms & ~MARUFS_PERM_ALL))
		return -EINVAL;

	if (req->node_id == 0 || req->pid == 0)
		return -EINVAL;

	struct marufs_rat_entry *rat_entry =
		marufs_rat_entry_get(sbi, rat_entry_id);
	if (!rat_entry ||
	    READ_CXL_LE32(rat_entry->state) != MARUFS_RAT_ENTRY_ALLOCATED)
		return -EINVAL;

	MARUFS_CXL_RMB(&rat_entry->default_perms, 64);

	/* Single pass: upsert hit, no space, or free slot to claim. */
	u32 free_idx;
	int ret = marufs_deleg_try_upsert(rat_entry->deleg_entries,
					  MARUFS_DELEG_MAX_ENTRIES, req,
					  &free_idx);
	if (ret <= 0)
		return ret; /* 0 = upserted, -ENOSPC = full */

	struct marufs_deleg_entry *de =
		marufs_rat_deleg_entry(rat_entry, free_idx);
	if (!de)
		return -EINVAL;

	/* Reserve slot visibly so readers racing by see GRANTING, not stale ACTIVE. */
	WRITE_LE32(de->state, MARUFS_DELEG_GRANTING);
	MARUFS_CXL_WMB(&de->state, sizeof(de->state));

	WRITE_LE32(de->node_id, req->node_id);
	WRITE_LE32(de->pid, req->pid);
	WRITE_LE32(de->perms, req->perms);

	/*
	 * Identity binding (birth_time + exe_inode):
	 *   same-node target → eager (resolve target's identity now, closes
	 *                      the grant→first-mmap timing window where an
	 *                      attacker could hijack the target via execve)
	 *   cross-node target → lazy (kernel can't reach remote task; the
	 *                      consumer's first mmap captures it)
	 */
	u64 t_birth = 0, t_ino = 0;
	u32 t_dev = 0;
	if (req->node_id == sbi->node_id)
		marufs_get_pid_identity(req->pid, &t_birth, &t_ino, &t_dev);
	WRITE_LE64(de->birth_time, t_birth);
	WRITE_LE64(de->exe_inode_ino, t_ino);
	WRITE_LE32(de->exe_inode_dev, t_dev);

	WRITE_LE64(de->granted_at, ktime_get_real_ns());
	MARUFS_CXL_WMB(de, sizeof(*de));

	/* Publish — ensured fields are globally visible before state transition. */
	WRITE_LE32(de->state, MARUFS_DELEG_ACTIVE);
	MARUFS_CXL_WMB(&de->state, sizeof(de->state));

	marufs_le16_cas_inc(&rat_entry->deleg_num_entries);
	return 0;
}
