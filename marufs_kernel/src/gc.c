// SPDX-License-Identifier: GPL-2.0-only
/* gc.c - MARUFS Tombstone Garbage Collection */

#include <linux/delay.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/kthread.h>
#include <linux/module.h>
#include <linux/pid.h>

#include "marufs.h"

#define MARUFS_GC_INTERVAL_MS 10000 /* Run GC every 10 seconds */
#define MARUFS_GC_SHARD_DIVISOR \
	4 /* Round-robin: sweep ~25% of shards per cycle (min 1) */

/*
 * Admin node orphan policy
 *
 * Entries with node_id==0 are crash orphans — the writer died before
 * stamping its node_id.  No node's "node_id == sbi->node_id" filter
 * matches them, so the normal GC path cannot reach them.
 *
 * node_id==1 is the designated admin node.  Only the admin node
 * tracks node_id==0 orphans in the local DRAM tracker and later
 * CAS-claims them (node_id 0→1, timestamp=now).  Once claimed,
 * the next GC cycle's normal path reclaims them like any other
 * dead-process resource owned by node 1.
 *
 * This eliminates CAS contention that would occur if every node
 * raced to claim the same orphan entries.
 *
 * Affected paths:
 *   - marufs_is_stale_inserting()   (index.c)  — node_id==0 → admin only
 *   - nrht_is_stale()               (nrht.c)   — node_id==0 → admin only
 *   - marufs_gc_reclaim_dead_regions()          — ALLOCATING + node_id==0
 *   - marufs_gc_sweep_dead_delegations()        — GRANTING + granted_at==0
 */

/*
 * marufs_entry_reclaim_slot - reclaim stale INSERTING entry to TOMBSTONE
 * @entry: index entry pointer
 *
 * Reclaims to TOMBSTONE (not EMPTY) because the entry may already be
 * linked to a bucket chain (crash between link_to_bucket and publish).
 * A chain-in EMPTY would let flat scan claim it for a different bucket,
 * corrupting the original chain. TOMBSTONE is safe — reused in-place
 * by the next insert on the same bucket via check_duplicate.
 *
 * Returns true if slot was reclaimed.
 */
static bool marufs_entry_reclaim_slot(struct marufs_index_entry *entry)
{
	WRITE_LE64(entry->name_hash, 0);
	MARUFS_CXL_WMB(entry, sizeof(*entry));
	return marufs_le32_cas(&entry->state, MARUFS_ENTRY_INSERTING,
			       MARUFS_ENTRY_TOMBSTONE) == MARUFS_ENTRY_INSERTING;
}

/*
 * marufs_gc_track_orphan - track orphaned entry in DRAM
 * @sbi: superblock info
 * @entry: CXL entry pointer (index, deleg, or RAT)
 * @type: entry type for dispatch during sweep
 *
 * Tracks entry locally with discovery timestamp. Does NOT write to CXL
 * to avoid cacheline clobbering with live writers on other nodes/CPUs.
 *
 * Thread safety: gc_orphans[] and gc_orphan_count are only accessed by the
 * single GC kthread — no locking required.
 */
void marufs_gc_track_orphan(struct marufs_sb_info *sbi, void *entry,
			   enum marufs_orphan_type type)
{
	/* Already tracked? Skip duplicate registration */
	u32 i;
	for (i = 0; i < sbi->gc_orphan_count; i++) {
		if (sbi->gc_orphans[i].entry == entry)
			return;
	}

	/* Full — retry next cycle after sweep frees slots */
	if (sbi->gc_orphan_count >= MARUFS_GC_ORPHAN_MAX)
		return;

	i = sbi->gc_orphan_count++;
	sbi->gc_orphans[i].entry = entry;
	sbi->gc_orphans[i].discovered_at = ktime_get_real_ns();
	sbi->gc_orphans[i].type = type;
}

/*
 * marufs_gc_is_orphan_stuck - check if tracked entry is still in expected state
 * Returns true if entry is still stuck (eligible for reclaim on timeout).
 * Returns false if state changed (should be removed from tracker).
 */
static bool marufs_gc_is_orphan_stuck(void *entry,
					 enum marufs_orphan_type type)
{
	MARUFS_CXL_RMB(entry, 64); /* Invalidate CL0 (state & node_id field) */

	switch (type) {
	case MARUFS_ORPHAN_INDEX: {
		struct marufs_index_entry *e = entry;
		return READ_CXL_LE32(e->state) == MARUFS_ENTRY_INSERTING &&
		       READ_CXL_LE32(e->node_id) == 0;
	}
	case MARUFS_ORPHAN_DELEG: {
		struct marufs_deleg_entry *de = entry;
		return READ_CXL_LE32(de->state) == MARUFS_DELEG_GRANTING &&
		       (READ_CXL_LE32(de->node_id) == 0 ||
			READ_CXL_LE64(de->granted_at) == 0);
	}
	case MARUFS_ORPHAN_DELEG_UNBOUND: {
		struct marufs_deleg_entry *de = entry;
		return READ_CXL_LE32(de->state) == MARUFS_DELEG_ACTIVE &&
		       READ_CXL_LE64(de->birth_time) == 0;
	}
	case MARUFS_ORPHAN_RAT: {
		struct marufs_rat_entry *re = entry;
		/* Invalidate CL2 (owner_node_id field) */
		MARUFS_CXL_RMB(&re->owner_node_id, 64);
		return READ_LE32(re->state) == MARUFS_RAT_ENTRY_ALLOCATING &&
		       READ_LE16(re->owner_node_id) == 0;
	}
	case MARUFS_ORPHAN_NRHT: {
		struct marufs_nrht_entry *e = entry;
		return READ_CXL_LE32(e->state) == MARUFS_ENTRY_INSERTING &&
		       READ_CXL_LE32(e->inserter_node) == 0;
	}
	default:
		return false;
	}
}

/*
 * marufs_gc_claim_orphan - claim ownership of a timed-out orphan entry
 * @sbi: superblock info (for node_id)
 *
 * CAS node_id from 0 to this node, then stamp a fresh timestamp.
 * Actual reclaim happens in the next GC cycle via the normal path
 * (is_stale_inserting / sweep_dead_delegations / is_orphaned),
 * which gives a second timeout window before reclaim.
 *
 * Returns true if claimed (or already has node_id for DELEG).
 */
static bool marufs_gc_claim_orphan(struct marufs_sb_info *sbi, void *entry,
				   enum marufs_orphan_type type)
{
	u64 now = ktime_get_real_ns();

	switch (type) {
	case MARUFS_ORPHAN_INDEX: {
		struct marufs_index_entry *e = entry;

		if (marufs_le32_cas(&e->node_id, 0, sbi->node_id) != 0)
			return false;
		WRITE_LE64(e->created_at, now);
		MARUFS_CXL_WMB(e, sizeof(*e));
		return true;
	}
	case MARUFS_ORPHAN_DELEG: {
		struct marufs_deleg_entry *de = entry;
		u32 de_node = READ_CXL_LE32(de->node_id);

		if (de_node == 0) {
			if (marufs_le32_cas(&de->node_id, 0, sbi->node_id) != 0)
				return false;
		} else if (de_node != sbi->node_id) {
			return false; /* Another node owns this entry */
		}
		WRITE_LE64(de->granted_at, now);
		MARUFS_CXL_WMB(de, sizeof(*de));
		return true;
	}
	case MARUFS_ORPHAN_DELEG_UNBOUND: {
		struct marufs_deleg_entry *de = entry;
		/*
		 * Stamp sentinel birth_time=1 so normal sweep's
		 * owner_is_dead(pid, 1) will flag it for reclaim.
		 * If the real process already bound, CAS fails — harmless.
		 */
		if (marufs_le64_cas(&de->birth_time, 0, 1) != 0)
			return false;
		MARUFS_CXL_WMB(de, sizeof(*de));
		return true;
	}
	case MARUFS_ORPHAN_RAT: {
		struct marufs_rat_entry *re = entry;

		if (marufs_le16_cas(&re->owner_node_id, 0, sbi->node_id) != 0)
			return false;
		WRITE_LE64(re->alloc_time, now);
		MARUFS_CXL_WMB(re, sizeof(*re));
		return true;
	}
	case MARUFS_ORPHAN_NRHT: {
		struct marufs_nrht_entry *e = entry;

		if (marufs_le32_cas(&e->inserter_node, 0, sbi->node_id) != 0)
			return false;
		WRITE_LE64(e->created_at, now);
		MARUFS_CXL_WMB(e, 64);
		return true;
	}
	default:
		return false;
	}
}

/*
 * marufs_gc_sweep_orphans - reclaim timed-out orphaned entries
 * @sbi: superblock info
 *
 * Walks the DRAM orphan tracker. For each entry:
 *   1. If state changed (no longer stuck) → remove from tracker
 *   2. If timeout expired → reclaim, then remove
 *   3. Otherwise → keep tracking
 *
 * Returns number of reclaimed entries.
 */
static int marufs_gc_sweep_orphans(struct marufs_sb_info *sbi)
{
	int reclaimed = 0;
	u32 i = 0;
	u64 now = ktime_get_real_ns();

	while (i < sbi->gc_orphan_count) {
		struct marufs_orphan_tracker *t = &sbi->gc_orphans[i];

		if (!marufs_gc_is_orphan_stuck(t->entry, t->type))
			goto remove;

		if (now > t->discovered_at &&
		    (now - t->discovered_at) > MARUFS_STALE_TIMEOUT_NS) {
			if (marufs_gc_claim_orphan(sbi, t->entry, t->type))
				reclaimed++;
			goto remove;
		}

		i++;
		continue;
remove:
		sbi->gc_orphans[i] = sbi->gc_orphans[--sbi->gc_orphan_count];
	}

	return reclaimed;
}

/*
 * marufs_is_stale_inserting - check if INSERTING entry is stale and reclaimable
 * @sbi: superblock info
 * @e: index entry (caller verified state == INSERTING)
 * @shard_id: shard containing the entry
 * @slot_idx: slot index within shard
 *
 * Policy:
 *   - node_id == this node: check created_at timeout
 *   - node_id == 0 (orphan): track in DRAM, never write to CXL
 *   - node_id == other node: skip
 *   - created_at == 0 (same node): use DRAM tracker instead of CXL stamp
 *
 * Returns:
 *   1  = stale + reclaimable → caller may reclaim
 *   0  = not yet stale (tracked/deferred)
 *  -1  = not this node's entry
 */
/*
 * Pure staleness check — no side effects (no DRAM tracking).
 * Safe to call from any context (GC thread, syscall path).
 * GC tombstone_sweep() handles DRAM tracking for return 0 cases.
 */
static int marufs_is_stale_inserting(struct marufs_sb_info *sbi,
				     struct marufs_index_entry *e)
{
	u32 inserter_node = READ_CXL_LE32(e->node_id);
	u64 created_at, now;

	if (inserter_node == 0)
		return (sbi->node_id == 1) ? 0 : -1; /* Only admin node tracks orphans */

	if (inserter_node != sbi->node_id)
		return -1;

	/* Same node — check CXL timestamp */
	created_at = READ_CXL_LE64(e->created_at);
	if (created_at == 0)
		return 0; /* Timestamp not yet visible — can't determine */

	now = ktime_get_real_ns();
	if (now > created_at && (now - created_at) > MARUFS_STALE_TIMEOUT_NS)
		return 1;

	return 0;
}

/*
 * marufs_gc_sweep_stale_entries - reclaim stale INSERTING entries in shard
 * @sbi: superblock info
 * @shard_id: shard to clean
 *
 * TOMBSTONE entries are NOT converted to EMPTY here.  Without chain unlink,
 * converting TOMBSTONE→EMPTY allows the flat EMPTY scan to claim the slot
 * for a different bucket, corrupting the original bucket's chain.  Instead,
 * TOMBSTONE entries are reused in-place by the insert path (same-bucket
 * chain reuse in __marufs_index_insert step 3a).  Proper TOMBSTONE→EMPTY
 * with chain unlink will be added with token-ring (PR 2).
 *
 * Stale INSERTING entries (crashed mid-insert) are reclaimed to TOMBSTONE
 * after timeout via CAS(INSERTING → TOMBSTONE).
 *
 * Returns number of reclaimed entries, or negative on error.
 */
static int marufs_gc_sweep_stale_entries(struct marufs_sb_info *sbi, u32 shard_id)
{
	struct marufs_shard_header *sh = marufs_shard_header_get(sbi, shard_id);
	if (!sh)
		return -EINVAL;

	u32 num_entries = READ_LE32(sh->num_entries);
	int inserting_reclaimed = 0;

	for (u32 i = 0; i < num_entries; i++) {
		struct marufs_index_entry *entry =
			marufs_shard_entry(sbi, shard_id, i);
		if (!entry)
			continue;

		u32 state = READ_CXL_LE32(entry->state);
		if (state == MARUFS_ENTRY_INSERTING) {
			int ret = marufs_is_stale_inserting(sbi, entry);
			if (ret == 1) {
				if (marufs_entry_reclaim_slot(entry))
					inserting_reclaimed++;
			} else if (ret == 0) {
				marufs_gc_track_orphan(sbi, entry,
						       MARUFS_ORPHAN_INDEX);
			}
		}
	}

	if (inserting_reclaimed > 0)
		pr_debug(
			"gc reclaimed %d stale INSERTING entries from shard %u\n",
			inserting_reclaimed, shard_id);

	return inserting_reclaimed;
}

/*
 * marufs_gc_sweep_dead_delegations - clean dead delegation entries for this node
 * @sbi: superblock info
 * @rat_entry: RAT entry whose delegation table to scan
 *
 * Scans the delegation table in the region header. For entries where
 * node_id matches this node, checks if the delegated process is still alive.
 * Dead entries are marked EMPTY.
 *
 * Returns number of entries cleaned, or negative on error.
 */
static int marufs_gc_sweep_dead_delegations(struct marufs_sb_info *sbi,
					    struct marufs_rat_entry *rat_entry)
{
	int cleaned = 0;

	if (READ_CXL_LE32(rat_entry->state) != MARUFS_RAT_ENTRY_ALLOCATED)
		return 0;

	for (u32 i = 0; i < MARUFS_DELEG_MAX_ENTRIES; i++) {
		struct marufs_deleg_entry *de =
			marufs_rat_deleg_entry(rat_entry, i);
		if (!de)
			continue;

		u32 de_state = READ_CXL_LE32(de->state);

		switch (de_state) {
		case MARUFS_DELEG_EMPTY:
			break;

		case MARUFS_DELEG_GRANTING: {
			u64 granted_at = READ_CXL_LE64(de->granted_at);

			if (granted_at == 0) {
				u32 de_node = READ_CXL_LE32(de->node_id);

				if (de_node != 0 || sbi->node_id == 1)
					marufs_gc_track_orphan(sbi, de,
							       MARUFS_ORPHAN_DELEG);
			} else if (ktime_get_real_ns() - granted_at >
				   MARUFS_STALE_TIMEOUT_NS) {
				if (marufs_le32_cas(&de->state,
						    MARUFS_DELEG_GRANTING,
						    MARUFS_DELEG_EMPTY) ==
				    MARUFS_DELEG_GRANTING)
					cleaned++;
			}
			break;
		}
		case MARUFS_DELEG_ACTIVE: {
			u32 de_node = READ_CXL_LE32(de->node_id);
			u32 de_pid = READ_CXL_LE32(de->pid);
			u64 de_birth = READ_CXL_LE64(de->birth_time);

			if (de_node != sbi->node_id || de_pid == 0)
				break;

			/*
			 * birth_time == 0: lazy init not yet done by delegated
			 * process.  Cannot use owner_is_dead() because it would
			 * compare real birth_time against 0 and false-positive.
			 * Use granted_at timeout instead (same pattern as GRANTING).
			 */
			if (de_birth == 0) {
				marufs_gc_track_orphan(sbi, de,
						       MARUFS_ORPHAN_DELEG_UNBOUND);
				break;
			}

			if (!marufs_owner_is_dead(de_pid, de_birth))
				break;

			if (marufs_le32_cas(&de->state, MARUFS_DELEG_ACTIVE,
					    MARUFS_DELEG_EMPTY) ==
			    MARUFS_DELEG_ACTIVE) {
				marufs_le16_cas_dec(
					&rat_entry->deleg_num_entries);
				cleaned++;
			}
			break;
		}
		}
	}

	return cleaned;
}

/*
 * marufs_has_active_delegations - check if any active delegation entries exist
 * @rat_entry: RAT entry to check
 *
 * Returns true if at least one ACTIVE delegation entry exists.
 */
static bool marufs_has_active_delegations(struct marufs_rat_entry *rat_entry)
{
	if (READ_CXL_LE64(rat_entry->phys_offset) == 0)
		return false;

	for (u32 i = 0; i < MARUFS_DELEG_MAX_ENTRIES; i++) {
		struct marufs_deleg_entry *de =
			marufs_rat_deleg_entry(rat_entry, i);
		if (!de)
			continue;
		if (READ_LE32(de->state) == MARUFS_DELEG_ACTIVE)
			return true;
	}

	return false;
}

/*
 * marufs_is_orphaned - check if RAT entry owner is dead with no active users
 * @entry: RAT entry pointer (caller must validate and filter by node_id)
 *
 * Returns true when:
 *   1. Owner process is dead (pid=0 uses alloc_time timeout)
 *   2. No active delegations remain
 *
 * Pure predicate — no side effects. Caller handles DRAM tracking
 * for alloc_time=0 case (pid=0, timestamp not yet written).
 */
static bool marufs_is_orphaned(struct marufs_rat_entry *entry)
{
	MARUFS_CXL_RMB(&entry->default_perms, 64);

	u32 owner_pid = READ_LE32(entry->owner_pid);
	u64 birth_time = READ_LE64(entry->owner_birth_time);

	if (owner_pid == 0) {
		u64 alloc_time = READ_LE64(entry->alloc_time);

		if (alloc_time == 0)
			return false; /* Timestamp not yet written */

		u64 now = ktime_get_real_ns();
		if (now <= alloc_time ||
		    (now - alloc_time) <= MARUFS_STALE_TIMEOUT_NS)
			return false;
	} else if (!marufs_owner_is_dead(owner_pid, birth_time)) {
		return false;
	}

	if (marufs_has_active_delegations(entry))
		return false;

	return true;
}

/*
 * marufs_can_force_unlink - check if an unowned RAT entry can be force-deleted
 * @sbi: superblock info
 * @rat_entry_id: RAT entry index
 *
 * For dir.c unlink fallback: allows deleting a file whose owner is dead
 * and has no active delegations, without requiring DELETE permission.
 */
bool marufs_can_force_unlink(struct marufs_sb_info *sbi, u32 rat_entry_id)
{
	struct marufs_rat_entry *entry =
		marufs_rat_entry_get(sbi, rat_entry_id);
	if (!entry)
		return false;
	if (READ_LE16(entry->owner_node_id) != sbi->node_id)
		return false;
	return marufs_is_orphaned(entry);
}

/*
 * marufs_gc_cleanup_rat_entry - full index + RAT cleanup for a reclaimable entry
 * @sbi: superblock info
 * @entry: RAT entry
 *
 * Removes index entry by name, then frees the RAT entry.
 */
static void marufs_gc_cleanup_rat_entry(struct marufs_sb_info *sbi,
					struct marufs_rat_entry *entry)
{
	char name_buf[MARUFS_NAME_MAX + 1];
	size_t name_len;
	int ret;

	MARUFS_CXL_RMB(entry->name, sizeof(entry->name));
	memcpy(name_buf, entry->name, MARUFS_NAME_MAX);
	name_buf[MARUFS_NAME_MAX] = '\0';
	name_len = strnlen(name_buf, MARUFS_NAME_MAX);

	if (name_len > 0) {
		ret = marufs_index_delete(sbi, name_buf, name_len);
		if (ret && ret != -ENOENT)
			pr_warn("gc index_delete failed for '%s': %d\n",
				name_buf, ret);
	}

	marufs_rat_free_entry(entry);
}

/*
 * marufs_gc_reclaim_dead_regions - reclaim regions from dead processes
 * @sbi: superblock info
 *
 * Three-phase GC for each ALLOCATED RAT entry:
 *   Phase A: Sweep dead delegation entries for this node (all regions)
 *   Phase B: Filter to regions owned by this node with dead owner
 *   Phase C: Skip reclaim if active delegations remain
 *
 * A region is only reclaimed when the owner is dead AND all delegation
 * entries have been cleared (by each node's GC sweeping its own entries).
 *
 * Returns number of reclaimed regions.
 */
int marufs_gc_reclaim_dead_regions(struct marufs_sb_info *sbi)
{
	u32 i;
	int reclaimed = 0;

	if (!sbi)
		return -EINVAL;

	for (i = 0; i < MARUFS_MAX_RAT_ENTRIES; i++) {
		struct marufs_rat_entry *entry = marufs_rat_entry_get(sbi, i);
		if (!entry) {
			pr_info_ratelimited(
				"gc: no RAT loaded, skipping dead-process scan\n");
			return 0;
		}

		u32 state = READ_LE32(entry->state);
		if (state == MARUFS_RAT_ENTRY_FREE) {
			clear_bit(i, sbi->gc_nrht_bitmap);
			continue;
		}

		/* Maintain NRHT bitmap for Phase 4 */
		if (state == MARUFS_RAT_ENTRY_ALLOCATED &&
		    READ_LE32(entry->region_type) == MARUFS_REGION_NRHT)
			set_bit(i, sbi->gc_nrht_bitmap);
		else
			clear_bit(i, sbi->gc_nrht_bitmap);

		/* Sweep dead delegation entries for this node (runs on all regions) */
		marufs_gc_sweep_dead_delegations(sbi, entry);

		/* Orphan with no owner node (crash before node_id written) */
		u16 owner_node = READ_LE16(entry->owner_node_id);
		if (owner_node == 0 && state == MARUFS_RAT_ENTRY_ALLOCATING) {
			if (sbi->node_id == 1)
				marufs_gc_track_orphan(sbi, entry, MARUFS_ORPHAN_RAT);
			continue;
		}

		/* Remaining logic is owner-node only */
		if (owner_node != sbi->node_id)
			continue;

		if (!marufs_is_orphaned(entry)) {
			/* Track ALLOCATING with alloc_time not yet written */
			if (state == MARUFS_RAT_ENTRY_ALLOCATING &&
			    READ_LE32(entry->owner_pid) == 0)
				marufs_gc_track_orphan(sbi, entry,
						       MARUFS_ORPHAN_RAT);
			continue;
		}

		/* ALLOCATED needs CAS to DELETING first (race with unlink) */
		if (state == MARUFS_RAT_ENTRY_ALLOCATED) {
			u32 old_state = marufs_le32_cas(
				&entry->state, MARUFS_RAT_ENTRY_ALLOCATED,
				MARUFS_RAT_ENTRY_DELETING);
			if (old_state != MARUFS_RAT_ENTRY_ALLOCATED)
				continue; /* unlink already preempted */
		}

		pr_info("gc reclaiming RAT entry %u (state=%u, pid=%u)\n", i,
			state, READ_LE32(entry->owner_pid));
		marufs_gc_cleanup_rat_entry(sbi, entry);
		reclaimed++;
	}

	return reclaimed;
}

/*
 * marufs_gc_thread_fn - background GC thread function
 * @data: pointer to marufs_sb_info
 *
 * Runs periodically to:
 *   1. Reclaim regions from dead processes
 *   2. Sweep tombstone entries from shards exceeding threshold
 * Thread runs every MARUFS_GC_INTERVAL_MS (10 seconds).
 */
static int marufs_gc_thread_fn(void *data)
{
	struct marufs_sb_info *sbi = data;
	pr_info("gc thread started for node %u\n", sbi->node_id);

	while (!kthread_should_stop()) {
		msleep_interruptible(MARUFS_GC_INTERVAL_MS);

		if (atomic_read(&sbi->gc_paused))
			continue;

		u32 shards_per_cycle =
			max(sbi->num_shards / MARUFS_GC_SHARD_DIVISOR, 1U);

		/* Phase 1: Reclaim regions from dead processes */
		marufs_gc_reclaim_dead_regions(sbi);

		/* Phase 2: Round-robin sweep of stale INSERTING entries */
		for (u32 s = 0; s < shards_per_cycle; s++) {
			marufs_gc_sweep_stale_entries(sbi, sbi->gc_next_shard);
			sbi->gc_next_shard =
				(sbi->gc_next_shard + 1) % sbi->num_shards;
		}

		/* Phase 3: Reclaim timed-out orphaned entries from DRAM tracker */
		marufs_gc_sweep_orphans(sbi);

		/* Phase 4: Sweep stale INSERTING entries in NRHT regions */
		marufs_nrht_gc_sweep_all(sbi);

		atomic_inc(&sbi->gc_epoch);
	}

	pr_info("gc thread exiting for node %u (active=%d)\n", sbi->node_id,
		atomic_read(&sbi->gc_active));
	return 0;
}

/*
 * marufs_gc_start - start background GC thread
 * @sbi: superblock info
 *
 * Called during mount. Creates a kernel thread that periodically
 * reclaims regions from dead processes.
 *
 * Returns 0 on success, negative on error.
 */
int marufs_gc_start(struct marufs_sb_info *sbi)
{
	if (!sbi)
		return -EINVAL;

	if (sbi->gc_thread) {
		pr_warn("gc thread already running\n");
		return -EEXIST;
	}

	/* Hold module reference so rmmod waits for GC exit */
	if (!try_module_get(THIS_MODULE))
		return -ENODEV;

	atomic_set(&sbi->gc_active, 1);
	atomic_set(&sbi->gc_paused, 0);
	atomic_set(&sbi->gc_epoch, 0);
	sbi->gc_orphan_count = 0;

	sbi->gc_thread = kthread_run(marufs_gc_thread_fn, sbi, "marufs-gc-%u",
				     sbi->node_id);
	if (IS_ERR(sbi->gc_thread)) {
		int ret = PTR_ERR(sbi->gc_thread);
		sbi->gc_thread = NULL;
		atomic_set(&sbi->gc_active, 0);
		module_put(THIS_MODULE);
		pr_err("failed to start gc thread: %d\n", ret);
		return ret;
	}

	pr_info("gc thread started (interval=%dms)\n", MARUFS_GC_INTERVAL_MS);
	return 0;
}

/*
 * marufs_gc_stop - stop background GC thread
 * @sbi: superblock info
 *
 * Called during unmount. Stops the background GC thread.
 */
void marufs_gc_stop(struct marufs_sb_info *sbi)
{
	if (!sbi || !sbi->gc_thread)
		return;

	pr_info("stopping gc thread for node %u\n", sbi->node_id);
	atomic_set(&sbi->gc_active, 0);
	kthread_stop(sbi->gc_thread);
	sbi->gc_thread = NULL;
	module_put(THIS_MODULE);
}

/*
 * marufs_gc_restart - restart GC thread (safe even if thread crashed)
 * @sbi: superblock info
 *
 * Stops existing thread (if any) and starts a fresh one.
 * kthread_stop() is safe on crashed kthreads (task stays zombie
 * because kthread_create holds a reference).
 */
int marufs_gc_restart(struct marufs_sb_info *sbi)
{
	if (!sbi)
		return -EINVAL;

	pr_info("gc restart requested for node %u\n", sbi->node_id);

	/* Stop existing thread cleanly (safe even if already dead) */
	marufs_gc_stop(sbi);

	return marufs_gc_start(sbi);
}
