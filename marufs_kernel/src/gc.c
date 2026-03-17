// SPDX-License-Identifier: GPL-2.0-only
/* gc.c - MARUFS Tombstone Garbage Collection */

#include <asm/cmpxchg.h>
#include <linux/delay.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/kthread.h>
#include <linux/module.h>
#include <linux/pid.h>

#include "marufs.h"

#define MARUFS_GC_TOMBSTONE_THRESHOLD_PCT 25                  /* Trigger GC at 25% tombstones */
#define MARUFS_GC_INTERVAL_MS 10000                           /* Run GC every 10 seconds */
#define MARUFS_GC_INSERTING_TIMEOUT_NS (30ULL * NSEC_PER_SEC) /* 30s stale INSERTING */

/*
 * marufs_gc_needs_sweep - check if shard needs GC sweep
 * @sbi: superblock info
 * @shard_id: shard to check
 *
 * Returns true if tombstone+INSERTING percentage exceeds threshold.
 * sh->tombstone_entries tracks both TOMBSTONE and stale INSERTING counts.
 */
bool marufs_gc_needs_sweep(struct marufs_sb_info* sbi, u32 shard_id)
{
    u32 tombstones;
    struct marufs_shard_header* sh;

    if (!sbi || shard_id >= sbi->num_shards)
        return false;

    if (!sbi->entries_per_shard)
        return false;

    sh = &sbi->shard_table[shard_id];
    tombstones = READ_LE32(sh->tombstone_entries);

    if (tombstones == 0)
        return false;

    return (tombstones * 100 / sbi->entries_per_shard) >= MARUFS_GC_TOMBSTONE_THRESHOLD_PCT;
}

/*
 * marufs_gc_tombstone_sweep - clean up tombstone entries in shard
 * @sbi: superblock info
 * @shard_id: shard to clean
 *
 * Searches entry array for TOMBSTONE entries and changes them to EMPTY via CAS.
 * Note: does not unlink from bucket chain (avoids ABA problem).
 * Lookups naturally skip TOMBSTONE/EMPTY entries.
 *
 * Returns number of reclaimed entries, or negative on error.
 */
int marufs_gc_tombstone_sweep(struct marufs_sb_info* sbi, u32 shard_id)
{
    struct marufs_shard_header* sh;
    struct marufs_index_entry_hot* entries;
    struct marufs_index_entry_cold* cold_entries;
    u32 num_entries;
    u32 i;
    int tombstone_reclaimed = 0;
    int inserting_reclaimed = 0;

    if (!sbi || shard_id >= sbi->num_shards)
        return -EINVAL;

    sh = &sbi->shard_table[shard_id];
    num_entries = READ_LE32(sh->num_entries);
    if (unlikely(!marufs_shard_geometry_valid(1, num_entries)))
        return -EIO; /* Corrupted shard */

    entries = marufs_shard_hot_entries(sbi, shard_id);
    if (!entries)
        return -EINVAL;

    cold_entries = marufs_shard_cold_entries(sbi, shard_id);
    if (!cold_entries)
        return -EINVAL;

    for (i = 0; i < num_entries; i++)
    {
        struct marufs_index_entry_hot* e = &entries[i];
        __le32 state = READ_ONCE(e->state);

        if (state == MARUFS_ENTRY_TOMBSTONE_LE)
        {
            /* Clear fields BEFORE CAS (prevent stale data in reused slot) */
            memset(cold_entries[i].name, 0, sizeof(cold_entries[i].name));
            WRITE_LE64(e->name_hash, 0);
            MARUFS_CXL_WMB(e, sizeof(*e)); /* Ensure clears visible before state transition */

            /* CAS: TOMBSTONE -> EMPTY */
            if (cmpxchg(&e->state, MARUFS_ENTRY_TOMBSTONE_LE,
                        MARUFS_ENTRY_EMPTY_LE) ==
                MARUFS_ENTRY_TOMBSTONE_LE)
            {
                tombstone_reclaimed++;

                /* Lower free-slot hint so insert finds reclaimed slot faster */
                if (sbi->shard_free_hint)
                {
                    u32 cur = (u32)atomic_read(&sbi->shard_free_hint[shard_id]);
                    if (i < cur)
                        atomic_set(&sbi->shard_free_hint[shard_id], i);
                }
            }
        }
        else if (state == MARUFS_ENTRY_INSERTING_LE)
        {
            /*
             * Stale INSERTING: process crashed mid-insert.
             * If created_at==0, the inserter hasn't stamped it yet (or crashed
             * before writing). Stamp it now so timeout starts; the normal
             * inserter will overwrite with its own value immediately after.
             * Reclaim only after timeout expires (30s), avoiding false reclaim
             * of entries still being inserted.
             */
            u64 created_at = READ_LE64(cold_entries[i].created_at);
            u64 now = ktime_get_real_ns();

            if (created_at == 0)
            {
                WRITE_LE64(cold_entries[i].created_at, now);
                MARUFS_CXL_WMB(&cold_entries[i], sizeof(cold_entries[i]));
                continue; /* Re-check next GC cycle */
            }

            if ((now > created_at) &&
                ((now - created_at) > MARUFS_GC_INSERTING_TIMEOUT_NS))
            {
                /* Clear fields before CAS (same pattern as tombstone sweep) */
                memset(cold_entries[i].name, 0, sizeof(cold_entries[i].name));
                WRITE_LE64(e->name_hash, 0);
                MARUFS_CXL_WMB(e, sizeof(*e));

                if (cmpxchg(&e->state, MARUFS_ENTRY_INSERTING_LE,
                            MARUFS_ENTRY_EMPTY_LE) ==
                    MARUFS_ENTRY_INSERTING_LE)
                {
                    inserting_reclaimed++;

                    if (sbi->shard_free_hint)
                    {
                        u32 cur = (u32)atomic_read(&sbi->shard_free_hint[shard_id]);
                        if (i < cur)
                            atomic_set(&sbi->shard_free_hint[shard_id], i);
                    }

                    pr_debug("gc reclaimed stale INSERTING entry shard %u slot %u (age %lluns)\n",
                             shard_id, i, now - created_at);
                }
            }
        }
    }

    /* Update tombstone count — only subtract actual tombstone reclaims */
    if (tombstone_reclaimed > 0)
    {
        marufs_le32_cas_dec(&sh->tombstone_entries, (u32)tombstone_reclaimed);

        pr_debug("gc swept %d tombstones from shard %u\n",
                 tombstone_reclaimed, shard_id);
    }

    if (inserting_reclaimed > 0)
        pr_debug("gc reclaimed %d stale INSERTING entries from shard %u\n",
                 inserting_reclaimed, shard_id);

    return tombstone_reclaimed + inserting_reclaimed;
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
static int marufs_gc_sweep_dead_delegations(struct marufs_sb_info* sbi,
                                            struct marufs_rat_entry* rat_entry,
                                            u32 rat_entry_id)
{
    struct marufs_region_header* rhdr;
    struct marufs_deleg_table* dt;
    u32 num_entries;
    u32 i;
    int cleaned = 0;

    if (READ_LE64(rat_entry->phys_offset) == 0)
        return 0;

    rhdr = marufs_region_hdr(sbi, rat_entry_id);
    if (!rhdr)
        return 0;

    dt = &rhdr->deleg_table;
    MARUFS_CXL_RMB(dt, sizeof(*dt));

    if (READ_LE32(dt->magic) != MARUFS_DELEG_MAGIC)
        return 0;

    num_entries = READ_LE32(dt->max_entries);
    if (num_entries > MARUFS_DELEG_MAX_ENTRIES)
        num_entries = MARUFS_DELEG_MAX_ENTRIES;

    for (i = 0; i < num_entries; i++)
    {
        struct marufs_deleg_entry* de = &dt->entries[i];
        u32 de_node, de_pid;
        u64 de_birth;

        /* Reclaim stale GRANTING entries (grant crashed mid-write) */
        if (READ_LE32(de->state) == MARUFS_DELEG_GRANTING)
        {
            u64 granted_at = READ_LE64(de->granted_at);
            u64 now = ktime_get_real_ns();

            if (granted_at == 0 ||
                (now - granted_at) > MARUFS_GC_INSERTING_TIMEOUT_NS)
            {
                /* Clear fields before CAS to EMPTY to prevent
                 * race with new EMPTY->GRANTING claimer */
                WRITE_LE32(de->node_id, 0);
                WRITE_LE32(de->pid, 0);
                WRITE_LE32(de->perms, 0);
                WRITE_LE64(de->birth_time, 0);
                WRITE_LE64(de->granted_at, 0);
                MARUFS_CXL_WMB(de, sizeof(*de));
                if (CAS_LE32(&de->state, MARUFS_DELEG_GRANTING,
                             MARUFS_DELEG_EMPTY) == MARUFS_DELEG_GRANTING)
                {
                    cleaned++;
                    pr_debug("gc reclaimed stale GRANTING deleg entry %u\n", i);
                }
            }
            continue;
        }

        if (READ_LE32(de->state) != MARUFS_DELEG_ACTIVE)
            continue;

        de_node = READ_LE32(de->node_id);
        if (de_node != sbi->node_id)
            continue;

        de_pid = READ_LE32(de->pid);
        if (de_pid == 0)
            continue;

        de_birth = READ_LE64(de->birth_time);

        if (!marufs_owner_is_dead(de_pid, de_birth))
            continue;

        /* Clear fields before CAS to EMPTY to prevent
         * race with new EMPTY->GRANTING claimer */
        WRITE_LE32(de->node_id, 0);
        WRITE_LE32(de->pid, 0);
        WRITE_LE32(de->perms, 0);
        WRITE_LE64(de->birth_time, 0);
        WRITE_LE64(de->granted_at, 0);
        MARUFS_CXL_WMB(de, sizeof(*de));

        if (CAS_LE32(&de->state, MARUFS_DELEG_ACTIVE,
                     MARUFS_DELEG_EMPTY) != MARUFS_DELEG_ACTIVE)
            continue; /* Already cleared by another node */

        pr_debug("gc clearing dead delegation: node=%u pid=%u\n",
                 de_node, de_pid);

        /* Atomic counter decrement */
        {
            u32 old_count;
            do
            {
                old_count = READ_LE32(dt->num_entries);
                if (old_count == 0)
                    break;
            } while (CAS_LE32(&dt->num_entries, old_count,
                              old_count - 1) != old_count);
        }

        cleaned++;
    }

    return cleaned;
}

/*
 * marufs_gc_has_active_delegations - check if any active delegation entries exist
 * @sbi: superblock info
 * @rat_entry: RAT entry to check
 *
 * Returns true if at least one ACTIVE delegation entry exists in the
 * region's delegation table.
 */
bool marufs_gc_has_active_delegations(struct marufs_sb_info* sbi,
                                      struct marufs_rat_entry* rat_entry,
                                      u32 rat_entry_id)
{
    struct marufs_region_header* rhdr;
    struct marufs_deleg_table* dt;
    u32 num_entries;
    u32 i;

    if (READ_LE64(rat_entry->phys_offset) == 0)
        return false;

    rhdr = marufs_region_hdr(sbi, rat_entry_id);
    if (!rhdr)
        return false;

    dt = &rhdr->deleg_table;
    MARUFS_CXL_RMB(dt, sizeof(*dt));

    if (READ_LE32(dt->magic) != MARUFS_DELEG_MAGIC)
        return false;

    num_entries = READ_LE32(dt->max_entries);
    if (num_entries > MARUFS_DELEG_MAX_ENTRIES)
        num_entries = MARUFS_DELEG_MAX_ENTRIES;

    for (i = 0; i < num_entries; i++)
    {
        if (READ_LE32(dt->entries[i].state) == MARUFS_DELEG_ACTIVE)
            return true;
    }

    return false;
}

/*
 * marufs_is_orphaned - check if RAT entry has no live owner or active users
 * @sbi: superblock info
 * @rat_entry_id: RAT entry index
 *
 * A region is orphaned when:
 *   1. Entry is in ALLOCATED/DELETING/ALLOCATING state (not FREE)
 *   2. This node owns the entry
 *   3. Owner process is dead (pid=0 uses alloc_time timeout)
 *   4. No active delegations remain
 *
 * Used by both GC (dead-process reclaim) and unlink (orphaned fallback).
 * Returns true if orphaned (safe to reclaim/delete).
 */
bool marufs_is_orphaned(struct marufs_sb_info* sbi, u32 rat_entry_id)
{
    struct marufs_rat_entry* entry;
    u32 state, owner_node, owner_pid;
    u64 birth_time;

    entry = marufs_rat_get(sbi, rat_entry_id);
    if (!entry)
        return false;

    MARUFS_CXL_RMB(entry, sizeof(*entry));
    state = READ_LE32(entry->state);
    if (state == MARUFS_RAT_ENTRY_FREE)
        return true; /* Already reclaimed */
    if (state != MARUFS_RAT_ENTRY_ALLOCATED &&
        state != MARUFS_RAT_ENTRY_DELETING &&
        state != MARUFS_RAT_ENTRY_ALLOCATING)
        return false;

    owner_node = READ_LE32(entry->owner_node_id);
    owner_pid = READ_LE32(entry->owner_pid);
    birth_time = READ_LE64(entry->owner_birth_time);

    if (owner_node != sbi->node_id)
        return false;

    if (owner_pid == 0)
    {
        /* Fields not yet written (ALLOCATING) — use alloc_time timeout */
        u64 alloc_time = READ_LE64(entry->alloc_time);
        u64 now = ktime_get_real_ns();

        if (alloc_time == 0 ||
            (now - alloc_time) <= MARUFS_GC_INSERTING_TIMEOUT_NS)
            return false;
    }
    else if (!marufs_owner_is_dead(owner_pid, birth_time))
    {
        return false;
    }

    /* Skip if active delegations remain */
    if (marufs_gc_has_active_delegations(sbi, entry, rat_entry_id))
    {
        pr_debug("skipping RAT %u - active delegations remain\n", rat_entry_id);
        return false;
    }

    return true;
}

/*
 * marufs_gc_cleanup_rat_entry - full index + RAT cleanup for a reclaimable entry
 * @sbi: superblock info
 * @entry: RAT entry
 * @rat_entry_id: RAT entry index
 *
 * Removes all index references (name-refs + main entry) then frees the RAT entry.
 * Used by both normal dead-process reclaim and stuck DELETING recovery.
 */
static void marufs_gc_cleanup_rat_entry(struct marufs_sb_info* sbi,
                                        struct marufs_rat_entry* entry,
                                        u32 rat_entry_id)
{
    char name_buf[MARUFS_NAME_MAX + 1];
    size_t name_len;
    int ret;

    memcpy(name_buf, entry->name, MARUFS_NAME_MAX);
    name_buf[MARUFS_NAME_MAX] = '\0';
    name_len = strnlen(name_buf, MARUFS_NAME_MAX);

    ret = marufs_index_delete_by_region(sbi, rat_entry_id);
    if (ret && ret != -ENOENT)
    {
        pr_warn("gc index_delete_by_region failed for RAT %u: %d\n",
                rat_entry_id, ret);
        return; /* index entry still live — don't free RAT */
    }

    if (name_len > 0)
    {
        ret = marufs_index_delete(sbi, name_buf, name_len);
        if (ret && ret != -ENOENT)
            pr_warn("gc index_delete failed for RAT %u '%s': %d\n",
                    rat_entry_id, name_buf, ret);
    }

    marufs_rat_free_entry(sbi, rat_entry_id);
}

/*
 * marufs_gc_dead_process_regions - reclaim regions from dead processes
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
int marufs_gc_dead_process_regions(struct marufs_sb_info* sbi)
{
    u32 i;
    int reclaimed = 0;
    struct marufs_rat* rat;

    if (!sbi)
        return -EINVAL;

    /* RAT mode - reclaim RAT entries */
    if (!sbi->rat)
    {
        pr_info_ratelimited("gc: no RAT loaded, skipping dead-process scan\n");
        return 0;
    }

    rat = sbi->rat;

    for (i = 0; i < MARUFS_MAX_RAT_ENTRIES; i++)
    {
        struct marufs_rat_entry* entry = &rat->entries[i];
        u32 state;

        MARUFS_CXL_RMB(entry, sizeof(*entry));
        state = READ_LE32(entry->state);

        /*
         * Recover stuck transient states from crashed processes.
         * ALLOCATING: pre-index, safe to free directly.
         * DELETING: may have dangling index entries, needs full cleanup.
         */
        if (state == MARUFS_RAT_ENTRY_ALLOCATING)
        {
            if (!marufs_is_orphaned(sbi, i))
                continue;
            pr_info("gc recovering stuck ALLOCATING RAT entry %u (pid=%u)\n",
                    i, READ_LE32(entry->owner_pid));
            marufs_rat_free_entry(sbi, i);
            reclaimed++;
            continue;
        }

        if (state == MARUFS_RAT_ENTRY_DELETING)
        {
            if (!marufs_is_orphaned(sbi, i))
                continue;
            pr_info("gc recovering stuck DELETING RAT entry %u (pid=%u)\n",
                    i, READ_LE32(entry->owner_pid));
            marufs_gc_cleanup_rat_entry(sbi, entry, i);
            reclaimed++;
            continue;
        }

        if (state != MARUFS_RAT_ENTRY_ALLOCATED)
            continue;

        /* Sweep dead delegation entries for this node (runs on all regions) */
        marufs_gc_sweep_dead_delegations(sbi, entry, i);

        /* Check if entry is reclaimable (owner dead + no active delegations) */
        if (!marufs_is_orphaned(sbi, i))
            continue;

        /* CAS: ALLOCATED → DELETING (preempt — prevent race with unlink) */
        {
            u32 old_state = CAS_LE32(&entry->state,
                                     MARUFS_RAT_ENTRY_ALLOCATED,
                                     MARUFS_RAT_ENTRY_DELETING);
            if (old_state != MARUFS_RAT_ENTRY_ALLOCATED)
                continue; /* unlink already preempted */
        }

        pr_info("gc reclaiming RAT entry %u from dead process %u\n",
                i, READ_LE32(entry->owner_pid));
        marufs_gc_cleanup_rat_entry(sbi, entry, i);
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
static int marufs_gc_thread_fn(void* data)
{
    struct marufs_sb_info* sbi = data;
    pr_info("gc thread started for node %u\n", sbi->node_id);

    while (!kthread_should_stop())
    {
        /* Skip sweep when paused (e.g., during test runs) */
        if (!atomic_read(&sbi->gc_paused))
        {
            u32 s;

            /* Phase 1: Reclaim regions from dead processes */
            marufs_gc_dead_process_regions(sbi);

            /* Phase 2: Sweep tombstones + stale INSERTING from shards
             * exceeding threshold. INSERTING entries are counted in
             * sh->tombstone_entries so threshold naturally triggers. */
            for (s = 0; s < sbi->num_shards; s++)
            {
                if (marufs_gc_needs_sweep(sbi, s))
                    marufs_gc_tombstone_sweep(sbi, s);
            }

            /* Bump epoch so watchdog/sysfs can detect thread liveness */
            atomic_inc(&sbi->gc_epoch);
        }

        /* Sleep for interval or until woken/stopped */
        msleep_interruptible(MARUFS_GC_INTERVAL_MS);
    }

    pr_info("gc thread exiting for node %u (active=%d)\n",
            sbi->node_id, atomic_read(&sbi->gc_active));
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
int marufs_gc_start(struct marufs_sb_info* sbi)
{
    if (!sbi)
        return -EINVAL;

    if (sbi->gc_thread)
    {
        pr_warn("gc thread already running\n");
        return -EEXIST;
    }

    /* Hold module reference so rmmod waits for GC exit */
    if (!try_module_get(THIS_MODULE))
        return -ENODEV;

    atomic_set(&sbi->gc_active, 1);
    atomic_set(&sbi->gc_paused, 0);
    atomic_set(&sbi->gc_epoch, 0);

    sbi->gc_thread = kthread_run(marufs_gc_thread_fn, sbi,
                                 "marufs-gc-%u", sbi->node_id);
    if (IS_ERR(sbi->gc_thread))
    {
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
void marufs_gc_stop(struct marufs_sb_info* sbi)
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
int marufs_gc_restart(struct marufs_sb_info* sbi)
{
    if (!sbi)
        return -EINVAL;

    pr_info("gc restart requested for node %u\n", sbi->node_id);

    /* Stop existing thread cleanly (safe even if already dead) */
    marufs_gc_stop(sbi);

    return marufs_gc_start(sbi);
}
