// SPDX-License-Identifier: GPL-2.0-only
/*
 * index.c - MARUFS Global Index CAS operations
 *
 * Lock-free global index for file metadata. Index is sharded by filename hash,
 * with each shard containing a hash-bucket array and flat entry array.
 * All mutations use CAS (cmpxchg) on entry state fields and bucket head
 * pointers for multi-node safety.
 *
 * Entry state machine:
 *   EMPTY  --CAS-->  INSERTING  --WRITE_ONCE-->  VALID  --CAS-->  TOMBSTONE
 *
 * Bucket chains are singly-linked via next_in_bucket, terminating with
 * MARUFS_BUCKET_END (0xFFFFFFFF).
 */

#include <asm/cmpxchg.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/prefetch.h>
#include <linux/slab.h>
#include <linux/string.h>
#include <linux/types.h>

#include "marufs.h"

/* Maximum CAS retries for bucket chain insertion */
#define MARUFS_INDEX_CAS_RETRIES 2048


/*
 * marufs_index_check_duplicate - check for duplicate name in bucket chain
 * @entries: shard hot entry array
 * @buckets: shard bucket array
 * @bucket_idx: bucket to check
 * @num_entries: total entries in shard
 * @hash: name hash to match
 * @name: filename to match
 * @namelen: filename length
 *
 * Return: 0 if no duplicate, -EEXIST if name already exists
 */
static int marufs_index_check_duplicate(struct marufs_index_entry_hot* entries,
                                        struct marufs_index_entry_cold* cold_entries,
                                        u32* buckets, u32 bucket_idx,
                                        u32 num_entries, u64 hash,
                                        const char* name, size_t namelen)
{
    u32 cur = READ_LE32(buckets[bucket_idx]);
    u32 steps = 0;

    while (cur != MARUFS_BUCKET_END && cur < num_entries)
    {
        struct marufs_index_entry_hot* e = &entries[cur];
        __le32 st = READ_ONCE(e->state);

        if (++steps > num_entries)
        {
            pr_err("bucket chain cycle detected (dup check, bucket %u)\n",
                   bucket_idx);
            return -EIO;
        }

        MARUFS_CXL_RMB(e, sizeof(*e));
        if ((st == MARUFS_ENTRY_VALID_LE ||
             st == MARUFS_ENTRY_INSERTING_LE) &&
            READ_LE64(e->name_hash) == hash)
        {
            struct marufs_index_entry_cold* c = &cold_entries[cur];

            MARUFS_CXL_RMB(c, sizeof(*c));
            if (strncmp(c->name, name, namelen) == 0 &&
                (namelen >= sizeof(c->name) || c->name[namelen] == '\0'))
            {
                /* Stale INSERTING: reclaim inline if older than timeout */
                if (st == MARUFS_ENTRY_INSERTING_LE)
                {
                    u64 created_at = READ_LE64(c->created_at);
                    u64 now = ktime_get_real_ns();

                    if ((created_at != 0 && now > created_at) &&
                        ((now - created_at) > MARUFS_STALE_TIMEOUT_NS))
                    {
                        /* Clear fields BEFORE CAS to prevent concurrent
                         * reader seeing EMPTY state with stale data */
                        memset(c->name, 0, sizeof(c->name));
                        WRITE_LE64(e->name_hash, 0);
                        MARUFS_CXL_WMB(e, sizeof(*e));

                        if (cmpxchg(&e->state, MARUFS_ENTRY_INSERTING_LE,
                                    MARUFS_ENTRY_EMPTY_LE) ==
                            MARUFS_ENTRY_INSERTING_LE)
                        {
                            pr_debug("reclaimed stale INSERTING '%.*s' inline\n",
                                     (int)namelen, name);
                            continue; /* Cleared — no longer a duplicate */
                        }
                    }
                }

                pr_debug("index_insert: name '%.*s' already exists\n",
                         (int)namelen, name);
                return -EEXIST;
            }
        }
        cur = READ_LE32(e->next_in_bucket);
    }

    return 0;
}

/*
 * marufs_index_link_and_publish - link entry to bucket and publish
 * @entry: entry to link (in INSERTING state)
 * @entry_idx: index of entry
 * @buckets: shard bucket array
 * @bucket_idx: target bucket
 *
 * Links entry to bucket chain head via CAS, then transitions state from
 * INSERTING to VALID.
 *
 * Return: 0 on success, -EAGAIN if CAS retries exhausted
 */
static int marufs_index_link_and_publish(struct marufs_index_entry_hot* entry,
                                         u32 entry_idx, u32* buckets,
                                         u32 bucket_idx)
{
    __le32 old_head;
    int retries;

    /*
     * Link to bucket chain via CAS on bucket head.
     * Prepend: new entry becomes head, old head becomes our next.
     */
    retries = 0;
    for (;;)
    {
        old_head = READ_ONCE(buckets[bucket_idx]);

        WRITE_ONCE(entry->next_in_bucket, old_head);
        MARUFS_CXL_WMB(entry, sizeof(*entry));

        if (cmpxchg(&buckets[bucket_idx], old_head,
                    (__force __le32)entry_idx) == old_head)
            break;

        if (++retries >= MARUFS_INDEX_CAS_RETRIES)
        {
            /*
             * Extremely rare: revert entry to EMPTY for slot reuse.
             */
            pr_err("index_insert: bucket CAS failed after %d retries, reverting\n",
                   retries);
            WRITE_ONCE(entry->state, MARUFS_ENTRY_EMPTY_LE);
            MARUFS_CXL_WMB(entry, sizeof(*entry));
            return -EAGAIN;
        }
        cpu_relax();
    }

    /*
     * Finalize - transition INSERTING -> VALID.
     * Entry is now visible to readers.
     */
    MARUFS_CXL_WMB(entry, sizeof(*entry));
    WRITE_ONCE(entry->state, MARUFS_ENTRY_VALID_LE);
    MARUFS_CXL_WMB(entry, sizeof(*entry));

    return 0;
}

/*
 * marufs_index_insert - insert new file into global index
 * @sbi: superblock info
 * @name: filename
 * @namelen: filename length
 * @region_id: region where data is allocated (= RAT entry ID)
 * @file_size: initial file size (usually 0)
 * @uid: owner UID
 * @gid: owner GID
 * @mode: POSIX mode bits
 * @out_entry_idx: output - global entry index within shard
 *
 * 3-phase CAS protocol:
 *   1. CAS entry state EMPTY -> INSERTING (reserve slot)
 *   2. CAS bucket head to link entry into hash chain
 *   3. WRITE_ONCE state to VALID (publish)
 *
 * Return: 0 on success, -ENOSPC if shard full, -EEXIST if name exists
 */
/* Internal insert with caller-supplied hash */
static int __marufs_index_insert(struct marufs_sb_info* sbi, const char* name,
                                 size_t namelen, u64 hash, u32 region_id,
                                 u64 file_size, u32 uid, u32 gid, u16 mode,
                                 u16 flags, u32* out_entry_idx)
{
    struct marufs_shard_cache* sc;
    u32 shard_id, bucket_idx;
    struct marufs_index_entry_hot* hot_entries;
    struct marufs_index_entry_cold* cold_entries;
    u32* buckets;
    u32 num_entries;
    u32 entry_idx;
    struct marufs_index_entry_hot* hot;
    struct marufs_index_entry_cold* cold;
    __le32 old_state;
    int ret;

    /* Step 1: select shard + bucket from hash (shard_cache = local DRAM) */
    shard_id = marufs_shard_id(hash, sbi->shard_mask);
    sc = &sbi->shard_cache[shard_id];
    num_entries = sc->num_entries;

    if (unlikely(!marufs_shard_geometry_valid(sc->num_buckets, num_entries)))
    {
        pr_err("shard %u corrupted (buckets=%u entries=%u) - reformat needed\n",
               shard_id, sc->num_buckets, num_entries);
        return -EIO;
    }

    bucket_idx = marufs_bucket_idx(hash, sc->num_buckets);
    hot_entries = sc->hot_entries;
    cold_entries = sc->cold_entries;
    buckets = sc->buckets;

    if (!hot_entries || !cold_entries || !buckets)
    {
        pr_err("index_insert: NULL entries/buckets for shard %u\n",
               shard_id);
        return -EINVAL;
    }

    /*
     * Step 2: check for duplicate name before reserving slot.
     * Walk bucket chain to find existing VALID entry with same name.
     *
     * NOTE: This pre-insert check has a TOCTOU window — two nodes can both
     * pass it and insert the same name. Post-insert dedup after step 6
     * detects and resolves this: the loser (higher entry_idx) rolls back
     * to TOMBSTONE and returns -EEXIST.
     */
    ret = marufs_index_check_duplicate(hot_entries, cold_entries,
                                       buckets, bucket_idx,
                                       num_entries, hash, name, namelen);
    if (ret)
        return ret;

    /*
     * Step 3: scan hot entry array to find EMPTY slot and reserve it via
     * CAS EMPTY -> INSERTING.  Start from shard_free_hint to skip
     * known-occupied prefix (H-P1: O(1) amortized vs O(n) linear).
     */
    hot = NULL;
    cold = NULL;
    {
        u32 hint = 0;
        u32 scan;

        if (sbi->shard_free_hint)
            hint = (u32)atomic_read(&sbi->shard_free_hint[shard_id]);
        if (hint >= num_entries)
            hint = 0;

        for (scan = 0; scan < num_entries; scan++)
        {
            struct marufs_index_entry_hot* e;

            entry_idx = hint + scan;
            if (entry_idx >= num_entries)
                entry_idx -= num_entries;
            e = &hot_entries[entry_idx];

            old_state = READ_ONCE(e->state);
            if (old_state != MARUFS_ENTRY_EMPTY_LE)
                continue;

            /* Try CAS: EMPTY -> INSERTING */
            if (cmpxchg(&e->state, MARUFS_ENTRY_EMPTY_LE,
                        MARUFS_ENTRY_INSERTING_LE) ==
                MARUFS_ENTRY_EMPTY_LE)
            {
                struct marufs_shard_header* sh;
                hot = e;
                cold = &cold_entries[entry_idx];
                /* Stamp created_at immediately so GC can detect stale INSERTING */
                WRITE_LE64(cold->created_at, ktime_get_real_ns());
                MARUFS_CXL_WMB(cold, sizeof(*cold));
                /* Count INSERTING as GC candidate (decremented on VALID transition) */
                sh = &sbi->shard_table[shard_id];
                marufs_le32_cas_inc(&sh->tombstone_entries);
                /* Advance hint past claimed slot */
                if (sbi->shard_free_hint)
                    atomic_set(&sbi->shard_free_hint[shard_id],
                               (entry_idx + 1 < num_entries) ? entry_idx + 1 : 0);
                break;
            }
        }
    }

    if (!hot)
    {
        pr_debug("index_insert: shard %u full (%u entries)\n",
                 shard_id, num_entries);
        return -ENOSPC;
    }

    /*
     * Step 4: fill hot + cold entry fields. In INSERTING state we own it
     * exclusively - no other node will touch it.
     */
    {
        u64 now_ns = ktime_get_real_ns();
        size_t copy_len;

        /* Hot entry fields */
        WRITE_LE64(hot->name_hash, hash);
        WRITE_LE32(hot->region_id, region_id);
        WRITE_LE64(hot->file_size, file_size);
        WRITE_LE16(hot->flags, flags);
        hot->_pad0 = 0;

        /* Cold entry fields — name lives in cold */
        memset(cold->name, 0, sizeof(cold->name));
        copy_len = namelen;
        if (copy_len > sizeof(cold->name) - 1)
            copy_len = sizeof(cold->name) - 1;
        memcpy(cold->name, name, copy_len);

        WRITE_LE32(cold->uid, uid);
        WRITE_LE32(cold->gid, gid);
        WRITE_LE16(cold->mode, mode);
        cold->_pad0 = 0;
        /* created_at already stamped after CAS(EMPTY→INSERTING) for GC */
        WRITE_LE64(cold->modified_at, now_ns);
    }

    /* Ensure all field writes are visible before linking */
    MARUFS_CXL_WMB(hot, sizeof(*hot));
    MARUFS_CXL_WMB(cold, sizeof(*cold));

    /*
     * Steps 5-6: link to bucket chain and publish entry.
     */
    ret = marufs_index_link_and_publish(hot, entry_idx, buckets,
                                        bucket_idx);
    if (ret)
        return ret;

    /* INSERTING→VALID succeeded: no longer a GC candidate */
    marufs_le32_cas_dec(&sbi->shard_table[shard_id].tombstone_entries, 1);

    /*
     * Post-insert duplicate detection (H5):
     * Two nodes can pass the pre-insert check and both insert the same name.
     * Now that we're VALID and linked, re-walk the bucket chain. If another
     * VALID entry with the same name exists, the entry with the higher
     * entry_idx loses and rolls back to TOMBSTONE.
     *
     * This is safe because bucket chain is a linked list — once both entries
     * are VALID and linked, any walk sees both.
     */
    {
        u32 cur = READ_LE32(buckets[bucket_idx]);

        while (cur != MARUFS_BUCKET_END && cur < num_entries)
        {
            struct marufs_index_entry_hot* e = &hot_entries[cur];

            if (cur != entry_idx &&
                READ_ONCE(e->state) == MARUFS_ENTRY_VALID_LE &&
                READ_LE64(e->name_hash) == hash)
            {
                /* Hash match — verify name in cold entry */
                struct marufs_index_entry_cold* c = &cold_entries[cur];

                if (strncmp(c->name, name, namelen) == 0 &&
                    (namelen >= sizeof(c->name) || c->name[namelen] == '\0'))
                {
                    /* Duplicate found — higher entry_idx loses */
                    if (entry_idx > cur)
                    {
                        pr_info(
                            "index_insert: post-insert dup '%.*s' "
                            "(entry %u loses to %u)\n",
                            (int)namelen, name, entry_idx, cur);
                        WRITE_ONCE(hot->state, MARUFS_ENTRY_TOMBSTONE_LE);
                        MARUFS_CXL_WMB(hot, sizeof(*hot));
                        marufs_le32_cas_inc(
                            &sbi->shard_table[shard_id].tombstone_entries);
                        return -EEXIST;
                    }
                    /* We win (lower idx) — the other node will roll back */
                }
            }
            cur = READ_LE32(e->next_in_bucket);
        }
    }

    *out_entry_idx = entry_idx;
    pr_debug("index_insert: '%.*s' -> shard %u entry %u (region %u)\n",
             (int)namelen, name, shard_id, entry_idx, region_id);

    return 0;
}

int marufs_index_insert(struct marufs_sb_info* sbi, const char* name,
                        size_t namelen, u32 region_id,
                        u64 file_size, u32 uid, u32 gid, u16 mode, u16 flags,
                        u32* out_entry_idx)
{
    u64 hash;

    if (!sbi || !name || !out_entry_idx)
        return -EINVAL;
    if (namelen == 0 || namelen > MARUFS_NAME_MAX)
        return -ENAMETOOLONG;

    hash = marufs_hash_name(name, namelen);
    return __marufs_index_insert(sbi, name, namelen, hash, region_id,
                                 file_size, uid, gid, mode, flags,
                                 out_entry_idx);
}

int marufs_index_insert_hash(struct marufs_sb_info* sbi, const char* name,
                             size_t namelen, u64 name_hash, u32 region_id,
                             u64 file_size, u32 uid, u32 gid, u16 mode,
                             u16 flags, u32* out_entry_idx)
{
    if (!sbi || !name || !out_entry_idx)
        return -EINVAL;
    if (namelen == 0 || namelen > MARUFS_NAME_MAX)
        return -ENAMETOOLONG;

    /* name_hash == 0 이면 SHA-256 fallback */
    if (name_hash == 0)
        name_hash = marufs_hash_name(name, namelen);

    return __marufs_index_insert(sbi, name, namelen, name_hash, region_id,
                                 file_size, uid, gid, mode, flags,
                                 out_entry_idx);
}

/*
 * marufs_index_lookup - lookup file by name in global index
 * @sbi: superblock info
 * @name: filename to search
 * @namelen: filename length
 * @out_entry: output pointer to found entry (NULL if not found)
 *
 * Lock-free read: walks bucket chain checking for VALID entries with matching
 * name_hash and name. Uses rmb() after state check before reading entry fields.
 *
 * Return: 0 on success, -ENOENT if not found
 */
/* Internal lookup with caller-supplied hash (shard_cache + chain prefetch) */
static int __marufs_index_lookup(struct marufs_sb_info* sbi, const char* name,
                                 size_t namelen, u64 hash,
                                 struct marufs_index_entry_hot** out_entry)
{
    struct marufs_shard_cache* sc;
    u32 shard_id, bucket_idx;
    u32 cur;

    *out_entry = NULL;

    shard_id = marufs_shard_id(hash, sbi->shard_mask);
    sc = &sbi->shard_cache[shard_id]; /* local DRAM — no CXL read */

    if (unlikely(!marufs_shard_geometry_valid(sc->num_buckets, sc->num_entries)))
    {
        pr_err("shard %u corrupted (buckets=%u entries=%u) - reformat needed\n",
               shard_id, sc->num_buckets, sc->num_entries);
        return -EIO;
    }

    bucket_idx = marufs_bucket_idx(hash, sc->num_buckets);

    if (!sc->hot_entries || !sc->buckets)
        return -ENOENT;

    /* Walk bucket chain (hot entries only — 32B stride, 2 per CL) */
    cur = READ_LE32(sc->buckets[bucket_idx]);

    {
        u32 steps = 0;

        while (cur != MARUFS_BUCKET_END && cur < sc->num_entries)
        {
            struct marufs_index_entry_hot* e = &sc->hot_entries[cur];
            u32 next = READ_LE32(e->next_in_bucket);
            __le32 state;

            if (++steps > sc->num_entries)
            {
                pr_err("bucket chain cycle detected (lookup, shard %u bucket %u)\n",
                       shard_id, bucket_idx);
                return -EIO;
            }

            /* Chain walk prefetch: issue CXL fetch for next entry early */
            if (next != MARUFS_BUCKET_END && next < sc->num_entries)
                prefetch(&sc->hot_entries[next]);

            /* Read state first, barrier before reading fields */
            state = READ_ONCE(e->state);
            MARUFS_CXL_RMB(e, sizeof(*e));

            if (state == MARUFS_ENTRY_VALID_LE &&
                READ_LE64(e->name_hash) == hash)
            {
                /* Hash match — fetch cold entry for name verification */
                struct marufs_index_entry_cold* c = &sc->cold_entries[cur];

                MARUFS_CXL_RMB(c, sizeof(*c));
                if (strncmp(c->name, name, namelen) == 0 &&
                    (namelen >= sizeof(c->name) || c->name[namelen] == '\0'))
                {
                    *out_entry = e;
                    return 0;
                }
            }

            cur = next;
        }
    }

    return -ENOENT;
}

int marufs_index_lookup(struct marufs_sb_info* sbi, const char* name,
                        size_t namelen, struct marufs_index_entry_hot** out_entry)
{
    u64 hash;

    if (!sbi || !name || !out_entry)
        return -EINVAL;
    if (namelen == 0 || namelen > MARUFS_NAME_MAX)
        return -ENOENT;

    hash = marufs_hash_name(name, namelen);
    return __marufs_index_lookup(sbi, name, namelen, hash, out_entry);
}

int marufs_index_lookup_hash(struct marufs_sb_info* sbi, const char* name,
                             size_t namelen, u64 name_hash,
                             struct marufs_index_entry_hot** out_entry)
{
    if (!sbi || !name || !out_entry)
        return -EINVAL;
    if (namelen == 0 || namelen > MARUFS_NAME_MAX)
        return -ENOENT;

    /* name_hash == 0 이면 SHA-256 fallback */
    if (name_hash == 0)
        name_hash = marufs_hash_name(name, namelen);

    return __marufs_index_lookup(sbi, name, namelen, name_hash, out_entry);
}

/*
 * marufs_index_delete - delete file from global index (tombstone)
 * @sbi: superblock info
 * @name: filename to delete
 * @namelen: filename length
 *
 * Finds entry then atomically transitions VALID -> TOMBSTONE via CAS.
 * Entry remains in bucket chain (GC removes later). Frees associated region
 * slot.
 *
 * Return: 0 on success, -ENOENT if not found
 */
int marufs_index_delete(struct marufs_sb_info* sbi, const char* name,
                        size_t namelen)
{
    struct marufs_index_entry_hot* entry;
    u64 hash;
    u32 shard_id;
    int ret;

    if (!sbi || !name)
        return -EINVAL;

    /* Search for entry */
    ret = marufs_index_lookup(sbi, name, namelen, &entry);
    if (ret)
        return ret;

    /* CAS: VALID -> TOMBSTONE */
    if (cmpxchg(&entry->state, MARUFS_ENTRY_VALID_LE,
                MARUFS_ENTRY_TOMBSTONE_LE) !=
        MARUFS_ENTRY_VALID_LE)
    {
        /*
         * State changed between lookup and CAS - another node deleted
         * or was already transitioning.
         */
        pr_debug("index_delete: CAS failed for '%.*s', state changed\n",
                 (int)namelen, name);
        return -ENOENT;
    }

    MARUFS_CXL_WMB(entry, sizeof(*entry));

    /* Track counts in kernel RAM (WC partial-write safe) */
    hash = marufs_hash_name(name, namelen);
    shard_id = marufs_shard_id(hash, sbi->shard_mask);

    marufs_le32_cas_inc(&sbi->shard_table[shard_id].tombstone_entries);

    /* RAT cleanup is handled in marufs_unlink() for region files */
    pr_debug("index_delete: '%.*s' tombstoned\n",
             (int)namelen, name);

    return 0;
}

/*
 * marufs_index_delete_by_region - tombstone all name-ref entries for a region
 * @sbi: superblock info
 * @rat_entry_id: RAT entry ID whose name-ref entries should be removed
 *
 * Scans all shards for VALID entries with matching region_id and
 * MARUFS_ENTRY_FLAG_NAME_REF flag, then CAS transitions them to TOMBSTONE.
 *
 * Return: number of tombstoned entries
 */
int marufs_index_delete_by_region(struct marufs_sb_info* sbi, u32 rat_entry_id)
{
    u32 s;
    int deleted = 0;

    if (!sbi)
        return 0;

    /* Fast path: skip full scan if no name-refs registered for this region */
    {
        struct marufs_rat_entry* rat_e = marufs_rat_get(sbi, rat_entry_id);
        if (rat_e && READ_LE32(rat_e->name_ref_count) == 0)
            return 0;
    }

    for (s = 0; s < sbi->num_shards; s++)
    {
        struct marufs_shard_header* sh = &sbi->shard_table[s];
        struct marufs_index_entry_hot* hot_entries;
        u32 num_entries;
        u32 i;

        num_entries = READ_LE32(sh->num_entries);
        hot_entries = marufs_shard_hot_entries(sbi, s);
        if (!hot_entries)
            continue;

        for (i = 0; i < num_entries; i++)
        {
            struct marufs_index_entry_hot* e = &hot_entries[i];

            if (READ_ONCE(e->state) != MARUFS_ENTRY_VALID_LE)
                continue;

            MARUFS_CXL_RMB(e, sizeof(*e));

            if (READ_LE32(e->region_id) != rat_entry_id)
                continue;
            if (!(le16_to_cpu(READ_ONCE(e->flags)) & MARUFS_ENTRY_FLAG_NAME_REF))
                continue;

            /* CAS: VALID -> TOMBSTONE */
            if (cmpxchg(&e->state, MARUFS_ENTRY_VALID_LE,
                        MARUFS_ENTRY_TOMBSTONE_LE) ==
                MARUFS_ENTRY_VALID_LE)
            {
                MARUFS_CXL_WMB(e, sizeof(*e));

                marufs_le32_cas_inc(&sh->tombstone_entries);
                deleted++;
            }
        }
    }

    /* Decrement name_ref_count on RAT entry */
    if (deleted)
    {
        struct marufs_rat_entry* rat_e = marufs_rat_get(sbi, rat_entry_id);
        if (rat_e)
            marufs_rat_name_ref_adjust(rat_e, -deleted);

        pr_debug("deleted %d name-ref entries for region %u\n",
                 deleted, rat_entry_id);
    }

    return deleted;
}

/*
 * marufs_index_iterate - iterate all VALID entries for readdir
 * @sbi: superblock info
 * @start_shard: shard to start from
 * @start_entry: entry index to start from within shard
 * @next_shard: output - next shard to continue
 * @next_entry: output - next entry to continue
 * @out_entry: output - found VALID entry
 *
 * Sequentially scans all shards and their entry arrays to find VALID entries.
 * Returns one entry at a time, caller uses next_shard/next_entry outputs to
 * resume iteration.
 *
 * Return: 0 if entry found, -ENOENT if no more entries
 */
int marufs_index_iterate(struct marufs_sb_info* sbi, u32 start_shard,
                         u32 start_entry, u32* next_shard, u32* next_entry,
                         struct marufs_index_entry_hot** out_entry,
                         struct marufs_index_entry_cold** out_cold)
{
    u32 s, e;
    u32 num_shards;

    if (!sbi || !next_shard || !next_entry || !out_entry || !out_cold)
        return -EINVAL;

    *out_entry = NULL;
    *out_cold = NULL;
    num_shards = sbi->num_shards;

    for (s = start_shard; s < num_shards; s++)
    {
        struct marufs_shard_header* sh = &sbi->shard_table[s];
        struct marufs_index_entry_hot* hot_entries;
        struct marufs_index_entry_cold* cold_entries;
        u32 num_entries;
        u32 first_entry;

        num_entries = READ_LE32(sh->num_entries);
        if (unlikely(!marufs_shard_geometry_valid(1, num_entries)))
            continue; /* Skip corrupted shard */

        hot_entries = marufs_shard_hot_entries(sbi, s);
        if (!hot_entries)
            continue;

        cold_entries = marufs_shard_cold_entries(sbi, s);
        if (!cold_entries)
            continue;

        /* Start from start_entry in starting shard, otherwise from 0 */
        first_entry = (s == start_shard) ? start_entry : 0;

        for (e = first_entry; e < num_entries; e++)
        {
            struct marufs_index_entry_hot* ent = &hot_entries[e];
            __le32 state;

            state = READ_ONCE(ent->state);
            MARUFS_CXL_RMB(ent, sizeof(*ent));

            if (state == MARUFS_ENTRY_VALID_LE)
            {
                *out_entry = ent;
                *out_cold = &cold_entries[e];

                /* Set continuation point to next entry */
                if (e + 1 < num_entries)
                {
                    *next_shard = s;
                    *next_entry = e + 1;
                }
                else
                {
                    *next_shard = s + 1;
                    *next_entry = 0;
                }
                return 0;
            }
        }
    }

    /* No more entries */
    *next_shard = num_shards;
    *next_entry = 0;
    return -ENOENT;
}
