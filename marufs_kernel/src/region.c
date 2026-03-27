// SPDX-License-Identifier: GPL-2.0-only
/*
 * region.c - MARUFS Region Allocator and Offset Name Management
 *
 * Two-phase region allocation:
 *   1. open(O_CREAT): marufs_rat_alloc_entry() reserves a RAT entry (size=0)
 *   2. ftruncate(N):  marufs_region_init() finds contiguous space, inits header
 *
 * Offset name management:
 *   Applications can name offsets within the data area via ioctl.
 *   Names are stored in a per-region hash index (up to 8160 entries, 128-byte names).
 *   Hash-based bucket chain provides O(1) lookup (same pattern as global index).
 */

#include <asm/cmpxchg.h>
#include <linux/bitops.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/slab.h>
#include <linux/string.h>
#include <linux/types.h>

#include "marufs.h"

#define MARUFS_REGION_INIT_MAX_RETRIES 10
#define MARUFS_ALLOC_LOCK_TIMEOUT_NS (5ULL * NSEC_PER_SEC) /* 5s stale lock */

/* ============================================================================
 * RAT (Region Allocation Table) management
 * ============================================================================ */

/* Temporary span for sorted gap search */
struct marufs_region_span {
	u64 offset;
	u64 end;
};

/*
 * marufs_find_contiguous_space - find contiguous free space in device
 * @sbi: superblock info
 * @size: requested size (bytes, must be 2MB aligned)
 * @out_offset: output offset from device start
 *
 * Collects allocated RAT entries, sorts by offset, then does a single
 * linear scan through the gaps. O(n log n) vs previous O(n²).
 *
 * Returns 0 on success, -ENOSPC if no space available.
 */
int marufs_find_contiguous_space(struct marufs_sb_info *sbi, u64 size,
				 u64 *out_offset)
{
	struct marufs_rat *rat;
	struct marufs_region_span *spans;
	u32 count = 0;
	u64 regions_start;
	u64 device_size;
	u64 candidate;
	u32 i, j;
	int ret;

	if (!sbi || !sbi->rat || !out_offset)
		return -EINVAL;

	spans = kmalloc_array(MARUFS_MAX_RAT_ENTRIES, sizeof(*spans),
			      GFP_KERNEL);
	if (!spans)
		return -ENOMEM;

	rat = sbi->rat;
	MARUFS_CXL_RMB(rat, sizeof(*rat));

	regions_start = READ_LE64(rat->regions_start);
	device_size = READ_LE64(rat->device_size);

	pr_debug(
		"find_contiguous_space: size=%llu, regions_start=0x%llx, device_size=%llu\n",
		size, regions_start, device_size);

	/* Collect all allocated regions with valid physical placement */
	for (i = 0; i < MARUFS_MAX_RAT_ENTRIES; i++) {
		struct marufs_rat_entry *entry = &rat->entries[i];
		u64 entry_offset, entry_size;

		MARUFS_CXL_RMB(entry, sizeof(*entry));
		if (READ_LE32(entry->state) != MARUFS_RAT_ENTRY_ALLOCATED)
			continue;

		entry_offset = READ_LE64(entry->phys_offset);
		entry_size = READ_LE64(entry->size);

		if (entry_offset == 0 || entry_size == 0)
			continue;

		/* Skip corrupted entries (overflow protection) */
		if (entry_size > device_size ||
		    entry_offset > device_size - entry_size)
			continue;

		spans[count].offset = entry_offset;
		spans[count].end = entry_offset + entry_size;
		count++;
	}

	/* Insertion sort by offset (max 256 elements, cache-friendly) */
	for (i = 1; i < count; i++) {
		struct marufs_region_span tmp = spans[i];

		j = i;
		while (j > 0 && spans[j - 1].offset > tmp.offset) {
			spans[j] = spans[j - 1];
			j--;
		}
		spans[j] = tmp;
	}

	/* Single linear scan through sorted spans for first gap >= size */
	candidate = regions_start;
	for (i = 0; i < count; i++) {
		if (candidate + size <= spans[i].offset) {
			*out_offset = candidate;
			pr_debug(
				"found contiguous space at offset 0x%llx size %llu\n",
				candidate, size);
			ret = 0;
			goto out;
		}

		if (spans[i].end > candidate)
			candidate = (spans[i].end + MARUFS_ALIGN_2MB - 1) &
				    ~(MARUFS_ALIGN_2MB - 1);
	}

	/* Check space after last region */
	if (candidate + size <= device_size) {
		*out_offset = candidate;
		pr_debug("found contiguous space at offset 0x%llx size %llu\n",
			 candidate, size);
		ret = 0;
		goto out;
	}

	pr_err("no contiguous space for size %llu\n", size);
	ret = -ENOSPC;

out:
	kfree(spans);
	return ret;
}

/*
 * marufs_rat_alloc_entry - allocate a RAT entry
 * @sbi: superblock info
 * @name: region file name
 * @size: region size (bytes), 0 for reservation mode
 * @offset: physical offset in device, 0 for reservation mode
 * @out_rat_entry_id: output RAT entry ID
 *
 * Finds a free RAT entry and initializes it.
 * When size=0 and offset=0, this is a reservation (two-phase create).
 * Returns 0 on success, negative error code on failure.
 */
int marufs_rat_alloc_entry(struct marufs_sb_info *sbi, const char *name,
			   u64 size, u64 offset, u32 *out_rat_entry_id)
{
	struct marufs_rat *rat;
	struct marufs_rat_entry *entry;
	u32 i;
	u32 old_state;
	size_t name_len;
	u64 alloc_time;

	if (!sbi || !sbi->rat || !name || !out_rat_entry_id)
		return -EINVAL;

	rat = sbi->rat;
	name_len = strlen(name);
	if (name_len > MARUFS_NAME_MAX)
		return -ENAMETOOLONG;

	/* Find free entry */
	for (i = 0; i < MARUFS_MAX_RAT_ENTRIES; i++) {
		entry = &rat->entries[i];

		MARUFS_CXL_RMB(entry, sizeof(*entry));
		old_state = READ_LE32(entry->state);
		if (old_state != MARUFS_RAT_ENTRY_FREE)
			continue;

		/* Try to claim this entry with CAS: FREE → ALLOCATING */
		old_state = CAS_LE32(&entry->state, MARUFS_RAT_ENTRY_FREE,
				     MARUFS_RAT_ENTRY_ALLOCATING);
		if (old_state != MARUFS_RAT_ENTRY_FREE)
			continue; /* Lost race, try next */

		/* Initialize fields while in ALLOCATING state (invisible to others) */
		WRITE_LE32(entry->rat_entry_id, i);
		memset(entry->name, 0, sizeof(entry->name));
		memcpy(entry->name, name, name_len);
		WRITE_LE64(entry->phys_offset, offset);
		WRITE_LE64(entry->size, size);
		WRITE_LE32(entry->owner_node_id, sbi->node_id);
		WRITE_LE32(entry->owner_pid, current->pid);
		WRITE_LE64(entry->owner_birth_time,
			   ktime_to_ns(current->start_boottime));

		alloc_time = ktime_get_real_ns();
		WRITE_LE64(entry->alloc_time, alloc_time);
		WRITE_LE64(entry->modified_at, alloc_time);
		WRITE_LE32(entry->default_perms, 0);

		MARUFS_CXL_WMB(
			entry,
			sizeof(*entry)); /* Ensure all fields visible before state transition */

		/* Publish: ALLOCATING → ALLOCATED (fields now valid) */
		WRITE_LE32(entry->state, MARUFS_RAT_ENTRY_ALLOCATED);
		MARUFS_CXL_WMB(entry, sizeof(*entry));

		*out_rat_entry_id = i;
		pr_debug(
			"allocated RAT entry %u for '%s' at offset %llu size %llu\n",
			i, name, offset, size);
		return 0;
	}

	pr_err("no free RAT entries\n");
	return -ENOSPC;
}

/*
 * marufs_rat_free_entry - free a RAT entry
 * @sbi: superblock info
 * @rat_entry_id: RAT entry ID to free
 *
 * Accepts entry in ALLOCATED, DELETING, or ALLOCATING state.
 * Clears metadata fields, then CAS transitions to FREE.
 */
void marufs_rat_free_entry(struct marufs_sb_info *sbi, u32 rat_entry_id)
{
	struct marufs_rat *rat;
	struct marufs_rat_entry *entry;
	u32 cur_state;

	if (!sbi || !sbi->rat || rat_entry_id >= MARUFS_MAX_RAT_ENTRIES)
		return;

	rat = sbi->rat;
	entry = &rat->entries[rat_entry_id];

	/* Clear entry fields BEFORE state transition (H-S1: prevent reuse race) */
	memset(entry->name, 0, sizeof(entry->name));
	WRITE_LE64(entry->phys_offset, 0);
	WRITE_LE64(entry->size, 0);
	WRITE_LE32(entry->owner_node_id, 0);
	WRITE_LE32(entry->owner_pid, 0);
	WRITE_LE64(entry->owner_birth_time, 0);
	WRITE_LE32(entry->default_perms, 0);
	WRITE_LE32(entry->name_ref_count, 0);
	MARUFS_CXL_WMB(
		entry,
		sizeof(*entry)); /* Ensure clears visible before state transition */

	/* CAS to FREE: try DELETING first, then ALLOCATED, then ALLOCATING (rollback) */
	cur_state = CAS_LE32(&entry->state, MARUFS_RAT_ENTRY_DELETING,
			     MARUFS_RAT_ENTRY_FREE);
	if (cur_state != MARUFS_RAT_ENTRY_DELETING) {
		cur_state = CAS_LE32(&entry->state, MARUFS_RAT_ENTRY_ALLOCATED,
				     MARUFS_RAT_ENTRY_FREE);
		if (cur_state != MARUFS_RAT_ENTRY_ALLOCATED)
			CAS_LE32(&entry->state, MARUFS_RAT_ENTRY_ALLOCATING,
				 MARUFS_RAT_ENTRY_FREE);
	}
	MARUFS_CXL_WMB(entry, sizeof(*entry));

	pr_debug("freed RAT entry %u\n", rat_entry_id);
}

/* ============================================================================
 * Region initialization (called from ftruncate path)
 * ============================================================================ */

/*
 * marufs_region_init - allocate physical space and initialize region header
 * @sbi: superblock info
 * @rat_entry_id: RAT entry ID (must be pre-allocated in reservation mode)
 * @data_size: user-requested data size in bytes
 *
 * Called from marufs_setattr() when ftruncate sets file size for the first time.
 * Finds contiguous space, initializes region header with name table,
 * and updates the RAT entry with physical offset/size.
 *
 * Region layout (v2: header in pool, data starts at phys_offset):
 *   [Data area (data_size, 2MB aligned)]
 *   Total region_size = align_2MB(data_size)
 *
 * Returns 0 on success, negative error code on failure.
 */
int marufs_region_init(struct marufs_sb_info *sbi, u32 rat_entry_id,
		       u64 data_size)
{
	struct marufs_rat_entry *rat_entry;
	struct marufs_region_header *rh;
	u64 data_size_aligned;
	u64 region_size;
	u64 region_offset;
	int ret;
	int retries = 0;

	if (!sbi || !sbi->rat || rat_entry_id >= MARUFS_MAX_RAT_ENTRIES)
		return -EINVAL;

	if (data_size == 0)
		return -EINVAL;

	rat_entry = &sbi->rat->entries[rat_entry_id];

	/* Verify entry is allocated and not yet initialized */
	MARUFS_CXL_RMB(rat_entry, sizeof(*rat_entry));
	if (READ_LE32(rat_entry->state) != MARUFS_RAT_ENTRY_ALLOCATED)
		return -EINVAL;
	if (READ_LE64(rat_entry->phys_offset) != 0)
		return -EEXIST; /* Already initialized */

	/* Reject obviously oversized requests (H-S3: integer overflow protection) */
	if (data_size > sbi->total_size)
		return -ENOSPC;

	/* Calculate region size: data only (header is in pool, not in region) */
	data_size_aligned = (data_size + MARUFS_ALIGN_2MB - 1) &
			    ~(MARUFS_ALIGN_2MB - 1);
	region_size = data_size_aligned;

	/* Post-alignment overflow check */
	if (region_size < data_size_aligned || region_size > sbi->total_size)
		return -ENOSPC;

	/*
	 * Global allocation lock: CAS spinlock on RAT header.
	 *
	 * Lock-free overlap detection is fundamentally racy — a scan can miss
	 * a concurrent CAS on an entry it already passed. Since ftruncate is
	 * not a hot path, a simple CAS spinlock is correct and sufficient.
	 *
	 * Stale lock recovery: if holder crashed, lock stays set in CXL memory.
	 * Detect via local timing — if we've been waiting longer than timeout,
	 * force unlock and retry. Uses only local clock (no cross-node skew).
	 */
	{
		struct marufs_rat *rat = sbi->rat;
		u64 wait_start = 0;

		while (retries < MARUFS_REGION_INIT_MAX_RETRIES) {
			if (CAS_LE32(&rat->alloc_lock, 0, 1) == 0)
				break; /* acquired */

			if (wait_start == 0) {
				wait_start = ktime_get_real_ns();
			} else {
				u64 waited = ktime_get_real_ns() - wait_start;

				if (waited > MARUFS_ALLOC_LOCK_TIMEOUT_NS) {
					pr_warn("region_init: force-unlocking stale alloc_lock (waited %lluns)\n",
						waited);
					WRITE_LE32(rat->alloc_lock, 0);
					MARUFS_CXL_WMB(&rat->alloc_lock,
						       sizeof(rat->alloc_lock));
					wait_start =
						0; /* reset for next attempt */
					continue;
				}
			}

			cpu_relax();
			retries++;
		}
		if (retries >= MARUFS_REGION_INIT_MAX_RETRIES) {
			pr_err("region_init: alloc_lock contention (rat_entry=%u)\n",
			       rat_entry_id);
			return -EAGAIN;
		}
	}

	MARUFS_CXL_RMB(
		sbi->rat,
		sizeof(*sbi->rat)); /* ensure lock acquisition visible before space scan */

	ret = marufs_find_contiguous_space(sbi, region_size, &region_offset);
	if (ret) {
		pr_err("no contiguous space for region (size=%llu)\n",
		       region_size);
		WRITE_LE32(sbi->rat->alloc_lock, 0); /* unlock */
		MARUFS_CXL_WMB(&sbi->rat->alloc_lock,
			       sizeof(sbi->rat->alloc_lock));
		return ret;
	}

	/*
	 * Atomically claim physical offset via CAS(0 → region_offset).
	 * Under the lock, no other node can concurrently allocate overlapping space.
	 */
	if (CAS_LE64(&rat_entry->phys_offset, 0, region_offset) != 0) {
		WRITE_LE32(sbi->rat->alloc_lock, 0); /* unlock */
		MARUFS_CXL_WMB(&sbi->rat->alloc_lock,
			       sizeof(sbi->rat->alloc_lock));
		return -EEXIST; /* Same entry double-init race */
	}

	WRITE_LE64(rat_entry->size, region_size);
	MARUFS_CXL_WMB(rat_entry, sizeof(*rat_entry));

	/* Release allocation lock */
	WRITE_LE32(sbi->rat->alloc_lock, 0);
	MARUFS_CXL_WMB(&sbi->rat->alloc_lock, sizeof(sbi->rat->alloc_lock));

	/* Validate the entire region fits in DAX mapping */
	if (!marufs_dax_range_valid(sbi, region_offset, region_size)) {
		pr_err("region_init: region 0x%llx+%llu exceeds DAX mapping\n",
		       region_offset, region_size);
		WRITE_LE64(rat_entry->phys_offset, 0);
		WRITE_LE64(rat_entry->size, 0);
		MARUFS_CXL_WMB(rat_entry, sizeof(*rat_entry));
		return -ENOSPC;
	}

	/* Initialize region header in pool (v2: headers consolidated in pool) */
	rh = marufs_region_hdr(sbi, rat_entry_id);
	if (!rh)
		return -EIO;

	memset(rh, 0, sizeof(*rh));
	WRITE_LE32(rh->magic, MARUFS_REGION_MAGIC);
	WRITE_LE32(rh->region_id, rat_entry_id);
	WRITE_LE32(rh->state, MARUFS_REGION_ACTIVE);
	WRITE_LE32(rh->flags, 0);
	WRITE_LE64(rh->data_size, data_size);
	WRITE_LE64(rh->created_at, ktime_get_real_ns());
	WRITE_LE64(rh->modified_at, ktime_get_real_ns());

	/* Initialize delegation table in region header */
	{
		struct marufs_deleg_table *dt = &rh->deleg_table;

		WRITE_LE32(dt->magic, MARUFS_DELEG_MAGIC);
		WRITE_LE32(dt->num_entries, 0);
		WRITE_LE32(dt->max_entries, MARUFS_DELEG_MAX_ENTRIES);
		/* entries already zeroed by memset(rh, 0, ...) — state=EMPTY=0 */
	}

	MARUFS_CXL_WMB(rh, sizeof(*rh));

	pr_debug(
		"region_init rat=%u region_offset=0x%llx region_size=%llu data_size=%llu\n",
		rat_entry_id, region_offset, region_size, data_size);

	return 0;
}
