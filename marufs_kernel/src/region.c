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
static int marufs_find_contiguous_space(struct marufs_sb_info *sbi, u64 size,
					u64 *out_offset)
{
	struct marufs_rat *rat = marufs_rat_get(sbi);
	struct marufs_region_span *spans;
	u32 count = 0;
	u64 regions_start;
	u64 device_size;
	u64 candidate;
	u32 i, j;
	int ret;

	if (!rat || !out_offset)
		return -EINVAL;

	spans = kmalloc_array(MARUFS_MAX_RAT_ENTRIES, sizeof(*spans),
			      GFP_KERNEL);
	if (!spans)
		return -ENOMEM;

	regions_start = READ_CXL_LE64(rat->regions_start);
	device_size = READ_CXL_LE64(rat->device_size);

	pr_debug(
		"find_contiguous_space: size=%llu, regions_start=0x%llx, device_size=%llu\n",
		size, regions_start, device_size);

	/* Collect all allocated regions with valid physical placement */
	for (i = 0; i < MARUFS_MAX_RAT_ENTRIES; i++) {
		u64 entry_offset, entry_size;
		struct marufs_rat_entry *entry = marufs_rat_entry_get(sbi, i);
		if (!entry) {
			ret = -EIO;
			goto out;
		}

		if (READ_CXL_LE32(entry->state) != MARUFS_RAT_ENTRY_ALLOCATED)
			continue;

		entry_offset = READ_CXL_LE64(entry->phys_offset);
		entry_size = READ_CXL_LE64(entry->size);
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
			candidate =
				marufs_align_up(spans[i].end, MARUFS_ALIGN_2MB);
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
 * marufs_rat_entry_reset - zero all mutable fields and optionally fill new values
 * @entry: RAT entry (caller holds exclusive state: ALLOCATING or pre-FREE)
 * @sbi: superblock info (NULL for clear-only)
 * @name: filename (NULL for clear-only)
 * @name_len: filename length
 * @offset: physical offset
 * @size: region size
 */
static void marufs_rat_entry_reset(struct marufs_rat_entry *entry,
				   struct marufs_sb_info *sbi, const char *name,
				   size_t name_len, u64 offset, u64 size)
{
	/* Zero everything, then restore state */
	u32 saved_state = READ_CXL_LE32(entry->state);
	memset(entry, 0, sizeof(*entry));
	WRITE_LE32(entry->state, saved_state);

	if (sbi && name) {
		u64 now = ktime_get_real_ns();

		memcpy(entry->name, name, name_len);
		WRITE_LE64(entry->phys_offset, offset);
		WRITE_LE64(entry->size, size);
		WRITE_LE16(entry->owner_node_id, sbi->node_id);
		WRITE_LE32(entry->owner_pid, current->pid);
		WRITE_LE64(entry->owner_birth_time,
			   ktime_to_ns(current->start_boottime));
		WRITE_LE64(entry->alloc_time, now);
		WRITE_LE64(entry->modified_at, now);
	}

	MARUFS_CXL_WMB(entry, sizeof(*entry));
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
	u32 i;
	size_t name_len;

	if (!sbi || !name || !out_rat_entry_id)
		return -EINVAL;

	name_len = strlen(name);
	if (name_len > MARUFS_NAME_MAX)
		return -ENAMETOOLONG;

	/* Find free entry */
	for (i = 0; i < MARUFS_MAX_RAT_ENTRIES; i++) {
		u32 old_state;
		struct marufs_rat_entry *entry = marufs_rat_entry_get(sbi, i);
		if (!entry)
			return -EIO;

		old_state = READ_CXL_LE32(entry->state);
		if (old_state != MARUFS_RAT_ENTRY_FREE)
			continue;

		/* Try to claim this entry with CAS: FREE → ALLOCATING */
		old_state = marufs_le32_cas(&entry->state,
					    MARUFS_RAT_ENTRY_FREE,
					    MARUFS_RAT_ENTRY_ALLOCATING);
		if (old_state != MARUFS_RAT_ENTRY_FREE)
			continue; /* Lost race, try next */

		/* Initialize fields while in ALLOCATING state (invisible to others) */
		marufs_rat_entry_reset(entry, sbi, name, name_len, offset,
				       size);

		/* Publish: ALLOCATING → ALLOCATED (fields now valid) */
		WRITE_LE32(entry->state, MARUFS_RAT_ENTRY_ALLOCATED);
		MARUFS_CXL_WMB(entry, 64);

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
 * @entry: RAT entry pointer (caller must validate)
 *
 * Accepts entry in ALLOCATED, DELETING, or ALLOCATING state.
 * Clears metadata fields, then CAS transitions to FREE.
 */
void marufs_rat_free_entry(struct marufs_rat_entry *entry)
{
	u32 cur_state;

	if (!entry)
		return;

	/* Clear entry fields BEFORE state transition (H-S1: prevent reuse race) */
	marufs_rat_entry_reset(entry, NULL, NULL, 0, 0, 0);

	/* CAS to FREE: try DELETING first, then ALLOCATED, then ALLOCATING (rollback) */
	cur_state = marufs_le32_cas(&entry->state, MARUFS_RAT_ENTRY_DELETING,
				    MARUFS_RAT_ENTRY_FREE);
	if (cur_state != MARUFS_RAT_ENTRY_DELETING) {
		cur_state = marufs_le32_cas(&entry->state,
					    MARUFS_RAT_ENTRY_ALLOCATED,
					    MARUFS_RAT_ENTRY_FREE);
		if (cur_state != MARUFS_RAT_ENTRY_ALLOCATED)
			marufs_le32_cas(&entry->state,
					MARUFS_RAT_ENTRY_ALLOCATING,
					MARUFS_RAT_ENTRY_FREE);
	}
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
	struct marufs_rat *rat = marufs_rat_get(sbi);
	struct marufs_rat_entry *entry =
		marufs_rat_entry_get(sbi, rat_entry_id);
	u64 region_size;
	u64 region_offset;
	u64 wait_start = 0;
	int ret;
	int retries = 0;

	if (!entry || data_size == 0)
		return -EINVAL;

	/* Verify entry is allocated and not yet initialized */
	if (READ_CXL_LE32(entry->state) != MARUFS_RAT_ENTRY_ALLOCATED)
		return -EINVAL;
	if (READ_CXL_LE64(entry->phys_offset) != 0)
		return -EEXIST; /* Already initialized */

	/* Reject obviously oversized requests */
	if (data_size > sbi->total_size)
		return -ENOSPC;

	/* Align region size to 2MB boundary */
	region_size = marufs_align_up(data_size, MARUFS_ALIGN_2MB);
	if (region_size > sbi->total_size)
		return -ENOSPC;

	/*
	 * Global allocation lock: CAS spinlock on RAT header.
	 * Will be replaced by token-ring in a future PR.
	 *
	 * Stale lock recovery: if holder crashed, force-unlock after timeout.
	 */
	while (retries < MARUFS_REGION_INIT_MAX_RETRIES) {
		if (marufs_le32_cas(&rat->alloc_lock, 0, 1) == 0)
			break;

		if (wait_start == 0) {
			wait_start = ktime_get_real_ns();
		} else {
			u64 waited = ktime_get_real_ns() - wait_start;

			if (waited > MARUFS_ALLOC_LOCK_TIMEOUT_NS) {
				pr_warn("region_init: force-unlocking stale alloc_lock (waited %lluns)\n",
					waited);
				marufs_le32_cas(&rat->alloc_lock, 1, 0);
				wait_start = 0;
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

	/* ── Critical section (lock held) ──────────────────────────── */

	/* Find contiguous free space */
	ret = marufs_find_contiguous_space(sbi, region_size, &region_offset);
	if (ret) {
		pr_err("no contiguous space for region (size=%llu)\n",
		       region_size);
		goto unlock;
	}

	/* Validate region fits in DAX mapping */
	if (!marufs_dax_range_valid(sbi, region_offset, region_size)) {
		pr_err("region_init: region 0x%llx+%llu exceeds DAX mapping\n",
		       region_offset, region_size);
		ret = -ENOSPC;
		goto unlock;
	}

	/* Commit: write phys_offset + size to RAT entry */
	WRITE_LE64(entry->phys_offset, region_offset);
	WRITE_LE64(entry->size, region_size);
	MARUFS_CXL_WMB(entry, 64);

unlock:
	WRITE_LE32(rat->alloc_lock, 0);
	MARUFS_CXL_WMB(&rat->alloc_lock, sizeof(rat->alloc_lock));

	if (ret) {
		pr_err("region_init failed for rat_entry=%u (err=%d)\n",
		       rat_entry_id, ret);
		return ret;
	}

	pr_debug(
		"region_init rat=%u region_offset=0x%llx region_size=%llu data_size=%llu\n",
		rat_entry_id, region_offset, region_size, data_size);

	return 0;
}
