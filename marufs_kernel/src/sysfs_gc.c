// SPDX-License-Identifier: GPL-2.0-only
/*
 * sysfs_gc.c - GC monitoring sysfs attributes (deleg_info, gc_status).
 *
 * Manual GC control (trigger/stop/pause/restart) is in sysfs_debug.c.
 */

#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/kobject.h>
#include <linux/module.h>
#include <linux/sysfs.h>

#include "marufs.h"
#include "sysfs_internal.h"
#include "sysfs_gc.h"

/*
 * /sys/fs/marufs/deleg_info - per-region delegation detail
 * Write a region_id (RAT index), then read back all delegation entries.
 */
static u32 deleg_info_region_id;

static ssize_t deleg_info_show(struct kobject *kobj,
			       struct kobj_attribute *attr, char *buf)
{
	int len = 0;

	mutex_lock(&marufs_sysfs_lock);
	struct marufs_sb_info *sbi = marufs_sysfs_get_sbi();
	if (!sbi) {
		mutex_unlock(&marufs_sysfs_lock);
		return sysfs_emit(buf, "No filesystem mounted\n");
	}

	u32 rid = deleg_info_region_id;
	struct marufs_rat_entry *entry = marufs_rat_entry_get(sbi, rid);
	if (!entry) {
		mutex_unlock(&marufs_sysfs_lock);
		return sysfs_emit(buf, "Invalid region_id %u\n", rid);
	}

	u32 state = READ_LE32(entry->state);
	if (state != MARUFS_RAT_ENTRY_ALLOCATED) {
		mutex_unlock(&marufs_sysfs_lock);
		return sysfs_emit(buf, "region %u not ALLOCATED (state=%u)\n",
				  rid, state);
	}

	char name[MARUFS_NAME_MAX + 1];
	memcpy(name, entry->name, MARUFS_NAME_MAX);
	name[MARUFS_NAME_MAX] = '\0';
	len += sysfs_emit_at(buf, len, "region: %u  name: %s\n", rid, name);

	for (u32 i = 0; i < MARUFS_DELEG_MAX_ENTRIES; i++) {
		struct marufs_deleg_entry *de = &entry->deleg_entries[i];
		u32 ds = READ_CXL_LE32(de->state);

		if (ds == MARUFS_DELEG_EMPTY)
			continue;

		MARUFS_CXL_RMB(de, sizeof(*de));
		len += sysfs_emit_at(
			buf, len,
			"  deleg[%u]: state=%u node=%u pid=%u perms=0x%x birth_time=%llu\n",
			i, ds, READ_CXL_LE32(de->node_id),
			READ_CXL_LE32(de->pid), READ_CXL_LE32(de->perms),
			READ_CXL_LE64(de->birth_time));
	}

	mutex_unlock(&marufs_sysfs_lock);
	return len;
}

static ssize_t deleg_info_store(struct kobject *kobj,
				struct kobj_attribute *attr, const char *buf,
				size_t count)
{
	u32 rid;
	if (kstrtou32(buf, 0, &rid))
		return -EINVAL;
	if (rid >= MARUFS_MAX_RAT_ENTRIES)
		return -EINVAL;

	deleg_info_region_id = rid;
	return count;
}

struct kobj_attribute deleg_info_attr =
	__ATTR(deleg_info, 0644, deleg_info_show, deleg_info_store);

/*
 * GC status - show per-node GC thread liveness and epoch counter.
 *   cat gc_status  =>  "node1:alive epoch=42 node2:dead epoch=0"
 */
static ssize_t gc_status_show(struct kobject *kobj, struct kobj_attribute *attr,
			      char *buf)
{
	int len = 0;
	int i;

	mutex_lock(&marufs_sysfs_lock);
	for (i = 0; i < MARUFS_MAX_MOUNTS; i++) {
		struct marufs_sb_info *sbi = marufs_sysfs_sbi_list[i];
		if (!sbi)
			continue;

		{
			bool has_thread = sbi->gc_thread != NULL;
			int epoch = atomic_read(&sbi->gc_epoch);

			len += sysfs_emit_at(buf, len, "node%u:%s epoch=%d ",
					     sbi->node_id,
					     has_thread ? "running" : "stopped",
					     epoch);
		}
	}
	mutex_unlock(&marufs_sysfs_lock);

	if (len == 0)
		return sysfs_emit(buf, "no mounts\n");

	len += sysfs_emit_at(buf, len, "\n");
	return len;
}

struct kobj_attribute gc_status_attr =
	__ATTR(gc_status, 0444, gc_status_show, NULL);
