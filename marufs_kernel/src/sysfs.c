// SPDX-License-Identifier: GPL-2.0-only
/*
 * sysfs.c - MARUFS sysfs interface (core).
 *
 * Owns the registered-sbi list and the umbrella attribute groups.
 * Domain-specific attributes are split out:
 *   sysfs_me.c     - ME inspection + per-CPU stats
 *   sysfs_gc.c     - GC monitoring (deleg_info, gc_status)
 *   sysfs_nrht.c   - NRHT chain-depth histogram
 *   sysfs_debug.c  - fault injection + manual GC control (debug group)
 */

#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/kobject.h>
#include <linux/module.h>
#include <linux/sysfs.h>

#include "marufs.h"
#include "sysfs_internal.h"
#include "sysfs_me.h"
#include "sysfs_gc.h"
#include "sysfs_nrht.h"
#include "sysfs_debug.h"

static struct kobject *marufs_kobj;

/* Shared with split sysfs_*.c files via sysfs_internal.h. */
struct marufs_sb_info *marufs_sysfs_sbi_list[MARUFS_MAX_MOUNTS];
DEFINE_MUTEX(marufs_sysfs_lock); /* Protects sbi_list access */

struct marufs_sb_info *marufs_sysfs_get_sbi(void)
{
	int i;
	for (i = 0; i < MARUFS_MAX_MOUNTS; i++) {
		if (marufs_sysfs_sbi_list[i])
			return marufs_sysfs_sbi_list[i];
	}
	return NULL;
}

struct marufs_sb_info *marufs_sysfs_find_by_node(u32 node_id)
{
	int i;
	for (i = 0; i < MARUFS_MAX_MOUNTS; i++) {
		if (marufs_sysfs_sbi_list[i] &&
		    marufs_sysfs_sbi_list[i]->node_id == node_id)
			return marufs_sysfs_sbi_list[i];
	}
	return NULL;
}

/* /sys/fs/marufs/version */
static ssize_t version_show(struct kobject *kobj, struct kobj_attribute *attr,
			    char *buf)
{
	return sysfs_emit(buf, "%d\n", MARUFS_VERSION);
}
static struct kobj_attribute version_attr = __ATTR_RO(version);

/* /sys/fs/marufs/region_info */
static ssize_t region_info_show(struct kobject *kobj,
				struct kobj_attribute *attr, char *buf)
{
	struct marufs_sb_info *sbi;
	int len = 0;
	u32 i;

	mutex_lock(&marufs_sysfs_lock);
	sbi = marufs_sysfs_get_sbi();
	if (!sbi) {
		mutex_unlock(&marufs_sysfs_lock);
		return sysfs_emit(buf, "No filesystem mounted\n");
	}

	/* RAT mode: show RAT entries */
	len += sysfs_emit_at(
		buf, len, "RAT_Entry\tNode\tPID\tState\tSize\tOffset\tName\n");

	for (i = 0; i < MARUFS_MAX_RAT_ENTRIES; i++) {
		u32 state, owner_node, owner_pid;
		u64 size, offset;
		const char *state_str;
		char name[MARUFS_NAME_MAX + 1];
		struct marufs_rat_entry *entry = marufs_rat_entry_get(sbi, i);
		if (!entry)
			continue;

		state = READ_LE32(entry->state);
		if (state != MARUFS_RAT_ENTRY_ALLOCATED)
			continue;

		owner_node = READ_LE16(entry->owner_node_id);
		owner_pid = READ_LE32(entry->owner_pid);
		size = READ_LE64(entry->size);
		offset = READ_LE64(entry->phys_offset);

		/* Copy name safely */
		memcpy(name, entry->name, MARUFS_NAME_MAX);
		name[MARUFS_NAME_MAX] = '\0';

		state_str = "ALLOCATED";

		len += sysfs_emit_at(buf, len,
				     "%u\t%u\t%u\t%s\t%llu\t0x%llx\t%s\n", i,
				     owner_node, owner_pid, state_str, size,
				     offset, name);
	}

	mutex_unlock(&marufs_sysfs_lock);
	return len;
}
static struct kobj_attribute region_info_attr = __ATTR_RO(region_info);

/* /sys/fs/marufs/perm_info */
static ssize_t perm_info_show(struct kobject *kobj, struct kobj_attribute *attr,
			      char *buf)
{
	struct marufs_sb_info *sbi;
	int len = 0;
	u32 i;

	mutex_lock(&marufs_sysfs_lock);
	sbi = marufs_sysfs_get_sbi();
	if (!sbi) {
		mutex_unlock(&marufs_sysfs_lock);
		return sysfs_emit(buf, "No filesystem mounted\n");
	}

	if (!sbi->rat) {
		mutex_unlock(&marufs_sysfs_lock);
		return sysfs_emit(buf, "No RAT\n");
	}

	len += sysfs_emit_at(buf, len, "RAT_Entry\tDefault\tDelegations\n");

	MARUFS_CXL_RMB(sbi->rat, sizeof(*sbi->rat));
	for (i = 0; i < MARUFS_MAX_RAT_ENTRIES; i++) {
		u32 state, default_perms, num_deleg;
		struct marufs_rat_entry *entry = marufs_rat_entry_get(sbi, i);
		if (!entry)
			continue;

		state = READ_LE32(entry->state);
		if (state != MARUFS_RAT_ENTRY_ALLOCATED)
			continue;

		/* Invalidate CL2 (default_perms, owner_node_id, deleg_num_entries, ...) */
		MARUFS_CXL_RMB(&entry->default_perms, 64);
		default_perms = READ_CXL_LE16(entry->default_perms);
		num_deleg = READ_CXL_LE16(entry->deleg_num_entries);
		len += sysfs_emit_at(buf, len, "%u\t0x%04x\t%u\n", i,
				     default_perms, num_deleg);
	}

	mutex_unlock(&marufs_sysfs_lock);
	return len;
}
static struct kobj_attribute perm_info_attr = __ATTR_RO(perm_info);

/* /sys/fs/marufs/daxheap_bufid - expose buf_id for secondary mounts */
#ifdef CONFIG_DAXHEAP
extern u64 marufs_daxheap_bufid;

static ssize_t daxheap_bufid_show(struct kobject *kobj,
				  struct kobj_attribute *attr, char *buf)
{
	return sysfs_emit(buf, "0x%llx\n", marufs_daxheap_bufid);
}
static struct kobj_attribute daxheap_bufid_attr = __ATTR_RO(daxheap_bufid);
#endif

static struct attribute *marufs_attrs[] = {
	&version_attr.attr,
	&region_info_attr.attr,
	&perm_info_attr.attr,
	&deleg_info_attr.attr,
	&gc_status_attr.attr,
	&me_info_attr.attr,
	&me_poll_stats_attr.attr,
	&me_fine_stats_attr.attr,
	&me_per_shard_acquire_attr.attr,
	&me_poll_thread_cpu_attr.attr,
	&nrht_chain_depth_attr.attr,
#ifdef CONFIG_DAXHEAP
	&daxheap_bufid_attr.attr,
#endif
	NULL,
};

static struct attribute_group marufs_attr_group = {
	.attrs = marufs_attrs,
};

static struct attribute *marufs_debug_attrs[] = {
	&me_freeze_heartbeat_attr.attr,
	&me_sync_is_holder_attr.attr,
	&gc_trigger_attr.attr,
	&gc_stop_attr.attr,
	&gc_pause_attr.attr,
	&gc_restart_attr.attr,
	NULL,
};

static struct attribute_group marufs_debug_attr_group = {
	.name = "debug",
	.attrs = marufs_debug_attrs,
};

int marufs_sysfs_init(void)
{
	marufs_kobj = kobject_create_and_add(MARUFS_MODULE_NAME, fs_kobj);
	if (!marufs_kobj)
		return -ENOMEM;
	int ret = sysfs_create_group(marufs_kobj, &marufs_attr_group);
	if (ret)
		return ret;

	ret = sysfs_create_group(marufs_kobj, &marufs_debug_attr_group);
	if (ret)
		return ret;

	return 0;
}

void marufs_sysfs_exit(void)
{
	if (marufs_kobj) {
		sysfs_remove_group(marufs_kobj, &marufs_attr_group);
		sysfs_remove_group(marufs_kobj, &marufs_debug_attr_group);
		kobject_put(marufs_kobj);
		marufs_kobj = NULL;
	}
}

int marufs_sysfs_register(struct marufs_sb_info *sbi)
{
	int i;
	mutex_lock(&marufs_sysfs_lock);
	for (i = 0; i < MARUFS_MAX_MOUNTS; i++) {
		if (!marufs_sysfs_sbi_list[i]) {
			marufs_sysfs_sbi_list[i] = sbi;
			mutex_unlock(&marufs_sysfs_lock);
			return 0;
		}
	}
	mutex_unlock(&marufs_sysfs_lock);
	pr_warn("sysfs: max %d mounts reached, sysfs may not track this mount\n",
		MARUFS_MAX_MOUNTS);
	return 0; /* Non-fatal: mount still succeeds */
}

void marufs_sysfs_unregister(struct marufs_sb_info *sbi)
{
	int i;
	mutex_lock(&marufs_sysfs_lock);
	for (i = 0; i < MARUFS_MAX_MOUNTS; i++) {
		if (marufs_sysfs_sbi_list[i] == sbi) {
			marufs_sysfs_sbi_list[i] = NULL;
			mutex_unlock(&marufs_sysfs_lock);
			return;
		}
	}
	mutex_unlock(&marufs_sysfs_lock);
}
