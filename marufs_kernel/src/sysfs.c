// SPDX-License-Identifier: GPL-2.0-only
/* sysfs.c - MARUFS sysfs Interface */

#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/kobject.h>
#include <linux/module.h>
#include <linux/sysfs.h>

#include "marufs.h"

static struct kobject *marufs_kobj;

#define MARUFS_MAX_MOUNTS 4
static struct marufs_sb_info *marufs_sysfs_sbi_list[MARUFS_MAX_MOUNTS];
static DEFINE_MUTEX(marufs_sysfs_lock); /* Protects sbi_list access */

/* Return first registered sbi (all share same CXL memory, any is valid for reads) */
static struct marufs_sb_info *marufs_sysfs_get_sbi(void)
{
	int i;
	for (i = 0; i < MARUFS_MAX_MOUNTS; i++) {
		if (marufs_sysfs_sbi_list[i])
			return marufs_sysfs_sbi_list[i];
	}
	return NULL;
}

#define marufs_sysfs_sbi marufs_sysfs_get_sbi()

/*
 * marufs_sysfs_find_by_node - find sbi by node_id
 * Returns NULL if not found.
 */
static struct marufs_sb_info *marufs_sysfs_find_by_node(u32 node_id)
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

/* GC trigger - write to trigger manual GC */
static ssize_t gc_trigger_store(struct kobject *kobj,
				struct kobj_attribute *attr, const char *buf,
				size_t count)
{
	struct marufs_sb_info *sbi;
	int reclaimed;

	mutex_lock(&marufs_sysfs_lock);
	sbi = marufs_sysfs_get_sbi();
	if (!sbi) {
		mutex_unlock(&marufs_sysfs_lock);
		pr_err("no filesystem mounted\n");
		return -ENODEV;
	}

	pr_debug("manual GC triggered via sysfs\n");
	reclaimed = marufs_gc_reclaim_dead_regions(sbi);
	mutex_unlock(&marufs_sysfs_lock);
	pr_debug("manual GC reclaimed %d regions\n", reclaimed);

	return count;
}

static struct kobj_attribute gc_trigger_attr =
	__ATTR(gc_trigger, 0200, NULL, gc_trigger_store);

/*
 * GC stop - write node_id to stop specific node's GC, "all" to stop all.
 *   echo 1 > gc_stop       # stop node_id=1
 *   echo all > gc_stop     # stop all
 */
static ssize_t gc_stop_store(struct kobject *kobj, struct kobj_attribute *attr,
			     const char *buf, size_t count)
{
	mutex_lock(&marufs_sysfs_lock);
	if (strncmp(buf, "all", 3) == 0) {
		int i;

		pr_info("emergency GC stop (all) via sysfs\n");
		for (i = 0; i < MARUFS_MAX_MOUNTS; i++) {
			if (marufs_sysfs_sbi_list[i])
				marufs_gc_stop(marufs_sysfs_sbi_list[i]);
		}
	} else {
		u32 node_id;
		struct marufs_sb_info *sbi;

		if (kstrtou32(buf, 0, &node_id)) {
			mutex_unlock(&marufs_sysfs_lock);
			return -EINVAL;
		}

		sbi = marufs_sysfs_find_by_node(node_id);
		if (!sbi) {
			mutex_unlock(&marufs_sysfs_lock);
			return -ENOENT;
		}

		pr_info("GC stop for node %u via sysfs\n", node_id);
		marufs_gc_stop(sbi);
	}
	mutex_unlock(&marufs_sysfs_lock);

	return count;
}

static struct kobj_attribute gc_stop_attr =
	__ATTR(gc_stop, 0200, NULL, gc_stop_store);

/*
 * GC pause - temporarily pause GC without killing thread.
 * Read:  shows pause state per node (e.g., "node1:0 node2:1")
 * Write: "1" or "0" for all, "node_id:1" or "node_id:0" for specific node.
 *   echo 1 > gc_pause       # pause all
 *   echo 0 > gc_pause       # resume all
 *   echo 1:1 > gc_pause     # pause node_id=1
 *   echo 2:0 > gc_pause     # resume node_id=2
 */
static ssize_t gc_pause_show(struct kobject *kobj, struct kobj_attribute *attr,
			     char *buf)
{
	int len = 0;
	int i;

	mutex_lock(&marufs_sysfs_lock);
	for (i = 0; i < MARUFS_MAX_MOUNTS; i++) {
		if (marufs_sysfs_sbi_list[i])
			len += sysfs_emit_at(
				buf, len, "node%u:%d ",
				marufs_sysfs_sbi_list[i]->node_id,
				atomic_read(
					&marufs_sysfs_sbi_list[i]->gc_paused));
	}
	mutex_unlock(&marufs_sysfs_lock);

	if (len == 0)
		return sysfs_emit(buf, "0\n");

	len += sysfs_emit_at(buf, len, "\n");
	return len;
}

static ssize_t gc_pause_store(struct kobject *kobj, struct kobj_attribute *attr,
			      const char *buf, size_t count)
{
	u32 node_id;
	int val;
	const char *colon;

	colon = strchr(buf, ':');
	if (colon) {
		/* "node_id:0" or "node_id:1" format */
		struct marufs_sb_info *sbi;

		if (kstrtou32(buf, 0, &node_id))
			return -EINVAL;
		if (kstrtoint(colon + 1, 0, &val))
			return -EINVAL;

		mutex_lock(&marufs_sysfs_lock);
		sbi = marufs_sysfs_find_by_node(node_id);
		if (!sbi) {
			mutex_unlock(&marufs_sysfs_lock);
			return -ENOENT;
		}

		atomic_set(&sbi->gc_paused, val ? 1 : 0);
		mutex_unlock(&marufs_sysfs_lock);
		pr_debug("GC %s for node %u via sysfs\n",
			 val ? "paused" : "resumed", node_id);
	} else {
		/* Plain "0" or "1" — apply to all */
		bool pause;
		int i;

		if (kstrtobool(buf, &pause))
			return -EINVAL;

		mutex_lock(&marufs_sysfs_lock);
		for (i = 0; i < MARUFS_MAX_MOUNTS; i++) {
			if (marufs_sysfs_sbi_list[i])
				atomic_set(&marufs_sysfs_sbi_list[i]->gc_paused,
					   pause ? 1 : 0);
		}
		mutex_unlock(&marufs_sysfs_lock);
		pr_debug("GC %s (all) via sysfs\n",
			 pause ? "paused" : "resumed");
	}

	return count;
}

static struct kobj_attribute gc_pause_attr =
	__ATTR(gc_pause, 0644, gc_pause_show, gc_pause_store);

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

static struct kobj_attribute gc_status_attr =
	__ATTR(gc_status, 0444, gc_status_show, NULL);

/*
 * GC restart - restart dead GC threads.
 *   echo 1 > gc_restart     # restart node_id=1
 *   echo all > gc_restart   # restart all dead threads
 */
static ssize_t gc_restart_store(struct kobject *kobj,
				struct kobj_attribute *attr, const char *buf,
				size_t count)
{
	mutex_lock(&marufs_sysfs_lock);
	if (strncmp(buf, "all", 3) == 0) {
		int i;

		for (i = 0; i < MARUFS_MAX_MOUNTS; i++) {
			if (marufs_sysfs_sbi_list[i])
				marufs_gc_restart(marufs_sysfs_sbi_list[i]);
		}
	} else {
		u32 node_id;
		struct marufs_sb_info *sbi;

		if (kstrtou32(buf, 0, &node_id)) {
			mutex_unlock(&marufs_sysfs_lock);
			return -EINVAL;
		}

		sbi = marufs_sysfs_find_by_node(node_id);
		if (!sbi) {
			mutex_unlock(&marufs_sysfs_lock);
			return -ENODEV;
		}

		marufs_gc_restart(sbi);
	}
	mutex_unlock(&marufs_sysfs_lock);

	return count;
}

static struct kobj_attribute gc_restart_attr =
	__ATTR(gc_restart, 0200, NULL, gc_restart_store);

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
	&gc_trigger_attr.attr,
	&gc_stop_attr.attr,
	&gc_pause_attr.attr,
	&gc_status_attr.attr,
	&gc_restart_attr.attr,
#ifdef CONFIG_DAXHEAP
	&daxheap_bufid_attr.attr,
#endif
	NULL,
};

static struct attribute_group marufs_attr_group = {
	.attrs = marufs_attrs,
};

int marufs_sysfs_init(void)
{
	marufs_kobj = kobject_create_and_add(MARUFS_MODULE_NAME, fs_kobj);
	if (!marufs_kobj)
		return -ENOMEM;
	return sysfs_create_group(marufs_kobj, &marufs_attr_group);
}

void marufs_sysfs_exit(void)
{
	if (marufs_kobj) {
		sysfs_remove_group(marufs_kobj, &marufs_attr_group);
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
