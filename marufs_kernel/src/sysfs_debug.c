// SPDX-License-Identifier: GPL-2.0-only
/*
 * sysfs_debug.c - MARUFS test-only sysfs attrs (fault injection for ME
 * crash-detection tests and bootstrap chaos tests). Not for production use.
 */

#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/kobject.h>
#include <linux/module.h>
#include <linux/sysfs.h>

#include "marufs.h"
#include "me.h"
#include "sysfs_debug.h"
#include "sysfs_internal.h"

/*
 * me_freeze_heartbeat - fault injection: simulate a crashed node.
 *
 * Read: one line per registered mount — "node=<id> frozen=<0|1>".
 * Write format: "<node_id> <0|1>".
 *   Sets the debug_freeze_poll flag on every ME instance owned by any
 *   sbi whose node_id matches <node_id>. A non-zero value makes that
 *   node's poll thread skip the entire poll cycle (heartbeat, grant,
 *   doorbell), so from peers' point of view the node looks crashed —
 *   their wait_for_token deadline path will exercise the liveness
 *   probe and self-takeover. Zero re-enables normal operation.
 *
 * Scoping by node_id (not a module-global flag) lets local multi-mount
 * setups freeze one node without killing all peers on the same machine.
 */
static ssize_t me_freeze_heartbeat_show(struct kobject *kobj,
					struct kobj_attribute *attr, char *buf)
{
	ssize_t n = 0;

	mutex_lock(&marufs_sysfs_lock);
	for (int i = 0; i < MARUFS_MAX_MOUNTS; i++) {
		struct marufs_sb_info *sbi = marufs_sysfs_sbi_list[i];
		if (!sbi)
			continue;

		mutex_lock(&sbi->me_list_lock);
		struct marufs_me_instance *me;
		int frozen = 0;
		list_for_each_entry(me, &sbi->me_list, list_node) {
			frozen = atomic_read(&me->debug_freeze_poll);
			break; /* uniform across sbi's MEs */
		}
		mutex_unlock(&sbi->me_list_lock);

		n += scnprintf(buf + n, PAGE_SIZE - n, "node=%u frozen=%d\n",
			       sbi->node_id, frozen);
	}
	mutex_unlock(&marufs_sysfs_lock);
	return n;
}

static ssize_t me_freeze_heartbeat_store(struct kobject *kobj,
					 struct kobj_attribute *attr,
					 const char *buf, size_t count)
{
	unsigned int node_id, val;
	if (sscanf(buf, "%u %u", &node_id, &val) != 2)
		return -EINVAL;

	mutex_lock(&marufs_sysfs_lock);
	for (int i = 0; i < MARUFS_MAX_MOUNTS; i++) {
		struct marufs_sb_info *sbi = marufs_sysfs_sbi_list[i];
		if (!sbi || sbi->node_id != node_id)
			continue;

		mutex_lock(&sbi->me_list_lock);
		struct marufs_me_instance *me;
		list_for_each_entry(me, &sbi->me_list, list_node)
			atomic_set(&me->debug_freeze_poll, !!val);
		mutex_unlock(&sbi->me_list_lock);
	}
	mutex_unlock(&marufs_sysfs_lock);
	return count;
}

struct kobj_attribute me_freeze_heartbeat_attr =
	__ATTR(me_freeze_heartbeat, 0600, me_freeze_heartbeat_show,
	       me_freeze_heartbeat_store);

/*
 * me_sync_is_holder - test-only: reset DRAM is_holder flags to match CB.
 *
 * Writing any value walks every registered mount's MEs and per-shard
 * forces `sh->is_holder = (cb.holder == me->node_id)`. Used between
 * fault-injection tests to purge stale flags left by takeover races
 * (peer reclaimed CB while our DRAM flag never got updated because
 * poll was frozen). Production relies on the poll thread keeping
 * DRAM in sync via grant/receive paths, so this knob is test-only.
 */
static ssize_t me_sync_is_holder_store(struct kobject *kobj,
				       struct kobj_attribute *attr,
				       const char *buf, size_t count)
{
	mutex_lock(&marufs_sysfs_lock);
	for (int i = 0; i < MARUFS_MAX_MOUNTS; i++) {
		struct marufs_sb_info *sbi = marufs_sysfs_sbi_list[i];
		if (!sbi)
			continue;

		mutex_lock(&sbi->me_list_lock);
		struct marufs_me_instance *me;
		list_for_each_entry(me, &sbi->me_list, list_node) {
			for (u32 s = 0; s < me->num_shards; s++) {
				struct marufs_me_shard *sh = &me->shards[s];
				u32 h = me_cb_snapshot(&me->cbs[s], NULL);
				if (h == me->node_id)
					ME_BECOME_HOLDER(sh);
				else
					ME_LOSE_HOLDER(sh);
			}
		}
		mutex_unlock(&sbi->me_list_lock);
	}
	mutex_unlock(&marufs_sysfs_lock);
	return count;
}

struct kobj_attribute me_sync_is_holder_attr =
	__ATTR(me_sync_is_holder, 0200, NULL, me_sync_is_holder_store);

/*
 * GC trigger - write any value to trigger manual GC on all local mounts.
 * Each mount's GC sweeps its own node's dead entries. Debug-only:
 * normal operation relies on the per-sbi GC kthread.
 */
static ssize_t gc_trigger_store(struct kobject *kobj,
				struct kobj_attribute *attr, const char *buf,
				size_t count)
{
	int i;

	if (!capable(CAP_SYS_ADMIN))
		return -EPERM;

	mutex_lock(&marufs_sysfs_lock);
	for (i = 0; i < MARUFS_MAX_MOUNTS; i++) {
		if (marufs_sysfs_sbi_list[i])
			marufs_gc_reclaim_dead_regions(
				marufs_sysfs_sbi_list[i]);
	}
	mutex_unlock(&marufs_sysfs_lock);

	return count;
}

struct kobj_attribute gc_trigger_attr =
	__ATTR(gc_trigger, 0200, NULL, gc_trigger_store);

/*
 * GC stop - write node_id to stop specific node's GC, "all" to stop all.
 *   echo 1 > gc_stop       # stop node_id=1
 *   echo all > gc_stop     # stop all
 */
static ssize_t gc_stop_store(struct kobject *kobj, struct kobj_attribute *attr,
			     const char *buf, size_t count)
{
	if (!capable(CAP_SYS_ADMIN))
		return -EPERM;

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

struct kobj_attribute gc_stop_attr =
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
	if (!capable(CAP_SYS_ADMIN))
		return -EPERM;

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

struct kobj_attribute gc_pause_attr =
	__ATTR(gc_pause, 0600, gc_pause_show, gc_pause_store);

/*
 * GC restart - restart dead GC threads.
 *   echo 1 > gc_restart     # restart node_id=1
 *   echo all > gc_restart   # restart all dead threads
 */
static ssize_t gc_restart_store(struct kobject *kobj,
				struct kobj_attribute *attr, const char *buf,
				size_t count)
{
	if (!capable(CAP_SYS_ADMIN))
		return -EPERM;

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

struct kobj_attribute gc_restart_attr =
	__ATTR(gc_restart, 0200, NULL, gc_restart_store);

/*
 * bootstrap_inject_stuck_formatter is now a module param
 * (/sys/module/<name>/parameters/bootstrap_inject_stuck_formatter).
 * It must be set BEFORE mounting the formatter node, so a per-sbi sysfs
 * attribute (which only exists after sb construction) is too late.
 * The old per-sbi implementation has been removed.
 */

/*
 * bootstrap_dump - read-only: dump bootstrap slot table for all mounts.
 *
 * Read: one line per slot per mount.
 */
static ssize_t bootstrap_dump_show(struct kobject *kobj,
				   struct kobj_attribute *attr, char *buf)
{
	ssize_t n = 0;

	mutex_lock(&marufs_sysfs_lock);
	for (int i = 0; i < MARUFS_MAX_MOUNTS; i++) {
		struct marufs_sb_info *sbi = marufs_sysfs_sbi_list[i];
		if (!sbi)
			continue;
		n += scnprintf(buf + n, PAGE_SIZE - n, "=== mount node=%u ===\n",
			       sbi->node_id);
		if (n < PAGE_SIZE - 1)
			n += marufs_bootstrap_dump_slots(
				sbi, buf + n, PAGE_SIZE - n);
	}
	mutex_unlock(&marufs_sysfs_lock);
	return n;
}

struct kobj_attribute bootstrap_dump_attr =
	__ATTR(bootstrap_dump, 0400, bootstrap_dump_show, NULL);
