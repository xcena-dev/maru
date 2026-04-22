// SPDX-License-Identifier: GPL-2.0-only
/* sysfs.c - MARUFS sysfs Interface */

#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/kobject.h>
#include <linux/module.h>
#include <linux/sysfs.h>

#include "marufs.h"
#include "me.h"

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

static struct kobj_attribute deleg_info_attr =
	__ATTR(deleg_info, 0644, deleg_info_show, deleg_info_store);

/*
 * GC trigger - write any value to trigger manual GC on all local mounts.
 * Each mount's GC sweeps its own node's dead entries.
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

static struct kobj_attribute gc_pause_attr =
	__ATTR(gc_pause, 0600, gc_pause_show, gc_pause_store);

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

static struct kobj_attribute gc_restart_attr =
	__ATTR(gc_restart, 0200, NULL, gc_restart_store);

/*
 * /sys/fs/marufs/me_info - dump ME (Mutual Exclusion) state for debugging.
 *
 * Write a tag to select scope, then read:
 *   echo global   > me_info        # only Global ME
 *   echo <rid>    > me_info        # only NRHT ME for region <rid>
 *   echo all      > me_info        # all registered ME instances (default)
 *
 * Output per ME instance:
 *   [<tag>] strategy=<order|request> shards=<S> max_nodes=<N> local_node=<id>
 *   members: <active node_id list>
 *   shard <s>: holder=<h|NONE> state=<FREE|HELD|RELEASING> hb=<heartbeat>
 *              gen=<generation> acq=<acquire_count> holding=<local> waiters=<local>
 *              cached_succ=<id> last_hb_obs=<ns>
 */
#define MARUFS_ME_INFO_ALL ((u32) - 1)
#define MARUFS_ME_INFO_GLOB ((u32) - 2)
static u32 me_info_filter = MARUFS_ME_INFO_ALL;

static const char *me_state_str(u32 s)
{
	switch (s) {
	case 0:
		return "FREE";
	case 1:
		return "HELD";
	case 2:
		return "RELEASING";
	default:
		return "?";
	}
}

static const char *me_tag_for(struct marufs_sb_info *sbi,
			      struct marufs_me_instance *me, char *buf,
			      size_t buflen)
{
	if (me == sbi->me) {
		snprintf(buf, buflen, "global");
		return buf;
	}
	for (u32 i = 0; i < MARUFS_MAX_RAT_ENTRIES; i++) {
		if (sbi->nrht_me[i] == me) {
			snprintf(buf, buflen, "nrht[%u]", i);
			return buf;
		}
	}
	snprintf(buf, buflen, "unknown");
	return buf;
}

static int me_info_emit_one(char *buf, int len, struct marufs_sb_info *sbi,
			    struct marufs_me_instance *me)
{
	char tag[24];
	me_tag_for(sbi, me, tag, sizeof(tag));

	MARUFS_CXL_RMB(me->header, sizeof(*me->header));
	len += sysfs_emit_at(
		buf, len,
		"[%s] strategy=%s shards=%u max_nodes=%u local_node=%u active=%d\n",
		tag, me->strategy == MARUFS_ME_REQUEST ? "request" : "order",
		me->num_shards, me->max_nodes, me->node_id,
		atomic_read(&me->active));

	/* Membership: list ACTIVE nodes */
	len += sysfs_emit_at(buf, len, "  members:");
	int first = 1;
	/* slot[i] is for external node_id (i + 1) */
	for (u32 n = 0; n < me->max_nodes; n++) {
		struct marufs_me_membership_slot *ms = &me->membership[n];
		MARUFS_CXL_RMB(ms, sizeof(*ms));
		u32 status = READ_CXL_LE32(ms->status);
		if (status == MARUFS_ME_ACTIVE) {
			len += sysfs_emit_at(buf, len, "%s%u",
					     first ? " " : ",", n + 1);
			first = 0;
		}
	}
	if (first)
		len += sysfs_emit_at(buf, len, " (none)");
	len += sysfs_emit_at(buf, len, "\n");

	for (u32 s = 0; s < me->num_shards; s++) {
		struct marufs_me_cb *cb = &me->cbs[s];
		MARUFS_CXL_RMB(cb, sizeof(*cb));

		u32 holder = READ_CXL_LE32(cb->holder);
		u32 state = READ_CXL_LE32(cb->state);
		u64 gen = READ_CXL_LE64(cb->generation);
		u64 acq = READ_CXL_LE64(cb->acquire_count);

		/* Heartbeat now lives per-node on the holder's membership slot */
		u64 hb = 0;
		if (marufs_me_is_valid_node(me, holder)) {
			struct marufs_me_membership_slot *hms =
				&me->membership[holder - 1];
			MARUFS_CXL_RMB(&hms->heartbeat, 8);
			hb = READ_CXL_LE64(hms->heartbeat);
		}

		char hbuf[16];
		if (holder == (u32)0xFF || holder == (u32)-1)
			snprintf(hbuf, sizeof(hbuf), "NONE");
		else
			snprintf(hbuf, sizeof(hbuf), "%u", holder);

		len += sysfs_emit_at(
			buf, len,
			"  shard %u: holder=%s state=%s hb=%llu gen=%llu acq=%llu holding=%d waiters=%d cached_succ=%u last_hb_obs=%llu\n",
			s, hbuf, me_state_str(state), hb, gen, acq,
			atomic_read(&me->holding[s]),
			atomic_read(&me->local_waiters[s]),
			me->cached_successor ? me->cached_successor[s] : 0,
			me->last_heartbeat_time ? me->last_heartbeat_time[s] :
						  0);

		/* Own doorbell slot — surfaces token-pass state for debugging
		 * (who rang, which seq, what gen). last_* are DRAM baselines
		 * used by wait_for_token's phantom filter.
		 */
		if (me->slots) {
			struct marufs_me_slot *ms = me_my_slot(me, s);
			MARUFS_CXL_RMB(ms, sizeof(*ms));
			u32 from = READ_CXL_LE32(ms->from_node);
			u64 tseq = READ_CXL_LE64(ms->token_seq);
			u64 cgaw = READ_CXL_LE64(ms->cb_gen_at_write);
			u32 req = READ_CXL_LE32(ms->requesting);
			u32 rseq = READ_CXL_LE32(ms->sequence);
			u64 rat = READ_CXL_LE64(ms->requested_at);
			u64 gat = READ_CXL_LE64(ms->granted_at);
			u64 last_seq = me->last_token_seq ?
					       me->last_token_seq[s] :
					       0;
			u64 last_gen = me->last_cb_gen ? me->last_cb_gen[s] :
							 0;

			len += sysfs_emit_at(
				buf, len,
				"    my_slot: from=%u seq=%llu cb_gen_at_write=%llu last_seq=%llu last_gen=%llu req=%u rseq=%u req_at=%llu grant_at=%llu\n",
				from, tseq, cgaw, last_seq, last_gen, req,
				rseq, rat, gat);
		}

		/* Guard against PAGE_SIZE overflow */
		if (len > PAGE_SIZE - 512) {
			len += sysfs_emit_at(buf, len, "  ... (truncated)\n");
			return len;
		}
	}
	return len;
}

static ssize_t me_info_show(struct kobject *kobj, struct kobj_attribute *attr,
			    char *buf)
{
	mutex_lock(&marufs_sysfs_lock);

	u32 filter = me_info_filter;
	int len = sysfs_emit_at(buf, 0, "filter=");
	if (filter == MARUFS_ME_INFO_ALL)
		len += sysfs_emit_at(buf, len, "all\n");
	else if (filter == MARUFS_ME_INFO_GLOB)
		len += sysfs_emit_at(buf, len, "global\n");
	else
		len += sysfs_emit_at(buf, len, "nrht[%u]\n", filter);

	/* Iterate ALL registered sbis — NRHT ME instances live only in the
	* sbi whose node_id first triggered nrht_me_get. Without this loop,
	* reading from a mount that hasn't done NRHT ops hides those MEs.
	*/
	int any_sbi = 0;
	for (int i = 0; i < MARUFS_MAX_MOUNTS; i++) {
		struct marufs_sb_info *sbi = marufs_sysfs_sbi_list[i];
		if (!sbi)
			continue;
		any_sbi = 1;

		len += sysfs_emit_at(buf, len, "== node %u ==\n", sbi->node_id);

		mutex_lock(&sbi->me_list_lock);
		struct marufs_me_instance *me;
		list_for_each_entry(me, &sbi->me_list, list_node) {
			if (filter == MARUFS_ME_INFO_GLOB && me != sbi->me)
				continue;
			if (filter != MARUFS_ME_INFO_ALL &&
			    filter != MARUFS_ME_INFO_GLOB) {
				if (filter >= MARUFS_MAX_RAT_ENTRIES ||
				    sbi->nrht_me[filter] != me)
					continue;
			}
			len = me_info_emit_one(buf, len, sbi, me);
			if (len > PAGE_SIZE - 256)
				break;
		}
		mutex_unlock(&sbi->me_list_lock);
		if (len > PAGE_SIZE - 256)
			break;
	}
	mutex_unlock(&marufs_sysfs_lock);

	if (!any_sbi)
		return sysfs_emit(buf, "No filesystem mounted\n");
	if (len == 0)
		return sysfs_emit(buf, "no ME instances\n");
	return len;
}

static ssize_t me_info_store(struct kobject *kobj, struct kobj_attribute *attr,
			     const char *buf, size_t count)
{
	u32 rid;

	if (sysfs_streq(buf, "all"))
		me_info_filter = MARUFS_ME_INFO_ALL;
	else if (sysfs_streq(buf, "global"))
		me_info_filter = MARUFS_ME_INFO_GLOB;
	else if (!kstrtou32(buf, 0, &rid) && rid < MARUFS_MAX_RAT_ENTRIES)
		me_info_filter = rid;
	else
		return -EINVAL;
	return count;
}

static struct kobj_attribute me_info_attr =
	__ATTR(me_info, 0644, me_info_show, me_info_store);

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
	&gc_trigger_attr.attr,
	&gc_stop_attr.attr,
	&gc_pause_attr.attr,
	&gc_status_attr.attr,
	&gc_restart_attr.attr,
	&me_info_attr.attr,
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
