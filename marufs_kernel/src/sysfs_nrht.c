// SPDX-License-Identifier: GPL-2.0-only
/*
 * sysfs_nrht.c - NRHT (Non-Resident Hash Table) sysfs attributes.
 */

#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/kobject.h>
#include <linux/module.h>
#include <linux/sysfs.h>

#include "marufs.h"
#include "nrht_stats.h"
#include "sysfs_internal.h"
#include "sysfs_nrht.h"

/*
 * /sys/fs/marufs/nrht_chain_depth - NRHT bucket-chain walk histogram.
 *
 * Write any value → reset across every sbi.
 */
static ssize_t nrht_chain_depth_show(struct kobject *kobj,
				     struct kobj_attribute *attr, char *buf)
{
	int len = 0;
	int any_sbi = 0;

	mutex_lock(&marufs_sysfs_lock);
	for (int i = 0; i < MARUFS_MAX_MOUNTS; i++) {
		struct marufs_sb_info *sbi = marufs_sysfs_sbi_list[i];
		if (!sbi || !sbi->nrht_stats)
			continue;

		any_sbi = 1;
		struct marufs_nrht_stats_pcpu agg = { 0 };
		int cpu;

		for_each_possible_cpu(cpu) {
			struct marufs_nrht_stats_pcpu *p =
				per_cpu_ptr(sbi->nrht_stats, cpu);

			agg.find_chain_count += p->find_chain_count;
			agg.find_chain_steps_total += p->find_chain_steps_total;
			for (int b = 0; b < MARUFS_NRHT_DEPTH_BUCKETS; b++)
				agg.chain_depth_buckets[b] +=
					p->chain_depth_buckets[b];
		}

		u64 avg_steps = agg.find_chain_count ?
					agg.find_chain_steps_total /
						agg.find_chain_count :
					0;
		len += sysfs_emit_at(
			buf, len,
			"node=%u find_chain count=%llu steps_total=%llu avg=%llu\n",
			sbi->node_id, agg.find_chain_count,
			agg.find_chain_steps_total, avg_steps);
		len += sysfs_emit_at(buf, len, "  depth_log2=[");
		for (int b = 0; b < MARUFS_NRHT_DEPTH_BUCKETS; b++)
			len += sysfs_emit_at(buf, len, "%s%llu", b ? "," : "",
					     agg.chain_depth_buckets[b]);
		len += sysfs_emit_at(buf, len, "]\n");
	}
	mutex_unlock(&marufs_sysfs_lock);

	if (!any_sbi)
		return sysfs_emit(buf, "No filesystem mounted\n");
	return len;
}

static ssize_t nrht_chain_depth_store(struct kobject *kobj,
				      struct kobj_attribute *attr,
				      const char *buf, size_t count)
{
	int cpu;

	mutex_lock(&marufs_sysfs_lock);
	for (int i = 0; i < MARUFS_MAX_MOUNTS; i++) {
		struct marufs_sb_info *sbi = marufs_sysfs_sbi_list[i];

		if (!sbi || !sbi->nrht_stats)
			continue;
		for_each_possible_cpu(cpu) {
			memset(per_cpu_ptr(sbi->nrht_stats, cpu), 0,
			       sizeof(struct marufs_nrht_stats_pcpu));
		}
	}
	mutex_unlock(&marufs_sysfs_lock);
	return count;
}

struct kobj_attribute nrht_chain_depth_attr = __ATTR(
	nrht_chain_depth, 0644, nrht_chain_depth_show, nrht_chain_depth_store);
