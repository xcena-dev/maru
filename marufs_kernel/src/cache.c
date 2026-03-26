// SPDX-License-Identifier: GPL-2.0-only
/* cache.c - MARUFS Entry Cache (stub) */

#include <linux/fs.h>
#include <linux/kernel.h>

#include "marufs.h"

int marufs_cache_init(struct marufs_sb_info* sbi)
{
    sbi->entry_cache = NULL;
    pr_debug("cache initialized (stub)\n");
    return 0;
}

void marufs_cache_destroy(struct marufs_sb_info* sbi)
{
    sbi->entry_cache = NULL;
    pr_debug("cache destroyed (stub)\n");
}
