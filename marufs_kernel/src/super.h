/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * super.h - Superblock fill / VFS mount entry point.
 */

#ifndef _MARUFS_SUPER_H
#define _MARUFS_SUPER_H

struct super_block;

int marufs_fill_super(struct super_block *sb, void *data, int silent);

#endif /* _MARUFS_SUPER_H */
