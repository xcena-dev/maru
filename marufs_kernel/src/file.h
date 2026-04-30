/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * file.h - VFS file_operations / address_space_operations exports.
 */

#ifndef _MARUFS_FILE_H
#define _MARUFS_FILE_H

#include <linux/fs.h>

extern const struct file_operations marufs_file_ops;
extern const struct address_space_operations marufs_aops;

#endif /* _MARUFS_FILE_H */
