/* SPDX-License-Identifier: Apache-2.0 WITH Linux-syscall-note */
/*
 * marufs_uapi.h - MARUFS userspace API definitions
 *
 * Shared header for kernel module and userspace programs.
 * Contains ioctl structures, commands, and constants.
 */

#ifndef _MARUFS_UAPI_H
#define _MARUFS_UAPI_H

#ifdef __KERNEL__
#include <linux/types.h>
#include <linux/ioctl.h>
#else
#include <stdint.h>
#include <linux/types.h>
#include <sys/ioctl.h>
#endif

/* ── Constants ─────────────────────────────────────────────────────── */

enum marufs_constants {
	MARUFS_MAX_NODE_ID = 8,
	MARUFS_NAME_MAX = 63,
	MARUFS_DELEG_MAX = 29, /* Max delegation entries per region */
	MARUFS_BATCH_FIND_MAX = 32,
	MARUFS_BATCH_STORE_MAX = 32,
};

/* ME (Mutual Exclusion) strategy */
enum marufs_me_strategy {
	MARUFS_ME_ORDER = 0, /* Order-driven: token ring circulation */
	MARUFS_ME_REQUEST = 1, /* Request-driven: holder grants on demand */
};

/* Permission bitmask */
enum marufs_perm {
	MARUFS_PERM_READ = 0x0001, /* read() and mmap(PROT_READ) */
	MARUFS_PERM_WRITE = 0x0002, /* mmap(PROT_WRITE), page_mkwrite */
	MARUFS_PERM_DELETE = 0x0004, /* unlink */
	MARUFS_PERM_ADMIN =
		0x0008, /* chown, perm_set_default (ownership control) */
	MARUFS_PERM_IOCTL = 0x0010, /* name_offset, clear_name ioctls */
	MARUFS_PERM_GRANT =
		0x0020, /* perm_grant to third parties (auth proxy) */
	MARUFS_PERM_ALL = 0x003F,
};

/* ── ioctl structures ──────────────────────────────────────────────── */

/* Name-ref registration: name → (target_region:offset) in NRHT.
 * Used for single ioctl and as array element for batch ioctl. */
struct marufs_name_offset_req {
	char name[MARUFS_NAME_MAX + 1]; /* input: name */
	__u64 offset; /* input: offset within target region's data area */
	__u64 name_hash; /* input: pre-computed hash (0 = auto) */
	__s32 target_region_fd; /* input: fd of target region file */
	__s32 status; /* output: 0=success, negative errno (batch) */
};

/* Name-ref lookup: name → (region_name, offset) from NRHT.
 * Used for single ioctl and as array element for batch ioctl. */
struct marufs_find_name_req {
	char name[MARUFS_NAME_MAX + 1]; /* input: name to search */
	char region_name[MARUFS_NAME_MAX +
			 1]; /* output: target region file name */
	__u64 offset; /* output: offset within target region's data area */
	__u64 name_hash; /* input: pre-computed hash (0 = auto) */
	__s32 status; /* output: 0=found, negative errno (batch) */
	__u8 _pad[4];
};

/* Batch find name request */
struct marufs_batch_find_req {
	__u32 count; /* input: number of entries (max MARUFS_BATCH_FIND_MAX) */
	__u32 found; /* output: number of entries successfully found */
	__u64 entries; /* input/output: userspace pointer to marufs_find_name_req[] */
};

/* Batch name-offset store request */
struct marufs_batch_name_offset_req {
	__u32 count; /* input: number of entries (max MARUFS_BATCH_STORE_MAX) */
	__u32 stored; /* output: number of entries successfully stored */
	__u64 entries; /* input/output: userspace pointer to marufs_name_offset_req[] */
};

/* Permission delegation ioctl */
struct marufs_perm_req {
	__u32 node_id; /* Target node (must be > 0) */
	__u32 pid; /* Target PID (must be > 0) */
	__u32 perms; /* Permission bitmask (MARUFS_PERM_*) */
	__u32 reserved;
};

/* Ownership transfer (caller becomes new owner) */
struct marufs_chown_req {
	__u32 reserved; /* must be 0 */
};

/* DMA-BUF export (DAXHEAP mode only) */
struct marufs_dmabuf_req {
	__u64 size; /* IN: export size (0 = entire buffer) */
	__s32 fd; /* OUT: DMA-BUF file descriptor */
	__u32 flags; /* Reserved, must be 0 */
};

/* NRHT initialization request */
struct marufs_nrht_init_req {
	__u32 max_entries; /* 0 = default (524288), total across all shards */
	__u32 num_shards; /* 0 = default (64), must be power of 2 */
	__u32 num_buckets; /* 0 = default (max_entries / 4), total across all shards */
	__u32 me_strategy; /* 0 = order (default), 1 = request */
};

/* ── ioctl commands ────────────────────────────────────────────────── */

/* Global name management */
#define MARUFS_IOC_NAME_OFFSET _IOW('X', 1, struct marufs_name_offset_req)
#define MARUFS_IOC_FIND_NAME _IOWR('X', 2, struct marufs_find_name_req)
#define MARUFS_IOC_CLEAR_NAME _IOW('X', 3, struct marufs_name_offset_req)
#define MARUFS_IOC_BATCH_FIND_NAME _IOWR('X', 4, struct marufs_batch_find_req)
#define MARUFS_IOC_BATCH_NAME_OFFSET \
	_IOWR('X', 6, struct marufs_batch_name_offset_req)

/* Permission delegation */
#define MARUFS_IOC_PERM_GRANT _IOW('X', 10, struct marufs_perm_req)
#define MARUFS_IOC_PERM_SET_DEFAULT _IOW('X', 13, struct marufs_perm_req)

/* Ownership transfer */
#define MARUFS_IOC_CHOWN _IOW('X', 14, struct marufs_chown_req)

/* NRHT (Name-Ref Hash Table) */
#define MARUFS_IOC_NRHT_INIT _IOW('X', 20, struct marufs_nrht_init_req)
/* Explicit ME ring join — optional pre-warm alternative to lazy-init on
 * first NAME_OFFSET. Idempotent (re-joining a cached instance is a no-op).
 */
#define MARUFS_IOC_NRHT_JOIN _IO('X', 21)

/* DMA-BUF */
#define MARUFS_IOC_DMABUF_EXPORT _IOWR('X', 0x50, struct marufs_dmabuf_req)

#endif /* _MARUFS_UAPI_H */
