/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * compat.h - MARUFS kernel version compatibility shim
 *
 * Consolidates all LINUX_VERSION_CODE checks.
 * Covers kernel API changes from 5.x through 6.5+.
 */

#ifndef _MARUFS_COMPAT_H
#define _MARUFS_COMPAT_H

#include <linux/fs.h>
#include <linux/version.h>

/* VFS idmap parameter abstraction (5.12, 6.3) */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 3, 0)
#define MARUFS_IDMAP_PARAM_COMMA struct mnt_idmap *idmap,
#define MARUFS_IDMAP_ARG_COMMA idmap,
#elif LINUX_VERSION_CODE >= KERNEL_VERSION(5, 12, 0)
#define MARUFS_IDMAP_PARAM_COMMA struct user_namespace *mnt_userns,
#define MARUFS_IDMAP_ARG_COMMA mnt_userns,
#else
#define MARUFS_IDMAP_PARAM_COMMA /* empty */
#define MARUFS_IDMAP_ARG_COMMA /* empty */
#endif

/*
 * generic_fillattr() wrapper — inline function to avoid preprocessor
 * argument counting issues with MARUFS_IDMAP_ARG_COMMA trailing comma macro.
 */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 3, 0)
static inline void marufs_generic_fillattr(struct mnt_idmap *idmap,
					   u32 req_mask, struct inode *inode,
					   struct kstat *stat)
{
	generic_fillattr(idmap, req_mask, inode, stat);
}
#elif LINUX_VERSION_CODE >= KERNEL_VERSION(5, 12, 0)
static inline void marufs_generic_fillattr(struct user_namespace *mnt_userns,
					   u32 req_mask, struct inode *inode,
					   struct kstat *stat)
{
	generic_fillattr(mnt_userns, inode, stat);
}
#else
static inline void marufs_generic_fillattr(u32 req_mask, struct inode *inode,
					   struct kstat *stat)
{
	generic_fillattr(inode, stat);
}
#endif

/* setattr_prepare() wrapper */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 3, 0)
static inline int marufs_setattr_prepare(struct mnt_idmap *idmap,
					 struct dentry *dentry,
					 struct iattr *attr)
{
	return setattr_prepare(idmap, dentry, attr);
}
#elif LINUX_VERSION_CODE >= KERNEL_VERSION(5, 12, 0)
static inline int marufs_setattr_prepare(struct user_namespace *mnt_userns,
					 struct dentry *dentry,
					 struct iattr *attr)
{
	return setattr_prepare(mnt_userns, dentry, attr);
}
#else
static inline int marufs_setattr_prepare(struct dentry *dentry,
					 struct iattr *attr)
{
	return setattr_prepare(dentry, attr);
}
#endif

/* SLAB_MEM_SPREAD removed in 6.8 */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 8, 0)
#define MARUFS_SLAB_MEM_SPREAD 0
#else
#define MARUFS_SLAB_MEM_SPREAD SLAB_MEM_SPREAD
#endif

/* d_revalidate() signature changed in 6.12:
 *   old: int (*)(struct dentry *, unsigned int)
 *   new: int (*)(struct inode *, const struct qstr *, struct dentry *, unsigned int)
 */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 12, 0)
#define MARUFS_D_REVALIDATE_ARGS                                           \
	struct inode *dir, const struct qstr *name, struct dentry *dentry, \
		unsigned int flags
#else
#define MARUFS_D_REVALIDATE_ARGS struct dentry *dentry, unsigned int flags
#endif

/* s_d_op: use set_default_d_op() on 6.17+ (direct __s_d_op write
 * skips DCACHE_OP_REVALIDATE flag), fall back to direct s_d_op on older. */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 17, 0)
#define MARUFS_SET_D_OP(sb, ops) set_default_d_op(sb, ops)
#else
#define MARUFS_SET_D_OP(sb, ops) ((sb)->s_d_op = (ops))
#endif

/* call_mmap() renamed to vfs_mmap() in 6.17 */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 17, 0)
#define marufs_call_mmap(file, vma) vfs_mmap(file, vma)
#else
#define marufs_call_mmap(file, vma) call_mmap(file, vma)
#endif

/* ============================================================================
 * daxheap integration (optional, CONFIG_DAXHEAP)
 * ============================================================================
 *
 * When CONFIG_DAXHEAP is defined, MARUFS can use daxheap as a memory backend
 * for WC (Write-Combining) mmap and DMA-BUF export to GPU.
 *
 * External dependency: daxheap.ko must be loaded before mounting with
 * the "daxheap" mount option.
 *
 * Kernel API (provided by daxheap.ko):
 *   daxheap_kern_alloc(size, flags) → dma_buf*  (0 size = full device)
 *   daxheap_kern_free(dmabuf)                    (release buffer)
 *
 * MARUFS uses standard dma_buf ops for access:
 *   dma_buf_vmap()  → kernel VA for metadata (WB)
 *   dma_buf_mmap()  → userspace mapping (WC via daxheap's pgprot_writecombine)
 *   dma_buf_fd()    → export fd for GPU import (cudaHostRegister etc.)
 */
#ifdef CONFIG_DAXHEAP
#include <linux/dma-buf.h>
#include <linux/iosys-map.h>
#include <daxheap_kapi.h>
#else
/* Stubs when daxheap is not available */
static inline struct dma_buf *daxheap_kern_alloc(size_t size,
						 unsigned int flags)
{
	return ERR_PTR(-ENOSYS);
}

static inline void daxheap_kern_free(struct dma_buf *dmabuf)
{
}

static inline int daxheap_kern_get_id(struct dma_buf *dmabuf, u64 *out_id)
{
	return -ENOSYS;
}

static inline int daxheap_kern_grant(struct dma_buf *dmabuf, u16 target_host,
				     u32 perms)
{
	return -ENOSYS;
}

static inline struct dma_buf *daxheap_kern_import(u64 id)
{
	return ERR_PTR(-ENOSYS);
}
#endif /* CONFIG_DAXHEAP */

#endif /* _MARUFS_COMPAT_H */
