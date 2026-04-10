// SPDX-License-Identifier: GPL-2.0-only
/* file.c - MARUFS file operations (open, read, mmap, ioctl) */

#include <linux/dax.h>
#include <linux/dma-buf.h>
#include <linux/file.h>
#include <linux/fs.h>
#include <linux/highmem.h>
#include <linux/huge_mm.h>
#include <linux/io.h>
#include <linux/iomap.h>
#include <linux/mman.h>
#include <linux/pagemap.h>
#include <linux/pgtable.h>
#include <linux/prefetch.h>
#include <linux/uio.h>
#include <linux/writeback.h>

#include "compat.h"
#include "marufs.h"

struct marufs_file_ctx {
	struct inode *inode;
	struct marufs_inode_info *xi;
	struct marufs_sb_info *sbi;
};

/* Pre-allocated batch buffer to avoid kvmalloc per ioctl */
#define MARUFS_BATCH_BUF_SIZE \
	(MARUFS_BATCH_FIND_MAX * sizeof(struct marufs_find_name_req))

struct marufs_file_priv {
	void *batch_buf;
};

static inline void marufs_file_ctx_init(struct marufs_file_ctx *ctx,
					struct file *file)
{
	ctx->inode = file_inode(file);
	ctx->xi = marufs_inode_get(ctx->inode);
	ctx->sbi = marufs_sb_get(ctx->inode->i_sb);
}

/* open() always allowed — permission checks happen at data access time */
static int marufs_open(struct inode *inode, struct file *file)
{
	int ret = generic_file_open(inode, file);
	if (ret)
		return ret;

	struct marufs_file_priv *priv =
		kvmalloc(sizeof(*priv) + MARUFS_BATCH_BUF_SIZE, GFP_KERNEL);
	if (!priv)
		return -ENOMEM;

	priv->batch_buf = priv + 1;
	file->private_data = priv;
	return 0;
}

static int marufs_release(struct inode *inode, struct file *file)
{
	kvfree(file->private_data);
	return 0;
}

/* Direct CXL read — bypasses page cache, copies to user buffer */
static ssize_t marufs_read_iter(struct kiocb *iocb, struct iov_iter *to)
{
	struct marufs_file_ctx fc;
	marufs_file_ctx_init(&fc, iocb->ki_filp);

	int ret = marufs_check_permission(fc.sbi, fc.xi->rat_entry_id,
					  MARUFS_PERM_READ);
	if (ret)
		return ret;

	/*
	 * Cross-node i_size sync: remote ftruncate updates RAT but not our
	 * DRAM i_size (d_revalidate=0 only affects new lookups, not open fds).
	 */
	struct marufs_rat_entry *rat_e =
		marufs_rat_entry_get(fc.sbi, fc.xi->rat_entry_id);
	if (rat_e) {
		u64 fresh_size = READ_LE64(rat_e->size);
		if (fresh_size != (u64)fc.inode->i_size)
			i_size_write(fc.inode, fresh_size);

		if (fc.xi->data_phys_offset == 0) {
			u64 phys = READ_CXL_LE64(rat_e->phys_offset);
			if (phys != 0)
				fc.xi->data_phys_offset = phys;
		}
	}

	loff_t pos = iocb->ki_pos;
	size_t count = iov_iter_count(to);
	if (pos >= fc.inode->i_size)
		return 0;
	if (pos + count > fc.inode->i_size)
		count = fc.inode->i_size - pos;
	if (count == 0)
		return 0;

	if (unlikely(fc.xi->data_phys_offset == 0)) {
		pr_debug("read on uninitialized region (ftruncate pending)\n");
		return 0;
	}

	if (unlikely(!marufs_validate_region_addr(
		    fc.sbi, fc.xi->data_phys_offset, pos + count))) {
		pr_err("read range invalid slot_base=0x%llx pos=%lld count=%zu\n",
		       fc.xi->data_phys_offset, pos, count);
		return -EIO;
	}

	/* RMB: WC writer → WB reader cache coherence */
	void *data_ptr =
		(char *)fc.sbi->dax_base + fc.xi->data_phys_offset + pos;
	MARUFS_CXL_RMB(data_ptr, count);

	size_t copied = copy_to_iter(data_ptr, count, to);
	if (copied == 0)
		return -EFAULT;

	iocb->ki_pos += copied;
	return copied;
}

/* write() rejected — data writes only via mmap(PROT_WRITE) */
static ssize_t marufs_write_iter(struct kiocb *iocb, struct iov_iter *from)
{
	return -EACCES;
}

/* Page fault: delegate to filemap_fault (page cache) */
static vm_fault_t marufs_fault(struct vm_fault *vmf)
{
	return filemap_fault(vmf);
}

/* Write fault: permission check + mark dirty */
static vm_fault_t marufs_page_mkwrite(struct vm_fault *vmf)
{
	struct marufs_file_ctx fc;
	marufs_file_ctx_init(&fc, vmf->vma->vm_file);

	int ret = marufs_check_permission(fc.sbi, fc.xi->rat_entry_id,
					  MARUFS_PERM_WRITE);
	if (ret) {
		pr_warn("page_mkwrite denied - rat_entry %u (pid=%d)\n",
			fc.xi->rat_entry_id, current->pid);
		return VM_FAULT_SIGBUS;
	}

	struct page *page = vmf->page;
	lock_page(page);
	if (page->mapping != fc.inode->i_mapping) {
		unlock_page(page);
		return VM_FAULT_NOPAGE;
	}

	set_page_dirty(page);
	return VM_FAULT_LOCKED;
}

static const struct vm_operations_struct marufs_vm_ops = {
	.fault = marufs_fault,
	.page_mkwrite = marufs_page_mkwrite,
};

static int marufs_mmap(struct file *file, struct vm_area_struct *vma)
{
	/* Reject VM_WRITE on O_RDONLY fd */
	if ((vma->vm_flags & VM_WRITE) && !(file->f_mode & FMODE_WRITE))
		return -EACCES;

	struct marufs_file_ctx fc;
	marufs_file_ctx_init(&fc, file);

	struct inode *inode = fc.inode;
	struct marufs_sb_info *sbi = fc.sbi;
	struct marufs_inode_info *xi = fc.xi;

	int ret = marufs_check_permission(sbi, xi->rat_entry_id,
					  MARUFS_PERM_READ);
	if (ret)
		return ret;

	if (vma->vm_flags & VM_WRITE) {
		ret = marufs_check_permission(sbi, xi->rat_entry_id,
					      MARUFS_PERM_WRITE);
		if (ret)
			return ret;
	}

	if (xi->data_phys_offset == 0 || inode->i_size == 0)
		return -ENODATA;

	/* DAXHEAP: delegate to dma_buf (WC mapping, ~57.8 GB/s GPU bandwidth) */
	if (sbi->dax_mode == MARUFS_DAX_HEAP) {
#ifdef CONFIG_DAXHEAP
		u64 user_offset;
		unsigned long map_size;
		unsigned long buf_pgoff;

		if (!sbi->heap_dmabuf)
			return -EINVAL;

		user_offset = (u64)vma->vm_pgoff << PAGE_SHIFT;
		map_size = vma->vm_end - vma->vm_start;
		if (user_offset + map_size > inode->i_size)
			return -EINVAL;

		if (unlikely(!marufs_validate_region_addr(
			    sbi, fc.xi->data_phys_offset,
			    user_offset + map_size)))
			return -EIO;

		buf_pgoff = (fc.xi->data_phys_offset + user_offset) >>
			    PAGE_SHIFT;

		ret = dma_buf_mmap(sbi->heap_dmabuf, vma, buf_pgoff);
		if (ret) {
			pr_err("dma_buf_mmap failed: %d\n", ret);
			return ret;
		}

		pr_debug("mmap DAXHEAP WC inode=%lu buf_pgoff=%lu size=%lu\n",
			 inode->i_ino, buf_pgoff, map_size);
		return 0;
#else
		return -ENOSYS;
#endif
	}

	/*
	 * DEV_DAX: delegate to device_dax driver — NVIDIA cudaHostRegister
	 * needs device_dax vm_ops on ZONE_DEVICE pages.
	 * Fallback: remap_pfn_range (no GPU DMA but mmap works).
	 */
	if (sbi->dax_mode == MARUFS_DAX_DEV) {
		u64 user_offset;
		unsigned long map_size;

		/* Bounds check */
		user_offset = (u64)vma->vm_pgoff << PAGE_SHIFT;
		map_size = vma->vm_end - vma->vm_start;
		if (user_offset + map_size > inode->i_size)
			return -EINVAL;

		if (unlikely(!marufs_validate_region_addr(
			    sbi, xi->data_phys_offset, user_offset + map_size)))
			return -EIO;

		if (sbi->dax_filp && sbi->dax_filp->f_op &&
		    sbi->dax_filp->f_op->mmap) {
			{
				unsigned long orig_pgoff = vma->vm_pgoff;

				vma->vm_pgoff += fc.xi->data_phys_offset >>
						 PAGE_SHIFT;

				vma_set_file(vma, sbi->dax_filp);
				ret = sbi->dax_filp->f_op->mmap(sbi->dax_filp,
								vma);
				pr_debug(
					"mmap DEV_DAX delegated inode=%lu pgoff=%lu ret=%d\n",
					inode->i_ino, vma->vm_pgoff, ret);
				if (ret) {
					vma->vm_pgoff = orig_pgoff;
					return ret;
				}

				vma->vm_page_prot =
					pgprot_writecombine(vma->vm_page_prot);
			}

			return 0;
		}

		/* Fallback: remap_pfn_range (WC pgprot) */
		{
			phys_addr_t phys_addr = sbi->phys_base +
						xi->data_phys_offset +
						user_offset;

			vm_flags_set(vma,
				     VM_PFNMAP | VM_DONTEXPAND | VM_DONTCOPY);
			vma->vm_page_prot =
				pgprot_writecombine(vma->vm_page_prot);

			ret = remap_pfn_range(vma, vma->vm_start,
					      phys_addr >> PAGE_SHIFT, map_size,
					      vma->vm_page_prot);
			if (ret) {
				pr_err("remap_pfn_range failed: %d\n", ret);
				return ret;
			}

			pr_debug(
				"mmap DEV_DAX fallback PFN inode=%lu phys=0x%llx\n",
				inode->i_ino, (unsigned long long)phys_addr);
			return 0;
		}
	}

	return -EINVAL;
}

static loff_t marufs_llseek(struct file *file, loff_t offset, int whence)
{
	return generic_file_llseek(file, offset, whence);
}

/* Zero-fill new pages — data arrives via mmap writes */
static int marufs_read_folio(struct file *file, struct folio *folio)
{
	struct page *page = &folio->page;

	zero_user_segments(page, 0, PAGE_SIZE, 0, 0);
	SetPageUptodate(page);
	unlock_page(page);

	return 0;
}

const struct address_space_operations marufs_aops = {
	.read_folio = marufs_read_folio,
	.dirty_folio = filemap_dirty_folio,
};

/* DAXHEAP: export DMA-BUF fd for GPU import */
static long marufs_ioctl_dmabuf_export(struct marufs_sb_info *sbi,
				       struct marufs_inode_info *xi,
				       struct marufs_dmabuf_req *dreq)
{
	if (sbi->dax_mode != MARUFS_DAX_HEAP)
		return -EOPNOTSUPP;

	int ret = marufs_check_permission(sbi, xi->rat_entry_id,
					  MARUFS_PERM_READ | MARUFS_PERM_WRITE);
	if (ret)
		return ret;

#ifdef CONFIG_DAXHEAP
	if (!sbi->heap_dmabuf)
		return -EINVAL;

	int fd = get_unused_fd_flags(O_CLOEXEC);
	if (fd < 0)
		return fd;

	dreq->fd = fd;
	get_file(sbi->heap_dmabuf->file);
	fd_install(fd, sbi->heap_dmabuf->file);

	pr_debug("DMABUF_EXPORT fd=%d size=%zu\n", fd, sbi->heap_dmabuf->size);
	return 0;
#else
	return -ENOSYS;
#endif
}

/* Resolve a userspace region fd to its RAT entry ID */
static int nrht_resolve_target_fd(int target_fd, u32 *out_region_id)
{
	struct fd f = fdget((unsigned int)target_fd);
	if (!fd_file(f))
		return -EBADF;

	if (fd_file(f)->f_op != &marufs_file_ops) {
		fdput(f);
		return -EINVAL;
	}

	struct marufs_file_ctx fc;
	marufs_file_ctx_init(&fc, fd_file(f));

	*out_region_id = fc.xi->rat_entry_id;

	fdput(f);
	return 0;
}

/* Per-entry NRHT store: resolve target fd → insert name-ref */
static int nrht_store_one(struct marufs_sb_info *sbi, u32 owner_rid,
			  struct marufs_name_offset_req *req)
{
	req->name[sizeof(req->name) - 1] = '\0';
	size_t nlen = strnlen(req->name, sizeof(req->name));

	u32 target_rid;
	int ret = nrht_resolve_target_fd(req->target_region_fd, &target_rid);
	if (ret)
		return ret;

	return marufs_nrht_insert(sbi, owner_rid, req->name, nlen,
				  req->name_hash, req->offset, target_rid);
}

/* ── ioctl handler: BATCH_NAME_OFFSET (batched NRHT insert) ─────────── */
static long marufs_ioctl_batch_name_offset(
	struct marufs_sb_info *sbi, struct marufs_inode_info *xi,
	struct marufs_batch_name_offset_req *breq, void *batch_buf)
{
	if (breq->count == 0 || breq->count > MARUFS_BATCH_STORE_MAX)
		return -EINVAL;

	struct marufs_name_offset_req *bent = batch_buf;
	size_t buf_size = (size_t)breq->count * sizeof(*bent);

	if (copy_from_user(bent, (void __user *)u64_to_user_ptr(breq->entries),
			   buf_size))
		return -EFAULT;

	breq->stored = 0;
	for (u32 i = 0; i < breq->count; i++) {
		bent[i].status =
			nrht_store_one(sbi, xi->rat_entry_id, &bent[i]);
		if (bent[i].status == 0)
			breq->stored++;
	}

	if (copy_to_user((void __user *)u64_to_user_ptr(breq->entries), bent,
			 buf_size))
		return -EFAULT;

	return 0;
}

/* Per-entry NRHT find: lookup name → fill offset + region_name */
static int nrht_find_one(struct marufs_sb_info *sbi, u32 owner_rid,
			 struct marufs_find_name_req *req)
{
	req->name[sizeof(req->name) - 1] = '\0';
	size_t nlen = strnlen(req->name, sizeof(req->name));

	u64 offset;
	u32 target_region_id;
	int ret = marufs_nrht_lookup(sbi, owner_rid, req->name, nlen,
				     req->name_hash, &offset,
				     &target_region_id);
	if (ret)
		return ret;

	req->offset = offset;

	memset(req->region_name, 0, sizeof(req->region_name));
	struct marufs_rat_entry *rat_e =
		marufs_rat_entry_get(sbi, target_region_id);
	if (rat_e) {
		MARUFS_CXL_RMB(rat_e->name, sizeof(rat_e->name));
		memcpy(req->region_name, rat_e->name,
		       min(sizeof(req->region_name) - 1, sizeof(rat_e->name)));
	}
	return 0;
}

/* ── ioctl handler: BATCH_FIND_NAME (batched NRHT lookup) ────────────── */
static long marufs_ioctl_batch_find_name(struct marufs_sb_info *sbi,
					 struct marufs_inode_info *xi,
					 struct marufs_batch_find_req *breq,
					 void *batch_buf)
{
	if (breq->count == 0 || breq->count > MARUFS_BATCH_FIND_MAX)
		return -EINVAL;

	struct marufs_find_name_req *bent = batch_buf;
	size_t buf_size = (size_t)breq->count * sizeof(*bent);

	if (copy_from_user(bent, (void __user *)u64_to_user_ptr(breq->entries),
			   buf_size))
		return -EFAULT;

	breq->found = 0;
	for (u32 i = 0; i < breq->count; i++) {
		bent[i].status = nrht_find_one(sbi, xi->rat_entry_id, &bent[i]);
		if (bent[i].status == 0)
			breq->found++;
	}

	if (copy_to_user((void __user *)u64_to_user_ptr(breq->entries), bent,
			 buf_size))
		return -EFAULT;

	return 0;
}

/* ── ioctl handler: CLEAR_NAME (remove name-ref from NRHT) ──────────── */
static long marufs_ioctl_clear_name(struct marufs_sb_info *sbi,
				    struct marufs_inode_info *xi,
				    struct marufs_name_offset_req *req)
{
	req->name[sizeof(req->name) - 1] = '\0';
	size_t name_len = strnlen(req->name, sizeof(req->name));

	return marufs_nrht_delete(sbi, xi->rat_entry_id, req->name, name_len,
				  req->name_hash);
}

/* ── ioctl handler: PERM_GRANT ──────────────────────────────────────── */
static long marufs_ioctl_perm_grant(struct marufs_sb_info *sbi,
				    struct marufs_inode_info *xi,
				    struct marufs_perm_req *preq)
{
	/* ADMIN can grant anything — check first */
	int ret;
	if (marufs_check_permission(sbi, xi->rat_entry_id, MARUFS_PERM_ADMIN) !=
	    0) {
		/* GRANT can grant, but not ADMIN or GRANT itself */
		ret = marufs_check_permission(sbi, xi->rat_entry_id,
					      MARUFS_PERM_GRANT);
		if (ret)
			return ret;

		if (preq->perms & (MARUFS_PERM_ADMIN | MARUFS_PERM_GRANT))
			return -EPERM;
	}

	ret = marufs_deleg_grant(sbi, xi->rat_entry_id, preq);
	if (ret)
		return ret;

	pr_debug("granted perms=0x%x to node=%u pid=%u on rat_entry %u\n",
		 preq->perms, preq->node_id, preq->pid, xi->rat_entry_id);
	return 0;
}

/* ── ioctl handler: CHOWN (ownership transfer to caller) ───────────── */
static long marufs_ioctl_chown(struct marufs_sb_info *sbi,
			       struct marufs_inode_info *xi,
			       struct marufs_chown_req *req)
{
	int ret = marufs_check_permission(sbi, xi->rat_entry_id,
					  MARUFS_PERM_ADMIN);
	if (ret)
		return ret;

	struct marufs_rat_entry *rat_entry =
		marufs_rat_entry_get(sbi, xi->rat_entry_id);
	if (!rat_entry)
		return -EIO;

	/* CAS ALLOCATED→ALLOCATING: block GC during ownership transfer */
	u32 old_state = marufs_le32_cas(&rat_entry->state,
					MARUFS_RAT_ENTRY_ALLOCATED,
					MARUFS_RAT_ENTRY_ALLOCATING);
	if (old_state != MARUFS_RAT_ENTRY_ALLOCATED)
		return -EAGAIN;

	WRITE_LE64(rat_entry->alloc_time, ktime_get_real_ns());
	MARUFS_CXL_WMB(rat_entry, 64); // CL0

	WRITE_LE16(rat_entry->default_perms, 0);
	WRITE_LE16(rat_entry->owner_node_id, sbi->node_id);
	WRITE_LE32(rat_entry->owner_pid, current->pid);
	WRITE_LE64(rat_entry->owner_birth_time,
		   ktime_to_ns(current->start_boottime));
	WRITE_LE16(rat_entry->deleg_num_entries, 0);
	MARUFS_CXL_WMB(&rat_entry->default_perms, 64); // CL2

	for (u32 i = 0; i < MARUFS_DELEG_MAX_ENTRIES; i++) {
		struct marufs_deleg_entry *de =
			marufs_rat_deleg_entry(rat_entry, i);
		if (!de)
			continue;
		WRITE_LE32(de->state, MARUFS_DELEG_EMPTY);
		MARUFS_CXL_WMB(de, sizeof(*de));
	}

	/* Publish: ALLOCATING → ALLOCATED (ownership transfer complete) */
	WRITE_LE32(rat_entry->state, MARUFS_RAT_ENTRY_ALLOCATED);
	MARUFS_CXL_WMB(rat_entry, sizeof(*rat_entry));

	pr_info_ratelimited("chown rat_entry %u -> node=%u pid=%d\n",
			    xi->rat_entry_id, sbi->node_id, current->pid);
	return 0;
}

/* ── ioctl handler: NRHT_INIT (format NRHT hash table) ──────────────── */
static long marufs_ioctl_nrht_init(struct marufs_sb_info *sbi,
				   struct marufs_inode_info *xi,
				   struct marufs_nrht_init_req *nreq)
{
	return marufs_nrht_init(sbi, xi->rat_entry_id, nreq->max_entries,
				nreq->num_shards, nreq->num_buckets);
}

/* ── ioctl handler: PERM_SET_DEFAULT ────────────────────────────────── */
static long marufs_ioctl_perm_set_default(struct marufs_sb_info *sbi,
					  struct marufs_inode_info *xi,
					  struct marufs_perm_req *preq)
{
	int ret = marufs_check_permission(sbi, xi->rat_entry_id,
					  MARUFS_PERM_ADMIN);
	if (ret) {
		pr_debug("PERM_SET_DEFAULT denied - rat_entry %u (pid=%d)\n",
			 xi->rat_entry_id, current->pid);
		return ret;
	}

	if (preq->perms & ~MARUFS_PERM_ALL)
		return -EINVAL;

	struct marufs_rat_entry *rat_entry =
		marufs_rat_entry_get(sbi, xi->rat_entry_id);
	if (!rat_entry)
		return -EIO;

	WRITE_LE16(rat_entry->default_perms, preq->perms);
	MARUFS_CXL_WMB(rat_entry, sizeof(*rat_entry));

	pr_debug("set default_perms=0x%x on rat_entry %u\n", preq->perms,
		 xi->rat_entry_id);
	return 0;
}

static long marufs_ioctl(struct file *file, unsigned int cmd, unsigned long arg)
{
	struct marufs_file_ctx fc;
	marufs_file_ctx_init(&fc, file);

	union {
		struct marufs_name_offset_req name;
		struct marufs_find_name_req find;
		struct marufs_batch_name_offset_req batch_store;
		struct marufs_batch_find_req batch_find;
		struct marufs_perm_req perm;
		struct marufs_chown_req chown;
		struct marufs_nrht_init_req nrht_init;
		struct marufs_dmabuf_req dmabuf;
	} payload;

	size_t req_size = _IOC_SIZE(cmd);
	if (req_size > sizeof(payload))
		return -ENOTTY;
	if (copy_from_user(&payload, (void __user *)arg, req_size))
		return -EFAULT;

	struct marufs_file_priv *priv = file->private_data;
	long ret;

	switch (cmd) {
	case MARUFS_IOC_NAME_OFFSET:
		ret = marufs_check_permission(fc.sbi, fc.xi->rat_entry_id,
					      MARUFS_PERM_IOCTL);
		if (!ret)
			ret = nrht_store_one(fc.sbi, fc.xi->rat_entry_id,
					     &payload.name);
		break;

	case MARUFS_IOC_BATCH_NAME_OFFSET:
		ret = marufs_check_permission(fc.sbi, fc.xi->rat_entry_id,
					      MARUFS_PERM_IOCTL);
		if (!ret)
			ret = marufs_ioctl_batch_name_offset(
				fc.sbi, fc.xi, &payload.batch_store,
				priv->batch_buf);
		break;

	case MARUFS_IOC_FIND_NAME:
		ret = nrht_find_one(fc.sbi, fc.xi->rat_entry_id, &payload.find);
		break;

	case MARUFS_IOC_BATCH_FIND_NAME:
		ret = marufs_ioctl_batch_find_name(
			fc.sbi, fc.xi, &payload.batch_find, priv->batch_buf);
		break;

	case MARUFS_IOC_CLEAR_NAME:
		ret = marufs_check_permission(fc.sbi, fc.xi->rat_entry_id,
					      MARUFS_PERM_IOCTL);
		if (!ret)
			ret = marufs_ioctl_clear_name(fc.sbi, fc.xi,
						      &payload.name);
		break;

	case MARUFS_IOC_PERM_GRANT:
		ret = marufs_ioctl_perm_grant(fc.sbi, fc.xi, &payload.perm);
		break;

	case MARUFS_IOC_PERM_SET_DEFAULT:
		ret = marufs_ioctl_perm_set_default(fc.sbi, fc.xi,
						    &payload.perm);
		break;

	case MARUFS_IOC_CHOWN:
		ret = marufs_ioctl_chown(fc.sbi, fc.xi, &payload.chown);
		break;

	case MARUFS_IOC_NRHT_INIT:
		ret = marufs_check_permission(fc.sbi, fc.xi->rat_entry_id,
					      MARUFS_PERM_IOCTL);
		if (!ret)
			ret = marufs_ioctl_nrht_init(fc.sbi, fc.xi,
						     &payload.nrht_init);
		break;

	case MARUFS_IOC_DMABUF_EXPORT:
		ret = marufs_ioctl_dmabuf_export(fc.sbi, fc.xi,
						 &payload.dmabuf);
		break;

	default:
		return -ENOTTY;
	}

	/* Centralized copy_to_user for _IOWR ioctls */
	if (ret == 0 && (_IOC_DIR(cmd) & _IOC_READ))
		if (copy_to_user((void __user *)arg, &payload, req_size))
			ret = -EFAULT;

	return ret;
}

/*
 * marufs_get_unmapped_area - get aligned virtual address for mmap
 *
 * DEV_DAX mode: delegate to device_dax's get_unmapped_area so the
 * kernel allocates a 2MB-aligned address. Without this, mmap() picks
 * a 4KB-aligned address and device_dax's mmap handler rejects it.
 */
static unsigned long marufs_get_unmapped_area(struct file *file,
					      unsigned long addr,
					      unsigned long len,
					      unsigned long pgoff,
					      unsigned long flags)
{
	struct marufs_file_ctx fc;
	marufs_file_ctx_init(&fc, file);
	struct marufs_sb_info *sbi = fc.sbi;

	if (sbi->dax_mode == MARUFS_DAX_DEV && sbi->dax_filp &&
	    sbi->dax_filp->f_op && sbi->dax_filp->f_op->get_unmapped_area) {
		return sbi->dax_filp->f_op->get_unmapped_area(
			sbi->dax_filp, addr, len, pgoff, flags);
	}

	return mm_get_unmapped_area(current->mm, file, addr, len, pgoff, flags);
}

const struct file_operations marufs_file_ops = {
	.owner = THIS_MODULE,
	.llseek = marufs_llseek,
	.read_iter = marufs_read_iter,
	.write_iter = marufs_write_iter,
	.mmap = marufs_mmap,
	.get_unmapped_area = marufs_get_unmapped_area,
	.open = marufs_open,
	.release = marufs_release,
	.fsync = noop_fsync,
	.unlocked_ioctl = marufs_ioctl,
};
