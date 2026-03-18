// SPDX-License-Identifier: GPL-2.0-only
/*
 * file.c - MARUFS file operations
 *
 * ============================================================================
 * Overview
 * ============================================================================
 *
 * Implements file I/O operations for MARUFS:
 * - File open/close (open/release)
 * - Read operations (read_iter)
 * - mmap operations (page cache based)
 * - Page fault handling (filemap_fault)
 *
 * WORM (Write-Once, Read-Many) semantics:
 * - write() system calls are rejected after file creation (-EACCES)
 * - However, direct writes after mmap(PROT_WRITE) are allowed
 * - This is necessary for XCMP P2P memory sharing
 *
 * Why this design?
 * - XCMP server allocates memory, writes pattern, then delegates to client
 * - Client mmaps the same file and reads the data
 * - Page cache allows both processes to share the same physical pages
 */

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

/* ============================================================================
 * File open/close operations
 * ============================================================================ */

/*
 * marufs_open - open file
 *
 * @inode: inode of file being opened
 * @file: VFS file structure
 *
 * open() is allowed for any process — permission enforcement happens on
 * actual data access (read, mmap, write, ioctl). This enables the
 * challenge-response authentication flow: a requester opens the file to
 * obtain an fd, then calls AUTH_BEGIN/AUTH_VERIFY ioctls to acquire
 * permissions before accessing data.
 *
 * WORM policy:
 * - O_RDWR open is allowed (needed for mmap PROT_WRITE)
 * - Actual writes are blocked in write_iter
 *
 * Return: 0 (success), negative error code on failure
 */
static int marufs_open(struct inode* inode, struct file* file)
{
    return generic_file_open(inode, file);
}

/*
 * marufs_release - close file
 *
 * @inode: inode of file being closed
 * @file: VFS file structure
 *
 * No special cleanup needed - page cache is managed by VFS
 *
 * Return: 0 (always success)
 */
static int marufs_release(struct inode* inode, struct file* file)
{
    /* No cleanup needed */
    return 0;
}

/* ============================================================================
 * Read/write operations
 * ============================================================================ */

/*
 * marufs_read_iter - read file (read system call)
 *
 * @iocb: I/O control block (file pointer, position, etc.)
 * @to: user buffer iterator
 *
 * Reads data directly from CXL memory and copies to user buffer
 *
 * Data path:
 * CXL memory → copy_to_iter() → user buffer
 *
 * Note: This function performs direct read without going through page cache
 *       mmap access uses page cache
 *
 * Return: number of bytes read, or negative error code
 */
static ssize_t marufs_read_iter(struct kiocb* iocb, struct iov_iter* to)
{
    struct inode* inode = file_inode(iocb->ki_filp);
    struct marufs_inode_info* xi = MARUFS_I(inode);
    struct marufs_sb_info* sbi = MARUFS_SB(inode->i_sb);
    loff_t pos = iocb->ki_pos;
    size_t count = iov_iter_count(to);
    void* data_ptr;
    size_t copied;

    /* Read fresh data from shared memory (cross-node support) */
    MARUFS_CXL_RMB(sbi->dax_base, 64);

    /* Permission check: READ required (always, even if data_phys_offset==0) */
    {
        int perm_ret = marufs_check_permission(sbi, xi->rat_entry_id,
                                             MARUFS_PERM_READ);
        if (perm_ret)
            return perm_ret;
    }

    /*
     * Cross-node file_size sync: another node may have set the size via
     * ftruncate, so re-read the latest file_size from the CXL hot entry.
     * The locally cached i_size in DRAM would never reflect a remote
     * ftruncate (d_revalidate=0 only affects new lookups, not open fds).
     */
    {
        struct marufs_index_entry_hot* hot;

        hot = marufs_shard_hot_entry(sbi, xi->shard_id, xi->entry_idx);
        if (hot && READ_ONCE(hot->state) == MARUFS_ENTRY_VALID_LE)
        {
            u64 fresh_size = READ_LE64(hot->file_size);

            if (fresh_size != (u64)inode->i_size)
                i_size_write(inode, fresh_size);

            /* Refresh data_phys_offset after remote ftruncate */
            if (xi->data_phys_offset == 0)
            {
                struct marufs_rat_entry* rat_e;

                rat_e = marufs_rat_get(sbi, xi->rat_entry_id);
                if (rat_e)
                {
                    u64 phys = READ_LE64(rat_e->phys_offset);

                    if (phys != 0)
                        xi->data_phys_offset = phys;
                }
            }
        }
    }

    /* Boundary check: read beyond EOF */
    if (pos >= inode->i_size)
        return 0; /* EOF */

    /* Adjust read size: don't read beyond EOF */
    if (pos + count > inode->i_size)
        count = inode->i_size - pos;

    if (count == 0)
        return 0;

    /* Use cached data_phys_offset (points to start of data area) */
    if (unlikely(xi->data_phys_offset == 0))
    {
        pr_debug("read on file with uninitialized region (ftruncate pending)\n");
        return 0; /* No data yet */
    }

    if (unlikely(!marufs_validate_slot_addr(sbi, xi->data_phys_offset, pos + count)))
    {
        pr_err("read range invalid slot_base=0x%llx pos=%lld count=%zu\n",
               xi->data_phys_offset, pos, count);
        return -EIO;
    }

    data_ptr = (char*)sbi->dax_base + xi->data_phys_offset + pos;

    /* Invalidate CPU cache for data region to ensure cross-node freshness.
     * Writer may use WC mmap (no CPU cache), but reader's WB memremap
     * may hold stale cache lines from region init or prior reads. */
    MARUFS_CXL_RMB(data_ptr, count);

    /* Copy data to user buffer */
    copied = copy_to_iter(data_ptr, count, to);
    if (copied == 0)
        return -EFAULT; /* User buffer access failed */

    /* Update file position */
    iocb->ki_pos += copied;
    return copied;
}

/*
 * marufs_write_iter - write file (write system call)
 *
 * @iocb: I/O control block
 * @from: user buffer iterator
 *
 * WORM policy: always reject
 * - write() system calls are not allowed after file creation
 * - Data can only be written via mmap
 *
 * Return: -EACCES (always)
 */
static ssize_t marufs_write_iter(struct kiocb* iocb, struct iov_iter* from)
{
    /* WORM: writes not allowed after file creation */
    return -EACCES;
}

/* ============================================================================
 * DAX page fault handler (future implementation)
 * ============================================================================
 *
 * DAX (Direct Access) maps CXL memory directly into process address space
 * without going through page cache.
 *
 * Currently using page cache-based implementation for legacy compatibility.
 */

/* ============================================================================
 * Page cache-based mmap implementation
 * ============================================================================
 *
 * Core implementation of P2P memory sharing.
 *
 * How it works:
 * 1. Process A mmaps file and writes data
 * 2. Data is stored in page cache
 * 3. Process B mmaps the same file
 * 4. Process B shares the same page cache pages
 * 5. Process B immediately sees data written by A
 *
 * Why use page cache?
 * - Kernel automatically manages page sharing
 * - No complex synchronization code needed
 * - filemap_fault handles all edge cases
 */

/*
 * marufs_fault - Page fault handler
 *
 * @vmf: Fault information structure
 *
 * Uses standard VFS filemap_fault to get pages from page cache
 *
 * filemap_fault operations:
 * 1. Search for page in page cache
 * 2. If not found, call read_folio to allocate/read page
 * 3. Map page into process address space
 *
 * Why not implement directly?
 * - filemap_fault is decades-proven code
 * - Correctly handles page lock, race conditions, etc.
 * - Direct implementation can cause serious bugs like D-state (deadlock)
 *
 * Return: VM_FAULT_* constants
 */
static vm_fault_t marufs_fault(struct vm_fault* vmf)
{
    return filemap_fault(vmf);
}

/*
 * marufs_page_mkwrite - Page write fault handler
 *
 * @vmf: Fault information structure
 *
 * Called when writing to a read-only page
 *
 * Operations:
 * 1. Acquire page lock
 * 2. Verify page is still valid
 * 3. Mark page as dirty (for later writeback)
 * 4. Return with lock held (kernel releases it)
 *
 * Why is this needed?
 * - Called on first write to mmap(PROT_WRITE) page
 * - Must mark page dirty for later disk write
 * - Page lock prevents concurrent access issues
 *
 * Return: VM_FAULT_LOCKED (page returned with lock held)
 */
static vm_fault_t marufs_page_mkwrite(struct vm_fault* vmf)
{
    struct page* page = vmf->page;
    struct inode* inode = file_inode(vmf->vma->vm_file);
    struct marufs_inode_info* xi = MARUFS_I(inode);
    struct marufs_sb_info* sbi = MARUFS_SB(inode->i_sb);
    int ret;

    /* Permission check: WRITE required for write fault */
    ret = marufs_check_permission(sbi, xi->rat_entry_id, MARUFS_PERM_WRITE);
    if (ret)
    {
        pr_warn("page_mkwrite denied - rat_entry %u (pid=%d)\n",
                xi->rat_entry_id, current->tgid);
        return VM_FAULT_SIGBUS;
    }

    /* Lock page for stabilization */
    lock_page(page);

    /* Verify page still belongs to this file */
    if (page->mapping != inode->i_mapping)
    {
        unlock_page(page);
        return VM_FAULT_NOPAGE; /* Page no longer valid */
    }

    /* Mark dirty so writeback can save it */
    set_page_dirty(page);

    /* Return with lock held - kernel releases it */
    return VM_FAULT_LOCKED;
}

/*
 * mmap VM operations table
 *
 * filemap_fault + page_mkwrite combination implements P2P sharing
 */
static const struct vm_operations_struct marufs_vm_ops = {
    .fault = marufs_fault,               /* Read fault: get from page cache */
    .page_mkwrite = marufs_page_mkwrite, /* Write fault: mark dirty */
};

/*
 * marufs_mmap - setup memory mapping
 *
 * @file: file to map
 * @vma: virtual memory area structure
 *
 * Called by mmap() system call
 *
 * Setup:
 * - Set VM operations table to marufs_vm_ops
 * - Later page faults will call marufs_fault
 *
 * Return: 0 (success)
 */
static int marufs_mmap(struct file* file, struct vm_area_struct* vma)
{
    struct inode* inode = file_inode(file);
    struct marufs_inode_info* xi = MARUFS_I(inode);
    struct marufs_sb_info* sbi = MARUFS_SB(inode->i_sb);
    int ret;

    /*
     * WORM permission check:
     * If file opened read-only (no FMODE_WRITE),
     * reject write mapping (VM_WRITE).
     *
     * Acts as additional protection layer when daemon
     * passes O_RDONLY FD to delegatee.
     */
    if ((vma->vm_flags & VM_WRITE) && !(file->f_mode & FMODE_WRITE))
    {
        pr_debug("mmap denied - file opened read-only\n");
        return -EACCES;
    }

    /* Permission check: READ required for all mappings */
    ret = marufs_check_permission(sbi, xi->rat_entry_id, MARUFS_PERM_READ);
    if (ret)
    {
        pr_debug("mmap read denied - rat_entry %u (pid=%d)\n",
                 xi->rat_entry_id, current->tgid);
        return ret;
    }

    /* Permission check: WRITE required for write mappings */
    if (vma->vm_flags & VM_WRITE)
    {
        ret = marufs_check_permission(sbi, xi->rat_entry_id, MARUFS_PERM_WRITE);
        if (ret)
        {
            pr_debug("mmap write denied - rat_entry %u (pid=%d)\n",
                     xi->rat_entry_id, current->tgid);
            return ret;
        }
    }

    /*
     * DAXHEAP mode: delegate mmap to dma_buf (WC mapping).
     *
     * daxheap's dma_buf .mmap op applies pgprot_writecombine() internally,
     * providing ~57.8 GB/s GPU bandwidth vs ~17 GB/s with UC mapping.
     *
     * Kernel metadata access (sbi->dax_base via dma_buf_vmap) stays WB —
     * CAS and memory barriers work normally.
     *
     * dma_buf_mmap() takes a page offset within the buffer. We calculate
     * this from the region file's data_phys_offset + user-requested offset.
     */
    if (sbi->dax_mode == MARUFS_DAX_HEAP && xi->data_phys_offset != 0)
    {
#ifdef CONFIG_DAXHEAP
        u64 user_offset;
        unsigned long map_size;
        unsigned long buf_pgoff;

        if (inode->i_size == 0)
            return -ENODATA;

        if (!sbi->heap_dmabuf)
            return -EINVAL;

        user_offset = (u64)vma->vm_pgoff << PAGE_SHIFT;
        map_size = vma->vm_end - vma->vm_start;
        if (user_offset + map_size > inode->i_size)
            return -EINVAL;

        /* Validate slot address range */
        if (unlikely(!marufs_validate_slot_addr(sbi, xi->data_phys_offset,
                                              user_offset + map_size)))
            return -EIO;

        /* Page offset within the dma_buf for this region file's data. */
        buf_pgoff = (xi->data_phys_offset + user_offset) >> PAGE_SHIFT;

        ret = dma_buf_mmap(sbi->heap_dmabuf, vma, buf_pgoff);
        if (ret)
        {
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
     * DEV_DAX mode: delegate mmap to device_dax driver.
     *
     * NVIDIA driver recognizes device_dax VMAs and handles
     * cudaHostRegister correctly. By delegating the mmap to the
     * underlying /dev/daxX.Y file, the VMA gets device_dax's vm_ops
     * and NVIDIA's pin_user_pages path works on ZONE_DEVICE pages.
     *
     * Fallback: if dax_filp is unavailable (open failed at mount),
     * use remap_pfn_range direct path (no GPU DMA but mmap still works).
     */
    if (sbi->dax_mode == MARUFS_DAX_DEV && xi->data_phys_offset != 0)
    {
        u64 user_offset;
        unsigned long map_size;

        if (inode->i_size == 0)
            return -ENODATA;

        /* Bounds check */
        user_offset = (u64)vma->vm_pgoff << PAGE_SHIFT;
        map_size = vma->vm_end - vma->vm_start;
        if (user_offset + map_size > inode->i_size)
            return -EINVAL;

        if (unlikely(!marufs_validate_slot_addr(sbi, xi->data_phys_offset,
                                              user_offset + map_size)))
            return -EIO;

        if (sbi->dax_filp && sbi->dax_filp->f_op && sbi->dax_filp->f_op->mmap)
        {
            /* Translate file offset to device offset */
            {
                unsigned long orig_pgoff = vma->vm_pgoff;

                vma->vm_pgoff += xi->data_phys_offset >> PAGE_SHIFT;

                /* Swap VMA to device_dax file — NVIDIA recognizes this */
                vma_set_file(vma, sbi->dax_filp);
                ret = sbi->dax_filp->f_op->mmap(sbi->dax_filp, vma);
                pr_debug("mmap DEV_DAX delegated inode=%lu pgoff=%lu ret=%d\n",
                         inode->i_ino, vma->vm_pgoff, ret);
                if (ret)
                {
                    vma->vm_pgoff = orig_pgoff;
                    return ret;
                }

                /* Override to WC for GPU DMA cache coherency */
                vma->vm_page_prot = pgprot_writecombine(vma->vm_page_prot);
            }

            /* Prefault is handled by userspace via madvise(MADV_POPULATE_READ/WRITE)
             * after mmap returns. Kernel-side handle_mm_fault() in mmap context
             * risks deadlock (mmap_write_lock already held by VFS). */

            return 0;
        }

        /* Fallback: remap_pfn_range direct path (WC for GPU DMA coherency) */
        {
            phys_addr_t phys_addr = sbi->phys_base + xi->data_phys_offset + user_offset;

            vm_flags_set(vma, VM_PFNMAP | VM_DONTEXPAND | VM_DONTCOPY);
            vma->vm_page_prot = pgprot_writecombine(vma->vm_page_prot);

            ret = remap_pfn_range(vma, vma->vm_start,
                                  phys_addr >> PAGE_SHIFT,
                                  map_size, vma->vm_page_prot);
            if (ret)
            {
                pr_err("remap_pfn_range failed: %d\n", ret);
                return ret;
            }

            pr_debug("mmap DEV_DAX fallback PFN inode=%lu phys=0x%llx\n",
                     inode->i_ino, (unsigned long long)phys_addr);
            return 0;
        }
    }

    /* No supported DAX mode matched */
    return -EINVAL;
}

/*
 * marufs_llseek - file position seek
 *
 * Uses standard VFS implementation
 */
static loff_t marufs_llseek(struct file* file, loff_t offset, int whence)
{
    return generic_file_llseek(file, offset, whence);
}

/* ============================================================================
 * Address space operations
 * ============================================================================
 *
 * Handles data movement between page cache and block device.
 *
 * read_folio: block device → page cache
 * writepage: page cache → block device
 * dirty_folio: mark page as dirty
 */

/*
 * marufs_read_folio - read page
 *
 * @file: file to read (can be NULL)
 * @folio: Folio (page group) to fill with data
 *
 * Called by filemap_fault when page is not in page cache
 *
 * Current implementation:
 * - Initialize page to zero
 * - Actual data is filled via mmap writes
 *
 * Why initialize to zero?
 * - MARUFS is a WORM filesystem
 * - Files are created with size set but no data
 * - Server writes data via mmap, then client reads
 *
 * Return: 0 (success)
 */
static int marufs_read_folio(struct file* file, struct folio* folio)
{
    struct page* page = &folio->page;

    /* Fill page with zeros - data is written via mmap */
    zero_user_segments(page, 0, PAGE_SIZE, 0, 0);

    /* Mark page as up-to-date */
    SetPageUptodate(page);

    /* Unlock page (read_folio contract) */
    unlock_page(page);

    return 0;
}

/*
 * Address space operations table
 *
 * Callback functions for page cache management
 */
const struct address_space_operations marufs_aops = {
    .read_folio = marufs_read_folio,      /* Read page */
    .dirty_folio = filemap_dirty_folio, /* Mark dirty (standard) */
};

/* ============================================================================
 * ioctl operations
 * ============================================================================
 */

/*
 * marufs_ioctl_dmabuf_export - export DMA-BUF fd for GPU import (DAXHEAP only)
 *
 * Allocates an fd backed by the daxheap dma_buf, allowing GPU frameworks
 * (e.g., cudaMemcpy) to import the buffer directly.
 */
static long marufs_ioctl_dmabuf_export(struct marufs_sb_info* sbi,
                                     struct marufs_inode_info* xi,
                                     unsigned long arg)
{
    int ret;

    /* Only available in DAXHEAP mode */
    if (sbi->dax_mode != MARUFS_DAX_HEAP)
        return -EOPNOTSUPP;

    /* Permission check: READ + WRITE (DMA-BUF grants full access) */
    ret = marufs_check_permission(sbi, xi->rat_entry_id,
                                MARUFS_PERM_READ | MARUFS_PERM_WRITE);
    if (ret)
        return ret;

#ifdef CONFIG_DAXHEAP
    {
        struct marufs_dmabuf_req dreq;
        int fd;

        if (!sbi->heap_dmabuf)
            return -EINVAL;

        if (copy_from_user(&dreq, (void __user*)arg, sizeof(dreq)))
            return -EFAULT;

        /*
         * Allocate fd and copy to user BEFORE installing,
         * so we can clean up on copy_to_user failure without leaking.
         */
        fd = get_unused_fd_flags(O_CLOEXEC);
        if (fd < 0)
            return fd;

        dreq.fd = fd;
        if (copy_to_user((void __user*)arg, &dreq, sizeof(dreq)))
        {
            put_unused_fd(fd);
            return -EFAULT;
        }

        /* Install fd only after copy_to_user succeeds */
        get_file(sbi->heap_dmabuf->file);
        fd_install(fd, sbi->heap_dmabuf->file);

        pr_debug("DMABUF_EXPORT fd=%d size=%zu\n",
                 fd, sbi->heap_dmabuf->size);
        return 0;
    }
#else
    return -ENOSYS;
#endif
}

/**
 * marufs_ioctl - Handle ioctl operations for region files
 * @file: file pointer
 * @cmd: ioctl command
 * @arg: ioctl argument
 *
 * Supports slot naming operations for region files with slot metadata.
 *
 * Return: 0 on success, negative error code on failure
 */
/* ── ioctl handler: NAME_OFFSET (register name-ref) ─────────────────── */
static long marufs_ioctl_name_offset(struct marufs_sb_info* sbi,
                                   struct marufs_inode_info* xi,
                                   unsigned long arg)
{
    struct marufs_name_offset_req req;
    size_t name_len;
    u32 entry_idx;
    int ret;

    if (copy_from_user(&req, (void __user*)arg, sizeof(req)))
        return -EFAULT;

    req.name[sizeof(req.name) - 1] = '\0';
    name_len = strnlen(req.name, sizeof(req.name));

    /* Validate offset falls within region data area */
    {
        struct marufs_rat_entry* rat_e = marufs_rat_get(sbi, xi->rat_entry_id);
        u64 data_size;

        if (!rat_e)
            return -EIO;

        /* v2: region_size == data_size (no header in region) */
        data_size = READ_LE64(rat_e->size);
        if (req.offset >= data_size)
            return -EINVAL;
    }

    ret = marufs_index_insert_hash(sbi, req.name, name_len,
                                 req.name_hash,
                                 xi->rat_entry_id, req.offset,
                                 current_uid().val, current_gid().val,
                                 0, MARUFS_ENTRY_FLAG_NAME_REF,
                                 &entry_idx);
    if (ret == -EEXIST)
    {
        /* Upsert: update offset only if changed */
        struct marufs_index_entry_hot* existing;

        ret = marufs_index_lookup_hash(sbi, req.name, name_len,
                                     req.name_hash, &existing);
        if (ret)
            return ret;
        if (!(le16_to_cpu(existing->flags) & MARUFS_ENTRY_FLAG_NAME_REF))
            return -EEXIST; /* Name collision with region file */
        if (READ_LE32(existing->region_id) != xi->rat_entry_id)
            return -EACCES; /* Name-ref belongs to a different region */
        if (READ_LE64(existing->file_size) != req.offset)
        {
            WRITE_LE64(existing->file_size, req.offset);
            MARUFS_CXL_WMB(existing, sizeof(*existing));
        }
    }
    else if (ret < 0)
    {
        pr_err("name_offset insert failed: %d\n", ret);
        return ret;
    }
    else
    {
        /* New insert succeeded — increment name_ref_count on RAT entry */
        struct marufs_rat_entry* rat_e = marufs_rat_get(sbi, xi->rat_entry_id);
        if (rat_e)
            marufs_rat_name_ref_adjust(rat_e, 1);
    }

    pr_debug("name_ref '%s' -> rat=%u offset=%llu\n",
             req.name, xi->rat_entry_id, req.offset);
    return 0;
}

/* ── ioctl handler: BATCH_NAME_OFFSET (batched store) ───────────────── */
static long marufs_ioctl_batch_name_offset(struct marufs_sb_info* sbi,
                                         struct marufs_inode_info* xi,
                                         unsigned long arg)
{
    struct marufs_batch_name_offset_req breq;
    struct marufs_batch_name_offset_entry* bent;
    size_t buf_size;
    u32 i, new_inserts = 0;

    if (copy_from_user(&breq, (void __user*)arg, sizeof(breq)))
        return -EFAULT;

    if (breq.count == 0 || breq.count > MARUFS_BATCH_STORE_MAX)
        return -EINVAL;

    buf_size = (size_t)breq.count * sizeof(*bent);
    bent = kvmalloc(buf_size, GFP_KERNEL);
    if (!bent)
        return -ENOMEM;

    if (copy_from_user(bent,
                       (void __user*)u64_to_user_ptr(breq.entries),
                       buf_size))
    {
        kvfree(bent);
        return -EFAULT;
    }

    /* Validate all offsets fall within region data area */
    {
        struct marufs_rat_entry* rat_e = marufs_rat_get(sbi, xi->rat_entry_id);
        u64 data_size;

        if (!rat_e)
        {
            kvfree(bent);
            return -EIO;
        }

        /* v2: region_size == data_size (no header in region) */
        data_size = READ_LE64(rat_e->size);
        for (i = 0; i < breq.count; i++)
        {
            if (bent[i].offset >= data_size)
            {
                kvfree(bent);
                return -EINVAL;
            }
        }
    }

    /* Phase 1: Prefetch bucket heads + save computed hash (#6) */
    for (i = 0; i < breq.count; i++)
    {
        u64 h = bent[i].name_hash;
        u32 sid, bid;
        struct marufs_shard_cache* sc;

        if (h == 0)
        {
            bent[i].name[sizeof(bent[i].name) - 1] = '\0';
            h = marufs_hash_name(bent[i].name,
                               strnlen(bent[i].name, sizeof(bent[i].name)));
            bent[i].name_hash = h;
        }
        sid = marufs_shard_id(h, sbi->shard_mask);
        sc = &sbi->shard_cache[sid];
        bid = marufs_bucket_idx(h, sc->num_buckets);
        prefetch(&sc->buckets[bid]);
    }

    /* Phase 1b: Prefetch first chain entries (#1) */
    for (i = 0; i < breq.count; i++)
    {
        u64 h = bent[i].name_hash;
        u32 sid, bid, head;
        struct marufs_shard_cache* sc;

        sid = marufs_shard_id(h, sbi->shard_mask);
        sc = &sbi->shard_cache[sid];
        bid = marufs_bucket_idx(h, sc->num_buckets);
        head = READ_LE32(sc->buckets[bid]);
        if (head != MARUFS_BUCKET_END && head < sc->num_entries)
            prefetch(&sc->hot_entries[head]);
    }

    /* Phase 2: Process entries (bucket heads + first chain entries in cache) */
    breq.stored = 0;
    for (i = 0; i < breq.count; i++)
    {
        struct marufs_index_entry_hot* existing;
        size_t nlen;
        u32 eidx;
        int r;

        bent[i].name[sizeof(bent[i].name) - 1] = '\0';
        nlen = strnlen(bent[i].name, sizeof(bent[i].name));

        r = marufs_index_insert_hash(sbi, bent[i].name, nlen,
                                   bent[i].name_hash,
                                   xi->rat_entry_id, bent[i].offset,
                                   current_uid().val, current_gid().val,
                                   0, MARUFS_ENTRY_FLAG_NAME_REF,
                                   &eidx);
        if (r == -EEXIST)
        {
            /* Upsert: update offset only if changed */
            r = marufs_index_lookup_hash(sbi, bent[i].name, nlen,
                                       bent[i].name_hash, &existing);
            if (r == 0 &&
                (le16_to_cpu(existing->flags) & MARUFS_ENTRY_FLAG_NAME_REF) &&
                READ_LE32(existing->region_id) == xi->rat_entry_id)
            {
                if (READ_LE64(existing->file_size) != bent[i].offset)
                {
                    WRITE_LE64(existing->file_size, bent[i].offset);
                    MARUFS_CXL_WMB(existing, sizeof(*existing));
                }
                bent[i].status = 0;
                breq.stored++;
                continue;
            }
            bent[i].status = r ? r : -EEXIST;
            continue;
        }
        else if (r < 0)
        {
            bent[i].status = r;
            continue;
        }

        bent[i].status = 0;
        breq.stored++;
        new_inserts++;
    }

    /* Bulk increment name_ref_count for new inserts (not upserts) */
    if (new_inserts > 0)
    {
        struct marufs_rat_entry* rat_e = marufs_rat_get(sbi, xi->rat_entry_id);
        if (rat_e)
            marufs_rat_name_ref_adjust(rat_e, (s32)new_inserts);
    }

    if (copy_to_user(
            (void __user*)u64_to_user_ptr(breq.entries),
            bent, buf_size))
    {
        kvfree(bent);
        return -EFAULT;
    }

    kvfree(bent);

    if (copy_to_user((void __user*)arg, &breq, sizeof(breq)))
        return -EFAULT;
    return 0;
}

/* ── ioctl handler: CLEAR_NAME (remove name-ref) ────────────────────── */
static long marufs_ioctl_clear_name(struct marufs_sb_info* sbi,
                                  u32 caller_rat_entry_id,
                                  unsigned long arg)
{
    struct marufs_name_offset_req req;
    struct marufs_index_entry_hot* entry;
    size_t name_len;
    int ret;

    if (copy_from_user(&req, (void __user*)arg, sizeof(req)))
        return -EFAULT;

    req.name[sizeof(req.name) - 1] = '\0';
    name_len = strnlen(req.name, sizeof(req.name));

    /* Lookup entry using user-provided hash (user may have registered with custom hash) */
    ret = marufs_index_lookup_hash(sbi, req.name, name_len,
                                   req.name_hash, &entry);
    if (ret)
        return ret;

    MARUFS_CXL_RMB(entry, sizeof(*entry));

    /* Only allow clearing name-ref entries that belong to caller's region */
    if (!(le16_to_cpu(READ_ONCE(entry->flags)) & MARUFS_ENTRY_FLAG_NAME_REF))
        return -EINVAL; /* Not a name-ref entry */

    if (READ_LE32(entry->region_id) != caller_rat_entry_id)
        return -EACCES; /* Name-ref belongs to a different region */

    /* CAS: VALID → TOMBSTONE */
    if (cmpxchg(&entry->state, MARUFS_ENTRY_VALID_LE,
                MARUFS_ENTRY_TOMBSTONE_LE) != MARUFS_ENTRY_VALID_LE)
        return -ENOENT;

    MARUFS_CXL_WMB(entry, sizeof(*entry));

    /* Track shard counts — use entry's stored hash for correct shard */
    {
        u64 hash = READ_LE64(entry->name_hash);
        u32 shard_id = marufs_shard_id(hash, sbi->shard_mask);
        marufs_le32_cas_inc(&sbi->shard_table[shard_id].tombstone_entries);
    }

    /* Decrement name_ref_count on RAT entry */
    {
        struct marufs_rat_entry* rat_e = marufs_rat_get(sbi, caller_rat_entry_id);
        if (rat_e)
            marufs_rat_name_ref_adjust(rat_e, -1);
    }

    return 0;
}

/* ── ioctl handler: PERM_GRANT ──────────────────────────────────────── */
static long marufs_ioctl_perm_grant(struct marufs_sb_info* sbi,
                                  struct marufs_inode_info* xi,
                                  unsigned long arg)
{
    struct marufs_perm_req preq;
    int ret;

    if (copy_from_user(&preq, (void __user*)arg, sizeof(preq)))
        return -EFAULT;

    /* ADMIN can grant anything — check first */
    if (marufs_check_permission(sbi, xi->rat_entry_id,
                                MARUFS_PERM_ADMIN) != 0)
    {
        /* GRANT can grant, but not ADMIN or GRANT itself */
        ret = marufs_check_permission(sbi, xi->rat_entry_id,
                                      MARUFS_PERM_GRANT);
        if (ret)
            return ret;

        if (preq.perms & (MARUFS_PERM_ADMIN | MARUFS_PERM_GRANT))
            return -EPERM;
    }

    ret = marufs_deleg_grant(sbi, xi->rat_entry_id, &preq);
    if (ret)
        return ret;

    pr_debug("granted perms=0x%x to node=%u pid=%u on rat_entry %u\n",
             preq.perms, preq.node_id, preq.pid, xi->rat_entry_id);
    return 0;
}

/* ── ioctl handler: CHOWN (ownership transfer to caller) ───────────── */
static long marufs_ioctl_chown(struct marufs_sb_info* sbi,
                              struct marufs_inode_info* xi,
                              unsigned long arg)
{
    struct marufs_chown_req req;
    struct marufs_rat_entry* rat_entry;
    struct marufs_region_header* rhdr;
    struct marufs_deleg_table* dt;
    u32 old_state;
    u32 num_entries, i;
    int ret;

    if (copy_from_user(&req, (void __user*)arg, sizeof(req)))
        return -EFAULT;

    if (req.reserved != 0)
        return -EINVAL;

    /* Require ADMIN permission (owner or delegated) */
    ret = marufs_check_permission(sbi, xi->rat_entry_id, MARUFS_PERM_ADMIN);
    if (ret)
        return ret;

    rat_entry = marufs_rat_get(sbi, xi->rat_entry_id);
    if (!rat_entry)
        return -EIO;

    /*
     * Atomic ownership transfer: CAS ALLOCATED → ALLOCATING to prevent
     * GC from observing a partially written ownership state.
     * GC skips ALLOCATING entries until alloc_time timeout (30s).
     */
    old_state = CAS_LE32(&rat_entry->state,
                         MARUFS_RAT_ENTRY_ALLOCATED,
                         MARUFS_RAT_ENTRY_ALLOCATING);
    if (old_state != MARUFS_RAT_ENTRY_ALLOCATED)
        return -EAGAIN;

    /* Refresh alloc_time to reset GC stale-lock timeout */
    WRITE_LE64(rat_entry->alloc_time, ktime_get_real_ns());
    MARUFS_CXL_WMB(rat_entry, sizeof(*rat_entry));

    /* Update ownership fields (safe from GC while in ALLOCATING state) */
    WRITE_LE32(rat_entry->owner_node_id, sbi->node_id);
    WRITE_LE32(rat_entry->owner_pid, current->tgid);
    WRITE_LE64(rat_entry->owner_birth_time,
               ktime_to_ns(current->group_leader->start_boottime));

    /* Reset default_perms: clear previous owner's settings */
    WRITE_LE32(rat_entry->default_perms, 0);
    MARUFS_CXL_WMB(rat_entry, sizeof(*rat_entry));

    /* Clear delegation table: revoke all grants from previous owner */
    rhdr = marufs_region_hdr(sbi, xi->rat_entry_id);
    if (rhdr)
    {
        dt = &rhdr->deleg_table;
        MARUFS_CXL_RMB(dt, sizeof(*dt));

        if (READ_LE32(dt->magic) == MARUFS_DELEG_MAGIC)
        {
            num_entries = READ_LE32(dt->max_entries);
            if (num_entries > MARUFS_DELEG_MAX_ENTRIES)
                num_entries = MARUFS_DELEG_MAX_ENTRIES;

            for (i = 0; i < num_entries; i++)
            {
                struct marufs_deleg_entry* de = &dt->entries[i];

                if (READ_LE32(de->state) == MARUFS_DELEG_ACTIVE)
                {
                    marufs_deleg_entry_clear(de);
                    WRITE_LE32(de->state, MARUFS_DELEG_EMPTY);
                }
            }
            WRITE_LE32(dt->num_entries, 0);
            MARUFS_CXL_WMB(dt, sizeof(*dt));
        }
    }

    /* Publish: ALLOCATING → ALLOCATED (ownership transfer complete) */
    WRITE_LE32(rat_entry->state, MARUFS_RAT_ENTRY_ALLOCATED);
    MARUFS_CXL_WMB(rat_entry, sizeof(*rat_entry));

    pr_info_ratelimited("chown rat_entry %u -> node=%u pid=%d\n",
                        xi->rat_entry_id, sbi->node_id, current->tgid);
    return 0;
}

/* ── ioctl handler: PERM_SET_DEFAULT ────────────────────────────────── */
static long marufs_ioctl_perm_set_default(struct marufs_sb_info* sbi,
                                        struct marufs_inode_info* xi,
                                        unsigned long arg)
{
    struct marufs_perm_req preq;
    struct marufs_rat_entry* rat_entry;
    int ret;

    if (copy_from_user(&preq, (void __user*)arg, sizeof(preq)))
        return -EFAULT;

    ret = marufs_check_permission(sbi, xi->rat_entry_id, MARUFS_PERM_ADMIN);
    if (ret)
    {
        pr_debug("PERM_SET_DEFAULT denied - rat_entry %u (pid=%d)\n",
                 xi->rat_entry_id, current->tgid);
        return ret;
    }

    if (preq.perms & ~MARUFS_PERM_ALL)
        return -EINVAL;

    rat_entry = marufs_rat_get(sbi, xi->rat_entry_id);
    if (!rat_entry)
        return -EIO;
    WRITE_LE32(rat_entry->default_perms, preq.perms);
    MARUFS_CXL_WMB(rat_entry, sizeof(*rat_entry));

    pr_debug("set default_perms=0x%x on rat_entry %u\n",
             preq.perms, xi->rat_entry_id);
    return 0;
}

static const char* marufs_ioctl_name(unsigned int cmd)
{
    switch (cmd)
    {
        case MARUFS_IOC_NAME_OFFSET:
            return "NAME_OFFSET";
        case MARUFS_IOC_BATCH_NAME_OFFSET:
            return "BATCH_NAME_OFFSET";
        case MARUFS_IOC_FIND_NAME:
            return "FIND_NAME";
        case MARUFS_IOC_BATCH_FIND_NAME:
            return "BATCH_FIND_NAME";
        case MARUFS_IOC_CLEAR_NAME:
            return "CLEAR_NAME";
        case MARUFS_IOC_PERM_GRANT:
            return "PERM_GRANT";
        case MARUFS_IOC_PERM_SET_DEFAULT:
            return "PERM_SET_DEFAULT";
        case MARUFS_IOC_CHOWN:
            return "CHOWN";
        case MARUFS_IOC_DMABUF_EXPORT:
            return "DMABUF_EXPORT";
        default:
            return "UNKNOWN";
    }
}

static long marufs_ioctl(struct file* file, unsigned int cmd, unsigned long arg)
{
    struct inode* inode = file_inode(file);
    struct marufs_inode_info* xi = MARUFS_I(inode);
    struct marufs_sb_info* sbi = MARUFS_SB(inode->i_sb);
    int ret;

    pr_debug("ioctl %s (0x%x) pid=%d rat=%u\n",
             marufs_ioctl_name(cmd), cmd, current->tgid, xi->rat_entry_id);

    switch (cmd)
    {
        case MARUFS_IOC_NAME_OFFSET:
            ret = marufs_check_permission(sbi, xi->rat_entry_id, MARUFS_PERM_IOCTL);
            if (ret)
                return ret;
            return marufs_ioctl_name_offset(sbi, xi, arg);

        case MARUFS_IOC_BATCH_NAME_OFFSET:
            ret = marufs_check_permission(sbi, xi->rat_entry_id, MARUFS_PERM_IOCTL);
            if (ret)
                return ret;
            return marufs_ioctl_batch_name_offset(sbi, xi, arg);

        case MARUFS_IOC_FIND_NAME:
        case MARUFS_IOC_BATCH_FIND_NAME:
            return marufs_dir_ioctl(file, cmd, arg);

        case MARUFS_IOC_CLEAR_NAME:
            ret = marufs_check_permission(sbi, xi->rat_entry_id, MARUFS_PERM_IOCTL);
            if (ret)
                return ret;
            return marufs_ioctl_clear_name(sbi, xi->rat_entry_id, arg);

        case MARUFS_IOC_PERM_GRANT:
            return marufs_ioctl_perm_grant(sbi, xi, arg);

        case MARUFS_IOC_PERM_SET_DEFAULT:
            return marufs_ioctl_perm_set_default(sbi, xi, arg);

        case MARUFS_IOC_CHOWN:
            return marufs_ioctl_chown(sbi, xi, arg);

        case MARUFS_IOC_DMABUF_EXPORT:
            return marufs_ioctl_dmabuf_export(sbi, xi, arg);

        default:
            return -ENOTTY;
    }
}

/*
 * marufs_get_unmapped_area - get aligned virtual address for mmap
 *
 * DEV_DAX mode: delegate to device_dax's get_unmapped_area so the
 * kernel allocates a 2MB-aligned address. Without this, mmap() picks
 * a 4KB-aligned address and device_dax's mmap handler rejects it.
 */
static unsigned long marufs_get_unmapped_area(struct file* file,
                                            unsigned long addr,
                                            unsigned long len,
                                            unsigned long pgoff,
                                            unsigned long flags)
{
    struct inode* inode = file_inode(file);
    struct marufs_sb_info* sbi = MARUFS_SB(inode->i_sb);

    if (sbi->dax_mode == MARUFS_DAX_DEV && sbi->dax_filp &&
        sbi->dax_filp->f_op && sbi->dax_filp->f_op->get_unmapped_area)
    {
        return sbi->dax_filp->f_op->get_unmapped_area(
            sbi->dax_filp, addr, len, pgoff, flags);
    }

    return mm_get_unmapped_area(current->mm, file, addr, len, pgoff, flags);
}

/* ============================================================================
 * File operations table
 * ============================================================================
 *
 * Function pointer table called by VFS for file operations
 */
const struct file_operations marufs_file_ops = {
    .owner = THIS_MODULE,
    .llseek = marufs_llseek,                       /* lseek() */
    .read_iter = marufs_read_iter,                 /* read() */
    .write_iter = marufs_write_iter,               /* write() - WORM: returns -EACCES */
    .mmap = marufs_mmap,                           /* mmap() */
    .get_unmapped_area = marufs_get_unmapped_area, /* 2MB alignment for DEV_DAX */
    .open = marufs_open,                           /* open() */
    .release = marufs_release,                     /* close() */
    .fsync = noop_fsync,                         /* fsync() - WORM: nothing to sync */
    .unlocked_ioctl = marufs_ioctl,                /* ioctl() - slot naming ops */
};
