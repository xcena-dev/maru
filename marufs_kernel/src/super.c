// SPDX-License-Identifier: GPL-2.0-only
/*
 * super.c - MARUFS Partitioned Global Index superblock and mount
 *
 * Architecture:
 * - Global Superblock (4 KB) at offset 0
 * - Shard Table (num_shards x 64B headers)
 * - Index Pool (per-shard bucket + entry arrays)
 * - Regions (per-node data: header + bitmap + slots)
 *
 * All nodes access the global index via CAS operations on CXL shared memory.
 * No GCS (Global Chunk Server) is required.
 */

#include <linux/blkdev.h>
#include <linux/crc32.h>
#include <linux/dax.h>
#include <linux/dma-buf.h>
#include <linux/fs.h>
#include <linux/io.h>
#include <linux/iversion.h>
#include <linux/log2.h>
#include <linux/module.h>
#include <linux/parser.h>
#include <linux/seq_file.h>
#include <linux/slab.h>
#include <linux/statfs.h>
#include <linux/time64.h>
#include <linux/uio.h>
#include <linux/vmalloc.h>

#include "compat.h"
#include "marufs.h"

/* Module parameter: node_id (must be > 0) */
int marufs_node_id = 1;
module_param_named(node_id, marufs_node_id, int, 0444);
MODULE_PARM_DESC(node_id,
		 "Node ID for multi-node access control (must be > 0)");

/* DAXHEAP multi-mount sharing: buf_id based, refcount for lifecycle */
#ifdef CONFIG_DAXHEAP
static DEFINE_MUTEX(marufs_daxheap_lock);
u64 marufs_daxheap_bufid; /* buf_id from primary alloc (0 = none) */
static atomic_t marufs_daxheap_mounts = ATOMIC_INIT(0);
#endif

/* Free per-shard DRAM resources */
static void marufs_free_shard_resources(struct marufs_sb_info *sbi)
{
	kvfree(sbi->shard_cache);
	sbi->shard_cache = NULL;
}

/* Mount option parsing */
enum MARUFS_MOUNT_OPTION {
	OPTION_NODE_ID,
	OPTION_DAXDEV,
	OPTION_DAXHEAP,
	OPTION_DAXHEAP_IMPORT,
	OPTION_FORMAT,
	OPTION_ERROR,
};

static const match_table_t marufs_tokens = {
	{ OPTION_NODE_ID, "node_id=%d" },
	{ OPTION_DAXDEV, "daxdev=%s" },
	{ OPTION_DAXHEAP, "daxheap=%s" },
	{ OPTION_DAXHEAP_IMPORT, "daxheap_import_id=%s" },
	{ OPTION_FORMAT, "format" },
	{ OPTION_ERROR, NULL },
};

struct marufs_mount_opts {
	int node_id;
	char daxdev[128];
	bool format; /* in-kernel format on mount */
	bool use_daxheap;
	u64 daxheap_size;
	u64 daxheap_bufid; /* secondary: import existing buffer by ID */
};

static int marufs_parse_options(char *options, struct marufs_mount_opts *opts)
{
	substring_t args[MAX_OPT_ARGS];
	char *p;
	int token;
	int option;

	/* Set defaults from module parameter */
	opts->node_id = marufs_node_id;
	opts->daxdev[0] = '\0';
	opts->use_daxheap = false;
	opts->format = false;
	opts->daxheap_size = 0;
	opts->daxheap_bufid = 0;

	if (!options)
		return 0;

	while ((p = strsep(&options, ",")) != NULL) {
		if (!*p)
			continue;

		token = match_token(p, marufs_tokens, args);
		switch (token) {
		case OPTION_NODE_ID:
			if (match_int(&args[0], &option))
				return -EINVAL;
			if (option <= 0) {
				pr_err("node_id must be > 0 (got %d)\n",
				       option);
				return -EINVAL;
			}
			opts->node_id = option;
			break;
		case OPTION_DAXDEV:
			match_strlcpy(opts->daxdev, &args[0],
				      sizeof(opts->daxdev));
			break;
		case OPTION_DAXHEAP: {
			char size_str[64];

			match_strlcpy(size_str, &args[0], sizeof(size_str));
			opts->daxheap_size = memparse(size_str, NULL);
			if (opts->daxheap_size == 0) {
				pr_err("daxheap= requires non-zero size (use daxheap_bufid= for secondary)\n");
				return -EINVAL;
			}
			opts->use_daxheap = true;
			break;
		}
		case OPTION_DAXHEAP_IMPORT: {
			char id_str[32];
			int ret;

			match_strlcpy(id_str, &args[0], sizeof(id_str));
			ret = kstrtoull(id_str, 0, &opts->daxheap_bufid);
			if (ret || opts->daxheap_bufid == 0) {
				pr_err("daxheap_bufid= requires non-zero buffer ID\n");
				return -EINVAL;
			}
			opts->use_daxheap = true;
			break;
		}
		case OPTION_FORMAT:
			opts->format = true;
			break;
		default:
			pr_err("unrecognized mount option: %s\n", p);
			return -EINVAL;
		}
	}

	return 0;
}

/* inode cache */
static struct kmem_cache *marufs_inode_cachep;

/* ============================================================================
 * inode cache management
 * ============================================================================ */

static struct inode *marufs_alloc_inode(struct super_block *sb)
{
	struct marufs_inode_info *xi;

	xi = kmem_cache_alloc(marufs_inode_cachep, GFP_KERNEL);
	if (!xi) {
		return NULL;
	}

	marufs_inode_info_init(xi);

	return &xi->vfs_inode;
}

static void marufs_free_inode(struct inode *inode)
{
	kmem_cache_free(marufs_inode_cachep, marufs_inode_get(inode));
}

/* ============================================================================
 * Filesystem statistics
 * ============================================================================ */

static int marufs_statfs(struct dentry *dentry, struct kstatfs *buf)
{
	struct super_block *sb = dentry->d_sb;
	struct marufs_sb_info *sbi = marufs_sb_get(sb);
	u64 used_size = 0;
	u32 used_entries = 0;
	u32 i;

	/* RAT: compute used size and entry count */
	for (i = 0; i < MARUFS_MAX_RAT_ENTRIES; i++) {
		struct marufs_rat_entry *entry = marufs_rat_entry_get(sbi, i);
		if (!entry)
			return -EIO;

		u32 state = READ_LE32(entry->state);
		if (state == MARUFS_RAT_ENTRY_ALLOCATED ||
		    state == MARUFS_RAT_ENTRY_DELETING) {
			used_size += READ_LE64(entry->size);
			used_entries++;
		}
	}

	buf->f_files = MARUFS_MAX_RAT_ENTRIES;
	buf->f_ffree = MARUFS_MAX_RAT_ENTRIES - used_entries;

	buf->f_type = MARUFS_MAGIC;
	buf->f_bsize = PAGE_SIZE;
	buf->f_blocks = sbi->total_size / PAGE_SIZE;
	buf->f_bfree = (sbi->total_size - used_size) / PAGE_SIZE;
	buf->f_bavail = buf->f_bfree;
	buf->f_namelen = MARUFS_NAME_MAX;

	return 0;
}

/* ============================================================================
 * Superblock operations table
 * ============================================================================ */

static int marufs_show_options(struct seq_file *m, struct dentry *root)
{
	struct marufs_sb_info *sbi = root->d_sb->s_fs_info;

	seq_printf(m, ",node_id=%u", sbi->node_id);

	if (sbi->dax_mode == MARUFS_DAX_DEV && sbi->daxdev_path[0])
		seq_printf(m, ",daxdev=%s", sbi->daxdev_path);
	else if (sbi->dax_mode == MARUFS_DAX_HEAP)
		seq_puts(m, ",daxheap");
	else if (sbi->use_dax)
		seq_puts(m, ",dax");

	return 0;
}

static const struct super_operations marufs_sops = {
	.alloc_inode = marufs_alloc_inode,
	.free_inode = marufs_free_inode,
	.write_inode = marufs_write_inode,
	.evict_inode = marufs_evict_inode,
	.drop_inode = generic_delete_inode,
	.statfs = marufs_statfs,
	.show_options = marufs_show_options,
};

/* ============================================================================
 * Dentry operations - dcache invalidation for cross-node visibility
 * ============================================================================
 *
 * Since MARUFS is based on CXL shared memory, other nodes can create/delete files.
 * Always fail revalidation so VFS re-calls lookup for fresh data.
 */

static int marufs_d_revalidate(MARUFS_D_REVALIDATE_ARGS)
{
	/* Always return 0 -> VFS re-calls lookup -> read fresh data from CXL */
	return 0;
}

static const struct dentry_operations marufs_dentry_ops = {
	.d_revalidate = marufs_d_revalidate,
};

/* ============================================================================
 * Unified DAX abstraction layer
 * ============================================================================
 *
 * marufs_dax_acquire() / marufs_dax_release()
 *
 * Both DEV_DAX (character device) and DAXHEAP produce:
 *   sbi->dax_base  = mapped memory pointer
 *   sbi->use_dax   = true
 *   sbi->total_size = total size
 *
 * After that, all filesystem logic only references sbi->dax_base and use_dax.
 */

/* Helper: read u64 value from sysfs */
static int marufs_read_sysfs_u64(const char *path, u64 *out)
{
	struct file *f;
	char buf[64];
	loff_t pos = 0;
	ssize_t len;

	f = filp_open(path, O_RDONLY, 0);
	if (IS_ERR(f))
		return PTR_ERR(f);

	memset(buf, 0, sizeof(buf));
	len = kernel_read(f, buf, sizeof(buf) - 1, &pos);
	filp_close(f, NULL);

	if (len <= 0)
		return -EIO;

	buf[len] = '\0';
	return kstrtoull(buf, 0, out) < 0 ? -EINVAL : 0;
}

/* DEV_DAX: read physical address/size from sysfs and perform memremap */
static int marufs_dax_acquire_devdax(struct marufs_sb_info *sbi,
				     const char *devpath)
{
	const char *devname;
	char sysfs_path[256];
	u64 phys_addr, dev_size;
	void *mapped;
	int ret;

	devname = strrchr(devpath, '/');
	devname = devname ? devname + 1 : devpath;

	pr_debug("acquiring DEV_DAX device %s via memremap\n", devname);

	snprintf(sysfs_path, sizeof(sysfs_path),
		 "/sys/bus/dax/devices/%s/resource", devname);
	ret = marufs_read_sysfs_u64(sysfs_path, &phys_addr);
	if (ret) {
		pr_err("cannot read resource for %s (%d)\n", devname, ret);
		return ret;
	}

	snprintf(sysfs_path, sizeof(sysfs_path), "/sys/bus/dax/devices/%s/size",
		 devname);
	ret = marufs_read_sysfs_u64(sysfs_path, &dev_size);
	if (ret || dev_size == 0) {
		pr_err("cannot read size for %s (%d)\n", devname, ret);
		return ret ? ret : -EINVAL;
	}

	pr_debug("%s phys=0x%llx size=%llu (%llu MB)\n", devname, phys_addr,
		 dev_size, dev_size >> 20);

	mapped = memremap(phys_addr, dev_size, MEMREMAP_WB);
	if (!mapped) {
		pr_err("memremap failed for %s\n", devname);
		return -ENOMEM;
	}

	sbi->dax_base = mapped;
	sbi->phys_base = phys_addr; /* Store physical address for DAX mmap */
	sbi->dax_nr_pages = dev_size >> PAGE_SHIFT;
	sbi->total_size = dev_size;
	strscpy(sbi->daxdev_path, devpath, sizeof(sbi->daxdev_path));

	/*
	 * Detect ZONE_DEVICE struct pages
	 *
	 * If the device_dax driver has already created ZONE_DEVICE struct pages
	 * via devm_memremap_pages(), use VM_MIXEDMAP + vmf_insert_mixed() path
	 * so get_user_pages() works. This enables GPU DMA (cudaMemcpy etc.)
	 * to transfer directly from CXL memory without a bounce buffer.
	 */
	sbi->has_struct_pages = pfn_valid(phys_addr >> PAGE_SHIFT);
	if (sbi->has_struct_pages)
		pr_debug(
			"DEV_DAX %s - ZONE_DEVICE pages detected, VM_MIXEDMAP enabled (GPU direct DMA capable)\n",
			devname);
	else
		pr_debug(
			"DEV_DAX %s - no struct pages, using VM_PFNMAP (GPU DMA via bounce buffer)\n",
			devname);

	/* Open DAX device file for mmap delegation to device_dax driver.
	 * This allows NVIDIA driver to recognize the VMA as device_dax-backed,
	 * enabling cudaHostRegister on CXL memory. */
	sbi->dax_filp = filp_open(devpath, O_RDWR, 0);
	if (IS_ERR(sbi->dax_filp)) {
		pr_warn("failed to open DAX device %s for mmap: %ld\n", devpath,
			PTR_ERR(sbi->dax_filp));
		sbi->dax_filp = NULL;
	}

	pr_debug("DEV_DAX %s acquired (%llu bytes, mapped at %p)\n", devname,
		 dev_size, mapped);
	return 0;
}

/*
 * DAXHEAP: allocate full-device buffer from daxheap kernel API.
 *
 * daxheap provides WC (Write-Combining) mmap for GPU high-bandwidth access
 * (~57.8 GB/s vs ~17 GB/s with memremap WB→UC).
 *
 * Uses standard dma_buf kernel interface:
 *   daxheap_kern_alloc() → dma_buf* (contiguous CXL buffer)
 *   dma_buf_vmap()       → kernel VA for metadata access (WB)
 *   dma_buf_mmap()       → userspace mapping (WC, handled by daxheap)
 */
static int marufs_dax_acquire_daxheap(struct marufs_sb_info *sbi, u64 bufid)
{
#ifdef CONFIG_DAXHEAP
	struct dma_buf *dmabuf;
	u64 allocated_id;
	int ret;

	if (bufid == 0) {
		/* Primary mount: allocate new buffer (sbi->total_size > 0)
		 * Hold lock through alloc to prevent TOCTOU double allocation */
		mutex_lock(&marufs_daxheap_lock);
		if (marufs_daxheap_bufid != 0) {
			mutex_unlock(&marufs_daxheap_lock);
			pr_err("DAXHEAP buffer already allocated (bufid=0x%llx), use daxheap_bufid= for secondary\n",
			       marufs_daxheap_bufid);
			return -EEXIST;
		}

		dmabuf = daxheap_kern_alloc(sbi->total_size, 0);
		if (IS_ERR(dmabuf)) {
			mutex_unlock(&marufs_daxheap_lock);
			pr_err("daxheap_kern_alloc failed: %ld\n",
			       PTR_ERR(dmabuf));
			return PTR_ERR(dmabuf);
		}

		ret = daxheap_kern_get_id(dmabuf, &allocated_id);
		if (ret) {
			mutex_unlock(&marufs_daxheap_lock);
			pr_err("daxheap_kern_get_id failed: %d\n", ret);
			daxheap_kern_free(dmabuf);
			return ret;
		}

		/* Grant all hosts read+write access so secondary mounts can import */
		ret = daxheap_kern_grant(dmabuf, DAXHEAP_KERN_GRANT_ANY_HOST,
					 DAXHEAP_KERN_GRANT_READ |
						 DAXHEAP_KERN_GRANT_WRITE);
		if (ret) {
			mutex_unlock(&marufs_daxheap_lock);
			pr_err("daxheap_kern_grant failed: %d\n", ret);
			daxheap_kern_free(dmabuf);
			return ret;
		}

		marufs_daxheap_bufid = allocated_id;
		atomic_inc(&marufs_daxheap_mounts);
		mutex_unlock(&marufs_daxheap_lock);

		pr_debug(
			"DAXHEAP primary: allocated %zu bytes (bufid=0x%llx, granted all hosts)\n",
			dmabuf->size, allocated_id);
	} else {
		/* Secondary mount: import existing buffer by ID */
		dmabuf = daxheap_kern_import(bufid);
		if (IS_ERR(dmabuf)) {
			pr_err("daxheap_kern_import(0x%llx) failed: %ld\n",
			       bufid, PTR_ERR(dmabuf));
			return PTR_ERR(dmabuf);
		}

		mutex_lock(&marufs_daxheap_lock);
		atomic_inc(&marufs_daxheap_mounts);
		mutex_unlock(&marufs_daxheap_lock);

		pr_debug(
			"DAXHEAP secondary: imported bufid=0x%llx (%zu bytes)\n",
			bufid, dmabuf->size);
	}

	/* Kernel vmap for metadata access (WB — CAS/barriers work normally) */
	ret = dma_buf_vmap(dmabuf, &sbi->heap_map);
	if (ret) {
		pr_err("dma_buf_vmap failed: %d\n", ret);
		mutex_lock(&marufs_daxheap_lock);
		if (atomic_dec_and_test(&marufs_daxheap_mounts)) {
			marufs_daxheap_bufid = 0;
		}
		mutex_unlock(&marufs_daxheap_lock);
		dma_buf_put(dmabuf);
		return ret;
	}

	/*
	 * Skip daxheap shared metadata area.
	 *
	 * daxheap stores its own metadata (primary 64KB + mirror 64KB = 128KB)
	 * at offset 0 of the CXL region.  The chunk allocator does not reserve
	 * these chunks, so our buffer starts at region->vaddr.  If we write
	 * MARUFS metadata at offset 0 we collide with daxheap's heartbeat and
	 * allocation table updates, causing deterministic shard corruption.
	 *
	 */
	sbi->dax_base = sbi->heap_map.vaddr;
	sbi->total_size = dmabuf->size;
	sbi->dax_nr_pages = sbi->total_size >> PAGE_SHIFT;
	sbi->heap_dmabuf = dmabuf;

	pr_debug("DAXHEAP buffer acquired: size=%zu dax_base=%p mounts=%d\n",
		 dmabuf->size, sbi->dax_base,
		 atomic_read(&marufs_daxheap_mounts));
	return 0;
#else
	pr_err("daxheap support not compiled (CONFIG_DAXHEAP not set)\n");
	return -ENOSYS;
#endif
}

/*
 * marufs_gsb_checksum - CRC32 over immutable superblock fields.
 *
 * Covers: magic, version, total_size, shard_table_offset, rat_offset,
 *         num_shards, buckets_per_shard, entries_per_shard.
 * Skips: checksum (self), reserved.
 */
static u32 marufs_gsb_checksum(const struct marufs_superblock *gsb)
{
	return crc32(~0, (const u8 *)gsb,
		     offsetof(struct marufs_superblock, checksum));
}

/*
 * marufs_format_device - in-kernel format for DEV_DAX and DAXHEAP modes.
 *
 * Initialises filesystem metadata directly on the mapped memory.
 * Layout: superblock + shard table + bucket/entry arrays + RAT.
 *
 * Uses memset_io for DAXHEAP (WC-mapped), memset for DEV_DAX (WB memremap).
 */
static int marufs_format_device(struct marufs_sb_info *sbi)
{
	void *base = sbi->dax_base;
	u64 total = sbi->total_size;
	u32 num_shards = MARUFS_REGION_NUM_SHARDS;
	u32 entries_per_shard = MARUFS_REGION_ENTRIES_PER_SHARD;
	u32 buckets_per_shard = MARUFS_REGION_BUCKETS_PER_SHARD;
	u64 bucket_array_start, total_bucket_bytes;
	u64 entry_array_start, total_entry_bytes;
	u64 index_pool_end;
	u64 rat_offset, rat_size, regions_start;
	struct marufs_superblock *gsb;
	struct marufs_rat *rat;
	u32 i;
	u32 *bucket_base;

	pr_debug("formatting %s (%llu bytes, %llu MB)\n",
		 sbi->dax_mode == MARUFS_DAX_HEAP ? "DAXHEAP buffer" :
						    "DEV_DAX device",
		 total, total >> 20);

	/* --- Layout calculation --- */
	bucket_array_start = MARUFS_INDEX_BUCKET_OFFSET;
	total_bucket_bytes = (u64)num_shards * buckets_per_shard * sizeof(u32);
	entry_array_start = bucket_array_start + total_bucket_bytes;
	total_entry_bytes =
		(u64)num_shards * entries_per_shard * MARUFS_INDEX_ENTRY_SIZE;
	index_pool_end = entry_array_start + total_entry_bytes;
	rat_offset = index_pool_end;
	rat_size = sizeof(struct marufs_rat);
	regions_start =
		marufs_align_up(rat_offset + rat_size, MARUFS_ALIGN_2MB);

	if (regions_start >= total) {
		pr_err("device too small for metadata (%llu < %llu)\n", total,
		       regions_start);
		return -ENOSPC;
	}

	if (entry_array_start != MARUFS_INDEX_ENTRY_OFFSET ||
	    rat_offset != MARUFS_RAT_OFFSET ||
	    regions_start != MARUFS_REGION_OFFSET) {
		pr_err("FORMAT FAILED — layout mismatched\n");
		return -EIO;
	}

	/* --- Zero the metadata area --- */
	if (sbi->dax_mode == MARUFS_DAX_HEAP)
		memset_io(base, 0, regions_start); /* WC-mapped memory */
	else
		memset(base, 0, regions_start); /* WB memremap */
	MARUFS_CXL_WMB(base, regions_start);

	/* --- Step 1: Superblock --- */
	gsb = marufs_gsb_get(sbi);
	WRITE_LE32(gsb->magic, MARUFS_MAGIC);
	WRITE_LE32(gsb->version, MARUFS_VERSION);
	WRITE_LE64(gsb->total_size, total);
	WRITE_LE32(gsb->num_shards, num_shards);
	WRITE_LE32(gsb->buckets_per_shard, buckets_per_shard);
	WRITE_LE32(gsb->entries_per_shard, entries_per_shard);
	WRITE_LE64(gsb->shard_table_offset, MARUFS_SHARD_TABLE_OFFSET);
	WRITE_LE64(gsb->rat_offset, rat_offset);
	WRITE_LE32(gsb->checksum, marufs_gsb_checksum(gsb));

	/* --- Step 2: Shard table --- */
	for (i = 0; i < num_shards; i++) {
		u64 off = MARUFS_SHARD_TABLE_OFFSET +
			  (u64)i * MARUFS_SHARD_HEADER_SIZE;
		struct marufs_shard_header *sh =
			(struct marufs_shard_header *)((char *)base + off);

		WRITE_LE32(sh->magic, MARUFS_SHARD_MAGIC);
		WRITE_LE32(sh->shard_id, i);
		WRITE_LE32(sh->num_buckets, buckets_per_shard);
		WRITE_LE32(sh->num_entries, entries_per_shard);
		WRITE_LE64(sh->bucket_array_offset,
			   bucket_array_start +
				   (u64)i * buckets_per_shard * sizeof(u32));
		WRITE_LE64(sh->entry_array_offset,
			   entry_array_start + (u64)i * entries_per_shard *
						       MARUFS_INDEX_ENTRY_SIZE);
	}

	/* --- Step 3: Bucket arrays (all MARUFS_BUCKET_END) --- */
	bucket_base = (u32 *)((char *)base + bucket_array_start);
	for (i = 0; i < num_shards * buckets_per_shard; i++)
		WRITE_LE32(bucket_base[i], MARUFS_BUCKET_END);

	/* --- Step 4: Entry arrays already zeroed (ENTRY_EMPTY = 0) --- */

	/* --- Step 5: RAT --- */
	rat = (struct marufs_rat *)((char *)base + rat_offset);
	WRITE_LE32(rat->magic, MARUFS_RAT_MAGIC);
	WRITE_LE32(rat->version, 1);
	WRITE_LE32(rat->num_entries, 0);
	WRITE_LE32(rat->max_entries, MARUFS_MAX_RAT_ENTRIES);
	WRITE_LE64(rat->device_size, total);
	WRITE_LE64(rat->rat_offset, rat_offset);
	WRITE_LE64(rat->regions_start, regions_start);
	WRITE_LE64(rat->total_allocated, 0);
	WRITE_LE64(rat->total_free, total - regions_start);

	/* Full barrier to ensure all metadata is flushed (WC memory) */
	MARUFS_CXL_WMB(base, regions_start);

	/* --- Verify format: read back critical fields --- */
	{
		struct marufs_shard_header *vsh;
		u32 v_magic, v_buckets, v_entries;
		u32 v_rat_magic;
		u32 v_rat_state0;

		MARUFS_CXL_RMB(base, regions_start);

		/* Check the first shard */
		vsh = (struct marufs_shard_header *)((char *)base +
						     MARUFS_SHARD_TABLE_OFFSET);
		v_magic = READ_CXL_LE32(vsh->magic);
		v_buckets = READ_CXL_LE32(vsh->num_buckets);
		v_entries = READ_CXL_LE32(vsh->num_entries);
		pr_debug(
			"format verify the first shard: magic=0x%x buckets=%u entries=%u\n",
			v_magic, v_buckets, v_entries);

		/* Check the last shard (previously corrupted) */
		vsh = (struct marufs_shard_header
			       *)((char *)base + MARUFS_SHARD_TABLE_OFFSET +
				  (MARUFS_REGION_NUM_SHARDS - 1) *
					  MARUFS_SHARD_HEADER_SIZE);
		v_magic = READ_CXL_LE32(vsh->magic);
		v_buckets = READ_CXL_LE32(vsh->num_buckets);
		v_entries = READ_CXL_LE32(vsh->num_entries);
		pr_debug(
			"format verify the last shard: magic=0x%x buckets=%u entries=%u\n",
			v_magic, v_buckets, v_entries);

		/* Check RAT magic and first entry state */
		v_rat_magic = READ_CXL_LE32(rat->magic);
		v_rat_state0 = READ_CXL_LE32(rat->entries[0].state);
		pr_debug("format verify RAT: magic=0x%x entry[0].state=%u\n",
			 v_rat_magic, v_rat_state0);

		if (v_magic != MARUFS_SHARD_MAGIC ||
		    v_buckets != buckets_per_shard) {
			pr_err("FORMAT VERIFICATION FAILED — WC memory not flushed\n");
			return -EIO;
		}
	}

	pr_info("format complete (shards=%u, entries/shard=%u, rat@0x%llx, regions@0x%llx)\n",
		num_shards, entries_per_shard, rat_offset, regions_start);
	return 0;
}

/*
 * marufs_dax_acquire - unified DAX memory acquisition
 *
 * After return, sbi->dax_base and sbi->use_dax are set.
 * All subsequent filesystem code references only these two fields.
 */
static int marufs_dax_acquire(struct marufs_sb_info *sbi,
			      struct super_block *sb, u64 daxheap_bufid)
{
	int ret;

	if (sbi->dax_mode == MARUFS_DAX_HEAP) {
		ret = marufs_dax_acquire_daxheap(sbi, daxheap_bufid);
	} else if (sbi->dax_mode == MARUFS_DAX_DEV) {
		ret = marufs_dax_acquire_devdax(sbi, sbi->daxdev_path);
	} else {
		pr_err("unsupported DAX mode %d\n", sbi->dax_mode);
		return -EINVAL;
	}

	if (ret) {
		return ret;
	}

	sbi->use_dax = (sbi->dax_base != NULL);
	return 0;
}

/*
 * marufs_dax_release - unified DAX memory release
 *
 * Calls the appropriate release function based on mode.
 */
static void marufs_dax_release(struct marufs_sb_info *sbi,
			       struct super_block *sb)
{
	if (sbi->dax_mode == MARUFS_DAX_HEAP) {
#ifdef CONFIG_DAXHEAP
		if (sbi->heap_dmabuf) {
			dma_buf_vunmap(sbi->heap_dmabuf, &sbi->heap_map);
			dma_buf_put(sbi->heap_dmabuf);
			sbi->heap_dmabuf = NULL;

			mutex_lock(&marufs_daxheap_lock);
			if (atomic_dec_and_test(&marufs_daxheap_mounts)) {
				pr_debug(
					"DAXHEAP last mount released, clearing bufid\n");
				marufs_daxheap_bufid = 0;
			} else {
				pr_debug(
					"DAXHEAP mount released (remaining=%d)\n",
					atomic_read(&marufs_daxheap_mounts));
			}
			mutex_unlock(&marufs_daxheap_lock);
		}
#endif
	} else if (sbi->dax_mode == MARUFS_DAX_DEV) {
		if (sbi->dax_base)
			memunmap(sbi->dax_base);
	}

	sbi->dax_base = NULL;
	sbi->use_dax = false;
}

/* ============================================================================
 * Read and validate Global Superblock
 * ============================================================================ */

static int marufs_read_superblock(struct marufs_sb_info *sbi,
				  struct super_block *sb, int silent)
{
	struct marufs_superblock *gsb;
	u32 magic, version;

	gsb = marufs_gsb_get(sbi);
	if (!gsb)
		return -EINVAL;

	/* Validate magic */
	magic = READ_CXL_LE32(gsb->magic);
	if (magic != MARUFS_MAGIC) {
		if (!silent)
			pr_err("invalid magic 0x%x (expected 0x%x)\n", magic,
			       MARUFS_MAGIC);
		return -EINVAL;
	}

	/* Validate version (v2 required: region header pool layout) */
	version = READ_CXL_LE32(gsb->version);
	if (version != MARUFS_VERSION) {
		if (!silent)
			pr_err("unsupported version %u (expected %u, reformat required)\n",
			       version, MARUFS_VERSION);
		return -EINVAL;
	}

	/* Validate CRC32 (immutable fields only, excludes checksum/reserved) */
	u32 stored = READ_CXL_LE32(gsb->checksum);
	u32 computed = marufs_gsb_checksum(gsb);
	if (stored != computed) {
		if (!silent)
			pr_err("superblock checksum mismatch (stored=0x%x, computed=0x%x)\n",
			       stored, computed);
		return -EINVAL;
	}

	sbi->gsb = gsb;

	/* Copy geometry fields to sbi */
	sbi->total_size = READ_CXL_LE64(gsb->total_size);
	sbi->num_shards = READ_CXL_LE32(gsb->num_shards);
	if (sbi->num_shards == 0 || !is_power_of_2(sbi->num_shards)) {
		pr_err("num_shards %u is not a power of 2\n", sbi->num_shards);
		return -EINVAL;
	}
	sbi->shard_mask = sbi->num_shards - 1;
	sbi->buckets_per_shard = READ_CXL_LE32(gsb->buckets_per_shard);
	if (sbi->buckets_per_shard == 0 ||
	    !is_power_of_2(sbi->buckets_per_shard)) {
		pr_err("buckets_per_shard %u is not a power of 2\n",
		       sbi->buckets_per_shard);
		return -EINVAL;
	}
	sbi->bucket_mask = sbi->buckets_per_shard - 1;
	sbi->entries_per_shard = READ_CXL_LE32(gsb->entries_per_shard);

	pr_debug("superblock validated\n");
	pr_debug("  total_size=%llu, shards=%u, entries/shard=%u\n",
		 sbi->total_size, sbi->num_shards, sbi->entries_per_shard);

	return 0;
}

/* ============================================================================
 * Initialize Shard Table
 * ============================================================================ */

static int marufs_init_shard_table(struct marufs_sb_info *sbi)
{
	u64 shard_table_offset = READ_CXL_LE64(sbi->gsb->shard_table_offset);
	u32 i;

	/* Validate shard_table_offset within DAX range */
	if (!marufs_dax_range_valid(sbi, shard_table_offset,
				    (u64)sbi->num_shards *
					    MARUFS_SHARD_HEADER_SIZE)) {
		pr_err("shard_table_offset 0x%llx out of range\n",
		       shard_table_offset);
		return -EINVAL;
	}

	struct marufs_shard_header *shard_table =
		(struct marufs_shard_header *)((char *)sbi->dax_base +
					       shard_table_offset);

	/*
	 * Allocate shard cache first — header pointers, bucket/entry array
	 * pointers, and free_hint are all consolidated here.
	 * GFP_ZERO ensures free_hint starts at 0.
	 */
	sbi->shard_cache = kvmalloc_array(sbi->num_shards,
					  sizeof(struct marufs_shard_cache),
					  GFP_KERNEL | __GFP_ZERO);
	if (!sbi->shard_cache)
		return -ENOMEM;

	/* Populate header pointers and validate each shard */
	for (i = 0; i < sbi->num_shards; i++) {
		struct marufs_shard_header *sh = &shard_table[i];

		MARUFS_CXL_RMB(sh, sizeof(*sh));
		sbi->shard_cache[i].header = sh;

		if (READ_CXL_LE32(sh->magic) != MARUFS_SHARD_MAGIC) {
			pr_err("bad shard magic at %u (0x%x)\n", i,
			       READ_CXL_LE32(sh->magic));
			marufs_free_shard_resources(sbi);
			return -EINVAL;
		}
		if (READ_CXL_LE32(sh->shard_id) != i) {
			pr_err("shard_id mismatch at %u (got %u)\n", i,
			       READ_CXL_LE32(sh->shard_id));
			marufs_free_shard_resources(sbi);
			return -EINVAL;
		}

		u32 nb = READ_LE32(sh->num_buckets);
		u32 ne = READ_LE32(sh->num_entries);
		if (!marufs_shard_geometry_valid(nb, ne)) {
			pr_err("shard %u geometry invalid: buckets=%u entries=%u (expected %u)\n",
			       i, nb, ne, sbi->entries_per_shard);
			marufs_free_shard_resources(sbi);
			return -EINVAL;
		}

		u64 boff = READ_CXL_LE64(sh->bucket_array_offset);
		u64 entry_off = READ_CXL_LE64(sh->entry_array_offset);

		/* Validate offsets + array sizes within device bounds */
		if (!marufs_dax_range_valid(sbi, boff, (u64)nb * sizeof(u32)) ||
		    !marufs_dax_range_valid(sbi, entry_off,
					    (u64)ne *
						    MARUFS_INDEX_ENTRY_SIZE)) {
			pr_err("shard %u: offset out of bounds "
			       "(bucket=%llu entry=%llu total=%llu)\n",
			       i, boff, entry_off, sbi->total_size);
			marufs_free_shard_resources(sbi);
			return -EINVAL;
		}

		sbi->shard_cache[i].buckets =
			(u32 *)((char *)sbi->dax_base + boff);
		sbi->shard_cache[i].entries =
			(struct marufs_index_entry *)((char *)sbi->dax_base +
						      entry_off);
		spin_lock_init(&sbi->shard_cache[i].insert_lock);
	}

	pr_debug(
		"shard table initialized (%u shards validated, RAM counters + shard cache allocated)\n",
		sbi->num_shards);

	return 0;
}

/* ============================================================================
 * Load and validate RAT (Region Allocation Table)
 * ============================================================================ */

static int marufs_load_rat(struct marufs_sb_info *sbi)
{
	u64 rat_offset;
	struct marufs_rat *rat;
	u32 magic, version;

	/* Get RAT offset from superblock */
	rat_offset = READ_CXL_LE64(sbi->gsb->rat_offset);

	/* RAT is mandatory - fail if not present */
	if (rat_offset == 0) {
		pr_err("RAT not present! rat_offset=0 (filesystem too old or corrupted)\n");
		return -EINVAL;
	}

	/* Validate rat_offset within DAX range */
	if (!marufs_dax_range_valid(sbi, rat_offset,
				    sizeof(struct marufs_rat))) {
		pr_err("rat_offset 0x%llx out of range\n", rat_offset);
		return -EINVAL;
	}

	/* Map RAT from CXL memory */
	rat = (struct marufs_rat *)((char *)sbi->dax_base + rat_offset);
	MARUFS_CXL_RMB(
		rat,
		sizeof(*rat)); /* Memory barrier for cross-node visibility */

	/* Validate RAT magic */
	magic = READ_CXL_LE32(rat->magic);
	if (magic != MARUFS_RAT_MAGIC) {
		pr_err("invalid RAT magic 0x%x (expected 0x%x) at offset 0x%llx\n",
		       magic, MARUFS_RAT_MAGIC, rat_offset);
		return -EINVAL;
	}

	/* Validate RAT version */
	version = READ_CXL_LE32(rat->version);
	if (version != 1) {
		pr_err("unsupported RAT version %u (expected 1)\n", version);
		return -EINVAL;
	}

	/* Store RAT pointer and offsets */
	sbi->rat = rat;
	sbi->rat_offset = rat_offset;
	sbi->regions_start_offset = READ_CXL_LE64(rat->regions_start);

	pr_debug("RAT loaded at offset 0x%llx, RAT pointer=%p\n", rat_offset,
		 rat);
	pr_debug("  device_size=%llu, regions_start=0x%llx\n",
		 READ_CXL_LE64(rat->device_size), sbi->regions_start_offset);
	pr_debug("  num_entries=%u/%u\n", READ_CXL_LE32(rat->num_entries),
		 READ_CXL_LE32(rat->max_entries));
	pr_debug("  total_allocated=%llu, total_free=%llu\n",
		 READ_CXL_LE64(rat->total_allocated),
		 READ_CXL_LE64(rat->total_free));

	return 0;
}

/* ============================================================================
 * Create root directory inode
 * ============================================================================ */

static struct inode *marufs_make_root_inode(struct super_block *sb)
{
	struct inode *inode;
	struct marufs_inode_info *xi;

	inode = new_inode(sb);
	if (!inode)
		return ERR_PTR(-ENOMEM);

	inode->i_ino = MARUFS_ROOT_INO;
	inode->i_mode = S_IFDIR | MARUFS_ROOT_DIR_MODE;
	inode->i_uid = GLOBAL_ROOT_UID;
	inode->i_gid = GLOBAL_ROOT_GID;

	inode_set_atime_to_ts(inode, current_time(inode));
	inode_set_mtime_to_ts(inode, current_time(inode));
	inode_set_ctime_to_ts(inode, current_time(inode));

	inode->i_op = &marufs_dir_inode_ops;
	inode->i_fop = &marufs_dir_ops;

	set_nlink(inode, 2);

	xi = marufs_inode_get(inode);
	xi->region_id = 0;
	xi->entry_idx = 0;
	xi->shard_id = 0;

	return inode;
}

/* ============================================================================
 * Common mount handling - shared by DEV_DAX and DAXHEAP paths
 * ============================================================================ */

static int marufs_fill_super_common(struct super_block *sb,
				    struct marufs_sb_info *sbi, int silent)
{
	struct inode *root_inode;
	struct dentry *root_dentry;
	int ret;

	/* Step 1: Read superblock */
	ret = marufs_read_superblock(sbi, sb, silent);
	if (ret) {
		pr_err("failed to read superblock\n");
		pr_err("mount with 'format' option to initialize: mount -t %s -o daxdev=...,node_id=%d,format none /mnt/...\n",
		       MARUFS_MODULE_NAME, sbi->node_id);
		return ret;
	}

	/* Step 2: Initialize shard table */
	ret = marufs_init_shard_table(sbi);
	if (ret) {
		pr_err("failed to initialize shard table\n");
		goto err_free_gsb;
	}

	/* Step 2.5: Load RAT (Region Allocation Table) */
	ret = marufs_load_rat(sbi);
	if (ret) {
		pr_err("failed to load RAT\n");
		goto err_free_gsb;
	}

	/* Step 3: RAT mode - no fixed region initialization needed */
	/* RAT entries are allocated on-demand during file creation */

	/* Step 4: Configure VFS superblock */
	sb->s_magic = MARUFS_MAGIC;
	sb->s_blocksize = MARUFS_VFS_BLOCK_SIZE;
	sb->s_blocksize_bits = MARUFS_VFS_BLOCK_SIZE_BITS;
	sb->s_maxbytes = MAX_LFS_FILESIZE;
	sb->s_op = &marufs_sops;
	MARUFS_SET_D_OP(sb, &marufs_dentry_ops);
	sb->s_time_gran = 1;

	/* Step 5: Create root inode */
	root_inode = marufs_make_root_inode(sb);
	if (IS_ERR(root_inode)) {
		ret = PTR_ERR(root_inode);
		goto err_free_gsb;
	}

	root_dentry = d_make_root(root_inode);
	if (!root_dentry) {
		ret = -ENOMEM;
		goto err_free_gsb;
	}

	sb->s_root = root_dentry;

	/* Step 6: Initialize cache */
	ret = marufs_cache_init(sbi);
	if (ret)
		pr_warn("failed to initialize entry cache: %d\n", ret);

	/* Step 7: Register sysfs */
	ret = marufs_sysfs_register(sbi);
	if (ret)
		pr_warn("failed to register sysfs: %d\n", ret);

	/* Step 8: Start background GC thread */
	ret = marufs_gc_start(sbi);
	if (ret)
		pr_warn("failed to start gc thread: %d\n", ret);

	pr_info("filesystem mounted (node=%u, shards=%u)\n", sbi->node_id,
		sbi->num_shards);
	return 0;

err_free_gsb:
	/* Free shard_cache and per-shard DRAM arrays allocated before RAT loading */
	marufs_free_shard_resources(sbi);

	return ret;
}

/* ============================================================================
 * Mount handling - unified marufs_fill_super (DEV_DAX and DAXHEAP)
 * ============================================================================ */

int marufs_fill_super(struct super_block *sb, void *data, int silent)
{
	struct marufs_sb_info *sbi;
	struct marufs_mount_opts opts;
	int ret;

	/* Parse mount options */
	ret = marufs_parse_options((char *)data, &opts);
	if (ret)
		return ret;

	/* Allocate marufs_sb_info */
	sbi = kzalloc(sizeof(*sbi), GFP_KERNEL);
	if (!sbi)
		return -ENOMEM;

	sb->s_fs_info = sbi;
	sbi->node_id = opts.node_id;
	spin_lock_init(&sbi->lock);

	/* Validate node_id range */
	if (opts.node_id > MARUFS_MAX_NODE_ID) {
		pr_err("node_id=%d exceeds maximum %d\n", opts.node_id,
		       MARUFS_MAX_NODE_ID);
		kfree(sbi);
		return -EINVAL;
	}

	/* Determine DAX mode from parsed options */
	if (opts.use_daxheap) {
		/* DAXHEAP path: WC mmap via daxheap buffer */
		sbi->dax_mode = MARUFS_DAX_HEAP;
		sbi->total_size =
			opts.daxheap_size; /* 0 for secondary (bufid import) */
		if (opts.daxheap_bufid)
			pr_debug(
				"fill_super DAXHEAP secondary bufid=0x%llx (node_id=%d)\n",
				opts.daxheap_bufid, opts.node_id);
		else
			pr_debug(
				"fill_super DAXHEAP primary size=%llu (%llu MB, node_id=%d)\n",
				opts.daxheap_size, opts.daxheap_size >> 20,
				opts.node_id);
	} else if (opts.daxdev[0]) {
		/* DEV_DAX path */
		sbi->dax_mode = MARUFS_DAX_DEV;
		strscpy(sbi->daxdev_path, opts.daxdev,
			sizeof(sbi->daxdev_path));
		pr_debug("fill_super DEV_DAX %s (node_id=%d)\n", opts.daxdev,
			 opts.node_id);
	} else {
		pr_err("must specify daxdev= or daxheap= mount option\n");
		ret = -EINVAL;
		goto err_free_sbi;
	}

	/* Unified DAX acquisition */
	ret = marufs_dax_acquire(sbi, sb, opts.daxheap_bufid);
	if (ret) {
		if (opts.use_daxheap)
			pr_err("DAXHEAP acquisition failed\n");
		else if (opts.daxdev[0])
			pr_err("DEV_DAX acquisition failed for %s\n",
			       opts.daxdev);
		goto err_free_sbi;
	}

	/* In-kernel format: DAXHEAP always formats (primary), DEV_DAX when format option set */
	if (sbi->dax_mode == MARUFS_DAX_HEAP) {
		if (opts.daxheap_bufid != 0) {
			/* Secondary: buffer must already be formatted by primary */
			struct marufs_superblock *gsb = marufs_gsb_get(sbi);

			if (!gsb || READ_CXL_LE32(gsb->magic) != MARUFS_MAGIC) {
				pr_err("DAXHEAP secondary mount but buffer not formatted\n");
				ret = -EINVAL;
				goto err_release_dax;
			}
		} else {
			/* Primary: always format (daxheap may reuse physical memory) */
			ret = marufs_format_device(sbi);
			if (ret) {
				pr_err("in-kernel format failed\n");
				goto err_release_dax;
			}
		}
	} else if (opts.format) {
		/* DEV_DAX: format when mount option 'format' is specified */
		ret = marufs_format_device(sbi);
		if (ret) {
			pr_err("in-kernel format failed\n");
			goto err_release_dax;
		}
	}

	/* Common handling */
	ret = marufs_fill_super_common(sb, sbi, silent);
	if (ret)
		goto err_release_dax;

	return 0;

err_release_dax:
	marufs_dax_release(sbi, sb);
err_free_sbi:
	marufs_free_shard_resources(sbi);
	kfree(sbi);
	sb->s_fs_info = NULL;
	return ret;
}

/* ============================================================================
 * Mount/unmount callbacks
 * ============================================================================ */

static struct dentry *marufs_mount(struct file_system_type *fs_type, int flags,
				   const char *dev_name, void *data)
{
	/* All modes (DEV_DAX, DAXHEAP) use mount_nodev — no block device */
	return mount_nodev(fs_type, flags, data, marufs_fill_super);
}

static void marufs_kill_sb(struct super_block *sb)
{
	struct marufs_sb_info *sbi = marufs_sb_get(sb);

	if (sbi) {
		/* Stop background GC thread */
		marufs_gc_stop(sbi);

		/* RAT mode: regions persist across mounts, GC handles cleanup */

		/* Unregister sysfs */
		marufs_sysfs_unregister(sbi);

		/* Destroy entry cache */
		marufs_cache_destroy(sbi);

		/*
		 * Release non-DAX buffers. In DAX mode, shard_table and gsb
		 * point into CXL memory -- nothing to free.
		 */

		/* Free per-shard RAM hint, counters, and shard cache */
		marufs_free_shard_resources(sbi);

		/* Close DAX device file (mmap delegation) */
		if (sbi->dax_filp) {
			filp_close(sbi->dax_filp, NULL);
			sbi->dax_filp = NULL;
		}

		/* Release DAX mapping */
		marufs_dax_release(sbi, sb);

		kfree(sbi);
	}

	kill_anon_super(sb);

	pr_info("filesystem unmounted\n");
}

/* ============================================================================
 * Filesystem type registration
 * ============================================================================ */

static struct file_system_type marufs_fs_type = {
	.owner = THIS_MODULE,
	.name = MARUFS_MODULE_NAME,
	.mount = marufs_mount,
	.kill_sb = marufs_kill_sb,
	.fs_flags = 0, /* No FS_REQUIRES_DEV - DEV_DAX support (mount_nodev) */
};

/* ============================================================================
 * Inode cache initialization
 * ============================================================================ */

static void marufs_inode_init_once(void *obj)
{
	struct marufs_inode_info *xi = obj;
	inode_init_once(&xi->vfs_inode);
}

/* ============================================================================
 * Module init/exit
 * ============================================================================ */

static int __init marufs_init(void)
{
	int ret;

	marufs_inode_cachep = kmem_cache_create(
		"marufs_inode_cache", sizeof(struct marufs_inode_info), 0,
		SLAB_RECLAIM_ACCOUNT | MARUFS_SLAB_MEM_SPREAD,
		marufs_inode_init_once);
	if (!marufs_inode_cachep) {
		return -ENOMEM;
	}

	ret = register_filesystem(&marufs_fs_type);
	if (ret) {
		kmem_cache_destroy(marufs_inode_cachep);
		return ret;
	}

	/* Initialize sysfs interface */
	ret = marufs_sysfs_init();
	if (ret) {
		pr_err("failed to initialize sysfs\n");
		unregister_filesystem(&marufs_fs_type);
		kmem_cache_destroy(marufs_inode_cachep);
		return ret;
	}

	pr_info("Partitioned Global Index filesystem loaded (node_id=%d)\n",
		marufs_node_id);

	return 0;
}

static void __exit marufs_exit(void)
{
	pr_info("unloading module\n");

	/* Clean up sysfs interface */
	marufs_sysfs_exit();

	unregister_filesystem(&marufs_fs_type);
	rcu_barrier();
	kmem_cache_destroy(marufs_inode_cachep);

#ifdef CONFIG_DAXHEAP
	/* Safety: clear leaked bufid on module unload */
	mutex_lock(&marufs_daxheap_lock);
	if (marufs_daxheap_bufid != 0) {
		pr_warn("clearing leaked DAXHEAP bufid=0x%llx on module unload\n",
			marufs_daxheap_bufid);
		marufs_daxheap_bufid = 0;
	}
	mutex_unlock(&marufs_daxheap_lock);
#endif

	pr_info("module unloaded\n");
}

module_init(marufs_init);
module_exit(marufs_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("XCMP Team");
MODULE_DESCRIPTION(
	"MARUFS - Partitioned Global Index filesystem for CXL shared memory");
MODULE_VERSION("1.0");
MODULE_SOFTDEP("pre: daxheap");
#ifdef CONFIG_DAXHEAP
MODULE_IMPORT_NS("DMA_BUF");
#endif
