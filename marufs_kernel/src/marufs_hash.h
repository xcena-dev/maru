/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * marufs_hash.h - Hash + index helpers (orthogonal to on-disk layout).
 *
 * Filename hashing, shard/bucket selection, VFS ino synthesis, and
 * power-of-2 alignment. Stand-alone — no dependencies on layout structs.
 */

#ifndef _MARUFS_HASH_H
#define _MARUFS_HASH_H

#include <linux/types.h>
#include <linux/unaligned.h>
#include <crypto/sha2.h>

/*
 * marufs_shard_idx - select shard from 64-bit name hash.
 * Uses upper 16 bits (bits 63..48) masked with shard_mask.
 * @name_hash: 64-bit hash of filename
 * @shard_mask: num_shards - 1 (num_shards must be power of 2)
 */
static inline u32 marufs_shard_idx(u64 name_hash, u32 shard_mask)
{
	return (u32)((name_hash >> 48) & shard_mask);
}

/*
 * marufs_bucket_idx - select bucket within shard.
 * Uses bits 47..32 of hash masked with bucket_mask.
 * @name_hash: 64-bit hash of filename
 * @bucket_mask: num_buckets - 1 (num_buckets must be power of 2)
 */
static inline u32 marufs_bucket_idx(u64 name_hash, u32 bucket_mask)
{
	return (u32)((name_hash >> 32) & bucket_mask);
}

/*
 * marufs_make_ino - synthesize VFS inode number from region_id.
 * +2 to skip ino 0 (null/invalid) and ino 1 (root directory).
 */
static inline unsigned long marufs_make_ino(u32 region_id)
{
	return (unsigned long)region_id + 2;
}

/* Extract region_id from VFS inode number. */
static inline u32 marufs_ino_to_region(unsigned long ino)
{
	return (u32)(ino - 2);
}

/* Align @val up to next @align boundary. @align must be power of 2. */
static inline u64 marufs_align_up(u64 val, u64 align)
{
	return (val + align - 1) & ~(align - 1);
}

/*
 * marufs_hash_name - SHA-256 truncated hash for filename.
 * Upper bits used for shard selection, middle bits for bucket index.
 */
static inline u64 marufs_hash_name(const char *name, size_t len)
{
	u8 digest[SHA256_DIGEST_SIZE];
	sha256((const u8 *)name, len, digest);
	return get_unaligned_le64(digest);
}

#endif /* _MARUFS_HASH_H */
