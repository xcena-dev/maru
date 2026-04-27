/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * marufs_endian.h - Endian + CXL barrier + CAS primitives.
 *
 * CXL shared memory needs both endian conversion (on-disk = little-endian)
 * and compiler/cache barriers for cross-host visibility. These macros are
 * used by every layer that touches CXL fields, so they live in their own
 * header to keep includes precise.
 */

#ifndef _MARUFS_ENDIAN_H
#define _MARUFS_ENDIAN_H

#include <linux/types.h>

/* Read little-endian field with compiler barrier (DRAM / general use) */
#define READ_LE32(field) le32_to_cpu(READ_ONCE(field))
#define READ_LE64(field) le64_to_cpu(READ_ONCE(field))
#define READ_LE16(field) le16_to_cpu(READ_ONCE(field))

/* Read little-endian field from CXL memory (caller must issue RMB first) */
#define READ_CXL_LE32(field) le32_to_cpu(field)
#define READ_CXL_LE64(field) le64_to_cpu(field)
#define READ_CXL_LE16(field) le16_to_cpu(field)

/* Write little-endian field with compiler barrier */
#define WRITE_LE32(field, val) WRITE_ONCE(field, cpu_to_le32(val))
#define WRITE_LE64(field, val) WRITE_ONCE(field, cpu_to_le64(val))
#define WRITE_LE16(field, val) WRITE_ONCE(field, cpu_to_le16(val))

/*
 * CXL multi-node memory barriers
 *
 * CXL 3.0: hardware cache coherence guaranteed across hosts.
 *          Standard wmb()/rmb() suffice for cross-node visibility.
 * CXL 2.0: no cross-host coherence in spec.
 *          Explicit clwb/clflushopt required to flush/invalidate so
 *          writes reach CXL memory and reads fetch fresh data.
 *
 * MARUFS_CXL_WMB(addr, len) - Call after writing CXL fields.
 * MARUFS_CXL_RMB(addr, len) - Call before reading CXL fields.
 *
 * Enable CXL 2.0 mode via -DCONFIG_MARUFS_CXL2_COMPAT.
 */
#ifdef CONFIG_MARUFS_CXL2_COMPAT

#include <linux/libnvdimm.h>

#define MARUFS_CXL_WMB(addr, len)                          \
	do {                                               \
		wmb();                                     \
		arch_wb_cache_pmem((void *)(addr), (len)); \
	} while (0)

#define MARUFS_CXL_RMB(addr, len)                            \
	do {                                                 \
		arch_invalidate_pmem((void *)(addr), (len)); \
		rmb();                                       \
	} while (0)

#else /* CXL 3.0: hardware coherence guaranteed across hosts */

#define MARUFS_CXL_WMB(addr, len) wmb()
#define MARUFS_CXL_RMB(addr, len) rmb()

#endif /* CONFIG_MARUFS_CXL2_COMPAT */

/*
 * marufs_le{16,32,64}_cas - Atomic compare-and-swap on little-endian CXL
 * fields. Returns old value in CPU byte order. On success, flushes write
 * via WMB for cross-node visibility (CXL 2.0). On CXL 2.0 also performs
 * post-CAS verification: invalidate cacheline + re-read. If a peer
 * overwrote between our cmpxchg and flush, report mismatch as CAS
 * failure with the actual current value. CXL 3.0 skips verification.
 */
static inline u16 marufs_le16_cas(__le16 *ptr, u16 old, u16 new)
{
	u16 exp = cpu_to_le16(old);
	u16 ret = cmpxchg((u16 *)ptr, exp, cpu_to_le16(new));

	if (ret == exp) {
		MARUFS_CXL_WMB(ptr, sizeof(*ptr));
#ifdef CONFIG_MARUFS_CXL2_COMPAT
		MARUFS_CXL_RMB(ptr, sizeof(*ptr));
		if (unlikely(READ_CXL_LE16(*ptr) != new))
			return READ_CXL_LE16(*ptr);
#endif
	}
	return le16_to_cpu(ret);
}

static inline u32 marufs_le32_cas(__le32 *ptr, u32 old, u32 new)
{
	u32 exp = cpu_to_le32(old);
	u32 ret = cmpxchg((u32 *)ptr, exp, cpu_to_le32(new));

	if (ret == exp) {
		MARUFS_CXL_WMB(ptr, sizeof(*ptr));
#ifdef CONFIG_MARUFS_CXL2_COMPAT
		MARUFS_CXL_RMB(ptr, sizeof(*ptr));
		if (unlikely(READ_CXL_LE32(*ptr) != new))
			return READ_CXL_LE32(*ptr);
#endif
	}
	return le32_to_cpu(ret);
}

static inline u64 marufs_le64_cas(__le64 *ptr, u64 old, u64 new)
{
	u64 exp = cpu_to_le64(old);
	u64 ret = cmpxchg((u64 *)ptr, exp, cpu_to_le64(new));

	if (ret == exp) {
		MARUFS_CXL_WMB(ptr, sizeof(*ptr));
#ifdef CONFIG_MARUFS_CXL2_COMPAT
		MARUFS_CXL_RMB(ptr, sizeof(*ptr));
		if (unlikely(READ_CXL_LE64(*ptr) != new))
			return READ_CXL_LE64(*ptr);
#endif
	}
	return le64_to_cpu(ret);
}

/*
 * marufs_le{16,32}_cas_inc/dec - CAS-based atomic increment/decrement.
 * Safe for concurrent multi-node access. Underflow/overflow guarded.
 */
static inline void marufs_le16_cas_inc(__le16 *p)
{
	u16 old_val;
	do {
		old_val = READ_CXL_LE16(*p);
		if (old_val == U16_MAX)
			return; /* overflow guard */
	} while (marufs_le16_cas(p, old_val, old_val + 1) != old_val);
}

static inline void marufs_le16_cas_dec(__le16 *p)
{
	u16 old_val;
	do {
		old_val = READ_CXL_LE16(*p);
		if (old_val == 0)
			return; /* underflow guard */
	} while (marufs_le16_cas(p, old_val, old_val - 1) != old_val);
}

static inline void marufs_le32_cas_inc(__le32 *p)
{
	u32 old_val;
	do {
		old_val = READ_CXL_LE32(*p);
		if (old_val == U32_MAX)
			return; /* overflow guard */
	} while (marufs_le32_cas(p, old_val, old_val + 1) != old_val);
}

static inline void marufs_le32_cas_dec(__le32 *p)
{
	u32 old_val;
	do {
		old_val = READ_CXL_LE32(*p);
		if (old_val == 0)
			return; /* underflow guard */
	} while (marufs_le32_cas(p, old_val, old_val - 1) != old_val);
}

#endif /* _MARUFS_ENDIAN_H */
