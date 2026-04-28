// SPDX-License-Identifier: GPL-2.0-only
/*
 * bootstrap.c - MARUFS auto-mount bootstrap slot management.
 *
 * Bootstrap is ONLY for mount-time election + format gate.
 * Runtime liveness is owned entirely by ME (not bootstrap).
 *
 * Algorithm (last-writer-wins, settle window):
 *   1. Write magic + random_token + status=CLAIMED to a target slot.
 *   2. Sleep bootstrap_settle_ms so peers can complete writes.
 *   3. Reread: if our random_token is still present, we won.
 *
 * No CAS needed: a 64B slot is one CL; cross-host CXL writes resolve at
 * last-store-to-CL granularity.  The random_token uniquely identifies the
 * winner after the settle window.
 */

#include <linux/delay.h>
#include <linux/module.h>
#include <linux/random.h>
#include <linux/slab.h>

#include "marufs.h"
#include "bootstrap.h"
#include "me.h"

enum marufs_bootstrap_configuration {
	/* Timing constants (milliseconds) */
	MARUFS_BOOTSTRAP_SETTLE_MS = 20,
	MARUFS_BOOTSTRAP_POLL_MS = 50,
	MARUFS_BOOTSTRAP_FORMAT_TIMEOUT_MS = 30000
};

/* Module parameter: settle window in ms (tunable for high-latency fabrics) */
static unsigned int bootstrap_settle_ms = MARUFS_BOOTSTRAP_SETTLE_MS;
module_param(bootstrap_settle_ms, uint, 0600);
MODULE_PARM_DESC(bootstrap_settle_ms,
		 "Bootstrap slot claim settle window in ms (default 20)");

/* Module parameter: stuck formatter detection timeout in ms */
static unsigned int bootstrap_format_timeout_ms =
	MARUFS_BOOTSTRAP_FORMAT_TIMEOUT_MS;
module_param(bootstrap_format_timeout_ms, uint, 0600);
MODULE_PARM_DESC(bootstrap_format_timeout_ms,
		 "Stuck formatter detection timeout in ms (default 30000)");

/*
 * Module parameter: chaos fault injection — if non-zero, the auto-mount
 * formatter skips GSB-magic write and slot[0] status→CLAIMED promotion,
 * leaving slot[0] stuck at FORMATTING forever.  Joiners will detect this
 * via bootstrap_format_timeout_ms and exercise the steal path.
 * Set BEFORE mounting the formatter node.  Default 0 (disabled).
 */
static unsigned int bootstrap_inject_stuck_formatter;
module_param(bootstrap_inject_stuck_formatter, uint, 0600);
MODULE_PARM_DESC(
	bootstrap_inject_stuck_formatter,
	"DEBUG: if non-zero, formatter skips GSB-magic write (chaos test). Default 0.");

/*
 * Module parameter: chaos race-window expander.  If non-zero, after a free
 * slot is identified but before the status=CLAIMED write, sleep this many
 * microseconds.  Widening this window lets concurrent mounters observe the
 * same free slot and both write their token, exercising the last-writer-wins
 * resolution.  Default 0 (no artificial delay).
 */
static unsigned int bootstrap_debug_pre_write_delay_us;
module_param(bootstrap_debug_pre_write_delay_us, uint, 0600);
MODULE_PARM_DESC(
	bootstrap_debug_pre_write_delay_us,
	"DEBUG: delay (us) between free-slot scan and CLAIMED write to widen race window. Default 0.");

/*
 * marufs_bootstrap_should_inject_stuck - readable from mount path before
 * sbi is fully constructed (unlike the old per-sbi sysfs flag).
 */
bool marufs_bootstrap_should_inject_stuck(void)
{
	return READ_LE32(bootstrap_inject_stuck_formatter) != 0;
}

/* ── Internal helpers ─────────────────────────────────────────────────── */

static inline struct marufs_bootstrap_slot *__slot(struct marufs_sb_info *sbi,
						   int idx)
{
	return marufs_bootstrap_slot_get(sbi, idx);
}

/* Generate a non-zero random token (claim race tiebreaker). */
static u64 __gen_nonzero_token(void)
{
	u64 t;
	do {
		t = get_random_u64();
	} while (t == 0);
	return t;
}

/*
 * Write claim fields to a slot, settle, then re-read to verify ownership.
 * Returns 0 on win (token survived), -EAGAIN on lost race.
 *
 * @slot:           target bootstrap slot (in CXL memory)
 * @status:         status to write (CLAIMED for normal, FORMATTING for steal)
 * @token:          our random_token (caller already generated)
 * @observed_token: optional out-param for the token observed after settle
 *                  (used by claim() for the "lost race" log line). May be NULL.
 */
static int __claim_write_and_verify(struct marufs_bootstrap_slot *slot,
				    u32 status, u64 token, u64 *observed_token)
{
	WRITE_LE32(slot->magic, MARUFS_BOOTSTRAP_MAGIC);
	WRITE_LE64(slot->random_token, token);
	WRITE_LE32(slot->status, status);
	MARUFS_CXL_WMB(slot, sizeof(*slot));

	msleep(bootstrap_settle_ms);

	MARUFS_CXL_RMB(slot, sizeof(*slot));
	u64 obs = READ_CXL_LE64(slot->random_token);
	if (observed_token)
		*observed_token = obs;
	return (obs == token) ? 0 : -EAGAIN;
}

/*
 * me_node_is_dead - check whether node @node_id has stopped ticking.
 *
 * Cross-host safe: uses observer-local counter snap (mirrors
 * me_handle_acquire_deadline in me.c).  Sample heartbeat counter, sleep
 * MARUFS_ME_LIVENESS_PROBE_NS on local clock, resample.  No counter advance
 * ⇒ owner not ticking ⇒ dead.
 *
 * Cross-host monotonic timestamps don't share an origin, so subtracting
 * heartbeat_ts is meaningless — only counter advance is portable.
 *
 * Returns false if ME not started, node_id invalid, or counter advanced.
 */
static bool me_node_is_dead(struct marufs_sb_info *sbi, u32 node_id)
{
	struct marufs_me_instance *me = sbi->me;
	if (!me)
		return false; /* ME not started — cannot conclude dead */
	if (!marufs_me_is_valid_node(me, node_id))
		return false;

	/* membership[] is indexed by internal idx = node_id - 1 */
	struct marufs_me_membership_slot *ms = &me->membership[node_id - 1];
	MARUFS_CXL_RMB(ms, sizeof(*ms));
	u64 hb_before = READ_CXL_LE64(ms->heartbeat);

	/* Observer-local probe interval — sleep on our clock, not theirs. */
	u64 probe_us = MARUFS_ME_LIVENESS_PROBE_NS / NSEC_PER_USEC;
	usleep_range(probe_us, probe_us + probe_us / 4);

	MARUFS_CXL_RMB(ms, sizeof(*ms));
	u64 hb_after = READ_CXL_LE64(ms->heartbeat);

	/* No advance ⇒ owner stopped ticking ⇒ dead */
	return (hb_before == hb_after);
}

/* ── marufs_bootstrap_init_area ────────────────────────────────────────── */

void marufs_bootstrap_init_area(void *base)
{
	BUILD_BUG_ON(sizeof(struct marufs_bootstrap_slot) !=
		     MARUFS_BOOTSTRAP_SLOT_SIZE);
	/*
	 * No-op documentation call. Bootstrap area is zeroed by format or by
	 * the kernel's zero-page guarantee for fresh CXL devices.
	 * EMPTY = 0, magic = 0 ≠ MARUFS_BOOTSTRAP_MAGIC — claim scan correctly
	 * treats all slots as available on a fresh device.
	 */
	(void)base;
}

/* ── marufs_bootstrap_claim ─────────────────────────────────────────────── */

int marufs_bootstrap_claim(struct marufs_sb_info *sbi, int *out_slot_idx)
{
	struct marufs_bootstrap_slot *slots = marufs_bootstrap_slot_get(sbi, 0);
	if (!slots)
		return -EINVAL;

	/*
	 * Single pass: pick the first reusable slot.
	 *
	 * A slot is reusable when:
	 *   (a) magic != MARUFS_BOOTSTRAP_MAGIC  (fresh/zeroed device), OR
	 *   (b) status == EMPTY                  (graceful umount), OR
	 *   (c) status == CLAIMED AND me_node_is_dead(idx+1) (owner crashed).
	 *
	 * FORMATTING is never reusable here — stuck-formatter steal handles
	 * stale FORMATTING on slot[0] via marufs_bootstrap_steal_stuck_slot0().
	 */
	int free_idx = -1;
	for (int i = 0; i < MARUFS_BOOTSTRAP_MAX_SLOTS; i++) {
		struct marufs_bootstrap_slot *s = &slots[i];
		MARUFS_CXL_RMB(s, sizeof(*s));

		/* (a) uninitialised slot on a fresh device */
		u32 mg = READ_CXL_LE32(s->magic);
		if (mg != MARUFS_BOOTSTRAP_MAGIC) {
			free_idx = i;
			break;
		}
		/* (b) graceful-umount EMPTY */
		u32 st = READ_CXL_LE32(s->status);
		if (st == MARUFS_BS_EMPTY) {
			free_idx = i;
			break;
		}
		/* (c) CLAIMED but ME says node is dead */
		if (st == MARUFS_BS_CLAIMED && me_node_is_dead(sbi, i + 1)) {
			free_idx = i;
			break;
		}
		/* FORMATTING — skip (claim in progress, steal path handles it) */
	}

	if (free_idx < 0)
		return -EBUSY; /* table full */

	u64 token = __gen_nonzero_token();

	/* Chaos hook: artificially widen the race window between free-slot
	 * detection and the CLAIMED write so concurrent mounters observe the
	 * same free slot and both write their token.  Disabled in production. */
	if (bootstrap_debug_pre_write_delay_us)
		usleep_range(bootstrap_debug_pre_write_delay_us,
			     bootstrap_debug_pre_write_delay_us + 100);

	/* Write claim: magic → token → status=CLAIMED → settle → verify */
	struct marufs_bootstrap_slot *tgt = &slots[free_idx];
	u64 observed;
	int ret = __claim_write_and_verify(tgt, MARUFS_BS_CLAIMED, token,
					   &observed);
	if (ret) {
		pr_info("bootstrap: claim race lost on slot %d (my_token=0x%016llx observed=0x%016llx)\n",
			free_idx, token, observed);
		return -EAGAIN; /* lost race; caller retries */
	}

	sbi->bootstrap_slot_idx = free_idx;
	sbi->bootstrap_token = token;
	*out_slot_idx = free_idx;
	return 0;
}

/* ── marufs_bootstrap_claim_explicit ───────────────────────────────────── */

int marufs_bootstrap_claim_explicit(struct marufs_sb_info *sbi, u32 node_id)
{
	if (node_id == 0 || node_id > MARUFS_BOOTSTRAP_MAX_SLOTS)
		return -EINVAL;

	int idx = (int)(node_id - 1);
	struct marufs_bootstrap_slot *tgt = __slot(sbi, idx);
	MARUFS_CXL_RMB(tgt, sizeof(*tgt));
	u32 mg = READ_CXL_LE32(tgt->magic);
	u32 st = READ_CXL_LE32(tgt->status);

	/* Slot must be uninitialized, EMPTY, or stale-CLAIMED */
	if (mg == MARUFS_BOOTSTRAP_MAGIC && st != MARUFS_BS_EMPTY) {
		if (st == MARUFS_BS_CLAIMED) {
			if (!me_node_is_dead(sbi, node_id)) {
				pr_err("bootstrap: slot %d (node_id %u) CLAIMED and live\n",
				       idx, node_id);
				return -EBUSY;
			}
			pr_info("bootstrap: slot %d (node_id %u) stale CLAIMED, stealing\n",
				idx, node_id);
		} else {
			pr_err("bootstrap: slot %d (node_id %u) not claimable (status=%u)\n",
			       idx, node_id, st);
			return -EBUSY;
		}
	}

	u64 token = __gen_nonzero_token();
	if (__claim_write_and_verify(tgt, MARUFS_BS_CLAIMED, token, NULL))
		return -EAGAIN;

	sbi->bootstrap_slot_idx = idx;
	sbi->bootstrap_token = token;
	return 0;
}

/* ── marufs_bootstrap_release ───────────────────────────────────────────── */

void marufs_bootstrap_release(struct marufs_sb_info *sbi)
{
	if (sbi->bootstrap_slot_idx < 0)
		return;

	/*
	 * Graceful umount: write EMPTY directly so the slot is immediately
	 * reusable by the next mounter. No kthread to stop — ME owns liveness.
	 */
	struct marufs_bootstrap_slot *tgt =
		__slot(sbi, sbi->bootstrap_slot_idx);
	WRITE_LE32(tgt->status, MARUFS_BS_EMPTY);
	MARUFS_CXL_WMB(tgt, sizeof(*tgt));
	pr_info("bootstrap: node %u slot %d released (EMPTY)\n", sbi->node_id,
		sbi->bootstrap_slot_idx);
	sbi->bootstrap_slot_idx = -1;
}

/* ── marufs_bootstrap_wait_for_format ──────────────────────────────────── */

int marufs_bootstrap_wait_for_format(struct marufs_sb_info *sbi)
{
	struct marufs_superblock *gsb = marufs_gsb_get(sbi);
	struct marufs_bootstrap_slot *slot0 = __slot(sbi, 0);
	if (!gsb || !slot0)
		return -EINVAL;

	pr_info("bootstrap: node %u (slot %d) waiting for formatter\n",
		sbi->node_id, sbi->bootstrap_slot_idx);

	u64 local_start = ktime_get_ns();
	u64 t_max_ns = (u64)bootstrap_format_timeout_ms * NSEC_PER_MSEC;

	while (1) {
		u32 slot0_st;

		/* Check if format complete */
		MARUFS_CXL_RMB(gsb, sizeof(*gsb));
		if (READ_CXL_LE32(gsb->magic) == MARUFS_MAGIC) {
			pr_info("bootstrap: format detected, node %u proceeding\n",
				sbi->node_id);
			return 0;
		}

		/* Check stuck formatter: slot[0] stuck at FORMATTING past timeout */
		MARUFS_CXL_RMB(slot0, sizeof(*slot0));
		slot0_st = READ_CXL_LE32(slot0->status);
		if (slot0_st == MARUFS_BS_FORMATTING) {
			u64 elapsed_ns = ktime_get_ns() - local_start;
			if (elapsed_ns > t_max_ns) {
				pr_warn("bootstrap: formatter stuck after %llu ms, entering recovery\n",
					elapsed_ns / NSEC_PER_MSEC);
				return -EAGAIN;
			}
		}

		msleep_interruptible(MARUFS_BOOTSTRAP_POLL_MS);
	}
}

/* ── marufs_bootstrap_set_status ────────────────────────────────────────── */

void marufs_bootstrap_set_status(struct marufs_sb_info *sbi, int slot_idx,
				 enum marufs_bootstrap_status status)
{
	struct marufs_bootstrap_slot *tgt =
		marufs_bootstrap_slot_get(sbi, slot_idx);
	if (!tgt)
		return;

	WRITE_LE32(tgt->status, status);
	MARUFS_CXL_WMB(tgt, sizeof(*tgt));
}

/* ── __bootstrap_zero_format_output ─────────────────────────────────────── */

/*
 * Zero the format output area (GSB + shard table), preserving the bootstrap
 * area.  Used before a re-format so stale layout data does not confuse the
 * new formatter.
 */
static void __bootstrap_zero_format_output(struct marufs_sb_info *sbi)
{
	u64 regions_start = MARUFS_REGION_OFFSET;

	void *base = sbi->dax_base;
	if (sbi->dax_mode == MARUFS_DAX_HEAP) {
		memset_io(base, 0, MARUFS_GSB_SIZE);
		memset_io(marufs_dax_ptr(sbi, MARUFS_SHARD_TABLE_OFFSET), 0,
			  regions_start - MARUFS_SHARD_TABLE_OFFSET);
	} else {
		memset(base, 0, MARUFS_GSB_SIZE);
		memset(marufs_dax_ptr(sbi, MARUFS_SHARD_TABLE_OFFSET), 0,
		       regions_start - MARUFS_SHARD_TABLE_OFFSET);
	}
	MARUFS_CXL_WMB(base, regions_start);
}

/* ── marufs_bootstrap_steal_stuck_slot0 ────────────────────────────────── */

int marufs_bootstrap_steal_stuck_slot0(struct marufs_sb_info *sbi)
{
	struct marufs_bootstrap_slot *slot0 = __slot(sbi, 0);
	u64 token = __gen_nonzero_token();

	if (__claim_write_and_verify(slot0, MARUFS_BS_FORMATTING, token,
				     NULL)) {
		pr_info("bootstrap: lost slot[0] steal race, retry\n");
		return -EAGAIN;
	}

	pr_info("bootstrap: node %u stole slot[0] for recovery\n",
		sbi->node_id);

	/* Zero the format output area, preserving bootstrap area.
	 * Strict re-format: safer than detecting partial-format state.
	 */
	__bootstrap_zero_format_output(sbi);

	sbi->bootstrap_slot_idx = 0;
	sbi->bootstrap_token = token;
	return 0;
}

/* ── Sysfs dump helper ──────────────────────────────────────────────────── */

static const char *const bs_status_names[] = {
	[MARUFS_BS_EMPTY] = "EMPTY",
	[MARUFS_BS_CLAIMED] = "CLAIMED",
	[MARUFS_BS_FORMATTING] = "FORMATTING",
};

ssize_t marufs_bootstrap_dump_slots(struct marufs_sb_info *sbi, char *buf)
{
	struct marufs_bootstrap_slot *slots = marufs_bootstrap_slot_get(sbi, 0);
	if (!slots)
		return scnprintf(buf, PAGE_SIZE,
				 "(bootstrap not initialized)\n");

	ssize_t n = 0;
	for (int i = 0; i < MARUFS_BOOTSTRAP_MAX_SLOTS; i++) {
		struct marufs_bootstrap_slot *s = &slots[i];
		MARUFS_CXL_RMB(s, sizeof(*s));

		u32 mg = READ_CXL_LE32(s->magic);
		u32 st = READ_CXL_LE32(s->status);
		u64 tok = READ_CXL_LE64(s->random_token);

		const char *stname = (st < ARRAY_SIZE(bs_status_names) &&
				      bs_status_names[st]) ?
					     bs_status_names[st] :
					     "?";

		n += scnprintf(buf + n, PAGE_SIZE - n,
			       "slot[%d] node_id=%d magic=0x%08x status=%s "
			       "token=0x%016llx%s\n",
			       i, i + 1, mg, stname, tok,
			       (i == sbi->bootstrap_slot_idx) ? " <mine>" : "");
	}
	return n;
}
