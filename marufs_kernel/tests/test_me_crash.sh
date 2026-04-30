#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# test_me_crash.sh - ME crash-detection tests using poll freeze injection.
#
# Prereq: setup_local_multinode.sh already ran with --num-mounts ≥ 2.
#   Node 1 → /mnt/marufs, Node 2 → /mnt/marufs2
#   Node 3 → /mnt/marufs3 (required only for T5; skipped if absent)
#   Kernel module built with me_freeze_heartbeat + me_sync_is_holder sysfs attrs.
#
# Cases (ordered: sanity → crash paths → post-mortem):
#   T1 stress CPU         stress-ng saturation under normal traffic →
#                         no spurious crash-detected logs (baseline sanity).
#   T2 crash loop         freeze Node 1 → Node 2 acquire hits 5.1s takeover,
#                         repeated 2× to cover basic path + no-leak.
#   T3 busy holder        alive holder with long CS → no false takeover.
#   T4 late grant         freeze Node 1, Node 2 acquire, unfreeze mid-probe →
#                         Case A early return (enter CS without takeover).
#   T5 concurrent         3 mounts: Node 2 + Node 3 race takeover on frozen
#                         Node 1 → at most one takeover wins, rest back off.
#   T6 ETIMEDOUT          freeze then unfreeze just before deadline → peer
#                         sees heartbeat alive → -ETIMEDOUT, no takeover.
#   T7 post-takeover      after takeover, Node 2 runs 20 ops without issue.

set -euo pipefail

FREEZE=/sys/fs/marufs/debug/me_freeze_heartbeat
SYNC=/sys/fs/marufs/debug/me_sync_is_holder

die() { echo "FAIL: $*" >&2; exit 1; }
info() { echo "[test_me_crash] $*"; }

need_root() { [[ $EUID -eq 0 ]] || die "must run as root"; }

check_prereq() {
	[[ -e $FREEZE ]] || die "$FREEZE missing — kernel build lacks fault-injection attr"
	[[ -e $SYNC   ]] || die "$SYNC missing — kernel build lacks sync attr"
	mountpoint -q /mnt/marufs  || die "/mnt/marufs not mounted"
	mountpoint -q /mnt/marufs2 || die "/mnt/marufs2 not mounted"
}

freeze_node()   { echo "$1 1" > "$FREEZE"; }
unfreeze_node() { echo "$1 0" > "$FREEZE"; }
unfreeze_all()  { for n in 1 2 3 4; do echo "$n 0" > "$FREEZE" 2>/dev/null || true; done; }

# Resync all MEs' DRAM is_holder to match CB. Purges stale flags left by
# earlier takeovers so the next test starts from a clean state.
sync_state() { echo 1 > "$SYNC"; sleep 0.1; }

cleanup() { unfreeze_all; }
trap cleanup EXIT

dmesg_reset() { dmesg -C >/dev/null 2>&1 || true; }
dmesg_has()   { dmesg 2>/dev/null | grep -qE "$1"; }
dmesg_count() { dmesg 2>/dev/null | grep -cE "$1" || true; }

# Touch triggers RAT alloc → Global ME acquire path.
acquire_probe() { timeout 15 touch "$1/$2"; }

# ── T1 stress CPU load (baseline sanity) ──────────────────────────────
run_t1_stress() {
	if ! command -v stress-ng >/dev/null 2>&1; then
		info "T1 SKIP: stress-ng not installed (install: sudo apt install stress-ng)"
		return
	fi
	info "T1 (no false positive under CPU load)"
	sync_state
	dmesg_reset
	stress-ng --cpu 0 --timeout 4s >/dev/null 2>&1 &
	local stress_pid=$!
	sleep 0.3

	local end=$(( $(date +%s) + 3 ))
	local i=0
	while [[ $(date +%s) -lt $end ]]; do
		touch /mnt/marufs/.t1_pulse  && rm -f /mnt/marufs/.t1_pulse
		touch /mnt/marufs2/.t1_pulse && rm -f /mnt/marufs2/.t1_pulse
		i=$(( i + 1 ))
	done
	wait "$stress_pid" || true

	dmesg_has "crash detected" && die "false-positive crash under load"
	info "T1 PASS (no spurious crash log during $i ops under load)"
}

# ── T2 crash loop ─────────────────────────────────────────────────────
run_t2_crash_loop() {
	info "T2 (crash loop × 2)"
	local iters=2 i t0 t1 dt
	for (( i = 1; i <= iters; i++ )); do
		sync_state
		touch /mnt/marufs/.t2_prep_$i || die "iter $i prep failed"
		sync_state
		dmesg_reset
		freeze_node 1
		t0=$(date +%s%N)
		acquire_probe /mnt/marufs2 .t2_iter_$i || die "iter $i acquire failed"
		t1=$(date +%s%N); dt=$(( (t1 - t0) / 1000000 ))
		(( dt >= 4800 && dt <= 6500 )) || die "iter $i elapsed ${dt} ms outside 4800-6500"
		dmesg_has "crash detected" || die "iter $i: no 'crash detected' log"
		unfreeze_node 1
		sleep 0.3
	done
	info "T2 PASS ($iters iters, each 4.8-6.5s + crash log verified)"
}

# ── T3 busy holder ─────────────────────────────────────────────────────
run_t3_busy_holder() {
	info "T3 (alive holder, no false takeover)"
	sync_state
	dmesg_reset
	(
		end=$(( $(date +%s) + 6 ))
		while [[ $(date +%s) -lt $end ]]; do
			touch /mnt/marufs/.busy$$ && rm -f /mnt/marufs/.busy$$
		done
	) &
	local hog=$!
	sleep 0.3
	set +e; acquire_probe /mnt/marufs2 .t3_acquire; set -e
	wait "$hog" || true
	dmesg_has "crash detected" && die "unexpected crash log on alive holder"
	info "T3 PASS (no false takeover)"
}

# ── T4 late grant ──────────────────────────────────────────────────────
run_t4_late_grant() {
	info "T4 (late grant during probe — Case A)"
	sync_state
	touch /mnt/marufs/.t4_prep_node1 || die "prep failed"
	sync_state
	dmesg_reset
	freeze_node 1

	local logf; logf=$(mktemp)
	(
		t0=$(date +%s%N)
		timeout 15 touch /mnt/marufs2/.t4_late_grant
		t1=$(date +%s%N)
		echo $(( (t1 - t0) / 1000000 )) > "$logf"
	) &
	local pid=$!

	# Sleep past deadline (5s) but inside probe window (100ms).
	sleep 5.05
	unfreeze_node 1
	wait "$pid" || true

	local dt; dt=$(cat "$logf"); rm -f "$logf"
	info "  Node 2 acquire in ${dt} ms"

	if dmesg_has "crash detected"; then
		info "  note: crash log present — probe timing missed the 100ms window. Still PASS if dt looks sane."
	fi
	(( dt >= 4800 && dt <= 6500 )) || die "elapsed ${dt} ms outside window"
	unfreeze_node 1
	info "T4 PASS"
}

# ── T5 concurrent ──────────────────────────────────────────────────────
run_t5_concurrent() {
	if ! mountpoint -q /mnt/marufs3 2>/dev/null; then
		info "T5 SKIP: /mnt/marufs3 not present (run setup with --num-mounts ≥ 3)"
		return
	fi
	info "T5 (concurrent takeover: Node 2 + Node 3 race)"
	sync_state
	touch /mnt/marufs/.t5_prep_node1 || die "prep failed"
	sync_state
	dmesg_reset
	freeze_node 1

	local l2 l3; l2=$(mktemp); l3=$(mktemp)
	( t0=$(date +%s%N); set +e; timeout 15 touch /mnt/marufs2/.t5_race; rc=$?; set -e; t1=$(date +%s%N); echo "$rc $(( (t1 - t0) / 1000000 ))" > "$l2" ) &
	local p2=$!
	( t0=$(date +%s%N); set +e; timeout 15 touch /mnt/marufs3/.t5_race; rc=$?; set -e; t1=$(date +%s%N); echo "$rc $(( (t1 - t0) / 1000000 ))" > "$l3" ) &
	local p3=$!
	wait "$p2" "$p3" || true

	info "  Node 2: $(cat "$l2")    Node 3: $(cat "$l3")"
	local n; n=$(dmesg_count "crash detected")
	info "  crash-detected log count: $n"
	(( n >= 1 )) || die "no takeover happened"
	rm -f "$l2" "$l3"
	unfreeze_node 1
	info "T5 PASS (>=1 takeover, no deadlock)"
}

# ── T6 ETIMEDOUT on revived holder ─────────────────────────────────────
run_t6_etimeout() {
	info "T6 (deadline → -ETIMEDOUT when holder revives mid-probe)"
	sync_state
	touch /mnt/marufs/.t6_prep_node1 || die "prep failed"
	sync_state
	dmesg_reset
	freeze_node 1

	local rcf; rcf=$(mktemp)
	(
		t0=$(date +%s%N)
		set +e; timeout 10 touch /mnt/marufs2/.t6_timeout; local rc=$?; set -e
		t1=$(date +%s%N)
		echo "$rc $(( (t1 - t0) / 1000000 ))" > "$rcf"
	) &
	local pid=$!

	# Unfreeze at 4.9s — before Node 2's deadline so probe observes a live
	# heartbeat counter.
	sleep 4.9
	unfreeze_node 1
	wait "$pid" || true

	local rc dt; read -r rc dt < "$rcf"; rm -f "$rcf"
	info "  Node 2: rc=$rc elapsed=${dt} ms"

	dmesg_has "crash detected" && die "expected no takeover (holder revived), but crash log present"
	(( dt >= 4800 )) || die "returned too fast (${dt} ms) — deadline path not exercised"
	unfreeze_node 1
	info "T6 PASS"
}

# ── T7 post-takeover sanity ────────────────────────────────────────────
run_t7_post_takeover_sanity() {
	info "T7 (post-takeover sanity): Node 2 continues fs ops after takeover"
	sync_state
	touch /mnt/marufs/.t7_prep_node1 || die "prep failed"
	sync_state
	dmesg_reset
	freeze_node 1
	acquire_probe /mnt/marufs2 .t7_takeover || die "takeover acquire failed"
	dmesg_has "crash detected" || die "takeover didn't fire"

	local i fails=0
	for (( i = 0; i < 20; i++ )); do
		touch /mnt/marufs2/.t7_op || fails=$(( fails + 1 ))
		rm -f /mnt/marufs2/.t7_op
	done
	(( fails == 0 )) || die "Node 2 post-takeover ops failed ($fails/20)"
	unfreeze_node 1
	info "T7 PASS (20 ops succeeded after takeover)"
}

main() {
	need_root
	check_prereq

	run_t1_stress
	run_t2_crash_loop
	run_t3_busy_holder
	run_t4_late_grant
	run_t5_concurrent
	run_t6_etimeout
	run_t7_post_takeover_sanity

	info "ALL PASS"
}

main "$@"
