#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# test_bootstrap_chaos.sh - Real bootstrap chaos: stuck recovery + concurrent mount race.
#
# Standalone test — manages module load/unload and mount lifecycle on its own.
# Does NOT depend on setup_local_multinode.sh.
#
# Tests:
#   T1  stuck-formatter recovery
#         Set bootstrap_inject_stuck_formatter=1, mount node A.
#         A becomes formatter but skips GSB-magic write; slot[0] stays FORMATTING.
#         Mount node B with format_timeout_ms=2000; B waits, detects stuck,
#         steals slot[0], re-formats, succeeds.
#         Verify: B's mount succeeded and slot[0]=CLAIMED.
#
#   T2  concurrent mount race (N nodes mount simultaneously)
#         Spawn N mounts in parallel.  All must succeed (last one becomes
#         formatter if needed; others retry or become joiners).
#         Verify: N CLAIMED slots with unique node_ids.
#
#   T3  serial mount → umount → remount (slot reuse)
#         Mount A; verify CLAIMED; umount A.
#         Dump: slot[0] EMPTY.
#         Mount A again; verify CLAIMED.
#
# Usage:
#   sudo MARUFS_DAX_DEVICE=/dev/dax6.0 ./tests/test_bootstrap_chaos.sh
#   sudo ./tests/test_bootstrap_chaos.sh --keep    # keep mounts on failure
#
# Environment overrides:
#   MARUFS_DAX_DEVICE   block device (default /dev/dax6.0)
#   MARUFS_MODULE_NAME  filesystem type name (default marufs)
#   MARUFS_MODULE_KO    path to marufs.ko (default ../build/marufs.ko)
#   MARUFS_T2_NODES     number of concurrent mounts for T2 (default 4)

set -uo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

DAX_DEVICE="${MARUFS_DAX_DEVICE:-/dev/dax2.0}"
MODULE_NAME="${MARUFS_MODULE_NAME:-marufs}"
MODULE_KO="${MARUFS_MODULE_KO:-${PROJECT_DIR}/build/${MODULE_NAME}.ko}"
MNT_BASE="/mnt/${MODULE_NAME}_chaos"
NUM_NODES_T2="${MARUFS_T2_NODES:-8}"
KEEP_ON_FAIL=false
ME_STRATEGY="${MARUFS_ME_STRATEGY:-request}"

# Sysfs paths (module params — writable before any mount)
PARAM_BASE="/sys/module/${MODULE_NAME}/parameters"
DUMP_BS="/sys/fs/${MODULE_NAME}/debug/bootstrap_dump"

# Tmp dir for parallel mount rc files
TMPDIR_RC="$(mktemp -d /tmp/marufs_chaos_XXXXXX)"
trap 'rm -rf "$TMPDIR_RC"' EXIT

PASS=0
FAIL=0

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------
info()  { echo "[chaos] $*"; }
pass()  { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail()  { echo "  FAIL: $1" >&2; FAIL=$((FAIL + 1)); }
die()   { echo "FATAL: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Pre-flight
# ---------------------------------------------------------------------------
[[ $EUID -eq 0 ]] || die "must run as root"
[[ -c "$DAX_DEVICE" ]] || die "DAX device not found: $DAX_DEVICE"
[[ -f "$MODULE_KO" ]]  || die "marufs.ko not found: $MODULE_KO (build first with make)"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --keep) KEEP_ON_FAIL=true; shift ;;
        *) die "unknown option: $1" ;;
    esac
done

# ---------------------------------------------------------------------------
# Module lifecycle helpers
# ---------------------------------------------------------------------------
module_loaded() { grep -q "^${MODULE_NAME} " /proc/modules 2>/dev/null; }

load_module() {
    if module_loaded; then
        info "module already loaded — unloading first"
        unload_module
    fi
    info "loading $MODULE_KO"
    insmod "$MODULE_KO" || die "insmod failed"
    # Verify module param path is accessible
    local deadline=$((SECONDS + 5))
    while [[ ! -d "$PARAM_BASE" ]] && [[ $SECONDS -lt $deadline ]]; do
        sleep 0.2
    done
    [[ -d "$PARAM_BASE" ]] || die "module param dir not found: $PARAM_BASE"
}

unload_module() {
    # Unmount all chaos mounts first
    local mp
    for mp in "${MNT_BASE}"_*; do
        [[ -d "$mp" ]] || continue
        if mountpoint -q "$mp" 2>/dev/null; then
            umount "$mp" 2>/dev/null || umount -l "$mp" 2>/dev/null || true
        fi
    done
    # Also catch any plain MNT_BASE mounts
    if mountpoint -q "$MNT_BASE" 2>/dev/null; then
        umount "$MNT_BASE" 2>/dev/null || umount -l "$MNT_BASE" 2>/dev/null || true
    fi
    if module_loaded; then
        # Try several rmmod attempts with backoff — sb teardown / kthreads
        # holding refs can take a moment after umount returns.
        local i
        for i in 1 2 3 4 5 6 7 8 9 10; do
            module_loaded || break
            rmmod "$MODULE_NAME" 2>/dev/null && break
            sleep 0.5
        done
        if module_loaded; then
            info "warning: module still loaded after 5s; lsmod shows:"
            lsmod | grep "^${MODULE_NAME}" | sed 's/^/  /'
            info "skipping further unload — manual rmmod may be needed"
        fi
    fi
}

# ---------------------------------------------------------------------------
# Mount helpers
# ---------------------------------------------------------------------------
mount_node() {
    # mount_node <mnt_suffix> [extra_opts] [timeout_seconds]
    # mounts at ${MNT_BASE}_${mnt_suffix}
    # Use unique source per call to avoid util-linux "already mounted" check
    # that triggers when source string ("none") matches /sys/fs/pstore etc.
    local suffix="$1"
    local extra="${2:-}"
    local t_seconds="${3:-}"
    local mp="${MNT_BASE}_${suffix}"
    local src="marufs_${suffix}"
    mkdir -p "$mp"
    local opts="daxdev=${DAX_DEVICE},me_strategy=${ME_STRATEGY}"
    [[ -n "$extra" ]] && opts="${opts},${extra}"
    if [[ -n "$t_seconds" ]]; then
        timeout "$t_seconds" mount -t "${MODULE_NAME}" -o "$opts" "$src" "$mp"
    else
        mount -t "${MODULE_NAME}" -o "$opts" "$src" "$mp"
    fi
}

umount_node() {
    local suffix="$1"
    local mp="${MNT_BASE}_${suffix}"
    if mountpoint -q "$mp" 2>/dev/null; then
        umount "$mp" || umount -l "$mp" || true
    fi
    rmdir "$mp" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Module param helpers
# ---------------------------------------------------------------------------
set_param() {
    local name="$1" val="$2"
    local path="${PARAM_BASE}/${name}"
    [[ -w "$path" ]] || die "module param not writable: $path"
    echo "$val" > "$path"
}

get_param() {
    local name="$1"
    cat "${PARAM_BASE}/${name}" 2>/dev/null || echo ""
}

# ---------------------------------------------------------------------------
# Slot dump helpers
# ---------------------------------------------------------------------------
dump_slots() {
    # Print only the slot lines from the first mount section (node=1).
    # Works whether there is one mount or many.  awk is robust against
    # missing-next-section (single-mount) scenarios that broke the pure
    # grep-pipe approach with pipefail.
    if [[ -r "$DUMP_BS" ]]; then
        awk '
            /^=== mount node=1 ===/ { in_section = 1; next }
            /^=== mount node=/      { in_section = 0; next }
            in_section
        ' "$DUMP_BS"
    else
        echo "(bootstrap_dump sysfs unavailable — no active mounts)"
    fi
}

count_claimed() {
    dump_slots | grep -c "status=CLAIMED" || true
}

slot0_status() {
    dump_slots | grep '^slot\[0\]' | grep -oE "status=[A-Z]+" | cut -d= -f2 || echo "?"
}

# dmesg scan: look for formatter/joiner election log lines added in super.c
dmesg_formatter_count() {
    dmesg | grep -c "bootstrap: formatter elected" 2>/dev/null || true
}
dmesg_joiner_count() {
    dmesg | grep -c "bootstrap: joiner waiting for format" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Cleanup on exit (unless --keep)
# ---------------------------------------------------------------------------
cleanup() {
    local rc=$?
    if [[ $rc -ne 0 ]] && [[ "$KEEP_ON_FAIL" == true ]]; then
        info "Keeping mounts for inspection (--keep). Module: $MODULE_NAME"
        info "Dump: cat $DUMP_BS"
        info "dmesg | grep bootstrap"
        return
    fi
    unload_module 2>/dev/null || true
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Device-wipe helper (used between tests to start from fresh state)
# ---------------------------------------------------------------------------
DAX_ZERO="${SCRIPT_DIR}/dax_zero"
wipe_device() {
    if [[ -x "$DAX_ZERO" ]]; then
        info "wiping $DAX_DEVICE for fresh state"
        "$DAX_ZERO" "$DAX_DEVICE" 2097152 >/dev/null || die "dax_zero failed"
    else
        info "warning: $DAX_ZERO not found; skipping wipe"
    fi
}

# ---------------------------------------------------------------------------
# T1: stuck-formatter recovery
# ---------------------------------------------------------------------------
info "================================================================"
info "T1: stuck-formatter recovery (inject_stuck + steal path)"
info "================================================================"

wipe_device
load_module

# Shorten stuck-detection timeout for fast test
set_param "bootstrap_format_timeout_ms" "2000"

# Activate fault injection BEFORE node A mounts (module param readable during fill_super)
set_param "bootstrap_inject_stuck_formatter" "1"
info "T1: inject_stuck=1, format_timeout_ms=2000"

# Node A mounts — wins slot[0], writes status=FORMATTING, then inject path
# returns -EAGAIN deliberately (mount fails) but bootstrap_release is skipped,
# so slot[0] stays at FORMATTING for node B to detect.
info "T1: mounting node A (mount expected to FAIL with -EAGAIN; slot[0] kept at FORMATTING)"
if mount_node "a" 2>/dev/null; then
    fail "T1: node A mount succeeded (expected -EAGAIN injection)"
    exit 1
else
    info "T1: node A mount failed as expected (inject_stuck path)"
fi

# Confirm slot[0] is FORMATTING via dmesg (bootstrap_dump sysfs requires a live sb)
if dmesg | tail -n 50 | grep -q "stuck-formatter injection active"; then
    info "T1: dmesg confirms inject_stuck path executed"
else
    fail "T1: dmesg missing inject_stuck log line"
    exit 1
fi

# Clear inject flag so node B formats normally after stealing
set_param "bootstrap_inject_stuck_formatter" "0"
info "T1: inject_stuck cleared for node B"

# Record dmesg baseline
dmesg_before=$(dmesg | wc -l)

# Node B mounts — should wait ~2s, detect stuck, steal slot[0], re-format
info "T1: mounting node B (joiner → should detect stuck → steal → format)"
if mount_node "b" "" 15; then
    pass "T1: node B mount succeeded after stuck-formatter recovery"
else
    fail "T1: node B mount timed out or failed (steal path broken)"
fi

# Verify slot[0] is CLAIMED (formatter stole and promoted)
sleep 0.3  # let sysfs registration settle
s0=$(slot0_status)
if [[ "$s0" == "CLAIMED" ]]; then
    pass "T1: slot[0] = CLAIMED after recovery"
else
    echo "    DEBUG: DUMP_BS=$DUMP_BS readable=$( [[ -r "$DUMP_BS" ]] && echo yes || echo no )"
    echo "    DEBUG: ls /sys/fs/${MODULE_NAME}/debug/:"
    ls -la "/sys/fs/${MODULE_NAME}/debug/" 2>&1 | sed 's/^/      /'
    echo "    DEBUG: raw dump (between markers):"
    echo "    >>>>"
    cat "$DUMP_BS" 2>&1 | sed 's/^/      /'
    echo "    <<<<"
    fail "T1: slot[0] = $s0 (expected CLAIMED)"
fi

# Verify dmesg shows steal path
if dmesg | tail -n +"$dmesg_before" | grep -q "stole slot\[0\]"; then
    pass "T1: dmesg confirms slot[0] steal"
else
    fail "T1: dmesg missing 'stole slot[0]' log (steal path not exercised?)"
fi

# Cleanup T1 (node A's mount failed by design, no umount needed)
umount_node "b"
unload_module
set +e; rmdir "${MNT_BASE}_a" "${MNT_BASE}_b" 2>/dev/null; set -e

# ---------------------------------------------------------------------------
# T2: concurrent mount race — N nodes mount simultaneously, M iterations
#
# Stress design (single-host limitations acknowledged):
#   1. File barrier so all N subshells reach mount() syscall together
#      (otherwise fork serialization makes 1st claim before 8th is even forked).
#   2. Per-thread random microsecond jitter post-barrier so different
#      iterations probe different race phasings.
#   3. Loop M iterations.  Each iteration: wipe device → load → barrier-release
#      → wait → verify → unmount → unload.  Aggregates stats across iterations.
#   4. Verifies: every iteration produces N unique CLAIMED slots, exactly 1
#      formatter elected, no mount returns non-zero.
# ---------------------------------------------------------------------------
NUM_ITERATIONS_T2="${MARUFS_T2_ITERS:-10}"
info "================================================================"
info "T2: concurrent mount race ($NUM_NODES_T2 nodes × $NUM_ITERATIONS_T2 iterations)"
info "================================================================"

DAX_ZERO="${SCRIPT_DIR}/dax_zero"
if [[ ! -x "$DAX_ZERO" ]]; then
    fail "T2: $DAX_ZERO not found — cannot wipe between iterations"
    exit 1
fi

# Helper: count dmesg occurrences of pattern since baseline line number
dmesg_count_since() {
    local baseline="$1" pattern="$2"
    dmesg | tail -n +"$baseline" | grep -c "$pattern" || true
}

# Helper: extract a field from all CLAIMED slots in dump (e.g., node_id, token)
extract_claimed_field() {
    local field="$1"
    dump_slots | grep "status=CLAIMED" | grep -oE "${field}=[0-9a-fx]+"
}

# Spawn N concurrent mounts via barrier file; populates rc files in $TMPDIR_RC
spawn_concurrent_mounts() {
    local n_mounts="$1" barrier_file="$2"
    local n mp src
    rm -f "$barrier_file" "${TMPDIR_RC}"/rc_*
    for ((n = 1; n <= n_mounts; n++)); do
        mp="${MNT_BASE}_t2_${n}"
        src="marufs_t2_${n}"
        mkdir -p "$mp"
        (
            # Spin-wait barrier — minimal cost, releases all simultaneously
            while [[ ! -e "$barrier_file" ]]; do : ; done
            # Random microsecond jitter (0-2ms) to scatter entries within
            # the same kernel scheduler tick — reorders thread arrival
            # at bootstrap_claim's CAS-less write+reread.
            local jitter=$((RANDOM % 2000))
            usleep "$jitter" 2>/dev/null || sleep 0
            mount -t "${MODULE_NAME}" \
                  -o "daxdev=${DAX_DEVICE},me_strategy=${ME_STRATEGY}" \
                  "$src" "$mp" 2>/dev/null
            echo $? > "${TMPDIR_RC}/rc_${n}"
        ) &
    done
    # Give subshells time to reach the barrier (200ms) then release them
    sleep 0.2
    touch "$barrier_file"
    wait
}

# Tally rc files; sets iter_ok and iter_fail in caller scope
tally_rc_files() {
    local n_mounts="$1"
    local n rc_file
    iter_ok=0
    iter_fail=0
    for ((n = 1; n <= n_mounts; n++)); do
        rc_file="${TMPDIR_RC}/rc_${n}"
        if [[ -f "$rc_file" ]] && [[ "$(cat "$rc_file")" -eq 0 ]]; then
            iter_ok=$((iter_ok + 1))
        else
            iter_fail=$((iter_fail + 1))
        fi
    done
}

# Verify per-iteration invariants; updates t2_iter_failures global
verify_iter_invariants() {
    local it="$1" iter_ok="$2" dmesg_baseline="$3"

    # Mount count
    if [[ $iter_ok -ne $NUM_NODES_T2 ]]; then
        info "  iter $it: $iter_ok/$NUM_NODES_T2 mounts succeeded"
        t2_iter_failures=$((t2_iter_failures + 1))
    fi

    # CLAIMED slot count
    local claimed
    claimed=$(count_claimed)
    if [[ "$claimed" -ne "$NUM_NODES_T2" ]]; then
        info "  iter $it: $claimed CLAIMED slots (expected $NUM_NODES_T2)"
        t2_iter_failures=$((t2_iter_failures + 1))
    fi

    # Unique node_ids
    local all_ids uniq_ids
    all_ids=$(extract_claimed_field "node_id" | sort)
    uniq_ids=$(echo "$all_ids" | sort -u)
    if [[ "$all_ids" != "$uniq_ids" ]]; then
        info "  iter $it: duplicate node_ids: $all_ids"
        t2_iter_failures=$((t2_iter_failures + 1))
    fi

    # Token uniqueness
    local all_toks uniq_toks n_toks n_uniq_toks
    all_toks=$(extract_claimed_field "token" | sort)
    uniq_toks=$(echo "$all_toks" | sort -u)
    n_toks=$(echo "$all_toks" | wc -l)
    n_uniq_toks=$(echo "$uniq_toks" | wc -l)
    if [[ "$n_toks" -ne "$n_uniq_toks" ]] || [[ "$n_toks" -ne "$NUM_NODES_T2" ]]; then
        info "  iter $it: token count=$n_toks unique=$n_uniq_toks (expect $NUM_NODES_T2 unique)"
        t2_iter_failures=$((t2_iter_failures + 1))
    fi

    # dmesg claim trace count
    local iter_claims n_iter_claims
    iter_claims=$(dmesg | tail -n +"$dmesg_baseline" \
        | grep -oE "bootstrap: node_id=[0-9]+ \(slot [0-9]+\)" | sort -u)
    n_iter_claims=$(echo "$iter_claims" | grep -c . || true)
    if [[ "$n_iter_claims" -ne "$iter_ok" ]]; then
        info "  iter $it: dmesg claim lines=$n_iter_claims, expected=$iter_ok"
        t2_iter_failures=$((t2_iter_failures + 1))
    fi
}

# Run a single T2 iteration; updates t2_total_* globals
run_t2_iteration() {
    local it="$1"
    info "T2: --- iteration $it/$NUM_ITERATIONS_T2 ---"

    "$DAX_ZERO" "$DAX_DEVICE" 2097152 >/dev/null || die "dax_zero failed"
    load_module
    set_param "bootstrap_format_timeout_ms" "30000"
    # Widen the race window between free-slot scan and CLAIMED write so
    # concurrent mounters actually collide on the same slot.  Without this,
    # the race window is microseconds and 8 contending mounts almost never
    # observe the same free slot — the CAS-less claim path is never exercised.
    set_param "bootstrap_debug_pre_write_delay_us" "${MARUFS_T2_DELAY_US:-5000}"

    local dmesg_baseline
    dmesg_baseline=$(dmesg | wc -l)
    local barrier_file="${TMPDIR_RC}/barrier_${it}"

    spawn_concurrent_mounts "$NUM_NODES_T2" "$barrier_file"

    local iter_ok iter_fail
    tally_rc_files "$NUM_NODES_T2"

    t2_total_ok=$((t2_total_ok + iter_ok))
    t2_total_fail=$((t2_total_fail + iter_fail))

    verify_iter_invariants "$it" "$iter_ok" "$dmesg_baseline"

    local iter_fmt iter_steal iter_race claimed
    claimed=$(count_claimed)
    iter_fmt=$(dmesg_count_since "$dmesg_baseline" "bootstrap: formatter elected")
    iter_steal=$(dmesg_count_since "$dmesg_baseline" "stole slot")
    iter_race=$(dmesg_count_since "$dmesg_baseline" "claim race lost")
    t2_total_formatter=$((t2_total_formatter + iter_fmt))
    t2_total_steal_attempts=$((t2_total_steal_attempts + iter_steal))

    info "  iter $it: ok=$iter_ok fail=$iter_fail claimed=$claimed formatter=$iter_fmt steal=$iter_steal race-lost=$iter_race"

    local n
    for ((n = 1; n <= NUM_NODES_T2; n++)); do
        umount_node "t2_${n}"
    done
    unload_module
}

t2_total_ok=0
t2_total_fail=0
t2_total_formatter=0
t2_total_steal_attempts=0
t2_iter_failures=0

for ((it = 1; it <= NUM_ITERATIONS_T2; it++)); do
    run_t2_iteration "$it"
done

# Aggregate verdict
expected_total=$((NUM_NODES_T2 * NUM_ITERATIONS_T2))
if [[ $t2_total_ok -eq $expected_total ]]; then
    pass "T2: all $expected_total mounts succeeded across $NUM_ITERATIONS_T2 iterations"
else
    fail "T2: $t2_total_ok/$expected_total mounts succeeded ($t2_total_fail failed)"
fi

if [[ $t2_total_formatter -eq $NUM_ITERATIONS_T2 ]]; then
    pass "T2: exactly 1 formatter per iteration (total $t2_total_formatter)"
else
    fail "T2: expected $NUM_ITERATIONS_T2 formatter elections, got $t2_total_formatter"
fi

if [[ $t2_iter_failures -eq 0 ]]; then
    pass "T2: every iteration passed all sanity checks"
else
    fail "T2: $t2_iter_failures iterations had sanity-check failures"
fi

info "T2: aggregate stats — total ok=$t2_total_ok fail=$t2_total_fail formatter=$t2_total_formatter steal_attempts=$t2_total_steal_attempts"

# ---------------------------------------------------------------------------
# T3: serial mount → umount → remount (slot reuse sanity)
# ---------------------------------------------------------------------------
info "================================================================"
info "T3: serial mount → umount → remount (slot reuse)"
info "================================================================"

load_module
set_param "bootstrap_format_timeout_ms" "30000"

info "T3: first mount"
if mount_node "t3"; then
    pass "T3: first mount succeeded"
else
    fail "T3: first mount failed"
fi

s0=$(slot0_status)
if [[ "$s0" == "CLAIMED" ]]; then
    pass "T3: slot[0] = CLAIMED after first mount"
else
    fail "T3: slot[0] = $s0 after first mount (expected CLAIMED)"
fi

info "T3: unmounting"
umount_node "t3"

# Slot[0] should now be EMPTY (graceful umount path)
# Need at least one mount active for dump sysfs to work; mount briefly again
# Actually slot status is in CXL memory; check via re-mount logging
# Instead: remount and check dmesg — if slot reused it won't re-format
dmesg_before_t3=$(dmesg | wc -l)

info "T3: remount (should reuse EMPTY slot without re-formatting)"
if mount_node "t3"; then
    pass "T3: remount succeeded"
else
    fail "T3: remount failed"
fi

s0_after=$(slot0_status)
if [[ "$s0_after" == "CLAIMED" ]]; then
    pass "T3: slot[0] = CLAIMED after remount"
else
    fail "T3: slot[0] = $s0_after after remount (expected CLAIMED)"
fi

# Verify no second format happened (formatter elected log would appear again only if format needed)
fmt_after=$(dmesg | tail -n +"$dmesg_before_t3" | grep -c "bootstrap: formatter elected" || true)
# Second mount on already-formatted device: slot[0] EMPTY → claim succeeds,
# needs_format=false → no formatter elected log (slot_idx=0 && !needs_format branch).
if [[ "$fmt_after" -eq 0 ]]; then
    pass "T3: no re-format on remount of existing device"
else
    fail "T3: unexpected re-format on remount ($fmt_after formatter-elected events)"
fi

umount_node "t3"
unload_module

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "Bootstrap chaos: PASS=$PASS FAIL=$FAIL"
if [[ $FAIL -eq 0 ]]; then
    echo "All bootstrap chaos tests PASSED."
    exit 0
else
    echo "Some bootstrap chaos tests FAILED."
    exit 1
fi
