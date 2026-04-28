#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# MARUFS Local Multi-Node Test Suite
#
# Tests multi-node behavior on a single host by mounting the same DEV_DAX
# device twice with different node_ids.
#
#   node i -> /mnt/marufs[i+1]  (i=0 -> /mnt/marufs; i>=1 -> /mnt/marufs<i+1>)
#
# Requirements:
#   - MARUFS module built (marufs.ko)
#   - DEV_DAX device available (e.g., /dev/dax6.0)
#   - sudo privileges
#
# Usage:
#   ./test_local_multinode.sh                        # Run with defaults (2 mounts)
#   ./test_local_multinode.sh --num-mounts 8         # 8-node simulation
#   ./test_local_multinode.sh --daxheap              # Run with DAXHEAP mode
#   ./test_local_multinode.sh --skip-setup           # Skip format/mount (already mounted)
#   ./test_local_multinode.sh --no-cleanup           # Keep mounts after tests
#   MARUFS_DAX_DEVICE=/dev/dax0.0 ./test_local_multinode.sh

set -uo pipefail

# --- Configuration (override via environment) ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

DAX_DEVICE="${MARUFS_DAX_DEVICE:-/dev/dax6.0}"
# Auto-detect module name from Makefile
_makefile_module_name=$(grep '^MODULE_NAME' "$PROJECT_DIR/Makefile" 2>/dev/null | head -1 | sed 's/.*:= *//')
MODULE_NAME="${MARUFS_MODULE_NAME:-${_makefile_module_name:-marufs}}"
NUM_MOUNTS="${MARUFS_NUM_MOUNTS:-2}"          # Simulated nodes (1..8)
# Legacy 2-node vars (also populated into MOUNT_POINTS[0..1] below).
# Test sections that reference MOUNT_POINT_0/_1 literally keep working
# unchanged; sections that want to sweep all nodes iterate MOUNT_POINTS[].
MOUNT_POINT_0="${MARUFS_MOUNT_0:-/mnt/${MODULE_NAME}}"
MOUNT_POINT_1="${MARUFS_MOUNT_1:-/mnt/${MODULE_NAME}2}"
NODE_ID_0="${MARUFS_NODE_0:-1}"
NODE_ID_1="${MARUFS_NODE_1:-2}"
MODULE_PATH="${MARUFS_MODULE:-$PROJECT_DIR/build/${MODULE_NAME}.ko}"
NUM_SHARDS="${MARUFS_NUM_SHARDS:-64}"
NUM_REGIONS="${MARUFS_NUM_REGIONS:-4}"

# DAXHEAP configuration
USE_DAXHEAP="${MARUFS_DAXHEAP:-false}"
DAXHEAP_DIR="${MARUFS_DAXHEAP_DIR:-}"
DAXHEAP_MODULE="${DAXHEAP_DIR}/kernel/core/daxheap.ko"
DAXHEAP_SIZE="${MARUFS_DAXHEAP_SIZE:-192G}"  # Allocation size from daxheap buffer

# --- Flags ---
SKIP_SETUP=false
DO_CLEANUP=true
# Simple loop — supports `--num-mounts 8` (separate arg) form.
while [ $# -gt 0 ]; do
    case "$1" in
        --skip-setup)  SKIP_SETUP=true; shift ;;
        --no-cleanup)  DO_CLEANUP=false; shift ;;
        --daxheap)     USE_DAXHEAP=true; shift ;;
        --num-mounts)  NUM_MOUNTS="$2"; shift 2 ;;
        --help|-h)
            echo "Usage: $0 [--skip-setup] [--no-cleanup] [--daxheap] [--num-mounts N]"
            echo ""
            echo "Options:"
            echo "  --skip-setup   Skip build/format/mount (assume already mounted)"
            echo "  --no-cleanup   Keep mounts after tests complete"
            echo "  --daxheap      Use DAXHEAP mode (WC mmap via daxheap buffer)"
            echo "  --num-mounts N Simulated nodes, 1..8 (default: 2)"
            echo ""
            echo "Environment variables:"
            echo "  MARUFS_DAX_DEVICE   DAX device (default: /dev/dax6.0)"
            echo "  MARUFS_NUM_MOUNTS   Simulated node count (default: 2, max: 8)"
            echo "  MARUFS_MOUNT_<i>    Override mount path for node i (i=0..N-1)"
            echo "  MARUFS_NODE_<i>     Override node_id for node i (default: i+1)"
            echo "  MARUFS_MODULE       Module path (default: \$PROJECT/marufs.ko)"
            echo "  MARUFS_DAXHEAP      true/false (default: false)"
            echo "  MARUFS_DAXHEAP_DIR  daxheap source dir (required when MARUFS_DAXHEAP=true)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

# --- Build MOUNT_POINTS[] / NODE_IDS[] arrays from NUM_MOUNTS + per-index envs ---
if ! [[ "$NUM_MOUNTS" =~ ^[1-8]$ ]]; then
    echo "--num-mounts must be 1..8 (got: $NUM_MOUNTS)" >&2
    exit 1
fi

MOUNT_POINTS=()
NODE_IDS=()
for ((_i = 0; _i < NUM_MOUNTS; _i++)); do
    # Index 0 -> MARUFS_MOUNT_0 or legacy MOUNT_POINT_0
    # Index 1 -> MARUFS_MOUNT_1 or legacy MOUNT_POINT_1
    # Index i>=2 -> MARUFS_MOUNT_<i> or /mnt/<module><i+1>
    _m_var="MARUFS_MOUNT_$_i"
    _n_var="MARUFS_NODE_$_i"
    if [ "$_i" -eq 0 ]; then
        _default_mp="$MOUNT_POINT_0"; _default_nid="$NODE_ID_0"
    elif [ "$_i" -eq 1 ]; then
        _default_mp="$MOUNT_POINT_1"; _default_nid="$NODE_ID_1"
    else
        _default_mp="/mnt/${MODULE_NAME}$((_i + 1))"
        _default_nid=$((_i + 1))
    fi
    MOUNT_POINTS[_i]="${!_m_var:-$_default_mp}"
    NODE_IDS[_i]="${!_n_var:-$_default_nid}"
done

# Keep legacy scalar aliases pointing at array slots (tests that reference
# MOUNT_POINT_0 / MOUNT_POINT_1 textually still work).
MOUNT_POINT_0="${MOUNT_POINTS[0]}"
MOUNT_POINT_1="${MOUNT_POINTS[1]:-${MOUNT_POINTS[0]}}"
NODE_ID_0="${NODE_IDS[0]}"
NODE_ID_1="${NODE_IDS[1]:-${NODE_IDS[0]}}"

# --- Colors ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# --- Counters ---
PASS=0
FAIL=0
TOTAL=0

# --- Test Runner ---
run_test() {
    local name="$1"
    shift
    local cmd="$*"

    TOTAL=$((TOTAL + 1))
    printf "${CYAN}[T%-2d]${NC} %-58s " "$TOTAL" "$name"

    local output
    local rc=0
    output=$(eval "$cmd" 2>&1) || rc=$?

    if [ $rc -eq 0 ]; then
        PASS=$((PASS + 1))
        echo -e "${GREEN}PASS${NC}"
    else
        FAIL=$((FAIL + 1))
        echo -e "${RED}FAIL${NC}"
        if [ -n "$output" ]; then
            echo "$output" | sed 's/^/       /'
        fi
    fi
}

# --- Helper: file count ---
file_count() {
    local mp="$1"
    ls -1 "$mp" 2>/dev/null | wc -l | tr -d '[:space:]'
}

# --- Helper: clean all mount points ---
clean_all() {
    local mp
    for mp in "${MOUNT_POINTS[@]}"; do
        rm -f "$mp"/* 2>/dev/null || true
    done
}

# --- Helper: build mount options for a given node_id ---
mount_opts_for() {
    local nid="$1"
    if [ "$USE_DAXHEAP" = true ]; then
        # First node_id (NODE_ID_0) is primary (allocates buffer),
        # all others are secondary (import by buf_id from sysfs)
        if [ "$nid" = "$NODE_ID_0" ]; then
            echo "daxheap=${DAXHEAP_SIZE},node_id=${nid}"
        else
            local bufid_file="/sys/fs/${MODULE_NAME}/daxheap_bufid"
            local bufid
            bufid=$(cat "$bufid_file" 2>/dev/null)
            if [ -z "$bufid" ] || [ "$bufid" = "0x0" ]; then
                echo >&2 "ERROR: no valid buf_id in $bufid_file (primary not mounted?)"
                echo "daxheap_bufid=INVALID,node_id=${nid}"
                return 1
            fi
            echo "daxheap_bufid=${bufid},node_id=${nid}"
        fi
    else
        local base="daxdev=${DAX_DEVICE},node_id=${nid}"
        # First DEV_DAX mount formats the device
        if [ "$nid" = "$NODE_ID_0" ]; then
            echo "${base},format"
        else
            echo "${base}"
        fi
    fi
}

# --- Setup ---
setup() {
    echo -e "${YELLOW}--- Setup ---${NC}"

    # Unmount all in reverse order (secondaries before primary)
    local _i _mp
    for ((_i = ${#MOUNT_POINTS[@]} - 1; _i >= 0; _i--)); do
        _mp="${MOUNT_POINTS[_i]}"
        mount | grep -q " $_mp " && sudo umount "$_mp" 2>/dev/null
    done

    # Unload modules if loaded
    if lsmod | grep -q "^${MODULE_NAME} "; then
        sudo rmmod "${MODULE_NAME}" 2>/dev/null || true
    fi
    # Note: daxheap module is NOT unloaded here — managed externally

    # Build if needed
    if [ ! -f "$MODULE_PATH" ]; then
        echo "  Building MARUFS..."
        cd "$PROJECT_DIR"
        if [ "$USE_DAXHEAP" = true ]; then
            make clean > /dev/null 2>&1 || true
            make DAXHEAP_DIR="$DAXHEAP_DIR" -j$(nproc) > /dev/null 2>&1
        else
            make clean > /dev/null 2>&1 || true
            make -j$(nproc) > /dev/null 2>&1
        fi
        cd "$SCRIPT_DIR"
    fi

    # Ensure daxheap is loaded (if daxheap mode), but don't reload if present
    if [ "$USE_DAXHEAP" = true ]; then
        if lsmod | grep -q "^daxheap "; then
            echo "  daxheap already loaded, skipping insmod"
        else
            echo "  Loading daxheap (dax_device=$DAX_DEVICE, host_id=$NODE_ID_0)..."
            sudo insmod "$DAXHEAP_MODULE" dax_device="$DAX_DEVICE" host_id="$NODE_ID_0"
            sleep 1
        fi
    fi

    # Load MARUFS module
    echo "  Loading module (node_id=$NODE_ID_0)..."
    sudo insmod "$MODULE_PATH" node_id=$NODE_ID_0

    # Create mount points (format happens on first mount via 'format' option)
    local _j
    for _j in "${!MOUNT_POINTS[@]}"; do
        sudo mkdir -p "${MOUNT_POINTS[_j]}"
    done

    # Mount each node (first one formats due to mount_opts_for's format flag).
    local _opts
    for _j in "${!MOUNT_POINTS[@]}"; do
        _opts=$(mount_opts_for "${NODE_IDS[_j]}")
        echo "  Mounting node ${NODE_IDS[_j]} -> ${MOUNT_POINTS[_j]} ($_opts)"
        sudo mount -t "${MODULE_NAME}" -o "$_opts" none "${MOUNT_POINTS[_j]}"
        sudo chmod 1777 "${MOUNT_POINTS[_j]}"
    done

    if [ "$USE_DAXHEAP" = true ]; then
        echo -e "  ${GREEN}Setup complete (DAXHEAP mode)${NC}"
    else
        echo -e "  ${GREEN}Setup complete${NC}"
    fi
    echo ""
}

# --- Cleanup ---
cleanup() {
    if [ "$DO_CLEANUP" = true ]; then
        echo ""
        echo -e "${YELLOW}--- Cleanup ---${NC}"
        local _i
        for ((_i = ${#MOUNT_POINTS[@]} - 1; _i >= 0; _i--)); do
            sudo umount "${MOUNT_POINTS[_i]}" 2>/dev/null || true
        done
        echo "  Mounts removed"
    fi
}
trap cleanup EXIT

# --- Remount helper (used within tests) ---
# Unmount all, re-format via mount option, remount all.
remount_fresh() {
    local _i _opts
    for ((_i = ${#MOUNT_POINTS[@]} - 1; _i >= 0; _i--)); do
        sudo umount "${MOUNT_POINTS[_i]}" 2>/dev/null || true
    done
    for _i in "${!MOUNT_POINTS[@]}"; do
        _opts=$(mount_opts_for "${NODE_IDS[_i]}")
        sudo mount -t "${MODULE_NAME}" -o "$_opts" none "${MOUNT_POINTS[_i]}"
        sudo chmod 1777 "${MOUNT_POINTS[_i]}"
    done
}

# ============================================================================
# Banner
# ============================================================================
MODE_LABEL="DEV_DAX"
if [ "$USE_DAXHEAP" = true ]; then
    MODE_LABEL="DAXHEAP (WC mmap)"
fi

echo "============================================"
echo "  MARUFS Local Multi-Node Test Suite"
echo "============================================"
echo "  Mode:    $MODE_LABEL"
echo "  Device:  $DAX_DEVICE"
echo "  Mounts:  $NUM_MOUNTS"
for _i in "${!MOUNT_POINTS[@]}"; do
    printf "    Node %d: %s (node_id=%s)\n" \
        "$_i" "${MOUNT_POINTS[_i]}" "${NODE_IDS[_i]}"
done
echo "  Module:  $MODULE_PATH"
if [ "$USE_DAXHEAP" = true ]; then
    echo "  daxheap: $DAXHEAP_MODULE"
fi
echo "============================================"
echo ""

# ============================================================================
# Setup
# ============================================================================
if [ "$SKIP_SETUP" = false ]; then
    setup
else
    echo -e "${YELLOW}--- Skipping setup (--skip-setup) ---${NC}"
    echo ""
fi

# Pre-flight check: every configured mount must be active
for _i in "${!MOUNT_POINTS[@]}"; do
    if ! mount | grep -q "${MOUNT_POINTS[_i]} type ${MODULE_NAME}"; then
        echo -e "${RED}ERROR: Mount $_i not active: ${MOUNT_POINTS[_i]}${NC}"
        mount | grep "${MODULE_NAME}" || true
        exit 1
    fi
done

# Pause GC to prevent dead-process reclamation during tests.
# Files created by short-lived processes (touch) would be reclaimed by GC
# before the test can verify them.
GC_PAUSE_FILE="/sys/fs/${MODULE_NAME}/debug/gc_pause"
GC_PAUSED=false
if [ -f "$GC_PAUSE_FILE" ]; then
    if bash -c "echo 1 > '$GC_PAUSE_FILE'" 2>/dev/null; then
        GC_PAUSED=true
    elif printf '1' | sudo -n tee "$GC_PAUSE_FILE" > /dev/null 2>&1; then
        GC_PAUSED=true
    fi
    if [ "$GC_PAUSED" = true ]; then
        echo -e "${CYAN}[INFO]${NC} GC paused for test duration"
    else
        echo -e "${YELLOW}[WARN]${NC} Could not pause GC - some tests may be flaky (run with sudo)"
    fi
fi

# Build test binaries
echo -e "${YELLOW}--- Building test binaries ---${NC}"
build_ok=true
for src in "$SCRIPT_DIR"/test_ioctl.c "$SCRIPT_DIR"/test_mmap.c "$SCRIPT_DIR"/test_mmap_cuda.c "$SCRIPT_DIR"/test_cross_process.c "$SCRIPT_DIR"/test_overlap.c "$SCRIPT_DIR"/test_chown_race.c "$SCRIPT_DIR"/test_mmap_notrunc.c "$SCRIPT_DIR"/test_negative.c "$SCRIPT_DIR"/test_nrht_race.c "$SCRIPT_DIR"/test_gc_deleg.c "$SCRIPT_DIR"/test_pid_reuse.c; do
    [ -f "$src" ] || continue
    bin="${src%.c}"
    name="$(basename "$src")"
    ldflags=""
    case "$name" in
        test_mmap_cuda.c) ldflags="-ldl" ;;
    esac
    if gcc -Wall -Wextra -I"$SCRIPT_DIR/../include" -o "$bin" "$src" $ldflags 2>&1; then
        echo -e "  ${GREEN}✓${NC} $name"
    else
        echo -e "  ${RED}✗${NC} $name"
        build_ok=false
    fi
done
$build_ok || echo -e "${YELLOW}[WARN]${NC} Some test binaries failed to build"
echo ""

# Clean all files before starting tests
clean_all

# Clear dmesg so kernel health checks only see messages from this test run
if [ "$(id -u)" -eq 0 ]; then
    dmesg -c > /dev/null 2>&1 || true
fi

# ============================================================================
# Section 1: Single Node Basic (Node 0)
# ============================================================================
echo -e "${YELLOW}=== Section 1: Single Node Basic (Node 0) ===${NC}"

run_test "Create single file" '
    touch $MOUNT_POINT_0/single.txt && [ -f $MOUNT_POINT_0/single.txt ]
'

run_test "Create 10 files" '
    for i in $(seq 1 10); do touch $MOUNT_POINT_0/multi_${i}.txt; done && \
    count=$(file_count $MOUNT_POINT_0) && [ "$count" -eq 11 ]
'

run_test "Delete 1 file, verify count" '
    rm $MOUNT_POINT_0/single.txt && \
    count=$(file_count $MOUNT_POINT_0) && [ "$count" -eq 10 ]
'

run_test "Touch existing file (idempotent)" '
    touch $MOUNT_POINT_0/multi_1.txt
'

run_test "Long filename (63 chars)" '
    LONG63=$(python3 -c "print(\"f\" * 63)")
    touch "$MOUNT_POINT_0/$LONG63" && \
    [ -f "$MOUNT_POINT_0/$LONG63" ] && \
    rm -f "$MOUNT_POINT_0/$LONG63"
'

run_test "Too-long filename rejected (192+ chars)" '
    LONG192=$(python3 -c "print(\"f\" * 192)")
    ! touch "$MOUNT_POINT_0/$LONG192" 2>/dev/null
'

run_test "WORM: write() rejected" '
    ! echo "data" > $MOUNT_POINT_0/multi_1.txt 2>/dev/null
'

run_test "Mass delete all files" '
    rm -f $MOUNT_POINT_0/*.txt 2>/dev/null; \
    count=$(file_count $MOUNT_POINT_0) && [ "$count" -eq 0 ]
'

run_test "statfs reports correct type" '
    stat -f -c "%t" $MOUNT_POINT_0 | grep -qi "4d415255"
'

echo ""

# ============================================================================
# Section 2: Single Node Basic (Node 1)
# ============================================================================
echo -e "${YELLOW}=== Section 2: Single Node Basic (Node 1) ===${NC}"
clean_all

run_test "Node 1: Create 5 files" '
    for i in $(seq 1 5); do touch $MOUNT_POINT_1/n1_${i}.txt; done && \
    count=$(file_count $MOUNT_POINT_1) && [ "$count" -ge 5 ]
'

run_test "Node 1: Delete 2 files" '
    rm $MOUNT_POINT_1/n1_1.txt $MOUNT_POINT_1/n1_2.txt && \
    [ ! -f $MOUNT_POINT_1/n1_1.txt ] && [ ! -f $MOUNT_POINT_1/n1_2.txt ]
'

run_test "Node 1: Cleanup" '
    rm -f $MOUNT_POINT_1/n1_*.txt
'

echo ""

# ============================================================================
# Section 3: Cross-Node Visibility
# ============================================================================
echo -e "${YELLOW}=== Section 3: Cross-Node Visibility ===${NC}"
clean_all

run_test "Node 0 creates file, Node 1 sees it" '
    touch $MOUNT_POINT_0/cross_from_n0.txt && \
    [ -f $MOUNT_POINT_1/cross_from_n0.txt ]
'

run_test "Node 1 creates file, Node 0 sees it" '
    touch $MOUNT_POINT_1/cross_from_n1.txt && \
    [ -f $MOUNT_POINT_0/cross_from_n1.txt ]
'

run_test "Both nodes see identical listing" '
    list0=$(ls -1 $MOUNT_POINT_0 2>/dev/null | sort) && \
    list1=$(ls -1 $MOUNT_POINT_1 2>/dev/null | sort) && \
    [ "$list0" = "$list1" ]
'

run_test "Node 0 creates 10 more, Node 1 sees all" '
    for i in $(seq 1 10); do touch $MOUNT_POINT_0/vis_${i}.txt; done && \
    count1=$(file_count $MOUNT_POINT_1) && [ "$count1" -eq 12 ]
'

run_test "Node 0 deletes files, Node 1 reflects" '
    for i in $(seq 1 10); do rm -f $MOUNT_POINT_0/vis_${i}.txt; done && \
    count1=$(file_count $MOUNT_POINT_1) && [ "$count1" -eq 2 ]
'

# Cleanup
rm -f $MOUNT_POINT_0/cross_from_n0.txt 2>/dev/null
rm -f $MOUNT_POINT_1/cross_from_n1.txt 2>/dev/null

# Cross-process visibility test (C binary with step-by-step sync)
CROSS_PROC_BIN="$SCRIPT_DIR/test_cross_process"
if [ -x "$CROSS_PROC_BIN" ]; then
    run_test "Cross-process: create/truncate/mmap/unlink visibility" '
        "$CROSS_PROC_BIN" "$MOUNT_POINT_0" "$MOUNT_POINT_1" 2>&1
    '
else
    echo -e "  ${YELLOW}SKIP${NC} cross-process test (binary not built)"
fi

echo ""

# ============================================================================
# Section 4: Access Control (Region Isolation)
# ============================================================================
echo -e "${YELLOW}=== Section 4: Access Control (Region Isolation) ===${NC}"
clean_all

run_test "Node 0 creates file in its region" '
    touch $MOUNT_POINT_0/acl_n0.txt && [ -f $MOUNT_POINT_0/acl_n0.txt ]
'

run_test "Node 1 creates file in its region" '
    touch $MOUNT_POINT_1/acl_n1.txt && [ -f $MOUNT_POINT_1/acl_n1.txt ]
'

run_test "Node 1 cannot delete Node 0 file" '
    ! rm $MOUNT_POINT_1/acl_n0.txt 2>/dev/null
'

run_test "Node 0 cannot delete Node 1 file" '
    ! rm $MOUNT_POINT_0/acl_n1.txt 2>/dev/null
'

run_test "Owner can delete own file (Node 0)" '
    rm $MOUNT_POINT_0/acl_n0.txt && [ ! -f $MOUNT_POINT_0/acl_n0.txt ]
'

run_test "Owner can delete own file (Node 1)" '
    rm $MOUNT_POINT_1/acl_n1.txt && [ ! -f $MOUNT_POINT_1/acl_n1.txt ]
'

echo ""

# ============================================================================
# Section 5: Concurrent Writes (CAS Contention)
# ============================================================================
echo -e "${YELLOW}=== Section 5: Concurrent Writes (CAS Contention) ===${NC}"
# Pause GC to prevent dead-process reclamation of files created by short-lived
# touch processes (each touch exits immediately, making owner_pid dead).
bash -c 'echo 1 > /sys/fs/'"${MODULE_NAME}"'/debug/gc_pause' 2>/dev/null || true
clean_all

run_test "Both nodes create 10 files each simultaneously" '
    for i in $(seq 1 10); do touch $MOUNT_POINT_0/cw_n0_${i}.txt; done &
    pid0=$!
    for i in $(seq 1 10); do touch $MOUNT_POINT_1/cw_n1_${i}.txt; done &
    pid1=$!
    wait $pid0 && wait $pid1 && \
    sleep 0.5 && \
    count0=$(file_count $MOUNT_POINT_0) && \
    count1=$(file_count $MOUNT_POINT_1) && \
    [ "$count0" -eq 20 ] && [ "$count1" -eq 20 ]
'

run_test "Mass concurrent: 50 files each" '
    clean_all
    for i in $(seq 1 50); do touch $MOUNT_POINT_0/mass_n0_${i}.txt; done &
    pid0=$!
    for i in $(seq 1 50); do touch $MOUNT_POINT_1/mass_n1_${i}.txt; done &
    pid1=$!
    wait $pid0 && wait $pid1 && \
    sleep 0.5 && \
    count0=$(file_count $MOUNT_POINT_0) && \
    count1=$(file_count $MOUNT_POINT_1) && \
    [ "$count0" -eq 100 ] && [ "$count1" -eq 100 ]
'

run_test "Cross-visibility consistency after concurrent writes" '
    list0=$(ls -1 $MOUNT_POINT_0 2>/dev/null | sort) && \
    list1=$(ls -1 $MOUNT_POINT_1 2>/dev/null | sort) && \
    [ "$list0" = "$list1" ]
'

run_test "Concurrent create+delete cycle" '
    (
        for i in $(seq 1 20); do touch $MOUNT_POINT_0/cd_n0_${i}.txt; done
        for i in $(seq 1 20); do rm -f $MOUNT_POINT_0/cd_n0_${i}.txt; done
        for i in $(seq 1 20); do touch $MOUNT_POINT_0/final_n0_${i}.txt; done
    ) &
    pid0=$!
    (
        for i in $(seq 1 20); do touch $MOUNT_POINT_1/cd_n1_${i}.txt; done
        for i in $(seq 1 20); do rm -f $MOUNT_POINT_1/cd_n1_${i}.txt; done
        for i in $(seq 1 20); do touch $MOUNT_POINT_1/final_n1_${i}.txt; done
    ) &
    pid1=$!
    wait $pid0 && wait $pid1 && \
    sleep 0.5 && \
    count0=$(file_count $MOUNT_POINT_0) && \
    count1=$(file_count $MOUNT_POINT_1) && \
    [ "$count0" -eq "$count1" ]
'

# Cleanup concurrent files
rm -f $MOUNT_POINT_0/cw_n0_*.txt $MOUNT_POINT_0/mass_n0_*.txt $MOUNT_POINT_0/final_n0_*.txt 2>/dev/null || true
rm -f $MOUNT_POINT_1/cw_n1_*.txt $MOUNT_POINT_1/mass_n1_*.txt $MOUNT_POINT_1/final_n1_*.txt 2>/dev/null || true

# Concurrent ftruncate: verify no physical space overlap (C2 fix)
SYSFS_BASE="/sys/fs/${MODULE_NAME}"
OVERLAP_BIN="$SCRIPT_DIR/test_overlap"
if [ -x "$OVERLAP_BIN" ]; then
    run_test "Concurrent ftruncate: no physical overlap (20 rounds)" '
        clean_all
        "$OVERLAP_BIN" "$MOUNT_POINT_0" "$MOUNT_POINT_1" "$SYSFS_BASE/region_info" 20 2>&1
    '
else
    echo -e "  ${YELLOW}SKIP${NC} overlap test (binary not built)"
fi

echo ""

# ============================================================================
# Section 6: Stress Tests
# ============================================================================
echo -e "${YELLOW}=== Section 6: Stress Tests ===${NC}"
clean_all

run_test "Rapid create/delete 100 files" '
    for i in $(seq 1 100); do touch $MOUNT_POINT_0/stress_${i}.txt; done && \
    for i in $(seq 1 100); do rm -f $MOUNT_POINT_0/stress_${i}.txt; done && \
    count=$(file_count $MOUNT_POINT_0) && [ "$count" -eq 0 ]
'

run_test "Create 200 files, verify, delete all" '
    for i in $(seq 1 200); do touch $MOUNT_POINT_0/many_${i}.txt; done && \
    count=$(file_count $MOUNT_POINT_0) && [ "$count" -eq 200 ] && \
    for i in $(seq 1 200); do rm -f $MOUNT_POINT_0/many_${i}.txt; done && \
    count2=$(file_count $MOUNT_POINT_0) && [ "$count2" -eq 0 ]
'

run_test "Concurrent: Node 0 writes + Node 1 reads" '
    for i in $(seq 1 20); do ls $MOUNT_POINT_1/ > /dev/null 2>&1; done &
    pid1=$!
    for i in $(seq 1 50); do touch $MOUNT_POINT_0/wr_${i}.txt; done && \
    wait $pid1 && \
    count0=$(file_count $MOUNT_POINT_0) && \
    count1=$(file_count $MOUNT_POINT_1) && \
    [ "$count0" -eq 50 ] && [ "$count1" -eq 50 ]
'

# Cleanup stress files
rm -f $MOUNT_POINT_0/wr_*.txt 2>/dev/null || true
# Resume GC after concurrent/stress sections
bash -c 'echo 0 > /sys/fs/'"${MODULE_NAME}"'/debug/gc_pause' 2>/dev/null || true
echo ""

# ============================================================================
# Section 7: (removed - persistence test unreliable with process-scoped files)
echo ""

# ============================================================================
# Section 8: Kernel Health
# ============================================================================
echo ""

# ============================================================================
# Section 9: ioctl Tests (two-phase create + offset naming)
# ============================================================================
echo -e "${YELLOW}=== Section 9: ioctl Tests (offset naming) ===${NC}"

IOCTL_TEST_SRC="$SCRIPT_DIR/test_ioctl.c"
IOCTL_TEST_BIN="$SCRIPT_DIR/test_ioctl"

if [ -f "$IOCTL_TEST_SRC" ]; then
    # Compile test_ioctl.c
    run_test "Compile test_ioctl.c" '
        gcc -Wall -Wextra -I"$SCRIPT_DIR/../include" -o "$IOCTL_TEST_BIN" "$IOCTL_TEST_SRC" 2>&1
    '

    if [ -x "$IOCTL_TEST_BIN" ]; then
        # Run ioctl test on Node 0
        run_test "ioctl: two-phase create + naming (Node 0)" '
            "$IOCTL_TEST_BIN" "$MOUNT_POINT_0" 128 "$NODE_ID_0" 2>&1
        '

        # Fresh state for Node 1 test
        run_test "ioctl: two-phase create + naming (Node 1)" '
            "$IOCTL_TEST_BIN" "$MOUNT_POINT_1" 128 "$NODE_ID_1" 2>&1
        '
    else
        echo -e "  ${YELLOW}SKIP${NC} (compilation failed)"
    fi
else
    echo -e "  ${YELLOW}SKIP${NC} (test_ioctl.c not found)"
fi

echo ""

# ============================================================================
# Section 10: Permission Delegation (multi-node ioctl)
# ============================================================================
echo -e "${YELLOW}=== Section 10: Permission Delegation (multi-node) ===${NC}"

if [ -x "$IOCTL_TEST_BIN" ]; then
    # Full multi-node test: single-node + permission delegation
    # peer_node = NODE_ID_1 because mount2 is MOUNT_POINT_1
    run_test "Permission delegation (Node 0 owner, Node 1 peer)" '
        "$IOCTL_TEST_BIN" "$MOUNT_POINT_0" "$MOUNT_POINT_1" "$NODE_ID_1" 128 "$NODE_ID_0" 2>&1
    '

    # Reverse direction: Node 1 owner, Node 0 peer
    run_test "Permission delegation (Node 1 owner, Node 0 peer)" '
        "$IOCTL_TEST_BIN" "$MOUNT_POINT_1" "$MOUNT_POINT_0" "$NODE_ID_0" 128 "$NODE_ID_1" 2>&1
    '
else
    echo -e "  ${YELLOW}SKIP${NC} (test_ioctl not compiled — Section 9 may have failed)"
fi

echo ""

# ============================================================================
# Section 11: DAXHEAP-Specific Tests
# ============================================================================
if [ "$USE_DAXHEAP" = true ]; then
    echo -e "${YELLOW}=== Section 11: DAXHEAP Tests ===${NC}"

    if [ "$(id -u)" -eq 0 ]; then
        run_test "dmesg: DAXHEAP buffer acquired" '
            dmesg | grep -q "marufs.*DAXHEAP buffer acquired"
        '

        run_test "dmesg: fill_super DAXHEAP mode" '
            dmesg | grep -q "marufs.*fill_super DAXHEAP"
        '

        run_test "daxheap module is loaded" '
            lsmod | grep -q "^daxheap "
        '

        run_test "DAXHEAP: create + read cross-node" '
            touch $MOUNT_POINT_0/dh_test.txt && \
            [ -f $MOUNT_POINT_1/dh_test.txt ] && \
            rm -f $MOUNT_POINT_0/dh_test.txt
        '

        # DMA-BUF export ioctl test (requires test binary)
        DMABUF_TEST_SRC="$SCRIPT_DIR/test_dmabuf.c"
        DMABUF_TEST_BIN="$SCRIPT_DIR/test_dmabuf"

        if [ -f "$DMABUF_TEST_SRC" ]; then
            run_test "Compile test_dmabuf.c" '
                gcc -Wall -Wextra -o "$DMABUF_TEST_BIN" "$DMABUF_TEST_SRC" 2>&1
            '
            if [ -x "$DMABUF_TEST_BIN" ]; then
                run_test "DMA-BUF export ioctl" '
                    "$DMABUF_TEST_BIN" "$MOUNT_POINT_0" 2>&1
                '
            else
                echo -e "  ${YELLOW}SKIP${NC} DMA-BUF export (compilation failed)"
            fi
        else
            echo -e "  ${YELLOW}SKIP${NC} DMA-BUF export test (test_dmabuf.c not found)"
        fi

        # WC mmap verification (requires test binary)
        WC_TEST_SRC="$SCRIPT_DIR/test_wc_mmap.c"
        WC_TEST_BIN="$SCRIPT_DIR/test_wc_mmap"

        if [ -f "$WC_TEST_SRC" ]; then
            run_test "Compile test_wc_mmap.c" '
                gcc -Wall -Wextra -o "$WC_TEST_BIN" "$WC_TEST_SRC" 2>&1
            '
            if [ -x "$WC_TEST_BIN" ]; then
                run_test "WC mmap: write-combining mapping" '
                    "$WC_TEST_BIN" "$MOUNT_POINT_0" 2>&1
                '
            else
                echo -e "  ${YELLOW}SKIP${NC} WC mmap (compilation failed)"
            fi
        else
            echo -e "  ${YELLOW}SKIP${NC} WC mmap test (test_wc_mmap.c not found)"
        fi
    else
        echo -e "  ${YELLOW}SKIP${NC} (requires root - run with sudo for this section)"
    fi

    echo ""
fi

# ============================================================================
# Section 12: Sysfs Interface
# ============================================================================
echo -e "${YELLOW}=== Section 12: Sysfs Interface ===${NC}"

if [ "$(id -u)" -eq 0 ] && [ -d "$SYSFS_BASE" ]; then
    run_test "sysfs: version readable" '
        ver=$(cat $SYSFS_BASE/version) && [ -n "$ver" ] && [ "$ver" -ge 1 ]
    '

    run_test "sysfs: region_info readable" '
        cat $SYSFS_BASE/region_info > /dev/null
    '

    run_test "sysfs: region_info shows entries after create" '
        touch $MOUNT_POINT_0/sysfs_r.txt && \
        info=$(cat $SYSFS_BASE/region_info) && \
        echo "$info" | grep -q "ALLOCATED" && \
        rm -f $MOUNT_POINT_0/sysfs_r.txt
    '

    run_test "sysfs: perm_info readable" '
        cat $SYSFS_BASE/perm_info > /dev/null
    '

    run_test "sysfs: gc_trigger writable" '
        echo 1 > $SYSFS_BASE/debug/gc_trigger
    '

    run_test "sysfs: gc_stop writable" '
        echo 1 > $SYSFS_BASE/debug/gc_stop
    '

    # gc_stop kills threads — restart them for subsequent tests
    run_test "sysfs: gc_restart after stop" '
        echo all > $SYSFS_BASE/debug/gc_restart
    '
else
    echo -e "  ${YELLOW}SKIP${NC} (requires root and mounted filesystem)"
fi

echo ""

# ============================================================================
# Section 13: GC Dead-Process Reclamation
# ============================================================================
echo -e "${YELLOW}=== Section 13: GC Dead-Process Reclamation ===${NC}"

if [ "$(id -u)" -eq 0 ] && [ -d "$SYSFS_BASE" ]; then
    run_test "Dead process file reclaimed by GC" '
        # Subprocess creates file and immediately exits
        bash -c "touch $MOUNT_POINT_0/gc_dead.txt" && \
        sleep 0.2 && \
        # File should exist before GC
        [ -f $MOUNT_POINT_0/gc_dead.txt ] && \
        # Trigger GC to reclaim dead-process RAT entry
        echo 1 > $SYSFS_BASE/debug/gc_trigger && \
        sleep 0.5 && \
        # File should be gone (owner process exited)
        [ ! -f $MOUNT_POINT_0/gc_dead.txt ]
    '

    run_test "Dead process file gone from both nodes" '
        bash -c "touch $MOUNT_POINT_0/gc_dead2.txt" && \
        sleep 0.2 && \
        [ -f $MOUNT_POINT_1/gc_dead2.txt ] && \
        echo 1 > $SYSFS_BASE/debug/gc_trigger && \
        sleep 0.5 && \
        [ ! -f $MOUNT_POINT_0/gc_dead2.txt ] && \
        [ ! -f $MOUNT_POINT_1/gc_dead2.txt ]
    '

else
    echo -e "  ${YELLOW}SKIP${NC} (requires root and mounted filesystem)"
fi

echo ""

# ============================================================================
# Section 14: Stat/Getattr Cross-Node
# ============================================================================
echo -e "${YELLOW}=== Section 14: Stat/Getattr Cross-Node ===${NC}"
clean_all

run_test "Cross-node: stat mode matches" '
    touch $MOUNT_POINT_0/stat_xn.txt && \
    mode0=$(stat -c "%a" $MOUNT_POINT_0/stat_xn.txt) && \
    mode1=$(stat -c "%a" $MOUNT_POINT_1/stat_xn.txt) && \
    [ "$mode0" = "$mode1" ]
'

run_test "Cross-node: stat uid matches" '
    uid0=$(stat -c "%u" $MOUNT_POINT_0/stat_xn.txt) && \
    uid1=$(stat -c "%u" $MOUNT_POINT_1/stat_xn.txt) && \
    [ "$uid0" = "$uid1" ]
'

run_test "Cross-node: stat size matches (0 before truncate)" '
    size0=$(stat -c "%s" $MOUNT_POINT_0/stat_xn.txt) && \
    size1=$(stat -c "%s" $MOUNT_POINT_1/stat_xn.txt) && \
    [ "$size0" = "0" ] && [ "$size1" = "0" ]
'

run_test "Cross-node: stat filetype is regular" '
    type0=$(stat -c "%F" $MOUNT_POINT_0/stat_xn.txt) && \
    type1=$(stat -c "%F" $MOUNT_POINT_1/stat_xn.txt) && \
    echo "$type0" | grep -qi "regular" && \
    echo "$type1" | grep -qi "regular"
'

rm -f $MOUNT_POINT_0/stat_xn.txt 2>/dev/null || true
echo ""

# ============================================================================
# Section 15: Edge Cases
# ============================================================================
echo -e "${YELLOW}=== Section 15: Edge Cases ===${NC}"
clean_all

run_test "Empty file: open, stat, close" '
    touch $MOUNT_POINT_0/edge_empty.txt && \
    size=$(stat -c "%s" $MOUNT_POINT_0/edge_empty.txt) && \
    [ "$size" = "0" ] && \
    rm -f $MOUNT_POINT_0/edge_empty.txt
'

run_test "Duplicate create is idempotent" '
    touch $MOUNT_POINT_0/edge_dup.txt && \
    touch $MOUNT_POINT_0/edge_dup.txt && \
    count=$(ls -1 $MOUNT_POINT_0 | grep "^edge_dup.txt$" | wc -l) && \
    [ "$count" -eq 1 ] && \
    rm -f $MOUNT_POINT_0/edge_dup.txt
'

run_test "Delete + recreate same filename" '
    touch $MOUNT_POINT_0/edge_recycle.txt && \
    rm -f $MOUNT_POINT_0/edge_recycle.txt && \
    [ ! -f $MOUNT_POINT_0/edge_recycle.txt ] && \
    touch $MOUNT_POINT_0/edge_recycle.txt && \
    [ -f $MOUNT_POINT_0/edge_recycle.txt ] && \
    rm -f $MOUNT_POINT_0/edge_recycle.txt
'

run_test "Rapid create/delete same file (10 cycles)" '
    ok=true
    for i in $(seq 1 10); do
        touch $MOUNT_POINT_0/edge_rapid.txt || { ok=false; break; }
        rm -f $MOUNT_POINT_0/edge_rapid.txt || { ok=false; break; }
    done
    $ok && [ ! -f $MOUNT_POINT_0/edge_rapid.txt ]
'

run_test "Cross-node: file visible immediately after create" '
    touch $MOUNT_POINT_0/edge_imm.txt && \
    [ -f $MOUNT_POINT_1/edge_imm.txt ] && \
    rm -f $MOUNT_POINT_0/edge_imm.txt
'

run_test "Cross-node: file gone immediately after delete" '
    touch $MOUNT_POINT_0/edge_del.txt && \
    [ -f $MOUNT_POINT_1/edge_del.txt ] && \
    rm -f $MOUNT_POINT_0/edge_del.txt && \
    sleep 0.2 && \
    [ ! -f $MOUNT_POINT_1/edge_del.txt ]
'

run_test "Node 1 create + Node 0 delete-recreate" '
    touch $MOUNT_POINT_1/edge_cross_recycle.txt && \
    [ -f $MOUNT_POINT_0/edge_cross_recycle.txt ] && \
    rm -f $MOUNT_POINT_1/edge_cross_recycle.txt && \
    sleep 0.2 && \
    touch $MOUNT_POINT_0/edge_cross_recycle.txt && \
    [ -f $MOUNT_POINT_1/edge_cross_recycle.txt ] && \
    rm -f $MOUNT_POINT_0/edge_cross_recycle.txt
'

echo ""

# ============================================================================
# Section 16: mmap Data Integrity (C test)
# ============================================================================
echo -e "${YELLOW}=== Section 16: mmap Data Integrity (C test) ===${NC}"

MMAP_TEST_SRC="$SCRIPT_DIR/test_mmap.c"
MMAP_TEST_BIN="$SCRIPT_DIR/test_mmap"

if [ -f "$MMAP_TEST_SRC" ]; then
    run_test "Compile test_mmap.c" '
        gcc -Wall -Wextra -I"$SCRIPT_DIR/../include" -o "$MMAP_TEST_BIN" "$MMAP_TEST_SRC" 2>&1
    '

    if [ -x "$MMAP_TEST_BIN" ]; then
        # Single-node mmap: write pattern, read back, WORM checks
        run_test "mmap: single-node data integrity (Node 0)" '
            "$MMAP_TEST_BIN" "$MOUNT_POINT_0" 128 2>&1
        '

        # Cross-node mmap: owner writes, peer reads and verifies
        run_test "mmap: cross-node visibility (Node 0 -> Node 1)" '
            "$MMAP_TEST_BIN" "$MOUNT_POINT_0" "$MOUNT_POINT_1" "$NODE_ID_1" 128 2>&1
        '
    else
        echo -e "  ${YELLOW}SKIP${NC} (compilation failed)"
    fi
else
    echo -e "  ${YELLOW}SKIP${NC} (test_mmap.c not found)"
fi

echo ""

# ============================================================================
# Section 17: Large Region mmap Tests
# ============================================================================
echo -e "${YELLOW}=== Section 17: Large Region mmap Tests ===${NC}"

if [ -x "$MMAP_TEST_BIN" ]; then
    # 1GB region - tests device_dax alignment with larger mapping
    run_test "mmap: 1GB region (single-node)" '
        "$MMAP_TEST_BIN" "$MOUNT_POINT_0" 1024 2>&1
    '

    run_test "mmap: 1GB region cross-node (Node 0 -> Node 1)" '
        "$MMAP_TEST_BIN" "$MOUNT_POINT_0" "$MOUNT_POINT_1" "$NODE_ID_1" 1024 2>&1
    '

    # 4GB region - triggered device_dax unaligned VMA bug
    run_test "mmap: 4GB region (single-node)" '
        "$MMAP_TEST_BIN" "$MOUNT_POINT_0" 4096 2>&1
    '

    run_test "mmap: 4GB region cross-node (Node 0 -> Node 1)" '
        "$MMAP_TEST_BIN" "$MOUNT_POINT_0" "$MOUNT_POINT_1" "$NODE_ID_1" 4096 2>&1
    '
else
    echo -e "  ${YELLOW}SKIP${NC} (test_mmap not compiled — Section 16 may have failed)"
fi

echo ""

# ============================================================================
# Section 18: mmap Permission & cudaHostRegister Tests
# ============================================================================
echo -e "${YELLOW}=== Section 18: mmap Permission & cudaHostRegister ===${NC}"

CUDA_TEST_SRC="$SCRIPT_DIR/test_mmap_cuda.c"
CUDA_TEST_BIN="$SCRIPT_DIR/test_mmap_cuda"

if [ -f "$CUDA_TEST_SRC" ]; then
    run_test "Compile test_mmap_cuda.c" '
        gcc -Wall -Wextra -I"$SCRIPT_DIR/../include" -o "$CUDA_TEST_BIN" "$CUDA_TEST_SRC" -ldl 2>&1
    '

    if [ -x "$CUDA_TEST_BIN" ]; then
        clean_all
        run_test "mmap perm & CUDA: owner vs reader" '
            "$CUDA_TEST_BIN" "$MOUNT_POINT_0" "$MOUNT_POINT_1" "$NODE_ID_1" 2>&1
        '
    fi
else
    echo -e "  ${YELLOW}SKIP${NC} (test_mmap_cuda.c not found)"
fi

echo ""

# ============================================================================
# Section 19: Hot/Cold Entry Split (63-char name boundary)
# ============================================================================
echo -e "${YELLOW}=== Section 19: Hot/Cold Entry Split ===${NC}"
clean_all

run_test "Max-length filename (63 chars) create + lookup" '
    LONG_NAME="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" && \
    touch "$MOUNT_POINT_0/$LONG_NAME" && \
    [ -f "$MOUNT_POINT_0/$LONG_NAME" ] && \
    [ -f "$MOUNT_POINT_1/$LONG_NAME" ] && \
    rm -f "$MOUNT_POINT_0/$LONG_NAME"
'

run_test "63-char name cross-node visibility" '
    NAME63="BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB" && \
    touch "$MOUNT_POINT_0/$NAME63" && \
    ls "$MOUNT_POINT_1/" | grep -q "$NAME63" && \
    rm -f "$MOUNT_POINT_0/$NAME63"
'

run_test "Overlong filename (64 chars) rejected" '
    NAME64="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" && \
    ! touch "$MOUNT_POINT_0/$NAME64" 2>/dev/null
'

run_test "Similar names differ only in last char" '
    touch "$MOUNT_POINT_0/test_name_diff_A" && \
    touch "$MOUNT_POINT_0/test_name_diff_B" && \
    [ -f "$MOUNT_POINT_1/test_name_diff_A" ] && \
    [ -f "$MOUNT_POINT_1/test_name_diff_B" ] && \
    rm -f "$MOUNT_POINT_0/test_name_diff_A" "$MOUNT_POINT_0/test_name_diff_B"
'

echo ""

# ============================================================================
# Section 20: RAT Exhaustion (ENOSPC)
# ============================================================================
echo -e "${YELLOW}=== Section 20: RAT Exhaustion (ENOSPC) ===${NC}"
clean_all

run_test "Create files until ENOSPC, then cleanup" '
    created=0
    for i in $(seq 1 260); do
        if touch "$MOUNT_POINT_0/enospc_${i}.txt" 2>/dev/null; then
            created=$((created + 1))
        else
            break
        fi
    done
    # Should have hit the limit (RAT has 256 entries, some used by system)
    [ "$created" -lt 260 ] && \
    # Verify ENOSPC: next create should fail
    ! touch "$MOUNT_POINT_0/enospc_overflow.txt" 2>/dev/null && \
    # Cleanup: delete all created files
    for i in $(seq 1 $created); do
        rm -f "$MOUNT_POINT_0/enospc_${i}.txt" 2>/dev/null
    done && \
    # After cleanup, creation should work again
    touch "$MOUNT_POINT_0/enospc_after.txt" && \
    rm -f "$MOUNT_POINT_0/enospc_after.txt"
'

echo ""

# ============================================================================
# Section 21: Tombstone GC Sweep Verification
# ============================================================================
echo -e "${YELLOW}=== Section 21: Tombstone GC Sweep ===${NC}"

if [ -w "/sys/fs/${MODULE_NAME}/debug/gc_trigger" ] 2>/dev/null; then
    clean_all

    run_test "Create+delete 50 files, GC sweep reclaims slots" '
        for i in $(seq 1 50); do touch "$MOUNT_POINT_0/gc_sweep_${i}.txt"; done && \
        for i in $(seq 1 50); do rm -f "$MOUNT_POINT_0/gc_sweep_${i}.txt"; done && \
        echo 1 > /sys/fs/${MODULE_NAME}/debug/gc_trigger && \
        sleep 1 && \
        # After GC, should be able to create files (slots reclaimed)
        for i in $(seq 1 50); do touch "$MOUNT_POINT_0/gc_verify_${i}.txt"; done && \
        count=$(file_count "$MOUNT_POINT_0") && \
        [ "$count" -eq 50 ] && \
        for i in $(seq 1 50); do rm -f "$MOUNT_POINT_0/gc_verify_${i}.txt"; done
    '
else
    echo -e "  ${YELLOW}SKIP${NC} (requires root for gc_trigger)"
fi

echo ""

# ============================================================================
# Section 22: Negative / Error Path Tests
# ============================================================================
echo -e "${YELLOW}=== Section 22: Negative / Error Path ===${NC}"
clean_all

run_test "Open non-existent file fails with ENOENT" '
    ! cat "$MOUNT_POINT_0/nonexistent_file_xyz" 2>/dev/null
'

run_test "Delete non-existent file fails" '
    ! rm "$MOUNT_POINT_0/nonexistent_del_xyz" 2>/dev/null
'

run_test "write() on MARUFS file rejected (WORM)" '
    touch "$MOUNT_POINT_0/worm_test.txt" && \
    ! bash -c "echo data > $MOUNT_POINT_0/worm_test.txt" 2>/dev/null; \
    rm -f "$MOUNT_POINT_0/worm_test.txt"
'

run_test "Cross-node delete of other node file rejected" '
    touch "$MOUNT_POINT_0/xn_perm.txt" && \
    ! rm "$MOUNT_POINT_1/xn_perm.txt" 2>/dev/null && \
    [ -f "$MOUNT_POINT_0/xn_perm.txt" ] && \
    rm -f "$MOUNT_POINT_0/xn_perm.txt"
'

echo ""

# ============================================================================
# Section 23: CHOWN Race Condition Tests
# ============================================================================
echo -e "${YELLOW}=== Section 23: CHOWN Race Condition ===${NC}"

CHOWN_RACE_BIN="$SCRIPT_DIR/test_chown_race"
if [ -x "$CHOWN_RACE_BIN" ]; then
    run_test "CHOWN race condition tests" '
        "$CHOWN_RACE_BIN" "$MOUNT_POINT_0" "$MOUNT_POINT_1" "$NODE_ID_1" 128 2>&1
    '
else
    echo -e "  ${YELLOW}SKIP${NC} (test_chown_race not compiled)"
fi

echo ""

# Resume GC after tests
if [ "$GC_PAUSED" = true ] && [ -f "$GC_PAUSE_FILE" ]; then
    bash -c "echo 0 > '$GC_PAUSE_FILE'" 2>/dev/null || \
        printf '0' | sudo -n tee "$GC_PAUSE_FILE" > /dev/null 2>&1 || true
    echo -e "${CYAN}[INFO]${NC} GC resumed"
fi

# ============================================================================
# Section 24: Name-Ref Benchmark (C test)
# ============================================================================
echo -e "${YELLOW}=== Section 24: Name-Ref Benchmark ===${NC}"

BENCH_SRC="$SCRIPT_DIR/bench_name_ref.c"
BENCH_BIN="$SCRIPT_DIR/bench_name_ref"

if [ -f "$BENCH_SRC" ]; then
    run_test "Compile bench_name_ref.c" '
        gcc -O2 -Wall -Wextra -Wpedantic -I"$SCRIPT_DIR/../include" -o "$BENCH_BIN" "$BENCH_SRC" 2>&1
    '

    if [ -x "$BENCH_BIN" ]; then
        run_test "bench_name_ref: basic (Node 0)" '
            "$BENCH_BIN" "$MOUNT_POINT_0" -n 100 2>&1
        '

        run_test "bench_name_ref: stress (Node 0, prefill + shuffle)" '
            "$BENCH_BIN" "$MOUNT_POINT_0" -n 100 --prefill 10000 --shuffle 2>&1
        '
    else
        echo -e "  ${YELLOW}SKIP${NC} (compilation failed)"
    fi
else
    echo -e "  ${YELLOW}SKIP${NC} (bench_name_ref.c not found)"
fi

echo ""

# ============================================================================
# Section 25: mmap on Untruncated Region (ENODATA)
# ============================================================================
echo -e "${YELLOW}=== Section 25: mmap on Untruncated Region ===${NC}"

NOTRUNC_BIN="$SCRIPT_DIR/test_mmap_notrunc"
if [ -x "$NOTRUNC_BIN" ]; then
    run_test "mmap on untruncated region returns ENODATA" '
        "$NOTRUNC_BIN" "$MOUNT_POINT_0" 2>&1
    '
else
    echo -e "  ${YELLOW}SKIP${NC} (test_mmap_notrunc not compiled)"
fi

echo ""

# ============================================================================
# Section 26: Negative / Error Path (C test)
# ============================================================================
echo -e "${YELLOW}=== Section 26: Negative Error Path (C test) ===${NC}"

NEGATIVE_BIN="$SCRIPT_DIR/test_negative"
if [ -x "$NEGATIVE_BIN" ]; then
    run_test "Negative tests: single-node" '
        "$NEGATIVE_BIN" "$MOUNT_POINT_0" 2>&1
    '

    run_test "Negative tests: multi-node (GRANT escalation)" '
        "$NEGATIVE_BIN" "$MOUNT_POINT_0" "$MOUNT_POINT_1" "$NODE_ID_1" 2>&1
    '
else
    echo -e "  ${YELLOW}SKIP${NC} (test_negative not compiled)"
fi

echo ""

# ============================================================================
# Section 27: NRHT CAS Race Test
# ============================================================================
echo -e "${YELLOW}=== Section 27: NRHT CAS Race ===${NC}"

NRHT_RACE_BIN="$SCRIPT_DIR/test_nrht_race"
if [ -x "$NRHT_RACE_BIN" ]; then
    run_test "NRHT concurrent insert/delete race ($NUM_MOUNTS mounts)" '
        "$NRHT_RACE_BIN" "${MOUNT_POINTS[@]}" 2>&1
    '
else
    echo -e "  ${YELLOW}SKIP${NC} (test_nrht_race not compiled)"
fi

echo ""

# ============================================================================
# Section 28: GC Dead Delegation Sweep
# ============================================================================
echo -e "${YELLOW}=== Section 28: GC Dead Delegation Sweep ===${NC}"

GC_DELEG_BIN="$SCRIPT_DIR/test_gc_deleg"
if [ -x "$GC_DELEG_BIN" ] && [ -d "$SYSFS_BASE" ]; then
    run_test "GC dead delegation sweep" '
        "$GC_DELEG_BIN" "$MOUNT_POINT_0" "$MOUNT_POINT_1" "$NODE_ID_1" "$SYSFS_BASE" 2>&1
    '
else
    echo -e "  ${YELLOW}SKIP${NC} (test_gc_deleg not compiled or sysfs unavailable)"
fi

echo ""

# ============================================================================
# Section 29: PID Reuse birth_time Security
# ============================================================================
echo -e "${YELLOW}=== Section 29: PID Reuse birth_time ===${NC}"

PID_REUSE_BIN="$SCRIPT_DIR/test_pid_reuse"
if [ -x "$PID_REUSE_BIN" ] && [ "$(id -u)" -eq 0 ]; then
    run_test "PID reuse: birth_time prevents stale delegation" '
        "$PID_REUSE_BIN" "$MOUNT_POINT_0" "$MOUNT_POINT_1" "$NODE_ID_1" 2>&1
    '
else
    echo -e "  ${YELLOW}SKIP${NC} (test_pid_reuse not compiled or not root)"
fi

echo ""

# ============================================================================
# Section 30: ME Crash Detection (T1-T7)
# ============================================================================
# Delegates to standalone test_me_crash.sh. Exercises poll-freeze fault
# injection: takeover timing, false-positive suppression, late-grant /
# ETIMEDOUT paths, concurrent takeover race, post-takeover sanity.
#
# Runs last because:
#   - manipulates dmesg ring (uses dmesg -C)
#   - T1 saturates CPU with stress-ng (soft dep — self-skips if absent)
#   - T5 needs ≥3 mounts (self-skips if /mnt/marufs3 absent)
#   - ~50s total runtime
echo -e "${YELLOW}=== Section 30: ME Crash Detection ===${NC}"

ME_CRASH_SCRIPT="$SCRIPT_DIR/test_me_crash.sh"
if [ -x "$ME_CRASH_SCRIPT" ] \
    && [ -w "/sys/fs/marufs/debug/me_freeze_heartbeat" ] \
    && [ "$(id -u)" -eq 0 ]; then
    run_test "ME crash detection (T1-T7, ~50s)" '
        bash "$ME_CRASH_SCRIPT" 2>&1
    '
else
    echo -e "  ${YELLOW}SKIP${NC} (test_me_crash.sh missing, fault-inject sysfs absent, or not root)"
fi

echo ""

# ============================================================================
# Section 31: Bootstrap Auto-Mount
# ============================================================================
# Verifies auto-mount assigns unique node_ids, exactly one formatter,
# and that bootstrap slot table is consistent after mount.
# Requires /sys/fs/marufs/debug/bootstrap_dump sysfs entry.
echo -e "${YELLOW}=== Section 31: Bootstrap Auto-Mount ===${NC}"

BOOTSTRAP_DUMP_SYSFS="/sys/fs/${MODULE_NAME}/debug/bootstrap_dump"

run_test "bootstrap_dump sysfs accessible" '
    [ -r "'"$BOOTSTRAP_DUMP_SYSFS"'" ] 2>&1
'

run_test "All mounted nodes have CLAIMED bootstrap slots" '
    dump=$(cat "'"$BOOTSTRAP_DUMP_SYSFS"'" 2>&1) && \
    claimed_count=$(echo "$dump" | grep -c "status=CLAIMED") && \
    [ "$claimed_count" -ge 1 ] && \
    echo "CLAIMED slots: $claimed_count" 2>&1
'

run_test "No FORMATTING slots remain after mount" '
    dump=$(cat "'"$BOOTSTRAP_DUMP_SYSFS"'" 2>&1) && \
    stuck=$(echo "$dump" | grep -c "status=FORMATTING" || true) && \
    [ "${stuck:-0}" -eq 0 ] && \
    echo "No stuck FORMATTING slots" 2>&1
'

# Each mount marks its own slot with "<mine>". Extract those lines to get
# per-mount owned slots, then verify node_ids are unique across mounts.
run_test "Each mount owns a unique bootstrap slot" '
    dump=$(cat "'"$BOOTSTRAP_DUMP_SYSFS"'" 2>&1) && \
    mine_lines=$(echo "$dump" | grep "<mine>") && \
    node_ids=$(echo "$mine_lines" | grep -oE "node_id=[0-9]+" | sort) && \
    uniq_ids=$(echo "$node_ids" | sort -u) && \
    [ -n "$node_ids" ] && \
    [ "$node_ids" = "$uniq_ids" ] && \
    echo "Owned slots: $(echo "$mine_lines" | wc -l)" 2>&1
'

echo ""

# ============================================================================
# Section 32: Bootstrap Chaos (via test_bootstrap_chaos.sh)
# ============================================================================
# Delegates to test_bootstrap_chaos.sh:
#   T1 stuck-formatter recovery, T2 concurrent mount race, T3 slot reuse.
echo -e "${YELLOW}=== Section 32: Bootstrap Chaos ===${NC}"

BOOTSTRAP_CHAOS_SCRIPT="$SCRIPT_DIR/test_bootstrap_chaos.sh"
BOOTSTRAP_SETUP_SCRIPT="$SCRIPT_DIR/setup_local_multinode.sh"
# Chaos needs exclusive module ownership (rmmod/insmod cycles).
# If marufs mounts are active, auto-teardown before running chaos.
# Note: chaos leaves the module unloaded — re-run setup_local_multinode.sh
# manually if subsequent tests need a mounted filesystem.
if [ -x "$BOOTSTRAP_CHAOS_SCRIPT" ] && [ "$(id -u)" -eq 0 ]; then
    chaos_active_mounts=$(mount | grep -c "type ${MODULE_NAME} " || true)
    if [ "${chaos_active_mounts:-0}" -gt 0 ]; then
        echo -e "  ${YELLOW}NOTE${NC}: ${chaos_active_mounts} marufs mounts active — auto-teardown before chaos"
        "$BOOTSTRAP_SETUP_SCRIPT" --teardown >/dev/null 2>&1 || \
            echo -e "  ${YELLOW}WARN${NC}: teardown script failed; chaos may abort"
    fi
    run_test "Bootstrap chaos tests (T1-T3, ~30s)" '
        bash "'"$BOOTSTRAP_CHAOS_SCRIPT"'" 2>&1
    '
    echo -e "  ${YELLOW}NOTE${NC}: chaos finished — module unloaded. Re-run setup if needed."
else
    echo -e "  ${YELLOW}SKIP${NC} (test_bootstrap_chaos.sh missing or not root)"
fi

echo ""

# ============================================================================
# Summary
# ============================================================================
echo "============================================"
echo "  MARUFS Local Multi-Node Test Results"
echo "============================================"
echo -e "  Mode:   $MODE_LABEL"
echo -e "  PASSED: ${GREEN}${PASS}${NC} / ${TOTAL}"
echo -e "  FAILED: ${RED}${FAIL}${NC}"
echo "============================================"

# ============================================================================
# Cleanup
# ============================================================================
if [ "$DO_CLEANUP" = true ]; then
    echo ""
    echo -e "${CYAN}[INFO]${NC} Cleaning up..."

    # Unmount all nodes in reverse order
    for ((_ci = ${#MOUNT_POINTS[@]} - 1; _ci >= 0; _ci--)); do
        _cmp="${MOUNT_POINTS[_ci]}"
        if mount | grep -q "$_cmp type ${MODULE_NAME}"; then
            umount "$_cmp" 2>/dev/null \
                && echo -e "  ${GREEN}✓${NC} Unmounted $_cmp" \
                || echo -e "  ${YELLOW}!${NC} Failed to unmount $_cmp"
        fi
    done

    # Unload MARUFS module
    if lsmod | grep -q "^${MODULE_NAME} "; then
        rmmod "${MODULE_NAME}" 2>/dev/null && echo -e "  ${GREEN}✓${NC} Unloaded ${MODULE_NAME} module" || echo -e "  ${YELLOW}!${NC} Failed to unload module"
    fi

    # Note: daxheap module is NOT unloaded here — managed externally

    echo -e "${CYAN}[INFO]${NC} Cleanup complete"
fi

echo ""

if [ "$FAIL" -eq 0 ]; then
    echo -e "${GREEN}All tests passed.${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed.${NC}"
    exit 1
fi
