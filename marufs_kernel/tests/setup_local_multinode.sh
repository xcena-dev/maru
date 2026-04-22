#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# setup_local_multinode.sh - MARUFS Local Multi-Node Environment Setup
#
# Builds MARUFS, formats a DEV_DAX device with a shared region pool,
# and mounts it twice with different node_ids for multi-node testing.
#
# Usage:
#   sudo ./setup_local_multinode.sh                          # Default: /dev/dax6.0
#   sudo ./setup_local_multinode.sh --device /dev/dax0.0     # Custom device
#   sudo ./setup_local_multinode.sh --daxheap                # DAXHEAP mode (WC mmap)
#   sudo ./setup_local_multinode.sh --teardown               # Unmount + unload
#   sudo ./setup_local_multinode.sh --status                 # Show current state
#
# After setup:
#   Node 0: /mnt/marufs   (read-write via shared pool allocation)
#   Node 1: /mnt/marufs2  (read-write via shared pool allocation)
#   Both nodes can read all files and write to dynamically allocated regions.

set -euo pipefail

# --- Configuration (override via environment or flags) ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

MODULE_NAME="${MARUFS_MODULE_NAME:-marufs}"  # Filesystem type and module name
DAX_DEVICE="${MARUFS_DAX_DEVICE:-/dev/dax6.0}"
NUM_MOUNTS="${MARUFS_NUM_MOUNTS:-2}"          # Number of simulated nodes (1..8)
# Per-index defaults: MARUFS_MOUNT_<i>=... / MARUFS_NODE_<i>=... override below.
# Mount 0 uses /mnt/<module>, mount i>0 uses /mnt/<module>(i+1). Node IDs default to i+1.
MOUNT_POINT_0="${MARUFS_MOUNT_0:-/mnt/${MODULE_NAME}}"
MOUNT_POINT_1="${MARUFS_MOUNT_1:-/mnt/${MODULE_NAME}2}"
NODE_ID_0="${MARUFS_NODE_0:-1}"
NODE_ID_1="${MARUFS_NODE_1:-2}"
BUILD_DIR="${MARUFS_BUILD_DIR:-$PROJECT_DIR/build}"
NUM_SHARDS="${MARUFS_NUM_SHARDS:-64}"
NUM_REGIONS="${MARUFS_NUM_REGIONS:-4}"
REGION_OWNERS="${MARUFS_REGION_OWNERS:-}"  # auto-set below if empty
CHMOD_MODE="${MARUFS_CHMOD:-1777}"         # permissions for mount points
ME_STRATEGY="${MARUFS_ME_STRATEGY:-request}" # ME strategy: order or request

# DAXHEAP configuration
USE_DAXHEAP="${MARUFS_DAXHEAP:-false}"
DAXHEAP_DIR="${MARUFS_DAXHEAP_DIR:-}"
DAXHEAP_MODULE="${DAXHEAP_DIR}/kernel/core/daxheap.ko"
DAXHEAP_SIZE="${MARUFS_DAXHEAP_SIZE:-192G}"  # Allocation size from daxheap buffer

# Derived paths
MODULE_PATH="$BUILD_DIR/${MODULE_NAME}.ko"

# --- Colors ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()    { echo -e "${CYAN}[INFO]${NC} [$MODULE_NAME] $1"; }
log_success() { echo -e "${GREEN}[ OK ]${NC} [$MODULE_NAME] $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} [$MODULE_NAME] $1"; }
log_error()   { echo -e "${RED}[ERR ]${NC} [$MODULE_NAME] $1"; }

# --- Helper: get device size in bytes from sysfs ---
get_dax_size_bytes() {
    local dev_name
    dev_name=$(basename "$DAX_DEVICE")
    local size_file="/sys/bus/dax/devices/${dev_name}/size"
    if [ -f "$size_file" ]; then
        cat "$size_file"
    else
        echo "0"
    fi
}

# --- Argument Parsing ---
ACTION="setup"
SKIP_BUILD=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --device)       DAX_DEVICE="$2"; shift 2 ;;
        --mount-0)      MOUNT_POINT_0="$2"; shift 2 ;;
        --mount-1)      MOUNT_POINT_1="$2"; shift 2 ;;
        --node-0)       NODE_ID_0="$2"; shift 2 ;;
        --node-1)       NODE_ID_1="$2"; shift 2 ;;
        --num-mounts)   NUM_MOUNTS="$2"; shift 2 ;;
        --num-regions)  NUM_REGIONS="$2"; shift 2 ;;
        --num-shards)   NUM_SHARDS="$2"; shift 2 ;;
        --daxheap)      USE_DAXHEAP=true; shift ;;
        --daxheap-dir)  DAXHEAP_DIR="$2"; DAXHEAP_MODULE="${DAXHEAP_DIR}/kernel/core/daxheap.ko"; shift 2 ;;
        --daxheap-size) DAXHEAP_SIZE="$2"; shift 2 ;;
        --skip-build)   SKIP_BUILD=true; shift ;;
        --me-strategy)  ME_STRATEGY="$2"; shift 2 ;;
        --teardown)     ACTION="teardown"; shift ;;
        --status)       ACTION="status"; shift ;;
        --help|-h)
            cat <<'EOF'
Usage: sudo ./setup_local_multinode.sh [OPTIONS]

Actions:
  (default)         Build, format, mount (full setup)
  --teardown        Unmount all and unload module
  --status          Show current state

Options:
  --device DEV      DAX device (default: /dev/dax6.0)
  --daxheap         Use DAXHEAP mode (WC mmap for GPU high-bandwidth)
  --daxheap-dir DIR daxheap source directory (required when --daxheap)
  --daxheap-size SZ daxheap allocation size (default: 100G)
  --num-mounts N    Number of mount points / simulated nodes, 1..8 (default: 2)
  --mount-0 PATH    Node 0 mount point (default: /mnt/marufs)
  --mount-1 PATH    Node 1 mount point (default: /mnt/marufs2)
                    (use MARUFS_MOUNT_<i>=... env var for indices >= 2)
  --node-0 ID       Node 0 ID (default: 1)
  --node-1 ID       Node 1 ID (default: 2)
                    (use MARUFS_NODE_<i>=... env var for indices >= 2)
  --num-regions N   Number of regions (default: 4)
  --num-shards N    Number of shards (default: 64)
  --skip-build      Skip build step (use existing binaries)
  --me-strategy S   ME strategy: order (default) or request

Environment:
  MARUFS_DAX_DEVICE, MARUFS_MOUNT_0, MARUFS_MOUNT_1, MARUFS_NODE_0, MARUFS_NODE_1,
  MARUFS_BUILD_DIR, MARUFS_NUM_SHARDS, MARUFS_NUM_REGIONS, MARUFS_CHMOD,
  MARUFS_DAXHEAP (true/false), MARUFS_DAXHEAP_DIR

Examples:
  sudo ./setup_local_multinode.sh
  sudo ./setup_local_multinode.sh --daxheap
  sudo ./setup_local_multinode.sh --device /dev/dax0.0 --num-regions 2
  sudo ./setup_local_multinode.sh --teardown
EOF
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# --- Validate + expand NUM_MOUNTS into MOUNT_POINTS[] / NODE_IDS[] ---
if ! [[ "$NUM_MOUNTS" =~ ^[1-8]$ ]]; then
    log_error "--num-mounts must be 1..8 (got: $NUM_MOUNTS)"
    exit 1
fi

# Default mount path for index i: /mnt/<module>  (i=0)  |  /mnt/<module>(i+1) (i>0)
default_mount_for_idx() {
    local i=$1
    if [ "$i" -eq 0 ]; then
        echo "/mnt/${MODULE_NAME}"
    else
        echo "/mnt/${MODULE_NAME}$((i + 1))"
    fi
}

MOUNT_POINTS=()
NODE_IDS=()
for ((i = 0; i < NUM_MOUNTS; i++)); do
    # MARUFS_MOUNT_<i> / MARUFS_NODE_<i> env overrides, else defaults.
    m_var="MARUFS_MOUNT_$i"
    n_var="MARUFS_NODE_$i"
    default_mp=$(default_mount_for_idx "$i")
    default_nid=$((i + 1))

    # Preserve --mount-0/--mount-1/--node-0/--node-1 CLI flags for back-compat.
    if [ "$i" -eq 0 ]; then default_mp="$MOUNT_POINT_0"; default_nid="$NODE_ID_0"; fi
    if [ "$i" -eq 1 ]; then default_mp="$MOUNT_POINT_1"; default_nid="$NODE_ID_1"; fi

    MOUNT_POINTS[i]="${!m_var:-$default_mp}"
    NODE_IDS[i]="${!n_var:-$default_nid}"
done

# Note: REGION_OWNERS variable is legacy and no longer used
# Regions are now dynamically allocated from a shared pool
if [ -z "$REGION_OWNERS" ]; then
    REGION_OWNERS=$(IFS=,; echo "${NODE_IDS[*]}")
fi

# ============================================================================
# Status
# ============================================================================
do_status() {
    echo "============================================"
    echo "  ${MODULE_NAME} Environment Status"
    echo "============================================"

    # daxheap module
    echo -n "  daxheap: "
    if grep -q "^daxheap " /proc/modules; then
        echo -e "${GREEN}loaded${NC}"
    else
        echo -e "${YELLOW}not loaded${NC}"
    fi

    # Module
    echo -n "  Module:  "
    if grep -q "^${MODULE_NAME} " /proc/modules; then
        echo -e "${GREEN}loaded${NC}"
        local node_id_file="/sys/fs/${MODULE_NAME}/node_id"
        [ -f "$node_id_file" ] && echo "           node_id=$(cat $node_id_file)"
    else
        echo -e "${YELLOW}not loaded${NC}"
    fi

    # Mounts
    for ((i = 0; i < NUM_MOUNTS; i++)); do
        local mp="${MOUNT_POINTS[i]}"
        echo -n "  Mount $i: "
        if mount | grep -q "$mp type ${MODULE_NAME}"; then
            local count=$(ls -1 "$mp" 2>/dev/null | wc -l)
            echo -e "${GREEN}$mp${NC} ($count files)"
        else
            echo -e "${YELLOW}not mounted${NC} ($mp)"
        fi
    done

    # Device — detect from mount if available, fallback to DAX_DEVICE
    local actual_dev="$DAX_DEVICE"
    local mounted_opts
    mounted_opts=$(mount | grep "type ${MODULE_NAME}" | head -1 | sed -n 's/.*daxdev=\([^,)]*\).*/\1/p')
    if [ -n "$mounted_opts" ]; then
        actual_dev="$mounted_opts"
    fi
    echo -n "  Device:  "
    if [ -e "$actual_dev" ]; then
        echo -e "${GREEN}$actual_dev${NC}"
        local size_file="/sys/bus/dax/devices/$(basename $actual_dev)/size"
        if [ -f "$size_file" ]; then
            local size_bytes=$(cat "$size_file")
            local size_gb=$(awk "BEGIN {printf \"%.1f\", $size_bytes/1024/1024/1024}")
            echo "           size=${size_gb} GB"
        fi
    else
        echo -e "${RED}$actual_dev not found${NC}"
    fi

    # Filesystem stats (based on mount 0)
    if mount | grep -q "${MOUNT_POINTS[0]} type ${MODULE_NAME}"; then
        echo ""
        echo "  Filesystem:"
        df -h "${MOUNT_POINTS[0]}" 2>/dev/null | tail -1 | awk '{printf "    Size: %s  Used: %s  Avail: %s  Use%%: %s\n", $2, $3, $4, $5}'
    fi

    echo "============================================"
}

# ============================================================================
# Teardown
# ============================================================================
do_teardown() {
    echo "============================================"
    echo "  ${MODULE_NAME} Environment Teardown"
    echo "============================================"

    # Delegate unmount + unload to uninstall.sh
    MARUFS_MODULE_NAME="$MODULE_NAME" "$PROJECT_DIR/uninstall.sh" --force

    # Note: daxheap module is NOT unloaded here — managed externally

    echo ""
    log_success "Teardown complete"
}

# ============================================================================
# Setup
# ============================================================================
do_setup() {
    local mode_label="DEV_DAX"
    if [ "$USE_DAXHEAP" = true ]; then
        mode_label="DAXHEAP (WC mmap)"
    fi

    echo "============================================"
    echo "  ${MODULE_NAME} Local Multi-Node Setup"
    echo "============================================"
    echo "  Mode:         $mode_label"
    echo "  Device:       $DAX_DEVICE"
    echo "  Mounts:       $NUM_MOUNTS"
    for ((i = 0; i < NUM_MOUNTS; i++)); do
        printf "    Node %d:     %s (node_id=%s)\n" \
            "$i" "${MOUNT_POINTS[i]}" "${NODE_IDS[i]}"
    done
    echo "  Regions:      $NUM_REGIONS (shared pool with dynamic allocation)"
    echo "  Shards:       $NUM_SHARDS"
    if [ "$USE_DAXHEAP" = true ]; then
        echo "  daxheap dir:  $DAXHEAP_DIR"
    fi
    echo "============================================"
    echo ""

    # --- Pre-flight checks ---
    log_info "Pre-flight checks..."

    if [ "$(id -u)" -ne 0 ]; then
        log_error "Must run as root (sudo)"
        exit 1
    fi

    if [ "$USE_DAXHEAP" = false ] && [ ! -e "$DAX_DEVICE" ]; then
        log_error "Device not found: $DAX_DEVICE"
        exit 1
    fi

    if [ "$USE_DAXHEAP" = true ] && [ ! -f "$DAXHEAP_MODULE" ]; then
        log_error "daxheap module not found: $DAXHEAP_MODULE"
        log_error "Build daxheap first: cd $DAXHEAP_DIR/kernel/core && make"
        exit 1
    fi

    log_success "Pre-flight OK"
    echo ""

    # --- Step 1-3: Build + Clean + Load via install.sh ---
    log_info "Step 1-3/5: Build, clean, and load module via install.sh..."

    local install_args="--node-id $NODE_ID_0 --build-dir $BUILD_DIR --module-name $MODULE_NAME"
    if [ "$SKIP_BUILD" = true ]; then
        install_args="$install_args --skip-build"
    fi
    if [ "$USE_DAXHEAP" = true ]; then
        install_args="$install_args --daxheap --daxheap-dir $DAXHEAP_DIR"
    fi

    # Clean existing state first via uninstall.sh
    MARUFS_MODULE_NAME="$MODULE_NAME" "$PROJECT_DIR/uninstall.sh" --force 2>/dev/null || true

    # Load daxheap before MARUFS if needed
    if [ "$USE_DAXHEAP" = true ]; then
        if grep -q "^daxheap " /proc/modules; then
            log_info "  daxheap already loaded, skipping insmod"
        else
            log_info "  Loading daxheap (dax_device=$DAX_DEVICE, host_id=$NODE_ID_0)..."
            insmod "$DAXHEAP_MODULE" dax_device="$DAX_DEVICE" host_id="$NODE_ID_0"
            sleep 1
            if ! grep -q "^daxheap " /proc/modules; then
                log_error "daxheap module load failed"
                exit 1
            fi
            log_success "daxheap loaded"
        fi
    fi

    # Build + load MARUFS module (no mount — we do dual mount below)
    "$PROJECT_DIR/install.sh" $install_args
    if [ $? -ne 0 ]; then
        log_error "install.sh failed"
        exit 1
    fi

    log_success "Module ready"

    # --- Step 4: Mount (first mount formats; rest attach) ---
    log_info "Step 4/4: Mounting $NUM_MOUNTS nodes..."

    for ((i = 0; i < NUM_MOUNTS; i++)); do
        mkdir -p "${MOUNT_POINTS[i]}"
    done

    if [ "$USE_DAXHEAP" = true ]; then
        # DAXHEAP: primary allocates the shared buffer, secondaries attach via bufid.
        log_info "  DAXHEAP primary mount (buffer size: $DAXHEAP_SIZE)"
        mount -t "${MODULE_NAME}" \
            -o "daxheap=${DAXHEAP_SIZE},node_id=${NODE_IDS[0]}" \
            none "${MOUNT_POINTS[0]}"
        chmod "$CHMOD_MODE" "${MOUNT_POINTS[0]}"
        log_success "Node ${NODE_IDS[0]} -> ${MOUNT_POINTS[0]} (DAXHEAP primary, ${DAXHEAP_SIZE})"

        # Read buf_id from sysfs (published by primary mount)
        local bufid_file="/sys/fs/${MODULE_NAME}/daxheap_bufid"
        if [ ! -f "$bufid_file" ]; then
            log_error "buf_id sysfs not found: $bufid_file"
            exit 1
        fi
        local bufid
        bufid=$(cat "$bufid_file")
        if [ "$bufid" = "0x0" ] || [ -z "$bufid" ]; then
            log_error "Primary mount did not publish a valid buf_id"
            exit 1
        fi

        for ((i = 1; i < NUM_MOUNTS; i++)); do
            log_info "  DAXHEAP secondary mount $i (bufid=${bufid})"
            mount -t "${MODULE_NAME}" \
                -o "daxheap_bufid=${bufid},node_id=${NODE_IDS[i]}" \
                none "${MOUNT_POINTS[i]}"
            chmod "$CHMOD_MODE" "${MOUNT_POINTS[i]}"
            log_success "Node ${NODE_IDS[i]} -> ${MOUNT_POINTS[i]} (DAXHEAP secondary, bufid=${bufid})"
        done
    else
        # DEV_DAX: first mount formats the device, subsequent mounts attach.
        mount -t "${MODULE_NAME}" \
            -o daxdev="$DAX_DEVICE",node_id="${NODE_IDS[0]}",format,me_strategy="$ME_STRATEGY" \
            none "${MOUNT_POINTS[0]}"
        chmod "$CHMOD_MODE" "${MOUNT_POINTS[0]}"
        log_success "Node ${NODE_IDS[0]} -> ${MOUNT_POINTS[0]} (format)"

        for ((i = 1; i < NUM_MOUNTS; i++)); do
            mount -t "${MODULE_NAME}" \
                -o daxdev="$DAX_DEVICE",node_id="${NODE_IDS[i]}",me_strategy="$ME_STRATEGY" \
                none "${MOUNT_POINTS[i]}"
            chmod "$CHMOD_MODE" "${MOUNT_POINTS[i]}"
            log_success "Node ${NODE_IDS[i]} -> ${MOUNT_POINTS[i]}"
        done
    fi

    # --- Done ---
    echo ""
    echo "============================================"
    echo -e "  ${GREEN}Setup Complete!${NC} ($mode_label)"
    echo "============================================"
    echo ""
    echo "  Quick test:"
    echo "    touch ${MOUNT_POINTS[0]}/hello.txt     # Node ${NODE_IDS[0]} creates"
    echo "    ls ${MOUNT_POINTS[1]:-${MOUNT_POINTS[0]}}/             # Cross-node visibility"
    echo ""
    echo "  Run tests:"
    if [ "$USE_DAXHEAP" = true ]; then
        echo "    ./tests/test_local_multinode.sh --daxheap --skip-setup --no-cleanup"
    else
        echo "    ./tests/test_local_multinode.sh --skip-setup --no-cleanup"
    fi
    echo ""
    echo "  Teardown:"
    echo "    sudo ./tests/setup_local_multinode.sh --teardown"
    echo ""
}

# ============================================================================
# Main
# ============================================================================
case "$ACTION" in
    setup)    do_setup ;;
    teardown) do_teardown ;;
    status)   do_status ;;
esac
