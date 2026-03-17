#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# install.sh - Kernel filesystem module installation script
#
# Builds the kernel module, loads the module,
# and optionally formats + mounts a device.
#
# Usage:
#   sudo ./install.sh                              # Build + load module only
#   sudo ./install.sh --mount /dev/dax0.0          # Build + load + format + mount
#   sudo ./install.sh --daxheap                    # Build with DAXHEAP support
#   sudo ./install.sh --skip-build                 # Load pre-built module
#   sudo ./install.sh --module-name myfs           # Custom module name

set -e

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;36m'
NC='\033[0m' # No Color

# Default values
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_NAME="${MARUFS_MODULE_NAME:-marufs}"
BUILD_DIR="${MARUFS_BUILD_DIR:-$SCRIPT_DIR/build}"
NODE_ID=1
DO_MOUNT=0
DO_FORMAT=0
DEVICE=""
MOUNT_POINT=""
SKIP_BUILD=false
USE_DAXHEAP=false
DAXHEAP_DIR="${MARUFS_DAXHEAP_DIR:-/home/mcpark/daxheap}"

# Helper functions with module name prefix
log_info()    { echo -e "${BLUE}[INFO]${NC} [$MODULE_NAME] $1"; }
log_success() { echo -e "${GREEN}[ OK ]${NC} [$MODULE_NAME] $1"; }
log_error()   { echo -e "${RED}[ERR ]${NC} [$MODULE_NAME] $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} [$MODULE_NAME] $1"; }

# Print usage
usage() {
    cat << EOF
Usage: sudo $0 [OPTIONS]

Options:
  --mount <device>          Mount device (e.g., /dev/dax0.0)
  --format                  Format device on mount (first mount only)
  --node-id <id>            Node ID (default: 1)
  --mount-point <path>      Mount point (default: /mnt/<module-name>)
  --build-dir <path>        Build directory (default: build)
  --module-name <name>      Module name (default: $MODULE_NAME)
  --daxheap                 Build with DAXHEAP support
  --daxheap-dir <path>      DAXHEAP source directory
  --skip-build              Skip build step (use existing binaries)
  -h, --help                Print help

Examples:
  sudo $0                                           # Build + load module
  sudo $0 --mount /dev/dax0.0 --format              # Build + load + format + mount
  sudo $0 --mount /dev/dax0.0                       # Build + load + mount (no format)
  sudo $0 --mount /dev/dax0.0 --node-id 2           # Set node_id=2
  sudo $0 --daxheap --mount /dev/dax0.0             # DAXHEAP mode
  sudo $0 --module-name myfs --mount /dev/dax0.0    # Custom module name
EOF
    exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --mount)        DO_MOUNT=1; DEVICE="$2"; shift 2 ;;
        --format)       DO_FORMAT=1; shift ;;
        --node-id)      NODE_ID="$2"; shift 2 ;;
        --mount-point)  MOUNT_POINT="$2"; shift 2 ;;
        --build-dir)    BUILD_DIR="$2"; shift 2 ;;
        --module-name)  MODULE_NAME="$2"; shift 2 ;;
        --daxheap)      USE_DAXHEAP=true; shift ;;
        --daxheap-dir)  DAXHEAP_DIR="$2"; shift 2 ;;
        --skip-build)   SKIP_BUILD=true; shift ;;
        -h|--help)      usage ;;
        *)              log_error "Unknown option: $1"; usage ;;
    esac
done

# Default mount point based on module name
if [ -z "$MOUNT_POINT" ]; then
    MOUNT_POINT="/mnt/${MODULE_NAME}"
fi

# Validate --mount option
if [ $DO_MOUNT -eq 1 ] && [ -z "$DEVICE" ]; then
    log_error "Device must be specified when using --mount option"
    usage
fi

MODULE_PATH="$BUILD_DIR/${MODULE_NAME}.ko"

echo "============================================"
echo "  ${MODULE_NAME} Installation"
echo "============================================"
echo ""

cd "$SCRIPT_DIR"

# Step 1: Build
if [ "$SKIP_BUILD" = false ]; then
    log_info "Step 1/4: Building..."

    if [ "$USE_DAXHEAP" = true ]; then
        log_info "Building with DAXHEAP_DIR=$DAXHEAP_DIR..."
        make clean > /dev/null 2>&1 || true
        make MODULE_NAME="$MODULE_NAME" DAXHEAP_DIR="$DAXHEAP_DIR" -j128
    else
        make clean > /dev/null 2>&1 || true
        make MODULE_NAME="$MODULE_NAME" -j128
    fi

    if [ ! -f "$MODULE_PATH" ]; then
        log_error "Build failed: $MODULE_PATH not found"
        exit 1
    fi

    log_success "Build complete"
else
    log_info "Step 1/4: Skipping build (--skip-build)"
    if [ ! -f "$MODULE_PATH" ]; then
        log_error "Module not found: $MODULE_PATH. Run without --skip-build first."
        exit 1
    fi
fi
echo ""

# Step 2: Unload existing module
log_info "Step 2/4: Checking existing module..."

if grep -q "^${MODULE_NAME} " /proc/modules 2>/dev/null; then
    if mount | grep -q " type ${MODULE_NAME} "; then
        log_error "${MODULE_NAME} is mounted. Unmount first or use uninstall.sh"
        exit 1
    fi
    log_warn "Unloading existing ${MODULE_NAME} module..."
    rmmod "${MODULE_NAME}"
    log_success "Existing module unloaded"
else
    log_info "No existing module loaded"
fi
echo ""

# Step 3: Load module
log_info "Step 3/4: Loading module (node_id=$NODE_ID)..."

insmod "$MODULE_PATH" node_id=$NODE_ID

if ! grep -q "^${MODULE_NAME} " /proc/modules 2>/dev/null; then
    log_error "Module load failed"
    exit 1
fi

log_success "Module loaded (node_id=$NODE_ID)"
echo ""

# Step 4: Mount (optional)
if [ $DO_MOUNT -eq 1 ]; then
    if [ $DO_FORMAT -eq 1 ]; then
        log_info "Step 4/4: Formatting and mounting..."
    else
        log_info "Step 4/4: Mounting..."
    fi

    if [ ! -e "$DEVICE" ]; then
        log_error "Device not found: $DEVICE"
        exit 1
    fi

    mkdir -p "$MOUNT_POINT"

    if mount | grep -q " $MOUNT_POINT "; then
        log_warn "$MOUNT_POINT already mounted. Unmounting..."
        umount "$MOUNT_POINT"
    fi

    MOUNT_OPTS="daxdev=$DEVICE,node_id=$NODE_ID"
    if [ $DO_FORMAT -eq 1 ]; then
        MOUNT_OPTS="$MOUNT_OPTS,format"
    fi

    log_info "Mounting: $DEVICE -> $MOUNT_POINT (node_id=$NODE_ID)"
    mount -t "${MODULE_NAME}" -o "$MOUNT_OPTS" none "$MOUNT_POINT"

    if ! mount | grep -q "$MOUNT_POINT type ${MODULE_NAME}"; then
        log_error "Mount failed"
        exit 1
    fi

    log_success "Mounted: $DEVICE -> $MOUNT_POINT"
else
    log_info "Step 4/4: Skipping format/mount (no --mount option)"
fi

echo ""
echo "============================================"
echo "  ${MODULE_NAME} Installation Complete!"
echo "============================================"
echo "  Module: ${MODULE_NAME} (node_id=$NODE_ID)"
if [ $DO_MOUNT -eq 1 ]; then
    echo "  Mount:  $DEVICE -> $MOUNT_POINT"
fi
echo ""
echo "  Uninstall: sudo ./uninstall.sh"
echo ""
