#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# uninstall.sh - MARUFS uninstallation script
#
# Unmounts all MARUFS filesystems and unloads the kernel module.
#
# Usage:
#   sudo ./uninstall.sh           # Unmount all + unload module
#   sudo ./uninstall.sh --force   # Force unmount + unload module
#

set -e

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;36m'
NC='\033[0m' # No Color

MODULE_NAME="${MARUFS_MODULE_NAME:-marufs}"
FORCE_UMOUNT=0

# Helper functions with module name prefix
log_info()    { echo -e "${BLUE}[INFO]${NC} [$MODULE_NAME] $1"; }
log_success() { echo -e "${GREEN}[ OK ]${NC} [$MODULE_NAME] $1"; }
log_error()   { echo -e "${RED}[ERR ]${NC} [$MODULE_NAME] $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} [$MODULE_NAME] $1"; }

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --force)        FORCE_UMOUNT=1; shift ;;
        --module-name)  MODULE_NAME="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: sudo $0 [--force] [--module-name <name>]"
            exit 0
            ;;
        *)  log_error "Unknown option: $1"; exit 1 ;;
    esac
done

echo "============================================"
echo "  ${MODULE_NAME} Uninstallation"
echo "============================================"
echo ""

# Step 1: Unmount all filesystems
log_info "Step 1/2: Checking for ${MODULE_NAME} mounts..."

MOUNTED_FS=$(mount | grep " type ${MODULE_NAME} " | awk '{print $3}' || true)

if [ -z "$MOUNTED_FS" ]; then
    log_info "No mounted ${MODULE_NAME} filesystems found"
else
    while read -r mp; do
        if [ $FORCE_UMOUNT -eq 1 ]; then
            log_info "Force unmounting: $mp"
            umount -f "$mp" 2>/dev/null || {
                log_error "Force unmount failed: $mp"
                exit 1
            }
        else
            log_info "Unmounting: $mp"
            umount "$mp" 2>/dev/null || {
                log_error "Unmount failed: $mp (use --force or check: lsof | grep $mp)"
                exit 1
            }
        fi
        log_success "Unmounted: $mp"
    done <<< "$MOUNTED_FS"
fi
echo ""

# Step 2: Unload module
log_info "Step 2/2: Unloading ${MODULE_NAME} module..."

if ! grep -q "^${MODULE_NAME} " /proc/modules 2>/dev/null; then
    log_info "${MODULE_NAME} module is not loaded"
else
    rmmod "${MODULE_NAME}"
    if grep -q "^${MODULE_NAME} " /proc/modules 2>/dev/null; then
        log_error "Module unload failed (check dmesg)"
        exit 1
    fi
    log_success "Module unloaded"
fi

echo ""
echo "============================================"
echo "  Uninstallation Complete!"
echo "============================================"
echo ""
echo "  Reinstall: sudo ./install.sh"
echo ""
