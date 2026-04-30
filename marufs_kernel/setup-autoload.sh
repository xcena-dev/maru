#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# setup-autoload.sh - Configure MARUFS kernel module auto-load at boot
#
# Usage:
#   sudo ./scripts/setup-autoload.sh                           # Install + auto-load
#   sudo ./scripts/setup-autoload.sh --mount /dev/dax6.0       # + auto-mount (fstab)
#   sudo ./scripts/setup-autoload.sh --mount /dev/dax6.0 --systemd  # + auto-mount (systemd)
#   sudo ./scripts/setup-autoload.sh --uninstall               # Remove all config
#   sudo ./scripts/setup-autoload.sh --status                  # Show current state

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Find project root: walk up from SCRIPT_DIR until Makefile with MODULE_NAME is found
PROJECT_DIR="$SCRIPT_DIR"
while [ "$PROJECT_DIR" != "/" ]; do
    [ -f "$PROJECT_DIR/Makefile" ] && grep -q 'MODULE_NAME' "$PROJECT_DIR/Makefile" 2>/dev/null && break
    PROJECT_DIR="$(dirname "$PROJECT_DIR")"
done

MODULE_NAME="marufs"
MODULE_KO="$PROJECT_DIR/build/${MODULE_NAME}.ko"
KERNEL_VER="$(uname -r)"
INSTALL_DIR="/lib/modules/${KERNEL_VER}/extra"
MODPROBE_CONF="/etc/modprobe.d/${MODULE_NAME}.conf"
MODULES_CONF="/etc/modules-load.d/${MODULE_NAME}.conf"
SYSTEMD_UNIT="/etc/systemd/system/${MODULE_NAME}-mount.service"
MOUNT_POINT="/mnt/${MODULE_NAME}"
NODE_ID=1

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()    { echo -e "${CYAN}[INFO]${NC} $1"; }
log_ok()      { echo -e "${GREEN}[ OK ]${NC} $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_err()     { echo -e "${RED}[ERR ]${NC} $1"; }

# --- Argument parsing ---
ACTION="install"
DAX_DEVICE=""
USE_SYSTEMD=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --mount)       DAX_DEVICE="$2"; shift 2 ;;
        --systemd)     USE_SYSTEMD=true; shift ;;
        --node-id)     NODE_ID="$2"; shift 2 ;;
        --uninstall)   ACTION="uninstall"; shift ;;
        --status)      ACTION="status"; shift ;;
        --help|-h)
            cat <<'USAGE'
Usage: sudo ./scripts/setup-autoload.sh [OPTIONS]

Actions:
  (default)         Install module + configure auto-load
  --uninstall       Remove all auto-load/mount config
  --status          Show current configuration state

Options:
  --mount DEV       Also configure auto-mount (e.g., --mount /dev/dax6.0)
  --systemd         Use systemd unit instead of fstab for auto-mount
  --node-id ID      Node ID for mount (default: 1)
USAGE
            exit 0
            ;;
        *) log_err "Unknown option: $1"; exit 1 ;;
    esac
done

# --- Root check ---
if [ "$(id -u)" -ne 0 ]; then
    log_err "Must run as root (sudo)"
    exit 1
fi

# ============================================================================
# STATUS
# ============================================================================
do_status() {
    echo "============================================"
    echo "  MARUFS Auto-Load Status"
    echo "============================================"

    # Module installed?
    if [ -f "${INSTALL_DIR}/${MODULE_NAME}.ko" ]; then
        log_ok "Module installed: ${INSTALL_DIR}/${MODULE_NAME}.ko"
    else
        log_warn "Module not installed in system path"
    fi

    # Module loaded?
    if lsmod | grep -q "^${MODULE_NAME} "; then
        log_ok "Module loaded"
    else
        log_warn "Module not loaded"
    fi

    # Auto-load config?
    if [ -f "$MODULES_CONF" ]; then
        log_ok "Auto-load: $MODULES_CONF"
    else
        log_warn "Auto-load not configured"
    fi

    # modprobe config?
    if [ -f "$MODPROBE_CONF" ]; then
        log_ok "Modprobe config: $MODPROBE_CONF"
        echo "       $(cat "$MODPROBE_CONF" | grep -v '^#' | grep -v '^$')"
    else
        log_info "No modprobe config (using defaults)"
    fi

    # Auto-mount?
    if grep -q "${MODULE_NAME}" /etc/fstab 2>/dev/null; then
        log_ok "Auto-mount: fstab"
        grep "${MODULE_NAME}" /etc/fstab | sed 's/^/       /'
    elif [ -f "$SYSTEMD_UNIT" ]; then
        local enabled
        enabled=$(systemctl is-enabled ${MODULE_NAME}-mount.service 2>/dev/null || echo "disabled")
        log_ok "Auto-mount: systemd ($enabled)"
    else
        log_info "Auto-mount not configured"
    fi

    # Current mounts?
    if mount | grep -q "${MODULE_NAME}"; then
        log_ok "Active mounts:"
        mount | grep "${MODULE_NAME}" | sed 's/^/       /'
    else
        log_info "No active mounts"
    fi

    echo "============================================"
}

# ============================================================================
# UNINSTALL
# ============================================================================
do_uninstall() {
    echo "============================================"
    echo "  MARUFS Auto-Load Uninstall"
    echo "============================================"

    # Remove systemd unit
    if [ -f "$SYSTEMD_UNIT" ]; then
        systemctl disable ${MODULE_NAME}-mount.service 2>/dev/null || true
        rm -f "$SYSTEMD_UNIT"
        systemctl daemon-reload
        log_ok "Removed systemd unit"
    fi

    # Remove fstab entries
    if grep -q "${MODULE_NAME}" /etc/fstab 2>/dev/null; then
        sed -i "/${MODULE_NAME}/d" /etc/fstab
        # Remove leftover comment
        sed -i '/# MARUFS CXL filesystem/d' /etc/fstab
        log_ok "Removed fstab entries"
    fi

    # Remove auto-load config
    if [ -f "$MODULES_CONF" ]; then
        rm -f "$MODULES_CONF"
        log_ok "Removed $MODULES_CONF"
    fi

    # Remove modprobe config
    if [ -f "$MODPROBE_CONF" ]; then
        rm -f "$MODPROBE_CONF"
        log_ok "Removed $MODPROBE_CONF"
    fi

    # Remove installed module
    if [ -f "${INSTALL_DIR}/${MODULE_NAME}.ko" ]; then
        rm -f "${INSTALL_DIR}/${MODULE_NAME}.ko"
        depmod -a
        log_ok "Removed module from system path"
    fi

    log_ok "Uninstall complete"
    echo "============================================"
}

# ============================================================================
# INSTALL
# ============================================================================
do_install() {
    echo "============================================"
    echo "  MARUFS Auto-Load Setup"
    echo "============================================"

    # Step 1: Check module exists
    if [ ! -f "$MODULE_KO" ]; then
        log_err "Module not found: $MODULE_KO"
        log_info "Build first: cd $PROJECT_DIR && make -j128"
        exit 1
    fi
    log_ok "Module found: $MODULE_KO"

    # Step 2: Install to system path
    mkdir -p "$INSTALL_DIR"
    cp "$MODULE_KO" "$INSTALL_DIR/"
    depmod -a
    log_ok "Installed to $INSTALL_DIR/"

    # Verify
    if ! modinfo "$MODULE_NAME" > /dev/null 2>&1; then
        log_err "modinfo failed after install"
        exit 1
    fi
    log_ok "modinfo verification passed"

    # Step 3: Auto-load config
    echo "$MODULE_NAME" > "$MODULES_CONF"
    log_ok "Auto-load configured: $MODULES_CONF"

    # Step 4: Create mount point
    mkdir -p "$MOUNT_POINT"
    log_ok "Mount point: $MOUNT_POINT"

    # Step 5: Auto-mount (if --mount specified)
    if [ -n "$DAX_DEVICE" ]; then
        if [ ! -e "$DAX_DEVICE" ]; then
            log_warn "DAX device $DAX_DEVICE not found (will use nofail)"
        fi

        if [ "$USE_SYSTEMD" = true ]; then
            cat > "$SYSTEMD_UNIT" << UNIT
[Unit]
Description=Mount MARUFS CXL filesystem
After=systemd-modules-load.service
ConditionPathExists=$DAX_DEVICE

[Service]
Type=oneshot
RemainAfterExit=yes

ExecStart=/bin/mount -t ${MODULE_NAME} -o daxdev=${DAX_DEVICE},node_id=${NODE_ID} none ${MOUNT_POINT}
ExecStart=/bin/chmod 1777 ${MOUNT_POINT}

ExecStop=/bin/umount ${MOUNT_POINT}

[Install]
WantedBy=multi-user.target
UNIT
            systemctl daemon-reload
            systemctl enable ${MODULE_NAME}-mount.service
            log_ok "Auto-mount: systemd unit enabled"
        else
            # fstab (remove old entries first)
            sed -i "/${MODULE_NAME}/d" /etc/fstab
            sed -i '/# MARUFS CXL filesystem/d' /etc/fstab
            cat >> /etc/fstab << FSTAB

# MARUFS CXL filesystem (auto-generated by setup-autoload.sh)
none  ${MOUNT_POINT}  ${MODULE_NAME}  daxdev=${DAX_DEVICE},node_id=${NODE_ID},nofail  0  0
FSTAB
            log_ok "Auto-mount: fstab configured"
        fi
    else
        log_info "Auto-mount skipped (use --mount /dev/daxX.Y to enable)"
    fi

    echo ""
    echo "============================================"
    log_ok "Setup complete!"
    echo "============================================"
    echo ""
    echo "  Module will auto-load on next boot."
    if [ -n "$DAX_DEVICE" ]; then
        echo "  Filesystem will auto-mount on next boot."
        echo ""
        echo "  To mount now:"
        echo "    sudo modprobe ${MODULE_NAME}"
        if [ "$USE_SYSTEMD" = true ]; then
            echo "    sudo systemctl start ${MODULE_NAME}-mount.service"
        else
            echo "    sudo systemctl daemon-reload"
            echo "    sudo mount ${MOUNT_POINT}"
        fi
    fi
    echo ""
}

# --- Dispatch ---
case "$ACTION" in
    install)   do_install ;;
    uninstall) do_uninstall ;;
    status)    do_status ;;
esac
