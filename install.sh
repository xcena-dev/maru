#!/usr/bin/env bash
set -euo pipefail

# NOTE: We recommend using a virtual environment before running this script.
#   python3 -m venv .venv && source .venv/bin/activate

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Parse arguments -------------------------------------------------------

usage() {
    echo "Usage: $(basename "$0") [OPTIONS]"
    echo ""
    echo "Build and install Maru (Python package + maru-resource-manager)."
    echo ""
    echo "Options:"
    echo "  -h, --help    Show this help message"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# --- Check prerequisites ---------------------------------------------------

check_cmd() {
    if ! command -v "$1" &>/dev/null; then
        echo "Error: $1 is not installed."
        echo "Run: sudo apt-get install -y $2"
        exit 1
    fi
}

check_cmd python3 python3
check_cmd cmake cmake
check_cmd gcc build-essential

# --- Check virtual environment ---------------------------------------------

if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected."
    echo "We recommend using a venv: python3 -m venv .venv && source .venv/bin/activate"
    echo ""
fi

# --- Install Python package ------------------------------------------------

echo "Installing Maru Python package ..."
pip install -e "${SCRIPT_DIR}"

# --- Build and install resource manager ------------------------------------

echo ""
echo "Building and installing maru-resource-manager ..."
echo "This step requires root privileges."
sudo "$(which install-maru-resource-manager)"

echo ""
echo "Build complete."
