#!/usr/bin/env bash
set -euo pipefail

# NOTE: We recommend using a virtual environment before running this script.
#   uv venv --python 3.12 .venv && source .venv/bin/activate
# (or, without uv: python3 -m venv .venv && source .venv/bin/activate)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Parse arguments -------------------------------------------------------

INSTALL_RM=1

usage() {
    echo "Usage: $(basename "$0") [OPTIONS]"
    echo ""
    echo "Build and install Maru (Python package + maru-resource-manager)."
    echo ""
    echo "Options:"
    echo "  --no-rm      Skip building/installing maru-resource-manager"
    echo "  -h, --help   Show this help message"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-rm)
            INSTALL_RM=0
            shift
            ;;
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
    echo "We recommend using a venv: uv venv --python 3.12 .venv && source .venv/bin/activate"
    echo ""
fi

# --- Select the Python installer -------------------------------------------

# uv is preferred (much faster resolve/build); pip remains a working fallback.
if command -v uv &>/dev/null; then
    INSTALL_CMD=(uv pip install)
    # uv refuses to guess a target when no virtual environment is active,
    # so point it at the python3 on PATH to match pip's behaviour.
    if [ -z "${VIRTUAL_ENV:-}" ]; then
        INSTALL_CMD+=(--python "$(command -v python3)")
    fi
else
    echo "Note: uv not found, falling back to pip."
    echo "      Installing uv makes this step significantly faster:"
    echo "        curl -LsSf https://astral.sh/uv/install.sh | sh"
    echo ""
    INSTALL_CMD=(pip install)
fi

# --- Install Python package ------------------------------------------------

echo "Installing Maru Python package ..."
"${INSTALL_CMD[@]}" -e "${SCRIPT_DIR}"

# --- Build and install resource manager ------------------------------------

if [ "$INSTALL_RM" -eq 1 ]; then
    echo ""
    echo "Building and installing maru-resource-manager ..."
    echo "This step requires root privileges."
    sudo "$(which install-maru-resource-manager)"
fi

echo ""
echo "Build complete."
