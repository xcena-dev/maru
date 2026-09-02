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

# --- Decide whether the KV placement kernels can be built ------------------

# setup.py describes the maru_kv_ops CUDA extension through
# torch.utils.cpp_extension, so PyTorch has to be importable while the build
# runs. A PEP 517 isolated build only gets the packages in
# [build-system].requires, so it never sees the PyTorch installed in the target
# environment and the extension is skipped without a word — the vLLM connector
# then copies one layer at a time, which is materially slower. Turning
# isolation off is what points the build at the interpreter the serving engine
# uses, and it means the build backend has to be present there already.
TARGET_PYTHON="${VIRTUAL_ENV:+${VIRTUAL_ENV}/bin/python}"
TARGET_PYTHON="${TARGET_PYTHON:-$(command -v python3)}"

BUILD_ARGS=()
if "$TARGET_PYTHON" -c 'import torch' >/dev/null 2>&1; then
    echo "PyTorch found; building Maru with the KV placement kernels ..."
    "${INSTALL_CMD[@]}" "setuptools>=61.0" wheel
    BUILD_ARGS=(--no-build-isolation)
else
    echo "Note: PyTorch is not installed in the target environment."
    echo "      Maru installs without the KV placement kernels, and the vLLM"
    echo "      connector falls back to a copy per layer. Install PyTorch and"
    echo "      the CUDA toolkit, then rerun this script to build them."
    echo ""
fi

# --- Install Python package ------------------------------------------------

echo "Installing Maru Python package ..."
"${INSTALL_CMD[@]}" ${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"} -e "${SCRIPT_DIR}"

# --- Report whether the kernels are callable -------------------------------

# An unbuilt extension raises nothing at runtime, so the install is where it
# can still be reported against what was asked for.
if [ ${#BUILD_ARGS[@]} -gt 0 ]; then
    if "$TARGET_PYTHON" -c 'import maru_kv_ops, sys; sys.exit(0 if maru_kv_ops.is_available() else 1)' 2>/dev/null; then
        echo "KV placement kernels: built."
    else
        echo ""
        echo "Warning: the KV placement kernels did not build."
        "$TARGET_PYTHON" -c 'import maru_kv_ops; print("         reason:", maru_kv_ops.import_error())' 2>/dev/null || true
        echo "         The vLLM connector will use its slower per-layer copy path."
    fi
fi

# --- Build and install resource manager ------------------------------------

if [ "$INSTALL_RM" -eq 1 ]; then
    echo ""
    echo "Building and installing maru-resource-manager ..."
    echo "This step requires root privileges."
    sudo "$(which install-maru-resource-manager)"
fi

echo ""
echo "Build complete."
