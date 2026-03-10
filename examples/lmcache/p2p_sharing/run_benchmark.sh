#!/bin/bash
# P2P KV cache sharing benchmark — delegates to run_benchmark.py
# Measures TTFT speedup: Instance 1 stores KV cache -> Instance 2 retrieves via P2P
if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python "$SCRIPT_DIR/run_benchmark.py" "$@"
