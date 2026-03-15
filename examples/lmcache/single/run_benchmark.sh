#!/bin/bash
# Single-instance KV cache benchmark -- delegates to run_benchmark.py
# Measures TTFT speedup: Query 1 computes KV -> Query 2 retrieves from Maru cache
if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python "$SCRIPT_DIR/run_benchmark.py" "$@"
