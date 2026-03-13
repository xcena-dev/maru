#!/bin/bash
# Benchmark for single SGLang instance — measures TTFT with repeated prompts
if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../common" && pwd)"
exec python "$COMMON_DIR/run_benchmark.py" \
    --single-port "${SGLANG_PORT}" \
    --mode single \
    "$@"
