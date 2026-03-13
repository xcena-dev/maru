#!/bin/bash
# P2P benchmark: measures TTFT speedup between inst1 (cold) and inst2 (warm/P2P hit).
#
# Usage:
#   bash run_benchmark.sh              # Default P2P benchmark
#   bash run_benchmark.sh --repeat-count 3  # Multiple runs

cd "$(dirname "${BASH_SOURCE[0]}")"
[ -f "env.sh" ] && source env.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../common" && pwd)"

exec python "$COMMON_DIR/run_benchmark.py" \
    --port1 "${SGLANG_INST1_PORT}" \
    --port2 "${SGLANG_INST2_PORT}" \
    --mode p2p \
    "$@"
