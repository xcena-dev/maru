#!/bin/bash
# Run P2P KV cache sharing benchmark between two vLLM instances
# Measures TTFT to validate KV cache sharing through CXL.
#
# Prerequisites:
#   1. maru-server running
#   2. Two vLLM instances running via launch_vllm.sh
#
# Usage:
#   ./run_benchmark.sh [--model MODEL] [--max-tokens N] [--repeat-count N]

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

python "$SCRIPT_DIR/run_benchmark.py" \
    --port1 "$MARU_INST1_PORT" \
    --port2 "$MARU_INST2_PORT" \
    "$@"
